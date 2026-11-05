package ttrpc

import (
	"context"
	"errors"
	"net"
	"sync"
	"time"

	"github.com/containerd/ttrpc"
	rpcttrpc "github.com/wignerStan/infra/go/network/rpc/client/ttrpc"
	actionsv1 "github.com/wignerStan/infra/go/network/rpc/protocols/actions"
	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/descriptor"
	"github.com/wignerStan/remote-storage-gateway/go/contracts/control/broker"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	actionsService  = "infra.actions.v1.Actions"
	maxInflight     = 16
	maxQueuedBytes  = 4 * descriptor.MaxBytes
	metadataTimeout = 30 * time.Second
)

type metadataClient struct {
	path, service string
	ctx           context.Context
	cancel        context.CancelFunc
	mu            sync.Mutex
	closed        bool
	active, bytes int
	pending       sync.WaitGroup
	once          sync.Once
	done          chan struct{}
}

func dialMetadata(ctx context.Context, path, service string) (*metadataClient, error) {
	if ctx == nil {
		return nil, broker.ErrInvalid
	}
	bounded, cancel := context.WithTimeout(ctx, metadataTimeout)
	defer cancel()
	wire, err := rpcttrpc.Dial(bounded, path, peerHandshake{})
	if err != nil {
		return nil, err
	}
	closeMetadataWire(wire)
	owned, stop := context.WithCancel(context.Background())
	return &metadataClient{path: path, service: service, ctx: owned, cancel: stop, done: make(chan struct{})}, nil
}

// Each finite call owns its native connection. Native unary ttrpc carries a
// deadline but has no per-call cancellation message: closing this connection
// cancels the remote callback without interrupting another admitted operation.
// Calls are never replayed, including after an unknown mutation outcome.
func (c *metadataClient) call(ctx context.Context, method string, input []byte) ([]byte, error) {
	if ctx == nil || len(input) == 0 || len(input) > descriptor.MaxBytes {
		return nil, broker.ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, broker.ErrUnavailable
	}
	if c.active >= maxInflight || c.bytes+len(input) > maxQueuedBytes {
		c.mu.Unlock()
		return nil, status.Error(codes.ResourceExhausted, "control capacity reached")
	}
	c.active++
	c.bytes += len(input)
	c.pending.Add(1)
	c.mu.Unlock()
	defer func() { c.mu.Lock(); c.active--; c.bytes -= len(input); c.mu.Unlock(); c.pending.Done() }()
	bounded, cancel := context.WithTimeout(ctx, metadataTimeout)
	defer cancel()
	stopOwner := context.AfterFunc(c.ctx, cancel)
	defer stopOwner()
	wire, err := rpcttrpc.Dial(bounded, c.path, peerHandshake{}, ttrpc.WithUnaryClientInterceptor(func(ctx context.Context, req *ttrpc.Request, resp *ttrpc.Response, _ *ttrpc.UnaryClientInfo, invoke ttrpc.Invoker) error {
		// Transport metadata has no place in this typed contract. In particular,
		// an incoming data request must not forward its authorization headers.
		req.Metadata = nil
		if len(req.Payload) > descriptor.MaxBytes+512 {
			return broker.ErrInvalid
		}
		if err := invoke(ctx, req, resp); err != nil {
			return err
		}
		if len(resp.Payload) > descriptor.MaxBytes+512 {
			return broker.ErrInvalid
		}
		return nil
	}))
	if err != nil {
		if bounded.Err() != nil {
			return nil, bounded.Err()
		}
		return nil, broker.ErrUnavailable
	}
	defer closeMetadataWire(wire)
	stopCall := context.AfterFunc(bounded, func() { _ = wire.Close() })
	defer stopCall()
	reply := new(actionsv1.CallReply)
	err = wire.Call(bounded, actionsService, "Call", &actionsv1.CallRequest{Binding: binding(c.service, method), Input: input}, reply)
	if err != nil {
		if bounded.Err() != nil {
			return nil, bounded.Err()
		}
		if errors.Is(err, broker.ErrInvalid) {
			return nil, err
		}
		return nil, clientError(err)
	}
	if len(reply.Output) == 0 || len(reply.Output) > descriptor.MaxBytes || len(reply.ProtoReflect().GetUnknown()) != 0 {
		return nil, broker.ErrInvalid
	}
	return reply.Output, nil
}
func closeMetadataWire(wire *ttrpc.Client) {
	_ = wire.Close()
	_ = wire.UserOnCloseWait(context.Background())
}

func (c *metadataClient) Close(ctx context.Context) error {
	if ctx == nil {
		return broker.ErrInvalid
	}
	c.once.Do(func() {
		c.mu.Lock()
		c.closed = true
		c.cancel()
		c.mu.Unlock()
		go func() { c.pending.Wait(); close(c.done) }()
	})
	select {
	case <-c.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type controlMethod func(context.Context, []byte) ([]byte, error)

// Server owns the finite control bindings and their callback lifetimes. Native
// ttrpc owns framing, protobuf encoding and protocol dispatch. Close joins the
// admitted callbacks as well as ending socket acceptance.
type Server struct {
	wire          *ttrpc.Server
	methods       map[string]controlMethod
	ctx           context.Context
	cancel        context.CancelFunc
	mu            sync.Mutex
	closing       bool
	active, bytes int
	listeners     map[net.Listener]struct{}
	connections   map[*controlConn]struct{}
	pending       sync.WaitGroup
	once          sync.Once
	done          chan struct{}
	err           error
}

func newServer(methods map[string]controlMethod) (*Server, error) {
	ctx, cancel := context.WithCancel(context.Background())
	s := &Server{ctx: ctx, cancel: cancel, methods: methods, listeners: map[net.Listener]struct{}{}, connections: map[*controlConn]struct{}{}, done: make(chan struct{})}
	wire, err := ttrpc.NewServer(ttrpc.WithUnaryServerInterceptor(s.intercept))
	if err != nil {
		cancel()
		return nil, err
	}
	s.wire = wire
	wire.RegisterService(actionsService, &ttrpc.ServiceDesc{Methods: map[string]ttrpc.Method{"Call": s.action}})
	return s, nil
}
func (s *Server) intercept(ctx context.Context, unmarshal ttrpc.Unmarshaler, _ *ttrpc.UnaryServerInfo, next ttrpc.Method) (any, error) {
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		return nil, publicStatus(broker.ErrUnavailable)
	}
	if s.active >= maxInflight {
		s.mu.Unlock()
		return nil, status.Error(codes.ResourceExhausted, "control capacity reached")
	}
	s.active++
	s.pending.Add(1)
	s.mu.Unlock()
	defer func() { s.mu.Lock(); s.active--; s.mu.Unlock(); s.pending.Done() }()
	if scope, ok := ctx.Value(connectionScopeKey{}).(*connectionScope); ok {
		if !scope.begin() {
			return nil, publicStatus(broker.ErrUnavailable)
		}
		defer scope.pending.Done()
	}
	bounded, stop := context.WithTimeout(ctx, metadataTimeout)
	defer stop()
	if err := bounded.Err(); err != nil {
		return nil, publicStatus(err)
	}
	return next(bounded, unmarshal)
}

func (s *Server) action(ctx context.Context, unmarshal func(any) error) (any, error) {
	req := new(actionsv1.CallRequest)
	if err := unmarshal(req); err != nil {
		return nil, publicStatus(broker.ErrInvalid)
	}
	if len(req.Input) == 0 || len(req.Input) > descriptor.MaxBytes || len(req.Binding) > 256 || len(req.ProtoReflect().GetUnknown()) != 0 {
		return nil, publicStatus(broker.ErrInvalid)
	}
	if metadata, ok := ttrpc.GetMetadata(ctx); ok && len(metadata) != 0 {
		return nil, publicStatus(broker.ErrInvalid)
	}
	method, ok := s.methods[req.Binding]
	if !ok {
		return nil, status.Error(codes.Unimplemented, "control binding not registered")
	}
	s.mu.Lock()
	if s.bytes+len(req.Input) > maxQueuedBytes {
		s.mu.Unlock()
		return nil, status.Error(codes.ResourceExhausted, "control byte capacity reached")
	}
	s.bytes += len(req.Input)
	s.mu.Unlock()
	defer func() { s.mu.Lock(); s.bytes -= len(req.Input); s.mu.Unlock() }()
	output, err := method(ctx, req.Input)
	if err != nil {
		return nil, publicStatus(err)
	}
	if err := ctx.Err(); err != nil {
		return nil, publicStatus(err)
	}
	if len(output) == 0 || len(output) > descriptor.MaxBytes {
		return nil, publicStatus(broker.ErrInvalid)
	}
	return &actionsv1.CallReply{Output: output}, nil
}

func (s *Server) Done() <-chan struct{} { return s.done }
func (s *Server) Serve(listener net.Listener) error {
	if listener == nil {
		return broker.ErrInvalid
	}
	err := s.serve(s.ctx, listener)
	_ = s.Close(context.Background())
	return err
}
func (s *Server) serve(ctx context.Context, listener net.Listener) error {
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		listener.Close()
		return nil
	}
	s.listeners[listener] = struct{}{}
	s.pending.Add(1)
	s.mu.Unlock()
	defer func() { s.mu.Lock(); delete(s.listeners, listener); s.mu.Unlock(); s.pending.Done() }()
	err := s.wire.Serve(ctx, &controlListener{Listener: listener, server: s})
	if errors.Is(err, ttrpc.ErrServerClosed) || errors.Is(err, net.ErrClosed) {
		return nil
	}
	return err
}
func (s *Server) ServeConn(ctx context.Context, conn net.Conn) error {
	if ctx == nil || conn == nil {
		return broker.ErrInvalid
	}
	scope := new(connectionScope)
	bounded, cancel := context.WithCancel(context.WithValue(ctx, connectionScopeKey{}, scope))
	defer func() { cancel(); scope.retire() }()
	stop := context.AfterFunc(s.ctx, cancel)
	defer stop()
	listener := &singleListener{conn: conn, done: make(chan struct{})}
	stopConn := context.AfterFunc(bounded, func() { _ = listener.Close() })
	defer stopConn()
	return s.serve(bounded, listener)
}
func (s *Server) Close(ctx context.Context) error {
	if ctx == nil {
		return broker.ErrInvalid
	}
	s.once.Do(func() {
		s.mu.Lock()
		s.closing = true
		s.cancel()
		listeners := make([]net.Listener, 0, len(s.listeners))
		for l := range s.listeners {
			listeners = append(listeners, l)
		}
		conns := make([]*controlConn, 0, len(s.connections))
		for c := range s.connections {
			conns = append(conns, c)
		}
		s.mu.Unlock()
		// Native Close alone does not join active callbacks. Closing every
		// accepted connection also interrupts a blocked receive or write.
		var errs []error
		for _, l := range listeners {
			if err := l.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
				errs = append(errs, err)
			}
		}
		for _, c := range conns {
			_ = c.Close()
		}
		if err := s.wire.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			errs = append(errs, err)
		}
		s.err = errors.Join(errs...)
		go func() { s.pending.Wait(); close(s.done) }()
	})
	select {
	case <-s.done:
		return s.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

type controlListener struct {
	net.Listener
	server *Server
}

func (l *controlListener) Accept() (net.Conn, error) {
	for {
		conn, err := l.Listener.Accept()
		if err != nil {
			return nil, err
		}
		if err = checkPeer(conn); err != nil {
			conn.Close()
			continue
		}
		l.server.mu.Lock()
		if l.server.closing || len(l.server.connections) >= 2*maxInflight {
			l.server.mu.Unlock()
			conn.Close()
			continue
		}
		c := &controlConn{Conn: conn, server: l.server}
		l.server.connections[c] = struct{}{}
		l.server.mu.Unlock()
		return c, nil
	}
}

type controlConn struct {
	net.Conn
	server *Server
	once   sync.Once
}

func (c *controlConn) Close() error {
	err := c.Conn.Close()
	c.once.Do(func() { c.server.mu.Lock(); delete(c.server.connections, c); c.server.mu.Unlock() })
	return err
}

type singleListener struct {
	conn       net.Conn
	once, stop sync.Once
	done       chan struct{}
}

func (l *singleListener) Accept() (net.Conn, error) {
	var c net.Conn
	l.once.Do(func() { c = &singleConn{Conn: l.conn, listener: l} })
	if c != nil {
		return c, nil
	}
	<-l.done
	return nil, net.ErrClosed
}
func (l *singleListener) Addr() net.Addr { return l.conn.LocalAddr() }
func (l *singleListener) Close() error {
	l.stop.Do(func() { close(l.done); _ = l.conn.Close() })
	return nil
}

type singleConn struct {
	net.Conn
	listener *singleListener
}

func (c *singleConn) Close() error { return c.listener.Close() }

// ServeConn must retain callback custody even after native Serve stops reading.
type connectionScopeKey struct{}
type connectionScope struct {
	mu      sync.Mutex
	closing bool
	pending sync.WaitGroup
}

func (s *connectionScope) begin() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closing {
		return false
	}
	s.pending.Add(1)
	return true
}
func (s *connectionScope) retire() { s.mu.Lock(); s.closing = true; s.mu.Unlock(); s.pending.Wait() }
