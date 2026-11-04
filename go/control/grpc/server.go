//go:build infra_grpc

package grpc

import (
	"context"
	grpcserver "github.com/wignerStan/infra/go/network/rpc/grpc/server"
	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/descriptor"
	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/descriptor/admission"
	dp "github.com/wignerStan/network-data-plane-worker/go/contracts/worker"
	cfg "github.com/wignerStan/remote-storage-gateway/go/contracts/control"
	"github.com/wignerStan/remote-storage-gateway/go/contracts/control/broker"
	pb "github.com/wignerStan/remote-storage-gateway/go/control/grpc/rpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
	"net"
	"time"
)

type workerEntry struct {
	pb.UnimplementedWorkerServer
	control broker.WorkerControl
}

func mapped(v any, e error) (*pb.Metadata, error) {
	if e != nil {
		return nil, status.Error(codes.FailedPrecondition, "control operation rejected")
	}
	out, e := encode(v)
	if e != nil {
		return nil, status.Error(codes.Internal, "invalid control result")
	}
	return out, nil
}
func (e *workerEntry) Info(ctx context.Context, _ *pb.Empty) (*pb.Metadata, error) {
	v, err := e.control.Info(ctx)
	return mapped(v, err)
}
func (e *workerEntry) Manifest(ctx context.Context, _ *pb.Empty) (*pb.Metadata, error) {
	v, err := e.control.Manifest(ctx)
	return mapped(v, err)
}
func (e *workerEntry) Observed(ctx context.Context, _ *pb.Empty) (*pb.Metadata, error) {
	v, err := e.control.Observed(ctx)
	return mapped(v, err)
}
func (e *workerEntry) Descriptor(ctx context.Context, in *pb.DescriptorRequest) (*pb.Metadata, error) {
	if in == nil || !descriptor.ValidDigest(in.Digest) {
		return nil, status.Error(codes.InvalidArgument, "invalid descriptor digest")
	}
	v, err := e.control.Descriptor(ctx, in.Digest)
	if err != nil || v.Digest != in.Digest || len(v.Bytes) > descriptor.MaxBytes {
		return nil, status.Error(codes.NotFound, "descriptor unavailable")
	}
	return &pb.Metadata{Json: v.Bytes}, nil
}
func (e *workerEntry) Replace(ctx context.Context, in *pb.ReplaceRequest) (*pb.Metadata, error) {
	var req struct {
		Expected  dp.Version   `json:"expected"`
		Selection dp.Selection `json:"selection"`
	}
	if in == nil || len(in.Json) > descriptor.MaxBytes || admission.StrictJSON(in.Json, &req) != nil || in.Name != req.Selection.Name {
		return nil, status.Error(codes.InvalidArgument, "invalid replacement")
	}
	v, err := e.control.Replace(ctx, in.Name, req.Expected, req.Selection)
	return mapped(v, err)
}

type decisionEntry struct {
	pb.UnimplementedDecisionsServer
	broker broker.DecisionBroker
	names  map[string]bool
}

func (e *decisionEntry) Decide(ctx context.Context, in *pb.Metadata) (*pb.Metadata, error) {
	var intent broker.TransferIntent
	if in == nil || len(in.Json) > broker.MaxMetadataBytes || admission.StrictJSON(in.Json, &intent) != nil || broker.ValidateIntent(intent, e.names) != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid decision metadata")
	}
	intent.Principal = principal(ctx)
	v, err := e.broker.Decide(ctx, intent)
	if err != nil {
		return nil, status.Error(codes.PermissionDenied, "decision denied")
	}
	return mapped(v, nil)
}

// Serve reuses Infra's gRPC lifecycle. TLS URI SANs bind the request principal;
// callers cannot choose a principal in the serialized intent.
func serve(ctx context.Context, ln net.Listener, c cfg.RPCServer, worker broker.WorkerControl, decisions broker.DecisionBroker, names []string, started chan struct{}) error {
	if worker == nil && decisions == nil {
		return broker.ErrInvalid
	}
	if e := validateServer(c); e != nil {
		return e
	}
	t, e := tlsConfig(c.TLS, true)
	if e != nil {
		return e
	}
	allowed := map[string]bool{}
	for _, n := range names {
		allowed[n] = true
	}
	return grpcserver.ServeContext(ctx, ln, grpcserver.GRPCServer{ShutdownTimeout: 5 * time.Second, Options: []grpc.ServerOption{grpc.Creds(credentials.NewTLS(t)), grpc.UnaryInterceptor(authenticate(c.Peers)), grpc.MaxRecvMsgSize(descriptor.MaxBytes + 1024), grpc.MaxSendMsgSize(descriptor.MaxBytes + 1024), grpc.MaxConcurrentStreams(16)}, Register: func(reg grpc.ServiceRegistrar) {
		defer close(started)
		if worker != nil {
			pb.RegisterWorkerServer(reg, &workerEntry{control: worker})
		}
		if decisions != nil {
			pb.RegisterDecisionsServer(reg, &decisionEntry{broker: decisions, names: allowed})
		}
	}})
}

type Server struct {
	cancel context.CancelFunc
	done   chan struct{}
	err    error
}

func StartServer(ctx context.Context, c cfg.RPCServer, worker broker.WorkerControl, decisions broker.DecisionBroker, names []string) (*Server, error) {
	if ctx == nil {
		return nil, broker.ErrInvalid
	}
	if err := validateServer(c); err != nil {
		return nil, err
	}
	if _, err := tlsConfig(c.TLS, true); err != nil {
		return nil, err
	}
	ln, err := net.Listen("tcp", c.Listen)
	if err != nil {
		return nil, err
	}
	run, cancel := context.WithCancel(context.Background())
	s := &Server{cancel: cancel, done: make(chan struct{})}
	started := make(chan struct{})
	go func() {
		defer close(s.done)
		defer ln.Close()
		s.err = serve(run, ln, c, worker, decisions, names, started)
	}()
	select {
	case <-started:
		return s, nil
	case <-s.done:
		cancel()
		return nil, s.err
	case <-ctx.Done():
		cancel()
		<-s.done
		return nil, ctx.Err()
	}
}
func (s *Server) Done() <-chan struct{} { return s.done }
func (s *Server) Close(ctx context.Context) error {
	if ctx == nil {
		return broker.ErrInvalid
	}
	s.cancel()
	select {
	case <-s.done:
		return s.err
	case <-ctx.Done():
		return ctx.Err()
	}
}
