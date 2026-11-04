// Package ttrpc binds finite typed control operations to Infra's metadata carrier.
// It has no object-content operations and does not import a data-plane runtime.
package ttrpc

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"time"

	carrier "github.com/wignerStan/infra/go/network/actions/ttrpc"
	clock "github.com/wignerStan/infra/go/time/clock"
	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/descriptor"
	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/descriptor/admission"
	dp "github.com/wignerStan/network-data-plane-worker/go/contracts/worker"
	"github.com/wignerStan/remote-storage-gateway/go/contracts/control/broker"
)

const (
	infoMethod       = "worker.info"
	manifestMethod   = "worker.manifest"
	observedMethod   = "worker.observed"
	descriptorMethod = "worker.descriptor"
	replaceMethod    = "worker.replace"
	decideMethod     = "broker.decide"
)

func limits() carrier.Limits {
	return carrier.Limits{MaxPayload: descriptor.MaxBytes, MaxInflight: 16, MaxQueuedBytes: 4 * descriptor.MaxBytes, Timeout: 30 * time.Second}
}

type WorkerClient struct{ wire *carrier.Client }

func DialWorker(ctx context.Context, path string) (*WorkerClient, error) {
	c, e := carrier.Dial(ctx, path, limits())
	if e != nil {
		return nil, e
	}
	return &WorkerClient{c}, nil
}
func (c *WorkerClient) Close(ctx context.Context) error { return c.wire.Close(ctx) }
func call(ctx context.Context, c *carrier.Client, method string, input any) ([]byte, error) {
	b, e := json.Marshal(input)
	if e != nil || len(b) > descriptor.MaxBytes {
		return nil, broker.ErrInvalid
	}
	return c.Call(ctx, method, b)
}
func decode(b []byte, out any) error {
	if len(b) == 0 || len(b) > descriptor.MaxBytes || admission.StrictJSON(b, out) != nil {
		return broker.ErrInvalid
	}
	return nil
}
func (c *WorkerClient) Info(ctx context.Context) (broker.InstanceInfo, error) {
	var v broker.InstanceInfo
	b, e := call(ctx, c.wire, infoMethod, struct{}{})
	if e == nil {
		e = decode(b, &v)
	}
	if e == nil && (v.Contract != broker.Contract || v.ID == "") {
		e = broker.ErrInvalid
	}
	return v, e
}
func (c *WorkerClient) Manifest(ctx context.Context) (dp.Manifest, error) {
	var v dp.Manifest
	b, e := call(ctx, c.wire, manifestMethod, struct{}{})
	if e == nil {
		e = decode(b, &v)
	}
	if e == nil && v.Schema != dp.CapabilitySchema {
		e = broker.ErrInvalid
	}
	return v, e
}
func (c *WorkerClient) Observed(ctx context.Context) ([]dp.Observed, error) {
	var v []dp.Observed
	b, e := call(ctx, c.wire, observedMethod, struct{}{})
	if e == nil {
		e = decode(b, &v)
	}
	return v, e
}
func (c *WorkerClient) Descriptor(ctx context.Context, digest string) (descriptor.Record, error) {
	if !descriptor.ValidDigest(digest) {
		return descriptor.Record{}, broker.ErrInvalid
	}
	b, e := call(ctx, c.wire, descriptorMethod, descriptorRequest{digest})
	if e != nil {
		return descriptor.Record{}, e
	}
	v, e := admission.Decode(b)
	if e != nil || v.Digest != digest {
		return descriptor.Record{}, broker.ErrInvalid
	}
	return v, nil
}
func (c *WorkerClient) Replace(ctx context.Context, name string, expected dp.Version, selection dp.Selection) (dp.Version, error) {
	var v dp.Version
	if name == "" || selection.Name != name {
		return v, broker.ErrInvalid
	}
	b, e := call(ctx, c.wire, replaceMethod, replaceRequest{name, expected, selection})
	if e == nil {
		e = decode(b, &v)
	}
	return v, e
}

type DecisionClient struct {
	wire    *carrier.Client
	allowed map[string]bool
}

func DialDecision(ctx context.Context, path string, metadataNames []string) (*DecisionClient, error) {
	c, e := carrier.Dial(ctx, path, limits())
	if e != nil {
		return nil, e
	}
	return &DecisionClient{c, names(metadataNames)}, nil
}
func (c *DecisionClient) Close(ctx context.Context) error { return c.wire.Close(ctx) }
func (c *DecisionClient) Decide(ctx context.Context, input broker.TransferIntent) (broker.Decision, error) {
	var out broker.Decision
	if e := broker.ValidateIntent(input, c.allowed); e != nil {
		return out, e
	}
	started := (clock.SystemClock{}).Now()
	b, e := call(ctx, c.wire, decideMethod, input)
	if e == nil {
		e = decode(b, &out)
	}
	if e == nil {
		e = broker.ValidateDecision(input, out)
	}
	if e == nil {
		deadline, _ := ctx.Deadline()
		if !(clock.SystemClock{}).Now().Before(out.ExpiresAt(started, deadline)) {
			e = context.DeadlineExceeded
		}
	}
	return out, e
}
func names(in []string) map[string]bool {
	v := map[string]bool{}
	for _, n := range in {
		v[n] = true
	}
	return v
}

type descriptorRequest struct {
	Digest string `json:"digest"`
}
type replaceRequest struct {
	Name      string       `json:"name"`
	Expected  dp.Version   `json:"expected"`
	Selection dp.Selection `json:"selection"`
}

// Server owns the finite metadata dispatcher. The assembly owns private Unix
// socket creation and identity binding; it never authenticates an instance ID
// supplied in a request as a peer identity.
type Server struct{ wire *carrier.Server }

func (s *Server) Serve(listener net.Listener) error { return s.wire.Serve(listener) }
func (s *Server) ServeConn(ctx context.Context, conn net.Conn) error {
	return s.wire.ServeConn(ctx, conn)
}
func (s *Server) Close(ctx context.Context) error { return s.wire.Close(ctx) }
func NewWorkerServer(control broker.WorkerControl) (*Server, error) {
	if control == nil {
		return nil, broker.ErrInvalid
	}
	s, e := carrier.NewServer(&workerEntry{control}, limits())
	if e != nil {
		return nil, e
	}
	return &Server{s}, nil
}

type workerEntry struct{ control broker.WorkerControl }

func status(e error) error {
	if e == nil {
		return nil
	}
	if errors.Is(e, context.Canceled) || errors.Is(e, context.DeadlineExceeded) {
		return e
	}
	if errors.Is(e, broker.ErrInvalid) {
		return carrier.ErrInvalid
	}
	if errors.Is(e, broker.ErrDenied) {
		return &carrier.StatusError{Code: 7}
	}
	return carrier.ErrClosed
}
func emitJSON(emit func([]byte) error, v any) error {
	b, e := json.Marshal(v)
	if e != nil || len(b) > descriptor.MaxBytes {
		return carrier.ErrInvalid
	}
	return emit(b)
}
func (e *workerEntry) Dispatch(ctx context.Context, method string, b []byte, stream bool, emit func([]byte) error) error {
	if stream {
		return carrier.ErrInvalid
	}
	switch method {
	case infoMethod, manifestMethod, observedMethod:
		if decode(b, &struct{}{}) != nil {
			return carrier.ErrInvalid
		}
		switch method {
		case infoMethod:
			v, err := e.control.Info(ctx)
			if err != nil {
				return status(err)
			}
			return emitJSON(emit, v)
		case manifestMethod:
			v, err := e.control.Manifest(ctx)
			if err != nil {
				return status(err)
			}
			return emitJSON(emit, v)
		default:
			v, err := e.control.Observed(ctx)
			if err != nil {
				return status(err)
			}
			return emitJSON(emit, v)
		}
	case descriptorMethod:
		var in descriptorRequest
		if decode(b, &in) != nil || !descriptor.ValidDigest(in.Digest) {
			return carrier.ErrInvalid
		}
		record, err := e.control.Descriptor(ctx, in.Digest)
		if err != nil {
			return status(err)
		}
		verified, err := admission.Decode(record.Bytes)
		if err != nil || verified.Digest != in.Digest || record.Digest != in.Digest {
			return carrier.ErrInvalid
		}
		return emit(record.Bytes)
	case replaceMethod:
		var in replaceRequest
		if decode(b, &in) != nil || in.Name == "" || in.Name != in.Selection.Name {
			return carrier.ErrInvalid
		}
		v, err := e.control.Replace(ctx, in.Name, in.Expected, in.Selection)
		if err != nil {
			return status(err)
		}
		return emitJSON(emit, v)
	default:
		return carrier.ErrNotFound
	}
}

// DecisionOptions binds principal from a trusted accepted connection context.
// A caller-provided instance ID or HTTP header is not an identity provider.
type DecisionOptions struct {
	Principal     func(context.Context) (string, error)
	MetadataNames []string
}

func NewDecisionServer(service broker.DecisionBroker, o DecisionOptions) (*Server, error) {
	if service == nil || o.Principal == nil {
		return nil, broker.ErrInvalid
	}
	s, e := carrier.NewServer(&decisionEntry{service, o.Principal, names(o.MetadataNames)}, limits())
	if e != nil {
		return nil, e
	}
	return &Server{s}, nil
}

type decisionEntry struct {
	service   broker.DecisionBroker
	principal func(context.Context) (string, error)
	allowed   map[string]bool
}

func (e *decisionEntry) Dispatch(ctx context.Context, method string, b []byte, stream bool, emit func([]byte) error) error {
	if stream || method != decideMethod {
		return carrier.ErrInvalid
	}
	var in broker.TransferIntent
	if decode(b, &in) != nil || broker.ValidateIntent(in, e.allowed) != nil {
		return carrier.ErrInvalid
	}
	principal, err := e.principal(ctx)
	if err != nil || principal == "" || len(principal) > 256 {
		return &carrier.StatusError{Code: 7}
	}
	in.Principal = principal
	out, err := e.service.Decide(ctx, in)
	if err != nil {
		return status(err)
	}
	if broker.ValidateDecision(in, out) != nil {
		return carrier.ErrInvalid
	}
	return emitJSON(emit, out)
}

const vfsInfoMethod = "vfs.info"

type VFSClient struct{ wire *carrier.Client }

func DialVFS(ctx context.Context, path string) (*VFSClient, error) {
	c, e := carrier.Dial(ctx, path, limits())
	if e != nil {
		return nil, e
	}
	return &VFSClient{c}, nil
}
func (c *VFSClient) Close(ctx context.Context) error { return c.wire.Close(ctx) }
func (c *VFSClient) Info(ctx context.Context) (broker.InstanceInfo, error) {
	var out broker.InstanceInfo
	b, e := call(ctx, c.wire, vfsInfoMethod, struct{}{})
	if e == nil {
		e = decode(b, &out)
	}
	if e == nil && (out.Contract != broker.Contract || out.ID == "") {
		e = broker.ErrInvalid
	}
	return out, e
}
func NewVFSServer(control broker.VFSControl) (*Server, error) {
	if control == nil {
		return nil, broker.ErrInvalid
	}
	s, e := carrier.NewServer(&vfsEntry{control}, limits())
	if e != nil {
		return nil, e
	}
	return &Server{s}, nil
}

type vfsEntry struct{ control broker.VFSControl }

func (e *vfsEntry) Dispatch(ctx context.Context, method string, b []byte, stream bool, emit func([]byte) error) error {
	if stream || method != vfsInfoMethod || decode(b, &struct{}{}) != nil {
		return carrier.ErrInvalid
	}
	out, err := e.control.Info(ctx)
	if err != nil {
		return status(err)
	}
	return emitJSON(emit, out)
}
