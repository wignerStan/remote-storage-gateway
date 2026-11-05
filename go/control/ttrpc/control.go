// Package ttrpc binds finite typed control operations to the official Actions
// protobuf over native ttrpc. Object-content operations are not registered.
package ttrpc

import (
	"context"
	"encoding/json"
	"errors"

	clock "github.com/wignerStan/infra/go/time/clock"
	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/descriptor"
	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/descriptor/admission"
	dp "github.com/wignerStan/network-data-plane-worker/go/contracts/worker"
	"github.com/wignerStan/remote-storage-gateway/go/contracts/control/broker"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	infoMethod       = "worker.info"
	manifestMethod   = "worker.manifest"
	observedMethod   = "worker.observed"
	descriptorMethod = "worker.descriptor"
	replaceMethod    = "worker.replace"
	decideMethod     = "broker.decide"
)

// The role remains part of the Actions binding, so a broker socket cannot admit
// a worker-management action even when both transports use Actions.Call.
func binding(service, method string) string { return service + "/" + method }

type WorkerClient struct{ wire *metadataClient }

func DialWorker(ctx context.Context, path string) (*WorkerClient, error) {
	c, err := dialMetadata(ctx, path, broker.WorkerRPCService)
	if err != nil {
		return nil, err
	}
	return &WorkerClient{c}, nil
}
func (c *WorkerClient) Close(ctx context.Context) error { return c.wire.Close(ctx) }
func call(ctx context.Context, c *metadataClient, method string, input any) ([]byte, error) {
	b, err := json.Marshal(input)
	if err != nil || len(b) > descriptor.MaxBytes {
		return nil, broker.ErrInvalid
	}
	return c.call(ctx, method, b)
}
func decode(b []byte, out any) error {
	if len(b) == 0 || len(b) > descriptor.MaxBytes || admission.StrictJSON(b, out) != nil {
		return broker.ErrInvalid
	}
	return nil
}
func (c *WorkerClient) Info(ctx context.Context) (broker.InstanceInfo, error) {
	var v broker.InstanceInfo
	b, err := call(ctx, c.wire, infoMethod, struct{}{})
	if err == nil {
		err = decode(b, &v)
	}
	if err == nil && (v.Contract != broker.Contract || v.ID == "") {
		err = broker.ErrInvalid
	}
	return v, err
}
func (c *WorkerClient) Manifest(ctx context.Context) (dp.Manifest, error) {
	var v dp.Manifest
	b, err := call(ctx, c.wire, manifestMethod, struct{}{})
	if err == nil {
		err = decode(b, &v)
	}
	if err == nil && v.Schema != dp.CapabilitySchema {
		err = broker.ErrInvalid
	}
	return v, err
}
func (c *WorkerClient) Observed(ctx context.Context) ([]dp.Observed, error) {
	var v []dp.Observed
	b, err := call(ctx, c.wire, observedMethod, struct{}{})
	if err == nil {
		err = decode(b, &v)
	}
	return v, err
}
func (c *WorkerClient) Descriptor(ctx context.Context, digest string) (descriptor.Record, error) {
	if !descriptor.ValidDigest(digest) {
		return descriptor.Record{}, broker.ErrInvalid
	}
	b, err := call(ctx, c.wire, descriptorMethod, descriptorRequest{digest})
	if err != nil {
		return descriptor.Record{}, err
	}
	v, err := admission.Decode(b)
	if err != nil || v.Digest != digest {
		return descriptor.Record{}, broker.ErrInvalid
	}
	return v, nil
}
func (c *WorkerClient) Replace(ctx context.Context, name string, expected dp.Version, selection dp.Selection) (dp.Version, error) {
	var v dp.Version
	if name == "" || selection.Name != name {
		return v, broker.ErrInvalid
	}
	b, err := call(ctx, c.wire, replaceMethod, replaceRequest{name, expected, selection})
	if err == nil {
		err = decode(b, &v)
	}
	return v, err
}

type DecisionClient struct {
	wire    *metadataClient
	allowed map[string]bool
}

func DialDecision(ctx context.Context, path string, metadataNames []string) (*DecisionClient, error) {
	c, err := dialMetadata(ctx, path, broker.DecisionRPCService)
	if err != nil {
		return nil, err
	}
	return &DecisionClient{c, names(metadataNames)}, nil
}
func (c *DecisionClient) Close(ctx context.Context) error { return c.wire.Close(ctx) }
func (c *DecisionClient) Decide(ctx context.Context, input broker.TransferIntent) (broker.Decision, error) {
	var out broker.Decision
	if ctx == nil {
		return out, broker.ErrInvalid
	}
	if err := broker.ValidateIntent(input, c.allowed); err != nil {
		return out, err
	}
	started := (clock.SystemClock{}).Now()
	b, err := call(ctx, c.wire, decideMethod, input)
	if err == nil {
		err = decode(b, &out)
	}
	if err == nil {
		err = broker.ValidateDecision(input, out)
	}
	if err == nil {
		deadline, _ := ctx.Deadline()
		if !(clock.SystemClock{}).Now().Before(out.ExpiresAt(started, deadline)) {
			err = context.DeadlineExceeded
		}
	}
	return out, err
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

func encode(v any) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil || len(b) > descriptor.MaxBytes {
		return nil, broker.ErrInvalid
	}
	return b, nil
}
func empty(b []byte) error { return decode(b, &struct{}{}) }

func NewWorkerServer(control broker.WorkerControl) (*Server, error) {
	if control == nil {
		return nil, broker.ErrInvalid
	}
	return newServer(map[string]controlMethod{
		binding(broker.WorkerRPCService, infoMethod): func(ctx context.Context, b []byte) ([]byte, error) {
			if err := empty(b); err != nil {
				return nil, err
			}
			v, err := control.Info(ctx)
			if err != nil {
				return nil, err
			}
			return encode(v)
		},
		binding(broker.WorkerRPCService, manifestMethod): func(ctx context.Context, b []byte) ([]byte, error) {
			if err := empty(b); err != nil {
				return nil, err
			}
			v, err := control.Manifest(ctx)
			if err != nil {
				return nil, err
			}
			return encode(v)
		},
		binding(broker.WorkerRPCService, observedMethod): func(ctx context.Context, b []byte) ([]byte, error) {
			if err := empty(b); err != nil {
				return nil, err
			}
			v, err := control.Observed(ctx)
			if err != nil {
				return nil, err
			}
			return encode(v)
		},
		binding(broker.WorkerRPCService, descriptorMethod): func(ctx context.Context, b []byte) ([]byte, error) {
			var in descriptorRequest
			if decode(b, &in) != nil || !descriptor.ValidDigest(in.Digest) {
				return nil, broker.ErrInvalid
			}
			record, err := control.Descriptor(ctx, in.Digest)
			if err != nil {
				return nil, err
			}
			verified, err := admission.Decode(record.Bytes)
			if err != nil || verified.Digest != in.Digest || record.Digest != in.Digest {
				return nil, broker.ErrInvalid
			}
			return record.Bytes, nil
		},
		binding(broker.WorkerRPCService, replaceMethod): func(ctx context.Context, b []byte) ([]byte, error) {
			var in replaceRequest
			if decode(b, &in) != nil || in.Name == "" || in.Name != in.Selection.Name {
				return nil, broker.ErrInvalid
			}
			v, err := control.Replace(ctx, in.Name, in.Expected, in.Selection)
			if err != nil {
				return nil, err
			}
			return encode(v)
		},
	})
}

// Principal is bound by the admitted transport, never serialized by a caller.
type DecisionOptions struct {
	Principal     func(context.Context) (string, error)
	MetadataNames []string
}

func NewDecisionServer(service broker.DecisionBroker, o DecisionOptions) (*Server, error) {
	if service == nil || o.Principal == nil {
		return nil, broker.ErrInvalid
	}
	allowed := names(o.MetadataNames)
	return newServer(map[string]controlMethod{
		binding(broker.DecisionRPCService, decideMethod): func(ctx context.Context, b []byte) ([]byte, error) {
			var in broker.TransferIntent
			if decode(b, &in) != nil || broker.ValidateIntent(in, allowed) != nil {
				return nil, broker.ErrInvalid
			}
			principal, err := o.Principal(ctx)
			if err != nil || principal == "" || len(principal) > 256 {
				return nil, broker.ErrDenied
			}
			in.Principal = principal
			out, err := service.Decide(ctx, in)
			if err != nil {
				return nil, err
			}
			if broker.ValidateDecision(in, out) != nil {
				return nil, broker.ErrInvalid
			}
			return encode(out)
		},
	})
}

// Only stable contract errors cross the process boundary; provider errors and
// their messages cannot expose configuration or credentials to a caller.
func publicStatus(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, "control canceled")
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, "control deadline exceeded")
	case errors.Is(err, broker.ErrInvalid):
		return status.Error(codes.InvalidArgument, "invalid control metadata")
	case errors.Is(err, broker.ErrRejected):
		return status.Error(codes.FailedPrecondition, "desired state rejected")
	case errors.Is(err, broker.ErrDenied):
		return status.Error(codes.PermissionDenied, "control denied")
	default:
		return status.Error(codes.Unavailable, "control unavailable")
	}
}
func clientError(err error) error {
	switch status.Code(err) {
	case codes.OK:
		return nil
	case codes.Canceled:
		return context.Canceled
	case codes.DeadlineExceeded:
		return context.DeadlineExceeded
	case codes.InvalidArgument:
		return broker.ErrInvalid
	case codes.FailedPrecondition:
		return broker.ErrRejected
	case codes.PermissionDenied, codes.Unauthenticated:
		return broker.ErrDenied
	case codes.ResourceExhausted, codes.Unimplemented:
		return err
	default:
		return broker.ErrUnavailable
	}
}
