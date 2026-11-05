// Package httpapi contributes typed control-plane management entries to the
// schema effect tool. The application assembly mounts their derived routes.
package httpapi

import (
	"context"
	"errors"

	api "github.com/wignerStan/infra/go/network/http/openapi"
	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/descriptor"
	dp "github.com/wignerStan/network-data-plane-worker/go/contracts/worker"
	"github.com/wignerStan/remote-storage-gateway/go/contracts/control/broker"
	"github.com/wignerStan/remote-storage-gateway/go/controlplane"
)

type Error struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (e *Error) Error() string  { return e.Message }
func (e *Error) GetStatus() int { return e.Code }
func reject(code int) error     { return &Error{code, "managed worker unavailable"} }

type Empty struct{}
type InstanceInput struct {
	Instance string `path:"instance"`
}
type DescriptorInput struct {
	InstanceInput
	Digest string `path:"digest"`
}
type ReplaceInput struct {
	InstanceInput
	Name string `path:"name"`
	Body ReplaceBody
}
type ReplaceBody struct {
	Expected  dp.Version   `json:"expected"`
	Selection dp.Selection `json:"selection"`
}
type Output[T any] struct{ Body T }
type Registry interface {
	Instances() []controlplane.Instance
	Worker(string) (broker.WorkerControl, error)
}
type Entry struct{ manager Registry }

func NewEntry(m Registry) *Entry { return &Entry{m} }
func (e *Entry) Instances(context.Context, *Empty) (*Output[[]controlplane.Instance], error) {
	if e.manager == nil {
		return nil, reject(503)
	}
	return &Output[[]controlplane.Instance]{e.manager.Instances()}, nil
}
func (e *Entry) control(id string) (broker.WorkerControl, error) {
	if e.manager == nil {
		return nil, reject(503)
	}
	c, err := e.manager.Worker(id)
	if err != nil {
		return nil, reject(503)
	}
	return c, nil
}
func (e *Entry) Manifest(ctx context.Context, in *InstanceInput) (*Output[dp.Manifest], error) {
	c, err := e.control(in.Instance)
	if err != nil {
		return nil, err
	}
	v, err := c.Manifest(ctx)
	if err != nil {
		return nil, reject(502)
	}
	return &Output[dp.Manifest]{v}, nil
}
func (e *Entry) Observed(ctx context.Context, in *InstanceInput) (*Output[[]dp.Observed], error) {
	c, err := e.control(in.Instance)
	if err != nil {
		return nil, err
	}
	v, err := c.Observed(ctx)
	if err != nil {
		return nil, reject(502)
	}
	return &Output[[]dp.Observed]{v}, nil
}
func (e *Entry) Descriptor(ctx context.Context, in *DescriptorInput) (*api.RawJSON, error) {
	c, err := e.control(in.Instance)
	if err != nil {
		return nil, err
	}
	v, err := c.Descriptor(ctx, in.Digest)
	if err != nil {
		return nil, reject(502)
	}
	return &api.RawJSON{Body: v.Bytes, ContentType: "application/json", ETag: `"` + v.Digest + `"`}, nil
}
func (e *Entry) Replace(ctx context.Context, in *ReplaceInput) (*Output[dp.Version], error) {
	if in.Name != in.Body.Selection.Name {
		return nil, reject(400)
	}
	c, err := e.control(in.Instance)
	if err != nil {
		return nil, err
	}
	v, err := c.Replace(ctx, in.Name, in.Body.Expected, in.Body.Selection)
	if err != nil {
		if errors.Is(err, broker.ErrInvalid) {
			return nil, reject(400)
		}
		if errors.Is(err, broker.ErrRejected) {
			return nil, &Error{409, "desired worker state rejected"}
		}
		if errors.Is(err, broker.ErrUnavailable) {
			return nil, reject(503)
		}
		return nil, reject(502)
	}
	return &Output[dp.Version]{v}, nil
}
func operation(id, method, path string) api.Operation {
	return api.Operation{ID: id, Method: method, Path: path, MaxBodyBytes: descriptor.MaxBytes, RejectUnknownQueryParameters: true}
}
func (e *Entry) RegisterRoutes(table api.RouteTable) error {
	if err := api.Register[Empty, Output[[]controlplane.Instance]](table, operation("gateway-instances", "GET", "/instances"), e.Instances); err != nil {
		return err
	}
	if err := api.Register[InstanceInput, Output[dp.Manifest]](table, operation("gateway-instance-manifest", "GET", "/instances/{instance}/manifest"), e.Manifest); err != nil {
		return err
	}
	if err := api.Register[InstanceInput, Output[[]dp.Observed]](table, operation("gateway-instance-observed", "GET", "/instances/{instance}/observed"), e.Observed); err != nil {
		return err
	}
	if err := api.Register[ReplaceInput, Output[dp.Version]](table, operation("gateway-instance-replace", "POST", "/instances/{instance}/clients/{name}"), e.Replace); err != nil {
		return err
	}
	return api.RegisterRawJSON[DescriptorInput, descriptor.Descriptor](table, operation("gateway-instance-descriptor", "GET", "/instances/{instance}/descriptors/{digest}"), e.Descriptor)
}
