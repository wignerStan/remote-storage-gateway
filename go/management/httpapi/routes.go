package httpapi

import (
	"github.com/wignerStan/infra/go/code/schema/api"
	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/descriptor"
	"github.com/wignerStan/network-data-plane-worker/go/management"
)

// RegisterRoutes contributes typed operations to the schema effect tool. Its
// common binding retains concrete I/O for derivation; this entry never receives
// a framework API, serving engine, or an erased handler.
func (e *Entry) RegisterRoutes(table api.RouteTable) error {
	operation := func(id, method, path string) api.Operation {
		return api.Operation{ID: id, Method: method, Path: path, MaxBodyBytes: descriptor.MaxBytes, RejectUnknownQueryParameters: true}
	}
	if err := api.Register(table, operation("worker.manifest", "GET", management.ManifestPath), e.Manifest); err != nil {
		return err
	}
	if err := api.Register(table, operation("worker.observed", "GET", management.ObservedPath), e.Observed); err != nil {
		return err
	}
	if err := api.RegisterRawJSON[DescriptorRequest, descriptor.Descriptor](table, operation("worker.descriptor", "GET", management.DescriptorPrefix+"{digest}"), e.Descriptor); err != nil {
		return err
	}
	return api.Register(table, operation("worker.client.replace", "POST", management.ClientPrefix+"{name}"), e.Replace)
}
