package httpapi

import (
	"context"

	"github.com/wignerStan/infra/go/code/schema/api"
	workercontract "github.com/wignerStan/network-data-plane-worker/go/contracts/worker"
	"github.com/wignerStan/network-data-plane-worker/go/management"
)

// ManagementEntry is the typed management boundary registered with the HTTP
// facility. Transport construction and serving belong to product assembly.
type ManagementEntry interface {
	Manifest(context.Context, *ManifestRequest) (*ManifestResponse, error)
	Observed(context.Context, *ObservedRequest) (*ObservedResponse, error)
	Descriptor(context.Context, *DescriptorRequest) (*api.RawJSON, error)
	Replace(context.Context, *ReplaceRequest) (*ReplaceResponse, error)
	RegisterRoutes(api.RouteTable) error
	Guard(context.Context, api.Request) error
}

type ManifestRequest struct{}
type ManifestResponse struct{ Body workercontract.Manifest }
type ObservedRequest struct{}
type ObservedResponse struct{ Body []workercontract.Observed }
type DescriptorRequest struct {
	Digest string `path:"digest" pattern:"^sha256:[a-f0-9]{64}$"`
}
type ReplaceRequest struct {
	Name string `path:"name" pattern:"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$"`
	Body management.ReplaceRequest
}
type ReplaceResponse struct{ Body workercontract.Version }

var _ ManagementEntry = (*Entry)(nil)
