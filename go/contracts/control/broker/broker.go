// Package broker describes metadata-only control between the control plane and
// independently managed worker processes. Object bodies and provider credentials
// have no representation in this protocol.
package broker

import (
	"context"
	"errors"
	"time"

	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/descriptor"
	workercontract "github.com/wignerStan/network-data-plane-worker/go/contracts/worker"
)

const Contract = "remote-storage-gateway/control-broker/v1"
const MaxMetadataBytes = 8 << 10
const MaxMetadataEntries = 32
const MaxTTLSeconds = 300
const MaxMessageBytes int64 = 64 << 20
const MaxInflightBytes int64 = 256 << 20

var (
	ErrInvalid     = errors.New("invalid control metadata")
	ErrUnavailable = errors.New("worker control unavailable")
	ErrDenied      = errors.New("transfer decision denied")
)

type InstanceInfo struct {
	LocalDataSocket string `json:"local_data_socket,omitempty"`
	Contract        string `json:"contract"`
	ID              string `json:"id"`
	Ready           bool   `json:"ready"`
	DataEndpoint    string `json:"data_endpoint"`
}

// WorkerControl is the control plane's finite management surface for one child.
// It cannot read, open, upload, or otherwise transfer object contents.
type WorkerControl interface {
	Info(context.Context) (InstanceInfo, error)
	Manifest(context.Context) (workercontract.Manifest, error)
	Observed(context.Context) ([]workercontract.Observed, error)
	Descriptor(context.Context, string) (descriptor.Record, error)
	Replace(context.Context, string, workercontract.Version, workercontract.Selection) (workercontract.Version, error)
}

// TransferIntent describes the authorization scope of an incoming RPC. Principal
// is supplied by the trusted worker transport context, never by request headers
// or serialized control requests. Metadata contains only configured safe names.
type TransferIntent struct {
	Method    string            `json:"method"`
	Authority string            `json:"authority"`
	Principal string            `json:"-"`
	SourceID  string            `json:"source_id,omitempty"`
	TargetID  string            `json:"target_id,omitempty"`
	Metadata  map[string]string `json:"metadata,omitempty"`
}

type Decision struct {
	RouteID            string   `json:"route_id"`
	WorkerDataEndpoint string   `json:"worker_data_endpoint"`
	ApprovedUpstreamID string   `json:"approved_upstream_id"`
	TTLSeconds         int      `json:"ttl_seconds"`
	MaxMessageBytes    int64    `json:"max_message_bytes"`
	MaxInflightBytes   int64    `json:"max_inflight_bytes"`
	Methods            []string `json:"methods"`
}

// DecisionBroker grants metadata-only routing decisions. The worker resolves an
// approved upstream ID locally; no upstream address or credential crosses this
// boundary. Callers measure expiry from the physical request start, not receipt.
type DecisionBroker interface {
	Decide(context.Context, TransferIntent) (Decision, error)
}

// ExpiresAt never extends a grant beyond the bounded physical-start lease or an
// already shorter caller deadline. Validation belongs to the admitting entry.
func (d Decision) ExpiresAt(start time.Time, deadline time.Time) time.Time {
	expiry := start.Add(time.Duration(d.TTLSeconds) * time.Second)
	if !deadline.IsZero() && deadline.Before(expiry) {
		return deadline
	}
	return expiry
}

// VFSControl is metadata-only; the independent materializer owns its data port.
type VFSControl interface {
	Info(context.Context) (InstanceInfo, error)
}
