//go:build dataplane_l7

package runtime

import (
	"context"
	clock "github.com/wignerStan/infra/go/time/clock"
	"strings"

	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/encoded"
	"github.com/wignerStan/remote-storage-gateway/go/contracts/control/broker"
)

// NewAuthorizer adapts only routing metadata to the CP decision broker. The
// returned grant contains a logical approved upstream ID, never its address,
// provider credentials, or a body decoded by the control plane.
func NewAuthorizer(decider broker.DecisionBroker, metadataNames []string) (func(context.Context, encoded.RequestMetadata) (encoded.Grant, error), error) {
	if decider == nil {
		return nil, broker.ErrInvalid
	}
	allowed := map[string]bool{}
	for _, name := range metadataNames {
		probe := broker.TransferIntent{Method: "/control.Validation/Metadata", Authority: "worker.invalid", Metadata: map[string]string{name: "safe"}}
		if name != strings.ToLower(name) || broker.ValidateIntent(probe, map[string]bool{name: true}) != nil {
			return nil, broker.ErrInvalid
		}
		allowed[name] = true
	}
	if len(allowed) > broker.MaxMetadataEntries {
		return nil, broker.ErrInvalid
	}
	return func(ctx context.Context, in encoded.RequestMetadata) (encoded.Grant, error) {
		if ctx == nil {
			return encoded.Grant{}, broker.ErrInvalid
		}
		started := (clock.SystemClock{}).Now()
		intent := broker.TransferIntent{Method: in.Method, Authority: in.Authority, SourceID: in.SourceID, TargetID: in.TargetID, Metadata: map[string]string{}}
		for name, value := range in.Metadata {
			if allowed[name] {
				intent.Metadata[name] = value
			}
		}
		if e := broker.ValidateIntent(intent, allowed); e != nil {
			return encoded.Grant{}, e
		}
		decision, e := decider.Decide(ctx, intent)
		if e != nil {
			return encoded.Grant{}, e
		}
		if e = broker.ValidateDecision(intent, decision); e != nil {
			return encoded.Grant{}, e
		}
		deadline, _ := ctx.Deadline()
		expiry := decision.ExpiresAt(started, deadline)
		if !(clock.SystemClock{}).Now().Before(expiry) {
			return encoded.Grant{}, context.DeadlineExceeded
		}
		return encoded.Grant{RouteID: decision.RouteID, UpstreamID: decision.ApprovedUpstreamID, ExpiresUnixNano: expiry.UnixNano(), MaxMessageBytes: decision.MaxMessageBytes, MaxInflightBytes: decision.MaxInflightBytes}, nil
	}, nil
}
