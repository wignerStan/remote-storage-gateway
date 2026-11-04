//go:build dataplane_l7

package runtime

import (
	"context"
	"testing"
	"time"

	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/encoded"
	"github.com/wignerStan/remote-storage-gateway/go/contracts/control/broker"
)

type brokerFunc func(context.Context, broker.TransferIntent) (broker.Decision, error)

func (f brokerFunc) Decide(ctx context.Context, in broker.TransferIntent) (broker.Decision, error) {
	return f(ctx, in)
}
func TestAuthorizerProjectsMetadataAndPreservesPhysicalDeadline(t *testing.T) {
	var observed broker.TransferIntent
	service := brokerFunc(func(_ context.Context, in broker.TransferIntent) (broker.Decision, error) {
		observed = in
		return broker.Decision{RouteID: "build", WorkerDataEndpoint: "https://worker.invalid", ApprovedUpstreamID: "approved-buildbarn", TTLSeconds: 30, MaxMessageBytes: 1 << 20, MaxInflightBytes: 2 << 20, Methods: []string{in.Method}}, nil
	})
	authorize, e := NewAuthorizer(service, []string{"x-request-id"})
	if e != nil {
		t.Fatal(e)
	}
	deadline := time.Now().Add(10 * time.Second)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	grant, e := authorize(ctx, encoded.RequestMetadata{Method: "/build.bazel.remote.execution.v2.Execution/Execute", Authority: "worker.invalid", Metadata: map[string]string{"authorization": "Bearer private", "x-request-id": "trace"}})
	if e != nil {
		t.Fatal(e)
	}
	if len(observed.Metadata) != 1 || observed.Metadata["x-request-id"] != "trace" {
		t.Fatal("credential metadata reached broker")
	}
	if grant.UpstreamID != "approved-buildbarn" || grant.ExpiresUnixNano != deadline.UnixNano() {
		t.Fatal("grant broadened logical route/deadline", grant)
	}
	if _, e = NewAuthorizer(service, []string{"authorization"}); e == nil {
		t.Fatal("credential metadata configured for CP")
	}
}
func TestAuthorizerRejectsDecisionExpiredDuringRPC(t *testing.T) {
	service := brokerFunc(func(_ context.Context, in broker.TransferIntent) (broker.Decision, error) {
		time.Sleep(1100 * time.Millisecond)
		return broker.Decision{RouteID: "build", WorkerDataEndpoint: "https://worker.invalid", ApprovedUpstreamID: "buildbarn", TTLSeconds: 1, MaxMessageBytes: 1 << 20, MaxInflightBytes: 2 << 20, Methods: []string{in.Method}}, nil
	})
	authorize, e := NewAuthorizer(service, nil)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = authorize(context.Background(), encoded.RequestMetadata{Method: "/build.Execution/Execute", Authority: "worker.invalid"}); e != context.DeadlineExceeded {
		t.Fatalf("response renewed expired physical-start grant: %v", e)
	}
}
