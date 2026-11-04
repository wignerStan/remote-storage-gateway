package broker

import (
	"testing"
	"time"
)

func TestControlMetadataCannotTransportCredentials(t *testing.T) {
	base := TransferIntent{Method: "/build.bazel.remote.execution.v2.Execution/Execute", Authority: "worker.internal"}
	if err := ValidateIntent(base, nil); err != nil {
		t.Fatal(err)
	}
	cases := []map[string]string{{"authorization": "Bearer private"}, {"cookie": "private"}, {"x-request-id": "ok\r\nauthorization: private"}, {"x-unknown": "private"}}
	allowed := map[string]bool{"authorization": true, "cookie": true, "x-request-id": true}
	for _, headers := range cases {
		in := base
		in.Metadata = headers
		if err := ValidateIntent(in, allowed); err == nil {
			t.Fatalf("admitted unsafe metadata keys: %v", headers)
		}
	}
	in := base
	in.Metadata = map[string]string{"x-request-id": "request-1"}
	if err := ValidateIntent(in, allowed); err != nil {
		t.Fatal(err)
	}
	if err := ValidateIntent(in, nil); err == nil {
		t.Fatal("empty allowlist admitted metadata")
	}
}

func TestDecisionCannotBroadenMethodEndpointOrLease(t *testing.T) {
	intent := TransferIntent{Method: "/build.bazel.remote.execution.v2.Execution/Execute", Authority: "worker.internal"}
	grant := Decision{RouteID: "build", WorkerDataEndpoint: "https://worker.internal", ApprovedUpstreamID: "buildbarn", TTLSeconds: 30, MaxMessageBytes: 4 << 20, MaxInflightBytes: 8 << 20, Methods: []string{intent.Method}}
	if err := ValidateDecision(intent, grant); err != nil {
		t.Fatal(err)
	}
	cases := []func(*Decision){func(d *Decision) { d.WorkerDataEndpoint = "https://name:private@worker.internal" }, func(d *Decision) { d.WorkerDataEndpoint = "https://worker.internal/?token=private" }, func(d *Decision) { d.Methods = []string{"*"} }, func(d *Decision) { d.Methods = []string{"/other.Service/Method"} }, func(d *Decision) { d.TTLSeconds = MaxTTLSeconds + 1 }, func(d *Decision) { d.MaxInflightBytes = MaxInflightBytes + 1 }}
	for n, mutate := range cases {
		bad := grant
		mutate(&bad)
		if err := ValidateDecision(intent, bad); err == nil {
			t.Fatalf("admitted escaped grant %d", n)
		}
	}
	start := time.Unix(1000, 0)
	if got := grant.ExpiresAt(start, time.Time{}); !got.Equal(time.Unix(1030, 0)) {
		t.Fatal(got)
	}
	if got := grant.ExpiresAt(start, time.Unix(1020, 0)); !got.Equal(time.Unix(1020, 0)) {
		t.Fatal(got)
	}
}
