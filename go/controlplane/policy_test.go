package controlplane

import (
	"context"
	"errors"
	cpconfig "github.com/wignerStan/remote-storage-gateway/go/contracts/control"
	"github.com/wignerStan/remote-storage-gateway/go/contracts/control/broker"
	"testing"
)

func TestTransferPolicyIsExactBoundedAndAmbiguityDenies(t *testing.T) {
	in := broker.TransferIntent{Principal: "worker-a", Authority: "cache.local", Method: "/google.bytestream.ByteStream/Read", SourceID: "cas", TargetID: "upstream"}
	rule := cpconfig.Rule{InstanceID: in.Principal, Authority: in.Authority, SourceID: in.SourceID, TargetID: in.TargetID, Decision: broker.Decision{RouteID: "cas", WorkerDataEndpoint: "http://127.0.0.1:8080", ApprovedUpstreamID: "cas", TTLSeconds: 30, MaxMessageBytes: 1 << 20, MaxInflightBytes: 4 << 20, Methods: []string{in.Method}}}
	rules := []cpconfig.Rule{rule}
	grant, e := resolve(rules, in)
	if e != nil {
		t.Fatal(e)
	}
	grant.Methods[0] = "/evil.Service/Read"
	if rules[0].Decision.Methods[0] != in.Method {
		t.Fatal("caller mutated policy")
	}
	for _, change := range []func(*broker.TransferIntent){func(i *broker.TransferIntent) { i.Principal = "worker-b" }, func(i *broker.TransferIntent) { i.Method = "/google.bytestream.ByteStream/Write" }, func(i *broker.TransferIntent) { i.Authority = "other.local" }, func(i *broker.TransferIntent) { i.SourceID = "other" }, func(i *broker.TransferIntent) { i.TargetID = "other" }} {
		copy := in
		change(&copy)
		if _, e = resolve(rules, copy); !errors.Is(e, broker.ErrDenied) {
			t.Fatal("non-exact request admitted", copy, e)
		}
	}
	if _, e = resolve(append(rules, rule), in); !errors.Is(e, broker.ErrDenied) {
		t.Fatal("ambiguous policy admitted")
	}
	rule.Decision.TTLSeconds = 0
	if _, e = resolve([]cpconfig.Rule{rule}, in); !errors.Is(e, broker.ErrDenied) {
		t.Fatal("unbounded grant admitted")
	}
}
func TestManagerDefaultDeniesAndCloseIsIdempotent(t *testing.T) {
	m, e := New(cpconfig.Config{})
	if e != nil {
		t.Fatal(e)
	}
	if e = m.Start(context.Background()); e != nil {
		t.Fatal(e)
	}
	if _, e = m.Decide(context.Background(), broker.TransferIntent{}); !errors.Is(e, broker.ErrDenied) {
		t.Fatal(e)
	}
	if e = m.Close(context.Background()); e != nil {
		t.Fatal(e)
	}
	if e = m.Close(context.Background()); e != nil {
		t.Fatal(e)
	}
	if e = m.Start(context.Background()); !errors.Is(e, broker.ErrUnavailable) {
		t.Fatal(e)
	}
}
