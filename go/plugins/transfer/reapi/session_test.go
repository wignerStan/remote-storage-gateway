package reapi

import (
	"context"
	"encoding/json"
	"github.com/wignerStan/infra/go/code/schema"
	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/descriptor"
	"strings"
	"testing"
	"time"

	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/encoded"
)

func factory(t *testing.T) *Factory {
	t.Helper()
	doc, e := schema.Derive[Configuration](schema.Identity{ID: "reapi.test", Version: "1", Owner: PluginID})
	if e != nil {
		t.Fatal(e)
	}
	raw, e := json.Marshal(doc.Schema)
	if e != nil {
		t.Fatal(e)
	}
	f, e := NewFactory(descriptor.Descriptor{Schema: descriptor.Schema, PluginID: PluginID, PluginVersion: "test", Contract: encoded.Contract, Runtime: "go", BuildDigest: "sha256:" + strings.Repeat("a", 64), Implementations: []descriptor.Implementation{{ID: "grpc", ConfigSchema: raw, Capabilities: []string{"encoded-grpc"}}}})
	if e != nil {
		t.Fatal(e)
	}
	return f
}
func start(method string) encoded.Start {
	return encoded.Start{ID: 7, Request: encoded.RequestMetadata{Method: method}, Grant: encoded.Grant{RouteID: "route", UpstreamID: "allowlisted", ExpiresUnixNano: time.Now().Add(time.Minute).UnixNano(), MaxMessageBytes: 65536, MaxInflightBytes: 131072}}
}
func TestOfficialMethodsAndPassiveConfiguration(t *testing.T) {
	f := factory(t)
	for _, method := range []string{"/google.bytestream.ByteStream/Read", "/google.bytestream.ByteStream/Write", "/google.bytestream.ByteStream/QueryWriteStatus", "/build.bazel.remote.execution.v2.ContentAddressableStorage/BatchReadBlobs", "/build.bazel.remote.execution.v2.Execution/Execute"} {
		if _, e := f.New(context.Background(), start(method)); e != nil {
			t.Fatalf("official %s: %v", method, e)
		}
	}
	if _, e := f.New(context.Background(), start("/arbitrary.Proxy/Fetch")); e == nil {
		t.Fatal("accepted unknown method")
	}
	cfg := start("/google.bytestream.ByteStream/Read")
	cfg.Configuration = json.RawMessage(`{"upstream":"https://arbitrary.invalid"}`)
	if _, e := f.New(context.Background(), cfg); e == nil {
		t.Fatal("configuration installed destination")
	}
	d := f.Descriptor()
	d.Implementations[0].Capabilities[0] = "mutated"
	d.Implementations[0].ConfigSchema[0] = 'x'
	if f.Descriptor().Implementations[0].Capabilities[0] == "mutated" {
		t.Fatal("descriptor mutable")
	}
}
func TestLeaseGenerationAndCompletionReplayFailClosed(t *testing.T) {
	ctx := context.Background()
	for _, bad := range []encoded.Event{{ID: 99, Kind: encoded.Open, Side: encoded.Upstream}, {ID: 1, Kind: encoded.Receive, Side: encoded.Upstream}} {
		s, e := factory(t).New(ctx, start("/google.bytestream.ByteStream/Write"))
		if e != nil {
			t.Fatal(e)
		}
		_, _ = s.Step(ctx, encoded.Event{Kind: encoded.Begin})
		if _, e = s.Step(ctx, bad); e == nil {
			t.Fatal("accepted forged completion")
		}
	}
	s, e := factory(t).New(ctx, start("/google.bytestream.ByteStream/Write"))
	if e != nil {
		t.Fatal(e)
	}
	a, _ := s.Step(ctx, encoded.Event{Kind: encoded.Begin})
	actions, e := s.Step(ctx, encoded.Event{ID: a[0].ID, Kind: encoded.Open, Side: encoded.Upstream})
	if e != nil {
		t.Fatal(e)
	}
	if _, e = s.Step(ctx, encoded.Event{ID: actions[0].ID, Kind: encoded.Receive, Side: encoded.Downstream, Frame: encoded.FrameRef{ID: 1, Generation: 8, Bytes: 1}}); e == nil {
		t.Fatal("accepted foreign generation")
	}
}
func TestBackpressureAndHalfCloseArePluginPolicy(t *testing.T) {
	ctx := context.Background()
	s, e := factory(t).New(ctx, start("/google.bytestream.ByteStream/Write"))
	if e != nil {
		t.Fatal(e)
	}
	a, _ := s.Step(ctx, encoded.Event{Kind: encoded.Begin})
	a, e = s.Step(ctx, encoded.Event{ID: a[0].ID, Kind: encoded.Open, Side: encoded.Upstream})
	if e != nil || len(a) != 2 {
		t.Fatal(a, e)
	}
	receive := a[0]
	a, e = s.Step(ctx, encoded.Event{ID: receive.ID, Kind: encoded.Receive, Side: encoded.Downstream, Frame: encoded.FrameRef{ID: 1, Generation: 7, Bytes: 12}})
	if e != nil || len(a) != 1 || a[0].Kind != encoded.Send {
		t.Fatal(a, e)
	}
	a, e = s.Step(ctx, encoded.Event{ID: a[0].ID, Kind: encoded.Send, Side: encoded.Upstream})
	if e != nil || len(a) != 1 || a[0].Kind != encoded.Release {
		t.Fatal(a, e)
	}
	a, e = s.Step(ctx, encoded.Event{ID: a[0].ID, Kind: encoded.Release, Side: encoded.Upstream})
	if e != nil || len(a) != 1 || a[0].Kind != encoded.Receive {
		t.Fatal(a, e)
	}
	a, e = s.Step(ctx, encoded.Event{ID: a[0].ID, Kind: encoded.Receive, Side: encoded.Downstream, EOF: true})
	if e != nil || len(a) != 1 || a[0].Kind != encoded.HalfClose {
		t.Fatal(a, e)
	}
}
