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
	for _, method := range []string{"/google.bytestream.ByteStream/Read", "/google.bytestream.ByteStream/Write", "/google.bytestream.ByteStream/QueryWriteStatus", "/build.bazel.remote.execution.v2.ContentAddressableStorage/BatchReadBlobs", "/build.bazel.remote.execution.v2.Execution/Execute", "/google.longrunning.Operations/GetOperation"} {
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
	receive := findAction(t, actions, encoded.Receive, encoded.Downstream)
	if _, e = s.Step(ctx, encoded.Event{ID: receive.ID, Kind: encoded.Receive, Side: encoded.Downstream, Frame: encoded.FrameRef{ID: 1, Generation: 8, Bytes: 1}}); e == nil {
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
	if e != nil {
		t.Fatal(a, e)
	}
	receive := findAction(t, a, encoded.Receive, encoded.Downstream)
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

func findAction(t *testing.T, actions []encoded.Action, kind encoded.Kind, side encoded.Side) encoded.Action {
	t.Helper()
	for _, a := range actions {
		if a.Kind == kind && a.Side == side {
			return a
		}
	}
	t.Fatalf("missing %s action for %d", kind, side)
	return encoded.Action{}
}
func opened(t *testing.T, method string) (encoded.Session, []encoded.Action) {
	t.Helper()
	s, e := factory(t).New(context.Background(), start(method))
	if e != nil {
		t.Fatal(e)
	}
	a, e := s.Step(context.Background(), encoded.Event{Kind: encoded.Begin})
	if e != nil {
		t.Fatal(e)
	}
	actions, e := s.Step(context.Background(), encoded.Event{ID: a[0].ID, Kind: encoded.Open, Side: encoded.Upstream})
	if e != nil {
		t.Fatal(e)
	}
	return s, actions
}

func TestInitialHeadersDoNotWaitForResponseBody(t *testing.T) {
	ctx := context.Background()
	s, actions := opened(t, "/google.bytestream.ByteStream/Write")
	header := findAction(t, actions, encoded.ReceiveHeaders, encoded.Upstream)
	a, e := s.Step(ctx, encoded.Event{ID: header.ID, Kind: encoded.ReceiveHeaders, Side: encoded.Upstream, Headers: 42})
	if e != nil {
		t.Fatal(e)
	}
	publish := findAction(t, a, encoded.Headers, 0)
	if publish.Result != 42 {
		t.Fatal("header identity changed")
	}
	a, e = s.Step(ctx, encoded.Event{ID: publish.ID, Kind: encoded.Headers})
	if e != nil || len(a) != 0 {
		t.Fatal("headers require no response-body event", a, e)
	}
}
func TestResponseCannotOvertakeInitialHeaders(t *testing.T) {
	ctx := context.Background()
	s, actions := opened(t, "/google.bytestream.ByteStream/Read")
	response := findAction(t, actions, encoded.Receive, encoded.Upstream)
	header := findAction(t, actions, encoded.ReceiveHeaders, encoded.Upstream)
	a, e := s.Step(ctx, encoded.Event{ID: response.ID, Kind: encoded.Receive, Side: encoded.Upstream, Frame: encoded.FrameRef{ID: 9, Generation: 7, Bytes: 3}})
	if e != nil || len(a) != 0 {
		t.Fatal("response escaped before headers", a, e)
	}
	a, e = s.Step(ctx, encoded.Event{ID: header.ID, Kind: encoded.ReceiveHeaders, Side: encoded.Upstream, Headers: 42})
	if e != nil {
		t.Fatal(e)
	}
	publish := findAction(t, a, encoded.Headers, 0)
	a, e = s.Step(ctx, encoded.Event{ID: publish.ID, Kind: encoded.Headers})
	if e != nil {
		t.Fatal(e)
	}
	send := findAction(t, a, encoded.Send, encoded.Downstream)
	if send.Frame.ID != 9 {
		t.Fatal("deferred frame changed")
	}
}
func TestMissingUnaryMessagesFailInsteadOfEmptySuccess(t *testing.T) {
	ctx := context.Background()
	s, actions := opened(t, "/build.bazel.remote.execution.v2.Capabilities/GetCapabilities")
	down := findAction(t, actions, encoded.Receive, encoded.Downstream)
	if _, e := s.Step(ctx, encoded.Event{ID: down.ID, Kind: encoded.Receive, Side: encoded.Downstream, EOF: true}); e == nil {
		t.Fatal("empty unary request accepted")
	}
	s, actions = opened(t, "/build.bazel.remote.execution.v2.Capabilities/GetCapabilities")
	header := findAction(t, actions, encoded.ReceiveHeaders, encoded.Upstream)
	a, e := s.Step(ctx, encoded.Event{ID: header.ID, Kind: encoded.ReceiveHeaders, Side: encoded.Upstream, Headers: 42})
	if e != nil {
		t.Fatal(e)
	}
	publish := findAction(t, a, encoded.Headers, 0)
	if _, e = s.Step(ctx, encoded.Event{ID: publish.ID, Kind: encoded.Headers}); e != nil {
		t.Fatal(e)
	}
	up := findAction(t, actions, encoded.Receive, encoded.Upstream)
	if _, e = s.Step(ctx, encoded.Event{ID: up.ID, Kind: encoded.Receive, Side: encoded.Upstream, EOF: true}); e == nil {
		t.Fatal("empty unary response accepted")
	}
}
