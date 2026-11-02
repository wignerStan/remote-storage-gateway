package management

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/client"
	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/descriptor"
	"github.com/wignerStan/network-data-plane-worker/go/pluginruntime"
	"github.com/wignerStan/network-data-plane-worker/go/worker"
)

type factory struct {
	record       descriptor.Record
	preparations atomic.Int32
}

func (f *factory) Descriptor() descriptor.Descriptor   { return f.record.Descriptor }
func (f *factory) DescriptorRecord() descriptor.Record { return f.record }
func (f *factory) Prepare(context.Context, client.PluginConfig) (client.Candidate, error) {
	f.preparations.Add(1)
	return candidate{}, nil
}

type candidate struct{}

func (candidate) Validate(context.Context) error  { return nil }
func (candidate) Abort(context.Context) error     { return nil }
func (candidate) Publish() (client.Client, error) { return instance{}, nil }

type instance struct{}

func (instance) Close(context.Context) error { return nil }
func (instance) Stat(context.Context, client.ObjectRef) (client.ObjectInfo, error) {
	return client.ObjectInfo{}, client.ErrUnsupported
}
func (instance) List(context.Context, client.ListRequest) (client.ListResult, error) {
	return client.ListResult{}, client.ErrUnsupported
}
func (instance) Open(context.Context, client.OpenRequest) (client.ReadPlan, error) {
	return client.ReadPlan{}, client.ErrUnsupported
}
func (instance) Put(context.Context, client.PutRequest) (client.PutResult, error) {
	return client.PutResult{}, client.ErrUnsupported
}
func (instance) Delete(context.Context, client.ObjectRef) error { return client.ErrUnsupported }

func setup(t *testing.T) (*Handler, *pluginruntime.Manager, *factory) {
	t.Helper()
	r, err := descriptor.Encode(descriptor.Descriptor{Schema: descriptor.Schema, PluginID: "test.client", PluginVersion: "1", Contract: client.Contract, Runtime: "go", BuildDigest: "sha256:" + strings.Repeat("a", 64), Implementations: []descriptor.Implementation{{ID: "test", ConfigSchema: descriptor.ConfigSchema{Version: 1}, Capabilities: []string{"stat"}}}})
	if err != nil {
		t.Fatal(err)
	}
	r, err = descriptor.Decode(append([]byte(" \n"), r.Bytes...))
	if err != nil {
		t.Fatal(err)
	}
	f := &factory{record: r}
	registry := pluginruntime.NewRegistry()
	if err = registry.Register(f); err != nil {
		t.Fatal(err)
	}
	m, err := pluginruntime.NewManager(registry, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { m.Close(context.Background()) })
	manifest, err := worker.Capabilities(registry)
	if err != nil {
		t.Fatal(err)
	}
	h, err := NewHandler(Options{Registry: registry, Manager: m, Manifest: manifest, ClientNames: []string{"account"}})
	if err != nil {
		t.Fatal(err)
	}
	return h, m, f
}

func TestSDKPassiveDiscoveryExactDescriptorAndCAS(t *testing.T) {
	h, m, f := setup(t)
	s := httptest.NewServer(h)
	defer s.Close()
	c, err := NewClient(s.URL, s.Client())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	manifest, err := c.Manifest(ctx)
	if err != nil || len(manifest.PluginDescriptors) != 1 {
		t.Fatal(manifest, err)
	}
	r, err := c.Descriptor(ctx, manifest.PluginDescriptors[0].DescriptorDigest)
	if err != nil || !bytes.Equal(r.Bytes, f.record.Bytes) {
		t.Fatal("exact bytes changed", err)
	}
	state, err := c.Observed(ctx)
	if err != nil || len(state) != 0 || f.preparations.Load() != 0 {
		t.Fatal("passive management initialized provider", err)
	}
	selection := pluginruntime.Selection{Name: "account", PluginID: r.Descriptor.PluginID, DescriptorDigest: r.Digest, ImplementationID: "test", Public: []byte(`{}`)}
	expected := m.Current(selection.Name)
	v, err := c.Replace(ctx, selection.Name, expected, selection)
	if err != nil || v.Generation == 0 || f.preparations.Load() != 1 {
		t.Fatal(v, err)
	}
	if _, err = c.Replace(ctx, selection.Name, expected, selection); err == nil {
		t.Fatal("stale CAS accepted")
	} else {
		var status *StatusError
		if !errors.As(err, &status) || status.StatusCode != http.StatusConflict {
			t.Fatal(err)
		}
	}
	state, err = c.Observed(ctx)
	if err != nil || len(state) != 1 || state[0].Version != v {
		t.Fatal(state, err)
	}
	if _, err = c.Replace(ctx, "another", v, selection); !errors.Is(err, ErrInvalid) {
		t.Fatal("mismatched route identity accepted", err)
	}
}

func TestRegisteredRoutesFailClosed(t *testing.T) {
	h, _, _ := setup(t)
	for _, test := range []struct {
		method, path, body string
		status             int
	}{
		{"POST", ManifestPath, "{}", 405},
		{"GET", "/v1/object/open", "", 404},
		{"POST", ClientPrefix + "unknown", "{}", 404},
		{"POST", ClientPrefix + "account", `{"expected":{},"expected":{},"selection":{}}`, 400},
		{"GET", ObservedPath + "?target=http://arbitrary.invalid", "", 400},
		{"POST", ClientPrefix + "account", strings.Repeat(" ", descriptor.MaxBytes+1), 400},
	} {
		r := httptest.NewRequest(test.method, test.path, strings.NewReader(test.body))
		r.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != test.status {
			t.Fatalf("%s %s: %d", test.method, test.path, w.Code)
		}
	}
	routes := RouteDefinitions()
	routes[0].Path = "tampered"
	if RouteDefinitions()[0].Path != ManifestPath || !IsPath("/v1/unknown") || IsPath("/dav/file") {
		t.Fatal("route definitions not isolated")
	}
}

func TestSDKRejectsDigestMismatchRedirectsAndRemoteDiagnostics(t *testing.T) {
	_, _, f := setup(t)
	var redirected atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { redirected.Add(1) }))
	defer target.Close()
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, DescriptorPrefix) {
			w.Header().Set("Content-Type", "application/json")
			w.Write(f.record.Bytes)
			return
		}
		if r.URL.Path == ManifestPath {
			http.Redirect(w, r, target.URL, http.StatusFound)
			return
		}
		http.Error(w, "provider-token-secret", http.StatusForbidden)
	}))
	defer s.Close()
	c, err := NewClient(s.URL, s.Client())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = c.Descriptor(context.Background(), "sha256:"+strings.Repeat("b", 64)); !errors.Is(err, ErrInvalid) {
		t.Fatal("wrong descriptor approved", err)
	}
	if _, err = c.Manifest(context.Background()); err == nil || redirected.Load() != 0 {
		t.Fatal("redirect followed", err)
	}
	if _, err = c.Observed(context.Background()); err == nil || strings.Contains(err.Error(), "secret") {
		t.Fatal("remote diagnostic escaped", err)
	}
	for _, url := range []string{s.URL + "/path", "http://user:password@localhost", "file:///tmp/x", s.URL + "?x=y"} {
		if _, err = NewClient(url, nil); err == nil {
			t.Fatal("unsafe origin", url)
		}
	}
}

func TestSDKBoundsResponseAndCancellation(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, strings.Repeat(" ", descriptor.MaxBytes+1))
	}))
	defer s.Close()
	c, err := NewClient(s.URL, s.Client())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = c.Observed(context.Background()); !errors.Is(err, ErrInvalid) {
		t.Fatal("oversized frame admitted", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err = c.Observed(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
