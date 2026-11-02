package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/gin-gonic/gin"
	"github.com/wignerStan/infra/go/code/schema"
	schemaapi "github.com/wignerStan/infra/go/code/schema/api"
	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/client"
	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/descriptor"
	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/descriptor/admission"
	"github.com/wignerStan/network-data-plane-worker/go/management"
	"github.com/wignerStan/network-data-plane-worker/go/pluginruntime"
	"github.com/wignerStan/network-data-plane-worker/go/worker"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

type factory struct {
	record       descriptor.Record
	preparations atomic.Int32
}

func (f *factory) Descriptor() descriptor.Descriptor {
	return f.record.Descriptor
}
func (f *factory) DescriptorRecord() descriptor.Record {
	return f.record
}
func (f *factory) Prepare(context.Context, client.PluginConfig) (client.Candidate, error) {
	f.preparations.Add(1)
	return candidate{}, nil
}

type candidate struct{}

func (candidate) Validate(context.Context) error {
	return nil
}
func (candidate) Abort(context.Context) error {
	return nil
}
func (candidate) Publish() (client.Client, error) {
	return instance{}, nil
}

type instance struct{}

func (instance) Close(context.Context) error {
	return nil
}
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
func (instance) Delete(context.Context, client.ObjectRef) error {
	return client.ErrUnsupported
}

// The fixture is product assembly: the entry supplies typed registrations and
// guards, while the HTTP facility alone constructs and serves the router.
type testHandler struct {
	http.Handler
	entry ManagementEntry
	table *schemaapi.Table
}

func (h *testHandler) Routes() []schemaapi.RouteInfo { return h.table.Routes() }

func setup(t *testing.T) (*testHandler, *pluginruntime.Manager, *factory) {
	t.Helper()
	doc, err := schema.Derive[struct{}](schema.Identity{ID: "worker.test.config", Version: "1", Owner: "network-data-plane-worker"})
	if err != nil {
		t.Fatal(err)
	}
	configSchema, err := json.Marshal(doc.Schema)
	if err != nil {
		t.Fatal(err)
	}
	r, err := admission.Encode(descriptor.Descriptor{Schema: descriptor.Schema, PluginID: "test.client", PluginVersion: "1", Contract: client.Contract, Runtime: "go", BuildDigest: "sha256:" + strings.Repeat("a", 64), Implementations: []descriptor.Implementation{{ID: "test", ConfigSchema: configSchema, Capabilities: []string{"stat"}}}})
	if err != nil {
		t.Fatal(err)
	}
	r, err = admission.Decode(append([]byte(" \n"), r.Bytes...))
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
	t.Cleanup(func() {
		m.Close(context.Background())
	})
	manifest, err := worker.Capabilities(registry)
	if err != nil {
		t.Fatal(err)
	}
	entry, err := NewEntry(Options{Registry: registry, Manager: m, Manifest: manifest, ClientNames: []string{"account"}})
	if err != nil {
		t.Fatal(err)
	}
	table, err := schemaapi.Compose(schemaapi.Config{
		Title: "Worker management test", Version: "1",
		OpenAPIPath: "/v1/openapi", SchemasPath: "/v1/schemas",
		Guard: entry.Guard, Headers: map[string]string{"Cache-Control": "no-store"},
	}, entry.RegisterRoutes)
	if err != nil {
		t.Fatal(err)
	}
	router := gin.New()
	router.RedirectTrailingSlash = false
	router.RedirectFixedPath = false
	router.HandleMethodNotAllowed = true
	if err = schemaapi.MountGin(router, nil, table); err != nil {
		t.Fatal(err)
	}
	return &testHandler{Handler: router, entry: entry, table: table}, m, f
}
func TestSDKPassiveDiscoveryExactDescriptorAndCAS(t *testing.T) {
	h, m, f := setup(t)
	s := httptest.NewServer(h)
	defer s.Close()
	c, err := management.NewClient(s.URL, s.Client())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	manifest, err := c.Manifest(ctx)
	if err != nil || len(manifest.PluginDescriptors) != 1 {
		t.Fatal(manifest, err)
	}
	openAPI, err := c.OpenAPI(ctx)
	if err != nil {
		t.Fatal("generated OpenAPI unavailable", err)
	}
	var document struct {
		OpenAPI    string                                `json:"openapi"`
		Paths      map[string]map[string]json.RawMessage `json:"paths"`
		Components struct {
			Schemas map[string]json.RawMessage `json:"schemas"`
		} `json:"components"`
	}
	if json.Unmarshal(openAPI, &document) != nil || len(document.Paths) != len(h.Routes()) || document.Paths[management.ClientPrefix+"{name}"]["post"] == nil || document.Components.Schemas["ReplaceRequest"] == nil {
		t.Fatal("typed routes and DTOs were not generated into OpenAPI")
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
		var status *management.StatusError
		if !errors.As(err, &status) || status.StatusCode != http.StatusConflict {
			t.Fatal(err)
		}
	}
	state, err = c.Observed(ctx)
	if err != nil || len(state) != 1 || state[0].Version != v {
		t.Fatal(state, err)
	}
	if _, err = c.Replace(ctx, "another", v, selection); !errors.Is(err, management.ErrInvalid) {
		t.Fatal("mismatched route identity accepted", err)
	}
}

func TestTypedEntryDiscoveryCannotMutateApprovalState(t *testing.T) {
	h, _, f := setup(t)
	ctx := context.Background()
	first, err := h.entry.Manifest(ctx, &ManifestRequest{})
	if err != nil {
		t.Fatal(err)
	}
	first.Body.Controls[0] = "tampered"
	first.Body.PluginDescriptors[0].PluginID = "tampered"
	next, err := h.entry.Manifest(ctx, &ManifestRequest{})
	if err != nil || next.Body.Controls[0] == "tampered" || next.Body.PluginDescriptors[0].PluginID == "tampered" {
		t.Fatal("typed response exposed mutable entry approval state", err)
	}
	raw, err := h.entry.Descriptor(ctx, &DescriptorRequest{Digest: f.record.Digest})
	if err != nil {
		t.Fatal(err)
	}
	raw.Body[0] = 'X'
	nextRaw, err := h.entry.Descriptor(ctx, &DescriptorRequest{Digest: f.record.Digest})
	if err != nil || !bytes.Equal(nextRaw.Body, f.record.Bytes) || f.preparations.Load() != 0 {
		t.Fatal("typed descriptor response changed frozen bytes or initialized provider", err)
	}
}

func TestRegisteredRoutesFailClosed(t *testing.T) {
	h, _, _ := setup(t)
	for _, test := range []struct {
		method, path, body string
		status             int
	}{
		{"POST", management.ManifestPath, "{}", 405},
		{"GET", "/v1/object/open", "", 404},
		{"POST", management.ClientPrefix + "unknown", "{}", 404},
		{"POST", management.ClientPrefix + "unknown", `{"expected":`, 404},
		{"POST", management.ClientPrefix + "account", `{"expected":{},"expected":{},"selection":{}}`, 400},
		{"GET", management.ObservedPath + "?target=http://arbitrary.invalid", "", 400},
		{"GET", "/v1/%73tate", "", 400},
		{"POST", management.ClientPrefix + "account", strings.Repeat(" ", descriptor.MaxBytes+1), 413},
	} {
		r := httptest.NewRequest(test.method, test.path, strings.NewReader(test.body))
		r.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != test.status {
			t.Fatalf("%s %s: %d", test.method, test.path, w.Code)
		}
	}
	routes := h.Routes()
	firstPath := routes[0].Path
	routes[0].Path = "tampered"
	if h.Routes()[0].Path != firstPath || !management.IsPath("/v1/unknown") || management.IsPath("/dav/file") {
		t.Fatal("route definitions not isolated")
	}
}
func TestSDKRejectsDigestMismatchRedirectsAndRemoteDiagnostics(t *testing.T) {
	_, _, f := setup(t)
	var redirected atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		redirected.Add(1)
	}))
	defer target.Close()
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, management.DescriptorPrefix) {
			w.Header().Set("Content-Type", "application/json")
			w.Write(f.record.Bytes)
			return
		}
		if r.URL.Path == management.ManifestPath {
			http.Redirect(w, r, target.URL, http.StatusFound)
			return
		}
		http.Error(w, "provider-token-secret", http.StatusForbidden)
	}))
	defer s.Close()
	c, err := management.NewClient(s.URL, s.Client())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = c.Descriptor(context.Background(), "sha256:"+strings.Repeat("b", 64)); !errors.Is(err, management.ErrInvalid) {
		t.Fatal("wrong descriptor approved", err)
	}
	if _, err = c.Manifest(context.Background()); err == nil || redirected.Load() != 0 {
		t.Fatal("redirect followed", err)
	}
	if _, err = c.Observed(context.Background()); err == nil || strings.Contains(err.Error(), "secret") {
		t.Fatal("remote diagnostic escaped", err)
	}
	for _, url := range []string{s.URL + "/path", "http://user:password@localhost", "file:///tmp/x", s.URL + "?x=y"} {
		if _, err = management.NewClient(url, nil); err == nil {
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
	c, err := management.NewClient(s.URL, s.Client())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = c.Observed(context.Background()); !errors.Is(err, management.ErrInvalid) {
		t.Fatal("oversized frame admitted", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err = c.Observed(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
