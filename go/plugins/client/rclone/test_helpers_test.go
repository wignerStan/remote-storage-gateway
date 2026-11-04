package rcloneclient

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wignerStan/infra/go/code/schema"
	port "github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/client"
	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/descriptor"
	"golang.org/x/net/webdav"
)

const testBuildDigest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
const testPassword = "fixture-password-not-a-live-secret"

type secretLease struct {
	value  []byte
	closes atomic.Int32
	reads  atomic.Int32
}

func (s *secretLease) Bytes() []byte { s.reads.Add(1); return s.value }
func (s *secretLease) Close()        { s.closes.Add(1); clear(s.value) }

type resolverFunc func(context.Context, string) (port.Secret, error)

func (f resolverFunc) Resolve(ctx context.Context, name string) (port.Secret, error) {
	return f(ctx, name)
}

// The entry package owns published schema assembly and has its own tests. These
// same-package runtime tests inject a typed fixture without importing that entry
// (which would introduce an import cycle).
func fixtureSchema[T any](t *testing.T, backend string) json.RawMessage {
	t.Helper()
	document, err := schema.Derive[T](schema.Identity{ID: PluginID + "/configuration/" + backend, Version: "1", Owner: PluginID})
	if err != nil {
		t.Fatal(err)
	}
	return mustJSON(t, document.Schema)
}

func testFactory(t *testing.T) *Factory {
	t.Helper()
	capabilities := []string{"stat", "list", "open", "put", "delete", "range", "unknown-size-put", "list-pagination"}
	return NewFactory(descriptor.Descriptor{Schema: descriptor.Schema, PluginID: PluginID, PluginVersion: "0.1.0-alpha.1", Contract: port.Contract, Runtime: "go", BuildDigest: testBuildDigest, Implementations: []descriptor.Implementation{
		{ID: "s3", ConfigSchema: fixtureSchema[S3Configuration](t, "s3"), Capabilities: append([]string(nil), capabilities...)},
		{ID: "webdav", ConfigSchema: fixtureSchema[WebDAVConfiguration](t, "webdav"), Capabilities: []string{"stat", "list", "open", "put", "delete", "range", "list-pagination"}},
		{ID: "webdav-streaming", ConfigSchema: fixtureSchema[WebDAVConfiguration](t, "webdav-streaming"), Capabilities: append([]string(nil), capabilities...)},
		{ID: "drive", ConfigSchema: fixtureSchema[DriveConfiguration](t, "drive"), Capabilities: append([]string(nil), capabilities...)},
	}})
}
func passwordResolver() port.SecretResolver {
	return resolverFunc(func(_ context.Context, name string) (port.Secret, error) {
		if name != "password" {
			return nil, port.ErrNotFound
		}
		return &secretLease{value: []byte(testPassword)}, nil
	})
}

func closeClient(t *testing.T, c port.Client) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := c.Close(ctx); err != nil {
		t.Errorf("client cleanup: %v", err)
	}
}
func waitSignal(t *testing.T, ch <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(3 * time.Second):
		t.Fatalf("%s did not occur", name)
	}
}

type observedBody struct {
	io.ReadCloser
	once    *sync.Once
	entered chan struct{}
}

func (r observedBody) Read(p []byte) (int, error) {
	n, e := r.ReadCloser.Read(p)
	if n > 0 {
		r.once.Do(func() { close(r.entered) })
	}
	return n, e
}

type davFixture struct {
	server      *httptest.Server
	root        string
	requests    atomic.Int32
	putBytes    chan struct{}
	putOnce     sync.Once
	blocked     chan struct{}
	blockOnce   sync.Once
	canceled    chan struct{}
	cancelOnce  sync.Once
	release     chan struct{}
	releaseOnce sync.Once
}

func newDAV(t *testing.T) *davFixture {
	t.Helper()
	f := &davFixture{root: t.TempDir(), putBytes: make(chan struct{}), blocked: make(chan struct{}), canceled: make(chan struct{}), release: make(chan struct{})}
	handler := &webdav.Handler{Prefix: "/dav", FileSystem: webdav.Dir(f.root), LockSystem: webdav.NewMemLS()}
	f.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.requests.Add(1)
		user, password, ok := r.BasicAuth()
		if !ok || user != "fixture" || password != testPassword {
			w.Header().Set("WWW-Authenticate", `Basic realm="fixture"`)
			w.WriteHeader(401)
			return
		}
		if r.URL.Path == "/dav/provider-error" {
			http.Error(w, "private-upstream-diagnostic-secret", 500)
			return
		}
		if r.Method == http.MethodGet && r.URL.Path == "/dav/stalled" {
			w.Header().Set("Content-Length", "65536")
			w.WriteHeader(200)
			w.(http.Flusher).Flush()
			f.blockOnce.Do(func() { close(f.blocked) })
			select {
			case <-r.Context().Done():
				f.cancelOnce.Do(func() { close(f.canceled) })
			case <-f.release:
				_, _ = io.CopyN(w, zeroReader{}, 65536)
			}
			return
		}
		if r.Method == http.MethodPut {
			r.Body = observedBody{r.Body, &f.putOnce, f.putBytes}
		}
		handler.ServeHTTP(w, r)
	}))
	t.Cleanup(func() { f.releaseOnce.Do(func() { close(f.release) }); f.server.Close() })
	return f
}
func (f *davFixture) URL() string { return f.server.URL + "/dav/" }
func (f *davFixture) unblock()    { f.releaseOnce.Do(func() { close(f.release) }) }
func (f *davFixture) write(t *testing.T, name string, data []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(filepath.Join(f.root, name)), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(f.root, name), data, 0600); err != nil {
		t.Fatal(err)
	}
}

type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) { clear(p); return len(p), nil }
func davClient(t *testing.T, f *davFixture) *Client {
	return davClientAs(t, f, "webdav")
}
func davClientAs(t *testing.T, f *davFixture, implementation string) *Client {
	t.Helper()
	input := WebDAVPublicConfig{URL: f.URL(), Username: "fixture"}
	raw := mustJSON(t, input)
	candidate, err := testFactory(t).Prepare(context.Background(), port.PluginConfig{ImplementationID: implementation, Public: raw, Secrets: passwordResolver()})
	if err != nil {
		t.Fatal(err)
	}
	if err = candidate.Validate(context.Background()); err != nil {
		_ = candidate.Abort(context.Background())
		t.Fatal(err)
	}
	published, err := candidate.Publish()
	if err != nil {
		t.Fatal(err)
	}
	c, ok := published.(*Client)
	if !ok {
		t.Fatalf("unexpected client %T", published)
	}
	t.Cleanup(func() { f.unblock(); closeClient(t, c) })
	return c
}
