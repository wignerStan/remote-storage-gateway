package rcloneclient

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	port "github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/client"
)

func TestDirectStreamingWebDAVReachesRemoteBeforeEOF(t *testing.T) {
	f := newDAV(t)
	client := davClientAs(t, f, "webdav-streaming")
	input, producer := io.Pipe()
	defer input.Close()
	defer producer.Close()
	completed := make(chan error, 1)
	go func() {
		result, err := client.Put(context.Background(), port.PutRequest{Path: "live", Body: input, Size: -1})
		if err == nil && result.Info.Size != 9 {
			err = errors.New("remote size did not match streamed input")
		}
		completed <- err
	}()
	if _, err := io.WriteString(producer, "live-"); err != nil {
		t.Fatal(err)
	}
	waitSignal(t, f.putBytes, "upload reached authenticated DAV before EOF")
	select {
	case err := <-completed:
		t.Fatal("stream was buffered or completed before EOF", err)
	default:
	}
	if _, err := io.WriteString(producer, "body"); err != nil {
		t.Fatal(err)
	}
	_ = producer.Close()
	if err := <-completed; err != nil {
		t.Fatal(err)
	}
	plan, err := client.Open(context.Background(), port.OpenRequest{Path: "live", Offset: 5, Length: 4})
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(plan.Body)
	_ = plan.Body.Close()
	if err != nil || string(data) != "body" || plan.Info.Size != 9 {
		t.Fatalf("ordinary range read: %q %+v %v", data, plan.Info, err)
	}
	if err := client.Delete(context.Background(), port.ObjectRef{Path: "live"}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Stat(context.Background(), port.ObjectRef{Path: "live"}); !errors.Is(err, port.ErrNotFound) {
		t.Fatal("ordinary deletion did not reach provider", err)
	}
}

func TestWorkerCursorScopeAdmissionBeforeProvider(t *testing.T) {
	f := newDAV(t)
	f.write(t, "alpha", []byte("a"))
	client := davClient(t, f)
	before := f.requests.Load()
	for _, request := range []port.ListRequest{
		{Cursor: strings.Repeat("A", base64.RawURLEncoding.EncodedLen(4096)+1)},
		{Cursor: base64.RawURLEncoding.EncodeToString([]byte("sub/alpha"))},
		{Cursor: base64.RawURLEncoding.EncodeToString([]byte("../alpha"))},
		{Cursor: base64.RawURLEncoding.EncodeToString([]byte("/alpha"))},
		{Cursor: base64.RawURLEncoding.EncodeToString([]byte("a\x00b"))},
		{Path: "../outside"},
	} {
		if _, err := client.List(context.Background(), request); !errors.Is(err, port.ErrInvalid) {
			t.Fatalf("unsafe or wrong-directory cursor admitted: %+v %v", request, err)
		}
	}
	if f.requests.Load() != before {
		t.Fatal("invalid cursor acquired provider work")
	}
}

func gatewayArchive(t *testing.T) []byte {
	t.Helper()
	var buffer bytes.Buffer
	w := zip.NewWriter(&buffer)
	for _, entry := range []struct{ name, body string }{{"alpha", "first"}, {"nested/beta", "second"}, {"zeta", "last"}} {
		file, err := w.Create(entry.name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := io.WriteString(file, entry.body); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buffer.Bytes()
}

func TestArchiveWebDAVBindingPagingRangeAndReadOnly(t *testing.T) {
	f := newDAV(t)
	f.write(t, "pack.zip", gatewayArchive(t))
	ctx, cancel := context.WithCancel(context.Background())
	input := WebDAVArchivePublicConfig{WebDAVPublicConfig: WebDAVPublicConfig{URL: f.URL(), Username: "fixture"}, ArchivePath: "pack.zip"}
	candidate, err := testFactory(t).Prepare(ctx, port.PluginConfig{ImplementationID: "archive-webdav", Public: mustJSON(t, input), Secrets: passwordResolver()})
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	cancel() // Successful preparation owns a generation, not this request.
	if err := candidate.Validate(context.Background()); err != nil {
		_ = candidate.Abort(context.Background())
		t.Fatal(err)
	}
	client, err := candidate.Publish()
	if err != nil {
		t.Fatal(err)
	}
	defer closeClient(t, client)
	var cursor string
	for _, expected := range []string{"/alpha", "/nested", "/zeta"} {
		page, err := client.List(context.Background(), port.ListRequest{Path: "/", Limit: 1, Cursor: cursor})
		if err != nil || len(page.Objects) != 1 || page.Objects[0].Path != expected {
			t.Fatalf("archive page %s: %+v %v", expected, page, err)
		}
		cursor = page.NextCursor
	}
	if cursor != "" {
		t.Fatal("archive final page has another cursor")
	}
	directory, err := client.Stat(context.Background(), port.ObjectRef{Path: "nested"})
	if err != nil || !directory.IsDir {
		t.Fatal("archive implicit directory", directory, err)
	}
	plan, err := client.Open(context.Background(), port.OpenRequest{Path: "nested/beta", Offset: 1, Length: 3})
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(plan.Body)
	_ = plan.Body.Close()
	if err != nil || string(data) != "eco" || plan.Info.Size != 6 {
		t.Fatalf("archive range: %q %+v %v", data, plan.Info, err)
	}
	before := f.requests.Load()
	body := &countReader{}
	if _, err := client.Put(context.Background(), port.PutRequest{Path: "alpha", Size: -1, Body: body}); !errors.Is(err, port.ErrUnsupported) || body.reads.Load() != 0 {
		t.Fatal("archive write consumed data", err)
	}
	if err := client.Delete(context.Background(), port.ObjectRef{Path: "alpha"}); !errors.Is(err, port.ErrUnsupported) {
		t.Fatal("archive delete was admitted", err)
	}
	if f.requests.Load() != before {
		t.Fatal("archive mutation reached backing provider")
	}
}

func TestInvalidArchiveSelectionAndDescriptorResolveNoSecrets(t *testing.T) {
	var resolves atomic.Int32
	secrets := resolverFunc(func(context.Context, string) (port.Secret, error) { resolves.Add(1); return nil, port.ErrNotFound })
	factory := testFactory(t)
	for _, name := range []string{"", "../outside", "a/../pack.zip", "a\\pack.zip", "a:pack.zip"} {
		public := WebDAVArchivePublicConfig{WebDAVPublicConfig: WebDAVPublicConfig{URL: "https://fixture.invalid/dav/"}, ArchivePath: name}
		candidate, err := factory.Prepare(context.Background(), port.PluginConfig{ImplementationID: "archive-webdav", Public: mustJSON(t, public), Secrets: secrets})
		if !errors.Is(err, port.ErrInvalid) || candidate != nil {
			t.Fatal("invalid archive selection constructed backend", name, err)
		}
	}
	value := factory.Descriptor()
	for i := range value.Implementations {
		if value.Implementations[i].ID == "webdav" {
			value.Implementations[i].ConfigSchema = json.RawMessage(`{"type":"object","properties":{"url":{"type":"string"}},"required":["url"],"additionalProperties":false}`)
		}
	}
	_, err := NewFactory(value).Prepare(context.Background(), port.PluginConfig{ImplementationID: "webdav", Public: []byte(`{"url":"https://fixture.invalid/dav/"}`), Secrets: secrets})
	if !errors.Is(err, port.ErrInvalid) || resolves.Load() != 0 {
		t.Fatal("descriptor/public admission resolved undeclared secrets", err, resolves.Load())
	}
}

func TestSourceArchiveWebDAVPreservesTypedModesAndLinkData(t *testing.T) {
	testSourceArchive(t, testFactory(t))
}

func testSourceArchive(t *testing.T, factory port.Factory) {
	t.Helper()
	var buffer bytes.Buffer
	w := zip.NewWriter(&buffer)
	for _, entry := range []struct {
		name, body string
		mode       os.FileMode
	}{
		{"src/tool", "#!/bin/sh\necho source\n", 0755},
		{"run", "src/tool", os.ModeSymlink | 0777},
	} {
		header := &zip.FileHeader{Name: entry.name, Method: zip.Deflate}
		header.SetMode(entry.mode)
		member, err := w.CreateHeader(header)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = io.WriteString(member, entry.body); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	f := newDAV(t)
	f.write(t, "source-v1.zip", buffer.Bytes())
	ctx := context.Background()
	input := WebDAVArchivePublicConfig{WebDAVPublicConfig: WebDAVPublicConfig{URL: f.URL(), Username: "fixture"}, ArchivePath: "source-v1.zip"}
	// Selecting an ordinary archive never silently enables source link handling.
	if candidate, err := factory.Prepare(ctx, port.PluginConfig{ImplementationID: "archive-webdav", Public: mustJSON(t, input), Secrets: passwordResolver()}); err == nil {
		_ = candidate.Abort(ctx)
		t.Fatal("ordinary archive admitted a source symlink")
	}
	input.SourceView = true
	candidate, err := factory.Prepare(ctx, port.PluginConfig{ImplementationID: "archive-webdav", Public: mustJSON(t, input), Secrets: passwordResolver()})
	if err != nil {
		t.Fatal(err)
	}
	if err := candidate.Validate(ctx); err != nil {
		_ = candidate.Abort(ctx)
		t.Fatal(err)
	}
	client, err := candidate.Publish()
	if err != nil {
		t.Fatal(err)
	}
	defer closeClient(t, client)
	for name, expected := range map[string]os.FileMode{"/": os.ModeDir | 0555, "/src": os.ModeDir | 0555, "/src/tool": 0755, "/run": os.ModeSymlink | 0777} {
		entry, err := client.Stat(ctx, port.ObjectRef{Path: name})
		if err != nil || entry.Mode == nil || *entry.Mode != uint32(expected) || !port.ValidFileMode(*entry.Mode, entry.IsDir) {
			t.Fatalf("typed source stat %s: %+v %v", name, entry, err)
		}
	}
	page, err := client.List(ctx, port.ListRequest{Path: "/", Limit: 1})
	if err != nil || len(page.Objects) != 1 || page.Objects[0].Path != "/run" || page.Objects[0].Mode == nil || *page.Objects[0].Mode != uint32(os.ModeSymlink|0777) {
		t.Fatal("source list lost link type", page, err)
	}
	plan, err := client.Open(ctx, port.OpenRequest{Path: "/run", Offset: 0, Length: -1})
	if err != nil {
		t.Fatal(err)
	}
	body, readErr := io.ReadAll(plan.Body)
	closeErr := plan.Body.Close()
	if readErr != nil || closeErr != nil || string(body) != "src/tool" || plan.Info.Mode == nil || *plan.Info.Mode != uint32(os.ModeSymlink|0777) {
		t.Fatalf("source link bytes or mode: %q %+v %v %v", body, plan.Info, readErr, closeErr)
	}
}

func TestCandidateAbortCancelsConcurrentValidation(t *testing.T) {
	entered := make(chan struct{})
	canceled := make(chan struct{})
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Drain PROPFIND's request body so net/http can observe the client's
		// disconnect while this handler deliberately withholds the response.
		_, _ = io.Copy(io.Discard, r.Body)
		close(entered)
		select {
		case <-r.Context().Done():
			close(canceled)
		case <-release:
		}
	}))
	defer server.Close()
	defer close(release)
	candidate, err := testFactory(t).Prepare(context.Background(), port.PluginConfig{ImplementationID: "webdav", Public: mustJSON(t, WebDAVPublicConfig{URL: server.URL + "/"})})
	if err != nil {
		t.Fatal(err)
	}
	validated := make(chan error, 1)
	go func() { validated <- candidate.Validate(context.Background()) }()
	waitSignal(t, entered, "remote validation")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := candidate.Abort(ctx); err != nil {
		t.Fatal("Abort could not cancel in-flight Validate", err)
	}
	waitSignal(t, canceled, "aborted validation request cancellation")
	if err := <-validated; !errors.Is(err, context.Canceled) {
		t.Fatal("validation did not observe abort", err)
	}
	if client, err := candidate.Publish(); !errors.Is(err, port.ErrInvalid) || client != nil {
		t.Fatal("aborted candidate became publishable", err)
	}
}
