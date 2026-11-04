package rcloneclient

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	port "github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/client"
	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/descriptor/admission"
	"github.com/wignerStan/network-data-plane-worker/go/nativeclient"
)

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestFactoryInjectedDescriptorAndStrictSecrets(t *testing.T) {
	f := newDAV(t)
	factory := testFactory(t)
	record, err := admission.Encode(factory.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	if record.Descriptor.PluginID != PluginID || len(record.Descriptor.Implementations) != 7 {
		t.Fatal("missing declared backend inventory")
	}
	if f.requests.Load() != 0 {
		t.Fatal("passive descriptor constructed remote backend")
	}
	expected := map[string]map[string]bool{"s3": {"access_key_id": true, "secret_access_key": true, "session_token": false}, "webdav": {"password": false}, "webdav-streaming": {"password": false}, "drive": {"token": true, "client_secret": false}}
	for _, backend := range []string{"s3", "webdav", "drive"} {
		expected["archive-"+backend] = expected[backend]
	}
	for _, impl := range record.Descriptor.Implementations {
		fields := admission.SecretFields(impl)
		if len(fields) != len(expected[impl.ID]) {
			t.Fatalf("%s secret fields: %+v", impl.ID, fields)
		}
		for _, field := range fields {
			required, ok := expected[impl.ID][field.Name]
			if !ok || field.Required != required {
				t.Fatalf("invalid secret semantics: %+v", field)
			}
		}
	}
	var resolves atomic.Int32
	secrets := resolverFunc(func(context.Context, string) (port.Secret, error) { resolves.Add(1); return nil, port.ErrNotFound })
	bad := []string{`{"url":"` + f.URL() + `","unknown":true}`, `{"url":"` + f.URL() + `","password":"inline-secret"}`, `{"url":"` + f.URL() + `","url":"` + f.URL() + `"}`}
	for _, public := range bad {
		if candidate, err := factory.Prepare(context.Background(), port.PluginConfig{ImplementationID: "webdav", Public: []byte(public), Secrets: secrets}); err == nil {
			_ = candidate.Abort(context.Background())
			t.Fatal("unsafe public config admitted")
		}
	}
	if resolves.Load() != 0 || f.requests.Load() != 0 {
		t.Fatal("invalid structure acquired credentials or contacted provider")
	}
	partial := &secretLease{value: []byte("partial-secret")}
	_, err = factory.Prepare(context.Background(), port.PluginConfig{ImplementationID: "webdav", Public: mustJSON(t, WebDAVPublicConfig{URL: f.URL()}), Secrets: resolverFunc(func(context.Context, string) (port.Secret, error) {
		return partial, errors.New("private-resolver-secret")
	})})
	if !errors.Is(err, port.ErrInvalid) || partial.closes.Load() != 1 || partial.reads.Load() != 0 {
		t.Fatalf("failed resolution lost partial secret custody: error=%v closes=%d", err, partial.closes.Load())
	}
	_, err = factory.Prepare(context.Background(), port.PluginConfig{ImplementationID: "s3", Public: []byte(`{"region":"test","bucket":"bucket"}`), Secrets: secrets})
	if !errors.Is(err, port.ErrInvalid) {
		t.Fatal("missing required credentials accepted", err)
	}
}

func TestActualRcloneNativePluginLifecycle(t *testing.T) {
	artifactPath := os.Getenv("REMOTE_GATEWAY_RCLONE_PLUGIN")
	if artifactPath == "" {
		t.Skip("set REMOTE_GATEWAY_RCLONE_PLUGIN to the independently built rclone plugin")
	}
	artifact, err := os.Open(artifactPath)
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.New()
	if _, err = io.Copy(hash, artifact); err != nil {
		t.Fatal(err)
	}
	_ = artifact.Close()
	digest := hex.EncodeToString(hash.Sum(nil))
	f := newDAV(t)
	record, err := nativeclient.Inspect(context.Background(), artifactPath, digest, PluginID, "go")
	if err != nil {
		t.Fatal(err)
	}
	factory, err := nativeclient.NewFactory(context.Background(), nativeclient.Artifact{Path: artifactPath, SHA256: digest, DescriptorDigest: record.Digest, PluginID: PluginID, Language: "go"})
	if err != nil {
		t.Fatal(err)
	}
	if f.requests.Load() != 0 || record.Descriptor.PluginID != PluginID {
		t.Fatal("native passive discovery contacted backend or changed identity")
	}
	t.Run("source-view", func(t *testing.T) { testSourceArchive(t, factory) })
	for _, test := range []struct {
		implementation, public string
		missing                bool
	}{
		{"s3", `{"region":"test","bucket":"bucket"}`, false},
		{"webdav", `{"url":"` + f.URL() + `"}`, false},
		{"webdav", `{"url":"` + f.URL() + `"}`, true},
	} {
		partial := &secretLease{value: []byte("partial-native-secret")}
		candidate, err := factory.Prepare(context.Background(), port.PluginConfig{ImplementationID: test.implementation, Public: []byte(test.public), Secrets: resolverFunc(func(context.Context, string) (port.Secret, error) {
			if test.missing {
				return partial, port.ErrNotFound
			}
			return partial, errors.New("private-native-resolver-error")
		})})
		if candidate != nil {
			_ = candidate.Abort(context.Background())
		}
		if partial.closes.Load() != 1 || partial.reads.Load() != 0 || !test.missing && err == nil {
			t.Fatalf("native resolver failure lost partial secret ownership: closes=%d error=%v", partial.closes.Load(), err)
		}
	}
	candidate, err := factory.Prepare(context.Background(), port.PluginConfig{ImplementationID: "webdav-streaming", Public: mustJSON(t, WebDAVPublicConfig{URL: f.URL(), Username: "fixture"}), Secrets: passwordResolver()})
	if err != nil {
		t.Fatal(err)
	}
	if err = candidate.Validate(context.Background()); err != nil {
		_ = candidate.Abort(context.Background())
		t.Fatal(err)
	}
	client, err := candidate.Publish()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.unblock(); closeClient(t, client) })
	input, producer := io.Pipe()
	defer input.Close()
	defer producer.Close()
	result := make(chan error, 1)
	go func() {
		_, err := client.Put(context.Background(), port.PutRequest{Path: "native", Size: -1, Body: input})
		result <- err
	}()
	if _, err = producer.Write([]byte("native-")); err != nil {
		t.Fatal(err)
	}
	waitSignal(t, f.putBytes, "native plugin upload reached remote before EOF")
	select {
	case err = <-result:
		t.Fatalf("native unknown-size upload completed before EOF: %v", err)
	default:
	}
	if _, err = producer.Write([]byte("stream")); err != nil {
		t.Fatal(err)
	}
	_ = producer.Close()
	if err = <-result; err != nil {
		t.Fatal(err)
	}
	stat, err := client.Stat(context.Background(), port.ObjectRef{Path: "native"})
	if err != nil || stat.Size != 13 {
		t.Fatalf("native stat: %+v %v", stat, err)
	}
	plan, err := client.Open(context.Background(), port.OpenRequest{Path: "native", Offset: 7, Length: 6})
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(plan.Body)
	_ = plan.Body.Close()
	if err != nil || string(data) != "stream" {
		t.Fatalf("native range: %q %v", data, err)
	}
	f.write(t, "stalled", make([]byte, 65536))
	plan, err = client.Open(context.Background(), port.OpenRequest{Path: "stalled", Length: -1})
	if err != nil {
		t.Fatal(err)
	}
	readDone := make(chan error, 1)
	go func() { _, err := plan.Body.Read(make([]byte, 1)); readDone <- err }()
	waitSignal(t, f.blocked, "native read stalled in actual remote")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err = client.Close(ctx); err != nil {
		f.unblock()
		t.Fatal(err)
	}
	waitSignal(t, f.canceled, "native close canceled owned HTTP request")
	select {
	case <-readDone:
	case <-ctx.Done():
		t.Fatal("native close lost admitted body read")
	}
	_ = plan.Body.Close()

}

func TestWorkerPagingAndErrorMapping(t *testing.T) {
	f := newDAV(t)
	for _, name := range []string{"alpha", "beta", "gamma"} {
		f.write(t, name, []byte(name))
	}
	c := davClient(t, f)
	cursor := ""
	for _, expected := range []string{"/alpha", "/beta", "/gamma"} {
		page, err := c.List(context.Background(), port.ListRequest{Path: "/", Limit: 1, Cursor: cursor})
		if err != nil || len(page.Objects) != 1 || page.Objects[0].Path != expected {
			t.Fatalf("Worker page %q: %+v %v", expected, page, err)
		}
		cursor = page.NextCursor
	}
	if cursor != "" {
		t.Fatal("last Worker page retained cursor")
	}
	before := f.requests.Load()
	for _, request := range []port.ListRequest{{Limit: -1}, {Limit: 1001}, {Cursor: "invalid!"}} {
		if _, err := c.List(context.Background(), request); !errors.Is(err, port.ErrInvalid) {
			t.Fatalf("invalid Worker page admitted: %+v %v", request, err)
		}
	}
	if f.requests.Load() != before {
		t.Fatal("invalid Worker cursor/limit contacted provider")
	}
	if _, err := c.Stat(context.Background(), port.ObjectRef{Path: "missing"}); !errors.Is(err, port.ErrNotFound) {
		t.Fatalf("storage not-found was not mapped to Worker error: %v", err)
	}
	if _, err := c.Stat(context.Background(), port.ObjectRef{Path: "provider-error"}); !errors.Is(err, port.ErrProvider) || strings.Contains(err.Error(), "private") {
		t.Fatalf("provider error escaped Worker boundary: %v", err)
	}
	for _, invalid := range []string{"../outside", "/a/../b", "a\\b", "a\x00b"} {
		if _, err := c.Stat(context.Background(), port.ObjectRef{Path: invalid}); !errors.Is(err, port.ErrInvalid) {
			t.Fatalf("invalid storage key lost Worker classification: %q %v", invalid, err)
		}
	}
}

func TestFactoryConservativeWebDAVRejectsUnknownSize(t *testing.T) {
	f := newDAV(t)
	c := davClient(t, f)
	body := &countReader{}
	_, err := c.Put(context.Background(), port.PutRequest{Path: "unknown", Body: body, Size: -1})
	if !errors.Is(err, port.ErrUnsupported) || body.reads.Load() != 0 {
		t.Fatalf("conservative implementation consumed unsupported body: %v", err)
	}
}

type countReader struct{ reads atomic.Int32 }

func (r *countReader) Read([]byte) (int, error) { r.reads.Add(1); return 0, io.EOF }

// Materialization belongs to the independent VFS process. A stale desired state
// must fail before construction, never silently add a local worker cache.
func TestFactoryRejectsInProcessMaterialization(t *testing.T) {
	f := newDAV(t)
	raw := mustJSON(t, map[string]any{"url": f.URL(), "username": "fixture", "materialization": map[string]any{"directory": t.TempDir(), "max_bytes": 1048576, "max_age_seconds": 60}})
	if _, err := testFactory(t).Prepare(context.Background(), port.PluginConfig{ImplementationID: "webdav", Public: raw, Secrets: passwordResolver()}); !errors.Is(err, port.ErrInvalid) {
		t.Fatalf("stale materializer config admitted: %v", err)
	}
}
