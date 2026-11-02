package rcloneclient

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
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
	if record.Descriptor.PluginID != PluginID || len(record.Descriptor.Implementations) != 4 {
		t.Fatal("missing declared backend inventory")
	}
	if f.requests.Load() != 0 {
		t.Fatal("passive descriptor constructed remote backend")
	}
	expected := map[string]map[string]bool{"s3": {"access_key_id": true, "secret_access_key": true, "session_token": false}, "webdav": {"password": false}, "webdav-streaming": {"password": false}, "drive": {"token": true, "client_secret": false}}
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

	t.Run("materialized-read-drain", func(t *testing.T) {
		cachedRemote := newDAV(t)
		cachedRemote.write(t, "stalled", make([]byte, 65536))
		cacheRoot := t.TempDir()
		configuration := WebDAVPublicConfig{URL: cachedRemote.URL(), Username: "fixture", Materialization: &MaterializationConfig{Directory: cacheRoot, MaxBytes: 1 << 20, MaxAgeSeconds: 60}}
		candidate, err := factory.Prepare(context.Background(), port.PluginConfig{ImplementationID: "webdav", Public: mustJSON(t, configuration), Secrets: passwordResolver()})
		if err != nil {
			t.Fatal(err)
		}
		if err = candidate.Validate(context.Background()); err != nil {
			_ = candidate.Abort(context.Background())
			t.Fatal(err)
		}
		cached, err := candidate.Publish()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { cachedRemote.unblock(); closeClient(t, cached) })
		plan, err := cached.Open(context.Background(), port.OpenRequest{Path: "stalled", Length: -1})
		if err != nil {
			t.Fatal(err)
		}
		readDone := make(chan error, 1)
		go func() { _, err := plan.Body.Read(make([]byte, 1)); readDone <- err }()
		waitSignal(t, cachedRemote.blocked, "native VFS remote GET")
		for _, namespace := range []string{"vfs", "vfsMeta"} {
			paths, err := filepath.Glob(filepath.Join(cacheRoot, "read-cache-*", namespace))
			if err != nil || len(paths) != 1 {
				t.Fatalf("native materialization silently bypassed %s cache: %v %v", namespace, paths, err)
			}
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		err = cached.Close(ctx)
		if err != nil {
			cachedRemote.unblock()
			t.Errorf("native VFS close failed to drain: %v", err)
		} else {
			waitSignal(t, cachedRemote.canceled, "native VFS request cancellation")
		}
		cachedRemote.unblock()
		select {
		case <-readDone:
		case <-time.After(3 * time.Second):
			t.Fatal("native cached read did not settle")
		}
		_ = plan.Body.Close()
		entries, err := os.ReadDir(cacheRoot)
		if err != nil || len(entries) != 0 {
			t.Fatalf("native generation retained cache after Close: %v %v", entries, err)
		}
	})
	t.Run("materialized-request-cancel-keeps-peer", func(t *testing.T) {
		f := newDAV(t)
		f.write(t, "stalled", make([]byte, 65536))
		configuration := WebDAVPublicConfig{URL: f.URL(), Username: "fixture", Materialization: &MaterializationConfig{Directory: t.TempDir(), MaxBytes: 1 << 20, MaxAgeSeconds: 60}}
		candidate, err := factory.Prepare(context.Background(), port.PluginConfig{ImplementationID: "webdav", Public: mustJSON(t, configuration), Secrets: passwordResolver()})
		if err != nil {
			t.Fatal(err)
		}
		if err = candidate.Validate(context.Background()); err != nil {
			_ = candidate.Abort(context.Background())
			t.Fatal(err)
		}
		cached, err := candidate.Publish()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { f.unblock(); closeClient(t, cached) })
		assertCachedRequestCancellationKeepsPeer(t, cached, f)
	})
	t.Run("materialized-cancel-last-handle", func(t *testing.T) {
		f := newDAV(t)
		f.write(t, "stalled", make([]byte, 65536))
		configuration := WebDAVPublicConfig{URL: f.URL(), Username: "fixture", Materialization: &MaterializationConfig{Directory: t.TempDir(), MaxBytes: 1 << 20, MaxAgeSeconds: 60}}
		candidate, err := factory.Prepare(context.Background(), port.PluginConfig{ImplementationID: "webdav", Public: mustJSON(t, configuration), Secrets: passwordResolver()})
		if err != nil {
			t.Fatal(err)
		}
		if err = candidate.Validate(context.Background()); err != nil {
			_ = candidate.Abort(context.Background())
			t.Fatal(err)
		}
		cached, err := candidate.Publish()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { f.unblock(); closeClient(t, cached) })
		assertCachedLastHandleCancellation(t, cached, f)
	})
}

func TestWorkerPagingAndErrorMapping(t *testing.T) {
	f := newDAV(t)
	for _, name := range []string{"alpha", "beta", "gamma"} {
		f.write(t, name, []byte(name))
	}
	c := davClient(t, f, nil)
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
	c := davClient(t, f, nil)
	body := &countReader{}
	_, err := c.Put(context.Background(), port.PutRequest{Path: "unknown", Body: body, Size: -1})
	if !errors.Is(err, port.ErrUnsupported) || body.reads.Load() != 0 {
		t.Fatalf("conservative implementation consumed unsupported body: %v", err)
	}
}

type countReader struct{ reads atomic.Int32 }

func (r *countReader) Read([]byte) (int, error) { r.reads.Add(1); return 0, io.EOF }

func assertCachedRequestCancellationKeepsPeer(t *testing.T, c port.Client, f *davFixture) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	first, err := c.Open(ctx, port.OpenRequest{Path: "stalled", Length: -1})
	if err != nil {
		t.Fatal(err)
	}
	// Nonzero seek opens the peer's VFS cache item synchronously, ensuring the
	// canceled handle is never the only opened owner even under slow scheduling.
	peer, err := c.Open(context.Background(), port.OpenRequest{Path: "stalled", Offset: 1, Length: -1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeClient(t, c); _ = first.Body.Close(); _ = peer.Body.Close() })
	firstDone, peerDone := make(chan error, 1), make(chan error, 1)
	go func() { _, err := first.Body.Read(make([]byte, 1)); firstDone <- err }()
	go func() { _, err := peer.Body.Read(make([]byte, 1)); peerDone <- err }()
	waitSignal(t, f.blocked, "shared cached remote GET")
	cancel()
	select {
	case err = <-firstDone:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("canceled cached read classification: %v", err)
		}
	case <-time.After(time.Second):
		t.Error("cached request cancellation did not release its caller Read")
		// Retiring the generation is rescue cleanup only, never the behavior
		// under test: a single caller must not cancel the peer's shared fetch.
		closeClient(t, c)
		select {
		case <-firstDone:
		case <-time.After(time.Second):
			t.Fatal("rescue failed to join first read")
		}
		select {
		case <-peerDone:
		case <-time.After(time.Second):
			t.Fatal("rescue failed to join peer read")
		}
		return
	}
	closed := make(chan error, 1)
	go func() { closed <- first.Body.Close() }()
	select {
	case err = <-closed:
		if err != nil {
			t.Errorf("canceled handle Close: %v", err)
		}
	case <-time.After(time.Second):
		closeClient(t, c)
		t.Fatal("canceled handle Close blocked on peer fetch")
	}
	select {
	case err = <-peerDone:
		t.Fatalf("canceling one handle terminated peer read: %v", err)
	default:
	}
	select {
	case <-f.canceled:
		t.Fatal("canceling one handle canceled shared backend request")
	default:
	}
	f.unblock()
	select {
	case err = <-peerDone:
		if err != nil {
			t.Fatalf("peer did not finish shared fetch: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("peer read failed to finish")
	}
}

func assertCachedLastHandleCancellation(t *testing.T, c port.Client, f *davFixture) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	plan, err := c.Open(ctx, port.OpenRequest{Path: "stalled", Length: -1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeClient(t, c); _ = plan.Body.Close() })
	readDone := make(chan error, 1)
	go func() { _, err := plan.Body.Read(make([]byte, 1)); readDone <- err }()
	waitSignal(t, f.blocked, "last cached handle remote read")
	cancel()
	select {
	case err = <-readDone:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("last cached read cancellation: %v", err)
		}
	case <-time.After(time.Second):
		closeClient(t, c)
		t.Fatal("last cached caller Read did not return")
	}
	closed := make(chan error, 1)
	go func() { closed <- plan.Body.Close() }()
	select {
	case err = <-closed:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("last cached Close: %v", err)
		}
	case <-time.After(time.Second):
		closeClient(t, c)
		t.Fatal("last cached Close did not cancel/join background input")
	}
	if _, err = c.Stat(context.Background(), port.ObjectRef{Path: "stalled"}); err != nil {
		t.Fatalf("closing one stream retired the generation: %v", err)
	}
	// The generation remains usable after the last stream closes. Retiring it
	// must join cache-owned work before removing the cache directory.
	closeCtx, closeCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer closeCancel()
	if err = c.Close(closeCtx); err != nil {
		f.unblock()
		t.Fatal("generation failed to join retained cache prefetch", err)
	}
	waitSignal(t, f.canceled, "generation canceled retained cache input")
}

func TestNoDeferredCacheWorkAfterGenerationClose(t *testing.T) {
	const helper = "REMOTE_GATEWAY_CACHE_CLOSE_PROBE"
	if os.Getenv("REMOTE_GATEWAY_RCLONE_PLUGIN") == "" {
		t.Skip("set REMOTE_GATEWAY_RCLONE_PLUGIN to validate native generation teardown")
	}
	if os.Getenv(helper) != "1" {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestNoDeferredCacheWorkAfterGenerationClose$", "-test.v")
		command.Env = append(os.Environ(), helper+"=1")
		output, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("isolated generation-close probe: %v\n%s", err, output)
		}
		for _, diagnostic := range []string{"close after grace period failed", "failed to write metadata"} {
			if bytes.Contains(output, []byte(diagnostic)) {
				t.Fatalf("deferred cache work outlived generation:\n%s", output)
			}
		}
		return
	}
	factories := map[string]port.Factory{}
	if artifactPath := os.Getenv("REMOTE_GATEWAY_RCLONE_PLUGIN"); artifactPath != "" {
		artifact, err := os.Open(artifactPath)
		if err != nil {
			t.Fatal(err)
		}
		hash := sha256.New()
		_, err = io.Copy(hash, artifact)
		_ = artifact.Close()
		if err != nil {
			t.Fatal(err)
		}
		digest := hex.EncodeToString(hash.Sum(nil))
		record, err := nativeclient.Inspect(context.Background(), artifactPath, digest, PluginID, "go")
		if err != nil {
			t.Fatal(err)
		}
		factory, err := nativeclient.NewFactory(context.Background(), nativeclient.Artifact{Path: artifactPath, SHA256: digest, DescriptorDigest: record.Digest, PluginID: PluginID, Language: "go"})
		if err != nil {
			t.Fatal(err)
		}
		factories["native"] = factory
	}
	var roots []string
	cacheParent := t.TempDir()
	for name, factory := range factories {
		t.Run(name, func(t *testing.T) {
			f := newDAV(t)
			f.write(t, "cached", []byte("cache-payload"))
			root := filepath.Join(cacheParent, name)
			if err := os.Mkdir(root, 0700); err != nil {
				t.Fatal(err)
			}
			roots = append(roots, root)
			configuration := WebDAVPublicConfig{URL: f.URL(), Username: "fixture", Materialization: &MaterializationConfig{Directory: root, MaxBytes: 1 << 20, MaxAgeSeconds: 60}}
			candidate, err := factory.Prepare(context.Background(), port.PluginConfig{ImplementationID: "webdav", Public: mustJSON(t, configuration), Secrets: passwordResolver()})
			if err != nil {
				t.Fatal(err)
			}
			if err = candidate.Validate(context.Background()); err != nil {
				_ = candidate.Abort(context.Background())
				t.Fatal(err)
			}
			c, err := candidate.Publish()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { closeClient(t, c) })
			plan, err := c.Open(context.Background(), port.OpenRequest{Path: "cached", Length: -1})
			if err != nil {
				t.Fatal(err)
			}
			data, err := io.ReadAll(plan.Body)
			if err != nil || string(data) != "cache-payload" {
				t.Fatal("cache read", err)
			}
			if err = plan.Body.Close(); err != nil {
				t.Fatal(err)
			}
			closeClient(t, c)
			entries, err := os.ReadDir(root)
			if err != nil || len(entries) != 0 {
				t.Errorf("generation retained cache directory: %v %v", entries, err)
			}
			// A deleted pathname alone does not prove release: an open file can
			// survive unlink. Inspect actual kernel descriptors before waiting
			// across the pinned upstream's five-second delayed-close horizon.
			fds, err := os.ReadDir("/proc/self/fd")
			if err != nil {
				t.Fatal(err)
			}
			for _, fd := range fds {
				target, err := os.Readlink(filepath.Join("/proc/self/fd", fd.Name()))
				if err == nil && strings.HasPrefix(target, root+string(os.PathSeparator)) {
					t.Errorf("cache descriptor outlived generation Close: %s", target)
				}
			}
		})
	}
	// This wait is the regression oracle: the old five-second handle-grace
	// callback attempted metadata writes after generation teardown succeeded.
	time.Sleep(5500 * time.Millisecond)
	for _, root := range roots {
		entries, err := os.ReadDir(root)
		if err != nil || len(entries) != 0 {
			t.Errorf("deferred work recreated retired cache content: %v %v", entries, err)
		}
	}
}
