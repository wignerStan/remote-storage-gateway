//go:build dataplane_l7

package runtime

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	workerconfig "github.com/wignerStan/remote-storage-gateway/go/contracts/worker"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	infraexec "github.com/wignerStan/infra/go/process/exec"
	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/descriptor"
	"github.com/wignerStan/network-data-plane-worker/go/nativeclient"
	"github.com/wignerStan/network-data-plane-worker/go/pluginruntime"
	"github.com/wignerStan/network-data-plane-worker/go/worker"
	workerbox "github.com/wignerStan/remote-storage-gateway/go/worker"
	"golang.org/x/net/webdav"
)

const password = "integration-password-not-a-live-secret"

type watchedBody struct {
	io.ReadCloser
	once     *sync.Once
	received chan struct{}
}

func (b watchedBody) Read(p []byte) (int, error) {
	n, e := b.ReadCloser.Read(p)
	if n > 0 {
		b.once.Do(func() { close(b.received) })
	}
	return n, e
}

func TestDynamicGatewayStreamingAndGeneration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	_, source, _, _ := runtime.Caller(0)
	root := filepath.Dir(filepath.Dir(filepath.Dir(source)))
	so := filepath.Join(t.TempDir(), "openlist-client.so")
	// The source builder binds this exact patched upstream module in go.work.
	// Build its plugin rather than reaching into the retired editable fork.
	spec := infraexec.NewSpec(filepath.Join(runtime.GOROOT(), "bin", "go"), "build", "-buildmode=c-shared", "-buildvcs=false", "-ldflags", "-X main.buildDigest=sha256:"+strings.Repeat("2", 64), "-o", so, "github.com/OpenListTeam/OpenList/v4/cmd/openlist-client-plugin").WithTimeout(3 * time.Minute).WithEnv(infraexec.EnvInheritAllButRedacted())
	spec.Dir = root
	out, err := infraexec.RunContext(ctx, spec)
	if err != nil {
		t.Fatalf("build independent provider: %v %s%s", err, out.StdoutString(), out.StderrString())
	}
	elf, err := os.ReadFile(so)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(elf)
	artifact := nativeclient.Artifact{Path: so, SHA256: hex.EncodeToString(sum[:]), PluginID: "client.openlist", Language: "go"}
	record, err := nativeclient.Inspect(ctx, artifact.Path, artifact.SHA256, artifact.PluginID, artifact.Language)
	if err != nil {
		t.Fatal(err)
	}
	artifact.DescriptorDigest = record.Digest
	received := make(chan struct{})
	var once sync.Once
	dir := t.TempDir()
	if err = os.Mkdir(filepath.Join(dir, "many"), 0700); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 257; i++ {
		if err = os.WriteFile(filepath.Join(dir, "many", fmt.Sprintf("item-%03d", i)), nil, 0600); err != nil {
			t.Fatal(err)
		}
	}
	dav := &webdav.Handler{FileSystem: webdav.Dir(dir), LockSystem: webdav.NewMemLS()}
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			r.Body = watchedBody{r.Body, &once, received}
			if r.URL.Path == "/rejected" {
				_, _ = io.Copy(io.Discard, r.Body)
				http.Error(w, "test provider failure", http.StatusServiceUnavailable)
				return
			}
		}
		dav.ServeHTTP(w, r)
	}))
	defer backend.Close()
	auth := filepath.Join(t.TempDir(), "auth")
	if err = os.WriteFile(auth, []byte(password), 0600); err != nil {
		t.Fatal(err)
	}
	public, _ := json.Marshal(map[string]string{"address": backend.URL})
	selection := pluginruntime.Selection{Name: "files", PluginID: artifact.PluginID, DescriptorDigest: record.Digest, ImplementationID: "webdav", Public: public}
	config := workerconfig.RuntimeConfig{Listen: "127.0.0.1:0", Username: "operator", AuthSecret: "ingress", Worker: worker.Bootstrap{Schema: worker.BootstrapSchema, Artifacts: []nativeclient.Artifact{artifact}, Clients: []pluginruntime.Selection{selection}, SecretFiles: map[string]string{"ingress": auth}}, Mounts: []workerconfig.Mount{{Name: "files", Client: "files"}, {Name: "readonly", Client: "files", ReadOnly: true}}}

	t.Run("managed-independent-worker", func(t *testing.T) {
		executable := filepath.Join(t.TempDir(), "storage-worker")
		build := infraexec.NewSpec(filepath.Join(runtime.GOROOT(), "bin", "go"), "build", "-tags", "dataplane_l7", "-o", executable, "./cmd/storage-worker").WithTimeout(time.Minute).WithEnv(infraexec.EnvInheritAllButRedacted())
		build.Dir = root
		output, e := infraexec.RunContext(ctx, build)
		if e != nil {
			t.Fatalf("build worker executable: %v %s%s", e, output.StdoutString(), output.StderrString())
		}
		controlDir, e := os.MkdirTemp("/tmp", "trade-real-worker-")
		if e != nil {
			t.Fatal(e)
		}
		defer os.RemoveAll(controlDir)
		managed, e := workerbox.New(workerbox.Options{Executable: executable, Config: workerconfig.ProcessConfig{RuntimeConfig: config, InstanceID: "independent-worker", ControlSocket: filepath.Join(controlDir, "rpc")}, ReadyTimeout: 15 * time.Second, StopTimeout: 3 * time.Second})
		if e != nil {
			t.Fatal(e)
		}
		defer managed.Close(context.Background())
		startup, cancel := context.WithCancel(ctx)
		if e = managed.Start(startup); e != nil {
			cancel()
			t.Fatal(e)
		}
		cancel()
		control, e := managed.Control()
		if e != nil {
			t.Fatal(e)
		}
		info, e := control.Info(ctx)
		if e != nil || !info.Ready || managed.State().PID == os.Getpid() {
			t.Fatal("real independent worker not ready", e)
		}
		recordFromChild, e := control.Descriptor(ctx, record.Digest)
		if e != nil || !bytes.Equal(recordFromChild.Bytes, record.Bytes) {
			t.Fatal("child descriptor exact bytes", e)
		}
		req, e := http.NewRequestWithContext(ctx, "PUT", info.DataEndpoint+"/dav/files/managed-process", strings.NewReader("separate-worker-data"))
		if e != nil {
			t.Fatal(e)
		}
		req.SetBasicAuth("operator", password)
		response, e := http.DefaultClient.Do(req)
		if e != nil {
			t.Fatal(e)
		}
		response.Body.Close()
		if response.StatusCode != 201 {
			t.Fatalf("managed upload status %d", response.StatusCode)
		}
		req, e = http.NewRequestWithContext(ctx, "GET", info.DataEndpoint+"/remote-dav/files/managed-process", nil)
		if e != nil {
			t.Fatal(e)
		}
		req.SetBasicAuth("operator", password)
		req.Header.Set("Range", "bytes=0-7")
		response, e = http.DefaultClient.Do(req)
		if e != nil {
			t.Fatal(e)
		}
		data, e := io.ReadAll(response.Body)
		response.Body.Close()
		if e != nil || response.StatusCode != 206 || string(data) != "separate" {
			t.Fatalf("managed range %d %q %v", response.StatusCode, data, e)
		}
		if e = managed.Close(ctx); e != nil {
			t.Fatal(e)
		}
	})
	bad := config
	bad.Worker.Artifacts = append([]nativeclient.Artifact(nil), config.Worker.Artifacts...)
	bad.Worker.Artifacts[0].DescriptorDigest = "sha256:" + strings.Repeat("0", 64)
	if rejected, e := assemble(ctx, bad); e == nil {
		rejected.Close(ctx)
		t.Fatal("unapproved descriptor admitted")
	}
	rt, err := assemble(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	reserved := config
	reserved.Worker.Clients = append([]pluginruntime.Selection(nil), config.Worker.Clients...)
	reserved.Worker.Clients[0].SecretRefs = map[string]string{"password": "ingress"}
	if rejected, err := assemble(ctx, reserved); err == nil {
		rejected.Close(ctx)
		t.Fatal("ingress credential admitted into provider scope")
	}
	defer func() {
		if e := rt.Close(ctx); e != nil {
			t.Error(e)
		}
	}()
	server := httptest.NewServer(rt.handler)
	defer server.Close()
	call := func(method, path string, body io.Reader, auth bool) *http.Response {
		t.Helper()
		req, e := http.NewRequestWithContext(ctx, method, server.URL+path, body)
		if e != nil {
			t.Fatal(e)
		}
		if auth {
			req.SetBasicAuth("operator", password)
		}
		if method == "POST" && strings.HasPrefix(path, "/v1/") {
			req.Header.Set("Content-Type", "application/json")
		}
		resp, e := server.Client().Do(req)
		if e != nil {
			t.Fatal(e)
		}
		return resp
	}
	expected := rt.Manager.Current("files")
	forbidden := selection
	forbidden.SecretRefs = map[string]string{"password": "ingress"}
	forbiddenPayload, _ := json.Marshal(map[string]any{"expected": expected, "selection": forbidden})
	rejected := call("POST", "/v1/clients/files", bytes.NewReader(forbiddenPayload), true)
	rejected.Body.Close()
	if rejected.StatusCode != http.StatusConflict || rt.Manager.Current("files") != expected {
		t.Fatal("desired state acquired a host credential or changed the active generation")
	}
	for _, path := range []string{"/v1/state", "/v1/manifest", "/v1/descriptors/" + record.Digest, "/dav/files"} {
		resp := call("GET", path, nil, false)
		resp.Body.Close()
		if resp.StatusCode != 401 {
			t.Fatalf("unauthenticated %s status=%d", path, resp.StatusCode)
		}
	}
	health := call("GET", "/healthz", nil, false)
	health.Body.Close()
	if health.StatusCode != 204 {
		t.Fatal("health endpoint unavailable")
	}
	resp := call("GET", "/v1/descriptors/"+record.Digest, nil, true)
	actual, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || !bytes.Equal(actual, record.Bytes) || resp.Header.Get("ETag") != `"`+record.Digest+`"` {
		t.Fatal("descriptor response changed approved exact bytes/digest")
	}
	requestRoot, _ := http.NewRequestWithContext(ctx, "PROPFIND", server.URL+"/dav", nil)
	requestRoot.SetBasicAuth("operator", password)
	requestRoot.Header.Set("Depth", "1")
	rootResponse, err := server.Client().Do(requestRoot)
	if err != nil {
		t.Fatal(err)
	}
	rootListing, _ := io.ReadAll(rootResponse.Body)
	rootResponse.Body.Close()
	if rootResponse.StatusCode != http.StatusMultiStatus || !bytes.Contains(rootListing, []byte("/dav/files/")) || !bytes.Contains(rootListing, []byte("/dav/readonly/")) {
		t.Fatalf("virtual root browsing failed: status=%d body=%s", rootResponse.StatusCode, rootListing)
	}
	requestMany, _ := http.NewRequestWithContext(ctx, "PROPFIND", server.URL+"/dav/files/many", nil)
	requestMany.SetBasicAuth("operator", password)
	requestMany.Header.Set("Depth", "1")
	manyResponse, err := server.Client().Do(requestMany)
	if err != nil {
		t.Fatal(err)
	}
	manyListing, _ := io.ReadAll(manyResponse.Body)
	manyResponse.Body.Close()
	if manyResponse.StatusCode != http.StatusMultiStatus || !bytes.Contains(manyListing, []byte("/dav/files/many/item-256")) || bytes.Count(manyListing, []byte("/dav/files/many/item-")) != 257 {
		t.Fatalf("unpaginated directory over256 failed: status=%d entries=%d", manyResponse.StatusCode, bytes.Count(manyListing, []byte("/dav/files/many/item-")))
	}
	var manifest worker.Manifest
	resp = call("GET", "/v1/manifest", nil, true)
	err = json.NewDecoder(resp.Body).Decode(&manifest)
	resp.Body.Close()
	if err != nil || manifest.Schema != worker.CapabilitySchema || len(manifest.PluginDescriptors) != 1 || manifest.PluginDescriptors[0].DescriptorDigest != record.Digest {
		t.Fatal("manifest did not report approved Worker descriptor", err)
	}

	// Hold the producer after its first chunk. Receiving bytes at the real
	// upstream proves that neither the gateway nor the DSO waits for the full body.
	payload := bytes.Repeat([]byte("0123456789abcdef"), 128<<10)
	reader, writer := io.Pipe()
	defer reader.Close()
	defer writer.Close()
	resume := make(chan struct{})
	var resumeOnce sync.Once
	release := func() { resumeOnce.Do(func() { close(resume) }) }
	defer release()
	go func() {
		_, e := writer.Write(payload[:64<<10])
		if e == nil {
			select {
			case <-resume:
			case <-ctx.Done():
				e = ctx.Err()
			}
		}
		if e == nil {
			_, e = writer.Write(payload[64<<10:])
		}
		writer.CloseWithError(e)
	}()
	request, err := http.NewRequestWithContext(ctx, "PUT", server.URL+"/dav/files/payload", reader)
	if err != nil {
		t.Fatal(err)
	}
	request.SetBasicAuth("operator", password)
	type response struct {
		r *http.Response
		e error
	}
	done := make(chan response, 1)
	go func() { r, e := server.Client().Do(request); done <- response{r, e} }()
	select {
	case <-received:
	case result := <-done:
		if result.r != nil {
			result.r.Body.Close()
		}
		t.Fatalf("upload finished before producer: %v", result.e)
	case <-time.After(10 * time.Second):
		t.Fatal("upstream never received initial body chunk")
	}
	original := rt.Manager.Current("files")
	update := func(expected pluginruntime.Version) *http.Response {
		t.Helper()
		b, _ := json.Marshal(struct {
			Expected  pluginruntime.Version   `json:"expected"`
			Selection pluginruntime.Selection `json:"selection"`
		}{expected, selection})
		return call("POST", "/v1/clients/files", bytes.NewReader(b), true)
	}
	resp = update(original)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatal("replacement rejected", resp.StatusCode)
	}
	current := rt.Manager.Current("files")
	if current.Generation <= original.Generation || current.Incarnation != original.Incarnation {
		t.Fatal("replacement did not advance generation")
	}
	observed := rt.Manager.Observed()
	if len(observed) != 1 || observed[0].Draining != 1 {
		t.Fatalf("active upload did not pin retired generation: %+v", observed)
	}
	resp = update(original)
	resp.Body.Close()
	if resp.StatusCode != 409 || rt.Manager.Current("files") != current {
		t.Fatal("stale expected generation accepted")
	}
	release()
	select {
	case result := <-done:
		if result.e != nil {
			t.Fatal(result.e)
		}
		body, _ := io.ReadAll(result.r.Body)
		result.r.Body.Close()
		if result.r.StatusCode != 201 {
			t.Fatalf("PUT status=%d body=%s", result.r.StatusCode, body)
		}
	case <-ctx.Done():
		t.Fatal("streaming PUT did not complete")
	}
	persisted, err := os.ReadFile(filepath.Join(dir, "payload"))
	if err != nil || !bytes.Equal(payload, persisted) {
		t.Fatal("upstream payload differs", err)
	}
	request, _ = http.NewRequestWithContext(ctx, "GET", server.URL+"/dav/files/payload", nil)
	request.SetBasicAuth("operator", password)
	request.Header.Set("Range", "bytes=19-50")
	resp, err = server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	actual, err = io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil || resp.StatusCode != 206 || !bytes.Equal(actual, payload[19:51]) {
		t.Fatalf("range status=%d body=%q error=%v", resp.StatusCode, actual, err)
	}
	resp = call("PUT", "/dav/files/rejected", strings.NewReader("provider rejects after reading upload"), true)
	resp.Body.Close()
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		t.Fatal("provider upload failure reported success")
	}
	resp = call("PUT", "/dav/readonly/forbidden", strings.NewReader("no"), true)
	resp.Body.Close()
	if resp.StatusCode != 403 {
		t.Fatal("read-only mutation admitted", resp.StatusCode)
	}
	request, _ = http.NewRequestWithContext(ctx, "PUT", server.URL+"/dav/files/conditional", strings.NewReader("no"))
	request.SetBasicAuth("operator", password)
	request.Header.Set("If-None-Match", "*")
	resp, err = server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotImplemented {
		t.Fatal("unsupported conditional mutation silently admitted")
	}
}

func TestReadConfigRejectsDuplicateAndUnknownFields(t *testing.T) {
	for _, data := range []string{`{"listen":"a","listen":"b"}`, `{"unexpected":true}`, `{"worker":{"schema":"x","secret_files":{},"unknown":true}}`, strings.Repeat(" ", descriptor.MaxBytes) + "{}"} {
		if _, e := ReadConfig(strings.NewReader(data)); e == nil {
			t.Fatal("ambiguous or unknown configuration accepted")
		}
	}
}
