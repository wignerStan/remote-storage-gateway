//go:build dataplane_l7

package httpapi_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	infraexec "github.com/wignerStan/infra/go/process/exec"
	dp "github.com/wignerStan/network-data-plane-worker/go/contracts/worker"
	"github.com/wignerStan/network-data-plane-worker/go/nativeclient"
	config "github.com/wignerStan/remote-storage-gateway/go/contracts/worker"
	worker "github.com/wignerStan/remote-storage-gateway/go/worker/runtime"
	"golang.org/x/net/webdav"
)

// Exercise the real selected provider .so and public Worker DAV entry against
// an HTTP origin's explicit overload, rather than manufacturing a file error.
func TestNativeDAVOriginOverloadPreservesMetadataAndFailureStatus(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	plugin := os.Getenv("REMOTE_GATEWAY_OPENLIST_PLUGIN")
	if plugin == "" {
		plugin = filepath.Join(t.TempDir(), "client-openlist.so")
		spec := infraexec.NewSpec(filepath.Join(runtime.GOROOT(), "bin", "go"), "build", "-buildmode=c-shared", "-buildvcs=false", "-ldflags", "-X main.buildDigest=sha256:"+strings.Repeat("2", 64), "-o", plugin, "github.com/OpenListTeam/OpenList/v4/cmd/openlist-client-plugin").WithTimeout(time.Minute).WithEnv(infraexec.EnvInheritAllButRedacted())
		spec.Dir = filepath.Join("..", "..")
		output, err := infraexec.RunContext(ctx, spec)
		if err != nil {
			t.Fatalf("build native provider: %v %s%s", err, output.StdoutString(), output.StderrString())
		}
	}
	binary, err := os.ReadFile(plugin)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(binary)
	artifact := dp.Artifact{Path: plugin, SHA256: hex.EncodeToString(sum[:]), PluginID: "client.openlist", Language: "go"}
	record, err := nativeclient.Inspect(ctx, artifact.Path, artifact.SHA256, artifact.PluginID, artifact.Language)
	if err != nil {
		t.Fatal(err)
	}
	artifact.DescriptorDigest = record.Digest
	files := t.TempDir()
	for _, name := range []string{"ok.txt", "open-overload.txt", "stat-overload.txt"} {
		if err = os.WriteFile(filepath.Join(files, name), []byte("native bytes"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	native := &webdav.Handler{FileSystem: webdav.Dir(files), LockSystem: webdav.NewMemLS()}
	var reads atomic.Int32
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			reads.Add(1)
		}
		if r.URL.Path == "/stat-overload.txt" || r.URL.Path == "/open-overload.txt" && r.Method == "GET" {
			w.Header().Set("Retry-After", "1")
			http.Error(w, "busy", 503)
			return
		}
		native.ServeHTTP(w, r)
	}))
	defer origin.Close()
	dir, err := os.MkdirTemp("/tmp", "trade-dav-status-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	auth := filepath.Join(dir, "password")
	const password = "native-overload-integration-password"
	if err = os.WriteFile(auth, []byte(password), 0600); err != nil {
		t.Fatal(err)
	}
	public, _ := json.Marshal(map[string]string{"address": origin.URL})
	selected := dp.Selection{Name: "files", PluginID: artifact.PluginID, DescriptorDigest: record.Digest, ImplementationID: "webdav", Public: public}
	service, err := worker.New(ctx, config.ProcessConfig{InstanceID: "overload-worker", ControlSocket: filepath.Join(dir, "control"), RuntimeConfig: config.RuntimeConfig{Listen: "127.0.0.1:0", Username: "operator", AuthSecret: "ingress", Worker: dp.Bootstrap{Schema: dp.BootstrapSchema, Artifacts: []dp.Artifact{artifact}, Clients: []dp.Selection{selected}, SecretFiles: map[string]string{"ingress": auth}}, Mounts: []config.Mount{{Name: "files", Client: "files"}}}})
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close(context.Background())
	if err = service.Start(ctx); err != nil {
		t.Fatal(err)
	}
	observed, err := service.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 5 * time.Second}
	request := func(method, name string, headers map[string]string) (*http.Response, []byte) {
		t.Helper()
		q, e := http.NewRequestWithContext(ctx, method, observed.DataEndpoint+"/dav/files/"+name, nil)
		if e != nil {
			t.Fatal(e)
		}
		q.SetBasicAuth("operator", password)
		for key, value := range headers {
			q.Header.Set(key, value)
		}
		response, e := client.Do(q)
		if e != nil {
			t.Fatal(e)
		}
		body, e := io.ReadAll(response.Body)
		response.Body.Close()
		if e != nil {
			t.Fatalf("%s %s body: %v", method, name, e)
		}
		return response, body
	}
	for _, tc := range []struct {
		method, name string
		headers      map[string]string
		status       int
	}{
		{"GET", "stat-overload.txt", nil, 503},
		{"HEAD", "stat-overload.txt", nil, 503},
		{"PROPFIND", "stat-overload.txt", map[string]string{"Depth": "0"}, 503},
		{"GET", "open-overload.txt", nil, 503},
		{"GET", "missing.txt", nil, 404},
	} {
		response, body := request(tc.method, tc.name, tc.headers)
		if response.StatusCode != tc.status {
			t.Fatalf("%s %s status%d want%d body%q", tc.method, tc.name, response.StatusCode, tc.status, body)
		}
		if tc.status == 503 && response.Header.Get("Retry-After") != "1" {
			t.Fatal("provider overload lost Retry-After")
		}
	}

	// ServeContent owns a pipe goroutine for multipart ranges. Its file read may
	// publish a provider failure concurrently with the response writer. Once MIME
	// framing has been sent, preserve that partial response; otherwise return503.
	for range 8 {
		q, e := http.NewRequestWithContext(ctx, "GET", observed.DataEndpoint+"/dav/files/open-overload.txt", nil)
		if e != nil {
			t.Fatal(e)
		}
		q.SetBasicAuth("operator", password)
		q.Header.Set("Range", "bytes=0-1,8-9")
		response, e := client.Do(q)
		if e != nil {
			t.Fatal(e)
		}
		body, readError := io.ReadAll(response.Body)
		response.Body.Close()
		switch response.StatusCode {
		case http.StatusServiceUnavailable:
			if readError != nil || response.Header.Get("Retry-After") != "1" || string(body) != "Client unavailable\n" {
				t.Fatalf("multipart prebody failure: %v %q", readError, body)
			}
		case http.StatusPartialContent:
			if readError == nil || response.Header.Get("Retry-After") != "" || strings.Contains(string(body), "Client unavailable") {
				t.Fatalf("committed multipart failure rewritten: %v %q", readError, body)
			}
		default:
			t.Fatalf("multipart provider failure status%d", response.StatusCode)
		}
	}
	before := reads.Load()
	for _, tc := range []struct {
		method  string
		headers map[string]string
		status  int
	}{
		{"HEAD", nil, 200},
		{"PROPFIND", map[string]string{"Depth": "0"}, 207},
		{"GET", map[string]string{"If-Modified-Since": time.Now().Add(time.Hour).UTC().Format(http.TimeFormat)}, 304},
		{"GET", map[string]string{"Range": "bytes=999-"}, 416},
	} {
		response, _ := request(tc.method, "open-overload.txt", tc.headers)
		if response.StatusCode != tc.status {
			t.Fatalf("metadata%s status%d want%d", tc.method, response.StatusCode, tc.status)
		}
	}
	if reads.Load() != before {
		t.Fatal("metadata-only requests opened provider payload")
	}
	response, body := request("GET", "ok.txt", nil)
	if response.StatusCode != 200 || string(body) != "native bytes" {
		t.Fatalf("normal GET changed: %d %q", response.StatusCode, body)
	}
	response, body = request("GET", "ok.txt", map[string]string{"Range": "bytes=1-4"})
	if response.StatusCode != 206 || string(body) != "ativ" {
		t.Fatalf("normal range changed: %d %q", response.StatusCode, body)
	}
}
