//go:build linux && dataplane_l7

package manager_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/descriptor/admission"
	storageipc "github.com/wignerStan/network-data-plane-worker/go/contracts/storageipc"
	dp "github.com/wignerStan/network-data-plane-worker/go/contracts/worker"
	rclonebox "github.com/wignerStan/rclone/go/box"
	cpconfig "github.com/wignerStan/remote-storage-gateway/go/contracts/control"
	"github.com/wignerStan/remote-storage-gateway/go/contracts/control/broker"
	workerconfig "github.com/wignerStan/remote-storage-gateway/go/contracts/worker"
	control "github.com/wignerStan/remote-storage-gateway/go/control/ttrpc"
	bundle "github.com/wignerStan/remote-storage-gateway/go/manager"
	"golang.org/x/net/webdav"
)

// These acceptance tests consume installed source-built owner artifacts. They
// never synthesize a replacement executable, plugin or UI inside the fixture.
func TestGatewayBundleAcceptance(t *testing.T) {
	binary, plugin := os.Getenv("REMOTE_GATEWAY_BUNDLE_BIN"), os.Getenv("REMOTE_GATEWAY_OPENLIST_PLUGIN")
	if binary == "" || plugin == "" {
		t.Skip("bundle acceptance requires installed REMOTE_GATEWAY_BUNDLE_BIN and REMOTE_GATEWAY_OPENLIST_PLUGIN owner artifacts; no fixture build fallback")
	}
	for _, path := range []string{binary, plugin} {
		if !filepath.IsAbs(path) {
			t.Fatalf("installed artifact must have an absolute path: %q", path)
		}
		if info, err := os.Stat(path); err != nil || !info.Mode().IsRegular() {
			t.Fatalf("installed artifact unavailable: %q: %v", path, err)
		}
	}
	file, err := os.Open(plugin)
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.New()
	_, err = io.Copy(hash, file)
	file.Close()
	if err != nil {
		t.Fatal(err)
	}
	digest := hex.EncodeToString(hash.Sum(nil))
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	// Loading the native plugin happens in a separate invocation of the actual
	// bundle Worker role, never in the test/manager/control-plane process.
	inspect := exec.CommandContext(ctx, binary, "worker", "--inspect-artifact", plugin, "--artifact-sha256", digest, "--plugin-id", "client.openlist", "--language", "go")
	var stderr bytes.Buffer
	inspect.Stderr = &stderr
	output, err := inspect.Output()
	if err != nil {
		t.Fatalf("bundled Worker passive inspection failed: %v: %s", err, stderr.String())
	}
	record, err := admission.Decode(bytes.TrimSuffix(output, []byte("\n")))
	if err != nil {
		t.Fatalf("exact installed descriptor admission: %v", err)
	}
	artifact := dp.Artifact{Path: plugin, SHA256: digest, PluginID: "client.openlist", DescriptorDigest: record.Digest, Language: "go"}

	t.Run("same_ELF_TLS_UI_auth_DAV_and_SIGTERM_drain", func(t *testing.T) {
		f := newBundleFixture(t, binary, artifact)
		p := f.start(t)
		children := f.ready(t, p)
		f.assertDatabase(t)
		for _, endpoint := range []string{f.controlURL + "/ping", f.publicURL + "/ping"} {
			status, body := f.request(t, http.MethodGet, endpoint, false)
			if status != http.StatusOK || string(body) != "pong" {
				t.Fatalf("control-plane readiness %s: status=%d body=%q", endpoint, status, body)
			}
		}
		status, body := f.request(t, http.MethodGet, f.publicURL+"/", false)
		if status != http.StatusOK || !bytes.Contains(bytes.ToLower(body), []byte("<html")) {
			t.Fatalf("embedded control-plane UI not served through Worker TLS: status=%d body=%q", status, body)
		}
		// OpenList uses a JSON application code as well as the HTTP status. A
		// Worker Basic credential must never become an administrator session.
		for _, authenticated := range []bool{false, true} {
			status, body = f.request(t, http.MethodGet, f.publicURL+"/api/admin/storage/list", authenticated)
			var response struct {
				Code int `json:"code"`
			}
			if json.Unmarshal(body, &response) != nil || response.Code != http.StatusUnauthorized {
				t.Fatalf("CP admin auth bypass: basic=%v status=%d body=%q", authenticated, status, body)
			}
		}
		for _, path := range []string{"/v1/state", "/v1/manifest", "/dav/files/object"} {
			status, _ = f.request(t, http.MethodGet, f.publicURL+path, false)
			if status != http.StatusUnauthorized {
				t.Fatalf("Worker auth bypass for %s: %d", path, status)
			}
		}
		for _, path := range []string{"/d/object", "/p/object", "/api/fs/get", "/api/fs/list", "/api/fs/put", "/s3/bucket/object", "/@emby/Items"} {
			method := http.MethodGet
			if strings.HasPrefix(path, "/api/fs/") {
				method = http.MethodPost
			}
			for _, endpoint := range []string{f.publicURL, f.controlURL} {
				status, _ = f.request(t, method, endpoint+path, true)
				if status != http.StatusNotFound {
					t.Fatalf("legacy payload route admitted: %s %s%s: %d", method, endpoint, path, status)
				}
			}
		}
		status, body = f.request(t, http.MethodGet, f.publicURL+"/dav/files/object", true)
		if status != http.StatusOK || !bytes.Equal(body, f.payload) {
			t.Fatalf("real plugin WebDAV read: status=%d bytes=%d", status, len(body))
		}
		status, body = f.request(t, http.MethodGet, f.publicURL+"/v1/descriptors/"+record.Digest, true)
		if status != http.StatusOK || !bytes.Equal(body, record.Bytes) {
			t.Fatalf("approved descriptor bytes changed: status=%d bytes=%d", status, len(body))
		}

		f.drainSlowRead(t, p, children)
	})

	t.Run("same_ELF_control_worker_and_independent_VFS_tiny_files", func(t *testing.T) {
		rcloneBinary := os.Getenv("REMOTE_GATEWAY_RCLONE_BIN")
		if rcloneBinary == "" {
			t.Skip("requires the independently source-built rclone owner artifact")
		}
		if info, e := os.Stat(rcloneBinary); !filepath.IsAbs(rcloneBinary) || e != nil || !info.Mode().IsRegular() {
			t.Fatalf("invalid rclone owner artifact %q: %v", rcloneBinary, e)
		}
		f := newBundleFixture(t, binary, artifact)
		f.enableMaterializer(t, rcloneBinary)
		const files, concurrency = 128, 8
		contents := make(map[string][]byte, files)
		directory := filepath.Join(f.dir, "origin", "tiny")
		if e := os.Mkdir(directory, 0700); e != nil {
			t.Fatal(e)
		}
		for i := range files {
			name := fmt.Sprintf("file-%03d", i)
			contents[name] = []byte(fmt.Sprintf("bundle-tiny-%03d:%s\n", i, strings.Repeat("x", i%32+1)))
			if e := os.WriteFile(filepath.Join(directory, name), contents[name], 0600); e != nil {
				t.Fatal(e)
			}
		}
		p := f.start(t)
		children := f.ready(t, p)
		f.assertDatabase(t)
		if len(children) != 3 || children["vfs"] == 0 {
			t.Fatalf("manager, CP, Worker and VFS did not form four independent processes: %+v", children)
		}
		t.Logf("four processes manager=%d control-plane=%d worker=%d independent-vfs=%d", p.cmd.Process.Pid, children["control-plane"], children["worker"], children["vfs"])
		// The real CP is metadata-only; direct object routes remain absent and cannot
		// reach the origin. Public object traffic still enters the Worker's TLS port.
		before := f.originReads.Load()
		for _, path := range []string{"/dav/files/tiny/file-000", "/api/fs/get", "/api/fs/list"} {
			method := http.MethodGet
			if strings.HasPrefix(path, "/api/fs/") {
				method = http.MethodPost
			}
			status, _ := f.request(t, method, f.controlURL+path, true)
			if status != http.StatusNotFound {
				t.Fatalf("CP exposed object route %s: %d", path, status)
			}
		}
		for name := range contents {
			status, _ := f.request(t, http.MethodHead, f.publicURL+"/dav/files/tiny/"+name, true)
			if status != http.StatusOK {
				t.Fatalf("tiny metadata %s: %d", name, status)
			}
		}
		if got := f.originReads.Load(); got != before {
			t.Fatalf("metadata caused origin body reads: before=%d after=%d", before, got)
		}
		pass := func() {
			t.Helper()
			jobs := make(chan string, files)
			failures := make(chan error, files)
			var joined sync.WaitGroup
			for range concurrency {
				joined.Add(1)
				go func() {
					defer joined.Done()
					for name := range jobs {
						request, e := http.NewRequestWithContext(t.Context(), http.MethodGet, f.publicURL+"/dav/files/tiny/"+name, nil)
						if e != nil {
							failures <- e
							continue
						}
						request.SetBasicAuth("operator", bundlePassword)
						response, e := f.client.Do(request)
						if e != nil {
							failures <- e
							continue
						}
						body, readErr := io.ReadAll(io.LimitReader(response.Body, 4096))
						closeErr := response.Body.Close()
						if response.StatusCode != http.StatusOK || readErr != nil || closeErr != nil || !bytes.Equal(body, contents[name]) {
							failures <- fmt.Errorf("tiny %s: status=%d bytes=%d read=%v close=%v", name, response.StatusCode, len(body), readErr, closeErr)
						}
					}
				}()
			}
			for name := range contents {
				jobs <- name
			}
			close(jobs)
			joined.Wait()
			close(failures)
			for e := range failures {
				t.Error(e)
			}
			if t.Failed() {
				t.FailNow()
			}
		}
		pass()
		cold := f.originReads.Load()
		if cold-before != files {
			t.Fatalf("cold pass origin reads=%d; expected one each for %d distinct files", cold-before, files)
		}
		pass()
		if after := f.originReads.Load(); after != cold {
			t.Fatalf("hot pass bypassed the independent cache: before=%d after=%d", cold, after)
		}
		if f.originActive.Load() != 0 || f.originPeak.Load() > concurrency {
			t.Fatalf("origin work escaped capacity: active=%d peak=%d limit=%d", f.originActive.Load(), f.originPeak.Load(), concurrency)
		}
		t.Logf("tiny-files=%d cold-origin-GETs=%d hot-extra-origin-GETs=0 peak=%d limit=%d", files, cold-before, f.originPeak.Load(), concurrency)
		status, body := f.request(t, http.MethodGet, f.publicURL+"/remote-dav/files/tiny/file-000", true)
		if status != http.StatusOK || !bytes.Equal(body, contents["file-000"]) || f.originReads.Load() != cold+1 {
			t.Fatal("raw alias failed to bypass the independent cache")
		}
		// Replacement of the raw generation must also invalidate a cached hit. The
		// old materializer cannot resurrect its former grant or fall back to raw I/O.
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()
		manager, e := control.DialWorker(ctx, f.config.Workers[0].Config.ControlSocket)
		if e != nil {
			t.Fatal(e)
		}
		defer manager.Close(context.Background())
		states, e := manager.Observed(ctx)
		if e != nil {
			t.Fatal(e)
		}
		var raw dp.Version
		for _, state := range states {
			if state.Name == "origin" && state.Accepting {
				raw = state.Version
			}
		}
		if raw.Generation == 0 {
			t.Fatal("raw origin generation was not observed")
		}
		selection := f.config.Workers[0].Config.Worker.Clients[0]
		replacement, e := manager.Replace(ctx, "origin", raw, selection)
		if e != nil || replacement == raw {
			t.Fatalf("raw generation replacement: %+v %v", replacement, e)
		}
		beforeRetiredRead := f.originReads.Load()
		status, body = f.request(t, http.MethodGet, f.publicURL+"/dav/files/tiny/file-000", true)
		if status != http.StatusServiceUnavailable || string(body) != "Client unavailable\n" || f.originReads.Load() != beforeRetiredRead {
			t.Fatalf("retired raw session did not retain the unavailable contract: status=%d body=%q reads=%d", status, body, f.originReads.Load()-beforeRetiredRead)
		}
		status, body = f.request(t, http.MethodGet, f.publicURL+"/remote-dav/files/tiny/file-000", true)
		if status != http.StatusOK || !bytes.Equal(body, contents["file-000"]) || f.originReads.Load() != beforeRetiredRead+1 {
			t.Fatal("fresh raw generation did not remain independently usable")
		}
		status, _ = f.request(t, http.MethodGet, f.publicURL+"/healthz", false)
		if status != http.StatusNoContent {
			t.Fatalf("Worker became unhealthy after raw replacement: %d", status)
		}
		status, body = f.request(t, http.MethodGet, f.controlURL+"/ping", false)
		if status != http.StatusOK || string(body) != "pong" {
			t.Fatalf("CP became unhealthy after raw replacement: status=%d body=%q", status, body)
		}
		if e := p.cmd.Process.Signal(syscall.SIGTERM); e != nil {
			t.Fatal(e)
		}
		if e := p.wait(t, 35*time.Second); e != nil {
			t.Fatalf("four-process bundle SIGTERM: %v; %s", e, p.diagnostics())
		}
		f.assertRetired(t, children)
	})

	t.Run("same_ELF_control_worker_and_independent_VFS_cold_SIGTERM_drain", func(t *testing.T) {
		rcloneBinary := os.Getenv("REMOTE_GATEWAY_RCLONE_BIN")
		if rcloneBinary == "" {
			t.Skip("requires the independently source-built rclone owner artifact")
		}
		if info, e := os.Stat(rcloneBinary); !filepath.IsAbs(rcloneBinary) || e != nil || !info.Mode().IsRegular() {
			t.Fatalf("invalid rclone owner artifact %q: %v", rcloneBinary, e)
		}
		f := newBundleFixture(t, binary, artifact)
		f.enableMaterializer(t, rcloneBinary)
		p := f.start(t)
		children := f.ready(t, p)
		if children["vfs"] == 0 || len(children) != 3 {
			t.Fatalf("independent VFS process absent: %+v", children)
		}
		// Readiness warms only object. The first slow request therefore traverses
		// Worker -> native VFS -> Worker IPC/shared-memory -> actual origin.
		f.drainSlowRead(t, p, children)
		t.Logf("four-process cold VFS read completed through SIGTERM before all children and owned sockets retired")
	})

	t.Run("worker_startup_failure_reaps_control_plane", func(t *testing.T) {
		f := newBundleFixture(t, binary, artifact)
		// Occupy only the Worker's listener. Passive config admission succeeds,
		// CP must become ready, and Worker then fails after creating its UDS.
		occupied, err := net.Listen("tcp", f.config.Workers[0].Config.Listen)
		if err != nil {
			t.Fatal(err)
		}
		defer occupied.Close()
		p := f.start(t)
		children := make(map[string]int)
		deadline := time.After(80 * time.Second)
		for {
			for role, pid := range bundleChildren(p.cmd.Process.Pid) {
				children[role] = pid
			}
			select {
			case <-p.done:
				if p.err == nil {
					t.Fatal("Worker bind failure reported successful manager exit")
				}
				if children["control-plane"] == 0 {
					t.Fatal("startup failure test did not observe the actual control-plane child")
				}
				f.assertDatabase(t) // proves this was not an early config rejection
				if err := occupied.Close(); err != nil {
					t.Fatal(err)
				}
				f.assertRetired(t, children)
				return
			case <-deadline:
				t.Fatalf("startup failure did not retire bundle: %s", p.diagnostics())
			case <-time.After(5 * time.Millisecond):
			}
		}
	})
	for _, role := range []string{"worker", "control-plane"} {
		t.Run(role+"_SIGKILL_retires_whole_group", func(t *testing.T) {
			f := newBundleFixture(t, binary, artifact)
			p := f.start(t)
			children := f.ready(t, p)
			if err := syscall.Kill(children[role], syscall.SIGKILL); err != nil {
				t.Fatal(err)
			}
			if err := p.wait(t, 40*time.Second); err == nil {
				t.Fatal("unexpected child death reported successful manager exit")
			}
			f.assertRetired(t, children)
		})
	}
}

func (f *bundleFixture) drainSlowRead(t *testing.T, p *bundleProcess, children map[string]int) {
	t.Helper()
	// Hold a real provider read in flight while SIGTERM retires the manager.
	// Reaping alone is insufficient: the admitted exchange must finish first.
	type readResult struct {
		status int
		body   []byte
		err    error
	}
	read := make(chan readResult, 1)
	go func() {
		request, _ := http.NewRequest(http.MethodGet, f.publicURL+"/dav/files/slow", nil)
		request.SetBasicAuth("operator", bundlePassword)
		response, err := f.client.Do(request)
		if err != nil {
			read <- readResult{err: err}
			return
		}
		body, err := io.ReadAll(response.Body)
		response.Body.Close()
		read <- readResult{response.StatusCode, body, err}
	}()
	select {
	case <-f.readStarted:
	case <-time.After(10 * time.Second):
		t.Fatal("provider streaming read did not start")
	}
	if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case <-p.done:
		t.Fatal("bundle exited before the admitted provider read drained")
	case <-time.After(150 * time.Millisecond):
	}
	f.releaseRead()
	select {
	case result := <-read:
		if result.err != nil || result.status != http.StatusOK || !bytes.Equal(result.body, f.payload) {
			t.Fatalf("SIGTERM truncated admitted provider read: status=%d bytes=%d error=%v", result.status, len(result.body), result.err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("admitted provider read failed to drain")
	}
	if err := p.wait(t, 35*time.Second); err != nil {
		t.Fatalf("SIGTERM exit: %v; %s", err, p.diagnostics())
	}
	f.assertRetired(t, children)
}

const bundlePassword = "bundle-private-ingress-password"

type bundleFixture struct {
	dir, binary, publicURL, controlURL    string
	config                                bundle.Config
	client                                *http.Client
	payload                               []byte
	readStarted                           chan struct{}
	releaseRead                           func()
	originReads, originActive, originPeak atomic.Int32
}

func newBundleFixture(t *testing.T, binary string, artifact dp.Artifact) *bundleFixture {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "trade-bundle-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	f := &bundleFixture{dir: dir, binary: binary, payload: bytes.Repeat([]byte("same-ELF-real-plugin-payload\n"), 16384), readStarted: make(chan struct{})}
	release := make(chan struct{})
	var releaseOnce, startOnce sync.Once
	f.releaseRead = func() { releaseOnce.Do(func() { close(release) }) }
	originDir := filepath.Join(dir, "origin")
	if err = os.Mkdir(originDir, 0700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"object", "slow"} {
		if err = os.WriteFile(filepath.Join(originDir, name), f.payload, 0600); err != nil {
			t.Fatal(err)
		}
	}
	dav := &webdav.Handler{FileSystem: webdav.Dir(originDir), LockSystem: webdav.NewMemLS()}
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			f.originReads.Add(1)
			active := f.originActive.Add(1)
			defer f.originActive.Add(-1)
			for peak := f.originPeak.Load(); active > peak; peak = f.originPeak.Load() {
				if f.originPeak.CompareAndSwap(peak, active) {
					break
				}
			}
		}
		if r.Header.Get("Authorization") != "" {
			t.Error("Worker ingress credentials leaked to origin")
		}
		if r.Method == http.MethodGet && r.URL.Path == "/slow" {
			w = &bundleGatedWriter{ResponseWriter: w, gate: func() {
				startOnce.Do(func() { close(f.readStarted); <-release })
			}}
		}
		dav.ServeHTTP(w, r)
	}))
	t.Cleanup(func() { f.releaseRead(); origin.Close() })
	address := func() string {
		t.Helper()
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer listener.Close()
		return listener.Addr().String()
	}
	workerAddress, controlAddress := address(), address()
	f.controlURL = "http://" + controlAddress
	_, publicPort, _ := net.SplitHostPort(workerAddress)
	f.publicURL = "https://gateway.test:" + publicPort
	cert, key, roots := gatewayDNSCertificate(t)
	certPath, keyPath, passwordPath := filepath.Join(dir, "tls.crt"), filepath.Join(dir, "tls.key"), filepath.Join(dir, "password")
	for path, content := range map[string][]byte{certPath: cert, keyPath: key, passwordPath: []byte(bundlePassword)} {
		if err := os.WriteFile(path, content, 0600); err != nil {
			t.Fatal(err)
		}
	}
	transport := &http.Transport{Proxy: nil, TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}, DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
		if addr == "gateway.test:"+publicPort {
			addr = workerAddress
		}
		return (&net.Dialer{}).DialContext(ctx, network, addr)
	}}
	f.client = &http.Client{Transport: transport, Timeout: 20 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	t.Cleanup(transport.CloseIdleConnections)
	public, err := json.Marshal(map[string]string{"address": origin.URL})
	if err != nil {
		t.Fatal(err)
	}
	runDir := filepath.Join(dir, "run")
	controlSocket := filepath.Join(runDir, "worker")
	process := workerconfig.ProcessConfig{
		InstanceID: "bundle-worker", ControlSocket: controlSocket, DataEndpoint: f.publicURL,
		RuntimeConfig: workerconfig.RuntimeConfig{
			Listen: workerAddress, Username: "operator", AuthSecret: "ingress",
			TLS:          &workerconfig.TLSOptions{CertificateFile: certPath, PrivateKeySecret: "tls-key"},
			ControlPlane: &workerconfig.ControlPlaneRoute{Host: "gateway.test", Upstream: f.controlURL},
			Worker: dp.Bootstrap{Schema: dp.BootstrapSchema, Artifacts: []dp.Artifact{artifact},
				Clients:     []dp.Selection{{Name: "origin", PluginID: artifact.PluginID, DescriptorDigest: artifact.DescriptorDigest, ImplementationID: "webdav", Public: public}},
				SecretFiles: map[string]string{"ingress": passwordPath, "tls-key": keyPath}},
			Mounts: []workerconfig.Mount{{Name: "files", Client: "origin", ReadOnly: true}},
		},
	}
	dataDir := filepath.Join(dir, "cp")
	f.config = bundle.Config{RuntimeDirectory: runDir, ControlPlanes: []bundle.ControlPlaneSpec{{ID: "bundle-cp", DataDirectory: dataDir, ConfigFile: filepath.Join(dataDir, "config.json"), ReadyURL: f.controlURL + "/ping", Control: cpconfig.Config{Workers: []cpconfig.WorkerEndpoint{{ID: process.InstanceID, ControlSocket: controlSocket, DataEndpoint: f.publicURL}}}}}, Workers: []bundle.WorkerSpec{{Config: process}}}
	return f
}

func (f *bundleFixture) enableMaterializer(t *testing.T, executable string) {
	t.Helper()
	worker := &f.config.Workers[0].Config
	run := f.config.RuntimeDirectory
	worker.BrokerSocket = filepath.Join(run, "broker")
	worker.StorageIPCSocket = filepath.Join(run, "storage-ipc")
	worker.StorageOfferSocket = filepath.Join(run, "storage-offer")
	auth := filepath.Join(f.dir, "cache-password")
	if e := os.WriteFile(auth, []byte("independent-vfs-private-password"), 0600); e != nil {
		t.Fatal(e)
	}
	worker.Worker.SecretFiles["cache-auth"] = auth
	worker.ClientSecretRefs = []string{"cache-auth"}
	cached := worker.Worker.Clients[0]
	cached.Name = "cached"
	worker.Worker.Clients = append(worker.Worker.Clients, cached)
	worker.Mounts[0].Client, worker.Mounts[0].RemoteClient = "cached", "origin"
	listener, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	address := listener.Addr().String()
	listener.Close()
	cached.Public, e = json.Marshal(map[string]string{"address": "http://" + address + "/dav/", "username": "cache-user"})
	if e != nil {
		t.Fatal(e)
	}
	cached.SecretRefs = map[string]string{"password": "cache-auth"}
	f.config.Materializers = []bundle.MaterializerSpec{{Executable: executable, ActivateClient: &cached, Config: rclonebox.ProcessConfig{
		InstanceID: "bundle-vfs", ControlSocket: filepath.Join(run, "vfs"), Listen: address,
		Remote:        storageipc.Endpoint{Socket: worker.StorageIPCSocket, OfferSocket: worker.StorageOfferSocket, Namespace: "files", ExpectedWorkerID: worker.InstanceID},
		LocalUsername: "cache-user", LocalPasswordFile: auth, Directory: filepath.Join(f.dir, "cache"),
		MaxBytes: 8 << 20, MaxAgeSeconds: 60, MaxCacheEntries: 256, MaxOpenFiles: 8, MaxDirectoryEntries: 256, MaxConcurrentRequests: 8,
	}}}
	plane := &f.config.ControlPlanes[0]
	plane.Control.Workers[0].BrokerSocket = worker.BrokerSocket
	plane.Control.Rules = []cpconfig.Rule{{InstanceID: worker.InstanceID, Authority: "storage-ipc", SourceID: "files", TargetID: "origin", Decision: broker.Decision{
		RouteID: "bundle-materialize", WorkerDataEndpoint: worker.DataEndpoint, ApprovedUpstreamID: "origin", TTLSeconds: 120,
		MaxMessageBytes: 256 << 10, MaxInflightBytes: 8 * (256 << 10), Methods: []string{storageipc.AuthorizeMethod},
	}}}
}

func (f *bundleFixture) materializerActivated() bool {
	if len(f.config.Materializers) == 0 {
		return true
	}
	// Before activation this public route reads the raw origin on every call.
	// The first VFS call may warm the existing object; a subsequent exact read
	// with no origin GET witnesses the manager's real cached-route publication.
	// These probes precede the separately measured tiny-file baseline.
	before := f.originReads.Load()
	request, err := http.NewRequest(http.MethodGet, f.publicURL+"/dav/files/object", nil)
	if err != nil {
		return false
	}
	request.SetBasicAuth("operator", bundlePassword)
	response, err := f.client.Do(request)
	if err != nil {
		return false
	}
	body, readErr := io.ReadAll(io.LimitReader(response.Body, int64(len(f.payload)+1)))
	closeErr := response.Body.Close()
	return response.StatusCode == http.StatusOK && readErr == nil && closeErr == nil && bytes.Equal(body, f.payload) && f.originReads.Load() == before
}

type bundleGatedWriter struct {
	http.ResponseWriter
	gate func()
}

func (w *bundleGatedWriter) Write(b []byte) (int, error) {
	n, err := w.ResponseWriter.Write(b)
	if flusher, ok := w.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
	w.gate()
	return n, err
}

type bundleProcess struct {
	cmd     *exec.Cmd
	done    chan struct{}
	err     error
	logPath string
}

func (f *bundleFixture) start(t *testing.T) *bundleProcess {
	t.Helper()
	config, err := json.Marshal(f.config)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(f.dir, "bundle.json")
	if err = os.WriteFile(path, config, 0600); err != nil {
		t.Fatal(err)
	}
	p := &bundleProcess{cmd: exec.Command(f.binary, "manager", "--config", path), done: make(chan struct{}), logPath: filepath.Join(f.dir, "bundle.log")}
	p.cmd.Dir = f.dir
	p.cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	// Do not inherit ambient OpenList overrides or proxy credentials. All
	// persistent config/data and secrets belong to this private local fixture.
	p.cmd.Env = []string{"PATH=/usr/bin:/bin", "HOME=" + f.dir, "TMPDIR=" + f.dir, "NO_PROXY=127.0.0.1,localhost,gateway.test", "GOTRACEBACK=single"}
	log, err := os.OpenFile(p.logPath, os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	p.cmd.Stdout, p.cmd.Stderr = log, log
	if err = p.cmd.Start(); err != nil {
		log.Close()
		t.Fatal(err)
	}
	go func() { p.err = p.cmd.Wait(); log.Close(); close(p.done) }()
	t.Cleanup(func() {
		f.releaseRead()
		select {
		case <-p.done:
		default:
			_ = p.cmd.Process.Signal(syscall.SIGTERM)
			select {
			case <-p.done:
			case <-time.After(35 * time.Second):
				_ = syscall.Kill(-p.cmd.Process.Pid, syscall.SIGKILL)
				<-p.done
			}
		}
		// Also retire any leaked descendants when a lifecycle assertion fails.
		_ = syscall.Kill(-p.cmd.Process.Pid, syscall.SIGKILL)
		if t.Failed() {
			t.Logf("bundle diagnostics: %s", p.diagnostics())
		}
	})
	return p
}

func (p *bundleProcess) diagnostics() string {
	b, _ := os.ReadFile(p.logPath)
	if len(b) > 8192 {
		b = b[len(b)-8192:]
	}
	return string(b)
}
func (p *bundleProcess) wait(t *testing.T, timeout time.Duration) error {
	t.Helper()
	select {
	case <-p.done:
		return p.err
	case <-time.After(timeout):
		t.Fatalf("bundle failed to exit within %s: %s", timeout, p.diagnostics())
		return nil
	}
}

// A Go process can fork on any OS thread. Linux exposes each thread's children
// separately, so inspecting only the thread-group leader misses live roles.
// Comparing executable inodes also supports a symlinked installation path.
func bundleChildren(parent int) map[string]int {
	out := make(map[string]int)
	tasks, _ := filepath.Glob(fmt.Sprintf("/proc/%d/task/*/children", parent))
	seen := make(map[int]bool)
	for _, task := range tasks {
		data, _ := os.ReadFile(task)
		for _, text := range strings.Fields(string(data)) {
			pid, err := strconv.Atoi(text)
			if err != nil || pid <= 0 || seen[pid] {
				continue
			}
			seen[pid] = true
			cmdline, _ := os.ReadFile(fmt.Sprintf("/proc/%d/cmdline", pid))
			args := bytes.Split(cmdline, []byte{0})
			if len(args) > 2 && string(args[1]) == "serve" && string(args[2]) == "managed-vfs" {
				out["vfs"] = pid
			}
			if len(args) > 1 && (string(args[1]) == "worker" || string(args[1]) == "control-plane") {
				out[string(args[1])] = pid
			}
		}
	}
	return out
}

// The entry emits this complete bounded record after Manager.Start succeeds.
// A healthy child HTTP endpoint alone does not witness that parent phase.
func (p *bundleProcess) managerStarted() bool {
	file, err := os.Open(p.logPath)
	if err != nil {
		return false
	}
	defer file.Close()
	body, err := io.ReadAll(io.LimitReader(file, 64<<10))
	if err != nil {
		return false
	}
	for _, line := range bytes.SplitAfter(body, []byte("\n")) {
		if bytes.Equal(line, []byte("storage-gateway manager ready\n")) {
			return true
		}
	}
	return false
}

func (f *bundleFixture) ready(t *testing.T, p *bundleProcess) map[string]int {
	t.Helper()
	deadline := time.After(80 * time.Second)
	for {
		children := bundleChildren(p.cmd.Process.Pid)
		if p.managerStarted() && len(children) == 2+len(f.config.Materializers) && f.materializerActivated() {
			// The Worker must not exist before CP /ping is genuinely ready.
			status, body := f.request(t, http.MethodGet, f.controlURL+"/ping", false)
			if status != http.StatusOK || string(body) != "pong" {
				t.Fatalf("Worker spawned before CP ready: status=%d body=%q", status, body)
			}
			response, err := f.client.Get(f.publicURL + "/healthz")
			if err == nil {
				io.Copy(io.Discard, response.Body)
				response.Body.Close()
				if response.StatusCode == http.StatusNoContent {
					seen := map[int]bool{p.cmd.Process.Pid: true, os.Getpid(): true}
					installed, err := os.Stat(f.binary)
					if err != nil {
						t.Fatal(err)
					}
					for role, pid := range children {
						if pid <= 0 || seen[pid] {
							t.Fatalf("roles share PID: %s=%d", role, pid)
						}
						seen[pid] = true
						expected := installed
						if role == "vfs" {
							expected, err = os.Stat(f.config.Materializers[0].Executable)
							if err != nil {
								t.Fatal(err)
							}
						}
						actual, err := os.Stat(fmt.Sprintf("/proc/%d/exe", pid))
						if err != nil || !os.SameFile(expected, actual) {
							t.Fatalf("%s PID %d did not execute its admitted owner artifact: %v", role, pid, err)
						}
					}
					actual, err := os.Stat(fmt.Sprintf("/proc/%d/exe", p.cmd.Process.Pid))
					if err != nil || !os.SameFile(installed, actual) {
						t.Fatalf("manager ELF identity mismatch: %v", err)
					}
					return children
				}
			}
		}
		select {
		case <-p.done:
			t.Fatalf("bundle exited before readiness: %v; %s", p.err, p.diagnostics())
		case <-deadline:
			t.Fatalf("bundle readiness timeout: %s", p.diagnostics())
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func (f *bundleFixture) request(t *testing.T, method, endpoint string, authenticated bool) (int, []byte) {
	t.Helper()
	request, err := http.NewRequest(method, endpoint, nil)
	if err != nil {
		t.Fatal(err)
	}
	if authenticated {
		request.SetBasicAuth("operator", bundlePassword)
	}
	response, err := f.client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 2<<20))
	if err != nil {
		t.Fatal(err)
	}
	return response.StatusCode, body
}

func (f *bundleFixture) assertDatabase(t *testing.T) {
	t.Helper()
	spec := f.config.ControlPlanes[0]
	config, err := os.ReadFile(spec.ConfigFile)
	if err != nil || !json.Valid(config) {
		t.Fatalf("CP did not create default config: %v", err)
	}
	db, err := os.Open(filepath.Join(spec.DataDirectory, "data.db"))
	if err != nil {
		t.Fatalf("CP SQLite database absent: %v", err)
	}
	defer db.Close()
	header := make([]byte, 16)
	if _, err = io.ReadFull(db, header); err != nil || string(header) != "SQLite format 3\x00" {
		t.Fatalf("CP did not initialize real SQLite database: header=%q error=%v", header, err)
	}
}

func (f *bundleFixture) assertRetired(t *testing.T, children map[string]int) {
	t.Helper()
	for role, pid := range children {
		if err := syscall.Kill(pid, 0); !errors.Is(err, syscall.ESRCH) {
			t.Errorf("%s PID %d was not reaped: %v", role, pid, err)
		}
	}
	for _, spec := range f.config.Workers {
		for _, path := range []string{spec.Config.ControlSocket, spec.Config.BrokerSocket, spec.Config.StorageIPCSocket, spec.Config.StorageOfferSocket} {
			if path == "" {
				continue
			}
			if _, err := os.Lstat(path); !os.IsNotExist(err) {
				t.Errorf("owned UDS survived retirement: %s: %v", path, err)
			}
		}
	}
	for _, materializer := range f.config.Materializers {
		if _, err := os.Lstat(materializer.Config.ControlSocket); !os.IsNotExist(err) {
			t.Errorf("materializer UDS survived retirement: %v", err)
		}
		listener, err := net.Listen("tcp", materializer.Config.Listen)
		if err != nil {
			t.Errorf("materializer listener survived retirement: %v", err)
		} else {
			listener.Close()
		}
	}
	// Released listeners can be rebound; no daemon remains behind the parent.
	_, cpAddress, _ := strings.Cut(f.controlURL, "://")
	for _, addr := range []string{cpAddress, f.config.Workers[0].Config.Listen} {
		listener, err := net.Listen("tcp", addr)
		if err != nil {
			t.Errorf("listener %s survived bundle retirement: %v", addr, err)
		} else {
			listener.Close()
		}
	}
}
