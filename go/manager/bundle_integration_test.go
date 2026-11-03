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
	"syscall"
	"testing"
	"time"

	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/descriptor/admission"
	dp "github.com/wignerStan/network-data-plane-worker/go/contracts/worker"
	cpconfig "github.com/wignerStan/remote-storage-gateway/go/contracts/control"
	workerconfig "github.com/wignerStan/remote-storage-gateway/go/contracts/worker"
	bundle "github.com/wignerStan/remote-storage-gateway/go/manager"
	"golang.org/x/net/webdav"
)

// These are acceptance tests of the installed, remotely built product. They
// deliberately never build a replacement executable or plugin on the test host.
func TestGatewayBundleAcceptance(t *testing.T) {
	binary, plugin := os.Getenv("REMOTE_GATEWAY_BUNDLE_BIN"), os.Getenv("REMOTE_GATEWAY_OPENLIST_PLUGIN")
	if binary == "" || plugin == "" {
		t.Skip("actual bundle acceptance requires REMOTE_GATEWAY_BUNDLE_BIN and REMOTE_GATEWAY_OPENLIST_PLUGIN from the Buildbarn/Incus producer; no local build fallback")
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
		if err = p.cmd.Process.Signal(syscall.SIGTERM); err != nil {
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

const bundlePassword = "bundle-private-ingress-password"

type bundleFixture struct {
	dir, binary, publicURL, controlURL string
	config                             bundle.Config
	client                             *http.Client
	payload                            []byte
	readStarted                        chan struct{}
	releaseRead                        func()
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
			if len(args) > 1 && (string(args[1]) == "worker" || string(args[1]) == "control-plane") {
				out[string(args[1])] = pid
			}
		}
	}
	return out
}

func (f *bundleFixture) ready(t *testing.T, p *bundleProcess) map[string]int {
	t.Helper()
	deadline := time.After(80 * time.Second)
	for {
		children := bundleChildren(p.cmd.Process.Pid)
		if len(children) == 2 {
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
						actual, err := os.Stat(fmt.Sprintf("/proc/%d/exe", pid))
						if err != nil || !os.SameFile(installed, actual) {
							t.Fatalf("%s PID %d did not self-exec installed ELF: %v", role, pid, err)
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
