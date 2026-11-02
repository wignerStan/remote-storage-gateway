package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	processexec "github.com/wignerStan/infra/go/process/exec"
	"github.com/wignerStan/infra/go/process/supervisor"
	port "github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/client"
	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/descriptor"
	dp "github.com/wignerStan/network-data-plane-worker/go/contracts/worker"
	"github.com/wignerStan/remote-storage-gateway/go/contracts/control/broker"
	config "github.com/wignerStan/remote-storage-gateway/go/contracts/vfs"
	workerconfig "github.com/wignerStan/remote-storage-gateway/go/contracts/worker"
	control "github.com/wignerStan/remote-storage-gateway/go/control/ttrpc"
	"github.com/wignerStan/remote-storage-gateway/go/gateway"
	gatewayhttp "github.com/wignerStan/remote-storage-gateway/go/gateway/httpapi"
)

const dataPassword = "data-plane-only-password-1234"
const localPassword = "local-cache-only-password-5678"

type originFixture struct {
	pack                                   []byte
	itemOpens                              atomic.Int32
	smallFiles                             int
	started, canceled, release             chan struct{}
	startedOnce, canceledOnce, releaseOnce sync.Once
	opens, remoteGets, publicRequests      atomic.Int32
	localSocket                            atomic.Value
}

func (f *originFixture) unblock() { f.releaseOnce.Do(func() { close(f.release) }) }
func (f *originFixture) Stat(_ context.Context, r port.ObjectRef) (port.ObjectInfo, error) {
	name := strings.Trim(r.Path, "/")
	if name == "" {
		return port.ObjectInfo{Path: "/", IsDir: true}, nil
	}
	size := int64(len("materialized-through-data-plane"))
	if name == "pack.zip" && f.pack != nil {
		size = int64(len(f.pack))
	} else if name == "stalled" {
		size = 65536
	} else if strings.HasPrefix(name, "small-") && strings.HasSuffix(name, ".bin") {
		index, err := strconv.Atoi(strings.TrimSuffix(strings.TrimPrefix(name, "small-"), ".bin"))
		if err != nil || index < 0 || index >= f.smallFiles {
			return port.ObjectInfo{}, port.ErrNotFound
		}
		size = int64(len("tiny"))
	} else if name == "empty" && f.smallFiles > 0 {
		return port.ObjectInfo{Path: "/empty", IsDir: true}, nil
	} else if name != "item" {
		return port.ObjectInfo{}, port.ErrNotFound
	}
	return port.ObjectInfo{Path: "/" + name, Size: size, ModTime: time.Unix(1700000000, 0), ContentType: "application/octet-stream"}, nil
}
func (f *originFixture) List(ctx context.Context, req port.ListRequest) (port.ListResult, error) {
	if strings.Trim(req.Path, "/") == "empty" {
		return port.ListResult{}, nil
	}
	a, _ := f.Stat(ctx, port.ObjectRef{Path: "item"})
	b, _ := f.Stat(ctx, port.ObjectRef{Path: "stalled"})
	objects := []port.ObjectInfo{a, b}
	if f.pack != nil {
		p, _ := f.Stat(ctx, port.ObjectRef{Path: "pack.zip"})
		objects = append(objects, p)
	}
	for i := 0; i < f.smallFiles; i++ {
		v, _ := f.Stat(ctx, port.ObjectRef{Path: fmt.Sprintf("small-%d.bin", i)})
		objects = append(objects, v)
	}
	return port.ListResult{Objects: objects}, nil
}
func (f *originFixture) Open(ctx context.Context, r port.OpenRequest) (port.ReadPlan, error) {
	info, err := f.Stat(ctx, port.ObjectRef{Path: r.Path})
	if err != nil {
		return port.ReadPlan{}, err
	}
	f.opens.Add(1)
	if strings.Trim(r.Path, "/") == "item" {
		f.itemOpens.Add(1)
	}
	if strings.Trim(r.Path, "/") == "stalled" {
		f.startedOnce.Do(func() { close(f.started) })
		remaining := info.Size - r.Offset
		if r.Length >= 0 && r.Length < remaining {
			remaining = r.Length
		}
		return port.ReadPlan{Info: info, Body: &stalledBody{ctx: ctx, fixture: f, remaining: remaining}}, nil
	}
	data := []byte("materialized-through-data-plane")
	if strings.Trim(r.Path, "/") == "pack.zip" {
		data = f.pack
	}
	if strings.HasPrefix(strings.Trim(r.Path, "/"), "small-") {
		data = []byte("tiny")
	}
	if r.Offset > int64(len(data)) {
		return port.ReadPlan{}, port.ErrInvalid
	}
	data = data[r.Offset:]
	if r.Length >= 0 && r.Length < int64(len(data)) {
		data = data[:r.Length]
	}
	return port.ReadPlan{Info: info, Body: io.NopCloser(bytes.NewReader(data))}, nil
}
func (*originFixture) Put(context.Context, port.PutRequest) (port.PutResult, error) {
	return port.PutResult{}, port.ErrUnsupported
}
func (*originFixture) Delete(context.Context, port.ObjectRef) error { return port.ErrUnsupported }
func (*originFixture) Close(context.Context) error                  { return nil }

type stalledBody struct {
	ctx       context.Context
	fixture   *originFixture
	remaining int64
}

func (b *stalledBody) Read(p []byte) (int, error) {
	select {
	case <-b.ctx.Done():
		b.fixture.canceledOnce.Do(func() { close(b.fixture.canceled) })
		return 0, b.ctx.Err()
	case <-b.fixture.release:
	}
	if b.remaining <= 0 {
		return 0, io.EOF
	}
	if int64(len(p)) > b.remaining {
		p = p[:b.remaining]
	}
	clear(p)
	b.remaining -= int64(len(p))
	return len(p), nil
}
func (*stalledBody) Close() error { return nil }

type fixtureManager struct{ client port.Client }

func (m fixtureManager) Pin(string) (port.Lease, error) { return fixtureLease{m.client}, nil }
func (fixtureManager) Close(context.Context) error      { return nil }

type fixtureLease struct{ client port.Client }

func (l fixtureLease) Client() port.Client { return l.client }
func (fixtureLease) Version() dp.Version {
	return dp.Version{Incarnation: "fixture-worker", Generation: 1}
}
func (fixtureLease) Release() {}

type fixtureControl struct {
	endpoint string
	source   *originFixture
}

func (f fixtureControl) Info(context.Context) (broker.InstanceInfo, error) {
	socket := ""
	if f.source != nil {
		socket, _ = f.source.localSocket.Load().(string)
	}
	return broker.InstanceInfo{Contract: broker.Contract, ID: "approved-worker", Ready: true, DataEndpoint: f.endpoint, LocalDataSocket: socket}, nil
}
func (fixtureControl) Manifest(context.Context) (dp.Manifest, error) {
	return dp.Manifest{}, broker.ErrInvalid
}
func (fixtureControl) Observed(context.Context) ([]dp.Observed, error) { return nil, broker.ErrInvalid }
func (fixtureControl) Descriptor(context.Context, string) (descriptor.Record, error) {
	return descriptor.Record{}, broker.ErrInvalid
}
func (fixtureControl) Replace(context.Context, string, dp.Version, dp.Selection) (dp.Version, error) {
	return dp.Version{}, broker.ErrInvalid
}

func setup(t *testing.T) (config.ProcessConfig, *originFixture) { return setupTransport(t, false) }
func setupTransport(t *testing.T, unix bool) (config.ProcessConfig, *originFixture) {
	t.Helper()
	directory, err := os.MkdirTemp("/tmp", "trade-vfs-component-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(directory) })
	source := &originFixture{started: make(chan struct{}), canceled: make(chan struct{}), release: make(chan struct{})}
	t.Cleanup(source.unblock)
	flow, err := gateway.New(workerconfig.RuntimeConfig{Username: "data", Worker: dp.Bootstrap{Clients: []dp.Selection{{Name: "origin"}}}, Mounts: []workerconfig.Mount{{Name: "bucket", Client: "origin", RemoteClient: "origin", ReadOnly: true}}}, fixtureManager{source}, []byte(dataPassword))
	if err != nil {
		t.Fatal(err)
	}
	router := gin.New()
	gatewayhttp.RegisterGin(router, flow, http.NotFoundHandler(), http.NotFoundHandler())
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/remote-dav/") {
			source.remoteGets.Add(1)
		}
		router.ServeHTTP(w, r)
	})
	var data *httptest.Server
	if unix {
		data = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { source.publicRequests.Add(1); handler.ServeHTTP(w, r) }))
	} else {
		data = httptest.NewServer(handler)
	}
	advertised := data.URL
	localSocket := ""
	if unix {
		// The public TLS server's certificate does not cover localhost and is not
		// in the process trust store. Local reads must never use that listener.
		advertised = strings.Replace(data.URL, "127.0.0.1", "localhost", 1)
		localSocket = filepath.Join(directory, "raw.sock")
		listener, e := net.Listen("unix", localSocket)
		if e != nil {
			t.Fatal(e)
		}
		if e = os.Chmod(localSocket, 0600); e != nil {
			t.Fatal(e)
		}
		raw := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !strings.HasPrefix(r.URL.Path, "/remote-dav/") || (r.Method != "GET" && r.Method != "HEAD" && r.Method != "PROPFIND" && r.Method != "OPTIONS") {
				http.NotFound(w, r)
				return
			}
			handler.ServeHTTP(w, r)
		})}
		go func() { _ = raw.Serve(listener) }()
		t.Cleanup(func() { _ = raw.Close() })
	}
	source.localSocket.Store(localSocket)
	t.Cleanup(func() { source.unblock(); data.Close(); _ = flow.Close(context.Background()) })
	socket := filepath.Join(directory, "worker.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.Chmod(socket, 0600); err != nil {
		t.Fatal(err)
	}
	metadata, err := control.NewWorkerServer(fixtureControl{endpoint: advertised, source: source})
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = metadata.Serve(listener) }()
	t.Cleanup(func() { _ = metadata.Close(context.Background()) })
	dataFile, localFile := filepath.Join(directory, "data-auth"), filepath.Join(directory, "local-auth")
	if err = os.WriteFile(dataFile, []byte(dataPassword), 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(localFile, []byte(localPassword), 0600); err != nil {
		t.Fatal(err)
	}
	return config.ProcessConfig{InstanceID: "materializer", ControlSocket: filepath.Join(directory, "vfs.sock"), Listen: "127.0.0.1:0", WorkerControlSocket: socket, WorkerInstanceID: "approved-worker", WorkerLocalDataSocket: localSocket, DataEndpoint: advertised + "/remote-dav/bucket/", DataUsername: "data", DataPasswordFile: dataFile, LocalUsername: "cache", LocalPasswordFile: localFile, Directory: filepath.Join(directory, "cache"), MaxBytes: 1 << 20, MaxAgeSeconds: 60}, source
}
func start(t *testing.T, c config.ProcessConfig) *Runtime {
	t.Helper()
	app, err := New(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	if err = app.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := app.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	return app
}
func wait(t *testing.T, ch <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(3 * time.Second):
		t.Fatalf("%s did not settle", name)
	}
}

func TestReadOnlyMaterializerAuthenticationAndDataPlaneOriginBinding(t *testing.T) {
	c, source := setup(t)
	app := start(t, c)
	info, err := app.Info(context.Background())
	if err != nil || !info.Ready {
		t.Fatal(info, err)
	}
	for _, test := range []struct {
		username, password, method string
		status                     int
	}{{"data", dataPassword, "GET", 401}, {"cache", localPassword, "PUT", 405}, {"cache", localPassword, "GET", 200}} {
		request, _ := http.NewRequest(test.method, info.DataEndpoint+"item", nil)
		request.SetBasicAuth(test.username, test.password)
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(response.Body)
		_ = response.Body.Close()
		if err != nil || response.StatusCode != test.status {
			t.Fatal(response.StatusCode, string(body), err)
		}
	}
	if source.remoteGets.Load() != 1 {
		t.Fatal("data payload bypassed or repeated remote HTTP fetch", source.remoteGets.Load())
	}
	for _, change := range []func(*config.ProcessConfig){func(c *config.ProcessConfig) {
		c.DataEndpoint = strings.Replace(c.DataEndpoint, "/remote-dav/", "/dav/", 1)
	}, func(c *config.ProcessConfig) { c.WorkerInstanceID = "other-worker" }, func(c *config.ProcessConfig) { c.DataEndpoint = "http://127.0.0.1:1/remote-dav/bucket/" }, func(c *config.ProcessConfig) { c.Listen = "0.0.0.0:0" }, func(c *config.ProcessConfig) { c.LocalPasswordFile = c.DataPasswordFile }, func(c *config.ProcessConfig) { c.MaxConcurrentRequests = 1025 }, func(c *config.ProcessConfig) { c.MaxOpenFiles = -1 }, func(c *config.ProcessConfig) { c.MaxCacheEntries = -1 }, func(c *config.ProcessConfig) { c.MaxDirectoryEntries = -1 }} {
		invalid := c
		change(&invalid)
		bad, err := New(context.Background(), invalid)
		if err == nil {
			_ = bad.Close(context.Background())
			t.Fatal("invalid materializer admitted")
		}
	}
	if _, err = ReadConfig(strings.NewReader(`{"backend":"drive","token":"private"}`)); err == nil {
		t.Fatal("provider configuration accepted")
	}
}

func TestCancelCachedReadKeepsPeerAndRetirementSettlesLastOwner(t *testing.T) {
	for _, unix := range []bool{false, true} {
		t.Run(fmt.Sprintf("unix=%t", unix), func(t *testing.T) { cancelCachedRead(t, unix) })
	}
}
func cancelCachedRead(t *testing.T, unix bool) {
	c, source := setupTransport(t, unix)
	app := start(t, c)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	first, err := app.service.OpenFile(ctx, "stalled", os.O_RDONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	peer, err := app.service.OpenFile(context.Background(), "stalled", os.O_RDONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	if _, err = peer.Seek(1, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	firstDone, peerDone := make(chan error, 1), make(chan error, 1)
	go func() { _, err := first.Read(make([]byte, 1)); firstDone <- err }()
	go func() { _, err := peer.Read(make([]byte, 1)); peerDone <- err }()
	wait(t, source.started, "actual Data Plane raw GET")
	cancel()
	select {
	case err = <-firstDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("caller cancellation retained cache wait")
	}
	if err = first.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err = <-peerDone:
		t.Fatal("peer was terminated", err)
	default:
	}
	select {
	case <-source.canceled:
		t.Fatal("one canceled read stopped shared Data Plane transfer")
	default:
	}
	closeCtx, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	if err = app.Close(closeCtx); err != nil {
		source.unblock()
		t.Fatal(err)
	}
	select {
	case <-peerDone:
	case <-time.After(time.Second):
		source.unblock()
		t.Fatal("retirement did not settle peer")
	}
	wait(t, source.canceled, "owned Data Plane request cancellation")
	entries, err := os.ReadDir(c.Directory)
	if err != nil || len(entries) != 0 {
		t.Fatal("cache directory survived retired handles", entries, err)
	}
}

func TestMaterializerProcessLeavesNoLateCacheWritesOrDescriptors(t *testing.T) {
	if os.Getenv("GATEWAY_VFS_LIFECYCLE_PROBE") == "1" {
		c, err := ReadConfig(os.Stdin)
		if err != nil {
			t.Fatal(err)
		}
		request := func(app *Runtime) {
			info, err := app.Info(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			q, _ := http.NewRequest("GET", info.DataEndpoint+"item", nil)
			q.SetBasicAuth(c.LocalUsername, localPassword)
			response, err := http.DefaultClient.Do(q)
			if err != nil {
				t.Fatal(err)
			}
			_, err = io.Copy(io.Discard, response.Body)
			_ = response.Body.Close()
			if err != nil || response.StatusCode != 200 {
				t.Fatal(err, response.StatusCode)
			}
		}
		run := func(i int) {
			cfg := c
			cfg.InstanceID = fmt.Sprintf("probe-%d", i)
			cfg.ControlSocket = c.ControlSocket + fmt.Sprint(i)
			app, err := New(context.Background(), cfg)
			if err != nil {
				t.Fatal(err)
			}
			if err = app.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			request(app)
			if err = app.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
		}
		run(0)
		targets := func() map[string]string {
			entries, _ := os.ReadDir("/proc/self/fd")
			out := map[string]string{}
			for _, entry := range entries {
				target, _ := os.Readlink("/proc/self/fd/" + entry.Name())
				out[entry.Name()] = target
			}
			return out
		}
		beforeTargets := targets()
		before, err := os.ReadDir("/proc/self/fd")
		if err != nil {
			t.Fatal(err)
		}
		for i := 1; i <= 5; i++ {
			run(i)
		}
		// The old default handle grace was five seconds. Observe beyond that point
		// while the process lives, so deletion cannot hide a deferred metadata write.
		time.Sleep(6 * time.Second)
		after, err := os.ReadDir("/proc/self/fd")
		if err != nil {
			t.Fatal(err)
		}
		if len(after) > len(before)+2 {
			t.Fatalf("descriptor growth %d -> %d before=%v after=%v", len(before), len(after), beforeTargets, targets())
		}
		entries, err := os.ReadDir(c.Directory)
		if err != nil || len(entries) != 0 {
			t.Fatal("late cache work survived close", entries, err)
		}
		fmt.Printf("vfs-lifecycle-probe-complete fd=%d->%d\n", len(before), len(after))
		return
	}
	for _, unix := range []bool{false, true} {
		t.Run(fmt.Sprintf("unix=%t", unix), func(t *testing.T) {
			c, _ := setupTransport(t, unix)
			payload, err := json.Marshal(c)
			if err != nil {
				t.Fatal(err)
			}
			owner, err := supervisor.New(supervisor.Options{MaxChildren: 1, GracePeriod: time.Second})
			if err != nil {
				t.Fatal(err)
			}
			defer owner.Close(context.Background())
			command := processexec.NewStartSpec(os.Args[0], "-test.run=^TestMaterializerProcessLeavesNoLateCacheWritesOrDescriptors$", "-test.v").WithExtraEnv("GATEWAY_VFS_LIFECYCLE_PROBE", "1").WithExtraEnv("GORACE", "atexit_sleep_ms=0").WithStdin(bytes.NewReader(payload))
			child, err := owner.Start(context.Background(), supervisor.Spec{Command: command})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			if err = child.Wait(ctx); err != nil {
				t.Fatalf("materializer subprocess: %v %s%s", err, child.Diagnostics().Stdout, child.Diagnostics().Stderr)
			}
			output := append(child.Diagnostics().Stdout, child.Diagnostics().Stderr...)
			if !bytes.Contains(output, []byte("vfs-lifecycle-probe-complete")) {
				t.Fatal("subprocess probe did not finish")
			}
			for _, line := range strings.Split(string(output), "\n") {
				if strings.HasPrefix(line, "vfs-lifecycle-probe-complete") {
					t.Log(line)
				}
			}
			for _, forbidden := range []string{"close after grace period failed", "failed to write metadata"} {
				if bytes.Contains(output, []byte(forbidden)) {
					t.Fatalf("deferred cache work: %s", output)
				}
			}
		})
	}
}

func TestUnixDataSocketPreservesPublicTLSIdentityAndCache(t *testing.T) {
	c, source := setupTransport(t, true)
	bad := c
	bad.WorkerLocalDataSocket = filepath.Join(filepath.Dir(c.WorkerLocalDataSocket), "different.sock")
	if app, err := New(context.Background(), bad); err == nil {
		_ = app.Close(context.Background())
		t.Fatal("unobserved data socket admitted")
	}
	app := start(t, c)
	info, err := app.Info(context.Background())
	if err != nil || !info.Ready {
		t.Fatal(info, err)
	}
	for _, ranged := range []bool{false, true} {
		req, _ := http.NewRequest("GET", info.DataEndpoint+"item", nil)
		req.SetBasicAuth(c.LocalUsername, localPassword)
		want := "materialized-through-data-plane"
		status := 200
		if ranged {
			req.Header.Set("Range", "bytes=2-6")
			want = want[2:7]
			status = 206
		}
		response, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(response.Body)
		_ = response.Body.Close()
		if err != nil || response.StatusCode != status || string(body) != want {
			t.Fatal(response.StatusCode, string(body), err)
		}
	}
	if source.remoteGets.Load() != 1 || source.publicRequests.Load() != 0 {
		t.Fatal("local payload used public TLS or repeated cache miss", source.remoteGets.Load(), source.publicRequests.Load())
	}
	// Existing cached bytes are not authority to keep serving a changed worker.
	source.localSocket.Store(bad.WorkerLocalDataSocket)
	if info, err := app.Info(context.Background()); err != nil || info.Ready {
		t.Fatal("changed data socket retained readiness", info, err)
	}
	req, _ := http.NewRequest("GET", info.DataEndpoint+"item", nil)
	req.SetBasicAuth(c.LocalUsername, localPassword)
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusServiceUnavailable {
		t.Fatal("changed data socket retained cached access", response.StatusCode)
	}
	if source.remoteGets.Load() != 1 || source.publicRequests.Load() != 0 {
		t.Fatal("identity failure escaped to network")
	}
	if err := app.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(c.Directory)
	if err != nil || len(entries) != 0 {
		t.Fatal("Unix cache survived retirement", entries, err)
	}
}
