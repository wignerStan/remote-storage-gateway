package worker

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/descriptor"
	dp "github.com/wignerStan/network-data-plane-worker/go/contracts/worker"
	"github.com/wignerStan/remote-storage-gateway/go/contracts/control/broker"
	config "github.com/wignerStan/remote-storage-gateway/go/contracts/worker"
	control "github.com/wignerStan/remote-storage-gateway/go/control/ttrpc"
)

func TestMain(m *testing.M) {
	if len(os.Args) == 2 && os.Args[1] == "--config=-" {
		os.Exit(helper())
	}
	os.Exit(m.Run())
}
func helper() int {
	var cfg config.ProcessConfig
	if json.NewDecoder(io.LimitReader(os.Stdin, 1<<20)).Decode(&cfg) != nil {
		return 2
	}
	if strings.HasPrefix(cfg.InstanceID, "observe-proxy") {
		snapshot := map[string]environmentValue{}
		for _, key := range []string{"HTTP_PROXY", "HTTPS_PROXY", "NO_PROXY", "http_proxy", "https_proxy", "no_proxy", "WORKER_PRIVATE_SECRET"} {
			value, present := os.LookupEnv(key)
			snapshot[key] = environmentValue{value, present}
		}
		if json.NewEncoder(os.Stdout).Encode(snapshot) != nil {
			return 6
		}
	}
	listener, e := net.Listen("unix", cfg.ControlSocket)
	if e != nil {
		return 3
	}
	defer listener.Close()
	if os.Chmod(cfg.ControlSocket, 0600) != nil {
		return 4
	}
	server, e := control.NewWorkerServer(helperControl{cfg.InstanceID})
	if e != nil {
		return 5
	}
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	<-ctx.Done()
	_ = server.Close(context.Background())
	<-done
	return 0
}

type helperControl struct{ id string }

func (h helperControl) Info(context.Context) (broker.InstanceInfo, error) {
	return broker.InstanceInfo{Contract: broker.Contract, ID: h.id, Ready: h.id != "never-ready", DataEndpoint: "http://127.0.0.1:1"}, nil
}
func (h helperControl) Manifest(context.Context) (dp.Manifest, error) {
	return dp.Manifest{Schema: dp.CapabilitySchema}, nil
}
func (h helperControl) Observed(context.Context) ([]dp.Observed, error) { return []dp.Observed{}, nil }
func (h helperControl) Descriptor(context.Context, string) (descriptor.Record, error) {
	return descriptor.Record{}, broker.ErrInvalid
}
func (h helperControl) Replace(context.Context, string, dp.Version, dp.Selection) (dp.Version, error) {
	return dp.Version{}, broker.ErrInvalid
}
func testBox(t *testing.T, id string) (*Box, string) { return testBoxWithProxy(t, id, nil) }
func testBoxWithProxy(t *testing.T, id string, proxy ProxyEnvironment) (*Box, string) {
	t.Helper()
	dir, e := os.MkdirTemp("/tmp", "trade-box-")
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	executable, e := os.Executable()
	if e != nil {
		t.Fatal(e)
	}
	b, e := New(Options{Executable: executable, ProxyEnvironment: proxy, Config: config.ProcessConfig{InstanceID: id, ControlSocket: filepath.Join(dir, "rpc")}, ReadyTimeout: 3 * time.Second, StopTimeout: time.Second})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if e := b.Close(ctx); e != nil {
			t.Error(e)
		}
	})
	return b, dir
}
func TestBoxOwnsIndependentProcessAndTypedControl(t *testing.T) {
	b, _ := testBox(t, "worker-one")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if e := b.Start(ctx); e != nil {
		t.Fatal(e)
	}
	cancel()
	if state := b.State(); state.Phase != "ready" || state.PID <= 0 || state.PID == os.Getpid() {
		t.Fatalf("not independent: %+v", state)
	}
	ctl, e := b.Control()
	if e != nil {
		t.Fatal(e)
	}
	info, e := ctl.Info(context.Background())
	if e != nil || info.ID != "worker-one" || !info.Ready {
		t.Fatal("startup context killed child", e)
	}
	if e = b.SetConfig(config.ProcessConfig{}); e == nil {
		t.Fatal("running config replaced without control admission")
	}
	var group sync.WaitGroup
	for n := 0; n < 8; n++ {
		group.Add(1)
		go func() {
			defer group.Done()
			if e := b.Close(context.Background()); e != nil {
				t.Error(e)
			}
		}()
	}
	group.Wait()
	select {
	case <-b.observerDone:
	default:
		t.Fatal("Close returned before observer joined")
	}
	if b.State().Phase != "closed" {
		t.Fatal(b.State())
	}
	if _, e = b.Control(); e == nil {
		t.Fatal("retired child control remained available")
	}
}
func TestBoxCanceledReadinessReapsActualChild(t *testing.T) {
	b, _ := testBox(t, "never-ready")
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- b.Start(ctx) }()
	deadline := time.Now().Add(3 * time.Second)
	for b.State().PID == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	pid := b.State().PID
	if pid <= 0 {
		cancel()
		t.Fatal("child never started")
	}
	cancel()
	select {
	case e := <-result:
		if !errors.Is(e, context.Canceled) {
			t.Fatalf("startup cancellation: %v", e)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("startup child was abandoned")
	}
	if e := syscall.Kill(pid, 0); !errors.Is(e, syscall.ESRCH) {
		t.Fatalf("child was not reaped: %v", e)
	}
}
func TestBoxRefusesExistingControlPath(t *testing.T) {
	b, _ := testBox(t, "worker-one")
	path := b.options.Config.ControlSocket
	if e := os.WriteFile(path, []byte("existing"), 0600); e != nil {
		t.Fatal(e)
	}
	if e := b.Start(context.Background()); e == nil {
		t.Fatal("existing control path adopted")
	}
	got, e := os.ReadFile(path)
	if e != nil || string(got) != "existing" {
		t.Fatal("existing path changed")
	}
}

func TestBoxCloseJoinsConcurrentStartup(t *testing.T) {
	b, _ := testBox(t, "never-ready")
	result := make(chan error, 1)
	go func() { result <- b.Start(context.Background()) }()
	deadline := time.Now().Add(3 * time.Second)
	for b.State().PID == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	pid := b.State().PID
	if pid <= 0 {
		t.Fatal("child did not start")
	}
	if e := b.Close(context.Background()); e != nil {
		t.Fatal(e)
	}
	select {
	case <-b.startDone:
	default:
		t.Fatal("Close did not join startup registration")
	}
	select {
	case e := <-result:
		if e == nil {
			t.Fatal("closing child reported ready")
		}
	case <-time.After(time.Second):
		t.Fatal("startup escaped owned close")
	}
	if e := syscall.Kill(pid, 0); !errors.Is(e, syscall.ESRCH) {
		t.Fatalf("starting child survived Close: %v", e)
	}
}

type environmentValue struct {
	Value   string
	Present bool
}

func TestBoxPassesOnlyExplicitProxyEnvironment(t *testing.T) {
	for _, key := range []string{"HTTP_PROXY", "HTTPS_PROXY", "NO_PROXY", "http_proxy", "https_proxy", "no_proxy"} {
		t.Setenv(key, "parent-fixture-proxy")
	}
	t.Setenv("WORKER_PRIVATE_SECRET", "never-in-child")
	for _, explicit := range []bool{false, true} {
		name := "nil"
		if explicit {
			name = "explicit"
		}
		t.Run(name, func(t *testing.T) {
			proxy := ProxyEnvironment(nil)
			if explicit {
				proxy = ProxyEnvironment{"HTTP_PROXY": "http://fixture.invalid:3128", "HTTPS_PROXY": "", "no_proxy": "127.0.0.1"}
			}
			b, _ := testBoxWithProxy(t, "observe-proxy-"+name, proxy)
			if explicit {
				proxy["HTTP_PROXY"] = "http://mutated.invalid:1"
			}
			if e := b.Start(context.Background()); e != nil {
				t.Fatal(e)
			}
			var observed map[string]environmentValue
			if e := json.Unmarshal(b.child.Diagnostics().Stdout, &observed); e != nil {
				t.Fatal("subprocess did not report environment", e)
			}
			if explicit && observed["HTTP_PROXY"].Value != "http://fixture.invalid:3128" {
				t.Fatal("caller mutation changed admitted child proxy")
			}
			for key, value := range observed {
				expected, ok := b.options.ProxyEnvironment[key]
				if value.Present != ok || ok && value.Value != expected {
					t.Fatalf("unexpected environment for %s: present=%v", key, value.Present)
				}
			}
			if observed["WORKER_PRIVATE_SECRET"].Present {
				t.Fatal("unrelated parent secret inherited")
			}
		})
	}
	executable, e := os.Executable()
	if e != nil {
		t.Fatal(e)
	}
	valid, _ := testBox(t, "valid-env-check")
	if _, e = New(Options{Executable: executable, Config: valid.options.Config, ProxyEnvironment: ProxyEnvironment{"WORKER_PRIVATE_SECRET": "forbidden"}}); e == nil {
		t.Fatal("arbitrary environment grant accepted")
	}
}
