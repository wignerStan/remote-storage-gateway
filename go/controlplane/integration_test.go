//go:build dataplane_l7

package controlplane_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	carrier "github.com/wignerStan/infra/go/network/actions/ttrpc"
	processexec "github.com/wignerStan/infra/go/process/exec"
	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/descriptor/admission"
	dp "github.com/wignerStan/network-data-plane-worker/go/contracts/worker"
	"github.com/wignerStan/remote-storage-gateway/go/contracts/control/broker"
	vfsconfig "github.com/wignerStan/remote-storage-gateway/go/contracts/vfs"
	workerconfig "github.com/wignerStan/remote-storage-gateway/go/contracts/worker"
	control "github.com/wignerStan/remote-storage-gateway/go/control/ttrpc"
	"github.com/wignerStan/remote-storage-gateway/go/controlplane"
	"golang.org/x/net/webdav"
)

func TestManagedWorkerAndIndependentMaterializer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	_, source, _, _ := runtime.Caller(0)
	root := filepath.Dir(filepath.Dir(source))
	bin := t.TempDir()
	run := func(args ...string) processexec.RunOutput {
		t.Helper()
		spec := processexec.NewSpec(filepath.Join(runtime.GOROOT(), "bin", "go"), args...).WithTimeout(time.Minute).WithEnv(processexec.EnvInheritAllButRedacted())
		spec.Dir = root
		out, e := processexec.RunContext(ctx, spec)
		if e != nil {
			t.Fatalf("source build: %v %s%s", e, out.StdoutString(), out.StderrString())
		}
		return out
	}
	plugin := filepath.Join(bin, "client-openlist.so")
	run("build", "-buildmode=c-shared", "-buildvcs=false", "-ldflags", "-X main.buildDigest=sha256:"+strings.Repeat("7", 64), "-o", plugin, "github.com/OpenListTeam/OpenList/v4/cmd/openlist-client-plugin")
	workerBinary := filepath.Join(bin, "storage-worker")
	run("build", "-tags", "dataplane_l7", "-o", workerBinary, "./cmd/storage-worker")
	vfsBinary := filepath.Join(bin, "storage-vfs")
	run("build", "-tags", "dataplane_l7", "-o", vfsBinary, "./cmd/storage-vfs")
	// Inspect in a different executable: even this test's CP process never imports
	// nativeclient/pluginruntime/cloud/vfs implementation packages.
	inspector := filepath.Join(bin, "inspect")
	run("build", "-tags", "dataplane_l7", "-o", inspector, "github.com/wignerStan/network-data-plane-worker/go/cmd/network-data-plane-worker")
	elf, e := os.Open(plugin)
	if e != nil {
		t.Fatal(e)
	}
	hash := sha256.New()
	_, e = io.Copy(hash, elf)
	elf.Close()
	if e != nil {
		t.Fatal(e)
	}
	digest := hex.EncodeToString(hash.Sum(nil))
	spec := processexec.NewSpec(inspector, "--inspect-artifact", plugin, "--artifact-sha256", digest, "--plugin-id", "client.openlist", "--language", "go").WithTimeout(time.Minute)
	out, e := processexec.RunContext(ctx, spec)
	if e != nil {
		t.Fatalf("separate passive inspector: %v %s", e, out.StderrString())
	}
	record, e := admission.Decode(bytes.TrimSuffix(out.Stdout, []byte("\n")))
	if e != nil {
		t.Fatal(e)
	}
	payload := bytes.Repeat([]byte("separate-process-remote-data\n"), 8192)
	originDir := t.TempDir()
	if e = os.WriteFile(filepath.Join(originDir, "object"), payload, 0600); e != nil {
		t.Fatal(e)
	}
	var originReads atomic.Int32
	dav := &webdav.Handler{FileSystem: webdav.Dir(originDir), LockSystem: webdav.NewMemLS()}
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			originReads.Add(1)
		}
		if r.Header.Get("Authorization") != "" {
			t.Error("DP or local VFS credential reached origin")
		}
		dav.ServeHTTP(w, r)
	}))
	defer origin.Close()
	dir, e := os.MkdirTemp("/tmp", "trade-three-axes-")
	if e != nil {
		t.Fatal(e)
	}
	defer os.RemoveAll(dir)
	ingress := filepath.Join(dir, "ingress")
	local := filepath.Join(dir, "local-auth")
	const dataPassword = "dp-only-ingress-password"
	const localPassword = "private-local-vfs-password"
	for path, value := range map[string]string{ingress: dataPassword, local: localPassword} {
		if e = os.WriteFile(path, []byte(value), 0600); e != nil {
			t.Fatal(e)
		}
	}
	// Publication uses exact plugin-owned public bytes; CP never edits its schema.
	marshal := func(v any) json.RawMessage {
		t.Helper()
		b, e := json.Marshal(v)
		if e != nil {
			t.Fatal(e)
		}
		return b
	}
	base := dp.Selection{Name: "origin", PluginID: "client.openlist", DescriptorDigest: record.Digest, ImplementationID: "webdav", Public: marshal(map[string]string{"address": origin.URL})}
	cached := base
	cached.Name = "cached"
	address := func() string {
		t.Helper()
		listener, e := net.Listen("tcp", "127.0.0.1:0")
		if e != nil {
			t.Fatal(e)
		}
		addr := listener.Addr().String()
		listener.Close()
		return addr
	}
	workerAddress := address()
	vfsAddress := address()
	workerEndpoint := "http://" + workerAddress
	activate := cached
	activate.Public = marshal(map[string]string{"address": "http://" + vfsAddress + "/dav/", "username": "cache-user"})
	activate.SecretRefs = map[string]string{"password": "cache-auth"}
	workerSocket := filepath.Join(dir, "worker")
	brokerSocket := filepath.Join(dir, "broker")
	vfsSocket := filepath.Join(dir, "vfs")
	process := workerconfig.ProcessConfig{RuntimeConfig: workerconfig.RuntimeConfig{Listen: workerAddress, Username: "dp-user", AuthSecret: "ingress", ClientSecretRefs: []string{"cache-auth"}, Worker: dp.Bootstrap{Schema: dp.BootstrapSchema, Artifacts: []dp.Artifact{{Path: plugin, SHA256: digest, PluginID: "client.openlist", DescriptorDigest: record.Digest, Language: "go"}}, Clients: []dp.Selection{base, cached}, SecretFiles: map[string]string{"ingress": ingress, "cache-auth": local}}, Mounts: []workerconfig.Mount{{Name: "files", Client: "cached", RemoteClient: "origin", ReadOnly: true}}}, InstanceID: "worker-one", ControlSocket: workerSocket, BrokerSocket: brokerSocket, DataEndpoint: workerEndpoint}
	method := "/build.bazel.remote.execution.v2.Execution/Execute"
	manager, e := controlplane.New(controlplane.Config{Workers: []controlplane.WorkerSpec{{Executable: workerBinary, Config: process}}, VFS: []controlplane.VFSSpec{{Executable: vfsBinary, ActivateClient: &activate, Config: vfsconfig.ProcessConfig{InstanceID: "vfs-one", WorkerInstanceID: "worker-one", WorkerControlSocket: workerSocket, ControlSocket: vfsSocket, Listen: vfsAddress, DataEndpoint: "/remote-dav/files/", DataUsername: "dp-user", DataPasswordFile: ingress, LocalUsername: "cache-user", LocalPasswordFile: local, Directory: filepath.Join(dir, "cache"), MaxBytes: 1 << 20, MaxAgeSeconds: 60}}}, MetadataNames: []string{"x-request-id"}, Rules: []controlplane.Rule{{InstanceID: "worker-one", Authority: "worker.invalid", Decision: broker.Decision{RouteID: "build", WorkerDataEndpoint: workerEndpoint, ApprovedUpstreamID: "buildbarn", TTLSeconds: 30, MaxMessageBytes: 1 << 20, MaxInflightBytes: 2 << 20, Methods: []string{method}}}}})
	if e != nil {
		t.Fatal(e)
	}
	defer manager.Close(context.Background())
	startup, startCancel := context.WithCancel(ctx)
	if e = manager.Start(startup); e != nil {
		startCancel()
		t.Fatal(e)
	}
	startCancel()
	instances := manager.Instances()
	if len(instances) != 2 || instances[0].PID <= 0 || instances[1].PID <= 0 || instances[0].PID == instances[1].PID || instances[0].PID == os.Getpid() || instances[1].PID == os.Getpid() {
		t.Fatalf("axes not separate processes: %+v", instances)
	}
	for _, instance := range instances {
		if instance.Phase != "ready" {
			t.Fatalf("startup context killed child: %+v", instance)
		}
	}
	raw, e := control.DialDecision(ctx, brokerSocket, []string{"x-request-id"})
	if e != nil {
		t.Fatal(e)
	}
	defer raw.Close(context.Background())
	beforeDecision := originReads.Load()
	grant, e := raw.Decide(ctx, broker.TransferIntent{Method: method, Authority: "worker.invalid", Principal: "forged", Metadata: map[string]string{"x-request-id": "metadata-only"}})
	if e != nil || grant.ApprovedUpstreamID != "buildbarn" {
		t.Fatal("actual CP broker grant", e)
	}
	wire, e := carrier.Dial(ctx, brokerSocket, carrier.Limits{MaxPayload: 1 << 20, MaxInflight: 16, Timeout: time.Second})
	if e != nil {
		t.Fatal(e)
	}
	defer wire.Close(context.Background())
	if _, e = wire.Call(ctx, "broker.decide", []byte(`{"method":"/build.Execution/Execute","authority":"worker.invalid","payload":"object-bytes"}`)); e == nil {
		t.Fatal("CP accepted file payload field")
	}
	if originReads.Load() != beforeDecision {
		t.Fatal("metadata decision initiated object I/O")
	}
	fetch := func() []byte {
		t.Helper()
		request, e := http.NewRequestWithContext(ctx, "GET", workerEndpoint+"/dav/files/object", nil)
		if e != nil {
			t.Fatal(e)
		}
		request.SetBasicAuth("dp-user", dataPassword)
		response, e := http.DefaultClient.Do(request)
		if e != nil {
			t.Fatal(e)
		}
		data, e := io.ReadAll(response.Body)
		response.Body.Close()
		if e != nil || response.StatusCode != 200 {
			t.Fatalf("public DP read %d %v", response.StatusCode, e)
		}
		return data
	}
	if got := fetch(); !bytes.Equal(got, payload) {
		t.Fatal("materialized public bytes differ")
	}
	firstReads := originReads.Load()
	if firstReads == 0 {
		t.Fatal("cache miss never reached remote")
	}
	if got := fetch(); !bytes.Equal(got, payload) {
		t.Fatal("cache hit public bytes differ")
	}
	if originReads.Load() != firstReads {
		t.Fatal("second public DP read bypassed independent cache")
	}
	request, e := http.NewRequestWithContext(ctx, "GET", workerEndpoint+"/remote-dav/files/object", nil)
	if e != nil {
		t.Fatal(e)
	}
	request.SetBasicAuth("dp-user", dataPassword)
	response, e := http.DefaultClient.Do(request)
	if e != nil {
		t.Fatal(e)
	}
	_, e = io.Copy(io.Discard, response.Body)
	response.Body.Close()
	if e != nil || response.StatusCode != 200 || originReads.Load() <= firstReads {
		t.Fatal("remote alias looped through cache")
	}

	for _, method := range []string{"PUT", "DELETE"} {
		request, e := http.NewRequestWithContext(ctx, method, workerEndpoint+"/dav/files/object", strings.NewReader("denied"))
		if e != nil {
			t.Fatal(e)
		}
		request.SetBasicAuth("dp-user", dataPassword)
		response, e := http.DefaultClient.Do(request)
		if e != nil {
			t.Fatal(e)
		}
		response.Body.Close()
		if response.StatusCode != http.StatusForbidden {
			t.Fatalf("cached readonly %s admitted: %d", method, response.StatusCode)
		}
	}
	if e = manager.Close(ctx); e != nil {
		t.Fatal(e)
	}
	for _, instance := range instances {
		if e = syscall.Kill(instance.PID, 0); !errors.Is(e, syscall.ESRCH) {
			t.Fatalf("child not reaped %s: %v", instance.Kind, e)
		}
	}
}
