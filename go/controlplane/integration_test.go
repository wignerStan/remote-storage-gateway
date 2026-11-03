//go:build dataplane_l7

package controlplane_test

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	carrier "github.com/wignerStan/infra/go/network/rpc/ttrpc"
	processexec "github.com/wignerStan/infra/go/process/exec"
	supervision "github.com/wignerStan/infra/go/process/supervisor"
	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/descriptor/admission"
	storageipc "github.com/wignerStan/network-data-plane-worker/go/contracts/storageipc"
	dp "github.com/wignerStan/network-data-plane-worker/go/contracts/worker"
	rclonebox "github.com/wignerStan/rclone/go/box"
	"github.com/wignerStan/remote-storage-gateway/go/contracts/control/broker"
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
	rcloneBinary := os.Getenv("REMOTE_GATEWAY_RCLONE_BIN")
	if rcloneBinary == "" {
		t.Skip("requires separately source-built rclone CLI")
	}
	if !filepath.IsAbs(rcloneBinary) {
		t.Fatal("absolute admitted rclone binary required")
	}
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
	// Distinct bodies and lengths detect cross-file buffer/cache aliasing. Keep
	// the fixture small enough for both complete passes to fit the admitted cache.
	const tinyFiles = 128
	const tinyConcurrency = 8
	tinyDirectory := filepath.Join(originDir, "tiny")
	if e = os.Mkdir(tinyDirectory, 0700); e != nil {
		t.Fatal(e)
	}
	tinyContents := make(map[string][]byte, tinyFiles)
	tinyReads := make(map[string]*atomic.Int32, tinyFiles)
	for i := range tinyFiles {
		name := fmt.Sprintf("item-%03d.txt", i)
		data := []byte(fmt.Sprintf("tiny-%03d:%s\n", i, strings.Repeat("x", i%32+1)))
		if e = os.WriteFile(filepath.Join(tinyDirectory, name), data, 0600); e != nil {
			t.Fatal(e)
		}
		tinyContents[name] = data
		tinyReads["/tiny/"+name] = new(atomic.Int32)
	}
	var originReads atomic.Int32
	var tinyActive, tinyPeak atomic.Int32
	dav := &webdav.Handler{FileSystem: webdav.Dir(originDir), LockSystem: webdav.NewMemLS()}
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			originReads.Add(1)
			if reads := tinyReads[r.URL.Path]; reads != nil {
				reads.Add(1)
				active := tinyActive.Add(1)
				defer tinyActive.Add(-1)
				for {
					previous := tinyPeak.Load()
					if active <= previous || tinyPeak.CompareAndSwap(previous, active) {
						break
					}
				}
			}
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
	_, publicPort, e := net.SplitHostPort(workerAddress)
	if e != nil {
		t.Fatal(e)
	}
	workerEndpoint := "https://gateway.test:" + publicPort
	certificate, key, roots := gatewayDNSCertificate(t)
	certificatePath := filepath.Join(dir, "gateway.crt")
	keyPath := filepath.Join(dir, "gateway.key")
	if e = os.WriteFile(certificatePath, certificate, 0644); e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(keyPath, key, 0600); e != nil {
		t.Fatal(e)
	}
	// Only the test client supplies trust/DNS dialing for its isolated fixture.
	// The independent rclone backend gets neither this trust root nor a DNS shim;
	// it reads through the admitted Worker IPC/shared-memory session.
	publicTransport := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}, DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
		if address != "gateway.test:"+publicPort {
			return nil, errors.New("unexpected public test origin")
		}
		return (&net.Dialer{}).DialContext(ctx, network, workerAddress)
	}}
	defer publicTransport.CloseIdleConnections()
	publicClient := &http.Client{Transport: publicTransport}
	storageSocket := filepath.Join(dir, "storage-ipc")
	offerSocket := filepath.Join(dir, "storage-offer")
	activate := cached
	activate.Public = marshal(map[string]string{"address": "http://" + vfsAddress + "/dav/", "username": "cache-user"})
	activate.SecretRefs = map[string]string{"password": "cache-auth"}
	workerSocket := filepath.Join(dir, "worker")
	brokerSocket := filepath.Join(dir, "broker")
	vfsSocket := filepath.Join(dir, "vfs")
	process := workerconfig.ProcessConfig{RuntimeConfig: workerconfig.RuntimeConfig{StorageIPCSocket: storageSocket, StorageOfferSocket: offerSocket, TLS: &workerconfig.TLSOptions{CertificateFile: certificatePath, PrivateKeySecret: "tls-key"}, Listen: workerAddress, Username: "dp-user", AuthSecret: "ingress", ClientSecretRefs: []string{"cache-auth"}, Worker: dp.Bootstrap{Schema: dp.BootstrapSchema, Artifacts: []dp.Artifact{{Path: plugin, SHA256: digest, PluginID: "client.openlist", DescriptorDigest: record.Digest, Language: "go"}}, Clients: []dp.Selection{base, cached}, SecretFiles: map[string]string{"ingress": ingress, "cache-auth": local, "tls-key": keyPath}}, Mounts: []workerconfig.Mount{{Name: "files", Client: "cached", RemoteClient: "origin", ReadOnly: true}}}, InstanceID: "worker-one", ControlSocket: workerSocket, BrokerSocket: brokerSocket, DataEndpoint: workerEndpoint}
	method := "/build.bazel.remote.execution.v2.Execution/Execute"
	manager, e := controlplane.New(controlplane.Config{
		Workers:       []controlplane.WorkerSpec{{Executable: workerBinary, Config: process}},
		Materializers: []controlplane.MaterializerSpec{{Executable: rcloneBinary, ActivateClient: &activate, Config: rclonebox.ProcessConfig{InstanceID: "rclone-one", ControlSocket: vfsSocket, Listen: vfsAddress, Remote: storageipc.Endpoint{Socket: storageSocket, OfferSocket: offerSocket, Namespace: "files", ExpectedWorkerID: "worker-one"}, LocalUsername: "cache-user", LocalPasswordFile: local, Directory: filepath.Join(dir, "cache"), MaxBytes: 8 << 20, MaxAgeSeconds: 60, MaxCacheEntries: 256, MaxOpenFiles: tinyConcurrency, MaxDirectoryEntries: 256, MaxConcurrentRequests: tinyConcurrency}}},
		MetadataNames: []string{"x-request-id"},
		Rules: []controlplane.Rule{
			{InstanceID: "worker-one", Authority: "worker.invalid", Decision: broker.Decision{RouteID: "build", WorkerDataEndpoint: workerEndpoint, ApprovedUpstreamID: "buildbarn", TTLSeconds: 30, MaxMessageBytes: 1 << 20, MaxInflightBytes: 2 << 20, Methods: []string{method}}},
			{InstanceID: "worker-one", Authority: "storage-ipc", SourceID: "files", TargetID: "origin", Decision: broker.Decision{RouteID: "materialize", WorkerDataEndpoint: workerEndpoint, ApprovedUpstreamID: "origin", TTLSeconds: 120, MaxMessageBytes: 256 << 10, MaxInflightBytes: tinyConcurrency * (256 << 10), Methods: []string{storageipc.AuthorizeMethod}}},
		},
	})
	if e != nil {
		t.Fatal(e)
	}
	defer manager.Close(context.Background())
	startup, startCancel := context.WithCancel(ctx)
	if e = manager.Start(startup); e != nil {
		var failure *supervision.StartError
		if errors.As(e, &failure) {
			t.Logf("managed start cause=%v stdout=%q stderr=%q", failure.Cause, failure.Output.Stdout, failure.Output.Stderr)
		}
		startCancel()
		t.Fatal(e)
	}
	startCancel()

	observedControl, e := manager.Worker("worker-one")
	if e != nil {
		t.Fatal(e)
	}
	observedWorker, e := observedControl.Info(ctx)
	if e != nil || observedWorker.StorageSocket != storageSocket || observedWorker.OfferSocket != offerSocket || observedWorker.Incarnation == "" || observedWorker.DataEndpoint != workerEndpoint {
		t.Fatal("local channel changed public TLS identity", e)
	}
	if stat, e := os.Stat(storageSocket); e != nil || stat.Mode().Perm() != 0600 {
		t.Fatal("storage IPC socket is not private", e)
	}
	materializerControl, e := rclonebox.Dial(ctx, vfsSocket)
	if e != nil {
		t.Fatal(e)
	}
	materializerInfo, e := materializerControl.Info(ctx)
	materializerControl.Close(context.Background())
	if e != nil || !materializerInfo.Ready {
		t.Fatal("materializer metadata unavailable", e)
	}
	t.Logf("effective materializer capacity: opens=%d requests=%d public-test-concurrency=%d", materializerInfo.MaxOpenFiles, materializerInfo.MaxConcurrentRequests, tinyConcurrency)
	if materializerInfo.MaxOpenFiles != tinyConcurrency || materializerInfo.MaxConcurrentRequests != tinyConcurrency {
		t.Fatal("fixture did not negotiate its configured eight-operation budget")
	}

	// The same trusted certificate MUST fail a loopback-IP name check. VFS did
	// not gain an InsecureSkipVerify/ServerName escape to make this topology pass.
	wrongNameTransport := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}}
	wrongNameClient := &http.Client{Transport: wrongNameTransport}
	wrongRequest, _ := http.NewRequestWithContext(ctx, "GET", "https://"+workerAddress+"/remote-dav/files/object", nil)
	wrongRequest.SetBasicAuth("dp-user", dataPassword)
	rejectedResponse, rejectedError := wrongNameClient.Do(wrongRequest)
	if rejectedResponse != nil {
		rejectedResponse.Body.Close()
	}
	wrongNameTransport.CloseIdleConnections()
	var wrongName x509.HostnameError
	if !errors.As(rejectedError, &wrongName) {
		t.Fatalf("DNS-only certificate unexpectedly accepted loopback name: %v", rejectedError)
	}
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
	wire, e := carrier.Dial(ctx, brokerSocket, carrier.Protocol{Service: broker.DecisionRPCService}, carrier.Limits{MaxPayload: 1 << 20, MaxInflight: 16, Timeout: time.Second})
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
		response, e := publicClient.Do(request)
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
	response, e := publicClient.Do(request)
	if e != nil {
		t.Fatal(e)
	}
	_, e = io.Copy(io.Discard, response.Body)
	response.Body.Close()
	if e != nil || response.StatusCode != 200 || originReads.Load() <= firstReads {
		t.Fatal("remote alias looped through cache")
	}

	t.Run("tiny-files-metadata-and-two-cached-passes", func(t *testing.T) {
		beforeMetadata := originReads.Load()
		for name, expected := range tinyContents {
			request, e := http.NewRequestWithContext(ctx, "HEAD", workerEndpoint+"/dav/files/tiny/"+name, nil)
			if e != nil {
				t.Fatal(e)
			}
			request.SetBasicAuth("dp-user", dataPassword)
			response, e := publicClient.Do(request)
			if e != nil {
				t.Fatal(e)
			}
			response.Body.Close()
			if response.StatusCode != http.StatusOK || response.ContentLength != int64(len(expected)) {
				t.Fatalf("HEAD %s: status=%d length=%d expected=%d", name, response.StatusCode, response.ContentLength, len(expected))
			}
		}
		request, e := http.NewRequestWithContext(ctx, "PROPFIND", workerEndpoint+"/dav/files/tiny/", strings.NewReader(`<propfind xmlns="DAV:"><allprop/></propfind>`))
		if e != nil {
			t.Fatal(e)
		}
		request.SetBasicAuth("dp-user", dataPassword)
		request.Header.Set("Content-Type", "application/xml")
		request.Header.Set("Depth", "1")
		response, e := publicClient.Do(request)
		if e != nil {
			t.Fatal(e)
		}
		listing, readError := io.ReadAll(io.LimitReader(response.Body, 2<<20))
		response.Body.Close()
		if readError != nil || response.StatusCode != http.StatusMultiStatus {
			t.Fatalf("PROPFIND: status=%d read=%v", response.StatusCode, readError)
		}
		var metadata struct {
			Responses []struct {
				Href string `xml:"href"`
			} `xml:"response"`
		}
		if e = xml.Unmarshal(listing, &metadata); e != nil {
			t.Fatal(e)
		}
		seen := map[string]bool{}
		for _, item := range metadata.Responses {
			name := filepath.Base(item.Href)
			if _, exists := tinyContents[name]; exists {
				if seen[name] {
					t.Fatalf("duplicate directory metadata for %s", name)
				}
				seen[name] = true
			}
		}
		if len(seen) != tinyFiles {
			t.Fatalf("PROPFIND returned %d of %d tiny files", len(seen), tinyFiles)
		}
		if originReads.Load() != beforeMetadata {
			t.Fatalf("HEAD/PROPFIND fetched origin bodies: before=%d after=%d", beforeMetadata, originReads.Load())
		}

		getPass := func(route string, concurrency int) []error {
			t.Helper()
			jobs := make(chan string)
			failures := make(chan error, tinyFiles)
			var workers sync.WaitGroup
			for range concurrency {
				workers.Go(func() {
					for name := range jobs {
						request, e := http.NewRequestWithContext(ctx, "GET", workerEndpoint+route+"/files/tiny/"+name, nil)
						if e != nil {
							failures <- e
							continue
						}
						request.SetBasicAuth("dp-user", dataPassword)
						response, e := publicClient.Do(request)
						if e != nil {
							failures <- fmt.Errorf("GET %s: %w", name, e)
							continue
						}
						body, e := io.ReadAll(io.LimitReader(response.Body, 4096))
						response.Body.Close()
						if e != nil || response.StatusCode != http.StatusOK || !bytes.Equal(body, tinyContents[name]) {
							failures <- fmt.Errorf("GET %s: status=%d size=%d read=%v", name, response.StatusCode, len(body), e)
						}
					}
				})
			}
			for name := range tinyContents {
				jobs <- name
			}
			close(jobs)
			workers.Wait()
			close(failures)
			var errors []error
			for failure := range failures {
				errors = append(errors, failure)
			}
			return errors
		}
		// The public raw alias exercises the same native provider and tiny EOF
		// boundary while bypassing the materializer, without using a private URL.
		if failures := getPass("/remote-dav", tinyConcurrency); len(failures) != 0 {
			for _, failure := range failures {
				t.Error(failure)
			}
			t.Fatal("native raw tiny-file control failed")
		}
		beforeCachedPass := originReads.Load()
		beforeCachedFiles := map[string]int32{}
		for name, reads := range tinyReads {
			beforeCachedFiles[name] = reads.Load()
		}
		if failures := getPass("/dav", tinyConcurrency); len(failures) != 0 {
			// A serial retry is diagnostic only: the required cold burst still
			// fails. It distinguishes throughput admission from a tiny-body codec.
			serialFailures := getPass("/dav", 1)
			t.Logf("cold burst failures=%d, subsequent serial failures=%d", len(failures), len(serialFailures))
			for _, failure := range failures {
				t.Error(failure)
			}
			for _, failure := range serialFailures {
				t.Logf("serial diagnostic: %v", failure)
			}
			t.FailNow()
		}
		firstPassReads := originReads.Load()
		for name, reads := range tinyReads {
			if reads.Load() <= beforeCachedFiles[name] {
				t.Fatalf("first public GET never fetched %s from origin", name)
			}
		}
		if failures := getPass("/dav", tinyConcurrency); len(failures) != 0 {
			for _, failure := range failures {
				t.Error(failure)
			}
			t.FailNow()
		}
		if originReads.Load() != firstPassReads {
			t.Fatalf("second tiny-file pass bypassed cache: origin GETs before=%d after=%d", firstPassReads, originReads.Load())
		}
		if tinyActive.Load() != 0 || tinyPeak.Load() > tinyConcurrency {
			t.Fatalf("origin concurrency escaped bound or retained work: active=%d peak=%d limit=%d", tinyActive.Load(), tinyPeak.Load(), tinyConcurrency)
		}
		t.Logf("tiny-files=%d metadata-origin-GETs=0 raw-tiny-control=PASS first-pass-origin-GETs=%d second-pass-extra-origin-GETs=0 observed-origin-peak=%d concurrency-limit=%d", tinyFiles, firstPassReads-beforeCachedPass, tinyPeak.Load(), tinyConcurrency)
	})

	for _, method := range []string{"PUT", "DELETE"} {
		request, e := http.NewRequestWithContext(ctx, method, workerEndpoint+"/dav/files/object", strings.NewReader("denied"))
		if e != nil {
			t.Fatal(e)
		}
		request.SetBasicAuth("dp-user", dataPassword)
		response, e := publicClient.Do(request)
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
	if _, e = os.Lstat(storageSocket); !os.IsNotExist(e) {
		t.Fatal("owned storage IPC socket outlived child retirement", e)
	}
	for _, instance := range instances {
		if e = syscall.Kill(instance.PID, 0); !errors.Is(e, syscall.ESRCH) {
			t.Fatalf("child not reaped %s: %v", instance.Kind, e)
		}
	}
}

func gatewayDNSCertificate(t *testing.T) ([]byte, []byte, *x509.CertPool) {
	t.Helper()
	key, e := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if e != nil {
		t.Fatal(e)
	}
	now := time.Now()
	template := &x509.Certificate{SerialNumber: big.NewInt(1), SubjectKeyId: []byte{1}, DNSNames: []string{"gateway.test"}, NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, e := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if e != nil {
		t.Fatal(e)
	}
	encoded, e := x509.MarshalPKCS8PrivateKey(key)
	if e != nil {
		t.Fatal(e)
	}
	certificate := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	private := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: encoded})
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(certificate) {
		t.Fatal("fixture trust root invalid")
	}
	return certificate, private, pool
}
