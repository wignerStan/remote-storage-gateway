//go:build dataplane_l7

package runtime

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	remote "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	dp "github.com/wignerStan/network-data-plane-worker/go/contracts/worker"
	"github.com/wignerStan/network-data-plane-worker/go/encodedruntime/nativebridge"
	"github.com/wignerStan/network-data-plane-worker/go/nativeclient"
	"github.com/wignerStan/remote-storage-gateway/go/contracts/control/broker"
	workerconfig "github.com/wignerStan/remote-storage-gateway/go/contracts/worker"
	control "github.com/wignerStan/remote-storage-gateway/go/control/ttrpc"
	"golang.org/x/net/webdav"
	bs "google.golang.org/genproto/googleapis/bytestream"
	g "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type encodedBackend struct {
	remote.UnimplementedCapabilitiesServer
	bs.UnimplementedByteStreamServer
	calls            atomic.Int32
	blocked, stopped chan struct{}
}

func (b *encodedBackend) GetCapabilities(ctx context.Context, _ *remote.GetCapabilitiesRequest) (*remote.ServerCapabilities, error) {
	b.calls.Add(1)
	md, _ := metadata.FromIncomingContext(ctx)
	if len(md.Get("authorization")) != 0 {
		return nil, status.Error(codes.Internal, "ingress credential leaked")
	}
	return &remote.ServerCapabilities{CacheCapabilities: &remote.CacheCapabilities{MaxBatchTotalSizeBytes: 87654}}, nil
}
func (b *encodedBackend) Read(_ *bs.ReadRequest, s bs.ByteStream_ReadServer) error {
	close(b.blocked)
	<-s.Context().Done()
	close(b.stopped)
	return s.Context().Err()
}
func (*encodedBackend) Write(s bs.ByteStream_WriteServer) error {
	if e := s.SendHeader(metadata.Pairs("worker-origin-header", "early")); e != nil {
		return e
	}
	r, e := s.Recv()
	if e != nil {
		return e
	}
	if !r.FinishWrite {
		return status.Error(codes.InvalidArgument, "fixture expects final chunk")
	}
	return s.SendAndClose(&bs.WriteResponse{CommittedSize: int64(len(r.Data))})
}
func TestEncodedWorkerListenerTLSBrokerAuthenticationAndJoinedClose(t *testing.T) {
	artifact := os.Getenv("REMOTE_GATEWAY_REAPI_PLUGIN")
	if artifact == "" {
		t.Skip("requires independently built REAPI native plugin")
	}
	body, e := os.ReadFile(artifact)
	if e != nil {
		t.Fatal(e)
	}
	sum := sha256.Sum256(body)
	approval := dp.Artifact{Path: artifact, SHA256: hex.EncodeToString(sum[:]), PluginID: "transfer.reapi", Language: "go"}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	record, e := nativebridge.Inspect(ctx, approval)
	if e != nil {
		t.Fatal(e)
	}
	approval.DescriptorDigest = record.Digest
	origin := &encodedBackend{blocked: make(chan struct{}), stopped: make(chan struct{})}
	upstream := g.NewServer()
	remote.RegisterCapabilitiesServer(upstream, origin)
	bs.RegisterByteStreamServer(upstream, origin)
	listener, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	go upstream.Serve(listener)
	defer upstream.Stop()
	var decisions atomic.Int32
	service := brokerFunc(func(_ context.Context, in broker.TransferIntent) (broker.Decision, error) {
		decisions.Add(1)
		if in.Principal != "trusted-worker" || in.SourceID != "build-source" || in.TargetID != "build-target" || in.Authority == "" || len(in.Metadata) != 0 {
			t.Error("broker scope or payload privacy violated", in)
		}
		return broker.Decision{RouteID: "build", WorkerDataEndpoint: "https://worker.invalid", ApprovedUpstreamID: "local-buildbarn", TTLSeconds: 10, MaxMessageBytes: 1 << 20, MaxInflightBytes: 2 << 20, Methods: []string{in.Method}}, nil
	})
	brokerServer, e := control.NewDecisionServer(service, control.DecisionOptions{Principal: func(context.Context) (string, error) { return "trusted-worker", nil }})
	if e != nil {
		t.Fatal(e)
	}
	// t.TempDir includes the long test name and can exceed Linux's sockaddr_un
	// limit when the caller correctly scopes TMPDIR under its task directory.
	brokerDirectory, e := os.MkdirTemp(os.TempDir(), "b-")
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { _ = os.RemoveAll(brokerDirectory) })
	socket := filepath.Join(brokerDirectory, "broker.sock")
	unix, e := net.Listen("unix", socket)
	if e != nil {
		t.Fatal(e)
	}
	if e = os.Chmod(socket, 0600); e != nil {
		t.Fatal(e)
	}
	go brokerServer.Serve(unix)
	defer brokerServer.Close(context.Background())
	tlsOptions, secretFiles, roots := ingressCertificate(t)
	authFile := filepath.Join(t.TempDir(), "ingress-password")
	if e = os.WriteFile(authFile, []byte("management-password"), 0600); e != nil {
		t.Fatal(e)
	}
	secretFiles["ingress-password"] = authFile
	config := workerconfig.RuntimeConfig{Listen: "127.0.0.1:0", Username: "operator", AuthSecret: "ingress-password", TLS: &tlsOptions, Worker: dp.Bootstrap{Schema: dp.BootstrapSchema, SecretFiles: secretFiles}, Encoded: &workerconfig.EncodedOptions{Artifact: approval, SourceID: "build-source", TargetID: "build-target", Upstreams: []workerconfig.EncodedUpstream{{ID: "local-buildbarn", Address: listener.Addr().String()}}}}
	expectedPublications := map[string]bool{record.Digest: true}
	clientPayload := "shared-exporter-client-stream"
	if clientPath := os.Getenv("REMOTE_GATEWAY_RCLONE_PLUGIN"); clientPath != "" {
		elf, e := os.ReadFile(clientPath)
		if e != nil {
			t.Fatal(e)
		}
		sum := sha256.Sum256(elf)
		clientApproval := dp.Artifact{Path: clientPath, SHA256: hex.EncodeToString(sum[:]), PluginID: "client.rclone", Language: "go"}
		publication, e := nativeclient.Inspect(ctx, clientPath, clientApproval.SHA256, "client.rclone", "go")
		if e != nil {
			t.Fatal(e)
		}
		clientApproval.DescriptorDigest = publication.Digest
		config.Worker.Artifacts = []dp.Artifact{clientApproval}
		expectedPublications[publication.Digest] = true
		root := t.TempDir()
		if e = os.WriteFile(filepath.Join(root, "hello.txt"), []byte(clientPayload), 0600); e != nil {
			t.Fatal(e)
		}
		dav := httptest.NewServer(&webdav.Handler{FileSystem: webdav.Dir(root), LockSystem: webdav.NewMemLS()})
		defer dav.Close()
		public, e := json.Marshal(map[string]string{"url": dav.URL})
		if e != nil {
			t.Fatal(e)
		}
		config.Worker.Clients = []dp.Selection{{Name: "dav", PluginID: "client.rclone", ImplementationID: "webdav", DescriptorDigest: publication.Digest, Public: public}}
		config.Mounts = []workerconfig.Mount{{Name: "files", Client: "dav"}}
	}
	runtime, e := New(ctx, workerconfig.ProcessConfig{RuntimeConfig: config, InstanceID: "trusted-worker", BrokerSocket: socket})
	if e != nil {
		t.Fatal(e)
	}
	if e = runtime.Start(ctx); e != nil {
		t.Fatal(e)
	}
	manifest, e := runtime.Manifest(ctx)
	if e != nil || len(manifest.PluginDescriptors) != len(expectedPublications) {
		t.Fatalf("encoded discovery=%v %v", manifest, e)
	}
	for _, publication := range manifest.PluginDescriptors {
		if !expectedPublications[publication.DescriptorDigest] {
			t.Fatal("unapproved capability publication")
		}
	}
	publication, e := runtime.Descriptor(ctx, record.Digest)
	if e != nil || !bytes.Equal(publication.Bytes, record.Bytes) {
		t.Fatal("exact descriptor publication changed", e)
	}
	publication.Bytes[0] = 'x'
	publication, e = runtime.Descriptor(ctx, record.Digest)
	if e != nil || !bytes.Equal(publication.Bytes, record.Bytes) {
		t.Fatal("caller mutated approved publication", e)
	}
	defer runtime.Close(context.Background())
	info, e := runtime.Info(ctx)
	if e != nil {
		t.Fatal(e)
	}
	transport := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}, ForceAttemptHTTP2: true}
	defer transport.CloseIdleConnections()
	httpClient := &http.Client{Transport: transport}
	for _, path := range []string{"/v1/manifest", "/v1/descriptors/" + record.Digest} {
		request, e := http.NewRequestWithContext(ctx, http.MethodGet, info.DataEndpoint+path, nil)
		if e != nil {
			t.Fatal(e)
		}
		request.SetBasicAuth("operator", "management-password")
		response, e := httpClient.Do(request)
		if e != nil {
			t.Fatal(e)
		}
		payload, e := io.ReadAll(io.LimitReader(response.Body, 1<<20))
		_ = response.Body.Close()
		if e != nil || response.StatusCode != http.StatusOK || response.ProtoMajor != 2 {
			t.Fatalf("live discovery %s: status=%d protocol=%d error=%v", path, response.StatusCode, response.ProtoMajor, e)
		}
		if path == "/v1/manifest" {
			var published dp.Manifest
			if e = json.Unmarshal(payload, &published); e != nil || len(published.PluginDescriptors) != len(expectedPublications) {
				t.Fatalf("live manifest stale: %s %v", payload, e)
			}
		} else if !bytes.Equal(payload, record.Bytes) {
			t.Fatal("HTTP descriptor changed exact approved bytes")
		}
	}
	if len(config.Worker.Artifacts) > 0 {
		request, e := http.NewRequestWithContext(ctx, http.MethodGet, info.DataEndpoint+"/dav/files/hello.txt", nil)
		if e != nil {
			t.Fatal(e)
		}
		request.SetBasicAuth("operator", "management-password")
		response, e := httpClient.Do(request)
		if e != nil {
			t.Fatal(e)
		}
		payload, e := io.ReadAll(response.Body)
		_ = response.Body.Close()
		if e != nil || response.StatusCode != http.StatusOK || string(payload) != clientPayload {
			t.Fatalf("client exporter failed after encoded exporter load: %d %q %v", response.StatusCode, payload, e)
		}
		t.Log("actual independent client.rclone + transfer.reapi exporters coexist and serve on Worker TLS listener")
	}
	conn, e := g.NewClient(strings.TrimPrefix(info.DataEndpoint, "https://"), g.WithTransportCredentials(credentials.NewTLS(&tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12})))
	if e != nil {
		t.Fatal(e)
	}
	defer conn.Close()
	client := remote.NewCapabilitiesClient(conn)
	if _, e = client.GetCapabilities(ctx, &remote.GetCapabilitiesRequest{}); status.Code(e) != codes.Unauthenticated {
		t.Fatalf("unauthenticated request=%v", e)
	}
	if decisions.Load() != 0 || origin.calls.Load() != 0 {
		t.Fatal("unauthenticated call reached CP or origin")
	}
	auth := metadata.AppendToOutgoingContext(ctx, "authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte("operator:management-password")))
	result, e := client.GetCapabilities(auth, &remote.GetCapabilitiesRequest{})
	if e != nil || result.GetCacheCapabilities().GetMaxBatchTotalSizeBytes() != 87654 {
		t.Fatalf("actual Worker TLS gRPC=%v %v", result, e)
	}
	if decisions.Load() != 1 || origin.calls.Load() != 1 {
		t.Fatal("transfer not decided once by metadata broker")
	}
	upload, e := bs.NewByteStreamClient(conn).Write(auth)
	if e != nil {
		t.Fatal(e)
	}
	initial, e := upload.Header()
	if e != nil || len(initial.Get("worker-origin-header")) != 1 {
		t.Fatal("Worker withheld initial upstream headers until body", initial, e)
	}
	body := []byte("early-header-body")
	if e = upload.Send(&bs.WriteRequest{ResourceName: "uploads/header-fixture", Data: body, FinishWrite: true}); e != nil {
		t.Fatal(e)
	}
	committed, e := upload.CloseAndRecv()
	if e != nil || committed.CommittedSize != int64(len(body)) {
		t.Fatal(committed, e)
	}
	stream, e := bs.NewByteStreamClient(conn).Read(auth, &bs.ReadRequest{ResourceName: "blocked"})
	if e != nil {
		t.Fatal(e)
	}
	select {
	case <-origin.blocked:
	case <-ctx.Done():
		t.Fatal("origin not opened")
	}
	canceled, stop := context.WithCancel(ctx)
	stop()
	_ = runtime.Close(canceled)
	if e = runtime.Close(ctx); e != nil {
		t.Fatal(e)
	}
	select {
	case <-origin.stopped:
	default:
		t.Fatal("Worker close completed before physical origin cancellation")
	}
	if _, e = stream.Recv(); e == nil {
		t.Fatal("stream survived Worker close")
	}
}
