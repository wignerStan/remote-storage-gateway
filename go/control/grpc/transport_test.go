//go:build infra_grpc

package grpc

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"github.com/wignerStan/infra/go/config/schema"
	port "github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/client"
	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/descriptor"
	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/descriptor/admission"
	dp "github.com/wignerStan/network-data-plane-worker/go/contracts/worker"
	cfg "github.com/wignerStan/remote-storage-gateway/go/contracts/control"
	"github.com/wignerStan/remote-storage-gateway/go/contracts/control/broker"
	pb "github.com/wignerStan/remote-storage-gateway/go/contracts/control/rpc"
	"math/big"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func identities(t *testing.T) (cfg.TLSIdentity, cfg.TLSIdentity, cfg.TLSIdentity) {
	t.Helper()
	dir := t.TempDir()
	caKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test-ca"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	der, e := x509.CreateCertificate(rand.Reader, ca, ca, &caKey.PublicKey, caKey)
	if e != nil {
		t.Fatal(e)
	}
	ca, _ = x509.ParseCertificate(der)
	caPath := filepath.Join(dir, "ca.pem")
	if e = os.WriteFile(caPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0600); e != nil {
		t.Fatal(e)
	}
	makeIdentity := func(id string, n int64) cfg.TLSIdentity {
		k, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		uri, _ := url.Parse("spiffe://gateway.test/" + id)
		cert := &x509.Certificate{SerialNumber: big.NewInt(n), Subject: pkix.Name{CommonName: id}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), DNSNames: []string{"localhost"}, URIs: []*url.URL{uri}, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth}, KeyUsage: x509.KeyUsageDigitalSignature}
		der, e := x509.CreateCertificate(rand.Reader, cert, ca, &k.PublicKey, caKey)
		if e != nil {
			t.Fatal(e)
		}
		key, e := x509.MarshalPKCS8PrivateKey(k)
		if e != nil {
			t.Fatal(e)
		}
		out := cfg.TLSIdentity{CAFile: caPath, CertificateFile: filepath.Join(dir, id+".crt"), PrivateKeyFile: filepath.Join(dir, id+".key"), ServerName: "localhost", PeerURI: "spiffe://gateway.test/control"}
		if e = os.WriteFile(out.CertificateFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0600); e != nil {
			t.Fatal(e)
		}
		if e = os.WriteFile(out.PrivateKeyFile, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: key}), 0600); e != nil {
			t.Fatal(e)
		}
		return out
	}
	return makeIdentity("control", 2), makeIdentity("worker", 3), makeIdentity("unassigned", 4)
}

type decisions struct{ seen chan string }

func (d *decisions) Decide(_ context.Context, in broker.TransferIntent) (broker.Decision, error) {
	d.seen <- in.Principal
	return broker.Decision{RouteID: "route", WorkerDataEndpoint: "https://worker.test", ApprovedUpstreamID: "origin", TTLSeconds: 1, MaxMessageBytes: 1024, MaxInflightBytes: 2048, Methods: []string{in.Method}}, nil
}
func TestRemoteDecisionsUseGRPCAndBindCertificateIdentity(t *testing.T) {
	serverID, workerID, otherID := identities(t)
	listener, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	address := listener.Addr().String()
	listener.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	service := &decisions{seen: make(chan string, 8)}
	server, e := StartServer(ctx, cfg.RPCServer{Listen: address, TLS: serverID, Peers: map[string]string{"spiffe://gateway.test/worker": "assigned-worker"}}, nil, service, []string{"x-request-id"})
	if e != nil {
		t.Fatal(e)
	}
	defer server.Close(context.Background())
	client, e := DialDecision(cfg.RemoteWorker{Address: address, TLS: workerID}, []string{"x-request-id"})
	if e != nil {
		t.Fatal(e)
	}
	defer client.Close(context.Background())
	in := broker.TransferIntent{Principal: "forged", Method: "/google.bytestream.ByteStream/Read", Authority: "cache.test", Metadata: map[string]string{"x-request-id": "request-1"}}
	if _, e = client.Decide(ctx, in); e != nil {
		t.Fatal(e)
	}
	if got := <-service.seen; got != "assigned-worker" {
		t.Fatalf("serialized identity overrode certificate: %s", got)
	}
	denied, e := DialDecision(cfg.RemoteWorker{Address: address, TLS: otherID}, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer denied.Close(context.Background())
	if _, e = denied.Decide(ctx, broker.TransferIntent{Method: in.Method, Authority: in.Authority}); e == nil {
		t.Fatal("unassigned certificate admitted")
	}
	// The generated finite protocol rejects unrecognized payload/body fields.
	raw := pb.NewDecisionsClient(client.conn)
	if _, e = raw.Decide(ctx, &pb.Metadata{Json: []byte(`{"method":"/x.y/Z","authority":"cache.test","body":"object-data"}`)}); e == nil {
		t.Fatal("object content accepted as control metadata")
	}
	if _, e = raw.Decide(ctx, &pb.Metadata{Json: []byte(strings.Repeat("x", broker.MaxMetadataBytes+1))}); e == nil {
		t.Fatal("oversized decision metadata accepted")
	}
	select {
	case got := <-service.seen:
		t.Fatalf("rejected request reached decision service: %s", got)
	default:
	}
	if e = server.Close(ctx); e != nil {
		t.Fatal(e)
	}
	select {
	case <-server.Done():
	default:
		t.Fatal("server shutdown returned before serving loop joined")
	}
}
func TestRemoteEndpointHasNoPlaintextOrAnonymousFallback(t *testing.T) {
	if _, e := connect(cfg.RemoteWorker{Address: "http://localhost:9000"}); e == nil {
		t.Fatal("URL accepted as gRPC address")
	}
	if _, e := connect(cfg.RemoteWorker{Address: "localhost:9000"}); e == nil {
		t.Fatal("missing mTLS identity admitted")
	}
	_, worker, _ := identities(t)
	worker.ServerName = ""
	if _, e := tlsConfig(worker, false); e != nil {
		t.Fatal(e)
	}
	b, e := json.Marshal(broker.TransferIntent{Principal: "cannot-serialize"})
	if e != nil || strings.Contains(string(b), "cannot-serialize") {
		t.Fatal("principal leaked to control message")
	}
}

type workerFixture struct {
	record  descriptor.Record
	updates atomic.Int32
}

func (f *workerFixture) Info(context.Context) (broker.InstanceInfo, error) {
	return broker.InstanceInfo{Contract: broker.Contract, ID: "worker-1", Ready: true, DataEndpoint: "https://worker.test"}, nil
}
func (f *workerFixture) Manifest(context.Context) (dp.Manifest, error) {
	return dp.Manifest{Schema: dp.CapabilitySchema, BuildDigest: "sha256:" + strings.Repeat("a", 64)}, nil
}
func (f *workerFixture) Observed(context.Context) ([]dp.Observed, error) { return []dp.Observed{}, nil }
func (f *workerFixture) Descriptor(context.Context, string) (descriptor.Record, error) {
	return f.record, nil
}
func (f *workerFixture) Replace(_ context.Context, name string, expected dp.Version, selection dp.Selection) (dp.Version, error) {
	f.updates.Add(1)
	return dp.Version{Incarnation: expected.Incarnation, Generation: expected.Generation + 1, DescriptorDigest: selection.DescriptorDigest}, nil
}
func TestRemoteWorkerPreservesDescriptorAndFiniteManagement(t *testing.T) {
	cpID, workerID, otherID := identities(t)
	cpID.PeerURI = "spiffe://gateway.test/worker"
	otherID.PeerURI = cpID.PeerURI
	type Configuration struct {
		Name string `json:"name"`
	}
	doc, e := schema.Derive[Configuration]()
	if e != nil {
		t.Fatal(e)
	}
	raw, e := json.Marshal(doc)
	if e != nil {
		t.Fatal(e)
	}
	record, e := admission.Encode(descriptor.Descriptor{Schema: descriptor.Schema, PluginID: "client.fixture", PluginVersion: "1.0.0", Contract: port.Contract, Runtime: "go", BuildDigest: "sha256:" + strings.Repeat("b", 64), Implementations: []descriptor.Implementation{{ID: "fixture", ConfigSchema: raw, Capabilities: []string{"stat"}}}})
	if e != nil {
		t.Fatal(e)
	}
	record, e = admission.Decode(append([]byte("\n"), record.Bytes...))
	if e != nil {
		t.Fatal(e)
	}
	impl := &workerFixture{record: record}
	ln, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	address := ln.Addr().String()
	ln.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	server, e := StartServer(ctx, cfg.RPCServer{Listen: address, TLS: workerID, Peers: map[string]string{"spiffe://gateway.test/control": "storage-control"}}, impl, nil, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer server.Close(context.Background())
	client, e := DialWorker(cfg.RemoteWorker{Address: address, TLS: cpID})
	if e != nil {
		t.Fatal(e)
	}
	defer client.Close(context.Background())
	info, e := client.Info(ctx)
	if e != nil || !info.Ready || info.ID != "worker-1" {
		t.Fatal(info, e)
	}
	if _, e = client.Manifest(ctx); e != nil {
		t.Fatal(e)
	}
	if _, e = client.Observed(ctx); e != nil {
		t.Fatal(e)
	}
	received, e := client.Descriptor(ctx, record.Digest)
	if e != nil || !bytes.Equal(received.Bytes, record.Bytes) {
		t.Fatal("descriptor bytes changed across Pods", e)
	}
	selection := dp.Selection{Name: "files", PluginID: "client.fixture", ImplementationID: "fixture", DescriptorDigest: record.Digest}
	if _, e = client.Replace(ctx, "files", dp.Version{Incarnation: "incarnation", Generation: 1}, selection); e != nil || impl.updates.Load() != 1 {
		t.Fatal(e)
	}
	rawClient := pb.NewWorkerClient(client.conn)
	if _, e = rawClient.Replace(ctx, &pb.ReplaceRequest{Name: "other", Json: []byte(`{"selection":{"name":"files"}}`)}); e == nil || impl.updates.Load() != 1 {
		t.Fatal("name mismatch reached replacement")
	}
	denied, e := DialWorker(cfg.RemoteWorker{Address: address, TLS: otherID})
	if e != nil {
		t.Fatal(e)
	}
	defer denied.Close(context.Background())
	if _, e = denied.Info(ctx); e == nil {
		t.Fatal("unassigned control plane admitted")
	}
	wrongURI := cpID
	wrongURI.PeerURI = "spiffe://gateway.test/not-worker"
	mismatch, e := DialWorker(cfg.RemoteWorker{Address: address, TLS: wrongURI})
	if e != nil {
		t.Fatal(e)
	}
	defer mismatch.Close(context.Background())
	attempt, stop := context.WithTimeout(ctx, time.Second)
	defer stop()
	if _, e = mismatch.Info(attempt); e == nil {
		t.Fatal("server URI identity mismatch admitted")
	}
}
