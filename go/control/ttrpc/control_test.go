package ttrpc

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/containerd/ttrpc"
	"github.com/wignerStan/infra/go/config/schema"
	rpcttrpc "github.com/wignerStan/infra/go/network/rpc/client/ttrpc"
	actionsv1 "github.com/wignerStan/infra/go/network/rpc/protocols/actions"
	port "github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/client"
	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/descriptor"
	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/descriptor/admission"
	dp "github.com/wignerStan/network-data-plane-worker/go/contracts/worker"
	"github.com/wignerStan/remote-storage-gateway/go/contracts/control/broker"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func socket(t *testing.T, s *Server) string {
	t.Helper()
	dir, e := os.MkdirTemp("/tmp", "trade-control-")
	if e != nil {
		t.Fatal(e)
	}
	path := filepath.Join(dir, "rpc")
	listener, e := net.Listen("unix", path)
	if e != nil {
		t.Fatal(e)
	}
	if e = os.Chmod(path, 0600); e != nil {
		t.Fatal(e)
	}
	done := make(chan error, 1)
	go func() { done <- s.Serve(listener) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if e := s.Close(ctx); e != nil {
			t.Error(e)
		}
		select {
		case e := <-done:
			if e != nil {
				t.Error(e)
			}
		case <-ctx.Done():
			t.Error("metadata server did not drain")
		}
		os.RemoveAll(dir)
	})
	return path
}

type fixtureControl struct {
	record  descriptor.Record
	updates atomic.Int32
}

func (f *fixtureControl) Info(context.Context) (broker.InstanceInfo, error) {
	return broker.InstanceInfo{Contract: broker.Contract, ID: "worker-1", Ready: true, DataEndpoint: "https://worker.invalid"}, nil
}
func (f *fixtureControl) Manifest(context.Context) (dp.Manifest, error) {
	return dp.Manifest{Schema: dp.CapabilitySchema}, nil
}
func (f *fixtureControl) Observed(context.Context) ([]dp.Observed, error) {
	return []dp.Observed{}, nil
}
func (f *fixtureControl) Descriptor(context.Context, string) (descriptor.Record, error) {
	return f.record, nil
}
func (f *fixtureControl) Replace(_ context.Context, name string, _ dp.Version, s dp.Selection) (dp.Version, error) {
	f.updates.Add(1)
	return dp.Version{Generation: 1}, nil
}
func TestWorkerControlPreservesExactDescriptorAndRejectsPayloadOperations(t *testing.T) {
	type Configuration struct {
		Name string `json:"name"`
	}
	document, e := schema.Derive[Configuration]()
	if e != nil {
		t.Fatal(e)
	}
	raw, e := json.Marshal(document)
	if e != nil {
		t.Fatal(e)
	}
	record, e := admission.Encode(descriptor.Descriptor{Schema: descriptor.Schema, PluginID: "client.fixture", PluginVersion: "1.0.0", Contract: port.Contract, Runtime: "go", BuildDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Implementations: []descriptor.Implementation{{ID: "fixture", ConfigSchema: raw, Capabilities: []string{"stat"}}}})
	if e != nil {
		t.Fatal(e)
	}
	record, e = admission.Decode(append([]byte("\n"), record.Bytes...))
	if e != nil {
		t.Fatal(e)
	}
	impl := &fixtureControl{record: record}
	server, e := NewWorkerServer(impl)
	if e != nil {
		t.Fatal(e)
	}
	path := socket(t, server)
	client, e := DialWorker(context.Background(), path)
	if e != nil {
		t.Fatal(e)
	}
	defer client.Close(context.Background())
	wrong, e := rpcttrpc.Dial(context.Background(), path, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer wrong.Close()
	if _, e = rawCall(context.Background(), wrong, broker.DecisionRPCService, infoMethod, []byte("{}")); status.Code(e) != codes.Unimplemented {
		t.Fatal("worker socket admitted foreign protobuf service", e)
	}
	got, e := client.Descriptor(context.Background(), record.Digest)
	if e != nil || !bytes.Equal(got.Bytes, record.Bytes) {
		t.Fatal("descriptor exact bytes lost", e)
	}
	wire, e := rpcttrpc.Dial(context.Background(), path, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer wire.Close()
	for _, method := range []string{"worker.open", "worker.read", "worker.put", "worker.exec"} {
		if _, e := rawCall(context.Background(), wire, broker.WorkerRPCService, method, []byte(`{}`)); e == nil {
			t.Fatalf("payload operation admitted: %s", method)
		}
	}
	if _, e = rawCall(context.Background(), wire, broker.WorkerRPCService, infoMethod, []byte(`{"body":"object-data"}`)); e == nil {
		t.Fatal("unknown payload field admitted")
	}
	if _, e = client.Replace(context.Background(), "one", dp.Version{}, dp.Selection{Name: "two"}); e == nil || impl.updates.Load() != 0 {
		t.Fatal("mismatched selection entered runtime")
	}
}

type decisionFunc func(context.Context, broker.TransferIntent) (broker.Decision, error)

func (f decisionFunc) Decide(ctx context.Context, in broker.TransferIntent) (broker.Decision, error) {
	return f(ctx, in)
}
func TestDecisionBindsTrustedPrincipalAndRejectsClaimedIdentity(t *testing.T) {
	var calls atomic.Int32
	service := decisionFunc(func(_ context.Context, in broker.TransferIntent) (broker.Decision, error) {
		calls.Add(1)
		if in.Principal != "trusted-worker" {
			t.Errorf("untrusted principal %q", in.Principal)
		}
		return broker.Decision{RouteID: "route", WorkerDataEndpoint: "https://worker.invalid", ApprovedUpstreamID: "buildbarn", TTLSeconds: 10, MaxMessageBytes: 1 << 20, MaxInflightBytes: 2 << 20, Methods: []string{in.Method}}, nil
	})
	server, e := NewDecisionServer(service, DecisionOptions{Principal: func(context.Context) (string, error) { return "trusted-worker", nil }, MetadataNames: []string{"x-request-id"}})
	if e != nil {
		t.Fatal(e)
	}
	path := socket(t, server)
	client, e := DialDecision(context.Background(), path, []string{"x-request-id"})
	if e != nil {
		t.Fatal(e)
	}
	defer client.Close(context.Background())
	in := broker.TransferIntent{Method: "/build.bazel.remote.execution.v2.Execution/Execute", Authority: "worker.invalid", Principal: "forged", Metadata: map[string]string{"x-request-id": "one"}}
	if _, e = client.Decide(context.Background(), in); e != nil {
		t.Fatal(e)
	}
	wire, e := rpcttrpc.Dial(context.Background(), path, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer wire.Close()
	if _, e = rawCall(context.Background(), wire, broker.DecisionRPCService, decideMethod, []byte(`{"method":"/x.Service/Call","authority":"worker.invalid","principal":"forged"}`)); e == nil {
		t.Fatal("serialized principal assertion admitted")
	}
	in.Metadata = map[string]string{"authorization": "Bearer private"}
	if _, e = client.Decide(context.Background(), in); e == nil {
		t.Fatal("credential-bearing metadata sent")
	}
	if calls.Load() != 1 {
		t.Fatalf("invalid request reached decision service: %d", calls.Load())
	}
}

func rawCall(ctx context.Context, wire *ttrpc.Client, service, method string, input []byte) ([]byte, error) {
	reply := new(actionsv1.CallReply)
	err := wire.Call(ctx, actionsService, "Call", &actionsv1.CallRequest{Binding: binding(service, method), Input: input}, reply)
	return reply.Output, err
}
