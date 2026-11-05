package ttrpc

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/containerd/ttrpc"
	actionsv1 "github.com/wignerStan/infra/go/network/rpc/protocols/actions"
	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/descriptor"
	"github.com/wignerStan/remote-storage-gateway/go/contracts/control/broker"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// Exercise the exact registered Actions handler and admission interceptor. This
// isolates application semantics from permission to create AF_UNIX listeners.
func invokeAction(ctx context.Context, server *Server, request *actionsv1.CallRequest) (*actionsv1.CallReply, error) {
	payload, err := proto.Marshal(request)
	if err != nil {
		return nil, err
	}
	value, err := server.intercept(ctx, func(value any) error {
		return proto.Unmarshal(payload, value.(proto.Message))
	}, nil, server.action)
	if err != nil {
		return nil, err
	}
	return value.(*actionsv1.CallReply), nil
}

func TestActionsRejectUnexpectedMetadataBeforeOwner(t *testing.T) {
	var calls atomic.Int32
	server, err := NewWorkerServer(&infoControl{info: func(context.Context) (broker.InstanceInfo, error) {
		calls.Add(1)
		return broker.InstanceInfo{Contract: broker.Contract, ID: "worker"}, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close(context.Background())
	for _, tc := range []struct {
		name, bind     string
		input, unknown []byte
		metadata       bool
		code           codes.Code
	}{
		{"empty", binding(broker.WorkerRPCService, infoMethod), nil, nil, false, codes.InvalidArgument},
		{"oversize", binding(broker.WorkerRPCService, infoMethod), bytes.Repeat([]byte(" "), descriptor.MaxBytes+1), nil, false, codes.InvalidArgument},
		{"unknown protobuf", binding(broker.WorkerRPCService, infoMethod), []byte("{}"), []byte{0x18, 1}, false, codes.InvalidArgument},
		{"object body", binding(broker.WorkerRPCService, infoMethod), []byte(`{"body":"object-data"}`), nil, false, codes.InvalidArgument},
		{"role", binding(broker.DecisionRPCService, infoMethod), []byte("{}"), nil, false, codes.Unimplemented},
		{"data operation", binding(broker.WorkerRPCService, "worker.read"), []byte("{}"), nil, false, codes.Unimplemented},
		{"transport credential", binding(broker.WorkerRPCService, infoMethod), []byte("{}"), nil, true, codes.InvalidArgument},
	} {
		t.Run(tc.name, func(t *testing.T) {
			request := &actionsv1.CallRequest{Binding: tc.bind, Input: tc.input}
			request.ProtoReflect().SetUnknown(tc.unknown)
			ctx := server.ctx
			if tc.metadata {
				ctx = ttrpc.WithMetadata(ctx, ttrpc.MD{"authorization": []string{"Bearer private"}})
			}
			if _, err := invokeAction(ctx, server, request); status.Code(err) != tc.code {
				t.Fatalf("unexpected status: %v, want %v", err, tc.code)
			}
		})
	}
	if calls.Load() != 0 {
		t.Fatalf("rejected request reached owner: %d", calls.Load())
	}
	reply, err := invokeAction(server.ctx, server, &actionsv1.CallRequest{Binding: binding(broker.WorkerRPCService, infoMethod), Input: []byte("{}")})
	var info broker.InstanceInfo
	if err != nil || decode(reply.Output, &info) != nil || info.ID != "worker" || calls.Load() != 1 {
		t.Fatalf("finite info call failed: %+v %v", info, err)
	}
}

func TestActionsBindTrustedPrincipal(t *testing.T) {
	var calls atomic.Int32
	service := decisionFunc(func(_ context.Context, input broker.TransferIntent) (broker.Decision, error) {
		calls.Add(1)
		if input.Principal != "trusted-worker" {
			t.Errorf("unexpected principal %q", input.Principal)
		}
		return broker.Decision{RouteID: "cas", WorkerDataEndpoint: "https://worker.example", ApprovedUpstreamID: "cas", TTLSeconds: 30, MaxMessageBytes: 1 << 20, MaxInflightBytes: 2 << 20, Methods: []string{input.Method}}, nil
	})
	server, err := NewDecisionServer(service, DecisionOptions{Principal: func(context.Context) (string, error) { return "trusted-worker", nil }, MetadataNames: []string{"x-request-id"}})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close(context.Background())
	for _, input := range []string{
		`{"method":"/google.bytestream.ByteStream/Read","authority":"cache.example","principal":"forged"}`,
		`{"method":"/google.bytestream.ByteStream/Read","authority":"cache.example","metadata":{"authorization":"Bearer private"}}`,
	} {
		if _, err := invokeAction(server.ctx, server, &actionsv1.CallRequest{Binding: binding(broker.DecisionRPCService, decideMethod), Input: []byte(input)}); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("untrusted identity or credential reached owner: %v", err)
		}
	}
	if calls.Load() != 0 {
		t.Fatal("invalid intent reached decision owner")
	}
	if _, err := invokeAction(server.ctx, server, &actionsv1.CallRequest{Binding: binding(broker.DecisionRPCService, decideMethod), Input: []byte(`{"method":"/google.bytestream.ByteStream/Read","authority":"cache.example","metadata":{"x-request-id":"one"}}`)}); err != nil || calls.Load() != 1 {
		t.Fatalf("admitted intent failed: %v", err)
	}
}

func TestActionsRetainOwnersThroughCloseAndEnforceBounds(t *testing.T) {
	for _, tc := range []struct {
		name  string
		count int
		input []byte
	}{
		{"concurrency", maxInflight, []byte("{}")},
		{"bytes", maxQueuedBytes / descriptor.MaxBytes, bytes.Repeat([]byte(" "), descriptor.MaxBytes)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			started, canceled, release := make(chan struct{}, tc.count), make(chan struct{}, tc.count), make(chan struct{})
			var once sync.Once
			defer once.Do(func() { close(release) })
			server, err := newServer(map[string]controlMethod{binding(broker.WorkerRPCService, infoMethod): func(ctx context.Context, _ []byte) ([]byte, error) {
				started <- struct{}{}
				<-ctx.Done()
				canceled <- struct{}{}
				<-release
				return nil, ctx.Err()
			}})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { once.Do(func() { close(release) }); _ = server.Close(context.Background()) }()
			results := make(chan error, tc.count)
			request := &actionsv1.CallRequest{Binding: binding(broker.WorkerRPCService, infoMethod), Input: tc.input}
			for i := 0; i < tc.count; i++ {
				go func() { _, err := invokeAction(server.ctx, server, request); results <- err }()
				waitControl(t, started)
			}
			if _, err := invokeAction(server.ctx, server, request); status.Code(err) != codes.ResourceExhausted {
				t.Fatalf("bound lost: %v", err)
			}
			bounded, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
			defer cancel()
			if err := server.Close(bounded); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("Close abandoned owner: %v", err)
			}
			for i := 0; i < tc.count; i++ {
				waitControl(t, canceled)
			}
			select {
			case <-server.Done():
				t.Fatal("closed before callbacks returned")
			default:
			}
			if _, err := invokeAction(context.Background(), server, request); status.Code(err) != codes.Unavailable {
				t.Fatalf("retiring server admitted callback: %v", err)
			}
			once.Do(func() { close(release) })
			if err := server.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
			for i := 0; i < tc.count; i++ {
				if err := <-results; status.Code(err) != codes.Canceled {
					t.Fatalf("cancellation lost: %v", err)
				}
			}
		})
	}
}

func TestActionsPreserveStableErrorsAndHideUnknownMessages(t *testing.T) {
	for _, want := range []error{context.Canceled, context.DeadlineExceeded, broker.ErrInvalid, broker.ErrDenied, broker.ErrRejected, broker.ErrUnavailable} {
		if got := clientError(publicStatus(want)); !errors.Is(got, want) {
			t.Fatalf("roundtrip %v: %v", want, got)
		}
	}
	secret := errors.New("private backend failure /storage/secret")
	wire := publicStatus(secret)
	if status.Code(wire) != codes.Unavailable || strings.Contains(wire.Error(), "secret") || !errors.Is(clientError(wire), broker.ErrUnavailable) {
		t.Fatalf("unsafe error: %v", wire)
	}
}
