package ttrpc

import (
	"bytes"
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/containerd/ttrpc"
	rpcttrpc "github.com/wignerStan/infra/go/network/rpc/client/ttrpc"
	actionsv1 "github.com/wignerStan/infra/go/network/rpc/protocols/actions"
	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/descriptor"
	dp "github.com/wignerStan/network-data-plane-worker/go/contracts/worker"
	"github.com/wignerStan/remote-storage-gateway/go/contracts/control/broker"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type infoControl struct {
	fixtureControl
	info func(context.Context) (broker.InstanceInfo, error)
}

func (c *infoControl) Info(ctx context.Context) (broker.InstanceInfo, error) { return c.info(ctx) }
func waitControl(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("control callback did not reach expected state")
	}
}
func rawControl(t *testing.T, path string) *ttrpc.Client {
	t.Helper()
	wire, err := rpcttrpc.Dial(context.Background(), path, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = wire.Close() })
	return wire
}

func TestNativeControlCancellationKeepsIndependentCallsUsable(t *testing.T) {
	started, canceled := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	service := &infoControl{info: func(ctx context.Context) (broker.InstanceInfo, error) {
		if calls.Add(1) == 1 {
			close(started)
			<-ctx.Done()
			close(canceled)
			return broker.InstanceInfo{}, ctx.Err()
		}
		return broker.InstanceInfo{Contract: broker.Contract, ID: "worker"}, nil
	}}
	server, err := NewWorkerServer(service)
	if err != nil {
		t.Fatal(err)
	}
	client, err := DialWorker(context.Background(), socket(t, server))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(context.Background())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() { _, err := client.Info(ctx); result <- err }()
	waitControl(t, started)
	cancel()
	waitControl(t, canceled)
	if err = <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel result: %v", err)
	}
	if _, err = client.Info(context.Background()); err != nil {
		t.Fatalf("cancel damaged independent call: %v", err)
	}
}

func TestNativeControlCloseJoinsAdmittedOwner(t *testing.T) {
	started, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	service := &infoControl{info: func(ctx context.Context) (broker.InstanceInfo, error) {
		close(started)
		<-ctx.Done()
		close(canceled)
		<-release
		return broker.InstanceInfo{}, ctx.Err()
	}}
	server, err := NewWorkerServer(service)
	if err != nil {
		t.Fatal(err)
	}
	client, err := DialWorker(context.Background(), socket(t, server))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(context.Background())
	result := make(chan error, 1)
	go func() { _, err := client.Info(context.Background()); result <- err }()
	waitControl(t, started)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if err = server.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Close returned before owner exit: %v", err)
	}
	waitControl(t, canceled)
	select {
	case <-server.Done():
		t.Fatal("reported drained while callback still active")
	default:
	}
	once.Do(func() { close(release) })
	if err = server.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err = <-result; err == nil {
		t.Fatal("closed call succeeded")
	}
}

func TestNativeControlWireBoundsAndFiniteBindings(t *testing.T) {
	var called atomic.Int32
	server, err := NewWorkerServer(&infoControl{info: func(context.Context) (broker.InstanceInfo, error) {
		called.Add(1)
		return broker.InstanceInfo{Contract: broker.Contract, ID: "worker"}, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	path := socket(t, server)
	wire := rawControl(t, path)
	cases := []struct {
		name, bind string
		data       []byte
		unknown    []byte
		code       codes.Code
	}{
		{"oversize", binding(broker.WorkerRPCService, infoMethod), bytes.Repeat([]byte(" "), descriptor.MaxBytes+1), nil, codes.InvalidArgument},
		{"protobuf extension", binding(broker.WorkerRPCService, infoMethod), []byte("{}"), []byte{0x18, 1}, codes.InvalidArgument},
		{"foreign role", binding(broker.DecisionRPCService, infoMethod), []byte("{}"), nil, codes.Unimplemented},
		{"payload", binding(broker.WorkerRPCService, "worker.read"), []byte("{}"), nil, codes.Unimplemented},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := &actionsv1.CallRequest{Binding: tc.bind, Input: tc.data}
			req.ProtoReflect().SetUnknown(tc.unknown)
			err := wire.Call(context.Background(), actionsService, "Call", req, new(actionsv1.CallReply))
			if status.Code(err) != tc.code {
				t.Fatalf("status %v, want %v: %v", status.Code(err), tc.code, err)
			}
		})
	}
	if err = wire.Call(context.Background(), actionsService, "CallStream", &actionsv1.CallRequest{Binding: binding(broker.WorkerRPCService, infoMethod), Input: []byte("{}")}, new(actionsv1.CallReply)); status.Code(err) != codes.Unimplemented {
		t.Fatalf("stream surface admitted: %v", err)
	}
	marked := ttrpc.WithMetadata(context.Background(), ttrpc.MD{"authorization": []string{"Bearer private"}})
	if _, err = rawCall(marked, wire, broker.WorkerRPCService, infoMethod, []byte("{}")); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("transport metadata admitted: %v", err)
	}
	if called.Load() != 0 {
		t.Fatalf("invalid request reached owner %d times", called.Load())
	}
	client, err := DialWorker(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(context.Background())
	if _, err = client.Info(marked); err != nil {
		t.Fatalf("typed client leaked transport headers: %v", err)
	}
}

func TestNativeControlAdmissionBounds(t *testing.T) {
	for _, tc := range []struct {
		name  string
		count int
		input []byte
	}{
		{"concurrency", maxInflight, []byte("{}")},
		{"bytes", maxQueuedBytes / descriptor.MaxBytes, append([]byte("{}"), bytes.Repeat([]byte(" "), descriptor.MaxBytes-2)...)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			started, release := make(chan struct{}, maxInflight), make(chan struct{})
			var once sync.Once
			defer once.Do(func() { close(release) })
			server, err := newServer(map[string]controlMethod{binding(broker.WorkerRPCService, infoMethod): func(ctx context.Context, input []byte) ([]byte, error) {
				started <- struct{}{}
				select {
				case <-release:
					return []byte("{}"), nil
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}})
			if err != nil {
				t.Fatal(err)
			}
			wire := rawControl(t, socket(t, server))
			results := make(chan error, tc.count)
			for i := 0; i < tc.count; i++ {
				go func() {
					_, err := rawCall(context.Background(), wire, broker.WorkerRPCService, infoMethod, tc.input)
					results <- err
				}()
				waitControl(t, started)
			}
			if _, err = rawCall(context.Background(), wire, broker.WorkerRPCService, infoMethod, tc.input); status.Code(err) != codes.ResourceExhausted {
				t.Fatalf("admission bound lost: %v", err)
			}
			once.Do(func() { close(release) })
			for i := 0; i < tc.count; i++ {
				if err = <-results; err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

type rejectedControl struct{ fixtureControl }

func (*rejectedControl) Replace(context.Context, string, dp.Version, dp.Selection) (dp.Version, error) {
	return dp.Version{}, broker.ErrRejected
}
func TestNativeControlPreservesDesiredStateRejection(t *testing.T) {
	server, err := NewWorkerServer(&rejectedControl{})
	if err != nil {
		t.Fatal(err)
	}
	client, err := DialWorker(context.Background(), socket(t, server))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(context.Background())
	if _, err = client.Replace(context.Background(), "one", dp.Version{}, dp.Selection{Name: "one"}); !errors.Is(err, broker.ErrRejected) {
		t.Fatalf("rejection lost: %v", err)
	}
}
func TestNativeControlRejectsNonUnixPeer(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	if err := checkPeer(a); !errors.Is(err, broker.ErrDenied) {
		t.Fatal("non-Unix peer admitted", err)
	}
}
