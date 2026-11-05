package controlplane

import (
	"context"
	"errors"
	"testing"

	cpconfig "github.com/wignerStan/remote-storage-gateway/go/contracts/control"
	"github.com/wignerStan/remote-storage-gateway/go/contracts/control/broker"
)

func TestFailedControllerStartDoesNotPublishReadiness(t *testing.T) {
	c, err := New(cpconfig.Config{Workers: []cpconfig.WorkerEndpoint{{ID: "worker-a", DataEndpoint: "https://storage.example", Remote: &cpconfig.RemoteWorker{Address: "127.0.0.1:9443"}}}})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(context.Background())
	for range 2 {
		if err = c.Start(context.Background()); !errors.Is(err, broker.ErrInvalid) {
			t.Fatalf("invalid TLS identity was not rejected on a clean attempt: %v", err)
		}
		if c.Ready() || len(c.remotes) != 0 {
			t.Fatal("partial startup exposed control readiness")
		}
		if _, err = c.Worker("worker-a"); !errors.Is(err, broker.ErrUnavailable) {
			t.Fatalf("failed startup exposed a worker client: %v", err)
		}
	}
}

type assignedWorker struct {
	retirementWorker
	info broker.InstanceInfo
}

func (w *assignedWorker) Info(context.Context) (broker.InstanceInfo, error) { return w.info, nil }

type interruptedWorker struct {
	retirementWorker
	cancel context.CancelFunc
	err    error
}

func (w *interruptedWorker) Info(context.Context) (broker.InstanceInfo, error) {
	if w.cancel != nil {
		w.cancel()
	}
	return broker.InstanceInfo{}, w.err
}

func TestDecisionPreservesCancellationDuringWorkerInfo(t *testing.T) {
	for _, test := range []struct {
		name      string
		cancel    bool
		err, want error
	}{
		{"caller-canceled", true, broker.ErrUnavailable, context.Canceled},
		{"downstream-deadline", false, context.DeadlineExceeded, context.DeadlineExceeded},
	} {
		t.Run(test.name, func(t *testing.T) {
			c, err := New(cpconfig.Config{Workers: []cpconfig.WorkerEndpoint{{ID: "worker-a", ControlSocket: "/run/gateway-control"}}})
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close(context.Background())
			if err = c.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			w := &interruptedWorker{err: test.err}
			if test.cancel {
				w.cancel = cancel
			}
			c.remotes["worker-a"] = w
			if _, err = c.Decide(ctx, broker.TransferIntent{Principal: "worker-a"}); !errors.Is(err, test.want) {
				t.Fatalf("interrupted Worker query became an authorization denial: %v", err)
			}
		})
	}
}

func TestDecisionsRequireTheAssignedWorkerDataEndpoint(t *testing.T) {
	const endpoint = "https://storage.example"
	const method = "/google.bytestream.ByteStream/Read"
	w := cpconfig.WorkerEndpoint{ID: "worker-a", ControlSocket: "/run/gateway-control", DataEndpoint: endpoint}
	decision := broker.Decision{RouteID: "cas", WorkerDataEndpoint: endpoint, ApprovedUpstreamID: "cas", TTLSeconds: 30, MaxMessageBytes: 1 << 20, MaxInflightBytes: 4 << 20, Methods: []string{method}}
	c, err := New(cpconfig.Config{Workers: []cpconfig.WorkerEndpoint{w}, Rules: []cpconfig.Rule{{InstanceID: w.ID, Authority: "cache.example", Decision: decision}}})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(context.Background())
	if err = c.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	remote := &assignedWorker{info: broker.InstanceInfo{Contract: broker.Contract, ID: w.ID, Ready: true, DataEndpoint: endpoint}}
	c.remotes[w.ID] = remote
	in := broker.TransferIntent{Principal: w.ID, Authority: "cache.example", Method: method}
	if _, err = c.Decide(context.Background(), in); err != nil {
		t.Fatal(err)
	}
	remote.info.DataEndpoint = "https://other.example"
	if _, err = c.Decide(context.Background(), in); !errors.Is(err, broker.ErrDenied) {
		t.Fatalf("decision admitted for another data endpoint: %v", err)
	}
	if got := c.Instances(); len(got) != 1 || got[0].Phase != "unavailable" {
		t.Fatalf("unexpected assignment reported ready: %+v", got)
	}
}
