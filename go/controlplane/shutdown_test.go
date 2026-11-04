package controlplane

import (
	"context"
	"errors"
	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/descriptor"
	dp "github.com/wignerStan/network-data-plane-worker/go/contracts/worker"
	cfg "github.com/wignerStan/remote-storage-gateway/go/contracts/control"
	"github.com/wignerStan/remote-storage-gateway/go/contracts/control/broker"
	"sync/atomic"
	"testing"
	"time"
)

type retirementWorker struct {
	entered chan struct{}
	release chan struct{}
	failure error
	calls   atomic.Int32
}

func (w *retirementWorker) Close(context.Context) error {
	w.calls.Add(1)
	if w.entered != nil {
		close(w.entered)
	}
	if w.release != nil {
		<-w.release
	}
	return w.failure
}
func (*retirementWorker) Info(context.Context) (broker.InstanceInfo, error) {
	return broker.InstanceInfo{}, broker.ErrUnavailable
}
func (*retirementWorker) Manifest(context.Context) (dp.Manifest, error) {
	return dp.Manifest{}, broker.ErrUnavailable
}
func (*retirementWorker) Observed(context.Context) ([]dp.Observed, error) {
	return nil, broker.ErrUnavailable
}
func (*retirementWorker) Descriptor(context.Context, string) (descriptor.Record, error) {
	return descriptor.Record{}, broker.ErrUnavailable
}
func (*retirementWorker) Replace(context.Context, string, dp.Version, dp.Selection) (dp.Version, error) {
	return dp.Version{}, broker.ErrUnavailable
}
func TestControllerCloseContinuesAfterErrorAndCanceledWaiter(t *testing.T) {
	c, e := New(cfg.Config{})
	if e != nil {
		t.Fatal(e)
	}
	failure := errors.New("retirement failure")
	bad := &retirementWorker{failure: failure}
	held := &retirementWorker{entered: make(chan struct{}), release: make(chan struct{})}
	c.remotes["bad"] = bad
	c.remotes["held"] = held
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if e = c.Close(ctx); !errors.Is(e, context.Canceled) {
		t.Fatalf("canceled waiter: %v", e)
	}
	select {
	case <-held.entered:
	case <-time.After(time.Second):
		t.Fatal("cleanup abandoned after caller cancellation or earlier error")
	}
	close(held.release)
	ctx, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	if e = c.Close(ctx); !errors.Is(e, failure) {
		t.Fatal("cleanup error lost", e)
	}
	if e = c.Close(context.Background()); !errors.Is(e, failure) {
		t.Fatal(e)
	}
	if bad.calls.Load() != 1 || held.calls.Load() != 1 {
		t.Fatal("resource retired more than once")
	}
}
