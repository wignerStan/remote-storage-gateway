package httpapi

import (
	"context"
	"errors"
	"testing"

	dp "github.com/wignerStan/network-data-plane-worker/go/contracts/worker"
	"github.com/wignerStan/remote-storage-gateway/go/contracts/control/broker"
	"github.com/wignerStan/remote-storage-gateway/go/controlplane"
)

type rejectingWorker struct {
	broker.WorkerControl
	expected dp.Version
	calls    int
}

func (w *rejectingWorker) Replace(_ context.Context, _ string, expected dp.Version, _ dp.Selection) (dp.Version, error) {
	w.expected = expected
	w.calls++
	return dp.Version{}, broker.ErrRejected
}

type assignedRegistry struct{ worker broker.WorkerControl }

func (r assignedRegistry) Instances() []controlplane.Instance          { return nil }
func (r assignedRegistry) Worker(string) (broker.WorkerControl, error) { return r.worker, nil }

func TestRejectedReplacementPreservesExpectedVersionAndNeverRetries(t *testing.T) {
	w := &rejectingWorker{}
	e := NewEntry(assignedRegistry{worker: w})
	expected := dp.Version{Incarnation: "worker-incarnation", Generation: 12}
	_, err := e.Replace(context.Background(), &ReplaceInput{
		InstanceInput: InstanceInput{Instance: "worker-a"}, Name: "files",
		Body: ReplaceBody{Expected: expected, Selection: dp.Selection{Name: "files"}},
	})
	var response *Error
	if !errors.As(err, &response) || response.Code != 409 {
		t.Fatalf("desired-state rejection lost its conflict response: %v", err)
	}
	if w.calls != 1 || w.expected != expected {
		t.Fatalf("replacement retried or changed its expected generation: calls=%d expected=%+v", w.calls, w.expected)
	}
}
