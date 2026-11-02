package runtime

import (
	"context"
	"errors"
	"github.com/wignerStan/infra/go/network/proxy/l7"
	"github.com/wignerStan/remote-storage-gateway/go/contracts/control/broker"
	config "github.com/wignerStan/remote-storage-gateway/go/contracts/vfs"
	control "github.com/wignerStan/remote-storage-gateway/go/control/ttrpc"
	service "github.com/wignerStan/remote-storage-gateway/go/vfs"
	"sync"
)

type Runtime struct {
	config            config.ProcessConfig
	service           *service.Service
	origin            *control.WorkerClient
	http              *l7.Server
	control           *control.Server
	mu                sync.Mutex
	started, retiring bool
	endpoint          string
	once              sync.Once
	done              chan struct{}
	closeErr          error
}

func (r *Runtime) Done() <-chan struct{} { return r.done }
func (r *Runtime) Info(ctx context.Context) (broker.InstanceInfo, error) {
	r.mu.Lock()
	ready := r.started && !r.retiring
	endpoint := r.endpoint
	r.mu.Unlock()
	if ready && r.service.CheckOrigin(ctx) != nil {
		ready = false
	}
	return broker.InstanceInfo{Contract: broker.Contract, ID: r.config.InstanceID, Ready: ready, DataEndpoint: endpoint}, nil
}

// Close retires cache-owned reads before waiting for HTTP handlers; a canceled
// waiter does not abandon handle, backend, or private socket cleanup.
func (r *Runtime) Close(ctx context.Context) error {
	if ctx == nil {
		return broker.ErrInvalid
	}
	r.once.Do(func() {
		r.mu.Lock()
		r.retiring = true
		r.mu.Unlock()
		go func() {
			cacheErr := r.service.Close(context.Background())
			// Pending admission/health queries cannot hold HTTP drain open.
			originErr := r.origin.Close(context.Background())
			httpErr := r.http.Shutdown(context.Background())
			controlErr := r.control.Close(context.Background())
			r.mu.Lock()
			r.closeErr = errors.Join(cacheErr, httpErr, controlErr, originErr)
			r.mu.Unlock()
			close(r.done)
		}()
	})
	select {
	case <-r.done:
		r.mu.Lock()
		defer r.mu.Unlock()
		return r.closeErr
	case <-ctx.Done():
		return ctx.Err()
	}
}
