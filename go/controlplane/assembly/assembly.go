// Package assembly loads control configuration and owns metadata front doors.
// It starts no Worker or VFS process; the top-level manager or Kubernetes does.
package assembly

import (
	"context"
	"encoding/json"
	"errors"
	configsource "github.com/wignerStan/infra/go/config/source"
	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/descriptor/admission"
	cfg "github.com/wignerStan/remote-storage-gateway/go/contracts/control"
	"github.com/wignerStan/remote-storage-gateway/go/contracts/control/broker"
	controlgrpc "github.com/wignerStan/remote-storage-gateway/go/control/grpc"
	control "github.com/wignerStan/remote-storage-gateway/go/control/ttrpc"
	"github.com/wignerStan/remote-storage-gateway/go/controlplane"
	"io"
	"os"
	"sync"
)

type App struct {
	*controlplane.Controller
	config    cfg.Config
	life      sync.Mutex
	ready     bool
	brokers   []*control.Server
	remote    *controlgrpc.Server
	stop      context.Context
	cancel    context.CancelFunc
	once      sync.Once
	done      chan struct{}
	closeErr  error
	observers sync.WaitGroup
}

func Load(path string) (*App, error) {
	var c cfg.Config
	if path == "" {
		return New(c)
	}
	var b []byte
	var e error
	if path == "-" {
		b, e = io.ReadAll(io.LimitReader(os.Stdin, (1<<20)+1))
	} else {
		b, e = (configsource.File{Path: path, Limit: 1 << 20}).Read(context.Background())
	}
	if e != nil || len(b) > 1<<20 || admission.StrictJSON(b, &c) != nil {
		return nil, broker.ErrInvalid
	}
	return New(c)
}
func New(c cfg.Config) (*App, error) {
	b, e := json.Marshal(c)
	if e != nil || len(b) > 1<<20 || json.Unmarshal(b, &c) != nil {
		return nil, broker.ErrInvalid
	}
	controller, e := controlplane.New(c)
	if e != nil {
		return nil, e
	}
	stop, cancel := context.WithCancel(context.Background())
	return &App{Controller: controller, config: c, stop: stop, cancel: cancel, done: make(chan struct{})}, nil
}
func (a *App) Start(ctx context.Context) (err error) {
	if ctx == nil {
		return broker.ErrInvalid
	}
	a.life.Lock()
	defer a.life.Unlock()
	defer func() {
		if err != nil {
			a.beginClose()
		}
	}()
	startup, cancel := context.WithCancel(ctx)
	defer cancel()
	stopCancel := context.AfterFunc(a.stop, cancel)
	defer stopCancel()
	if err = startup.Err(); err != nil {
		return err
	}
	if err = a.Controller.Start(startup); err != nil {
		return err
	}
	if a.config.RuntimeDirectory != "" {
		if err = control.PreparePrivateDirectory(a.config.RuntimeDirectory); err != nil {
			return err
		}
	}
	for _, w := range a.config.Workers {
		if w.BrokerSocket == "" {
			continue
		}
		var s *control.Server
		s, err = control.ListenDecision(w.BrokerSocket, w.ID, a.Controller, a.config.MetadataNames)
		if err != nil {
			return err
		}
		a.brokers = append(a.brokers, s)
	}
	if a.config.RPC != nil {
		a.remote, err = controlgrpc.StartServer(startup, *a.config.RPC, nil, a.Controller, a.config.MetadataNames)
		if err != nil {
			return err
		}
	}
	for _, server := range a.brokers {
		a.observers.Add(1)
		go func(s *control.Server) {
			defer a.observers.Done()
			select {
			case <-s.Done():
				a.beginClose()
			case <-a.stop.Done():
			}
		}(server)
	}
	if a.remote != nil {
		a.observers.Add(1)
		go func() {
			defer a.observers.Done()
			select {
			case <-a.remote.Done():
				a.beginClose()
			case <-a.stop.Done():
			}
		}()
	}
	a.ready = true
	return nil
}
func (a *App) Ready() bool {
	a.life.Lock()
	defer a.life.Unlock()
	if !a.ready || a.stop.Err() != nil || !a.Controller.Ready() {
		return false
	}
	for _, server := range a.brokers {
		select {
		case <-server.Done():
			return false
		default:
		}
	}
	if a.remote != nil {
		select {
		case <-a.remote.Done():
			return false
		default:
		}
	}
	return true
}
func (a *App) beginClose() {
	a.once.Do(func() {
		a.cancel()
		go func() {
			a.life.Lock()
			a.ready = false
			var errs []error
			for _, server := range a.brokers {
				errs = append(errs, server.Close(context.Background()))
			}
			if a.remote != nil {
				errs = append(errs, a.remote.Close(context.Background()))
			}
			errs = append(errs, a.Controller.Close(context.Background()))
			a.life.Unlock()
			a.observers.Wait()
			a.closeErr = errors.Join(errs...)
			close(a.done)
		}()
	})
}
func (a *App) Close(ctx context.Context) error {
	if ctx == nil {
		return broker.ErrInvalid
	}
	a.beginClose()
	select {
	case <-a.done:
		return a.closeErr
	case <-ctx.Done():
		return ctx.Err()
	}
}
