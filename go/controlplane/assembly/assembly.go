// Package assembly loads control configuration and owns metadata front doors.
// It starts no Worker or VFS process; the top-level manager or Kubernetes does.
package assembly

import (
	"context"
	"encoding/json"
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
	config  cfg.Config
	life    sync.Mutex
	ready   bool
	brokers []*control.Server
	remote  *controlgrpc.Server
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
	return &App{Controller: controller, config: c}, nil
}
func (a *App) Start(ctx context.Context) (err error) {
	if ctx == nil {
		return broker.ErrInvalid
	}
	a.life.Lock()
	defer a.life.Unlock()
	defer func() {
		if err != nil {
			a.close(context.Background())
		}
	}()
	if err = a.Controller.Start(ctx); err != nil {
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
		a.remote, err = controlgrpc.StartServer(ctx, *a.config.RPC, nil, a.Controller, a.config.MetadataNames)
		if err != nil {
			return err
		}
	}
	a.ready = true
	return nil
}
func (a *App) Ready() bool {
	a.life.Lock()
	defer a.life.Unlock()
	if !a.ready || !a.Controller.Ready() {
		return false
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
func (a *App) close(ctx context.Context) error {
	a.ready = false
	for _, s := range a.brokers {
		if e := s.Close(ctx); e != nil {
			return e
		}
	}
	if a.remote != nil {
		if e := a.remote.Close(ctx); e != nil {
			return e
		}
	}
	return a.Controller.Close(ctx)
}
func (a *App) Close(ctx context.Context) error {
	if ctx == nil {
		return broker.ErrInvalid
	}
	a.life.Lock()
	defer a.life.Unlock()
	return a.close(ctx)
}
