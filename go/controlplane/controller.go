// Package controlplane coordinates Worker state and metadata-only decisions.
// It never starts, signals or waits for another process.
package controlplane

import (
	"context"
	"encoding/json"
	cpconfig "github.com/wignerStan/remote-storage-gateway/go/contracts/control"
	"github.com/wignerStan/remote-storage-gateway/go/contracts/control/broker"
	controlgrpc "github.com/wignerStan/remote-storage-gateway/go/control/grpc"
	control "github.com/wignerStan/remote-storage-gateway/go/control/ttrpc"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

type Instance struct {
	Kind  string `json:"kind"`
	ID    string `json:"id"`
	Phase string `json:"phase"`
}
type Controller struct {
	mu               sync.Mutex
	config           cpconfig.Config
	clients          map[string]*control.WorkerClient
	remotes          map[string]broker.WorkerControl
	started, closing bool
}

func New(c cpconfig.Config) (*Controller, error) {
	b, err := json.Marshal(c)
	if err != nil || len(b) > 1<<20 {
		return nil, broker.ErrInvalid
	}
	if json.Unmarshal(b, &c) != nil || len(c.Workers) > 64 || len(c.Rules) > 256 || len(c.MetadataNames) > broker.MaxMetadataEntries {
		return nil, broker.ErrInvalid
	}
	if c.RuntimeDirectory != "" && (!filepath.IsAbs(c.RuntimeDirectory) || filepath.Clean(c.RuntimeDirectory) != c.RuntimeDirectory) {
		return nil, broker.ErrInvalid
	}
	ids := map[string]cpconfig.WorkerEndpoint{}
	paths := map[string]bool{}
	for _, w := range c.Workers {
		if w.ID == "" || len(w.ID) > 128 || ids[w.ID].ID != "" || (w.Remote == nil) == (w.ControlSocket == "") {
			return nil, broker.ErrInvalid
		}
		for _, path := range []string{w.ControlSocket, w.BrokerSocket} {
			if path == "" {
				continue
			}
			if !filepath.IsAbs(path) || filepath.Clean(path) != path || len(path) > 107 || paths[path] || c.RuntimeDirectory != "" && filepath.Dir(path) != c.RuntimeDirectory {
				return nil, broker.ErrInvalid
			}
			paths[path] = true
		}
		ids[w.ID] = w
	}
	for _, r := range c.Rules {
		w, ok := ids[r.InstanceID]
		if !ok || r.Decision.WorkerDataEndpoint != w.DataEndpoint || len(r.Decision.Methods) == 0 {
			return nil, broker.ErrInvalid
		}
		for _, method := range r.Decision.Methods {
			in := broker.TransferIntent{Method: method, Authority: r.Authority, SourceID: r.SourceID, TargetID: r.TargetID}
			if broker.ValidateIntent(in, nil) != nil || broker.ValidateDecision(in, r.Decision) != nil {
				return nil, broker.ErrInvalid
			}
		}
	}
	return &Controller{config: c, clients: map[string]*control.WorkerClient{}, remotes: map[string]broker.WorkerControl{}}, nil
}
func (c *Controller) Start(ctx context.Context) error {
	if ctx == nil {
		return broker.ErrInvalid
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.started || c.closing {
		return broker.ErrUnavailable
	}
	c.started = true
	for _, w := range c.config.Workers {
		if w.Remote != nil {
			client, err := newRemote(w)
			if err != nil {
				return err
			}
			c.remotes[w.ID] = client
		}
	}

	return nil
}

// Ready reports control entry readiness. Worker liveness is queried separately,
// so control-plane startup never depends on a Worker waiting for this broker.
func (c *Controller) Ready() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.started && !c.closing
}
func (c *Controller) Worker(id string) (broker.WorkerControl, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.started || c.closing {
		return nil, broker.ErrUnavailable
	}
	if r := c.remotes[id]; r != nil {
		return r, nil
	}
	if r := c.clients[id]; r != nil {
		return r, nil
	}
	for _, w := range c.config.Workers {
		if w.ID == id && w.ControlSocket != "" {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			client, err := control.DialWorker(ctx, w.ControlSocket)
			if err != nil {
				return nil, err
			}
			c.clients[id] = client
			return client, nil
		}
	}
	return nil, broker.ErrUnavailable
}
func (c *Controller) Instances() []Instance {
	c.mu.Lock()
	workers := append([]cpconfig.WorkerEndpoint(nil), c.config.Workers...)
	c.mu.Unlock()
	out := make([]Instance, 0, len(workers))
	for _, w := range workers {
		phase := "unavailable"
		client, err := c.Worker(w.ID)
		if err == nil {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			info, e := client.Info(ctx)
			cancel()
			if e == nil && info.Ready && info.ID == w.ID {
				phase = "ready"
			}
		}
		out = append(out, Instance{Kind: "worker", ID: w.ID, Phase: phase})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}
func (c *Controller) Decide(ctx context.Context, in broker.TransferIntent) (broker.Decision, error) {
	if ctx == nil {
		return broker.Decision{}, broker.ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return broker.Decision{}, err
	}
	client, err := c.Worker(in.Principal)
	if err != nil {
		return broker.Decision{}, broker.ErrDenied
	}
	info, err := client.Info(ctx)
	if err != nil || !info.Ready || info.ID != in.Principal {
		return broker.Decision{}, broker.ErrDenied
	}
	return resolve(c.config.Rules, in)
}
func (c *Controller) Close(ctx context.Context) error {
	if ctx == nil {
		return broker.ErrInvalid
	}
	c.mu.Lock()
	c.closing = true

	clients := make([]*control.WorkerClient, 0, len(c.clients))
	for _, client := range c.clients {
		clients = append(clients, client)
	}
	c.mu.Unlock()
	for _, client := range clients {
		if err := client.Close(ctx); err != nil {
			return err
		}
	}
	for _, client := range c.remotes {
		if v, ok := client.(interface{ Close(context.Context) error }); ok {
			if err := v.Close(ctx); err != nil {
				return err
			}
		}
	}
	return nil
}

var _ broker.DecisionBroker = (*Controller)(nil)

func newRemote(w cpconfig.WorkerEndpoint) (broker.WorkerControl, error) {
	return controlgrpc.DialWorker(*w.Remote)
}
