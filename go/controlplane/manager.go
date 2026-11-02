// Package controlplane owns child assignments and metadata-only transfer policy.
// Payloads, native loaders and remote storage engines remain outside this process.
package controlplane

import (
	"context"
	configsource "github.com/wignerStan/infra/go/config/source"
	"github.com/wignerStan/infra/go/process/supervisor"
	vfsconfig "github.com/wignerStan/remote-storage-gateway/go/contracts/vfs"

	"path/filepath"
	"sort"
	"sync"

	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/descriptor/admission"
	dp "github.com/wignerStan/network-data-plane-worker/go/contracts/worker"
	"github.com/wignerStan/remote-storage-gateway/go/contracts/control/broker"
	workerconfig "github.com/wignerStan/remote-storage-gateway/go/contracts/worker"
	control "github.com/wignerStan/remote-storage-gateway/go/control/ttrpc"
	"github.com/wignerStan/remote-storage-gateway/go/worker"
)

type WorkerSpec struct {
	Executable string                     `json:"executable"`
	Config     workerconfig.ProcessConfig `json:"config"`
}
type Rule struct {
	InstanceID string          `json:"instance_id"`
	Authority  string          `json:"authority"`
	SourceID   string          `json:"source_id,omitempty"`
	TargetID   string          `json:"target_id,omitempty"`
	Decision   broker.Decision `json:"decision"`
}
type VFSSpec struct {
	ActivateClient *dp.Selection           `json:"activate_client,omitempty"`
	Executable     string                  `json:"executable"`
	Config         vfsconfig.ProcessConfig `json:"config"`
}
type Config struct {
	RuntimeDirectory string       `json:"runtime_directory,omitempty"`
	VFS              []VFSSpec    `json:"vfs,omitempty"`
	Workers          []WorkerSpec `json:"workers"`
	Rules            []Rule       `json:"rules,omitempty"`
	MetadataNames    []string     `json:"metadata_names,omitempty"`
}
type Instance struct {
	Kind  string `json:"kind"`
	ID    string `json:"id"`
	PID   int    `json:"pid"`
	Phase string `json:"phase"`
}
type Manager struct {
	materializers map[string]*materializer
	supervisor    *supervisor.Supervisor
	stop          context.Context
	cancel        context.CancelFunc
	life          sync.Mutex
	mu            sync.RWMutex
	config        Config
	boxes         map[string]*worker.Box
	brokers       []*control.Server
	ready         bool
	closed        bool
	started       bool
	closing       bool
	closeOnce     sync.Once
	done          chan struct{}
}

func Load(path string) (*Manager, error) {
	if path == "" {
		return New(Config{})
	}
	b, e := (configsource.File{Path: path, Limit: 1 << 20}).Read(context.Background())
	if e != nil {
		return nil, broker.ErrInvalid
	}
	var c Config
	if admission.StrictJSON(b, &c) != nil {
		return nil, broker.ErrInvalid
	}
	return New(c)
}
func New(c Config) (*Manager, error) {
	if len(c.VFS) > 64 || len(c.Workers) > 64 || len(c.Rules) > 256 || len(c.MetadataNames) > broker.MaxMetadataEntries {
		return nil, broker.ErrInvalid
	}
	stop, cancel := context.WithCancel(context.Background())
	m := &Manager{stop: stop, cancel: cancel, boxes: map[string]*worker.Box{}, done: make(chan struct{})}
	// Own the complete config, including nested decision method slices and maps.
	c, e := copyConfig(c)
	if e != nil {
		cancel()
		return nil, e
	}
	if c.RuntimeDirectory != "" && (!filepath.IsAbs(c.RuntimeDirectory) || filepath.Clean(c.RuntimeDirectory) != c.RuntimeDirectory) {
		cancel()
		return nil, broker.ErrInvalid
	}
	m.config = c
	admitted := false
	defer func() {
		if !admitted {
			_ = m.Close(context.Background())
		}
	}()
	m.materializers = map[string]*materializer{}
	m.supervisor, e = supervisor.New(supervisor.Options{MaxChildren: 64})
	if e != nil {
		return nil, e
	}
	paths := map[string]bool{}
	for _, spec := range c.Workers {
		id := spec.Config.InstanceID
		if m.boxes[id] != nil {
			return nil, broker.ErrInvalid
		}
		for _, path := range []string{spec.Config.ControlSocket, spec.Config.BrokerSocket} {
			if path == "" {
				continue
			}
			if paths[path] || !filepath.IsAbs(path) || len(path) > 107 || c.RuntimeDirectory != "" && filepath.Dir(path) != c.RuntimeDirectory {
				return nil, broker.ErrInvalid
			}
			paths[path] = true
		}
		box, e := worker.New(worker.Options{Executable: spec.Executable, Config: spec.Config, ProxyEnvironment: proxyEnvironment()})
		if e != nil {
			return nil, e
		}
		m.boxes[id] = box
	}
	for _, s := range c.VFS {
		id := s.Config.InstanceID
		if id == "" || m.boxes[id] != nil || m.materializers[id] != nil || !filepath.IsAbs(s.Executable) || !filepath.IsAbs(s.Config.ControlSocket) || len(s.Config.ControlSocket) > 107 || paths[s.Config.ControlSocket] || c.RuntimeDirectory != "" && filepath.Dir(s.Config.ControlSocket) != c.RuntimeDirectory {
			return nil, broker.ErrInvalid
		}
		paths[s.Config.ControlSocket] = true
		found := false
		for _, w := range c.Workers {
			if w.Config.InstanceID == s.Config.WorkerInstanceID && w.Config.ControlSocket == s.Config.WorkerControlSocket {
				found = true
			}
		}
		if !found {
			return nil, broker.ErrInvalid
		}
		if s.ActivateClient != nil {
			assigned := false
			for _, w := range c.Workers {
				if w.Config.InstanceID == s.Config.WorkerInstanceID {
					for _, mount := range w.Config.Mounts {
						if mount.RemoteClient == s.ActivateClient.Name {
							return nil, broker.ErrInvalid
						}
						if mount.Client == s.ActivateClient.Name {
							if !mount.ReadOnly || mount.RemoteClient == "" || mount.RemoteClient == mount.Client {
								return nil, broker.ErrInvalid
							}
							assigned = true
						}
					}
				}
			}
			if !assigned {
				return nil, broker.ErrInvalid
			}
		}
		m.materializers[id] = &materializer{}
	}
	for _, r := range c.Rules {
		if m.boxes[r.InstanceID] == nil || len(r.Decision.Methods) == 0 {
			return nil, broker.ErrInvalid
		}
		for _, method := range r.Decision.Methods {
			in := broker.TransferIntent{Method: method, Authority: r.Authority, SourceID: r.SourceID, TargetID: r.TargetID}
			if broker.ValidateIntent(in, nil) != nil || broker.ValidateDecision(in, r.Decision) != nil {
				return nil, broker.ErrInvalid
			}
		}
		found := false
		for _, s := range c.Workers {
			if s.Config.InstanceID == r.InstanceID {
				found = s.Config.BrokerSocket != "" && s.Config.DataEndpoint == r.Decision.WorkerDataEndpoint
			}
		}
		if !found {
			return nil, broker.ErrInvalid
		}
	}
	admitted = true
	return m, nil
}
func (m *Manager) Start(ctx context.Context) (err error) {
	if ctx == nil {
		return broker.ErrInvalid
	}
	m.life.Lock()
	defer m.life.Unlock()
	startup, cancel := context.WithCancel(ctx)
	defer cancel()
	finish := context.AfterFunc(m.stop, cancel)
	defer finish()
	m.mu.Lock()
	if m.started || m.closing {
		m.mu.Unlock()
		return broker.ErrUnavailable
	}
	m.started = true
	m.mu.Unlock()
	defer func() {
		if err != nil {
			m.beginClose()
		}
	}()
	if m.config.RuntimeDirectory != "" {
		if err = control.PreparePrivateDirectory(m.config.RuntimeDirectory); err != nil {
			return err
		}
	}
	for _, s := range m.config.Workers {
		if s.Config.BrokerSocket != "" {
			server, e := control.ListenDecision(s.Config.BrokerSocket, s.Config.InstanceID, m, m.config.MetadataNames)
			if e != nil {
				return e
			}
			m.mu.Lock()
			m.brokers = append(m.brokers, server)
			m.mu.Unlock()
		}
		if err = m.boxes[s.Config.InstanceID].Start(startup); err != nil {
			return err
		}
	}
	for _, s := range m.config.VFS {
		if err = m.startVFS(startup, s); err != nil {
			return err
		}
	}
	m.mu.Lock()
	m.ready = true
	m.mu.Unlock()
	return nil
}

// Ready requires completed activation and live child processes, not only the
// management HTTP listener or a completed spawn call.
func (m *Manager) Ready() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if !m.ready || m.closing {
		return false
	}
	for _, box := range m.boxes {
		if box.State().Phase != "ready" {
			return false
		}
	}
	for _, v := range m.materializers {
		if v.child == nil || !v.child.IsReady() {
			return false
		}
	}
	return true
}
func (m *Manager) Instances() []Instance {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]Instance, 0, len(m.boxes))
	for _, b := range m.boxes {
		s := b.State()
		out = append(out, Instance{Kind: "worker", ID: s.ID, PID: s.PID, Phase: s.Phase})
	}
	for id, v := range m.materializers {
		state := Instance{Kind: "vfs", ID: id, Phase: "new"}
		if v.child != nil {
			state.PID = v.child.PID()
			state.Phase = "exited"
			if v.child.IsReady() {
				state.Phase = "ready"
			}
		}
		if m.closing {
			state.Phase = "stopping"
		}
		if m.closed {
			state.Phase = "closed"
		}
		out = append(out, state)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}
func (m *Manager) Worker(id string) (broker.WorkerControl, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	b := m.boxes[id]
	if m.closing || b == nil {
		return nil, broker.ErrUnavailable
	}
	return b.Control()
}
func (m *Manager) Decide(ctx context.Context, in broker.TransferIntent) (broker.Decision, error) {
	if ctx == nil {
		return broker.Decision{}, broker.ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return broker.Decision{}, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	b := m.boxes[in.Principal]
	if m.closing || b == nil || b.State().Phase != "ready" {
		return broker.Decision{}, broker.ErrDenied
	}
	return resolve(m.config.Rules, in)
}
func (m *Manager) beginClose() {
	m.closeOnce.Do(func() {
		m.cancel()
		m.mu.Lock()
		m.closing = true
		m.mu.Unlock()
		go func() {
			m.life.Lock()
			defer m.life.Unlock()
			// Retire materializers first while the data-plane remote path lives.
			if m.supervisor != nil {
				_ = m.supervisor.Close(context.Background())
			}
			for _, v := range m.materializers {
				if v.control != nil {
					_ = v.control.Close(context.Background())
				}
			}
			var joined sync.WaitGroup
			for _, box := range m.boxes {
				joined.Add(1)
				go func(box *worker.Box) { defer joined.Done(); _ = box.Close(context.Background()) }(box)
			}
			joined.Wait()
			for _, server := range m.brokers {
				_ = server.Close(context.Background())
			}
			m.mu.Lock()
			m.closed = true
			m.mu.Unlock()
			close(m.done)
		}()
	})
}
func (m *Manager) Close(ctx context.Context) error {
	if ctx == nil {
		return broker.ErrInvalid
	}
	m.beginClose()
	select {
	case <-m.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

var _ broker.DecisionBroker = (*Manager)(nil)
