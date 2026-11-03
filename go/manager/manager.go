// Package manager assembles the same-host bundle. Control planes, workers and
// optional materializers remain separate child processes.
package manager

import (
	"context"
	"errors"
	"fmt"
	configsource "github.com/wignerStan/infra/go/config/source"
	rclonebox "github.com/wignerStan/rclone/go/box"

	"github.com/wignerStan/infra/go/process/supervisor"
	"path/filepath"
	"sort"
	"sync"
	"time"

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
type MaterializerSpec struct {
	ActivateClient *dp.Selection           `json:"activate_client,omitempty"`
	Executable     string                  `json:"executable"`
	Config         rclonebox.ProcessConfig `json:"config"`
}
type Config struct {
	ControlPlanes    []ControlPlaneSpec `json:"control_planes"`
	RuntimeDirectory string             `json:"runtime_directory,omitempty"`
	Materializers    []MaterializerSpec `json:"materializers,omitempty"`
	Workers          []WorkerSpec       `json:"workers"`
}
type Instance struct {
	Kind  string `json:"kind"`
	ID    string `json:"id"`
	PID   int    `json:"pid"`
	Phase string `json:"phase"`
}
type Manager struct {
	materializers map[string]*materializer
	stop          context.Context
	cancel        context.CancelFunc
	life          sync.Mutex
	mu            sync.RWMutex
	config        Config
	boxes         map[string]*worker.Box
	group         *supervisor.Group
	clients       map[string]*control.WorkerClient
	planes        map[string]*controlPlane
	ready         bool
	closed        bool
	started       bool
	closing       bool
	closeOnce     sync.Once
	done          chan struct{}
	closeErr      error
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
	if len(c.ControlPlanes) > 16 || len(c.Materializers) > 64 || len(c.Workers) > 64 {
		return nil, broker.ErrInvalid
	}
	stop, cancel := context.WithCancel(context.Background())
	m := &Manager{stop: stop, cancel: cancel, boxes: map[string]*worker.Box{}, clients: map[string]*control.WorkerClient{}, planes: map[string]*controlPlane{}, done: make(chan struct{})}
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
	paths := map[string]bool{}
	for _, spec := range c.Workers {
		id := spec.Config.InstanceID
		if m.boxes[id] != nil {
			return nil, broker.ErrInvalid
		}
		for _, path := range []string{spec.Config.ControlSocket, spec.Config.BrokerSocket, spec.Config.StorageIPCSocket, spec.Config.StorageOfferSocket} {
			if path == "" {
				continue
			}
			if paths[path] || !filepath.IsAbs(path) || len(path) > 107 || c.RuntimeDirectory != "" && filepath.Dir(path) != c.RuntimeDirectory {
				return nil, broker.ErrInvalid
			}
			paths[path] = true
		}
		executable := spec.Executable
		arguments := []string(nil)
		if executable == "" {
			self, err := supervisor.SelfCommand()
			e = err
			executable = self.Program
			if e != nil {
				return nil, e
			}
			arguments = []string{"worker"}
		}
		box, e := worker.New(worker.Options{Executable: executable, Arguments: arguments, Config: spec.Config, ProxyEnvironment: proxyEnvironment()})
		if e != nil {
			return nil, e
		}
		m.boxes[id] = box
	}
	for _, s := range c.Materializers {
		id := s.Config.InstanceID
		if id == "" || m.boxes[id] != nil || m.materializers[id] != nil || !filepath.IsAbs(s.Executable) || !filepath.IsAbs(s.Config.ControlSocket) || len(s.Config.ControlSocket) > 107 || paths[s.Config.ControlSocket] || c.RuntimeDirectory != "" && filepath.Dir(s.Config.ControlSocket) != c.RuntimeDirectory {
			return nil, broker.ErrInvalid
		}
		paths[s.Config.ControlSocket] = true
		found := false
		for _, w := range c.Workers {
			if w.Config.InstanceID != s.Config.Remote.ExpectedWorkerID {
				continue
			}
			remote := s.Config.Remote
			if w.Config.StorageIPCSocket == "" || w.Config.StorageOfferSocket == "" || remote.Socket != "" && remote.Socket != w.Config.StorageIPCSocket || remote.OfferSocket != "" && remote.OfferSocket != w.Config.StorageOfferSocket {
				return nil, broker.ErrInvalid
			}
			for _, mount := range w.Config.Mounts {
				if mount.Name == remote.Namespace && mount.RemoteClient != "" {
					found = true
				}
			}
		}
		if !found {
			return nil, broker.ErrInvalid
		}
		if s.ActivateClient != nil {
			assigned := false
			for _, w := range c.Workers {
				if w.Config.InstanceID == s.Config.Remote.ExpectedWorkerID {
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
	for _, spec := range c.ControlPlanes {
		if m.planes[spec.ID] != nil {
			return nil, broker.ErrInvalid
		}
		plane, e := newControlPlane(spec)
		if e != nil {
			return nil, e
		}
		m.planes[spec.ID] = plane
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
	roles := make([]supervisor.Role, 0, len(m.config.ControlPlanes)+len(m.config.Workers))
	dependencies := make([]string, 0, len(m.config.ControlPlanes))
	for _, spec := range m.config.ControlPlanes {
		plane := m.planes[spec.ID]
		process, err := plane.processSpec()
		if err != nil {
			return err
		}
		name := "control-plane/" + spec.ID
		roles = append(roles, supervisor.Role{Name: name, Spec: process})
		dependencies = append(dependencies, name)
	}
	for _, spec := range m.config.Workers {
		process, err := m.boxes[spec.Config.InstanceID].ProcessSpec()
		if err != nil {
			return err
		}
		roles = append(roles, supervisor.Role{Name: "worker/" + spec.Config.InstanceID, Depends: dependencies, Spec: process})
	}
	if len(roles) != 0 {
		group, err := supervisor.StartGroup(startup, supervisor.Plan{Options: supervisor.Options{MaxChildren: 80, GracePeriod: 30 * time.Second, DiagnosticBytes: 64 << 10}, Roles: roles})
		if err != nil {
			return err
		}
		m.mu.Lock()
		m.group = group
		m.mu.Unlock()
	}
	for _, s := range m.config.Materializers {
		if err = m.startMaterializer(startup, s); err != nil {
			return fmt.Errorf("rclone %s: %w", s.Config.InstanceID, err)
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
	if m.group != nil {
		for _, spec := range m.config.Workers {
			child, ok := m.group.Child("worker/" + spec.Config.InstanceID)
			if !ok {
				return false
			}
			select {
			case <-child.Done():
				return false
			default:
			}
		}
		for _, spec := range m.config.ControlPlanes {
			child, ok := m.group.Child("control-plane/" + spec.ID)
			if !ok {
				return false
			}
			select {
			case <-child.Done():
				return false
			default:
			}
		}
	}
	for _, v := range m.materializers {
		if v.box == nil || v.box.State().Phase != "ready" {
			return false
		}
	}
	return true
}
func (m *Manager) Instances() []Instance {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]Instance, 0, len(m.boxes))
	appendRole := func(kind, id string) {
		state := Instance{Kind: kind, ID: id, Phase: "new"}
		if m.group != nil {
			if child, ok := m.group.Child(kind + "/" + id); ok {
				state.PID = child.PID()
				state.Phase = "ready"
				select {
				case <-child.Done():
					state.Phase = "exited"
				default:
				}
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
	for _, spec := range m.config.Workers {
		appendRole("worker", spec.Config.InstanceID)
	}
	for _, spec := range m.config.ControlPlanes {
		appendRole("control-plane", spec.ID)
	}
	for id, v := range m.materializers {
		state := Instance{Kind: "rclone", ID: id, Phase: "new"}
		if v.box != nil {
			child := v.box.State()
			state.PID = child.PID
			state.Phase = child.Phase
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
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closing || m.group == nil || m.boxes[id] == nil {
		return nil, broker.ErrUnavailable
	}
	child, ok := m.group.Child("worker/" + id)
	if !ok {
		return nil, broker.ErrUnavailable
	}
	select {
	case <-child.Done():
		return nil, broker.ErrUnavailable
	default:
	}
	if client := m.clients[id]; client != nil {
		return client, nil
	}
	for _, spec := range m.config.Workers {
		if spec.Config.InstanceID == id {
			ctx, cancel := context.WithTimeout(m.stop, time.Second)
			defer cancel()
			client, err := control.DialWorker(ctx, spec.Config.ControlSocket)
			if err != nil {
				return nil, err
			}
			m.clients[id] = client
			return client, nil
		}
	}
	return nil, broker.ErrUnavailable
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
			var errs []error
			// Retire materializers first while the data-plane remote path lives.
			for _, v := range m.materializers {
				if v.box != nil {
					errs = append(errs, v.box.Close(context.Background()))
				}
			}
			if m.group != nil {
				errs = append(errs, m.group.Close(context.Background()))
			}
			for _, client := range m.clients {
				errs = append(errs, client.Close(context.Background()))
			}
			var joined sync.WaitGroup
			for _, box := range m.boxes {
				joined.Add(1)
				go func(box *worker.Box) { defer joined.Done(); _ = box.Close(context.Background()) }(box)
			}
			joined.Wait()
			for _, plane := range m.planes {
				_ = plane.close(context.Background())
			}
			m.mu.Lock()
			m.closeErr = errors.Join(errs...)
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
		m.mu.RLock()
		err := m.closeErr
		m.mu.RUnlock()
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}
