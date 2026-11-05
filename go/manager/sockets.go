package manager

import (
	"context"
	"errors"
	"github.com/wignerStan/infra/go/process/supervisor"
	"github.com/wignerStan/remote-storage-gateway/go/contracts/control/broker"
	control "github.com/wignerStan/remote-storage-gateway/go/control/ttrpc"
	"os"
	"syscall"
)

func workerSockets(s WorkerSpec) []string {
	return []string{s.Config.ControlSocket, s.Config.StorageIPCSocket, s.Config.StorageOfferSocket}
}
func planeSockets(s ControlPlaneSpec) []string {
	var out []string
	for _, w := range s.Control.Workers {
		if w.BrokerSocket != "" {
			out = append(out, w.BrokerSocket)
		}
	}
	return out
}

// Before any child exists, every selected runtime endpoint must be vacant.
func (m *Manager) admitSockets() error {
	paths := map[string]bool{}
	for _, s := range m.config.Workers {
		for _, p := range workerSockets(s) {
			paths[p] = true
		}
	}
	for _, s := range m.config.ControlPlanes {
		for _, p := range planeSockets(s) {
			paths[p] = true
		}
	}
	for _, s := range m.config.Materializers {
		_, socket, _ := s.assignment()
		paths[socket] = true
	}
	for p := range paths {
		if p != "" {
			if e := control.ValidateVacantSocket(p); e != nil {
				return e
			}
		}
	}
	return nil
}
func (m *Manager) trackSockets(paths []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, p := range paths {
		if p == "" {
			continue
		}
		info, e := os.Lstat(p)
		if os.IsNotExist(e) {
			continue
		}
		if e != nil {
			return e
		}
		stat, ok := info.Sys().(*syscall.Stat_t)
		if info.Mode()&os.ModeSocket == 0 || !ok || int(stat.Uid) != os.Geteuid() {
			return broker.ErrInvalid
		}
		if previous := m.sockets[p]; previous != nil && !os.SameFile(previous, info) {
			return broker.ErrInvalid
		}
		m.sockets[p] = info
	}
	return nil
}
func (m *Manager) watchSockets(spec *supervisor.Spec, paths []string) {
	ready := spec.Ready
	spec.Ready = func(ctx context.Context, child *supervisor.Child) error {
		if err := m.trackSockets(paths); err != nil {
			return err
		}
		var readiness error
		if ready != nil {
			readiness = ready(ctx, child)
		}
		return errors.Join(readiness, m.trackSockets(paths))
	}
}

// Reaped children cannot unlink after SIGKILL. Retire only the exact socket
// inode observed for this admitted launch; replacements remain with their owner.
func (m *Manager) retireSockets() error {
	var errs []error
	for path, owned := range m.sockets {
		info, e := os.Lstat(path)
		if os.IsNotExist(e) {
			continue
		}
		if e != nil {
			errs = append(errs, e)
			continue
		}
		if info.Mode()&os.ModeSocket == 0 || !os.SameFile(info, owned) {
			errs = append(errs, broker.ErrInvalid)
			continue
		}
		errs = append(errs, os.Remove(path))
	}
	return errors.Join(errs...)
}
