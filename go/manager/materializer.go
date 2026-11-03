package manager

import (
	"context"
	rclonebox "github.com/wignerStan/rclone/go/box"
	"github.com/wignerStan/remote-storage-gateway/go/contracts/control/broker"
)

type materializer struct{ box *rclonebox.Box }

func (m *Manager) startMaterializer(ctx context.Context, s MaterializerSpec) error {
	workerControl, err := m.Worker(s.Config.Remote.ExpectedWorkerID)
	if err != nil {
		return err
	}
	info, err := workerControl.Info(ctx)
	if err != nil || !info.Ready || info.ID != s.Config.Remote.ExpectedWorkerID {
		return broker.ErrUnavailable
	}
	if info.StorageSocket == "" || info.OfferSocket == "" || info.Incarnation == "" {
		return broker.ErrUnavailable
	}
	remote := &s.Config.Remote
	if remote.Socket != "" && remote.Socket != info.StorageSocket || remote.OfferSocket != "" && remote.OfferSocket != info.OfferSocket || remote.ExpectedIncarnation != "" && remote.ExpectedIncarnation != info.Incarnation {
		return broker.ErrInvalid
	}
	remote.Socket = info.StorageSocket
	remote.OfferSocket = info.OfferSocket
	remote.ExpectedIncarnation = info.Incarnation
	child, err := rclonebox.New(rclonebox.Config{Executable: s.Executable, ProcessConfig: s.Config})
	if err != nil {
		return err
	}
	m.mu.Lock()
	m.materializers[s.Config.InstanceID] = &materializer{box: child}
	m.mu.Unlock()
	if err = child.Start(ctx); err != nil {
		return err
	}
	state, err := child.Info(ctx)
	if err != nil || !state.Ready || state.WorkerID != info.ID || state.Namespace != remote.Namespace || state.Incarnation != info.Incarnation {
		return broker.ErrUnavailable
	}
	if s.ActivateClient != nil {
		states, err := workerControl.Observed(ctx)
		if err != nil {
			return err
		}
		for _, state := range states {
			if state.Name == s.ActivateClient.Name && state.Accepting {
				_, err = workerControl.Replace(ctx, state.Name, state.Version, *s.ActivateClient)
				return err
			}
		}
		return broker.ErrUnavailable
	}
	return nil
}
