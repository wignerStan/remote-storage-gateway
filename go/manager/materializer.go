package manager

import (
	"context"
	"errors"

	dp "github.com/wignerStan/network-data-plane-worker/go/contracts/worker"
	"github.com/wignerStan/remote-storage-gateway/go/contracts/control/broker"
)

type materializer struct {
	profile string
	box     materializerProcess
}

type materializerControl interface {
	Info(context.Context) (broker.InstanceInfo, error)
	Observed(context.Context) ([]dp.Observed, error)
	Replace(context.Context, string, dp.Version, dp.Selection) (dp.Version, error)
}

func (m *Manager) startMaterializer(ctx context.Context, s MaterializerSpec) error {
	_, _, remote := s.assignment()
	workerControl, err := m.Worker(remote.ExpectedWorkerID)
	if err != nil {
		return err
	}
	return m.startMaterializerProcess(ctx, s, workerControl, newMaterializer)
}

func (m *Manager) startMaterializerProcess(ctx context.Context, s MaterializerSpec, workerControl materializerControl, create func(MaterializerSpec) (materializerProcess, error)) (err error) {
	if ctx == nil || workerControl == nil || create == nil {
		return broker.ErrInvalid
	}
	if err = validateMaterializerProfile(s); err != nil {
		return err
	}
	id, _, remote := s.assignment()
	info, err := workerControl.Info(ctx)
	if err != nil || !info.Ready || info.ID != remote.ExpectedWorkerID {
		return broker.ErrUnavailable
	}
	if info.StorageSocket == "" || info.OfferSocket == "" || info.Incarnation == "" {
		return broker.ErrUnavailable
	}
	if remote.Socket != "" && remote.Socket != info.StorageSocket || remote.OfferSocket != "" && remote.OfferSocket != info.OfferSocket || remote.ExpectedIncarnation != "" && remote.ExpectedIncarnation != info.Incarnation {
		return broker.ErrInvalid
	}
	remote.Socket = info.StorageSocket
	remote.OfferSocket = info.OfferSocket
	remote.ExpectedIncarnation = info.Incarnation
	s, err = s.withRemote(remote)
	if err != nil {
		return err
	}
	child, err := create(s)
	if err != nil {
		return err
	}
	m.mu.Lock()
	m.materializers[id] = &materializer{profile: s.profile(), box: child}
	m.mu.Unlock()
	// A failed candidate is joined before returning. Other started roles are
	// retired by Manager.Start's rollback; their errors remain in Manager.Close.
	defer func() {
		if err != nil {
			err = errors.Join(err, child.Close(context.Background()))
		}
	}()
	if err = child.Start(ctx); err != nil {
		return err
	}
	state, err := child.Info(ctx)
	if err != nil || !state.Ready || state.ID != id || state.WorkerID != info.ID || state.Namespace != remote.Namespace || state.Incarnation != info.Incarnation {
		return broker.ErrUnavailable
	}
	if s.profile() == MaterializerBuildBarnSource {
		if !state.SourceView || state.Generation == nil || *state.Generation != *s.BuildBarnConfig.ExpectedGeneration {
			return broker.ErrUnavailable
		}
	} else if state.SourceView || state.Generation != nil {
		return broker.ErrUnavailable
	}
	if s.ActivateClient == nil {
		return nil
	}
	if s.ExpectedActivation != nil {
		_, err = workerControl.Replace(ctx, s.ActivateClient.Name, *s.ExpectedActivation, *s.ActivateClient)
		return err
	}
	// Preserve the legacy raw-cache activation flow. Immutable source views
	// require ExpectedActivation above and can never enter this discovery path.
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
