package controlplane

import (
	"bytes"
	"context"
	"encoding/json"
	"net/url"
	"strings"
	"time"

	processexec "github.com/wignerStan/infra/go/process/exec"
	"github.com/wignerStan/infra/go/process/supervisor"
	"github.com/wignerStan/remote-storage-gateway/go/contracts/control/broker"
	control "github.com/wignerStan/remote-storage-gateway/go/control/ttrpc"
)

type materializer struct {
	child   *supervisor.Child
	control *control.VFSClient
}

func (m *Manager) startVFS(ctx context.Context, s VFSSpec) error {
	workerControl, e := m.Worker(s.Config.WorkerInstanceID)
	if e != nil {
		return e
	}
	workerInfo, e := workerControl.Info(ctx)
	if e != nil || !workerInfo.Ready || workerInfo.ID != s.Config.WorkerInstanceID {
		return broker.ErrUnavailable
	}

	expectedSocket := ""
	for _, assigned := range m.config.Workers {
		if assigned.Config.InstanceID == s.Config.WorkerInstanceID && assigned.Config.ControlSocket == s.Config.WorkerControlSocket {
			expectedSocket = assigned.Config.LocalDataSocket
		}
	}
	if workerInfo.LocalDataSocket != expectedSocket || s.Config.WorkerLocalDataSocket != "" && s.Config.WorkerLocalDataSocket != expectedSocket {
		return broker.ErrInvalid
	}
	s.Config.WorkerLocalDataSocket = expectedSocket
	endpoint, e := materializerEndpoint(workerInfo.DataEndpoint, s.Config.DataEndpoint)
	if e != nil {
		return e
	}
	s.Config.DataEndpoint = endpoint
	if e = control.ValidateVacantSocket(s.Config.ControlSocket); e != nil {
		return e
	}
	payload, e := json.Marshal(s.Config)
	if e != nil || len(payload) > 1<<20 {
		return broker.ErrInvalid
	}
	defer clear(payload)
	var client *control.VFSClient
	child, e := m.supervisor.Start(ctx, supervisor.Spec{Command: processexec.NewStartSpec(s.Executable, "--config=-").WithStdin(bytes.NewReader(payload)), ReadyTimeout: 30 * time.Second, Ready: func(ctx context.Context, _ *supervisor.Child) error {
		probe, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		defer cancel()
		candidate, e := control.DialVFS(probe, s.Config.ControlSocket)
		if e != nil {
			return e
		}
		status, e := candidate.Info(probe)
		if e != nil || !status.Ready || status.ID != s.Config.InstanceID {
			candidate.Close(context.Background())
			return broker.ErrUnavailable
		}
		client = candidate
		return nil
	}})
	if e != nil {
		if client != nil {
			client.Close(context.Background())
		}
		return e
	}
	m.mu.Lock()
	m.materializers[s.Config.InstanceID] = &materializer{child, client}
	m.mu.Unlock()
	if s.ActivateClient != nil {
		states, e := workerControl.Observed(ctx)
		if e != nil {
			return e
		}
		for _, state := range states {
			if state.Name == s.ActivateClient.Name && state.Accepting {
				_, e = workerControl.Replace(ctx, state.Name, state.Version, *s.ActivateClient)
				return e
			}
		}
		return broker.ErrUnavailable
	}
	return nil
}

// Operator configuration selects only the remote-only mount path. The actual
// origin comes from the ready child's typed control port, including dynamic ports.
func materializerEndpoint(observed, selected string) (string, error) {
	origin, e := url.Parse(observed)
	if e != nil || origin.Host == "" || (origin.Scheme != "http" && origin.Scheme != "https") || origin.User != nil || origin.RawQuery != "" || origin.Fragment != "" || (origin.Path != "" && origin.Path != "/") {
		return "", broker.ErrInvalid
	}
	path, e := url.Parse(selected)
	if e != nil || path.User != nil || path.RawQuery != "" || path.Fragment != "" || path.RawPath != "" || path.Opaque != "" {
		return "", broker.ErrInvalid
	}
	if path.IsAbs() && (path.Scheme != origin.Scheme || path.Host != origin.Host) {
		return "", broker.ErrInvalid
	}
	if !path.IsAbs() && path.Host != "" {
		return "", broker.ErrInvalid
	}
	parts := strings.Split(path.Path, "/")
	if len(parts) != 4 || parts[0] != "" || parts[1] != "remote-dav" || parts[2] == "" || parts[2] == "." || parts[2] == ".." || strings.ContainsAny(parts[2], "\\\x00") || parts[3] != "" {
		return "", broker.ErrInvalid
	}
	origin.Path = path.Path
	return origin.String(), nil
}
