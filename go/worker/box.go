// Package worker manages a separate storage-worker process. It imports only
// passive configuration, process supervision and metadata control transport.
package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	processexec "github.com/wignerStan/infra/go/process/exec"
	"github.com/wignerStan/infra/go/process/supervisor"
	"github.com/wignerStan/remote-storage-gateway/go/contracts/control/broker"
	config "github.com/wignerStan/remote-storage-gateway/go/contracts/worker"
	control "github.com/wignerStan/remote-storage-gateway/go/control/ttrpc"
)

type ProxyEnvironment map[string]string

type Options struct {
	ProxyEnvironment ProxyEnvironment
	Arguments        []string
	Executable       string
	Config           config.ProcessConfig
	ReadyTimeout     time.Duration
	StopTimeout      time.Duration
}
type State struct {
	ID    string
	PID   int
	Phase string
}
type Box struct {
	mu           sync.Mutex
	options      Options
	supervisor   *supervisor.Supervisor
	child        *supervisor.Child
	control      *control.WorkerClient
	state        State
	closing      bool
	done         chan struct{}
	closeOnce    sync.Once
	closeErr     error
	startDone    chan struct{}
	observerDone chan struct{}
}

func New(o Options) (*Box, error) {
	if len(o.Arguments) > 8 {
		return nil, broker.ErrInvalid
	}
	for _, a := range o.Arguments {
		if len(a) > 256 || strings.ContainsAny(a, "\x00\r\n") {
			return nil, broker.ErrInvalid
		}
	}
	o.Arguments = append([]string(nil), o.Arguments...)
	if !filepath.IsAbs(o.Executable) || o.ReadyTimeout < 0 || o.ReadyTimeout > 2*time.Minute || o.StopTimeout < 0 || o.StopTimeout > 10*time.Minute {
		return nil, broker.ErrInvalid
	}
	if o.ReadyTimeout == 0 {
		o.ReadyTimeout = 30 * time.Second
	}
	if o.StopTimeout == 0 {
		o.StopTimeout = 30 * time.Second
	}
	proxies := ProxyEnvironment{}
	for key, value := range o.ProxyEnvironment {
		switch key {
		case "HTTP_PROXY", "HTTPS_PROXY", "NO_PROXY", "http_proxy", "https_proxy", "no_proxy":
		default:
			return nil, broker.ErrInvalid
		}
		if len(value) > 8192 {
			return nil, broker.ErrInvalid
		}
		for _, c := range value {
			if c == 0 || c == '\r' || c == '\n' {
				return nil, broker.ErrInvalid
			}
		}
		proxies[key] = value
	}
	o.ProxyEnvironment = proxies
	s, e := supervisor.New(supervisor.Options{MaxChildren: 1, GracePeriod: o.StopTimeout, DiagnosticBytes: 64 << 10})
	if e != nil {
		return nil, e
	}
	b := &Box{options: o, supervisor: s, state: State{Phase: "new"}, done: make(chan struct{})}
	if e = b.SetConfig(o.Config); e != nil {
		_ = s.Close(context.Background())
		return nil, e
	}
	return b, nil
}
func cloneConfig(in config.ProcessConfig) (config.ProcessConfig, error) {
	var out config.ProcessConfig
	b, e := json.Marshal(in)
	if e != nil || len(b) > 1<<20 {
		return out, broker.ErrInvalid
	}
	if json.Unmarshal(b, &out) != nil {
		return out, broker.ErrInvalid
	}
	return out, nil
}
func (b *Box) SetConfig(c config.ProcessConfig) error {
	copy, e := cloneConfig(c)
	if e != nil || copy.InstanceID == "" || len(copy.InstanceID) > 128 || !filepath.IsAbs(copy.ControlSocket) || len(copy.ControlSocket) > 107 || copy.BrokerSocket != "" && !filepath.IsAbs(copy.BrokerSocket) {
		return broker.ErrInvalid
	}
	if (copy.StorageIPCSocket == "") != (copy.StorageOfferSocket == "") {
		return broker.ErrInvalid
	}
	seen := map[string]bool{filepath.Clean(copy.ControlSocket): true, filepath.Clean(copy.BrokerSocket): true}
	for _, p := range []string{copy.StorageIPCSocket, copy.StorageOfferSocket} {
		if p == "" {
			continue
		}
		if !filepath.IsAbs(p) || filepath.Clean(p) != p || len(p) > 107 || seen[p] {
			return broker.ErrInvalid
		}
		seen[p] = true
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state.Phase != "new" || b.closing {
		return errors.New("worker config is already admitted")
	}
	b.options.Config = copy
	b.state.ID = copy.InstanceID
	return nil
}
func privateSocket(path string) error {
	info, e := os.Lstat(filepath.Dir(path))
	if e != nil || !info.IsDir() || info.Mode().Perm()&0077 != 0 {
		return broker.ErrInvalid
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || int(stat.Uid) != os.Geteuid() {
		return broker.ErrInvalid
	}
	if _, e = os.Lstat(path); !os.IsNotExist(e) {
		return broker.ErrInvalid
	}
	return nil
}
func (b *Box) Start(ctx context.Context) error {
	if ctx == nil {
		return broker.ErrInvalid
	}
	b.mu.Lock()
	if b.state.Phase != "new" || b.closing {
		b.mu.Unlock()
		return broker.ErrUnavailable
	}
	b.state.Phase = "starting"
	b.startDone = make(chan struct{})
	startDone := b.startDone
	o := b.options
	b.mu.Unlock()
	success := false
	defer func() {
		b.mu.Lock()
		if !success && !b.closing {
			b.state.Phase = "failed"
		}
		b.mu.Unlock()
		close(startDone)
	}()
	if e := privateSocket(o.Config.ControlSocket); e != nil {
		b.mu.Lock()
		b.state.Phase = "failed"
		b.mu.Unlock()
		return e
	}
	payload, e := json.Marshal(o.Config)
	if e != nil {
		return broker.ErrInvalid
	}
	defer clear(payload)
	command := processexec.NewStartSpec(o.Executable, append(o.Arguments, "--config=-")...).WithStdin(bytes.NewReader(payload))
	for key, value := range o.ProxyEnvironment {
		command = command.WithExtraEnv(key, value)
	}
	var client *control.WorkerClient
	child, e := b.supervisor.Start(ctx, supervisor.Spec{Command: command, ReadyTimeout: o.ReadyTimeout, Ready: func(ready context.Context, c *supervisor.Child) error {
		b.mu.Lock()
		b.state.PID = c.PID()
		b.mu.Unlock()
		attempt, cancel := context.WithTimeout(ready, 500*time.Millisecond)
		defer cancel()
		candidate, err := control.DialWorker(attempt, o.Config.ControlSocket)
		if err != nil {
			return err
		}
		info, err := candidate.Info(attempt)
		if err != nil || !info.Ready || info.ID != o.Config.InstanceID || o.Config.StorageIPCSocket != "" && (info.StorageSocket != o.Config.StorageIPCSocket || info.OfferSocket != o.Config.StorageOfferSocket || info.Incarnation == "") {
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
		b.mu.Lock()
		if !b.closing {
			b.state.Phase = "failed"
		}
		b.mu.Unlock()
		return e
	}
	b.mu.Lock()
	if b.closing {
		b.mu.Unlock()
		client.Close(context.Background())
		child.Stop(context.Background())
		return broker.ErrUnavailable
	}
	b.child, b.control = child, client
	b.state.Phase = "ready"
	b.observerDone = make(chan struct{})
	observerDone := b.observerDone
	b.mu.Unlock()
	go func() {
		defer close(observerDone)
		_ = child.Wait(context.Background())
		_ = client.Close(context.Background())
		b.mu.Lock()
		if !b.closing {
			b.state.Phase = "exited"
		}
		b.mu.Unlock()
	}()
	success = true
	return nil
}
func (b *Box) State() State { b.mu.Lock(); defer b.mu.Unlock(); return b.state }
func (b *Box) Control() (broker.WorkerControl, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closing || b.state.Phase != "ready" || b.control == nil {
		return nil, broker.ErrUnavailable
	}
	return b.control, nil
}
func (b *Box) Close(ctx context.Context) error {
	if ctx == nil {
		return broker.ErrInvalid
	}
	b.closeOnce.Do(func() {
		b.mu.Lock()
		b.closing = true
		b.state.Phase = "stopping"
		startDone := b.startDone
		b.mu.Unlock()
		go func() {
			err := b.supervisor.Close(context.Background())
			if startDone != nil {
				<-startDone
			}
			b.mu.Lock()
			client, observerDone := b.control, b.observerDone
			b.mu.Unlock()
			if client != nil {
				_ = client.Close(context.Background())
			}
			if observerDone != nil {
				<-observerDone
			}
			b.mu.Lock()
			b.closeErr = err
			b.state.Phase = "closed"
			b.mu.Unlock()
			close(b.done)
		}()
	})
	select {
	case <-b.done:
		b.mu.Lock()
		e := b.closeErr
		b.mu.Unlock()
		return e
	case <-ctx.Done():
		return ctx.Err()
	}
}

// ProcessSpec exposes this admitted process plan to an assembly-owned Infra
// group. The group owns the child; this Box has not started an independent owner.
func (b *Box) ProcessSpec() (supervisor.Spec, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state.Phase != "new" || b.closing {
		return supervisor.Spec{}, broker.ErrUnavailable
	}
	o := b.options
	if e := privateSocket(o.Config.ControlSocket); e != nil {
		return supervisor.Spec{}, e
	}
	payload, e := json.Marshal(o.Config)
	if e != nil {
		return supervisor.Spec{}, broker.ErrInvalid
	}
	command := processexec.NewStartSpec(o.Executable, append(append([]string(nil), o.Arguments...), "--config=-")...)
	if o.Executable == "/proc/self/exe" {
		command, e = supervisor.SelfCommand(append(append([]string(nil), o.Arguments...), "--config=-")...)
		if e != nil {
			return supervisor.Spec{}, e
		}
	}
	command = command.WithStdin(bytes.NewReader(payload))
	for key, value := range o.ProxyEnvironment {
		command = command.WithExtraEnv(key, value)
	}
	return supervisor.Spec{Command: command, ReadyTimeout: o.ReadyTimeout, Ready: func(ctx context.Context, child *supervisor.Child) error {
		attempt, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		defer cancel()
		client, e := control.DialWorker(attempt, o.Config.ControlSocket)
		if e != nil {
			return e
		}
		defer client.Close(context.Background())
		info, e := client.Info(attempt)
		if e != nil || !info.Ready || info.ID != o.Config.InstanceID || o.Config.StorageIPCSocket != "" && (info.StorageSocket != o.Config.StorageIPCSocket || info.OfferSocket != o.Config.StorageOfferSocket || info.Incarnation == "") {
			return broker.ErrUnavailable
		}
		return nil
	}}, nil
}
