//go:build dataplane_l7

package runtime

import (
	"context"
	"errors"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"

	"github.com/wignerStan/infra/go/network/proxy/l7"
	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/descriptor"
	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/descriptor/admission"
	dp "github.com/wignerStan/network-data-plane-worker/go/contracts/worker"
	"github.com/wignerStan/remote-storage-gateway/go/contracts/control/broker"
	workerconfig "github.com/wignerStan/remote-storage-gateway/go/contracts/worker"
	controlgrpc "github.com/wignerStan/remote-storage-gateway/go/control/grpc"
	control "github.com/wignerStan/remote-storage-gateway/go/control/ttrpc"
)

func ReadConfig(input io.Reader) (workerconfig.ProcessConfig, error) {
	var c workerconfig.ProcessConfig
	if input == nil {
		return c, broker.ErrInvalid
	}
	b, e := io.ReadAll(io.LimitReader(input, descriptor.MaxBytes+1))
	if e != nil || len(b) > descriptor.MaxBytes {
		return c, broker.ErrInvalid
	}
	if admission.StrictJSON(b, &c) != nil {
		return c, broker.ErrInvalid
	}
	return c, nil
}
func New(ctx context.Context, c workerconfig.ProcessConfig) (*Runtime, error) {
	if ctx == nil {
		return nil, broker.ErrInvalid
	}
	if c.DataEndpoint != "" {
		u, e := url.Parse(c.DataEndpoint)
		if e != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
			return nil, broker.ErrInvalid
		}
	}
	if c.BrokerSocket != "" && c.RemoteBroker != nil {
		return nil, broker.ErrInvalid
	}
	r, e := assemble(ctx, c.RuntimeConfig)
	if e != nil {
		return nil, e
	}
	r.process = c
	if e = r.prepareEncoded(ctx); e != nil {
		_ = r.Close(context.Background())
		return nil, e
	}
	return r, nil
}
func (r *Runtime) Start(ctx context.Context) error {
	if ctx == nil {
		return broker.ErrInvalid
	}
	if e := ctx.Err(); e != nil {
		return e
	}
	r.lifeMu.Lock()
	defer r.lifeMu.Unlock()
	if r.started || r.retiring {
		return broker.ErrUnavailable
	}
	var listener net.Listener
	var entry *control.Server
	if r.process.ControlSocket != "" {
		p := r.process.ControlSocket
		parent, e := os.Stat(filepath.Dir(p))
		if e != nil || !filepath.IsAbs(p) || len(p) > 107 || r.process.InstanceID == "" || !parent.IsDir() || parent.Mode().Perm()&0077 != 0 {
			return broker.ErrInvalid
		}
		if _, e = os.Lstat(p); !os.IsNotExist(e) {
			return broker.ErrInvalid
		}
		listener, e = net.Listen("unix", p)
		if e != nil {
			return e
		}
		if e = os.Chmod(p, 0600); e != nil {
			listener.Close()
			return e
		}
		entry, e = control.NewWorkerServer(r)
		if e != nil {
			listener.Close()
			return e
		}
	}

	storage, e := r.prepareStorageIPC(ctx)
	if e != nil {
		if listener != nil {
			listener.Close()
		}
		return e
	}
	defer func() {
		if !r.started && storage != nil {
			_ = storage.close(context.Background())
		}
	}()
	server, e := l7.NewServer(l7.ServerOptions{Handler: r.handler, TLSConfig: r.tlsConfig})
	if e != nil {
		if listener != nil {
			listener.Close()
		}
		return e
	}
	data, e := net.Listen("tcp", r.config.Listen)
	if e != nil {
		if listener != nil {
			listener.Close()
		}
		return e
	}

	if e = server.Start(data); e != nil {
		data.Close()
		if listener != nil {
			listener.Close()
		}
		return e
	}
	r.httpServer = server
	r.storageIPC = storage
	r.controlServer = entry
	r.started = true
	if r.process.DataEndpoint == "" {
		scheme := "http://"
		if r.tlsConfig != nil {
			scheme = "https://"
		}
		r.process.DataEndpoint = scheme + data.Addr().String()
	}
	if r.process.RPC != nil {
		remote, err := controlgrpc.StartServer(ctx, *r.process.RPC, r, nil, nil)
		if err != nil {
			_ = server.Shutdown(context.Background())
			if listener != nil {
				listener.Close()
			}
			return err
		}
		r.remoteServer = remote
	}
	if entry != nil {
		go func() { _ = entry.Serve(listener) }()
	}
	go func() {
		var storageDone, remoteDone <-chan struct{}
		if storage != nil {
			storageDone = storage.server.Done()
		}
		if r.remoteServer != nil {
			remoteDone = r.remoteServer.Done()
		}
		select {
		case <-server.Done():
		case <-storageDone:
		case <-remoteDone:
		}
		close(r.stopped)
	}()

	return nil
}
func (r *Runtime) Done() <-chan struct{} { return r.stopped }
func (r *Runtime) Close(ctx context.Context) error {
	if ctx == nil {
		return broker.ErrInvalid
	}
	r.closeOnce.Do(func() {
		r.lifeMu.Lock()
		r.retiring = true
		server, storage, entry, remote := r.httpServer, r.storageIPC, r.controlServer, r.remoteServer
		r.lifeMu.Unlock()
		go func() {
			var errs []error
			errs = append(errs, r.closeEncoded(context.Background()))
			if remote != nil {
				errs = append(errs, remote.Close(context.Background()))
			}
			if entry != nil {
				errs = append(errs, entry.Close(context.Background()))
			}
			if server != nil {
				errs = append(errs, server.Shutdown(context.Background()))
			}
			if storage != nil {
				errs = append(errs, storage.close(context.Background()))
			}
			if r.Runtime != nil {
				errs = append(errs, r.Runtime.Close(context.Background()))
			} else if r.Manager != nil {
				errs = append(errs, r.Manager.Close(context.Background()))
			}
			if r.transport != nil {
				r.transport.CloseIdleConnections()
			}
			r.lifeMu.Lock()
			r.closeErr = errors.Join(errs...)
			r.lifeMu.Unlock()
			close(r.closeDone)
		}()
	})
	select {
	case <-r.closeDone:
		r.lifeMu.Lock()
		e := r.closeErr
		r.lifeMu.Unlock()
		return e
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (r *Runtime) Info(context.Context) (broker.InstanceInfo, error) {
	r.lifeMu.Lock()
	defer r.lifeMu.Unlock()
	ready := r.started && !r.retiring
	if ready && r.httpServer != nil {
		select {
		case <-r.httpServer.Done():
			ready = false
		default:
		}
	}
	if ready && r.storageIPC != nil {
		select {
		case <-r.storageIPC.server.Done():
			ready = false
		default:
		}
	}
	if ready && r.remoteServer != nil {
		select {
		case <-r.remoteServer.Done():
			ready = false
		default:
		}
	}
	info := broker.InstanceInfo{Contract: broker.Contract, ID: r.process.InstanceID, Ready: ready, DataEndpoint: r.process.DataEndpoint}
	if ready && r.storageIPC != nil {
		info.StorageSocket = r.config.StorageIPCSocket
		info.OfferSocket = r.config.StorageOfferSocket
		info.Incarnation = r.storageIPC.server.Info().Incarnation
	}
	return info, nil
}
func (r *Runtime) Manifest(context.Context) (dp.Manifest, error) {
	m := r.manifest
	m.Controls = append([]string{}, m.Controls...)
	m.Lanes = append([]string{}, m.Lanes...)
	m.PluginRuntimes = append([]string{}, m.PluginRuntimes...)
	m.PluginDescriptors = append(m.PluginDescriptors[:0:0], m.PluginDescriptors...)
	if r.encoded != nil && r.encoded.runtime != nil {
		record := r.encoded.runtime.Record()
		m.PluginDescriptors = append(m.PluginDescriptors, dp.PluginCapability{PluginID: record.Descriptor.PluginID, Contract: record.Descriptor.Contract, PluginVersion: record.Descriptor.PluginVersion, DescriptorDigest: record.Digest})
	}
	return m, nil
}
func (r *Runtime) Observed(context.Context) ([]dp.Observed, error) { return r.Manager.Observed(), nil }
func (r *Runtime) Descriptor(_ context.Context, digest string) (descriptor.Record, error) {
	if r.encoded != nil && r.encoded.runtime != nil {
		record := r.encoded.runtime.Record()
		if record.Digest == digest {
			return record, nil
		}
	}
	if r.Registry == nil {
		return descriptor.Record{}, broker.ErrInvalid
	}
	for _, record := range r.Registry.Records() {
		if record.Digest == digest {
			return record, nil
		}
	}
	return descriptor.Record{}, broker.ErrInvalid
}
func (r *Runtime) Replace(ctx context.Context, name string, expected dp.Version, selection dp.Selection) (dp.Version, error) {
	allowed := false
	for _, s := range r.config.Worker.Clients {
		allowed = allowed || s.Name == name
	}
	if !allowed || selection.Name != name {
		return dp.Version{}, broker.ErrInvalid
	}
	return r.Manager.Replace(ctx, expected, selection)
}
