//go:build dataplane_l7

package runtime

import (
	"context"
	"errors"
	"path/filepath"

	transport "github.com/wignerStan/network-data-plane-worker/go/clienttransport/ttrpc"
	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/encoded"
	"github.com/wignerStan/network-data-plane-worker/go/contracts/storageipc"
	"github.com/wignerStan/remote-storage-gateway/go/contracts/control/broker"
	workerconfig "github.com/wignerStan/remote-storage-gateway/go/contracts/worker"
	control "github.com/wignerStan/remote-storage-gateway/go/control/ttrpc"
)

type storageBridge struct {
	server          *transport.Server
	broker          decisionClient
	metadata, offer *control.PrivateUnixListener
}

// Only an explicit raw assignment is exposed to materializers. Ordinary public
// mounts remain usable without gaining a storage IPC namespace or selecting
// their potentially cached public client as a fallback.
func storageNamespaces(mounts []workerconfig.Mount) (map[string]string, error) {
	names := map[string]string{}
	for _, mount := range mounts {
		if mount.RemoteClient == "" {
			continue
		}
		if mount.Name == "" || names[mount.Name] != "" {
			return nil, broker.ErrInvalid
		}
		names[mount.Name] = mount.RemoteClient
	}
	return names, nil
}

// prepareStorageIPC admits only operator-assigned raw mount clients. The control
// broker grants namespace metadata; the Worker creates and owns payload arenas.
func (r *Runtime) prepareStorageIPC(ctx context.Context) (_ *storageBridge, err error) {
	p, q := r.config.StorageIPCSocket, r.config.StorageOfferSocket
	if p == "" && q == "" {
		return nil, nil
	}
	if p == "" || q == "" || r.process.InstanceID == "" || r.process.BrokerSocket == "" && r.process.RemoteBroker == nil || r.Manager == nil {
		return nil, broker.ErrInvalid
	}
	seen := map[string]bool{filepath.Clean(r.process.ControlSocket): true, filepath.Clean(r.process.BrokerSocket): true}
	for _, socket := range []string{p, q} {
		if filepath.Clean(socket) != socket || seen[socket] {
			return nil, broker.ErrInvalid
		}
		seen[socket] = true
		if e := control.ValidateVacantSocket(socket); e != nil {
			return nil, e
		}
	}
	names, err := storageNamespaces(r.config.Mounts)
	if err != nil {
		return nil, err
	}
	bridge := &storageBridge{}
	defer func() {
		if err != nil {
			_ = bridge.close(context.Background())
		}
	}()
	bridge.broker, err = r.decisionClient(ctx, nil)
	if err != nil {
		return nil, broker.ErrUnavailable
	}
	authorize, err := NewAuthorizer(bridge.broker, nil)
	if err != nil {
		return nil, err
	}
	bridge.server, err = transport.NewServer(transport.ServerOptions{WorkerID: r.process.InstanceID, Clients: r.Manager, Namespaces: names, Authorize: func(ctx context.Context, namespace, client string) (transport.Grant, error) {
		grant, e := authorize(ctx, encoded.RequestMetadata{Method: storageipc.AuthorizeMethod, Authority: storageipc.Authority, SourceID: namespace, TargetID: client})
		if e != nil || grant.UpstreamID != client {
			return transport.Grant{}, broker.ErrDenied
		}
		return transport.Grant{ExpiresUnixNano: grant.ExpiresUnixNano, MaxReadBytes: grant.MaxMessageBytes, MaxInflightBytes: grant.MaxInflightBytes}, nil
	}})
	if err != nil {
		return nil, err
	}
	bridge.metadata, err = control.ListenPrivateUnix("unix", p)
	if err != nil {
		return nil, err
	}
	bridge.offer, err = control.ListenPrivateUnix("unixpacket", q)
	if err != nil {
		return nil, err
	}
	if err = bridge.server.Start(bridge.metadata.UnixListener, bridge.offer.UnixListener); err != nil {
		return nil, err
	}
	return bridge, nil
}
func (b *storageBridge) close(ctx context.Context) error {
	if b == nil {
		return nil
	}
	var errs []error
	if b.server != nil {
		errs = append(errs, b.server.Close(ctx))
	}
	// Worker retains the native listener API. Its shutdown closes the transport;
	// these owners then retire only the inodes created by this bridge.
	for _, listener := range []*control.PrivateUnixListener{b.metadata, b.offer} {
		if listener != nil {
			errs = append(errs, listener.Close())
		}
	}
	if b.broker != nil {
		errs = append(errs, b.broker.Close(context.Background()))
	}
	return errors.Join(errs...)
}
