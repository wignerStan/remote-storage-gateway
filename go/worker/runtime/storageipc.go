//go:build dataplane_l7

package runtime

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"

	transport "github.com/wignerStan/network-data-plane-worker/go/clienttransport/ttrpc"
	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/encoded"
	"github.com/wignerStan/network-data-plane-worker/go/contracts/storageipc"
	"github.com/wignerStan/remote-storage-gateway/go/contracts/control/broker"
	control "github.com/wignerStan/remote-storage-gateway/go/control/ttrpc"
)

type storageBridge struct {
	server *transport.Server
	broker *control.DecisionClient
}

// prepareStorageIPC admits only operator-assigned raw mount clients. The control
// broker grants namespace metadata; the Worker creates and owns payload arenas.
func (r *Runtime) prepareStorageIPC(ctx context.Context) (_ *storageBridge, err error) {
	p, q := r.config.StorageIPCSocket, r.config.StorageOfferSocket
	if p == "" && q == "" {
		return nil, nil
	}
	if p == "" || q == "" || r.process.InstanceID == "" || r.process.BrokerSocket == "" || r.Manager == nil {
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
	names := map[string]string{}
	for _, mount := range r.config.Mounts {
		if mount.Name == "" || mount.RemoteClient == "" || names[mount.Name] != "" {
			return nil, broker.ErrInvalid
		}
		names[mount.Name] = mount.RemoteClient
	}
	bridge := &storageBridge{}
	defer func() {
		if err != nil {
			_ = bridge.close(context.Background())
		}
	}()
	bridge.broker, err = control.DialDecision(ctx, r.process.BrokerSocket, nil)
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
	meta, err := net.ListenUnix("unix", &net.UnixAddr{Name: p, Net: "unix"})
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			meta.Close()
		}
	}()
	offer, err := net.ListenUnix("unixpacket", &net.UnixAddr{Name: q, Net: "unixpacket"})
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			offer.Close()
		}
	}()
	if os.Chmod(p, 0600) != nil || os.Chmod(q, 0600) != nil {
		return nil, broker.ErrUnavailable
	}
	if err = bridge.server.Start(meta, offer); err != nil {
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
	if b.broker != nil {
		errs = append(errs, b.broker.Close(context.Background()))
	}
	return errors.Join(errs...)
}
