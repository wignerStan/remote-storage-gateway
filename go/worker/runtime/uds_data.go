//go:build dataplane_l7

package runtime

import (
	"net"
	"os"
	"path/filepath"

	"github.com/wignerStan/infra/go/network/proxy/l7"
	"github.com/wignerStan/remote-storage-gateway/go/contracts/control/broker"
	control "github.com/wignerStan/remote-storage-gateway/go/control/ttrpc"
	gatewayhttpapi "github.com/wignerStan/remote-storage-gateway/go/gateway/httpapi"
)

// prepareLocalData binds a separate private HTTP data socket. It never uses the
// ttrpc carrier and never changes the public listener's TLS identity.
func (r *Runtime) prepareLocalData() (*l7.Server, net.Listener, error) {
	path := r.config.LocalDataSocket
	if path == "" {
		return nil, nil, nil
	}
	if filepath.Clean(path) != path || path == filepath.Clean(r.process.ControlSocket) || path == filepath.Clean(r.process.BrokerSocket) {
		return nil, nil, broker.ErrInvalid
	}
	if err := control.ValidateVacantSocket(path); err != nil {
		return nil, nil, err
	}
	server, err := l7.NewServer(l7.ServerOptions{Handler: gatewayhttpapi.RemoteReadOnly(r.Runtime, r.handler)})
	if err != nil {
		return nil, nil, err
	}
	listener, err := net.Listen("unix", path)
	if err != nil {
		return nil, nil, err
	}
	if err = os.Chmod(path, 0600); err != nil {
		listener.Close()
		return nil, nil, err
	}
	return server, listener, nil
}
