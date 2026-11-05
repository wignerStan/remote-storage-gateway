package ttrpc

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"sync"
	"syscall"

	"github.com/wignerStan/remote-storage-gateway/go/contracts/control/broker"
)

// Each worker has a distinct private broker socket. Its assigned identity is
// bound by this accepted transport, never selected from serialized metadata.
func ListenDecision(path, id string, service broker.DecisionBroker, metadataNames []string) (*Server, error) {
	if id == "" || len(id) > 256 {
		return nil, broker.ErrInvalid
	}
	listener, e := ListenPrivateUnix("unix", path)
	if e != nil {
		return nil, e
	}
	server, e := NewDecisionServer(service, DecisionOptions{Principal: func(context.Context) (string, error) { return id, nil }, MetadataNames: metadataNames})
	if e != nil {
		listener.Close()
		return nil, e
	}
	go func() { _ = server.Serve(listener) }()
	return server, nil
}

// PrivateUnixListener retains the created socket identity independently of the
// native listener. A downstream owner may close UnixListener directly; Close
// still retires only this launch's pathname and preserves any replacement.
type PrivateUnixListener struct {
	*net.UnixListener
	path     string
	identity os.FileInfo
	once     sync.Once
	err      error
}

// ListenPrivateUnix is shared by the product's stream metadata and seqpacket
// offer listeners. It does not change the native listener API or wire protocol.
func ListenPrivateUnix(network, path string) (*PrivateUnixListener, error) {
	if network != "unix" && network != "unixpacket" || ValidateVacantSocket(path) != nil {
		return nil, broker.ErrInvalid
	}
	listener, err := net.ListenUnix(network, &net.UnixAddr{Name: path, Net: network})
	if err != nil {
		return nil, err
	}
	// net.UnixListener otherwise unlinks by pathname even when another owner
	// has replaced the original inode before shutdown.
	listener.SetUnlinkOnClose(false)
	identity, err := os.Lstat(path)
	if err != nil || identity.Mode()&os.ModeSocket == 0 {
		_ = listener.Close()
		return nil, broker.ErrInvalid
	}
	owned := &PrivateUnixListener{UnixListener: listener, path: path, identity: identity}
	if err = os.Chmod(path, 0600); err != nil {
		_ = owned.Close()
		return nil, err
	}
	return owned, nil
}
func (l *PrivateUnixListener) Close() error {
	l.once.Do(func() {
		err := l.UnixListener.Close()
		if errors.Is(err, net.ErrClosed) {
			err = nil
		}
		l.err = errors.Join(err, retireSocketPath(l.path, l.identity))
	})
	return l.err
}

func retireSocketPath(path string, owned os.FileInfo) error {
	current, err := os.Lstat(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if owned == nil || !os.SameFile(owned, current) {
		return broker.ErrInvalid
	}
	return os.Remove(path)
}

// ValidateVacantSocket admits an operator-owned control path without unlinking
// pre-existing entries. Listener creation remains with the process serving it.
func ValidateVacantSocket(path string) error {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || len(path) > 107 {
		return broker.ErrInvalid
	}
	info, e := os.Lstat(filepath.Dir(path))
	if e != nil || !info.IsDir() || info.Mode().Perm()&0077 != 0 {
		return broker.ErrInvalid
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != uint32(os.Geteuid()) {
		return broker.ErrInvalid
	}
	if _, e = os.Lstat(path); !os.IsNotExist(e) {
		return broker.ErrInvalid
	}
	return nil
}

// PreparePrivateDirectory creates one selected child of an existing runtime
// volume. Existing paths must already belong to this identity and be private.
func PreparePrivateDirectory(path string) error {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return broker.ErrInvalid
	}
	if e := os.Mkdir(path, 0700); e != nil && !os.IsExist(e) {
		return broker.ErrInvalid
	}
	info, e := os.Lstat(path)
	if e != nil || !info.IsDir() || info.Mode().Perm()&0077 != 0 {
		return broker.ErrInvalid
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != uint32(os.Geteuid()) {
		return broker.ErrInvalid
	}
	return nil
}

// Both ends authenticate the Unix peer before handing a socket to native ttrpc.
type peerHandshake struct{}

func (peerHandshake) Handshake(ctx context.Context, conn net.Conn) (net.Conn, any, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	if err := checkPeer(conn); err != nil {
		return nil, nil, err
	}
	return conn, nil, nil
}
func checkPeer(conn net.Conn) error {
	if single, ok := conn.(*singleConn); ok {
		conn = single.Conn
	}
	unix, ok := conn.(*net.UnixConn)
	if !ok {
		return broker.ErrDenied
	}
	raw, err := unix.SyscallConn()
	if err != nil {
		return broker.ErrDenied
	}
	var inner error
	var credential *syscall.Ucred
	err = raw.Control(func(fd uintptr) {
		credential, inner = syscall.GetsockoptUcred(int(fd), syscall.SOL_SOCKET, syscall.SO_PEERCRED)
	})
	if err != nil || inner != nil || credential == nil || credential.Pid <= 0 || credential.Uid != uint32(os.Geteuid()) {
		return broker.ErrDenied
	}
	return nil
}
