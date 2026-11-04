package ttrpc

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"syscall"

	"github.com/wignerStan/remote-storage-gateway/go/contracts/control/broker"
)

// Each worker has a distinct private broker socket. Its assigned identity is
// bound by this accepted transport, never selected from serialized metadata.
func ListenDecision(path, id string, service broker.DecisionBroker, metadataNames []string) (*Server, error) {
	info, e := os.Lstat(filepath.Dir(path))
	if e != nil || !info.IsDir() || info.Mode().Perm()&0077 != 0 {
		return nil, broker.ErrInvalid
	}
	if _, e = os.Lstat(path); !os.IsNotExist(e) {
		return nil, broker.ErrInvalid
	}
	listener, e := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if e != nil {
		return nil, e
	}
	if e = os.Chmod(path, 0600); e != nil {
		listener.Close()
		return nil, e
	}
	server, e := NewDecisionServer(service, DecisionOptions{Principal: func(context.Context) (string, error) { return id, nil }, MetadataNames: metadataNames})
	if e != nil {
		listener.Close()
		return nil, e
	}
	go func() { _ = server.Serve(&peerListener{listener: listener, uid: uint32(os.Geteuid())}) }()
	return server, nil
}

type peerListener struct {
	listener *net.UnixListener
	uid      uint32
}

func (l *peerListener) Addr() net.Addr { return l.listener.Addr() }
func (l *peerListener) Close() error   { return l.listener.Close() }
func (l *peerListener) Accept() (net.Conn, error) {
	for {
		conn, e := l.listener.AcceptUnix()
		if e != nil {
			return nil, e
		}
		raw, e := conn.SyscallConn()
		var credential *syscall.Ucred
		if e == nil {
			e = raw.Control(func(fd uintptr) {
				credential, e = syscall.GetsockoptUcred(int(fd), syscall.SOL_SOCKET, syscall.SO_PEERCRED)
			})
		}
		if e == nil && credential != nil && credential.Uid == l.uid {
			return conn, nil
		}
		conn.Close()
	}
}

// ValidateVacantSocket admits an operator-owned control path without unlinking
// pre-existing entries. Listener creation remains with the process serving it.
func ValidateVacantSocket(path string) error {
	if !filepath.IsAbs(path) || len(path) > 107 {
		return broker.ErrInvalid
	}
	info, e := os.Lstat(filepath.Dir(path))
	if e != nil || !info.IsDir() || info.Mode().Perm()&0077 != 0 {
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
