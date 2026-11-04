package manager

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/wignerStan/infra/go/process/supervisor"
	"github.com/wignerStan/remote-storage-gateway/go/contracts/control/broker"
)

func TestSocketRetirementPreservesReplacements(t *testing.T) {
	for _, replacement := range []string{"none", "socket", "file"} {
		t.Run(replacement, func(t *testing.T) {
			dir, err := os.MkdirTemp("/tmp", "trade-manager-socket-")
			if err != nil {
				t.Fatal(err)
			}
			defer os.RemoveAll(dir)
			path := filepath.Join(dir, "worker")
			listen := func() *net.UnixListener {
				l, e := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
				if e != nil {
					t.Fatal(e)
				}
				l.SetUnlinkOnClose(false)
				return l
			}
			m, err := New(Config{})
			if err != nil {
				t.Fatal(err)
			}
			first := listen()
			if err = m.trackSockets([]string{path}); err != nil {
				t.Fatal(err)
			}
			if err = first.Close(); err != nil {
				t.Fatal(err)
			}
			if replacement != "none" {
				// Preserve the original inode so replacement identity cannot be
				// obscured by immediate filesystem inode reuse.
				if err = os.Rename(path, path+".retired"); err != nil {
					t.Fatal(err)
				}
				if replacement == "socket" {
					next := listen()
					defer next.Close()
				} else if err = os.WriteFile(path, []byte("foreign"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			err = m.Close(context.Background())
			if replacement == "none" {
				if err != nil {
					t.Fatal(err)
				}
				if _, e := os.Lstat(path); !os.IsNotExist(e) {
					t.Fatalf("owned crashed endpoint remains: %v", e)
				}
			} else {
				if !errors.Is(err, broker.ErrInvalid) {
					t.Fatalf("replacement not reported: %v", err)
				}
				if _, e := os.Lstat(path); e != nil {
					t.Fatalf("foreign replacement removed: %v", e)
				}
			}
		})
	}
}

func TestFailedReadinessStillTracksCreatedSocket(t *testing.T) {
	dir, err := os.MkdirTemp("/tmp", "trade-manager-readiness-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "worker")
	m, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	failed := errors.New("child readiness failed")
	spec := supervisor.Spec{Ready: func(context.Context, *supervisor.Child) error {
		l, e := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
		if e != nil {
			t.Fatal(e)
		}
		l.SetUnlinkOnClose(false)
		if e = l.Close(); e != nil {
			t.Fatal(e)
		}
		return failed
	}}
	m.watchSockets(&spec, []string{path})
	if err = spec.Ready(context.Background(), nil); !errors.Is(err, failed) {
		t.Fatal(err)
	}
	if err = m.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Lstat(path); !os.IsNotExist(err) {
		t.Fatalf("failed startup leaked socket: %v", err)
	}
}
