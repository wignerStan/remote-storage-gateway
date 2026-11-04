package ttrpc

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/wignerStan/remote-storage-gateway/go/contracts/control/broker"
)

func privateListenerPath(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "gateway-owner-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return filepath.Join(dir, "endpoint")
}

func TestPrivateSocketRetirementPreservesReplacements(t *testing.T) {
	for _, replacement := range []string{"owned", "missing", "file", "symlink"} {
		t.Run(replacement, func(t *testing.T) {
			path := privateListenerPath(t)
			// The identity/retirement primitive works on filesystem inodes. A
			// regular file lets this test run without AF_UNIX creation rights.
			if err := os.WriteFile(path, []byte("owned"), 0600); err != nil {
				t.Fatal(err)
			}
			owned, err := os.Lstat(path)
			if err != nil {
				t.Fatal(err)
			}
			if replacement != "owned" {
				// Keep the inode alive so immediate reuse cannot hide replacement.
				if err := os.Rename(path, path+".old"); err != nil {
					t.Fatal(err)
				}
			}
			switch replacement {
			case "file":
				if err := os.WriteFile(path, []byte("foreign"), 0600); err != nil {
					t.Fatal(err)
				}
			case "symlink":
				if err := os.Symlink(path+".old", path); err != nil {
					t.Fatal(err)
				}
			}
			before, _ := os.Lstat(path)
			err = retireSocketPath(path, owned)
			if replacement == "owned" || replacement == "missing" {
				if err != nil {
					t.Fatal(err)
				}
				if _, err := os.Lstat(path); !os.IsNotExist(err) {
					t.Fatalf("owned endpoint remains: %v", err)
				}
				return
			}
			if !errors.Is(err, broker.ErrInvalid) {
				t.Fatalf("replacement was not reported: %v", err)
			}
			after, err := os.Lstat(path)
			if err != nil || !os.SameFile(before, after) {
				t.Fatalf("replacement was removed or changed: %v", err)
			}
		})
	}
}

func TestPrivateSocketAdmissionLeavesExistingPath(t *testing.T) {
	path := privateListenerPath(t)
	if err := os.WriteFile(path, []byte("foreign"), 0600); err != nil {
		t.Fatal(err)
	}
	before, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	if listener, err := ListenPrivateUnix("unix", path); listener != nil || !errors.Is(err, broker.ErrInvalid) {
		t.Fatalf("existing path admitted: %v", err)
	}
	after, err := os.Lstat(path)
	if err != nil || !os.SameFile(before, after) {
		t.Fatalf("failed admission changed existing path: %v", err)
	}
}

func TestNativePrivateUnixListenerRetiresOnlyItsOwnPath(t *testing.T) {
	for _, network := range []string{"unix", "unixpacket"} {
		t.Run(network, func(t *testing.T) {
			path := privateListenerPath(t)
			listener, err := ListenPrivateUnix(network, path)
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			// Worker.Start closes this concrete native listener first. Default
			// unlink must be disabled and the outer owner's Close still cleans it.
			if err := listener.UnixListener.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := os.Lstat(path); err != nil {
				t.Fatalf("native close unlinked pathname: %v", err)
			}
			if err := listener.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := os.Lstat(path); !os.IsNotExist(err) {
				t.Fatalf("owned path leaked: %v", err)
			}
			if err := os.WriteFile(path, []byte("later owner"), 0600); err != nil {
				t.Fatal(err)
			}
			if err := listener.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := os.Lstat(path); err != nil {
				t.Fatalf("repeat close removed new owner: %v", err)
			}
		})
		t.Run(network+" replacement", func(t *testing.T) {
			path := privateListenerPath(t)
			listener, err := ListenPrivateUnix(network, path)
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			if err := os.Rename(path, path+".old"); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, []byte("foreign"), 0600); err != nil {
				t.Fatal(err)
			}
			before, _ := os.Lstat(path)
			for range 2 {
				if err := listener.Close(); !errors.Is(err, broker.ErrInvalid) {
					t.Fatalf("replacement not retained: %v", err)
				}
			}
			after, err := os.Lstat(path)
			if err != nil || !os.SameFile(before, after) {
				t.Fatalf("foreign replacement removed: %v", err)
			}
		})
	}
}
