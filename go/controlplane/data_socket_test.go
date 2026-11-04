package controlplane

import (
	"github.com/wignerStan/remote-storage-gateway/go/contracts/control/broker"
	vfsconfig "github.com/wignerStan/remote-storage-gateway/go/contracts/vfs"
	workerconfig "github.com/wignerStan/remote-storage-gateway/go/contracts/worker"
	"os"
	"path/filepath"
	"testing"
)

func TestLocalDataAssignmentsRejectForeignSocketAndCollision(t *testing.T) {
	dir, e := os.MkdirTemp("/tmp", "trade-data-assign-")
	if e != nil {
		t.Fatal(e)
	}
	defer os.RemoveAll(dir)
	meta := filepath.Join(dir, "meta")
	data := filepath.Join(dir, "raw")
	for _, which := range []string{"foreign-socket", "metadata-collision"} {
		t.Run(which, func(t *testing.T) {
			selected := data
			expected := filepath.Join(dir, "other")
			if which == "metadata-collision" {
				selected = meta
				expected = meta
			}
			_, e := New(Config{Workers: []WorkerSpec{{Executable: "/bin/true", Config: workerconfig.ProcessConfig{RuntimeConfig: workerconfig.RuntimeConfig{LocalDataSocket: selected}, InstanceID: "worker", ControlSocket: meta}}}, VFS: []VFSSpec{{Executable: "/bin/true", Config: vfsconfig.ProcessConfig{InstanceID: "vfs", ControlSocket: filepath.Join(dir, "vfs"), WorkerInstanceID: "worker", WorkerControlSocket: meta, WorkerLocalDataSocket: expected}}}})
			if e != broker.ErrInvalid {
				t.Fatalf("unapproved local data assignment admitted: %v", e)
			}
		})
	}
}
