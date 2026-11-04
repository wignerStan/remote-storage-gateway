package manager

import (
	"context"
	storageipc "github.com/wignerStan/network-data-plane-worker/go/contracts/storageipc"
	rclonebox "github.com/wignerStan/rclone/go/box"
	"github.com/wignerStan/remote-storage-gateway/go/contracts/control/broker"
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
	good := Config{Workers: []WorkerSpec{{Executable: "/bin/true", Config: workerconfig.ProcessConfig{RuntimeConfig: workerconfig.RuntimeConfig{StorageIPCSocket: data, StorageOfferSocket: filepath.Join(dir, "offer"), Mounts: []workerconfig.Mount{{Name: "files", Client: "cached", RemoteClient: "origin", ReadOnly: true}}}, InstanceID: "worker", ControlSocket: meta}}}, Materializers: []MaterializerSpec{{Executable: "/bin/true", Config: rclonebox.ProcessConfig{InstanceID: "rclone", ControlSocket: filepath.Join(dir, "rclone"), Remote: storageipc.Endpoint{ExpectedWorkerID: "worker", Socket: data, OfferSocket: filepath.Join(dir, "offer"), Namespace: "files"}}}}}
	manager, err := New(good)
	if err != nil {
		t.Fatalf("valid raw IPC assignment rejected: %v", err)
	}
	if err = manager.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, which := range []string{"foreign-socket", "metadata-collision"} {
		t.Run(which, func(t *testing.T) {
			selected := data
			expected := filepath.Join(dir, "other")
			if which == "metadata-collision" {
				selected = meta
				expected = meta
			}
			candidate, e := copyConfig(good)
			if e != nil {
				t.Fatal(e)
			}
			candidate.Workers[0].Config.StorageIPCSocket = selected
			candidate.Materializers[0].Config.Remote.Socket = expected
			_, e = New(candidate)
			if e != broker.ErrInvalid {
				t.Fatalf("unapproved local data assignment admitted: %v", e)
			}
		})
	}
}
