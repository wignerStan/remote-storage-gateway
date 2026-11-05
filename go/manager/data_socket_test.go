package manager

import (
	"context"
	storageipc "github.com/wignerStan/network-data-plane-worker/go/contracts/storageipc"
	dp "github.com/wignerStan/network-data-plane-worker/go/contracts/worker"
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

func TestMaterializerActivationIsBoundToItsAssignedNamespace(t *testing.T) {
	const dir = "/run/gateway-namespace-test"
	good := Config{
		Workers: []WorkerSpec{{Executable: "/nonexistent-gateway-namespace-worker", Config: workerconfig.ProcessConfig{
			InstanceID: "worker", ControlSocket: dir + "/control",
			RuntimeConfig: workerconfig.RuntimeConfig{
				StorageIPCSocket: dir + "/data", StorageOfferSocket: dir + "/offer",
				Mounts: []workerconfig.Mount{
					{Name: "alpha", Client: "cached-alpha", RemoteClient: "raw-alpha", ReadOnly: true},
					{Name: "beta", Client: "cached-beta", RemoteClient: "raw-beta", ReadOnly: true},
					{Name: "logs", Client: "logs"},
				},
			},
		}}},
		Materializers: []MaterializerSpec{{
			Executable:     "/nonexistent-gateway-namespace-rclone",
			ActivateClient: &dp.Selection{Name: "cached-alpha"},
			Config: rclonebox.ProcessConfig{InstanceID: "rclone", ControlSocket: dir + "/rclone",
				Remote: storageipc.Endpoint{ExpectedWorkerID: "worker", Namespace: "alpha"}},
		}},
	}
	for _, test := range []struct {
		name, target string
		valid        bool
	}{
		{"assigned-cache", "cached-alpha", true},
		{"other-namespace-cache", "cached-beta", false},
		{"ordinary-mount", "logs", false},
		{"raw-origin-recursion", "raw-alpha", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			candidate, err := copyConfig(good)
			if err != nil {
				t.Fatal(err)
			}
			candidate.Materializers[0].ActivateClient.Name = test.target
			manager, err := New(candidate)
			if manager != nil {
				defer manager.Close(context.Background())
			}
			if test.valid && err != nil || !test.valid && err != broker.ErrInvalid {
				t.Fatalf("namespace activation admission: valid=%t error=%v", test.valid, err)
			}
		})
	}
}
