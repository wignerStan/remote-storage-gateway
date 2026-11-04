package manager

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	cpconfig "github.com/wignerStan/remote-storage-gateway/go/contracts/control"
	"github.com/wignerStan/remote-storage-gateway/go/contracts/control/broker"
	workerconfig "github.com/wignerStan/remote-storage-gateway/go/contracts/worker"
)

func assignedBundle() Config {
	const dir = "/run/gateway-assignment-test"
	return Config{
		RuntimeDirectory: dir,
		Workers: []WorkerSpec{{Executable: "/nonexistent-gateway-assignment-worker", Config: workerconfig.ProcessConfig{
			InstanceID: "worker-a", ControlSocket: dir + "/worker", BrokerSocket: dir + "/broker", DataEndpoint: "https://storage.example",
		}}},
		ControlPlanes: []ControlPlaneSpec{{
			ID: "cp-a", DataDirectory: "/nonexistent-gateway-assignment-data", ConfigFile: "/nonexistent-gateway-assignment-data/config.json", ReadyURL: "http://127.0.0.1:5244/ping",
			Control: cpconfig.Config{Workers: []cpconfig.WorkerEndpoint{{ID: "worker-a", ControlSocket: dir + "/worker", BrokerSocket: dir + "/broker", DataEndpoint: "https://storage.example"}}},
		}},
	}
}

func TestBundleRejectsInconsistentControlAssignmentsBeforeStartup(t *testing.T) {
	for name, change := range map[string]func(*Config){
		"foreign-control-socket": func(c *Config) { c.ControlPlanes[0].Control.Workers[0].ControlSocket += "-other" },
		"foreign-broker-socket":  func(c *Config) { c.ControlPlanes[0].Control.Workers[0].BrokerSocket += "-other" },
		"foreign-data-endpoint":  func(c *Config) { c.ControlPlanes[0].Control.Workers[0].DataEndpoint = "https://other.example" },
		"unassigned-worker":      func(c *Config) { c.ControlPlanes[0].Control.Workers = nil },
		"missing-local-worker":   func(c *Config) { c.ControlPlanes[0].Control.Workers[0].ID = "worker-b" },
		"duplicate-assignment": func(c *Config) {
			plane := c.ControlPlanes[0]
			plane.ID, plane.DataDirectory, plane.ConfigFile, plane.ReadyURL = "cp-b", "/other-data", "/other-data/config.json", "http://127.0.0.1:5245/ping"
			c.ControlPlanes = append(c.ControlPlanes, plane)
		},
		"aliased-ready-port": func(c *Config) {
			plane := c.ControlPlanes[0]
			plane.ID, plane.DataDirectory, plane.ConfigFile, plane.ReadyURL = "cp-b", "/other-data", "/other-data/config.json", "http://127.0.0.1:05244/ping"
			plane.Control.Workers = nil
			c.ControlPlanes = append(c.ControlPlanes, plane)
		},
		"shared-sqlite-directory": func(c *Config) {
			plane := c.ControlPlanes[0]
			plane.ID, plane.ReadyURL = "cp-b", "http://127.0.0.1:5245/ping"
			plane.Control.Workers = nil
			c.ControlPlanes = append(c.ControlPlanes, plane)
		},
		"unadmitted-transfer-policy": func(c *Config) { c.ControlPlanes[0].Control.Rules = []cpconfig.Rule{{InstanceID: "unassigned"}} },
	} {
		t.Run(name, func(t *testing.T) {
			c := assignedBundle()
			change(&c)
			m, err := New(c)
			if m != nil {
				_ = m.Close(context.Background())
			}
			if !errors.Is(err, broker.ErrInvalid) {
				t.Fatalf("inconsistent bundle admitted: %v", err)
			}
		})
	}
}

func TestCheckConfigDoesNotStartChildrenOrCreateRuntimeDirectories(t *testing.T) {
	c := assignedBundle()
	dir := t.TempDir()
	c.RuntimeDirectory = filepath.Join(dir, "runtime")
	c.Workers[0].Config.ControlSocket = filepath.Join(c.RuntimeDirectory, "worker")
	c.Workers[0].Config.BrokerSocket = filepath.Join(c.RuntimeDirectory, "broker")
	c.ControlPlanes[0].Control.Workers[0].ControlSocket = c.Workers[0].Config.ControlSocket
	c.ControlPlanes[0].Control.Workers[0].BrokerSocket = c.Workers[0].Config.BrokerSocket
	c.ControlPlanes[0].DataDirectory = filepath.Join(dir, "data")
	c.ControlPlanes[0].ConfigFile = filepath.Join(dir, "data", "config.json")
	b, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "bundle.json")
	if err = os.WriteFile(path, b, 0600); err != nil {
		t.Fatal(err)
	}
	if err = Run(context.Background(), []string{"--config", path, "--check-config"}); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{c.RuntimeDirectory, c.ControlPlanes[0].DataDirectory} {
		if _, err = os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("validation changed runtime state at %s: %v", path, err)
		}
	}
}
