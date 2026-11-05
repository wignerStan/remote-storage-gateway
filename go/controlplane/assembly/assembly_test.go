package assembly

import (
	"context"
	cfg "github.com/wignerStan/remote-storage-gateway/go/contracts/control"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func privateDirectory(t *testing.T) string {
	t.Helper()
	dir, e := os.MkdirTemp("/tmp", "trade-control-assembly-")
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}
func assignments(dir string) cfg.Config {
	return cfg.Config{RuntimeDirectory: dir, Workers: []cfg.WorkerEndpoint{{ID: "worker-a", ControlSocket: filepath.Join(dir, "worker-a"), BrokerSocket: filepath.Join(dir, "broker-a"), DataEndpoint: "https://worker-a.invalid"}, {ID: "worker-b", ControlSocket: filepath.Join(dir, "worker-b"), BrokerSocket: filepath.Join(dir, "broker-b"), DataEndpoint: "https://worker-b.invalid"}}}
}
func TestLocalBrokerDeathRetiresAllControlEntries(t *testing.T) {
	dir := privateDirectory(t)
	a, e := New(assignments(dir))
	if e != nil {
		t.Fatal(e)
	}
	defer a.Close(context.Background())
	if e = a.Start(context.Background()); e != nil {
		t.Fatal(e)
	}
	if !a.Ready() {
		t.Fatal("control entries not ready")
	}
	if e = a.brokers[0].Close(context.Background()); e != nil {
		t.Fatal(e)
	}
	select {
	case <-a.done:
	case <-time.After(time.Second):
		t.Fatal("local broker death left other metadata entries live")
	}
	if a.Ready() {
		t.Fatal("dead broker retained readiness")
	}
	for _, name := range []string{"broker-a", "broker-b"} {
		if _, e = os.Lstat(filepath.Join(dir, name)); !os.IsNotExist(e) {
			t.Fatal("private socket not retired", name, e)
		}
	}
}
func TestPartialStartupPreservesForeignEntryAndReapsOwnedSocket(t *testing.T) {
	dir := privateDirectory(t)
	c := assignments(dir)
	foreign := filepath.Join(dir, "broker-b")
	if e := os.WriteFile(foreign, []byte("foreign"), 0600); e != nil {
		t.Fatal(e)
	}
	a, e := New(c)
	if e != nil {
		t.Fatal(e)
	}
	if e = a.Start(context.Background()); e == nil {
		t.Fatal("existing foreign entry accepted")
	}
	if e = a.Close(context.Background()); e != nil {
		t.Fatal(e)
	}
	if b, e := os.ReadFile(foreign); e != nil || string(b) != "foreign" {
		t.Fatal("foreign entry modified", e)
	}
	if _, e = os.Lstat(filepath.Join(dir, "broker-a")); !os.IsNotExist(e) {
		t.Fatal("owned socket survived failed startup", e)
	}
}
