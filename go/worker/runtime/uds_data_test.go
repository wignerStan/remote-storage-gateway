//go:build dataplane_l7

package runtime

import (
	config "github.com/wignerStan/remote-storage-gateway/go/contracts/worker"
	"os"
	"path/filepath"
	"testing"
)

func TestLocalDataListenerPreservesExistingPath(t *testing.T) {
	dir, e := os.MkdirTemp("/tmp", "trade-local-data-")
	if e != nil {
		t.Fatal(e)
	}
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "raw")
	if e = os.WriteFile(path, []byte("existing"), 0600); e != nil {
		t.Fatal(e)
	}
	r := &Runtime{config: config.RuntimeConfig{LocalDataSocket: path}, process: config.ProcessConfig{ControlSocket: filepath.Join(dir, "meta")}}
	if _, _, e = r.prepareLocalData(); e == nil {
		t.Fatal("existing private data path adopted")
	}
	b, e := os.ReadFile(path)
	if e != nil || string(b) != "existing" {
		t.Fatal("foreign existing path removed")
	}
}
