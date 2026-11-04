//go:build dataplane_l7

package runtime

import (
	"context"
	dp "github.com/wignerStan/network-data-plane-worker/go/contracts/worker"
	config "github.com/wignerStan/remote-storage-gateway/go/contracts/worker"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPreapprovedProviderSecretsRejectHostAliasesBeforeLoading(t *testing.T) {
	dir := t.TempDir()
	host := filepath.Join(dir, "host")
	if e := os.WriteFile(host, []byte("host-secret-not-for-provider"), 0600); e != nil {
		t.Fatal(e)
	}
	hard := filepath.Join(dir, "hard")
	if e := os.Link(host, hard); e != nil {
		t.Fatal(e)
	}
	sym := filepath.Join(dir, "symbolic")
	if e := os.Symlink(host, sym); e != nil {
		t.Fatal(e)
	}
	for _, test := range []struct{ name, path, ref string }{{"same-ref", host, "ingress"}, {"same-path", host, "alias"}, {"cleaned-path", dir + "/./host", "alias"}, {"hardlink", hard, "alias"}, {"symlink", sym, "alias"}} {
		t.Run(test.name, func(t *testing.T) {
			cfg := config.RuntimeConfig{Listen: "127.0.0.1:0", Username: "operator", AuthSecret: "ingress", ClientSecretRefs: []string{test.ref}, Worker: dp.Bootstrap{Schema: dp.BootstrapSchema, Artifacts: []dp.Artifact{{}}, Clients: []dp.Selection{{Name: "files"}}, SecretFiles: map[string]string{"ingress": host, test.ref: test.path}}, Mounts: []config.Mount{{Name: "files", Client: "files"}}}
			if r, e := assemble(context.Background(), cfg); e == nil {
				r.Close(context.Background())
				t.Fatal("host secret grant admitted")
			} else if !strings.Contains(e.Error(), "overlaps ingress") {
				t.Fatalf("guard did not reject before native artifact load: %v", e)
			}
		})
	}
}
