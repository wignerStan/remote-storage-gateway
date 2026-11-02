package secrets_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/client"
	"github.com/wignerStan/network-data-plane-worker/go/secrets"
)

func TestFileResolverRetainsPermissionBoundsAndZeroesClosedLease(t *testing.T) {
	dir := t.TempDir()
	private := filepath.Join(dir, "private")
	if err := os.WriteFile(private, []byte("operator-secret"), 0600); err != nil {
		t.Fatal(err)
	}
	resolver := secrets.FileResolver{"key": private}
	lease, err := resolver.Resolve(context.Background(), "key")
	if err != nil {
		t.Fatal(err)
	}
	borrowed := lease.Bytes()
	if string(borrowed) != "operator-secret" {
		t.Fatal("resolved secret changed")
	}
	lease.Close()
	if lease.Bytes() != nil || !bytes.Equal(borrowed, make([]byte, len(borrowed))) {
		t.Fatal("closed secret retained readable bytes")
	}
	lease.Close()
	if _, err := resolver.Resolve(context.Background(), "missing"); !errors.Is(err, client.ErrNotFound) {
		t.Fatalf("missing reference: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := resolver.Resolve(ctx, "key"); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled reference: %v", err)
	}
	public := filepath.Join(dir, "public")
	if err := os.WriteFile(public, []byte("not-private"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(public, 0644); err != nil {
		t.Fatal(err)
	}
	large := filepath.Join(dir, "oversized")
	if err := os.WriteFile(large, make([]byte, 65537), 0600); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{public, large, dir} {
		if lease, err := (secrets.FileResolver{"key": path}).Resolve(context.Background(), "key"); !errors.Is(err, client.ErrInvalid) {
			if lease != nil {
				lease.Close()
			}
			t.Fatalf("accepted non-private, oversized or nonregular secret %q: %v", path, err)
		}
	}
}
