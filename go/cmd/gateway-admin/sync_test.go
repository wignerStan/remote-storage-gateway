package main

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	process "github.com/wignerStan/infra/go/process/exec"
)

type fakeRunner struct {
	cert, key []byte
	calls     []string
	fail      string
	payload   []byte
}

func (f *fakeRunner) run(_ context.Context, s process.RunSpec) (process.RunOutput, error) {
	phase := ""
	if s.Program == "tailscale" {
		phase = "cert"
		if len(s.Args) != 6 || s.Args[0] != "cert" || s.Args[1] != "--cert-file" || s.Args[3] != "--key-file" {
			return process.RunOutput{}, errors.New("invalid cert argv")
		}
		if err := os.WriteFile(s.Args[2], f.cert, 0644); err != nil {
			return process.RunOutput{}, err
		}
		if err := os.WriteFile(s.Args[4], f.key, 0600); err != nil {
			return process.RunOutput{}, err
		}
	} else if s.Program == "incus" {
		joined := strings.Join(s.Args, " ")
		switch {
		case strings.Contains(joined, " apply "):
			phase = "apply"
			f.payload = append([]byte(nil), s.Stdin...)
		case strings.Contains(joined, " rollout restart "):
			phase = "restart"
		case strings.Contains(joined, " rollout status "):
			phase = "status"
		default:
			return process.RunOutput{}, errors.New("unexpected kubectl operation")
		}
		if strings.Contains(joined, "sensitive") || s.Timeout <= 0 || s.MaxStdout > 4096 || s.MaxStderr > 4096 {
			return process.RunOutput{}, errors.New("unbounded process descriptor or argv secret")
		}
	} else {
		return process.RunOutput{}, errors.New("unexpected executable")
	}
	f.calls = append(f.calls, phase)
	if phase == f.fail {
		return process.RunOutput{ExitCode: 1, Stderr: []byte("sensitive-provider-secret")}, errors.New("sensitive-process-diagnostic")
	}
	return process.RunOutput{ExitCode: 0}, nil
}

func fixture(t *testing.T) (options, *fakeRunner) {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "trade-storage-gateway-admin-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	o := options{config: filepath.Join(dir, "gateway.json"), secretsDir: filepath.Join(dir, "secrets"), stateDir: filepath.Join(dir, "state"), domain: "incus.example.ts.net", instance: "k3s-cilium", namespace: "selfhost", secret: "storage-gateway", deployment: "storage-gateway"}
	if err = os.Mkdir(o.secretsDir, 0700); err != nil {
		t.Fatal(err)
	}
	config := []byte(`{"listen":":8443","username":"operator","auth_secret":"auth","tls":{"certificate_file":"/run/gateway-secrets/tls.crt","private_key_secret":"tls-key"},"control_plane":{"host":"incus.example.ts.net","upstream":"http://openlist.selfhost.svc.cluster.local:5244"},"worker":{"schema":"network-data-plane-worker/bootstrap/v1","artifacts":[],"clients":[],"secret_files":{"auth":"/run/gateway-secrets/ingress-password","tls-key":"/run/gateway-secrets/tls-key"}},"mounts":[]}`)
	if err = os.WriteFile(o.config, config, 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(o.secretsDir, "ingress-password"), []byte("sensitive-auth-value"), 0600); err != nil {
		t.Fatal(err)
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: o.domain}, DNSNames: []string{o.domain}, NotBefore: now.Add(-time.Hour), NotAfter: now.Add(24 * time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	private, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return o, &fakeRunner{cert: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), key: pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: private})}
}

func TestPublishAndUnchangedPayloadSkipsApplyAndRollout(t *testing.T) {
	o, f := fixture(t)
	changed, err := synchronize(context.Background(), o, f.run)
	if err != nil || !changed {
		t.Fatal("initial sync failed", err)
	}
	if strings.Join(f.calls, ",") != "cert,apply,restart,status" {
		t.Fatal(f.calls)
	}
	var secret kubeSecret
	if json.Unmarshal(f.payload, &secret) != nil || secret.Metadata.Name != o.secret || secret.Metadata.Namespace != o.namespace || !bytes.Equal(secret.Data["tls-key"], f.key) || string(secret.Data["ingress-password"]) != "sensitive-auth-value" {
		t.Fatal("Secret payload omitted or changed allowlisted values")
	}
	st, err := os.Stat(filepath.Join(o.stateDir, "last-success.json"))
	if err != nil || st.Mode().Perm() != 0600 {
		t.Fatal("successful marker not private", err)
	}
	changed, err = synchronize(context.Background(), o, f.run)
	if err != nil || changed {
		t.Fatal("unchanged payload republished", err)
	}
	if strings.Join(f.calls, ",") != "cert,apply,restart,status,cert" {
		t.Fatal(f.calls)
	}
}

func TestFailedPublicationDoesNotAdvanceMarkerAndRetries(t *testing.T) {
	for _, phase := range []string{"apply", "restart", "status"} {
		t.Run(phase, func(t *testing.T) {
			o, f := fixture(t)
			f.fail = phase
			if _, err := synchronize(context.Background(), o, f.run); err == nil || strings.Contains(err.Error(), "sensitive") {
				t.Fatal("failure missing or private diagnostics leaked", err)
			}
			if _, err := os.Stat(filepath.Join(o.stateDir, "last-success.json")); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("failed rollout created success marker", err)
			}
			f.fail = ""
			if changed, err := synchronize(context.Background(), o, f.run); err != nil || !changed {
				t.Fatal("retry failed", err)
			}
		})
	}
}

func TestFailedUpdatePreservesPreviousSuccessfulFingerprint(t *testing.T) {
	o, f := fixture(t)
	if _, err := synchronize(context.Background(), o, f.run); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(o.stateDir, "last-success.json")
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(o.secretsDir, "ingress-password"), []byte("changed-sensitive-value"), 0600); err != nil {
		t.Fatal(err)
	}
	f.fail = "status"
	if _, err = synchronize(context.Background(), o, f.run); err == nil {
		t.Fatal("failed rollout accepted")
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("failed rollout changed committed fingerprint", err)
	}
	f.fail = ""
	if changed, err := synchronize(context.Background(), o, f.run); err != nil || !changed {
		t.Fatal("changed payload not retried", err)
	}
}

func TestRejectsUnsafeSecretPathsModesAndDomain(t *testing.T) {
	t.Run("domain", func(t *testing.T) {
		o, f := fixture(t)
		o.domain = "wrong.example.ts.net"
		if _, err := synchronize(context.Background(), o, f.run); err == nil || len(f.calls) != 0 {
			t.Fatal("domain mismatch admitted")
		}
	})
	t.Run("private", func(t *testing.T) {
		o, f := fixture(t)
		if err := os.Chmod(filepath.Join(o.secretsDir, "ingress-password"), 0644); err != nil {
			t.Fatal(err)
		}
		if _, err := synchronize(context.Background(), o, f.run); err == nil {
			t.Fatal("public secret accepted")
		}
		for _, call := range f.calls {
			if call == "apply" {
				t.Fatal("unsafe secret published")
			}
		}
	})
	t.Run("symlink", func(t *testing.T) {
		o, f := fixture(t)
		if err := os.Symlink("ingress-password", filepath.Join(o.secretsDir, "tls-key")); err != nil {
			t.Fatal(err)
		}
		if _, err := synchronize(context.Background(), o, f.run); err == nil || len(f.calls) != 0 {
			t.Fatal("symlink TLS output overwritten")
		}
	})
	t.Run("allowlist", func(t *testing.T) {
		o, f := fixture(t)
		b, err := os.ReadFile(o.config)
		if err != nil {
			t.Fatal(err)
		}
		b = bytes.ReplaceAll(b, []byte("/run/gateway-secrets/ingress-password"), []byte("/etc/shadow"))
		if err = os.WriteFile(o.config, b, 0600); err != nil {
			t.Fatal(err)
		}
		if _, err = synchronize(context.Background(), o, f.run); err == nil || len(f.calls) != 0 {
			t.Fatal("unmounted secret path admitted")
		}
	})
}
