package main

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"time"

	"github.com/wignerStan/infra/go/files/filelock"
	process "github.com/wignerStan/infra/go/process/exec"
	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/descriptor"
	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/descriptor/admission"
	"github.com/wignerStan/network-data-plane-worker/go/gateway"
)

type options struct{ config, secretsDir, stateDir, domain, instance, namespace, secret, deployment string }
type runner func(context.Context, process.RunSpec) (process.RunOutput, error)

func execute(ctx context.Context, s process.RunSpec) (process.RunOutput, error) {
	return process.RunContext(ctx, s)
}

var dnsLabel = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$`)
var secretKey = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,252}$`)

func validDNS(v string) bool {
	if len(v) == 0 || len(v) > 253 {
		return false
	}
	for _, label := range strings.Split(v, ".") {
		if !dnsLabel.MatchString(label) {
			return false
		}
	}
	return true
}
func (o options) validate() error {
	if !validDNS(o.domain) || !strings.Contains(o.domain, ".") || !dnsLabel.MatchString(o.instance) || !dnsLabel.MatchString(o.namespace) || !validDNS(o.secret) || !validDNS(o.deployment) {
		return errors.New("invalid operator domain or resource names")
	}
	for _, path := range []string{o.config, o.secretsDir, o.stateDir} {
		if !filepath.IsAbs(path) || filepath.Clean(path) != path || path == "/" {
			return errors.New("operator paths must be absolute and clean")
		}
	}
	return nil
}

// readFile verifies the opened descriptor, rejects a final symlink and avoids
// blocking on a substituted FIFO. Private inputs must have no group/world bits.
func readFile(path string, limit int64, private bool) ([]byte, error) {
	before, err := os.Lstat(path)
	if err != nil || !before.Mode().IsRegular() {
		return nil, errors.New("operator input must be a regular file")
	}
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, errors.New("operator input unavailable")
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil || !st.Mode().IsRegular() || !os.SameFile(before, st) || private && st.Mode().Perm()&0077 != 0 || st.Size() > limit {
		return nil, errors.New("operator input permissions, identity or size invalid")
	}
	b, err := io.ReadAll(io.LimitReader(f, limit+1))
	if err != nil || int64(len(b)) > limit {
		clear(b)
		return nil, errors.New("operator input size exceeded")
	}
	return b, nil
}
func privateDirectory(path string, create bool) error {
	if create {
		if err := os.MkdirAll(path, 0700); err != nil {
			return errors.New("private state directory unavailable")
		}
	}
	st, err := os.Lstat(path)
	if err != nil || !st.IsDir() || st.Mode().Perm()&0077 != 0 {
		return errors.New("operator directory must be private and not a symlink")
	}
	return nil
}
func invoke(ctx context.Context, run runner, program string, args []string, input []byte, timeout time.Duration) error {
	s := process.NewSpec(program, args...).WithTimeout(timeout).WithStdin(input)
	s.MaxStdout = 4096
	s.MaxStderr = 4096
	out, err := run(ctx, s)
	clear(out.Stdout)
	clear(out.Stderr)
	if err != nil || !out.ExitOK() || out.StdoutTruncated || out.StderrTruncated {
		return errors.New("operator subprocess failed or output exceeded bounds")
	}
	return nil
}

type kubeSecret struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	Metadata   struct {
		Name      string `json:"name"`
		Namespace string `json:"namespace"`
	} `json:"metadata"`
	Type string            `json:"type"`
	Data map[string][]byte `json:"data"`
}
type marker struct {
	Schema string `json:"schema"`
	SHA256 string `json:"sha256"`
}

const markerSchema = "storage-gateway/tls-sync/v1"

func synchronize(ctx context.Context, o options, run runner) (bool, error) {
	if err := o.validate(); err != nil {
		return false, err
	}
	if err := privateDirectory(filepath.Dir(o.config), false); err != nil {
		return false, err
	}
	if err := privateDirectory(o.secretsDir, false); err != nil {
		return false, err
	}
	if err := privateDirectory(o.stateDir, true); err != nil {
		return false, err
	}
	guard, err := filelock.Acquire(ctx, filepath.Join(o.stateDir, "sync.lock"), filelock.Exclusive)
	if err != nil {
		return false, errors.New("TLS publication lock unavailable")
	}
	defer guard.Close()
	config, err := readFile(o.config, descriptor.MaxBytes, true)
	if err != nil {
		return false, err
	}
	defer clear(config)
	var cfg gateway.Config
	if admission.StrictJSON(config, &cfg) != nil || cfg.TLS == nil || cfg.TLS.CertificateFile != "/run/gateway-secrets/tls.crt" || cfg.Worker.SecretFiles[cfg.TLS.PrivateKeySecret] != "/run/gateway-secrets/tls-key" || len(cfg.Worker.SecretFiles) > 256 {
		return false, errors.New("gateway TLS references invalid")
	}
	if cfg.ControlPlane != nil && cfg.ControlPlane.Host != o.domain {
		return false, errors.New("certificate domain disagrees with configured control-plane host")
	}
	// Validate the full allowlist before asking tailscaled to write either file.
	names := map[string]bool{}
	for _, path := range cfg.Worker.SecretFiles {
		name := filepath.Base(path)
		if filepath.Dir(path) != "/run/gateway-secrets" || filepath.Clean(path) != path || !secretKey.MatchString(name) || name == "gateway.json" || name == "tls.crt" {
			return false, errors.New("configured secret path is outside the mounted allowlist")
		}
		names[name] = true
	}
	certPath := filepath.Join(filepath.Dir(o.config), "tls.crt")
	keyPath := filepath.Join(o.secretsDir, "tls-key")
	for _, output := range []struct {
		path    string
		private bool
	}{{certPath, false}, {keyPath, true}} {
		if _, statErr := os.Lstat(output.path); statErr == nil {
			old, readErr := readFile(output.path, 64<<10, output.private)
			clear(old)
			if readErr != nil {
				return false, errors.New("existing TLS output is not a safe regular file")
			}
		} else if !errors.Is(statErr, os.ErrNotExist) {
			return false, errors.New("TLS output path unavailable")
		}
	}
	if err = invoke(ctx, run, "tailscale", []string{"cert", "--cert-file", certPath, "--key-file", keyPath, o.domain}, nil, 2*time.Minute); err != nil {
		return false, errors.New("Tailscale certificate renewal failed")
	}
	data := map[string][]byte{"gateway.json": config}
	defer func() {
		for _, value := range data {
			clear(value)
		}
	}()
	cert, err := readFile(certPath, 64<<10, false)
	if err != nil {
		return false, errors.New("TLS certificate unavailable")
	}
	data["tls.crt"] = cert
	for name := range names {
		value, readErr := readFile(filepath.Join(o.secretsDir, name), 64<<10, true)
		if readErr != nil {
			return false, errors.New("allowlisted secret unavailable or not private")
		}
		data[name] = value
		total := 0
		for _, b := range data {
			total += len(b)
		}
		if total > descriptor.MaxBytes {
			return false, errors.New("Secret payload exceeded bounds")
		}
	}
	pair, err := tls.X509KeyPair(cert, data["tls-key"])
	if err != nil || len(pair.Certificate) == 0 {
		return false, errors.New("invalid TLS certificate/key pair")
	}
	leaf, err := x509.ParseCertificate(pair.Certificate[0])
	if err != nil || leaf.VerifyHostname(o.domain) != nil || time.Now().Before(leaf.NotBefore) || !time.Now().Before(leaf.NotAfter) {
		return false, errors.New("TLS certificate domain or validity invalid")
	}
	s := kubeSecret{APIVersion: "v1", Kind: "Secret", Type: "Opaque", Data: data}
	s.Metadata.Name = o.secret
	s.Metadata.Namespace = o.namespace
	payload, err := json.Marshal(s)
	if err != nil || len(payload) > descriptor.MaxBytes {
		return false, errors.New("Kubernetes Secret payload exceeded bounds")
	}
	defer clear(payload)
	digest := sha256.Sum256(payload)
	fingerprint := hex.EncodeToString(digest[:])
	markerPath := filepath.Join(o.stateDir, "last-success.json")
	previous, readErr := readFile(markerPath, 1024, true)
	if readErr == nil {
		var last marker
		if admission.StrictJSON(previous, &last) != nil || last.Schema != markerSchema {
			return false, errors.New("invalid successful-publication marker")
		}
		if last.SHA256 == fingerprint {
			return false, nil
		}
	} else if _, statErr := os.Lstat(markerPath); !errors.Is(statErr, os.ErrNotExist) {
		return false, errors.New("successful-publication marker unavailable")
	}
	base := []string{"exec", o.instance, "--", "kubectl", "--namespace", o.namespace}
	if err = invoke(ctx, run, "incus", append(append([]string(nil), base...), "apply", "-f", "-"), payload, 90*time.Second); err != nil {
		return false, errors.New("Kubernetes Secret apply failed")
	}
	if err = invoke(ctx, run, "incus", append(append([]string(nil), base...), "rollout", "restart", "deployment/"+o.deployment), nil, 90*time.Second); err != nil {
		return false, errors.New("gateway rollout restart failed")
	}
	if err = invoke(ctx, run, "incus", append(append([]string(nil), base...), "rollout", "status", "deployment/"+o.deployment, "--timeout=180s"), nil, 4*time.Minute); err != nil {
		return false, errors.New("gateway rollout did not complete")
	}
	if err = guard.CheckPath(); err != nil {
		return false, errors.New("TLS publication lock path changed")
	}
	if err = writeMarker(markerPath, marker{markerSchema, fingerprint}); err != nil {
		return false, errors.New("successful-publication marker could not be persisted")
	}
	return true, nil
}

func writeMarker(path string, value marker) error {
	b, err := json.Marshal(value)
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".last-success-")
	if err != nil {
		return err
	}
	temporary := f.Name()
	defer os.Remove(temporary)
	if _, err = f.Write(b); err != nil {
		f.Close()
		return err
	}
	if err = f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = os.Rename(temporary, path); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
