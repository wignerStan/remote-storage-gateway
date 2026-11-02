// Package worker exposes passive artifact discovery and bootstrap validation.
// Concrete ingress protocols live in product compositions, not in this package.
package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/client"
	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/descriptor"
	"github.com/wignerStan/network-data-plane-worker/go/nativeclient"
	"github.com/wignerStan/network-data-plane-worker/go/pluginruntime"
	"io"
	"os"
	"path/filepath"
	"sort"
)

const BootstrapSchema = "network-data-plane-worker/bootstrap/v1"
const CapabilitySchema = "network-data-plane-worker/capabilities/v1"

type Bootstrap struct {
	Schema      string                    `json:"schema"`
	Artifacts   []nativeclient.Artifact   `json:"artifacts"`
	Clients     []pluginruntime.Selection `json:"clients"`
	SecretFiles map[string]string         `json:"secret_files,omitempty"`
}

func ReadBootstrap(path string) (Bootstrap, error) {
	var out Bootstrap
	f, e := os.Open(path)
	if e != nil {
		return out, e
	}
	defer f.Close()
	b, e := io.ReadAll(io.LimitReader(f, descriptor.MaxBytes+1))
	if e != nil {
		return out, e
	}
	if e = descriptor.StrictJSON(b, &out); e != nil {
		return out, e
	}
	if out.Schema != BootstrapSchema || len(out.Artifacts) > 32 || len(out.Clients) > 64 || len(out.SecretFiles) > 256 {
		return out, errors.New("unsupported or oversized bootstrap")
	}
	for ref, path := range out.SecretFiles {
		if ref == "" || len(ref) > 4096 || !filepath.IsAbs(path) {
			return out, errors.New("invalid operator secret reference")
		}
	}
	return out, nil
}
func Discover(ctx context.Context, c Bootstrap) (*pluginruntime.Registry, error) {
	r := pluginruntime.NewRegistry()
	for _, a := range c.Artifacts {
		f, e := nativeclient.NewFactory(ctx, a)
		if e != nil {
			return nil, e
		}
		if e = r.Register(f); e != nil {
			return nil, e
		}
	}
	return r, nil
}
func Validate(c Bootstrap, r *pluginruntime.Registry) error {
	seen := map[string]bool{}
	records := r.Records()
	for _, s := range c.Clients {
		if s.Name == "" || len(s.Name) > 128 || seen[s.Name] {
			return errors.New("invalid client assignment name")
		}
		seen[s.Name] = true
		found := false
		for _, record := range records {
			if record.Descriptor.PluginID != s.PluginID || record.Digest != s.DescriptorDigest {
				continue
			}
			a, ok := record.Descriptor.Implementation(s.ImplementationID)
			if !ok {
				return errors.New("unavailable implementation")
			}
			if e := descriptor.ValidateConfig(a, s.Public, s.SecretRefs); e != nil {
				return e
			}
			found = true
		}
		if !found {
			return errors.New("unapproved client assignment")
		}
		for _, ref := range s.SecretRefs {
			if c.SecretFiles[ref] == "" {
				return errors.New("unavailable operator secret reference")
			}
		}
	}
	return nil
}

type PluginCapability struct {
	PluginID         string `json:"plugin_id"`
	Contract         string `json:"contract"`
	PluginVersion    string `json:"plugin_version"`
	DescriptorDigest string `json:"descriptor_digest"`
}
type Manifest struct {
	Schema            string             `json:"schema"`
	Implementation    string             `json:"implementation"`
	Lanes             []string           `json:"lanes"`
	Controls          []string           `json:"controls"`
	PluginRuntimes    []string           `json:"plugin_runtimes"`
	PluginDescriptors []PluginCapability `json:"plugin_descriptors"`
	BuildDigest       string             `json:"build_digest"`
}

func Capabilities(r *pluginruntime.Registry) (Manifest, error) {
	lanes := []string{}
	if l4Enabled {
		lanes = append(lanes, "l4")
	}
	if l7Enabled {
		lanes = append(lanes, "l7")
	}
	m := Manifest{Schema: CapabilitySchema, Implementation: "go", Lanes: lanes, Controls: []string{}, PluginRuntimes: []string{"native"}, PluginDescriptors: []PluginCapability{}}
	if r != nil {
		for _, v := range r.Records() {
			m.PluginDescriptors = append(m.PluginDescriptors, PluginCapability{v.Descriptor.PluginID, v.Descriptor.Contract, v.Descriptor.PluginVersion, v.Digest})
		}
	}
	sort.Slice(m.PluginDescriptors, func(i, j int) bool { return m.PluginDescriptors[i].PluginID < m.PluginDescriptors[j].PluginID })
	exe, e := os.Executable()
	if e != nil {
		return m, e
	}
	f, e := os.Open(exe)
	if e != nil {
		return m, e
	}
	defer f.Close()
	h := sha256.New()
	if _, e = io.Copy(h, f); e != nil {
		return m, e
	}
	m.BuildDigest = "sha256:" + hex.EncodeToString(h.Sum(nil))
	return m, nil
}

// FileSecrets is an operator-local resolver; desired state contains only its
// declared references. Ordinary management reads never expose these bytes.
type FileSecrets map[string]string

func (s FileSecrets) Resolve(ctx context.Context, ref string) (client.Secret, error) {
	if e := ctx.Err(); e != nil {
		return nil, e
	}
	path, ok := s[ref]
	if !ok {
		return nil, client.ErrNotFound
	}
	f, e := os.Open(path)
	if e != nil {
		return nil, client.ErrInvalid
	}
	defer f.Close()
	info, e := f.Stat()
	if e != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
		return nil, client.ErrInvalid
	}
	b, e := io.ReadAll(io.LimitReader(f, 65537))
	if e != nil || len(b) > 65536 {
		clear(b)
		return nil, client.ErrInvalid
	}
	return &secret{b}, nil
}

type secret struct{ b []byte }

func (s *secret) Bytes() []byte { return s.b }
func (s *secret) Close()        { clear(s.b); s.b = nil }
func WriteJSON(w io.Writer, v any) error {
	e := json.NewEncoder(w)
	e.SetIndent("", "  ")
	return e.Encode(v)
}
