// Package worker exposes passive artifact discovery and bootstrap validation.
// Concrete ingress protocols live in product compositions, not in this package.
package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/descriptor"
	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/descriptor/admission"
	workercontract "github.com/wignerStan/network-data-plane-worker/go/contracts/worker"
	"github.com/wignerStan/network-data-plane-worker/go/nativeclient"
	"github.com/wignerStan/network-data-plane-worker/go/pluginruntime"
	"github.com/wignerStan/network-data-plane-worker/go/secrets"
	"io"
	"os"
	"path/filepath"
	"sort"
)

const BootstrapSchema = workercontract.BootstrapSchema
const CapabilitySchema = workercontract.CapabilitySchema

type Bootstrap = workercontract.Bootstrap

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
	if e = admission.StrictJSON(b, &out); e != nil {
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
			if e := admission.ValidateConfig(a, s.Public, s.SecretRefs); e != nil {
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

type PluginCapability = workercontract.PluginCapability
type Manifest = workercontract.Manifest

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
			m.PluginDescriptors = append(m.PluginDescriptors, PluginCapability{PluginID: v.Descriptor.PluginID, Contract: v.Descriptor.Contract, PluginVersion: v.Descriptor.PluginVersion, DescriptorDigest: v.Digest})
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

type FileSecrets = secrets.FileResolver

func WriteJSON(w io.Writer, v any) error {
	e := json.NewEncoder(w)
	e.SetIndent("", "  ")
	return e.Encode(v)
}
