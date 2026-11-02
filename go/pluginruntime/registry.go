// Package pluginruntime binds approved descriptors to typed client generations.
package pluginruntime

import (
	"errors"
	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/client"
	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/descriptor"
	"reflect"
	"sort"
	"sync"
)

type registration struct {
	factory        client.Factory
	record         descriptor.Record
	artifactDigest string
}
type Registry struct {
	mu      sync.RWMutex
	plugins map[string]registration
}

func NewRegistry() *Registry { return &Registry{plugins: map[string]registration{}} }
func (r *Registry) Register(f client.Factory) error {
	if f == nil {
		return errors.New("nil factory")
	}
	var record descriptor.Record
	var e error
	if exact, ok := f.(interface{ DescriptorRecord() descriptor.Record }); ok {
		supplied := exact.DescriptorRecord()
		record, e = descriptor.Decode(supplied.Bytes)
		if e != nil || record.Digest != supplied.Digest || !reflect.DeepEqual(record.Descriptor, f.Descriptor()) {
			return errors.New("descriptor bytes disagree")
		}
	} else {
		record, e = descriptor.Encode(f.Descriptor())
		if e != nil {
			return e
		}
	}
	if record.Descriptor.Contract != client.Contract {
		return errors.New("client factory contract mismatch")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.plugins == nil {
		r.plugins = map[string]registration{}
	}
	key := record.Descriptor.PluginID + "\x00" + record.Digest
	if _, ok := r.plugins[key]; ok {
		return errors.New("duplicate plugin identity")
	}
	artifactDigest := ""
	if artifact, ok := f.(interface{ ArtifactDigest() string }); ok {
		artifactDigest = artifact.ArtifactDigest()
		if !descriptor.ValidDigest(artifactDigest) {
			return errors.New("invalid artifact digest")
		}
	}
	if len(r.plugins) >= 64 {
		return errors.New("approved artifact inventory exceeded")
	}
	r.plugins[key] = registration{factory: f, record: record, artifactDigest: artifactDigest}
	return nil
}
func (r *Registry) Records() []descriptor.Record {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := []descriptor.Record{}
	for _, p := range r.plugins {
		v, _ := descriptor.Decode(p.record.Bytes)
		out = append(out, v)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Descriptor.PluginID == out[j].Descriptor.PluginID {
			return out[i].Digest < out[j].Digest
		}
		return out[i].Descriptor.PluginID < out[j].Descriptor.PluginID
	})
	return out
}
func (r *Registry) resolve(s Selection) (registration, descriptor.Implementation, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	v, ok := r.plugins[s.PluginID+"\x00"+s.DescriptorDigest]
	if !ok || v.record.Digest != s.DescriptorDigest {
		return registration{}, descriptor.Implementation{}, errors.New("unapproved descriptor")
	}
	v.record, _ = descriptor.Decode(v.record.Bytes)
	a, ok := v.record.Descriptor.Implementation(s.ImplementationID)
	if !ok {
		return registration{}, a, errors.New("implementation unavailable")
	}
	return v, a, nil
}
