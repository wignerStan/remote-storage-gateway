// Package httpapi owns the Worker management HTTP boundary.
package httpapi

import (
	"encoding/json"
	"errors"
	"github.com/danielgtaylor/huma/v2"
	"github.com/wignerStan/infra/go/code/schema"
	infrahttpapi "github.com/wignerStan/infra/go/network/httpapi"
	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/descriptor"
	workercontract "github.com/wignerStan/network-data-plane-worker/go/contracts/worker"
	"github.com/wignerStan/network-data-plane-worker/go/management"
	"github.com/wignerStan/network-data-plane-worker/go/pluginruntime"
	"net/http"
)

type Options struct {
	Registry    *pluginruntime.Registry
	Manager     *pluginruntime.Manager
	Manifest    workercontract.Manifest
	ClientNames []string
}
type Handler struct {
	records  map[string][]byte
	manager  *pluginruntime.Manager
	manifest workercontract.Manifest
	names    map[string]bool
	api      huma.API
	router   http.Handler
}

// NewHandler freezes approval material and registers typed operations. The
// product composition must authenticate and authorize this entire boundary.
func NewHandler(o Options) (*Handler, error) {
	if o.Registry == nil || o.Manager == nil || o.Manifest.Schema != workercontract.CapabilitySchema || !descriptor.ValidDigest(o.Manifest.BuildDigest) || len(o.ClientNames) > 64 {
		return nil, errors.New("invalid management registration")
	}
	h := &Handler{records: map[string][]byte{}, manager: o.Manager, names: map[string]bool{}}
	identities := map[string]descriptor.Descriptor{}
	for _, record := range o.Registry.Records() {
		h.records[record.Digest] = record.Bytes
		identities[record.Digest] = record.Descriptor
	}
	if len(o.Manifest.PluginDescriptors) != len(h.records) {
		return nil, errors.New("manifest disagrees with approved descriptors")
	}
	seen := map[string]bool{}
	for _, p := range o.Manifest.PluginDescriptors {
		_, exists := h.records[p.DescriptorDigest]
		if !exists || seen[p.DescriptorDigest] {
			return nil, errors.New("descriptor unavailable")
		}
		seen[p.DescriptorDigest] = true
		identity := identities[p.DescriptorDigest]
		if p.PluginID != identity.PluginID || p.Contract != identity.Contract || p.PluginVersion != identity.PluginVersion {
			return nil, errors.New("descriptor identity disagrees")
		}
	}
	for _, name := range o.ClientNames {
		if !management.ValidName(name) || h.names[name] {
			return nil, errors.New("invalid registered client name")
		}
		h.names[name] = true
	}
	h.manifest = o.Manifest
	h.manifest.Controls = append([]string{}, o.Manifest.Controls...)
	h.manifest.Lanes = append([]string{}, o.Manifest.Lanes...)
	h.manifest.PluginRuntimes = append([]string{}, o.Manifest.PluginRuntimes...)
	h.manifest.PluginDescriptors = append([]workercontract.PluginCapability{}, o.Manifest.PluginDescriptors...)
	hasContract := false
	for _, control := range h.manifest.Controls {
		hasContract = hasContract || control == management.Contract
	}
	if !hasContract {
		h.manifest.Controls = append(h.manifest.Controls, management.Contract)
	}
	b, err := json.Marshal(h.manifest)
	if err != nil || len(b) > descriptor.MaxBytes {
		return nil, errors.New("oversized manifest")
	}
	mux := http.NewServeMux()
	config := schema.OpenAPIConfig("Network data-plane Worker management", "1")
	config.OpenAPIPath = "/v1/openapi"
	config.SchemasPath = "/v1/schemas"
	config.DocsPath = ""
	api, err := infrahttpapi.NewAPI(infrahttpapi.APIConfig{Mux: mux, Config: config, MaxBodyBytes: descriptor.MaxBytes})
	if err != nil {
		return nil, err
	}
	h.api = api
	h.router = mux
	h.register()
	return h, nil
}
