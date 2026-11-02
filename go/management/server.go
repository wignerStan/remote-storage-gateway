package management

import (
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"strings"

	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/descriptor"
	"github.com/wignerStan/network-data-plane-worker/go/pluginruntime"
	"github.com/wignerStan/network-data-plane-worker/go/worker"
)

type Options struct {
	Registry    *pluginruntime.Registry
	Manager     *pluginruntime.Manager
	Manifest    worker.Manifest
	ClientNames []string
}

type Handler struct {
	records  map[string][]byte
	manager  *pluginruntime.Manager
	manifest []byte
	names    map[string]bool
}

// NewHandler freezes the operator's client-name admission set and passive
// manifest. The caller MUST authenticate and authorize all management routes.
// No handler in this package resolves credentials or proxies object payloads.
func NewHandler(o Options) (*Handler, error) {
	if o.Registry == nil || o.Manager == nil || o.Manifest.Schema != worker.CapabilitySchema || !descriptor.ValidDigest(o.Manifest.BuildDigest) || len(o.ClientNames) > 64 {
		return nil, errors.New("invalid management registration")
	}
	h := &Handler{records: map[string][]byte{}, manager: o.Manager, names: map[string]bool{}}
	for _, record := range o.Registry.Records() {
		h.records[record.Digest] = record.Bytes
	}
	if len(o.Manifest.PluginDescriptors) != len(h.records) {
		return nil, errors.New("management manifest disagrees with approved descriptors")
	}
	seen := map[string]bool{}
	for _, p := range o.Manifest.PluginDescriptors {
		b, exists := h.records[p.DescriptorDigest]
		if !exists || seen[p.DescriptorDigest] {
			return nil, errors.New("management descriptor unavailable")
		}
		seen[p.DescriptorDigest] = true
		r, err := descriptor.Decode(b)
		if err != nil || p.PluginID != r.Descriptor.PluginID || p.Contract != r.Descriptor.Contract || p.PluginVersion != r.Descriptor.PluginVersion {
			return nil, errors.New("management descriptor identity disagrees")
		}
	}
	for _, name := range o.ClientNames {
		if !ValidName(name) || h.names[name] {
			return nil, errors.New("invalid management client name")
		}
		h.names[name] = true
	}
	o.Manifest.Controls = append([]string(nil), o.Manifest.Controls...)
	hasContract := false
	for _, control := range o.Manifest.Controls {
		hasContract = hasContract || control == Contract
	}
	if !hasContract {
		o.Manifest.Controls = append(o.Manifest.Controls, Contract)
	}
	b, err := json.Marshal(o.Manifest)
	if err != nil || len(b) > descriptor.MaxBytes {
		return nil, errors.New("oversized management manifest")
	}
	h.manifest = b
	return h, nil
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	if r.URL.RawQuery != "" || r.URL.RawPath != "" && r.URL.EscapedPath() != r.URL.Path {
		http.Error(w, "Invalid management route", http.StatusBadRequest)
		return
	}
	path := r.URL.Path
	switch {
	case path == ManifestPath:
		if !method(w, r, http.MethodGet) {
			return
		}
		writeFrame(w, h.manifest)
	case path == ObservedPath:
		if !method(w, r, http.MethodGet) {
			return
		}
		writeJSON(w, h.manager.Observed())
	case strings.HasPrefix(path, DescriptorPrefix):
		if !method(w, r, http.MethodGet) {
			return
		}
		digest := strings.TrimPrefix(path, DescriptorPrefix)
		if !descriptor.ValidDigest(digest) {
			http.NotFound(w, r)
			return
		}
		if b, ok := h.records[digest]; ok {
			w.Header().Set("ETag", `"`+digest+`"`)
			writeFrame(w, b)
			return
		}
		http.NotFound(w, r)
	case strings.HasPrefix(path, ClientPrefix):
		if !method(w, r, http.MethodPost) {
			return
		}
		name := strings.TrimPrefix(path, ClientPrefix)
		if !h.names[name] {
			http.NotFound(w, r)
			return
		}
		media, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if err != nil || media != "application/json" {
			http.Error(w, "JSON required", http.StatusUnsupportedMediaType)
			return
		}
		var update ReplaceRequest
		b, err := io.ReadAll(io.LimitReader(r.Body, descriptor.MaxBytes+1))
		if err != nil || descriptor.StrictJSON(b, &update) != nil || update.validate(name) != nil {
			http.Error(w, "Invalid desired state", http.StatusBadRequest)
			return
		}
		version, err := h.manager.Replace(r.Context(), update.Expected, update.Selection)
		if err != nil {
			http.Error(w, "Desired state rejected", http.StatusConflict)
			return
		}
		writeJSON(w, version)
	default:
		http.NotFound(w, r)
	}
}

func method(w http.ResponseWriter, r *http.Request, allowed string) bool {
	if r.Method == allowed {
		return true
	}
	w.Header().Set("Allow", allowed)
	http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	return false
}
func writeJSON(w http.ResponseWriter, v any) {
	b, err := json.Marshal(v)
	if err != nil || len(b) > descriptor.MaxBytes {
		http.Error(w, "Management response unavailable", http.StatusServiceUnavailable)
		return
	}
	writeFrame(w, b)
}
func writeFrame(w http.ResponseWriter, b []byte) {
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(b)
}
