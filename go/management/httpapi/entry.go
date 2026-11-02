package httpapi

import (
	"context"
	"github.com/danielgtaylor/huma/v2"
	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/descriptor"
	workercontract "github.com/wignerStan/network-data-plane-worker/go/contracts/worker"
	"github.com/wignerStan/network-data-plane-worker/go/management"
	"github.com/wignerStan/network-data-plane-worker/go/pluginruntime"
	"net/http"
	"reflect"
	"sort"
	"strings"
)

type manifestOutput struct{ Body workercontract.Manifest }
type observedOutput struct{ Body []pluginruntime.Observed }
type descriptorInput struct {
	Digest string `path:"digest" pattern:"^sha256:[a-f0-9]{64}$"`
}
type descriptorOutput struct {
	ContentType string `header:"Content-Type"`
	ETag        string `header:"ETag"`
	Body        []byte
}
type replaceInput struct {
	Name string `path:"name" pattern:"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$"`
	Body management.ReplaceRequest
}
type replaceOutput struct{ Body pluginruntime.Version }

// DescriptorSchema describes the Worker-owned public response type.
func DescriptorSchema(registry huma.Registry) *huma.Schema {
	return registry.Schema(reflect.TypeFor[descriptor.Descriptor](), true, "WorkerPluginDescriptor")
}

func (h *Handler) register() {
	operation := func(id, method, path string) huma.Operation {
		return huma.Operation{OperationID: id, Method: method, Path: path, MaxBodyBytes: descriptor.MaxBytes, RejectUnknownQueryParameters: true}
	}
	huma.Register(h.api, operation("worker.manifest", http.MethodGet, management.ManifestPath), func(context.Context, *struct{}) (*manifestOutput, error) {
		return &manifestOutput{Body: h.manifest}, nil
	})
	huma.Register(h.api, operation("worker.observed", http.MethodGet, management.ObservedPath), func(context.Context, *struct{}) (*observedOutput, error) {
		return &observedOutput{Body: h.manager.Observed()}, nil
	})
	op := operation("worker.descriptor", http.MethodGet, management.DescriptorPrefix+"{digest}")
	op.Responses = map[string]*huma.Response{"200": {Description: "Exact approved descriptor bytes", Content: map[string]*huma.MediaType{"application/json": {Schema: DescriptorSchema(h.api.OpenAPI().Components.Schemas)}}}}
	huma.Register(h.api, op, func(_ context.Context, input *descriptorInput) (*descriptorOutput, error) {
		b, exists := h.records[input.Digest]
		if !exists {
			return nil, huma.Error404NotFound("Descriptor unavailable")
		}
		return &descriptorOutput{ContentType: "application/json", ETag: `"` + input.Digest + `"`, Body: b}, nil
	})
	huma.Register(h.api, operation("worker.client.replace", http.MethodPost, management.ClientPrefix+"{name}"), func(ctx context.Context, input *replaceInput) (*replaceOutput, error) {
		if !h.names[input.Name] {
			return nil, huma.Error404NotFound("Client unavailable")
		}
		if input.Body.Selection.Name != input.Name {
			return nil, huma.Error400BadRequest("Route and client identity disagree")
		}
		version, err := h.manager.Replace(ctx, input.Body.Expected, input.Body.Selection)
		if err != nil {
			return nil, huma.Error409Conflict("Desired state rejected")
		}
		return &replaceOutput{Body: version}, nil
	})
}
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	if r.URL.RawQuery != "" || (r.URL.RawPath != "" && r.URL.EscapedPath() != r.URL.Path) {
		http.Error(w, "Invalid management route", http.StatusBadRequest)
		return
	}
	if r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, management.ClientPrefix) {
		if !h.names[strings.TrimPrefix(r.URL.Path, management.ClientPrefix)] {
			http.NotFound(w, r)
			return
		}
	}
	h.router.ServeHTTP(w, r)
}

// Routes projects the actual generated OpenAPI operation set.
func (h *Handler) Routes() []management.Route {
	out := []management.Route{}
	for path, item := range h.api.OpenAPI().Paths {
		for method, op := range map[string]*huma.Operation{"GET": item.Get, "POST": item.Post, "PUT": item.Put, "PATCH": item.Patch, "DELETE": item.Delete, "HEAD": item.Head, "OPTIONS": item.Options, "TRACE": item.Trace} {
			if op != nil {
				out = append(out, management.Route{ID: op.OperationID, Method: method, Path: path, Contract: management.Contract})
			}
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Path == out[j].Path {
			return out[i].Method < out[j].Method
		}
		return out[i].Path < out[j].Path
	})
	return out
}
