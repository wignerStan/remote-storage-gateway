package httpapi

import (
	"context"
	"strings"

	"github.com/wignerStan/infra/go/code/schema/api"
	"github.com/wignerStan/network-data-plane-worker/go/management"
)

func (e *Entry) Manifest(context.Context, *ManifestRequest) (*ManifestResponse, error) {
	// A direct typed caller cannot mutate the next discovery response or
	// change the entry's admitted capability ceiling through a response.
	manifest := e.manifest
	manifest.Controls = append([]string{}, manifest.Controls...)
	manifest.Lanes = append([]string{}, manifest.Lanes...)
	manifest.PluginRuntimes = append([]string{}, manifest.PluginRuntimes...)
	manifest.PluginDescriptors = append(manifest.PluginDescriptors[:0:0], manifest.PluginDescriptors...)
	return &ManifestResponse{Body: manifest}, nil
}

func (e *Entry) Observed(context.Context, *ObservedRequest) (*ObservedResponse, error) {
	return &ObservedResponse{Body: e.manager.Observed()}, nil
}

func (e *Entry) Descriptor(_ context.Context, input *DescriptorRequest) (*api.RawJSON, error) {
	b, exists := e.records[input.Digest]
	if !exists {
		return nil, api.Error(404, "Descriptor unavailable")
	}
	return &api.RawJSON{ContentType: "application/json", ETag: `"` + input.Digest + `"`, Body: append([]byte(nil), b...)}, nil
}

func (e *Entry) Replace(ctx context.Context, input *ReplaceRequest) (*ReplaceResponse, error) {
	if !e.names[input.Name] {
		return nil, api.Error(404, "Client unavailable")
	}
	if input.Body.Selection.Name != input.Name {
		return nil, api.Error(400, "Route and client identity disagree")
	}
	version, err := e.manager.Replace(ctx, input.Body.Expected, input.Body.Selection)
	if err != nil {
		return nil, api.Error(409, "Desired state rejected")
	}
	return &ReplaceResponse{Body: version}, nil
}

// Guard runs before body decoding so an unregistered client cannot enter the
// desired-state decoder, and encoded paths or query operands fail shut.
func (e *Entry) Guard(_ context.Context, request api.Request) error {
	if request.RawQuery != "" || request.RawPath != "" && request.EscapedPath != request.Path {
		return api.Error(400, "Invalid management route")
	}
	if request.Method == "POST" && strings.HasPrefix(request.Path, management.ClientPrefix) && !e.names[strings.TrimPrefix(request.Path, management.ClientPrefix)] {
		return api.Error(404, "Client unavailable")
	}
	return nil
}
