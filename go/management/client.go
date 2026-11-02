package management

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"mime"
	"net/http"
	"net/url"
	"time"

	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/descriptor"
	workercontract "github.com/wignerStan/network-data-plane-worker/go/contracts/worker"
	"github.com/wignerStan/network-data-plane-worker/go/pluginruntime"
)

type Client struct {
	base url.URL
	http *http.Client
}

// NewClient binds a preconfigured operator endpoint. Authentication belongs to
// the supplied HTTP transport; credentials must not be put in URLs. Redirects
// are refused, so a management origin cannot forward credentials elsewhere.
func NewClient(baseURL string, httpClient *http.Client) (*Client, error) {
	u, err := url.Parse(baseURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "" || u.Opaque != "" {
		return nil, ErrInvalid
	}
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	copyClient := *httpClient
	copyClient.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &Client{base: *u, http: &copyClient}, nil
}

func (c *Client) call(ctx context.Context, method, path string, input any) ([]byte, error) {
	var body io.Reader
	if input != nil {
		b, err := json.Marshal(input)
		if err != nil || len(b) > descriptor.MaxBytes {
			return nil, ErrInvalid
		}
		body = bytes.NewReader(b)
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	u := c.base
	u.Path = path
	u.RawPath = ""
	r, err := http.NewRequestWithContext(ctx, method, u.String(), body)
	if err != nil {
		return nil, ErrInvalid
	}
	r.Header.Set("Accept", "application/json")
	if input != nil {
		r.Header.Set("Content-Type", "application/json")
	}
	response, err := c.http.Do(r)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, ErrUnavailable
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, &StatusError{response.StatusCode}
	}
	media, _, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if err != nil || media != "application/json" {
		return nil, ErrInvalid
	}
	b, err := io.ReadAll(io.LimitReader(response.Body, descriptor.MaxBytes+1))
	if err != nil || len(b) == 0 || len(b) > descriptor.MaxBytes {
		return nil, ErrInvalid
	}
	return b, nil
}

func (c *Client) Manifest(ctx context.Context) (workercontract.Manifest, error) {
	var out workercontract.Manifest
	b, err := c.call(ctx, http.MethodGet, ManifestPath, nil)
	if err != nil {
		return out, err
	}
	if descriptor.StrictJSON(b, &out) != nil || out.Schema != workercontract.CapabilitySchema || !descriptor.ValidDigest(out.BuildDigest) || len(out.PluginDescriptors) > 64 {
		return workercontract.Manifest{}, ErrInvalid
	}
	supported := false
	for _, control := range out.Controls {
		supported = supported || control == Contract
	}
	if !supported {
		return workercontract.Manifest{}, ErrInvalid
	}
	for _, p := range out.PluginDescriptors {
		if !ValidName(p.PluginID) || !descriptor.ValidDigest(p.DescriptorDigest) {
			return workercontract.Manifest{}, ErrInvalid
		}
	}
	return out, nil
}

// Descriptor verifies and retains byte-exact approval material. No JSON
// normalization is permitted between hashing and control-plane storage.
func (c *Client) Descriptor(ctx context.Context, digest string) (descriptor.Record, error) {
	if !descriptor.ValidDigest(digest) {
		return descriptor.Record{}, ErrInvalid
	}
	b, err := c.call(ctx, http.MethodGet, DescriptorPrefix+digest, nil)
	if err != nil {
		return descriptor.Record{}, err
	}
	record, err := descriptor.Decode(b)
	if err != nil || record.Digest != digest {
		return descriptor.Record{}, ErrInvalid
	}
	return record, nil
}

func (c *Client) Observed(ctx context.Context) ([]pluginruntime.Observed, error) {
	b, err := c.call(ctx, http.MethodGet, ObservedPath, nil)
	if err != nil {
		return nil, err
	}
	var out []pluginruntime.Observed
	if descriptor.StrictJSON(b, &out) != nil || out == nil || len(out) > 4096 {
		return nil, ErrInvalid
	}
	seen := map[string]bool{}
	for _, state := range out {
		if !ValidName(state.Name) || seen[state.Name] || !ValidName(state.PluginID) || !ValidName(state.ImplementationID) || state.Version.Incarnation == "" || len(state.Version.Incarnation) > 128 || state.Version.Generation == 0 || !descriptor.ValidDigest(state.Version.DescriptorDigest) || (state.Version.ArtifactDigest != "" && !descriptor.ValidDigest(state.Version.ArtifactDigest)) || state.Draining < 0 {
			return nil, ErrInvalid
		}
		seen[state.Name] = true
	}
	return out, nil
}

// Replace sends one CAS attempt and never retries. A timeout is ambiguous:
// consult Observed before deciding whether to submit another desired revision.
func (c *Client) Replace(ctx context.Context, name string, expected pluginruntime.Version, selection pluginruntime.Selection) (pluginruntime.Version, error) {
	request := ReplaceRequest{expected, selection}
	if request.validate(name) != nil {
		return pluginruntime.Version{}, ErrInvalid
	}
	b, err := c.call(ctx, http.MethodPost, ClientPrefix+name, request)
	if err != nil {
		return pluginruntime.Version{}, err
	}
	var out pluginruntime.Version
	if descriptor.StrictJSON(b, &out) != nil || out.Incarnation != expected.Incarnation || out.Generation <= expected.Generation || out.DescriptorDigest != selection.DescriptorDigest || (out.ArtifactDigest != "" && !descriptor.ValidDigest(out.ArtifactDigest)) {
		return pluginruntime.Version{}, ErrInvalid
	}
	return out, nil
}
