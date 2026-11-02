// Package management owns the bounded Worker control HTTP contract and SDK.
// It carries descriptors, observations and desired state, never object bodies.
// Product compositions authenticate and authorize access before invoking Handler.
package management

import (
	"errors"
	"regexp"
	"strings"

	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/descriptor"
	workercontract "github.com/wignerStan/network-data-plane-worker/go/contracts/worker"
)

const Contract = "network-data-plane-worker/management/v1"
const ManifestPath = "/v1/manifest"
const DescriptorPrefix = "/v1/descriptors/"
const ObservedPath = "/v1/state"
const ClientPrefix = "/v1/clients/"
const OpenAPIPath = "/v1/openapi.json"

// Route describes a Worker-owned registration. Provider IDs and configuration
// fields belong to exact plugin descriptors, not to the management router.
type Route struct {
	ID       string `json:"id"`
	Method   string `json:"method"`
	Path     string `json:"path"`
	Contract string `json:"contract"`
}

// IsPath reserves the versioned control namespace, including unknown routes so
// they fail closed rather than falling through into another upstream.
func IsPath(path string) bool { return path == "/v1" || strings.HasPrefix(path, "/v1/") }

var namePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$`)

func ValidName(name string) bool { return namePattern.MatchString(name) }

type ReplaceRequest struct {
	Expected  workercontract.Version   `json:"expected"`
	Selection workercontract.Selection `json:"selection"`
}

func (r ReplaceRequest) validate(name string) error {
	if !ValidName(name) || r.Selection.Name != name || !ValidName(r.Selection.PluginID) || !ValidName(r.Selection.ImplementationID) || !descriptor.ValidDigest(r.Selection.DescriptorDigest) {
		return errors.New("invalid client selection")
	}
	v := r.Expected
	if v.Incarnation == "" || len(v.Incarnation) > 128 || (v.DescriptorDigest != "" && !descriptor.ValidDigest(v.DescriptorDigest)) || (v.ArtifactDigest != "" && !descriptor.ValidDigest(v.ArtifactDigest)) {
		return errors.New("invalid expected generation")
	}
	return nil
}

// StatusError excludes remote response bodies and provider diagnostics, which
// may contain signed URLs or secrets. Callers may classify the HTTP status.
type StatusError struct{ StatusCode int }

func (e *StatusError) Error() string { return "worker management request rejected" }

var ErrUnavailable = errors.New("worker management unavailable")
var ErrInvalid = errors.New("invalid worker management response")
