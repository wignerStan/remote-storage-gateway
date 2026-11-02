// Package worker defines passive Worker capability contracts. It does not load
// plugin artifacts, resolve secrets, or select runtime implementations.
package worker

import "encoding/json"

const CapabilitySchema = "network-data-plane-worker/capabilities/v1"

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

// Desired and observed state are passive public wire values. Their runtime
// implementation is not part of the SDK or schema declaration dependency.
type Selection struct {
	Name             string            `json:"name"`
	PluginID         string            `json:"plugin_id"`
	DescriptorDigest string            `json:"descriptor_digest"`
	ImplementationID string            `json:"implementation_id"`
	Public           json.RawMessage   `json:"public"`
	SecretRefs       map[string]string `json:"secret_refs,omitempty"`
}
type Version struct {
	Incarnation      string `json:"incarnation"`
	Generation       uint64 `json:"generation"`
	DescriptorDigest string `json:"descriptor_digest"`
	ArtifactDigest   string `json:"artifact_digest,omitempty"`
}
type Observed struct {
	Name             string  `json:"name"`
	Version          Version `json:"version"`
	PluginID         string  `json:"plugin_id"`
	ImplementationID string  `json:"implementation_id"`
	Accepting        bool    `json:"accepting"`
	Draining         int     `json:"draining"`
}
