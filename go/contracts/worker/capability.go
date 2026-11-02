// Package worker defines passive Worker capability contracts. It does not load
// plugin artifacts, resolve secrets, or select runtime implementations.
package worker

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
