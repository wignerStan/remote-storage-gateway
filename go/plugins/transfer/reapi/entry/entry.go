// Package entry publishes the REAPI plugin's owned configuration and descriptor.
package entry

import (
	"encoding/json"
	"github.com/wignerStan/infra/go/code/schema"
	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/descriptor"
	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/encoded"
	"github.com/wignerStan/remote-storage-gateway/go/plugins/transfer/reapi"
)

func NewFactory(buildDigest string) (*reapi.Factory, error) {
	doc, e := schema.Derive[reapi.Configuration](schema.Identity{ID: "transfer.reapi.configuration", Version: "1", Owner: reapi.PluginID})
	if e != nil {
		return nil, e
	}
	raw, e := json.Marshal(doc.Schema)
	if e != nil {
		return nil, e
	}
	return reapi.NewFactory(descriptor.Descriptor{Schema: descriptor.Schema, PluginID: reapi.PluginID, PluginVersion: "0.2.0-alpha.1", Contract: encoded.Contract, Runtime: "go", BuildDigest: buildDigest, Implementations: []descriptor.Implementation{{ID: "grpc", ConfigSchema: raw, Capabilities: []string{"encoded-grpc", "bidi-streaming", "half-close", "opaque-frame-leases", "early-response-headers"}}}})
}
