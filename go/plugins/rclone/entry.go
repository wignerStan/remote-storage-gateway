// Package rclone publishes the concrete rclone plugin's configuration schemas
// and immutable descriptor before composing its runtime factory.
package rclone

import (
	"encoding/json"

	"github.com/wignerStan/infra/go/code/schema"
	port "github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/client"
	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/descriptor"
	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/descriptor/admission"
	"github.com/wignerStan/network-data-plane-worker/go/storage/rcloneclient"
)

func configurationSchema[T any](backend string) (json.RawMessage, error) {
	document, err := schema.Derive[T](schema.Identity{ID: rcloneclient.PluginID + "/configuration/" + backend, Version: "1", Owner: rcloneclient.PluginID})
	if err != nil {
		return nil, err
	}
	return json.Marshal(document.Schema)
}

// NewFactory derives only this plugin's concrete implementation models and
// publishes the descriptor consumed by the runtime adapter.
func NewFactory(buildDigest string) (*rcloneclient.Factory, error) {
	s3, err := configurationSchema[rcloneclient.S3Configuration]("s3")
	if err != nil {
		return nil, err
	}
	dav, err := configurationSchema[rcloneclient.WebDAVConfiguration]("webdav")
	if err != nil {
		return nil, err
	}
	streamingDAV, err := configurationSchema[rcloneclient.WebDAVConfiguration]("webdav-streaming")
	if err != nil {
		return nil, err
	}
	drive, err := configurationSchema[rcloneclient.DriveConfiguration]("drive")
	if err != nil {
		return nil, err
	}
	capabilities := []string{"stat", "list", "open", "put", "delete", "range", "unknown-size-put", "list-pagination"}
	davCapabilities := []string{"stat", "list", "open", "put", "delete", "range", "list-pagination"}
	published := descriptor.Descriptor{Schema: descriptor.Schema, PluginID: rcloneclient.PluginID, PluginVersion: "0.1.0-alpha.1", Contract: port.Contract, Runtime: "go", BuildDigest: buildDigest, Implementations: []descriptor.Implementation{
		{ID: "s3", DisplayName: "rclone S3", ConfigSchema: s3, Capabilities: capabilities},
		{ID: "webdav", DisplayName: "rclone WebDAV", ConfigSchema: dav, Capabilities: davCapabilities},
		{ID: "webdav-streaming", DisplayName: "rclone-compatible WebDAV (chunked uploads)", ConfigSchema: streamingDAV, Capabilities: capabilities},
		{ID: "drive", DisplayName: "rclone Google Drive", ConfigSchema: drive, Capabilities: capabilities},
	}}
	if err := admission.Validate(published); err != nil {
		return nil, err
	}
	return rcloneclient.NewFactory(published), nil
}
