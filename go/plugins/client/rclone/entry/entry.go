// Package rclone publishes the concrete rclone plugin's configuration schemas
// and immutable descriptor before composing its runtime factory.
package rclone

import (
	"encoding/json"

	"github.com/wignerStan/infra/go/config/schema"
	port "github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/client"
	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/descriptor"
	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/descriptor/admission"
	"github.com/wignerStan/remote-storage-gateway/go/plugins/client/rclone"
)

func configurationSchema[T any]() (json.RawMessage, error) {
	document, err := schema.Derive[T]()
	if err != nil {
		return nil, err
	}
	return json.Marshal(document)
}

// NewFactory derives only this plugin's concrete implementation models and
// publishes the descriptor consumed by the runtime adapter.
func NewFactory(buildDigest string) (*rcloneclient.Factory, error) {
	s3, err := configurationSchema[rcloneclient.S3Configuration]()
	if err != nil {
		return nil, err
	}
	dav, err := configurationSchema[rcloneclient.WebDAVConfiguration]()
	if err != nil {
		return nil, err
	}
	streamingDAV, err := configurationSchema[rcloneclient.WebDAVConfiguration]()
	if err != nil {
		return nil, err
	}
	drive, err := configurationSchema[rcloneclient.DriveConfiguration]()
	if err != nil {
		return nil, err
	}
	archiveS3, err := configurationSchema[rcloneclient.S3ArchiveConfiguration]()
	if err != nil {
		return nil, err
	}
	archiveDAV, err := configurationSchema[rcloneclient.WebDAVArchiveConfiguration]()
	if err != nil {
		return nil, err
	}
	archiveDrive, err := configurationSchema[rcloneclient.DriveArchiveConfiguration]()
	if err != nil {
		return nil, err
	}
	readOnlyCapabilities := []string{"stat", "list", "open", "range", "list-pagination"}
	capabilities := []string{"stat", "list", "open", "put", "delete", "range", "unknown-size-put", "list-pagination"}
	davCapabilities := []string{"stat", "list", "open", "put", "delete", "range", "list-pagination"}
	published := descriptor.Descriptor{Schema: descriptor.Schema, PluginID: rcloneclient.PluginID, PluginVersion: "0.3.0-alpha.1", Contract: port.Contract, Runtime: "go", BuildDigest: buildDigest, Implementations: []descriptor.Implementation{
		{ID: "s3", DisplayName: "rclone S3", ConfigSchema: s3, Capabilities: capabilities},
		{ID: "webdav", DisplayName: "rclone WebDAV", ConfigSchema: dav, Capabilities: davCapabilities},
		{ID: "webdav-streaming", DisplayName: "rclone-compatible WebDAV (chunked uploads)", ConfigSchema: streamingDAV, Capabilities: capabilities},
		{ID: "drive", DisplayName: "rclone Google Drive", ConfigSchema: drive, Capabilities: capabilities},
		{ID: "archive-s3", DisplayName: "Read-only ZIP on S3", ConfigSchema: archiveS3, Capabilities: readOnlyCapabilities},
		{ID: "archive-webdav", DisplayName: "Read-only ZIP on WebDAV", ConfigSchema: archiveDAV, Capabilities: readOnlyCapabilities},
		{ID: "archive-drive", DisplayName: "Read-only ZIP on Google Drive", ConfigSchema: archiveDrive, Capabilities: readOnlyCapabilities},
	}}
	if err := admission.Validate(published); err != nil {
		return nil, err
	}
	return rcloneclient.NewFactory(published), nil
}
