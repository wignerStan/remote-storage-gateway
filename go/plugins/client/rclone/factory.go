package rcloneclient

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"path"
	"strconv"
	"strings"
	"sync"

	"github.com/rclone/rclone/fs"
	rclonestore "github.com/wignerStan/infra/go/storage/blob/rclone"
	port "github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/client"
	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/descriptor"
	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/descriptor/admission"
)

const PluginID = "client.rclone"

type S3PublicConfig struct {
	Endpoint       string `json:"endpoint,omitempty"`
	Region         string `json:"region" minLength:"1"`
	Bucket         string `json:"bucket" minLength:"1"`
	Root           string `json:"root,omitempty"`
	ForcePathStyle bool   `json:"force_path_style,omitempty"`
}
type S3Configuration struct {
	S3PublicConfig
	AccessKeyID     string `json:"access_key_id" writeOnly:"true" minLength:"1"`
	SecretAccessKey string `json:"secret_access_key" writeOnly:"true" minLength:"1"`
	SessionToken    string `json:"session_token,omitempty" writeOnly:"true"`
}
type WebDAVPublicConfig struct {
	URL      string `json:"url" minLength:"1"`
	Root     string `json:"root,omitempty"`
	Username string `json:"username,omitempty"`
}
type WebDAVConfiguration struct {
	WebDAVPublicConfig
	Password string `json:"password,omitempty" writeOnly:"true"`
}
type DrivePublicConfig struct {
	RootFolderID string `json:"root_folder_id,omitempty"`
	Root         string `json:"root,omitempty"`
	ClientID     string `json:"client_id,omitempty"`
}
type DriveConfiguration struct {
	DrivePublicConfig
	Token        string `json:"token" writeOnly:"true" minLength:"1"`
	ClientSecret string `json:"client_secret,omitempty" writeOnly:"true"`
}

// Archive implementations bind one immutable ZIP to a read-only namespace.
// The underlying backend credentials retain the same secret declarations.
type S3ArchivePublicConfig struct {
	S3PublicConfig
	ArchivePath string `json:"archive_path" minLength:"1" maxLength:"4096"`
	SourceView  bool   `json:"source_view,omitempty"`
}
type S3ArchiveConfiguration struct {
	S3Configuration
	ArchivePath string `json:"archive_path" minLength:"1" maxLength:"4096"`
	SourceView  bool   `json:"source_view,omitempty"`
}
type WebDAVArchivePublicConfig struct {
	WebDAVPublicConfig
	ArchivePath string `json:"archive_path" minLength:"1" maxLength:"4096"`
	SourceView  bool   `json:"source_view,omitempty"`
}
type WebDAVArchiveConfiguration struct {
	WebDAVConfiguration
	ArchivePath string `json:"archive_path" minLength:"1" maxLength:"4096"`
	SourceView  bool   `json:"source_view,omitempty"`
}
type DriveArchivePublicConfig struct {
	DrivePublicConfig
	ArchivePath string `json:"archive_path" minLength:"1" maxLength:"4096"`
	SourceView  bool   `json:"source_view,omitempty"`
}
type DriveArchiveConfiguration struct {
	DriveConfiguration
	ArchivePath string `json:"archive_path" minLength:"1" maxLength:"4096"`
	SourceView  bool   `json:"source_view,omitempty"`
}

// Factory retains the descriptor published by the plugin entry. Configuration
// types and provider behavior stay here; schema publication stays with entry.
type Factory struct{ published descriptor.Descriptor }

func NewFactory(published descriptor.Descriptor) *Factory {
	return &Factory{published: cloneDescriptor(published)}
}

func (f *Factory) Descriptor() descriptor.Descriptor { return cloneDescriptor(f.published) }

func cloneDescriptor(value descriptor.Descriptor) descriptor.Descriptor {
	value.Implementations = append([]descriptor.Implementation(nil), value.Implementations...)
	for i := range value.Implementations {
		implementation := &value.Implementations[i]
		implementation.ConfigSchema = append(json.RawMessage(nil), implementation.ConfigSchema...)
		implementation.Capabilities = append([]string(nil), implementation.Capabilities...)
	}
	return value
}

func credential(ctx context.Context, resolver port.SecretResolver, name string, required bool) (string, error) {
	if resolver == nil {
		if required {
			return "", port.ErrInvalid
		}
		return "", nil
	}
	secret, err := resolver.Resolve(ctx, name)
	if secret != nil {
		defer secret.Close()
	}
	if canceled := ctx.Err(); canceled != nil {
		return "", canceled
	}
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return "", failure(err)
		}
		if !required && errors.Is(err, port.ErrNotFound) {
			return "", nil
		}
		return "", port.ErrInvalid
	}
	if secret == nil {
		return "", port.ErrInvalid
	}
	value := secret.Bytes()
	if len(value) > 65536 || required && len(value) == 0 {
		return "", port.ErrInvalid
	}
	return string(value), nil
}
func endpoint(value string) bool {
	u, err := url.Parse(value)
	return err == nil && u.Host != "" && (u.Scheme == "http" || u.Scheme == "https") && u.User == nil && u.RawQuery == "" && u.Fragment == ""
}

// Prepare decodes the operator-supplied public configuration and resolves only
// the secrets declared by the exact plugin descriptor before backend admission.
func (f *Factory) Prepare(ctx context.Context, input port.PluginConfig) (port.Candidate, error) {
	if f == nil || ctx == nil {
		return nil, port.ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	published := f.Descriptor()
	if err := admission.Validate(published); err != nil || published.PluginID != PluginID || published.Contract != port.Contract || published.Runtime != "go" {
		return nil, port.ErrInvalid
	}
	implementation, found := published.Implementation(input.ImplementationID)
	if !found {
		return nil, port.ErrUnsupported
	}
	values := map[string]string{}
	var root string
	var expectedSecrets map[string]bool
	backend := strings.TrimPrefix(input.ImplementationID, "archive-")
	archived := backend != input.ImplementationID
	if archived && input.ImplementationID != "archive-s3" && input.ImplementationID != "archive-webdav" && input.ImplementationID != "archive-drive" {
		return nil, port.ErrUnsupported
	}
	var archivePath string
	var sourceView bool
	switch backend {
	case "s3":
		var config S3PublicConfig
		var decode error
		if archived {
			var ac S3ArchivePublicConfig
			decode = admission.StrictJSON(input.Public, &ac)
			config = ac.S3PublicConfig
			archivePath = ac.ArchivePath
			sourceView = ac.SourceView
		} else {
			decode = admission.StrictJSON(input.Public, &config)
		}
		bucket, bucketErr := namespacePath(config.Bucket, false)
		prefix, rootErr := namespacePath(config.Root, true)
		if decode != nil || config.Region == "" || bucketErr != nil || bucket != config.Bucket || path.Base(bucket) != bucket || rootErr != nil || config.Endpoint != "" && !endpoint(config.Endpoint) {
			return nil, port.ErrInvalid
		}
		values = map[string]string{"provider": "Other", "env_auth": "false", "region": config.Region, "endpoint": config.Endpoint, "force_path_style": strconv.FormatBool(config.ForcePathStyle), "upload_concurrency": "1", "chunk_size": "5Mi"}
		root = bucket
		if prefix != "" {
			root += "/" + prefix
		}
		expectedSecrets = map[string]bool{"access_key_id": true, "secret_access_key": true, "session_token": false}
	case "webdav", "webdav-streaming":
		backend = "webdav"
		var config WebDAVPublicConfig
		var decode error
		if archived {
			var ac WebDAVArchivePublicConfig
			decode = admission.StrictJSON(input.Public, &ac)
			config = ac.WebDAVPublicConfig
			archivePath = ac.ArchivePath
			sourceView = ac.SourceView
		} else {
			decode = admission.StrictJSON(input.Public, &config)
		}
		var rootErr error
		root, rootErr = namespacePath(config.Root, true)
		if decode != nil || !endpoint(config.URL) || rootErr != nil {
			return nil, port.ErrInvalid
		}
		vendor := "other"
		if input.ImplementationID == "webdav-streaming" {
			vendor = "rclone"
		}
		values = map[string]string{"url": config.URL, "user": config.Username, "vendor": vendor}
		expectedSecrets = map[string]bool{"password": false}
	case "drive":
		var config DrivePublicConfig
		var decode error
		if archived {
			var ac DriveArchivePublicConfig
			decode = admission.StrictJSON(input.Public, &ac)
			config = ac.DrivePublicConfig
			archivePath = ac.ArchivePath
			sourceView = ac.SourceView
		} else {
			decode = admission.StrictJSON(input.Public, &config)
		}
		var rootErr error
		root, rootErr = namespacePath(config.Root, true)
		if decode != nil || rootErr != nil {
			return nil, port.ErrInvalid
		}
		values = map[string]string{"root_folder_id": config.RootFolderID, "client_id": config.ClientID}
		expectedSecrets = map[string]bool{"token": true, "client_secret": false}
	default:
		return nil, port.ErrUnsupported
	}
	var archive *rclonestore.ArchiveOptions
	if archived {
		archive = &rclonestore.ArchiveOptions{Path: archivePath, SourceView: sourceView}
	}
	options := rclonestore.FilesystemOptions{Archive: archive}
	if rclonestore.ValidateFilesystemOptions(options) != nil || !declaresSecrets(implementation, expectedSecrets) {
		return nil, port.ErrInvalid
	}
	// Resolve in a stable order, only after all public admission checks. These
	// names must match the implementation's published write-only schema fields.
	for _, name := range []string{"access_key_id", "secret_access_key", "session_token", "password", "token", "client_secret"} {
		required, declared := expectedSecrets[name]
		if !declared {
			continue
		}
		value, err := credential(ctx, input.Secrets, name, required)
		if err != nil {
			return nil, err
		}
		if name == "token" && !json.Valid([]byte(value)) {
			return nil, port.ErrInvalid
		}
		if name == "password" {
			name = "pass"
		}
		values[name] = value
	}
	store, err := rclonestore.OpenFilesystem(ctx, func(owned context.Context) (fs.Fs, error) {
		return openBackend(owned, backend, root, values)
	}, options)
	if err != nil {
		return nil, failure(err)
	}
	return &candidate{client: &Client{store: store, readOnly: archived, unknownSize: !archived && input.ImplementationID != "webdav"}}, nil
}

type candidate struct {
	mu                                        sync.Mutex
	client                                    *Client
	validated, validating, published, aborted bool
}

func (c *candidate) Validate(ctx context.Context) error {
	c.mu.Lock()
	if c.aborted || c.published || c.validating {
		c.mu.Unlock()
		return port.ErrInvalid
	}
	c.validating = true
	c.validated = false
	c.mu.Unlock()
	_, err := c.client.List(ctx, port.ListRequest{Path: "/", Limit: 1})
	c.mu.Lock()
	defer c.mu.Unlock()
	c.validating = false
	if c.aborted {
		if err != nil {
			return err
		}
		return port.ErrInvalid
	}
	if err == nil {
		c.validated = true
	}
	return err
}
func (c *candidate) Publish() (port.Client, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.validated || c.validating || c.published || c.aborted {
		return nil, port.ErrInvalid
	}
	c.published = true
	return c.client, nil
}
func (c *candidate) Abort(ctx context.Context) error {
	c.mu.Lock()
	if c.published {
		c.mu.Unlock()
		return port.ErrInvalid
	}
	c.aborted = true
	c.mu.Unlock()
	return c.client.Close(ctx)
}
