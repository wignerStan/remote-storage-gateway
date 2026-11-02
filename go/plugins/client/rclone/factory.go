package rcloneclient

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"strconv"
	"strings"
	"sync"

	"github.com/wignerStan/infra/go/storage/cloud"
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
}
type S3ArchiveConfiguration struct {
	S3Configuration
	ArchivePath string `json:"archive_path" minLength:"1" maxLength:"4096"`
}
type WebDAVArchivePublicConfig struct {
	WebDAVPublicConfig
	ArchivePath string `json:"archive_path" minLength:"1" maxLength:"4096"`
}
type WebDAVArchiveConfiguration struct {
	WebDAVConfiguration
	ArchivePath string `json:"archive_path" minLength:"1" maxLength:"4096"`
}
type DriveArchivePublicConfig struct {
	DrivePublicConfig
	ArchivePath string `json:"archive_path" minLength:"1" maxLength:"4096"`
}
type DriveArchiveConfiguration struct {
	DriveConfiguration
	ArchivePath string `json:"archive_path" minLength:"1" maxLength:"4096"`
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
	if err != nil {
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

func (f *Factory) Prepare(ctx context.Context, input port.PluginConfig) (port.Candidate, error) {
	if err := admission.Validate(f.Descriptor()); err != nil {
		return nil, port.ErrInvalid
	}
	values := map[string]string{}
	var root string
	backend := strings.TrimPrefix(input.ImplementationID, "archive-")
	archived := backend != input.ImplementationID
	if archived && input.ImplementationID != "archive-s3" && input.ImplementationID != "archive-webdav" && input.ImplementationID != "archive-drive" {
		return nil, port.ErrUnsupported
	}
	var archivePath string
	switch backend {
	case "s3":
		var config S3PublicConfig
		var decode error
		if archived {
			var ac S3ArchivePublicConfig
			decode = admission.StrictJSON(input.Public, &ac)
			config = ac.S3PublicConfig
			archivePath = ac.ArchivePath
		} else {
			decode = admission.StrictJSON(input.Public, &config)
		}
		if decode != nil || config.Region == "" || config.Bucket == "" || strings.Contains(config.Bucket, "/") || config.Endpoint != "" && !endpoint(config.Endpoint) {
			return nil, port.ErrInvalid
		}
		access, err := credential(ctx, input.Secrets, "access_key_id", true)
		if err != nil {
			return nil, err
		}
		secret, err := credential(ctx, input.Secrets, "secret_access_key", true)
		if err != nil {
			return nil, err
		}
		token, err := credential(ctx, input.Secrets, "session_token", false)
		if err != nil {
			return nil, err
		}
		values = map[string]string{"provider": "Other", "env_auth": "false", "access_key_id": access, "secret_access_key": secret, "session_token": token, "region": config.Region, "endpoint": config.Endpoint, "force_path_style": strconv.FormatBool(config.ForcePathStyle), "upload_concurrency": "1", "chunk_size": "5Mi"}
		root = config.Bucket
		if config.Root != "" {
			root += "/" + config.Root
		}
	case "webdav", "webdav-streaming":
		backend = "webdav"
		var config WebDAVPublicConfig
		var decode error
		if archived {
			var ac WebDAVArchivePublicConfig
			decode = admission.StrictJSON(input.Public, &ac)
			config = ac.WebDAVPublicConfig
			archivePath = ac.ArchivePath
		} else {
			decode = admission.StrictJSON(input.Public, &config)
		}
		if decode != nil || !endpoint(config.URL) {
			return nil, port.ErrInvalid
		}
		password, err := credential(ctx, input.Secrets, "password", false)
		if err != nil {
			return nil, err
		}
		vendor := "other"
		if input.ImplementationID == "webdav-streaming" {
			vendor = "rclone"
		}
		values = map[string]string{"url": config.URL, "user": config.Username, "pass": password, "vendor": vendor}
		root = config.Root
	case "drive":
		var config DrivePublicConfig
		var decode error
		if archived {
			var ac DriveArchivePublicConfig
			decode = admission.StrictJSON(input.Public, &ac)
			config = ac.DrivePublicConfig
			archivePath = ac.ArchivePath
		} else {
			decode = admission.StrictJSON(input.Public, &config)
		}
		if decode != nil {
			return nil, port.ErrInvalid
		}
		token, err := credential(ctx, input.Secrets, "token", true)
		if err != nil || !json.Valid([]byte(token)) {
			return nil, port.ErrInvalid
		}
		secret, err := credential(ctx, input.Secrets, "client_secret", false)
		if err != nil {
			return nil, err
		}
		values = map[string]string{"root_folder_id": config.RootFolderID, "client_id": config.ClientID, "client_secret": secret, "token": token}
		root = config.Root
	default:
		return nil, port.ErrUnsupported
	}
	var archive *cloud.ArchiveOptions
	if archived {
		if archivePath == "" || backend == "webdav-streaming" {
			return nil, port.ErrInvalid
		}
		archive = &cloud.ArchiveOptions{Path: archivePath}
	}
	store, err := cloud.New(ctx, cloud.Config{Backend: backend, Root: root, Parameters: values, Archive: archive})
	if err != nil {
		return nil, failure(err)
	}
	return &candidate{client: &Client{store: store, readOnly: archived}}, nil
}

type candidate struct {
	mu                            sync.Mutex
	client                        *Client
	validated, published, aborted bool
}

func (c *candidate) Validate(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.aborted || c.published {
		return port.ErrInvalid
	}
	_, err := c.client.List(ctx, port.ListRequest{Path: "/", Limit: 1})
	if err == nil {
		c.validated = true
	}
	return err
}
func (c *candidate) Publish() (port.Client, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.validated || c.published || c.aborted {
		return nil, port.ErrInvalid
	}
	c.published = true
	return c.client, nil
}
func (c *candidate) Abort(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.published {
		return port.ErrInvalid
	}
	c.aborted = true
	return c.client.Close(ctx)
}
