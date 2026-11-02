package rcloneclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	_ "github.com/rclone/rclone/backend/drive"
	_ "github.com/rclone/rclone/backend/local" // VFS cache storage, not an advertised client backend.
	_ "github.com/rclone/rclone/backend/s3"
	_ "github.com/rclone/rclone/backend/webdav"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/config/obscure"
	port "github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/client"
	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/descriptor"
	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/descriptor/admission"
)

const PluginID = "client.rclone"

type S3PublicConfig struct {
	Endpoint        string           `json:"endpoint,omitempty"`
	Region          string           `json:"region" minLength:"1"`
	Bucket          string           `json:"bucket" minLength:"1"`
	Root            string           `json:"root,omitempty"`
	ForcePathStyle  bool             `json:"force_path_style,omitempty"`
	Materialization *Materialization `json:"materialization,omitempty"`
}
type S3Configuration struct {
	S3PublicConfig
	AccessKeyID     string `json:"access_key_id" writeOnly:"true" minLength:"1"`
	SecretAccessKey string `json:"secret_access_key" writeOnly:"true" minLength:"1"`
	SessionToken    string `json:"session_token,omitempty" writeOnly:"true"`
}
type WebDAVPublicConfig struct {
	URL             string           `json:"url" minLength:"1"`
	Root            string           `json:"root,omitempty"`
	Username        string           `json:"username,omitempty"`
	Materialization *Materialization `json:"materialization,omitempty"`
}
type WebDAVConfiguration struct {
	WebDAVPublicConfig
	Password string `json:"password,omitempty" writeOnly:"true"`
}
type DrivePublicConfig struct {
	RootFolderID    string           `json:"root_folder_id,omitempty"`
	Root            string           `json:"root,omitempty"`
	ClientID        string           `json:"client_id,omitempty"`
	Materialization *Materialization `json:"materialization,omitempty"`
}
type DriveConfiguration struct {
	DrivePublicConfig
	Token        string `json:"token" writeOnly:"true" minLength:"1"`
	ClientSecret string `json:"client_secret,omitempty" writeOnly:"true"`
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

// isolatedConfig receives refreshes only for this generation, never the process
// global rclone config file. The operator owns the persisted secret references.
type isolatedConfig struct {
	mu     sync.Mutex
	values configmap.Simple
}

func (m *isolatedConfig) Get(name string) (string, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	value, ok := m.values[name]
	return value, ok
}
func (m *isolatedConfig) Set(name, value string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.values[name] = value
}

var nextGeneration atomic.Uint64

func (f *Factory) Prepare(ctx context.Context, input port.PluginConfig) (port.Candidate, error) {
	if err := admission.Validate(f.Descriptor()); err != nil {
		return nil, port.ErrInvalid
	}
	values := configmap.Simple{}
	var root string
	var materialization *Materialization
	backend := input.ImplementationID
	switch input.ImplementationID {
	case "s3":
		var config S3PublicConfig
		if admission.StrictJSON(input.Public, &config) != nil || config.Region == "" || config.Bucket == "" || strings.Contains(config.Bucket, "/") || config.Endpoint != "" && !endpoint(config.Endpoint) {
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
		values = configmap.Simple{"provider": "Other", "env_auth": "false", "access_key_id": access, "secret_access_key": secret, "session_token": token, "region": config.Region, "endpoint": config.Endpoint, "force_path_style": strconv.FormatBool(config.ForcePathStyle), "upload_concurrency": "1", "chunk_size": "5Mi"}
		root = config.Bucket
		if config.Root != "" {
			root += "/" + config.Root
		}
		materialization = config.Materialization
	case "webdav", "webdav-streaming":
		backend = "webdav"
		var config WebDAVPublicConfig
		if admission.StrictJSON(input.Public, &config) != nil || !endpoint(config.URL) {
			return nil, port.ErrInvalid
		}
		password, err := credential(ctx, input.Secrets, "password", false)
		if err != nil {
			return nil, err
		}
		if password != "" {
			password, err = obscure.Obscure(password)
			if err != nil {
				return nil, port.ErrProvider
			}
		}
		vendor := "other"
		if input.ImplementationID == "webdav-streaming" {
			vendor = "rclone"
		}
		values = configmap.Simple{"url": config.URL, "user": config.Username, "pass": password, "vendor": vendor}
		root = config.Root
		materialization = config.Materialization
	case "drive":
		var config DrivePublicConfig
		if admission.StrictJSON(input.Public, &config) != nil {
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
		values = configmap.Simple{"root_folder_id": config.RootFolderID, "client_id": config.ClientID, "client_secret": secret, "token": token}
		root = config.Root
		materialization = config.Materialization
	default:
		return nil, port.ErrUnsupported
	}
	if _, err := key(root); err != nil {
		return nil, err
	}
	registration, err := fs.Find(backend)
	if err != nil {
		return nil, port.ErrUnsupported
	}
	for _, option := range registration.Options {
		if _, exists := values[option.Name]; !exists {
			option.Value = nil
			values[option.Name] = option.String()
		}
	}
	configuration := &isolatedConfig{values: values}
	// Different generations never share rclone's VFS active cache identity.
	name := fmt.Sprintf("worker-rclone-%d", nextGeneration.Add(1))
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	backendContext, cancelBackend := context.WithCancel(context.WithoutCancel(ctx))
	stopPreparation := context.AfterFunc(ctx, cancelBackend)
	remote, err := registration.NewFs(backendContext, name, root, configuration)
	stopPreparation()
	if err != nil {
		cancelBackend()
		return nil, failure(err)
	}
	client, err := New(backendContext, remote, materialization)
	if err != nil {
		cancelBackend()
		if close := remote.Features().Shutdown; close != nil {
			_ = close(context.Background())
		}
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		cancelBackend()
		_ = client.Close(context.Background())
		return nil, err
	}
	client.backendCancel = cancelBackend
	return &candidate{client: client}, nil
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
