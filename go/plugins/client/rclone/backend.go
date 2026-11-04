package rcloneclient

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"path"
	"strings"
	"sync"
	"unicode/utf8"

	_ "github.com/rclone/rclone/backend/drive"
	_ "github.com/rclone/rclone/backend/s3"
	_ "github.com/rclone/rclone/backend/webdav"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/obscure"
	port "github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/client"
	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/descriptor"
	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/descriptor/admission"
)

// backendParameters is private to one provider generation. In particular OAuth
// refreshes update this map, never an ambient rclone configuration file. Reads
// and refresh writes can occur concurrently inside a provider.
type backendParameters struct {
	mu     sync.RWMutex
	values map[string]string
}

func (p *backendParameters) Get(key string) (string, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	value, ok := p.values[key]
	return value, ok
}

func (p *backendParameters) Set(key, value string) {
	p.mu.Lock()
	p.values[key] = value
	p.mu.Unlock()
}

// openBackend is the product's provider selection boundary. Defaults come from
// the selected provider's declared options, without environment/config/flag
// overrides or the global filesystem cache used by rclone's command line.
func openBackend(ctx context.Context, backend, root string, values map[string]string) (fs.Fs, error) {
	registered, err := fs.Find(backend)
	if err != nil {
		return nil, port.ErrUnsupported
	}
	parameters := &backendParameters{values: map[string]string{}}
	for _, option := range registered.Options {
		option.Value = nil
		parameters.Set(option.Name, option.String())
	}
	for key, value := range values {
		parameters.Set(key, value)
	}
	if backend == "webdav" && values["pass"] != "" {
		password, err := obscure.Obscure(values["pass"])
		if err != nil {
			return nil, port.ErrProvider
		}
		parameters.Set("pass", password)
	}
	var identity [16]byte
	if _, err := rand.Read(identity[:]); err != nil {
		return nil, port.ErrProvider
	}
	return registered.NewFs(ctx, "gateway-"+hex.EncodeToString(identity[:]), root, parameters)
}

// namespacePath admits both relative and absolute Worker paths, with a single
// optional trailing slash for directories. Cursors use the relative form.
func namespacePath(name string, directory bool) (string, error) {
	if len(name) > 4096 || !utf8.ValidString(name) || strings.ContainsAny(name, "\\\x00") {
		return "", port.ErrInvalid
	}
	if directory && (name == "" || name == "/") {
		return "", nil
	}
	name = strings.TrimPrefix(name, "/")
	if directory {
		name = strings.TrimSuffix(name, "/")
	}
	if name == "" || path.Clean(name) != name || strings.HasPrefix(name, "/") {
		return "", port.ErrInvalid
	}
	parts := strings.Split(name, "/")
	if len(parts) > 64 {
		return "", port.ErrInvalid
	}
	for _, part := range parts {
		if part == "" || part == "." || part == ".." || len(part) > 255 {
			return "", port.ErrInvalid
		}
	}
	for _, r := range name {
		if r < 0x20 || r == 0x7f {
			return "", port.ErrInvalid
		}
	}
	return name, nil
}

func declaresSecrets(implementation descriptor.Implementation, expected map[string]bool) bool {
	fields := admission.SecretFields(implementation)
	if len(fields) != len(expected) {
		return false
	}
	for _, field := range fields {
		required, ok := expected[field.Name]
		if !ok || required != field.Required {
			return false
		}
	}
	return true
}
