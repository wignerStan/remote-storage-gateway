// Package gateway owns private storage namespace policy and exchange use-flows.
// HTTP binding and resource/bootstrap composition live outside this package.
package gateway

import (
	"context"
	"crypto/subtle"
	"errors"
	"os"
	"path"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/client"
	workercontract "github.com/wignerStan/network-data-plane-worker/go/contracts/worker"
)

type Mount struct {
	Name     string `json:"name"`
	Client   string `json:"client"`
	ReadOnly bool   `json:"read_only,omitempty"`
}
type TLSOptions struct {
	CertificateFile  string `json:"certificate_file"`
	PrivateKeySecret string `json:"private_key_secret"`
}
type ControlPlaneRoute struct {
	Host     string `json:"host"`
	Upstream string `json:"upstream"`
}
type Config struct {
	Listen       string                   `json:"listen"`
	Username     string                   `json:"username"`
	AuthSecret   string                   `json:"auth_secret"`
	Worker       workercontract.Bootstrap `json:"worker"`
	Mounts       []Mount                  `json:"mounts"`
	TLS          *TLSOptions              `json:"tls,omitempty"`
	ControlPlane *ControlPlaneRoute       `json:"control_plane,omitempty"`
}
type Runtime struct {
	clients               client.ManagedClients
	mounts                []Mount
	username, controlHost string
	password              []byte
	mu                    sync.RWMutex
	closeOnce             sync.Once
}

func New(config Config, clients client.ManagedClients, password []byte) (*Runtime, error) {
	if config.Username == "" || len(password) < 16 || len(config.Mounts) > 64 || len(config.Mounts) > 0 && clients == nil {
		return nil, errors.New("invalid gateway grants")
	}
	known := map[string]bool{}
	for _, selection := range config.Worker.Clients {
		known[selection.Name] = true
	}
	seen := map[string]bool{}
	for _, mount := range config.Mounts {
		if mount.Name == "" || strings.ContainsAny(mount.Name, "/\\\x00") || mount.Name == "." || mount.Name == ".." || seen[mount.Name] || !known[mount.Client] {
			return nil, errors.New("invalid gateway mount")
		}
		seen[mount.Name] = true
	}
	controlHost := ""
	if config.ControlPlane != nil {
		controlHost = config.ControlPlane.Host
	}
	return &Runtime{clients: clients, mounts: append([]Mount(nil), config.Mounts...), username: config.Username, password: append([]byte(nil), password...), controlHost: controlHost}, nil
}
func (r *Runtime) Close(ctx context.Context) error {
	if r.clients != nil {
		if err := r.clients.Close(ctx); err != nil {
			return err
		}
	}
	r.closeOnce.Do(func() { r.mu.Lock(); clear(r.password); r.mu.Unlock() })
	return nil
}
func (r *Runtime) Authorize(username, password string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return subtle.ConstantTimeCompare([]byte(username), []byte(r.username)) == 1 && subtle.ConstantTimeCompare([]byte(password), r.password) == 1
}
func (r *Runtime) Mounts() []Mount { return append([]Mount(nil), r.mounts...) }

type Route uint8

const (
	Storage Route = iota
	Administration
	Misdirected
	Denied
)

func (r *Runtime) Route(host, path string) Route {
	reserved := path == "/healthz" || path == "/v1" || strings.HasPrefix(path, "/v1/") || path == "/dav" || strings.HasPrefix(path, "/dav/")
	if r.controlHost == "" || reserved {
		return Storage
	}
	if host != r.controlHost {
		return Misdirected
	}
	for _, exact := range []string{"/", "/ping", "/favicon.ico", "/robots.txt", "/manifest.json", "/api/public/settings", "/api/public/init_status"} {
		if path == exact {
			return Administration
		}
	}
	for _, prefix := range []string{"/api/admin", "/api/auth", "/api/authn", "/api/me", "/@manage", "/@login", "/assets", "/_app", "/static", "/images", "/icons"} {
		if path == prefix || strings.HasPrefix(path, prefix+"/") {
			return Administration
		}
	}
	return Denied
}
func (r *Runtime) Resolve(name string) (Mount, string, error) {
	if !utf8.ValidString(name) || strings.ContainsAny(name, "\\\x00") {
		return Mount{}, "", os.ErrInvalid
	}
	for _, segment := range strings.Split(name, "/") {
		if segment == ".." {
			return Mount{}, "", os.ErrPermission
		}
	}
	name = path.Clean("/" + strings.TrimPrefix(name, "/"))
	if name == "/" {
		return Mount{}, "/", nil
	}
	parts := strings.SplitN(strings.TrimPrefix(name, "/"), "/", 2)
	for _, mount := range r.mounts {
		if mount.Name == parts[0] {
			object := "/"
			if len(parts) == 2 {
				object += parts[1]
			}
			return mount, object, nil
		}
	}
	return Mount{}, "", os.ErrNotExist
}

type Exchange struct {
	leases map[string]client.Lease
	grants map[string]Mount
	once   sync.Once
}

func (r *Runtime) BeginExchange(name string, mutation bool) (*Exchange, error) {
	mount, _, err := r.Resolve(name)
	if err != nil {
		return nil, err
	}
	if mutation && mount.ReadOnly {
		return nil, os.ErrPermission
	}
	selected := []Mount{mount}
	if mount.Name == "" {
		selected = r.mounts
	}
	x := &Exchange{leases: map[string]client.Lease{}, grants: map[string]Mount{}}
	for _, grant := range selected {
		x.grants[grant.Name] = grant
		if x.leases[grant.Client] != nil {
			continue
		}
		lease, err := r.clients.Pin(grant.Client)
		if err != nil {
			x.Release()
			return nil, err
		}
		x.leases[grant.Client] = lease
	}
	return x, nil
}
func (x *Exchange) Release() {
	x.once.Do(func() {
		for _, lease := range x.leases {
			lease.Release()
		}
	})
}
func (x *Exchange) ClientFor(name string) (client.Client, error) {
	grant, ok := x.grants[name]
	if !ok || x.leases[grant.Client] == nil {
		return nil, os.ErrPermission
	}
	return scopedClient{Client: x.leases[grant.Client].Client(), readOnly: grant.ReadOnly}, nil
}
func (x *Exchange) Version(name string) workercontract.Version {
	grant, ok := x.grants[name]
	if !ok || x.leases[grant.Client] == nil {
		return workercontract.Version{}
	}
	return x.leases[grant.Client].Version()
}

type scopedClient struct {
	client.Client
	readOnly bool
}

func (c scopedClient) Put(ctx context.Context, request client.PutRequest) (client.PutResult, error) {
	if c.readOnly {
		return client.PutResult{}, os.ErrPermission
	}
	return c.Client.Put(ctx, request)
}
func (c scopedClient) Delete(ctx context.Context, request client.ObjectRef) error {
	if c.readOnly {
		return os.ErrPermission
	}
	return c.Client.Delete(ctx, request)
}
func (c scopedClient) Close(context.Context) error { return nil }
