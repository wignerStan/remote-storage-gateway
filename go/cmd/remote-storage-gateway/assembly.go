//go:build dataplane_l7

package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"github.com/gin-gonic/gin"
	schemaapi "github.com/wignerStan/infra/go/code/schema/api"
	"github.com/wignerStan/infra/go/network/proxy/l7"
	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/descriptor"
	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/descriptor/admission"
	"github.com/wignerStan/network-data-plane-worker/go/gateway"
	gatewayhttpapi "github.com/wignerStan/network-data-plane-worker/go/gateway/httpapi"
	"github.com/wignerStan/network-data-plane-worker/go/management/httpapi"
	"github.com/wignerStan/network-data-plane-worker/go/pluginruntime"
	"github.com/wignerStan/network-data-plane-worker/go/worker"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

func loadConfig(path string) (gateway.Config, error) {
	var c gateway.Config
	f, err := os.Open(path)
	if err != nil {
		return c, err
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, descriptor.MaxBytes+1))
	if err != nil {
		return c, err
	}
	if err = admission.StrictJSON(b, &c); err != nil {
		return c, err
	}
	return c, nil
}
func assemble(ctx context.Context, c gateway.Config) (runtime *application, err error) {
	encoded, err := json.Marshal(c)
	if err != nil {
		return nil, err
	}
	if err = admission.StrictJSON(encoded, &c); err != nil {
		return nil, err
	}
	if c.Listen == "" || c.Username == "" || c.AuthSecret == "" || c.Worker.Schema != worker.BootstrapSchema || len(c.Worker.Artifacts) == 0 || len(c.Worker.Artifacts) > 16 || len(c.Worker.Clients) == 0 || len(c.Worker.Clients) > 64 || len(c.Mounts) == 0 || len(c.Mounts) > 64 {
		return nil, errors.New("invalid gateway configuration")
	}
	secrets := worker.FileSecrets(c.Worker.SecretFiles)
	// Management can reconfigure providers, but cannot grant a provider access
	// to the ingress password or TLS key. Only operator-selected client refs
	// enter the provider resolver; host credentials stay under the host owner.
	clientSecrets := worker.FileSecrets{}
	reserved := map[string]bool{c.AuthSecret: true}
	if c.TLS != nil {
		reserved[c.TLS.PrivateKeySecret] = true
	}
	for _, selection := range c.Worker.Clients {
		for _, ref := range selection.SecretRefs {
			for hostRef := range reserved {
				if ref == hostRef || secrets[ref] != "" && secrets[ref] == secrets[hostRef] {
					return nil, errors.New("provider credential overlaps ingress credentials")
				}
			}
			clientSecrets[ref] = secrets[ref]
		}
	}
	auth, err := secrets.Resolve(ctx, c.AuthSecret)
	if err != nil {
		return nil, errors.New("gateway authentication secret unavailable")
	}
	password := append([]byte(nil), auth.Bytes()...)
	auth.Close()
	defer clear(password)
	if len(password) < 16 {
		return nil, errors.New("gateway authentication secret too short")
	}
	registry, err := worker.Discover(ctx, c.Worker)
	if err != nil {
		return nil, errors.New("approved plugin artifact could not load")
	}
	if err = worker.Validate(c.Worker, registry); err != nil {
		return nil, err
	}
	manifest, err := worker.Capabilities(registry)
	if err != nil {
		return nil, err
	}
	manager, err := pluginruntime.NewManager(registry, clientSecrets)
	if err != nil {
		return nil, err
	}
	r := &application{Manager: manager, Registry: registry, config: c, manifest: manifest}
	defer func() {
		if err != nil {
			_ = r.Close(context.Background())
		}
	}()
	if err = r.prepareIngress(ctx); err != nil {
		return nil, err
	}
	clients := map[string]bool{}
	for _, selection := range c.Worker.Clients {
		if clients[selection.Name] {
			return nil, errors.New("duplicate client selection")
		}
		clients[selection.Name] = true
		if _, err = manager.Replace(ctx, manager.Current(selection.Name), selection); err != nil {
			return nil, err
		}
	}
	names := make([]string, 0, len(clients))
	for name := range clients {
		names = append(names, name)
	}
	r.Runtime, err = gateway.New(r.config, manager, password)
	if err != nil {
		return nil, err
	}
	entry, err := httpapi.NewEntry(httpapi.Options{Registry: registry, Manager: manager, Manifest: manifest, ClientNames: names})
	if err != nil {
		return nil, err
	}
	routes, err := schemaapi.Compose(schemaapi.Config{
		Title: "Network data-plane Worker management", Version: "1",
		OpenAPIPath: "/v1/openapi", SchemasPath: "/v1/schemas",
		Guard: entry.Guard, Headers: map[string]string{"Cache-Control": "no-store"},
	}, entry.RegisterRoutes)
	if err != nil {
		return nil, err
	}
	engine := gin.New()
	engine.RedirectTrailingSlash = false
	engine.RedirectFixedPath = false
	engine.HandleMethodNotAllowed = true
	if err = engine.SetTrustedProxies(nil); err != nil {
		return nil, err
	}
	if err = schemaapi.MountGin(engine, nil, routes); err != nil {
		return nil, err
	}
	r.control = engine
	router := gin.New()
	gatewayhttpapi.RegisterGin(router, r.Runtime, r.control, r.proxy)
	r.handler = router
	return r, nil
}
func requestHost(host string) string {
	if h, _, e := net.SplitHostPort(host); e == nil {
		host = h
	}
	return strings.ToLower(strings.TrimSuffix(host, "."))
}

func (r *application) prepareIngress(ctx context.Context) error {
	if r.config.TLS != nil {
		var err error
		r.tlsConfig, err = r.loadTLSConfig(ctx)
		if err != nil {
			return err
		}
	}
	c := r.config.ControlPlane
	if c == nil {
		return nil
	}
	if r.config.TLS == nil || c.Host == "" || len(c.Host) > 253 || strings.ContainsAny(c.Host, "/\\@?#:%\x00 \t\r\n") {
		return errors.New("control-plane ingress requires TLS and one exact DNS host")
	}
	c.Host = requestHost(c.Host)
	u, e := url.Parse(c.Upstream)
	if e != nil || u.Hostname() == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Opaque != "" || (u.Path != "" && u.Path != "/") {
		return errors.New("invalid control-plane upstream")
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.ResponseHeaderTimeout = 30 * time.Second
	r.transport = transport
	r.upstream = *u
	r.proxy, e = l7.NewProxy(l7.ProxyOptions{Transport: transport, PassHostHeader: true, Select: r.selectControlTarget})
	return e
}

// TLS terminates in the data-plane server. Certificate renewal updates the
// operator Secret and rolls the process, preserving drain and secret ownership.
func (r *application) loadTLSConfig(ctx context.Context) (*tls.Config, error) {
	c := r.config.TLS
	if c == nil {
		return nil, nil
	}
	if c.CertificateFile == "" || c.PrivateKeySecret == "" {
		return nil, errors.New("TLS certificate and private-key reference required")
	}
	f, err := os.Open(c.CertificateFile)
	if err != nil {
		return nil, errors.New("TLS certificate unavailable")
	}
	defer f.Close()
	stat, err := f.Stat()
	if err != nil || !stat.Mode().IsRegular() || stat.Size() > 65536 {
		return nil, errors.New("invalid TLS certificate file")
	}
	cert, err := io.ReadAll(io.LimitReader(f, 65537))
	if err != nil || len(cert) > 65536 {
		return nil, errors.New("invalid TLS certificate file")
	}
	key, err := worker.FileSecrets(r.config.Worker.SecretFiles).Resolve(ctx, c.PrivateKeySecret)
	if err != nil {
		return nil, errors.New("TLS private key unavailable")
	}
	defer key.Close()
	pair, err := tls.X509KeyPair(cert, key.Bytes())
	if err != nil {
		return nil, errors.New("invalid TLS certificate/key pair")
	}
	return &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{pair}}, nil
}
