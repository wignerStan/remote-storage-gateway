//go:build dataplane_l7

package runtime

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"github.com/gin-gonic/gin"
	schemaapi "github.com/wignerStan/infra/go/network/http/openapi"
	openapigin "github.com/wignerStan/infra/go/network/http/openapi/gin"
	"github.com/wignerStan/infra/go/network/proxy/l7"
	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/descriptor/admission"
	"github.com/wignerStan/network-data-plane-worker/go/management/httpapi"
	"github.com/wignerStan/network-data-plane-worker/go/pluginruntime"
	"github.com/wignerStan/network-data-plane-worker/go/worker"
	workerconfig "github.com/wignerStan/remote-storage-gateway/go/contracts/worker"
	"github.com/wignerStan/remote-storage-gateway/go/gateway"
	gatewayhttpapi "github.com/wignerStan/remote-storage-gateway/go/gateway/httpapi"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func assemble(ctx context.Context, c workerconfig.RuntimeConfig) (runtime *Runtime, err error) {
	encoded, err := json.Marshal(c)
	if err != nil {
		return nil, err
	}
	if err = admission.StrictJSON(encoded, &c); err != nil {
		return nil, err
	}
	if c.Listen == "" || c.Username == "" || c.AuthSecret == "" || c.Worker.Schema != worker.BootstrapSchema || len(c.Worker.Artifacts) > 16 || len(c.Worker.Clients) > 64 || len(c.Mounts) > 64 {
		return nil, errors.New("invalid gateway configuration")
	}
	if c.Encoded == nil && (len(c.Worker.Artifacts) == 0 || len(c.Worker.Clients) == 0 || len(c.Mounts) == 0) {
		return nil, errors.New("worker has no configured data service")
	}
	secrets := worker.FileSecrets(c.Worker.SecretFiles)
	// Management can reconfigure providers, but cannot grant a provider access
	// to the ingress password or TLS key. Only operator-preapproved client refs
	// enter the provider resolver; host credentials stay under the host owner.
	clientSecrets := worker.FileSecrets{}
	reserved := map[string]bool{c.AuthSecret: true}
	if c.TLS != nil {
		reserved[c.TLS.PrivateKeySecret] = true
	}

	if len(c.ClientSecretRefs) > 128 {
		return nil, errors.New("too many provider secret grants")
	}
	grant := func(ref string) error {
		selected := secrets[ref]
		if ref == "" || selected == "" {
			return errors.New("provider credential ref unavailable")
		}
		selectedInfo, _ := os.Stat(selected)
		for hostRef := range reserved {
			hostPath := secrets[hostRef]
			hostInfo, _ := os.Stat(hostPath)
			sameFile := selectedInfo != nil && hostInfo != nil && os.SameFile(selectedInfo, hostInfo)
			if ref == hostRef || hostPath != "" && (filepath.Clean(selected) == filepath.Clean(hostPath) || sameFile) {
				return errors.New("provider credential overlaps ingress credentials")
			}
		}
		clientSecrets[ref] = selected
		return nil
	}
	for _, ref := range c.ClientSecretRefs {
		if err := grant(ref); err != nil {
			return nil, err
		}
	}
	for _, selection := range c.Worker.Clients {
		for _, ref := range selection.SecretRefs {
			if err := grant(ref); err != nil {
				return nil, err
			}
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
	r := &Runtime{providerSecrets: clientSecrets, Manager: manager, Registry: registry, config: c, manifest: manifest, closeDone: make(chan struct{}), stopped: make(chan struct{})}
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
	entry, err := httpapi.NewEntry(httpapi.Options{Control: r, ClientNames: names})
	if err != nil {
		return nil, err
	}
	routes, err := schemaapi.Compose(schemaapi.Config{
		Title: "Network data-plane Worker management", Version: "1",
	}, entry.RegisterRoutes)
	if err != nil {
		return nil, err
	}
	engine, err := openapigin.New(routes, openapigin.Options{OpenAPIPath: "/v1/openapi", SchemasPath: "/v1/schemas", Guard: entry.Guard, Headers: map[string]string{"Cache-Control": "no-store"}})
	if err != nil {
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

func (r *Runtime) prepareIngress(ctx context.Context) error {
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
func (r *Runtime) loadTLSConfig(ctx context.Context) (*tls.Config, error) {
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
