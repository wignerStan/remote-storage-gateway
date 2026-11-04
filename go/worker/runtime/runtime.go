//go:build dataplane_l7

package runtime

import (
	"context"
	"crypto/tls"
	"errors"
	"github.com/wignerStan/infra/go/network/proxy/l7"
	"github.com/wignerStan/network-data-plane-worker/go/pluginruntime"
	"github.com/wignerStan/network-data-plane-worker/go/worker"
	workerconfig "github.com/wignerStan/remote-storage-gateway/go/contracts/worker"
	control "github.com/wignerStan/remote-storage-gateway/go/control/ttrpc"
	"github.com/wignerStan/remote-storage-gateway/go/gateway"
	"net/http"
	"net/url"
	"sync"
)

type Runtime struct {
	encoded         *encodedBridge
	providerSecrets worker.FileSecrets
	*gateway.Runtime
	Manager           *pluginruntime.Manager
	Registry          *pluginruntime.Registry
	config            workerconfig.RuntimeConfig
	manifest          worker.Manifest
	proxy             http.Handler
	transport         *http.Transport
	control           http.Handler
	handler           http.Handler
	tlsConfig         *tls.Config
	upstream          url.URL
	closeOnce         sync.Once
	lifeMu            sync.Mutex
	process           workerconfig.ProcessConfig
	httpServer        *l7.Server
	localDataServer   *l7.Server
	controlServer     *control.Server
	started, retiring bool
	closeDone         chan struct{}
	closeErr          error
	stopped           chan struct{}
}

func (r *Runtime) TLSConfig(context.Context) (*tls.Config, error) { return r.tlsConfig, nil }

func (r *Runtime) selectControlTarget(ctx context.Context, request l7.Envelope) (url.URL, error) {
	original, err := url.ParseRequestURI(request.URL)
	if err != nil {
		return url.URL{}, errors.New("invalid request target")
	}
	target := r.upstream
	target.Path, target.RawPath, target.RawQuery = original.Path, original.RawPath, original.RawQuery
	return target, nil
}
