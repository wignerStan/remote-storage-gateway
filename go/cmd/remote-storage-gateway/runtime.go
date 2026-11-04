//go:build dataplane_l7

package main

import (
	"context"
	"crypto/tls"
	"errors"
	"github.com/wignerStan/infra/go/network/proxy/l7"
	"github.com/wignerStan/network-data-plane-worker/go/gateway"
	"github.com/wignerStan/network-data-plane-worker/go/pluginruntime"
	"github.com/wignerStan/network-data-plane-worker/go/worker"
	"net/http"
	"net/url"
	"sync"
)

type application struct {
	*gateway.Runtime
	Manager   *pluginruntime.Manager
	Registry  *pluginruntime.Registry
	config    gateway.Config
	manifest  worker.Manifest
	proxy     http.Handler
	transport *http.Transport
	control   http.Handler
	handler   http.Handler
	tlsConfig *tls.Config
	upstream  url.URL
	closeOnce sync.Once
}

func (r *application) Close(ctx context.Context) error {
	var err error
	if r.Runtime != nil {
		err = r.Runtime.Close(ctx)
	} else {
		err = r.Manager.Close(ctx)
	}
	if err == nil {
		r.closeOnce.Do(func() {
			if r.transport != nil {
				r.transport.CloseIdleConnections()
			}
		})
	}
	return err
}
func (r *application) TLSConfig(context.Context) (*tls.Config, error) { return r.tlsConfig, nil }

func (r *application) selectControlTarget(ctx context.Context, request l7.Envelope) (url.URL, error) {
	original, err := url.ParseRequestURI(request.URL)
	if err != nil {
		return url.URL{}, errors.New("invalid request target")
	}
	target := r.upstream
	target.Path, target.RawPath, target.RawQuery = original.Path, original.RawPath, original.RawQuery
	return target, nil
}
