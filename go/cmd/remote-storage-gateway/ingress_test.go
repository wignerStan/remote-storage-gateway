//go:build dataplane_l7

package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"github.com/gin-gonic/gin"
	"github.com/wignerStan/network-data-plane-worker/go/gateway"
	gatewayhttpapi "github.com/wignerStan/network-data-plane-worker/go/gateway/httpapi"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wignerStan/infra/go/network/proxy/l7"
	"github.com/wignerStan/network-data-plane-worker/go/worker"
)

func ingressCertificate(t *testing.T) (gateway.TLSOptions, map[string]string, *x509.CertPool) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "ingress test CA"}, NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	der, err := x509.CreateCertificate(rand.Reader, ca, ca, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	ca, err = x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	leaf := &x509.Certificate{SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "control.example.test"}, NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour), IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, DNSNames: []string{"control.example.test"}, KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	cert, err := x509.CreateCertificate(rand.Reader, leaf, ca, &leafKey.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	private, err := x509.MarshalPKCS8PrivateKey(leafKey)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	certFile, keyFile := filepath.Join(dir, "certificate.pem"), filepath.Join(dir, "private-key.pem")
	if err = os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert}), 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: private}), 0600); err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(ca)
	return gateway.TLSOptions{CertificateFile: certFile, PrivateKeySecret: "ingress-key"}, map[string]string{"ingress-key": keyFile}, roots
}

func TestControlPlaneIngressTLSProxyPreservesRequestAndManagementBoundary(t *testing.T) {
	const host = "CONTROL.EXAMPLE.TEST.:443"
	const path = "/api/admin/storage/config%2Fentry?version=7&tag=first&tag=second"
	const body = `{"storage_id":42,"enabled":true}`
	var calls atomic.Int32
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		payload, err := io.ReadAll(r.Body)
		if err != nil || string(payload) != body || r.Method != http.MethodPost || r.Host != host || r.RequestURI != path {
			t.Errorf("request changed: method=%s host=%s target=%s body=%q error=%v", r.Method, r.Host, r.RequestURI, payload, err)
		}
		if r.Header.Get("Authorization") != "Bearer control-plane-session" || r.Header.Get("Cookie") != "session=control-plane-cookie" || r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("control-plane authentication or content type changed: %v", r.Header)
		}
		if r.Header.Get("Forwarded") != "" || r.Header.Get("X-Real-IP") != "" || r.Header.Get("X-Forwarded-Evil") != "" || r.Header.Get("X-Forwarded-For") != "127.0.0.1" || r.Header.Get("X-Forwarded-Proto") != "https" || r.Header.Get("X-Forwarded-Host") != host {
			t.Errorf("untrusted forwarding identity reached backend: %v", r.Header)
		}
		w.Header().Set("Set-Cookie", "session=renewed; Secure; HttpOnly")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte("control-plane accepted"))
	}))
	defer backend.Close()
	tlsOptions, secretFiles, roots := ingressCertificate(t)
	var err error
	runtime := &application{config: gateway.Config{Username: "operator", TLS: &tlsOptions, Worker: worker.Bootstrap{SecretFiles: secretFiles}, ControlPlane: &gateway.ControlPlaneRoute{Host: "control.example.test", Upstream: backend.URL}}}
	if err := runtime.prepareIngress(context.Background()); err != nil {
		t.Fatal(err)
	}
	runtime.Runtime, err = gateway.New(runtime.config, nil, []byte("management-password"))
	if err != nil {
		t.Fatal(err)
	}
	router := gin.New()
	gatewayhttpapi.RegisterGin(router, runtime.Runtime, http.NotFoundHandler(), runtime.proxy)
	runtime.handler = router
	defer runtime.transport.CloseIdleConnections()
	tlsConfig, err := runtime.TLSConfig(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	server, err := l7.NewServer(l7.ServerOptions{TLSConfig: tlsConfig, Handler: runtime.handler})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	if err = server.Start(listener); err != nil {
		listener.Close()
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			t.Error(err)
		}
	}()
	transport := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots}, ForceAttemptHTTP2: true}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 5 * time.Second}
	address := "https://" + listener.Addr().String()
	request, _ := http.NewRequest(http.MethodPost, address+path, strings.NewReader(body))
	request.Host = host
	request.Header.Set("Authorization", "Bearer control-plane-session")
	request.Header.Set("Cookie", "session=control-plane-cookie")
	request.Header.Set("Content-Type", "application/json")
	for _, name := range []string{"Forwarded", "X-Real-IP", "X-Forwarded-For", "X-Forwarded-Proto", "X-Forwarded-Host", "X-Forwarded-Evil"} {
		request.Header.Set(name, "spoof")
	}
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := io.ReadAll(response.Body)
	response.Body.Close()
	if err != nil || response.StatusCode != http.StatusCreated || string(payload) != "control-plane accepted" || response.Header.Get("Set-Cookie") != "session=renewed; Secure; HttpOnly" || response.ProtoMajor != 2 || response.TLS == nil {
		t.Fatalf("response changed: status=%d protocol=%s body=%q headers=%v error=%v", response.StatusCode, response.Proto, payload, response.Header, err)
	}
	for _, test := range []struct {
		host, path string
		status     int
	}{
		{"unknown.example.test", "/api/admin/storage", http.StatusMisdirectedRequest},
		{host, "/v1", http.StatusUnauthorized},
		{host, "/v1/state", http.StatusUnauthorized},
		{host, "/v1/manifest", http.StatusUnauthorized},
		{host, "/dav/files", http.StatusUnauthorized},
	} {
		request, _ := http.NewRequest(http.MethodGet, address+test.path, nil)
		request.Host = test.host
		request.Header.Set("Authorization", "Bearer control-plane-session")
		response, err := client.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, response.Body)
		response.Body.Close()
		if response.StatusCode != test.status {
			t.Errorf("host=%s path=%s status=%d, want %d", test.host, test.path, response.StatusCode, test.status)
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("management or unknown-host traffic leaked to control-plane backend: calls=%d", calls.Load())
	}
}

func TestControlPlaneIngressRejectsInvalidConfiguration(t *testing.T) {
	tlsOptions, secrets, _ := ingressCertificate(t)
	for _, upstream := range []string{"", "ftp://backend.test", "http://operator:password@backend.test", "http://backend.test/base", "http://backend.test/?query=value", "http://backend.test/#fragment", "http://backend.test:bad"} {
		runtime := &application{config: gateway.Config{TLS: &tlsOptions, Worker: worker.Bootstrap{SecretFiles: secrets}, ControlPlane: &gateway.ControlPlaneRoute{Host: "control.example.test", Upstream: upstream}}}
		if err := runtime.prepareIngress(context.Background()); err == nil {
			runtime.transport.CloseIdleConnections()
			t.Errorf("accepted invalid upstream %q", upstream)
		}
	}
	for _, options := range []*gateway.TLSOptions{nil, {}, {CertificateFile: tlsOptions.CertificateFile, PrivateKeySecret: "absent"}, {CertificateFile: filepath.Join(t.TempDir(), "absent.pem"), PrivateKeySecret: tlsOptions.PrivateKeySecret}} {
		runtime := &application{config: gateway.Config{TLS: options, Worker: worker.Bootstrap{SecretFiles: secrets}, ControlPlane: &gateway.ControlPlaneRoute{Host: "control.example.test", Upstream: "http://127.0.0.1:8080"}}}
		if err := runtime.prepareIngress(context.Background()); err == nil {
			runtime.transport.CloseIdleConnections()
			t.Error("accepted control-plane ingress without usable TLS material")
		}
	}
}
