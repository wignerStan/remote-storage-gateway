// Package grpc binds the finite Gateway control protocol to authenticated gRPC.
// Only metadata crosses this port; payload streams remain in Worker plugins.
package grpc

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	cfg "github.com/wignerStan/remote-storage-gateway/go/contracts/control"
	"github.com/wignerStan/remote-storage-gateway/go/contracts/control/broker"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"io"
	"os"
	"strings"
	"time"
)

func tlsConfig(c cfg.TLSIdentity, server bool) (*tls.Config, error) {
	if c.CAFile == "" || c.CertificateFile == "" || c.PrivateKeyFile == "" {
		return nil, broker.ErrInvalid
	}
	f, e := os.Open(c.CAFile)
	if e != nil {
		return nil, broker.ErrInvalid
	}
	b, e := io.ReadAll(io.LimitReader(f, (1<<20)+1))
	f.Close()
	if e != nil || len(b) > 1<<20 {
		return nil, broker.ErrInvalid
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(b) {
		return nil, broker.ErrInvalid
	}
	cert, e := tls.LoadX509KeyPair(c.CertificateFile, c.PrivateKeyFile)
	if e != nil {
		return nil, broker.ErrInvalid
	}
	t := &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{cert}, RootCAs: roots, ServerName: c.ServerName}
	if !server {
		if !strings.HasPrefix(c.PeerURI, "spiffe://") || len(c.PeerURI) > 512 {
			return nil, broker.ErrInvalid
		}
		t.VerifyConnection = func(state tls.ConnectionState) error {
			if len(state.VerifiedChains) == 0 || len(state.PeerCertificates) == 0 {
				return errors.New("unverified remote workload")
			}
			for _, uri := range state.PeerCertificates[0].URIs {
				if uri.String() == c.PeerURI {
					return nil
				}
			}
			return errors.New("unexpected remote workload identity")
		}
	}
	if server {
		t.ClientCAs = roots
		t.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return t, nil
}

type principalKey struct{}

func principal(ctx context.Context) string { v, _ := ctx.Value(principalKey{}).(string); return v }
func authenticate(peers map[string]string) grpc.UnaryServerInterceptor {
	allowed := map[string]string{}
	for key, value := range peers {
		allowed[key] = value
	}
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
		p, ok := peer.FromContext(ctx)
		if !ok {
			return nil, status.Error(codes.Unauthenticated, "workload identity required")
		}
		auth, ok := p.AuthInfo.(credentials.TLSInfo)
		if !ok || len(auth.State.VerifiedChains) == 0 || len(auth.State.PeerCertificates) == 0 {
			return nil, status.Error(codes.Unauthenticated, "verified workload identity required")
		}
		id := ""
		for _, uri := range auth.State.PeerCertificates[0].URIs {
			if v := allowed[uri.String()]; v != "" {
				if id != "" && id != v {
					return nil, status.Error(codes.PermissionDenied, "ambiguous workload identity")
				}
				id = v
			}
		}
		if id == "" {
			return nil, status.Error(codes.PermissionDenied, "unassigned workload identity")
		}
		bounded, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		return next(context.WithValue(bounded, principalKey{}, id), req)
	}
}
func validateServer(c cfg.RPCServer) error {
	if len(c.Peers) == 0 || len(c.Peers) > 64 {
		return broker.ErrInvalid
	}
	for uri, id := range c.Peers {
		if !strings.HasPrefix(uri, "spiffe://") || len(uri) > 512 || id == "" || len(id) > 128 {
			return broker.ErrInvalid
		}
	}
	return nil
}
