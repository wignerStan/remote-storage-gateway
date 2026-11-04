//go:build dataplane_l7

package runtime

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"mime"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/encoded"
	encodedgrpc "github.com/wignerStan/network-data-plane-worker/go/encodedruntime/grpc"
	"github.com/wignerStan/network-data-plane-worker/go/encodedruntime/nativebridge"

	g "google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

type encodedOrigin struct {
	conn          *g.ClientConn
	authorization []byte
}
type encodedBridge struct {
	carrier *encodedgrpc.Handler
	runtime *nativebridge.Runtime
	broker  decisionClient
	origins map[string]*encodedOrigin
	once    sync.Once
	done    chan struct{}
	err     error
}

func (r *Runtime) prepareEncoded(ctx context.Context) (err error) {
	cfg := r.config.Encoded
	if cfg == nil {
		return nil
	}
	if r.tlsConfig == nil || r.process.BrokerSocket == "" && r.process.RemoteBroker == nil || cfg.SourceID == "" || cfg.TargetID == "" || len(cfg.Upstreams) == 0 || len(cfg.Upstreams) > 32 {
		return errors.New("invalid encoded routing configuration")
	}
	bridge := &encodedBridge{origins: map[string]*encodedOrigin{}, done: make(chan struct{})}
	defer func() {
		if err != nil {
			_ = bridge.close(context.Background())
		}
	}()
	bridge.runtime, err = nativebridge.New(ctx, cfg.Artifact)
	if err != nil {
		return errors.New("approved encoded plugin unavailable")
	}
	bridge.broker, err = r.decisionClient(ctx, cfg.MetadataNames)
	if err != nil {
		return errors.New("decision broker unavailable")
	}
	authorize, err := NewAuthorizer(bridge.broker, cfg.MetadataNames)
	if err != nil {
		return err
	}
	for _, up := range cfg.Upstreams {
		host, port, e := net.SplitHostPort(up.Address)
		number, pErr := strconv.Atoi(port)
		if e != nil || pErr != nil || number < 1 || number > 65535 || host == "" || len(up.Address) > 512 || strings.ContainsAny(up.Address, "/\\\x00\n\r") || up.ID == "" || len(up.ID) > 128 || bridge.origins[up.ID] != nil || !up.TLS && (up.AuthSecret != "" || up.CAFile != "" || up.ServerName != "") {
			return errors.New("invalid approved encoded upstream")
		}
		var transport credentials.TransportCredentials = insecure.NewCredentials()
		if up.TLS {
			config := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: up.ServerName}
			if config.ServerName == "" {
				config.ServerName = host
			}
			if up.CAFile != "" {
				file, e := os.Open(up.CAFile)
				if e != nil {
					return errors.New("upstream CA unavailable")
				}
				pem, e := io.ReadAll(io.LimitReader(file, 1<<20+1))
				_ = file.Close()
				if e != nil || len(pem) > 1<<20 {
					return errors.New("invalid upstream CA")
				}
				pool := x509.NewCertPool()
				if !pool.AppendCertsFromPEM(pem) {
					return errors.New("invalid upstream CA")
				}
				config.RootCAs = pool
			}
			transport = credentials.NewTLS(config)
		}
		origin := &encodedOrigin{}
		bridge.origins[up.ID] = origin
		if up.AuthSecret != "" {
			lease, e := r.providerSecrets.Resolve(ctx, up.AuthSecret)
			if e != nil {
				if lease != nil {
					lease.Close()
				}
				return errors.New("upstream credential unavailable")
			}
			if lease == nil {
				return errors.New("upstream credential unavailable")
			}
			token := lease.Bytes()
			valid := len(token) > 0 && len(token) <= 8192
			for _, b := range token {
				if b < 32 || b > 126 {
					valid = false
				}
			}
			if valid {
				origin.authorization = append([]byte(nil), token...)
			}
			lease.Close()
			if !valid {
				return errors.New("invalid upstream credential")
			}
		}
		origin.conn, err = g.NewClient("passthrough:///"+up.Address, g.WithTransportCredentials(transport), g.WithMaxHeaderListSize(32<<10))
		if err != nil {
			return errors.New("upstream transport unavailable")
		}
	}
	bridge.carrier, err = encodedgrpc.New(encodedgrpc.Config{Runtime: bridge.runtime, MaxSessions: cfg.MaxSessions, MaxAggregateBytes: cfg.MaxAggregateBytes, Authorize: func(ctx context.Context, request encoded.RequestMetadata) (encoded.Grant, error) {
		request.SourceID = cfg.SourceID
		request.TargetID = cfg.TargetID
		return authorize(ctx, request)
	}, OpenUpstream: func(ctx context.Context, id, method string, shape encoded.Shape) (g.ClientStream, error) {
		origin := bridge.origins[id]
		if origin == nil {
			return nil, errors.New("unapproved upstream")
		}
		if len(origin.authorization) > 0 {
			ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("authorization", string(origin.authorization)))
		}
		return origin.conn.NewStream(ctx, &g.StreamDesc{ClientStreams: shape.ClientStreaming, ServerStreams: shape.ServerStreaming}, method, g.ForceCodecV2(encodedgrpc.RawCodec()), g.MaxCallRecvMsgSize(64<<20), g.MaxCallSendMsgSize(64<<20))
	}})
	if err != nil {
		return err
	}
	previous := r.handler
	r.handler = http.HandlerFunc(func(w http.ResponseWriter, q *http.Request) {
		contentType, _, _ := mime.ParseMediaType(q.Header.Get("Content-Type"))
		if q.ProtoMajor == 2 && (contentType == "application/grpc" || strings.HasPrefix(contentType, "application/grpc+")) {
			username, password, ok := q.BasicAuth()
			if !ok || !r.Runtime.Authorize(username, password) {
				w.Header().Set("Content-Type", "application/grpc")
				w.Header().Set("Grpc-Status", "16")
				w.Header().Set("Grpc-Message", "authentication required")
				w.WriteHeader(http.StatusOK)
				return
			}
			bridge.carrier.ServeHTTP(w, q)
			return
		}
		previous.ServeHTTP(w, q)
	})
	r.encoded = bridge
	return nil
}
func (r *Runtime) closeEncoded(ctx context.Context) error {
	if r.encoded == nil {
		return nil
	}
	return r.encoded.close(ctx)
}
func (b *encodedBridge) close(ctx context.Context) error {
	b.once.Do(func() {
		go func() {
			var failures []error
			if b.carrier != nil {
				failures = append(failures, b.carrier.Close(context.Background()))
			}
			if b.runtime != nil {
				failures = append(failures, b.runtime.Close(context.Background()))
			}
			for _, origin := range b.origins {
				if origin.conn != nil {
					failures = append(failures, origin.conn.Close())
				}
				clear(origin.authorization)
			}
			if b.broker != nil {
				failures = append(failures, b.broker.Close(context.Background()))
			}
			b.err = errors.Join(failures...)
			close(b.done)
		}()
	})
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-b.done:
		return b.err
	}
}
