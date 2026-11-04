// Package runtime assembles the independent materializer process.
package runtime

import (
	"bytes"
	"context"
	"github.com/gin-gonic/gin"
	"github.com/wignerStan/infra/go/config/codec"
	"github.com/wignerStan/infra/go/network/proxy/l7"
	"github.com/wignerStan/infra/go/storage/cloud"
	materialization "github.com/wignerStan/infra/go/storage/vfs"
	"github.com/wignerStan/network-data-plane-worker/go/secrets"
	"github.com/wignerStan/remote-storage-gateway/go/contracts/control/broker"
	config "github.com/wignerStan/remote-storage-gateway/go/contracts/vfs"
	control "github.com/wignerStan/remote-storage-gateway/go/control/ttrpc"
	service "github.com/wignerStan/remote-storage-gateway/go/vfs"
	"github.com/wignerStan/remote-storage-gateway/go/vfs/httpapi"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

func ReadConfig(input io.Reader) (config.ProcessConfig, error) {
	var cfg config.ProcessConfig
	if input == nil {
		return cfg, broker.ErrInvalid
	}
	data, err := io.ReadAll(io.LimitReader(input, (1<<20)+1))
	if err != nil || len(data) > 1<<20 || codec.DecodeJSON(data, &cfg) != nil {
		return cfg, broker.ErrInvalid
	}
	return cfg, nil
}
func privateDirectory(socket string) bool {
	if !filepath.IsAbs(socket) || len(socket) > 107 {
		return false
	}
	info, err := os.Stat(filepath.Dir(socket))
	return err == nil && info.IsDir() && info.Mode().Perm()&0077 == 0
}
func validate(c config.ProcessConfig) error {
	host, port, err := net.SplitHostPort(c.Listen)
	ip := net.ParseIP(host)
	number, e := strconv.Atoi(port)
	if err != nil || e != nil || ip == nil || !ip.IsLoopback() || number < 0 || number > 65535 {
		return broker.ErrInvalid
	}
	if c.InstanceID == "" || len(c.InstanceID) > 128 || c.WorkerInstanceID == "" || len(c.WorkerInstanceID) > 128 || c.LocalUsername == "" || len(c.LocalUsername) > 128 || c.DataUsername == "" || len(c.DataUsername) > 128 || !privateDirectory(c.ControlSocket) || !privateDirectory(c.WorkerControlSocket) || c.ControlSocket == c.WorkerControlSocket {
		return broker.ErrInvalid
	}
	if c.WorkerLocalDataSocket != "" && (!privateDirectory(c.WorkerLocalDataSocket) || c.WorkerLocalDataSocket == c.WorkerControlSocket || c.WorkerLocalDataSocket == c.ControlSocket) {
		return broker.ErrInvalid
	}
	if !filepath.IsAbs(c.LocalPasswordFile) || !filepath.IsAbs(c.DataPasswordFile) || filepath.Clean(c.LocalPasswordFile) == filepath.Clean(c.DataPasswordFile) || c.MaxAgeSeconds < 1 || c.MaxAgeSeconds > 31536000 {
		return broker.ErrInvalid
	}
	if materialization.ValidateOptions(&materialization.Options{Directory: c.Directory, MaxBytes: c.MaxBytes, MaxAge: time.Duration(c.MaxAgeSeconds) * time.Second}) != nil {
		return broker.ErrInvalid
	}
	target, err := url.Parse(c.DataEndpoint)
	if err != nil || (target.Scheme != "http" && target.Scheme != "https") || target.Host == "" || target.User != nil || target.RawQuery != "" || target.Fragment != "" || target.RawPath != "" || len(c.DataEndpoint) > 4096 {
		return broker.ErrInvalid
	}
	parts := strings.Split(strings.Trim(target.Path, "/"), "/")
	if len(parts) != 2 || parts[0] != "remote-dav" || parts[1] == "" || parts[1] == "." || parts[1] == ".." || strings.ContainsAny(parts[1], "\\\x00") || target.Path != "/remote-dav/"+parts[1]+"/" {
		return broker.ErrInvalid
	}
	return nil
}

func New(ctx context.Context, c config.ProcessConfig) (*Runtime, error) {
	if ctx == nil || validate(c) != nil {
		return nil, broker.ErrInvalid
	}
	origin, err := control.DialWorker(ctx, c.WorkerControlSocket)
	if err != nil {
		return nil, service.ErrUnavailable
	}
	ok := false
	defer func() {
		if !ok {
			_ = origin.Close(context.Background())
		}
	}()
	info, err := origin.Info(ctx)
	target, _ := url.Parse(c.DataEndpoint)
	base, e := url.Parse(info.DataEndpoint)
	if err != nil || e != nil || info.ID != c.WorkerInstanceID || !info.Ready || info.Contract != broker.Contract || info.LocalDataSocket != c.WorkerLocalDataSocket || base.User != nil || base.RawQuery != "" || base.Fragment != "" || (base.Path != "" && base.Path != "/") || base.Scheme != target.Scheme || !strings.EqualFold(base.Host, target.Host) {
		return nil, service.ErrUnavailable
	}
	resolver := secrets.FileResolver{"data": c.DataPasswordFile, "local": c.LocalPasswordFile}
	dataSecret, err := resolver.Resolve(ctx, "data")
	if err != nil {
		return nil, broker.ErrInvalid
	}
	defer dataSecret.Close()
	localSecret, err := resolver.Resolve(ctx, "local")
	if err != nil {
		return nil, broker.ErrInvalid
	}
	defer localSecret.Close()
	if len(dataSecret.Bytes()) < 16 || len(localSecret.Bytes()) < 16 || bytes.Equal(dataSecret.Bytes(), localSecret.Bytes()) {
		return nil, broker.ErrInvalid
	}
	parameters := map[string]string{"url": c.DataEndpoint, "user": c.DataUsername, "pass": string(dataSecret.Bytes()), "vendor": "other"}
	if c.WorkerLocalDataSocket != "" {
		// The observed public origin remains the identity fence. Local payloads
		// use the separately admitted raw HTTP socket, without weakening TLS.
		parameters["url"] = "http://localhost" + target.Path
		parameters["unix_socket"] = c.WorkerLocalDataSocket
	}
	store, err := cloud.New(ctx, cloud.Config{Backend: "webdav", Parameters: parameters, Materialization: &materialization.Options{Directory: c.Directory, MaxBytes: c.MaxBytes, MaxAge: time.Duration(c.MaxAgeSeconds) * time.Second}})
	if err != nil {
		return nil, service.ErrUnavailable
	}
	// Readiness proves the approved remote namespace is actually accessible;
	// control-socket liveness alone cannot admit a bad data credential.
	if _, err = store.List(ctx, ""); err != nil {
		_ = store.Close(context.Background())
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, service.ErrUnavailable
	}
	flow, err := service.New(c, store, origin.Info, localSecret.Bytes())
	if err != nil {
		_ = store.Close(context.Background())
		return nil, err
	}
	router := gin.New()
	httpapi.RegisterGin(router, flow)
	server, err := l7.NewServer(l7.ServerOptions{Handler: router, MaxHeaderBytes: 64 << 10})
	if err != nil {
		_ = flow.Close(context.Background())
		return nil, err
	}
	r := &Runtime{config: c, service: flow, origin: origin, http: server, done: make(chan struct{})}
	entry, err := control.NewVFSServer(r)
	if err != nil {
		_ = flow.Close(context.Background())
		return nil, err
	}
	r.control = entry
	ok = true
	return r, nil
}

func (r *Runtime) Start(ctx context.Context) error {
	if ctx == nil {
		return broker.ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.started || r.retiring {
		return broker.ErrUnavailable
	}
	if _, err := os.Lstat(r.config.ControlSocket); !os.IsNotExist(err) {
		return broker.ErrInvalid
	}
	metadata, err := net.Listen("unix", r.config.ControlSocket)
	if err != nil {
		return err
	}
	if err = os.Chmod(r.config.ControlSocket, 0600); err != nil {
		_ = metadata.Close()
		return err
	}
	listener, err := net.Listen("tcp", r.config.Listen)
	if err != nil {
		_ = metadata.Close()
		return err
	}
	if err = r.http.Start(listener); err != nil {
		_ = listener.Close()
		_ = metadata.Close()
		return err
	}
	r.endpoint = "http://" + listener.Addr().String() + "/dav/"
	r.started = true
	go func() { _ = r.control.Serve(metadata); _ = r.Close(context.Background()) }()
	go func() { <-r.http.Done(); _ = r.Close(context.Background()) }()
	return nil
}
