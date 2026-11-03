package manager

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/wignerStan/infra/go/process/supervisor"
	cpconfig "github.com/wignerStan/remote-storage-gateway/go/contracts/control"
	"github.com/wignerStan/remote-storage-gateway/go/contracts/control/broker"
	"net"
	"net/http"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type ControlPlaneSpec struct {
	ID            string          `json:"id"`
	DataDirectory string          `json:"data_directory"`
	ConfigFile    string          `json:"config_file"`
	ReadyURL      string          `json:"ready_url"`
	Control       cpconfig.Config `json:"control"`
}
type controlPlane struct {
	spec ControlPlaneSpec

	payload []byte
	port    string
}

func newControlPlane(spec ControlPlaneSpec) (*controlPlane, error) {
	u, e := url.Parse(spec.ReadyURL)
	if e != nil || u.Scheme != "http" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Path != "/ping" || u.Opaque != "" {
		return nil, broker.ErrInvalid
	}
	host, port, e := net.SplitHostPort(u.Host)
	number, nerr := strconv.Atoi(port)
	if e != nil || nerr != nil || number < 1 || number > 65535 || host != "127.0.0.1" || spec.ID == "" || len(spec.ID) > 128 || !filepath.IsAbs(spec.DataDirectory) || !filepath.IsAbs(spec.ConfigFile) || strings.ContainsAny(spec.ID, "/\x00\r\n") {
		return nil, broker.ErrInvalid
	}
	payload, e := json.Marshal(spec.Control)
	if e != nil || len(payload) > 1<<20 {
		return nil, broker.ErrInvalid
	}
	return &controlPlane{spec: spec, payload: payload, port: port}, nil
}
func (p *controlPlane) processSpec() (supervisor.Spec, error) {
	command, e := supervisor.SelfCommand("control-plane", "server", "--data", p.spec.DataDirectory, "--config", p.spec.ConfigFile)
	if e != nil {
		return supervisor.Spec{}, e
	}
	command = command.WithStdin(bytes.NewReader(p.payload))
	for key, value := range proxyEnvironment() {
		command = command.WithExtraEnv(key, value)
	}
	for key, value := range map[string]string{"OPENLIST_GATEWAY_CONTROL_FILE": "-", "GATEWAY_SAME_HOST":"1", "OPENLIST_ADDR": "127.0.0.1", "OPENLIST_HTTP_PORT": p.port, "OPENLIST_HTTPS_PORT": "-1", "OPENLIST_FORCE_HTTPS": "false", "OPENLIST_ENABLE_H3": "false"} {
		command = command.WithExtraEnv(key, value)
	}
	client := &http.Client{Transport: &http.Transport{Proxy: nil}, Timeout: time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}

	return supervisor.Spec{Command: command, ReadyTimeout: time.Minute, Ready: func(ctx context.Context, _ *supervisor.Child) error {
		r, e := http.NewRequestWithContext(ctx, "GET", p.spec.ReadyURL, nil)
		if e != nil {
			return e
		}
		resp, e := client.Do(r)
		if e != nil {
			return broker.ErrUnavailable
		}
		defer resp.Body.Close()
		if resp.StatusCode != 200 {
			return broker.ErrUnavailable
		}
		return nil
	}}, nil
}

func (p *controlPlane) close(context.Context) error { clear(p.payload); return nil }
