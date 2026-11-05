package manager

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	processexec "github.com/wignerStan/infra/go/process/exec"
	workerconfig "github.com/wignerStan/remote-storage-gateway/go/contracts/worker"
	"github.com/wignerStan/remote-storage-gateway/go/worker"
)

type childEnvironmentValue struct {
	Value   string
	Present bool
}

var childEnvironmentKeys = []string{
	"PATH", "HTTP_PROXY", "HTTPS_PROXY", "GATEWAY_PRIVATE_SECRET",
	"OPENLIST_CONTROL_TARGETS_FILE", "OPENLIST_GATEWAY_CONTROL_FILE", "GATEWAY_SAME_HOST",
	"OPENLIST_ADDR", "OPENLIST_HTTP_PORT", "OPENLIST_HTTPS_PORT", "OPENLIST_FORCE_HTTPS", "OPENLIST_ENABLE_H3",
}

func TestManagerChildEnvironmentProbe(t *testing.T) {
	if len(os.Args) < 2 || os.Args[len(os.Args)-1] != "--gateway-child-environment-probe" {
		return
	}
	observed := map[string]childEnvironmentValue{}
	for _, key := range childEnvironmentKeys {
		value, present := os.LookupEnv(key)
		observed[key] = childEnvironmentValue{value, present}
	}
	if err := json.NewEncoder(os.Stdout).Encode(observed); err != nil {
		os.Exit(2)
	}
	os.Exit(0)
}

func observeChildEnvironment(t *testing.T, command processexec.StartSpec) map[string]childEnvironmentValue {
	t.Helper()
	// Run the real admitted environment through Infra's process launcher. Only
	// the role argv changes so this test binary reports the child's environment.
	command.Args = []string{"-test.run=^TestManagerChildEnvironmentProbe$", "--", "--gateway-child-environment-probe"}
	var stdout, stderr bytes.Buffer
	child, err := processexec.Start(command.WithStdout(&stdout).WithStderr(&stderr))
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- child.Wait() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("child environment probe failed: %v: %s", err, stderr.String())
		}
	case <-time.After(10 * time.Second):
		_ = child.Kill()
		<-done
		t.Fatal("child environment probe timed out")
	}
	var observed map[string]childEnvironmentValue
	if err := json.Unmarshal(stdout.Bytes(), &observed); err != nil {
		t.Fatalf("child did not report its environment: %v: %s", err, stdout.String())
	}
	return observed
}

func TestControlPlaneProcessEnvironment(t *testing.T) {
	for _, key := range childEnvironmentKeys {
		t.Setenv(key, "unapproved-parent-value")
	}
	t.Setenv("PATH", "/parent-fixture-path")
	t.Setenv("HTTP_PROXY", "http://fixture.invalid:3128")
	t.Setenv("HTTPS_PROXY", "")
	base := map[string]childEnvironmentValue{
		"PATH":                          {"/parent-fixture-path", true},
		"HTTP_PROXY":                    {"http://fixture.invalid:3128", true},
		"HTTPS_PROXY":                   {"", true},
		"OPENLIST_GATEWAY_CONTROL_FILE": {"-", true},
		"GATEWAY_SAME_HOST":             {"1", true},
		"OPENLIST_ADDR":                 {"127.0.0.1", true},
		"OPENLIST_HTTP_PORT":            {"5244", true},
		"OPENLIST_HTTPS_PORT":           {"-1", true},
		"OPENLIST_FORCE_HTTPS":          {"false", true},
		"OPENLIST_ENABLE_H3":            {"false", true},
	}
	for _, tc := range []struct {
		name    string
		targets childEnvironmentValue
	}{
		{"targets-file", childEnvironmentValue{"/run/control-secrets/targets.json", true}},
		{"explicit-empty", childEnvironmentValue{"", true}},
		{"unset", childEnvironmentValue{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("OPENLIST_CONTROL_TARGETS_FILE", tc.targets.Value)
			if !tc.targets.Present {
				if err := os.Unsetenv("OPENLIST_CONTROL_TARGETS_FILE"); err != nil {
					t.Fatal(err)
				}
			}
			plane, err := newControlPlane(ControlPlaneSpec{
				ID: "environment-probe", DataDirectory: "/unused-environment-probe", ConfigFile: "/unused-environment-probe/config.json", ReadyURL: "http://127.0.0.1:5244/ping",
			})
			if err != nil {
				t.Fatal(err)
			}
			defer plane.close(context.Background())
			spec, err := plane.processSpec()
			if err != nil {
				t.Fatal(err)
			}
			observed := observeChildEnvironment(t, spec.Command)
			for _, key := range childEnvironmentKeys {
				want := base[key]
				if key == "OPENLIST_CONTROL_TARGETS_FILE" {
					want = tc.targets
				}
				if got := observed[key]; got != want {
					t.Errorf("child %s = %#v, want %#v", key, got, want)
				}
			}
		})
	}
	t.Run("worker-isolation", func(t *testing.T) {
		t.Setenv("OPENLIST_CONTROL_TARGETS_FILE", "/run/control-secrets/targets.json")
		dir, err := os.MkdirTemp("/tmp", "gateway-env-")
		if err != nil {
			t.Fatal(err)
		}
		defer os.RemoveAll(dir)
		executable, err := os.Executable()
		if err != nil {
			t.Fatal(err)
		}
		box, err := worker.New(worker.Options{
			Executable: executable, ProxyEnvironment: proxyEnvironment(),
			Config: workerconfig.ProcessConfig{InstanceID: "environment-probe", ControlSocket: filepath.Join(dir, "control")},
		})
		if err != nil {
			t.Fatal(err)
		}
		defer box.Close(context.Background())
		spec, err := box.ProcessSpec()
		if err != nil {
			t.Fatal(err)
		}
		observed := observeChildEnvironment(t, spec.Command)
		for _, key := range childEnvironmentKeys {
			want := childEnvironmentValue{}
			if key == "PATH" || key == "HTTP_PROXY" || key == "HTTPS_PROXY" {
				want = base[key]
			}
			if got := observed[key]; got != want {
				t.Errorf("worker child %s = %#v, want %#v", key, got, want)
			}
		}
	})
}
