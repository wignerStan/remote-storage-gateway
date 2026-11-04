// gateway-admin performs bounded operator maintenance without handling data-path
// requests. External processes cross Infra's process/exec boundary only.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 || args[0] != "tls-sync" {
		return fmt.Errorf("usage: gateway-admin tls-sync --domain <tailscale-DNS-name> [options]")
	}
	f := flag.NewFlagSet("tls-sync", flag.ContinueOnError)
	var o options
	f.StringVar(&o.config, "config", "/etc/storage-gateway/gateway.json", "private operator gateway configuration")
	f.StringVar(&o.secretsDir, "secrets-dir", "/etc/storage-gateway/secrets", "private directory containing configured secret basenames")
	f.StringVar(&o.stateDir, "state-dir", "/var/lib/storage-gateway", "private successful-publication state and lock directory")
	f.StringVar(&o.domain, "domain", "", "exact Tailscale certificate DNS name")
	f.StringVar(&o.instance, "instance", "k3s-cilium", "Incus Kubernetes instance")
	f.StringVar(&o.namespace, "namespace", "selfhost", "Kubernetes namespace")
	f.StringVar(&o.secret, "secret", "storage-gateway", "Kubernetes Secret name")
	f.StringVar(&o.deployment, "deployment", "storage-gateway", "Kubernetes Deployment name")
	if err := f.Parse(args[1:]); err != nil {
		return err
	}
	if f.NArg() != 0 {
		return fmt.Errorf("unexpected positional arguments")
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()
	changed, err := synchronize(ctx, o, execute)
	if err != nil {
		return err
	}
	if changed {
		fmt.Println("Gateway Secret applied and deployment rollout completed")
	} else {
		fmt.Println("Gateway TLS/config payload unchanged")
	}
	return nil
}
