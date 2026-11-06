package manager

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"
)

func Run(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("manager", flag.ContinueOnError)
	path := flags.String("config", "", "private bundle configuration file")
	check := flags.Bool("check-config", false, "validate bundle assignments without starting processes")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *path == "" || len(flags.Args()) != 0 {
		return fmt.Errorf("manager requires --config")
	}
	m, err := Load(*path)
	if err != nil {
		return fmt.Errorf("invalid bundle configuration")
	}
	defer m.Close(context.Background())
	if len(m.config.ControlPlanes) == 0 || len(m.config.Workers) == 0 {
		return fmt.Errorf("bundle requires independent control-plane and worker roles")
	}
	if *check {
		return nil
	}
	startup, cancel := context.WithTimeout(ctx, 2*time.Minute)
	err = m.Start(startup)
	cancel()
	if err != nil {
		return err
	}
	// Publish only after the manager has completed every role's startup and
	// materializer activation. A child's open listener is an earlier lifecycle
	// phase and cannot stand in for the bundle's own readiness.
	if _, err := fmt.Fprintln(os.Stdout, "storage-gateway manager ready"); err != nil {
		return fmt.Errorf("publish manager readiness: %w", err)
	}
	// Any failed role retires the bundle; the host/container owner restarts it.
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return m.Close(context.Background())
		case <-tick.C:
			if !m.Ready() {
				return fmt.Errorf("bundle child stopped")
			}
		}
	}
}
