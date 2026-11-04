//go:build dataplane_l7

// storage-worker is the independently supervised data-plane application.
package main

import (
	"context"
	"flag"
	"fmt"
	workerruntime "github.com/wignerStan/remote-storage-gateway/go/worker/runtime"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	if e := run(); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
func run() error {
	path := flag.String("config", "", "private worker JSON path, or - for structured stdin")
	check := flag.Bool("check", false, "validate configured plugin candidates without listening")
	flag.Parse()
	if *path == "" {
		return fmt.Errorf("-config is required")
	}
	var input io.Reader = os.Stdin
	if *path != "-" {
		file, e := os.Open(*path)
		if e != nil {
			return fmt.Errorf("worker configuration unavailable")
		}
		defer file.Close()
		input = file
	}
	cfg, e := workerruntime.ReadConfig(input)
	if e != nil {
		return fmt.Errorf("invalid worker configuration")
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	initial, cancel := context.WithTimeout(ctx, 2*time.Minute)
	runtime, e := workerruntime.New(initial, cfg)
	cancel()
	if e != nil {
		return e
	}
	defer runtime.Close(context.Background())
	if *check {
		return nil
	}
	if e = runtime.Start(ctx); e != nil {
		return e
	}
	select {
	case <-ctx.Done():
	case <-runtime.Done():
	}
	return runtime.Close(context.Background())
}
