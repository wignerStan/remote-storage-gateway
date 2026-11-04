// storage-vfs is an independent, Data-Plane-backed materializer application.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	runtime "github.com/wignerStan/remote-storage-gateway/go/vfs/runtime"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() error {
	path := flag.String("config", "", "private materializer JSON path, or - for structured stdin")
	flag.Parse()
	if *path == "" {
		return errors.New("-config is required")
	}
	var input io.Reader = os.Stdin
	if *path != "-" {
		file, err := os.Open(*path)
		if err != nil {
			return errors.New("materializer configuration unavailable")
		}
		defer file.Close()
		input = file
	}
	config, err := runtime.ReadConfig(input)
	if err != nil {
		return errors.New("invalid materializer configuration")
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	prepare, cancel := context.WithTimeout(ctx, time.Minute)
	app, err := runtime.New(prepare, config)
	cancel()
	if err != nil {
		return err
	}
	defer app.Close(context.Background())
	if err = app.Start(ctx); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
	case <-app.Done():
	}
	return app.Close(context.Background())
}
