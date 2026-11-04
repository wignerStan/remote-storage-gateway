//go:build dataplane_l7

// Package entry dispatches the Worker role inside a standalone or bundled executable.
package entry

import (
	"context"
	"flag"
	"fmt"
	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/client"
	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/descriptor"
	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/encoded"
	dp "github.com/wignerStan/network-data-plane-worker/go/contracts/worker"
	"github.com/wignerStan/network-data-plane-worker/go/encodedruntime/nativebridge"
	"github.com/wignerStan/network-data-plane-worker/go/nativeclient"
	workerruntime "github.com/wignerStan/remote-storage-gateway/go/worker/runtime"
	"io"
	"os"
	"time"
)

func Run(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("worker", flag.ContinueOnError)
	path := flags.String("config", "", "private worker JSON path, or - for structured stdin")
	inspect := flags.String("inspect-artifact", "", "passive approved native artifact inspection")
	sha := flags.String("artifact-sha256", "", "approved artifact SHA256")
	plugin := flags.String("plugin-id", "", "expected native plugin ID")
	language := flags.String("language", "", "expected native language")
	contract := flags.String("contract", client.Contract, "expected Worker-owned contract")
	check := flags.Bool("check", false, "validate configured plugin candidates without listening")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if len(flags.Args()) != 0 {
		return fmt.Errorf("unexpected worker arguments")
	}
	if *inspect != "" {
		var record descriptor.Record
		var err error
		switch *contract {
		case client.Contract:
			record, err = nativeclient.Inspect(ctx, *inspect, *sha, *plugin, *language)
		case encoded.Contract:
			record, err = nativebridge.Inspect(ctx, dp.Artifact{Path: *inspect, SHA256: *sha, PluginID: *plugin, Language: *language})
		default:
			return fmt.Errorf("unsupported artifact contract")
		}
		if err != nil {
			return err
		}
		_, err = os.Stdout.Write(append(record.Bytes, '\n'))
		return err
	}
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
