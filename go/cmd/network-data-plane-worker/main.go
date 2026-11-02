package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/wignerStan/network-data-plane-worker/go/nativeclient"
	"github.com/wignerStan/network-data-plane-worker/go/pluginruntime"
	"github.com/wignerStan/network-data-plane-worker/go/worker"
	"os"
	"time"
)

func main() {
	if e := run(); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
func run() error {
	f := flag.NewFlagSet("network-data-plane-worker", flag.ContinueOnError)
	print := f.Bool("print-capabilities", false, "report approved installed artifact capabilities without constructing providers")
	check := f.String("check-config", "", "validate bootstrap structure and approved descriptors without provider sessions")
	config := f.String("config", "", "operator bootstrap for capability reporting")
	inspect := f.String("inspect-artifact", "", "passively inspect an installed hash-approved .so")
	sha := f.String("artifact-sha256", "", "approved artifact SHA256")
	plugin := f.String("plugin-id", "", "expected native plugin identity")
	language := f.String("language", "", "expected go or rust native language")
	if e := f.Parse(os.Args[1:]); e != nil {
		return e
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if *inspect != "" {
		r, e := nativeclient.Inspect(ctx, *inspect, *sha, *plugin, *language)
		if e != nil {
			return e
		}
		_, e = os.Stdout.Write(append(r.Bytes, '\n'))
		return e
	}
	path := *config
	if *check != "" {
		path = *check
	}
	registry := pluginruntime.NewRegistry()
	if path != "" {
		c, e := worker.ReadBootstrap(path)
		if e != nil {
			return e
		}
		registry, e = worker.Discover(ctx, c)
		if e != nil {
			return e
		}
		if e = worker.Validate(c, registry); e != nil {
			return e
		}
	}
	if *check != "" {
		fmt.Fprintln(os.Stdout, "Configuration and exact descriptor references accepted; provider sessions were not constructed.")
		return nil
	}
	if *print {
		m, e := worker.Capabilities(registry)
		if e != nil {
			return e
		}
		return worker.WriteJSON(os.Stdout, m)
	}
	return fmt.Errorf("use --print-capabilities, --check-config <file>, or --inspect-artifact; run a product composition for data listeners")
}
