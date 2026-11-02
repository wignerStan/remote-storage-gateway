//go:build dataplane_l7

package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/wignerStan/infra/go/network/proxy/l7"
	"net"
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
	config := flag.String("config", "", "private gateway config JSON")
	check := flag.Bool("check", false, "validate and prepare configured plugin clients without listening")
	flag.Parse()
	if *config == "" {
		return fmt.Errorf("-config is required")
	}
	c, e := loadConfig(*config)
	if e != nil {
		return fmt.Errorf("invalid gateway configuration")
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	initial, cancel := context.WithTimeout(ctx, 2*time.Minute)
	runtime, e := assemble(initial, c)
	cancel()
	if e != nil {
		return e
	}
	defer runtime.Close(context.Background())
	if *check {
		fmt.Println("Approved plugins and client candidates validated")
		return nil
	}
	tlsConfig, e := runtime.TLSConfig(ctx)
	if e != nil {
		return e
	}
	server, e := l7.NewServer(l7.ServerOptions{Handler: runtime.handler, TLSConfig: tlsConfig})
	if e != nil {
		return e
	}
	listener, e := net.Listen("tcp", c.Listen)
	if e != nil {
		return e
	}
	if e = server.Start(listener); e != nil {
		listener.Close()
		return e
	}
	fmt.Printf("Remote storage gateway listening on %s\n", listener.Addr())
	select {
	case <-ctx.Done():
	case <-server.Done():
	}
	// The listener stops accepting first. Native operations keep their generation
	// until HTTP drain and then Worker cleanup have actually completed.
	return server.Shutdown(context.Background())
}
