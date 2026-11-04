//go:build !infra_grpc

package grpc

import (
	"context"
	cfg "github.com/wignerStan/remote-storage-gateway/go/contracts/control"
	"github.com/wignerStan/remote-storage-gateway/go/contracts/control/broker"
)

type Server struct{}

func StartServer(context.Context, cfg.RPCServer, broker.WorkerControl, broker.DecisionBroker, []string) (*Server, error) {
	return nil, broker.ErrUnavailable
}
func (*Server) Done() <-chan struct{}       { return nil }
func (*Server) Close(context.Context) error { return nil }
