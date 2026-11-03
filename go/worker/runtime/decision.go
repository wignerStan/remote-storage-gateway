//go:build dataplane_l7

package runtime

import (
	"context"
	"github.com/wignerStan/remote-storage-gateway/go/contracts/control/broker"
	controlgrpc "github.com/wignerStan/remote-storage-gateway/go/control/grpc"
	control "github.com/wignerStan/remote-storage-gateway/go/control/ttrpc"
)

type decisionClient interface {
	broker.DecisionBroker
	Close(context.Context) error
}

func (r *Runtime) decisionClient(ctx context.Context, names []string) (decisionClient, error) {
	if r.process.RemoteBroker != nil {
		return controlgrpc.DialDecision(*r.process.RemoteBroker, names)
	}
	return control.DialDecision(ctx, r.process.BrokerSocket, names)
}
