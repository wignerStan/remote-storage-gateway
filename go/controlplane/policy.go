package controlplane

import (
	cpconfig "github.com/wignerStan/remote-storage-gateway/go/contracts/control"
	"github.com/wignerStan/remote-storage-gateway/go/contracts/control/broker"
)

// resolve grants only an exact principal/authority/source/target and method.
// Ambiguous assignments fail closed rather than depending on file order.
func resolve(rules []cpconfig.Rule, in broker.TransferIntent) (broker.Decision, error) {
	var grant broker.Decision
	found := false
	for _, r := range rules {
		if r.InstanceID != in.Principal || r.Authority != in.Authority || r.SourceID != in.SourceID || r.TargetID != in.TargetID {
			continue
		}
		if broker.ValidateDecision(in, r.Decision) != nil {
			continue
		}
		if found {
			return broker.Decision{}, broker.ErrDenied
		}
		found = true
		grant = r.Decision
	}
	if !found {
		return grant, broker.ErrDenied
	}
	grant.Methods = append([]string(nil), grant.Methods...)
	return grant, nil
}
