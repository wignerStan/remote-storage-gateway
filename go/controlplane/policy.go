package controlplane

import (
	"encoding/json"
	"github.com/wignerStan/remote-storage-gateway/go/contracts/control/broker"
)

func copyConfig(c Config) (Config, error) {
	var out Config
	b, e := json.Marshal(c)
	if e != nil || len(b) > 1<<20 {
		return out, broker.ErrInvalid
	}
	if json.Unmarshal(b, &out) != nil {
		return out, broker.ErrInvalid
	}
	return out, nil
}

// resolve grants only an exact principal/authority/source/target and method.
// Ambiguous assignments fail closed rather than depending on file order.
func resolve(rules []Rule, in broker.TransferIntent) (broker.Decision, error) {
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
