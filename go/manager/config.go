package manager

import (
	"encoding/json"
	"github.com/wignerStan/remote-storage-gateway/go/contracts/control/broker"
	"path/filepath"
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

// validateAssignments checks the product wiring before a child can be started.
// A bundle with no control planes may still be supervised by an external CP.
// Within a complete bundle, every local Worker belongs to exactly one CP.
func validateAssignments(c Config) error {
	workers := make(map[string]WorkerSpec, len(c.Workers))
	for _, w := range c.Workers {
		workers[w.Config.InstanceID] = w
	}
	assigned := make(map[string]bool, len(workers))
	dataDirectories := make(map[string]bool, len(c.ControlPlanes))
	readyURLs := make(map[string]bool, len(c.ControlPlanes))
	for _, plane := range c.ControlPlanes {
		// SQLite has one writer process per data directory. Reject lexical aliases
		// as well; runtime storage permissions remain the operator's boundary.
		if filepath.Clean(plane.DataDirectory) != plane.DataDirectory || filepath.Clean(plane.ConfigFile) != plane.ConfigFile || dataDirectories[plane.DataDirectory] || readyURLs[plane.ReadyURL] {
			return broker.ErrInvalid
		}
		dataDirectories[plane.DataDirectory] = true
		readyURLs[plane.ReadyURL] = true
		for _, endpoint := range plane.Control.Workers {
			if assigned[endpoint.ID] {
				return broker.ErrInvalid
			}
			assigned[endpoint.ID] = true
			local, ok := workers[endpoint.ID]
			if !ok {
				if endpoint.Remote == nil {
					return broker.ErrInvalid
				}
				continue
			}
			if endpoint.Remote != nil || endpoint.ControlSocket != local.Config.ControlSocket || endpoint.BrokerSocket != local.Config.BrokerSocket || endpoint.DataEndpoint != local.Config.DataEndpoint {
				return broker.ErrInvalid
			}
		}
	}
	if len(c.ControlPlanes) != 0 {
		for id := range workers {
			if !assigned[id] {
				return broker.ErrInvalid
			}
		}
	}
	return nil
}
