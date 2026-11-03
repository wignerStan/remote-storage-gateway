package manager

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
