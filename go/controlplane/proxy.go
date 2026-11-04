package controlplane

import (
	config "github.com/wignerStan/infra/go/config"
	"github.com/wignerStan/remote-storage-gateway/go/worker"
)

// proxyEnvironment snapshots only operator transport settings. Presence is
// queried separately because EnvString intentionally omits explicit empties.
func proxyEnvironment() worker.ProxyEnvironment {
	result := worker.ProxyEnvironment{}
	for _, key := range []string{"HTTP_PROXY", "HTTPS_PROXY", "NO_PROXY", "http_proxy", "https_proxy", "no_proxy"} {
		if len(config.EnvPresentBool(key, key).Pairs()) == 0 {
			continue
		}
		result[key] = ""
		for _, pair := range config.EnvString(key, key).Pairs() {
			if text, ok := pair.Value.AsJSON().(string); ok {
				result[key] = text
			}
		}
	}
	return result
}
