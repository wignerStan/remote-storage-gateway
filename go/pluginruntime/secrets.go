package pluginruntime

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/client"
	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/descriptor"
)

func descriptorConfig(a descriptor.Implementation, p json.RawMessage, s map[string]string) error {
	return descriptor.ValidateConfig(a, p, s)
}

type scopedSecrets struct {
	a      descriptor.Implementation
	refs   map[string]string
	source client.SecretResolver
}

func (s scopedSecrets) Resolve(ctx context.Context, name string) (client.Secret, error) {
	declared := false
	for _, f := range s.a.ConfigSchema.Fields {
		if f.Path == name && f.Secret {
			declared = true
			break
		}
	}
	if !declared {
		return nil, client.ErrInvalid
	}
	ref, ok := s.refs[name]
	if !ok {
		return nil, client.ErrNotFound
	}
	if s.source == nil {
		return nil, client.ErrInvalid
	}
	value, err := s.source.Resolve(ctx, ref)
	if err != nil {
		// A failed resolver must not transfer a partially resolved credential.
		if value != nil {
			value.Close()
		}
		// Only an absent field is optional. A mapped but unavailable operator
		// reference must fail closed, including wrapped ErrNotFound values.
		if errors.Is(err, client.ErrNotFound) {
			return nil, client.ErrInvalid
		}
		return nil, err
	}
	if value == nil {
		return nil, client.ErrInvalid
	}
	return value, nil
}
