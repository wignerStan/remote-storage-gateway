package pluginruntime

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/client"
	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/descriptor"
)

type secretResolverFunc func(context.Context, string) (client.Secret, error)

func (f secretResolverFunc) Resolve(ctx context.Context, ref string) (client.Secret, error) {
	return f(ctx, ref)
}

type trackedSecret struct {
	value  []byte
	closed bool
}

func (s *trackedSecret) Bytes() []byte { return s.value }
func (s *trackedSecret) Close()        { clear(s.value); s.closed = true }

type optionalSecretFactory struct {
	*fakeFactory
	published atomic.Int32
}

func (f *optionalSecretFactory) Descriptor() descriptor.Descriptor {
	d := f.fakeFactory.Descriptor()
	d.Implementations[0].ConfigSchema.Fields = []descriptor.Field{{Path: "token", Type: "string", Secret: true, WriteOnly: true}}
	return d
}
func (f *optionalSecretFactory) Prepare(ctx context.Context, cfg client.PluginConfig) (client.Candidate, error) {
	secret, err := cfg.Secrets.Resolve(ctx, "token")
	if err != nil && !errors.Is(err, client.ErrNotFound) {
		return nil, err
	}
	if secret != nil {
		secret.Close()
	}
	candidate, err := f.fakeFactory.Prepare(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &countedCandidate{Candidate: candidate, published: &f.published}, nil
}

type countedCandidate struct {
	client.Candidate
	published *atomic.Int32
}

func (c *countedCandidate) Publish() (client.Client, error) {
	instance, err := c.Candidate.Publish()
	if err == nil {
		c.published.Add(1)
	}
	return instance, err
}

func TestOptionalSecretAbsenceAndUnavailableExplicitReference(t *testing.T) {
	registry := NewRegistry()
	f := &optionalSecretFactory{fakeFactory: &fakeFactory{instances: make(chan *fakeClient, 8)}}
	if err := registry.Register(f); err != nil {
		t.Fatal(err)
	}
	var resolved atomic.Int32
	m, err := NewManager(registry, secretResolverFunc(func(context.Context, string) (client.Secret, error) {
		resolved.Add(1)
		return nil, fmt.Errorf("private resolver detail: %w", client.ErrNotFound)
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close(context.Background())
	s := Selection{Name: "one", PluginID: "test.client", DescriptorDigest: registry.Records()[0].Digest, ImplementationID: "fake", Public: []byte(`{}`)}
	version, err := m.Replace(context.Background(), m.Current(s.Name), s)
	if err != nil || f.published.Load() != 1 || resolved.Load() != 0 {
		t.Fatal("true optional absence did not publish without resolving", err)
	}
	old := <-f.instances
	s.SecretRefs = map[string]string{"token": "missing-operator-reference"}
	_, err = m.Replace(context.Background(), version, s)
	if !errors.Is(err, client.ErrProvider) || strings.Contains(err.Error(), "private") {
		t.Fatal("explicit unavailable reference did not fail safely", err)
	}
	if resolved.Load() != 1 || f.published.Load() != 1 || m.Current(s.Name) != version {
		t.Fatal("unavailable reference changed published generation")
	}
	select {
	case <-old.closed:
		t.Fatal("failed secret resolution closed the current generation")
	default:
	}
}

func TestScopedSecretsClosesPartialValuesAndRejectsMissingValues(t *testing.T) {
	implementation := descriptor.Implementation{ConfigSchema: descriptor.ConfigSchema{Fields: []descriptor.Field{{Path: "token", Secret: true}}}}
	for _, test := range []struct {
		name                   string
		sourceError, errorWant error
	}{
		{"missing", fmt.Errorf("private: %w", client.ErrNotFound), client.ErrInvalid},
		{"cancelled", context.Canceled, context.Canceled},
		{"failure", errors.New("private resolver failure"), nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			partial := &trackedSecret{value: []byte("private-credential")}
			s := scopedSecrets{a: implementation, refs: map[string]string{"token": "operator-ref"}, source: secretResolverFunc(func(context.Context, string) (client.Secret, error) { return partial, test.sourceError })}
			value, err := s.Resolve(context.Background(), "token")
			if value != nil || err == nil || !partial.closed || strings.Trim(string(partial.value), "\x00") != "" {
				t.Fatal("failed resolution leaked secret ownership")
			}
			if test.errorWant != nil && !errors.Is(err, test.errorWant) {
				t.Fatal("failure classification changed", err)
			}
		})
	}
	s := scopedSecrets{a: implementation, refs: map[string]string{"token": "operator-ref"}, source: secretResolverFunc(func(context.Context, string) (client.Secret, error) { return nil, nil })}
	if value, err := s.Resolve(context.Background(), "token"); value != nil || !errors.Is(err, client.ErrInvalid) {
		t.Fatal("nil successful credential accepted")
	}
	if value, err := s.Resolve(context.Background(), "undeclared"); value != nil || !errors.Is(err, client.ErrInvalid) {
		t.Fatal("undeclared field accepted")
	}
}
