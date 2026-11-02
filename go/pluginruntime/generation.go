package pluginruntime

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	lifecycle "github.com/wignerStan/infra/go/runtime/pluginlifecycle"
	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/client"
	workercontract "github.com/wignerStan/network-data-plane-worker/go/contracts/worker"
	"io"
	"sync"
)

type Selection = workercontract.Selection
type Version = workercontract.Version
type generation struct {
	version   Version
	selection Selection
	runtime   *lifecycle.Runtime
	provider  lifecycle.Provider[client.Client]
	done      chan struct{}
	err       error
}
type Manager struct {
	mu          sync.Mutex
	registry    *Registry
	resolver    client.SecretResolver
	incarnation string
	sequence    uint64
	current     map[string]*generation
	all         map[*generation]struct{}
	closed      bool
	done        chan struct{}
	outcome     error
}

func NewManager(r *Registry, resolver client.SecretResolver) (*Manager, error) {
	if r == nil {
		return nil, errors.New("registry required")
	}
	b := make([]byte, 16)
	if _, e := rand.Read(b); e != nil {
		return nil, e
	}
	return &Manager{registry: r, resolver: resolver, incarnation: hex.EncodeToString(b), current: map[string]*generation{}, all: map[*generation]struct{}{}}, nil
}
func (m *Manager) Current(name string) Version {
	m.mu.Lock()
	defer m.mu.Unlock()
	if g := m.current[name]; g != nil {
		return g.version
	}
	return Version{Incarnation: m.incarnation}
}
func (m *Manager) Replace(ctx context.Context, expected Version, s Selection) (Version, error) {
	if s.Name == "" || len(s.Name) > 128 {
		return Version{}, errors.New("invalid client name")
	}
	reg, a, e := m.registry.resolve(s)
	if e != nil {
		return Version{}, e
	}
	if e = descriptorConfig(a, s.Public, s.SecretRefs); e != nil {
		return Version{}, e
	}
	m.mu.Lock()
	v := Version{Incarnation: m.incarnation}
	if g := m.current[s.Name]; g != nil {
		v = g.version
	}
	if m.closed || expected != v {
		m.mu.Unlock()
		return Version{}, errors.New("stale or closed worker generation")
	}
	if m.sequence == ^uint64(0) {
		m.mu.Unlock()
		return Version{}, errors.New("generation namespace exhausted")
	}
	m.sequence++
	reservedGeneration := m.sequence
	m.mu.Unlock()
	cfg := client.PluginConfig{ImplementationID: s.ImplementationID, Public: s.Public, Secrets: scopedSecrets{a: a, refs: s.SecretRefs, source: m.resolver}}
	candidate, e := reg.factory.Prepare(ctx, cfg)
	if e != nil {
		return Version{}, safeError(e)
	}
	if candidate == nil {
		return Version{}, errors.New("nil candidate")
	}
	published := false
	defer func() {
		if !published {
			_ = candidate.Abort(context.Background())
		}
	}()
	if e = candidate.Validate(ctx); e != nil {
		return Version{}, safeError(e)
	}
	resource := lifecycle.Resource{ID: "client", Provides: []string{client.Contract}}
	schema, _ := lifecycle.NewResourceSchema([]lifecycle.Resource{resource})
	composition, e := lifecycle.NewComposition(schema)
	if e != nil {
		return Version{}, e
	}
	defer composition.Rollback(context.Background())
	port, e := lifecycle.Install(ctx, composition, resource, func(context.Context) (client.Client, lifecycle.Reversal, error) {
		c, e := candidate.Publish()
		if e != nil {
			return nil, nil, safeError(e)
		}
		if c == nil {
			return nil, nil, errors.New("nil client")
		}
		published = true
		return c, func() error { return c.Close(context.Background()) }, nil
	})
	if e != nil {
		return Version{}, e
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	now := Version{Incarnation: m.incarnation}
	old := m.current[s.Name]
	if old != nil {
		now = old.version
	}
	if m.closed || now != expected {
		return Version{}, errors.New("generation changed during prepare")
	}
	runtime, e := composition.Commit(ctx)
	if e != nil {
		return Version{}, e
	}
	version := Version{Incarnation: m.incarnation, Generation: reservedGeneration, DescriptorDigest: s.DescriptorDigest, ArtifactDigest: reg.artifactDigest}
	g := &generation{version: version, selection: s, runtime: runtime, provider: port, done: make(chan struct{})}
	m.current[s.Name] = g
	m.all[g] = struct{}{}
	if old != nil {
		m.retireLocked(old)
	}
	return version, nil
}
func safeError(e error) error {
	if errors.Is(e, context.Canceled) || errors.Is(e, context.DeadlineExceeded) {
		return e
	}
	return client.ErrProvider
}
func (m *Manager) retireLocked(g *generation) {
	select {
	case <-g.done:
		return
	default:
	}
	go func() {
		e := g.runtime.Shutdown(context.Background())
		m.mu.Lock()
		g.err = e
		close(g.done)
		if e == nil {
			delete(m.all, g)
		}
		m.mu.Unlock()
	}()
}

type Lease struct {
	version Version
	call    *lifecycle.Invocation[client.Client]
}

func (l *Lease) Client() client.Client { return l.call.Value() }
func (l *Lease) Version() Version      { return l.version }
func (l *Lease) Release() {
	if l != nil && l.call != nil {
		l.call.Release()
	}
}
func (m *Manager) Pin(name string) (*Lease, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	g := m.current[name]
	if m.closed || g == nil {
		return nil, errors.New("client unavailable")
	}
	call, e := g.provider.Enter()
	if e != nil {
		return nil, e
	}
	return &Lease{g.version, call}, nil
}
func (m *Manager) Close(ctx context.Context) error {
	m.mu.Lock()
	if m.done == nil {
		m.closed = true
		m.done = make(chan struct{})
		gens := make([]*generation, 0, len(m.all))
		for g := range m.all {
			gens = append(gens, g)
		}
		go func() {
			var outcome error
			for _, g := range gens {
				outcome = errors.Join(outcome, g.runtime.Shutdown(context.Background()))
			}
			m.mu.Lock()
			m.outcome = outcome
			close(m.done)
			m.mu.Unlock()
		}()
	}
	done := m.done
	m.mu.Unlock()
	select {
	case <-done:
		return m.outcome
	case <-ctx.Done():
		return ctx.Err()
	}
}

type Observed = workercontract.Observed

func (m *Manager) Observed() []Observed {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := []Observed{}
	for name, g := range m.current {
		n := 0
		for old := range m.all {
			if old != g && old.selection.Name == name {
				n++
			}
		}
		out = append(out, Observed{Name: name, Version: g.version, PluginID: g.selection.PluginID, ImplementationID: g.selection.ImplementationID, Accepting: !m.closed, Draining: n})
	}
	return out
}

// Client returns a per-operation adapter. A protocol exchange needing multiple
// calls on one generation must use Pin and retain its Lease through completion.
func (m *Manager) Client(name string) client.Client { return &boundClient{m, name} }

type boundClient struct {
	m    *Manager
	name string
}

func (b *boundClient) Stat(ctx context.Context, r client.ObjectRef) (client.ObjectInfo, error) {
	l, e := b.m.Pin(b.name)
	if e != nil {
		return client.ObjectInfo{}, e
	}
	defer l.Release()
	return l.Client().Stat(ctx, r)
}
func (b *boundClient) List(ctx context.Context, r client.ListRequest) (client.ListResult, error) {
	l, e := b.m.Pin(b.name)
	if e != nil {
		return client.ListResult{}, e
	}
	defer l.Release()
	return l.Client().List(ctx, r)
}
func (b *boundClient) Put(ctx context.Context, r client.PutRequest) (client.PutResult, error) {
	l, e := b.m.Pin(b.name)
	if e != nil {
		return client.PutResult{}, e
	}
	defer l.Release()
	return l.Client().Put(ctx, r)
}
func (b *boundClient) Delete(ctx context.Context, r client.ObjectRef) error {
	l, e := b.m.Pin(b.name)
	if e != nil {
		return e
	}
	defer l.Release()
	return l.Client().Delete(ctx, r)
}
func (b *boundClient) Close(context.Context) error { return nil }
func (b *boundClient) Open(ctx context.Context, r client.OpenRequest) (client.ReadPlan, error) {
	l, e := b.m.Pin(b.name)
	if e != nil {
		return client.ReadPlan{}, e
	}
	p, e := l.Client().Open(ctx, r)
	if e != nil {
		l.Release()
		return p, e
	}
	if p.Body == nil {
		l.Release()
		return client.ReadPlan{}, errors.New("nil read body")
	}
	p.Body = &leasedReader{ReadCloser: p.Body, release: l.Release}
	return p, nil
}

type leasedReader struct {
	io.ReadCloser
	release func()
	once    sync.Once
	err     error
}

func (r *leasedReader) Close() error {
	r.once.Do(func() { r.err = r.ReadCloser.Close(); r.release() })
	return r.err
}
