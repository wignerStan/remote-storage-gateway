package pluginruntime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/wignerStan/infra/go/code/schema"
	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/client"
	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/descriptor"
	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/descriptor/admission"
	"io"
	"sync/atomic"
	"testing"
	"time"
)

type fakeFactory struct {
	failed    atomic.Bool
	instances chan *fakeClient
}

func generatedConfig[T any]() descriptor.ConfigSchema {
	doc, err := schema.Derive[T](schema.Identity{ID: "worker.test.config", Version: "1", Owner: "network-data-plane-worker"})
	if err != nil {
		panic(err)
	}
	b, err := json.Marshal(doc.Schema)
	if err != nil {
		panic(err)
	}
	return b
}
func (f *fakeFactory) Descriptor() descriptor.Descriptor {
	return descriptor.Descriptor{Schema: descriptor.Schema, PluginID: "test.client", PluginVersion: "1", Contract: client.Contract, Runtime: "go", BuildDigest: "sha256:" + string(bytes.Repeat([]byte("a"), 64)), Implementations: []descriptor.Implementation{{ID: "fake", ConfigSchema: generatedConfig[struct{}](), Capabilities: []string{"stat", "open"}}}}
}
func (f *fakeFactory) Prepare(context.Context, client.PluginConfig) (client.Candidate, error) {
	if f.failed.Load() {
		return nil, errors.New("private provider token must not escape")
	}
	c := &fakeClient{closed: make(chan struct{})}
	f.instances <- c
	return &fakeCandidate{c}, nil
}

type fakeCandidate struct{ c *fakeClient }

func (c *fakeCandidate) Validate(context.Context) error {
	return nil
}
func (c *fakeCandidate) Publish() (client.Client, error) {
	return c.c, nil
}
func (c *fakeCandidate) Abort(context.Context) error {
	return nil
}

type fakeClient struct{ closed chan struct{} }

func (c *fakeClient) Close(context.Context) error {
	close(c.closed)
	return nil
}
func (c *fakeClient) Stat(context.Context, client.ObjectRef) (client.ObjectInfo, error) {
	return client.ObjectInfo{Path: "pinned"}, nil
}
func (c *fakeClient) List(context.Context, client.ListRequest) (client.ListResult, error) {
	return client.ListResult{}, client.ErrUnsupported
}
func (c *fakeClient) Open(context.Context, client.OpenRequest) (client.ReadPlan, error) {
	return client.ReadPlan{Body: io.NopCloser(bytes.NewReader([]byte("stream")))}, nil
}
func (c *fakeClient) Put(context.Context, client.PutRequest) (client.PutResult, error) {
	return client.PutResult{}, client.ErrUnsupported
}
func (c *fakeClient) Delete(context.Context, client.ObjectRef) error {
	return client.ErrUnsupported
}
func setup(t *testing.T) (*Manager, *fakeFactory, Selection) {
	t.Helper()
	r := NewRegistry()
	f := &fakeFactory{instances: make(chan *fakeClient, 8)}
	if e := r.Register(f); e != nil {
		t.Fatal(e)
	}
	if e := r.Register(f); e == nil {
		t.Fatal("duplicate admitted")
	}
	m, e := NewManager(r, nil)
	if e != nil {
		t.Fatal(e)
	}
	return m, f, Selection{Name: "one", PluginID: "test.client", DescriptorDigest: r.Records()[0].Digest, ImplementationID: "fake", Public: []byte(`{}`)}
}
func TestPublishFailureAndPinnedDrain(t *testing.T) {
	m, f, s := setup(t)
	v, e := m.Replace(context.Background(), m.Current(s.Name), s)
	if e != nil {
		t.Fatal(e)
	}
	old := <-f.instances
	lease, e := m.Pin(s.Name)
	if e != nil {
		t.Fatal(e)
	}
	f.failed.Store(true)
	if _, e = m.Replace(context.Background(), v, s); !errors.Is(e, client.ErrProvider) {
		t.Fatalf("provider failure not sanitized: %v", e)
	}
	if m.Current(s.Name) != v {
		t.Fatal("failed candidate replaced current")
	}
	f.failed.Store(false)
	next, e := m.Replace(context.Background(), v, s)
	if e != nil || next.Generation == v.Generation {
		t.Fatal("replacement failed", e)
	}
	<-f.instances
	select {
	case <-old.closed:
		t.Fatal("closed beneath pinned exchange")
	default:
	}
	if _, e = lease.Client().Stat(context.Background(), client.ObjectRef{}); e != nil {
		t.Fatal(e)
	}
	lease.Release()
	select {
	case <-old.closed:
	case <-time.After(time.Second):
		t.Fatal("old generation did not drain")
	}
	if _, e = m.Replace(context.Background(), v, s); e == nil {
		t.Fatal("stale generation admitted")
	}
	bad := m.Current(s.Name)
	bad.Incarnation = "stale"
	if _, e = m.Replace(context.Background(), bad, s); e == nil {
		t.Fatal("stale incarnation admitted")
	}
	if e = m.Close(context.Background()); e != nil {
		t.Fatal(e)
	}
}
func TestCancelledCloseKeepsCleanupOwnership(t *testing.T) {
	m, f, s := setup(t)
	if _, e := m.Replace(context.Background(), m.Current(s.Name), s); e != nil {
		t.Fatal(e)
	}
	c := <-f.instances
	lease, e := m.Pin(s.Name)
	if e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if e = m.Close(ctx); !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	select {
	case <-c.closed:
		t.Fatal("cleanup abandoned pin")
	default:
	}
	lease.Release()
	if e = m.Close(context.Background()); e != nil {
		t.Fatal(e)
	}
	select {
	case <-c.closed:
	default:
		t.Fatal("cleanup did not finish")
	}
}
func TestOpenBodyPinsGeneration(t *testing.T) {
	m, f, s := setup(t)
	v, e := m.Replace(context.Background(), m.Current(s.Name), s)
	if e != nil {
		t.Fatal(e)
	}
	old := <-f.instances
	p, e := m.Client(s.Name).Open(context.Background(), client.OpenRequest{Length: -1})
	if e != nil {
		t.Fatal(e)
	}
	if _, e = m.Replace(context.Background(), v, s); e != nil {
		t.Fatal(e)
	}
	<-f.instances
	select {
	case <-old.closed:
		t.Fatal("body lost generation")
	default:
	}
	b, e := io.ReadAll(p.Body)
	if e != nil || string(b) != "stream" {
		t.Fatal(e)
	}
	p.Body.Close()
	m.Close(context.Background())
}

type exactFactory struct {
	*fakeFactory
	record descriptor.Record
}

func (f *exactFactory) DescriptorRecord() descriptor.Record {
	return f.record
}
func (f *exactFactory) Descriptor() descriptor.Descriptor {
	return f.record.Descriptor
}
func TestRegistryPreservesExactBytesAndDetachesReads(t *testing.T) {
	f := &fakeFactory{instances: make(chan *fakeClient, 8)}
	r, _ := admission.Encode(f.Descriptor())
	r, e := admission.Decode(append([]byte(" \n"), r.Bytes...))
	if e != nil {
		t.Fatal(e)
	}
	registry := NewRegistry()
	if e = registry.Register(&exactFactory{f, r}); e != nil {
		t.Fatal(e)
	}
	got := registry.Records()[0]
	if got.Digest != r.Digest || !bytes.Equal(got.Bytes, r.Bytes) {
		t.Fatal("descriptor bytes were normalized")
	}
	got.Descriptor.Implementations[0].Capabilities[0] = "tampered"
	got.Bytes[0] = '!'
	fresh := registry.Records()[0]
	if fresh.Descriptor.Implementations[0].Capabilities[0] != "stat" || fresh.Bytes[0] != ' ' {
		t.Fatal("descriptor record aliases mutable caller")
	}
}
func TestReplacementSelectsApprovedDescriptorVersion(t *testing.T) {
	registry := NewRegistry()
	f := &fakeFactory{instances: make(chan *fakeClient, 8)}
	first, e := admission.Encode(f.Descriptor())
	if e != nil {
		t.Fatal(e)
	}
	secondDescriptor := f.Descriptor()
	secondDescriptor.PluginVersion = "2"
	second, e := admission.Encode(secondDescriptor)
	if e != nil {
		t.Fatal(e)
	}
	for _, record := range []descriptor.Record{first, second} {
		if e = registry.Register(&exactFactory{f, record}); e != nil {
			t.Fatal(e)
		}
	}
	m, e := NewManager(registry, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer m.Close(context.Background())
	s := Selection{Name: "one", PluginID: first.Descriptor.PluginID, DescriptorDigest: first.Digest, ImplementationID: "fake", Public: []byte(`{}`)}
	v, e := m.Replace(context.Background(), m.Current(s.Name), s)
	if e != nil {
		t.Fatal(e)
	}
	<-f.instances
	s.DescriptorDigest = second.Digest
	next, e := m.Replace(context.Background(), v, s)
	if e != nil {
		t.Fatal(e)
	}
	<-f.instances
	if next.DescriptorDigest != second.Digest || next.Generation <= v.Generation {
		t.Fatal("approved version was not selected", next)
	}
	stale := next
	stale.DescriptorDigest = first.Digest
	if _, e = m.Replace(context.Background(), stale, s); e == nil {
		t.Fatal("descriptor-mismatched proof admitted")
	}
}
func TestGenerationExhaustionNeverReusesPublishedIdentity(t *testing.T) {
	m, f, s := setup(t)
	m.sequence = ^uint64(0) - 1
	v, e := m.Replace(context.Background(), m.Current(s.Name), s)
	if e != nil || v.Generation != ^uint64(0) {
		t.Fatal(v, e)
	}
	<-f.instances
	for range 2 {
		if _, e = m.Replace(context.Background(), v, s); e == nil {
			t.Fatal("generation identity reused after exhaustion")
		}
	}
	if m.Current(s.Name) != v {
		t.Fatal("exhaustion withdrew current generation")
	}
	m.Close(context.Background())
}
