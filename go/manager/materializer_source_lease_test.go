package manager

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wignerStan/infra/go/config/schema"
	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/client"
	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/descriptor"
	dp "github.com/wignerStan/network-data-plane-worker/go/contracts/worker"
	"github.com/wignerStan/network-data-plane-worker/go/pluginruntime"
)

// This test uses the actual Worker registry and its leases. The process fixture
// is deliberately limited to Manager lifecycle/control binding; native process
// transport and the immutable source data path have their own owner tests.
type sourceLeaseFactory struct {
	configSchema descriptor.ConfigSchema
	instances    chan *sourceLeaseClient
}

func (f *sourceLeaseFactory) Descriptor() descriptor.Descriptor {
	return descriptor.Descriptor{Schema: descriptor.Schema, PluginID: "test.source-cache", PluginVersion: "1", Contract: client.Contract, Runtime: "go", BuildDigest: "sha256:" + strings.Repeat("a", 64), Implementations: []descriptor.Implementation{{ID: "cache", ConfigSchema: f.configSchema, Capabilities: []string{"stat", "open"}}}}
}
func (*sourceLeaseFactory) ArtifactDigest() string { return "sha256:" + strings.Repeat("b", 64) }
func (f *sourceLeaseFactory) Prepare(context.Context, client.PluginConfig) (client.Candidate, error) {
	c := &sourceLeaseClient{done: make(chan struct{})}
	f.instances <- c
	return &sourceLeaseCandidate{c}, nil
}

type sourceLeaseCandidate struct{ client *sourceLeaseClient }

func (*sourceLeaseCandidate) Validate(context.Context) error    { return nil }
func (c *sourceLeaseCandidate) Publish() (client.Client, error) { return c.client, nil }
func (c *sourceLeaseCandidate) Abort(ctx context.Context) error { return c.client.Close(ctx) }

type sourceLeaseClient struct {
	closed atomic.Bool
	once   sync.Once
	done   chan struct{}
}

func (c *sourceLeaseClient) Close(context.Context) error {
	c.once.Do(func() { c.closed.Store(true); close(c.done) })
	return nil
}
func (c *sourceLeaseClient) Stat(context.Context, client.ObjectRef) (client.ObjectInfo, error) {
	if c.closed.Load() {
		return client.ObjectInfo{}, errors.New("lease already retired")
	}
	return client.ObjectInfo{Path: "retained-source"}, nil
}
func (*sourceLeaseClient) List(context.Context, client.ListRequest) (client.ListResult, error) {
	return client.ListResult{}, client.ErrUnsupported
}
func (*sourceLeaseClient) Open(context.Context, client.OpenRequest) (client.ReadPlan, error) {
	return client.ReadPlan{Body: io.NopCloser(strings.NewReader("retained-source"))}, nil
}
func (*sourceLeaseClient) Put(context.Context, client.PutRequest) (client.PutResult, error) {
	return client.PutResult{}, client.ErrUnsupported
}
func (*sourceLeaseClient) Delete(context.Context, client.ObjectRef) error {
	return client.ErrUnsupported
}

type registrySourceControl struct {
	*sourceControlFixture
	registry *pluginruntime.Manager
}

func (c *registrySourceControl) Replace(ctx context.Context, name string, expected dp.Version, selection dp.Selection) (dp.Version, error) {
	if _, err := c.sourceControlFixture.Replace(ctx, name, expected, selection); err != nil {
		return dp.Version{}, err
	}
	return c.registry.Replace(ctx, expected, selection)
}

func TestBuildBarnSourceCASFailureAndSuccessPreservePinnedWorkerLease(t *testing.T) {
	doc, err := schema.Derive[struct{}]()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	factory := &sourceLeaseFactory{configSchema: raw, instances: make(chan *sourceLeaseClient, 8)}
	approved := pluginruntime.NewRegistry()
	if err = approved.Register(factory); err != nil {
		t.Fatal(err)
	}
	registry, err := pluginruntime.NewManager(approved, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = registry.Close(context.Background()) })
	selection := dp.Selection{Name: "source-cache", PluginID: factory.Descriptor().PluginID, DescriptorDigest: approved.Records()[0].Digest, ImplementationID: "cache", Public: json.RawMessage(`{}`)}
	first, err := registry.Replace(context.Background(), registry.Current(selection.Name), selection)
	if err != nil {
		t.Fatal(err)
	}
	firstClient := <-factory.instances
	oldLease, err := registry.Pin(selection.Name)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(oldLease.Release)
	second, err := registry.Replace(context.Background(), first, selection)
	if err != nil {
		t.Fatal(err)
	}
	secondClient := <-factory.instances
	currentLease, err := registry.Pin(selection.Name)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(currentLease.Release)
	control := &registrySourceControl{sourceControlFixture: sourceControl(), registry: registry}
	spec := sourceBundle().Materializers[0]
	spec.ActivateClient, spec.ExpectedActivation = &selection, &first
	failed := sourceProcess(spec)
	err = bareSourceManager().startMaterializerProcess(context.Background(), spec, control, func(MaterializerSpec) (materializerProcess, error) { return failed, nil })
	if err == nil || registry.Current(selection.Name) != second || !failed.closed.Load() || control.observedCalls != 0 || control.replaceCalls != 1 || control.expected != first {
		t.Fatalf("stale activation advanced the registry or changed its expected version: %v", err)
	}
	if firstClient.closed.Load() || secondClient.closed.Load() {
		t.Fatal("failed candidate closed a retained registry generation")
	}
	if _, err = oldLease.Client().Stat(context.Background(), client.ObjectRef{}); err != nil {
		t.Fatal("old lease unusable after rejected activation", err)
	}
	if _, err = currentLease.Client().Stat(context.Background(), client.ObjectRef{}); err != nil {
		t.Fatal("current lease unusable after rejected activation", err)
	}

	spec.ExpectedActivation = &second
	accepted := sourceProcess(spec)
	if err = bareSourceManager().startMaterializerProcess(context.Background(), spec, control, func(MaterializerSpec) (materializerProcess, error) { return accepted, nil }); err != nil {
		t.Fatal(err)
	}
	<-factory.instances
	if registry.Current(selection.Name) == second || control.expected != second || control.observedCalls != 0 || control.replaceCalls != 2 || accepted.closed.Load() {
		t.Fatal("pinned activation was not published exactly once")
	}
	if firstClient.closed.Load() || secondClient.closed.Load() {
		t.Fatal("successful replacement closed a retained lease")
	}
	if _, err = currentLease.Client().Stat(context.Background(), client.ObjectRef{}); err != nil {
		t.Fatal("retained lease unusable after accepted activation", err)
	}
	oldLease.Release()
	currentLease.Release()
	for _, retired := range []*sourceLeaseClient{firstClient, secondClient} {
		select {
		case <-retired.done:
		case <-time.After(time.Second):
			t.Fatal("retired generation was not drained after lease release")
		}
	}
	if err = accepted.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestBuildBarnSourceCloseRetainsCleanupAfterCancelledWaiter(t *testing.T) {
	cfg := sourceBundle()
	m, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	child := sourceProcess(cfg.Materializers[0])
	child.closeError = errors.New("joined child cleanup")
	child.closeEntered, child.closeRelease = make(chan struct{}), make(chan struct{})
	m.materializers[child.info.ID].box = child
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err = m.Close(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal("cancelled waiter did not return its own cancellation", err)
	}
	select {
	case <-child.closeEntered:
	case <-time.After(time.Second):
		t.Fatal("cleanup ownership was abandoned")
	}
	select {
	case <-m.done:
		t.Fatal("manager closed before materializer joined")
	default:
	}
	close(child.closeRelease)
	if err = m.Close(context.Background()); !errors.Is(err, child.closeError) {
		t.Fatal("joined cleanup error was not retained", err)
	}
	if !child.closed.Load() || child.closes.Load() != 1 || m.Ready() {
		t.Fatal("materializer was not closed exactly once")
	}
}
