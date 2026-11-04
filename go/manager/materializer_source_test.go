package manager

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	buildbarnbox "github.com/wignerStan/buildbarn-storage/go/vfs/rclone/mount"
	"github.com/wignerStan/network-data-plane-worker/go/contracts/storageipc"
	dp "github.com/wignerStan/network-data-plane-worker/go/contracts/worker"
	rclonebox "github.com/wignerStan/rclone/go/box"
	"github.com/wignerStan/remote-storage-gateway/go/contracts/control/broker"
	workerconfig "github.com/wignerStan/remote-storage-gateway/go/contracts/worker"
)

func sourceBundle() Config {
	const dir = "/run/gateway-source-test"
	sourceVersion := dp.Version{Incarnation: "source-registry", Generation: ^uint64(0) - 3, DescriptorDigest: "sha256:" + strings.Repeat("a", 64), ArtifactDigest: "sha256:" + strings.Repeat("b", 64)}
	activationVersion := dp.Version{Incarnation: "cached-registry", Generation: 11, DescriptorDigest: "sha256:" + strings.Repeat("c", 64), ArtifactDigest: "sha256:" + strings.Repeat("d", 64)}
	return Config{
		RuntimeDirectory: dir,
		Workers: []WorkerSpec{{Executable: "/bin/true", Config: workerconfig.ProcessConfig{
			InstanceID: "worker-source", ControlSocket: dir + "/worker.sock",
			RuntimeConfig: workerconfig.RuntimeConfig{StorageIPCSocket: dir + "/raw.sock", StorageOfferSocket: dir + "/offer.sock", Mounts: []workerconfig.Mount{{Name: "source", Client: "source-cache", RemoteClient: "published-source", ReadOnly: true}}},
		}}},
		Materializers: []MaterializerSpec{{
			Profile: MaterializerBuildBarnSource, Executable: "/bin/true",
			ActivateClient:     &dp.Selection{Name: "source-cache", PluginID: "client.cache", DescriptorDigest: activationVersion.DescriptorDigest, ImplementationID: "webdav", Public: json.RawMessage(`{}`)},
			ExpectedActivation: &activationVersion,
			BuildBarnConfig: &buildbarnbox.ProcessConfig{
				SourceView: true, ExpectedGeneration: &sourceVersion,
				FilterRules: []string{"+ /src/**", "- **"}, FilterFile: "/var/lib/gateway/source.filter",
				InstanceID: "buildbarn-source", ControlSocket: dir + "/source.sock", Listen: "127.0.0.1:12345",
				Remote:        storageipc.Endpoint{Namespace: "source", ExpectedWorkerID: "worker-source"},
				LocalUsername: "source-cache", LocalPasswordFile: "/var/lib/gateway/local-auth", Directory: "/var/lib/gateway/source-cache", MaxBytes: 8 << 20, MaxAgeSeconds: 60,
				MaxCacheEntries: 256, MaxOpenFiles: 4, MaxDirectoryEntries: 256, MaxConcurrentRequests: 4,
			},
		}},
	}
}

func TestBuildBarnSourceProfileRejectsInvalidAssignmentBeforeStartup(t *testing.T) {
	for name, mutate := range map[string]func(*Config){
		"unknown-profile":    func(c *Config) { c.Materializers[0].Profile = "buildbarn-latest" },
		"missing-config":     func(c *Config) { c.Materializers[0].BuildBarnConfig = nil },
		"mixed-products":     func(c *Config) { c.Materializers[0].Config.InstanceID = "raw" },
		"source-disabled":    func(c *Config) { c.Materializers[0].BuildBarnConfig.SourceView = false },
		"missing-generation": func(c *Config) { c.Materializers[0].BuildBarnConfig.ExpectedGeneration = nil },
		"zero-generation":    func(c *Config) { c.Materializers[0].BuildBarnConfig.ExpectedGeneration.Generation = 0 },
		"bad-artifact":       func(c *Config) { c.Materializers[0].BuildBarnConfig.ExpectedGeneration.ArtifactDigest = "unbound" },
		"bad-registry": func(c *Config) {
			c.Materializers[0].BuildBarnConfig.ExpectedGeneration.Incarnation = "invalid/registry"
		},
		"missing-activation-version": func(c *Config) { c.Materializers[0].ExpectedActivation = nil },
		"bad-activation-version":     func(c *Config) { c.Materializers[0].ExpectedActivation.ArtifactDigest = "unbound" },
		"unused-activation-version":  func(c *Config) { c.Materializers[0].ActivateClient = nil },
		"relative-filter-file":       func(c *Config) { c.Materializers[0].BuildBarnConfig.FilterFile = "source.filter" },
		"multiline-filter":           func(c *Config) { c.Materializers[0].BuildBarnConfig.FilterRules = []string{"+ /src/**\n- **"} },
		"unbounded-cache":            func(c *Config) { c.Materializers[0].BuildBarnConfig.MaxBytes = 0 },
		"public-listener":            func(c *Config) { c.Materializers[0].BuildBarnConfig.Listen = "0.0.0.0:12345" },
		"foreign-worker":             func(c *Config) { c.Materializers[0].BuildBarnConfig.Remote.ExpectedWorkerID = "another-worker" },
		"foreign-socket":             func(c *Config) { c.Materializers[0].BuildBarnConfig.Remote.Socket = "/run/other/raw.sock" },
		"foreign-namespace":          func(c *Config) { c.Materializers[0].BuildBarnConfig.Remote.Namespace = "another-source" },
		"recursive-activation":       func(c *Config) { c.Materializers[0].ActivateClient.Name = "published-source" },
		"writable-source-without-activation": func(c *Config) {
			c.Materializers[0].ActivateClient = nil
			c.Materializers[0].ExpectedActivation = nil
			c.Workers[0].Config.Mounts[0].ReadOnly = false
		},
	} {
		t.Run(name, func(t *testing.T) {
			cfg := sourceBundle()
			mutate(&cfg)
			m, err := New(cfg)
			if m != nil {
				_ = m.Close(context.Background())
			}
			if !errors.Is(err, broker.ErrInvalid) {
				t.Fatalf("invalid source assignment admitted: %v", err)
			}
		})
	}
}

func TestBuildBarnSourceConfigPreservesTypedValuesAndCallerOwnership(t *testing.T) {
	cfg := sourceBundle()
	want := *cfg.Materializers[0].BuildBarnConfig.ExpectedGeneration
	wantActivation := *cfg.Materializers[0].ExpectedActivation
	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "bundle.json")
	if err = os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	loaded, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = loaded.Close(context.Background()) })
	m, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Close(context.Background()) })
	cfg.Materializers[0].BuildBarnConfig.ExpectedGeneration.ArtifactDigest = "mutated"
	cfg.Materializers[0].ExpectedActivation.Generation++
	cfg.Materializers[0].BuildBarnConfig.FilterRules[0] = "mutated"
	for _, managed := range []*Manager{loaded, m} {
		spec := managed.config.Materializers[0]
		if *spec.BuildBarnConfig.ExpectedGeneration != want || *spec.ExpectedActivation != wantActivation || spec.BuildBarnConfig.FilterRules[0] != "+ /src/**" || spec.BuildBarnConfig.FilterFile != "/var/lib/gateway/source.filter" {
			t.Fatal("config lost a typed value or retained mutable caller ownership")
		}
		instances := managed.Instances()
		found := false
		for _, instance := range instances {
			if instance.ID == spec.BuildBarnConfig.InstanceID {
				found = instance.Kind == MaterializerBuildBarnSource
			}
		}
		if !found {
			t.Fatal("source product identity was not exposed")
		}
		if managed.Ready() {
			t.Fatal("unstarted source materializer became ready")
		}
	}
}

type sourceControlFixture struct {
	info          broker.InstanceInfo
	observed      []dp.Observed
	observedCalls int
	replaceCalls  int
	expected      dp.Version
	selection     dp.Selection
	replaceError  error
}

func (c *sourceControlFixture) Info(context.Context) (broker.InstanceInfo, error) { return c.info, nil }
func (c *sourceControlFixture) Observed(context.Context) ([]dp.Observed, error) {
	c.observedCalls++
	return c.observed, nil
}
func (c *sourceControlFixture) Replace(_ context.Context, _ string, expected dp.Version, selection dp.Selection) (dp.Version, error) {
	c.replaceCalls++
	c.expected, c.selection = expected, selection
	return dp.Version{}, c.replaceError
}

func sourceControl() *sourceControlFixture {
	return &sourceControlFixture{info: broker.InstanceInfo{ID: "worker-source", Ready: true, StorageSocket: "/run/gateway-source-test/raw.sock", OfferSocket: "/run/gateway-source-test/offer.sock", Incarnation: "storage-transport"}}
}

type sourceProcessFixture struct {
	info                              materializerInfo
	startError, infoError, closeError error
	closeEntered, closeRelease        chan struct{}
	starts                            atomic.Int32
	closes                            atomic.Int32
	closed                            atomic.Bool
}

func (p *sourceProcessFixture) Start(context.Context) error { p.starts.Add(1); return p.startError }
func (p *sourceProcessFixture) Info(context.Context) (materializerInfo, error) {
	return p.info, p.infoError
}
func (p *sourceProcessFixture) State() materializerState {
	phase := "ready"
	if p.closed.Load() {
		phase = "closed"
	}
	return materializerState{ID: p.info.ID, Phase: phase}
}
func (p *sourceProcessFixture) Close(context.Context) error {
	if p.closes.Add(1) == 1 && p.closeEntered != nil {
		close(p.closeEntered)
	}
	if p.closeRelease != nil {
		<-p.closeRelease
	}
	p.closed.Store(true)
	return p.closeError
}
func sourceProcess(s MaterializerSpec) *sourceProcessFixture {
	generation := *s.BuildBarnConfig.ExpectedGeneration
	return &sourceProcessFixture{info: materializerInfo{ID: s.BuildBarnConfig.InstanceID, Ready: true, WorkerID: "worker-source", Namespace: "source", Incarnation: "storage-transport", SourceView: true, Generation: &generation}}
}
func bareSourceManager() *Manager { return &Manager{materializers: map[string]*materializer{}} }

func TestBuildBarnSourceActivationCarriesPublishedAndExpectedVersionsExactly(t *testing.T) {
	spec := sourceBundle().Materializers[0]
	control := sourceControl()
	child := sourceProcess(spec)
	wantSource, wantActivation := *spec.BuildBarnConfig.ExpectedGeneration, *spec.ExpectedActivation
	var supplied MaterializerSpec
	create := func(s MaterializerSpec) (materializerProcess, error) { supplied = s; return child, nil }
	if err := bareSourceManager().startMaterializerProcess(context.Background(), spec, control, create); err != nil {
		t.Fatal(err)
	}
	if supplied.BuildBarnConfig.Remote.Socket != control.info.StorageSocket || supplied.BuildBarnConfig.Remote.OfferSocket != control.info.OfferSocket || supplied.BuildBarnConfig.Remote.ExpectedIncarnation != control.info.Incarnation {
		t.Fatal("materializer did not receive the supervised Worker's live transport identity")
	}
	if *supplied.BuildBarnConfig.ExpectedGeneration != wantSource || *supplied.ExpectedActivation != wantActivation || !reflect.DeepEqual(supplied.BuildBarnConfig.FilterRules, spec.BuildBarnConfig.FilterRules) || supplied.BuildBarnConfig.FilterFile != spec.BuildBarnConfig.FilterFile {
		t.Fatal("typed source assignment changed before constructor")
	}
	if control.observedCalls != 0 || control.replaceCalls != 1 || control.expected != wantActivation || !reflect.DeepEqual(control.selection, *spec.ActivateClient) {
		t.Fatalf("activation did not use the exact pinned target: %+v", control)
	}
	if child.closes.Load() != 0 {
		t.Fatal("successful candidate was retired")
	}
	if spec.BuildBarnConfig.Remote.ExpectedIncarnation != "" {
		t.Fatal("startup mutated caller-owned config")
	}
	supplied.BuildBarnConfig.ExpectedGeneration.ArtifactDigest = "mutated"
	supplied.BuildBarnConfig.FilterRules[0] = "mutated"
	if *spec.BuildBarnConfig.ExpectedGeneration != wantSource || spec.BuildBarnConfig.FilterRules[0] != "+ /src/**" {
		t.Fatal("constructor retained caller-owned source values")
	}
}

func TestBuildBarnSourceMetadataMismatchClosesBeforeActivation(t *testing.T) {
	for name, mutate := range map[string]func(*materializerInfo){
		"not-ready":                 func(i *materializerInfo) { i.Ready = false },
		"wrong-child":               func(i *materializerInfo) { i.ID = "another-child" },
		"wrong-worker":              func(i *materializerInfo) { i.WorkerID = "another-worker" },
		"wrong-namespace":           func(i *materializerInfo) { i.Namespace = "another-source" },
		"wrong-storage-incarnation": func(i *materializerInfo) { i.Incarnation = "another-storage" },
		"raw-profile":               func(i *materializerInfo) { i.SourceView = false },
		"missing-generation":        func(i *materializerInfo) { i.Generation = nil },
		"wrong-registry":            func(i *materializerInfo) { i.Generation.Incarnation = "another-registry" },
		"wrong-generation":          func(i *materializerInfo) { i.Generation.Generation++ },
		"wrong-descriptor":          func(i *materializerInfo) { i.Generation.DescriptorDigest = "sha256:" + strings.Repeat("e", 64) },
		"wrong-artifact":            func(i *materializerInfo) { i.Generation.ArtifactDigest = "sha256:" + strings.Repeat("f", 64) },
	} {
		t.Run(name, func(t *testing.T) {
			spec := sourceBundle().Materializers[0]
			control := sourceControl()
			child := sourceProcess(spec)
			mutate(&child.info)
			m := bareSourceManager()
			err := m.startMaterializerProcess(context.Background(), spec, control, func(MaterializerSpec) (materializerProcess, error) { return child, nil })
			if !errors.Is(err, broker.ErrUnavailable) || child.closes.Load() != 1 || !child.closed.Load() || control.replaceCalls != 0 || control.observedCalls != 0 || m.Ready() {
				t.Fatalf("mismatched candidate leaked readiness or activation: err=%v closes=%d replace=%d", err, child.closes.Load(), control.replaceCalls)
			}
		})
	}
}

func TestBuildBarnSourceWorkerMismatchDoesNotConstructCandidate(t *testing.T) {
	for name, mutate := range map[string]func(*sourceControlFixture){
		"not-ready":       func(c *sourceControlFixture) { c.info.Ready = false },
		"wrong-worker":    func(c *sourceControlFixture) { c.info.ID = "different-worker" },
		"empty-transport": func(c *sourceControlFixture) { c.info.Incarnation = "" },
		"wrong-transport": func(c *sourceControlFixture) { c.info.Incarnation = "new-storage" },
		"wrong-socket":    func(c *sourceControlFixture) { c.info.StorageSocket += "-different" },
		"wrong-offer":     func(c *sourceControlFixture) { c.info.OfferSocket += "-different" },
	} {
		t.Run(name, func(t *testing.T) {
			spec := sourceBundle().Materializers[0]
			control := sourceControl()
			spec.BuildBarnConfig.Remote.Socket = control.info.StorageSocket
			spec.BuildBarnConfig.Remote.OfferSocket = control.info.OfferSocket
			spec.BuildBarnConfig.Remote.ExpectedIncarnation = control.info.Incarnation
			mutate(control)
			called := false
			err := bareSourceManager().startMaterializerProcess(context.Background(), spec, control, func(MaterializerSpec) (materializerProcess, error) { called = true; return sourceProcess(spec), nil })
			if err == nil || called || control.replaceCalls != 0 {
				t.Fatal("foreign worker constructed a source candidate", err)
			}
		})
	}
}

func TestBuildBarnSourceFailedStartJoinsCleanupAndRetainsErrors(t *testing.T) {
	spec := sourceBundle().Materializers[0]
	child := sourceProcess(spec)
	child.startError, child.closeError = errors.New("start failed"), errors.New("cleanup failed")
	child.closeEntered, child.closeRelease = make(chan struct{}), make(chan struct{})
	control := sourceControl()
	result := make(chan error, 1)
	go func() {
		result <- bareSourceManager().startMaterializerProcess(context.Background(), spec, control, func(MaterializerSpec) (materializerProcess, error) { return child, nil })
	}()
	select {
	case <-child.closeEntered:
	case <-time.After(time.Second):
		t.Fatal("rollback did not start")
	}
	select {
	case err := <-result:
		t.Fatal("rollback returned before cleanup joined", err)
	default:
	}
	close(child.closeRelease)
	select {
	case err := <-result:
		if !errors.Is(err, child.startError) || !errors.Is(err, child.closeError) || !child.closed.Load() || control.replaceCalls != 0 {
			t.Fatal("rollback lost cleanup ownership or original error", err)
		}
	case <-time.After(time.Second):
		t.Fatal("rollback did not join cleanup")
	}
}

func TestMaterializerConstructorSelectsExactProduct(t *testing.T) {
	spec := sourceBundle().Materializers[0]
	control := sourceControl()
	spec.BuildBarnConfig.Remote.Socket = control.info.StorageSocket
	spec.BuildBarnConfig.Remote.OfferSocket = control.info.OfferSocket
	spec.BuildBarnConfig.Remote.ExpectedIncarnation = control.info.Incarnation
	process, err := newMaterializer(spec)
	if err != nil {
		t.Fatal(err)
	}
	defer process.Close(context.Background())
	if _, ok := process.(sourceMaterializer); !ok {
		t.Fatalf("BuildBarn profile selected %T", process)
	}
	c := spec.BuildBarnConfig
	raw := MaterializerSpec{Executable: spec.Executable, Config: rclonebox.ProcessConfig{InstanceID: c.InstanceID, ControlSocket: c.ControlSocket, Listen: c.Listen, Remote: c.Remote, LocalUsername: c.LocalUsername, LocalPasswordFile: c.LocalPasswordFile, Directory: c.Directory, MaxBytes: c.MaxBytes, MaxAgeSeconds: c.MaxAgeSeconds}}
	process, err = newMaterializer(raw)
	if err != nil {
		t.Fatal(err)
	}
	defer process.Close(context.Background())
	if _, ok := process.(rawMaterializer); !ok {
		t.Fatalf("legacy raw config selected %T", process)
	}
	raw.BuildBarnConfig = spec.BuildBarnConfig
	if _, err := newMaterializer(raw); !errors.Is(err, broker.ErrInvalid) {
		t.Fatal("raw constructor accepted a mixed source contract", err)
	}
}

func TestRawCacheMaterializerKeepsLegacyActivation(t *testing.T) {
	sourceSpec := sourceBundle().Materializers[0]
	control := sourceControl()
	legacyVersion := *sourceSpec.ExpectedActivation
	legacyVersion.Generation++
	control.observed = []dp.Observed{{Name: sourceSpec.ActivateClient.Name, Version: legacyVersion, Accepting: true}}
	spec := MaterializerSpec{Executable: sourceSpec.Executable, ActivateClient: sourceSpec.ActivateClient, Config: rclonebox.ProcessConfig{InstanceID: "raw-cache", ControlSocket: "/run/gateway-source-test/cache.sock", Remote: sourceSpec.BuildBarnConfig.Remote}}
	child := &sourceProcessFixture{info: materializerInfo{ID: "raw-cache", Ready: true, WorkerID: control.info.ID, Namespace: spec.Config.Remote.Namespace, Incarnation: control.info.Incarnation}}
	var supplied MaterializerSpec
	err := bareSourceManager().startMaterializerProcess(context.Background(), spec, control, func(c MaterializerSpec) (materializerProcess, error) { supplied = c; return child, nil })
	if err != nil || control.observedCalls != 1 || control.replaceCalls != 1 || control.expected != legacyVersion || child.closed.Load() || supplied.BuildBarnConfig != nil || supplied.Config.Remote.ExpectedIncarnation != control.info.Incarnation {
		t.Fatalf("legacy raw-cache flow changed: %v", err)
	}
}
