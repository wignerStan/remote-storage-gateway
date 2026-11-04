package manager

import (
	"context"
	"encoding/json"

	buildbarnbox "github.com/wignerStan/buildbarn-storage/go/vfs/rclone/mount"
	"github.com/wignerStan/buildbarn-storage/go/vfs/rclone/source"
	"github.com/wignerStan/network-data-plane-worker/go/contracts/storageipc"
	dp "github.com/wignerStan/network-data-plane-worker/go/contracts/worker"
	rclonebox "github.com/wignerStan/rclone/go/box"
	"github.com/wignerStan/remote-storage-gateway/go/contracts/control/broker"
)

const (
	MaterializerRawCache        = "raw-cache"
	MaterializerBuildBarnSource = "buildbarn-source"
)

// MaterializerSpec selects one product's typed process contract. Config retains
// the existing independent-rclone raw-cache shape; a source view explicitly
// selects BuildBarnConfig instead. Neither contract contains origin credentials.
type MaterializerSpec struct {
	Profile        string        `json:"profile,omitempty"`
	ActivateClient *dp.Selection `json:"activate_client,omitempty"`
	// ExpectedActivation is the cached client's compare-and-swap version. It is
	// distinct from BuildBarnConfig.ExpectedGeneration, which identifies the
	// immutable source remote. Source activation never discovers a new expected
	// version with Observed or retries a rejected replacement against a new head.
	ExpectedActivation *dp.Version                 `json:"expected_activation,omitempty"`
	Executable         string                      `json:"executable"`
	Config             rclonebox.ProcessConfig     `json:"config,omitempty"`
	BuildBarnConfig    *buildbarnbox.ProcessConfig `json:"buildbarn_config,omitempty"`
}

func (s MaterializerSpec) profile() string {
	if s.Profile == "" {
		return MaterializerRawCache
	}
	return s.Profile
}

func (s MaterializerSpec) assignment() (string, string, storageipc.Endpoint) {
	if s.profile() == MaterializerBuildBarnSource && s.BuildBarnConfig != nil {
		c := s.BuildBarnConfig
		return c.InstanceID, c.ControlSocket, c.Remote
	}
	return s.Config.InstanceID, s.Config.ControlSocket, s.Config.Remote
}

func validateMaterializerProfile(s MaterializerSpec) error {
	if s.ExpectedActivation != nil && (s.ActivateClient == nil || !source.ValidGeneration(*s.ExpectedActivation)) {
		return broker.ErrInvalid
	}
	switch s.profile() {
	case MaterializerRawCache:
		if s.BuildBarnConfig != nil {
			return broker.ErrInvalid
		}
	case MaterializerBuildBarnSource:
		if s.BuildBarnConfig == nil || s.Config != (rclonebox.ProcessConfig{}) || !s.BuildBarnConfig.SourceView || s.BuildBarnConfig.ExpectedGeneration == nil || !source.ValidGeneration(*s.BuildBarnConfig.ExpectedGeneration) || s.ActivateClient != nil && s.ExpectedActivation == nil {
			return broker.ErrInvalid
		}
	default:
		return broker.ErrInvalid
	}
	return nil
}

// validateSourceAssignment applies the owner package's finite configuration
// validation before any bundle child starts. Only unobserved storage transport
// fields get temporary values in this local copy; expected source/activation
// generations remain the publisher's exact values. Startup revalidates the real
// Worker transport incarnation through the same typed constructor.
func validateSourceAssignment(s MaterializerSpec, socket, offerSocket string) error {
	if s.profile() != MaterializerBuildBarnSource {
		return nil
	}
	c := *s.BuildBarnConfig
	if c.Remote.Socket == "" {
		c.Remote.Socket = socket
	}
	if c.Remote.OfferSocket == "" {
		c.Remote.OfferSocket = offerSocket
	}
	if c.Remote.ExpectedIncarnation == "" {
		c.Remote.ExpectedIncarnation = "pending-worker-start"
	}
	if buildbarnbox.ValidateProcessConfig(c) != nil {
		return broker.ErrInvalid
	}
	return nil
}

// withRemote owns the typed config before filling in the supervised Worker's
// live transport identity. The published source and activation versions are
// copied exactly, including artifact_digest, and are never inferred here.
func (s MaterializerSpec) withRemote(remote storageipc.Endpoint) (MaterializerSpec, error) {
	var copy MaterializerSpec
	data, err := json.Marshal(s)
	if err != nil || len(data) > 32<<10 || json.Unmarshal(data, &copy) != nil {
		return MaterializerSpec{}, broker.ErrInvalid
	}
	if copy.profile() == MaterializerBuildBarnSource {
		copy.BuildBarnConfig.Remote = remote
	} else {
		copy.Config.Remote = remote
	}
	return copy, nil
}

type materializerState struct {
	ID    string
	PID   int
	Phase string
}
type materializerInfo struct {
	ID          string
	Ready       bool
	WorkerID    string
	Namespace   string
	Incarnation string
	SourceView  bool
	Generation  *dp.Version
}

type materializerProcess interface {
	Start(context.Context) error
	State() materializerState
	Info(context.Context) (materializerInfo, error)
	Close(context.Context) error
}

type rawMaterializer struct{ *rclonebox.Box }

func (b rawMaterializer) State() materializerState {
	s := b.Box.State()
	return materializerState{ID: s.ID, PID: s.PID, Phase: s.Phase}
}
func (b rawMaterializer) Info(ctx context.Context) (materializerInfo, error) {
	i, err := b.Box.Info(ctx)
	return materializerInfo{ID: i.ID, Ready: i.Ready, WorkerID: i.WorkerID, Namespace: i.Namespace, Incarnation: i.Incarnation}, err
}

type sourceMaterializer struct{ *buildbarnbox.Box }

func (b sourceMaterializer) State() materializerState {
	s := b.Box.State()
	return materializerState{ID: s.ID, PID: s.PID, Phase: s.Phase}
}
func (b sourceMaterializer) Info(ctx context.Context) (materializerInfo, error) {
	i, err := b.Box.Info(ctx)
	return materializerInfo{ID: i.ID, Ready: i.Ready, WorkerID: i.WorkerID, Namespace: i.Namespace, Incarnation: i.Incarnation, SourceView: i.SourceView, Generation: i.Generation}, err
}

func newMaterializer(s MaterializerSpec) (materializerProcess, error) {
	if err := validateMaterializerProfile(s); err != nil {
		return nil, err
	}
	if s.profile() == MaterializerBuildBarnSource {
		b, err := buildbarnbox.New(buildbarnbox.Config{Executable: s.Executable, ProcessConfig: *s.BuildBarnConfig})
		if err != nil {
			return nil, err
		}
		return sourceMaterializer{b}, nil
	}
	b, err := rclonebox.New(rclonebox.Config{Executable: s.Executable, ProcessConfig: s.Config})
	if err != nil {
		return nil, err
	}
	return rawMaterializer{b}, nil
}
