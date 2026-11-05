//go:build dataplane_l7

package runtime

import (
	"errors"
	"reflect"
	"testing"

	"github.com/wignerStan/remote-storage-gateway/go/contracts/control/broker"
	workerconfig "github.com/wignerStan/remote-storage-gateway/go/contracts/worker"
)

func TestStorageNamespacesExposeOnlyExplicitRawAssignments(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mounts []workerconfig.Mount
		want   map[string]string
	}{
		{
			name: "mixed cached and ordinary mounts",
			mounts: []workerconfig.Mount{
				{Name: "files", Client: "cached-files", RemoteClient: "raw-files", ReadOnly: true},
				{Name: "logs", Client: "direct-logs"},
				{Name: "archive", Client: "cached-archive", RemoteClient: "raw-archive", ReadOnly: true},
			},
			want: map[string]string{"files": "raw-files", "archive": "raw-archive"},
		},
		{
			name:   "ordinary mount never falls back to public client",
			mounts: []workerconfig.Mount{{Name: "ordinary", Client: "cached"}},
			want:   map[string]string{},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			names, err := storageNamespaces(tc.mounts)
			if err != nil || !reflect.DeepEqual(names, tc.want) {
				t.Fatalf("storage namespace assignment: got %v, want %v: %v", names, tc.want, err)
			}
		})
	}
}

func TestStorageNamespacesRejectAmbiguousRawAssignments(t *testing.T) {
	for _, mounts := range [][]workerconfig.Mount{
		{{Name: "", Client: "cached", RemoteClient: "raw"}},
		{{Name: "files", RemoteClient: "raw-a"}, {Name: "files", RemoteClient: "raw-b"}},
	} {
		if _, err := storageNamespaces(mounts); !errors.Is(err, broker.ErrInvalid) {
			t.Fatalf("invalid raw namespace admitted: %v", err)
		}
	}
}
