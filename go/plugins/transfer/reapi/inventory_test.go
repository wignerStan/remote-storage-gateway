package reapi

import (
	"context"
	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/encoded"
	"testing"
)

func TestPinnedOfficialMethodInventory(t *testing.T) {
	expected := map[string]encoded.Shape{
		"/build.bazel.remote.execution.v2.Capabilities/GetCapabilities":               {},
		"/build.bazel.remote.execution.v2.ActionCache/GetActionResult":                {},
		"/build.bazel.remote.execution.v2.ActionCache/UpdateActionResult":             {},
		"/build.bazel.remote.execution.v2.ContentAddressableStorage/FindMissingBlobs": {},
		"/build.bazel.remote.execution.v2.ContentAddressableStorage/BatchUpdateBlobs": {},
		"/build.bazel.remote.execution.v2.ContentAddressableStorage/BatchReadBlobs":   {},
		"/build.bazel.remote.execution.v2.ContentAddressableStorage/GetTree":          {ServerStreaming: true},
		"/build.bazel.remote.execution.v2.ContentAddressableStorage/SplitBlob":        {},
		"/build.bazel.remote.execution.v2.ContentAddressableStorage/SpliceBlob":       {},
		"/build.bazel.remote.execution.v2.Execution/Execute":                          {ServerStreaming: true},
		"/build.bazel.remote.execution.v2.Execution/WaitExecution":                    {ServerStreaming: true},
		"/google.bytestream.ByteStream/Read":                                          {ServerStreaming: true},
		"/google.bytestream.ByteStream/Write":                                         {ClientStreaming: true},
		"/google.bytestream.ByteStream/QueryWriteStatus":                              {},
		"/google.longrunning.Operations/GetOperation":                                 {},
	}
	f := factory(t)
	if len(f.methods) != len(expected) {
		t.Fatalf("method exposure changed: got %d want %d", len(f.methods), len(expected))
	}
	for method, shape := range expected {
		if got, ok := f.methods[method]; !ok || got != shape {
			t.Errorf("%s shape=%+v exists=%v", method, got, ok)
		}
	}
	for _, method := range []string{"/google.longrunning.Operations/CancelOperation", "/google.longrunning.Operations/DeleteOperation", "/google.longrunning.Operations/ListOperations", "/google.longrunning.Operations/WaitOperation", "/build.bazel.remote.execution.v2.ContentAddressableStorage/GetChunkMapping", "/build.bazel.remote.execution.v2.ContentAddressableStorage/RegisterChunkMapping"} {
		if _, e := f.New(context.Background(), start(method)); e == nil {
			t.Errorf("unadvertised method admitted: %s", method)
		}
	}
}
