// Package reapi implements transfer policy for official REAPI and ByteStream
// methods. Encoded protobuf payloads remain opaque host-owned frame leases.
package reapi

import (
	"context"
	"errors"

	remote "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/descriptor"
	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/descriptor/admission"
	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/encoded"
	bs "google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/protobuf/reflect/protoreflect"
)

const PluginID = "transfer.reapi"

type Configuration struct{}
type Factory struct {
	record  descriptor.Record
	methods map[string]encoded.Shape
}

func NewFactory(d descriptor.Descriptor) (*Factory, error) {
	if d.PluginID != PluginID || d.Contract != encoded.Contract {
		return nil, errors.New("invalid REAPI publication")
	}
	record, e := admission.Encode(d)
	if e != nil {
		return nil, e
	}
	f := &Factory{record: record, methods: map[string]encoded.Shape{}}
	f.add(remote.File_build_bazel_remote_execution_v2_remote_execution_proto)
	f.add(bs.File_google_bytestream_bytestream_proto)
	return f, nil
}
func (f *Factory) add(file protoreflect.FileDescriptor) {
	services := file.Services()
	for i := 0; i < services.Len(); i++ {
		s := services.Get(i)
		switch string(s.FullName()) {
		case "build.bazel.remote.execution.v2.Capabilities", "build.bazel.remote.execution.v2.ContentAddressableStorage", "build.bazel.remote.execution.v2.ActionCache", "build.bazel.remote.execution.v2.Execution", "google.bytestream.ByteStream":
		default:
			continue
		}
		methods := s.Methods()
		for j := 0; j < methods.Len(); j++ {
			m := methods.Get(j)
			f.methods["/"+string(s.FullName())+"/"+string(m.Name())] = encoded.Shape{ClientStreaming: m.IsStreamingClient(), ServerStreaming: m.IsStreamingServer()}
		}
	}
}
func (f *Factory) Descriptor() descriptor.Descriptor {
	r, _ := admission.Decode(f.record.Bytes)
	return r.Descriptor
}
func (f *Factory) New(ctx context.Context, start encoded.Start) (encoded.Session, error) {
	shape, ok := f.methods[start.Request.Method]
	if !ok {
		return nil, &encoded.Failure{Code: 12, Message: "RPC method unsupported"}
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	// The carrier admits and expires the grant using its own clock. Plugin
	// policy consumes that admitted value without acquiring a second clock.
	if start.ID == 0 || start.Grant.UpstreamID == "" || start.Grant.ExpiresUnixNano <= 0 || start.Grant.MaxMessageBytes <= 0 || start.Grant.MaxInflightBytes < start.Grant.MaxMessageBytes {
		return nil, &encoded.Failure{Code: 7, Message: "invalid transfer grant"}
	}
	if len(start.Configuration) > 0 {
		var cfg Configuration
		if admission.StrictJSON(start.Configuration, &cfg) != nil {
			return nil, &encoded.Failure{Code: 3, Message: "invalid transfer configuration"}
		}
	}
	return &session{generation: start.ID, shape: shape, maxBytes: start.Grant.MaxMessageBytes, pending: map[uint64]encoded.Action{}}, nil
}
