module github.com/wignerStan/remote-storage-gateway/go

go 1.27.3

require (
	cloud.google.com/go/longrunning v0.9.0
	github.com/bazelbuild/remote-apis v0.0.0-20260331222004-becdd8f9ff81
	github.com/containerd/ttrpc v1.2.10
	github.com/danielgtaylor/huma/v2 v2.40.0
	github.com/gin-gonic/gin v1.12.0
	github.com/rclone/rclone v1.75.2
	github.com/wignerStan/infra/go v0.0.0
	github.com/wignerStan/infra/go/network/rpc/protocols/actions v0.0.0
	github.com/wignerStan/infra/go/plugins/native v0.0.0
	github.com/wignerStan/infra/go/runtime/lifecycle v0.0.0
	github.com/wignerStan/network-data-plane-worker/go v0.3.1
	github.com/wignerStan/rclone/go v0.1.1
	golang.org/x/sync v0.24.0
	google.golang.org/genproto/googleapis/bytestream v0.0.0-20260526163538-3dc84a4a5aaa
	google.golang.org/grpc v1.85.0-dev.0.20260825072537-93e31b48545e
)
