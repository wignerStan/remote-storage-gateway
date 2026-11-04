//go:build linux && cgo

package main

import (
	"github.com/wignerStan/infra/go/plugins/native/provider"
	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/encoded"
	"github.com/wignerStan/network-data-plane-worker/go/encodedruntime/nativebridge"
	"github.com/wignerStan/remote-storage-gateway/go/plugins/transfer/reapi"
	"github.com/wignerStan/remote-storage-gateway/go/plugins/transfer/reapi/entry"
)

var buildDigest string

func init() {
	factory, err := entry.NewFactory(buildDigest)
	if err != nil {
		panic(err)
	}
	provider.Register(provider.Definition{Package: reapi.PluginID, Variant: encoded.Variant, New: func() (provider.Service, error) { return nativebridge.NewService(factory) }})
}
func main() {}
