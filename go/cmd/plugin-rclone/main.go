// Command plugin-rclone is built as a dynamically loaded native library.
package main

import (
	"github.com/wignerStan/network-data-plane-worker/go/nativeclient/provider"
	"github.com/wignerStan/remote-storage-gateway/go/plugins/client/rclone/entry"
)

var buildDigest string

func init() {
	factory, err := rclone.NewFactory(buildDigest)
	if err != nil {
		return
	}
	provider.RegisterFactory(factory)
}
func main() {}
