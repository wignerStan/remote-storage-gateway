package httpapi

import (
	"context"
	"github.com/wignerStan/network-data-plane-worker/go/gateway"
	"golang.org/x/net/webdav"
	"os"
)

// This adapter only binds the private workflow's Go file value to the entry's
// WebDAV representation. Upload tasks, grants and drain stay with the workflow.
type filesystem struct{ *gateway.Filesystem }

func (f *filesystem) OpenFile(ctx context.Context, name string, flags int, mode os.FileMode) (webdav.File, error) {
	return f.Filesystem.OpenFile(ctx, name, flags, mode)
}
