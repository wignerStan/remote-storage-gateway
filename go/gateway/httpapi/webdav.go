package httpapi

import (
	"context"
	"github.com/wignerStan/remote-storage-gateway/go/gateway"
	"golang.org/x/net/webdav"
	"mime"
	"os"
	"path"
)

// This adapter only binds the private workflow's Go file value to the entry's
// WebDAV representation. Upload tasks, grants and drain stay with the workflow.
type filesystem struct{ *gateway.Filesystem }

func (f *filesystem) OpenFile(ctx context.Context, name string, flags int, mode os.FileMode) (webdav.File, error) {
	file, err := f.Filesystem.OpenFile(ctx, name, flags, mode)
	if err != nil {
		return nil, err
	}
	return &davFile{File: file}, nil
}

func (f *filesystem) Stat(ctx context.Context, name string) (os.FileInfo, error) {
	info, err := f.Filesystem.Stat(ctx, name)
	if err != nil {
		return nil, err
	}
	return metadata{info}, nil
}

// PROPFIND describes namespace metadata without fetching every child payload.
type metadata struct{ os.FileInfo }

func (m metadata) ContentType(context.Context) (string, error) { return contentType(m.Name()), nil }
func contentType(name string) string {
	value := mime.TypeByExtension(path.Ext(name))
	if value == "" {
		value = "application/octet-stream"
	}
	return value
}

type davFile struct{ webdav.File }

func (f *davFile) Stat() (os.FileInfo, error) {
	info, err := f.File.Stat()
	if err != nil {
		return nil, err
	}
	return metadata{info}, nil
}
func (f *davFile) Readdir(count int) ([]os.FileInfo, error) {
	entries, err := f.File.Readdir(count)
	for i := range entries {
		entries[i] = metadata{entries[i]}
	}
	return entries, err
}
