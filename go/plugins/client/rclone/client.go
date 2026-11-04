// Package rcloneclient maps the Infra rclone filesystem adapter to the Worker's
// streaming client contract. This plugin always accesses its remote directly;
// optional materialization belongs to the independent VFS application.
package rcloneclient

import (
	"context"
	"encoding/base64"
	"errors"
	"io"
	"os"
	"strings"

	"github.com/wignerStan/infra/go/storage/cloud"
	port "github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/client"
)

type Client struct{ store *cloud.Store }

func failure(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, io.EOF):
		return io.EOF
	case errors.Is(err, context.Canceled):
		return context.Canceled
	case errors.Is(err, context.DeadlineExceeded):
		return context.DeadlineExceeded
	case errors.Is(err, os.ErrInvalid):
		return port.ErrInvalid
	case errors.Is(err, os.ErrNotExist):
		return port.ErrNotFound
	case errors.Is(err, errors.ErrUnsupported):
		return port.ErrUnsupported
	default:
		return port.ErrProvider
	}
}

func info(entry cloud.Entry) port.ObjectInfo {
	return port.ObjectInfo{Path: entry.Path, Size: entry.Size, IsDir: entry.IsDir, ModTime: entry.ModTime}
}

func (c *Client) Stat(ctx context.Context, ref port.ObjectRef) (port.ObjectInfo, error) {
	entry, err := c.store.Stat(ctx, ref.Path)
	return info(entry), failure(err)
}

// List owns Worker cursor and page semantics. The storage adapter returns a
// bounded, sorted directory inventory independently of the Worker wire format.
func (c *Client) List(ctx context.Context, request port.ListRequest) (port.ListResult, error) {
	if request.Limit < 0 || request.Limit > 1000 {
		return port.ListResult{}, port.ErrInvalid
	}
	if request.Limit == 0 {
		request.Limit = 100
	}
	after := ""
	if request.Cursor != "" {
		raw, err := base64.RawURLEncoding.DecodeString(request.Cursor)
		if err != nil || len(raw) > 4096 {
			return port.ListResult{}, port.ErrInvalid
		}
		after = string(raw)
	}
	entries, err := c.store.List(ctx, request.Path)
	if err != nil {
		return port.ListResult{}, failure(err)
	}
	result := port.ListResult{Objects: []port.ObjectInfo{}}
	for _, entry := range entries {
		if strings.TrimPrefix(entry.Path, "/") <= after {
			continue
		}
		if len(result.Objects) == request.Limit {
			last := strings.TrimPrefix(result.Objects[len(result.Objects)-1].Path, "/")
			result.NextCursor = base64.RawURLEncoding.EncodeToString([]byte(last))
			break
		}
		result.Objects = append(result.Objects, info(entry))
	}
	return result, nil
}

func (c *Client) Open(ctx context.Context, request port.OpenRequest) (port.ReadPlan, error) {
	body, entry, err := c.store.Open(ctx, request.Path, request.Offset, request.Length)
	if err != nil {
		return port.ReadPlan{}, failure(err)
	}
	return port.ReadPlan{Info: info(entry), Body: readBody{body: body}}, nil
}

type readBody struct{ body io.ReadCloser }

func (r readBody) Read(buffer []byte) (int, error) {
	n, err := r.body.Read(buffer)
	return n, failure(err)
}
func (r readBody) Close() error { return failure(r.body.Close()) }

func (c *Client) Put(ctx context.Context, request port.PutRequest) (port.PutResult, error) {
	entry, err := c.store.Put(ctx, request.Path, request.Body, request.Size, request.ContentType, request.Metadata)
	return port.PutResult{Info: info(entry)}, failure(err)
}

func (c *Client) Delete(ctx context.Context, ref port.ObjectRef) error {
	return failure(c.store.Delete(ctx, ref.Path))
}
func (c *Client) Close(ctx context.Context) error { return failure(c.store.Close(ctx)) }

var _ port.Client = (*Client)(nil)
