// Package rcloneclient adapts rclone object storage to the Worker's streaming
// client port. Remote operations do not require a local spool or VFS cache.
package rcloneclient

import (
	"context"
	"encoding/base64"
	"errors"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/object"
	"github.com/rclone/rclone/vfs"
	"github.com/rclone/rclone/vfs/vfscommon"
	port "github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/client"
)

// Materialization opts into a private, generation-owned VFS read cache. Writes
// still commit directly to the remote; closing a VFS write-back handle is never
// presented as a completed remote upload. No stable host pathname is promised.
type Materialization struct {
	Directory     string `json:"directory" minLength:"1"`
	MaxBytes      int64  `json:"max_bytes" minimum:"1048576" doc:"VFS eviction target; active file ranges can temporarily exceed it."`
	MaxAgeSeconds int    `json:"max_age_seconds" minimum:"1" maximum:"31536000"`
}

type Client struct {
	remote         fs.Fs
	mu             sync.Mutex
	closing        bool
	active         sync.WaitGroup
	readers        map[*reader]struct{}
	ctx            context.Context
	cancel         context.CancelFunc
	backendCancel  context.CancelFunc
	done           chan struct{}
	closeErr       error
	view           *vfs.VFS
	cacheDirectory string
}

// New accepts a constructed backend. A nil materialization leaves all data on
// the remote and does not create a local cache directory.
func New(ctx context.Context, remote fs.Fs, materialization *Materialization) (*Client, error) {
	if remote == nil {
		return nil, port.ErrInvalid
	}
	lifetime, cancel := context.WithCancel(context.WithoutCancel(ctx))
	c := &Client{remote: remote, ctx: lifetime, cancel: cancel, done: make(chan struct{}), readers: map[*reader]struct{}{}}
	if materialization != nil {
		if !filepath.IsAbs(materialization.Directory) || materialization.MaxBytes < 1<<20 || materialization.MaxAgeSeconds < 1 || materialization.MaxAgeSeconds > 31536000 {
			cancel()
			return nil, port.ErrInvalid
		}
		if err := os.MkdirAll(materialization.Directory, 0700); err != nil {
			cancel()
			return nil, port.ErrProvider
		}
		directory, err := os.MkdirTemp(materialization.Directory, "read-cache-")
		if err != nil {
			cancel()
			return nil, port.ErrProvider
		}
		c.cacheDirectory = directory
		cacheCtx, _ := fs.AddConfig(lifetime)
		options := vfscommon.Opt
		options.CacheDir = directory
		options.ReadOnly = true
		options.CacheMode = vfscommon.CacheModeFull
		// Worker stream leases already define handle custody. A delayed VFS
		// handle-close timer must not outlive the generation and its directory.
		// Downloaded cache data remains reusable until normal eviction.
		options.HandleCaching = 0
		options.CacheMaxSize = fs.SizeSuffix(materialization.MaxBytes)
		options.CacheMaxAge = fs.Duration(time.Duration(materialization.MaxAgeSeconds) * time.Second)
		options.PollInterval = 0
		c.view = vfs.New(cacheCtx, remote, &options)
		if c.view.Opt.CacheMode != vfscommon.CacheModeFull {
			c.view.Shutdown()
			cancel()
			_ = os.RemoveAll(directory)
			return nil, port.ErrProvider
		}
	}
	return c, nil
}

func key(name string) (string, error) {
	if strings.ContainsAny(name, "\\\x00") {
		return "", port.ErrInvalid
	}
	for _, part := range strings.Split(name, "/") {
		if part == "." || part == ".." {
			return "", port.ErrInvalid
		}
	}
	return strings.Trim(path.Clean("/"+name), "/"), nil
}
func failure(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	if errors.Is(err, fs.ErrorObjectNotFound) || errors.Is(err, fs.ErrorDirNotFound) || errors.Is(err, os.ErrNotExist) {
		return port.ErrNotFound
	}
	return port.ErrProvider
}
func info(ctx context.Context, entry fs.DirEntry) port.ObjectInfo {
	_, directory := entry.(fs.Directory)
	return port.ObjectInfo{Path: "/" + strings.Trim(entry.Remote(), "/"), Size: entry.Size(), IsDir: directory, ModTime: entry.ModTime(ctx)}
}
func (c *Client) begin(ctx context.Context) (context.Context, func(), error) {
	c.mu.Lock()
	if c.closing {
		c.mu.Unlock()
		return nil, nil, port.ErrProvider
	}
	c.active.Add(1)
	c.mu.Unlock()
	operation, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(c.ctx, cancel)
	return operation, func() { stop(); cancel(); c.active.Done() }, nil
}
func (c *Client) Stat(ctx context.Context, ref port.ObjectRef) (port.ObjectInfo, error) {
	name, err := key(ref.Path)
	if err != nil {
		return port.ObjectInfo{}, err
	}
	ctx, finish, err := c.begin(ctx)
	if err != nil {
		return port.ObjectInfo{}, err
	}
	defer finish()
	if name == "" {
		return port.ObjectInfo{Path: "/", IsDir: true}, nil
	}
	value, err := c.remote.NewObject(ctx, name)
	if err == nil {
		return info(ctx, value), nil
	}
	if !errors.Is(err, fs.ErrorObjectNotFound) && !errors.Is(err, fs.ErrorNotAFile) {
		return port.ObjectInfo{}, failure(err)
	}
	parent := path.Dir(name)
	if parent == "." {
		parent = ""
	}
	entries, err := c.remote.List(ctx, parent)
	if err != nil {
		return port.ObjectInfo{}, failure(err)
	}
	for _, entry := range entries {
		if strings.Trim(entry.Remote(), "/") == name {
			return info(ctx, entry), nil
		}
	}
	return port.ObjectInfo{}, port.ErrNotFound
}
func (c *Client) List(ctx context.Context, request port.ListRequest) (port.ListResult, error) {
	name, err := key(request.Path)
	if err != nil {
		return port.ListResult{}, err
	}
	if request.Limit < 0 || request.Limit > 1000 {
		return port.ListResult{}, port.ErrInvalid
	}
	if request.Limit == 0 {
		request.Limit = 100
	}
	after := ""
	if request.Cursor != "" {
		raw, e := base64.RawURLEncoding.DecodeString(request.Cursor)
		if e != nil || len(raw) > 4096 {
			return port.ListResult{}, port.ErrInvalid
		}
		after = string(raw)
	}
	ctx, finish, err := c.begin(ctx)
	if err != nil {
		return port.ListResult{}, err
	}
	defer finish()
	entries, err := c.remote.List(ctx, name)
	if err != nil {
		return port.ListResult{}, failure(err)
	}
	if len(entries) > 10000 {
		return port.ListResult{}, port.ErrUnsupported
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Remote() < entries[j].Remote() })
	out := port.ListResult{Objects: []port.ObjectInfo{}}
	for _, entry := range entries {
		if entry.Remote() <= after {
			continue
		}
		if len(out.Objects) == request.Limit {
			out.NextCursor = base64.RawURLEncoding.EncodeToString([]byte(out.Objects[len(out.Objects)-1].Path[1:]))
			break
		}
		out.Objects = append(out.Objects, info(ctx, entry))
	}
	return out, nil
}

type reader struct {
	io.ReadCloser
	once     sync.Once
	finish   func()
	closeErr error
	cancel   context.CancelFunc
}

func (r *reader) Read(p []byte) (int, error) {
	n, err := r.ReadCloser.Read(p)
	if err == io.EOF {
		return n, err
	}
	return n, failure(err)
}

func (r *reader) Close() error {
	r.once.Do(func() {
		r.cancel()
		r.closeErr = failure(r.ReadCloser.Close())
		r.finish()
	})
	return r.closeErr
}
func (c *Client) Open(ctx context.Context, request port.OpenRequest) (port.ReadPlan, error) {
	name, err := key(request.Path)
	if err != nil || name == "" || request.Offset < 0 || request.Length < -1 {
		return port.ReadPlan{}, port.ErrInvalid
	}
	ctx, finish, err := c.begin(ctx)
	if err != nil {
		return port.ReadPlan{}, err
	}
	value, err := c.remote.NewObject(ctx, name)
	if err != nil {
		finish()
		return port.ReadPlan{}, failure(err)
	}
	ctx, cancelBody := context.WithCancel(ctx)
	var body io.ReadCloser
	if c.view != nil {
		handle, e := c.view.OpenFile(name, os.O_RDONLY, 0)
		err = e
		if err == nil {
			_, err = handle.Seek(request.Offset, io.SeekStart)
			if contextual, ok := handle.(contextualReadCloser); ok {
				body = &cachedReadCloser{ctx: ctx, handle: contextual}
			} else {
				err = port.ErrUnsupported
			}
			if err != nil {
				_ = handle.Close()
			}
		}
	} else {
		options := []fs.OpenOption{}
		if request.Offset != 0 || request.Length >= 0 {
			end := int64(-1)
			if request.Length > 0 {
				end = request.Offset + request.Length - 1
				if end < request.Offset {
					cancelBody()
					finish()
					return port.ReadPlan{}, port.ErrInvalid
				}
			}
			options = append(options, &fs.RangeOption{Start: request.Offset, End: end})
		}
		body, err = value.Open(ctx, options...)
	}
	if err != nil {
		cancelBody()
		finish()
		return port.ReadPlan{}, failure(err)
	}
	if request.Length >= 0 {
		body = &limitedReadCloser{Reader: io.LimitReader(body, request.Length), Closer: body}
	}
	r := &reader{ReadCloser: body, cancel: cancelBody}
	r.finish = func() { c.mu.Lock(); delete(c.readers, r); c.mu.Unlock(); finish() }
	c.mu.Lock()
	if c.closing {
		c.mu.Unlock()
		_ = r.Close()
		return port.ReadPlan{}, port.ErrProvider
	}
	c.readers[r] = struct{}{}
	c.mu.Unlock()
	return port.ReadPlan{Info: info(ctx, value), Body: r}, nil
}

type limitedReadCloser struct {
	io.Reader
	io.Closer
}

type contextualReadCloser interface {
	ReadContext(context.Context, []byte) (int, error)
	Close() error
}

type cachedReadCloser struct {
	ctx    context.Context
	handle contextualReadCloser
}

func (r *cachedReadCloser) Read(p []byte) (int, error) {
	return r.handle.ReadContext(r.ctx, p)
}

func (r *cachedReadCloser) Close() error { return r.handle.Close() }

func (c *Client) Put(ctx context.Context, request port.PutRequest) (port.PutResult, error) {
	name, err := key(request.Path)
	if err != nil || name == "" || request.Body == nil || request.Size < -1 {
		return port.PutResult{}, port.ErrInvalid
	}
	ctx, finish, err := c.begin(ctx)
	if err != nil {
		return port.PutResult{}, err
	}
	defer finish()
	source := object.NewStaticObjectInfo(name, time.Now(), request.Size, true, nil, c.remote).WithMimeType(request.ContentType).WithMetadata(fs.Metadata(request.Metadata))
	var result fs.Object
	if request.Size < 0 {
		put := c.remote.Features().PutStream
		if put == nil {
			return port.PutResult{}, port.ErrUnsupported
		}
		result, err = put(ctx, request.Body, source)
	} else {
		result, err = c.remote.Put(ctx, request.Body, source)
	}
	if err != nil {
		return port.PutResult{}, failure(err)
	}
	if c.view != nil {
		c.view.FlushDirCache()
	}
	return port.PutResult{Info: info(ctx, result)}, nil
}
func (c *Client) Delete(ctx context.Context, ref port.ObjectRef) error {
	name, err := key(ref.Path)
	if err != nil || name == "" {
		return port.ErrInvalid
	}
	ctx, finish, err := c.begin(ctx)
	if err != nil {
		return err
	}
	defer finish()
	value, err := c.remote.NewObject(ctx, name)
	if err != nil {
		return failure(err)
	}
	err = value.Remove(ctx)
	if err == nil && c.view != nil {
		c.view.FlushDirCache()
	}
	return failure(err)
}
func (c *Client) Close(ctx context.Context) error {
	c.mu.Lock()
	if !c.closing {
		c.closing = true
		c.cancel()
		if c.backendCancel != nil {
			c.backendCancel()
		}
		readers := make([]*reader, 0, len(c.readers))
		for r := range c.readers {
			readers = append(readers, r)
		}
		go func() {
			// VFS owns a detached cache/download context. Cancel it before
			// waiting for a reader whose Close may itself wait for that download.
			if c.view != nil {
				c.view.Shutdown()
			}
			for _, r := range readers {
				_ = r.Close()
			}
			c.active.Wait()
			if shutdown := c.remote.Features().Shutdown; shutdown != nil {
				c.closeErr = failure(shutdown(context.Background()))
			}
			if c.cacheDirectory != "" {
				if err := os.RemoveAll(c.cacheDirectory); err != nil && c.closeErr == nil {
					c.closeErr = port.ErrProvider
				}
			}
			close(c.done)
		}()
	}
	c.mu.Unlock()
	select {
	case <-c.done:
		return c.closeErr
	case <-ctx.Done():
		return ctx.Err()
	}
}
