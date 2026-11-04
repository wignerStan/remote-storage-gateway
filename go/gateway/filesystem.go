package gateway

import (
	"context"
	"errors"
	"io"
	"os"
	"path"
	"sync"
	"time"

	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/client"
)

// Filesystem owns the namespace file workflow, including upload tasks and drain.
// Its API uses Go filesystem values; the entry maps them to the WebDAV contract.
type Filesystem struct{ service *Runtime }

type exchangeKey struct{}
type exchange struct {
	flow        *Exchange
	size        int64
	contentType string
}

func BindExchange(ctx context.Context, flow *Exchange, size int64, contentType string) context.Context {
	return context.WithValue(ctx, exchangeKey{}, &exchange{flow: flow, size: size, contentType: contentType})
}
func NewFilesystem(service *Runtime) *Filesystem { return &Filesystem{service: service} }

func (f *Filesystem) selected(ctx context.Context, name string) (Mount, string, client.Client, error) {
	m, p, e := f.service.Resolve(name)
	if e != nil {
		return m, p, nil, e
	}
	if m.Name == "" {
		return m, p, nil, nil
	}
	x, _ := ctx.Value(exchangeKey{}).(*exchange)
	if x == nil || x.flow == nil {
		return m, p, nil, os.ErrPermission
	}
	c, e := x.flow.ClientFor(m.Name)
	return m, p, c, e
}
func (f *Filesystem) Mkdir(context.Context, string, os.FileMode) error { return os.ErrPermission }
func (f *Filesystem) Rename(context.Context, string, string) error     { return os.ErrPermission }
func storageError(e error) error {
	if errors.Is(e, client.ErrNotFound) {
		return os.ErrNotExist
	}
	if errors.Is(e, client.ErrInvalid) {
		return os.ErrInvalid
	}
	if errors.Is(e, client.ErrUnsupported) {
		return os.ErrPermission
	}
	return e
}
func (f *Filesystem) Stat(ctx context.Context, name string) (os.FileInfo, error) {
	m, p, c, e := f.selected(ctx, name)
	if e != nil {
		return nil, e
	}
	if m.Name == "" {
		return info{client.ObjectInfo{Path: "/", IsDir: true}}, nil
	}
	v, e := c.Stat(ctx, client.ObjectRef{Path: p})
	if e != nil {
		return nil, storageError(e)
	}
	if p == "/" {
		v.Path = m.Name
	}
	return info{v}, nil
}
func (f *Filesystem) RemoveAll(ctx context.Context, name string) error {
	m, p, c, e := f.selected(ctx, name)
	if e != nil {
		return e
	}
	if m.Name == "" || p == "/" || m.ReadOnly {
		return os.ErrPermission
	}
	v, e := c.Stat(ctx, client.ObjectRef{Path: p})
	if e != nil {
		return storageError(e)
	}
	// The Client port's delete operation does not imply recursive tree deletion.
	if v.IsDir {
		return os.ErrPermission
	}
	return storageError(c.Delete(ctx, client.ObjectRef{Path: p}))
}
func (f *Filesystem) OpenFile(ctx context.Context, name string, flags int, _ os.FileMode) (*file, error) {
	m, p, c, e := f.selected(ctx, name)
	if e != nil {
		return nil, e
	}
	ctx, cancel := context.WithCancel(ctx)
	file := &file{ctx: ctx, cancel: cancel, client: c, object: p, fs: f, root: m.Name == "", info: client.ObjectInfo{Path: p}}
	write := flags&(os.O_WRONLY|os.O_RDWR) != 0
	if write {
		if m.Name == "" || p == "/" || m.ReadOnly || flags&os.O_APPEND != 0 || flags&os.O_TRUNC == 0 {
			cancel()
			return nil, os.ErrPermission
		}
		x, _ := ctx.Value(exchangeKey{}).(*exchange)
		if x == nil {
			cancel()
			return nil, os.ErrPermission
		}
		reader, writer := io.Pipe()
		file.writer = writer
		file.putDone = make(chan putOutcome, 1)
		file.stop = context.AfterFunc(ctx, func() { _ = writer.CloseWithError(ctx.Err()) })
		go func() {
			v, e := c.Put(ctx, client.PutRequest{Path: p, Body: reader, Size: x.size, ContentType: x.contentType})
			_ = reader.CloseWithError(e)
			file.putDone <- putOutcome{v, e}
		}()
		return file, nil
	}
	if m.Name == "" {
		file.info.IsDir = true
		return file, nil
	}
	v, e := c.Stat(ctx, client.ObjectRef{Path: p})
	if e != nil {
		cancel()
		return nil, storageError(e)
	}
	file.info = v
	return file, nil
}

type info struct{ v client.ObjectInfo }

func (i info) Name() string { return path.Base(i.v.Path) }
func (i info) Size() int64  { return i.v.Size }
func (i info) Mode() os.FileMode {
	if i.v.IsDir {
		return os.ModeDir | 0555
	}
	return 0644
}
func (i info) ModTime() time.Time { return i.v.ModTime }
func (i info) IsDir() bool        { return i.v.IsDir }
func (i info) Sys() any           { return nil }

type putOutcome struct {
	result client.PutResult
	err    error
}
type file struct {
	ctx      context.Context
	cancel   context.CancelFunc
	client   client.Client
	fs       *Filesystem
	object   string
	root     bool
	info     client.ObjectInfo
	reader   io.ReadCloser
	writer   *io.PipeWriter
	putDone  chan putOutcome
	stop     func() bool
	position int64
	entries  []os.FileInfo
	listed   bool
	mu       sync.Mutex
	close    sync.Once
	closeErr error
}

func (f *file) Read(p []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.info.IsDir || f.writer != nil {
		return 0, os.ErrPermission
	}
	if len(p) == 0 {
		return 0, nil
	}
	if f.position >= f.info.Size {
		return 0, io.EOF
	}
	if f.reader == nil {
		v, e := f.client.Open(f.ctx, client.OpenRequest{Path: f.object, Offset: f.position, Length: -1})
		if e != nil {
			return 0, storageError(e)
		}
		if v.Body == nil {
			return 0, client.ErrProvider
		}
		f.reader = v.Body
	}
	n, e := f.reader.Read(p)
	f.position += int64(n)
	return n, e
}
func (f *file) Write(p []byte) (int, error) {
	if f.writer == nil {
		return 0, os.ErrPermission
	}
	n, e := f.writer.Write(p)
	f.mu.Lock()
	f.info.Size += int64(n)
	f.mu.Unlock()
	return n, e
}
func (f *file) Seek(offset int64, whence int) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.writer != nil || f.info.IsDir {
		return 0, os.ErrPermission
	}
	var base int64
	switch whence {
	case io.SeekStart:
	case io.SeekCurrent:
		base = f.position
	case io.SeekEnd:
		base = f.info.Size
	default:
		return 0, os.ErrInvalid
	}
	if offset < -base || offset > 0 && base > int64(^uint64(0)>>1)-offset {
		return 0, os.ErrInvalid
	}
	position := base + offset
	if position != f.position && f.reader != nil {
		if e := f.reader.Close(); e != nil {
			return 0, e
		}
		f.reader = nil
	}
	f.position = position
	return position, nil
}
func (f *file) Stat() (os.FileInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return info{f.info}, nil
}
func (f *file) Readdir(count int) ([]os.FileInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.info.IsDir {
		return nil, os.ErrInvalid
	}
	if !f.listed {
		if f.root {
			for _, m := range f.fs.service.Mounts() {
				f.entries = append(f.entries, info{client.ObjectInfo{Path: m.Name, IsDir: true}})
			}
		} else {
			cursor := ""
			seen := map[string]bool{}
			for {
				v, e := f.client.List(f.ctx, client.ListRequest{Path: f.object, Cursor: cursor})
				if e != nil {
					return nil, storageError(e)
				}
				for _, object := range v.Objects {
					f.entries = append(f.entries, info{object})
				}
				if len(f.entries) > 10000 {
					return nil, client.ErrUnsupported
				}
				if v.NextCursor == "" {
					break
				}
				if v.NextCursor == cursor || seen[v.NextCursor] {
					return nil, client.ErrInvalid
				}
				seen[v.NextCursor] = true
				cursor = v.NextCursor
			}
		}
		f.listed = true
	}
	if count <= 0 {
		v := f.entries
		f.entries = nil
		return v, nil
	}
	if len(f.entries) == 0 {
		return nil, io.EOF
	}
	n := min(count, len(f.entries))
	v := f.entries[:n]
	f.entries = f.entries[n:]
	return v, nil
}
func (f *file) Close() error {
	f.close.Do(func() {
		if f.writer != nil {
			_ = f.writer.Close()
			outcome := <-f.putDone
			f.closeErr = storageError(outcome.err)
			f.mu.Lock()
			if outcome.err == nil {
				f.info = outcome.result.Info
			}
			f.mu.Unlock()
			if f.stop != nil {
				f.stop()
			}
			f.cancel()
			return
		}
		f.cancel()
		f.mu.Lock()
		defer f.mu.Unlock()
		if f.reader != nil {
			f.closeErr = f.reader.Close()
		}
	})
	return f.closeErr
}
