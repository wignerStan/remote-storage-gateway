// Package vfs owns the independent materializer namespace and read-only flow.
package vfs

import (
	"context"
	"crypto/subtle"
	"errors"
	"github.com/wignerStan/infra/go/storage/cloud"
	"github.com/wignerStan/remote-storage-gateway/go/contracts/control/broker"
	config "github.com/wignerStan/remote-storage-gateway/go/contracts/vfs"
	"io"
	"net"
	"net/url"
	"os"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var ErrUnavailable = errors.New("materializer unavailable")

type Service struct {
	store                      *cloud.Store
	observe                    func(context.Context) (broker.InstanceInfo, error)
	workerID, origin, username string
	password                   []byte
	mu                         sync.RWMutex
	closing                    atomic.Bool
}

// New binds a pre-admitted Data Plane store to a private read-only namespace.
func New(c config.ProcessConfig, store *cloud.Store, observe func(context.Context) (broker.InstanceInfo, error), password []byte) (*Service, error) {
	if store == nil || observe == nil || c.InstanceID == "" || c.WorkerInstanceID == "" || c.LocalUsername == "" || len(password) < 16 {
		return nil, broker.ErrInvalid
	}
	u, err := url.Parse(c.DataEndpoint)
	if err != nil {
		return nil, broker.ErrInvalid
	}
	return &Service{store: store, observe: observe, workerID: c.WorkerInstanceID, origin: u.Scheme + "://" + strings.ToLower(u.Host), username: c.LocalUsername, password: append([]byte(nil), password...)}, nil
}
func (s *Service) CheckOrigin(ctx context.Context) error {
	if s.closing.Load() {
		return ErrUnavailable
	}
	info, err := s.observe(ctx)
	if err != nil || !info.Ready || info.ID != s.workerID || info.Contract != broker.Contract {
		return ErrUnavailable
	}
	u, err := url.Parse(info.DataEndpoint)
	if err != nil || u.Scheme+"://"+strings.ToLower(u.Host) != s.origin {
		return ErrUnavailable
	}
	return nil
}
func (s *Service) Authorize(ctx context.Context, username, password string) (bool, error) {
	s.mu.RLock()
	ok := subtle.ConstantTimeCompare([]byte(username), []byte(s.username)) == 1 && subtle.ConstantTimeCompare([]byte(password), s.password) == 1
	s.mu.RUnlock()
	if !ok {
		return false, nil
	}
	if err := s.CheckOrigin(ctx); err != nil {
		return false, err
	}
	return true, nil
}
func (s *Service) Close(ctx context.Context) error {
	s.closing.Store(true)
	s.mu.Lock()
	clear(s.password)
	s.mu.Unlock()
	return s.store.Close(ctx)
}
func (s *Service) OpenFile(ctx context.Context, name string, flags int, _ os.FileMode) (*File, error) {
	if flags&(os.O_WRONLY|os.O_RDWR|os.O_CREATE|os.O_APPEND|os.O_TRUNC) != 0 {
		return nil, os.ErrPermission
	}
	if s.closing.Load() {
		return nil, net.ErrClosed
	}
	if strings.ContainsAny(name, "\\\x00") {
		return nil, os.ErrInvalid
	}
	for _, segment := range strings.Split(name, "/") {
		if segment == ".." || segment == "." {
			return nil, os.ErrPermission
		}
	}
	name = strings.Trim(path.Clean("/"+name), "/")
	entry, err := s.store.Stat(ctx, name)
	if err != nil {
		return nil, err
	}
	lifetime, cancel := context.WithCancel(ctx)
	var body io.ReadCloser
	if !entry.IsDir {
		body, entry, err = s.store.Open(lifetime, name, 0, -1)
		if err != nil {
			cancel()
			return nil, err
		}
	}
	return &File{service: s, ctx: lifetime, cancel: cancel, name: name, entry: entry, body: body}, nil
}
func (s *Service) Stat(ctx context.Context, name string) (os.FileInfo, error) {
	if s.closing.Load() {
		return nil, net.ErrClosed
	}
	if strings.ContainsAny(name, "\\\x00") {
		return nil, os.ErrInvalid
	}
	for _, segment := range strings.Split(name, "/") {
		if segment == ".." || segment == "." {
			return nil, os.ErrPermission
		}
	}
	entry, err := s.store.Stat(ctx, strings.Trim(path.Clean("/"+name), "/"))
	if err != nil {
		return nil, err
	}
	return fileInfo{entry}, nil
}
func (*Service) Mkdir(context.Context, string, os.FileMode) error { return os.ErrPermission }
func (*Service) RemoveAll(context.Context, string) error          { return os.ErrPermission }
func (*Service) Rename(context.Context, string, string) error     { return os.ErrPermission }

type File struct {
	service         *Service
	ctx             context.Context
	cancel          context.CancelFunc
	name            string
	entry           cloud.Entry
	mu              sync.Mutex
	body            io.ReadCloser
	offset          int64
	closed          bool
	entries         []os.FileInfo
	directoryOffset int
}

func (f *File) Read(data []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return 0, net.ErrClosed
	}
	if f.entry.IsDir {
		return 0, os.ErrInvalid
	}
	if f.body == nil {
		body, _, err := f.service.store.Open(f.ctx, f.name, f.offset, -1)
		if err != nil {
			return 0, err
		}
		f.body = body
	}
	n, err := f.body.Read(data)
	f.offset += int64(n)
	return n, err
}
func (*File) Write([]byte) (int, error) { return 0, os.ErrPermission }
func (f *File) Seek(offset int64, whence int) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return 0, net.ErrClosed
	}
	var base int64
	switch whence {
	case io.SeekStart:
	case io.SeekCurrent:
		base = f.offset
	case io.SeekEnd:
		if f.entry.Size < 0 {
			return 0, os.ErrInvalid
		}
		base = f.entry.Size
	default:
		return 0, os.ErrInvalid
	}
	position := base + offset
	if position < 0 || (offset > 0 && position < base) {
		return 0, os.ErrInvalid
	}
	if position != f.offset && !f.entry.IsDir {
		if f.body != nil {
			_ = f.body.Close()
			f.body = nil
		}
		// End/zero seeks used by HTTP metadata framing do not prefetch data.
		if position > 0 && position < f.entry.Size {
			body, _, err := f.service.store.Open(f.ctx, f.name, position, -1)
			if err != nil {
				return f.offset, err
			}
			f.body = body
		}
	}
	f.offset = position
	return position, nil
}
func (f *File) Close() error {
	f.cancel()
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return nil
	}
	f.closed = true
	if f.body != nil {
		return f.body.Close()
	}
	return nil
}
func (f *File) Stat() (os.FileInfo, error) { return fileInfo{f.entry}, nil }
func (f *File) Readdir(count int) ([]os.FileInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return nil, net.ErrClosed
	}
	if !f.entry.IsDir {
		return nil, os.ErrInvalid
	}
	if f.entries == nil {
		entries, err := f.service.store.List(f.ctx, f.name)
		if err != nil {
			return nil, err
		}
		f.entries = make([]os.FileInfo, 0, len(entries))
		for _, entry := range entries {
			f.entries = append(f.entries, fileInfo{entry})
		}
	}
	if f.directoryOffset >= len(f.entries) {
		if count > 0 {
			return nil, io.EOF
		}
		return []os.FileInfo{}, nil
	}
	end := len(f.entries)
	if count > 0 && end-f.directoryOffset > count {
		end = f.directoryOffset + count
	}
	out := append([]os.FileInfo(nil), f.entries[f.directoryOffset:end]...)
	f.directoryOffset = end
	return out, nil
}

type fileInfo struct{ entry cloud.Entry }

func (f fileInfo) Name() string { return path.Base(f.entry.Path) }
func (f fileInfo) Size() int64  { return f.entry.Size }
func (f fileInfo) Mode() os.FileMode {
	if f.entry.IsDir {
		return os.ModeDir | 0555
	}
	return 0444
}
func (f fileInfo) ModTime() time.Time { return f.entry.ModTime }
func (f fileInfo) IsDir() bool        { return f.entry.IsDir }
func (fileInfo) Sys() any             { return nil }
