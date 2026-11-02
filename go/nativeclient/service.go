package nativeclient

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/client"
	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/descriptor"
	"io"
	"sync"
	"time"
)

// Service is embedded in an independently built .so. Its construction is
// passive. Provider construction occurs only through prepare/validate/publish.
type Service struct {
	mu             sync.Mutex
	phase          sync.Mutex
	factory        client.Factory
	record         descriptor.Record
	root           context.Context
	cancel         context.CancelFunc
	closing        bool
	calls          sync.WaitGroup
	candidate      client.Candidate
	instance       client.Client
	secrets        *secretSet
	validated      bool
	readers        map[uint64]*readOwner
	uploads        map[uint64]*putOwner
	fenced         map[uint64]time.Time
	done           chan struct{}
	outcome        error
	implementation descriptor.Implementation
}
type readOwner struct {
	mu     sync.Mutex
	readMu sync.Mutex
	ready  chan struct{}
	plan   client.ReadPlan
	cancel context.CancelFunc
	closed bool
	once   sync.Once
	err    error
}
type putOwner struct {
	mu        sync.Mutex
	reader    *io.PipeReader
	writer    *io.PipeWriter
	cancel    context.CancelFunc
	done      chan struct{}
	result    client.PutResult
	err       error
	committed bool
	expected  int64
	written   int64
}

func NewService(f client.Factory) (*Service, error) {
	if f == nil {
		return nil, client.ErrInvalid
	}
	r, e := descriptor.Encode(f.Descriptor())
	if e != nil || r.Descriptor.Contract != client.Contract {
		return nil, client.ErrInvalid
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Service{factory: f, record: r, root: ctx, cancel: cancel, readers: map[uint64]*readOwner{}, uploads: map[uint64]*putOwner{}, fenced: map[uint64]time.Time{}}, nil
}
func (s *Service) Call(ctx context.Context, method string, input []byte) ([]byte, error) {
	if e := ctx.Err(); e != nil {
		return nil, e
	}
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		return nil, client.ErrInvalid
	}
	s.calls.Add(1)
	s.mu.Unlock()
	defer s.calls.Done()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stopRoot := context.AfterFunc(s.root, cancel)
	defer stopRoot()
	switch method {
	case Describe:
		if len(input) > 0 && string(input) != "{}" {
			return nil, client.ErrInvalid
		}
		return append([]byte(nil), s.record.Bytes...), nil
	case Prepare, Validate, Publish, Abort:
		return s.lifecycle(ctx, method, input)
	case Stat:
		var r client.ObjectRef
		if e := descriptor.StrictJSON(input, &r); e != nil {
			return nil, client.ErrInvalid
		}
		c, e := s.live("stat")
		if e != nil {
			return nil, e
		}
		v, e := c.Stat(ctx, r)
		return marshalResult(v, e)
	case List:
		var r client.ListRequest
		if e := descriptor.StrictJSON(input, &r); e != nil {
			return nil, client.ErrInvalid
		}
		if r.Limit < 0 || r.Limit > 4096 {
			return nil, client.ErrInvalid
		}
		c, e := s.live("list")
		if e != nil {
			return nil, e
		}
		v, e := c.List(ctx, r)
		if len(v.Objects) > 4096 {
			return nil, client.ErrInvalid
		}
		return marshalResult(v, e)
	case Delete:
		var r client.ObjectRef
		if e := descriptor.StrictJSON(input, &r); e != nil {
			return nil, client.ErrInvalid
		}
		c, e := s.live("delete")
		if e != nil {
			return nil, e
		}
		return marshalResult(struct{}{}, c.Delete(ctx, r))
	case Open:
		return s.open(ctx, input)
	case Read:
		return s.read(ctx, input)
	case ReadClose:
		var r StreamFrame
		if e := descriptor.StrictJSON(input, &r); e != nil {
			return nil, client.ErrInvalid
		}
		return marshalResult(struct{}{}, s.closeReader(r.ID))
	case PutBegin:
		return s.beginPut(ctx, input)
	case PutWrite:
		return s.write(ctx, input)
	case PutCommit:
		return s.commitPut(ctx, input)
	case PutAbort:
		var r StreamFrame
		if e := descriptor.StrictJSON(input, &r); e != nil {
			return nil, client.ErrInvalid
		}
		return marshalResult(struct{}{}, s.abortPut(r.ID))
	default:
		return nil, client.ErrUnsupported
	}
}
func marshalResult(v any, e error) ([]byte, error) {
	if e != nil {
		return nil, e
	}
	b, e := json.Marshal(v)
	if len(b) > descriptor.MaxBytes {
		return nil, client.ErrInvalid
	}
	return b, e
}
func (s *Service) lifecycle(ctx context.Context, method string, input []byte) ([]byte, error) {
	s.phase.Lock()
	defer s.phase.Unlock()
	switch method {
	case Prepare:
		if s.candidate != nil || s.instance != nil {
			return nil, client.ErrInvalid
		}
		var r PrepareRequest
		// Strict decoding can populate secrets before rejecting a later field.
		// Erase all decoded plaintext on every path, including validation failure.
		defer func() {
			for k, v := range r.Secrets {
				clear(v)
				delete(r.Secrets, k)
			}
		}()
		if descriptor.StrictJSON(input, &r) != nil {
			return nil, client.ErrInvalid
		}
		a, ok := s.record.Descriptor.Implementation(r.ImplementationID)
		if !ok {
			return nil, client.ErrInvalid
		}
		refs := map[string]string{}
		for name, v := range r.Secrets {
			if len(v) > 64<<10 {
				return nil, client.ErrInvalid
			}
			refs[name] = "resolved"
		}
		if descriptor.ValidateConfig(a, r.Public, refs) != nil {
			return nil, client.ErrInvalid
		}
		ss := &secretSet{values: map[string][]byte{}, declared: map[string]bool{}}
		for name, value := range r.Secrets {
			ss.values[name] = append([]byte(nil), value...)
		}
		for _, f := range a.ConfigSchema.Fields {
			if f.Secret {
				ss.declared[f.Path] = true
			}
		}
		c, e := s.factory.Prepare(ctx, client.PluginConfig{ImplementationID: r.ImplementationID, Public: r.Public, Secrets: ss})
		if e != nil {
			ss.close()
			return nil, e
		}
		if c == nil {
			ss.close()
			return nil, client.ErrInvalid
		}
		s.candidate = c
		s.secrets = ss
		s.implementation = a
	case Validate:
		if s.candidate == nil {
			return nil, client.ErrInvalid
		}
		s.validated = false
		if e := s.candidate.Validate(ctx); e != nil {
			return nil, e
		}
		s.validated = true
	case Publish:
		if !s.validated || s.candidate == nil || s.instance != nil {
			return nil, client.ErrInvalid
		}
		c, e := s.candidate.Publish()
		if e != nil {
			return nil, e
		}
		if c == nil {
			return nil, client.ErrInvalid
		}
		s.instance = c
		s.candidate = nil
		s.secrets.close()
		s.secrets = nil
	case Abort:
		if s.candidate != nil {
			e := s.candidate.Abort(ctx)
			if e != nil {
				return nil, e
			}
			s.candidate = nil
		}
		if s.secrets != nil {
			s.secrets.close()
			s.secrets = nil
		}
	}
	return []byte(`{}`), nil
}
func (s *Service) live(cap string) (client.Client, error) {
	s.phase.Lock()
	defer s.phase.Unlock()
	if s.instance == nil {
		return nil, client.ErrInvalid
	}
	for _, v := range s.implementation.Capabilities {
		if v == cap {
			return s.instance, nil
		}
	}
	return nil, client.ErrUnsupported
}
func (s *Service) allowed(cap string) bool {
	for _, v := range s.implementation.Capabilities {
		if v == cap {
			return true
		}
	}
	return false
}
func (s *Service) streamContext(deadline int64) (context.Context, context.CancelFunc, error) {
	now := time.Now()
	d := time.Unix(0, deadline)
	if deadline == 0 || d.After(now.Add(24*time.Hour)) || !d.After(now) {
		return nil, nil, client.ErrInvalid
	}
	ctx, c := context.WithDeadline(s.root, d)
	return ctx, c, nil
}
func (s *Service) admitIDLocked(id uint64) error {
	if id == 0 || s.closing || len(s.readers)+len(s.uploads) >= MaxStreams {
		return client.ErrInvalid
	}
	for k, expiry := range s.fenced {
		if time.Now().After(expiry) {
			delete(s.fenced, k)
		}
	}
	if _, ok := s.fenced[id]; ok {
		return client.ErrInvalid
	}
	if s.readers[id] != nil || s.uploads[id] != nil {
		return client.ErrInvalid
	}
	return nil
}
func (s *Service) open(ctx context.Context, input []byte) ([]byte, error) {
	var r OpenFrame
	if descriptor.StrictJSON(input, &r) != nil || r.Request.Offset < 0 || r.Request.Length < -1 {
		return nil, client.ErrInvalid
	}
	c, e := s.live("open")
	if e != nil {
		return nil, e
	}
	if (r.Request.Offset != 0 || r.Request.Length >= 0) && !s.allowed("range") {
		return nil, client.ErrUnsupported
	}
	stream, cancel, e := s.streamContext(r.Deadline)
	if e != nil {
		return nil, e
	}
	o := &readOwner{ready: make(chan struct{}), cancel: cancel}
	s.mu.Lock()
	e = s.admitIDLocked(r.ID)
	if e == nil {
		s.readers[r.ID] = o
	}
	s.mu.Unlock()
	if e != nil {
		cancel()
		return nil, e
	}
	stop := context.AfterFunc(ctx, cancel)
	plan, e := c.Open(stream, r.Request)
	stop()
	o.mu.Lock()
	o.plan = plan
	close(o.ready)
	o.mu.Unlock()
	if e != nil || ctx.Err() != nil || plan.Body == nil {
		_ = s.closeReader(r.ID)
		if e != nil {
			return nil, e
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, client.ErrInvalid
	}
	o.mu.Lock()
	closed := o.closed
	o.mu.Unlock()
	if closed {
		return nil, context.Canceled
	}
	return marshalResult(OpenResult{plan.Info}, nil)
}
func (s *Service) read(ctx context.Context, input []byte) ([]byte, error) {
	var r StreamFrame
	if descriptor.StrictJSON(input, &r) != nil || r.Credit < 1 || r.Credit > ChunkBytes {
		return nil, client.ErrInvalid
	}
	s.mu.Lock()
	o := s.readers[r.ID]
	s.mu.Unlock()
	if o == nil {
		return nil, client.ErrNotFound
	}
	select {
	case <-o.ready:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	o.readMu.Lock()
	defer o.readMu.Unlock()
	o.mu.Lock()
	closed, body := o.closed, o.plan.Body
	o.mu.Unlock()
	if closed || body == nil {
		return nil, client.ErrInvalid
	}
	stop := context.AfterFunc(ctx, func() { _ = o.close() })
	defer stop()
	b := make([]byte, r.Credit+1)
	n, e := body.Read(b[1:])
	if e != nil && !errors.Is(e, io.EOF) {
		return nil, e
	}
	if n == 0 && e == nil {
		return nil, io.ErrNoProgress
	}
	if errors.Is(e, io.EOF) {
		b[0] = 1
	}
	return b[:n+1], nil
}
func (o *readOwner) close() error {
	o.cancel()
	<-o.ready
	o.once.Do(func() {
		o.mu.Lock()
		o.closed = true
		body := o.plan.Body
		o.mu.Unlock()
		if body != nil {
			o.err = body.Close()
		}
	})
	return o.err
}
func (s *Service) fenceUnknownLocked(id uint64) error {
	if id == 0 || len(s.fenced) >= 1024 {
		return client.ErrInvalid
	}
	s.fenced[id] = time.Now().Add(6 * time.Minute)
	return nil
}
func (s *Service) closeReader(id uint64) error {
	s.mu.Lock()
	o := s.readers[id]
	if o == nil {
		e := s.fenceUnknownLocked(id)
		s.mu.Unlock()
		return e
	}
	s.mu.Unlock()
	err := o.close()
	s.mu.Lock()
	delete(s.readers, id)
	s.mu.Unlock()
	return err
}
func (s *Service) beginPut(ctx context.Context, input []byte) ([]byte, error) {
	var r PutFrame
	if descriptor.StrictJSON(input, &r) != nil || r.Size < -1 {
		return nil, client.ErrInvalid
	}
	c, e := s.live("put")
	if e != nil {
		return nil, e
	}
	if r.Size < 0 && !s.allowed("unknown-size-put") {
		return nil, client.ErrUnsupported
	}
	stream, cancel, e := s.streamContext(r.Deadline)
	if e != nil {
		return nil, e
	}
	reader, writer := io.Pipe()
	p := &putOwner{reader: reader, writer: writer, cancel: cancel, done: make(chan struct{}), expected: r.Size}
	s.mu.Lock()
	e = s.admitIDLocked(r.ID)
	if e == nil {
		s.uploads[r.ID] = p
	}
	s.mu.Unlock()
	if e != nil {
		cancel()
		reader.Close()
		writer.Close()
		return nil, e
	}
	go func() {
		p.result, p.err = c.Put(stream, client.PutRequest{Path: r.Path, Body: reader, Size: r.Size, ContentType: r.ContentType, Metadata: r.Metadata})
		reader.CloseWithError(p.err)
		close(p.done)
	}()
	if ctx.Err() != nil {
		_ = s.abortPut(r.ID)
		return nil, ctx.Err()
	}
	return []byte(`{}`), nil
}
func (s *Service) write(ctx context.Context, input []byte) ([]byte, error) {
	id, b, e := decodeWrite(input)
	if e != nil {
		return nil, e
	}
	s.mu.Lock()
	p := s.uploads[id]
	s.mu.Unlock()
	if p == nil {
		return nil, client.ErrNotFound
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.committed {
		return nil, client.ErrInvalid
	}
	if p.expected >= 0 && int64(len(b)) > p.expected-p.written {
		return nil, client.ErrInvalid
	}
	stop := context.AfterFunc(ctx, func() { p.cancel(); p.writer.CloseWithError(ctx.Err()) })
	n, e := p.writer.Write(b)
	stop()
	if e != nil {
		return nil, e
	}
	p.written += int64(n)
	if n != len(b) {
		return nil, io.ErrShortWrite
	}
	return []byte(`{}`), nil
}
func (s *Service) commitPut(ctx context.Context, input []byte) ([]byte, error) {
	var r StreamFrame
	if descriptor.StrictJSON(input, &r) != nil {
		return nil, client.ErrInvalid
	}
	s.mu.Lock()
	p := s.uploads[r.ID]
	s.mu.Unlock()
	if p == nil {
		return nil, client.ErrNotFound
	}
	p.mu.Lock()
	if p.committed || p.expected >= 0 && p.written != p.expected {
		p.mu.Unlock()
		return nil, client.ErrInvalid
	}
	p.committed = true
	p.writer.Close()
	p.mu.Unlock()
	select {
	case <-p.done:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	p.cancel()
	s.mu.Lock()
	delete(s.uploads, r.ID)
	s.mu.Unlock()
	return marshalResult(p.result, p.err)
}
func (s *Service) abortPut(id uint64) error {
	s.mu.Lock()
	p := s.uploads[id]
	if p == nil {
		e := s.fenceUnknownLocked(id)
		s.mu.Unlock()
		return e
	}
	s.mu.Unlock()
	p.cancel()
	p.reader.CloseWithError(context.Canceled)
	p.writer.CloseWithError(context.Canceled)
	<-p.done
	s.mu.Lock()
	delete(s.uploads, id)
	s.mu.Unlock()
	return nil
}

// Close starts an owned cleanup continuation. A cancelled waiter cannot release
// the native instance beneath outstanding body operations or provider goroutines.
func (s *Service) Close(ctx context.Context) error {
	s.mu.Lock()
	if s.done == nil {
		s.closing = true
		s.done = make(chan struct{})
		s.cancel()
		go s.close()
	}
	done := s.done
	s.mu.Unlock()
	select {
	case <-done:
		return s.outcome
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (s *Service) close() {
	s.mu.Lock()
	reads := make([]uint64, 0, len(s.readers))
	puts := make([]uint64, 0, len(s.uploads))
	for id := range s.readers {
		reads = append(reads, id)
	}
	for id := range s.uploads {
		puts = append(puts, id)
	}
	s.mu.Unlock()
	var e error
	for _, id := range reads {
		e = errors.Join(e, s.closeReader(id))
	}
	for _, id := range puts {
		e = errors.Join(e, s.abortPut(id))
	}
	s.calls.Wait()
	s.phase.Lock()
	if s.candidate != nil {
		e = errors.Join(e, s.candidate.Abort(context.Background()))
	}
	if s.instance != nil {
		e = errors.Join(e, s.instance.Close(context.Background()))
	}
	if s.secrets != nil {
		s.secrets.close()
	}
	s.phase.Unlock()
	s.mu.Lock()
	s.outcome = e
	close(s.done)
	s.mu.Unlock()
}

type secretSet struct {
	values   map[string][]byte
	declared map[string]bool
}

func (s *secretSet) Resolve(ctx context.Context, name string) (client.Secret, error) {
	if e := ctx.Err(); e != nil {
		return nil, e
	}
	if !s.declared[name] {
		return nil, client.ErrInvalid
	}
	v, ok := s.values[name]
	if !ok {
		return nil, client.ErrNotFound
	}
	return &secretBytes{value: append([]byte(nil), v...)}, nil
}
func (s *secretSet) close() {
	if s == nil {
		return
	}
	for k, v := range s.values {
		clear(v)
		delete(s.values, k)
	}
}

type secretBytes struct{ value []byte }

func (s *secretBytes) Bytes() []byte { return s.value }
func (s *secretBytes) Close()        { clear(s.value); s.value = nil }
