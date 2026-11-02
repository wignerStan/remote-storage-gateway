package nativeclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/wignerStan/infra/go/code/schema"
	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/client"
	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/descriptor"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type testFactory struct {
	prepared atomic.Int32
	c        *testClient
}

func generatedConfig[T any]() descriptor.ConfigSchema {
	doc, err := schema.Derive[T](schema.Identity{ID: "worker.test.config", Version: "1", Owner: "network-data-plane-worker"})
	if err != nil {
		panic(err)
	}
	b, err := json.Marshal(doc.Schema)
	if err != nil {
		panic(err)
	}
	return b
}

func (f *testFactory) Descriptor() descriptor.Descriptor {
	return descriptor.Descriptor{Schema: descriptor.Schema, PluginID: "test.native", PluginVersion: "1", Contract: client.Contract, Runtime: "go", BuildDigest: "sha256:" + string(bytes.Repeat([]byte("a"), 64)), Implementations: []descriptor.Implementation{{ID: "fake", ConfigSchema: generatedConfig[struct{}](), Capabilities: []string{"stat", "list", "open", "put", "delete", "range", "unknown-size-put"}}}}
}
func (f *testFactory) Prepare(context.Context, client.PluginConfig) (client.Candidate, error) {
	f.prepared.Add(1)
	return &testCandidate{f.c}, nil
}

type testCandidate struct{ c *testClient }

func (c *testCandidate) Validate(context.Context) error  { return nil }
func (c *testCandidate) Publish() (client.Client, error) { return c.c, nil }
func (c *testCandidate) Abort(context.Context) error     { return nil }

type testClient struct {
	open     func(context.Context) (io.ReadCloser, error)
	uploaded atomic.Int64
	steps    chan int
	putDone  chan struct{}
	once     sync.Once
}

func (c *testClient) Stat(context.Context, client.ObjectRef) (client.ObjectInfo, error) {
	return client.ObjectInfo{}, nil
}
func (c *testClient) List(context.Context, client.ListRequest) (client.ListResult, error) {
	return client.ListResult{}, nil
}
func (c *testClient) Open(ctx context.Context, _ client.OpenRequest) (client.ReadPlan, error) {
	if c.open == nil {
		return client.ReadPlan{Body: io.NopCloser(bytes.NewReader([]byte("hello")))}, nil
	}
	b, e := c.open(ctx)
	return client.ReadPlan{Body: b}, e
}
func (c *testClient) Put(_ context.Context, r client.PutRequest) (client.PutResult, error) {
	defer c.once.Do(func() { close(c.putDone) })
	b := make([]byte, 8192)
	for {
		n, e := r.Body.Read(b)
		c.uploaded.Add(int64(n))
		if n > 0 && c.steps != nil {
			c.steps <- n
		}
		if e == io.EOF {
			return client.PutResult{Info: client.ObjectInfo{Size: c.uploaded.Load()}}, nil
		}
		if e != nil {
			return client.PutResult{}, e
		}
	}
}
func (c *testClient) Delete(context.Context, client.ObjectRef) error { return nil }
func (c *testClient) Close(context.Context) error                    { return nil }
func call(t *testing.T, s *Service, method string, v any) []byte {
	t.Helper()
	b, _ := json.Marshal(v)
	out, e := s.Call(context.Background(), method, b)
	if e != nil {
		t.Fatalf("%s: %v", method, e)
	}
	return out
}
func ready(t *testing.T, c *testClient) *Service {
	t.Helper()
	f := &testFactory{c: c}
	s, e := NewService(f)
	if e != nil {
		t.Fatal(e)
	}
	call(t, s, Describe, struct{}{})
	if f.prepared.Load() != 0 {
		t.Fatal("describe constructed provider")
	}
	call(t, s, Prepare, PrepareRequest{ImplementationID: "fake", Public: []byte(`{}`)})
	if _, e := s.Call(context.Background(), Publish, []byte(`{}`)); e == nil {
		t.Fatal("publish skipped typed validation")
	}
	call(t, s, Validate, struct{}{})
	call(t, s, Publish, struct{}{})
	t.Cleanup(func() { s.Close(context.Background()) })
	return s
}
func TestStreamingPutAndDeclaredSize(t *testing.T) {
	c := &testClient{putDone: make(chan struct{})}
	s := ready(t, c)
	size := int64(4 << 20)
	call(t, s, PutBegin, PutFrame{ID: 1, Path: "object", Size: size, Deadline: time.Now().Add(time.Minute).UnixNano()})
	chunk := bytes.Repeat([]byte("x"), ChunkBytes)
	for done := int64(0); done < size; done += int64(len(chunk)) {
		frame, _ := writeFrame(1, chunk)
		if _, e := s.Call(context.Background(), PutWrite, frame); e != nil {
			t.Fatal(e)
		}
	}
	if c.uploaded.Load() == 0 {
		t.Fatal("body was buffered until commit")
	}
	call(t, s, PutCommit, StreamFrame{ID: 1})
	if c.uploaded.Load() != size {
		t.Fatal("stream size mismatch")
	}
	select {
	case <-c.putDone:
	default:
		t.Fatal("put completion not joined")
	}
	c2 := &testClient{putDone: make(chan struct{})}
	s2 := ready(t, c2)
	call(t, s2, PutBegin, PutFrame{ID: 2, Size: 100, Deadline: time.Now().Add(time.Minute).UnixNano()})
	frame, _ := writeFrame(2, []byte("short"))
	if _, e := s2.Call(context.Background(), PutWrite, frame); e != nil {
		t.Fatal(e)
	}
	raw, _ := json.Marshal(StreamFrame{ID: 2})
	if _, e := s2.Call(context.Background(), PutCommit, raw); e == nil {
		t.Fatal("truncated known-size commit admitted")
	}
	call(t, s2, PutAbort, StreamFrame{ID: 2})
}
func TestPutAbortJoinsProvider(t *testing.T) {
	c := &testClient{putDone: make(chan struct{})}
	s := ready(t, c)
	call(t, s, PutBegin, PutFrame{ID: 7, Size: -1, Deadline: time.Now().Add(time.Minute).UnixNano()})
	frame, _ := writeFrame(7, []byte("partial"))
	if _, e := s.Call(context.Background(), PutWrite, frame); e != nil {
		t.Fatal(e)
	}
	call(t, s, PutAbort, StreamFrame{ID: 7})
	select {
	case <-c.putDone:
	default:
		t.Fatal("abort released before provider completed")
	}
}
func TestCancelledNativeReadClosesUpstream(t *testing.T) {
	reader, writer := io.Pipe()
	defer writer.Close()
	entered := make(chan struct{})
	body := &watchedReader{ReadCloser: reader, entered: entered}
	s := ready(t, &testClient{open: func(context.Context) (io.ReadCloser, error) { return body, nil }})
	call(t, s, Open, OpenFrame{ID: 9, Request: client.OpenRequest{Length: -1}, Deadline: time.Now().Add(time.Minute).UnixNano()})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	b, _ := json.Marshal(StreamFrame{ID: 9, Credit: 1024})
	go func() { _, e := s.Call(ctx, Read, b); done <- e }()
	<-entered
	cancel()
	select {
	case e := <-done:
		if e == nil {
			t.Fatal("cancelled read succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("native read cancellation did not close upstream")
	}
	call(t, s, ReadClose, StreamFrame{ID: 9})
}

type watchedReader struct {
	io.ReadCloser
	entered chan struct{}
	once    sync.Once
}

func (r *watchedReader) Read(b []byte) (int, error) {
	r.once.Do(func() { close(r.entered) })
	return r.ReadCloser.Read(b)
}
func TestCloseBeforeDelayedOpenFencesCreation(t *testing.T) {
	var entered atomic.Int32
	s := ready(t, &testClient{open: func(context.Context) (io.ReadCloser, error) {
		entered.Add(1)
		return io.NopCloser(bytes.NewReader(nil)), nil
	}})
	call(t, s, ReadClose, StreamFrame{ID: 99})
	b, _ := json.Marshal(OpenFrame{ID: 99, Request: client.OpenRequest{Length: -1}, Deadline: time.Now().Add(time.Minute).UnixNano()})
	if _, e := s.Call(context.Background(), Open, b); e == nil {
		t.Fatal("late open admitted after cancellation")
	}
	if entered.Load() != 0 {
		t.Fatal("fenced open reached provider")
	}
}
func TestPendingOpenCancellationJoins(t *testing.T) {
	entered := make(chan struct{})
	finished := make(chan struct{})
	s := ready(t, &testClient{open: func(ctx context.Context) (io.ReadCloser, error) {
		close(entered)
		<-ctx.Done()
		close(finished)
		return nil, ctx.Err()
	}})
	raw, _ := json.Marshal(OpenFrame{ID: 10, Request: client.OpenRequest{Length: -1}, Deadline: time.Now().Add(time.Minute).UnixNano()})
	done := make(chan error, 1)
	go func() { _, e := s.Call(context.Background(), Open, raw); done <- e }()
	<-entered
	call(t, s, ReadClose, StreamFrame{ID: 10})
	select {
	case <-finished:
	default:
		t.Fatal("open was not joined")
	}
	if e := <-done; !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
}
