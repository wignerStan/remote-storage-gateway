package nativeclient

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	native "github.com/wignerStan/infra/go/runtime/nativeplugin"
	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/client"
	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/descriptor"
	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/descriptor/admission"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Artifact is an operator-installed immutable artifact approval. Desired state
// references its plugin/descriptor identity and cannot install executable code.
// SHA256 binds ELF bytes. Descriptor.BuildDigest names build provenance and is
// intentionally distinct from the impossible self-embedded ELF digest.
type Artifact struct {
	Path             string `json:"path"`
	SHA256           string `json:"sha256"`
	DescriptorDigest string `json:"descriptor_digest"`
	PluginID         string `json:"plugin_id"`
	Language         string `json:"language"`
}
type Factory struct {
	artifact Artifact
	record   descriptor.Record
}

func NewFactory(ctx context.Context, a Artifact) (*Factory, error) {
	if !descriptor.ValidDigest(a.DescriptorDigest) {
		return nil, errors.New("exact descriptor approval required")
	}
	f := &Factory{artifact: a}
	n, e := f.load()
	if e != nil {
		return nil, e
	}
	defer n.Close(context.Background())
	b, e := n.Call(ctx, Describe, []byte(`{}`))
	if e != nil {
		return nil, mapError(e)
	}
	r, e := admission.Decode(b)
	if e != nil || r.Digest != a.DescriptorDigest || r.Descriptor.PluginID != a.PluginID || r.Descriptor.Contract != client.Contract || r.Descriptor.Runtime != a.Language {
		return nil, errors.New("approved descriptor mismatch")
	}
	f.record = r
	return f, nil
}

// Inspect describes an already installed, hash-approved artifact without provider
// construction. The returned exact bytes must be approved separately before use.
func Inspect(ctx context.Context, path, sha256, pluginID, language string) (descriptor.Record, error) {
	f := &Factory{artifact: Artifact{Path: path, SHA256: sha256, PluginID: pluginID, Language: language}}
	n, e := f.load()
	if e != nil {
		return descriptor.Record{}, e
	}
	defer n.Close(context.Background())
	b, e := n.Call(ctx, Describe, []byte(`{}`))
	if e != nil {
		return descriptor.Record{}, mapError(e)
	}
	r, e := admission.Decode(b)
	if e != nil || r.Descriptor.PluginID != pluginID || r.Descriptor.Runtime != language || r.Descriptor.Contract != client.Contract {
		return descriptor.Record{}, errors.New("invalid passive client descriptor")
	}
	return r, nil
}
func (f *Factory) ArtifactDigest() string {
	return "sha256:" + strings.TrimPrefix(f.artifact.SHA256, "sha256:")
}
func (f *Factory) Artifact() Artifact { return f.artifact }
func (f *Factory) Descriptor() descriptor.Descriptor {
	r, _ := admission.Decode(f.record.Bytes)
	return r.Descriptor
}
func (f *Factory) DescriptorRecord() descriptor.Record {
	r, _ := admission.Decode(f.record.Bytes)
	return r
}
func (f *Factory) load() (*native.Client, error) {
	a := f.artifact
	hash := strings.TrimPrefix(a.SHA256, "sha256:")
	if !filepath.IsAbs(a.Path) || len(hash) != 64 || !descriptor.ValidDigest("sha256:"+hash) || (a.DescriptorDigest != "" && !descriptor.ValidDigest(a.DescriptorDigest)) || a.PluginID == "" || (a.Language != "go" && a.Language != "rust") {
		return nil, errors.New("invalid installed artifact approval")
	}
	file, e := os.Open(a.Path)
	if e != nil {
		return nil, errors.New("approved artifact unavailable")
	}
	defer file.Close()
	stat, e := file.Stat()
	if e != nil || !stat.Mode().IsRegular() || stat.Size() > 256<<20 {
		return nil, errors.New("invalid artifact file")
	}
	h := sha256.New()
	if _, e = io.Copy(h, file); e != nil || hex.EncodeToString(h.Sum(nil)) != hash {
		return nil, errors.New("artifact digest mismatch")
	}
	language, unload := native.Go, native.Retain
	if a.Language == "rust" {
		language, unload = native.Rust, native.Explicit
	}
	// The canonical loader duplicates and retains this hashed inode for the mapping
	// lifetime. Installation is operator-governed and immutable; descriptor approval
	// does not defend against a privileged operator mutating an inode in place.
	n, e := native.LoadFile(file, native.Expected{Package: a.PluginID, Variant: Variant, Kind: native.Tool, Language: language, UnloadPolicy: unload}, native.Limits{MaxInput: descriptor.MaxBytes, MaxReply: descriptor.MaxBytes, MaxInflight: 64, Timeout: 120 * time.Second})
	if e != nil {
		return nil, errors.New("native artifact rejected")
	}
	return n, nil
}
func (f *Factory) Prepare(ctx context.Context, cfg client.PluginConfig) (client.Candidate, error) {
	a, ok := f.record.Descriptor.Implementation(cfg.ImplementationID)
	if !ok {
		return nil, client.ErrInvalid
	}
	secrets := map[string][]byte{}
	defer func() {
		for _, v := range secrets {
			clear(v)
		}
	}()
	refs := map[string]string{}
	for _, field := range admission.SecretFields(a) {
		if cfg.Secrets == nil {
			if field.Required {
				return nil, client.ErrNotFound
			}
			continue
		}
		secret, e := cfg.Secrets.Resolve(ctx, field.Name)
		if errors.Is(e, client.ErrNotFound) && !field.Required {
			continue
		}
		if e != nil {
			return nil, mapError(e)
		}
		if secret == nil {
			return nil, client.ErrInvalid
		}
		value := secret.Bytes()
		if len(value) > 64<<10 {
			secret.Close()
			return nil, client.ErrInvalid
		}
		secrets[field.Name] = append([]byte(nil), value...)
		secret.Close()
		refs[field.Name] = "resolved"
	}
	if len(cfg.Public) == 0 {
		cfg.Public = []byte(`{}`)
	}
	if e := admission.ValidateConfig(a, cfg.Public, refs); e != nil {
		return nil, e
	}
	n, e := f.load()
	if e != nil {
		return nil, e
	}
	failed := true
	defer func() {
		if failed {
			_ = n.Close(context.Background())
		}
	}()
	raw, e := n.Call(ctx, Describe, []byte(`{}`))
	if e != nil {
		return nil, mapError(e)
	}
	record, e := admission.Decode(raw)
	if e != nil || record.Digest != f.record.Digest {
		return nil, errors.New("loaded descriptor differs from approved descriptor")
	}
	b, e := encode(PrepareRequest{cfg.ImplementationID, cfg.Public, secrets})
	if e != nil {
		return nil, e
	}
	defer clear(b)
	if _, e = n.Call(ctx, Prepare, b); e != nil {
		return nil, mapError(e)
	}
	failed = false
	return &candidate{client: &remoteClient{native: n}}, nil
}

type candidate struct {
	mu                            sync.Mutex
	client                        *remoteClient
	validated, published, aborted bool
}

func (c *candidate) Validate(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.aborted || c.published {
		return client.ErrInvalid
	}
	if _, e := c.client.call(ctx, Validate, struct{}{}); e != nil {
		return e
	}
	c.validated = true
	return nil
}
func (c *candidate) Publish() (client.Client, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.validated || c.aborted || c.published {
		return nil, client.ErrInvalid
	}
	if _, e := c.client.call(context.Background(), Publish, struct{}{}); e != nil {
		return nil, e
	}
	c.published = true
	return c.client, nil
}
func (c *candidate) Abort(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.published {
		return client.ErrInvalid
	}
	if c.aborted {
		return c.client.Close(ctx)
	}
	c.aborted = true
	_, e := c.client.call(ctx, Abort, struct{}{})
	return errors.Join(e, c.client.Close(ctx))
}

type remoteClient struct {
	native   *native.Client
	sequence atomic.Uint64
}

func (c *remoteClient) call(ctx context.Context, method string, v any) ([]byte, error) {
	b, e := encode(v)
	if e != nil {
		return nil, client.ErrInvalid
	}
	out, e := c.native.Call(ctx, method, b)
	return out, mapError(e)
}
func decodeResult[T any](b []byte, e error) (T, error) {
	var out T
	if e != nil {
		return out, e
	}
	if admission.StrictJSON(b, &out) != nil {
		return out, client.ErrProvider
	}
	return out, nil
}
func (c *remoteClient) Stat(ctx context.Context, r client.ObjectRef) (client.ObjectInfo, error) {
	b, e := c.call(ctx, Stat, r)
	return decodeResult[client.ObjectInfo](b, e)
}
func (c *remoteClient) List(ctx context.Context, r client.ListRequest) (client.ListResult, error) {
	b, e := c.call(ctx, List, r)
	return decodeResult[client.ListResult](b, e)
}
func (c *remoteClient) Delete(ctx context.Context, r client.ObjectRef) error {
	_, e := c.call(ctx, Delete, r)
	return e
}
func (c *remoteClient) Close(ctx context.Context) error { return c.native.Close(ctx) }
func streamDeadline(ctx context.Context) int64 {
	if d, ok := ctx.Deadline(); ok {
		return d.UnixNano()
	}
	return time.Now().Add(time.Hour).UnixNano()
}
func (c *remoteClient) nextID() (uint64, error) {
	id := c.sequence.Add(1)
	if id == 0 {
		return 0, client.ErrInvalid
	}
	return id, nil
}
func (c *remoteClient) closeStream(method string, id uint64) error {
	for {
		_, e := c.call(context.Background(), method, StreamFrame{ID: id})
		if errors.Is(e, context.DeadlineExceeded) {
			continue
		}
		return e
	}
}
func (c *remoteClient) Open(ctx context.Context, r client.OpenRequest) (client.ReadPlan, error) {
	id, e := c.nextID()
	if e != nil {
		return client.ReadPlan{}, e
	}
	b, e := c.call(ctx, Open, OpenFrame{id, r, streamDeadline(ctx)})
	if e != nil {
		return client.ReadPlan{}, errors.Join(e, c.closeStream(ReadClose, id))
	}
	v, e := decodeResult[OpenResult](b, nil)
	if e != nil {
		return client.ReadPlan{}, errors.Join(e, c.closeStream(ReadClose, id))
	}
	return client.ReadPlan{Info: v.Info, Body: &remoteReader{client: c, id: id, ctx: ctx}}, nil
}

type remoteReader struct {
	mu          sync.Mutex
	client      *remoteClient
	id          uint64
	ctx         context.Context
	pending     []byte
	eof, closed bool
	closeOnce   sync.Once
	closeErr    error
}

func (r *remoteReader) Read(b []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(b) == 0 {
		return 0, nil
	}
	if r.closed {
		return 0, io.ErrClosedPipe
	}
	if len(r.pending) > 0 {
		n := copy(b, r.pending)
		r.pending = r.pending[n:]
		return n, nil
	}
	if r.eof {
		return 0, io.EOF
	}
	raw, e := r.client.call(r.ctx, Read, StreamFrame{ID: r.id, Credit: min(len(b), ChunkBytes)})
	if e != nil {
		return 0, e
	}
	data, eof, e := readFrame(raw)
	if e != nil {
		return 0, e
	}
	if len(data) > min(len(b), ChunkBytes) {
		return 0, client.ErrProvider
	}
	r.eof = eof
	n := copy(b, data)
	if n < len(data) {
		r.pending = append([]byte(nil), data[n:]...)
	}
	if n == 0 && eof {
		return 0, io.EOF
	}
	return n, nil
}
func (r *remoteReader) Close() error {
	r.closeOnce.Do(func() {
		r.closeErr = r.client.closeStream(ReadClose, r.id)
		r.mu.Lock()
		r.closed = true
		r.pending = nil
		r.mu.Unlock()
	})
	return r.closeErr
}
func (c *remoteClient) Put(ctx context.Context, r client.PutRequest) (out client.PutResult, err error) {
	if r.Body == nil || r.Size < -1 {
		return out, client.ErrInvalid
	}
	id, e := c.nextID()
	if e != nil {
		return out, e
	}
	if _, e = c.call(ctx, PutBegin, PutFrame{id, r.Path, r.Size, r.ContentType, r.Metadata, streamDeadline(ctx)}); e != nil {
		return out, errors.Join(e, c.closeStream(PutAbort, id))
	}
	finished := false
	defer func() {
		if !finished {
			err = errors.Join(err, c.closeStream(PutAbort, id))
		}
	}()
	buf := make([]byte, ChunkBytes)
	var size int64
	for {
		if e = ctx.Err(); e != nil {
			return out, e
		}
		n, readErr := r.Body.Read(buf)
		if n < 0 || n > len(buf) {
			return out, client.ErrInvalid
		}
		if n > 0 {
			size += int64(n)
			if r.Size >= 0 && size > r.Size {
				return out, client.ErrInvalid
			}
			frame, _ := writeFrame(id, buf[:n])
			_, e = c.native.Call(ctx, PutWrite, frame)
			if e != nil {
				return out, mapError(e)
			}
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return out, readErr
		}
		if n == 0 {
			return out, io.ErrNoProgress
		}
	}
	if r.Size >= 0 && size != r.Size {
		return out, io.ErrUnexpectedEOF
	}
	b, e := c.call(ctx, PutCommit, StreamFrame{ID: id})
	if e != nil {
		return out, e
	}
	out, e = decodeResult[client.PutResult](b, nil)
	if e != nil {
		return out, e
	}
	finished = true
	return out, nil
}

// ErrorCode preserves typed domain errors without exposing provider messages.
func ErrorCode(e error) int32 {
	switch {
	case e == nil:
		return 0
	case errors.Is(e, context.DeadlineExceeded):
		return 1
	case errors.Is(e, context.Canceled):
		return 2
	case errors.Is(e, client.ErrNotFound):
		return 1001
	case errors.Is(e, client.ErrUnsupported):
		return 1002
	case errors.Is(e, client.ErrInvalid):
		return 1003
	default:
		return 5
	}
}
func mapError(e error) error {
	if e == nil {
		return nil
	}
	if errors.Is(e, context.Canceled) || errors.Is(e, context.DeadlineExceeded) {
		return e
	}
	var nativeError *native.StatusError
	if errors.As(e, &nativeError) {
		switch nativeError.Code {
		case 1001:
			return client.ErrNotFound
		case 1002:
			return client.ErrUnsupported
		case 1003:
			return client.ErrInvalid
		}
	}
	return client.ErrProvider
}
