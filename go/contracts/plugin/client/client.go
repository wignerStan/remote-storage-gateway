// Package client owns the worker's versioned streaming client port.
package client

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/descriptor"
	workercontract "github.com/wignerStan/network-data-plane-worker/go/contracts/worker"
	"io"
	"time"
)

const Contract = "network-data-plane-worker/client/v1"

var (
	ErrNotFound    = errors.New("object not found")
	ErrUnsupported = errors.New("operation unsupported")
	ErrInvalid     = errors.New("invalid client request")
	ErrProvider    = errors.New("client provider failed")
)

type ObjectRef struct {
	Path string `json:"path"`
}
type ObjectInfo struct {
	Path        string            `json:"path"`
	Size        int64             `json:"size"`
	IsDir       bool              `json:"is_dir"`
	ModTime     time.Time         `json:"mod_time"`
	ETag        string            `json:"etag,omitempty"`
	ContentType string            `json:"content_type,omitempty"`
	Metadata    map[string]string `json:"metadata,omitempty"`
}
type ListRequest struct {
	Path   string `json:"path"`
	Cursor string `json:"cursor,omitempty"`
	Limit  int    `json:"limit,omitempty"`
}
type ListResult struct {
	Objects    []ObjectInfo `json:"objects"`
	NextCursor string       `json:"next_cursor,omitempty"`
}

// Length -1 means through EOF. Offset zero/Length -1 is a full read.
type OpenRequest struct {
	Path   string `json:"path"`
	Offset int64  `json:"offset"`
	Length int64  `json:"length"`
}
type ReadPlan struct {
	Info ObjectInfo
	Body io.ReadCloser
}
type PutRequest struct {
	Path        string
	Body        io.Reader
	Size        int64
	ContentType string
	Metadata    map[string]string
}
type PutResult struct {
	Info ObjectInfo `json:"info"`
}
type Client interface {
	Stat(context.Context, ObjectRef) (ObjectInfo, error)
	List(context.Context, ListRequest) (ListResult, error)
	Open(context.Context, OpenRequest) (ReadPlan, error)
	Put(context.Context, PutRequest) (PutResult, error)
	Delete(context.Context, ObjectRef) error
	Close(context.Context) error
}

// Lease retains the exact client generation through real operation completion.
type Lease interface {
	Client() Client
	Version() workercontract.Version
	Release()
}

// ManagedClients is the host capability consumed by namespace use-flows.
// Close stops admission and retains cleanup ownership beyond cancelled waiters.
type ManagedClients interface {
	Pin(string) (Lease, error)
	Close(context.Context) error
}
type Secret interface {
	Bytes() []byte
	Close()
}
type SecretResolver interface {
	Resolve(context.Context, string) (Secret, error)
}
type PluginConfig struct {
	ImplementationID string
	Public           json.RawMessage
	Secrets          SecretResolver
}
type Factory interface {
	Descriptor() descriptor.Descriptor
	Prepare(context.Context, PluginConfig) (Candidate, error)
}
type Candidate interface {
	Validate(context.Context) error
	Publish() (Client, error)
	Abort(context.Context) error
}
