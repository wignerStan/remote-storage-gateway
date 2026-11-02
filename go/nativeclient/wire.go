// Package nativeclient owns the typed client/v1 wire over InfraPluginV1 async
// operations. Metadata is bounded JSON; body frames are bounded raw bytes.
package nativeclient

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/client"
)

const Variant = "client-v1"
const ChunkBytes = 64 << 10
const MaxStreams = 128
const (
	Describe  = "descriptor"
	Prepare   = "prepare"
	Validate  = "validate"
	Publish   = "publish"
	Abort     = "abort"
	Stat      = "stat"
	List      = "list"
	Open      = "open"
	Read      = "read"
	ReadClose = "read-close"
	PutBegin  = "put-begin"
	PutWrite  = "put-write"
	PutCommit = "put-commit"
	PutAbort  = "put-abort"
	Delete    = "delete"
)

type PrepareRequest struct {
	ImplementationID string            `json:"implementation_id"`
	Public           json.RawMessage   `json:"public"`
	Secrets          map[string][]byte `json:"secrets,omitempty"`
}
type OpenFrame struct {
	ID       uint64             `json:"id"`
	Request  client.OpenRequest `json:"request"`
	Deadline int64              `json:"deadline_unix_nano"`
}
type OpenResult struct {
	Info client.ObjectInfo `json:"info"`
}
type StreamFrame struct {
	ID     uint64 `json:"id"`
	Credit int    `json:"credit,omitempty"`
}
type PutFrame struct {
	ID          uint64            `json:"id"`
	Path        string            `json:"path"`
	Size        int64             `json:"size"`
	ContentType string            `json:"content_type,omitempty"`
	Metadata    map[string]string `json:"metadata,omitempty"`
	Deadline    int64             `json:"deadline_unix_nano"`
}

func encode(v any) ([]byte, error) { return json.Marshal(v) }

// writeFrame is ID(u64 network order), followed by 1..ChunkBytes body bytes.
func writeFrame(id uint64, b []byte) ([]byte, error) {
	if id == 0 || len(b) == 0 || len(b) > ChunkBytes {
		return nil, client.ErrInvalid
	}
	out := make([]byte, 8+len(b))
	binary.BigEndian.PutUint64(out, id)
	copy(out[8:], b)
	return out, nil
}
func decodeWrite(b []byte) (uint64, []byte, error) {
	if len(b) <= 8 || len(b) > 8+ChunkBytes {
		return 0, nil, client.ErrInvalid
	}
	id := binary.BigEndian.Uint64(b)
	if id == 0 {
		return 0, nil, client.ErrInvalid
	}
	return id, b[8:], nil
}

// Read replies prefix one terminal-EOF byte (0 or 1), then 0..ChunkBytes data.
func readFrame(b []byte) ([]byte, bool, error) {
	if len(b) == 0 || len(b) > ChunkBytes+1 || b[0] > 1 || len(b) == 1 && b[0] == 0 {
		return nil, false, errors.New("invalid native read frame")
	}
	return b[1:], b[0] == 1, nil
}
