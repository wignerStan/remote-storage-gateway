// Package protocol owns the streaming HTTP protocol execution port; concrete
// protocol identities and semantics belong to independently installed plugins.
package protocol

import (
	"context"
	"io"
)

const Contract = "network-data-plane-worker/protocol/v1"

type Request struct {
	Method, Path string
	Headers      map[string][]string
	Body         io.ReadCloser
	Size         int64
}
type Response interface {
	WriteHeader(int, map[string][]string) error
	Write([]byte) (int, error)
	Flush() error
}
type Protocol interface {
	Handle(context.Context, Request, Response) error
	Close(context.Context) error
}
