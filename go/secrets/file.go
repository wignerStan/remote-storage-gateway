// Package secrets resolves operator-owned secret references without loading a
// Worker runtime or any native plugin artifact.
package secrets

import (
	"context"
	"io"
	"os"

	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/client"
)

// FileResolver is an operator-local resolver; desired state contains only its
// declared references. Ordinary management reads never expose these bytes.
type FileResolver map[string]string

func (s FileResolver) Resolve(ctx context.Context, ref string) (client.Secret, error) {
	if e := ctx.Err(); e != nil {
		return nil, e
	}
	path, ok := s[ref]
	if !ok {
		return nil, client.ErrNotFound
	}
	f, e := os.Open(path)
	if e != nil {
		return nil, client.ErrInvalid
	}
	defer f.Close()
	info, e := f.Stat()
	if e != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
		return nil, client.ErrInvalid
	}
	b, e := io.ReadAll(io.LimitReader(f, 65537))
	if e != nil || len(b) > 65536 {
		clear(b)
		return nil, client.ErrInvalid
	}
	return &secret{b}, nil
}

type secret struct{ b []byte }

func (s *secret) Bytes() []byte { return s.b }
func (s *secret) Close()        { clear(s.b); s.b = nil }
