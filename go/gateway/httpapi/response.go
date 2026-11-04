package httpapi

import (
	"context"
	"errors"
	"io"
	"net/http"
	"sync/atomic"

	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/client"
)

type providerFailuresKey struct{}
type providerFailures struct{ failed atomic.Bool }

func (f *providerFailures) record(err error) {
	if f != nil && errors.Is(err, client.ErrProvider) {
		f.failed.Store(true)
	}
}
func providerFailuresFrom(ctx context.Context) *providerFailures {
	f, _ := ctx.Value(providerFailuresKey{}).(*providerFailures)
	return f
}
func recordProviderFailure(ctx context.Context, err error) { providerFailuresFrom(ctx).record(err) }

// The native DAV handler maps every failed OpenFile to 404 and its ServeContent
// path announces success before lazy body opening. Retain only headers until a
// first write or handler completion; no payload is buffered and no I/O replayed.
func serveDAVResponse(w http.ResponseWriter, r *http.Request, next http.Handler) {
	failures := &providerFailures{}
	response := &davResponse{ResponseWriter: w, headers: w.Header().Clone(), failures: failures}
	next.ServeHTTP(response, r.WithContext(context.WithValue(r.Context(), providerFailuresKey{}, failures)))
	response.finish()
}

type davResponse struct {
	http.ResponseWriter
	headers                http.Header
	failures               *providerFailures
	status                 int
	committed, unavailable bool
}

func (w *davResponse) Header() http.Header { return w.headers }
func (w *davResponse) WriteHeader(status int) {
	if w.committed || w.status != 0 {
		return
	}
	// DAV emits no informational responses; preserve the interface's final-status
	// semantics without committing a successful content header prematurely.
	w.status = status
}
func (w *davResponse) commit() {
	if w.committed {
		return
	}
	destination := w.ResponseWriter.Header()
	clear(destination)
	for name, values := range w.headers {
		destination[name] = append([]string(nil), values...)
	}
	status := w.status
	if status == 0 {
		status = http.StatusOK
	}
	w.committed = true
	w.ResponseWriter.WriteHeader(status)
}
func (w *davResponse) fail() {
	if w.committed {
		return
	}
	for _, name := range []string{"Content-Length", "Content-Range", "Content-Encoding", "ETag", "Last-Modified", "Accept-Ranges"} {
		w.headers.Del(name)
	}
	w.headers.Set("Content-Type", "text/plain; charset=utf-8")
	w.headers.Set("Retry-After", "1")
	w.status = http.StatusServiceUnavailable
	w.unavailable = true
	w.commit()
	_, _ = io.WriteString(w.ResponseWriter, "Client unavailable\n")
}
func (w *davResponse) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if !w.committed && w.failures.failed.Load() {
		w.fail()
	}
	if w.unavailable {
		return len(p), nil
	}
	w.commit()
	return w.ResponseWriter.Write(p)
}
func (w *davResponse) finish() {
	if !w.committed && w.failures.failed.Load() {
		w.fail()
		return
	}
	w.commit()
}
