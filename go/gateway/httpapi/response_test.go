package httpapi

import (
	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/client"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestDAVProviderFailureBeforePayloadReplacesPendingStatus(t *testing.T) {
	for _, status := range []int{200, 404, 207} {
		recorder := httptest.NewRecorder()
		serveDAVResponse(recorder, httptest.NewRequest("GET", "/dav/object", nil), http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Length", "987")
			w.Header().Set("Content-Range", "bytes 0-986/987")
			w.Header().Set("ETag", "stale")
			w.Header().Set("Content-Encoding", "gzip")
			w.WriteHeader(status)
			_, _ = w.Write(nil)
			recordProviderFailure(r.Context(), client.ErrProvider)
			if status != 200 {
				_, _ = io.WriteString(w, "native error body")
			}
		}))
		if recorder.Code != 503 || recorder.Header().Get("Retry-After") != "1" || recorder.Body.String() != "Client unavailable\n" {
			t.Fatalf("pending%d became%d %q", status, recorder.Code, recorder.Body.String())
		}
		for _, h := range []string{"Content-Length", "Content-Range", "ETag", "Content-Encoding"} {
			if recorder.Header().Get(h) != "" {
				t.Fatalf("stale representation header%s", h)
			}
		}
	}
}
func TestDAVProviderFailureAfterPayloadNeverRewritesOrReplays(t *testing.T) {
	recorder := httptest.NewRecorder()
	calls := 0
	serveDAVResponse(recorder, httptest.NewRequest("GET", "/dav/object", nil), http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.WriteHeader(206)
		_, _ = io.WriteString(w, "already streamed")
		recordProviderFailure(r.Context(), client.ErrProvider)
	}))
	if recorder.Code != 206 || recorder.Body.String() != "already streamed" || calls != 1 || recorder.Header().Get("Retry-After") != "" {
		t.Fatalf("stream rewritten: %d %s", recorder.Code, recorder.Body.String())
	}
}
func TestDAVNonProviderErrorsPreserveProtocolStatus(t *testing.T) {
	for _, status := range []int{304, 404, 416} {
		recorder := httptest.NewRecorder()
		serveDAVResponse(recorder, httptest.NewRequest("HEAD", "/dav/object", nil), http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			recordProviderFailure(r.Context(), client.ErrNotFound)
			w.WriteHeader(status)
		}))
		if recorder.Code != status || strings.Contains(recorder.Body.String(), "unavailable") {
			t.Fatalf("status%d changed to%d", status, recorder.Code)
		}
	}
}
