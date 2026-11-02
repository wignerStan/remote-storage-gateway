package httpapi

import (
	"github.com/wignerStan/remote-storage-gateway/go/gateway"
	"mime"
	"net/http"
	"strings"
)

// RemoteReadOnly limits the private Unix data entry to authenticated reads of
// remote-only mounts. It cannot invoke administration, cached routes or gRPC.
func RemoteReadOnly(service *gateway.Runtime, handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, q *http.Request) {
		if q.URL.Path != "/remote-dav" && !strings.HasPrefix(q.URL.Path, "/remote-dav/") {
			http.NotFound(w, q)
			return
		}
		switch q.Method {
		case http.MethodGet, http.MethodHead, http.MethodOptions, "PROPFIND":
		default:
			w.Header().Set("Allow", "GET, HEAD, OPTIONS, PROPFIND")
			http.Error(w, "read-only data channel", http.StatusMethodNotAllowed)
			return
		}
		media, _, _ := mime.ParseMediaType(q.Header.Get("Content-Type"))
		if media == "application/grpc" || strings.HasPrefix(media, "application/grpc+") {
			http.Error(w, "unsupported data protocol", http.StatusUnsupportedMediaType)
			return
		}
		user, password, ok := q.BasicAuth()
		if !ok || service == nil || !service.Authorize(user, password) {
			w.Header().Set("WWW-Authenticate", `Basic realm="storage-gateway"`)
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		handler.ServeHTTP(w, q)
	})
}
