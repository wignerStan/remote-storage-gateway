package httpapi

import (
	"context"
	"errors"
	"github.com/gin-gonic/gin"
	"github.com/wignerStan/remote-storage-gateway/go/gateway"
	"golang.org/x/net/webdav"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
)

// RegisterGin binds this entry's routes to the application-owned Gin engine.
// WebDAV remains the protocol adapter; the listener, TLS and schema tools have
// no ownership of the product's namespace or authentication decisions.
func RegisterGin(router *gin.Engine, r *gateway.Runtime, control, proxy http.Handler) {
	router.RedirectTrailingSlash = false
	router.RedirectFixedPath = false
	router.HandleMethodNotAllowed = true
	router.Use(func(c *gin.Context) {
		q := c.Request
		switch r.Route(requestHost(q.Host), q.URL.Path) {
		case gateway.Administration:
			proxy.ServeHTTP(c.Writer, q)
			c.Abort()
			return
		case gateway.Misdirected:
			http.Error(c.Writer, "Misdirected Request", http.StatusMisdirectedRequest)
			c.Abort()
			return
		case gateway.Denied:
			http.NotFound(c.Writer, q)
			c.Abort()
			return
		}
		if q.URL.Path == "/healthz" && q.Method == http.MethodGet {
			c.Next()
			return
		}
		username, password, authenticated := q.BasicAuth()
		if !authenticated || !r.Authorize(username, password) {
			c.Header("WWW-Authenticate", `Basic realm="storage-gateway", charset="UTF-8"`)
			http.Error(c.Writer, "Unauthorized", http.StatusUnauthorized)
			c.Abort()
			return
		}
		c.Next()
	})
	router.GET("/healthz", func(c *gin.Context) { c.Status(http.StatusNoContent) })
	router.Any("/v1", gin.WrapH(control))
	router.Any("/v1/*path", gin.WrapH(control))

	for _, prefix := range []string{"/dav", "/remote-dav"} {
		dav := &webdav.Handler{Prefix: prefix, FileSystem: &filesystem{Filesystem: gateway.NewFilesystem(r)}, LockSystem: webdav.NewMemLS()}
		handler := func(c *gin.Context) { serveWebDAV(c.Writer, c.Request, r, prefix, dav) }
		for _, route := range []string{prefix, prefix + "/*path"} {
			for _, method := range []string{"GET", "HEAD", "PROPFIND", "OPTIONS", "PUT", "DELETE"} {
				router.Handle(method, route, handler)
			}
		}
	}
	router.NoMethod(func(c *gin.Context) {
		if c.Request.URL.Path == "/dav" || strings.HasPrefix(c.Request.URL.Path, "/dav/") || c.Request.URL.Path == "/remote-dav" || strings.HasPrefix(c.Request.URL.Path, "/remote-dav/") {
			c.Header("Allow", "GET, HEAD, PROPFIND, OPTIONS, PUT, DELETE")
			http.Error(c.Writer, "Operation unsupported", http.StatusMethodNotAllowed)
			return
		}
		http.NotFound(c.Writer, c.Request)
	})
	router.NoRoute(func(c *gin.Context) { http.NotFound(c.Writer, c.Request) })
}

func serveWebDAV(w http.ResponseWriter, q *http.Request, r *gateway.Runtime, prefix string, dav *webdav.Handler) {
	if q.Method == "OPTIONS" {
		w.Header().Set("DAV", "1")
		w.Header().Set("Allow", "GET, HEAD, PROPFIND, OPTIONS, PUT, DELETE")
		w.WriteHeader(http.StatusOK)
		return
	}
	if q.Method == "PROPFIND" && q.Header.Get("Depth") != "0" && q.Header.Get("Depth") != "1" {
		http.Error(w, "Depth must be 0 or 1", http.StatusForbidden)
		return
	}
	if (q.Method == "PUT" || q.Method == "DELETE") && (q.Header.Get("If-Match") != "" || q.Header.Get("If-None-Match") != "" || q.Header.Get("If") != "") {
		http.Error(w, "Conditional mutation unsupported", http.StatusNotImplemented)
		return
	}
	name := strings.TrimPrefix(q.URL.Path, prefix)
	var flow *gateway.Exchange
	var e error
	if prefix == "/remote-dav" {
		flow, e = r.BeginRemoteExchange(name, q.Method == "PUT" || q.Method == "DELETE")
	} else {
		flow, e = r.BeginExchange(name, q.Method == "PUT" || q.Method == "DELETE")
	}
	if errors.Is(e, os.ErrPermission) {
		http.Error(w, "Read only mount", http.StatusForbidden)
		return
	}
	if e != nil {
		if _, _, resolution := r.Resolve(name); resolution == nil {
			http.Error(w, "Client unavailable", http.StatusServiceUnavailable)
			return
		}
	}
	if e == nil {
		defer flow.Release()
		mount, _, _ := r.Resolve(name)
		if version := flow.Version(mount.Name); version.Incarnation != "" {
			w.Header().Set("X-Worker-Incarnation", version.Incarnation)
		}
		ctx, cancel := context.WithCancel(q.Context())
		defer cancel()
		ctx = gateway.BindExchange(ctx, flow, q.ContentLength, q.Header.Get("Content-Type"))
		q = q.WithContext(ctx)
		if q.Method == "PUT" {
			q.Body = &cancelBody{ReadCloser: q.Body, cancel: cancel}
		}
	}
	if q.Method == http.MethodGet || q.Method == http.MethodHead {
		w.Header().Set("Content-Type", contentType(q.URL.Path))
		w.Header().Set("X-Content-Type-Options", "nosniff")
	}
	dav.ServeHTTP(w, q)
	return
}

type cancelBody struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (b *cancelBody) Read(p []byte) (int, error) {
	n, e := b.ReadCloser.Read(p)
	if e != nil && e != io.EOF {
		b.cancel()
	}
	return n, e
}

func requestHost(host string) string {
	if name, _, e := net.SplitHostPort(host); e == nil {
		host = name
	}
	return strings.ToLower(strings.TrimSuffix(host, "."))
}
