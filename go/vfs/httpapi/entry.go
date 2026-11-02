// Package httpapi binds the materializers private read-only DAV entry.
package httpapi

import (
	"context"
	"github.com/gin-gonic/gin"
	service "github.com/wignerStan/remote-storage-gateway/go/vfs"
	"golang.org/x/net/webdav"
	"net/http"
	"os"
)

func RegisterGin(router *gin.Engine, materializer *service.Service) {
	router.RedirectTrailingSlash = false
	router.RedirectFixedPath = false
	router.HandleMethodNotAllowed = true
	router.Use(func(c *gin.Context) {
		if c.Request.Method == http.MethodGet && c.Request.URL.Path == "/healthz" {
			c.Next()
			return
		}
		username, password, present := c.Request.BasicAuth()
		if !present {
			unauthorized(c)
			return
		}
		allowed, err := materializer.Authorize(c.Request.Context(), username, password)
		if err != nil {
			c.AbortWithStatus(http.StatusServiceUnavailable)
			return
		}
		if !allowed {
			unauthorized(c)
			return
		}
		c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 16<<10)
		c.Next()
	})
	router.GET("/healthz", func(c *gin.Context) { c.Status(http.StatusNoContent) })
	handler := &webdav.Handler{Prefix: "/dav", FileSystem: &filesystem{materializer}, LockSystem: webdav.NewMemLS()}
	for _, route := range []string{"/dav", "/dav/*path"} {
		for _, method := range []string{"GET", "HEAD", "PROPFIND", "OPTIONS"} {
			router.Handle(method, route, func(c *gin.Context) {
				if c.Request.Method == "PROPFIND" && c.GetHeader("Depth") != "0" && c.GetHeader("Depth") != "1" {
					c.AbortWithStatus(http.StatusForbidden)
					return
				}
				handler.ServeHTTP(c.Writer, c.Request)
			})
		}
	}
	router.NoMethod(func(c *gin.Context) {
		c.Header("Allow", "GET, HEAD, PROPFIND, OPTIONS")
		c.Status(http.StatusMethodNotAllowed)
	})
	router.NoRoute(func(c *gin.Context) { c.Status(http.StatusNotFound) })
}
func unauthorized(c *gin.Context) {
	c.Header("WWW-Authenticate", `Basic realm="private-materializer", charset="UTF-8"`)
	c.AbortWithStatus(http.StatusUnauthorized)
}

type filesystem struct{ *service.Service }

func (f *filesystem) OpenFile(ctx context.Context, name string, flags int, mode os.FileMode) (webdav.File, error) {
	return f.Service.OpenFile(ctx, name, flags, mode)
}
