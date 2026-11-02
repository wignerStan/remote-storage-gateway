package httpapi

import (
	"context"
	config "github.com/wignerStan/remote-storage-gateway/go/contracts/worker"
	"github.com/wignerStan/remote-storage-gateway/go/gateway"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestPrivateDataEntryIsAuthenticatedRemoteReadsOnly(t *testing.T) {
	service, e := gateway.New(config.RuntimeConfig{Username: "worker"}, nil, []byte("private-worker-password"))
	if e != nil {
		t.Fatal(e)
	}
	defer service.Close(context.Background())
	calls := 0
	entry := RemoteReadOnly(service, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls++; w.WriteHeader(204) }))
	for _, test := range []struct {
		method, path, contentType string
		auth                      bool
		want                      int
	}{{"GET", "/remote-dav/files/object", "", true, 204}, {"HEAD", "/remote-dav/files/object", "", true, 204}, {"PROPFIND", "/remote-dav/files/", "application/xml", true, 204}, {"OPTIONS", "/remote-dav/files/", "", true, 204}, {"GET", "/remote-dav/files/object", "", false, 401}, {"PUT", "/remote-dav/files/object", "", true, 405}, {"DELETE", "/remote-dav/files/object", "", true, 405}, {"GET", "/dav/files/object", "", true, 404}, {"GET", "/v1/manifest", "", true, 404}, {"GET", "/api/admin/control", "", true, 404}, {"POST", "/build.Execution/Execute", "application/grpc", true, 404}, {"GET", "/remote-dav/files/object", "application/grpc+proto", true, 415}} {
		before := calls
		q := httptest.NewRequest(test.method, test.path, strings.NewReader("request"))
		q.Header.Set("Content-Type", test.contentType)
		if test.auth {
			q.SetBasicAuth("worker", "private-worker-password")
		}
		w := httptest.NewRecorder()
		entry.ServeHTTP(w, q)
		if w.Code != test.want {
			t.Fatalf("%s %s status%d want%d", test.method, test.path, w.Code, test.want)
		}
		if test.want != 204 && calls != before {
			t.Fatal("rejected private data request reached downstream")
		}
	}
}
