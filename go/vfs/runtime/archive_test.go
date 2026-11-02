//go:build dataplane_l7

package runtime

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	port "github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/client"
	dp "github.com/wignerStan/network-data-plane-worker/go/contracts/worker"
	"github.com/wignerStan/network-data-plane-worker/go/nativeclient"
	"github.com/wignerStan/network-data-plane-worker/go/secrets"
	workerconfig "github.com/wignerStan/remote-storage-gateway/go/contracts/worker"
	"github.com/wignerStan/remote-storage-gateway/go/gateway"
	gatewayhttp "github.com/wignerStan/remote-storage-gateway/go/gateway/httpapi"
)

func TestNativeArchiveOverIndependentVFSKeepsPackedCache(t *testing.T) {
	artifactPath := os.Getenv("REMOTE_GATEWAY_RCLONE_PLUGIN")
	if artifactPath == "" {
		t.Skip("requires source-built native rclone plugin")
	}
	cfg, source := setupTransport(t, true)
	var pack bytes.Buffer
	writer := zip.NewWriter(&pack)
	const members = 128
	for i := 0; i < members; i++ {
		entry, err := writer.CreateHeader(&zip.FileHeader{Name: fmt.Sprintf("tiny-%03d.txt", i), Method: zip.Store})
		if err != nil {
			t.Fatal(err)
		}
		if _, err = io.WriteString(entry, strings.Repeat(fmt.Sprintf("payload-%03d", i), 256)); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	source.pack = pack.Bytes()
	// Exceed ZIP's footer/index tail so members require real VFS range reads.
	if len(source.pack) <= 65557 {
		t.Fatal("fixture fits entirely in ZIP footer cache")
	}
	cfg.MaxCacheEntries = 2
	cfg.MaxOpenFiles = 8
	cfg.MaxConcurrentRequests = 4
	materializer := start(t, cfg)
	info, err := materializer.Info(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	binary, err := os.ReadFile(artifactPath)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(binary)
	artifact := nativeclient.Artifact{Path: artifactPath, SHA256: hex.EncodeToString(digest[:]), PluginID: "client.rclone", Language: "go"}
	record, err := nativeclient.Inspect(context.Background(), artifact.Path, artifact.SHA256, artifact.PluginID, artifact.Language)
	if err != nil {
		t.Fatal(err)
	}
	artifact.DescriptorDigest = record.Digest
	factory, err := nativeclient.NewFactory(context.Background(), artifact)
	if err != nil {
		t.Fatal(err)
	}
	public, _ := json.Marshal(map[string]string{"url": info.DataEndpoint, "username": "cache", "archive_path": "pack.zip"})
	candidate, err := factory.Prepare(context.Background(), port.PluginConfig{ImplementationID: "archive-webdav", Public: public, Secrets: secrets.FileResolver{"password": cfg.LocalPasswordFile}})
	if err != nil {
		t.Fatal(err)
	}
	published := false
	defer func() {
		if !published {
			_ = candidate.Abort(context.Background())
		}
	}()
	if err = candidate.Validate(context.Background()); err != nil {
		t.Fatal(err)
	}
	client, err := candidate.Publish()
	if err != nil {
		t.Fatal(err)
	}
	published = true
	defer func() {
		if err := client.Close(context.Background()); err != nil {
			t.Error(err)
		}
	}()
	flow, err := gateway.New(workerconfig.RuntimeConfig{Username: "front", Worker: dp.Bootstrap{Clients: []dp.Selection{{Name: "archive"}}}, Mounts: []workerconfig.Mount{{Name: "archive", Client: "archive", RemoteClient: "archive", ReadOnly: true}}}, fixtureManager{client}, []byte(dataPassword))
	if err != nil {
		t.Fatal(err)
	}
	defer flow.Close(context.Background())
	router := gin.New()
	gatewayhttp.RegisterGin(router, flow, http.NotFoundHandler(), http.NotFoundHandler())
	frontend := httptest.NewServer(router)
	defer frontend.Close()
	readPass := func() {
		t.Helper()
		jobs := make(chan int)
		errs := make(chan error, members)
		var work sync.WaitGroup
		for i := 0; i < 4; i++ {
			work.Add(1)
			go func() {
				defer work.Done()
				for n := range jobs {
					req, _ := http.NewRequest("GET", fmt.Sprintf("%s/dav/archive/tiny-%03d.txt", frontend.URL, n), nil)
					req.SetBasicAuth("front", dataPassword)
					resp, err := frontend.Client().Do(req)
					if err != nil {
						errs <- err
						continue
					}
					data, err := io.ReadAll(resp.Body)
					_ = resp.Body.Close()
					if err != nil || resp.StatusCode != 200 || string(data) != strings.Repeat(fmt.Sprintf("payload-%03d", n), 256) {
						errs <- fmt.Errorf("member%d: status%d data%q err%v", n, resp.StatusCode, data, err)
					}
				}
			}()
		}
		for n := 0; n < members; n++ {
			jobs <- n
		}
		close(jobs)
		work.Wait()
		close(errs)
		for err := range errs {
			t.Error(err)
		}
	}
	readPass()
	firstGets := source.remoteGets.Load()
	readPass()
	if source.publicRequests.Load() != 0 {
		t.Fatal("archive materialization used public TLS instead of approved raw socket")
	}
	if firstGets < 1 || source.remoteGets.Load() != firstGets {
		t.Fatalf("second pass fetched remote ZIP: %d -> %d", firstGets, source.remoteGets.Load())
	}
	count := 0
	if err := filepath.WalkDir(cfg.Directory, func(_ string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.Type().IsRegular() {
			count++
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if count > 4 {
		t.Fatalf("packed cache grew per member: %d regular files for %d members", count, members)
	}
	req, _ := http.NewRequest("PUT", frontend.URL+"/dav/archive/tiny-000.txt", bytes.NewBufferString("mutation"))
	req.SetBasicAuth("front", dataPassword)
	resp, err := frontend.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != 403 {
		t.Fatalf("archive mutated: %d", resp.StatusCode)
	}
	if _, err = client.Put(context.Background(), port.PutRequest{Path: "tiny-000.txt", Size: 1, Body: bytes.NewBufferString("x")}); err == nil {
		t.Fatal("native archive accepted put")
	}
	if err = client.Delete(context.Background(), port.ObjectRef{Path: "tiny-000.txt"}); err == nil {
		t.Fatal("native archive accepted delete")
	}
	t.Logf("%d members, two passes with four concurrent readers each, %d origin GETs, %d local cache files", members, source.remoteGets.Load(), count)
	// Use a bounded waiter while actual cache retirement remains owned by runtime.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err = client.Close(ctx); err != nil {
		t.Fatal(err)
	}
}
