package runtime

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestSmallFileMetadataDoesNotMaterializeAndRepeatedReadsHitCache(t *testing.T) {
	cfg, source := setupTransport(t, true)
	source.smallFiles = 128
	cfg.MaxConcurrentRequests = 4
	app := start(t, cfg)
	state, err := app.Info(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Timeout: 5 * time.Second}
	t.Cleanup(client.CloseIdleConnections)
	request := func(method, name string) error {
		req, _ := http.NewRequest(method, state.DataEndpoint+name, nil)
		req.SetBasicAuth("cache", localPassword)
		if method == "PROPFIND" {
			req.Header.Set("Depth", "1")
		}
		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		body, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if err != nil {
			return err
		}
		want := 200
		if method == "PROPFIND" {
			want = 207
		}
		if resp.StatusCode != want {
			return fmt.Errorf("%s %s: status %d", method, name, resp.StatusCode)
		}
		if method == "GET" && string(body) != "tiny" {
			return fmt.Errorf("wrong small-file body: %q", body)
		}
		return nil
	}
	burst := func(method string) {
		t.Helper()
		jobs := make(chan int)
		errs := make(chan error, source.smallFiles)
		var workers sync.WaitGroup
		for n := 0; n < 4; n++ {
			workers.Add(1)
			go func() {
				defer workers.Done()
				for i := range jobs {
					if err := request(method, fmt.Sprintf("small-%d.bin", i)); err != nil {
						errs <- err
					}
				}
			}()
		}
		for i := 0; i < source.smallFiles; i++ {
			jobs <- i
		}
		close(jobs)
		workers.Wait()
		close(errs)
		for err := range errs {
			t.Error(err)
		}
	}
	burst("HEAD")
	// Unknown extensions, conditional GET and invalid ranges must remain metadata-only.
	head, _ := http.NewRequest("HEAD", state.DataEndpoint+"stalled", nil)
	head.SetBasicAuth("cache", localPassword)
	response, err := client.Do(head)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != 200 || response.Header.Get("Content-Type") != "application/octet-stream" {
		t.Fatal("extensionless HEAD", response.StatusCode, response.Header)
	}
	etag := response.Header.Get("ETag")
	if etag == "" {
		t.Fatal("HEAD omitted ETag")
	}
	for _, test := range []struct {
		header, value string
		status        int
	}{{"If-None-Match", etag, 304}, {"Range", "bytes=65536-70000", 416}} {
		req, _ := http.NewRequest("GET", state.DataEndpoint+"stalled", nil)
		req.SetBasicAuth("cache", localPassword)
		req.Header.Set(test.header, test.value)
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != test.status {
			t.Fatal(test.header, resp.StatusCode)
		}
	}
	if err := request("PROPFIND", ""); err != nil {
		t.Fatal(err)
	}
	if got := source.opens.Load(); got != 0 {
		t.Fatalf("metadata opened %d payloads", got)
	}
	// Metadata may create cache directories, but no cache payload or metadata file.
	if err := filepath.WalkDir(cfg.Directory, func(name string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.Type().IsRegular() {
			return fmt.Errorf("metadata populated cache: %s", name)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	burst("GET")
	first := source.remoteGets.Load()
	burst("GET")
	if first != int32(source.smallFiles) || source.remoteGets.Load() != first {
		t.Fatalf("cache misses/hits: first=%d final=%d files=%d", first, source.remoteGets.Load(), source.smallFiles)
	}
	t.Logf("%d tiny files: metadata payload opens=0; origin GETs after two passes=%d", source.smallFiles, source.remoteGets.Load())
}

func TestRequestAdmissionFailsFastAndReleasesAfterCancellation(t *testing.T) {
	cfg, source := setupTransport(t, true)
	cfg.MaxConcurrentRequests = 1
	app := start(t, cfg)
	info, err := app.Info(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		req, _ := http.NewRequestWithContext(ctx, "GET", info.DataEndpoint+"stalled", nil)
		req.SetBasicAuth("cache", localPassword)
		resp, e := http.DefaultClient.Do(req)
		if e == nil {
			_, e = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
		}
		done <- e
	}()
	wait(t, source.started, "admitted payload")
	req, _ := http.NewRequest("HEAD", info.DataEndpoint+"item", nil)
	req.SetBasicAuth("cache", localPassword)
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusServiceUnavailable || response.Header.Get("Retry-After") == "" {
		t.Fatalf("busy response: %d %v", response.StatusCode, response.Header)
	}
	health, err := http.Get(info.DataEndpoint[:len(info.DataEndpoint)-len("dav/")] + "healthz")
	if err != nil {
		t.Fatal(err)
	}
	_ = health.Body.Close()
	if health.StatusCode != 204 {
		t.Fatal("health consumed request slot")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("cancelled request retained admission")
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		response, err = http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = response.Body.Close()
		if response.StatusCode == 200 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("slot not released: %d", response.StatusCode)
		}
		time.Sleep(time.Millisecond)
	}
	if source.itemOpens.Load() != 0 {
		t.Fatal("rejected/HEAD requests opened payload", source.itemOpens.Load())
	}
}
