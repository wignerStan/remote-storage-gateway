package controlplane

import (
	"os"
	"testing"
)

func TestProxySnapshotPreservesExplicitEmptyAndExcludesSecrets(t *testing.T) {
	for _, key := range []string{"HTTP_PROXY", "HTTPS_PROXY", "NO_PROXY", "http_proxy", "https_proxy", "no_proxy"} {
		t.Setenv(key, "")
	}
	t.Setenv("HTTP_PROXY", "http://fixture.invalid:3128")
	t.Setenv("WORKER_PRIVATE_SECRET", "not-a-proxy")
	if e := os.Unsetenv("no_proxy"); e != nil {
		t.Fatal(e)
	}
	got := proxyEnvironment()
	if len(got) != 5 || got["HTTP_PROXY"] != "http://fixture.invalid:3128" {
		t.Fatal("approved proxy settings omitted")
	}
	if value, present := got["HTTPS_PROXY"]; !present || value != "" {
		t.Fatal("explicit empty proxy became unset")
	}
	if _, present := got["no_proxy"]; present {
		t.Fatal("unset proxy became explicit empty")
	}
	if _, present := got["WORKER_PRIVATE_SECRET"]; present {
		t.Fatal("secret captured as proxy setting")
	}
}
