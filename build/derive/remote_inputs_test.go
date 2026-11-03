package main

import (
	"strings"
	"testing"
)

func TestRemoteBuildEnvironmentRejectsInjection(t *testing.T) {
	values := map[string]string{"gateway_digest": "sha256:abc", "rclone_digest": "sha256:def", "product_commit": "123456", "ui_version": "4.2.9", "rclone_version": "v1.75.2"}
	raw, err := remoteBuildEnv(values)
	if err != nil || !strings.Contains(string(raw), "rclone_version=v1.75.2\n") {
		t.Fatalf("invalid build metadata: %s %v", raw, err)
	}
	for _, injected := range []string{"bad\nTOKEN=secret", "$(read-secret)", "bad value", "`command`"} {
		values["ui_version"] = injected
		if _, err := remoteBuildEnv(values); err == nil {
			t.Fatalf("unsafe environment value accepted: %q", injected)
		}
	}
}
