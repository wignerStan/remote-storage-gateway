package main

import (
	"archive/tar"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	buildinputs "github.com/wignerStan/infra/go/code/build/source"
)

func TestRemoteManifestHasItsOwnBoundedReader(t *testing.T) {
	manifest := remoteInputManifest{Schema: "storage-gateway/build-inputs/v1", Config: map[string]string{"product_commit": "reviewed"}}
	for i := 0; i < 12000; i++ {
		manifest.Files = append(manifest.Files, buildinputs.File{Path: fmt.Sprintf("workspace/source/%06d.go", i), Source: "/admitted/source/module/version/path/package.go", SHA256: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", Mode: "0644"})
	}
	raw, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) <= 1<<20 {
		t.Fatal("fixture must exceed the ordinary metadata bound")
	}
	file := filepath.Join(t.TempDir(), "manifest.json")
	if err = os.WriteFile(file, raw, 0644); err != nil {
		t.Fatal(err)
	}
	var read remoteInputManifest
	if err = readJSON(file, &read); err == nil {
		t.Fatal("ordinary config bound was weakened")
	}
	if err = readJSONLimit(file, &read, 128<<20); err != nil || len(read.Files) != len(manifest.Files) {
		t.Fatalf("real-size manifest unreadable: %d %v", len(read.Files), err)
	}
	if err = readJSONLimit(file, &read, int64(len(raw)-1)); err == nil {
		t.Fatal("oversized manifest accepted")
	}
}

func TestUpstreamPackageVersionAllowsItsOwnMetadata(t *testing.T) {
	file := filepath.Join(t.TempDir(), "package.json")
	for _, fixture := range []struct{ body, version string }{
		{`{"name":"openlist-frontend","version":"4.2.9","private":true,"scripts":{"build":"vite build"},"dependencies":{"solid-js":"1.9.9"}}`, "4.2.9"},
		{`{"name":"missing-version"}`, ""},
		{`{"version":42}`, ""},
	} {
		if err := os.WriteFile(file, []byte(fixture.body), 0644); err != nil {
			t.Fatal(err)
		}
		version, err := readUIVersion(file)
		if fixture.version == "" {
			if err == nil {
				t.Fatal("invalid upstream version accepted")
			}
		} else if err != nil || version != fixture.version {
			t.Fatalf("valid upstream package rejected: %q %v", version, err)
		}
	}
}

func TestRemoteSnapshotRejectsRelabelledAndChangedInputs(t *testing.T) {
	file := filepath.Join(t.TempDir(), "source.go")
	if err := os.WriteFile(file, []byte("original"), 0644); err != nil {
		t.Fatal(err)
	}
	hash, err := hashFile(file)
	if err != nil {
		t.Fatal(err)
	}
	expected := map[string]string{"gateway_digest": "reviewed-gateway", "rclone_digest": "independent-rclone"}
	manifest := remoteInputManifest{Schema: "storage-gateway/build-inputs/v1", Config: expected, Files: []buildinputs.File{{Path: "workspace/source.go", Source: file, SHA256: hash, Mode: "0644"}}}
	if err := validateRemoteSnapshot(manifest, expected); err != nil {
		t.Fatal(err)
	}
	if err := validateRemoteSnapshot(manifest, map[string]string{"gateway_digest": "changed-gateway", "rclone_digest": "independent-rclone"}); err == nil {
		t.Fatal("stale output can be relabelled as changed source")
	}
	if err := validateRemoteSnapshot(manifest, map[string]string{"gateway_digest": "reviewed-gateway", "rclone_digest": "gateway-digest-used-for-rclone"}); err == nil {
		t.Fatal("independent producer identity lost")
	}
	if err := os.WriteFile(file, []byte("edited after preparation"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := validateRemoteSnapshot(manifest, expected); err == nil {
		t.Fatal("mutated captured input accepted")
	}
}

func TestRemoteUIArchiveAdmission(t *testing.T) {
	for _, kind := range []string{"valid", "dot-prefix", "traversal", "link", "duplicate", "duplicate-alias", "missing-index", "oversized"} {
		t.Run(kind, func(t *testing.T) {
			root := t.TempDir()
			archive := filepath.Join(root, "ui.tar")
			file, err := os.Create(archive)
			if err != nil {
				t.Fatal(err)
			}
			writer := tar.NewWriter(file)
			write := func(name string, flag byte, size int64) {
				t.Helper()
				if err := writer.WriteHeader(&tar.Header{Name: name, Typeflag: flag, Size: size, Mode: 0644, Linkname: "outside"}); err != nil {
					t.Fatal(err)
				}
				if flag == tar.TypeReg && size == 2 {
					if _, err := writer.Write([]byte("ok")); err != nil {
						t.Fatal(err)
					}
				}
			}
			switch kind {
			case "traversal":
				write("../escape", tar.TypeReg, 2)
			case "link":
				write("index.html", tar.TypeSymlink, 0)
			case "duplicate":
				write("index.html", tar.TypeReg, 2)
				write("index.html", tar.TypeReg, 2)
			case "duplicate-alias":
				write("index.html", tar.TypeReg, 2)
				write("./index.html", tar.TypeReg, 2)
			case "dot-prefix":
				write("./", tar.TypeDir, 0)
				write("./assets/", tar.TypeDir, 0)
				write("./index.html", tar.TypeReg, 2)
				write("./assets/style.css", tar.TypeReg, 2)
			case "missing-index":
				write("other.html", tar.TypeReg, 2)
			case "oversized":
				write("index.html", tar.TypeReg, 129<<20)
			default:
				write("index.html", tar.TypeReg, 2)
				write("assets/style.css", tar.TypeReg, 2)
			}
			// An oversized header is enough to exercise admission before reading data.
			_ = writer.Close()
			if err := file.Close(); err != nil {
				t.Fatal(err)
			}
			destination := filepath.Join(root, "dist")
			if err := os.Mkdir(destination, 0755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(destination, "previous"), []byte("preserve"), 0644); err != nil {
				t.Fatal(err)
			}
			err = extractRemoteUI(archive, destination)
			if kind == "valid" || kind == "dot-prefix" {
				if err != nil {
					t.Fatal(err)
				}
				if data, err := os.ReadFile(filepath.Join(destination, "index.html")); err != nil || string(data) != "ok" {
					t.Fatalf("bad UI: %q %v", data, err)
				}
			} else {
				if err == nil {
					t.Fatal("unsafe archive accepted")
				}
				if data, err := os.ReadFile(filepath.Join(destination, "previous")); err != nil || string(data) != "preserve" {
					t.Fatalf("previous UI lost: %q %v", data, err)
				}
				if _, err := os.Stat(filepath.Join(root, "escape")); !os.IsNotExist(err) {
					t.Fatalf("traversal escaped: %v", err)
				}
			}
		})
	}
}

func TestNativeReleaseRejectsStaleLinkManifestAndIncompleteArtifacts(t *testing.T) {
	config := map[string]string{"gateway_digest": "source-ui-sdk-cc", "product_commit": "commit", "ui_dist_digest": "ui", "native_sdk_receipt_sha256": "sdk-one"}
	modules := map[string]string{"gateway": "github.com/OpenListTeam/OpenList/v4", "openlist-control-plane": "github.com/OpenListTeam/OpenList/v4", "client-openlist.so": "github.com/OpenListTeam/OpenList/v4", "storage-worker": "github.com/wignerStan/remote-storage-gateway/go", "client-rclone.so": "github.com/wignerStan/remote-storage-gateway/go", "transfer-reapi.so": "github.com/wignerStan/remote-storage-gateway/go", "gateway-admin": "github.com/wignerStan/remote-storage-gateway/go"}
	release := nativeReleaseReceipt{Schema: "storage-gateway/native-release/v1", InputManifestSHA256: "manifest-one", Config: config, Execution: "local-go", Artifacts: map[string]nativeArtifactReceipt{}}
	for name, module := range modules {
		release.Artifacts[name] = nativeArtifactReceipt{Schema: "storage-gateway/native-build/v1", InputManifestSHA256: "manifest-one", Artifact: name, SHA256: strings.Repeat("a", 64), ProductCommit: "commit", BuildDigest: config["gateway_digest"], UIDistDigest: "ui", GoVersion: "1.27.3", Module: module, Execution: "local-go", Command: []string{"go", "build"}}
	}
	if err := validateNativeRelease(release, config, "manifest-one", false); err != nil {
		t.Fatal(err)
	}
	changed := map[string]string{}
	for k, v := range config {
		changed[k] = v
	}
	changed["native_sdk_receipt_sha256"] = "sdk-two"
	if validateNativeRelease(release, changed, "manifest-one", false) == nil {
		t.Fatal("release admitted after SDK changed")
	}
	if validateNativeRelease(release, config, "manifest-two", false) == nil {
		t.Fatal("release admitted with different source closure")
	}
	delete(release.Artifacts, "transfer-reapi.so")
	if validateNativeRelease(release, config, "manifest-one", false) == nil {
		t.Fatal("incomplete native release admitted")
	}
}
func TestImageCompositionRejectsChangedArtifact(t *testing.T) {
	b := &builder{output: t.TempDir()}
	b.ca = filepath.Join(b.output, "ca")
	if err := os.MkdirAll(filepath.Join(b.output, "bin"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(b.output, "bin/storage-worker"), []byte("replaced binary"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := b.copyImageCompositions(filepath.Join(b.output, "composition"), map[string]string{"storage-worker": strings.Repeat("a", 64)}); err == nil {
		t.Fatal("changed artifact copied into image inputs")
	}
}
func TestSDKFlagsRejectExternalLinkInputs(t *testing.T) {
	prefix := t.TempDir()
	for _, name := range []string{"include", "lib"} {
		if err := os.MkdirAll(filepath.Join(prefix, name), 0755); err != nil {
			t.Fatal(err)
		}
	}
	flags := "-L" + filepath.Join(prefix, "lib") + " -linfra_shm_snapshot"
	if err := validateSDKFlags(prefix, "-I"+filepath.Join(prefix, "include"), flags); err != nil {
		t.Fatal(err)
	}
	if err := validateSDKFlags(prefix, "-I"+filepath.Join(prefix, "include"), flags+" /external/libforeign.a"); err == nil {
		t.Fatal("unverified archive admitted")
	}
}
