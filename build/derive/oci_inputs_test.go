package main

import (
	"encoding/json"
	buildinputs "github.com/wignerStan/infra/go/code/build/source"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func ociFixture(t *testing.T) *builder {
	t.Helper()
	base := t.TempDir()
	b := &builder{root: filepath.Join(base, "packager"), output: filepath.Join(base, "output"), gatewayBuild: filepath.Join(base, "gateway-build"), gatewaySource: filepath.Join(base, "gateway-source"), rcloneBuild: filepath.Join(base, "rclone-build"), materializer: filepath.Join(base, "rclone-source"), ca: filepath.Join(base, "ca")}
	put := func(path, body string) {
		t.Helper()
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(body), 0644); err != nil {
			t.Fatal(err)
		}
	}
	git := func(root string, args ...string) string {
		t.Helper()
		s, err := b.git(root, args...)
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	for _, root := range []string{b.root, b.gatewaySource, b.materializer} {
		put(filepath.Join(root, "README"), "fixture source, not a built ELF")
		git(root, "init")
		git(root, "config", "user.name", "Fixture")
		git(root, "config", "user.email", "fixture@example.invalid")
	}
	oldAPI := filepath.Join(b.gatewaySource, "linked-rclone.go")
	newAPI := filepath.Join(b.materializer, "go/api.go")
	put(oldAPI, "package api\n")
	put(newAPI, "package api\n")
	for _, root := range []string{b.root, b.gatewaySource, b.materializer} {
		git(root, "add", ".")
		git(root, "commit", "-m", "fixture")
	}
	gatewayCommit := git(b.gatewaySource, "rev-parse", "HEAD")
	cliCommit := git(b.materializer, "rev-parse", "HEAD")
	apiHash, _ := hashFile(oldAPI)
	conf := map[string]string{"product_commit": gatewayCommit, "gateway_digest": "sha256:" + strings.Repeat("a", 64), "ui_dist_digest": strings.Repeat("b", 64), "rclone_version": "v1.75.2"}
	gm := remoteInputManifest{Schema: "storage-gateway/build-inputs/v1", Config: conf, Files: []buildinputs.File{{Path: "workspace/rclone/go/api.go", Source: oldAPI, SHA256: apiHash, Mode: "0644"}}}
	if err := writeJSON(filepath.Join(b.gatewayBuild, "build-inputs/manifest.json"), gm); err != nil {
		t.Fatal(err)
	}
	inputHash, _ := hashFile(filepath.Join(b.gatewayBuild, "build-inputs/manifest.json"))
	release := nativeReleaseReceipt{Schema: "storage-gateway/native-release/v1", InputManifestSHA256: inputHash, Config: conf, Execution: "local-go", Artifacts: map[string]nativeArtifactReceipt{}}
	modules := map[string]string{"gateway": "github.com/OpenListTeam/OpenList/v4", "openlist-control-plane": "github.com/OpenListTeam/OpenList/v4", "client-openlist.so": "github.com/OpenListTeam/OpenList/v4", "storage-worker": "github.com/wignerStan/remote-storage-gateway/go", "client-rclone.so": "github.com/wignerStan/remote-storage-gateway/go", "transfer-reapi.so": "github.com/wignerStan/remote-storage-gateway/go", "gateway-admin": "github.com/wignerStan/remote-storage-gateway/go"}
	for name, module := range modules {
		put(filepath.Join(b.gatewayBuild, "bin", name), "fixture bytes "+name)
		hash, _ := hashFile(filepath.Join(b.gatewayBuild, "bin", name))
		record := nativeArtifactReceipt{Schema: "storage-gateway/native-build/v1", InputManifestSHA256: inputHash, Artifact: name, SHA256: hash, ProductCommit: gatewayCommit, BuildDigest: conf["gateway_digest"], UIDistDigest: conf["ui_dist_digest"], GoVersion: "1.27.3", Module: module, Execution: "local-go", Command: []string{"go", "build"}}
		release.Artifacts[name] = record
		if err := writeJSON(filepath.Join(b.gatewayBuild, "receipts", name+".json"), record); err != nil {
			t.Fatal(err)
		}
	}
	if err := writeJSON(filepath.Join(b.gatewayBuild, "receipts/native-release.json"), release); err != nil {
		t.Fatal(err)
	}
	put(filepath.Join(b.rcloneBuild, "bin/rclone"), "fixture independent CLI")
	cliHash, _ := hashFile(filepath.Join(b.rcloneBuild, "bin/rclone"))
	cliDigest := "sha256:" + strings.Repeat("c", 64)
	cm := remoteInputManifest{Schema: "rclone/build-inputs/v1", Config: map[string]string{"product_commit": cliCommit, "rclone_digest": cliDigest, "rclone_version": "v1.75.2"}, Files: []buildinputs.File{{Path: "workspace/rclone/go/api.go", Source: newAPI, SHA256: apiHash, Mode: "0644"}}}
	if err := writeJSON(filepath.Join(b.rcloneBuild, "build-inputs/manifest.json"), cm); err != nil {
		t.Fatal(err)
	}
	cliInputHash, _ := hashFile(filepath.Join(b.rcloneBuild, "build-inputs/manifest.json"))
	owner := rcloneNativeOwner{Schema: "rclone-native-build/v1", Binary: filepath.Join(b.rcloneBuild, "bin/rclone"), Execution: "local-go", GoVersion: "1.27.3", InputManifestSHA256: cliInputHash, BuildDigest: cliDigest, Product: ociSource{Root: b.materializer, Commit: cliCommit}, SHA256: cliHash, Source: json.RawMessage(`{"schema":"go-module-product-source/v1","module":"github.com/rclone/rclone","version":"v1.75.2"}`)}
	if err := writeJSON(filepath.Join(b.rcloneBuild, "receipts/rclone.json"), owner); err != nil {
		t.Fatal(err)
	}
	if err := writeJSON(filepath.Join(b.rcloneBuild, "receipts/rclone.build.json"), map[string]string{"schema": "rclone/native-build/v1", "binary_sha256": cliHash, "native_build_digest": cliDigest, "go_version": "1.27.3", "rclone_version": "v1.75.2"}); err != nil {
		t.Fatal(err)
	}
	put(b.ca, "public CA fixture")
	return b
}
func TestOCIAdmissionPreservesSeparateNativeOwnersWithoutCopyingPrograms(t *testing.T) {
	b := ociFixture(t)
	original, _ := os.ReadFile(filepath.Join(b.gatewayBuild, "receipts/native-release.json"))
	if err := b.ociInputs(); err != nil {
		t.Fatal(err)
	}
	var assembly ociAssembly
	if err := readJSON(filepath.Join(b.output, "oci-inputs/assembly.json"), &assembly); err != nil {
		t.Fatal(err)
	}
	if len(assembly.Artifacts) != 9 || assembly.Gateway.Source.Commit == assembly.Rclone.Source.Commit || assembly.Gateway.Execution != "local-go" || assembly.Rclone.Execution != "local-go" || assembly.RcloneAPIFiles != 1 {
		t.Fatalf("incorrect owner composition: %+v", assembly)
	}
	after, _ := os.ReadFile(filepath.Join(b.gatewayBuild, "receipts/native-release.json"))
	if string(original) != string(after) {
		t.Fatal("original WITH_RCLONE=0 receipt was rewritten")
	}
	files, _ := os.ReadDir(filepath.Join(b.output, "oci-inputs"))
	if len(files) != 3 {
		t.Fatal("OCI admission copied native binaries instead of referencing their owners")
	}
}
func TestOCIAdmissionRejectsChangedOrSymlinkCLIAndPreservesPreviousInputs(t *testing.T) {
	for _, kind := range []string{"changed", "symlink"} {
		t.Run(kind, func(t *testing.T) {
			b := ociFixture(t)
			if err := b.ociInputs(); err != nil {
				t.Fatal(err)
			}
			name := filepath.Join(b.output, "oci-inputs/assembly.json")
			before, _ := os.ReadFile(name)
			binary := filepath.Join(b.rcloneBuild, "bin/rclone")
			if kind == "changed" {
				if err := os.WriteFile(binary, []byte("changed CLI"), 0644); err != nil {
					t.Fatal(err)
				}
			} else {
				saved := binary + ".saved"
				if err := os.Rename(binary, saved); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(saved, binary); err != nil {
					t.Fatal(err)
				}
			}
			if err := b.ociInputs(); err == nil {
				t.Fatal("changed independent CLI admitted")
			}
			after, _ := os.ReadFile(name)
			if string(before) != string(after) {
				t.Fatal("failed admission replaced previous complete inputs")
			}
		})
	}
}
