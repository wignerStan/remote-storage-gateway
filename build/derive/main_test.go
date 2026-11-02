package main

import (
	"encoding/json"
	"golang.org/x/mod/sumdb/dirhash"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func fixture(t *testing.T) (*builder, relationship) {
	t.Helper()
	root := t.TempDir()
	b := &builder{root: root, output: filepath.Join(root, "output")}
	source := filepath.Join(root, "third_party/fixture")
	if e := os.MkdirAll(source, 0755); e != nil {
		t.Fatal(e)
	}
	git := func(args ...string) string {
		t.Helper()
		s, e := b.git(source, args...)
		if e != nil {
			t.Fatal(e)
		}
		return s
	}
	git("init")
	git("config", "user.name", "Fixture")
	git("config", "user.email", "fixture@example.invalid")
	git("remote", "add", "origin", "https://example.invalid/upstream.git")
	if e := os.WriteFile(filepath.Join(source, "provider.txt"), []byte("upstream\n"), 0644); e != nil {
		t.Fatal(e)
	}
	git("add", "provider.txt")
	git("commit", "-m", "upstream fixture")
	commit := git("rev-parse", "HEAD")
	if e := os.WriteFile(filepath.Join(source, "provider.txt"), []byte("patched\n"), 0644); e != nil {
		t.Fatal(e)
	}
	git("add", "provider.txt")
	tree := git("write-tree")
	diff := git("diff", "--cached", "--binary", "--full-index") + "\n"
	p := filepath.Join(root, "patches/fixture/0001.patch")
	if e := os.MkdirAll(filepath.Dir(p), 0755); e != nil {
		t.Fatal(e)
	}
	if e := os.WriteFile(p, []byte(diff), 0644); e != nil {
		t.Fatal(e)
	}
	git("reset", "--hard", "HEAD")
	if _, e := b.git(root, "init"); e != nil {
		t.Fatal(e)
	}
	if _, e := b.git(root, "update-index", "--add", "--cacheinfo", "160000,"+commit+",third_party/fixture"); e != nil {
		t.Fatal(e)
	}
	for _, pair := range [][2]string{{"user.name", "Fixture"}, {"user.email", "fixture@example.invalid"}} {
		if _, e := b.git(root, "config", pair[0], pair[1]); e != nil {
			t.Fatal(e)
		}
	}
	if _, e := b.git(root, "commit", "-m", "own exact source pin"); e != nil {
		t.Fatal(e)
	}
	hash, e := hashFile(p)
	if e != nil {
		t.Fatal(e)
	}
	r := relationship{Schema: "git-source-relationship/v1", Kind: "patched_third_party", Path: "third_party/fixture", Integration: "submodule", Upstream: upstream{URL: "https://example.invalid/upstream.git", Commit: commit}, Patches: []patch{{Order: 1, Path: "patches/fixture/0001.patch", SHA256: hash}}, ResultTree: &tree}
	return b, r
}

func TestPatchReplayPreservesPristineSource(t *testing.T) {
	b, r := fixture(t)
	destination := b.sourcePath("fixture")
	if e := b.materialize(r, destination); e != nil {
		t.Fatal(e)
	}
	got, e := os.ReadFile(filepath.Join(destination, "provider.txt"))
	if e != nil || string(got) != "patched\n" {
		t.Fatalf("derived content %q %v", got, e)
	}
	got, e = os.ReadFile(filepath.Join(b.root, r.Path, "provider.txt"))
	if e != nil || string(got) != "upstream\n" {
		t.Fatalf("upstream modified %q %v", got, e)
	}
	if e = b.verifyDerived(r); e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(filepath.Join(destination, "provider.txt"), []byte("unrecorded\n"), 0644); e != nil {
		t.Fatal(e)
	}
	if e = b.verifyDerived(r); e == nil {
		t.Fatal("unrecorded materialized edit accepted")
	}
}

func TestPatchAdmissionRejectsTamperAndDirtyUpstreamBeforeClone(t *testing.T) {
	for _, name := range []string{"hash", "order", "traversal", "dirty", "parent-pin"} {
		t.Run(name, func(t *testing.T) {
			b, r := fixture(t)
			switch name {
			case "hash":
				r.Patches[0].SHA256 = strings.Repeat("0", 64)
			case "order":
				r.Patches[0].Order = 2
			case "traversal":
				r.Patches[0].Path = "patches/../../outside.patch"
			case "dirty":
				if e := os.WriteFile(filepath.Join(b.root, r.Path, "provider.txt"), []byte("dirty"), 0644); e != nil {
					t.Fatal(e)
				}
			case "parent-pin":
				if _, e := b.git(b.root, "update-index", "--add", "--cacheinfo", "160000,"+strings.Repeat("f", 40)+",third_party/fixture"); e != nil {
					t.Fatal(e)
				}
			}
			destination := b.sourcePath("fixture")
			if e := b.materialize(r, destination); e == nil {
				t.Fatal("invalid input accepted")
			}
			if _, e := os.Lstat(destination); !os.IsNotExist(e) {
				t.Fatal("materialized before input admission")
			}
		})
	}
}

func TestWrongResultTreePublishesNoSourceReceipt(t *testing.T) {
	b, r := fixture(t)
	wrong := strings.Repeat("0", 40)
	r.ResultTree = &wrong
	if e := b.materialize(r, b.sourcePath("fixture")); e == nil {
		t.Fatal("wrong resulting tree accepted")
	}
	if _, e := os.Stat(filepath.Join(b.output, "receipts/fixture-source.json")); !os.IsNotExist(e) {
		t.Fatal("failed derivation published receipt")
	}
}

func TestUnrecordedSourceFileRejected(t *testing.T) {
	b, r := fixture(t)
	dir := b.sourcePath("fixture")
	if e := b.materialize(r, dir); e != nil {
		t.Fatal(e)
	}
	if e := os.WriteFile(filepath.Join(dir, "injected.go"), []byte("package injected"), 0644); e != nil {
		t.Fatal(e)
	}
	if e := b.verifyDerived(r); e == nil {
		t.Fatal("unrecorded compilation unit accepted")
	}
}

func TestAssetCopyRejectsSymlinkAndDigestTracksContent(t *testing.T) {
	src := t.TempDir()
	if e := os.WriteFile(filepath.Join(src, "index.html"), []byte("original"), 0644); e != nil {
		t.Fatal(e)
	}
	dst := filepath.Join(t.TempDir(), "dist")
	if e := copyTree(src, dst); e != nil {
		t.Fatal(e)
	}
	first, e := treeDigest(dst)
	if e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(filepath.Join(dst, "index.html"), []byte("changed"), 0644); e != nil {
		t.Fatal(e)
	}
	second, e := treeDigest(dst)
	if e != nil {
		t.Fatal(e)
	}
	if first == second {
		t.Fatal("asset mutation not represented")
	}
	if e = os.Symlink("index.html", filepath.Join(src, "alias")); e != nil {
		t.Fatal(e)
	}
	if e = copyTree(src, filepath.Join(t.TempDir(), "dist")); e == nil {
		t.Fatal("symlink asset accepted")
	}
}

func TestBuildWorkspaceOverridesAmbientWorkspace(t *testing.T) {
	t.Setenv("GOWORK", "/tmp/ambient-workspace-that-must-not-be-used/go.work")
	b := &builder{output: t.TempDir()}
	got, e := b.command(t.TempDir(), "/usr/bin/printenv", "GOWORK")
	if e != nil || got != "off" {
		t.Fatalf("ambient workspace leaked: %q %v", got, e)
	}
}

func TestShallowSourceUsesExplicitLocalClone(t *testing.T) {
	b, r := fixture(t)
	if e := os.WriteFile(filepath.Join(b.root, r.Path, ".git/shallow"), []byte(r.Upstream.Commit+"\n"), 0644); e != nil {
		t.Fatal(e)
	}
	if e := b.materialize(r, b.sourcePath("fixture")); e != nil {
		t.Fatal(e)
	}
	shallow, e := b.git(b.sourcePath("fixture"), "rev-parse", "--is-shallow-repository")
	if e != nil || shallow != "true" {
		t.Fatalf("shallow boundary changed: %q %v", shallow, e)
	}
	var receipt struct {
		CloneMode string `json:"clone_mode"`
	}
	data, e := os.ReadFile(filepath.Join(b.output, "receipts/fixture-source.json"))
	if e != nil {
		t.Fatal(e)
	}
	if e = json.Unmarshal(data, &receipt); e != nil {
		t.Fatal(e)
	}
	if receipt.CloneMode != "local-shallow" {
		t.Fatalf("mode=%q", receipt.CloneMode)
	}
}

func TestImageCompositionsContainEmbeddedCPAndIndependentPlugins(t *testing.T) {
	b := &builder{output: t.TempDir()}
	dir := filepath.Join(b.output, "bin")
	if e := os.MkdirAll(dir, 0755); e != nil {
		t.Fatal(e)
	}
	for _, name := range []string{"storage-worker", "rclone", "transfer-reapi.so", "client-openlist.so", "client-rclone.so", "openlist-control-plane"} {
		if e := os.WriteFile(filepath.Join(dir, name), []byte(name), 0644); e != nil {
			t.Fatal(e)
		}
	}
	b.ca = filepath.Join(b.output, "public-ca")
	if e := os.WriteFile(b.ca, []byte("public trust"), 0644); e != nil {
		t.Fatal(e)
	}
	if e := b.imageInputs(); e != nil {
		t.Fatal(e)
	}
	for _, composition := range []string{"gateway", "control-plane"} {
		var manifest map[string]string
		if e := readJSON(filepath.Join(b.output, "image-inputs", composition, "inputs.json"), &manifest); e != nil {
			t.Fatal(e)
		}
		want := map[string]int{"gateway": 5, "control-plane": 7}[composition]
		if len(manifest) != want {
			t.Fatalf("%s has %d inputs", composition, len(manifest))
		}
		for name, digest := range manifest {
			actual, e := hashFile(filepath.Join(b.output, "image-inputs", composition, name))
			if e != nil || actual != digest {
				t.Fatalf("%s digest mismatch", name)
			}
		}
	}
}

func moduleFixture(t *testing.T) (*builder, string, modulePatchSeries) {
	t.Helper()
	b := &builder{root: t.TempDir(), infra: t.TempDir(), output: t.TempDir()}
	cache := t.TempDir()
	for name, value := range map[string]string{"go.mod": "module github.com/rclone/rclone\n\ngo 1.27.3\n", "provider.txt": "upstream\n"} {
		if e := os.WriteFile(filepath.Join(cache, name), []byte(value), 0644); e != nil {
			t.Fatal(e)
		}
	}
	for _, root := range []string{filepath.Join(b.infra, "go/storage/cloud"), filepath.Join(b.infra, "go/storage/vfs"), filepath.Join(b.infra, "patches/rclone")} {
		if e := os.MkdirAll(root, 0755); e != nil {
			t.Fatal(e)
		}
	}
	body := "diff --git a/provider.txt b/provider.txt\n--- a/provider.txt\n+++ b/provider.txt\n@@ -1 +1 @@\n-upstream\n+patched\n"
	patchPath := filepath.Join(b.infra, "patches/rclone/0001.patch")
	if e := os.WriteFile(patchPath, []byte(body), 0644); e != nil {
		t.Fatal(e)
	}
	hash, e := hashFile(patchPath)
	if e != nil {
		t.Fatal(e)
	}
	series := modulePatchSeries{Schema: "go-module-patch-series/v1", Module: rcloneModule, Patches: []patch{{Order: 1, Path: "patches/rclone/0001.patch", SHA256: hash}}}
	if e = writeJSON(filepath.Join(b.infra, "patches/rclone/series.json"), series); e != nil {
		t.Fatal(e)
	}
	selectVersion(t, b, cache, "v1.75.2")
	b.downloadModule = func(module, version string) (moduleDownload, error) {
		sum, e := dirhash.HashDir(cache, module+"@"+version, dirhash.Hash1)
		if e != nil {
			return moduleDownload{}, e
		}
		modSum, e := dirhash.Hash1([]string{"go.mod"}, func(name string) (io.ReadCloser, error) { return os.Open(filepath.Join(cache, name)) })
		return moduleDownload{Path: module, Version: version, Dir: cache, Sum: sum, GoModSum: modSum}, e
	}
	return b, cache, series
}
func selectVersion(t *testing.T, b *builder, cache, version string) {
	t.Helper()
	body := "module example.invalid/infra\n\ngo 1.27.3\n\nrequire github.com/rclone/rclone " + version + "\n"
	if e := os.WriteFile(filepath.Join(b.infra, "go/go.mod"), []byte(body), 0644); e != nil {
		t.Fatal(e)
	}
	sum, e := dirhash.HashDir(cache, rcloneModule+"@"+version, dirhash.Hash1)
	if e != nil {
		t.Fatal(e)
	}
	modSum, e := dirhash.Hash1([]string{"go.mod"}, func(name string) (io.ReadCloser, error) { return os.Open(filepath.Join(cache, name)) })
	if e != nil {
		t.Fatal(e)
	}
	sums := rcloneModule + " " + version + " " + sum + "\n" + rcloneModule + " " + version + "/go.mod " + modSum + "\n"
	if e = os.WriteFile(filepath.Join(b.infra, "go/go.sum"), []byte(sums), 0644); e != nil {
		t.Fatal(e)
	}
}

func TestModuleVersionComesOnlyFromSelectedInfraGoMod(t *testing.T) {
	b, cache, _ := moduleFixture(t)
	selectVersion(t, b, cache, "v1.75.3")
	requested := ""
	download := b.downloadModule
	b.downloadModule = func(module, version string) (moduleDownload, error) {
		requested = module + "@" + version
		return download(module, version)
	}
	if e := b.materializeRcloneModule(); e != nil {
		t.Fatal(e)
	}
	if requested != rcloneModule+"@v1.75.3" {
		t.Fatalf("wrong module selection: %s", requested)
	}
	var receipt moduleSourceReceipt
	if e := readJSON(b.moduleReceiptPath(), &receipt); e != nil {
		t.Fatal(e)
	}
	if receipt.Version != "v1.75.3" || receipt.Module != rcloneModule || receipt.DerivedContentHash == "" {
		t.Fatal("version/measurement missing")
	}
	if _, e := os.Lstat(filepath.Join(b.rcloneModulePath(), ".git")); !os.IsNotExist(e) {
		t.Fatal("Git version authority introduced")
	}
	if _, e := b.load("rclone"); e == nil {
		t.Fatal("Go module accepted as Git relationship")
	}
}

func TestModuleCacheRemainsReadOnlyAndPatchesStayExternal(t *testing.T) {
	b, cache, _ := moduleFixture(t)
	before, e := dirhash.HashDir(cache, rcloneModule+"@v1.75.2", dirhash.Hash1)
	if e != nil {
		t.Fatal(e)
	}
	for _, name := range []string{"go.mod", "provider.txt"} {
		if e = os.Chmod(filepath.Join(cache, name), 0444); e != nil {
			t.Fatal(e)
		}
	}
	if e = os.Chmod(cache, 0555); e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { os.Chmod(cache, 0755) })
	if e = b.materializeRcloneModule(); e != nil {
		t.Fatal(e)
	}
	after, e := dirhash.HashDir(cache, rcloneModule+"@v1.75.2", dirhash.Hash1)
	if e != nil || before != after {
		t.Fatal("canonical cache changed")
	}
	body, e := os.ReadFile(filepath.Join(b.rcloneModulePath(), "provider.txt"))
	if e != nil || string(body) != "patched\n" {
		t.Fatal("external patch was not applied")
	}
}

func TestModuleAndPatchTamperingFailBeforeMaterialization(t *testing.T) {
	for _, kind := range []string{"cache", "download-version", "checksum", "patch", "order", "path", "version-in-series"} {
		t.Run(kind, func(t *testing.T) {
			b, cache, series := moduleFixture(t)
			switch kind {
			case "cache":
				if e := os.WriteFile(filepath.Join(cache, "provider.txt"), []byte("corrupt"), 0644); e != nil {
					t.Fatal(e)
				}
				b.downloadModule = func(module, version string) (moduleDownload, error) {
					sum, modSum, e := b.moduleChecksums(version)
					return moduleDownload{Path: module, Version: version, Dir: cache, Sum: sum, GoModSum: modSum}, e
				}
			case "download-version":
				download := b.downloadModule
				b.downloadModule = func(module, version string) (moduleDownload, error) {
					info, e := download(module, version)
					info.Version = "v0.0.1"
					return info, e
				}
			case "checksum":
				path := filepath.Join(b.infra, "go/go.sum")
				body, _ := os.ReadFile(path)
				if e := os.WriteFile(path, []byte(strings.Replace(string(body), "h1:", "h1:wrong", 1)), 0644); e != nil {
					t.Fatal(e)
				}
			case "patch":
				if e := os.WriteFile(filepath.Join(b.infra, series.Patches[0].Path), []byte("unreviewed"), 0644); e != nil {
					t.Fatal(e)
				}
			case "order":
				series.Patches[0].Order = 2
				if e := writeJSON(filepath.Join(b.infra, "patches/rclone/series.json"), series); e != nil {
					t.Fatal(e)
				}
			case "path":
				series.Patches[0].Path = "patches/../escape.patch"
				if e := writeJSON(filepath.Join(b.infra, "patches/rclone/series.json"), series); e != nil {
					t.Fatal(e)
				}
			case "version-in-series":
				data, _ := json.Marshal(series)
				var raw map[string]any
				json.Unmarshal(data, &raw)
				raw["version"] = "v9.9.9"
				if e := writeJSON(filepath.Join(b.infra, "patches/rclone/series.json"), raw); e != nil {
					t.Fatal(e)
				}
			}
			if e := b.materializeRcloneModule(); e == nil {
				t.Fatal("unadmitted input accepted")
			}
			if _, e := os.Lstat(b.rcloneModulePath()); !os.IsNotExist(e) {
				t.Fatal("invalid source was materialized")
			}
		})
	}
}

func TestModuleSnapshotTamperingIsNotSilentlyReset(t *testing.T) {
	b, _, _ := moduleFixture(t)
	if e := b.materializeRcloneModule(); e != nil {
		t.Fatal(e)
	}
	path := filepath.Join(b.rcloneModulePath(), "provider.txt")
	if e := os.WriteFile(path, []byte("local edit"), 0644); e != nil {
		t.Fatal(e)
	}
	if e := b.materializeRcloneModule(); e == nil {
		t.Fatal("changed snapshot accepted")
	}
	body, _ := os.ReadFile(path)
	if string(body) != "local edit" {
		t.Fatal("changed snapshot silently reset")
	}
}

func TestProductCannotShadowInfraModulePatchAuthority(t *testing.T) {
	b, _, _ := moduleFixture(t)
	if e := writeJSON(filepath.Join(b.root, ".git-source-relationships/rclone.json"), map[string]string{"module": rcloneModule}); e != nil {
		t.Fatal(e)
	}
	if _, e := b.rcloneSeries(); e == nil {
		t.Fatal("legacy Git relationship accepted")
	}
	if e := os.Remove(filepath.Join(b.root, ".git-source-relationships/rclone.json")); e != nil {
		t.Fatal(e)
	}
	if e := os.MkdirAll(filepath.Join(b.root, "patches/rclone"), 0755); e != nil {
		t.Fatal(e)
	}
	if _, e := b.rcloneSeries(); e == nil {
		t.Fatal("duplicate product patch owner accepted")
	}
}

func TestNativeBuildIdentityTracksBackendPatchesAndRecipe(t *testing.T) {
	b, _, _ := moduleFixture(t)
	for _, args := range [][]string{{"init"}, {"config", "user.name", "Fixture"}, {"config", "user.email", "fixture@example.invalid"}, {"commit", "--allow-empty", "-m", "Infra fixture"}} {
		if _, e := b.git(b.infra, args...); e != nil {
			t.Fatal(e)
		}
	}
	productInputs := map[string]string{"go/client.go": "package client\n", ".git-source-relationships/openlist.json": "product source\n", "patches/openlist/0001.patch": "OpenList patch\n", "build/toolchain.json": "tools\n", "Makefile": "flags\n"}
	infraInputs := map[string]string{"go/storage/cloud/backend.go": "package cloud\n", "go/storage/vfs/cache.go": "package vfs\n"}
	for _, input := range []struct {
		root   string
		values map[string]string
	}{{b.root, productInputs}, {b.infra, infraInputs}} {
		for name, content := range input.values {
			path := filepath.Join(input.root, name)
			if e := os.MkdirAll(filepath.Dir(path), 0755); e != nil {
				t.Fatal(e)
			}
			if e := os.WriteFile(path, []byte(content), 0644); e != nil {
				t.Fatal(e)
			}
		}
	}
	for _, name := range []string{"go/go.mod", "go/go.sum", "patches/rclone/series.json", "patches/rclone/0001.patch"} {
		data, e := os.ReadFile(filepath.Join(b.infra, name))
		if e != nil {
			t.Fatal(e)
		}
		infraInputs[name] = string(data)
	}
	baseline, e := b.provenanceDigest()
	if e != nil {
		t.Fatal(e)
	}
	again, e := b.provenanceDigest()
	if e != nil || again != baseline {
		t.Fatal("unstable fingerprint")
	}
	for _, input := range []struct {
		label, root string
		values      map[string]string
	}{{"product", b.root, productInputs}, {"infra", b.infra, infraInputs}} {
		for name, original := range input.values {
			t.Run(input.label+"/"+name, func(t *testing.T) {
				path := filepath.Join(input.root, name)
				if e := os.WriteFile(path, []byte(original+"\n"), 0644); e != nil {
					t.Fatal(e)
				}
				changed, e := b.provenanceDigest()
				if e != nil || changed == baseline {
					t.Fatalf("changed source kept identity: %v", e)
				}
				if e := os.WriteFile(path, []byte(original), 0644); e != nil {
					t.Fatal(e)
				}
			})
		}
	}
}

func TestDataPlaneRemainsIndependentVersionedModule(t *testing.T) {
	base := t.TempDir()
	b := builder{root: filepath.Join(base, "gateway"), dataPlane: filepath.Join(base, "core")}
	for _, item := range []struct{ path, content string }{
		{filepath.Join(b.root, "go/go.mod"), "module github.com/wignerStan/remote-storage-gateway/go\nrequire github.com/wignerStan/network-data-plane-worker/go v0.1.0-alpha.1\n"},
		{filepath.Join(b.dataPlane, "go/go.mod"), "module github.com/wignerStan/network-data-plane-worker/go\n"},
	} {
		if e := os.MkdirAll(filepath.Dir(item.path), 0755); e != nil {
			t.Fatal(e)
		}
		if e := os.WriteFile(item.path, []byte(item.content), 0644); e != nil {
			t.Fatal(e)
		}
	}
	if _, e := b.dataPlaneModule(); e != nil {
		t.Fatal(e)
	}
	b.dataPlane = b.root
	if _, e := b.dataPlaneModule(); e == nil {
		t.Fatal("embedded Core source accepted")
	}
}
