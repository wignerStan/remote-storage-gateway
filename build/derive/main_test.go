package main

import (
	"encoding/json"
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
	for _, name := range []string{"remote-storage-gateway", "client-openlist.so", "client-rclone.so", "openlist-control-plane"} {
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
		want := 4
		if composition == "control-plane" {
			want = 2
		}
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

func TestNativeBuildIdentityTracksBackendPatchesAndRecipe(t *testing.T) {
	b, _ := fixture(t)
	b.infra = filepath.Join(b.root, "third_party/fixture")
	inputs := map[string]string{
		"go/client.go":                          "package client\n",
		".git-source-relationships/rclone.json": "pinned upstream and result tree\n",
		"patches/rclone/0001.patch":             "original backend patch\n",
		"build/toolchain.json":                  "pinned tools\n",
		"Makefile":                              "native build flags\n",
	}
	for name, content := range inputs {
		path := filepath.Join(b.root, name)
		if e := os.MkdirAll(filepath.Dir(path), 0755); e != nil {
			t.Fatal(e)
		}
		if e := os.WriteFile(path, []byte(content), 0644); e != nil {
			t.Fatal(e)
		}
	}
	baseline, e := b.provenanceDigest()
	if e != nil {
		t.Fatal(e)
	}
	again, e := b.provenanceDigest()
	if e != nil || again != baseline {
		t.Fatalf("unstable build identity: %q %v", again, e)
	}
	for name, original := range inputs {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(b.root, name)
			if e := os.WriteFile(path, []byte(original+"changed\n"), 0644); e != nil {
				t.Fatal(e)
			}
			changed, e := b.provenanceDigest()
			if e != nil || changed == baseline {
				t.Fatalf("changed build input kept identity: %q %v", changed, e)
			}
			if e := os.WriteFile(path, []byte(original), 0644); e != nil {
				t.Fatal(e)
			}
		})
	}
}
