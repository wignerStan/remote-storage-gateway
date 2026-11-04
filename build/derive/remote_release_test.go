package main

import (
	"archive/tar"
	"os"
	"path/filepath"
	"testing"

	buildinputs "github.com/wignerStan/infra/go/code/buildsource"
)

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
	for _, kind := range []string{"valid", "traversal", "link", "duplicate", "missing-index", "oversized"} {
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
			if kind == "valid" {
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
