package main

import (
	"archive/tar"
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"strings"

	buildinputs "github.com/wignerStan/infra/go/code/build/source"
)

func validateRemoteSnapshot(manifest remoteInputManifest, expected map[string]string) error {
	if manifest.Schema != "storage-gateway/build-inputs/v1" || !reflect.DeepEqual(manifest.Config, expected) || len(manifest.Files) == 0 {
		return fmt.Errorf("prepared native inputs differ from current source; run prepare")
	}
	return buildinputs.Verify(context.Background(), manifest.Files)
}

func (b *builder) admitRemoteSnapshot() (remoteInputManifest, error) {
	var manifest remoteInputManifest
	if err := readJSONLimit(filepath.Join(b.output, "build-inputs/manifest.json"), &manifest, 128<<20); err != nil {
		return manifest, err
	}
	expected, err := b.buildConfig()
	if err != nil {
		return manifest, err
	}
	return manifest, validateRemoteSnapshot(manifest, expected)
}

// extractRemoteUI admits only a bounded dist tree into a new, empty directory.
// A failed archive never replaces the embedded UI from the previous build.
func extractRemoteUI(archive, destination string) error {
	file, err := os.Open(archive)
	if err != nil {
		return err
	}
	defer file.Close()
	if err = os.MkdirAll(filepath.Dir(destination), 0755); err != nil {
		return err
	}
	temporary, err := os.MkdirTemp(filepath.Dir(destination), ".remote-ui-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(temporary)
	reader := tar.NewReader(file)
	seen := map[string]bool{}
	var total int64
	for count := 0; ; count++ {
		header, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		name := strings.TrimSuffix(strings.TrimPrefix(header.Name, "./"), "/")
		if (name == "." || name == "") && header.Typeflag == tar.TypeDir {
			continue
		}
		if count >= 20000 || name == "" || path.IsAbs(name) || path.Clean(name) != name || name == ".." || strings.HasPrefix(name, "../") || strings.ContainsAny(name, "\\\x00\r\n") || seen[name] {
			return fmt.Errorf("invalid or duplicate UI archive member %q", header.Name)
		}
		seen[name] = true
		target := filepath.Join(temporary, filepath.FromSlash(name))
		switch header.Typeflag {
		case tar.TypeDir:
			if err = os.MkdirAll(target, 0755); err != nil {
				return err
			}
		case tar.TypeReg:
			if header.Size < 0 || header.Size > 128<<20 || total > (512<<20)-header.Size {
				return fmt.Errorf("UI archive size exceeds bound")
			}
			total += header.Size
			if err = os.MkdirAll(filepath.Dir(target), 0755); err != nil {
				return err
			}
			output, err := os.OpenFile(target, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
			if err != nil {
				return err
			}
			_, copyErr := io.CopyN(output, reader, header.Size)
			closeErr := output.Close()
			if copyErr != nil {
				return copyErr
			}
			if closeErr != nil {
				return closeErr
			}
		default:
			return fmt.Errorf("UI archive has unsupported member %q", name)
		}
	}
	if info, err := os.Stat(filepath.Join(temporary, "index.html")); err != nil || !info.Mode().IsRegular() {
		return fmt.Errorf("UI archive has no root index.html")
	}
	if err = os.RemoveAll(destination); err != nil {
		return err
	}
	return os.Rename(temporary, destination)
}

func (b *builder) collectRemoteUI(archive string) error {
	destination := filepath.Join(b.sourcePath("openlist"), "public/dist")
	if err := extractRemoteUI(archive, destination); err != nil {
		return err
	}
	distHash, err := treeDigest(destination)
	if err != nil {
		return err
	}
	archiveHash, err := hashFile(archive)
	if err != nil {
		return err
	}
	lockHash, err := hashFile(filepath.Join(b.sourcePath("openlist-ui"), "pnpm-lock.yaml"))
	if err != nil {
		return err
	}
	version, err := readUIVersion(filepath.Join(b.sourcePath("openlist-ui"), "package.json"))
	if err != nil {
		return err
	}
	source, err := b.load("openlist-ui")
	if err != nil {
		return err
	}
	return writeJSON(filepath.Join(b.output, "receipts/ui.json"), map[string]any{
		"dist_sha256": distHash, "archive_sha256": archiveHash,
		"embedded_input": destination, "pnpm_lock_sha256": lockHash,
		"toolchain": b.tools, "upstream": source.Upstream, "version": version,
		"execution": "local-node-pnpm", "cache": "local pnpm content store",
	})
}
