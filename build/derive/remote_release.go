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

	buildinputs "github.com/wignerStan/infra/go/code/buildsource"
)

func (b *builder) bazel(args ...string) (string, error) {
	command := []string{"GATEWAY_BUILD_INPUTS=" + filepath.Join(b.output, "build-inputs/manifest.json"), "RCLONE_BUILD_INPUTS=" + filepath.Join(b.output, "materializer/build-inputs/manifest.json"), b.mise, "exec", "bazel@9.3.1", "--", "bazel"}
	return b.command(b.root, "env", append(command, args...)...)
}

func (b *builder) remoteBuild(targets ...string) error {
	args := []string{"build", "--config=buildbarn", "--lockfile_mode=off"}
	if b.materializer != filepath.Join(filepath.Dir(b.root), "rclone") {
		args = append(args, "--override_module=rclone_data_plane="+b.materializer)
	}
	_, err := b.bazel(append(args, targets...)...)
	return err
}

func validateRemoteSnapshot(manifest remoteInputManifest, expected map[string]string) error {
	if manifest.Schema != "storage-gateway/build-inputs/v1" || !reflect.DeepEqual(manifest.Config, expected) || len(manifest.Files) == 0 {
		return fmt.Errorf("prepared remote inputs differ from current source; run prepare")
	}
	return buildinputs.Verify(context.Background(), manifest.Files)
}

func (b *builder) admitRemoteSnapshot() (remoteInputManifest, error) {
	var manifest remoteInputManifest
	if err := readJSONLimit(filepath.Join(b.output, "build-inputs/manifest.json"), &manifest, 128<<20); err != nil {
		return manifest, err
	}
	digest, err := b.provenanceDigest()
	if err != nil {
		return manifest, err
	}
	commit, err := b.git(b.root, "rev-parse", "HEAD")
	if err != nil {
		return manifest, err
	}
	native, err := b.nativeFingerprint()
	if err != nil {
		return manifest, err
	}
	version, err := b.rcloneVersion()
	if err != nil {
		return manifest, err
	}
	uiVersion, err := readUIVersion(filepath.Join(b.sourcePath("openlist-ui"), "package.json"))
	if err != nil {
		return manifest, err
	}
	expected := map[string]string{"gateway_digest": "sha256:" + digest, "rclone_digest": native, "product_commit": commit, "ui_version": uiVersion, "rclone_version": version}
	return manifest, validateRemoteSnapshot(manifest, expected)
}

func (b *builder) collectRemote() error {
	manifest, err := b.admitRemoteSnapshot()
	if err != nil {
		return err
	}
	// Owning the build prevents stale outputs being relabelled after a failed or
	// differently configured external invocation. UI is explicitly top-level.
	if err = b.remoteBuild("//:native_release", "//:ui_dist", "@rclone_data_plane//:native_rclone/rclone.json"); err != nil {
		return err
	}
	if _, err = b.admitRemoteSnapshot(); err != nil {
		return err
	}
	bin, err := b.bazel("info", "--lockfile_mode=off", "bazel-bin")
	if err != nil {
		return err
	}
	bin = strings.TrimSpace(bin)
	if err = b.collectRemoteUI(filepath.Join(bin, "ui_dist", "ui-dist.tar")); err != nil {
		return err
	}
	product := manifest.Config["product_commit"]
	infra, err := b.git(b.infra, "rev-parse", "HEAD")
	if err != nil {
		return err
	}
	if err = os.MkdirAll(filepath.Join(b.output, "bin"), 0755); err != nil {
		return err
	}
	for _, name := range []string{"openlist-control-plane", "storage-worker", "client-openlist.so", "client-rclone.so", "transfer-reapi.so", "gateway-admin", "rclone"} {
		source := filepath.Join(bin, "native_release", name)
		target := filepath.Join(b.output, "bin", name)
		if err = copyInput(source, target); err != nil {
			return fmt.Errorf("remote output %s: %w", name, err)
		}
		mode := os.FileMode(0755)
		if filepath.Ext(name) == ".so" {
			mode = 0644
		}
		if err = os.Chmod(target, mode); err != nil {
			return err
		}
		hash, err := hashFile(target)
		if err != nil {
			return err
		}
		digest := manifest.Config["gateway_digest"]
		if name == "rclone" {
			digest = manifest.Config["rclone_digest"]
		}
		if err = writeJSON(filepath.Join(b.output, "receipts", name+".json"), map[string]any{"artifact": name, "sha256": hash, "product_commit": product, "infra_commit": infra, "build_digest": digest, "execution": "Buildbarn", "cache": "shared REAPI AC/CAS"}); err != nil {
			return err
		}
	}
	_, err = b.nativeTool("-binary", filepath.Join(b.output, "bin/rclone"), "-binary-receipt", filepath.Join(bin, "external/rclone_data_plane+/native_rclone/rclone.json"), "collect-artifact")
	return err
}

func (b *builder) remoteImages() error {
	if _, err := b.admitRemoteSnapshot(); err != nil {
		return err
	}
	if err := b.remoteBuild("--output_groups=+oci_tarball,+digest", "//:gateway_image", "//:control_plane_image", "@rclone_data_plane//:rclone_image"); err != nil {
		return err
	}
	_, err := b.admitRemoteSnapshot()
	return err
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
		"execution": "Buildbarn", "cache": "shared REAPI AC/CAS",
	})
}
