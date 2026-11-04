package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
	"unicode"

	buildinputs "github.com/wignerStan/infra/go/code/build/source"
	processexec "github.com/wignerStan/infra/go/process/exec"
)

// remoteInputManifest adds product-owned identities to the SDK source capture.
type remoteInputManifest struct {
	Schema string             `json:"schema"`
	Files  []buildinputs.File `json:"files"`
	Config map[string]string  `json:"config"`
}

func remoteMetadataCommand(dir, program string, env map[string]string, args ...string) ([]byte, error) {
	spec := processexec.NewSpec(program, args...).WithTimeout(10 * time.Minute).WithEnv(buildEnvironment(env))
	spec.Dir, spec.MaxStdout, spec.MaxStderr = dir, 64<<20, 2<<20
	result, err := processexec.RunContext(context.Background(), spec)
	if err != nil {
		return nil, fmt.Errorf("offline input metadata %s: %w\n%s", program, err, result.Stderr)
	}
	if result.StdoutTruncated || result.StderrTruncated {
		return nil, errors.New("offline input metadata exceeded output bound")
	}
	return result.Stdout, nil
}

func remoteBuildEnv(values map[string]string) ([]byte, error) {
	keys := []string{"gateway_digest", "rclone_digest", "product_commit", "ui_version", "rclone_version"}
	var output strings.Builder
	for _, key := range keys {
		value := values[key]
		if value == "" || strings.ContainsAny(value, " \t\r\n\x00'\"`$\\") || strings.ContainsFunc(value, unicode.IsControl) {
			return nil, fmt.Errorf("invalid fixed build value %s", key)
		}
		fmt.Fprintf(&output, "%s=%s\n", key, value)
	}
	return []byte(output.String()), nil
}

func (b *builder) remoteInputs() error {
	admitted, err := b.sources()
	if err != nil {
		return err
	}
	for _, source := range admitted {
		if err = b.verify(source); err != nil {
			return err
		}
		if err = b.verifyDerived(source); err != nil {
			return err
		}
	}
	version, err := b.rcloneVersion()
	if err != nil {
		return err
	}
	series, err := b.rcloneSeries()
	if err != nil {
		return err
	}
	if _, err = b.verifyModuleSnapshot(version, series); err != nil {
		return err
	}
	program, err := b.selectedGo()
	if err != nil {
		return err
	}
	goRoot, err := b.goCommand(b.root, nil, "env", "GOROOT")
	if err != nil {
		return err
	}
	link, err := b.linkInputs()
	if err != nil {
		return err
	}
	inputDir := filepath.Join(b.output, "build-inputs")
	roots := []buildinputs.Root{
		{Source: filepath.Join(b.root, "go"), Stage: "workspace/gateway/go"},
		{Source: filepath.Join(b.infra, "go"), Stage: "workspace/infra/go"},
		{Source: filepath.Join(b.infra, "c"), Stage: "workspace/infra/c"},
		{Source: filepath.Join(b.dataPlane, "go"), Stage: "workspace/worker/go"},
		{Source: filepath.Join(b.materializer, "go"), Stage: "workspace/rclone/go"},
		{Source: filepath.Join(b.buildbarn, "go"), Stage: "workspace/buildbarn/go"},
		{Source: b.sourcePath("openlist"), Stage: "workspace/openlist", Exclude: []string{".git", "node_modules"}},
		{Source: b.sourcePath("openlist-ui"), Stage: "workspace/openlist-ui", Exclude: []string{".git", "dist", "node_modules"}},
		{Source: b.rcloneModulePath(), Stage: "workspace/rclone-common"},
		{Source: goRoot, Stage: "tools/go"},
		{Source: link.SDK, Stage: "tools/infra-native-sdk"},
	}
	profiles := []buildinputs.Profile{{Workspace: filepath.Join(b.output, "go.work"), Stage: "go.work", Targets: []buildinputs.Target{
		{Directory: filepath.Join(b.root, "go"), Package: "./cmd/storage-worker", Tags: []string{"dataplane_l7", "infra_grpc"}, CGOEnabled: true},
		{Directory: filepath.Join(b.root, "go"), Package: "./cmd/plugin-rclone", CGOEnabled: true},
		{Directory: filepath.Join(b.root, "go"), Package: "./cmd/plugin-reapi", CGOEnabled: true},
		{Directory: filepath.Join(b.root, "go"), Package: "./cmd/gateway-admin", CGOEnabled: true},
		{Directory: b.sourcePath("openlist"), Package: ".", Tags: []string{"infra_grpc"}},
		{Directory: b.sourcePath("openlist"), Package: "./cmd/gateway-bundle", Tags: []string{"dataplane_l7", "infra_grpc"}, CGOEnabled: true},
		{Directory: b.sourcePath("openlist"), Package: "./cmd/openlist-client-plugin", CGOEnabled: true},
	}}}
	files, err := buildinputs.Prepare(context.Background(), buildinputs.Config{Roots: roots, GeneratedDirectory: inputDir, GoBinary: program, GoEnv: b.nativeEnvironment(), AllowModuleDownloads: true, Profiles: profiles})
	if err != nil {
		return err
	}
	for name, path := range link.Tools {
		digest, err := hashFile(path)
		if err != nil {
			return err
		}
		files = append(files, buildinputs.File{Path: "tools/native/" + name, Source: path, SHA256: digest, Mode: "0755"})
	}
	config, err := b.buildConfig()
	if err != nil {
		return err
	}
	caHash, err := hashFile(b.ca)
	if err != nil {
		return err
	}
	files = append(files, buildinputs.File{Path: "runtime/ca-certificates.crt", Source: b.ca, SHA256: caHash, Mode: "0644"})
	sort.Slice(files, func(i, j int) bool { return files[i].Path < files[j].Path })
	return writeJSON(filepath.Join(inputDir, "manifest.json"), remoteInputManifest{"storage-gateway/build-inputs/v1", files, config})
}

// Upstream package metadata has its own vocabulary. Read the version projection
// without imposing the closed schema used for product-owned configuration.
func readUIVersion(path string) (string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	var pkg struct {
		Version string `json:"version"`
	}
	if err = json.Unmarshal(raw, &pkg); err != nil {
		return "", err
	}
	if strings.TrimSpace(pkg.Version) == "" {
		return "", fmt.Errorf("UI package metadata has no version")
	}
	return pkg.Version, nil
}

func (b *builder) nativeTool(args ...string) (string, error) {
	program, err := b.selectedGo()
	if err != nil {
		return "", err
	}
	command := []string{"run", "./build/derive", "-root", b.materializer, "-out", filepath.Join(b.output, "materializer"), "-infra-root", b.infra, "-data-plane-root", b.dataPlane, "-go", program, "-jobs", fmt.Sprint(b.parallelism()), "-native-sdk-root", b.nativeSDKRoot, "-pkg-config", b.pkgConfig}
	env := b.nativeEnvironment()
	env["GOWORK"] = filepath.Join(b.output, "bootstrap/go.work")
	raw, err := remoteMetadataCommand(filepath.Join(b.materializer, "go"), program, env, append(command, args...)...)
	return strings.TrimSpace(string(raw)), err
}
func (b *builder) nativeFingerprint() (string, error) { return b.nativeTool("fingerprint") }
