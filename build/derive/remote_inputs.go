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

	buildinputs "github.com/wignerStan/infra/go/code/buildsource"
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
	// The standalone source owner computes its own fingerprint. Never substitute
	// an old binary receipt after source or build recipe changes.
	native, err := b.nativeFingerprint()
	if err != nil {
		return err
	}
	inputDir := filepath.Join(b.output, "build-inputs")
	if err := os.MkdirAll(inputDir, 0755); err != nil {
		return err
	}
	roots := []buildinputs.Root{
		{Source: filepath.Join(b.root, "go"), Stage: "workspace/gateway/go"},
		{Source: filepath.Join(b.infra, "go"), Stage: "workspace/infra/go"},
		{Source: filepath.Join(b.infra, "c"), Stage: "workspace/infra/c"},
		{Source: filepath.Join(b.infra, "rust/crates/interop"), Stage: "workspace/infra/rust/crates/interop"},
		{Source: filepath.Join(b.infra, "third_party/huma"), Stage: "workspace/infra/third_party/huma", Exclude: []string{".git"}},
		{Source: filepath.Join(b.dataPlane, "go"), Stage: "workspace/worker/go"},
		{Source: filepath.Join(b.materializer, "go"), Stage: "workspace/rclone/go"},
		{Source: b.sourcePath("openlist"), Stage: "workspace/openlist", Exclude: []string{".git", "public/dist", "node_modules"}},
		{Source: b.sourcePath("openlist-ui"), Stage: "workspace/openlist-ui", Exclude: []string{".git", "dist", "node_modules"}},
		{Source: b.rcloneModulePath(), Stage: "workspace/rclone-common"},
		{Source: filepath.Join(b.output, "materializer/source/rclone"), Stage: "workspace/rclone-native"},
	}
	var goRoot string
	for _, tool := range []struct{ name, version string }{{"go", b.tools.Go}, {"node", b.tools.Node}, {"pnpm", b.tools.PNPM}} {
		raw, err := remoteMetadataCommand(b.root, b.mise, nil, "where", tool.name+"@"+tool.version)
		if err != nil {
			return err
		}
		path := strings.TrimSpace(string(raw))
		root := buildinputs.Root{Source: path, Stage: "tools/" + tool.name}
		if tool.name == "pnpm" {
			// Remote UI invokes the pinned JS distribution with the pinned Node
			// executable, rather than uploading three standalone Node ELF aliases.
			root.Exclude = []string{"pnpm", "pnpx", "pnx", "pn"}
		}
		roots = append(roots, root)
		if tool.name == "go" {
			goRoot = path
		}
	}
	goProgram := filepath.Join(goRoot, "bin/go")
	files, err := buildinputs.Prepare(context.Background(), buildinputs.Config{
		Roots: roots, GeneratedDirectory: inputDir, GoBinary: goProgram, GoEnv: map[string]string{"GOOS": "linux", "GOARCH": "amd64"}, AllowModuleDownloads: true,
		Profiles: []buildinputs.Profile{
			{Workspace: filepath.Join(b.output, "go.work"), Stage: "go.work", Targets: []buildinputs.Target{
				{Directory: filepath.Join(b.root, "go"), Package: "./cmd/storage-worker", Tags: []string{"dataplane_l7", "infra_grpc"}, CGOEnabled: true},
				{Directory: filepath.Join(b.root, "go"), Package: "./cmd/plugin-rclone", CGOEnabled: true},
				{Directory: filepath.Join(b.root, "go"), Package: "./cmd/plugin-reapi", CGOEnabled: true},
				{Directory: filepath.Join(b.root, "go"), Package: "./cmd/gateway-admin", CGOEnabled: true},
				{Directory: b.sourcePath("openlist"), Package: ".", Tags: []string{"infra_grpc"}, CGOEnabled: false},
				{Directory: b.sourcePath("openlist"), Package: "./cmd/gateway-bundle", Tags: []string{"dataplane_l7", "infra_grpc"}, CGOEnabled: true},
				{Directory: b.sourcePath("openlist"), Package: "./cmd/openlist-client-plugin", CGOEnabled: true},
			}},
			{Workspace: filepath.Join(b.output, "materializer/go.work"), Stage: "go-native.work", Targets: []buildinputs.Target{
				{Directory: filepath.Join(b.output, "materializer/source/rclone"), Package: ".", CGOEnabled: true},
				{Directory: filepath.Join(b.materializer, "go"), Package: "./build/derive", CGOEnabled: true},
			}},
		},
	})
	if err != nil {
		return err
	}
	digest, err := b.provenanceDigest()
	if err != nil {
		return err
	}
	commitRaw, err := remoteMetadataCommand(b.root, "git", map[string]string{"GIT_OPTIONAL_LOCKS": "0"}, "rev-parse", "HEAD")
	if err != nil {
		return err
	}
	uiVersion, err := readUIVersion(filepath.Join(b.sourcePath("openlist-ui"), "package.json"))
	if err != nil {
		return err
	}
	config := map[string]string{"gateway_digest": "sha256:" + digest, "rclone_digest": native, "product_commit": strings.TrimSpace(string(commitRaw)), "ui_version": uiVersion, "rclone_version": version}
	buildEnv, err := remoteBuildEnv(config)
	if err != nil {
		return err
	}
	file := filepath.Join(inputDir, "build.env")
	if err = os.WriteFile(file, buildEnv, 0644); err != nil {
		return err
	}
	digest, err = hashFile(file)
	if err != nil {
		return err
	}
	files = append(files, buildinputs.File{Path: "build.env", Source: file, SHA256: digest, Mode: "0644"})
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
	raw, err := remoteMetadataCommand(b.root, b.mise, nil, "where", "go@"+b.tools.Go)
	if err != nil {
		return "", err
	}
	program := filepath.Join(strings.TrimSpace(string(raw)), "bin/go")
	command := []string{"run", "./build/derive", "-root", b.materializer, "-out", filepath.Join(b.output, "materializer"), "-infra-root", b.infra, "-data-plane-root", b.dataPlane, "-mise", b.mise}
	raw, err = remoteMetadataCommand(filepath.Join(b.materializer, "go"), program, map[string]string{"GOWORK": filepath.Join(b.output, "materializer/go.work"), "GOPROXY": "off", "GOSUMDB": "off"}, append(command, args...)...)
	return strings.TrimSpace(string(raw)), err
}

func (b *builder) nativeFingerprint() (string, error) { return b.nativeTool("fingerprint") }
