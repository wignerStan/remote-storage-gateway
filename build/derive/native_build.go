package main

import (
	"context"
	"crypto/sha256"
	"debug/buildinfo"
	"debug/elf"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	processexec "github.com/wignerStan/infra/go/process/exec"
	"golang.org/x/mod/modfile"
)

// validateOutput accepts caller-owned build directories, but never a source
// checkout or the shared module cache. Source derivation must remain disposable.
func validateOutput(output string, sources ...string) error {
	if !filepath.IsAbs(output) || filepath.Clean(output) == string(filepath.Separator) {
		return errors.New("build output must be an absolute external directory")
	}
	canonical := func(path string) (string, error) {
		path, err := filepath.Abs(path)
		if err != nil {
			return "", err
		}
		var suffix []string
		for {
			resolved, err := filepath.EvalSymlinks(path)
			if err == nil {
				for i := len(suffix) - 1; i >= 0; i-- {
					resolved = filepath.Join(resolved, suffix[i])
				}
				return resolved, nil
			}
			if !os.IsNotExist(err) {
				return "", err
			}
			parent := filepath.Dir(path)
			if parent == path {
				return "", err
			}
			suffix = append(suffix, filepath.Base(path))
			path = parent
		}
	}
	out, err := canonical(output)
	if err != nil {
		return err
	}
	if cache := os.Getenv("GOMODCACHE"); cache != "" {
		sources = append(sources, cache)
	}
	for _, source := range sources {
		root, err := canonical(source)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, out)
		if err != nil {
			return err
		}
		inside := rel == "." || rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
		parent, err := filepath.Rel(out, root)
		if err != nil {
			return err
		}
		contains := parent == "." || parent != ".." && !strings.HasPrefix(parent, ".."+string(filepath.Separator))
		if inside || contains {
			return fmt.Errorf("build output %s must be disjoint from source/cache %s", output, source)
		}
	}
	return nil
}

func (b *builder) parallelism() int {
	if b.jobs < 1 {
		return 2
	}
	return b.jobs
}
func (b *builder) execute(dir, program string, env map[string]string, args ...string) (string, error) {
	values := map[string]string{"GOWORK": "off", "GOTOOLCHAIN": "local", "GIT_OPTIONAL_LOCKS": "0", "HUSKY": "0", "CI": "true"}
	for key, value := range env {
		values[key] = value
	}
	spec := processexec.NewSpec(program, args...).WithTimeout(30 * time.Minute).WithEnv(buildEnvironment(values))
	spec.Dir, spec.MaxStdout, spec.MaxStderr = dir, 8<<20, 8<<20
	result, err := processexec.RunContext(context.Background(), spec)
	if err != nil {
		return "", fmt.Errorf("%s failed in %s: %w\n%s%s", program, dir, err, result.Stdout, result.Stderr)
	}
	if result.StdoutTruncated || result.StderrTruncated {
		return "", errors.New("native tool output exceeded bound")
	}
	return strings.TrimSpace(string(result.Stdout)), nil
}
func (b *builder) goCommand(dir string, env map[string]string, args ...string) (string, error) {
	if b.mise != "" {
		return b.execute(dir, b.mise, env, append([]string{"exec", "go@" + b.tools.Go, "--", "go"}, args...)...)
	}
	program := b.goBinary
	if program == "" {
		program = "go"
	}
	return b.execute(dir, program, env, args...)
}
func (b *builder) selectedTool(name, configured, version string) (string, error) {
	program := configured
	if program == "" {
		program = name
	}
	if b.mise != "" {
		root, err := b.command(b.root, b.mise, "where", name+"@"+version)
		if err != nil {
			return "", err
		}
		program = filepath.Join(root, "bin", name)
		if _, err = os.Stat(program); os.IsNotExist(err) {
			program = filepath.Join(root, name)
		}
	}
	program, err := exec.LookPath(program)
	if err != nil {
		return "", fmt.Errorf("native %s %s is required: %w", name, version, err)
	}
	return filepath.Abs(program)
}
func (b *builder) selectedGo() (string, error) {
	program, err := b.selectedTool("go", b.goBinary, b.tools.Go)
	if err != nil {
		return "", err
	}
	actual, err := b.execute(b.root, program, nil, "env", "GOVERSION")
	if err != nil {
		return "", err
	}
	if actual != "go"+b.tools.Go {
		return "", fmt.Errorf("compiler %s reports %s; require go%s", program, actual, b.tools.Go)
	}
	return program, nil
}
func (b *builder) nativeEnvironment() map[string]string {
	env := map[string]string{
		"GOWORK": filepath.Join(b.output, "go.work"), "GOTOOLCHAIN": "local", "GOOS": "linux", "GOARCH": "amd64", "CGO_ENABLED": "1",
		"CGO_CFLAGS": "-I" + filepath.Join(b.infra, "c/interop/plugins/include"),
		"GOMAXPROCS": fmt.Sprint(b.parallelism()), "GOMEMLIMIT": "768MiB",
		// Derived source identity is recorded by the verified manifest and owner receipt.
		"GOFLAGS": "-buildvcs=false",
		// Ambient compiler search paths are outside this source/SDK recipe.
		"CPATH": "", "C_INCLUDE_PATH": "", "CPLUS_INCLUDE_PATH": "", "OBJC_INCLUDE_PATH": "",
		"LIBRARY_PATH": "", "COMPILER_PATH": "", "GCC_EXEC_PREFIX": "",
	}
	for _, key := range []string{"GOCACHE", "GOMODCACHE", "GOPROXY", "GOSUMDB", "CC", "CXX", "GOMEMLIMIT", "PKG_CONFIG", "PKG_CONFIG_PATH", "PKG_CONFIG_LIBDIR", "CGO_CPPFLAGS", "CGO_CXXFLAGS", "CGO_LDFLAGS"} {
		if value := os.Getenv(key); value != "" {
			env[key] = value
		}
	}
	if value := os.Getenv("CGO_CFLAGS"); value != "" {
		env["CGO_CFLAGS"] += " " + value
	}
	if b.pkgConfig != "" {
		env["PKG_CONFIG"] = b.pkgConfig
	}
	if b.nativeSDKRoot != "" {
		env["PKG_CONFIG_PATH"] = filepath.Join(b.nativeSDKRoot, "lib/pkgconfig") + string(os.PathListSeparator) + os.Getenv("PKG_CONFIG_PATH")
	}
	return env
}
func officialHuma(root string) (string, error) {
	path := filepath.Join(root, "go/go.mod")
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	manifest, err := modfile.Parse(path, raw, nil)
	if err != nil {
		return "", err
	}
	const module = "github.com/danielgtaylor/huma/v2"
	for _, replacement := range manifest.Replace {
		if replacement.Old.Path == module {
			return "", errors.New("Huma must remain the official versioned module without a local replacement")
		}
	}
	for _, requirement := range manifest.Require {
		if requirement.Mod.Path == module && strings.HasPrefix(requirement.Mod.Version, "v2.") {
			return requirement.Mod.Version, nil
		}
	}
	return "", errors.New("selected Infra must require the official Huma v2 module")
}
func replaceDirectory(source, target string) error {
	if err := os.MkdirAll(filepath.Dir(target), 0755); err != nil {
		return err
	}
	backup := target + ".previous"
	if _, err := os.Lstat(backup); err == nil {
		return fmt.Errorf("unreconciled prior output %s", backup)
	} else if !os.IsNotExist(err) {
		return err
	}
	hadPrevious := false
	if _, err := os.Lstat(target); err == nil {
		if err = os.Rename(target, backup); err != nil {
			return err
		}
		hadPrevious = true
	} else if !os.IsNotExist(err) {
		return err
	}
	if err := os.Rename(source, target); err != nil {
		if hadPrevious {
			if restore := os.Rename(backup, target); restore != nil {
				return fmt.Errorf("publish %v; restore %v", err, restore)
			}
		}
		return err
	}
	if hadPrevious {
		return os.RemoveAll(backup)
	}
	return nil
}
func (b *builder) buildUI() error {
	relationship, err := b.load("openlist-ui")
	if err != nil {
		return err
	}
	if err = b.verify(relationship); err != nil {
		return err
	}
	if err = b.verifyDerived(relationship); err != nil {
		return err
	}
	node, err := b.selectedTool("node", b.nodeBinary, b.tools.Node)
	if err != nil {
		return err
	}
	pnpm, err := b.selectedTool("pnpm", b.pnpmBinary, b.tools.PNPM)
	if err != nil {
		return err
	}
	directory := b.sourcePath("openlist-ui")
	env := map[string]string{"PATH": filepath.Dir(node) + string(os.PathListSeparator) + os.Getenv("PATH"), "NODE_OPTIONS": "--max-old-space-size=1536", "npm_config_manage_package_manager_versions": "false", "CI": "true", "HUSKY": "0"}
	version, err := b.execute(directory, node, env, "--version")
	if err != nil {
		return err
	}
	if version != "v"+b.tools.Node {
		return fmt.Errorf("Node reports %s; require v%s", version, b.tools.Node)
	}
	version, err = b.execute(directory, pnpm, env, "--version")
	if err != nil {
		return err
	}
	if version != b.tools.PNPM {
		return fmt.Errorf("pnpm reports %s; require %s", version, b.tools.PNPM)
	}
	raw, err := os.ReadFile(filepath.Join(directory, "package.json"))
	if err != nil {
		return err
	}
	var pkg struct {
		PackageManager string `json:"packageManager"`
	}
	if err = json.Unmarshal(raw, &pkg); err != nil {
		return err
	}
	if pkg.PackageManager != "pnpm@"+b.tools.PNPM {
		return fmt.Errorf("upstream package manager %q differs from pinned pnpm", pkg.PackageManager)
	}
	lock := filepath.Join(directory, "pnpm-lock.yaml")
	lockHash, err := hashFile(lock)
	if err != nil {
		return err
	}
	// Install the actual frozen dependency graph into the derived UI checkout.
	// Never synthesize node_modules or replace the upstream build command.
	env["NODE_ENV"] = "development"
	text, err := b.execute(directory, pnpm, env, "install", "--frozen-lockfile", "--ignore-scripts", "--child-concurrency=2", "--network-concurrency=4")
	if text != "" {
		fmt.Println(text)
	}
	if err != nil {
		return err
	}
	env["NODE_ENV"], env["npm_config_offline"] = "production", "true"
	text, err = b.execute(directory, pnpm, env, "run", "build")
	if text != "" {
		fmt.Println(text)
	}
	if err != nil {
		return err
	}
	actualLock, err := hashFile(lock)
	if err != nil {
		return err
	}
	if actualLock != lockHash {
		return errors.New("UI build changed the admitted frozen lockfile")
	}
	if err = b.verifyDerived(relationship); err != nil {
		return err
	}
	dist := filepath.Join(directory, "dist")
	if info, err := os.Lstat(filepath.Join(dist, "index.html")); err != nil || !info.Mode().IsRegular() {
		return errors.New("UI build produced no regular dist/index.html")
	}
	public := filepath.Join(b.sourcePath("openlist"), "public")
	if err = os.MkdirAll(public, 0755); err != nil {
		return err
	}
	stage, err := os.MkdirTemp(public, ".native-ui-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(stage)
	if err = copyTree(dist, stage); err != nil {
		return err
	}
	// The upstream embeds all:dist, including its tracked README. Preserve that
	// admitted source byte-for-byte alongside generated UI files before capture.
	readme, err := b.git(b.sourcePath("openlist"), "show", ":public/dist/README.md")
	if err != nil {
		return err
	}
	if err = os.WriteFile(filepath.Join(stage, "README.md"), []byte(readme), 0644); err != nil {
		return err
	}
	distHash, err := treeDigest(stage)
	if err != nil {
		return err
	}
	destination := filepath.Join(public, "dist")
	if err = replaceDirectory(stage, destination); err != nil {
		return err
	}
	uiVersion, err := readUIVersion(filepath.Join(directory, "package.json"))
	if err != nil {
		return err
	}
	return writeJSON(filepath.Join(b.output, "receipts/ui.json"), map[string]any{"schema": "storage-gateway/native-ui/v1", "dist_sha256": distHash, "embedded_input": destination, "pnpm_lock_sha256": lockHash, "toolchain": b.tools, "upstream": relationship.Upstream, "version": uiVersion, "execution": "local-node-pnpm", "cache": "local pnpm content store"})
}
func (b *builder) buildConfig() (map[string]string, error) {
	digest, err := b.provenanceDigest()
	if err != nil {
		return nil, err
	}
	commit, err := b.git(b.root, "rev-parse", "HEAD")
	if err != nil {
		return nil, err
	}
	status, err := b.git(b.root, "status", "--porcelain", "--untracked-files=all")
	if err != nil {
		return nil, err
	}
	if status != "" {
		commit += "-dirty"
	}
	version, err := b.rcloneVersion()
	if err != nil {
		return nil, err
	}
	uiVersion, err := readUIVersion(filepath.Join(b.sourcePath("openlist-ui"), "package.json"))
	if err != nil {
		return nil, err
	}
	var ui struct {
		Schema        string    `json:"schema"`
		DistSHA256    string    `json:"dist_sha256"`
		EmbeddedInput string    `json:"embedded_input"`
		LockSHA256    string    `json:"pnpm_lock_sha256"`
		Toolchain     toolchain `json:"toolchain"`
		Upstream      upstream  `json:"upstream"`
		Version       string    `json:"version"`
		Execution     string    `json:"execution"`
		Cache         string    `json:"cache"`
	}
	if err = readJSON(filepath.Join(b.output, "receipts/ui.json"), &ui); err != nil {
		return nil, fmt.Errorf("build the current UI before input admission: %w", err)
	}
	dist := filepath.Join(b.sourcePath("openlist"), "public/dist")
	actual, err := treeDigest(dist)
	if err != nil {
		return nil, err
	}
	lockHash, err := hashFile(filepath.Join(b.sourcePath("openlist-ui"), "pnpm-lock.yaml"))
	if err != nil {
		return nil, err
	}
	source, err := b.load("openlist-ui")
	if err != nil {
		return nil, err
	}
	if ui.Schema != "storage-gateway/native-ui/v1" || ui.DistSHA256 != actual || ui.EmbeddedInput != dist || ui.LockSHA256 != lockHash || ui.Version != uiVersion || ui.Toolchain != b.tools || ui.Upstream != source.Upstream || ui.Execution != "local-node-pnpm" {
		return nil, errors.New("embedded UI differs from admitted native build receipt")
	}
	config := map[string]string{"product_commit": commit, "ui_version": uiVersion, "rclone_version": version, "ui_dist_digest": actual, "ui_lock_digest": lockHash}
	if b.withRclone {
		native, err := b.nativeFingerprint()
		if err != nil {
			return nil, err
		}
		config["rclone_digest"] = native
	}
	link, err := b.linkInputs()
	if err != nil {
		return nil, err
	}
	for key, value := range link.Config {
		config[key] = value
	}
	boundInput, err := json.Marshal(struct {
		Source     string            `json:"source"`
		UI         string            `json:"ui"`
		Lock       string            `json:"lock"`
		NativeLink map[string]string `json:"native_link"`
	}{digest, actual, lockHash, link.Config})
	if err != nil {
		return nil, err
	}
	bound := sha256.Sum256(boundInput)
	config["gateway_digest"] = "sha256:" + hex.EncodeToString(bound[:])
	return config, nil
}
func (b *builder) prepareNative() error {
	if err := b.buildUI(); err != nil {
		return err
	}
	if b.withRclone {
		if _, err := b.nativeTool("source"); err != nil {
			return err
		}
	}
	return b.remoteInputs()
}
func (b *builder) buildNative() error {
	program, err := b.selectedGo()
	if err != nil {
		return err
	}
	if _, err = b.admitRemoteSnapshot(); err != nil {
		if err = b.prepareNative(); err != nil {
			return err
		}
	}
	manifest, err := b.admitRemoteSnapshot()
	if err != nil {
		return err
	}
	stage, err := os.MkdirTemp(b.output, "native-bin-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(stage)
	uiFlags := "-X github.com/OpenListTeam/OpenList/v4/internal/conf.WebVersion=" + manifest.Config["ui_version"] + " -X github.com/OpenListTeam/OpenList/v4/internal/conf.GitCommit=" + manifest.Config["product_commit"]
	pluginFlags := "-X main.buildDigest=" + manifest.Config["gateway_digest"]
	type artifact struct {
		name, directory, target, tags, mode, flags, module string
		cgo                                                bool
	}
	specifications := []artifact{
		{"storage-worker", filepath.Join(b.root, "go"), "./cmd/storage-worker", "dataplane_l7,infra_grpc", "", "", "github.com/wignerStan/remote-storage-gateway/go", true},
		{"transfer-reapi.so", filepath.Join(b.root, "go"), "./cmd/plugin-reapi", "", "c-shared", pluginFlags, "github.com/wignerStan/remote-storage-gateway/go", true},
		{"client-rclone.so", filepath.Join(b.root, "go"), "./cmd/plugin-rclone", "", "c-shared", pluginFlags, "github.com/wignerStan/remote-storage-gateway/go", true},
		{"gateway-admin", filepath.Join(b.root, "go"), "./cmd/gateway-admin", "", "", "", "github.com/wignerStan/remote-storage-gateway/go", true},
		{"openlist-control-plane", b.sourcePath("openlist"), ".", "infra_grpc", "", uiFlags, "github.com/OpenListTeam/OpenList/v4", false},
		{"client-openlist.so", b.sourcePath("openlist"), "./cmd/openlist-client-plugin", "", "c-shared", pluginFlags, "github.com/OpenListTeam/OpenList/v4", true},
		{"gateway", b.sourcePath("openlist"), "./cmd/gateway-bundle", "dataplane_l7,infra_grpc", "", uiFlags, "github.com/OpenListTeam/OpenList/v4", true},
	}
	inputHash, err := hashFile(filepath.Join(b.output, "build-inputs/manifest.json"))
	if err != nil {
		return err
	}
	receipts := make(map[string]any)
	for _, specification := range specifications {
		env := b.nativeEnvironment()
		env["GOPROXY"], env["GOSUMDB"], env["GONOPROXY"] = "off", "off", "none"
		if !specification.cgo {
			env["CGO_ENABLED"] = "0"
		}
		args := []string{"build", "-mod=readonly", "-p=" + fmt.Sprint(b.parallelism()), "-trimpath", "-buildvcs=false"}
		if specification.tags != "" {
			args = append(args, "-tags", specification.tags)
		}
		if specification.mode != "" {
			args = append(args, "-buildmode", specification.mode)
		}
		if specification.flags != "" {
			args = append(args, "-ldflags", specification.flags)
		}
		target := filepath.Join(stage, specification.name)
		args = append(args, "-o", target, specification.target)
		fmt.Printf("Compiling %s with local Go\n", specification.name)
		text, err := b.execute(specification.directory, program, env, args...)
		if text != "" {
			fmt.Println(text)
		}
		if err != nil {
			return err
		}
		native, err := elf.Open(target)
		if err != nil {
			return fmt.Errorf("native artifact %s: %w", specification.name, err)
		}
		native.Close()
		info, err := buildinfo.ReadFile(target)
		if err != nil {
			return err
		}
		if info.GoVersion != "go"+b.tools.Go || info.Main.Path != specification.module {
			return fmt.Errorf("native artifact %s has wrong compiler/module identity", specification.name)
		}
		hash, err := hashFile(target)
		if err != nil {
			return err
		}
		mode := os.FileMode(0755)
		if specification.mode == "c-shared" {
			mode = 0644
			_ = os.Remove(strings.TrimSuffix(target, ".so") + ".h")
		}
		if err = os.Chmod(target, mode); err != nil {
			return err
		}
		receipts[specification.name] = map[string]any{"schema": "storage-gateway/native-build/v1", "input_manifest_sha256": inputHash, "artifact": specification.name, "sha256": hash, "product_commit": manifest.Config["product_commit"], "build_digest": manifest.Config["gateway_digest"], "ui_dist_digest": manifest.Config["ui_dist_digest"], "go_version": b.tools.Go, "module": info.Main.Path, "execution": "local-go", "cache": "local Go build and module cache", "command": append([]string{program}, args...)}
	}
	if b.withRclone {
		text, err := b.nativeTool("build")
		if text != "" {
			fmt.Println(text)
		}
		if err != nil {
			return err
		}
		source := filepath.Join(b.output, "materializer/bin/rclone")
		target := filepath.Join(stage, "rclone")
		if err = copyInput(source, target); err != nil {
			return err
		}
		if err = os.Chmod(target, 0755); err != nil {
			return err
		}
		hash, err := hashFile(target)
		if err != nil {
			return err
		}
		receipts["rclone"] = map[string]any{"schema": "storage-gateway/native-import/v1", "artifact": "rclone", "sha256": hash, "build_digest": manifest.Config["rclone_digest"], "owner_receipt": filepath.Join(b.output, "materializer/receipts/rclone.json"), "execution": "local-go"}
	}
	if _, err = b.admitRemoteSnapshot(); err != nil {
		return err
	}
	// Publish only after every requested artifact and its source closure pass.
	if err = replaceDirectory(stage, filepath.Join(b.output, "bin")); err != nil {
		return err
	}
	for name, receipt := range receipts {
		if err = writeJSON(filepath.Join(b.output, "receipts", name+".json"), receipt); err != nil {
			return err
		}
	}
	return writeJSON(filepath.Join(b.output, "receipts/native-release.json"), map[string]any{"schema": "storage-gateway/native-release/v1", "input_manifest_sha256": inputHash, "config": manifest.Config, "artifacts": receipts, "execution": "local-go", "independent_rclone": b.withRclone})
}

func (b *builder) runLinkTool(dir, program string, env map[string]string, args ...string) (string, error) {
	return b.execute(dir, program, env, args...)
}

// nativeLinkInputs describes the real C tool and canonical shared-memory SDK
// selected for this build. Its files join the same immutable input manifest as
// Go sources; SDK receipts remain the SDK producer's records.
type nativeLinkInputs struct {
	SDK    string
	Tools  map[string]string
	Config map[string]string
}

type sharedMemorySDKReceipt struct {
	Schema                string `json:"schema"`
	SourceCommit          string `json:"source_commit"`
	SourceCommitEvidence  string `json:"source_commit_evidence"`
	CanonicalSourceCommit string `json:"canonical_source_commit"`
	ABIProfile            int    `json:"abi_profile"`
	Target                struct {
		OS      string `json:"os"`
		Machine string `json:"machine"`
	} `json:"target"`
	Artifacts []struct {
		Path   string `json:"path"`
		SHA256 string `json:"sha256"`
	} `json:"artifacts"`
}

func (b *builder) linkInputs() (nativeLinkInputs, error) {
	result := nativeLinkInputs{Tools: map[string]string{}, Config: map[string]string{}}
	env := b.nativeEnvironment()
	program := env["PKG_CONFIG"]
	if program == "" {
		program = "pkg-config"
	}
	program, err := exec.LookPath(program)
	if err != nil {
		return result, fmt.Errorf("native pkg-config tool is required: %w", err)
	}
	program, err = filepath.EvalSymlinks(program)
	if err != nil {
		return result, err
	}
	prefix, err := b.runLinkTool(b.root, program, env, "--variable=prefix", "infra-shared-memory")
	if err != nil {
		return result, fmt.Errorf("canonical infra-shared-memory SDK is required; install selected Infra c/shared-memory and set NATIVE_SDK_ROOT or PKG_CONFIG_PATH: %w", err)
	}
	if strings.TrimSpace(prefix) == "" {
		return result, errors.New("pkg-config returned no canonical infra-shared-memory SDK prefix")
	}
	prefix, err = filepath.Abs(strings.TrimSpace(prefix))
	if err == nil {
		prefix, err = filepath.EvalSymlinks(prefix)
	}
	if err != nil {
		return result, err
	}
	if b.nativeSDKRoot != "" {
		selected, err := filepath.Abs(b.nativeSDKRoot)
		if err == nil {
			selected, err = filepath.EvalSymlinks(selected)
		}
		if err != nil || selected != prefix {
			return result, errors.New("pkg-config resolved a different SDK from the explicit native-sdk-root")
		}
	}
	pcdir, err := b.runLinkTool(b.root, program, env, "--variable=pcfiledir", "infra-shared-memory")
	if err != nil {
		return result, err
	}
	selectedPC := filepath.Join(strings.TrimSpace(pcdir), "infra-shared-memory.pc")
	selectedPC, err = filepath.EvalSymlinks(selectedPC)
	if err != nil {
		return result, err
	}
	expectedPC, err := filepath.EvalSymlinks(filepath.Join(prefix, "lib/pkgconfig/infra-shared-memory.pc"))
	if err != nil {
		return result, err
	}
	if selectedPC != expectedPC {
		return result, errors.New("pkg-config selected a different infra-shared-memory.pc than the verified SDK")
	}
	cflags, err := b.runLinkTool(b.root, program, env, "--cflags", "infra-shared-memory")
	if err != nil {
		return result, err
	}
	libs, err := b.runLinkTool(b.root, program, env, "--libs", "--static", "infra-shared-memory")
	if err != nil {
		return result, err
	}
	if err = validateSDKFlags(prefix, cflags, libs); err != nil {
		return result, err
	}
	result.Config["native_sdk_pkg_config_file"] = selectedPC
	result.Config["native_sdk_cflags"] = cflags
	result.Config["native_sdk_libs"] = libs
	var receipt sharedMemorySDKReceipt
	path := filepath.Join(prefix, "shared-memory-sdk.json")
	if err = readJSON(path, &receipt); err != nil {
		return result, fmt.Errorf("read canonical SDK receipt: %w", err)
	}
	if receipt.Schema != "infra/shared-memory-native-sdk/v1" || receipt.ABIProfile != 2 || receipt.Target.OS != "linux" || receipt.Target.Machine != "x86_64" || receipt.SourceCommitEvidence != "declared-build-input" {
		return result, errors.New("native SDK identity is not canonical Linux x86_64 ABI profile 2")
	}
	if len(receipt.SourceCommit) != 40 && len(receipt.SourceCommit) != 64 {
		return result, errors.New("native SDK receipt omits exact producer source commit")
	}
	if _, err = hex.DecodeString(receipt.SourceCommit); err != nil {
		return result, errors.New("invalid native SDK producer commit")
	}
	var authority struct {
		CanonicalCommit string `json:"canonical_commit"`
		CanonicalFiles  []struct {
			Path   string `json:"path"`
			SHA256 string `json:"sha256"`
		} `json:"canonical_files"`
	}
	raw, err := os.ReadFile(filepath.Join(b.infra, "c/shared-memory/native-source.json"))
	if err != nil {
		return result, err
	}
	if err = json.Unmarshal(raw, &authority); err != nil {
		return result, err
	}
	if receipt.CanonicalSourceCommit != authority.CanonicalCommit || authority.CanonicalCommit == "" {
		return result, errors.New("native SDK canonical source identity differs from selected Infra")
	}
	expected := map[string]bool{"include/infra/shm_snapshot.h": true, "lib/libinfra_shm_snapshot.a": true, "lib/pkgconfig/infra-shared-memory.pc": true}
	for _, artifact := range receipt.Artifacts {
		if !expected[artifact.Path] {
			return result, errors.New("native SDK receipt has unknown or duplicated artifacts")
		}
		delete(expected, artifact.Path)
		actual, err := hashFile(filepath.Join(prefix, artifact.Path))
		if err != nil || actual != artifact.SHA256 {
			return result, fmt.Errorf("native SDK artifact identity differs: %s", artifact.Path)
		}
		if artifact.Path == "include/infra/shm_snapshot.h" {
			matched := false
			for _, file := range authority.CanonicalFiles {
				if file.Path == "c/shared-memory/"+artifact.Path && file.SHA256 == actual {
					canonical, err := hashFile(filepath.Join(b.infra, file.Path))
					matched = err == nil && canonical == actual
				}
			}
			if !matched {
				return result, errors.New("native SDK header differs from selected canonical producer")
			}
		}
	}
	if len(expected) != 0 {
		return result, errors.New("native SDK receipt is missing linked artifacts")
	}
	result.SDK = prefix
	result.Tools["pkg-config"] = program
	result.Config["native_sdk_root"] = prefix
	result.Config["native_sdk_source_commit"] = receipt.SourceCommit
	result.Config["native_sdk_canonical_commit"] = receipt.CanonicalSourceCommit
	result.Config["native_sdk_receipt_sha256"], err = hashFile(path)
	if err != nil {
		return result, err
	}
	goProgram, err := b.selectedGo()
	if err != nil {
		return result, err
	}
	metadata, err := b.runLinkTool(b.root, goProgram, env, "env", "-json", "GOFLAGS", "CC", "CGO_CFLAGS", "CGO_CPPFLAGS", "CGO_CXXFLAGS", "CGO_LDFLAGS", "PKG_CONFIG")
	if err != nil {
		return result, err
	}
	var settings map[string]string
	if err = json.Unmarshal([]byte(metadata), &settings); err != nil {
		return result, err
	}
	if err = validateNativeFlags(settings, filepath.Join(b.infra, "c/interop/plugins/include")); err != nil {
		return result, err
	}
	compiler, err := exec.LookPath(settings["CC"])
	if err != nil {
		return result, fmt.Errorf("native CC must identify one executable: %w", err)
	}
	compiler, err = filepath.EvalSymlinks(compiler)
	if err != nil {
		return result, err
	}
	result.Tools["cc"] = compiler
	for key, value := range settings {
		result.Config["native_"+strings.ToLower(key)] = value
	}
	for _, key := range []string{"PKG_CONFIG_PATH", "PKG_CONFIG_LIBDIR"} {
		result.Config["native_"+strings.ToLower(key)] = env[key]
	}
	for name, file := range result.Tools {
		result.Config["native_"+name+"_path"] = file
		result.Config["native_"+name+"_sha256"], err = hashFile(file)
		if err != nil {
			return result, err
		}
	}
	return result, nil
}

// The SDK owns one include directory and one static archive. Do not accept a
// foreign .pc that reports an admitted prefix but redirects its actual inputs.
// The product's native recipe admits the canonical plugin include and flags
// that do not add files. Extra headers, archives, search paths, response files
// and compiler plugins are not part of its captured source/SDK manifest.
func validateNativeFlags(settings map[string]string, pluginInclude string) error {
	for _, key := range []string{"CGO_CFLAGS", "CGO_CPPFLAGS", "CGO_CXXFLAGS", "CGO_LDFLAGS"} {
		for _, flag := range strings.Fields(settings[key]) {
			switch flag {
			case "-O0", "-O1", "-O2", "-O3", "-Os", "-Og", "-g", "-g0", "-g1", "-g2", "-g3", "-fPIC", "-pthread":
				continue
			}
			if key != "CGO_LDFLAGS" && flag == "-I"+pluginInclude {
				continue
			}
			return fmt.Errorf("%s contains an input outside the declared native recipe", key)
		}
	}
	return nil
}

func validateSDKFlags(prefix, cflags, libs string) error {
	required := map[string]bool{"include": false, "lib": false, "archive": false}
	for _, flag := range strings.Fields(cflags + " " + libs) {
		switch {
		case strings.HasPrefix(flag, "-I"), strings.HasPrefix(flag, "-L"):
			kind := "include"
			if flag[:2] == "-L" {
				kind = "lib"
			}
			actual, err := filepath.EvalSymlinks(filepath.Clean(flag[2:]))
			want, wantErr := filepath.EvalSymlinks(filepath.Join(prefix, kind))
			if err != nil || wantErr != nil || actual != want {
				return fmt.Errorf("SDK pkg-config flag escapes its verified %s directory: %s", kind, flag)
			}
			required[kind] = true
		case flag == "-linfra_shm_snapshot":
			required["archive"] = true
		default:
			return fmt.Errorf("unadmitted SDK pkg-config link input: %s", flag)
		}
	}
	for kind, found := range required {
		if !found {
			return fmt.Errorf("SDK pkg-config omitted required %s input", kind)
		}
	}
	return nil
}

type nativeArtifactReceipt struct {
	Schema              string   `json:"schema"`
	InputManifestSHA256 string   `json:"input_manifest_sha256,omitempty"`
	Artifact            string   `json:"artifact"`
	SHA256              string   `json:"sha256"`
	ProductCommit       string   `json:"product_commit,omitempty"`
	BuildDigest         string   `json:"build_digest"`
	UIDistDigest        string   `json:"ui_dist_digest,omitempty"`
	GoVersion           string   `json:"go_version,omitempty"`
	Module              string   `json:"module,omitempty"`
	Execution           string   `json:"execution"`
	Cache               string   `json:"cache,omitempty"`
	Command             []string `json:"command,omitempty"`
	OwnerReceipt        string   `json:"owner_receipt,omitempty"`
}
type nativeReleaseReceipt struct {
	Schema              string                           `json:"schema"`
	InputManifestSHA256 string                           `json:"input_manifest_sha256"`
	Config              map[string]string                `json:"config"`
	Artifacts           map[string]nativeArtifactReceipt `json:"artifacts"`
	Execution           string                           `json:"execution"`
	IndependentRclone   bool                             `json:"independent_rclone"`
}

func validateNativeRelease(release nativeReleaseReceipt, config map[string]string, inputHash string, withRclone bool) error {
	if release.Schema != "storage-gateway/native-release/v1" || release.Execution != "local-go" || release.InputManifestSHA256 != inputHash || !reflect.DeepEqual(release.Config, config) || release.IndependentRclone != withRclone {
		return errors.New("native release differs from the current admitted source, SDK, toolchain or build selection")
	}
	modules := map[string]string{"gateway": "github.com/OpenListTeam/OpenList/v4", "openlist-control-plane": "github.com/OpenListTeam/OpenList/v4", "client-openlist.so": "github.com/OpenListTeam/OpenList/v4", "storage-worker": "github.com/wignerStan/remote-storage-gateway/go", "client-rclone.so": "github.com/wignerStan/remote-storage-gateway/go", "transfer-reapi.so": "github.com/wignerStan/remote-storage-gateway/go", "gateway-admin": "github.com/wignerStan/remote-storage-gateway/go"}
	count := len(modules)
	if withRclone {
		count++
	}
	if len(release.Artifacts) != count {
		return errors.New("native release has an incomplete or unexpected artifact set")
	}
	for name, module := range modules {
		record, ok := release.Artifacts[name]
		if !ok || record.Schema != "storage-gateway/native-build/v1" || record.Artifact != name || record.Module != module || record.Execution != "local-go" || record.GoVersion != "1.27.3" || record.InputManifestSHA256 != inputHash || record.BuildDigest != config["gateway_digest"] || record.ProductCommit != config["product_commit"] || record.UIDistDigest != config["ui_dist_digest"] || len(record.Command) == 0 {
			return fmt.Errorf("native artifact receipt differs from current release: %s", name)
		}
	}
	if withRclone {
		record := release.Artifacts["rclone"]
		if record.Schema != "storage-gateway/native-import/v1" || record.Artifact != "rclone" || record.Execution != "local-go" || record.BuildDigest != config["rclone_digest"] || record.OwnerReceipt == "" {
			return errors.New("independent rclone import differs from its owner build")
		}
	}
	return nil
}
func (b *builder) admitNativeRelease(manifest remoteInputManifest) (nativeReleaseReceipt, error) {
	var release nativeReleaseReceipt
	inputHash, err := hashFile(filepath.Join(b.output, "build-inputs/manifest.json"))
	if err != nil {
		return release, err
	}
	if err = readJSON(filepath.Join(b.output, "receipts/native-release.json"), &release); err != nil {
		return release, err
	}
	if err = validateNativeRelease(release, manifest.Config, inputHash, b.withRclone); err != nil {
		return release, err
	}
	for name, receipt := range release.Artifacts {
		var individual nativeArtifactReceipt
		if err = readJSON(filepath.Join(b.output, "receipts", name+".json"), &individual); err != nil {
			return release, err
		}
		if !reflect.DeepEqual(individual, receipt) {
			return release, fmt.Errorf("artifact receipt differs from completed native release: %s", name)
		}
		actual, err := hashFile(filepath.Join(b.output, "bin", name))
		if err != nil {
			return release, err
		}
		if actual != receipt.SHA256 {
			return release, fmt.Errorf("native artifact changed since completed build: %s", name)
		}
	}
	if b.withRclone {
		var owner struct {
			Schema            string `json:"schema"`
			BinarySHA256      string `json:"binary_sha256"`
			NativeBuildDigest string `json:"native_build_digest"`
			GoVersion         string `json:"go_version"`
			RcloneVersion     string `json:"rclone_version"`
		}
		if err = readJSON(filepath.Join(b.output, "materializer/receipts/rclone.build.json"), &owner); err != nil {
			return release, err
		}
		record := release.Artifacts["rclone"]
		if owner.Schema != "rclone/native-build/v1" || owner.BinarySHA256 != record.SHA256 || owner.NativeBuildDigest != record.BuildDigest || owner.GoVersion != b.tools.Go || owner.RcloneVersion != manifest.Config["rclone_version"] || record.OwnerReceipt != filepath.Join(b.output, "materializer/receipts/rclone.json") {
			return release, errors.New("independent rclone owner receipt differs from imported ELF")
		}
	}
	return release, nil
}

func (b *builder) imageInputs() error {
	manifest, err := b.admitRemoteSnapshot()
	if err != nil {
		return err
	}
	release, err := b.admitNativeRelease(manifest)
	if err != nil {
		return err
	}
	expected := map[string]string{}
	for name, receipt := range release.Artifacts {
		expected[name] = receipt.SHA256
	}
	expected["ca-certificates.crt"], err = hashFile(b.ca)
	if err != nil {
		return err
	}
	stage, err := os.MkdirTemp(b.output, "image-inputs-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(stage)
	if err = b.copyImageCompositions(stage, expected); err != nil {
		return err
	}
	manifest, err = b.admitRemoteSnapshot()
	if err != nil {
		return err
	}
	if _, err = b.admitNativeRelease(manifest); err != nil {
		return err
	}
	return replaceDirectory(stage, filepath.Join(b.output, "image-inputs"))
}

func (b *builder) copyImageCompositions(stage string, expected map[string]string) error {
	compositions := map[string][]string{"gateway": {"storage-worker", "client-openlist.so", "client-rclone.so", "transfer-reapi.so", "ca-certificates.crt"}, "control-plane": {"openlist-control-plane", "ca-certificates.crt"}, "bundle": {"gateway", "client-openlist.so", "client-rclone.so", "transfer-reapi.so", "ca-certificates.crt"}}
	if b.withRclone {
		compositions["bundle"] = append(compositions["bundle"], "rclone")
	}
	for _, composition := range []string{"gateway", "control-plane", "bundle"} {
		dir := filepath.Join(stage, composition)
		if err := os.MkdirAll(dir, 0755); err != nil {
			return err
		}
		manifest := map[string]string{}
		for _, name := range compositions[composition] {
			src := filepath.Join(b.output, "bin", name)
			if name == "ca-certificates.crt" {
				src = b.ca
			}
			dst := filepath.Join(dir, name)
			if err := copyInput(src, dst); err != nil {
				return err
			}
			actual, err := hashFile(dst)
			if err != nil {
				return err
			}
			if expected[name] == "" || actual != expected[name] {
				return fmt.Errorf("native input changed during composition: %s", name)
			}
			manifest[name] = actual
		}
		if err := writeJSON(filepath.Join(dir, "inputs.json"), manifest); err != nil {
			return err
		}
	}
	return nil
}
