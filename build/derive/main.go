// Command derive builds one pinned OpenList derivation without editing upstream.
// Invoke from go/: go run ../build/derive/main.go [flags] source|ui|cp|plugin|test.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	processexec "github.com/wignerStan/infra/go/process/exec"
	"golang.org/x/mod/modfile"
	"golang.org/x/mod/semver"
	"golang.org/x/mod/sumdb/dirhash"
)

type patch struct {
	Order  int    `json:"order"`
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
}
type upstream struct {
	URL    string `json:"url"`
	Commit string `json:"commit"`
}
type relationship struct {
	Schema             string   `json:"schema_version"`
	Kind               string   `json:"kind"`
	Path               string   `json:"path"`
	Owner              string   `json:"owner"`
	Integration        string   `json:"integration"`
	Upstream           upstream `json:"upstream"`
	Patches            []patch  `json:"patches,omitempty"`
	PatchBundle        *string  `json:"patch_bundle,omitempty"`
	BuildPath          *string  `json:"build_path,omitempty"`
	DependencyManifest any      `json:"dependency_manifest,omitempty"`
	PublicAPI          string   `json:"public_api"`
	ResultTree         *string  `json:"result_tree"`
	Licenses           []string `json:"licenses"`
}
type toolchain struct {
	Go   string `json:"go"`
	Node string `json:"node"`
	PNPM string `json:"pnpm"`
}
type builder struct {
	nativeSDKRoot, pkgConfig                           string
	gatewayBuild, gatewaySource, rcloneBuild           string
	root, output, infra, dataPlane, materializer, mise string
	buildbarn                                          string
	goBinary, nodeBinary, pnpmBinary                   string
	jobs                                               int
	withRclone                                         bool
	referenceRoot                                      string
	ca                                                 string
	release                                            bool
	tools                                              toolchain
	downloadModule                                     func(string, string) (moduleDownload, error)
}

const rcloneModule = "github.com/rclone/rclone"

type modulePatchSeries struct {
	Schema       string  `json:"schema"`
	Module       string  `json:"module"`
	Patches      []patch `json:"patches"`
	BazelPatches []patch `json:"bazel_patches,omitempty"`
}
type moduleDownload struct {
	Path, Version, Dir, Sum, GoModSum string
	Error                             string
}
type moduleSourceReceipt struct {
	Schema               string  `json:"schema"`
	Module               string  `json:"module"`
	Version              string  `json:"version"`
	ModuleSum            string  `json:"module_sum"`
	GoModSum             string  `json:"go_mod_sum"`
	CanonicalDirectory   string  `json:"canonical_directory"`
	OwnerRepository      string  `json:"owner_repository"`
	OwnerGoModSHA256     string  `json:"owner_go_mod_sha256"`
	PatchInventorySHA256 string  `json:"patch_inventory_sha256"`
	Patches              []patch `json:"patches"`
	DerivedDirectory     string  `json:"derived_directory"`
	DerivedContentHash   string  `json:"derived_content_hash"`
}

func (b *builder) rcloneVersion() (string, error) {
	path := filepath.Join(b.infra, "go/go.mod")
	body, e := os.ReadFile(path)
	if e != nil {
		return "", e
	}
	file, e := modfile.Parse(path, body, nil)
	if e != nil {
		return "", e
	}
	for _, require := range file.Require {
		if require.Mod.Path == rcloneModule {
			if !semver.IsValid(require.Mod.Version) {
				return "", errors.New("Infra must require a concrete rclone package version")
			}
			return require.Mod.Version, nil
		}
	}
	return "", errors.New("selected Infra go/go.mod must require the rclone module")
}
func (b *builder) rcloneSeries() (modulePatchSeries, error) {
	var series modulePatchSeries
	for _, root := range []string{b.root, b.infra} {
		if _, e := os.Lstat(filepath.Join(root, ".git-source-relationships/rclone.json")); e == nil {
			return series, errors.New("rclone is a Go module dependency, not a Git source relationship")
		} else if !errors.Is(e, fs.ErrNotExist) {
			return series, e
		}
	}
	if _, e := os.Lstat(filepath.Join(b.root, "patches/rclone")); e == nil {
		return series, errors.New("rclone patches belong to Infra, not the product")
	} else if !errors.Is(e, fs.ErrNotExist) {
		return series, e
	}
	if e := readJSON(filepath.Join(b.infra, "go/patches/rclone/series.json"), &series); e != nil {
		return series, e
	}
	if series.Schema != "go-module-patch-series/v1" || series.Module != rcloneModule || len(series.Patches) == 0 {
		return series, errors.New("invalid Infra rclone patch inventory")
	}
	for i, p := range series.Patches {
		if p.Order != i+1 || !safeRelative(p.Path) || strings.Contains(p.Path, "\\") || !strings.HasSuffix(p.Path, ".patch") || len(p.SHA256) != 64 {
			return series, errors.New("invalid ordered rclone patch")
		}
		// Infra paths are relative to the inventory directory. Only the native
		// semantic series applies here; Bazel annotations never patch Go source.
		series.Patches[i].Path = filepath.Join("go/patches/rclone", p.Path)
	}
	return series, nil
}
func (b *builder) moduleChecksums(version string) (string, string, error) {
	body, e := os.ReadFile(filepath.Join(b.infra, "go/go.sum"))
	if e != nil {
		return "", "", e
	}
	var sum, modSum string
	for _, line := range strings.Split(string(body), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 3 || fields[0] != rcloneModule {
			continue
		}
		if fields[1] == version {
			sum = fields[2]
		}
		if fields[1] == version+"/go.mod" {
			modSum = fields[2]
		}
	}
	if !strings.HasPrefix(sum, "h1:") || !strings.HasPrefix(modSum, "h1:") {
		return "", "", errors.New("selected Infra go.sum must contain rclone package integrity checksums")
	}
	return sum, modSum, nil
}
func (b *builder) fetchModule(version string) (moduleDownload, error) {
	if b.downloadModule != nil {
		return b.downloadModule(rcloneModule, version)
	}
	if e := os.MkdirAll(b.output, 0755); e != nil {
		return moduleDownload{}, e
	}
	// Explicit module@version works outside a main module. Downloads cannot
	// update the selected Infra or product go.mod/go.sum files.
	raw, e := b.goCommand(b.output, nil, "mod", "download", "-json", rcloneModule+"@"+version)
	if e != nil {
		return moduleDownload{}, e
	}
	var info moduleDownload
	if e = json.Unmarshal([]byte(raw), &info); e != nil {
		return info, e
	}
	if info.Error != "" {
		return info, errors.New("Go module download failed")
	}
	return info, nil
}
func verifyModuleCache(info moduleDownload, module, version, sum, modSum string) error {
	if info.Path != module || info.Version != version || !filepath.IsAbs(info.Dir) || info.Sum != sum || info.GoModSum != modSum {
		return errors.New("downloaded module identity/checksum differs from Infra dependency")
	}
	actual, e := dirhash.HashDir(info.Dir, module+"@"+version, dirhash.Hash1)
	if e != nil {
		return e
	}
	if actual != sum {
		return errors.New("canonical Go module cache integrity mismatch")
	}
	actual, e = dirhash.Hash1([]string{"go.mod"}, func(name string) (io.ReadCloser, error) { return os.Open(filepath.Join(info.Dir, name)) })
	if e != nil {
		return e
	}
	if actual != modSum {
		return errors.New("canonical module go.mod integrity mismatch")
	}
	return nil
}
func (b *builder) rcloneModulePath() string { return b.sourcePath("rclone-module") }
func (b *builder) moduleReceiptPath() string {
	return filepath.Join(b.output, "receipts/rclone-module-source.json")
}
func (b *builder) verifyModuleSnapshot(version string, series modulePatchSeries) (moduleSourceReceipt, error) {
	var receipt moduleSourceReceipt
	if e := readJSON(b.moduleReceiptPath(), &receipt); e != nil {
		return receipt, e
	}
	sum, modSum, e := b.moduleChecksums(version)
	if e != nil {
		return receipt, e
	}
	inventory, e := hashFile(filepath.Join(b.infra, "go/patches/rclone/series.json"))
	if e != nil {
		return receipt, e
	}
	if receipt.Schema != "go-module-derived-source/v1" || receipt.Module != rcloneModule || receipt.Version != version || receipt.ModuleSum != sum || receipt.GoModSum != modSum || receipt.PatchInventorySHA256 != inventory || receipt.DerivedDirectory != b.rcloneModulePath() {
		return receipt, errors.New("module snapshot inputs changed; select a fresh build directory")
	}
	a, _ := json.Marshal(receipt.Patches)
	c, _ := json.Marshal(series.Patches)
	if string(a) != string(c) {
		return receipt, errors.New("module snapshot patch inventory changed")
	}
	actual, e := treeDigest(receipt.DerivedDirectory)
	if e != nil {
		return receipt, e
	}
	if "sha256:"+actual != receipt.DerivedContentHash {
		return receipt, errors.New("derived module snapshot integrity changed")
	}
	return receipt, nil
}
func (b *builder) materializeRcloneModule() error {
	version, e := b.rcloneVersion()
	if e != nil {
		return e
	}
	series, e := b.rcloneSeries()
	if e != nil {
		return e
	}
	sum, modSum, e := b.moduleChecksums(version)
	if e != nil {
		return e
	}
	for _, patch := range series.Patches {
		actual, e := hashFile(filepath.Join(b.infra, patch.Path))
		if e != nil {
			return e
		}
		if actual != patch.SHA256 {
			return errors.New("Infra rclone patch checksum mismatch")
		}
	}
	info, e := b.fetchModule(version)
	if e != nil {
		return e
	}
	if e = verifyModuleCache(info, rcloneModule, version, sum, modSum); e != nil {
		return e
	}
	destination := b.rcloneModulePath()
	var receipt moduleSourceReceipt
	if _, e = os.Lstat(destination); e == nil {
		receipt, e = b.verifyModuleSnapshot(version, series)
		if e != nil {
			return e
		}
	} else if !errors.Is(e, fs.ErrNotExist) {
		return e
	} else {
		if e = copyTree(info.Dir, destination); e != nil {
			return e
		}
		if e = verifyModuleCache(moduleDownload{Path: rcloneModule, Version: version, Dir: destination, Sum: sum, GoModSum: modSum}, rcloneModule, version, sum, modSum); e != nil {
			return e
		}
		for _, patch := range series.Patches {
			path := filepath.Join(b.infra, patch.Path)
			if _, e = b.git(destination, "apply", "--check", path); e != nil {
				return e
			}
			if _, e = b.git(destination, "apply", path); e != nil {
				return e
			}
		}
		actual, e := treeDigest(destination)
		if e != nil {
			return e
		}
		inventory, e := hashFile(filepath.Join(b.infra, "go/patches/rclone/series.json"))
		if e != nil {
			return e
		}
		receipt = moduleSourceReceipt{Schema: "go-module-derived-source/v1", Module: rcloneModule, Version: version, ModuleSum: sum, GoModSum: modSum, PatchInventorySHA256: inventory, Patches: series.Patches, DerivedDirectory: destination, DerivedContentHash: "sha256:" + actual}
	}
	// This hash measures the derived bytes; it is never a package-version or
	// predeclared Git result-tree acceptance gate.
	if e = verifyModuleCache(info, rcloneModule, version, sum, modSum); e != nil {
		return e
	}
	ownerHash, e := hashFile(filepath.Join(b.infra, "go/go.mod"))
	if e != nil {
		return e
	}
	receipt.OwnerRepository = b.infra
	receipt.OwnerGoModSHA256 = ownerHash
	receipt.CanonicalDirectory = info.Dir
	return writeJSON(b.moduleReceiptPath(), receipt)
}

func main() {
	b := builder{}
	flag.StringVar(&b.referenceRoot, "source-references", "", "optional existing upstream Git object references; no source copies")
	flag.StringVar(&b.root, "root", "..", "canonical monorepo root")
	flag.StringVar(&b.output, "out", filepath.Join(os.TempDir(), "storage-gateway-native-build"), "external build directory")
	flag.StringVar(&b.infra, "infra-root", "/develop/infra", "explicit Infra source checkout (reviewed and clean for release)")
	flag.StringVar(&b.dataPlane, "data-plane-root", "/develop/network/data-plane-worker", "explicit independently owned Data Plane module checkout")
	flag.StringVar(&b.materializer, "rclone-root", "/develop/network/rclone", "independently derived rclone application and process module")
	flag.StringVar(&b.buildbarn, "buildbarn-root", "", "explicit BuildBarn source materializer module checkout")
	flag.StringVar(&b.mise, "mise", "", "optional Mise executable for pinned toolchains")
	flag.StringVar(&b.goBinary, "go", "go", "native Go executable")
	flag.StringVar(&b.nativeSDKRoot, "native-sdk-root", "", "optional verified canonical shared-memory SDK prefix; defaults to pkg-config discovery")
	flag.StringVar(&b.pkgConfig, "pkg-config", os.Getenv("PKG_CONFIG"), "pkg-config executable (default pkg-config)")
	flag.StringVar(&b.nodeBinary, "node", "node", "native Node executable")
	flag.StringVar(&b.pnpmBinary, "pnpm", "pnpm", "native pnpm executable")
	flag.IntVar(&b.jobs, "jobs", 2, "maximum compiler parallelism")
	flag.BoolVar(&b.withRclone, "with-rclone", false, "also compile the independent rclone executable")
	flag.StringVar(&b.ca, "ca-bundle", "/etc/ssl/certs/ca-certificates.crt", "public runtime CA bundle")
	flag.BoolVar(&b.release, "release", false, "require clean exact product and Infra source commits")
	flag.StringVar(&b.gatewayBuild, "gateway-native-build", "", "completed original Gateway native build directory for OCI-only assembly")
	flag.StringVar(&b.gatewaySource, "gateway-native-source", "", "clean source checkout recorded by the original Gateway build")
	flag.StringVar(&b.rcloneBuild, "rclone-native-build", "", "completed independent rclone native build directory for OCI-only assembly")
	flag.Parse()
	if flag.NArg() != 1 {
		fmt.Fprintln(os.Stderr, "expected one stage: source, ui, cp, plugin, test, inputs")
		os.Exit(2)
	}
	if err := b.run(flag.Arg(0)); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func readJSON(path string, value any) error {
	return readJSONLimit(path, value, 1<<20)
}

func readJSONLimit(path string, value any, limit int64) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return err
	}
	if limit <= 0 || info.Size() > limit {
		return fmt.Errorf("JSON exceeds %d byte limit: %s", limit, path)
	}
	d := json.NewDecoder(io.LimitReader(f, limit+1))
	d.DisallowUnknownFields()
	if err = d.Decode(value); err != nil {
		return fmt.Errorf("decode %s: %w", path, err)
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		return fmt.Errorf("unexpected trailing JSON: %s", path)
	}
	return nil
}
func writeJSON(path string, value any) error {
	b, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	if err = os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err = os.WriteFile(tmp, append(b, '\n'), 0644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
func hashFile(path string) (string, error) {
	f, e := os.Open(path)
	if e != nil {
		return "", e
	}
	defer f.Close()
	h := sha256.New()
	if _, e = io.Copy(h, f); e != nil {
		return "", e
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
func safeRelative(p string) bool {
	return p != "" && !filepath.IsAbs(p) && filepath.Clean(p) == p && p != ".." && !strings.HasPrefix(p, ".."+string(filepath.Separator))
}

func buildEnvironment(overrides map[string]string) processexec.EnvPolicy {
	values := make(map[string]string)
	for _, item := range os.Environ() {
		if key, value, ok := strings.Cut(item, "="); ok {
			values[key] = value
		}
	}
	for key, value := range overrides {
		values[key] = value
	}
	// Search paths outside the captured native recipe must be absent, even
	// when supplied as overrides. Empty GCC_EXEC_PREFIX changes cc1 lookup;
	// an empty search-path component can also admit the working directory.
	for _, key := range []string{"CPATH", "C_INCLUDE_PATH", "CPLUS_INCLUDE_PATH", "OBJC_INCLUDE_PATH", "LIBRARY_PATH", "COMPILER_PATH", "GCC_EXEC_PREFIX"} {
		delete(values, key)
	}
	return processexec.EnvClean(values)
}

func (b *builder) command(dir, program string, args ...string) (string, error) {
	s := processexec.NewSpec(program, args...).WithTimeout(30 * time.Minute).WithEnv(buildEnvironment(map[string]string{
		"GOWORK":             "off",
		"GIT_OPTIONAL_LOCKS": "0",
		"HUSKY":              "0", "CI": "true",
	}))
	s.Dir = dir
	s.MaxStdout = 8 << 20
	s.MaxStderr = 8 << 20
	o, e := processexec.RunContext(context.Background(), s)
	if e != nil {
		return "", fmt.Errorf("%s failed in %s: %w\n%s%s", program, dir, e, o.Stdout, o.Stderr)
	}
	if o.StdoutTruncated || o.StderrTruncated {
		return "", fmt.Errorf("%s output exceeded limit", program)
	}
	return strings.TrimSpace(string(o.Stdout)), nil
}
func (b *builder) git(dir string, args ...string) (string, error) {
	return b.command(dir, "git", args...)
}
func (b *builder) verify(r relationship) error {
	if r.Schema != "git-source-relationship/v5" || r.Kind != "upstream_product_build" || r.Integration != "build" || !safeRelative(r.Path) || !strings.HasPrefix(r.Path, "build/") || r.BuildPath == nil || *r.BuildPath != r.Path || r.ResultTree == nil || len(r.Upstream.Commit) != 40 || r.Upstream.URL == "" {
		return errors.New("invalid upstream product relationship")
	}
	if raw, err := hex.DecodeString(r.Upstream.Commit); err != nil || len(raw) != 20 || strings.ToLower(r.Upstream.Commit) != r.Upstream.Commit {
		return errors.New("invalid immutable upstream commit")
	}
	if _, err := b.git(b.root, "check-ignore", "--no-index", "--", r.Path+"/"); err != nil {
		return errors.New("upstream product materialization must be ignored")
	}
	tracked, err := b.git(b.root, "ls-files", "--", r.Path)
	if err != nil || tracked != "" {
		return errors.New("upstream product must not be parent tracked")
	}
	if r.PatchBundle == nil {
		if len(r.Patches) != 0 {
			return errors.New("patch list without package authority")
		}
		return nil
	}
	if *r.PatchBundle != "patches/"+filepath.Base(r.Path) {
		return errors.New("wrong patch package")
	}
	resolved, err := b.packagePatches(*r.PatchBundle)
	if err != nil {
		return err
	}
	a, _ := json.Marshal(resolved)
	c, _ := json.Marshal(r.Patches)
	if string(a) != string(c) {
		return errors.New("patch list differs from package order authority")
	}
	return nil
}
func (b *builder) packagePatches(bundle string) ([]patch, error) {
	data, err := os.ReadFile(filepath.Join(b.root, bundle, "apply_order.txt"))
	if err != nil || len(data) > 1<<20 {
		return nil, errors.New("invalid patch package order")
	}
	var result []patch
	seen := map[string]bool{}
	targets := map[string]bool{}
	for _, row := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		fields := strings.Fields(row)
		if len(fields) != 3 || fields[0] != fmt.Sprint(len(result)+1) || !safeRelative(fields[1]) || !strings.HasSuffix(fields[1], ".patch") || seen[fields[1]] {
			return nil, errors.New("invalid semantic patch order")
		}
		seen[fields[1]] = true
		path := filepath.Join(bundle, fields[1])
		payload, err := os.ReadFile(filepath.Join(b.root, path))
		if err != nil || len(payload) > 8<<20 {
			return nil, errors.New("invalid semantic patch bytes")
		}
		hash, err := hashFile(filepath.Join(b.root, path))
		if err != nil || hash != fields[2] {
			return nil, errors.New("semantic patch hash mismatch")
		}
		if strings.Count(string(payload), "diff --git ") != 1 {
			return nil, errors.New("semantic patch must own one target")
		}
		header := strings.Fields(strings.SplitN(string(payload), "\n", 2)[0])
		if len(header) != 4 || !strings.HasPrefix(header[2], "a/") || header[3] != "b/"+strings.TrimPrefix(header[2], "a/") {
			return nil, errors.New("rename/copy patch rejected")
		}
		target := strings.TrimPrefix(header[2], "a/")
		if !safeRelative(target) || targets[target] || !strings.HasSuffix(fields[1], "/"+target+".patch") {
			return nil, errors.New("duplicate or misplaced semantic patch target")
		}
		targets[target] = true
		result = append(result, patch{Order: len(result) + 1, Path: path, SHA256: hash})
	}
	if len(result) == 0 {
		return nil, errors.New("empty patch package")
	}
	if err = filepath.WalkDir(filepath.Join(b.root, bundle), func(path string, d fs.DirEntry, e error) error {
		if e != nil {
			return e
		}
		if !d.IsDir() && strings.HasSuffix(path, ".patch") {
			rel, e := filepath.Rel(filepath.Join(b.root, bundle), path)
			if e != nil || !seen[rel] {
				return errors.New("unlisted semantic patch")
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return result, nil
}
func (b *builder) materialize(r relationship, destination string) error {
	if e := b.verify(r); e != nil {
		return e
	}
	if _, e := os.Lstat(destination); e == nil {
		return fmt.Errorf("destination exists; select a fresh build output: %s", destination)
	} else if !errors.Is(e, fs.ErrNotExist) {
		return e
	}
	if e := os.MkdirAll(filepath.Dir(destination), 0755); e != nil {
		return e
	}
	source := r.Upstream.URL
	reference := ""
	if b.referenceRoot != "" {
		candidate := filepath.Join(b.referenceRoot, filepath.Base(r.Path))
		if _, err := b.git(candidate, "cat-file", "-e", r.Upstream.Commit+"^{commit}"); err == nil {
			source = candidate
			reference = candidate
		}
	}
	cloneArgs := []string{"clone", "--no-checkout", source, destination}
	cloneMode := "upstream"
	if reference != "" {
		shallow, err := b.git(reference, "rev-parse", "--is-shallow-repository")
		if err != nil {
			return err
		}
		if shallow == "true" {
			cloneMode = "local-shallow"
		} else {
			cloneArgs = []string{"clone", "--reference", reference, "--dissociate", "--no-hardlinks", "--no-checkout", source, destination}
			cloneMode = "reference-dissociated"
		}
	}
	if _, err := b.git(b.root, cloneArgs...); err != nil {
		return err
	}
	if _, err := b.git(destination, "remote", "set-url", "origin", r.Upstream.URL); err != nil {
		return err
	}
	if _, e := b.git(destination, "checkout", "--detach", r.Upstream.Commit); e != nil {
		return e
	}
	for _, p := range r.Patches {
		if _, e := b.git(destination, "apply", "--index", filepath.Join(b.root, p.Path)); e != nil {
			return e
		}
	}
	tree, e := b.git(destination, "write-tree")
	if e != nil {
		return e
	}
	want := r.ResultTree
	if tree != *want {
		return fmt.Errorf("derived tree mismatch for %s: got %s want %s", r.Path, tree, *want)
	}
	return b.recordSource(r, tree, cloneMode)
}

func (b *builder) recordSource(r relationship, tree, cloneMode string) error {
	owner := b.root
	commit, e := b.git(owner, "rev-parse", "HEAD")
	if e != nil {
		return e
	}
	status, e := b.git(owner, "status", "--porcelain", "--untracked-files=all")
	if e != nil {
		return e
	}
	r.Patches = nil
	return writeJSON(filepath.Join(b.output, "receipts", filepath.Base(r.Path)+"-source.json"), struct {
		Relationship     relationship `json:"relationship"`
		Tree             string       `json:"tree"`
		CloneMode        string       `json:"clone_mode"`
		SourceRepository string       `json:"source_repository"`
		SourceCommit     string       `json:"source_commit"`
		SourceDirty      bool         `json:"source_dirty"`
	}{r, tree, cloneMode, owner, commit, status != ""})
}
func (b *builder) sourcePath(name string) string {
	if name == "rclone-module" {
		return filepath.Join(b.output, "source", name)
	}
	return filepath.Join(b.root, "build", name)
}
func (b *builder) load(name string) (relationship, error) {
	var r relationship
	switch name {
	case "openlist", "openlist-ui":
	default:
		return r, fmt.Errorf("unknown admitted source %q", name)
	}
	if e := readJSON(filepath.Join(b.root, ".git-source-relationships", name+".json"), &r); e != nil {
		return r, e
	}
	if r.Path != "build/"+name || r.Schema != "git-source-relationship/v5" {
		return r, fmt.Errorf("unexpected canonical path for %s", name)
	}
	if r.PatchBundle != nil {
		var err error
		r.Patches, err = b.packagePatches(*r.PatchBundle)
		if err != nil {
			return r, err
		}
	}
	return r, nil
}
func (b *builder) sources() ([]relationship, error) {
	var result []relationship
	for _, name := range []string{"openlist", "openlist-ui"} {
		r, e := b.load(name)
		if e != nil {
			return nil, e
		}
		result = append(result, r)
	}
	return result, nil
}
func (b *builder) verifyDerived(r relationship) error {
	dir := b.sourcePath(filepath.Base(r.Path))
	actual, e := b.git(dir, "write-tree")
	if e != nil {
		return e
	}
	want := r.ResultTree
	if want == nil {
		tree, e := b.git(filepath.Join(b.root, r.Path), "rev-parse", "HEAD^{tree}")
		if e != nil {
			return e
		}
		want = &tree
	}
	if actual != *want {
		return errors.New("materialized index differs from admitted source tree")
	}
	if _, e = b.git(dir, "diff", "--exit-code", "--", ".", ":(exclude)public/dist"); e != nil {
		return errors.New("materialized source has unrecorded modifications")
	}
	extra, e := b.git(dir, "ls-files", "--others", "--exclude-standard")
	if e != nil {
		return e
	}
	if extra != "" {
		return errors.New("materialized source has unrecorded files")
	}
	return nil
}
func (b *builder) run(stage string) error {
	if stage == "oci-inputs" {
		return b.ociInputs()
	}
	var e error
	if strings.TrimSpace(b.buildbarn) == "" {
		return errors.New("select the BuildBarn source materializer module with -buildbarn-root (BUILDBARN_ROOT)")
	}
	for _, p := range []*string{&b.root, &b.output, &b.infra, &b.dataPlane, &b.materializer, &b.buildbarn} {
		*p, e = filepath.Abs(*p)
		if e != nil {
			return e
		}
	}
	if e = validateOutput(b.output, b.root, b.infra, b.dataPlane, b.materializer, b.buildbarn); e != nil {
		return e
	}
	if b.jobs < 1 || b.jobs > 16 {
		return errors.New("jobs must be between 1 and 16")
	}
	if e = os.MkdirAll(b.output, 0755); e != nil {
		return e
	}
	if e = readJSON(filepath.Join(b.root, "build/toolchain.json"), &b.tools); e != nil {
		return e
	}
	if b.release {
		for _, p := range []string{b.root, b.infra, b.dataPlane, b.materializer, b.buildbarn} {
			s, e := b.git(p, "status", "--porcelain", "--untracked-files=all")
			if e != nil {
				return e
			}
			if s != "" {
				return fmt.Errorf("release source must be clean: %s", p)
			}
		}
	}
	switch stage {
	case "source":
		admitted, e := b.sources()
		if e != nil {
			return e
		}
		for _, r := range admitted {
			name := filepath.Base(r.Path)
			if _, e = os.Stat(b.sourcePath(name)); e == nil {
				if e = b.verify(r); e != nil {
					return e
				}
				if e = b.verifyDerived(r); e != nil {
					return e
				}
				tree, e := b.git(b.sourcePath(name), "write-tree")
				if e != nil {
					return e
				}
				if e = b.recordSource(r, tree, "existing-verified"); e != nil {
					return e
				}
			} else if e = b.materialize(r, b.sourcePath(name)); e != nil {
				return e
			}
		}
		if e = b.materializeRcloneModule(); e != nil {
			return e
		}
		return b.workspace()
	case "prepare":
		return b.prepareNative()
	case "remote-inputs":
		return b.remoteInputs()
	case "verify-inputs":
		_, e := b.admitRemoteSnapshot()
		return e
	case "module-source":
		return b.materializeRcloneModule()
	case "ui":
		return b.buildUI()
	case "cp", "plugin", "build":
		return b.buildNative()
	case "images":
		return errors.New("image assembly is separate from native compilation; use inputs and the documented OCI recipe")
	case "collect":
		return b.buildNative()
	case "test":
		return b.compile(stage)
	case "inputs":
		return b.imageInputs()
	case "fingerprint":
		digest, e := b.provenanceDigest()
		if e != nil {
			return e
		}
		fmt.Println("sha256:" + digest)
		return nil
	default:
		return fmt.Errorf("unknown stage %q", stage)
	}
}

type moduleJSON struct {
	Module  struct{ Path string }
	Require []struct {
		Path    string
		Version string
	}
}

func (b *builder) workspace() error {
	if _, err := officialHuma(b.infra); err != nil {
		return err
	}
	// Discover module identities from go.mod through Go itself. Only required
	// first-party modules join the workspace, never adjacent legacy forks.
	available := map[string]string{}
	err := filepath.WalkDir(filepath.Join(b.infra, "go"), func(path string, d fs.DirEntry, e error) error {
		if e != nil {
			return e
		}
		if d.IsDir() && (d.Name() == "testdata" || d.Name() == ".git") {
			return filepath.SkipDir
		}
		if d.Name() != "go.mod" {
			return nil
		}
		raw, e := b.goCommand(filepath.Dir(path), nil, "mod", "edit", "-json")
		if e != nil {
			return e
		}
		var m moduleJSON
		if e = json.Unmarshal([]byte(raw), &m); e != nil {
			return e
		}
		available[m.Module.Path] = filepath.Dir(path)
		return nil
	})
	if err != nil {
		return err
	}
	// A derived module is an admitted source, not a global module-cache edit.
	admitted, e := b.sources()
	if e != nil {
		return e
	}
	for _, r := range admitted {
		dir := b.sourcePath(filepath.Base(r.Path))
		if _, e = os.Stat(filepath.Join(dir, "go.mod")); os.IsNotExist(e) {
			continue
		} else if e != nil {
			return e
		}
		raw, e := b.goCommand(dir, nil, "mod", "edit", "-json")
		if e != nil {
			return e
		}
		var m moduleJSON
		if e = json.Unmarshal([]byte(raw), &m); e != nil {
			return e
		}
		available[m.Module.Path] = dir
	}
	rcloneVersion, e := b.rcloneVersion()
	if e != nil {
		return e
	}
	series, e := b.rcloneSeries()
	if e != nil {
		return e
	}
	if _, e = b.verifyModuleSnapshot(rcloneVersion, series); e != nil {
		return e
	}
	available[rcloneModule] = b.rcloneModulePath()
	workerDir := filepath.Join(b.root, "go")
	raw, e := b.goCommand(workerDir, nil, "mod", "edit", "-json")
	if e != nil {
		return e
	}
	var workerModule moduleJSON
	if e = json.Unmarshal([]byte(raw), &workerModule); e != nil {
		return e
	}
	if workerModule.Module.Path != "github.com/wignerStan/remote-storage-gateway/go" {
		return errors.New("product source must be the Gateway module")
	}
	coreDir, e := b.dataPlaneModule()
	if e != nil {
		return e
	}
	available["github.com/wignerStan/network-data-plane-worker/go"] = coreDir
	materializerDir, e := b.externalModule(b.materializer, "github.com/wignerStan/rclone/go")
	if e != nil {
		return e
	}
	available["github.com/wignerStan/rclone/go"] = materializerDir
	buildbarnDir, e := b.externalModule(b.buildbarn, "github.com/wignerStan/buildbarn-storage/go")
	if e != nil {
		return e
	}
	available["github.com/wignerStan/buildbarn-storage/go"] = buildbarnDir
	available[workerModule.Module.Path] = workerDir
	queue := []string{filepath.Join(b.root, "go"), b.sourcePath("openlist")}
	used := map[string]bool{}
	var dirs []string
	var replacements []string
	for len(queue) > 0 {
		dir := queue[0]
		queue = queue[1:]
		if used[dir] {
			continue
		}
		used[dir] = true
		dirs = append(dirs, dir)
		raw, e := b.goCommand(dir, nil, "mod", "edit", "-json")
		if e != nil {
			return e
		}
		var m moduleJSON
		if e = json.Unmarshal([]byte(raw), &m); e != nil {
			return e
		}
		for _, r := range m.Require {
			if r.Path == rcloneModule && semver.Compare(r.Version, rcloneVersion) > 0 {
				return fmt.Errorf("module %s requires rclone %s above selected Infra version %s", m.Module.Path, r.Version, rcloneVersion)
			}
			if p, ok := available[r.Path]; ok {
				queue = append(queue, p)
				replacements = append(replacements, "-replace="+r.Path+"@"+r.Version+"="+p)
			}
		}
	}
	sort.Strings(dirs)
	work := filepath.Join(b.output, "go.work")
	if e = os.WriteFile(work, []byte("go "+b.tools.Go+"\n"), 0644); e != nil {
		return e
	}
	args := []string{"work", "edit"}
	for _, dir := range dirs {
		args = append(args, "-use="+dir)
	}
	args = append(args, replacements...)
	args = append(args, work)
	if _, e := b.goCommand(b.output, nil, args...); e != nil {
		return e
	}
	return writeJSON(filepath.Join(b.output, "receipts/workspace.json"), map[string]any{"modules": dirs, "infra_root": b.infra, "gateway_module": filepath.Join(b.root, "go"), "data_plane_module": coreDir, "rclone_module": materializerDir, "buildbarn_module": buildbarnDir})
}

// externalModule admits an explicitly selected independent checkout by module
// identity and required package version. Adjacency is never source authority.
func (b *builder) externalModule(root, module string) (string, error) {
	productPath := filepath.Join(b.root, "go/go.mod")
	body, err := os.ReadFile(productPath)
	if err != nil {
		return "", err
	}
	product, err := modfile.Parse(productPath, body, nil)
	if err != nil {
		return "", err
	}
	version := ""
	for _, requirement := range product.Require {
		if requirement.Mod.Path == module {
			version = requirement.Mod.Version
		}
	}
	if !semver.IsValid(version) || version == "v0.0.0" {
		return "", fmt.Errorf("Gateway must require a package version for %s", module)
	}
	dir := filepath.Join(root, "go")
	relative, err := filepath.Rel(b.root, dir)
	if err != nil || relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return "", errors.New("module must be an independent source repository")
	}
	selected := filepath.Join(dir, "go.mod")
	body, err = os.ReadFile(selected)
	if err != nil {
		return "", err
	}
	parsed, err := modfile.Parse(selected, body, nil)
	if err != nil {
		return "", err
	}
	if parsed.Module == nil || parsed.Module.Mod.Path != module {
		return "", errors.New("wrong independent module identity")
	}
	return dir, nil
}
func (b *builder) dataPlaneModule() (string, error) {
	return b.externalModule(b.dataPlane, "github.com/wignerStan/network-data-plane-worker/go")
}

func copyTree(src, dst string) error {
	return filepath.WalkDir(src, func(path string, d fs.DirEntry, e error) error {
		if e != nil {
			return e
		}
		rel, e := filepath.Rel(src, path)
		if e != nil {
			return e
		}
		target := filepath.Join(dst, rel)
		if d.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("generated UI contains symlink: %s", rel)
		}
		if d.IsDir() {
			return os.MkdirAll(target, 0755)
		}
		if !d.Type().IsRegular() {
			return fmt.Errorf("generated UI contains special file: %s", rel)
		}
		in, e := os.Open(path)
		if e != nil {
			return e
		}
		defer in.Close()
		out, e := os.OpenFile(target, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
		if e != nil {
			return e
		}
		_, e = io.Copy(out, in)
		closeErr := out.Close()
		if e != nil {
			return e
		}
		return closeErr
	})
}
func treeDigest(root string, exclude ...string) (string, error) {
	var paths []string
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, e error) error {
		if e != nil {
			return e
		}
		if d.IsDir() {
			rel, _ := filepath.Rel(root, p)
			for _, skip := range exclude {
				if rel == skip {
					return filepath.SkipDir
				}
			}
		}
		if !d.IsDir() {
			if !d.Type().IsRegular() {
				return errors.New("nonregular generated asset")
			}
			paths = append(paths, p)
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	sort.Strings(paths)
	h := sha256.New()
	for _, p := range paths {
		rel, _ := filepath.Rel(root, p)
		digest, e := hashFile(p)
		if e != nil {
			return "", e
		}
		fmt.Fprintf(h, "%s\x00%s\n", filepath.ToSlash(rel), digest)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
func (b *builder) compile(stage string) error {
	if stage != "test" {
		return fmt.Errorf("unknown native compile stage %q", stage)
	}
	r, err := b.load("openlist")
	if err != nil {
		return err
	}
	if err = b.verify(r); err != nil {
		return err
	}
	if err = b.verifyDerived(r); err != nil {
		return err
	}
	text, err := b.goCommand(b.sourcePath("openlist"), b.nativeEnvironment(), "test", "-p="+fmt.Sprint(b.parallelism()), "-race", "./clientplugin", "./pluginentry", "./pkg/gowebdav", "./pkg/storage/s3session", "./internal/controlplane/...", "./server")
	if text != "" {
		fmt.Println(text)
	}
	return err
}

func (b *builder) provenanceDigest() (string, error) {
	h := sha256.New()
	// Both native artifacts depend on the product's admitted sources and build
	// recipe. A backend patch alone must invalidate the advertised build ID.
	for _, name := range []string{"go", ".git-source-relationships", "patches", "build"} {
		exclude := []string(nil)
		if name == "build" {
			exclude = []string{"openlist", "openlist-ui"}
		}
		digest, e := treeDigest(filepath.Join(b.root, name), exclude...)
		if e != nil {
			return "", e
		}
		fmt.Fprintf(h, "%s %s\n", name, digest)
	}
	for _, name := range []string{"Makefile"} {
		recipe, e := hashFile(filepath.Join(b.root, name))
		if e != nil {
			return "", e
		}
		fmt.Fprintf(h, "%s %s\n", name, recipe)
	}
	infra, e := b.git(b.infra, "rev-parse", "HEAD")
	if e != nil {
		return "", e
	}
	fmt.Fprintf(h, "infra %s\n", infra)
	huma, e := officialHuma(b.infra)
	if e != nil {
		return "", e
	}
	fmt.Fprintf(h, "official-huma %s\n", huma)
	// Infra owns the filesystem adapter and its patched upstream. Bind actual
	// selected inputs as well as its commit so dirty development snapshots and
	// backend-only patch updates cannot advertise an unchanged build identity.
	rclone, e := b.rcloneSeries()
	if e != nil {
		return "", e
	}
	version, e := b.rcloneVersion()
	if e != nil {
		return "", e
	}
	fmt.Fprintf(h, "infra/module %s@%s\n", rcloneModule, version)
	for _, name := range []string{"go/patches/rclone/series.json", "go/go.mod", "go/go.sum"} {
		digest, e := hashFile(filepath.Join(b.infra, name))
		if e != nil {
			return "", e
		}
		fmt.Fprintf(h, "infra/%s %s\n", name, digest)
	}
	storage, e := treeDigest(filepath.Join(b.infra, "go"))
	if e != nil {
		return "", e
	}
	fmt.Fprintf(h, "infra/go %s\n", storage)
	native, e := treeDigest(filepath.Join(b.infra, "c/interop/plugins"))
	if e != nil {
		return "", e
	}
	fmt.Fprintf(h, "infra/c/interop/plugins %s\n", native)
	for _, patch := range rclone.Patches {
		digest, e := hashFile(filepath.Join(b.infra, patch.Path))
		if e != nil {
			return "", e
		}
		fmt.Fprintf(h, "infra/%d/%s %s\n", patch.Order, patch.Path, digest)
	}
	if b.dataPlane != "" {
		dir, e := b.dataPlaneModule()
		if e != nil {
			return "", e
		}
		digest, e := treeDigest(dir)
		if e != nil {
			return "", e
		}
		fmt.Fprintf(h, "data-plane/go %s\n", digest)
	}
	// Manager links both external process bindings even when no materializer
	// executable is bundled. Bind their selected source bytes to its build ID.
	for _, input := range []struct{ root, module, label string }{
		{b.materializer, "github.com/wignerStan/rclone/go", "rclone/go"},
		{b.buildbarn, "github.com/wignerStan/buildbarn-storage/go", "buildbarn/go"},
	} {
		if input.root == "" {
			continue
		}
		dir, e := b.externalModule(input.root, input.module)
		if e != nil {
			return "", e
		}
		digest, e := treeDigest(dir)
		if e != nil {
			return "", e
		}
		fmt.Fprintf(h, "%s %s\n", input.label, digest)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func copyInput(src, dst string) error {
	info, e := os.Lstat(src)
	if e != nil {
		return e
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("image input is not a regular file: %s", src)
	}
	in, e := os.Open(src)
	if e != nil {
		return e
	}
	defer in.Close()
	out, e := os.OpenFile(dst+".tmp", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if e != nil {
		return e
	}
	_, e = io.Copy(out, in)
	closeErr := out.Close()
	if e != nil {
		return e
	}
	if closeErr != nil {
		return closeErr
	}
	if e = os.Chmod(dst+".tmp", info.Mode().Perm()); e != nil {
		return e
	}
	return os.Rename(dst+".tmp", dst)
}
