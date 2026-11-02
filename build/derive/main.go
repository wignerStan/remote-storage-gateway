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
	Schema      string   `json:"schema_version"`
	Kind        string   `json:"kind"`
	Path        string   `json:"path"`
	Owner       string   `json:"owner"`
	Integration string   `json:"integration"`
	Upstream    upstream `json:"upstream"`
	Patches     []patch  `json:"patches"`
	PublicAPI   string   `json:"public_api"`
	ResultTree  *string  `json:"result_tree"`
	Licenses    []string `json:"licenses"`
}
type toolchain struct {
	Go   string `json:"go"`
	Node string `json:"node"`
	PNPM string `json:"pnpm"`
}
type builder struct {
	root, output, infra, dataPlane, materializer, mise string
	ca                                                 string
	release                                            bool
	tools                                              toolchain
	downloadModule                                     func(string, string) (moduleDownload, error)
}

const rcloneModule = "github.com/rclone/rclone"

type modulePatchSeries struct {
	Schema  string  `json:"schema"`
	Module  string  `json:"module"`
	Patches []patch `json:"patches"`
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
	if e := readJSON(filepath.Join(b.infra, "patches/rclone/series.json"), &series); e != nil {
		return series, e
	}
	if series.Schema != "go-module-patch-series/v1" || series.Module != rcloneModule || len(series.Patches) == 0 {
		return series, errors.New("invalid Infra rclone patch inventory")
	}
	for i, p := range series.Patches {
		if p.Order != i+1 || !safeRelative(p.Path) || !strings.HasPrefix(p.Path, "patches/rclone/") || len(p.SHA256) != 64 {
			return series, errors.New("invalid ordered rclone patch")
		}
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
	raw, e := b.command(b.output, b.mise, "exec", "go@"+b.tools.Go, "--", "go", "mod", "download", "-json", rcloneModule+"@"+version)
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
	inventory, e := hashFile(filepath.Join(b.infra, "patches/rclone/series.json"))
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
		inventory, e := hashFile(filepath.Join(b.infra, "patches/rclone/series.json"))
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
	flag.StringVar(&b.root, "root", "..", "canonical monorepo root")
	flag.StringVar(&b.output, "out", "/tmp/trade-remote-gateway-build", "external build directory")
	flag.StringVar(&b.infra, "infra-root", "/develop/infra", "explicit Infra source checkout (reviewed and clean for release)")
	flag.StringVar(&b.dataPlane, "data-plane-root", "/develop/network/data-plane-worker", "explicit independently owned Data Plane module checkout")
	flag.StringVar(&b.materializer, "rclone-root", "/develop/network/rclone", "independently derived rclone application and process module")
	flag.StringVar(&b.mise, "mise", "mise", "mise executable")
	flag.StringVar(&b.ca, "ca-bundle", "/etc/ssl/certs/ca-certificates.crt", "public runtime CA bundle")
	flag.BoolVar(&b.release, "release", false, "require clean exact product and Infra source commits")
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
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	d := json.NewDecoder(io.LimitReader(f, 1<<20))
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
	return processexec.EnvClean(values)
}

func (b *builder) command(dir, program string, args ...string) (string, error) {
	s := processexec.NewSpec(program, args...).WithTimeout(30 * time.Minute).WithEnv(buildEnvironment(map[string]string{
		"GOWORK": "off", "GOCACHE": filepath.Join(b.output, "go-cache"),
		"GIT_OPTIONAL_LOCKS": "0",
		"XDG_CACHE_HOME":     filepath.Join(b.output, "cache"), "npm_config_cache": filepath.Join(b.output, "npm-cache"),
		"HUSKY": "0", "CI": "true",
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
	if r.Schema != "git-source-relationship/v1" || r.Integration != "submodule" || !safeRelative(r.Path) || !strings.HasPrefix(r.Path, "third_party/") {
		return errors.New("invalid source relationship")
	}
	owner := b.root
	source := filepath.Join(owner, r.Path)
	entry, e := b.git(owner, "ls-files", "--stage", "--", r.Path)
	if e != nil {
		return e
	}
	fields := strings.Fields(entry)
	if len(fields) != 4 || fields[0] != "160000" || fields[1] != r.Upstream.Commit || fields[2] != "0" || fields[3] != r.Path {
		return fmt.Errorf("%s parent gitlink differs from admitted upstream", r.Path)
	}
	head, e := b.git(source, "rev-parse", "HEAD")
	if e != nil {
		return e
	}
	if head != r.Upstream.Commit {
		return fmt.Errorf("%s upstream commit mismatch", r.Path)
	}
	remote, e := b.git(source, "remote", "get-url", "origin")
	if e != nil {
		return e
	}
	if remote != r.Upstream.URL {
		return fmt.Errorf("%s upstream URL mismatch", r.Path)
	}
	status, e := b.git(source, "status", "--porcelain", "--untracked-files=all")
	if e != nil {
		return e
	}
	if status != "" {
		return fmt.Errorf("%s must remain pristine", r.Path)
	}
	for i, p := range r.Patches {
		if p.Order != i+1 || !safeRelative(p.Path) || !strings.HasPrefix(p.Path, "patches/") {
			return errors.New("invalid ordered patch path")
		}
		actual, e := hashFile(filepath.Join(owner, p.Path))
		if e != nil {
			return e
		}
		if actual != p.SHA256 {
			return fmt.Errorf("patch hash mismatch: %s", p.Path)
		}
	}
	if r.Kind == "patched_third_party" && (len(r.Patches) == 0 || r.ResultTree == nil) {
		return errors.New("patched source needs patch stack and result tree")
	}
	if r.Kind == "upstream_submodule" && (len(r.Patches) != 0 || r.ResultTree != nil) {
		return errors.New("pristine source cannot declare patches")
	}
	return nil
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
	source := filepath.Join(b.root, r.Path)
	shallow, e := b.git(source, "rev-parse", "--is-shallow-repository")
	if e != nil {
		return e
	}
	cloneArgs := []string{"clone", "--reference", source, "--dissociate", "--no-checkout", source, destination}
	cloneMode := "reference-dissociated"
	if shallow == "true" {
		// Git forbids shallow repositories as reference alternates. A local
		// clone preserves their declared shallow boundary without unshallowing.
		cloneArgs = []string{"clone", "--no-checkout", source, destination}
		cloneMode = "local-shallow"
	}
	if _, e := b.git(b.root, cloneArgs...); e != nil {
		return e
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
	if want == nil {
		t, e := b.git(source, "rev-parse", "HEAD^{tree}")
		if e != nil {
			return e
		}
		want = &t
	}
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
	return writeJSON(filepath.Join(b.output, "receipts", filepath.Base(r.Path)+"-source.json"), struct {
		Relationship     relationship `json:"relationship"`
		Tree             string       `json:"tree"`
		CloneMode        string       `json:"clone_mode"`
		SourceRepository string       `json:"source_repository"`
		SourceCommit     string       `json:"source_commit"`
		SourceDirty      bool         `json:"source_dirty"`
	}{r, tree, cloneMode, owner, commit, status != ""})
}
func (b *builder) sourcePath(name string) string { return filepath.Join(b.output, "source", name) }
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
	if r.Path != "third_party/"+name {
		return r, fmt.Errorf("unexpected canonical path for %s", name)
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
	var e error
	for _, p := range []*string{&b.root, &b.output, &b.infra, &b.dataPlane, &b.materializer} {
		*p, e = filepath.Abs(*p)
		if e != nil {
			return e
		}
	}
	if !strings.HasPrefix(b.output, "/tmp/trade-") && !strings.HasPrefix(b.output, "/var/tmp/trade-") {
		return errors.New("build output must be /tmp/trade-* or /var/tmp/trade-*")
	}
	if e = readJSON(filepath.Join(b.root, "build/toolchain.json"), &b.tools); e != nil {
		return e
	}
	if b.release {
		for _, p := range []string{b.root, b.infra, b.dataPlane, b.materializer} {
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
	case "module-source":
		return b.materializeRcloneModule()
	case "ui":
		return b.ui()
	case "cp", "plugin", "test":
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

// sdkHuma admits the SDK owner's tracked module. A neighboring checkout or a
// dependency's ignored replace directive cannot silently select schema code.
func sdkHuma(root string) (string, error) {
	var record relationship
	if err := readJSON(filepath.Join(root, ".git-source-relationships/huma.json"), &record); err != nil {
		return "", err
	}
	if record.Schema != "git-source-relationship/v1" || record.Kind != "third_party_implementation" || record.Integration != "tracked" || record.Path != "third_party/huma" || record.Owner == "" {
		return "", errors.New("SDK Huma requires its tracked source relationship")
	}
	directory := filepath.Join(root, record.Path)
	actual, err := filepath.EvalSymlinks(directory)
	if err != nil || actual != filepath.Clean(directory) {
		return "", errors.New("SDK Huma source must be the declared owned directory")
	}
	parse := func(path string) (*modfile.File, error) {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		return modfile.Parse(path, data, nil)
	}
	owner, err := parse(filepath.Join(root, "go/go.mod"))
	if err != nil {
		return "", err
	}
	const module = "github.com/danielgtaylor/huma/v2"
	declared := false
	for _, replacement := range owner.Replace {
		if replacement.Old.Path == module && replacement.New.Version == "" && filepath.Clean(filepath.Join(root, "go", replacement.New.Path)) == directory {
			declared = true
		}
	}
	if !declared {
		return "", errors.New("SDK Huma source is not declared by the owner go.mod")
	}
	selected, err := parse(filepath.Join(directory, "go.mod"))
	if err != nil {
		return "", err
	}
	if selected.Module == nil || selected.Module.Mod.Path != module {
		return "", errors.New("SDK Huma module identity mismatch")
	}
	return directory, nil
}

func (b *builder) workspace() error {
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
		raw, e := b.command(filepath.Dir(path), b.mise, "exec", "go@"+b.tools.Go, "--", "go", "mod", "edit", "-json")
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
	huma, e := sdkHuma(b.infra)
	if e != nil {
		return e
	}
	available["github.com/danielgtaylor/huma/v2"] = huma
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
		raw, e := b.command(dir, b.mise, "exec", "go@"+b.tools.Go, "--", "go", "mod", "edit", "-json")
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
	raw, e := b.command(workerDir, b.mise, "exec", "go@"+b.tools.Go, "--", "go", "mod", "edit", "-json")
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
		raw, e := b.command(dir, b.mise, "exec", "go@"+b.tools.Go, "--", "go", "mod", "edit", "-json")
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
	args := []string{"exec", "go@" + b.tools.Go, "--", "go", "work", "edit"}
	for _, dir := range dirs {
		args = append(args, "-use="+dir)
	}
	args = append(args, replacements...)
	args = append(args, work)
	if _, e := b.command(b.output, b.mise, args...); e != nil {
		return e
	}
	return writeJSON(filepath.Join(b.output, "receipts/workspace.json"), map[string]any{"modules": dirs, "infra_root": b.infra, "gateway_module": filepath.Join(b.root, "go"), "data_plane_module": coreDir, "rclone_module": materializerDir})
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
func treeDigest(root string) (string, error) {
	var paths []string
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, e error) error {
		if e != nil {
			return e
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
func (b *builder) ui() error {
	src := b.sourcePath("openlist-ui")
	r, e := b.load("openlist-ui")
	if e != nil {
		return e
	}
	if e = b.verify(r); e != nil {
		return e
	}
	if e = b.verifyDerived(r); e != nil {
		return e
	}
	var pkg struct {
		Version        string
		PackageManager string `json:"packageManager"`
	}
	data, e := os.ReadFile(filepath.Join(src, "package.json"))
	if e != nil {
		return e
	}
	if e = json.Unmarshal(data, &pkg); e != nil {
		return e
	}
	if pkg.PackageManager != "pnpm@"+b.tools.PNPM {
		return errors.New("pinned UI package manager does not match toolchain")
	}
	base := []string{"exec", "node@" + b.tools.Node, "pnpm@" + b.tools.PNPM, "--", "pnpm"}
	args := append(append([]string{}, base...), "install", "--frozen-lockfile", "--store-dir", filepath.Join(b.output, "pnpm-store"))
	if _, e = b.command(src, b.mise, args...); e != nil {
		return e
	}
	args = append(append([]string{}, base...), "run", "build")
	if _, e = b.command(src, b.mise, args...); e != nil {
		return e
	}
	// public/dist in the disposable derived source is the existing go:embed input.
	dst := filepath.Join(b.sourcePath("openlist"), "public/dist")
	if _, e = os.Stat(filepath.Join(src, "dist/index.html")); e != nil {
		return fmt.Errorf("UI build produced no index.html: %w", e)
	}
	if e = os.RemoveAll(dst); e != nil {
		return e
	}
	if e = copyTree(filepath.Join(src, "dist"), dst); e != nil {
		return e
	}
	digest, e := treeDigest(dst)
	if e != nil {
		return e
	}
	lock, e := hashFile(filepath.Join(src, "pnpm-lock.yaml"))
	if e != nil {
		return e
	}
	return writeJSON(filepath.Join(b.output, "receipts/ui.json"), map[string]any{"upstream": r.Upstream, "version": pkg.Version, "pnpm_lock_sha256": lock, "dist_sha256": digest, "toolchain": b.tools, "embedded_input": dst})
}
func (b *builder) compile(stage string) error {
	src := b.sourcePath("openlist")
	if _, e := os.Stat(filepath.Join(b.output, "receipts/openlist-source.json")); e != nil {
		return e
	}
	work := filepath.Join(b.output, "go.work")
	if _, e := os.Stat(work); e != nil {
		return e
	}
	if e := os.MkdirAll(filepath.Join(b.output, "bin"), 0755); e != nil {
		return e
	}
	args := []string{"exec", "go@" + b.tools.Go, "--", "go"}
	var artifact string
	var buildFingerprint string
	r, e := b.load("openlist")
	if e != nil {
		return e
	}
	if e = b.verify(r); e != nil {
		return e
	}
	if e = b.verifyDerived(r); e != nil {
		return e
	}
	if stage == "test" {
		args = append(args, "test", "-race", "./clientplugin", "./pluginentry", "./pkg/gowebdav", "./pkg/storage/s3session", "./internal/controlplane/...", "./server")
	} else if stage == "plugin" {
		artifact = "client-openlist.so"
		provenance, e := b.provenanceDigest()
		if e != nil {
			return e
		}
		buildFingerprint = "sha256:" + provenance
		args = append(args, "build", "-trimpath", "-buildvcs=false", "-buildmode=c-shared", "-ldflags", "-X main.buildDigest=sha256:"+provenance, "-o", filepath.Join(b.output, "bin", artifact), "./cmd/openlist-client-plugin")
	} else {
		var ui struct {
			DistSHA string `json:"dist_sha256"`
			Version string `json:"version"`
		}
		data, e := os.ReadFile(filepath.Join(b.output, "receipts/ui.json"))
		if e != nil {
			return errors.New("build UI source before compiling embedded CP")
		}
		if e = json.Unmarshal(data, &ui); e != nil {
			return e
		}
		actual, e := treeDigest(filepath.Join(src, "public/dist"))
		if e != nil {
			return e
		}
		if actual != ui.DistSHA {
			return errors.New("embedded UI differs from reviewed build receipt")
		}
		artifact = "openlist-control-plane"
		productID, e := b.git(b.root, "rev-parse", "HEAD")
		if e != nil {
			return e
		}
		dirty, e := b.git(b.root, "status", "--porcelain")
		if e != nil {
			return e
		}
		if dirty != "" {
			productID += "-dirty"
		}
		flags := "-X github.com/OpenListTeam/OpenList/v4/internal/conf.WebVersion=" + ui.Version + " -X github.com/OpenListTeam/OpenList/v4/internal/conf.GitCommit=" + productID
		args = append(args, "build", "-trimpath", "-buildvcs=false", "-ldflags", flags, "-o", filepath.Join(b.output, "bin", artifact), ".")
	}
	env := map[string]string{"GOWORK": work, "GOCACHE": filepath.Join(b.output, "go-cache"), "CGO_CFLAGS": "-I" + filepath.Join(b.infra, "rust/crates/interop/native-plugin-protocol/include") + " -I" + filepath.Join(b.infra, "c/plugin-interop/include")}
	if stage == "cp" {
		env["CGO_ENABLED"] = "0"
	} else {
		env["CGO_ENABLED"] = "1"
	}
	s := processexec.NewSpec(b.mise, args...).WithTimeout(30 * time.Minute).WithEnv(buildEnvironment(env))
	s.Dir = src
	s.MaxStdout = 8 << 20
	s.MaxStderr = 8 << 20
	o, e := processexec.RunContext(context.Background(), s)
	if e != nil {
		return fmt.Errorf("%s failed: %w\n%s%s", stage, e, o.Stdout, o.Stderr)
	}
	fmt.Print(string(o.Stdout))
	if stage == "test" {
		return nil
	}
	hash, e := hashFile(filepath.Join(b.output, "bin", artifact))
	if e != nil {
		return e
	}
	product, e := b.git(b.root, "rev-parse", "HEAD")
	if e != nil {
		return e
	}
	infra, e := b.git(b.infra, "rev-parse", "HEAD")
	if e != nil {
		return e
	}
	productDirty, e := b.git(b.root, "status", "--porcelain")
	if e != nil {
		return e
	}
	infraDirty, e := b.git(b.infra, "status", "--porcelain")
	if e != nil {
		return e
	}
	return writeJSON(filepath.Join(b.output, "receipts", artifact+".json"), map[string]any{"artifact": artifact, "sha256": hash, "derived_tree": r.ResultTree, "patch_digest": patchDigest(r), "build_digest": buildFingerprint, "product_commit": product, "product_dirty": productDirty != "", "infra_commit": infra, "infra_dirty": infraDirty != "", "toolchain": b.tools})
}
func patchDigest(r relationship) string {
	h := sha256.New()
	fmt.Fprintln(h, r.Upstream.Commit)
	for _, p := range r.Patches {
		fmt.Fprintf(h, "%d %s %s\n", p.Order, p.Path, p.SHA256)
	}
	return hex.EncodeToString(h.Sum(nil))
}

func (b *builder) provenanceDigest() (string, error) {
	h := sha256.New()
	// Both native artifacts depend on the product's admitted sources and build
	// recipe. A backend patch alone must invalidate the advertised build ID.
	for _, name := range []string{"go", ".git-source-relationships", "patches", "build"} {
		digest, e := treeDigest(filepath.Join(b.root, name))
		if e != nil {
			return "", e
		}
		fmt.Fprintf(h, "%s %s\n", name, digest)
	}
	recipe, e := hashFile(filepath.Join(b.root, "Makefile"))
	if e != nil {
		return "", e
	}
	infra, e := b.git(b.infra, "rev-parse", "HEAD")
	if e != nil {
		return "", e
	}
	fmt.Fprintf(h, "Makefile %s\ninfra %s\n", recipe, infra)
	huma, e := sdkHuma(b.infra)
	if e != nil {
		return "", e
	}
	humaHash, e := treeDigest(huma)
	if e != nil {
		return "", e
	}
	relationshipHash, e := hashFile(filepath.Join(b.infra, ".git-source-relationships/huma.json"))
	if e != nil {
		return "", e
	}
	fmt.Fprintf(h, "infra/huma %s %s\n", humaHash, relationshipHash)
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
	for _, name := range []string{"patches/rclone/series.json", "go/go.mod", "go/go.sum"} {
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
	return os.Rename(dst+".tmp", dst)
}

func (b *builder) imageInputs() error {
	compositions := map[string][]string{"gateway": {"storage-worker", "client-openlist.so", "client-rclone.so", "transfer-reapi.so", "ca-certificates.crt"}, "control-plane": {"openlist-control-plane", "storage-worker", "rclone", "client-openlist.so", "client-rclone.so", "transfer-reapi.so", "ca-certificates.crt"}}
	for _, composition := range []string{"gateway", "control-plane"} {
		dir := filepath.Join(b.output, "image-inputs", composition)
		if e := os.MkdirAll(dir, 0755); e != nil {
			return e
		}
		manifest := map[string]string{}
		for _, name := range compositions[composition] {
			src := filepath.Join(b.output, "bin", name)
			if name == "ca-certificates.crt" {
				src = b.ca
			}
			dst := filepath.Join(dir, name)
			if e := copyInput(src, dst); e != nil {
				return e
			}
			hash, e := hashFile(dst)
			if e != nil {
				return e
			}
			manifest[name] = hash
		}
		if e := writeJSON(filepath.Join(dir, "inputs.json"), manifest); e != nil {
			return e
		}
	}
	return nil
}
