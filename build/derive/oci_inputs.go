package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"

	buildinputs "github.com/wignerStan/infra/go/code/build/source"
)

// OCI admission composes completed owners. It never changes a native receipt,
// its execution identity, or WITH_RCLONE selection and never compiles a program.
type ociFile struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
	Bytes  int64  `json:"bytes"`
}
type ociSource struct {
	Root   string `json:"root"`
	Commit string `json:"commit"`
	Dirty  string `json:"dirty"`
}
type ociOwner struct {
	Source      ociSource `json:"source"`
	Execution   string    `json:"execution"`
	BuildDigest string    `json:"build_digest"`
	Receipt     ociFile   `json:"receipt"`
	Inputs      ociFile   `json:"input_manifest"`
}
type ociAssembly struct {
	Schema         string             `json:"schema"`
	Packager       ociSource          `json:"packager"`
	Gateway        ociOwner           `json:"gateway"`
	Rclone         ociOwner           `json:"rclone"`
	Artifacts      map[string]ociFile `json:"artifacts"`
	RcloneAPIFiles int                `json:"compatible_rclone_api_files"`
}
type rcloneNativeOwner struct {
	Schema              string            `json:"schema"`
	Binary              string            `json:"binary"`
	Cache               string            `json:"cache"`
	DataPlane           ociSource         `json:"data_plane"`
	Execution           string            `json:"execution"`
	GoVersion           string            `json:"go_version"`
	Infra               ociSource         `json:"infra"`
	InputManifestSHA256 string            `json:"input_manifest_sha256"`
	BuildDigest         string            `json:"native_build_digest"`
	LinkInputs          map[string]string `json:"native_link_inputs"`
	Product             ociSource         `json:"product"`
	SHA256              string            `json:"sha256"`
	Source              json.RawMessage   `json:"source"`
}

func measureOCIFile(name string) (ociFile, error) {
	info, err := os.Lstat(name)
	if err != nil {
		return ociFile{}, err
	}
	if !info.Mode().IsRegular() || info.Size() == 0 {
		return ociFile{}, fmt.Errorf("OCI input must be a nonempty regular file: %s", name)
	}
	digest, err := hashFile(name)
	return ociFile{Path: name, SHA256: digest, Bytes: info.Size()}, err
}
func (b *builder) ociSource(root, expected string) (ociSource, error) {
	commit, err := b.git(root, "rev-parse", "HEAD")
	if err != nil {
		return ociSource{}, err
	}
	dirty, err := b.git(root, "status", "--porcelain", "--untracked-files=all")
	if err != nil {
		return ociSource{}, err
	}
	if dirty != "" || expected != "" && commit != expected {
		return ociSource{}, fmt.Errorf("OCI owner source differs from its completed native build: %s", root)
	}
	return ociSource{Root: root, Commit: commit}, nil
}
func (b *builder) ociSelection(verifySource bool) (ociAssembly, error) {
	var result ociAssembly
	var err error
	result.Schema = "storage-gateway/oci-assembly/v1"
	result.Packager, err = b.ociSource(b.root, "")
	if err != nil {
		return result, err
	}
	var gatewayInputs remoteInputManifest
	gatewayManifest := filepath.Join(b.gatewayBuild, "build-inputs/manifest.json")
	if err = readJSONLimit(gatewayManifest, &gatewayInputs, 128<<20); err != nil {
		return result, err
	}
	if gatewayInputs.Schema != "storage-gateway/build-inputs/v1" || len(gatewayInputs.Files) == 0 || len(gatewayInputs.Config["product_commit"]) != 40 || gatewayInputs.Config["gateway_digest"] == "" || gatewayInputs.Config["rclone_version"] == "" {
		return result, errors.New("invalid Gateway owner source manifest")
	}
	// Reuse the native release contract against its original manifest, not the
	// newer packaging checkout's fingerprint. Original producer fields stay intact.
	original := *b
	original.output = b.gatewayBuild
	original.withRclone = false
	original.tools.Go = "1.27.3"
	native, err := original.admitNativeRelease(gatewayInputs)
	if err != nil {
		return result, err
	}
	result.Gateway.Source, err = b.ociSource(b.gatewaySource, gatewayInputs.Config["product_commit"])
	if err != nil {
		return result, err
	}
	result.Gateway.Execution = native.Execution
	result.Gateway.BuildDigest = gatewayInputs.Config["gateway_digest"]
	result.Gateway.Receipt, err = measureOCIFile(filepath.Join(b.gatewayBuild, "receipts/native-release.json"))
	if err != nil {
		return result, err
	}
	result.Gateway.Inputs, err = measureOCIFile(gatewayManifest)
	if err != nil {
		return result, err
	}
	result.Artifacts = map[string]ociFile{}
	for name := range native.Artifacts {
		result.Artifacts[name], err = measureOCIFile(filepath.Join(b.gatewayBuild, "bin", name))
		if err != nil {
			return result, err
		}
	}
	var owner rcloneNativeOwner
	ownerPath := filepath.Join(b.rcloneBuild, "receipts/rclone.json")
	if err = readJSON(ownerPath, &owner); err != nil {
		return result, err
	}
	var action struct {
		Schema      string `json:"schema"`
		SHA256      string `json:"binary_sha256"`
		BuildDigest string `json:"native_build_digest"`
		GoVersion   string `json:"go_version"`
		Version     string `json:"rclone_version"`
	}
	if err = readJSON(filepath.Join(b.rcloneBuild, "receipts/rclone.build.json"), &action); err != nil {
		return result, err
	}
	var materializerInputs remoteInputManifest
	materializerManifest := filepath.Join(b.rcloneBuild, "build-inputs/manifest.json")
	if err = readJSONLimit(materializerManifest, &materializerInputs, 128<<20); err != nil {
		return result, err
	}
	result.Rclone.Inputs, err = measureOCIFile(materializerManifest)
	if err != nil {
		return result, err
	}
	cli, err := measureOCIFile(filepath.Join(b.rcloneBuild, "bin/rclone"))
	if err != nil {
		return result, err
	}
	if owner.Schema != "rclone-native-build/v1" || owner.Execution != "local-go" || owner.GoVersion != "1.27.3" || owner.Product.Dirty != "" || owner.Binary != cli.Path || owner.SHA256 != cli.SHA256 || owner.InputManifestSHA256 != result.Rclone.Inputs.SHA256 || materializerInputs.Schema != "rclone/build-inputs/v1" || len(materializerInputs.Files) == 0 || owner.Product.Commit != materializerInputs.Config["product_commit"] || owner.BuildDigest != materializerInputs.Config["rclone_digest"] || action.Schema != "rclone/native-build/v1" || action.SHA256 != cli.SHA256 || action.BuildDigest != owner.BuildDigest || action.GoVersion != owner.GoVersion || action.Version != gatewayInputs.Config["rclone_version"] || action.Version != materializerInputs.Config["rclone_version"] {
		return result, errors.New("independent rclone owner, action, source manifest or ELF differ")
	}
	var moduleSource map[string]any
	if err = json.Unmarshal(owner.Source, &moduleSource); err != nil {
		return result, err
	}
	if moduleSource["schema"] != "go-module-product-source/v1" || moduleSource["module"] != rcloneModule || moduleSource["version"] != action.Version {
		return result, errors.New("rclone package version differs from its native source receipt")
	}
	result.Rclone.Source, err = b.ociSource(b.materializer, owner.Product.Commit)
	if err != nil {
		return result, err
	}
	if filepath.Clean(owner.Product.Root) != b.materializer {
		return result, errors.New("rclone receipt names a different source checkout")
	}
	result.Rclone.Execution = owner.Execution
	result.Rclone.BuildDigest = owner.BuildDigest
	result.Rclone.Receipt, err = measureOCIFile(ownerPath)
	if err != nil {
		return result, err
	}
	result.Artifacts["rclone"] = cli
	// A separately updated CLI may retain the same imported Box/config API. Check
	// every captured API file against that owner's actual source; no SHA bridge or
	// receipt rewrite makes a changed API compatible with an already-linked ELF.
	var api []buildinputs.File
	for _, file := range gatewayInputs.Files {
		if strings.HasPrefix(file.Path, "workspace/rclone/go/") {
			file.Source = filepath.Join(b.materializer, "go", strings.TrimPrefix(file.Path, "workspace/rclone/go/"))
			api = append(api, file)
		}
	}
	if len(api) == 0 {
		return result, errors.New("Gateway build has no independent rclone API evidence")
	}
	if err = buildinputs.Verify(context.Background(), api); err != nil {
		return result, fmt.Errorf("rclone API differs from the linked Gateway: %w", err)
	}
	result.RcloneAPIFiles = len(api)
	if verifySource {
		if err = buildinputs.Verify(context.Background(), gatewayInputs.Files); err != nil {
			return result, err
		}
		if err = buildinputs.Verify(context.Background(), materializerInputs.Files); err != nil {
			return result, err
		}
	}
	result.Artifacts["ca-certificates.crt"], err = measureOCIFile(b.ca)
	return result, err
}
func (b *builder) ociInputs() error {
	for _, value := range []*string{&b.root, &b.output, &b.gatewayBuild, &b.gatewaySource, &b.rcloneBuild, &b.materializer, &b.ca} {
		if strings.TrimSpace(*value) == "" {
			return errors.New("OCI admission requires explicit completed Gateway/source and independent rclone builds")
		}
		absolute, err := filepath.Abs(*value)
		if err != nil {
			return err
		}
		*value = absolute
	}
	if err := validateOutput(b.output, b.root, b.gatewayBuild, b.gatewaySource, b.rcloneBuild, b.materializer); err != nil {
		return err
	}
	if err := os.MkdirAll(b.output, 0755); err != nil {
		return err
	}
	selection, err := b.ociSelection(true)
	if err != nil {
		return err
	}
	stage, err := os.MkdirTemp(b.output, "oci-inputs-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(stage)
	if err = writeJSON(filepath.Join(stage, "assembly.json"), selection); err != nil {
		return err
	}
	// The files remain with their owners. Publish only references and complete
	// owner receipts; Bazel uploads those bytes once to the shared CAS.
	for name, source := range map[string]string{"gateway-native-release.json": selection.Gateway.Receipt.Path, "rclone-native-build.json": selection.Rclone.Receipt.Path} {
		if err = copyInput(source, filepath.Join(stage, name)); err != nil {
			return err
		}
	}
	final, err := b.ociSelection(false)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(selection, final) {
		return errors.New("native owners changed during OCI input admission")
	}
	return replaceDirectory(stage, filepath.Join(b.output, "oci-inputs"))
}
