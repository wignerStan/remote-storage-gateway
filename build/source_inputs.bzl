"""Declare immutable source/tool references once; Buildbarn owns their CAS blobs."""
def _safe(path):
    return type(path) == "string" and path and not path.startswith("/") and "\\" not in path and all([part not in ["", ".", ".."] for part in path.split("/")]) and "\n" not in path and "\r" not in path and "\t" not in path

def _inputs_impl(ctx):
    location = ctx.getenv("GATEWAY_BUILD_INPUTS")
    if not location or not location.startswith("/"):
        fail("GATEWAY_BUILD_INPUTS must select a prepared source manifest")
    manifest = json.decode(ctx.read(ctx.path(location)))
    if manifest.get("schema") != "storage-gateway/build-inputs/v1":
        fail("unsupported build input manifest")
    seen, payloads, rows = {}, {}, []
    ui, node, pnpm = [], [], []
    for item in manifest["files"]:
        path, source, digest, mode = item["path"], item["source"], item["sha256"], item["mode"]
        if not _safe(path) or path in seen or not source.startswith("/") or len(digest) != 64 or mode not in ["0644", "0755", "0444", "0555"]:
            fail("invalid or duplicate source input: " + path)
        seen[path] = True
        payload = "payload/" + digest
        if payload not in payloads:
            ctx.symlink(ctx.path(source), payload)
            payloads[payload] = digest
        rows.append(path + "\t" + payload + "\t" + mode)
        # Standard npm rules consume the pristine UI paths and lock graph.
        if path.startswith("workspace/openlist-ui/"):
            ctx.symlink(ctx.path(source), path)
            ui.append(path[len("workspace/openlist-ui/"):])
        elif path.startswith("tools/node/") or path.startswith("tools/pnpm/"):
            ctx.symlink(ctx.path(source), path)
            if path.startswith("tools/node/"):
                node.append(path)
            else:
                pnpm.append(path)
        elif path == "runtime/ca-certificates.crt":
            ctx.symlink(ctx.path(source), path)
    ctx.file("source-map.tsv", "\n".join(sorted(rows)) + "\n")
    ctx.file("payload.sha256", "".join([payloads[name] + "  " + name + "\n" for name in sorted(payloads)]))
    config = manifest["config"]
    keys = ["gateway_digest", "rclone_digest", "product_commit", "ui_version", "rclone_version"]
    if sorted(config.keys()) != sorted(keys):
        fail("build parameters must contain the exact release contract")
    for key in keys:
        if type(config[key]) != "string" or not config[key] or "\n" in config[key] or "\r" in config[key]:
            fail("invalid release parameter: " + key)
    ctx.file("build.env", "".join([key + "=" + config[key] + "\n" for key in keys]))
    if "runtime/ca-certificates.crt" not in seen:
        fail("missing public CA bundle")
    exports = sorted(payloads) + ["source-map.tsv", "payload.sha256", "build.env", "runtime/ca-certificates.crt"] + node + pnpm
    ctx.file("BUILD.bazel", "exports_files(" + repr(exports) + ")\nfilegroup(name='files',srcs=" + repr(sorted(payloads) + ["payload.sha256"]) + ",visibility=['//visibility:public'])\nfilegroup(name='node_toolchain',srcs=" + repr(node) + ",visibility=['//visibility:public'])\nfilegroup(name='pnpm_toolchain',srcs=" + repr(pnpm) + ",visibility=['//visibility:public'])\n")
    ctx.file("workspace/openlist-ui/BUILD.bazel", "load('@ui_npm//:defs.bzl','npm_link_all_packages')\nnpm_link_all_packages(name='node_modules')\nexports_files(" + repr(ui) + ")\nfilegroup(name='ui_sources',srcs=" + repr(ui) + ",visibility=['//visibility:public'])\n")

source_inputs = repository_rule(implementation = _inputs_impl, local = True, environ = ["GATEWAY_BUILD_INPUTS"])
