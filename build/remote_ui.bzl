"""Offline UI bundling from standard rules_js lockfile-derived npm inputs."""

load("@aspect_rules_js//js:providers.bzl", "JsInfo")

def _relative(value):
    return value and not value.startswith("/") and not any([part in ("", ".", "..") for part in value.split("/")]) and not any([char in value for char in ["\t", "\n", "\r"]])

def _ui_impl(ctx):
    package = ctx.file.package_json
    if package.basename != "package.json":
        fail("UI package_json must name package.json")
    source_root = package.dirname + "/"
    entries = {}
    for file in ctx.files.srcs:
        if not file.path.startswith(source_root):
            fail("UI source lies outside its package root: " + file.path)
        relative = file.path[len(source_root):]
        if not _relative(relative) or relative.split("/")[0] in ["node_modules", "dist", ".git"]:
            fail("invalid UI source input: " + relative)
        if relative in entries and entries[relative] != file.path:
            fail("duplicate UI source input: " + relative)
        entries[relative] = file.path
    for required in ["package.json", "pnpm-lock.yaml", "pnpm-workspace.yaml"]:
        if required not in entries:
            fail("missing frozen UI input: " + required)
    manifest = ctx.actions.declare_file(ctx.label.name + "/ui-source-map.tsv")
    ctx.actions.write(manifest, "".join([entries[name] + "\t" + name + "\n" for name in sorted(entries)]))

    dependencies = ctx.attr.npm[JsInfo]
    npm_root = ctx.bin_dir.path + "/" + ctx.attr.npm.label.workspace_root + "/" + ctx.attr.npm.label.package
    output = ctx.actions.declare_file(ctx.label.name + "/ui-dist.tar")
    inputs = depset(
        [manifest, ctx.file.node, ctx.file.pnpm] + ctx.files.srcs + ctx.files.tools,
        transitive = [dependencies.npm_sources, dependencies.transitive_sources],
    )
    ctx.actions.run(
        executable = ctx.executable._action,
        inputs = inputs,
        outputs = [output],
        arguments = [manifest.path, npm_root, ctx.file.node.path, ctx.file.pnpm.path, output.path, ctx.attr.node_version, ctx.attr.pnpm_version],
        env = {"PATH": "/usr/bin:/bin", "LANG": "C.UTF-8", "LC_ALL": "C.UTF-8", "TZ": "UTC"},
        mnemonic = "GatewayUIBundle",
        progress_message = "Building frozen OpenList UI with Node " + ctx.attr.node_version,
        execution_requirements = {"no-local": "1"},
    )
    return [DefaultInfo(files = depset([output]))]

remote_ui = rule(
    implementation = _ui_impl,
    attrs = {
        "srcs": attr.label_list(allow_files = True, mandatory = True),
        "package_json": attr.label(allow_single_file = True, mandatory = True),
        "npm": attr.label(providers = [JsInfo], mandatory = True),
        "node": attr.label(allow_single_file = True, mandatory = True),
        "pnpm": attr.label(allow_single_file = True, mandatory = True),
        "tools": attr.label_list(allow_files = True, mandatory = True),
        "node_version": attr.string(default = "24.11.0"),
        "pnpm_version": attr.string(default = "11.28.9"),
        "_action": attr.label(default = Label("//build:ui-action.sh"), executable = True, cfg = "exec", allow_single_file = True),
    },
)
