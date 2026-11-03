"""Admit only explicit, digest-checked public runtime programs and trust roots."""
_COMPOSITIONS = {
    "gateway": struct(variable = "GATEWAY_IMAGE_INPUTS", names = ["storage-worker", "client-openlist.so", "client-rclone.so", "transfer-reapi.so", "ca-certificates.crt"]),
    "control-plane": struct(variable = "CONTROL_PLANE_IMAGE_INPUTS", names = ["openlist-control-plane", "storage-worker", "rclone", "client-openlist.so", "client-rclone.so", "transfer-reapi.so", "ca-certificates.crt"]),
}
def _runtime_inputs_impl(ctx):
    composition = _COMPOSITIONS[ctx.attr.composition]
    directory = ctx.getenv(composition.variable)
    if not directory or not directory.startswith("/"):
        fail("Set " + composition.variable + " to an absolute reviewed input directory")
    root = ctx.path(directory)
    manifest = json.decode(ctx.read(root.get_child("inputs.json")))
    if type(manifest) != "dict" or sorted(manifest.keys()) != sorted(composition.names):
        fail("inputs.json must contain exactly the permitted filenames and SHA-256 values")
    for name in composition.names:
        expected = manifest[name]
        if type(expected) != "string" or len(expected) != 64:
            fail("Invalid reviewed SHA-256 for " + name)
        source = root.get_child(name)
        if not source.exists:
            fail("Missing reviewed input: " + str(source))
        result = ctx.execute(["/usr/bin/sha256sum", "--", str(source)], quiet = True)
        if result.return_code != 0 or result.stdout.split(" ")[0] != expected:
            fail("Reviewed input SHA-256 mismatch: " + name)
        ctx.symlink(source, name)
    exports = list(composition.names)
    if ctx.attr.composition == "control-plane":
        ctx.file("_working_directory", "")
        exports.append("_working_directory")
    ctx.file("BUILD.bazel", "exports_files(" + repr(exports) + ")\n")
runtime_inputs = repository_rule(
    implementation = _runtime_inputs_impl,
    local = True,
    environ = ["GATEWAY_IMAGE_INPUTS", "CONTROL_PLANE_IMAGE_INPUTS"],
    attrs = {"composition": attr.string(default = "gateway", values = ["gateway", "control-plane"])},
)
