"""Consume the product's completed-owner OCI admission; never rebuild programs."""
_NAMES = ["gateway", "storage-worker", "openlist-control-plane", "gateway-admin", "client-openlist.so", "client-rclone.so", "transfer-reapi.so", "rclone", "ca-certificates.crt"]
def _hash(ctx, source, expected):
    result = ctx.execute(["/usr/bin/sha256sum", "--", str(source)], quiet = True)
    if result.return_code != 0 or result.stdout.split(" ")[0] != expected:
        fail("Admitted OCI input changed: " + str(source))
def _impl(ctx):
    location = ctx.getenv("GATEWAY_OCI_INPUTS")
    if not location or not location.startswith("/"):
        fail("GATEWAY_OCI_INPUTS must name the producer's admitted assembly.json")
    manifest_path = ctx.path(location)
    assembly = json.decode(ctx.read(manifest_path))
    if assembly.get("schema") != "storage-gateway/oci-assembly/v1" or sorted(assembly.get("artifacts", {}).keys()) != sorted(_NAMES):
        fail("OCI assembly requires the exact native owner artifact set and CA")
    for name in _NAMES:
        item = assembly["artifacts"][name]
        if not item["path"].startswith("/") or len(item["sha256"]) != 64 or item["bytes"] <= 0:
            fail("Malformed admitted artifact: " + name)
        source = ctx.path(item["path"])
        _hash(ctx, source, item["sha256"])
        ctx.symlink(source, name)
    for owner, output in [("gateway", "gateway-native-release.json"), ("rclone", "rclone-native-build.json")]:
        receipt = assembly[owner]["receipt"]
        _hash(ctx, ctx.path(receipt["path"]), receipt["sha256"])
        copied = manifest_path.dirname.get_child(output)
        _hash(ctx, copied, receipt["sha256"])
        ctx.symlink(copied, output)
    ctx.symlink(manifest_path, "assembly.json")
    ctx.file("provenance.bzl", "GATEWAY_SOURCE = " + repr(assembly["gateway"]["source"]["commit"]) + "\nRCLONE_SOURCE = " + repr(assembly["rclone"]["source"]["commit"]) + "\n")
    ctx.file("BUILD.bazel", "exports_files(" + repr(_NAMES + ["assembly.json", "gateway-native-release.json", "rclone-native-build.json", "provenance.bzl"]) + ")\n")
native_inputs = repository_rule(implementation = _impl, local = True, environ = ["GATEWAY_OCI_INPUTS"])
