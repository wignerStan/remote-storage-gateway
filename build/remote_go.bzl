"""One offline Go release action, with outputs in the shared REAPI CAS."""

_OUTPUTS = {
    "control_plane": "%{name}/openlist-control-plane",
    "worker": "%{name}/storage-worker",
    "openlist_plugin": "%{name}/client-openlist.so",
    "rclone_plugin": "%{name}/client-rclone.so",
    "transfer_plugin": "%{name}/transfer-reapi.so",
    "admin": "%{name}/gateway-admin",
    "rclone": "%{name}/rclone",
}

def _release_impl(ctx):
    outputs = [getattr(ctx.outputs, name) for name in _OUTPUTS]
    args = ctx.actions.args()
    args.add(ctx.file.source_map)
    args.add(ctx.file.build_env)
    args.add(ctx.file.ui)
    args.add(outputs[0].dirname)
    args.add(ctx.file.materializer)
    ctx.actions.run(
        executable = ctx.executable.runner,
        arguments = [args],
        inputs = depset(
            direct = [ctx.file.source_map, ctx.file.build_env, ctx.file.ui, ctx.file.materializer],
            transitive = [target[DefaultInfo].files for target in ctx.attr.inputs],
        ),
        outputs = outputs,
        env = {"PATH": "/usr/bin:/bin"},
        use_default_shell_env = False,
        mnemonic = "GatewayGoRelease",
        progress_message = "Compiling six Gateway release artifacts on Buildbarn",
    )
    return [DefaultInfo(files = depset(outputs))]

remote_go_release = rule(
    implementation = _release_impl,
    attrs = {
        "inputs": attr.label_list(allow_files = True, mandatory = True),
        "source_map": attr.label(allow_single_file = True, mandatory = True),
        "build_env": attr.label(allow_single_file = True, mandatory = True),
        "ui": attr.label(allow_single_file = True, mandatory = True),
        "materializer": attr.label(allow_single_file = True, mandatory = True),
        "runner": attr.label(
            default = Label("//build:remote-go-action.sh"),
            allow_single_file = True,
            executable = True,
            cfg = "exec",
        ),
    },
    outputs = _OUTPUTS,
)
