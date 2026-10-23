use serde_json::json;
use std::{
    path::{Path, PathBuf},
    process::Command,
};
pub fn artifact(dir: &Path, retire: u64, close: u64, retain: bool) -> PathBuf {
    artifact_with_delay(dir, retire, close, retain, 0)
}
pub fn artifact_with_delay(
    dir: &Path,
    retire: u64,
    close: u64,
    retain: bool,
    delay_ns: u64,
) -> PathBuf {
    artifact_custom(dir, retire, close, retain, delay_ns, 0, false)
}
#[allow(dead_code)]
pub fn artifact_with_admission_delay(
    dir: &Path,
    retire: u64,
    close: u64,
    retain: bool,
    delay_ns: u64,
) -> PathBuf {
    artifact_custom(dir, retire, close, retain, 0, delay_ns, false)
}
#[allow(dead_code)]
pub fn artifact_with_stale_terminal(dir: &Path, retire: u64, close: u64) -> PathBuf {
    artifact_custom(dir, retire, close, true, 0, 0, true)
}
fn artifact_custom(
    dir: &Path,
    retire: u64,
    close: u64,
    retain: bool,
    delay_ns: u64,
    admission_delay_ns: u64,
    drop_stale: bool,
) -> PathBuf {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut source =
        std::fs::read_to_string(root.join("tests/fixtures/message_actions.c")).unwrap();
    let start = source.find("static uint32_t call(").unwrap();
    let end = source.find("static uint32_t next(").unwrap();
    let metadata = serde_json::to_string(&json!({"version":1,"decision":"forward","upstream":"keep", "socket_lifetime":{"retire_after_ms":retire,"close_after_ms":close},"retain_upstream_on_downstream_close":retain})).unwrap();
    let marker = serde_json::to_string(&dir.join("events")).unwrap();
    let drop_stale = u8::from(drop_stale);
    let delay_secs = delay_ns / 1_000_000_000;
    let delay_nanos = delay_ns % 1_000_000_000;
    source.replace_range(start..end, &format!(r#"
static void mark(char kind) {{ FILE *file=fopen({marker}, "ab"); if (!file) abort(); fputc(kind,file); fclose(file); }}
static uint32_t call(uint64_t state, uint64_t handle, InfraBytes method, InfraBytes input, InfraCallMeta meta, InfraOwnedBytes *out) {{
    (void)state; (void)handle; (void)input; (void)meta;
    if (begins(method,"openai.responses.v1/response.create/") || begins(method,"openai.responses.v1/response/"))
    {{
        if (++requests==2) {{struct timespec delay={{{}, {}}};nanosleep(&delay,NULL);}}
        return action({}, "{{\"type\":\"response.create\",\"model\":\"m\",\"input\":[]}}",out);
    }}
    if (begins(method,"openai.responses.v1/transport.detached/")) mark('d');
    if (begins(method,"openai.responses.v1/transport.closed/")) {{
        if (input.len > 20 && memmem(input.ptr,input.len,"\"side\":\"upstream\"",17)) mark('u'); else mark('c');
    }}
    if (begins(method,"openai.responses.v1/response.created/")) {{
        struct timespec delay={{{delay_secs},{delay_nanos}}};nanosleep(&delay,NULL);if ({delay_ns}) mark('m');
    }}
    if (begins(method,"openai.responses.v1/response.completed/")) {{
        if ({drop_stale} && ++completed==2) {{
            if (!memmem(input.ptr,input.len,"\"id\":\"first\"",12)) return INFRA_FAILED;
            mark('s');return action("{{\"version\":1,\"decision\":\"drop\",\"upstream\":\"keep\"}}","",out);
        }}
        struct timespec delay={{{delay_secs},{delay_nanos}}};nanosleep(&delay,NULL);mark('t');
    }}
    const uint8_t allow=0; return copy(&allow,1,out);
}}
"#,admission_delay_ns/1_000_000_000,admission_delay_ns%1_000_000_000,serde_json::to_string(&metadata).unwrap()));
    if admission_delay_ns > 0 {
        // The remote Go production path supports concurrent callbacks. Use a
        // Function only for that fixture; local ABI Objects stay single-flight.
        source = source.replace(
            "INFRA_PLUGIN_OBJECT, INFRA_LANGUAGE_RUST",
            "INFRA_PLUGIN_FUNCTION, INFRA_LANGUAGE_RUST",
        );
    }
    source = format!("#define _GNU_SOURCE\n#include <stdio.h>\n{source}");
    let input = dir.join("lifetime.c");
    let path = dir.join(if admission_delay_ns > 0 {
        "lifetime-function.so"
    } else {
        "lifetime.so"
    });
    std::fs::write(&input, source).unwrap();
    assert!(
        Command::new("cc")
            .args(["-shared", "-fPIC", "-std=c11", "-I"])
            .arg(root.join("../infra/rust/crates/interop/plugin-abi/include"))
            .arg(input)
            .arg("-o")
            .arg(&path)
            .status()
            .unwrap()
            .success()
    );
    path
}
