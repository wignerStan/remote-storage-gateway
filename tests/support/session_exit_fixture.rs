use std::{
    path::{Path, PathBuf},
    process::Command,
};
// Shared by independent process and native-carrier test harnesses.
#[allow(dead_code)]
#[derive(Clone, Copy)]
pub enum Fault {
    None,
    Cleanup,
    Completion,
    CompletionDelay,
}
pub fn artifact(dir: &Path, observer: bool, delay_ns: u64) -> PathBuf {
    artifact_with_fault(dir, observer, delay_ns, Fault::None)
}
pub fn artifact_with_fault(dir: &Path, observer: bool, delay_ns: u64, fault: Fault) -> PathBuf {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let name = if observer { "observer" } else { "actor" };
    let marker = dir.join(format!("{name}.closed"));
    let library = dir.join(format!("{name}.so"));
    let mut source =
        std::fs::read_to_string(root.join("tests/fixtures/metadata_callback.c")).unwrap();
    let anchor = "    (void)state; (void)handle; (void)meta;\n";
    let code = r#"    const char *closed_prefix = "codex.responses.v1/transport.closed/";
    if (method.len >= strlen(closed_prefix) && !memcmp(method.ptr, closed_prefix, strlen(closed_prefix))) {
        FILE *file = fopen(TEST_CLEANUP_PATH, "ab");
        if (!file) return INFRA_FAILED;
        fputc('x', file); fclose(file);
        struct timespec delay = {0, TEST_DELAY_NS}; nanosleep(&delay, NULL);
#if TEST_CLEANUP_FAIL
        return INFRA_FAILED;
#endif
        const uint8_t allow[] = {0}; return copy_reply(allow, 1, out);
    }
    if (method.len == 5 && !memcmp(method.ptr,"admit",5)) {
        const char *reply="{\"version\":1,\"decision\":\"accept\"}";
        return copy_reply((const uint8_t*)reply,strlen(reply),out);
    }
    if (method.len == 8 && !memcmp(method.ptr,"complete",8)) {
        FILE *file=fopen(TEST_FINISH_PATH,"ab");
        if (!file) return INFRA_FAILED;
        fputc('x',file);fclose(file);
        struct timespec delay={0,TEST_FINISH_DELAY_NS};nanosleep(&delay,NULL);
#if TEST_FINISH_FAIL
        return INFRA_FAILED;
#else
        const char *reply="{\"version\":1,\"decision\":\"continue\"}";
        return copy_reply((const uint8_t*)reply,strlen(reply),out);
#endif
    }
#if TEST_OBSERVER
    { const uint8_t allow[] = {0}; return copy_reply(allow, 1, out); }
#endif
"#;
    source = source.replacen(anchor, &format!("{anchor}{code}"), 1);
    source = source.replacen("const char *prefix = \"codex.responses.v1/response/\";", "const char *ws = \"codex.responses.v1/response.create/\"; const char *prefix = method.len >= strlen(ws) && !memcmp(method.ptr,ws,strlen(ws)) ? ws : \"codex.responses.v1/response/\";", 1);

    source = format!("#define _POSIX_C_SOURCE 200809L\n#include <time.h>\n{source}");
    let input = dir.join(format!("{name}.c"));
    std::fs::write(&input, source).unwrap();
    assert!(
        Command::new("cc")
            .args(["-shared", "-fPIC", "-std=c11", "-I"])
            .arg(root.join("../../infra/rust/crates/interop/plugin-abi/include"))
            .arg("-DTEST_PLUGIN_KIND=INFRA_PLUGIN_OBJECT")
            .arg("-DTEST_HTTP_TO_WS")
            .arg(format!("-DTEST_OBSERVER={}", u8::from(observer)))
            .arg(format!("-DTEST_DELAY_NS={delay_ns}"))
            .arg(format!(
                "-DTEST_CLEANUP_FAIL={}",
                u8::from(matches!(fault, Fault::Cleanup))
            ))
            .arg(format!(
                "-DTEST_FINISH_FAIL={}",
                u8::from(matches!(fault, Fault::Completion))
            ))
            .arg(format!(
                "-DTEST_FINISH_DELAY_NS={}",
                if matches!(fault, Fault::CompletionDelay) {
                    350_000_000
                } else {
                    0
                }
            ))
            .arg(format!(
                "-DTEST_FINISH_PATH={}",
                serde_json::to_string(&dir.join(format!("{name}.finish"))).unwrap()
            ))
            .arg(format!(
                "-DTEST_CLEANUP_PATH={}",
                serde_json::to_string(&marker).unwrap()
            ))
            .arg(input)
            .arg("-o")
            .arg(&library)
            .status()
            .unwrap()
            .success()
    );
    library
}
