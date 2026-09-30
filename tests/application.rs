//! Real process assembly, native callback loading, logging and signal shutdown.
#![cfg(unix)]
use network_actions::proto::{CallRequest, actions_client::ActionsClient};
use serde_json::{Value, json};
use std::{
    fs,
    net::TcpListener as ReservedPort,
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus, Stdio},
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::timeout,
};

const WAIT: Duration = Duration::from_secs(10);
struct Process {
    child: Child,
    stdout: PathBuf,
    stderr: PathBuf,
}
impl Process {
    fn spawn(directory: &Path, args: &[&str]) -> Self {
        let stdout = directory.join("stdout.log");
        let stderr = directory.join("stderr.log");
        let child = Command::new(env!("CARGO_BIN_EXE_pingora-worker"))
            .args(args)
            .stdout(Stdio::from(fs::File::create(&stdout).unwrap()))
            .stderr(Stdio::from(fs::File::create(&stderr).unwrap()))
            .spawn()
            .unwrap();
        Self {
            child,
            stdout,
            stderr,
        }
    }
    fn wait(&mut self) -> ExitStatus {
        let deadline = Instant::now() + WAIT;
        loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                return status;
            }
            assert!(
                Instant::now() < deadline,
                "process did not exit: {}",
                self.errors()
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }
    fn errors(&self) -> String {
        fs::read_to_string(&self.stderr).unwrap()
    }
}
impl Drop for Process {
    fn drop(&mut self) {
        if !matches!(self.child.try_wait(), Ok(Some(_))) {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

#[test]
fn cli_help_and_check_validate_without_loading_plugins_or_opening_listeners() {
    let directory = tempfile::tempdir().unwrap();
    let mut help = Process::spawn(directory.path(), &["--help"]);
    assert!(help.wait().success(), "{}", help.errors());
    assert!(
        fs::read_to_string(&help.stdout)
            .unwrap()
            .contains("--check-config")
    );

    // Occupied listener ports and a missing selected artifact must not affect checking.
    let data = ReservedPort::bind("127.0.0.1:0").unwrap();
    let management = ReservedPort::bind("127.0.0.1:0").unwrap();
    let access = directory.path().join("access");
    let body = directory.path().join("body");
    let config = directory.path().join("config.json");
    fs::write(&config, json!({
        "upstream":"http://127.0.0.1:1", "listen":data.local_addr().unwrap(),
        "management":{"listen":management.local_addr().unwrap()},
        "callbacks":[{"path":directory.path().join("missing.so"),"package":"trusted.metadata", "hooks":["pre_routing"]}],
        "logging":{"access_file":{"directory":access,"keep_files":30},
            "body_file":{"directory":body,"keep_files":2},"body_max_bytes":1024}
    }).to_string()).unwrap();
    let args = ["--config", config.to_str().unwrap(), "--check-config"];
    let mut check = Process::spawn(directory.path(), &args);
    assert!(check.wait().success(), "{}", check.errors());
    assert!(
        fs::read_to_string(&check.stdout)
            .unwrap()
            .contains("configuration valid")
    );
    assert!(
        !access.exists() && !body.exists(),
        "configuration checking created log sinks"
    );

    fs::write(&config, b"{invalid-json").unwrap();
    let mut invalid = Process::spawn(directory.path(), &args);
    assert!(!invalid.wait().success());
    assert!(invalid.errors().contains("cannot load configuration"));
    assert!(!access.exists() && !body.exists());
}

async fn request_head(socket: &mut TcpStream) -> String {
    let mut bytes = Vec::new();
    while !bytes.ends_with(b"\r\n\r\n") {
        bytes.push(socket.read_u8().await.unwrap());
        assert!(bytes.len() <= 65536);
    }
    String::from_utf8(bytes).unwrap()
}
fn records(directory: &Path) -> (String, Vec<Value>) {
    let mut text = String::new();
    for entry in fs::read_dir(directory).unwrap() {
        text.push_str(&fs::read_to_string(entry.unwrap().path()).unwrap());
    }
    let records = text
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    (text, records)
}
fn callback_artifact(directory: &Path) -> PathBuf {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let artifact = directory.join("metadata.so");
    assert!(
        Command::new("cc")
            .args(["-shared", "-fPIC", "-std=c11", "-I"])
            .arg(root.join("../infra/rust/crates/interop/plugin-abi/include"))
            .arg(root.join("tests/fixtures/metadata_callback.c"))
            .arg("-o")
            .arg(&artifact)
            .status()
            .unwrap()
            .success()
    );
    artifact
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn executable_loads_callback_proxies_post_and_drains_separate_logs_on_sigterm() {
    const BODY: &[u8] = br#"{"api_key":"body-secret","ordinary":"customer-visible"}"#;
    let directory = tempfile::tempdir().unwrap();
    let origin = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin_address = origin.local_addr().unwrap();
    let origin_task = tokio::spawn(async move {
        timeout(WAIT, async {
            let (mut socket, _) = origin.accept().await.unwrap();
            let head = request_head(&mut socket).await;
            assert!(head.starts_with("POST /upload?q=kept HTTP/1.1\r\n"));
            assert!(
                head.to_ascii_lowercase()
                    .contains("x-native: installed\r\n"),
                "{head}"
            );
            let mut body = vec![0; BODY.len()];
            socket.read_exact(&mut body).await.unwrap();
            assert_eq!(
                body, BODY,
                "explicit dump must not alter the native request body"
            );
            socket
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
                .await
                .unwrap();
        })
        .await
        .unwrap();
    });
    // Reserve both ports together, then release them immediately before process start.
    let data = ReservedPort::bind("127.0.0.1:0").unwrap();
    let management = ReservedPort::bind("127.0.0.1:0").unwrap();
    let data_address = data.local_addr().unwrap();
    let management_address = management.local_addr().unwrap();
    let access_dir = directory.path().join("access");
    let body_dir = directory.path().join("body");
    let config = directory.path().join("config.json");
    fs::write(&config, json!({
        "upstream":format!("http://{origin_address}"), "listen":data_address,
        "management":{"listen":management_address},"threads":1,"shutdown_seconds":2,
        "logging":{"filter":"off","access_file":{"directory":access_dir,"keep_files":30},
            "body_file":{"directory":body_dir,"keep_files":2},"body_max_bytes":1024},
        "callbacks":[{"path":callback_artifact(directory.path()),"package":"worker.metadata.test",
            "hooks":["pre_routing"], "config":{"version":1,"decision":"continue","set_headers":[["x-native","installed"]]}}]
    }).to_string()).unwrap();
    drop((data, management));
    let mut process = Process::spawn(directory.path(), &["--config", config.to_str().unwrap()]);
    let deadline = Instant::now() + WAIT;
    let mut client = loop {
        if let Some(status) = process.child.try_wait().unwrap() {
            panic!("worker exited at startup {status}: {}", process.errors());
        }
        if let Ok(client) = TcpStream::connect(data_address).await {
            break client;
        }
        assert!(
            Instant::now() < deadline,
            "listener did not start: {}",
            process.errors()
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    let mut management = timeout(
        WAIT,
        ActionsClient::connect(format!("http://{management_address}")),
    )
    .await
    .unwrap()
    .unwrap();
    let snapshot = timeout(
        WAIT,
        management.call(CallRequest {
            binding: "worker.get".into(),
            input: b"{}".to_vec(),
        }),
    )
    .await
    .unwrap()
    .unwrap()
    .into_inner();
    assert_eq!(
        serde_json::from_slice::<Value>(&snapshot.output).unwrap(),
        json!({"revision":1,"accepting":true})
    );
    drop(management);
    timeout(WAIT, async {
        client.write_all(format!("POST /upload?q=kept HTTP/1.1\r\nHost: downstream\r\nAuthorization: Bearer header-secret\r\nConnection: close\r\nContent-Length: {}\r\n\r\n", BODY.len()).as_bytes()).await.unwrap();
        client.write_all(BODY).await.unwrap();
        let mut response = Vec::new();
        client.read_to_end(&mut response).await.unwrap();
        assert!(response.starts_with(b"HTTP/1.1 200"), "{}", String::from_utf8_lossy(&response));
        assert!(response.ends_with(b"ok"));
    }).await.unwrap();
    origin_task.await.unwrap();
    drop(client);
    assert!(
        Command::new("kill")
            .args(["-TERM", &process.child.id().to_string()])
            .status()
            .unwrap()
            .success()
    );
    assert!(process.wait().success(), "{}", process.errors());
    let (access_text, access) = records(&access_dir);
    let (body_text, dumps) = records(&body_dir);
    assert_eq!(dumps.len(), 1, "{body_text}");
    assert_eq!(dumps[0]["target"], "proxy_pingora::body_dump");
    assert_eq!(dumps[0]["fields"]["outcome"], "captured");
    let detail: Value =
        serde_json::from_str(dumps[0]["fields"]["detail"].as_str().unwrap()).unwrap();
    assert!(detail["api_key"].as_str().unwrap().starts_with("sha256:"));
    assert_eq!(detail["ordinary"], "customer-visible");
    let request_id = &dumps[0]["fields"]["request_id"];
    assert!(
        access
            .iter()
            .any(|record| record["target"] == "proxy_pingora::access"
                && &record["fields"]["request_id"] == request_id
                && record["fields"]["status"] == 200
                && record["fields"]["completed"] == true),
        "missing drained access outcome: {access_text}"
    );
    assert!(!access_text.contains("customer-visible") && !access_text.contains("body_dump"));
    let stdout = fs::read_to_string(&process.stdout).unwrap();
    let stderr = process.errors();
    assert!(!stdout.contains("customer-visible") && !stderr.contains("customer-visible"));
    for output in [access_text, body_text, stdout, stderr] {
        assert!(!output.contains("body-secret") && !output.contains("header-secret"));
    }
    assert!(
        std::net::TcpStream::connect_timeout(&data_address, Duration::from_millis(100)).is_err()
    );
    assert!(
        std::net::TcpStream::connect_timeout(&management_address, Duration::from_millis(100))
            .is_err()
    );
}
