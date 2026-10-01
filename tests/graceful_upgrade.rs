//! Real executable upgrades use Pingora's native listener FD transfer on Linux.
#![cfg(target_os = "linux")]

use serde_json::{Value, json};
use std::{
    fs,
    net::{SocketAddr, TcpListener as ReservedPort},
    os::unix::fs::FileTypeExt,
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus, Stdio},
    sync::Arc,
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Notify,
    task::{JoinHandle, JoinSet},
    time::{sleep, timeout},
};

const WAIT: Duration = Duration::from_secs(15);
const POLL: Duration = Duration::from_millis(20);

struct Process {
    child: Child,
    stdout: PathBuf,
    stderr: PathBuf,
}
impl Process {
    fn spawn(directory: &Path, name: &str, config: &Path, upgrade: bool) -> Self {
        let stdout = directory.join(format!("{name}.stdout"));
        let stderr = directory.join(format!("{name}.stderr"));
        let mut command = Command::new(env!("CARGO_BIN_EXE_pingora-worker"));
        command.arg("--config").arg(config);
        if upgrade {
            command.arg("--upgrade");
        }
        let child = command
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
    fn output(&self) -> String {
        format!(
            "{}\n{}",
            fs::read_to_string(&self.stdout).unwrap(),
            fs::read_to_string(&self.stderr).unwrap()
        )
    }
    fn assert_running(&mut self) {
        assert!(
            self.child.try_wait().unwrap().is_none(),
            "worker exited: {}",
            self.output()
        );
    }
    fn signal(&self, signal: &str) {
        assert!(
            Command::new("kill")
                .arg(signal)
                .arg(self.child.id().to_string())
                .status()
                .unwrap()
                .success()
        );
    }
    async fn wait(&mut self) -> ExitStatus {
        timeout(WAIT, async {
            loop {
                if let Some(status) = self.child.try_wait().unwrap() {
                    return status;
                }
                sleep(POLL).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("worker did not exit: {}", self.output()))
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

struct Origin {
    address: SocketAddr,
    task: JoinHandle<()>,
    release: Arc<Notify>,
    started: Arc<Notify>,
}
impl Drop for Origin {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn head(stream: &mut TcpStream) -> Vec<u8> {
    let mut bytes = Vec::new();
    while !bytes.ends_with(b"\r\n\r\n") {
        bytes.push(stream.read_u8().await.unwrap());
        assert!(bytes.len() <= 65536);
    }
    bytes
}
async fn origin(label: &'static str) -> Origin {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let release = Arc::new(Notify::new());
    let held = Arc::clone(&release);
    let started = Arc::new(Notify::new());
    let reached = Arc::clone(&started);
    let task = tokio::spawn(async move {
        let mut requests = JoinSet::new();
        loop {
            tokio::select! {
                accepted = listener.accept() => {
                    let (mut socket, _) = accepted.unwrap();
                    let held = Arc::clone(&held);
                    let reached = Arc::clone(&reached);
                    requests.spawn(async move {
                        let request = head(&mut socket).await;
                        if request.starts_with(b"GET /slow ") {
                            socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 11\r\nConnection: close\r\n\r\nold-").await.unwrap();
                            reached.notify_one();
                            held.notified().await;
                            socket.write_all(b"drained").await.unwrap();
                        } else {
                            socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{label}", label.len()).as_bytes()).await.unwrap();
                        }
                    });
                }
                finished = requests.join_next(), if !requests.is_empty() => { finished.unwrap().unwrap(); }
            }
        }
    });
    Origin {
        address,
        task,
        release,
        started,
    }
}
fn config(
    directory: &Path,
    name: &str,
    listen: SocketAddr,
    upstream: SocketAddr,
    socket: &Path,
) -> PathBuf {
    let path = directory.join(format!("{name}.json"));
    fs::write(
        &path,
        json!({
            "listen":listen, "upstream":format!("http://{upstream}"),
            "threads":4,"shutdown_seconds":3,"io_timeout_ms":30000,
            "upgrade":{"socket":socket},"logging":{"filter":"info"}
        })
        .to_string(),
    )
    .unwrap();
    path
}
async fn ready(process: &mut Process, address: SocketAddr) {
    timeout(WAIT, async {
        loop {
            process.assert_running();
            if TcpStream::connect(address).await.is_ok() {
                return;
            }
            sleep(POLL).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("listener did not start: {}", process.output()));
}
async fn get(address: SocketAddr) -> Vec<u8> {
    timeout(WAIT, async {
        let mut socket = TcpStream::connect(address).await.unwrap();
        socket
            .write_all(b"GET / HTTP/1.1\r\nHost: local\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        // These origins advertise Content-Length. Liveness requires the full
        // HTTP response, independently of how the peer subsequently closes TCP.
        let mut response = head(&mut socket).await;
        assert!(
            response.starts_with(b"HTTP/1.1 200"),
            "{}",
            String::from_utf8_lossy(&response)
        );
        let header = std::str::from_utf8(&response).unwrap();
        let lengths: Vec<usize> = header
            .lines()
            .filter_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-length").then(|| {
                    value
                        .trim()
                        .parse()
                        .expect("invalid response Content-Length")
                })
            })
            .collect();
        assert_eq!(
            lengths.len(),
            1,
            "expected one response Content-Length: {header}"
        );
        assert!(lengths[0] <= 1024 * 1024);
        let offset = response.len();
        response.resize(offset + lengths[0], 0);
        socket
            .read_exact(&mut response[offset..])
            .await
            .expect("worker truncated the framed HTTP response");
        response
    })
    .await
    .unwrap()
}
fn listener_inode(address: SocketAddr) -> String {
    let table = fs::read_to_string("/proc/net/tcp").unwrap();
    let port = format!("{:04X}", address.port());
    table
        .lines()
        .skip(1)
        .find_map(|line| {
            let fields: Vec<_> = line.split_whitespace().collect();
            (fields[1].ends_with(&format!(":{port}")) && fields[3] == "0A")
                .then(|| format!("socket:[{}]", fields[9]))
        })
        .expect("missing native listening socket")
}
fn owns_socket(process: &Process, inode: &str) -> bool {
    fs::read_dir(format!("/proc/{}/fd", process.child.id()))
        .unwrap()
        .any(|entry| {
            entry
                .ok()
                .and_then(|entry| fs::read_link(entry.path()).ok())
                .is_some_and(|target| target.to_string_lossy() == inode)
        })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn executable_upgrade_preserves_listener_and_drains_old_in_flight_response() {
    // Keep the UDS path short even when the repository lives in a deep worktree.
    let directory = tempfile::tempdir_in("/tmp").unwrap();
    let socket = directory.path().join("upgrade.sock");
    let old_origin = origin("old").await;
    let new_origin = origin("new").await;
    let reservation = ReservedPort::bind("127.0.0.1:0").unwrap();
    let address = reservation.local_addr().unwrap();
    let old_config = config(
        directory.path(),
        "old",
        address,
        old_origin.address,
        &socket,
    );
    let new_config = config(
        directory.path(),
        "new",
        address,
        new_origin.address,
        &socket,
    );
    drop(reservation);
    let mut old = Process::spawn(directory.path(), "old", &old_config, false);
    ready(&mut old, address).await;
    assert!(get(address).await.ends_with(b"old"));
    let inode = listener_inode(address);
    assert!(owns_socket(&old, &inode));

    let mut held = TcpStream::connect(address).await.unwrap();
    held.write_all(b"GET /slow HTTP/1.1\r\nHost: local\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    timeout(WAIT, old_origin.started.notified()).await.unwrap();

    let mut replacement = Process::spawn(directory.path(), "new", &new_config, true);
    timeout(WAIT, async {
        loop {
            replacement.assert_running();
            old.assert_running();
            if fs::symlink_metadata(&socket).is_ok_and(|metadata| metadata.file_type().is_socket())
            {
                break;
            }
            sleep(POLL).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "replacement did not wait for FD transfer: {}",
            replacement.output()
        )
    });
    // Do not connect to the transfer socket: Pingora expects SCM_RIGHTS there.
    assert!(get(address).await.ends_with(b"old"));
    old.signal("-QUIT");

    let mut saw_new = false;
    timeout(WAIT, async {
        loop {
            old.assert_running();
            replacement.assert_running();
            let response = get(address).await;
            assert!(
                response.ends_with(b"old") || response.ends_with(b"new"),
                "{response:?}"
            );
            saw_new |= response.ends_with(b"new");
            if saw_new
                && old
                    .output()
                    .contains("Broadcast graceful shutdown complete")
            {
                break;
            }
            sleep(POLL).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "handoff did not finish: old={} new={}",
            old.output(),
            replacement.output()
        )
    });
    assert_eq!(
        listener_inode(address),
        inode,
        "upgrade rebound the TCP port"
    );
    assert!(
        owns_socket(&replacement, &inode),
        "replacement did not inherit the original listener"
    );
    // Old request workers are draining while replacement accepts on the same FD.
    old.assert_running();
    old_origin.release.notify_one();
    let mut tail = Vec::new();
    timeout(WAIT, held.read_to_end(&mut tail))
        .await
        .unwrap()
        .unwrap();
    assert!(tail.starts_with(b"HTTP/1.1 200"), "{tail:?}");
    assert!(tail.ends_with(b"old-drained"), "{tail:?}");
    assert!(old.wait().await.success(), "{}", old.output());
    for _ in 0..4 {
        assert!(get(address).await.ends_with(b"new"));
    }
    replacement.signal("-TERM");
    assert!(
        replacement.wait().await.success(),
        "{}",
        replacement.output()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn invalid_replacement_plugin_fails_before_handoff_and_old_keeps_serving() {
    let directory = tempfile::tempdir_in("/tmp").unwrap();
    let socket = directory.path().join("upgrade.sock");
    let origin = origin("old").await;
    let reservation = ReservedPort::bind("127.0.0.1:0").unwrap();
    let address = reservation.local_addr().unwrap();
    let old_config = config(directory.path(), "old", address, origin.address, &socket);
    let invalid_config = config(
        directory.path(),
        "invalid",
        address,
        origin.address,
        &socket,
    );
    let mut value: Value = serde_json::from_slice(&fs::read(&invalid_config).unwrap()).unwrap();
    value["callbacks"] = json!([{"path":directory.path().join("missing.so"),"package":"trusted.metadata","hooks":["pre_routing"]}]);
    fs::write(&invalid_config, value.to_string()).unwrap();
    drop(reservation);
    let mut old = Process::spawn(directory.path(), "old", &old_config, false);
    ready(&mut old, address).await;
    assert!(get(address).await.ends_with(b"old"));
    let inode = listener_inode(address);
    let mut invalid = Process::spawn(directory.path(), "invalid", &invalid_config, true);
    assert!(
        !invalid.wait().await.success(),
        "invalid plugin unexpectedly loaded"
    );
    assert!(
        invalid
            .output()
            .contains("native notifier initialization failed: Load"),
        "{}",
        invalid.output()
    );
    assert!(
        !socket.exists(),
        "failed replacement entered native FD handoff"
    );
    old.assert_running();
    assert_eq!(listener_inode(address), inode);
    for _ in 0..4 {
        assert!(get(address).await.ends_with(b"old"));
    }
    old.signal("-TERM");
    assert!(old.wait().await.success(), "{}", old.output());
}
