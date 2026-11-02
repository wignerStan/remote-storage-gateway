#![cfg(feature = "l7")]
//! A retained active generation remains owned during real process SIGTERM.
#![cfg(target_os = "linux")]
use serde_json::json;
use std::{
    fs,
    path::Path,
    process::{Child, Command, Stdio},
    sync::Arc,
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpSocket, TcpStream},
    sync::Notify,
    time::{sleep, timeout},
};
#[path = "support/generation_lifetime_fixture.rs"]
mod fixture;
const WAIT: Duration = Duration::from_secs(10);
struct Process(Child);
impl Drop for Process {
    fn drop(&mut self) {
        if matches!(self.0.try_wait(), Ok(None)) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
}
async fn head(socket: &mut TcpStream) -> Vec<u8> {
    let mut bytes = Vec::new();
    while !bytes.ends_with(b"\r\n\r\n") {
        bytes.push(socket.read_u8().await.unwrap());
        assert!(bytes.len() < 65536);
    }
    bytes
}
async fn frame(socket: &mut TcpStream, masked: bool) -> (u8, Vec<u8>) {
    let op = socket.read_u8().await.unwrap() & 15;
    let len = socket.read_u8().await.unwrap();
    assert_eq!(len & 128 != 0, masked);
    let len = match len & 127 {
        126 => socket.read_u16().await.unwrap() as usize,
        n => n as usize,
    };
    let mut mask = [0; 4];
    if masked {
        socket.read_exact(&mut mask).await.unwrap();
    }
    let mut body = vec![0; len];
    socket.read_exact(&mut body).await.unwrap();
    for (i, b) in body.iter_mut().enumerate() {
        if masked {
            *b ^= mask[i % 4];
        }
    }
    (op, body)
}
fn owned_listener(process: &Process) -> Option<std::net::SocketAddr> {
    let owned = fs::read_dir(format!("/proc/{}/fd", process.0.id()))
        .ok()?
        .filter_map(|entry| fs::read_link(entry.ok()?.path()).ok())
        .collect::<Vec<_>>();
    fs::read_to_string("/proc/net/tcp")
        .ok()?
        .lines()
        .skip(1)
        .find_map(|line| {
            let fields = line.split_whitespace().collect::<Vec<_>>();
            let (ip, port) = fields.get(1)?.split_once(':')?;
            if fields.get(3) != Some(&"0A")
                || ip != "0100007F"
                || !owned
                    .iter()
                    .any(|path| path.to_string_lossy() == format!("socket:[{}]", fields[9]))
            {
                return None;
            }
            Some(
                format!("127.0.0.1:{}", u16::from_str_radix(port, 16).ok()?)
                    .parse()
                    .unwrap(),
            )
        })
}

async fn marker(dir: &Path, wanted: char) {
    timeout(WAIT, async {
        while !fs::read_to_string(dir.join("events"))
            .unwrap_or_default()
            .contains(wanted)
        {
            sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sigterm_drains_detached_active_generation_before_callback_shutdown() {
    shutdown_case(true).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sigterm_drains_attached_active_generation_after_process_grace() {
    shutdown_case(false).await;
}
async fn shutdown_case(detach: bool) {
    timeout(Duration::from_secs(20),async{
    let directory=tempfile::tempdir().unwrap();let dir=directory.path();
    let origin=TcpListener::bind("127.0.0.1:0").await.unwrap();let origin_addr=origin.local_addr().unwrap();
    let started=Arc::new(Notify::new());let release=Arc::new(Notify::new());let begin=started.clone();let end=release.clone();
    let upstream=tokio::spawn(async move{
        let(mut socket,_)=origin.accept().await.unwrap();let request=String::from_utf8(head(&mut socket).await).unwrap();assert!(request.starts_with("GET /v1/responses HTTP/1.1"));
        let key=request.lines().find_map(|line|line.split_once(':').filter(|(name,_)|name.eq_ignore_ascii_case("sec-websocket-key")).map(|(_,v)|v.trim())).unwrap();let accept=proxy_pingora::connect_upstream::websocket_session::handshake::accept_key(key).unwrap();
        socket.write_all(format!("HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Accept: {accept}\r\n\r\n").as_bytes()).await.unwrap();
        assert_eq!(frame(&mut socket, true).await.0,1);begin.notify_one();end.notified().await;
        socket.write_all(&proxy_protocols::websocket::encode_server(1,br#"{"type":"response.completed","response":{"id":"settled-after-sigterm","output":[]}}"#).unwrap()).await.unwrap();
        assert_eq!(frame(&mut socket, true).await.0,8);
        let mut tail=Vec::new();socket.read_to_end(&mut tail).await.unwrap();
    });
    // The child binds its own port. A parent bind/drop can be reclaimed by another fixture.
    let listen="127.0.0.1:0";
    let config=dir.join("worker.json");fs::write(&config,serde_json::to_vec(&json!({"listen":listen,"upstream":format!("http://{origin_addr}"),"threads":2,"shutdown_seconds":3,"io_timeout_ms":5000,"callbacks":[{"binding":"direct","path":fixture::artifact(dir,3540000,3600000,true),"package":"worker.sessions.test","message_codec":"openai.responses.v1","message_actions":true}]})).unwrap()).unwrap();
    let mut process=Process(Command::new(env!("CARGO_BIN_EXE_network-data-plane-worker")).arg("--config").arg(config).stdout(Stdio::null()).stderr(Stdio::from(fs::File::create(dir.join("stderr")).unwrap())).spawn().unwrap());
    let address=timeout(WAIT,async{loop{assert!(process.0.try_wait().unwrap().is_none(),"worker failed before admission");if let Some(address)=owned_listener(&process){return address;}sleep(Duration::from_millis(10)).await;}}).await.unwrap();
    let mut client=TcpStream::connect(address).await.unwrap();
    client.write_all(b"GET /v1/responses HTTP/1.1\r\nHost: local\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n").await.unwrap();assert!(head(&mut client).await.starts_with(b"HTTP/1.1 101"));
    client.write_all(&proxy_protocols::websocket::encode_client(1,br#"{"type":"response.create","input":[]}"#).unwrap()).await.unwrap();started.notified().await;
    let mut client=Some(client);
    if detach { drop(client.take());marker(dir,'d').await;assert_eq!(fs::read_to_string(dir.join("events")).unwrap(),"d"); }
    assert!(Command::new("kill").args(["-TERM",&process.0.id().to_string()]).status().unwrap().success());sleep(Duration::from_millis(1600)).await;release.notify_one();
    if let Some(socket)=client.as_mut() {
        assert_eq!(frame(socket,false).await,(1,br#"{"type":"response.completed","response":{"id":"settled-after-sigterm","output":[]}}"#.to_vec()));
        assert_eq!(frame(socket,false).await,(8,1001_u16.to_be_bytes().to_vec()));
        socket.write_all(&proxy_protocols::websocket::encode_client(8,&1001_u16.to_be_bytes()).unwrap()).await.unwrap();
    }
    drop(client);
    let upstream_result=upstream.await; assert!(upstream_result.is_ok(), "retained origin failed: {upstream_result:?}; events={:?}; stderr={}", fs::read_to_string(dir.join("events")), fs::read_to_string(dir.join("stderr")).unwrap()); let status=timeout(WAIT,async{loop{if let Some(status)=process.0.try_wait().unwrap(){return status;}sleep(Duration::from_millis(10)).await;}}).await.expect("process failed to join retained generation");
    assert!(status.success(),"{}",fs::read_to_string(dir.join("stderr")).unwrap());assert_eq!(fs::read_to_string(dir.join("events")).unwrap(),if detach {"dtc"} else {"tc"});
    assert!(TcpStream::connect(address).await.is_err());let listener=TcpSocket::new_v4().unwrap();listener.set_reuseaddr(true).unwrap();listener.bind(address).unwrap();
 }).await.unwrap();
}
