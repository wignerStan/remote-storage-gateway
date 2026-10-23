//! Real SIGTERM drains active WS and HTTP-to-WS turns and releases the listener.
#![cfg(unix)]
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
#[path = "support/session_exit_fixture.rs"]
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
    let second = socket.read_u8().await.unwrap();
    assert_eq!(second & 128 != 0, masked);
    let len = match second & 127 {
        126 => socket.read_u16().await.unwrap() as usize,
        127 => socket.read_u64().await.unwrap() as usize,
        n => n as usize,
    };
    let mut mask = [0; 4];
    if masked {
        socket.read_exact(&mut mask).await.unwrap();
    }
    let mut bytes = vec![0; len];
    socket.read_exact(&mut bytes).await.unwrap();
    if masked {
        for (i, b) in bytes.iter_mut().enumerate() {
            *b ^= mask[i % 4];
        }
    }
    (op, bytes)
}
async fn wait_exit(process: &mut Process, dir: &Path) {
    let status = timeout(WAIT, async {
        loop {
            if let Some(status) = process.0.try_wait().unwrap() {
                return status;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "worker hung during SIGTERM: {}",
            fs::read_to_string(dir.join("stderr")).unwrap()
        )
    });
    assert!(
        status.success(),
        "worker shutdown failed: {}",
        fs::read_to_string(dir.join("stderr")).unwrap()
    );
}
async fn scenario(http: bool) {
    timeout(Duration::from_secs(15),async{
    let directory=tempfile::tempdir().unwrap();let dir=directory.path();
    let origin=TcpListener::bind("127.0.0.1:0").await.unwrap();let origin_addr=origin.local_addr().unwrap();
    let started=Arc::new(Notify::new());let release=Arc::new(Notify::new());let start=started.clone();let end=release.clone();
    let origin_task=tokio::spawn(async move{
        let(mut socket,_)=origin.accept().await.unwrap();let request=String::from_utf8(head(&mut socket).await).unwrap();assert!(request.starts_with("GET /backend-api/codex/responses HTTP/1.1"));
        let key=request.lines().find_map(|line|line.split_once(':').filter(|(name,_)|name.eq_ignore_ascii_case("sec-websocket-key")).map(|(_,v)|v.trim())).unwrap();let accept=proxy_pingora::connect_upstream::websocket_session::handshake::accept_key(key).unwrap();
        socket.write_all(format!("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {accept}\r\n\r\n").as_bytes()).await.unwrap();
        assert_eq!(frame(&mut socket,true).await.0,1);
        socket.write_all(&proxy_protocols::websocket::encode_server(1,br#"{"type":"response.output_text.delta","delta":"pending"}"#).unwrap()).await.unwrap();start.notify_one();end.notified().await;
        socket.write_all(&proxy_protocols::websocket::encode_server(1,br#"{"type":"response.completed","response":{"id":"shutdown-finished","output":[]}}"#).unwrap()).await.unwrap();
        let mut tail=Vec::new();socket.read_to_end(&mut tail).await.unwrap();
    });
    let reserved=std::net::TcpListener::bind("127.0.0.1:0").unwrap();let address=reserved.local_addr().unwrap();
    let config=dir.join("worker.json");fs::write(&config,serde_json::to_vec(&json!({"listen":address,"upstream":format!("http://{origin_addr}"),"threads":2,"shutdown_seconds":2,"io_timeout_ms":5000,"callbacks":[{"binding":"direct","path":fixture::artifact(dir,false,0),"package":"worker.metadata.test","hooks":[],"message_codec":"codex.responses.v1","message_actions":true,"message_http_bridge":true,"config":{"version":1,"decision":"forward","upstream":"connect","path":"/backend-api/codex/responses"}}]})).unwrap()).unwrap();drop(reserved);
    let mut process=Process(Command::new(env!("CARGO_BIN_EXE_pingora-worker")).arg("--config").arg(config).stdout(Stdio::null()).stderr(Stdio::from(fs::File::create(dir.join("stderr")).unwrap())).spawn().unwrap());
    let mut client=timeout(WAIT,async{loop{if let Ok(socket)=TcpStream::connect(address).await{return socket;}assert!(process.0.try_wait().unwrap().is_none(),"worker failed before admission");sleep(Duration::from_millis(10)).await;}}).await.unwrap();
    if http{let body=br#"{"input":[]}"#;client.write_all(format!("POST /v1/responses HTTP/1.1\r\nHost: local\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",body.len()).as_bytes()).await.unwrap();client.write_all(body).await.unwrap();}
    else{client.write_all(b"GET /v1/responses HTTP/1.1\r\nHost: local\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n").await.unwrap();assert!(head(&mut client).await.starts_with(b"HTTP/1.1 101"));client.write_all(&proxy_protocols::websocket::encode_client(1,br#"{"type":"response.create","input":[]}"#).unwrap()).await.unwrap();assert_eq!(frame(&mut client,false).await.0,1);}
    started.notified().await;assert!(Command::new("kill").args(["-TERM",&process.0.id().to_string()]).status().unwrap().success());sleep(Duration::from_millis(100)).await;release.notify_one();
    if http{let mut body=Vec::new();client.read_to_end(&mut body).await.unwrap();assert!(body.starts_with(b"HTTP/1.1 200"));assert!(String::from_utf8_lossy(&body).contains("shutdown-finished"));}
    else{let(op,complete)=frame(&mut client,false).await;assert_eq!(op,1);assert_eq!(serde_json::from_slice::<serde_json::Value>(&complete).unwrap()["type"],"response.completed");client.write_all(&proxy_protocols::websocket::encode_client(8,&1000_u16.to_be_bytes()).unwrap()).await.unwrap();assert_eq!(frame(&mut client,false).await.0,8);}
    drop(client);origin_task.await.unwrap();wait_exit(&mut process,dir).await;
    assert!(dir.join("actor.closed").exists(),"process exit omitted the session owner cleanup event");
    assert!(TcpStream::connect(address).await.is_err(),"exited worker retained listener");let bind=TcpSocket::new_v4().unwrap();bind.set_reuseaddr(true).unwrap();bind.bind(address).unwrap();drop(bind);
}).await.unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sigterm_drains_an_active_controlled_websocket_then_releases_listener() {
    scenario(false).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sigterm_drains_an_active_http_to_ws_bridge_then_releases_listener() {
    scenario(true).await;
}
