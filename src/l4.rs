//! Worker-owned static TCP lane using infra's raw transport facility.
use serde::Deserialize;
use std::{io::Read, net::SocketAddr, path::PathBuf, time::Duration};
use tokio::{
    net::{TcpListener, TcpStream},
    task::JoinSet,
};
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub listen: SocketAddr,
    pub upstream: SocketAddr,
    #[serde(default = "default_connections")]
    pub max_connections: usize,
    #[serde(default = "default_connect")]
    pub connect_timeout_ms: u64,
    #[serde(default = "default_age")]
    pub max_connection_age_ms: u64,
}
fn default_connections() -> usize {
    128
}
fn default_connect() -> u64 {
    10_000
}
fn default_age() -> u64 {
    3_600_000
}
impl Config {
    pub fn load(path: &std::path::Path) -> Result<Self, String> {
        let file = std::fs::File::open(path).map_err(|_| "L4 configuration unavailable")?;
        let mut bytes = Vec::new();
        file.take((1 << 20) + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| "L4 configuration unreadable")?;
        if bytes.len() > 1 << 20 {
            return Err("L4 configuration exceeds bound".into());
        }
        let config: Self =
            serde_json::from_slice(&bytes).map_err(|_| "invalid L4 configuration")?;
        config.validate()?;
        Ok(config)
    }
    pub fn validate(&self) -> Result<(), String> {
        if self.listen == self.upstream
            || self.upstream.port() == 0
            || self.max_connections == 0
            || self.max_connections > 1024
            || !(1..=300_000).contains(&self.connect_timeout_ms)
            || !(1..=86_400_000).contains(&self.max_connection_age_ms)
        {
            return Err("invalid L4 route or bounds".into());
        }
        Ok(())
    }
}
pub fn start(args: Vec<std::ffi::OsString>) -> Result<(), String> {
    if args.len() == 1 && matches!(args[0].to_str(), Some("--help" | "-h")) {
        println!("network-data-plane-worker --lane l4 --config <file.json> [--check-config]");
        return Ok(());
    }
    let mut args = args.into_iter();
    let (mut path, mut check) = (None, false);
    while let Some(arg) = args.next() {
        match arg.to_str() {
            Some("--config") if path.is_none() => {
                path = Some(PathBuf::from(args.next().ok_or("--config requires path")?))
            }
            Some("--check-config") if !check => check = true,
            _ => return Err("expected --lane l4 --config <file.json> [--check-config]".into()),
        }
    }
    let config = Config::load(&path.ok_or("--config required")?)?;
    if check {
        println!("configuration valid");
        return Ok(());
    }
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|_| "L4 runtime unavailable")?
        .block_on(run(config))
}
pub async fn forward(mut downstream: TcpStream, config: &Config) -> std::io::Result<()> {
    let mut upstream = tokio::time::timeout(
        Duration::from_millis(config.connect_timeout_ms),
        TcpStream::connect(config.upstream),
    )
    .await??;
    tokio::time::timeout(
        Duration::from_millis(config.max_connection_age_ms),
        proxy_pingora::l4::bridge(&mut downstream, &mut upstream),
    )
    .await??;
    Ok(())
}
async fn run(config: Config) -> Result<(), String> {
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .map_err(|_| "L4 signal owner unavailable")?;
    let listener = TcpListener::bind(config.listen)
        .await
        .map_err(|_| "L4 listener unavailable")?;
    let mut tasks = JoinSet::new();
    let mut outcome = Ok(());
    loop {
        tokio::select! {
            _=tokio::signal::ctrl_c()=>break,
            _=terminate.recv()=>break,
            Some(_)=tasks.join_next(),if !tasks.is_empty()=>{},
            accepted=listener.accept()=>{match accepted{Ok((socket,_))=>{if tasks.len()>=config.max_connections{drop(socket);continue;}let config=config.clone();tasks.spawn(async move{let _=forward(socket,&config).await;});},Err(_)=>{outcome=Err("L4 accept failed".into());break;}}}
        }
    }
    drop(listener);
    while tasks.join_next().await.is_some() {}
    outcome
}
#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    #[tokio::test]
    async fn configured_route_preserves_write_half_close() {
        let origin = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = origin.local_addr().unwrap();
        let upstream = tokio::spawn(async move {
            let (mut stream, _) = origin.accept().await.unwrap();
            let mut b = Vec::new();
            stream.read_to_end(&mut b).await.unwrap();
            assert_eq!(b, b"request");
            stream.write_all(b"response").await.unwrap();
        });
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let local = listener.local_addr().unwrap();
        let config = Config {
            listen: local,
            upstream: address,
            max_connections: 4,
            connect_timeout_ms: 1000,
            max_connection_age_ms: 3000,
        };
        let task = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            forward(socket, &config).await.unwrap();
        });
        let mut client = TcpStream::connect(local).await.unwrap();
        client.write_all(b"request").await.unwrap();
        client.shutdown().await.unwrap();
        let mut b = Vec::new();
        client.read_to_end(&mut b).await.unwrap();
        assert_eq!(b, b"response");
        task.await.unwrap();
        upstream.await.unwrap();
    }
}
