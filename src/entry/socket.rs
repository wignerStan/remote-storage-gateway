use std::{
    net::SocketAddr,
    os::unix::fs::{MetadataExt, PermissionsExt},
    path::PathBuf,
};

/// Listener selection is supplied by the release application, not plugin method input.
pub enum ActionEndpoint {
    Tcp(SocketAddr),
    Unix(PathBuf),
}
impl ActionEndpoint {
    pub fn parse(raw: &str) -> Result<Self, String> {
        if let Some(path) = raw.trim().strip_prefix("unix://") {
            if !path.starts_with('/') || path.contains('\0') || path.ends_with('/') {
                return Err("worker gRPC unix socket must name an absolute socket path".into());
            }
            return Ok(Self::Unix(path.into()));
        }
        let addr: SocketAddr = raw
            .trim()
            .parse()
            .map_err(|_| "worker gRPC address must be unix or loopback host:port")?;
        if !addr.ip().is_loopback() {
            return Err("worker gRPC address must be loopback".into());
        }
        Ok(Self::Tcp(addr))
    }
}
#[derive(Clone, Copy)]
pub struct SocketPermissions {
    pub mode: u32,
    pub group: Option<u32>,
}
impl Default for SocketPermissions {
    fn default() -> Self {
        Self {
            mode: 0o600,
            group: None,
        }
    }
}
pub(super) enum Bound {
    Tcp(tokio::net::TcpListener),
    Unix(tokio::net::UnixListener),
}
pub(super) struct OwnedSocket {
    path: PathBuf,
    device: u64,
    inode: u64,
}
impl Drop for OwnedSocket {
    fn drop(&mut self) {
        if let Ok(metadata) = std::fs::symlink_metadata(&self.path) {
            if metadata.dev() == self.device && metadata.ino() == self.inode {
                let _ = std::fs::remove_file(&self.path);
            }
        }
    }
}
pub(super) async fn bind(
    endpoint: ActionEndpoint,
    permissions: SocketPermissions,
) -> Result<(Bound, Option<OwnedSocket>), String> {
    match endpoint {
        ActionEndpoint::Tcp(address) => {
            if !address.ip().is_loopback() {
                return Err("management TCP must be loopback".into());
            }
            let listener = tokio::net::TcpListener::bind(address)
                .await
                .map_err(|e| e.to_string())?;
            Ok((Bound::Tcp(listener), None))
        }
        ActionEndpoint::Unix(path) => {
            if !path.is_absolute() || permissions.mode & !0o660 != 0 {
                return Err("invalid internal socket path or permissions".into());
            }
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
            }
            let listener = tokio::net::UnixListener::bind(&path).map_err(|e| e.to_string())?;
            let metadata = std::fs::symlink_metadata(&path).map_err(|e| e.to_string())?;
            let owned = OwnedSocket {
                path: path.clone(),
                device: metadata.dev(),
                inode: metadata.ino(),
            };
            if let Some(group) = permissions.group {
                std::os::unix::fs::chown(&path, None, Some(group)).map_err(|e| e.to_string())?;
            }
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(permissions.mode))
                .map_err(|e| e.to_string())?;
            Ok((Bound::Unix(listener), Some(owned)))
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn never_replace_existing_files_or_listeners() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("sentinel");
        std::fs::write(&file, b"keep").unwrap();
        assert!(bind(
            ActionEndpoint::Unix(file.clone()),
            SocketPermissions::default()
        )
        .await
        .is_err());
        assert_eq!(std::fs::read(file).unwrap(), b"keep");
        let socket = dir.path().join("entry.sock");
        let (_listener, owned) = bind(
            ActionEndpoint::Unix(socket.clone()),
            SocketPermissions::default(),
        )
        .await
        .unwrap();
        assert!(bind(
            ActionEndpoint::Unix(socket.clone()),
            SocketPermissions::default()
        )
        .await
        .is_err());
        assert!(tokio::net::UnixStream::connect(&socket).await.is_ok());
        drop(owned);
        assert!(!socket.exists());
    }
    #[tokio::test]
    async fn close_does_not_remove_replaced_socket_path() {
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("entry.sock");
        let (_listener, owned) = bind(
            ActionEndpoint::Unix(socket.clone()),
            SocketPermissions::default(),
        )
        .await
        .unwrap();
        std::fs::remove_file(&socket).unwrap();
        std::fs::write(&socket, b"replacement").unwrap();
        drop(owned);
        assert_eq!(std::fs::read(socket).unwrap(), b"replacement");
    }
    #[tokio::test]
    async fn callers_cannot_bypass_endpoint_validation() {
        assert!(bind(
            ActionEndpoint::Tcp("0.0.0.0:0".parse().unwrap()),
            SocketPermissions::default()
        )
        .await
        .is_err());
        assert!(ActionEndpoint::parse("unix://relative").is_err());
    }
}
