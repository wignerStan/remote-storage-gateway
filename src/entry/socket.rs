//! Compiled local-listener policy. Socket ownership mechanisms live in infra.
use network_sockets::{
    BoundListener, LocalEndpoint, SocketPermissions, StaleUnixSocketPolicy, UnixSocketLease,
};
pub(super) async fn bind(
    endpoint: LocalEndpoint,
    permissions: SocketPermissions,
) -> Result<(BoundListener, Option<UnixSocketLease>), String> {
    network_sockets::bind_local(endpoint, permissions, StaleUnixSocketPolicy::RemoveIfStale).await
}
