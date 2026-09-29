//! Generic worker composition, independent of provider business and release policy.
pub mod composition;
pub mod entry;
pub mod management;
mod process;
pub use process::Process;
pub use proxy_pingora::http_ingress::{Handler, ListenerConfig};
use std::{future::Future, sync::Arc};

/// Run the installed plugin composition on the native Pingora data path.
pub fn run<A, F, Fut>(config: ListenerConfig, install: F) -> Result<(), Box<dyn std::error::Error>>
where
    A: proxy_pingora::http_ingress::Application,
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<Arc<A>, String>>,
{
    proxy_pingora::ProxyFacility::run_http(config, install)
}
