//! Process lifecycle adapter. The installed runtime owns teardown of all effects;
//! the worker receives only its data-path capabilities and local configuration.
use pingora_core::{apps::ReusedHttpStream, protocols::http::ServerSession, server::ShutdownWatch};
use plugin_lifecycle::Runtime;
use std::sync::Arc;

pub struct Process<H: proxy_pingora::http_ingress::Handler> {
    worker: Arc<proxy_pingora::http_ingress::Worker<H>>,
    runtime: Runtime,
}
impl<H: proxy_pingora::http_ingress::Handler> Process<H> {
    pub fn new(worker: Arc<proxy_pingora::http_ingress::Worker<H>>, runtime: Runtime) -> Self {
        Self { worker, runtime }
    }
    /// Close one mounted axis while preserving the listener and independent axes.
    pub async fn close_axis(&self, name: &str) -> Result<(), Vec<String>> {
        self.runtime.require::<Runtime>(name).map_err(|e| vec![e])?;
        self.runtime.close_resources(&[name]).await
    }
}
#[async_trait::async_trait]
impl<H: proxy_pingora::http_ingress::Handler> proxy_pingora::http_ingress::Application
    for Process<H>
{
    async fn serve(
        self: &Arc<Self>,
        session: ServerSession,
        shutdown: &ShutdownWatch,
    ) -> Option<ReusedHttpStream> {
        self.worker.serve(session, shutdown).await
    }
    async fn maintain(&self) {
        self.worker.maintain().await;
    }
    async fn shutdown(&self) {
        if let Err(errors) = self.runtime.shutdown().await {
            tracing::error!(?errors, "plugin shutdown failed");
        }
    }
}
