//! Axis composition reuses the existing runtime's gates, rollback and dependency order.
use plugin_lifecycle::{Composition, Provider, Resource, Reversal, Runtime};
use std::{future::Future, sync::Arc};

/// An independently composed axis; internal resource names stay local to its runtime.
pub struct AxisRuntime {
    runtime: Runtime,
}
impl AxisRuntime {
    pub fn bind<T: Send + Sync + 'static>(
        &self,
        consumer: &str,
        provider: &str,
        capability: &str,
    ) -> Result<Provider<T>, String> {
        self.runtime.bind(consumer, provider, capability)
    }
}
/// Mount an axis under an explicit parent resource/dependency declaration.
/// Cancelling installation or shutdown only cancels the waiter; existing lifecycle
/// owners finish rollback/draining. Failed inverses keep dependencies quarantined.
pub async fn mount_axis<F>(
    parent: &mut Composition,
    resource: Resource,
    setup: F,
) -> Result<Provider<AxisRuntime>, String>
where
    F: Future<Output = Result<Runtime, String>> + Send + 'static,
{
    parent
        .install(resource, async move {
            let runtime = setup.await?;
            let owner = runtime.clone();
            let inverse: Reversal = Box::new(move || {
                Box::pin(async move { owner.shutdown().await.map_err(|errors| errors.join("; ")) })
            });
            Ok((Arc::new(AxisRuntime { runtime }), inverse))
        })
        .await
}
