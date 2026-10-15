//! Composition-selected actions use the infra publication owner and its inverse.
use super::ControlServer;
use action_execution::{Action, Error};
use action_router::{Publication, PublishedRoutes, Router};
use plugin_lifecycle::{Composition, Provider, Resource, Reversal};
use plugin_loading::{Graph, InstanceActions};
use std::sync::Arc;

impl ControlServer {
    /// Install compiled/script actions as an independently closed publication.
    pub async fn install_actions(
        &self,
        composition: &mut Composition,
        resource: Resource,
        namespace: &str,
        actions: Vec<Action>,
    ) -> Result<Provider<PublishedRoutes>, String> {
        self.install_publication(composition, resource, namespace, move |router| {
            router
                .register(actions)
                .map(|routes| Arc::new(routes) as Arc<dyn Publication>)
        })
        .await
    }
    /// Publish native actions without transferring the entire graph's ownership.
    pub async fn install_native_actions(
        &self,
        composition: &mut Composition,
        resource: Resource,
        namespace: &str,
        graph: Arc<Graph>,
        actions: Vec<InstanceActions>,
    ) -> Result<Provider<PublishedRoutes>, String> {
        self.install_publication(composition, resource, namespace, move |router| {
            graph
                .register_actions(&router, actions)
                .map(|routes| Arc::new(routes) as Arc<dyn Publication>)
                .map_err(|error| Error::Failed(error.to_string()))
        })
        .await
    }
    async fn install_publication<F>(
        &self,
        composition: &mut Composition,
        resource: Resource,
        namespace: &str,
        publish: F,
    ) -> Result<Provider<PublishedRoutes>, String>
    where
        F: FnOnce(Router) -> Result<Arc<dyn Publication>, Error> + Send + 'static,
    {
        let namespace = namespace.to_owned();
        let facility = self.facility.clone();
        composition
            .install(resource, async move {
                let routes = Arc::new(
                    facility
                        .publish(&namespace, publish)
                        .await
                        .map_err(|error| error.to_string())?,
                );
                let owner = Arc::clone(&routes);
                let inverse: Reversal = Box::new(move || {
                    Box::pin(async move { owner.close().await.map_err(|error| error.to_string()) })
                });
                Ok((routes, inverse))
            })
            .await
    }
}
