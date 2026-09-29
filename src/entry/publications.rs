//! Per-owner route publication on a shared entry. No worker callback permissions.
use super::SharedEntry;
use action_router::{Action, Router};
use plugin_lifecycle::{Composition, Provider, Resource, Reversal};
use plugin_loading::{Graph, InstanceActions};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

enum Publication {
    Native(plugin_loading::ActionRegistration),
    Local(action_router::Registration),
}
impl Publication {
    fn revoke(&self) {
        match self {
            Self::Native(p) => p.revoke(),
            Self::Local(p) => p.revoke(),
        }
    }
    fn drain(&self) {
        match self {
            Self::Native(p) => p.drain(),
            Self::Local(p) => p.drain(),
        }
    }
}
struct Slot {
    generation: Arc<()>,
    publication: Option<Arc<Publication>>,
}
#[derive(Default)]
pub(super) struct State {
    closed: bool,
    slots: BTreeMap<String, Slot>,
}
impl State {
    pub(super) fn new(worker: Option<action_router::Registration>) -> Arc<Mutex<Self>> {
        let mut state = Self::default();
        if let Some(worker) = worker {
            state.slots.insert(
                "worker".into(),
                Slot {
                    generation: Arc::new(()),
                    publication: Some(Arc::new(Publication::Local(worker))),
                },
            );
        }
        Arc::new(Mutex::new(state))
    }
}
/// An installed axis owns only these routes. Composition owns its inverse.
pub struct AxisRoutes {
    name: String,
    generation: Arc<()>,
    state: Arc<Mutex<State>>,
    publication: Arc<Publication>,
}
impl AxisRoutes {
    /// Close admission without stopping the listener or other axes.
    pub fn revoke(&self) {
        self.publication.revoke();
    }
    async fn close(&self) -> Result<(), String> {
        self.revoke();
        let publication = Arc::clone(&self.publication);
        tokio::task::spawn_blocking(move || publication.drain())
            .await
            .map_err(|e| e.to_string())?;
        remove(&self.state, &self.name, &self.generation);
        Ok(())
    }
}
// Never drop handlers under the registry lock; a destructor can call host code.
fn remove(state: &Mutex<State>, name: &str, generation: &Arc<()>) {
    let removed = {
        let mut state = state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state
            .slots
            .get(name)
            .is_some_and(|slot| Arc::ptr_eq(&slot.generation, generation))
        {
            state.slots.remove(name)
        } else {
            None
        }
    };
    drop(removed);
}
struct Reservation {
    name: String,
    generation: Arc<()>,
    state: Arc<Mutex<State>>,
    committed: bool,
}
impl Drop for Reservation {
    fn drop(&mut self) {
        if !self.committed {
            remove(&self.state, &self.name, &self.generation);
        }
    }
}
impl SharedEntry {
    /// Register compiled or script-adapted actions as one independently closed owner.
    pub async fn install_actions(
        &self,
        composition: &mut Composition,
        resource: Resource,
        namespace: &str,
        actions: Vec<Action>,
    ) -> Result<Provider<AxisRoutes>, String> {
        self.install_publication(composition, resource, namespace, move |router| {
            router
                .register(actions)
                .map(Publication::Local)
                .map_err(|e| e.to_string())
        })
        .await
    }
    /// Register native actions without transferring ownership of the entire graph.
    pub async fn install_native_actions(
        &self,
        composition: &mut Composition,
        resource: Resource,
        namespace: &str,
        graph: Arc<Graph>,
        actions: Vec<InstanceActions>,
    ) -> Result<Provider<AxisRoutes>, String> {
        self.install_publication(composition, resource, namespace, move |router| {
            graph
                .register_actions(&router, actions)
                .map(Publication::Native)
                .map_err(|e| e.to_string())
        })
        .await
    }
    async fn install_publication<F>(
        &self,
        composition: &mut Composition,
        resource: Resource,
        namespace: &str,
        publish: F,
    ) -> Result<Provider<AxisRoutes>, String>
    where
        F: FnOnce(Router) -> Result<Publication, String> + Send + 'static,
    {
        let router = self
            .router
            .with_contract_namespace(namespace)
            .map_err(|e| e.to_string())?;
        let name = namespace.to_owned();
        let state = Arc::clone(&self.publications);
        composition
            .install(resource, async move {
                let generation = Arc::new(());
                {
                    let mut current = state
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    if current.closed {
                        return Err("shared entry is closed".into());
                    }
                    if current.slots.contains_key(&name) {
                        return Err(format!("action namespace already installed: {name}"));
                    }
                    current.slots.insert(
                        name.clone(),
                        Slot {
                            generation: Arc::clone(&generation),
                            publication: None,
                        },
                    );
                }
                let mut reservation = Reservation {
                    name: name.clone(),
                    generation: Arc::clone(&generation),
                    state: Arc::clone(&state),
                    committed: false,
                };
                let publication = Arc::new(publish(router)?);
                let accepted = {
                    let mut current = state
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    if current.closed {
                        false
                    } else {
                        current
                            .slots
                            .get_mut(&name)
                            .expect("owned reservation")
                            .publication = Some(Arc::clone(&publication));
                        true
                    }
                };
                if !accepted {
                    publication.revoke();
                    tokio::task::spawn_blocking(move || publication.drain())
                        .await
                        .map_err(|e| e.to_string())?;
                    return Err("shared entry closed during registration".into());
                }
                reservation.committed = true;
                let routes = Arc::new(AxisRoutes {
                    name,
                    generation,
                    state,
                    publication,
                });
                let owner = Arc::clone(&routes);
                let inverse: Reversal =
                    Box::new(move || Box::pin(async move { owner.close().await }));
                Ok((routes, inverse))
            })
            .await
    }
}
pub(super) fn close_all(
    state: Arc<Mutex<State>>,
) -> impl std::future::Future<Output = Result<(), String>> + Send {
    let publications: Vec<_> = {
        let mut state = state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.closed = true;
        state
            .slots
            .values()
            .filter_map(|slot| slot.publication.clone())
            .collect()
    };
    for publication in &publications {
        publication.revoke();
    }
    async move {
        tokio::task::spawn_blocking(move || {
            for publication in publications {
                publication.drain();
            }
        })
        .await
        .map_err(|e| e.to_string())
    }
}
