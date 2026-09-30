//! Process assembly selects and installs infra owners before opening the data path.
use crate::{
    config::{Config, Language},
    native_notifier::{NativeNotifier, FRAME_LIMIT},
    proxy::{NativeProxy, ProxyContract},
};
use plugin_abi::{Expected, Limits, PluginType, UnloadPolicy};
use plugin_lifecycle::{Composition, Resource, ResourceSchema, Reversal};
use plugin_loading::Function;
use proxy_pingora::worker::{surface::Callbacks, WorkerControl};
use std::sync::Arc;

const NOTIFIERS: Resource = Resource {
    id: "native-notifiers",
    provides: &["worker-notifiers"],
    requires: &[],
};
const MANAGEMENT: Resource = Resource {
    id: "management-entry",
    provides: &["management-entry"],
    requires: &[],
};
struct NativeSet {
    callbacks: Vec<Arc<dyn Callbacks<ProxyContract>>>,
    functions: Vec<Arc<Function>>,
}
impl NativeSet {
    async fn shutdown(&self) -> Result<(), String> {
        let mut errors = Vec::new();
        for function in self.functions.iter().rev() {
            if let Err(error) = function.shutdown().await {
                errors.push(error);
            }
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors.join("; "))
        }
    }
}

pub fn run(config: Config) -> Result<(), String> {
    config.validate()?;
    let _logging = diagnostic_output::runtime::init_logging(config.logging())
        .map_err(|error| format!("logging initialization failed: {error}"))?;
    let listener = config.listener();
    proxy_pingora::ProxyFacility::run_proxy(listener, move || assemble(config))
        .map_err(|error| error.to_string())
}

pub async fn assemble(config: Config) -> Result<NativeProxy, String> {
    config.validate()?;
    let proxy_config = config.proxy_config()?;
    let body_limit = config.logging.body_max_bytes;
    let control = Arc::new(WorkerControl::default());
    let resources = if config.management.is_some() {
        vec![NOTIFIERS, MANAGEMENT]
    } else {
        vec![NOTIFIERS]
    };
    let mut composition = Composition::new(ResourceSchema::new(&resources)?);
    let callbacks = config
        .callbacks
        .into_iter()
        .map(|callback| {
            let input = text_processing::json::to_vec_bounded(&callback.config, FRAME_LIMIT)
                .map_err(|_| "invalid native initialization metadata")?;
            Ok((callback, input))
        })
        .collect::<Result<Vec<_>, &str>>()?;
    composition
        .install(NOTIFIERS, async move {
            let mut native = NativeSet {
                callbacks: Vec::new(),
                functions: Vec::new(),
            };
            for (callback, input) in callbacks {
                let language = match callback.language {
                    Language::Rust => plugin_abi::Language::Rust,
                    Language::Go => plugin_abi::Language::Go,
                };
                let unload = match callback.language {
                    Language::Rust => UnloadPolicy::Explicit,
                    Language::Go => UnloadPolicy::Retain,
                };
                let expected = Expected::v1(
                    &callback.package,
                    &callback.variant,
                    PluginType::Function,
                    language,
                    unload,
                );
                let result = unsafe {
                    // The configuration explicitly selects trusted native code. Infra
                    // validates its exact identity and owns every call and cleanup lease.
                    Function::load(
                        callback.path.clone(),
                        expected,
                        Limits {
                            max_input: FRAME_LIMIT,
                            max_reply: FRAME_LIMIT,
                            max_inflight: callback.max_inflight,
                        },
                        input,
                        callback.max_inflight,
                    )
                    .await
                };
                let function = match result {
                    Ok(function) => Arc::new(function),
                    Err(_) => {
                        let cleanup = native.shutdown().await;
                        return Err(if cleanup.is_ok() {
                            "native notifier initialization failed".into()
                        } else {
                            "native notifier initialization and cleanup failed".into()
                        });
                    }
                };
                native.functions.push(Arc::clone(&function));
                match NativeNotifier::new(function, &callback) {
                    Ok(notifier) => native.callbacks.push(Arc::new(notifier)),
                    Err(error) => {
                        let _ = native.shutdown().await;
                        return Err(error);
                    }
                }
            }
            let native = Arc::new(native);
            let owner = Arc::clone(&native);
            let inverse: Reversal =
                Box::new(move || Box::pin(async move { owner.shutdown().await }));
            Ok((native, inverse))
        })
        .await?;
    let entry = if let Some(management) = config.management {
        Some(
            crate::entry::install(
                &mut composition,
                MANAGEMENT,
                crate::entry::Registration {
                    worker_control: Some(Arc::clone(&control)),
                    endpoint: network_sockets::LocalEndpoint::Tcp(management.listen),
                    permissions: network_sockets::SocketPermissions::default(),
                },
            )
            .await?,
        )
    } else {
        None
    };
    let runtime = composition.commit().await?;
    let native = runtime.require::<NativeSet>(NOTIFIERS.id)?.enter()?;
    let callbacks = native.callbacks.clone();
    drop(native);
    let proxy = match NativeProxy::new(proxy_config, control, callbacks) {
        Ok(proxy) => proxy,
        Err(error) => {
            let _ = runtime.shutdown().await;
            return Err(error);
        }
    };
    if let Some(entry) = entry {
        if let Err(error) = entry.activate() {
            let _ = runtime.shutdown().await;
            return Err(error);
        }
    }
    Ok(proxy.with_body_dump_limit(body_limit).with_runtime(runtime))
}
