//! Process assembly selects and installs infra owners before opening the data path.
use crate::{
    config::{CallbackBackend, Config, Language},
    native_notifier::{FRAME_LIMIT, NativeNotifier},
    proxy::{NativeProxy, ProxyContract},
};
use plugin_abi::{Expected, Limits, UnloadPolicy};
use plugin_lifecycle::{Composition, Resource, ResourceSchema, Reversal};
use plugin_loading::{Callback, CallbackOptions};
use proxy_pingora::worker::{WorkerControl, surface::Callbacks};
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
const NATIVE_ACTIONS: Resource = Resource {
    id: "native-actions",
    provides: &["native-actions"],
    requires: &[
        plugin_lifecycle::ResourceRequirement {
            resource: "native-notifiers",
            capability: "worker-notifiers",
        },
        plugin_lifecycle::ResourceRequirement {
            resource: "management-entry",
            capability: "management-entry",
        },
    ],
};
struct NativeSet {
    callbacks: Vec<Arc<dyn Callbacks<ProxyContract>>>,
    instances: Vec<Arc<Callback>>,
    messages: Vec<(String, crate::proxy::message_codec::MessageCallback)>,
}
impl NativeSet {
    async fn shutdown(&self) -> Result<(), String> {
        for instance in &self.instances {
            instance.withdraw();
        }
        let mut pending = tokio::task::JoinSet::new();
        for instance in &self.instances {
            let instance = Arc::clone(instance);
            pending.spawn(async move { instance.shutdown().await });
        }
        let mut errors = Vec::new();
        while let Some(result) = pending.join_next().await {
            match result {
                Ok(Ok(())) => {}
                Ok(Err(error)) => errors.push(error),
                Err(error) => errors.push(error.to_string()),
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
    let options = config.process_options(false)?;
    run_with_options(config, options)
}

pub fn run_with_options(
    config: Config,
    options: proxy_pingora::http_ingress::ProcessOptions,
) -> Result<(), String> {
    config.validate()?;
    options.validate()?;
    let _logging = diagnostic_output::runtime::init_logging(config.logging())
        .map_err(|error| format!("logging initialization failed: {error}"))?;
    let listener = config.listener();
    proxy_pingora::ProxyFacility::run_proxy_with_options(listener, options, move || {
        assemble(config)
    })
    .map_err(|error| error.to_string())
}

pub async fn assemble(config: Config) -> Result<NativeProxy, String> {
    assemble_with_codecs(config, {
        let mut codecs = llm_protocols::codec::builtins();
        codecs.push(Arc::new(
            proxy_pingora::http_ingress::message_codec::WebSocketCodec,
        ));
        codecs
    })
    .await
}

/// Install trusted host codec implementations before admitting native callbacks.
pub async fn assemble_with_codecs(
    config: Config,
    mut codecs: Vec<Arc<dyn proxy_pingora::http_ingress::message_codec::CodecPlugin>>,
) -> Result<NativeProxy, String> {
    config.validate()?;
    for callback in &config.callbacks {
        if let Some(descriptor) = &callback.codec_descriptor {
            codecs.push(Arc::new(
                proxy_pingora::http_ingress::message_codec::RegisteredCodec::new(
                    descriptor.clone(),
                )?,
            ));
        }
    }
    let codecs = crate::proxy::message_codec::resolve_all(
        &codecs,
        config
            .callbacks
            .iter()
            .filter_map(|callback| callback.message_codec.clone()),
    )?;
    let proxy_config = config.proxy_config()?;
    let body_limit = config.logging.body_max_bytes;
    let inspection = config.inspection;
    let control = Arc::new(WorkerControl::default());
    let resources = if config.callbacks.iter().any(|c| !c.actions.is_empty()) {
        vec![NOTIFIERS, MANAGEMENT, NATIVE_ACTIONS]
    } else if config.management.is_some() {
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
    let (actions_send, actions_receive) = tokio::sync::oneshot::channel();
    composition
        .install(NOTIFIERS, async move {
            let mut actions = Vec::new();
            let mut native = NativeSet {
                callbacks: Vec::new(),
                instances: Vec::new(),
                messages: Vec::new(),
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
                let kind = callback.plugin_kind();
                let expected =
                    Expected::v1(&callback.package, &callback.variant, kind, language, unload);
                let options = CallbackOptions {
                    scheduling: match callback.scheduling {
                        crate::config::CallbackScheduling::Blocking => {
                            plugin_loading::CallbackScheduling::Blocking
                        }
                        crate::config::CallbackScheduling::Inline => {
                            plugin_loading::CallbackScheduling::Inline
                        }
                    },
                    max_inflight: callback.max_inflight,
                    max_waiting: callback.max_waiting,
                    instances: callback.instances,
                    startup_timeout: std::time::Duration::from_millis(callback.startup_timeout_ms),
                    shutdown_timeout: std::time::Duration::from_millis(
                        callback.shutdown_timeout_ms,
                    ),
                };
                let mut limits = Limits {
                    max_input: if callback.message_codec.is_some() {
                        inspection.max_message_bytes.max(FRAME_LIMIT)
                    } else {
                        FRAME_LIMIT
                    },
                    max_reply: callback
                        .actions
                        .iter()
                        .map(|a| a.contract.max_output)
                        .max()
                        .unwrap_or(0)
                        .max(FRAME_LIMIT),
                    max_inflight: callback.max_inflight,
                };
                if callback.message_actions {
                    limits.max_reply = limits
                        .max_reply
                        .max(inspection.max_message_bytes + FRAME_LIMIT + 5);
                }
                limits.max_input = limits.max_input.max(
                    callback
                        .actions
                        .iter()
                        .map(|a| a.contract.max_input)
                        .max()
                        .unwrap_or(0),
                );
                // Notifier participants share this process. The plugin runtime owns
                // admission, instance affinity, cancellation pins and cleanup.
                let result = match callback.backend {
                    CallbackBackend::Native => unsafe {
                        if let Some(graph) = &callback.graph {
                            Callback::load_graph_with_options(
                                graph.runtime()?,
                                graph.callback_instance.clone(),
                                limits,
                                options,
                            )
                            .await
                        } else {
                            Callback::load_with_options(
                                callback.path.clone(),
                                expected,
                                limits,
                                input,
                                options,
                            )
                            .await
                        }
                    },
                    CallbackBackend::Wasm => {
                        #[cfg(feature = "wasm")]
                        {
                            Callback::load_wasm(
                                callback.path.clone(),
                                expected,
                                limits,
                                input,
                                options,
                                callback.wasm_limits.unwrap_or_default().sandbox(),
                            )
                            .await
                        }
                        #[cfg(not(feature = "wasm"))]
                        {
                            Err("Wasm callbacks require the worker wasm feature".into())
                        }
                    }
                };
                let instance = match result {
                    Ok(instance) => Arc::new(instance),
                    Err(error) => {
                        let cleanup = native.shutdown().await;
                        return Err(if cleanup.is_ok() {
                            format!("native notifier initialization failed: {error}")
                        } else {
                            format!("native notifier initialization and cleanup failed: {error}")
                        });
                    }
                };
                native.instances.push(Arc::clone(&instance));
                for action in &callback.actions {
                    actions.push(action_router::Action {
                        contract: action.contract.clone(),
                        bindings: action.bindings.clone(),
                        handler: Arc::new(
                            match plugin_loading::CallbackAction::new(
                                Arc::clone(&instance),
                                action.method.clone(),
                                std::time::Duration::from_millis(callback.timeout_ms),
                            ) {
                                Ok(handler) => handler,
                                Err(error) => {
                                    let _ = native.shutdown().await;
                                    return Err(error);
                                }
                            },
                        ),
                    });
                }
                if let Some(codec) = &callback.message_codec {
                    native.messages.push((
                        codec.clone(),
                        crate::proxy::message_codec::MessageCallback::new(
                            Arc::clone(&instance),
                            std::time::Duration::from_millis(callback.timeout_ms),
                            callback.message_responses,
                        )
                        .with_actions(
                            callback.message_actions,
                            callback.message_proxy,
                            crate::native_notifier::validate_patch_headers(
                                &callback.patch_headers,
                            )?,
                        ),
                    ));
                }
                if callback.hooks.is_empty() {
                    continue;
                }
                match NativeNotifier::new(instance, &callback) {
                    Ok(notifier) => native.callbacks.push(Arc::new(notifier)),
                    Err(error) => {
                        let _ = native.shutdown().await;
                        return Err(error);
                    }
                }
            }
            let _ = actions_send.send(actions);
            let native = Arc::new(native);
            let owner = Arc::clone(&native);
            let inverse: Reversal =
                Box::new(move || Box::pin(async move { owner.shutdown().await }));
            Ok((native, inverse))
        })
        .await?;
    let actions = match actions_receive.await {
        Ok(actions) => actions,
        Err(_) => {
            let cleanup = composition.rollback().await;
            return Err(format!(
                "native action setup canceled; cleanup: {cleanup:?}"
            ));
        }
    };
    let entry = if let Some(management) = config.management {
        let installed = crate::entry::install(
            &mut composition,
            MANAGEMENT,
            crate::entry::Registration {
                worker_control: Some(Arc::clone(&control)),
                endpoint: network_sockets::LocalEndpoint::parse(&management.listen)?,
                permissions: network_sockets::SocketPermissions::default(),
            },
        )
        .await;
        match installed {
            Ok(entry) => Some(entry),
            Err(error) => {
                let cleanup = composition.rollback().await;
                return Err(format!(
                    "management setup failed: {error}; cleanup: {cleanup:?}"
                ));
            }
        }
    } else {
        None
    };
    if let Some(entry) = &entry
        && !actions.is_empty()
        && let Err(error) = entry
            .install_actions(&mut composition, NATIVE_ACTIONS, "native", actions)
            .await
    {
        let cleanup = composition.rollback().await;
        return Err(format!(
            "native action publication failed: {error}; cleanup: {cleanup:?}"
        ));
    }
    let runtime = composition.commit().await?;
    let native = runtime.require::<NativeSet>(NOTIFIERS.id)?.enter()?;
    let callbacks = native.callbacks.clone();
    let messages = native.messages.clone();
    drop(native);
    let proxy = match NativeProxy::new(proxy_config, control, callbacks) {
        Ok(proxy) => proxy,
        Err(error) => {
            let _ = runtime.shutdown().await;
            return Err(error);
        }
    };
    if let Some(entry) = entry
        && let Err(error) = entry.activate()
    {
        let _ = runtime.shutdown().await;
        return Err(error);
    }
    Ok(proxy
        .with_inspection_limits(inspection)
        .with_message_callbacks(messages, codecs)
        .with_body_dump_limit(body_limit)
        .with_runtime(runtime))
}
