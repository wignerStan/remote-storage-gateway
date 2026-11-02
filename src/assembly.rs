//! Process assembly selects and installs infra owners before opening the data path.
pub mod control;

use crate::{
    config::{CallbackBackend, CallbackBinding, Config},
    native_notifier::{FRAME_LIMIT, NativeNotifier},
    proxy::{NativeProxy, ProxyContract},
};
use plugin_abi::{Expected, Limits};
use plugin_lifecycle::{Composition, Resource, ResourceSchema, Reversal};
use plugin_loading::{Callback, CallbackOptions};
use proxy_pingora::worker::{WorkerControl, surface::Callbacks};
use std::sync::Arc;

const NOTIFIER_MEMORY: Resource = Resource {
    id: "notifier-memory",
    provides: &["notifier-memory"],
    requires: &[],
};
const REMOTE_AXES: Resource = Resource {
    id: "remote-axes",
    provides: &["remote-axis-bindings"],
    requires: &[],
};
const PLUGIN_CONTROL: Resource = Resource {
    id: "plugin-control",
    provides: &["plugin-control-routes"],
    requires: &[plugin_lifecycle::ResourceRequirement {
        resource: "remote-axes",
        capability: "remote-axis-bindings",
    }],
};
const SHARED_PLUGIN_ROUTES: Resource = Resource {
    id: "plugin-control",
    provides: &["plugin-control-routes"],
    requires: &[
        plugin_lifecycle::ResourceRequirement {
            resource: "remote-axes",
            capability: "remote-axis-bindings",
        },
        plugin_lifecycle::ResourceRequirement {
            resource: "management-control",
            capability: "management-control",
        },
    ],
};
const NOTIFIERS: Resource = Resource {
    id: "native-notifiers",
    provides: &["worker-notifiers"],
    requires: &[
        plugin_lifecycle::ResourceRequirement {
            resource: "remote-axes",
            capability: "remote-axis-bindings",
        },
        plugin_lifecycle::ResourceRequirement {
            resource: "notifier-memory",
            capability: "notifier-memory",
        },
    ],
};
const RETAINED_SESSIONS: Resource = Resource {
    id: "retained-sessions",
    provides: &["retained-sessions"],
    requires: &[plugin_lifecycle::ResourceRequirement {
        resource: "native-notifiers",
        capability: "worker-notifiers",
    }],
};
const MANAGEMENT: Resource = Resource {
    id: "management-control",
    provides: &["management-control"],
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
            resource: "management-control",
            capability: "management-control",
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
    let health = _logging.health();
    let capture_budget = _logging.capture_budget();
    proxy_pingora::ProxyFacility::run_proxy_with_options(listener, options, move || {
        assemble_selected(config, default_codecs(), Some(health), capture_budget)
    })
    .map_err(|error| error.to_string())
}

pub async fn assemble(config: Config) -> Result<NativeProxy, String> {
    assemble_with_codecs(config, default_codecs()).await
}

fn default_codecs() -> Vec<Arc<dyn proxy_pingora::http_ingress::message_codec::CodecPlugin>> {
    let mut codecs = llm_message_codec::builtins();
    codecs.push(Arc::new(
        proxy_pingora::http_ingress::message_codec::WebSocketCodec,
    ));
    codecs
}

/// Install trusted host codec implementations before admitting native callbacks.
pub async fn assemble_with_codecs(
    config: Config,
    codecs: Vec<Arc<dyn proxy_pingora::http_ingress::message_codec::CodecPlugin>>,
) -> Result<NativeProxy, String> {
    assemble_selected(
        config,
        codecs,
        None,
        diagnostic_output::runtime::default_capture_budget(),
    )
    .await
}

async fn assemble_selected(
    config: Config,
    mut codecs: Vec<Arc<dyn proxy_pingora::http_ingress::message_codec::CodecPlugin>>,
    logging_health: Option<diagnostic_output::runtime::LoggingHealth>,
    capture_budget: Arc<admission_gate::budget::ResourceBudget>,
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
    let shutdown_grace = std::time::Duration::from_secs(config.shutdown_seconds);
    let control = Arc::new(WorkerControl::default());
    let operations = Arc::new(diagnostic_output::operations::Operations::default());
    let mut resources = if config.callbacks.iter().any(|c| !c.actions.is_empty()) {
        vec![
            NOTIFIER_MEMORY,
            REMOTE_AXES,
            NOTIFIERS,
            MANAGEMENT,
            NATIVE_ACTIONS,
        ]
    } else if config.management.is_some() {
        vec![NOTIFIER_MEMORY, REMOTE_AXES, NOTIFIERS, MANAGEMENT]
    } else {
        vec![NOTIFIER_MEMORY, REMOTE_AXES, NOTIFIERS]
    };
    resources.push(RETAINED_SESSIONS);
    if let Some(ingress) = &config.plugin_ingress {
        resources.push(if ingress.shared() {
            SHARED_PLUGIN_ROUTES
        } else {
            PLUGIN_CONTROL
        });
    }
    let surface = crate::remote_axis::publication(&config.callbacks)?;
    let mut composition = Composition::new(ResourceSchema::new(&resources)?);
    let memory = Arc::new(plugin_lifecycle::coordination::memory::Coordinator::new(
        plugin_lifecycle::coordination::memory::Limits::default(),
    )?);
    let memory_owner = Arc::clone(&memory);
    composition
        .install(NOTIFIER_MEMORY, async move {
            let closing = Arc::clone(&memory_owner);
            let inverse: Reversal = Box::new(move || {
                Box::pin(async move { closing.close().await.map_err(|errors| errors.join("; ")) })
            });
            Ok((memory_owner, inverse))
        })
        .await?;
    let (remote_send, remote_receive) = tokio::sync::oneshot::channel();
    composition
        .install(REMOTE_AXES, async move {
            let axes = Arc::new(
                crate::remote_axis::RemotePluginManager::connect(
                    config.axis_bindings,
                    surface,
                    inspection.max_requests,
                )
                .await?,
            );
            let closing = Arc::clone(&axes);
            let _ = remote_send.send(Arc::clone(&axes));
            let inverse: Reversal =
                Box::new(move || Box::pin(async move { closing.close().await }));
            Ok((axes, inverse))
        })
        .await?;
    let remote_axes = remote_receive
        .await
        .map_err(|_| "remote axis setup cancelled")?;
    let callback_axes = Arc::clone(&remote_axes);
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
            for (index, (callback, input)) in callbacks.into_iter().enumerate() {
                let subscriber = match memory
                    .subscribe(plugin_registry::RegistrationSpec {
                        name: format!("callback-{index}"),
                        contract: "worker.metadata.v1".into(),
                    })
                    .await
                {
                    Ok(subscriber) => subscriber,
                    Err(error) => {
                        let _ = native.shutdown().await;
                        return Err(error);
                    }
                };
                if callback.resolved_binding() == CallbackBinding::Remote {
                    let binding = callback_axes.binding(
                        callback.axis.as_deref().ok_or("remote axis is absent")?,
                        callback
                            .registration
                            .as_deref()
                            .ok_or("remote registration is absent")?,
                    )?;
                    if let Some(codec) = &callback.message_codec {
                        native.messages.push((
                            codec.clone(),
                            crate::proxy::message_codec::MessageCallback::remote(
                                binding.clone(),
                                std::time::Duration::from_millis(callback.timeout_ms),
                                callback.message_responses,
                            )
                            .with_actions(
                                callback.message_actions,
                                callback.message_proxy,
                                crate::native_notifier::validate_patch_headers(
                                    &callback.patch_headers,
                                )?,
                            )
                            .with_http_bridge(callback.message_http_bridge),
                        ));
                    }
                    if callback.hooks.is_empty() {
                        continue;
                    }
                    match NativeNotifier::remote(binding, &callback) {
                        Ok(notifier) => native
                            .callbacks
                            .push(Arc::new(notifier.with_subscriber(subscriber))),
                        Err(error) => {
                            let _ = native.shutdown().await;
                            return Err(error);
                        }
                    }
                    continue;
                }
                let (language, unload) = callback.language.native()?;
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
                    actions.push(action_execution::Action {
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
                        )
                        .with_http_bridge(callback.message_http_bridge),
                    ));
                }
                if callback.hooks.is_empty() {
                    continue;
                }
                match NativeNotifier::new(instance, &callback) {
                    Ok(notifier) => native
                        .callbacks
                        .push(Arc::new(notifier.with_subscriber(subscriber))),
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
    let server = if let Some(management) = config.management {
        let installed = control::install(
            &mut composition,
            MANAGEMENT,
            control::Registration {
                protocol: management.protocol,
                worker_control: Some(Arc::clone(&control)),
                logging_health,
                endpoint: network_sockets::LocalEndpoint::parse(&management.listen)?,
                permissions: network_sockets::SocketPermissions::default(),
            },
            Arc::clone(&operations),
        )
        .await;
        match installed {
            Ok(server) => Some(server),
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
    if let Some(server) = &server
        && !actions.is_empty()
        && let Err(error) = server
            .install_actions(&mut composition, NATIVE_ACTIONS, "native", actions)
            .await
    {
        let cleanup = composition.rollback().await;
        return Err(format!(
            "native action publication failed: {error}; cleanup: {cleanup:?}"
        ));
    }
    let plugin_server = match config.plugin_ingress {
        Some(crate::config::PluginControl::Shared { .. }) => {
            let installed = server
                .as_ref()
                .ok_or("shared plugin control requires management facility")?
                .install_actions(
                    &mut composition,
                    SHARED_PLUGIN_ROUTES,
                    "plugin",
                    crate::entry::plugins::actions(Arc::clone(&remote_axes)),
                )
                .await;
            if let Err(error) = installed {
                let cleanup = composition.rollback().await;
                return Err(format!(
                    "plugin publication failed: {error}; cleanup: {cleanup:?}"
                ));
            }
            None
        }
        Some(crate::config::PluginControl::Dedicated { listen }) => match control::install_plugins(
            &mut composition,
            PLUGIN_CONTROL,
            listen,
            Arc::clone(&remote_axes),
        )
        .await
        {
            Ok(server) => Some(server),
            Err(error) => {
                let cleanup = composition.rollback().await;
                return Err(format!(
                    "plugin ingress setup failed: {error}; cleanup: {cleanup:?}"
                ));
            }
        },
        None => None,
    };
    let retained_sessions = crate::proxy::retained_sessions::RetainedSessions::new(
        inspection.max_requests,
        shutdown_grace,
    );
    let installing = Arc::clone(&retained_sessions);
    composition
        .install(RETAINED_SESSIONS, async move {
            let closing = Arc::clone(&installing);
            let inverse: Reversal =
                Box::new(move || Box::pin(async move { closing.close().await }));
            Ok((installing, inverse))
        })
        .await?;
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
    if let Some(server) = server
        && let Err(error) = server.activate()
    {
        let _ = runtime.shutdown().await;
        return Err(error);
    }
    if let Some(server) = plugin_server
        && let Err(error) = server.activate()
    {
        let _ = runtime.shutdown().await;
        return Err(error);
    }
    Ok(proxy
        .with_remote_plugins(remote_axes)
        .with_operations(operations)
        .with_inspection_limits(inspection)
        .with_message_callbacks(messages, codecs)
        .with_body_dump_limit(body_limit)
        .with_body_dump_budget(capture_budget)
        .with_retained_sessions(retained_sessions)
        .with_runtime(runtime))
}
