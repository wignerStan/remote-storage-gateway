//! Process assembly selects and installs infra owners before opening the data path.
use crate::{
    config::{Config, Language},
    native_notifier::{FRAME_LIMIT, NativeNotifier},
    proxy::{NativeProxy, ProxyContract},
};
use plugin_abi::{Expected, Limits, UnloadPolicy};
use plugin_lifecycle::{Composition, Resource, ResourceSchema, Reversal};
use plugin_loading::Callback;
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
struct NativeSet {
    callbacks: Vec<Arc<dyn Callbacks<ProxyContract>>>,
    instances: Vec<Arc<Callback>>,
    messages: Vec<(String, crate::proxy::message_codec::MessageCallback)>,
}
impl NativeSet {
    async fn shutdown(&self) -> Result<(), String> {
        let mut errors = Vec::new();
        for instance in self.instances.iter().rev() {
            if let Err(error) = instance.shutdown().await {
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
    codecs: Vec<Arc<dyn proxy_pingora::http_ingress::message_codec::CodecPlugin>>,
) -> Result<NativeProxy, String> {
    config.validate()?;
    let codecs = crate::proxy::message_codec::resolve_all(
        &codecs,
        config
            .callbacks
            .iter()
            .filter_map(|callback| callback.message_codec.clone()),
    )?;
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
                let result = unsafe {
                    // The configuration explicitly selects trusted native code. Infra
                    // validates its exact identity and owns every call and cleanup lease.
                    Callback::load(
                        callback.path.clone(),
                        expected,
                        Limits {
                            max_input: if callback.message_codec.is_some() {
                                crate::proxy::message_codec::MESSAGE_LIMIT
                            } else {
                                FRAME_LIMIT
                            },
                            max_reply: FRAME_LIMIT,
                            max_inflight: callback.max_inflight,
                        },
                        input,
                        callback.max_inflight,
                    )
                    .await
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
                if let Some(codec) = &callback.message_codec {
                    native.messages.push((
                        codec.clone(),
                        crate::proxy::message_codec::MessageCallback::new(
                            Arc::clone(&instance),
                            std::time::Duration::from_millis(callback.timeout_ms),
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
        .with_message_callbacks(messages, codecs)
        .with_body_dump_limit(body_limit)
        .with_runtime(runtime))
}
