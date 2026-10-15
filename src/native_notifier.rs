//! One worker metadata projection and validator for direct and remote bindings.
//! Infra owns native allocations and remote connection generations.
use crate::{
    config::NativeCallback,
    proxy::{ProxyContract, RequestFacts},
};
use network_sockets::{DialRequest, DialTarget, SocketDecision, SocketPhase};
use pingora_http::{RequestHeader, ResponseHeader};
use plugin_loading::Callback;
use proxy_pingora::worker::surface::{
    CallbackHook, Callbacks, DialDecision, PoolDecision, PoolPhase,
};
use serde::{Deserialize, Serialize};
use std::{sync::Arc, time::Duration};

#[cfg(test)]
#[path = "native_notifier_tests.rs"]
mod tests;

pub const FRAME_LIMIT: usize = 64 * 1024;
const HOOKS: &[&str] = &[
    "pre_ingress",
    "pre_routing",
    "authenticate",
    "routing",
    "admit",
    "prepare_request_body",
    "before_upstream_send",
    "upstream_response",
    "prepare_response_body",
    "before_downstream_write",
    "complete",
    "before_dial",
    "pool",
    "socket",
];
pub fn validate_hooks(hooks: &[String]) -> Result<(), String> {
    if hooks.is_empty() || hooks.len() > HOOKS.len() {
        return Err("callback hooks must be nonempty and bounded".into());
    }
    let mut seen = std::collections::BTreeSet::new();
    for hook in hooks {
        if !HOOKS.contains(&hook.as_str()) || !seen.insert(hook) {
            return Err("callback hooks contain an unsupported or duplicate stage".into());
        }
    }
    Ok(())
}

fn protected_header(name: &http::header::HeaderName) -> bool {
    matches!(
        name.as_str(),
        "host"
            | "content-length"
            | "transfer-encoding"
            | "connection"
            | "keep-alive"
            | "proxy-connection"
            | "upgrade"
            | "trailer"
            | "te"
            | "sec-websocket-key"
            | "sec-websocket-accept"
            | "sec-websocket-version"
            | "sec-websocket-extensions"
            | "sec-websocket-protocol"
    )
}

/// Parse the host's bounded write grant; protected transport fields are never grantable.
pub fn validate_patch_headers(names: &[String]) -> Result<Vec<http::header::HeaderName>, String> {
    if names.len() > 64 {
        return Err("callback patch headers must be a bounded list of valid names".into());
    }
    let mut granted = Vec::with_capacity(names.len());
    for name in names {
        let name = http::header::HeaderName::from_bytes(name.as_bytes())
            .map_err(|_| "invalid callback patch header name")?;
        if protected_header(&name) {
            return Err(
                "callback patch headers cannot grant transport or WebSocket negotiation fields"
                    .into(),
            );
        }
        if !granted.contains(&name) {
            granted.push(name);
        }
    }
    Ok(granted)
}

#[derive(Serialize)]
struct Metadata<'a> {
    version: u8,
    hook: &'a str,
    request_id: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    client_ip: Option<std::net::IpAddr>,
    method: &'a str,
    path: &'a str,
    status: Option<u16>,
    upstream_reused: bool,
    completed: bool,
    headers: SelectedHeaders<'a>,
    #[serde(skip_serializing_if = "Option::is_none")]
    network: Option<NetworkMetadata<'a>>,
}
#[derive(Clone, Copy)]
enum HttpMetadata<'a> {
    Request(&'a RequestHeader),
    Response(&'a ResponseHeader),
}
#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum NetworkMetadata<'a> {
    BeforeDial {
        target: TargetMetadata<'a>,
        timeout_ms: u128,
        tcp_nodelay: bool,
    },
    Pool {
        phase: PoolMetadata,
    },
    Socket {
        phase: SocketMetadata,
    },
}
#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum TargetMetadata<'a> {
    Host {
        name: &'a str,
        port: u16,
    },
    Tcp {
        address: std::net::SocketAddr,
    },
    #[cfg(unix)]
    Unix {
        path: &'a std::path::Path,
    },
}
#[derive(Serialize)]
#[serde(tag = "name", rename_all = "snake_case")]
enum PoolMetadata {
    BeforeCheckout,
    BeforeReturn { reused: bool, healthy: bool },
}
#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
enum SocketMetadata {
    BeforeUse,
    BeforeReuse,
}

/// Serialize the allowlist directly into the bounded frame, without collecting
/// an intermediate header list for each callback invocation.
struct SelectedHeaders<'a> {
    source: &'a http::HeaderMap,
    names: &'a [http::header::HeaderName],
}
impl Serialize for SelectedHeaders<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::{Error, SerializeSeq};
        let mut sequence = serializer.serialize_seq(None)?;
        for name in self.names {
            for value in self.source.get_all(name) {
                let value = value.to_str().map_err(|_| {
                    S::Error::custom("selected callback header is not textual metadata")
                })?;
                sequence.serialize_element(&(name.as_str(), value))?;
            }
        }
        sequence.end()
    }
}
#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Decision {
    Continue,
    Accept,
    Deny,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Reply {
    version: u8,
    decision: Decision,
    #[serde(default)]
    set_headers: Vec<(String, String)>,
    #[serde(default)]
    remove_headers: Vec<String>,
}
impl Reply {
    fn check(self, hook: &str, patch_headers: &[http::header::HeaderName]) -> Result<Self, String> {
        if !HOOKS.contains(&hook) {
            return Err("unsupported notifier stage".into());
        }
        let network = matches!(hook, "before_dial" | "pool" | "socket");
        let patchable = !network && !matches!(hook, "authenticate" | "admit" | "complete");
        if !patchable && (!self.set_headers.is_empty() || !self.remove_headers.is_empty()) {
            return Err("this notifier stage does not permit header patches".into());
        }
        if self.version != 1 || self.set_headers.len() + self.remove_headers.len() > 64 {
            return Err("invalid notifier reply version or patch count".into());
        }
        if !network && matches!(self.decision, Decision::Deny) {
            return Err("native notifier denied the request".into());
        }
        for name in self
            .set_headers
            .iter()
            .map(|(name, _)| name)
            .chain(&self.remove_headers)
        {
            let parsed = http::header::HeaderName::from_bytes(name.as_bytes())
                .map_err(|_| "invalid notifier header name")?;
            let reason = if protected_header(&parsed) {
                Some("protected_header")
            } else if !patch_headers.contains(&parsed) {
                Some("ungranted_patch_header")
            } else {
                None
            };
            if let Some(reason) = reason {
                tracing::trace!(target: "proxy_pingora::worker::notifier", hook, header = &parsed.as_str()[..parsed.as_str().len().min(128)], outcome = "error", reason, "native metadata patch rejected");
                return Err("notifier header patch is outside the host grant".into());
            }
        }
        for (_, value) in &self.set_headers {
            http::header::HeaderValue::from_str(value)
                .map_err(|_| "invalid notifier header value")?;
        }
        Ok(self)
    }
    fn no_patch(self) -> Result<Decision, String> {
        if !self.set_headers.is_empty() || !self.remove_headers.is_empty() {
            return Err("this notifier stage does not permit header patches".into());
        }
        Ok(self.decision)
    }
    fn request(self, head: &mut RequestHeader) -> Result<(), String> {
        for name in self.remove_headers {
            head.remove_header(&name);
        }
        for (name, value) in self.set_headers {
            head.insert_header(name, value)
                .map_err(|_| "invalid request header patch")?;
        }
        Ok(())
    }
    fn response(self, head: &mut ResponseHeader) -> Result<(), String> {
        for name in self.remove_headers {
            head.remove_header(&name);
        }
        for (name, value) in self.set_headers {
            head.insert_header(name, value)
                .map_err(|_| "invalid response header patch")?;
        }
        Ok(())
    }
}

enum NotifierBinding {
    Direct(Arc<Callback>),
    Remote(crate::remote_axis::RemotePluginBinding),
}

pub struct NativeNotifier {
    callback: NotifierBinding,
    interests: u16,
    timeout: Duration,
    headers: Vec<http::header::HeaderName>,
    patch_headers: Arc<[http::header::HeaderName]>,
}
impl NativeNotifier {
    pub fn new(callback: Arc<Callback>, config: &NativeCallback) -> Result<Self, String> {
        Self::with_binding(NotifierBinding::Direct(callback), config)
    }
    pub(crate) fn remote(
        binding: crate::remote_axis::RemotePluginBinding,
        config: &NativeCallback,
    ) -> Result<Self, String> {
        Self::with_binding(NotifierBinding::Remote(binding), config)
    }
    fn with_binding(callback: NotifierBinding, config: &NativeCallback) -> Result<Self, String> {
        validate_hooks(&config.hooks)?;
        let patch_headers = validate_patch_headers(&config.patch_headers)?.into();
        let interests = config.hooks.iter().fold(0, |bits, hook| {
            bits | (1 << HOOKS.iter().position(|known| known == hook).unwrap())
        });
        let mut headers = Vec::with_capacity(config.headers.len());
        for name in &config.headers {
            let name = http::header::HeaderName::from_bytes(name.as_bytes())
                .map_err(|_| "invalid callback header name".to_string())?;
            if !headers.contains(&name) {
                headers.push(name);
            }
        }
        Ok(Self {
            callback,
            interests,
            timeout: Duration::from_millis(config.timeout_ms),
            headers,
            patch_headers,
        })
    }
    fn interested(&self, hook: &str) -> bool {
        HOOKS
            .iter()
            .position(|known| *known == hook)
            .is_some_and(|index| self.interests & (1 << index) != 0)
    }
    async fn invoke(&self, hook: &'static str, facts: &RequestFacts) -> Result<Reply, String> {
        self.invoke_headers(hook, facts, &facts.headers).await
    }
    async fn invoke_headers(
        &self,
        hook: &'static str,
        facts: &RequestFacts,
        source: &http::HeaderMap,
    ) -> Result<Reply, String> {
        self.invoke_metadata(hook, facts, source, None, None).await
    }
    async fn invoke_metadata(
        &self,
        hook: &'static str,
        facts: &RequestFacts,
        source: &http::HeaderMap,
        network: Option<NetworkMetadata<'_>>,
        head: Option<HttpMetadata<'_>>,
    ) -> Result<Reply, String> {
        if !HOOKS.contains(&hook) {
            return Err("unsupported notifier stage".into());
        }
        if !self.interested(hook) {
            tracing::trace!(target: "proxy_pingora::worker::notifier", hook, outcome = "error", reason = "unregistered_hook", "native metadata invocation rejected");
            return Err("notifier stage is outside the host grant".into());
        }
        let metadata = Metadata {
            version: 1,
            hook,
            request_id: facts.request_id,
            client_ip: facts.client_ip,
            method: match head {
                Some(HttpMetadata::Request(request)) => request.method.as_str(),
                _ => &facts.method,
            },
            path: match head {
                Some(HttpMetadata::Request(request)) => request.uri.path(),
                _ => &facts.path,
            },
            status: match head {
                Some(HttpMetadata::Response(response)) => Some(response.status.as_u16()),
                _ => facts.status,
            },
            upstream_reused: facts.upstream_reused,
            completed: facts.completed,
            network,
            headers: SelectedHeaders {
                source,
                names: &self.headers,
            },
        };
        let input = text_processing::json::to_vec_bounded(&metadata, FRAME_LIMIT)
            .map_err(|_| "could not encode bounded notifier metadata")?;

        let patch_headers = Arc::clone(&self.patch_headers);
        let result = match &self.callback {
            NotifierBinding::Direct(callback) => callback
                .call_with_reply(hook, input, self.timeout, (), move |bytes| {
                    serde_json::from_slice::<Reply>(bytes)
                        .map_err(|_| "invalid notifier metadata reply".to_string())
                        .and_then(|reply| reply.check(hook, &patch_headers))
                })
                .await
                .map_err(|_| "native notifier call failed".to_string())?,
            NotifierBinding::Remote(binding) => {
                let bytes = binding
                    .call(hook, input, self.timeout)
                    .await
                    .map_err(|error| format!("remote metadata call failed: {error}"))?;
                serde_json::from_slice::<Reply>(&bytes)
                    .map_err(|_| "invalid notifier metadata reply".to_string())
                    .and_then(|reply| reply.check(hook, &patch_headers))
            }
        };
        let outcome = match &result {
            Ok(reply) => match reply.decision {
                Decision::Continue => "continue",
                Decision::Accept => "accept",
                Decision::Deny => "deny",
            },
            Err(_) => "error",
        };
        tracing::trace!(target: "proxy_pingora::worker::notifier", hook, outcome, "native metadata decision");
        result
    }
    async fn request_hook(
        &self,
        hook: &'static str,
        request: &mut RequestHeader,
        facts: &mut RequestFacts,
    ) -> Result<(), String> {
        self.invoke_metadata(
            hook,
            facts,
            &request.headers,
            None,
            Some(HttpMetadata::Request(request)),
        )
        .await?
        .request(request)
    }
    async fn response_hook(
        &self,
        hook: &'static str,
        response: &mut ResponseHeader,
        facts: &RequestFacts,
    ) -> Result<(), String> {
        self.invoke_metadata(
            hook,
            facts,
            &response.headers,
            None,
            Some(HttpMetadata::Response(response)),
        )
        .await?
        .response(response)
    }
}
#[async_trait::async_trait]
impl Callbacks<ProxyContract> for NativeNotifier {
    fn supports(&self, hook: CallbackHook) -> bool {
        let name = match hook {
            CallbackHook::PreIngress => "pre_ingress",
            CallbackHook::PreRouting => "pre_routing",
            CallbackHook::Authenticate => "authenticate",
            CallbackHook::Routing => "routing",
            CallbackHook::Admit => "admit",
            CallbackHook::PrepareRequestBody => "prepare_request_body",
            CallbackHook::BeforeUpstreamSend => "before_upstream_send",
            CallbackHook::UpstreamResponse => "upstream_response",
            CallbackHook::PrepareResponseBody => "prepare_response_body",
            CallbackHook::BeforeDownstreamWrite => "before_downstream_write",
            CallbackHook::BeforeDial => "before_dial",
            CallbackHook::Pool => "pool",
            CallbackHook::Socket => "socket",
            _ => return false,
        };
        self.interested(name)
    }
    async fn pre_ingress(
        &self,
        _: u8,
        request: &mut RequestHeader,
        facts: &mut RequestFacts,
    ) -> Result<(), String> {
        self.request_hook("pre_ingress", request, facts).await
    }
    async fn pre_routing(
        &self,
        _: u8,
        request: &mut RequestHeader,
        facts: &mut RequestFacts,
    ) -> Result<(), String> {
        self.request_hook("pre_routing", request, facts).await
    }
    async fn authenticate(&self, _: u8, facts: &RequestFacts) -> Result<Option<()>, String> {
        Ok(matches!(
            self.invoke("authenticate", facts).await?.no_patch()?,
            Decision::Accept
        )
        .then_some(()))
    }
    async fn routing(
        &self,
        _: u8,
        request: &mut RequestHeader,
        facts: &mut RequestFacts,
    ) -> Result<(), String> {
        self.request_hook("routing", request, facts).await
    }
    async fn admit(&self, _: u8, facts: &RequestFacts) -> Result<Option<()>, String> {
        Ok(matches!(
            self.invoke("admit", facts).await?.no_patch()?,
            Decision::Accept
        )
        .then_some(()))
    }
    async fn prepare_request_body(
        &self,
        _: u8,
        request: &mut RequestHeader,
        _: &(),
        facts: &mut RequestFacts,
    ) -> Result<(), String> {
        self.request_hook("prepare_request_body", request, facts)
            .await
    }
    async fn before_upstream_send(
        &self,
        _: u8,
        request: &mut RequestHeader,
        _: &(),
        facts: &mut RequestFacts,
    ) -> Result<(), String> {
        self.request_hook("before_upstream_send", request, facts)
            .await
    }
    async fn upstream_response(
        &self,
        _: u8,
        response: &mut ResponseHeader,
        _: &(),
        facts: &mut RequestFacts,
    ) -> Result<(), String> {
        self.response_hook("upstream_response", response, facts)
            .await
    }
    async fn prepare_response_body(
        &self,
        _: u8,
        response: &mut ResponseHeader,
        _: &(),
        facts: &mut RequestFacts,
    ) -> Result<(), String> {
        self.response_hook("prepare_response_body", response, facts)
            .await
    }
    async fn before_downstream_write(
        &self,
        _: u8,
        response: &mut ResponseHeader,
        _: &(),
        facts: &mut RequestFacts,
    ) -> Result<(), String> {
        self.response_hook("before_downstream_write", response, facts)
            .await
    }
    async fn complete(&self, facts: &RequestFacts) -> Result<u32, String> {
        if self.interested("complete") {
            self.invoke("complete", facts).await?.no_patch()?;
        }
        Ok(0)
    }
    async fn before_dial(
        &self,
        _: u8,
        request: &DialRequest,
        facts: &RequestFacts,
    ) -> Result<Option<DialDecision>, String> {
        request
            .validate()
            .map_err(|_| "invalid native dial metadata")?;
        let target = match &request.target {
            // Host targets describe resolver inputs, never URLs with credentials.
            DialTarget::Host { name, port } => {
                if name.contains(['@', '/', '?', '#']) {
                    return Err("native dial host must not contain URL or credential syntax".into());
                }
                TargetMetadata::Host { name, port: *port }
            }
            DialTarget::Tcp(address) => TargetMetadata::Tcp { address: *address },
            #[cfg(unix)]
            DialTarget::Unix(path) => TargetMetadata::Unix { path },
        };
        let decision = self
            .invoke_metadata(
                "before_dial",
                facts,
                &facts.headers,
                Some(NetworkMetadata::BeforeDial {
                    target,
                    timeout_ms: request.timeout.as_millis(),
                    tcp_nodelay: request.tcp_nodelay,
                }),
                None,
            )
            .await?
            .no_patch()?;
        Ok(match decision {
            Decision::Accept => Some(DialDecision::Allow),
            Decision::Deny => Some(DialDecision::Deny),
            Decision::Continue => None,
        })
    }
    async fn pool(
        &self,
        _: u8,
        phase: PoolPhase,
        facts: &RequestFacts,
    ) -> Result<Option<PoolDecision>, String> {
        let phase = match phase {
            PoolPhase::BeforeCheckout => PoolMetadata::BeforeCheckout,
            PoolPhase::BeforeReturn {
                reused, healthy, ..
            } => PoolMetadata::BeforeReturn { reused, healthy },
        };
        let decision = self
            .invoke_metadata(
                "pool",
                facts,
                &facts.headers,
                Some(NetworkMetadata::Pool { phase }),
                None,
            )
            .await?
            .no_patch()?;
        Ok(match decision {
            Decision::Accept => Some(PoolDecision::Reuse),
            Decision::Deny => Some(PoolDecision::Discard),
            Decision::Continue => None,
        })
    }
    async fn socket(
        &self,
        _: u8,
        phase: SocketPhase,
        facts: &RequestFacts,
    ) -> Result<Option<SocketDecision>, String> {
        let phase = match phase {
            SocketPhase::BeforeUse => SocketMetadata::BeforeUse,
            SocketPhase::BeforeReuse => SocketMetadata::BeforeReuse,
        };
        let decision = self
            .invoke_metadata(
                "socket",
                facts,
                &facts.headers,
                Some(NetworkMetadata::Socket { phase }),
                None,
            )
            .await?
            .no_patch()?;
        Ok(match decision {
            Decision::Accept => Some(SocketDecision::Continue),
            Decision::Deny => Some(SocketDecision::Close),
            Decision::Continue => None,
        })
    }
    async fn maintain(&self) {}
}
