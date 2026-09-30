//! Worker-owned metadata contract; native allocation and call lifetimes stay in infra.
use crate::{
    config::NativeCallback,
    proxy::{ProxyContract, RequestFacts},
};
use pingora_http::{RequestHeader, ResponseHeader};
use plugin_loading::Function;
use proxy_pingora::worker::surface::{CallbackHook, Callbacks};
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

#[derive(Serialize)]
struct Metadata<'a> {
    version: u8,
    hook: &'a str,
    request_id: u64,
    method: &'a str,
    path: &'a str,
    status: Option<u16>,
    upstream_reused: bool,
    completed: bool,
    headers: SelectedHeaders<'a>,
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
    fn check(self) -> Result<Self, String> {
        if self.version != 1 || self.set_headers.len() + self.remove_headers.len() > 64 {
            return Err("invalid notifier reply version or patch count".into());
        }
        if matches!(self.decision, Decision::Deny) {
            return Err("native notifier denied the request".into());
        }
        for name in self
            .set_headers
            .iter()
            .map(|(name, _)| name)
            .chain(&self.remove_headers)
        {
            let canonical = name.to_ascii_lowercase();
            if [
                "host",
                "content-length",
                "transfer-encoding",
                "connection",
                "keep-alive",
                "proxy-connection",
                "upgrade",
                "trailer",
                "te",
            ]
            .contains(&canonical.as_str())
            {
                return Err(
                    "notifier cannot override transport framing or selected authority".into(),
                );
            }
            http::header::HeaderName::from_bytes(name.as_bytes())
                .map_err(|_| "invalid notifier header name")?;
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

pub struct NativeNotifier {
    function: Arc<Function>,
    interests: u16,
    timeout: Duration,
    headers: Vec<http::header::HeaderName>,
}
impl NativeNotifier {
    pub fn new(function: Arc<Function>, config: &NativeCallback) -> Result<Self, String> {
        validate_hooks(&config.hooks)?;
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
            function,
            interests,
            timeout: Duration::from_millis(config.timeout_ms),
            headers,
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
        let metadata = Metadata {
            version: 1,
            hook,
            request_id: facts.request_id,
            method: &facts.method,
            path: &facts.path,
            status: facts.status,
            upstream_reused: facts.upstream_reused,
            completed: facts.completed,
            headers: SelectedHeaders {
                source,
                names: &self.headers,
            },
        };
        let input = text_processing::json::to_vec_bounded(&metadata, FRAME_LIMIT)
            .map_err(|_| "could not encode bounded notifier metadata")?;

        let result = self
            .function
            .call_with_reply(hook, input, self.timeout, (), |bytes| {
                serde_json::from_slice::<Reply>(bytes)
                    .map_err(|_| "invalid notifier metadata reply".to_string())
                    .and_then(Reply::check)
            })
            .await
            .map_err(|_| "native notifier call failed".to_string())?;
        tracing::trace!(target: "proxy_pingora::worker::notifier", hook, outcome = if result.is_ok() { "continue" } else { "error" }, "native metadata decision");
        result
    }
    async fn request_hook(
        &self,
        hook: &'static str,
        request: &mut RequestHeader,
        facts: &mut RequestFacts,
    ) -> Result<(), String> {
        let reply = self.invoke_headers(hook, facts, &request.headers).await?;
        let changed = !reply.set_headers.is_empty() || !reply.remove_headers.is_empty();
        reply.request(request)?;
        if changed {
            facts.headers = request.headers.clone();
        }
        Ok(())
    }
    async fn response_hook(
        &self,
        hook: &'static str,
        response: &mut ResponseHeader,
        facts: &RequestFacts,
    ) -> Result<(), String> {
        self.invoke_headers(hook, facts, &response.headers)
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
    async fn maintain(&self) {}
}
