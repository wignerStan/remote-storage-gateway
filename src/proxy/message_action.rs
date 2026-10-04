//! Versioned native results: bounded metadata followed by an optional raw body.
//! No native allocation or borrowed C ABI reply escapes the scoped visitor.
use super::*;
use bytes::Bytes;
use serde::Deserialize;

const TAG: u8 = 2;
const PREFIX: usize = 5;
const METADATA_LIMIT: usize = crate::native_notifier::FRAME_LIMIT;

#[derive(Clone, Default)]
pub(super) struct Grant {
    pub enabled: bool,
    pub proxy: bool,
    pub headers: Vec<http::header::HeaderName>,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(super) enum Decision {
    #[default]
    Forward,
    Reply,
    Drop,
}
#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(super) enum Upstream {
    #[default]
    Keep,
    Connect,
    Replace,
    Disconnect,
    /// Drop the upstream, send the reply, then close the client socket.
    Close,
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Metadata {
    pub version: u8,
    pub decision: Decision,
    #[serde(default)]
    pub upstream: Upstream,
    #[serde(default)]
    pub set_headers: Vec<(String, String)>,
    #[serde(default)]
    pub remove_headers: Vec<String>,
    // Absence keeps the selected proxy; explicit null selects direct egress.
    #[serde(default, deserialize_with = "proxy_update")]
    pub proxy: Option<Option<String>>,
    pub path: Option<String>,
}
fn proxy_update<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Option<Option<String>>, D::Error> {
    Option::<String>::deserialize(deserializer).map(Some)
}

pub(super) struct Action {
    pub metadata: Metadata,
    pub payload: Bytes,
}

pub(super) enum Reply {
    Allow,
    Deny,
    ProtocolError(Bytes),
    Action(Action),
}

/// Decode directly inside the native reply lease. Unchanged allow/deny results
/// allocate nothing. Replacement bytes cross the lifetime boundary once.
pub(super) fn decode(
    reply: &[u8],
    grant: &Grant,
    response: bool,
    max_bytes: usize,
) -> Result<Reply> {
    match reply {
        [0] => return Ok(Reply::Allow),
        [1] => return Ok(Reply::Deny),
        _ => {}
    }
    if reply.first() != Some(&TAG) {
        if reply.is_empty() || reply.len() > max_bytes {
            return Err(refusal(502, "invalid message callback reply size"));
        }
        return Ok(Reply::ProtocolError(Bytes::copy_from_slice(reply)));
    }
    if !grant.enabled || reply.len() < PREFIX {
        return Err(refusal(502, "message actions outside the host grant"));
    }
    let length = u32::from_le_bytes(reply[1..PREFIX].try_into().expect("checked prefix")) as usize;
    if length == 0 || length > METADATA_LIMIT || length > reply.len() - PREFIX {
        return Err(refusal(502, "invalid message action metadata length"));
    }
    let payload = &reply[PREFIX + length..];
    if payload.len() > max_bytes {
        return Err(refusal(502, "message replacement exceeds host budget"));
    }
    let metadata: Metadata = serde_json::from_slice(&reply[PREFIX..PREFIX + length])
        .map_err(|_| refusal(502, "invalid message action metadata"))?;
    metadata.validate(grant, response, payload)?;
    Ok(Reply::Action(Action {
        metadata,
        payload: Bytes::copy_from_slice(payload),
    }))
}

impl Metadata {
    fn validate(&self, grant: &Grant, response: bool, payload: &[u8]) -> Result<()> {
        if self.version != 1 || self.set_headers.len() + self.remove_headers.len() > 64 {
            return Err(refusal(
                502,
                "invalid message action version or patch count",
            ));
        }
        if (self.decision == Decision::Reply && payload.is_empty())
            || (self.decision == Decision::Drop && !payload.is_empty())
            || (self.decision != Decision::Forward
                && matches!(self.upstream, Upstream::Connect | Upstream::Replace))
            || (self.decision == Decision::Forward
                && matches!(self.upstream, Upstream::Disconnect | Upstream::Close))
        {
            return Err(refusal(502, "inconsistent message action"));
        }
        if response
            && (matches!(self.upstream, Upstream::Connect | Upstream::Replace)
                || !self.set_headers.is_empty()
                || !self.remove_headers.is_empty()
                || self.proxy.is_some()
                || self.path.is_some())
        {
            return Err(refusal(
                502,
                "response cannot change upstream request metadata",
            ));
        }
        for name in self
            .set_headers
            .iter()
            .map(|(n, _)| n)
            .chain(&self.remove_headers)
        {
            let parsed = http::header::HeaderName::from_bytes(name.as_bytes())
                .map_err(|_| refusal(502, "invalid message header name"))?;
            if !grant.headers.contains(&parsed) {
                return Err(refusal(502, "message header patch outside the host grant"));
            }
        }
        for (_, value) in &self.set_headers {
            http::header::HeaderValue::from_str(value)
                .map_err(|_| refusal(502, "invalid message header value"))?;
        }
        if let Some(proxy) = &self.proxy {
            if !grant.proxy {
                return Err(refusal(
                    502,
                    "message proxy selection outside the host grant",
                ));
            }
            if let Some(proxy) = proxy {
                validate_proxy(proxy)?;
            }
        }
        if let Some(path) = &self.path {
            let uri: http::Uri = path
                .parse()
                .map_err(|_| refusal(502, "invalid message request path"))?;
            if path.len() > 8192
                || !path.starts_with('/')
                || path.starts_with("//")
                || uri.scheme().is_some()
                || uri.authority().is_some()
                || path.contains('#')
            {
                return Err(refusal(502, "message path must be origin form"));
            }
        }
        Ok(())
    }

    /// Apply validated metadata to an owned draft. The caller publishes that
    /// draft only after the complete callback chain and codec validation pass.
    pub fn apply(&self, request: &mut RequestHeader, proxy: &mut Option<Url>) -> Result<()> {
        for name in &self.remove_headers {
            request.remove_header(name);
        }
        for (name, value) in &self.set_headers {
            request.insert_header(name.clone(), value.clone())?;
        }
        if let Some(path) = &self.path {
            request.set_uri(
                path.parse()
                    .map_err(|_| refusal(502, "invalid message request path"))?,
            );
        }
        if let Some(update) = &self.proxy {
            *proxy = update
                .as_ref()
                .map(|value| validate_proxy(value))
                .transpose()?;
        }
        Ok(())
    }
}

fn validate_proxy(value: &str) -> Result<Url> {
    let proxy = Url::parse(value).map_err(|_| refusal(502, "invalid message proxy"))?;
    if value.len() > 8192
        || !matches!(proxy.scheme(), "http" | "https" | "socks5" | "socks5h")
        || proxy.host_str().is_none()
        || proxy.query().is_some()
        || proxy.fragment().is_some()
        || !matches!(proxy.path(), "" | "/")
    {
        return Err(refusal(502, "unsupported message proxy"));
    }
    Ok(proxy)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};
    fn encoded(metadata: Value, payload: &[u8]) -> Vec<u8> {
        let header = serde_json::to_vec(&metadata).unwrap();
        let mut reply = vec![TAG];
        reply.extend_from_slice(&(header.len() as u32).to_le_bytes());
        reply.extend_from_slice(&header);
        reply.extend_from_slice(payload);
        reply
    }
    fn grant() -> Grant {
        Grant {
            enabled: true,
            proxy: true,
            headers: crate::native_notifier::validate_patch_headers(&["cookie".into()]).unwrap(),
        }
    }
    #[test]
    fn binary_payload_preserves_bytes_and_explicit_direct_proxy() {
        let reply = encoded(
            json!({"version":1,"decision":"forward","upstream":"replace",
            "set_headers":[["cookie","session=new"]],"proxy":null,"path":"/v1/responses?x=1"}),
            b"{ \"type\":\"response.create\" }",
        );
        let Reply::Action(action) = decode(&reply, &grant(), false, 1024).unwrap() else {
            panic!()
        };
        assert_eq!(action.payload.as_ref(), b"{ \"type\":\"response.create\" }");
        let mut request = RequestHeader::build("GET", b"/", None).unwrap();
        let mut proxy = Some(Url::parse("http://localhost:3128").unwrap());
        action.metadata.apply(&mut request, &mut proxy).unwrap();
        assert!(proxy.is_none());
        assert_eq!(request.uri.to_string(), "/v1/responses?x=1");
        assert_eq!(request.headers["cookie"], "session=new");
    }
    #[test]
    fn malformed_or_ungranted_actions_never_publish_partial_patches() {
        for metadata in [
            json!({"version":2,"decision":"forward"}),
            json!({"version":1,"decision":"forward","set_headers":[["cookie","ok"],["host","evil"]]}),
            json!({"version":1,"decision":"forward","path":"https://evil/"}),
            json!({"version":1,"decision":"forward","path":"//evil/"}),
            json!({"version":1,"decision":"reply","upstream":"replace"}),
            json!({"version":1,"decision":"forward","upstream":"disconnect"}),
            json!({"version":1,"decision":"forward","unknown":true}),
        ] {
            assert!(decode(&encoded(metadata, b"{}"), &grant(), false, 1024).is_err());
        }
        let action = encoded(json!({"version":1,"decision":"forward"}), b"{}");
        assert!(decode(&action, &Grant::default(), false, 1024).is_err());
        assert!(decode(&action, &grant(), false, 1).is_err());
        for cut in 0..action.len() - 2 {
            assert!(decode(&action[..cut], &grant(), false, 1024).is_err());
        }
    }
    #[test]
    fn response_can_disconnect_with_reply_but_cannot_select_or_patch_a_route() {
        let reply = encoded(
            json!({"version":1,"decision":"reply","upstream":"disconnect"}),
            b"{\"type\":\"error\"}",
        );
        let close = encoded(
            json!({"version":1,"decision":"reply","upstream":"close"}),
            b"{\"type\":\"error\"}",
        );
        assert!(matches!(
            decode(&close, &grant(), true, 1024).unwrap(),
            Reply::Action(action) if action.metadata.upstream == Upstream::Close
        ));
        assert!(matches!(
            decode(&reply, &grant(), true, 1024).unwrap(),
            Reply::Action(_)
        ));
        for metadata in [
            json!({"version":1,"decision":"forward","upstream":"connect"}),
            json!({"version":1,"decision":"forward","proxy":null}),
            json!({"version":1,"decision":"forward","set_headers":[["cookie","x"]]}),
        ] {
            assert!(decode(&encoded(metadata, b""), &grant(), true, 1024).is_err());
        }
    }
}
