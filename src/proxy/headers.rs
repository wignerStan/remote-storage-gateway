//! Fixed-route header policy and native protocol framing, without body collection.
use super::*;
use http::Version;

const HANDSHAKE_HEADERS: [&str; 6] = [
    "upgrade",
    "sec-websocket-key",
    "sec-websocket-accept",
    "sec-websocket-version",
    "sec-websocket-extensions",
    "sec-websocket-protocol",
];

fn connection_upgrades(headers: &http::HeaderMap) -> bool {
    let mut upgrade = false;
    for value in headers.get_all("connection") {
        let Ok(value) = value.to_str() else {
            return false;
        };
        for token in value.split(',').map(str::trim) {
            if token.eq_ignore_ascii_case("close") {
                return false;
            }
            upgrade |= token.eq_ignore_ascii_case("upgrade");
        }
    }
    upgrade
}

pub(super) fn validate_request_handshake(
    request: &RequestHeader,
    original: &RequestHeader,
) -> Result<()> {
    if HANDSHAKE_HEADERS.iter().any(|name| {
        !request
            .headers
            .get_all(*name)
            .iter()
            .eq(original.headers.get_all(*name).iter())
    }) {
        return Err(refusal(
            400,
            "callbacks cannot change protocol handshake negotiation",
        ));
    }
    Ok(())
}

/// Check the origin's protocol selection before accepting its raw 101 head.
/// The endpoint remains responsible for cryptographic WebSocket accept validation.
pub(super) fn validate_upstream_upgrade(
    request: &RequestHeader,
    response: &ResponseHeader,
) -> Result<()> {
    if response.status.as_u16() != 101 {
        return Ok(());
    }
    let offered = request
        .headers
        .get_all("upgrade")
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .map(str::trim)
        .filter(|token| !token.is_empty())
        .collect::<Vec<_>>();
    let mut selected = Vec::new();
    for value in response.headers.get_all("upgrade") {
        let value = value
            .to_str()
            .map_err(|_| refusal(502, "invalid upstream protocol upgrade"))?;
        for token in value.split(',') {
            let token = token.trim();
            if token.is_empty()
                || !offered
                    .iter()
                    .any(|offered| token.eq_ignore_ascii_case(offered))
            {
                return Err(refusal(
                    502,
                    "upstream selected an unoffered protocol upgrade",
                ));
            }
            selected.push(token);
        }
    }
    let connection_upgrade = connection_upgrades(&response.headers);
    let request_upgrade = connection_upgrades(&request.headers);
    if request.version != Version::HTTP_11
        || selected.is_empty()
        || !connection_upgrade
        || !request_upgrade
    {
        return Err(refusal(502, "invalid upstream protocol upgrade"));
    }
    if selected
        .iter()
        .any(|token| token.eq_ignore_ascii_case("websocket"))
    {
        let mut accept = response.headers.get_all("sec-websocket-accept").iter();
        if request.method != http::Method::GET
            || selected.len() != 1
            || !accept
                .next()
                .is_some_and(|value| value.to_str().is_ok_and(|value| !value.trim().is_empty()))
            || accept.next().is_some()
        {
            return Err(refusal(502, "invalid upstream WebSocket handshake"));
        }
    }
    Ok(())
}

pub(super) fn validate_ingress(request: &RequestHeader) -> Result<()> {
    validate_transfer_coding(&request.headers, 400)?;
    if request.headers.contains_key("content-length")
        && request.headers.contains_key("transfer-encoding")
    {
        return Err(refusal(400, "ambiguous ingress body framing"));
    }
    if !matches!(
        request.version,
        Version::HTTP_10 | Version::HTTP_11 | Version::HTTP_2
    ) {
        return Err(refusal(505, "unsupported parsed HTTP version"));
    }
    if request.method == http::Method::CONNECT {
        return Err(refusal(
            405,
            "CONNECT requires a configured tunnel listener",
        ));
    }
    Ok(())
}

/// Pingora removes chunk framing, but does not decode other transfer codings.
/// Reject them before callbacks or forwarding can erase their declaration.
pub(super) fn validate_transfer_coding(headers: &http::HeaderMap, status: u16) -> Result<()> {
    let mut values = headers.get_all("transfer-encoding").iter();
    let Some(value) = values.next() else {
        return Ok(());
    };
    if values.next().is_some()
        || !value
            .to_str()
            .is_ok_and(|value| value.trim().eq_ignore_ascii_case("chunked"))
    {
        return Err(refusal(status, "unsupported transfer coding"));
    }
    Ok(())
}

/// Strip Connection-nominated fields before applying the fixed transport policy.
fn strip_hop_headers(headers: &mut http::HeaderMap) -> Result<()> {
    let mut nominated = Vec::new();
    for value in headers.get_all("connection") {
        let value = value
            .to_str()
            .map_err(|_| refusal(400, "invalid connection metadata"))?;
        for token in value.split(',') {
            let name = http::header::HeaderName::from_bytes(token.trim().as_bytes())
                .map_err(|_| refusal(400, "invalid connection token"))?;
            if matches!(
                name.as_str(),
                "content-length" | "transfer-encoding" | "host"
            ) {
                return Err(refusal(400, "invalid hop-by-hop framing declaration"));
            }
            nominated.push(name);
        }
    }
    for name in nominated {
        if name != "upgrade" {
            headers.remove(name);
        }
    }
    for name in [
        "proxy-authorization",
        "proxy-connection",
        "keep-alive",
        "te",
    ] {
        headers.remove(name);
    }
    Ok(())
}

pub(super) fn request_head(
    request: &mut RequestHeader,
    original: &RequestHeader,
    upstream: &Url,
    reuse: bool,
    variable: bool,
    has_body: bool,
) -> Result<()> {
    strip_hop_headers(&mut request.headers)?;
    request.insert_header(
        "host",
        &upstream[url::Position::BeforeHost..url::Position::AfterPort],
    )?;
    let path = request
        .uri
        .path_and_query()
        .map_or("/", |path| path.as_str())
        .parse()
        .map_err(|_| refusal(400, "invalid request target"))?;
    request.set_uri(path);
    request.insert_header(
        "connection",
        if request.headers.contains_key("upgrade") {
            "upgrade"
        } else if reuse {
            "keep-alive"
        } else {
            "close"
        },
    )?;
    // A header callback cannot invent framing for the original decoded bytes.
    // Variable transforms opt out explicitly; otherwise preserve the wire length.
    request.remove_header("content-length");
    request.remove_header("transfer-encoding");
    for name in ["content-length", "transfer-encoding"] {
        if let Some(value) = original.headers.get(name) {
            request.insert_header(name, value.clone())?;
        }
    }
    if variable && has_body {
        request.remove_header("content-length");
        request.insert_header("transfer-encoding", "chunked")?;
        // Digests of the original encoded entity cannot describe changed bytes.
        request.remove_header("content-md5");
        request.remove_header("digest");
        request.remove_header("trailer");
    }
    Ok(())
}

/// Metadata was already finalized before acquisition. Apply only the carrier's
/// selected HTTP version and framing when Pingora produces its wire header.
pub(super) fn wire_request(
    request: &mut RequestHeader,
    version: Version,
    reuse: bool,
    has_body: bool,
) -> Result<()> {
    request.set_version(version);
    if version == Version::HTTP_2 {
        for name in [
            "connection",
            "transfer-encoding",
            "upgrade",
            "keep-alive",
            "proxy-connection",
        ] {
            request.remove_header(name);
        }
    } else {
        if has_body && !request.headers.contains_key("content-length") {
            // HTTP/1.0 cannot delimit a streaming upload with chunked coding.
            request.set_version(Version::HTTP_11);
            request.insert_header("transfer-encoding", "chunked")?;
        }
        request.insert_header(
            "connection",
            if request.headers.contains_key("upgrade") {
                "upgrade"
            } else if reuse {
                "keep-alive"
            } else {
                "close"
            },
        )?;
    }
    Ok(())
}

/// Only framing metadata is retained, without another whole-header snapshot.
pub(super) struct ResponseFraming {
    pub status: http::StatusCode,
    length: Option<http::HeaderValue>,
    handshake: [Vec<http::HeaderValue>; HANDSHAKE_HEADERS.len()],
}
impl ResponseFraming {
    pub fn capture(response: &ResponseHeader) -> Self {
        Self {
            status: response.status,
            handshake: std::array::from_fn(|index| {
                if response.status.as_u16() == 101 {
                    response
                        .headers
                        .get_all(HANDSHAKE_HEADERS[index])
                        .iter()
                        .cloned()
                        .collect()
                } else {
                    Vec::new()
                }
            }),
            length: if response.headers.contains_key("transfer-encoding") {
                None
            } else {
                response.headers.get("content-length").cloned()
            },
        }
    }
    pub fn validate(&self, response: &ResponseHeader, method: &http::Method) -> Result<()> {
        let no_body = |status: http::StatusCode| {
            *method == http::Method::HEAD || matches!(status.as_u16(), 204 | 304)
        };
        if (self.status.as_u16() == 101
            && HANDSHAKE_HEADERS.iter().enumerate().any(|(index, name)| {
                !response
                    .headers
                    .get_all(*name)
                    .iter()
                    .eq(self.handshake[index].iter())
            }))
            || (self.status.as_u16() == 101 && !connection_upgrades(&response.headers))
            || (self.status.as_u16() == 101) != (response.status.as_u16() == 101)
            || (response.status.is_informational() && response.status.as_u16() != 101)
            || no_body(self.status) != no_body(response.status)
        {
            return Err(refusal(
                502,
                "callbacks cannot change native response body semantics",
            ));
        }
        Ok(())
    }
}

pub(super) fn response_head(
    response: &mut ResponseHeader,
    original: &ResponseFraming,
    method: &http::Method,
    downstream_version: Version,
    variable: bool,
) -> Result<bool> {
    original.validate(response, method)?;
    strip_hop_headers(&mut response.headers)?;
    response.remove_header("content-length");
    response.remove_header("transfer-encoding");
    if !variable && let Some(length) = &original.length {
        response.insert_header("content-length", length.clone())?;
    }
    if response.status.as_u16() == 101 || response.status.as_u16() == 204 {
        response.remove_header("content-length");
    }
    if variable {
        for name in ["content-md5", "digest", "etag", "trailer"] {
            response.remove_header(name);
        }
    }
    let may_have_body =
        *method != http::Method::HEAD && !matches!(response.status.as_u16(), 101 | 204 | 304);
    let unknown_length = may_have_body && !response.headers.contains_key("content-length");
    let close_delimited = downstream_version == Version::HTTP_10 && unknown_length;
    if downstream_version == Version::HTTP_11 && unknown_length {
        response.set_version(Version::HTTP_11);
        response.insert_header("transfer-encoding", "chunked")?;
    } else if close_delimited {
        response.set_version(Version::HTTP_10);
    }
    if response.status.as_u16() != 101 {
        response.remove_header("connection");
    }
    if downstream_version == Version::HTTP_2 {
        for name in [
            "connection",
            "transfer-encoding",
            "upgrade",
            "keep-alive",
            "proxy-connection",
        ] {
            response.remove_header(name);
        }
    }
    Ok(close_delimited)
}

#[cfg(test)]
#[path = "header_tests.rs"]
mod tests;
