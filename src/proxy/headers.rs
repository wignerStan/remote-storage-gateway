//! Fixed-route header policy and native protocol framing, without body collection.
use super::*;
use http::Version;

pub(super) fn validate_ingress(request: &RequestHeader) -> Result<()> {
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
    upgrade: Option<http::HeaderValue>,
}
impl ResponseFraming {
    pub fn capture(response: &ResponseHeader) -> Self {
        Self {
            status: response.status,
            upgrade: response.headers.get("upgrade").cloned(),
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
        if (self.status.as_u16() == 101 && response.headers.get("upgrade") != self.upgrade.as_ref())
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
) -> Result<()> {
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
    if downstream_version != Version::HTTP_2
        && may_have_body
        && !response.headers.contains_key("content-length")
    {
        response.set_version(Version::HTTP_11);
        response.insert_header("transfer-encoding", "chunked")?;
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
    Ok(())
}

#[cfg(test)]
#[path = "header_tests.rs"]
mod tests;
