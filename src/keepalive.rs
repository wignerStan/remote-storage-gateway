//! Application policy for infra-owned WebSocket transport maintenance.
use serde::Deserialize;
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct WebSocketKeepalive {
    pub enabled: bool,
    pub upstream: bool,
    pub downstream: bool,
    pub interval_ms: u64,
    pub pong_timeout_ms: u64,
}
impl Default for WebSocketKeepalive {
    fn default() -> Self {
        Self {
            enabled: false,
            upstream: true,
            downstream: true,
            interval_ms: 30_000,
            pong_timeout_ms: 10_000,
        }
    }
}
impl WebSocketKeepalive {
    pub fn validate(&self, io_timeout: Duration) -> Result<(), String> {
        let interval = Duration::from_millis(self.interval_ms);
        let timeout = Duration::from_millis(self.pong_timeout_ms);
        let total = interval.checked_add(timeout);
        if interval.is_zero()
            || timeout.is_zero()
            || total.and_then(|d| Instant::now().checked_add(d)).is_none()
        {
            return Err("websocket_keepalive intervals must be positive and representable".into());
        }
        if self.enabled && (!self.upstream && !self.downstream) {
            return Err("websocket_keepalive requires at least one connection direction".into());
        }
        if self.enabled && total.is_none_or(|total| total >= io_timeout) {
            return Err(
                "websocket_keepalive interval plus pong timeout must be shorter than io_timeout_ms"
                    .into(),
            );
        }
        Ok(())
    }
    pub(crate) fn build(
        &self,
        session: &pingora_proxy::Session,
        upstream: bool,
    ) -> pingora_error::Result<Option<Box<dyn pingora_proxy::UpgradedKeepalive>>> {
        if !self.enabled
            || !(if upstream {
                self.upstream
            } else {
                self.downstream
            })
            || !session
                .req_header()
                .headers
                .get_all("upgrade")
                .iter()
                .any(|value| {
                    value.to_str().is_ok_and(|s| {
                        s.split(',')
                            .any(|t| t.trim().eq_ignore_ascii_case("websocket"))
                    })
                })
        {
            return Ok(None);
        }
        Ok(Some(Box::new(
            proxy_pingora::http_ingress::keepalive::WebSocketHeartbeat::new(
                upstream,
                Duration::from_millis(self.interval_ms),
                Duration::from_millis(self.pong_timeout_ms),
            )?,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn keepalive_must_finish_before_native_io_timeout() {
        let mut config = WebSocketKeepalive {
            enabled: true,
            ..Default::default()
        };
        assert!(config.validate(Duration::from_secs(60)).is_ok());
        assert!(config.validate(Duration::from_secs(40)).is_err());
        config.pong_timeout_ms = 0;
        assert!(config.validate(Duration::from_secs(60)).is_err());
        config.pong_timeout_ms = u64::MAX;
        assert!(config.validate(Duration::from_secs(60)).is_err());
    }
    #[test]
    fn disabled_defaults_do_not_restrict_short_http_deadlines() {
        assert!(
            WebSocketKeepalive::default()
                .validate(Duration::from_millis(1))
                .is_ok()
        );
        assert!(serde_json::from_str::<WebSocketKeepalive>(r#"{"unknown":1}"#).is_err());
    }
}
