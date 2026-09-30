//! Compiled fixed-route defaults and one metadata projection per boundary.
use super::*;

pub(super) struct CompiledPolicy {
    pub(super) reuse: bool,
}
#[async_trait]
impl Callbacks<ProxyContract> for CompiledPolicy {
    fn supports(&self, hook: proxy_pingora::worker::surface::CallbackHook) -> bool {
        use proxy_pingora::worker::surface::CallbackHook;
        matches!(
            hook,
            CallbackHook::PreRouting
                | CallbackHook::Routing
                | CallbackHook::Authenticate
                | CallbackHook::Admit
                | CallbackHook::Pool
        )
    }
    async fn pre_routing(
        &self,
        _: u8,
        request: &mut RequestHeader,
        facts: &mut RequestFacts,
    ) -> std::result::Result<(), String> {
        snapshot_request(request, facts);
        Ok(())
    }
    async fn routing(
        &self,
        _: u8,
        request: &mut RequestHeader,
        facts: &mut RequestFacts,
    ) -> std::result::Result<(), String> {
        snapshot_request(request, facts);
        Ok(())
    }
    async fn authenticate(
        &self,
        _: u8,
        _: &RequestFacts,
    ) -> std::result::Result<Option<()>, String> {
        Ok(Some(()))
    }
    async fn admit(&self, _: u8, _: &RequestFacts) -> std::result::Result<Option<()>, String> {
        Ok(Some(()))
    }
    async fn complete(&self, _: &RequestFacts) -> std::result::Result<u32, String> {
        Ok(0)
    }
    async fn pool(
        &self,
        _: u8,
        _: PoolPhase,
        _: &RequestFacts,
    ) -> std::result::Result<Option<PoolDecision>, String> {
        Ok(Some(if self.reuse {
            PoolDecision::Reuse
        } else {
            PoolDecision::Discard
        }))
    }
    async fn maintain(&self) {}
}

/// One metadata projection per completed request-head phase, shared by all
/// decision callbacks. Native transform frames borrow the live head directly.
pub(super) fn snapshot_request(request: &RequestHeader, facts: &mut RequestFacts) {
    facts.headers = request.headers.clone();
    facts.method = request.method.to_string();
    facts.path = request.uri.path().to_owned();
}
