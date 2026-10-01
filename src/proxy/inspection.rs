//! Worker-owned policy composed from the infra weighted reservation primitive.
use super::*;
use admission_gate::budget::{Reservation, ResourceBudget};
pub(super) struct InspectionBudget {
    pub limits: crate::config::Inspection,
    requests: Arc<ResourceBudget>,
    memory: Arc<ResourceBudget>,
}
impl InspectionBudget {
    pub fn new(limits: crate::config::Inspection) -> Self {
        Self {
            requests: ResourceBudget::new(limits.max_requests),
            memory: ResourceBudget::new(limits.max_buffered_bytes),
            limits,
        }
    }
    pub fn request(&self) -> Result<Reservation> {
        self.requests
            .try_reserve(1)
            .ok_or_else(|| refusal(503, "worker request capacity exhausted"))
    }
    pub fn buffers(&self) -> Result<Arc<Reservation>> {
        // Simultaneous request/response wire, decoded payload, assembly/output
        // copies, segment bookkeeping and one native zstd decoding window.
        let bytes = self
            .limits
            .max_message_bytes
            .checked_mul(8)
            .and_then(|n| n.checked_add(8 * 1024 * 1024 + 512 * 1024))
            .ok_or_else(|| refusal(503, "inspection reservation overflow"))?;
        self.memory
            .try_reserve(bytes)
            .map(Arc::new)
            .ok_or_else(|| refusal(503, "worker inspection memory exhausted"))
    }
    pub fn deadline(&self) -> tokio::time::Instant {
        tokio::time::Instant::now() + Duration::from_millis(self.limits.message_timeout_ms)
    }
}
