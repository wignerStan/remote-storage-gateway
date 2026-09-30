//! Body setup hands ownership to the carrier before metadata finalization.
use super::*;

pub(super) fn take_plan(slot: &mut ChunkChain) -> Result<(ChunkChain, bool)> {
    let preserves_length = slot.seal().map_err(lifecycle_error)?;
    let mut closed = ChunkChain::default();
    closed.seal().map_err(lifecycle_error)?;
    Ok((std::mem::replace(slot, closed), preserves_length))
}

/// Facts retain a closed empty slot. A metadata callback cannot replace the
/// active processor or reopen configuration once wire framing has been chosen.
pub(super) fn ensure_closed(slot: &ChunkChain) -> Result<()> {
    slot.ensure_ready().map_err(lifecycle_error)?;
    if !slot.is_empty() {
        return Err(refusal(502, "body configuration changed after setup"));
    }
    Ok(())
}
