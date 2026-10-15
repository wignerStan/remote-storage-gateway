//! Entry handlers decode and route operations to their owning axes.
//! Facilities own listeners and wire multiplexing; assembly registers these handlers.
pub(crate) mod plugins;
pub mod worker;
