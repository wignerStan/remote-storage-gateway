//! Native Pingora application and independently reusable composition APIs.
pub mod assembly;
pub mod composition;
pub mod config;
pub mod entry;
pub mod keepalive;
mod native_notifier;
mod observability;
mod process;
pub mod proxy;
pub mod remote_axis;
pub use process::Process;
