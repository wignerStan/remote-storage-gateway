//! Native Pingora application and independently reusable composition APIs.
pub mod assembly;
pub mod composition;
pub mod config;
pub mod entry;
pub mod keepalive;
pub mod management;
mod native_notifier;
mod observability;
mod process;
pub mod proxy;
pub use process::Process;
