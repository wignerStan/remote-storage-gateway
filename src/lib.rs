//! Native Pingora application and independently reusable composition APIs.
pub mod assembly;
pub mod composition;
pub mod config;
pub mod entry;
pub mod management;
mod native_notifier;
mod process;
pub mod proxy;
pub use process::Process;
