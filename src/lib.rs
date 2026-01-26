//! RustDB — A single-node, transactional, disk-backed database.

pub mod config;
pub mod catalog;
pub mod storage;
pub mod buffer;
pub mod wal;
pub mod txn;
pub mod query;
pub mod protocol;
pub mod server;

// re export for convenience.
pub use config::Config;
pub use anyhow::Result;
