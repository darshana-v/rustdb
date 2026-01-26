//! RustDB server binary.
//! Usage: rustdb [CONFIG_PATH]

use anyhow::Result;
use rustdb::Config;
use std::env;
use std::path::PathBuf;
use tracing_subscriber::EnvFilter;

fn main() -> Result<()> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let config = match env::args().nth(1) {
        Some(path) => Config::from_path(&PathBuf::from(path))?,
        None => Config::default_config(),
    };

    tracing::info!(listen_addr = %config.listen_addr, "RustDB starting (Phase 0 bootstrap)");
    // start TCP server and run until shutdown
    tracing::info!("RustDB exiting (no server yet)");
    Ok(())
}
