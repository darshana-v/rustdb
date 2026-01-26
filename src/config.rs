//! Configuration loading and defaults.

use anyhow::Result;
use serde::Deserialize;
use std::path::Path;

/// Runtime configuration for RustDB.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct Config {
    /// Page size in bytes. Default 8192 (8 KB).
    pub page_size: u32,

    /// Buffer pool size (number of pages). Default 1024.
    pub buffer_pool_size: usize,

    /// Whether to fsync WAL on commit. Default true.
    pub wal_sync: bool,

    /// Listen address for TCP server. Default "127.0.0.1:7643".
    pub listen_addr: String,

    /// Max concurrent connections. Default 16.
    pub max_connections: usize,

    /// Data directory (heap, WAL, catalog). Default ".".
    pub data_dir: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            page_size: 8192,
            buffer_pool_size: 1024,
            wal_sync: true,
            listen_addr: "127.0.0.1:7643".to_string(),
            max_connections: 16,
            data_dir: ".".to_string(),
        }
    }
}

impl Config {
    /// Load config from a TOML file. Defaults to `Config::default()`.
    /// Empty file returns default config.
    pub fn from_path(path: &Path) -> Result<Self> {
        let s = std::fs::read_to_string(path)?;
        if s.trim().is_empty() {
            return Ok(Self::default());
        }
        let c: Config = toml::from_str(&s)?;
        c.validate()?;
        Ok(c)
    }

    /// Use default config. Convenience for tests and minimal setups.
    pub fn default_config() -> Self {
        Self::default()
    }

    fn validate(&self) -> Result<()> {
        if self.page_size == 0 || self.page_size % 256 != 0 {
            anyhow::bail!("page_size must be a positive multiple of 256");
        }
        if self.buffer_pool_size == 0 {
            anyhow::bail!("buffer_pool_size must be positive");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_valid() {
        let c = Config::default();
        c.validate().unwrap();
        assert_eq!(c.page_size, 8192);
    }
}
