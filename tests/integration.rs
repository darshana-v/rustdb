//! Integration tests for RustDB.

use rustdb::Config;

#[test]
fn config_default_is_valid() {
    let c = Config::default_config();
    assert_eq!(c.page_size, 8192);
    assert!(c.wal_sync);
}
