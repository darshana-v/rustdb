//! Integration tests for RustDB.

use rustdb::storage::{row_encode, row_decode, Value, ColumnType, Page, PageFlags, HeapFile};
use rustdb::Config;
use tempfile::NamedTempFile;

#[test]
fn config_default_is_valid() {
    let c = Config::default_config();
    assert_eq!(c.page_size, 8192);
    assert!(c.wal_sync);
}

#[test]
fn phase1_row_page_heap_roundtrip() {
    let schema = vec![ColumnType::Int, ColumnType::Text, ColumnType::Bool];
    let values = vec![
        Value::Int(42),
        Value::Text("hello".to_string()),
        Value::Bool(true),
    ];
    let row_bytes = row_encode(&schema, &values, 1, 0).unwrap();
    let mut page = Page::new(0, PageFlags::Heap);
    page.insert(&row_bytes).unwrap();
    let tmp = NamedTempFile::new().unwrap();
    let mut heap = HeapFile::create(tmp.path()).unwrap();
    heap.append_page(&page).unwrap();
    let read = heap.read_page(0).unwrap();
    let slot = read.get_slot(0).unwrap();
    let (txn, tomb, decoded) = row_decode(&schema, slot).unwrap();
    assert_eq!(txn, 1);
    assert_eq!(tomb, 0);
    assert_eq!(decoded, values);
}
