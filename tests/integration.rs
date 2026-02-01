//! Integration tests for RustDB.

use rustdb::storage::{
    row_encode, row_decode, Value, ColumnType, Page, PageFlags, HeapFile, BTree, RowRef,
};
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

#[test]
fn phase2_heap_btree_integration() {
    let schema = vec![ColumnType::Int, ColumnType::Text];
    let data_tmp = NamedTempFile::new().unwrap();
    let idx_tmp = NamedTempFile::new().unwrap();
    let mut heap = HeapFile::create(data_tmp.path()).unwrap();
    let mut btree = BTree::create(idx_tmp.path()).unwrap();
    for (pk, name) in [(10, "alice"), (20, "bob"), (5, "carol")] {
        let values = vec![Value::Int(pk), Value::Text(name.to_string())];
        let row_bytes = row_encode(&schema, &values, 1, 0).unwrap();
        let mut page = Page::new(0, PageFlags::Heap);
        let slot = page.insert(&row_bytes).unwrap();
        let page_id = heap.append_page(&page).unwrap();
        btree.insert(pk, RowRef::new(page_id, slot as u16)).unwrap();
    }
    let r = btree.get(20).unwrap().unwrap();
    let page = heap.read_page(r.page_id).unwrap();
    let slot = page.get_slot(r.slot as usize).unwrap();
    let (_, _, decoded) = row_decode(&schema, slot).unwrap();
    assert_eq!(decoded[0], Value::Int(20));
    assert_eq!(decoded[1], Value::Text("bob".to_string()));
}
