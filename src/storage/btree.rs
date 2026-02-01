//! B-tree index for primary key. Keys are i64; values point to heap (page_id, slot).

use anyhow::Result;

use super::heap::{HeapFile, PageId};
use super::page::{Page, PageFlags, PAGE_SIZE};

/// Pointer to a row in the heap: page id + slot index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RowRef {
    pub page_id: PageId,
    pub slot: u16,
}

impl RowRef {
    pub fn new(page_id: PageId, slot: u16) -> Self {
        Self { page_id, slot }
    }
}

// B-tree layout (after 32-byte page header)
// Leaf: next_leaf_page_id (4) | num_entries (2) | [key:8][page_id:4][slot:2]*
// Internal: num_keys (2) | [child0:4][key1:8][child1:4]...[child_n:4]
const BTREE_BODY_START: usize = 32;
const LEAF_ENTRY_SIZE: usize = 8 + 4 + 2; // key + page_id + slot
const INTERNAL_KEY_SIZE: usize = 4 + 8;   // child + key (last child stored separately)

fn leaf_max_entries() -> usize {
    (PAGE_SIZE - BTREE_BODY_START - 4 - 2) / LEAF_ENTRY_SIZE // -4 next, -2 num
}

fn internal_max_keys() -> usize {
    (PAGE_SIZE - BTREE_BODY_START - 2 - 4) / INTERNAL_KEY_SIZE // -2 num, -4 first child
}

/// B-tree index. Root is always page 0. Keys are i64 (primary key); values are RowRef.
pub struct BTree {
    index_heap: HeapFile,
}

impl BTree {
    /// Create new B-tree with empty root leaf. Overwrites index file.
    pub fn create<P: std::path::AsRef<std::path::Path>>(path: P) -> Result<Self> {
        let mut index_heap = HeapFile::create(path)?;
        let root = Self::alloc_empty_leaf(&mut index_heap)?;
        assert_eq!(root, 0);
        Ok(Self { index_heap })
    }

    /// Open existing B-tree. Root must be page 0.
    pub fn open<P: std::path::AsRef<std::path::Path>>(path: P) -> Result<Self> {
        let index_heap = HeapFile::open(path)?;
        Ok(Self { index_heap })
    }

    fn alloc_empty_leaf(heap: &mut HeapFile) -> Result<PageId> {
        let mut page = Page::new(0, PageFlags::Leaf);
        Self::leaf_set_next(&mut page, 0);
        Self::leaf_set_num_entries(&mut page, 0);
        heap.append_page(&page)
    }

    fn leaf_set_next(page: &mut Page, next: PageId) {
        let off = BTREE_BODY_START;
        page.as_bytes_mut()[off..off + 4].copy_from_slice(&next.to_le_bytes());
    }
    fn leaf_next(page: &Page) -> PageId {
        let off = BTREE_BODY_START;
        u32::from_le_bytes(page.as_bytes()[off..off + 4].try_into().unwrap())
    }
    fn leaf_set_num_entries(page: &mut Page, n: u16) {
        let off = BTREE_BODY_START + 4;
        page.as_bytes_mut()[off..off + 2].copy_from_slice(&n.to_le_bytes());
    }
    fn leaf_num_entries(page: &Page) -> u16 {
        let off = BTREE_BODY_START + 4;
        u16::from_le_bytes(page.as_bytes()[off..off + 2].try_into().unwrap())
    }
    fn leaf_entry_offset(idx: usize) -> usize {
        BTREE_BODY_START + 6 + idx * LEAF_ENTRY_SIZE
    }
    fn leaf_get_key(page: &Page, idx: usize) -> i64 {
        let off = Self::leaf_entry_offset(idx);
        i64::from_le_bytes(page.as_bytes()[off..off + 8].try_into().unwrap())
    }
    fn leaf_get_ref(page: &Page, idx: usize) -> RowRef {
        let off = Self::leaf_entry_offset(idx) + 8;
        let page_id = u32::from_le_bytes(page.as_bytes()[off..off + 4].try_into().unwrap());
        let slot = u16::from_le_bytes(page.as_bytes()[off + 4..off + 6].try_into().unwrap());
        RowRef { page_id, slot }
    }
    fn leaf_set_entry(page: &mut Page, idx: usize, key: i64, r: RowRef) {
        let off = Self::leaf_entry_offset(idx);
        page.as_bytes_mut()[off..off + 8].copy_from_slice(&key.to_le_bytes());
        page.as_bytes_mut()[off + 8..off + 12].copy_from_slice(&r.page_id.to_le_bytes());
        page.as_bytes_mut()[off + 12..off + 14].copy_from_slice(&r.slot.to_le_bytes());
    }
    fn leaf_insert_at(page: &mut Page, idx: usize, key: i64, r: RowRef) {
        let n = Self::leaf_num_entries(page) as usize;
        for i in (idx..n).rev() {
            Self::leaf_set_entry(
                page,
                i + 1,
                Self::leaf_get_key(page, i),
                Self::leaf_get_ref(page, i),
            );
        }
        Self::leaf_set_entry(page, idx, key, r);
        Self::leaf_set_num_entries(page, (n + 1) as u16);
    }

    fn internal_set_num_keys(page: &mut Page, n: u16) {
        let off = BTREE_BODY_START;
        page.as_bytes_mut()[off..off + 2].copy_from_slice(&n.to_le_bytes());
    }
    fn internal_num_keys(page: &Page) -> u16 {
        let off = BTREE_BODY_START;
        u16::from_le_bytes(page.as_bytes()[off..off + 2].try_into().unwrap())
    }
    fn internal_child_offset(idx: usize) -> usize {
        BTREE_BODY_START + 2 + idx * (4 + 8) // first child at 0, then (child, key) pairs
    }
    fn internal_get_child(page: &Page, idx: usize) -> PageId {
        let off = Self::internal_child_offset(idx);
        u32::from_le_bytes(page.as_bytes()[off..off + 4].try_into().unwrap())
    }
    fn internal_get_key(page: &Page, idx: usize) -> i64 {
        let off = Self::internal_child_offset(idx) + 4;
        i64::from_le_bytes(page.as_bytes()[off..off + 8].try_into().unwrap())
    }
    fn internal_set_child_key(page: &mut Page, idx: usize, child: PageId, key: i64) {
        let off = Self::internal_child_offset(idx);
        page.as_bytes_mut()[off..off + 4].copy_from_slice(&child.to_le_bytes());
        page.as_bytes_mut()[off + 4..off + 12].copy_from_slice(&key.to_le_bytes());
    }
    fn internal_set_last_child(page: &mut Page, n: usize, child: PageId) {
        let off = Self::internal_child_offset(n);
        page.as_bytes_mut()[off..off + 4].copy_from_slice(&child.to_le_bytes());
    }

    /// Lookup key. Returns RowRef if found.
    pub fn get(&mut self, key: i64) -> Result<Option<RowRef>> {
        self.get_from(0, key)
    }

    fn get_from(&mut self, page_id: PageId, key: i64) -> Result<Option<RowRef>> {
        let page = self.index_heap.read_page(page_id)?;
        let flags = Self::flags(&page);
        if flags == PageFlags::Leaf as u16 {
            let n = Self::leaf_num_entries(&page);
            for i in 0..n as usize {
                let k = Self::leaf_get_key(&page, i);
                if k == key {
                    return Ok(Some(Self::leaf_get_ref(&page, i)));
                }
                if k > key {
                    return Ok(None);
                }
            }
            Ok(None)
        } else {
            let n = Self::internal_num_keys(&page);
            let mut child_idx = 0;
            for i in 0..n as usize {
                if key < Self::internal_get_key(&page, i) {
                    break;
                }
                child_idx = i + 1;
            }
            let child = Self::internal_get_child(&page, child_idx);
            self.get_from(child, key)
        }
    }

    fn flags(page: &Page) -> u16 {
        u16::from_le_bytes(page.as_bytes()[8..10].try_into().unwrap())
    }

    /// Insert (key, value). Returns error on duplicate key for now.
    pub fn insert(&mut self, key: i64, value: RowRef) -> Result<()> {
        if self.index_heap.num_pages() == 0 {
            anyhow::bail!("empty btree");
        }
        if let Some((sk, sp)) = self.insert_into(0, key, value)? {
            self.split_root(sk, sp)?;
        }
        Ok(())
    }

    fn insert_into(
        &mut self,
        page_id: PageId,
        key: i64,
        value: RowRef,
    ) -> Result<Option<(i64, PageId)>> {
        let mut page = self.index_heap.read_page(page_id)?;
        let flags = Self::flags(&page);
        if flags == PageFlags::Leaf as u16 {
            let n = Self::leaf_num_entries(&page) as usize;
            let mut idx = n;
            for i in 0..n {
                let k = Self::leaf_get_key(&page, i);
                if k == key {
                    anyhow::bail!("duplicate key {}", key);
                }
                if k > key {
                    idx = i;
                    break;
                }
            }
            Self::leaf_insert_at(&mut page, idx, key, value);
            self.index_heap.write_page(page_id, &page)?;
            let max = leaf_max_entries();
            if n + 1 > max {
                return Ok(Some(self.split_leaf(page_id, &mut page)?));
            }
            Ok(None)
        } else {
            let n = Self::internal_num_keys(&page) as usize;
            let mut child_idx = 0;
            for i in 0..n {
                if key < Self::internal_get_key(&page, i) {
                    break;
                }
                child_idx = i + 1;
            }
            let child_id = Self::internal_get_child(&page, child_idx);
            let split = self.insert_into(child_id, key, value)?;
            if let Some((split_key, split_page_id)) = split {
                self.insert_internal_child(&mut page, child_idx, split_key, split_page_id);
                let n = Self::internal_num_keys(&page) as usize;
                let max_internal = internal_max_keys();
                if n > max_internal {
                    return Ok(Some(self.split_internal(page_id, &mut page)?));
                }
                self.index_heap.write_page(page_id, &page)?;
            }
            Ok(None)
        }
    }

    fn insert_internal_child(
        &mut self,
        page: &mut Page,
        after_child_idx: usize,
        key: i64,
        right_page_id: PageId,
    ) {
        let n = Self::internal_num_keys(page) as usize;
        let last_child = Self::internal_get_child(page, n);
        for i in (after_child_idx + 1..n).rev() {
            let c = Self::internal_get_child(page, i);
            let k = Self::internal_get_key(page, i);
            Self::internal_set_child_key(page, i + 1, c, k);
        }
        Self::internal_set_child_key(page, after_child_idx + 1, right_page_id, key);
        Self::internal_set_last_child(page, n + 1, last_child);
        Self::internal_set_num_keys(page, (n + 1) as u16);
    }

    fn split_leaf(&mut self, page_id: PageId, page: &mut Page) -> Result<(i64, PageId)> {
        let n = Self::leaf_num_entries(page) as usize;
        let mid = n / 2;
        let split_key = Self::leaf_get_key(page, mid);
        let mut new_page = Page::new(0, PageFlags::Leaf);
        Self::leaf_set_next(&mut new_page, Self::leaf_next(page));
        Self::leaf_set_num_entries(&mut new_page, (n - mid) as u16);
        for i in 0..(n - mid) {
            Self::leaf_set_entry(
                &mut new_page,
                i,
                Self::leaf_get_key(page, mid + i),
                Self::leaf_get_ref(page, mid + i),
            );
        }
        Self::leaf_set_num_entries(page, mid as u16);
        Self::leaf_set_next(page, self.index_heap.num_pages());
        let new_id = self.index_heap.append_page(&new_page)?;
        self.index_heap.write_page(page_id, page)?;
        Ok((split_key, new_id))
    }

    fn split_internal(&mut self, page_id: PageId, page: &mut Page) -> Result<(i64, PageId)> {
        let n = Self::internal_num_keys(page) as usize;
        let mid = n / 2;
        let promote_key = Self::internal_get_key(page, mid);
        let mut new_page = Page::new(0, PageFlags::Internal);
        Self::internal_set_num_keys(&mut new_page, (n - mid - 1) as u16);
        Self::internal_set_last_child(&mut new_page, 0, Self::internal_get_child(page, mid + 1));
        for i in 0..(n - mid - 1) {
            let c = Self::internal_get_child(page, mid + 2 + i);
            let k = Self::internal_get_key(page, mid + 1 + i);
            Self::internal_set_child_key(&mut new_page, i + 1, c, k);
        }
        let right_id = self.index_heap.append_page(&new_page)?;
        Self::internal_set_num_keys(page, mid as u16);
        self.index_heap.write_page(page_id, page)?;
        Ok((promote_key, right_id))
    }

    fn split_root(&mut self, promote_key: i64, right_page_id: PageId) -> Result<()> {
        let left_page = self.index_heap.read_page(0)?;
        let left_id = self.index_heap.append_page(&left_page)?;
        let mut new_root = Page::new(0, PageFlags::Internal);
        Self::internal_set_num_keys(&mut new_root, 1);
        Self::internal_set_last_child(&mut new_root, 0, left_id);
        Self::internal_set_child_key(&mut new_root, 1, right_page_id, promote_key);
        self.index_heap.write_page(0, &new_root)?;
        Ok(())
    }

    /// Range scan: yields (key, RowRef) for keys in [start, end) (end exclusive).
    pub fn range_scan(&mut self, start: i64, end: i64) -> Result<Vec<(i64, RowRef)>> {
        let mut out = Vec::new();
        self.range_scan_from(0, start, end, &mut out)?;
        Ok(out)
    }

    fn range_scan_from(
        &mut self,
        page_id: PageId,
        start: i64,
        end: i64,
        out: &mut Vec<(i64, RowRef)>,
    ) -> Result<()> {
        let page = self.index_heap.read_page(page_id)?;
        let flags = Self::flags(&page);
        if flags == PageFlags::Leaf as u16 {
            let n = Self::leaf_num_entries(&page);
            for i in 0..n as usize {
                let k = Self::leaf_get_key(&page, i);
                if k >= end {
                    return Ok(());
                }
                if k >= start {
                    out.push((k, Self::leaf_get_ref(&page, i)));
                }
            }
            let next = Self::leaf_next(&page);
            if next != 0 {
                self.range_scan_from(next, start, end, out)?;
            }
            Ok(())
        } else {
            let n = Self::internal_num_keys(&page);
            let mut i = 0usize;
            while i < n as usize {
                let k = Self::internal_get_key(&page, i);
                if end <= k {
                    let child = Self::internal_get_child(&page, i);
                    self.range_scan_from(child, start, end, out)?;
                    return Ok(());
                }
                if start <= k {
                    let child = Self::internal_get_child(&page, i);
                    self.range_scan_from(child, start, end, out)?;
                }
                i += 1;
            }
            let child = Self::internal_get_child(&page, n as usize);
            self.range_scan_from(child, start, end, out)
        }
    }

    pub fn num_pages(&self) -> PageId {
        self.index_heap.num_pages()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn btree_insert_get() {
        let tmp = NamedTempFile::new().unwrap();
        let mut bt = BTree::create(tmp.path()).unwrap();
        bt.insert(10, RowRef::new(1, 0)).unwrap();
        bt.insert(20, RowRef::new(2, 1)).unwrap();
        bt.insert(5, RowRef::new(0, 2)).unwrap();
        assert_eq!(bt.get(10).unwrap(), Some(RowRef::new(1, 0)));
        assert_eq!(bt.get(5).unwrap(), Some(RowRef::new(0, 2)));
        assert_eq!(bt.get(20).unwrap(), Some(RowRef::new(2, 1)));
        assert_eq!(bt.get(7).unwrap(), None);
    }

    #[test]
    fn btree_range_scan() {
        let tmp = NamedTempFile::new().unwrap();
        let mut bt = BTree::create(tmp.path()).unwrap();
        for i in 0..10 {
            bt.insert(i * 10, RowRef::new(i, 0)).unwrap();
        }
        let r = bt.range_scan(25, 55).unwrap();
        assert_eq!(r.len(), 3);
        assert_eq!(r[0].0, 30);
        assert_eq!(r[1].0, 40);
        assert_eq!(r[2].0, 50);
    }

    #[test]
    fn btree_split_under_load() {
        let tmp = NamedTempFile::new().unwrap();
        let mut bt = BTree::create(tmp.path()).unwrap();
        let n = 500;
        for i in 0..n {
            bt.insert(i as i64, RowRef::new((i % 100) as u32, (i % 10) as u16)).unwrap();
        }
        assert!(bt.num_pages() > 1);
        for i in 0..n {
            let r = bt.get(i as i64).unwrap().unwrap();
            assert_eq!(r.page_id, (i % 100) as u32);
            assert_eq!(r.slot, (i % 10) as u16);
        }
    }

    #[test]
    fn btree_reopen_persists() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path();
        {
            let mut bt = BTree::create(path).unwrap();
            bt.insert(42, RowRef::new(7, 3)).unwrap();
        }
        let mut bt = BTree::open(path).unwrap();
        assert_eq!(bt.get(42).unwrap(), Some(RowRef::new(7, 3)));
    }
}
