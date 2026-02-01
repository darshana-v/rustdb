//! Page format v1: 8 KB slotted page. Header + slot directory + row area.
//! Row area grows downward from end of page; slot directory grows upward from header.

use anyhow::{bail, ensure, Result};
use std::io::{Read, Seek, SeekFrom, Write};

use super::row::ROW_HEADER_LEN;

pub const PAGE_SIZE: usize = 8192;
pub const PAGE_MAGIC: u32 = 0x5253_4442; // "RSDB" in hex

pub const HEADER_LEN: usize = 32;
const OFFSET_MAGIC: usize = 0;
const OFFSET_PAGE_ID: usize = 4;
const OFFSET_FLAGS: usize = 8;
const OFFSET_N_SLOTS: usize = 10;
const OFFSET_FREE_END: usize = 12;
const SLOT_SIZE: usize = 4; // offset u16, length u16
const SLOT_DIR_START: usize = HEADER_LEN;

#[repr(u16)]
pub enum PageFlags {
    Heap = 0,
    Leaf = 1,
    Internal = 2,
}

/// Slotted page. Slot directory at [HEADER_LEN..); row area [free_end..PAGE_SIZE).
/// Rows grow downward from PAGE_SIZE; free_end is the low end of the free region.
#[derive(Clone)]
pub struct Page {
    data: [u8; PAGE_SIZE],
}

impl Page {
    pub fn new(page_id: u32, flags: PageFlags) -> Self {
        let mut p = Self {
            data: [0u8; PAGE_SIZE],
        };
        p.set_magic(PAGE_MAGIC);
        p.set_page_id(page_id);
        p.set_flags(flags as u16);
        p.set_n_slots(0);
        p.set_free_end(PAGE_SIZE as u16);
        p
    }

    fn set_magic(&mut self, v: u32) {
        self.data[OFFSET_MAGIC..OFFSET_MAGIC + 4].copy_from_slice(&v.to_le_bytes());
    }
    fn magic(&self) -> u32 {
        u32::from_le_bytes(self.data[OFFSET_MAGIC..OFFSET_MAGIC + 4].try_into().unwrap())
    }
    pub fn page_id(&self) -> u32 {
        u32::from_le_bytes(self.data[OFFSET_PAGE_ID..OFFSET_PAGE_ID + 4].try_into().unwrap())
    }
    /// Set page id (e.g. when appending to heap).
    pub fn set_page_id(&mut self, v: u32) {
        self.data[OFFSET_PAGE_ID..OFFSET_PAGE_ID + 4].copy_from_slice(&v.to_le_bytes());
    }
    fn set_flags(&mut self, v: u16) {
        self.data[OFFSET_FLAGS..OFFSET_FLAGS + 2].copy_from_slice(&v.to_le_bytes());
    }
    fn raw_n_slots(&self) -> u16 {
        u16::from_le_bytes(self.data[OFFSET_N_SLOTS..OFFSET_N_SLOTS + 2].try_into().unwrap())
    }
    fn set_n_slots(&mut self, v: u16) {
        self.data[OFFSET_N_SLOTS..OFFSET_N_SLOTS + 2].copy_from_slice(&v.to_le_bytes());
    }
    fn free_end(&self) -> u16 {
        u16::from_le_bytes(self.data[OFFSET_FREE_END..OFFSET_FREE_END + 2].try_into().unwrap())
    }
    fn set_free_end(&mut self, v: u16) {
        self.data[OFFSET_FREE_END..OFFSET_FREE_END + 2].copy_from_slice(&v.to_le_bytes());
    }

    fn slot_dir_end(&self) -> usize {
        SLOT_DIR_START + self.raw_n_slots() as usize * SLOT_SIZE
    }

    /// Free space: between slot dir end and free_end. Must fit new row + one new slot.
    pub fn free_space(&self) -> usize {
        let end = self.slot_dir_end();
        let start = self.free_end() as usize;
        if start <= end { 0 } else { start - end - SLOT_SIZE }
    }

    /// Insert row bytes. Returns `Some(slot_index)` on success, `None` if no space.
    pub fn insert(&mut self, row: &[u8]) -> Option<usize> {
        let need = row.len() + SLOT_SIZE;
        if self.free_space() < need {
            return None;
        }
        let n = self.raw_n_slots();
        let new_free = self.free_end() as usize - row.len();
        let slot_offset = new_free as u16;
        let slot_len = row.len() as u16;

        self.data[new_free..new_free + row.len()].copy_from_slice(row);
        self.set_free_end(new_free as u16);
        let slot_pos = SLOT_DIR_START + n as usize * SLOT_SIZE;
        self.data[slot_pos..slot_pos + 2].copy_from_slice(&slot_offset.to_le_bytes());
        self.data[slot_pos + 2..slot_pos + 4].copy_from_slice(&slot_len.to_le_bytes());
        self.set_n_slots(n + 1);
        Some(n as usize)
    }

    /// Get row bytes at slot. Returns `None` if slot invalid.
    pub fn get_slot(&self, slot_id: usize) -> Option<&[u8]> {
        if slot_id >= self.raw_n_slots() as usize {
            return None;
        }
        let pos = SLOT_DIR_START + slot_id * SLOT_SIZE;
        let offset = u16::from_le_bytes(self.data[pos..pos + 2].try_into().unwrap()) as usize;
        let len = u16::from_le_bytes(self.data[pos + 2..pos + 4].try_into().unwrap()) as usize;
        if offset + len > PAGE_SIZE {
            return None;
        }
        Some(&self.data[offset..offset + len])
    }

    /// Mark row at slot as deleted (tombstone = 1). Row must have at least ROW_HEADER_LEN bytes.
    pub fn delete_slot(&mut self, slot_id: usize) -> Result<()> {
        if slot_id >= self.raw_n_slots() as usize {
            bail!("invalid slot {}", slot_id);
        }
        let pos = SLOT_DIR_START + slot_id * SLOT_SIZE;
        let offset = u16::from_le_bytes(self.data[pos..pos + 2].try_into().unwrap()) as usize;
        let len = u16::from_le_bytes(self.data[pos + 2..pos + 4].try_into().unwrap()) as usize;
        ensure!(len >= ROW_HEADER_LEN, "row too short for tombstone");
        let tombstone_offset = offset + 8; // after txn_id
        self.data[tombstone_offset] = 1;
        Ok(())
    }

    /// Iterator over (slot_id, row_bytes). Skips tombstoned rows if you check header yourself.
    pub fn iter_slots(&self) -> impl Iterator<Item = (usize, &[u8])> {
        let n = self.raw_n_slots() as usize;
        (0..n).filter_map(move |i| self.get_slot(i).map(|r| (i, r)))
    }

    pub fn n_slots(&self) -> usize {
        self.raw_n_slots() as usize
    }

    /// Read page from a Seek + Read (e.g. `File`).
    pub fn read<R: Read + Seek>(r: &mut R) -> Result<Self> {
        let mut data = [0u8; PAGE_SIZE];
        r.read_exact(&mut data)?;
        let p = Self { data };
        ensure!(p.magic() == PAGE_MAGIC, "invalid page magic");
        Ok(p)
    }

    /// Read page at offset `page_id * PAGE_SIZE` in file.
    pub fn read_at<R: Read + Seek>(r: &mut R, page_id: u32) -> Result<Self> {
        r.seek(SeekFrom::Start((page_id as u64) * (PAGE_SIZE as u64)))?;
        Self::read(r)
    }

    /// Write entire page to Write + Seek.
    pub fn write<W: Write + Seek>(&self, w: &mut W) -> Result<()> {
        w.write_all(&self.data)?;
        Ok(())
    }

    /// Write page at offset `page_id * PAGE_SIZE`.
    pub fn write_at<W: Write + Seek>(&self, w: &mut W, page_id: u32) -> Result<()> {
        w.seek(SeekFrom::Start((page_id as u64) * (PAGE_SIZE as u64)))?;
        self.write(w)
    }

    pub fn as_bytes(&self) -> &[u8; PAGE_SIZE] {
        &self.data
    }

    /// Mutable byte slice for B-tree and other formats that lay out the body manually.
    pub fn as_bytes_mut(&mut self) -> &mut [u8; PAGE_SIZE] {
        &mut self.data
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn page_header_roundtrip() {
        let p = Page::new(7, PageFlags::Heap);
        assert_eq!(p.page_id(), 7);
        assert_eq!(p.n_slots(), 0);
        assert_eq!(p.free_space(), PAGE_SIZE - HEADER_LEN - SLOT_SIZE);
    }

    #[test]
    fn insert_get_one() {
        let mut p = Page::new(0, PageFlags::Heap);
        let row = b"hello world";
        let idx = p.insert(row).unwrap();
        assert_eq!(idx, 0);
        assert_eq!(p.get_slot(0).unwrap(), row);
    }

    #[test]
    fn insert_get_delete() {
        let mut p = Page::new(0, PageFlags::Heap);
        let mut row = vec![0u8; 20];
        row[0..8].copy_from_slice(&1u64.to_le_bytes());
        row[8] = 0;
        let idx = p.insert(&row).unwrap();
        p.delete_slot(idx).unwrap();
        let s = p.get_slot(idx).unwrap();
        assert_eq!(s[8], 1);
    }

    #[test]
    fn insert_fill_then_no_space() {
        let mut p = Page::new(0, PageFlags::Heap);
        let mut n = 0;
        while p.insert(&[0u8; 64]).is_some() {
            n += 1;
        }
        assert!(n > 0);
        assert!(p.insert(&[0u8; 64]).is_none());
    }

    #[test]
    fn read_write_roundtrip() {
        let mut p = Page::new(1, PageFlags::Heap);
        p.insert(b"row1").unwrap();
        p.insert(b"row2").unwrap();
        let mut buf = Cursor::new(vec![0u8; PAGE_SIZE * 2]);
        p.write_at(&mut buf, 0).unwrap();
        buf.set_position(0);
        let q = Page::read_at(&mut buf, 0).unwrap();
        assert_eq!(q.page_id(), 1);
        assert_eq!(q.get_slot(0).unwrap(), b"row1");
        assert_eq!(q.get_slot(1).unwrap(), b"row2");
    }
}
