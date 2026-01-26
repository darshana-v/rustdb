//! Heap file: create/open, append pages, read pages. One file per table.

use anyhow::{ensure, Result};
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::Path;

#[allow(unused_imports)]
use super::page::{Page, PageFlags, PAGE_SIZE};

pub type PageId = u32;

/// A heap file stores pages sequentially
// Page N lives at: offset N * PAGE_SIZE.
pub struct HeapFile {
    path: std::path::PathBuf,
    file: File,
    num_pages: PageId,
}

impl HeapFile {
    /// Create a new heap file. Overwrites if it exists.
    pub fn create<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)?;
        Ok(Self {
            path,
            file,
            num_pages: 0,
        })
    }

    /// Open an existing heap file. Returns error if file doesn't exist.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new().read(true).write(true).open(&path)?;
        let len = file.metadata()?.len();
        ensure!(
            len % (PAGE_SIZE as u64) == 0,
            "heap file size not multiple of page size"
        );
        let num_pages = (len / (PAGE_SIZE as u64)) as PageId;
        Ok(Self {
            path,
            file,
            num_pages,
        })
    }

    /// Append a page to the end of the file. Assigns the next PageId and writes it.
    /// Returns the assigned PageId.
    pub fn append_page(&mut self, page: &Page) -> Result<PageId> {
        let id = self.num_pages;
        let mut p = page.clone();
        p.set_page_id(id);
        let mut w = BufWriter::new(&mut self.file);
        p.write_at(&mut w, id)?;
        w.flush()?;
        self.num_pages += 1;
        Ok(id)
    }

    /// Read a page by id. Returns error if page_id >= num_pages.
    pub fn read_page(&mut self, page_id: PageId) -> Result<Page> {
        ensure!(page_id < self.num_pages, "page id {} out of range", page_id);
        Page::read_at(&mut self.file, page_id)
    }

    /// Number of pages in the file.
    pub fn num_pages(&self) -> PageId {
        self.num_pages
    }

    /// Path to the heap file.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn create_append_read() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path();
        let mut heap = HeapFile::create(path).unwrap();
        assert_eq!(heap.num_pages(), 0);

        let mut p0 = Page::new(0, PageFlags::Heap);
        p0.insert(b"row0").unwrap();
        let id0 = heap.append_page(&p0).unwrap();
        assert_eq!(id0, 0);
        assert_eq!(heap.num_pages(), 1);

        let mut p1 = Page::new(0, PageFlags::Heap);
        p1.insert(b"row1").unwrap();
        let id1 = heap.append_page(&p1).unwrap();
        assert_eq!(id1, 1);
        assert_eq!(heap.num_pages(), 2);

        let r0 = heap.read_page(0).unwrap();
        assert_eq!(r0.get_slot(0).unwrap(), b"row0");
        let r1 = heap.read_page(1).unwrap();
        assert_eq!(r1.get_slot(0).unwrap(), b"row1");
    }

    #[test]
    fn open_existing() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path();
        {
            let mut heap = HeapFile::create(path).unwrap();
            let p = Page::new(0, PageFlags::Heap);
            heap.append_page(&p).unwrap();
        }
        let heap = HeapFile::open(path).unwrap();
        assert_eq!(heap.num_pages(), 1);
    }
}
