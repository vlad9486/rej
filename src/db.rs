use std::{io, path::Path};

use thiserror::Error;

use super::{
    file::{FileIo, IoOptions},
    page::PagePtr,
    wal::{Wal, WalError, WAL_SIZE, FreelistCache},
    btree,
    value::DataPage,
};

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct DbValue {
    ptr: PagePtr<DataPage>,
}

#[derive(Debug, Error)]
pub enum DbError {
    #[error("{0}")]
    Io(#[from] io::Error),
    #[error("{0}")]
    WalError(#[from] WalError),
}

#[derive(Debug)]
pub struct DbStats {
    pub total: u32,
    pub free: u32,
    pub seq: u64,
}

pub struct Db {
    file: FileIo,
    wal: Wal,
}

impl Db {
    pub fn new(path: impl AsRef<Path>, cfg: IoOptions) -> Result<Self, DbError> {
        let create = !path.as_ref().exists();
        let file = FileIo::new(path, create, cfg)?;
        let wal = Wal::new(create, &file)?;

        Ok(Db { file, wal })
    }

    /// # Panics
    /// if offset plus buf length is bigger than `DataPage::CAPACITY`
    /// unlimited value size is not implemented yet
    pub fn write(&self, value: &DbValue, offset: usize, buf: &[u8]) -> io::Result<()> {
        let mut page = DataPage {
            len: buf.len(),
            data: [0; DataPage::CAPACITY],
        };
        page.data[offset..][..buf.len()].clone_from_slice(buf);
        self.file.write(value.ptr, &page)
    }

    pub fn length(&self, value: &DbValue) -> usize {
        self.file.read().page(value.ptr).len
    }

    /// # Panics
    /// if offset plus buf length is smaller than the value size
    pub fn read(&self, value: &DbValue, offset: usize, buf: &mut [u8]) {
        let view = self.file.read();
        let page = view.page(value.ptr);
        let data = &page.data[..page.len][offset..][..buf.len()];
        buf.clone_from_slice(data);
    }

    pub fn read_to_vec(&self, value: &DbValue) -> Vec<u8> {
        let view = self.file.read();
        let page = view.page(value.ptr);
        page.data[..page.len].to_vec()
    }

    pub fn retrieve(&self, key: &[u8]) -> Option<DbValue> {
        let head_ptr = self.wal.lock().current_head();
        let view = self.file.read();
        let ptr = btree::get(&view, head_ptr, key)?;
        Some(DbValue { ptr })
    }

    pub fn iterator(&self, key: Option<&[u8]>, forward: bool) -> DbIterator {
        let head_ptr = self.wal.lock().current_head();
        let view = self.file.read();
        DbIterator(btree::It::new(&view, head_ptr, forward, key))
    }

    pub fn next(&self, it: &mut DbIterator) -> Option<(Vec<u8>, DbValue)> {
        it.0.next(&self.file.read())
            .map(|(key, ptr)| (key, DbValue { ptr }))
    }

    pub fn insert(&self, key: &[u8]) -> Result<DbValue, DbError> {
        let mut wal_lock = self.wal.lock();

        let old_head = wal_lock.current_head();
        let mut fl_old = wal_lock.freelist_cache();
        let mut fl_new = FreelistCache::empty();
        let (new_head, ptr) = btree::insert(&self.file, old_head, &mut fl_old, &mut fl_new, key)?;
        wal_lock.new_head(&self.file, new_head, fl_old, fl_new)?;

        Ok(DbValue { ptr })
    }

    pub fn remove(&self, key: &[u8]) -> Result<Option<DbValue>, DbError> {
        let mut wal_lock = self.wal.lock();

        let old_head = wal_lock.current_head();
        let mut fl_old = wal_lock.freelist_cache();
        let mut fl_new = FreelistCache::empty();
        let (new_head, ptr) = btree::remove(&self.file, old_head, &mut fl_old, &mut fl_new, key)?;
        wal_lock.new_head(&self.file, new_head, fl_old, fl_new)?;

        Ok(ptr.map(|ptr| DbValue { ptr }))
    }

    pub fn stats(&self) -> DbStats {
        let total = self.file.pages() - WAL_SIZE - FreelistCache::SIZE;

        let wal_lock = self.wal.lock();
        let free = wal_lock.freelist_size(&self.file);
        let seq = wal_lock.seq();

        DbStats { total, free, seq }
    }
}

pub struct DbIterator(btree::It);
