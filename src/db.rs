use std::{io, path::Path};

use thiserror::Error;

use super::{
    page::PagePtr,
    runtime::{AbstractIo, AbstractViewer, Rt},
    file::{FileIo, IoOptions},
    btree,
    node::Key,
    wal::{Wal, WalError, DbStats},
    value::DataPage,
};

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct DbValue {
    ptr: PagePtr<DataPage>,
}

pub struct DbIterator(btree::It);

#[derive(Debug, Error)]
pub enum DbError {
    #[error("{0}")]
    Io(#[from] io::Error),
    #[error("{0}")]
    WalError(#[from] WalError),
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
        let page_offset = memoffset::offset_of!(DataPage, data) + offset;
        self.file
            .write_range(value.ptr, &page, 0..(page_offset + buf.len()))
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

    pub fn print(&self) {
        let head_ptr = self.wal.lock().current_head();
        let view = self.file.read();
        btree::print(&view, head_ptr);
    }

    pub fn retrieve(&self, table_id: u32, key: &[u8]) -> Option<DbValue> {
        let head_ptr = self.wal.lock().current_head();
        let key = Key {
            table_id,
            bytes: key.into(),
        };
        let view = self.file.read();
        let ptr = btree::get(&view, head_ptr, key)?;
        Some(DbValue { ptr })
    }

    pub fn iterator(&self, table_id: u32, key: Option<&[u8]>, forward: bool) -> DbIterator {
        let head_ptr = self.wal.lock().current_head();
        let view = self.file.read();
        DbIterator(btree::It::new(&view, head_ptr, forward, table_id, key))
    }

    pub fn next(&self, it: &mut DbIterator) -> Option<(Vec<u8>, DbValue)> {
        it.0.next(&self.file.read())
            .map(|(key, ptr)| (key, DbValue { ptr }))
    }

    pub fn insert(&self, table_id: u32, key: &[u8]) -> Result<DbValue, DbError> {
        let key = Key {
            table_id,
            bytes: key.into(),
        };

        let mut wal_lock = self.wal.lock();
        let old_head = wal_lock.current_head();
        let (alloc, free) = wal_lock.cache_mut();
        let io = &self.file;
        let mut storage = Default::default();
        let mut rt = Rt {
            alloc,
            free,
            io,
            storage: &mut storage,
        };
        let (new_head, ptr) = btree::insert(rt.reborrow(), old_head, key)?;
        rt.flush()?;
        wal_lock.new_head(&self.file, new_head)?;

        Ok(DbValue { ptr })
    }

    pub fn remove(&self, table_id: u32, key: &[u8]) -> Result<Option<DbValue>, DbError> {
        let key = Key {
            table_id,
            bytes: key.into(),
        };

        let mut wal_lock = self.wal.lock();
        let old_head = wal_lock.current_head();
        let (alloc, free) = wal_lock.cache_mut();
        let io = &self.file;
        let mut storage = Default::default();
        let mut rt = Rt {
            alloc,
            free,
            io,
            storage: &mut storage,
        };
        let (new_head, ptr) = btree::remove(rt.reborrow(), old_head, key)?;
        rt.flush()?;
        wal_lock.new_head(&self.file, new_head)?;

        Ok(ptr.map(|ptr| DbValue { ptr }))
    }

    pub fn stats(&self) -> DbStats {
        self.wal.lock().stats(&self.file)
    }
}
