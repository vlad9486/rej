use std::{io, path::Path};

use thiserror::Error;

use super::{
    page::PagePtr,
    runtime::{AbstractIo, AbstractViewer, Rt, Alloc, Free},
    file::{FileIo, IoOptions},
    btree,
    node::Key,
    wal::{Wal, WalError, DbStats},
    value::MetadataPage,
};

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct DbValue {
    ptr: PagePtr<MetadataPage>,
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

    pub fn allocate(&self) -> Result<DbValue, DbError> {
        let mut wal_lock = self.wal.lock();
        let (alloc, _) = wal_lock.cache_mut();
        let ptr = alloc.alloc();
        wal_lock.fill_cache(&self.file)?;

        Ok(DbValue { ptr })
    }

    pub fn deallocate(&self, value: DbValue) -> Result<(), WalError> {
        let mut wal_lock = self.wal.lock();
        let (_, free) = wal_lock.cache_mut();
        free.free(value.ptr);
        wal_lock.collect_garbage(&self.file)
    }

    /// # Panics
    /// if buf length is bigger than `DataPage::CAPACITY`
    /// unlimited value size is not implemented yet
    pub fn write(&self, value: &DbValue, buf: &[u8]) -> io::Result<()> {
        let mut page = MetadataPage::empty();
        let len = page.put_data(buf);
        self.file.write_range(value.ptr, &page, 0..len)
    }

    pub fn length(&self, value: &DbValue) -> usize {
        self.file.read().page(value.ptr).len()
    }

    /// # Panics
    /// if offset plus buf length is smaller than the value size
    pub fn read(&self, value: &DbValue, offset: usize, buf: &mut [u8]) {
        self.file.read().page(value.ptr).read(offset, buf);
    }

    pub fn read_to_vec(&self, value: &DbValue) -> Vec<u8> {
        let view = self.file.read();
        let value = view.page(value.ptr);
        let mut buf = vec![0; value.len()];
        value.read(0, &mut buf);
        buf
    }

    #[cfg(test)]
    pub fn print<K, D>(&self, k: K)
    where
        K: Fn(&[u8]) -> D,
        D: std::fmt::Display,
    {
        let mut wal_lock = self.wal.lock();
        let old_head = wal_lock.current_head();
        let (alloc, free) = wal_lock.cache_mut();
        let io = &self.file;
        let mut storage = Default::default();
        let rt = Rt::new(alloc, free, io, &mut storage);

        btree::print(rt, old_head, k, true);
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

    pub fn insert(
        &self,
        value: &DbValue,
        table_id: u32,
        key: &[u8],
    ) -> Result<Option<DbValue>, DbError> {
        let key = Key {
            table_id,
            bytes: key.into(),
        };

        let mut wal_lock = self.wal.lock();
        let old_head = wal_lock.current_head();
        let (alloc, free) = wal_lock.cache_mut();
        let io = &self.file;
        let mut storage = Default::default();
        let mut rt = Rt::new(alloc, free, io, &mut storage);
        let (new_head, old) = btree::insert(rt.reborrow(), old_head, value.ptr, key)?;
        rt.flush()?;
        wal_lock.new_head(&self.file, new_head)?;

        Ok(old.map(|ptr| DbValue { ptr }))
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
        let mut rt = Rt::new(alloc, free, io, &mut storage);
        let (new_head, ptr) = btree::remove(rt.reborrow(), old_head, key)?;
        rt.flush()?;
        wal_lock.new_head(&self.file, new_head)?;

        Ok(ptr.map(|ptr| DbValue { ptr }))
    }

    pub fn stats(&self) -> DbStats {
        self.wal.lock().stats(&self.file)
    }
}
