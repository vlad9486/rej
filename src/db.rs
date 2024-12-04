use std::{io, path::Path};

use thiserror::Error;

use super::{
    file::{FileIo, IoOptions},
    page::PagePtr,
    wal::{Wal, WalError, WAL_SIZE, FreelistCache},
    btree::{self, DataPage},
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
        if create {
            log::info!("did initialize empty database");
        } else {
            log::info!("did open database, will unroll log");
            wal.lock().unroll(&file)?;
            log::info!("did unroll log");
        }

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
        let read_lock = self.file.read();
        read_lock.page(value.ptr).len
    }

    /// # Panics
    /// if offset plus buf length is smaller than the value size
    pub fn read(&self, value: &DbValue, offset: usize, buf: &mut [u8]) {
        let read_lock = self.file.read();
        let page = read_lock.page(value.ptr);
        let data = &page.data[..page.len][offset..][..buf.len()];
        buf.clone_from_slice(data);
    }

    pub fn read_to_vec(&self, value: &DbValue) -> Vec<u8> {
        let read_lock = self.file.read();
        let page = read_lock.page(value.ptr);
        page.data[..page.len].to_vec()
    }

    pub fn retrieve(&self, key: &[u8]) -> Option<DbValue> {
        let head_ptr = self.wal.lock().current_head();
        let view = self.file.read();
        let ptr = btree::get(&view, head_ptr, key)?;
        Some(DbValue { ptr })
    }

    pub fn insert(&self, key: &[u8]) -> Result<DbValue, DbError> {
        let mut wal_lock = self.wal.lock();

        let old_head = wal_lock.current_head();
        let mut fl_old = wal_lock.freelist_cache();
        while !fl_old.is_full() {
            fl_old.put(wal_lock.alloc(&self.file)?);
        }
        let mut fl_new = FreelistCache::empty();
        let (new_head, ptr) = btree::insert(&self.file, old_head, &mut fl_old, &mut fl_new, key)?;
        for ptr in &mut fl_old {
            if let Some(ptr) = fl_new.put(ptr) {
                wal_lock.free(&self.file, ptr)?;
            }
        }
        wal_lock.new_head(&self.file, new_head, fl_new)?;

        Ok(DbValue { ptr })
    }

    // TODO: remove value
    pub fn remove(&self, key: &[u8; 11]) -> Result<(), DbError> {
        let _ = key;
        unimplemented!()
    }

    pub fn stats(&self) -> DbStats {
        let total = self.file.pages() - WAL_SIZE - FreelistCache::SIZE;

        let wal_lock = self.wal.lock();
        let free = wal_lock.freelist_size(&self.file);
        let seq = wal_lock.seq();

        DbStats { total, free, seq }
    }
}
