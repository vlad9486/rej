use std::{io, iter, path::Path};

use thiserror::Error;

use super::{
    file::{FileIo, IoOptions},
    page::{PagePtr, RawPtr},
    wal::{Wal, WalError, WAL_SIZE},
    btree::{self, DataPage},
};

pub struct DbValue {
    len: usize,
    ptr: PagePtr<DataPage>,
}

impl DbValue {
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// # Panics
    /// if buf is smaller than the value size
    pub fn read(&self, db: &Db, buf: &mut [u8]) {
        let read_lock = db.file.read();
        let data = &read_lock.page(self.ptr).data;
        buf.clone_from_slice(&data[..buf.len()]);
    }

    pub fn read_to_vec(&self, db: &Db) -> Vec<u8> {
        let mut v = vec![0; self.len];
        self.read(db, &mut v);
        v
    }

    /// # Panics
    /// if buf is bigger than `DataPage::CAPACITY`
    /// unlimited value size is not implemented yet
    pub fn write(&self, db: &Db, buf: &[u8]) -> io::Result<()> {
        let mut page = DataPage {
            len: buf.len(),
            data: [0; DataPage::CAPACITY],
        };
        page.data[..buf.len()].clone_from_slice(buf);
        db.file.write(self.ptr, &page)
    }
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

    pub fn retrieve(&self, key: &[u8; 11]) -> Option<DbValue> {
        let head_ptr = self.wal.lock().current_head();
        let view = self.file.read();
        let ptr = btree::get(&view, head_ptr, key)?;
        let len = view.page(ptr).len;
        Some(DbValue { len, ptr })
    }

    pub fn insert(&self, key: &[u8; 11]) -> Result<DbValue, DbError> {
        let mut wal_lock = self.wal.lock();

        let mut stem_ptr = iter::repeat_with(|| wal_lock.alloc(&self.file))
            .filter_map(Result::ok)
            .take(6)
            .map(Some)
            .collect::<Vec<_>>();
        let new_head = stem_ptr
            .first()
            .expect("cannot fail")
            .expect("cannot fail")
            .cast();
        let old_head = wal_lock.current_head();
        let ptr = btree::insert(&self.file, old_head, &mut stem_ptr, key)?;
        for unused in stem_ptr {
            if let Some(unused) = unused {
                wal_lock.free(&self.file, unused)?;
            }
        }
        wal_lock.new_head(&self.file, new_head)?;

        let len = self.file.read().page(ptr).len;
        Ok(DbValue { len, ptr })
    }

    // TODO: remove value
    pub fn remove(&self, key: &[u8; 11]) -> Result<(), DbError> {
        let _ = key;
        unimplemented!()
    }

    pub fn stats(&self) -> DbStats {
        let total = self.file.pages() - WAL_SIZE;

        let wal_lock = self.wal.lock();
        let free = wal_lock.free_list_size(&self.file);
        let seq = wal_lock.seq();

        DbStats { total, free, seq }
    }
}
