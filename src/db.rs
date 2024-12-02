use std::{io, iter, path::Path};

use thiserror::Error;

use super::{
    file::{FileIo, IoOptions},
    page::PagePtr,
    wal::{Wal, WalError},
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
}

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
        wal.lock().unroll(&file)?;

        Ok(Db { file, wal })
    }

    pub fn get(&self, key: &[u8; 11]) -> Option<DbValue> {
        let wal_lock = self.wal.lock();
        let view = self.file.read();
        let ptr = btree::get(&view, wal_lock.current_head(), key)?;
        let len = view.page(ptr).len;
        Some(DbValue { len, ptr })
    }

    pub fn insert(&self, key: &[u8; 11], data: &[u8]) -> Result<Option<DbValue>, DbError> {
        let mut wal_lock = self.wal.lock();
        let data_ptr = wal_lock.alloc(&self.file)?;
        let mut page = DataPage {
            len: data.len(),
            data: [0; DataPage::CAPACITY],
        };
        page.data[..data.len()].clone_from_slice(data);
        self.file.write(data_ptr, &page)?;

        let stem_ptr = iter::repeat_with(|| wal_lock.alloc(&self.file))
            .filter_map(Result::ok)
            .take(6)
            .collect::<Vec<_>>();
        let mut stem_ptr_slice = stem_ptr.as_slice();
        let new_head = *stem_ptr_slice.first().unwrap();
        let old_head = wal_lock.current_head();
        let old_ptr = btree::insert(&self.file, old_head, &mut stem_ptr_slice, key, data_ptr)?;
        for unused in stem_ptr_slice {
            wal_lock.free(&self.file, *unused)?;
        }
        wal_lock.new_head(&self.file, new_head)?;

        Ok(old_ptr.map(|ptr| {
            let len = self.file.read().page(ptr).len;
            DbValue { len, ptr }
        }))
    }
}
