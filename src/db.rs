use std::{io, path::Path};

use thiserror::Error;

use super::{
    file::{FileIo, IoOptions, PlainData, PageView},
    page::PagePtr,
    wal::{Wal, WalError},
};

pub struct DbView<'a>(PageView<'a>);

impl DbView<'_> {
    pub fn page<T>(&self, ptr: PagePtr<T>) -> &T
    where
        T: PlainData,
    {
        self.0.page(Some(ptr))
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

    pub fn read(&self) -> DbView<'_> {
        DbView(self.file.read())
    }

    pub fn alloc<T>(&self) -> Result<PagePtr<T>, DbError> {
        Ok(self.wal.lock().alloc(&self.file)?)
    }

    pub fn free<T>(&self, ptr: PagePtr<T>) -> Result<(), DbError> {
        Ok(self.wal.lock().free(&self.file, ptr)?)
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempdir::TempDir;

    use super::{IoOptions, Db, super::page::RawPtr};

    #[test]
    fn allocate() {
        let env = env_logger::Env::new().filter_or("RUST_LOG", "info");
        env_logger::try_init_from_env(env).unwrap_or_default();

        let cfg = IoOptions::default();
        let dir = TempDir::new("rej").unwrap();
        let path = dir.path().join("test-basic");

        let db = Db::new(&path, cfg).unwrap();
        let ptr = db.alloc::<()>().unwrap();
        assert_eq!(ptr.raw_number(), 0x100);
        db.free(ptr).unwrap();
        drop(db);

        let db = Db::new(&path, cfg).unwrap();
        let ptr = db.alloc::<()>().unwrap();
        assert_eq!(ptr.raw_number(), 0x100);
        drop(db);

        fs::copy(path, "target/db").unwrap();
    }
}
