use std::{io, mem, path::Path};

use thiserror::Error;

use super::{
    page::{PagePtr, RawPtr},
    runtime::{AbstractIo, AbstractViewer, Rt, Alloc, Free},
    cipher::{CipherError, Params},
    file::{FileIo, IoOptions},
    btree,
    node::Key,
    wal::{Wal, WalLock, WalError, DbStats},
    value::MetadataPage,
};

pub enum Entry<'a, 'k> {
    Occupied(Occupied<'a>),
    Vacant(Vacant<'a, 'k>),
}

impl<'a, 'k> Entry<'a, 'k> {
    pub fn into_db_iter(self) -> DbIterator {
        match self {
            Self::Occupied(v) => {
                let inner = Some(v.inner);
                DbIterator { inner }
            }
            Self::Vacant(v) => {
                let inner = v.inner.has_value().then_some(v.inner);
                DbIterator { inner }
            }
        }
    }

    pub fn occupied(self) -> Option<Occupied<'a>> {
        match self {
            Self::Occupied(v) => Some(v),
            Self::Vacant(_) => None,
        }
    }

    pub fn vacant(self) -> Option<Vacant<'a, 'k>> {
        match self {
            Self::Occupied(_) => None,
            Self::Vacant(v) => Some(v),
        }
    }
}

pub struct Occupied<'a> {
    inner: btree::EntryInner,
    lock: WalLock<'a>,
    file: &'a FileIo,
}

pub struct Vacant<'a, 'k> {
    inner: btree::EntryInner,
    lock: WalLock<'a>,
    file: &'a FileIo,
    key: Key<'k>,
}

#[derive(Clone, Copy)]
pub struct Value<'a> {
    ptr: PagePtr<MetadataPage>,
    file: &'a FileIo,
}

pub struct DbIterator {
    inner: Option<btree::EntryInner>,
}

impl<'a> Vacant<'a, '_> {
    pub fn insert(self) -> Result<Value<'a>, DbError> {
        let Vacant {
            inner,
            mut lock,
            file,
            key: path,
        } = self;
        let wal_lock = &mut lock;

        let (alloc, _) = wal_lock.cache_mut();
        let ptr = alloc.alloc();
        self.file.write(ptr, &MetadataPage::empty())?;

        let (alloc, free) = wal_lock.cache_mut();
        let mut storage = Default::default();
        let mut rt = Rt::new(alloc, free, file, &mut storage);
        let new_head = inner.insert(rt.reborrow(), ptr, &path)?;
        rt.flush()?;
        wal_lock.new_head(self.file, new_head)?;

        Ok(Value { ptr, file })
    }
}

impl<'a> Occupied<'a> {
    pub fn into_value(self) -> Value<'a> {
        let ptr = self.inner.meta();
        let Occupied { file, .. } = self;
        Value { ptr, file }
    }

    pub fn remove(self) -> Result<Value<'a>, DbError> {
        let Occupied {
            inner,
            mut lock,
            file,
        } = self;
        let wal_lock = &mut lock;

        let ptr = inner.meta();

        let old = mem::replace(wal_lock.orphan_mut(), Some(ptr.cast()));
        let (alloc, free) = wal_lock.cache_mut();
        let mut storage = Default::default();
        let mut rt = Rt::new(alloc, free, file, &mut storage);
        let new_head = inner.remove(rt.reborrow())?;
        rt.flush()?;

        if let Some(old) = old {
            free.free(old.cast::<MetadataPage>());
        }
        wal_lock.new_head(file, new_head)?;

        Ok(Value { ptr, file })
    }
}

impl Value<'_> {
    pub fn read(&self, plain: bool, offset: usize, buf: &mut [u8]) {
        let view = self.file.read();
        if plain {
            view.page(self.ptr).read_plain(offset, buf);
        } else {
            view.page(self.ptr).read_indirect(&view, offset, buf);
        }
    }

    pub fn read_to_vec(&self, plain: bool, offset: usize, len: usize) -> Vec<u8> {
        let mut buf = vec![0; len];
        self.read(plain, offset, &mut buf);
        buf
    }
}

#[derive(Debug, Error)]
pub enum DbError {
    #[error("{0}")]
    Io(#[from] io::Error),
    #[error("{0}")]
    WalError(#[from] WalError),
    #[error("cipher: {0}")]
    Cipher(#[from] CipherError),
}

pub struct Db {
    file: FileIo,
    wal: Wal,
}

impl Db {
    pub fn new(path: impl AsRef<Path>, cfg: IoOptions, params: Params) -> Result<Self, DbError> {
        let create = params.create();
        let file = FileIo::new(path, cfg, params)?;
        let wal = Wal::new(create, &file)?;

        Ok(Db { file, wal })
    }

    /// Makes sense only for encrypted database
    pub fn m_lock(&self) {
        self.file.m_lock();
    }

    /// Makes sense only for encrypted database
    pub fn crypt_shred(&self, seed: &[u8]) -> Result<(), DbError> {
        self.file.crypt_shred(seed)?;

        Ok(())
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

    pub fn entry<'a, 'k>(&'a self, table_id: u32, key: &'k [u8]) -> Entry<'a, 'k> {
        let path = Key {
            table_id,
            bytes: key.into(),
        };
        let lock = self.wal.lock();
        let file = &self.file;
        let view = file.read();

        let (inner, occupied) = btree::EntryInner::new(&view, lock.current_head(), &path);
        if occupied {
            Entry::Occupied(Occupied { inner, lock, file })
        } else {
            Entry::Vacant(Vacant {
                inner,
                lock,
                file,
                key: path,
            })
        }
    }

    pub fn next<'a>(&'a self, it: &mut DbIterator) -> Option<(u32, Vec<u8>, Value<'a>)> {
        let inner = it.inner.as_mut()?;
        let view = self.file.read();
        let key = inner.key(&view);
        let value = Value {
            ptr: inner.meta(),
            file: &self.file,
        };

        btree::EntryInner::next(&mut it.inner, &view);

        Some((key.table_id, key.bytes.into_owned(), value))
    }

    pub fn stats(&self) -> DbStats {
        self.wal.lock().stats(&self.file)
    }

    /// # Panics
    /// if buf length is bigger than `1536 kiB`
    pub fn rewrite(&self, value: Value<'_>, plain: bool, buf: &[u8]) -> Result<(), DbError> {
        let mut page = *value.file.read().page(value.ptr);
        if plain {
            page.put_plain_at(0, buf);
            value.file.write(value.ptr, &page)?;
            value.file.sync()?;
        } else {
            let mut wal_lock = self.wal.lock();
            let (alloc, free) = wal_lock.cache_mut();

            page.deallocate_indirect(free);
            page.put_indirect_at(alloc, value.file, 0, buf)?;
            value.file.write(value.ptr, &page)?;
            value.file.sync()?;

            wal_lock.fill_cache(value.file)?;
        }

        Ok(())
    }

    pub fn write_at(
        &self,
        value: Value<'_>,
        plain: bool,
        offset: usize,
        buf: &[u8],
    ) -> Result<(), DbError> {
        let mut page = *value.file.read().page(value.ptr);
        if plain {
            page.put_plain_at(offset, buf);
        } else {
            let mut wal_lock = self.wal.lock();
            let (alloc, _free) = wal_lock.cache_mut();
            page.put_indirect_at(alloc, value.file, offset, buf)?;
        }
        value.file.write(value.ptr, &page)?;
        value.file.sync()?;

        Ok(())
    }
}

pub mod ext {
    use std::sync::Arc;

    use super::{AbstractIo, AbstractViewer, DbIterator, Value};
    pub use super::{Db, DbError, Entry};

    fn decode_header(header: &[u8]) -> usize {
        u64::from_le_bytes(header[..8].try_into().unwrap()) as usize
    }

    fn encode_header(header: &mut [u8], len: usize) {
        header[..8].clone_from_slice(&(len as u64).to_le_bytes());
    }

    fn len(v: &Value<'_>) -> usize {
        let view = v.file.read();
        let metadata = view.page(v.ptr);
        decode_header(metadata.header_plain())
    }

    pub fn get(db: &Db, table_id: u32, key: &[u8]) -> Option<Vec<u8>> {
        let v = db.entry(table_id, key).occupied()?.into_value();
        let len = len(&v);
        let plain = len <= 0xf00;

        Some(v.read_to_vec(plain, 0, len))
    }

    pub fn put(db: &Db, table_id: u32, key: &[u8], buf: &[u8]) -> Result<(), DbError> {
        let v = match db.entry(table_id, key) {
            Entry::Occupied(v) => v.into_value(),
            Entry::Vacant(v) => v.insert()?,
        };
        let len = len(&v);
        if len <= 0xf00 {
            let mut with_header = vec![0; buf.len() + 0x100];
            encode_header(&mut with_header[..0x100], buf.len());
            with_header[0x100..].clone_from_slice(buf);
            db.rewrite(v, true, &with_header)
        } else {
            let mut header = [0; 0x100];
            encode_header(&mut header, buf.len());
            db.rewrite(v, true, &header)?;
            db.rewrite(v, false, buf)
        }
    }

    pub fn del(db: &Db, table_id: u32, key: &[u8]) -> Result<(), DbError> {
        match db.entry(table_id, key) {
            Entry::Occupied(v) => v.remove().map(drop),
            Entry::Vacant(_) => Ok(()),
        }
    }

    pub struct DbIteratorOwned {
        inner: DbIterator,
        db: Arc<Db>,
        table_id: u32,
    }

    pub fn iter(db: Arc<Db>, table_id: u32, key: &[u8]) -> DbIteratorOwned {
        let inner = db.entry(table_id, key).into_db_iter();
        DbIteratorOwned {
            inner,
            db,
            table_id,
        }
    }

    impl Iterator for DbIteratorOwned {
        type Item = (Vec<u8>, Vec<u8>);

        fn next(&mut self) -> Option<Self::Item> {
            self.db
                .next(&mut self.inner)
                .filter(|(table_id, _, _)| *table_id == self.table_id)
                .map(|(_, k, v)| {
                    let len = len(&v);
                    let plain = len <= 0xf00;
                    (k, v.read_to_vec(plain, 0, len))
                })
        }
    }
}
