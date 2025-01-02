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

pub enum Entry<'a> {
    Occupied(Occupied<'a>),
    Vacant(Vacant<'a>),
}

impl<'a> Entry<'a> {
    pub fn into_db_iter(self) -> DbIterator {
        match self {
            Self::Occupied(v) => {
                let view = v.file.read();
                DbIterator {
                    inner: v.inner.has_next(&view).then_some(v.inner),
                }
            }
            Self::Vacant(v) => {
                let view = v.file.read();
                DbIterator {
                    inner: v.inner.has_next(&view).then_some(v.inner),
                }
            }
        }
    }

    pub fn occupied(self) -> Option<Occupied<'a>> {
        match self {
            Self::Occupied(v) => Some(v),
            Self::Vacant(_) => None,
        }
    }

    pub fn vacant(self) -> Option<Vacant<'a>> {
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

#[derive(Clone, Copy)]
pub struct Value<'a> {
    ptr: PagePtr<MetadataPage>,
    file: &'a FileIo,
}

pub struct Vacant<'a> {
    inner: btree::EntryInner,
    lock: WalLock<'a>,
    file: &'a FileIo,
    key: Key<'a>,
}

pub struct DbIterator {
    inner: Option<btree::EntryInner>,
}

impl<'a> Vacant<'a> {
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
        wal_lock.fill_cache(file)?;

        let (alloc, free) = wal_lock.cache_mut();
        let mut storage = Default::default();
        let mut rt = Rt::new(alloc, free, file, &mut storage);
        let new_head = inner.insert(rt.reborrow(), ptr, &path)?;
        rt.flush()?;
        wal_lock.new_head(self.file, new_head)?;

        Ok(Value { ptr, file })
    }
}

impl Value<'_> {
    pub fn length(&self) -> usize {
        self.file.read().page(self.ptr).len()
    }

    pub fn read(&self, offset: usize, buf: &mut [u8]) {
        let view = self.file.read();
        view.page(self.ptr).read(&view, offset, buf);
    }

    pub fn read_to_vec(&self) -> Vec<u8> {
        let view = self.file.read();
        let value = view.page(self.ptr);
        let mut buf = vec![0; value.len()];
        value.read(&view, 0, &mut buf);
        buf
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

    pub fn entry<'a, 'b>(&'a self, table_id: u32, key: &'b [u8]) -> Entry<'a>
    where
        'b: 'a,
    {
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
        let (table_id, k) = (key.table_id, key.bytes.into_owned());

        let value = Value {
            ptr: inner.meta(),
            file: &self.file,
        };

        inner.next(&view);
        if !inner.has_next(&view) {
            it.inner = None;
        }

        Some((table_id, k, value))
    }

    pub fn stats(&self) -> DbStats {
        self.wal.lock().stats(&self.file)
    }

    /// # Panics
    /// if buf length is bigger than `1536 kiB`
    pub fn rewrite(&self, value: Value<'_>, buf: &[u8]) -> Result<(), DbError> {
        let mut wal_lock = self.wal.lock();
        let (alloc, free) = wal_lock.cache_mut();

        let mut page = *value.file.read().page(value.ptr);
        page.deallocate(free);
        page.put_data(alloc, value.file, buf)?;
        self.file.write(value.ptr, &page)?;
        self.file.sync()?;

        wal_lock.fill_cache(value.file)?;

        Ok(())
    }

    pub fn write_at(&self, value: Value<'_>, offset: usize, buf: &[u8]) -> Result<(), DbError> {
        let mut page = *value.file.read().page(value.ptr);
        page.put_at(offset, buf);
        value.file.write(value.ptr, &page)?;
        value.file.sync()?;

        Ok(())
    }
}

pub mod ext {
    use std::sync::Arc;

    use super::{DbIterator, AbstractIo, AbstractViewer, btree};
    pub use super::{Db, DbError, Entry};

    fn va(view: &impl AbstractViewer, inner: &btree::EntryInner) -> Vec<u8> {
        let ptr = inner.meta();
        let value = view.page(ptr);
        let mut buf = vec![0; value.len()];
        value.read(view, 0, &mut buf);
        buf
    }

    pub fn get(db: &Db, table_id: u32, key: &[u8]) -> Option<Vec<u8>> {
        Some(
            db.entry(table_id, key)
                .occupied()?
                .into_value()
                .read_to_vec(),
        )
    }

    pub fn put(db: &Db, table_id: u32, key: &[u8], buf: &[u8]) -> Result<(), DbError> {
        match db.entry(table_id, key) {
            Entry::Occupied(v) => db.rewrite(v.into_value(), buf),
            Entry::Vacant(v) => db.rewrite(v.insert()?, buf),
        }
    }

    pub fn del(db: &Db, table_id: u32, key: &[u8]) -> Result<(), DbError> {
        match db.entry(table_id, key) {
            Entry::Occupied(v) => v.remove().map(drop),
            Entry::Vacant(_) => Ok(()),
        }
    }

    pub fn edit<F, T, E>(db: &Db, table_id: u32, key: &[u8], f: F) -> Result<Result<T, E>, DbError>
    where
        F: Fn(Vec<u8>) -> Result<(Vec<u8>, T), E>,
    {
        let x = match db.entry(table_id, key) {
            Entry::Occupied(v) => {
                let view = v.file.read();
                let bytes = va(&view, &v.inner);
                let (new, x) = match f(bytes) {
                    Ok(v) => v,
                    Err(err) => return Ok(Err(err)),
                };
                db.rewrite(v.into_value(), &new)?;
                x
            }
            Entry::Vacant(v) => {
                let (new, x) = match f(vec![]) {
                    Ok(v) => v,
                    Err(err) => return Ok(Err(err)),
                };
                db.rewrite(v.insert()?, &new)?;
                x
            }
        };

        Ok(Ok(x))
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
                .map(|(_, k, v)| (k, v.read_to_vec()))
        }
    }
}
