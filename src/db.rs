use std::{borrow::Cow, io, mem, path::Path};

use thiserror::Error;

use super::{
    page::{PagePtr, RawPtr},
    runtime::{AbstractIo, Rt, Alloc, Free, PBox},
    cipher::{CipherError, Params},
    file::FileIo,
    btree,
    node::Key,
    wal::{Wal, WalLock, WalError, DbStats},
    value::MetadataPage,
};

pub enum Entry<'a, K> {
    Occupied(Occupied<'a>),
    Empty(EmptyCell<'a>),
    Vacant(Vacant<'a, K>),
}

impl<'a, K> Entry<'a, K> {
    pub fn into_db_iter(self) -> DbIterator {
        match self {
            Self::Occupied(v) => {
                let inner = Some(v.inner);
                DbIterator { inner }
            }
            Self::Empty(v) => {
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
        if let Self::Occupied(v) = self {
            Some(v)
        } else {
            None
        }
    }

    pub fn empty(self) -> Option<EmptyCell<'a>> {
        if let Self::Empty(v) = self {
            Some(v)
        } else {
            None
        }
    }

    pub fn vacant(self) -> Option<Vacant<'a, K>> {
        if let Self::Vacant(v) = self {
            Some(v)
        } else {
            None
        }
    }
}

pub struct Occupied<'a> {
    inner: btree::EntryInner,
    lock: WalLock<'a>,
    file: &'a FileIo,
}

pub struct EmptyCell<'a> {
    inner: btree::EntryInner,
    lock: WalLock<'a>,
    file: &'a FileIo,
}

pub struct Vacant<'a, K> {
    inner: btree::EntryInner,
    lock: WalLock<'a>,
    file: &'a FileIo,
    table_id: u32,
    bytes: K,
}

#[derive(Clone, Copy)]
pub struct Value<'a> {
    ptr: PagePtr<MetadataPage>,
    file: &'a FileIo,
}

pub struct DbIterator {
    inner: Option<btree::EntryInner>,
}

impl<'a, K> Vacant<'a, K>
where
    K: AsRef<[u8]>,
{
    pub fn insert_empty(self) -> Result<(), DbError> {
        self.insert_inner::<false>().map(drop)
    }

    pub fn insert(self) -> Result<Value<'a>, DbError> {
        self.insert_inner::<true>().map(Option::unwrap)
    }

    fn insert_inner<const METADATA: bool>(self) -> Result<Option<Value<'a>>, DbError> {
        let Vacant {
            inner,
            mut lock,
            file,
            table_id,
            bytes,
        } = self;
        let wal_lock = &mut lock;

        let path = Key {
            table_id,
            bytes: Cow::Borrowed(bytes.as_ref()),
        };

        let (alloc, free) = wal_lock.cache_mut();
        let mut storage = Default::default();
        let mut rt = Rt::new(alloc, free, file, &mut storage);

        let ptr = METADATA.then(|| {
            let ptr = rt.create();
            *rt.mutate::<MetadataPage>(ptr) = MetadataPage::empty();
            ptr
        });

        let new_head = inner.insert(rt.reborrow(), ptr, &path)?;
        rt.flush()?;
        wal_lock.new_head(self.file, new_head)?;

        Ok(ptr.map(|ptr| Value { ptr, file }))
    }
}

impl<'a> EmptyCell<'a> {
    pub fn occupy(mut self) -> Occupied<'a> {
        let (alloc, _) = self.lock.cache_mut();
        self.inner.set_meta(alloc.alloc());
        let EmptyCell { inner, lock, file } = self;
        Occupied { inner, lock, file }
    }

    pub fn remove(self) -> Result<(), DbError> {
        let EmptyCell {
            inner,
            mut lock,
            file,
        } = self;
        let wal_lock = &mut lock;

        let (alloc, free) = wal_lock.cache_mut();
        let mut storage = Default::default();
        let mut rt = Rt::new(alloc, free, file, &mut storage);
        let new_head = inner.remove(rt.reborrow())?;
        rt.flush()?;

        wal_lock.new_head(file, new_head)?;

        Ok(())
    }
}

impl<'a> Occupied<'a> {
    pub fn into_value(self) -> Value<'a> {
        self.as_value()
    }

    pub fn as_value(&self) -> Value<'a> {
        let ptr = self.inner.meta().expect("must be metadata");
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

        let ptr = inner.meta().expect("must be metadata");
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
    pub fn read(&self, offset: usize, buf: &mut [u8]) {
        let mut page = PBox::new(4096, [0; 0x1000]);
        self.file
            .read_page(self.ptr.cast(), &mut page)
            .expect("read should not fail");
        buf.clone_from_slice(&page[offset..][..buf.len()]);
    }

    pub fn read_to_vec(&self, offset: usize, len: usize) -> Vec<u8> {
        let mut buf = vec![0; len];
        self.read(offset, &mut buf);
        buf
    }

    pub fn write_at(&self, offset: usize, buf: &[u8]) -> Result<(), DbError> {
        let mut page = PBox::new(4096, [0; 0x1000]);
        self.file.read_page(self.ptr.cast(), &mut page)?;
        page[offset..][..buf.len()].clone_from_slice(buf);
        self.file.write_bytes(self.ptr.cast(), &mut *page)?;

        Ok(())
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
    pub fn new(path: impl AsRef<Path>, params: Params) -> Result<Self, DbError> {
        let create = params.create();
        let file = FileIo::new(path, params)?;
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
    pub fn with_simulator(mut self, crash_at: u32, mess_page: bool) -> Self {
        use super::file::Simulator;

        self.file.simulator = Simulator {
            crash_at,
            mess_page,
        };
        self
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

    pub fn entry<K>(&self, table_id: u32, bytes: K) -> Entry<'_, K>
    where
        K: AsRef<[u8]>,
    {
        let path = Key {
            table_id,
            bytes: Cow::Borrowed(bytes.as_ref()),
        };
        let lock = self.wal.lock();
        let file = &self.file;

        let (inner, occupied) = btree::EntryInner::new(file, lock.current_head(), &path);
        if occupied {
            if inner.meta().is_some() {
                Entry::Occupied(Occupied { inner, lock, file })
            } else {
                Entry::Empty(EmptyCell { inner, lock, file })
            }
        } else {
            Entry::Vacant(Vacant {
                inner,
                lock,
                file,
                table_id,
                bytes,
            })
        }
    }

    pub fn next<'a>(&'a self, it: &mut DbIterator) -> Option<(u32, Vec<u8>, Option<Value<'a>>)> {
        let file = &self.file;
        let inner = it.inner.as_mut()?;
        let key = inner.key(file);
        let value = inner.meta().map(|ptr| Value { ptr, file });

        btree::EntryInner::next(&mut it.inner, file);

        Some((key.table_id, key.bytes.into_owned(), value))
    }

    pub fn stats(&self) -> DbStats {
        self.wal.lock().stats(&self.file)
    }
}
