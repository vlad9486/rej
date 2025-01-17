use std::{io, marker::PhantomData, mem, path::Path};

use thiserror::Error;

use super::{
    page::{PagePtr, RawPtr},
    runtime::{AbstractIo, Rt, Alloc, Free},
    cipher::{CipherError, Params},
    runtime::PlainData,
    file::FileIo,
    wal::{Wal, WalLock, WalError, DbStats},
    value::MetadataPage,
    node::Node,
    btree,
};

pub enum Entry<'a, N, K> {
    Occupied(Occupied<'a, N>),
    Empty(EmptyCell<'a, N>),
    Vacant(Vacant<'a, N, K>),
}

impl<'a, N, K> Entry<'a, N, K>
where
    N: Copy + PlainData + Node,
{
    pub fn into_db_iter(self) -> DbIterator<N> {
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

    pub fn occupied(self) -> Option<Occupied<'a, N>> {
        if let Self::Occupied(v) = self {
            Some(v)
        } else {
            None
        }
    }

    pub fn empty(self) -> Option<EmptyCell<'a, N>> {
        if let Self::Empty(v) = self {
            Some(v)
        } else {
            None
        }
    }

    pub fn vacant(self) -> Option<Vacant<'a, N, K>> {
        if let Self::Vacant(v) = self {
            Some(v)
        } else {
            None
        }
    }
}

pub struct Occupied<'a, N> {
    inner: btree::EntryInner<N>,
    lock: WalLock<'a>,
    file: &'a FileIo,
}

pub struct EmptyCell<'a, N> {
    inner: btree::EntryInner<N>,
    lock: WalLock<'a>,
    file: &'a FileIo,
}

pub struct Vacant<'a, N, K> {
    inner: btree::EntryInner<N>,
    lock: WalLock<'a>,
    file: &'a FileIo,
    bytes: K,
}

#[derive(Clone, Copy)]
pub struct Value<'a> {
    ptr: PagePtr<MetadataPage>,
    file: &'a FileIo,
}

pub struct DbIterator<N> {
    inner: Option<btree::EntryInner<N>>,
}

impl<'a, N, K> Vacant<'a, N, K>
where
    N: Copy + PlainData + Node,
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
            bytes,
        } = self;
        let wal_lock = &mut lock;

        let (alloc, free) = wal_lock.cache_mut();
        let mut storage = Default::default();
        let mut rt = Rt::new(alloc, free, file, &mut storage);

        let ptr = METADATA.then(|| {
            let ptr = rt.create();
            *rt.mutate::<MetadataPage>(ptr) = MetadataPage::empty();
            ptr
        });

        let new_head = inner.insert(rt.reborrow(), ptr, bytes.as_ref());
        rt.flush()?;
        wal_lock.new_head(self.file, new_head)?;

        Ok(ptr.map(|ptr| Value { ptr, file }))
    }
}

impl<'a, N> EmptyCell<'a, N>
where
    N: Copy + PlainData + Node,
{
    pub fn occupy(mut self) -> Occupied<'a, N> {
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
        let new_head = inner.remove(rt.reborrow());
        rt.flush()?;

        wal_lock.new_head(file, new_head)?;

        Ok(())
    }
}

impl<'a, N> Occupied<'a, N>
where
    N: Copy + PlainData + Node,
{
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
        let new_head = inner.remove(rt.reborrow());
        rt.flush()?;

        if let Some(old) = old {
            free.free(old.cast::<MetadataPage>());
        }
        wal_lock.new_head(file, new_head)?;

        Ok(Value { ptr, file })
    }
}

impl Value<'_> {
    pub fn read(&self, offset: usize, buf: &mut [u8]) -> Result<(), DbError> {
        let page = self.file.read_page(self.ptr.raw_number())?;
        buf.clone_from_slice(&page[offset..][..buf.len()]);

        Ok(())
    }

    pub fn read_to_vec(&self, offset: usize, len: usize) -> Result<Vec<u8>, DbError> {
        let mut buf = vec![0; len];
        self.read(offset, &mut buf)?;

        Ok(buf)
    }

    pub fn write_at(&self, offset: usize, buf: &[u8]) -> Result<(), DbError> {
        let mut page = self.file.read_page(self.ptr.raw_number())?;
        page[offset..][..buf.len()].clone_from_slice(buf);
        self.file.write_page(self.ptr.raw_number(), page)?;

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

pub struct Db<N> {
    file: FileIo,
    wal: Wal,
    phantom_data: PhantomData<N>,
}

impl<N> Db<N> {
    pub fn new(path: impl AsRef<Path>, params: Params) -> Result<Self, DbError> {
        let create = params.create();
        let file = FileIo::new(path, params)?;
        let wal = Wal::new(create, &file)?;

        Ok(Db {
            file,
            wal,
            phantom_data: PhantomData,
        })
    }

    /// Makes sense only for encrypted database
    pub fn m_lock(&self) {
        self.file.m_lock();
    }

    pub fn sync(&self) -> Result<(), DbError> {
        self.file.sync()?;

        Ok(())
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

    pub fn stats(&self) -> DbStats {
        self.wal.lock().stats(&self.file)
    }
}

impl<N> Db<N>
where
    N: Copy + PlainData + Node,
{
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

        btree::print::<N, K, D>(rt, old_head, k, true);
    }

    pub fn entry<K>(&self, bytes: K) -> Entry<'_, N, K>
    where
        K: AsRef<[u8]>,
    {
        let lock = self.wal.lock();
        let file = &self.file;

        let (inner, occupied) = btree::EntryInner::new(file, lock.current_head(), bytes.as_ref());
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
                bytes,
            })
        }
    }

    pub fn next<'a>(&'a self, it: &mut DbIterator<N>) -> Option<(Vec<u8>, Option<Value<'a>>)> {
        let file = &self.file;
        let inner = it.inner.as_mut()?;
        let key = inner.key(file);
        let value = inner.meta().map(|ptr| Value { ptr, file });

        btree::EntryInner::next(&mut it.inner, file);

        Some((key, value))
    }
}
