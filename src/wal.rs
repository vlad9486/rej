use std::{
    io,
    ops::DerefMut,
    sync::{Mutex, MutexGuard},
};

use thiserror::Error;

use super::{
    page::{PagePtr, RawPtr},
    runtime::{Alloc, Free, PlainData, AbstractIo, PageKind},
    file::FileIo,
};

#[derive(Debug, Error)]
pub enum WalError {
    #[error("{0}")]
    Io(#[from] io::Error),
    #[error("bad write-ahead log")]
    BadWal,
}

#[derive(Debug)]
pub struct DbStats {
    pub total: u32,
    pub cached: u32,
    pub free: u32,
    pub used: u32,
    pub seq: u64,
    pub writes: u32,
}

pub struct Wal(Mutex<RecordSeq>);

impl Wal {
    const SIZE: u32 = 0x100;

    pub fn new(create: bool, file: &FileIo) -> Result<Self, WalError> {
        if create {
            let head = PagePtr::from_raw_number(Self::SIZE)
                .ok_or(io::Error::from(io::ErrorKind::UnexpectedEof))?;
            for pos in 0..Self::SIZE {
                let inner = RecordSeq {
                    seq: pos.into(),
                    garbage: FreelistCache::empty(),
                    cache: FreelistCache::empty(),
                    size: Self::SIZE + 1,
                    __padding: 0,
                    freelist: None,
                    head,
                    orphan: None,
                };
                let page = RecordPage::new(inner);
                let ptr = file.grow(pos, 1)?;

                file.write(ptr, PageKind::Log, page)?;
            }
            let head = file.grow(Self::SIZE, 1)?.expect("must yield some");

            let s = Self(Mutex::new(RecordSeq {
                seq: (Self::SIZE - 1).into(),
                garbage: FreelistCache::empty(),
                cache: FreelistCache::empty(),
                size: Self::SIZE + 1,
                __padding: 0,
                freelist: None,
                head,
                orphan: None,
            }));
            s.lock().fill_cache(file, None)?;
            file.sync()?;

            log::info!("did initialize empty database");

            Ok(s)
        } else {
            let it = (0..Self::SIZE)
                .map(PagePtr::<RecordPage>::from_raw_number)
                .map(|ptr| file.read(ptr))
                .filter_map(|p| p.check().copied());

            let inner = it.max_by(|a, b| a.seq.cmp(&b.seq));

            let wal = inner.map(Mutex::new).map(Self).ok_or(WalError::BadWal)?;

            let mut lock = wal.lock();
            let stats = lock.stats(file);
            log::info!("did open database, will unroll log, stats: {stats:?}");
            lock.unroll(file)?;
            let orphan = lock.orphan_mut().take();
            lock.fill_cache(file, orphan)?;
            drop(lock);
            log::info!("did unroll log");

            Ok(wal)
        }
    }

    pub fn lock(&self) -> WalLock<'_> {
        WalLock(self.0.lock().expect("poisoned"))
    }
}

pub struct WalLock<'a>(MutexGuard<'a, RecordSeq>);

impl WalLock<'_> {
    pub fn stats(&self, file: &FileIo) -> DbStats {
        let total = self.0.size - Wal::SIZE;
        let cached = self.0.cache.len();
        let free = self.freelist_size(file) + self.0.garbage.len();
        let used = total - cached - free;
        let seq = self.0.seq;

        DbStats {
            total,
            cached,
            free,
            used,
            seq,
            writes: file.writes(),
        }
    }

    fn ptr(&self) -> Option<PagePtr<RecordPage>> {
        Self::seq_to_ptr(self.0.seq)
    }

    fn seq_to_ptr(seq: u64) -> Option<PagePtr<RecordPage>> {
        let pos = (seq % u64::from(Wal::SIZE)) as u32;
        PagePtr::<RecordPage>::from_raw_number(pos)
    }

    fn next(&mut self) {
        self.0.seq = self.0.seq.wrapping_add(1);
    }

    fn write(&mut self, file: &FileIo) -> Result<(), WalError> {
        self.next();
        let page = RecordPage::new(*self.0);
        file.write(self.ptr(), PageKind::Log, page)?;

        Ok(())
    }

    fn unroll(&mut self, file: &FileIo) -> Result<(), WalError> {
        let mut reverse = self.0.seq;

        loop {
            let page = file.read(Self::seq_to_ptr(reverse));
            if let Some(inner) = page.check() {
                *self.0 = *inner;
                break;
            } else {
                reverse = reverse.wrapping_sub(1);
            }
        }

        file.set_pages(self.0.size)?;

        Ok(())
    }

    fn fill_cache(&mut self, file: &FileIo, orphan: Option<PagePtr<()>>) -> Result<(), WalError> {
        struct FreelistCacheIter<'a>(&'a mut FreelistCache);

        impl<'a> Iterator for FreelistCacheIter<'a> {
            type Item = PagePtr<FreePage>;

            fn next(&mut self) -> Option<PagePtr<FreePage>> {
                self.0.take()
            }
        }

        let mut freelist = self.0.freelist;
        let (cache, garbage) = self.cache_mut();
        let garbage = FreelistCacheIter(garbage);
        let orphan = orphan.map(|ptr| (PageKind::Data, ptr.cast()));
        let mut iter = garbage.map(|ptr| (PageKind::Tree, ptr)).chain(orphan);

        loop {
            if !cache.is_full() {
                if let Some((_, ptr)) = iter.next() {
                    cache.put(ptr);
                    continue;
                }
            }

            break;
        }

        while let Some((kind, ptr)) = iter.next() {
            let page = FreePage { next: freelist };
            file.write(ptr, kind, page)?;
            freelist = Some(ptr);
        }

        while !self.0.cache.is_full() {
            if let Some(ptr) = freelist {
                self.0.cache.put(ptr);
                freelist = file.read(ptr).next;
            } else {
                break;
            }
        }
        let freelist_change = self.0.freelist != freelist;
        self.0.freelist = freelist;

        let resize = !self.0.cache.is_full();
        if resize {
            let ptr = file
                .grow(self.0.size, self.0.cache.capacity())?
                .expect("grow must yield value");
            self.0.size += self.0.cache.capacity();
            for i in 0..self.0.cache.capacity() {
                self.0.cache.put(ptr.add(i));
            }
        }

        if freelist_change || resize {
            self.write(file)?;
        }

        Ok(())
    }

    pub fn new_head<T>(
        &mut self,
        file: &FileIo,
        head: PagePtr<T>,
        orphan: Option<PagePtr<()>>,
    ) -> Result<(), WalError> {
        self.0.head = head.cast();
        self.write(file)?;
        self.fill_cache(file, orphan)?;

        Ok(())
    }

    pub fn current_head<T>(&self) -> PagePtr<T> {
        self.0.head.cast()
    }

    pub fn cache_mut(&mut self) -> (&mut FreelistCache, &mut FreelistCache) {
        let inner = self.0.deref_mut();
        (&mut inner.cache, &mut inner.garbage)
    }

    pub fn orphan_mut(&mut self) -> &mut Option<PagePtr<()>> {
        &mut self.0.orphan
    }

    fn freelist_size(&self, file: &FileIo) -> u32 {
        let mut x = 0;
        let mut freelist = self.0.freelist;

        while freelist.is_some() {
            x += 1;
            freelist = file.read(freelist).next;
        }
        x
    }
}

#[repr(C, align(0x1000))]
#[derive(Clone, Copy)]
struct RecordPage {
    checksum: u64,
    inner: RecordSeq,
}

impl RecordPage {
    fn new(inner: RecordSeq) -> Self {
        let checksum = crc64::crc64(0, inner.as_bytes());
        RecordPage { checksum, inner }
    }

    fn check_old(&self) -> Option<&RecordSeq> {
        let l = 0xc98;
        (self.checksum == crc64::crc64(0, &self.inner.as_bytes()[..l])).then_some(&self.inner)
    }

    fn check(&self) -> Option<&RecordSeq> {
        (self.checksum == crc64::crc64(0, self.inner.as_bytes()))
            .then_some(&self.inner)
            .or_else(|| self.check_old())
    }
}

#[repr(C)]
#[derive(Clone, Copy)]
struct RecordSeq {
    seq: u64,
    garbage: FreelistCache,
    cache: FreelistCache,
    size: u32,
    __padding: u32,
    freelist: Option<PagePtr<FreePage>>,
    head: PagePtr<()>,
    orphan: Option<PagePtr<()>>,
}

#[derive(Clone, Copy)]
pub struct FreelistCache {
    pos: u32,
    pages: [Option<PagePtr<FreePage>>; CACHE_SIZE],
}

#[cfg(feature = "small")]
pub const CACHE_SIZE: usize = 0x1cf;
#[cfg(not(feature = "small"))]
pub const CACHE_SIZE: usize = 0x18f;

impl Alloc for FreelistCache {
    fn alloc<T>(&mut self) -> PagePtr<T>
    where
        T: PlainData,
    {
        let ptr = self
            .take()
            .expect("BUG: must be big enough, increase size of freelist cache")
            .cast();
        log::debug!("alloc {}, {ptr:?}", T::NAME);
        ptr
    }
}

impl Free for FreelistCache {
    fn free<T>(&mut self, ptr: PagePtr<T>)
    where
        T: PlainData,
    {
        if self.is_full() {
            panic!("BUG: must have enough space, increase size of freelist cache");
        }
        log::debug!("free {} {:?}", T::NAME, ptr);
        self.put(ptr.cast());
    }
}

impl FreelistCache {
    pub const SIZE: u32 = CACHE_SIZE as u32;

    const fn empty() -> Self {
        FreelistCache {
            pos: 0,
            pages: [None; CACHE_SIZE],
        }
    }

    const fn is_full(&self) -> bool {
        self.capacity() == 0
    }

    const fn capacity(&self) -> u32 {
        Self::SIZE - self.pos
    }

    const fn is_empty(&self) -> bool {
        self.len() == 0
    }

    const fn len(&self) -> u32 {
        self.pos
    }

    fn put(&mut self, ptr: PagePtr<FreePage>) {
        self.pages[self.pos as usize] = Some(ptr);
        self.pos += 1;
    }

    fn take(&mut self) -> Option<PagePtr<FreePage>> {
        if self.is_empty() {
            None
        } else {
            self.pos -= 1;
            self.pages[self.pos as usize]
        }
    }
}

unsafe impl PlainData for RecordPage {
    const NAME: &str = "Record";
}

unsafe impl PlainData for RecordSeq {
    const NAME: &str = "RecordInner";
}

#[repr(C, align(0x1000))]
#[derive(Clone, Copy)]
struct FreePage {
    next: Option<PagePtr<FreePage>>,
}

unsafe impl PlainData for FreePage {
    const NAME: &str = "Free";
}
