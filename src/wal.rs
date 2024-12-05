use std::{io, mem};

use parking_lot::{Mutex, MutexGuard};
use thiserror::Error;

use super::{
    file::{FileIo, PlainData, PageView},
    page::{PagePtr, RawPtr, PAGE_SIZE},
    node::{Alloc, Free, NodePage},
};

#[derive(Debug, Error)]
pub enum WalError {
    #[error("{0}")]
    Io(#[from] io::Error),
    #[error("bad write-ahead log")]
    BadWal,
}

pub const WAL_SIZE: u32 = 0x100;

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
    pub fn new(create: bool, file: &FileIo) -> Result<Self, WalError> {
        if create {
            let head = PagePtr::from_raw_number(WAL_SIZE)
                .ok_or(io::Error::from(io::ErrorKind::UnexpectedEof))?;
            for pos in 0..WAL_SIZE {
                let inner = RecordSeq {
                    seq: pos.into(),
                    size: WAL_SIZE + 1,
                    freelist: None,
                    head,
                    garbage: FreelistCache::empty(),
                    cache: FreelistCache::empty(),
                };
                let page = RecordPage::new(inner);
                let ptr = file.grow(1)?;

                file.write(ptr, &page)?;
            }
            let head = file.grow(1)?.expect("must yield some");

            file.sync()?;

            let s = Self(Mutex::new(RecordSeq {
                seq: (WAL_SIZE - 1).into(),
                size: file.pages(),
                freelist: None,
                head,
                garbage: FreelistCache::empty(),
                cache: FreelistCache::empty(),
            }));
            s.lock().fill_cache(file)?;

            log::info!("did initialize empty database");

            Ok(s)
        } else {
            let view = file.read();

            let it = (0..WAL_SIZE)
                .map(PagePtr::from_raw_number)
                .map(|ptr| view.page::<RecordPage>(ptr))
                .filter_map(RecordPage::check);

            let mut inner = None::<&RecordSeq>;
            for item in it {
                if inner.map_or(0, |i| i.seq) > item.seq {
                    break;
                } else {
                    inner = Some(item);
                }
            }

            let wal = inner
                .copied()
                .map(Mutex::new)
                .map(Self)
                .ok_or(WalError::BadWal)?;

            let mut lock = wal.lock();
            let stats = lock.stats(&file);
            log::info!("did open database, will unroll log, stats: {stats:?}");
            lock.unroll(file, view)?;
            lock.collect_garbage(file)?;
            lock.fill_cache(file)?;
            drop(lock);
            log::info!("did unroll log");

            Ok(wal)
        }
    }

    pub fn lock(&self) -> WalLock<'_> {
        WalLock(self.0.lock())
    }
}

pub struct WalLock<'a>(MutexGuard<'a, RecordSeq>);

impl WalLock<'_> {
    pub fn stats(&self, file: &FileIo) -> DbStats {
        let total = file.pages() - WAL_SIZE;
        let cached = self.0.cache.len();
        let free = self.freelist_size(file);
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
        let pos = (seq % u64::from(WAL_SIZE)) as u32;
        PagePtr::<RecordPage>::from_raw_number(pos)
    }

    fn next(&mut self) {
        self.0.seq = self.0.seq.wrapping_add(1);
    }

    fn write(&mut self, file: &FileIo) -> Result<(), WalError> {
        self.next();
        let page = RecordPage::new(*self.0);
        file.write(self.ptr(), &page)?;
        file.sync()?;

        Ok(())
    }

    fn unroll(&mut self, file: &FileIo, view: PageView<'_>) -> Result<(), WalError> {
        let mut reverse = self.0.seq;

        loop {
            let page = view.page(Self::seq_to_ptr(reverse));
            if let Some(inner) = page.check() {
                *self.0 = *inner;
                break;
            } else {
                reverse = reverse.wrapping_sub(1);
            };
        }

        drop(view);
        file.set_pages(self.0.size)?;

        Ok(())
    }

    fn fill_cache(&mut self, file: &FileIo) -> Result<(), WalError> {
        let mut freelist = self.0.freelist;
        let mut alloc = || {
            if let Some(head) = freelist {
                freelist = file.read().page(head).next;
                Ok(head)
            } else {
                file.grow(1).map(|p| p.expect("grow must yield value"))
            }
        };

        log::info!(
            "fill cache, will allocate {} pages",
            self.0.cache.capacity()
        );
        while !self.0.cache.is_full() {
            self.0.cache.put(alloc()?);
        }

        self.0.freelist = freelist;
        self.0.size = file.pages();

        self.write(file)?;

        Ok(())
    }

    fn collect_garbage(&mut self, file: &FileIo) -> Result<(), WalError> {
        log::info!("collect garbage, will free {} pages", self.0.garbage.len());

        let mut freelist = self.0.freelist;
        for ptr in &mut self.0.garbage {
            let page = FreePage {
                next: freelist,
                _data: [0; FreePage::PAD],
            };
            file.write_range(ptr, &page, 0..mem::size_of::<Option<PagePtr<FreePage>>>())?;
            freelist = Some(ptr);
        }
        self.0.freelist = freelist;

        self.write(file)
    }

    pub fn new_head(
        mut self,
        file: &FileIo,
        head: PagePtr<NodePage>,
        garbage: FreelistCache,
    ) -> Result<(), WalError> {
        self.0.head = head;
        self.0.garbage = garbage;
        self.write(file)?;
        self.collect_garbage(file)?;
        self.fill_cache(file)?;

        Ok(())
    }

    pub fn current_head(&self) -> PagePtr<NodePage> {
        self.0.head
    }

    pub fn cache_mut(&mut self) -> &mut FreelistCache {
        &mut self.0.cache
    }

    fn freelist_size(&self, file: &FileIo) -> u32 {
        let mut x = 0;
        let mut freelist = self.0.freelist;

        let view = file.read();

        while freelist.is_some() {
            x += 1;
            freelist = view.page(freelist).next;
        }
        x
    }
}

#[repr(C)]
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

    fn check(&self) -> Option<&RecordSeq> {
        // return None if checksum is wrong
        (self.checksum == crc64::crc64(0, self.inner.as_bytes())).then_some(&self.inner)
    }
}

#[repr(C)]
#[derive(Clone, Copy)]
struct RecordSeq {
    seq: u64,
    garbage: FreelistCache,
    cache: FreelistCache,
    size: u32,
    freelist: Option<PagePtr<FreePage>>,
    head: PagePtr<NodePage>,
}

#[derive(Clone, Copy)]
pub struct FreelistCache {
    pos: u32,
    pages: [Option<PagePtr<FreePage>>; CACHE_SIZE],
}

const CACHE_SIZE: usize = 0x18f;

impl Alloc for FreelistCache {
    fn alloc<T>(&mut self) -> PagePtr<T> {
        self.next()
            .expect("BUG: must be big enough, increase size of freelist cache")
            .cast()
    }
}

impl Free for FreelistCache {
    fn free<T>(&mut self, ptr: PagePtr<T>) {
        if self.put(ptr.cast()).is_some() {
            panic!("BUG: must have enough space, increase size of freelist cache");
        }
    }
}

impl FreelistCache {
    pub const SIZE: u32 = CACHE_SIZE as u32;

    pub const fn empty() -> Self {
        FreelistCache {
            pos: 0,
            pages: [None; CACHE_SIZE],
        }
    }

    pub fn is_full(&self) -> bool {
        self.capacity() == 0
    }

    pub fn capacity(&self) -> u32 {
        Self::SIZE - self.pos
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn len(&self) -> u32 {
        self.pos
    }

    pub fn put(&mut self, ptr: PagePtr<FreePage>) -> Option<PagePtr<FreePage>> {
        if self.is_full() {
            return Some(ptr);
        }
        self.pages[self.pos as usize] = Some(ptr);
        self.pos += 1;

        None
    }
}

impl Iterator for FreelistCache {
    type Item = PagePtr<FreePage>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.is_empty() {
            None
        } else {
            self.pos -= 1;
            self.pages[self.pos as usize]
        }
    }
}

unsafe impl PlainData for RecordPage {}

unsafe impl PlainData for RecordSeq {}

pub struct FreePage {
    next: Option<PagePtr<FreePage>>,
    _data: [u8; Self::PAD],
}

impl FreePage {
    const PAD: usize = PAGE_SIZE as usize - mem::size_of::<Option<PagePtr<FreePage>>>();
}

unsafe impl PlainData for FreePage {}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempdir::TempDir;

    use super::super::file::{FileIo, IoOptions};

    #[test]
    fn wal() {
        let env = env_logger::Env::new().filter_or("RUST_LOG", "info");
        env_logger::try_init_from_env(env).unwrap_or_default();

        let cfg = IoOptions::default();
        let dir = TempDir::new("rej").unwrap();
        let path = dir.path().join("test-basic");

        let file = FileIo::new(&path, true, cfg).unwrap();
        // let wal = Wal::new(true, &file).unwrap();
        // let ptr = wal.lock().alloc::<()>(&file).unwrap();
        // assert_eq!(ptr.raw_number(), 1 + WAL_SIZE);
        // drop(wal);

        // let wal = Wal::new(false, &file).unwrap();
        // let ptr = wal.lock().alloc::<()>(&file).unwrap();
        // assert_eq!(ptr.raw_number(), 1 + WAL_SIZE);
        // drop(wal);

        drop(file);
        fs::copy(path, "target/db").unwrap();
    }
}
