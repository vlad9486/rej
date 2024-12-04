use std::{cmp::Ordering, io, mem, num::NonZeroU64};

use parking_lot::{Mutex, MutexGuard};
use thiserror::Error;

use super::{
    file::{FileIo, PlainData},
    page::{PagePtr, RawPtr, PAGE_SIZE},
    btree::NodePage,
};

#[derive(Debug, Error)]
pub enum WalError {
    #[error("{0}")]
    Io(#[from] io::Error),
    #[error("bad write-ahead log")]
    BadWal,
}

pub const WAL_SIZE: u32 = 0x100;

pub struct Wal(Mutex<RecordSeq>);

impl Wal {
    pub fn new(create: bool, file: &FileIo) -> Result<Self, WalError> {
        let inner = if create {
            let head = PagePtr::from_raw_number(WAL_SIZE)
                .ok_or(io::Error::from(io::ErrorKind::UnexpectedEof))?;
            for pos in 0..WAL_SIZE {
                let inner = RecordSeq {
                    seq: pos.into(),
                    freelist_cache: FreelistCache::empty(),
                    freelist: None,
                    head,
                };
                let page = RecordPage::new(inner, Record::Done);
                let ptr = file.grow(1)?;

                file.write(ptr, &page)?;
            }
            let head = file.grow(1)?.expect("must yield some");

            let ptr = file
                .grow::<FreePage>(FreelistCache::SIZE.into())?
                .expect("must yield some")
                .raw_number();
            let mut pages = [None; FreelistCache::SIZE as usize];
            for (n, page) in pages.iter_mut().enumerate() {
                *page = PagePtr::from_raw_number(ptr + n as u32);
            }

            file.sync()?;

            Mutex::new(RecordSeq {
                seq: (WAL_SIZE - 1).into(),
                freelist_cache: FreelistCache {
                    pos: FreelistCache::SIZE,
                    pages,
                },
                freelist: None,
                head,
            })
        } else {
            let view = file.read();

            let inner = *(0..WAL_SIZE)
                .map(PagePtr::from_raw_number)
                .map(|ptr| view.page::<RecordPage>(ptr))
                .filter_map(RecordPage::check)
                .map(|(inner, _)| inner)
                .max()
                .ok_or(WalError::BadWal)?;

            Mutex::new(inner)
        };

        Ok(Wal(inner))
    }

    pub fn lock(&self) -> WalLock<'_> {
        WalLock(self.0.lock())
    }
}

pub struct WalLock<'a>(MutexGuard<'a, RecordSeq>);

impl WalLock<'_> {
    pub fn seq(&self) -> u64 {
        self.0.seq
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

    fn write(&mut self, file: &FileIo, body: Record) -> Result<(), WalError> {
        self.next();
        let page = RecordPage::new(*self.0, body);
        file.write(self.ptr(), &page)?;
        file.sync()?;
        log::debug!("action: {body:?}");

        Ok(())
    }

    pub fn unroll(mut self, file: &FileIo) -> Result<(), WalError> {
        let view = file.read();

        let mut reverse = self.0.seq;

        loop {
            let page = view.page(Self::seq_to_ptr(reverse));
            let Some((inner, body)) = page.check() else {
                reverse = reverse.wrapping_sub(1);
                continue;
            };
            match *body {
                Record::Done => break,
                Record::Allocate { old_head } => {
                    self.next();
                    self.0.freelist = Some(old_head);
                    self.write(file, Record::RevertedAlloc)?;
                }
                Record::Free { old_data } => {
                    let old_head = view.page(inner.freelist).next;
                    file.write_range(
                        inner.freelist,
                        &FreePage {
                            next: old_data,
                            _data: [0; FreePage::PAD],
                        },
                        0..mem::size_of::<Option<PagePtr<FreePage>>>(),
                    )?;
                    self.next();
                    self.0.freelist = old_head;
                    self.write(file, Record::RevertedFree)?;
                }
                Record::RevertedAlloc => {}
                Record::RevertedFree => {}
            }
            reverse = reverse.wrapping_sub(1);
        }

        Ok(())
    }

    pub fn alloc<T>(&mut self, file: &FileIo) -> Result<PagePtr<T>, WalError> {
        let view = file.read();

        let (old_head, next) = if let Some(head) = view.page(self.ptr()).inner.freelist {
            let next = view.page(head).next;
            drop(view);
            (head, next)
        } else {
            drop(view);
            let head = file.grow(1)?.expect("grow must yield value");
            (head, None)
        };

        self.0.freelist = next;
        self.write(file, Record::Allocate { old_head })?;

        Ok(old_head.cast())
    }

    pub fn free<T>(&mut self, file: &FileIo, ptr: PagePtr<T>) -> Result<(), WalError> {
        let view = file.read();
        let old_head = view.page(self.ptr()).inner.freelist;

        // write current head into the page to free
        let ptr = ptr.cast::<FreePage>();
        // store in log
        let old_data = view.page(ptr).next;
        self.0.freelist = Some(ptr);
        self.write(file, Record::Free { old_data })?;
        file.write_range(
            ptr,
            &FreePage {
                next: old_head,
                _data: [0; FreePage::PAD],
            },
            0..mem::size_of::<Option<PagePtr<FreePage>>>(),
        )?;

        Ok(())
    }

    pub fn new_head(
        mut self,
        file: &FileIo,
        head: PagePtr<NodePage>,
        freelist_cache: FreelistCache,
    ) -> Result<(), WalError> {
        self.0.head = head;
        self.0.freelist_cache = freelist_cache;
        self.write(file, Record::Done)
    }

    pub fn current_head(&self) -> PagePtr<NodePage> {
        self.0.head
    }

    pub fn freelist_size(&self, file: &FileIo) -> u32 {
        let mut x = 0;
        let mut freelist = self.0.freelist;

        let view = file.read();

        while freelist.is_some() {
            x += 1;
            freelist = view.page(freelist).next;
        }
        x
    }

    pub fn freelist_cache(&mut self) -> FreelistCache {
        self.0.freelist_cache
    }
}

#[repr(C)]
#[derive(Clone, Copy)]
struct RecordPage {
    checksum: Option<NonZeroU64>,
    inner: RecordSeq,
    body: Record,
}

impl RecordPage {
    fn checksum<T>(value: &T) -> NonZeroU64
    where
        T: PlainData,
    {
        let checksum = crc64::crc64(0, value.as_bytes()).saturating_add(1);
        unsafe { NonZeroU64::new_unchecked(checksum) }
    }

    fn new(inner: RecordSeq, body: Record) -> Self {
        let checksum = Some(Self::checksum(&inner));
        RecordPage {
            checksum,
            inner,
            body,
        }
    }

    fn check(&self) -> Option<(&RecordSeq, &Record)> {
        // return None if no checksum or checksum is wrong
        (self.checksum? == Self::checksum(&self.inner)).then_some((&self.inner, &self.body))
    }
}

#[repr(C)]
#[derive(Clone, Copy)]
struct RecordSeq {
    seq: u64,
    freelist_cache: FreelistCache,
    freelist: Option<PagePtr<FreePage>>,
    head: PagePtr<NodePage>,
}

impl PartialEq for RecordSeq {
    fn eq(&self, other: &Self) -> bool {
        self.seq.eq(&other.seq)
    }
}

impl Eq for RecordSeq {}

impl PartialOrd for RecordSeq {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for RecordSeq {
    fn cmp(&self, other: &Self) -> Ordering {
        self.seq.cmp(&other.seq)
    }
}

#[derive(Clone, Copy)]
pub struct FreelistCache {
    pos: u32,
    pages: [Option<PagePtr<FreePage>>; Self::SIZE as usize],
}

impl FreelistCache {
    pub const SIZE: u32 = 0x2b7;

    pub const fn empty() -> Self {
        FreelistCache {
            pos: 0,
            pages: [None; Self::SIZE as usize],
        }
    }

    pub fn is_full(&self) -> bool {
        self.capacity() == 0
    }

    pub fn capacity(&self) -> u32 {
        Self::SIZE - self.pos
    }

    pub fn put(&mut self, page: PagePtr<FreePage>) -> Option<PagePtr<FreePage>> {
        if self.is_full() {
            return Some(page);
        }
        self.pages[self.pos as usize] = Some(page.cast());
        self.pos += 1;

        None
    }
}

impl Iterator for FreelistCache {
    type Item = PagePtr<FreePage>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos == 0 {
            None
        } else {
            self.pos -= 1;
            self.pages[self.pos as usize].map(PagePtr::cast)
        }
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
enum Record {
    Done,
    Allocate { old_head: PagePtr<FreePage> },
    Free { old_data: Option<PagePtr<FreePage>> },
    RevertedAlloc,
    RevertedFree,
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

    use super::{
        RawPtr, Wal,
        super::file::{FileIo, IoOptions},
    };

    #[test]
    fn allocate() {
        let env = env_logger::Env::new().filter_or("RUST_LOG", "info");
        env_logger::try_init_from_env(env).unwrap_or_default();

        let cfg = IoOptions::default();
        let dir = TempDir::new("rej").unwrap();
        let path = dir.path().join("test-basic");

        let file = FileIo::new(&path, true, cfg).unwrap();
        let wal = Wal::new(true, &file).unwrap();
        let ptr = wal.lock().alloc::<()>(&file).unwrap();
        assert_eq!(ptr.raw_number(), 0x101);
        wal.lock().free(&file, ptr).unwrap();
        drop(wal);

        let wal = Wal::new(false, &file).unwrap();
        let ptr = wal.lock().alloc::<()>(&file).unwrap();
        assert_eq!(ptr.raw_number(), 0x101);
        drop(wal);

        fs::copy(path, "target/db").unwrap();
    }
}
