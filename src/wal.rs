use std::{cmp::Ordering, io, mem, num::NonZeroU64};

use parking_lot::{Mutex, MutexGuard};
use thiserror::Error;

use super::{
    file::{FileIo, PlainData},
    page::{PagePtr, RawPtr, PAGE_SIZE},
};

#[derive(Debug, Error)]
pub enum WalError {
    #[error("{0}")]
    Io(#[from] io::Error),
    #[error("bad write-ahead log")]
    BadWal,
}

pub const WAL_SIZE: u32 = 0x100;

pub struct Wal(Mutex<u64>);

impl Wal {
    pub fn new(create: bool, file: &FileIo) -> Result<Self, WalError> {
        let seq = if create {
            log::info!("initialize empty database");
            for pos in 0..WAL_SIZE {
                let inner = RecordSeq {
                    seq: pos.into(),
                    freelist: None,
                    body: Record::Done,
                };
                let page = RecordPage::new(inner);
                let ptr = file.grow()?;

                file.write(ptr, &page)?;
            }
            (WAL_SIZE - 1).into()
        } else {
            let view = file.read();

            let page = (0..WAL_SIZE)
                .map(PagePtr::from_raw_number)
                .map(|ptr| view.page::<RecordPage>(ptr))
                .filter_map(RecordPage::check)
                .max()
                .ok_or(WalError::BadWal)?;

            page.seq
        };

        Ok(Self(Mutex::new(seq)))
    }

    pub fn lock(&self) -> WalLock<'_> {
        WalLock(self.0.lock())
    }
}

pub struct WalLock<'a>(MutexGuard<'a, u64>);

impl WalLock<'_> {
    fn seq(&self) -> u64 {
        *self.0
    }

    fn ptr(&self) -> Option<PagePtr<RecordPage>> {
        Self::seq_to_ptr(self.seq())
    }

    fn seq_to_ptr(seq: u64) -> Option<PagePtr<RecordPage>> {
        let pos = (seq % u64::from(WAL_SIZE)) as u32;
        PagePtr::<RecordPage>::from_raw_number(pos)
    }

    fn next(&mut self) {
        *self.0 = self.0.wrapping_add(1);
    }

    fn write(
        &mut self,
        file: &FileIo,
        body: Record,
        freelist: Option<PagePtr<FreePage>>,
    ) -> Result<(), WalError> {
        self.next();
        let seq = self.seq();
        let page = RecordPage::new(RecordSeq {
            seq,
            freelist,
            body,
        });
        file.write(self.ptr(), &page)?;
        log::info!("freelist: {freelist:?}, action: {body:?}");

        Ok(())
    }

    pub fn unroll(mut self, file: &FileIo) -> Result<(), WalError> {
        log::info!("unroll log");

        let view = file.read();

        let mut reverse = self.seq();

        loop {
            let page = view.page(Self::seq_to_ptr(reverse));
            let Some(inner) = page.check() else {
                reverse = reverse.wrapping_sub(1);
                continue;
            };
            match inner.body {
                Record::Done => break,
                Record::Allocate { old_head } => {
                    self.next();
                    self.write(file, Record::RevertedAlloc, Some(old_head))?;
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
                    self.write(file, Record::RevertedFree, old_head)?;
                }
                Record::RevertedAlloc => {}
                Record::RevertedFree => {}
            }
            reverse = reverse.wrapping_sub(1);
        }
        log::info!("unrolled");

        Ok(())
    }

    pub fn alloc<T>(mut self, file: &FileIo) -> Result<PagePtr<T>, WalError> {
        let view = file.read();

        let (old_head, next) = if let Some(head) = view.page(self.ptr()).inner.freelist {
            let next = view.page(Some(head)).next;
            drop(view);
            (head, next)
        } else {
            drop(view);
            let head = file.grow().map(Option::unwrap)?;
            (head, None)
        };

        self.write(&file, Record::Allocate { old_head }, next)?;

        Ok(old_head.cast())
    }

    pub fn free<T>(mut self, file: &FileIo, ptr: PagePtr<T>) -> Result<(), WalError> {
        let view = file.read();
        let old_head = view.page(self.ptr()).inner.freelist;

        // write current head into the page to free
        let ptr = ptr.cast::<FreePage>();
        // store in log
        let old_data = view.page(Some(ptr)).next;
        self.write(file, Record::Free { old_data }, Some(ptr))?;
        file.write_range(
            Some(ptr),
            &FreePage {
                next: old_head,
                _data: [0; FreePage::PAD],
            },
            0..mem::size_of::<Option<PagePtr<FreePage>>>(),
        )?;

        Ok(())
    }
}

#[repr(C)]
#[derive(Clone, Copy)]
struct RecordPage {
    checksum: Option<NonZeroU64>,
    inner: RecordSeq,
}

impl RecordPage {
    fn checksum<T>(value: &T) -> NonZeroU64
    where
        T: PlainData,
    {
        let checksum = crc64::crc64(0, value.as_bytes()).saturating_add(1);
        unsafe { NonZeroU64::new_unchecked(checksum) }
    }

    fn new(inner: RecordSeq) -> Self {
        let checksum = Some(Self::checksum(&inner));
        RecordPage { inner, checksum }
    }

    fn check(&self) -> Option<&RecordSeq> {
        // return None if no checksum or checksum is wrong
        (self.checksum? == Self::checksum(&self.inner)).then_some(&self.inner)
    }
}

#[repr(C)]
#[derive(Clone, Copy)]
struct RecordSeq {
    seq: u64,
    freelist: Option<PagePtr<FreePage>>,
    body: Record,
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

struct FreePage {
    next: Option<PagePtr<FreePage>>,
    _data: [u8; Self::PAD],
}

impl FreePage {
    const PAD: usize = PAGE_SIZE as usize - mem::size_of::<Option<PagePtr<FreePage>>>();
}

unsafe impl PlainData for FreePage {}
