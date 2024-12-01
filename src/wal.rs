use std::{cmp::Ordering, io, num::NonZeroU64, path::Path};

use thiserror::Error;

use super::{
    file::{FileIo, IoOptions, PlainData, PageView},
    page::{PagePtr, RawPtr},
    seq::{Seq, SeqLock, WAL_SIZE},
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
    #[error("bad write-ahead log")]
    BadWal,
}

pub struct Db {
    file: FileIo,
    seq: Seq,
}

impl Db {
    pub fn new(path: impl AsRef<Path>, cfg: IoOptions) -> Result<Self, DbError> {
        let create = !path.as_ref().exists();
        let file = FileIo::new(path, create, cfg)?;
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
                .ok_or(DbError::BadWal)?;

            page.seq
        };

        Ok(Db {
            file,
            seq: Seq::new(seq),
        })
    }

    pub fn unroll(&self) -> io::Result<()> {
        log::info!("unroll log");

        let mut seq_lock = self.seq.lock();
        let view = self.file.read();

        loop {
            let page = view.page(seq_lock.ptr());
            let Some(inner) = page.check() else {
                seq_lock.prev();
                continue;
            };
            match inner.body {
                Record::Done => break,
                Record::Allocate { old_head } => {
                    // TODO: revert allocation somehow
                    let _ = old_head;
                }
                Record::Free { old_head } => {
                    // TODO: decide what to do
                    let _ = old_head;
                }
            }
            seq_lock.prev();
        }

        Ok(())
    }

    pub fn read(&self) -> DbView<'_> {
        DbView(self.file.read())
    }

    fn write_log(
        &self,
        mut seq_lock: SeqLock<'_>,
        body: Record,
        freelist: Option<PagePtr<FreePage>>,
    ) -> io::Result<()> {
        seq_lock.next();
        let seq = seq_lock.seq();
        let page = RecordPage::new(RecordSeq {
            seq,
            freelist,
            body,
        });
        self.file.write(seq_lock.ptr(), &page)?;
        log::info!("freelist: {freelist:?}, action: {body:?}");

        Ok(())
    }

    pub fn alloc<T>(&self) -> io::Result<PagePtr<T>> {
        let seq_lock = self.seq.lock();

        let view = self.file.read();
        let (old_head, next) = if let Some(head) = view.page(seq_lock.ptr()).inner.freelist {
            let next = view.page(Some(head)).next;
            drop(view);
            (head, next)
        } else {
            drop(view);
            let head = self.file.grow().map(Option::unwrap)?;
            (head, None)
        };

        self.write_log(seq_lock, Record::Allocate { old_head }, next)?;
        Ok(old_head.cast())
    }

    pub fn free<T>(&self, ptr: PagePtr<T>) -> io::Result<()> {
        let seq_lock = self.seq.lock();

        let view = self.file.read();
        let old_head = view.page(seq_lock.ptr()).inner.freelist;

        // write current head into the page to free
        let ptr = ptr.cast::<FreePage>();
        self.file.write(Some(ptr), &FreePage { next: old_head })?;

        self.write_log(seq_lock, Record::Free { old_head }, Some(ptr))?;

        Ok(())
    }
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct RecordPage {
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
    Free { old_head: Option<PagePtr<FreePage>> },
}

unsafe impl PlainData for RecordPage {}

unsafe impl PlainData for RecordSeq {}

struct FreePage {
    next: Option<PagePtr<FreePage>>,
}

unsafe impl PlainData for FreePage {}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempdir::TempDir;

    use super::{IoOptions, Db, RawPtr, WAL_SIZE};

    #[test]
    fn allocate() {
        let env = env_logger::Env::new()
            .filter_or("RUST_LOG", "info")
            .write_style("MY_LOG_STYLE");
        env_logger::try_init_from_env(env).unwrap_or_default();
        let cfg = IoOptions::default();
        let dir = TempDir::new("rej").unwrap();
        let path = dir.path().join("test-basic");

        let db = Db::new(&path, cfg).unwrap();
        let ptr = db.alloc::<()>().unwrap();
        assert_eq!(ptr.raw_number(), WAL_SIZE);
        // db.free(ptr).unwrap();

        drop(db);
        let db = Db::new(&path, cfg).unwrap();
        db.unroll().unwrap();
        drop(db);

        fs::copy(path, "target/db").unwrap();
    }
}
