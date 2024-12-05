use std::{
    fs, io, mem,
    ops::Range,
    path::Path,
    slice,
    sync::{
        atomic::{AtomicU32, Ordering},
        RwLock, RwLockReadGuard,
    },
};

use memmap2::Mmap;
use fs4::fs_std::FileExt;

use super::{
    utils,
    page::{PagePtr, RawPtr, PAGE_SIZE},
};

/// # Safety
/// must obey `repr(C)`, must be bitwise copy and has size less or equal `PAGE_SIZE`
pub unsafe trait PlainData
where
    Self: Sized,
{
    fn as_this(slice: &[u8], offset: usize) -> &Self {
        unsafe { &*slice.as_ptr().add(offset).cast::<Self>() }
    }

    fn as_bytes(&self) -> &[u8] {
        let raw_ptr = (self as *const Self).cast();
        unsafe { slice::from_raw_parts(raw_ptr, mem::size_of::<Self>()) }
    }
}

pub struct PageView<'a>(RwLockReadGuard<'a, Mmap>);

impl PageView<'_> {
    pub fn page<T>(&self, ptr: impl Into<Option<PagePtr<T>>>) -> &T
    where
        T: PlainData,
    {
        let offset = (ptr.into().map_or(0, PagePtr::raw_number) as u64 * PAGE_SIZE) as usize;
        T::as_this(&self.0, offset)
    }
}

#[derive(Default, Clone, Copy)]
pub struct IoOptions {
    pub direct_write: bool,
    pub mmap_populate: bool,
    #[cfg(test)]
    pub simulator: Simulator,
}

#[cfg(test)]
#[derive(Clone, Copy)]
pub struct Simulator {
    pub crash_at: u32,
    pub mess_page: bool,
}

#[cfg(test)]
impl Default for Simulator {
    fn default() -> Self {
        Simulator {
            crash_at: u32::MAX,
            mess_page: false,
        }
    }
}

#[cfg(test)]
impl IoOptions {
    pub fn simulator(crash_at: u32, mess_page: bool) -> Self {
        IoOptions {
            simulator: Simulator {
                crash_at,
                mess_page,
            },
            ..Default::default()
        }
    }
}

pub struct FileIo {
    cfg: IoOptions,
    file: fs::File,
    file_len: AtomicU32,
    write_counter: AtomicU32,
    mapped: RwLock<Mmap>,
}

impl FileIo {
    pub fn new(path: impl AsRef<Path>, create: bool, cfg: IoOptions) -> io::Result<Self> {
        let file = utils::open_file(path, create, cfg.direct_write)?;
        file.lock_exclusive()?;

        let file_len = AtomicU32::new((file.metadata()?.len() / PAGE_SIZE) as u32);
        let mapped = RwLock::new(utils::mmap(&file, cfg.mmap_populate)?);

        Ok(FileIo {
            cfg,
            file,
            file_len,
            write_counter: AtomicU32::new(0),
            mapped,
        })
    }

    pub fn read(&self) -> PageView<'_> {
        PageView(self.mapped.read().expect("poisoned"))
    }

    pub fn write_range<T>(
        &self,
        ptr: impl Into<Option<PagePtr<T>>>,
        page: &T,
        range: Range<usize>,
    ) -> io::Result<()>
    where
        T: PlainData,
    {
        let offset =
            (ptr.into().map_or(0, PagePtr::raw_number) as u64) * PAGE_SIZE + range.start as u64;
        let slice = page
            .as_bytes()
            .get(range)
            .expect("`range` must be in the range");
        self.write_stats(offset);
        utils::write_at(&self.file, slice, offset)
    }

    pub fn write<T>(&self, ptr: impl Into<Option<PagePtr<T>>>, page: &T) -> io::Result<()>
    where
        T: PlainData,
    {
        let offset = (ptr.into().map_or(0, PagePtr::raw_number) as u64) * PAGE_SIZE;
        self.write_stats(offset);
        utils::write_at(&self.file, page.as_bytes(), offset)
    }

    fn write_stats(&self, offset: u64) {
        let old = self.write_counter.fetch_add(1, Ordering::SeqCst);
        #[cfg(test)]
        {
            use rand::RngCore;

            let simulator = self.cfg.simulator;
            if old == simulator.crash_at {
                if simulator.mess_page {
                    let mut data = [0; PAGE_SIZE as usize];
                    rand::thread_rng().fill_bytes(&mut data);
                    utils::write_at(&self.file, &data, offset).unwrap()
                }
                panic!("intentional panic for test");
            }
        }
        #[cfg(not(test))]
        let _ = (old, offset);
    }

    pub fn sync(&self) -> io::Result<()> {
        self.file.sync_all()
    }

    pub fn grow<T>(&self, n: u32) -> io::Result<Option<PagePtr<T>>> {
        let mut lock = self.mapped.write().expect("poisoned");

        let old_len = self.file_len.load(Ordering::SeqCst);

        self.file.set_len((old_len + n) as u64 * PAGE_SIZE)?;
        self.file_len.store(old_len + n, Ordering::SeqCst);
        *lock = utils::mmap(&self.file, self.cfg.mmap_populate)?;

        Ok(PagePtr::from_raw_number(old_len))
    }

    pub fn set_pages(&self, pages: u32) -> io::Result<()> {
        let mut lock = self.mapped.write().expect("poisoned");
        self.file.set_len((pages as u64) * PAGE_SIZE)?;
        self.file_len.store(pages, Ordering::SeqCst);
        *lock = utils::mmap(&self.file, self.cfg.mmap_populate)?;

        Ok(())
    }

    pub fn pages(&self) -> u32 {
        self.file_len.load(Ordering::SeqCst)
    }

    pub fn writes(&self) -> u32 {
        self.write_counter.load(Ordering::SeqCst)
    }
}
