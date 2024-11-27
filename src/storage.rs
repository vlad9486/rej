use std::{
    fmt, fs,
    io::{self, Seek as _, Write as _},
    marker::PhantomData,
    mem,
    num::NonZeroU32,
    ops::{Deref, Range},
    path::Path,
    slice,
};

use fs4::fs_std::FileExt;
use memmap2::Mmap;
use parking_lot::{Mutex, RwLock, RwLockReadGuard};
use thiserror::Error;

const PAGE_SIZE: u64 = 0x1000;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("{0}")]
    Io(#[from] io::Error),
    #[error("no root page")]
    NoRootPage,
    #[error("bad list index {0}")]
    BadListIndex(usize),
}

#[derive(Default, Clone, Copy)]
pub struct StorageConfig {
    pub direct_write: bool,
    pub mmap_populate: bool,
}

pub struct Storage {
    cfg: StorageConfig,
    file: Mutex<fs::File>,
    mapped: RwLock<Mmap>,
    freelist_lock: Mutex<()>,
}

pub struct PagePtr<T>(NonZeroU32, PhantomData<T>);

impl<T> fmt::Debug for PagePtr<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0.get())
    }
}

const PTR_SIZE: usize = mem::size_of::<NonZeroU32>();

impl<T> Copy for PagePtr<T> {}

impl<T> Clone for PagePtr<T> {
    fn clone(&self) -> Self {
        Self(self.0, self.1)
    }
}

impl<T> PartialEq for PagePtr<T> {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl<T> Eq for PagePtr<T> {}

impl<T> PartialOrd for PagePtr<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.0.partial_cmp(&other.0)
    }
}

impl<T> Ord for PagePtr<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.cmp(&other.0)
    }
}

impl<T> PagePtr<T> {
    fn offset(&self) -> u64 {
        self.0.get() as u64 * PAGE_SIZE
    }

    fn cast<U>(self) -> PagePtr<U> {
        PagePtr(self.0, PhantomData)
    }
}

pub struct PageView<'a, P>(RwLockReadGuard<'a, Mmap>, PagePtr<P>);

impl<'a, P> Deref for PageView<'a, P>
where
    P: Page,
{
    type Target = P;

    fn deref(&self) -> &Self::Target {
        P::as_this(&self.0, Some(self.1))
    }
}

impl Storage {
    pub fn open(
        path: impl AsRef<Path>,
        create: bool,
        cfg: StorageConfig,
    ) -> Result<Self, StorageError> {
        let file = utils::open_file(path, create, cfg.direct_write)?;
        file.lock_exclusive()?;
        if create {
            file.set_len(PAGE_SIZE)?;
        }
        let mapped = RwLock::new(utils::mmap(&file, cfg.mmap_populate)?);
        let file = Mutex::new(file);

        Ok(Storage {
            cfg,
            file,
            mapped,
            freelist_lock: Mutex::new(()),
        })
    }

    fn read_head(&self, index: usize) -> Option<PagePtr<FreePage>> {
        let lock = self.mapped.read();
        let b = &lock[(index * PTR_SIZE)..((index + 1) * PTR_SIZE)];
        let raw_ptr = u32::from_le_bytes(b.try_into().expect("cannot fail"));
        NonZeroU32::new(raw_ptr).map(|p| PagePtr(p, PhantomData))
    }

    fn write_head(
        &self,
        index: usize,
        head: Option<PagePtr<FreePage>>,
    ) -> Result<(), StorageError> {
        let mut lock = self.file.lock();
        lock.seek(io::SeekFrom::Start((index * PTR_SIZE) as u64))?;
        let head = head.as_ref().map_or(0, |p| p.0.get());
        lock.write_all(&head.to_le_bytes())?;

        Ok(())
    }

    pub fn read<T>(&self, ptr: PagePtr<T>) -> PageView<'_, T>
    where
        T: Page,
    {
        PageView(self.mapped.read(), ptr)
    }

    pub fn write_range<T>(
        &self,
        ptr: PagePtr<T>,
        page: &T,
        range: Range<usize>,
    ) -> Result<(), StorageError>
    where
        T: Page,
    {
        let mut lock = self.file.lock();
        lock.seek(io::SeekFrom::Start(ptr.offset() + range.start as u64))?;
        lock.write_all(&page.as_bytes()[range])?;

        Ok(())
    }

    pub fn write<T>(&self, ptr: PagePtr<T>, page: &T) -> Result<(), StorageError>
    where
        T: Page,
    {
        self.write_range(ptr, page, 0..mem::size_of::<T>())
    }

    fn grow<T>(&self) -> Result<PagePtr<T>, StorageError>
    where
        T: Page,
    {
        let lock = self.file.lock();
        let old_len = lock.metadata()?.len();
        lock.set_len(old_len + PAGE_SIZE)?;
        *self.mapped.write() = utils::mmap(&lock, self.cfg.mmap_populate)?;
        drop(lock);

        let Some(non_zero) = NonZeroU32::new((old_len / PAGE_SIZE) as u32) else {
            return Err(StorageError::NoRootPage);
        };
        Ok(PagePtr(non_zero, PhantomData))
    }

    /// Allocate a new page, could contain any garbage data
    pub fn allocate<T>(&self) -> Result<PagePtr<T>, StorageError>
    where
        T: Page,
    {
        let lock = self.freelist_lock.lock();
        if let Some(result) = self.read_head(0) {
            let head = self.read(result).next;
            self.write_head(0, head)?;

            Ok(result.cast())
        } else {
            drop(lock);
            self.grow()
        }
    }

    /// Free the page
    pub fn free<T>(&self, ptr: PagePtr<T>) -> Result<(), StorageError>
    where
        T: Page,
    {
        let ptr = ptr.cast::<FreePage>();
        let mut free_page = *self.read(ptr);
        let lock = self.freelist_lock.lock();
        free_page.next = self.read_head(0);
        self.write_range(ptr, &free_page, 0..PTR_SIZE)?;
        self.write_head(0, Some(ptr))?;
        drop(lock);

        Ok(())
    }

    /// `index` must not be zero, `index` must be smaller `PAGE_SIZE as usize / PTR_SIZE`
    pub fn head<T>(&self, index: usize) -> Result<PagePtr<T>, StorageError> {
        if index == 0 || index >= PAGE_SIZE as usize / PTR_SIZE {
            return Err(StorageError::BadListIndex(index));
        }
        if let Some(ptr) = self.read_head(index) {
            Ok(ptr.cast())
        } else {
            let ptr = self.allocate()?;
            self.write_head(index, Some(ptr))?;

            Ok(ptr.cast())
        }
    }
}

/// # Safety
/// must obey `repr(C)` and has size less or equal `PAGE_SIZE`
pub unsafe trait Page
where
    Self: Sized,
{
    fn as_this(slice: &[u8], ptr: Option<PagePtr<Self>>) -> &Self {
        let offset = ptr.as_ref().map_or(0, PagePtr::offset);
        unsafe { &*slice.as_ptr().add(offset as usize).cast::<Self>() }
    }

    fn as_bytes(&self) -> &[u8] {
        let raw_ptr = (self as *const Self).cast();
        unsafe { slice::from_raw_parts(raw_ptr, mem::size_of::<Self>()) }
    }
}

#[repr(C)]
#[derive(Clone, Copy)]
struct FreePage {
    next: Option<PagePtr<FreePage>>,
    data: [u8; PAGE_SIZE as usize - PTR_SIZE],
}

unsafe impl Page for FreePage {}

mod utils {
    use std::{fs, io, path::Path};

    use memmap2::Mmap;

    #[cfg(unix)]
    pub fn open_file(
        path: impl AsRef<Path>,
        create: bool,
        direct_write: bool,
    ) -> io::Result<fs::File> {
        use std::os::unix::fs::OpenOptionsExt;

        #[cfg(any(target_os = "linux", target_os = "android"))]
        const O_DIRECT: libc::c_int = libc::O_DIRECT;

        #[cfg(not(any(target_os = "linux", target_os = "android")))]
        const O_DIRECT: libc::c_int = 0;

        let mut open_options = fs::OpenOptions::new();
        open_options.write(true).read(true);
        if create {
            open_options.create_new(true);
        }
        if direct_write {
            open_options.custom_flags(O_DIRECT);
        }
        Ok(open_options.open(path)?)
    }

    #[cfg(windows)]
    pub fn open_file(path: impl AsRef<Path>, create: bool, direct_write: bool) -> io::Result<File> {
        let mut open_options = OpenOptions::new();
        open_options.write(true).read(true);
        if create {
            open_options.create_new(true);
        }
        Ok(open_options.open(path)?)
    }

    #[cfg(unix)]
    pub fn mmap(file: &fs::File, populate: bool) -> io::Result<Mmap> {
        use memmap2::MmapOptions;

        let mut options = MmapOptions::new();
        if populate {
            options.populate();
        }
        let mmap = unsafe { options.map(file)? };
        // On Unix we advice the OS that page access will be random.
        mmap.advise(memmap2::Advice::Random)?;
        Ok(mmap)
    }

    // On Windows there is no advice to give.
    #[cfg(windows)]
    pub fn mmap(file: &File, populate: bool) -> io::Result<Mmap> {
        let mmap = unsafe { Mmap::map(file)? };
        Ok(mmap)
    }
}

#[cfg(test)]
mod tests {
    use tempdir::TempDir;

    use super::{Storage, StorageConfig, Page};

    #[repr(C)]
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub struct P {
        data: [u64; 512],
    }

    unsafe impl Page for P {}

    #[test]
    fn basic() {
        let cfg = StorageConfig::default();
        let dir = TempDir::new("rej").unwrap();
        let path = dir.path().join("test-basic");

        let st = Storage::open(&path, true, cfg).unwrap();
        let ptr = st.allocate::<P>().unwrap();
        let mut page = *st.read(ptr);
        page.data[0] = 0xdeadbeef_abcdef00;
        page.data[1] = 0x1234567890;
        st.write_range(ptr, &page, 0..16).unwrap();
        drop(st);

        let st = Storage::open(&path, false, cfg).unwrap();
        let retrieved = st.read(ptr);
        assert_eq!(*retrieved, page);
    }

    #[test]
    fn allocation() {
        let cfg = StorageConfig::default();
        let dir = TempDir::new("rej").unwrap();
        let path = dir.path().join("test-allocation");

        let st = Storage::open(&path, true, cfg).unwrap();
        let a = st.allocate::<P>().unwrap();
        let b = st.allocate::<P>().unwrap();
        let c = st.allocate::<P>().unwrap();
        let d = st.allocate::<P>().unwrap();

        st.free(b).unwrap();
        st.free(d).unwrap();

        let e = st.allocate::<P>().unwrap();
        let f = st.allocate::<P>().unwrap();

        assert!((e == b && f == d) || (e == d && f == b));

        st.free(a).unwrap();
        st.free(c).unwrap();
        st.free(e).unwrap();
        st.free(f).unwrap();
    }
}
