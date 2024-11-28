use std::{
    fmt, fs, io,
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

use super::utils;

const PAGE_SIZE: u64 = 0x1000;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("{0}")]
    Io(#[from] io::Error),
    #[error("no root page")]
    NoRootPage,
    #[error("bad static type")]
    BadStaticType,
}

#[derive(Default, Clone, Copy)]
pub struct StorageConfig {
    pub direct_write: bool,
    pub mmap_populate: bool,
}

pub struct Storage<S> {
    cfg: StorageConfig,
    file: fs::File,
    mapped: RwLock<Mmap>,
    lock: Mutex<()>,
    phantom_data: PhantomData<S>,
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

pub struct StaticPageView<'a, P>(RwLockReadGuard<'a, Mmap>, PhantomData<P>);

impl<'a, P> Deref for StaticPageView<'a, P>
where
    P: Page,
{
    type Target = P;

    fn deref(&self) -> &Self::Target {
        &FreePage::<P>::as_this(&self.0, None).data
    }
}

impl<S> Storage<S>
where
    S: Page + Copy,
{
    pub fn open(
        path: impl AsRef<Path>,
        create: bool,
        cfg: StorageConfig,
    ) -> Result<Self, StorageError> {
        if mem::size_of::<FreePage<S>>() > PAGE_SIZE as usize {
            return Err(StorageError::BadStaticType);
        }

        let file = utils::open_file(path, create, cfg.direct_write)?;
        file.lock_exclusive()?;
        if create {
            file.set_len(PAGE_SIZE)?;
        }
        let mapped = RwLock::new(utils::mmap(&file, cfg.mmap_populate)?);

        Ok(Storage {
            cfg,
            file,
            mapped,
            lock: Mutex::new(()),
            phantom_data: PhantomData,
        })
    }

    fn read_head(&self) -> Option<PagePtr<FreePage<S>>> {
        let lock = self.mapped.read();
        let b = &lock[0..PTR_SIZE];
        let raw_ptr = u32::from_le_bytes(b.try_into().expect("cannot fail"));
        NonZeroU32::new(raw_ptr).map(|p| PagePtr(p, PhantomData))
    }

    fn write_head(&self, head: Option<PagePtr<FreePage<S>>>) -> Result<(), StorageError> {
        let head = head.as_ref().map_or(0, |p| p.0.get());
        utils::write_at(&self.file, &head.to_le_bytes(), 0)?;

        Ok(())
    }

    pub fn read_static(&self) -> StaticPageView<'_, S> {
        StaticPageView(self.mapped.read(), PhantomData)
    }

    pub fn modify_static<F, T>(&self, mut f: F) -> Result<T, StorageError>
    where
        F: FnMut(&mut S) -> T,
    {
        let lock = self.lock.lock();
        let mut data = FreePage::<S>::as_this(&*self.mapped.read(), None).data;
        let result = f(&mut data);
        let offset = memoffset::offset_of!(FreePage::<S>, data);
        utils::write_at(&self.file, data.as_bytes(), offset as u64)?;
        drop(lock);

        Ok(result)
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
        let offset = ptr.offset() + range.start as u64;
        utils::write_at(&self.file, &page.as_bytes()[range], offset)?;

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
        let old_len = self.file.metadata()?.len();
        self.file.set_len(old_len + PAGE_SIZE)?;
        *self.mapped.write() = utils::mmap(&self.file, self.cfg.mmap_populate)?;

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
        let lock = self.lock.lock();
        if let Some(result) = self.read_head() {
            let head = self.read(result).next;
            self.write_head(head)?;

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
        let ptr = ptr.cast::<FreePage<S>>();
        let mut free_page = *self.read(ptr);
        let lock = self.lock.lock();
        free_page.next = self.read_head();
        self.write_range(ptr, &free_page, 0..PTR_SIZE)?;
        self.write_head(Some(ptr))?;
        drop(lock);

        Ok(())
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
struct FreePage<S> {
    next: Option<PagePtr<FreePage<S>>>,
    data: S,
}

unsafe impl<S> Page for FreePage<S> where S: Sized {}
