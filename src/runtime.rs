use std::{collections::BTreeMap, io, mem, slice};

use crate::page::{RawPtr, PAGE_SIZE};

use super::page::PagePtr;

/// # Safety
/// `Self` must:
/// - obey `repr(C)`
/// - be bitwise copy
/// - has size less or equal `PAGE_SIZE`.
/// - be free of padding
pub unsafe trait PlainData
where
    Self: Sized,
{
    const NAME: &str;

    fn as_this(slice: &[u8]) -> &Self {
        unsafe { &*slice.as_ptr().cast::<Self>() }
    }

    fn as_bytes(&self) -> &[u8] {
        let raw_ptr = (self as *const Self).cast();
        unsafe { slice::from_raw_parts(raw_ptr, mem::size_of::<Self>()) }
    }

    fn as_bytes_mut(&mut self) -> &mut [u8] {
        let raw_ptr = (self as *mut Self).cast();
        unsafe { slice::from_raw_parts_mut(raw_ptr, mem::size_of::<Self>()) }
    }

    fn as_this_mut(slice: &mut [u8]) -> &mut Self {
        unsafe { &mut *slice.as_mut_ptr().cast::<Self>() }
    }
}

pub trait Alloc {
    fn alloc<T>(&mut self) -> PagePtr<T>
    where
        T: PlainData;
}

pub trait Free {
    fn free<T>(&mut self, ptr: PagePtr<T>)
    where
        T: PlainData;
}

pub trait AbstractViewer {
    fn page<T>(&self, ptr: impl Into<Option<PagePtr<T>>>) -> &T
    where
        T: PlainData;
}

pub trait AbstractIo {
    type Viewer<'a>: AbstractViewer
    where
        Self: 'a;

    fn read(&self) -> Self::Viewer<'_>;

    fn write<T>(&self, ptr: impl Into<Option<PagePtr<T>>>, page: &T) -> io::Result<()>
    where
        T: PlainData;
}

#[derive(Clone, Copy)]
pub struct GenericPage([u8; PAGE_SIZE as usize]);

unsafe impl PlainData for GenericPage {
    const NAME: &str = "generic page";

    fn as_bytes(&self) -> &[u8] {
        self.0.as_ref()
    }
}

pub struct Rt<'a, A, F, Io> {
    pub alloc: &'a mut A,
    free: &'a mut F,
    pub io: &'a Io,
    storage: &'a mut BTreeMap<u32, Box<GenericPage>>,
}

impl<A, F, Io> Rt<'_, A, F, Io> {
    pub fn reborrow(&mut self) -> Rt<'_, A, F, Io> {
        Rt {
            alloc: &mut *self.alloc,
            free: &mut *self.free,
            io: self.io,
            storage: &mut *self.storage,
        }
    }
}

impl<'a, A, F, Io> Rt<'a, A, F, Io>
where
    A: Alloc,
    F: Free,
{
    pub fn new(
        alloc: &'a mut A,
        free: &'a mut F,
        io: &'a Io,
        storage: &'a mut BTreeMap<u32, Box<GenericPage>>,
    ) -> Self {
        Rt {
            alloc,
            free,
            io,
            storage,
        }
    }

    pub fn realloc<T>(&mut self, ptr: &mut PagePtr<T>)
    where
        T: PlainData,
    {
        self.free.free(mem::replace(ptr, self.alloc.alloc()));
    }
}

impl<A, F, Io> Rt<'_, A, F, Io>
where
    A: Alloc,
    F: Free,
    Io: AbstractIo,
{
    pub fn create<T>(&mut self) -> PagePtr<T>
    where
        T: PlainData,
    {
        let v = Box::new(GenericPage([0; PAGE_SIZE as usize]));
        let ptr = self.alloc.alloc();
        self.storage.insert(ptr.raw_number(), v);

        ptr
    }

    pub fn read<T>(&mut self, view: &Io::Viewer<'_>, ptr: &mut PagePtr<T>)
    where
        T: PlainData,
    {
        let v = Box::new(*view.page(ptr.cast::<GenericPage>()));
        self.free.free(mem::replace(ptr, self.alloc.alloc::<T>()));
        self.storage.insert(ptr.raw_number(), v);
    }

    pub fn mutate<T>(&mut self, ptr: PagePtr<T>) -> &mut T
    where
        T: PlainData,
    {
        let bytes = self
            .storage
            .get_mut(&ptr.raw_number())
            .expect("read or create before mutate")
            .as_bytes_mut();
        T::as_this_mut(bytes)
    }

    pub fn look<T>(&self, ptr: PagePtr<T>) -> &T
    where
        T: PlainData,
    {
        let bytes = self
            .storage
            .get(&ptr.raw_number())
            .expect("read or create before mutate")
            .as_bytes();
        T::as_this(bytes)
    }

    pub fn flush(self) -> io::Result<()> {
        for (n, page) in self.storage {
            self.io.write(PagePtr::from_raw_number(*n), page.as_ref())?;
        }

        Ok(())
    }
}
