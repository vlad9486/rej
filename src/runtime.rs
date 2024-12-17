use std::{collections::BTreeMap, io, mem, ops::Deref, slice};

use super::page::{PagePtr, RawPtr, PAGE_SIZE};

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

    fn as_this_mut(slice: &mut [u8]) -> &mut Self {
        unsafe { &mut *slice.as_mut_ptr().cast::<Self>() }
    }
}

unsafe impl PlainData for [u8; PAGE_SIZE as usize] {
    const NAME: &str = "PlainData";
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
    type Page<'a, T>: Deref<Target = T>
    where
        Self: 'a,
        T: PlainData + 'a;

    fn page<'a, T>(&'a self, ptr: impl Into<Option<PagePtr<T>>>) -> Self::Page<'a, T>
    where
        T: PlainData + 'a;
}

pub trait AbstractIo {
    type Viewer<'a>: AbstractViewer
    where
        Self: 'a;

    fn read(&self) -> Self::Viewer<'_>;

    fn write<T>(&self, ptr: impl Into<Option<PagePtr<T>>>, page: &T) -> io::Result<()>
    where
        T: PlainData,
    {
        self.write_bytes(ptr.into().map(PagePtr::cast), page.as_bytes())
    }

    fn write_bytes(&self, ptr: impl Into<Option<PagePtr<()>>>, bytes: &[u8]) -> io::Result<()>;
}

pub struct Rt<'a, A, F, Io> {
    pub alloc: &'a mut A,
    pub free: &'a mut F,
    pub io: &'a Io,
    storage: &'a mut BTreeMap<u32, Vec<u8>>,
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
        storage: &'a mut BTreeMap<u32, Vec<u8>>,
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
        let ptr = self.alloc.alloc();
        self.storage
            .insert(ptr.raw_number(), vec![0; mem::size_of::<T>()]);

        ptr
    }

    pub fn read<T>(&mut self, view: &Io::Viewer<'_>, ptr: &mut PagePtr<T>)
    where
        T: PlainData,
    {
        let v = view.page::<[u8; PAGE_SIZE as usize]>(ptr.cast())[..mem::size_of::<T>()].to_vec();
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
            .expect("read or create before mutate");
        T::as_this_mut(&mut *bytes)
    }

    pub fn look<T>(&self, ptr: PagePtr<T>) -> &T
    where
        T: PlainData,
    {
        let bytes = self
            .storage
            .get(&ptr.raw_number())
            .expect("read or create before mutate");
        T::as_this(bytes)
    }

    pub fn flush(self) -> io::Result<()> {
        for (n, page) in self.storage {
            self.io
                .write_bytes(PagePtr::from_raw_number(*n), page.as_ref())?;
        }

        Ok(())
    }
}
