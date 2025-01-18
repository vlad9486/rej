use std::{collections::BTreeMap, io, mem, slice};

use aligned_vec::{ABox, ConstAlign};

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

    fn as_this_mut(slice: &mut [u8]) -> &mut Self {
        unsafe { &mut *slice.as_mut_ptr().cast::<Self>() }
    }

    fn as_bytes(&self) -> &[u8] {
        let raw_ptr = (self as *const Self).cast();
        unsafe { slice::from_raw_parts(raw_ptr, mem::size_of::<Self>()) }
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

pub trait AbstractIo {
    fn read_page(&self, n: u32) -> io::Result<PBox>;

    fn read<T>(&self, ptr: impl Into<Option<PagePtr<T>>>) -> T
    where
        T: PlainData + Copy,
    {
        // TODO: unwrap
        let page = self
            .read_page(ptr.into().map_or(0, PagePtr::raw_number))
            .unwrap();
        *T::as_this(&*page)
    }

    fn write<T>(
        &self,
        ptr: impl Into<Option<PagePtr<T>>>,
        kind: PageKind,
        value: T,
    ) -> io::Result<()>
    where
        T: PlainData,
    {
        let mut page = PBox::new(4096, [0; PAGE_SIZE as usize]);
        let bytes = value.as_bytes();
        page[..bytes.len()].clone_from_slice(bytes);

        self.write_page(ptr.into().map_or(0, PagePtr::raw_number), kind, page)
    }

    fn write_page(&self, n: u32, kind: PageKind, page: PBox) -> io::Result<()>;
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub enum PageKind {
    Log,
    Tree,
    Data,
    Clear,
}

pub type PBox = ABox<[u8; PAGE_SIZE as usize], ConstAlign<{ PAGE_SIZE as usize }>>;

pub struct Rt<'a, A, F, Io> {
    pub alloc: &'a mut A,
    pub free: &'a mut F,
    pub io: &'a Io,
    storage: &'a mut BTreeMap<u32, PBox>,
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
        storage: &'a mut BTreeMap<u32, PBox>,
    ) -> Self {
        Rt {
            alloc,
            free,
            io,
            storage,
        }
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
        let v = PBox::new(4096, [0; PAGE_SIZE as usize]);
        self.storage.insert(ptr.raw_number(), v);

        ptr
    }

    pub fn read<T>(&mut self, ptr: &mut PagePtr<T>)
    where
        T: PlainData,
    {
        // TODO: unwrap
        let page = self.io.read_page(ptr.raw_number()).unwrap();
        self.free.free(mem::replace(ptr, self.alloc.alloc::<T>()));
        self.storage.insert(ptr.raw_number(), page);
    }

    pub fn set<T>(&mut self, ptr: &mut PagePtr<T>, v: T)
    where
        T: PlainData,
    {
        self.free.free(mem::replace(ptr, self.alloc.alloc::<T>()));
        let mut page = PBox::new(4096, [0; PAGE_SIZE as usize]);
        page[..v.as_bytes().len()].clone_from_slice(v.as_bytes());
        self.storage.insert(ptr.raw_number(), page);
    }

    pub fn mutate<T>(&mut self, ptr: PagePtr<T>) -> &mut T
    where
        T: PlainData,
    {
        let bytes = self
            .storage
            .get_mut(&ptr.raw_number())
            .expect("read or create before mutate");
        T::as_this_mut(&mut **bytes)
    }

    pub fn look<T>(&self, ptr: PagePtr<T>) -> &T
    where
        T: PlainData,
    {
        let bytes = self
            .storage
            .get(&ptr.raw_number())
            .expect("read or create before mutate");
        T::as_this(&**bytes)
    }

    pub fn flush(self) -> io::Result<()> {
        for (n, page) in mem::take(self.storage) {
            self.io.write_page(n, PageKind::Tree, page)?;
        }

        Ok(())
    }
}
