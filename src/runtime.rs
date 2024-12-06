use std::{io, mem, slice};

use super::page::PagePtr;

/// # Safety
/// must obey `repr(C)`, must be bitwise copy and has size less or equal `PAGE_SIZE`
pub unsafe trait PlainData
where
    Self: Sized,
{
    const NAME: &str;

    fn as_this(slice: &[u8], offset: usize) -> &Self {
        unsafe { &*slice.as_ptr().add(offset).cast::<Self>() }
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

pub struct Rt<'a, A, F, Io> {
    pub alloc: &'a mut A,
    pub free: &'a mut F,
    pub io: &'a Io,
}

impl<A, F, Io> Rt<'_, A, F, Io> {
    pub fn reborrow(&mut self) -> Rt<'_, A, F, Io> {
        Rt {
            alloc: &mut *self.alloc,
            free: &mut *self.free,
            io: self.io,
        }
    }
}
