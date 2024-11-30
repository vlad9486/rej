use std::{fmt, marker::PhantomData, num::NonZeroU32};

pub const PAGE_SIZE: u64 = 0x1000;

pub struct PagePtr<T>(NonZeroU32, PhantomData<T>);

pub trait RawPtr
where
    Self: Sized,
{
    type Casted<U>;

    fn from_raw_number(number: u32) -> Option<Self>;
    fn raw_number(&self) -> u32;
    fn cast<U>(self) -> Self::Casted<U>;
}

impl<T> RawPtr for PagePtr<T> {
    type Casted<U> = PagePtr<U>;

    fn from_raw_number(number: u32) -> Option<Self> {
        NonZeroU32::new(number).map(|n| Self(n, PhantomData))
    }

    fn raw_number(&self) -> u32 {
        self.0.get()
    }

    fn cast<U>(self) -> Self::Casted<U> {
        PagePtr(self.0, PhantomData)
    }
}

pub trait RawOffset
where
    Self: Sized,
{
    fn from_raw_offset(offset: u64) -> Option<Self>;
    fn raw_offset(self) -> u64;
}

impl<T> RawOffset for T
where
    T: RawPtr,
{
    fn from_raw_offset(offset: u64) -> Option<Self> {
        T::from_raw_number((offset / PAGE_SIZE) as u32)
    }

    fn raw_offset(self) -> u64 {
        self.raw_number() as u64 * PAGE_SIZE
    }
}

impl<T> fmt::Debug for PagePtr<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0.get())
    }
}

impl<T> Copy for PagePtr<T> {}

impl<T> Clone for PagePtr<T> {
    fn clone(&self) -> Self {
        *self
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
        Some(self.cmp(other))
    }
}

impl<T> Ord for PagePtr<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.cmp(&other.0)
    }
}
