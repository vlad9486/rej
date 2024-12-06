use std::mem;

use super::{page::PAGE_SIZE, runtime::PlainData};

#[repr(C)]
#[derive(Clone, Copy)]
pub struct DataPage {
    pub len: usize,
    pub data: [u8; Self::CAPACITY],
}

impl DataPage {
    pub const CAPACITY: usize = PAGE_SIZE as usize - mem::size_of::<usize>();
}

unsafe impl PlainData for DataPage {}
