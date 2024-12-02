use std::{io, mem};

use super::{
    file::{PlainData, FileIo, PageView},
    page::{PagePtr, PAGE_SIZE},
};

const M: usize = 256;

pub fn get(
    view: &PageView<'_>,
    mut ptr: PagePtr<NodePage>,
    key: &[u8; 11],
) -> Option<PagePtr<DataPage>> {
    loop {
        let page = view.page(ptr);
        let idx = page.keys().binary_search(key).ok()?;
        let child = page.child[idx];
        if page.leaf {
            return unsafe { child.leaf };
        } else {
            ptr = unsafe { child.node? };
        }
    }
}

pub fn insert(
    file: &FileIo,
    old_head: PagePtr<NodePage>,
    stem_ptr: &mut &[PagePtr<NodePage>],
    key: &[u8; 11],
    data: PagePtr<DataPage>,
) -> io::Result<Option<PagePtr<DataPage>>> {
    let mut take = || {
        let (ptr, rest) = stem_ptr.split_first().unwrap();
        *stem_ptr = rest;
        *ptr
    };
    let view_lock = file.read();

    let old_ptr = old_head;

    // TODO: loop, balance
    let new_ptr = take();
    let mut node = *view_lock.page(old_ptr);
    let idx = node.keys().binary_search(key).unwrap_or_else(|idx| {
        let old = node.len;
        node.len = old + 1;
        for i in (idx..(old as usize)).rev() {
            node.keys[i + 1] = node.keys[i];
            node.child[i + 1] = node.child[i];
        }
        node.keys[idx] = *key;
        node.child[idx] = Child { leaf: None };
        idx
    });
    node.leaf = true;
    let old = unsafe { &mut node.child[idx].leaf }.replace(data);
    file.write(new_ptr, &node)?;

    Ok(old)
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct NodePage {
    child: [Child; M],
    keys: [[u8; 11]; M],
    next: Option<PagePtr<NodePage>>,
    prev: Option<PagePtr<NodePage>>,
    deep: Option<PagePtr<KeyPage>>,
    leaf: bool,
    len: u8,
}

unsafe impl PlainData for NodePage {}

impl NodePage {
    fn keys(&self) -> &[[u8; 11]] {
        &self.keys[..self.len as usize]
    }
}

#[repr(C)]
#[derive(Clone, Copy)]
struct KeyPage {
    keys: [[u8; 15]; M],
    deep: Option<PagePtr<KeyPage>>,
    len: u8,
}

unsafe impl PlainData for KeyPage {}

impl KeyPage {
    // TODO: extended key size
    #[allow(dead_code)]
    fn keys(&self) -> &[[u8; 15]] {
        &self.keys[..self.len as usize]
    }
}

#[repr(C)]
#[derive(Clone, Copy)]
union Child {
    node: Option<PagePtr<NodePage>>,
    leaf: Option<PagePtr<DataPage>>,
}

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

#[cfg(test)]
mod tests {}
