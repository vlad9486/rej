use std::io;

use super::{
    file::{FileIo, PageView},
    page::PagePtr,
    node::{Alloc, Free, NodePage, Child},
};

pub fn get<T>(view: &PageView<'_>, mut ptr: PagePtr<NodePage>, key: &[u8]) -> Option<PagePtr<T>> {
    loop {
        let node = view.page(ptr);
        let idx = node.search(view, key).ok()?;
        match node.get_child(idx)? {
            Child::Node(p) => ptr = p,
            Child::Leaf(p) => return Some(p),
        }
    }
}

pub fn insert<T>(
    file: &FileIo,
    old_head: PagePtr<NodePage>,
    fl_old: &mut impl Alloc,
    fl_new: &mut impl Free,
    key: &[u8],
) -> io::Result<(PagePtr<NodePage>, PagePtr<T>)> {
    let view = file.read();

    let old_ptr = old_head;

    // TODO: loop, balance
    let new_ptr = fl_old.alloc();
    let mut node = *view.page(old_ptr);
    fl_new.free(old_ptr);
    let (idx, exact) = match node.search(&view, key) {
        Ok(idx) => (idx, true),
        Err(idx) => (idx, false),
    };
    if !exact {
        node.insert(file, fl_old, fl_new, idx, key)?;
    }

    let child = node.get_child_or_insert_with(idx, || {
        log::debug!("use metadata page");
        fl_old.alloc()
    });
    file.write(new_ptr, &node)?;

    match child {
        Child::Node(_) => unimplemented!(),
        Child::Leaf(ptr) => Ok((new_ptr, ptr)),
    }
}

// TODO: remove value
pub fn remove<T>(
    file: &FileIo,
    old_head: PagePtr<NodePage>,
    fl_old: &mut impl Alloc,
    fl_new: &mut impl Free,
    key: &[u8],
) -> io::Result<(PagePtr<NodePage>, Option<PagePtr<T>>)> {
    let _ = (file, old_head, fl_old, fl_new, key);
    unimplemented!()
}
