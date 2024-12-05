use std::io;

use super::{
    file::{FileIo, PageView},
    page::PagePtr,
    node::{Alloc, Free, NodePage, Child},
};

pub fn get<T>(
    view: &PageView<'_>,
    mut ptr: PagePtr<NodePage>,
    table_id: u32,
    key: &[u8],
) -> Option<PagePtr<T>> {
    loop {
        let node = view.page(ptr);
        let idx = node.search(view, table_id, key).ok()?;
        match node.get_child(idx)? {
            Child::Node(p) => ptr = p,
            Child::Leaf(p) => return Some(p),
        }
    }
}

pub struct It(Option<ItInner>);

pub struct ItInner {
    ptr: PagePtr<NodePage>,
    forward: bool,
    idx: u16,
}

impl ItInner {
    fn new(
        view: &PageView<'_>,
        head_ptr: PagePtr<NodePage>,
        forward: bool,
        table_id: u32,
        key: Option<&[u8]>,
    ) -> Option<Self> {
        let mut ptr = head_ptr;
        let mut node = view.page(ptr);
        if node.is_empty() {
            return None;
        }

        loop {
            let idx = key.map_or((1 - usize::from(forward)) * (node.len() - 1), |key| {
                node.search(view, table_id, key)
                    .unwrap_or_else(|idx| idx + usize::from(forward))
            });
            match node.get_child::<()>(idx)? {
                Child::Node(p) => ptr = p,
                Child::Leaf(_) => {
                    return Some(ItInner {
                        ptr,
                        forward,
                        idx: idx as u16,
                    })
                }
            }
            node = view.page(ptr);
        }
    }
}

impl It {
    pub fn new(
        view: &PageView<'_>,
        head_ptr: PagePtr<NodePage>,
        forward: bool,
        table_id: u32,
        key: Option<&[u8]>,
    ) -> Self {
        Self(ItInner::new(view, head_ptr, forward, table_id, key))
    }

    pub fn next<T>(&mut self, view: &PageView<'_>) -> Option<(Vec<u8>, PagePtr<T>)> {
        let inner = self.0.as_mut()?;

        let idx = usize::from(inner.idx);
        let page = view.page(inner.ptr);
        if idx < page.len() {
            if inner.forward {
                inner.idx += 1;
            } else if inner.idx != 0 {
                inner.idx -= 1;
            } else {
                if let Some(ptr) = page.prev {
                    inner.ptr = ptr;
                    inner.idx = u16::MAX;
                } else {
                    self.0 = None;
                }
            }
            match page.get_child(idx)? {
                Child::Leaf(p) => Some((page.get_key(view, idx), p)),
                _ => panic!("BUG: `ptr` should point on leaf node"),
            }
        } else {
            if !inner.forward {
                inner.idx = page.len() as u16 - 1;
            } else {
                if let Some(ptr) = page.next {
                    inner.ptr = ptr;
                    inner.idx = 0;
                } else {
                    self.0 = None;
                }
            }
            // Warning: recursion
            self.next(view)
        }
    }
}

pub fn insert<T>(
    file: &FileIo,
    old_head: PagePtr<NodePage>,
    fl_old: &mut impl Alloc,
    fl_new: &mut impl Free,
    table_id: u32,
    key: &[u8],
) -> io::Result<(PagePtr<NodePage>, PagePtr<T>)> {
    let view = file.read();

    let old_ptr = old_head;

    // TODO: loop, balance
    let new_ptr = fl_old.alloc();
    let mut node = *view.page(old_ptr);
    fl_new.free(old_ptr);
    let (idx, exact) = match node.search(&view, table_id, key) {
        Ok(idx) => (idx, true),
        Err(idx) => (idx, false),
    };
    if !exact {
        node.insert(file, fl_old, fl_new, idx, table_id, key)?;
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
