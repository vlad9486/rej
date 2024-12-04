use std::{io, mem};

use super::{
    file::{PlainData, FileIo, PageView},
    page::{PagePtr, PAGE_SIZE, RawPtr},
    wal::FreelistCache,
};

const M: usize = 0x200;

pub fn get(
    view: &PageView<'_>,
    mut ptr: PagePtr<NodePage>,
    key: &[u8],
) -> Option<PagePtr<DataPage>> {
    loop {
        let page = view.page(ptr);
        let idx = page.search(view, key).ok()?;
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
    fl_old: &mut FreelistCache,
    fl_new: &mut FreelistCache,
    key: &[u8],
) -> io::Result<(PagePtr<NodePage>, PagePtr<DataPage>)> {
    let view_lock = file.read();

    let old_ptr = old_head;

    let mut alloc = || fl_old.next().expect("must be big enough");
    let mut free = |ptr| {
        if fl_new.put(ptr).is_some() {
            panic!("must have enough space");
        }
    };

    // TODO: loop, balance
    let new_ptr = alloc().cast();
    let mut node = *view_lock.page(old_ptr);
    free(old_ptr.cast());
    for [left, right] in &mut node.deep {
        if let Some(ptr) = left {
            let page = *view_lock.page(*ptr);
            free(ptr.cast());
            let new_ptr = alloc().cast();
            file.write(new_ptr, &page)?;
            *ptr = new_ptr;
        }
        if let Some(ptr) = right {
            let page = *view_lock.page(*ptr);
            free(ptr.cast());
            let new_ptr = alloc().cast();
            file.write(new_ptr, &page)?;
            *ptr = new_ptr;
        }
    }
    let idx = node.search(&view_lock, key).unwrap_or_else(|idx| {
        node.insert(file, || alloc().cast(), idx, key).unwrap();
        idx
    });
    node.leaf = true;
    let ptr = *unsafe { &mut node.child[idx].leaf }.get_or_insert_with(|| {
        log::debug!("use metadata page");
        alloc().cast()
    });
    file.write(new_ptr, &node)?;

    Ok((new_ptr, ptr))
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct NodePage {
    child: [Child; M],
    keys_len: [u16; M],
    // maximal key size is `0x40 * 0x10 = 1024` bytes
    deep: [[Option<PagePtr<KeyPage>>; 2]; 0x40],
    next: Option<PagePtr<NodePage>>,
    prev: Option<PagePtr<NodePage>>,
    leaf: bool,
    len: usize,
}

unsafe impl PlainData for NodePage {}

impl NodePage {
    // TODO: SIMD optimization
    fn search(&self, view: &PageView<'_>, mut key: &[u8]) -> Result<usize, usize> {
        let mut buffer = [[0; 0x10]; M];
        let mut range = 0..self.len;
        let mut depth = 0;
        while !key.is_empty() {
            let mut key_b = [0; 0x10];
            let l = key.len().min(0x10);
            key_b[..l].clone_from_slice(&key[..l]);
            key = &key[l..];

            if let Some(ptr) = self.deep[depth][0] {
                buffer[..(M / 2)].clone_from_slice(&view.page(ptr).keys);
            } else {
                buffer[..(M / 2)].clone_from_slice(&[[0; 0x10]; M / 2]);
            }

            if let Some(ptr) = self.deep[depth][1] {
                buffer[(M / 2)..].clone_from_slice(&view.page(ptr).keys);
            } else {
                buffer[(M / 2)..].clone_from_slice(&[[0; 0x10]; M / 2]);
            }

            let i = buffer[range.clone()]
                .binary_search(&key_b)
                .map_err(|i| range.start + i)?;

            range = (range.start + i)..(range.start + i + 1);

            while range.start > 0 && buffer[range.start - 1] == key_b {
                range.start -= 1;
            }

            while range.end < self.len - 1 && buffer[range.end] == key_b {
                range.end += 1;
            }

            depth += 1;
        }

        if range.len() == 1 {
            Ok(range.start)
        } else {
            Err(range.start)
        }
    }

    fn insert(
        &mut self,
        file: &FileIo,
        mut alloc: impl FnMut() -> PagePtr<KeyPage>,
        idx: usize,
        key: &[u8],
    ) -> io::Result<()> {
        let old = self.len;
        self.len = old + 1;

        for i in (idx..old).rev() {
            self.child[i + 1] = self.child[i];
        }
        self.child[idx] = Child { leaf: None };

        if self.len <= M / 2 {
            self.insert_half(file, &mut alloc, old, idx, 0, key)?;
        } else if idx >= M / 2 {
            self.insert_half(file, &mut alloc, old - M / 2, idx - M / 2, 1, key)?;
        } else {
            let carry = self.insert_half(file, &mut alloc, M / 2, idx, 0, key)?;
            self.insert_half(file, &mut alloc, old - M / 2, 0, 1, &carry)?;
        }

        Ok(())
    }

    fn insert_half(
        &mut self,
        file: &FileIo,
        mut alloc: impl FnMut() -> PagePtr<KeyPage>,
        end: usize,
        idx: usize,
        half: usize,
        key: &[u8],
    ) -> io::Result<Vec<u8>> {
        let mut carry = vec![];
        let mut it = key.chunks(0x10);
        let mut depth = 0;
        let view = file.read();
        loop {
            let was_absent = self.deep[depth][half].is_none();
            let chunk = it.next();
            if was_absent && chunk.is_none() {
                break;
            }
            let ptr = *self.deep[depth][half].get_or_insert_with(&mut alloc);
            let mut page = *view.page(ptr);
            if was_absent {
                log::debug!("use key page");
                page.keys = [[0; 0x10]; M / 2];
            }
            for i in (idx..end).rev() {
                if i + 1 == M / 2 {
                    carry.extend_from_slice(&page.keys[i]);
                } else {
                    page.keys[i + 1] = page.keys[i];
                }
            }
            page.keys[idx] = [0; 0x10];
            if let Some(chunk) = chunk {
                page.keys[idx][..chunk.len()].clone_from_slice(chunk);
            }
            file.write(ptr, &page)?;

            depth += 1;
        }
        Ok(carry)
    }
}

#[repr(C)]
#[derive(Clone, Copy)]
struct KeyPage {
    keys: [[u8; 0x10]; M / 2],
}

unsafe impl PlainData for KeyPage {}

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
