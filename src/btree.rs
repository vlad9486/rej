use std::io;

use super::{
    file::{PlainData, FileIo, PageView},
    page::{PagePtr, RawPtr},
};

const M: usize = 0x200;

pub fn get<T>(view: &PageView<'_>, mut ptr: PagePtr<NodePage>, key: &[u8]) -> Option<PagePtr<T>> {
    loop {
        let page = view.page(ptr);
        let idx = page.search(view, key).ok()?;
        let child = page.child[idx];
        if page.leaf {
            return PagePtr::from_raw_number(child);
        } else {
            ptr = PagePtr::from_raw_number(child)?;
        }
    }
}

pub trait Alloc {
    fn alloc<T>(&mut self) -> PagePtr<T>;
}

pub trait Free {
    fn free<T>(&mut self, ptr: PagePtr<T>);
}

pub fn insert<T>(
    file: &FileIo,
    old_head: PagePtr<NodePage>,
    fl_old: &mut impl Alloc,
    fl_new: &mut impl Free,
    key: &[u8],
) -> io::Result<(PagePtr<NodePage>, PagePtr<T>)> {
    let view_lock = file.read();

    let old_ptr = old_head;

    // TODO: loop, balance
    let new_ptr = fl_old.alloc();
    let mut node = *view_lock.page(old_ptr);
    fl_new.free(old_ptr);
    let (idx, exact) = match node.search(&view_lock, key) {
        Ok(idx) => (idx, true),
        Err(idx) => (idx, false),
    };
    if !exact {
        let mut realloc_half = |half| {
            for couple in &mut node.deep {
                if let Some::<PagePtr<KeyPage>>(ptr) = &mut couple[half] {
                    fl_new.free(*ptr);
                    let page = *view_lock.page(*ptr);
                    let new_ptr = fl_old.alloc();
                    file.write(new_ptr, &page)?;
                    *ptr = new_ptr;
                }
            }

            Ok::<_, io::Error>(())
        };
        realloc_half(0)?;
        if idx >= M / 2 {
            realloc_half(1)?;
        }

        node.insert(file, fl_old, idx, key)?;
    }

    node.leaf = true;
    if node.child[idx] == 0 {
        log::debug!("use metadata page");
        node.child[idx] = fl_old.alloc::<T>().raw_number();
    }
    let ptr = unsafe { PagePtr::from_raw_number(node.child[idx]).unwrap_unchecked() };
    file.write(new_ptr, &node)?;

    Ok((new_ptr, ptr))
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

#[repr(C)]
#[derive(Clone, Copy)]
pub struct NodePage {
    child: [u32; M],
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
        fl_old: &mut impl Alloc,
        idx: usize,
        key: &[u8],
    ) -> io::Result<()> {
        let old = self.len;
        self.len = old + 1;

        for i in (idx..old).rev() {
            self.child[i + 1] = self.child[i];
        }
        self.child[idx] = 0;

        if self.len <= M / 2 {
            self.insert_half::<0>(file, fl_old, old, idx, key)?;
        } else if idx >= M / 2 {
            self.insert_half::<1>(file, fl_old, old - M / 2, idx - M / 2, key)?;
        } else {
            let carry = self.insert_half::<0>(file, fl_old, M / 2, idx, key)?;
            self.insert_half::<1>(file, fl_old, old - M / 2, 0, &carry)?;
        }

        Ok(())
    }

    fn insert_half<const HALF: usize>(
        &mut self,
        file: &FileIo,
        fl_old: &mut impl Alloc,
        end: usize,
        idx: usize,
        key: &[u8],
    ) -> io::Result<Vec<u8>> {
        let mut carry = vec![];
        let mut it = key.chunks(0x10);
        let mut depth = 0;
        let view = file.read();
        loop {
            let was_absent = self.deep[depth][HALF].is_none();
            let chunk = it.next();
            if was_absent && chunk.is_none() {
                break;
            }
            let ptr = *self.deep[depth][HALF].get_or_insert_with(|| fl_old.alloc());
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
