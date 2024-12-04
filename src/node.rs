use std::io;

use super::{
    file::{PlainData, FileIo, PageView},
    page::{PagePtr, RawPtr},
};

pub trait Alloc {
    fn alloc<T>(&mut self) -> PagePtr<T>;
}

pub trait Free {
    fn free<T>(&mut self, ptr: PagePtr<T>);
}

const M: usize = 0x200;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct NodePage {
    child: [Option<PagePtr<Self>>; M],
    keys_len: [u16; M],
    // maximal key size is `0x40 * 0x10 = 1024` bytes
    deep: [[Option<PagePtr<KeyPage>>; 2]; 0x40],
    pub next: Option<PagePtr<Self>>,
    pub prev: Option<PagePtr<Self>>,
    stem: bool,
    len: usize,
}

pub enum Child<T> {
    Node(PagePtr<NodePage>),
    Leaf(PagePtr<T>),
}

unsafe impl PlainData for NodePage {}

impl NodePage {
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn get_key(&self, view: &PageView<'_>, idx: usize) -> Vec<u8> {
        let len = self.keys_len[idx];
        let depth = ((len + 0x10 - 1) / 0x10) as usize;
        let mut v = Vec::with_capacity(0x400);
        for i in &self.deep[..depth] {
            if idx < M / 2 {
                let ptr = i[0].expect("BUG key length inconsistent with key pages");
                let page = view.page(ptr);
                v.extend_from_slice(&page.keys[idx]);
            } else {
                let idx = idx - M / 2;
                let ptr = i[1].expect("BUG key length inconsistent with key pages");
                let page = view.page(ptr);
                v.extend_from_slice(&page.keys[idx]);
            }
        }
        v
    }

    pub fn get_child<T>(&self, idx: usize) -> Option<Child<T>> {
        let ptr = self.child[idx];
        match self.stem {
            true => ptr.map(Child::Node),
            false => ptr.map(PagePtr::cast).map(Child::Leaf),
        }
    }

    pub fn get_child_or_insert_with<T, F>(&mut self, idx: usize, f: F) -> Child<T>
    where
        F: FnOnce() -> PagePtr<Self>,
    {
        let ptr = *self.child[idx].get_or_insert_with(f);
        match self.stem {
            true => Child::Node(ptr),
            false => Child::Leaf(ptr.cast()),
        }
    }

    // TODO: SIMD optimization
    pub fn search(&self, view: &PageView<'_>, mut key: &[u8]) -> Result<usize, usize> {
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

    pub fn insert(
        &mut self,
        file: &FileIo,
        fl_old: &mut impl Alloc,
        fl_new: &mut impl Free,
        idx: usize,
        key: &[u8],
    ) -> io::Result<()> {
        let view = file.read();

        let mut realloc_half = |half| {
            for couple in &mut self.deep {
                if let Some::<PagePtr<KeyPage>>(ptr) = &mut couple[half] {
                    fl_new.free(*ptr);
                    let page = *view.page(*ptr);
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

        let old = self.len;
        self.len = old + 1;

        for i in (idx..old).rev() {
            self.child[i + 1] = self.child[i];
            self.keys_len[i + 1] = self.keys_len[i];
        }
        self.child[idx] = None;
        self.keys_len[idx] = key.len() as u16;

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
