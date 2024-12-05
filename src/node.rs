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

const M: usize = 0x100;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct NodePage {
    child: [Option<PagePtr<Self>>; M],
    keys_len: [u16; M],
    table_id: [u32; M],
    // maximal key size is `0x40 * 0x10 = 1 kiB` bytes
    deep: [Option<PagePtr<KeyPage>>; 0x40],
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

    pub fn key_len(&self, idx: usize) -> usize {
        // let l = self.keys_len[idx / 2];
        // (l[idx % 2] as usize) << 8 | ((l[3] >> ((idx % 2) * 4)) as usize & 0xf)
        self.keys_len[idx] as usize
    }

    pub fn get_key(&self, view: &PageView<'_>, idx: usize) -> Vec<u8> {
        let len = self.key_len(idx);
        let depth = (len + 0x10 - 1) / 0x10;
        let mut v = Vec::with_capacity(0x400);
        for i in &self.deep[..depth] {
            let ptr = i.expect("BUG key length inconsistent with key pages");
            let page = view.page(ptr);
            v.extend_from_slice(&page.keys[idx]);
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
    pub fn search(
        &self,
        view: &PageView<'_>,
        table_id: u32,
        mut key: &[u8],
    ) -> Result<usize, usize> {
        let mut depth = 0;

        let i = self.table_id[..self.len].binary_search(&table_id)?;
        let mut range = i..(i + 1);

        while range.start > 0 && self.table_id[range.start - 1] == table_id {
            range.start -= 1;
        }

        while range.end < self.len - 1 && self.table_id[range.end] == table_id {
            range.end += 1;
        }

        while !key.is_empty() {
            let mut key_b = [0; 0x10];
            let l = key.len().min(0x10);
            key_b[..l].clone_from_slice(&key[..l]);
            key = &key[l..];

            let buffer = if let Some(ptr) = self.deep[depth] {
                &view.page(ptr).keys
            } else {
                break;
            };

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
        table_id: u32,
        key: &[u8],
    ) -> io::Result<()> {
        let view = file.read();

        for couple in &mut self.deep {
            if let Some(ptr) = couple {
                fl_new.free(*ptr);
                let page = *view.page(*ptr);
                let new_ptr = fl_old.alloc();
                file.write(new_ptr, &page)?;
                *ptr = new_ptr;
            }
        }

        let old = self.len;
        if old == M {
            panic!("BUG: handle overflow");
        }
        self.len = old + 1;

        for i in (idx..old).rev() {
            self.child[i + 1] = self.child[i];
            self.keys_len[i + 1] = self.keys_len[i];
            self.table_id[i + 1] = self.table_id[i];
        }
        self.child[idx] = None;
        self.keys_len[idx] = key.len() as u16;
        self.table_id[idx] = table_id;

        let mut it = key.chunks(0x10);
        let mut depth = 0;
        let view = file.read();
        loop {
            let was_absent = self.deep[depth].is_none();
            let chunk = it.next();
            if was_absent && chunk.is_none() {
                break;
            }
            let ptr = *self.deep[depth].get_or_insert_with(|| fl_old.alloc());
            let mut page = *view.page(ptr);
            if was_absent {
                log::debug!("use key page");
                page.keys = [[0; 0x10]; M];
            } else {
                for i in (idx..old).rev() {
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
        Ok(())
    }
}

#[repr(C)]
#[derive(Clone, Copy)]
struct KeyPage {
    keys: [[u8; 0x10]; M],
}

unsafe impl PlainData for KeyPage {}
