use std::{borrow::Cow, io, mem};

use super::{
    page::{PagePtr, RawPtr},
    runtime::{PlainData, Alloc, Free, AbstractIo, AbstractViewer, Rt},
};

pub const M: usize = 0x100;
const K: usize = M / 2;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct NodePage {
    // if the node is root or branch, the pointer is `Self`,
    // but if the node is leaf, the pointer is a metadata page
    pub child: [Option<PagePtr<Self>>; M],
    // length in bytes of each key
    keys_len: [u16; M],
    // table id is a prefix of the key
    table_id: [u32; M],
    // pointers to additional pages that stores keys
    // maximal key size is `0x40 * 0x10 = 1 kiB`
    key: [Option<PagePtr<KeyPage>>; 0x40],
    // for fast iterating, only relevant for leaf nodes
    pub next: Option<PagePtr<Self>>,
    pub prev: Option<PagePtr<Self>>,
    // if stem is true than the node is root or branch
    // otherwise it is a leaf
    stem: u16,
    // number of children
    len: u16,
}

pub enum Child<T> {
    Node(PagePtr<NodePage>),
    Leaf(PagePtr<T>),
}

unsafe impl PlainData for NodePage {
    const NAME: &str = "Node";
}

#[repr(C)]
#[derive(Clone, Copy)]
struct KeyPage {
    keys: [[u8; 0x10]; M],
}

unsafe impl PlainData for KeyPage {
    const NAME: &str = "Key";
}

pub struct Key<'a> {
    pub table_id: u32,
    pub bytes: Cow<'a, [u8]>,
}

impl NodePage {
    pub const fn empty() -> Self {
        NodePage {
            child: [None; M],
            keys_len: [0; M],
            table_id: [0; M],
            key: [None; 64],
            next: None,
            prev: None,
            stem: 1,
            len: 0,
        }
    }

    pub fn append_child(&mut self, ptr: PagePtr<NodePage>) {
        self.child[self.len()] = Some(ptr);
        self.len += 1;
    }

    pub fn len(&self) -> usize {
        self.len as usize
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn is_leaf(&self) -> bool {
        self.stem == 0
    }

    pub fn key_len(&self, idx: usize) -> usize {
        self.keys_len[idx] as usize
    }

    pub fn get_key<'c>(&self, view: &impl AbstractViewer, idx: usize) -> Key<'c> {
        let len = self.key_len(idx);
        let depth = len.div_ceil(0x10);
        // start with small allocation, optimistically assume the key is small
        let mut v = Vec::with_capacity(0x10 * 4);
        for i in &self.key[..depth] {
            let ptr = i.expect("BUG key length inconsistent with key pages");
            let page = view.page(ptr);
            v.extend_from_slice(&page.keys[idx]);
        }
        v.truncate(len);
        Key {
            table_id: self.table_id[idx],
            bytes: Cow::Owned(v),
        }
    }

    pub fn get_child<T>(&self, idx: usize) -> Option<Child<T>> {
        let ptr = self.child[idx];
        match self.is_leaf() {
            false => ptr.map(Child::Node),
            true => ptr.map(PagePtr::cast).map(Child::Leaf),
        }
    }

    // TODO: SIMD optimization
    pub fn search(&self, view: &impl AbstractViewer, key: &Key) -> Result<usize, usize> {
        let mut depth = 0;

        let len = self.len() - usize::from(!self.is_leaf());

        let i = self.table_id[..len].binary_search(&key.table_id)?;
        let mut range = i..(i + 1);

        while range.start > 0 && self.table_id[range.start - 1] == key.table_id {
            range.start -= 1;
        }

        while range.end < len - 1 && self.table_id[range.end] == key.table_id {
            range.end += 1;
        }

        let mut key = key.bytes.as_ref();

        while !key.is_empty() {
            let mut key_b = [0; 0x10];
            let l = key.len().min(0x10);
            key_b[..l].clone_from_slice(&key[..l]);
            key = &key[l..];

            let buffer = if let Some(ptr) = self.key[depth] {
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

            while range.end < len - 1 && buffer[range.end] == key_b {
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

    fn split(
        &mut self,
        rt: Rt<'_, impl Alloc, impl Free, impl AbstractIo>,
        this_ptr: PagePtr<NodePage>,
    ) -> io::Result<(PagePtr<Self>, Self)> {
        let new_ptr = rt.alloc.alloc();
        let mut new = NodePage::empty();
        new.stem = self.stem;
        new.len = K as u16;
        self.len = K as u16;

        new.next = mem::replace(&mut self.next, Some(new_ptr));
        new.prev = Some(this_ptr);

        new.child[..K].clone_from_slice(&self.child[K..]);
        self.child[K..].iter_mut().for_each(|x| *x = None);
        new.keys_len[..K].clone_from_slice(&self.keys_len[K..]);
        self.keys_len[K..].iter_mut().for_each(|x| *x = 0);
        new.table_id[..K].clone_from_slice(&self.table_id[K..]);
        self.table_id[K..].iter_mut().for_each(|x| *x = 0);

        let view = rt.io.read();
        for (i, key_page_ptr) in self.key.iter().enumerate() {
            if let Some(ptr) = key_page_ptr {
                let mut key_page = *view.page(*ptr);
                let new_right_ptr = rt.alloc.alloc();
                new.key[i] = Some(new_right_ptr);
                let mut new_page = KeyPage {
                    keys: [[0; 0x10]; M],
                };
                new_page.keys[..K].clone_from_slice(&key_page.keys[K..]);
                key_page.keys[K..].iter_mut().for_each(|x| *x = [0; 0x10]);
                rt.io.write(new_right_ptr, &new_page)?;

                rt.io.write(*ptr, &key_page)?;
            }
        }

        Ok((new_ptr, new))
    }

    pub fn insert<'c, T>(
        &mut self,
        mut rt: Rt<'_, impl Alloc, impl Free, impl AbstractIo>,
        this_ptr: PagePtr<NodePage>,
        new_child_ptr: PagePtr<NodePage>,
        idx: usize,
        key: &Key,
    ) -> io::Result<Option<(Key<'c>, PagePtr<NodePage>)>>
    where
        T: PlainData,
    {
        let old_len = self.len();
        self.len = (old_len + 1) as u16;

        for i in (idx..old_len).rev() {
            self.child[i + 1] = self.child[i];
            self.keys_len[i + 1] = self.keys_len[i];
            self.table_id[i + 1] = self.table_id[i];
        }

        self.child[idx] = Some(new_child_ptr);
        if !self.is_leaf() {
            self.child.swap(idx, idx + 1);
        }
        self.keys_len[idx] = key.bytes.len() as u16;
        self.table_id[idx] = key.table_id;
        self.insert_key(rt.reborrow(), idx, old_len, &key.bytes)?;

        if self.len() == M {
            let (new_ptr, new) = self.split(rt.reborrow(), this_ptr)?;
            rt.io.write(new_ptr, &new)?;
            let key = self.get_key(&rt.io.read(), K - 1);
            rt.io.write(this_ptr, &*self)?;

            Ok(Some((key, new_ptr)))
        } else {
            rt.io.write(this_ptr, &*self)?;
            Ok(None)
        }
    }

    // TODO: optimize
    fn insert_key(
        &mut self,
        rt: Rt<'_, impl Alloc, impl Free, impl AbstractIo>,
        idx: usize,
        old_len: usize,
        key: &[u8],
    ) -> io::Result<()> {
        let view = rt.io.read();
        for ptr in self.key.iter_mut().flatten() {
            let page = *view.page(*ptr);
            rt.free.free(*ptr);
            let new_ptr = rt.alloc.alloc();
            *ptr = new_ptr;
            rt.io.write(new_ptr, &page)?;
        }

        let mut it = key.chunks(0x10);
        let mut depth = 0;
        loop {
            let was_absent = self.key[depth].is_none();
            let chunk = it.next();
            if was_absent && chunk.is_none() {
                break;
            }
            let ptr = *self.key[depth].get_or_insert_with(|| rt.alloc.alloc());
            let mut page = *view.page(ptr);
            if was_absent {
                page.keys = [[0; 0x10]; M];
            } else {
                for i in (idx..old_len).rev() {
                    page.keys[i + 1] = page.keys[i];
                }
            }
            page.keys[idx] = [0; 0x10];
            if let Some(chunk) = chunk {
                page.keys[idx][..chunk.len()].clone_from_slice(chunk);
            }
            rt.io.write(ptr, &page)?;

            depth += 1;
        }

        Ok(())
    }
}
