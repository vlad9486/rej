use std::mem;

use super::{
    page::PagePtr,
    runtime::{PlainData, Alloc, Free, AbstractIo, Rt},
    file::FileIo,
    wal::FreelistCache,
};

pub type R<'a> = Rt<'a, FreelistCache, FreelistCache, FileIo>;

pub trait Node
where
    Self: Sized,
{
    const M: usize;

    fn empty() -> Self;

    fn append_child(&mut self, ptr: PagePtr<Self>);

    fn child(&self, idx: usize) -> &Option<PagePtr<Self>>;

    fn child_mut(&mut self, idx: usize) -> &mut Option<PagePtr<Self>>;

    fn len(&self) -> usize;

    fn can_donate(&self) -> bool {
        self.len() > Self::M / 2
    }

    fn is_leaf(&self) -> bool;

    fn read_key(&self, file: &FileIo, idx: usize) -> Vec<u8>;

    fn get_key(&self, rt: R<'_>, idx: usize) -> Vec<u8>;

    fn search(&self, file: &FileIo, key: &[u8]) -> Result<usize, usize>;

    fn realloc_keys(&mut self, rt: R<'_>);

    fn insert(
        &mut self,
        rt: R<'_>,
        ptr: Option<PagePtr<Self>>,
        idx: usize,
        key: &[u8],
        rev: bool,
    ) -> Option<(Vec<u8>, PagePtr<Self>)>;

    fn remove(&mut self, rt: R<'_>, idx: usize, rev: bool) -> (Option<PagePtr<Self>>, Vec<u8>);

    fn set_key(&mut self, rt: R<'_>, idx: usize, key: &[u8]) -> Vec<u8>;

    fn merge(&mut self, other: &Self, rt: R<'_>, key: &[u8], old: bool) -> Vec<u8>;

    fn free(&self, rt: R<'_>);
}

#[repr(C, align(0x1000))]
#[derive(Clone, Copy)]
pub struct NodeCPage {
    child: [Option<PagePtr<Self>>; Self::M],
    keys: [[u8; 0x10]; Self::M],
    stem: u16,
    len: u16,
}

unsafe impl PlainData for NodeCPage {
    const NAME: &str = "NodeCPage";
}

impl Node for NodeCPage {
    #[cfg(feature = "small")]
    const M: usize = 0x8;

    #[cfg(not(feature = "small"))]
    const M: usize = 0xc0;

    fn empty() -> Self {
        NodeCPage {
            child: [None; Self::M],
            keys: [[0; 0x10]; Self::M],
            stem: 1,
            len: 0,
        }
    }

    fn append_child(&mut self, ptr: PagePtr<Self>) {
        self.child[self.len()] = Some(ptr);
        self.len += 1;
    }

    fn child(&self, idx: usize) -> &Option<PagePtr<Self>> {
        &self.child[idx]
    }

    fn child_mut(&mut self, idx: usize) -> &mut Option<PagePtr<Self>> {
        &mut self.child[idx]
    }

    fn len(&self) -> usize {
        self.len as usize
    }

    fn is_leaf(&self) -> bool {
        self.stem == 0
    }

    fn read_key(&self, _file: &FileIo, idx: usize) -> Vec<u8> {
        self.keys[idx].to_vec()
    }

    fn get_key(&self, _rt: R<'_>, idx: usize) -> Vec<u8> {
        self.keys[idx].to_vec()
    }

    fn search(&self, _file: &FileIo, key: &[u8]) -> Result<usize, usize> {
        let len = self.len() - usize::from(!self.is_leaf());
        self.keys[..len].binary_search(key.try_into().unwrap())
    }

    fn realloc_keys(&mut self, _rt: R<'_>) {}

    fn insert(
        &mut self,
        mut rt: R<'_>,
        new_child_ptr: Option<PagePtr<Self>>,
        idx: usize,
        key: &[u8],
        rev: bool,
    ) -> Option<(Vec<u8>, PagePtr<Self>)> {
        let old_len = self.len();
        self.len = (old_len + 1) as u16;

        for i in (idx..old_len).rev() {
            self.child[i + 1] = self.child[i];
            self.keys[i + 1] = self.keys[i];
        }

        self.child[idx] = new_child_ptr;
        self.keys[idx] = key.try_into().unwrap();
        if rev {
            self.child.swap(idx, idx + 1);
        }

        fn split(this: &mut NodeCPage, mut rt: R<'_>) -> PagePtr<NodeCPage> {
            const K: usize = NodeCPage::M / 2;

            let new_ptr = rt.create();
            let new = rt.mutate::<NodeCPage>(new_ptr);
            new.stem = this.stem;
            new.len = K as u16;
            this.len = K as u16;

            new.child[..K].clone_from_slice(&this.child[K..]);
            this.child[K..].iter_mut().for_each(|x| *x = None);
            new.keys[..K].clone_from_slice(&this.keys[K..]);
            this.keys[K..].iter_mut().for_each(|x| *x = [0; 0x10]);

            new_ptr
        }

        if self.len() == Self::M {
            let new_ptr = split(self, rt.reborrow());
            let key = self.get_key(rt.reborrow(), Self::M / 2 - 1);

            Some((key, new_ptr))
        } else {
            None
        }
    }

    fn remove(&mut self, _rt: R<'_>, idx: usize, rev: bool) -> (Option<PagePtr<Self>>, Vec<u8>) {
        let new_len = self.len() - 1;
        self.len = new_len as u16;

        let old_ptr = self.child[idx];
        let old_key = self.keys[idx];

        if rev {
            self.child.swap(idx, idx + 1);
        }

        for i in idx..new_len {
            self.child[i] = self.child[i + 1];
            self.keys[i] = self.keys[i + 1];
        }
        // just in case
        self.child[new_len] = None;

        (old_ptr, old_key.to_vec())
    }

    fn set_key(&mut self, _rt: R<'_>, idx: usize, key: &[u8]) -> Vec<u8> {
        mem::replace(&mut self.keys[idx], key.try_into().unwrap()).to_vec()
    }

    fn merge(&mut self, other: &Self, mut rt: R<'_>, key: &[u8], _old: bool) -> Vec<u8> {
        let new_len = self.len + other.len;
        if !self.is_leaf() {
            self.set_key(rt.reborrow(), self.len() - 1, key);
        }
        let to = (self.len as usize)..(new_len as usize);
        let from = 0..(other.len as usize);
        self.child[to.clone()].clone_from_slice(&other.child[from.clone()]);
        self.keys[to.clone()].clone_from_slice(&other.keys[from.clone()]);
        self.len = new_len;
        self.keys[(new_len as usize) - 1].to_vec()
    }

    fn free(&self, _rt: R<'_>) {}
}

#[repr(C, align(0x1000))]
#[derive(Clone, Copy)]
pub struct NodePage {
    // if the node is root or branch, the pointer is `Self`,
    // but if the node is leaf, the pointer is a metadata page
    child: [Option<PagePtr<Self>>; Self::M],
    // length in bytes of each key
    keys_len: [u16; Self::M],
    // pointers to additional pages that stores keys
    // maximal key size is `0x40 * 0x10 = 1 kiB`
    key: [Option<PagePtr<KeyPage>>; 0x40],
    // if stem is true than the node is root or branch
    // otherwise it is a leaf
    stem: u16,
    // number of children
    len: u16,
}

unsafe impl PlainData for NodePage {
    const NAME: &str = "Node";
}

#[repr(C, align(0x1000))]
#[derive(Clone, Copy)]
struct KeyPage {
    keys: [[u8; 0x10]; NodePage::M],
}

unsafe impl PlainData for KeyPage {
    const NAME: &str = "Key";
}

impl NodePage {
    fn keys_ptr(&self) -> impl Iterator<Item = PagePtr<KeyPage>> {
        self.key
            .into_iter()
            .take_while(Option::is_some)
            .map(Option::unwrap)
    }

    fn split(&mut self, mut rt: Rt<'_, impl Alloc, impl Free, impl AbstractIo>) -> PagePtr<Self> {
        const K: usize = NodePage::M / 2;

        let new_ptr = rt.create();
        let new = rt.mutate::<Self>(new_ptr);
        new.stem = self.stem;
        new.len = K as u16;
        self.len = K as u16;

        new.child[..K].clone_from_slice(&self.child[K..]);
        self.child[K..].iter_mut().for_each(|x| *x = None);
        new.keys_len[..K].clone_from_slice(&self.keys_len[K..]);
        self.keys_len[K..].iter_mut().for_each(|x| *x = 0);

        let mut new_keys = [None; 0x40];
        for (ptr, new) in self.key.iter().zip(new_keys.iter_mut()) {
            let Some(ptr) = *ptr else {
                break;
            };
            let new_page_ptr = rt.create();

            let mut temp = [[0; 16]; K];
            let key_page = rt.mutate(ptr);
            key_page.keys[K..]
                .iter_mut()
                .zip(temp.iter_mut())
                .for_each(|(from, to)| *to = mem::take(from));

            let new_page = rt.mutate::<KeyPage>(new_page_ptr);
            *new = Some(new_page_ptr);
            new_page.keys[..K].clone_from_slice(&temp);
        }

        rt.mutate::<Self>(new_ptr).key = new_keys;

        new_ptr
    }

    fn insert_key(
        &mut self,
        mut rt: Rt<'_, impl Alloc, impl Free, impl AbstractIo>,
        idx: usize,
        old_len: usize,
        key: &[u8],
    ) {
        let mut it = key.chunks(0x10);
        for ptr in &mut self.key {
            let chunk = it.next();
            let absent = ptr.is_none();
            if absent && chunk.is_none() {
                break;
            }
            let ptr = *ptr.get_or_insert_with(|| rt.create());
            let page = rt.mutate(ptr);
            if !absent {
                for i in (idx..old_len).rev() {
                    page.keys[i + 1] = page.keys[i];
                }
            }
            page.keys[idx] = [0; 0x10];
            if let Some(chunk) = chunk {
                page.keys[idx][..chunk.len()].clone_from_slice(chunk);
            }
        }
    }
}

impl Node for NodePage {
    #[cfg(feature = "small")]
    const M: usize = 0x8;

    #[cfg(not(feature = "small"))]
    const M: usize = 0x100;

    fn empty() -> Self {
        NodePage {
            child: [None; Self::M],
            keys_len: [0; Self::M],
            key: [None; 64],
            stem: 1,
            len: 0,
        }
    }

    fn append_child(&mut self, ptr: PagePtr<Self>) {
        self.child[self.len()] = Some(ptr);
        self.len += 1;
    }

    fn child(&self, idx: usize) -> &Option<PagePtr<Self>> {
        &self.child[idx]
    }

    fn child_mut(&mut self, idx: usize) -> &mut Option<PagePtr<Self>> {
        &mut self.child[idx]
    }

    fn len(&self) -> usize {
        self.len as usize
    }

    fn is_leaf(&self) -> bool {
        self.stem == 0
    }

    fn read_key(&self, file: &FileIo, idx: usize) -> Vec<u8> {
        let len = self.keys_len[idx] as usize;
        let depth = len.div_ceil(0x10);
        // start with small allocation, optimistically assume the key is small
        let mut v = Vec::with_capacity(0x10 * 4);
        for i in &self.key[..depth] {
            let ptr = i.expect("BUG key length inconsistent with key pages");
            let page = file.read(ptr);
            v.extend_from_slice(&page.keys[idx]);
        }
        v.truncate(len);
        v
    }

    fn get_key(&self, rt: R<'_>, idx: usize) -> Vec<u8> {
        // start with small allocation, optimistically assume the key is small
        let mut v = Vec::with_capacity(0x10 * 4);
        for ptr in self.keys_ptr() {
            let page = rt.look(ptr);
            v.extend_from_slice(&page.keys[idx]);
        }
        v.truncate(self.keys_len[idx] as usize);
        v
    }

    // TODO: SIMD optimization
    fn search(&self, file: &FileIo, key: &[u8]) -> Result<usize, usize> {
        use std::ops::Range;

        let len = self.len() - usize::from(!self.is_leaf());

        #[inline(always)]
        fn extend_range<C>(len: usize, i: usize, range: &mut Range<usize>, cmp: C)
        where
            C: Fn(usize) -> bool,
        {
            let orig = range.clone();

            *range = (range.start + i)..(range.start + i + 1);

            while range.start > 0 && cmp(range.start - 1) {
                range.start -= 1;
            }

            while range.end < len - 1 && cmp(range.end) {
                range.end += 1;
            }

            range.start = range.start.max(orig.start);
            range.end = range.end.min(orig.end);
        }

        let mut range = 0..len;

        let mut chunks = key.chunks(0x10);
        let mut pointers = self.keys_ptr();

        for (ptr, chunk) in (&mut pointers).zip(&mut chunks) {
            let buffer = &file.read(ptr).keys;

            let mut key_b = [0; 0x10];
            let l = chunk.len().min(0x10);
            key_b[..l].clone_from_slice(&chunk[..l]);

            let i = buffer[range.clone()]
                .binary_search(&key_b)
                .map_err(|i| range.start + i)?;

            extend_range(len, i, &mut range, |i| buffer[i] == key_b);
        }

        let original_len = key.len() as u16;
        let i = self.keys_len[range.clone()]
            .binary_search(&original_len)
            .map_err(|i| range.start + i)?;

        extend_range(len, i, &mut range, |i| self.keys_len[i] == original_len);

        if chunks.next().is_some() {
            Err(range.end)
        } else if pointers.next().is_some() {
            if range.len() == 1 {
                Ok(range.start)
            } else {
                Err(range.start)
            }
        } else if range.len() == 1 {
            Ok(range.start)
        } else {
            panic!("BUG: two identical keys detected {}", hex::encode(key));
        }
    }

    fn realloc_keys(&mut self, mut rt: R) {
        for ptr in self.key.iter_mut().flatten() {
            rt.read(ptr);
        }
    }

    fn insert(
        &mut self,
        mut rt: R,
        new_child_ptr: Option<PagePtr<Self>>,
        idx: usize,
        key: &[u8],
        rev: bool,
    ) -> Option<(Vec<u8>, PagePtr<Self>)> {
        let old_len = self.len();
        self.len = (old_len + 1) as u16;

        for i in (idx..old_len).rev() {
            self.child[i + 1] = self.child[i];
            self.keys_len[i + 1] = self.keys_len[i];
        }

        self.child[idx] = new_child_ptr;
        if rev {
            self.child.swap(idx, idx + 1);
        }
        self.keys_len[idx] = key.len() as u16;
        self.insert_key(rt.reborrow(), idx, old_len, key);

        if self.len() == Self::M {
            let new_ptr = self.split(rt.reborrow());
            let key = self.get_key(rt.reborrow(), Self::M / 2 - 1);

            Some((key, new_ptr))
        } else {
            None
        }
    }

    fn remove(&mut self, mut rt: R, idx: usize, rev: bool) -> (Option<PagePtr<Self>>, Vec<u8>) {
        let new_len = self.len() - 1;
        self.len = new_len as u16;

        let old_ptr = self.child[idx];
        let old_key_len = self.keys_len[idx];

        if rev {
            self.child.swap(idx, idx + 1);
        }

        for i in idx..new_len {
            self.child[i] = self.child[i + 1];
            self.keys_len[i] = self.keys_len[i + 1];
        }
        // just in case
        self.child[new_len] = None;

        // start with small allocation, optimistically assume the key is small
        let mut v = Vec::with_capacity(0x10 * 4);
        for ptr in self.keys_ptr() {
            let page = rt.mutate(ptr);
            v.extend_from_slice(&page.keys[idx]);
            for i in idx..new_len {
                page.keys[i] = page.keys[i + 1];
            }
        }
        v.truncate(old_key_len as usize);

        (old_ptr, v)
    }

    fn set_key(&mut self, mut rt: R, idx: usize, key: &[u8]) -> Vec<u8> {
        let old_key_len = mem::replace(&mut self.keys_len[idx], key.len() as u16);

        let chunks = key.chunks(0x10);

        let mut v = Vec::with_capacity(0x10 * 4);
        for (ptr, chunk) in self.key.iter_mut().zip(chunks) {
            let ptr = ptr.get_or_insert_with(|| rt.create());
            let page = rt.mutate(*ptr);
            v.extend_from_slice(&page.keys[idx]);

            page.keys[idx] = [0; 0x10];
            let l = chunk.len().min(0x10);
            page.keys[idx][..l].clone_from_slice(&chunk[..l]);
        }
        v.truncate(old_key_len as usize);
        v
    }

    fn merge(&mut self, other: &Self, mut rt: R<'_>, key: &[u8], old: bool) -> Vec<u8> {
        let new_len = self.len + other.len;
        if !self.is_leaf() {
            self.set_key(rt.reborrow(), self.len() - 1, key);
        }
        let to = (self.len as usize)..(new_len as usize);
        let from = 0..(other.len as usize);
        self.child[to.clone()].clone_from_slice(&other.child[from.clone()]);
        // self.keys_len[to.clone()].clone_from_slice(&other.keys_len[from.clone()]);
        // self.table_id[to.clone()].clone_from_slice(&other.table_id[from.clone()]);
        // TODO: optimize
        let mut last_key = None;
        if old {
            for (to, from) in to.zip(from) {
                let key = other.read_key(rt.io, from);
                if !key.is_empty() {
                    last_key = Some(key.clone());
                }
                self.set_key(rt.reborrow(), to, &key);
            }
        } else {
            for (to, from) in to.zip(from) {
                let key = other.get_key(rt.reborrow(), from);
                if !key.is_empty() {
                    last_key = Some(key.clone());
                }
                self.set_key(rt.reborrow(), to, &key);
            }
        }
        self.len = new_len;
        last_key.expect("loop must be not empty")
    }

    fn free(&self, rt: R<'_>) {
        for ptr in self.keys_ptr() {
            rt.free.free(ptr);
        }
    }
}
