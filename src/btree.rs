use super::{
    file::{PlainData, FileIo},
    page::{PagePtr, PAGE_SIZE},
    db::DbView,
};

const M: usize = 128;

#[derive(Clone, Copy, Default)]
pub struct DataDescriptor {
    pub ptr: Option<PagePtr<DataPage>>,
    pub len: usize,
}

pub fn get<const KEY_SIZE: usize>(
    view: &DbView<'_>,
    mut ptr: PagePtr<NodePage<KEY_SIZE>>,
    key: &[u8; KEY_SIZE],
) -> Option<DataDescriptor> {
    loop {
        let page = view.page(ptr);
        let idx = page.keys().binary_search(key).ok()?;
        let child = page.child[idx];
        if page.leaf {
            let ptr = unsafe { child.leaf };
            let len = page.lengths[idx / 2].get(idx);
            return if len != 0 && ptr.is_none() {
                None
            } else {
                Some(DataDescriptor { ptr, len })
            };
        } else {
            ptr = unsafe { child.node? };
        }
    }
}

pub fn insert<const KEY_SIZE: usize>(
    file: &FileIo,
    stem_ptr: &mut &[PagePtr<NodePage<KEY_SIZE>>],
    key: &[u8; KEY_SIZE],
    data: DataDescriptor,
) -> Option<DataDescriptor> {
    let _ = (file, stem_ptr, key, data);
    Lengths::default().set(0, data.len);
    unimplemented!()
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct NodePage<const KEY_SIZE: usize> {
    child: [Child<KEY_SIZE>; M],
    lengths: [Lengths; M / 2],
    keys: [[u8; KEY_SIZE]; M],
    leaf: bool,
    len: usize,
}

unsafe impl<const KEY_SIZE: usize> PlainData for NodePage<KEY_SIZE> {}

impl<const KEY_SIZE: usize> NodePage<KEY_SIZE> {
    fn keys(&self) -> &[[u8; KEY_SIZE]] {
        &self.keys[..self.len]
    }
}

#[repr(C)]
#[derive(Clone, Copy, Default)]
struct Lengths {
    hi: [[u8; 4]; 2],
    lo: [u8; 2],
    least: u8,
}

impl Lengths {
    #[cfg(test)]
    fn check() {
        assert_eq!(PAGE_SIZE, 1 << 12);
    }

    fn set(&mut self, idx: usize, value: usize) {
        let hi = (value >> 12) as u32;
        self.hi[idx % 2] = hi.to_ne_bytes();

        let lo = ((value % (PAGE_SIZE as usize)) >> 4) as u8;
        self.lo[idx % 2] = lo;

        let shift = 4 * (idx % 2);
        self.least &= 0xf << (4 - shift);
        self.least |= (value as u8 & 0xf) << shift;
    }

    fn get(&self, idx: usize) -> usize {
        ((u32::from_ne_bytes(self.hi[idx % 2]) as usize) << 12)
            + ((self.lo[idx % 2] as usize) << 4)
            + ((self.least >> (4 * (idx % 2))) & 0xf) as usize
    }
}

#[repr(C)]
#[derive(Clone, Copy)]
union Child<const KEY_SIZE: usize> {
    node: Option<PagePtr<NodePage<KEY_SIZE>>>,
    leaf: Option<PagePtr<DataPage>>,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct DataPage(pub [u8; PAGE_SIZE as usize]);

unsafe impl PlainData for DataPage {}

#[cfg(test)]
mod tests {
    use std::mem;

    use super::{NodePage, Lengths};

    #[test]
    fn length_pack() {
        Lengths::check();

        let mut length = Lengths::default();
        length.set(0, 0x12345678_abc);
        length.set(1, 0xfedcba98_765);

        assert_eq!(length.get(0), 0x12345678_abc);
        assert_eq!(length.get(1), 0xfedcba98_765);

        length.set(0, 0xfedcba98_765);
        length.set(1, 0x12345678_abc);

        assert_eq!(length.get(0), 0xfedcba98_765);
        assert_eq!(length.get(1), 0x12345678_abc);
    }

    #[test]
    fn node_size() {
        let len = mem::size_of::<NodePage<22>>();
        println!("{len}");
        assert!(len <= 0x1000);
    }
}
