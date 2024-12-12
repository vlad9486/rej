use super::{
    page::{PAGE_SIZE, PagePtr},
    runtime::PlainData,
};

#[repr(C)]
#[derive(Clone, Copy)]
pub struct MetadataPage {
    len: u64,
    data: Data,
}

#[repr(C)]
#[derive(Clone, Copy)]
enum Data {
    Immediately([u8; 4084]),
    #[allow(dead_code)]
    Indirect([Option<PagePtr<[u8; PAGE_SIZE as usize]>>; 1021]),
}

impl MetadataPage {
    pub const fn empty() -> Self {
        MetadataPage {
            len: 0,
            data: Data::Immediately([0; 4084]),
        }
    }

    pub fn len(&self) -> usize {
        self.len as usize
    }

    pub fn put_data(&mut self, buf: &[u8]) -> usize {
        self.len = buf.len() as u64;
        match &mut self.data {
            Data::Immediately(data) => {
                data[..buf.len()].clone_from_slice(buf);
            }
            Data::Indirect(_) => unimplemented!(),
        }
        // size of discriminant is 4
        memoffset::offset_of!(MetadataPage, data) + 4 + buf.len()
    }

    pub fn read(&self, offset: usize, buf: &mut [u8]) {
        match &self.data {
            Data::Immediately(data) => {
                buf.clone_from_slice(&data[offset..][..buf.len()]);
            }
            Data::Indirect(_) => unimplemented!(),
        }
    }
}

unsafe impl PlainData for MetadataPage {
    const NAME: &str = "Metadata";
}
