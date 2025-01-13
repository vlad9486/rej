use super::{page::PAGE_SIZE, runtime::PlainData};

#[repr(C)]
#[derive(Clone, Copy)]
pub struct MetadataPage {
    plain: [u8; PAGE_SIZE as usize],
}

impl MetadataPage {
    pub const fn empty() -> Self {
        MetadataPage {
            plain: [0; PAGE_SIZE as _],
        }
    }

    pub fn put_plain_at(&mut self, offset: usize, buf: &[u8]) {
        let this = &mut self.plain;
        this[offset..][..buf.len()].clone_from_slice(buf);
    }

    pub fn read_plain(&self, offset: usize, buf: &mut [u8]) {
        let this = &self.plain;
        buf.clone_from_slice(&this[offset..][..buf.len()]);
    }
}

unsafe impl PlainData for MetadataPage {
    const NAME: &str = "Metadata";
}
