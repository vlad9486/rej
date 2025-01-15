use super::{page::PAGE_SIZE, runtime::PlainData};

#[repr(C, align(0x1000))]
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
}

unsafe impl PlainData for MetadataPage {
    const NAME: &str = "Metadata";
}
