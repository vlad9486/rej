use std::io;

use super::{
    page::{PAGE_SIZE, PagePtr},
    runtime::{PlainData, AbstractIo, AbstractViewer, Alloc, Free},
};

#[repr(C)]
#[derive(Clone, Copy)]
pub union MetadataPage {
    plain: [u8; PAGE_SIZE as usize],
    indirect: Data,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct Data {
    header: [u8; 0x100],
    pointers: [Option<PagePtr<[u8; PAGE_SIZE as usize]>>; 0x3c0],
}

impl MetadataPage {
    pub const fn empty() -> Self {
        MetadataPage {
            plain: [0; PAGE_SIZE as _],
        }
    }

    pub fn put_plain_at(&mut self, offset: usize, buf: &[u8]) {
        let this = unsafe { &mut self.plain };
        this[offset..][..buf.len()].clone_from_slice(buf);
    }

    pub fn put_indirect_at(
        &mut self,
        alloc: &mut impl Alloc,
        io: &impl AbstractIo,
        offset: usize,
        buf: &[u8],
    ) -> io::Result<()> {
        if offset != 0 {
            unimplemented!();
        }

        let this = unsafe { &mut self.indirect };
        for (chunk, ptr) in buf.chunks(PAGE_SIZE as usize).zip(this.pointers.iter_mut()) {
            let ptr = ptr.get_or_insert_with(|| alloc.alloc());
            let mut page = [0; PAGE_SIZE as usize];
            page[..chunk.len()].clone_from_slice(chunk);
            io.write(*ptr, &page)?;
        }

        Ok(())
    }

    pub fn read_plain(&self, offset: usize, buf: &mut [u8]) {
        let this = unsafe { &self.plain };
        buf.clone_from_slice(&this[offset..][..buf.len()]);
    }

    pub fn read_indirect(&self, view: &impl AbstractViewer, offset: usize, mut buf: &mut [u8]) {
        let this = unsafe { &self.indirect };

        const P: usize = PAGE_SIZE as usize;

        let idx = offset / P;
        let mut offset = offset % P;
        let mut pointers = this.pointers[idx..].iter().copied();
        while !buf.is_empty() {
            let Some(ptr) = pointers.next() else {
                break;
            };
            let page = view.page(ptr);
            let l = (P - offset).min(buf.len());
            buf[..l].clone_from_slice(&page[offset..(offset + l)]);
            buf = &mut buf[l..];
            offset = 0;
        }
    }

    pub fn deallocate_indirect(&self, free: &mut impl Free) {
        let this = unsafe { &self.indirect };
        for item in this.pointers.iter().flatten() {
            free.free(*item);
        }
    }
}

unsafe impl PlainData for MetadataPage {
    const NAME: &str = "Metadata";
}
