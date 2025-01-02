use std::io;

use super::{
    page::{PAGE_SIZE, PagePtr},
    runtime::{PlainData, AbstractIo, AbstractViewer, Alloc, Free},
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

    pub fn put_at(&mut self, offset: usize, buf: &[u8]) {
        match &mut self.data {
            Data::Immediately(array) => {
                array[offset..][..buf.len()].clone_from_slice(buf);
                self.len = self.len.max((offset + buf.len()) as u64);
            }
            Data::Indirect(_) => unimplemented!(),
        }
    }

    // TODO: rework big value
    pub fn put_data(
        &mut self,
        alloc: &mut impl Alloc,
        io: &impl AbstractIo,
        buf: &[u8],
    ) -> io::Result<()> {
        self.len = buf.len() as u64;
        if buf.len() <= 4084 {
            let mut array = [0; 4084];
            array[..buf.len()].clone_from_slice(buf);
            self.data = Data::Immediately(array);
        } else {
            let mut array = [None; 1021];
            for (chunk, ptr) in buf.chunks(PAGE_SIZE as usize).zip(array.iter_mut()) {
                let ptr = ptr.get_or_insert_with(|| alloc.alloc());
                let mut page = [0; PAGE_SIZE as usize];
                page[..chunk.len()].clone_from_slice(chunk);
                io.write(*ptr, &page)?;
            }
            self.data = Data::Indirect(array);
        }

        Ok(())
    }

    pub fn read(&self, view: &impl AbstractViewer, offset: usize, buf: &mut [u8]) {
        let len = buf.len();
        let mut buf = &mut buf[..(self.len as usize - offset).min(len)];
        match &self.data {
            Data::Immediately(array) => {
                buf.clone_from_slice(&array[offset..][..buf.len()]);
            }
            Data::Indirect(array) => {
                const P: usize = PAGE_SIZE as usize;

                let idx = offset / P;
                let mut offset = offset % P;
                let mut pointers = array[idx..].iter().copied();
                while !buf.is_empty() {
                    let Some(ptr) = pointers.next().flatten() else {
                        break;
                    };
                    let page = view.page(ptr);
                    let l = (P - offset).min(buf.len());
                    buf[..l].clone_from_slice(&page[offset..(offset + l)]);
                    buf = &mut buf[l..];
                    offset = 0;
                }
            }
        }
    }

    pub fn deallocate(&self, free: &mut impl Free) {
        match &self.data {
            Data::Immediately(_) => {}
            Data::Indirect(list) => {
                for item in list.iter().flatten() {
                    free.free(*item);
                }
            }
        }
    }
}

unsafe impl PlainData for MetadataPage {
    const NAME: &str = "Metadata";
}
