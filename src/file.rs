use std::{
    fs, io,
    path::Path,
    sync::atomic::{AtomicU32, Ordering},
};

use fs4::fs_std::FileExt;

use super::{
    utils,
    page::{PagePtr, RawPtr, PAGE_SIZE},
    runtime::AbstractIo,
};
use super::cipher::{self, Cipher, CipherError, Params, CRYPTO_SIZE};

#[cfg(test)]
#[derive(Clone, Copy)]
pub struct Simulator {
    pub crash_at: u32,
    pub mess_page: bool,
}

#[cfg(test)]
impl Default for Simulator {
    fn default() -> Self {
        Simulator {
            crash_at: u32::MAX,
            mess_page: false,
        }
    }
}

pub struct FileIo {
    file: fs::File,
    write_counter: AtomicU32,
    cipher: Cipher,
    regular_file: bool,
    #[cfg(test)]
    pub simulator: Simulator,
}

impl FileIo {
    const CRYPTO_PAGES: u32 = (CRYPTO_SIZE as u64 / PAGE_SIZE) as u32;

    pub fn new(path: impl AsRef<Path>, params: Params) -> Result<Self, CipherError> {
        use std::os::unix::fs::FileTypeExt;

        let file = utils::open_file(path, true)?;
        let metadata = file.metadata()?;
        let regular_file = !metadata.file_type().is_block_device();
        if regular_file {
            file.lock_exclusive()?;
            if params.create() {
                file.set_len(CRYPTO_SIZE as u64)?;
            }
        }

        let cipher = Cipher::new(&file, params)?;

        Ok(FileIo {
            file,
            write_counter: AtomicU32::new(0),
            cipher,
            regular_file,
            #[cfg(test)]
            simulator: Simulator::default(),
        })
    }

    pub fn m_lock(&self) {
        utils::m_lock(&self.cipher);
    }

    pub fn crypt_shred(&self, seed: &[u8]) -> Result<(), CipherError> {
        let blob = cipher::shred(seed)?;
        if !blob.is_empty() {
            utils::write_at(&self.file, &blob, 0)?;
        }
        Ok(())
    }

    fn write_stats(&self, offset: u64) {
        let old = self.write_counter.fetch_add(1, Ordering::SeqCst);
        #[cfg(test)]
        {
            use rand::RngCore;

            if old == self.simulator.crash_at {
                if self.simulator.mess_page {
                    let mut data = [0; PAGE_SIZE as usize];
                    rand::thread_rng().fill_bytes(&mut data);
                    utils::write_at(&self.file, &data, offset).unwrap_or_default();
                }
                panic!("intentional panic for test");
            }
        }
        #[cfg(not(test))]
        let _ = (old, offset);
    }

    pub fn sync(&self) -> io::Result<()> {
        self.file.sync_all()
    }

    pub fn grow<T>(&self, old: u32, n: u32) -> io::Result<Option<PagePtr<T>>> {
        if self.regular_file {
            self.file
                .set_len((old + n + Self::CRYPTO_PAGES) as u64 * PAGE_SIZE)?;
        }

        #[cfg(feature = "cipher")]
        {
            use super::runtime::PBox;

            let mut page = PBox::new(4096, [0; PAGE_SIZE as usize]);

            for i in 0..n {
                self._write(old + i, &mut *page)?;
            }
        }

        Ok(PagePtr::from_raw_number(old))
    }

    pub fn set_pages(&self, pages: u32) -> io::Result<()> {
        if self.regular_file {
            self.file
                .set_len((pages + Self::CRYPTO_PAGES) as u64 * PAGE_SIZE)?;
        }

        Ok(())
    }

    pub fn writes(&self) -> u32 {
        self.write_counter.load(Ordering::SeqCst)
    }

    fn _write(&self, n: u32, bytes: &mut [u8]) -> io::Result<()> {
        let offset = u64::from(n) * PAGE_SIZE;
        self.write_stats(offset);

        self.cipher.encrypt(bytes, n);
        utils::write_at(&self.file, &*bytes, offset + (CRYPTO_SIZE as u64))
    }
}

impl AbstractIo for FileIo {
    fn read_page(
        &self,
        ptr: impl Into<Option<PagePtr<()>>>,
        page: &mut [u8; PAGE_SIZE as usize],
    ) -> io::Result<()> {
        let n = ptr.into().map_or(0, PagePtr::raw_number);
        let offset = (u64::from(n) * PAGE_SIZE) + CRYPTO_SIZE as u64;
        utils::read_at(&self.file, page, offset)?;
        self.cipher.decrypt(page, n);

        Ok(())
    }

    fn write_bytes(&self, ptr: impl Into<Option<PagePtr<()>>>, bytes: &mut [u8]) -> io::Result<()> {
        self._write(ptr.into().map_or(0, PagePtr::raw_number), bytes)
    }
}
