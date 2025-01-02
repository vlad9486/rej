use std::{
    fs, io,
    path::Path,
    sync::atomic::{AtomicU32, Ordering},
};

use fs4::fs_std::FileExt;

use super::{
    utils,
    page::{PagePtr, RawPtr, PAGE_SIZE},
    runtime::{PlainData, AbstractIo, AbstractViewer},
};
use super::cipher::{self, Cipher, CipherError, Params, CRYPTO_SIZE, EncryptedPage, DecryptedPage};

#[derive(Default, Clone)]
pub struct IoOptions {
    pub direct_write: bool,
    #[cfg(test)]
    pub simulator: Simulator,
}

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

#[cfg(test)]
impl IoOptions {
    pub fn simulator(crash_at: u32, mess_page: bool) -> Self {
        IoOptions {
            simulator: Simulator {
                crash_at,
                mess_page,
            },
            ..Default::default()
        }
    }
}

pub struct FileIo {
    file: fs::File,
    write_counter: AtomicU32,
    cipher: Cipher,
    regular_file: bool,
    #[cfg(test)]
    simulator: Simulator,
}

impl FileIo {
    const CRYPTO_PAGES: u32 = (CRYPTO_SIZE as u64 / PAGE_SIZE) as u32;

    pub fn new(
        path: impl AsRef<Path>,
        cfg: IoOptions,
        params: Params,
    ) -> Result<Self, CipherError> {
        use std::os::unix::fs::FileTypeExt;

        let file = utils::open_file(path, params.create(), cfg.direct_write)?;
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
            simulator: cfg.simulator,
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
        for i in 0..n {
            self._write(old + i, &[0; PAGE_SIZE as usize])?;
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

    fn _write(&self, n: u32, bytes: &[u8]) -> io::Result<()> {
        let offset = u64::from(n) * PAGE_SIZE;
        self.write_stats(offset);
        let page = EncryptedPage::new(bytes, &self.cipher, n);
        utils::write_at(&self.file, &page, offset + (CRYPTO_SIZE as u64))
    }
}

impl AbstractIo for FileIo {
    type Viewer<'a> = PageView<'a>;

    fn read(&self) -> Self::Viewer<'_> {
        PageView(&self.file, &self.cipher)
    }

    fn write_bytes(&self, ptr: impl Into<Option<PagePtr<()>>>, bytes: &[u8]) -> io::Result<()> {
        self._write(ptr.into().map_or(0, PagePtr::raw_number), bytes)
    }
}

pub struct PageView<'a>(&'a fs::File, &'a Cipher);

impl AbstractViewer for PageView<'_> {
    type Page<'a, T>
        = DecryptedPage<'a, T>
    where
        Self: 'a,
        T: PlainData + 'a;

    fn page<'a, T>(&'a self, ptr: impl Into<Option<PagePtr<T>>>) -> Self::Page<'a, T>
    where
        T: PlainData + 'a,
    {
        let n = ptr.into().map_or(0, PagePtr::raw_number);
        let offset = (u64::from(n) * PAGE_SIZE) + CRYPTO_SIZE as u64;
        let mut page = Box::new([0; PAGE_SIZE as usize]);
        // TODO: how to handle this? introduce a cache
        utils::read_at(&self.0, page.as_mut(), offset).expect("reading should not fail");
        DecryptedPage::new(page, self.1, n)
    }
}
