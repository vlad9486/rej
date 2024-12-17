use std::{
    fs, io,
    path::Path,
    sync::{
        atomic::{AtomicU32, Ordering},
        RwLock, RwLockReadGuard,
    },
};

use memmap2::Mmap;
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
    pub mmap_populate: bool,
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
    cfg: IoOptions,
    file: fs::File,
    file_len: AtomicU32,
    write_counter: AtomicU32,
    mapped: RwLock<Mmap>,
    cipher: Cipher,
}

impl FileIo {
    const CRYPTO_PAGES: u32 = (CRYPTO_SIZE as u64 / PAGE_SIZE) as u32;

    pub fn new(
        path: impl AsRef<Path>,
        cfg: IoOptions,
        params: Params,
    ) -> Result<Self, CipherError> {
        let file = utils::open_file(path, params.create(), cfg.direct_write)?;
        file.lock_exclusive()?;

        if params.create() {
            file.set_len(CRYPTO_SIZE as u64)?;
        }

        let map = utils::mmap(&file, cfg.mmap_populate)?;

        let cipher = Cipher::new(&file, &map, params)?;

        let file_len = AtomicU32::new((file.metadata()?.len() / PAGE_SIZE) as u32);
        let mapped = RwLock::new(map);

        Ok(FileIo {
            cfg,
            file,
            file_len,
            write_counter: AtomicU32::new(0),
            mapped,
            cipher,
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

            let simulator = self.cfg.simulator;
            if old == simulator.crash_at {
                if simulator.mess_page {
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

    pub fn grow<T>(&self, n: u32) -> io::Result<Option<PagePtr<T>>> {
        let mut lock = self.mapped.write().expect("poisoned");

        let old_len = self.file_len.load(Ordering::SeqCst);

        self.file.set_len((old_len + n) as u64 * PAGE_SIZE)?;
        self.file_len.store(old_len + n, Ordering::SeqCst);
        *lock = utils::mmap(&self.file, self.cfg.mmap_populate)?;

        let n = old_len - Self::CRYPTO_PAGES;
        #[cfg(feature = "cipher")]
        self._write(n, &[0; PAGE_SIZE as usize])?;

        Ok(PagePtr::from_raw_number(n))
    }

    pub fn set_pages(&self, pages: u32) -> io::Result<()> {
        let pages = pages + Self::CRYPTO_PAGES;

        let mut lock = self.mapped.write().expect("poisoned");
        self.file.set_len((pages as u64) * PAGE_SIZE)?;
        self.file_len.store(pages, Ordering::SeqCst);
        *lock = utils::mmap(&self.file, self.cfg.mmap_populate)?;

        Ok(())
    }

    pub fn pages(&self) -> u32 {
        self.file_len.load(Ordering::SeqCst) - Self::CRYPTO_PAGES
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
        PageView(self.mapped.read().expect("poisoned"), &self.cipher)
    }

    fn write_bytes(&self, ptr: impl Into<Option<PagePtr<()>>>, bytes: &[u8]) -> io::Result<()> {
        self._write(ptr.into().map_or(0, PagePtr::raw_number), bytes)
    }
}

pub struct PageView<'a>(RwLockReadGuard<'a, Mmap>, &'a Cipher);

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
        let offset = (u64::from(n) * PAGE_SIZE) as usize + CRYPTO_SIZE;
        DecryptedPage::new(&self.0[offset..][..(PAGE_SIZE as usize)], self.1, n)
    }
}
