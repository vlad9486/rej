use std::{
    collections::BTreeMap,
    fs, io, mem,
    path::Path,
    sync::{
        atomic::{AtomicU32, Ordering},
        Mutex,
    },
};

use fs4::fs_std::FileExt;
use io_uring::IoUring;

use super::{
    utils,
    page::{PagePtr, RawPtr, PAGE_SIZE},
    runtime::{AbstractIo, PBox},
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
    cache: Mutex<Cache>,
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
            cache: Mutex::new(Cache::run()?),
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
        self.cache
            .lock()
            .expect("poisoned")
            .flush(&self.file, &self.cipher)
    }

    pub fn grow<T>(&self, old: u32, n: u32) -> io::Result<Option<PagePtr<T>>> {
        if self.regular_file {
            self.file
                .set_len((old + n + Self::CRYPTO_PAGES) as u64 * PAGE_SIZE)?;
        }

        use super::runtime::PBox;

        let mut cache = self.cache.lock().expect("poisoned");
        for i in old..(old + n) {
            let page = PBox::new(4096, [0; PAGE_SIZE as usize]);
            cache.write(&self.file, &self.cipher, i, page)?;
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
}

impl AbstractIo for FileIo {
    fn read_page(&self, n: u32) -> io::Result<PBox> {
        self.cache
            .lock()
            .expect("poisoned")
            .read(&self.file, &self.cipher, n)
    }

    fn write_page(&self, n: u32, page: PBox) -> io::Result<()> {
        self.write_stats(u64::from(n) * PAGE_SIZE);

        self.cache
            .lock()
            .expect("poisoned")
            .write(&self.file, &self.cipher, n, page)
    }

    fn write_batch(&self, it: impl IntoIterator<Item = (u32, PBox)>) -> io::Result<()> {
        self.cache.lock().expect("poisoned").write_batch(
            &self.file,
            &self.cipher,
            it.into_iter().map(|(n, page)| {
                self.write_stats(u64::from(n) * PAGE_SIZE);
                (n, page)
            }),
        )
    }
}

fn n_to_o(n: u32) -> u64 {
    (u64::from(n) * PAGE_SIZE) + CRYPTO_SIZE as u64
}

pub struct Cache {
    ring: IoUring,
    inner: BTreeMap<u32, PBox>,
}

impl Cache {
    fn run() -> io::Result<Self> {
        let ring = IoUring::new(1024)?;
        Ok(Cache {
            ring,
            inner: BTreeMap::default(),
        })
    }
}

impl Cache {
    fn flush(&mut self, file: &fs::File, cipher: &Cipher) -> io::Result<()> {
        let mut map = mem::take(&mut self.inner);
        let it = map.iter_mut().map(|(n, page)| {
            cipher.encrypt(&mut **page, *n);
            (n_to_o(*n), page.as_ptr())
        });
        utils::write_v_at(file, &mut self.ring, it)?;

        Ok(())
    }

    fn write(&mut self, file: &fs::File, cipher: &Cipher, n: u32, page: PBox) -> io::Result<()> {
        self.inner.insert(n, page);
        if self.inner.len() > 256 {
            self.flush(file, cipher)?;
        }

        Ok(())
    }

    fn write_batch(
        &mut self,
        file: &fs::File,
        cipher: &Cipher,
        it: impl IntoIterator<Item = (u32, PBox)>,
    ) -> io::Result<()> {
        self.inner.extend(it);
        if self.inner.len() > 256 {
            self.flush(file, cipher)?;
        }

        Ok(())
    }

    fn read(&mut self, file: &fs::File, cipher: &Cipher, n: u32) -> io::Result<PBox> {
        if let Some(page) = self.inner.get(&n) {
            return Ok(page.clone());
        }

        let mut page = PBox::new(4096, [0; PAGE_SIZE as usize]);

        utils::read_at(file, &mut *page, n_to_o(n))?;
        cipher.decrypt(&mut *page, n);
        Ok(page)
    }
}
