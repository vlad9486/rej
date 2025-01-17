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
            regular_file,
            cache: Mutex::new(Cache::new(cipher)?),
            #[cfg(test)]
            simulator: Simulator::default(),
        })
    }

    pub fn m_lock(&self) {
        utils::m_lock(&self.cache.lock().expect("poisoned").cipher);
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
        self.cache.lock().expect("poisoned").sync(&self.file)
    }

    pub fn grow<T>(&self, old: u32, n: u32) -> io::Result<Option<PagePtr<T>>> {
        self.set_pages(old + n)?;

        use super::runtime::PBox;

        let mut cache = self.cache.lock().expect("poisoned");
        for i in old..(old + n) {
            let page = PBox::new(4096, [0; PAGE_SIZE as usize]);
            cache.write(&self.file, i, page)?;
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
        self.cache.lock().expect("poisoned").read(&self.file, n)
    }

    fn write_page(&self, n: u32, page: PBox) -> io::Result<()> {
        self.write_stats(u64::from(n) * PAGE_SIZE);

        self.cache
            .lock()
            .expect("poisoned")
            .write(&self.file, n, page)
    }

    fn write_batch(&self, it: impl IntoIterator<Item = (u32, PBox)>) -> io::Result<()> {
        // no special treatment for batch
        for (n, page) in it {
            self.write_page(n, page)?;
        }

        Ok(())
    }
}

fn n_to_o(n: u32) -> u64 {
    (u64::from(n) * PAGE_SIZE) + CRYPTO_SIZE as u64
}

struct Cache {
    cipher: Cipher,
    ring: IoUring,
    log: Option<(u32, CacheItem)>,
    inner: BTreeMap<u32, CacheItem>,
}

struct CacheItem {
    page: PBox,
    dirty: bool,
}

impl Cache {
    fn new(cipher: Cipher) -> io::Result<Self> {
        Ok(Cache {
            cipher,
            ring: IoUring::new(1024)?,
            log: None,
            inner: BTreeMap::default(),
        })
    }
}

impl Cache {
    fn sync(&mut self, file: &fs::File) -> io::Result<()> {
        use io_uring::{opcode, types};
        use std::os::unix::io::AsRawFd;

        let mut map = mem::take(&mut self.inner);
        let mut log = self.log.take();
        let it = map
            .iter_mut()
            .chain(log.as_mut().map(|(n, p)| (&*n, p)))
            .filter(|(_, item)| item.dirty)
            .map(|(n, item)| {
                let data = &mut *item.page;
                self.cipher.encrypt(data, *n);
                (*n, data.as_ptr())
            });

        let fd = file.as_raw_fd();

        for (n, ptr) in it {
            let op = opcode::Write::new(types::Fd(fd), ptr, 0x1000)
                .offset(n_to_o(n))
                .build()
                .user_data(n as _);

            if unsafe { self.ring.submission().push(&op).is_err() } {
                let l = self.ring.submission().len();
                self.ring.submit_and_wait(l)?;
                self.ring.completion().sync();
                while let Some(cqe) = self.ring.completion().next() {
                    if cqe.result() < 0 {
                        log::error!("Error: {}", io::Error::from_raw_os_error(-cqe.result()));
                    }
                }
                unsafe { self.ring.submission().push(&op).unwrap() };
            }
        }

        let l = self.ring.submission().len();
        if l == 0 {
            return Ok(());
        }

        self.ring.submit_and_wait(l)?;
        while let Some(cqe) = self.ring.completion().next() {
            if cqe.result() < 0 {
                log::error!("Error: {}", io::Error::from_raw_os_error(-cqe.result()));
            }
        }

        Ok(())
    }

    fn write(&mut self, _file: &fs::File, n: u32, page: PBox) -> io::Result<()> {
        let item = CacheItem { page, dirty: true };
        if n < 256 {
            self.log = Some((n, item));
        } else {
            self.inner.insert(n.into(), item);
        }

        Ok(())
    }

    fn read(&mut self, file: &fs::File, n: u32) -> io::Result<PBox> {
        if let Some(item) = self.inner.get(&n) {
            return Ok(item.page.clone());
        }

        let mut page = PBox::new(4096, [0; PAGE_SIZE as usize]);

        utils::read_at(file, &mut *page, n_to_o(n))?;
        self.cipher.decrypt(&mut *page, n);
        if n >= 256 {
            let item = CacheItem {
                page: page.clone(),
                dirty: false,
            };
            self.inner.insert(n, item);
        }
        Ok(page)
    }
}
