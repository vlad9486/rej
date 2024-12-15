use std::{
    fs, io,
    marker::PhantomData,
    ops::Deref,
    path::Path,
    sync::{
        atomic::{AtomicU32, Ordering},
        RwLock, RwLockReadGuard,
    },
};

use memmap2::Mmap;
use fs4::fs_std::FileExt;

#[cfg(feature = "cipher")]
use {
    adiantum::cipher::{KeyInit, zeroize::Zeroize},
    chacha20::XChaCha12,
    aes::Aes256,
    chacha20poly1305::ChaCha20Poly1305,
};

use super::{
    utils,
    page::{PagePtr, RawPtr, PAGE_SIZE},
    runtime::{PlainData, AbstractIo, AbstractViewer},
};

#[derive(Default, Clone)]
pub struct IoOptions {
    pub direct_write: bool,
    pub mmap_populate: bool,
    #[cfg(test)]
    pub simulator: Simulator,
    #[cfg(feature = "cipher")]
    pub passphrase: String,
    #[cfg(feature = "cipher")]
    pub crypto_seed: [u8; 32],
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

#[cfg(feature = "cipher")]
type Cipher = adiantum::Cipher<XChaCha12, Aes256>;

#[cfg(feature = "cipher")]
const CRYPTO_SIZE: usize = 0x100000;
#[cfg(not(feature = "cipher"))]
const CRYPTO_SIZE: usize = 0;

pub struct FileIo {
    cfg: IoOptions,
    file: fs::File,
    file_len: AtomicU32,
    write_counter: AtomicU32,
    mapped: RwLock<Mmap>,
    #[cfg(feature = "cipher")]
    cipher: Cipher,
}

impl FileIo {
    const CRYPTO_PAGES: u32 = (CRYPTO_SIZE as u64 / PAGE_SIZE) as u32;

    pub fn new(path: impl AsRef<Path>, create: bool, cfg: IoOptions) -> io::Result<Self> {
        let file = utils::open_file(path, create, cfg.direct_write)?;
        file.lock_exclusive()?;

        #[cfg(feature = "cipher")]
        let mut cfg = cfg;

        #[cfg(feature = "cipher")]
        let (cipher, mapping) = if create {
            file.set_len(CRYPTO_SIZE as u64)?;
            let c = Self::crypt_setup(&file, &cfg.passphrase, cfg.crypto_seed)?;
            cfg.crypto_seed.zeroize();
            cfg.passphrase.zeroize();
            (c, utils::mmap(&file, cfg.mmap_populate)?)
        } else {
            let mapping = utils::mmap(&file, cfg.mmap_populate)?;
            let c = Self::crypt_open(&mapping, &cfg.passphrase)?;
            cfg.passphrase.zeroize();
            (c, mapping)
        };
        #[cfg(not(feature = "cipher"))]
        let mapping = utils::mmap(&file, cfg.mmap_populate)?;

        let file_len = AtomicU32::new((file.metadata()?.len() / PAGE_SIZE) as u32);
        let mapped = RwLock::new(mapping);

        Ok(FileIo {
            cfg,
            file,
            file_len,
            write_counter: AtomicU32::new(0),
            mapped,
            #[cfg(feature = "cipher")]
            cipher,
        })
    }

    #[cfg(feature = "cipher")]
    fn password_aead(phrase: &str, salt: [u8; 16]) -> ChaCha20Poly1305 {
        use argon2::{
            password_hash::SaltString, ParamsBuilder, PasswordHasher, Argon2, Algorithm, Version,
        };
        use chacha20poly1305::aead::generic_array::GenericArray;

        let salt = SaltString::encode_b64(&salt).expect("length should be good");
        let mut param_builder = ParamsBuilder::new();
        param_builder.m_cost(1 << 18);
        param_builder.t_cost(3);

        let hasher = Argon2::new(
            Algorithm::Argon2id,
            Version::V0x13,
            param_builder.build().expect("params should be good"),
        );
        let hash = hasher
            .hash_password(phrase.as_bytes(), &salt)
            .unwrap_or_else(|err| {
                log::error!("cipher error: {err}");
                panic!();
            })
            .hash
            .expect("must not fail");
        if hash.len() != 32 {
            log::error!("expected 32 bytes hash, actual: {}", hash.len());
            panic!();
        }

        let key = GenericArray::from_slice(hash.as_bytes());

        ChaCha20Poly1305::new(key)
    }

    #[cfg(feature = "cipher")]
    pub fn crypt_setup(
        file: &fs::File,
        passphrase: &str,
        mut crypto_seed: [u8; 32],
    ) -> io::Result<Cipher> {
        use sha3::{
            Sha3_256, Shake256,
            digest::{Update, ExtendableOutput, XofReader},
        };
        use hkdf::Hkdf;
        use chacha20poly1305::aead::{AeadInPlace, generic_array::GenericArray};

        let mut rng = Shake256::default().chain(crypto_seed).finalize_xof();
        crypto_seed.zeroize();

        let mut full_buf = [0; CRYPTO_SIZE];
        rng.read(&mut full_buf);

        let (salt, buf) = full_buf
            .split_first_chunk_mut::<0x10>()
            .expect("cannot fail");
        let (tag, buf) = buf.split_first_chunk_mut::<0x10>().expect("cannot fail");

        let hkdf = Hkdf::<Sha3_256>::new(Some(&*salt), &*buf);
        let mut main_key = [0; 32];
        hkdf.expand(b"main_key", &mut main_key)
            .expect("cannot fail");
        let cipher = Cipher::new(GenericArray::from_slice(&main_key));
        main_key.zeroize();

        let aead = Self::password_aead(passphrase, *salt);
        *tag = aead
            .encrypt_in_place_detached(&GenericArray::default(), b"main_blob", buf)
            .expect("cannot fail")
            .into();

        utils::write_at(file, &full_buf, 0)?;

        Ok(cipher)
    }

    #[cfg(feature = "cipher")]
    pub fn crypt_open(view: &Mmap, passphrase: &str) -> io::Result<Cipher> {
        use chacha20poly1305::aead::{AeadInPlace, generic_array::GenericArray};
        use sha3::Sha3_256;
        use hkdf::Hkdf;

        let mut full_buf = [0; CRYPTO_SIZE];
        full_buf.clone_from_slice(&view[..CRYPTO_SIZE]);

        let (salt, buf) = full_buf
            .split_first_chunk_mut::<0x10>()
            .expect("cannot fail");
        let (tag, buf) = buf.split_first_chunk_mut::<0x10>().expect("cannot fail");

        let aead = Self::password_aead(passphrase, *salt);
        aead.decrypt_in_place_detached(
            &GenericArray::default(),
            b"main_blob",
            buf,
            GenericArray::from_slice(tag),
        )
        .map_err(|_| io::ErrorKind::Other)?;

        let hkdf = Hkdf::<Sha3_256>::new(Some(&*salt), &*buf);
        let mut main_key = [0; 32];
        hkdf.expand(b"main_key", &mut main_key)
            .expect("cannot fail");
        let cipher = Cipher::new(GenericArray::from_slice(&main_key));
        main_key.zeroize();
        buf.zeroize();

        Ok(cipher)
    }

    #[cfg(feature = "cipher")]
    pub fn crypt_shred(&self, mut crypto_seed: [u8; 32]) -> io::Result<()> {
        use sha3::{
            Shake256,
            digest::{Update, XofReader, ExtendableOutput},
        };

        let mut rng = Shake256::default().chain(crypto_seed).finalize_xof();
        crypto_seed.zeroize();
        let mut buf = [0; CRYPTO_SIZE];
        rng.read(&mut buf);
        utils::write_at(&self.file, &buf, 0)
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
                    utils::write_at(&self.file, &data, offset).unwrap()
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
        match () {
            #[cfg(feature = "cipher")]
            () => {
                let offset = offset + CRYPTO_SIZE as u64;
                let mut buf = [0; PAGE_SIZE as usize];
                buf[..bytes.len()].clone_from_slice(bytes);
                self.cipher.encrypt(&mut buf, &n.to_le_bytes());
                utils::write_at(&self.file, &buf, offset)
            }
            #[cfg(not(feature = "cipher"))]
            () => utils::write_at(&self.file, bytes, offset),
        }
    }
}

impl AbstractIo for FileIo {
    type Viewer<'a> = PageView<'a>;

    fn read(&self) -> Self::Viewer<'_> {
        PageView(
            self.mapped.read().expect("poisoned"),
            #[cfg(feature = "cipher")]
            &self.cipher,
        )
    }

    fn write<T>(&self, ptr: impl Into<Option<PagePtr<T>>>, page: &T) -> io::Result<()>
    where
        T: PlainData,
    {
        self._write(ptr.into().map_or(0, PagePtr::raw_number), page.as_bytes())
    }

    fn write_bytes(&self, ptr: impl Into<Option<PagePtr<()>>>, bytes: &[u8]) -> io::Result<()> {
        self._write(ptr.into().map_or(0, PagePtr::raw_number), bytes)
    }
}

pub struct PageView<'a>(
    RwLockReadGuard<'a, Mmap>,
    #[cfg(feature = "cipher")] &'a Cipher,
);

pub struct PageViewOffset<'a, T> {
    #[cfg(not(feature = "cipher"))]
    view: &'a PageView<'a>,
    #[cfg(not(feature = "cipher"))]
    offset: usize,
    #[cfg(feature = "cipher")]
    page: [u8; PAGE_SIZE as usize],
    phantom_data: PhantomData<(&'a (), T)>,
}

impl<T> Deref for PageViewOffset<'_, T>
where
    T: PlainData,
{
    type Target = T;

    fn deref(&self) -> &Self::Target {
        match () {
            #[cfg(feature = "cipher")]
            () => T::as_this(&self.page),
            #[cfg(not(feature = "cipher"))]
            () => T::as_this(&self.view.0[self.offset..]),
        }
    }
}

impl AbstractViewer for PageView<'_> {
    type Page<'a, T>
        = PageViewOffset<'a, T>
    where
        Self: 'a,
        T: PlainData;

    fn page<T>(&self, ptr: impl Into<Option<PagePtr<T>>>) -> Self::Page<'_, T>
    where
        T: PlainData,
    {
        let n = ptr.into().map_or(0, PagePtr::raw_number);
        let offset = (u64::from(n) * PAGE_SIZE) as usize;

        PageViewOffset {
            #[cfg(not(feature = "cipher"))]
            view: self,
            #[cfg(not(feature = "cipher"))]
            offset,
            #[cfg(feature = "cipher")]
            page: {
                let offset = offset + CRYPTO_SIZE;
                let mut buf = [0; PAGE_SIZE as usize];
                buf.clone_from_slice(&self.0[offset..][..PAGE_SIZE as usize]);
                self.1.decrypt(&mut buf, &n.to_le_bytes());
                buf
            },
            phantom_data: PhantomData,
        }
    }
}
