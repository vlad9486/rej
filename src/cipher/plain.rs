use std::{fs, io, marker::PhantomData, ops::Deref};

use memmap2::Mmap;
use thiserror::Error;

use super::runtime::PlainData;

pub struct Cipher;

pub enum Params {
    Create,
    Open,
}

impl Params {
    #[cfg(test)]
    pub fn new_mock(create: bool) -> Self {
        if create {
            Self::Create
        } else {
            Self::Open
        }
    }

    pub fn create(&self) -> bool {
        matches!(self, &Self::Create)
    }
}

#[derive(Debug, Error)]
pub enum CipherError {
    #[error("io: {0}")]
    Io(#[from] io::Error),
}

pub const CRYPTO_SIZE: usize = 0;

impl Cipher {
    pub fn new(file: &fs::File, map: &Mmap, params: Params) -> Result<Self, CipherError> {
        let _ = (file, map, params);
        Ok(Self)
    }
}

pub fn shred(seed: &[u8]) -> Result<Vec<u8>, CipherError> {
    let _ = seed;
    Ok(vec![])
}

pub struct DecryptedPage<'a, T> {
    page: &'a [u8],
    phantom_data: PhantomData<T>,
}

pub struct EncryptedPage<'a> {
    page: &'a [u8],
}

impl<'a, T> DecryptedPage<'a, T> {
    pub fn new(slice: &'a [u8], cipher: &Cipher, n: u32) -> Self {
        let &Cipher = cipher;
        let _ = n;
        DecryptedPage {
            page: slice,
            phantom_data: PhantomData,
        }
    }
}

impl<'a> EncryptedPage<'a> {
    pub fn new(slice: &'a [u8], cipher: &Cipher, n: u32) -> Self {
        let &Cipher = cipher;
        let _ = n;
        EncryptedPage { page: slice }
    }
}

impl<T> Deref for DecryptedPage<'_, T>
where
    T: PlainData,
{
    type Target = T;

    fn deref(&self) -> &Self::Target {
        T::as_this(self.page)
    }
}

impl Deref for EncryptedPage<'_> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.page
    }
}
