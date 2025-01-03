use std::{fs, io, marker::PhantomData, ops::Deref};

use thiserror::Error;

use super::{page::PAGE_SIZE, runtime::PlainData};

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
    pub fn new(file: &fs::File, params: Params) -> Result<Self, CipherError> {
        let _ = (file, params);
        Ok(Self)
    }
}

pub fn shred(seed: &[u8]) -> Result<Vec<u8>, CipherError> {
    let _ = seed;
    Ok(vec![])
}

pub struct DecryptedPage<'a, T> {
    page: Box<[u8; PAGE_SIZE as usize]>,
    phantom_data: PhantomData<&'a T>,
}

pub struct EncryptedPage<'a> {
    page: &'a [u8],
}

impl<T> DecryptedPage<'_, T> {
    pub fn new(page: Box<[u8; PAGE_SIZE as usize]>, cipher: &Cipher, n: u32) -> Self {
        let &Cipher = cipher;
        let _ = n;
        DecryptedPage {
            page,
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
        T::as_this(self.page.as_ref())
    }
}

impl Deref for EncryptedPage<'_> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.page
    }
}
