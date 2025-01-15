use std::{fs, io};

use thiserror::Error;

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

    pub fn decrypt(&self, page: &mut [u8], n: u32) {
        let _ = (page, n);
    }

    pub fn encrypt(&self, page: &mut [u8], n: u32) {
        let _ = (page, n);
    }
}

pub fn shred(seed: &[u8]) -> Result<Vec<u8>, CipherError> {
    let _ = seed;
    Ok(vec![])
}
