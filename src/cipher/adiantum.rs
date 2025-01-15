use std::{fs, io};

use aligned_vec::{avec, AVec, ConstAlign};

use {
    adiantum::cipher::{zeroize::Zeroize, KeyInit},
    aes::Aes256,
    chacha20::XChaCha12,
    chacha20poly1305::ChaCha20Poly1305,
    thiserror::Error,
};

use super::utils;

pub struct Cipher(adiantum::Cipher<XChaCha12, Aes256>);

pub enum Params<'a> {
    Create { secret: Secret<'a>, seed: &'a [u8] },
    Open { secret: Secret<'a> },
}

impl Params<'_> {
    #[cfg(test)]
    pub fn new_mock(create: bool) -> Self {
        if create {
            Self::Create {
                secret: Secret::Pw {
                    pw: "qwerty",
                    time: 1,
                    memory: 0x1000,
                },
                seed: [1; 32].as_slice(),
            }
        } else {
            Self::Open {
                secret: Secret::Pw {
                    pw: "qwerty",
                    time: 1,
                    memory: 0x1000,
                },
            }
        }
    }

    pub fn create(&self) -> bool {
        matches!(self, &Self::Create { .. })
    }
}

pub enum Secret<'a> {
    Pw { pw: &'a str, time: u32, memory: u32 },
    Key(&'a [u8; 32]),
}

#[derive(Debug, Error)]
pub enum CipherError {
    #[error("io: {0}")]
    Io(#[from] io::Error),
    #[error("bad password")]
    BadPassword,
    #[error("wrong secret")]
    WrongSecret,
    #[error("seed is too short")]
    BadSeed,
    #[error("invalid argon2 complexity")]
    InvalidComplexity,
    #[error("key blob is too short")]
    BadKeyBlob,
}

pub const CRYPTO_SIZE: usize = 1 << 20;

fn password_aead(secret: Secret<'_>, salt: [u8; 16]) -> Result<ChaCha20Poly1305, CipherError> {
    use argon2::{password_hash::SaltString, ParamsBuilder, PasswordHasher, Argon2, Algorithm, Version};
    use chacha20poly1305::aead::generic_array::GenericArray;

    let hash;
    let key = match secret {
        Secret::Pw { pw, time, memory } => {
            let salt = SaltString::encode_b64(&salt).expect("length should be good");
            let mut param_builder = ParamsBuilder::new();
            param_builder.m_cost(memory);
            param_builder.t_cost(time);

            let hasher = Argon2::new(
                Algorithm::Argon2id,
                Version::V0x13,
                param_builder
                    .build()
                    .map_err(|_| CipherError::InvalidComplexity)?,
            );
            hash = hasher
                .hash_password(pw.as_bytes(), &salt)
                .map_err(|_| CipherError::BadPassword)?
                .hash
                .ok_or(CipherError::BadPassword)?;
            if hash.len() != 32 {
                return Err(CipherError::BadPassword);
            }
            hash.as_bytes()
        }
        Secret::Key(key) => key,
    };
    let key = GenericArray::from_slice(key);

    Ok(ChaCha20Poly1305::new(key))
}

impl Cipher {
    pub fn new(file: &fs::File, params: Params<'_>) -> Result<Self, CipherError> {
        match params {
            Params::Create { secret, seed } => {
                let (cipher, blob) = Self::setup(secret, seed)?;
                utils::write_at(file, &blob, 0)?;
                Ok(cipher)
            }
            Params::Open { secret } => {
                let mut blob = vec![0; CRYPTO_SIZE];
                utils::read_at(file, &mut blob, 0)?;
                Self::open(blob, secret)
            }
        }
    }

    fn setup(
        secret: Secret<'_>,
        seed: &[u8],
    ) -> Result<(Self, AVec<u8, ConstAlign<4096>>), CipherError> {
        use sha3::{
            Sha3_256, Shake256,
            digest::{Update, ExtendableOutput, XofReader},
        };
        use hkdf::Hkdf;
        use chacha20poly1305::aead::{AeadInPlace, generic_array::GenericArray};

        if seed.len() < 32 {
            return Err(CipherError::BadSeed);
        }

        let mut rng = Shake256::default().chain(seed).finalize_xof();
        let mut full_buf = avec![[4096]| 0; CRYPTO_SIZE];
        rng.read(&mut full_buf);

        let (salt, buf) = full_buf
            .split_first_chunk_mut::<0x10>()
            .expect("cannot fail");
        let (tag, buf) = buf.split_first_chunk_mut::<0x10>().expect("cannot fail");

        let hkdf = Hkdf::<Sha3_256>::new(Some(&*salt), &*buf);
        let mut main_key = [0; 32];
        hkdf.expand(b"main_key", &mut main_key)
            .expect("cannot fail");
        let cipher = Self(adiantum::Cipher::new(GenericArray::from_slice(&main_key)));
        main_key.zeroize();

        *tag = password_aead(secret, *salt)?
            .encrypt_in_place_detached(&GenericArray::default(), b"main_blob", buf)
            .expect("cannot fail")
            .into();

        Ok((cipher, full_buf))
    }

    fn open(mut full_buf: Vec<u8>, secret: Secret<'_>) -> Result<Cipher, CipherError> {
        use chacha20poly1305::aead::{AeadInPlace, generic_array::GenericArray};
        use sha3::Sha3_256;
        use hkdf::Hkdf;

        let (salt, buf) = full_buf
            .split_first_chunk_mut::<0x10>()
            .expect("cannot fail");
        let (tag, buf) = buf.split_first_chunk_mut::<0x10>().expect("cannot fail");

        password_aead(secret, *salt)?
            .decrypt_in_place_detached(
                &GenericArray::default(),
                b"main_blob",
                buf,
                GenericArray::from_slice(&*tag),
            )
            .map_err(|_| CipherError::WrongSecret)?;

        let hkdf = Hkdf::<Sha3_256>::new(Some(&*salt), &*buf);
        let mut main_key = [0; 32];
        hkdf.expand(b"main_key", &mut main_key)
            .expect("cannot fail");
        let cipher = Self(adiantum::Cipher::new(GenericArray::from_slice(&main_key)));
        main_key.zeroize();
        buf.zeroize();

        Ok(cipher)
    }

    pub fn decrypt(&self, page: &mut [u8], n: u32) {
        self.0.decrypt(page, &n.to_le_bytes());
    }

    pub fn encrypt(&self, page: &mut [u8], n: u32) {
        self.0.encrypt(page, &n.to_le_bytes());
    }
}

pub fn shred(seed: &[u8]) -> Result<Vec<u8>, CipherError> {
    use sha3::{
        Shake256,
        digest::{Update, XofReader, ExtendableOutput},
    };

    if seed.len() < 32 {
        return Err(CipherError::BadSeed);
    }

    let mut rng = Shake256::default().chain(seed).finalize_xof();
    let mut full_buf = vec![0; CRYPTO_SIZE];
    rng.read(&mut full_buf);

    Ok(full_buf)
}
