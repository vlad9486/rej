#[cfg(feature = "cipher")]
use super::utils;

#[cfg(feature = "cipher")]
mod adiantum;
#[cfg(feature = "cipher")]
pub use self::adiantum::{Secret, Params, Cipher, CipherError, CRYPTO_SIZE, shred};

#[cfg(not(feature = "cipher"))]
mod plain;
#[cfg(not(feature = "cipher"))]
pub use self::plain::{Params, Cipher, CipherError, CRYPTO_SIZE, shred};
