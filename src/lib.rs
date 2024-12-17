//! Database
//! Maximal size: (2 ^ 44) B = 16 TiB
//! Maximal key size: (2 ^ 10) B = 1 kiB
//! Maximal number of records: 2 ^ 30

mod utils;
mod page;
mod runtime;

#[cfg(feature = "cipher")]
mod cipher;
#[cfg(not(feature = "cipher"))]
mod cipher_mock;
mod file;
mod wal;

mod value;
mod node;
mod btree;
mod db;

#[cfg(test)]
mod tests;

#[cfg(feature = "cipher")]
pub use self::cipher::{Secret, Params, CipherError};

#[cfg(not(feature = "cipher"))]
pub use self::cipher_mock::Params;

pub use self::{
    file::IoOptions,
    wal::{DbStats, WalError},
    db::{Db, DbError, DbIterator, Value, Entry, Occupied, Vacant, ext},
};
