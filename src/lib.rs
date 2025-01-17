//! Database
//! Maximal size: (2 ^ 44) B = 16 TiB
//! Maximal key size: (2 ^ 10) B = 1 kiB
//! Maximal number of records: 2 ^ 30
//! Maximal value size: 1572864 B = 1536 kiB

mod utils;
mod page;
mod runtime;

mod cipher;
mod file;
mod wal;

mod value;
mod node;
mod btree;
mod db;

#[cfg(test)]
mod tests;

#[cfg(feature = "cipher")]
pub use self::cipher::Secret;

pub use self::{
    cipher::{Params, CipherError},
    wal::{DbStats, WalError},
    node::{NodePage, NodeCPage},
    db::{Db, DbError, DbIterator, Value, Entry, Occupied, Vacant},
};
