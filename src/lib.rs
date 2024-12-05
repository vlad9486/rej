//! Database
//! Maximal size: (2 ^ 44) B = 16 TiB
//! Maximal key size: (2 ^ 10) B = 1 kiB
//! Maximal number of records: 2 ^ 30

mod utils;
mod file;
mod page;
mod wal;
mod node;
mod btree;
mod value;
mod db;

pub use self::{
    file::IoOptions,
    wal::{DbStats, WalError},
    db::{Db, DbError, DbValue, DbIterator},
};
