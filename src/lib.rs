//! Database
//! Maximal size: (2 ^ 44) B = 16 TiB
//! Maximal key size: (2 ^ 10) B = 1 kiB
//! Maximal number of records: 2 ^ 30

mod page;

mod file;

mod wal;
mod db;

mod btree;

mod utils;

pub use self::{
    file::IoOptions,
    wal::WalError,
    db::{Db, DbStats, DbError, DbValue},
};
