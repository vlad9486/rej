mod page;

mod file;

mod wal;
mod db;

// temporal pub
pub mod btree;

mod utils;

// temporal API
pub use self::{page::PagePtr, file::PlainData, db::DbView};

pub use self::{
    file::IoOptions,
    wal::WalError,
    db::{Db, DbError},
};
