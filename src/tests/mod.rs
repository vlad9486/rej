#[cfg(not(feature = "small"))]
mod recovery;
#[cfg(feature = "small")]
mod basic;
#[cfg(not(feature = "small"))]
mod basic_big;

use tempdir::TempDir;
use rand::{rngs::StdRng, SeedableRng};

use crate::{Db, IoOptions};

pub fn with_db<F, T>(seed: u64, f: F) -> T
where
    F: FnOnce(Db, &mut StdRng) -> T,
{
    let mut rng = StdRng::seed_from_u64(seed);

    let dir = TempDir::new_in("target/tmp", "rej").unwrap();
    let path = dir.path().join("test-insert");

    let db = Db::new(&path, IoOptions::default()).unwrap();
    drop(db);

    let db = Db::new(&path, IoOptions::default()).unwrap();
    f(db, &mut rng)
}
