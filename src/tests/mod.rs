#[cfg(not(feature = "small"))]
mod recovery;
#[cfg(feature = "small")]
mod basic;
#[cfg(not(feature = "small"))]
mod basic_big;

use tempdir::TempDir;
use rand::{rngs::StdRng, SeedableRng};

use crate::{Db, Params};

pub fn with_db<F, T>(seed: u64, f: F) -> T
where
    F: FnOnce(Db, &mut StdRng) -> T,
{
    let env = env_logger::Env::new().filter_or(
        "RUST_LOG",
        "rej::btree=info,rej::node=info,rej::tests::basic=info",
    );
    env_logger::try_init_from_env(env).unwrap_or_default();

    let mut rng = StdRng::seed_from_u64(seed);

    let dir = TempDir::new_in("target/tmp", "rej").unwrap();
    let path = dir.path().join("test-insert");

    let db = Db::new(&path, Params::new_mock(true)).unwrap();
    drop(db);

    let db = Db::new(&path, Params::new_mock(false)).unwrap();
    f(db, &mut rng)
}
