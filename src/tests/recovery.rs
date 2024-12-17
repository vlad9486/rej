use std::{fs, panic, path::Path};

use tempdir::TempDir;

use crate::{Db, DbError, DbIterator, DbStats, IoOptions, wal::FreelistCache, Params};

fn populate(db: Db) -> Result<DbStats, DbError> {
    let data = |s| (s..128u8).collect::<Vec<u8>>();
    db.entry(0, b"some key 1, long")
        .vacant()
        .unwrap()
        .insert()?
        .rewrite(&data(10))?;
    db.entry(0, b"some key 6, too                long")
        .vacant()
        .unwrap()
        .insert()?
        .rewrite(&data(20))?;
    db.entry(0, b"some key 3")
        .vacant()
        .unwrap()
        .insert()?
        .rewrite(&data(30))?;

    Ok(db.stats())
}

// TODO: proper check
fn check(db: Db) -> bool {
    struct It {
        inner: DbIterator,
        db: Db,
    }

    impl Iterator for It {
        type Item = (Vec<u8>, Vec<u8>);

        fn next(&mut self) -> Option<Self::Item> {
            self.db.next(&mut self.inner)
        }
    }

    let mut it = It {
        inner: db.entry(0, b"").into_db_iter(),
        db,
    };
    let cnt = (&mut it).count();
    let stats = it.db.stats();

    stats.cached == FreelistCache::SIZE
        && (false
            || (cnt == 0 && stats.used <= 2)
            || (cnt == 1 && stats.used <= 4)
            || (cnt == 2 && stats.used <= 7)
            || (cnt == 3 && stats.used <= 8))
}

fn recovery_test<const MESS_PAGE: bool>() {
    let env = env_logger::Env::new().filter_or("RUST_LOG", "warn");
    env_logger::try_init_from_env(env).unwrap_or_default();

    let dir = TempDir::new_in("target/tmp", "rej").unwrap();
    let path = dir.path().join("test-recovery");

    let db = Db::new(&path, IoOptions::default(), Params::new_mock(true)).unwrap();
    drop(db);

    let db = Db::new(&path, IoOptions::default(), Params::new_mock(false)).unwrap();
    let stats = populate(db).unwrap();

    for i in 0..(stats.writes - 1) {
        crash_test(&path, IoOptions::simulator(i, MESS_PAGE));
    }
}

fn crash_test(path: &Path, cfg: IoOptions) {
    fs::remove_file(path).unwrap_or_default();
    let db = Db::new(path, IoOptions::default(), Params::new_mock(true)).unwrap();
    drop(db);

    let err = panic::catch_unwind(move || {
        let db = Db::new(path, cfg, Params::new_mock(false)).unwrap();
        populate(db).unwrap();
    })
    .unwrap_err()
    .downcast::<&str>()
    .unwrap();
    assert_eq!(*err, "intentional panic for test");

    let db = Db::new(path, IoOptions::default(), Params::new_mock(false)).unwrap();
    assert!(check(db));
}

#[test]
fn recovery() {
    recovery_test::<false>();
}

#[test]
#[ignore = "TODO: Protect metadata page against hardware failure."]
fn recovery_messed_page() {
    recovery_test::<true>();
}
