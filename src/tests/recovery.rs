use std::{fs, panic, path::Path};

use tempdir::TempDir;

use crate::{Db, DbError, DbIterator, DbStats, DbValue, IoOptions};

fn populate(db: Db) -> Result<DbStats, DbError> {
    let data = |s| (s..128u8).collect::<Vec<u8>>();
    let v = db.insert(0, b"some key 1, long")?;
    db.write(&v, 0, &data(10))?;
    let v = db.insert(0, b"some key 6, too                long")?;
    db.write(&v, 0, &data(60))?;
    let v = db.insert(0, b"some key 3")?;
    db.write(&v, 0, &data(30))?;

    Ok(db.stats())
}

fn check(db: Db) -> bool {
    struct It {
        inner: DbIterator,
        db: Db,
    }

    impl Iterator for It {
        type Item = (Vec<u8>, DbValue);

        fn next(&mut self) -> Option<Self::Item> {
            self.db.next(&mut self.inner)
        }
    }

    let mut it = It {
        inner: db.iterator(0, None, true),
        db,
    };
    let cnt = (&mut it).count();
    let stats = it.db.stats();

    stats.cached == 399
        && (false
            || (cnt == 0 && stats.used == 1)
            || (cnt == 1 && stats.used == 3)
            || (cnt == 2 && stats.used == 6)
            || (cnt == 3 && stats.used == 7))
}

fn recovery_test<const MESS_PAGE: bool>() {
    let env = env_logger::Env::new().filter_or("RUST_LOG", "warn");
    env_logger::try_init_from_env(env).unwrap_or_default();

    let dir = TempDir::new_in("target/tmp", "rej").unwrap();
    let path = dir.path().join("test-recovery");

    let db = Db::new(&path, IoOptions::default()).unwrap();
    drop(db);

    let db = Db::new(&path, IoOptions::default()).unwrap();
    let stats = populate(db).unwrap();

    for i in 0..(stats.writes - 1) {
        crash_test(&path, IoOptions::simulator(i, MESS_PAGE));
    }
}

fn crash_test(path: &Path, cfg: IoOptions) {
    let db = Db::new(path, IoOptions::default()).unwrap();
    drop(db);

    let err = panic::catch_unwind(move || {
        let db = Db::new(path, cfg).unwrap();
        populate(db).unwrap();
    })
    .unwrap_err()
    .downcast::<&str>()
    .unwrap();
    assert_eq!(*err, "intentional panic for test");

    let db = Db::new(path, IoOptions::default()).unwrap();
    assert!(check(db));
    fs::remove_file(path).unwrap();
}

#[test]
fn recovery() {
    recovery_test::<false>();
}

#[test]
fn recovery_messed_page() {
    recovery_test::<true>();
}
