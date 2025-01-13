use std::{fs, panic, path::Path};

use tempdir::TempDir;

use crate::{Db, DbError, DbStats, IoOptions, Params};

fn populate(db: Db) -> Result<DbStats, DbError> {
    let data = |s| {
        128u64
            .to_le_bytes()
            .into_iter()
            .chain(s..128u8)
            .collect::<Vec<u8>>()
    };
    db.entry(0, b"some key 1, long")
        .vacant()
        .unwrap()
        .insert()?
        .write_at(0, &data(10))?;
    db.entry(0, b"some key 6, too                long")
        .vacant()
        .unwrap()
        .insert()?
        .write_at(0, &data(20))?;
    db.entry(0, b"some key 3")
        .vacant()
        .unwrap()
        .insert()?
        .write_at(0, &data(30))?;

    Ok(db.stats())
}

// TODO: proper check
fn check(db: Db) -> bool {
    let stats = db.stats();
    db.print(|k| std::str::from_utf8(k).unwrap().to_owned());
    let mut it = db.entry(0, b"").into_db_iter();
    let mut cnt = 0;
    while db.next(&mut it).is_some() {
        cnt += 1;
    }
    log::debug!("{cnt}, {stats:?}");

    false
        || (cnt == 0 && stats.used <= 1)
        || (cnt == 1 && stats.used <= 3)
        || (cnt == 2 && stats.used <= 6)
        || (cnt == 3 && stats.used <= 7)
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
