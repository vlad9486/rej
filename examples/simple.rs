use std::{fs, str};

use rej::Db;

fn main() {
    let env = env_logger::Env::new().filter_or("RUST_LOG", "debug");
    env_logger::try_init_from_env(env).unwrap_or_default();

    fs::remove_file("target/db").unwrap_or_default();
    let cfg = Default::default();
    let db = Db::new("target/db", cfg).unwrap();
    drop(db);
    let db = Db::new("target/db", cfg).unwrap();

    let data = |s| (s..128u8).collect::<Vec<u8>>();

    log::info!("{:?}", db.stats());
    let v = db.insert(b"some key 1, long").unwrap();
    db.write(&v, 0, &data(10)).unwrap();

    log::info!("{:?}", db.stats());
    let v = db.insert(b"some key 6, too                long").unwrap();
    db.write(&v, 0, &data(60)).unwrap();

    log::info!("{:?}", db.stats());
    let v = db.insert(b"some key 3").unwrap();
    db.write(&v, 0, &data(30)).unwrap();

    log::info!("{:?}", db.stats());

    drop(db);
    let db = Db::new("target/db", cfg).unwrap();

    let v = db.retrieve(b"some key 6, too                long").unwrap();
    assert_eq!(db.read_to_vec(&v), data(60));

    let mut it = db.iterator(None, true);
    while let Some((k, _)) = db.next(&mut it) {
        log::info!("{}", str::from_utf8(&k).unwrap());
    }
    let mut it = db.iterator(None, false);
    while let Some((k, _)) = db.next(&mut it) {
        log::info!("{}", str::from_utf8(&k).unwrap());
    }
}
