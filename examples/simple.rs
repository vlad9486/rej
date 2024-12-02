use std::fs;

use rej::Db;

fn main() {
    let env = env_logger::Env::new().filter_or("RUST_LOG", "debug");
    env_logger::try_init_from_env(env).unwrap_or_default();

    fs::remove_file("target/db").unwrap_or_default();
    let cfg = Default::default();
    let db = Db::new("target/db", cfg).unwrap();
    log::info!("{:?}", db.stats());

    let data = |s| (s..128u8).collect::<Vec<u8>>();

    let v = db.insert(b"some key 1\0").unwrap();
    db.write(&v, 0, &data(10)).unwrap();

    let v = db.insert(b"some key 6\0").unwrap();
    db.write(&v, 0, &data(60)).unwrap();

    let v = db.insert(b"some key 3\0").unwrap();
    db.write(&v, 0, &data(30)).unwrap();

    log::info!("{:?}", db.stats());

    drop(db);
    let db = Db::new("target/db", cfg).unwrap();

    let v = db.retrieve(b"some key 6\0").unwrap();
    assert_eq!(db.read_to_vec(&v), data(60));
}
