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

    db.insert(b"some key 1\0")
        .unwrap()
        .write(&db, &data(10))
        .unwrap();

    db.insert(b"some key 6\0")
        .unwrap()
        .write(&db, &data(60))
        .unwrap();

    db.insert(b"some key 3\0")
        .unwrap()
        .write(&db, &data(30))
        .unwrap();

    log::info!("{:?}", db.stats());

    drop(db);
    let db = Db::new("target/db", cfg).unwrap();

    let actual = db.retrieve(b"some key 6\0").unwrap().read_to_vec(&db);
    assert_eq!(actual, data(60));
}
