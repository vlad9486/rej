use std::{fs, path::PathBuf};

use rej::Db;

fn main() {
    let env = env_logger::Env::new().filter_or("RUST_LOG", "debug");
    env_logger::try_init_from_env(env).unwrap_or_default();

    let path = PathBuf::from("target/db_big");

    fs::remove_file(&path).unwrap_or_default();
    let cfg = Default::default();
    let db = Db::new(&path, cfg).unwrap();
    drop(db);
    let db = Db::new(&path, cfg).unwrap();

    let mut indexes = (0..1000).collect::<Vec<u16>>();

    {
        use rand::{rngs::StdRng, SeedableRng, seq::SliceRandom};
        let mut rng = StdRng::seed_from_u64(0x123);

        indexes.shuffle(&mut rng);
    }

    for i in &indexes {
        let key = format!("key {i:03}");
        log::info!("insert {key}");
        let value = db.insert(0, key.as_bytes()).unwrap();
        db.write(&value, 0, &i.to_le_bytes()).unwrap();
    }

    for i in indexes {
        let key = format!("key {i:03}");
        let value = db
            .retrieve(0, key.as_bytes())
            .unwrap_or_else(|| panic!("{key}"));
        assert_eq!(db.read_to_vec(&value), &i.to_le_bytes());
    }

    db.remove(0, b"key 030").unwrap().unwrap();
    assert!(db.retrieve(0, b"key 030").is_none());

    // let mut it = db.iterator(0, None, true);
    // while let Some((k, _)) = db.next(&mut it) {
    //     log::info!("{}", std::str::from_utf8(&k).unwrap());
    // }
}
