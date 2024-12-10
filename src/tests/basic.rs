use std::iter;

use tempdir::TempDir;
use rand::{rngs::StdRng, SeedableRng};

use crate::{Db, IoOptions};

fn with_db<F, T>(seed: u64, f: F) -> T
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

#[test]
fn keys() {
    with_db(0x123, |db, rng| {
        use rand::seq::SliceRandom;

        let mut keys = (0..100)
            .map(|i| {
                [0, 1]
                    .into_iter()
                    .map(move |e| iter::repeat(e).take(i * 8).collect::<Vec<u8>>())
            })
            .flatten()
            .collect::<Vec<_>>();
        let printer = |x: &[u8]| format!("{}_{}", x.len() / 8, x.get(0).copied().unwrap_or(3));

        keys.shuffle(rng);
        for key in &keys {
            println!("{}", printer(key));
            db.insert(0, key).unwrap();
            db.print(printer);
        }

        keys.shuffle(rng);
        for key in &keys {
            db.retrieve(0, key)
                .unwrap_or_else(|| panic!("{}", printer(key)));
        }
    })
}

#[test]
fn remove_merge_with_right() {
    with_db(0x123, |db, _rng| {
        for i in 0..9 {
            db.insert(5, &[i]).unwrap();
        }
        db.remove(5, &[3]).unwrap();
        db.print(|key| key[0]);
        db.remove(5, &[4]).unwrap();
        db.print(|key| key[0]);
    })
}

#[test]
fn remove_borrow() {
    with_db(0x123, |db, _rng| {
        for i in 0..9 {
            db.insert(5, &[i]).unwrap();
        }
        db.remove(5, &[3]).unwrap();
        db.insert(5, &[3]).unwrap();
        db.remove(5, &[5]).unwrap();
    })
}
