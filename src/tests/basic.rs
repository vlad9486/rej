use std::iter;

use rand::seq::SliceRandom;

use super::with_db;

#[test]
fn keys() {
    with_db(0x123, |db, rng| {
        let mut keys = (1..100)
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
            let value = db.allocate().unwrap();
            db.insert(&value, 0, key).unwrap();
        }

        keys.shuffle(rng);
        for key in &keys {
            db.retrieve(0, key)
                .unwrap_or_else(|| panic!("{}", printer(key)));
        }

        keys.shuffle(rng);
        for key in &keys {
            log::debug!("will {}", printer(key));
            let value = db.remove(0, key).unwrap().unwrap();

            db.deallocate(value).unwrap();
        }
        db.print(printer);
    })
}

#[test]
fn remove_merge_with_right() {
    with_db(0x123, |db, _rng| {
        for i in 0..8 {
            let value = db.allocate().unwrap();
            db.insert(&value, 5, &[i]).unwrap();
        }
        db.print(|key| key[0]);
        db.remove(5, &[3]).unwrap();
        db.print(|key| key[0]);
    })
}

#[test]
fn remove_merge_with_left() {
    with_db(0x123, |db, _rng| {
        for i in 0..8 {
            let value = db.allocate().unwrap();
            db.insert(&value, 5, &[i]).unwrap();
        }
        db.print(|key| key[0]);
        db.remove(5, &[5]).unwrap();
        db.print(|key| key[0]);
    })
}

#[test]
fn remove_borrow() {
    with_db(0x123, |db, _rng| {
        for i in 0..9 {
            let value = db.allocate().unwrap();
            db.insert(&value, 5, &[i]).unwrap();
        }
        let value = db.remove(5, &[3]).unwrap().unwrap();
        db.print(|key| key[0]);
        db.insert(&value, 5, &[3]).unwrap();
        db.print(|key| key[0]);
        db.remove(5, &[5]).unwrap();
        db.print(|key| key[0]);
    })
}

#[test]
fn remove_all() {
    with_db(0x123, |db, rng| {
        let mut keys = (0..17).map(|i| vec![i]).collect::<Vec<_>>();
        for key in &keys {
            let value = db.allocate().unwrap();
            db.insert(&value, 0, key).unwrap();
            db.rewrite(&value, key).unwrap();
        }
        let printer = |key: &[u8]| key[0];
        db.print(printer);

        keys.shuffle(rng);
        for key in &keys {
            println!("{}", printer(key));
            let value = db.remove(0, key).unwrap().unwrap_or_else(|| {
                db.print(printer);
                panic!();
            });
            assert_eq!(db.read_to_vec(&value), key.clone());
            db.print(printer);
        }
    })
}
