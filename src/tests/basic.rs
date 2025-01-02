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
            db.entry(0, key)
                .vacant()
                .unwrap_or_else(|| panic!("{}", printer(key)))
                .insert()
                .unwrap();
        }

        keys.shuffle(rng);
        for key in &keys {
            db.entry(0, key)
                .occupied()
                .unwrap_or_else(|| panic!("{}", printer(key)));
        }

        keys.shuffle(rng);
        for key in &keys {
            log::debug!("will {}", printer(key));
            db.entry(0, key)
                .occupied()
                .unwrap_or_else(|| panic!("{}", printer(key)))
                .remove()
                .unwrap();
        }
        db.print(printer);
    })
}

#[test]
fn remove_merge_with_right() {
    with_db(0x123, |db, _rng| {
        for i in 0..8 {
            db.entry(5, &[i]).vacant().unwrap().insert().unwrap();
        }
        db.print(|key| key[0]);
        db.entry(5, &[3]).occupied().unwrap().remove().unwrap();
        db.print(|key| key[0]);
    })
}

#[test]
fn remove_merge_with_left() {
    with_db(0x123, |db, _rng| {
        for i in 0..8 {
            db.entry(5, &[i]).vacant().unwrap().insert().unwrap();
        }
        db.print(|key| key[0]);
        db.entry(5, &[5]).occupied().unwrap().remove().unwrap();
        db.print(|key| key[0]);
    })
}

#[test]
fn remove_borrow() {
    with_db(0x123, |db, _rng| {
        for i in 0..9 {
            db.entry(5, &[i]).vacant().unwrap().insert().unwrap();
        }
        db.entry(5, &[3]).occupied().unwrap().remove().unwrap();
        db.print(|key| key[0]);
        db.entry(5, &[3]).vacant().unwrap().insert().unwrap();
        db.print(|key| key[0]);
        db.entry(5, &[5]).occupied().unwrap().remove().unwrap();
        db.print(|key| key[0]);
    })
}

#[test]
fn remove_all() {
    with_db(0x123, |db, rng| {
        let mut keys = (0..17).map(|i| vec![i]).collect::<Vec<_>>();
        for key in &keys {
            let value = db.entry(5, key).vacant().unwrap().insert().unwrap();
            db.rewrite(value, key).unwrap()
        }
        let printer = |key: &[u8]| key[0];
        db.print(printer);

        keys.shuffle(rng);
        for key in &keys {
            log::debug!("{}", printer(key));
            let value = db
                .entry(5, key)
                .occupied()
                .unwrap_or_else(|| {
                    db.print(printer);
                    panic!();
                })
                .remove()
                .unwrap();
            assert_eq!(value.read_to_vec(), key.clone());
            db.print(printer);
        }
    })
}
