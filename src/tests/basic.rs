use std::iter;

use rand::{seq::SliceRandom, Rng};

use crate::NodePage;

use super::with_db;

#[test]
fn scan() {
    with_db::<_, _, NodePage>(0x123, |db, rng| {
        let mut rand_key = |i: u16| {
            let mut v = rng.gen::<[u8; 16]>();
            v[..2].clone_from_slice(&i.to_be_bytes());
            v
        };
        let rand_key_list = (0..1000).map(|i| rand_key(i * 4)).collect::<Vec<_>>();
        let mut keys = Vec::with_capacity(300);
        keys.extend(rand_key_list);
        keys.shuffle(rng);
        for key in &keys {
            db.entry(key)
                .vacant()
                .unwrap()
                .insert()
                .unwrap()
                .write_at(0, key)
                .unwrap();
        }

        let start = 10u16;
        let mut it = db.entry(&(start * 4).to_be_bytes()).into_db_iter();
        let mut expected = start..1000;
        while let Some((key, value)) = db.next(&mut it) {
            log::debug!("{}", hex::encode(&key));
            let expected = expected.next().unwrap();
            let value = value.unwrap().read_to_vec(0, 16).unwrap();
            assert_eq!(key, value);
            assert_eq!(key[0..2], (expected * 4).to_be_bytes());
        }
    })
}

#[test]
fn keys() {
    with_db::<_, _, NodePage>(0x123, |db, rng| {
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
            db.entry(key)
                .vacant()
                .unwrap_or_else(|| panic!("{}", printer(key)))
                .insert()
                .unwrap();
        }

        keys.shuffle(rng);
        for key in &keys {
            db.entry(key)
                .occupied()
                .unwrap_or_else(|| panic!("{}", printer(key)));
        }

        keys.shuffle(rng);
        for key in &keys {
            log::debug!("will {}", printer(key));
            db.entry(key)
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
    with_db::<_, _, NodePage>(0x123, |db, _rng| {
        for i in 0..8 {
            db.entry(&[i]).vacant().unwrap().insert().unwrap();
        }
        db.print(|key| key[0]);
        db.entry(&[3]).occupied().unwrap().remove().unwrap();
        db.print(|key| key[0]);
    })
}

#[test]
fn remove_merge_with_left() {
    with_db::<_, _, NodePage>(0x123, |db, _rng| {
        for i in 0..8 {
            db.entry(&[i]).vacant().unwrap().insert().unwrap();
        }
        db.print(|key| key[0]);
        db.entry(&[5]).occupied().unwrap().remove().unwrap();
        db.print(|key| key[0]);
    })
}

#[test]
fn remove_borrow() {
    with_db::<_, _, NodePage>(0x123, |db, _rng| {
        for i in 0..9 {
            db.entry(&[i]).vacant().unwrap().insert().unwrap();
        }
        db.entry(&[3]).occupied().unwrap().remove().unwrap();
        db.print(|key| key[0]);
        db.entry(&[3]).vacant().unwrap().insert().unwrap();
        db.print(|key| key[0]);
        db.entry(&[5]).occupied().unwrap().remove().unwrap();
        db.print(|key| key[0]);
    })
}

#[test]
fn remove_all() {
    with_db::<_, _, NodePage>(0x123, |db, rng| {
        let mut keys = (0..17).map(|i| vec![i]).collect::<Vec<_>>();
        for key in &keys {
            db.entry(key)
                .vacant()
                .unwrap()
                .insert()
                .unwrap()
                .write_at(0, key)
                .unwrap();
        }
        let printer = |key: &[u8]| key[0];
        db.print(printer);

        keys.shuffle(rng);
        for key in &keys {
            log::debug!("{}", printer(key));
            let vec = db
                .entry(key)
                .occupied()
                .unwrap_or_else(|| {
                    db.print(printer);
                    panic!();
                })
                .remove()
                .unwrap()
                .read_to_vec(0, 1)
                .unwrap();
            assert_eq!(vec, key.clone());
            db.print(printer);
        }
    })
}
