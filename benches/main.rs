use criterion::{criterion_group, criterion_main, Criterion, black_box};

criterion_group!(benches, insert);
criterion_main!(benches);

use tempdir::TempDir;

use rej::{Db, Params, ext};

#[cfg(feature = "cipher")]
use rej::Secret;

fn insert(c: &mut Criterion) {
    let dir = TempDir::new_in("target/tmp", "rej").unwrap();
    let path = dir.path().join("bench-insert");

    #[cfg(feature = "cipher")]
    let seed = rand::random::<[u8; 32]>();

    #[cfg(feature = "cipher")]
    let create_params = Params::Create {
        secret: Secret::Pw {
            pw: "qwerty",
            time: 1,
            memory: 0x100,
        },
        seed: seed.as_slice(),
    };

    #[cfg(not(feature = "cipher"))]
    let create_params = Params::Create;

    let db = Db::new(&path, Default::default(), create_params).unwrap();

    const NUM: u16 = 100;
    let mut indexes = (0..NUM).collect::<Vec<_>>();

    {
        use rand::{rngs::StdRng, SeedableRng, seq::SliceRandom};
        let mut rng = StdRng::seed_from_u64(0x123);

        indexes.shuffle(&mut rng);
    }

    c.bench_function("insert", |b| {
        b.iter(|| {
            let mut key = *b"key key key asd asd asd     ";
            for i in &indexes {
                key[24..26].clone_from_slice(&i.to_le_bytes());
                ext::put(&db, 0, &key, &[0, 1]).unwrap();
            }
            for i in 0..NUM {
                key[24..26].clone_from_slice(&i.to_le_bytes());
                black_box(ext::get(&db, 0, &key).unwrap());
            }
            black_box(db.stats());
        })
    });
}
