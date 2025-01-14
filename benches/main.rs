use criterion::{criterion_group, criterion_main, Criterion, black_box};

criterion_group!(benches, insert);
criterion_main!(benches);

use tempdir::TempDir;

use rej::{Db, Params};

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

    let db = Db::new(&path, create_params).unwrap();

    // prepare
    let mut key = *b"preparation     preparation";
    for i in 0..=255u8 {
        key[24] = i;
        db.entry(0, &key)
            .vacant()
            .unwrap()
            .insert()
            .unwrap()
            .write_at(0, &[0, 1])
            .unwrap();
    }

    c.bench_function("insert", |b| {
        b.iter(|| {
            let key = *b"key key key asd asd asd     ";
            db.entry(0, &key)
                .vacant()
                .unwrap()
                .insert()
                .unwrap()
                .write_at(0, &[0, 1])
                .unwrap();
            let value = db.entry(0, &key).occupied().unwrap().remove().unwrap();
            black_box(value.read_to_vec(0, 2));
            black_box(db.stats());
        })
    });
}
