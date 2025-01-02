use super::with_db;

#[test]
fn big_value() {
    with_db(0x123, |db, rng| {
        use rand::RngCore;

        let mut data = vec![0; 0x170023];
        rng.fill_bytes(&mut data);

        let value = db
            .entry(0, b"big_value")
            .vacant()
            .unwrap()
            .insert()
            .unwrap();
        db.rewrite(value, &data).unwrap();

        let stored = db
            .entry(0, b"big_value")
            .occupied()
            .unwrap()
            .into_value()
            .read_to_vec();
        assert_eq!(data, stored);
    });
}

#[test]
fn big() {
    with_db(0x123, |db, rng| {
        use rand::seq::SliceRandom;

        const NUM: u16 = 1000;
        let mut indexes = (0..NUM).collect::<Vec<_>>();

        indexes.shuffle(rng);
        for i in &indexes {
            let key = format!("key                  {i:03}");
            let value = db
                .entry(0, key.as_bytes())
                .vacant()
                .unwrap()
                .insert()
                .unwrap();
            db.rewrite(value, &i.to_le_bytes()).unwrap();
        }

        for i in 0..NUM {
            let key = format!("key                  {i:03}");
            let vec = db
                .entry(0, key.as_bytes())
                .occupied()
                .unwrap()
                .into_value()
                .read_to_vec();
            assert_eq!(vec, &i.to_le_bytes());
        }

        indexes.shuffle(rng);
        for i in indexes {
            let key = format!("key                  {i:03}");
            let value = db
                .entry(0, key.as_bytes())
                .occupied()
                .unwrap_or_else(|| panic!("{key}"))
                .remove()
                .unwrap();
            println!("deleted {key}");
            assert_eq!(value.read_to_vec(), &i.to_le_bytes());
            assert!(db.entry(0, key.as_bytes()).vacant().is_some());
        }
    })
}
