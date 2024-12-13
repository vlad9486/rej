use super::with_db;

#[test]
fn big_value() {
    with_db(0x123, |db, rng| {
        use rand::RngCore;

        let mut data = vec![0; 0x170000];
        rng.fill_bytes(&mut data);

        let value = db.allocate().unwrap();
        db.rewrite(&value, &data).unwrap();
        db.insert(&value, 0, b"big_value").unwrap();

        let value = db.retrieve(0, b"big_value").unwrap();
        let stored = db.read_to_vec(&value);
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
            let value = db.allocate().unwrap();
            db.insert(&value, 0, key.as_bytes()).unwrap();
            db.rewrite(&value, &i.to_le_bytes()).unwrap();
        }

        for i in 0..NUM {
            let key = format!("key                  {i:03}");
            let value = db
                .retrieve(0, key.as_bytes())
                .unwrap_or_else(|| panic!("{key}"));
            assert_eq!(db.read_to_vec(&value), &i.to_le_bytes());
        }

        indexes.shuffle(rng);
        for i in indexes {
            let key = format!("key                  {i:03}");
            let value = db
                .remove(0, key.as_bytes())
                .unwrap()
                .unwrap_or_else(|| panic!("{key}"));
            println!("deleted {key}");
            assert_eq!(db.read_to_vec(&value), &i.to_le_bytes());
            assert!(db.retrieve(0, key.as_bytes()).is_none());
            db.deallocate(value).unwrap();
        }
    })
}
