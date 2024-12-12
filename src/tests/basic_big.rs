use super::with_db;

#[test]
fn big() {
    with_db(0x123, |db, rng| {
        use rand::seq::SliceRandom;

        const NUM: u16 = 1000;
        let mut indexes = (0..NUM).collect::<Vec<_>>();

        indexes.shuffle(rng);
        for i in &indexes {
            let key = format!("key                  {i:03}");
            let value = db.insert(0, key.as_bytes()).unwrap();
            db.write(&value, &i.to_le_bytes()).unwrap();
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
        }
    })
}
