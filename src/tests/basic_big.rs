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
            db.entry(0, key.as_bytes())
                .vacant()
                .unwrap()
                .insert()
                .unwrap()
                .write_at(0, &i.to_le_bytes())
                .unwrap();
        }

        for i in 0..NUM {
            let key = format!("key                  {i:03}");
            let vec = db
                .entry(0, key.as_bytes())
                .occupied()
                .unwrap()
                .into_value()
                .read_to_vec(0, 2)
                .unwrap();
            assert_eq!(vec, &i.to_le_bytes());
        }

        indexes.shuffle(rng);
        for i in indexes {
            let key = format!("key                  {i:03}");
            let vec = db
                .entry(0, key.as_bytes())
                .occupied()
                .unwrap_or_else(|| panic!("{key}"))
                .remove()
                .unwrap()
                .read_to_vec(0, 2)
                .unwrap();
            println!("deleted {key}");
            assert_eq!(vec, &i.to_le_bytes());
            assert!(db.entry(0, key.as_bytes()).vacant().is_some());
        }
    })
}
