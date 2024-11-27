use std::{str, fs, str::Utf8Error};

use rej::{Page, PagePtr, Storage, StorageConfig};

#[repr(C)]
#[derive(Clone, Copy)]
struct MyPage {
    name: [u8; 40],
    extension: Option<PagePtr<Photo>>,
}

impl MyPage {
    fn set_name(&mut self, name: &str) {
        if name.len() <= 40 {
            let len = name.as_bytes().len();
            self.name[..len].clone_from_slice(name.as_bytes());
        }
    }

    fn get_name(&self) -> Result<&str, Utf8Error> {
        Ok(str::from_utf8(&self.name)?.trim_end_matches('\0'))
    }
}

// `MyPage` must be `repr(C)` and size must be less or equal `0x1000`
unsafe impl Page for MyPage {}

#[repr(C)]
#[derive(Clone, Copy)]
struct Photo {
    len: u16,
    bytes: [u8; 4000],
}

impl Photo {
    fn set_photo(&mut self, bytes: &[u8]) {
        self.len = bytes.len() as u16;
        self.bytes[..bytes.len()].clone_from_slice(bytes);
    }

    fn get_photo(&self) -> &[u8] {
        &self.bytes[0..(self.len as usize)]
    }
}

// `Photo` must be `repr(C)` and size must be less or equal `0x1000`
unsafe impl Page for Photo {}

fn main() {
    let cfg = StorageConfig::default();
    let storage = Storage::open("target/db", true, cfg).unwrap();

    // My page is the head of the storage at index 1.
    // Index 0 is reserved for allocator.
    let my_page_ptr = storage.head::<MyPage>(1).unwrap();

    // Allocate a page for the photo, it will be linked to my page.
    let photo_ptr = storage.allocate::<Photo>().unwrap();

    // Edit the page
    let mut my_page = *storage.read(my_page_ptr);
    my_page.set_name("Vladyslav");
    // Attach the photo, so the database won't lost it.
    my_page.extension = Some(photo_ptr);
    storage.write(my_page_ptr, &my_page).unwrap();

    // Edit the photo
    let mut photo = *storage.read(photo_ptr);
    photo.set_photo(b"...image bytes, let say, in png format...");
    storage.write(photo_ptr, &photo).unwrap();

    drop(storage);

    // Reopen storage
    let storage = Storage::open("target/db", false, cfg).unwrap();

    let my_page_ptr = storage.head::<MyPage>(1).unwrap();
    let my_page = storage.read(my_page_ptr);
    let name = my_page.get_name().unwrap();
    println!("my name is: {name}");
    assert_eq!(name, "Vladyslav");
    let photo_ptr = my_page.extension.expect("must be a photo");
    // Here `my_page` is a lock, we can have many of them,
    // but as long as there is at least one, the storage cannot grow.
    drop(my_page);

    let photo = storage.read(photo_ptr);
    assert_eq!(
        photo.get_photo(),
        b"...image bytes, let say, in png format..."
    );
    drop(photo);

    // let's remove the photo
    let mut my_page = *storage.read(my_page_ptr);
    if let Some(photo) = my_page.extension.take() {
        storage.free(photo).unwrap();
    }
    // the `extension` pointer is at range 40..44
    storage.write_range(my_page_ptr, &my_page, 40..44).unwrap();

    drop(storage);
    fs::remove_file("target/db").unwrap();
}
