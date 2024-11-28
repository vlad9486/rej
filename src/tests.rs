use tempdir::TempDir;

use super::{Storage, StorageConfig, Page};

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct P {
    data: [u64; 512],
}

unsafe impl Page for P {}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct S {
    data: [u64; 511],
}

unsafe impl Page for S {}

#[test]
fn basic() {
    let cfg = StorageConfig::default();
    let dir = TempDir::new("rej").unwrap();
    let path = dir.path().join("test-basic");

    let st = Storage::<S>::open(&path, true, cfg).unwrap();
    let ptr = st.allocate::<P>().unwrap();
    let mut page = *st.read(ptr);
    page.data[0] = 0xdeadbeef_abcdef00;
    page.data[1] = 0x1234567890;
    st.write_range(ptr, &page, 0..16).unwrap();
    st.write_static(&S { data: [1; 511] }).unwrap();
    drop(st);

    let st = Storage::<S>::open(&path, false, cfg).unwrap();
    let retrieved = st.read(ptr);
    let sta = st.read_static();
    assert_eq!(*retrieved, page);
    assert_eq!(sta.data[5], 1);
}

#[test]
fn allocation() {
    let cfg = StorageConfig::default();
    let dir = TempDir::new("rej").unwrap();
    let path = dir.path().join("test-allocation");

    let st = Storage::<S>::open(&path, true, cfg).unwrap();
    let a = st.allocate::<P>().unwrap();
    let b = st.allocate::<P>().unwrap();
    let c = st.allocate::<P>().unwrap();
    let d = st.allocate::<P>().unwrap();

    st.free(b).unwrap();
    st.free(d).unwrap();

    let e = st.allocate::<P>().unwrap();
    let f = st.allocate::<P>().unwrap();

    assert!((e == b && f == d) || (e == d && f == b));

    st.free(a).unwrap();
    st.free(c).unwrap();
    st.free(e).unwrap();
    st.free(f).unwrap();
}
