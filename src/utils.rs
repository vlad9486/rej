use std::{fs, io, path::Path};

use memmap2::Mmap;

#[cfg(unix)]
pub fn write_at(file: &fs::File, buf: &[u8], offset: u64) -> io::Result<()> {
    use std::os::unix::fs::FileExt;

    file.write_all_at(buf, offset)
}

#[cfg(windows)]
pub fn write_at(file: &fs::File, mut buf: &[u8], mut offset: u64) -> io::Result<()> {
    use std::os::windows::fs::FileExt;

    while !buf.is_empty() {
        let len = file.seek_write(buf, offset)?;
        buf = &buf[len..];
        offset += len as u64;
    }

    Ok(())
}

#[cfg(unix)]
pub fn open_file(path: impl AsRef<Path>, create: bool, direct_write: bool) -> io::Result<fs::File> {
    use std::os::unix::fs::OpenOptionsExt;

    #[cfg(any(target_os = "linux", target_os = "android"))]
    const O_DIRECT: libc::c_int = libc::O_DIRECT;

    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    const O_DIRECT: libc::c_int = 0;

    let mut open_options = fs::OpenOptions::new();
    open_options.write(true).read(true);
    if create {
        open_options.create_new(true);
    }
    if direct_write {
        open_options.custom_flags(O_DIRECT);
    }
    open_options.open(path)
}

#[cfg(windows)]
pub fn open_file(path: impl AsRef<Path>, create: bool, direct_write: bool) -> io::Result<fs::File> {
    let mut open_options = fs::OpenOptions::new();
    let _ = direct_write;
    open_options.write(true).read(true);
    if create {
        open_options.create_new(true);
    }
    open_options.open(path)
}

#[cfg(unix)]
pub fn mmap(file: &fs::File, populate: bool) -> io::Result<Mmap> {
    use memmap2::MmapOptions;

    let mut options = MmapOptions::new();
    if populate {
        options.populate();
    }
    let mmap = unsafe { options.map(file)? };
    // On Unix we advice the OS that page access will be random.
    mmap.advise(memmap2::Advice::Random)?;
    Ok(mmap)
}

// On Windows there is no advice to give.
#[cfg(windows)]
pub fn mmap(file: &fs::File, populate: bool) -> io::Result<Mmap> {
    let _ = populate;
    let mmap = unsafe { Mmap::map(file)? };
    Ok(mmap)
}
