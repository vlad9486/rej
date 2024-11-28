use std::{fs, io, path::Path};

use memmap2::Mmap;

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
    Ok(open_options.open(path)?)
}

#[cfg(windows)]
pub fn open_file(path: impl AsRef<Path>, create: bool, direct_write: bool) -> io::Result<File> {
    let mut open_options = OpenOptions::new();
    open_options.write(true).read(true);
    if create {
        open_options.create_new(true);
    }
    Ok(open_options.open(path)?)
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
pub fn mmap(file: &File, populate: bool) -> io::Result<Mmap> {
    let mmap = unsafe { Mmap::map(file)? };
    Ok(mmap)
}
