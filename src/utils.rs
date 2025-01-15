use std::{fs, io, path::Path};

#[cfg(unix)]
pub fn m_lock<T>(p: &T) -> bool {
    use std::{ptr, mem};

    let ptr = ptr::from_ref(p).cast();
    let len = mem::size_of_val(p);

    unsafe { libc::mlock(ptr, len) == 0 }
}

#[cfg(windows)]
pub fn m_lock<T>(p: &T) -> bool {
    use std::{ptr, mem};
    use windows_sys::Win32::System::Memory;

    let ptr = ptr::from_ref(p).cast();
    let len = mem::size_of_val(p);

    unsafe { Memory::VirtualLock(ptr, len) != 0 }
}

#[cfg(unix)]
pub fn write_at(file: &fs::File, buf: &[u8], offset: u64) -> io::Result<()> {
    use std::os::unix::fs::FileExt;

    file.write_all_at(buf, offset)
}

#[allow(dead_code)]
#[cfg(unix)]
pub fn write_v_at(
    file: &fs::File,
    ring: &std::cell::UnsafeCell<io_uring::IoUring>,
    buffers: impl Iterator<Item = (u64, *const u8)>,
) -> io::Result<()> {
    use io_uring::{opcode, types};
    use std::os::unix::io::AsRawFd;

    let fd = file.as_raw_fd();

    let ring = unsafe { &mut *ring.get() };

    let mut l = 0;
    for (offset, ptr) in buffers {
        l += 1;
        log::info!("write offset {offset}");

        let op = opcode::Write::new(types::Fd(fd), ptr, 0x1000)
            .offset(offset)
            .build();
        unsafe {
            ring.submission()
                .push(&op)
                .map_err(|err| io::Error::new(io::ErrorKind::Other, err))?;
        }
    }
    ring.submit_and_wait(l)?;

    for i in 0..l {
        let cqe = ring.completion().next().unwrap();
        if cqe.result() < 0 {
            log::error!("Error: {}", io::Error::from_raw_os_error(-cqe.result()));
        } else {
            log::info!("result for {i} is {}", cqe.result());
        }
    }

    Ok(())
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
pub fn read_at(file: &fs::File, buf: &mut [u8], offset: u64) -> io::Result<()> {
    use std::os::unix::fs::FileExt;

    file.read_exact_at(buf, offset)
}

#[cfg(unix)]
pub fn open_file(path: impl AsRef<Path>, direct_write: bool) -> io::Result<fs::File> {
    use std::os::unix::fs::OpenOptionsExt;

    #[cfg(any(target_os = "linux", target_os = "android"))]
    const O_DIRECT: libc::c_int = libc::O_DIRECT;

    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    const O_DIRECT: libc::c_int = 0;

    let mut open_options = fs::OpenOptions::new();
    open_options.write(true).read(true);
    if !path.as_ref().exists() {
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
