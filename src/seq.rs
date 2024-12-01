use parking_lot::{Mutex, MutexGuard};

use super::{
    page::{PagePtr, RawPtr},
    wal::RecordPage,
};

pub const WAL_SIZE: u32 = 0x100;

pub struct Seq(Mutex<u64>);

impl Seq {
    pub fn new(seq: u64) -> Self {
        Self(Mutex::new(seq))
    }

    pub fn lock(&self) -> SeqLock<'_> {
        SeqLock(self.0.lock())
    }
}

pub struct SeqLock<'a>(MutexGuard<'a, u64>);

impl SeqLock<'_> {
    pub fn seq(&self) -> u64 {
        *self.0
    }

    pub fn ptr(&self) -> Option<PagePtr<RecordPage>> {
        let seq = self.seq();
        let pos = (seq % u64::from(WAL_SIZE)) as u32;
        PagePtr::<RecordPage>::from_raw_number(pos)
    }

    pub fn next(&mut self) {
        *self.0 = self.0.wrapping_add(1);
    }

    pub fn prev(&mut self) {
        *self.0 = self.0.wrapping_sub(1);
    }
}
