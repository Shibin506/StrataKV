//! The LOCK file: at most one open database per directory, across processes and within one process.
//!
//! Why not `File::try_lock` (an `flock` lock)? An `flock` lock belongs to the open file, and a child process
//! briefly shares every open file of its parent while it starts. If any thread starts a child process just
//! as a database is closed, the lock outlives the close, and reopening the database fails with `Locked`
//! (measured: half of 20,000 reopens refused while another thread started 154 child processes).
//!
//! So, like RocksDB, StrataKV uses a POSIX record lock (`fcntl(F_SETLK)`), which belongs to the process
//! and is never inherited by children, plus a list of directories this process has open, because POSIX
//! locks do not stop the same process from locking twice. One more POSIX rule shapes the code: closing
//! *any* handle to the file releases the process's lock, so the list is checked before the file is opened.

use std::collections::BTreeSet;
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use crate::error::{Error, Result};

pub const LOCK_FILE: &str = "LOCK";

static HELD: Mutex<BTreeSet<PathBuf>> = Mutex::new(BTreeSet::new());

/// Held for as long as the database is open; dropping it releases the lock.
#[derive(Debug)]
pub struct DirLock {
    file: Option<File>,
    key: PathBuf,
}

pub fn acquire(dir: &Path) -> Result<DirLock> {
    let key = dir.canonicalize()?;
    let mut held = HELD.lock().unwrap_or_else(|e| e.into_inner());
    if held.contains(&key) {
        return Err(Error::Locked(dir.to_path_buf()));
    }
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(dir.join(LOCK_FILE))?;
    if !try_lock(&file)? {
        return Err(Error::Locked(dir.to_path_buf()));
    }
    held.insert(key.clone());
    Ok(DirLock { file: Some(file), key })
}

impl Drop for DirLock {
    fn drop(&mut self) {
        // Close the file (releasing the POSIX lock) while still holding the list: otherwise another thread
        // could lock the directory in between, and this close would release that thread's lock.
        let mut held = HELD.lock().unwrap_or_else(|e| e.into_inner());
        drop(self.file.take());
        held.remove(&self.key);
    }
}

/// Ok(false) if another process holds the lock.
#[cfg(unix)]
fn try_lock(file: &File) -> Result<bool> {
    use std::os::fd::AsRawFd;
    // SAFETY: a zeroed `flock` is a valid value (all integer fields); `fcntl` only reads it.
    let mut fl: libc::flock = unsafe { std::mem::zeroed() };
    fl.l_type = libc::F_WRLCK as _;
    fl.l_whence = libc::SEEK_SET as _;
    fl.l_start = 0;
    fl.l_len = 0; // the whole file
    // SAFETY: a valid open descriptor and a pointer to a live `flock`.
    if unsafe { libc::fcntl(file.as_raw_fd(), libc::F_SETLK, &fl) } == 0 {
        return Ok(true);
    }
    let err = std::io::Error::last_os_error();
    match err.raw_os_error() {
        Some(libc::EAGAIN) | Some(libc::EACCES) => Ok(false),
        _ => Err(err.into()),
    }
}

#[cfg(not(unix))]
fn try_lock(file: &File) -> Result<bool> {
    match file.try_lock() {
        Ok(()) => Ok(true),
        Err(std::fs::TryLockError::WouldBlock) => Ok(false),
        Err(std::fs::TryLockError::Error(e)) => Err(e.into()),
    }
}
