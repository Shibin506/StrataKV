//! The database: a write-ahead log plus an in-memory table (Phase 1).

use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, RwLock};

use crate::batch::WriteBatch;
use crate::error::{Error, Result};
use crate::memtable::{Lookup, Memtable};
use crate::wal::{self, WalWriter};

const LOCK_FILE: &str = "LOCK";
const WAL_FILE: &str = "wal.log";

/// When a write is forced to stable storage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncMode {
    /// fsync (`sync_data`) before every write returns. An acknowledged write survives a process
    /// crash and a power cut. The default.
    Always,
    /// Hand the bytes to the operating system and return. An acknowledged write survives the process
    /// being killed, but may be lost if the machine loses power before the OS writes it out.
    Never,
}

#[derive(Debug, Clone)]
pub struct Options {
    pub sync: SyncMode,
    pub create_if_missing: bool,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            sync: SyncMode::Always,
            create_if_missing: true,
        }
    }
}

/// What opening the database found in the log.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecoveryReport {
    /// Batches replayed from the log.
    pub batches: usize,
    /// Bytes at the end of the log that held an unfinished write and were removed.
    pub torn_bytes: u64,
}

struct WriteSide {
    wal: WalWriter,
    last_seq: u64,
    /// Set when a log write or fsync failed: what reached the disk is unknown, so nothing more is
    /// written until the database is reopened and the log is read back.
    poisoned: Option<String>,
}

pub struct Db {
    dir: PathBuf,
    options: Options,
    /// Writers queue here; it is held while the log is written and fsynced.
    write: Mutex<WriteSide>,
    /// Readers only take this, so a slow fsync never blocks a read.
    mem: RwLock<Memtable>,
    recovery: RecoveryReport,
    _lock: File,
}

impl Db {
    pub fn open(dir: impl AsRef<Path>, options: Options) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        if !dir.exists() {
            if !options.create_if_missing {
                return Err(Error::InvalidArgument(format!("{} does not exist", dir.display())));
            }
            fs::create_dir_all(&dir)?;
            if let Some(parent) = dir.parent().filter(|p| !p.as_os_str().is_empty()) {
                wal::sync_dir(parent)?;
            }
        }

        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(dir.join(LOCK_FILE))?;
        match lock.try_lock() {
            Ok(()) => {}
            Err(std::fs::TryLockError::WouldBlock) => return Err(Error::Locked(dir)),
            Err(std::fs::TryLockError::Error(e)) => return Err(e.into()),
        }

        let wal_path = dir.join(WAL_FILE);
        let recovered = wal::recover(&wal_path, 0)?;
        let mut mem = Memtable::default();
        let mut last_seq = 0;
        for (seq, batch) in &recovered.records {
            mem.apply(batch);
            last_seq = *seq;
        }
        let wal = WalWriter::open(&wal_path, recovered.valid_len)?;
        Ok(Self {
            dir,
            options,
            write: Mutex::new(WriteSide {
                wal,
                last_seq,
                poisoned: None,
            }),
            mem: RwLock::new(mem),
            recovery: RecoveryReport {
                batches: recovered.records.len(),
                torn_bytes: recovered.torn_bytes,
            },
            _lock: lock,
        })
    }

    pub fn put(&self, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> Result<()> {
        let mut b = WriteBatch::new();
        b.put(key, value);
        self.write(&b)
    }

    pub fn delete(&self, key: impl Into<Vec<u8>>) -> Result<()> {
        let mut b = WriteBatch::new();
        b.delete(key);
        self.write(&b)
    }

    /// Applies every operation in `batch`, atomically: after a crash, all of them or none are visible.
    /// When this returns `Ok`, the batch is in the log (and, with `SyncMode::Always`, on stable storage).
    pub fn write(&self, batch: &WriteBatch) -> Result<()> {
        batch.validate()?;
        if batch.is_empty() {
            return Ok(());
        }
        let mut w = self.write.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(why) = &w.poisoned {
            return Err(Error::Poisoned(why.clone()));
        }
        let seq = w.last_seq + 1;
        if let Err(e) = w.wal.append(seq, batch, self.options.sync == SyncMode::Always) {
            if !matches!(e, Error::InvalidArgument(_)) {
                w.poisoned = Some(e.to_string());
            }
            return Err(e);
        }
        w.last_seq = seq;
        // Still holding the write lock: batches reach the memtable in log order.
        self.mem.write().unwrap_or_else(|e| e.into_inner()).apply(batch);
        Ok(())
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let mem = self.mem.read().unwrap_or_else(|e| e.into_inner());
        Ok(match mem.get(key) {
            Lookup::Found(v) => Some(v.to_vec()),
            Lookup::Deleted | Lookup::Absent => None,
        })
    }

    pub fn path(&self) -> &Path {
        &self.dir
    }

    pub fn recovery_report(&self) -> RecoveryReport {
        self.recovery
    }

    /// Current size of the write-ahead log in bytes.
    pub fn wal_bytes(&self) -> u64 {
        self.write.lock().unwrap_or_else(|e| e.into_inner()).wal.len()
    }

    /// Sequence number of the newest applied batch.
    pub fn last_sequence(&self) -> u64 {
        self.write.lock().unwrap_or_else(|e| e.into_inner()).last_seq
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_failed_log_write_is_never_visible_and_stops_further_writes() {
        let dir = tempfile::tempdir().unwrap();
        {
            let db = Db::open(dir.path(), Options::default()).unwrap();
            db.put("before", "1").unwrap();
            db.write.lock().unwrap().wal.break_for_test();
            assert!(matches!(db.put("lost", "2"), Err(Error::Io(_))));
            assert_eq!(db.get(b"lost").unwrap(), None, "a write that failed must not be readable");
            assert!(
                matches!(db.put("after", "3"), Err(Error::Poisoned(_))),
                "no writes after an unknown log state"
            );
            assert_eq!(db.get(b"before").unwrap().as_deref(), Some(&b"1"[..]), "reads still work");
        }
        let db = Db::open(dir.path(), Options::default()).unwrap();
        assert_eq!(db.get(b"before").unwrap().as_deref(), Some(&b"1"[..]));
        assert_eq!(db.get(b"lost").unwrap(), None);
        db.put("after", "3").unwrap();
    }
}
