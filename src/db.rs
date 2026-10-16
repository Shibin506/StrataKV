//! The database (Phase 2): a write-ahead log, an in-memory table, and sorted tables on disk.
//!
//! Reads look at the newest data first: the memtable, then the memtable being flushed, then the tables
//! from newest to oldest. The first layer that knows the key answers, and a delete (tombstone) in a
//! newer layer hides any older value.
//!
//! Flush, step by step (each step is a crash point in the tests):
//!
//! 1. fsync the current log, then start a new log for new writes;
//! 2. write the full memtable as a table (`NNNNNN.sst.tmp`, fsync, rename, fsync the directory);
//! 3. replace `CURRENT` atomically: the new table is listed and the new log is the oldest one needed;
//! 4. delete the old log.
//!
//! A crash before step 3 leaves the old `CURRENT`: the old log is replayed and the half-made table is
//! deleted as a leftover. A crash after it leaves the new state; an old log that was not deleted yet is
//! ignored and removed. Either way no acknowledged write is lost and nothing is applied twice.

use std::collections::HashSet;
use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};

use crate::batch::WriteBatch;
use crate::error::{Error, Result};
use crate::failpoint::{self, Point};
use crate::memtable::{Lookup, Memtable};
use crate::sstable::{self, Table};
use crate::version::{self, TableEntry, VersionState};
use crate::wal::{self, WalWriter, log_path};

const LOCK_FILE: &str = "LOCK";

/// When a write is forced to stable storage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncMode {
    /// fsync before every write returns. An acknowledged write survives a process crash and a power
    /// cut. The default.
    Always,
    /// Hand the bytes to the operating system and return. An acknowledged write survives the process
    /// being killed, but may be lost if the machine loses power before the OS writes it out.
    Never,
}

#[derive(Debug, Clone)]
pub struct Options {
    pub sync: SyncMode,
    pub create_if_missing: bool,
    /// Flush the memtable to a table once it holds about this many bytes of keys and values.
    pub memtable_bytes: usize,
    /// Target size of a table's data blocks (one block is read per lookup).
    pub block_bytes: usize,
    /// Bloom filter size: 10 bits per key gives about 1% false positives.
    pub bloom_bits_per_key: u32,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            sync: SyncMode::Always,
            create_if_missing: true,
            memtable_bytes: 4 << 20,
            block_bytes: 4096,
            bloom_bits_per_key: 10,
        }
    }
}

/// What opening the database found.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RecoveryReport {
    /// Batches replayed from logs.
    pub batches: usize,
    /// Bytes at the end of the newest log that held an unfinished write and were removed.
    pub torn_bytes: u64,
    /// Tables opened (each checked against its size, footer and checksums).
    pub tables: usize,
    /// Logs replayed (more than one means a flush was interrupted).
    pub logs: usize,
    /// Leftover files from an interrupted flush that were deleted.
    pub leftovers_removed: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Stats {
    pub tables: usize,
    pub table_bytes: u64,
    pub memtable_bytes: usize,
    pub wal_number: u64,
    pub wal_bytes: u64,
    pub last_sequence: u64,
    pub flushes: u64,
    /// Data blocks read from tables since the database was opened (a Bloom filter "no" reads none).
    pub block_reads: u64,
}

struct WriteSide {
    wal: WalWriter,
    wal_number: u64,
    last_seq: u64,
    version: VersionState,
    flushes: u64,
    /// Set when a disk operation failed and the files' state is unknown: no more writes until reopen.
    poisoned: Option<String>,
}

struct ReadState {
    mem: Memtable,
    /// The memtable being written to a table: still readable until the table is in place.
    imm: Option<Arc<Memtable>>,
    /// Newest first.
    tables: Arc<Vec<Arc<Table>>>,
}

pub struct Db {
    dir: PathBuf,
    options: Options,
    write: Mutex<WriteSide>,
    state: RwLock<ReadState>,
    recovery: RecoveryReport,
    _lock: File,
}

fn number_of(name: &str, ext: &str) -> Option<u64> {
    name.strip_suffix(ext)
        .filter(|s| s.len() >= 6 && s.bytes().all(|b| b.is_ascii_digit()))
        .and_then(|s| s.parse().ok())
}

impl Db {
    pub fn open(dir: impl AsRef<Path>, options: Options) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        if options.memtable_bytes == 0 || options.block_bytes == 0 {
            return Err(Error::InvalidArgument("memtable_bytes and block_bytes must be above zero".into()));
        }
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

        let mut version = match version::load(&dir)? {
            Some(v) => v,
            None => {
                let v = VersionState::fresh();
                version::store(&dir, &v, false)?;
                v
            }
        };

        // Tables listed in CURRENT: each must exist with exactly the recorded size and pass its checks.
        let mut tables = Vec::with_capacity(version.tables.len());
        for t in &version.tables {
            tables.push(Arc::new(Table::open(&dir, t.id, t.size)?));
        }

        // Logs still needed, oldest first.
        let mut logs: Vec<u64> = Vec::new();
        for entry in fs::read_dir(&dir)? {
            let name = entry?.file_name().to_string_lossy().into_owned();
            if let Some(n) = number_of(&name, ".log").filter(|&n| n >= version.log_number) {
                logs.push(n);
            }
        }
        logs.sort_unstable();
        if logs.is_empty() {
            logs.push(version.log_number);
        }

        let mut mem = Memtable::default();
        let mut last_seq = version.last_seq;
        let mut report = RecoveryReport {
            tables: tables.len(),
            logs: logs.len(),
            ..Default::default()
        };
        let mut newest_valid_len = 0;
        for (i, &n) in logs.iter().enumerate() {
            let path = log_path(&dir, n);
            let recovered = wal::recover(&path, last_seq)?;
            let is_newest = i + 1 == logs.len();
            if recovered.torn_bytes > 0 && !is_newest {
                // An older log is always fsynced before a newer one is started, so it cannot end in an
                // unfinished write.
                return Err(Error::Corruption {
                    file: path,
                    offset: recovered.valid_len,
                    reason: "an older log ends in an unfinished write".into(),
                });
            }
            for (seq, batch) in &recovered.records {
                mem.apply(batch);
                last_seq = *seq;
            }
            report.batches += recovered.records.len();
            if is_newest {
                report.torn_bytes = recovered.torn_bytes;
                newest_valid_len = recovered.valid_len;
            }
        }
        let wal_number = *logs.last().unwrap();
        let wal = WalWriter::open(&log_path(&dir, wal_number), newest_valid_len)?;
        let max_table = version.tables.iter().map(|t| t.id).max().unwrap_or(0);
        version.next_file = version.next_file.max(wal_number + 1).max(max_table + 1);

        // Leftovers of an interrupted flush: half-written files, tables CURRENT does not list, old logs.
        let live: HashSet<u64> = version.tables.iter().map(|t| t.id).collect();
        for entry in fs::read_dir(&dir)? {
            let name = entry?.file_name().to_string_lossy().into_owned();
            let leftover = name.ends_with(".tmp")
                || number_of(&name, ".sst").is_some_and(|n| !live.contains(&n))
                || number_of(&name, ".log").is_some_and(|n| n < version.log_number);
            if leftover {
                fs::remove_file(dir.join(&name))?;
                report.leftovers_removed += 1;
            }
        }
        if report.leftovers_removed > 0 {
            wal::sync_dir(&dir)?;
        }

        Ok(Self {
            dir,
            options,
            write: Mutex::new(WriteSide {
                wal,
                wal_number,
                last_seq,
                version,
                flushes: 0,
                poisoned: None,
            }),
            state: RwLock::new(ReadState {
                mem,
                imm: None,
                tables: Arc::new(tables),
            }),
            recovery: report,
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
    ///
    /// If the write fills the memtable, it is flushed to a table before returning. If that flush fails,
    /// the write itself is still durable (it is in the log) and `Ok` is returned, but the database refuses
    /// later writes until it is reopened.
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
        let full = {
            let mut s = self.state.write().unwrap_or_else(|e| e.into_inner());
            s.mem.apply(batch); // still holding the write lock: batches reach the memtable in log order
            s.mem.approx_bytes() >= self.options.memtable_bytes
        };
        if full && let Err(e) = self.flush_locked(&mut w) {
            w.poisoned = Some(format!("flush failed: {e}"));
        }
        Ok(())
    }

    /// Writes the memtable to a table now, even if it is not full.
    pub fn flush(&self) -> Result<()> {
        let mut w = self.write.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(why) = &w.poisoned {
            return Err(Error::Poisoned(why.clone()));
        }
        self.flush_locked(&mut w)
            .inspect_err(|e| w.poisoned = Some(format!("flush failed: {e}")))
    }

    fn flush_locked(&self, w: &mut WriteSide) -> Result<()> {
        if self.state.read().unwrap_or_else(|e| e.into_inner()).mem.is_empty() {
            return Ok(());
        }
        // 1. The old log must be complete on disk before any write goes to the new one.
        w.wal.sync()?;
        let new_log = w.version.next_file;
        w.version.next_file += 1;
        let new_wal = WalWriter::open(&log_path(&self.dir, new_log), 0)?;
        let imm = {
            let mut s = self.state.write().unwrap_or_else(|e| e.into_inner());
            let imm = Arc::new(std::mem::take(&mut s.mem));
            s.imm = Some(imm.clone());
            imm
        };
        w.wal = new_wal;
        w.wal_number = new_log;
        failpoint::hit(Point::FlushAfterWalRotate);

        // 2. The table.
        let id = w.version.next_file;
        w.version.next_file += 1;
        let meta = sstable::write_table(&self.dir, id, imm.iter(), self.options.block_bytes, self.options.bloom_bits_per_key)?;
        let table = Arc::new(Table::open(&self.dir, id, meta.size)?);

        // 3. The switch.
        let mut next = w.version.clone();
        next.tables.insert(0, TableEntry { id, size: meta.size });
        next.log_number = new_log;
        next.last_seq = w.last_seq;
        version::store(&self.dir, &next, true)?;
        w.version = next;
        {
            let mut s = self.state.write().unwrap_or_else(|e| e.into_inner());
            let mut tables = Vec::with_capacity(s.tables.len() + 1);
            tables.push(table);
            tables.extend(s.tables.iter().cloned());
            s.tables = Arc::new(tables);
            s.imm = None;
        }
        w.flushes += 1;

        // 4. Old logs are no longer needed.
        for entry in fs::read_dir(&self.dir)? {
            let name = entry?.file_name().to_string_lossy().into_owned();
            if number_of(&name, ".log").is_some_and(|n| n < new_log) {
                fs::remove_file(self.dir.join(&name))?;
            }
        }
        failpoint::hit(Point::FlushAfterWalDelete);
        Ok(())
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let (imm, tables) = {
            let s = self.state.read().unwrap_or_else(|e| e.into_inner());
            match s.mem.get(key) {
                Lookup::Found(v) => return Ok(Some(v.to_vec())),
                Lookup::Deleted => return Ok(None),
                Lookup::Absent => {}
            }
            (s.imm.clone(), s.tables.clone())
        };
        if let Some(imm) = imm {
            match imm.get(key) {
                Lookup::Found(v) => return Ok(Some(v.to_vec())),
                Lookup::Deleted => return Ok(None),
                Lookup::Absent => {}
            }
        }
        for t in tables.iter() {
            match t.get(key)? {
                Lookup::Found(v) => return Ok(Some(v)),
                Lookup::Deleted => return Ok(None),
                Lookup::Absent => {}
            }
        }
        Ok(None)
    }

    pub fn path(&self) -> &Path {
        &self.dir
    }

    pub fn recovery_report(&self) -> RecoveryReport {
        self.recovery
    }

    /// The log file new writes go to.
    pub fn wal_path(&self) -> PathBuf {
        log_path(&self.dir, self.write.lock().unwrap_or_else(|e| e.into_inner()).wal_number)
    }

    /// Current size of the write-ahead log in bytes.
    pub fn wal_bytes(&self) -> u64 {
        self.write.lock().unwrap_or_else(|e| e.into_inner()).wal.len()
    }

    /// Sequence number of the newest applied batch.
    pub fn last_sequence(&self) -> u64 {
        self.write.lock().unwrap_or_else(|e| e.into_inner()).last_seq
    }

    pub fn stats(&self) -> Stats {
        let w = self.write.lock().unwrap_or_else(|e| e.into_inner());
        let s = self.state.read().unwrap_or_else(|e| e.into_inner());
        Stats {
            tables: s.tables.len(),
            table_bytes: s.tables.iter().map(|t| t.size()).sum(),
            memtable_bytes: s.mem.approx_bytes(),
            wal_number: w.wal_number,
            wal_bytes: w.wal.len(),
            last_sequence: w.last_seq,
            flushes: w.flushes,
            block_reads: s.tables.iter().map(|t| t.block_reads()).sum(),
        }
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
