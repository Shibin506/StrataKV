//! The database: a write-ahead log, an in-memory table, and levels of sorted tables on disk.
//!
//! Reads look at the newest data first: the memtable, the memtable being flushed, the level-0 tables
//! (newest first), then at most one table in each deeper level. The first layer that knows the key
//! answers, and a delete (tombstone) in a newer layer hides any older value.
//!
//! Flush (each step is a crash point in the tests): fsync the log and start a new one; write the full
//! memtable as a level-0 table; replace `CURRENT` atomically; delete the old log.
//!
//! Compaction (see `compaction.rs`) runs on a background thread: merge the chosen tables into new ones;
//! replace `CURRENT` atomically (inputs out, outputs in); delete the inputs. A crash before the switch
//! leaves the outputs as leftovers (deleted on open); a crash after it leaves the inputs as leftovers.

use std::collections::{HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, RwLock};
use std::thread::JoinHandle;

use crate::batch::WriteBatch;
use crate::compaction::{self, Levels, Merge, Tuning, WriteSettings};
use crate::error::{Error, Result};
use crate::failpoint::{self, Point};
use crate::memtable::{Lookup, Memtable};
use crate::sstable::{self, Table, TableMeta};
use crate::version::{self, MAX_LEVELS, StoreFor, TableEntry, VersionState};
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
    /// Compact level 0 into level 1 once it has this many tables.
    pub l0_compaction_trigger: usize,
    /// Size limit of level 1; each deeper level may be `level_multiplier` times bigger.
    pub level_base_bytes: u64,
    pub level_multiplier: u64,
    /// Compaction splits its output into tables of about this size.
    pub target_file_bytes: u64,
    /// Run compactions on a background thread. If false, call `compact()` yourself.
    pub background_compaction: bool,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            sync: SyncMode::Always,
            create_if_missing: true,
            memtable_bytes: 4 << 20,
            block_bytes: 4096,
            bloom_bits_per_key: 10,
            l0_compaction_trigger: 4,
            level_base_bytes: 16 << 20,
            level_multiplier: 10,
            target_file_bytes: 2 << 20,
            background_compaction: true,
        }
    }
}

impl Options {
    fn tuning(&self) -> Tuning {
        Tuning {
            l0_trigger: self.l0_compaction_trigger,
            level_base_bytes: self.level_base_bytes,
            level_multiplier: self.level_multiplier,
            target_file_bytes: self.target_file_bytes,
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
    /// Leftover files from an interrupted flush or compaction that were deleted.
    pub leftovers_removed: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct LevelStats {
    pub tables: usize,
    pub bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Stats {
    pub tables: usize,
    pub table_bytes: u64,
    /// Index = level.
    pub levels: Vec<LevelStats>,
    pub memtable_bytes: usize,
    pub wal_number: u64,
    pub wal_bytes: u64,
    pub last_sequence: u64,
    pub flushes: u64,
    pub compactions: u64,
    /// Compactions that only relabelled a table with the next level (nothing rewritten).
    pub trivial_moves: u64,
    /// Bytes of tables written by flushes and by compactions: with the bytes written by users, this
    /// gives the write amplification.
    pub flush_bytes_written: u64,
    pub compaction_bytes_written: u64,
    /// Data blocks read from tables since the database was opened (a Bloom filter "no" reads none).
    pub block_reads: u64,
    /// The last background compaction error, if any. Background compaction stops after an error;
    /// data stays safe and readable, and reopening the database retries.
    pub compaction_error: Option<String>,
}

/// One table as `levels()` reports it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableInfo {
    pub id: u64,
    pub level: usize,
    pub size: u64,
    pub entries: u64,
    pub smallest: Vec<u8>,
    pub largest: Vec<u8>,
}

struct WriteSide {
    wal: WalWriter,
    wal_number: u64,
    last_seq: u64,
    version: VersionState,
    /// Set when a disk operation failed and the files' state is unknown: no more writes until reopen.
    poisoned: Option<String>,
}

struct ReadState {
    mem: Memtable,
    /// The memtable being written to a table: still readable until the table is in place.
    imm: Option<Arc<Memtable>>,
    levels: Arc<Levels>,
}

#[derive(Default)]
struct Counters {
    flushes: AtomicU64,
    compactions: AtomicU64,
    trivial_moves: AtomicU64,
    flush_bytes: AtomicU64,
    compaction_bytes: AtomicU64,
}

#[derive(Default)]
struct BgSignal {
    pending: bool,
    shutdown: bool,
}

struct Inner {
    dir: PathBuf,
    options: Options,
    write: Mutex<WriteSide>,
    state: RwLock<ReadState>,
    /// Only one compaction at a time; holds where each level's last compaction ended.
    compaction: Mutex<Vec<Vec<u8>>>,
    bg: Mutex<BgSignal>,
    bg_cv: Condvar,
    stop: AtomicBool,
    bg_error: Mutex<Option<String>>,
    counters: Counters,
    recovery: RecoveryReport,
    _lock: File,
}

pub struct Db {
    inner: Arc<Inner>,
    background: Option<JoinHandle<()>>,
}

fn number_of(name: &str, ext: &str) -> Option<u64> {
    name.strip_suffix(ext)
        .filter(|s| s.len() >= 6 && s.bytes().all(|b| b.is_ascii_digit()))
        .and_then(|s| s.parse().ok())
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

/// Arranges tables into levels: level 0 newest first, deeper levels by key, checking that tables in a
/// deeper level never overlap (that is what lets a read check only one of them).
fn arrange(entries: &[TableEntry], tables: &HashMap<u64, Arc<Table>>, dir: &Path) -> Result<Levels> {
    let mut levels: Levels = vec![Vec::new(); MAX_LEVELS as usize];
    for e in entries {
        levels[e.level as usize].push(tables[&e.id].clone());
    }
    levels[0].sort_by_key(|t| std::cmp::Reverse(t.id()));
    for (n, level) in levels.iter_mut().enumerate().skip(1) {
        level.sort_by(|a, b| a.smallest().cmp(b.smallest()));
        for pair in level.windows(2) {
            if pair[0].largest() >= pair[1].smallest() {
                return Err(Error::Corruption {
                    file: dir.join(version::CURRENT),
                    offset: 0,
                    reason: format!("tables {} and {} overlap in level {n}", pair[0].id(), pair[1].id()),
                });
            }
        }
    }
    Ok(levels)
}

impl Db {
    pub fn open(dir: impl AsRef<Path>, options: Options) -> Result<Self> {
        let inner = Arc::new(Inner::open(dir.as_ref(), options)?);
        let background = if inner.options.background_compaction {
            let worker = inner.clone();
            let handle = std::thread::Builder::new()
                .name("stratakv-compaction".into())
                .spawn(move || worker.background_loop())?;
            inner.wake_compaction();
            Some(handle)
        } else {
            None
        };
        Ok(Self { inner, background })
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
        self.inner.write(batch)
    }

    /// Writes the memtable to a table now, even if it is not full.
    pub fn flush(&self) -> Result<()> {
        let mut w = lock(&self.inner.write);
        if let Some(why) = &w.poisoned {
            return Err(Error::Poisoned(why.clone()));
        }
        self.inner
            .flush_locked(&mut w)
            .inspect_err(|e| w.poisoned = Some(format!("flush failed: {e}")))
    }

    /// Runs compactions until no level is over its limit (what the background thread does).
    /// Returns how many ran.
    pub fn compact(&self) -> Result<usize> {
        let mut n = 0;
        while self.inner.compact_once()? {
            n += 1;
        }
        Ok(n)
    }

    /// Flushes, then merges every table into one sorted run at the deepest level in use. Afterwards every
    /// key has exactly one version on disk and no deletes are left.
    pub fn compact_all(&self) -> Result<()> {
        self.flush()?;
        self.inner.compact_everything()
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.inner.get(key)
    }

    pub fn path(&self) -> &Path {
        &self.inner.dir
    }

    pub fn recovery_report(&self) -> RecoveryReport {
        self.inner.recovery
    }

    /// The log file new writes go to.
    pub fn wal_path(&self) -> PathBuf {
        log_path(&self.inner.dir, lock(&self.inner.write).wal_number)
    }

    /// Current size of the write-ahead log in bytes.
    pub fn wal_bytes(&self) -> u64 {
        lock(&self.inner.write).wal.len()
    }

    /// Sequence number of the newest applied batch.
    pub fn last_sequence(&self) -> u64 {
        lock(&self.inner.write).last_seq
    }

    /// Every table, by level (level 0 newest first, deeper levels in key order).
    pub fn levels(&self) -> Vec<Vec<TableInfo>> {
        let levels = self.inner.state.read().unwrap_or_else(|e| e.into_inner()).levels.clone();
        levels
            .iter()
            .enumerate()
            .map(|(level, tables)| {
                tables
                    .iter()
                    .map(|t| TableInfo {
                        id: t.id(),
                        level,
                        size: t.size(),
                        entries: t.entries(),
                        smallest: t.smallest().to_vec(),
                        largest: t.largest().to_vec(),
                    })
                    .collect()
            })
            .collect()
    }

    pub fn stats(&self) -> Stats {
        let inner = &self.inner;
        let w = lock(&inner.write);
        let s = inner.state.read().unwrap_or_else(|e| e.into_inner());
        let levels: Vec<LevelStats> = s
            .levels
            .iter()
            .map(|l| LevelStats {
                tables: l.len(),
                bytes: compaction::level_bytes(l),
            })
            .collect();
        let c = &inner.counters;
        Stats {
            tables: levels.iter().map(|l| l.tables).sum(),
            table_bytes: levels.iter().map(|l| l.bytes).sum(),
            levels,
            memtable_bytes: s.mem.approx_bytes(),
            wal_number: w.wal_number,
            wal_bytes: w.wal.len(),
            last_sequence: w.last_seq,
            flushes: c.flushes.load(Ordering::Relaxed),
            compactions: c.compactions.load(Ordering::Relaxed),
            trivial_moves: c.trivial_moves.load(Ordering::Relaxed),
            flush_bytes_written: c.flush_bytes.load(Ordering::Relaxed),
            compaction_bytes_written: c.compaction_bytes.load(Ordering::Relaxed),
            block_reads: s.levels.iter().flatten().map(|t| t.block_reads()).sum(),
            compaction_error: lock(&inner.bg_error).clone(),
        }
    }

    /// Blocks until the background thread has no compaction left to do (for tests and benchmarks).
    pub fn wait_for_compactions(&self) {
        loop {
            {
                let _running = lock(&self.inner.compaction);
                let levels = self.inner.state.read().unwrap_or_else(|e| e.into_inner()).levels.clone();
                let idle = compaction::pick(&levels, &self.inner.options.tuning(), &mut vec![Vec::new(); MAX_LEVELS as usize]).is_none();
                if idle || self.background.is_none() || lock(&self.inner.bg_error).is_some() {
                    return;
                }
            }
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
    }
}

impl Drop for Db {
    fn drop(&mut self) {
        self.inner.stop.store(true, Ordering::Relaxed);
        lock(&self.inner.bg).shutdown = true;
        self.inner.bg_cv.notify_all();
        if let Some(h) = self.background.take() {
            let _ = h.join();
        }
    }
}

impl Inner {
    fn open(dir: &Path, options: Options) -> Result<Self> {
        let dir = dir.to_path_buf();
        if options.memtable_bytes == 0 || options.block_bytes == 0 || options.l0_compaction_trigger == 0 || options.level_multiplier < 2 {
            return Err(Error::InvalidArgument(
                "memtable_bytes, block_bytes, l0_compaction_trigger must be above 0, level_multiplier at least 2".into(),
            ));
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

        let lock_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(dir.join(LOCK_FILE))?;
        match lock_file.try_lock() {
            Ok(()) => {}
            Err(std::fs::TryLockError::WouldBlock) => return Err(Error::Locked(dir)),
            Err(std::fs::TryLockError::Error(e)) => return Err(e.into()),
        }

        let mut version = match version::load(&dir)? {
            Some(v) => v,
            None => {
                let v = VersionState::fresh();
                version::store(&dir, &v, StoreFor::Open)?;
                v
            }
        };

        // Tables listed in CURRENT: each must exist with exactly the recorded size and pass its checks.
        let mut tables = HashMap::new();
        for t in &version.tables {
            tables.insert(t.id, Arc::new(Table::open(&dir, t.id, t.size)?));
        }
        let levels = arrange(&version.tables, &tables, &dir)?;

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

        // Leftovers of an interrupted flush or compaction: half-written files, tables CURRENT does not
        // list, old logs.
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

        Ok(Inner {
            dir,
            options,
            write: Mutex::new(WriteSide {
                wal,
                wal_number,
                last_seq,
                version,
                poisoned: None,
            }),
            state: RwLock::new(ReadState {
                mem,
                imm: None,
                levels: Arc::new(levels),
            }),
            compaction: Mutex::new(vec![Vec::new(); MAX_LEVELS as usize]),
            bg: Mutex::new(BgSignal::default()),
            bg_cv: Condvar::new(),
            stop: AtomicBool::new(false),
            bg_error: Mutex::new(None),
            counters: Counters::default(),
            recovery: report,
            _lock: lock_file,
        })
    }

    fn write(&self, batch: &WriteBatch) -> Result<()> {
        batch.validate()?;
        if batch.is_empty() {
            return Ok(());
        }
        let mut w = lock(&self.write);
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
        next.tables.push(TableEntry {
            id,
            size: meta.size,
            level: 0,
        });
        next.log_number = new_log;
        next.last_seq = w.last_seq;
        version::store(&self.dir, &next, StoreFor::Flush)?;
        w.version = next;
        {
            let mut s = self.state.write().unwrap_or_else(|e| e.into_inner());
            let mut levels = (*s.levels).clone();
            levels[0].insert(0, table);
            s.levels = Arc::new(levels);
            s.imm = None;
        }
        self.counters.flushes.fetch_add(1, Ordering::Relaxed);
        self.counters.flush_bytes.fetch_add(meta.size, Ordering::Relaxed);

        // 4. Old logs are no longer needed.
        for entry in fs::read_dir(&self.dir)? {
            let name = entry?.file_name().to_string_lossy().into_owned();
            if number_of(&name, ".log").is_some_and(|n| n < new_log) {
                fs::remove_file(self.dir.join(&name))?;
            }
        }
        failpoint::hit(Point::FlushAfterWalDelete);
        self.wake_compaction();
        Ok(())
    }

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let (imm, levels) = {
            let s = self.state.read().unwrap_or_else(|e| e.into_inner());
            match s.mem.get(key) {
                Lookup::Found(v) => return Ok(Some(v.to_vec())),
                Lookup::Deleted => return Ok(None),
                Lookup::Absent => {}
            }
            (s.imm.clone(), s.levels.clone())
        };
        if let Some(imm) = imm {
            match imm.get(key) {
                Lookup::Found(v) => return Ok(Some(v.to_vec())),
                Lookup::Deleted => return Ok(None),
                Lookup::Absent => {}
            }
        }
        let candidates = levels[0].iter().chain(levels[1..].iter().filter_map(|files| {
            // Deeper levels do not overlap: at most one table can hold the key.
            let i = files.partition_point(|t| t.largest() < key);
            files.get(i).filter(|t| t.smallest() <= key)
        }));
        for t in candidates {
            match t.get(key)? {
                Lookup::Found(v) => return Ok(Some(v)),
                Lookup::Deleted => return Ok(None),
                Lookup::Absent => {}
            }
        }
        Ok(None)
    }

    // ---------- compaction ----------

    fn wake_compaction(&self) {
        if self.options.background_compaction {
            lock(&self.bg).pending = true;
            self.bg_cv.notify_all();
        }
    }

    fn background_loop(self: Arc<Self>) {
        loop {
            {
                let mut sig = lock(&self.bg);
                while !sig.pending && !sig.shutdown {
                    sig = self.bg_cv.wait(sig).unwrap_or_else(|e| e.into_inner());
                }
                if sig.shutdown {
                    return;
                }
                sig.pending = false;
            }
            loop {
                if self.stop.load(Ordering::Relaxed) || lock(&self.bg_error).is_some() {
                    break;
                }
                match self.compact_once() {
                    Ok(true) => continue,
                    Ok(false) => break,
                    Err(e) => {
                        *lock(&self.bg_error) = Some(e.to_string());
                        break;
                    }
                }
            }
        }
    }

    fn settings(&self) -> WriteSettings<'_> {
        WriteSettings {
            dir: &self.dir,
            block_bytes: self.options.block_bytes,
            bloom_bits_per_key: self.options.bloom_bits_per_key,
            target_file_bytes: self.options.target_file_bytes,
            stop: &self.stop,
        }
    }

    fn next_file_id(&self) -> u64 {
        let mut w = lock(&self.write);
        let id = w.version.next_file;
        w.version.next_file += 1;
        id
    }

    /// Runs the most urgent compaction, if one is needed. Returns whether one ran.
    fn compact_once(&self) -> Result<bool> {
        let mut pointers = lock(&self.compaction);
        let levels = self.state.read().unwrap_or_else(|e| e.into_inner()).levels.clone();
        let Some(plan) = compaction::pick(&levels, &self.options.tuning(), &mut pointers) else {
            return Ok(false);
        };
        if plan.is_trivial_move() {
            let t = plan.upper[0].clone();
            self.install(&[t.id()], vec![(t, plan.output_level)], false)?;
            self.counters.trivial_moves.fetch_add(1, Ordering::Relaxed);
            return Ok(true);
        }
        let done = self.merge_into(&plan.inputs_newest_first(), plan.output_level, &levels)?;
        drop(pointers);
        Ok(done)
    }

    fn compact_everything(&self) -> Result<()> {
        let _running = lock(&self.compaction);
        let levels = self.state.read().unwrap_or_else(|e| e.into_inner()).levels.clone();
        let inputs: Vec<Arc<Table>> = levels.iter().flatten().cloned().collect();
        if inputs.is_empty() {
            return Ok(());
        }
        let deepest = levels.iter().rposition(|l| !l.is_empty()).unwrap_or(0).max(1);
        self.merge_into(&inputs, deepest, &levels)?;
        Ok(())
    }

    /// Merges `inputs` (newest first) into new tables at `output_level` and installs them.
    /// Returns false if the database is shutting down (nothing changed).
    fn merge_into(&self, inputs: &[Arc<Table>], output_level: usize, levels: &Levels) -> Result<bool> {
        let input_ids: HashSet<u64> = inputs.iter().map(|t| t.id()).collect();
        // A tombstone can be dropped once no older version of its key can exist below the output level.
        let deeper: Vec<Arc<Table>> = levels
            .iter()
            .skip(output_level + 1)
            .flatten()
            .filter(|t| !input_ids.contains(&t.id()))
            .cloned()
            .collect();
        let drop_tombstone = |key: &[u8]| !deeper.iter().any(|t| t.overlaps(key, key));
        let Some(outputs) = compaction::write_outputs(Merge::new(inputs), &self.settings(), || self.next_file_id(), drop_tombstone)? else {
            return Ok(false);
        };
        failpoint::hit(Point::CompactionAfterOutputs);
        let opened: Vec<(Arc<Table>, usize)> = outputs
            .iter()
            .map(|o: &TableMeta| Table::open(&self.dir, o.id, o.size).map(|t| (Arc::new(t), output_level)))
            .collect::<Result<_>>()?;
        let written: u64 = outputs.iter().map(|o| o.size).sum();
        let removed: Vec<u64> = inputs.iter().map(|t| t.id()).collect();
        self.install(&removed, opened, true)?;
        self.counters.compactions.fetch_add(1, Ordering::Relaxed);
        self.counters.compaction_bytes.fetch_add(written, Ordering::Relaxed);
        Ok(true)
    }

    /// Atomically replaces `removed` tables by `added` ones (each with its level) in CURRENT and for
    /// readers, then deletes the removed files unless they were only moved.
    fn install(&self, removed: &[u64], added: Vec<(Arc<Table>, usize)>, delete_removed: bool) -> Result<()> {
        let removed_set: HashSet<u64> = removed.iter().copied().collect();
        {
            let mut w = lock(&self.write);
            if let Some(why) = &w.poisoned {
                for (t, _) in &added {
                    if delete_removed {
                        let _ = fs::remove_file(sstable::table_path(&self.dir, t.id()));
                    }
                }
                return Err(Error::Poisoned(why.clone()));
            }
            let mut next = w.version.clone();
            next.tables.retain(|e| !removed_set.contains(&e.id));
            next.tables.extend(added.iter().map(|(t, level)| TableEntry {
                id: t.id(),
                size: t.size(),
                level: *level as u32,
            }));
            if let Err(e) = version::store(&self.dir, &next, StoreFor::Compaction) {
                w.poisoned = Some(format!("could not record a compaction: {e}"));
                return Err(e);
            }
            w.version = next;
            let mut s = self.state.write().unwrap_or_else(|e| e.into_inner());
            let mut levels: Levels = s
                .levels
                .iter()
                .map(|l| l.iter().filter(|t| !removed_set.contains(&t.id())).cloned().collect())
                .collect();
            for (t, level) in added {
                levels[level].push(t);
            }
            levels[0].sort_by_key(|t| std::cmp::Reverse(t.id()));
            for l in levels.iter_mut().skip(1) {
                l.sort_by(|a, b| a.smallest().cmp(b.smallest()));
            }
            s.levels = Arc::new(levels);
        }
        if delete_removed {
            // Readers that still hold a removed table keep reading it: on Unix an open file outlives its name.
            for id in removed {
                fs::remove_file(sstable::table_path(&self.dir, *id))?;
            }
            wal::sync_dir(&self.dir)?;
            failpoint::hit(Point::CompactionAfterInputsDeleted);
        }
        Ok(())
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
            lock(&db.inner.write).wal.break_for_test();
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
