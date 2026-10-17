//! The three engines behind one small interface. Every engine gets the same keys, values and order.

use std::path::Path;
use std::time::Duration;

use rusqlite::{Connection, OptionalExtension, params};

/// Durability for single writes. `Never`: survives the process being killed (data is in the OS), may
/// lose the last writes on power loss. `Always`: fsync before every write returns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Sync {
    Never,
    Always,
}

pub trait Engine {
    fn put(&mut self, key: &[u8], value: &[u8]);
    /// All of `items` as one atomic write (one batch / one transaction).
    fn put_batch(&mut self, items: &[(Vec<u8>, Vec<u8>)]);
    /// The value's length, or `None` if the key is missing.
    fn get(&mut self, key: &[u8]) -> Option<usize>;
    /// Reads up to `n` entries starting at `start` (a seek, then `n` steps). Returns the bytes read.
    fn scan(&mut self, start: &[u8], n: usize) -> usize;
    /// Reads every entry in key order. Returns how many.
    fn scan_all(&mut self) -> usize;
    /// Waits until the engine has no background work left (compactions, or a SQLite WAL checkpoint).
    fn settle(&mut self);
    /// Level-0 tables right now (LSM engines only): many of them means writes outran compaction.
    fn level0_files(&self) -> Option<u64> {
        None
    }
    /// Engine-specific numbers for the report.
    fn notes(&self) -> Vec<(String, String)> {
        Vec::new()
    }
}

pub fn open(engine: &str, dir: &Path, sync: Sync) -> Box<dyn Engine> {
    match engine {
        "stratakv" => Box::new(Strata::open(dir, sync)),
        "rocksdb" => Box::new(Rocks::open(dir, sync)),
        "sqlite" => Box::new(Sqlite::open(dir, sync)),
        other => panic!("unknown engine {other:?} (stratakv, rocksdb, sqlite)"),
    }
}

// ---------------------------------------------------------------------------------------------------
// StrataKV, at its defaults: 4 MiB memtable, 4 KiB blocks, 10-bit Bloom filters, L1 = 16 MiB, x10 per level.

pub struct Strata {
    db: stratakv::Db,
}

impl Strata {
    fn open(dir: &Path, sync: Sync) -> Self {
        let sync = match sync {
            Sync::Never => stratakv::SyncMode::Never,
            Sync::Always => stratakv::SyncMode::Always,
        };
        Self {
            db: stratakv::Db::open(
                dir,
                stratakv::Options {
                    sync,
                    ..Default::default()
                },
            )
            .unwrap(),
        }
    }
}

impl Engine for Strata {
    fn put(&mut self, key: &[u8], value: &[u8]) {
        self.db.put(key, value).unwrap();
    }

    fn put_batch(&mut self, items: &[(Vec<u8>, Vec<u8>)]) {
        let mut b = stratakv::WriteBatch::new();
        for (k, v) in items {
            b.put(k.as_slice(), v.as_slice());
        }
        self.db.write(&b).unwrap();
    }

    fn get(&mut self, key: &[u8]) -> Option<usize> {
        self.db.get(key).unwrap().map(|v| v.len())
    }

    fn scan(&mut self, start: &[u8], n: usize) -> usize {
        self.db.scan(start..).unwrap().take(n).map(|r| r.unwrap().1.len()).sum()
    }

    fn scan_all(&mut self) -> usize {
        self.db.scan::<&[u8], _>(..).unwrap().inspect(|r| assert!(r.is_ok())).count()
    }

    fn settle(&mut self) {
        self.db.wait_for_compactions();
    }

    fn level0_files(&self) -> Option<u64> {
        Some(self.db.stats().levels[0].tables as u64)
    }

    fn notes(&self) -> Vec<(String, String)> {
        let s = self.db.stats();
        let levels: Vec<String> = s.levels.iter().map(|l| l.tables.to_string()).collect();
        vec![
            ("tables per level".into(), levels.join(" ")),
            ("compactions".into(), s.compactions.to_string()),
            ("trivial moves".into(), s.trivial_moves.to_string()),
            (
                "write stalls".into(),
                format!("{} ({:.0} ms in total)", s.write_stalls, s.write_stall_micros as f64 / 1000.0),
            ),
        ]
    }
}

// ---------------------------------------------------------------------------------------------------
// RocksDB, given the same tree shape as StrataKV (memtable, file and level sizes, Bloom filter, block size,
// no compression), so the comparison is about the implementation and not about different tuning.
// Everything else stays at RocksDB's defaults (32 MiB block cache, 2 background jobs).

pub struct Rocks {
    db: rocksdb::DB,
    write: rocksdb::WriteOptions,
}

impl Rocks {
    fn open(dir: &Path, sync: Sync) -> Self {
        let mut opts = rocksdb::Options::default();
        opts.create_if_missing(true);
        opts.set_compression_type(rocksdb::DBCompressionType::None);
        opts.set_write_buffer_size(4 << 20);
        opts.set_level_zero_file_num_compaction_trigger(4);
        opts.set_max_bytes_for_level_base(16 << 20);
        opts.set_max_bytes_for_level_multiplier(10.0);
        opts.set_target_file_size_base(2 << 20);
        opts.set_level_compaction_dynamic_level_bytes(false);
        let mut table = rocksdb::BlockBasedOptions::default();
        table.set_block_size(4096);
        table.set_bloom_filter(10.0, false);
        opts.set_block_based_table_factory(&table);
        let mut write = rocksdb::WriteOptions::default();
        write.set_sync(sync == Sync::Always);
        Self {
            db: rocksdb::DB::open(&opts, dir).unwrap(),
            write,
        }
    }

    fn int(&self, name: &str) -> u64 {
        self.db.property_int_value(name).unwrap().unwrap_or(0)
    }
}

impl Engine for Rocks {
    fn put(&mut self, key: &[u8], value: &[u8]) {
        self.db.put_opt(key, value, &self.write).unwrap();
    }

    fn put_batch(&mut self, items: &[(Vec<u8>, Vec<u8>)]) {
        let mut b = rocksdb::WriteBatch::default();
        for (k, v) in items {
            b.put(k, v);
        }
        self.db.write_opt(b, &self.write).unwrap();
    }

    fn get(&mut self, key: &[u8]) -> Option<usize> {
        self.db.get_pinned(key).unwrap().map(|v| v.len())
    }

    fn scan(&mut self, start: &[u8], n: usize) -> usize {
        let mut it = self.db.raw_iterator();
        it.seek(start);
        let mut bytes = 0;
        for _ in 0..n {
            match it.value() {
                Some(v) => bytes += v.len(),
                None => break,
            }
            it.next();
        }
        it.status().unwrap();
        bytes
    }

    fn scan_all(&mut self) -> usize {
        let mut it = self.db.raw_iterator();
        it.seek_to_first();
        let mut n = 0;
        while it.valid() {
            n += 1;
            it.next();
        }
        it.status().unwrap();
        n
    }

    fn settle(&mut self) {
        // Like StrataKV's wait_for_compactions: wait for flushes and compactions, do not force any.
        loop {
            let busy = self.int("rocksdb.compaction-pending")
                + self.int("rocksdb.num-running-compactions")
                + self.int("rocksdb.num-running-flushes")
                + self.int("rocksdb.mem-table-flush-pending");
            if busy == 0 {
                return;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    fn level0_files(&self) -> Option<u64> {
        Some(self.int("rocksdb.num-files-at-level0"))
    }

    fn notes(&self) -> Vec<(String, String)> {
        let levels: Vec<String> = (0..7)
            .map(|l| self.int(&format!("rocksdb.num-files-at-level{l}")).to_string())
            .collect();
        // "Cumulative stall: 00:00:0.123 H:M:S, 4.5 percent" in RocksDB's own statistics text.
        let stats = self.db.property_value("rocksdb.stats").unwrap().unwrap_or_default();
        let stall = stats
            .lines()
            .find(|l| l.starts_with("Cumulative stall:"))
            .unwrap_or("Cumulative stall: n/a")
            .trim_start_matches("Cumulative stall: ");
        vec![
            ("tables per level".into(), levels.join(" ")),
            ("write stall time".into(), stall.to_string()),
        ]
    }
}

// ---------------------------------------------------------------------------------------------------
// SQLite in WAL mode (how it is usually run when speed matters), one table with the key as primary key.
// synchronous=NORMAL in WAL mode does not fsync on commit: the same guarantee as `Sync::Never`.
// synchronous=FULL fsyncs the WAL on every commit: the same as `Sync::Always`.

pub struct Sqlite {
    conn: Connection,
}

impl Sqlite {
    fn open(dir: &Path, sync: Sync) -> Self {
        std::fs::create_dir_all(dir).unwrap();
        let conn = Connection::open(dir.join("kv.sqlite3")).unwrap();
        let mode: String = conn.query_row("PRAGMA journal_mode=WAL", [], |r| r.get(0)).unwrap();
        assert_eq!(mode, "wal");
        conn.execute_batch(match sync {
            Sync::Never => "PRAGMA synchronous=NORMAL;",
            Sync::Always => "PRAGMA synchronous=FULL;",
        })
        .unwrap();
        conn.execute_batch("CREATE TABLE IF NOT EXISTS kv (k BLOB PRIMARY KEY, v BLOB NOT NULL) WITHOUT ROWID;")
            .unwrap();
        Self { conn }
    }
}

impl Engine for Sqlite {
    fn put(&mut self, key: &[u8], value: &[u8]) {
        let conn = &mut self.conn;
        conn.prepare_cached("INSERT OR REPLACE INTO kv (k, v) VALUES (?1, ?2)")
            .unwrap()
            .execute(params![key, value])
            .unwrap();
    }

    fn put_batch(&mut self, items: &[(Vec<u8>, Vec<u8>)]) {
        let conn = &mut self.conn;
        let tx = conn.transaction().unwrap();
        {
            let mut stmt = tx.prepare_cached("INSERT OR REPLACE INTO kv (k, v) VALUES (?1, ?2)").unwrap();
            for (k, v) in items {
                stmt.execute(params![k, v]).unwrap();
            }
        }
        tx.commit().unwrap();
    }

    fn get(&mut self, key: &[u8]) -> Option<usize> {
        let conn = &mut self.conn;
        let mut stmt = conn.prepare_cached("SELECT length(v) FROM kv WHERE k = ?1").unwrap();
        stmt.query_row(params![key], |r| r.get::<_, i64>(0))
            .optional()
            .unwrap()
            .map(|n| n as usize)
    }

    fn scan(&mut self, start: &[u8], n: usize) -> usize {
        let conn = &mut self.conn;
        let mut stmt = conn.prepare_cached("SELECT v FROM kv WHERE k >= ?1 ORDER BY k LIMIT ?2").unwrap();
        let mut rows = stmt.query(params![start, n as i64]).unwrap();
        let mut bytes = 0;
        while let Some(row) = rows.next().unwrap() {
            bytes += row.get_ref(0).unwrap().as_blob().unwrap().len();
        }
        bytes
    }

    fn scan_all(&mut self) -> usize {
        let conn = &mut self.conn;
        let mut stmt = conn.prepare_cached("SELECT k, v FROM kv ORDER BY k").unwrap();
        let mut rows = stmt.query([]).unwrap();
        let mut n = 0;
        while rows.next().unwrap().is_some() {
            n += 1;
        }
        n
    }

    fn settle(&mut self) {
        // Copy the WAL back into the main file, the background work SQLite otherwise does every 1000 pages.
        self.conn.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(())).unwrap();
    }
}
