//! Phase 2: memtables flushed to sorted tables on disk.

use std::collections::BTreeMap;
use std::fs;

use proptest::prelude::*;
use stratakv::{Db, Error, Options, SyncMode, WriteBatch};

/// These tests are about flushing, so tables are left to pile up (no compaction; see tests/compaction.rs).
fn small(memtable_bytes: usize) -> Options {
    Options {
        sync: SyncMode::Never,
        memtable_bytes,
        block_bytes: 512,
        background_compaction: false,
        ..Options::default()
    }
}

fn value(i: u32) -> Vec<u8> {
    format!("value-{i}-{}", "x".repeat((i % 50) as usize)).into_bytes()
}

#[test]
fn many_flushes_and_every_value_still_reads_back() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = Db::open(dir.path(), small(8 * 1024)).unwrap();
        for i in 0..20_000u32 {
            db.put(format!("key{:08}", i.wrapping_mul(2_654_435_761) % 1_000_000), value(i))
                .unwrap();
        }
        let s = db.stats();
        assert!(s.tables >= 50, "expected many tables, got {}", s.tables);
        assert_eq!(s.flushes as usize, s.tables);
        assert!(
            s.memtable_bytes < 8 * 1024 + 200 && s.wal_bytes < 64 * 1024,
            "the log is restarted at every flush: {s:?}"
        );
        for i in (0..20_000u32).step_by(7) {
            assert_eq!(
                db.get(format!("key{:08}", i.wrapping_mul(2_654_435_761) % 1_000_000).as_bytes())
                    .unwrap(),
                Some(value(i))
            );
        }
    }
    let db = Db::open(dir.path(), small(8 * 1024)).unwrap();
    let r = db.recovery_report();
    assert!(r.tables >= 50 && r.logs == 1 && r.leftovers_removed == 0, "{r:?}");
    for i in 0..20_000u32 {
        assert_eq!(
            db.get(format!("key{:08}", i.wrapping_mul(2_654_435_761) % 1_000_000).as_bytes())
                .unwrap(),
            Some(value(i))
        );
    }
}

#[test]
fn newer_layers_win_and_a_delete_hides_older_tables() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), small(1 << 20)).unwrap();
    db.put("k", "v1").unwrap();
    db.put("other", "o").unwrap();
    db.flush().unwrap();
    db.put("k", "v2").unwrap();
    db.flush().unwrap();
    assert_eq!(db.get(b"k").unwrap().as_deref(), Some(&b"v2"[..]), "the newer table wins");
    db.delete("k").unwrap();
    assert_eq!(db.get(b"k").unwrap(), None, "a delete in the memtable hides both tables");
    db.flush().unwrap();
    assert_eq!(db.get(b"k").unwrap(), None, "a delete in a table hides older tables");
    drop(db);
    let db = Db::open(dir.path(), small(1 << 20)).unwrap();
    assert_eq!(db.get(b"k").unwrap(), None);
    assert_eq!(db.get(b"other").unwrap().as_deref(), Some(&b"o"[..]));
    db.put("k", "v3").unwrap();
    assert_eq!(db.get(b"k").unwrap().as_deref(), Some(&b"v3"[..]));
    assert_eq!(db.stats().tables, 3);
}

#[test]
fn bloom_filters_keep_lookups_for_missing_keys_off_the_disk() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), small(64 * 1024)).unwrap();
    for i in 0..30_000u32 {
        db.put(format!("present-{i}"), "v").unwrap();
    }
    db.flush().unwrap();
    let tables = db.stats().tables as u64;
    assert!(tables >= 5);
    let before = db.stats().block_reads;
    let lookups = 20_000u64;
    for i in 0..lookups {
        assert_eq!(db.get(format!("absent-{i}").as_bytes()).unwrap(), None);
    }
    let reads = db.stats().block_reads - before;
    // Without filters every lookup would read one block per table: lookups * tables.
    let rate = reads as f64 / (lookups * tables) as f64;
    eprintln!(
        "bloom: {lookups} lookups for missing keys over {tables} tables read {reads} blocks (without filters: {})",
        lookups * tables
    );
    assert!(
        rate < 0.02,
        "{reads} block reads for {lookups} missing keys over {tables} tables (rate {rate:.4})"
    );
    // And a key that IS there costs one block read (from the one table that holds it), not one per table.
    let before = db.stats().block_reads;
    for i in (0..30_000u32).step_by(100) {
        db.get(format!("present-{i}").as_bytes()).unwrap().unwrap();
    }
    let per_hit = (db.stats().block_reads - before) as f64 / 300.0;
    assert!(per_hit < 1.1, "{per_hit} block reads per present key");
}

#[test]
fn leftovers_from_an_interrupted_flush_are_removed() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = Db::open(dir.path(), small(1 << 20)).unwrap();
        db.put("a", "1").unwrap();
        db.flush().unwrap();
        db.put("b", "2").unwrap();
    }
    for stray in ["000900.sst", "000901.sst.tmp", "CURRENT.tmp", "000000.log"] {
        fs::write(dir.path().join(stray), b"junk").unwrap();
    }
    let db = Db::open(dir.path(), small(1 << 20)).unwrap();
    assert_eq!(db.recovery_report().leftovers_removed, 4);
    for stray in ["000900.sst", "000901.sst.tmp", "CURRENT.tmp", "000000.log"] {
        assert!(!dir.path().join(stray).exists(), "{stray}");
    }
    assert_eq!(db.get(b"a").unwrap().as_deref(), Some(&b"1"[..]));
    assert_eq!(db.get(b"b").unwrap().as_deref(), Some(&b"2"[..]));
}

#[test]
fn a_missing_or_shortened_table_stops_the_open() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = Db::open(dir.path(), small(1 << 20)).unwrap();
        db.put("a", "1").unwrap();
        db.flush().unwrap();
    }
    let table = fs::read_dir(dir.path())
        .unwrap()
        .map(|e| e.unwrap().path())
        .find(|p| p.extension().is_some_and(|e| e == "sst"))
        .unwrap();
    let data = fs::read(&table).unwrap();
    fs::write(&table, &data[..data.len() - 10]).unwrap();
    assert!(matches!(Db::open(dir.path(), small(1 << 20)), Err(Error::Corruption { .. })));
    fs::remove_file(&table).unwrap();
    assert!(Db::open(dir.path(), small(1 << 20)).is_err());
}

#[test]
fn damaged_current_stops_the_open() {
    let dir = tempfile::tempdir().unwrap();
    Db::open(dir.path(), small(1 << 20)).unwrap().put("a", "1").unwrap();
    let path = dir.path().join("CURRENT");
    let mut text = fs::read(&path).unwrap();
    text[25] ^= 0x01;
    fs::write(&path, &text).unwrap();
    assert!(matches!(Db::open(dir.path(), small(1 << 20)), Err(Error::Corruption { .. })));
}

#[test]
fn readers_and_writers_on_many_threads_with_flushes() {
    let dir = tempfile::tempdir().unwrap();
    let db = std::sync::Arc::new(Db::open(dir.path(), small(16 * 1024)).unwrap());
    let threads: Vec<_> = (0..8u32)
        .map(|t| {
            let db = db.clone();
            std::thread::spawn(move || {
                for i in 0..2_000u32 {
                    db.put(format!("t{t}-{i}"), format!("{t}:{i}")).unwrap();
                    // Read back something written earlier: it may be in the memtable, being flushed, or in a table.
                    let j = i / 2;
                    assert_eq!(
                        db.get(format!("t{t}-{j}").as_bytes()).unwrap().unwrap(),
                        format!("{t}:{j}").as_bytes()
                    );
                }
            })
        })
        .collect();
    for t in threads {
        t.join().unwrap();
    }
    assert!(db.stats().flushes > 10);
}

#[derive(Debug, Clone)]
enum Step {
    Put(u8, Vec<u8>),
    Delete(u8),
    Batch(Vec<(u8, Option<Vec<u8>>)>),
    Flush,
    Reopen,
}

fn step() -> impl Strategy<Value = Step> {
    let key = 0u8..24;
    let value = proptest::collection::vec(any::<u8>(), 0..60);
    prop_oneof![
        5 => (key.clone(), value.clone()).prop_map(|(k, v)| Step::Put(k, v)),
        2 => key.clone().prop_map(Step::Delete),
        2 => proptest::collection::vec((key, proptest::option::of(value)), 0..6).prop_map(Step::Batch),
        1 => Just(Step::Flush),
        1 => Just(Step::Reopen),
    ]
}

fn key(k: u8) -> Vec<u8> {
    format!("key-{k:02}").into_bytes()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(96))]

    /// With a tiny memtable (flushes happen constantly), any mix of writes, deletes, batches, flushes and
    /// reopenings reads exactly like a plain in-memory map.
    #[test]
    fn matches_a_simple_model_across_flushes(steps in proptest::collection::vec(step(), 1..120), memtable in 64usize..600) {
        let dir = tempfile::tempdir().unwrap();
        let mut db = Db::open(dir.path(), small(memtable)).unwrap();
        let mut model: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
        for s in steps {
            match s {
                Step::Put(k, v) => { db.put(key(k), v.clone()).unwrap(); model.insert(key(k), v); }
                Step::Delete(k) => { db.delete(key(k)).unwrap(); model.remove(&key(k)); }
                Step::Batch(ops) => {
                    let mut b = WriteBatch::new();
                    for (k, v) in ops {
                        match v {
                            Some(v) => { b.put(key(k), v.clone()); model.insert(key(k), v); }
                            None => { b.delete(key(k)); model.remove(&key(k)); }
                        }
                    }
                    db.write(&b).unwrap();
                }
                Step::Flush => db.flush().unwrap(),
                Step::Reopen => { drop(db); db = Db::open(dir.path(), small(memtable)).unwrap(); }
            }
            for k in 0..24u8 {
                prop_assert_eq!(db.get(&key(k)).unwrap(), model.get(&key(k)).cloned(), "key {}", k);
            }
        }
    }
}
