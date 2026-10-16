//! Phase 3: compaction merges tables down the levels.

use std::collections::BTreeMap;
use std::fs;

use proptest::prelude::*;
use stratakv::{Db, Options, SyncMode, WriteBatch};

/// A tiny tree: 4 KiB memtables and tables, level 1 holds 16 KiB, each deeper level 10x more.
fn tiny(background: bool) -> Options {
    Options {
        sync: SyncMode::Never,
        memtable_bytes: 4096,
        block_bytes: 512,
        target_file_bytes: 4096,
        level_base_bytes: 16 * 1024,
        background_compaction: background,
        ..Options::default()
    }
}

fn check_level_rules(db: &Db) {
    for (level, tables) in db.levels().iter().enumerate().skip(1) {
        for pair in tables.windows(2) {
            assert!(
                pair[0].largest < pair[1].smallest,
                "level {level}: tables {} and {} overlap",
                pair[0].id,
                pair[1].id
            );
        }
    }
}

#[test]
fn overwritten_data_stops_using_disk_space() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), tiny(true)).unwrap();
    let value = |round: u32, k: u32| format!("{round:04}-{k:04}-{}", "v".repeat(80));
    for round in 0..100u32 {
        for k in 0..200u32 {
            db.put(format!("key{k:04}"), value(round, k)).unwrap();
        }
    }
    db.wait_for_compactions();
    let s = db.stats();
    let live = 200 * (7 + value(0, 0).len()) as u64;
    let written = 100 * live;
    assert!(s.compactions > 10, "{s:?}");
    assert!(
        s.table_bytes < 6 * live,
        "{} bytes of tables for {live} bytes of live data ({written} written)",
        s.table_bytes
    );
    for k in 0..200u32 {
        assert_eq!(db.get(format!("key{k:04}").as_bytes()).unwrap().unwrap(), value(99, k).as_bytes());
    }
    check_level_rules(&db);
    eprintln!(
        "overwrites: {written} bytes written, {} bytes on disk after compaction ({} tables); write amplification {:.1}",
        s.table_bytes,
        s.tables,
        (s.flush_bytes_written + s.compaction_bytes_written) as f64 / written as f64
    );
}

#[test]
fn deleting_everything_and_compacting_leaves_nothing_on_disk() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = Db::open(dir.path(), tiny(false)).unwrap();
        for i in 0..5_000u32 {
            db.put(format!("key{i:05}"), vec![b'x'; 50]).unwrap();
        }
        db.compact().unwrap();
        assert!(db.stats().table_bytes > 200_000);
        for i in 0..5_000u32 {
            db.delete(format!("key{i:05}")).unwrap();
        }
        assert_eq!(db.get(b"key00042").unwrap(), None);
        db.compact_all().unwrap();
        let s = db.stats();
        assert_eq!((s.tables, s.table_bytes), (0, 0), "every value and every tombstone is gone: {s:?}");
    }
    let db = Db::open(dir.path(), tiny(false)).unwrap();
    assert_eq!(db.get(b"key00042").unwrap(), None);
    let ssts = fs::read_dir(dir.path())
        .unwrap()
        .filter(|e| e.as_ref().unwrap().path().extension().is_some_and(|x| x == "sst"))
        .count();
    assert_eq!(ssts, 0);
}

#[test]
fn a_tombstone_is_kept_while_an_older_value_exists_below() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), tiny(false)).unwrap();
    db.put("k", "old").unwrap();
    db.compact_all().unwrap(); // "old" now sits in level 1
    assert_eq!(db.levels()[1].len(), 1);
    db.delete("k").unwrap();
    for i in 0..300u32 {
        db.put(format!("filler{i:04}"), vec![b'f'; 40]).unwrap(); // flushes; the tombstone lands in level 0
    }
    db.compact().unwrap();
    assert_eq!(
        db.get(b"k").unwrap(),
        None,
        "the delete must still hide the old value after any compaction"
    );
    db.compact_all().unwrap();
    assert_eq!(db.get(b"k").unwrap(), None);
}

#[test]
fn levels_keep_their_shape_under_random_writes() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), tiny(true)).unwrap();
    let mut model = BTreeMap::new();
    let mut x: u64 = 0x9e37_79b9;
    for i in 0..30_000u32 {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        let key = format!("key{:06}", x % 20_000);
        if x.is_multiple_of(7) {
            db.delete(key.clone()).unwrap();
            model.remove(&key);
        } else {
            let v = format!("v{i}");
            db.put(key.clone(), v.clone()).unwrap();
            model.insert(key, v);
        }
    }
    db.wait_for_compactions();
    let s = db.stats();
    check_level_rules(&db);
    assert!(s.levels[0].tables < 4, "level 0 is compacted down: {:?}", s.levels);
    for (level, l) in s.levels.iter().enumerate().skip(1).take(5) {
        let limit = 16 * 1024u64 * 10u64.pow(level as u32 - 1);
        let deepest = s.levels.iter().rposition(|l| l.tables > 0).unwrap();
        if level < deepest {
            assert!(l.bytes <= limit, "level {level} is {} bytes, limit {limit}", l.bytes);
        }
    }
    // Reads cost at most one block per level that is in use (plus level 0 tables), whatever the key.
    let used_levels = s.levels.iter().skip(1).filter(|l| l.tables > 0).count() as u64 + s.levels[0].tables as u64;
    let before = db.stats().block_reads;
    for k in (0..20_000u64).step_by(37) {
        let key = format!("key{k:06}");
        assert_eq!(db.get(key.as_bytes()).unwrap().as_deref(), model.get(&key).map(|v| v.as_bytes()));
    }
    let per_get = (db.stats().block_reads - before) as f64 / (20_000 / 37 + 1) as f64;
    assert!(
        per_get <= used_levels as f64,
        "{per_get} block reads per lookup with {used_levels} levels in use"
    );
    eprintln!(
        "random writes: levels {:?}; {per_get:.2} block reads per lookup",
        s.levels.iter().map(|l| l.tables).collect::<Vec<_>>()
    );
    drop(db);
    let db = Db::open(dir.path(), tiny(true)).unwrap();
    for (k, v) in model.iter().step_by(13) {
        assert_eq!(db.get(k.as_bytes()).unwrap().as_deref(), Some(v.as_bytes()));
    }
}

#[test]
fn readers_and_writers_on_many_threads_while_compacting() {
    let dir = tempfile::tempdir().unwrap();
    let db = std::sync::Arc::new(Db::open(dir.path(), tiny(true)).unwrap());
    let threads: Vec<_> = (0..8u32)
        .map(|t| {
            let db = db.clone();
            std::thread::spawn(move || {
                for i in 0..3_000u32 {
                    db.put(format!("t{t}-{:05}", i % 700), format!("{t}:{i}")).unwrap();
                    // Each slot is overwritten every 700 writes: a read must see the newest value, wherever it
                    // lives (memtable, a table being compacted, or a new compaction output).
                    let got = db.get(format!("t{t}-{:05}", i % 700).as_bytes()).unwrap().unwrap();
                    assert_eq!(got, format!("{t}:{i}").as_bytes());
                    if i >= 700 {
                        let older_slot = (i + 1) % 700; // last written at i + 1 - 700
                        let got = db.get(format!("t{t}-{older_slot:05}").as_bytes()).unwrap().unwrap();
                        assert_eq!(got, format!("{t}:{}", i + 1 - 700).as_bytes());
                    }
                }
            })
        })
        .collect();
    for t in threads {
        t.join().unwrap();
    }
    db.wait_for_compactions();
    let s = db.stats();
    assert!(s.compactions > 0 && s.compaction_error.is_none(), "{s:?}");
    check_level_rules(&db);
    for t in 0..8u32 {
        for slot in 0..700u32 {
            let last_i = if 2_800 + slot < 3_000 { 2_800 + slot } else { 2_100 + slot }; // last i < 3000 with i % 700 == slot
            assert_eq!(
                db.get(format!("t{t}-{slot:05}").as_bytes()).unwrap().unwrap(),
                format!("{t}:{last_i}").as_bytes()
            );
        }
    }
}

#[test]
fn a_database_from_phase_two_opens_and_is_compacted() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = Db::open(dir.path(), tiny(false)).unwrap();
        for i in 0..2_000u32 {
            db.put(format!("key{i:05}"), format!("value{i}")).unwrap();
        }
        db.flush().unwrap();
        assert!(db.levels()[1..].iter().all(|l| l.is_empty()), "only level 0 so far");
    }
    // Rewrite CURRENT in the Phase 2 format: no levels.
    let path = dir.path().join("CURRENT");
    let text = fs::read_to_string(&path).unwrap();
    let mut body: String = text
        .lines()
        .filter(|l| !l.starts_with("crc "))
        .map(|l| {
            if l.starts_with("table ") {
                l.rsplit_once(' ').unwrap().0.to_string()
            } else {
                l.replace("stratakv-current 2", "stratakv-current 1")
            }
        })
        .collect::<Vec<_>>()
        .join("\n");
    body.push('\n');
    fs::write(&path, format!("{body}crc {:08x}\n", crc32fast::hash(body.as_bytes()))).unwrap();

    let db = Db::open(dir.path(), tiny(false)).unwrap();
    assert!(db.compact().unwrap() > 0);
    check_level_rules(&db);
    for i in (0..2_000u32).step_by(11) {
        assert_eq!(
            db.get(format!("key{i:05}").as_bytes()).unwrap().unwrap(),
            format!("value{i}").as_bytes()
        );
    }
    assert!(fs::read_to_string(&path).unwrap().starts_with("stratakv-current 2"));
}

#[test]
fn a_damaged_table_stops_compaction_but_not_writes() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = Db::open(dir.path(), tiny(false)).unwrap();
        for i in 0..2_000u32 {
            db.put(format!("key{i:05}"), vec![b'v'; 40]).unwrap();
        }
    }
    let victim = fs::read_dir(dir.path())
        .unwrap()
        .map(|e| e.unwrap().path())
        .find(|p| p.extension().is_some_and(|x| x == "sst"))
        .unwrap();
    let mut data = fs::read(&victim).unwrap();
    // Damage a later data block: the table still opens (it reads its first block and its index), and the
    // damage is found when compaction reads the block.
    data[1500] ^= 0xff;
    fs::write(&victim, &data).unwrap();

    let db = Db::open(dir.path(), tiny(true)).unwrap();
    db.wait_for_compactions();
    let err = db.stats().compaction_error.expect("the damaged block is noticed by compaction");
    assert!(err.contains("checksum"), "{err}");
    db.put("still", "writable").unwrap();
    assert_eq!(db.get(b"still").unwrap().as_deref(), Some(&b"writable"[..]));
}

#[derive(Debug, Clone)]
enum Step {
    Put(u8, Vec<u8>),
    Delete(u8),
    Batch(Vec<(u8, Option<Vec<u8>>)>),
    Flush,
    Compact,
    CompactAll,
    Reopen,
}

fn step() -> impl Strategy<Value = Step> {
    let key = 0u8..40;
    let value = proptest::collection::vec(any::<u8>(), 0..80);
    prop_oneof![
        8 => (key.clone(), value.clone()).prop_map(|(k, v)| Step::Put(k, v)),
        3 => key.clone().prop_map(Step::Delete),
        2 => proptest::collection::vec((key, proptest::option::of(value)), 0..8).prop_map(Step::Batch),
        1 => Just(Step::Flush),
        2 => Just(Step::Compact),
        1 => Just(Step::CompactAll),
        1 => Just(Step::Reopen),
    ]
}

fn key(k: u8) -> Vec<u8> {
    format!("key-{k:02}").into_bytes()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    /// Any mix of writes, deletes, batches, flushes, compactions and reopenings, on a tree tiny enough that
    /// every operation moves tables around, reads exactly like a plain in-memory map.
    #[test]
    fn matches_a_simple_model_across_compactions(steps in proptest::collection::vec(step(), 1..200), memtable in 100usize..800) {
        let dir = tempfile::tempdir().unwrap();
        let opts = Options { memtable_bytes: memtable, target_file_bytes: memtable as u64, level_base_bytes: 2 * memtable as u64, l0_compaction_trigger: 2, ..tiny(false) };
        let mut db = Db::open(dir.path(), opts.clone()).unwrap();
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
                Step::Compact => { db.compact().unwrap(); }
                Step::CompactAll => db.compact_all().unwrap(),
                Step::Reopen => { drop(db); db = Db::open(dir.path(), opts.clone()).unwrap(); }
            }
            for k in 0..40u8 {
                prop_assert_eq!(db.get(&key(k)).unwrap(), model.get(&key(k)).cloned(), "key {}", k);
            }
        }
        check_level_rules(&db);
    }
}
