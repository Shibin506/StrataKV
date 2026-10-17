//! Phase 4: range scans and snapshots.

use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::Arc;
use std::thread;

use proptest::prelude::*;
use stratakv::{Db, Options, SyncMode, WriteBatch};

/// A tiny tree (4 KiB memtables and tables), so a few hundred writes already spread over several levels.
fn tiny() -> Options {
    Options {
        sync: SyncMode::Never,
        memtable_bytes: 4096,
        block_bytes: 512,
        target_file_bytes: 4096,
        level_base_bytes: 16 * 1024,
        background_compaction: false,
        ..Options::default()
    }
}

fn key(i: u32) -> Vec<u8> {
    format!("key{i:05}").into_bytes()
}

fn all(scan: stratakv::Scan) -> Vec<(Vec<u8>, Vec<u8>)> {
    scan.collect::<Result<_, _>>().unwrap()
}

fn strs(pairs: &[(&str, &str)]) -> Vec<(Vec<u8>, Vec<u8>)> {
    pairs.iter().map(|(k, v)| (k.as_bytes().to_vec(), v.as_bytes().to_vec())).collect()
}

#[test]
fn a_scan_returns_the_latest_values_in_key_order() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), tiny()).unwrap();
    for (k, v) in [("c", "3"), ("a", "1"), ("e", "5"), ("b", "2"), ("d", "4")] {
        db.put(k, v).unwrap();
    }
    db.put("b", "2b").unwrap();
    db.delete("d").unwrap();
    assert_eq!(
        all(db.scan::<&str, _>(..).unwrap()),
        strs(&[("a", "1"), ("b", "2b"), ("c", "3"), ("e", "5")])
    );
    assert_eq!(all(db.scan("b".."e").unwrap()), strs(&[("b", "2b"), ("c", "3")]));
    assert_eq!(all(db.scan("b"..="e").unwrap()), strs(&[("b", "2b"), ("c", "3"), ("e", "5")]));
    assert_eq!(all(db.scan("bb"..).unwrap()), strs(&[("c", "3"), ("e", "5")]));
    assert_eq!(all(db.scan(.."c").unwrap()), strs(&[("a", "1"), ("b", "2b")]));
    assert_eq!(all(db.scan("x"..).unwrap()), vec![]);
    assert_eq!(
        all(db.scan::<&str, _>((Bound::Excluded("b"), Bound::Excluded("e"))).unwrap()),
        strs(&[("c", "3")])
    );
}

#[test]
fn scans_see_the_same_thing_in_memory_on_disk_and_after_compaction() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), tiny()).unwrap();
    let mut model = BTreeMap::new();
    for round in 0..6u32 {
        for i in 0..300u32 {
            let k = key((i * 7 + round * 13) % 400);
            if (i + round) % 5 == 0 {
                db.delete(k.clone()).unwrap();
                model.remove(&k);
            } else {
                let v = format!("v{round}-{i}").into_bytes();
                db.put(k.clone(), v.clone()).unwrap();
                model.insert(k, v);
            }
        }
        let want: Vec<_> = model.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        assert_eq!(all(db.scan::<&[u8], _>(..).unwrap()), want, "round {round}, before compaction");
        db.compact().unwrap();
        assert_eq!(all(db.scan::<&[u8], _>(..).unwrap()), want, "round {round}, after compaction");
        let lo = key(100);
        let hi = key(250);
        let want: Vec<_> = model.range(lo.clone()..hi.clone()).map(|(k, v)| (k.clone(), v.clone())).collect();
        assert_eq!(all(db.scan(lo..hi).unwrap()), want, "round {round}, a middle range");
    }
    assert!(db.stats().tables > 1, "the data really is spread over several tables");
    db.compact_all().unwrap();
    let want: Vec<_> = model.into_iter().collect();
    assert_eq!(all(db.scan::<&[u8], _>(..).unwrap()), want);
}

#[test]
fn scan_prefix_covers_exactly_the_keys_with_that_prefix() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), tiny()).unwrap();
    for k in ["user:1", "user:2", "user:10", "usera", "user", "use", "order:1", "user:\u{ff}"] {
        db.put(k, "x").unwrap();
    }
    db.put(b"p\xff\xff".to_vec(), "y").unwrap();
    db.put(b"p\xff\xffz".to_vec(), "y").unwrap();
    db.put(b"q".to_vec(), "y").unwrap();
    let keys = |s: stratakv::Scan| all(s).into_iter().map(|(k, _)| k).collect::<Vec<_>>();
    assert_eq!(
        keys(db.scan_prefix("user:").unwrap()),
        vec![
            b"user:1".to_vec(),
            b"user:10".to_vec(),
            b"user:2".to_vec(),
            "user:\u{ff}".as_bytes().to_vec()
        ]
    );
    assert_eq!(keys(db.scan_prefix("user").unwrap()).len(), 6);
    assert_eq!(
        keys(db.scan_prefix(b"p\xff\xff").unwrap()),
        vec![b"p\xff\xff".to_vec(), b"p\xff\xffz".to_vec()],
        "a prefix ending in 0xff"
    );
    assert_eq!(keys(db.scan_prefix("").unwrap()).len(), 11, "the empty prefix is everything");
}

#[test]
fn a_snapshot_keeps_seeing_the_past_through_overwrites_deletes_flushes_and_compactions() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), tiny()).unwrap();
    for i in 0..200 {
        db.put(key(i), format!("old{i}")).unwrap();
    }
    let snap = db.snapshot();
    for round in 0..5 {
        for i in 0..200 {
            if i % 3 == 0 {
                db.delete(key(i)).unwrap();
            } else {
                db.put(key(i), format!("new{round}-{i}")).unwrap();
            }
        }
        db.compact().unwrap();
    }
    db.compact_all().unwrap();
    for i in 0..200 {
        assert_eq!(
            snap.get(&key(i)).unwrap(),
            Some(format!("old{i}").into_bytes()),
            "the snapshot still sees key {i}"
        );
        let latest = if i % 3 == 0 { None } else { Some(format!("new4-{i}").into_bytes()) };
        assert_eq!(db.get(&key(i)).unwrap(), latest);
    }
    let seen = all(snap.scan::<&[u8], _>(..).unwrap());
    assert_eq!(seen.len(), 200);
    assert!(
        seen.iter()
            .enumerate()
            .all(|(i, (k, v))| *k == key(i as u32) && *v == format!("old{i}").into_bytes())
    );
}

#[test]
fn a_snapshot_does_not_see_keys_written_after_it() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), tiny()).unwrap();
    db.put("a", "1").unwrap();
    db.delete("gone").unwrap();
    let snap = db.snapshot();
    db.put("b", "2").unwrap();
    db.put("gone", "back").unwrap();
    db.delete("a").unwrap();
    db.flush().unwrap();
    assert_eq!(snap.get(b"b").unwrap(), None);
    assert_eq!(snap.get(b"gone").unwrap(), None);
    assert_eq!(snap.get(b"a").unwrap(), Some(b"1".to_vec()));
    assert_eq!(all(snap.scan::<&str, _>(..).unwrap()), strs(&[("a", "1")]));
    assert_eq!(all(db.scan::<&str, _>(..).unwrap()), strs(&[("b", "2"), ("gone", "back")]));
    assert_eq!(snap.sequence() + 3, db.last_sequence());
}

#[test]
fn old_versions_are_reclaimed_once_the_last_snapshot_is_dropped() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), tiny()).unwrap();
    let value = "v".repeat(100);
    for i in 0..100 {
        db.put(key(i), value.clone()).unwrap();
    }
    let snap = db.snapshot();
    let twin = snap.clone();
    for i in 0..100 {
        db.delete(key(i)).unwrap();
    }
    db.compact_all().unwrap();
    let held = db.stats().table_bytes;
    assert!(held > 100 * 100, "the old values are kept for the snapshots: {held} bytes");
    assert_eq!(db.stats().snapshots, 2, "two open handles on one sequence number");

    drop(snap);
    db.compact_all().unwrap();
    assert!(db.stats().table_bytes > 100 * 100, "the clone still holds them");
    assert_eq!(twin.get(&key(7)).unwrap(), Some(value.clone().into_bytes()));

    drop(twin);
    assert_eq!(db.stats().snapshots, 0);
    db.compact_all().unwrap();
    assert_eq!(db.stats().table_bytes, 0, "nothing is live and nothing can see the old values");
}

#[test]
fn a_scan_is_not_disturbed_by_writes_made_while_it_runs() {
    // With background threads, the memtable the scan is reading is frozen, flushed and compacted away meanwhile.
    for background in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let db = Db::open(
            dir.path(),
            Options {
                background_compaction: background,
                ..tiny()
            },
        )
        .unwrap();
        for i in 0..500 {
            db.put(key(i), "before").unwrap();
        }
        let mut scan = db.scan::<&[u8], _>(..).unwrap();
        let first = scan.next().unwrap().unwrap();
        let flushes = db.stats().flushes;
        for round in 0..20 {
            for i in 0..500 {
                db.put(key(i), format!("after {round}")).unwrap();
            }
        }
        db.put("zzz", "new key").unwrap();
        db.wait_for_compactions();
        db.compact_all().unwrap();
        assert!(db.stats().flushes > flushes + 10, "many flushes happened while the scan was open");
        let rest: Vec<_> = scan.collect::<Result<_, _>>().unwrap();
        assert_eq!(first, (key(0), b"before".to_vec()));
        assert_eq!(rest.len(), 499, "background {background}");
        assert!(
            rest.iter().all(|(_, v)| v == b"before"),
            "the scan reads the moment it started (background {background})"
        );
        assert_eq!(db.get(&key(3)).unwrap(), Some(b"after 19".to_vec()));
    }
}

#[test]
fn snapshots_see_whole_batches_or_nothing_under_concurrent_writers() {
    // Each batch moves 1 from one account to another, so the sum is always 1000 in any consistent view.
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(
        Db::open(
            dir.path(),
            Options {
                background_compaction: true,
                ..tiny()
            },
        )
        .unwrap(),
    );
    let accounts = 10u32;
    for i in 0..accounts {
        db.put(key(i), 100u64.to_le_bytes()).unwrap();
    }
    let read = |v: Vec<u8>| u64::from_le_bytes(v.try_into().unwrap());
    let writers: Vec<_> = (0..4u32)
        .map(|t| {
            let db = db.clone();
            thread::spawn(move || {
                for n in 0..300u32 {
                    let (from, to) = ((n + t) % 10, (n * 3 + t + 1) % 10);
                    if from == to {
                        continue;
                    }
                    // Read-modify-write is not atomic across threads, so each thread owns its own transfer:
                    // it moves 1 from `from` to `to` by writing both new values in one batch under a lock.
                    let _guard = LOCK.lock().unwrap();
                    let a = u64::from_le_bytes(db.get(&key(from)).unwrap().unwrap().try_into().unwrap());
                    let b = u64::from_le_bytes(db.get(&key(to)).unwrap().unwrap().try_into().unwrap());
                    if a == 0 {
                        continue;
                    }
                    let mut batch = WriteBatch::new();
                    batch.put(key(from), (a - 1).to_le_bytes());
                    batch.put(key(to), (b + 1).to_le_bytes());
                    db.write(&batch).unwrap();
                }
            })
        })
        .collect();
    let mut views = 0;
    while writers.iter().any(|w| !w.is_finished()) || views < 50 {
        let snap = db.snapshot();
        let by_get: u64 = (0..accounts).map(|i| read(snap.get(&key(i)).unwrap().unwrap())).sum();
        let by_scan: u64 = all(snap.scan::<&[u8], _>(..).unwrap()).into_iter().map(|(_, v)| read(v)).sum();
        let live: u64 = all(db.scan::<&[u8], _>(..).unwrap()).into_iter().map(|(_, v)| read(v)).sum();
        assert_eq!((by_get, by_scan, live), (1000, 1000, 1000), "view {views}");
        views += 1;
    }
    for w in writers {
        w.join().unwrap();
    }
    db.wait_for_compactions();
    assert_eq!(db.stats().compaction_error, None);
}

static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[test]
fn snapshots_end_when_the_database_is_reopened() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = Db::open(dir.path(), tiny()).unwrap();
        db.put("a", "1").unwrap();
        let _snap = db.snapshot();
        db.put("a", "2").unwrap();
    }
    let db = Db::open(dir.path(), tiny()).unwrap();
    assert_eq!(db.stats().snapshots, 0);
    assert_eq!(db.get(b"a").unwrap(), Some(b"2".to_vec()));
}

#[derive(Debug, Clone)]
enum Step {
    Put(u8, Vec<u8>),
    Delete(u8),
    Batch(Vec<(u8, Option<Vec<u8>>)>),
    Flush,
    Compact,
    CompactAll,
    TakeSnapshot,
    DropSnapshot(usize),
}

fn step() -> impl Strategy<Value = Step> {
    let value = proptest::collection::vec(any::<u8>(), 0..60);
    prop_oneof![
        8 => (0..40u8, value.clone()).prop_map(|(k, v)| Step::Put(k, v)),
        3 => (0..40u8).prop_map(Step::Delete),
        2 => proptest::collection::vec((0..40u8, proptest::option::of(value)), 1..8).prop_map(Step::Batch),
        1 => Just(Step::Flush),
        1 => Just(Step::Compact),
        1 => Just(Step::CompactAll),
        2 => Just(Step::TakeSnapshot),
        1 => any::<usize>().prop_map(Step::DropSnapshot),
    ]
}

type Model = BTreeMap<Vec<u8>, Vec<u8>>;

fn model_key(k: u8) -> Vec<u8> {
    format!("k{k:02}").into_bytes()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    /// Every open snapshot reads exactly like a copy of the model taken when it was opened, and scans of
    /// random ranges match the model, through any mix of writes, flushes and compactions.
    #[test]
    fn snapshots_and_scans_match_copies_of_a_simple_model(steps in proptest::collection::vec(step(), 1..150), memtable in 100usize..800, lo in 0..40u8, len in 0..40u8) {
        let dir = tempfile::tempdir().unwrap();
        let opts = Options { memtable_bytes: memtable, target_file_bytes: memtable as u64, level_base_bytes: 2 * memtable as u64, l0_compaction_trigger: 2, ..tiny() };
        let db = Db::open(dir.path(), opts).unwrap();
        let mut model: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
        let mut snaps: Vec<(stratakv::Snapshot, Model)> = Vec::new();
        let (start, end) = (model_key(lo), model_key(lo.saturating_add(len)));
        for s in steps {
            match s {
                Step::Put(k, v) => { db.put(model_key(k), v.clone()).unwrap(); model.insert(model_key(k), v); }
                Step::Delete(k) => { db.delete(model_key(k)).unwrap(); model.remove(&model_key(k)); }
                Step::Batch(ops) => {
                    let mut b = WriteBatch::new();
                    for (k, v) in ops {
                        match v {
                            Some(v) => { b.put(model_key(k), v.clone()); model.insert(model_key(k), v); }
                            None => { b.delete(model_key(k)); model.remove(&model_key(k)); }
                        }
                    }
                    db.write(&b).unwrap();
                }
                Step::Flush => db.flush().unwrap(),
                Step::Compact => { db.compact().unwrap(); }
                Step::CompactAll => db.compact_all().unwrap(),
                Step::TakeSnapshot => snaps.push((db.snapshot(), model.clone())),
                Step::DropSnapshot(i) => { if !snaps.is_empty() { let i = i % snaps.len(); snaps.remove(i); } }
            }
            let want: Vec<_> = model.range(start.clone()..end.clone()).map(|(k, v)| (k.clone(), v.clone())).collect();
            prop_assert_eq!(all(db.scan(start.clone()..end.clone()).unwrap()), want);
            for (snap, copy) in &snaps {
                for k in 0..40u8 {
                    prop_assert_eq!(snap.get(&model_key(k)).unwrap(), copy.get(&model_key(k)).cloned(), "snapshot at {}, key {}", snap.sequence(), k);
                }
                let want: Vec<_> = copy.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
                prop_assert_eq!(all(snap.scan::<&[u8], _>(..).unwrap()), want);
            }
        }
        drop(snaps);
        db.compact_all().unwrap();
        let want: Vec<_> = model.into_iter().collect();
        prop_assert_eq!(all(db.scan::<&[u8], _>(..).unwrap()), want);
    }
}
