use std::collections::BTreeMap;
use std::fs;

use proptest::prelude::*;
use stratakv::{Db, Error, Options, SyncMode, WriteBatch};

fn opts(sync: SyncMode) -> Options {
    Options {
        sync,
        ..Options::default()
    }
}

#[test]
fn put_get_overwrite_delete() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), Options::default()).unwrap();
    assert_eq!(db.get(b"a").unwrap(), None);
    db.put("a", "1").unwrap();
    db.put("a", "2").unwrap();
    db.put("", "empty key").unwrap();
    db.put("b", vec![0u8; 0]).unwrap();
    assert_eq!(db.get(b"a").unwrap().as_deref(), Some(&b"2"[..]));
    assert_eq!(db.get(b"").unwrap().as_deref(), Some(&b"empty key"[..]));
    assert_eq!(
        db.get(b"b").unwrap().as_deref(),
        Some(&b""[..]),
        "an empty value is not the same as no value"
    );
    db.delete("a").unwrap();
    db.delete("never-existed").unwrap();
    assert_eq!(db.get(b"a").unwrap(), None);
}

#[test]
fn everything_survives_reopening() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = Db::open(dir.path(), Options::default()).unwrap();
        for i in 0..500u32 {
            db.put(format!("k{i}"), i.to_le_bytes().to_vec()).unwrap();
        }
        for i in (0..500u32).step_by(3) {
            db.delete(format!("k{i}")).unwrap();
        }
    }
    let db = Db::open(dir.path(), Options::default()).unwrap();
    assert_eq!(db.recovery_report().batches, 500 + 167);
    assert_eq!(db.recovery_report().torn_bytes, 0);
    for i in 0..500u32 {
        let want = (i % 3 != 0).then(|| i.to_le_bytes().to_vec());
        assert_eq!(db.get(format!("k{i}").as_bytes()).unwrap(), want, "k{i}");
    }
    assert_eq!(db.last_sequence(), 667);
}

#[test]
fn a_second_open_of_the_same_directory_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), Options::default()).unwrap();
    assert!(matches!(Db::open(dir.path(), Options::default()), Err(Error::Locked(_))));
    drop(db);
    Db::open(dir.path(), Options::default()).unwrap();
}

#[test]
fn missing_directory_is_created_unless_told_not_to() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("a/b/db");
    assert!(matches!(
        Db::open(
            &path,
            Options {
                create_if_missing: false,
                ..Options::default()
            }
        ),
        Err(Error::InvalidArgument(_))
    ));
    Db::open(&path, Options::default()).unwrap().put("x", "y").unwrap();
}

#[test]
fn a_rejected_write_changes_nothing_and_does_not_block_later_writes() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), Options::default()).unwrap();
    let mut b = WriteBatch::new();
    b.put("ok", "1").put(vec![b'k'; stratakv::batch::MAX_KEY_LEN + 1], "too long");
    assert!(matches!(db.write(&b), Err(Error::InvalidArgument(_))));
    assert_eq!(db.get(b"ok").unwrap(), None, "a rejected batch is not partly applied");
    db.put("after", "fine").unwrap();
    assert!(db.wal_bytes() > 0);
}

#[test]
fn a_batch_cut_by_a_crash_is_all_or_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let before;
    {
        let db = Db::open(dir.path(), Options::default()).unwrap();
        db.put("first", "1").unwrap();
        before = db.wal_bytes();
        let mut b = WriteBatch::new();
        for i in 0..100 {
            b.put(format!("batch{i}"), vec![b'v'; 100]);
        }
        db.write(&b).unwrap();
    }
    let wal = dir.path().join("wal.log");
    let full = fs::read(&wal).unwrap();
    for cut in [
        before as usize + 1,
        before as usize + 9,
        (before as usize + full.len()) / 2,
        full.len() - 1,
    ] {
        fs::write(&wal, &full[..cut]).unwrap();
        let db = Db::open(dir.path(), Options::default()).unwrap();
        assert_eq!(db.get(b"first").unwrap().as_deref(), Some(&b"1"[..]));
        for i in 0..100 {
            assert_eq!(
                db.get(format!("batch{i}").as_bytes()).unwrap(),
                None,
                "cut {cut}: half a batch must never appear"
            );
        }
        assert_eq!(db.recovery_report().torn_bytes as usize, cut - before as usize);
        assert_eq!(fs::metadata(&wal).unwrap().len(), before, "the torn tail is removed from the file");
    }
}

#[test]
fn damage_in_the_middle_of_the_log_stops_the_open() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = Db::open(dir.path(), Options::default()).unwrap();
        for i in 0..10 {
            db.put(format!("k{i}"), "value").unwrap();
        }
    }
    let wal = dir.path().join("wal.log");
    let mut data = fs::read(&wal).unwrap();
    data[30] ^= 0x10;
    fs::write(&wal, &data).unwrap();
    match Db::open(dir.path(), Options::default()) {
        Err(Error::Corruption { offset, .. }) => assert_eq!(offset, 0),
        other => panic!("expected corruption, got {:?}", other.map(|_| ())),
    }
}

#[test]
fn readers_and_writers_on_many_threads() {
    let dir = tempfile::tempdir().unwrap();
    let db = std::sync::Arc::new(Db::open(dir.path(), opts(SyncMode::Never)).unwrap());
    let threads: Vec<_> = (0..8u32)
        .map(|t| {
            let db = db.clone();
            std::thread::spawn(move || {
                for i in 0..500u32 {
                    db.put(format!("t{t}-{i}"), format!("{t}:{i}")).unwrap();
                    assert_eq!(
                        db.get(format!("t{t}-{i}").as_bytes()).unwrap().unwrap(),
                        format!("{t}:{i}").as_bytes()
                    );
                }
            })
        })
        .collect();
    for t in threads {
        t.join().unwrap();
    }
    assert_eq!(db.last_sequence(), 8 * 500);
    drop(db);
    let db = Db::open(dir.path(), Options::default()).unwrap();
    assert_eq!(db.get(b"t7-499").unwrap().as_deref(), Some(&b"7:499"[..]));
}

// ---------- model-based property tests ----------

#[derive(Debug, Clone)]
enum Step {
    Put(u8, Vec<u8>),
    Delete(u8),
    Batch(Vec<(u8, Option<Vec<u8>>)>),
    Reopen,
}

fn step() -> impl Strategy<Value = Step> {
    let key = 0u8..16;
    let value = proptest::collection::vec(any::<u8>(), 0..40);
    prop_oneof![
        4 => (key.clone(), value.clone()).prop_map(|(k, v)| Step::Put(k, v)),
        2 => key.clone().prop_map(Step::Delete),
        2 => proptest::collection::vec((key, proptest::option::of(value)), 0..6).prop_map(Step::Batch),
        1 => Just(Step::Reopen),
    ]
}

fn key(k: u8) -> Vec<u8> {
    format!("key-{k}").into_bytes()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    /// After any sequence of writes, deletes, batches and reopenings, every key reads exactly what a
    /// plain in-memory map says it should.
    #[test]
    fn matches_a_simple_model(steps in proptest::collection::vec(step(), 1..60)) {
        let dir = tempfile::tempdir().unwrap();
        let mut db = Db::open(dir.path(), opts(SyncMode::Never)).unwrap();
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
                Step::Reopen => { drop(db); db = Db::open(dir.path(), opts(SyncMode::Never)).unwrap(); }
            }
            for k in 0..16u8 {
                prop_assert_eq!(db.get(&key(k)).unwrap(), model.get(&key(k)).cloned());
            }
        }
    }

    /// Cutting the log at ANY byte gives exactly the state after the batches that were whole.
    #[test]
    fn any_cut_of_the_log_gives_a_prefix_of_the_batches(
        batches in proptest::collection::vec(proptest::collection::vec((0u8..8, proptest::option::of(proptest::collection::vec(any::<u8>(), 0..30))), 1..5), 1..20),
        cut_fraction in 0.0f64..=1.0,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let mut states = vec![BTreeMap::new()];
        let mut ends = vec![0u64];
        {
            let db = Db::open(dir.path(), opts(SyncMode::Never)).unwrap();
            let mut model: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
            for ops in &batches {
                let mut b = WriteBatch::new();
                for (k, v) in ops {
                    match v {
                        Some(v) => { b.put(key(*k), v.clone()); model.insert(key(*k), v.clone()); }
                        None => { b.delete(key(*k)); model.remove(&key(*k)); }
                    }
                }
                db.write(&b).unwrap();
                states.push(model.clone());
                ends.push(db.wal_bytes());
            }
        }
        let wal = dir.path().join("wal.log");
        let full = fs::read(&wal).unwrap();
        let cut = ((full.len() as f64) * cut_fraction) as usize;
        fs::write(&wal, &full[..cut]).unwrap();
        let whole = ends.iter().filter(|&&e| e as usize <= cut).count() - 1;
        let db = Db::open(dir.path(), opts(SyncMode::Never)).unwrap();
        for k in 0..8u8 {
            prop_assert_eq!(db.get(&key(k)).unwrap(), states[whole].get(&key(k)).cloned());
        }
    }
}
