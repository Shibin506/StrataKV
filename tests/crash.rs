//! Crash tests: no acknowledged write is ever lost, and no batch is ever half applied.
//!
//! 1. Random kills: a child process writes batches as fast as it can and prints `ack N` after each
//!    write returns. We SIGKILL it at a random moment, reopen the database, and check it against what
//!    was acknowledged. The same database is reused across rounds (recovery after recovery), and half
//!    the rounds use a small memtable so kills also land in the middle of flushes.
//! 2. Every step of a flush: the writer aborts at exactly one step of the flush protocol (see
//!    `stratakv::failpoint`), for each step in turn.
//!
//! Rounds: STRATAKV_CRASH_ROUNDS (default 60). What this does and does not prove: a killed process
//! leaves its written data in the OS page cache, so this tests process crashes. Power loss (data that
//! never left the cache) is simulated by the torn-write tests in tests/db.rs and src/wal.rs.

use std::io::{BufRead, BufReader};
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::time::Duration;

use stratakv::{Db, Options, SyncMode, WriteBatch};

fn value_for(n: u64) -> Vec<u8> {
    let len = 1 + (n.wrapping_mul(2_654_435_761) % 8192) as usize;
    (0..len).map(|i| (n as usize).wrapping_mul(31).wrapping_add(i) as u8).collect()
}

/// The batch the writer process writes as number `n`.
fn batch_for(n: u64) -> WriteBatch {
    let mut b = WriteBatch::new();
    let v = value_for(n);
    b.put(format!("k{n:010}a"), v.clone()).put(format!("k{n:010}b"), v);
    if n > 0 {
        b.delete(format!("last-{}", n - 1));
    }
    b.put(format!("last-{n}"), n.to_string());
    b
}

/// Small deterministic random numbers (no extra crate needed).
struct XorShift(u64);
impl XorShift {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
}

/// Checks the database against what the writer acknowledged and returns how many batches are committed.
/// Committed batches must be exactly 0..m with m >= acked; at most one unacknowledged batch may have
/// finished (written, but the process died before printing its ack).
fn verify(db: &Db, acked: u64, ctx: &str) -> u64 {
    let present = |n: u64| db.get(format!("k{n:010}a").as_bytes()).unwrap();
    let mut m = acked;
    while present(m).is_some() {
        m += 1;
    }
    assert!(m <= acked + 1, "{ctx}: {m} batches committed but only {acked} acknowledged");
    for n in 0..acked {
        assert_eq!(
            present(n).as_deref(),
            Some(&value_for(n)[..]),
            "{ctx}: acknowledged batch {n} lost or wrong"
        );
    }
    for n in 0..m {
        assert_eq!(
            db.get(format!("k{n:010}b").as_bytes()).unwrap().as_deref(),
            Some(&value_for(n)[..]),
            "{ctx}: batch {n} half applied"
        );
    }
    for n in m..m + 3 {
        assert_eq!(
            db.get(format!("k{n:010}b").as_bytes()).unwrap(),
            None,
            "{ctx}: batch {n} half applied"
        );
    }
    // Deletes are part of the same batches: only the newest `last-*` key may exist.
    if m >= 1 {
        assert_eq!(
            db.get(format!("last-{}", m - 1).as_bytes()).unwrap().as_deref(),
            Some((m - 1).to_string().as_bytes()),
            "{ctx}"
        );
    }
    if m >= 2 {
        assert_eq!(db.get(format!("last-{}", m - 2).as_bytes()).unwrap(), None, "{ctx}: delete lost");
    }
    m
}

#[test]
fn no_acknowledged_write_is_lost_when_the_process_is_killed() {
    let rounds: u64 = std::env::var("STRATAKV_CRASH_ROUNDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(60);
    let dir = tempfile::tempdir().unwrap();
    let mut rng = XorShift(0x5eed_1234_abcd_0001);
    let mut next = 0u64; // first batch number the next writer will use
    let (mut checked, mut torn_seen, mut leftovers_seen) = (0u64, 0u64, 0u64);

    for round in 0..rounds {
        let sync = if round % 2 == 0 { "always" } else { "never" };
        let memtable = if round % 4 < 2 { "32768" } else { "4194304" };
        let mut child = Command::new(env!("CARGO_BIN_EXE_stratakv-crashwriter"))
            .args([dir.path().to_str().unwrap(), &next.to_string(), sync, memtable])
            .stdout(Stdio::piped())
            .spawn()
            .expect("spawn writer");
        let stdout = child.stdout.take().unwrap();
        let (tx, rx) = mpsc::channel();
        let reader = std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                let Ok(line) = line else { break };
                let n: u64 = line.strip_prefix("ack ").unwrap().parse().unwrap();
                if tx.send(n).is_err() {
                    break;
                }
            }
        });
        // Wait for the first acknowledgement (so the writer is really writing), then kill at a random moment.
        let first = rx.recv_timeout(Duration::from_secs(20)).expect("writer never acknowledged a write");
        std::thread::sleep(Duration::from_micros(rng.next() % 30_000));
        child.kill().unwrap(); // SIGKILL on Unix
        child.wait().unwrap();
        reader.join().unwrap();
        let last_acked = rx.try_iter().last().unwrap_or(first); // what was left in the pipe was acknowledged too
        let acked = last_acked + 1;

        let db = Db::open(
            dir.path(),
            Options {
                sync: SyncMode::Never,
                ..Options::default()
            },
        )
        .expect("reopen after kill");
        torn_seen += (db.recovery_report().torn_bytes > 0) as u64;
        leftovers_seen += (db.recovery_report().leftovers_removed > 0) as u64;
        next = verify(&db, acked, &format!("round {round}"));
        checked = acked;
    }
    eprintln!(
        "random kills: {rounds} kills, {checked} acknowledged batches checked, {torn_seen} recoveries removed a torn tail, \
         {leftovers_seen} cleaned up after an interrupted flush or compaction"
    );
}

/// Crashes the writer at each step of the flush and compaction protocols, then checks that nothing acknowledged is lost,
/// no leftover file survives the next open, and the database keeps working.
#[test]
fn a_crash_at_every_step_of_a_flush_or_compaction_loses_nothing() {
    for (name, _) in stratakv::failpoint::ALL {
        for sync in ["always", "never"] {
            let dir = tempfile::tempdir().unwrap();
            let out = Command::new(env!("CARGO_BIN_EXE_stratakv-crashwriter"))
                .args([dir.path().to_str().unwrap(), "0", sync, "65536", name])
                .stderr(Stdio::null())
                .output()
                .expect("run writer");
            let ctx = format!("{name} ({sync})");
            assert!(!out.status.success(), "{ctx}: the writer should have crashed at the crash point");
            let acked = String::from_utf8_lossy(&out.stdout).lines().count() as u64;
            assert!(acked > 2, "{ctx}: the writer crashed before writing anything");

            // No background compaction while inspecting the directory (a compaction in progress has
            // temporary files that are not leftovers); compactions are run explicitly below.
            let small = Options {
                sync: SyncMode::Never,
                memtable_bytes: 16384,
                target_file_bytes: 16384,
                level_base_bytes: 65536,
                background_compaction: false,
                ..Options::default()
            };
            let db = Db::open(dir.path(), small.clone()).unwrap_or_else(|e| panic!("{ctx}: reopen failed: {e}"));
            let m = verify(&db, acked, &ctx);

            // Leftovers are gone: no temporary files and every table on disk is listed in CURRENT.
            let names: Vec<String> = std::fs::read_dir(dir.path())
                .unwrap()
                .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
                .collect();
            assert!(!names.iter().any(|n| n.ends_with(".tmp")), "{ctx}: {names:?}");
            assert_eq!(
                names.iter().filter(|n| n.ends_with(".sst")).count(),
                db.stats().tables,
                "{ctx}: {names:?}"
            );

            // The database keeps working: write more (with flushes), reopen, and everything is there.
            for n in m..m + 200 {
                db.write(&batch_for(n)).unwrap();
            }
            db.compact().unwrap();
            drop(db);
            let db = Db::open(dir.path(), small).unwrap();
            verify(&db, m + 200, &ctx);
            let logs = std::fs::read_dir(dir.path())
                .unwrap()
                .filter(|e| e.as_ref().unwrap().file_name().to_string_lossy().ends_with(".log"))
                .count();
            assert_eq!(logs, 1, "{ctx}: old logs are removed once a flush completes");
        }
    }
}
