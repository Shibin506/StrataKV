//! Kill -9 crash test: no acknowledged write is ever lost, and no batch is ever half applied.
//!
//! A child process writes batches as fast as it can and prints `ack N` after each write returns.
//! We SIGKILL it at a random moment, reopen the database, and check it against what was acknowledged.
//! The same database is reused across rounds, so recovery after recovery (and appending after a
//! removed torn tail) is tested too.
//!
//! Rounds: STRATAKV_CRASH_ROUNDS (default 60). What this does and does not prove: a killed process
//! leaves its written data in the OS page cache, so this tests process crashes. Power loss (data
//! that never left the cache) is simulated by the torn-write tests in tests/db.rs and src/wal.rs.

use std::io::{BufRead, BufReader};
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::time::Duration;

use stratakv::{Db, Options, SyncMode};

fn value_for(n: u64) -> Vec<u8> {
    let len = 1 + (n.wrapping_mul(2_654_435_761) % 8192) as usize;
    (0..len).map(|i| (n as usize).wrapping_mul(31).wrapping_add(i) as u8).collect()
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

#[test]
fn no_acknowledged_write_is_lost_when_the_process_is_killed() {
    let rounds: u64 = std::env::var("STRATAKV_CRASH_ROUNDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(60);
    let dir = tempfile::tempdir().unwrap();
    let mut rng = XorShift(0x5eed_1234_abcd_0001);
    let mut next = 0u64; // first batch number the next writer will use
    let (mut total_acked, mut torn_seen) = (0u64, 0u64);

    for round in 0..rounds {
        let sync = if round % 2 == 0 { "always" } else { "never" };
        let mut child = Command::new(env!("CARGO_BIN_EXE_stratakv-crashwriter"))
            .args([dir.path().to_str().unwrap(), &next.to_string(), sync])
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
        let _ = rx.recv_timeout(Duration::from_secs(20)).expect("writer never acknowledged a write");
        std::thread::sleep(Duration::from_micros(rng.next() % 30_000));
        child.kill().unwrap(); // SIGKILL on Unix
        child.wait().unwrap();
        reader.join().unwrap();
        let last_acked = rx.try_iter().last().unwrap_or(next); // everything left in the pipe was acknowledged too
        let acked_count = last_acked + 1;

        let db = Db::open(
            dir.path(),
            Options {
                sync: SyncMode::Never,
                ..Options::default()
            },
        )
        .expect("reopen after kill");
        torn_seen += (db.recovery_report().torn_bytes > 0) as u64;

        // Committed batches must be exactly 0..m with m >= acked_count; at most one unacknowledged batch
        // may have finished (written, but killed before printing its ack).
        let present = |n: u64| db.get(format!("k{n:010}a").as_bytes()).unwrap();
        let mut m = acked_count;
        while present(m).is_some() {
            m += 1;
        }
        assert!(
            m <= acked_count + 1,
            "round {round}: {} batches committed but only {acked_count} acknowledged",
            m
        );
        for n in 0..acked_count {
            let a = db.get(format!("k{n:010}a").as_bytes()).unwrap();
            assert_eq!(
                a.as_deref(),
                Some(&value_for(n)[..]),
                "round {round}: acknowledged batch {n} lost or wrong"
            );
        }
        for n in 0..m {
            assert_eq!(
                db.get(format!("k{n:010}b").as_bytes()).unwrap().as_deref(),
                Some(&value_for(n)[..]),
                "round {round}: batch {n} half applied"
            );
        }
        for n in m..m + 3 {
            assert_eq!(
                db.get(format!("k{n:010}b").as_bytes()).unwrap(),
                None,
                "round {round}: batch {n} half applied"
            );
        }
        // Deletes are part of the same batches: only the newest `last-*` key may exist.
        assert_eq!(
            db.get(format!("last-{}", m - 1).as_bytes()).unwrap().as_deref(),
            Some((m - 1).to_string().as_bytes())
        );
        if m >= 2 {
            assert_eq!(
                db.get(format!("last-{}", m - 2).as_bytes()).unwrap(),
                None,
                "round {round}: delete lost"
            );
        }
        total_acked = acked_count;
        next = m;
    }
    eprintln!("crash test: {rounds} kills, {total_acked} acknowledged batches checked, {torn_seen} recoveries removed a torn tail");
}
