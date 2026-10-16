//! Helper for the crash test (`tests/crash.rs`): writes batches forever and prints `ack N` only after
//! batch N's write has returned Ok. The test kills this process with SIGKILL at a random moment.
//!
//! Batch N puts `kNNNNNNNNa` and `kNNNNNNNNb` (same deterministic value, up to 8 KiB, so writes often
//! span several disk blocks), deletes `last-(N-1)` and puts `last-N`. So after any crash exactly one
//! `last-*` key may exist, and the `a`/`b` pair proves the batch was applied atomically.
//!
//! Usage: stratakv-crashwriter <dir> <first-batch-number> <always|never> [memtable-bytes] [crash-point]
//!
//! A small memtable makes flushes frequent, so random kills land in the middle of them too. A crash
//! point (see `stratakv::failpoint`) makes the process abort at exactly that step of the first flush.

use std::io::Write;

use stratakv::{Db, Options, SyncMode, WriteBatch};

pub fn value_for(n: u64) -> Vec<u8> {
    let len = 1 + (n.wrapping_mul(2_654_435_761) % 8192) as usize;
    (0..len).map(|i| (n as usize).wrapping_mul(31).wrapping_add(i) as u8).collect()
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let dir = &args[1];
    let mut n: u64 = args[2].parse().expect("first batch number");
    let sync = if args[3] == "always" { SyncMode::Always } else { SyncMode::Never };
    let mut options = Options {
        sync,
        ..Options::default()
    };
    if let Some(bytes) = args.get(4) {
        options.memtable_bytes = bytes.parse().expect("memtable bytes");
    }
    let db = Db::open(dir, options).expect("open");
    if let Some(point) = args.get(5) {
        assert!(stratakv::failpoint::arm(point), "unknown crash point {point}");
    }
    let stdout = std::io::stdout();
    let mut out = stdout.lock();
    loop {
        let mut b = WriteBatch::new();
        let v = value_for(n);
        b.put(format!("k{n:010}a"), v.clone()).put(format!("k{n:010}b"), v);
        if n > 0 {
            b.delete(format!("last-{}", n - 1));
        }
        b.put(format!("last-{n}"), n.to_string());
        db.write(&b).expect("write");
        writeln!(out, "ack {n}").unwrap();
        out.flush().unwrap();
        n += 1;
    }
}
