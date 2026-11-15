//! Reopen a database left behind by `stratakv-crashwriter` (for example after `kill -9`) and check that
//! every acknowledged batch survived, whole. The same checks as `tests/crash.rs`, as a command you can run.
//!
//!     cargo run --release --bin stratakv-crashwriter -- /tmp/kv 0 always > acks.txt &
//!     sleep 2; kill -9 $!
//!     cargo run --release --example recover -- /tmp/kv "$(tail -1 acks.txt | cut -d' ' -f2)"

use std::process::ExitCode;

use stratakv::{Db, Options};

// Must match `value_for` in src/bin/stratakv-crashwriter.rs.
fn value_for(n: u64) -> Vec<u8> {
    let len = 1 + (n.wrapping_mul(2_654_435_761) % 8192) as usize;
    (0..len).map(|i| (n as usize).wrapping_mul(31).wrapping_add(i) as u8).collect()
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().collect();
    let (Some(dir), Some(last_ack)) = (args.get(1), args.get(2).and_then(|s| s.parse::<u64>().ok())) else {
        eprintln!("usage: recover <dir> <last acknowledged batch>");
        return ExitCode::from(2);
    };
    let started = std::time::Instant::now();
    let db = Db::open(dir, Options::default()).expect("open");
    let opened = started.elapsed();
    let get = |key: String| db.get(key.as_bytes()).expect("read");

    // How many batches are on disk? At most one more than was acknowledged (it hit the disk, the ack didn't
    // reach us before the kill).
    let mut on_disk = 0;
    while get(format!("k{on_disk:010}a")).is_some() {
        on_disk += 1;
    }
    let mut problems = 0;
    for n in 0..on_disk {
        let v = Some(value_for(n));
        if get(format!("k{n:010}a")) != v || get(format!("k{n:010}b")) != v {
            println!("batch {n}: lost, wrong or half applied");
            problems += 1;
        }
    }
    let acked = last_ack + 1;
    if on_disk < acked {
        println!("LOST: {} acknowledged batches are missing", acked - on_disk);
        problems += 1;
    }
    if on_disk > acked + 1 {
        println!("{} batches on disk were never acknowledged", on_disk - acked - 1);
        problems += 1;
    }
    if on_disk >= 2 && get(format!("last-{}", on_disk - 2)).is_some() {
        println!("a delete inside a batch was lost");
        problems += 1;
    }

    println!("reopened in {:.1?} after the crash", opened);
    println!(
        "acknowledged batches: {acked}   on disk: {on_disk}   (keys checked: {})",
        on_disk * 2
    );
    if problems == 0 {
        println!("OK: every acknowledged write survived, and every batch is all-or-nothing");
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}
