//! Benchmarks StrataKV against RocksDB and SQLite on the same workloads (in the spirit of LevelDB's db_bench).
//!
//!   stratakv-bench run --engine stratakv --dir /tmp/b --out results/stratakv-1.json [--n 1000000] [--value 100]
//!   stratakv-bench report results/*.json            # Markdown tables (median over runs)
//!
//! Run each engine in its own process (bench/run.sh does): peak memory and bytes written are per process.

mod engines;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Instant;

use engines::{Engine, Sync};
use serde_json::{Value, json};

const KEY_BYTES: usize = 16;

struct Args {
    engine: String,
    dir: PathBuf,
    out: Option<PathBuf>,
    n: u64,
    value: usize,
    seed: u64,
    sync_ops: u64,
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        Some("run") => run(parse(&args[1..])),
        Some("report") => report(&args[1..]),
        _ => {
            eprintln!("usage: stratakv-bench run --engine <stratakv|rocksdb|sqlite> --dir <dir> [--out f.json] [--n N] [--value BYTES]");
            eprintln!("       stratakv-bench report <results.json>...");
            std::process::exit(2);
        }
    }
}

fn parse(args: &[String]) -> Args {
    let mut a = Args {
        engine: String::new(),
        dir: PathBuf::new(),
        out: None,
        n: 1_000_000,
        value: 100,
        seed: 42,
        sync_ops: 1000,
    };
    let mut it = args.iter();
    while let Some(flag) = it.next() {
        let v = it.next().unwrap_or_else(|| panic!("{flag} needs a value"));
        match flag.as_str() {
            "--engine" => a.engine = v.clone(),
            "--dir" => a.dir = v.into(),
            "--out" => a.out = Some(v.into()),
            "--n" => a.n = v.parse().unwrap(),
            "--value" => a.value = v.parse().unwrap(),
            "--seed" => a.seed = v.parse().unwrap(),
            "--sync-ops" => a.sync_ops = v.parse().unwrap(),
            other => panic!("unknown flag {other}"),
        }
    }
    assert!(
        !a.engine.is_empty() && !a.dir.as_os_str().is_empty(),
        "--engine and --dir are required"
    );
    a
}

// ---------------------------------------------------------------------------------------------------
// Inputs: present keys are the even numbers 0, 2, 4, ...; the odd numbers in between are the missing keys,
// so lookups for missing keys land inside every table's key range (the case a Bloom filter is for).

fn key(id: u64) -> Vec<u8> {
    format!("{id:016}").into_bytes()
}

/// SplitMix64: small, fast, and the same sequence on every machine.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }

    fn shuffled(&mut self, n: u64) -> Vec<u64> {
        let mut v: Vec<u64> = (0..n).collect();
        for i in (1..v.len()).rev() {
            v.swap(i, self.below(i as u64 + 1) as usize);
        }
        v
    }
}

/// Values are slices of 1 MiB of random bytes at random offsets: incompressible, and cheap to produce.
struct Values {
    pool: Vec<u8>,
    len: usize,
}

impl Values {
    fn new(rng: &mut Rng, len: usize) -> Self {
        let pool = (0..(1 << 20) / 8).flat_map(|_| rng.next().to_le_bytes()).collect();
        Self { pool, len }
    }

    fn get(&self, rng: &mut Rng) -> &[u8] {
        let at = rng.below((self.pool.len() - self.len) as u64) as usize;
        &self.pool[at..at + self.len]
    }
}

// ---------------------------------------------------------------------------------------------------
// Measuring.

/// Bytes this process has passed to write() and friends so far (whether or not they have reached the disk yet).
fn bytes_written() -> u64 {
    proc_field("/proc/self/io", "wchar:")
}

/// Peak resident memory of this process, in bytes.
fn peak_rss() -> u64 {
    proc_field("/proc/self/status", "VmHWM:") * 1024
}

fn proc_field(file: &str, name: &str) -> u64 {
    let text = std::fs::read_to_string(file).unwrap_or_default();
    text.lines()
        .find_map(|l| l.strip_prefix(name))
        .and_then(|v| v.split_whitespace().next())
        .and_then(|v| v.parse().ok())
        .unwrap_or(0)
}

fn dir_bytes(dir: &Path) -> u64 {
    let mut total = 0;
    for e in std::fs::read_dir(dir).unwrap().flatten() {
        let m = e.metadata().unwrap();
        total += if m.is_dir() { dir_bytes(&e.path()) } else { m.len() };
    }
    total
}

/// Times each operation, and the whole run.
struct Timer {
    lat: Vec<u32>, // nanoseconds, capped at ~4 s
    start: Instant,
}

impl Timer {
    fn new(ops: u64) -> Self {
        Self {
            lat: Vec::with_capacity(ops as usize),
            start: Instant::now(),
        }
    }

    fn time<T>(&mut self, f: impl FnOnce() -> T) -> T {
        let t = Instant::now();
        let out = f();
        self.lat.push(t.elapsed().as_nanos().min(u32::MAX as u128) as u32);
        out
    }

    fn finish(mut self, name: &str, extra: Value) -> Value {
        let secs = self.start.elapsed().as_secs_f64();
        self.lat.sort_unstable();
        let pct = |p: f64| -> f64 {
            if self.lat.is_empty() {
                return 0.0;
            }
            let i = ((self.lat.len() as f64 * p).ceil() as usize).clamp(1, self.lat.len()) - 1;
            self.lat[i] as f64 / 1000.0
        };
        let ops = self.lat.len() as f64;
        let mut v = json!({
            "workload": name, "ops": ops, "secs": secs, "ops_per_sec": ops / secs,
            "p50_us": pct(0.50), "p99_us": pct(0.99), "p999_us": pct(0.999), "max_us": pct(1.0),
        });
        if let (Value::Object(m), Value::Object(e)) = (&mut v, extra) {
            m.extend(e);
        }
        eprintln!(
            "  {name:<22} {:>12.0} ops/s   p50 {:>8.2} us   p99 {:>9.2} us   p99.9 {:>9.2} us   max {:>10.1} us",
            ops / secs,
            pct(0.5),
            pct(0.99),
            pct(0.999),
            pct(1.0)
        );
        v
    }
}

/// Waits for background work, returning how long it took.
fn settle(db: &mut dyn Engine) -> f64 {
    let t = Instant::now();
    db.settle();
    t.elapsed().as_secs_f64()
}

// ---------------------------------------------------------------------------------------------------

fn run(a: Args) {
    let _ = std::fs::remove_dir_all(&a.dir);
    std::fs::create_dir_all(&a.dir).unwrap();
    let mut rng = Rng(a.seed);
    let values = Values::new(&mut rng, a.value);
    let entry_bytes = (KEY_BYTES + a.value) as u64;
    let user_bytes = a.n * entry_bytes;
    let mut results = Vec::new();
    eprintln!(
        "{}: {} keys of {KEY_BYTES} bytes, values of {} bytes ({:.0} MB)",
        a.engine,
        a.n,
        a.value,
        user_bytes as f64 / 1e6
    );

    // 1. Bulk load: sorted keys, 1,000 per batch.
    {
        let dir = a.dir.join("fillseq");
        let mut db = engines::open(&a.engine, &dir, Sync::Never);
        let mut t = Timer::new(a.n / 1000);
        let mut batch = Vec::with_capacity(1000);
        for id in 0..a.n {
            batch.push((key(id * 2), values.get(&mut rng).to_vec()));
            if batch.len() == 1000 || id + 1 == a.n {
                t.time(|| db.put_batch(&batch));
                batch.clear();
            }
        }
        let mut r = t.finish("fillseq (batches of 1000)", json!({}));
        // One "op" above was a batch; report entries per second like the other write workloads.
        r["ops_per_sec"] = json!(a.n as f64 / r["secs"].as_f64().unwrap());
        r["unit"] = json!("entries/s; latency is per batch of 1000");
        r["settle_secs"] = json!(settle(db.as_mut()));
        r["disk_bytes"] = json!(dir_bytes(&dir));
        results.push(r);
        drop(db);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    // 2. Durable writes: fsync before every write returns.
    {
        let dir = a.dir.join("fillsync");
        let mut db = engines::open(&a.engine, &dir, Sync::Always);
        let mut t = Timer::new(a.sync_ops);
        for id in rng.shuffled(a.sync_ops) {
            let v = values.get(&mut rng);
            t.time(|| db.put(&key(id * 2), v));
        }
        results.push(t.finish("fillsync (fsync each)", json!({})));
        drop(db);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    // 3. The main database: single writes in random order, then reads and scans, then overwrites.
    let dir = a.dir.join("main");
    let mut db = engines::open(&a.engine, &dir, Sync::Never);
    let order = rng.shuffled(a.n);
    {
        let written = bytes_written();
        let mut t = Timer::new(a.n);
        let mut max_l0 = 0;
        for (i, &id) in order.iter().enumerate() {
            let v = values.get(&mut rng);
            t.time(|| db.put(&key(id * 2), v));
            if i % 1000 == 0 {
                max_l0 = max_l0.max(db.level0_files().unwrap_or(0));
            }
        }
        let during = bytes_written() - written;
        let mut r = t.finish("fillrandom", json!({ "max_level0_files": db.level0_files().map(|_| max_l0) }));
        r["settle_secs"] = json!(settle(db.as_mut()));
        let total = bytes_written() - written;
        r["bytes_written"] = json!(total);
        r["write_amplification"] = json!(total as f64 / user_bytes as f64);
        r["write_amplification_during"] = json!(during as f64 / user_bytes as f64);
        r["disk_bytes"] = json!(dir_bytes(&dir));
        r["space_amplification"] = json!(dir_bytes(&dir) as f64 / user_bytes as f64);
        results.push(r);
    }
    {
        let mut t = Timer::new(a.n);
        let mut found = 0u64;
        for id in rng.shuffled(a.n) {
            found += t.time(|| db.get(&key(id * 2))).is_some() as u64;
        }
        assert_eq!(found, a.n, "every key must be found");
        results.push(t.finish("readrandom", json!({})));
    }
    {
        let mut t = Timer::new(a.n);
        let mut found = 0u64;
        for id in rng.shuffled(a.n) {
            found += t.time(|| db.get(&key(id * 2 + 1))).is_some() as u64;
        }
        assert_eq!(found, 0, "no missing key may be found");
        results.push(t.finish("readmissing", json!({})));
    }
    {
        let scans = (a.n / 10).max(1);
        let mut t = Timer::new(scans);
        let mut bytes = 0;
        for _ in 0..scans {
            let start = key(rng.below(a.n) * 2);
            bytes += t.time(|| db.scan(&start, 100));
        }
        assert!(bytes > 0);
        results.push(t.finish("seekrandom (100 keys)", json!({})));
    }
    {
        let t = Instant::now();
        let n = db.scan_all();
        assert_eq!(n as u64, a.n);
        let secs = t.elapsed().as_secs_f64();
        eprintln!("  {:<22} {:>12.0} entries/s", "readseq (full scan)", n as f64 / secs);
        results.push(
            json!({ "workload": "readseq (full scan)", "ops": n, "secs": secs, "ops_per_sec": n as f64 / secs, "unit": "entries/s" }),
        );
    }
    {
        let written = bytes_written();
        let mut t = Timer::new(a.n);
        for _ in 0..a.n {
            let id = rng.below(a.n);
            let v = values.get(&mut rng);
            t.time(|| db.put(&key(id * 2), v));
        }
        let mut r = t.finish("overwrite", json!({}));
        r["settle_secs"] = json!(settle(db.as_mut()));
        let total = bytes_written() - written;
        r["write_amplification"] = json!(total as f64 / user_bytes as f64);
        r["disk_bytes"] = json!(dir_bytes(&dir));
        r["space_amplification"] = json!(dir_bytes(&dir) as f64 / user_bytes as f64);
        results.push(r);
    }
    {
        let mut t = Timer::new(a.n);
        for id in rng.shuffled(a.n) {
            assert!(t.time(|| db.get(&key(id * 2))).is_some());
        }
        results.push(t.finish("readrandom (after overwrite)", json!({})));
    }
    let notes: BTreeMap<String, String> = db.notes().into_iter().collect();
    drop(db);
    let t = Instant::now();
    let db = engines::open(&a.engine, &dir, Sync::Never);
    let reopen = t.elapsed().as_secs_f64();
    drop(db);
    eprintln!(
        "  reopen {:.1} ms, peak memory {:.0} MB, {notes:?}",
        reopen * 1000.0,
        peak_rss() as f64 / 1e6
    );

    let out = json!({
        "engine": a.engine, "n": a.n, "key_bytes": KEY_BYTES, "value_bytes": a.value, "user_bytes": user_bytes,
        "reopen_ms": reopen * 1000.0, "peak_rss_bytes": peak_rss(), "notes": notes, "results": results,
        "cpus": std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1),
    });
    std::fs::remove_dir_all(&a.dir).unwrap();
    match &a.out {
        Some(path) => {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).unwrap();
            }
            std::fs::write(path, serde_json::to_string_pretty(&out).unwrap()).unwrap()
        }
        None => println!("{}", serde_json::to_string_pretty(&out).unwrap()),
    }
}

// ---------------------------------------------------------------------------------------------------
// Report: median over runs of each engine, as Markdown.

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(f64::total_cmp);
    if v.is_empty() {
        return f64::NAN;
    }
    let m = v.len() / 2;
    if v.len() % 2 == 1 { v[m] } else { (v[m - 1] + v[m]) / 2.0 }
}

fn report(files: &[String]) {
    let mut runs: BTreeMap<String, Vec<Value>> = BTreeMap::new();
    for f in files {
        let v: Value = serde_json::from_str(&std::fs::read_to_string(f).unwrap()).unwrap();
        runs.entry(v["engine"].as_str().unwrap().to_string()).or_default().push(v);
    }
    let order = ["stratakv", "rocksdb", "sqlite"];
    let engines: Vec<&str> = order.iter().copied().filter(|e| runs.contains_key(*e)).collect();
    let first = &runs[engines[0]][0];
    let workloads: Vec<String> = first["results"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["workload"].as_str().unwrap().to_string())
        .collect();
    let stat = |engine: &str, workload: &str, field: &str| -> f64 {
        median(
            runs[engine]
                .iter()
                .filter_map(|run| {
                    run["results"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .find(|r| r["workload"] == workload)
                        .and_then(|r| r[field].as_f64())
                })
                .collect(),
        )
    };
    let top = |engine: &str, field: &str| median(runs[engine].iter().filter_map(|r| r[field].as_f64()).collect());
    let header = |first: &str| {
        println!("| {first} | {} |", engines.join(" | "));
        println!("|---|{}", "---:|".repeat(engines.len()));
    };
    let counts: Vec<String> = engines.iter().map(|e| format!("{e}: {}", runs[*e].len())).collect();
    println!(
        "{} keys of {} bytes + values of {} bytes ({:.0} MB of user data), {} CPUs. Median of runs ({}).\n",
        first["n"],
        first["key_bytes"],
        first["value_bytes"],
        first["user_bytes"].as_f64().unwrap() / 1e6,
        first["cpus"],
        counts.join(", ")
    );

    println!("**Throughput** (operations per second; higher is better)\n");
    header("Workload");
    for w in &workloads {
        let cells: Vec<String> = engines.iter().map(|e| human(stat(e, w, "ops_per_sec"))).collect();
        println!("| {w} | {} |", cells.join(" | "));
    }

    println!("\n**Latency** (microseconds, p50 / p99 / p99.9; lower is better)\n");
    header("Workload");
    for w in workloads.iter().filter(|w| !w.starts_with("readseq")) {
        let cells: Vec<String> = engines
            .iter()
            .map(|e| {
                format!(
                    "{} / {} / {}",
                    us(stat(e, w, "p50_us")),
                    us(stat(e, w, "p99_us")),
                    us(stat(e, w, "p999_us"))
                )
            })
            .collect();
        println!("| {w} | {} |", cells.join(" | "));
    }

    println!("\n**Space, write amplification, memory**\n");
    header("Measure");
    let row = |label: &str, f: &dyn Fn(&str) -> String| {
        let cells: Vec<String> = engines.iter().map(|e| f(e)).collect();
        println!("| {label} | {} |", cells.join(" | "));
    };
    row("Disk after fillrandom + settle", &|e| mb(stat(e, "fillrandom", "disk_bytes")));
    row("Disk after overwrite + settle", &|e| mb(stat(e, "overwrite", "disk_bytes")));
    row("Write amplification, fillrandom (bytes written / user bytes)", &|e| {
        format!("{:.1}x", stat(e, "fillrandom", "write_amplification"))
    });
    row("Write amplification, overwrite", &|e| {
        format!("{:.1}x", stat(e, "overwrite", "write_amplification"))
    });
    row("Time to finish background work after fillrandom", &|e| {
        format!("{:.2} s", stat(e, "fillrandom", "settle_secs"))
    });
    row("Most level-0 tables seen during fillrandom", &|e| {
        let v = stat(e, "fillrandom", "max_level0_files");
        if v.is_nan() { "n/a".into() } else { format!("{v:.0}") }
    });
    row("Reopen time", &|e| format!("{:.1} ms", top(e, "reopen_ms")));
    row("Peak memory (whole run)", &|e| mb(top(e, "peak_rss_bytes")));
}

fn human(v: f64) -> String {
    if v >= 1e6 {
        format!("{:.2} M", v / 1e6)
    } else if v >= 1e3 {
        format!("{:.0} K", v / 1e3)
    } else {
        format!("{v:.0}")
    }
}

fn us(v: f64) -> String {
    if v >= 100.0 { format!("{v:.0}") } else { format!("{v:.1}") }
}

fn mb(v: f64) -> String {
    format!("{:.0} MB", v / 1e6)
}
