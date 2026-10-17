# Phase 5: benchmarks against RocksDB and SQLite

## How to run it

```bash
bench/run.sh                         # 1,000,000 keys, 3 runs of each engine, about 20 minutes
N=200000 RUNS=1 bench/run.sh         # a quick check
BENCH_DIR=/mnt/ssd/bench bench/run.sh  # put the databases on the disk you want to measure
```

The benchmark is a separate crate (`bench/`), so StrataKV itself never depends on RocksDB or SQLite. Each engine
runs in its own process (so memory and bytes-written counters are its own), runs are interleaved (a slow
moment on the machine hits every engine alike), and the report is the median of 3 runs. Raw results:
`bench/results/*.json`; the report: `bench/results/report.md`.

## The workloads (in the spirit of LevelDB's `db_bench`)

Keys are 16 bytes, values 100 random (incompressible) bytes. Present keys are the even numbers; lookups for
missing keys use the odd numbers in between, so they land inside every table's key range.

| Workload | What it does |
|---|---|
| fillseq | 1,000,000 keys in sorted order, 1,000 per batch (a bulk load) |
| fillsync | 1,000 single writes, each fsynced before it returns |
| fillrandom | 1,000,000 single writes in random order (no fsync) |
| readrandom | every key once, in random order |
| readmissing | 1,000,000 lookups of keys that do not exist |
| seekrandom | 100,000 scans: seek to a random key, read the next 100 |
| readseq | one scan over everything |
| overwrite | 1,000,000 random overwrites of existing keys |

## Making it fair

| | StrataKV | RocksDB 11.8 | SQLite 3 |
|---|---|---|---|
| Durability (single writes) | `SyncMode::Never` / `Always` | `sync=false` / `true` | WAL mode, `synchronous=NORMAL` / `FULL` |
| Shape of the tree | 4 MiB memtable, 4 KiB blocks, 10-bit Bloom filters, L0 trigger 4, L1 16 MiB, x10, 2 MiB files | **set to the same** | B-tree, `WITHOUT ROWID` table |
| Compression | none | none | none |
| Cache | the OS page cache only | 32 MiB block cache (its default) | 2 MiB page cache (its default) |
| Background threads | 1 flush + 1 compaction | 2 background jobs (its default) | none |

Every engine is built with `-C target-cpu=native`. That matters: RocksDB only uses SSE4.2, AVX2 and PCLMUL (fast
CRC32C checksums, among others) when told the CPU has them. A first run without it made RocksDB's bulk load
19% slower (1.55 M/s instead of 1.91 M/s).

"Write amplification" is the bytes the process passed to `write()` (from `/proc/self/io`, so the same meter for all
three engines, logs included) divided by the bytes of keys and values written by the benchmark.

## Results

1,000,000 keys (116 MB of user data), 4-core Intel Xeon @ 2.1 GHz cloud VM, Linux 6.18, median of 3 runs.

**Throughput** (operations per second; higher is better)

| Workload | stratakv | rocksdb | sqlite |
|---|---:|---:|---:|
| fillseq (batches of 1000) | 752 K | 1.91 M | 480 K |
| fillsync (fsync each) | 6 K | 7 K | 5 K |
| fillrandom | 381 K | 386 K | 20 K |
| readrandom | 238 K | 153 K | 199 K |
| readmissing | 1.02 M | 578 K | 201 K |
| seekrandom (100 keys) | 26 K | 23 K | 45 K |
| readseq (full scan) | 5.50 M | 4.56 M | 10.37 M |
| overwrite | 381 K | 382 K | 31 K |
| readrandom (after overwrite) | 246 K | 152 K | 199 K |

**Latency** (microseconds, p50 / p99 / p99.9; lower is better)

| Workload | stratakv | rocksdb | sqlite |
|---|---:|---:|---:|
| fillseq (batches of 1000) | 439 / 12219 / 19181 | 260 / 2147 / 6005 | 924 / 21657 / 23612 |
| fillsync (fsync each) | 148 / 413 / 1203 | 129 / 308 / 859 | 193 / 414 / 1430 |
| fillrandom | 1.5 / 11.4 / 33.4 | 2.1 / 6.9 / 30.7 | 9.9 / 57.0 / 19766 |
| readrandom | 3.8 / 9.9 / 34.3 | 5.9 / 19.6 / 45.8 | 4.5 / 11.3 / 42.0 |
| readmissing | 0.8 / 5.1 / 17.6 | 1.4 / 7.5 / 22.8 | 4.5 / 12.2 / 39.3 |
| seekrandom (100 keys) | 35.0 / 75.1 / 179 | 38.8 / 91.8 / 177 | 20.2 / 48.8 / 86.9 |
| overwrite | 1.6 / 11.2 / 31.5 | 2.2 / 6.5 / 33.7 | 9.1 / 37.7 / 3292 |
| readrandom (after overwrite) | 3.7 / 9.8 / 35.6 | 5.9 / 21.2 / 47.8 | 4.5 / 11.8 / 39.7 |

**Space, write amplification, memory**

| Measure | stratakv | rocksdb | sqlite |
|---|---:|---:|---:|
| Disk after fillrandom + settle | 136 MB | 117 MB | 140 MB |
| Disk after overwrite + settle | 153 MB | 144 MB | 140 MB |
| Write amplification, fillrandom (bytes written / user bytes) | 6.0x | 5.1x | 115.5x |
| Write amplification, overwrite | 7.1x | 5.9x | 70.3x |
| Time to finish background work after fillrandom | 1.14 s | 0.34 s | 0.02 s |
| Most level-0 tables seen during fillrandom | 8 | 7 | n/a |
| Reopen time | 6.2 ms | 28.2 ms | 0.3 ms |
| Peak memory (whole run) | 61 MB | 95 MB | 38 MB |

## What the numbers say

- **Random writes:** StrataKV and RocksDB are level (381 K vs 386 K per second); SQLite is about 19x slower,
  because every single-row transaction rewrites whole 4 KiB B-tree pages into its WAL (write amplification 115x
  vs 5-6x). This is the workload LSM trees exist for.
- **Point reads:** StrataKV is fastest here (238 K reads/s, 1.0 M misses/s). The comparison has a catch: the whole
  database fits in this machine's RAM (15 GB), so every engine reads from memory and the numbers measure CPU and
  system-call costs, not the disk. RocksDB pays for a block cache that cannot help here (the OS cache already
  holds everything) and for crossing from Rust into C++ on every call. On data bigger than RAM the order may change.
- **Lookups of missing keys** are where the Bloom filters show: 1.0 M/s, against 201 K/s for SQLite, which has to
  walk its B-tree for every miss.
- **Scans:** SQLite's B-tree wins (45 K scans of 100 keys per second); StrataKV and RocksDB are close (26 K vs
  23 K). A B-tree keeps neighbouring keys together in one place; an LSM tree has to merge several sorted runs.
- **Bulk load:** RocksDB is 2.5x faster (1.91 M vs 752 K entries/s). See "Known gaps".
- **Space:** after random writes StrataKV uses 136 MB for 116 MB of data, RocksDB 117 MB, SQLite 140 MB.
- **Memory:** StrataKV peaks at 61 MB, RocksDB 95 MB, SQLite 38 MB.
- **fsync:** all three manage 5-7 K durable writes per second, which is the speed of this VM's virtual disk,
  not of the engines. On a laptop SSD expect different numbers.

## What the benchmark found (and what was fixed)

The first run, at 100,000 keys, showed two problems, and chasing a failing test turned up a third:

1. **Scans were about 100x slower than RocksDB** (410 scans/s instead of 39,000). A scan copied the whole
   matching part of the memtable when it started: up to 4 MB for "100 keys from here". The memtable is now
   shared with running scans (it has its own lock, and versions are only ever added), and a scan reads it lazily in
   chunks of 32, 64, ... 4096 entries. Deeper levels are now read as one chain each, opening the next table only
   when needed. Result: **63x faster**, now level with RocksDB.
2. **A write that filled the memtable wrote the whole 4 MB table itself**, while holding the write lock, so every
   other writer waited about 25 ms. Flushes now run on a background thread: the writer only freezes the full
   memtable and starts a new log (fast), and keeps going. A write waits only if the memtable fills again before the
   previous flush is done (a *write stall*, now counted in `stats().write_stalls`; none happened in these runs).
   Bulk-load batch p99 dropped from 26 ms to 6 ms at 100,000 keys, and random writes got faster (371 K to 498 K/s).
   The table records the last sequence number *of the memtable it holds*, not the newest one when it finishes:
   recovery requires every logged write to come after it, so the other choice would make a reopen after a crash
   fail. The crash test at every flush and compaction step passed 800 times in a row with the new code.
3. **Reopening a database could fail with "already open" if another thread started a process at that moment.**
   The crash test failed once and could not be reproduced; the cause turned out to be an old bug. The LOCK file
   used an `flock` lock, which belongs to the open file, and a child process briefly shares all of its parent's
   open files while it starts. Measured: **10,571 of 20,000 reopens refused** while another thread started 154
   child processes. StrataKV now does what RocksDB does: a POSIX `fcntl` lock (it belongs to the process, children
   never inherit it) plus a list of directories this process has open (POSIX locks do not stop a process from
   locking twice). Now 0 of 20,000 are refused. Tests: `reopening_works_while_other_threads_start_processes`,
   `another_process_cannot_open_an_open_database_and_a_refused_open_keeps_the_lock`.

Mutation checks for the lock: removing the in-process list, going back to `flock`, and opening the file before
checking the list (which would release the first database's lock) each make a test fail.

## Known gaps

- **Bulk load is 2.5x slower than RocksDB.** It is not the fsync of the old log at each memtable switch: with that
  fsync removed (an experiment, not kept) throughput was the same, and only the p99 improved (13 ms to 5 ms).
  The likely cause is copying: each entry is copied into the batch, again into the log record, and again into the
  memtable, with an allocation each time, while RocksDB encodes a batch once into one buffer and inserts from it.
- **The p99 of a bulk-load batch is 12 ms** (RocksDB 2 ms): the old 4 MB log is fsynced on the writer's path at each
  memtable switch. StrataKV keeps that on purpose: it is what lets recovery promise that after a power cut you get
  a clean prefix of your history, never an error. Syncing the log in small steps while it is written (like
  RocksDB's `wal_bytes_per_sync`) would make the final fsync cheap.
- **Compaction catches up 3x more slowly** (1.1 s vs 0.3 s after `fillrandom`): one compaction thread, against
  RocksDB's two background jobs and its sub-compactions.
- **No write slowdown:** level 0 reached 8 tables (trigger 4) under random writes. StrataKV never slows writers
  down to let compaction catch up, so under a long write burst reads get slower instead.
- **No block cache:** StrataKV relies on the OS page cache, and every block read is a system call plus a checksum.
  Profiling scans showed 36% of their time in memory allocation and 9% in zero-filling the buffer for each block
  read; a block cache would remove both for hot data.
- **What this benchmark does not show:** data bigger than RAM, many client threads, other machines. One cloud VM,
  one client thread.
