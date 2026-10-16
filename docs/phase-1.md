# Phase 1: the write-ahead log

## The idea in plain words

A database keeps data in memory because memory is fast. But memory disappears when a program crashes or
the power goes out. So before changing anything in memory, StrataKV writes the change to the end of a
file on disk (the **write-ahead log**) and waits until the disk confirms it (**fsync**). Only then does it
answer "OK".

If the program crashes, the next start reads the log from the beginning and replays every change. The
memory is rebuilt exactly as it was.

## The hard part: crashes in the middle of a write

A crash can happen while a record is half written. The log must tell apart:

| What is at the end of the file | What it means | What recovery does |
|---|---|---|
| a partial record, or zeros | the write was in progress and was never acknowledged | cut it off and carry on |
| a damaged record with good records after it | real damage (a bad disk, a bug) | stop with `Error::Corruption` |

Every record carries a **CRC32 checksum** of its length and contents, so damage is detected. Before
anything is cut off as an unfinished write, recovery checks that no valid record exists further on: a
damaged length field could otherwise make an early record look like the end of the file, and silently
throw away good data after it.

## Other safety rules

- **Batches are atomic.** A batch is one log record, so it is either fully replayed or not at all.
- **A failed disk write stops all writes.** If a write or fsync fails, what reached the disk is unknown
  (PostgreSQL's "fsyncgate" showed that retrying an fsync can silently lose data). StrataKV refuses further
  writes until the database is reopened and the log is read back.
- **The log is fixed before new writes.** An unfinished write found at startup is removed from the file, so
  new records never follow garbage.
- **One owner.** A `LOCK` file stops two processes from writing the same database.
- **New files are made durable.** After creating the log, the directory itself is fsynced, so the file
  cannot vanish after a power cut.

## Sync modes

| `SyncMode` | Survives the process being killed | Survives a power cut | Speed |
|---|---|---|---|
| `Always` (default) | yes | yes | one fsync per write (Phase 5 groups them) |
| `Never` | yes | no: the OS may not have written it yet | much faster |

## Files

- `src/wal.rs`: record format, recovery, the appending writer
- `src/batch.rs`: atomic groups of puts and deletes, strict decoding
- `src/memtable.rs`: the in-memory sorted table (deletes kept as tombstones for later phases)
- `src/db.rs`: open, write, get; locking; refusing writes after a failure
- `src/bin/stratakv-crashwriter.rs` + `tests/crash.rs`: the kill -9 crash test

## Not in Phase 1

The log grows forever and all data lives in memory. Phase 2 writes the in-memory table to sorted files on
disk when it gets big and starts a new log.
