//! StrataKV: a crash-safe key-value storage engine built as a log-structured merge tree.
//!
//! Every write goes to a checksummed write-ahead log (fsynced by default) before it is acknowledged,
//! then to an in-memory table. A full memtable is written to an immutable sorted table on disk
//! (`sstable`), and the small `CURRENT` file (`version`) switches atomically to the new set of files.

pub mod batch;
pub mod bloom;
pub mod compaction;
pub mod db;
pub mod error;
#[doc(hidden)]
pub mod failpoint;
mod lock;
pub mod memtable;
pub mod sstable;
pub mod version;
pub mod wal;

pub use batch::WriteBatch;
pub use db::{Db, LevelStats, Options, RecoveryReport, Scan, Snapshot, Stats, SyncMode, TableInfo};
pub use error::{Error, Result};
