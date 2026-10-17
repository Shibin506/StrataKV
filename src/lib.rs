//! StrataKV: a crash-safe key-value storage engine built as a log-structured merge tree (LSM tree).
//!
//! Every write goes to a checksummed write-ahead log (fsynced by default) before it is acknowledged,
//! then to an in-memory table. A full memtable is written to an immutable sorted table on disk
//! (`sstable`) by a background thread, background compaction merges tables down a series of levels, and
//! the small `CURRENT` file (`version`) switches atomically to each new set of files.
//!
//! ```
//! use stratakv::{Db, Options, WriteBatch};
//!
//! # fn main() -> stratakv::Result<()> {
//! # let dir = tempfile::tempdir().unwrap();
//! # let path = dir.path();
//! let db = Db::open(path, Options::default())?;     // fsync before every write returns (the default)
//! db.put("user:1", "Ada")?;
//! assert_eq!(db.get(b"user:1")?.as_deref(), Some(&b"Ada"[..]));
//!
//! let mut batch = WriteBatch::new();                 // all or nothing, even across a crash
//! batch.put("account:a", "90").put("account:b", "110");
//! db.write(&batch)?;
//! db.delete("user:1")?;
//!
//! let snap = db.snapshot();                          // a frozen view: later writes are invisible to it
//! db.put("account:a", "0")?;
//! assert_eq!(snap.get(b"account:a")?.as_deref(), Some(&b"90"[..]));
//!
//! let accounts: Vec<(Vec<u8>, Vec<u8>)> = db.scan_prefix("account:")?.collect::<stratakv::Result<_>>()?;
//! assert_eq!(accounts, vec![(b"account:a".to_vec(), b"0".to_vec()), (b"account:b".to_vec(), b"110".to_vec())]);
//! assert_eq!(db.scan("account:b"..)?.count(), 1);    // any range: a..b, a..=b, ..b, (Bound, Bound)
//! # Ok(())
//! # }
//! ```

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
