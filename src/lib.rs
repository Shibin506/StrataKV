//! StrataKV: a crash-safe key-value storage engine built as a log-structured merge tree.
//!
//! Phase 1: every write goes to a checksummed write-ahead log (fsynced by default) before it is
//! acknowledged, then to an in-memory table. Opening the database replays the log, removes an
//! unfinished write at its end, and reports damage anywhere else as corruption.

pub mod batch;
pub mod db;
pub mod error;
pub mod memtable;
pub mod wal;

pub use batch::WriteBatch;
pub use db::{Db, Options, RecoveryReport, SyncMode};
pub use error::{Error, Result};
