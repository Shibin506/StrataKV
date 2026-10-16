//! Crash points for testing: a test helper process names one point, and the process aborts the moment
//! it reaches that point, exactly like a crash there. Unarmed points cost one atomic load.

use std::sync::atomic::{AtomicU8, Ordering};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Point {
    /// A new log was created; the full memtable is not on disk yet.
    FlushAfterWalRotate = 1,
    /// The table is written and fsynced as `NNNNNN.sst.tmp`, not yet renamed.
    FlushAfterTableTmp,
    /// The table has its final name but `CURRENT` does not list it yet.
    FlushAfterTableRename,
    /// The new `CURRENT.tmp` is written, not yet renamed.
    FlushAfterCurrentTmp,
    /// `CURRENT` lists the table; the old log has not been deleted yet.
    FlushAfterCurrentRename,
    /// The old log is deleted.
    FlushAfterWalDelete,
    /// Every output table of a compaction is complete under its final name; `CURRENT` still lists the inputs.
    CompactionAfterOutputs,
    /// The new `CURRENT.tmp` for a compaction is written, not yet renamed.
    CompactionAfterCurrentTmp,
    /// `CURRENT` lists the outputs; the input tables have not been deleted yet.
    CompactionAfterCurrentRename,
    /// The input tables are deleted.
    CompactionAfterInputsDeleted,
}

pub const ALL: [(&str, Point); 10] = [
    ("flush-after-wal-rotate", Point::FlushAfterWalRotate),
    ("flush-after-table-tmp", Point::FlushAfterTableTmp),
    ("flush-after-table-rename", Point::FlushAfterTableRename),
    ("flush-after-current-tmp", Point::FlushAfterCurrentTmp),
    ("flush-after-current-rename", Point::FlushAfterCurrentRename),
    ("flush-after-wal-delete", Point::FlushAfterWalDelete),
    ("compaction-after-outputs", Point::CompactionAfterOutputs),
    ("compaction-after-current-tmp", Point::CompactionAfterCurrentTmp),
    ("compaction-after-current-rename", Point::CompactionAfterCurrentRename),
    ("compaction-after-inputs-deleted", Point::CompactionAfterInputsDeleted),
];

/// The crash points that belong to a flush (the first six) and to a compaction (the rest).
pub fn is_compaction_point(name: &str) -> bool {
    name.starts_with("compaction-")
}

static ARMED: AtomicU8 = AtomicU8::new(0);

/// Makes the process abort when it reaches `name`. For crash tests only.
pub fn arm(name: &str) -> bool {
    match ALL.iter().find(|(n, _)| *n == name) {
        Some((_, p)) => {
            ARMED.store(*p as u8, Ordering::SeqCst);
            true
        }
        None => false,
    }
}

#[inline]
pub(crate) fn hit(p: Point) {
    if ARMED.load(Ordering::Relaxed) == p as u8 {
        std::process::abort();
    }
}
