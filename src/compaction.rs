//! Compaction: merging tables down the levels.
//!
//! * **Level 0** holds tables straight from flushes. Their key ranges overlap, so a read checks each of
//!   them (newest first).
//! * **Levels 1..6** each hold tables whose key ranges do **not** overlap, sorted by key. A read checks at
//!   most one table per level (a binary search on the ranges). Each level may be 10x bigger than the
//!   one above it.
//!
//! When level 0 has too many tables, or a level is over its size limit, tables are merged into the next
//! level. The merge keeps only the newest version of each key and drops a delete (tombstone) once no
//! older version of that key can exist further down. That is how overwritten and deleted data stops
//! using disk space.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::error::Result;
use crate::sstable::{OwnedEntry, Table, TableBuilder, TableIter, TableMeta};
use crate::version::MAX_LEVELS;

/// The shape of the tree. See `Options` for what each one means.
#[derive(Debug, Clone, Copy)]
pub struct Tuning {
    pub l0_trigger: usize,
    pub level_base_bytes: u64,
    pub level_multiplier: u64,
    pub target_file_bytes: u64,
}

impl Tuning {
    /// Size limit of `level` (1 or deeper).
    pub fn level_limit(&self, level: usize) -> u64 {
        self.level_base_bytes
            .saturating_mul(self.level_multiplier.saturating_pow(level as u32 - 1))
    }
}

/// Tables per level: level 0 newest first, deeper levels sorted by smallest key.
pub type Levels = Vec<Vec<Arc<Table>>>;

#[derive(Debug)]
pub struct Plan {
    pub level: usize,
    pub output_level: usize,
    /// Inputs from `level` (level 0: newest first).
    pub upper: Vec<Arc<Table>>,
    /// Inputs from `output_level`, sorted by key.
    pub lower: Vec<Arc<Table>>,
}

impl Plan {
    /// A single table with nothing to merge with below it can simply be relabelled with the next level.
    pub fn is_trivial_move(&self) -> bool {
        self.upper.len() == 1 && self.lower.is_empty()
    }

    /// Every input, newest first: the merge keeps the first version of each key it sees.
    pub fn inputs_newest_first(&self) -> Vec<Arc<Table>> {
        self.upper.iter().chain(self.lower.iter()).cloned().collect()
    }
}

fn key_range(tables: &[Arc<Table>]) -> Option<(Vec<u8>, Vec<u8>)> {
    let nonempty: Vec<_> = tables.iter().filter(|t| !t.is_empty()).collect();
    let lo = nonempty.iter().map(|t| t.smallest()).min()?;
    let hi = nonempty.iter().map(|t| t.largest()).max()?;
    Some((lo.to_vec(), hi.to_vec()))
}

fn overlapping(level: &[Arc<Table>], lo: &[u8], hi: &[u8]) -> Vec<Arc<Table>> {
    level.iter().filter(|t| t.overlaps(lo, hi)).cloned().collect()
}

pub fn level_bytes(level: &[Arc<Table>]) -> u64 {
    level.iter().map(|t| t.size()).sum()
}

/// The most urgent compaction, if any level is over its limit. `pointers[level]` remembers where the
/// last compaction of that level ended, so successive compactions walk through the whole key space.
pub fn pick(levels: &Levels, tuning: &Tuning, pointers: &mut [Vec<u8>]) -> Option<Plan> {
    let mut best: Option<(f64, usize)> = None;
    for (level, tables) in levels.iter().enumerate().take(MAX_LEVELS as usize - 1) {
        let score = if level == 0 {
            tables.len() as f64 / tuning.l0_trigger as f64
        } else {
            level_bytes(tables) as f64 / tuning.level_limit(level) as f64
        };
        if score >= 1.0 && best.is_none_or(|(s, _)| score > s) {
            best = Some((score, level));
        }
    }
    let (_, level) = best?;
    let output_level = level + 1;
    let upper: Vec<Arc<Table>> = if level == 0 {
        levels[0].clone() // all of them: level-0 tables overlap, and newer ones must stay above older ones
    } else {
        let files = &levels[level];
        let pointer = &pointers[level];
        let chosen = files
            .iter()
            .find(|t| t.smallest() > pointer.as_slice())
            .unwrap_or(&files[0])
            .clone();
        pointers[level] = chosen.largest().to_vec();
        vec![chosen]
    };
    let lower = match key_range(&upper) {
        Some((lo, hi)) => overlapping(levels.get(output_level).map_or(&[][..], |l| l), &lo, &hi),
        None => Vec::new(),
    };
    Some(Plan {
        level,
        output_level,
        upper,
        lower,
    })
}

/// A pending entry in the merge: key, index of the source table (0 = newest), value or tombstone.
type Head = (Vec<u8>, usize, Option<Vec<u8>>);

/// Merges tables (given newest first) into one stream in key order, keeping the newest version of each key.
pub struct Merge {
    iters: Vec<TableIter>,
    heap: BinaryHeap<Reverse<Head>>,
    started: bool,
}

impl Merge {
    pub fn new(newest_first: &[Arc<Table>]) -> Self {
        Self {
            iters: newest_first.iter().map(|t| t.iter()).collect(),
            heap: BinaryHeap::new(),
            started: false,
        }
    }

    fn pull(&mut self, i: usize) -> Result<()> {
        if let Some(item) = self.iters[i].next() {
            let (k, v) = item?;
            self.heap.push(Reverse((k, i, v)));
        }
        Ok(())
    }
}

impl Iterator for Merge {
    type Item = Result<OwnedEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.started {
            self.started = true;
            for i in 0..self.iters.len() {
                if let Err(e) = self.pull(i) {
                    return Some(Err(e));
                }
            }
        }
        // Smallest key first; for equal keys the smallest source index (the newest table) comes first.
        let Reverse((key, source, value)) = self.heap.pop()?;
        if let Err(e) = self.pull(source) {
            return Some(Err(e));
        }
        while self.heap.peek().is_some_and(|Reverse((k, _, _))| *k == key) {
            let Reverse((_, older, _)) = self.heap.pop().unwrap();
            if let Err(e) = self.pull(older) {
                return Some(Err(e));
            }
        }
        Some(Ok((key, value)))
    }
}

pub struct WriteSettings<'a> {
    pub dir: &'a Path,
    pub block_bytes: usize,
    pub bloom_bits_per_key: u32,
    pub target_file_bytes: u64,
    pub stop: &'a AtomicBool,
}

/// Writes the merged stream as new tables of about `target_file_bytes` each. Tombstones for which
/// `drop_tombstone` says no older version can exist are left out. Returns `None` if `stop` was set
/// (the partial outputs are deleted).
pub fn write_outputs(
    merged: impl Iterator<Item = Result<OwnedEntry>>,
    settings: &WriteSettings<'_>,
    mut next_id: impl FnMut() -> u64,
    drop_tombstone: impl Fn(&[u8]) -> bool,
) -> Result<Option<Vec<TableMeta>>> {
    let mut outputs: Vec<TableMeta> = Vec::new();
    let mut builder: Option<TableBuilder> = None;
    let result = (|| -> Result<bool> {
        for (n, item) in merged.enumerate() {
            if n % 1024 == 0 && settings.stop.load(Ordering::Relaxed) {
                return Ok(false);
            }
            let (key, value) = item?;
            if value.is_none() && drop_tombstone(&key) {
                continue;
            }
            if builder.is_none() {
                builder = Some(TableBuilder::new(
                    settings.dir,
                    next_id(),
                    settings.block_bytes,
                    settings.bloom_bits_per_key,
                )?);
            }
            let b = builder.as_mut().unwrap();
            b.add(&key, value.as_deref())?;
            if b.approx_size() >= settings.target_file_bytes {
                outputs.push(builder.take().unwrap().finish()?);
            }
        }
        if let Some(b) = builder.take() {
            outputs.push(b.finish()?);
        }
        Ok(true)
    })();
    match result {
        Ok(true) => Ok(Some(outputs)),
        other => {
            // Never leave half a compaction behind in this session (an interrupted one is cleaned up on open).
            for o in &outputs {
                let _ = std::fs::remove_file(crate::sstable::table_path(settings.dir, o.id));
            }
            other.map(|_| None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sstable::write_table;

    fn table(dir: &Path, id: u64, entries: &[(&str, Option<&str>)]) -> Arc<Table> {
        let m = write_table(dir, id, entries.iter().map(|(k, v)| (k.as_bytes(), v.map(str::as_bytes))), 64, 10).unwrap();
        Arc::new(Table::open(dir, id, m.size).unwrap())
    }

    #[test]
    fn merge_keeps_the_newest_version_and_the_order() {
        let dir = tempfile::tempdir().unwrap();
        let newest = table(dir.path(), 3, &[("b", Some("b3")), ("d", None)]);
        let middle = table(dir.path(), 2, &[("a", Some("a2")), ("b", Some("b2")), ("c", Some("c2"))]);
        let oldest = table(dir.path(), 1, &[("a", Some("a1")), ("d", Some("d1")), ("e", Some("e1"))]);
        let out: Vec<OwnedEntry> = Merge::new(&[newest, middle, oldest]).collect::<Result<_>>().unwrap();
        let want: Vec<OwnedEntry> = [
            ("a", Some("a2")),
            ("b", Some("b3")),
            ("c", Some("c2")),
            ("d", None),
            ("e", Some("e1")),
        ]
        .iter()
        .map(|(k, v)| (k.as_bytes().to_vec(), v.map(|v| v.as_bytes().to_vec())))
        .collect();
        assert_eq!(out, want);
    }

    #[test]
    fn outputs_are_split_by_size_and_tombstones_dropped_when_allowed() {
        let dir = tempfile::tempdir().unwrap();
        let entries: Vec<(String, Option<String>)> = (0..500)
            .map(|i| (format!("k{i:04}"), (i % 10 != 0).then(|| "v".repeat(50))))
            .collect();
        let t = {
            let m = write_table(
                dir.path(),
                1,
                entries.iter().map(|(k, v)| (k.as_bytes(), v.as_ref().map(|v| v.as_bytes()))),
                256,
                10,
            )
            .unwrap();
            Arc::new(Table::open(dir.path(), 1, m.size).unwrap())
        };
        let stop = AtomicBool::new(false);
        let settings = WriteSettings {
            dir: dir.path(),
            block_bytes: 256,
            bloom_bits_per_key: 10,
            target_file_bytes: 4096,
            stop: &stop,
        };
        let mut id = 10;
        let outs = write_outputs(
            Merge::new(std::slice::from_ref(&t)),
            &settings,
            || {
                id += 1;
                id
            },
            |_| true,
        )
        .unwrap()
        .unwrap();
        assert!(outs.len() > 3, "split into several tables: {}", outs.len());
        assert_eq!(outs.iter().map(|o| o.entries).sum::<u64>(), 450, "the 50 tombstones are dropped");
        let mut previous_last: Option<Vec<u8>> = None;
        for o in &outs {
            let t = Table::open(dir.path(), o.id, o.size).unwrap();
            if let Some(p) = &previous_last {
                assert!(t.smallest() > p.as_slice(), "outputs do not overlap");
            }
            previous_last = Some(t.largest().to_vec());
        }
    }

    #[test]
    fn a_stopped_compaction_leaves_no_files() {
        let dir = tempfile::tempdir().unwrap();
        let t = table(dir.path(), 1, &[("a", Some("1")), ("b", Some("2"))]);
        let stop = AtomicBool::new(true);
        let settings = WriteSettings {
            dir: dir.path(),
            block_bytes: 64,
            bloom_bits_per_key: 10,
            target_file_bytes: 1,
            stop: &stop,
        };
        assert!(write_outputs(Merge::new(&[t]), &settings, || 5, |_| false).unwrap().is_none());
        assert!(!dir.path().join("000005.sst").exists());
    }

    #[test]
    fn picking_level_zero_takes_every_table_and_the_overlapping_ones_below() {
        let dir = tempfile::tempdir().unwrap();
        let l0: Vec<_> = (0..4)
            .map(|i| table(dir.path(), 10 + i, &[("m", Some("x")), ("p", Some("y"))]))
            .collect();
        let l1 = vec![
            table(dir.path(), 1, &[("a", Some("1")), ("c", Some("1"))]),
            table(dir.path(), 2, &[("n", Some("1")), ("o", Some("1"))]),
            table(dir.path(), 3, &[("x", Some("1"))]),
        ];
        let tuning = Tuning {
            l0_trigger: 4,
            level_base_bytes: 1 << 30,
            level_multiplier: 10,
            target_file_bytes: 1 << 20,
        };
        let levels = vec![l0, l1];
        let plan = pick(&levels, &tuning, &mut vec![Vec::new(); 7]).unwrap();
        assert_eq!((plan.level, plan.output_level, plan.upper.len()), (0, 1, 4));
        assert_eq!(
            plan.lower.iter().map(|t| t.id()).collect::<Vec<_>>(),
            vec![2],
            "only the table overlapping m..p"
        );
        let mut fewer = levels.clone();
        fewer[0].pop();
        assert!(
            pick(&fewer, &tuning, &mut vec![Vec::new(); 7]).is_none(),
            "3 level-0 tables are under the trigger"
        );
    }

    #[test]
    fn deeper_levels_rotate_through_their_tables() {
        let dir = tempfile::tempdir().unwrap();
        let l1 = vec![
            table(dir.path(), 1, &[("a", Some("1"))]),
            table(dir.path(), 2, &[("b", Some("1"))]),
            table(dir.path(), 3, &[("c", Some("1"))]),
        ];
        let tuning = Tuning {
            l0_trigger: 4,
            level_base_bytes: 1,
            level_multiplier: 10,
            target_file_bytes: 1 << 20,
        };
        let levels = vec![vec![], l1];
        let mut pointers = vec![Vec::new(); 7];
        let picks: Vec<u64> = (0..4)
            .map(|_| pick(&levels, &tuning, &mut pointers).unwrap().upper[0].id())
            .collect();
        assert_eq!(picks, vec![1, 2, 3, 1]);
        assert!(pick(&levels, &tuning, &mut pointers).unwrap().is_trivial_move());
    }
}
