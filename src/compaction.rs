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
use crate::memtable::Versioned;
use crate::sstable::{Table, TableBuilder, TableMeta};
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

/// A pending entry in the merge: key, sequence number (newest first), source index (0 = newest), value.
type Head = (Vec<u8>, Reverse<u64>, usize, Option<Vec<u8>>);

/// A source of versions in internal order (a table, or a copy of part of a memtable).
pub type Source = Box<dyn Iterator<Item = Result<Versioned>> + Send>;

/// Merges sources into one stream of every version, in internal order (key, then newest first). Sources
/// are given newest first: if two hold the exact same (key, sequence number), only the newest source's
/// entry is kept (this only happens with format-1 tables, whose entries all have sequence number 0).
pub struct Merge {
    sources: Vec<Source>,
    heap: BinaryHeap<Reverse<Head>>,
    started: bool,
}

impl Merge {
    pub fn new(sources: Vec<Source>) -> Self {
        Self {
            sources,
            heap: BinaryHeap::new(),
            started: false,
        }
    }

    pub fn of_tables(newest_first: &[Arc<Table>]) -> Self {
        Self::new(newest_first.iter().map(|t| Box::new(t.iter()) as Source).collect())
    }

    fn pull(&mut self, i: usize) -> Result<()> {
        if let Some(item) = self.sources[i].next() {
            let (k, seq, v) = item?;
            self.heap.push(Reverse((k, Reverse(seq), i, v)));
        }
        Ok(())
    }
}

impl Iterator for Merge {
    type Item = Result<Versioned>;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.started {
            self.started = true;
            for i in 0..self.sources.len() {
                if let Err(e) = self.pull(i) {
                    return Some(Err(e));
                }
            }
        }
        let Reverse((key, Reverse(seq), source, value)) = self.heap.pop()?;
        if let Err(e) = self.pull(source) {
            return Some(Err(e));
        }
        while self
            .heap
            .peek()
            .is_some_and(|Reverse((k, Reverse(s), _, _))| *k == key && *s == seq)
        {
            let Reverse((_, _, older, _)) = self.heap.pop().unwrap();
            if let Err(e) = self.pull(older) {
                return Some(Err(e));
            }
        }
        Some(Ok((key, seq, value)))
    }
}

pub struct WriteSettings<'a> {
    pub dir: &'a Path,
    pub block_bytes: usize,
    pub bloom_bits_per_key: u32,
    pub target_file_bytes: u64,
    pub stop: &'a AtomicBool,
}

/// Which versions a compaction may leave out.
pub struct Retention<F: Fn(&[u8]) -> bool> {
    /// The oldest snapshot still open (`u64::MAX` if none): every version newer than it must be kept, and
    /// so must the newest version at or below it.
    pub smallest_snapshot: u64,
    /// Whether no older version of the key can exist below the output level (so a tombstone may go).
    pub nothing_older_below: F,
}

/// Writes the merged stream as new tables of about `target_file_bytes` each, never splitting the versions
/// of one key between two tables (so tables in a level never overlap). Leaves out:
///
/// * a version when a newer version of the same key is already visible to every open snapshot;
/// * a tombstone that every snapshot sees and that hides nothing below the output level.
///
/// Returns `None` if `stop` was set (the partial outputs are deleted).
pub fn write_outputs<F: Fn(&[u8]) -> bool>(
    merged: impl Iterator<Item = Result<Versioned>>,
    settings: &WriteSettings<'_>,
    mut next_id: impl FnMut() -> u64,
    retention: &Retention<F>,
) -> Result<Option<Vec<TableMeta>>> {
    let mut outputs: Vec<TableMeta> = Vec::new();
    let mut builder: Option<TableBuilder> = None;
    let result = (|| -> Result<bool> {
        let mut current_key: Option<Vec<u8>> = None;
        let mut newer_seq: Option<u64> = None; // sequence number of the previous (newer) version of the current key
        for (n, item) in merged.enumerate() {
            if n % 1024 == 0 && settings.stop.load(Ordering::Relaxed) {
                return Ok(false);
            }
            let (key, seq, value) = item?;
            if current_key.as_deref() != Some(key.as_slice()) {
                current_key = Some(key.clone());
                newer_seq = None;
            }
            let shadowed = newer_seq.is_some_and(|newer| newer <= retention.smallest_snapshot);
            let dead_tombstone = value.is_none() && seq <= retention.smallest_snapshot && (retention.nothing_older_below)(&key);
            newer_seq = Some(seq);
            if shadowed || dead_tombstone {
                continue;
            }
            // Cut a new file only between keys: all versions of one key stay in one table.
            if let Some(b) = &builder
                && b.approx_size() >= settings.target_file_bytes
                && b.last_key() != Some(key.as_slice())
            {
                outputs.push(builder.take().unwrap().finish()?);
            }
            if builder.is_none() {
                builder = Some(TableBuilder::new(
                    settings.dir,
                    next_id(),
                    settings.block_bytes,
                    settings.bloom_bits_per_key,
                )?);
            }
            builder.as_mut().unwrap().add(&key, seq, value.as_deref())?;
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

    fn table(dir: &Path, id: u64, entries: &[(&str, u64, Option<&str>)]) -> Arc<Table> {
        let m = write_table(
            dir,
            id,
            entries.iter().map(|(k, s, v)| (k.as_bytes(), *s, v.map(str::as_bytes))),
            64,
            10,
        )
        .unwrap();
        Arc::new(Table::open(dir, id, m.size).unwrap())
    }

    fn owned(entries: &[(&str, u64, Option<&str>)]) -> Vec<Versioned> {
        entries
            .iter()
            .map(|(k, s, v)| (k.as_bytes().to_vec(), *s, v.map(|v| v.as_bytes().to_vec())))
            .collect()
    }

    fn keep_all() -> Retention<fn(&[u8]) -> bool> {
        Retention {
            smallest_snapshot: 0,
            nothing_older_below: |_| false,
        }
    }

    #[test]
    fn merge_yields_every_version_in_order() {
        let dir = tempfile::tempdir().unwrap();
        let newest = table(dir.path(), 3, &[("b", 30, Some("b3")), ("d", 31, None)]);
        let middle = table(
            dir.path(),
            2,
            &[("a", 20, Some("a2")), ("b", 21, Some("b2")), ("c", 22, Some("c2"))],
        );
        let oldest = table(
            dir.path(),
            1,
            &[("a", 10, Some("a1")), ("d", 11, Some("d1")), ("e", 12, Some("e1"))],
        );
        let out: Vec<Versioned> = Merge::of_tables(&[newest, middle, oldest]).collect::<Result<_>>().unwrap();
        assert_eq!(
            out,
            owned(&[
                ("a", 20, Some("a2")),
                ("a", 10, Some("a1")),
                ("b", 30, Some("b3")),
                ("b", 21, Some("b2")),
                ("c", 22, Some("c2")),
                ("d", 31, None),
                ("d", 11, Some("d1")),
                ("e", 12, Some("e1"))
            ])
        );
    }

    #[test]
    fn identical_versions_keep_the_newest_source() {
        let dir = tempfile::tempdir().unwrap();
        let newer = table(dir.path(), 2, &[("a", 0, Some("new"))]);
        let older = table(dir.path(), 1, &[("a", 0, Some("old"))]);
        let out: Vec<Versioned> = Merge::of_tables(&[newer, older]).collect::<Result<_>>().unwrap();
        assert_eq!(out, owned(&[("a", 0, Some("new"))]));
    }

    fn compact(entries: &[(&str, u64, Option<&str>)], smallest_snapshot: u64, nothing_below: bool) -> Vec<Versioned> {
        let dir = tempfile::tempdir().unwrap();
        let t = table(dir.path(), 1, entries);
        let stop = AtomicBool::new(false);
        let settings = WriteSettings {
            dir: dir.path(),
            block_bytes: 64,
            bloom_bits_per_key: 10,
            target_file_bytes: 1 << 20,
            stop: &stop,
        };
        let retention = Retention {
            smallest_snapshot,
            nothing_older_below: move |_: &[u8]| nothing_below,
        };
        let outs = write_outputs(Merge::of_tables(&[t]), &settings, || 9, &retention).unwrap().unwrap();
        outs.iter()
            .flat_map(|o| Arc::new(Table::open(dir.path(), o.id, o.size).unwrap()).entries_in_order().unwrap())
            .collect()
    }

    #[test]
    fn without_snapshots_only_the_newest_version_survives() {
        let input = [
            ("a", 30, Some("a3")),
            ("a", 20, Some("a2")),
            ("a", 10, Some("a1")),
            ("b", 15, None),
            ("b", 5, Some("b0")),
        ];
        assert_eq!(
            compact(&input, u64::MAX, false),
            owned(&[("a", 30, Some("a3")), ("b", 15, None)]),
            "the tombstone stays: something older may be below"
        );
        assert_eq!(
            compact(&input, u64::MAX, true),
            owned(&[("a", 30, Some("a3"))]),
            "nothing below: the tombstone and what it hid go"
        );
    }

    #[test]
    fn a_snapshot_keeps_the_versions_it_can_see() {
        let input = [
            ("a", 30, Some("a3")),
            ("a", 20, Some("a2")),
            ("a", 10, Some("a1")),
            ("b", 15, None),
            ("b", 5, Some("b0")),
        ];
        // A snapshot at 25 sees a2, and b as deleted: with nothing below, the tombstone and b0 can both go. At 12 it sees a1 and b0.
        assert_eq!(compact(&input, 25, true), owned(&[("a", 30, Some("a3")), ("a", 20, Some("a2"))]));
        assert_eq!(
            compact(&input, 25, false),
            owned(&[("a", 30, Some("a3")), ("a", 20, Some("a2")), ("b", 15, None)])
        );
        assert_eq!(
            compact(&input, 12, true),
            owned(&[
                ("a", 30, Some("a3")),
                ("a", 20, Some("a2")),
                ("a", 10, Some("a1")),
                ("b", 15, None),
                ("b", 5, Some("b0"))
            ])
        );
        assert_eq!(
            compact(&input, 0, true),
            owned(&input),
            "the oldest snapshot predates everything: all kept"
        );
    }

    #[test]
    fn outputs_are_split_by_size_but_never_inside_one_key() {
        let dir = tempfile::tempdir().unwrap();
        let mut entries: Vec<(String, u64, Option<String>)> = Vec::new();
        for i in 0..200u64 {
            for v in (0..3u64).rev() {
                entries.push((format!("k{i:04}"), i * 10 + v, Some("v".repeat(40))));
            }
        }
        let t = {
            let m = write_table(
                dir.path(),
                1,
                entries.iter().map(|(k, s, v)| (k.as_bytes(), *s, v.as_ref().map(|v| v.as_bytes()))),
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
            target_file_bytes: 1000,
            stop: &stop,
        };
        let mut id = 10;
        let outs = write_outputs(
            Merge::of_tables(std::slice::from_ref(&t)),
            &settings,
            || {
                id += 1;
                id
            },
            &keep_all(),
        )
        .unwrap()
        .unwrap();
        assert!(outs.len() > 10, "split into several tables: {}", outs.len());
        assert_eq!(outs.iter().map(|o| o.entries).sum::<u64>(), 600, "every version kept");
        let mut previous_last: Option<Vec<u8>> = None;
        for o in &outs {
            let t = Table::open(dir.path(), o.id, o.size).unwrap();
            if let Some(p) = &previous_last {
                assert!(
                    t.smallest() > p.as_slice(),
                    "outputs do not overlap, even with several versions per key"
                );
            }
            previous_last = Some(t.largest().to_vec());
        }
    }

    #[test]
    fn a_stopped_compaction_leaves_no_files() {
        let dir = tempfile::tempdir().unwrap();
        let t = table(dir.path(), 1, &[("a", 1, Some("1")), ("b", 2, Some("2"))]);
        let stop = AtomicBool::new(true);
        let settings = WriteSettings {
            dir: dir.path(),
            block_bytes: 64,
            bloom_bits_per_key: 10,
            target_file_bytes: 1,
            stop: &stop,
        };
        assert!(
            write_outputs(Merge::of_tables(&[t]), &settings, || 5, &keep_all())
                .unwrap()
                .is_none()
        );
        assert!(!dir.path().join("000005.sst").exists());
    }

    #[test]
    fn picking_level_zero_takes_every_table_and_the_overlapping_ones_below() {
        let dir = tempfile::tempdir().unwrap();
        let l0: Vec<_> = (0..4)
            .map(|i| table(dir.path(), 10 + i, &[("m", 1, Some("x")), ("p", 1, Some("y"))]))
            .collect();
        let l1 = vec![
            table(dir.path(), 1, &[("a", 1, Some("1")), ("c", 1, Some("1"))]),
            table(dir.path(), 2, &[("n", 1, Some("1")), ("o", 1, Some("1"))]),
            table(dir.path(), 3, &[("x", 1, Some("1"))]),
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
            table(dir.path(), 1, &[("a", 1, Some("1"))]),
            table(dir.path(), 2, &[("b", 1, Some("1"))]),
            table(dir.path(), 3, &[("c", 1, Some("1"))]),
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
