//! Sorted String Tables: the immutable files memtables are flushed to and compactions write.
//!
//! ```text
//! +---------+---------+-----+--------------+-------------+--------------------+
//! | block 0 | block 1 | ... | index block  | bloom block | footer (60 bytes)  |
//! +---------+---------+-----+--------------+-------------+--------------------+
//! ```
//!
//! * A data block (about `block_bytes`) holds entries ordered by key, then by sequence number from newest
//!   to oldest: `key_len u32, key, seq u64, tag u8` then, for a value (tag 1), `value_len u32, value`.
//!   Tag 0 is a tombstone: the key was deleted at that sequence number.
//! * The index lists, per block, its last key and sequence number, offset, length and CRC32. A lookup
//!   for "the newest version of K at or below snapshot S" binary-searches it and reads exactly one block.
//! * The Bloom filter (over keys) lets a lookup for a missing key skip the data almost always.
//! * The footer locates the index and filter, carries their CRC32s and the entry count, is itself
//!   checksummed, and ends with a magic number that also gives the format version. Every byte read
//!   from a table is checked.
//!
//! Format 1 (Phases 2 and 3) had no sequence numbers; such tables are still read, every entry counting as
//! sequence number 0 (older than anything written since).
//!
//! Tables are written to `NNNNNN.sst.tmp`, fsynced, then renamed into place, so a table that exists
//! under its final name is always complete.

use std::cmp::Ordering as Cmp;
use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Write};
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::bloom::{Bloom, hash64};
use crate::error::{Error, Result};
use crate::failpoint::{self, Point};
use crate::memtable::{Lookup, Versioned};
use crate::wal::sync_dir;

const MAGIC_V1: u64 = 0x5354_5241_5441_5353; // "STRATASS": no sequence numbers
const MAGIC_V2: u64 = 0x5354_5241_5441_5332; // "STRATAS2"
pub const FOOTER_LEN: usize = 60;
const TAG_TOMBSTONE: u8 = 0;
const TAG_VALUE: u8 = 1;

/// Internal order: key ascending, then sequence number descending (newest first).
pub fn internal_cmp(a_key: &[u8], a_seq: u64, b_key: &[u8], b_seq: u64) -> Cmp {
    a_key.cmp(b_key).then(b_seq.cmp(&a_seq))
}

pub fn table_path(dir: &Path, id: u64) -> PathBuf {
    dir.join(format!("{id:06}.sst"))
}

fn tmp_path(dir: &Path, id: u64) -> PathBuf {
    dir.join(format!("{id:06}.sst.tmp"))
}

#[derive(Debug, Clone)]
struct BlockHandle {
    last_key: Vec<u8>,
    last_seq: u64,
    offset: u64,
    len: u32,
    crc: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TableMeta {
    pub id: u64,
    pub size: u64,
    pub entries: u64,
}

/// Writes one table, entry by entry, in internal order. Used by flushes and by compaction, which splits
/// its output into several tables by watching `approx_size`.
pub struct TableBuilder {
    dir: PathBuf,
    id: u64,
    out: BufWriter<File>,
    block_bytes: usize,
    bloom_bits_per_key: u32,
    offset: u64,
    block: Vec<u8>,
    index: Vec<BlockHandle>,
    hashes: Vec<u64>,
    last: Option<(Vec<u8>, u64)>,
    count: u64,
    /// Whether the flush crash points fire (tests only).
    flush_points: bool,
}

impl TableBuilder {
    pub fn new(dir: &Path, id: u64, block_bytes: usize, bloom_bits_per_key: u32) -> Result<Self> {
        let file = OpenOptions::new().write(true).create(true).truncate(true).open(tmp_path(dir, id))?;
        Ok(Self {
            dir: dir.to_path_buf(),
            id,
            out: BufWriter::new(file),
            block_bytes,
            bloom_bits_per_key,
            offset: 0,
            block: Vec::with_capacity(block_bytes + 1024),
            index: Vec::new(),
            hashes: Vec::new(),
            last: None,
            count: 0,
            flush_points: false,
        })
    }

    /// Adds one version of a key; `None` is a tombstone. Entries must come in internal order.
    pub fn add(&mut self, key: &[u8], seq: u64, value: Option<&[u8]>) -> Result<()> {
        if let Some((pk, ps)) = &self.last {
            if internal_cmp(pk, *ps, key, seq) != Cmp::Less {
                return Err(Error::InvalidArgument(
                    "table entries must be in strictly increasing (key, newest first) order".into(),
                ));
            }
            if pk.as_slice() != key {
                self.hashes.push(hash64(key));
            }
        } else {
            self.hashes.push(hash64(key));
        }
        self.block.extend_from_slice(&(key.len() as u32).to_le_bytes());
        self.block.extend_from_slice(key);
        self.block.extend_from_slice(&seq.to_le_bytes());
        match value {
            Some(v) => {
                self.block.push(TAG_VALUE);
                self.block.extend_from_slice(&(v.len() as u32).to_le_bytes());
                self.block.extend_from_slice(v);
            }
            None => self.block.push(TAG_TOMBSTONE),
        }
        self.count += 1;
        self.last = Some((key.to_vec(), seq));
        if self.block.len() >= self.block_bytes {
            self.finish_block()?;
        }
        Ok(())
    }

    fn finish_block(&mut self) -> Result<()> {
        if self.block.is_empty() {
            return Ok(());
        }
        self.out.write_all(&self.block)?;
        let (last_key, last_seq) = self.last.clone().unwrap_or_default();
        self.index.push(BlockHandle {
            last_key,
            last_seq,
            offset: self.offset,
            len: self.block.len() as u32,
            crc: crc32fast::hash(&self.block),
        });
        self.offset += self.block.len() as u64;
        self.block.clear();
        Ok(())
    }

    pub fn entries(&self) -> u64 {
        self.count
    }

    /// The key of the last entry added.
    pub fn last_key(&self) -> Option<&[u8]> {
        self.last.as_ref().map(|(k, _)| k.as_slice())
    }

    /// Bytes written so far plus the block being built (index and filter not counted).
    pub fn approx_size(&self) -> u64 {
        self.offset + self.block.len() as u64
    }

    /// Writes index, filter and footer, fsyncs, and renames the table into place.
    pub fn finish(mut self) -> Result<TableMeta> {
        self.finish_block()?;
        let mut idx = Vec::new();
        idx.extend_from_slice(&(self.index.len() as u32).to_le_bytes());
        for h in &self.index {
            idx.extend_from_slice(&(h.last_key.len() as u32).to_le_bytes());
            idx.extend_from_slice(&h.last_key);
            idx.extend_from_slice(&h.last_seq.to_le_bytes());
            idx.extend_from_slice(&h.offset.to_le_bytes());
            idx.extend_from_slice(&h.len.to_le_bytes());
            idx.extend_from_slice(&h.crc.to_le_bytes());
        }
        let bloom = Bloom::build(&self.hashes, self.bloom_bits_per_key).encode();
        let (index_off, bloom_off) = (self.offset, self.offset + idx.len() as u64);
        self.out.write_all(&idx)?;
        self.out.write_all(&bloom)?;

        let mut footer = Vec::with_capacity(FOOTER_LEN);
        footer.extend_from_slice(&index_off.to_le_bytes());
        footer.extend_from_slice(&(idx.len() as u64).to_le_bytes());
        footer.extend_from_slice(&crc32fast::hash(&idx).to_le_bytes());
        footer.extend_from_slice(&bloom_off.to_le_bytes());
        footer.extend_from_slice(&(bloom.len() as u64).to_le_bytes());
        footer.extend_from_slice(&crc32fast::hash(&bloom).to_le_bytes());
        footer.extend_from_slice(&self.count.to_le_bytes());
        let fcrc = crc32fast::hash(&footer);
        footer.extend_from_slice(&fcrc.to_le_bytes());
        footer.extend_from_slice(&MAGIC_V2.to_le_bytes());
        debug_assert_eq!(footer.len(), FOOTER_LEN);
        self.out.write_all(&footer)?;

        let file = self.out.into_inner().map_err(|e| Error::Io(e.into_error()))?;
        file.sync_all()?;
        drop(file);
        if self.flush_points {
            failpoint::hit(Point::FlushAfterTableTmp);
        }
        fs::rename(tmp_path(&self.dir, self.id), table_path(&self.dir, self.id))?;
        sync_dir(&self.dir)?;
        if self.flush_points {
            failpoint::hit(Point::FlushAfterTableRename);
        }
        Ok(TableMeta {
            id: self.id,
            size: bloom_off + bloom.len() as u64 + FOOTER_LEN as u64,
            entries: self.count,
        })
    }
}

/// Writes `entries` (in internal order; `None` = tombstone) as table `id` in `dir` (a flush).
/// When this returns, the table is durable under its final name.
pub fn write_table<K: AsRef<[u8]>, V: AsRef<[u8]>>(
    dir: &Path,
    id: u64,
    entries: impl Iterator<Item = (K, u64, Option<V>)>,
    block_bytes: usize,
    bloom_bits_per_key: u32,
) -> Result<TableMeta> {
    let mut b = TableBuilder::new(dir, id, block_bytes, bloom_bits_per_key)?;
    b.flush_points = true;
    for (k, s, v) in entries {
        b.add(k.as_ref(), s, v.as_ref().map(|v| v.as_ref()))?;
    }
    b.finish()
}

/// An open, verified table. Cheap to share between threads (reads use positioned I/O).
#[derive(Debug)]
pub struct Table {
    id: u64,
    path: PathBuf,
    file: File,
    size: u64,
    entries: u64,
    /// Format 1 entries carry no sequence numbers.
    v1: bool,
    index: Vec<BlockHandle>,
    bloom: Bloom,
    block_reads: AtomicU64,
    smallest: Vec<u8>,
    largest: Vec<u8>,
}

fn read_at(file: &File, offset: u64, buf: &mut [u8]) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::FileExt;
        file.read_exact_at(buf, offset)
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::FileExt;
        let mut done = 0;
        while done < buf.len() {
            let n = file.seek_read(&mut buf[done..], offset + done as u64)?;
            if n == 0 {
                return Err(std::io::ErrorKind::UnexpectedEof.into());
            }
            done += n;
        }
        Ok(())
    }
}

impl Table {
    /// Opens table `id`, checking its size against `expected_size` (from `CURRENT`), the footer, the
    /// checksums of the index and the filter, and its first block.
    pub fn open(dir: &Path, id: u64, expected_size: u64) -> Result<Self> {
        let path = table_path(dir, id);
        let file = File::open(&path)?;
        let size = file.metadata()?.len();
        let corrupt = |offset: u64, reason: String| Error::Corruption {
            file: path.clone(),
            offset,
            reason,
        };
        if size != expected_size {
            return Err(corrupt(0, format!("table is {size} bytes, CURRENT says {expected_size}")));
        }
        if size < FOOTER_LEN as u64 {
            return Err(corrupt(0, "too small to hold a footer".into()));
        }
        let mut f = [0u8; FOOTER_LEN];
        read_at(&file, size - FOOTER_LEN as u64, &mut f)?;
        let u64_at = |i: usize| u64::from_le_bytes(f[i..i + 8].try_into().unwrap());
        let u32_at = |i: usize| u32::from_le_bytes(f[i..i + 4].try_into().unwrap());
        let foot = size - FOOTER_LEN as u64;
        let v1 = match u64_at(52) {
            MAGIC_V2 => false,
            MAGIC_V1 => true,
            _ => return Err(corrupt(foot, "bad magic number (not a StrataKV table, or cut short)".into())),
        };
        if crc32fast::hash(&f[..48]) != u32_at(48) {
            return Err(corrupt(foot, "footer checksum mismatch".into()));
        }
        let (index_off, index_len, index_crc) = (u64_at(0), u64_at(8), u32_at(16));
        let (bloom_off, bloom_len, bloom_crc) = (u64_at(20), u64_at(28), u32_at(36));
        let entries = u64_at(40);
        if index_off.checked_add(index_len) != Some(bloom_off) || bloom_off.checked_add(bloom_len) != Some(foot) {
            return Err(corrupt(foot, "footer offsets do not add up".into()));
        }

        let mut idx = vec![0u8; index_len as usize];
        read_at(&file, index_off, &mut idx)?;
        if crc32fast::hash(&idx) != index_crc {
            return Err(corrupt(index_off, "index checksum mismatch".into()));
        }
        let index = decode_index(&idx, index_off, v1).map_err(|r| corrupt(index_off, r))?;

        let mut bl = vec![0u8; bloom_len as usize];
        read_at(&file, bloom_off, &mut bl)?;
        if crc32fast::hash(&bl) != bloom_crc {
            return Err(corrupt(bloom_off, "filter checksum mismatch".into()));
        }
        let bloom = Bloom::decode(&bl).ok_or_else(|| corrupt(bloom_off, "unreadable filter".into()))?;
        let mut t = Table {
            id,
            path,
            file,
            size,
            entries,
            v1,
            index,
            bloom,
            block_reads: AtomicU64::new(0),
            smallest: Vec::new(),
            largest: Vec::new(),
        };
        if let Some(first) = t.index.first().cloned() {
            let block = t.read_block_uncounted(&first)?;
            let (k, _, _, _) = decode_entry(&block, v1).map_err(|reason| Error::Corruption {
                file: t.path.clone(),
                offset: first.offset,
                reason,
            })?;
            t.smallest = k.to_vec();
            t.largest = t.index.last().unwrap().last_key.clone();
        }
        Ok(t)
    }

    pub fn id(&self) -> u64 {
        self.id
    }
    pub fn size(&self) -> u64 {
        self.size
    }
    /// Number of entries (versions), tombstones included.
    pub fn entries(&self) -> u64 {
        self.entries
    }
    /// The first and last key in the table (both empty for an empty table).
    pub fn smallest(&self) -> &[u8] {
        &self.smallest
    }
    pub fn largest(&self) -> &[u8] {
        &self.largest
    }
    pub fn is_empty(&self) -> bool {
        self.entries == 0
    }
    /// Whether the key range `[lo, hi]` overlaps this table's.
    pub fn overlaps(&self, lo: &[u8], hi: &[u8]) -> bool {
        !self.is_empty() && self.smallest.as_slice() <= hi && lo <= self.largest.as_slice()
    }
    /// Whether any key within the bounds could be in this table.
    pub fn overlaps_bounds(&self, start: Bound<&[u8]>, end: Bound<&[u8]>) -> bool {
        if self.is_empty() {
            return false;
        }
        let after_start = match start {
            Bound::Included(s) => self.largest.as_slice() >= s,
            Bound::Excluded(s) => self.largest.as_slice() > s,
            Bound::Unbounded => true,
        };
        let before_end = match end {
            Bound::Included(e) => self.smallest.as_slice() <= e,
            Bound::Excluded(e) => self.smallest.as_slice() < e,
            Bound::Unbounded => true,
        };
        after_start && before_end
    }
    /// Data blocks read from disk since the table was opened.
    pub fn block_reads(&self) -> u64 {
        self.block_reads.load(Ordering::Relaxed)
    }

    fn read_block(&self, h: &BlockHandle) -> Result<Vec<u8>> {
        self.block_reads.fetch_add(1, Ordering::Relaxed);
        self.read_block_uncounted(h)
    }

    fn read_block_uncounted(&self, h: &BlockHandle) -> Result<Vec<u8>> {
        let mut buf = vec![0u8; h.len as usize];
        read_at(&self.file, h.offset, &mut buf)?;
        if crc32fast::hash(&buf) != h.crc {
            return Err(Error::Corruption {
                file: self.path.clone(),
                offset: h.offset,
                reason: "data block checksum mismatch".into(),
            });
        }
        Ok(buf)
    }

    /// Whether the Bloom filter rules the key out (no disk read needed).
    pub fn filter_excludes(&self, key: &[u8]) -> bool {
        !self.bloom.may_contain(key)
    }

    /// The newest version of `key` with a sequence number at or below `snapshot`.
    pub fn get(&self, key: &[u8], snapshot: u64) -> Result<Lookup<Vec<u8>>> {
        if self.filter_excludes(key) {
            return Ok(Lookup::Absent);
        }
        // The first block whose last entry is at or after (key, snapshot) holds the answer, if any.
        let i = self
            .index
            .partition_point(|h| internal_cmp(&h.last_key, h.last_seq, key, snapshot) == Cmp::Less);
        let Some(h) = self.index.get(i) else { return Ok(Lookup::Absent) };
        let block = self.read_block(h)?;
        let mut rest = block.as_slice();
        while !rest.is_empty() {
            let (k, seq, v, tail) = decode_entry(rest, self.v1).map_err(|r| Error::Corruption {
                file: self.path.clone(),
                offset: h.offset,
                reason: r,
            })?;
            rest = tail;
            match k.cmp(key) {
                Cmp::Less => continue,
                Cmp::Equal if seq > snapshot => continue,
                Cmp::Equal => return Ok(v.map_or(Lookup::Deleted, |v| Lookup::Found(v.to_vec()))),
                Cmp::Greater => break,
            }
        }
        Ok(Lookup::Absent)
    }

    /// Streams every entry in internal order, one block in memory at a time (compaction input).
    pub fn iter(self: &Arc<Self>) -> TableIter {
        self.iter_from(Bound::Unbounded)
    }

    /// Streams the entries whose key is at or after `start`.
    pub fn iter_from(self: &Arc<Self>, start: Bound<&[u8]>) -> TableIter {
        let first_block = match start {
            Bound::Included(s) => self.index.partition_point(|h| h.last_key.as_slice() < s),
            Bound::Excluded(s) => self.index.partition_point(|h| h.last_key.as_slice() <= s),
            Bound::Unbounded => 0,
        };
        TableIter {
            table: self.clone(),
            next_block: first_block,
            block: Vec::new(),
            pos: 0,
            failed: false,
            start: match start {
                Bound::Included(s) => Bound::Included(s.to_vec()),
                Bound::Excluded(s) => Bound::Excluded(s.to_vec()),
                Bound::Unbounded => Bound::Unbounded,
            },
        }
    }

    /// Every entry in internal order, each block checked as it is read.
    pub fn entries_in_order(self: &Arc<Self>) -> Result<Vec<Versioned>> {
        self.iter().collect()
    }
}

/// See [`Table::iter`]. Yields `Err` once (and then stops) if a block fails its checks.
pub struct TableIter {
    table: Arc<Table>,
    next_block: usize,
    block: Vec<u8>,
    pos: usize,
    failed: bool,
    start: Bound<Vec<u8>>,
}

impl Iterator for TableIter {
    type Item = Result<Versioned>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.failed {
                return None;
            }
            while self.pos >= self.block.len() {
                let h = self.table.index.get(self.next_block)?;
                match self.table.read_block(h) {
                    Ok(b) => self.block = b,
                    Err(e) => {
                        self.failed = true;
                        return Some(Err(e));
                    }
                }
                self.next_block += 1;
                self.pos = 0;
            }
            match decode_entry(&self.block[self.pos..], self.table.v1) {
                Ok((k, seq, v, rest)) => {
                    self.pos = self.block.len() - rest.len();
                    let before_start = match &self.start {
                        Bound::Included(s) => k < s.as_slice(),
                        Bound::Excluded(s) => k <= s.as_slice(),
                        Bound::Unbounded => false,
                    };
                    if before_start {
                        continue;
                    }
                    self.start = Bound::Unbounded; // entries are sorted: no need to check again
                    return Some(Ok((k.to_vec(), seq, v.map(<[u8]>::to_vec))));
                }
                Err(r) => {
                    self.failed = true;
                    let offset = self.table.index[self.next_block - 1].offset;
                    return Some(Err(Error::Corruption {
                        file: self.table.path.clone(),
                        offset,
                        reason: r,
                    }));
                }
            }
        }
    }
}

type Entry<'a> = (&'a [u8], u64, Option<&'a [u8]>, &'a [u8]);

fn take(buf: &[u8], n: usize) -> std::result::Result<(&[u8], &[u8]), String> {
    (buf.len() >= n)
        .then(|| buf.split_at(n))
        .ok_or_else(|| "entry runs past the end of its block".to_string())
}

fn decode_entry(buf: &[u8], v1: bool) -> std::result::Result<Entry<'_>, String> {
    let (len, buf) = take(buf, 4)?;
    let (key, buf) = take(buf, u32::from_le_bytes(len.try_into().unwrap()) as usize)?;
    let (seq, buf) = if v1 {
        (0, buf)
    } else {
        let (s, buf) = take(buf, 8)?;
        (u64::from_le_bytes(s.try_into().unwrap()), buf)
    };
    let (tag, buf) = take(buf, 1)?;
    match tag[0] {
        TAG_TOMBSTONE => Ok((key, seq, None, buf)),
        TAG_VALUE => {
            let (len, buf) = take(buf, 4)?;
            let (v, buf) = take(buf, u32::from_le_bytes(len.try_into().unwrap()) as usize)?;
            Ok((key, seq, Some(v), buf))
        }
        t => Err(format!("unknown entry tag {t}")),
    }
}

fn decode_index(mut buf: &[u8], data_end: u64, v1: bool) -> std::result::Result<Vec<BlockHandle>, String> {
    let (n, rest) = take(buf, 4)?;
    buf = rest;
    let n = u32::from_le_bytes(n.try_into().unwrap()) as usize;
    let mut out: Vec<BlockHandle> = Vec::with_capacity(n.min(1 << 20));
    let mut expected_offset = 0u64;
    for _ in 0..n {
        let (len, rest) = take(buf, 4)?;
        let (key, rest) = take(rest, u32::from_le_bytes(len.try_into().unwrap()) as usize)?;
        let (last_seq, rest) = if v1 {
            (0, rest)
        } else {
            let (s, rest) = take(rest, 8)?;
            (u64::from_le_bytes(s.try_into().unwrap()), rest)
        };
        let (fixed, rest) = take(rest, 16)?;
        buf = rest;
        let offset = u64::from_le_bytes(fixed[0..8].try_into().unwrap());
        let len = u32::from_le_bytes(fixed[8..12].try_into().unwrap());
        let crc = u32::from_le_bytes(fixed[12..16].try_into().unwrap());
        if offset != expected_offset {
            return Err("index blocks are not contiguous".into());
        }
        if out
            .last()
            .is_some_and(|p| internal_cmp(&p.last_key, p.last_seq, key, last_seq) != Cmp::Less)
        {
            return Err("index keys are not increasing".into());
        }
        expected_offset = offset + len as u64;
        out.push(BlockHandle {
            last_key: key.to_vec(),
            last_seq,
            offset,
            len,
            crc,
        });
    }
    if !buf.is_empty() || expected_offset != data_end {
        return Err("index does not cover the data exactly".into());
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Keys 0..n, each with 1 to 3 versions (sequence numbers below 1000), some versions tombstones.
    fn sample(n: usize) -> Vec<Versioned> {
        let mut out = Vec::new();
        for i in 0..n {
            for v in (0..=(i % 3) as u64).rev() {
                let seq = 10 * i as u64 + v;
                let value = (!(i + v as usize).is_multiple_of(5)).then(|| vec![(i % 251) as u8; (i * 7 + v as usize) % 200]);
                out.push((format!("key{i:06}").into_bytes(), seq, value));
            }
        }
        out
    }

    fn write(dir: &Path, id: u64, data: &[Versioned], block: usize) -> TableMeta {
        write_table(dir, id, data.iter().map(|(k, s, v)| (k.as_slice(), *s, v.as_deref())), block, 10).unwrap()
    }

    fn open(dir: &Path, m: TableMeta) -> Arc<Table> {
        Arc::new(Table::open(dir, m.id, m.size).unwrap())
    }

    /// What `get(key, snapshot)` must answer, worked out from the plain list.
    fn expected(data: &[Versioned], key: &[u8], snapshot: u64) -> Lookup<Vec<u8>> {
        match data.iter().find(|(k, s, _)| k.as_slice() == key && *s <= snapshot) {
            Some((_, _, Some(v))) => Lookup::Found(v.clone()),
            Some((_, _, None)) => Lookup::Deleted,
            None => Lookup::Absent,
        }
    }

    #[test]
    fn every_version_is_found_at_every_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let data = sample(1500);
        let t = open(dir.path(), write(dir.path(), 7, &data, 256));
        assert!(!tmp_path(dir.path(), 7).exists(), "the temporary file is renamed");
        assert_eq!(t.entries() as usize, data.len());
        for i in (0..1500).step_by(7) {
            let key = format!("key{i:06}").into_bytes();
            for snapshot in [0, 10 * i as u64, 10 * i as u64 + 1, 10 * i as u64 + 2, 10 * i as u64 + 3, u64::MAX] {
                assert_eq!(
                    t.get(&key, snapshot).unwrap(),
                    expected(&data, &key, snapshot),
                    "key {i} at {snapshot}"
                );
            }
        }
        for probe in [&b""[..], b"key", b"key000000x", b"key999999", b"zzz"] {
            assert_eq!(t.get(probe, u64::MAX).unwrap(), Lookup::Absent);
        }
        assert_eq!(t.entries_in_order().unwrap(), data);
        assert_eq!((t.smallest(), t.largest()), (&data[0].0[..], &data.last().unwrap().0[..]));
        assert!(t.overlaps(b"key000100", b"key000200") && t.overlaps(b"a", b"key000000") && !t.overlaps(b"z", b"zz"));
    }

    #[test]
    fn iterating_from_a_start_key() {
        let dir = tempfile::tempdir().unwrap();
        let data = sample(300);
        let t = open(dir.path(), write(dir.path(), 1, &data, 128));
        for start in [0usize, 1, 57, 299] {
            let key = format!("key{start:06}");
            let got: Vec<Versioned> = t.iter_from(Bound::Included(key.as_bytes())).collect::<Result<_>>().unwrap();
            let want: Vec<Versioned> = data.iter().filter(|(k, _, _)| k.as_slice() >= key.as_bytes()).cloned().collect();
            assert_eq!(got, want, "from {key}");
            let got: Vec<Versioned> = t.iter_from(Bound::Excluded(key.as_bytes())).collect::<Result<_>>().unwrap();
            let want: Vec<Versioned> = data.iter().filter(|(k, _, _)| k.as_slice() > key.as_bytes()).cloned().collect();
            assert_eq!(got, want, "after {key}");
        }
    }

    #[test]
    fn an_empty_table_and_a_one_entry_table() {
        let dir = tempfile::tempdir().unwrap();
        let t = open(dir.path(), write(dir.path(), 1, &[], 4096));
        assert_eq!(t.get(b"a", u64::MAX).unwrap(), Lookup::Absent);
        assert!(!t.overlaps_bounds(Bound::Unbounded, Bound::Unbounded));
        let one = vec![(b"k".to_vec(), 5, Some(b"v".to_vec()))];
        let t = open(dir.path(), write(dir.path(), 2, &one, 4096));
        assert_eq!(t.get(b"k", 5).unwrap(), Lookup::Found(b"v".to_vec()));
        assert_eq!(t.get(b"k", 4).unwrap(), Lookup::Absent);
    }

    #[test]
    fn entries_out_of_order_are_refused() {
        let dir = tempfile::tempdir().unwrap();
        for bad in [
            vec![(b"b".to_vec(), 1, None), (b"a".to_vec(), 1, None)],
            vec![(b"a".to_vec(), 1, None), (b"a".to_vec(), 2, None)], // same key: newest must come first
            vec![(b"a".to_vec(), 1, None), (b"a".to_vec(), 1, None)],
        ] {
            let r = write_table(
                dir.path(),
                1,
                bad.iter().map(|(k, s, v): &Versioned| (k.as_slice(), *s, v.as_deref())),
                4096,
                10,
            );
            assert!(r.is_err());
        }
    }

    #[test]
    fn every_damaged_byte_is_detected() {
        let dir = tempfile::tempdir().unwrap();
        let data = sample(120);
        let meta = write(dir.path(), 1, &data, 256);
        let path = table_path(dir.path(), 1);
        let clean = fs::read(&path).unwrap();
        for byte in 0..clean.len() {
            let mut bad = clean.clone();
            bad[byte] ^= 0x04;
            fs::write(&path, &bad).unwrap();
            // Damage must surface either when the table is opened (footer, index, filter, first block) or when
            // the damaged block is read. It must never produce a wrong answer.
            let Ok(t) = Table::open(dir.path(), 1, meta.size) else { continue };
            match Arc::new(t).entries_in_order() {
                Err(Error::Corruption { .. }) => {}
                Ok(read) => panic!("byte {byte}: damage went unnoticed (entries equal: {})", read == data),
                Err(e) => panic!("byte {byte}: {e}"),
            }
        }
    }

    #[test]
    fn a_table_cut_short_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let meta = write(dir.path(), 1, &sample(100), 256);
        let path = table_path(dir.path(), 1);
        let data = fs::read(&path).unwrap();
        fs::write(&path, &data[..data.len() - 1]).unwrap();
        assert!(matches!(Table::open(dir.path(), 1, meta.size), Err(Error::Corruption { .. })));
        assert!(matches!(Table::open(dir.path(), 1, meta.size - 1), Err(Error::Corruption { .. })));
    }

    /// Writes a table in format 1 (Phases 2 and 3: no sequence numbers) to check it still reads.
    fn write_v1(dir: &Path, id: u64, entries: &[(&str, Option<&str>)]) -> u64 {
        let mut block = Vec::new();
        for (k, v) in entries {
            block.extend_from_slice(&(k.len() as u32).to_le_bytes());
            block.extend_from_slice(k.as_bytes());
            match v {
                Some(v) => {
                    block.push(TAG_VALUE);
                    block.extend_from_slice(&(v.len() as u32).to_le_bytes());
                    block.extend_from_slice(v.as_bytes());
                }
                None => block.push(TAG_TOMBSTONE),
            }
        }
        let last = entries.last().unwrap().0;
        let mut idx = 1u32.to_le_bytes().to_vec();
        idx.extend_from_slice(&(last.len() as u32).to_le_bytes());
        idx.extend_from_slice(last.as_bytes());
        idx.extend_from_slice(&0u64.to_le_bytes());
        idx.extend_from_slice(&(block.len() as u32).to_le_bytes());
        idx.extend_from_slice(&crc32fast::hash(&block).to_le_bytes());
        let bloom = Bloom::build(&entries.iter().map(|(k, _)| hash64(k.as_bytes())).collect::<Vec<_>>(), 10).encode();
        let mut file = block.clone();
        let index_off = file.len() as u64;
        file.extend_from_slice(&idx);
        let bloom_off = file.len() as u64;
        file.extend_from_slice(&bloom);
        let mut footer = Vec::new();
        footer.extend_from_slice(&index_off.to_le_bytes());
        footer.extend_from_slice(&(idx.len() as u64).to_le_bytes());
        footer.extend_from_slice(&crc32fast::hash(&idx).to_le_bytes());
        footer.extend_from_slice(&bloom_off.to_le_bytes());
        footer.extend_from_slice(&(bloom.len() as u64).to_le_bytes());
        footer.extend_from_slice(&crc32fast::hash(&bloom).to_le_bytes());
        footer.extend_from_slice(&(entries.len() as u64).to_le_bytes());
        let c = crc32fast::hash(&footer);
        footer.extend_from_slice(&c.to_le_bytes());
        footer.extend_from_slice(&MAGIC_V1.to_le_bytes());
        file.extend_from_slice(&footer);
        fs::write(table_path(dir, id), &file).unwrap();
        file.len() as u64
    }

    #[test]
    fn format_one_tables_still_read_with_every_entry_at_sequence_zero() {
        let dir = tempfile::tempdir().unwrap();
        let size = write_v1(dir.path(), 3, &[("a", Some("1")), ("b", None), ("c", Some("3"))]);
        let t = Arc::new(Table::open(dir.path(), 3, size).unwrap());
        assert_eq!(t.get(b"a", 0).unwrap(), Lookup::Found(b"1".to_vec()));
        assert_eq!(t.get(b"b", u64::MAX).unwrap(), Lookup::Deleted);
        assert_eq!(t.get(b"d", u64::MAX).unwrap(), Lookup::Absent);
        assert_eq!(
            t.entries_in_order().unwrap(),
            vec![
                (b"a".to_vec(), 0, Some(b"1".to_vec())),
                (b"b".to_vec(), 0, None),
                (b"c".to_vec(), 0, Some(b"3".to_vec()))
            ]
        );
    }
}
