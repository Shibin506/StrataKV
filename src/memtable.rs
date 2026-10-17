//! The in-memory table: every version of every key written since the log was started.
//!
//! Entries are ordered by key, then by sequence number from newest to oldest, so the newest version a
//! reader may see (`seq <= snapshot`) is the first one found. A deleted key is kept as a tombstone
//! (`None`), so a delete can hide an older value stored on disk.

use std::cmp::Reverse;
use std::collections::{BTreeMap, VecDeque};
use std::ops::Bound;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock, RwLockReadGuard};

use crate::batch::{Op, WriteBatch};

/// One version of a key: key, sequence number, value (`None` = tombstone).
pub type Versioned = (Vec<u8>, u64, Option<Vec<u8>>);

/// Ordered by key, then newest version first.
type VersionKey = (Vec<u8>, Reverse<u64>);
type Map = BTreeMap<VersionKey, Option<Vec<u8>>>;

/// Shared between the writer and readers (scans hold it in an `Arc` while they run), so it has its own lock.
/// Versions are only ever added, never changed or removed: a reader that remembers where it stopped can
/// continue later and see exactly what it would have seen in one go, plus newer versions it skips.
#[derive(Debug, Default)]
pub struct Memtable {
    map: RwLock<Map>,
    approx_bytes: AtomicUsize,
}

/// What a memtable or a table knows about a key.
#[derive(Debug, PartialEq, Eq)]
pub enum Lookup<V> {
    Found(V),
    /// The newest visible version of the key is a delete: older layers must not be consulted.
    Deleted,
    /// This layer has no visible version of the key: look in older layers.
    Absent,
}

impl Memtable {
    fn map(&self) -> RwLockReadGuard<'_, Map> {
        self.map.read().unwrap_or_else(|e| e.into_inner())
    }

    /// Applies a batch; every operation gets the batch's sequence number (within one batch, the last
    /// operation on a key wins).
    pub fn apply(&self, seq: u64, batch: &WriteBatch) {
        let mut map = self.map.write().unwrap_or_else(|e| e.into_inner());
        let (mut added, mut removed) = (0, 0);
        for op in batch.ops() {
            let (key, value) = match op {
                Op::Put { key, value } => (key, Some(value.clone())),
                Op::Delete { key } => (key, None),
            };
            added += key.len() + 8 + value.as_ref().map_or(0, Vec::len);
            if let Some(old) = map.insert((key.clone(), Reverse(seq)), value) {
                removed += key.len() + 8 + old.map_or(0, |v| v.len());
            }
        }
        self.approx_bytes.fetch_add(added - removed, Ordering::Relaxed);
    }

    /// The newest version of `key` with a sequence number at or below `snapshot`.
    pub fn get(&self, key: &[u8], snapshot: u64) -> Lookup<Vec<u8>> {
        match self.map().range((key.to_vec(), Reverse(snapshot))..).next() {
            Some(((k, _), v)) if k.as_slice() == key => match v {
                Some(v) => Lookup::Found(v.clone()),
                None => Lookup::Deleted,
            },
            _ => Lookup::Absent,
        }
    }

    /// Read access to every version in order (used to write the memtable to a table). Writers wait while
    /// the guard is held, so only use it on a memtable that no longer receives writes.
    pub fn read(&self) -> Frozen<'_> {
        Frozen(self.map())
    }

    /// Every version whose key is within the bounds, in order, read lazily in small chunks: a scan that
    /// stops after 100 keys copies about 100 entries, not the whole memtable.
    pub fn range(self: &Arc<Self>, start: Bound<&[u8]>, end: Bound<&[u8]>) -> MemRange {
        let lo = match start {
            Bound::Included(k) => Bound::Included((k.to_vec(), Reverse(u64::MAX))),
            Bound::Excluded(k) => Bound::Excluded((k.to_vec(), Reverse(0))),
            Bound::Unbounded => Bound::Unbounded,
        };
        let hi = match end {
            Bound::Included(k) => Bound::Included((k.to_vec(), Reverse(0))),
            Bound::Excluded(k) => Bound::Excluded((k.to_vec(), Reverse(u64::MAX))),
            Bound::Unbounded => Bound::Unbounded,
        };
        let empty = match (&lo, &hi) {
            (Bound::Included(a), Bound::Included(b)) => a > b,
            (Bound::Included(a) | Bound::Excluded(a), Bound::Excluded(b)) | (Bound::Excluded(a), Bound::Included(b)) => a >= b,
            _ => false,
        };
        MemRange {
            mem: self.clone(),
            next: lo,
            end: hi,
            buf: VecDeque::new(),
            chunk: FIRST_CHUNK,
            done: empty,
        }
    }

    /// Keys plus values (all versions), roughly: what a flush to disk would have to write.
    pub fn approx_bytes(&self) -> usize {
        self.approx_bytes.load(Ordering::Relaxed)
    }

    /// Number of versions held.
    pub fn len(&self) -> usize {
        self.map().len()
    }

    pub fn is_empty(&self) -> bool {
        self.map().is_empty()
    }
}

pub struct Frozen<'a>(RwLockReadGuard<'a, Map>);

impl Frozen<'_> {
    /// Every version, ordered by key then newest first.
    pub fn iter(&self) -> impl Iterator<Item = (&[u8], u64, Option<&[u8]>)> {
        self.0.iter().map(|((k, Reverse(s)), v)| (k.as_slice(), *s, v.as_deref()))
    }
}

const FIRST_CHUNK: usize = 32;
const MAX_CHUNK: usize = 4096;

/// See [`Memtable::range`]. Each chunk is copied under the memtable's lock, which is then released, so a
/// slow reader never holds up writers. Chunks start small (short scans stay cheap) and double (long scans
/// lock rarely).
pub struct MemRange {
    mem: Arc<Memtable>,
    next: Bound<VersionKey>,
    end: Bound<VersionKey>,
    buf: VecDeque<Versioned>,
    chunk: usize,
    done: bool,
}

impl Iterator for MemRange {
    type Item = Versioned;

    fn next(&mut self) -> Option<Versioned> {
        if self.buf.is_empty() && !self.done {
            let map = self.mem.map();
            self.buf.extend(
                map.range((self.next.clone(), self.end.clone()))
                    .take(self.chunk)
                    .map(|((k, Reverse(s)), v)| (k.clone(), *s, v.clone())),
            );
            drop(map);
            match self.buf.back() {
                Some((k, s, _)) if self.buf.len() == self.chunk => self.next = Bound::Excluded((k.clone(), Reverse(*s))),
                _ => self.done = true,
            }
            self.chunk = (self.chunk * 2).min(MAX_CHUNK);
        }
        self.buf.pop_front()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn readers_see_the_newest_version_at_or_below_their_snapshot() {
        let m = Arc::new(Memtable::default());
        let mut b = WriteBatch::new();
        b.put("a", "1");
        m.apply(1, &b);
        let mut b = WriteBatch::new();
        b.put("a", "2").put("a", "22").delete("b");
        m.apply(2, &b);
        let mut b = WriteBatch::new();
        b.delete("a");
        m.apply(3, &b);
        assert_eq!(m.get(b"a", 0), Lookup::Absent);
        assert_eq!(m.get(b"a", 1), Lookup::Found(b"1".to_vec()));
        assert_eq!(m.get(b"a", 2), Lookup::Found(b"22".to_vec()), "the last operation in a batch wins");
        assert_eq!(m.get(b"a", 3), Lookup::Deleted);
        assert_eq!(m.get(b"a", u64::MAX), Lookup::Deleted);
        assert_eq!(m.get(b"b", 2), Lookup::Deleted);
        assert_eq!(m.get(b"b", 1), Lookup::Absent);
        assert_eq!(m.get(b"", 9), Lookup::Absent);
        assert_eq!(m.len(), 4);
        assert_eq!(
            m.read().iter().map(|(k, s, _)| (k.to_vec(), s)).collect::<Vec<_>>(),
            vec![(b"a".to_vec(), 3), (b"a".to_vec(), 2), (b"a".to_vec(), 1), (b"b".to_vec(), 2)]
        );
    }

    #[test]
    fn range_bounds() {
        let m = Arc::new(Memtable::default());
        for (i, k) in ["a", "b", "c", "d"].iter().enumerate() {
            let mut b = WriteBatch::new();
            b.put(*k, "v");
            m.apply(i as u64 + 1, &b);
        }
        let keys = |v: MemRange| v.map(|(k, _, _)| String::from_utf8(k).unwrap()).collect::<Vec<_>>();
        assert_eq!(keys(m.range(Bound::Included(b"b"), Bound::Excluded(b"d"))), ["b", "c"]);
        assert_eq!(keys(m.range(Bound::Excluded(b"b"), Bound::Included(b"d"))), ["c", "d"]);
        assert_eq!(keys(m.range(Bound::Unbounded, Bound::Unbounded)), ["a", "b", "c", "d"]);
    }

    #[test]
    fn empty_and_reversed_ranges_are_empty_not_a_panic() {
        let m = Arc::new(Memtable::default());
        let mut b = WriteBatch::new();
        b.put("b", "v");
        m.apply(1, &b);
        assert_eq!(m.range(Bound::Included(b"c"), Bound::Included(b"a")).count(), 0);
        assert_eq!(m.range(Bound::Excluded(b"b"), Bound::Excluded(b"b")).count(), 0);
        assert_eq!(m.range(Bound::Included(b"b"), Bound::Excluded(b"b")).count(), 0);
        assert_eq!(m.range(Bound::Excluded(b"b"), Bound::Included(b"b")).count(), 0);
        assert_eq!(m.range(Bound::Included(b"b"), Bound::Included(b"b")).count(), 1);
    }

    #[test]
    fn a_long_range_is_read_in_chunks_and_misses_nothing() {
        let m = Arc::new(Memtable::default());
        for i in 0..20_000u64 {
            let mut b = WriteBatch::new();
            b.put(format!("k{:06}", i % 7000), "v");
            m.apply(i + 1, &b);
        }
        let all: Vec<_> = m.range(Bound::Unbounded, Bound::Unbounded).collect();
        let expected: Vec<_> = m.read().iter().map(|(k, s, v)| (k.to_vec(), s, v.map(<[u8]>::to_vec))).collect();
        assert_eq!(all.len(), 20_000);
        assert_eq!(all, expected);
        let some: Vec<_> = m
            .range(Bound::Excluded(b"k000100"), Bound::Included(b"k006000"))
            .map(|(k, s, _)| (k, s))
            .collect();
        assert_eq!(some.first().unwrap().0, b"k000101");
        assert_eq!(some.last().unwrap().0, b"k006000");
        assert!(
            some.windows(2)
                .all(|w| (&w[0].0, std::cmp::Reverse(w[0].1)) < (&w[1].0, std::cmp::Reverse(w[1].1))),
            "strictly in order"
        );
    }

    #[test]
    fn writes_during_a_range_read_never_repeat_or_lose_older_versions() {
        let m = Arc::new(Memtable::default());
        for i in 0..1000u64 {
            let mut b = WriteBatch::new();
            b.put(format!("k{i:04}"), "old");
            m.apply(i + 1, &b);
        }
        let mut it = m.range(Bound::Unbounded, Bound::Unbounded);
        let mut seen = Vec::new();
        for n in 0..1000u64 {
            let (k, s, _) = it.next().unwrap();
            seen.push((k, s));
            let mut b = WriteBatch::new(); // a newer version of every key, behind and ahead of the reader
            b.put(format!("k{:04}", (n * 7) % 1000), "new");
            m.apply(10_000 + n, &b);
            if seen.len() == 999 {
                break;
            }
        }
        seen.extend(it.map(|(k, s, _)| (k, s)));
        let old: Vec<_> = seen.iter().filter(|(_, s)| *s <= 1000).cloned().collect();
        assert_eq!(
            old,
            (0..1000u64).map(|i| (format!("k{i:04}").into_bytes(), i + 1)).collect::<Vec<_>>()
        );
        assert!(
            seen.windows(2)
                .all(|w| (&w[0].0, std::cmp::Reverse(w[0].1)) < (&w[1].0, std::cmp::Reverse(w[1].1)))
        );
    }
}
