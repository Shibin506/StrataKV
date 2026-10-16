//! The in-memory table: every version of every key written since the log was started.
//!
//! Entries are ordered by key, then by sequence number from newest to oldest, so the newest version a
//! reader may see (`seq <= snapshot`) is the first one found. A deleted key is kept as a tombstone
//! (`None`), so a delete can hide an older value stored on disk.

use std::cmp::Reverse;
use std::collections::BTreeMap;
use std::ops::Bound;

use crate::batch::{Op, WriteBatch};

/// One version of a key: key, sequence number, value (`None` = tombstone).
pub type Versioned = (Vec<u8>, u64, Option<Vec<u8>>);

/// Ordered by key, then newest version first.
type VersionKey = (Vec<u8>, Reverse<u64>);

#[derive(Debug, Default)]
pub struct Memtable {
    map: BTreeMap<VersionKey, Option<Vec<u8>>>,
    approx_bytes: usize,
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
    /// Applies a batch; every operation gets the batch's sequence number (within one batch, the last
    /// operation on a key wins).
    pub fn apply(&mut self, seq: u64, batch: &WriteBatch) {
        for op in batch.ops() {
            let (key, value) = match op {
                Op::Put { key, value } => (key, Some(value.clone())),
                Op::Delete { key } => (key, None),
            };
            let added = key.len() + 8 + value.as_ref().map_or(0, Vec::len);
            if let Some(old) = self.map.insert((key.clone(), Reverse(seq)), value) {
                self.approx_bytes -= key.len() + 8 + old.map_or(0, |v| v.len());
            }
            self.approx_bytes += added;
        }
    }

    /// The newest version of `key` with a sequence number at or below `snapshot`.
    pub fn get(&self, key: &[u8], snapshot: u64) -> Lookup<&[u8]> {
        match self.map.range((key.to_vec(), Reverse(snapshot))..).next() {
            Some(((k, _), v)) if k.as_slice() == key => match v {
                Some(v) => Lookup::Found(v.as_slice()),
                None => Lookup::Deleted,
            },
            _ => Lookup::Absent,
        }
    }

    /// Every version, ordered by key then newest first.
    pub fn iter(&self) -> impl Iterator<Item = (&[u8], u64, Option<&[u8]>)> {
        self.map.iter().map(|((k, Reverse(s)), v)| (k.as_slice(), *s, v.as_deref()))
    }

    /// A copy of every version whose key is within the bounds (a range scan reads from this copy, so later
    /// writes to the memtable do not affect it).
    pub fn range(&self, start: Bound<&[u8]>, end: Bound<&[u8]>) -> Vec<Versioned> {
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
        self.map
            .range((lo, hi))
            .map(|((k, Reverse(s)), v)| (k.clone(), *s, v.clone()))
            .collect()
    }

    /// Keys plus values (all versions), roughly: what a flush to disk would have to write.
    pub fn approx_bytes(&self) -> usize {
        self.approx_bytes
    }

    /// Number of versions held.
    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn readers_see_the_newest_version_at_or_below_their_snapshot() {
        let mut m = Memtable::default();
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
        assert_eq!(m.get(b"a", 1), Lookup::Found(&b"1"[..]));
        assert_eq!(m.get(b"a", 2), Lookup::Found(&b"22"[..]), "the last operation in a batch wins");
        assert_eq!(m.get(b"a", 3), Lookup::Deleted);
        assert_eq!(m.get(b"a", u64::MAX), Lookup::Deleted);
        assert_eq!(m.get(b"b", 2), Lookup::Deleted);
        assert_eq!(m.get(b"b", 1), Lookup::Absent);
        assert_eq!(m.get(b"", 9), Lookup::Absent);
        assert_eq!(m.len(), 4);
        assert_eq!(
            m.iter().map(|(k, s, _)| (k.to_vec(), s)).collect::<Vec<_>>(),
            vec![(b"a".to_vec(), 3), (b"a".to_vec(), 2), (b"a".to_vec(), 1), (b"b".to_vec(), 2)]
        );
    }

    #[test]
    fn range_bounds() {
        let mut m = Memtable::default();
        for (i, k) in ["a", "b", "c", "d"].iter().enumerate() {
            let mut b = WriteBatch::new();
            b.put(*k, "v");
            m.apply(i as u64 + 1, &b);
        }
        let keys = |v: Vec<Versioned>| v.into_iter().map(|(k, _, _)| String::from_utf8(k).unwrap()).collect::<Vec<_>>();
        assert_eq!(keys(m.range(Bound::Included(b"b"), Bound::Excluded(b"d"))), ["b", "c"]);
        assert_eq!(keys(m.range(Bound::Excluded(b"b"), Bound::Included(b"d"))), ["c", "d"]);
        assert_eq!(keys(m.range(Bound::Unbounded, Bound::Unbounded)), ["a", "b", "c", "d"]);
    }
}
