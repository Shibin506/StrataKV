//! The in-memory table: the newest version of every key written since the log was started.
//! A deleted key is kept as a tombstone (`None`), which later phases need so that a delete can hide
//! an older value stored on disk.

use std::collections::BTreeMap;

use crate::batch::{Op, WriteBatch};

#[derive(Debug, Default)]
pub struct Memtable {
    map: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    approx_bytes: usize,
}

/// What a memtable or a table knows about a key.
#[derive(Debug, PartialEq, Eq)]
pub enum Lookup<V> {
    Found(V),
    /// The newest version of the key is a delete: older tables must not be consulted.
    Deleted,
    /// This layer knows nothing about the key: look in older layers.
    Absent,
}

impl Memtable {
    pub fn apply(&mut self, batch: &WriteBatch) {
        for op in batch.ops() {
            let (key, value) = match op {
                Op::Put { key, value } => (key, Some(value.clone())),
                Op::Delete { key } => (key, None),
            };
            let added = key.len() + value.as_ref().map_or(0, Vec::len);
            match self.map.insert(key.clone(), value) {
                Some(old) => {
                    self.approx_bytes = self.approx_bytes - old.map_or(0, |v| v.len()) + added - key.len();
                }
                None => self.approx_bytes += added,
            }
        }
    }

    pub fn get(&self, key: &[u8]) -> Lookup<&[u8]> {
        match self.map.get(key) {
            Some(Some(v)) => Lookup::Found(v.as_slice()),
            Some(None) => Lookup::Deleted,
            None => Lookup::Absent,
        }
    }

    /// Entries in key order, tombstones as `None`.
    pub fn iter(&self) -> impl Iterator<Item = (&[u8], Option<&[u8]>)> {
        self.map.iter().map(|(k, v)| (k.as_slice(), v.as_deref()))
    }

    /// Keys plus values, roughly: what a flush to disk would have to write.
    pub fn approx_bytes(&self) -> usize {
        self.approx_bytes
    }

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
    fn newest_write_wins_and_deletes_are_remembered() {
        let mut m = Memtable::default();
        let mut b = WriteBatch::new();
        b.put("a", "1").put("a", "22").delete("b");
        m.apply(&b);
        assert_eq!(m.get(b"a"), Lookup::Found(&b"22"[..]));
        assert_eq!(m.get(b"b"), Lookup::Deleted);
        assert_eq!(m.get(b"c"), Lookup::Absent);
        assert_eq!(m.approx_bytes(), 1 + 2 + 1);
    }
}
