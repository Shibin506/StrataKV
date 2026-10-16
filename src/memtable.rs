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

/// What the memtable knows about a key.
#[derive(Debug, PartialEq, Eq)]
pub enum Lookup<'a> {
    Found(&'a [u8]),
    Deleted,
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

    pub fn get(&self, key: &[u8]) -> Lookup<'_> {
        match self.map.get(key) {
            Some(Some(v)) => Lookup::Found(v),
            Some(None) => Lookup::Deleted,
            None => Lookup::Absent,
        }
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
        assert_eq!(m.get(b"a"), Lookup::Found(b"22"));
        assert_eq!(m.get(b"b"), Lookup::Deleted);
        assert_eq!(m.get(b"c"), Lookup::Absent);
        assert_eq!(m.approx_bytes(), 1 + 2 + 1);
    }
}
