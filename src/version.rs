//! `CURRENT`: the one small file that says which files make up the database.
//!
//! ```text
//! stratakv-current 2
//! next_file 19
//! log 18
//! last_seq 41230
//! table 17 4194816 0
//! table 12 2097152 1
//! crc 1f2e3d4c
//! ```
//!
//! `log` is the oldest write-ahead log still needed: every log with that number or higher is replayed
//! on open, and older ones are leftovers. Each `table` line gives the table's id, exact size (so a table
//! cut short is noticed) and level. `last_seq` is the newest sequence number that is in a table.
//!
//! The file is replaced atomically: written as `CURRENT.tmp`, fsynced, renamed over `CURRENT`, and the
//! directory fsynced. After a crash the database is either entirely in the old state or entirely in the
//! new one. (LevelDB appends edits to a MANIFEST log instead; rewriting this file is simpler and costs
//! a few kilobytes per change at the sizes StrataKV targets.)

use std::fs;
use std::io::Write;
use std::path::Path;

use crate::error::{Error, Result};
use crate::failpoint::{self, Point};
use crate::wal::sync_dir;

pub const CURRENT: &str = "CURRENT";
const HEADER: &str = "stratakv-current 2";
/// Written by Phase 2 (no levels): every table is read as level 0.
const HEADER_V1: &str = "stratakv-current 1";
pub const MAX_LEVELS: u32 = 7;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TableEntry {
    pub id: u64,
    pub size: u64,
    pub level: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionState {
    pub next_file: u64,
    pub log_number: u64,
    pub last_seq: u64,
    pub tables: Vec<TableEntry>,
}

/// Which crash points a `store` call may fire (tests only).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoreFor {
    Open,
    Flush,
    Compaction,
}

impl VersionState {
    pub fn fresh() -> Self {
        Self {
            next_file: 2,
            log_number: 1,
            last_seq: 0,
            tables: Vec::new(),
        }
    }

    fn encode(&self) -> String {
        let mut body = format!(
            "{HEADER}\nnext_file {}\nlog {}\nlast_seq {}\n",
            self.next_file, self.log_number, self.last_seq
        );
        for t in &self.tables {
            body.push_str(&format!("table {} {} {}\n", t.id, t.size, t.level));
        }
        let crc = crc32fast::hash(body.as_bytes());
        body.push_str(&format!("crc {crc:08x}\n"));
        body
    }

    fn decode(text: &str) -> std::result::Result<Self, String> {
        let body_end = text.rfind("crc ").ok_or("no checksum line")?;
        let (body, crc_line) = text.split_at(body_end);
        if crc_line != format!("crc {:08x}\n", crc32fast::hash(body.as_bytes())) {
            return Err("checksum mismatch".into());
        }
        let mut lines = body.lines();
        let v1 = match lines.next() {
            Some(HEADER) => false,
            Some(HEADER_V1) => true,
            _ => return Err("unknown format version".into()),
        };
        let mut field = |name: &str| -> std::result::Result<u64, String> {
            let line = lines.next().ok_or(format!("missing {name}"))?;
            line.strip_prefix(name)
                .and_then(|v| v.strip_prefix(' '))
                .and_then(|v| v.parse().ok())
                .ok_or(format!("bad {name} line: {line:?}"))
        };
        let (next_file, log_number, last_seq) = (field("next_file")?, field("log")?, field("last_seq")?);
        let mut tables = Vec::new();
        for line in lines {
            let parts: Vec<&str> = line.split(' ').collect();
            let num = |i: usize| parts.get(i).and_then(|p| p.parse::<u64>().ok());
            let entry = match (parts.first(), parts.len(), v1) {
                (Some(&"table"), 3, true) => num(1).zip(num(2)).map(|(id, size)| TableEntry { id, size, level: 0 }),
                (Some(&"table"), 4, false) => match (num(1), num(2), num(3)) {
                    (Some(id), Some(size), Some(level)) if level < MAX_LEVELS as u64 => Some(TableEntry {
                        id,
                        size,
                        level: level as u32,
                    }),
                    _ => None,
                },
                _ => None,
            };
            tables.push(entry.ok_or(format!("bad table line: {line:?}"))?);
        }
        Ok(Self {
            next_file,
            log_number,
            last_seq,
            tables,
        })
    }
}

pub fn load(dir: &Path) -> Result<Option<VersionState>> {
    let path = dir.join(CURRENT);
    match fs::read_to_string(&path) {
        Ok(text) => VersionState::decode(&text).map(Some).map_err(|reason| Error::Corruption {
            file: path,
            offset: 0,
            reason,
        }),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
}

pub fn store(dir: &Path, state: &VersionState, why: StoreFor) -> Result<()> {
    let tmp = dir.join("CURRENT.tmp");
    let mut f = fs::File::create(&tmp)?;
    f.write_all(state.encode().as_bytes())?;
    f.sync_all()?;
    drop(f);
    match why {
        StoreFor::Flush => failpoint::hit(Point::FlushAfterCurrentTmp),
        StoreFor::Compaction => failpoint::hit(Point::CompactionAfterCurrentTmp),
        StoreFor::Open => {}
    }
    fs::rename(&tmp, dir.join(CURRENT))?;
    sync_dir(dir)?;
    match why {
        StoreFor::Flush => failpoint::hit(Point::FlushAfterCurrentRename),
        StoreFor::Compaction => failpoint::hit(Point::CompactionAfterCurrentRename),
        StoreFor::Open => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> VersionState {
        VersionState {
            next_file: 19,
            log_number: 18,
            last_seq: 41230,
            tables: vec![
                TableEntry {
                    id: 17,
                    size: 100,
                    level: 0,
                },
                TableEntry { id: 12, size: 5, level: 3 },
            ],
        }
    }

    #[test]
    fn round_trip() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(load(dir.path()).unwrap(), None);
        store(dir.path(), &sample(), StoreFor::Open).unwrap();
        assert_eq!(load(dir.path()).unwrap(), Some(sample()));
        assert!(!dir.path().join("CURRENT.tmp").exists());
    }

    #[test]
    fn phase_two_files_still_open_with_every_table_at_level_zero() {
        let body = "stratakv-current 1\nnext_file 9\nlog 8\nlast_seq 5\ntable 7 100\ntable 4 50\n";
        let text = format!("{body}crc {:08x}\n", crc32fast::hash(body.as_bytes()));
        let v = VersionState::decode(&text).unwrap();
        assert_eq!(
            v.tables,
            vec![
                TableEntry {
                    id: 7,
                    size: 100,
                    level: 0
                },
                TableEntry { id: 4, size: 50, level: 0 }
            ]
        );
    }

    #[test]
    fn levels_out_of_range_are_refused() {
        let body = "stratakv-current 2\nnext_file 9\nlog 8\nlast_seq 5\ntable 7 100 7\n";
        let text = format!("{body}crc {:08x}\n", crc32fast::hash(body.as_bytes()));
        assert!(VersionState::decode(&text).unwrap_err().contains("bad table line"));
    }

    #[test]
    fn any_damage_is_corruption() {
        let dir = tempfile::tempdir().unwrap();
        store(dir.path(), &sample(), StoreFor::Open).unwrap();
        let clean = fs::read(dir.path().join(CURRENT)).unwrap();
        for i in 0..clean.len() {
            let mut bad = clean.clone();
            bad[i] ^= 0x01;
            fs::write(dir.path().join(CURRENT), &bad).unwrap();
            assert!(load(dir.path()).is_err(), "byte {i}");
        }
        fs::write(dir.path().join(CURRENT), &clean[..clean.len() - 3]).unwrap();
        assert!(load(dir.path()).is_err());
    }
}
