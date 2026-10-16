//! `CURRENT`: the one small file that says which files make up the database.
//!
//! ```text
//! stratakv-current 1
//! next_file 9
//! log 8
//! last_seq 41230
//! table 7 4194816
//! table 4 4190002
//! crc 1f2e3d4c
//! ```
//!
//! `log` is the oldest write-ahead log still needed: every log with that number or higher is replayed
//! on open, and older ones are leftovers. `table` lines (newest first) give each table's exact size,
//! so a table that was cut short is noticed. `last_seq` is the newest sequence number that is in a table.
//!
//! The file is replaced atomically: written as `CURRENT.tmp`, fsynced, renamed over `CURRENT`, and the
//! directory fsynced. After a crash, the database is either entirely in the old state or entirely in
//! the new one.

use std::fs;
use std::io::Write;
use std::path::Path;

use crate::error::{Error, Result};
use crate::failpoint::{self, Point};
use crate::wal::sync_dir;

pub const CURRENT: &str = "CURRENT";
const HEADER: &str = "stratakv-current 1";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TableEntry {
    pub id: u64,
    pub size: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionState {
    pub next_file: u64,
    pub log_number: u64,
    pub last_seq: u64,
    /// Newest first.
    pub tables: Vec<TableEntry>,
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
            body.push_str(&format!("table {} {}\n", t.id, t.size));
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
        if lines.next() != Some(HEADER) {
            return Err("unknown format version".into());
        }
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
            let mut parts = line.split(' ');
            match (
                parts.next(),
                parts.next().and_then(|p| p.parse().ok()),
                parts.next().and_then(|p| p.parse().ok()),
                parts.next(),
            ) {
                (Some("table"), Some(id), Some(size), None) => tables.push(TableEntry { id, size }),
                _ => return Err(format!("bad table line: {line:?}")),
            }
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

/// `during_flush` arms the flush crash points (tests only); opening a new database does not.
pub fn store(dir: &Path, state: &VersionState, during_flush: bool) -> Result<()> {
    let tmp = dir.join("CURRENT.tmp");
    let mut f = fs::File::create(&tmp)?;
    f.write_all(state.encode().as_bytes())?;
    f.sync_all()?;
    drop(f);
    if during_flush {
        failpoint::hit(Point::FlushAfterCurrentTmp);
    }
    fs::rename(&tmp, dir.join(CURRENT))?;
    sync_dir(dir)?;
    if during_flush {
        failpoint::hit(Point::FlushAfterCurrentRename);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(load(dir.path()).unwrap(), None);
        let s = VersionState {
            next_file: 9,
            log_number: 8,
            last_seq: 41230,
            tables: vec![TableEntry { id: 7, size: 100 }, TableEntry { id: 4, size: 5 }],
        };
        store(dir.path(), &s, false).unwrap();
        assert_eq!(load(dir.path()).unwrap(), Some(s));
        assert!(!dir.path().join("CURRENT.tmp").exists());
    }

    #[test]
    fn any_damage_is_corruption() {
        let dir = tempfile::tempdir().unwrap();
        store(
            dir.path(),
            &VersionState {
                next_file: 3,
                log_number: 2,
                last_seq: 7,
                tables: vec![TableEntry { id: 1, size: 99 }],
            },
            false,
        )
        .unwrap();
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
