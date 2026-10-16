//! The write-ahead log: every batch is appended here (and, by default, fsynced) before the write
//! returns, so a crash can never lose a write that was acknowledged.
//!
//! Record layout (little endian):
//!
//! ```text
//! +-----------+-----------+------------------+-----------------------+
//! | crc32 u32 | len u32   | sequence u64     | encoded WriteBatch    |
//! +-----------+-----------+------------------+-----------------------+
//!               \___________ covered by crc32 (len and payload) ____/
//! ```
//!
//! What a crash can leave at the end of the file, and what recovery does:
//! a partial header, a record cut short, a last record whose bytes did not all reach the disk, or a
//! run of zero bytes (file systems may extend a file before its data lands). All of these are a
//! *torn tail*: the write was never acknowledged, so recovery stops there and cuts the file back.
//! A record that fails its checksum while more data follows it cannot be explained by a crash in
//! an append-only file, so it is reported as corruption instead of being skipped. And before
//! anything is cut off as a torn tail, recovery checks that no valid record exists further on: a
//! damaged *length* field can make an early record look as if it ran past the end of the file, and
//! treating that as a torn tail would silently drop every good record after it.

use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::batch::WriteBatch;
use crate::error::{Error, Result};

pub const HEADER_LEN: usize = 8;
/// Largest payload one record may carry (sequence number + encoded batch).
pub const MAX_RECORD_LEN: usize = 256 * 1024 * 1024;

#[derive(Debug)]
pub struct Recovered {
    pub records: Vec<(u64, WriteBatch)>,
    /// Bytes of the file that hold whole, verified records.
    pub valid_len: u64,
    /// Bytes after `valid_len` that were discarded as a torn tail (0 if the file ended cleanly).
    pub torn_bytes: u64,
}

fn checksum(len_bytes: &[u8; 4], payload: &[u8]) -> u32 {
    let mut h = crc32fast::Hasher::new();
    h.update(len_bytes);
    h.update(payload);
    h.finalize()
}

pub(crate) fn encode_record(seq: u64, batch: &WriteBatch, out: &mut Vec<u8>) -> Result<()> {
    let start = out.len();
    out.extend_from_slice(&[0u8; HEADER_LEN]);
    out.extend_from_slice(&seq.to_le_bytes());
    batch.encode_into(out);
    let payload_len = out.len() - start - HEADER_LEN;
    if payload_len > MAX_RECORD_LEN {
        out.truncate(start);
        return Err(Error::InvalidArgument(format!(
            "batch of {payload_len} bytes is over the {MAX_RECORD_LEN}-byte limit"
        )));
    }
    let len_bytes = (payload_len as u32).to_le_bytes();
    let crc = checksum(&len_bytes, &out[start + HEADER_LEN..]);
    out[start..start + 4].copy_from_slice(&crc.to_le_bytes());
    out[start + 4..start + 8].copy_from_slice(&len_bytes);
    Ok(())
}

/// Reads every whole record of a log. `after_seq` is the highest sequence number already applied
/// elsewhere: sequence numbers in the log must be strictly increasing and above it.
pub fn recover(path: &Path, after_seq: u64) -> Result<Recovered> {
    let data = match fs::read(path) {
        Ok(d) => d,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Vec::new(),
        Err(e) => return Err(e.into()),
    };
    let corrupt = |offset: usize, reason: String| Error::Corruption {
        file: path.to_path_buf(),
        offset: offset as u64,
        reason,
    };

    let mut records = Vec::new();
    let mut last_seq = after_seq;
    let mut pos = 0usize;
    while pos < data.len() {
        let rest = &data[pos..];
        if rest.iter().all(|&b| b == 0) || rest.len() < HEADER_LEN {
            break; // zero-filled or partial header: torn tail
        }
        let crc = u32::from_le_bytes(rest[0..4].try_into().unwrap());
        let len_bytes: [u8; 4] = rest[4..8].try_into().unwrap();
        let len = u32::from_le_bytes(len_bytes) as usize;
        let torn = HEADER_LEN + len > rest.len()                    // runs past the end of the file
            || (HEADER_LEN + len == rest.len() && checksum(&len_bytes, &rest[HEADER_LEN..]) != crc); // last record, damaged
        if torn {
            if let Some(at) = next_valid_record(&data, pos + 1, last_seq) {
                return Err(corrupt(pos, format!("damaged record, but a valid record follows at byte {at}")));
            }
            break;
        }
        let payload = &rest[HEADER_LEN..HEADER_LEN + len];
        if checksum(&len_bytes, payload) != crc {
            return Err(corrupt(pos, "checksum mismatch in a record that is followed by more data".into()));
        }
        if len < 8 {
            return Err(corrupt(pos, format!("record payload of {len} bytes is too short")));
        }
        let seq = u64::from_le_bytes(payload[..8].try_into().unwrap());
        if seq <= last_seq {
            return Err(corrupt(pos, format!("sequence number {seq} does not follow {last_seq}")));
        }
        let batch = WriteBatch::decode(&payload[8..]).map_err(|r| corrupt(pos, r))?;
        records.push((seq, batch));
        last_seq = seq;
        pos += HEADER_LEN + len;
    }
    Ok(Recovered {
        records,
        valid_len: pos as u64,
        torn_bytes: (data.len() - pos) as u64,
    })
}

/// The first offset at or after `from` where a whole record with a valid checksum, a sequence number
/// above `last_seq` and a decodable batch starts. Only used on the recovery path after damage.
fn next_valid_record(data: &[u8], from: usize, last_seq: u64) -> Option<usize> {
    (from..data.len().saturating_sub(HEADER_LEN)).find(|&q| {
        let rest = &data[q..];
        let len_bytes: [u8; 4] = rest[4..8].try_into().unwrap();
        let len = u32::from_le_bytes(len_bytes) as usize;
        if !(8..=MAX_RECORD_LEN).contains(&len) || HEADER_LEN + len > rest.len() {
            return false;
        }
        let payload = &rest[HEADER_LEN..HEADER_LEN + len];
        checksum(&len_bytes, payload) == u32::from_le_bytes(rest[0..4].try_into().unwrap())
            && u64::from_le_bytes(payload[..8].try_into().unwrap()) > last_seq
            && WriteBatch::decode(&payload[8..]).is_ok()
    })
}

pub struct WalWriter {
    file: File,
    path: PathBuf,
    len: u64,
    buf: Vec<u8>,
}

impl WalWriter {
    /// Opens the log for appending after cutting it back to `valid_len` (dropping any torn tail),
    /// so that new records never follow garbage.
    pub fn open(path: &Path, valid_len: u64) -> Result<Self> {
        let existed = path.exists();
        let file = OpenOptions::new().read(true).write(true).create(true).truncate(false).open(path)?;
        if file.metadata()?.len() != valid_len {
            file.set_len(valid_len)?;
            file.sync_all()?;
        }
        if !existed {
            sync_dir(path.parent().unwrap_or(Path::new(".")))?;
        }
        let mut w = Self {
            file,
            path: path.to_path_buf(),
            len: valid_len,
            buf: Vec::new(),
        };
        use std::io::Seek;
        w.file.seek(std::io::SeekFrom::Start(valid_len))?;
        Ok(w)
    }

    /// Appends one record. With `sync`, the data is on stable storage when this returns.
    pub fn append(&mut self, seq: u64, batch: &WriteBatch, sync: bool) -> Result<()> {
        self.buf.clear();
        encode_record(seq, batch, &mut self.buf)?;
        self.file.write_all(&self.buf)?;
        if sync {
            self.file.sync_data()?;
        }
        self.len += self.buf.len() as u64;
        Ok(())
    }

    pub fn len(&self) -> u64 {
        self.len
    }

    /// Test hook: swaps the file for a read-only handle, so the next append fails like a full or
    /// broken disk would.
    #[cfg(test)]
    pub(crate) fn break_for_test(&mut self) {
        self.file = File::open(&self.path).unwrap();
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// Makes a newly created (or renamed) file's directory entry durable.
pub(crate) fn sync_dir(dir: &Path) -> Result<()> {
    #[cfg(unix)]
    File::open(dir)?.sync_all()?;
    let _ = dir;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn batch(i: u32) -> WriteBatch {
        let mut b = WriteBatch::new();
        b.put(format!("key{i}"), vec![i as u8; (i as usize % 7) * 50]);
        b
    }

    fn write_log(path: &Path, n: u32) -> Vec<u64> {
        let mut w = WalWriter::open(path, 0).unwrap();
        let mut ends = vec![];
        for i in 1..=n {
            w.append(i as u64, &batch(i), false).unwrap();
            ends.push(w.len());
        }
        ends
    }

    #[test]
    fn empty_and_missing_logs() {
        let dir = tempfile::tempdir().unwrap();
        let r = recover(&dir.path().join("none.log"), 0).unwrap();
        assert!(r.records.is_empty() && r.valid_len == 0 && r.torn_bytes == 0);
    }

    #[test]
    fn every_cut_point_recovers_exactly_the_whole_records() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.log");
        let ends = write_log(&path, 6);
        let full = fs::read(&path).unwrap();
        for cut in 0..=full.len() {
            fs::write(&path, &full[..cut]).unwrap();
            let r = recover(&path, 0).unwrap();
            let whole = ends.iter().filter(|&&e| e as usize <= cut).count();
            assert_eq!(r.records.len(), whole, "cut at {cut}");
            assert_eq!(r.valid_len as usize + r.torn_bytes as usize, cut);
            for (k, (seq, b)) in r.records.iter().enumerate() {
                assert_eq!((*seq, b), ((k + 1) as u64, &batch(k as u32 + 1)));
            }
        }
    }

    #[test]
    fn zero_filled_tail_is_a_torn_tail() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.log");
        write_log(&path, 3);
        let mut data = fs::read(&path).unwrap();
        data.extend(std::iter::repeat_n(0u8, 4096));
        fs::write(&path, &data).unwrap();
        let r = recover(&path, 0).unwrap();
        assert_eq!((r.records.len(), r.torn_bytes), (3, 4096));
    }

    #[test]
    fn damage_in_the_last_record_is_a_torn_tail_but_damage_before_it_is_corruption() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.log");
        let ends = write_log(&path, 4);
        let clean = fs::read(&path).unwrap();

        let mut last = clean.clone();
        let i = last.len() - 1;
        last[i] ^= 0xff;
        fs::write(&path, &last).unwrap();
        let r = recover(&path, 0).unwrap();
        assert_eq!(r.records.len(), 3);

        // Any single damaged bit in the first three records (header, length or data) is reported as
        // corruption: never silently dropped as if it were the end of the log.
        for byte in 0..ends[2] as usize {
            for bit in [0x01u8, 0x80] {
                let mut bad = clean.clone();
                bad[byte] ^= bit;
                fs::write(&path, &bad).unwrap();
                assert!(
                    matches!(recover(&path, 0), Err(Error::Corruption { .. })),
                    "byte {byte} bit {bit:#x}"
                );
            }
        }
    }

    #[test]
    fn sequence_numbers_must_increase() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.log");
        let mut w = WalWriter::open(&path, 0).unwrap();
        w.append(5, &batch(1), false).unwrap();
        w.append(5, &batch(2), false).unwrap();
        w.append(6, &batch(3), false).unwrap();
        assert!(matches!(recover(&path, 0), Err(Error::Corruption { .. })));
        assert!(matches!(recover(&path, 5), Err(Error::Corruption { .. })));
    }

    #[test]
    fn reopening_cuts_the_torn_tail_before_appending() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.log");
        write_log(&path, 2);
        let mut data = fs::read(&path).unwrap();
        data.extend_from_slice(&[0xde, 0xad, 0xbe]);
        fs::write(&path, &data).unwrap();
        let r = recover(&path, 0).unwrap();
        let mut w = WalWriter::open(&path, r.valid_len).unwrap();
        assert_eq!(fs::metadata(&path).unwrap().len(), r.valid_len, "the file itself is cut back");
        w.append(3, &batch(3), false).unwrap();
        let again = recover(&path, 0).unwrap();
        assert_eq!((again.records.len(), again.torn_bytes), (3, 0));
    }

    #[test]
    fn a_long_torn_tail_is_gone_even_when_the_next_record_is_short() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.log");
        let ends = write_log(&path, 2);
        let mut data = fs::read(&path).unwrap();
        data.extend(std::iter::repeat_n(0u8, 10_000));
        fs::write(&path, &data).unwrap();
        let r = recover(&path, 0).unwrap();
        let mut w = WalWriter::open(&path, r.valid_len).unwrap();
        w.append(3, &WriteBatch::new(), false).unwrap();
        assert_eq!(fs::metadata(&path).unwrap().len(), w.len());
        let again = recover(&path, 0).unwrap();
        assert_eq!((again.records.len(), again.torn_bytes, again.valid_len), (3, 0, ends[1] + 20));
    }
}
