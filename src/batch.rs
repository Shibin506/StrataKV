//! A group of writes that is applied atomically: after a crash, either every operation in a batch
//! is visible or none of them is.

use crate::error::{Error, Result};

pub const MAX_KEY_LEN: usize = 64 * 1024;
pub const MAX_VALUE_LEN: usize = 64 * 1024 * 1024;

const TAG_DELETE: u8 = 0;
const TAG_PUT: u8 = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Op {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

impl Op {
    pub fn key(&self) -> &[u8] {
        match self {
            Op::Put { key, .. } | Op::Delete { key } => key,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WriteBatch {
    ops: Vec<Op>,
}

impl WriteBatch {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn put(&mut self, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> &mut Self {
        self.ops.push(Op::Put {
            key: key.into(),
            value: value.into(),
        });
        self
    }

    pub fn delete(&mut self, key: impl Into<Vec<u8>>) -> &mut Self {
        self.ops.push(Op::Delete { key: key.into() });
        self
    }

    pub fn ops(&self) -> &[Op] {
        &self.ops
    }

    pub fn len(&self) -> usize {
        self.ops.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }

    pub(crate) fn validate(&self) -> Result<()> {
        for op in &self.ops {
            if op.key().len() > MAX_KEY_LEN {
                return Err(Error::InvalidArgument(format!(
                    "key of {} bytes is over the {MAX_KEY_LEN}-byte limit",
                    op.key().len()
                )));
            }
            if let Op::Put { value, .. } = op
                && value.len() > MAX_VALUE_LEN
            {
                return Err(Error::InvalidArgument(format!(
                    "value of {} bytes is over the {MAX_VALUE_LEN}-byte limit",
                    value.len()
                )));
            }
        }
        Ok(())
    }

    /// `count u32` then, per operation, `tag u8, key_len u32, key, [value_len u32, value]`. Little endian.
    pub(crate) fn encode_into(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&(self.ops.len() as u32).to_le_bytes());
        for op in &self.ops {
            match op {
                Op::Put { key, value } => {
                    out.push(TAG_PUT);
                    put_bytes(out, key);
                    put_bytes(out, value);
                }
                Op::Delete { key } => {
                    out.push(TAG_DELETE);
                    put_bytes(out, key);
                }
            }
        }
    }

    /// Strict decoding: trailing bytes, unknown tags and over-long lengths are all errors.
    pub(crate) fn decode(mut buf: &[u8]) -> std::result::Result<Self, String> {
        let count = take_u32(&mut buf)? as usize;
        let mut ops = Vec::with_capacity(count.min(1024));
        for _ in 0..count {
            let tag = *buf.first().ok_or("batch ends inside an operation")?;
            buf = &buf[1..];
            let key = take_bytes(&mut buf, MAX_KEY_LEN)?;
            ops.push(match tag {
                TAG_PUT => Op::Put {
                    key,
                    value: take_bytes(&mut buf, MAX_VALUE_LEN)?,
                },
                TAG_DELETE => Op::Delete { key },
                t => return Err(format!("unknown operation tag {t}")),
            });
        }
        if !buf.is_empty() {
            return Err(format!("{} unexpected bytes after the last operation", buf.len()));
        }
        Ok(Self { ops })
    }
}

fn put_bytes(out: &mut Vec<u8>, b: &[u8]) {
    out.extend_from_slice(&(b.len() as u32).to_le_bytes());
    out.extend_from_slice(b);
}

fn take_u32(buf: &mut &[u8]) -> std::result::Result<u32, String> {
    if buf.len() < 4 {
        return Err("batch ends inside a length".into());
    }
    let v = u32::from_le_bytes(buf[..4].try_into().unwrap());
    *buf = &buf[4..];
    Ok(v)
}

fn take_bytes(buf: &mut &[u8], max: usize) -> std::result::Result<Vec<u8>, String> {
    let n = take_u32(buf)? as usize;
    if n > max {
        return Err(format!("length {n} is over the limit {max}"));
    }
    if buf.len() < n {
        return Err("batch ends inside a key or value".into());
    }
    let v = buf[..n].to_vec();
    *buf = &buf[n..];
    Ok(v)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        let mut b = WriteBatch::new();
        b.put("a", "1").delete("b").put("", "").put(vec![0u8, 255], vec![7u8; 1000]);
        let mut buf = Vec::new();
        b.encode_into(&mut buf);
        assert_eq!(WriteBatch::decode(&buf).unwrap(), b);
    }

    #[test]
    fn rejects_trailing_bytes_and_truncation() {
        let mut b = WriteBatch::new();
        b.put("key", "value");
        let mut buf = Vec::new();
        b.encode_into(&mut buf);
        for cut in 0..buf.len() {
            assert!(WriteBatch::decode(&buf[..cut]).is_err(), "cut at {cut}");
        }
        buf.push(0);
        assert!(WriteBatch::decode(&buf).is_err());
    }

    #[test]
    fn rejects_unknown_tag() {
        let buf = [1, 0, 0, 0, 9, 0, 0, 0, 0];
        assert!(WriteBatch::decode(&buf).unwrap_err().contains("unknown operation tag"));
    }

    #[test]
    fn size_limits() {
        let mut b = WriteBatch::new();
        b.put(vec![0u8; MAX_KEY_LEN + 1], "v");
        assert!(b.validate().is_err());
        let mut ok = WriteBatch::new();
        ok.put(vec![0u8; MAX_KEY_LEN], "v");
        assert!(ok.validate().is_ok());
    }
}
