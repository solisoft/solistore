//! Write batches: a set of puts, deletes and range deletes applied atomically.
//!
//! A batch is encoded once, as it is built, in the form its WAL record takes:
//!
//! ```text
//! payload := first_seq u64 LE | count u32 LE | op*
//! op      := 1 cf key value      put
//!          | 0 cf key            delete
//!          | 2 cf start end      delete [start, end)
//! cf      := varint;  key, value, start, end := varint length | bytes
//! ```
//!
//! Each op takes the next sequence number after `first_seq`.

use crate::coding::{
    get_bytes, get_u32_le, get_u64_le, get_u8, get_varint_u32, put_bytes, put_varint,
};
use crate::db::ColumnFamily;
use crate::error::{Error, Result};

pub(crate) const HEADER_LEN: usize = 12;
const OP_DELETE: u8 = 0;
const OP_PUT: u8 = 1;
const OP_DELETE_RANGE: u8 = 2;

#[derive(Clone)]
pub struct WriteBatch {
    /// Encoded ops, without the 12-byte header (written at commit time).
    ops: Vec<u8>,
    count: u32,
    /// The column families this batch writes, to refuse a dropped one.
    cfs: Vec<ColumnFamily>,
}

impl Default for WriteBatch {
    fn default() -> Self {
        Self::new()
    }
}

impl WriteBatch {
    pub fn new() -> Self {
        WriteBatch {
            ops: Vec::new(),
            count: 0,
            cfs: Vec::new(),
        }
    }

    pub fn put(&mut self, cf: &ColumnFamily, key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) {
        self.op(cf, OP_PUT);
        put_bytes(&mut self.ops, key.as_ref());
        put_bytes(&mut self.ops, value.as_ref());
    }

    pub fn delete(&mut self, cf: &ColumnFamily, key: impl AsRef<[u8]>) {
        self.op(cf, OP_DELETE);
        put_bytes(&mut self.ops, key.as_ref());
    }

    /// Delete every key in `[start, end)`. An empty range is a no-op.
    pub fn delete_range(
        &mut self,
        cf: &ColumnFamily,
        start: impl AsRef<[u8]>,
        end: impl AsRef<[u8]>,
    ) {
        let (start, end) = (start.as_ref(), end.as_ref());
        if start >= end {
            return;
        }
        self.op(cf, OP_DELETE_RANGE);
        put_bytes(&mut self.ops, start);
        put_bytes(&mut self.ops, end);
    }

    fn op(&mut self, cf: &ColumnFamily, kind: u8) {
        self.ops.push(kind);
        put_varint(&mut self.ops, cf.id() as u64);
        self.count += 1;
        if !self.cfs.iter().any(|c| c.id() == cf.id()) {
            self.cfs.push(cf.clone());
        }
    }

    /// Number of operations (each takes one sequence number).
    pub fn len(&self) -> u32 {
        self.count
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Encoded size, which is what the batch costs in the WAL.
    pub fn size_in_bytes(&self) -> usize {
        HEADER_LEN + self.ops.len()
    }

    pub fn clear(&mut self) {
        self.ops.clear();
        self.count = 0;
        self.cfs.clear();
    }

    pub(crate) fn column_families(&self) -> &[ColumnFamily] {
        &self.cfs
    }

    pub(crate) fn header(&self, first_seq: u64) -> [u8; HEADER_LEN] {
        let mut header = [0; HEADER_LEN];
        header[..8].copy_from_slice(&first_seq.to_le_bytes());
        header[8..].copy_from_slice(&self.count.to_le_bytes());
        header
    }

    pub(crate) fn ops_bytes(&self) -> &[u8] {
        &self.ops
    }

    pub(crate) fn ops(&self) -> Vec<BatchOp<'_>> {
        decode_ops(&self.ops, self.count).expect("a batch built through its API decodes")
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum BatchOp<'a> {
    Put {
        cf: u32,
        key: &'a [u8],
        value: &'a [u8],
    },
    Delete {
        cf: u32,
        key: &'a [u8],
    },
    DeleteRange {
        cf: u32,
        start: &'a [u8],
        end: &'a [u8],
    },
}

/// A decoded batch: its first sequence number and its ops in order.
pub struct DecodedBatch<'a> {
    pub first_seq: u64,
    pub ops: Vec<BatchOp<'a>>,
}

pub fn decode(payload: &[u8]) -> Result<DecodedBatch<'_>> {
    let corrupt = || Error::Corruption("malformed write batch".into());
    let mut input = payload;
    let first_seq = get_u64_le(&mut input).ok_or_else(corrupt)?;
    let count = get_u32_le(&mut input).ok_or_else(corrupt)?;
    Ok(DecodedBatch {
        first_seq,
        ops: decode_ops(input, count)?,
    })
}

/// The ops of an encoded batch body (no header).
pub fn decode_ops(mut input: &[u8], count: u32) -> Result<Vec<BatchOp<'_>>> {
    let corrupt = || Error::Corruption("malformed write batch".into());
    let mut ops = Vec::with_capacity(count.min(1 << 16) as usize);
    for _ in 0..count {
        let kind = get_u8(&mut input).ok_or_else(corrupt)?;
        let cf = get_varint_u32(&mut input).ok_or_else(corrupt)?;
        let a = get_bytes(&mut input).ok_or_else(corrupt)?;
        ops.push(match kind {
            OP_PUT => BatchOp::Put {
                cf,
                key: a,
                value: get_bytes(&mut input).ok_or_else(corrupt)?,
            },
            OP_DELETE => BatchOp::Delete { cf, key: a },
            OP_DELETE_RANGE => BatchOp::DeleteRange {
                cf,
                start: a,
                end: get_bytes(&mut input).ok_or_else(corrupt)?,
            },
            _ => return Err(corrupt()),
        });
    }
    if !input.is_empty() {
        return Err(corrupt());
    }
    Ok(ops)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_batch_decodes_to_what_was_added() {
        let cf = ColumnFamily::for_tests(3, "c");
        let mut batch = WriteBatch::new();
        batch.put(&cf, b"k1", b"v1");
        batch.delete(&cf, b"k2");
        batch.delete_range(&cf, b"a", b"m");
        batch.delete_range(&cf, b"z", b"a"); // empty: dropped
        assert_eq!(batch.len(), 3);

        let mut payload = batch.header(42).to_vec();
        payload.extend_from_slice(batch.ops_bytes());
        let decoded = decode(&payload).unwrap();
        assert_eq!(decoded.first_seq, 42);
        assert_eq!(
            decoded.ops,
            vec![
                BatchOp::Put {
                    cf: 3,
                    key: b"k1",
                    value: b"v1"
                },
                BatchOp::Delete { cf: 3, key: b"k2" },
                BatchOp::DeleteRange {
                    cf: 3,
                    start: b"a",
                    end: b"m"
                },
            ]
        );
    }

    #[test]
    fn a_truncated_or_padded_batch_is_corrupt() {
        let cf = ColumnFamily::for_tests(0, "default");
        let mut batch = WriteBatch::new();
        batch.put(&cf, b"key", b"value");
        let mut payload = batch.header(1).to_vec();
        payload.extend_from_slice(batch.ops_bytes());
        for cut in 0..payload.len() {
            assert!(decode(&payload[..cut]).is_err(), "cut at {cut}");
        }
        payload.push(0);
        assert!(decode(&payload).is_err());
    }
}
