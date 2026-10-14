//! Blocks: sorted key/value entries with prefix-compressed keys.
//!
//! ```text
//! block   := entry* | restart u32 LE * n | n u32 LE
//! entry   := shared varint | unshared varint | value_len varint | key[shared..] | value
//! ```
//!
//! Every `RESTART_INTERVAL`-th key is stored whole (`shared` = 0) and its
//! offset listed in the restart array, so a seek binary-searches the restart
//! points and then scans at most an interval. Keys are internal keys and
//! compare with [`key::compare`].

use std::cmp::Ordering;

use bytes::Bytes;

use crate::coding::{get_varint, put_varint};
use crate::error::{Error, Result};
use crate::key;

pub const RESTART_INTERVAL: usize = 16;

pub struct BlockBuilder {
    buf: Vec<u8>,
    restarts: Vec<u32>,
    interval: usize,
    counter: usize,
    last_key: Vec<u8>,
}

impl BlockBuilder {
    pub fn new(interval: usize) -> Self {
        BlockBuilder {
            buf: Vec::new(),
            restarts: vec![0],
            interval,
            counter: 0,
            last_key: Vec::new(),
        }
    }

    /// Keys must be added in order.
    pub fn add(&mut self, key: &[u8], value: &[u8]) {
        let shared = if self.counter < self.interval {
            self.last_key
                .iter()
                .zip(key)
                .take_while(|(a, b)| a == b)
                .count()
        } else {
            self.restarts.push(self.buf.len() as u32);
            self.counter = 0;
            0
        };
        put_varint(&mut self.buf, shared as u64);
        put_varint(&mut self.buf, (key.len() - shared) as u64);
        put_varint(&mut self.buf, value.len() as u64);
        self.buf.extend_from_slice(&key[shared..]);
        self.buf.extend_from_slice(value);
        self.last_key.clear();
        self.last_key.extend_from_slice(key);
        self.counter += 1;
    }

    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    pub fn size_estimate(&self) -> usize {
        self.buf.len() + 4 * self.restarts.len() + 4
    }

    /// The finished block; the builder is reset for the next one.
    pub fn finish(&mut self) -> Vec<u8> {
        let mut out = std::mem::take(&mut self.buf);
        for r in &self.restarts {
            out.extend_from_slice(&r.to_le_bytes());
        }
        out.extend_from_slice(&(self.restarts.len() as u32).to_le_bytes());
        self.restarts = vec![0];
        self.counter = 0;
        self.last_key.clear();
        out
    }
}

/// A decoded block, shared by every iterator over it.
#[derive(Clone)]
pub struct Block {
    data: Bytes,
    /// Where the restart array starts (= where entries end).
    restarts: usize,
    num_restarts: usize,
}

fn corrupt() -> Error {
    Error::Corruption("malformed block".into())
}

impl Block {
    pub fn new(data: Bytes) -> Result<Block> {
        let n = data.len().checked_sub(4).ok_or_else(corrupt)?;
        let num_restarts = u32::from_le_bytes(data[n..].try_into().expect("4 bytes")) as usize;
        let restarts = num_restarts
            .checked_mul(4)
            .and_then(|len| n.checked_sub(len))
            .ok_or_else(corrupt)?;
        if num_restarts == 0 {
            return Err(corrupt());
        }
        Ok(Block {
            data,
            restarts,
            num_restarts,
        })
    }

    pub fn size(&self) -> usize {
        self.data.len()
    }

    fn restart_point(&self, i: usize) -> usize {
        let at = self.restarts + 4 * i;
        u32::from_le_bytes(self.data[at..at + 4].try_into().expect("4 bytes")) as usize
    }

    pub fn iter(&self) -> BlockIter {
        BlockIter {
            block: self.clone(),
            offset: self.restarts,
            next_offset: self.restarts,
            key: Vec::new(),
            value: (0, 0),
            restart: 0,
            corrupt: false,
        }
    }
}

/// A cursor over one block.
pub struct BlockIter {
    block: Block,
    /// Offset of the current entry; `block.restarts` when not valid.
    offset: usize,
    next_offset: usize,
    key: Vec<u8>,
    value: (usize, usize),
    /// The restart interval holding the current entry.
    restart: usize,
    corrupt: bool,
}

impl BlockIter {
    pub fn valid(&self) -> bool {
        self.offset < self.block.restarts
    }

    pub fn key(&self) -> &[u8] {
        &self.key
    }

    pub fn value(&self) -> Bytes {
        self.block.data.slice(self.value.0..self.value.1)
    }

    pub fn status(&self) -> Result<()> {
        if self.corrupt {
            Err(corrupt())
        } else {
            Ok(())
        }
    }

    fn invalidate(&mut self) {
        self.offset = self.block.restarts;
        self.next_offset = self.block.restarts;
    }

    /// Decode the entry at `next_offset`, building on the current key.
    fn parse_next(&mut self) -> bool {
        self.offset = self.next_offset;
        if self.offset >= self.block.restarts {
            self.invalidate();
            return false;
        }
        let data = &self.block.data[..self.block.restarts];
        let mut input = &data[self.offset..];
        let header = (|| {
            Some((
                get_varint(&mut input)?,
                get_varint(&mut input)?,
                get_varint(&mut input)?,
            ))
        })();
        let Some((shared, unshared, value_len)) = header else {
            return self.fail();
        };
        let (shared, unshared, value_len) =
            (shared as usize, unshared as usize, value_len as usize);
        let key_start = data.len() - input.len();
        let Some(value_end) = key_start
            .checked_add(unshared)
            .and_then(|v| v.checked_add(value_len))
        else {
            return self.fail();
        };
        if shared > self.key.len() || value_end > data.len() {
            return self.fail();
        }
        self.key.truncate(shared);
        self.key
            .extend_from_slice(&data[key_start..key_start + unshared]);
        self.value = (key_start + unshared, value_end);
        self.next_offset = value_end;
        while self.restart + 1 < self.block.num_restarts
            && self.block.restart_point(self.restart + 1) <= self.offset
        {
            self.restart += 1;
        }
        true
    }

    fn fail(&mut self) -> bool {
        self.corrupt = true;
        self.invalidate();
        false
    }

    fn seek_to_restart(&mut self, i: usize) {
        self.key.clear();
        self.restart = i;
        self.next_offset = self.block.restart_point(i);
    }

    pub fn seek_to_first(&mut self) {
        self.seek_to_restart(0);
        self.parse_next();
    }

    pub fn seek_to_last(&mut self) {
        self.seek_to_restart(self.block.num_restarts - 1);
        while self.parse_next() && self.next_offset < self.block.restarts {}
    }

    pub fn next(&mut self) {
        debug_assert!(self.valid());
        self.parse_next();
    }

    pub fn prev(&mut self) {
        debug_assert!(self.valid());
        let current = self.offset;
        // Back to the restart point before the current entry, then forward
        // to the entry just before it.
        let mut r = self.restart;
        while self.block.restart_point(r) >= current {
            if r == 0 {
                self.invalidate();
                return;
            }
            r -= 1;
        }
        self.seek_to_restart(r);
        while self.parse_next() && self.next_offset < current {}
    }

    /// The first entry with a key at or after `target`.
    pub fn seek(&mut self, target: &[u8]) {
        // Last restart point whose key is before `target`.
        let (mut left, mut right) = (0, self.block.num_restarts - 1);
        while left < right {
            let mid = (left + right).div_ceil(2);
            self.seek_to_restart(mid);
            if !self.parse_next() {
                return;
            }
            if key::compare(&self.key, target) == Ordering::Less {
                left = mid;
            } else {
                right = mid - 1;
            }
        }
        self.seek_to_restart(left);
        while self.parse_next() {
            if key::compare(&self.key, target) != Ordering::Less {
                return;
            }
        }
    }

    /// The last entry with a key at or before `target`.
    pub fn seek_for_prev(&mut self, target: &[u8]) {
        self.seek(target);
        if !self.valid() {
            if !self.corrupt {
                self.seek_to_last();
            }
        } else if key::compare(&self.key, target) == Ordering::Greater {
            self.prev();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::key::{encode, Kind};

    fn keys(n: u64) -> Vec<Vec<u8>> {
        (0..n)
            .map(|i| encode(0, format!("key{i:05}").as_bytes(), 1, Kind::Put))
            .collect()
    }

    fn build(keys: &[Vec<u8>]) -> Block {
        let mut b = BlockBuilder::new(RESTART_INTERVAL);
        for (i, k) in keys.iter().enumerate() {
            b.add(k, format!("v{i}").as_bytes());
        }
        Block::new(Bytes::from(b.finish())).unwrap()
    }

    #[test]
    fn iterates_both_ways_across_restart_points() {
        let ks = keys(100);
        let block = build(&ks);
        let mut it = block.iter();
        it.seek_to_first();
        let mut seen = Vec::new();
        while it.valid() {
            seen.push(it.key().to_vec());
            it.next();
        }
        assert_eq!(seen, ks);
        it.seek_to_last();
        let mut back = Vec::new();
        while it.valid() {
            back.push(it.key().to_vec());
            it.prev();
        }
        back.reverse();
        assert_eq!(back, ks);
    }

    #[test]
    fn seek_finds_the_first_key_at_or_after_and_seek_for_prev_the_last_at_or_before() {
        let ks: Vec<_> = (0..50u64)
            .map(|i| encode(0, format!("k{:03}", i * 2).as_bytes(), 1, Kind::Put))
            .collect();
        let block = build(&ks);
        let mut it = block.iter();
        for probe in 0..101u64 {
            let target = encode(0, format!("k{probe:03}").as_bytes(), 1, Kind::Put);
            it.seek(&target);
            let expect = ks
                .iter()
                .find(|k| key::compare(k, &target) != Ordering::Less);
            assert_eq!(
                it.valid().then(|| it.key().to_vec()).as_ref(),
                expect,
                "seek {probe}"
            );
            it.seek_for_prev(&target);
            let expect = ks
                .iter()
                .rev()
                .find(|k| key::compare(k, &target) != Ordering::Greater);
            assert_eq!(
                it.valid().then(|| it.key().to_vec()).as_ref(),
                expect,
                "seek_for_prev {probe}"
            );
        }
    }

    #[test]
    fn values_are_slices_of_the_block() {
        let block = build(&keys(3));
        let mut it = block.iter();
        it.seek_to_first();
        it.next();
        assert_eq!(&it.value()[..], b"v1");
    }

    #[test]
    fn garbage_is_an_error_not_a_panic() {
        for len in 0..64 {
            let data: Vec<u8> = (0..len).map(|i| (i * 37 + 11) as u8).collect();
            if let Ok(block) = Block::new(Bytes::from(data)) {
                let mut it = block.iter();
                it.seek_to_first();
                while it.valid() {
                    it.next();
                }
                it.seek(&encode(0, b"x", 1, Kind::Put));
            }
        }
    }
}
