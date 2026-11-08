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

/// Length of the common prefix of `a` and `b`, compared eight bytes at a
/// time.
#[inline]
fn shared_prefix(a: &[u8], b: &[u8]) -> usize {
    let n = a.len().min(b.len());
    let mut i = 0;
    while i + 8 <= n {
        let x = u64::from_le_bytes(a[i..i + 8].try_into().expect("8 bytes"))
            ^ u64::from_le_bytes(b[i..i + 8].try_into().expect("8 bytes"));
        if x != 0 {
            return i + (x.trailing_zeros() / 8) as usize;
        }
        i += 8;
    }
    while i < n && a[i] == b[i] {
        i += 1;
    }
    i
}

/// Decode an entry header (`shared`, `unshared`, `value_len`), returning it
/// with the header's length. Keys in SoliDB's layout are short, so `shared`
/// and `unshared` take one byte and `value_len` one or two; those cases skip
/// the general varint loop.
#[inline(always)]
fn entry_header(input: &[u8]) -> Option<(usize, usize, usize, usize)> {
    if input.len() >= 4 && input[0] < 0x80 && input[1] < 0x80 {
        let (shared, unshared) = (input[0] as usize, input[1] as usize);
        if input[2] < 0x80 {
            return Some((shared, unshared, input[2] as usize, 3));
        }
        if input[3] < 0x80 {
            let value_len = (input[2] & 0x7f) as usize | (input[3] as usize) << 7;
            return Some((shared, unshared, value_len, 4));
        }
    }
    let mut rest = input;
    let shared = get_varint(&mut rest)?;
    let unshared = get_varint(&mut rest)?;
    let value_len = get_varint(&mut rest)?;
    Some((
        usize::try_from(shared).ok()?,
        usize::try_from(unshared).ok()?,
        usize::try_from(value_len).ok()?,
        input.len() - rest.len(),
    ))
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
            shared_prefix(&self.last_key, key)
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

    /// The key stored whole at restart point `i`, read in place; `None` if
    /// the entry is malformed.
    fn restart_key(&self, i: usize) -> Option<&[u8]> {
        let data = &self.data[..self.restarts];
        let at = self.restart_point(i);
        let (shared, unshared, _, header) = entry_header(data.get(at..)?)?;
        if shared != 0 {
            return None;
        }
        let start = at.checked_add(header)?;
        data.get(start..start.checked_add(unshared)?)
    }

    pub fn iter(&self) -> BlockIter {
        self.clone().into_iter_with(Vec::new())
    }

    /// A cursor over this block that builds its keys in `key`, the buffer
    /// of a cursor over an earlier block: a scan allocates no key per
    /// block.
    pub fn into_iter_with(self, mut key: Vec<u8>) -> BlockIter {
        key.clear();
        BlockIter {
            offset: self.restarts,
            next_offset: self.restarts,
            block: self,
            key,
            value: (0, 0),
            corrupt: false,
        }
    }

    /// The keys stored whole at the restart points, in order, each with
    /// how many entries its restart interval holds (counted by skipping
    /// over their headers, no key rebuilt). A malformed entry is an error.
    pub fn restart_keys(&self) -> impl Iterator<Item = Result<(&[u8], usize)>> + '_ {
        let data = &self.data[..self.restarts];
        (0..self.num_restarts).map(move |i| {
            let at = self.restart_point(i);
            let (shared, unshared, _, header) =
                entry_header(data.get(at..).ok_or_else(corrupt)?).ok_or_else(corrupt)?;
            if shared != 0 {
                return Err(corrupt());
            }
            let key_start = at.checked_add(header).ok_or_else(corrupt)?;
            let key = data
                .get(key_start..key_start.checked_add(unshared).ok_or_else(corrupt)?)
                .ok_or_else(corrupt)?;
            let end = if i + 1 < self.num_restarts {
                self.restart_point(i + 1)
            } else {
                self.restarts
            };
            let (mut entries, mut next) = (0, at);
            while next < end {
                let (_, unshared, value_len, header) =
                    entry_header(data.get(next..).ok_or_else(corrupt)?).ok_or_else(corrupt)?;
                next = [header, unshared, value_len]
                    .iter()
                    .try_fold(next, |at, len| at.checked_add(*len))
                    .ok_or_else(corrupt)?;
                entries += 1;
            }
            Ok((key, entries))
        })
    }

    /// The block's bytes, which the value ranges below index into.
    pub fn bytes(&self) -> &[u8] {
        &self.data
    }

    /// Start bringing the whole block into the CPU's caches, for a walk
    /// about to read it.
    ///
    /// Each cached block is its own allocation, and a walk reads one entry
    /// every few hundred bytes, too few for the hardware prefetcher to
    /// follow within 4 KiB: without this, a warm full scan spent most of
    /// its time waiting for the memory of each next entry.
    #[inline]
    pub fn prefetch(&self) {
        #[cfg(all(target_arch = "x86_64", not(miri)))]
        {
            use std::arch::x86_64::{_mm_prefetch, _MM_HINT_T0};
            let (start, len) = (self.data.as_ptr(), self.data.len());
            // SAFETY: a prefetch is a hint. It never faults and has no
            // effect a program can observe besides timing; every address is
            // inside a live slice anyway.
            #[allow(unused_unsafe)]
            let line =
                |at: usize| unsafe { _mm_prefetch::<_MM_HINT_T0>(start.wrapping_add(at).cast()) };
            let mut at = 0;
            while at + 256 <= len {
                line(at);
                line(at + 64);
                line(at + 128);
                line(at + 192);
                at += 256;
            }
            while at < len {
                line(at);
                at += 64;
            }
        }
    }

    /// A handle on part of the block, which keeps the whole block alive.
    pub fn slice(&self, range: std::ops::Range<usize>) -> Bytes {
        self.data.slice(range)
    }

    /// The first entry at or after `target`, read in place: its key is left
    /// in `key`, and where it lies returned; `Ok(None)` when every entry is
    /// before `target`. [`BlockIter::seek`] is this search; a point read
    /// calls it directly, taking no handle on the block: each handle is a
    /// write to the block's shared reference count, which every thread
    /// reading a hot block contends on.
    pub fn seek_entry(&self, target: &[u8], key: &mut Vec<u8>) -> Result<Option<Entry>> {
        // The last restart point whose key is before `target`.
        let (mut left, mut right) = (0, self.num_restarts - 1);
        while left < right {
            let mid = (left + right).div_ceil(2);
            let restart_key = self.restart_key(mid).ok_or_else(corrupt)?;
            if key::compare(restart_key, target) == Ordering::Less {
                left = mid;
            } else {
                right = mid - 1;
            }
        }
        let data = &self.data[..self.restarts];
        let mut at = self.restart_point(left);
        key.clear();
        while at < self.restarts {
            let (value, next) = parse_entry(data, at, key).ok_or_else(corrupt)?;
            if key::compare(key, target) != Ordering::Less {
                return Ok(Some(Entry { offset: at, value }));
            }
            at = next;
        }
        Ok(None)
    }
}

/// Where [`Block::seek_entry`] found its entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Entry {
    /// The entry's first byte.
    pub offset: usize,
    /// Its value's range in the block; the next entry starts at the end.
    pub value: (usize, usize),
}

/// Decode the entry at `at` of a block's entries (`data`), rebuilding its
/// key in `key` from the one before it. Returns the value's range and where
/// the next entry starts; `None`, with `key` untouched, if the entry is
/// malformed.
#[inline(always)]
fn parse_entry(data: &[u8], at: usize, key: &mut Vec<u8>) -> Option<((usize, usize), usize)> {
    let (shared, unshared, value_len, header) = entry_header(data.get(at..)?)?;
    let key_start = at.checked_add(header)?;
    let value_start = key_start.checked_add(unshared)?;
    let value_end = value_start.checked_add(value_len)?;
    if shared > key.len() || value_end > data.len() {
        return None;
    }
    key.truncate(shared);
    key.extend_from_slice(&data[key_start..value_start]);
    Some(((value_start, value_end), value_end))
}

/// A cursor over one block.
pub struct BlockIter {
    block: Block,
    /// Offset of the current entry; `block.restarts` when not valid.
    offset: usize,
    next_offset: usize,
    key: Vec<u8>,
    value: (usize, usize),
    corrupt: bool,
}

impl BlockIter {
    #[inline]
    pub fn valid(&self) -> bool {
        self.offset < self.block.restarts
    }

    #[inline]
    pub fn key(&self) -> &[u8] {
        &self.key
    }

    pub fn value(&self) -> Bytes {
        self.block.data.slice(self.value.0..self.value.1)
    }

    /// The value in place, without a handle on the block.
    #[inline]
    pub fn value_slice(&self) -> &[u8] {
        &self.block.data[self.value.0..self.value.1]
    }

    /// The key buffer, for a cursor over the next block.
    pub fn into_key(self) -> Vec<u8> {
        self.key
    }

    /// Where the current entry starts in the block: which entry it is.
    pub fn offset(&self) -> usize {
        self.offset
    }

    /// Where the entry after the current one starts.
    pub fn next_offset(&self) -> usize {
        self.next_offset
    }

    /// The value of the entry after the current one, read in place without
    /// moving (its key is not rebuilt): an index looked at one block ahead.
    pub fn peek_next_value(&self) -> Option<&[u8]> {
        if !self.valid() {
            return None;
        }
        let data = &self.block.data[..self.block.restarts];
        let (_, unshared, value_len, header) = entry_header(data.get(self.next_offset..)?)?;
        let start = self
            .next_offset
            .checked_add(header)?
            .checked_add(unshared)?;
        data.get(start..start.checked_add(value_len)?)
    }

    /// Size of the decoded block the values slice.
    pub fn block_size(&self) -> usize {
        self.block.size()
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
    #[inline]
    fn parse_next(&mut self) -> bool {
        self.offset = self.next_offset;
        if self.offset >= self.block.restarts {
            self.invalidate();
            return false;
        }
        let data = &self.block.data[..self.block.restarts];
        match parse_entry(data, self.offset, &mut self.key) {
            Some((value, next)) => {
                self.value = value;
                self.next_offset = next;
                true
            }
            None => self.fail(),
        }
    }

    fn fail(&mut self) -> bool {
        self.corrupt = true;
        self.invalidate();
        false
    }

    fn seek_to_restart(&mut self, i: usize) {
        self.key.clear();
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

    #[inline]
    pub fn next(&mut self) {
        debug_assert!(self.valid());
        self.parse_next();
    }

    pub fn prev(&mut self) {
        debug_assert!(self.valid());
        let current = self.offset;
        // Back to the last restart point before the current entry, then
        // forward to the entry just before it.
        let (mut lo, mut hi) = (0, self.block.num_restarts);
        while lo < hi {
            let mid = (lo + hi) / 2;
            if self.block.restart_point(mid) < current {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        if lo == 0 {
            self.invalidate();
            return;
        }
        self.seek_to_restart(lo - 1);
        while self.parse_next() && self.next_offset < current {}
    }

    /// The first entry with a key at or after `target`.
    pub fn seek(&mut self, target: &[u8]) {
        match self.block.seek_entry(target, &mut self.key) {
            Ok(Some(entry)) => {
                self.offset = entry.offset;
                self.value = entry.value;
                self.next_offset = entry.value.1;
            }
            Ok(None) => self.invalidate(),
            Err(_) => {
                self.fail();
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

    /// The point read's in-place search lands where the iterator's seek
    /// does, for every target: on a key, between keys, before the first
    /// and past the last, with a key buffer left over from earlier reads.
    #[test]
    fn seek_entry_finds_what_the_iterator_seek_finds() {
        let ks: Vec<_> = (0..300u64)
            .map(|i| encode(0, format!("k{:04}", i * 3).as_bytes(), 9, Kind::Put))
            .collect();
        let block = build(&ks);
        let mut key = b"left over from an earlier read".to_vec();
        let mut targets: Vec<Vec<u8>> = (0..905u64)
            .map(|i| encode(0, format!("k{i:04}").as_bytes(), 9, Kind::Put))
            .collect();
        targets.push(encode(0, b"a", 1, Kind::Put));
        targets.push(encode(0, b"z", 1, Kind::Put));
        // Newer and older than the stored version of a key.
        targets.push(encode(0, b"k0003", 10, Kind::Put));
        targets.push(encode(0, b"k0003", 8, Kind::Put));
        for target in &targets {
            let mut it = block.iter();
            it.seek(target);
            let found = block.seek_entry(target, &mut key).unwrap();
            assert_eq!(found.is_some(), it.valid(), "{target:?}");
            if let Some(entry) = found {
                let (start, end) = entry.value;
                assert_eq!(key, it.key(), "{target:?}");
                assert_eq!(&block.bytes()[start..end], &it.value()[..], "{target:?}");
            }
        }
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
    fn a_block_of_short_keys_can_be_searched() {
        // Found by fuzzing: a corrupt block's keys may be shorter than a
        // trailer, and a seek compared them by slicing it off.
        let mut b = BlockBuilder::new(2);
        for k in [&b""[..], b"a", b"ab", b"abcdefgh", b"b"] {
            b.add(k, b"v");
        }
        let block = Block::new(Bytes::from(b.finish())).unwrap();
        let mut it = block.iter();
        for target in [&b""[..], b"a", b"zz", &encode(0, b"x", 1, Kind::Put)] {
            it.seek(target);
            it.seek_for_prev(target);
        }
        it.seek_to_first();
        while it.valid() {
            it.next();
        }
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
