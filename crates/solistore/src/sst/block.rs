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
pub(crate) fn shared_prefix(a: &[u8], b: &[u8]) -> usize {
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

/// Where each of a block's entries (`data`, up to the restart array)
/// starts, read from their headers alone, when there are at most
/// [`MAX_LISTED`] and the block is under 64 KiB; else none. A malformed
/// entry also lists none: searches report it.
fn list_entries(data: &[u8]) -> ([u16; MAX_LISTED], u8) {
    let mut starts = [0u16; MAX_LISTED];
    if data.len() > usize::from(u16::MAX) {
        return (starts, 0);
    }
    let (mut at, mut n) = (0, 0);
    while at < data.len() {
        if n == MAX_LISTED {
            return (starts, 0);
        }
        let Some((_, unshared, value_len, header)) = entry_header(&data[at..]) else {
            return (starts, 0);
        };
        starts[n] = at as u16;
        n += 1;
        at = match at
            .checked_add(header)
            .and_then(|a| a.checked_add(unshared))
            .and_then(|a| a.checked_add(value_len))
        {
            Some(next) if next <= data.len() => next,
            _ => return (starts, 0),
        };
    }
    (starts, n as u8)
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

/// Blocks of at most this many entries keep where each one starts.
const MAX_LISTED: usize = 16;

/// A decoded block, shared by every iterator over it.
#[derive(Clone)]
pub struct Block {
    data: Bytes,
    /// Where the restart array starts (= where entries end).
    restarts: usize,
    num_restarts: usize,
    /// Where each entry starts, for a block of at most [`MAX_LISTED`]
    /// entries within 64 KiB (`listed` of them; 0 for any other): found
    /// from the entries' headers when the block is decoded, so that a read
    /// can fetch every header it will step over at once (see
    /// [`Block::prefetch_entries`]). Kept in the block's handle, which the
    /// block cache holds in the entry a hit has already read: no miss of
    /// its own.
    starts: [u16; MAX_LISTED],
    listed: u8,
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
        let (starts, listed) = list_entries(&data[..restarts]);
        Ok(Block {
            data,
            restarts,
            num_restarts,
            starts,
            listed,
        })
    }

    /// Start fetching the cache lines of every entry's header and the key
    /// bytes after it, from the entries' places alone (nothing for a block
    /// that does not keep them).
    ///
    /// A seek steps over the entries one after another, and each header
    /// says where the next one is: with entries of a few hundred bytes,
    /// each is a cache miss that only begins once the one before is read.
    /// Asked for together, they arrive together. A SoliDB document block
    /// holds about nine.
    #[inline]
    pub fn prefetch_entries(&self) {
        let base = self.data.as_ptr();
        for &at in &self.starts[..self.listed as usize] {
            let at = base.wrapping_add(at as usize);
            crate::hint::prefetch_line(at);
            // Header and key bytes may run into the next line.
            crate::hint::prefetch_line(at.wrapping_add(32));
        }
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

    /// Start bringing the whole block into the CPU's caches, for a read
    /// about to walk it.
    ///
    /// Each cached block is its own allocation, and a walk reads one entry
    /// every few hundred bytes, too few for the hardware prefetcher to
    /// follow within 4 KiB: without this, a warm full scan spent most of
    /// its time waiting for the memory of each next entry, and a point
    /// read waited for each entry it stepped over in turn.
    #[inline]
    pub fn prefetch(&self) {
        crate::hint::prefetch(&self.data);
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
        self.seek_entry_in(target, key, self.full_range())
    }

    /// Every restart point, as a range for [`Block::seek_entry_in`].
    pub fn full_range(&self) -> (usize, usize) {
        (0, self.num_restarts - 1)
    }

    /// [`Block::seek_entry`], knowing that the last restart point whose key
    /// is before `target` (or the first, if none is) lies in `range`: from
    /// [`Fences::range`].
    pub fn seek_entry_in(
        &self,
        target: &[u8],
        key: &mut Vec<u8>,
        range: (usize, usize),
    ) -> Result<Option<Entry>> {
        // The last restart point whose key is before `target`.
        let (mut left, mut right) = range;
        if left > right || right >= self.num_restarts {
            return Err(corrupt());
        }
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
        let start = self.restart_point(left);
        match seek_in_place(data, start, target, key)? {
            Some(found) => Ok(found),
            None => seek_rebuilding(data, start, target, key),
        }
    }
}

/// The first entry from `at` on that is at or after `target`, without
/// rebuilding the keys stepped over: `Ok(Some(..))` once decided, its key
/// then left in `key`; `Ok(None)` for keys this cannot order on their bytes
/// alone (an identity that is a prefix of the other's, among others), for
/// [`seek_rebuilding`] to decide.
///
/// A key repeats `shared` bytes of the one before. Say the last key agreed
/// with the target on `same` bytes and was below it at the next one, inside
/// both identities. A key repeating more than `same` bytes has that same
/// smaller byte there: it is below the target too, whatever follows. A key
/// repeating at most `same` bytes is the target's first `shared` bytes
/// followed by its own: only those are compared. A walk over a block
/// rebuilt every key (a copy) and compared it from its first byte; SoliDB's
/// keys share 30-odd bytes, and differ in the last few digits.
#[inline]
fn seek_in_place(
    data: &[u8],
    mut at: usize,
    target: &[u8],
    key: &mut Vec<u8>,
) -> Result<Option<Option<Entry>>> {
    let Some(target_id) = target.len().checked_sub(key::TRAILER_LEN) else {
        return Ok(None);
    };
    // Bytes the last key shares with the target; whether it is below the
    // target at the next one, inside the target's identity; its length.
    let (mut same, mut skip, mut last_len) = (0, false, 0);
    while at < data.len() {
        let (shared, unshared, value_len, header) =
            entry_header(&data[at..]).ok_or_else(corrupt)?;
        // Saturated: an overflow lands past the end, which is refused.
        let key_start = at.saturating_add(header);
        let value_start = key_start.saturating_add(unshared);
        let value_end = value_start.saturating_add(value_len);
        if value_end > data.len() || shared > last_len {
            return Err(corrupt());
        }
        last_len = shared + unshared;
        if shared > same {
            if !skip {
                return Ok(None);
            }
            at = value_end;
            continue;
        }
        // This key is `target[..shared] ‖ own`.
        let own = &data[key_start..value_start];
        let p = shared + shared_prefix(own, &target[shared..]);
        let Some(key_id) = last_len.checked_sub(key::TRAILER_LEN) else {
            return Ok(None);
        };
        let order = if p < key_id && p < target_id {
            // The identities differ at `p`.
            own[p - shared].cmp(&target[p])
        } else if key_id != target_id {
            // One identity is a prefix of the other: the shorter is first.
            key_id.cmp(&target_id)
        } else {
            // The same identity: newest first. The trailer is this key's
            // own last bytes, unless it repeats some of the last key's.
            let trailer = match own.get(key_id.wrapping_sub(shared)..) {
                Some(tail) if shared <= key_id => {
                    u64::from_le_bytes(tail.try_into().expect("a whole trailer"))
                }
                _ => {
                    let mut trailer = [0u8; key::TRAILER_LEN];
                    for (i, b) in trailer.iter_mut().enumerate() {
                        let at = key_id + i;
                        *b = if at < shared {
                            target[at]
                        } else {
                            own[at - shared]
                        };
                    }
                    u64::from_le_bytes(trailer)
                }
            };
            key::trailer(target).cmp(&trailer)
        };
        if order != Ordering::Less {
            key.clear();
            key.extend_from_slice(&target[..shared]);
            key.extend_from_slice(own);
            return Ok(Some(Some(Entry {
                offset: at,
                value: (value_start, value_end),
            })));
        }
        // Below the target. Later keys that repeat more than `p` bytes of
        // this one are below it too only if it differs at `p` by a smaller
        // byte inside both identities.
        skip = p < key_id && p < target_id;
        same = p;
        at = value_end;
    }
    Ok(Some(None))
}

/// [`seek_in_place`] the plain way: every key rebuilt in `key` and compared
/// whole.
fn seek_rebuilding(
    data: &[u8],
    mut at: usize,
    target: &[u8],
    key: &mut Vec<u8>,
) -> Result<Option<Entry>> {
    key.clear();
    while at < data.len() {
        let (value, next) = parse_entry(data, at, key).ok_or_else(corrupt)?;
        if key::compare(key, target) != Ordering::Less {
            return Ok(Some(Entry { offset: at, value }));
        }
        at = next;
    }
    Ok(None)
}

/// A table's index reduced, per data block, to integers: the eight bytes
/// of its separator's `cf ‖ user key` after the prefix every separator
/// shares, and where the block starts in the file. A point read
/// binary-searches the words for its data block and takes the block's
/// place from `offsets`, without reading the index block; a seek narrows
/// the index's restart points the same way. Only separators that tie
/// with the target on their word need whole keys.
///
/// SoliDB's keys share 20-odd bytes (column family, keyspace, `doc:`), and
/// a 64 MiB table's index holds ~16,000 entries over ~600 KB: a get used to
/// binary-search its restart keys, decode a handful of entries and two
/// varints there (~900 instructions and two or three cache misses), where
/// the words and offsets take 12 bytes a block (~200 KB for such a table,
/// charged to the block cache's budget like the index).
pub struct Fences {
    prefix: Vec<u8>,
    words: Vec<u64>,
    /// Where each data block starts, then where the last one ends: block
    /// `i` and its trailer are `offsets[i]..offsets[i + 1]`.
    offsets: Vec<u32>,
    /// Index entries per restart point.
    interval: usize,
}

/// The eight bytes of `id` from `at`, zero-padded, as a big-endian word:
/// words order as the ids they come from, ties aside.
#[inline]
pub(crate) fn word_after(id: &[u8], at: usize) -> u64 {
    let tail = id.get(at..).unwrap_or_default();
    if let Some(word) = tail.first_chunk::<8>() {
        return u64::from_be_bytes(*word);
    }
    let mut word = [0u8; 8];
    word[..tail.len()].copy_from_slice(tail);
    u64::from_be_bytes(word)
}

/// Where a target falls among a table's separators.
#[derive(Clone, Copy)]
enum Place {
    /// At or before the first.
    First,
    /// After the last.
    Past,
    /// After the separators `..below` and before those from `not_above`;
    /// the ones between tie with it on their word.
    Words { below: usize, not_above: usize },
}

impl Fences {
    /// The fences of a table's `index`, whose data blocks end at
    /// `data_end`; `handle` reads an entry's value as the place of its
    /// block and the end of its trailer. `None` for an index that is not
    /// as the table builder writes it: entries out of order, blocks not
    /// back to back, restart points not evenly spaced, or data past 4 GiB.
    /// Such a table is searched through its index alone.
    pub fn new(
        index: &Block,
        data_end: u64,
        handle: impl Fn(&[u8]) -> Option<(u64, u64)>,
    ) -> Option<Fences> {
        if data_end > u64::from(u32::MAX) {
            return None;
        }
        let first = key::cf_and_user_key(index.restart_key(0)?).to_vec();
        let mut it = index.iter();
        it.seek_to_last();
        if !it.valid() {
            return None;
        }
        let last = key::cf_and_user_key(it.key());
        let shared = first.iter().zip(last).take_while(|(a, b)| a == b).count();
        let prefix = first[..shared].to_vec();

        let (mut words, mut offsets) = (Vec::new(), Vec::new());
        // The entry each restart point starts at.
        let mut starts = Vec::with_capacity(index.num_restarts);
        it.seek_to_first();
        while it.valid() {
            if starts.len() < index.num_restarts && it.offset() == index.restart_point(starts.len())
            {
                starts.push(words.len());
            }
            let id = key::cf_and_user_key(it.key());
            let word = word_after(id, shared);
            if !id.starts_with(&prefix) || words.last().is_some_and(|&w| w > word) {
                return None;
            }
            // Each block starts where the one before ended.
            let (offset, end) = handle(it.value_slice())?;
            match offsets.last() {
                None => offsets.push(u32::try_from(offset).ok()?),
                Some(&o) if u64::from(o) == offset => {}
                Some(_) => return None,
            }
            if end <= offset {
                return None;
            }
            offsets.push(u32::try_from(end).ok()?);
            words.push(word);
            it.next();
        }
        it.status().ok()?;
        let interval = match starts.get(1) {
            Some(&i) => i,
            None => words.len(),
        };
        let even = starts.len() == index.num_restarts
            && interval > 0
            && starts.iter().enumerate().all(|(j, &e)| e == j * interval);
        if words.is_empty() || !even || offsets.last() != Some(&(data_end as u32)) {
            return None;
        }
        Some(Fences {
            prefix,
            words,
            offsets,
            interval,
        })
    }

    /// Bytes held.
    pub fn size(&self) -> usize {
        self.prefix.len() + 8 * self.words.len() + 4 * self.offsets.len()
    }

    /// Data blocks.
    pub fn blocks(&self) -> usize {
        self.words.len()
    }

    /// The number of the data block that starts at `offset`. For a block
    /// the index gave, where the fences tied: its slot in the block cache
    /// goes by this number.
    #[inline]
    pub fn number_of(&self, offset: u64) -> Option<usize> {
        let offset = u32::try_from(offset).ok()?;
        self.offsets[..self.words.len()].binary_search(&offset).ok()
    }

    /// Where data block `i` lies: its offset and length, trailer excluded
    /// (`trailer` bytes).
    pub fn handle(&self, i: usize, trailer: u64) -> (u64, u64) {
        let (start, end) = (self.offsets[i], self.offsets[i + 1]);
        (u64::from(start), u64::from(end - start) - trailer)
    }

    #[inline]
    fn place(&self, target: &[u8]) -> Place {
        let id = key::cf_and_user_key(target);
        let p = self.prefix.len();
        let m = p.min(id.len());
        // A slice comparison: on a prefix of 20-odd bytes, the library's
        // vectorised memcmp takes a quarter of the instructions of
        // `key::cmp_bytes`.
        match id[..m].cmp(&self.prefix[..m]) {
            Ordering::Less => return Place::First,
            Ordering::Greater => return Place::Past,
            Ordering::Equal if id.len() < p => return Place::First,
            Ordering::Equal => {}
        }
        // A smaller word is a smaller key, a larger word a larger one:
        // only equal words need whole keys.
        let t = word_after(id, p);
        let below = self.words.partition_point(|&w| w < t);
        // Most targets tie with no separator. Those that do may tie with
        // thousands: in a table holding many collections, the word after
        // the table's shared prefix can be a keyspace byte and `doc:`,
        // the same for every block of one collection. A walk over the
        // ties made a get on such a table 2-3x slower than 0.3.0.
        let not_above = if self.words.get(below) == Some(&t) {
            below + self.words[below..].partition_point(|&w| w == t)
        } else {
            below
        };
        Place::Words { below, not_above }
    }

    /// The data block holding the first entry at or after `target`:
    /// `Some(None)` when every entry of the table is before it, and `None`
    /// when separators tie with it on their word, for [`Fences::range`]
    /// and the index to decide. Reads go through [`Fences::locate`].
    #[cfg(test)]
    pub fn block(&self, target: &[u8]) -> Option<Option<usize>> {
        self.block_at(self.place(target))
    }

    /// [`Fences::block`], or where it cannot tell, [`Fences::range`]:
    /// both from one search.
    #[inline]
    pub fn locate(&self, target: &[u8]) -> std::result::Result<Option<usize>, (usize, usize)> {
        let place = self.place(target);
        self.block_at(place).ok_or_else(|| self.range_at(place))
    }

    #[inline]
    fn block_at(&self, place: Place) -> Option<Option<usize>> {
        match place {
            Place::First => Some(Some(0)),
            Place::Past => Some(None),
            Place::Words { below, not_above } if below == not_above => {
                Some((below < self.words.len()).then_some(below))
            }
            Place::Words { .. } => None,
        }
    }

    /// Where among the index's restart points the last one whose key is
    /// before `target` lies (the first, if none is): a range for
    /// [`Block::seek_entry_in`].
    pub fn range(&self, target: &[u8]) -> (usize, usize) {
        self.range_at(self.place(target))
    }

    #[inline]
    fn range_at(&self, place: Place) -> (usize, usize) {
        let restart = |entry: usize| entry / self.interval;
        match place {
            Place::First => (0, 0),
            Place::Past => {
                let last = restart(self.words.len() - 1);
                (last, last)
            }
            // A restart point's key is its first entry's: the one holding
            // the last entry below the target has a smaller key, and any
            // whose first entry is above the ties a larger one.
            Place::Words { below, not_above } => (
                below.checked_sub(1).map_or(0, restart),
                not_above.checked_sub(1).map_or(0, restart),
            ),
        }
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
        self.seek_in(target, self.block.full_range());
    }

    /// [`BlockIter::seek`] within a restart range ([`Block::seek_entry_in`]).
    pub fn seek_in(&mut self, target: &[u8], range: (usize, usize)) {
        match self.block.seek_entry_in(target, &mut self.key, range) {
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
    use proptest::prelude::*;

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

    fn byte() -> impl Strategy<Value = u8> {
        prop::sample::select(vec![0u8, 1, b'a', b'b', 0xff])
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(512))]

        /// A seek narrowed by the fences lands where the full search does,
        /// on the first key at or after the target (by a plain walk of the
        /// sorted keys), for keys sharing a prefix (or not), versions of
        /// one key across restart points, targets inside, before and after
        /// the block, and any restart interval. Where the fences name the
        /// block on their own, it is that entry's, at its place.
        #[test]
        fn fences_find_the_entry_the_full_search_finds(
            prefix in prop::collection::vec(byte(), 0..20),
            ids in prop::collection::vec((prop::collection::vec(byte(), 0..12), 1..4u64), 1..200),
            targets in prop::collection::vec(
                (0..3u32, any::<bool>(), prop::collection::vec(byte(), 0..14), 0..6u64),
                1..40,
            ),
            interval in 1..6usize,
        ) {
            let mut keys: Vec<Vec<u8>> = ids
                .iter()
                .flat_map(|(suffix, versions)| {
                    let user_key = [&prefix[..], suffix].concat();
                    (1..=*versions).map(move |seq| encode(1, &user_key, seq, Kind::Put))
                })
                .collect();
            keys.sort_by(|a, b| key::compare(a, b));
            keys.dedup();
            // An index: entry `i` names block `i`, at `10 i..10 i + 10`.
            let mut b = BlockBuilder::new(interval);
            for (i, k) in keys.iter().enumerate() {
                b.add(k, &(i as u32).to_le_bytes());
            }
            let block = Block::new(Bytes::from(b.finish())).unwrap();
            let number = |value: &[u8]| u32::from_le_bytes(value.try_into().unwrap()) as usize;
            let fences = Fences::new(&block, keys.len() as u64 * 10, |v| {
                let i = u64::from(u32::from_le_bytes(v.try_into().ok()?));
                Some((i * 10, i * 10 + 10))
            })
            .expect("the fences of a well-formed index");
            prop_assert_eq!(fences.blocks(), keys.len());
            let (mut a, mut z) = (Vec::new(), Vec::new());
            for (cf, prefixed, suffix, seq) in targets {
                let user_key = if prefixed { [&prefix[..], &suffix[..]].concat() } else { suffix };
                let target = encode(cf, &user_key, seq, Kind::Put);
                let first = keys.iter().position(|k| key::compare(k, &target) != Ordering::Less);
                let want = block.seek_entry(&target, &mut a).unwrap();
                prop_assert_eq!(want.map(|e| number(&block.bytes()[e.value.0..e.value.1])), first);
                let got = block.seek_entry_in(&target, &mut z, fences.range(&target)).unwrap();
                prop_assert_eq!(got, want, "{:?}", target);
                if let Some(i) = first {
                    prop_assert_eq!(&z, &keys[i]);
                    prop_assert_eq!(&a, &keys[i]);
                }
                if let Some(found) = fences.block(&target) {
                    prop_assert_eq!(found, first, "{:?}", target);
                    if let Some(i) = found {
                        prop_assert_eq!(fences.handle(i, 0), (i as u64 * 10, 10));
                    }
                }
                match fences.locate(&target) {
                    Ok(found) => prop_assert_eq!(Some(found), fences.block(&target)),
                    Err(range) => {
                        prop_assert_eq!(fences.block(&target), None);
                        prop_assert_eq!(range, fences.range(&target));
                    }
                }
            }
        }
    }

    /// Two collections in one table, as SoliDB's shared keyspace has them:
    /// the table's keys share only the column family and part of the
    /// keyspace prefix, so every key of one collection has the same word
    /// (its last prefix byte, then `doc:000`). The fences must still lead
    /// a seek to the right entry, through the index.
    #[test]
    fn fences_among_thousands_of_ties() {
        let key_of = |coll: u8, n: u32| {
            let mut user = vec![0, 0, 0, 1, 0, 0, 0, coll];
            user.extend_from_slice(format!("doc:{n:020}").as_bytes());
            encode(1, &user, 7, Kind::Put)
        };
        let keys: Vec<Vec<u8>> = [3u8, 4]
            .iter()
            .flat_map(|&coll| (0..2000).map(move |n| key_of(coll, n * 2)))
            .collect();
        let mut b = BlockBuilder::new(4);
        for (i, k) in keys.iter().enumerate() {
            b.add(k, &(i as u32).to_le_bytes());
        }
        let block = Block::new(Bytes::from(b.finish())).unwrap();
        let fences = Fences::new(&block, keys.len() as u64 * 10, |v| {
            let i = u64::from(u32::from_le_bytes(v.try_into().ok()?));
            Some((i * 10, i * 10 + 10))
        })
        .expect("fences");
        let (mut a, mut z) = (Vec::new(), Vec::new());
        for coll in [3u8, 4] {
            for n in [0, 1, 2, 777, 1998, 3998, 3999, 4001] {
                let target = key_of(coll, n);
                let want = block.seek_entry(&target, &mut a).unwrap();
                let range = fences.locate(&target).expect_err("the word ties");
                // The range spans the collection's ties: the index decides.
                assert!(range.1 - range.0 > 100, "{range:?}");
                let got = block.seek_entry_in(&target, &mut z, range).unwrap();
                assert_eq!(got, want, "coll {coll}, n {n}");
                assert_eq!(z, a);
            }
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(2048))]

        /// The in-place seek finds what a plain walk of the sorted keys
        /// finds, on keys made to defeat it: identities that are prefixes of
        /// one another, zero and 0xff bytes, versions whose trailers share
        /// bytes (so that keys repeat part of a trailer), both kinds, and
        /// any restart interval; targets at, between and around them.
        #[test]
        fn a_seek_in_place_lands_where_a_walk_does(
            ids in prop::collection::vec(
                (prop::collection::vec(byte(), 0..10), prop::collection::vec(0..4u64, 1..4)),
                1..60,
            ),
            interval in 1..20usize,
            targets in prop::collection::vec(
                (0..3u32, prop::collection::vec(byte(), 0..11), 0..5u64, any::<bool>()),
                1..40,
            ),
        ) {
            // Sequence numbers 0x0100..: neighbours share trailer bytes.
            let seq = |s: u64| 0x0100 + s;
            let mut keys: Vec<Vec<u8>> = ids
                .iter()
                .flat_map(|(id, seqs)| {
                    seqs.iter().map(move |&s| {
                        let kind = if s % 2 == 0 { Kind::Put } else { Kind::Delete };
                        encode(1, id, seq(s), kind)
                    })
                })
                .collect();
            keys.sort_by(|a, b| key::compare(a, b));
            keys.dedup();
            let mut b = BlockBuilder::new(interval);
            for (i, k) in keys.iter().enumerate() {
                b.add(k, &(i as u32).to_le_bytes());
            }
            let block = Block::new(Bytes::from(b.finish())).unwrap();
            let mut all: Vec<Vec<u8>> = targets
                .iter()
                .map(|(cf, id, s, put)| {
                    encode(*cf, id, seq(*s), if *put { Kind::Put } else { Kind::Delete })
                })
                .collect();
            all.extend(keys.iter().cloned());
            let mut key = b"left over".to_vec();
            for target in &all {
                let first = keys.iter().position(|k| key::compare(k, target) != Ordering::Less);
                let found = block.seek_entry(target, &mut key).unwrap();
                let at = found.map(|e| u32::from_le_bytes(block.bytes()[e.value.0..e.value.1].try_into().unwrap()) as usize);
                prop_assert_eq!(at, first, "{:?}", target);
                if let Some(i) = first {
                    prop_assert_eq!(&key, &keys[i]);
                }
            }
        }
    }

    /// Fences are only built for an index as the table builder writes it.
    #[test]
    fn fences_refuse_blocks_that_are_not_back_to_back() {
        let ks = keys(40);
        let mut b = BlockBuilder::new(4);
        for (i, k) in ks.iter().enumerate() {
            b.add(k, &(i as u32).to_le_bytes());
        }
        let block = Block::new(Bytes::from(b.finish())).unwrap();
        let at = |gap: u64| {
            move |v: &[u8]| {
                let i = u64::from(u32::from_le_bytes(v.try_into().ok()?));
                let start = i * 10 + if i >= 20 { gap } else { 0 };
                Some((start, start + 10))
            }
        };
        assert!(Fences::new(&block, 400, at(0)).is_some());
        assert!(
            Fences::new(&block, 401, at(0)).is_none(),
            "data ends elsewhere"
        );
        assert!(
            Fences::new(&block, 401, at(1)).is_none(),
            "a gap between blocks"
        );
        assert!(Fences::new(&block, 1 << 33, |_| Some((0, 1 << 33))).is_none());
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
