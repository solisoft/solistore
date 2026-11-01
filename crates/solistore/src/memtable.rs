//! The memtable: recent writes, ordered by internal key, readable while
//! being written.
//!
//! Writers only ever add entries (a delete is an entry too), and readers
//! filter by sequence number, so a reader never sees part of a batch whose
//! sequence numbers are above its snapshot even while that batch is being
//! inserted.
//!
//! Range deletes are kept apart, in a short list: SoliDB issues one per
//! dropped collection or database, so there are few of them, and a point
//! lookup can afford to scan them.

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use parking_lot::RwLock;

use crate::error::Result;
use crate::iter::InternalIterator;
use crate::key::{self, Kind};
use crate::skiplist::{Cursor, SkipList};

/// What a range tombstone counts toward the write buffer on top of its
/// bounds. Entries count exactly what their skiplist node takes.
const TOMBSTONE_OVERHEAD: usize = std::mem::size_of::<RangeTombstone>();

/// A value handed out by a point lookup, as its own allocation when it is a
/// small part of the buffer it slices (`backing` bytes).
///
/// A `Bytes` slice keeps its whole buffer alive: a 250-byte document read
/// from a table pinned its 4 KiB decoded block for as long as the caller
/// held it — past the block's eviction from the cache. A
/// `multi_get` of 5,000 documents held ~3,600 blocks that way until the
/// query ended; with 64 such queries in flight SoliDB's live heap was
/// ~450 MB above RocksDB's, whose bindings copy every value out. Copying a
/// value that small costs nothing next to the decompression that produced
/// it. A value that is most of its buffer is handed out as it is.
pub(crate) fn detach(value: Bytes, backing: usize) -> Bytes {
    if value.len().saturating_mul(2) < backing {
        Bytes::copy_from_slice(&value)
    } else {
        value
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeTombstone {
    pub cf: u32,
    pub start: Bytes,
    pub end: Bytes,
    pub seq: u64,
}

impl RangeTombstone {
    pub fn covers(&self, cf: u32, key: &[u8]) -> bool {
        self.cf == cf && &self.start[..] <= key && key < &self.end[..]
    }
}

/// The highest sequence number, at most `snapshot`, of a tombstone covering
/// `key` (0 if none). A version older than it is deleted.
pub fn covering_seq<'a>(
    tombstones: impl IntoIterator<Item = &'a RangeTombstone>,
    cf: u32,
    key: &[u8],
    snapshot: u64,
) -> u64 {
    tombstones
        .into_iter()
        .filter(|t| t.seq <= snapshot && t.covers(cf, key))
        .map(|t| t.seq)
        .max()
        .unwrap_or(0)
}

/// The newest version of a key visible at a snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Version {
    pub seq: u64,
    pub kind: Kind,
    pub value: Bytes,
}

pub struct Memtable {
    /// Keys, values and nodes, all in the list's arena, which counts what
    /// it hands out.
    map: SkipList,
    tombstones: RwLock<Vec<RangeTombstone>>,
    /// `tombstones` is not empty. Lets every point read skip the lock: a
    /// read lock is a write to a shared cache line, on every get.
    has_tombstones: AtomicBool,
    /// Bytes held by range tombstones; entries are counted by `map`.
    tombstone_bytes: AtomicUsize,
    /// The highest sequence number inserted.
    max_seq: AtomicU64,
    /// The first WAL file holding any of this memtable's writes: once it is
    /// flushed, WALs below the next memtable's `first_wal` are dead.
    first_wal: u64,
}

impl Memtable {
    pub fn new(first_wal: u64) -> Self {
        Memtable {
            map: SkipList::new(),
            tombstones: RwLock::new(Vec::new()),
            has_tombstones: AtomicBool::new(false),
            tombstone_bytes: AtomicUsize::new(0),
            max_seq: AtomicU64::new(0),
            first_wal,
        }
    }

    pub fn first_wal(&self) -> u64 {
        self.first_wal
    }

    /// The highest sequence number in this memtable (0 if empty).
    pub fn max_seq(&self) -> u64 {
        self.max_seq.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub fn insert(&self, cf: u32, key: &[u8], seq: u64, kind: Kind, value: &[u8]) {
        self.insert_entry(cf, key, seq, kind, value);
        self.account(seq);
    }

    /// Insert without updating the highest sequence number, which
    /// [`Memtable::account`] does once for a whole batch: eight writers
    /// bumping the same atomic for every entry would bounce its cache line
    /// between cores. The arena counts the entry's size in the writer's
    /// shard.
    pub fn insert_entry(&self, cf: u32, key: &[u8], seq: u64, kind: Kind, value: &[u8]) {
        // A batch never repeats a sequence number, so the key is new.
        self.map.insert(
            &[&cf.to_be_bytes(), key, &key::trailer_bytes(seq, kind)],
            value,
        );
    }

    pub fn account(&self, max_seq: u64) {
        self.max_seq.fetch_max(max_seq, Ordering::Relaxed);
    }

    pub fn insert_range_tombstone(&self, cf: u32, start: &[u8], end: &[u8], seq: u64) {
        self.max_seq.fetch_max(seq, Ordering::Relaxed);
        self.tombstone_bytes.fetch_add(
            start.len() + end.len() + TOMBSTONE_OVERHEAD,
            Ordering::Relaxed,
        );
        self.has_tombstones.store(true, Ordering::Release);
        self.tombstones.write().push(RangeTombstone {
            cf,
            start: Bytes::copy_from_slice(start),
            end: Bytes::copy_from_slice(end),
            seq,
        });
    }

    /// Memory held, in bytes: what the arena handed out for entries
    /// (padding and lost tails included) and the range tombstones. Not
    /// counted: what the arena's open blocks still hold in reserve, at most
    /// 512 KiB.
    pub fn approximate_size(&self) -> usize {
        self.map.memory_usage() + self.tombstone_bytes.load(Ordering::Relaxed)
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty() && !self.has_tombstones.load(Ordering::Acquire)
    }

    /// The newest version of `key` with a sequence number at most `snapshot`.
    pub fn get(&self, cf: u32, key: &[u8], snapshot: u64) -> Option<Version> {
        let probe = key::at_snapshot(cf, key, snapshot);
        let at = self.map.seek_ge(&probe)?;
        // SAFETY: `at` comes from `self.map`.
        let found = key::parse(unsafe { self.map.key(at) })?;
        // A copy, as RocksDB's bindings make: a slice of the arena would
        // keep the whole memtable alive past its flush.
        (found.cf == cf && found.user_key == key).then(|| Version {
            seq: found.seq,
            kind: found.kind,
            value: Bytes::copy_from_slice(unsafe { self.map.value(at) }),
        })
    }

    pub fn tombstones(&self) -> Vec<RangeTombstone> {
        if !self.has_tombstones.load(Ordering::Acquire) {
            return Vec::new();
        }
        self.tombstones.read().clone()
    }

    pub fn range_tombstone_seq(&self, cf: u32, key: &[u8], snapshot: u64) -> u64 {
        if !self.has_tombstones.load(Ordering::Acquire) {
            return 0;
        }
        covering_seq(self.tombstones.read().iter(), cf, key, snapshot)
    }

    /// Every entry in order, for a flush.
    pub fn entries(&self) -> impl Iterator<Item = (&[u8], &[u8])> + '_ {
        self.map.iter()
    }
}

/// A cursor over a memtable. Forward steps follow the list's bottom
/// links; backward steps search again from the head, as the list has no
/// back links.
pub struct MemIter {
    mem: Arc<Memtable>,
    /// Always a cursor of `mem.map`, which `mem` keeps alive: every
    /// `unsafe` read below relies on it.
    at: Option<Cursor>,
}

impl MemIter {
    pub fn new(mem: Arc<Memtable>) -> Self {
        MemIter { mem, at: None }
    }

    fn current(&self) -> Cursor {
        self.at.expect("valid")
    }
}

impl InternalIterator for MemIter {
    fn seek_ge(&mut self, target: &[u8]) {
        self.at = self.mem.map.seek_ge(target);
    }

    fn seek_le(&mut self, target: &[u8]) {
        self.at = self.mem.map.seek_le(target);
    }

    fn seek_to_last(&mut self) {
        self.at = self.mem.map.last();
    }

    fn next(&mut self) {
        if let Some(at) = self.at {
            // SAFETY: `at` is a cursor of `mem.map`.
            self.at = unsafe { self.mem.map.next(at) };
        }
    }

    fn prev(&mut self) {
        if let Some(at) = self.at {
            // SAFETY: as in `next`.
            self.at = self.mem.map.seek_lt(unsafe { self.mem.map.key(at) });
        }
    }

    fn valid(&self) -> bool {
        self.at.is_some()
    }

    fn key(&self) -> &[u8] {
        // SAFETY: as in `next`.
        unsafe { self.mem.map.key(self.current()) }
    }

    fn value(&self) -> Bytes {
        // SAFETY: as in `next`.
        Bytes::copy_from_slice(unsafe { self.mem.map.value(self.current()) })
    }

    fn status(&self) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_reader_sees_the_newest_version_at_its_snapshot() {
        let mem = Memtable::new(1);
        mem.insert(0, b"k", 1, Kind::Put, b"v1");
        mem.insert(0, b"k", 5, Kind::Put, b"v5");
        mem.insert(0, b"k", 9, Kind::Delete, b"");
        assert_eq!(mem.get(0, b"k", 0), None);
        assert_eq!(
            mem.get(0, b"k", 4).unwrap().value,
            Bytes::from_static(b"v1")
        );
        assert_eq!(
            mem.get(0, b"k", 8).unwrap().value,
            Bytes::from_static(b"v5")
        );
        assert_eq!(mem.get(0, b"k", 9).unwrap().kind, Kind::Delete);
    }

    #[test]
    fn column_families_and_prefixes_do_not_leak_into_each_other() {
        let mem = Memtable::new(1);
        mem.insert(1, b"k", 1, Kind::Put, b"cf1");
        mem.insert(0, b"kk", 2, Kind::Put, b"longer");
        assert_eq!(mem.get(0, b"k", 10), None);
        assert_eq!(mem.get(1, b"kk", 10), None);
    }

    #[test]
    fn range_tombstones_apply_at_and_after_their_sequence() {
        let mem = Memtable::new(1);
        mem.insert_range_tombstone(0, b"b", b"d", 7);
        assert_eq!(mem.range_tombstone_seq(0, b"c", 6), 0);
        assert_eq!(mem.range_tombstone_seq(0, b"c", 7), 7);
        assert_eq!(mem.range_tombstone_seq(0, b"d", 7), 0, "end is exclusive");
        assert_eq!(
            mem.range_tombstone_seq(1, b"c", 7),
            0,
            "other column family"
        );
    }

    #[test]
    fn the_cursor_walks_both_ways() {
        let mem = Arc::new(Memtable::new(1));
        for (i, k) in [b"a", b"b", b"c"].iter().enumerate() {
            mem.insert(0, *k, i as u64 + 1, Kind::Put, b"");
        }
        let mut it = MemIter::new(mem);
        it.seek_ge(&key::first_of(0, b"b"));
        assert_eq!(key::parse(it.key()).unwrap().user_key, b"b");
        it.next();
        assert_eq!(key::parse(it.key()).unwrap().user_key, b"c");
        it.next();
        assert!(!it.valid());
        it.seek_le(&key::last_of(0, b"b"));
        assert_eq!(key::parse(it.key()).unwrap().user_key, b"b");
        it.prev();
        assert_eq!(key::parse(it.key()).unwrap().user_key, b"a");
        it.prev();
        assert!(!it.valid());
    }
}
