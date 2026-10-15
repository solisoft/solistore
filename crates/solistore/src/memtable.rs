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

use std::ops::Bound;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use crossbeam_skiplist::map::Entry;
use crossbeam_skiplist::SkipMap;
use parking_lot::RwLock;

use crate::error::Result;
use crate::iter::InternalIterator;
use crate::key::{self, KeyRef, Kind, MemKey};

/// What an entry counts toward the write buffer on top of its key and
/// value bytes (which live in a writer's arena chunk, see [`arena_copy`]).
///
/// Its real cost is higher: a crossbeam skiplist node with two `Bytes`
/// handles is ~112 bytes after allocator rounding. Counting that made a
/// 64 MiB memtable hold ~30% fewer entries than RocksDB's, which counts
/// ~30 bytes of overhead per entry, so it flushed 45% more often. Each
/// extra level-0 file meant another L0->L1 compaction rewriting all of L1.
/// On SoliDB's update phase (1M documents), compaction writes went from
/// 757 to 515 bytes per update and throughput up 5%. The memtables' real
/// footprint is then up to ~1.3x the write buffer size.
const ENTRY_OVERHEAD: usize = 32;

/// Size of the chunks writers copy keys and values into.
const ARENA_CHUNK: usize = 1 << 20;

thread_local! {
    static ARENA: std::cell::RefCell<bytes::BytesMut> = std::cell::RefCell::new(bytes::BytesMut::new());
}

/// A value handed out by a point lookup, as its own allocation when it is a
/// small part of the buffer it slices (`backing` bytes).
///
/// A `Bytes` slice keeps its whole buffer alive: a 250-byte document read
/// from a table pinned its 4 KiB decoded block, and one read from a memtable
/// its 1 MiB arena chunk, for as long as the caller held it — past the
/// block's eviction from the cache and past the memtable's flush. A
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

/// Copy `len` bytes, written by `fill`, into this thread's arena chunk and
/// return them as one shared buffer. Entries are then slices of it: one
/// allocation per megabyte instead of two per entry. Allocations were most
/// of a memtable's real size for SoliDB's small index entries, which made
/// memtables flush with little data in them and level 0 compact over and
/// over.
pub fn arena_copy(len: usize, fill: impl FnOnce(&mut bytes::BytesMut)) -> Bytes {
    ARENA.with(|arena| {
        let mut arena = arena.borrow_mut();
        if arena.capacity() - arena.len() < len {
            *arena = bytes::BytesMut::with_capacity(len.max(ARENA_CHUNK));
        }
        fill(&mut arena);
        arena.split().freeze()
    })
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
    map: SkipMap<MemKey, Bytes>,
    tombstones: RwLock<Vec<RangeTombstone>>,
    /// `tombstones` is not empty. Lets every point read skip the lock: a
    /// read lock is a write to a shared cache line, on every get.
    has_tombstones: AtomicBool,
    size: AtomicUsize,
    /// The highest sequence number inserted.
    max_seq: AtomicU64,
    /// The first WAL file holding any of this memtable's writes: once it is
    /// flushed, WALs below the next memtable's `first_wal` are dead.
    first_wal: u64,
}

impl Memtable {
    pub fn new(first_wal: u64) -> Self {
        Memtable {
            map: SkipMap::new(),
            tombstones: RwLock::new(Vec::new()),
            has_tombstones: AtomicBool::new(false),
            size: AtomicUsize::new(0),
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
        let bytes = self.insert_entry(cf, key, seq, kind, value);
        self.account(bytes, seq);
    }

    /// Insert without updating the shared size and sequence counters;
    /// returns the bytes to [`Memtable::account`] for. Writers inserting a
    /// batch account once for all of it: eight writers bumping the same two
    /// atomics for every entry would bounce their cache line between cores.
    #[cfg(test)]
    pub fn insert_entry(&self, cf: u32, key: &[u8], seq: u64, kind: Kind, value: &[u8]) -> usize {
        let encoded = MemKey(Bytes::from(key::encode(cf, key, seq, kind)));
        self.map.insert(encoded, Bytes::copy_from_slice(value));
        key.len() + value.len() + ENTRY_OVERHEAD
    }

    /// Insert an entry whose encoded key and value are already slices of an
    /// arena buffer; returns its size for [`Memtable::account`].
    pub fn insert_shared(&self, encoded_key: Bytes, value: Bytes) -> usize {
        let bytes = encoded_key.len() + value.len() + ENTRY_OVERHEAD;
        self.map.insert(MemKey(encoded_key), value);
        bytes
    }

    pub fn account(&self, bytes: usize, max_seq: u64) {
        self.size.fetch_add(bytes, Ordering::Relaxed);
        self.max_seq.fetch_max(max_seq, Ordering::Relaxed);
    }

    pub fn insert_range_tombstone(&self, cf: u32, start: &[u8], end: &[u8], seq: u64) {
        self.max_seq.fetch_max(seq, Ordering::Relaxed);
        self.size
            .fetch_add(start.len() + end.len() + ENTRY_OVERHEAD, Ordering::Relaxed);
        self.has_tombstones.store(true, Ordering::Release);
        self.tombstones.write().push(RangeTombstone {
            cf,
            start: Bytes::copy_from_slice(start),
            end: Bytes::copy_from_slice(end),
            seq,
        });
    }

    /// Approximate memory held, in bytes.
    pub fn approximate_size(&self) -> usize {
        self.size.load(Ordering::Relaxed)
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty() && !self.has_tombstones.load(Ordering::Acquire)
    }

    /// The newest version of `key` with a sequence number at most `snapshot`.
    pub fn get(&self, cf: u32, key: &[u8], snapshot: u64) -> Option<Version> {
        let probe = key::at_snapshot(cf, key, snapshot);
        let entry = self.map.lower_bound(Bound::Included(KeyRef::new(&probe)))?;
        let found = key::parse(&entry.key().0)?;
        (found.cf == cf && found.user_key == key).then(|| Version {
            seq: found.seq,
            kind: found.kind,
            value: detach(entry.value().clone(), ARENA_CHUNK),
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
    pub fn entries(&self) -> impl Iterator<Item = (Bytes, Bytes)> + '_ {
        self.map
            .iter()
            .map(|e| (e.key().0.clone(), e.value().clone()))
    }
}

/// A cursor over a memtable. Each step is a fresh search from the current
/// key, which keeps the cursor independent of the skiplist's lifetimes.
pub struct MemIter {
    mem: Arc<Memtable>,
    current: Option<(Bytes, Bytes)>,
}

impl MemIter {
    pub fn new(mem: Arc<Memtable>) -> Self {
        MemIter { mem, current: None }
    }
}

fn owned(entry: Option<Entry<'_, MemKey, Bytes>>) -> Option<(Bytes, Bytes)> {
    entry.map(|e| (e.key().0.clone(), e.value().clone()))
}

impl InternalIterator for MemIter {
    fn seek_ge(&mut self, target: &[u8]) {
        self.current = owned(
            self.mem
                .map
                .lower_bound(Bound::Included(KeyRef::new(target))),
        );
    }

    fn seek_le(&mut self, target: &[u8]) {
        self.current = owned(
            self.mem
                .map
                .upper_bound(Bound::Included(KeyRef::new(target))),
        );
    }

    fn seek_to_last(&mut self) {
        self.current = owned(self.mem.map.back());
    }

    fn next(&mut self) {
        if let Some((key, _)) = self.current.take() {
            self.current = owned(self.mem.map.lower_bound(Bound::Excluded(KeyRef::new(&key))));
        }
    }

    fn prev(&mut self) {
        if let Some((key, _)) = self.current.take() {
            self.current = owned(self.mem.map.upper_bound(Bound::Excluded(KeyRef::new(&key))));
        }
    }

    fn valid(&self) -> bool {
        self.current.is_some()
    }

    fn key(&self) -> &[u8] {
        &self.current.as_ref().expect("valid").0
    }

    fn value(&self) -> Bytes {
        self.current.as_ref().expect("valid").1.clone()
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
