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
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use parking_lot::RwLock;

use crate::key::{InternalKey, Kind};

/// Bookkeeping per entry, on top of key and value, for size accounting.
const ENTRY_OVERHEAD: usize = 64;

#[derive(Debug, Clone)]
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

/// The newest version of a key visible at a snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Version {
    pub seq: u64,
    pub kind: Kind,
    pub value: Bytes,
}

#[derive(Default)]
pub struct Memtable {
    map: SkipMap<InternalKey, Bytes>,
    tombstones: RwLock<Vec<RangeTombstone>>,
    size: AtomicUsize,
}

impl Memtable {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&self, cf: u32, key: &[u8], seq: u64, kind: Kind, value: &[u8]) {
        self.size
            .fetch_add(key.len() + value.len() + ENTRY_OVERHEAD, Ordering::Relaxed);
        let key = InternalKey::new(cf, Bytes::copy_from_slice(key), seq, kind);
        self.map.insert(key, Bytes::copy_from_slice(value));
    }

    pub fn insert_range_tombstone(&self, cf: u32, start: &[u8], end: &[u8], seq: u64) {
        self.size
            .fetch_add(start.len() + end.len() + ENTRY_OVERHEAD, Ordering::Relaxed);
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

    /// The newest version of `key` with a sequence number at most `snapshot`.
    pub fn get(&self, cf: u32, key: &[u8], snapshot: u64) -> Option<Version> {
        self.newest_at(cf, Bytes::copy_from_slice(key), snapshot)
    }

    pub(crate) fn newest_at(&self, cf: u32, key: Bytes, snapshot: u64) -> Option<Version> {
        let probe = InternalKey::at_snapshot(cf, key, snapshot);
        let entry = self.map.lower_bound(Bound::Included(&probe))?;
        let found = entry.key();
        (found.cf == cf && found.user_key == probe.user_key).then(|| Version {
            seq: found.seq,
            kind: found.kind,
            value: entry.value().clone(),
        })
    }

    /// The highest sequence number, at most `snapshot`, of a range delete
    /// covering `key` (0 if none). A version older than it is deleted.
    pub fn range_tombstone_seq(&self, cf: u32, key: &[u8], snapshot: u64) -> u64 {
        self.tombstones
            .read()
            .iter()
            .filter(|t| t.seq <= snapshot && t.covers(cf, key))
            .map(|t| t.seq)
            .max()
            .unwrap_or(0)
    }

    /// The first entry at or after `bound` (`Bound::Excluded` for strictly after).
    pub(crate) fn seek_ge(&self, bound: Bound<&InternalKey>) -> Option<InternalKey> {
        self.map.lower_bound(bound).map(|e| e.key().clone())
    }

    /// The last entry at or before `bound` (`Bound::Excluded` for strictly before).
    pub(crate) fn seek_le(&self, bound: Bound<&InternalKey>) -> Option<InternalKey> {
        self.map.upper_bound(bound).map(|e| e.key().clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_reader_sees_the_newest_version_at_its_snapshot() {
        let mem = Memtable::new();
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
        let mem = Memtable::new();
        mem.insert(1, b"k", 1, Kind::Put, b"cf1");
        mem.insert(0, b"kk", 2, Kind::Put, b"longer");
        assert_eq!(mem.get(0, b"k", 10), None);
        assert_eq!(mem.get(1, b"kk", 10), None);
    }

    #[test]
    fn range_tombstones_apply_at_and_after_their_sequence() {
        let mem = Memtable::new();
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
}
