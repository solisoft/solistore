//! Iterators over one column family, bounded, in one direction.
//!
//! SoliDB's iterators never change direction once created (RocksDB's
//! `IteratorMode`), so a forward and a reverse iterator are separate walks
//! rather than one cursor that can turn around.
//!
//! An iterator reads at the snapshot taken when it was created: batches
//! committed afterwards are invisible to it, and so is every part of a batch
//! that was still being applied.

use std::ops::Bound;
use std::sync::Arc;

use bytes::Bytes;

use crate::error::Result;
use crate::key::{InternalKey, Kind};
use crate::memtable::Memtable;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Forward,
    Reverse,
}

/// Where an iterator starts, as in RocksDB's `IteratorMode`.
#[derive(Debug, Clone, Copy)]
pub enum IteratorMode<'a> {
    /// The first key (or the lower bound), walking forward.
    Start,
    /// The last key (or just under the upper bound), walking backward.
    End,
    /// The first key at or after this one (`Forward`), or the last key at or
    /// before it (`Reverse`).
    From(&'a [u8], Direction),
}

/// Bounds in the style of RocksDB's `iterate_lower_bound` /
/// `iterate_upper_bound`: lower inclusive, upper exclusive.
#[derive(Debug, Clone, Default)]
pub struct ReadOptions {
    pub lower_bound: Option<Vec<u8>>,
    pub upper_bound: Option<Vec<u8>>,
}

pub struct DbIterator {
    mem: Arc<Memtable>,
    cf: u32,
    snapshot: u64,
    lower: Option<Bytes>,
    upper: Option<Bytes>,
    direction: Direction,
    /// Where the next step searches from; `None` once exhausted.
    next: Option<Bound<InternalKey>>,
}

impl DbIterator {
    pub(crate) fn new(
        mem: Arc<Memtable>,
        cf: u32,
        snapshot: u64,
        opts: ReadOptions,
        mode: IteratorMode<'_>,
    ) -> Self {
        let lower = opts.lower_bound.map(Bytes::from);
        let upper = opts.upper_bound.map(Bytes::from);
        let (direction, from) = match mode {
            IteratorMode::Start => (Direction::Forward, None),
            IteratorMode::End => (Direction::Reverse, None),
            IteratorMode::From(key, direction) => (direction, Some(Bytes::copy_from_slice(key))),
        };
        let start = match direction {
            Direction::Forward => {
                let begin = match (from, &lower) {
                    (Some(from), Some(lower)) => from.max(lower.clone()),
                    (Some(from), None) => from,
                    (None, Some(lower)) => lower.clone(),
                    (None, None) => Bytes::new(),
                };
                Bound::Included(InternalKey::first_of(cf, begin))
            }
            Direction::Reverse => match (from, &upper) {
                (Some(from), Some(upper)) if from >= *upper => {
                    Bound::Excluded(InternalKey::first_of(cf, upper.clone()))
                }
                (Some(from), _) => Bound::Included(InternalKey::last_of(cf, from)),
                (None, Some(upper)) => Bound::Excluded(InternalKey::first_of(cf, upper.clone())),
                (None, None) => match cf.checked_add(1) {
                    Some(next_cf) => Bound::Excluded(InternalKey::first_of(next_cf, Bytes::new())),
                    None => Bound::Unbounded,
                },
            },
        };
        DbIterator {
            mem,
            cf,
            snapshot,
            lower,
            upper,
            direction,
            next: Some(start),
        }
    }

    /// The next user key in the walk (visible or not), or `None` past the
    /// end of the range.
    fn step(&mut self) -> Option<Bytes> {
        let bound = self.next.take()?;
        let found = match self.direction {
            Direction::Forward => self.mem.seek_ge(bound.as_ref()),
            Direction::Reverse => self.mem.seek_le(bound.as_ref()),
        }?;
        if found.cf != self.cf {
            return None;
        }
        let key = found.user_key;
        match self.direction {
            Direction::Forward => {
                if self.upper.as_ref().is_some_and(|upper| key >= *upper) {
                    return None;
                }
                self.next = Some(Bound::Excluded(InternalKey::last_of(self.cf, key.clone())));
            }
            Direction::Reverse => {
                if self.lower.as_ref().is_some_and(|lower| key < *lower) {
                    return None;
                }
                self.next = Some(Bound::Excluded(InternalKey::first_of(self.cf, key.clone())));
            }
        }
        Some(key)
    }
}

impl Iterator for DbIterator {
    type Item = Result<(Bytes, Bytes)>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let key = self.step()?;
            let Some(version) = self.mem.newest_at(self.cf, key.clone(), self.snapshot) else {
                continue; // every version of this key is newer than the snapshot
            };
            if version.kind == Kind::Put
                && self.mem.range_tombstone_seq(self.cf, &key, self.snapshot) < version.seq
            {
                return Some(Ok((key, version.value)));
            }
        }
    }
}
