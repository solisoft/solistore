//! Iterators: cursors over internal keys, their merge, and the user-facing
//! iterator over one column family.
//!
//! SoliDB's iterators never change direction once created (RocksDB's
//! `IteratorMode`), so a forward and a reverse iterator are separate walks
//! rather than one cursor that can turn around.
//!
//! An iterator reads at the snapshot taken when it was created: batches
//! committed afterwards are invisible to it, and so is every part of a batch
//! that was still being applied.

use std::cmp::Ordering;
use std::sync::Arc;

use bytes::Bytes;

use crate::error::{Error, Result};
use crate::key::{self, Kind};
use crate::memtable::{covering_seq, RangeTombstone};
use crate::version::SuperVersion;

/// A cursor over internal keys in [`key::compare`] order.
pub trait InternalIterator: Send {
    /// Position at the first entry at or after `target`.
    fn seek_ge(&mut self, target: &[u8]);
    /// Position at the last entry at or before `target`.
    fn seek_le(&mut self, target: &[u8]);
    fn seek_to_last(&mut self);
    fn next(&mut self);
    fn prev(&mut self);
    fn valid(&self) -> bool;
    fn key(&self) -> &[u8];
    fn value(&self) -> Bytes;
    /// An I/O or corruption error that made the cursor stop early.
    fn status(&self) -> Result<()>;
}

#[cfg(test)]
pub fn collect_all(it: &mut dyn InternalIterator) -> Vec<(Vec<u8>, Bytes)> {
    let mut out = Vec::new();
    // Not an all-zero key: its trailer would sort after cf 0's empty key.
    it.seek_ge(&key::first_of(0, b""));
    while it.valid() {
        out.push((it.key().to_vec(), it.value()));
        it.next();
    }
    out
}

/// The union of several cursors, in order. Used in one direction per
/// positioning: `next` after `seek_ge`, `prev` after `seek_le`/`seek_to_last`.
///
/// The valid children are kept in a binary heap on their current keys
/// (smallest on top walking forward, largest walking backward), so a step
/// costs a few comparisons however many tables level 0 holds.
pub struct MergingIterator {
    children: Vec<Box<dyn InternalIterator>>,
    heap: Vec<usize>,
    forward: bool,
}

impl MergingIterator {
    pub fn new(children: Vec<Box<dyn InternalIterator>>) -> Self {
        MergingIterator {
            children,
            heap: Vec::new(),
            forward: true,
        }
    }

    /// Whether child `a` comes before child `b` in the walk's direction.
    fn before(&self, a: usize, b: usize) -> bool {
        let order = key::compare(self.children[a].key(), self.children[b].key());
        if self.forward {
            order == Ordering::Less
        } else {
            order == Ordering::Greater
        }
    }

    fn sift_down(&mut self, mut at: usize) {
        loop {
            let (left, right) = (2 * at + 1, 2 * at + 2);
            let mut best = at;
            if left < self.heap.len() && self.before(self.heap[left], self.heap[best]) {
                best = left;
            }
            if right < self.heap.len() && self.before(self.heap[right], self.heap[best]) {
                best = right;
            }
            if best == at {
                return;
            }
            self.heap.swap(at, best);
            at = best;
        }
    }

    fn rebuild(&mut self, forward: bool) {
        self.forward = forward;
        self.heap = (0..self.children.len())
            .filter(|&i| self.children[i].valid())
            .collect();
        for at in (0..self.heap.len() / 2).rev() {
            self.sift_down(at);
        }
    }

    /// The top child moved: drop it if exhausted, then restore the heap.
    fn advanced(&mut self) {
        if !self.children[self.heap[0]].valid() {
            let last = self.heap.len() - 1;
            self.heap.swap(0, last);
            self.heap.pop();
        }
        if !self.heap.is_empty() {
            self.sift_down(0);
        }
    }
}

impl InternalIterator for MergingIterator {
    fn seek_ge(&mut self, target: &[u8]) {
        self.children.iter_mut().for_each(|c| c.seek_ge(target));
        self.rebuild(true);
    }

    fn seek_le(&mut self, target: &[u8]) {
        self.children.iter_mut().for_each(|c| c.seek_le(target));
        self.rebuild(false);
    }

    fn seek_to_last(&mut self) {
        self.children.iter_mut().for_each(|c| c.seek_to_last());
        self.rebuild(false);
    }

    fn next(&mut self) {
        debug_assert!(self.forward);
        if let Some(&top) = self.heap.first() {
            self.children[top].next();
            self.advanced();
        }
    }

    fn prev(&mut self) {
        debug_assert!(!self.forward);
        if let Some(&top) = self.heap.first() {
            self.children[top].prev();
            self.advanced();
        }
    }

    fn valid(&self) -> bool {
        !self.heap.is_empty()
    }

    fn key(&self) -> &[u8] {
        self.children[self.heap[0]].key()
    }

    fn value(&self) -> Bytes {
        self.children[self.heap[0]].value()
    }

    fn status(&self) -> Result<()> {
        self.children.iter().try_for_each(|c| c.status())
    }
}

// ---------------------------------------------------------------- the user iterator

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
    merged: MergingIterator,
    /// Keeps every memtable and file the cursors read alive.
    _sv: Arc<SuperVersion>,
    cf: u32,
    snapshot: u64,
    lower: Option<Vec<u8>>,
    upper: Option<Vec<u8>>,
    direction: Direction,
    tombstones: Vec<RangeTombstone>,
    done: bool,
}

impl DbIterator {
    pub(crate) fn new(
        sv: Arc<SuperVersion>,
        cf: u32,
        snapshot: u64,
        opts: ReadOptions,
        mode: IteratorMode<'_>,
    ) -> Result<Self> {
        let mut merged = MergingIterator::new(sv.iterators(cf)?);
        let (lower, upper) = (opts.lower_bound, opts.upper_bound);
        let direction = match mode {
            IteratorMode::Start => {
                merged.seek_ge(&key::first_of(cf, lower.as_deref().unwrap_or_default()));
                Direction::Forward
            }
            IteratorMode::From(from, Direction::Forward) => {
                let begin = match &lower {
                    Some(lower) if lower.as_slice() > from => lower.as_slice(),
                    _ => from,
                };
                merged.seek_ge(&key::first_of(cf, begin));
                Direction::Forward
            }
            IteratorMode::End => {
                match (&upper, cf.checked_add(1)) {
                    (Some(upper), _) => merged.seek_le(&key::first_of(cf, upper)),
                    (None, Some(next_cf)) => merged.seek_le(&key::first_of(next_cf, b"")),
                    (None, None) => merged.seek_to_last(),
                }
                Direction::Reverse
            }
            IteratorMode::From(from, Direction::Reverse) => {
                match &upper {
                    Some(upper) if from >= upper.as_slice() => {
                        merged.seek_le(&key::first_of(cf, upper))
                    }
                    _ => merged.seek_le(&key::last_of(cf, from)),
                }
                Direction::Reverse
            }
        };
        let tombstones = sv.tombstones(cf, snapshot);
        Ok(DbIterator {
            merged,
            _sv: sv,
            cf,
            snapshot,
            lower,
            upper,
            direction,
            tombstones,
            done: false,
        })
    }

    fn finish(&mut self) -> Option<Result<(Bytes, Bytes)>> {
        self.done = true;
        self.merged.status().err().map(Err)
    }

    fn corrupt(&mut self) -> Option<Result<(Bytes, Bytes)>> {
        self.done = true;
        Some(Err(Error::Corruption("bad internal key".into())))
    }

    fn visible(&self, user_key: &[u8], seq: u64, kind: Kind) -> bool {
        kind == Kind::Put && covering_seq(&self.tombstones, self.cf, user_key, self.snapshot) < seq
    }

    fn same_key(&self, user_key: &[u8]) -> bool {
        self.merged.valid()
            && key::parse(self.merged.key())
                .is_some_and(|p| p.cf == self.cf && p.user_key == user_key)
    }

    fn next_forward(&mut self) -> Option<Result<(Bytes, Bytes)>> {
        loop {
            if !self.merged.valid() {
                return self.finish();
            }
            let Some(k) = key::parse(self.merged.key()) else {
                return self.corrupt();
            };
            if k.cf != self.cf
                || self
                    .upper
                    .as_deref()
                    .is_some_and(|upper| k.user_key >= upper)
            {
                return self.finish();
            }
            if k.seq > self.snapshot {
                self.merged.next();
                continue;
            }
            // The newest visible version; skip the older ones.
            let user_key = Bytes::copy_from_slice(k.user_key);
            let (seq, kind, value) = (k.seq, k.kind, self.merged.value());
            self.merged.next();
            while self.same_key(&user_key) {
                self.merged.next();
            }
            if self.visible(&user_key, seq, kind) {
                return Some(Ok((user_key, value)));
            }
        }
    }

    fn next_reverse(&mut self) -> Option<Result<(Bytes, Bytes)>> {
        loop {
            if !self.merged.valid() {
                return self.finish();
            }
            let Some(k) = key::parse(self.merged.key()) else {
                return self.corrupt();
            };
            if k.cf != self.cf
                || self
                    .lower
                    .as_deref()
                    .is_some_and(|lower| k.user_key < lower)
            {
                return self.finish();
            }
            // Walking backward, a key's versions come oldest first: the last
            // one at or below the snapshot is the visible one.
            let user_key = Bytes::copy_from_slice(k.user_key);
            let mut newest: Option<(u64, Kind, Bytes)> = None;
            while self.same_key(&user_key) {
                let p = key::parse(self.merged.key()).expect("checked by same_key");
                if p.seq <= self.snapshot {
                    newest = Some((p.seq, p.kind, self.merged.value()));
                }
                self.merged.prev();
            }
            if let Some((seq, kind, value)) = newest {
                if self.visible(&user_key, seq, kind) {
                    return Some(Ok((user_key, value)));
                }
            }
        }
    }
}

impl Iterator for DbIterator {
    type Item = Result<(Bytes, Bytes)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        match self.direction {
            Direction::Forward => self.next_forward(),
            Direction::Reverse => self.next_reverse(),
        }
    }
}
