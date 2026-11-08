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
use crate::snapshot::Snapshot;
use crate::version::{Source, SuperVersion};

/// A cursor over internal keys in [`key::compare`] order.
pub trait InternalIterator: Send {
    /// Position at the first entry at or after `target`.
    fn seek_ge(&mut self, target: &[u8]);
    /// Position at the last entry at or before `target`.
    fn seek_le(&mut self, target: &[u8]);
    fn seek_to_last(&mut self);
    fn next(&mut self);
    /// [`InternalIterator::next`], then whether the cursor is still valid:
    /// one walk down a stack of cursors instead of two.
    #[inline]
    fn advance(&mut self) -> bool {
        self.next();
        self.valid()
    }
    fn prev(&mut self);
    fn valid(&self) -> bool;
    fn key(&self) -> &[u8];
    /// The value, as a handle that outlives the cursor.
    fn value(&self) -> Bytes;
    /// The value in place, valid until the cursor moves: no copy and no
    /// reference count.
    fn value_slice(&self) -> &[u8];
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
/// (smallest on top walking forward, largest walking backward). A step
/// moves the top child and compares it with the runner-up alone, which is
/// remembered between steps: one comparison while the same child keeps
/// winning, as it does through a run of keys only one source holds, and
/// none at all when one child is left. A child whose keys have all left the
/// walk's range (see [`MergingIterator::with_bound`]) is dropped instead of
/// being compared at every step.
pub struct MergingIterator<C: InternalIterator> {
    children: Vec<C>,
    heap: Vec<usize>,
    forward: bool,
    /// Where in `heap` the runner-up is (1 or 2), when known, and a copy of
    /// its key: a step compares the top child's key with it alone, and the
    /// runner-up does not move until it becomes the top.
    second: Option<usize>,
    second_key: Vec<u8>,
    /// A `cf ‖ user key` bound: walking forward, a child at or past it has
    /// nothing more for the walk; walking backward, one below it.
    bound: Option<Vec<u8>>,
}

impl<C: InternalIterator> MergingIterator<C> {
    pub fn new(children: Vec<C>) -> Self {
        MergingIterator {
            children,
            heap: Vec::new(),
            forward: true,
            second: None,
            second_key: Vec::new(),
            bound: None,
        }
    }

    /// Where the walk ends, as a `cf ‖ user key`: forward, the first key it
    /// does not read; backward, the last key it reads. Only prunes children:
    /// the caller still checks every key it takes against its own bounds.
    pub fn with_bound(mut self, bound: Option<Vec<u8>>) -> Self {
        self.bound = bound;
        self
    }

    /// Whether child `a` comes before child `b` in the walk's direction.
    #[inline]
    fn before(&self, a: usize, b: usize) -> bool {
        let order = key::compare(self.children[a].key(), self.children[b].key());
        if self.forward {
            order == Ordering::Less
        } else {
            order == Ordering::Greater
        }
    }

    /// Whether a valid child has nothing left within the bound.
    #[inline]
    fn exhausted(&self, child: usize) -> bool {
        let Some(bound) = &self.bound else {
            return false;
        };
        let id = key::cf_and_user_key(self.children[child].key());
        if self.forward {
            key::cmp_bytes(id, bound) != Ordering::Less
        } else {
            key::cmp_bytes(id, bound) == Ordering::Less
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
        self.second = None;
        self.heap = (0..self.children.len())
            .filter(|&i| self.children[i].valid() && !self.exhausted(i))
            .collect();
        for at in (0..self.heap.len() / 2).rev() {
            self.sift_down(at);
        }
    }

    /// Where the runner-up is in the heap, worked out (and its key copied)
    /// once per change of the heap's shape.
    #[inline]
    fn second(&mut self) -> usize {
        if let Some(second) = self.second {
            return second;
        }
        let at = if self.heap.len() > 2 && self.before(self.heap[2], self.heap[1]) {
            2
        } else {
            1
        };
        self.second = Some(at);
        self.second_key.clear();
        self.second_key
            .extend_from_slice(self.children[self.heap[at]].key());
        at
    }

    /// Drop the top child: exhausted, or past the bound.
    fn pop_top(&mut self) {
        self.second = None;
        let last = self.heap.len() - 1;
        self.heap.swap(0, last);
        self.heap.pop();
        if !self.heap.is_empty() {
            self.sift_down(0);
        }
    }

    /// The top child moved (`alive`: it is still valid): drop it if
    /// exhausted, then restore the heap.
    #[inline]
    fn advanced(&mut self, alive: bool) {
        if !alive {
            self.pop_top();
            return;
        }
        // Alone: nothing to compare with. The caller checks its bounds.
        if self.heap.len() == 1 {
            return;
        }
        let second = self.second();
        let top = self.heap[0];
        let order = key::compare(self.children[top].key(), &self.second_key);
        if order
            == if self.forward {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        {
            return;
        }
        if self.exhausted(top) {
            self.pop_top();
            return;
        }
        // The runner-up wins: it moves up, and the old top sinks from its
        // place.
        self.second = None;
        self.heap.swap(0, second);
        self.sift_down(second);
    }
}

impl<C: InternalIterator> InternalIterator for MergingIterator<C> {
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

    #[inline]
    fn next(&mut self) {
        debug_assert!(self.forward);
        if let Some(&top) = self.heap.first() {
            let alive = self.children[top].advance();
            self.advanced(alive);
        }
    }

    fn prev(&mut self) {
        debug_assert!(!self.forward);
        if let Some(&top) = self.heap.first() {
            self.children[top].prev();
            let alive = self.children[top].valid();
            self.advanced(alive);
        }
    }

    #[inline]
    fn valid(&self) -> bool {
        !self.heap.is_empty()
    }

    #[inline]
    fn key(&self) -> &[u8] {
        self.children[self.heap[0]].key()
    }

    fn value(&self) -> Bytes {
        self.children[self.heap[0]].value()
    }

    #[inline]
    fn value_slice(&self) -> &[u8] {
        self.children[self.heap[0]].value_slice()
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
/// `iterate_upper_bound` (lower inclusive, upper exclusive), and the
/// snapshot to read, as RocksDB's `ReadOptions::set_snapshot`.
///
/// Build it with `..Default::default()`: fields may be added.
#[derive(Debug, Clone, Default)]
pub struct ReadOptions {
    pub lower_bound: Option<Vec<u8>>,
    pub upper_bound: Option<Vec<u8>>,
    /// Read as of this snapshot of the same database (see
    /// [`crate::Db::snapshot`]) rather than as of now: iterators made on
    /// several threads with one snapshot see exactly the same data.
    pub snapshot: Option<Snapshot>,
}

/// An iterator over one column family, at one snapshot.
///
/// Entries come two ways: as an [`Iterator`] of owned `(Bytes, Bytes)`, or
/// borrowed from [`DbIterator::next_ref`], which walking forward copies and
/// allocates nothing per entry. The two may be mixed.
pub struct DbIterator {
    merged: MergingIterator<Source>,
    /// Keeps every memtable and file the cursors read alive.
    _sv: Arc<SuperVersion>,
    cf: u32,
    snapshot: u64,
    direction: Direction,
    /// Walking forward, the first `cf ‖ user key` past the range (`None`:
    /// nothing is, the last column family without an upper bound).
    /// Walking backward, the first one in it.
    end: Option<Vec<u8>>,
    /// The range tombstones of the column family visible at the snapshot
    /// that reach into the iterator's bounds.
    tombstones: Vec<RangeTombstone>,
    done: bool,
    /// `cf ‖ user key` of the entry last returned, or being skipped.
    id: Vec<u8>,
    /// Walking forward: the cursors still stand on the entry last returned,
    /// whose value is read in place until the next step.
    on_entry: bool,
    /// Walking forward: entries of `self.id` are older versions of a key
    /// already handled, to be skipped.
    skipping: bool,
    /// Walking backward, the value of the entry last returned: the cursors
    /// are already past it.
    value: Bytes,
}

/// What a step of the walk found.
enum Step {
    Entry,
    End,
    Corrupt,
}

impl DbIterator {
    pub(crate) fn new(
        sv: Arc<SuperVersion>,
        cf: u32,
        snapshot: u64,
        opts: ReadOptions,
        mode: IteratorMode<'_>,
    ) -> Result<Self> {
        let ReadOptions {
            lower_bound: lower,
            upper_bound: upper,
            ..
        } = opts;
        let id_of = |user_key: &[u8]| [&cf.to_be_bytes()[..], user_key].concat();
        let direction = match mode {
            IteratorMode::Start | IteratorMode::From(_, Direction::Forward) => Direction::Forward,
            IteratorMode::End | IteratorMode::From(_, Direction::Reverse) => Direction::Reverse,
        };
        let end = match direction {
            Direction::Forward => match (&upper, cf.checked_add(1)) {
                (Some(upper), _) => Some(id_of(upper)),
                (None, Some(next_cf)) => Some(next_cf.to_be_bytes().to_vec()),
                (None, None) => None,
            },
            Direction::Reverse => Some(id_of(lower.as_deref().unwrap_or_default())),
        };
        let mut merged = MergingIterator::new(sv.iterators(cf)?).with_bound(end.clone());
        match mode {
            IteratorMode::Start => {
                merged.seek_ge(&key::first_of(cf, lower.as_deref().unwrap_or_default()));
            }
            IteratorMode::From(from, Direction::Forward) => {
                let begin = match &lower {
                    Some(lower) if lower.as_slice() > from => lower.as_slice(),
                    _ => from,
                };
                merged.seek_ge(&key::first_of(cf, begin));
            }
            IteratorMode::End => match (&upper, cf.checked_add(1)) {
                (Some(upper), _) => merged.seek_le(&key::first_of(cf, upper)),
                (None, Some(next_cf)) => merged.seek_le(&key::first_of(next_cf, b"")),
                (None, None) => merged.seek_to_last(),
            },
            IteratorMode::From(from, Direction::Reverse) => match &upper {
                Some(upper) if from >= upper.as_slice() => {
                    merged.seek_le(&key::first_of(cf, upper))
                }
                _ => merged.seek_le(&key::last_of(cf, from)),
            },
        }
        // Only the tombstones that reach into the bounds can cover a key
        // the iterator returns; most scans then check none.
        let mut tombstones = sv.tombstones(cf, snapshot);
        tombstones.retain(|t| {
            t.start < t.end
                && lower.as_deref().is_none_or(|l| &t.end[..] > l)
                && upper.as_deref().is_none_or(|u| &t.start[..] < u)
        });
        Ok(DbIterator {
            merged,
            _sv: sv,
            cf,
            snapshot,
            direction,
            end,
            tombstones,
            done: false,
            id: Vec::new(),
            on_entry: false,
            skipping: false,
            value: Bytes::new(),
        })
    }

    /// The next entry, borrowed: the key and value stay valid until the next
    /// call on this iterator.
    ///
    /// Walking forward, the value is read in place (in the block cache or
    /// the memtable) and the key is copied into a buffer the iterator
    /// reuses: no allocation and no reference count per entry. Walking
    /// backward, a key's newest version is only known once its older ones
    /// have been passed, so its value is held as `Bytes` meanwhile.
    pub fn next_ref(&mut self) -> Option<Result<(&[u8], &[u8])>> {
        if self.done {
            return None;
        }
        let step = match self.direction {
            Direction::Forward => self.step_forward(),
            Direction::Reverse => self.step_reverse(),
        };
        match step {
            Step::Entry => {
                let user_key = &self.id[key::CF_LEN..];
                let value = match self.direction {
                    Direction::Forward => self.merged.value_slice(),
                    Direction::Reverse => &self.value[..],
                };
                Some(Ok((user_key, value)))
            }
            Step::End => self.finish(),
            Step::Corrupt => self.corrupt(),
        }
    }

    fn finish<T>(&mut self) -> Option<Result<T>> {
        self.done = true;
        self.merged.status().err().map(Err)
    }

    fn corrupt<T>(&mut self) -> Option<Result<T>> {
        self.done = true;
        Some(Err(Error::Corruption("bad internal key".into())))
    }

    #[inline]
    fn visible(&self, user_key: &[u8], seq: u64) -> bool {
        // `<=`, as `get` does: no covering tombstone is 0, and since
        // compaction writes sequence 0 at the bottom of the tree, an entry
        // there with nothing over it must still be visible.
        self.tombstones.is_empty()
            || covering_seq(&self.tombstones, self.cf, user_key, self.snapshot) <= seq
    }

    /// Whether the cursors stand on another version of `self.id`.
    #[inline]
    fn on_same_key(&self) -> bool {
        self.merged.valid() && key::eq_bytes(key::cf_and_user_key(self.merged.key()), &self.id)
    }

    /// Move the cursors onto the next visible entry, walking forward, and
    /// leave its `cf ‖ user key` in `self.id`. Each entry passed is looked
    /// at once.
    #[inline]
    fn step_forward(&mut self) -> Step {
        if self.on_entry {
            self.on_entry = false;
            self.merged.next();
        }
        loop {
            if !self.merged.valid() {
                return Step::End;
            }
            let k = self.merged.key();
            if k.len() < key::MIN_LEN {
                return Step::Corrupt;
            }
            let id = key::cf_and_user_key(k);
            if self.skipping {
                // An older version of the key handled last.
                if key::eq_bytes(id, &self.id) {
                    self.merged.next();
                    continue;
                }
                self.skipping = false;
            }
            if self
                .end
                .as_deref()
                .is_some_and(|end| key::cmp_bytes(id, end) != Ordering::Less)
            {
                return Step::End;
            }
            let trailer = key::trailer(k);
            if trailer >> 8 > self.snapshot {
                self.merged.next();
                continue;
            }
            let Some(kind) = Kind::from_u8(trailer as u8) else {
                return Step::Corrupt;
            };
            self.id.clear();
            self.id.extend_from_slice(id);
            self.skipping = true;
            // The newest version at the snapshot: visible, or its key is
            // deleted and every version skipped.
            if kind == Kind::Put && self.visible(&self.id[key::CF_LEN..], trailer >> 8) {
                self.on_entry = true;
                return Step::Entry;
            }
            self.merged.next();
        }
    }

    /// Move the cursors past the next visible entry, walking backward: its
    /// `cf ‖ user key` is left in `self.id` and its value in `self.value`.
    fn step_reverse(&mut self) -> Step {
        loop {
            if !self.merged.valid() {
                return Step::End;
            }
            let Some(k) = key::parse(self.merged.key()) else {
                return Step::Corrupt;
            };
            let id = key::cf_and_user_key(self.merged.key());
            if k.cf != self.cf
                || self
                    .end
                    .as_deref()
                    .is_some_and(|begin| key::cmp_bytes(id, begin) == Ordering::Less)
            {
                return Step::End;
            }
            // Walking backward, a key's versions come oldest first: the last
            // one at or below the snapshot is the visible one.
            self.id.clear();
            self.id.extend_from_slice(id);
            let mut newest: Option<(u64, Kind)> = None;
            while self.on_same_key() {
                let Some(p) = key::parse(self.merged.key()) else {
                    return Step::Corrupt;
                };
                if p.seq <= self.snapshot {
                    newest = Some((p.seq, p.kind));
                    self.value = self.merged.value();
                }
                self.merged.prev();
            }
            if let Some((seq, Kind::Put)) = newest {
                if self.visible(&self.id[key::CF_LEN..], seq) {
                    return Step::Entry;
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
        let step = match self.direction {
            Direction::Forward => self.step_forward(),
            Direction::Reverse => self.step_reverse(),
        };
        match step {
            Step::Entry => {
                let value = match self.direction {
                    Direction::Forward => self.merged.value(),
                    Direction::Reverse => std::mem::take(&mut self.value),
                };
                Some(Ok((Bytes::copy_from_slice(&self.id[key::CF_LEN..]), value)))
            }
            Step::End => self.finish(),
            Step::Corrupt => self.corrupt(),
        }
    }
}
