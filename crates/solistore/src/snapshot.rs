//! Snapshots: one consistent view of the database for many reads.
//!
//! solistore keeps no old versions for readers: a flush writes only the
//! newest version of each key of a memtable, and a compaction only the
//! newest version of each key of its inputs. What a reader sees is fixed by
//! the [`SuperVersion`] it holds — the memtables and table files live when
//! it started, none of which ever changes — together with the sequence
//! number it reads at. An iterator has always held both.
//!
//! A [`Snapshot`] holds the same two things, for as many reads as needed:
//! iterators and point reads made from it, on any thread and at any time,
//! read the same sources at the same sequence number, whatever was written,
//! flushed or compacted since. Compaction never has to keep a version for
//! it: the files it replaces stay on disk, and the memtables in memory,
//! until the last snapshot or iterator holding them is dropped.
//!
//! That is also the cost of holding one: it pins up to
//! `max_immutable_memtables + 1` memtables and every table file of its
//! version, including those compaction has since replaced. Drop it when
//! the reads are done.

use std::fmt;
use std::sync::Arc;

use bytes::Bytes;

use crate::db::ColumnFamily;
use crate::error::Result;
use crate::iter::{DbIterator, IteratorMode, ReadOptions};
use crate::version::SuperVersion;

/// A consistent, read-only view of the whole database at one sequence
/// number, made by [`crate::Db::snapshot`] and released when dropped.
///
/// Cheap to clone and to share between threads: several iterators over
/// disjoint ranges, on as many threads, see exactly the same data, and
/// never part of a batch.
#[derive(Clone)]
pub struct Snapshot {
    sv: Arc<SuperVersion>,
    seq: u64,
    /// The database it was taken of (see `Db::iterator_opt`).
    db: u64,
}

impl Snapshot {
    /// `sv` must have been loaded before `seq` was read (see
    /// [`crate::version`]).
    pub(crate) fn new(sv: Arc<SuperVersion>, seq: u64, db: u64) -> Self {
        Snapshot { sv, seq, db }
    }

    pub(crate) fn parts(&self) -> (&Arc<SuperVersion>, u64, u64) {
        (&self.sv, self.seq, self.db)
    }

    /// The sequence number every read of this snapshot is at: the last
    /// committed batch it sees.
    pub fn sequence(&self) -> u64 {
        self.seq
    }

    /// The value of `key` in `cf`, as of the snapshot.
    pub fn get(&self, cf: &ColumnFamily, key: impl AsRef<[u8]>) -> Result<Option<Bytes>> {
        cf.check()?;
        self.sv.get(cf.id(), key.as_ref(), self.seq)
    }

    /// Several gets, as of the snapshot.
    pub fn multi_get<K: AsRef<[u8]>>(
        &self,
        cf: &ColumnFamily,
        keys: &[K],
    ) -> Result<Vec<Option<Bytes>>> {
        cf.check()?;
        keys.iter()
            .map(|k| self.sv.get(cf.id(), k.as_ref(), self.seq))
            .collect()
    }

    /// An iterator over `cf` as of the snapshot.
    pub fn iterator(&self, cf: &ColumnFamily, mode: IteratorMode<'_>) -> Result<DbIterator> {
        self.iterator_opt(cf, ReadOptions::default(), mode)
    }

    /// A bounded iterator over `cf` as of the snapshot (a snapshot in
    /// `opts` is not looked at: this one is read).
    pub fn iterator_opt(
        &self,
        cf: &ColumnFamily,
        opts: ReadOptions,
        mode: IteratorMode<'_>,
    ) -> Result<DbIterator> {
        cf.check()?;
        DbIterator::new(self.sv.clone(), cf.id(), self.seq, opts, mode)
    }

    /// Keys that cut `[lo, hi)` of `cf` into about `n` parts of equal size
    /// in this snapshot's memtables and tables; see
    /// [`crate::Db::split_points`].
    pub fn split_points(
        &self,
        cf: &ColumnFamily,
        lo: &[u8],
        hi: &[u8],
        n: usize,
    ) -> Result<Vec<Vec<u8>>> {
        cf.check()?;
        self.sv.split_points(cf.id(), lo, hi, n)
    }
}

impl fmt::Debug for Snapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Snapshot").field("seq", &self.seq).finish()
    }
}
