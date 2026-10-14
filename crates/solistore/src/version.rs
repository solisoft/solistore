//! What a reader reads: the memtables and table files live at one moment.
//!
//! A [`SuperVersion`] is immutable. Writers that change the set (a memtable
//! switch, a flush, a compaction) build a new one and swap it in; a reader
//! that loaded the old one keeps it, and with it every file it names, until
//! the reader is done. A file dropped from the live set is deleted when the
//! last version naming it goes away.
//!
//! A reader must load the super version *before* its snapshot sequence. A
//! flush keeps only the newest version of each key in a memtable, and every
//! entry of a sealed memtable is at or below the snapshot of anyone who
//! loads a later super version, so such a reader never needs what was
//! dropped. Loading in the other order would allow a stale snapshot against
//! a flushed file.

use std::cmp::Ordering;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::sync::{Arc, OnceLock};

use bytes::Bytes;

use crate::error::{Error, Result};
use crate::fs::Fs;
use crate::iter::InternalIterator;
use crate::key::{self, Kind};
use crate::memtable::{covering_seq, MemIter, Memtable, RangeTombstone, Version as Found};
use crate::sst::{BlockCache, TableIter, TableProps, TableReader};

pub const NUM_LEVELS: usize = 7;

pub fn table_name(number: u64) -> String {
    format!("{number:06}.sst")
}

pub fn parse_table_name(name: &str) -> Option<u64> {
    name.strip_suffix(".sst")?.parse().ok()
}

/// A table file, opened on first use and deleted once obsolete and unused.
pub struct TableFile {
    pub number: u64,
    pub props: TableProps,
    path: PathBuf,
    fs: Arc<dyn Fs>,
    cache: Arc<BlockCache>,
    reader: OnceLock<Arc<TableReader>>,
    /// Held while the reader is opened, so it is opened once: concurrent
    /// readers of a table nobody had opened yet each read its index and
    /// filter (a few MB) and all but one threw theirs away. With hundreds
    /// of SoliDB query threads reaching a compaction's new tables at once,
    /// that briefly took 600 MB.
    opening: parking_lot::Mutex<()>,
    obsolete: AtomicBool,
}

impl TableFile {
    pub fn new(
        fs: Arc<dyn Fs>,
        cache: Arc<BlockCache>,
        dir: &std::path::Path,
        number: u64,
        props: TableProps,
    ) -> Self {
        TableFile {
            number,
            props,
            path: dir.join(table_name(number)),
            fs,
            cache,
            reader: OnceLock::new(),
            opening: parking_lot::Mutex::new(()),
            obsolete: AtomicBool::new(false),
        }
    }

    pub fn reader(&self) -> Result<Arc<TableReader>> {
        if let Some(reader) = self.reader.get() {
            return Ok(reader.clone());
        }
        let _opening = self.opening.lock();
        if let Some(reader) = self.reader.get() {
            return Ok(reader.clone());
        }
        let reader = TableReader::open(self.fs.open_random(&self.path)?)?
            .with_cache(self.cache.clone(), self.number);
        Ok(self.reader.get_or_init(|| Arc::new(reader)).clone())
    }

    /// No longer in the live version: delete the file when the last reader
    /// still holding it lets go.
    pub fn mark_obsolete(&self) {
        self.obsolete.store(true, AtomicOrdering::Release);
    }

    /// Whether `cf ‖ user_key` falls between the table's first and last keys.
    pub fn may_hold(&self, cf_and_user_key: &[u8]) -> bool {
        key::cf_and_user_key(&self.props.smallest) <= cf_and_user_key
            && cf_and_user_key <= key::cf_and_user_key(&self.props.largest)
    }
}

impl Drop for TableFile {
    fn drop(&mut self) {
        if self.obsolete.load(AtomicOrdering::Acquire) {
            // Close this table's own handle first: Windows refuses to delete
            // a file that is open without FILE_SHARE_DELETE.
            drop(self.reader.take());
            self.fs.remove(&self.path).ok();
        }
    }
}

/// The table files and range tombstones of the database.
#[derive(Clone, Default)]
pub struct Version {
    /// Level 0 newest first (its files overlap); deeper levels sorted by key.
    pub levels: Vec<Vec<Arc<TableFile>>>,
    pub tombstones: Vec<RangeTombstone>,
}

impl Version {
    pub fn empty() -> Self {
        Version {
            levels: vec![Vec::new(); NUM_LEVELS],
            tombstones: Vec::new(),
        }
    }

    pub fn files(&self) -> impl Iterator<Item = &Arc<TableFile>> {
        self.levels.iter().flatten()
    }

    /// Sort level 0 newest first: a newer file shadows an older one.
    pub fn sort_level0(&mut self) {
        self.levels[0].sort_by_key(|f| std::cmp::Reverse(f.props.largest_seq));
    }
}

#[derive(Clone)]
pub struct SuperVersion {
    pub mem: Arc<Memtable>,
    /// Sealed memtables waiting to be flushed, newest first.
    pub imms: Vec<Arc<Memtable>>,
    pub version: Arc<Version>,
}

impl SuperVersion {
    /// The value of `user_key` at `snapshot`.
    pub fn get(&self, cf: u32, user_key: &[u8], snapshot: u64) -> Result<Option<Bytes>> {
        let Some(found) = self.newest(cf, user_key, snapshot)? else {
            return Ok(None);
        };
        if found.kind == Kind::Delete || self.tombstone_seq(cf, user_key, snapshot) > found.seq {
            return Ok(None);
        }
        Ok(Some(found.value))
    }

    /// The newest version at or below `snapshot`: sources are searched newest
    /// first, so the first hit wins.
    fn newest(&self, cf: u32, user_key: &[u8], snapshot: u64) -> Result<Option<Found>> {
        for mem in std::iter::once(&self.mem).chain(&self.imms) {
            if let Some(found) = mem.get(cf, user_key, snapshot) {
                return Ok(Some(found));
            }
        }
        let mut probe = Vec::with_capacity(4 + user_key.len());
        probe.extend_from_slice(&cf.to_be_bytes());
        probe.extend_from_slice(user_key);
        for file in &self.version.levels[0] {
            if file.may_hold(&probe) {
                if let Some(found) = file.reader()?.get(cf, user_key, snapshot)? {
                    return Ok(Some(found));
                }
            }
        }
        for level in &self.version.levels[1..] {
            // Sorted and disjoint: at most one file can hold the key.
            let at = level
                .partition_point(|f| key::cf_and_user_key(&f.props.largest) < probe.as_slice());
            if let Some(file) = level.get(at).filter(|f| f.may_hold(&probe)) {
                if let Some(found) = file.reader()?.get(cf, user_key, snapshot)? {
                    return Ok(Some(found));
                }
            }
        }
        Ok(None)
    }

    pub fn tombstone_seq(&self, cf: u32, user_key: &[u8], snapshot: u64) -> u64 {
        let mut seq = covering_seq(&self.version.tombstones, cf, user_key, snapshot);
        for mem in std::iter::once(&self.mem).chain(&self.imms) {
            seq = seq.max(mem.range_tombstone_seq(cf, user_key, snapshot));
        }
        seq
    }

    /// Every range tombstone of `cf` visible at `snapshot`.
    pub fn tombstones(&self, cf: u32, snapshot: u64) -> Vec<RangeTombstone> {
        let mut all: Vec<RangeTombstone> = self.version.tombstones.clone();
        for mem in std::iter::once(&self.mem).chain(&self.imms) {
            all.extend(mem.tombstones());
        }
        all.retain(|t| t.cf == cf && t.seq <= snapshot);
        all
    }

    /// One cursor per source that may hold keys of `cf`.
    pub fn iterators(&self, cf: u32) -> Result<Vec<Box<dyn InternalIterator>>> {
        let mut out: Vec<Box<dyn InternalIterator>> = Vec::new();
        for mem in std::iter::once(&self.mem).chain(&self.imms) {
            out.push(Box::new(MemIter::new(mem.clone())));
        }
        let holds_cf = |f: &TableFile| {
            let (lo, hi) = (key::parse(&f.props.smallest), key::parse(&f.props.largest));
            match (lo, hi) {
                (Some(lo), Some(hi)) => lo.cf <= cf && cf <= hi.cf,
                _ => true,
            }
        };
        for file in &self.version.levels[0] {
            if holds_cf(file) {
                out.push(Box::new(TableIter::new(file.reader()?)));
            }
        }
        // A deeper level is one sorted run: one cursor that opens only the
        // file it is in, however many files the level has.
        for level in &self.version.levels[1..] {
            let files: Vec<Arc<TableFile>> =
                level.iter().filter(|f| holds_cf(f)).cloned().collect();
            if !files.is_empty() {
                out.push(Box::new(LevelIter::new(files)));
            }
        }
        Ok(out)
    }
}

/// A cursor over a sorted run of disjoint tables (a level below 0).
pub struct LevelIter {
    files: Vec<Arc<TableFile>>,
    at: usize,
    current: Option<TableIter>,
    error: Option<Error>,
    fill_cache: bool,
}

impl LevelIter {
    pub fn new(files: Vec<Arc<TableFile>>) -> Self {
        LevelIter {
            files,
            at: 0,
            current: None,
            error: None,
            fill_cache: true,
        }
    }

    /// For a compaction: see [`TableIter::without_filling_cache`].
    pub fn without_filling_cache(mut self) -> Self {
        self.fill_cache = false;
        self
    }

    fn open(&mut self, at: usize) -> bool {
        self.at = at;
        self.current = None;
        match self.files[at].reader() {
            Ok(reader) => {
                let it = TableIter::new(reader);
                self.current = Some(if self.fill_cache {
                    it
                } else {
                    it.without_filling_cache()
                });
                true
            }
            Err(e) => {
                self.error = Some(e);
                false
            }
        }
    }

    fn current_valid(&self) -> bool {
        self.current.as_ref().is_some_and(|c| c.valid())
    }

    /// Move to the first entry of the following files until one has one.
    fn forward_to_valid(&mut self) {
        while !self.current_valid() && self.error.is_none() {
            if let Some(Err(e)) = self.current.as_ref().map(|c| c.status()) {
                self.error = Some(e);
                return;
            }
            if self.at + 1 >= self.files.len() {
                self.current = None;
                return;
            }
            if !self.open(self.at + 1) {
                return;
            }
            let first = self.files[self.at].props.smallest.clone();
            self.current.as_mut().expect("opened").seek_ge(&first);
        }
    }

    /// Move to the last entry of the preceding files until one has one.
    fn backward_to_valid(&mut self) {
        while !self.current_valid() && self.error.is_none() {
            if let Some(Err(e)) = self.current.as_ref().map(|c| c.status()) {
                self.error = Some(e);
                return;
            }
            if self.at == 0 {
                self.current = None;
                return;
            }
            if !self.open(self.at - 1) {
                return;
            }
            self.current.as_mut().expect("opened").seek_to_last();
        }
    }
}

impl InternalIterator for LevelIter {
    fn seek_ge(&mut self, target: &[u8]) {
        self.error = None;
        let at = self
            .files
            .partition_point(|f| key::compare(&f.props.largest, target) == Ordering::Less);
        if at >= self.files.len() {
            self.current = None;
            return;
        }
        if self.open(at) {
            self.current.as_mut().expect("opened").seek_ge(target);
            self.forward_to_valid();
        }
    }

    fn seek_le(&mut self, target: &[u8]) {
        self.error = None;
        let after = self
            .files
            .partition_point(|f| key::compare(&f.props.smallest, target) != Ordering::Greater);
        if after == 0 {
            self.current = None;
            return;
        }
        if self.open(after - 1) {
            self.current.as_mut().expect("opened").seek_le(target);
            self.backward_to_valid();
        }
    }

    fn seek_to_last(&mut self) {
        self.error = None;
        if self.files.is_empty() {
            self.current = None;
            return;
        }
        if self.open(self.files.len() - 1) {
            self.current.as_mut().expect("opened").seek_to_last();
            self.backward_to_valid();
        }
    }

    fn next(&mut self) {
        if let Some(c) = self.current.as_mut() {
            c.next();
        }
        self.forward_to_valid();
    }

    fn prev(&mut self) {
        if let Some(c) = self.current.as_mut() {
            c.prev();
        }
        self.backward_to_valid();
    }

    fn valid(&self) -> bool {
        self.error.is_none() && self.current_valid()
    }

    fn key(&self) -> &[u8] {
        self.current.as_ref().expect("valid").key()
    }

    fn value(&self) -> Bytes {
        self.current.as_ref().expect("valid").value()
    }

    fn status(&self) -> Result<()> {
        match &self.error {
            Some(e) => Err(e.clone()),
            None => self.current.as_ref().map_or(Ok(()), |c| c.status()),
        }
    }
}

/// Order two tables by their first key, for the sorted levels.
pub fn by_smallest(a: &Arc<TableFile>, b: &Arc<TableFile>) -> Ordering {
    key::compare(&a.props.smallest, &b.props.smallest)
}
