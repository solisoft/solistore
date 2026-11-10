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
use crate::sst::block::{shared_prefix, word_after, Block};
use crate::sst::table::{with_seek_key, BlockAt};
use crate::sst::{bloom, BlockCache, TableIter, TableProps, TableReader};

pub const NUM_LEVELS: usize = 7;

pub fn table_name(number: u64) -> String {
    format!("{number:06}.sst")
}

pub fn parse_table_name(name: &str) -> Option<u64> {
    name.strip_suffix(".sst")?.parse().ok()
}

/// Table files whose delete failed, to try again later. On Windows a file
/// another handle holds open cannot always be deleted; without a retry it
/// would stay until the next open removes orphans.
pub type Graveyard = Arc<parking_lot::Mutex<Vec<PathBuf>>>;

/// Delete what the graveyard holds; keep what still cannot be deleted.
pub fn retry_deletes(fs: &dyn Fs, graveyard: &Graveyard) {
    let mut pending = graveyard.lock();
    if pending.is_empty() {
        return;
    }
    pending.retain(|path| fs.remove(path).is_err() && fs.exists(path));
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
    graveyard: Option<Graveyard>,
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
            graveyard: None,
        }
    }

    /// Where a failed delete of this file goes to be retried.
    pub fn with_graveyard(mut self, graveyard: Graveyard) -> Self {
        self.graveyard = Some(graveyard);
        self
    }

    /// The table's reader, opened on first use. Lent: a point read holds
    /// the version naming this file for as long as it reads, and cloning the
    /// `Arc` would write its count, a line every thread reading the table
    /// contends on. A cursor that outlives the call clones it.
    pub fn reader(&self) -> Result<&Arc<TableReader>> {
        if let Some(reader) = self.reader.get() {
            return Ok(reader);
        }
        let _opening = self.opening.lock();
        if let Some(reader) = self.reader.get() {
            return Ok(reader);
        }
        let reader = TableReader::open(self.fs.open_random(&self.path)?)?
            .with_cache(self.cache.clone(), self.number);
        Ok(self.reader.get_or_init(|| Arc::new(reader)))
    }

    /// No longer in the live version: delete the file when the last reader
    /// still holding it lets go.
    pub fn mark_obsolete(&self) {
        self.obsolete.store(true, AtomicOrdering::Release);
    }

    /// The table's reader if it is open already: nothing is read.
    #[inline]
    pub fn opened(&self) -> Option<&Arc<TableReader>> {
        self.reader.get()
    }

    /// Whether `cf ‖ user_key` falls between the table's first and last keys.
    ///
    /// Slice comparisons, not `key::cmp_bytes`: bounds and probes share
    /// 20-odd bytes, which the library's vectorised memcmp compares in a
    /// quarter of the instructions.
    #[inline]
    pub fn may_hold(&self, cf_and_user_key: &[u8]) -> bool {
        key::cf_and_user_key(&self.props.smallest) <= cf_and_user_key
            && cf_and_user_key <= self.largest_id()
    }

    /// `cf ‖ user key` of the table's last key.
    #[inline]
    pub fn largest_id(&self) -> &[u8] {
        key::cf_and_user_key(&self.props.largest)
    }
}

impl Drop for TableFile {
    fn drop(&mut self) {
        if self.obsolete.load(AtomicOrdering::Acquire) {
            // Close this table's own handle first: Windows refuses to delete
            // a file that is open without FILE_SHARE_DELETE.
            drop(self.reader.take());
            if self.fs.remove(&self.path).is_err() && self.fs.exists(&self.path) {
                if let Some(graveyard) = &self.graveyard {
                    graveyard.lock().push(self.path.clone());
                }
            }
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
        if found.kind == Kind::Delete
            || self.has_tombstones() && self.tombstone_seq(cf, user_key, snapshot) > found.seq
        {
            return Ok(None);
        }
        Ok(Some(found.value))
    }

    /// Whether any range tombstone is in sight: the version's, or one in a
    /// memtable.
    #[inline]
    fn has_tombstones(&self) -> bool {
        !self.version.tombstones.is_empty()
            || std::iter::once(&self.mem)
                .chain(&self.imms)
                .any(|m| m.has_tombstones())
    }

    /// The newest version at or below `snapshot`: sources are searched newest
    /// first, so the first hit wins.
    fn newest(&self, cf: u32, user_key: &[u8], snapshot: u64) -> Result<Option<Found>> {
        // One key for every source, and one bloom hash for every table.
        let lookup = key::LookupKey::new(cf, user_key, snapshot);
        for mem in std::iter::once(&self.mem).chain(&self.imms) {
            if let Some(found) = mem.get_at(&lookup) {
                return Ok(Some(found));
            }
        }
        let probe = lookup.id();
        let levels = &self.version.levels;
        // The one file of each deeper level that may hold the key, found
        // before any table is probed: their filter lines, likely cache
        // misses each, are then fetched together with level 0's.
        let mut deeper: [Option<&Arc<TableFile>>; NUM_LEVELS] = [None; NUM_LEVELS];
        let mut any = false;
        for (slot, level) in deeper.iter_mut().zip(levels).skip(1) {
            *slot = file_for(level, probe);
            any |= slot.is_some();
        }
        if !any && !levels[0].iter().any(|f| f.may_hold(probe)) {
            return Ok(None);
        }
        let hash = bloom::hash(probe);
        for file in deeper.iter().flatten() {
            if let Some(reader) = file.opened() {
                reader.prefetch_filter(hash);
            }
        }
        for file in &levels[0] {
            if file.may_hold(probe) {
                if let Some(found) = file.reader()?.get_at(&lookup, hash)? {
                    return Ok(Some(found));
                }
            }
        }
        for file in deeper.iter().flatten() {
            if let Some(found) = file.reader()?.get_at(&lookup, hash)? {
                return Ok(Some(found));
            }
        }
        Ok(None)
    }

    /// The values of `keys` of `cf` at `snapshot`, handed to `emit` in the
    /// order of `keys` with their position, borrowed from the memtables and
    /// data blocks they were found in: [`crate::Db::multi_get_with`].
    ///
    /// One pass over the sources for all the keys, in key order: each
    /// memtable once, then each sorted run of tables (a level-0 file, or a
    /// deeper level) for the keys still missing, in stages that let their
    /// cache misses overlap (see [`SuperVersion::read_run`]). Keys that
    /// fall in one data block read it once.
    pub fn multi_get<K: AsRef<[u8]>>(
        &self,
        cf: u32,
        keys: &[K],
        snapshot: u64,
        mut emit: impl FnMut(usize, Option<ValueRef<'_>>),
    ) -> Result<()> {
        let mut probes: Vec<Probe<'_, '_>> = keys
            .iter()
            .map(|k| Probe {
                lookup: key::LookupKey::new(cf, k.as_ref(), snapshot),
                hash: 0,
                found: None,
            })
            .collect();
        let order = key_order(&probes);
        let mut missing = probes.len();
        for mem in std::iter::once(&self.mem).chain(&self.imms) {
            if missing == 0 || mem.has_no_entries() {
                continue;
            }
            for &i in &order {
                let p = &mut probes[i as usize];
                if p.found.is_none() {
                    if let Some((seq, kind, value)) = mem.get_ref(&p.lookup) {
                        p.found = Some(Hit::Mem { seq, kind, value });
                        missing -= 1;
                    }
                }
            }
        }
        // The data blocks the keys are found in: lent from their tables'
        // slots under this guard where the cache holds them, which lasts
        // until the values have been handed to `emit`.
        let guard = crossbeam_epoch::pin();
        let mut blocks: Vec<Held<'_>> = Vec::new();
        if missing > 0 {
            for p in probes.iter_mut().filter(|p| p.found.is_none()) {
                p.hash = bloom::hash(p.lookup.id());
            }
            with_seek_key(|key| -> Result<()> {
                let mut todo = Vec::new();
                // Level 0's files overlap, newest first: each is a run.
                let runs = self.version.levels[0]
                    .iter()
                    .map(std::slice::from_ref)
                    .chain(self.version.levels[1..].iter().map(Vec::as_slice));
                for run in runs {
                    if missing == 0 {
                        break;
                    }
                    if !run.is_empty() {
                        missing -= Self::read_run(
                            run,
                            &order,
                            &mut probes,
                            &mut blocks,
                            &mut todo,
                            key,
                            &guard,
                        )?;
                    }
                }
                Ok(())
            })?;
        }
        let tombstones = self.has_tombstones();
        for (i, p) in probes.iter().enumerate() {
            let (seq, value) = match p.found {
                Some(Hit::Mem {
                    seq,
                    kind: Kind::Put,
                    value,
                }) => (seq, ValueRef::Mem(value)),
                Some(Hit::Table {
                    seq,
                    kind: Kind::Put,
                    block,
                    value,
                }) => (
                    seq,
                    ValueRef::Block(blocks[block].block(), value.0, value.1),
                ),
                _ => {
                    emit(i, None);
                    continue;
                }
            };
            let deleted = tombstones && self.tombstone_seq(cf, p.lookup.user_key, snapshot) > seq;
            emit(i, (!deleted).then_some(value));
        }
        Ok(())
    }

    /// Look the keys still missing up in one sorted run of tables, in key
    /// order, a stage at a time over all of them: each key's file and its
    /// filter line fetched, then the filters read; each key's data block
    /// found from the fences and taken from the cache (which starts
    /// fetching its entries), a block shared by consecutive keys once; then
    /// each block searched. A single get waits for one cache miss after
    /// another; here those of different keys are in flight together.
    /// Returns how many keys it found.
    fn read_run<'r, 'g>(
        run: &'r [Arc<TableFile>],
        order: &[u32],
        probes: &mut [Probe<'_, '_>],
        blocks: &mut Vec<Held<'g>>,
        todo: &mut Vec<Todo<'r>>,
        key: &mut Vec<u8>,
        guard: &'g crossbeam_epoch::Guard,
    ) -> Result<usize> {
        todo.clear();
        let (mut at, mut reader) = (0, None);
        for &i in order {
            let p = &probes[i as usize];
            if p.found.is_some() {
                continue;
            }
            let id = p.lookup.id();
            while at < run.len() && run[at].largest_id() < id {
                at += 1;
                reader = None;
            }
            if at == run.len() {
                break;
            }
            if key::cf_and_user_key(&run[at].props.smallest) <= id {
                let r = match reader {
                    Some(r) => r,
                    None => &**run[at].reader()?,
                };
                reader = Some(r);
                r.prefetch_filter(p.hash);
                todo.push(Todo {
                    probe: i,
                    reader: r,
                    block: u32::MAX,
                    at: None,
                });
            }
        }
        todo.retain(|t| t.reader.may_contain(probes[t.probe as usize].hash));
        // Each key's block, read once for consecutive keys in it.
        let mut last: Option<(*const TableReader, u64, u32)> = None;
        for t in todo.iter_mut() {
            let Some(place) = t
                .reader
                .block_for(probes[t.probe as usize].lookup.internal(), key)?
            else {
                continue;
            };
            t.at = Some(place);
            let table: *const TableReader = t.reader;
            t.block = match last {
                Some((last_table, offset, slot))
                    if std::ptr::eq(last_table, table) && offset == place.handle.offset =>
                {
                    slot
                }
                _ => {
                    blocks.push(Held::take(t.reader, &place, guard)?);
                    let slot = (blocks.len() - 1) as u32;
                    last = Some((table, place.handle.offset, slot));
                    slot
                }
            };
        }
        let mut found = 0;
        for t in todo.iter() {
            let Some(place) = t.at else {
                continue;
            };
            let p = &mut probes[t.probe as usize];
            let mut block = t.block as usize;
            let mut located = t.reader.version_in(blocks[block].block(), &p.lookup, key)?;
            if located.is_none() {
                // Past every entry of its block: the next one's first
                // entry decides.
                if let Some(next) = t.reader.next_block(p.lookup.internal(), place.number)? {
                    blocks.push(Held::take(t.reader, &next, guard)?);
                    block = blocks.len() - 1;
                    located = t.reader.version_in(blocks[block].block(), &p.lookup, key)?;
                }
            }
            if let Some(Some(hit)) = located {
                p.found = Some(Hit::Table {
                    seq: hit.seq,
                    kind: hit.kind,
                    block,
                    value: hit.value,
                });
                found += 1;
            }
        }
        Ok(found)
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

    /// User keys that cut `[lo, hi)` of `cf` into about `n` parts of equal
    /// size: see [`crate::Db::split_points`].
    pub fn split_points(&self, cf: u32, lo: &[u8], hi: &[u8], n: usize) -> Result<Vec<Vec<u8>>> {
        if n <= 1 || lo >= hi {
            return Ok(Vec::new());
        }
        let prefix = cf.to_be_bytes();
        let (lo, hi) = ([&prefix[..], lo].concat(), [&prefix[..], hi].concat());
        // Every source's samples, then in key order across them: the bytes
        // below a key add up whatever memtable or file holds them.
        let mut samples: Vec<(&[u8], u64)> = Vec::new();
        for mem in std::iter::once(&self.mem).chain(&self.imms) {
            mem.size_samples(&lo, &hi, &mut samples);
        }
        for file in self.version.files() {
            let smallest = key::cf_and_user_key(&file.props.smallest);
            let largest = key::cf_and_user_key(&file.props.largest);
            if largest <= lo.as_slice() || smallest >= hi.as_slice() {
                continue;
            }
            file.reader()?.size_samples(&lo, &hi, &mut samples)?;
        }
        samples.sort_unstable_by(|a, b| key::cmp_bytes(a.0, b.0));
        let total: u128 = samples.iter().map(|&(_, bytes)| bytes as u128).sum();
        let (n, mut cuts) = (n as u128, Vec::new());
        let (mut below, mut next) = (0u128, 1u128);
        for (id, bytes) in samples {
            below += bytes as u128;
            // Past the next cut's share: cut here, once however many shares
            // this sample crossed.
            if next < n && below * n >= total * next {
                // Strictly inside `(lo, hi)`, so a whole column family
                // followed by a user key.
                let user_key = &id[key::CF_LEN..];
                if cuts
                    .last()
                    .is_none_or(|last: &Vec<u8>| last.as_slice() < user_key)
                {
                    cuts.push(user_key.to_vec());
                }
                while next < n && below * n >= total * next {
                    next += 1;
                }
            }
        }
        Ok(cuts)
    }

    /// One cursor per source that may hold keys of `cf`.
    pub fn iterators(&self, cf: u32) -> Result<Vec<Source>> {
        let mut out: Vec<Source> = Vec::new();
        for mem in std::iter::once(&self.mem).chain(&self.imms) {
            out.push(Source::Mem(MemIter::new(mem.clone())));
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
                out.push(Source::Table(TableIter::new(file.reader()?.clone())));
            }
        }
        // A deeper level is one sorted run: one cursor that opens only the
        // file it is in, however many files the level has.
        for level in &self.version.levels[1..] {
            let files: Vec<Arc<TableFile>> =
                level.iter().filter(|f| holds_cf(f)).cloned().collect();
            if !files.is_empty() {
                out.push(Source::Level(LevelIter::new(files)));
            }
        }
        Ok(out)
    }
}

/// The file of a sorted level whose key range holds `id` (`cf ‖ user
/// key`), if any.
#[inline]
fn file_for<'a>(level: &'a [Arc<TableFile>], id: &[u8]) -> Option<&'a Arc<TableFile>> {
    let at = level.partition_point(|f| f.largest_id() < id);
    // At or before its last key: only the first is left to check.
    level
        .get(at)
        .filter(|f| key::cf_and_user_key(&f.props.smallest) <= id)
}

/// A data block a batched read holds: lent from its table's slot for the
/// length of the read's guard, or a handle of its own when the cache had
/// to be asked (or filled).
enum Held<'g> {
    Lent(&'g Block),
    Owned(Block),
}

impl<'g> Held<'g> {
    fn take(reader: &TableReader, at: &BlockAt, guard: &'g crossbeam_epoch::Guard) -> Result<Self> {
        match reader.lent_block(at, guard) {
            Some(block) => Ok(Held::Lent(block)),
            None => reader.data_block_for(at, true).map(Held::Owned),
        }
    }

    #[inline]
    fn block(&self) -> &Block {
        match self {
            Held::Lent(block) => block,
            Held::Owned(block) => block,
        }
    }
}

/// A value a batched read found, borrowed from where it lies.
pub enum ValueRef<'a> {
    /// In a memtable's arena.
    Mem(&'a [u8]),
    /// In a data block, at this range.
    Block(&'a Block, usize, usize),
}

impl ValueRef<'_> {
    #[inline]
    pub fn bytes(&self) -> &[u8] {
        match self {
            ValueRef::Mem(value) => value,
            ValueRef::Block(block, start, end) => &block.bytes()[*start..*end],
        }
    }

    /// The value as a `get` hands it out: copied out of a memtable, and out
    /// of a block unless it is most of it (see `memtable::detach`).
    pub fn to_bytes(&self) -> Bytes {
        match self {
            ValueRef::Mem(value) => Bytes::copy_from_slice(value),
            ValueRef::Block(block, start, end) => {
                crate::memtable::detach_from(&block.bytes()[*start..*end], block.size(), || {
                    block.slice(*start..*end)
                })
            }
        }
    }
}

/// A key a batched read looks for, and what it found.
struct Probe<'k, 'v> {
    lookup: key::LookupKey<'k>,
    /// Its bloom hash, once a table is to be probed.
    hash: u64,
    found: Option<Hit<'v>>,
}

/// The newest version of a key at the snapshot, where a batched read found it.
#[derive(Clone, Copy)]
enum Hit<'v> {
    Mem {
        seq: u64,
        kind: Kind,
        value: &'v [u8],
    },
    /// In the read's block `block`, at `value`.
    Table {
        seq: u64,
        kind: Kind,
        block: usize,
        value: (usize, usize),
    },
}

/// A key a batched read is to look up in one table of a run.
struct Todo<'r> {
    probe: u32,
    reader: &'r TableReader,
    /// Its data block among the read's blocks, once taken.
    block: u32,
    at: Option<BlockAt>,
}

/// The positions of `probes` in key order. Sorted by the eight bytes of
/// their identities after the prefix all of them share, whole identities
/// only among equal words: a batch's keys share 20-odd bytes (column
/// family, keyspace, `doc:`), and comparing those at every step of the sort
/// cost more than the rest of the batch. Keys already in order are not
/// sorted.
fn key_order(probes: &[Probe<'_, '_>]) -> Vec<u32> {
    let mut order: Vec<u32> = (0..probes.len() as u32).collect();
    let Some(first) = probes.first() else {
        return order;
    };
    let first = first.lookup.id();
    let p = probes
        .iter()
        .map(|q| shared_prefix(first, q.lookup.id()))
        .min()
        .unwrap_or(0);
    let words: Vec<u64> = probes
        .iter()
        .map(|q| word_after(q.lookup.id(), p))
        .collect();
    let id = |i: u32| probes[i as usize].lookup.id();
    let before = |a: u32, b: u32| {
        words[a as usize]
            .cmp(&words[b as usize])
            .then_with(|| id(a).cmp(id(b)))
    };
    if !order.is_sorted_by(|&a, &b| before(a, b).is_le()) {
        order.sort_unstable_by(|&a, &b| before(a, b));
    }
    order
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
                let it = TableIter::new(reader.clone());
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

    #[inline]
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

    #[inline]
    fn advance(&mut self) -> bool {
        if let Some(c) = self.current.as_mut() {
            if c.advance() {
                return true;
            }
        }
        self.forward_to_valid();
        self.valid()
    }

    fn prev(&mut self) {
        if let Some(c) = self.current.as_mut() {
            c.prev();
        }
        self.backward_to_valid();
    }

    #[inline]
    fn valid(&self) -> bool {
        self.error.is_none() && self.current_valid()
    }

    #[inline]
    fn key(&self) -> &[u8] {
        self.current.as_ref().expect("valid").key()
    }

    fn value(&self) -> Bytes {
        self.current.as_ref().expect("valid").value()
    }

    #[inline]
    fn value_slice(&self) -> &[u8] {
        self.current.as_ref().expect("valid").value_slice()
    }

    fn status(&self) -> Result<()> {
        match &self.error {
            Some(e) => Err(e.clone()),
            None => self.current.as_ref().map_or(Ok(()), |c| c.status()),
        }
    }
}

/// A cursor of any kind the merge reads, dispatched without a virtual call
/// so that each step can be inlined into the merge.
pub enum Source {
    Mem(MemIter),
    Table(TableIter),
    Level(LevelIter),
}

macro_rules! each_source {
    ($self:expr, $it:ident => $body:expr) => {
        match $self {
            Source::Mem($it) => $body,
            Source::Table($it) => $body,
            Source::Level($it) => $body,
        }
    };
}

impl InternalIterator for Source {
    fn seek_ge(&mut self, target: &[u8]) {
        each_source!(self, it => it.seek_ge(target))
    }

    fn seek_le(&mut self, target: &[u8]) {
        each_source!(self, it => it.seek_le(target))
    }

    fn seek_to_last(&mut self) {
        each_source!(self, it => it.seek_to_last())
    }

    #[inline]
    fn next(&mut self) {
        each_source!(self, it => it.next())
    }

    #[inline]
    fn advance(&mut self) -> bool {
        each_source!(self, it => it.advance())
    }

    fn prev(&mut self) {
        each_source!(self, it => it.prev())
    }

    #[inline]
    fn valid(&self) -> bool {
        each_source!(self, it => it.valid())
    }

    #[inline]
    fn key(&self) -> &[u8] {
        each_source!(self, it => it.key())
    }

    fn value(&self) -> Bytes {
        each_source!(self, it => it.value())
    }

    #[inline]
    fn value_slice(&self) -> &[u8] {
        each_source!(self, it => it.value_slice())
    }

    fn status(&self) -> Result<()> {
        each_source!(self, it => it.status())
    }
}

/// Order two tables by their first key, for the sorted levels.
pub fn by_smallest(a: &Arc<TableFile>, b: &Arc<TableFile>) -> Ordering {
    key::compare(&a.props.smallest, &b.props.smallest)
}
