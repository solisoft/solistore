//! The database: opening and recovery, column families, the commit path,
//! memtable switches and flushes, and point reads.
//!
//! # Commit path
//!
//! Writers queue up. The writer at the head of the queue becomes the leader,
//! takes every batch waiting behind it (up to [`MAX_GROUP_BYTES`]), gives each
//! a contiguous run of sequence numbers, writes all of them to the WAL with
//! one `write` and makes them durable with one `fdatasync`, then applies them
//! to the memtable and publishes the group's last sequence number. Writers
//! arriving meanwhile form the next group. N concurrent commits therefore
//! cost one fsync, not N.
//!
//! # Visibility
//!
//! Readers take the published sequence number as their snapshot and ignore
//! every entry above it. The leader publishes only after its whole group is
//! in the memtable, and there is one leader at a time, so a reader sees a
//! batch entirely or not at all.
//!
//! # Memtables and flushes
//!
//! When the memtable reaches `write_buffer_size`, the leader seals it, starts
//! a new WAL and a new memtable, and wakes the flush thread, which writes the
//! sealed memtable as level-0 tables, records them in the MANIFEST and
//! deletes the WALs nothing live is left in. If flushes fall
//! `max_immutable_memtables` behind, writers wait.

use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;
use bytes::Bytes;
use parking_lot::{Condvar, Mutex, RwLock};

use crate::batch::{self, BatchOp, WriteBatch};
use crate::compaction;
use crate::error::{Error, Result};
use crate::flush;
use crate::fs::{Fs, StdFs, WritableFile};
use crate::iter::{DbIterator, IteratorMode, ReadOptions};
use crate::key::{Kind, MAX_SEQ};
use crate::log::{encode_record_parts, read_records};
use crate::manifest::{self, file_no_u32, Edit, ManifestState, ManifestWriter, DEFAULT_CF};
use crate::memtable::Memtable;
use crate::sst::BlockCache;
use crate::version::{by_smallest, table_name, SuperVersion, TableFile, Version, NUM_LEVELS};

/// The most a commit group writes to the WAL at once. A leader stops taking
/// batches past this, so one huge group cannot hold every writer up.
pub const MAX_GROUP_BYTES: usize = 4 << 20;

/// A MANIFEST past this size is replaced by a snapshot of the state.
const MANIFEST_ROLL_BYTES: u64 = 64 << 20;

/// When a commit is acknowledged relative to the disk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Durability {
    /// Every commit is fsynced before it returns: an acknowledged write
    /// survives power loss. Concurrent commits share one fsync.
    Full,
    /// Commits return once written to the OS; a background thread fsyncs
    /// every `sync_interval`, and a write with [`WriteOptions::sync`] waits
    /// for its own fsync. A crash of the process loses nothing; power loss
    /// can lose up to `sync_interval` of acknowledged writes, never a write
    /// without also losing every later one.
    Eventual { sync_interval: Duration },
}

#[derive(Clone)]
pub struct Options {
    pub create_if_missing: bool,
    pub error_if_exists: bool,
    pub durability: Durability,
    pub fs: Arc<dyn Fs>,
    /// Memtable size at which it is sealed and flushed.
    pub write_buffer_size: usize,
    /// Sealed memtables allowed to wait for a flush before writers stall.
    pub max_immutable_memtables: usize,
    /// Size at which a flush or compaction starts a new table file.
    pub target_file_size: u64,
    /// Level-0 files that start a compaction into level 1.
    pub level0_compaction_trigger: usize,
    /// Level-0 files at which each commit group is delayed by a millisecond.
    pub level0_slowdown_trigger: usize,
    /// Level-0 files at which writers wait for compaction.
    pub level0_stop_trigger: usize,
    /// Target size of level 1; each deeper level is `level_multiplier` times larger.
    pub max_bytes_for_level_base: u64,
    pub level_multiplier: u64,
    /// Ranges a compaction is split into and merged in parallel.
    pub max_subcompactions: usize,
    /// Compactions that may run at once, on disjoint files.
    pub max_background_compactions: usize,
    /// Bytes of decoded data blocks kept in memory, shared by every table.
    pub block_cache_size: u64,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            create_if_missing: true,
            error_if_exists: false,
            durability: Durability::Full,
            fs: Arc::new(StdFs),
            write_buffer_size: 64 << 20,
            // Three memtables at most, as SoliDB's RocksDB profile
            // (max_write_buffer_number = 3).
            max_immutable_memtables: 2,
            target_file_size: 64 << 20,
            level0_compaction_trigger: 4,
            level0_slowdown_trigger: 20,
            level0_stop_trigger: 36,
            max_bytes_for_level_base: 512 << 20,
            level_multiplier: 10,
            max_subcompactions: 4,
            max_background_compactions: 3,
            block_cache_size: 512 << 20,
        }
    }
}

impl fmt::Debug for Options {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Options")
            .field("create_if_missing", &self.create_if_missing)
            .field("error_if_exists", &self.error_if_exists)
            .field("durability", &self.durability)
            .field("fs", &self.fs)
            .field("write_buffer_size", &self.write_buffer_size)
            .field("max_immutable_memtables", &self.max_immutable_memtables)
            .field("target_file_size", &self.target_file_size)
            .field("level0_compaction_trigger", &self.level0_compaction_trigger)
            .field("level0_slowdown_trigger", &self.level0_slowdown_trigger)
            .field("level0_stop_trigger", &self.level0_stop_trigger)
            .field("max_bytes_for_level_base", &self.max_bytes_for_level_base)
            .field("level_multiplier", &self.level_multiplier)
            .field("max_subcompactions", &self.max_subcompactions)
            .field(
                "max_background_compactions",
                &self.max_background_compactions,
            )
            .field("block_cache_size", &self.block_cache_size)
            .finish()
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct WriteOptions {
    /// Wait for this write's fsync even in [`Durability::Eventual`]. Always
    /// the case in [`Durability::Full`].
    pub sync: bool,
}

/// Counters since the database was opened: where writers waited, and how
/// much work the background did. A snapshot; see [`Db::stats`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Stats {
    /// Commit groups and the batches they carried.
    pub commit_groups: u64,
    pub committed_batches: u64,
    pub wal_fsyncs: u64,
    /// Groups delayed 1 ms because level 0 passed the slowdown trigger.
    pub l0_slowdowns: u64,
    /// Microseconds writers were stopped by level 0 at the stop trigger.
    pub l0_stop_micros: u64,
    /// Microseconds writers waited for a flush because too many sealed
    /// memtables were queued.
    pub memtable_wait_micros: u64,
    pub flushes: u64,
    pub flushed_bytes: u64,
    pub compactions: u64,
    pub compaction_read_bytes: u64,
    pub compaction_written_bytes: u64,
    pub compaction_micros: u64,
}

#[derive(Default)]
struct Counters {
    commit_groups: AtomicU64,
    committed_batches: AtomicU64,
    wal_fsyncs: AtomicU64,
    l0_slowdowns: AtomicU64,
    l0_stop_micros: AtomicU64,
    memtable_wait_micros: AtomicU64,
    flushes: AtomicU64,
    flushed_bytes: AtomicU64,
    compactions: AtomicU64,
    compaction_read_bytes: AtomicU64,
    compaction_written_bytes: AtomicU64,
    compaction_micros: AtomicU64,
}

fn add(counter: &AtomicU64, n: u64) {
    counter.fetch_add(n, Ordering::Relaxed);
}

fn micros_since(start: Instant) -> u64 {
    start.elapsed().as_micros() as u64
}

/// What opening the database found in its logs.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RecoveryReport {
    pub wal_files: usize,
    pub batches: u64,
    /// Bytes after the last intact record of a WAL. Non-zero after a crash
    /// that cut a write short: none of those batches had been acknowledged
    /// in `Full` mode.
    pub discarded_bytes: u64,
    /// Table files no MANIFEST named (a flush cut short), deleted.
    pub orphan_tables: usize,
}

// ---------------------------------------------------------------- column families

/// A handle on a column family. Cheap to clone. Every handle on a column
/// family that has been dropped reports [`Error::ColumnFamilyDropped`], even
/// if one of the same name has been created since.
#[derive(Clone)]
pub struct ColumnFamily {
    inner: Arc<CfInner>,
}

struct CfInner {
    id: u32,
    name: String,
    dropped: AtomicBool,
}

impl ColumnFamily {
    fn new(id: u32, name: &str) -> Self {
        ColumnFamily {
            inner: Arc::new(CfInner {
                id,
                name: name.to_string(),
                dropped: AtomicBool::new(false),
            }),
        }
    }

    #[cfg(test)]
    pub(crate) fn for_tests(id: u32, name: &str) -> Self {
        Self::new(id, name)
    }

    pub fn name(&self) -> &str {
        &self.inner.name
    }

    pub(crate) fn id(&self) -> u32 {
        self.inner.id
    }

    fn check(&self) -> Result<()> {
        if self.inner.dropped.load(Ordering::Acquire) {
            Err(Error::ColumnFamilyDropped(self.inner.name.clone()))
        } else {
            Ok(())
        }
    }
}

impl fmt::Debug for ColumnFamily {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ColumnFamily({:?}, id {})",
            self.inner.name, self.inner.id
        )
    }
}

// ---------------------------------------------------------------- the database

/// An open database. Cheap to clone; closed when the last clone drops.
#[derive(Clone)]
pub struct Db {
    handle: Arc<Handle>,
}

/// Owns the background threads: dropping the last `Db` stops and joins them.
struct Handle {
    inner: Arc<Inner>,
    threads: Mutex<Vec<JoinHandle<()>>>,
}

struct Inner {
    dir: PathBuf,
    fs: Arc<dyn Fs>,
    durability: Durability,
    write_buffer_size: usize,
    max_imms: usize,
    target_file_size: u64,
    block_cache: Arc<BlockCache>,
    compaction: compaction::Config,
    l0_slowdown: usize,
    l0_stop: usize,
    /// Per level, the largest key last compacted out of it (round-robin).
    cursors: Mutex<Vec<Vec<u8>>>,
    cfs: RwLock<HashMap<String, ColumnFamily>>,
    structure: Mutex<Structure>,
    sv: ArcSwap<SuperVersion>,
    /// Serialises building and installing a new super version.
    sv_lock: Mutex<()>,
    /// Table files some compaction is working on; others leave them alone.
    compacting: Mutex<HashSet<u64>>,
    /// The highest sequence number a reader may see.
    visible_seq: AtomicU64,
    commit: Mutex<CommitState>,
    queue: Mutex<Queue>,
    queue_cv: Condvar,
    /// Mirrors of the queue's state, readable while spinning.
    queued: AtomicUsize,
    leader_busy: AtomicBool,
    /// Members of the last group, to gather as many for the next fsync.
    last_group: AtomicUsize,
    /// Wakes writers waiting for `visible_seq` to reach their group.
    publish: Mutex<()>,
    publish_cv: Condvar,
    /// The WAL being written, for the eventual-mode syncer.
    current_wal: Mutex<Arc<WalFile>>,
    /// A write, fsync or flush failed; see [`Error::Poisoned`].
    poisoned: AtomicBool,
    bg: Mutex<Background>,
    bg_cv: Condvar,
    recovery: RecoveryReport,
    counters: Counters,
    _lock: Box<dyn Send + Sync>,
}

#[derive(Default)]
struct Background {
    shutdown: bool,
    flush_error: Option<Error>,
}

/// The MANIFEST and the state it describes, changed together.
struct Structure {
    writer: ManifestWriter,
    state: ManifestState,
}

struct WalFile {
    file: Arc<dyn WritableFile>,
    number: u64,
    /// Written since its last fsync.
    dirty: AtomicBool,
}

/// The leader's working state. Only the current leader locks it (and a
/// forced memtable switch).
struct CommitState {
    wal: Arc<WalFile>,
    /// The last sequence number handed out. Readers see up to
    /// `visible_seq`, which catches up as groups finish their inserts.
    last_seq: u64,
    buf: Vec<u8>,
}

struct Pending {
    batch: WriteBatch,
    sync: bool,
    /// Set by the leader that wrote this batch to the WAL, under the queue lock.
    slot: Mutex<Option<Slot>>,
    /// `slot` is set: lets a waiting writer spin without the queue lock.
    ready: AtomicBool,
}

/// What a writer is told by its group's leader.
enum Slot {
    /// In the WAL: insert into `group.mem` from this sequence number.
    Insert(Arc<Group>, u64),
    Failed(Error),
}

/// A commit group after its WAL write: its members insert in parallel, and
/// the last one to finish publishes it, after the group before it.
struct Group {
    mem: Arc<Memtable>,
    first: u64,
    last: u64,
    remaining: AtomicUsize,
}

#[derive(Default)]
struct Queue {
    waiting: VecDeque<Arc<Pending>>,
    leader_active: bool,
}

/// How long a writer spins, yielding, before it sleeps on a condition
/// variable. Waking a sleeping thread costs tens of microseconds — as much
/// as the work it waits for — and most waits here end sooner than this.
const SPIN: Duration = Duration::from_micros(50);

/// Wait for `done`, at most [`SPIN`], without sleeping: a few busy spins,
/// then yielding the core on every check, so a waiting writer hands its CPU
/// to the flush and compaction threads instead of burning it. Returns
/// whether `done` became true.
fn spin_until(done: impl Fn() -> bool) -> bool {
    for _ in 0..64 {
        if done() {
            return true;
        }
        std::hint::spin_loop();
    }
    let start = Instant::now();
    while start.elapsed() < SPIN {
        if done() {
            return true;
        }
        std::thread::yield_now();
    }
    done()
}

pub(crate) fn wal_name(no: u64) -> String {
    format!("{no:06}.wal")
}

fn parse_wal_name(name: &str) -> Option<u64> {
    name.strip_suffix(".wal")?.parse().ok()
}

impl Db {
    pub fn open(path: impl AsRef<Path>, opts: Options) -> Result<Db> {
        let dir = path.as_ref().to_path_buf();
        let fs = opts.fs.clone();
        if opts.create_if_missing {
            fs.create_dir_all(&dir)?;
        }
        let lock = fs.lock(&dir.join("LOCK"))?;

        let mut state = match manifest::current(fs.as_ref(), &dir)? {
            Some(_) if opts.error_if_exists => {
                return Err(Error::InvalidArgument(format!(
                    "{} already exists",
                    dir.display()
                )))
            }
            Some(no) => manifest::read(fs.as_ref(), &dir, no)?,
            None if opts.create_if_missing => ManifestState::new_database(),
            None => {
                return Err(Error::InvalidArgument(format!(
                    "{} does not exist",
                    dir.display()
                )))
            }
        };
        let mut recovery = RecoveryReport::default();

        // Tables a crashed flush wrote but never recorded.
        let live: std::collections::HashSet<u64> = state
            .levels
            .iter()
            .flat_map(|l| l.keys().copied())
            .collect();
        let (highest, orphans) = flush::remove_orphans(fs.as_ref(), &dir, &|n| live.contains(&n))?;
        recovery.orphan_tables = orphans;
        state.next_file = state.next_file.max(highest + 1);

        // WALs below the log number were flushed; the rest may hold data.
        let mut wals: Vec<u64> = fs
            .list(&dir)?
            .iter()
            .filter_map(|name| parse_wal_name(name))
            .collect();
        wals.sort_unstable();
        for no in wals.iter().filter(|no| **no < state.log_number) {
            fs.remove(&dir.join(wal_name(*no))).ok();
        }
        wals.retain(|no| *no >= state.log_number);
        if let Some(last) = wals.last() {
            state.next_file = state.next_file.max(last + 1);
        }

        // A new WAL and a new MANIFEST for this run.
        let manifest_no = state.allocate_file();
        let wal_no = state.allocate_file();

        // Replay, oldest first, into the memtable this run starts with. With
        // everything flushed there is nothing to replay, and the sequence
        // resumes from what the MANIFEST and the tables record.
        let mem = Memtable::new(wals.first().copied().unwrap_or(wal_no));
        let mut last_seq = state.last_seq;
        for props in state.levels.iter().flat_map(|l| l.values()) {
            last_seq = last_seq.max(props.largest_seq);
        }
        for t in &state.tombstones {
            last_seq = last_seq.max(t.seq);
        }
        for &no in &wals {
            let data = fs.read(&dir.join(wal_name(no)))?;
            let log = read_records(&data, file_no_u32(no)?);
            recovery.wal_files += 1;
            recovery.discarded_bytes += log.discarded as u64;
            for record in log.records {
                let decoded = batch::decode(record)?;
                recovery.batches += 1;
                let count = decoded.ops.len() as u64;
                last_seq = last_seq.max(decoded.first_seq + count.saturating_sub(1));
                apply(&mem, decoded.ops, decoded.first_seq, |cf| {
                    state.cfs.contains_key(&cf)
                });
            }
        }

        // The MANIFEST's install syncs the directory, which makes the new
        // WAL's name durable too.
        let wal_file = fs.create(&dir.join(wal_name(wal_no)))?;
        let writer = ManifestWriter::install(fs.as_ref(), &dir, manifest_no, &state)?;
        for name in fs.list(&dir)? {
            if manifest::parse_manifest_name(&name).is_some_and(|no| no != manifest_no) {
                fs.remove(&dir.join(&name)).ok();
            }
        }

        let block_cache = Arc::new(BlockCache::new(opts.block_cache_size));
        let mut version = Version::empty();
        for (level, files) in state.levels.iter().enumerate() {
            for (number, props) in files {
                version.levels[level].push(Arc::new(TableFile::new(
                    fs.clone(),
                    block_cache.clone(),
                    &dir,
                    *number,
                    props.clone(),
                )));
            }
        }
        version.sort_level0();
        for level in &mut version.levels[1..] {
            level.sort_by(by_smallest);
        }
        version.tombstones = state.tombstones.clone();

        let cfs = state
            .cfs
            .iter()
            .map(|(id, name)| (name.clone(), ColumnFamily::new(*id, name)))
            .collect();
        let wal = Arc::new(WalFile {
            file: wal_file,
            number: wal_no,
            dirty: AtomicBool::new(false),
        });
        let inner = Arc::new(Inner {
            dir,
            fs,
            durability: opts.durability,
            write_buffer_size: opts.write_buffer_size.max(1),
            max_imms: opts.max_immutable_memtables.max(1),
            target_file_size: opts.target_file_size.max(1),
            block_cache,
            compaction: compaction::Config {
                l0_trigger: opts.level0_compaction_trigger.max(1),
                level_base_bytes: opts.max_bytes_for_level_base.max(1),
                level_multiplier: opts.level_multiplier.max(2),
                target_file_size: opts.target_file_size.max(1),
                subcompactions: opts.max_subcompactions.max(1),
            },
            l0_slowdown: opts.level0_slowdown_trigger.max(1),
            l0_stop: opts
                .level0_stop_trigger
                .max(opts.level0_slowdown_trigger)
                .max(1),
            cursors: Mutex::new(vec![Vec::new(); NUM_LEVELS]),
            cfs: RwLock::new(cfs),
            structure: Mutex::new(Structure { writer, state }),
            sv: ArcSwap::from_pointee(SuperVersion {
                mem: Arc::new(mem),
                imms: Vec::new(),
                version: Arc::new(version),
            }),
            sv_lock: Mutex::new(()),
            compacting: Mutex::new(HashSet::new()),
            visible_seq: AtomicU64::new(last_seq),
            commit: Mutex::new(CommitState {
                wal: wal.clone(),
                last_seq,
                buf: Vec::new(),
            }),
            queue: Mutex::new(Queue::default()),
            queue_cv: Condvar::new(),
            queued: AtomicUsize::new(0),
            leader_busy: AtomicBool::new(false),
            last_group: AtomicUsize::new(1),
            publish: Mutex::new(()),
            publish_cv: Condvar::new(),
            current_wal: Mutex::new(wal),
            poisoned: AtomicBool::new(false),
            bg: Mutex::new(Background::default()),
            bg_cv: Condvar::new(),
            recovery,
            counters: Counters::default(),
            _lock: lock,
        });

        let mut threads = Vec::new();
        let flusher = inner.clone();
        threads.push(
            std::thread::Builder::new()
                .name("solistore-flush".into())
                .spawn(move || flusher.flush_loop())
                .map_err(Error::from)?,
        );
        for i in 0..opts.max_background_compactions.max(1) {
            let compactor = inner.clone();
            threads.push(
                std::thread::Builder::new()
                    .name(format!("solistore-compact-{i}"))
                    .spawn(move || compactor.compaction_loop())
                    .map_err(Error::from)?,
            );
        }
        if let Durability::Eventual { sync_interval } = opts.durability {
            let syncer = inner.clone();
            threads.push(
                std::thread::Builder::new()
                    .name("solistore-wal-sync".into())
                    .spawn(move || syncer.sync_loop(sync_interval))
                    .map_err(Error::from)?,
            );
        }
        Ok(Db {
            handle: Arc::new(Handle {
                inner,
                threads: Mutex::new(threads),
            }),
        })
    }

    fn inner(&self) -> &Inner {
        &self.handle.inner
    }

    pub fn path(&self) -> &Path {
        &self.inner().dir
    }

    pub fn recovery_report(&self) -> &RecoveryReport {
        &self.inner().recovery
    }

    pub fn stats(&self) -> Stats {
        let c = &self.inner().counters;
        let get = |a: &AtomicU64| a.load(Ordering::Relaxed);
        Stats {
            commit_groups: get(&c.commit_groups),
            committed_batches: get(&c.committed_batches),
            wal_fsyncs: get(&c.wal_fsyncs),
            l0_slowdowns: get(&c.l0_slowdowns),
            l0_stop_micros: get(&c.l0_stop_micros),
            memtable_wait_micros: get(&c.memtable_wait_micros),
            flushes: get(&c.flushes),
            flushed_bytes: get(&c.flushed_bytes),
            compactions: get(&c.compactions),
            compaction_read_bytes: get(&c.compaction_read_bytes),
            compaction_written_bytes: get(&c.compaction_written_bytes),
            compaction_micros: get(&c.compaction_micros),
        }
    }

    /// Memory held by the memtables, approximately.
    pub fn memtable_bytes(&self) -> usize {
        let sv = self.inner().sv.load();
        std::iter::once(&sv.mem)
            .chain(&sv.imms)
            .map(|m| m.approximate_size())
            .sum()
    }

    /// Memory held by open tables' indexes and filters.
    pub fn table_readers_bytes(&self) -> usize {
        self.inner()
            .sv
            .load()
            .version
            .files()
            .map(|f| f.pinned_bytes())
            .sum()
    }

    /// Bytes held by the block cache.
    pub fn block_cache_usage(&self) -> u64 {
        self.inner().block_cache.usage()
    }

    /// Bytes of every live table file (RocksDB's `total-sst-files-size`).
    pub fn live_table_bytes(&self) -> u64 {
        self.inner()
            .sv
            .load()
            .version
            .files()
            .map(|f| f.props.size)
            .sum()
    }

    /// Range tombstones some table file may still hold deleted data for.
    pub fn pending_range_tombstones(&self) -> usize {
        self.inner().sv.load().version.tombstones.len()
    }

    /// Table files per level.
    pub fn files_per_level(&self) -> Vec<usize> {
        self.inner()
            .sv
            .load()
            .version
            .levels
            .iter()
            .map(Vec::len)
            .collect()
    }

    // ------------------------------------------------------------ column families

    pub fn cf_handle(&self, name: &str) -> Option<ColumnFamily> {
        self.inner().cfs.read().get(name).cloned()
    }

    pub fn default_cf(&self) -> ColumnFamily {
        self.cf_handle(DEFAULT_CF)
            .expect("the default column family always exists")
    }

    pub fn cf_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.inner().cfs.read().keys().cloned().collect();
        names.sort();
        names
    }

    /// Create a column family. Durable when this returns.
    pub fn create_cf(&self, name: &str) -> Result<ColumnFamily> {
        if name.is_empty() {
            return Err(Error::InvalidArgument("empty column family name".into()));
        }
        let inner = self.inner();
        let mut structure = inner.structure.lock();
        if inner.cfs.read().contains_key(name) {
            return Err(Error::ColumnFamilyExists(name.to_string()));
        }
        let id = structure.state.next_cf_id;
        let next = id
            .checked_add(1)
            .ok_or_else(|| Error::InvalidArgument("column family ids exhausted".into()))?;
        inner.append_edits(
            &mut structure,
            &[
                Edit::CreateCf {
                    id,
                    name: name.to_string(),
                },
                Edit::NextCfId(next),
            ],
        )?;
        let cf = ColumnFamily::new(id, name);
        inner.cfs.write().insert(name.to_string(), cf.clone());
        Ok(cf)
    }

    /// Drop a column family. Its data becomes unreachable at once, and every
    /// handle on it fails from now on; its table files are deleted, and
    /// what is left of it in memtables is skipped when they are flushed.
    /// Durable when this returns.
    pub fn drop_cf(&self, name: &str) -> Result<()> {
        if name == DEFAULT_CF {
            return Err(Error::InvalidArgument(
                "the default column family cannot be dropped".into(),
            ));
        }
        let inner = self.inner();
        let cf = self
            .cf_handle(name)
            .ok_or_else(|| Error::ColumnFamilyNotFound(name.to_string()))?;
        let id = cf.id();
        inner.log_and_apply(
            |sv| {
                let mut edits = vec![Edit::DropCf { id }];
                let mut version = (*sv.version).clone();
                let mut retired = Vec::new();
                for (level, files) in version.levels.iter_mut().enumerate() {
                    files.retain(|f| {
                        let doomed = compaction::file_bounds(f).is_some_and(|(c, _, _)| c == id);
                        if doomed {
                            edits.push(Edit::DeleteFile {
                                level: level as u32,
                                number: f.number,
                            });
                            retired.push(f.clone());
                        }
                        !doomed
                    });
                }
                Ok(Some(VersionChange {
                    edits,
                    sv: SuperVersion {
                        mem: sv.mem.clone(),
                        imms: sv.imms.clone(),
                        version: Arc::new(version),
                    },
                    retired,
                }))
            },
            || {},
        )?;
        cf.inner.dropped.store(true, Ordering::Release);
        inner.cfs.write().remove(name);
        Ok(())
    }

    /// Compact the part of `cf` between `start` and `end` (whole column
    /// family when both are `None`): flush, merge what level 0 holds there,
    /// and rewrite every file that may still hold range-deleted data — the
    /// space a dropped collection held comes back. Waits until done.
    pub fn compact_range(
        &self,
        cf: &ColumnFamily,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> Result<()> {
        cf.check()?;
        self.flush()?;
        let inner = self.inner();
        let prefix = cf.id().to_be_bytes();
        let lo = [&prefix[..], start.unwrap_or_default()].concat();
        let hi = match end {
            Some(end) => [&prefix[..], end].concat(),
            None => cf
                .id()
                .checked_add(1)
                .map_or_else(|| vec![0xff; 5], |next| next.to_be_bytes().to_vec()),
        };
        let wanted = |v: &Version, busy: compaction::Busy<'_>| {
            compaction::pick_for_tombstones(v, busy)
                .or_else(|| compaction::pick_level0_overlapping(v, &lo, &hi, busy))
        };
        // Bounded: writes arriving meanwhile could otherwise keep it going.
        for _ in 0..10_000 {
            match inner.claim(wanted) {
                Some((c, version)) => {
                    let outcome = inner.compact(&c, &version);
                    inner.release(&c);
                    outcome?;
                }
                None => {
                    // Done, unless what is left is held by a background
                    // compaction: wait for it.
                    let version = inner.sv.load().version.clone();
                    if wanted(&version, &|_| false).is_none() {
                        break;
                    }
                    inner.check()?;
                    let mut bg = inner.bg.lock();
                    inner.bg_cv.wait_for(&mut bg, Duration::from_millis(10));
                }
            }
        }
        Ok(())
    }

    /// Write a consistent copy of the database to `path`, which must not
    /// exist: every memtable is flushed, then each live table file is hard
    /// linked (copied across file systems) and a fresh MANIFEST written. The
    /// copy holds everything committed before the call and opens like any
    /// database. On the same file system it costs no space until the source
    /// compacts the linked files away — which also means it is no protection
    /// against losing that file system.
    pub fn checkpoint(&self, path: impl AsRef<Path>) -> Result<()> {
        let target = path.as_ref();
        let inner = self.inner();
        if inner.fs.exists(target) {
            return Err(Error::InvalidArgument(format!(
                "{} already exists",
                target.display()
            )));
        }
        self.flush()?;
        // The pinned version keeps its files alive while they are linked.
        let sv = inner.sv.load_full();
        let mut state = ManifestState::empty();
        state.cfs = inner.structure.lock().state.cfs.clone();
        state.next_cf_id = inner.structure.lock().state.next_cf_id;
        state.tombstones = sv.version.tombstones.clone();
        let mut highest = 0;
        for (level, files) in sv.version.levels.iter().enumerate() {
            for f in files {
                state.levels[level].insert(f.number, f.props.clone());
                state.last_seq = state.last_seq.max(f.props.largest_seq);
                highest = highest.max(f.number);
            }
        }
        for t in &state.tombstones {
            state.last_seq = state.last_seq.max(t.seq);
        }
        state.next_file = highest + 1;
        state.log_number = state.next_file; // no WAL in a checkpoint

        inner.fs.create_dir_all(target)?;
        for f in sv.version.files() {
            inner.fs.link(
                &inner.dir.join(table_name(f.number)),
                &target.join(table_name(f.number)),
            )?;
        }
        let manifest_no = state.allocate_file();
        ManifestWriter::install(inner.fs.as_ref(), target, manifest_no, &state)?;
        Ok(())
    }

    // ------------------------------------------------------------ writes

    pub fn put(
        &self,
        cf: &ColumnFamily,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
    ) -> Result<()> {
        let mut batch = WriteBatch::new();
        batch.put(cf, key, value);
        self.write(batch)
    }

    pub fn delete(&self, cf: &ColumnFamily, key: impl AsRef<[u8]>) -> Result<()> {
        let mut batch = WriteBatch::new();
        batch.delete(cf, key);
        self.write(batch)
    }

    pub fn write(&self, batch: WriteBatch) -> Result<()> {
        self.write_opt(batch, &WriteOptions::default())
    }

    /// Apply a batch atomically. Returns once it is visible to readers and
    /// as durable as the database's [`Durability`] (and `opts`) require.
    pub fn write_opt(&self, batch: WriteBatch, opts: &WriteOptions) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }
        for cf in batch.column_families() {
            cf.check()?;
        }
        self.inner().commit(batch, opts.sync)
    }

    /// Fsync the WAL now: everything acknowledged so far becomes durable.
    pub fn sync_wal(&self) -> Result<()> {
        let inner = self.inner();
        inner.check()?;
        let wal = inner.current_wal.lock().clone();
        wal.dirty.store(false, Ordering::Release);
        wal.file.sync().inspect_err(|_| inner.poison())?;
        Ok(())
    }

    /// Write every memtable out as table files and wait until it is done.
    pub fn flush(&self) -> Result<()> {
        let inner = self.inner();
        {
            let mut commit = inner.commit.lock();
            if !inner.sv.load().mem.is_empty() {
                inner.switch_memtable(&mut commit)?;
            }
        }
        let mut bg = inner.bg.lock();
        loop {
            inner.check()?;
            if let Some(e) = &bg.flush_error {
                return Err(e.clone());
            }
            if inner.sv.load().imms.is_empty() {
                return Ok(());
            }
            inner.bg_cv.wait(&mut bg);
        }
    }

    // ------------------------------------------------------------ reads

    pub fn get(&self, cf: &ColumnFamily, key: impl AsRef<[u8]>) -> Result<Option<Bytes>> {
        cf.check()?;
        let inner = self.inner();
        // The super version first, then the snapshot: see `version`.
        let sv = inner.sv.load();
        let snapshot = inner.visible_seq.load(Ordering::Acquire);
        sv.get(cf.id(), key.as_ref(), snapshot)
    }

    /// Several gets at one snapshot.
    pub fn multi_get<K: AsRef<[u8]>>(
        &self,
        cf: &ColumnFamily,
        keys: &[K],
    ) -> Result<Vec<Option<Bytes>>> {
        cf.check()?;
        let inner = self.inner();
        let sv = inner.sv.load();
        let snapshot = inner.visible_seq.load(Ordering::Acquire);
        keys.iter()
            .map(|k| sv.get(cf.id(), k.as_ref(), snapshot))
            .collect()
    }

    pub fn iterator(&self, cf: &ColumnFamily, mode: IteratorMode<'_>) -> Result<DbIterator> {
        self.iterator_opt(cf, ReadOptions::default(), mode)
    }

    pub fn iterator_opt(
        &self,
        cf: &ColumnFamily,
        opts: ReadOptions,
        mode: IteratorMode<'_>,
    ) -> Result<DbIterator> {
        cf.check()?;
        let inner = self.inner();
        let sv = inner.sv.load_full();
        let snapshot = inner.visible_seq.load(Ordering::Acquire);
        DbIterator::new(sv, cf.id(), snapshot, opts, mode)
    }
}

impl fmt::Debug for Db {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Db({})", self.inner().dir.display())
    }
}

impl Drop for Handle {
    fn drop(&mut self) {
        self.inner.bg.lock().shutdown = true;
        self.inner.bg_cv.notify_all();
        for thread in self.threads.lock().drain(..) {
            thread.join().ok();
        }
        // A clean close leaves nothing for the next open to lose.
        let wal = self.inner.current_wal.lock().clone();
        if !self.inner.poisoned.load(Ordering::Acquire) && wal.dirty.load(Ordering::Acquire) {
            wal.file.sync().ok();
        }
    }
}

/// Runs a closure when dropped: time an early-returning block.
struct Defer<F: FnMut()>(F);

impl<F: FnMut()> Drop for Defer<F> {
    fn drop(&mut self) {
        (self.0)()
    }
}

/// A structural change: its MANIFEST edits, the super version it leads to,
/// and the table files it removes.
struct VersionChange {
    edits: Vec<Edit>,
    sv: SuperVersion,
    retired: Vec<Arc<TableFile>>,
}

/// Finish a change against the range tombstones it leaves: drop every file
/// a tombstone deletes entirely, and every tombstone no file may hold
/// covered data for any more.
fn tidy(change: &mut VersionChange) {
    let mut version = (*change.sv.version).clone();
    for t in version.tombstones.clone() {
        for (level, files) in version.levels.iter_mut().enumerate() {
            files.retain(|f| {
                let doomed = compaction::fully_covered(f, &t);
                if doomed {
                    change.edits.push(Edit::DeleteFile {
                        level: level as u32,
                        number: f.number,
                    });
                    change.retired.push(f.clone());
                }
                !doomed
            });
        }
    }
    let files: Vec<Arc<TableFile>> = version.files().cloned().collect();
    version.tombstones.retain(|t| {
        let needed = files.iter().any(|f| compaction::may_hold_covered(f, t));
        if !needed {
            change.edits.push(Edit::RemoveRangeTombstone { seq: t.seq });
        }
        needed
    });
    change.sv.version = Arc::new(version);
}

/// Apply decoded ops to a memtable, numbering them from `first_seq`.
fn apply(mem: &Memtable, ops: Vec<BatchOp<'_>>, first_seq: u64, live: impl Fn(u32) -> bool) {
    let mut bytes = 0;
    let mut last = 0;
    for (seq, op) in (first_seq..).zip(ops) {
        match op {
            BatchOp::Put { cf, key, value } if live(cf) => {
                bytes += mem.insert_entry(cf, key, seq, Kind::Put, value)
            }
            BatchOp::Delete { cf, key } if live(cf) => {
                bytes += mem.insert_entry(cf, key, seq, Kind::Delete, b"")
            }
            BatchOp::DeleteRange { cf, start, end } if live(cf) => {
                mem.insert_range_tombstone(cf, start, end, seq)
            }
            _ => {} // a column family dropped before the crash
        }
        last = seq;
    }
    mem.account(bytes, last);
}

impl Inner {
    fn poison(&self) {
        self.poisoned.store(true, Ordering::Release);
        self.bg_cv.notify_all();
    }

    fn check(&self) -> Result<()> {
        if self.poisoned.load(Ordering::Acquire) {
            Err(Error::Poisoned)
        } else {
            Ok(())
        }
    }

    /// Run an I/O step of the write path; any failure poisons the database.
    fn io<T>(&self, step: impl FnOnce() -> Result<T>) -> Result<T> {
        step().inspect_err(|_| self.poison())
    }

    fn append_edits(&self, structure: &mut Structure, edits: &[Edit]) -> Result<()> {
        self.check()?;
        // A torn MANIFEST record would hide every later edit on replay.
        self.io(|| structure.writer.append(edits))?;
        for edit in edits {
            structure.state.apply(edit);
        }
        if structure.writer.size() > MANIFEST_ROLL_BYTES {
            // Start a new one holding just the current state.
            let old = structure.writer.number();
            let no = structure.state.allocate_file();
            let writer = self.io(|| {
                ManifestWriter::install(self.fs.as_ref(), &self.dir, no, &structure.state)
            })?;
            structure.writer = writer;
            self.fs
                .remove(&self.dir.join(manifest::manifest_name(old)))
                .ok();
        }
        Ok(())
    }

    fn allocate_file(&self) -> u64 {
        self.structure.lock().state.allocate_file()
    }

    fn install(&self, change: impl FnOnce(&SuperVersion) -> SuperVersion) {
        let _guard = self.sv_lock.lock();
        let current = self.sv.load_full();
        self.sv.store(Arc::new(change(&current)));
    }

    fn notify_background(&self) {
        let _bg = self.bg.lock();
        self.bg_cv.notify_all();
    }

    // ------------------------------------------------------------ commit

    fn commit(&self, batch: WriteBatch, sync: bool) -> Result<()> {
        let me = Arc::new(Pending {
            batch,
            sync,
            slot: Mutex::new(None),
            ready: AtomicBool::new(false),
        });
        let mut queue = self.queue.lock();
        queue.waiting.push_back(me.clone());
        self.queued.fetch_add(1, Ordering::Relaxed);
        let mut spun = false;
        let lead = loop {
            if me.ready.load(Ordering::Acquire) {
                break Err(me.slot.lock().take().expect("ready means set"));
            }
            let at_head = queue.waiting.front().is_some_and(|p| Arc::ptr_eq(p, &me));
            if at_head && !queue.leader_active {
                break Ok(());
            }
            if !spun {
                // Most waits end within microseconds: spin before sleeping.
                spun = true;
                parking_lot::MutexGuard::unlocked(&mut queue, || {
                    spin_until(|| {
                        me.ready.load(Ordering::Acquire)
                            || !self.leader_busy.load(Ordering::Acquire)
                    });
                });
                continue;
            }
            spun = false;
            self.queue_cv.wait(&mut queue);
        };
        let slot = match lead {
            Err(slot) => {
                drop(queue);
                slot
            }
            Ok(()) => {
                queue.leader_active = true;
                self.leader_busy.store(true, Ordering::Release);
                // A group that will be fsynced is worth gathering: when the
                // last group's writers come back, the first one would
                // otherwise pay a whole fsync alone while the rest queue up
                // behind it.
                let fsync = self.durability == Durability::Full || me.sync;
                let want = self.last_group.load(Ordering::Relaxed);
                if fsync && queue.waiting.len() < want {
                    parking_lot::MutexGuard::unlocked(&mut queue, || {
                        spin_until(|| self.queued.load(Ordering::Acquire) >= want);
                    });
                }
                // Take the group and write it to the WAL outside the queue
                // lock. The next leader can start on its own group as soon as
                // this one is in the WAL.
                let mut group = Vec::new();
                let mut bytes = 0;
                while let Some(next) = queue.waiting.front() {
                    let size = next.batch.size_in_bytes();
                    if !group.is_empty() && bytes + size > MAX_GROUP_BYTES {
                        break;
                    }
                    bytes += size;
                    group.push(queue.waiting.pop_front().expect("front exists"));
                }
                self.queued.fetch_sub(group.len(), Ordering::Relaxed);
                self.last_group.store(group.len(), Ordering::Relaxed);
                drop(queue);
                let guard = LeaderGuard {
                    inner: self,
                    group: &group,
                    me: &me,
                };
                let written = self.write_group(&group);
                guard.finish(written)
            }
        };
        match slot {
            Slot::Failed(e) => Err(e),
            Slot::Insert(group, first) => {
                self.insert_and_publish(&me.batch, &group, first);
                Ok(())
            }
        }
    }

    /// Give the group its sequence numbers and write it to the WAL, fsynced
    /// if the durability asks for it. Returns the group and each member's
    /// first sequence number.
    fn write_group(&self, group: &[Arc<Pending>]) -> Result<(Arc<Group>, Vec<u64>)> {
        self.check()?;
        let mut commit = self.commit.lock();
        self.throttle()?;
        if self.sv.load().mem.approximate_size() >= self.write_buffer_size {
            self.switch_memtable(&mut commit)?;
        }
        let CommitState { wal, last_seq, buf } = &mut *commit;
        let wal_no = file_no_u32(wal.number)?;
        buf.clear();
        let mut seq = *last_seq;
        let mut firsts = Vec::with_capacity(group.len());
        for pending in group {
            firsts.push(seq + 1);
            let header = pending.batch.header(seq + 1);
            encode_record_parts(buf, wal_no, &[&header, pending.batch.ops_bytes()]);
            seq += pending.batch.len() as u64;
        }
        if seq > MAX_SEQ {
            return Err(Error::InvalidArgument("sequence numbers exhausted".into()));
        }

        let sync = self.durability == Durability::Full || group.iter().any(|p| p.sync);
        self.io(|| Ok(wal.file.append(buf)?))?;
        add(&self.counters.commit_groups, 1);
        add(&self.counters.committed_batches, group.len() as u64);
        if sync {
            add(&self.counters.wal_fsyncs, 1);
            self.io(|| Ok(wal.file.sync()?))?;
        } else {
            wal.dirty.store(true, Ordering::Release);
        }
        if buf.capacity() > 2 * MAX_GROUP_BYTES {
            *buf = Vec::new();
        }
        let first = *last_seq + 1;
        *last_seq = seq;
        // Switches happen under the commit lock, and wait for every group
        // already handed out to be published: this memtable stays the one
        // this group belongs in until it is.
        let mem = self.sv.load().mem.clone();
        Ok((
            Arc::new(Group {
                mem,
                first,
                last: seq,
                remaining: AtomicUsize::new(group.len()),
            }),
            firsts,
        ))
    }

    /// Insert one member's batch; the group's last member to finish
    /// publishes it once every earlier group is published. Returns when the
    /// group is visible, so a writer always reads its own write.
    fn insert_and_publish(&self, batch: &WriteBatch, group: &Group, first: u64) {
        apply(&group.mem, batch.ops(), first, |_| true);
        if group.remaining.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.wait_published(group.first - 1);
            // Under the lock, so a writer that just found the old value is
            // already waiting when the notification goes out.
            let published = self.publish.lock();
            self.visible_seq.store(group.last, Ordering::Release);
            drop(published);
            self.publish_cv.notify_all();
        } else {
            self.wait_published(group.last);
        }
    }

    /// Wait until `visible_seq` reaches `seq`.
    fn wait_published(&self, seq: u64) {
        if spin_until(|| self.visible_seq.load(Ordering::Acquire) >= seq) {
            return;
        }
        let mut published = self.publish.lock();
        while self.visible_seq.load(Ordering::Acquire) < seq {
            self.publish_cv.wait(&mut published);
        }
    }

    /// Hold writes back while level 0 has more files than compaction keeps
    /// up with: a little past the slowdown trigger, entirely at the stop one.
    /// Reads slow down with every level-0 file, so this bounds them too.
    fn throttle(&self) -> Result<()> {
        let l0 = self.sv.load().version.levels[0].len();
        if l0 >= self.l0_stop {
            let start = Instant::now();
            let _count = Defer(|| add(&self.counters.l0_stop_micros, micros_since(start)));
            let mut bg = self.bg.lock();
            while self.sv.load().version.levels[0].len() >= self.l0_stop {
                self.check()?;
                if let Some(e) = &bg.flush_error {
                    return Err(e.clone());
                }
                if bg.shutdown {
                    return Err(Error::InvalidArgument("the database is closing".into()));
                }
                self.bg_cv.wait(&mut bg);
            }
        } else if l0 >= self.l0_slowdown {
            add(&self.counters.l0_slowdowns, 1);
            std::thread::sleep(Duration::from_millis(1));
        }
        Ok(())
    }

    /// Seal the memtable and start a new one with a new WAL. Called with the
    /// commit lock held. Waits while too many memtables are waiting to flush.
    fn switch_memtable(&self, commit: &mut CommitState) -> Result<()> {
        {
            let start = Instant::now();
            let _count = Defer(|| add(&self.counters.memtable_wait_micros, micros_since(start)));
            let mut bg = self.bg.lock();
            while self.sv.load().imms.len() >= self.max_imms {
                self.check()?;
                if let Some(e) = &bg.flush_error {
                    return Err(e.clone());
                }
                if bg.shutdown {
                    return Err(Error::InvalidArgument("the database is closing".into()));
                }
                self.bg_cv.wait(&mut bg);
            }
        }
        // A flush must not start on a memtable still being inserted into.
        self.wait_published(commit.last_seq);
        // Everything in the old WAL must be durable before anything lands in
        // the new one: a crash may not keep a later write and lose an
        // earlier one.
        if commit.wal.dirty.swap(false, Ordering::AcqRel) {
            self.io(|| Ok(commit.wal.file.sync()?))?;
        }
        let number = self.allocate_file();
        let file = self.io(|| Ok(self.fs.create(&self.dir.join(wal_name(number)))?))?;
        self.io(|| Ok(self.fs.sync_dir(&self.dir)?))?;
        let wal = Arc::new(WalFile {
            file,
            number,
            dirty: AtomicBool::new(false),
        });
        commit.wal = wal.clone();
        *self.current_wal.lock() = wal;

        self.install(|sv| SuperVersion {
            mem: Arc::new(Memtable::new(number)),
            imms: std::iter::once(sv.mem.clone())
                .chain(sv.imms.iter().cloned())
                .collect(),
            version: sv.version.clone(),
        });
        self.notify_background();
        Ok(())
    }

    // ------------------------------------------------------------ background

    fn flush_loop(&self) {
        loop {
            let imm = {
                let mut bg = self.bg.lock();
                loop {
                    if bg.shutdown || self.poisoned.load(Ordering::Acquire) {
                        return;
                    }
                    if let Some(imm) = self.sv.load().imms.last() {
                        break imm.clone();
                    }
                    self.bg_cv.wait(&mut bg);
                }
            };
            if let Err(e) = self.flush_memtable(&imm) {
                self.poison();
                self.bg.lock().flush_error = Some(e);
                self.notify_background();
                return;
            }
        }
    }

    /// Change the database's structure: compute the edits and the new super
    /// version from the current ones, log the edits, run `after_log`,
    /// install, and retire the files the change dropped. One change at a
    /// time, so a change computed from the current version is still valid
    /// when it is installed. `None` from `change` means nothing to do.
    fn log_and_apply(
        &self,
        change: impl FnOnce(&SuperVersion) -> Result<Option<VersionChange>>,
        after_log: impl FnOnce(),
    ) -> Result<bool> {
        let _guard = self.sv_lock.lock();
        let current = self.sv.load_full();
        let Some(mut change) = change(&current)? else {
            return Ok(false);
        };
        tidy(&mut change);
        {
            let mut structure = self.structure.lock();
            let next_file = structure.state.next_file;
            change.edits.push(Edit::NextFile(next_file));
            self.append_edits(&mut structure, &change.edits)?;
        }
        after_log();
        self.sv.store(Arc::new(change.sv));
        // Deleted once the last reader holding an older version is done.
        change.retired.iter().for_each(|f| f.mark_obsolete());
        Ok(true)
    }

    fn live_cfs(&self) -> HashSet<u32> {
        self.structure.lock().state.cfs.keys().copied().collect()
    }

    /// Write the oldest sealed memtable as level-0 tables and retire it.
    fn flush_memtable(&self, imm: &Arc<Memtable>) -> Result<()> {
        let tombstones = imm.tombstones();
        let (next_wal, watermark) = {
            let sv = self.sv.load();
            // WALs before the next memtable's first one hold nothing unflushed.
            let at = sv
                .imms
                .iter()
                .position(|m| Arc::ptr_eq(m, imm))
                .expect("the memtable being flushed");
            let next_wal = if at == 0 {
                sv.mem.first_wal()
            } else {
                sv.imms[at - 1].first_wal()
            };
            let watermark = sv
                .version
                .tombstones
                .iter()
                .chain(&tombstones)
                .map(|t| t.seq)
                .max()
                .unwrap_or(0);
            (next_wal, watermark)
        };
        let live = self.live_cfs();
        let files = flush::write_tables(
            self.fs.as_ref(),
            &self.dir,
            imm,
            &|cf| live.contains(&cf),
            self.target_file_size,
            watermark,
            || self.allocate_file(),
        )?;

        add(&self.counters.flushes, 1);
        add(
            &self.counters.flushed_bytes,
            files.iter().map(|(_, p)| p.size).sum(),
        );
        let tables: Vec<Arc<TableFile>> = files
            .iter()
            .map(|(number, props)| {
                Arc::new(TableFile::new(
                    self.fs.clone(),
                    self.block_cache.clone(),
                    &self.dir,
                    *number,
                    props.clone(),
                ))
            })
            .collect();
        self.log_and_apply(
            |sv| {
                let mut edits: Vec<Edit> = files
                    .iter()
                    .map(|(number, props)| Edit::AddFile {
                        level: 0,
                        number: *number,
                        props: props.clone(),
                    })
                    .collect();
                edits.extend(tombstones.iter().cloned().map(Edit::AddRangeTombstone));
                edits.push(Edit::LogNumber(next_wal));
                edits.push(Edit::LastSeq(imm.max_seq()));
                let mut version = (*sv.version).clone();
                version.levels[0].extend(tables);
                version.sort_level0();
                version.tombstones.extend(tombstones.iter().cloned());
                Ok(Some(VersionChange {
                    edits,
                    sv: SuperVersion {
                        mem: sv.mem.clone(),
                        imms: sv
                            .imms
                            .iter()
                            .filter(|m| !Arc::ptr_eq(m, imm))
                            .cloned()
                            .collect(),
                        version: Arc::new(version),
                    },
                    retired: Vec::new(),
                }))
            },
            // The LogNumber edit is durable: the old WALs can go, before the
            // new version is visible to anyone waiting for this flush.
            || {
                for name in self.fs.list(&self.dir).unwrap_or_default() {
                    if parse_wal_name(&name).is_some_and(|no| no < next_wal) {
                        self.fs.remove(&self.dir.join(&name)).ok();
                    }
                }
            },
        )?;
        self.notify_background();
        Ok(())
    }

    /// Pick a compaction with `choose` and reserve its files, atomically
    /// with respect to every other compaction.
    fn claim(
        &self,
        choose: impl FnOnce(&Version, compaction::Busy<'_>) -> Option<compaction::Compaction>,
    ) -> Option<(compaction::Compaction, Arc<Version>)> {
        let mut busy = self.compacting.lock();
        let version = self.sv.load().version.clone();
        let picked = choose(&version, &|n| busy.contains(&n))?;
        busy.extend(picked.all_inputs().map(|(_, f)| f.number));
        Some((picked, version))
    }

    fn release(&self, c: &compaction::Compaction) {
        let mut busy = self.compacting.lock();
        for (_, f) in c.all_inputs() {
            busy.remove(&f.number);
        }
        drop(busy);
        self.notify_background();
    }

    fn compaction_loop(&self) {
        loop {
            let (picked, version) = {
                let mut bg = self.bg.lock();
                loop {
                    if bg.shutdown || self.poisoned.load(Ordering::Acquire) {
                        return;
                    }
                    let cursors = self.cursors.lock().clone();
                    if let Some(claimed) =
                        self.claim(|v, busy| compaction::pick(v, &cursors, &self.compaction, busy))
                    {
                        break claimed;
                    }
                    self.bg_cv.wait(&mut bg);
                }
            };
            let outcome = self.compact(&picked, &version);
            self.release(&picked);
            if let Err(e) = outcome {
                self.poison();
                self.bg.lock().flush_error = Some(e);
                self.notify_background();
                return;
            }
        }
    }

    /// Run one compaction picked from `version`, its files reserved. If the
    /// inputs changed meanwhile (a range delete dropped one), the result is
    /// discarded.
    fn compact(&self, c: &compaction::Compaction, version: &Version) -> Result<()> {
        let start = Instant::now();
        let output_level = c.output_level;
        let outputs = if c.is_trivial_move() {
            Vec::new()
        } else {
            let live = self.live_cfs();
            compaction::run(
                c,
                version,
                &|cf| live.contains(&cf),
                self.fs.as_ref(),
                &self.dir,
                &self.compaction,
                &|| self.allocate_file(),
            )?
        };
        let new_tables: Vec<Arc<TableFile>> = if c.is_trivial_move() {
            vec![c.inputs[0].clone()]
        } else {
            outputs
                .iter()
                .map(|(number, props)| {
                    Arc::new(TableFile::new(
                        self.fs.clone(),
                        self.block_cache.clone(),
                        &self.dir,
                        *number,
                        props.clone(),
                    ))
                })
                .collect()
        };
        let applied = self.log_and_apply(
            |sv| {
                let current = c
                    .all_inputs()
                    .all(|(level, f)| sv.version.levels[level].iter().any(|g| Arc::ptr_eq(g, f)));
                if !current {
                    return Ok(None);
                }
                let mut edits: Vec<Edit> = c
                    .all_inputs()
                    .map(|(level, f)| Edit::DeleteFile {
                        level: level as u32,
                        number: f.number,
                    })
                    .collect();
                edits.extend(new_tables.iter().map(|f| Edit::AddFile {
                    level: output_level as u32,
                    number: f.number,
                    props: f.props.clone(),
                }));
                let mut version = (*sv.version).clone();
                for (level, f) in c.all_inputs() {
                    version.levels[level].retain(|g| !Arc::ptr_eq(g, f));
                }
                version.levels[output_level].extend(new_tables.iter().cloned());
                version.levels[output_level].sort_by(by_smallest);
                let retired = if c.is_trivial_move() {
                    Vec::new()
                } else {
                    c.all_inputs().map(|(_, f)| f.clone()).collect()
                };
                Ok(Some(VersionChange {
                    edits,
                    sv: SuperVersion {
                        mem: sv.mem.clone(),
                        imms: sv.imms.clone(),
                        version: Arc::new(version),
                    },
                    retired,
                }))
            },
            || {},
        )?;
        add(&self.counters.compactions, 1);
        add(&self.counters.compaction_micros, micros_since(start));
        if !c.is_trivial_move() {
            add(
                &self.counters.compaction_read_bytes,
                c.all_inputs().map(|(_, f)| f.props.size).sum(),
            );
            add(
                &self.counters.compaction_written_bytes,
                outputs.iter().map(|(_, p)| p.size).sum(),
            );
        }
        if !applied {
            for (number, _) in &outputs {
                self.fs.remove(&self.dir.join(table_name(*number))).ok();
            }
        }
        if c.output_level > c.level {
            self.cursors.lock()[c.level] = c.largest();
        }
        self.notify_background();
        Ok(())
    }

    fn sync_loop(&self, interval: Duration) {
        loop {
            {
                let mut bg = self.bg.lock();
                if !bg.shutdown {
                    self.bg_cv.wait_for(&mut bg, interval);
                }
                if bg.shutdown {
                    return;
                }
            }
            if self.poisoned.load(Ordering::Acquire) {
                return;
            }
            let wal = self.current_wal.lock().clone();
            if wal.dirty.swap(false, Ordering::AcqRel) && wal.file.sync().is_err() {
                self.poison();
            }
        }
    }
}

/// Tells every member of the group what to do next and frees the leader
/// slot — also if the leader panics, so the queue cannot wedge.
struct LeaderGuard<'a> {
    inner: &'a Inner,
    group: &'a [Arc<Pending>],
    me: &'a Arc<Pending>,
}

impl LeaderGuard<'_> {
    /// Hand out the slots and return the leader's own.
    fn finish(self, written: Result<(Arc<Group>, Vec<u64>)>) -> Slot {
        let mut mine = None;
        let mut queue = self.inner.queue.lock();
        for (i, pending) in self.group.iter().enumerate() {
            let slot = match &written {
                Ok((group, firsts)) => Slot::Insert(group.clone(), firsts[i]),
                Err(e) => Slot::Failed(e.clone()),
            };
            if Arc::ptr_eq(pending, self.me) {
                mine = Some(slot);
            } else {
                *pending.slot.lock() = Some(slot);
                pending.ready.store(true, Ordering::Release);
            }
        }
        queue.leader_active = false;
        self.inner.leader_busy.store(false, Ordering::Release);
        drop(queue);
        self.inner.queue_cv.notify_all();
        std::mem::forget(self);
        mine.expect("the leader is in its own group")
    }
}

impl Drop for LeaderGuard<'_> {
    fn drop(&mut self) {
        // Only reached by unwinding out of the WAL write.
        self.inner.poison();
        let mut queue = self.inner.queue.lock();
        for pending in self.group {
            if !Arc::ptr_eq(pending, self.me) {
                *pending.slot.lock() = Some(Slot::Failed(Error::Poisoned));
                pending.ready.store(true, Ordering::Release);
            }
        }
        queue.leader_active = false;
        self.inner.leader_busy.store(false, Ordering::Release);
        drop(queue);
        self.inner.queue_cv.notify_all();
    }
}
