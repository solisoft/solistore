//! The database: opening and recovery, column families, the commit path and
//! point reads.
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

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

use bytes::Bytes;
use parking_lot::{Condvar, Mutex, RwLock};

use crate::batch::{self, BatchOp, WriteBatch};
use crate::error::{Error, Result};
use crate::fs::{Fs, StdFs, WritableFile};
use crate::iter::{DbIterator, IteratorMode, ReadOptions};
use crate::key::Kind;
use crate::log::{encode_record_parts, read_records};
use crate::manifest::{self, file_no_u32, Edit, ManifestState, ManifestWriter, DEFAULT_CF};
use crate::memtable::Memtable;

/// The most a commit group writes to the WAL at once. A leader stops taking
/// batches past this, so one huge group cannot hold every writer up.
pub const MAX_GROUP_BYTES: usize = 4 << 20;

/// When a commit is acknowledged relative to the disk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Durability {
    /// Every commit is fsynced before it returns: an acknowledged write
    /// survives power loss. Concurrent commits share one fsync.
    Full,
    /// Commits return once written to the OS; a background thread fsyncs
    /// every `sync_interval`, and a write with [`WriteOptions::sync`] waits
    /// for its own fsync. A crash of the process loses nothing; power loss
    /// can lose up to `sync_interval` of acknowledged writes.
    Eventual { sync_interval: Duration },
}

#[derive(Clone)]
pub struct Options {
    pub create_if_missing: bool,
    pub error_if_exists: bool,
    pub durability: Durability,
    pub fs: Arc<dyn Fs>,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            create_if_missing: true,
            error_if_exists: false,
            durability: Durability::Full,
            fs: Arc::new(StdFs),
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
            .finish()
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct WriteOptions {
    /// Wait for this write's fsync even in [`Durability::Eventual`]. Always
    /// the case in [`Durability::Full`].
    pub sync: bool,
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

    fn is_dropped(&self) -> bool {
        self.inner.dropped.load(Ordering::Acquire)
    }

    fn check(&self) -> Result<()> {
        if self.is_dropped() {
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
    inner: Arc<Inner>,
}

struct Inner {
    dir: PathBuf,
    durability: Durability,
    cfs: RwLock<HashMap<String, ColumnFamily>>,
    structure: Mutex<Structure>,
    mem: Arc<Memtable>,
    /// The highest sequence number a reader may see.
    visible_seq: AtomicU64,
    commit: Mutex<CommitState>,
    queue: Mutex<Queue>,
    queue_cv: Condvar,
    wal: Arc<WalShared>,
    syncer: Option<Syncer>,
    recovery: RecoveryReport,
    _lock: Box<dyn Send + Sync>,
}

/// The MANIFEST and the state it describes, changed together.
struct Structure {
    writer: ManifestWriter,
    state: ManifestState,
}

/// The leader's working state. Only the current leader locks it.
struct CommitState {
    wal_no: u32,
    last_seq: u64,
    buf: Vec<u8>,
}

/// What the leader and the eventual-mode syncer share.
struct WalShared {
    file: Arc<dyn WritableFile>,
    /// Bytes have been written since the last fsync (eventual mode).
    dirty: AtomicBool,
    /// A write or fsync failed; see [`Error::Poisoned`].
    poisoned: AtomicBool,
}

impl WalShared {
    fn poison(&self) {
        self.poisoned.store(true, Ordering::Release);
    }

    fn check(&self) -> Result<()> {
        if self.poisoned.load(Ordering::Acquire) {
            Err(Error::Poisoned)
        } else {
            Ok(())
        }
    }
}

struct Pending {
    batch: WriteBatch,
    sync: bool,
    /// Set by the leader that committed this batch, under the queue lock.
    outcome: Mutex<Option<Result<()>>>,
}

#[derive(Default)]
struct Queue {
    waiting: VecDeque<Arc<Pending>>,
    leader_active: bool,
}

struct Syncer {
    stop: Arc<(Mutex<bool>, Condvar)>,
    thread: Option<JoinHandle<()>>,
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

        // Replay every WAL that may hold live data, oldest first.
        let mem = Arc::new(Memtable::new());
        let mut recovery = RecoveryReport::default();
        let mut last_seq = 0u64;
        let mut wals: Vec<u64> = fs
            .list(&dir)?
            .iter()
            .filter_map(|name| parse_wal_name(name))
            .filter(|no| *no >= state.log_number)
            .collect();
        wals.sort_unstable();
        for no in wals {
            let data = fs.read(&dir.join(wal_name(no)))?;
            let log = read_records(&data, file_no_u32(no)?);
            recovery.wal_files += 1;
            recovery.discarded_bytes += log.discarded as u64;
            for record in log.records {
                let decoded = batch::decode(record)?;
                recovery.batches += 1;
                let count = decoded.ops.len() as u64;
                apply(&mem, decoded.ops, decoded.first_seq, |cf| {
                    state.cfs.contains_key(&cf)
                });
                last_seq = last_seq.max(decoded.first_seq + count.saturating_sub(1));
            }
            state.next_file = state.next_file.max(no + 1);
        }

        // A new WAL and a new MANIFEST for this run. The MANIFEST's install
        // syncs the directory, which makes the WAL's name durable too.
        let manifest_no = state.allocate_file();
        let wal_no = state.allocate_file();
        let wal_file = fs.create(&dir.join(wal_name(wal_no)))?;
        let writer = ManifestWriter::install(fs.as_ref(), &dir, manifest_no, &state)?;
        for name in fs.list(&dir)? {
            if manifest::parse_manifest_name(&name).is_some_and(|no| no != manifest_no) {
                fs.remove(&dir.join(&name)).ok();
            }
        }

        let cfs = state
            .cfs
            .iter()
            .map(|(id, name)| (name.clone(), ColumnFamily::new(*id, name)))
            .collect();
        let wal = Arc::new(WalShared {
            file: wal_file,
            dirty: AtomicBool::new(false),
            poisoned: AtomicBool::new(false),
        });
        let syncer = match opts.durability {
            Durability::Full => None,
            Durability::Eventual { sync_interval } => {
                Some(Syncer::start(wal.clone(), sync_interval))
            }
        };
        Ok(Db {
            inner: Arc::new(Inner {
                dir,
                durability: opts.durability,
                cfs: RwLock::new(cfs),
                structure: Mutex::new(Structure { writer, state }),
                mem,
                visible_seq: AtomicU64::new(last_seq),
                commit: Mutex::new(CommitState {
                    wal_no: file_no_u32(wal_no)?,
                    last_seq,
                    buf: Vec::new(),
                }),
                queue: Mutex::new(Queue::default()),
                queue_cv: Condvar::new(),
                wal,
                syncer,
                recovery,
                _lock: lock,
            }),
        })
    }

    pub fn path(&self) -> &Path {
        &self.inner.dir
    }

    pub fn recovery_report(&self) -> &RecoveryReport {
        &self.inner.recovery
    }

    /// Memory held by the memtable, approximately.
    pub fn memtable_bytes(&self) -> usize {
        self.inner.mem.approximate_size()
    }

    // ------------------------------------------------------------ column families

    pub fn cf_handle(&self, name: &str) -> Option<ColumnFamily> {
        self.inner.cfs.read().get(name).cloned()
    }

    pub fn default_cf(&self) -> ColumnFamily {
        self.cf_handle(DEFAULT_CF)
            .expect("the default column family always exists")
    }

    pub fn cf_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.inner.cfs.read().keys().cloned().collect();
        names.sort();
        names
    }

    /// Create a column family. Durable when this returns.
    pub fn create_cf(&self, name: &str) -> Result<ColumnFamily> {
        if name.is_empty() {
            return Err(Error::InvalidArgument("empty column family name".into()));
        }
        let mut structure = self.inner.structure.lock();
        if self.inner.cfs.read().contains_key(name) {
            return Err(Error::ColumnFamilyExists(name.to_string()));
        }
        let id = structure.state.next_cf_id;
        let next = id
            .checked_add(1)
            .ok_or_else(|| Error::InvalidArgument("column family ids exhausted".into()))?;
        let edits = [
            Edit::CreateCf {
                id,
                name: name.to_string(),
            },
            Edit::NextCfId(next),
        ];
        self.append_edits(&mut structure, &edits)?;
        let cf = ColumnFamily::new(id, name);
        self.inner.cfs.write().insert(name.to_string(), cf.clone());
        Ok(cf)
    }

    /// Drop a column family. Its data becomes unreachable at once; every
    /// handle on it fails from now on. Durable when this returns.
    pub fn drop_cf(&self, name: &str) -> Result<()> {
        if name == DEFAULT_CF {
            return Err(Error::InvalidArgument(
                "the default column family cannot be dropped".into(),
            ));
        }
        let mut structure = self.inner.structure.lock();
        let cf = self
            .cf_handle(name)
            .ok_or_else(|| Error::ColumnFamilyNotFound(name.to_string()))?;
        self.append_edits(&mut structure, &[Edit::DropCf { id: cf.id() }])?;
        cf.inner.dropped.store(true, Ordering::Release);
        self.inner.cfs.write().remove(name);
        Ok(())
    }

    fn append_edits(&self, structure: &mut Structure, edits: &[Edit]) -> Result<()> {
        self.inner.wal.check()?;
        if let Err(e) = structure.writer.append(edits) {
            // A torn MANIFEST record would hide every later edit on replay.
            self.inner.wal.poison();
            return Err(e);
        }
        for edit in edits {
            structure.state.apply(edit);
        }
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
        self.inner.commit(batch, opts.sync)
    }

    /// Fsync the WAL now: everything acknowledged so far becomes durable.
    pub fn sync_wal(&self) -> Result<()> {
        self.inner.wal.check()?;
        self.inner
            .wal
            .file
            .sync()
            .inspect_err(|_| self.inner.wal.poison())?;
        Ok(())
    }

    // ------------------------------------------------------------ reads

    fn snapshot(&self) -> u64 {
        self.inner.visible_seq.load(Ordering::Acquire)
    }

    pub fn get(&self, cf: &ColumnFamily, key: impl AsRef<[u8]>) -> Result<Option<Bytes>> {
        cf.check()?;
        Ok(self.inner.get_at(cf.id(), key.as_ref(), self.snapshot()))
    }

    /// Several gets at one snapshot.
    pub fn multi_get<K: AsRef<[u8]>>(
        &self,
        cf: &ColumnFamily,
        keys: &[K],
    ) -> Result<Vec<Option<Bytes>>> {
        cf.check()?;
        let snapshot = self.snapshot();
        Ok(keys
            .iter()
            .map(|k| self.inner.get_at(cf.id(), k.as_ref(), snapshot))
            .collect())
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
        Ok(DbIterator::new(
            self.inner.mem.clone(),
            cf.id(),
            self.snapshot(),
            opts,
            mode,
        ))
    }
}

impl fmt::Debug for Db {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Db({})", self.inner.dir.display())
    }
}

/// Apply decoded ops to a memtable, numbering them from `first_seq`.
fn apply(mem: &Memtable, ops: Vec<BatchOp<'_>>, first_seq: u64, live: impl Fn(u32) -> bool) {
    for (seq, op) in (first_seq..).zip(ops) {
        match op {
            BatchOp::Put { cf, key, value } if live(cf) => {
                mem.insert(cf, key, seq, Kind::Put, value)
            }
            BatchOp::Delete { cf, key } if live(cf) => mem.insert(cf, key, seq, Kind::Delete, b""),
            BatchOp::DeleteRange { cf, start, end } if live(cf) => {
                mem.insert_range_tombstone(cf, start, end, seq)
            }
            _ => {} // a column family dropped before the crash
        }
    }
}

impl Inner {
    fn get_at(&self, cf: u32, key: &[u8], snapshot: u64) -> Option<Bytes> {
        let version = self.mem.get(cf, key, snapshot)?;
        if version.kind == Kind::Delete
            || self.mem.range_tombstone_seq(cf, key, snapshot) > version.seq
        {
            return None;
        }
        Some(version.value)
    }

    fn commit(&self, batch: WriteBatch, sync: bool) -> Result<()> {
        let me = Arc::new(Pending {
            batch,
            sync,
            outcome: Mutex::new(None),
        });
        let mut queue = self.queue.lock();
        queue.waiting.push_back(me.clone());
        loop {
            if let Some(outcome) = me.outcome.lock().take() {
                return outcome;
            }
            let at_head = queue.waiting.front().is_some_and(|p| Arc::ptr_eq(p, &me));
            if at_head && !queue.leader_active {
                break;
            }
            self.queue_cv.wait(&mut queue);
        }

        // Leader: take the group, commit it outside the queue lock.
        queue.leader_active = true;
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
        drop(queue);

        let guard = LeaderGuard {
            inner: self,
            group: &group,
            me: &me,
        };
        let outcome = self.commit_group(&group);
        guard.finish(outcome.clone());
        outcome
    }

    fn commit_group(&self, group: &[Arc<Pending>]) -> Result<()> {
        self.wal.check()?;
        let mut commit = self.commit.lock();
        let CommitState {
            wal_no,
            last_seq,
            buf,
        } = &mut *commit;
        buf.clear();
        let mut seq = *last_seq;
        let mut firsts = Vec::with_capacity(group.len());
        for pending in group {
            firsts.push(seq + 1);
            let header = pending.batch.header(seq + 1);
            encode_record_parts(buf, *wal_no, &[&header, pending.batch.ops_bytes()]);
            seq += pending.batch.len() as u64;
        }

        let sync = self.durability == Durability::Full || group.iter().any(|p| p.sync);
        if let Err(e) = self.wal.file.append(buf) {
            self.wal.poison();
            return Err(e.into());
        }
        if sync {
            if let Err(e) = self.wal.file.sync() {
                self.wal.poison();
                return Err(e.into());
            }
        } else {
            self.wal.dirty.store(true, Ordering::Release);
        }

        for (pending, first) in group.iter().zip(firsts) {
            apply(&self.mem, pending.batch.ops(), first, |_| true);
        }
        *last_seq = seq;
        self.visible_seq.store(seq, Ordering::Release);
        if buf.capacity() > 2 * MAX_GROUP_BYTES {
            *buf = Vec::new();
        }
        Ok(())
    }
}

/// Hands the group's outcome to its followers and frees the leader slot —
/// also if the leader panics, so the queue cannot wedge.
struct LeaderGuard<'a> {
    inner: &'a Inner,
    group: &'a [Arc<Pending>],
    me: &'a Arc<Pending>,
}

impl LeaderGuard<'_> {
    fn finish(self, outcome: Result<()>) {
        self.release(outcome);
        std::mem::forget(self);
    }

    fn release(&self, outcome: Result<()>) {
        let mut queue = self.inner.queue.lock();
        for pending in self.group {
            if !Arc::ptr_eq(pending, self.me) {
                *pending.outcome.lock() = Some(outcome.clone());
            }
        }
        queue.leader_active = false;
        drop(queue);
        self.inner.queue_cv.notify_all();
    }
}

impl Drop for LeaderGuard<'_> {
    fn drop(&mut self) {
        // Only reached by unwinding out of the commit.
        self.inner.wal.poison();
        self.release(Err(Error::Poisoned));
    }
}

impl Syncer {
    fn start(wal: Arc<WalShared>, interval: Duration) -> Syncer {
        let stop = Arc::new((Mutex::new(false), Condvar::new()));
        let signal = stop.clone();
        let thread = std::thread::Builder::new()
            .name("solistore-wal-sync".into())
            .spawn(move || loop {
                {
                    let mut stopped = signal.0.lock();
                    if !*stopped {
                        signal.1.wait_for(&mut stopped, interval);
                    }
                    if *stopped {
                        return;
                    }
                }
                if wal.poisoned.load(Ordering::Acquire) {
                    return;
                }
                if wal.dirty.swap(false, Ordering::AcqRel) && wal.file.sync().is_err() {
                    wal.poison();
                }
            })
            .expect("spawn the WAL sync thread");
        Syncer {
            stop,
            thread: Some(thread),
        }
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        if let Some(syncer) = &mut self.syncer {
            *syncer.stop.0.lock() = true;
            syncer.stop.1.notify_all();
            if let Some(thread) = syncer.thread.take() {
                thread.join().ok();
            }
        }
        // A clean close leaves nothing for the next open to lose.
        if !self.wal.poisoned.load(Ordering::Acquire) && self.wal.dirty.load(Ordering::Acquire) {
            self.wal.file.sync().ok();
        }
    }
}
