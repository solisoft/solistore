//! The file system, behind a trait so tests can crash it.
//!
//! [`StdFs`] is the real one. [`FaultFs`] is an in-memory file system that
//! knows which bytes and which directory entries have been made durable, and
//! can "crash": throw away everything that was not, optionally leaving a torn
//! tail of unsynced bytes behind. It also fails fsyncs on demand. Crash tests
//! run thousands of crash points through it in the time one process-level
//! SIGKILL test takes.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;

/// A file being written: appended to by one writer at a time, synced by any
/// thread (the commit leader, or the background syncer in eventual mode).
pub trait WritableFile: Send + Sync {
    fn append(&self, data: &[u8]) -> io::Result<()>;
    /// Make every appended byte durable (`fdatasync`).
    fn sync(&self) -> io::Result<()>;
    /// Start writing `[offset, offset + len)` back to the device without
    /// waiting for it, and without the journal flush an fsync costs
    /// (`sync_file_range(SYNC_FILE_RANGE_WRITE)` on Linux, RocksDB's
    /// `bytes_per_sync`). A large file then reaches the disk as it is
    /// written instead of in one burst at its final fsync. A no-op where
    /// the platform has nothing like it.
    fn start_writeback(&self, _offset: u64, _len: u64) {}
    /// From now on, keep the file's blocks allocated `chunk` bytes at a
    /// time ahead of what is appended (`fallocate`), the file growing with
    /// them: it then ends in zeros, which its reader must take for its end.
    /// An append into blocks the file already owns changes neither its size
    /// nor its extents, so the fdatasync after it flushes data and no file
    /// system metadata: measured on XFS over NVMe, 14.5 µs against 32 µs
    /// for an fdatasync after an append that grew the file. A no-op where
    /// the platform has nothing like it, or the file system refuses.
    fn allocate_ahead(&self, _chunk: u64) {}
}

/// A file read at arbitrary offsets by any number of threads (`pread`).
pub trait RandomAccessFile: Send + Sync {
    /// Fill `buf` from `offset`; a short file is an error.
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<()>;
    fn len(&self) -> u64;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

pub trait Fs: Send + Sync + fmt::Debug {
    fn create_dir_all(&self, path: &Path) -> io::Result<()>;
    /// Create `path`, truncating it if it exists, for appending.
    fn create(&self, path: &Path) -> io::Result<Arc<dyn WritableFile>>;
    fn open_random(&self, path: &Path) -> io::Result<Arc<dyn RandomAccessFile>>;
    fn read(&self, path: &Path) -> io::Result<Vec<u8>>;
    /// Names of the files directly in `dir`.
    fn list(&self, dir: &Path) -> io::Result<Vec<String>>;
    fn exists(&self, path: &Path) -> bool;
    fn rename(&self, from: &Path, to: &Path) -> io::Result<()>;
    /// Rename `from` to `to` and open it to be written again from its first
    /// byte. What is not overwritten keeps its old bytes. Overwriting blocks
    /// a file already owns changes neither its size nor its extents, so an
    /// fdatasync of it flushes data and no file system metadata (RocksDB's
    /// `recycle_log_file_num`).
    fn reuse(&self, from: &Path, to: &Path) -> io::Result<Arc<dyn WritableFile>>;
    /// Make `to` name the same file as `from` (a hard link), or a copy of
    /// it where the file system cannot link (another device). The bytes of
    /// `to` are durable when this returns; its name is not until `sync_dir`.
    fn link(&self, from: &Path, to: &Path) -> io::Result<()>;
    fn remove(&self, path: &Path) -> io::Result<()>;
    /// Make every byte of the existing file at `path` durable, through a
    /// handle of its own (`fdatasync`): a file this process did not write,
    /// or no longer holds open.
    fn sync_file(&self, path: &Path) -> io::Result<()>;
    /// Make the creations, renames and removals in `dir` durable.
    fn sync_dir(&self, dir: &Path) -> io::Result<()>;
    /// An exclusive lock on `path`, held until the returned guard drops.
    fn lock(&self, path: &Path) -> io::Result<Box<dyn Send + Sync>>;
}

// ---------------------------------------------------------------- StdFs

#[derive(Debug, Default, Clone, Copy)]
pub struct StdFs;

struct StdFile {
    file: File,
    /// Bytes appended, by the one writer.
    written: AtomicU64,
    /// The file's size: what it had when opened, plus what
    /// `allocate_ahead` added. Meaningful only while `ahead` is not 0.
    allocated: AtomicU64,
    /// The chunk `allocate_ahead` asked for, 0 for none (or once the file
    /// system refused).
    ahead: AtomicU64,
}

impl StdFile {
    fn new(file: File, len: u64) -> Self {
        Self {
            file,
            written: AtomicU64::new(0),
            allocated: AtomicU64::new(len),
            ahead: AtomicU64::new(0),
        }
    }

    /// Allocate the blocks an append of `len` bytes is about to need, a
    /// chunk at a time.
    #[cfg(target_os = "linux")]
    fn allocate_for(&self, len: u64) {
        use std::os::fd::AsRawFd;
        let chunk = self.ahead.load(Ordering::Relaxed);
        if chunk == 0 {
            return;
        }
        let end = self.written.load(Ordering::Relaxed) + len;
        let allocated = self.allocated.load(Ordering::Relaxed);
        if end <= allocated {
            return;
        }
        let grow = (end - allocated).div_ceil(chunk) * chunk;
        // SAFETY: a plain syscall on a file descriptor this struct owns; it
        // reads no memory.
        let done = unsafe { libc::fallocate(self.file.as_raw_fd(), 0, allocated as _, grow as _) };
        if done == 0 {
            self.allocated.store(allocated + grow, Ordering::Relaxed);
        } else {
            // Not supported here (or no space: the append will say so).
            self.ahead.store(0, Ordering::Relaxed);
        }
    }

    #[cfg(not(target_os = "linux"))]
    fn allocate_for(&self, _len: u64) {}
}

impl WritableFile for StdFile {
    fn append(&self, data: &[u8]) -> io::Result<()> {
        self.allocate_for(data.len() as u64);
        (&self.file).write_all(data)?;
        self.written.fetch_add(data.len() as u64, Ordering::Relaxed);
        Ok(())
    }

    fn allocate_ahead(&self, chunk: u64) {
        self.ahead.store(chunk, Ordering::Relaxed);
    }

    fn sync(&self) -> io::Result<()> {
        self.file.sync_data()
    }

    #[cfg(target_os = "linux")]
    fn start_writeback(&self, offset: u64, len: u64) {
        use std::os::fd::AsRawFd;
        // SAFETY: a plain syscall on a file descriptor this struct owns; it
        // reads no memory. A failure only means less smoothing, so it is
        // ignored.
        unsafe {
            libc::sync_file_range(
                self.file.as_raw_fd(),
                offset as _,
                len as _,
                libc::SYNC_FILE_RANGE_WRITE,
            );
        }
    }
}

struct StdRandomFile {
    file: File,
    len: u64,
}

impl RandomAccessFile for StdRandomFile {
    #[cfg(unix)]
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<()> {
        use std::os::unix::fs::FileExt;
        self.file.read_exact_at(buf, offset)
    }

    #[cfg(windows)]
    fn read_at(&self, mut offset: u64, mut buf: &mut [u8]) -> io::Result<()> {
        use std::os::windows::fs::FileExt;
        while !buf.is_empty() {
            match self.file.seek_read(buf, offset)? {
                0 => return Err(io::ErrorKind::UnexpectedEof.into()),
                n => {
                    buf = &mut buf[n..];
                    offset += n as u64;
                }
            }
        }
        Ok(())
    }

    fn len(&self) -> u64 {
        self.len
    }
}

impl Fs for StdFs {
    fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        std::fs::create_dir_all(path)
    }

    fn open_random(&self, path: &Path) -> io::Result<Arc<dyn RandomAccessFile>> {
        let mut opts = OpenOptions::new();
        opts.read(true);
        // As on Unix, an open table can still be deleted (an iterator may
        // hold one that compaction made obsolete); otherwise the delete
        // fails and the file stays until the next open removes orphans.
        #[cfg(windows)]
        {
            use std::os::windows::fs::OpenOptionsExt;
            const FILE_SHARE_READ: u32 = 0x1;
            const FILE_SHARE_WRITE: u32 = 0x2;
            const FILE_SHARE_DELETE: u32 = 0x4;
            opts.share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE);
        }
        let file = opts.open(path)?;
        let len = file.metadata()?.len();
        Ok(Arc::new(StdRandomFile { file, len }))
    }

    fn create(&self, path: &Path) -> io::Result<Arc<dyn WritableFile>> {
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;
        Ok(Arc::new(StdFile::new(file, 0)))
    }

    fn read(&self, path: &Path) -> io::Result<Vec<u8>> {
        std::fs::read(path)
    }

    fn list(&self, dir: &Path) -> io::Result<Vec<String>> {
        let mut names = Vec::new();
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            if entry.file_type()?.is_file() {
                names.push(entry.file_name().to_string_lossy().into_owned());
            }
        }
        Ok(names)
    }

    fn exists(&self, path: &Path) -> bool {
        path.exists()
    }

    // Durable once the directory is synced, which every caller does next
    // (on Windows too, see `sync_dir`).
    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        std::fs::rename(from, to)
    }

    fn reuse(&self, from: &Path, to: &Path) -> io::Result<Arc<dyn WritableFile>> {
        std::fs::rename(from, to)?;
        let file = OpenOptions::new().write(true).open(to)?;
        let len = file.metadata()?.len();
        Ok(Arc::new(StdFile::new(file, len)))
    }

    fn link(&self, from: &Path, to: &Path) -> io::Result<()> {
        if std::fs::hard_link(from, to).is_ok() {
            return Ok(());
        }
        std::fs::copy(from, to)?;
        // A write handle: Windows flushes no file through a read-only one.
        OpenOptions::new().write(true).open(to)?.sync_all()
    }

    fn remove(&self, path: &Path) -> io::Result<()> {
        std::fs::remove_file(path)
    }

    fn sync_file(&self, path: &Path) -> io::Result<()> {
        // A write handle: Windows flushes no file through a read-only one.
        OpenOptions::new().write(true).open(path)?.sync_data()
    }

    #[cfg(unix)]
    fn sync_dir(&self, dir: &Path) -> io::Result<()> {
        File::open(dir)?.sync_all()
    }

    /// NTFS journals a directory's entries, but the journal reaches the
    /// disk on its own schedule: a file created or renamed just before a
    /// power loss could be missing after it. A directory opened with
    /// `FILE_FLAG_BACKUP_SEMANTICS` takes `FlushFileBuffers`, which writes
    /// its entries out, as an fsync of the directory does on Unix.
    ///
    /// A file system with nothing of the kind (FAT, some network shares and
    /// filter drivers) answers that the call is not supported: nothing more
    /// can be made durable there, as before, and it is not an error. Any
    /// other failure is.
    #[cfg(windows)]
    fn sync_dir(&self, dir: &Path) -> io::Result<()> {
        use std::os::windows::fs::OpenOptionsExt;
        const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x0200_0000;
        const ERROR_INVALID_FUNCTION: i32 = 1;
        const ERROR_NOT_SUPPORTED: i32 = 50;
        let flushed = OpenOptions::new()
            .write(true)
            .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
            .open(dir)
            .and_then(|d| d.sync_all());
        match flushed {
            Err(e)
                if matches!(
                    e.raw_os_error(),
                    Some(ERROR_INVALID_FUNCTION | ERROR_NOT_SUPPORTED)
                ) =>
            {
                Ok(())
            }
            other => other,
        }
    }

    #[cfg(not(any(unix, windows)))]
    fn sync_dir(&self, _dir: &Path) -> io::Result<()> {
        Ok(())
    }

    fn lock(&self, path: &Path) -> io::Result<Box<dyn Send + Sync>> {
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        file.try_lock().map_err(|e| match e {
            std::fs::TryLockError::WouldBlock => io::Error::new(
                io::ErrorKind::WouldBlock,
                format!(
                    "{} is locked: another process has this database open",
                    path.display()
                ),
            ),
            std::fs::TryLockError::Error(e) => e,
        })?;
        Ok(Box::new(file))
    }
}

// ---------------------------------------------------------------- FaultFs

#[derive(Default)]
struct Node {
    data: Vec<u8>,
    /// Bytes `sync` has made durable; a crash keeps at most these (plus a
    /// torn tail, if asked for).
    synced: usize,
}

type NodeRef = Arc<Mutex<Node>>;

#[derive(Default)]
struct State {
    /// Bumped by every crash; handles from an earlier generation are dead.
    generation: u64,
    /// The namespace as the running process sees it.
    live: BTreeMap<PathBuf, NodeRef>,
    /// The namespace as it was at each directory's last `sync_dir`.
    durable: BTreeMap<PathBuf, NodeRef>,
    dirs: BTreeSet<PathBuf>,
    failing_syncs: u32,
    syncs: u64,
    sync_delay: std::time::Duration,
    /// Mutating operations left before a crash ([`FaultFs::crash_after`]).
    countdown: Option<u64>,
    countdown_torn: Option<Torn>,
}

/// An in-memory file system that can crash. Cloning shares the same files.
///
/// A handle from [`FaultFs::process`] belongs to one "process": once the
/// file system crashes, every operation through it fails, as nothing a dead
/// process does reaches the disk. Other handles see the file system as an
/// observer would and keep working across crashes.
#[derive(Clone, Default)]
pub struct FaultFs {
    state: Arc<Mutex<State>>,
    process: Option<u64>,
}

impl fmt::Debug for FaultFs {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("FaultFs")
    }
}

/// What a crash leaves of the bytes that were written but not synced.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Torn {
    /// Nothing: every file ends at its last sync.
    None,
    /// A pseudo-random prefix of the unsynced bytes survives, and with it
    /// sometimes a few garbage bytes, as a write cut short by power loss can
    /// leave. Deterministic for a given seed.
    Seeded(u64),
}

struct FaultFile {
    node: NodeRef,
    state: Arc<Mutex<State>>,
    generation: u64,
    /// Where the next append writes: past the end for a new file, over old
    /// bytes for a reused one.
    pos: Mutex<usize>,
}

fn crashed() -> io::Error {
    io::Error::other("FaultFs: this handle belongs to a crashed process")
}

impl WritableFile for FaultFile {
    fn append(&self, data: &[u8]) -> io::Result<()> {
        {
            let mut state = self.state.lock();
            if state.generation != self.generation {
                return Err(crashed());
            }
            tick_countdown(&mut state)?;
        }
        let mut pos = self.pos.lock();
        let mut node = self.node.lock();
        let end = *pos + data.len();
        if node.data.len() < end {
            node.data.resize(end, 0);
        }
        node.data[*pos..end].copy_from_slice(data);
        *pos = end;
        Ok(())
    }

    fn sync(&self) -> io::Result<()> {
        let delay = {
            let mut state = self.state.lock();
            if state.generation != self.generation {
                return Err(crashed());
            }
            tick_countdown(&mut state)?;
            state.syncs += 1;
            if state.failing_syncs > 0 {
                state.failing_syncs -= 1;
                return Err(io::Error::other("FaultFs: injected EIO on fsync"));
            }
            state.sync_delay
        };
        if !delay.is_zero() {
            std::thread::sleep(delay);
        }
        // The fsync takes effect under the state lock, after checking again
        // that no crash came in between: otherwise a sync racing another
        // thread's crash returned Ok and was acknowledged, while what it
        // synced was not in the post-crash state. A dead process cannot
        // acknowledge anything; the crash sweep reported it as lost data.
        let state = self.state.lock();
        if state.generation != self.generation {
            return Err(crashed());
        }
        let pos = *self.pos.lock();
        let mut node = self.node.lock();
        node.synced = node.synced.max(pos);
        drop(state);
        Ok(())
    }
}

fn crash_locked(state: &mut State, torn: Torn) {
    state.generation += 1;
    let mut seed = match torn {
        Torn::None => 0,
        Torn::Seeded(seed) => seed | 1,
    };
    let mut survivors = BTreeMap::new();
    for (path, node) in &state.durable {
        let node = node.lock();
        let mut data = node.data[..node.synced].to_vec();
        let unsynced = &node.data[node.synced..];
        if seed != 0 && !unsynced.is_empty() {
            let keep = (next_random(&mut seed) % (unsynced.len() as u64 + 1)) as usize;
            data.extend_from_slice(&unsynced[..keep]);
            if next_random(&mut seed).is_multiple_of(4) {
                let garbage = (next_random(&mut seed) % 16) as usize;
                data.extend((0..garbage).map(|_| next_random(&mut seed) as u8));
            }
        }
        let synced = data.len();
        survivors.insert(path.clone(), Arc::new(Mutex::new(Node { data, synced })));
    }
    state.live = survivors.clone();
    state.durable = survivors;
}

fn tick_countdown(state: &mut State) -> io::Result<()> {
    match state.countdown {
        Some(0) => {
            state.countdown = None;
            let torn = state.countdown_torn.unwrap_or(Torn::None);
            crash_locked(state, torn);
            Err(crashed())
        }
        Some(n) => {
            state.countdown = Some(n - 1);
            Ok(())
        }
        None => Ok(()),
    }
}

/// xorshift64*: enough randomness for torn tails without a dependency.
fn next_random(seed: &mut u64) -> u64 {
    *seed ^= *seed >> 12;
    *seed ^= *seed << 25;
    *seed ^= *seed >> 27;
    seed.wrapping_mul(0x2545_f491_4f6c_dd1d)
}

impl FaultFs {
    pub fn new() -> Self {
        Self::default()
    }

    /// Simulate power loss: every file not reachable through a synced
    /// directory entry disappears, and every file loses its unsynced bytes
    /// (all of them, or all but a torn prefix). Handles opened before the
    /// crash fail from now on.
    pub fn crash(&self, torn: Torn) {
        let mut state = self.state.lock();
        // A crash ends the run a countdown was set for.
        state.countdown = None;
        crash_locked(&mut state, torn);
    }

    /// Simulate a process crash: handles opened before it fail from now
    /// on, but nothing is lost. Unsynced bytes stay in the page cache, as
    /// they do when only the process dies.
    pub fn kill(&self) {
        let mut state = self.state.lock();
        state.countdown = None;
        state.generation += 1;
    }

    /// Crash on the `ops`-th mutating operation from now (appends, fsyncs,
    /// creations, renames, links, removals, directory syncs): that operation
    /// fails, and so does everything a process handle does after it.
    pub fn crash_after(&self, ops: u64, torn: Torn) {
        let mut state = self.state.lock();
        state.countdown = Some(ops);
        state.countdown_torn = Some(torn);
    }

    /// Whether a [`FaultFs::crash_after`] countdown has fired.
    pub fn countdown_fired(&self) -> bool {
        let state = self.state.lock();
        state.countdown.is_none() && state.countdown_torn.is_some()
    }

    /// A handle for one process, dead after the next crash.
    pub fn process(&self) -> FaultFs {
        FaultFs {
            state: self.state.clone(),
            process: Some(self.state.lock().generation),
        }
    }

    /// Account for a mutating operation; fails if this handle's process is
    /// dead or the countdown fires now.
    fn tick(&self, state: &mut State) -> io::Result<()> {
        if self.process.is_some_and(|g| g != state.generation) {
            return Err(crashed());
        }
        tick_countdown(state)
    }

    /// The next `n` fsyncs fail with EIO (and make nothing durable).
    pub fn fail_next_syncs(&self, n: u32) {
        self.state.lock().failing_syncs = n;
    }

    /// Make every fsync take this long, as a disk's would: commits then
    /// arrive while one is in flight, which is what group commit is for.
    pub fn set_sync_delay(&self, delay: std::time::Duration) {
        self.state.lock().sync_delay = delay;
    }

    /// File fsyncs so far (directory syncs not counted).
    pub fn sync_count(&self) -> u64 {
        self.state.lock().syncs
    }

    fn not_found(path: &Path) -> io::Error {
        io::Error::new(
            io::ErrorKind::NotFound,
            format!("FaultFs: {} not found", path.display()),
        )
    }
}

/// Reads see the file as it was when opened: tables are immutable once
/// written, which is the only way this crate reads at offsets.
struct FaultRandomFile(Vec<u8>);

impl RandomAccessFile for FaultRandomFile {
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<()> {
        let start =
            usize::try_from(offset).map_err(|_| io::Error::from(io::ErrorKind::UnexpectedEof))?;
        let src = start
            .checked_add(buf.len())
            .and_then(|end| self.0.get(start..end))
            .ok_or_else(|| io::Error::from(io::ErrorKind::UnexpectedEof))?;
        buf.copy_from_slice(src);
        Ok(())
    }

    fn len(&self) -> u64 {
        self.0.len() as u64
    }
}

impl Fs for FaultFs {
    fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        self.state.lock().dirs.insert(path.to_path_buf());
        Ok(())
    }

    fn open_random(&self, path: &Path) -> io::Result<Arc<dyn RandomAccessFile>> {
        Ok(Arc::new(FaultRandomFile(self.read(path)?)))
    }

    fn create(&self, path: &Path) -> io::Result<Arc<dyn WritableFile>> {
        let mut state = self.state.lock();
        self.tick(&mut state)?;
        let node: NodeRef = Arc::default();
        state.live.insert(path.to_path_buf(), node.clone());
        Ok(Arc::new(FaultFile {
            node,
            state: self.state.clone(),
            generation: state.generation,
            pos: Mutex::new(0),
        }))
    }

    fn read(&self, path: &Path) -> io::Result<Vec<u8>> {
        let state = self.state.lock();
        let node = state.live.get(path).ok_or_else(|| Self::not_found(path))?;
        let data = node.lock().data.clone();
        Ok(data)
    }

    fn list(&self, dir: &Path) -> io::Result<Vec<String>> {
        let state = self.state.lock();
        Ok(state
            .live
            .keys()
            .filter(|p| p.parent() == Some(dir))
            .filter_map(|p| p.file_name().map(|n| n.to_string_lossy().into_owned()))
            .collect())
    }

    fn exists(&self, path: &Path) -> bool {
        let state = self.state.lock();
        state.live.contains_key(path) || state.dirs.contains(path)
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        let mut state = self.state.lock();
        self.tick(&mut state)?;
        let node = state
            .live
            .remove(from)
            .ok_or_else(|| Self::not_found(from))?;
        state.live.insert(to.to_path_buf(), node);
        Ok(())
    }

    fn reuse(&self, from: &Path, to: &Path) -> io::Result<Arc<dyn WritableFile>> {
        let mut state = self.state.lock();
        self.tick(&mut state)?;
        let node = state
            .live
            .remove(from)
            .ok_or_else(|| Self::not_found(from))?;
        // Nothing written from here on is durable until synced; the stale
        // bytes behind it may or may not survive a crash.
        node.lock().synced = 0;
        state.live.insert(to.to_path_buf(), node.clone());
        Ok(Arc::new(FaultFile {
            node,
            state: self.state.clone(),
            generation: state.generation,
            pos: Mutex::new(0),
        }))
    }

    fn link(&self, from: &Path, to: &Path) -> io::Result<()> {
        let mut state = self.state.lock();
        self.tick(&mut state)?;
        let node = state
            .live
            .get(from)
            .cloned()
            .ok_or_else(|| Self::not_found(from))?;
        state.live.insert(to.to_path_buf(), node);
        Ok(())
    }

    fn remove(&self, path: &Path) -> io::Result<()> {
        let mut state = self.state.lock();
        self.tick(&mut state)?;
        state
            .live
            .remove(path)
            .map(|_| ())
            .ok_or_else(|| Self::not_found(path))
    }

    fn sync_file(&self, path: &Path) -> io::Result<()> {
        let mut state = self.state.lock();
        self.tick(&mut state)?;
        state.syncs += 1;
        if state.failing_syncs > 0 {
            state.failing_syncs -= 1;
            return Err(io::Error::other("FaultFs: injected EIO on fsync"));
        }
        let node = state
            .live
            .get(path)
            .cloned()
            .ok_or_else(|| Self::not_found(path))?;
        let mut node = node.lock();
        node.synced = node.data.len();
        Ok(())
    }

    fn sync_dir(&self, dir: &Path) -> io::Result<()> {
        let mut state = self.state.lock();
        self.tick(&mut state)?;
        let in_dir = |p: &PathBuf| p.parent() == Some(dir);
        state.durable.retain(|p, _| !in_dir(p));
        let entries: Vec<_> = state
            .live
            .iter()
            .filter(|(p, _)| in_dir(p))
            .map(|(p, n)| (p.clone(), n.clone()))
            .collect();
        state.durable.extend(entries);
        Ok(())
    }

    fn lock(&self, _path: &Path) -> io::Result<Box<dyn Send + Sync>> {
        Ok(Box::new(()))
    }
}

#[cfg(test)]
mod std_fs_tests {
    use super::*;

    /// On every platform CI runs: on Windows, a directory takes
    /// FlushFileBuffers through FILE_FLAG_BACKUP_SEMANTICS, and a rename
    /// replaces its target.
    #[test]
    fn a_directory_syncs_and_a_rename_replaces_its_target() {
        let dir = tempfile::tempdir().unwrap();
        let fs = StdFs;
        let (a, b) = (dir.path().join("a"), dir.path().join("b"));
        let f = fs.create(&a).unwrap();
        f.append(b"new").unwrap();
        f.sync().unwrap();
        std::fs::write(&b, b"old").unwrap();
        fs.rename(&a, &b).unwrap();
        fs.sync_dir(dir.path()).unwrap();
        assert!(!a.exists());
        assert_eq!(std::fs::read(&b).unwrap(), b"new");
        // `reuse` renames too, then writes the file again from its start.
        let c = dir.path().join("c");
        let w = fs.reuse(&b, &c).unwrap();
        w.append(b"N").unwrap();
        w.sync().unwrap();
        fs.sync_dir(dir.path()).unwrap();
        assert_eq!(std::fs::read(&c).unwrap(), b"New");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A file allocated ahead grows a chunk at a time and keeps what is
    /// appended at its start; reused, it grows again only past its old size.
    #[cfg(target_os = "linux")]
    #[test]
    fn a_file_allocated_ahead_grows_by_chunks() {
        let dir = tempfile::tempdir().unwrap();
        let fs = StdFs;
        let path = dir.path().join("wal");
        let len = |p: &Path| std::fs::metadata(p).unwrap().len();

        let plain = fs.create(&dir.path().join("plain")).unwrap();
        plain.append(&[7; 100]).unwrap();
        assert_eq!(len(&dir.path().join("plain")), 100);

        let f = fs.create(&path).unwrap();
        f.allocate_ahead(1 << 16);
        f.append(&[1; 100]).unwrap();
        if len(&path) == 100 {
            // The file system of this temporary directory has no fallocate.
            return;
        }
        assert_eq!(len(&path), 1 << 16);
        f.append(&[2; 1 << 16]).unwrap();
        assert_eq!(len(&path), 2 << 16);
        // One append larger than a chunk gets every chunk it needs.
        f.append(&[3; 3 << 16]).unwrap();
        assert_eq!(len(&path), 5 << 16);
        f.sync().unwrap();
        let data = std::fs::read(&path).unwrap();
        assert!(data[..100].iter().all(|&b| b == 1));
        assert!(data[100..100 + (1 << 16)].iter().all(|&b| b == 2));
        assert!(data[100 + (4 << 16)..].iter().all(|&b| b == 0));

        let again = dir.path().join("wal2");
        let g = fs.reuse(&path, &again).unwrap();
        g.allocate_ahead(1 << 16);
        g.append(&[9; 5 << 16]).unwrap();
        assert_eq!(len(&again), 5 << 16, "written over, not grown");
        g.append(&[9; 1]).unwrap();
        assert_eq!(len(&again), 6 << 16);
    }

    #[test]
    fn a_reused_file_keeps_what_was_synced_after_the_reuse() {
        let fs = FaultFs::new();
        let dir = Path::new("/db");
        fs.create_dir_all(dir).unwrap();
        let old = fs.create(&dir.join("87")).unwrap();
        old.append(b"old wal content").unwrap();
        old.sync().unwrap();
        fs.sync_dir(dir).unwrap();

        let new = fs.reuse(&dir.join("87"), &dir.join("96")).unwrap();
        new.append(b"NEW").unwrap();
        fs.sync_dir(dir).unwrap();
        new.sync().unwrap();
        // The old handle is still around (a WAL kept for a deferred fsync).
        old.sync().unwrap();

        fs.crash(Torn::None);
        let data = fs.read(&dir.join("96")).unwrap();
        assert!(
            data.starts_with(b"NEW"),
            "{:?}",
            String::from_utf8_lossy(&data)
        );
    }

    #[test]
    fn a_crash_keeps_synced_bytes_and_synced_names_only() {
        let fs = FaultFs::new();
        let dir = Path::new("/db");
        fs.create_dir_all(dir).unwrap();

        let a = fs.create(&dir.join("a")).unwrap();
        a.append(b"durable").unwrap();
        a.sync().unwrap();
        a.append(b" lost").unwrap();
        fs.sync_dir(dir).unwrap();
        // Created after the directory sync: its name is not durable.
        let b = fs.create(&dir.join("b")).unwrap();
        b.append(b"x").unwrap();
        b.sync().unwrap();

        fs.crash(Torn::None);
        assert_eq!(fs.read(&dir.join("a")).unwrap(), b"durable");
        assert!(!fs.exists(&dir.join("b")));
        assert!(
            a.append(b"late").is_err(),
            "a pre-crash handle must be dead"
        );
    }

    #[test]
    fn a_rename_is_durable_only_after_the_directory_sync() {
        let fs = FaultFs::new();
        let dir = Path::new("/db");
        let f = fs.create(&dir.join("tmp")).unwrap();
        f.append(b"v1").unwrap();
        f.sync().unwrap();
        fs.sync_dir(dir).unwrap();
        fs.rename(&dir.join("tmp"), &dir.join("CURRENT")).unwrap();
        fs.crash(Torn::None);
        assert!(fs.exists(&dir.join("tmp")));
        assert!(!fs.exists(&dir.join("CURRENT")));
    }

    #[test]
    fn injected_sync_failures_make_nothing_durable() {
        let fs = FaultFs::new();
        let dir = Path::new("/db");
        let f = fs.create(&dir.join("a")).unwrap();
        fs.sync_dir(dir).unwrap();
        f.append(b"data").unwrap();
        fs.fail_next_syncs(1);
        assert!(f.sync().is_err());
        fs.crash(Torn::None);
        assert_eq!(fs.read(&dir.join("a")).unwrap(), b"");
    }

    #[test]
    fn a_torn_crash_keeps_a_prefix_of_the_unsynced_bytes() {
        for seed in 0..64 {
            let fs = FaultFs::new();
            let dir = Path::new("/db");
            let f = fs.create(&dir.join("a")).unwrap();
            fs.sync_dir(dir).unwrap();
            f.append(b"synced|").unwrap();
            f.sync().unwrap();
            f.append(b"unsynced tail").unwrap();
            fs.crash(Torn::Seeded(seed));
            let data = fs.read(&dir.join("a")).unwrap();
            assert!(data.starts_with(b"synced|"));
            let tail = &data[7..];
            let real = tail
                .iter()
                .zip(b"unsynced tail")
                .take_while(|(a, b)| a == b)
                .count();
            assert!(real <= 13 && tail.len() <= real + 16);
        }
    }
}
