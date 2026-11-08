//! Table files: an immutable, sorted run of internal keys.
//!
//! ```text
//! table   := data block* | filter | properties | index | footer
//! each block on disk := contents | type u8 | crc32c u32 (of contents and type)
//!   type 0: contents is the block;  type 1: varint raw length | LZ4 block
//! index   := a block with one entry per data block: a separator key, at or
//!            after every key of the block and before every key of the
//!            next one, mapped to the block's handle (offset varint,
//!            length varint). The last block's separator is its last key.
//! footer  := index handle | filter handle | properties handle   (u64 LE pairs)
//!            | format version u32 | crc32c u32 (of the 52 bytes before) | magic u64
//! ```
//!
//! Opening a table reads its footer, index, filter and properties and keeps
//! them in memory; data blocks are read with `pread` when needed. Every byte
//! is checksummed before it is decoded.

use std::cell::RefCell;
use std::cmp::Ordering;
use std::sync::Arc;

use bytes::Bytes;

use super::block::{Block, BlockBuilder, BlockIter, RESTART_INTERVAL};
use super::bloom;
use super::cache::BlockCache;
use crate::coding::{get_varint, put_varint};
use crate::error::{Error, Result};
use crate::fs::{RandomAccessFile, WritableFile};
use crate::iter::InternalIterator;
use crate::key::{self, Kind};
use crate::memtable::Version;

pub const BLOCK_SIZE: usize = 4096;
/// Index entries stored whole every this many; those between share their
/// prefix with the one before. Readers take any interval (the restart
/// array is in the block), so old binaries read these tables.
///
/// Measured on the quick bench (1M documents, two runs each), against 1:
/// at 4, tables 1.6% smaller and point reads and scans as fast or a little
/// faster (a smaller index leaves more cache); 8 saves no more; at 16,
/// scans lose 3.5% and hot point reads 5%, for 0.4% less disk.
const INDEX_RESTART_INTERVAL: usize = 4;
const FOOTER_LEN: usize = 64;
const MAGIC: u64 = 0x736f_6c69_7374_6f72; // "solistor"
const FORMAT_VERSION: u32 = 1;
const TYPE_RAW: u8 = 0;
const TYPE_LZ4: u8 = 1;
const BLOCK_TRAILER: usize = 5;
/// Buffer this much before writing to the file.
const WRITE_CHUNK: usize = 1 << 20;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Handle {
    pub offset: u64,
    pub len: u64,
}

impl Handle {
    fn encode(&self, buf: &mut Vec<u8>) {
        put_varint(buf, self.offset);
        put_varint(buf, self.len);
    }

    fn decode(mut input: &[u8]) -> Result<Handle> {
        let corrupt = || Error::Corruption("malformed block handle".into());
        Ok(Handle {
            offset: get_varint(&mut input).ok_or_else(corrupt)?,
            len: get_varint(&mut input).ok_or_else(corrupt)?,
        })
    }
}

/// What a finished table holds, as the MANIFEST records it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableProps {
    pub size: u64,
    pub smallest: Vec<u8>,
    pub largest: Vec<u8>,
    pub smallest_seq: u64,
    pub largest_seq: u64,
    pub entries: u64,
    /// The highest range-tombstone sequence already applied when the file
    /// was written: no entry in it is covered by a tombstone at or below
    /// this. Set by whoever wrote the file (flush or compaction).
    pub tombstone_watermark: u64,
}

pub struct TableBuilder {
    file: Arc<dyn WritableFile>,
    out: Vec<u8>,
    offset: u64,
    data: BlockBuilder,
    index: BlockBuilder,
    /// The block just written, whose index entry waits for the next key:
    /// its separator is chosen between the two.
    pending: Option<Handle>,
    hashes: Vec<u64>,
    last_key: Vec<u8>,
    smallest: Option<Vec<u8>>,
    smallest_seq: u64,
    largest_seq: u64,
    entries: u64,
    deletions: u64,
    compress: bool,
    /// Reused for every block's compressed form.
    scratch: Vec<u8>,
    /// Bytes appended to the file so far.
    written: u64,
    /// Start write-back as chunks are written ([`TableBuilder::with_writeback`]).
    writeback: bool,
}

impl TableBuilder {
    pub fn new(file: Arc<dyn WritableFile>) -> Self {
        TableBuilder {
            file,
            out: Vec::with_capacity(WRITE_CHUNK + BLOCK_SIZE * 2),
            offset: 0,
            data: BlockBuilder::new(RESTART_INTERVAL),
            index: BlockBuilder::new(INDEX_RESTART_INTERVAL),
            pending: None,
            hashes: Vec::new(),
            last_key: Vec::new(),
            smallest: None,
            smallest_seq: u64::MAX,
            largest_seq: 0,
            entries: 0,
            deletions: 0,
            compress: true,
            scratch: Vec::new(),
            written: 0,
            writeback: false,
        }
    }

    /// Leave data blocks uncompressed.
    pub fn uncompressed(mut self) -> Self {
        self.compress = false;
        self
    }

    pub fn with_writeback(mut self, on: bool) -> Self {
        self.writeback = on;
        self
    }

    /// Internal keys must be added in order, each once.
    pub fn add(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        debug_assert!(
            self.last_key.is_empty() || key::compare(&self.last_key, key) == Ordering::Less
        );
        let parsed = key::parse(key).ok_or_else(|| Error::Corruption("bad internal key".into()))?;
        if self.last_key.is_empty()
            || key::cf_and_user_key(&self.last_key) != key::cf_and_user_key(key)
        {
            self.hashes.push(bloom::hash(key::cf_and_user_key(key)));
        }
        if self.smallest.is_none() {
            self.smallest = Some(key.to_vec());
        }
        if let Some(handle) = self.pending.take() {
            let separator = key::separator(&self.last_key, key);
            add_index_entry(
                &mut self.index,
                separator.as_deref().unwrap_or(&self.last_key),
                handle,
            );
        }
        self.smallest_seq = self.smallest_seq.min(parsed.seq);
        self.largest_seq = self.largest_seq.max(parsed.seq);
        self.entries += 1;
        if parsed.kind == Kind::Delete {
            self.deletions += 1;
        }
        self.data.add(key, value);
        self.last_key.clear();
        self.last_key.extend_from_slice(key);
        if self.data.size_estimate() >= BLOCK_SIZE {
            self.finish_data_block()?;
        }
        Ok(())
    }

    /// Bytes written so far, plus the open data block.
    pub fn file_size(&self) -> u64 {
        self.offset + self.data.size_estimate() as u64
    }

    fn finish_data_block(&mut self) -> Result<()> {
        if self.data.is_empty() {
            return Ok(());
        }
        let contents = self.data.finish();
        self.pending = Some(self.write_block(&contents, self.compress)?);
        Ok(())
    }

    fn write_block(&mut self, contents: &[u8], compress: bool) -> Result<Handle> {
        let start = self.out.len();
        let mut kind = TYPE_RAW;
        if compress {
            let max = lz4_flex::block::get_maximum_output_size(contents.len());
            if self.scratch.len() < max {
                self.scratch.resize(max, 0);
            }
            let compressed = lz4_flex::block::compress_into(contents, &mut self.scratch)
                .map_err(|e| Error::Corruption(format!("LZ4: {e}")))?;
            // Not worth decompressing for less than an eighth saved.
            if compressed + 5 < contents.len() - contents.len() / 8 {
                put_varint(&mut self.out, contents.len() as u64);
                self.out.extend_from_slice(&self.scratch[..compressed]);
                kind = TYPE_LZ4;
            }
        }
        if kind == TYPE_RAW {
            self.out.extend_from_slice(contents);
        }
        self.out.push(kind);
        let crc = crc32c::crc32c(&self.out[start..]);
        self.out.extend_from_slice(&crc.to_le_bytes());
        let len = (self.out.len() - start - BLOCK_TRAILER) as u64;
        let handle = Handle {
            offset: self.offset,
            len,
        };
        self.offset += (len as usize + BLOCK_TRAILER) as u64;
        if self.out.len() >= WRITE_CHUNK {
            self.file.append(&self.out)?;
            // RocksDB's bytes_per_sync, when asked for: start the write-back
            // of each chunk as it is written instead of in one burst at the
            // final fsync. Off by default, as in RocksDB and SoliDB's
            // profile: it keeps the device queue busy, and a WAL fsync
            // queued behind it waited milliseconds.
            let len = self.out.len() as u64;
            if self.writeback {
                self.file.start_writeback(self.written, len);
            }
            self.written += len;
            self.out.clear();
        }
        Ok(handle)
    }

    /// Write the filter, properties, index and footer, and fsync the file.
    pub fn finish(mut self) -> Result<TableProps> {
        self.finish_data_block()?;
        // No key follows the last block: its last key stands. A shorter
        // successor would save a few bytes per file and make every seek past
        // the file's end read its last block.
        if let Some(handle) = self.pending.take() {
            add_index_entry(&mut self.index, &self.last_key, handle);
        }
        let filter = self.write_block(&bloom::build(&self.hashes), false)?;
        let mut props = Vec::new();
        for v in [
            self.entries,
            self.deletions,
            self.smallest_seq,
            self.largest_seq,
        ] {
            put_varint(&mut props, v);
        }
        let props = self.write_block(&props, false)?;
        let index_contents = self.index.finish();
        let index = self.write_block(&index_contents, false)?;

        let mut footer = Vec::with_capacity(FOOTER_LEN);
        for h in [index, filter, props] {
            footer.extend_from_slice(&h.offset.to_le_bytes());
            footer.extend_from_slice(&h.len.to_le_bytes());
        }
        footer.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
        let crc = crc32c::crc32c(&footer);
        footer.extend_from_slice(&crc.to_le_bytes());
        footer.extend_from_slice(&MAGIC.to_le_bytes());
        self.out.extend_from_slice(&footer);
        self.offset += FOOTER_LEN as u64;
        self.file.append(&self.out)?;
        self.file.sync()?;
        Ok(TableProps {
            size: self.offset,
            smallest: self.smallest.unwrap_or_default(),
            largest: self.last_key,
            smallest_seq: if self.entries == 0 {
                0
            } else {
                self.smallest_seq
            },
            largest_seq: self.largest_seq,
            entries: self.entries,
            tombstone_watermark: 0,
        })
    }
}

fn add_index_entry(index: &mut BlockBuilder, separator: &[u8], handle: Handle) {
    let mut encoded = Vec::new();
    handle.encode(&mut encoded);
    index.add(separator, &encoded);
}

pub struct TableReader {
    file: Arc<dyn RandomAccessFile>,
    index: Block,
    /// Where the data blocks end (the filter follows them).
    data_end: u64,
    filter: Bytes,
    /// Bytes held in memory for this table (index and filter).
    pinned: usize,
    /// The shared block cache and this table's number in it.
    cache: Option<(Arc<BlockCache>, u64)>,
}

fn read_block(file: &dyn RandomAccessFile, handle: Handle) -> Result<Bytes> {
    let corrupt =
        |what: &str| Error::Corruption(format!("table block at {}: {what}", handle.offset));
    // Checked: a handle saying a length near the top of the range wrapped
    // past the trailer in release builds and panicked further on.
    let total = usize::try_from(handle.len)
        .ok()
        .and_then(|len| len.checked_add(BLOCK_TRAILER))
        .ok_or_else(|| corrupt("length"))?;
    if handle.offset.saturating_add(total as u64) > file.len() {
        return Err(corrupt("past the end of the file"));
    }
    let mut buf = vec![0u8; total];
    file.read_at(handle.offset, &mut buf)?;
    let (body, crc) = buf.split_at(total - 4);
    if crc32c::crc32c(body) != u32::from_le_bytes(crc.try_into().expect("4 bytes")) {
        return Err(corrupt("checksum mismatch"));
    }
    let (contents, kind) = body.split_at(body.len() - 1);
    match kind[0] {
        TYPE_RAW => {
            buf.truncate(contents.len());
            Ok(Bytes::from(buf))
        }
        TYPE_LZ4 => {
            let mut input = contents;
            let raw_len = get_varint(&mut input).ok_or_else(|| corrupt("length"))? as usize;
            // LZ4 expands at most ~255x: a longer claim cannot be ours, and
            // must not size an allocation. A fixed cap (64 MiB) here refused
            // blocks the writer had written: one value above it made a
            // compaction fail and poisoned the database.
            if raw_len > input.len().saturating_mul(255).saturating_add(64) {
                return Err(corrupt("implausible length"));
            }
            let raw = lz4_flex::block::decompress(input, raw_len).map_err(|_| corrupt("LZ4"))?;
            Ok(Bytes::from(raw))
        }
        _ => Err(corrupt("unknown block type")),
    }
}

impl TableReader {
    pub fn open(file: Arc<dyn RandomAccessFile>) -> Result<TableReader> {
        let len = file.len();
        if len < FOOTER_LEN as u64 {
            return Err(Error::Corruption("table shorter than its footer".into()));
        }
        let mut footer = [0u8; FOOTER_LEN];
        file.read_at(len - FOOTER_LEN as u64, &mut footer)?;
        let u64_at = |i: usize| u64::from_le_bytes(footer[i..i + 8].try_into().expect("8 bytes"));
        let crc = u32::from_le_bytes(footer[52..56].try_into().expect("4 bytes"));
        if u64_at(56) != MAGIC || crc32c::crc32c(&footer[..52]) != crc {
            return Err(Error::Corruption("bad table footer".into()));
        }
        let version = u32::from_le_bytes(footer[48..52].try_into().expect("4 bytes"));
        if version != FORMAT_VERSION {
            return Err(Error::Corruption(format!(
                "unknown table format version {version}"
            )));
        }
        let index = Handle {
            offset: u64_at(0),
            len: u64_at(8),
        };
        let filter = Handle {
            offset: u64_at(16),
            len: u64_at(24),
        };
        let filter_at = filter.offset;
        let index = Block::new(read_block(file.as_ref(), index)?)?;
        let filter = read_block(file.as_ref(), filter)?;
        let pinned = index.size() + filter.len();
        Ok(TableReader {
            cache: None,
            file,
            index,
            data_end: filter_at,
            filter,
            pinned,
        })
    }

    /// Serve data blocks through `cache`, where this table is `number`.
    pub fn with_cache(mut self, cache: Arc<BlockCache>, number: u64) -> Self {
        cache.charge(self.pinned as u64);
        self.cache = Some((cache, number));
        self
    }

    /// A data block, from the cache when it is there. `fill` says whether
    /// to keep a block read from disk: compactions read everything once
    /// and would only push out what readers need.
    fn data_block(&self, handle_bytes: &[u8], fill: bool) -> Result<Block> {
        self.data_block_at(Handle::decode(handle_bytes)?, fill)
    }

    /// A data block if the cache holds it; nothing is read.
    fn cached_block(&self, handle: Handle) -> Option<Block> {
        let (cache, number) = self.cache.as_ref()?;
        cache.get(*number, handle.offset)
    }

    fn data_block_at(&self, handle: Handle, fill: bool) -> Result<Block> {
        if let Some((cache, number)) = &self.cache {
            if let Some(block) = cache.get(*number, handle.offset) {
                return Ok(block);
            }
            let block = Block::new(read_block(self.file.as_ref(), handle)?)?;
            if fill {
                cache.insert(*number, handle.offset, block.clone());
            }
            return Ok(block);
        }
        Block::new(read_block(self.file.as_ref(), handle)?)
    }

    /// Where in the file the data for internal keys at or after `target`
    /// begins: the first data block that may hold one, or the end of the
    /// data. Read from the index alone.
    pub fn approximate_offset_of(&self, target: &[u8]) -> Result<u64> {
        let mut index = self.index.iter();
        index.seek(target);
        index.status()?;
        if !index.valid() {
            return Ok(self.data_end);
        }
        Ok(Handle::decode(index.value_slice())?.offset)
    }

    /// Samples of where the data of `[lo, hi)` (`cf ‖ user key`) lies, for
    /// cutting it into parts of equal size: one per index restart point
    /// (every [`INDEX_RESTART_INTERVAL`] blocks) inside the range, its
    /// separator's `cf ‖ user key` with the decoded bytes of the data blocks
    /// since the sample before it, at [`BLOCK_SIZE`] a block (each is cut
    /// once that full: decoded bytes compare with a memtable's, where bytes
    /// on disk would depend on compression). Read from the index alone.
    pub fn size_samples<'a>(
        &'a self,
        lo: &[u8],
        hi: &[u8],
        out: &mut Vec<(&'a [u8], u64)>,
    ) -> Result<()> {
        // The separator at a restart point ends the first block of its
        // interval: since the sample before, the blocks of that interval.
        let mut blocks = 1;
        for entry in self.index.restart_keys() {
            let (separator, entries) = entry?;
            let id = key::cf_and_user_key(separator);
            if key::cmp_bytes(id, lo).is_gt() && key::cmp_bytes(id, hi).is_lt() {
                out.push((id, (blocks * BLOCK_SIZE) as u64));
            }
            if key::cmp_bytes(id, hi).is_ge() {
                break;
            }
            blocks = entries;
        }
        Ok(())
    }

    /// The newest version of `user_key` at most `snapshot`, if this table has one.
    #[cfg(any(test, feature = "fuzzing"))]
    pub fn get(&self, cf: u32, user_key: &[u8], snapshot: u64) -> Result<Option<Version>> {
        let lookup = key::LookupKey::new(cf, user_key, snapshot);
        self.get_at(&lookup, bloom::hash(lookup.id()))
    }

    /// [`TableReader::get`] for a key a read built once for every table it
    /// probes, `hash` being its bloom hash. The index and the block are
    /// searched in place: the one handle taken is the data block's, from the
    /// cache. Each handle is a write to a reference count shared by every
    /// thread reading the table, and a read used to take six, on the reader,
    /// the index and the block.
    pub fn get_at(&self, lookup: &key::LookupKey<'_>, hash: u64) -> Result<Option<Version>> {
        if !bloom::may_contain(&self.filter, hash) {
            return Ok(None);
        }
        let target = lookup.internal();
        let handle = with_seek_key(|key| match self.index.seek_entry(target, key)? {
            Some(entry) => {
                let (start, end) = entry.value;
                Handle::decode(&self.index.bytes()[start..end]).map(Some)
            }
            None => Ok(None),
        })?;
        let Some(handle) = handle else {
            return Ok(None);
        };
        let block = self.data_block_at(handle, true)?;
        let found = with_seek_key(|key| -> Result<Option<Option<Version>>> {
            let Some(entry) = block.seek_entry(target, key)? else {
                return Ok(None);
            };
            let (start, end) = entry.value;
            version_of(key, lookup, || {
                crate::memtable::detach_from(&block.bytes()[start..end], block.size(), || {
                    block.slice(start..end)
                })
            })
            .map(Some)
        })?;
        match found {
            Some(version) => Ok(version),
            None => self.get_after_block(lookup),
        }
    }

    /// The end of a point read whose block holds nothing at or after the
    /// target: every version in it is newer than the snapshot, or the target
    /// falls between two blocks. The next block's first entry decides. Rare
    /// enough to go through iterators.
    fn get_after_block(&self, lookup: &key::LookupKey<'_>) -> Result<Option<Version>> {
        let mut index = self.index.iter();
        index.seek(lookup.internal());
        index.status()?;
        if !index.valid() {
            return Ok(None);
        }
        index.next();
        if !index.valid() {
            index.status()?;
            return Ok(None);
        }
        let mut it = self.data_block(index.value_slice(), true)?.iter();
        it.seek_to_first();
        it.status()?;
        if !it.valid() {
            return Ok(None);
        }
        version_of(it.key(), lookup, || {
            crate::memtable::detach(it.value(), it.block_size())
        })
    }
}

/// The version `key` (an internal key found by a seek) is, if it is one of
/// `lookup`'s key; `value` is called only then.
fn version_of(
    key: &[u8],
    lookup: &key::LookupKey<'_>,
    value: impl FnOnce() -> Bytes,
) -> Result<Option<Version>> {
    let found = key::parse(key).ok_or_else(|| Error::Corruption("bad key in table".into()))?;
    Ok(
        (found.cf == lookup.cf && found.user_key == lookup.user_key).then(|| Version {
            seq: found.seq,
            kind: found.kind,
            value: value(),
        }),
    )
}

/// Keys up to this size stay in a thread's seek buffer between reads.
const SEEK_KEY_KEPT: usize = 16 << 10;

thread_local! {
    /// The key a point read decodes while it seeks, reused by the thread's
    /// next reads: a read allocates nothing to seek.
    static SEEK_KEY: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
}

/// Run `f` with the thread's seek buffer, or a fresh one where it cannot be
/// had (a read from a thread-local destructor, or one nested in another):
/// those allocate, and nothing panics.
fn with_seek_key<T>(f: impl FnOnce(&mut Vec<u8>) -> T) -> T {
    let mut f = Some(f);
    let kept = SEEK_KEY.try_with(|cell| {
        let mut key = cell.try_borrow_mut().ok()?;
        let out = (f.take().expect("called once"))(&mut key);
        // One huge key read once must not pin its buffer for the thread's
        // life.
        if key.capacity() > SEEK_KEY_KEPT {
            *key = Vec::new();
        }
        Some(out)
    });
    match kept {
        Ok(Some(out)) => out,
        _ => (f.take().expect("not called yet"))(&mut Vec::new()),
    }
}

impl Drop for TableReader {
    fn drop(&mut self) {
        if let Some((cache, _)) = &self.cache {
            cache.release(self.pinned as u64);
        }
    }
}

/// A cursor over a whole table.
pub struct TableIter {
    table: Arc<TableReader>,
    index: BlockIter,
    data: Option<BlockIter>,
    /// The key buffer of the last block's cursor, for the next one.
    spare_key: Vec<u8>,
    /// Walking forward, the block after the current one, with where its
    /// entry is in the index: found in the cache, and its memory
    /// prefetched, on entering the current one (see [`Block::prefetch`]).
    ahead: Option<(usize, Block)>,
    error: Option<Error>,
    fill_cache: bool,
}

impl TableIter {
    pub fn new(table: Arc<TableReader>) -> Self {
        let index = table.index.iter();
        TableIter {
            table,
            index,
            data: None,
            spare_key: Vec::new(),
            ahead: None,
            error: None,
            fill_cache: true,
        }
    }

    /// For a compaction: blocks read from disk are not kept in the cache.
    pub fn without_filling_cache(mut self) -> Self {
        self.fill_cache = false;
        self
    }

    /// Open the data block the index points at.
    fn load(&mut self) -> bool {
        if let Some(data) = self.data.take() {
            self.spare_key = data.into_key();
        }
        if !self.index.valid() {
            if let Err(e) = self.index.status() {
                self.error = Some(e);
            }
            return false;
        }
        let block = match self.ahead.take() {
            Some((entry, block)) if entry == self.index.offset() => Ok(block),
            _ => Handle::decode(self.index.value_slice())
                .and_then(|handle| self.table.data_block_at(handle, self.fill_cache)),
        };
        match block {
            Ok(block) => {
                self.data = Some(block.into_iter_with(std::mem::take(&mut self.spare_key)));
                true
            }
            Err(e) => {
                self.error = Some(e);
                false
            }
        }
    }

    #[inline]
    fn data_valid(&self) -> bool {
        self.data.as_ref().is_some_and(|d| d.valid())
    }

    /// Walking forward into a block: find the next one in the cache and
    /// start bringing it into the CPU's caches while this one is read. A
    /// compaction, which reads blocks the cache mostly does not hold, does
    /// not look.
    fn look_ahead(&mut self) {
        self.ahead = None;
        if !self.fill_cache {
            return;
        }
        let Some(next) = self.index.peek_next_value() else {
            return;
        };
        if let Some(block) = Handle::decode(next)
            .ok()
            .and_then(|handle| self.table.cached_block(handle))
        {
            block.prefetch();
            self.ahead = Some((self.index.next_offset(), block));
        }
    }

    /// Past the end of a block: on to the first entry of the next one.
    fn skip_forward(&mut self) {
        while !self.data_valid() && self.error.is_none() {
            if let Some(d) = &self.data {
                if let Err(e) = d.status() {
                    self.error = Some(e);
                    return;
                }
            }
            if self.data.is_some() {
                self.index.next();
            }
            if !self.load() {
                return;
            }
            self.data.as_mut().expect("loaded").seek_to_first();
            self.look_ahead();
        }
    }

    /// Before the start of a block: back to the last entry of the previous one.
    fn skip_backward(&mut self) {
        while !self.data_valid() && self.error.is_none() {
            if let Some(d) = &self.data {
                if let Err(e) = d.status() {
                    self.error = Some(e);
                    return;
                }
            }
            if self.data.is_some() {
                self.index.prev();
            }
            if !self.load() {
                return;
            }
            self.data.as_mut().expect("loaded").seek_to_last();
        }
    }
}

impl InternalIterator for TableIter {
    fn seek_ge(&mut self, target: &[u8]) {
        self.error = None;
        self.index.seek(target);
        if self.load() {
            self.data.as_mut().expect("loaded").seek(target);
            if self.data_valid() {
                self.look_ahead();
            }
            self.skip_forward();
        }
    }

    fn seek_le(&mut self, target: &[u8]) {
        self.error = None;
        // The block holding the first key at or after `target`, or the last
        // block if every key is before it.
        self.index.seek(target);
        if !self.index.valid() {
            if let Err(e) = self.index.status() {
                self.error = Some(e);
                self.data = None;
                return;
            }
            self.index.seek_to_last();
        }
        if self.load() {
            self.data.as_mut().expect("loaded").seek_for_prev(target);
            self.skip_backward();
        }
    }

    fn seek_to_last(&mut self) {
        self.error = None;
        self.index.seek_to_last();
        if self.load() {
            self.data.as_mut().expect("loaded").seek_to_last();
            self.skip_backward();
        }
    }

    fn next(&mut self) {
        if let Some(d) = self.data.as_mut() {
            d.next();
        }
        self.skip_forward();
    }

    #[inline]
    fn advance(&mut self) -> bool {
        if let Some(d) = self.data.as_mut() {
            d.next();
            if d.valid() {
                return true;
            }
        }
        self.skip_forward();
        self.valid()
    }

    fn prev(&mut self) {
        if let Some(d) = self.data.as_mut() {
            d.prev();
        }
        self.skip_backward();
    }

    #[inline]
    fn valid(&self) -> bool {
        self.error.is_none() && self.data_valid()
    }

    #[inline]
    fn key(&self) -> &[u8] {
        self.data.as_ref().expect("valid").key()
    }

    fn value(&self) -> Bytes {
        self.data.as_ref().expect("valid").value()
    }

    #[inline]
    fn value_slice(&self) -> &[u8] {
        self.data.as_ref().expect("valid").value_slice()
    }

    fn status(&self) -> Result<()> {
        match &self.error {
            Some(e) => Err(e.clone()),
            None => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::{FaultFs, Fs};
    use std::path::Path;

    fn write_table(
        fs: &FaultFs,
        entries: &[(Vec<u8>, Vec<u8>)],
        compress: bool,
    ) -> (TableProps, Arc<TableReader>) {
        let path = Path::new("/t/000001.sst");
        let mut builder = TableBuilder::new(fs.create(path).unwrap());
        if !compress {
            builder = builder.uncompressed();
        }
        for (k, v) in entries {
            builder.add(k, v).unwrap();
        }
        let props = builder.finish().unwrap();
        let reader = Arc::new(TableReader::open(fs.open_random(path).unwrap()).unwrap());
        (props, reader)
    }

    fn entries(n: u32) -> Vec<(Vec<u8>, Vec<u8>)> {
        (0..n)
            .map(|i| {
                let value = format!("value {i} {}", "x".repeat((i % 50) as usize)).into_bytes();
                (
                    key::encode(
                        i % 3,
                        format!("k{i:06}").as_bytes(),
                        u64::from(i) + 1,
                        Kind::Put,
                    ),
                    value,
                )
            })
            .collect()
    }

    fn sorted(mut e: Vec<(Vec<u8>, Vec<u8>)>) -> Vec<(Vec<u8>, Vec<u8>)> {
        e.sort_by(|a, b| key::compare(&a.0, &b.0));
        e
    }

    #[test]
    fn a_table_iterates_forward_and_backward_and_answers_gets() {
        for compress in [true, false] {
            let fs = FaultFs::new();
            let data = sorted(entries(5000));
            let (props, table) = write_table(&fs, &data, compress);
            assert_eq!(props.entries, 5000);
            assert_eq!(props.smallest, data[0].0);
            assert_eq!(props.largest, data.last().unwrap().0);

            let mut it = TableIter::new(table.clone());
            it.seek_ge(&data[0].0);
            let mut forward = Vec::new();
            while it.valid() {
                forward.push((it.key().to_vec(), it.value().to_vec()));
                it.next();
            }
            assert_eq!(forward, data);
            it.seek_to_last();
            let mut backward = Vec::new();
            while it.valid() {
                backward.push(it.key().to_vec());
                it.prev();
            }
            backward.reverse();
            assert_eq!(
                backward,
                data.iter().map(|e| e.0.clone()).collect::<Vec<_>>()
            );

            for (k, v) in data.iter().step_by(97) {
                let p = key::parse(k).unwrap();
                let got = table.get(p.cf, p.user_key, p.seq).unwrap().unwrap();
                assert_eq!(&got.value[..], &v[..]);
                assert!(
                    table.get(p.cf, p.user_key, p.seq - 1).unwrap().is_none(),
                    "newer than the snapshot"
                );
            }
            assert!(table.get(0, b"absent", 1 << 40).unwrap().is_none());
        }
    }

    #[test]
    fn a_get_finds_an_older_version_in_the_next_block() {
        // Many versions of one key, spilling over several blocks.
        let fs = FaultFs::new();
        let data: Vec<_> = (1..=2000u64)
            .rev()
            .map(|seq| {
                (
                    key::encode(0, b"hot", seq, Kind::Put),
                    seq.to_le_bytes().to_vec(),
                )
            })
            .collect();
        let (_, table) = write_table(&fs, &data, true);
        for snapshot in [1u64, 2, 500, 1999, 2000, 5000] {
            let got = table.get(0, b"hot", snapshot).unwrap().unwrap();
            assert_eq!(got.seq, snapshot.min(2000));
        }
    }

    /// Keys that leave gaps at block boundaries, so the index holds
    /// shortened separators: sparse names, two versions for one key in
    /// three, three column families, and one hot key whose versions span
    /// blocks.
    fn gappy_entries() -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut e = Vec::new();
        for cf in 0..3u32 {
            for i in 0..1500u32 {
                let user = format!("user:{:05}:{}", i * 7, "z".repeat((i % 9) as usize));
                let versions = if i % 3 == 0 { 2 } else { 1 };
                for seq in (1..=versions).map(|v| u64::from(i) * 2 + v) {
                    e.push((
                        key::encode(cf, user.as_bytes(), seq, Kind::Put),
                        format!("{cf}/{user}/{seq}").into_bytes(),
                    ));
                }
            }
        }
        for seq in 1..=400u64 {
            e.push((key::encode(1, b"user:hot", seq, Kind::Put), vec![b'h'; 40]));
        }
        sorted(e)
    }

    /// Every index entry separates its block from the next one, and most
    /// are shorter than the block's last key.
    #[test]
    fn index_entries_are_separators() {
        let fs = FaultFs::new();
        let data = gappy_entries();
        let (_, table) = write_table(&fs, &data, true);
        let mut index = table.index.iter();
        index.seek_to_first();
        let (mut blocks, mut shortened) = (0, 0);
        let mut previous: Option<Vec<u8>> = None;
        while index.valid() {
            let separator = index.key().to_vec();
            let mut it = table.data_block(&index.value(), false).unwrap().iter();
            it.seek_to_first();
            let first = it.key().to_vec();
            if let Some(p) = &previous {
                assert_eq!(key::compare(p, &first), Ordering::Less, "block {blocks}");
            }
            it.seek_to_last();
            let last = it.key().to_vec();
            assert_ne!(
                key::compare(&last, &separator),
                Ordering::Greater,
                "block {blocks}"
            );
            if separator.len() < last.len() {
                shortened += 1;
            }
            previous = Some(separator);
            blocks += 1;
            index.next();
        }
        assert_eq!(previous.as_deref(), data.last().map(|e| e.0.as_slice()));
        assert!(blocks > 20, "{blocks} blocks");
        assert!(shortened * 2 > blocks, "{shortened} of {blocks} shortened");
    }

    /// Seeks in both directions and gets, at every key, every separator and
    /// around both, agree with a sorted list of the entries.
    #[test]
    fn seeks_and_gets_agree_with_the_entries_around_separators() {
        let fs = FaultFs::new();
        let data = gappy_entries();
        let (_, table) = write_table(&fs, &data, true);

        let mut targets = Vec::new();
        let mut index = table.index.iter();
        index.seek_to_first();
        while index.valid() {
            let id = key::cf_and_user_key(index.key()).to_vec();
            targets.push(index.key().to_vec());
            for seq in [0, 1, 7, key::MAX_SEQ] {
                for kind in [Kind::Put, Kind::Delete] {
                    let mut t = id.clone();
                    t.extend_from_slice(&key::trailer_bytes(seq, kind));
                    targets.push(t);
                }
            }
            // The separator's key cut one byte shorter, and one byte longer.
            if id.len() > 4 {
                targets.push(key::first_of_id(&id[..id.len() - 1]));
            }
            let mut longer = id.clone();
            longer.push(0);
            targets.push(key::first_of_id(&longer));
            index.next();
        }
        for (k, _) in data.iter().step_by(13) {
            let p = key::parse(k).unwrap();
            targets.push(k.clone());
            targets.push(key::first_of(p.cf, p.user_key));
            targets.push(key::last_of(p.cf, p.user_key));
            for len in 0..p.user_key.len() {
                targets.push(key::first_of(p.cf, &p.user_key[..len]));
            }
        }

        let mut it = TableIter::new(table.clone());
        for t in &targets {
            let at_or_after = data
                .iter()
                .find(|e| key::compare(&e.0, t) != Ordering::Less);
            it.seek_ge(t);
            it.status().unwrap();
            assert_eq!(
                it.valid().then(|| it.key().to_vec()),
                at_or_after.map(|e| e.0.clone())
            );

            let at_or_before = data
                .iter()
                .rev()
                .find(|e| key::compare(&e.0, t) != Ordering::Greater);
            it.seek_le(t);
            it.status().unwrap();
            assert_eq!(
                it.valid().then(|| it.key().to_vec()),
                at_or_before.map(|e| e.0.clone())
            );

            if let Some(p) = key::parse(t) {
                let probe = key::at_snapshot(p.cf, p.user_key, p.seq);
                let want = data
                    .iter()
                    .find(|e| key::compare(&e.0, &probe) != Ordering::Less)
                    .and_then(|e| {
                        let q = key::parse(&e.0).unwrap();
                        (q.cf == p.cf && q.user_key == p.user_key).then(|| e.1.clone())
                    });
                let got = table
                    .get(p.cf, p.user_key, p.seq)
                    .unwrap()
                    .map(|v| v.value.to_vec());
                assert_eq!(got, want);
            }
        }
    }

    #[test]
    fn a_flipped_byte_is_caught_by_a_checksum() {
        let fs = FaultFs::new();
        let data = sorted(entries(300));
        write_table(&fs, &data, true);
        let path = Path::new("/t/000001.sst");
        let original = fs.read(path).unwrap();
        for at in (0..original.len()).step_by(41) {
            let mut bad = original.clone();
            bad[at] ^= 0x10;
            let f = fs.create(path).unwrap();
            f.append(&bad).unwrap();
            let outcome = TableReader::open(fs.open_random(path).unwrap()).and_then(|t| {
                let t = Arc::new(t);
                let mut it = TableIter::new(t);
                it.seek_ge(&data[0].0);
                let mut n = 0;
                while it.valid() {
                    n += 1;
                    it.next();
                }
                it.status().map(|_| n)
            });
            match outcome {
                Ok(n) => assert_eq!(n, data.len(), "byte {at} flipped yet every entry read back"),
                Err(Error::Corruption(_)) => {}
                Err(e) => panic!("byte {at}: unexpected {e}"),
            }
        }
    }
}
