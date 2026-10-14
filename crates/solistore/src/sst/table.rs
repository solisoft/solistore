//! Table files: an immutable, sorted run of internal keys.
//!
//! ```text
//! table   := data block* | filter | properties | index | footer
//! each block on disk := contents | type u8 | crc32c u32 (of contents and type)
//!   type 0: contents is the block;  type 1: varint raw length | LZ4 block
//! index   := a block whose entries map the last key of each data block to
//!            its handle (offset varint, length varint)
//! footer  := index handle | filter handle | properties handle   (u64 LE pairs)
//!            | format version u32 | crc32c u32 (of the 52 bytes before) | magic u64
//! ```
//!
//! Opening a table reads its footer, index, filter and properties and keeps
//! them in memory; data blocks are read with `pread` when needed. Every byte
//! is checksummed before it is decoded.

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
            index: BlockBuilder::new(1),
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

    #[cfg(test)]
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
        let handle = self.write_block(&contents, self.compress)?;
        let mut encoded = Vec::new();
        handle.encode(&mut encoded);
        self.index.add(&self.last_key, &encoded);
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

pub struct TableReader {
    file: Arc<dyn RandomAccessFile>,
    index: Block,
    filter: Bytes,
    /// Bytes held in memory for this table (index and filter).
    pinned: usize,
    /// The shared block cache and this table's number in it.
    cache: Option<(Arc<BlockCache>, u64)>,
}

fn read_block(file: &dyn RandomAccessFile, handle: Handle) -> Result<Bytes> {
    let corrupt =
        |what: &str| Error::Corruption(format!("table block at {}: {what}", handle.offset));
    let total = usize::try_from(handle.len).map_err(|_| corrupt("length"))? + BLOCK_TRAILER;
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
            if raw_len > 64 << 20 {
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
        let index = Block::new(read_block(file.as_ref(), index)?)?;
        let filter = read_block(file.as_ref(), filter)?;
        let pinned = index.size() + filter.len();
        Ok(TableReader {
            cache: None,
            file,
            index,
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
        let handle = Handle::decode(handle_bytes)?;
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

    /// The newest version of `user_key` at most `snapshot`, if this table has one.
    pub fn get(&self, cf: u32, user_key: &[u8], snapshot: u64) -> Result<Option<Version>> {
        let target = key::at_snapshot(cf, user_key, snapshot);
        if !bloom::may_contain(&self.filter, bloom::hash(key::cf_and_user_key(&target))) {
            return Ok(None);
        }
        let mut index = self.index.iter();
        index.seek(&target);
        index.status()?;
        if !index.valid() {
            return Ok(None);
        }
        let mut it = self.data_block(&index.value(), true)?.iter();
        it.seek(&target);
        it.status()?;
        if !it.valid() {
            // Every version in this block is newer than the snapshot; the
            // next block may hold an older one.
            index.next();
            if !index.valid() {
                return Ok(None);
            }
            it = self.data_block(&index.value(), true)?.iter();
            it.seek_to_first();
            it.status()?;
            if !it.valid() {
                return Ok(None);
            }
        }
        let found =
            key::parse(it.key()).ok_or_else(|| Error::Corruption("bad key in table".into()))?;
        Ok(
            (found.cf == cf && found.user_key == user_key).then(|| Version {
                seq: found.seq,
                kind: found.kind,
                value: it.value(),
            }),
        )
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
        self.data = None;
        if !self.index.valid() {
            if let Err(e) = self.index.status() {
                self.error = Some(e);
            }
            return false;
        }
        match self.table.data_block(&self.index.value(), self.fill_cache) {
            Ok(block) => {
                self.data = Some(block.iter());
                true
            }
            Err(e) => {
                self.error = Some(e);
                false
            }
        }
    }

    fn data_valid(&self) -> bool {
        self.data.as_ref().is_some_and(|d| d.valid())
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

    fn prev(&mut self) {
        if let Some(d) = self.data.as_mut() {
            d.prev();
        }
        self.skip_backward();
    }

    fn valid(&self) -> bool {
        self.error.is_none() && self.data_valid()
    }

    fn key(&self) -> &[u8] {
        self.data.as_ref().expect("valid").key()
    }

    fn value(&self) -> Bytes {
        self.data.as_ref().expect("valid").value()
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
