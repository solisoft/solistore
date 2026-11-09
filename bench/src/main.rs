//! Storage-engine benchmark for SoliDB, on the access patterns the server
//! actually has. The baseline is RocksDB with the production `EngineProfile`
//! (db/src/storage/engine.rs) and the 2.x layout — one shared column family,
//! an 8-byte (db_id, coll_id) key prefix, drop = DeleteRange then
//! compact_range to reclaim, as keyspace_gc.rs does. The candidate is
//! solistore. Another engine is added by implementing the `Engine` trait and
//! a match arm in `open_engine`.
//!
//! Moved here from SoliDB's `feat/storage-engine-bench` branch, which keeps
//! the fjall runs of 2026-10-02.
//!
//! The workload is what the server does:
//! - a write is one batch holding the document and three index entries;
//! - an update reads the document, rewrites it, and swaps one index entry
//!   (delete the old `idx:age:` key, insert the new one);
//! - reads are primary-key gets (uniform, hot-set, and missing keys) and
//!   bounded index scans that then fetch each document;
//! - transactions commit with fsync, from one thread and from eight;
//! - an instance holds ~1,232 collections, 16 of them hot.
//!
//!   engine-bench run        --engine rocks --dir <path> [--label L] [options]
//!   engine-bench crash      --engine rocks --dir <path> [--secs 3]
//!   engine-bench visibility --engine rocks --dir <path> [--secs 15]
//!   engine-bench scan       --engine rocks --dir <path> [--docs 1000000] [--cache-mb 512]
//!   engine-bench point      --engine rocks --dir <path> [--docs 1000000] [--cache-mb 512]
//!
//! Every phase prints one JSON line with throughput, latency percentiles,
//! RSS, CPU, bytes written to disk by the process and the data directory size.
//! The RocksDB 11.8.1 baseline of 2026-10-02 is in results/.

use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

// ---------------------------------------------------------------- engine API

/// One operation of a write batch: key in a collection, value or tombstone.
struct Op {
    coll: u32,
    key: Vec<u8>,
    value: Option<Vec<u8>>,
}

impl Op {
    fn logical_bytes(&self) -> u64 {
        (self.key.len() + self.value.as_ref().map_or(0, |v| v.len())) as u64
    }
}

trait Engine: Send + Sync {
    fn create_collection(&self, coll: u32);
    fn drop_collection(&self, coll: u32);
    /// Work a drop leaves for later (RocksDB: compacting the dead range).
    fn reclaim(&self) {}
    fn write(&self, ops: Vec<Op>, sync: bool);
    fn get(&self, coll: u32, key: &[u8]) -> Option<Vec<u8>>;
    /// Keys in `[start, end)` of `coll`, ascending or descending, at most `limit`.
    fn scan_keys(
        &self,
        coll: u32,
        start: &[u8],
        end: &[u8],
        rev: bool,
        limit: usize,
    ) -> Vec<Vec<u8>>;
    /// Make everything durable.
    fn flush(&self);
    /// Every entry of `[start, end)` of `coll`, forward, through the
    /// engine's ordinary iterator (owned items). Returns the entries and
    /// their key and value bytes; the visitor does nothing else.
    fn full_scan(&self, _coll: u32, _start: &[u8], _end: &[u8]) -> (u64, u64) {
        unimplemented!("full_scan")
    }
    /// The same through the engine's copy-free path: slices valid until the
    /// next step.
    fn full_scan_ref(&self, coll: u32, start: &[u8], end: &[u8]) -> (u64, u64) {
        self.full_scan(coll, start, end)
    }
    /// Values of `keys` in `coll`, in one call where the engine has one;
    /// returns how many were found.
    fn multi_get(&self, coll: u32, keys: &[Vec<u8>]) -> usize {
        keys.iter().filter(|k| self.get(coll, k).is_some()).count()
    }
    /// The same range as one consistent read, split at `cuts` (user keys of
    /// `coll`, increasing) and scanned on one thread per part.
    fn parallel_scan(
        &self,
        _coll: u32,
        _start: &[u8],
        _end: &[u8],
        _cuts: &[Vec<u8>],
    ) -> (u64, u64) {
        unimplemented!("parallel_scan")
    }
    /// What the write path is doing now, for a per-second series: level-0
    /// files, then cumulative counters (writes slowed, ms stopped, commit
    /// groups, batches, WAL fsyncs, compactions), as far as the engine has
    /// them.
    fn write_state(&self) -> [u64; 7] {
        [0; 7]
    }
    /// Engine counters as a JSON object, printed with every phase.
    fn stats(&self) -> String {
        String::new()
    }
    /// Point reads through handles taken once, for the point mode: one
    /// reader per thread.
    fn point_reader(&self) -> Box<dyn PointReader + '_> {
        unimplemented!("point_reader")
    }
}

/// Point reads of keys already under their collection prefix, so that
/// the point mode times the engine's call and nothing around it.
trait PointReader {
    /// The value's length, if the key is there.
    fn get(&self, key: &[u8]) -> Option<usize>;
    /// Keys found and their value bytes, in one call.
    fn multi_get(&self, keys: &[&[u8]]) -> (usize, usize);
    /// The same, with the values borrowed where the engine can lend them
    /// (RocksDB's `multi_get` already returns pinned slices).
    fn multi_get_ref(&self, keys: &[&[u8]]) -> (usize, usize) {
        self.multi_get(keys)
    }
}

/// The engine whose counters each phase line carries.
static STATS_SOURCE: std::sync::Mutex<Option<Arc<dyn Engine>>> = std::sync::Mutex::new(None);

fn prefixed(coll: u32, key: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(8 + key.len());
    out.extend_from_slice(&1u32.to_be_bytes()); // db_id
    out.extend_from_slice(&coll.to_be_bytes());
    out.extend_from_slice(key);
    out
}

fn coll_range(coll: u32) -> (Vec<u8>, Vec<u8>) {
    (prefixed(coll, b""), prefixed(coll + 1, b""))
}

// ---------------------------------------------------------------- RocksDB

#[cfg(feature = "rocks")]
mod rocks {
    use super::*;
    use rust_rocksdb::{
        BlockBasedIndexType, BlockBasedOptions, Cache, ColumnFamilyDescriptor, DBCompressionType,
        DBWithThreadMode, IteratorMode, MultiThreaded, Options, ReadOptions, WriteBatch,
        WriteBufferManager, WriteOptions,
    };

    type Db = DBWithThreadMode<MultiThreaded>;
    const META: &str = "_meta";
    const SHARED: &str = "__keyspaces__";

    pub struct Rocks {
        db: Db,
        dead: std::sync::Mutex<Vec<u32>>,
        // Held for the life of the DB: the options only point at it.
        _wbm: Option<WriteBufferManager>,
    }

    /// The table options both the DB and each CF get: prod defaults plus the
    /// candidate settings under test.
    fn block_options(cache: &Cache, t: &RocksTuning) -> BlockBasedOptions {
        let mut block = BlockBasedOptions::default();
        block.set_block_cache(cache);
        block.set_bloom_filter(10.0, false);
        if t.cache_index {
            // Index and filter blocks live in the block cache (bounded,
            // evictable) instead of in table readers (unbounded with
            // max_open_files = -1, never evicted).
            block.set_cache_index_and_filter_blocks(true);
            block.set_cache_index_and_filter_blocks_with_high_priority(true);
            block.set_pin_l0_filter_and_index_blocks_in_cache(true);
        }
        if t.partitioned {
            // Only the small top level stays pinned; partitions are cached
            // and evicted like data blocks.
            block.set_index_type(BlockBasedIndexType::TwoLevelIndexSearch);
            block.set_partition_filters(true);
            block.set_metadata_block_size(4096);
            block.set_pin_top_level_index_and_filter(true);
        }
        block
    }

    /// `EngineProfile::prod()` and `tuned_cf_options()`, src/storage/engine.rs.
    fn cf_options(cache: &Cache, t: &RocksTuning) -> Options {
        let mut opts = Options::default();
        opts.set_compression_type(DBCompressionType::Lz4);
        opts.set_target_file_size_base(64 * 1024 * 1024);
        opts.set_target_file_size_multiplier(2);
        opts.set_write_buffer_size(64 * 1024 * 1024);
        opts.set_max_write_buffer_number(3);
        opts.set_min_write_buffer_number_to_merge(1);
        opts.set_arena_block_size(64 * 1024);
        opts.set_max_subcompactions(4);
        opts.set_block_based_table_factory(&block_options(cache, t));
        opts
    }

    fn db_options(cache: &Cache, t: &RocksTuning, wbm: Option<&WriteBufferManager>) -> Options {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        opts.set_compression_type(DBCompressionType::Lz4);
        opts.set_compression_options(-14, -1, 0, 0);
        opts.set_block_based_table_factory(&block_options(cache, t));
        opts.set_write_buffer_size(64 * 1024 * 1024);
        opts.set_max_write_buffer_number(4);
        opts.set_min_write_buffer_number_to_merge(1);
        opts.set_arena_block_size(64 * 1024);
        match wbm {
            // One budget for everything: memtable memory is charged to the
            // block cache, so the cache's capacity is the process's cap.
            Some(wbm) => opts.set_write_buffer_manager(wbm),
            None => opts.set_db_write_buffer_size(512 * 1024 * 1024),
        }
        opts.set_max_open_files(-1);
        opts.set_max_background_jobs(6);
        opts.set_max_subcompactions(4);
        opts.set_target_file_size_base(64 * 1024 * 1024);
        opts.set_target_file_size_multiplier(2);
        opts.set_max_bytes_for_level_base(512 * 1024 * 1024);
        opts.set_max_bytes_for_level_multiplier(10.0);
        opts.set_num_levels(7);
        opts.set_max_total_wal_size(2 * 1024 * 1024 * 1024);
        opts.set_keep_log_file_num(5);
        opts.set_max_log_file_size(64 * 1024 * 1024);
        opts.set_recycle_log_file_num(3);
        opts.set_enable_pipelined_write(true);
        opts
    }

    impl Rocks {
        pub fn open(dir: &Path, t: &RocksTuning) -> Self {
            let mb = 1024 * 1024;
            // With a write-buffer manager the memtables are charged to the
            // cache, so it grows by their budget: same total as prod.
            let capacity = (t.cache_mb.unwrap_or(512) + t.wbm_mb.unwrap_or(0)) * mb;
            let cache = if t.hyper_clock {
                Cache::new_hyper_clock_cache(capacity, 0)
            } else {
                Cache::new_lru_cache(capacity)
            };
            let wbm = t.wbm_mb.map(|budget| {
                WriteBufferManager::new_write_buffer_manager_with_cache(
                    budget * mb,
                    false,
                    cache.clone(),
                )
            });
            let cfs = ["default", META, SHARED]
                .iter()
                .map(|name| ColumnFamilyDescriptor::new(*name, cf_options(&cache, t)))
                .collect::<Vec<_>>();
            let db = Db::open_cf_descriptors(&db_options(&cache, t, wbm.as_ref()), dir, cfs)
                .expect("open rocksdb");
            Rocks {
                db,
                dead: std::sync::Mutex::new(Vec::new()),
                _wbm: wbm,
            }
        }
    }

    impl Engine for Rocks {
        fn create_collection(&self, coll: u32) {
            // 2.x: creating a collection is one registry write.
            let meta = self.db.cf_handle(META).unwrap();
            self.db
                .put_cf(&meta, format!("coll:{coll}"), coll.to_be_bytes())
                .unwrap();
        }

        fn drop_collection(&self, coll: u32) {
            let meta = self.db.cf_handle(META).unwrap();
            let shared = self.db.cf_handle(SHARED).unwrap();
            let (from, to) = coll_range(coll);
            let mut batch = WriteBatch::default();
            batch.delete_cf(&meta, format!("coll:{coll}"));
            batch.put_cf(&meta, format!("dead_ks:{coll}"), b"");
            batch.delete_range_cf(&shared, from, to);
            self.db.write(&batch).unwrap();
            self.dead.lock().unwrap().push(coll);
        }

        fn reclaim(&self) {
            // keyspace_gc.rs: compact each dropped range so its space returns.
            let shared = self.db.cf_handle(SHARED).unwrap();
            for coll in self.dead.lock().unwrap().drain(..) {
                let (from, to) = coll_range(coll);
                self.db.compact_range_cf(&shared, Some(from), Some(to));
            }
        }

        fn write(&self, ops: Vec<Op>, sync: bool) {
            let shared = self.db.cf_handle(SHARED).unwrap();
            let mut batch = WriteBatch::default();
            for op in ops {
                let key = prefixed(op.coll, &op.key);
                match op.value {
                    Some(v) => batch.put_cf(&shared, key, v),
                    None => batch.delete_cf(&shared, key),
                }
            }
            let mut wo = WriteOptions::default();
            wo.set_sync(sync);
            self.db.write_opt(&batch, &wo).unwrap();
        }

        fn get(&self, coll: u32, key: &[u8]) -> Option<Vec<u8>> {
            let shared = self.db.cf_handle(SHARED).unwrap();
            self.db.get_cf(&shared, prefixed(coll, key)).unwrap()
        }

        fn scan_keys(
            &self,
            coll: u32,
            start: &[u8],
            end: &[u8],
            rev: bool,
            limit: usize,
        ) -> Vec<Vec<u8>> {
            // keyspace.rs bounded_opts: every iterator is bounded.
            let shared = self.db.cf_handle(SHARED).unwrap();
            let mut ro = ReadOptions::default();
            ro.set_iterate_lower_bound(prefixed(coll, start));
            ro.set_iterate_upper_bound(prefixed(coll, end));
            let mode = if rev {
                IteratorMode::End
            } else {
                IteratorMode::Start
            };
            self.db
                .iterator_cf_opt(&shared, ro, mode)
                .take(limit)
                .map(|item| item.unwrap().0[8..].to_vec())
                .collect()
        }

        fn flush(&self) {
            for name in [META, SHARED] {
                let cf = self.db.cf_handle(name).unwrap();
                self.db.flush_cf(&cf).unwrap();
            }
        }

        fn multi_get(&self, coll: u32, keys: &[Vec<u8>]) -> usize {
            let shared = self.db.cf_handle(SHARED).unwrap();
            let keys: Vec<Vec<u8>> = keys.iter().map(|k| prefixed(coll, k)).collect();
            self.db
                .batched_multi_get_cf(&shared, &keys, false)
                .into_iter()
                .filter(|v| v.as_ref().unwrap().is_some())
                .count()
        }

        fn full_scan(&self, coll: u32, start: &[u8], end: &[u8]) -> (u64, u64) {
            let shared = self.db.cf_handle(SHARED).unwrap();
            let ro = bounded(coll, start, end);
            let (mut n, mut bytes) = (0, 0);
            for item in self.db.iterator_cf_opt(&shared, ro, IteratorMode::Start) {
                let (k, v) = item.unwrap();
                n += 1;
                bytes += (k.len() + v.len()) as u64;
            }
            (n, bytes)
        }

        fn full_scan_ref(&self, coll: u32, start: &[u8], end: &[u8]) -> (u64, u64) {
            let shared = self.db.cf_handle(SHARED).unwrap();
            let mut it = self
                .db
                .raw_iterator_cf_opt(&shared, bounded(coll, start, end));
            it.seek_to_first();
            count_raw(&mut it)
        }

        fn parallel_scan(
            &self,
            coll: u32,
            start: &[u8],
            end: &[u8],
            cuts: &[Vec<u8>],
        ) -> (u64, u64) {
            let shared = self.db.cf_handle(SHARED).unwrap();
            let snapshot = self.db.snapshot();
            let ranges = ranges(start, end, cuts);
            std::thread::scope(|scope| {
                let handles: Vec<_> = ranges
                    .iter()
                    .map(|(lo, hi)| {
                        let (shared, snapshot) = (&shared, &snapshot);
                        scope.spawn(move || {
                            let mut it =
                                snapshot.raw_iterator_cf_opt(shared, bounded(coll, lo, hi));
                            it.seek_to_first();
                            count_raw(&mut it)
                        })
                    })
                    .collect();
                handles
                    .into_iter()
                    .map(|h| h.join().unwrap())
                    .fold((0, 0), |a, b| (a.0 + b.0, a.1 + b.1))
            })
        }

        fn point_reader(&self) -> Box<dyn PointReader + '_> {
            Box::new(RocksPoint {
                db: &self.db,
                cf: self.db.cf_handle(SHARED).unwrap(),
            })
        }
    }

    /// SoliDB's reads: `get_pinned_cf`, and `batched_multi_get_cf` with
    /// unsorted input (storage/kv/mod.rs).
    struct RocksPoint<'a> {
        db: &'a Db,
        cf: Arc<rust_rocksdb::BoundColumnFamily<'a>>,
    }

    impl PointReader for RocksPoint<'_> {
        fn get(&self, key: &[u8]) -> Option<usize> {
            self.db
                .get_pinned_cf(&self.cf, key)
                .unwrap()
                .map(|v| v.len())
        }

        fn multi_get(&self, keys: &[&[u8]]) -> (usize, usize) {
            let mut out = (0, 0);
            for value in self
                .db
                .batched_multi_get_cf(&self.cf, keys.iter().copied(), false)
            {
                if let Some(v) = value.unwrap() {
                    out.0 += 1;
                    out.1 += v.len();
                }
            }
            out
        }
    }

    fn bounded(coll: u32, start: &[u8], end: &[u8]) -> ReadOptions {
        let mut ro = ReadOptions::default();
        ro.set_iterate_lower_bound(prefixed(coll, start));
        ro.set_iterate_upper_bound(prefixed(coll, end));
        ro
    }

    fn count_raw<D: rust_rocksdb::DBAccess>(
        it: &mut rust_rocksdb::DBRawIteratorWithThreadMode<'_, D>,
    ) -> (u64, u64) {
        let (mut n, mut bytes) = (0, 0);
        while let Some((k, v)) = it.item() {
            n += 1;
            bytes += (k.len() + v.len()) as u64;
            it.next();
        }
        it.status().unwrap();
        (n, bytes)
    }
}

/// `[start, end)` cut at `cuts` into consecutive ranges.
#[allow(dead_code)]
fn ranges(start: &[u8], end: &[u8], cuts: &[Vec<u8>]) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut bounds = vec![start.to_vec()];
    bounds.extend(cuts.iter().cloned());
    bounds.push(end.to_vec());
    bounds
        .windows(2)
        .map(|w| (w[0].clone(), w[1].clone()))
        .collect()
}

// ---------------------------------------------------------------- solistore

#[cfg(feature = "solistore")]
mod soli {
    use super::*;
    use solistore::{
        ColumnFamily, Db, Durability, IteratorMode, Options, ReadOptions, WriteBatch, WriteOptions,
    };

    const META: &str = "_meta";
    const SHARED: &str = "__keyspaces__";

    /// Entries and bytes of a borrowed walk.
    #[cfg(feature = "scan-api")]
    fn count_ref(mut it: solistore::DbIterator) -> (u64, u64) {
        let (mut n, mut bytes) = (0, 0);
        while let Some(item) = it.next_ref() {
            let (k, v) = item.unwrap();
            n += 1;
            bytes += (k.len() + v.len()) as u64;
        }
        (n, bytes)
    }

    /// The same layout as the RocksDB engine: collections share one column
    /// family under an 8-byte prefix, and `_meta` holds the registry.
    pub struct Soli {
        db: Db,
        meta: ColumnFamily,
        shared: ColumnFamily,
        dead: std::sync::Mutex<Vec<u32>>,
    }

    impl Soli {
        pub fn open(dir: &Path, durability: Durability, t: &SoliTuning) -> Self {
            let mut opts = Options {
                durability,
                ..Options::default()
            };
            if let Some(mb) = t.buffer_mb {
                opts.write_buffer_size = mb << 20;
            }
            if let Some(n) = t.compactions {
                opts.max_background_compactions = n;
            }
            if let Some(n) = t.l0_slowdown {
                opts.level0_slowdown_trigger = n;
            }
            if let Some(n) = t.recycle_wals {
                opts.recycle_wal_files = n;
            }
            if let Some(mb) = t.cache_mb {
                opts.block_cache_size = (mb as u64) << 20;
            }
            let db = Db::open(dir, opts).expect("open solistore");
            let cf = |name: &str| {
                db.cf_handle(name)
                    .unwrap_or_else(|| db.create_cf(name).unwrap())
            };
            let (meta, shared) = (cf(META), cf(SHARED));
            Soli {
                db,
                meta,
                shared,
                dead: std::sync::Mutex::new(Vec::new()),
            }
        }
    }

    impl Engine for Soli {
        fn create_collection(&self, coll: u32) {
            self.db
                .put(&self.meta, format!("coll:{coll}"), coll.to_be_bytes())
                .unwrap();
        }

        fn drop_collection(&self, coll: u32) {
            let (from, to) = coll_range(coll);
            let mut batch = WriteBatch::new();
            batch.delete(&self.meta, format!("coll:{coll}"));
            batch.put(&self.meta, format!("dead_ks:{coll}"), b"");
            batch.delete_range(&self.shared, from, to);
            self.db.write(batch).unwrap();
            self.dead.lock().unwrap().push(coll);
        }

        fn reclaim(&self) {
            // As keyspace_gc.rs does with RocksDB: compact each dropped range.
            for coll in self.dead.lock().unwrap().drain(..) {
                let (from, to) = coll_range(coll);
                self.db
                    .compact_range(&self.shared, Some(&from), Some(&to))
                    .unwrap();
            }
        }

        fn write(&self, ops: Vec<Op>, sync: bool) {
            let mut batch = WriteBatch::new();
            for op in ops {
                let key = prefixed(op.coll, &op.key);
                match op.value {
                    Some(v) => batch.put(&self.shared, key, v),
                    None => batch.delete(&self.shared, key),
                }
            }
            self.db.write_opt(batch, &WriteOptions { sync }).unwrap();
        }

        fn get(&self, coll: u32, key: &[u8]) -> Option<Vec<u8>> {
            self.db
                .get(&self.shared, prefixed(coll, key))
                .unwrap()
                .map(|v| v.to_vec())
        }

        fn scan_keys(
            &self,
            coll: u32,
            start: &[u8],
            end: &[u8],
            rev: bool,
            limit: usize,
        ) -> Vec<Vec<u8>> {
            let opts = ReadOptions {
                lower_bound: Some(prefixed(coll, start)),
                upper_bound: Some(prefixed(coll, end)),
                ..Default::default()
            };
            let mode = if rev {
                IteratorMode::End
            } else {
                IteratorMode::Start
            };
            self.db
                .iterator_opt(&self.shared, opts, mode)
                .unwrap()
                .take(limit)
                .map(|item| item.unwrap().0[8..].to_vec())
                .collect()
        }

        fn flush(&self) {
            self.db.flush().unwrap();
        }

        fn full_scan(&self, coll: u32, start: &[u8], end: &[u8]) -> (u64, u64) {
            let opts = ReadOptions {
                lower_bound: Some(prefixed(coll, start)),
                upper_bound: Some(prefixed(coll, end)),
                ..Default::default()
            };
            let (mut n, mut bytes) = (0, 0);
            for item in self
                .db
                .iterator_opt(&self.shared, opts, IteratorMode::Start)
                .unwrap()
            {
                let (k, v) = item.unwrap();
                n += 1;
                bytes += (k.len() + v.len()) as u64;
            }
            (n, bytes)
        }

        fn multi_get(&self, coll: u32, keys: &[Vec<u8>]) -> usize {
            let keys: Vec<Vec<u8>> = keys.iter().map(|k| prefixed(coll, k)).collect();
            let found = self.db.multi_get(&self.shared, &keys).unwrap();
            found.iter().filter(|v| v.is_some()).count()
        }

        #[cfg(feature = "scan-api")]
        fn full_scan_ref(&self, coll: u32, start: &[u8], end: &[u8]) -> (u64, u64) {
            let opts = ReadOptions {
                lower_bound: Some(prefixed(coll, start)),
                upper_bound: Some(prefixed(coll, end)),
                ..Default::default()
            };
            count_ref(
                self.db
                    .iterator_opt(&self.shared, opts, IteratorMode::Start)
                    .unwrap(),
            )
        }

        /// Without the scan API: one iterator per part, each at its own
        /// sequence number, cut where the caller says.
        #[cfg(not(feature = "scan-api"))]
        fn parallel_scan(
            &self,
            coll: u32,
            start: &[u8],
            end: &[u8],
            cuts: &[Vec<u8>],
        ) -> (u64, u64) {
            let ranges = ranges(start, end, cuts);
            std::thread::scope(|scope| {
                let handles: Vec<_> = ranges
                    .iter()
                    .map(|(lo, hi)| scope.spawn(move || self.full_scan(coll, lo, hi)))
                    .collect();
                handles
                    .into_iter()
                    .map(|h| h.join().unwrap())
                    .fold((0, 0), |a, b| (a.0 + b.0, a.1 + b.1))
            })
        }

        #[cfg(feature = "scan-api")]
        fn parallel_scan(
            &self,
            coll: u32,
            start: &[u8],
            end: &[u8],
            cuts: &[Vec<u8>],
        ) -> (u64, u64) {
            // One snapshot for every part, cut where the tables say the
            // bytes are (the caller's cuts only give the number of parts).
            let snapshot = self.db.snapshot();
            let (lo, hi) = (prefixed(coll, start), prefixed(coll, end));
            let cuts = snapshot
                .split_points(&self.shared, &lo, &hi, cuts.len() + 1)
                .unwrap();
            let ranges = ranges(&lo, &hi, &cuts);
            std::thread::scope(|scope| {
                let handles: Vec<_> = ranges
                    .into_iter()
                    .map(|(lo, hi)| {
                        let snapshot = snapshot.clone();
                        scope.spawn(move || {
                            // As SoliDB reads it: the snapshot in the options.
                            let opts = ReadOptions {
                                lower_bound: Some(lo),
                                upper_bound: Some(hi),
                                snapshot: Some(snapshot),
                            };
                            count_ref(
                                self.db
                                    .iterator_opt(&self.shared, opts, IteratorMode::Start)
                                    .unwrap(),
                            )
                        })
                    })
                    .collect();
                handles
                    .into_iter()
                    .map(|h| h.join().unwrap())
                    .fold((0, 0), |a, b| (a.0 + b.0, a.1 + b.1))
            })
        }

        fn write_state(&self) -> [u64; 7] {
            let s = self.db.stats();
            [
                self.db.files_per_level()[0] as u64,
                s.l0_slowdowns,
                s.l0_stop_micros / 1000,
                s.commit_groups,
                s.committed_batches,
                s.wal_fsyncs,
                s.compactions,
            ]
        }

        fn stats(&self) -> String {
            let s = self.db.stats();
            format!(
                concat!(
                    r#"{{"groups":{},"batches":{},"fsyncs":{},"bg_fsyncs":{},"l0_slowdowns":{},"l0_stop_ms":{},"#,
                    r#""memtable_wait_ms":{},"flushes":{},"flushed_mb":{},"compactions":{},"#,
                    r#""compaction_read_mb":{},"compaction_written_mb":{},"compaction_ms":{},"#,
                    r#""files_per_level":{:?},"block_cache_mb":{},"memtable_mb":{},"table_mb":{},"largest_wal_mb":{}}}"#
                ),
                s.commit_groups,
                s.committed_batches,
                s.wal_fsyncs,
                s.wal_background_fsyncs,
                s.l0_slowdowns,
                s.l0_stop_micros / 1000,
                s.memtable_wait_micros / 1000,
                s.flushes,
                s.flushed_bytes >> 20,
                s.compactions,
                s.compaction_read_bytes >> 20,
                s.compaction_written_bytes >> 20,
                s.compaction_micros / 1000,
                self.db.files_per_level(),
                self.db.block_cache_usage() >> 20,
                self.db.memtable_bytes() >> 20,
                self.db.live_table_bytes() >> 20,
                s.largest_wal_bytes >> 20,
            )
        }

        fn point_reader(&self) -> Box<dyn PointReader + '_> {
            Box::new(SoliPoint {
                db: &self.db,
                cf: &self.shared,
            })
        }
    }

    /// SoliDB's reads: `get`, and `multi_get` (storage/kv/mod.rs).
    struct SoliPoint<'a> {
        db: &'a Db,
        cf: &'a ColumnFamily,
    }

    impl PointReader for SoliPoint<'_> {
        fn get(&self, key: &[u8]) -> Option<usize> {
            self.db.get(self.cf, key).unwrap().map(|v| v.len())
        }

        fn multi_get(&self, keys: &[&[u8]]) -> (usize, usize) {
            let mut out = (0, 0);
            for v in self
                .db
                .multi_get(self.cf, keys)
                .unwrap()
                .into_iter()
                .flatten()
            {
                out.0 += 1;
                out.1 += v.len();
            }
            out
        }

        #[cfg(feature = "point-api")]
        fn multi_get_ref(&self, keys: &[&[u8]]) -> (usize, usize) {
            let mut out = (0, 0);
            self.db
                .multi_get_with(self.cf, keys, |_, value| {
                    if let Some(v) = value {
                        out.0 += 1;
                        out.1 += v.len();
                    }
                })
                .unwrap();
            out
        }
    }
}

// ---------------------------------------------------------------- SurrealKV

#[cfg(feature = "surrealkv")]
mod skv {
    use super::*;
    use surrealkv::{CompressionType, Durability, LSMIterator, Mode, Options, Tree, TreeBuilder};

    /// SurrealKV 1.x with the same layout: one tree, collections under the
    /// 8-byte prefix, the registry under its own prefix. It has no column
    /// families and no compact_range, so a drop is a delete_range and the
    /// space comes back with ordinary compaction.
    pub struct Skv {
        tree: Tree,
        rt: tokio::runtime::Runtime,
        /// Every commit fsynced (Durability::Immediate), or only sync writes.
        durable: bool,
        /// Commits retried after a write conflict.
        retries: AtomicU64,
    }

    fn meta_key(coll: u32) -> Vec<u8> {
        let mut key = 0u32.to_be_bytes().to_vec(); // db_id 0: the registry
        key.extend_from_slice(format!("coll:{coll}").as_bytes());
        key
    }

    impl Skv {
        /// `factory`: SurrealKV's own defaults (no compression, 64 KiB
        /// blocks, 32 MiB cache, 100 MiB memtable). Otherwise the budget
        /// the other engines get: 512 MiB cache, 64 MiB memtable, 4 KiB
        /// blocks, Snappy (it has no LZ4).
        pub fn open(dir: &Path, durable: bool, factory: bool) -> Self {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .expect("tokio runtime");
            let builder = if factory {
                TreeBuilder::new()
            } else {
                TreeBuilder::with_options(
                    Options::new()
                        .with_compression_per_level(vec![CompressionType::SnappyCompression; 6]),
                )
                .with_max_memtable_size(64 << 20)
                .with_block_cache_capacity(512 << 20)
                .with_block_size(4096)
            };
            let builder = builder.with_path(dir.to_path_buf());
            let tree = rt
                .block_on(async { builder.build() })
                .expect("open surrealkv");
            Skv {
                tree,
                rt,
                durable,
                retries: AtomicU64::new(0),
            }
        }

        /// Commit, retrying on a write conflict: SurrealKV checks
        /// conflicts optimistically even for write-only transactions, where
        /// a RocksDB or solistore batch simply applies. An application on
        /// SurrealKV retries the same way.
        fn commit(&self, f: impl Fn(&mut surrealkv::Transaction), sync: bool) {
            loop {
                let mut tx = self.tree.begin_with_mode(Mode::WriteOnly).expect("begin");
                f(&mut tx);
                if self.durable || sync {
                    tx.set_durability(Durability::Immediate);
                }
                match self.rt.block_on(tx.commit()) {
                    Ok(()) => return,
                    Err(surrealkv::Error::TransactionWriteConflict) => {
                        self.retries.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(e) => panic!("commit: {e:?}"),
                }
            }
        }
    }

    impl Engine for Skv {
        fn create_collection(&self, coll: u32) {
            self.commit(
                |tx| tx.set(meta_key(coll), coll.to_be_bytes().to_vec()).unwrap(),
                false,
            );
        }

        fn drop_collection(&self, coll: u32) {
            let (from, to) = coll_range(coll);
            self.commit(
                |tx| {
                    tx.delete(meta_key(coll)).unwrap();
                    tx.delete_range(from.clone(), to.clone()).unwrap();
                },
                false,
            );
        }

        fn write(&self, ops: Vec<Op>, sync: bool) {
            self.commit(
                |tx| {
                    for op in &ops {
                        let key = prefixed(op.coll, &op.key);
                        match &op.value {
                            Some(v) => tx.set(key, v.clone()).unwrap(),
                            None => tx.delete(key).unwrap(),
                        }
                    }
                },
                sync,
            );
        }

        fn get(&self, coll: u32, key: &[u8]) -> Option<Vec<u8>> {
            let tx = self.tree.begin_with_mode(Mode::ReadOnly).expect("begin");
            tx.get(prefixed(coll, key)).unwrap()
        }

        fn scan_keys(
            &self,
            coll: u32,
            start: &[u8],
            end: &[u8],
            rev: bool,
            limit: usize,
        ) -> Vec<Vec<u8>> {
            let tx = self.tree.begin_with_mode(Mode::ReadOnly).expect("begin");
            let mut it = tx
                .range(prefixed(coll, start), prefixed(coll, end))
                .unwrap();
            let mut out = Vec::new();
            let mut valid = if rev {
                it.seek_last().unwrap()
            } else {
                it.seek_first().unwrap()
            };
            while valid && out.len() < limit {
                out.push(it.key().user_key()[8..].to_vec());
                valid = if rev {
                    it.prev().unwrap()
                } else {
                    it.next().unwrap()
                };
            }
            out
        }

        fn flush(&self) {
            self.tree.flush_wal(true).expect("flush");
        }

        fn stats(&self) -> String {
            format!(
                r#"{{"conflict_retries":{}}}"#,
                self.retries.load(Ordering::Relaxed)
            )
        }
    }
}

/// solistore settings under test on top of its defaults.
#[derive(Clone, Default)]
struct SoliTuning {
    /// Memtable size; small values make flushes frequent (crash tests).
    buffer_mb: Option<usize>,
    /// Compactions allowed to run at once.
    compactions: Option<usize>,
    /// Dead WAL files kept for reuse.
    recycle_wals: Option<usize>,
    /// Level-0 files at which writes slow down.
    l0_slowdown: Option<usize>,
    /// Block cache size (default 512 MiB, SoliDB's).
    cache_mb: Option<usize>,
}

/// RocksDB settings under test on top of the prod profile.
#[derive(Clone, Default)]
struct RocksTuning {
    cache_index: bool,
    partitioned: bool,
    /// Memtable budget charged to the block cache (WriteBufferManager).
    wbm_mb: Option<usize>,
    hyper_clock: bool,
    /// Block cache size (default 512 MiB, SoliDB's).
    cache_mb: Option<usize>,
}

fn open_engine(p: &Params) -> Box<dyn Engine> {
    match p.engine.as_str() {
        #[cfg(feature = "rocks")]
        "rocks" => Box::new(rocks::Rocks::open(&p.dir, &p.rocks)),
        // Every commit fsynced before it returns: the default.
        #[cfg(feature = "solistore")]
        "solistore" => Box::new(soli::Soli::open(
            &p.dir,
            solistore::Durability::Full,
            &p.soli,
        )),
        // Writes acknowledged once in the OS, fsynced every 100 ms: what
        // RocksDB's unsynced writes are compared against.
        #[cfg(feature = "solistore")]
        "solistore-eventual" => Box::new(soli::Soli::open(
            &p.dir,
            solistore::Durability::Eventual {
                sync_interval: Duration::from_millis(100),
            },
            &p.soli,
        )),
        // Every commit fsynced (Durability::Immediate), as solistore's default.
        #[cfg(feature = "surrealkv")]
        "surrealkv" => Box::new(skv::Skv::open(&p.dir, true, false)),
        // SurrealKV's own default durability: commits do not wait for fsync.
        #[cfg(feature = "surrealkv")]
        "surrealkv-eventual" => Box::new(skv::Skv::open(&p.dir, false, false)),
        // The same, with SurrealKV's factory settings instead of the shared budget.
        #[cfg(feature = "surrealkv")]
        "surrealkv-defaults" => Box::new(skv::Skv::open(&p.dir, true, true)),
        #[cfg(feature = "surrealkv")]
        "surrealkv-eventual-defaults" => Box::new(skv::Skv::open(&p.dir, false, true)),
        other => panic!("unknown engine {other}"),
    }
}

// ---------------------------------------------------------------- parameters

#[derive(Clone)]
struct Params {
    engine: String,
    label: String,
    dir: PathBuf,
    collections: u32,
    hot: u32,
    docs: u64,
    threads: u64,
    gets: u64,
    scans: u64,
    updates: u64,
    sync_commits: u64,
    mixed_secs: u64,
    settle_secs: u64,
    crash_secs: u64,
    /// Full passes per scan phase, and threads of the parallel scan.
    passes: u64,
    scan_threads: usize,
    /// Load the scan collection in random key order (else ascending).
    scan_random: bool,
    /// Run only this scan phase (for profiling one path).
    only: Option<String>,
    /// Sustained writes: fsync every commit.
    sync: bool,
    rocks: RocksTuning,
    soli: SoliTuning,
}

impl Params {
    fn parse() -> (String, Params) {
        let args: Vec<String> = std::env::args().collect();
        let mode = args.get(1).cloned().unwrap_or_default();
        let mut p = Params {
            engine: String::new(),
            label: String::new(),
            dir: PathBuf::new(),
            collections: 1232,
            hot: 16,
            docs: 5_000_000,
            threads: 8,
            gets: 2_000_000,
            scans: 50_000,
            updates: 1_000_000,
            sync_commits: 2_000,
            mixed_secs: 30,
            settle_secs: 30,
            crash_secs: 3,
            passes: 5,
            scan_threads: 8,
            scan_random: true,
            only: None,
            sync: false,
            rocks: RocksTuning::default(),
            soli: SoliTuning::default(),
        };
        let mut i = 2;
        while i + 1 < args.len() {
            let value = args[i + 1].clone();
            match args[i].as_str() {
                "--engine" => p.engine = value,
                "--label" => p.label = value,
                "--dir" => p.dir = PathBuf::from(value),
                "--collections" => p.collections = value.parse().unwrap(),
                "--hot" => p.hot = value.parse().unwrap(),
                "--docs" => p.docs = value.parse().unwrap(),
                "--threads" => p.threads = value.parse().unwrap(),
                "--gets" => p.gets = value.parse().unwrap(),
                "--scans" => p.scans = value.parse().unwrap(),
                "--updates" => p.updates = value.parse().unwrap(),
                "--sync-commits" => p.sync_commits = value.parse().unwrap(),
                "--mixed-secs" => p.mixed_secs = value.parse().unwrap(),
                "--settle-secs" => p.settle_secs = value.parse().unwrap(),
                "--secs" => p.crash_secs = value.parse().unwrap(),
                "--rx-cache-index" => p.rocks.cache_index = value == "1",
                "--rx-partitioned" => p.rocks.partitioned = value == "1",
                "--rx-wbm-mb" => p.rocks.wbm_mb = Some(value.parse().unwrap()),
                "--rx-hcc" => p.rocks.hyper_clock = value == "1",
                "--sx-buffer-mb" => p.soli.buffer_mb = Some(value.parse().unwrap()),
                "--sx-compactions" => p.soli.compactions = Some(value.parse().unwrap()),
                "--sx-recycle-wals" => p.soli.recycle_wals = Some(value.parse().unwrap()),
                "--sx-l0-slowdown" => p.soli.l0_slowdown = Some(value.parse().unwrap()),
                "--cache-mb" => {
                    p.rocks.cache_mb = Some(value.parse().unwrap());
                    p.soli.cache_mb = Some(value.parse().unwrap());
                }
                "--passes" => p.passes = value.parse().unwrap(),
                "--scan-threads" => p.scan_threads = value.parse().unwrap(),
                "--order" => p.scan_random = value == "random",
                "--only" => p.only = Some(value),
                "--sync" => p.sync = value == "1",
                flag => panic!("unknown flag {flag}"),
            }
            i += 2;
        }
        assert!(
            !p.engine.is_empty() && !p.dir.as_os_str().is_empty(),
            "--engine and --dir are required"
        );
        if p.label.is_empty() {
            p.label = p.engine.clone();
        }
        (mode, p)
    }

    /// The flags a child process needs to open the same engine the same way.
    fn engine_args(&self) -> Vec<String> {
        let mut args = vec![
            "--engine".into(),
            self.engine.clone(),
            "--dir".into(),
            self.dir.display().to_string(),
        ];
        let switches = [
            ("--rx-cache-index", self.rocks.cache_index),
            ("--rx-partitioned", self.rocks.partitioned),
            ("--rx-hcc", self.rocks.hyper_clock),
        ];
        for (flag, on) in switches {
            args.extend([flag.to_string(), if on { "1" } else { "0" }.to_string()]);
        }
        if let Some(mb) = self.rocks.wbm_mb {
            args.extend(["--rx-wbm-mb".to_string(), mb.to_string()]);
        }
        if let Some(mb) = self.soli.buffer_mb {
            args.extend(["--sx-buffer-mb".to_string(), mb.to_string()]);
        }
        if let Some(n) = self.soli.compactions {
            args.extend(["--sx-compactions".to_string(), n.to_string()]);
        }
        if let Some(n) = self.soli.recycle_wals {
            args.extend(["--sx-recycle-wals".to_string(), n.to_string()]);
        }
        if let Some(mb) = self.soli.cache_mb {
            args.extend(["--cache-mb".to_string(), mb.to_string()]);
        }
        args
    }

    /// Collections 1..=hot hold the bulk; one write in 50 lands in one of
    /// the cold ones, so every collection ends up with a little data.
    fn coll_of(&self, n: u64) -> u32 {
        if n % 50 == 49 {
            let cold = (self.collections - self.hot) as u64;
            self.hot + 1 + ((n / 50) % cold) as u32
        } else {
            1 + (n % self.hot as u64) as u32
        }
    }
}

// ---------------------------------------------------------------- documents

const WORDS: &[&str] = &[
    "alpha", "river", "stone", "garden", "signal", "market", "orbit", "paper", "copper", "harbor",
    "winter", "lantern", "meadow", "engine", "silver", "canyon", "velvet", "pixel", "summit",
    "echo",
];
const CITIES: &[&str] = &[
    "paris", "lyon", "berlin", "madrid", "rome", "oslo", "lisbon", "vienna",
];

fn doc_key(n: u64) -> Vec<u8> {
    format!("doc:{n:020}").into_bytes()
}

fn age_key(age: u8, n: u64) -> Vec<u8> {
    let mut key = b"idx:age:".to_vec();
    key.push(age);
    key.extend_from_slice(format!(":{n:020}").as_bytes());
    key
}

fn document(n: u64, age: u8, city: &str, rng: &mut StdRng) -> Vec<u8> {
    let bio: Vec<&str> = (0..40)
        .map(|_| WORDS[rng.gen_range(0..WORDS.len())])
        .collect();
    format!(
        r#"{{"_key":"{n:020}","name":"user {n}","email":"user{n}@example.com","age":{age},"city":"{city}","bio":"{}","score":{}}}"#,
        bio.join(" "),
        rng.gen_range(0..1_000_000)
    )
    .into_bytes()
}

/// `"age":<n>` out of a stored document.
fn age_of(doc: &[u8]) -> u8 {
    let text = std::str::from_utf8(doc).unwrap();
    let start = text.find("\"age\":").unwrap() + 6;
    let end = start + text[start..].find(',').unwrap();
    text[start..end].parse().unwrap()
}

/// The document and its three index entries — what one SoliDB insert writes.
fn insert_ops(p: &Params, n: u64, rng: &mut StdRng) -> Vec<Op> {
    let coll = p.coll_of(n);
    let age: u8 = rng.gen_range(18..90);
    let city = CITIES[rng.gen_range(0..CITIES.len())];
    vec![
        Op {
            coll,
            key: doc_key(n),
            value: Some(document(n, age, city, rng)),
        },
        Op {
            coll,
            key: age_key(age, n),
            value: Some(Vec::new()),
        },
        Op {
            coll,
            key: format!("idx:email:user{n}@example.com:{n:020}").into_bytes(),
            value: Some(Vec::new()),
        },
        Op {
            coll,
            key: format!("idx:city:{city}:{n:020}").into_bytes(),
            value: Some(Vec::new()),
        },
    ]
}

// ---------------------------------------------------------------- measurement

/// Process counters at a point in time.
#[derive(Clone, Copy)]
struct Counters {
    at: Instant,
    cpu_ticks: u64,
    write_bytes: u64,
    read_bytes: u64,
}

fn proc_field(path: &str, field: &str) -> u64 {
    std::fs::read_to_string(path)
        .ok()
        .and_then(|s| {
            s.lines()
                .find(|l| l.starts_with(field))
                .and_then(|l| l.split_whitespace().nth(1))
                .and_then(|v| v.parse().ok())
        })
        .unwrap_or(0)
}

fn counters() -> Counters {
    // utime + stime, in clock ticks (fields 14 and 15 of /proc/self/stat).
    let stat = std::fs::read_to_string("/proc/self/stat").unwrap_or_default();
    let after_comm = stat.rsplit(") ").next().unwrap_or("");
    let fields: Vec<&str> = after_comm.split_whitespace().collect();
    let cpu_ticks = fields
        .get(11)
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(0)
        + fields
            .get(12)
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0);
    Counters {
        at: Instant::now(),
        cpu_ticks,
        // Bytes this process caused to be sent to the storage layer.
        write_bytes: proc_field("/proc/self/io", "write_bytes:"),
        read_bytes: proc_field("/proc/self/io", "read_bytes:"),
    }
}

fn dir_bytes(dir: &Path) -> u64 {
    let mut total = 0;
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let Ok(meta) = entry.metadata() else { continue };
            total += if meta.is_dir() {
                dir_bytes(&entry.path())
            } else {
                meta.len()
            };
        }
    }
    total
}

fn percentile(sorted: &[u32], pct: f64) -> u32 {
    if sorted.is_empty() {
        return 0;
    }
    sorted[((sorted.len() - 1) as f64 * pct).round() as usize]
}

/// One phase's result line.
struct Phase<'a> {
    p: &'a Params,
    name: &'a str,
    ops: u64,
    logical_bytes: u64,
    lat_us: Vec<u32>,
    extra: String,
}

impl<'a> Phase<'a> {
    fn new(p: &'a Params, name: &'a str) -> Self {
        Phase {
            p,
            name,
            ops: 0,
            logical_bytes: 0,
            lat_us: Vec::new(),
            extra: String::new(),
        }
    }

    fn print(mut self, before: Counters) {
        let after = counters();
        self.lat_us.sort_unstable();
        let secs = after.at.duration_since(before.at).as_secs_f64();
        let mb = |b: u64| b as f64 / (1024.0 * 1024.0);
        let written = after.write_bytes.saturating_sub(before.write_bytes);
        let write_amp = if self.logical_bytes > 0 {
            written as f64 / self.logical_bytes as f64
        } else {
            0.0
        };
        println!(
            concat!(
                r#"{{"label":"{}","engine":"{}","phase":"{}","ops":{},"secs":{:.3},"ops_per_sec":{:.0},"#,
                r#""p50_us":{},"p99_us":{},"p999_us":{},"max_us":{},"rss_mb":{},"hwm_mb":{},"disk_mb":{:.0},"#,
                r#""cpu_secs":{:.2},"write_mb":{:.0},"read_mb":{:.0},"logical_mb":{:.0},"write_amp":{:.2}{}}}"#
            ),
            self.p.label,
            self.p.engine,
            self.name,
            self.ops,
            secs,
            if secs > 0.0 {
                self.ops as f64 / secs
            } else {
                0.0
            },
            percentile(&self.lat_us, 0.50),
            percentile(&self.lat_us, 0.99),
            percentile(&self.lat_us, 0.999),
            self.lat_us.last().copied().unwrap_or(0),
            proc_field("/proc/self/status", "VmRSS:") / 1024,
            proc_field("/proc/self/status", "VmHWM:") / 1024,
            mb(dir_bytes(&self.p.dir)),
            (after.cpu_ticks - before.cpu_ticks) as f64 / 100.0,
            mb(written),
            mb(after.read_bytes.saturating_sub(before.read_bytes)),
            mb(self.logical_bytes),
            write_amp,
            {
                let stats = STATS_SOURCE
                    .lock()
                    .unwrap()
                    .as_ref()
                    .map(|e| e.stats())
                    .unwrap_or_default();
                let stats = if stats.is_empty() {
                    String::new()
                } else {
                    format!(r#","engine_stats":{stats}"#)
                };
                format!("{}{stats}", self.extra)
            }
        );
        std::io::stdout().flush().ok();
    }
}

fn timed<T>(lat: &mut Vec<u32>, f: impl FnOnce() -> T) -> T {
    let t = Instant::now();
    let out = f();
    lat.push(t.elapsed().as_micros().min(u32::MAX as u128) as u32);
    out
}

/// What each worker thread hands back.
#[derive(Default)]
struct Tally {
    lat: Vec<u32>,
    logical_bytes: u64,
}

/// Run `work(thread, &mut tally)` on every thread and merge the tallies.
fn parallel<F>(threads: u64, work: F) -> Tally
where
    F: Fn(u64, &mut Tally) + Sync,
{
    let mut all = Tally::default();
    std::thread::scope(|scope| {
        let handles: Vec<_> = (0..threads)
            .map(|t| {
                let work = &work;
                scope.spawn(move || {
                    let mut tally = Tally::default();
                    work(t, &mut tally);
                    tally
                })
            })
            .collect();
        for h in handles {
            let tally = h.join().unwrap();
            all.lat.extend(tally.lat);
            all.logical_bytes += tally.logical_bytes;
        }
    });
    all
}

/// Run a parallel phase and print it.
fn parallel_phase<F>(p: &Params, name: &str, ops: u64, work: F)
where
    F: Fn(u64, &mut Tally) + Sync,
{
    let before = counters();
    let tally = parallel(p.threads, work);
    let mut phase = Phase::new(p, name);
    phase.ops = ops;
    phase.lat_us = tally.lat;
    phase.logical_bytes = tally.logical_bytes;
    phase.print(before);
}

fn settle(engine: &dyn Engine, p: &Params, name: &str) {
    let before = counters();
    engine.flush();
    std::thread::sleep(Duration::from_secs(p.settle_secs));
    Phase::new(p, name).print(before);
}

// ---------------------------------------------------------------- operations

/// Read a document, rewrite it with a new age, swap its idx:age entry.
fn update(engine: &dyn Engine, p: &Params, n: u64, rng: &mut StdRng) -> u64 {
    let coll = p.coll_of(n);
    let old = engine
        .get(coll, &doc_key(n))
        .expect("updating a missing document");
    let old_age = age_of(&old);
    let new_age: u8 = rng.gen_range(18..90);
    let city = CITIES[rng.gen_range(0..CITIES.len())];
    let ops = vec![
        Op {
            coll,
            key: doc_key(n),
            value: Some(document(n, new_age, city, rng)),
        },
        Op {
            coll,
            key: age_key(old_age, n),
            value: None,
        },
        Op {
            coll,
            key: age_key(new_age, n),
            value: Some(Vec::new()),
        },
    ];
    let bytes = ops.iter().map(Op::logical_bytes).sum();
    engine.write(ops, false);
    bytes
}

/// 100 entries of idx:age in one hot collection, then each document.
fn index_scan(engine: &dyn Engine, p: &Params, rng: &mut StdRng, rev: bool) {
    let coll = rng.gen_range(1..=p.hot);
    let age: u8 = rng.gen_range(18..89);
    let mut from = b"idx:age:".to_vec();
    from.push(age);
    let mut to = b"idx:age:".to_vec();
    to.push(age + 1);
    for key in engine.scan_keys(coll, &from, &to, rev, 100) {
        let mut pk = b"doc:".to_vec();
        pk.extend_from_slice(&key[key.len() - 20..]);
        // An update may have moved the document to another age meanwhile;
        // the index entry then points at a document that still exists.
        assert!(engine.get(coll, &pk).is_some());
    }
}

// ---------------------------------------------------------------- run

fn run(p: Params) {
    std::fs::create_dir_all(&p.dir).unwrap();
    let engine: Arc<dyn Engine> = Arc::from(open_engine(&p));
    *STATS_SOURCE.lock().unwrap() = Some(engine.clone());

    // ~1,232 collections exist.
    let before = counters();
    let mut phase = Phase::new(&p, "create_collections");
    for coll in 1..=p.collections {
        timed(&mut phase.lat_us, || engine.create_collection(coll));
    }
    phase.ops = p.collections as u64;
    phase.print(before);

    // Bulk load: one batch per document (doc + 3 index entries), no fsync.
    parallel_phase(&p, "load", p.docs, |t, tally| {
        let mut rng = StdRng::seed_from_u64(t);
        let mut n = t;
        while n < p.docs {
            let ops = insert_ops(&p, n, &mut rng);
            tally.logical_bytes += ops.iter().map(Op::logical_bytes).sum::<u64>();
            timed(&mut tally.lat, || engine.write(ops, false));
            n += p.threads;
        }
    });

    // Transaction commits with fsync: one writer, then eight at once (where
    // an engine that groups commits shares one fsync among them).
    let next = AtomicU64::new(p.docs);
    for (name, threads) in [("sync_commit_1t", 1u64), ("sync_commit_8t", 8)] {
        let before = counters();
        let tally = parallel(threads, |t, tally| {
            let mut rng = StdRng::seed_from_u64(1_000 + t);
            for _ in 0..p.sync_commits / threads {
                let n = next.fetch_add(1, Ordering::SeqCst);
                let ops = insert_ops(&p, n, &mut rng);
                tally.logical_bytes += ops.iter().map(Op::logical_bytes).sum::<u64>();
                timed(&mut tally.lat, || engine.write(ops, true));
            }
        });
        let mut phase = Phase::new(&p, name);
        phase.ops = p.sync_commits;
        phase.lat_us = tally.lat;
        phase.logical_bytes = tally.logical_bytes;
        phase.print(before);
    }

    settle(engine.as_ref(), &p, "settle_after_load");

    // Primary-key gets: uniform over 5M documents (mostly uncached), a hot
    // set (90% of reads on 1% of keys), and keys that do not exist.
    parallel_phase(&p, "get_uniform", p.gets, |t, tally| {
        let mut rng = StdRng::seed_from_u64(10_000 + t);
        for _ in 0..p.gets / p.threads {
            let n = rng.gen_range(0..p.docs);
            timed(&mut tally.lat, || {
                assert!(
                    engine.get(p.coll_of(n), &doc_key(n)).is_some(),
                    "missing doc {n}"
                )
            });
        }
    });
    parallel_phase(&p, "get_hot", p.gets, |t, tally| {
        let mut rng = StdRng::seed_from_u64(11_000 + t);
        let hot = (p.docs / 100).max(1);
        for _ in 0..p.gets / p.threads {
            let n = if rng.gen_bool(0.9) {
                rng.gen_range(0..hot)
            } else {
                rng.gen_range(0..p.docs)
            };
            timed(&mut tally.lat, || {
                assert!(engine.get(p.coll_of(n), &doc_key(n)).is_some())
            });
        }
    });
    parallel_phase(&p, "get_missing", p.gets / 2, |t, tally| {
        let mut rng = StdRng::seed_from_u64(12_000 + t);
        for _ in 0..p.gets / 2 / p.threads {
            let n = p.docs * 10 + rng.gen_range(0..p.docs);
            timed(&mut tally.lat, || {
                assert!(engine.get(p.coll_of(n), &doc_key(n)).is_none())
            });
        }
    });

    for rev in [false, true] {
        parallel_phase(
            &p,
            if rev { "scan_desc_100" } else { "scan_asc_100" },
            p.scans,
            |t, tally| {
                let mut rng = StdRng::seed_from_u64(20_000 + t + rev as u64 * 100);
                for _ in 0..p.scans / p.threads {
                    timed(&mut tally.lat, || {
                        index_scan(engine.as_ref(), &p, &mut rng, rev)
                    });
                }
            },
        );
    }

    // Updates: read, rewrite, swap one index entry.
    parallel_phase(&p, "update", p.updates, |t, tally| {
        let mut rng = StdRng::seed_from_u64(30_000 + t);
        for _ in 0..p.updates / p.threads {
            let n = rng.gen_range(0..p.docs);
            let bytes = timed(&mut tally.lat, || update(engine.as_ref(), &p, n, &mut rng));
            tally.logical_bytes += bytes;
        }
    });

    // Mixed: 70% gets, 10% index scans, 20% updates, per-second series.
    let start = Instant::now();
    let before = counters();
    let secs = p.mixed_secs as usize;
    let mut series_ops = vec![0u64; secs];
    let mut series_lat: Vec<Vec<u32>> = vec![Vec::new(); secs];
    let mut all_lat = Vec::new();
    let mut logical = 0u64;
    std::thread::scope(|scope| {
        let handles: Vec<_> = (0..p.threads)
            .map(|t| {
                let (engine, p) = (&engine, &p);
                scope.spawn(move || {
                    let mut rng = StdRng::seed_from_u64(40_000 + t);
                    let mut samples: Vec<(usize, u32)> = Vec::new();
                    let mut bytes = 0u64;
                    loop {
                        let second = start.elapsed().as_secs() as usize;
                        if second >= secs {
                            break;
                        }
                        let roll: f64 = rng.gen();
                        let t0 = Instant::now();
                        if roll < 0.7 {
                            let n = rng.gen_range(0..p.docs);
                            assert!(engine.get(p.coll_of(n), &doc_key(n)).is_some());
                        } else if roll < 0.8 {
                            let rev = rng.gen_bool(0.5);
                            index_scan(engine.as_ref(), p, &mut rng, rev);
                        } else {
                            let n = rng.gen_range(0..p.docs);
                            bytes += update(engine.as_ref(), p, n, &mut rng);
                        }
                        samples.push((
                            second,
                            t0.elapsed().as_micros().min(u32::MAX as u128) as u32,
                        ));
                    }
                    (samples, bytes)
                })
            })
            .collect();
        for h in handles {
            let (samples, bytes) = h.join().unwrap();
            logical += bytes;
            for (second, us) in samples {
                series_ops[second] += 1;
                series_lat[second].push(us);
                all_lat.push(us);
            }
        }
    });
    let p99_series: Vec<u32> = series_lat
        .iter_mut()
        .map(|lat| {
            lat.sort_unstable();
            percentile(lat, 0.99)
        })
        .collect();
    let mut phase = Phase::new(&p, "mixed");
    phase.ops = all_lat.len() as u64;
    phase.lat_us = all_lat;
    phase.logical_bytes = logical;
    phase.extra = format!(r#","series_ops":{series_ops:?},"series_p99_us":{p99_series:?}"#);
    phase.print(before);

    settle(engine.as_ref(), &p, "settle_after_updates");

    // Drops: 100 near-idle collections, then 2 large ones.
    let disk_before = dir_bytes(&p.dir);
    let before = counters();
    let mut phase = Phase::new(&p, "drop_small_x100");
    for coll in (p.hot + 1)..(p.hot + 101) {
        timed(&mut phase.lat_us, || engine.drop_collection(coll));
    }
    phase.ops = 100;
    phase.print(before);
    let before = counters();
    let mut phase = Phase::new(&p, "drop_large_x2");
    for coll in [1, 2] {
        timed(&mut phase.lat_us, || engine.drop_collection(coll));
    }
    phase.ops = 2;
    phase.print(before);
    let before = counters();
    engine.reclaim();
    engine.flush();
    std::thread::sleep(Duration::from_secs(10));
    let freed = disk_before.saturating_sub(dir_bytes(&p.dir)) as f64 / (1024.0 * 1024.0);
    let mut phase = Phase::new(&p, "reclaim_after_drop");
    phase.extra = format!(r#","freed_mb":{freed:.0}"#);
    phase.print(before);

    // Restart, with a deadline: an engine that hangs on reopen (stock fjall
    // 3.1.11 did, in `Database::recover`) is a result to record, not a
    // reason to stall the whole benchmark.
    let before = counters();
    let (tx, rx) = std::sync::mpsc::channel();
    let reopen_params = p.clone();
    STATS_SOURCE.lock().unwrap().take();
    std::thread::spawn(move || {
        let close_start = Instant::now();
        drop(engine);
        let close_secs = close_start.elapsed().as_secs_f64();
        let engine = open_engine(&reopen_params);
        let rp = &reopen_params;
        for coll in (3..=rp.hot).chain((rp.hot + 101)..=rp.collections) {
            engine.create_collection(coll);
        }
        let n = (0..rp.docs).find(|n| rp.coll_of(*n) == 3).unwrap();
        let found = engine.get(3, &doc_key(n)).is_some();
        tx.send((close_secs, found)).ok();
        // Keep the reopened engine alive until the process exits.
        std::thread::park();
    });
    let mut phase = Phase::new(&p, "close_reopen");
    phase.ops = 1;
    match rx.recv_timeout(Duration::from_secs(120)) {
        Ok((close_secs, found)) => {
            assert!(found, "doc lost across restart");
            phase.extra = format!(r#","close_secs":{close_secs:.3},"hung":false"#);
            phase.print(before);
        }
        Err(_) => {
            phase.extra = r#","hung":true"#.to_string();
            phase.print(before);
            std::process::exit(0);
        }
    }
}

// ---------------------------------------------------------------- full scans

/// The collection the scan mode fills, and the range it reads: its documents.
const SCAN_COLL: u32 = 1;
const SCAN_FROM: &[u8] = b"doc:";
const SCAN_TO: &[u8] = b"doc;";

/// A document of about 430 bytes: the bench's usual shape, a longer bio.
fn scan_document(n: u64, rng: &mut StdRng) -> Vec<u8> {
    let bio: Vec<&str> = (0..45)
        .map(|_| WORDS[rng.gen_range(0..WORDS.len())])
        .collect();
    format!(
        r#"{{"_key":"{n:020}","name":"user {n}","email":"user{n}@example.com","age":{},"city":"{}","bio":"{}","score":{}}}"#,
        rng.gen_range(18..90),
        CITIES[rng.gen_range(0..CITIES.len())],
        bio.join(" "),
        rng.gen_range(0..1_000_000)
    )
    .into_bytes()
}

/// The engine at `p.dir` holding the scan collection: `docs` documents
/// loaded in batches of 100 (in random key order unless `--order seq`),
/// flushed and settled, unless a run with the same `--docs` already did,
/// so every binary reads the same tables.
fn scan_collection(p: &Params) -> Arc<dyn Engine> {
    std::fs::create_dir_all(&p.dir).unwrap();
    let marker = p.dir.join(format!("scan-loaded-{}", p.docs));
    let engine: Arc<dyn Engine> = Arc::from(open_engine(p));
    *STATS_SOURCE.lock().unwrap() = Some(engine.clone());
    if !marker.exists() {
        engine.create_collection(SCAN_COLL);
        load_documents(p, engine.as_ref(), "scan_load", p.docs, p.scan_random);
        settle(engine.as_ref(), p, "scan_settle");
        std::fs::write(&marker, b"").unwrap();
    }
    engine
}

/// Documents `0..docs` of the scan collection, in batches of 100 as an
/// import would write them, on `p.threads` writers.
fn load_documents(p: &Params, engine: &dyn Engine, name: &str, docs: u64, random: bool) {
    let mut order: Vec<u64> = (0..docs).collect();
    if random {
        use rand::seq::SliceRandom;
        order.shuffle(&mut StdRng::seed_from_u64(99));
    }
    let batches: Vec<&[u64]> = order.chunks(100).collect();
    parallel_phase(p, name, docs, |t, tally| {
        let mut rng = StdRng::seed_from_u64(50_000 + t);
        for batch in batches.iter().skip(t as usize).step_by(p.threads as usize) {
            let ops: Vec<Op> = batch
                .iter()
                .map(|&n| Op {
                    coll: SCAN_COLL,
                    key: doc_key(n),
                    value: Some(scan_document(n, &mut rng)),
                })
                .collect();
            tally.logical_bytes += ops.iter().map(Op::logical_bytes).sum::<u64>();
            timed(&mut tally.lat, || engine.write(ops, false));
        }
    });
}

/// Whole-collection reads, as SoliDB's full scans and aggregations do them:
/// `docs` documents of ~430 B in one collection (keys `doc:<n>`, under the
/// 8-byte prefix), loaded in random key order unless `--order seq`, then
/// flushed and left to settle. Then, warm, `passes` forward passes per
/// phase that only count entries and bytes:
///
/// - `full_scan`: the engine's ordinary iterator, one thread;
/// - `full_scan_ref`: its copy-free path (RocksDB's raw iterator);
/// - `parallel_scan_N`: one consistent read split into N ranges, one
///   thread each.
///
/// Each phase's latencies are per pass; `ops_per_sec` is entries a second.
/// The data directory is kept, marked as loaded, and reused by the next run
/// with the same `--docs`: every binary then reads the same tables.
fn scan(p: Params) {
    let engine = scan_collection(&p);

    // Cuts for the parallel scan where the engine does not choose its own:
    // the documents are numbered 0..docs, so equal counts are equal sizes.
    let threads = p.scan_threads.max(1) as u64;
    let cuts: Vec<Vec<u8>> = (1..threads)
        .map(|i| doc_key(p.docs * i / threads))
        .collect();

    let check = |(n, bytes): (u64, u64)| {
        assert_eq!(n, p.docs, "a pass saw {n} documents");
        bytes
    };
    // Warm the cache (and the OS's) with one pass of each path.
    let before = counters();
    let mut warm = Phase::new(&p, "scan_warm");
    timed(&mut warm.lat_us, || {
        check(engine.full_scan(SCAN_COLL, SCAN_FROM, SCAN_TO))
    });
    timed(&mut warm.lat_us, || {
        check(engine.full_scan_ref(SCAN_COLL, SCAN_FROM, SCAN_TO))
    });
    warm.ops = 2 * p.docs;
    warm.print(before);

    let name = format!("parallel_scan_{threads}");
    type Pass<'a> = &'a dyn Fn() -> (u64, u64);
    let phases: [(&str, Pass); 3] = [
        ("full_scan", &|| {
            engine.full_scan(SCAN_COLL, SCAN_FROM, SCAN_TO)
        }),
        ("full_scan_ref", &|| {
            engine.full_scan_ref(SCAN_COLL, SCAN_FROM, SCAN_TO)
        }),
        (name.as_str(), &|| {
            engine.parallel_scan(SCAN_COLL, SCAN_FROM, SCAN_TO, &cuts)
        }),
    ];
    for (name, pass) in phases {
        if p.only.as_deref().is_some_and(|only| only != name) {
            continue;
        }
        let before = counters();
        let mut phase = Phase::new(&p, name);
        let mut bytes = 0;
        for _ in 0..p.passes {
            bytes = timed(&mut phase.lat_us, || check(pass()));
        }
        let ms: Vec<String> = phase
            .lat_us
            .iter()
            .map(|us| format!("{:.1}", *us as f64 / 1000.0))
            .collect();
        phase.ops = p.passes * p.docs;
        phase.extra = format!(
            r#","entries":{},"bytes_per_pass":{bytes},"pass_ms":[{}]"#,
            p.docs,
            ms.join(",")
        );
        phase.print(before);
    }

    // Point reads of the same documents, warm: one key per call on one
    // thread and on `threads`, then `multi_get` of 10 keys (an index
    // lookup's documents) on `threads`.
    let gets = p.gets;
    let read_phase = |name: &str, threads: u64, batch: usize| {
        if p.only.as_deref().is_some_and(|only| only != name) {
            return;
        }
        let before = counters();
        let tally = parallel(threads, |t, tally| {
            let mut rng = StdRng::seed_from_u64(60_000 + t);
            for _ in 0..gets / threads / batch as u64 {
                let keys: Vec<Vec<u8>> = (0..batch)
                    .map(|_| doc_key(rng.gen_range(0..p.docs)))
                    .collect();
                let found = timed(&mut tally.lat, || {
                    if batch == 1 {
                        engine.get(SCAN_COLL, &keys[0]).is_some() as usize
                    } else {
                        engine.multi_get(SCAN_COLL, &keys)
                    }
                });
                assert_eq!(found, batch);
            }
        });
        let mut phase = Phase::new(&p, name);
        // Documents read a second.
        phase.ops = gets;
        phase.lat_us = tally.lat;
        phase.print(before);
    };
    read_phase("get_1t", 1, 1);
    read_phase(&format!("get_{}t", p.threads), p.threads, 1);
    read_phase(&format!("multi_get_10_{}t", p.threads), p.threads, 10);
}

// ---------------------------------------------------------------- point reads

/// The CPU's user-space instruction and cycle counters for the calling
/// thread (Linux, with perf_event_paranoid at 2 or below; `None` elsewhere).
#[cfg(target_os = "linux")]
mod cpu {
    /// `perf_event_attr` up to `config2` (PERF_ATTR_SIZE_VER1).
    #[repr(C)]
    #[derive(Default)]
    struct Attr {
        kind: u32,
        size: u32,
        config: u64,
        sample_period: u64,
        sample_type: u64,
        read_format: u64,
        flags: u64,
        wakeup_events: u32,
        bp_type: u32,
        config1: u64,
        config2: u64,
    }

    const DISABLED: u64 = 1;
    const EXCLUDE_KERNEL: u64 = 1 << 5;
    const EXCLUDE_HV: u64 = 1 << 6;
    const IOC_ENABLE: u64 = 0x2400;
    const IOC_DISABLE: u64 = 0x2401;
    const HW_CPU_CYCLES: u64 = 0;
    const HW_INSTRUCTIONS: u64 = 1;

    pub struct Counters([i32; 2]);

    fn open(config: u64) -> Option<i32> {
        let attr = Attr {
            size: std::mem::size_of::<Attr>() as u32,
            config,
            flags: DISABLED | EXCLUDE_KERNEL | EXCLUDE_HV,
            ..Attr::default()
        };
        // SAFETY: a valid attribute struct of the size it declares; this
        // thread (pid 0) on any CPU, no group, no flags.
        let fd = unsafe {
            libc::syscall(
                libc::SYS_perf_event_open,
                &attr as *const Attr,
                0,
                -1,
                -1,
                0,
            )
        };
        (fd >= 0).then_some(fd as i32)
    }

    /// Start counting this thread's instructions and cycles.
    pub fn start() -> Option<Counters> {
        let instructions = open(HW_INSTRUCTIONS)?;
        let Some(cycles) = open(HW_CPU_CYCLES) else {
            // SAFETY: a descriptor this function opened.
            unsafe { libc::close(instructions) };
            return None;
        };
        for fd in [instructions, cycles] {
            // SAFETY: a perf event descriptor; ENABLE takes no argument.
            unsafe { libc::ioctl(fd, IOC_ENABLE as _, 0) };
        }
        Some(Counters([instructions, cycles]))
    }

    impl Counters {
        /// Instructions and cycles since `start`.
        pub fn stop(self) -> [u64; 2] {
            let mut out = [0u64; 2];
            for (fd, value) in self.0.iter().zip(&mut out) {
                // SAFETY: descriptors `start` opened, read into a u64 as a
                // counter without read_format reads.
                unsafe {
                    libc::ioctl(*fd, IOC_DISABLE as _, 0);
                    libc::read(*fd, (value as *mut u64).cast(), 8);
                    libc::close(*fd);
                }
            }
            out
        }
    }
}

#[cfg(not(target_os = "linux"))]
mod cpu {
    pub struct Counters;

    pub fn start() -> Option<Counters> {
        None
    }

    impl Counters {
        pub fn stop(self) -> [u64; 2] {
            [0, 0]
        }
    }
}

/// How a point phase reads.
#[derive(Clone, Copy, PartialEq)]
enum Read {
    Get,
    Multi,
    MultiRef,
}

struct PointPhase {
    name: String,
    threads: u64,
    batch: usize,
    read: Read,
    seq: bool,
}

impl PointPhase {
    fn new(name: impl Into<String>, threads: u64, batch: usize, read: Read, seq: bool) -> Self {
        PointPhase {
            name: name.into(),
            threads,
            batch,
            read,
            seq,
        }
    }
}

/// `calls` random reads of `batch` documents of `0..docs` per thread, on
/// `threads` threads, keys built beforehand (a pool of up to 64k per
/// thread, read in turn) and handles taken once per thread: what is
/// measured is the engine's call. With `seq`, each call reads `batch`
/// consecutive documents, as an index whose entries follow the key order
/// would ask. Per thread, wall time and the CPU's counters cover the
/// calls alone; one call in eight is timed for the latency percentiles.
fn point_phase(p: &Params, engine: &dyn Engine, docs: u64, phase: &PointPhase) {
    let PointPhase {
        ref name,
        threads,
        batch,
        read,
        seq,
    } = *phase;
    let name = name.as_str();
    if p.only.as_deref().is_some_and(|only| only != name) {
        return;
    }
    let calls = (p.gets / threads / batch as u64).max(1);
    let before = counters();
    let results: Vec<(Vec<u32>, u64, Option<[u64; 2]>)> = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..threads)
            .map(|t| {
                scope.spawn(move || {
                    let reader = engine.point_reader();
                    let mut rng = StdRng::seed_from_u64(70_000 + t);
                    let groups = (calls as usize).min((1 << 16) / batch).max(1);
                    let pool: Vec<Vec<u8>> = (0..groups)
                        .flat_map(|_| {
                            let start = rng.gen_range(0..docs - batch as u64 + 1);
                            let keys: Vec<u64> = if seq {
                                (start..start + batch as u64).collect()
                            } else {
                                (0..batch).map(|_| rng.gen_range(0..docs)).collect()
                            };
                            keys.into_iter()
                                .map(|n| prefixed(SCAN_COLL, &doc_key(n)))
                                .collect::<Vec<_>>()
                        })
                        .collect();
                    let refs: Vec<&[u8]> = pool.iter().map(|k| k.as_slice()).collect();
                    let mut lat = Vec::with_capacity(calls as usize / 8 + 1);
                    let (mut found, mut bytes) = (0usize, 0usize);
                    let counters = cpu::start();
                    let begin = Instant::now();
                    for call in 0..calls as usize {
                        let at = (call % groups) * batch;
                        let keys = &refs[at..at + batch];
                        let t0 = (call % 8 == 0).then(Instant::now);
                        let (f, b) = match read {
                            Read::Get => reader.get(keys[0]).map_or((0, 0), |len| (1, len)),
                            Read::Multi => reader.multi_get(keys),
                            Read::MultiRef => reader.multi_get_ref(keys),
                        };
                        if let Some(t0) = t0 {
                            lat.push(t0.elapsed().as_nanos().min(u32::MAX as u128) as u32);
                        }
                        found += f;
                        bytes += b;
                    }
                    let busy = begin.elapsed().as_nanos() as u64;
                    let counted = counters.map(cpu::Counters::stop);
                    assert_eq!(found, calls as usize * batch, "{name}: documents missing");
                    assert!(bytes > 0);
                    (lat, busy, counted)
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });
    let docs_read = calls * threads * batch as u64;
    let busy_ns: u64 = results.iter().map(|r| r.1).sum();
    let mut lat: Vec<u32> = results.iter().flat_map(|r| r.0.iter().copied()).collect();
    lat.sort_unstable();
    let per_doc = |v: u64| v as f64 / docs_read as f64;
    let cpu = results.iter().map(|r| r.2).try_fold([0u64; 2], |acc, c| {
        c.map(|c| [acc[0] + c[0], acc[1] + c[1]])
    });
    let mut phase = Phase::new(p, name);
    phase.ops = docs_read;
    phase.lat_us = lat.iter().map(|ns| ns / 1000).collect();
    phase.extra = format!(
        concat!(
            r#","threads":{},"batch":{},"seq":{},"ns_per_doc":{:.1},"call_p50_ns":{},"call_p99_ns":{},"#,
            r#""instr_per_doc":{:.0},"cycles_per_doc":{:.0}"#
        ),
        threads,
        batch,
        seq,
        per_doc(busy_ns),
        percentile(&lat, 0.50),
        percentile(&lat, 0.99),
        cpu.map_or(0.0, |c| per_doc(c[0])),
        cpu.map_or(0.0, |c| per_doc(c[1])),
    );
    phase.print(before);
}

/// Point reads, warm, as SoliDB fetches the documents of a query:
/// `get` one key at a time, and `multi_get` of 10 and 20 keys (RocksDB's
/// `batched_multi_get_cf`, as SoliDB calls it), on one thread (a query at
/// concurrency 1) and on `--threads`, over the scan mode's collection (and
/// its data directory, which both modes share). Then the same reads of
/// documents still in the memtable, on a fresh database next to it.
///
/// Every phase line has `ns_per_doc` (thread time per document read),
/// the per-call latency percentiles in ns, and, on Linux, instructions
/// and cycles per document from the CPU's counters.
fn point(p: Params) {
    let engine = scan_collection(&p);
    // Every block into the cache.
    let before = counters();
    let mut warm = Phase::new(&p, "point_warm");
    timed(&mut warm.lat_us, || {
        engine.full_scan_ref(SCAN_COLL, SCAN_FROM, SCAN_TO)
    });
    warm.ops = p.docs;
    warm.print(before);

    let t = p.threads;
    let phases = [
        PointPhase::new("get_1t", 1, 1, Read::Get, false),
        PointPhase::new(format!("get_{t}t"), t, 1, Read::Get, false),
        PointPhase::new("multi_get_10_1t", 1, 10, Read::Multi, false),
        PointPhase::new("multi_get_20_1t", 1, 20, Read::Multi, false),
        PointPhase::new("multi_get_ref_10_1t", 1, 10, Read::MultiRef, false),
        PointPhase::new("multi_get_ref_20_1t", 1, 20, Read::MultiRef, false),
        PointPhase::new(format!("multi_get_10_{t}t"), t, 10, Read::Multi, false),
        PointPhase::new(format!("multi_get_20_{t}t"), t, 20, Read::Multi, false),
        PointPhase::new("multi_get_seq_20_1t", 1, 20, Read::Multi, true),
    ];
    for phase in &phases {
        point_phase(&p, engine.as_ref(), p.docs, phase);
    }
    *STATS_SOURCE.lock().unwrap() = None;
    drop(engine);

    // Recently written documents: 100k (about 45 MB, one memtable for
    // both engines), not flushed.
    let mem = Params {
        dir: p.dir.with_extension("mem"),
        ..p.clone()
    };
    let docs = p.docs.min(100_000);
    let _ = std::fs::remove_dir_all(&mem.dir);
    std::fs::create_dir_all(&mem.dir).unwrap();
    {
        let engine: Arc<dyn Engine> = Arc::from(open_engine(&mem));
        *STATS_SOURCE.lock().unwrap() = Some(engine.clone());
        engine.create_collection(SCAN_COLL);
        load_documents(&mem, engine.as_ref(), "mem_load", docs, true);
        let phases = [
            PointPhase::new("mem_get_1t", 1, 1, Read::Get, false),
            PointPhase::new("mem_multi_get_20_1t", 1, 20, Read::Multi, false),
        ];
        for phase in &phases {
            point_phase(&mem, engine.as_ref(), docs, phase);
        }
        *STATS_SOURCE.lock().unwrap() = None;
    }
    let _ = std::fs::remove_dir_all(&mem.dir);
}

// ---------------------------------------------------------------- sustained writes

/// SoliDB's inserts, sustained: `--threads` writers each commit one document
/// with its three index entries per batch (fsynced with `--sync 1`) for
/// `--mixed-secs` seconds, on a database that grows as it goes. One line
/// at the end, with per-second series: operations, p99 (µs), level-0
/// files, and per second writes slowed, ms stopped, batches per commit
/// group, WAL fsyncs and compactions finished. Where the throughput sags
/// as the data grows, the series says whether level 0 did.
fn sustain(p: Params) {
    std::fs::create_dir_all(&p.dir).unwrap();
    let engine: Arc<dyn Engine> = Arc::from(open_engine(&p));
    *STATS_SOURCE.lock().unwrap() = Some(engine.clone());
    for coll in 1..=p.collections {
        engine.create_collection(coll);
    }
    let secs = p.mixed_secs as usize;
    let next = AtomicU64::new(0);
    let stop = AtomicBool::new(false);
    let start = Instant::now();
    let before = counters();
    let mut states = Vec::new();
    let (lat, logical) = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..p.threads)
            .map(|t| {
                let (engine, p, next, stop) = (&engine, &p, &next, &stop);
                scope.spawn(move || {
                    let mut rng = StdRng::seed_from_u64(70_000 + t);
                    let (mut lat, mut bytes) = (Vec::new(), 0u64);
                    while !stop.load(Ordering::Relaxed) {
                        let n = next.fetch_add(1, Ordering::Relaxed);
                        let ops = insert_ops(p, n, &mut rng);
                        bytes += ops.iter().map(Op::logical_bytes).sum::<u64>();
                        let t0 = Instant::now();
                        engine.write(ops, p.sync);
                        let second = start.elapsed().as_secs() as usize;
                        lat.push((
                            second,
                            t0.elapsed().as_micros().min(u32::MAX as u128) as u32,
                        ));
                    }
                    (lat, bytes)
                })
            })
            .collect();
        for s in 1..=secs {
            std::thread::sleep(
                (start + Duration::from_secs(s as u64)).saturating_duration_since(Instant::now()),
            );
            states.push(engine.write_state());
        }
        stop.store(true, Ordering::Relaxed);
        let mut all = Vec::new();
        let mut logical = 0;
        for h in handles {
            let (lat, bytes) = h.join().unwrap();
            all.extend(lat);
            logical += bytes;
        }
        (all, logical)
    });
    let mut per_second: Vec<Vec<u32>> = vec![Vec::new(); secs];
    for &(second, us) in lat.iter().filter(|(s, _)| *s < secs) {
        per_second[second].push(us);
    }
    let ops: Vec<usize> = per_second.iter().map(Vec::len).collect();
    let p99: Vec<u32> = per_second
        .iter_mut()
        .map(|l| {
            l.sort_unstable();
            percentile(l, 0.99)
        })
        .collect();
    let delta = |i: usize| -> Vec<u64> {
        let mut prev = 0;
        states
            .iter()
            .map(|s| {
                let d = s[i] - prev.min(s[i]);
                prev = s[i];
                d
            })
            .collect()
    };
    let (groups, batches) = (delta(3), delta(4));
    let per_group: Vec<String> = groups
        .iter()
        .zip(&batches)
        .map(|(g, b)| format!("{:.1}", *b as f64 / (*g).max(1) as f64))
        .collect();
    let mut phase = Phase::new(&p, if p.sync { "sustain_sync" } else { "sustain" });
    phase.ops = ops.iter().sum::<usize>() as u64;
    phase.lat_us = lat.into_iter().map(|(_, us)| us).collect();
    phase.logical_bytes = logical;
    phase.extra = format!(
        r#","series_ops":{ops:?},"series_p99_us":{p99:?},"series_l0":{:?},"series_slowed":{:?},"series_stop_ms":{:?},"series_batches_per_group":[{}],"series_fsyncs":{:?},"series_compactions":{:?}"#,
        states.iter().map(|s| s[0]).collect::<Vec<_>>(),
        delta(1),
        delta(2),
        per_group.join(","),
        delta(5),
        delta(6),
    );
    phase.print(before);
}

// ---------------------------------------------------------------- crash

/// Child: commit fsync'd batches forever, printing each acknowledged seq.
fn crash_child(p: Params) {
    let engine = open_engine(&p);
    engine.create_collection(1);
    let single = Params {
        collections: 2,
        hot: 1,
        ..p.clone()
    };
    let mut rng = StdRng::seed_from_u64(7);
    let stdout = std::io::stdout();
    for seq in 0.. {
        let mut ops = insert_ops(&single, seq, &mut rng);
        for op in &mut ops {
            op.coll = 1;
        }
        engine.write(ops, true);
        let mut out = stdout.lock();
        writeln!(out, "{seq}").unwrap();
        out.flush().unwrap();
    }
}

/// Parent: SIGKILL the child mid-write, reopen, check every acknowledged
/// document is there and every document has all three index entries.
fn crash(p: Params) {
    let _ = std::fs::remove_dir_all(&p.dir);
    std::fs::create_dir_all(&p.dir).unwrap();
    let exe = std::env::current_exe().unwrap();
    let mut child = Command::new(exe)
        .arg("crash-child")
        .args(p.engine_args())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let acked = Arc::new(AtomicU64::new(u64::MAX));
    let reader = {
        let acked = acked.clone();
        let out = child.stdout.take().unwrap();
        std::thread::spawn(move || {
            for line in BufReader::new(out).lines().map_while(Result::ok) {
                if let Ok(seq) = line.trim().parse() {
                    acked.store(seq, Ordering::SeqCst);
                }
            }
        })
    };
    std::thread::sleep(Duration::from_secs(p.crash_secs));
    child.kill().unwrap(); // SIGKILL
    child.wait().unwrap();
    reader.join().unwrap();
    let last_acked = acked.load(Ordering::SeqCst);
    assert!(last_acked != u64::MAX, "child acknowledged nothing");

    let start = Instant::now();
    let engine = open_engine(&p);
    engine.create_collection(1);
    let reopen = start.elapsed();

    let lost = (0..=last_acked)
        .filter(|seq| engine.get(1, &doc_key(*seq)).is_none())
        .count();
    // Atomicity: a document present without its index entries (or the
    // reverse) means a batch was half applied.
    let docs = engine
        .scan_keys(1, b"doc:", b"doc;", false, usize::MAX)
        .len();
    let ages = engine
        .scan_keys(1, b"idx:age:", b"idx:age;", false, usize::MAX)
        .len();
    let emails = engine
        .scan_keys(1, b"idx:email:", b"idx:email;", false, usize::MAX)
        .len();
    let cities = engine
        .scan_keys(1, b"idx:city:", b"idx:city;", false, usize::MAX)
        .len();
    let torn = ages != docs || emails != docs || cities != docs;
    println!(
        r#"{{"label":"{}","engine":"{}","phase":"crash","acked":{},"lost_acked":{},"docs_found":{},"index_entries":[{},{},{}],"torn_batch":{},"reopen_secs":{:.3}}}"#,
        p.label,
        p.engine,
        last_acked + 1,
        lost,
        docs,
        ages,
        emails,
        cities,
        torn,
        reopen.as_secs_f64()
    );
}

// ---------------------------------------------------------------- visibility

/// Concurrent readers against concurrent writers, checking two properties
/// of what a reader can see while batches are being applied:
/// - read-your-acknowledged-writes: once `write` returned, the document is
///   visible to every thread;
/// - atomic visibility: if a document is visible, the three index entries
///   written in the same batch are visible too.
///
/// Both through `get` and through `multi_get`, which reads its whole
/// batch from one view of the memtables and tables.
fn visibility(p: Params) {
    let _ = std::fs::remove_dir_all(&p.dir);
    std::fs::create_dir_all(&p.dir).unwrap();
    let engine = open_engine(&p);
    for coll in 1..=p.collections {
        engine.create_collection(coll);
    }
    let writers = p.threads;
    // progress[t] = how many of writer t's documents (n = t + k·writers) are acknowledged.
    let progress: Vec<AtomicU64> = (0..writers).map(|_| AtomicU64::new(0)).collect();
    let stop = AtomicBool::new(false);
    let checked = AtomicU64::new(0);
    let missing_acked = AtomicU64::new(0);
    let torn = AtomicU64::new(0);
    std::thread::scope(|scope| {
        for t in 0..writers {
            let (engine, p, progress, stop) = (&engine, &p, &progress, &stop);
            scope.spawn(move || {
                let mut rng = StdRng::seed_from_u64(50_000 + t);
                let mut k = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    let n = t + k * writers;
                    engine.write(insert_ops(p, n, &mut rng), false);
                    k += 1;
                    progress[t as usize].store(k, Ordering::Release);
                }
            });
        }
        for r in 0..4u64 {
            let (engine, p, progress, stop) = (&engine, &p, &progress, &stop);
            let (checked, missing_acked, torn) = (&checked, &missing_acked, &torn);
            scope.spawn(move || {
                let mut rng = StdRng::seed_from_u64(60_000 + r);
                while !stop.load(Ordering::Relaxed) {
                    let t = rng.gen_range(0..writers);
                    let done = progress[t as usize].load(Ordering::Acquire);
                    // An acknowledged document, or one that may be in flight.
                    let (k, acked) = if rng.gen_bool(0.5) && done > 0 {
                        (rng.gen_range(done.saturating_sub(64)..done), true)
                    } else {
                        (done + rng.gen_range(0..4), false)
                    };
                    let n = t + k * writers;
                    let coll = p.coll_of(n);
                    if acked && r >= 2 {
                        // Half the readers also take every acknowledged
                        // document of this writer nearby in the same
                        // collection (about 30) in one batched read, which
                        // takes its own view of memtables and tables.
                        let batch: Vec<Vec<u8>> = (done.saturating_sub(64)..done)
                            .map(|k| t + k * writers)
                            .filter(|&m| p.coll_of(m) == coll)
                            .map(doc_key)
                            .collect();
                        let found = engine.multi_get(coll, &batch);
                        checked.fetch_add(batch.len() as u64, Ordering::Relaxed);
                        missing_acked.fetch_add((batch.len() - found) as u64, Ordering::Relaxed);
                    }
                    checked.fetch_add(1, Ordering::Relaxed);
                    match engine.get(coll, &doc_key(n)) {
                        None if acked => {
                            missing_acked.fetch_add(1, Ordering::Relaxed);
                        }
                        None => {}
                        Some(doc) => {
                            let text = String::from_utf8(doc.clone()).unwrap();
                            let city = text
                                .split("\"city\":\"")
                                .nth(1)
                                .unwrap()
                                .split('"')
                                .next()
                                .unwrap()
                                .to_string();
                            let keys = [
                                age_key(age_of(&doc), n),
                                format!("idx:email:user{n}@example.com:{n:020}").into_bytes(),
                                format!("idx:city:{city}:{n:020}").into_bytes(),
                            ];
                            if keys.iter().any(|key| engine.get(coll, key).is_none()) {
                                torn.fetch_add(1, Ordering::Relaxed);
                            }
                            // The document and its entries again, in one
                            // batched read: it must see what the gets saw.
                            let mut entries = keys.to_vec();
                            entries.push(doc_key(n));
                            if engine.multi_get(coll, &entries) != entries.len() {
                                torn.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                }
            });
        }
        std::thread::sleep(Duration::from_secs(p.crash_secs));
        stop.store(true, Ordering::Relaxed);
    });
    let written: u64 = progress.iter().map(|c| c.load(Ordering::Relaxed)).sum();
    println!(
        r#"{{"label":"{}","engine":"{}","phase":"visibility","secs":{},"written":{},"checked":{},"missing_acked":{},"torn_visible":{}}}"#,
        p.label,
        p.engine,
        p.crash_secs,
        written,
        checked.load(Ordering::Relaxed),
        missing_acked.load(Ordering::Relaxed),
        torn.load(Ordering::Relaxed)
    );
}

fn main() {
    let (mode, params) = Params::parse();
    match mode.as_str() {
        "run" => run(params),
        "crash" => crash(params),
        "crash-child" => crash_child(params),
        "visibility" => visibility(params),
        "scan" => scan(params),
        "point" => point(params),
        "sustain" => sustain(params),
        other => {
            panic!("unknown mode {other:?}: run | crash | visibility | scan | point | sustain")
        }
    }
}
