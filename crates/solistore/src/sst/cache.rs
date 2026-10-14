//! The block cache: decoded data blocks, shared by every table of a
//! database, bounded in bytes.
//!
//! The bound is a budget for table memory as a whole: the index and filter
//! every open table keeps in memory are charged against it, and the cache
//! holds what is left. RocksDB with SoliDB's profile keeps those outside its
//! cache and unbounded; here they cannot grow the process past the budget.

use quick_cache::sync::Cache;
use quick_cache::Weighter;

use super::block::Block;

/// A block is identified by its table's number and its offset in the file.
type Key = (u64, u64);

#[derive(Clone)]
struct BlockWeighter;

impl Weighter<Key, Block> for BlockWeighter {
    fn weight(&self, _key: &Key, block: &Block) -> u64 {
        // The bytes plus the entry's own bookkeeping.
        block.size() as u64 + 64
    }
}

pub struct BlockCache {
    cache: Cache<Key, Block, BlockWeighter>,
    budget: u64,
    /// Index and filter bytes of open tables.
    pinned: std::sync::atomic::AtomicU64,
}

impl BlockCache {
    pub fn new(capacity: u64) -> Self {
        // quick_cache sizes its tables from an item estimate: 4 KiB blocks.
        let items = (capacity / 4096).max(64) as usize;
        BlockCache {
            cache: Cache::with_weighter(items, capacity.max(1), BlockWeighter),
            budget: capacity.max(1),
            pinned: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// An opened table keeps `bytes` in memory: shrink the cache by as much.
    pub fn charge(&self, bytes: u64) {
        let pinned = self
            .pinned
            .fetch_add(bytes, std::sync::atomic::Ordering::Relaxed)
            + bytes;
        self.resize(pinned);
    }

    /// A table closed: give its bytes back to the cache.
    pub fn release(&self, bytes: u64) {
        let pinned = self
            .pinned
            .fetch_sub(bytes, std::sync::atomic::Ordering::Relaxed)
            - bytes;
        self.resize(pinned);
    }

    fn resize(&self, pinned: u64) {
        // Never below an eighth of the budget: past that point, pinned
        // memory is what the data set needs and starving the cache would
        // only turn every read into a disk read.
        let capacity = self
            .budget
            .saturating_sub(pinned)
            .max(self.budget / 8)
            .max(1);
        self.cache.set_capacity(capacity);
    }

    /// Index and filter bytes held by open tables.
    pub fn pinned(&self) -> u64 {
        self.pinned.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn get(&self, table: u64, offset: u64) -> Option<Block> {
        self.cache.get(&(table, offset))
    }

    pub fn insert(&self, table: u64, offset: u64, block: Block) {
        self.cache.insert((table, offset), block);
    }

    /// Bytes held (RocksDB's `block-cache-usage`).
    pub fn usage(&self) -> u64 {
        self.cache.weight()
    }
}
