//! The block cache: decoded data blocks, shared by every table of a
//! database, bounded in bytes.
//!
//! The bound is a budget for table memory as a whole: the index and filter
//! every open table keeps in memory are charged against it, and the cache
//! holds what is left. RocksDB with SoliDB's profile keeps those outside its
//! cache and unbounded; here they cannot grow the process past the budget.

use std::hash::{BuildHasher, Hasher};

use quick_cache::sync::{Cache, DefaultLifecycle};
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

/// Hashes a block key (two integers) with a multiply-xorshift mix. The
/// default, SipHash, guards against chosen keys, which table numbers and
/// file offsets are not; it was ~1% of SoliDB's CPU on point reads.
#[derive(Clone, Copy, Default)]
struct BlockKeyHash;

#[derive(Default)]
struct BlockKeyHasher(u64);

impl BuildHasher for BlockKeyHash {
    type Hasher = BlockKeyHasher;

    fn build_hasher(&self) -> BlockKeyHasher {
        BlockKeyHasher(0)
    }
}

impl Hasher for BlockKeyHasher {
    fn write(&mut self, bytes: &[u8]) {
        for chunk in bytes.chunks(8) {
            let mut word = [0u8; 8];
            word[..chunk.len()].copy_from_slice(chunk);
            self.write_u64(u64::from_le_bytes(word));
        }
    }

    #[inline]
    fn write_u64(&mut self, x: u64) {
        self.0 = (self.0.rotate_left(29) ^ x).wrapping_mul(0x9e37_79b9_7f4a_7c15);
    }

    #[inline]
    fn finish(&self) -> u64 {
        // murmur3's finaliser: every output bit depends on every input bit,
        // which the cache's shard and table indexing rely on.
        let mut h = self.0;
        h ^= h >> 33;
        h = h.wrapping_mul(0xff51_afd7_ed55_8ccd);
        h ^= h >> 33;
        h = h.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
        h ^ (h >> 33)
    }
}

pub struct BlockCache {
    cache: Cache<Key, Block, BlockWeighter, BlockKeyHash>,
    budget: u64,
    /// Index and filter bytes of open tables.
    pinned: std::sync::atomic::AtomicU64,
}

impl BlockCache {
    pub fn new(capacity: u64) -> Self {
        // quick_cache sizes its tables from an item estimate: 4 KiB blocks.
        let items = (capacity / 4096).max(64) as usize;
        BlockCache {
            cache: Cache::with(
                items,
                capacity.max(1),
                BlockWeighter,
                BlockKeyHash,
                DefaultLifecycle::default(),
            ),
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
