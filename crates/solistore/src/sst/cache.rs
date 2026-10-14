//! The block cache: decoded data blocks, shared by every table of a
//! database, bounded in bytes.

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
}

impl BlockCache {
    pub fn new(capacity: u64) -> Self {
        // quick_cache sizes its tables from an item estimate: 4 KiB blocks.
        let items = (capacity / 4096).max(64) as usize;
        BlockCache {
            cache: Cache::with_weighter(items, capacity.max(1), BlockWeighter),
        }
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
