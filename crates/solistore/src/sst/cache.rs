//! The block cache: decoded data blocks, shared by every table of a
//! database, bounded in bytes.
//!
//! The bound is a budget for table memory as a whole: the index and filter
//! every open table keeps in memory are charged against it, and the cache
//! holds what is left. RocksDB with SoliDB's profile keeps those outside its
//! cache and unbounded; here they cannot grow the process past the budget.

use std::hash::{BuildHasher, Hasher};
use std::ptr::NonNull;
use std::sync::atomic::{AtomicBool, AtomicPtr, Ordering};
use std::sync::Arc;

use crossbeam_epoch::Guard;
use quick_cache::sync::Cache;
use quick_cache::{Lifecycle, Weighter};

use super::block::Block;

/// A block is identified by its table's number and its offset in the file.
type Key = (u64, u64);

#[derive(Clone)]
struct BlockWeighter;

impl Weighter<Key, Cached> for BlockWeighter {
    fn weight(&self, _key: &Key, block: &Cached) -> u64 {
        // The bytes plus the entry's own bookkeeping.
        block.0.lent().block.size() as u64 + 96
    }
}

/// A cached block as a lock-free hit reads it: the block, and whether it
/// was read since the cache's eviction hand last passed (see
/// [`BlockLifecycle`]).
pub(crate) struct Lent {
    block: Block,
    referenced: AtomicBool,
}

impl Lent {
    /// The block, noted as read. A hit writes nothing shared unless the
    /// flag was clear: one store per block per pass of the eviction hand,
    /// where the cache's own bookkeeping was a locked write on every hit.
    #[inline]
    fn touch(&self) -> &Block {
        if !self.referenced.load(Ordering::Relaxed) {
            self.referenced.store(true, Ordering::Relaxed);
        }
        self.block.prefetch_entries();
        &self.block
    }
}

/// One slot per data block of a table, holding the block while the cache
/// does: what makes a hit one load.
///
/// A hit through the cache proper takes four locked read-modify-writes: the
/// shard's read lock, taken and released, and the block's reference count,
/// raised and dropped, the count being a likely miss in an allocation of
/// its own. Each is a full barrier on x86 and holds back the misses a
/// batched read has started to overlap; together they were about a third
/// of a warm point read.
///
/// The rules that keep a slot's pointer good:
/// - A slot is read only under an epoch guard ([`Slots::lent`]), and what
///   it lends lives as long as that guard.
/// - A slot only ever holds the block of a live [`Entry`] that names this
///   very slot as its home: it is set by someone holding that entry
///   ([`Entry::install`]), and cleared by the entry's own drop.
/// - The drop clears the slot first, then frees the block through the
///   epoch: not before every reader that could have loaded the pointer has
///   left its guard.
pub(crate) struct Slots(Box<[AtomicPtr<Lent>]>);

impl Slots {
    pub(crate) fn new(blocks: usize) -> Arc<Slots> {
        Arc::new(Slots(
            (0..blocks)
                .map(|_| AtomicPtr::new(std::ptr::null_mut()))
                .collect(),
        ))
    }

    /// Bytes the slots take, charged to the cache like a table's index.
    pub(crate) fn size(&self) -> usize {
        self.0.len() * std::mem::size_of::<AtomicPtr<Lent>>()
    }

    /// Block `number` if the cache holds it, lent for as long as `_guard`.
    #[inline]
    pub(crate) fn lent<'g>(&self, number: usize, _guard: &'g Guard) -> Option<&'g Block> {
        let lent = self.0.get(number)?.load(Ordering::Acquire);
        // SAFETY: a non-null slot points at the `Lent` of an entry that was
        // alive when the pointer was loaded, under `_guard`. The entry's
        // drop clears the slot before deferring the free to the epoch, so
        // the free waits for this guard (see `Slots`).
        unsafe { lent.as_ref() }.map(Lent::touch)
    }
}

/// The owner of a cached block: the cache holds one, and so does whoever
/// took the block out the slow way. Its drop is the one place a cached
/// block is freed, whatever dropped it (an eviction, a replacement under
/// the same key, the cache going away).
struct Entry {
    lent: NonNull<Lent>,
    /// The slot that may point at `lent`: a table's slots and the block's
    /// number in it.
    home: Option<(Arc<Slots>, usize)>,
}

// SAFETY: `lent` is a `Box<Lent>` this entry owns, freed only by its drop;
// `Lent` holds a `Block` (`Send + Sync`) and an atomic.
unsafe impl Send for Entry {}
unsafe impl Sync for Entry {}

/// A `Lent` on its way to being freed, for the epoch's closure.
struct Retired(NonNull<Lent>);
// SAFETY: as `Entry`; the closure holding it is the only owner left.
unsafe impl Send for Retired {}

impl Retired {
    /// # Safety
    /// Nothing may still read the `Lent`: called once, by the epoch.
    unsafe fn free(self) {
        drop(unsafe { Box::from_raw(self.0.as_ptr()) });
    }
}

impl Entry {
    fn new(block: Block, home: Option<(Arc<Slots>, usize)>) -> Arc<Entry> {
        let lent = Box::new(Lent {
            block,
            referenced: AtomicBool::new(false),
        });
        Arc::new(Entry {
            lent: NonNull::from(Box::leak(lent)),
            home,
        })
    }

    #[inline]
    fn lent(&self) -> &Lent {
        // SAFETY: the box lives until this entry's drop.
        unsafe { self.lent.as_ref() }
    }

    /// Point this entry's slot at its block, if the slot is empty. The
    /// caller holds the entry (`&self`), so its drop, which clears the
    /// slot, is still to come.
    fn install(&self) {
        if let Some((slots, number)) = &self.home {
            if let Some(slot) = slots.0.get(*number) {
                let _ = slot.compare_exchange(
                    std::ptr::null_mut(),
                    self.lent.as_ptr(),
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                );
            }
        }
    }

    /// Whether this entry's home is slot `number` of `slots`.
    fn lives_at(&self, slots: &Arc<Slots>, number: usize) -> bool {
        matches!(&self.home, Some((s, n)) if Arc::ptr_eq(s, slots) && *n == number)
    }
}

impl Drop for Entry {
    fn drop(&mut self) {
        // Out of the slot first, if it is this block the slot holds...
        if let Some((slots, number)) = &self.home {
            if let Some(slot) = slots.0.get(*number) {
                let _ = slot.compare_exchange(
                    self.lent.as_ptr(),
                    std::ptr::null_mut(),
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                );
            }
        }
        // ...then freed once no reader that could have loaded it is left
        // in its guard.
        let retired = Retired(self.lent);
        crossbeam_epoch::pin().defer(move || {
            // SAFETY: the slot no longer leads here, and the epoch runs
            // this after every guard that was held when it was cleared.
            unsafe { retired.free() }
        });
    }
}

/// A block as the cache proper holds it and hands it out (a clone of the
/// entry's handle).
#[derive(Clone)]
struct Cached(Arc<Entry>);

/// The cache's eviction hand asks whether an entry is pinned before
/// evicting it. A block read through its slot since the hand last asked
/// says yes, once: the second chance of a CLOCK, for the hits the cache's
/// own counters never see.
#[derive(Clone, Copy, Default)]
struct BlockLifecycle;

impl Lifecycle<Key, Cached> for BlockLifecycle {
    type RequestState = ();

    #[inline]
    fn is_pinned(&self, _key: &Key, block: &Cached) -> bool {
        let lent = block.0.lent();
        lent.referenced.load(Ordering::Relaxed) && lent.referenced.swap(false, Ordering::Relaxed)
    }

    #[inline]
    fn begin_request(&self) {}
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
    cache: Cache<Key, Cached, BlockWeighter, BlockKeyHash, BlockLifecycle>,
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
                BlockLifecycle,
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

    /// The block, by the cache proper: a handle of its own, and its entry
    /// headers already on their way to the CPU. `home` is the slot the
    /// caller would read it from next time: an entry that lives there is
    /// put back in its slot, and one that lives elsewhere (cached by
    /// another reader of the same table) is replaced by one that does.
    pub fn get(
        &self,
        table: u64,
        offset: u64,
        home: Option<(&Arc<Slots>, usize)>,
    ) -> Option<Block> {
        let entry = self.cache.get(&(table, offset))?.0;
        let block = entry.lent().touch().clone();
        if let Some((slots, number)) = home {
            if entry.lives_at(slots, number) {
                entry.install();
            } else {
                self.insert(table, offset, block.clone(), home);
            }
        }
        Some(block)
    }

    /// Keep `block`; `home` is the slot its table reads it from.
    pub fn insert(
        &self,
        table: u64,
        offset: u64,
        block: Block,
        home: Option<(&Arc<Slots>, usize)>,
    ) {
        let entry = Entry::new(
            block,
            home.map(|(slots, number)| (Arc::clone(slots), number)),
        );
        self.cache
            .insert((table, offset), Cached(Arc::clone(&entry)));
        // After the insert, and holding the entry: if the cache has already
        // let go of it, its drop (ours, below) clears the slot again.
        entry.install();
    }

    /// Bytes held (RocksDB's `block-cache-usage`).
    pub fn usage(&self) -> u64 {
        self.cache.weight()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::key::{encode, Kind};
    use crate::sst::block::BlockBuilder;
    use bytes::Bytes;

    /// A block whose one value says which block it is.
    fn block(number: usize) -> Block {
        let mut b = BlockBuilder::new(16);
        b.add(
            &encode(0, format!("key{number:05}").as_bytes(), 1, Kind::Put),
            &[number as u8; 200],
        );
        Block::new(Bytes::from(b.finish())).unwrap()
    }

    /// Whether `block` is block `number`, whole.
    fn is_block(block: &Block, number: usize) -> bool {
        let mut it = block.iter();
        it.seek_to_first();
        it.valid() && it.value()[..] == [number as u8; 200]
    }

    #[test]
    fn a_cached_block_is_lent_from_its_slot_until_it_is_evicted() {
        // Room for about four blocks of ~250 bytes and their bookkeeping.
        let cache = BlockCache::new(1500);
        let slots = Slots::new(64);
        let guard = crossbeam_epoch::pin();
        assert!(slots.lent(3, &guard).is_none());

        cache.insert(7, 3, block(3), Some((&slots, 3)));
        assert!(is_block(slots.lent(3, &guard).unwrap(), 3));
        // Not a slot of this table: nothing, not a panic.
        assert!(slots.lent(64, &guard).is_none());

        // Enough other blocks to push it out, with the guard still held.
        let lent = slots.lent(3, &guard).unwrap();
        for number in 4..64 {
            cache.insert(7, number as u64, block(number), Some((&slots, number)));
        }
        let held = (0..64)
            .filter(|&n| slots.lent(n, &crossbeam_epoch::pin()).is_some())
            .count();
        assert!(held < 16, "{held} blocks still in their slots");
        assert!(cache.usage() <= 1500, "{} bytes held", cache.usage());
        // What was lent before stays whole for as long as its guard.
        assert!(is_block(lent, 3));
        // A slot and the cache agree: a block lent is one the cache holds.
        for number in 0..64 {
            if slots.lent(number, &guard).is_some() {
                assert!(
                    cache.get(7, number as u64, None).is_some(),
                    "block {number}"
                );
            }
        }
    }

    #[test]
    fn a_hit_through_the_cache_puts_the_block_back_in_its_slot() {
        let cache = BlockCache::new(1 << 20);
        let slots = Slots::new(4);
        // Cached by a reader that does not know its number (an iterator).
        cache.insert(7, 2, block(2), None);
        let guard = crossbeam_epoch::pin();
        assert!(slots.lent(2, &guard).is_none());
        assert!(is_block(&cache.get(7, 2, Some((&slots, 2))).unwrap(), 2));
        assert!(is_block(slots.lent(2, &guard).unwrap(), 2));
    }

    /// Two readers of one table (the same number, two sets of slots): each
    /// ends up with the block in its own slot, and dropping one reader's
    /// entry never leaves the other's slot pointing at freed memory.
    #[test]
    fn a_block_cached_by_another_reader_of_the_table_moves_to_this_ones_slot() {
        let cache = BlockCache::new(1 << 20);
        let (first, second) = (Slots::new(4), Slots::new(4));
        cache.insert(7, 1, block(1), Some((&first, 1)));
        {
            let guard = crossbeam_epoch::pin();
            assert!(slots_hold(&first, 1, &guard));
            assert!(!slots_hold(&second, 1, &guard));
        }
        // The second reader asks the cache: the entry is replaced by one
        // that lives in its slot, and the first one's slot is cleared.
        assert!(is_block(&cache.get(7, 1, Some((&second, 1))).unwrap(), 1));
        let guard = crossbeam_epoch::pin();
        assert!(!slots_hold(&first, 1, &guard));
        assert!(is_block(second.lent(1, &guard).unwrap(), 1));
        drop(guard);
        // And once the cache is gone, so is every slot's block.
        drop(cache);
        let guard = crossbeam_epoch::pin();
        assert!(!slots_hold(&second, 1, &guard));
    }

    fn slots_hold(slots: &Slots, number: usize, guard: &Guard) -> bool {
        slots.lent(number, guard).is_some()
    }

    /// A block read through its slot survives one pass of the eviction
    /// hand, and no more unless it is read again.
    #[test]
    fn a_block_read_through_its_slot_is_spared_once() {
        let slots = Slots::new(1);
        let entry = Cached(Entry::new(block(0), Some((Arc::clone(&slots), 0))));
        entry.0.install();
        assert!(!BlockLifecycle.is_pinned(&(7, 0), &entry));
        assert!(slots.lent(0, &crossbeam_epoch::pin()).is_some());
        assert!(BlockLifecycle.is_pinned(&(7, 0), &entry));
        assert!(!BlockLifecycle.is_pinned(&(7, 0), &entry));
    }

    /// Readers lending blocks from their slots while the cache, far too
    /// small for the table, keeps evicting and refilling them: every block
    /// lent is the block asked for, whole, for as long as its guard.
    #[test]
    fn readers_and_evictions_race() {
        const BLOCKS: usize = 48;
        let (threads, rounds) = if cfg!(miri) { (3, 120) } else { (8, 200_000) };
        let cache = Arc::new(BlockCache::new(2500));
        let slots = Slots::new(BLOCKS);
        let lent = std::sync::atomic::AtomicUsize::new(0);
        std::thread::scope(|scope| {
            for thread in 0..threads {
                let (cache, slots, lent) = (&cache, &slots, &lent);
                scope.spawn(move || {
                    let mut seed = 0x9e37_79b9_7f4a_7c15u64.wrapping_mul(thread as u64 + 1);
                    let mut hits = 0;
                    for _ in 0..rounds {
                        // A batch: several blocks under one guard, each
                        // checked again after the others were taken.
                        let guard = crossbeam_epoch::pin();
                        let mut batch: Vec<(usize, &Block)> = Vec::new();
                        for _ in 0..4 {
                            seed ^= seed << 13;
                            seed ^= seed >> 7;
                            seed ^= seed << 17;
                            let number = (seed % BLOCKS as u64) as usize;
                            match slots.lent(number, &guard) {
                                Some(block) => {
                                    hits += 1;
                                    batch.push((number, block));
                                }
                                None => {
                                    let home = Some((slots, number));
                                    let found = match cache.get(7, number as u64, home) {
                                        Some(block) => block,
                                        None => {
                                            let fresh = block(number);
                                            cache.insert(7, number as u64, fresh.clone(), home);
                                            fresh
                                        }
                                    };
                                    assert!(is_block(&found, number), "block {number}");
                                }
                            }
                        }
                        for (number, block) in batch {
                            assert!(is_block(block, number), "block {number}");
                        }
                    }
                    lent.fetch_add(hits, Ordering::Relaxed);
                });
            }
        });
        assert!(
            lent.load(Ordering::Relaxed) > 0,
            "no read was ever lent a block"
        );
        assert!(cache.usage() <= 2500, "{} bytes held", cache.usage());
    }
}
