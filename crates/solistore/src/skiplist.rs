//! The memtable's skiplist: nodes, keys and values all in one arena.
//!
//! ```text
//! node := next *Node × height (top level first) | key_len u32 | value_len u32 | key | value
//!                                                 ^ the node's address
//! ```
//!
//! A node is one arena allocation holding its links, its key and its value,
//! so an entry costs its bytes plus 8 bytes of header and 8 per level
//! (1.33 levels on average), rounded up to 8: ~22 bytes, counted exactly.
//! The links sit before the address, level 0 nearest, so the key is found
//! without knowing the node's height. Lengths fit in 32 bits because a WAL
//! record does. The crossbeam skiplist this replaces cost ~112 bytes per
//! entry for its node and two `Bytes` handles into a separate arena, and the
//! memtable counted 32 of them.
//!
//! Inserts run concurrently without a lock, as RocksDB's `InlineSkipList`:
//! a node is linked level by level from the bottom with a compare-and-swap,
//! and a failed swap searches again from the predecessor it had found.
//! Nothing is ever removed, so a node, once reachable, stays valid until the
//! list is dropped; readers only follow `Acquire` loads of the links.
//!
//! The arena hands each thread a block at a time, so concurrent writers do
//! not share a counter per entry.

use std::alloc::{self, Layout};
use std::cell::Cell;
use std::cmp::Ordering;
use std::mem::{align_of, size_of};
use std::ptr::{self, NonNull};
use std::sync::atomic::{AtomicPtr, AtomicU64, Ordering::*};

use parking_lot::Mutex;

use crate::key;

/// Enough for 4^12 = 16M entries at a branching factor of 4.
pub const MAX_HEIGHT: usize = 12;
/// Each level holds a quarter of the nodes of the one below.
const BRANCHING: u64 = 4;

/// What a thread takes from the arena at a time.
const BLOCK: usize = 32 << 10;
/// Allocations above this go to the arena directly rather than through a
/// thread's block, so a block's unused tail stays small.
const LARGE: usize = BLOCK / 4;
/// The arena's chunks double from this size up to `MAX_CHUNK`, so a small
/// memtable (tests use tiny ones) costs little.
const FIRST_CHUNK: usize = 64 << 10;
const MAX_CHUNK: usize = 4 << 20;
const ALIGN: usize = align_of::<AtomicPtr<Node>>();
const LINK: usize = size_of::<AtomicPtr<Node>>();

/// The fixed part of a node; its links precede it and its key and value
/// follow it.
#[repr(C)]
struct Node {
    key_len: u32,
    value_len: u32,
}

const HEADER: usize = size_of::<Node>();

/// Bytes a node with these sizes takes in the arena.
fn node_size(height: usize, key_len: usize, value_len: usize) -> usize {
    (height * LINK + HEADER + key_len + value_len).next_multiple_of(ALIGN)
}

// Every accessor below takes a raw pointer that came from the arena (or is
// the head), never a reference: a node's links and bytes lie outside
// `Node`, and only the arena allocation's own pointer may reach them.

/// SAFETY: `node` is a fully initialised node with more than `level` levels.
unsafe fn link<'a>(node: *const Node, level: usize) -> &'a AtomicPtr<Node> {
    &*(node as *const u8)
        .sub((level + 1) * LINK)
        .cast::<AtomicPtr<Node>>()
}

/// SAFETY: `node` is a fully initialised node of a live list.
unsafe fn key_of<'a>(node: *const Node) -> &'a [u8] {
    std::slice::from_raw_parts((node as *const u8).add(HEADER), (*node).key_len as usize)
}

/// SAFETY: as [`key_of`].
unsafe fn value_of<'a>(node: *const Node) -> &'a [u8] {
    let key = key_of(node);
    std::slice::from_raw_parts(key.as_ptr().add(key.len()), (*node).value_len as usize)
}

static NEXT_ARENA: AtomicU64 = AtomicU64::new(1);

thread_local! {
    /// The block this thread allocates from: the arena's id, the next free
    /// byte and the bytes left. Ids are never reused, so a block of a
    /// dropped arena is never used again.
    static LOCAL: Cell<(u64, *mut u8, usize)> = const { Cell::new((0, ptr::null_mut(), 0)) };
}

struct Arena {
    id: u64,
    inner: Mutex<Chunks>,
}

struct Chunks {
    owned: Vec<(NonNull<u8>, Layout)>,
    at: *mut u8,
    left: usize,
    next_size: usize,
}

impl Chunks {
    /// `size` bytes, aligned, from the current chunk or a new one.
    fn take(&mut self, size: usize) -> *mut u8 {
        debug_assert_eq!(size % ALIGN, 0);
        if self.left < size {
            let chunk = size.max(self.next_size);
            self.next_size = (self.next_size * 2).min(MAX_CHUNK);
            let layout = Layout::from_size_align(chunk, ALIGN).expect("chunk layout");
            // SAFETY: the layout has a non-zero size.
            let p = NonNull::new(unsafe { alloc::alloc(layout) })
                .unwrap_or_else(|| alloc::handle_alloc_error(layout));
            self.owned.push((p, layout));
            self.at = p.as_ptr();
            self.left = chunk;
        }
        let p = self.at;
        // SAFETY: `size <= left`, so this stays inside the chunk.
        self.at = unsafe { self.at.add(size) };
        self.left -= size;
        p
    }
}

impl Arena {
    fn new() -> Self {
        Arena {
            id: NEXT_ARENA.fetch_add(1, Relaxed),
            inner: Mutex::new(Chunks {
                owned: Vec::new(),
                at: ptr::null_mut(),
                left: 0,
                next_size: FIRST_CHUNK,
            }),
        }
    }

    /// `size` bytes (a multiple of [`ALIGN`]), valid until the arena drops.
    fn alloc(&self, size: usize) -> *mut u8 {
        if size > LARGE {
            return self.inner.lock().take(size);
        }
        LOCAL.with(|local| {
            let (id, at, left) = local.get();
            if id == self.id && left >= size {
                // SAFETY: the block belongs to this arena, which is alive
                // (`&self`), and has `left >= size` bytes past `at`.
                local.set((id, unsafe { at.add(size) }, left - size));
                return at;
            }
            let block = self.inner.lock().take(BLOCK);
            // SAFETY: a fresh block of BLOCK > LARGE >= size bytes.
            local.set((self.id, unsafe { block.add(size) }, BLOCK - size));
            block
        })
    }
}

impl Drop for Arena {
    fn drop(&mut self) {
        for (p, layout) in self.inner.get_mut().owned.drain(..) {
            // SAFETY: allocated in `take` with this layout, freed once.
            unsafe { alloc::dealloc(p.as_ptr(), layout) };
        }
    }
}

thread_local! {
    static RNG: Cell<u64> = Cell::new({
        // Any non-zero seed that differs between threads.
        let local = 0u8;
        (&local as *const u8 as u64) | 1
    });
}

fn random_height() -> usize {
    RNG.with(|rng| {
        // xorshift64
        let mut x = rng.get();
        let mut height = 1;
        loop {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            if height == MAX_HEIGHT || x % BRANCHING != 0 {
                break;
            }
            height += 1;
        }
        rng.set(x);
        height
    })
}

/// An entry of a [`SkipList`], valid as long as the list it came from.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Cursor(NonNull<Node>);

// SAFETY: a cursor is a pointer to an immutable node (its links are atomic);
// what may be read through it is guarded by the list it came from.
unsafe impl Send for Cursor {}
unsafe impl Sync for Cursor {}

/// A concurrent, insert-only skiplist of distinct internal keys, ordered by
/// [`key::compare`].
pub struct SkipList {
    arena: Arena,
    /// A node of full height with an empty key, before every entry.
    head: NonNull<Node>,
}

// SAFETY: nodes are written once before they are published with a
// `Release` swap and never changed afterwards except through their atomic
// links; the arena is guarded by its mutex and per-thread blocks.
unsafe impl Send for SkipList {}
unsafe impl Sync for SkipList {}

impl Default for SkipList {
    fn default() -> Self {
        Self::new()
    }
}

impl SkipList {
    pub fn new() -> Self {
        let arena = Arena::new();
        let head = new_node(&arena, MAX_HEIGHT, &[], &[]);
        SkipList { arena, head }
    }

    /// Insert the key made of `key_parts` (concatenated) with `value`.
    /// Returns the bytes the entry takes, or 0 if the key was already
    /// there (the list is left as it was, apart from the unused node).
    pub fn insert(&self, key_parts: &[&[u8]], value: &[u8]) -> usize {
        let height = random_height();
        let key_len: usize = key_parts.iter().map(|p| p.len()).sum();
        let size = node_size(height, key_len, value.len());
        let node = new_node(&self.arena, height, key_parts, value).as_ptr();
        // SAFETY: `node` was just initialised and is not yet shared.
        let key = unsafe { key_of(node) };

        let mut prev = [self.head.as_ptr(); MAX_HEIGHT];
        let mut next = [ptr::null_mut(); MAX_HEIGHT];
        let mut x = self.head.as_ptr();
        for level in (0..MAX_HEIGHT).rev() {
            let (p, n) = self.find_splice(key, x, level);
            prev[level] = p;
            next[level] = n;
            x = p;
        }
        for level in 0..height {
            loop {
                if level == 0 && self.equals(next[0], key) {
                    return 0;
                }
                // SAFETY: `node` has `height > level` links; `prev[level]`
                // is the head or a published node, whose height is above
                // `level` because it was reached at that level.
                unsafe {
                    link(node, level).store(next[level], Relaxed);
                    match link(prev[level], level).compare_exchange(
                        next[level],
                        node,
                        Release,
                        Acquire,
                    ) {
                        Ok(_) => break,
                        Err(_) => {
                            // Another insert got in between: search this
                            // level again from the predecessor, which is
                            // still before `key` since nothing is removed.
                            let (p, n) = self.find_splice(key, prev[level], level);
                            prev[level] = p;
                            next[level] = n;
                        }
                    }
                }
            }
        }
        size
    }

    fn equals(&self, node: *mut Node, key: &[u8]) -> bool {
        // SAFETY: a link read from the list is null or a published node.
        !node.is_null() && key::compare(unsafe { key_of(node) }, key) == Ordering::Equal
    }

    /// From `start` (before `key`) along `level`: the last node before
    /// `key` and the one after it (null at the end).
    fn find_splice(&self, key: &[u8], start: *mut Node, level: usize) -> (*mut Node, *mut Node) {
        let mut x = start;
        loop {
            // SAFETY: `x` is the head or a published node reached at
            // `level`, so it has a link there; what it points to is null or
            // a published node.
            let n = unsafe { link(x, level).load(Acquire) };
            if !n.is_null() && key::compare(unsafe { key_of(n) }, key) == Ordering::Less {
                x = n;
            } else {
                return (x, n);
            }
        }
    }

    /// The last node for which `before(node key)` holds, or the head, and
    /// the node after it at the bottom level (null at the end): both as read
    /// in one pass.
    ///
    /// The second must come from the same load that stopped the search. A
    /// fresh load of the first node's link could return a node inserted
    /// meanwhile, for which `before` holds too: a seek would then answer a
    /// key before its target, and a point read would miss the key after it.
    fn search(&self, before: impl Fn(&[u8]) -> bool) -> (*mut Node, *mut Node) {
        let mut x = self.head.as_ptr();
        let mut level = MAX_HEIGHT - 1;
        loop {
            // SAFETY: as in `find_splice`.
            let n = unsafe { link(x, level).load(Acquire) };
            if !n.is_null() && before(unsafe { key_of(n) }) {
                x = n;
            } else if level == 0 {
                return (x, n);
            } else {
                level -= 1;
            }
        }
    }

    fn cursor(&self, node: *mut Node) -> Option<Cursor> {
        if node == self.head.as_ptr() {
            return None;
        }
        NonNull::new(node).map(Cursor)
    }

    pub fn is_empty(&self) -> bool {
        self.first().is_none()
    }

    pub fn first(&self) -> Option<Cursor> {
        // SAFETY: the head has every level.
        self.cursor(unsafe { link(self.head.as_ptr(), 0).load(Acquire) })
    }

    pub fn last(&self) -> Option<Cursor> {
        self.cursor(self.search(|_| true).0)
    }

    /// The first entry at or after `target`.
    pub fn seek_ge(&self, target: &[u8]) -> Option<Cursor> {
        self.cursor(self.search(|k| key::compare(k, target) == Ordering::Less).1)
    }

    /// The last entry at or before `target`.
    pub fn seek_le(&self, target: &[u8]) -> Option<Cursor> {
        self.cursor(
            self.search(|k| key::compare(k, target) != Ordering::Greater)
                .0,
        )
    }

    /// The last entry before `target`.
    pub fn seek_lt(&self, target: &[u8]) -> Option<Cursor> {
        self.cursor(self.search(|k| key::compare(k, target) == Ordering::Less).0)
    }

    /// The entry after `at`. `at` must come from this list.
    pub fn next(&self, at: Cursor) -> Option<Cursor> {
        // SAFETY: `at` is a published node of this list, alive with `self`.
        self.cursor(unsafe { link(at.0.as_ptr(), 0).load(Acquire) })
    }

    /// The key at `at`, which must come from this list.
    pub fn key(&self, at: Cursor) -> &[u8] {
        // SAFETY: as in `next`; the bytes live as long as `self`.
        unsafe { key_of(at.0.as_ptr()) }
    }

    /// The value at `at`, which must come from this list.
    pub fn value(&self, at: Cursor) -> &[u8] {
        // SAFETY: as in `key`.
        unsafe { value_of(at.0.as_ptr()) }
    }

    /// Every entry in order.
    pub fn iter(&self) -> impl Iterator<Item = (&[u8], &[u8])> + '_ {
        std::iter::successors(self.first(), |&c| self.next(c)).map(|c| (self.key(c), self.value(c)))
    }
}

/// Allocate and fill a node; its links start null.
fn new_node(arena: &Arena, height: usize, key_parts: &[&[u8]], value: &[u8]) -> NonNull<Node> {
    let key_len: usize = key_parts.iter().map(|p| p.len()).sum();
    // A WAL record's length is a u32, so no key or value it carried is longer.
    let key_len32 = u32::try_from(key_len).expect("a key under 4 GiB");
    let value_len32 = u32::try_from(value.len()).expect("a value under 4 GiB");
    let p = arena.alloc(node_size(height, key_len, value.len()));
    // SAFETY: `p` is aligned and has room for `height` links, the header,
    // the key and the value; each is written once, in place, before the
    // node is shared.
    unsafe {
        let links = p.cast::<AtomicPtr<Node>>();
        for level in 0..height {
            links.add(level).write(AtomicPtr::new(ptr::null_mut()));
        }
        let node = p.add(height * LINK).cast::<Node>();
        ptr::addr_of_mut!((*node).key_len).write(key_len32);
        ptr::addr_of_mut!((*node).value_len).write(value_len32);
        let mut at = node.cast::<u8>().add(HEADER);
        for part in key_parts {
            ptr::copy_nonoverlapping(part.as_ptr(), at, part.len());
            at = at.add(part.len());
        }
        ptr::copy_nonoverlapping(value.as_ptr(), at, value.len());
        NonNull::new_unchecked(node)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::key::{encode, Kind};
    use std::collections::BTreeMap;
    use std::sync::Arc;

    fn ik(i: u32) -> Vec<u8> {
        encode(
            0,
            format!("k{i:06}").as_bytes(),
            u64::from(i) + 1,
            Kind::Put,
        )
    }

    #[test]
    fn entries_come_back_in_key_order() {
        let list = SkipList::new();
        assert!(list.is_empty());
        let mut keys: Vec<u32> = (0..2000).map(|i| (i * 7919) % 2000).collect();
        for &i in &keys {
            assert!(list.insert(&[&ik(i)], format!("v{i}").as_bytes()) > 0);
        }
        keys.sort();
        let got: Vec<_> = list.iter().map(|(k, v)| (k.to_vec(), v.to_vec())).collect();
        let want: Vec<_> = keys
            .iter()
            .map(|&i| (ik(i), format!("v{i}").into_bytes()))
            .collect();
        assert_eq!(got, want);
        assert_eq!(list.key(list.last().unwrap()), &ik(1999)[..]);
    }

    #[test]
    fn a_key_given_in_parts_is_one_key_and_a_duplicate_is_refused() {
        let list = SkipList::new();
        let whole = encode(3, b"user", 9, Kind::Delete);
        let (a, b) = whole.split_at(5);
        assert!(list.insert(&[a, b], b"") > 0);
        assert_eq!(list.insert(&[&whole], b"other"), 0);
        let c = list.first().unwrap();
        assert_eq!(list.key(c), &whole[..]);
        assert_eq!(list.value(c), b"");
        assert_eq!(list.next(c), None);
    }

    #[test]
    fn seeks_agree_with_a_sorted_map() {
        let list = SkipList::new();
        let mut model = BTreeMap::new();
        for i in (0..3000u32).step_by(3) {
            list.insert(&[&ik(i)], &[]);
            model.insert(i, ());
        }
        let key_at = |c: Option<Cursor>| c.map(|c| list.key(c).to_vec());
        for probe in 0..3005u32 {
            let t = ik(probe);
            let ge = model.range(probe..).next().map(|(&i, _)| ik(i));
            let le = model.range(..=probe).next_back().map(|(&i, _)| ik(i));
            let lt = model.range(..probe).next_back().map(|(&i, _)| ik(i));
            assert_eq!(key_at(list.seek_ge(&t)), ge, "ge {probe}");
            assert_eq!(key_at(list.seek_le(&t)), le, "le {probe}");
            assert_eq!(key_at(list.seek_lt(&t)), lt, "lt {probe}");
        }
    }

    #[test]
    fn values_of_every_size_survive_including_large_ones() {
        let list = SkipList::new();
        for (i, len) in [
            0usize,
            1,
            7,
            8,
            9,
            LARGE - 1,
            LARGE + 1,
            BLOCK,
            3 * MAX_CHUNK,
        ]
        .into_iter()
        .enumerate()
        {
            let value: Vec<u8> = (0..len).map(|b| (b * 31 + i) as u8).collect();
            list.insert(&[&ik(i as u32)], &value);
            let c = list.seek_ge(&ik(i as u32)).unwrap();
            assert_eq!(list.value(c), &value[..], "len {len}");
        }
    }

    #[test]
    fn the_size_is_what_the_node_takes() {
        let list = SkipList::new();
        let size = list.insert(&[&ik(1)], b"12345");
        assert!(size >= HEADER + 8 + ik(1).len() + 5, "{size}");
        assert!(
            size <= HEADER + 8 * MAX_HEIGHT + ik(1).len() + 5 + ALIGN,
            "{size}"
        );
        assert_eq!(size % ALIGN, 0);
    }

    #[test]
    fn heights_follow_a_branching_of_four() {
        let n = 100_000;
        let total: usize = (0..n).map(|_| random_height()).sum();
        let mean = total as f64 / n as f64;
        assert!((1.28..1.39).contains(&mean), "mean height {mean}");
    }

    /// Writers insert concurrently while readers walk the list: every key
    /// lands, and a reader always sees a sorted list.
    #[test]
    fn concurrent_inserts_and_readers() {
        let list = Arc::new(SkipList::new());
        let threads = 8u32;
        let per = 5000u32;
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let readers: Vec<_> = (0..2)
            .map(|_| {
                let (list, stop) = (list.clone(), stop.clone());
                std::thread::spawn(move || {
                    while !stop.load(Relaxed) {
                        let mut last: Option<Vec<u8>> = None;
                        for (k, _) in list.iter() {
                            if let Some(l) = &last {
                                assert_eq!(key::compare(l, k), Ordering::Less);
                            }
                            last = Some(k.to_vec());
                        }
                    }
                })
            })
            .collect();
        let writers: Vec<_> = (0..threads)
            .map(|t| {
                let list = list.clone();
                std::thread::spawn(move || {
                    for i in 0..per {
                        // Interleaved key ranges, so writers contend.
                        let k = i * threads + t;
                        assert!(list.insert(&[&ik(k)], &k.to_le_bytes()) > 0);
                    }
                })
            })
            .collect();
        for w in writers {
            w.join().unwrap();
        }
        stop.store(true, Relaxed);
        for r in readers {
            r.join().unwrap();
        }
        let got: Vec<u32> = list
            .iter()
            .map(|(_, v)| u32::from_le_bytes(v.try_into().unwrap()))
            .collect();
        assert_eq!(got, (0..threads * per).collect::<Vec<_>>());
    }

    /// A seek to a key that is in the list finds exactly it while writers
    /// insert keys right before it. A seek that took the successor from a
    /// second load answered the key inserted meanwhile instead. Readers aim
    /// at the key just after where each writer is inserting.
    #[test]
    fn a_seek_is_not_fooled_by_a_neighbour_inserted_meanwhile() {
        use std::sync::atomic::AtomicU32;
        let list = Arc::new(SkipList::new());
        let n = 200_000u32;
        for i in 0..=n {
            list.insert(&[&ik(i * 4)], b"");
        }
        let progress: Arc<Vec<AtomicU32>> = Arc::new((0..3).map(|_| AtomicU32::new(0)).collect());
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let readers: Vec<_> = (0..3usize)
            .map(|r| {
                let (list, stop, progress) = (list.clone(), stop.clone(), progress.clone());
                std::thread::spawn(move || {
                    let mut seeks = 0u64;
                    while !stop.load(Relaxed) {
                        let p = progress[(seeks as usize + r) % 3].load(Relaxed);
                        let target = ik((p + 1).min(n) * 4);
                        let found = list.seek_ge(&target).map(|c| list.key(c).to_vec());
                        assert_eq!(found.as_deref(), Some(&target[..]), "seek_ge");
                        let found = list.seek_le(&target).map(|c| list.key(c).to_vec());
                        assert_eq!(found.as_deref(), Some(&target[..]), "seek_le");
                        seeks += 1;
                    }
                    seeks
                })
            })
            .collect();
        let writers: Vec<_> = (0..3u32)
            .map(|w| {
                let (list, progress) = (list.clone(), progress.clone());
                std::thread::spawn(move || {
                    for i in 0..n {
                        progress[w as usize].store(i, Relaxed);
                        list.insert(&[&ik(i * 4 + 1 + w)], b"");
                    }
                })
            })
            .collect();
        for w in writers {
            w.join().unwrap();
        }
        stop.store(true, Relaxed);
        let seeks: u64 = readers.into_iter().map(|r| r.join().unwrap()).sum();
        assert!(seeks > 1000, "{seeks} seeks");
        assert_eq!(list.iter().count(), 4 * n as usize + 1);
    }

    /// Two lists on one thread alternate: each allocates from its own arena.
    #[test]
    fn interleaved_lists_keep_their_own_blocks() {
        let (a, b) = (SkipList::new(), SkipList::new());
        for i in 0..5000u32 {
            a.insert(&[&ik(i)], b"a");
            b.insert(&[&ik(i)], b"b");
        }
        drop(b);
        let c = SkipList::new();
        for i in 0..5000u32 {
            c.insert(&[&ik(i)], b"c");
        }
        assert!(a.iter().all(|(_, v)| v == b"a"));
        assert!(c.iter().all(|(_, v)| v == b"c"));
        assert_eq!(a.iter().count(), 5000);
    }
}
