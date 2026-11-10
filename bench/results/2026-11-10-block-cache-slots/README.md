# Block-cache hits from a slot per block, 2026-11-10

The point-read notes of 2026-11-08 left one item: a block-cache hit takes
four locked read-modify-writes (the shard's read lock, taken and released;
the block's reference count, raised and dropped), 32–37% of a warm point
read. This is that item, in a smaller form than planned there. Branch
`perf/block-cache-hits`. No API change; one new dependency,
`crossbeam-epoch` (pure Rust).

## What changed

- **A slot per data block** (`sst/cache.rs`, `Slots`). Each table reader
  keeps one `AtomicPtr` per data block, indexed by the block number the
  fences give. A point read whose block is cached loads its slot under an
  epoch guard and writes nothing another thread reads.
- **quick_cache stays the owner.** It keeps the byte budget and decides
  evictions. Its value is now a handle to an `Entry`, which owns the block.
  The entry's drop is the one place a cached block is freed, whatever
  dropped it (an eviction, a replacement under the same key, the cache
  going away): it clears its slot, then frees the block through the epoch,
  once no reader that could have loaded the pointer is left in its guard.
- **Second chance.** A block read through its slot sets a flag, with a
  store only when the flag was clear. quick_cache's eviction hand asks
  whether an entry is pinned before evicting it (`Lifecycle::is_pinned`);
  a flagged block says yes once, and the flag is cleared. Hits through
  slots are otherwise invisible to quick_cache's own counters.
- **Who reads through slots.** `get` (one guard per table probed) and
  `multi_get` / `multi_get_with` (one guard for the batch, held until the
  values have been handed out). Scan iterators keep the cache's own path.
- **A block the index located gets its number too.** In tables of many
  collections the fences tie on a collection's word and the index decides
  (the notes of 2026-11-08, "Fences in tables of many collections"). Such
  a block had no number, so it never used its slot, and the path through
  the cache had become a little heavier: SoliDB built against the first
  version ran 1–2% slower on its three read queries. `Fences::number_of`
  finds the number from the block's offset (a binary search of the
  offsets). The point bench, with one collection, never saw this.
- Memory: 8 bytes per data block per open table, charged to the cache's
  budget like the index, and about 32 bytes more per cached block.

## The rules that keep a slot's pointer good

- A slot is read only under an epoch guard, and what it lends lives as
  long as that guard.
- A slot only ever holds the block of a live entry that names this very
  slot as its home: it is set by someone holding that entry, and cleared
  by that entry's own drop (a compare-and-swap either way, so a slot never
  loses a newer block to an older entry).
- An entry cached by another reader of the same table (the same table
  number, another set of slots) is not installed here: it is replaced by
  one whose home is this slot.

## Point bench (ns per document, lower is better)

`bench/point.sh`, rocky-build2 (Ryzen 9 9950X), cores 0–3 and their
siblings, 1M documents, 4 GiB cache, main (8699249) and the branch
alternated three times; medians. `point/`. This run is the first version
of the branch; the point bench's one collection does not tie, so
`number_of` is not on its path.

| | main | branch | branch / main |
|---|---:|---:|---:|
| `get`, 1 thread | 628 | 584 | 0.93 |
| `get`, 8 threads | 771 | 730 | 0.95 |
| `multi_get` of 10, 1 thread | 523 | 472 | 0.90 |
| `multi_get` of 20, 1 thread | 518 | 469 | 0.91 |
| `multi_get_with` of 10, 1 thread | 436 | 384 | 0.88 |
| `multi_get_with` of 20, 1 thread | 437 | 389 | 0.89 |
| `multi_get` of 10, 8 threads | 677 | 612 | 0.90 |
| `multi_get` of 20, 8 threads | 669 | 607 | 0.91 |
| 20 consecutive keys, 1 thread | 199 | 189 | 0.95 |
| memtable `get`, 1 thread | 310 | 305 | 0.98 |
| memtable `multi_get` of 20, 1 thread | 278 | 280 | 1.01 |

Per-call p50 of a `get`: 591 -> 521 ns on one thread, 772 -> 661 on eight.
Instructions per `get`: 2,944 -> 2,811. The three runs of each binary agree
within 1%.

The notes of 2026-11-08 estimated a get "near 580 instead of 685" on
rocky-build: about the same share.

## Quick bench (operations a second)

`bench/quick.sh`, same host and cores, main and the branch alternated
twice, the final branch. `quick/`. Means of the two runs:

| phase | main, eventual | branch, eventual | main, full | branch, full |
|---|---:|---:|---:|---:|
| load | 808,847 | 810,676 | 207,035 | 212,725 |
| fsynced commits, 1 thread | 35,089 | 37,869 | 53,687 | 54,403 |
| fsynced commits, 8 threads | 239,312 | 225,623 | 249,125 | 258,858 |
| get uniform | 5,052,745 | 5,252,491 | 5,038,850 | 5,228,827 |
| get hot | 9,365,461 | 9,614,798 | 9,418,879 | 9,569,669 |
| get missing | 23,932,455 | 20,741,919 | 23,971,838 | 23,609,503 |
| scan, 100 keys, ascending | 96,364 | 100,811 | 97,938 | 101,579 |
| scan, 100 keys, descending | 86,103 | 90,290 | 87,299 | 90,795 |
| update | 774,190 | 768,099 | 188,242 | 187,551 |
| mixed | 414,680 | 420,763 | 279,705 | 281,732 |

- Gets +3–4%, mixed +1%. Scans +4–5%, though their iterators still go
  through the cache proper: not explained here (the cache's entries are
  smaller now, a handle where the block was).
- **Get missing** reads 1.5% lower in three of the four pairs, and 25%
  lower in one (18.0M against 24.0M, the first eventual pair). A missing
  key is turned away by the filter before any block or guard is touched,
  so the branch adds nothing on that path; the fsynced commits of that
  same pass spread as widely (181k and 270k on the branch, 236k and 242k
  on main), on a host whose load average was about 2. Not explained
  further.
- Peak RSS 926–943 MB against 906–926 (eventual), 898–901 against 893–911
  (full).

## SoliDB built against the branch

SoliDB f4e2f35e, one client, its `benchmarks/compare` read phases, built
against solistore 0.3.2 and against the branch, alternated twice, three
runs of 8 s each. `solidb/`.

| query | 0.3.2 | branch |
|---|---:|---:|
| `by_customer` (about 10 documents by an index) | 25,215/s | 25,305/s |
| `recent` (20 documents in index order) | 14,718/s | 14,890/s |
| `join` (those 20, each with a `DOCUMENT()`) | 13,160/s | 13,470/s |

+0.4%, +1.2% and +2.4%. Its bank benchmark (transactions over the driver,
every commit fsynced) does not move.

## Two things tried on the way, and not kept

SoliDB decodes each document a batched read returns. `multi_get` hands
each out as a copy; `multi_get_with` lends it.

- **Lending the values to SoliDB's decoder** made its three queries 6%
  slower. Copying the 20 values first lets their cache misses overlap;
  decoding them where they lie waits for one after another.
- **Prefetching every value before the first is lent** (in `multi_get`)
  took that loss back and no more: the join then ran as with copies. The
  copy costs what the overlap saves.

## Tests and gates

- `cargo test --profile ci`: 116 tests pass (the 111, and five new ones in
  `sst/cache.rs`). `cargo fmt --check` and `cargo clippy --all-targets
  --all-features -D warnings` are clean. The loom tests (5) pass; they do
  not cover this change.
- New tests:
  - a cached block is lent from its slot until it is evicted, and what was
    lent stays whole for as long as its guard;
  - a hit through the cache puts the block back in its slot;
  - a block cached by another reader of the table moves to this one's
    slot, and the cache going away empties every slot;
  - a block read through its slot is spared by the eviction hand once;
  - `readers_and_evictions_race`: 8 threads, 200,000 rounds each, batches
    of four blocks under one guard on a cache far too small for the table,
    every block lent checked to be the one asked for, twice.
- **AddressSanitizer** (`-Zsanitizer=address`, nightly 2026-11-07) over the
  whole suite, the race included: clean.
- **Miri** (same nightly), the four single-threaded cache tests, with Tree
  Borrows and leaks ignored: no undefined behaviour. CI runs them. Why
  those flags:
  - crossbeam-epoch's own list fails Stacked Borrows;
  - a block retired to the epoch is still queued when the process exits.
- **Not covered:**
  - Miri on the race: it stops in parking_lot's futex call, which
    quick_cache's lock goes through when two threads meet;
  - a loom model of insert, evict and read, which the notes of 2026-11-08
    planned. The protocol leans on crossbeam-epoch's guarantees, which
    loom would have to model.

**Crash and visibility** (`bench/gate.sh`, the final branch, `gate/`):

| | acknowledged writes | lost | torn | max reopen | visibility checks | violations |
|---|---:|---:|---:|---:|---:|---:|
| eventual | 1,627,916 | 0 | 0 | 0.055 s | 74.4M | 0 |
| full | 1,627,470 | 0 | 0 | 0.092 s | 107.3M | 0 |
| eventual, 2 MiB memtables | 2,999,998 | 0 | 0 | 0.002 s | 105.3M | 0 |

## Next

- **The search inside a block** is now the largest part of a point read
  (in SoliDB's join: `seek_entry_in` and `restart_key`, 22% of the
  server's time, against 13% for the cache hit before this change). The
  key directory per cached block of the earlier notes is the candidate.
- **Fences in tables of many collections** still tie and go to the index;
  only the block's number is recovered here.
