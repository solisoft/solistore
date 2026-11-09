# Point reads and batched reads, 2026-11-08

SoliDB fetches the documents of a query by key: `get` one at a time, and
`multi_get` from the index paths and joins. This round set out to halve
the instructions a point read costs and to cut its latency. Branch
`perf/point-reads`, for 0.3.1: `multi_get_with` is new and no signature
changes.

`engine-bench point` (`bench/point.sh`) runs over the scan mode's
collection, sharing its data directory. That is 1M documents of ~430 B,
with keys `doc:<n>` under the 8-byte prefix, in one level-0 file and seven
bottom-level files. The cache is 4 GiB, and one full pass first makes
every read warm. Each phase reads 2M documents at random keys, except
`seq`, which reads 20 consecutive keys from a random start:

- `get`: one key per call.
- `multi_get` of 10 or 20 keys. RocksDB's is `batched_multi_get_cf` with
  unsorted input, as SoliDB calls it.
- `multi_get_with`: values lent instead of handed out. 0.3.0 has no such
  call, so its row is its `multi_get`; RocksDB's row is
  `batched_multi_get_cf` again, which already returns pinned slices.
- `memtable`: the same reads of 100k documents still in the memtable, on
  a fresh database.

Two measures:

- ns per document: thread time over documents read, so the 8-thread rows
  compare with the 1-thread ones.
- Instructions and cycles per document: the threads' user-space counters
  (`perf_event_open`).

The binaries:

- `0.3.0`: 951b793 (0.3.0 plus one test), with this bench built
  `--no-default-features --features rocks,solistore,scan-api`.
- `now`: the branch, with the same features plus `point-api`.
- `RocksDB`: 11.8.1 with SoliDB's profile, run from the `now` binary.

Both were built alike: Rust 1.98.1, fat LTO and one codegen unit (the
host's thin-LTO override was set aside). There were five repetitions,
the three binaries alternating and the order reversed every other
repetition. The tables give medians.

Host rocky-build: AMD EPYC 4244P (6 cores, 12 threads), 62 GiB, shared with
production services. Point runs were unpinned at `nice 10`, with a
one-minute load average of 1.5–2.3 (`log.txt`). The quick and crash
gates below were pinned to cores 0–11.

## ns per document (lower is better)

| | 0.3.0 | now | RocksDB | now / 0.3.0 | RocksDB / now |
|---|---:|---:|---:|---:|---:|
| `get`, 1 thread | 1,088 | **685** | 1,643 | 0.63 | 2.40 |
| `get`, 8 threads | 1,187 | **784** | 1,995 | 0.66 | 2.54 |
| `multi_get` of 10, 1 thread | 1,103 | **606** | 1,525 | 0.55 | 2.51 |
| `multi_get` of 20, 1 thread | 1,086 | **608** | 1,460 | 0.56 | 2.40 |
| `multi_get_with` of 10, 1 thread | 1,087 | **493** | 1,515 | 0.45 | 3.07 |
| `multi_get_with` of 20, 1 thread | 1,084 | **493** | 1,466 | 0.45 | 2.98 |
| `multi_get` of 10, 8 threads | 1,184 | **726** | 1,802 | 0.61 | 2.48 |
| `multi_get` of 20, 8 threads | 1,186 | **722** | 1,728 | 0.61 | 2.40 |
| 20 consecutive keys, 1 thread | 382 | **226** | 712 | 0.59 | 3.16 |
| memtable `get`, 1 thread | 400 | 390 | 622 | 0.98 | 1.60 |
| memtable `multi_get` of 20, 1 thread | 402 | **367** | 660 | 0.91 | 1.80 |

Ranges over the five runs (min–max):

| | 0.3.0 | now | RocksDB |
|---|---|---|---|
| `get`, 1 thread | 1,070–1,108 | 682–693 | 1,625–1,736 |
| `multi_get` of 20, 1 thread | 1,067–1,135 | 601–613 | 1,426–1,507 |
| `multi_get_with` of 20, 1 thread | 1,068–1,110 | 487–507 | 1,449–1,511 |
| `multi_get` of 20, 8 threads | 1,181–1,197 | 717–728 | 1,714–1,763 |
| memtable `get`, 1 thread | 391–450 | 377–401 | 606–671 |

Memtable gets were not worked on in this round and are unchanged. A
series before the fix below had their median at 398 -> 432, with runs of
both binaries anywhere from 382 to 505. They already take 0.63x
RocksDB's time.

## Instructions and cycles per document, 1 thread

| | 0.3.0 | now | RocksDB | instructions now / 0.3.0 |
|---|---:|---:|---:|---:|
| `get` | 3,435 / 5,562 | 2,987 / 3,502 | 12,018 / 8,406 | 0.87 |
| `multi_get` of 20 | 3,383 / 5,559 | 2,795 / 3,110 | 10,312 / 7,439 | 0.83 |
| `multi_get_with` of 20 | 3,383 / 5,549 | 2,590 / 2,511 | 10,312 / 7,501 | 0.77 |
| 20 consecutive keys | 3,381 / 1,957 | 2,342 / 1,146 | 9,126 / 3,633 | 0.69 |
| memtable `get` | 2,814 / 2,043 | 2,686 / 1,990 | 5,500 / 3,186 | 0.95 |

The goal of half the instructions is missed: a get costs 13% fewer, a
batched read of random keys 16–23% fewer (31% for consecutive keys).
The time saved is mostly cycles that waited on memory. A get now takes
37% fewer cycles (IPC 0.62 -> 0.85), and a lent batch of 20 overlaps
its misses well enough to run at 1.03.

Where a warm read's cycles go now (`perf record` of a line-tables build
over 20M reads, by function, `prof/`):

| | `multi_get` of 20 | `get` |
|---|---:|---:|
| block-cache hit: `BlockCache::get`, plus the block's `Bytes` cloned and dropped | 37% | 32% |
| the fences' binary search (`Fences::place`) | 8% | 9% |
| searching the block (`seek_entry_in`, `version_in`) | 21% | 15% |
| copying values out (`memmove`) | 4.5% | 5.6% |

No one layer holds half of a read, which is why halving the
instructions would take a structural change (Next, below).

## Per-call latency, ns (p50 / p99)

| | 0.3.0 | now | RocksDB |
|---|---:|---:|---:|
| `get`, 1 thread | 1,072 / 1,943 | 682 / 1,152 | 1,633 / 2,886 |
| `get`, 8 threads | 1,163 / 2,104 | 792 / 1,423 | 1,954 / 3,647 |
| `multi_get` of 20, 1 thread | 21,570 / 27,181 | 12,012 / 14,938 | 29,055 / 35,056 |
| `multi_get_with` of 20, 1 thread | 21,491 / 27,942 | 9,728 / 12,453 | 29,004 / 36,098 |
| `multi_get` of 20, 8 threads | 23,524 / 29,896 | 14,367 / 18,855 | 34,805 / 45,766 |

## What changed

**A get.**

- **Fences per block.** A table keeps, for each data block, an 8-byte word
  of its separator and the block's offset, where it kept one word per
  index restart point. A get finds its block and handle from them
  without walking the index; separators that tie on their word fall back
  to the index (see the fix below). That is ~12 bytes a block instead of ~2, or ~200 KB per
  64 MiB table, charged to the cache budget like the index and filter.
- **Seek in place.** A block is searched by reading entry headers and
  unshared bytes only, without rebuilding keys. Keys it cannot order on
  those bytes alone go to the old walk.
- **Entry offsets.** A block of at most 16 entries keeps where each one
  starts, found when it is decoded. A cache hit starts fetching those
  headers before the reference count's write (the `Cached` wrapper in
  `sst/cache.rs`), so a get steps over the entries without waiting for
  each in turn. Scans keep the whole-block prefetch of `look_ahead`.
- **Smaller changes:**
  - bloom lines prefetched before they are probed;
  - an unrolled `may_contain`;
  - `memcmp` for table bounds;
  - the range-tombstone pass skipped when there are none.

  The prefetches live in a new module, `hint.rs`, the crate's only new
  `unsafe`; it is compiled out under Miri.

**A regression the quick gate caught, and its fix.** The quick bench's
tables hold 64 collections and four kinds of key (`doc:` and three index
families). Their separators share only the column family and the start
of the keyspace prefix. So the word after that prefix is a keyspace byte
and `doc:000` (or `idx:…`), the same for every block of one collection:
thousands of ties per table. Ties fall back to the index, as they did in
0.3.0. But the fences counted the ties one by one, as 0.3.0 did, now
over four times as many words (one per block, not one per restart
point) and twice per get. On the branch before the fix, against 0.3.0
(`quick-ties-walked/`, operations a second, mean of 2):

| phase | 0.3.0 | branch, ties walked |
|---|---:|---:|
| get uniform | 3,205,913 | 1,443,085 (0.45x) |
| get hot | 5,222,106 | 1,728,900 (0.33x) |
| scan, 100 keys, ascending | 51,808 | 17,086 (0.33x) |
| mixed | 312,335 | 163,348 (0.52x) |

The point bench never saw this: its one collection's keys differ within
their first eight bytes past the prefix. Two changes fixed it:

- The end of a tie run is found by a binary search, entered only when the
  target ties (most do not).
- `Fences::locate` returns the block, or the index range when the words
  tie, from one search.

The unit test `fences_among_thousands_of_ties` builds an index of that
shape. With the fix, one run each (`ab/`): get uniform 3.13M -> 4.67M a
second against 0.3.0, get hot 5.28M -> 8.06M, scans 53.6k -> 81.4k. The
quick table below is the full series.

**`multi_get`** (`SuperVersion::multi_get`, `read_run` in `version.rs`):

- The keys are put in key order by their first 8 bytes past the prefix
  they share, and not sorted at all when they arrive in order.
- The super version is taken once.
- Each memtable is read once for all keys.
- Each run of tables (a level-0 file, or a deeper level) is read in
  stages over all the keys still missing: filters, then blocks from the
  fences, then the seeks.
- Consecutive keys in one block read it once.
- The values go to the caller in the order of the keys.

`Db::multi_get` and `Snapshot::multi_get` keep their signatures. The new
`multi_get_with` lends each value:

```rust
pub fn multi_get_with<K: AsRef<[u8]>>(
    &self,
    cf: &ColumnFamily,
    keys: &[K],
    f: impl FnMut(usize, Option<&[u8]>),
) -> Result<()>
```

It exists on `Db` and on `Snapshot`. `f` is called once per key, in the
order of `keys`, with the key's position and its value. The value is
borrowed from its memtable or cached block for the length of the call.
On an error, `f` is not called at all.

**The bench.**

- `engine-bench point` and `bench/point.sh`: the phases above.
- The `point-api` feature: off, the bench builds against 0.3.0.
- The `visibility` test now reads through `multi_get` as well. Half the
  readers take every acknowledged document of a writer nearby in one
  collection (about 30) in one batch, and all must be found. Every
  document seen is read again with its three index entries in one batch,
  and all four must be found.

## Tests and gates

All of these ran on the branch with the fix above, on rocky-build unless
stated otherwise.

- `cargo test --profile ci`, the whole suite: 111 tests pass.
  `cargo fmt --check` and `cargo clippy --all-targets --all-features -D
  warnings` are clean.
- The model test (`tests/model.rs`) gained `MultiGet` operations:
  - owned and lent;
  - live, and through kept snapshots;
  - with repeated and absent keys.

  4,096 cases pass (1.4 s), and 40,000 (13.4 s).
- `multi_get_reads_what_get_reads` (`tests/reads.rs`) reads
  SoliDB-shaped batches over level 0, deeper levels and the memtable.
  The data has overwrites, deletes, a range delete, values larger than
  half a block, and a snapshot.
- Property tests check the fences and the in-place seek against a plain
  walk, and `fences_among_thousands_of_ties` checks the case above
  (`sst/block.rs`).
- Mutations:
  - Four bugs put into the in-place seek were each caught.
  - A block reused for the next key when its offset matches but the table
    differs: the model test catches it.
  - The fallback to the next block, when a key lands past every entry of
    its block, removed: no test catches it, and none can. A separator
    sorts after every version of its block's last key and before every
    version of the next block's first. A lookup therefore lands past a
    block only for a key the table does not hold (a bloom false
    positive), and the next block's first entry is then another key:
    not found either way. The single get keeps the same fallback, as a
    guard.
- loom (`--cfg loom`, `loom_tests`): 5 pass. This round does not change
  the commit path.
- Miri ran on the Mac (aarch64), with the nightly of 2026-11-07; the
  build host has no nightly. It found nothing:
  - CI's set, `skiplist` and `memtable`: 15 tests, among them concurrent
    inserts with readers and the memtable's reads in place (`get_ref`),
    which `multi_get_with` lends from. 34 minutes.
  - The `sst` tests, with isolation off for proptest's `getcwd`: 14
    passed (8 in `block`, `fences_among_thousands_of_ties` among them;
    both of `bloom`; 4 of `table`). The table's separator test was
    stopped after 44 minutes in all. The two block proptests (512 and
    2,048 cases, fixed in the code) were left out; they and the
    separator test run natively in the suite.

  `block.rs`, `bloom.rs`, `table.rs` and `version.rs` hold no `unsafe`,
  and the prefetches are compiled out under Miri.

**Crash and visibility.** `bench/gate.sh` ran the `now` binary 20 times
(SIGKILL) plus one visibility run per configuration:

| | acknowledged writes | lost | torn | max reopen | visibility checks | violations |
|---|---:|---:|---:|---:|---:|---:|
| eventual | 819,707 | 0 | 0 | 0.037 s | 72.9M | 0 |
| full | 815,518 | 0 | 0 | 0.038 s | 80.6M | 0 |
| eventual, 2 MiB memtables (`--sx-buffer-mb 2`) | 1,544,020 | 0 | 0 | 0.003 s | 95.2M | 0 |

The visibility runs now read through `multi_get` too (above); with 2 MiB
memtables, those batches race many flushes. RocksDB, under the same
checks, counts 0 violations in 37.8M (`visibility-rocks/`): the checks
raise no false alarm on the reference engine.

**No regression elsewhere.** `bench/quick.sh` (1M documents, the
engine-bench phases) ran 0.3.0, now and RocksDB interleaved, 2 reps.
Means, operations a second:

| phase | RocksDB | 0.3.0 | now | now / 0.3.0 | now / RocksDB |
|---|---:|---:|---:|---:|---:|
| load | 424,254 | 558,195 | 547,196 | 0.98 | 1.29 |
| fsynced commits, 1 thread | 11,645 | 22,346 | 22,120 | 0.99 | 1.90 |
| fsynced commits, 8 threads | 52,081 | 181,595 | 175,434 | 0.97 | 3.37 |
| get uniform | 2,642,038 | 3,202,156 | 4,566,380 | 1.43 | 1.73 |
| get hot | 4,222,500 | 5,293,376 | 7,988,104 | 1.51 | 1.89 |
| get missing | 9,854,173 | 20,283,720 | 20,435,686 | 1.01 | 2.07 |
| scan, 100 keys, ascending | 41,807 | 52,928 | 82,838 | 1.57 | 1.98 |
| scan, 100 keys, descending | 39,618 | 49,364 | 76,224 | 1.54 | 1.92 |
| update | 571,828 | 572,418 | 573,140 | 1.00 | 1.00 |
| mixed | 227,496 | 313,842 | 412,318 | 1.31 | 1.81 |

Gets gain less here than in the point bench (1.43x a second against
1.59x): the fences of these tables tie and the index decides (above).
Scans gain 1.5x, and `mixed` follows the reads. The load and the 8-thread fsynced commits read
2–3% lower, within their spread:

- Load: 544.6k and 549.8k a second against 557.2k and 559.1k. The A/B
  run (`ab/`) had it the other way, 558.7k against 555.7k.
- Commits: 180.5k and 170.3k against 183.0k and 180.2k. That phase
  moves with the stalls that flushes and compactions cause on the disks
  the WAL shares.

Neither phase reads in the foreground. Compactions do, and each new
table now builds its fences when it is opened, a walk of its index.

Peak RSS is 955 MB against 916 (+4%). It is reached in `mixed`, which
does 31% more operations in its 10 s. After the read phases, RSS is
698–701 MB against 682–685 (+2%): the fences, and a larger block handle
(the inline entry offsets, ~40 bytes per cached block).

## Next

- **Instructions.** The ~3,000 left in a get are spread across the
  layers; halving them takes a structural change, such as a key
  directory kept per cached block (about 5% of the cache's memory). Not
  in this round.
- **Block-cache hits without locked writes.** A hit takes
  four locked read-modify-writes:
  - the shard's read lock, taken and released;
  - the block's `Bytes`, cloned (its reference count, in an allocation
    of its own, is a likely miss) and dropped.

  Each is a full barrier on x86, which also holds back the misses a
  batch has started to overlap. The hit is 37% of a `multi_get` of 20's
  cycles and 32% of a get's (above).

  The replacement is a slot per data block in each table: an
  `AtomicPtr`, indexed by the block number the fences already give,
  read under an epoch guard taken once per get or per batch, so a hit is
  one load.
  - Eviction unlinks the slot. The block is freed once no reader that
    could hold it is left in an older epoch.
  - The replacement policy and the byte budget move to insert and
    eviction. A hit only sets a referenced bit, with a plain store when
    it is not already set.
  - `multi_get_with`'s values live as long as the guard. `get` copies a
    small value, as it does now, and takes a reference only for a value
    that is most of its block.

  **Estimate.** About half of the hit's cycles come back: the barriers
  and the reference count's miss go, the slot's and the block's own
  misses stay. That would put a `multi_get` of 20 near 500 ns a document
  instead of 608, and a get near 580 instead of 685, give or take 30 ns.
  A get pays one epoch pin, a barrier, where a batch pays one for all
  its keys.

  **Effort.** One to two weeks:
  - the index and its reclamation (`crossbeam-epoch`, pure Rust, or a
    small epoch scheme of our own);
  - eviction against the byte budget;
  - a loom model of insert, evict and read;
  - Miri on the new `unsafe`;
  - the model test and the gates.
- **Fences in tables of many collections.** There the fences tie on a
  collection's word and the index decides, as above. A second word per
  run of ties, past the prefix the run shares, would let them decide
  there too.
- **Memtable gets** are unchanged in this round. They already run at
  0.63x RocksDB's time (390 against 622 ns a document).
