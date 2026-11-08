# Full scans, snapshots and split points, 2026-11-08

Bonfire #777: SoliDB measured a warm full pass over 1M documents at
178–185 ms on solistore (4 GiB cache), over its aggregation budget before
any work per document. Branch `perf/scan-api`.

`engine-bench scan` (bench/scan.sh): 1M documents of ~430 B in one
collection, keys `doc:<n>` under the 8-byte prefix (463 MB of keys and
values a pass), loaded in random key order, flushed and settled. The
tables: one level-0 file over the whole range and seven bottom-level files
(`files_per_level [1,0,0,0,0,0,7]`). Every pass is warm and only counts
entries and bytes. Then point reads of the same documents. Three
repetitions per cache size, binaries alternating; medians of all passes
(best in parentheses).

- `solistore/before-*`: a00db8f (0.2.1) with this bench built
  `--no-default-features --features rocks,solistore` (no scan API: the
  borrowed phase is the owned iterator, the parallel one 8 iterators at
  their own sequence numbers, cut at even key counts).
- `solistore/after-*`: the branch at 2a5d88e, before the index fences.
  Borrowed is `next_ref`; parallel is one
  snapshot passed in `ReadOptions` to 8 threads, cut at `split_points`
  (included in the time).
- `rocksdb/*`: RocksDB 11.8.1, SoliDB's profile. Owned is `iterator_cf_opt`,
  borrowed the raw iterator, parallel 8 raw iterators on one snapshot.

Host rocky-build: AMD EPYC 4244P (6 cores, 12 threads), 62 GiB, XFS on
RAID 1, shared with production services and other builds; runs unpinned at
`nice 10`. Medians moved by up to 40% under that load; the best passes did
not, and the tables below come from a quiet window.

## One thread, ms a pass (lower is better)

| | cache | before | after | RocksDB |
|---|---:|---:|---:|---:|
| owned iterator | 4 GiB | 147 | **84** | 136 |
| borrowed (`next_ref` / raw) | 4 GiB | 147 | **71** | 186 |
| owned iterator | 512 MiB | 149 | **83** | 149 |
| borrowed | 512 MiB | 149 | **71** | 193 |
| owned iterator | 256 MiB | 200 | **165** | 404 |
| borrowed | 256 MiB | 200 | **149** | 389 |

The data decodes to 433 MiB of blocks, which 512 MiB holds: only the
256 MiB runs decompress (LZ4) on every pass. With 4 GiB no block is read
after the first pass (`read_mb` 0, `block_cache_mb` 433 throughout): the
LZ4 time SoliDB's profile showed was the first pass, or tables a
compaction had just written (compactions do not fill the cache).

## Eight threads, one snapshot, ms a pass

| cache | before (no snapshot) | after | RocksDB |
|---:|---:|---:|---:|
| 4 GiB | 22.9 | **15.1** | 34.0 |
| 512 MiB | 22.9 | **15.0** | 34.8 |
| 256 MiB | 39.3 | **34.0** | 66.8 |

## Point reads, thousands of documents a second

Before the index fences, which add 7-8% more (below).

| | cache | before | after | RocksDB |
|---|---:|---:|---:|---:|
| `get`, 1 thread | 4 GiB | 744 | 756 | 476 |
| `get`, 8 threads | 4 GiB | 4,934 | 5,208 | 2,889 |
| `multi_get` of 10, 8 threads | 4 GiB | 5,112 | 5,406 | 3,653 |

## What changed, measured step by step

perf on rocky-build put the time in the merge's comparisons and in
`seek_to_first`; callgrind and `perf stat` showed why. The pass waited on
memory: each cached block is its own allocation, a walk reads one entry
every ~470 bytes, and the hardware prefetcher does not follow that across
a 4 KiB block, so the comparisons stalled on the bytes just parsed. Per
entry, warm, 4 GiB (perf stat, slope between 5 and 15 passes):

| | instructions | cycles | IPC | ms a pass |
|---|---:|---:|---:|---:|
| 0.2.1, owned iterator | 870 | 774 | 1.12 | 146 |
| branch, owned iterator | 756 | 424 | 1.78 | 81 |
| branch, `next_ref` | 546 | 373 | 1.46 | 70 |

The steps, `next_ref` from the second one (best pass of 15, one thread):

| step | ms a pass |
|---|---:|
| 0.2.1 (owned) | 146 |
| merge on an enum, runner-up compare, values in place, children past the range dropped | 138 |
| on entering a block, look the next one up in the cache and prefetch it | 92 |
| inline 8-byte key comparisons, one cursor walk per step, the runner-up's key cached | 76 |
| entry parsing inlined | 75 |

The point reads gained from the inline comparisons, then from the index
fences (`1201f0c`): a table keeps its index restart keys as integers, and
a get binary-searches those. Interleaved A/B of that commit alone, 4 GiB:
`get` 1 thread 759k -> 815k, 8 threads 5.22M -> 5.60M, `multi_get` of 10
5.39M -> 5.82M documents a second; 5,089 -> 3,776 instructions a get.

## No regression elsewhere

`quick/`: `bench/quick.sh` (1M documents, the 14 engine-bench phases), the
branch before the fences against 0.2.1 and RocksDB, interleaved, 2 reps,
medians (operations a second):

| phase | RocksDB | 0.2.1 | branch | /0.2.1 | /RocksDB |
|---|---:|---:|---:|---:|---:|
| load | 379,060 | 540,568 | 540,698 | 1.00 | 1.43 |
| fsynced commits, 1 thread | 12,564 | 21,008 | 21,750 | 1.04 | 1.73 |
| fsynced commits, 8 threads | 51,954 | 181,422 | 185,142 | 1.02 | 3.56 |
| get uniform | 2,648,491 | 3,823,076 | 3,993,862 | 1.04 | 1.51 |
| get hot | 4,307,186 | 7,628,280 | 8,034,894 | 1.05 | 1.87 |
| get missing | 10,013,754 | 20,249,727 | 20,531,530 | 1.01 | 2.05 |
| scan, 100 keys, ascending | 41,752 | 75,510 | 80,578 | 1.07 | 1.93 |
| scan, 100 keys, descending | 39,306 | 69,264 | 72,306 | 1.04 | 1.84 |
| update | 576,257 | 564,272 | 564,770 | 1.00 | 0.98 |
| mixed | 224,042 | 360,363 | 378,692 | 1.05 | 1.69 |

`update` is behind RocksDB on this host at 1M documents with 0.2.1 too
(0.98x); the 1.43x of the README is the 5M series on rocky-build2. An
earlier series, run while RocksDB's C++ compiled on the same disks, had
one branch rep at half the fsynced-commit rate; its second rep and both
reps above are level.

`crash-eventual/`, `crash-full/`: `bench/gate.sh` with `CORES=0-11`, 20
SIGKILL runs and one visibility run per durability mode: 825,901 and
827,433 acknowledged writes, 0 lost, 0 torn, reopen under 0.04 s; 35.5M
and 45.9M visibility checks, 0 violations.
