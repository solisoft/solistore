# Ahead of RocksDB on every phase, 2026-11-01

The series of `2026-11-01-all` left solistore-eventual behind RocksDB on
one phase: `sync_commit_1t`, 0.89×. That phase is 2,000 fsynced commits
from one thread right after the load, decided by one or two commits that
stall for 27–66 ms. This directory holds the A/B runs behind each change
of branch `perf/beat-rocksdb`, then the final series.

Every run is `engine-bench run` at 5M documents on rocky-build2. Each was
pinned to cores 16-23 with `nice -n 10 ionice -c2 -n7`, in a fresh process
and a fresh data directory, with release builds as in `bench/Cargo.toml`
(fat LTO). solistore ran in `Eventual` (100 ms), RocksDB 11.8.1 with
SoliDB's production profile and `sync=false` writes. Variants alternated
rep by rep.

## final/: the branch against RocksDB

`rocks-N` is the binary of `2026-11-01-all`. `r1-N` is the branch with
its defaults, and `r3-N` the same binary with `--sx-recycle-wals 3`.

Medians of 3, solistore over RocksDB:

| Phase | r1 (defaults) |
|---|---:|
| load | 1.15× |
| update | 1.43× |
| mixed | 1.45× |
| get uniform / hot / missing | 1.33× / 1.63× / 2.34× |
| scans ascending / descending | 1.76× / 1.69× |
| fsynced commits, 1 thread | 1.53× |
| fsynced commits, 8 threads | 1.62× |
| create / drop small / drop large / reopen | 1.41× / 2.75× / 1.38× / 2.47× |
| peak RSS | 0.98× |

The two fsynced-commit phases are decided by one or two stalls a run, for
both engines. The stalls come from table writes by flush and compaction,
which share the RAID 1 with the WAL's fsync. Over every comparable run of
the day (16 of the branch's builds with `wal_bytes_per_sync` and without
paced syncs, 9 of RocksDB), the medians are:

- `sync_commit_1t`: 32,085 against 22,396 (1.43×). 11 of the 16 runs are
  above RocksDB's median.
- `sync_commit_8t`: 225,950 against 139,467 (1.62×). 14 of the 16 runs
  are above it.

Three recycled WALs instead of one (`r3`, as RocksDB's profile keeps)
brought nothing clear, so the default stays at one.

## The A/B runs, each against the build before it

- `ab-wal-bytes-per-sync/`: `base` is b29426d, and `cand` adds
  `wal_bytes_per_sync` (1 MiB). `sync_commit_1t` went from 10,376 to
  37,919/s, and the worst commit from 17–66 ms to 2.5–12 ms. Every other
  phase stayed within 2%. **Kept.**
- `ab-point-reads/`: `cand2` adds point reads that seek index and block in
  place. Get hot went up 9%, get missing 30%, scans 14–15% (each scan does
  100 gets) and mixed 9%. Get uniform stayed flat (2 reps). **Kept.**
- `ab-split-by-bytes/`: `cand3` splits subcompactions at equal bytes read
  from the indexes, rather than at file boundaries. The load got 11% faster
  with no L0 slowdown at all, but compaction wrote 25% more over the run.
  `sync_commit_1t` fell to 0.49× and updates to 0.87×. **Dropped**: it
  needs a level-0 trigger tuned to go with it.
- `ab-paced-syncs/`: `cand4` adds the review fixes and writes each
  finished table out 4 MiB at a time before its fsync. The worst commit
  got shorter, from 12–21 ms to 2.3–15 ms. But in `with-rocksdb/`
  (alternating with RocksDB) a window of commits each waited about 3 ms.
  Over every rep, `sync_commit_1t` had a median of 22.6k with it and 27k
  without. **Dropped.**
