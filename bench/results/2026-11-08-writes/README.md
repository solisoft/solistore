# Sustained inserts and fsynced inserts, 2026-11-08

Bonfire #777, write part. SoliDB's harness on rocky-build2 (4 cores, 16 GB)
saw sustained inserts at c=32 fall from 160k to 118k a second as the
collection grew from 1M to ~10M documents (p99 0.66 -> 6.4 ms), and
fsynced inserts at 73k a second against PostgreSQL's 84k.

`engine-bench sustain` reproduces SoliDB's inserts: `--threads` writers,
each committing one document of ~400 B with its three index entries per
batch, for `--mixed-secs` seconds, on a database that grows as it goes
(~9M documents in 60 s here). One line at the end, with a per-second
series of operations, p99, level-0 files, groups slowed, ms stopped,
batches per commit group, WAL fsyncs and compactions. All runs: 32
writers pinned to 4 cores (`taskset -c 0-3`) on rocky-build, unpinned
background threads.

## Diagnosis

Level 0 climbs past the slowdown trigger (20 files) within ~20 s and the
cycle repeats every ~10 s: one compaction of all of level 0 into the base
level brings it back down, during which flushes keep adding files. More
compaction threads (`--sx-compactions 2`, 3) did not change it.

Under the trigger, each commit group slept 1 ms under the commit lock,
whatever its size: ~870 groups a second of ~21 batches, so ~18k inserts a
second against ~300k unthrottled, and p99 2.4 ms.
`diagnosis/before.jsonl` (4c6b9ab):

    sec   ops/s   p99us  L0  slowed
      0   366259     280   4       0
     21   325498     319  19       0
     30    18288    2473  22     872
     36   277619     325  11       0
     42    18874    2414  22     876

RocksDB, same run (`diagnosis/rocksdb.jsonl`): 115k-220k a second, no collapse.

## Change and results

Writes slow down gradually (`Inner::throttle`): past the trigger, a group
waits for its bytes at 256 MiB/s, a fifth less per file over it (4 MiB/s
at least), instead of a millisecond a group. Starting at 8 files
(`diagnosis/graduated-from-8-files.jsonl`) cost 10% of the throughput;
the trigger stays at 20.

60 s, 32 writers, 4 cores, eventual; two reps (`throttle/` has the second):

| | before | after | before | after |
|---|---:|---:|---:|---:|
| inserts a second, whole run | 190,825 | 188,645 | 167,210 | 180,531 |
| p99, whole run | 2,280 µs | 737 µs | 2,270 µs | 717 µs |
| worst second | 16,249 | 49,493 | 15,347 | 48,082 |
| worst second's p99 | 3,055 µs | 1,543 µs | 3,579 µs | 1,305 µs |
| seconds under 136.5k | 18 | 20 | 12 | 17 |

The cliff and the p99 spikes are gone; the cycle is not. Compaction of
level 0 on 4 cores does not keep up with ~300k inserts a second, and the
throttle trades a few deep seconds for more shallow ones. Holding 136.5k
every second needs compaction to keep up (see the report: level 0
compacted while a deeper compaction runs, intra-level-0 compaction, a
throttle on compaction debt as well as on level-0 files).

The quick engine-bench phases never reach the trigger (0 slowdowns), so
they run the same code as before.

## Tried and not kept: no yielding in the commit queue

`spin_until` spins 64 times, then yields the core for up to 50 µs before
sleeping. Without the yield phase, 32 writers on 4 cores went faster
(fsynced 154k -> 165k a second, p99 450 -> 290 µs, CPU -16%; unsynced
314k -> 334k), but `bench/quick.sh` (8 writers on 12 cores, 2 reps) lost
18% of its load, 24% of its updates and half its 8-thread fsynced commits
(183k -> 94k): with idle cores, a follower that sleeps pays a wake-up the
spin avoided. Halving the number of spinners was inconclusive in a noisy
hour. The spin needs to depend on writers against cores (e.g. no yield
phase while more writers wait than there are cores), measured on both
shapes.

## Fsynced inserts

solistore `Full` at 32 writers on 4 cores: 144k-165k a second with ~23
commits per fsync (`diagnosis/fsynced-solistore.jsonl`); RocksDB with
`sync=true`, 111k. This disk does not show the harness's 73k: its fsyncs
are faster than rocky-build2's, and with 32 writers the ceiling is about
32 / (fsync latency + handoff). The leader appends and fsyncs under the
commit lock, and the next group starts only after that fsync returns.
