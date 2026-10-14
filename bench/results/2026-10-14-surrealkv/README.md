# SurrealKV 1.0.0 against RocksDB 11.8.1 and solistore

`bench/quick.sh` was run once per engine on rocky-build2, cores 16-23: 1M
SoliDB-shaped documents (a write is the document plus three index
entries), 1M gets, 300k updates, 20k scans, 20k fsynced commits and 10 s
of mixed load. These are single runs, for orientation, not medians.

The SurrealKV engine (`surrealkv`, `surrealkv-eventual`) has the budget the
other engines get: 512 MiB block cache, 64 MiB memtable, 4 KiB blocks, and
Snappy, since it has no LZ4. `-defaults` is SurrealKV's factory setup: no
compression, 64 KiB blocks, 32 MiB cache, 100 MiB memtable. Its write-only
transactions still check for conflicts optimistically; conflicts are
retried, as an application would.

Unsynced writes (RocksDB `sync=false`, solistore and SurrealKV eventual),
ops/s:

| phase | RocksDB | solistore | SurrealKV | SurrealKV factory |
|---|---:|---:|---:|---:|
| load | 434,149 | 397,995 | 335,448 | 362,938 |
| fsynced commit, 1 thread | 15,664 | 27,500 | 13,989 | 13,730 |
| fsynced commit, 8 threads | 63,500 | 213,949 | 67,879 | 50,490 |
| get, uniform | 3,643,346 | 3,228,656 | 2,566,609 | 307,337 |
| get, hot | 6,452,672 | 6,640,285 | 4,202,062 | 1,629,066 |
| get, missing | 15,325,328 | 12,833,256 | 8,782,026 | 8,732,251 |
| scan, ascending 100 | 59,205 | 82,174 | 40,056 | 11,039 |
| scan, descending 100 | 58,795 | 87,988 | 52,298 | 36,174 |
| update | 650,630 | 483,940 | 376,158 | 169,930 |
| mixed | 289,654 | 346,127 | 179,802 | 38,838 |
| peak RSS | 948 MB | 973 MB | 1,176 MB | 659 MB |

Every commit fsynced (solistore Full, SurrealKV Durability::Immediate):

| phase | solistore | SurrealKV |
|---|---:|---:|
| load | 169,583 | 86,259 |
| fsynced commit, 8 threads | 228,495 | 94,906 |
| get, uniform | 4,605,854 | 2,170,465 |
| update | 174,226 | 90,017 |
| mixed | 163,015 | 153,994 |
| peak RSS | 900 MB | 1,136 MB |

On this workload, SurrealKV is behind RocksDB and solistore on nearly
every phase, at 1.2x their memory. Its tables are the smallest (381 MB
against 433 for solistore), and it reopens in 0.26 s (solistore 0.04 s).
Its factory setup collapses on reads that miss its 32 MiB cache (get
uniform 307k, mixed 39k) and needs 987 MB of disk.

Its published benchmark used 500,000 keys, a dataset that fits in its
memtable and cache, and a RocksDB whose durable commits ran at 8.5k/s
with 128 clients. RocksDB with group commit, as here, reaches 63.5k/s at
8 threads.
