# Point reads copy small values (2026-10-15)

SoliDB release build on each engine, 5M documents, 64 connections, 8 server
cores, interleaved, 2 repetitions.

- `server-5m/`: phases get, update, mixed_90_10, q_key, q_eq_project after
  the change. Peak RSS: RocksDB 2,042 MB, solistore 1,355 MB.
- `memtrace/`: RSS and jemalloc counters every second.
  `memje5m-*` is the load then `q_eq_project` (5,000 documents per query);
  `memje5m-solistore.txt` is before the change (RSS ~2.05 GB, allocated
  ~1.23 GB), `memje5m-solistore-detach.txt` after (RSS ~1.02 GB, allocated
  ~0.80 GB); RocksDB's is `memje5m-rocksdb.txt` (RSS ~1.8 GB).
  `memw-*` is the load then update and mixed_90_10 (solistore 1.05x RocksDB's
  peak; the rest is memtable entries' real size).

A/B of solistore with and without the change, same phases, 2 runs each
(ops/s): get 746k -> 753k, update 186k -> 188k, mixed_90_10 461k -> 468k.
