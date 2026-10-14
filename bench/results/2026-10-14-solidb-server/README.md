# SoliDB on both engines, whole server

The first measurement of solistore *inside* SoliDB rather than through the
engine bench. It used the same release binary (`rbuild --faithful`, SoliDB
`feat/storage-kv` at e1f9629e, built with `engine-solistore`). Each run
started on a fresh data directory, with `SOLIDB_ENGINE` choosing the
engine. The runs alternated RocksDB and solistore, 3 times each. The
server ran on cores 16-23 and the client on cores 24-31 (rocky-build2).
`run.sh` is the script.

- **Load:** `clients/rust-client/examples/engine_bench.rs` sent 1M
  documents of about 250 bytes, indexed on `group` and `n`. It used 64
  native driver connections with the query cache off. Each phase lasted
  15 s.
- **Durability:** solistore used Eventual 100 ms. RocksDB used SoliDB's
  `sync=false`.

Medians of 3, ops/s:

| phase | RocksDB | solistore | ratio | p99 µs (R / s) |
|---|---:|---:|---:|---|
| insert, 1 doc per request | 310,066 | 305,968 | 0.99 | 1,047 / 950 |
| get by key | 767,441 | 789,836 | 1.03 | 151 / 151 |
| update (merge) | 211,853 | 189,058 | 0.89 | 1,242 / 1,410 |
| query, index equality, LIMIT 10 | 147,529 | 141,146 | 0.96 | 1,239 / 1,203 |
| query, index range (≤2,000 entries) | 2,499 | 2,527 | 1.01 | 64,216 / 62,658 |
| mixed, 90% get / 10% update | 581,114 | 533,512 | 0.92 | 665 / 692 |
| peak RSS | 1,100 MB | 1,122 MB | 1.02× | |
| disk after the run | 421 MB | 304 MB | 0.72× | |

The server's 8 cores were the limit in every phase, so a ratio is close to
a ratio of CPU per operation. Updates are where solistore trails; that
matches the engine bench, where its write path was 0.82× RocksDB's.

Reopen time is not reported: the script only measures time until the port
accepts, and it read 0.052 s, its polling floor, for every run.

Two SoliDB findings, independent of the engine:

- **Index range scans read to the end of the index.** The planner takes
  only the first range conjunct (`n >= lo`), reads from there to the end of
  the index (bounded only by `SOLIDB_MAX_INTERMEDIATE_ROWS`), then applies
  `n < hi` as a residual filter; LIMIT is not pushed down either. The range
  phase therefore keeps its lower bound in the last 2,000 documents.
- **Connections are dropped under load.** When the runtime is saturated,
  the driver's 100-slot dispatch channel fills, and the server drops new
  connections ("driver dispatch channel full"). The tool now opens its
  connections once.
