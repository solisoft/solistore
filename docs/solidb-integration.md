# Putting solistore under SoliDB

Status: plan, 2026-10-14. solistore passed the M7 go/no-go gates
(bench/results/2026-10-14-m7). This document is how it replaces RocksDB in
SoliDB without a flag day: every step ships on its own and keeps RocksDB
working until the last one.

## What SoliDB uses today

Surveyed in `~/Work/soli/db` (file:line references are to that tree):

- One RocksDB for data (`storage/engine.rs:315-339`, column families
  `default`, `_meta`, `__keyspaces__`, plus legacy 1.x per-collection
  families) and a second one for the replication log (`sync/log.rs:143-150`).
- Collection code already goes through `KsDbExt` / `KsBatchExt` / `KsCf`
  (`storage/keyspace.rs:305-539`): ~200 `*_ks` calls in 14 files. Their
  signatures still use RocksDB types: `IteratorMode`, `ReadOptions`,
  `DBPinnableSlice`, `rust_rocksdb::Error`, `WriteBatch`.
- Raw column-family code bypasses those traits: `collection_registry.rs`,
  `engine.rs`, `pending_drops.rs`, `database.rs`, `cf_ops.rs`,
  `keyspace_gc.rs`, `keyspace_migration.rs`; `_meta` is only ever reached
  that way.
- `WriteBatch` crosses modules: ~55 constructions, ~10 functions taking
  `&mut WriteBatch` (crud, txn, versioning, columnar, registry), batches
  spanning `_meta` and `__keyspaces__`.
- Errors: one `From<rust_rocksdb::Error> for DbError` plus ~130
  `map_err(.. InternalError(format!(..)))` sites. `DbError` itself carries
  no RocksDB type, but the public `solidb::storage` API does: `RocksDb`,
  `Database::db_arc()`, `Collection::new/open`, `KsIter::Item`.
- RocksDB features in use, each with a solistore equivalent:

  | SoliDB uses | solistore |
  |---|---|
  | create/drop CF at run time, `list_cf` | `create_cf`, `drop_cf`, `cf_names` |
  | get, pinned get, multi_get | `get` (zero-copy `Bytes`), `multi_get` |
  | atomic batch across CFs, `sync` per write | `WriteBatch`, `WriteOptions { sync }` |
  | `delete_range` in a batch, then `compact_range` | same; reclaim measured at M7 |
  | bounded iterators, `From(k, Reverse)` as seek-for-prev | `iterator_opt` with the same modes |
  | `prefix_iterator` without extractor (= seek, caller stops) | `From(prefix, Forward)` |
  | checkpoint (`POST /_api/backup`) | `checkpoint` (hard links) |
  | memtable / table-reader / block-cache / files-per-level properties | `memtable_bytes`, `table_readers_bytes`, `block_cache_usage`, `files_per_level` |
  | `get_approximate_sizes_cf` (keyspace disk usage) | `approximate_size(cf, start, end)` |
  | `set_readahead_size` | no-op (pread); revisit if scans need it |
  | `delete_file_in_range_cf` (migration only) | not needed |

## Two decisions (taken 2026-10-14: Eventual 100 ms; refuse the 1.x layout)

1. **Durability.** SoliDB writes with `sync = false` today, except
   transaction commits. solistore's default is an fsync per commit
   group. Ship with `Durability::Eventual { 100 ms }`, which keeps today's
   behaviour (process crash loses nothing; power loss up to 100 ms) and
   measured at RocksDB's speed. Offer `--durability full` and decide on the
   default after measuring SoliDB's own workloads with both.
2. **The 1.x layout.** About 1,700 lines exist only for per-collection
   column families (migration, CF reuse grace, legacy branches, legacy disk
   usage). Since 2.0 every start migrates them. Recommendation: solistore
   refuses a database that is not fully on the shared-keyspace layout
   (`_meta storage_format=2`, no `pending_drop:` markers, no column family
   besides the three), and the legacy code is removed in the major version
   that drops RocksDB.

## Steps

### S1 — A storage interface in SoliDB, RocksDB behind it (no behaviour change)

**Done** on SoliDB's `feat/storage-kv` branch: `storage::kv::Kv` is an
enum-dispatched engine with RocksDB's method names and SoliDB's own types
(`KvCf`, `KvBatch`, `KvIter`, `KvError`, read and write options), and the
`RocksDb` alias names it. RocksDB is configured and opened in
`storage/kv/rocks.rs`; `rust_rocksdb` is named nowhere else outside tests of
the 1.x layout. A trait was not needed: a closed enum keeps every call
statically dispatched.

New module `storage::kv`, owned by SoliDB:

- `Kv` (open, `cf(name)`, `create_cf`, `drop_cf`, `cf_names`, `get`,
  `get_pinned`, `multi_get`, `write(batch, sync)`, `iter(cf, bounds, mode)`,
  `compact_range`, `flush`, `checkpoint`, `approximate_size`, `metrics()`),
  `KvBatch`, `KvIter`, `IterMode`, `Bounds`, `KvError`.
- `RocksKv` implements it over today's `RocksDb`.
- Move, in this order, each a reviewable change with the full test suite
  green in CI:
  1. `keyspace.rs`: `KsDbExt`/`KsBatchExt` over `Kv`/`KvBatch`; `KsIter`
     items become `Result<(Bytes, Bytes), KvError>`; drop `KsCf::raw()`
     (one caller, legacy disk usage).
  2. The `&mut WriteBatch` functions (crud, txn, versioning, columnar,
     registry) to `KvBatch`.
  3. `IteratorMode`/`ReadOptions` sites outside keyspace.rs (crud, scan,
     versioning, indexes, index_opt, driver) to `IterMode`/`Bounds`.
  4. Raw CF code (registry, engine, gc, cf_ops, pending_drops, database) to
     `Kv`.
  5. Metrics (`engine.rs:1095-1145`) and checkpoint (`engine.rs:410-438`)
     through `Kv::metrics`/`Kv::checkpoint`.
  6. `sync/log.rs` onto its own `Kv` instance.
  7. Public API: `Database::db_arc()` and `Collection::new/open` stop
     exposing `Arc<RocksDb>`.
- Done when `rust_rocksdb` is named only in `storage/kv/rocks.rs` and in
  tests that exercise the legacy layout.

Estimate: the largest step. ~30 files, mostly mechanical; the 130
`map_err` sites shrink to one `From<KvError>`.

### S2 — solistore behind a feature, chosen per data directory

**Done** (SoliDB `feat/storage-kv`, 2026-10-14):

- solistore is a second `Kv` engine behind `engine-solistore`, with a git
  dependency on this repository.
- Each data directory records its engine in an `ENGINE` file. An unmarked
  directory with RocksDB `OPTIONS-*` files is RocksDB's; any other
  ambiguous one is refused. A new directory takes `SOLIDB_ENGINE`.
- Durability on solistore is set by `SOLIDB_DURABILITY`: `eventual`
  (100 ms) by default, or `full`.
- `KvBatch` takes its engine from its first column family, and memory
  figures come from a database-wide `Kv::memory()`.
- `/metrics` exports `solidb_storage_*` counters on both engines.
- All 121 SoliDB test binaries pass on both engines.

Measured in the whole server against RocksDB 11.8.1, at 1M documents over
the native driver with the query cache off. The numbers are on the
results dashboard and in `bench/results/2026-10-14-solidb-server`.

| | Ratio |
|---|---|
| Driver phases, 64 connections | 0.99-1.11 |
| Driver phases, 256 connections | 1.00-1.14 |
| SDBQL queries, writes included | 1.02-1.16 |

Peak RSS is 1.02-1.08x RocksDB's. Getting there took these solistore
changes (d1fc8f5 to f3d1618):

- Block seeks without key copies.
- A fast block-cache hasher.
- No fsync inside a memtable switch, with point-in-time recovery across
  WALs.
- Memtables counted like RocksDB's.
- Each table opened once, before it is installed.
- A commit pipeline that holds up with more writers than cores.

The CI job that runs SoliDB's tests on solistore (`test-solistore`) is in
place, and the release job waits for it.

Since then (2026-10-15):

- Point reads copy a small value instead of handing out a slice that kept
  its 4 KiB block or 1 MiB arena chunk alive (53fa0eb). At 5M documents,
  with 64 connections each reading 5,000 documents per query, peak RSS went
  from 1.27x RocksDB's to 0.66x.
- A synced write in eventual mode fsyncs every older WAL itself (9ff023f).
  It could rely on the background syncer's fsync of one, started but not
  finished, and be lost with that WAL's tail. Seen once on macOS CI.
- Compaction to the bottom of the tree writes sequence 0, as RocksDB does:
  tables 1.07x RocksDB's size -> 1.02x after a 1M-document load.

- `storage/kv/solistore.rs`: `Kv` over `solistore::Db`, behind a cargo
  feature `engine-solistore` (git dependency on
  github.com/solisoft/solistore, crates.io once published).
- Missing in solistore: a read-only open (for the migration tool), and the
  `EngineProfile` knobs (prod/dev sizes) mapped onto `Options`.
  `approximate_size` is done.
- In SoliDB: `KvBatch::default()` cannot know its engine, so a batch takes
  its engine from the first column family written to it. Memory metrics
  move to a database-wide `Kv::metrics()`: solistore's figures are not
  per column family.
- The engine is recorded in the data directory (`ENGINE` file); a
  directory is opened with the engine that wrote it, so a binary with both
  can serve either. `--engine solistore` selects it for a new directory.
- CI: the `test` job runs twice, once per engine. A failure in one is a
  failure.

### S3 — Moving existing data

**Done** (SoliDB `feat/storage-kv`, 942074d5): `solidb-migrate-engine`
copies a stopped RocksDB data directory into solistore, verifies each
column family (entries, bytes, a hash of every key and value), swaps the
directories and keeps the RocksDB one as a backup. 1.47M entries took
1.1 s; the server answered the same afterwards. It opens RocksDB itself
(which takes its lock) rather than a checkpoint.

- `solidb-migrate-engine <dir> --to solistore`: open the RocksDB read-only
  from a checkpoint, copy every column family in key order in 4 MiB
  batches, copy the sync log, write `ENGINE`, verify per keyspace
  (key count and an xxh3 of keys and values), then swap directories. The
  source stays as a backup until removed by hand.
- Logical `solidb-dump` / `solidb-restore` remain the fallback.

### S4 — Proving it on real workloads, then rolling out

- The dev box's server (92 databases, 1,809 collections) on solistore for
  a week, both engines' `/metrics` compared.
- SoliDB's own benchmarks on both: the framework bench through the driver
  with the query cache off (see the profiling note in SoliDB's memory),
  the HTTP API tests, `bench_all.sh` for the clients.
- Then production, one instance at a time, with the RocksDB directory kept
  for rollback.

### S5 — Removing RocksDB

- Default engine solistore; RocksDB kept one minor version for rollback.
- Then remove `rust-rocksdb`, `storage/kv/rocks.rs`, the 1.x layout code,
  and the CI work that exists for C++: the Windows job can run tests again
  instead of a compile-only check, the musl/aarch64 cross g++ toolchains
  go, `LIBCLANG_PATH` goes, and the 95-minute compile that motivated all
  of this should drop to the Rust part.
- `jemalloc`'s `unprefixed_malloc_on_supported_platforms` existed so that
  RocksDB's C++ allocations went through jemalloc; reconsider it.
- MSRV 1.91 was set by rust-rocksdb 0.53; solistore needs no more than
  that, so it can stay or be lowered.

## Open items in solistore before S2

- Performance margins are thin on writes: load 0.81x and update 0.82x of
  RocksDB without sync (thresholds 0.8). Durable commits vary widely
  between runs (sync_commit_8t: 0.20x, 1.25x, 1.24x).
- Tables are ~2% larger than RocksDB's for the same data (was 7%, before
  sequences were zeroed at the bottom of the tree). The rest is the index,
  which keeps whole last keys where RocksDB keeps shortened separators.
- The memtable's real footprint is above what it counts: a skiplist node
  costs ~112 bytes, counted as 32 to flush as often as RocksDB. On write
  phases at 5M documents, peak RSS is 1.05x RocksDB's.
- Not done from the plan: dynamic level sizing, the arena skiplist proper
  (entries are arena-backed, nodes are not), `loom` on the commit path,
  Miri on the `unsafe` (one cast, one `sync_file_range` call), fuzzing of
  the decoders.
- Windows: `StdFs::sync_dir` is a no-op there and `start_writeback` does
  nothing; the CI cross-check compiles and runs the tests on Windows, but
  no crash test has run on NTFS.
