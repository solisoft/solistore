# solistore

[![CI](https://github.com/solisoft/solistore/actions/workflows/ci.yml/badge.svg)](https://github.com/solisoft/solistore/actions/workflows/ci.yml)
![Rust 1.91+](https://img.shields.io/badge/rust-1.91%2B-orange)
![no C toolchain](https://img.shields.io/badge/C%2FC%2B%2B-none-brightgreen)

**An embedded, ordered key-value store built as an LSM tree, in pure Rust.**
It fsyncs every commit by default and makes concurrent commits share that
fsync. It is written to replace RocksDB under
[SoliDB](https://github.com/solisoft/solidb).

```text
   commits ──► group commit ──► one WAL write + one fsync per group
                    │
                    ▼
   memtable (skiplist, entries in per-thread arenas) ──seal──► immutable memtables
                                                                  │ flush
                                                                  ▼
   L0  ▢ ▢ ▢ ▢      overlapping files, newest first
   L1  ▢▢▢▢▢▢▢▢     512 MiB of sorted, non-overlapping files ─┐
   L2  ▢▢▢▢▢▢▢▢▢…   ×10 per level                             ├─ leveled compaction,
   …                                                          ┘  parallel subcompactions
```

> **Status — not used in production yet.** solistore passed its go/no-go
> gates against RocksDB 11.8.1 on 2026-10-14. SoliDB now runs on it behind
> a feature flag, where it is ahead of RocksDB on every phase measured
> (see [Benchmarks](#benchmarks)). The on-disk format may still change
> before 1.0. See [the integration plan](docs/solidb-integration.md).

## Why another storage engine

SoliDB used RocksDB, which has three costs:

- **Build time.** Its C++ was most of SoliDB's build: CI measured 95 minutes
  of compilation for 18 seconds of tests. It also needs a cross g++
  toolchain for musl and aarch64, and Windows was limited to a compile-only
  check.
- **Memory.** It is hard to account for and harder to bound, with
  per-column-family structures that grow with the number of collections.
- **Durability.** SoliDB's ordinary writes were not fsynced, because an
  fsync per write was too slow.

solistore answers each of these with a design choice:

- **Pure Rust, no `-sys` crate.** Nothing in the dependency graph needs a C
  compiler, and CI enforces it (`scripts/check_no_c_toolchain.sh`).
- **One memory budget.** A shared block cache holds data blocks. Index and
  filter memory is charged to the same cache.
- **Durable commits that stay fast.** Writers queue behind a leader, which
  writes the whole group to the WAL and fsyncs once for all of them.
- **Cheap column families.** All of them share one LSM. Creating or dropping
  one is a single MANIFEST record, and opening a database with thousands of
  them reads no table file.

## Features

- Column families created and dropped at run time.
- Atomic write batches across column families: readers see a batch entirely
  or not at all.
- `get` returns `Bytes`, so a block-cache hit is served without a copy.
  `multi_get` is also available.
- Bounded iterators in both directions, with RocksDB's `Start`, `End` and
  `From(key, direction)` modes; `From(k, Reverse)` is seek-for-prev.
- Range deletes:
  - Kept at the version level rather than in tables.
  - A file wholly covered by a delete is dropped without being read.
  - Files that straddle the deleted range are compacted first.
- Leveled compaction with parallel subcompactions and trivial moves.
  Writers are slowed and then stalled on level-0 backlog.
- Point-in-time checkpoints made of hard links.
- Metrics:
  - Memtable bytes, table-reader bytes, block-cache usage and files per level.
  - Approximate size of a key range.
  - Group-commit sizes, stall times and compaction write volume.
- Two durability modes:

| Mode | A commit returns when… | Process crash | Power loss |
|---|---|---|---|
| `Durability::Full` (default) | its group's fsync completed | loses nothing | loses nothing acknowledged |
| `Durability::Eventual { sync_interval }` | it reached the OS; a background thread fsyncs every interval | loses nothing | loses at most `sync_interval` of acknowledged writes, never a write without every later one |

In either mode, a write with `WriteOptions { sync: true }` waits for its own
fsync. An fsync failure poisons the database, which then refuses further
writes rather than retrying (see "fsyncgate").

## Quick start

```rust
use std::time::Duration;
use solistore::{Db, Direction, Durability, IteratorMode, Options, WriteBatch};

let db = Db::open(dir, Options {
    durability: Durability::Eventual { sync_interval: Duration::from_millis(100) },
    ..Options::default()
})?;

let users = db.create_cf("users")?;
let index = db.create_cf("users_by_email")?;

// Atomic across column families.
let mut batch = WriteBatch::new();
batch.put(&users, b"u:1", br#"{"name":"Ada","email":"ada@example.com"}"#);
batch.put(&index, b"ada@example.com", b"u:1");
db.write(batch)?;

let id = db.get(&index, b"ada@example.com")?;          // Option<Bytes>

for item in db.iterator(&users, IteratorMode::From(b"u:", Direction::Forward))? {
    let (key, value) = item?;
}

let mut batch = WriteBatch::new();
batch.delete_range(&users, b"u:", b"u;");               // [start, end)
db.write(batch)?;

db.checkpoint(dir.join("backup"))?;
```

The full program is
[`crates/solistore/examples/quickstart.rs`](crates/solistore/examples/quickstart.rs).
Run it with `cargo run -p solistore --example quickstart`.

## How it is tested

A storage engine is only as good as its crash behaviour, so most of the test
code exercises failure paths:

- **Model-based property tests.** These run random sequences against a
  `BTreeMap` model: puts, deletes, range deletes, batches, gets, bounded
  scans in both directions, flushes, compactions, reopens, column-family
  create and drop, and opening a checkpoint. Tiny memtables and files force
  every background path to run. Set `PROPTEST_CASES` to go deeper; 20,000
  cases found a WAL-recycling race that 256 did not.
- **A deterministic crash sweep.** A fault-injecting filesystem (`FaultFs`)
  crashes the database at *every* I/O operation of a workload and leaves
  torn, unsynced tails. After each crash, the reopened database must equal
  the model at the last acknowledged commit.
- **Fault injection.** `FaultFs` also makes fsyncs fail or run slow, to
  exercise poisoning and group commit.
- **Process kills.** The benchmark's `crash` test SIGKILLs a writer 100
  times mid-load and checks every acknowledged batch. Its `visibility` test
  checks that no reader ever sees part of a batch.
- **Mutation testing.** Bugs are injected on purpose to check that the tests
  above catch them.

## Benchmarks

All figures are against RocksDB 11.8.1 with SoliDB's production profile.
Ratios are solistore throughput over RocksDB's; above 1.0 is faster.
solistore runs in `Eventual` mode (100 ms), and RocksDB with SoliDB's
`sync=false` writes, unless stated otherwise.

### Inside SoliDB, whole server

The same SoliDB binary ran on each engine, with 1M documents and two
indexes. The load came over SoliDB's native driver with the query cache
off; the server had 8 cores, the client 8 others. Figures are medians of
3 runs.

| Phase | 64 connections | 256 connections |
|---|---:|---:|
| insert, one document per request | 1.11× | 1.07× |
| get by key | 1.04× | 1.08× |
| update (read, merge, write) | 1.07× | 1.14× |
| query on an index (equality) | 1.05× | 1.10× |
| query on an index (range) | 1.05× | 1.10× |
| mixed, 90% reads | 0.99× | 1.00× |

SDBQL queries, 64 connections (8 for the scan and aggregate):

| Query | Ratio |
|---|---:|
| `FILTER d._key == @k` | 1.02× |
| index equality, `LIMIT 10` | 1.08× |
| `COLLECT WITH COUNT` over ~1,000 documents | 1.06× |
| projection of 1,000 rows | 1.04× |
| `SORT` on an index, `LIMIT 10` | 1.05× |
| join (correlated subquery) | 1.08× |
| 100 raw rows over the driver | 1.04× |
| `UPDATE` | 1.04× |
| `INSERT` | 1.16× |
| filter on an unindexed field (scan) | 1.12× |
| `COLLECT` over 100,000 documents | 1.12× |

solistore also uses less CPU per operation, for example 27 µs per update
against 32 µs. Peak RSS is 1.02–1.08× RocksDB's. With 256 connections,
RocksDB's p99 update latency is 4.4 ms and solistore's 3.0 ms.

### Storage engines alone

`bench/` runs a SoliDB-shaped workload on each engine directly: a write is
the document plus three index entries, and the phases are gets, scans,
updates, fsynced transactions and a ~1,232-collection catalog.

At 5M documents, medians of 3, the go/no-go series of 2026-10-14
(`bench/results/2026-10-14-m7`):

| Phase | Eventual vs RocksDB |
|---|---:|
| bulk load | 0.81× |
| updates | 0.82× |
| mixed read/write | 1.09× |
| get, uniform / hot / missing | 1.03× / 1.23× / 1.65× |
| scans | 1.34–1.43× |
| fsynced commits, 1 / 8 threads | 1.21× / 1.24× |

In full durability mode, solistore fsyncs *every* commit. It still runs
2.9× (1 thread) and 1.4× (8 threads) RocksDB's fsynced-commit throughput.

The gates that block integration all passed:

- 100 SIGKILL crashes with 14.87M acknowledged writes: 0 lost, 0 torn
  batches.
- 0 visibility violations in 42M checks.
- Reopen under 0.03 s.
- 260 MB reclaimed after a drop.
- RSS 0.95× RocksDB's, disk 1.09×.

The engine-only bench predates the commit-pipeline and memtable-sizing
work that put solistore ahead inside SoliDB; it has not been rerun at 5M
since.

### Against SurrealKV

SurrealKV 1.0.0 is in the same bench, with the budget the other engines
get: 512 MiB block cache, 64 MiB memtable, 4 KiB blocks and Snappy. At 1M
documents (single runs, `bench/results/2026-10-14-surrealkv`):

| Phase | RocksDB | solistore | SurrealKV |
|---|---:|---:|---:|
| load | 434k | 398k | 335k |
| fsynced commits, 8 threads | 63.5k | 214k | 68k |
| get, uniform | 3.64M | 3.23M | 2.57M |
| scan, 100 keys | 59k | 82k | 40k |
| update | 651k | 484k | 376k |
| mixed | 290k | 346k | 180k |
| peak RSS | 948 MB | 973 MB | 1,176 MB |

With every commit fsynced, solistore writes about twice as fast as
SurrealKV (load 170k against 86k, updates 174k against 90k).

With its factory settings (32 MiB cache, no compression), SurrealKV's
uniform gets fall to 307k a second. Its published benchmark used 500,000
keys, which fit in its memtable and cache.

Raw results are in `bench/results/`, as JSON lines per phase.

## Building

```bash
cargo build --release
cargo test --profile ci          # optimised, no LTO: crash and property tests run in seconds
cargo clippy --all-targets -- -D warnings
```

The benchmark is a separate workspace, so RocksDB's C++ never enters the
engine's build:

```bash
cargo build --release --manifest-path bench/Cargo.toml        # both engines
bench/target/release/engine-bench run --engine solistore --dir /fast/disk/bench
bench/gate.sh                                                  # the full go/no-go run
```

MSRV is Rust 1.91, SoliDB's floor. CI checks Linux, macOS, Windows and
musl.

## Repository layout

```text
crates/solistore/src/
  db.rs           open, commit pipeline, reads, column families, checkpoints
  batch.rs        WriteBatch and its WAL encoding
  log.rs          record framing shared by the WAL and the MANIFEST
  memtable.rs     skiplist memtable, entries in per-thread arenas
  sst/            table format: blocks, bloom filters, block cache, reader/builder
  version.rs      versions, super-versions (arc-swap), level iterators
  manifest.rs     version edits and recovery
  flush.rs        memtable → level 0
  compaction.rs   pickers, subcompactions, trivial moves
  iter.rs         merging iterator and the user-facing iterator
  fs.rs           filesystem trait: StdFs, and FaultFs for crash tests
bench/            SoliDB-shaped benchmark against RocksDB
docs/             integration plan for SoliDB
```

## Roadmap

- [x] M0–M7: WAL and group commit, tables, compaction, range tombstones,
  block cache, checkpoints and metrics, then the go/no-go benchmark.
- [x] SoliDB on solistore behind a feature, with the engine recorded per
  data directory. Ahead of RocksDB on every measured phase.
- [ ] A migration tool from RocksDB data directories (in progress).
- [ ] A trial on real workloads, then removing RocksDB from SoliDB.
- [ ] Dynamic level sizing, a fully arena-allocated skiplist, `loom` on the
  commit path, Miri on the `unsafe`, decoder fuzzing, and crash tests on
  NTFS.

## Licence

[O'Saasy License](LICENCE.md) © 2024-2026 SoliSoft SARL, the same as
SoliDB. It is MIT-style, except that you may not offer the software itself
as a competing hosted service.
