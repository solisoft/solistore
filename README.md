# solistore

An embedded, ordered key-value store built as an LSM tree, in pure Rust. It is
being written to replace RocksDB under [SoliDB](../db), and is not used there
yet: it goes in only if it beats the benchmark's go/no-go thresholds.

Why: RocksDB's C++ is most of SoliDB's build time (CI measured 95 minutes of
compilation against 18 seconds of tests), its memory is hard to account for,
and SoliDB's ordinary writes are not fsynced. solistore fsyncs every commit
by default and makes concurrent commits share one fsync.

## Layout

- `crates/solistore` — the engine.
- `bench/` — the SoliDB-shaped benchmark (its own workspace, so RocksDB never
  enters the engine's build). `results/` holds the RocksDB 11.8.1 baseline.
- `scripts/check_no_c_toolchain.sh` — fails if anything in the engine's
  dependency graph needs a C compiler.

## Building

On a workstation with `rbuild`:

```bash
rbuild check store                 # fmt + clippy
rbuild test store --profile ci     # tests
```

Elsewhere, the plain `cargo` equivalents. The benchmark:

```bash
cargo build --release --manifest-path bench/Cargo.toml                       # both engines
cargo build --release --manifest-path bench/Cargo.toml --no-default-features --features solistore
bench/target/release/engine-bench run --engine rocks --dir /fast/disk/bench
```
