# Archived results

Only `2026-10-02-rocksdb/` was copied here, as the baseline. The fjall
directories below stay on SoliDB's `feat/storage-engine-bench` branch.

fjall was evaluated as a RocksDB replacement on 2026-10-02 and abandoned. The
fjall engines, the vendored and patched fjall, and the report generator have
been removed from engine-bench; these JSON lines are kept as the record.

- `2026-10-02/` — first gate run: RocksDB vs fjall 3.1.10. The per-collection
  layout deadlocked. See its RESULTS.md.
- `2026-10-02-final/` — RocksDB vs fjall 3.1.11 (stock, tuned, five local
  patches, and shared keyspace), 5M documents, 3 repetitions, with crash and
  visibility tests.
  - Even patched, fjall needed 3–4.2× the memory and 1.7–2.7× the disk.
  - It ran 26–51% of RocksDB's fsync'd commits and 33% of its missing-key
    gets.
  - Stock fjall hung on reopen, and showed half-visible batches even
    through snapshot reads.
  - Rendered report: https://claude.ai/artifact/6DHGyDmKUe6FwMzNpVF6hr

- `2026-10-02-rocksdb/` — RocksDB 10.10.1 vs 11.8.1 on SoliDB's prod profile,
  and memory settings, 7 configurations × 3 reps at 5M documents, plus 4
  runs at 30M.
  - 11.8.1 is level with 10.10.1 at 5M. At 30M (one run each), updates are
    +39%, missing-key gets +21% and scans +15%.
  - Index and filter blocks in the cache, partitioned index/filters and a
    WriteBufferManager charged to the cache cost 10–60% of read throughput,
    and did not lower memory.
  - SoliDB moved to 11.8.1 with its settings unchanged (branch
    `feat/rocksdb-11`).
