# Fuzz targets

`cargo-fuzz` targets for solistore's decoders. They call
`solistore::fuzzing`, built with the crate's `fuzzing` feature. This is its
own workspace: libFuzzer is C++, and the engine's build must not see it.

| Target | Decodes |
|---|---|
| `log_records` | WAL and MANIFEST framing: records, header, end record, torn tails |
| `write_batch` | a WAL record's payload |
| `version_edits` | a MANIFEST record's payload, applied to a new state |
| `block` | a table block: iteration both ways and seeks |
| `bloom` | a bloom filter, probed |
| `table` | a whole table file: footer, index, filter, blocks |
| `open` | a database whose MANIFEST and WAL hold well-framed records saying anything |

Checksums guard every record and block, and a fuzzer does not forge
CRC32C, so `open` frames the fuzzed payloads itself: the decoders behind
the checksums see arbitrary content. A target fails on a panic, and on an
allocation above `-malloc_limit_mb`.

```sh
cargo install cargo-fuzz
cd fuzz
cargo +nightly fuzz run -O open -- -max_total_time=600 -malloc_limit_mb=256
```

First run, 2026-11-01, 5 then 10 minutes per target on the build server:
two panics, both fixed (a key shorter than its trailer in a corrupt block;
a MANIFEST without the default column family), then 3.7M to 220M runs per
target without a failure.
