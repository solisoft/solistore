//! solistore — an embedded, ordered key-value store built as an LSM tree.
//!
//! It exists to replace RocksDB under SoliDB, so its surface is what SoliDB
//! uses of RocksDB and no more: column families created and dropped at run
//! time, atomic batches across them, range deletes, bounded iterators in both
//! directions, checkpoints and a handful of size metrics.
//!
//! Every commit is fsynced before it is acknowledged unless the database is
//! opened in eventual mode; concurrent commits share one fsync.
