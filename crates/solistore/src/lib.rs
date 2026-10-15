//! solistore — an embedded, ordered key-value store built as an LSM tree.
//!
//! It exists to replace RocksDB under SoliDB, so its surface is what SoliDB
//! uses of RocksDB and no more: column families created and dropped at run
//! time, atomic batches across them, range deletes, bounded iterators in both
//! directions, checkpoints and a handful of size metrics.
//!
//! Every commit is fsynced before it is acknowledged unless the database is
//! opened in eventual mode; concurrent commits share one fsync.

mod batch;
mod coding;
mod compaction;
mod db;
mod error;
mod flush;
pub mod fs;
mod iter;
mod key;
mod log;
mod manifest;
mod memtable;
mod sst;
mod version;

pub use batch::WriteBatch;
pub use bytes::Bytes;
pub use db::{
    ColumnFamily, Db, Durability, IngestReport, Options, RecoveryReport, Stats, WriteOptions,
    MAX_GROUP_BYTES,
};
pub use error::{Error, Result};
pub use iter::{DbIterator, Direction, IteratorMode, ReadOptions};
