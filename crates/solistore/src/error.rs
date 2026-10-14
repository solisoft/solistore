use std::fmt;
use std::io;
use std::sync::Arc;

/// Everything an operation can fail with.
///
/// `Clone` because one commit group shares one outcome: the leader's fsync
/// either covered every batch in the group or none of them.
#[derive(Debug, Clone)]
pub enum Error {
    Io(Arc<io::Error>),
    /// Bytes on disk that cannot be what this crate wrote.
    Corruption(String),
    /// A write or fsync failed earlier. Whether the data reached the disk is
    /// unknown, and retrying the fsync would report success for pages the
    /// kernel already dropped (PostgreSQL's "fsyncgate"), so the database
    /// refuses writes until it is reopened and has recovered from its log.
    Poisoned,
    ColumnFamilyNotFound(String),
    ColumnFamilyExists(String),
    /// The handle names a column family that has since been dropped.
    ColumnFamilyDropped(String),
    InvalidArgument(String),
}

pub type Result<T> = std::result::Result<T, Error>;

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(e) => write!(f, "I/O error: {e}"),
            Error::Corruption(what) => write!(f, "corruption: {what}"),
            Error::Poisoned => write!(f, "a previous write or fsync failed; reopen the database"),
            Error::ColumnFamilyNotFound(name) => write!(f, "column family not found: {name}"),
            Error::ColumnFamilyExists(name) => write!(f, "column family already exists: {name}"),
            Error::ColumnFamilyDropped(name) => write!(f, "column family was dropped: {name}"),
            Error::InvalidArgument(what) => write!(f, "invalid argument: {what}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(e) => Some(e.as_ref()),
            _ => None,
        }
    }
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Error::Io(Arc::new(e))
    }
}
