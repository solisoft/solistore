//! Internal keys: a user key qualified by its column family and sequence.
//!
//! Every column family lives in one LSM, so the column family id leads the
//! order. Versions of one user key sort newest first, so the first entry at
//! or after `(cf, key, snapshot)` is the version a reader at `snapshot` sees.

use std::cmp::Ordering;

use bytes::Bytes;

/// Sequence numbers start at 1; 0 sorts after every real version of a key.
pub const MAX_SEQ: u64 = u64::MAX;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum Kind {
    Delete = 0,
    Put = 1,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InternalKey {
    pub cf: u32,
    pub user_key: Bytes,
    pub seq: u64,
    pub kind: Kind,
}

impl InternalKey {
    pub fn new(cf: u32, user_key: Bytes, seq: u64, kind: Kind) -> Self {
        InternalKey {
            cf,
            user_key,
            seq,
            kind,
        }
    }

    /// Sorts before every version of `user_key`.
    pub fn first_of(cf: u32, user_key: Bytes) -> Self {
        Self::new(cf, user_key, MAX_SEQ, Kind::Put)
    }

    /// Sorts after every version of `user_key`.
    pub fn last_of(cf: u32, user_key: Bytes) -> Self {
        Self::new(cf, user_key, 0, Kind::Delete)
    }

    /// Sorts just before the newest version of `user_key` visible at `snapshot`.
    pub fn at_snapshot(cf: u32, user_key: Bytes, snapshot: u64) -> Self {
        Self::new(cf, user_key, snapshot, Kind::Put)
    }
}

impl Ord for InternalKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.cf
            .cmp(&other.cf)
            .then_with(|| self.user_key.cmp(&other.user_key))
            .then_with(|| other.seq.cmp(&self.seq))
            .then_with(|| other.kind.cmp(&self.kind))
    }
}

impl PartialOrd for InternalKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ik(cf: u32, key: &'static [u8], seq: u64) -> InternalKey {
        InternalKey::new(cf, Bytes::from_static(key), seq, Kind::Put)
    }

    #[test]
    fn order_is_cf_then_key_then_newest_first() {
        let mut keys = vec![
            ik(1, b"a", 1),
            ik(0, b"b", 1),
            ik(0, b"a", 1),
            ik(0, b"a", 9),
            ik(0, b"ab", 5),
        ];
        keys.sort();
        assert_eq!(
            keys,
            vec![
                ik(0, b"a", 9),
                ik(0, b"a", 1),
                ik(0, b"ab", 5),
                ik(0, b"b", 1),
                ik(1, b"a", 1)
            ]
        );
    }

    #[test]
    fn a_key_that_prefixes_another_sorts_before_all_its_versions() {
        // Encoding the sequence into the key bytes would get this wrong.
        assert!(InternalKey::last_of(0, Bytes::from_static(b"a")) < ik(0, b"ab", MAX_SEQ));
    }

    #[test]
    fn bounds_bracket_every_version() {
        let key = Bytes::from_static(b"k");
        for seq in [1, 7, MAX_SEQ - 1] {
            for kind in [Kind::Put, Kind::Delete] {
                let v = InternalKey::new(0, key.clone(), seq, kind);
                assert!(InternalKey::first_of(0, key.clone()) <= v);
                assert!(v < InternalKey::last_of(0, key.clone()));
            }
        }
    }
}
