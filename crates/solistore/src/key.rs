//! Internal keys: a user key qualified by its column family and sequence.
//!
//! ```text
//! internal key := cf u32 BE | user key | trailer u64 LE,  trailer = seq << 8 | kind
//! ```
//!
//! The same encoding is used in the memtable and in table files. It does not
//! sort bytewise — a user key that prefixes another would sort wrong against
//! its trailer — so everything compares with [`compare`]: column family, then
//! user key, then sequence *descending*. Versions of one key therefore sort
//! newest first, and the first entry at or after `(cf, key, snapshot)` is the
//! version a reader at `snapshot` sees.

use std::borrow::Borrow;
use std::cmp::Ordering;

use bytes::Bytes;

/// Sequence numbers are 56 bits (the trailer keeps 8 for the kind). They
/// start at 1, so 0 sorts after every real version of a key.
pub const MAX_SEQ: u64 = (1 << 56) - 1;
pub const TRAILER_LEN: usize = 8;
const CF_LEN: usize = 4;
pub const MIN_LEN: usize = CF_LEN + TRAILER_LEN;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum Kind {
    Delete = 0,
    Put = 1,
}

impl Kind {
    pub fn from_u8(b: u8) -> Option<Kind> {
        match b {
            0 => Some(Kind::Delete),
            1 => Some(Kind::Put),
            _ => None,
        }
    }
}

/// The 8 bytes that end an internal key.
pub fn trailer_bytes(seq: u64, kind: Kind) -> [u8; TRAILER_LEN] {
    debug_assert!(seq <= MAX_SEQ);
    ((seq << 8) | kind as u64).to_le_bytes()
}

pub fn encode_into(buf: &mut Vec<u8>, cf: u32, user_key: &[u8], seq: u64, kind: Kind) {
    debug_assert!(seq <= MAX_SEQ);
    buf.reserve(MIN_LEN + user_key.len());
    buf.extend_from_slice(&cf.to_be_bytes());
    buf.extend_from_slice(user_key);
    buf.extend_from_slice(&((seq << 8) | kind as u64).to_le_bytes());
}

pub fn encode(cf: u32, user_key: &[u8], seq: u64, kind: Kind) -> Vec<u8> {
    let mut buf = Vec::new();
    encode_into(&mut buf, cf, user_key, seq, kind);
    buf
}

/// Sorts just before the newest version of `user_key` visible at `snapshot`.
pub fn at_snapshot(cf: u32, user_key: &[u8], snapshot: u64) -> Vec<u8> {
    encode(cf, user_key, snapshot.min(MAX_SEQ), Kind::Put)
}

/// Sorts before every version of `user_key`.
pub fn first_of(cf: u32, user_key: &[u8]) -> Vec<u8> {
    encode(cf, user_key, MAX_SEQ, Kind::Put)
}

/// Sorts before every version of the key whose `cf ‖ user key` is `id`.
pub fn first_of_id(id: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(id.len() + TRAILER_LEN);
    key.extend_from_slice(id);
    key.extend_from_slice(&((MAX_SEQ << 8) | Kind::Put as u64).to_le_bytes());
    key
}

/// Sorts after every version of `user_key`.
pub fn last_of(cf: u32, user_key: &[u8]) -> Vec<u8> {
    encode(cf, user_key, 0, Kind::Delete)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Parsed<'a> {
    pub cf: u32,
    pub user_key: &'a [u8],
    pub seq: u64,
    pub kind: Kind,
}

/// Split an internal key. `None` for bytes that cannot be one.
pub fn parse(key: &[u8]) -> Option<Parsed<'_>> {
    if key.len() < MIN_LEN {
        return None;
    }
    let (head, trailer) = key.split_at(key.len() - TRAILER_LEN);
    let trailer = u64::from_le_bytes(trailer.try_into().ok()?);
    Some(Parsed {
        cf: u32::from_be_bytes(head[..CF_LEN].try_into().ok()?),
        user_key: &head[CF_LEN..],
        seq: trailer >> 8,
        kind: Kind::from_u8(trailer as u8)?,
    })
}

/// The column family and user key, without the trailer: what identifies a
/// key across its versions, and what the bloom filter hashes.
pub fn cf_and_user_key(key: &[u8]) -> &[u8] {
    &key[..key.len() - TRAILER_LEN]
}

fn trailer(key: &[u8]) -> u64 {
    u64::from_le_bytes(key[key.len() - TRAILER_LEN..].try_into().expect("8 bytes"))
}

pub fn compare(a: &[u8], b: &[u8]) -> Ordering {
    // The cf prefix is big-endian, so it sorts bytewise with the user key.
    cf_and_user_key(a)
        .cmp(cf_and_user_key(b))
        .then_with(|| trailer(b).cmp(&trailer(a)))
}

/// A borrowed internal key that orders with [`compare`], so a sorted map of
/// [`MemKey`] can be searched with a plain slice.
#[derive(Debug, PartialEq, Eq)]
#[repr(transparent)]
pub struct KeyRef([u8]);

impl KeyRef {
    pub fn new(key: &[u8]) -> &KeyRef {
        // SAFETY: KeyRef is a repr(transparent) wrapper around [u8], so the
        // two have the same layout and the cast preserves the lifetime.
        unsafe { &*(key as *const [u8] as *const KeyRef) }
    }
}

impl Ord for KeyRef {
    fn cmp(&self, other: &Self) -> Ordering {
        compare(&self.0, &other.0)
    }
}

impl PartialOrd for KeyRef {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// An owned internal key, as the memtable stores it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemKey(pub Bytes);

impl Borrow<KeyRef> for MemKey {
    fn borrow(&self) -> &KeyRef {
        KeyRef::new(&self.0)
    }
}

impl Ord for MemKey {
    fn cmp(&self, other: &Self) -> Ordering {
        compare(&self.0, &other.0)
    }
}

impl PartialOrd for MemKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ik(cf: u32, key: &[u8], seq: u64) -> Vec<u8> {
        encode(cf, key, seq, Kind::Put)
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
        keys.sort_by(|a, b| compare(a, b));
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
        // Comparing the encodings bytewise would get this wrong.
        assert_eq!(
            compare(&last_of(0, b"a"), &first_of(0, b"ab")),
            Ordering::Less
        );
        assert_eq!(
            compare(&ik(0, b"a", 1), &ik(0, b"\x00", 1)),
            Ordering::Greater
        );
    }

    #[test]
    fn bounds_bracket_every_version() {
        for seq in [1, 7, MAX_SEQ - 1, MAX_SEQ] {
            for kind in [Kind::Put, Kind::Delete] {
                let v = encode(0, b"k", seq, kind);
                assert_ne!(compare(&first_of(0, b"k"), &v), Ordering::Greater);
                assert_eq!(compare(&v, &last_of(0, b"k")), Ordering::Less);
            }
        }
    }

    #[test]
    fn parse_inverts_encode() {
        let key = encode(7, b"user", 42, Kind::Delete);
        assert_eq!(
            parse(&key),
            Some(Parsed {
                cf: 7,
                user_key: b"user",
                seq: 42,
                kind: Kind::Delete
            })
        );
        assert_eq!(parse(b"short"), None);
    }
}
