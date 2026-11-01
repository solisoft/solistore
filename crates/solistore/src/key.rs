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

use std::cmp::Ordering;

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

/// A short internal key `s` with `last <= s < next`, for the index entry
/// between two table blocks; `None` when none is shorter than `last`.
///
/// Built on the `cf ‖ user key` of `last`, as LevelDB's
/// `FindShortestSeparator` with RocksDB's extension: keep the prefix shared
/// with `next`, then bump the first byte that can be bumped and drop the
/// rest. The result is the first version of that shorter key, so it sorts
/// after every version of `last`'s key and before every version of `next`'s.
///
/// It is derived from `last` rather than from `next` on purpose: a prefix of
/// `next` would also separate the blocks, but a seek to exactly that prefix —
/// the commonest seek there is — would then land on the earlier block and
/// read it for nothing. Any separator leaves such a gap: a seek to a key
/// after `last` and at or before the separator still reads the earlier
/// block, but no key of the table starts with the separator's own prefix.
///
/// It never cuts inside the column family, so the result is always a whole
/// internal key that [`parse`] accepts.
pub fn separator(last: &[u8], next: &[u8]) -> Option<Vec<u8>> {
    let (a, b) = (cf_and_user_key(last), cf_and_user_key(next));
    let p = a.iter().zip(b).take_while(|(x, y)| x == y).count();
    // Same key on both sides (its versions span the blocks), or `a` a
    // prefix of `b`: no key strictly between them is shorter than `a`.
    if p >= a.len() {
        return None;
    }
    // Out of order: `next` is not above `last`. Never so from a table
    // builder, but this indexes `b[p]`, so it is checked in release too.
    if p >= b.len() || a[p] > b[p] {
        return None;
    }
    let mut cut = if a[p].checked_add(1).is_some_and(|c| c < b[p]) {
        // a[..p] ‖ a[p]+1 sorts after `a` and before `b`.
        p
    } else {
        // a[p]+1 == b[p]: keep a[..=p], which is below `b`, and bump a later
        // byte of `a` to get above it.
        (p + 1..a.len()).find(|&i| a[i] != 0xff)?
    };
    // Column families that differ in a high byte: bump within the last cf
    // byte or the user key instead. Any byte after `p` keeps `a[..=p]`, so
    // the result stays below `b`.
    if cut < CF_LEN - 1 {
        cut = (CF_LEN - 1..a.len()).find(|&i| a[i] != 0xff)?;
    }
    // Bumping the last byte keeps the length: nothing gained.
    if cut + 1 >= a.len() {
        return None;
    }
    let mut id = a[..=cut].to_vec();
    id[cut] += 1;
    Some(first_of_id(&id))
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
///
/// Total over any bytes: a key shorter than a trailer, which only a corrupt
/// block or MANIFEST holds, is all identity and no trailer. [`compare`]
/// stays a total order on such keys, and [`parse`] reports them, instead of
/// a slice out of bounds in the middle of a seek (found by fuzzing).
pub fn cf_and_user_key(key: &[u8]) -> &[u8] {
    &key[..key.len().saturating_sub(TRAILER_LEN)]
}

fn trailer(key: &[u8]) -> u64 {
    key.len().checked_sub(TRAILER_LEN).map_or(0, |at| {
        u64::from_le_bytes(key[at..].try_into().expect("8 bytes"))
    })
}

pub fn compare(a: &[u8], b: &[u8]) -> Ordering {
    // The cf prefix is big-endian, so it sorts bytewise with the user key.
    cf_and_user_key(a)
        .cmp(cf_and_user_key(b))
        .then_with(|| trailer(b).cmp(&trailer(a)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

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

    fn seq_and_kind() -> impl Strategy<Value = (u64, Kind)> {
        (
            0..=MAX_SEQ,
            prop_oneof![Just(Kind::Put), Just(Kind::Delete)],
        )
    }

    /// Keys from a small alphabet, so pairs share prefixes and hit the 0xff
    /// and adjacent-byte cases.
    fn id() -> impl Strategy<Value = Vec<u8>> {
        prop::collection::vec(
            prop::sample::select(vec![0u8, 1, 2, b'a', b'b', 0xfe, 0xff]),
            0..12,
        )
    }

    /// Column families that differ in their last byte and in higher ones.
    fn cf() -> impl Strategy<Value = u32> {
        prop::sample::select(vec![
            0u32,
            1,
            2,
            0xff,
            0x100,
            0x200,
            0x1_0000,
            0x0200_0000,
            u32::MAX,
        ])
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(4096))]

        #[test]
        fn a_separator_is_shorter_and_sits_between_the_keys(
            cf_a in cf(), a in id(), (seq_a, kind_a) in seq_and_kind(),
            cf_b in cf(), b in id(), (seq_b, kind_b) in seq_and_kind(),
        ) {
            let (x, y) = (encode(cf_a, &a, seq_a, kind_a), encode(cf_b, &b, seq_b, kind_b));
            let (last, next) = match compare(&x, &y) {
                Ordering::Less => (x, y),
                Ordering::Greater => (y, x),
                Ordering::Equal => return Ok(()),
            };
            if let Some(s) = separator(&last, &next) {
                prop_assert!(s.len() < last.len());
                prop_assert!(parse(&s).is_some(), "not an internal key: {:?}", s);
                prop_assert_eq!(compare(&last, &s), Ordering::Less);
                prop_assert_eq!(compare(&s, &next), Ordering::Less);
                // Not just these two versions: every version of either key.
                let (l, n) = (cf_and_user_key(&last), cf_and_user_key(&next));
                prop_assert_eq!(compare(&first_of_id(l), &s), Ordering::Less);
                let mut last_of_l = l.to_vec();
                last_of_l.extend_from_slice(&trailer_bytes(0, Kind::Delete));
                prop_assert_eq!(compare(&last_of_l, &s), Ordering::Less);
                prop_assert_eq!(compare(&s, &first_of_id(n)), Ordering::Less);
                // And as short as such a key can be.
                prop_assert_eq!(cf_and_user_key(&s).len(), shortest_between(l, n));
            } else {
                // Only when no shorter key exists: the same key on both
                // sides, one a prefix of the other, or a bump that saves
                // nothing.
                let (l, n) = (cf_and_user_key(&last), cf_and_user_key(&next));
                prop_assert!(l == n || n.starts_with(l) || shortest_between(l, n) >= l.len());
            }
        }
    }

    /// Length of the shortest byte string strictly between `a` and `b`
    /// (bytewise) that is not a prefix of `b` and keeps a whole column
    /// family, by brute force over the prefixes of `a` bumped by one: any
    /// shorter key above `a` is at least one of those.
    fn shortest_between(a: &[u8], b: &[u8]) -> usize {
        (0..a.len())
            .filter(|&i| a[i] != 0xff)
            .map(|i| {
                let mut s = a[..=i].to_vec();
                s[i] += 1;
                s
            })
            .filter(|s| s.len() >= CF_LEN)
            .filter(|s| s.as_slice() > a && s.as_slice() < b && !b.starts_with(s))
            .map(|s| s.len())
            .min()
            .unwrap_or(usize::MAX)
    }

    #[test]
    fn separators_on_typical_keys() {
        let k = |key: &[u8]| encode(0, key, 5, Kind::Put);
        // A gap of more than one at the first difference: cut right there.
        assert_eq!(
            separator(&k(b"apple"), &k(b"cherry")),
            Some(first_of_id(&[0, 0, 0, 0, b'b']))
        );
        // Adjacent bytes: keep the shared byte and bump the next one.
        assert_eq!(
            separator(&k(b"user:0199zz"), &k(b"user:1000")),
            Some(first_of_id(b"\0\0\0\0user:02"))
        );
        // The same key spanning two blocks keeps its full key.
        assert_eq!(
            separator(&k(b"hot"), &encode(0, b"hot", 4, Kind::Put)),
            None
        );
        // A key that prefixes the next one cannot be shortened.
        assert_eq!(separator(&k(b"user"), &k(b"user:1")), None);
        // Consecutive numbers differ in their last byte: nothing to gain.
        assert_eq!(separator(&k(b"doc:00000123"), &k(b"doc:00000124")), None);
        // Across column families.
        assert_eq!(
            separator(
                &encode(0, b"zzz", 1, Kind::Put),
                &encode(2, b"a", 1, Kind::Put)
            ),
            Some(first_of_id(&[0, 0, 0, 1]))
        );
        // Column families that differ in a high byte: the cut moves down to
        // the last cf byte, so the result is still a whole internal key.
        let s = separator(
            &encode(0, b"x", 1, Kind::Put),
            &encode(0x200, b"a", 1, Kind::Put),
        );
        assert_eq!(s, Some(first_of_id(&[0, 0, 0, 1])));
        assert!(parse(&s.unwrap()).is_some());
        // Out of order: no separator, and no panic.
        assert_eq!(separator(&k(b"ab"), &k(b"a")), None);
        assert_eq!(separator(&k(b"b"), &k(b"a")), None);
    }

    /// Found by fuzzing: keys shorter than a trailer, which only corrupt
    /// data holds, made `compare` slice out of bounds. They compare as all
    /// identity, in a total order, and do not parse.
    #[test]
    fn keys_shorter_than_a_trailer_compare_and_do_not_parse() {
        let mut keys: Vec<Vec<u8>> = vec![
            vec![],
            b"a".to_vec(),
            b"abcdefg".to_vec(),
            ik(0, b"a", 1),
            vec![0xff; 3],
        ];
        keys.sort_by(|a, b| compare(a, b));
        for w in keys.windows(2) {
            assert_ne!(compare(&w[0], &w[1]), Ordering::Greater);
            assert_eq!(compare(&w[0], &w[1]), compare(&w[1], &w[0]).reverse());
        }
        assert_eq!(compare(b"abc", b"abc"), Ordering::Equal);
        assert!(keys
            .iter()
            .filter(|k| k.len() < MIN_LEN)
            .all(|k| parse(k).is_none()));
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
