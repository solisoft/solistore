//! An index over range tombstones, so a read does not test every one.
//!
//! Every read asks "the highest sequence number, at most the snapshot, of a
//! tombstone covering this key". Answered by walking the list, that is one
//! comparison per tombstone for every point read and every iterator step,
//! and a client that range-deletes often (SoliDB truncates a collection with
//! twelve of them, and test runners truncate every collection at each run)
//! made `covering_seq` the hottest function of a dev server: 101k tombstones
//! went through it in twenty minutes.
//!
//! Such a workload repeats the same few ranges with ever newer sequence
//! numbers, so tombstones are grouped by exact range first ([`Group`]: one
//! range, its sequence numbers sorted). The distinct ranges are then cut at
//! every start and end into non-overlapping fragments, each listing the
//! groups that cover all of it. A lookup is one binary search for the
//! fragment, then one per covering group for the newest sequence number at
//! or below the snapshot. A wide range over many narrow ones (a dropped
//! database over its collections) is listed in every fragment it spans
//! rather than making the lookup walk past it.
//!
//! The index is immutable; the owner rebuilds it when its tombstones change.

use bytes::Bytes;

use crate::memtable::RangeTombstone;

/// Every tombstone of one exact `[start, end)` of one column family.
#[derive(Debug, Clone)]
pub struct Group {
    pub cf: u32,
    pub start: Bytes,
    pub end: Bytes,
    /// Ascending, never empty.
    pub seqs: Vec<u64>,
}

impl Group {
    pub fn max_seq(&self) -> u64 {
        *self
            .seqs
            .last()
            .expect("a group holds at least one tombstone")
    }

    /// The group's newest tombstone: the one that covers most, for a test
    /// that only gets easier to pass as the sequence number grows.
    pub fn newest(&self) -> RangeTombstone {
        RangeTombstone {
            cf: self.cf,
            start: self.start.clone(),
            end: self.end.clone(),
            seq: self.max_seq(),
        }
    }

    /// The newest sequence number at most `snapshot` (0 if none).
    fn seq_at(&self, snapshot: u64) -> u64 {
        match self.seqs.partition_point(|&s| s <= snapshot) {
            0 => 0,
            n => self.seqs[n - 1],
        }
    }
}

/// A piece of key space no range boundary falls inside.
#[derive(Debug)]
struct Fragment {
    cf: u32,
    start: Bytes,
    end: Bytes,
    /// Indexes into `TombstoneIndex::groups` of the groups covering it all.
    groups: Vec<u32>,
}

#[derive(Debug, Default)]
pub struct TombstoneIndex {
    /// Sorted by `(cf, start, end)`.
    groups: Vec<Group>,
    /// Sorted by `(cf, start)`, disjoint.
    fragments: Vec<Fragment>,
    tombstones: usize,
}

impl TombstoneIndex {
    pub fn build<'a>(tombstones: impl IntoIterator<Item = &'a RangeTombstone>) -> Self {
        // An empty range covers nothing: leave it out.
        let mut all: Vec<&RangeTombstone> =
            tombstones.into_iter().filter(|t| t.start < t.end).collect();
        let count = all.len();
        all.sort_unstable_by(|a, b| {
            (a.cf, &a.start, &a.end, a.seq).cmp(&(b.cf, &b.start, &b.end, b.seq))
        });

        let mut groups: Vec<Group> = Vec::new();
        for t in all {
            match groups.last_mut() {
                Some(g) if g.cf == t.cf && g.start == t.start && g.end == t.end => {
                    g.seqs.push(t.seq)
                }
                _ => groups.push(Group {
                    cf: t.cf,
                    start: t.start.clone(),
                    end: t.end.clone(),
                    seqs: vec![t.seq],
                }),
            }
        }

        let mut fragments = Vec::new();
        let mut at = 0;
        while at < groups.len() {
            let cf = groups[at].cf;
            let len = groups[at..].iter().take_while(|g| g.cf == cf).count();
            fragment_cf(&groups, at, at + len, &mut fragments);
            at += len;
        }

        TombstoneIndex {
            groups,
            fragments,
            tombstones: count,
        }
    }

    /// Tombstones indexed (empty ranges excluded).
    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.tombstones
    }

    pub fn is_empty(&self) -> bool {
        self.tombstones == 0
    }

    /// The distinct ranges, sorted by `(cf, start, end)`.
    pub fn groups(&self) -> &[Group] {
        &self.groups
    }

    /// The highest sequence number, at most `snapshot`, of a tombstone
    /// covering `key` (0 if none). Same answer as
    /// [`crate::memtable::covering_seq`] over the tombstones indexed.
    pub fn covering_seq(&self, cf: u32, key: &[u8], snapshot: u64) -> u64 {
        let at = self
            .fragments
            .partition_point(|f| (f.cf, &f.start[..]) <= (cf, key));
        let Some(fragment) = at.checked_sub(1).map(|i| &self.fragments[i]) else {
            return 0;
        };
        if fragment.cf != cf || key >= &fragment.end[..] {
            return 0;
        }
        fragment
            .groups
            .iter()
            .map(|&g| self.groups[g as usize].seq_at(snapshot))
            .max()
            .unwrap_or(0)
    }
}

/// Cut `groups[from..to]`, all of one column family and sorted by start, at
/// every boundary, and push the covered pieces.
fn fragment_cf(groups: &[Group], from: usize, to: usize, out: &mut Vec<Fragment>) {
    let mut bounds: Vec<&Bytes> = groups[from..to]
        .iter()
        .flat_map(|g| [&g.start, &g.end])
        .collect();
    bounds.sort_unstable();
    bounds.dedup();

    let mut next = from;
    let mut active: Vec<u32> = Vec::new();
    for pair in bounds.windows(2) {
        let (lo, hi) = (pair[0], pair[1]);
        while next < to && groups[next].start <= *lo {
            active.push(next as u32);
            next += 1;
        }
        active.retain(|&g| groups[g as usize].end > *lo);
        if !active.is_empty() {
            out.push(Fragment {
                cf: groups[from].cf,
                start: lo.clone(),
                end: hi.clone(),
                groups: active.clone(),
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memtable::covering_seq;

    fn t(cf: u32, start: &str, end: &str, seq: u64) -> RangeTombstone {
        RangeTombstone {
            cf,
            start: Bytes::copy_from_slice(start.as_bytes()),
            end: Bytes::copy_from_slice(end.as_bytes()),
            seq,
        }
    }

    /// Agrees with the linear scan on every key and snapshot of a mix of
    /// repeated, nested, overlapping, adjacent and empty ranges over two
    /// column families.
    #[test]
    fn matches_linear_scan() {
        let tombstones = vec![
            t(1, "b", "d", 5),
            t(1, "b", "d", 9),
            t(1, "b", "d", 2),
            t(1, "a", "z", 4), // wide, over everything of cf 1
            t(1, "c", "f", 7), // overlaps b..d
            t(1, "f", "g", 3), // adjacent to c..f
            t(1, "m", "m", 8), // empty
            t(1, "p", "q", 11),
            t(2, "b", "d", 6), // same range, another cf
            t(0, "", "b", 1),
        ];
        let index = TombstoneIndex::build(&tombstones);
        assert_eq!(index.len(), 9);
        assert_eq!(index.groups().len(), 7);
        let keys = [
            "", "a", "a0", "b", "b5", "c", "cz", "d", "e", "f", "f5", "g", "m", "o", "p", "pz",
            "q", "y", "z", "zz",
        ];
        for cf in 0..4 {
            for key in keys {
                for snapshot in [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, u64::MAX] {
                    assert_eq!(
                        index.covering_seq(cf, key.as_bytes(), snapshot),
                        covering_seq(&tombstones, cf, key.as_bytes(), snapshot),
                        "cf {cf} key {key:?} snapshot {snapshot}"
                    );
                }
            }
        }
    }

    #[test]
    fn empty_index_covers_nothing() {
        let index = TombstoneIndex::build(&[]);
        assert!(index.is_empty());
        assert_eq!(index.covering_seq(0, b"k", u64::MAX), 0);
    }

    /// The case it was built for: thousands of truncates of the same ranges.
    #[test]
    fn repeated_ranges_collapse_into_groups() {
        let mut tombstones = Vec::new();
        for round in 0..1_000u64 {
            for coll in 0..12u64 {
                let start = format!("{coll:04}doc:");
                let end = format!("{coll:04}doc;");
                tombstones.push(t(7, &start, &end, round * 12 + coll + 1));
            }
        }
        let index = TombstoneIndex::build(&tombstones);
        assert_eq!(index.groups().len(), 12);
        assert_eq!(index.len(), 12_000);
        assert_eq!(index.covering_seq(7, b"0003doc:k", u64::MAX), 999 * 12 + 4);
        assert_eq!(index.covering_seq(7, b"0003doc:k", 100), 8 * 12 + 4);
        assert_eq!(index.covering_seq(7, b"0003doc;", u64::MAX), 0);
    }
}
