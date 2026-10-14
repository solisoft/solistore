//! Turning a sealed memtable into level-0 table files.
//!
//! Only the newest version of each key is written: every entry of a sealed
//! memtable is visible to every reader that will ever read the new files
//! (see [`crate::version`]), so the older versions are dead. So is anything
//! the memtable's own range tombstones cover, and anything in a dropped
//! column family. Delete markers are kept — they shadow older data in older
//! files.

use std::path::Path;

use crate::compaction::{LiveCf, OutputFiles};
use crate::error::Result;
use crate::fs::Fs;
use crate::key;
use crate::memtable::{covering_seq, Memtable};
use crate::sst::TableProps;

/// Write the memtable's live entries as tables of about `target_size`
/// bytes, cut at column family boundaries, with `watermark` as their
/// tombstone watermark. Each file is fsynced, then the directory. Returns
/// the files written, in key order.
pub fn write_tables(
    fs: &dyn Fs,
    dir: &Path,
    mem: &Memtable,
    live_cf: &(dyn Fn(u32) -> bool + Sync),
    target_size: u64,
    watermark: u64,
    mut allocate: impl FnMut() -> u64,
) -> Result<Vec<(u64, TableProps)>> {
    let tombstones = mem.tombstones();
    let mut out = OutputFiles::new(fs, dir, target_size, watermark);
    let mut live = LiveCf::new(live_cf);
    let mut last_id: Vec<u8> = Vec::new();
    for (k, v) in mem.entries() {
        let id = key::cf_and_user_key(&k);
        if !last_id.is_empty() && id == last_id.as_slice() {
            continue; // an older version of the key just handled
        }
        last_id.clear();
        last_id.extend_from_slice(id);
        let Some(parsed) = key::parse(&k) else {
            continue;
        };
        if !live.check(parsed.cf)
            || covering_seq(&tombstones, parsed.cf, parsed.user_key, u64::MAX) > parsed.seq
        {
            continue;
        }
        out.add(parsed.cf, &k, &v, &mut allocate)?;
    }
    out.finish()
}

/// Remove files a crash left behind: tables no MANIFEST names. Returns the
/// highest table number seen (so it is not reused) and how many were removed.
pub fn remove_orphans(fs: &dyn Fs, dir: &Path, live: &dyn Fn(u64) -> bool) -> Result<(u64, usize)> {
    let (mut highest, mut removed) = (0, 0);
    for name in fs.list(dir)? {
        if let Some(number) = crate::version::parse_table_name(&name) {
            highest = highest.max(number);
            if !live(number) {
                fs.remove(&dir.join(&name)).ok();
                removed += 1;
            }
        }
    }
    Ok((highest, removed))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::FaultFs;
    use crate::iter::collect_all;
    use crate::key::Kind;
    use crate::sst::{TableIter, TableReader};
    use crate::version::table_name;
    use std::sync::Arc;

    #[test]
    fn a_flush_keeps_the_newest_live_version_and_cuts_at_column_families() {
        let fs = FaultFs::new();
        let dir = Path::new("/db");
        let mem = Memtable::new(1);
        mem.insert(0, b"a", 1, Kind::Put, b"old");
        mem.insert(0, b"a", 5, Kind::Put, b"new");
        mem.insert(0, b"b", 2, Kind::Delete, b"");
        mem.insert(1, b"a", 3, Kind::Put, b"other cf");
        let mut next = 10;
        mem.insert(0, b"c", 4, Kind::Put, b"covered");
        mem.insert(0, b"d", 9, Kind::Put, b"newer than the tombstone");
        mem.insert_range_tombstone(0, b"c", b"e", 8);
        mem.insert(7, b"x", 6, Kind::Put, b"dropped column family");
        let files = write_tables(&fs, dir, &mem, &|cf| cf != 7, 1 << 20, 8, || {
            next += 1;
            next
        })
        .unwrap();
        assert_eq!(files.len(), 2, "one file per live column family");
        assert!(files.iter().all(|(_, p)| p.tombstone_watermark == 8));
        let reader = Arc::new(
            TableReader::open(fs.open_random(&dir.join(table_name(files[0].0))).unwrap()).unwrap(),
        );
        let entries = collect_all(&mut TableIter::new(reader));
        let parsed: Vec<_> = entries
            .iter()
            .map(|(k, v)| (key::parse(k).unwrap().user_key.to_vec(), v.to_vec()))
            .collect();
        assert_eq!(
            parsed,
            vec![
                (b"a".to_vec(), b"new".to_vec()),
                (b"b".to_vec(), Vec::new()),
                (b"d".to_vec(), b"newer than the tombstone".to_vec())
            ]
        );
    }
}
