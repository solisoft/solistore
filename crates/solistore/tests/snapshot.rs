//! Snapshots, borrowed iteration and split points.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use solistore::{
    ColumnFamily, Db, Direction, Durability, IteratorMode, Options, ReadOptions, Snapshot,
    WriteBatch,
};

/// Tiny memtables and files: every write path (switch, flush, compaction,
/// trivial move) runs many times while a test holds its snapshots.
fn small() -> Options {
    Options {
        durability: Durability::Eventual {
            sync_interval: Duration::from_millis(100),
        },
        write_buffer_size: 16 << 10,
        target_file_size: 32 << 10,
        max_bytes_for_level_base: 128 << 10,
        level_multiplier: 4,
        level0_compaction_trigger: 2,
        ..Options::default()
    }
}

fn bounded(lower: &[u8], upper: &[u8]) -> ReadOptions {
    ReadOptions {
        lower_bound: Some(lower.to_vec()),
        upper_bound: Some(upper.to_vec()),
    }
}

/// Every entry of `[lower, upper)` through `next_ref`.
fn scan_ref(
    snapshot: &Snapshot,
    cf: &ColumnFamily,
    lower: &[u8],
    upper: &[u8],
) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut it = snapshot
        .iterator_opt(cf, bounded(lower, upper), IteratorMode::Start)
        .unwrap();
    let mut out = Vec::new();
    while let Some(item) = it.next_ref() {
        let (k, v) = item.unwrap();
        out.push((k.to_vec(), v.to_vec()));
    }
    out
}

fn as_map(rows: Vec<(Vec<u8>, Vec<u8>)>) -> BTreeMap<Vec<u8>, Vec<u8>> {
    rows.into_iter().collect()
}

fn sst_files(dir: &std::path::Path) -> usize {
    std::fs::read_dir(dir)
        .unwrap()
        .filter(|e| {
            e.as_ref()
                .unwrap()
                .file_name()
                .to_string_lossy()
                .ends_with(".sst")
        })
        .count()
}

/// A snapshot reads the state it was taken at through every kind of read,
/// while the database is overwritten, deleted from, range-deleted, flushed
/// and compacted under it; the files it held are deleted once it is
/// dropped.
#[test]
fn a_snapshot_reads_its_state_across_writes_flushes_and_compactions() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), small()).unwrap();
    let cf = db.default_cf();
    let mut then = BTreeMap::new();
    for i in 0..2000u32 {
        let (k, v) = (format!("k{i:05}"), format!("v{i}-0").repeat(8));
        db.put(&cf, &k, &v).unwrap();
        then.insert(k.into_bytes(), v.into_bytes());
    }
    // Some of it in tables, some in the memtables.
    db.flush().unwrap();
    for i in (0..2000u32).step_by(7) {
        let (k, v) = (format!("k{i:05}"), format!("v{i}-1").repeat(8));
        db.put(&cf, &k, &v).unwrap();
        then.insert(k.into_bytes(), v.into_bytes());
    }
    let snapshot = db.snapshot();
    let seq = snapshot.sequence();

    // Overwrite everything, delete some, range-delete more, add new keys.
    for i in 0..2000u32 {
        db.put(&cf, format!("k{i:05}"), format!("v{i}-2").repeat(8))
            .unwrap();
    }
    for i in (0..2000u32).step_by(3) {
        db.delete(&cf, format!("k{i:05}")).unwrap();
    }
    let mut batch = WriteBatch::new();
    batch.delete_range(&cf, b"k00500", b"k01500");
    db.write(batch).unwrap();
    for i in 2000..2500u32 {
        db.put(&cf, format!("k{i:05}"), b"new").unwrap();
    }

    let check = |when: &str| {
        assert_eq!(snapshot.sequence(), seq);
        // Point reads.
        for i in (0..2600u32).step_by(13) {
            let k = format!("k{i:05}");
            assert_eq!(
                snapshot.get(&cf, &k).unwrap().map(|v| v.to_vec()),
                then.get(k.as_bytes()).cloned(),
                "{when}: get {k}"
            );
        }
        let keys: Vec<String> = (0..2600u32)
            .step_by(11)
            .map(|i| format!("k{i:05}"))
            .collect();
        let got = snapshot.multi_get(&cf, &keys).unwrap();
        for (k, v) in keys.iter().zip(got) {
            assert_eq!(
                v.map(|v| v.to_vec()),
                then.get(k.as_bytes()).cloned(),
                "{when}: multi_get {k}"
            );
        }
        // Owned and borrowed scans, both directions.
        let owned: Vec<(Vec<u8>, Vec<u8>)> = snapshot
            .iterator(&cf, IteratorMode::Start)
            .unwrap()
            .map(|r| r.map(|(k, v)| (k.to_vec(), v.to_vec())).unwrap())
            .collect();
        assert_eq!(as_map(owned.clone()), then, "{when}: forward scan");
        assert_eq!(
            scan_ref(&snapshot, &cf, b"", b"\xff"),
            owned,
            "{when}: next_ref"
        );
        let mut back: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        let mut it = snapshot.iterator(&cf, IteratorMode::End).unwrap();
        while let Some(item) = it.next_ref() {
            let (k, v) = item.unwrap();
            back.push((k.to_vec(), v.to_vec()));
        }
        back.reverse();
        assert_eq!(back, owned, "{when}: reverse next_ref");
        // A bounded scan from the middle.
        let from: Vec<(Vec<u8>, Vec<u8>)> = snapshot
            .iterator_opt(
                &cf,
                bounded(b"k00100", b"k00900"),
                IteratorMode::From(b"k00700", Direction::Reverse),
            )
            .unwrap()
            .map(|r| r.map(|(k, v)| (k.to_vec(), v.to_vec())).unwrap())
            .collect();
        let want: Vec<(Vec<u8>, Vec<u8>)> = then
            .range(b"k00100".to_vec()..=b"k00700".to_vec())
            .rev()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        assert_eq!(from, want, "{when}: bounded reverse scan");
    };
    check("after the writes");
    db.flush().unwrap();
    check("after a flush");
    db.compact_range(&cf, None, None).unwrap();
    check("after a compaction");
    // The database itself moved on.
    assert_eq!(db.get(&cf, b"k00003").unwrap(), None);
    assert_eq!(
        db.get(&cf, b"k02100").unwrap().as_deref(),
        Some(&b"new"[..])
    );
    assert!(db.snapshot().sequence() > seq);

    // A clone is the same snapshot; the files go once both are dropped.
    let clone = snapshot.clone();
    drop(snapshot);
    assert_eq!(
        clone.get(&cf, b"k00003").unwrap().map(|v| v.to_vec()),
        then.get(&b"k00003"[..]).cloned()
    );
    let held = sst_files(dir.path());
    drop(clone);
    let live: usize = db.files_per_level().iter().sum();
    assert!(
        held > live,
        "the snapshot held no replaced file ({held} on disk, {live} live)"
    );
    assert_eq!(
        sst_files(dir.path()),
        live,
        "replaced files outlived the snapshot"
    );
}

/// Writers commit batches that each put one key in every one of `RANGES`
/// disjoint ranges, plus a per-writer counter in each, while flushes and
/// compactions run. Readers take a snapshot and scan every range on its
/// own thread, the iterators created at different moments: each range
/// must show exactly the same batches, those up to the counter, never part
/// of one.
#[test]
fn snapshot_iterators_on_parallel_threads_see_the_same_whole_batches() {
    const RANGES: usize = 6;
    const WRITERS: u64 = 3;
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(Db::open(dir.path(), small()).unwrap());
    let cf = db.default_cf();
    let stop = Arc::new(AtomicBool::new(false));
    let checks = Arc::new(AtomicU64::new(0));
    let writers: Vec<_> = (0..WRITERS)
        .map(|w| {
            let (db, cf, stop) = (db.clone(), cf.clone(), stop.clone());
            std::thread::spawn(move || {
                let mut i = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    i += 1;
                    let mut batch = WriteBatch::new();
                    for r in 0..RANGES {
                        batch.put(&cf, format!("r{r}/w{w}/{i:08}"), [7u8; 100]);
                        batch.put(&cf, format!("r{r}/count/w{w}"), i.to_be_bytes());
                    }
                    // Every few batches, delete an older one whole.
                    if i.is_multiple_of(5) {
                        for r in 0..RANGES {
                            batch.delete(&cf, format!("r{r}/w{w}/{:08}", i - 4));
                        }
                    }
                    db.write(batch).unwrap();
                }
            })
        })
        .collect();

    let deadline = std::time::Instant::now() + Duration::from_secs(4);
    while std::time::Instant::now() < deadline {
        let snapshot = db.snapshot();
        let per_range: Vec<BTreeMap<Vec<u8>, Vec<u8>>> = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..RANGES)
                .map(|r| {
                    let (snapshot, cf) = (&snapshot, &cf);
                    scope.spawn(move || {
                        // Created at different times, as the database moves.
                        std::thread::sleep(Duration::from_millis(r as u64));
                        let (lo, hi) = (format!("r{r}/"), format!("r{r}0"));
                        as_map(scan_ref(snapshot, cf, lo.as_bytes(), hi.as_bytes()))
                    })
                })
                .collect();
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });
        // Strip the range prefix: every range must hold the same rows.
        let strip = |r: usize, rows: &BTreeMap<Vec<u8>, Vec<u8>>| -> BTreeMap<Vec<u8>, Vec<u8>> {
            rows.iter()
                .map(|(k, v)| (k[format!("r{r}/").len()..].to_vec(), v.clone()))
                .collect()
        };
        let first = strip(0, &per_range[0]);
        for (r, rows) in per_range.iter().enumerate().skip(1) {
            assert_eq!(strip(r, rows), first, "range {r} differs from range 0");
        }
        // And within a range, the batches are whole: writer w's live
        // entries are its batches up to its counter, less the deleted ones.
        for w in 0..WRITERS {
            let count = first
                .get(format!("count/w{w}").as_bytes())
                .map(|v| u64::from_be_bytes(v[..].try_into().unwrap()))
                .unwrap_or(0);
            let got: Vec<u64> = first
                .keys()
                .filter_map(|k| {
                    std::str::from_utf8(k)
                        .unwrap()
                        .strip_prefix(&format!("w{w}/"))
                        .map(|n| n.parse().unwrap())
                })
                .collect();
            let want: Vec<u64> = (1..=count)
                .filter(|i| !(i + 4 <= count && (i + 4).is_multiple_of(5)))
                .collect();
            assert_eq!(got, want, "writer {w} at counter {count}");
        }
        // The same snapshot, cut at its own split points, reads the same.
        let cuts = snapshot.split_points(&cf, b"r", b"s", 4).unwrap();
        let mut bounds = vec![b"r".to_vec()];
        bounds.extend(cuts);
        bounds.push(b"s".to_vec());
        let pieces: Vec<(Vec<u8>, Vec<u8>)> = bounds
            .windows(2)
            .flat_map(|w| scan_ref(&snapshot, &cf, &w[0], &w[1]))
            .collect();
        assert_eq!(pieces, scan_ref(&snapshot, &cf, b"r", b"s"));
        checks.fetch_add(1, Ordering::Relaxed);
    }
    stop.store(true, Ordering::Relaxed);
    writers.into_iter().for_each(|w| w.join().unwrap());
    assert!(checks.load(Ordering::Relaxed) > 5);
    assert!(
        db.stats().flushes > 0 && db.stats().compactions > 0,
        "no flush or compaction ran"
    );
}

/// Split points cut a range into parts of about equal size, strictly
/// inside it and in order; degenerate requests get none.
#[test]
fn split_points_cut_a_range_into_parts_of_about_equal_size() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), Options::default()).unwrap();
    let cf = db.default_cf();
    // Outside the range on both sides, then 100,000 keys of ~100 bytes.
    db.put(&cf, b"a", [0u8; 4096]).unwrap();
    db.put(&cf, b"z", [0u8; 4096]).unwrap();
    for chunk in 0..100u32 {
        let mut batch = WriteBatch::new();
        for i in chunk * 1000..(chunk + 1) * 1000 {
            batch.put(&cf, format!("doc:{i:08}"), [i as u8; 100]);
        }
        db.write(batch).unwrap();
    }
    assert!(
        db.split_points(&cf, b"doc:", b"doc;", 8)
            .unwrap()
            .is_empty(),
        "the memtable is not sampled"
    );
    db.flush().unwrap();
    db.compact_range(&cf, None, None).unwrap();

    let cuts = db.split_points(&cf, b"doc:", b"doc;", 8).unwrap();
    assert_eq!(cuts.len(), 7, "{cuts:?}");
    assert!(cuts.windows(2).all(|w| w[0] < w[1]));
    assert!(cuts
        .iter()
        .all(|c| c.as_slice() > &b"doc:"[..] && c.as_slice() < &b"doc;"[..]));
    let mut bounds = vec![b"doc:".to_vec()];
    bounds.extend(cuts);
    bounds.push(b"doc;".to_vec());
    for w in bounds.windows(2) {
        let n = db
            .iterator_opt(&cf, bounded(&w[0], &w[1]), IteratorMode::Start)
            .unwrap()
            .count();
        assert!(
            (10_000..=15_000).contains(&n),
            "a part of {n} keys: {bounds:?}"
        );
    }
    // The same cuts from a snapshot of the same tables.
    assert_eq!(
        db.snapshot()
            .split_points(&cf, b"doc:", b"doc;", 8)
            .unwrap(),
        bounds[1..8]
    );
    // Degenerate requests.
    assert!(db
        .split_points(&cf, b"doc:", b"doc;", 1)
        .unwrap()
        .is_empty());
    assert!(db
        .split_points(&cf, b"doc;", b"doc:", 8)
        .unwrap()
        .is_empty());
    assert!(db.split_points(&cf, b"b", b"c", 8).unwrap().is_empty());
    // More parts than samples: as many as there are.
    let many = db.split_points(&cf, b"doc:", b"doc;", 1_000_000).unwrap();
    assert!(many.len() > 100 && many.windows(2).all(|w| w[0] < w[1]));
    // Another column family has none of them.
    let other = db.create_cf("other").unwrap();
    assert!(db
        .split_points(&other, b"doc:", b"doc;", 8)
        .unwrap()
        .is_empty());
}

/// A snapshot of a dropped column family refuses reads, as the database does.
#[test]
fn a_snapshot_refuses_a_dropped_column_family() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), Options::default()).unwrap();
    let cf = db.create_cf("c").unwrap();
    db.put(&cf, b"k", b"v").unwrap();
    let snapshot = db.snapshot();
    assert_eq!(snapshot.get(&cf, b"k").unwrap().as_deref(), Some(&b"v"[..]));
    db.drop_cf("c").unwrap();
    assert!(snapshot.get(&cf, b"k").is_err());
    assert!(snapshot.iterator(&cf, IteratorMode::Start).is_err());
}
