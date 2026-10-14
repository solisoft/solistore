//! The commit path under concurrency and failure: group commit, atomic
//! visibility, and what an fsync error does.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use solistore::fs::{FaultFs, Torn};
use solistore::{Db, Error, IteratorMode, Options, WriteBatch};

fn open(fs: &FaultFs) -> Db {
    Db::open(
        "/db",
        Options {
            fs: Arc::new(fs.clone()),
            ..Options::default()
        },
    )
    .unwrap()
}

#[test]
fn concurrent_commits_share_fsyncs() {
    let fs = FaultFs::new();
    // An fsync that takes as long as a fast SSD's leaves room for a queue.
    fs.set_sync_delay(Duration::from_micros(500));
    let db = open(&fs);
    let cf = db.default_cf();
    let before = fs.sync_count();
    let (threads, per_thread) = (8u64, 100u64);
    std::thread::scope(|scope| {
        for t in 0..threads {
            let (db, cf) = (&db, &cf);
            scope.spawn(move || {
                for i in 0..per_thread {
                    db.put(cf, format!("{t}:{i}"), b"v").unwrap();
                }
            });
        }
    });
    let syncs = fs.sync_count() - before;
    let commits = threads * per_thread;
    assert!(
        syncs < commits / 2,
        "{syncs} fsyncs for {commits} commits: commits are not being grouped"
    );

    fs.crash(Torn::None);
    drop(db);
    let db = open(&fs);
    let count = db
        .iterator(&db.default_cf(), IteratorMode::Start)
        .unwrap()
        .count() as u64;
    assert_eq!(
        count, commits,
        "every acknowledged commit survives the crash"
    );
}

/// Readers racing writers never see part of a batch, and always see a batch
/// whose write has returned.
#[test]
fn a_batch_is_visible_entirely_or_not_at_all() {
    let dir = tempfile::tempdir().unwrap();
    // Small memtables: readers race memtable switches and flushes too.
    let opts = Options {
        write_buffer_size: 256 << 10,
        ..Options::default()
    };
    let db = Db::open(dir.path(), opts).unwrap();
    let cf = db.default_cf();
    let writers = 4u64;
    let progress: Vec<AtomicU64> = (0..writers).map(|_| AtomicU64::new(0)).collect();
    let stop = AtomicBool::new(false);
    let (checked, torn, missing) = (AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0));

    std::thread::scope(|scope| {
        for t in 0..writers {
            let (db, cf, progress, stop) = (&db, &cf, &progress, &stop);
            scope.spawn(move || {
                let mut k = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    let mut batch = WriteBatch::new();
                    for part in ["a", "b", "c", "d"] {
                        batch.put(cf, format!("{t}:{k:08}:{part}"), k.to_le_bytes());
                    }
                    db.write(batch).unwrap();
                    k += 1;
                    progress[t as usize].store(k, Ordering::Release);
                }
            });
        }
        for r in 0..4u64 {
            let (db, cf, progress, stop) = (&db, &cf, &progress, &stop);
            let (checked, torn, missing) = (&checked, &torn, &missing);
            scope.spawn(move || {
                let mut n = r;
                while !stop.load(Ordering::Relaxed) {
                    n = n
                        .wrapping_mul(6364136223846793005)
                        .wrapping_add(1442695040888963407);
                    let t = (n >> 33) % writers;
                    let done = progress[t as usize].load(Ordering::Acquire);
                    // An acknowledged batch, or one that may be in flight.
                    let k = if done > 0 && n % 2 == 0 {
                        done - 1 - (n >> 40) % done.min(32)
                    } else {
                        done
                    };
                    let acked = k < done;
                    let parts: Vec<bool> = ["a", "b", "c", "d"]
                        .iter()
                        .map(|p| db.get(cf, format!("{t}:{k:08}:{p}")).unwrap().is_some())
                        .collect();
                    checked.fetch_add(1, Ordering::Relaxed);
                    // Gets are separate snapshots, each at least as new as the
                    // one before: once part a is visible, the rest must be.
                    if parts[0] && parts.iter().any(|p| !p) {
                        torn.fetch_add(1, Ordering::Relaxed);
                    }
                    if acked && !parts[0] {
                        missing.fetch_add(1, Ordering::Relaxed);
                    }
                }
            });
        }
        std::thread::sleep(Duration::from_secs(2));
        stop.store(true, Ordering::Relaxed);
    });
    assert!(checked.load(Ordering::Relaxed) > 1000);
    assert_eq!(
        torn.load(Ordering::Relaxed),
        0,
        "a reader saw part of a batch"
    );
    assert_eq!(
        missing.load(Ordering::Relaxed),
        0,
        "an acknowledged batch was invisible"
    );
}

#[test]
fn an_iterator_reads_at_the_snapshot_it_was_created_at() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), Options::default()).unwrap();
    let cf = db.default_cf();
    db.put(&cf, b"a", b"1").unwrap();
    db.put(&cf, b"c", b"1").unwrap();
    let it = db.iterator(&cf, IteratorMode::Start).unwrap();
    db.put(&cf, b"b", b"later").unwrap();
    db.delete(&cf, b"c").unwrap();
    let keys: Vec<_> = it.map(|r| r.unwrap().0.to_vec()).collect();
    assert_eq!(keys, vec![b"a".to_vec(), b"c".to_vec()]);
}

#[test]
fn a_failed_fsync_poisons_writes_and_loses_no_acknowledged_data() {
    let fs = FaultFs::new();
    let db = open(&fs);
    let cf = db.default_cf();
    db.put(&cf, b"before", b"1").unwrap();

    fs.fail_next_syncs(1);
    assert!(db.put(&cf, b"failed", b"1").is_err());
    // No retry: the kernel may already have dropped the dirty pages.
    assert!(matches!(db.put(&cf, b"after", b"1"), Err(Error::Poisoned)));
    assert!(matches!(db.create_cf("x"), Err(Error::Poisoned)));
    assert_eq!(
        db.get(&cf, b"before").unwrap().as_deref(),
        Some(&b"1"[..]),
        "reads still work"
    );
    assert_eq!(
        db.get(&cf, b"failed").unwrap(),
        None,
        "a failed commit is never visible"
    );

    fs.crash(Torn::None);
    drop(db);
    let db = open(&fs);
    let cf = db.default_cf();
    assert!(db.get(&cf, b"before").unwrap().is_some());
    assert!(db.get(&cf, b"after").unwrap().is_none());
    db.put(&cf, b"reopened", b"1")
        .expect("a reopened database accepts writes");
}

#[test]
fn a_second_open_of_the_same_directory_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let _db = Db::open(dir.path(), Options::default()).unwrap();
    let err = Db::open(dir.path(), Options::default()).unwrap_err();
    assert!(err.to_string().contains("locked"), "{err}");
}

#[test]
fn reopening_replays_the_log_quickly() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = Db::open(dir.path(), Options::default()).unwrap();
        let cf = db.default_cf();
        let mut batch = WriteBatch::new();
        for i in 0..100_000u32 {
            batch.put(&cf, i.to_be_bytes(), [0u8; 100]);
            if batch.len() == 1000 {
                db.write(std::mem::take(&mut batch)).unwrap();
            }
        }
    }
    let start = Instant::now();
    let db = Db::open(dir.path(), Options::default()).unwrap();
    let elapsed = start.elapsed();
    assert_eq!(db.recovery_report().batches, 100);
    assert_eq!(
        db.iterator(&db.default_cf(), IteratorMode::End)
            .unwrap()
            .count(),
        100_000
    );
    assert!(elapsed < Duration::from_secs(2), "reopen took {elapsed:?}");
}

#[test]
fn flushed_data_survives_reopen_and_dead_wals_are_not_replayed() {
    let dir = tempfile::tempdir().unwrap();
    let opts = Options {
        write_buffer_size: 64 << 10,
        target_file_size: 32 << 10,
        ..Options::default()
    };
    let n = 20_000u32;
    {
        let db = Db::open(dir.path(), opts.clone()).unwrap();
        let cf = db.default_cf();
        for chunk in (0..n).collect::<Vec<_>>().chunks(100) {
            let mut batch = WriteBatch::new();
            for i in chunk {
                batch.put(&cf, i.to_be_bytes(), format!("value {i}"));
            }
            db.write(batch).unwrap();
        }
        // Overwrite and delete some, so tables shadow each other.
        for i in (0..n).step_by(7) {
            db.put(&cf, i.to_be_bytes(), b"updated").unwrap();
        }
        for i in (0..n).step_by(11) {
            db.delete(&cf, i.to_be_bytes()).unwrap();
        }
        db.flush().unwrap();
        // Several tables, wherever compaction has put them by now.
        assert!(
            db.files_per_level().iter().sum::<usize>() > 2,
            "{:?}",
            db.files_per_level()
        );
        assert_eq!(db.memtable_bytes(), 0);
        let wals = std::fs::read_dir(dir.path())
            .unwrap()
            .filter(|e| {
                e.as_ref()
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .ends_with(".wal")
            })
            .count();
        // The live WAL, plus at most three dead ones kept to be written
        // over (recycle_wal_files); none of them is replayed (checked below).
        assert!(
            (1..=4).contains(&wals),
            "{wals} WAL files after a full flush"
        );
    }
    let db = Db::open(dir.path(), opts).unwrap();
    assert_eq!(db.recovery_report().batches, 0, "nothing left to replay");
    let cf = db.default_cf();
    let expect = |i: u32| {
        if i.is_multiple_of(11) {
            None
        } else if i.is_multiple_of(7) {
            Some(b"updated".to_vec())
        } else {
            Some(format!("value {i}").into_bytes())
        }
    };
    for i in (0..n).step_by(13) {
        assert_eq!(
            db.get(&cf, i.to_be_bytes()).unwrap().map(|v| v.to_vec()),
            expect(i),
            "key {i}"
        );
    }
    let forward: Vec<u32> = db
        .iterator(&cf, IteratorMode::Start)
        .unwrap()
        .map(|r| u32::from_be_bytes(r.unwrap().0[..].try_into().unwrap()))
        .collect();
    let want: Vec<u32> = (0..n).filter(|i| i % 11 != 0).collect();
    assert_eq!(forward, want);
    let backward: Vec<u32> = db
        .iterator(&cf, IteratorMode::End)
        .unwrap()
        .map(|r| u32::from_be_bytes(r.unwrap().0[..].try_into().unwrap()))
        .collect();
    assert_eq!(backward, want.iter().rev().copied().collect::<Vec<_>>());
}

#[test]
fn a_table_left_by_a_crashed_flush_is_removed_on_open() {
    let fs = FaultFs::new();
    {
        let db = open(&fs);
        db.put(&db.default_cf(), b"k", b"v").unwrap();
    }
    // A flush that wrote its table but crashed before the MANIFEST edit.
    let orphan = fs.create(std::path::Path::new("/db/999999.sst")).unwrap();
    orphan.append(b"half a table").unwrap();
    orphan.sync().unwrap();
    use solistore::fs::Fs;
    fs.sync_dir(std::path::Path::new("/db")).unwrap();
    let db = open(&fs);
    assert_eq!(db.recovery_report().orphan_tables, 1);
    assert!(!fs.exists(std::path::Path::new("/db/999999.sst")));
    assert_eq!(
        db.get(&db.default_cf(), b"k").unwrap().as_deref(),
        Some(&b"v"[..])
    );
}

#[test]
fn compaction_moves_data_down_and_keeps_every_key() {
    let dir = tempfile::tempdir().unwrap();
    let opts = Options {
        write_buffer_size: 16 << 10,
        target_file_size: 8 << 10,
        max_bytes_for_level_base: 64 << 10,
        level_multiplier: 4,
        level0_compaction_trigger: 2,
        ..Options::default()
    };
    let n = 30_000u32;
    let expect = |i: u32, round: u32| {
        if i.is_multiple_of(5) {
            None
        } else {
            Some(format!("{i}:{round}").into_bytes())
        }
    };
    {
        let db = Db::open(dir.path(), opts.clone()).unwrap();
        let cf = db.default_cf();
        for round in 0..3u32 {
            for chunk in (0..n).collect::<Vec<_>>().chunks(500) {
                let mut batch = WriteBatch::new();
                for &i in chunk {
                    match expect(i, round) {
                        Some(v) => batch.put(&cf, i.to_be_bytes(), v),
                        None => batch.delete(&cf, i.to_be_bytes()),
                    }
                }
                db.write(batch).unwrap();
            }
        }
        db.flush().unwrap();
        // Compaction runs in the background; give it time to settle.
        let deadline = Instant::now() + Duration::from_secs(20);
        while db.files_per_level()[0] >= 2 && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(20));
        }
        let levels = db.files_per_level();
        assert!(levels[0] < 2, "level 0 not compacted: {levels:?}");
        assert!(
            levels[2..].iter().sum::<usize>() > 0,
            "nothing reached level 2: {levels:?}"
        );
        for i in (0..n).step_by(7) {
            assert_eq!(
                db.get(&cf, i.to_be_bytes()).unwrap().map(|v| v.to_vec()),
                expect(i, 2),
                "key {i}"
            );
        }
    }
    let db = Db::open(dir.path(), opts).unwrap();
    let cf = db.default_cf();
    let keys: Vec<u32> = db
        .iterator(&cf, IteratorMode::Start)
        .unwrap()
        .map(|r| {
            let (k, v) = r.unwrap();
            let i = u32::from_be_bytes(k[..].try_into().unwrap());
            assert_eq!(Some(v.to_vec()), expect(i, 2));
            i
        })
        .collect();
    assert_eq!(
        keys,
        (0..n).filter(|i| !i.is_multiple_of(5)).collect::<Vec<_>>()
    );
    // Every file on disk is live: compaction inputs were deleted.
    let on_disk = std::fs::read_dir(dir.path())
        .unwrap()
        .filter(|e| {
            e.as_ref()
                .unwrap()
                .file_name()
                .to_string_lossy()
                .ends_with(".sst")
        })
        .count();
    assert_eq!(on_disk, db.files_per_level().iter().sum::<usize>());
}

/// A collection is a key prefix in a shared column family; dropping it is
/// one range delete. `compact_range` must give its space back.
#[test]
fn a_range_delete_gives_its_space_back() {
    let dir = tempfile::tempdir().unwrap();
    let opts = Options {
        write_buffer_size: 256 << 10,
        target_file_size: 128 << 10,
        max_bytes_for_level_base: 1 << 20,
        level_multiplier: 4,
        ..Options::default()
    };
    let db = Db::open(dir.path(), opts).unwrap();
    let cf = db.default_cf();
    let value = vec![b'x'; 200];
    // Ten collections of 5,000 documents, interleaved as a server writes them.
    for i in 0..5_000u32 {
        let mut batch = WriteBatch::new();
        for coll in 0..10u32 {
            batch.put(
                &cf,
                [&coll.to_be_bytes()[..], &i.to_be_bytes()].concat(),
                &value,
            );
        }
        db.write(batch).unwrap();
    }
    db.compact_range(&cf, None, None).unwrap();
    let before = db.live_table_bytes();

    // Drop collections 3 and 4.
    let mut batch = WriteBatch::new();
    batch.delete_range(&cf, 3u32.to_be_bytes(), 5u32.to_be_bytes());
    db.write(batch).unwrap();
    assert!(db
        .get(&cf, [&3u32.to_be_bytes()[..], &7u32.to_be_bytes()].concat())
        .unwrap()
        .is_none());
    db.compact_range(&cf, Some(&3u32.to_be_bytes()), Some(&5u32.to_be_bytes()))
        .unwrap();

    let after = db.live_table_bytes();
    let freed = before.saturating_sub(after);
    assert!(
        freed * 10 >= before * 2 * 9 / 10,
        "freed {freed} of {before}: expected ~20%"
    );
    assert_eq!(
        db.pending_range_tombstones(),
        0,
        "nothing left for the tombstone to delete"
    );
    let left = db.iterator(&cf, IteratorMode::Start).unwrap().count();
    assert_eq!(left, 8 * 5_000);
    // And it is still right after a reopen.
    drop(db);
    let db = Db::open(dir.path(), Options::default()).unwrap();
    assert_eq!(
        db.iterator(&db.default_cf(), IteratorMode::Start)
            .unwrap()
            .count(),
        8 * 5_000
    );
}

#[test]
fn dropping_a_column_family_deletes_its_files() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(
        dir.path(),
        Options {
            write_buffer_size: 64 << 10,
            ..Options::default()
        },
    )
    .unwrap();
    let doomed = db.create_cf("doomed").unwrap();
    let kept = db.default_cf();
    for i in 0..5_000u32 {
        let mut batch = WriteBatch::new();
        batch.put(&doomed, i.to_be_bytes(), [1u8; 100]);
        batch.put(&kept, i.to_be_bytes(), [2u8; 100]);
        db.write(batch).unwrap();
    }
    db.flush().unwrap();
    let before = db.live_table_bytes();
    db.drop_cf("doomed").unwrap();
    assert!(
        db.live_table_bytes() * 10 < before * 6,
        "{} of {before} left",
        db.live_table_bytes()
    );
    let again = db.create_cf("doomed").unwrap();
    assert_eq!(
        db.iterator(&again, IteratorMode::Start).unwrap().count(),
        0,
        "a new cf of the same name is empty"
    );
    assert_eq!(
        db.iterator(&kept, IteratorMode::Start).unwrap().count(),
        5_000
    );
}

#[test]
fn a_checkpoint_is_a_consistent_openable_copy() {
    let dir = tempfile::tempdir().unwrap();
    let opts = Options {
        write_buffer_size: 32 << 10,
        ..Options::default()
    };
    let db = Db::open(dir.path().join("live"), opts.clone()).unwrap();
    let cf = db.create_cf("c").unwrap();
    for i in 0..10_000u32 {
        db.put(&cf, i.to_be_bytes(), b"before").unwrap();
    }
    let mut batch = WriteBatch::new();
    batch.delete_range(&cf, 100u32.to_be_bytes(), 200u32.to_be_bytes());
    db.write(batch).unwrap();
    db.checkpoint(dir.path().join("copy")).unwrap();
    assert!(
        db.checkpoint(dir.path().join("copy")).is_err(),
        "an existing target is refused"
    );
    for i in 0..10_000u32 {
        db.put(&cf, i.to_be_bytes(), b"after").unwrap();
    }
    db.compact_range(&cf, None, None).unwrap();

    let copy = Db::open(dir.path().join("copy"), opts).unwrap();
    let ccf = copy
        .cf_handle("c")
        .expect("column families are part of the copy");
    let rows: Vec<_> = copy
        .iterator(&ccf, IteratorMode::Start)
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(rows.len(), 10_000 - 100);
    assert!(
        rows.iter().all(|(_, v)| &v[..] == b"before"),
        "nothing written after the checkpoint"
    );
    copy.put(&ccf, b"new", b"row").unwrap();
    assert_eq!(
        db.get(&cf, b"new").unwrap(),
        None,
        "the copy is independent"
    );
}

#[test]
fn column_families_come_and_go_while_others_are_written() {
    let dir = tempfile::tempdir().unwrap();
    let opts = Options {
        write_buffer_size: 32 << 10,
        ..Options::default()
    };
    let db = Db::open(dir.path(), opts.clone()).unwrap();
    let stable = db.create_cf("stable").unwrap();
    let stop = AtomicBool::new(false);
    let written = AtomicU64::new(0);
    std::thread::scope(|scope| {
        for t in 0..3u64 {
            let (db, stable, stop, written) = (&db, &stable, &stop, &written);
            scope.spawn(move || {
                let mut i = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    db.put(stable, format!("{t}:{i:08}"), b"v").unwrap();
                    written.fetch_add(1, Ordering::Relaxed);
                    i += 1;
                }
            });
        }
        let (db, stop) = (&db, &stop);
        scope.spawn(move || {
            for round in 0..200 {
                let cf = db.create_cf("churn").unwrap();
                for i in 0..20u32 {
                    db.put(&cf, i.to_be_bytes(), format!("round {round}"))
                        .unwrap();
                }
                assert_eq!(db.iterator(&cf, IteratorMode::Start).unwrap().count(), 20);
                db.drop_cf("churn").unwrap();
                assert!(matches!(
                    db.put(&cf, b"late", b"x"),
                    Err(Error::ColumnFamilyDropped(_))
                ));
            }
            stop.store(true, Ordering::Relaxed);
        });
    });
    let total = written.load(Ordering::Relaxed) as usize;
    assert_eq!(
        db.iterator(&stable, IteratorMode::Start).unwrap().count(),
        total
    );
    drop(db);
    let db = Db::open(dir.path(), opts).unwrap();
    assert!(db.cf_handle("churn").is_none());
    assert_eq!(
        db.iterator(&db.cf_handle("stable").unwrap(), IteratorMode::Start)
            .unwrap()
            .count(),
        total
    );
}
