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
    let db = Db::open(dir.path(), Options::default()).unwrap();
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
