//! A crash at every I/O operation in turn.
//!
//! The database runs with tiny memtables, files and levels, so a few hundred
//! writes go through WAL rotations, flushes, compactions, trivial moves,
//! tombstone reclaim and MANIFEST edits. The file system dies at the N-th
//! mutating operation — wherever that falls, foreground or background — and
//! for each N the reopened database must hold every acknowledged batch, and
//! the one in flight either whole or not at all.

use std::collections::BTreeMap;
use std::sync::Arc;

use solistore::fs::{FaultFs, Fs, Torn};
use solistore::{Db, IteratorMode, Options, WriteBatch};

type Model = BTreeMap<Vec<u8>, Vec<u8>>;

fn options(fs: &FaultFs) -> Options {
    Options {
        fs: Arc::new(fs.clone()),
        write_buffer_size: 1024,
        max_immutable_memtables: 1,
        target_file_size: 512,
        max_bytes_for_level_base: 2048,
        level_multiplier: 2,
        level0_compaction_trigger: 2,
        level0_slowdown_trigger: 4,
        level0_stop_trigger: 6,
        ..Options::default()
    }
}

enum Op {
    Put(Vec<u8>, Vec<u8>),
    Delete(Vec<u8>),
    DeleteRange(Vec<u8>, Vec<u8>),
}

fn apply(model: &mut Model, ops: &[Op]) {
    for op in ops {
        match op {
            Op::Put(k, v) => {
                model.insert(k.clone(), v.clone());
            }
            Op::Delete(k) => {
                model.remove(k);
            }
            Op::DeleteRange(a, b) => model.retain(|k, _| !(a <= k && k < b)),
        }
    }
}

/// The batches a run writes: deterministic, overlapping keys, some deletes
/// and range deletes.
fn batch_ops(i: u32) -> Vec<Op> {
    let key = |n: u32| format!("k{:02}", n % 40).into_bytes();
    let mut ops = vec![Op::Put(
        key(i),
        format!("v{i}-{}", "x".repeat((i % 7) as usize * 10)).into_bytes(),
    )];
    ops.push(Op::Put(key(i * 7 + 3), format!("w{i}").into_bytes()));
    if i.is_multiple_of(5) {
        ops.push(Op::Delete(key(i * 3)));
    }
    if i.is_multiple_of(23) {
        ops.push(Op::DeleteRange(key(i), key(i + 6)));
    }
    ops
}

fn dump(db: &Db) -> Model {
    db.iterator(&db.default_cf(), IteratorMode::Start)
        .unwrap()
        .map(|r| r.map(|(k, v)| (k.to_vec(), v.to_vec())).unwrap())
        .collect()
}

/// One run: crash at the `n`-th operation, reopen, check. Returns whether
/// the countdown fired (false once `n` is past everything the run does).
fn run(n: u64, torn: Torn) -> bool {
    let fs = FaultFs::new();
    fs.crash_after(n, torn);
    let mut acked = Model::new();
    let mut in_flight: Option<Vec<Op>> = None;
    if let Ok(db) = Db::open("/db", options(&fs.process())) {
        let cf = db.default_cf();
        for i in 0..300u32 {
            let ops = batch_ops(i);
            let mut batch = WriteBatch::new();
            for op in &ops {
                match op {
                    Op::Put(k, v) => batch.put(&cf, k, v),
                    Op::Delete(k) => batch.delete(&cf, k),
                    Op::DeleteRange(a, b) => batch.delete_range(&cf, a, b),
                }
            }
            match db.write(batch) {
                Ok(()) => apply(&mut acked, &ops),
                Err(_) => {
                    in_flight = Some(ops);
                    break;
                }
            }
            if i % 50 == 49 && db.flush().is_err() {
                break;
            }
        }
    }
    let fired = fs.countdown_fired();
    if !fired {
        fs.crash(torn);
    }

    let db = Db::open("/db", options(&fs.process()))
        .unwrap_or_else(|e| panic!("crash at op {n} ({torn:?}): reopen failed: {e}"));
    let got = dump(&db);
    let mut with_in_flight = acked.clone();
    if let Some(ops) = &in_flight {
        apply(&mut with_in_flight, ops);
    }
    if got != acked && got != with_in_flight {
        let show = |k: &Vec<u8>| String::from_utf8_lossy(k).into_owned();
        let missing: Vec<_> = acked
            .keys()
            .filter(|k| !got.contains_key(*k))
            .map(show)
            .collect();
        let extra: Vec<_> = got
            .keys()
            .filter(|k| !acked.contains_key(*k) && !with_in_flight.contains_key(*k))
            .map(show)
            .collect();
        let changed: Vec<_> = got
            .iter()
            .filter(|(k, v)| {
                acked.get(*k).is_some_and(|a| a != *v) && with_in_flight.get(*k) != Some(*v)
            })
            .map(|(k, v)| {
                format!(
                    "{}={} (acked {})",
                    show(k),
                    String::from_utf8_lossy(v),
                    String::from_utf8_lossy(&acked[k])
                )
            })
            .collect();
        let mut files = fs.list(std::path::Path::new("/db")).unwrap_or_default();
        files.sort();
        panic!(
            "crash at op {n} ({torn:?}): recovered state is neither the acknowledged one nor it plus the batch in flight\n  missing {missing:?}\n  extra {extra:?}\n  changed {changed:?}\n  in flight: {}\n  recovery: {:?}\n  files: {files:?}",
            in_flight.is_some(),
            db.recovery_report(),
        );
    }
    // And it keeps working.
    db.put(&db.default_cf(), b"after", b"reopen").unwrap();
    fired
}

#[test]
fn a_crash_at_any_io_operation_keeps_every_acknowledged_batch() {
    let mut fired = 0;
    for n in 0u64.. {
        let torn = if n.is_multiple_of(2) {
            Torn::None
        } else {
            Torn::Seeded(n)
        };
        if !run(n, torn) {
            break;
        }
        fired += 1;
    }
    assert!(
        fired > 500,
        "only {fired} crash points: the workload is not reaching the background paths"
    );
}
