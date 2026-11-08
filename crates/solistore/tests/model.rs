//! Random operation sequences, checked against a `BTreeMap` model.
//!
//! Every sequence runs on [`FaultFs`] and mixes writes, reads, scans in both
//! directions, column family creation and drops, clean reopens and crashes
//! (with torn tails). In `Full` durability every acknowledged write must
//! survive a crash, so the database must equal the model after each one.
//!
//! Scans are read three ways (owned items, borrowed with `next_ref`, and
//! the two interleaved), through snapshots taken earlier against the model
//! as it was then, and cut at split points.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Arc;

use proptest::prelude::*;
use solistore::fs::{FaultFs, Torn};
use solistore::Db;
use solistore::{
    DbIterator, Direction, Durability, IteratorMode, Options, ReadOptions, Snapshot, WriteBatch,
};

const CFS: [&str; 4] = ["default", "c1", "c2", "c3"];

#[derive(Debug, Clone)]
enum Write {
    Put(usize, Vec<u8>, Vec<u8>),
    Delete(usize, Vec<u8>),
    DeleteRange(usize, Vec<u8>, Vec<u8>),
}

#[derive(Debug, Clone)]
enum Op {
    Batch(Vec<Write>),
    Get(usize, Vec<u8>),
    /// Several keys at once, repeats and absent keys included: owned, and
    /// borrowed through `multi_get_with`.
    MultiGet(usize, Vec<Vec<u8>>),
    Scan {
        cf: usize,
        lower: Option<Vec<u8>>,
        upper: Option<Vec<u8>>,
        mode: Mode,
    },
    CreateCf(usize),
    DropCf(usize),
    /// Keep a snapshot (at most a few: the oldest goes).
    TakeSnapshot,
    /// Read through a kept snapshot: a scan, then a get.
    ReadSnapshot {
        which: usize,
        cf: usize,
        lower: Option<Vec<u8>>,
        upper: Option<Vec<u8>>,
        mode: Mode,
        key: Vec<u8>,
    },
    /// Cut a column family at split points and scan the parts.
    Split(usize, usize),
    Reopen,
    Flush,
    Crash(u64),
}

#[derive(Debug, Clone)]
enum Mode {
    Start,
    End,
    From(Vec<u8>, bool),
}

/// Short keys over a three-letter alphabet: collisions, shared prefixes and
/// the empty key all come up often.
fn key() -> impl Strategy<Value = Vec<u8>> {
    prop::collection::vec(prop::sample::select(vec![b'a', b'b', b'c']), 0..4)
}

fn write() -> impl Strategy<Value = Write> {
    prop_oneof![
        4 => (0..CFS.len(), key(), prop::collection::vec(any::<u8>(), 0..8)).prop_map(|(c, k, v)| Write::Put(c, k, v)),
        2 => (0..CFS.len(), key()).prop_map(|(c, k)| Write::Delete(c, k)),
        1 => (0..CFS.len(), key(), key()).prop_map(|(c, a, b)| Write::DeleteRange(c, a, b)),
    ]
}

fn mode() -> impl Strategy<Value = Mode> {
    prop_oneof![
        Just(Mode::Start),
        Just(Mode::End),
        (key(), any::<bool>()).prop_map(|(k, rev)| Mode::From(k, rev))
    ]
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        8 => prop::collection::vec(write(), 1..5).prop_map(Op::Batch),
        4 => (0..CFS.len(), key()).prop_map(|(c, k)| Op::Get(c, k)),
        3 => (0..CFS.len(), prop::collection::vec(key(), 0..10)).prop_map(|(c, ks)| Op::MultiGet(c, ks)),
        3 => (0..CFS.len(), prop::option::of(key()), prop::option::of(key()), mode())
            .prop_map(|(cf, lower, upper, mode)| Op::Scan { cf, lower, upper, mode }),
        1 => (1..CFS.len()).prop_map(Op::CreateCf),
        1 => (1..CFS.len()).prop_map(Op::DropCf),
        1 => Just(Op::TakeSnapshot),
        2 => (0..4usize, 0..CFS.len(), prop::option::of(key()), prop::option::of(key()), mode(), key())
            .prop_map(|(which, cf, lower, upper, mode, key)| Op::ReadSnapshot { which, cf, lower, upper, mode, key }),
        1 => (0..CFS.len(), 2..6usize).prop_map(|(cf, n)| Op::Split(cf, n)),
        1 => Just(Op::Reopen),
        1 => Just(Op::Flush),
        1 => any::<u64>().prop_map(Op::Crash),
    ]
}

#[derive(Default, Clone, PartialEq, Debug)]
struct Model {
    live: BTreeSet<usize>,
    data: BTreeMap<(usize, Vec<u8>), Vec<u8>>,
}

impl Model {
    fn new() -> Self {
        Model {
            live: BTreeSet::from([0]),
            data: BTreeMap::new(),
        }
    }

    fn apply(&mut self, w: &Write) {
        match w {
            Write::Put(cf, k, v) => {
                self.data.insert((*cf, k.clone()), v.clone());
            }
            Write::Delete(cf, k) => {
                self.data.remove(&(*cf, k.clone()));
            }
            Write::DeleteRange(cf, a, b) => {
                self.data.retain(|(c, k), _| !(c == cf && a <= k && k < b))
            }
        }
    }

    fn cf_of(w: &Write) -> usize {
        match w {
            Write::Put(cf, ..) | Write::Delete(cf, _) | Write::DeleteRange(cf, ..) => *cf,
        }
    }

    fn scan(
        &self,
        cf: usize,
        lower: &Option<Vec<u8>>,
        upper: &Option<Vec<u8>>,
        mode: &Mode,
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        let in_bounds = |k: &Vec<u8>| {
            lower.as_ref().is_none_or(|l| k >= l) && upper.as_ref().is_none_or(|u| k < u)
        };
        let mut rows: Vec<_> = self
            .data
            .iter()
            .filter(|((c, k), _)| *c == cf && in_bounds(k))
            .map(|((_, k), v)| (k.clone(), v.clone()))
            .collect();
        match mode {
            Mode::Start => rows,
            Mode::End => {
                rows.reverse();
                rows
            }
            Mode::From(from, false) => rows.into_iter().filter(|(k, _)| k >= from).collect(),
            Mode::From(from, true) => {
                rows.retain(|(k, _)| k <= from);
                rows.reverse();
                rows
            }
        }
    }
}

/// Tiny memtables and files force switches, flushes, writer stalls and
/// multi-file levels within a few dozen writes.
fn open(fs: &FaultFs, write_buffer_size: usize) -> Db {
    let opts = Options {
        // A process handle: after a crash, nothing the dead process's
        // threads still try reaches the file system.
        fs: Arc::new(fs.process()),
        write_buffer_size,
        max_immutable_memtables: 2,
        target_file_size: 512,
        // Levels of a few kilobytes: compactions, trivial moves and level
        // iterators across several files all happen within one test case.
        max_bytes_for_level_base: 2048,
        level_multiplier: 4,
        level0_compaction_trigger: 2,
        level0_slowdown_trigger: 6,
        level0_stop_trigger: 10,
        ..Options::default()
    };
    Db::open(Path::new("/db"), opts).expect("open")
}

/// What a `multi_get_with` lends, copied out: it must call back once per
/// key, in the order of the keys.
fn lent(
    read: impl FnOnce(&mut dyn FnMut(usize, Option<&[u8]>)) -> solistore::Result<()>,
) -> Vec<Option<Vec<u8>>> {
    let mut out = Vec::new();
    read(&mut |i, value| {
        assert_eq!(i, out.len(), "called out of order");
        out.push(value.map(<[u8]>::to_vec));
    })
    .unwrap();
    out
}

fn iterator_mode(mode: &Mode) -> IteratorMode<'_> {
    match mode {
        Mode::Start => IteratorMode::Start,
        Mode::End => IteratorMode::End,
        Mode::From(k, rev) => IteratorMode::From(
            k,
            if *rev {
                Direction::Reverse
            } else {
                Direction::Forward
            },
        ),
    }
}

/// Every row of an iterator, read `how`: 0 owned, 1 borrowed, 2 the two
/// in turn.
fn drain(mut it: DbIterator, how: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut rows = Vec::new();
    for step in 0.. {
        let row = if how == 1 || (how == 2 && step % 2 == 1) {
            it.next_ref()
                .map(|r| r.map(|(k, v)| (k.to_vec(), v.to_vec())))
        } else {
            it.next().map(|r| r.map(|(k, v)| (k.to_vec(), v.to_vec())))
        };
        match row {
            Some(row) => rows.push(row.unwrap()),
            None => return rows,
        }
    }
    unreachable!()
}

/// A snapshot kept by the test, with the model at that moment and each
/// column family's incarnation (a re-created family is another one).
struct Kept {
    snapshot: Snapshot,
    model: Model,
    incarnations: [u32; CFS.len()],
}

/// The whole database, read back through the public API.
fn dump(db: &Db) -> Model {
    let mut model = Model {
        live: BTreeSet::new(),
        data: BTreeMap::new(),
    };
    for (i, name) in CFS.iter().enumerate() {
        if let Some(cf) = db.cf_handle(name) {
            model.live.insert(i);
            for row in db.iterator(&cf, IteratorMode::Start).unwrap() {
                let (k, v) = row.unwrap();
                model.data.insert((i, k.to_vec()), v.to_vec());
            }
        }
    }
    model
}

fn run(ops: Vec<Op>, buffer: usize) -> Result<(), TestCaseError> {
    let fs = FaultFs::new();
    let open = |fs: &FaultFs| open(fs, buffer);
    let mut db = open(&fs);
    // Bytes an earlier crash left unreadable at the end of a WAL are counted
    // at every open until that WAL is flushed and retired; a clean close
    // must never add to them.
    let mut torn_before = 0;
    let mut model = Model::new();
    let mut kept: Vec<Kept> = Vec::new();
    let mut incarnations = [0u32; CFS.len()];

    for op in ops {
        match op {
            Op::Batch(writes) => {
                if writes
                    .iter()
                    .any(|w| !model.live.contains(&Model::cf_of(w)))
                {
                    continue;
                }
                let mut batch = WriteBatch::new();
                for w in &writes {
                    let cf = db.cf_handle(CFS[Model::cf_of(w)]).unwrap();
                    match w {
                        Write::Put(_, k, v) => batch.put(&cf, k, v),
                        Write::Delete(_, k) => batch.delete(&cf, k),
                        Write::DeleteRange(_, a, b) => batch.delete_range(&cf, a, b),
                    }
                }
                db.write(batch).unwrap();
                writes.iter().for_each(|w| model.apply(w));
            }
            Op::Get(cf, k) => {
                let Some(handle) = db.cf_handle(CFS[cf]) else {
                    continue;
                };
                let got = db.get(&handle, &k).unwrap().map(|b| b.to_vec());
                prop_assert_eq!(got, model.data.get(&(cf, k)).cloned());
            }
            Op::MultiGet(cf, keys) => {
                let Some(handle) = db.cf_handle(CFS[cf]) else {
                    continue;
                };
                let want: Vec<Option<Vec<u8>>> = keys
                    .iter()
                    .map(|k| model.data.get(&(cf, k.clone())).cloned())
                    .collect();
                let got: Vec<_> = db
                    .multi_get(&handle, &keys)
                    .unwrap()
                    .into_iter()
                    .map(|v| v.map(|b| b.to_vec()))
                    .collect();
                prop_assert_eq!(&got, &want, "multi_get {:?}", keys);
                prop_assert_eq!(lent(|f| db.multi_get_with(&handle, &keys, f)), want);
            }
            Op::Scan {
                cf,
                lower,
                upper,
                mode,
            } => {
                let Some(handle) = db.cf_handle(CFS[cf]) else {
                    continue;
                };
                let opts = ReadOptions {
                    lower_bound: lower.clone(),
                    upper_bound: upper.clone(),
                    ..Default::default()
                };
                let want = model.scan(cf, &lower, &upper, &mode);
                for how in 0..3 {
                    let it = db
                        .iterator_opt(&handle, opts.clone(), iterator_mode(&mode))
                        .unwrap();
                    prop_assert_eq!(drain(it, how), want.clone(), "{:?}, read {}", mode, how);
                }
            }
            Op::TakeSnapshot => {
                if kept.len() == 3 {
                    kept.remove(0);
                }
                kept.push(Kept {
                    snapshot: db.snapshot(),
                    model: model.clone(),
                    incarnations,
                });
            }
            Op::ReadSnapshot {
                which,
                cf,
                lower,
                upper,
                mode,
                key,
            } => {
                let Some(k) = kept.get(which % kept.len().max(1)) else {
                    continue;
                };
                // The family must be the one the snapshot saw, still live:
                // a dropped one refuses reads, a re-created one is new.
                let Some(handle) = db.cf_handle(CFS[cf]) else {
                    continue;
                };
                if !k.model.live.contains(&cf) || k.incarnations[cf] != incarnations[cf] {
                    continue;
                }
                let opts = ReadOptions {
                    lower_bound: lower.clone(),
                    upper_bound: upper.clone(),
                    ..Default::default()
                };
                // Both ways in: from the snapshot, and through the options.
                let it = if which % 2 == 0 {
                    k.snapshot.iterator_opt(&handle, opts, iterator_mode(&mode))
                } else {
                    let opts = ReadOptions {
                        snapshot: Some(k.snapshot.clone()),
                        ..opts
                    };
                    db.iterator_opt(&handle, opts, iterator_mode(&mode))
                }
                .unwrap();
                prop_assert_eq!(
                    drain(it, which % 3),
                    k.model.scan(cf, &lower, &upper, &mode),
                    "snapshot scan {:?}",
                    mode
                );
                let got = k.snapshot.get(&handle, &key).unwrap().map(|b| b.to_vec());
                prop_assert_eq!(
                    got,
                    k.model.data.get(&(cf, key.clone())).cloned(),
                    "snapshot get"
                );
                let keys = [key.clone(), b"a".to_vec(), key, b"cb".to_vec(), Vec::new()];
                let want: Vec<Option<Vec<u8>>> = keys
                    .iter()
                    .map(|key| k.model.data.get(&(cf, key.clone())).cloned())
                    .collect();
                let got: Vec<_> = k
                    .snapshot
                    .multi_get(&handle, &keys)
                    .unwrap()
                    .into_iter()
                    .map(|v| v.map(|b| b.to_vec()))
                    .collect();
                prop_assert_eq!(&got, &want, "snapshot multi_get");
                prop_assert_eq!(lent(|f| k.snapshot.multi_get_with(&handle, &keys, f)), want);
            }
            Op::Split(cf, n) => {
                let Some(handle) = db.cf_handle(CFS[cf]) else {
                    continue;
                };
                // Keys are over a, b, c: [a, d) holds all but the empty one.
                let cuts = db.split_points(&handle, b"a", b"d", n).unwrap();
                prop_assert!(cuts.len() < n, "{} cuts for {} parts", cuts.len(), n);
                prop_assert!(cuts.windows(2).all(|w| w[0] < w[1]), "{:?}", cuts);
                prop_assert!(
                    cuts.iter()
                        .all(|c| c.as_slice() > b"a".as_slice() && c.as_slice() < b"d".as_slice()),
                    "{:?}",
                    cuts
                );
                let mut bounds = vec![b"a".to_vec()];
                bounds.extend(cuts);
                bounds.push(b"d".to_vec());
                let snapshot = db.snapshot();
                let mut parts = Vec::new();
                for w in bounds.windows(2) {
                    let opts = ReadOptions {
                        lower_bound: Some(w[0].clone()),
                        upper_bound: Some(w[1].clone()),
                        ..Default::default()
                    };
                    let it = snapshot
                        .iterator_opt(&handle, opts, IteratorMode::Start)
                        .unwrap();
                    parts.extend(drain(it, 1));
                }
                let all = model.scan(cf, &Some(b"a".to_vec()), &Some(b"d".to_vec()), &Mode::Start);
                prop_assert_eq!(parts, all);
            }
            Op::CreateCf(cf) => {
                if model.live.insert(cf) {
                    incarnations[cf] += 1;
                    db.create_cf(CFS[cf]).unwrap();
                } else {
                    prop_assert!(db.create_cf(CFS[cf]).is_err());
                }
            }
            Op::DropCf(cf) => {
                if model.live.remove(&cf) {
                    let old = db.cf_handle(CFS[cf]).unwrap();
                    db.drop_cf(CFS[cf]).unwrap();
                    model.data.retain(|(c, _), _| *c != cf);
                    prop_assert!(db.get(&old, b"a").is_err(), "a dropped handle must fail");
                } else {
                    prop_assert!(db.drop_cf(CFS[cf]).is_err());
                }
            }
            Op::Reopen => {
                kept.clear();
                drop(db);
                db = open(&fs);
                let torn = db.recovery_report().discarded_bytes;
                prop_assert!(
                    torn <= torn_before,
                    "a clean close tore {} bytes",
                    torn - torn_before
                );
                torn_before = torn;
                prop_assert_eq!(&dump(&db), &model);
            }
            Op::Flush => {
                db.flush().unwrap();
                prop_assert_eq!(&dump(&db), &model, "a flush changed what is visible");
            }
            Op::Crash(seed) => {
                kept.clear();
                fs.crash(Torn::Seeded(seed));
                drop(db);
                db = open(&fs);
                torn_before = db.recovery_report().discarded_bytes;
                prop_assert_eq!(&dump(&db), &model, "lost an acknowledged write");
            }
        }
    }
    kept.clear();
    drop(db);
    fs.crash(Torn::None);
    let db = open(&fs);
    prop_assert_eq!(&dump(&db), &model);
    Ok(())
}

proptest! {
    // PROPTEST_CASES raises it for a stress run.
    #![proptest_config(ProptestConfig {
        cases: std::env::var("PROPTEST_CASES").ok().and_then(|v| v.parse().ok()).unwrap_or(512),
        ..ProptestConfig::default()
    })]

    #[test]
    fn the_database_matches_the_model_across_reopens_and_crashes(
        ops in prop::collection::vec(op(), 1..80),
        buffer in prop::sample::select(vec![256usize, 4096, 64 << 20]),
    ) {
        run(ops, buffer)?;
    }
}

/// Eventual durability: after a crash the database holds a prefix of the
/// acknowledged batches, at least up to the last one that was synced.
#[test]
fn eventual_mode_recovers_a_synced_prefix() {
    use rand::{Rng, SeedableRng};
    for seed in 0..200u64 {
        let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
        let fs = FaultFs::new();
        let opts = Options {
            fs: Arc::new(fs.clone()),
            // An interval that never comes: the background syncer runs only
            // when `wal_bytes_per_sync` bytes wait. That is never here for
            // a third of the seeds, and after nearly every write for the
            // rest, racing the writes and the crash.
            durability: Durability::Eventual {
                sync_interval: std::time::Duration::from_secs(3600),
            },
            wal_bytes_per_sync: [1 << 20, 1, 64][seed as usize % 3],
            ..Options::default()
        };
        // A process handle: whatever the syncer is doing when the crash
        // comes fails, rather than reaching the disk that survives it.
        let db = Db::open(
            "/db",
            Options {
                fs: Arc::new(fs.process()),
                ..opts.clone()
            },
        )
        .unwrap();
        let cf = db.default_cf();
        let mut history = vec![BTreeMap::<Vec<u8>, Vec<u8>>::new()];
        let mut synced = 0;
        for i in 0..rng.gen_range(1..40u32) {
            let mut state = history.last().unwrap().clone();
            let key = vec![b'a' + rng.gen_range(0..4u8)];
            let mut batch = WriteBatch::new();
            batch.put(&cf, &key, i.to_le_bytes());
            state.insert(key, i.to_le_bytes().to_vec());
            let sync = rng.gen_bool(0.2);
            db.write_opt(batch, &solistore::WriteOptions { sync })
                .unwrap();
            history.push(state);
            if sync {
                synced = history.len() - 1;
            }
            if rng.gen_bool(0.05) {
                db.sync_wal().unwrap();
                synced = history.len() - 1;
            }
        }
        fs.crash(Torn::Seeded(seed));
        drop(db);
        let db = Db::open(
            "/db",
            Options {
                fs: Arc::new(fs.process()),
                ..opts
            },
        )
        .unwrap();
        let got: BTreeMap<Vec<u8>, Vec<u8>> = db
            .iterator(&db.default_cf(), IteratorMode::Start)
            .unwrap()
            .map(|r| r.map(|(k, v)| (k.to_vec(), v.to_vec())).unwrap())
            .collect();
        let at = history.iter().rposition(|state| *state == got);
        assert!(
            at.is_some_and(|at| at >= synced),
            "seed {seed}: recovered state is not a prefix ≥ {synced}"
        );
    }
}
