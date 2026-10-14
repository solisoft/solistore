//! Random operation sequences, checked against a `BTreeMap` model.
//!
//! Every sequence runs on [`FaultFs`] and mixes writes, reads, scans in both
//! directions, column family creation and drops, clean reopens and crashes
//! (with torn tails). In `Full` durability every acknowledged write must
//! survive a crash, so the database must equal the model after each one.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Arc;

use proptest::prelude::*;
use solistore::fs::{FaultFs, Torn};
use solistore::{Db, Direction, Durability, IteratorMode, Options, ReadOptions, WriteBatch};

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
    Scan {
        cf: usize,
        lower: Option<Vec<u8>>,
        upper: Option<Vec<u8>>,
        mode: Mode,
    },
    CreateCf(usize),
    DropCf(usize),
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
        3 => (0..CFS.len(), prop::option::of(key()), prop::option::of(key()), mode())
            .prop_map(|(cf, lower, upper, mode)| Op::Scan { cf, lower, upper, mode }),
        1 => (1..CFS.len()).prop_map(Op::CreateCf),
        1 => (1..CFS.len()).prop_map(Op::DropCf),
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
        fs: Arc::new(fs.clone()),
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
    let mut model = Model::new();

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
                };
                let it_mode = match &mode {
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
                };
                let got: Vec<_> = db
                    .iterator_opt(&handle, opts, it_mode)
                    .unwrap()
                    .map(|r| r.map(|(k, v)| (k.to_vec(), v.to_vec())).unwrap())
                    .collect();
                prop_assert_eq!(got, model.scan(cf, &lower, &upper, &mode), "{:?}", mode);
            }
            Op::CreateCf(cf) => {
                if model.live.insert(cf) {
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
                drop(db);
                db = open(&fs);
                prop_assert_eq!(
                    db.recovery_report().discarded_bytes,
                    0,
                    "a clean close tears nothing"
                );
                prop_assert_eq!(&dump(&db), &model);
            }
            Op::Flush => {
                db.flush().unwrap();
                prop_assert_eq!(&dump(&db), &model, "a flush changed what is visible");
            }
            Op::Crash(seed) => {
                fs.crash(Torn::Seeded(seed));
                drop(db);
                db = open(&fs);
                prop_assert_eq!(&dump(&db), &model, "lost an acknowledged write");
            }
        }
    }
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
            // Long enough that the background syncer never runs mid-test.
            durability: Durability::Eventual {
                sync_interval: std::time::Duration::from_secs(3600),
            },
            ..Options::default()
        };
        let db = Db::open("/db", opts.clone()).unwrap();
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
        let db = Db::open("/db", opts).unwrap();
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
