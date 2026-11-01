//! Compaction to the bottom of the tree writes sequence 0 for keys nothing
//! older can shadow. These are the cases where a zeroed sequence would turn
//! into wrong answers if the conditions for it were wrong.

use solistore::{Db, Options};

fn key(i: u32) -> [u8; 4] {
    i.to_be_bytes()
}

fn small() -> Options {
    Options {
        write_buffer_size: 8 << 10,
        target_file_size: 32 << 10,
        ..Options::default()
    }
}

fn compact_all(db: &Db) {
    db.flush().unwrap();
    let cf = db.default_cf();
    db.compact_range(&cf, None, None).unwrap();
}

/// A key written again after a range delete that covers it keeps its real
/// sequence: reads compare every tombstone with the entry, and at sequence 0
/// the older tombstone would hide the newer write.
#[test]
fn a_write_after_a_range_delete_survives_compaction_to_the_bottom() {
    let dir = tempfile::tempdir().unwrap();
    let check = |db: &Db, when: &str| {
        let cf = db.default_cf();
        for i in 0..1000u32 {
            let want = !(100..200).contains(&i) || (150..160).contains(&i);
            assert_eq!(
                db.get(&cf, key(i)).unwrap().is_some(),
                want,
                "{when}: key {i}"
            );
        }
        let n = db
            .iterator(&cf, solistore::IteratorMode::Start)
            .unwrap()
            .count();
        assert_eq!(n, 910, "{when}: keys iterated");
    };
    {
        let db = Db::open(dir.path(), small()).unwrap();
        let cf = db.default_cf();
        for i in 0..1000u32 {
            db.put(&cf, key(i), [i as u8; 40]).unwrap();
        }
        compact_all(&db);
        let mut batch = solistore::WriteBatch::new();
        batch.delete_range(&cf, key(100), key(200));
        db.write(batch).unwrap();
        for i in 150..160u32 {
            db.put(&cf, key(i), b"after the delete").unwrap();
        }
        compact_all(&db);
        check(&db, "after compaction");
        compact_all(&db);
        check(&db, "after a second compaction");
    }
    let db = Db::open(dir.path(), small()).unwrap();
    check(&db, "after reopening");
    // A later tombstone still deletes what it covers, zeroed or not.
    let cf = db.default_cf();
    let mut batch = solistore::WriteBatch::new();
    batch.delete_range(&cf, key(0), key(50));
    db.write(batch).unwrap();
    compact_all(&db);
    for i in 0..50u32 {
        assert!(db.get(&cf, key(i)).unwrap().is_none(), "key {i}");
    }
    assert!(db.get(&cf, key(155)).unwrap().is_some());
    assert!(db.get(&cf, key(50)).unwrap().is_some());
}

/// A newer version above a zeroed one wins, and so does a delete, before
/// and after they are compacted together and after reopening.
#[test]
fn newer_versions_win_over_zeroed_ones() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = Db::open(dir.path(), small()).unwrap();
        let cf = db.default_cf();
        for i in 0..2000u32 {
            db.put(&cf, key(i), b"v1").unwrap();
        }
        compact_all(&db);
        for i in (0..2000u32).step_by(3) {
            db.put(&cf, key(i), b"v2").unwrap();
        }
        for i in (1..2000u32).step_by(3) {
            db.delete(&cf, key(i)).unwrap();
        }
        db.flush().unwrap();
        let check = |db: &Db, when: &str| {
            for i in 0..2000u32 {
                let got = db.get(&cf, key(i)).unwrap();
                let want: Option<&[u8]> = match i % 3 {
                    0 => Some(b"v2"),
                    1 => None,
                    _ => Some(b"v1"),
                };
                assert_eq!(got.as_deref(), want, "{when}: key {i}");
            }
        };
        check(&db, "newer versions in level 0");
        compact_all(&db);
        check(&db, "compacted together");
    }
    let db = Db::open(dir.path(), small()).unwrap();
    let cf = db.default_cf();
    for i in 0..2000u32 {
        let got = db.get(&cf, key(i)).unwrap();
        let want: Option<&[u8]> = match i % 3 {
            0 => Some(b"v2"),
            1 => None,
            _ => Some(b"v1"),
        };
        assert_eq!(got.as_deref(), want, "after reopening: key {i}");
    }
}

/// A database written with fixed level targets, reopened with dynamic ones
/// and written more: every read still sees the newest write, and once
/// writes stop the upper levels drain into the bottom, which then holds
/// most of the data.
#[test]
fn a_fixed_layout_drains_into_the_bottom_under_dynamic_targets() {
    use std::collections::BTreeMap;
    use std::time::{Duration, Instant};
    let dir = tempfile::tempdir().unwrap();
    let opts = |dynamic: bool| Options {
        write_buffer_size: 16 << 10,
        target_file_size: 8 << 10,
        max_bytes_for_level_base: 64 << 10,
        level_multiplier: 4,
        level0_compaction_trigger: 2,
        level_compaction_dynamic_level_bytes: dynamic,
        ..Options::default()
    };
    let mut model: BTreeMap<u32, Option<Vec<u8>>> = BTreeMap::new();
    let write = |db: &Db, model: &mut BTreeMap<u32, Option<Vec<u8>>>, round: u32| {
        let cf = db.default_cf();
        for chunk in (0..20_000u32).collect::<Vec<_>>().chunks(400) {
            let mut batch = solistore::WriteBatch::new();
            for &i in chunk {
                let k = (i * 7919 + round * 13) % 20_000;
                if (k + round).is_multiple_of(6) {
                    batch.delete(&cf, key(k));
                    model.insert(k, None);
                } else {
                    let v = format!("{k}:{round}").into_bytes();
                    batch.put(&cf, key(k), &v);
                    model.insert(k, Some(v));
                }
            }
            db.write(batch).unwrap();
        }
    };
    let check = |db: &Db, model: &BTreeMap<u32, Option<Vec<u8>>>, when: &str| {
        let cf = db.default_cf();
        for (k, v) in model.iter().step_by(3) {
            assert_eq!(
                db.get(&cf, key(*k)).unwrap().map(|b| b.to_vec()).as_ref(),
                v.as_ref(),
                "{when}: key {k}"
            );
        }
    };
    let settle = |db: &Db| {
        let deadline = Instant::now() + Duration::from_secs(60);
        let mut last = (u64::MAX, Vec::new());
        while Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(200));
            let now = (db.stats().compactions, db.files_per_level());
            if now == last {
                return now.1;
            }
            last = now;
        }
        last.1
    };
    {
        let db = Db::open(dir.path(), opts(false)).unwrap();
        for round in 0..3 {
            write(&db, &mut model, round);
        }
        db.flush().unwrap();
        let levels = settle(&db);
        assert!(
            levels[1] + levels[2] > 0,
            "fixed targets fill the upper levels: {levels:?}"
        );
        assert_eq!(levels[6], 0, "and leave the bottom empty: {levels:?}");
        check(&db, &model, "fixed");
    }
    let db = Db::open(dir.path(), opts(true)).unwrap();
    check(&db, &model, "reopened");
    for round in 3..6 {
        write(&db, &mut model, round);
        check(&db, &model, "while draining");
    }
    db.flush().unwrap();
    let levels = settle(&db);
    check(&db, &model, "drained");
    let (upper, bottom): (usize, usize) = (levels[1..4].iter().sum(), levels[6]);
    assert_eq!(upper, 0, "levels 1-3 drained: {levels:?}");
    assert!(bottom > 0, "the bottom holds the data: {levels:?}");
}
