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
