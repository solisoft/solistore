//! `Db::ingest`: a whole column family loaded as bottom-level tables.

use solistore::{Db, Error, IteratorMode, Options, WriteBatch};

fn key(i: u32) -> Vec<u8> {
    format!("doc:{i:08}").into_bytes()
}

fn value(i: u32) -> Vec<u8> {
    format!("value {i} ")
        .repeat(1 + (i % 7) as usize)
        .into_bytes()
}

fn entries(n: u32) -> impl Iterator<Item = Result<(Vec<u8>, Vec<u8>), String>> {
    (0..n).map(|i| Ok((key(i), value(i))))
}

fn small() -> Options {
    Options {
        write_buffer_size: 64 << 10,
        target_file_size: 256 << 10,
        ..Options::default()
    }
}

/// Everything ingested reads back, by key and in order, before and after a
/// reopen, and nothing went through the WAL or a flush.
#[test]
fn an_ingested_column_family_reads_back_whole() {
    let dir = tempfile::tempdir().unwrap();
    let n = 50_000;
    {
        let db = Db::open(dir.path(), small()).unwrap();
        let cf = db.create_cf("bulk").unwrap();
        let report = db.ingest(&cf, entries(n), 4).unwrap();
        assert_eq!(report.entries, n as u64);
        assert!(report.files > 1, "{report:?}");
        assert_eq!(db.stats().flushes, 0, "no flush");
        assert_eq!(db.memtable_bytes(), 0, "no memtable");
        for i in (0..n).step_by(997) {
            assert_eq!(db.get(&cf, key(i)).unwrap().as_deref(), Some(&value(i)[..]));
        }
        assert_eq!(
            db.iterator(&cf, IteratorMode::Start).unwrap().count(),
            n as usize
        );
    }
    let db = Db::open(dir.path(), small()).unwrap();
    let cf = db.cf_handle("bulk").unwrap();
    let mut expected = 0u32;
    for item in db.iterator(&cf, IteratorMode::Start).unwrap() {
        let (k, v) = item.unwrap();
        assert_eq!(&k[..], &key(expected)[..]);
        assert_eq!(&v[..], &value(expected)[..]);
        expected += 1;
    }
    assert_eq!(expected, n);
}

/// Writes after an ingest win over the ingested entries (sequence 0), and
/// deletes hide them, through flushes and compactions.
#[test]
fn later_writes_and_deletes_win_over_ingested_entries() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), small()).unwrap();
    let cf = db.create_cf("bulk").unwrap();
    db.ingest(&cf, entries(10_000), 2).unwrap();
    for i in (0..10_000).step_by(3) {
        db.put(&cf, key(i), b"new").unwrap();
    }
    for i in (1..10_000).step_by(3) {
        db.delete(&cf, key(i)).unwrap();
    }
    let mut batch = WriteBatch::new();
    batch.delete_range(&cf, key(9_000), key(9_500));
    db.write(batch).unwrap();
    let check = |db: &Db, when: &str| {
        let cf = db.cf_handle("bulk").unwrap();
        for i in 0..10_000u32 {
            let got = db.get(&cf, key(i)).unwrap();
            let want: Option<Vec<u8>> = if (9_000..9_500).contains(&i) || i % 3 == 1 {
                None
            } else if i % 3 == 0 {
                Some(b"new".to_vec())
            } else {
                Some(value(i))
            };
            assert_eq!(got.map(|v| v.to_vec()), want, "{when}: key {i}");
        }
    };
    check(&db, "before compaction");
    db.compact_range(&cf, None, None).unwrap();
    check(&db, "after compaction");
    drop(db);
    let db = Db::open(dir.path(), small()).unwrap();
    check(&db, "after reopening");
}

/// An ingest into a column family that holds anything, or with keys out of
/// order, is refused and leaves nothing behind.
#[test]
fn an_ingest_is_refused_unless_the_family_is_untouched_and_keys_ascend() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), small()).unwrap();

    let used = db.create_cf("used").unwrap();
    db.put(&used, b"x", b"1").unwrap();
    assert!(matches!(
        db.ingest(&used, entries(10), 1),
        Err(Error::InvalidArgument(_))
    ));

    // A delete is enough: it would shadow entries at sequence 0.
    let deleted = db.create_cf("deleted").unwrap();
    db.delete(&deleted, b"x").unwrap();
    assert!(db.ingest(&deleted, entries(10), 1).is_err());

    let unordered = db.create_cf("unordered").unwrap();
    let bad = vec![
        Ok::<_, String>((b"b".to_vec(), b"1".to_vec())),
        Ok((b"a".to_vec(), b"2".to_vec())),
    ];
    assert!(db.ingest(&unordered, bad, 1).is_err());
    assert_eq!(
        db.iterator(&unordered, IteratorMode::Start)
            .unwrap()
            .count(),
        0
    );

    let failing = db.create_cf("failing").unwrap();
    let source = (0..10u32).map(|i| {
        if i == 5 {
            Err("source broke".to_string())
        } else {
            Ok((key(i), value(i)))
        }
    });
    let err = db.ingest(&failing, source, 2).unwrap_err().to_string();
    assert!(err.contains("source broke"), "{err}");
    assert_eq!(
        db.iterator(&failing, IteratorMode::Start).unwrap().count(),
        0
    );
}
