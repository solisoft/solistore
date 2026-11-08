//! What point reads hand out.

use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};
use solistore::{Bytes, Db, Options, WriteBatch};

/// A small value read back owns its bytes: holding it must not keep its
/// decoded block (or the memtable's arena chunk) alive. A `multi_get` of
/// 5,000 documents otherwise pinned thousands of blocks past their eviction.
#[test]
fn a_small_value_does_not_pin_its_block_or_arena() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), Options::default()).unwrap();
    let cf = db.default_cf();
    for i in 0..200u32 {
        db.put(&cf, format!("doc:{i:05}"), format!("value {i}").repeat(10))
            .unwrap();
    }
    let big = vec![7u8; 64 * 1024];
    db.put(&cf, b"big", &big).unwrap();

    // From the memtable.
    let v = db.get(&cf, b"doc:00042").unwrap().unwrap();
    assert!(v.is_unique(), "a memtable value is copied out of its arena");

    // From a table.
    db.flush().unwrap();
    let v = db.get(&cf, b"doc:00042").unwrap().unwrap();
    assert_eq!(v, "value 42".repeat(10).as_bytes());
    assert!(v.is_unique(), "a table value is copied out of its block");
    let got = db
        .multi_get(&cf, &[&b"doc:00001"[..], &b"doc:00150"[..]])
        .unwrap();
    assert!(got.iter().all(|v| v.as_ref().unwrap().is_unique()));

    // A value that is its whole block is handed out as it is.
    let v = db.get(&cf, b"big").unwrap().unwrap();
    assert_eq!(&v[..], &big[..]);
}

/// A value larger than any block size is written, flushed, compacted and
/// read back whole. A 70 MB compressible one used to be refused on read (a
/// 64 MiB cap the writer did not share), which poisoned the database during
/// the compaction that read it.
#[test]
fn a_very_large_value_survives_flush_compaction_and_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let big: Vec<u8> = (0..70u32 << 20).map(|i| (i % 251) as u8).collect();
    {
        let db = Db::open(dir.path(), Options::default()).unwrap();
        let cf = db.default_cf();
        db.put(&cf, b"a", b"before").unwrap();
        db.put(&cf, b"big", &big).unwrap();
        db.put(&cf, b"z", b"after").unwrap();
        db.flush().unwrap();
        db.compact_range(&cf, None, None).unwrap();
        assert_eq!(&db.get(&cf, b"big").unwrap().unwrap()[..], &big[..]);
    }
    let db = Db::open(dir.path(), Options::default()).unwrap();
    let cf = db.default_cf();
    assert_eq!(&db.get(&cf, b"big").unwrap().unwrap()[..], &big[..]);
    assert_eq!(db.get(&cf, b"z").unwrap().as_deref(), Some(&b"after"[..]));
}

/// `multi_get` and `multi_get_with` read what `get` reads, key for key and
/// in the order asked, from memtables, level 0 and deeper levels at once.
/// SoliDB-shaped keys (an 8-byte prefix, `doc:` and a number), only even
/// numbers written so that odd ones fall between them and between blocks;
/// batches with repeats, absent keys and runs of neighbours (one block for
/// several keys); overwrites, deletes, a range delete, values larger than
/// half a block, and an older snapshot.
#[test]
fn multi_get_reads_what_get_reads() {
    let dir = tempfile::tempdir().unwrap();
    let opts = Options {
        write_buffer_size: 256 << 10,
        target_file_size: 128 << 10,
        max_bytes_for_level_base: 1 << 20,
        level_multiplier: 4,
        ..Options::default()
    };
    let db = Db::open(dir.path(), opts).unwrap();
    let cf = db.create_cf("shared").unwrap();
    let key = |n: u64| {
        let mut k = vec![0, 0, 0, 1, 0, 0, 0, 7];
        k.extend_from_slice(format!("doc:{n:020}").as_bytes());
        k
    };
    let value = |n: u64, round: u64| {
        let len = if n.is_multiple_of(998) {
            3000
        } else {
            (n % 400) as usize + 30
        };
        format!(
            r#"{{"_key":{n},"round":{round},"bio":"{}"}}"#,
            "x".repeat(len)
        )
        .into_bytes()
    };
    let mut rng = StdRng::seed_from_u64(7);
    let mut order: Vec<u64> = (0..4000).map(|i| i * 2).collect();
    order.shuffle(&mut rng);
    for chunk in order.chunks(50) {
        let mut batch = WriteBatch::new();
        for &n in chunk {
            batch.put(&cf, key(n), value(n, 0));
        }
        db.write(batch).unwrap();
    }
    db.flush().unwrap();
    db.compact_range(&cf, None, None).unwrap();
    // Part of it again, flushed to level 0; then more in the memtable.
    for n in (0..8000).step_by(6) {
        db.put(&cf, key(n), value(n, 1)).unwrap();
    }
    db.flush().unwrap();
    let snapshot = db.snapshot();
    for n in (0..8000).step_by(10) {
        db.put(&cf, key(n), value(n, 2)).unwrap();
    }
    for n in (0..8000).step_by(14) {
        db.delete(&cf, key(n)).unwrap();
    }
    let mut batch = WriteBatch::new();
    batch.delete_range(&cf, key(3000), key(3400));
    db.write(batch).unwrap();
    let levels = db.files_per_level();
    assert!(
        levels[0] > 0 && levels[1..].iter().sum::<usize>() > 0,
        "{levels:?}"
    );

    for round in 0..400 {
        let len = rng.gen_range(0..40u64);
        let mut keys: Vec<Vec<u8>> = if round % 3 == 0 {
            let start = rng.gen_range(0..8000);
            (start..start + len).map(key).collect()
        } else {
            (0..len).map(|_| key(rng.gen_range(0..8100))).collect()
        };
        if keys.len() > 2 {
            keys.push(keys[1].clone());
        }
        let want: Vec<Option<Bytes>> = keys.iter().map(|k| db.get(&cf, k).unwrap()).collect();
        assert_eq!(db.multi_get(&cf, &keys).unwrap(), want, "round {round}");
        let mut lent = Vec::new();
        db.multi_get_with(&cf, &keys, |i, value| {
            assert_eq!(i, lent.len(), "in the order of the keys");
            lent.push(value.map(Bytes::copy_from_slice));
        })
        .unwrap();
        assert_eq!(lent, want, "round {round}, borrowed");
        let want: Vec<Option<Bytes>> = keys.iter().map(|k| snapshot.get(&cf, k).unwrap()).collect();
        assert_eq!(
            snapshot.multi_get(&cf, &keys).unwrap(),
            want,
            "round {round}, snapshot"
        );
    }
    // Large values come back whole, and the small ones own their bytes
    // (both only written in the first round).
    let got = db.multi_get(&cf, &[key(998), key(2)]).unwrap();
    assert_eq!(got[0].as_deref(), Some(&value(998, 0)[..]));
    assert!(got[1].as_ref().unwrap().is_unique());
}
