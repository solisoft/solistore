//! What point reads hand out.

use solistore::{Db, Options};

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
