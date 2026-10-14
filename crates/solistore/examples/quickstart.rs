//! The example from the README, kept compiling.

use std::time::Duration;

use solistore::{Db, Direction, Durability, IteratorMode, Options, WriteBatch};

fn main() -> solistore::Result<()> {
    let dir = tempfile::tempdir()?;

    // Full durability is the default: every commit is fsynced before it
    // returns, and concurrent commits share one fsync. Eventual mode
    // acknowledges once the write reaches the OS and fsyncs in the background.
    let db = Db::open(
        dir.path(),
        Options {
            durability: Durability::Eventual {
                sync_interval: Duration::from_millis(100),
            },
            ..Options::default()
        },
    )?;

    // Column families are cheap: one MANIFEST record, no file of their own.
    let users = db.create_cf("users")?;
    let index = db.create_cf("users_by_email")?;

    // A batch is atomic across column families: readers see all of it or none.
    let mut batch = WriteBatch::new();
    batch.put(
        &users,
        b"u:1",
        br#"{"name":"Ada","email":"ada@example.com"}"#,
    );
    batch.put(&index, b"ada@example.com", b"u:1");
    batch.put(
        &users,
        b"u:2",
        br#"{"name":"Linus","email":"linus@example.com"}"#,
    );
    batch.put(&index, b"linus@example.com", b"u:2");
    db.write(batch)?;

    // Point reads return `Bytes`: a block-cache hit is not copied.
    let id = db.get(&index, b"ada@example.com")?.expect("indexed");
    assert_eq!(&id[..], b"u:1");

    // Bounded iterators, both directions. `From(k, Reverse)` is seek-for-prev.
    for item in db.iterator(&users, IteratorMode::From(b"u:", Direction::Forward))? {
        let (key, value) = item?;
        println!(
            "{} = {}",
            String::from_utf8_lossy(&key),
            String::from_utf8_lossy(&value)
        );
    }

    // Range deletes drop whole table files without rewriting them.
    let mut batch = WriteBatch::new();
    batch.delete_range(&users, b"u:", b"u;");
    db.write(batch)?;
    assert!(db.get(&users, b"u:1")?.is_none());

    // A point-in-time copy made of hard links.
    db.checkpoint(dir.path().join("backup"))?;
    Ok(())
}
