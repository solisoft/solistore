//! Entry points for the fuzz targets in `fuzz/` (feature `fuzzing`, not
//! part of the API). Each takes arbitrary bytes and must return without
//! panicking or allocating out of proportion to its input, whatever the
//! bytes: corrupt input is an error, never a crash.
//!
//! Checksums guard every record and block on disk, and a fuzzer does not
//! forge CRC32C: fed raw files, it would never get past the framing. The
//! targets below therefore also hand the decoders payloads that are framed
//! correctly but say anything, as a bug in a writer, a checksum collision
//! or a file from another program would.

use std::path::Path;
use std::sync::Arc;

use bytes::Bytes;

use crate::fs::{FaultFs, Fs};
use crate::{batch, key, log, manifest, sst};

/// `data` cut into pieces, each prefixed by its length in one byte.
fn pieces(mut data: &[u8]) -> Vec<&[u8]> {
    let mut out = Vec::new();
    while let Some((&len, rest)) = data.split_first() {
        let len = (len as usize).min(rest.len());
        out.push(&rest[..len]);
        data = &rest[len..];
    }
    out
}

/// WAL and MANIFEST framing: records, header, end record, torn tails.
pub fn log_records(data: &[u8]) {
    let Some((no, body)) = data.split_first_chunk::<4>() else {
        return;
    };
    let log = log::read_records(body, u32::from_le_bytes(*no));
    assert!(log.discarded <= body.len());
    for record in log.records {
        assert!(record.len() <= body.len());
    }
}

/// A WAL record's payload: a write batch.
pub fn write_batch(data: &[u8]) {
    // Applied to a memtable, as replaying the WAL does.
    if let Ok(batch) = batch::decode(data) {
        let mem = crate::memtable::Memtable::new(1);
        for (seq, op) in (batch.first_seq..).zip(&batch.ops) {
            match *op {
                batch::BatchOp::Put { cf, key, value } => {
                    mem.insert_entry(cf, key, seq, key::Kind::Put, value)
                }
                batch::BatchOp::Delete { cf, key } => {
                    mem.insert_entry(cf, key, seq, key::Kind::Delete, b"")
                }
                batch::BatchOp::DeleteRange { cf, start, end } => {
                    mem.insert_range_tombstone(cf, start, end, seq)
                }
            }
        }
        let _ = mem.get(0, b"k", u64::MAX);
    }
}

/// MANIFEST records' payloads: the new database's snapshot, then the
/// input's pieces, replayed and checked as an open does.
pub fn version_edits(data: &[u8]) {
    let snapshot = manifest::encode_edits(&manifest::ManifestState::new_database().snapshot());
    let records = std::iter::once(&snapshot[..]).chain(pieces(data));
    if let Ok(state) = manifest::replay(records) {
        let _ = state.snapshot();
    }
}

/// A decoded table block: every iteration and seek.
pub fn block(data: &[u8]) {
    let Some((target_len, rest)) = data.split_first() else {
        return;
    };
    let (target, body) = rest.split_at((*target_len as usize).min(rest.len()));
    let Ok(block) = sst::block::Block::new(Bytes::copy_from_slice(body)) else {
        return;
    };
    let mut it = block.iter();
    it.seek_to_first();
    let mut n = 0;
    while it.valid() && n < 10_000 {
        let _ = (it.key(), it.value());
        it.next();
        n += 1;
    }
    it.seek_to_last();
    let mut n = 0;
    while it.valid() && n < 10_000 {
        it.prev();
        n += 1;
    }
    it.seek(target);
    it.seek_for_prev(target);
    let _ = it.status();
    // The point read's search, in place, agrees with a fresh iterator's.
    let mut fresh = block.iter();
    fresh.seek(target);
    let mut key = Vec::new();
    match block.seek_entry(target, &mut key) {
        Ok(Some(entry)) => {
            assert!(
                fresh.valid(),
                "seek_entry found an entry the iterator did not"
            );
            assert_eq!(key, fresh.key());
            let (start, end) = entry.value;
            assert_eq!(&block.bytes()[start..end], &fresh.value()[..]);
        }
        Ok(None) => assert!(!fresh.valid() && fresh.status().is_ok()),
        Err(_) => assert!(fresh.status().is_err()),
    }
}

/// A bloom filter, probed with hashes taken from the input.
pub fn bloom(data: &[u8]) {
    let Some((hash, filter)) = data.split_first_chunk::<8>() else {
        return;
    };
    let _ = sst::bloom::may_contain(filter, u64::from_le_bytes(*hash));
    let _ = sst::bloom::may_contain(filter, sst::bloom::hash(filter));
}

/// A whole table file: footer, index, filter, then every block it points
/// to, forward and backward, and point reads.
pub fn table(data: &[u8]) {
    let fs = FaultFs::new();
    let path = Path::new("/t/000001.sst");
    let file = fs.create(path).expect("in memory");
    file.append(data).expect("in memory");
    let Ok(reader) = sst::TableReader::open(fs.open_random(path).expect("in memory")) else {
        return;
    };
    let reader = Arc::new(reader);
    use crate::iter::InternalIterator;
    let mut it = sst::TableIter::new(reader.clone());
    it.seek_to_last();
    let mut n = 0;
    while it.valid() && n < 10_000 {
        it.prev();
        n += 1;
    }
    it.seek_ge(&key::first_of(0, b""));
    let mut n = 0;
    while it.valid() && n < 10_000 {
        // Read back what the iterator found, through the bloom filter and
        // the index.
        if let Some(p) = key::parse(it.key()) {
            let _ = reader.get(p.cf, p.user_key, p.seq);
        }
        it.next();
        n += 1;
    }
    let _ = it.status();
    let _ = reader.get(0, b"k", u64::MAX);
    let _ = reader.approximate_offset_of(&key::first_of(1, b"k"));
}

/// A database whose MANIFEST and WAL hold well-framed records saying
/// anything: open it, read it, write to it. The input's pieces go to the
/// MANIFEST (after a new database's snapshot) or to the WAL by their first
/// byte's low bit.
pub fn open(data: &[u8]) {
    let fs = Arc::new(FaultFs::new());
    let dir = Path::new("/db");
    fs.create_dir_all(dir).expect("in memory");
    let (manifest_no, wal_no) = (1u64, 2u64);
    let mut edits = manifest::ManifestState::new_database().snapshot();
    edits.push(manifest::Edit::LogNumber(wal_no));
    let mut manifest_log = Vec::new();
    log::encode_record(&mut manifest_log, 1, &manifest::encode_edits(&edits));
    let mut wal_log = Vec::new();
    log::encode_header(&mut wal_log, 2, 0);
    for piece in pieces(data) {
        match piece.split_first() {
            Some((tag, payload)) if tag & 1 == 0 => {
                log::encode_record(&mut manifest_log, 1, payload)
            }
            Some((_, payload)) => log::encode_record(&mut wal_log, 2, payload),
            None => {}
        }
    }
    let write = |name: String, bytes: &[u8]| {
        let file = fs.create(&dir.join(name)).expect("in memory");
        file.append(bytes).expect("in memory");
    };
    write(manifest::manifest_name(manifest_no), &manifest_log);
    write(crate::db::wal_name(wal_no), &wal_log);
    write(
        "CURRENT".into(),
        format!("{}\n", manifest::manifest_name(manifest_no)).as_bytes(),
    );

    let opts = crate::Options {
        fs: fs.clone(),
        create_if_missing: false,
        ..crate::Options::default()
    };
    let Ok(db) = crate::Db::open(dir, opts) else {
        return;
    };
    let cf = db.default_cf();
    if let Ok(it) = db.iterator(&cf, crate::IteratorMode::Start) {
        for item in it.take(10_000) {
            if item.is_err() {
                break;
            }
        }
    }
    let _ = db.get(&cf, b"k");
    let mut batch = crate::WriteBatch::new();
    batch.put(&cf, b"k", b"v");
    let _ = db.write(batch);
    let _ = db.flush();
}
