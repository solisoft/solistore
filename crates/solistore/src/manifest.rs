//! The MANIFEST: the durable record of the database's structure.
//!
//! It is a log (same framing as the WAL) of edits. `CURRENT` names the live
//! MANIFEST. Every open writes a fresh MANIFEST holding one snapshot edit and
//! switches `CURRENT` to it, so a MANIFEST is never appended to after a
//! crash, when its tail may be torn.
//!
//! ```text
//! edit := tag varint | fields
//!   1 CreateCf   id varint, name bytes
//!   2 DropCf     id varint
//!   3 NextCfId   varint      ids are never reused, dropped ones included
//!   4 NextFile   varint
//!   5 LogNumber  varint      WAL files numbered below this hold nothing live
//!   6 AddFile    level, number, size, smallest bytes, largest bytes,
//!                smallest_seq, largest_seq, entries, tombstone_watermark
//!                (all varints but the keys)
//!   7 DeleteFile level, number
//!   8 AddRangeTombstone  cf, start bytes, end bytes, seq
//!   9 LastSeq    varint      highest sequence number in a table or tombstone
//!  10 RemoveRangeTombstone  seq   no file holds anything it covers any more
//! ```

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use bytes::Bytes;

use crate::coding::{get_bytes, get_varint, get_varint_u32, put_bytes, put_varint};
use crate::error::{Error, Result};
use crate::fs::{Fs, WritableFile};
use crate::log::{encode_record, read_records};
use crate::memtable::RangeTombstone;
use crate::sst::TableProps;
use crate::version::NUM_LEVELS;

pub const DEFAULT_CF: &str = "default";
const CURRENT: &str = "CURRENT";
const CURRENT_TMP: &str = "CURRENT.tmp";

const TAG_CREATE_CF: u64 = 1;
const TAG_DROP_CF: u64 = 2;
const TAG_NEXT_CF_ID: u64 = 3;
const TAG_NEXT_FILE: u64 = 4;
const TAG_LOG_NUMBER: u64 = 5;
const TAG_ADD_FILE: u64 = 6;
const TAG_DELETE_FILE: u64 = 7;
const TAG_ADD_RANGE_TOMBSTONE: u64 = 8;
const TAG_LAST_SEQ: u64 = 9;
const TAG_REMOVE_RANGE_TOMBSTONE: u64 = 10;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Edit {
    CreateCf {
        id: u32,
        name: String,
    },
    DropCf {
        id: u32,
    },
    NextCfId(u32),
    NextFile(u64),
    LogNumber(u64),
    AddFile {
        level: u32,
        number: u64,
        props: TableProps,
    },
    DeleteFile {
        level: u32,
        number: u64,
    },
    AddRangeTombstone(RangeTombstone),
    LastSeq(u64),
    /// Tombstones are identified by their sequence number, which is unique.
    RemoveRangeTombstone {
        seq: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestState {
    pub cfs: BTreeMap<u32, String>,
    pub next_cf_id: u32,
    pub next_file: u64,
    pub log_number: u64,
    /// Sequence numbers up to this one have been used by flushed data. The
    /// WALs replayed at open can only raise it.
    pub last_seq: u64,
    /// Table files per level, by number.
    pub levels: Vec<BTreeMap<u64, TableProps>>,
    pub tombstones: Vec<RangeTombstone>,
}

impl ManifestState {
    pub fn new_database() -> Self {
        let mut state = Self::empty();
        state.cfs.insert(0, DEFAULT_CF.to_string());
        state.next_cf_id = 1;
        state.next_file = 1;
        state
    }

    /// Nothing at all, for replaying edits into.
    pub fn empty() -> Self {
        ManifestState {
            cfs: BTreeMap::new(),
            next_cf_id: 0,
            next_file: 0,
            log_number: 0,
            last_seq: 0,
            levels: vec![BTreeMap::new(); NUM_LEVELS],
            tombstones: Vec::new(),
        }
    }

    pub fn apply(&mut self, edit: &Edit) {
        match edit {
            Edit::CreateCf { id, name } => {
                self.cfs.insert(*id, name.clone());
                self.next_cf_id = self.next_cf_id.max(id + 1);
            }
            Edit::DropCf { id } => {
                self.cfs.remove(id);
            }
            Edit::NextCfId(n) => self.next_cf_id = self.next_cf_id.max(*n),
            Edit::NextFile(n) => self.next_file = self.next_file.max(*n),
            Edit::LogNumber(n) => self.log_number = self.log_number.max(*n),
            Edit::AddFile {
                level,
                number,
                props,
            } => {
                self.levels[*level as usize].insert(*number, props.clone());
                self.next_file = self.next_file.max(number + 1);
            }
            Edit::DeleteFile { level, number } => {
                self.levels[*level as usize].remove(number);
            }
            Edit::AddRangeTombstone(t) => self.tombstones.push(t.clone()),
            Edit::LastSeq(n) => self.last_seq = self.last_seq.max(*n),
            Edit::RemoveRangeTombstone { seq } => self.tombstones.retain(|t| t.seq != *seq),
        }
    }

    /// The edits that rebuild this state from nothing.
    pub fn snapshot(&self) -> Vec<Edit> {
        let mut edits: Vec<Edit> = self
            .cfs
            .iter()
            .map(|(id, name)| Edit::CreateCf {
                id: *id,
                name: name.clone(),
            })
            .collect();
        edits.push(Edit::NextCfId(self.next_cf_id));
        edits.push(Edit::NextFile(self.next_file));
        edits.push(Edit::LogNumber(self.log_number));
        edits.push(Edit::LastSeq(self.last_seq));
        for (level, files) in self.levels.iter().enumerate() {
            for (number, props) in files {
                edits.push(Edit::AddFile {
                    level: level as u32,
                    number: *number,
                    props: props.clone(),
                });
            }
        }
        edits.extend(self.tombstones.iter().cloned().map(Edit::AddRangeTombstone));
        edits
    }

    pub fn allocate_file(&mut self) -> u64 {
        let n = self.next_file;
        self.next_file += 1;
        n
    }
}

pub fn encode_edits(edits: &[Edit]) -> Vec<u8> {
    let mut buf = Vec::new();
    for edit in edits {
        match edit {
            Edit::CreateCf { id, name } => {
                put_varint(&mut buf, TAG_CREATE_CF);
                put_varint(&mut buf, *id as u64);
                put_bytes(&mut buf, name.as_bytes());
            }
            Edit::DropCf { id } => {
                put_varint(&mut buf, TAG_DROP_CF);
                put_varint(&mut buf, *id as u64);
            }
            Edit::NextCfId(n) => {
                put_varint(&mut buf, TAG_NEXT_CF_ID);
                put_varint(&mut buf, *n as u64);
            }
            Edit::NextFile(n) => {
                put_varint(&mut buf, TAG_NEXT_FILE);
                put_varint(&mut buf, *n);
            }
            Edit::LogNumber(n) => {
                put_varint(&mut buf, TAG_LOG_NUMBER);
                put_varint(&mut buf, *n);
            }
            Edit::AddFile {
                level,
                number,
                props,
            } => {
                put_varint(&mut buf, TAG_ADD_FILE);
                put_varint(&mut buf, *level as u64);
                put_varint(&mut buf, *number);
                put_varint(&mut buf, props.size);
                put_bytes(&mut buf, &props.smallest);
                put_bytes(&mut buf, &props.largest);
                put_varint(&mut buf, props.smallest_seq);
                put_varint(&mut buf, props.largest_seq);
                put_varint(&mut buf, props.entries);
                put_varint(&mut buf, props.tombstone_watermark);
            }
            Edit::DeleteFile { level, number } => {
                put_varint(&mut buf, TAG_DELETE_FILE);
                put_varint(&mut buf, *level as u64);
                put_varint(&mut buf, *number);
            }
            Edit::LastSeq(n) => {
                put_varint(&mut buf, TAG_LAST_SEQ);
                put_varint(&mut buf, *n);
            }
            Edit::RemoveRangeTombstone { seq } => {
                put_varint(&mut buf, TAG_REMOVE_RANGE_TOMBSTONE);
                put_varint(&mut buf, *seq);
            }
            Edit::AddRangeTombstone(t) => {
                put_varint(&mut buf, TAG_ADD_RANGE_TOMBSTONE);
                put_varint(&mut buf, t.cf as u64);
                put_bytes(&mut buf, &t.start);
                put_bytes(&mut buf, &t.end);
                put_varint(&mut buf, t.seq);
            }
        }
    }
    buf
}

pub fn decode_edits(payload: &[u8]) -> Result<Vec<Edit>> {
    let corrupt = || Error::Corruption("malformed MANIFEST edit".into());
    let mut input = payload;
    let mut edits = Vec::new();
    while !input.is_empty() {
        let tag = get_varint(&mut input).ok_or_else(corrupt)?;
        edits.push(match tag {
            TAG_CREATE_CF => {
                let id = get_varint_u32(&mut input).ok_or_else(corrupt)?;
                let name = get_bytes(&mut input).ok_or_else(corrupt)?;
                let name = String::from_utf8(name.to_vec()).map_err(|_| corrupt())?;
                Edit::CreateCf { id, name }
            }
            TAG_DROP_CF => Edit::DropCf {
                id: get_varint_u32(&mut input).ok_or_else(corrupt)?,
            },
            TAG_NEXT_CF_ID => Edit::NextCfId(get_varint_u32(&mut input).ok_or_else(corrupt)?),
            TAG_NEXT_FILE => Edit::NextFile(get_varint(&mut input).ok_or_else(corrupt)?),
            TAG_LOG_NUMBER => Edit::LogNumber(get_varint(&mut input).ok_or_else(corrupt)?),
            TAG_ADD_FILE => {
                let mut v = || get_varint(&mut input).ok_or_else(corrupt);
                let (level, number, size) = (v()?, v()?, v()?);
                if level as usize >= NUM_LEVELS {
                    return Err(corrupt());
                }
                let smallest = get_bytes(&mut input).ok_or_else(corrupt)?.to_vec();
                let largest = get_bytes(&mut input).ok_or_else(corrupt)?.to_vec();
                let mut v = || get_varint(&mut input).ok_or_else(corrupt);
                let props = TableProps {
                    size,
                    smallest,
                    largest,
                    smallest_seq: v()?,
                    largest_seq: v()?,
                    entries: v()?,
                    tombstone_watermark: v()?,
                };
                Edit::AddFile {
                    level: level as u32,
                    number,
                    props,
                }
            }
            TAG_DELETE_FILE => {
                let level = get_varint(&mut input).ok_or_else(corrupt)?;
                if level as usize >= NUM_LEVELS {
                    return Err(corrupt());
                }
                Edit::DeleteFile {
                    level: level as u32,
                    number: get_varint(&mut input).ok_or_else(corrupt)?,
                }
            }
            TAG_LAST_SEQ => Edit::LastSeq(get_varint(&mut input).ok_or_else(corrupt)?),
            TAG_REMOVE_RANGE_TOMBSTONE => Edit::RemoveRangeTombstone {
                seq: get_varint(&mut input).ok_or_else(corrupt)?,
            },
            TAG_ADD_RANGE_TOMBSTONE => {
                let cf = get_varint_u32(&mut input).ok_or_else(corrupt)?;
                let start = Bytes::copy_from_slice(get_bytes(&mut input).ok_or_else(corrupt)?);
                let end = Bytes::copy_from_slice(get_bytes(&mut input).ok_or_else(corrupt)?);
                let seq = get_varint(&mut input).ok_or_else(corrupt)?;
                Edit::AddRangeTombstone(RangeTombstone {
                    cf,
                    start,
                    end,
                    seq,
                })
            }
            _ => return Err(corrupt()),
        });
    }
    Ok(edits)
}

pub fn manifest_name(no: u64) -> String {
    format!("MANIFEST-{no:06}")
}

pub fn parse_manifest_name(name: &str) -> Option<u64> {
    name.strip_prefix("MANIFEST-")?.parse().ok()
}

/// The number of the MANIFEST `CURRENT` names, or `None` for a new database.
pub fn current(fs: &dyn Fs, dir: &Path) -> Result<Option<u64>> {
    let path = dir.join(CURRENT);
    if !fs.exists(&path) {
        return Ok(None);
    }
    let text = fs.read(&path)?;
    let text =
        std::str::from_utf8(&text).map_err(|_| Error::Corruption("CURRENT is not UTF-8".into()))?;
    parse_manifest_name(text.trim_end_matches('\n'))
        .map(Some)
        .ok_or_else(|| Error::Corruption(format!("CURRENT names {text:?}, not a MANIFEST")))
}

/// Replay a MANIFEST into the state it describes.
pub fn read(fs: &dyn Fs, dir: &Path, no: u64) -> Result<ManifestState> {
    let data = fs.read(&dir.join(manifest_name(no)))?;
    let file_no = file_no_u32(no)?;
    let log = read_records(&data, file_no);
    if log.records.is_empty() {
        // The snapshot record is synced before CURRENT ever names the file.
        return Err(Error::Corruption(format!(
            "{} holds no valid record",
            manifest_name(no)
        )));
    }
    let mut state = ManifestState::empty();
    for record in log.records {
        for edit in decode_edits(record)? {
            state.apply(&edit);
        }
    }
    state.next_file = state.next_file.max(no + 1);
    Ok(state)
}

pub(crate) fn file_no_u32(no: u64) -> Result<u32> {
    u32::try_from(no).map_err(|_| Error::Corruption(format!("file number {no} out of range")))
}

/// The live MANIFEST, appended to as the structure changes.
pub struct ManifestWriter {
    file: Arc<dyn WritableFile>,
    file_no: u32,
}

impl ManifestWriter {
    /// Write a new MANIFEST numbered `no` holding `state`, and make `CURRENT`
    /// name it. Durable when this returns, directory entries included.
    pub fn install(
        fs: &dyn Fs,
        dir: &Path,
        no: u64,
        state: &ManifestState,
    ) -> Result<ManifestWriter> {
        let file_no = file_no_u32(no)?;
        let file = fs.create(&dir.join(manifest_name(no)))?;
        let mut record = Vec::new();
        encode_record(&mut record, file_no, &encode_edits(&state.snapshot()));
        file.append(&record)?;
        file.sync()?;

        let tmp = dir.join(CURRENT_TMP);
        let current = fs.create(&tmp)?;
        current.append(format!("{}\n", manifest_name(no)).as_bytes())?;
        current.sync()?;
        fs.rename(&tmp, &dir.join(CURRENT))?;
        fs.sync_dir(dir)?;
        Ok(ManifestWriter { file, file_no })
    }

    /// Append edits and fsync them: a structural change is acknowledged only
    /// once durable, whatever the database's write durability.
    pub fn append(&self, edits: &[Edit]) -> Result<()> {
        let mut record = Vec::new();
        encode_record(&mut record, self.file_no, &encode_edits(edits));
        self.file.append(&record)?;
        self.file.sync()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::{FaultFs, Torn};

    #[test]
    fn edits_round_trip() {
        let edits = vec![
            Edit::CreateCf {
                id: 7,
                name: "__keyspaces__".into(),
            },
            Edit::DropCf { id: 3 },
            Edit::NextCfId(8),
            Edit::NextFile(12),
            Edit::LogNumber(9),
            Edit::AddFile {
                level: 3,
                number: 11,
                props: TableProps {
                    size: 4096,
                    smallest: b"smallest".to_vec(),
                    largest: b"largest".to_vec(),
                    smallest_seq: 1,
                    largest_seq: 99,
                    entries: 50,
                    tombstone_watermark: 42,
                },
            },
            Edit::DeleteFile {
                level: 0,
                number: 4,
            },
            Edit::LastSeq(1234),
            Edit::RemoveRangeTombstone { seq: 77 },
            Edit::AddRangeTombstone(RangeTombstone {
                cf: 2,
                start: Bytes::from_static(b"a"),
                end: Bytes::from_static(b"z"),
                seq: 77,
            }),
        ];
        assert_eq!(decode_edits(&encode_edits(&edits)).unwrap(), edits);
    }

    #[test]
    fn a_dropped_id_is_not_reused_after_a_snapshot() {
        let mut state = ManifestState::new_database();
        state.apply(&Edit::CreateCf {
            id: 1,
            name: "a".into(),
        });
        state.apply(&Edit::CreateCf {
            id: 2,
            name: "b".into(),
        });
        state.apply(&Edit::DropCf { id: 2 });
        let mut rebuilt = ManifestState::empty();
        for edit in state.snapshot() {
            rebuilt.apply(&edit);
        }
        assert_eq!(rebuilt.next_cf_id, 3);
        assert_eq!(rebuilt, state);
    }

    #[test]
    fn an_installed_manifest_survives_a_crash_with_its_appended_edits() {
        let fs = FaultFs::new();
        let dir = Path::new("/db");
        let mut state = ManifestState::new_database();
        let no = state.allocate_file();
        let writer = ManifestWriter::install(&fs, dir, no, &state).unwrap();
        writer
            .append(&[Edit::CreateCf {
                id: 1,
                name: "c".into(),
            }])
            .unwrap();
        fs.crash(Torn::Seeded(3));

        let no = current(&fs, dir).unwrap().unwrap();
        let read_back = read(&fs, dir, no).unwrap();
        assert_eq!(read_back.cfs.get(&1).map(String::as_str), Some("c"));
        assert_eq!(read_back.next_cf_id, 2);
    }
}
