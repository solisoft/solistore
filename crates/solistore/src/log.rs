//! Record framing shared by the WAL and the MANIFEST.
//!
//! ```text
//! record := crc32c u32 LE | len u32 LE | file_no u32 LE | type u8 | payload[len]
//! ```
//!
//! The checksum covers everything after itself, the length included, so a
//! torn length cannot send the reader off into the payload. `file_no` is the
//! number of the file the record was written to: a record copied from (or
//! left behind by) another file is rejected even if its checksum holds.
//!
//! Recovery is point-in-time: the reader stops at the first record that is
//! short or fails its checksum and ignores everything after it. A write cut
//! short by a crash can leave a hole followed by intact bytes (the disk
//! persisted a later page of the unsynced tail but not an earlier one); no
//! record in that region was acknowledged, so dropping it is correct. What
//! was dropped is reported, never silently.
//!
//! A log that was closed on purpose (a clean shutdown, a WAL rotation) ends
//! with an end record (type 2, empty). Anything after it — the old bytes of
//! a reused file — is not counted as dropped.

use crate::coding::{get_u32_le, get_u8};

pub const HEADER_LEN: usize = 13;
pub const TYPE_FULL: u8 = 1;
pub const TYPE_END: u8 = 2;

/// Append the record that closes a log.
pub fn encode_end(buf: &mut Vec<u8>, file_no: u32) {
    let start = buf.len();
    buf.extend_from_slice(&[0; 4]);
    buf.extend_from_slice(&0u32.to_le_bytes());
    buf.extend_from_slice(&file_no.to_le_bytes());
    buf.push(TYPE_END);
    let crc = crc32c::crc32c(&buf[start + 4..]);
    buf[start..start + 4].copy_from_slice(&crc.to_le_bytes());
}

/// Append one framed record to `buf`.
pub fn encode_record(buf: &mut Vec<u8>, file_no: u32, payload: &[u8]) {
    encode_record_parts(buf, file_no, &[payload]);
}

/// Append one framed record whose payload is `parts` concatenated, without
/// first copying them together (a batch's header is only known at commit).
pub fn encode_record_parts(buf: &mut Vec<u8>, file_no: u32, parts: &[&[u8]]) {
    let total: usize = parts.iter().map(|p| p.len()).sum();
    let len = u32::try_from(total).expect("a log record is limited to 4 GiB");
    let start = buf.len();
    buf.reserve(HEADER_LEN + total);
    buf.extend_from_slice(&[0; 4]);
    buf.extend_from_slice(&len.to_le_bytes());
    buf.extend_from_slice(&file_no.to_le_bytes());
    buf.push(TYPE_FULL);
    for part in parts {
        buf.extend_from_slice(part);
    }
    let crc = crc32c::crc32c(&buf[start + 4..]);
    buf[start..start + 4].copy_from_slice(&crc.to_le_bytes());
}

/// The records of one log file, and where reading stopped.
pub struct Records<'a> {
    pub records: Vec<&'a [u8]>,
    /// Bytes after the last good record (0 for a clean file).
    pub discarded: usize,
}

pub fn read_records(data: &[u8], file_no: u32) -> Records<'_> {
    let mut records = Vec::new();
    let mut offset = 0;
    while offset < data.len() {
        match decode_at(&data[offset..], file_no) {
            Some((TYPE_FULL, payload, consumed)) => {
                records.push(payload);
                offset += consumed;
            }
            Some(_) => {
                return Records {
                    records,
                    discarded: 0,
                }
            } // closed on purpose
            None => break,
        }
    }
    let rest = &data[offset..];
    // A reused file ends where its new records do: what follows is a stale
    // record of the file's previous life (another file number) or zeros.
    // That is the end of the log, not a torn write.
    let stale = rest.len() >= HEADER_LEN
        && (rest[..HEADER_LEN].iter().all(|&b| b == 0)
            || u32::from_le_bytes(rest[8..12].try_into().expect("4 bytes")) != file_no);
    Records {
        records,
        discarded: if stale { 0 } else { rest.len() },
    }
}

fn decode_at(data: &[u8], file_no: u32) -> Option<(u8, &[u8], usize)> {
    let mut header = data.get(..HEADER_LEN)?;
    let crc = get_u32_le(&mut header)?;
    let len = get_u32_le(&mut header)? as usize;
    let record_file = get_u32_le(&mut header)?;
    let kind = get_u8(&mut header)?;
    let end = HEADER_LEN.checked_add(len)?;
    let body = data.get(4..end)?;
    let known = kind == TYPE_FULL || (kind == TYPE_END && len == 0);
    if crc32c::crc32c(body) != crc || record_file != file_no || !known {
        return None;
    }
    Some((kind, &data[HEADER_LEN..end], end))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn log_of(file_no: u32, payloads: &[&[u8]]) -> Vec<u8> {
        let mut buf = Vec::new();
        for p in payloads {
            encode_record(&mut buf, file_no, p);
        }
        buf
    }

    #[test]
    fn records_round_trip() {
        let data = log_of(7, &[b"one", b"", b"three"]);
        let read = read_records(&data, 7);
        assert_eq!(read.records, vec![&b"one"[..], b"", b"three"]);
        assert_eq!(read.discarded, 0);
    }

    #[test]
    fn a_truncated_tail_is_dropped_at_every_cut() {
        let data = log_of(1, &[b"first", b"second record"]);
        let first_len = HEADER_LEN + 5;
        for cut in first_len..data.len() {
            let read = read_records(&data[..cut], 1);
            assert_eq!(read.records, vec![&b"first"[..]], "cut at {cut}");
            assert_eq!(read.discarded, cut - first_len);
        }
    }

    #[test]
    fn every_flipped_bit_is_caught() {
        let data = log_of(1, &[b"payload"]);
        for byte in 0..data.len() {
            for bit in 0..8 {
                let mut bad = data.clone();
                bad[byte] ^= 1 << bit;
                assert!(
                    read_records(&bad, 1).records.is_empty(),
                    "flip {byte}:{bit}"
                );
            }
        }
    }

    #[test]
    fn a_record_from_another_file_is_rejected() {
        let data = log_of(1, &[b"payload"]);
        assert!(read_records(&data, 2).records.is_empty());
    }

    #[test]
    fn reading_stops_at_a_hole_even_if_intact_records_follow() {
        let mut data = log_of(1, &[b"a", b"b", b"c"]);
        let second = HEADER_LEN + 1;
        data[second..second + 4].fill(0);
        let read = read_records(&data, 1);
        assert_eq!(read.records, vec![&b"a"[..]]);
        assert_eq!(read.discarded, data.len() - second);
    }
}

#[cfg(test)]
mod end_tests {
    use super::*;

    #[test]
    fn an_end_record_closes_the_log_whatever_follows() {
        let mut data = Vec::new();
        encode_record(&mut data, 3, b"kept");
        encode_end(&mut data, 3);
        data.extend_from_slice(b"old bytes of a reused file");
        let read = read_records(&data, 3);
        assert_eq!(read.records, vec![&b"kept"[..]]);
        assert_eq!(read.discarded, 0);
    }
}
