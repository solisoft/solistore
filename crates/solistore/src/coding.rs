//! Varints and length-prefixed byte strings, the building blocks of every
//! on-disk format here. Decoders take `&mut &[u8]` and advance it; they return
//! `None` on truncated input instead of panicking, because their input may be
//! a torn write.

pub fn put_varint(buf: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        buf.push((v as u8) | 0x80);
        v >>= 7;
    }
    buf.push(v as u8);
}

pub fn get_varint(input: &mut &[u8]) -> Option<u64> {
    let mut v = 0u64;
    for (i, &b) in input.iter().enumerate().take(10) {
        v |= ((b & 0x7f) as u64) << (7 * i);
        if b < 0x80 {
            *input = &input[i + 1..];
            return Some(v);
        }
    }
    None
}

pub fn get_varint_u32(input: &mut &[u8]) -> Option<u32> {
    get_varint(input).and_then(|v| u32::try_from(v).ok())
}

pub fn put_bytes(buf: &mut Vec<u8>, data: &[u8]) {
    put_varint(buf, data.len() as u64);
    buf.extend_from_slice(data);
}

pub fn get_bytes<'a>(input: &mut &'a [u8]) -> Option<&'a [u8]> {
    let len = usize::try_from(get_varint(input)?).ok()?;
    if input.len() < len {
        return None;
    }
    let (data, rest) = input.split_at(len);
    *input = rest;
    Some(data)
}

pub fn get_u8(input: &mut &[u8]) -> Option<u8> {
    let (&b, rest) = input.split_first()?;
    *input = rest;
    Some(b)
}

pub fn get_u32_le(input: &mut &[u8]) -> Option<u32> {
    let bytes: [u8; 4] = input.get(..4)?.try_into().ok()?;
    *input = &input[4..];
    Some(u32::from_le_bytes(bytes))
}

pub fn get_u64_le(input: &mut &[u8]) -> Option<u64> {
    let bytes: [u8; 8] = input.get(..8)?.try_into().ok()?;
    *input = &input[8..];
    Some(u64::from_le_bytes(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn varints_round_trip_at_every_width() {
        for v in [
            0,
            1,
            127,
            128,
            300,
            16_383,
            16_384,
            u32::MAX as u64,
            u64::MAX,
        ] {
            let mut buf = Vec::new();
            put_varint(&mut buf, v);
            let mut input = buf.as_slice();
            assert_eq!(get_varint(&mut input), Some(v));
            assert!(input.is_empty());
        }
    }

    #[test]
    fn truncated_input_is_none_not_a_panic() {
        let mut buf = Vec::new();
        put_varint(&mut buf, u64::MAX);
        let mut input = &buf[..buf.len() - 1];
        assert_eq!(get_varint(&mut input), None);

        let mut buf = Vec::new();
        put_bytes(&mut buf, b"hello");
        let mut input = &buf[..4];
        assert_eq!(get_bytes(&mut input), None);
        assert_eq!(get_u32_le(&mut &b"abc"[..]), None);
    }

    #[test]
    fn a_varint_wider_than_u32_is_rejected_as_u32() {
        let mut buf = Vec::new();
        put_varint(&mut buf, u32::MAX as u64 + 1);
        assert_eq!(get_varint_u32(&mut buf.as_slice()), None);
    }
}
