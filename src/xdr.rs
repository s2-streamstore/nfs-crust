use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::error::Error;

const ZERO_PADDING: [u8; 3] = [0; 3];

#[derive(Debug, Default)]
pub(crate) struct XdrEncoder {
    buf: BytesMut,
}

impl XdrEncoder {
    #[cfg(test)]
    pub(crate) fn new() -> Self {
        Self {
            buf: BytesMut::new(),
        }
    }

    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self {
            buf: BytesMut::with_capacity(capacity),
        }
    }

    pub(crate) fn put_u32(&mut self, value: u32) {
        self.buf.put_u32(value);
    }

    pub(crate) fn put_u64(&mut self, value: u64) {
        self.buf.put_u64(value);
    }

    pub(crate) fn put_bool(&mut self, value: bool) {
        self.put_u32(u32::from(value));
    }

    #[cfg(test)]
    pub(crate) fn put_fixed_opaque(&mut self, value: &[u8]) {
        self.put_bytes(value);
        self.put_padding(value.len());
    }

    pub(crate) fn put_bytes(&mut self, value: &[u8]) {
        self.buf.extend_from_slice(value);
    }

    pub(crate) fn put_opaque(&mut self, value: &[u8]) {
        self.put_u32(xdr_length_word(value.len()));
        if value.is_empty() {
            return;
        }
        self.put_bytes(value);
        self.put_padding(value.len());
    }

    pub(crate) fn put_string(&mut self, value: &str) {
        self.put_opaque(value.as_bytes());
    }

    pub(crate) fn freeze(self) -> Bytes {
        self.buf.freeze()
    }

    fn put_padding(&mut self, len: usize) {
        let padding = xdr_padding(len);
        if padding != 0 {
            self.buf.extend_from_slice(&ZERO_PADDING[..padding]);
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct XdrDecoder {
    buf: Bytes,
}

impl XdrDecoder {
    pub(crate) fn new(buf: Bytes) -> Self {
        Self { buf }
    }

    pub(crate) fn remaining(&self) -> usize {
        self.buf.remaining()
    }

    pub(crate) fn read_u32(&mut self) -> Result<u32, Error> {
        self.require(4)?;
        Ok(self.read_u32_after_require())
    }

    pub(crate) fn read_u64(&mut self) -> Result<u64, Error> {
        self.require(8)?;
        Ok(self.read_u64_after_require())
    }

    pub(crate) fn read_u32_after_require(&mut self) -> u32 {
        self.buf.get_u32()
    }

    pub(crate) fn read_u64_after_require(&mut self) -> u64 {
        self.buf.get_u64()
    }

    pub(crate) fn read_u32_pair(&mut self) -> Result<(u32, u32), Error> {
        self.require(8)?;
        Ok((self.read_u32_after_require(), self.read_u32_after_require()))
    }

    pub(crate) fn read_u32_triple(&mut self) -> Result<(u32, u32, u32), Error> {
        self.require(12)?;
        Ok((
            self.read_u32_after_require(),
            self.read_u32_after_require(),
            self.read_u32_after_require(),
        ))
    }

    pub(crate) fn read_u32_and_bytes<const N: usize>(&mut self) -> Result<(u32, [u8; N]), Error> {
        let total_len = 4usize
            .checked_add(N)
            .ok_or_else(|| Error::xdr("fixed read length overflow"))?;
        self.require(total_len)?;
        let word = self.read_u32_after_require();
        let mut bytes = [0; N];
        self.buf.copy_to_slice(&mut bytes);
        Ok((word, bytes))
    }

    pub(crate) fn read_u32_pair_and_bytes<const N: usize>(
        &mut self,
    ) -> Result<(u32, u32, [u8; N]), Error> {
        let total_len = 8usize
            .checked_add(N)
            .ok_or_else(|| Error::xdr("fixed read length overflow"))?;
        self.require(total_len)?;
        let first = self.read_u32_after_require();
        let second = self.read_u32_after_require();
        let mut bytes = [0; N];
        self.buf.copy_to_slice(&mut bytes);
        Ok((first, second, bytes))
    }

    pub(crate) fn read_bool(&mut self) -> Result<bool, Error> {
        decode_bool_word(self.read_u32()?)
    }

    pub(crate) fn read_bool_and_skip(&mut self, skip_len: usize) -> Result<bool, Error> {
        let total_len = 4usize
            .checked_add(skip_len)
            .ok_or_else(|| Error::xdr("bool skip length overflow"))?;
        self.require(total_len)?;
        let out = decode_bool_word(self.read_u32_after_require())?;
        self.buf.advance(skip_len);
        Ok(out)
    }

    pub(crate) fn read_bool_and_opaque(&mut self) -> Result<(bool, Bytes), Error> {
        let (value, len) = self.read_u32_pair()?;
        let value = decode_bool_word(value)?;
        let data = self.read_opaque_bytes(len as usize)?;
        Ok((value, data))
    }

    #[cfg(test)]
    pub(crate) fn read_fixed_opaque<const N: usize>(&mut self) -> Result<[u8; N], Error> {
        let padding = xdr_padding(N);
        let total_len = N
            .checked_add(padding)
            .ok_or_else(|| Error::xdr("fixed opaque length overflow"))?;
        self.require(total_len)?;
        let mut out = [0; N];
        self.buf.copy_to_slice(&mut out);
        self.buf.advance(padding);
        Ok(out)
    }

    pub(crate) fn read_bytes<const N: usize>(&mut self) -> Result<[u8; N], Error> {
        self.require(N)?;
        let mut out = [0; N];
        self.buf.copy_to_slice(&mut out);
        Ok(out)
    }

    pub(crate) fn skip_bytes(&mut self, len: usize) -> Result<(), Error> {
        self.require(len)?;
        self.skip_bytes_after_require(len);
        Ok(())
    }

    pub(crate) fn skip_bytes_after_require(&mut self, len: usize) {
        self.buf.advance(len);
    }

    pub(crate) fn read_opaque(&mut self) -> Result<Bytes, Error> {
        let len = self.read_u32()? as usize;
        self.read_opaque_bytes(len)
    }

    pub(crate) fn read_opaque_extent_len(&mut self) -> Result<usize, Error> {
        let len = self.read_u32()? as usize;
        self.require(opaque_padded_len(len)?)?;
        Ok(len)
    }

    pub(crate) fn finish_opaque_after_require(
        &mut self,
        len: usize,
        consumed: usize,
    ) -> Result<(), Error> {
        let skip_len = opaque_remaining_len(len, consumed)?;
        self.buf.advance(skip_len);
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn read_string(&mut self) -> Result<String, Error> {
        self.read_string_unless(|_| false)?
            .ok_or_else(|| Error::xdr("string was unexpectedly skipped"))
    }

    pub(crate) fn read_string_unless(
        &mut self,
        skip: impl FnOnce(&[u8]) -> bool,
    ) -> Result<Option<String>, Error> {
        let len = self.read_u32()? as usize;
        let padding = xdr_padding(len);
        let total_len = len
            .checked_add(padding)
            .ok_or_else(|| Error::xdr("string length overflow"))?;
        self.require(total_len)?;
        let value = &self.buf[..len];
        let skipped = skip(value);
        let string = if skipped {
            None
        } else {
            let value = std::str::from_utf8(value).map_err(|err| Error::xdr(err.to_string()))?;
            Some(value.to_owned())
        };
        self.buf.advance(total_len);
        Ok(string)
    }

    pub(crate) fn read_u32_skip_opaque_and_read_u32(&mut self) -> Result<(u32, u32), Error> {
        let (first, len) = self.read_u32_pair()?;
        let skip_len = opaque_padded_len(len as usize)?;
        let total_len = skip_len
            .checked_add(4)
            .ok_or_else(|| Error::xdr("opaque skip length overflow"))?;
        self.require(total_len)?;
        self.buf.advance(skip_len);
        Ok((first, self.read_u32_after_require()))
    }

    pub(crate) fn skip_opaque(&mut self) -> Result<(), Error> {
        let len = self.read_u32()? as usize;
        let skip_len = opaque_padded_len(len)?;
        if skip_len == 0 {
            return Ok(());
        }
        self.require(skip_len)?;
        self.buf.advance(skip_len);
        Ok(())
    }

    pub(crate) fn into_remaining(mut self) -> Bytes {
        let len = self.remaining();
        if len == 0 {
            Bytes::new()
        } else {
            self.buf.copy_to_bytes(len)
        }
    }

    fn read_opaque_bytes(&mut self, len: usize) -> Result<Bytes, Error> {
        if len == 0 {
            return Ok(Bytes::new());
        }
        let padding = xdr_padding(len);
        let total_len = opaque_padded_len(len)?;
        self.require(total_len)?;
        let out = self.buf.copy_to_bytes(len);
        self.buf.advance(padding);
        Ok(out)
    }

    pub(crate) fn require(&self, len: usize) -> Result<(), Error> {
        if self.buf.remaining() < len {
            return Err(Error::xdr(format!(
                "buffer underflow: need {len} bytes, have {}",
                self.buf.remaining()
            )));
        }
        Ok(())
    }
}

pub(crate) fn xdr_padding(len: usize) -> usize {
    (4 - (len & 3)) & 3
}

pub(crate) fn xdr_fixed_opaque_len(len: usize) -> usize {
    len.saturating_add(xdr_padding(len))
}

pub(crate) fn xdr_opaque_len(len: usize) -> usize {
    4usize.saturating_add(xdr_fixed_opaque_len(len))
}

fn xdr_length_word(len: usize) -> u32 {
    u32::try_from(len).expect("XDR variable-length value exceeds u32::MAX bytes")
}

fn decode_bool_word(value: u32) -> Result<bool, Error> {
    match value {
        0 => Ok(false),
        1 => Ok(true),
        value => Err(Error::xdr(format!("invalid XDR bool value {value}"))),
    }
}

fn opaque_padded_len(len: usize) -> Result<usize, Error> {
    len.checked_add(xdr_padding(len))
        .ok_or_else(|| Error::xdr("opaque length overflow"))
}

fn opaque_remaining_len(len: usize, consumed: usize) -> Result<usize, Error> {
    let remaining_value = len.checked_sub(consumed).ok_or_else(|| {
        Error::xdr(format!(
            "opaque decoder consumed {consumed} bytes from a {len} byte value"
        ))
    })?;
    remaining_value
        .checked_add(xdr_padding(len))
        .ok_or_else(|| Error::xdr("opaque length overflow"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn opaque_values_are_padded_to_four_bytes() {
        let mut enc = XdrEncoder::new();
        enc.put_opaque(b"abc");
        let bytes = enc.freeze();
        assert_eq!(&bytes[..], &[0, 0, 0, 3, b'a', b'b', b'c', 0]);

        let mut dec = XdrDecoder::new(bytes);
        assert_eq!(&dec.read_opaque().unwrap()[..], b"abc");
        assert_eq!(dec.remaining(), 0);
    }

    #[test]
    fn padding_uses_four_byte_alignment() {
        let expected = [0, 3, 2, 1, 0, 3, 2, 1, 0];
        for (len, padding) in expected.into_iter().enumerate() {
            assert_eq!(xdr_padding(len), padding);
        }
        assert_eq!(xdr_padding(usize::MAX), 1);
    }

    #[test]
    fn encoded_length_helpers_saturate_at_usize_max() {
        assert_eq!(xdr_fixed_opaque_len(usize::MAX), usize::MAX);
        assert_eq!(xdr_opaque_len(usize::MAX), usize::MAX);
    }

    #[test]
    fn xdr_length_word_accepts_largest_wire_length() {
        assert_eq!(xdr_length_word(u32::MAX as usize), u32::MAX);
    }

    #[cfg(target_pointer_width = "64")]
    #[test]
    #[should_panic(expected = "XDR variable-length value exceeds u32::MAX bytes")]
    fn xdr_length_word_rejects_unrepresentable_wire_length() {
        xdr_length_word(u32::MAX as usize + 1);
    }

    #[test]
    fn empty_opaque_values_skip_zero_length_buffer_work() {
        let mut enc = XdrEncoder::new();
        enc.put_opaque(&[]);
        enc.put_u32(9);
        let bytes = enc.freeze();
        assert_eq!(&bytes[..], &[0, 0, 0, 0, 0, 0, 0, 9]);

        let mut dec = XdrDecoder::new(bytes.clone());
        assert_eq!(dec.read_opaque().unwrap(), Bytes::new());
        assert_eq!(dec.read_u32().unwrap(), 9);
        assert_eq!(dec.remaining(), 0);

        let mut dec = XdrDecoder::new(bytes);
        dec.skip_opaque().unwrap();
        assert_eq!(dec.read_u32().unwrap(), 9);
        assert_eq!(dec.remaining(), 0);
    }

    #[test]
    fn aligned_opaque_values_do_not_emit_or_require_padding() {
        let mut enc = XdrEncoder::new();
        enc.put_opaque(b"abcd");
        let bytes = enc.freeze();
        assert_eq!(&bytes[..], &[0, 0, 0, 4, b'a', b'b', b'c', b'd']);

        let mut dec = XdrDecoder::new(bytes);
        assert_eq!(&dec.read_opaque().unwrap()[..], b"abcd");
        assert_eq!(dec.remaining(), 0);
    }

    #[test]
    fn skip_opaque_consumes_value_and_padding() {
        let mut enc = XdrEncoder::new();
        enc.put_opaque(b"abc");
        enc.put_u32(9);

        let mut dec = XdrDecoder::new(enc.freeze());
        dec.skip_opaque().unwrap();
        assert_eq!(dec.read_u32().unwrap(), 9);
        assert_eq!(dec.remaining(), 0);
    }

    #[test]
    fn skip_opaque_requires_padding_bytes() {
        let mut enc = XdrEncoder::new();
        enc.put_u32(3);
        enc.put_bytes(b"abc");

        let err = XdrDecoder::new(enc.freeze()).skip_opaque().unwrap_err();
        assert!(matches!(err, Error::Xdr(_)));
    }

    #[test]
    fn u32_opaque_u32_shape_checks_padding_and_following_word_together() {
        let mut enc = XdrEncoder::new();
        enc.put_u32(7);
        enc.put_opaque(b"abc");
        enc.put_u32(9);
        enc.put_u32(11);

        let mut dec = XdrDecoder::new(enc.freeze());
        assert_eq!(dec.read_u32_skip_opaque_and_read_u32().unwrap(), (7, 9));
        assert_eq!(dec.read_u32().unwrap(), 11);

        let mut enc = XdrEncoder::new();
        enc.put_u32(7);
        enc.put_opaque(b"abc");
        let err = XdrDecoder::new(enc.freeze())
            .read_u32_skip_opaque_and_read_u32()
            .unwrap_err();
        assert!(matches!(err, Error::Xdr(_)));
    }

    #[test]
    fn read_opaque_requires_padding_before_consuming_value() {
        let mut enc = XdrEncoder::new();
        enc.put_u32(3);
        enc.put_bytes(b"abc");

        let mut dec = XdrDecoder::new(enc.freeze());
        let err = dec.read_opaque().unwrap_err();
        assert!(matches!(err, Error::Xdr(_)));
        assert_eq!(dec.remaining(), 3);
    }

    #[test]
    fn strings_decode_directly_from_padded_opaque_data() {
        let mut enc = XdrEncoder::new();
        enc.put_string("abc");
        let bytes = enc.freeze();

        let mut dec = XdrDecoder::new(bytes);
        assert_eq!(dec.read_string().unwrap(), "abc");
        assert_eq!(dec.remaining(), 0);
    }

    #[test]
    fn strings_can_be_skipped_by_raw_bytes_before_allocation() {
        let mut enc = XdrEncoder::new();
        enc.put_string(".");
        enc.put_string("abc");
        let bytes = enc.freeze();

        let mut dec = XdrDecoder::new(bytes);
        assert_eq!(dec.read_string_unless(|bytes| bytes == b".").unwrap(), None);
        assert_eq!(
            dec.read_string_unless(|bytes| bytes == b".").unwrap(),
            Some("abc".to_owned())
        );
        assert_eq!(dec.remaining(), 0);
    }

    #[test]
    fn skipped_strings_are_not_utf8_validated() {
        let mut enc = XdrEncoder::new();
        enc.put_opaque(&[0xff]);
        enc.put_u32(9);

        let mut dec = XdrDecoder::new(enc.freeze());
        assert_eq!(dec.read_string_unless(|_| true).unwrap(), None);
        assert_eq!(dec.read_u32().unwrap(), 9);
    }

    #[test]
    fn unskipped_strings_validate_before_advancing() {
        let mut enc = XdrEncoder::new();
        enc.put_opaque(&[0xff]);

        let mut dec = XdrDecoder::new(enc.freeze());
        let err = dec.read_string_unless(|_| false).unwrap_err();
        assert!(matches!(err, Error::Xdr(_)));
        assert_eq!(dec.remaining(), 4);
    }

    #[test]
    fn strings_require_padding_before_running_skip_predicate() {
        let mut enc = XdrEncoder::new();
        enc.put_u32(3);
        enc.put_bytes(b"abc");

        let mut dec = XdrDecoder::new(enc.freeze());
        let mut predicate_called = false;
        let err = dec
            .read_string_unless(|_| {
                predicate_called = true;
                false
            })
            .unwrap_err();

        assert!(matches!(err, Error::Xdr(_)));
        assert!(!predicate_called);
        assert_eq!(dec.remaining(), 3);
    }

    #[test]
    fn opaque_values_can_be_decoded_in_place() {
        let mut value = XdrEncoder::new();
        value.put_u32(7);

        let mut enc = XdrEncoder::new();
        enc.put_opaque(&value.freeze());
        enc.put_u32(9);

        let mut dec = XdrDecoder::new(enc.freeze());
        let len = dec.read_opaque_extent_len().unwrap();
        let start = dec.remaining();
        assert_eq!(dec.read_u32().unwrap(), 7);
        let consumed = start - dec.remaining();
        dec.finish_opaque_after_require(len, consumed).unwrap();
        assert_eq!(dec.read_u32().unwrap(), 9);
        assert_eq!(dec.remaining(), 0);
    }

    #[test]
    fn opaque_extent_len_requires_padding_before_returning() {
        let mut enc = XdrEncoder::new();
        enc.put_u32(3);
        enc.put_bytes(b"abc");

        let mut dec = XdrDecoder::new(enc.freeze());
        let err = dec.read_opaque_extent_len().unwrap_err();
        assert!(matches!(err, Error::Xdr(_)));
        assert_eq!(dec.remaining(), 3);
    }

    #[test]
    fn finish_opaque_after_require_skips_tail_and_padding() {
        let mut enc = XdrEncoder::new();
        enc.put_opaque(b"abcde");
        enc.put_u32(9);

        let mut dec = XdrDecoder::new(enc.freeze());
        let len = dec.read_opaque_extent_len().unwrap();
        assert_eq!(dec.read_bytes::<1>().unwrap(), *b"a");
        dec.finish_opaque_after_require(len, 1).unwrap();
        assert_eq!(dec.read_u32().unwrap(), 9);
        assert_eq!(dec.remaining(), 0);
    }

    #[test]
    fn raw_byte_skips_do_not_consume_xdr_padding() {
        let mut dec = XdrDecoder::new(Bytes::from_static(b"abcde"));

        dec.skip_bytes(3).unwrap();
        assert_eq!(dec.remaining(), 2);
        dec.skip_bytes(1).unwrap();
        assert_eq!(dec.remaining(), 1);
    }

    #[test]
    fn raw_byte_skips_can_reuse_existing_bounds_check() {
        let mut dec = XdrDecoder::new(Bytes::from_static(b"abcde"));

        dec.require(3).unwrap();
        dec.skip_bytes_after_require(3);
        assert_eq!(dec.remaining(), 2);
    }

    #[test]
    fn raw_fixed_reads_do_not_consume_xdr_padding() {
        let mut dec = XdrDecoder::new(Bytes::from_static(b"abcde"));

        assert_eq!(dec.read_bytes::<3>().unwrap(), *b"abc");
        assert_eq!(dec.remaining(), 2);
    }

    #[test]
    fn u32_pair_reads_two_words_after_single_extent_check() {
        let mut enc = XdrEncoder::new();
        enc.put_u32(7);
        enc.put_u32(9);
        enc.put_u32(11);

        let mut dec = XdrDecoder::new(enc.freeze());
        assert_eq!(dec.read_u32_pair().unwrap(), (7, 9));
        assert_eq!(dec.read_u32().unwrap(), 11);

        let mut enc = XdrEncoder::new();
        enc.put_u32(7);
        let err = XdrDecoder::new(enc.freeze()).read_u32_pair().unwrap_err();
        assert!(matches!(err, Error::Xdr(_)));
    }

    #[test]
    fn u32_triple_reads_three_words_after_single_extent_check() {
        let mut enc = XdrEncoder::new();
        enc.put_u32(7);
        enc.put_u32(9);
        enc.put_u32(11);
        enc.put_u32(13);

        let mut dec = XdrDecoder::new(enc.freeze());
        assert_eq!(dec.read_u32_triple().unwrap(), (7, 9, 11));
        assert_eq!(dec.read_u32().unwrap(), 13);

        let mut enc = XdrEncoder::new();
        enc.put_u32(7);
        enc.put_u32(9);
        let err = XdrDecoder::new(enc.freeze()).read_u32_triple().unwrap_err();
        assert!(matches!(err, Error::Xdr(_)));
    }

    #[test]
    fn u32_and_bytes_reads_fixed_shape_after_single_extent_check() {
        let mut enc = XdrEncoder::new();
        enc.put_u32(7);
        enc.put_bytes(&[1, 2, 3, 4]);
        enc.put_u32(11);

        let mut dec = XdrDecoder::new(enc.freeze());
        assert_eq!(dec.read_u32_and_bytes::<4>().unwrap(), (7, [1, 2, 3, 4]));
        assert_eq!(dec.read_u32().unwrap(), 11);

        let mut enc = XdrEncoder::new();
        enc.put_u32(7);
        enc.put_bytes(&[1, 2, 3]);
        let err = XdrDecoder::new(enc.freeze())
            .read_u32_and_bytes::<4>()
            .unwrap_err();
        assert!(matches!(err, Error::Xdr(_)));
    }

    #[test]
    fn u32_pair_and_bytes_reads_fixed_shape_after_single_extent_check() {
        let mut enc = XdrEncoder::new();
        enc.put_u32(7);
        enc.put_u32(9);
        enc.put_bytes(&[1, 2, 3, 4]);
        enc.put_u32(11);

        let mut dec = XdrDecoder::new(enc.freeze());
        assert_eq!(
            dec.read_u32_pair_and_bytes::<4>().unwrap(),
            (7, 9, [1, 2, 3, 4])
        );
        assert_eq!(dec.read_u32().unwrap(), 11);

        let mut enc = XdrEncoder::new();
        enc.put_u32(7);
        enc.put_u32(9);
        enc.put_bytes(&[1, 2, 3]);
        let err = XdrDecoder::new(enc.freeze())
            .read_u32_pair_and_bytes::<4>()
            .unwrap_err();
        assert!(matches!(err, Error::Xdr(_)));
    }

    #[test]
    fn bool_and_skip_reads_bool_and_fixed_tail_together() {
        let mut enc = XdrEncoder::new();
        enc.put_bool(true);
        enc.put_u32(7);
        enc.put_u32(9);
        enc.put_u32(11);

        let mut dec = XdrDecoder::new(enc.freeze());
        assert!(dec.read_bool_and_skip(8).unwrap());
        assert_eq!(dec.read_u32().unwrap(), 11);

        let mut enc = XdrEncoder::new();
        enc.put_bool(true);
        let err = XdrDecoder::new(enc.freeze())
            .read_bool_and_skip(1)
            .unwrap_err();
        assert!(matches!(err, Error::Xdr(_)));
    }

    #[test]
    fn bool_and_opaque_reads_flag_and_payload_shape() {
        let mut enc = XdrEncoder::new();
        enc.put_bool(true);
        enc.put_opaque(b"abc");
        enc.put_u32(11);

        let mut dec = XdrDecoder::new(enc.freeze());
        let (flag, data) = dec.read_bool_and_opaque().unwrap();
        assert!(flag);
        assert_eq!(&data[..], b"abc");
        assert_eq!(dec.read_u32().unwrap(), 11);

        let mut enc = XdrEncoder::new();
        enc.put_u32(2);
        enc.put_opaque(b"abc");
        let err = XdrDecoder::new(enc.freeze())
            .read_bool_and_opaque()
            .unwrap_err();
        assert!(matches!(err, Error::Xdr(_)));

        let mut enc = XdrEncoder::new();
        enc.put_bool(true);
        enc.put_u32(3);
        enc.put_bytes(b"abc");
        let err = XdrDecoder::new(enc.freeze())
            .read_bool_and_opaque()
            .unwrap_err();
        assert!(matches!(err, Error::Xdr(_)));
    }

    #[test]
    fn raw_byte_writes_do_not_emit_xdr_padding() {
        let mut enc = XdrEncoder::new();

        enc.put_bytes(b"abc");
        assert_eq!(&enc.freeze()[..], b"abc");
    }

    #[test]
    fn fixed_opaque_uses_the_same_padding_rule() {
        let mut enc = XdrEncoder::new();
        enc.put_fixed_opaque(b"abcde");
        let bytes = enc.freeze();
        assert_eq!(&bytes[..], &[b'a', b'b', b'c', b'd', b'e', 0, 0, 0]);

        let mut dec = XdrDecoder::new(bytes);
        assert_eq!(dec.read_fixed_opaque::<5>().unwrap(), *b"abcde");
        assert_eq!(dec.remaining(), 0);
    }
}
