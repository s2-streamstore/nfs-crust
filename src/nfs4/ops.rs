use super::*;

#[derive(Debug, Clone)]
pub(crate) struct ReadData {
    pub(crate) eof: bool,
    pub(crate) data: Bytes,
}

#[derive(Debug, Clone)]
pub(crate) struct WriteData {
    pub(crate) count: u32,
    pub(super) committed: WriteStability,
    pub(crate) verifier: [u8; 8],
}

impl WriteData {
    pub(crate) fn requires_commit(&self) -> bool {
        self.committed != WriteStability::FileSync
    }

    #[cfg(test)]
    pub(crate) fn test_unstable(count: u32, verifier: [u8; 8]) -> Self {
        Self {
            count,
            committed: WriteStability::Unstable,
            verifier,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum WriteStability {
    Unstable,
    DataSync,
    FileSync,
}

impl WriteStability {
    pub(super) fn from_raw(value: u32) -> Result<Self, Error> {
        match value {
            UNSTABLE4 => Ok(Self::Unstable),
            DATA_SYNC4 => Ok(Self::DataSync),
            FILE_SYNC4 => Ok(Self::FileSync),
            other => Err(Error::protocol(format!(
                "server returned invalid write stable_how {other}"
            ))),
        }
    }
}

#[derive(Debug)]
pub(super) enum NfsOp<'a> {
    PutRootFh,
    PutFh(&'a FileHandle),
    Lookup(&'a str),
    SaveFh,
    RestoreFh,
    GetFh,
    GetAttr(Bitmap),
    Open {
        seqid: u32,
        clientid: u64,
        owner: &'a [u8],
        name: &'a str,
        file_mode: u32,
    },
    Close {
        seqid: u32,
        stateid: StateId,
    },
    Read {
        stateid: StateId,
        offset: u64,
        count: u32,
    },
    Write {
        stateid: StateId,
        offset: u64,
        data: Bytes,
    },
    Commit {
        offset: u64,
        count: u32,
    },
    Link(&'a str),
    Remove(&'a str),
    Verify {
        size: u64,
    },
    Rename {
        old_name: &'a str,
        new_name: &'a str,
    },
    CreateDir {
        name: &'a str,
        mode: u32,
    },
    ReadDir {
        cookie: u64,
        verifier: [u8; 8],
        dircount: u32,
        maxcount: u32,
        attrs: Bitmap,
    },
    ExchangeId {
        verifier: [u8; 8],
        owner: &'a [u8],
    },
    CreateSession {
        clientid: u64,
        sequenceid: u32,
        config: &'a NfsConfig,
    },
    ReclaimComplete {
        one_fs: bool,
    },
}

impl NfsOp<'_> {
    pub(super) fn opcode(&self) -> OpCode {
        match self {
            Self::PutRootFh => OpCode::PutRootFh,
            Self::PutFh(_) => OpCode::PutFh,
            Self::Lookup(_) => OpCode::Lookup,
            Self::SaveFh => OpCode::SaveFh,
            Self::RestoreFh => OpCode::RestoreFh,
            Self::GetFh => OpCode::GetFh,
            Self::GetAttr(_) => OpCode::GetAttr,
            Self::Open { .. } => OpCode::Open,
            Self::Close { .. } => OpCode::Close,
            Self::Read { .. } => OpCode::Read,
            Self::Write { .. } => OpCode::Write,
            Self::Commit { .. } => OpCode::Commit,
            Self::Link(_) => OpCode::Link,
            Self::Remove(_) => OpCode::Remove,
            Self::Verify { .. } => OpCode::Verify,
            Self::Rename { .. } => OpCode::Rename,
            Self::CreateDir { .. } => OpCode::Create,
            Self::ReadDir { .. } => OpCode::ReadDir,
            Self::ExchangeId { .. } => OpCode::ExchangeId,
            Self::CreateSession { .. } => OpCode::CreateSession,
            Self::ReclaimComplete { .. } => OpCode::ReclaimComplete,
        }
    }

    pub(super) fn encode(&self, enc: &mut XdrEncoder) {
        match self {
            Self::PutRootFh => enc.put_u32(OpCode::PutRootFh as u32),
            Self::PutFh(fh) => {
                enc.put_u32(OpCode::PutFh as u32);
                fh.encode(enc);
            }
            Self::Lookup(name) => {
                enc.put_u32(OpCode::Lookup as u32);
                enc.put_string(name);
            }
            Self::SaveFh => enc.put_u32(OpCode::SaveFh as u32),
            Self::RestoreFh => enc.put_u32(OpCode::RestoreFh as u32),
            Self::Verify { size } => {
                enc.put_u32(OpCode::Verify as u32);
                SetAttrs {
                    size: Some(*size),
                    mode: None,
                }
                .encode(enc);
            }
            Self::GetFh => enc.put_u32(OpCode::GetFh as u32),
            Self::GetAttr(bitmap) => {
                enc.put_u32(OpCode::GetAttr as u32);
                bitmap.encode(enc);
            }
            Self::Open {
                seqid,
                clientid,
                owner,
                name,
                file_mode,
            } => {
                enc.put_u32(OpCode::Open as u32);
                enc.put_u32(*seqid);
                enc.put_u32(OPEN4_SHARE_ACCESS_WRITE | OPEN4_SHARE_ACCESS_WANT_NO_DELEG);
                enc.put_u32(OPEN4_SHARE_DENY_NONE);
                enc.put_u64(*clientid);
                enc.put_opaque(owner);
                enc.put_u32(OPEN_CREATE);
                enc.put_u32(CREATE_GUARDED);
                SetAttrs {
                    size: Some(0),
                    mode: Some(*file_mode),
                }
                .encode(enc);
                enc.put_u32(CLAIM_NULL);
                enc.put_string(name);
            }
            Self::Close { seqid, stateid } => {
                enc.put_u32(OpCode::Close as u32);
                enc.put_u32(*seqid);
                stateid.encode(enc);
            }
            Self::Read {
                stateid,
                offset,
                count,
            } => {
                enc.put_u32(OpCode::Read as u32);
                stateid.encode(enc);
                enc.put_u64(*offset);
                enc.put_u32(*count);
            }
            Self::Write {
                stateid,
                offset,
                data,
            } => {
                enc.put_u32(OpCode::Write as u32);
                stateid.encode(enc);
                enc.put_u64(*offset);
                enc.put_u32(UNSTABLE4);
                enc.put_opaque(data);
            }
            Self::Commit { offset, count } => {
                enc.put_u32(OpCode::Commit as u32);
                enc.put_u64(*offset);
                enc.put_u32(*count);
            }
            Self::Link(name) => {
                enc.put_u32(OpCode::Link as u32);
                enc.put_string(name);
            }
            Self::Remove(name) => {
                enc.put_u32(OpCode::Remove as u32);
                enc.put_string(name);
            }
            Self::Rename { old_name, new_name } => {
                enc.put_u32(OpCode::Rename as u32);
                enc.put_string(old_name);
                enc.put_string(new_name);
            }
            Self::CreateDir { name, mode } => {
                enc.put_u32(OpCode::Create as u32);
                enc.put_u32(NF4DIR);
                enc.put_string(name);
                SetAttrs {
                    size: None,
                    mode: Some(*mode),
                }
                .encode(enc);
            }
            Self::ReadDir {
                cookie,
                verifier,
                dircount,
                maxcount,
                attrs,
            } => {
                enc.put_u32(OpCode::ReadDir as u32);
                enc.put_u64(*cookie);
                enc.put_bytes(verifier);
                enc.put_u32(*dircount);
                enc.put_u32(*maxcount);
                attrs.encode(enc);
            }
            Self::ExchangeId { verifier, owner } => {
                enc.put_u32(OpCode::ExchangeId as u32);
                enc.put_bytes(verifier);
                enc.put_opaque(owner);
                enc.put_u32(EXCHGID4_FLAG_USE_NON_PNFS);
                enc.put_u32(SP4_NONE);
                enc.put_u32(0); // nfs_impl_id4 array length
            }
            Self::CreateSession {
                clientid,
                sequenceid,
                config,
            } => {
                enc.put_u32(OpCode::CreateSession as u32);
                enc.put_u64(*clientid);
                enc.put_u32(*sequenceid);
                enc.put_u32(0);
                encode_channel_attrs(
                    enc,
                    config.max_request_size,
                    config.max_response_size,
                    config.session_slots,
                );
                encode_channel_attrs(enc, 8192, 8192, 1);
                enc.put_u32(0);
                enc.put_u32(0); // callback_sec_parms array length
            }
            Self::ReclaimComplete { one_fs } => {
                enc.put_u32(OpCode::ReclaimComplete as u32);
                enc.put_bool(*one_fs);
            }
        }
    }

    pub(super) fn encoded_len(&self) -> usize {
        let payload_len = match self {
            Self::PutRootFh | Self::GetFh | Self::SaveFh | Self::RestoreFh => 0,
            Self::Verify { .. } => SetAttrs {
                size: Some(0),
                mode: None,
            }
            .encoded_len(),
            Self::PutFh(fh) => fh.encoded_len(),
            Self::Lookup(name) | Self::Link(name) | Self::Remove(name) => xdr_string_len(name),
            Self::GetAttr(bitmap) => bitmap.encoded_len(),
            Self::Open { owner, name, .. } => {
                let create_len = 4usize.saturating_add(
                    SetAttrs {
                        size: Some(0),
                        mode: Some(0),
                    }
                    .encoded_len(),
                );
                saturating_encoded_len([
                    4,
                    4,
                    4,
                    8,
                    xdr_opaque_len(owner.len()),
                    4,
                    create_len,
                    4,
                    xdr_string_len(name),
                ])
            }
            Self::Close { .. } => 4usize.saturating_add(StateId::encoded_len()),
            Self::Read { .. } => saturating_encoded_len([StateId::encoded_len(), 8, 4]),
            Self::Write { data, .. } => {
                saturating_encoded_len([StateId::encoded_len(), 8, 4, xdr_opaque_len(data.len())])
            }
            Self::Commit { .. } => 8usize.saturating_add(4),
            Self::Rename { old_name, new_name } => {
                xdr_string_len(old_name).saturating_add(xdr_string_len(new_name))
            }
            Self::CreateDir { name, .. } => saturating_encoded_len([
                4,
                xdr_string_len(name),
                SetAttrs {
                    size: None,
                    mode: Some(0),
                }
                .encoded_len(),
            ]),
            Self::ReadDir { attrs, .. } => {
                saturating_encoded_len([8, xdr_fixed_opaque_len(8), 4, 4, attrs.encoded_len()])
            }
            Self::ExchangeId { owner, .. } => saturating_encoded_len([
                xdr_fixed_opaque_len(8),
                xdr_opaque_len(owner.len()),
                4,
                4,
                4,
            ]),
            Self::CreateSession { .. } => {
                saturating_encoded_len([8, 4, 4, channel_attrs_len(), channel_attrs_len(), 4, 4])
            }
            Self::ReclaimComplete { .. } => 4,
        };
        4usize.saturating_add(payload_len)
    }
}

#[derive(Debug, Clone, Default)]
struct SetAttrs {
    size: Option<u64>,
    mode: Option<u32>,
}

impl SetAttrs {
    pub(super) fn encode(&self, enc: &mut XdrEncoder) {
        let bitmap_words = self.bitmap_words();
        enc.put_u32(bitmap_words as u32);
        for word_index in 0..bitmap_words {
            enc.put_u32(self.bitmap_word(word_index));
        }

        let values_len = self.values_len();
        debug_assert_eq!(xdr_padding(values_len), 0);
        enc.put_u32(values_len as u32);
        if let Some(size) = self.size {
            enc.put_u64(size);
        }
        if let Some(mode) = self.mode {
            enc.put_u32(mode);
        }
    }

    fn encoded_len(&self) -> usize {
        let bitmap_len = 4usize.saturating_add(self.bitmap_words().saturating_mul(4));
        bitmap_len.saturating_add(xdr_opaque_len(self.values_len()))
    }

    fn bitmap_words(&self) -> usize {
        if self.mode.is_some() {
            (FATTR4_MODE / 32 + 1) as usize
        } else if self.size.is_some() {
            (FATTR4_SIZE / 32 + 1) as usize
        } else {
            0
        }
    }

    fn bitmap_word(&self, word_index: usize) -> u32 {
        let mut word = 0;
        if self.size.is_some() && FATTR4_SIZE / 32 == word_index as u32 {
            word |= 1 << (FATTR4_SIZE % 32);
        }
        if self.mode.is_some() && FATTR4_MODE / 32 == word_index as u32 {
            word |= 1 << (FATTR4_MODE % 32);
        }
        word
    }

    fn values_len(&self) -> usize {
        self.size
            .map(|_| 8usize)
            .unwrap_or_default()
            .saturating_add(self.mode.map(|_| 4).unwrap_or_default())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Bitmap {
    pub(super) words: SmallVec<[u32; INLINE_BITMAP_WORDS]>,
}

impl Bitmap {
    pub(super) fn empty() -> Self {
        Self {
            words: SmallVec::new(),
        }
    }

    pub(super) fn size_attr() -> Self {
        Self::inline_word(1 << FATTR4_SIZE)
    }

    pub(super) fn type_and_size_attrs() -> Self {
        Self::inline_word((1 << FATTR4_TYPE) | (1 << FATTR4_SIZE))
    }

    pub(super) fn inline_word(word: u32) -> Self {
        Self {
            words: SmallVec::from_slice(&[word]),
        }
    }

    pub(super) fn encode(&self, enc: &mut XdrEncoder) {
        enc.put_u32(self.words().len() as u32);
        for word in self.words() {
            enc.put_u32(*word);
        }
    }

    pub(super) fn encoded_len(&self) -> usize {
        4usize.saturating_add(self.words().len().saturating_mul(4))
    }

    #[cfg(test)]
    pub(super) fn decode(dec: &mut XdrDecoder) -> Result<Self, Error> {
        let len = dec.read_u32()? as usize;
        Self::decode_after_len(dec, len)
    }

    pub(super) fn decode_after_len(dec: &mut XdrDecoder, len: usize) -> Result<Self, Error> {
        require_bitmap_words(dec, len)?;
        let mut words = SmallVec::with_capacity(len);
        for _ in 0..len {
            words.push(dec.read_u32_after_require());
        }
        Ok(Self { words })
    }

    #[cfg(test)]
    pub(super) fn contains(&self, attr: u32) -> bool {
        let word = (attr / 32) as usize;
        let bit = attr % 32;
        self.words()
            .get(word)
            .map(|value| value & (1u32 << bit) != 0)
            .unwrap_or(false)
    }

    pub(super) fn attrs(&self) -> BitmapAttrs<'_> {
        BitmapAttrs {
            words: self.words(),
            word_index: 0,
            current_word: 0,
            current_word_index: 0,
        }
    }

    pub(super) fn words(&self) -> &[u32] {
        &self.words
    }
}

pub(super) struct BitmapAttrs<'a> {
    words: &'a [u32],
    word_index: usize,
    current_word: u32,
    current_word_index: u32,
}

impl Iterator for BitmapAttrs<'_> {
    type Item = u32;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.current_word != 0 {
                let bit = self.current_word.trailing_zeros();
                self.current_word &= self.current_word - 1;
                return Some(self.current_word_index * 32 + bit);
            }

            self.current_word = *self.words.get(self.word_index)?;
            self.current_word_index = self.word_index as u32;
            self.word_index += 1;
        }
    }
}
