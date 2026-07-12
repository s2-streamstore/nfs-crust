use super::*;

#[derive(Debug, Clone)]
pub(super) struct ExchangeIdResult {
    pub(super) clientid: u64,
    pub(super) sequenceid: u32,
}

#[derive(Debug, Clone)]
pub(super) struct CreateSessionResult {
    pub(super) sessionid: [u8; 16],
    pub(super) fore_attrs: ChannelAttrs,
}

#[derive(Debug, Clone)]
pub(super) struct ChannelAttrs {
    pub(super) max_request_size: u32,
    pub(super) max_response_size: u32,
    pub(super) maxoperations: u32,
    pub(super) maxrequests: u32,
}

pub(super) async fn exchange_id(
    rpc: &RpcClient,
    owner: &ClientOwner,
    client_verifier: u64,
) -> Result<ExchangeIdResult, Error> {
    let payload = encode_compound(
        "exchange-id",
        &[NfsOp::ExchangeId {
            verifier: client_verifier.to_be_bytes(),
            owner: owner.ownerid.as_bytes(),
        }],
    );
    let response = rpc
        .call(NFS_PROGRAM, NFS_VERSION, NFSPROC4_COMPOUND, payload)
        .await?;
    decode_exchange_id_compound(response)
}

pub(super) async fn create_session(
    rpc: &RpcClient,
    clientid: u64,
    sequenceid: u32,
    config: &NfsConfig,
) -> Result<CreateSessionResult, Error> {
    let payload = encode_compound(
        "create-session",
        &[NfsOp::CreateSession {
            clientid,
            sequenceid,
            config,
        }],
    );
    let response = rpc
        .call(NFS_PROGRAM, NFS_VERSION, NFSPROC4_COMPOUND, payload)
        .await?;
    let minimum_request_size = rpc.encoded_call_len(compound_with_sequence_encoded_len("", &[]));
    decode_create_session_compound(response, sequenceid, minimum_request_size)
}

pub(super) fn encode_compound(tag: &str, ops: &[NfsOp<'_>]) -> Bytes {
    encode_compound_inner(tag, None, ops)
}

pub(super) fn encode_compound_with_sequence(
    tag: &str,
    sequence: SequenceArgs,
    ops: &[NfsOp<'_>],
) -> RpcPayload {
    let capacity = compound_with_sequence_encoded_len(tag, ops).saturating_sub(write_data_len(ops));
    let mut enc = XdrEncoder::with_capacity(capacity);
    enc.put_string(tag);
    enc.put_u32(NFS_MINOR_VERSION);
    enc.put_u32(ops.len().saturating_add(1) as u32);
    encode_sequence_op(&mut enc, sequence);

    let mut segments = SmallVec::<[Bytes; 3]>::new();
    for op in ops {
        if let NfsOp::Write {
            stateid,
            offset,
            data,
        } = op
        {
            enc.put_u32(OpCode::Write as u32);
            stateid.encode(&mut enc);
            enc.put_u64(*offset);
            enc.put_u32(UNSTABLE4);
            enc.put_u32(data.len() as u32);
            segments.push(enc.freeze());
            segments.push(data.clone());
            enc = XdrEncoder::with_capacity(128);
            enc.put_bytes(&[0; 3][..xdr_padding(data.len())]);
        } else {
            op.encode(&mut enc);
        }
    }
    segments.push(enc.freeze());
    RpcPayload::from_segments(segments)
}

pub(super) fn encode_compound_inner(
    tag: &str,
    sequence: Option<SequenceArgs>,
    ops: &[NfsOp<'_>],
) -> Bytes {
    let mut capacity = compound_encoded_len(tag, ops);
    let mut op_count = ops.len();
    if sequence.is_some() {
        capacity = capacity.saturating_add(sequence_op_encoded_len());
        op_count = op_count.saturating_add(1);
    }
    let mut enc = XdrEncoder::with_capacity(capacity);
    enc.put_string(tag);
    enc.put_u32(NFS_MINOR_VERSION);
    enc.put_u32(op_count as u32);
    if let Some(sequence) = sequence {
        encode_sequence_op(&mut enc, sequence);
    }
    for op in ops {
        op.encode(&mut enc);
    }
    enc.freeze()
}

pub(super) fn encode_sequence_op(enc: &mut XdrEncoder, sequence: SequenceArgs) {
    enc.put_u32(OpCode::Sequence as u32);
    enc.put_bytes(&sequence.sessionid);
    enc.put_u32(sequence.sequenceid);
    enc.put_u32(sequence.slotid);
    enc.put_u32(sequence.highest_slotid);
    enc.put_bool(sequence.cachethis);
}

pub(super) fn compound_encoded_len(tag: &str, ops: &[NfsOp<'_>]) -> usize {
    ops.iter().fold(
        saturating_encoded_len([xdr_string_len(tag), 4, 4]),
        |total, op| total.saturating_add(op.encoded_len()),
    )
}

pub(super) fn compound_with_sequence_encoded_len(tag: &str, ops: &[NfsOp<'_>]) -> usize {
    compound_encoded_len(tag, ops).saturating_add(sequence_op_encoded_len())
}

pub(super) fn write_data_len(ops: &[NfsOp<'_>]) -> usize {
    ops.iter().fold(0usize, |total, op| match op {
        NfsOp::Write { data, .. } => total.saturating_add(data.len()),
        _ => total,
    })
}

pub(super) fn validate_compound_request_limits(
    tag: &str,
    ops: &[NfsOp<'_>],
    max_compound_ops: u32,
    max_request_size: u32,
    encoded_call_len: usize,
) -> Result<(), Error> {
    let operation_count = ops.len().saturating_add(1);
    if operation_count > max_compound_ops as usize {
        return Err(Error::protocol(format!(
            "{tag} compound has {operation_count} operations, exceeding negotiated limit {max_compound_ops}"
        )));
    }

    if encoded_call_len > max_request_size as usize {
        return Err(Error::protocol(format!(
            "{tag} RPC request is {encoded_call_len} bytes, exceeding negotiated request limit {max_request_size}"
        )));
    }
    Ok(())
}

pub(super) fn sequence_op_encoded_len() -> usize {
    saturating_encoded_len([4, xdr_fixed_opaque_len(16), 4, 4, 4, 4])
}

pub(super) fn xdr_string_len(value: &str) -> usize {
    xdr_opaque_len(value.len())
}

pub(super) fn saturating_encoded_len<const N: usize>(parts: [usize; N]) -> usize {
    parts
        .into_iter()
        .fold(0, |total, part| total.saturating_add(part))
}

#[derive(Debug)]
pub(super) struct CompoundDecodeError {
    pub(super) status: Option<NfsStatus>,
    pub(super) reply_index: Option<usize>,
    pub(super) error: Error,
}

impl CompoundDecodeError {
    pub(super) fn nfs(status: NfsStatus, op: OpCode) -> Self {
        Self {
            status: Some(status),
            reply_index: None,
            error: Error::nfs(status, op),
        }
    }

    pub(super) fn at_reply(mut self, reply_index: usize) -> Self {
        self.reply_index.get_or_insert(reply_index);
        self
    }

    pub(super) fn transient_status(&self) -> Option<NfsStatus> {
        self.status
            .filter(|status| *status == NfsStatus::DELAY || *status == NfsStatus::GRACE)
    }

    pub(super) fn into_error(self) -> Error {
        self.error
    }

    pub(super) fn completed_operation(&self, ops: &[NfsOp<'_>], op: OpCode) -> bool {
        let completed_user_ops = self.reply_index.unwrap_or_default().saturating_sub(1);
        ops.iter()
            .take(completed_user_ops)
            .any(|candidate| candidate.opcode() == op)
    }
}

impl From<Error> for CompoundDecodeError {
    fn from(error: Error) -> Self {
        Self {
            status: None,
            reply_index: None,
            error,
        }
    }
}

pub(super) fn transient_compound_retry_is_safe(
    ops: &[NfsOp<'_>],
    err: &CompoundDecodeError,
) -> bool {
    let Some(failed_reply) = err.reply_index else {
        return false;
    };
    let completed_user_ops = failed_reply.saturating_sub(1);
    !ops.iter().take(completed_user_ops).any(|op| {
        matches!(
            op,
            NfsOp::Open { .. } | NfsOp::Close { .. } | NfsOp::Link(_) | NfsOp::Rename { .. }
        )
    })
}

pub(super) fn decode_exchange_id_compound(bytes: Bytes) -> Result<ExchangeIdResult, Error> {
    decode_setup_compound(bytes, OpCode::ExchangeId, "EXCHANGE_ID", decode_exchange_id)
}

pub(super) fn decode_create_session_compound(
    bytes: Bytes,
    expected_sequenceid: u32,
    minimum_request_size: usize,
) -> Result<CreateSessionResult, Error> {
    decode_setup_compound(bytes, OpCode::CreateSession, "CREATE_SESSION", |dec| {
        decode_create_session(dec, expected_sequenceid, minimum_request_size)
    })
}

pub(super) fn decode_setup_compound<T>(
    bytes: Bytes,
    expected: OpCode,
    shape: &'static str,
    decode: impl FnOnce(&mut XdrDecoder) -> Result<T, Error>,
) -> Result<T, Error> {
    let mut replies =
        CompoundReplies::new(bytes, shape, 1).map_err(CompoundDecodeError::into_error)?;
    let mut result = None;
    if replies
        .advance(expected)
        .map_err(CompoundDecodeError::into_error)?
    {
        result = Some(decode(&mut replies.dec)?);
    }
    replies.finish().map_err(CompoundDecodeError::into_error)?;
    result
        .ok_or_else(|| Error::protocol(format!("compound response did not include {shape} result")))
}

pub(super) fn decode_no_result_compound(
    bytes: Bytes,
    expected: OpCode,
) -> Result<(), CompoundDecodeError> {
    let mut replies = CompoundReplies::new(bytes, expected.name(), 2)?;
    replies.sequence()?;
    let saw_expected = replies.advance(expected)?;
    replies.finish()?;
    if saw_expected {
        Ok(())
    } else {
        Err(Error::protocol(format!(
            "compound response did not include {expected:?} result"
        ))
        .into())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RemoveOutcome {
    Removed,
    Missing,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum CreateDirOutcome {
    Created,
    Exists,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum CreateDirHandleOutcome {
    Created(FileHandle),
    Exists,
}

/// Outcome of a compound built around a guarded `OPEN`(create): the name was
/// created and opened, or was already present.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum OpenOutcome<T> {
    Opened(T),
    Exists,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum CloseShape {
    File,
    Rename { parent_lookup_count: usize },
    LinkRemove { parent_lookup_count: usize },
}

impl CloseShape {
    fn reply_count(self) -> usize {
        match self {
            Self::File => 3,
            Self::Rename {
                parent_lookup_count,
            } => 7 + parent_lookup_count,
            Self::LinkRemove {
                parent_lookup_count,
            } => 8 + parent_lookup_count,
        }
    }
}

pub(super) fn decode_compound_header(
    dec: &mut XdrDecoder,
) -> Result<(NfsStatus, usize), CompoundDecodeError> {
    let (compound_status, reply_count) = dec.read_u32_skip_opaque_and_read_u32()?;
    let compound_status = NfsStatus(compound_status);
    let reply_count = reply_count as usize;
    if reply_count > dec.remaining() / 8 {
        return Err(Error::xdr(format!(
            "compound reply count {reply_count} exceeds remaining XDR data"
        ))
        .into());
    }
    Ok((compound_status, reply_count))
}

/// Walks the operation replies of a compound response. A server stops
/// replying at the first failed operation, so each expectation decodes only
/// while replies remain; `finish` then validates the overall reply count and
/// surfaces the compound status of a short reply sequence.
pub(super) struct CompoundReplies {
    dec: XdrDecoder,
    status: NfsStatus,
    reply_count: usize,
    expected_replies: usize,
    decoded: usize,
    shape: &'static str,
}

impl CompoundReplies {
    fn new(
        bytes: Bytes,
        shape: &'static str,
        expected_replies: usize,
    ) -> Result<Self, CompoundDecodeError> {
        let mut dec = XdrDecoder::new(bytes);
        let (status, reply_count) = decode_compound_header(&mut dec)?;
        if reply_count > expected_replies {
            return Err(reply_count_error(shape, reply_count, expected_replies));
        }
        Ok(Self {
            dec,
            status,
            reply_count,
            expected_replies,
            decoded: 0,
            shape,
        })
    }

    /// Consumes the next reply header when one remains, expecting `op`.
    /// Returns true when the reply is present, so its result body can be
    /// decoded from `dec`.
    fn advance(&mut self, op: OpCode) -> Result<bool, CompoundDecodeError> {
        self.advance_with(|dec, shape| decode_expected_op(dec, op, shape))
            .map(|present| present.is_some())
    }

    /// Like `advance` for operations with custom status handling; `decode`
    /// runs only when a reply remains.
    fn advance_with<T>(
        &mut self,
        decode: impl FnOnce(&mut XdrDecoder, &'static str) -> Result<T, CompoundDecodeError>,
    ) -> Result<Option<T>, CompoundDecodeError> {
        if self.decoded >= self.reply_count {
            return Ok(None);
        }
        let out = decode(&mut self.dec, self.shape).map_err(|err| err.at_reply(self.decoded))?;
        self.decoded += 1;
        Ok(Some(out))
    }

    fn sequence(&mut self) -> Result<(), CompoundDecodeError> {
        if self.advance(OpCode::Sequence)? {
            skip_sequence_data(&mut self.dec)?;
        }
        Ok(())
    }

    /// Consumes an expected reply and decodes its result body when present.
    fn result<T>(
        &mut self,
        op: OpCode,
        decode: impl FnOnce(&mut XdrDecoder) -> Result<T, Error>,
    ) -> Result<Option<T>, CompoundDecodeError> {
        if !self.advance(op)? {
            return Ok(None);
        }
        Ok(Some(decode(&mut self.dec)?))
    }

    /// Consumes a GETFH reply and returns its handle when present.
    fn getfh(&mut self) -> Result<Option<FileHandle>, CompoundDecodeError> {
        self.result(OpCode::GetFh, decode_file_handle)
    }

    /// Consumes a guarded-create OPEN reply, mapping OPEN+EXIST to a typed
    /// outcome instead of an error.
    fn open_stateid_or_exists(
        &mut self,
    ) -> Result<Option<OpenOutcome<StateId>>, CompoundDecodeError> {
        let Some(exists) =
            self.advance_with(|dec, shape| decode_expected_op_or_exist(dec, OpCode::Open, shape))?
        else {
            return Ok(None);
        };
        if exists {
            return Ok(Some(OpenOutcome::Exists));
        }
        Ok(Some(OpenOutcome::Opened(decode_open_stateid(
            &mut self.dec,
        )?)))
    }

    fn lookups(&mut self, count: usize) -> Result<(), CompoundDecodeError> {
        for _ in 0..count {
            if !self.advance(OpCode::Lookup)? {
                break;
            }
        }
        Ok(())
    }

    /// Consumes a RENAME reply, returning true when the publish completed.
    fn rename_publish(&mut self) -> Result<bool, CompoundDecodeError> {
        if !self.advance(OpCode::Rename)? {
            return Ok(false);
        }
        skip_change_info(&mut self.dec)?;
        skip_change_info(&mut self.dec)?;
        Ok(true)
    }

    /// Consumes the LINK+REMOVE publish tail (SAVEFH, PUTFH, lookups, LINK,
    /// REMOVE), returning true when the publish completed.
    fn link_remove_publish(
        &mut self,
        parent_lookup_count: usize,
    ) -> Result<bool, CompoundDecodeError> {
        self.advance(OpCode::SaveFh)?;
        self.advance(OpCode::PutFh)?;
        self.lookups(parent_lookup_count)?;
        if self.advance(OpCode::Link)? {
            skip_change_info(&mut self.dec)?;
        }
        if !self.advance(OpCode::Remove)? {
            return Ok(false);
        }
        skip_change_info(&mut self.dec)?;
        Ok(true)
    }

    fn finish(self) -> Result<(), CompoundDecodeError> {
        if self.decoded < self.reply_count {
            return Err(reply_count_error(
                self.shape,
                self.reply_count,
                self.expected_replies,
            ));
        }
        if !self.status.is_ok() {
            return Err(CompoundDecodeError::nfs(self.status, OpCode::Illegal));
        }
        Ok(())
    }
}

pub(super) fn reply_count_error(
    shape: &'static str,
    reply_count: usize,
    expected_replies: usize,
) -> CompoundDecodeError {
    Error::protocol(format!(
        "{shape} compound returned {reply_count} replies, expected {expected_replies}"
    ))
    .into()
}

pub(super) fn decode_expected_op(
    dec: &mut XdrDecoder,
    expected: OpCode,
    shape: &str,
) -> Result<(), CompoundDecodeError> {
    let (op_raw, status_raw) = dec.read_u32_pair()?;
    let status = NfsStatus(status_raw);
    let op = validate_expected_reply_op(op_raw, status, expected, shape)?;
    if !status.is_ok() {
        return Err(CompoundDecodeError::nfs(status, op));
    }
    Ok(())
}

pub(super) fn validate_expected_reply_op(
    op_raw: u32,
    status: NfsStatus,
    expected: OpCode,
    shape: &str,
) -> Result<OpCode, CompoundDecodeError> {
    if op_raw == expected as u32 {
        return Ok(expected);
    }

    // nfs_resop4 uses OP_ILLEGAL as its discriminant when the requested
    // operation is not supported. This is the only protocol-valid case in
    // which a reply position has a different opcode from the request.
    if op_raw == OpCode::Illegal as u32 && status.0 == status::OP_ILLEGAL {
        return Ok(OpCode::Illegal);
    }

    let op = OpCode::from_u32(op_raw)?;
    Err(Error::protocol(format!(
        "{shape} compound returned unexpected {op:?} reply, expected {expected:?}"
    ))
    .into())
}

pub(super) fn decode_read_compound(bytes: Bytes) -> Result<ReadData, CompoundDecodeError> {
    let mut replies = CompoundReplies::new(bytes, "READ", 3)?;
    replies.sequence()?;
    replies.advance(OpCode::PutFh)?;
    let read = replies.result(OpCode::Read, decode_read_data)?;
    replies.finish()?;
    read.ok_or_else(|| Error::protocol("compound response did not include READ result").into())
}

pub(super) fn decode_read_path_compound(
    bytes: Bytes,
    lookup_count: usize,
) -> Result<ReadData, CompoundDecodeError> {
    let mut replies = CompoundReplies::new(bytes, "READ path", 3 + lookup_count)?;
    replies.sequence()?;
    replies.advance(OpCode::PutFh)?;
    replies.lookups(lookup_count)?;
    let read = replies.result(OpCode::Read, decode_read_data)?;
    replies.finish()?;
    read.ok_or_else(|| Error::protocol("compound response did not include READ result").into())
}

pub(super) fn decode_fused_put_compound(
    bytes: Bytes,
    parent_lookup_count: usize,
    create_new: bool,
) -> Result<OpenOutcome<(StateId, FileHandle, WriteData)>, CompoundDecodeError> {
    let expected_replies = if create_new {
        11 + 2 * parent_lookup_count
    } else {
        10 + parent_lookup_count
    };
    let mut replies = CompoundReplies::new(bytes, "fused PUT", expected_replies)?;
    replies.sequence()?;
    replies.advance(OpCode::PutFh)?;
    replies.lookups(parent_lookup_count)?;
    if !create_new {
        replies.advance(OpCode::SaveFh)?;
    }
    let stateid = match replies.open_stateid_or_exists()? {
        Some(OpenOutcome::Exists) => return Ok(OpenOutcome::Exists),
        Some(OpenOutcome::Opened(stateid)) => Some(stateid),
        None => None,
    };
    let fh = replies.getfh()?;
    let write = replies.result(OpCode::Write, decode_write_data)?;
    if replies.advance(OpCode::Commit)? {
        let commit_verifier = replies.dec.read_bytes::<8>()?;
        let write = write.as_ref().ok_or_else(|| {
            Error::protocol("fused PUT response included COMMIT without a WRITE result")
        })?;
        if write.requires_commit() {
            ensure_write_verifier(write.verifier, commit_verifier)?;
        }
    }
    replies.advance(OpCode::Verify)?;
    let published = if create_new {
        replies.link_remove_publish(parent_lookup_count)?
    } else {
        replies.advance(OpCode::RestoreFh)?;
        replies.rename_publish()?
    };
    replies.finish()?;

    if !published {
        return Err(missing_publish_result());
    }
    let stateid =
        stateid.ok_or_else(|| Error::protocol("compound response did not include OPEN result"))?;
    let fh = fh.ok_or_else(|| Error::protocol("compound response did not include GETFH result"))?;
    let write =
        write.ok_or_else(|| Error::protocol("compound response did not include WRITE result"))?;
    Ok(OpenOutcome::Opened((stateid, fh, write)))
}

pub(super) fn decode_read_path_handle_compound(
    bytes: Bytes,
    lookup_count: usize,
) -> Result<(FileHandle, ReadData), CompoundDecodeError> {
    let mut replies = CompoundReplies::new(bytes, "READ path", 4 + lookup_count)?;
    replies.sequence()?;
    replies.advance(OpCode::PutFh)?;
    replies.lookups(lookup_count)?;
    let fh = replies.getfh()?;
    let read = replies.result(OpCode::Read, decode_read_data)?;
    replies.finish()?;
    let fh = fh.ok_or_else(|| Error::protocol("compound response did not include GETFH result"))?;
    let read =
        read.ok_or_else(|| Error::protocol("compound response did not include READ result"))?;
    Ok((fh, read))
}

pub(super) fn decode_read_path_size_compound(
    bytes: Bytes,
    lookup_count: usize,
) -> Result<(FileHandle, u64, ReadData), CompoundDecodeError> {
    let mut replies = CompoundReplies::new(bytes, "READ path", 5 + lookup_count)?;
    replies.sequence()?;
    replies.advance(OpCode::PutFh)?;
    replies.lookups(lookup_count)?;
    let fh = replies.getfh()?;
    let size = replies
        .result(OpCode::GetAttr, decode_fattr)?
        .and_then(|attrs| attrs.size);
    let read = replies.result(OpCode::Read, decode_read_data)?;
    replies.finish()?;
    let fh = fh.ok_or_else(|| Error::protocol("compound response did not include GETFH result"))?;
    let size = size
        .ok_or_else(|| Error::protocol("compound response did not include GETATTR size result"))?;
    let read =
        read.ok_or_else(|| Error::protocol("compound response did not include READ result"))?;
    Ok((fh, size, read))
}

pub(super) fn decode_getfh_compound(
    bytes: Bytes,
    start_op: OpCode,
    lookup_count: usize,
) -> Result<FileHandle, CompoundDecodeError> {
    let mut replies = CompoundReplies::new(bytes, "GETFH", 3 + lookup_count)?;
    replies.sequence()?;
    replies.advance(start_op)?;
    replies.lookups(lookup_count)?;
    let fh = replies.getfh()?;
    replies.finish()?;
    fh.ok_or_else(|| Error::protocol("compound response did not include GETFH result").into())
}

pub(super) fn decode_getfh_attrs_compound(
    bytes: Bytes,
    lookup_count: usize,
    attr_request: AttrRequest,
) -> Result<(FileHandle, FileAttrs), CompoundDecodeError> {
    let mut replies = CompoundReplies::new(bytes, "GETFH", 4 + lookup_count)?;
    replies.sequence()?;
    replies.advance(OpCode::PutFh)?;
    replies.lookups(lookup_count)?;
    let fh = replies.getfh()?;
    let attrs = replies.result(OpCode::GetAttr, decode_fattr)?;
    replies.finish()?;
    let fh = fh.ok_or_else(|| Error::protocol("compound response did not include GETFH result"))?;
    let attrs = attrs.unwrap_or_default();
    validate_file_attrs(attr_request, &attrs, "GETFH")?;
    Ok((fh, attrs))
}

pub(super) fn decode_open_file_compound(
    bytes: Bytes,
    parent_lookup_count: usize,
) -> Result<OpenOutcome<(StateId, FileHandle)>, CompoundDecodeError> {
    let mut replies = CompoundReplies::new(bytes, "OPEN", 4 + parent_lookup_count)?;
    replies.sequence()?;
    replies.advance(OpCode::PutFh)?;
    replies.lookups(parent_lookup_count)?;
    let stateid = match replies.open_stateid_or_exists()? {
        Some(OpenOutcome::Exists) => return Ok(OpenOutcome::Exists),
        Some(OpenOutcome::Opened(stateid)) => Some(stateid),
        None => None,
    };
    let fh = replies.getfh()?;
    replies.finish()?;
    let stateid =
        stateid.ok_or_else(|| Error::protocol("compound response did not include OPEN result"))?;
    let fh = fh.ok_or_else(|| Error::protocol("compound response did not include GETFH result"))?;
    Ok(OpenOutcome::Opened((stateid, fh)))
}

pub(super) fn validate_file_attrs(
    request: AttrRequest,
    attrs: &FileAttrs,
    shape: &str,
) -> Result<(), Error> {
    match request {
        AttrRequest::Size => {
            if attrs.size.is_none() {
                return Err(Error::protocol(format!(
                    "{shape} response did not include GETATTR size result"
                )));
            }
            Ok(())
        }
        AttrRequest::TypeAndSize => {
            if attrs.file_type.is_none() || attrs.size.is_none() {
                return Err(Error::protocol(format!(
                    "{shape} response did not include GETATTR type and size result"
                )));
            }
            Ok(())
        }
    }
}

pub(super) fn decode_close_compound(
    bytes: Bytes,
    shape: CloseShape,
    commit_verifier: Option<[u8; 8]>,
) -> Result<(), CompoundDecodeError> {
    let expected_replies = shape.reply_count() + usize::from(commit_verifier.is_some());
    let mut replies = CompoundReplies::new(bytes, "CLOSE", expected_replies)?;
    replies.sequence()?;
    replies.advance(OpCode::PutFh)?;
    if let Some(expected_verifier) = commit_verifier
        && replies.advance(OpCode::Commit)?
    {
        ensure_write_verifier(expected_verifier, replies.dec.read_bytes::<8>()?)?;
    }
    if !matches!(shape, CloseShape::File) {
        replies.advance(OpCode::Verify)?;
    }
    let saw_close = replies.advance(OpCode::Close)?;
    if saw_close {
        skip_stateid(&mut replies.dec)?;
    }

    let published = match shape {
        CloseShape::File => saw_close,
        CloseShape::Rename {
            parent_lookup_count,
        } => {
            replies.advance(OpCode::PutFh)?;
            replies.lookups(parent_lookup_count)?;
            replies.advance(OpCode::SaveFh)?;
            replies.rename_publish()?
        }
        CloseShape::LinkRemove {
            parent_lookup_count,
        } => replies.link_remove_publish(parent_lookup_count)?,
    };
    replies.finish()?;

    if !saw_close {
        return Err(Error::protocol("compound response did not include CLOSE result").into());
    }
    if !published {
        return Err(missing_publish_result());
    }
    Ok(())
}

pub(super) fn decode_close_batch_compound(
    bytes: Bytes,
    close_count: usize,
) -> Result<(), CompoundDecodeError> {
    let expected_replies = 1usize.saturating_add(close_count.saturating_mul(2));
    let mut replies = CompoundReplies::new(bytes, "batched CLOSE", expected_replies)?;
    replies.sequence()?;
    let mut decoded_closes = 0usize;
    for _ in 0..close_count {
        replies.advance(OpCode::PutFh)?;
        if replies.advance(OpCode::Close)? {
            skip_stateid(&mut replies.dec)?;
            decoded_closes += 1;
        }
    }
    replies.finish()?;
    if decoded_closes != close_count {
        return Err(Error::protocol(format!(
            "batched CLOSE response included {decoded_closes} of {close_count} CLOSE results"
        ))
        .into());
    }
    Ok(())
}

pub(super) fn decode_rename_publish_compound(
    bytes: Bytes,
    parent_lookup_count: usize,
) -> Result<(), CompoundDecodeError> {
    let mut replies = CompoundReplies::new(bytes, "RENAME publish", 4 + parent_lookup_count)?;
    replies.sequence()?;
    replies.advance(OpCode::PutFh)?;
    replies.lookups(parent_lookup_count)?;
    replies.advance(OpCode::SaveFh)?;
    let published = replies.rename_publish()?;
    replies.finish()?;
    if published {
        Ok(())
    } else {
        Err(missing_publish_result())
    }
}

pub(super) fn decode_link_remove_publish_compound(
    bytes: Bytes,
    parent_lookup_count: usize,
) -> Result<(), CompoundDecodeError> {
    let mut replies = CompoundReplies::new(bytes, "LINK+REMOVE publish", 6 + parent_lookup_count)?;
    replies.sequence()?;
    replies.advance(OpCode::PutFh)?;
    let published = replies.link_remove_publish(parent_lookup_count)?;
    replies.finish()?;
    if published {
        Ok(())
    } else {
        Err(missing_publish_result())
    }
}

pub(super) fn missing_publish_result() -> CompoundDecodeError {
    Error::protocol("compound response did not include the publish result").into()
}

pub(super) fn decode_write_compound(bytes: Bytes) -> Result<WriteData, CompoundDecodeError> {
    let mut replies = CompoundReplies::new(bytes, "WRITE", 3)?;
    replies.sequence()?;
    replies.advance(OpCode::PutFh)?;
    let write = replies.result(OpCode::Write, decode_write_data)?;
    replies.finish()?;
    write.ok_or_else(|| Error::protocol("compound response did not include WRITE result").into())
}

pub(super) fn decode_write_commit_compound(
    bytes: Bytes,
) -> Result<(WriteData, [u8; 8]), CompoundDecodeError> {
    let mut replies = CompoundReplies::new(bytes, "WRITE+COMMIT", 4)?;
    replies.sequence()?;
    replies.advance(OpCode::PutFh)?;
    let write = replies.result(OpCode::Write, decode_write_data)?;
    let commit = replies.result(OpCode::Commit, XdrDecoder::read_bytes::<8>)?;
    replies.finish()?;
    let write =
        write.ok_or_else(|| Error::protocol("compound response did not include WRITE result"))?;
    let commit =
        commit.ok_or_else(|| Error::protocol("compound response did not include COMMIT result"))?;
    Ok((write, commit))
}

pub(super) fn decode_readdir_compound(
    bytes: Bytes,
    max_entries: Option<usize>,
) -> Result<DirPage, CompoundDecodeError> {
    let mut replies = CompoundReplies::new(bytes, "READDIR", 3)?;
    replies.sequence()?;
    replies.advance(OpCode::PutFh)?;
    let page = replies.result(OpCode::ReadDir, |dec| decode_readdir(dec, max_entries))?;
    replies.finish()?;
    page.ok_or_else(|| Error::protocol("compound response did not include READDIR result").into())
}

pub(super) fn decode_readdir_path_compound(
    bytes: Bytes,
    max_entries: Option<usize>,
    parent_lookup_count: usize,
) -> Result<(FileHandle, DirPage), CompoundDecodeError> {
    let mut replies = CompoundReplies::new(bytes, "READDIR", 4 + parent_lookup_count)?;
    replies.sequence()?;
    replies.advance(OpCode::PutFh)?;
    replies.lookups(parent_lookup_count)?;
    let fh = replies.getfh()?;
    let page = replies.result(OpCode::ReadDir, |dec| decode_readdir(dec, max_entries))?;
    replies.finish()?;
    let fh = fh.ok_or_else(|| Error::protocol("compound response did not include GETFH result"))?;
    let page =
        page.ok_or_else(|| Error::protocol("compound response did not include READDIR result"))?;
    Ok((fh, page))
}

pub(super) fn decode_readdir_path_without_handle_compound(
    bytes: Bytes,
    max_entries: Option<usize>,
    parent_lookup_count: usize,
) -> Result<DirPage, CompoundDecodeError> {
    let mut replies = CompoundReplies::new(bytes, "READDIR", 3 + parent_lookup_count)?;
    replies.sequence()?;
    replies.advance(OpCode::PutFh)?;
    replies.lookups(parent_lookup_count)?;
    let page = replies.result(OpCode::ReadDir, |dec| decode_readdir(dec, max_entries))?;
    replies.finish()?;
    page.ok_or_else(|| Error::protocol("compound response did not include READDIR result").into())
}

pub(super) fn decode_create_dir_compound(
    bytes: Bytes,
    parent_lookup_count: usize,
) -> Result<CreateDirOutcome, CompoundDecodeError> {
    let mut replies = CompoundReplies::new(bytes, "CREATE directory", 3 + parent_lookup_count)?;
    replies.sequence()?;
    replies.advance(OpCode::PutFh)?;
    replies.lookups(parent_lookup_count)?;
    let mut outcome = None;
    if let Some(exists) = replies
        .advance_with(|dec, shape| decode_expected_op_or_exist(dec, OpCode::Create, shape))?
    {
        if exists {
            return Ok(CreateDirOutcome::Exists);
        }
        skip_change_info(&mut replies.dec)?;
        skip_bitmap(&mut replies.dec)?;
        outcome = Some(CreateDirOutcome::Created);
    }
    replies.finish()?;
    outcome.ok_or_else(|| Error::protocol("compound response did not include CREATE result").into())
}

pub(super) fn decode_create_dir_handle_compound(
    bytes: Bytes,
    parent_lookup_count: usize,
) -> Result<CreateDirHandleOutcome, CompoundDecodeError> {
    let mut replies =
        CompoundReplies::new(bytes, "CREATE directory handle", 4 + parent_lookup_count)?;
    replies.sequence()?;
    replies.advance(OpCode::PutFh)?;
    replies.lookups(parent_lookup_count)?;
    if let Some(exists) = replies
        .advance_with(|dec, shape| decode_expected_op_or_exist(dec, OpCode::Create, shape))?
    {
        if exists {
            return Ok(CreateDirHandleOutcome::Exists);
        }
        skip_change_info(&mut replies.dec)?;
        skip_bitmap(&mut replies.dec)?;
    }
    let fh = replies.getfh()?;
    replies.finish()?;
    fh.map(CreateDirHandleOutcome::Created).ok_or_else(|| {
        Error::protocol("compound response did not include CREATE directory handle result").into()
    })
}

/// Consumes an expected reply header for a guarded create op, mapping an
/// `EXIST` failure of that op to `true` instead of an error.
pub(super) fn decode_expected_op_or_exist(
    dec: &mut XdrDecoder,
    expected: OpCode,
    shape: &str,
) -> Result<bool, CompoundDecodeError> {
    let (op_raw, status_raw) = dec.read_u32_pair()?;
    let status = NfsStatus(status_raw);
    let op = validate_expected_reply_op(op_raw, status, expected, shape)?;
    if !status.is_ok() {
        if op == expected && status == NfsStatus::EXIST {
            return Ok(true);
        }
        return Err(CompoundDecodeError::nfs(status, op));
    }
    Ok(false)
}

pub(super) fn decode_remove_compound(
    bytes: Bytes,
    parent_lookup_count: usize,
) -> Result<RemoveOutcome, CompoundDecodeError> {
    let mut replies = CompoundReplies::new(bytes, "REMOVE", 3 + parent_lookup_count)?;
    let mut removed = false;
    let expect = |replies: &mut CompoundReplies, op| {
        replies.advance_with(|dec, _| decode_remove_expected_op(dec, op))
    };
    if let Some(missing) = expect(&mut replies, OpCode::Sequence)? {
        if missing {
            return Ok(RemoveOutcome::Missing);
        }
        skip_sequence_data(&mut replies.dec)?;
    }
    if expect(&mut replies, OpCode::PutFh)? == Some(true) {
        return Ok(RemoveOutcome::Missing);
    }
    for _ in 0..parent_lookup_count {
        match expect(&mut replies, OpCode::Lookup)? {
            Some(true) => return Ok(RemoveOutcome::Missing),
            Some(false) => {}
            None => break,
        }
    }
    if let Some(missing) = expect(&mut replies, OpCode::Remove)? {
        if missing {
            return Ok(RemoveOutcome::Missing);
        }
        skip_change_info(&mut replies.dec)?;
        removed = true;
    }
    replies.finish()?;

    if removed {
        Ok(RemoveOutcome::Removed)
    } else {
        Err(Error::protocol("compound response did not include REMOVE result").into())
    }
}

pub(super) fn decode_remove_expected_op(
    dec: &mut XdrDecoder,
    expected: OpCode,
) -> Result<bool, CompoundDecodeError> {
    let (op_raw, status_raw) = dec.read_u32_pair()?;
    let status = NfsStatus(status_raw);
    let op = validate_expected_reply_op(op_raw, status, expected, "REMOVE")?;
    if !status.is_ok() {
        if op == expected
            && status == NfsStatus::NOENT
            && matches!(expected, OpCode::PutFh | OpCode::Lookup | OpCode::Remove)
        {
            return Ok(true);
        }
        return Err(CompoundDecodeError::nfs(status, op));
    }
    Ok(false)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct SequenceResult {
    pub(super) highest_slotid: u32,
    pub(super) target_highest_slotid: u32,
    pub(super) status_flags: u32,
}

pub(super) fn decode_sequence_result(
    response: &Bytes,
    expected_sessionid: [u8; 16],
    expected_sequenceid: u32,
    expected_slotid: u32,
) -> Result<SequenceResult, Error> {
    let mut dec = XdrDecoder::new(response.clone());
    let (compound_status, reply_count) = dec.read_u32_skip_opaque_and_read_u32()?;
    if reply_count == 0 {
        let status = NfsStatus(compound_status);
        return if status.is_ok() {
            Err(Error::protocol(
                "successful compound response omitted its SEQUENCE result",
            ))
        } else {
            Err(Error::nfs(status, OpCode::Sequence))
        };
    }
    let (op, status_raw) = dec.read_u32_pair()?;
    if op != OpCode::Sequence as u32 {
        let returned = OpCode::from_u32(op)?;
        return Err(Error::protocol(format!(
            "compound response began with {returned:?}, expected SEQUENCE"
        )));
    }
    let status = NfsStatus(status_raw);
    if !status.is_ok() {
        return Err(Error::nfs(status, OpCode::Sequence));
    }

    let sessionid = dec.read_bytes::<16>()?;
    let sequenceid = dec.read_u32()?;
    let slotid = dec.read_u32()?;
    let highest_slotid = dec.read_u32()?;
    let target_highest_slotid = dec.read_u32()?;
    let status_flags = dec.read_u32()?;
    if sessionid != expected_sessionid {
        return Err(Error::protocol("SEQUENCE response session ID mismatch"));
    }
    if sequenceid != expected_sequenceid {
        return Err(Error::protocol(format!(
            "SEQUENCE response sequence ID {sequenceid} did not match request {expected_sequenceid}"
        )));
    }
    if slotid != expected_slotid {
        return Err(Error::protocol(format!(
            "SEQUENCE response slot ID {slotid} did not match request {expected_slotid}"
        )));
    }

    Ok(SequenceResult {
        highest_slotid,
        target_highest_slotid,
        status_flags,
    })
}

pub(super) fn validate_sequence_status_flags(
    status_flags: u32,
    reclaim_complete: bool,
) -> Result<(), Error> {
    let unknown = status_flags & !SEQ4_STATUS_KNOWN;
    if unknown != 0 {
        return Err(Error::protocol(format!(
            "SEQUENCE response contained unknown status flags 0x{unknown:08x}"
        )));
    }
    if status_flags & SEQ4_STATUS_STATE_LOST != 0 {
        return Err(Error::connection_lost(format!(
            "NFS server reported revoked client state (SEQUENCE flags 0x{status_flags:08x})"
        )));
    }
    if reclaim_complete && status_flags & SEQ4_STATUS_RESTART_RECLAIM_NEEDED != 0 {
        return Err(Error::connection_lost(
            "NFS server restarted and requires state recovery",
        ));
    }
    if status_flags & SEQ4_STATUS_BACKCHANNEL_FAULT != 0 {
        return Err(Error::connection_lost(
            "NFS server reported a fatal session backchannel fault",
        ));
    }
    if status_flags & SEQ4_STATUS_LEASE_MOVED != 0 {
        return Err(Error::Unsupported(
            "NFS server reported migrated lease state".to_owned(),
        ));
    }

    // Callback, callback-GSS, and device-ID notifications need no action:
    // this client creates no backchannel, asks for no delegations, uses
    // AUTH_SYS, and does not use pNFS device IDs.
    Ok(())
}

pub(super) fn latch_fatal_sequence_flags(
    latched: &AtomicU32,
    client_owner: &ClientOwner,
    session_client_verifier: u64,
    status_flags: u32,
) -> bool {
    let previous = latched.fetch_or(status_flags, Ordering::AcqRel);
    status_flags & SEQ4_STATUS_STATE_LOST != 0
        && previous & SEQ4_STATUS_STATE_LOST == 0
        && client_owner.rotate_verifier_if_current(session_client_verifier)
}

pub(super) fn skip_sequence_data(dec: &mut XdrDecoder) -> Result<(), Error> {
    dec.skip_bytes(SEQUENCE_RESULT_LEN)
}

pub(super) fn decode_file_handle(dec: &mut XdrDecoder) -> Result<FileHandle, Error> {
    let handle = dec.read_opaque()?;
    if handle.len() > NFS4_FHSIZE {
        return Err(Error::protocol(format!(
            "server returned a {}-byte filehandle, exceeding the NFSv4 maximum of {NFS4_FHSIZE}",
            handle.len()
        )));
    }
    // `read_opaque` may be a slice of the complete RPC response. Copy this
    // small bounded value so retaining a filehandle never retains a large
    // READ or directory response allocation.
    Ok(FileHandle(Bytes::copy_from_slice(&handle)))
}

pub(super) fn decode_read_data(dec: &mut XdrDecoder) -> Result<ReadData, Error> {
    let (eof, data) = dec.read_bool_and_opaque()?;
    Ok(ReadData { eof, data })
}

pub(super) fn decode_write_data(dec: &mut XdrDecoder) -> Result<WriteData, Error> {
    let (count, committed, verifier) = dec.read_u32_pair_and_bytes()?;
    let committed = WriteStability::from_raw(committed)?;
    Ok(WriteData {
        count,
        committed,
        verifier,
    })
}

/// A changed verifier means the server rebooted between WRITE and COMMIT, so
/// the write must be treated as lost; `ConnectionLost` keeps it retryable.
pub(crate) fn ensure_write_verifier(expected: [u8; 8], actual: [u8; 8]) -> Result<(), Error> {
    if actual != expected {
        return Err(Error::connection_lost(
            "NFS write verifier changed before COMMIT completed",
        ));
    }
    Ok(())
}

pub(super) fn skip_stateid(dec: &mut XdrDecoder) -> Result<(), Error> {
    dec.skip_bytes(StateId::encoded_len())
}

pub(super) fn decode_open_stateid(dec: &mut XdrDecoder) -> Result<StateId, Error> {
    let stateid = StateId::decode(dec)?;
    skip_change_info(dec)?;
    let (rflags, attrs_set_len) = dec.read_u32_pair()?;
    skip_bitmap_after_len(dec, attrs_set_len as usize)?;
    decode_open_delegation(dec)?;
    if rflags & OPEN4_RESULT_CONFIRM != 0 {
        return Err(Error::Unsupported(
            "server requested OPEN_CONFIRM, which is obsolete in NFSv4.1".to_owned(),
        ));
    }
    Ok(stateid)
}

pub(super) fn skip_change_info(dec: &mut XdrDecoder) -> Result<(), Error> {
    let _atomic = dec.read_bool_and_skip(CHANGE_INFO_AFTER_ATOMIC_LEN)?;
    Ok(())
}

pub(super) fn skip_bitmap(dec: &mut XdrDecoder) -> Result<(), Error> {
    let len = dec.read_u32()? as usize;
    skip_bitmap_after_len(dec, len)
}

pub(super) fn skip_bitmap_after_len(dec: &mut XdrDecoder, len: usize) -> Result<(), Error> {
    let bytes = require_bitmap_words(dec, len)?;
    dec.skip_bytes_after_require(bytes);
    Ok(())
}

pub(super) fn require_bitmap_words(dec: &XdrDecoder, len: usize) -> Result<usize, Error> {
    let bytes = len
        .checked_mul(4)
        .ok_or_else(|| Error::xdr("bitmap byte length overflow"))?;
    if bytes > dec.remaining() {
        return Err(Error::xdr(format!(
            "bitmap length {len} exceeds remaining XDR words"
        )));
    }
    Ok(bytes)
}

pub(super) fn decode_open_delegation(dec: &mut XdrDecoder) -> Result<(), Error> {
    let delegation_type = dec.read_u32()?;
    match delegation_type {
        OPEN_DELEGATE_NONE => Ok(()),
        OPEN_DELEGATE_NONE_EXT => {
            let why = dec.read_u32()?;
            if why == WND4_CONTENTION || why == WND4_RESOURCE {
                let _server_will_signal = dec.read_bool()?;
            }
            Ok(())
        }
        OPEN_DELEGATE_READ | OPEN_DELEGATE_WRITE => Err(Error::Unsupported(
            "server returned an OPEN delegation; callbacks are not implemented".to_owned(),
        )),
        other => Err(Error::protocol(format!(
            "server returned invalid OPEN delegation type {other}"
        ))),
    }
}

pub(super) fn decode_fattr(dec: &mut XdrDecoder) -> Result<FileAttrs, Error> {
    let bitmap_len = dec.read_u32()? as usize;
    if bitmap_len == 1 {
        let bitmap_word = dec.read_u32()?;
        return decode_fattr_single_bitmap_word(dec, bitmap_word);
    }

    let bitmap = Bitmap::decode_after_len(dec, bitmap_len)?;
    decode_fattr_values(dec, bitmap)
}

pub(super) fn decode_fattr_single_bitmap_word(
    dec: &mut XdrDecoder,
    bitmap_word: u32,
) -> Result<FileAttrs, Error> {
    match bitmap_word {
        FATTR4_SIZE_WORD => decode_size_attr(dec),
        FATTR4_TYPE_AND_SIZE_WORD => decode_type_and_size_attrs(dec),
        _ => decode_fattr_values(dec, Bitmap::inline_word(bitmap_word)),
    }
}

pub(super) fn decode_size_attr(dec: &mut XdrDecoder) -> Result<FileAttrs, Error> {
    let len = dec.read_opaque_extent_len()?;
    require_attr_len(len, 8)?;
    let size = dec.read_u64_after_require();
    if len > 8 {
        dec.finish_opaque_after_require(len, 8)?;
    }
    Ok(FileAttrs {
        file_type: None,
        size: Some(size),
    })
}

pub(super) fn decode_type_and_size_attrs(dec: &mut XdrDecoder) -> Result<FileAttrs, Error> {
    let len = dec.read_opaque_extent_len()?;
    require_attr_len(len, 12)?;
    let file_type = FileType::from_raw(dec.read_u32_after_require());
    let size = dec.read_u64_after_require();
    if len > 12 {
        dec.finish_opaque_after_require(len, 12)?;
    }
    Ok(FileAttrs {
        file_type: Some(file_type),
        size: Some(size),
    })
}

pub(super) fn require_attr_len(actual: usize, needed: usize) -> Result<(), Error> {
    if actual < needed {
        return Err(Error::xdr(format!(
            "attribute value length {actual} is shorter than required {needed} bytes"
        )));
    }
    Ok(())
}

pub(super) fn decode_fattr_values(
    dec: &mut XdrDecoder,
    bitmap: Bitmap,
) -> Result<FileAttrs, Error> {
    let len = dec.read_opaque_extent_len()?;
    let mut value_remaining = len;
    let mut attrs = FileAttrs::default();

    for attr in bitmap.attrs() {
        match attr {
            FATTR4_TYPE => {
                attrs.file_type = Some(FileType::from_raw(read_attr_u32(
                    dec,
                    &mut value_remaining,
                    attr,
                )?));
            }
            FATTR4_SIZE => {
                attrs.size = Some(read_attr_u64(dec, &mut value_remaining, attr)?);
            }
            FATTR4_MODE => {
                let _mode = read_attr_u32(dec, &mut value_remaining, attr)?;
            }
            other => {
                return Err(Error::Unsupported(format!(
                    "server returned unrequested or unsupported attribute {other}"
                )));
            }
        }
    }

    dec.finish_opaque_after_require(len, len - value_remaining)?;
    Ok(attrs)
}

pub(super) fn read_attr_u32(
    dec: &mut XdrDecoder,
    value_remaining: &mut usize,
    attr: u32,
) -> Result<u32, Error> {
    consume_attr_bytes(value_remaining, attr, 4)?;
    Ok(dec.read_u32_after_require())
}

pub(super) fn read_attr_u64(
    dec: &mut XdrDecoder,
    value_remaining: &mut usize,
    attr: u32,
) -> Result<u64, Error> {
    consume_attr_bytes(value_remaining, attr, 8)?;
    Ok(dec.read_u64_after_require())
}

pub(super) fn consume_attr_bytes(
    value_remaining: &mut usize,
    attr: u32,
    needed: usize,
) -> Result<(), Error> {
    if *value_remaining < needed {
        return Err(Error::xdr(format!(
            "attribute {attr} requires {needed} bytes, only {} remain",
            *value_remaining
        )));
    }
    *value_remaining -= needed;
    Ok(())
}

pub(super) fn decode_readdir(
    dec: &mut XdrDecoder,
    max_entries: Option<usize>,
) -> Result<DirPage, Error> {
    let verifier = dec.read_bytes::<8>()?;
    let mut entries = Vec::with_capacity(readdir_entries_capacity(dec.remaining(), max_entries));
    let entry_limit = max_entries.unwrap_or(usize::MAX);
    let mut last_cookie = 0;

    loop {
        let has_entry = dec.read_bool()?;
        if !has_entry {
            let eof = dec.read_bool()?;
            return Ok(DirPage {
                entries,
                eof,
                verifier,
                last_cookie,
            });
        }
        if entries.len() == entry_limit {
            return Ok(DirPage {
                entries,
                eof: false,
                verifier,
                last_cookie,
            });
        }

        let cookie = dec.read_u64()?;
        let name = dec.read_string_unless(is_dot_readdir_name)?;
        skip_fattr(dec)?;
        if let Some(name) = name {
            entries.push(DirEntry { name });
        }
        last_cookie = cookie;
    }
}

pub(super) fn skip_fattr(dec: &mut XdrDecoder) -> Result<(), Error> {
    skip_bitmap(dec)?;
    dec.skip_opaque()
}

pub(super) fn readdir_entries_capacity(remaining: usize, max_entries: Option<usize>) -> usize {
    let hinted = (remaining / READDIR_ENTRY_BYTE_HINT).min(READDIR_DECODE_PREALLOC_LIMIT);
    max_entries
        .map(|max_entries| hinted.min(max_entries))
        .unwrap_or(hinted)
}

pub(super) fn is_dot_readdir_name(name: &[u8]) -> bool {
    matches!(name, b"." | b"..")
}

pub(super) fn decode_exchange_id(dec: &mut XdrDecoder) -> Result<ExchangeIdResult, Error> {
    let clientid = dec.read_u64()?;
    let sequenceid = dec.read_u32()?;
    let _flags = dec.read_u32()?;
    decode_state_protect(dec)?;
    let _server_minor_id = dec.read_u64()?;
    dec.skip_opaque()?;
    dec.skip_opaque()?;
    skip_impl_id_array(dec)?;
    Ok(ExchangeIdResult {
        clientid,
        sequenceid,
    })
}

pub(super) fn decode_create_session(
    dec: &mut XdrDecoder,
    expected_sequenceid: u32,
    minimum_request_size: usize,
) -> Result<CreateSessionResult, Error> {
    let sessionid = dec.read_bytes::<16>()?;
    let sequenceid = dec.read_u32()?;
    if sequenceid != expected_sequenceid {
        return Err(Error::protocol(format!(
            "CREATE_SESSION response sequence ID {sequenceid} did not match request {expected_sequenceid}"
        )));
    }
    let _flags = dec.read_u32()?;
    let fore_attrs = decode_channel_attrs(dec)?;
    validate_fore_channel_minimums(&fore_attrs, minimum_request_size)?;
    let _back_attrs = decode_channel_attrs(dec)?;
    Ok(CreateSessionResult {
        sessionid,
        fore_attrs,
    })
}

pub(super) fn validate_fore_channel_minimums(
    attrs: &ChannelAttrs,
    minimum_request_size: usize,
) -> Result<(), Error> {
    if attrs.maxoperations < 2 {
        return Err(Error::protocol(format!(
            "CREATE_SESSION ca_maxoperations {} cannot fit SEQUENCE plus an operation",
            attrs.maxoperations
        )));
    }
    if (attrs.max_request_size as usize) < minimum_request_size {
        return Err(Error::protocol(format!(
            "CREATE_SESSION ca_maxrequestsize {} cannot fit the minimum {minimum_request_size}-byte RPC SEQUENCE request",
            attrs.max_request_size
        )));
    }
    if attrs.max_response_size < MIN_SEQUENCE_COMPOUND_RESPONSE_SIZE {
        return Err(Error::protocol(format!(
            "CREATE_SESSION ca_maxresponsesize {} cannot fit the minimum {MIN_SEQUENCE_COMPOUND_RESPONSE_SIZE}-byte RPC SEQUENCE response",
            attrs.max_response_size
        )));
    }
    Ok(())
}

pub(super) fn decode_state_protect(dec: &mut XdrDecoder) -> Result<(), Error> {
    match dec.read_u32()? {
        SP4_NONE => Ok(()),
        SP4_MACH_CRED => Err(Error::Unsupported(
            "server requires machine-credential state protection that the client did not request"
                .to_owned(),
        )),
        SP4_SSV => Err(Error::Unsupported(
            "server requires SSV state protection; AUTH_SYS-only client cannot continue".to_owned(),
        )),
        other => Err(Error::protocol(format!(
            "unknown state protection mode {other}"
        ))),
    }
}

pub(super) fn encode_channel_attrs(
    enc: &mut XdrEncoder,
    max_request: u32,
    max_response: u32,
    slots: u32,
) {
    enc.put_u32(0);
    enc.put_u32(max_request);
    enc.put_u32(max_response);
    enc.put_u32(max_response);
    enc.put_u32(64);
    enc.put_u32(slots);
    enc.put_u32(0);
}

pub(super) fn channel_attrs_len() -> usize {
    7 * 4
}

pub(super) fn decode_channel_attrs(dec: &mut XdrDecoder) -> Result<ChannelAttrs, Error> {
    let _headerpadsize = dec.read_u32()?;
    let max_request_size = nonzero_channel_limit(dec.read_u32()?, "ca_maxrequestsize")?;
    let max_response_size = nonzero_channel_limit(dec.read_u32()?, "ca_maxresponsesize")?;
    let _maxresponsesize_cached = dec.read_u32()?;
    let maxoperations = nonzero_channel_limit(dec.read_u32()?, "ca_maxoperations")?;
    let maxrequests = nonzero_channel_limit(dec.read_u32()?, "ca_maxrequests")?;
    let rdma_ird_len = dec.read_u32()? as usize;
    if rdma_ird_len > dec.remaining() / 4 {
        return Err(Error::xdr(format!(
            "RDMA IRD array length {rdma_ird_len} exceeds remaining XDR words"
        )));
    }
    dec.skip_bytes_after_require(rdma_ird_len * 4);
    Ok(ChannelAttrs {
        max_request_size,
        max_response_size,
        maxoperations,
        maxrequests,
    })
}

pub(super) fn nonzero_channel_limit(value: u32, field: &str) -> Result<u32, Error> {
    if value == 0 {
        return Err(Error::protocol(format!(
            "CREATE_SESSION returned zero {field}"
        )));
    }
    Ok(value)
}

pub(super) fn negotiated_config(mut config: NfsConfig, fore_attrs: &ChannelAttrs) -> NfsConfig {
    config.max_request_size = config.max_request_size.min(fore_attrs.max_request_size);
    config.max_response_size = config.max_response_size.min(fore_attrs.max_response_size);

    let max_request_payload = channel_payload_size(config.max_request_size);
    let max_response_payload = channel_payload_size(config.max_response_size);

    config.write_chunk_size = config.write_chunk_size.min(max_request_payload);
    config.read_chunk_size = config.read_chunk_size.min(max_response_payload);
    config.read_granularity = config.read_granularity.min(config.read_chunk_size);
    config.readdir_maxcount = config.readdir_maxcount.min(max_response_payload);
    config.readdir_dircount = config.readdir_dircount.min(config.readdir_maxcount).max(1);
    config.max_compound_ops = fore_attrs.maxoperations;
    config
}

pub(super) fn bounded_readdir_counts(
    config: &NfsConfig,
    max_entries_hint: Option<usize>,
) -> (u32, u32) {
    let hinted_maxcount = max_entries_hint
        .map(|entries| {
            entries
                .saturating_mul(READDIR_ENTRY_BYTE_HINT)
                .max(READDIR_MIN_HINT_BYTES as usize)
                .min(u32::MAX as usize) as u32
        })
        .unwrap_or(config.readdir_maxcount);
    let maxcount = config.readdir_maxcount.min(hinted_maxcount).max(1);
    let dircount = config.readdir_dircount.min(maxcount).max(1);
    (dircount, maxcount)
}

pub(super) fn channel_payload_size(channel_size: u32) -> u32 {
    channel_size.saturating_sub(CHANNEL_PAYLOAD_RESERVE).max(1)
}

/// A COMMIT count of zero means "to end of file", which is the correct
/// request when the byte count exceeds the protocol's u32 range.
pub(crate) fn commit_count(bytes: u64) -> u32 {
    u32::try_from(bytes).unwrap_or(0)
}

pub(super) fn skip_impl_id_array(dec: &mut XdrDecoder) -> Result<(), Error> {
    let len = dec.read_u32()? as usize;
    if len > dec.remaining() / IMPLEMENTATION_ID_MIN_LEN {
        return Err(Error::xdr(format!(
            "implementation id array length {len} exceeds remaining XDR data"
        )));
    }
    for _ in 0..len {
        dec.skip_opaque()?;
        dec.skip_opaque()?;
        dec.skip_bytes(IMPLEMENTATION_ID_TIMESTAMP_LEN)?;
    }
    Ok(())
}
