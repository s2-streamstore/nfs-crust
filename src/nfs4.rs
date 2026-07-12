use std::net::SocketAddr;
use std::ops::{Deref, DerefMut};
use std::process;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;
use smallvec::SmallVec;
use tokio::sync::{Mutex, MutexGuard, Notify, Semaphore, SemaphorePermit, mpsc};
use ulid::Ulid;

use crate::error::Error;
use crate::path::parse_export;
use crate::rpc::{AuthSys, RpcClient, RpcPayload, RpcTransport};
use crate::xdr::{XdrDecoder, XdrEncoder, xdr_fixed_opaque_len, xdr_opaque_len, xdr_padding};
use tracing::Instrument;

mod codec;
mod ops;
mod session;
mod slots;
#[cfg(test)]
mod tests;

use codec::*;
pub(crate) use codec::{OpenOutcome, RemoveOutcome, ensure_write_verifier};
use ops::*;
pub(crate) use ops::{ReadData, WriteData};
pub(crate) use session::NfsSession;
#[cfg(test)]
use session::{claim_transient_retry, transient_retry_delay};
use slots::SlotTable;

const NFS_PROGRAM: u32 = 100003;
const NFS_VERSION: u32 = 4;
const NFS_MINOR_VERSION: u32 = 1;
const NFSPROC4_COMPOUND: u32 = 1;

/// NFSv4.1 status codes from RFC 5661, accessed as `status::NAME`.
pub(crate) mod status {
    pub(crate) const OK: u32 = 0;
    pub(crate) const PERM: u32 = 1;
    pub(crate) const NOENT: u32 = 2;
    pub(crate) const ACCESS: u32 = 13;
    pub(crate) const INVAL: u32 = 22;
    pub(crate) const EXIST: u32 = 17;
    pub(crate) const DELAY: u32 = 10008;
    pub(crate) const GRACE: u32 = 10013;
    pub(crate) const NOTSUPP: u32 = 10004;
    pub(crate) const WRONGSEC: u32 = 10016;
    pub(crate) const STALE_CLIENTID: u32 = 10022;
    pub(crate) const BAD_STATEID: u32 = 10025;
    #[cfg(test)]
    pub(crate) const NOT_SAME: u32 = 10027;
    pub(crate) const OP_ILLEGAL: u32 = 10044;
    pub(crate) const BADSESSION: u32 = 10052;
    pub(crate) const BADSLOT: u32 = 10053;
    pub(crate) const COMPLETE_ALREADY: u32 = 10054;
    pub(crate) const SEQ_MISORDERED: u32 = 10063;
    pub(crate) const BAD_HIGH_SLOT: u32 = 10077;
    pub(crate) const DEADSESSION: u32 = 10078;
}

const NF4REG: u32 = 1;
const NF4DIR: u32 = 2;

const FATTR4_TYPE: u32 = 1;
const FATTR4_SIZE: u32 = 4;
const FATTR4_MODE: u32 = 33;
const FATTR4_SIZE_WORD: u32 = 1 << FATTR4_SIZE;
const FATTR4_TYPE_AND_SIZE_WORD: u32 = (1 << FATTR4_TYPE) | FATTR4_SIZE_WORD;

const OPEN4_SHARE_ACCESS_WRITE: u32 = 0x0000_0002;
const OPEN4_SHARE_ACCESS_WANT_NO_DELEG: u32 = 0x0000_0400;
const OPEN4_SHARE_DENY_NONE: u32 = 0;
const OPEN4_RESULT_CONFIRM: u32 = 0x0000_0002;

const OPEN_CREATE: u32 = 1;
const CREATE_GUARDED: u32 = 1;
const CLAIM_NULL: u32 = 0;

const OPEN_DELEGATE_NONE: u32 = 0;
const OPEN_DELEGATE_READ: u32 = 1;
const OPEN_DELEGATE_WRITE: u32 = 2;
const OPEN_DELEGATE_NONE_EXT: u32 = 3;
const WND4_CONTENTION: u32 = 1;
const WND4_RESOURCE: u32 = 2;

const EXCHGID4_FLAG_USE_NON_PNFS: u32 = 0x0001_0000;
const SP4_NONE: u32 = 0;
const SP4_MACH_CRED: u32 = 1;
const SP4_SSV: u32 = 2;

const UNSTABLE4: u32 = 0;
const DATA_SYNC4: u32 = 1;
const FILE_SYNC4: u32 = 2;
const CHANNEL_PAYLOAD_RESERVE: u32 = 1024;
const DEFAULT_MAX_RESPONSE_SIZE: u32 = 1024 * 1024 + CHANNEL_PAYLOAD_RESERVE;
const READDIR_ENTRY_BYTE_HINT: usize = 512;
// AWS EFS can leave larger all-in-one publication compounds unanswered for
// minutes even though the equivalent chunked publication path is healthy.
// Keep the proven low-latency fused shape for small objects and use the
// verified multi-compound path above it.
const MAX_FUSED_PUT_BYTES: usize = 128 * 1024;

fn fused_put_size_viable(len: usize, write_chunk_size: u32) -> bool {
    len <= write_chunk_size as usize && len <= MAX_FUSED_PUT_BYTES
}
const READDIR_DECODE_PREALLOC_LIMIT: usize = 256;
const READDIR_MIN_HINT_BYTES: u32 = 4096;
const NFS4_FHSIZE: usize = 128;
const INLINE_BITMAP_WORDS: usize = 2;
/// Sized so the put compounds — the largest common shapes, up to
/// `10 + 2 * parent_depth` ops for a fused if-not-exists put — build without
/// a heap allocation for typical parent depths.
const INLINE_COMPOUND_OPS: usize = 16;
const SEQUENCE_RESULT_LEN: usize = 16 + 5 * 4;
const MIN_SEQUENCE_COMPOUND_RESPONSE_SIZE: u32 = 24 + 12 + 8 + SEQUENCE_RESULT_LEN as u32;
const CHANGE_INFO_AFTER_ATOMIC_LEN: usize = 2 * 8;
const IMPLEMENTATION_ID_TIMESTAMP_LEN: usize = 8 + 4;
const IMPLEMENTATION_ID_MIN_LEN: usize = 4 + 4 + IMPLEMENTATION_ID_TIMESTAMP_LEN;
const OPEN_OWNER_PREFIX: &str = "nfs-crust-open-";
const DEFERRED_CLOSE_QUEUE_CAPACITY: usize = 256;
const MAX_DEFERRED_CLOSE_BATCH: usize = 16;

const SEQ4_STATUS_CB_PATH_DOWN: u32 = 0x0000_0001;
const SEQ4_STATUS_CB_GSS_CONTEXTS_EXPIRING: u32 = 0x0000_0002;
const SEQ4_STATUS_CB_GSS_CONTEXTS_EXPIRED: u32 = 0x0000_0004;
const SEQ4_STATUS_EXPIRED_ALL_STATE_REVOKED: u32 = 0x0000_0008;
const SEQ4_STATUS_EXPIRED_SOME_STATE_REVOKED: u32 = 0x0000_0010;
const SEQ4_STATUS_ADMIN_STATE_REVOKED: u32 = 0x0000_0020;
const SEQ4_STATUS_RECALLABLE_STATE_REVOKED: u32 = 0x0000_0040;
const SEQ4_STATUS_LEASE_MOVED: u32 = 0x0000_0080;
const SEQ4_STATUS_RESTART_RECLAIM_NEEDED: u32 = 0x0000_0100;
const SEQ4_STATUS_CB_PATH_DOWN_SESSION: u32 = 0x0000_0200;
const SEQ4_STATUS_BACKCHANNEL_FAULT: u32 = 0x0000_0400;
const SEQ4_STATUS_DEVID_CHANGED: u32 = 0x0000_0800;
const SEQ4_STATUS_DEVID_DELETED: u32 = 0x0000_1000;
const SEQ4_STATUS_KNOWN: u32 = SEQ4_STATUS_CB_PATH_DOWN
    | SEQ4_STATUS_CB_GSS_CONTEXTS_EXPIRING
    | SEQ4_STATUS_CB_GSS_CONTEXTS_EXPIRED
    | SEQ4_STATUS_EXPIRED_ALL_STATE_REVOKED
    | SEQ4_STATUS_EXPIRED_SOME_STATE_REVOKED
    | SEQ4_STATUS_ADMIN_STATE_REVOKED
    | SEQ4_STATUS_RECALLABLE_STATE_REVOKED
    | SEQ4_STATUS_LEASE_MOVED
    | SEQ4_STATUS_RESTART_RECLAIM_NEEDED
    | SEQ4_STATUS_CB_PATH_DOWN_SESSION
    | SEQ4_STATUS_BACKCHANNEL_FAULT
    | SEQ4_STATUS_DEVID_CHANGED
    | SEQ4_STATUS_DEVID_DELETED;
const SEQ4_STATUS_STATE_LOST: u32 = SEQ4_STATUS_EXPIRED_ALL_STATE_REVOKED
    | SEQ4_STATUS_EXPIRED_SOME_STATE_REVOKED
    | SEQ4_STATUS_ADMIN_STATE_REVOKED
    | SEQ4_STATUS_RECALLABLE_STATE_REVOKED;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub(crate) enum OpCode {
    Access = 3,
    Close = 4,
    Commit = 5,
    Create = 6,
    DelegPurge = 7,
    DelegReturn = 8,
    GetAttr = 9,
    GetFh = 10,
    Link = 11,
    Lock = 12,
    LockT = 13,
    LockU = 14,
    Lookup = 15,
    LookupP = 16,
    NVerify = 17,
    Open = 18,
    OpenAttr = 19,
    OpenConfirm = 20,
    OpenDowngrade = 21,
    PutFh = 22,
    PutPubFh = 23,
    PutRootFh = 24,
    Read = 25,
    ReadDir = 26,
    ReadLink = 27,
    Remove = 28,
    Rename = 29,
    Renew = 30,
    RestoreFh = 31,
    SaveFh = 32,
    SecInfo = 33,
    SetAttr = 34,
    SetClientId = 35,
    SetClientIdConfirm = 36,
    Verify = 37,
    Write = 38,
    ReleaseLockOwner = 39,
    BackchannelCtl = 40,
    BindConnToSession = 41,
    ExchangeId = 42,
    CreateSession = 43,
    DestroySession = 44,
    FreeStateId = 45,
    GetDirDelegation = 46,
    GetDeviceInfo = 47,
    GetDeviceList = 48,
    LayoutCommit = 49,
    LayoutGet = 50,
    LayoutReturn = 51,
    SecInfoNoName = 52,
    Sequence = 53,
    SetSsv = 54,
    TestStateId = 55,
    WantDelegation = 56,
    DestroyClientId = 57,
    ReclaimComplete = 58,
    Illegal = 10044,
}

impl OpCode {
    fn from_u32(value: u32) -> Result<Self, Error> {
        let op = match value {
            3 => Self::Access,
            4 => Self::Close,
            5 => Self::Commit,
            6 => Self::Create,
            7 => Self::DelegPurge,
            8 => Self::DelegReturn,
            9 => Self::GetAttr,
            10 => Self::GetFh,
            11 => Self::Link,
            12 => Self::Lock,
            13 => Self::LockT,
            14 => Self::LockU,
            15 => Self::Lookup,
            16 => Self::LookupP,
            17 => Self::NVerify,
            18 => Self::Open,
            19 => Self::OpenAttr,
            20 => Self::OpenConfirm,
            21 => Self::OpenDowngrade,
            22 => Self::PutFh,
            23 => Self::PutPubFh,
            24 => Self::PutRootFh,
            25 => Self::Read,
            26 => Self::ReadDir,
            27 => Self::ReadLink,
            28 => Self::Remove,
            29 => Self::Rename,
            30 => Self::Renew,
            31 => Self::RestoreFh,
            32 => Self::SaveFh,
            33 => Self::SecInfo,
            34 => Self::SetAttr,
            35 => Self::SetClientId,
            36 => Self::SetClientIdConfirm,
            37 => Self::Verify,
            38 => Self::Write,
            39 => Self::ReleaseLockOwner,
            40 => Self::BackchannelCtl,
            41 => Self::BindConnToSession,
            42 => Self::ExchangeId,
            43 => Self::CreateSession,
            44 => Self::DestroySession,
            45 => Self::FreeStateId,
            46 => Self::GetDirDelegation,
            47 => Self::GetDeviceInfo,
            48 => Self::GetDeviceList,
            49 => Self::LayoutCommit,
            50 => Self::LayoutGet,
            51 => Self::LayoutReturn,
            52 => Self::SecInfoNoName,
            53 => Self::Sequence,
            54 => Self::SetSsv,
            55 => Self::TestStateId,
            56 => Self::WantDelegation,
            57 => Self::DestroyClientId,
            58 => Self::ReclaimComplete,
            10044 => Self::Illegal,
            other => return Err(Error::protocol(format!("unknown NFS operation {other}"))),
        };
        Ok(op)
    }

    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::Access => "ACCESS",
            Self::Close => "CLOSE",
            Self::Commit => "COMMIT",
            Self::Create => "CREATE",
            Self::DelegPurge => "DELEGPURGE",
            Self::DelegReturn => "DELEGRETURN",
            Self::GetAttr => "GETATTR",
            Self::GetFh => "GETFH",
            Self::Link => "LINK",
            Self::Lock => "LOCK",
            Self::LockT => "LOCKT",
            Self::LockU => "LOCKU",
            Self::Lookup => "LOOKUP",
            Self::LookupP => "LOOKUPP",
            Self::NVerify => "NVERIFY",
            Self::Open => "OPEN",
            Self::OpenAttr => "OPENATTR",
            Self::OpenConfirm => "OPEN_CONFIRM",
            Self::OpenDowngrade => "OPEN_DOWNGRADE",
            Self::PutFh => "PUTFH",
            Self::PutPubFh => "PUTPUBFH",
            Self::PutRootFh => "PUTROOTFH",
            Self::Read => "READ",
            Self::ReadDir => "READDIR",
            Self::ReadLink => "READLINK",
            Self::Remove => "REMOVE",
            Self::Rename => "RENAME",
            Self::Renew => "RENEW",
            Self::RestoreFh => "RESTOREFH",
            Self::SaveFh => "SAVEFH",
            Self::SecInfo => "SECINFO",
            Self::SetAttr => "SETATTR",
            Self::SetClientId => "SETCLIENTID",
            Self::SetClientIdConfirm => "SETCLIENTID_CONFIRM",
            Self::Verify => "VERIFY",
            Self::Write => "WRITE",
            Self::ReleaseLockOwner => "RELEASE_LOCKOWNER",
            Self::BackchannelCtl => "BACKCHANNEL_CTL",
            Self::BindConnToSession => "BIND_CONN_TO_SESSION",
            Self::ExchangeId => "EXCHANGE_ID",
            Self::CreateSession => "CREATE_SESSION",
            Self::DestroySession => "DESTROY_SESSION",
            Self::FreeStateId => "FREE_STATEID",
            Self::GetDirDelegation => "GET_DIR_DELEGATION",
            Self::GetDeviceInfo => "GETDEVICEINFO",
            Self::GetDeviceList => "GETDEVICELIST",
            Self::LayoutCommit => "LAYOUTCOMMIT",
            Self::LayoutGet => "LAYOUTGET",
            Self::LayoutReturn => "LAYOUTRETURN",
            Self::SecInfoNoName => "SECINFO_NO_NAME",
            Self::Sequence => "SEQUENCE",
            Self::SetSsv => "SET_SSV",
            Self::TestStateId => "TEST_STATEID",
            Self::WantDelegation => "WANT_DELEGATION",
            Self::DestroyClientId => "DESTROY_CLIENTID",
            Self::ReclaimComplete => "RECLAIM_COMPLETE",
            Self::Illegal => "ILLEGAL",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct NfsStatus(pub(crate) u32);

impl NfsStatus {
    pub(crate) const NOENT: Self = Self(status::NOENT);
    pub(crate) const EXIST: Self = Self(status::EXIST);
    pub(crate) const DELAY: Self = Self(status::DELAY);
    pub(crate) const GRACE: Self = Self(status::GRACE);
    pub(crate) const COMPLETE_ALREADY: Self = Self(status::COMPLETE_ALREADY);
    #[cfg(test)]
    pub(crate) const BADSESSION: Self = Self(status::BADSESSION);

    pub(crate) fn code(self) -> u32 {
        self.0
    }

    pub(crate) fn is_ok(self) -> bool {
        self.0 == status::OK
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FileHandle(Bytes);

impl FileHandle {
    fn encode(&self, enc: &mut XdrEncoder) {
        enc.put_opaque(&self.0);
    }

    fn encoded_len(&self) -> usize {
        xdr_opaque_len(self.0.len())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct StateId {
    seqid: u32,
    other: [u8; 12],
}

impl StateId {
    fn anonymous() -> Self {
        Self {
            seqid: 0,
            other: [0; 12],
        }
    }

    /// The special stateid designating the compound's current stateid, set by
    /// a preceding OPEN in the same compound (RFC 5661 section 16.2.3.1.2).
    fn current() -> Self {
        Self {
            seqid: 1,
            other: [0; 12],
        }
    }

    fn encode(&self, enc: &mut XdrEncoder) {
        enc.put_u32(self.seqid);
        enc.put_bytes(&self.other);
    }

    fn encoded_len() -> usize {
        4 + xdr_fixed_opaque_len(12)
    }

    fn decode(dec: &mut XdrDecoder) -> Result<Self, Error> {
        let (seqid, other) = dec.read_u32_and_bytes()?;
        Ok(Self { seqid, other })
    }
}

#[derive(Debug, Clone)]
pub(crate) struct OpenedFile {
    fh: FileHandle,
    stateid: StateId,
    close_seqid: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AttrRequest {
    Size,
    TypeAndSize,
}

impl AttrRequest {
    fn bitmap(self) -> Bitmap {
        match self {
            Self::Size => Bitmap::size_attr(),
            Self::TypeAndSize => Bitmap::type_and_size_attrs(),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct FileAttrs {
    pub(crate) file_type: Option<FileType>,
    pub(crate) size: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FileType {
    Regular,
    Directory,
    Other(u32),
}

impl FileType {
    fn from_raw(value: u32) -> Self {
        match value {
            NF4REG => Self::Regular,
            NF4DIR => Self::Directory,
            other => Self::Other(other),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct DirEntry {
    pub(crate) name: String,
}

#[derive(Debug, Clone)]
pub(crate) struct DirPage {
    pub(crate) entries: Vec<DirEntry>,
    pub(crate) eof: bool,
    pub(crate) verifier: [u8; 8],
    pub(crate) last_cookie: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct NfsConfig {
    pub(crate) export: String,
    pub(crate) read_chunk_size: u32,
    pub(crate) read_granularity: u32,
    pub(crate) write_chunk_size: u32,
    pub(crate) pipeline_write_chunks: bool,
    pub(crate) readdir_dircount: u32,
    pub(crate) readdir_maxcount: u32,
    pub(crate) file_mode: u32,
    pub(crate) dir_mode: u32,
    pub(crate) session_slots: u32,
    pub(crate) max_request_size: u32,
    pub(crate) max_response_size: u32,
    pub(crate) max_compound_ops: u32,
    pub(crate) operation_timeout: Option<Duration>,
    pub(crate) transient_retries: u32,
    pub(crate) transient_retry_delay: Duration,
}

impl NfsConfig {
    pub(crate) fn new(export: impl Into<String>) -> Self {
        Self {
            export: export.into(),
            read_chunk_size: 1024 * 1024,
            read_granularity: 128 * 1024,
            write_chunk_size: 1024 * 1024,
            pipeline_write_chunks: true,
            readdir_dircount: 64 * 1024,
            readdir_maxcount: 1024 * 1024,
            file_mode: 0o644,
            dir_mode: 0o755,
            session_slots: 64,
            max_request_size: 16 * 1024 * 1024,
            max_response_size: DEFAULT_MAX_RESPONSE_SIZE,
            max_compound_ops: u32::MAX,
            operation_timeout: Some(Duration::from_secs(30)),
            transient_retries: 5,
            transient_retry_delay: Duration::from_secs(1),
        }
    }
}

type CompoundOps<'a> = SmallVec<[NfsOp<'a>; INLINE_COMPOUND_OPS]>;

fn root_path_ops_with_extra<S>(components: &[S], extra_ops: usize) -> CompoundOps<'_>
where
    S: AsRef<str>,
{
    let mut ops = CompoundOps::with_capacity(1 + components.len() + extra_ops);
    ops.push(NfsOp::PutRootFh);
    for component in components {
        ops.push(NfsOp::Lookup(component.as_ref()));
    }
    ops
}

fn export_path_ops_with_extra<'a, S>(
    export_root: &'a FileHandle,
    components: &'a [S],
    extra_ops: usize,
) -> CompoundOps<'a>
where
    S: AsRef<str> + 'a,
{
    let mut ops = CompoundOps::with_capacity(1 + components.len() + extra_ops);
    extend_export_path_ops(&mut ops, export_root, components);
    ops
}

fn extend_export_path_ops<'a, S>(
    ops: &mut CompoundOps<'a>,
    export_root: &'a FileHandle,
    components: &'a [S],
) where
    S: AsRef<str> + 'a,
{
    ops.push(NfsOp::PutFh(export_root));
    for component in components {
        ops.push(NfsOp::Lookup(component.as_ref()));
    }
}

#[cfg(test)]
fn link_remove_publish_ops<'a, S>(
    source_fh: &'a FileHandle,
    export_root: &'a FileHandle,
    parent: &'a [S],
    to_name: &'a str,
    from_name: &'a str,
) -> CompoundOps<'a>
where
    S: AsRef<str> + 'a,
{
    let mut ops = CompoundOps::with_capacity(5 + parent.len());
    ops.push(NfsOp::PutFh(source_fh));
    ops.push(NfsOp::SaveFh);
    extend_export_path_ops(&mut ops, export_root, parent);
    ops.push(NfsOp::Link(to_name));
    ops.push(NfsOp::Remove(from_name));
    ops
}

fn write_and_commit_ops(opened: &OpenedFile, offset: u64, data: Bytes) -> [NfsOp<'_>; 3] {
    let count = commit_count(data.len() as u64);
    [
        NfsOp::PutFh(&opened.fh),
        NfsOp::Write {
            stateid: opened.stateid,
            offset,
            data,
        },
        NfsOp::Commit { offset, count },
    ]
}

fn readdir_op<'a>(
    config: &NfsConfig,
    cookie: u64,
    verifier: [u8; 8],
    max_entries_hint: Option<usize>,
) -> NfsOp<'a> {
    let (dircount, maxcount) = bounded_readdir_counts(config, max_entries_hint);
    NfsOp::ReadDir {
        cookie,
        verifier,
        dircount,
        maxcount,
        attrs: Bitmap::empty(),
    }
}

#[derive(Debug, Clone, Copy)]
struct SequenceArgs {
    sessionid: [u8; 16],
    sequenceid: u32,
    slotid: u32,
    highest_slotid: u32,
    cachethis: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct ClientOwner {
    verifier: Arc<AtomicU64>,
    ownerid: String,
    owner_counter: Arc<AtomicU64>,
}

impl ClientOwner {
    pub(crate) fn new() -> Self {
        let nonce = Ulid::new();
        let verifier = verifier_from_ulid(nonce);
        let ownerid = format!("nfs-crust:{}:{nonce}", process::id());
        Self {
            verifier: Arc::new(AtomicU64::new(verifier)),
            ownerid,
            owner_counter: Arc::new(AtomicU64::new(1)),
        }
    }

    fn verifier_value(&self) -> u64 {
        self.verifier.load(Ordering::Acquire)
    }

    fn rotate_verifier_if_current(&self, expected: u64) -> bool {
        let mut replacement = verifier_from_ulid(Ulid::new());
        if replacement == expected {
            replacement = expected.wrapping_add(1);
        }
        self.verifier
            .compare_exchange(expected, replacement, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    fn next_open_owner(&self) -> String {
        let id = self.owner_counter.fetch_add(1, Ordering::Relaxed);
        format!("{OPEN_OWNER_PREFIX}{id}")
    }
}

fn verifier_from_ulid(ulid: Ulid) -> u64 {
    let bytes = ulid.to_bytes();
    u64::from_be_bytes([
        bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15],
    ])
}
