use std::collections::HashMap;
use std::env;
use std::io::{self, IoSlice};
use std::process;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::{BufMut, Bytes, BytesMut};
use parking_lot::Mutex as SyncMutex;
use rustls::{ClientConfig, RootCertStore};
use rustls_pki_types::{CertificateDer, ServerName};
use smallvec::SmallVec;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_rustls::TlsConnector;

#[cfg(feature = "aws-efs")]
use crate::aws_efs::EfsIamConfig;
use crate::error::Error;
use crate::xdr::{XdrDecoder, XdrEncoder, xdr_opaque_len, xdr_padding};

const RPC_VERSION: u32 = 2;
const CALL: u32 = 0;
const REPLY: u32 = 1;
const MSG_ACCEPTED: u32 = 0;
const MSG_DENIED: u32 = 1;
const SUCCESS: u32 = 0;
#[cfg(test)]
const AUTH_NONE: u32 = 0;
const AUTH_SYS: u32 = 1;
const AUTH_SYS_MAX_MACHINE_NAME_LEN: usize = 255;
const AUTH_SYS_MAX_GIDS: usize = 16;
const AUTH_SYS_MAX_BODY_LEN: usize = 400;
const LAST_FRAGMENT: u32 = 0x8000_0000;
const FRAGMENT_SIZE_MASK: u32 = 0x7fff_ffff;
const ZERO_PADDING: [u8; 3] = [0; 3];
const RPC_CALL_HEADER_LEN: usize = 8 * 4;
#[cfg(test)]
const RPC_AUTH_FLAVOR_LEN: usize = 4;
const RPC_AUTH_NONE_VERIFIER: [u8; 8] = [0; 8];
const INLINE_RPC_PAYLOAD_SEGMENTS: usize = 3;
const MAX_RPC_WRITE_SLICES_PER_RECORD: usize = 6 + INLINE_RPC_PAYLOAD_SEGMENTS;
const MAX_WRITE_BATCH_RECORDS: usize = 32;
// Rounded up to a size SmallVec implements Array for.
const MAX_WRITE_BATCH_SLICES: usize =
    (MAX_WRITE_BATCH_RECORDS * (MAX_RPC_WRITE_SLICES_PER_RECORD + 1)).next_power_of_two();
/// Coalesces the 4-byte record marker and small replies into one read syscall
/// while staying below tokio's BufReader large-read bypass, which keeps big
/// READ bodies on the existing single-copy path.
const READER_BUFFER_SIZE: usize = 64 * 1024;

type BoxedReader = Box<dyn AsyncRead + Unpin + Send + 'static>;
type BoxedWriter = Box<dyn AsyncWrite + Unpin + Send + 'static>;

#[derive(Debug, Clone)]
/// AUTH_SYS credentials used for NFS RPC calls.
pub struct AuthSys {
    /// Caller machine name encoded into the AUTH_SYS credential.
    machine_name: String,
    /// Effective user id.
    uid: u32,
    /// Effective primary group id.
    gid: u32,
    /// Additional group ids.
    gids: Vec<u32>,
}

impl AuthSys {
    /// Creates AUTH_SYS credentials with no additional group ids.
    pub fn new(machine_name: impl Into<String>, uid: u32, gid: u32) -> Self {
        Self {
            machine_name: machine_name.into(),
            uid,
            gid,
            gids: Vec::new(),
        }
    }

    /// Sets additional group ids on the credential.
    pub fn with_gids(mut self, gids: impl Into<Vec<u32>>) -> Self {
        self.gids = gids.into();
        self
    }

    /// Returns the caller machine name.
    pub fn machine_name(&self) -> &str {
        &self.machine_name
    }

    /// Returns the effective user id.
    pub fn uid(&self) -> u32 {
        self.uid
    }

    /// Returns the effective primary group id.
    pub fn gid(&self) -> u32 {
        self.gid
    }

    /// Returns the additional group ids.
    pub fn gids(&self) -> &[u32] {
        &self.gids
    }

    /// Validates that this credential fits the limits of RPC `AUTH_SYS`.
    pub fn validate(&self) -> Result<(), Error> {
        if self.machine_name.len() > AUTH_SYS_MAX_MACHINE_NAME_LEN {
            return Err(Error::invalid_config(format!(
                "AUTH_SYS machine name is {} bytes; the maximum is {AUTH_SYS_MAX_MACHINE_NAME_LEN}",
                self.machine_name.len()
            )));
        }
        if self.gids.len() > AUTH_SYS_MAX_GIDS {
            return Err(Error::invalid_config(format!(
                "AUTH_SYS has {} additional groups; the maximum is {AUTH_SYS_MAX_GIDS}",
                self.gids.len()
            )));
        }

        let body_len = self.encoded_body_len()?;
        if body_len > AUTH_SYS_MAX_BODY_LEN {
            return Err(Error::invalid_config(format!(
                "AUTH_SYS credential body is {body_len} bytes; the maximum is {AUTH_SYS_MAX_BODY_LEN}"
            )));
        }
        Ok(())
    }

    /// Builds AUTH_SYS credentials from the current process user.
    ///
    /// On Unix this uses the process's effective IDs and up to the protocol
    /// maximum of 16 supplementary groups. Other platforms must provide
    /// credentials explicitly with [`Self::new`].
    pub fn current_user() -> Result<Self, Error> {
        let machine_name = env::var("HOSTNAME")
            .ok()
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| "nfs-crust".to_owned());

        #[cfg(unix)]
        {
            // SAFETY: geteuid and getegid have no preconditions and cannot fail.
            let uid = checked_unix_id(unsafe { libc::geteuid() }, "effective user id")?;
            // SAFETY: geteuid and getegid have no preconditions and cannot fail.
            let gid = checked_unix_id(unsafe { libc::getegid() }, "effective group id")?;
            let gids = current_supplementary_groups(gid)?;
            let auth = Self::new(machine_name, uid, gid).with_gids(gids);
            auth.validate()?;
            Ok(auth)
        }

        #[cfg(not(unix))]
        {
            let _ = machine_name;
            Err(Error::invalid_config(
                "AUTH_SYS credentials cannot be inferred on this platform; provide AuthSys explicitly",
            ))
        }
    }

    fn encode_body(&self) -> Result<Bytes, Error> {
        self.validate()?;
        let encoded_body_len = self.encoded_body_len()?;
        let group_count = u32::try_from(self.gids.len())
            .map_err(|_| Error::invalid_config("AUTH_SYS group count does not fit in u32"))?;
        let mut enc = XdrEncoder::with_capacity(encoded_body_len);
        enc.put_u32(auth_stamp());
        enc.put_string(&self.machine_name);
        enc.put_u32(self.uid);
        enc.put_u32(self.gid);
        enc.put_u32(group_count);
        for gid in &self.gids {
            enc.put_u32(*gid);
        }
        Ok(enc.freeze())
    }

    fn encoded_body_len(&self) -> Result<usize, Error> {
        let group_bytes = self
            .gids
            .len()
            .checked_mul(4)
            .ok_or_else(|| Error::invalid_config("AUTH_SYS group list length overflow"))?;
        16usize
            .checked_add(xdr_opaque_len(self.machine_name.len()))
            .and_then(|len| len.checked_add(group_bytes))
            .ok_or_else(|| Error::invalid_config("AUTH_SYS credential length overflow"))
    }
}

#[cfg(unix)]
fn current_supplementary_groups(primary_gid: u32) -> Result<Vec<u32>, Error> {
    loop {
        // SAFETY: a zero size permits a null pointer and queries the required length.
        let count = unsafe { libc::getgroups(0, std::ptr::null_mut()) };
        if count < 0 {
            return Err(io::Error::last_os_error().into());
        }
        let count = usize::try_from(count)
            .map_err(|_| Error::invalid_config("supplementary group count is invalid"))?;
        let mut raw_groups: Vec<libc::gid_t> = vec![0; count];
        let count_arg = libc::c_int::try_from(raw_groups.len())
            .map_err(|_| Error::invalid_config("supplementary group count is too large"))?;

        // SAFETY: raw_groups has space for count_arg gid_t values. A dangling
        // pointer is permitted for the zero-length case because it is not read.
        let actual = unsafe { libc::getgroups(count_arg, raw_groups.as_mut_ptr()) };
        if actual < 0 {
            let err = io::Error::last_os_error();
            if err.raw_os_error() == Some(libc::EINVAL) {
                // The process group list grew between the size query and read.
                continue;
            }
            return Err(err.into());
        }
        let actual = usize::try_from(actual)
            .map_err(|_| Error::invalid_config("supplementary group count is invalid"))?;
        raw_groups.truncate(actual);

        let mut gids = Vec::with_capacity(raw_groups.len().min(AUTH_SYS_MAX_GIDS));
        for raw_gid in raw_groups {
            let gid = checked_unix_id(raw_gid, "supplementary group id")?;
            if gid != primary_gid && !gids.contains(&gid) {
                gids.push(gid);
                if gids.len() == AUTH_SYS_MAX_GIDS {
                    break;
                }
            }
        }
        return Ok(gids);
    }
}

#[cfg(unix)]
fn checked_unix_id<T>(value: T, name: &str) -> Result<u32, Error>
where
    T: TryInto<u32>,
{
    value
        .try_into()
        .map_err(|_| Error::invalid_config(format!("{name} does not fit in u32")))
}

#[derive(Debug, Clone)]
/// TLS settings for RPC transport connections.
///
/// The default trust store uses the WebPKI root certificates bundled with the
/// crate dependencies. Add DER-encoded roots for private CAs or test servers.
pub struct TlsConfig {
    server_name: String,
    root_certificates_der: Vec<Vec<u8>>,
    #[cfg(feature = "aws-efs")]
    client_auth: Option<TlsClientAuth>,
}

impl TlsConfig {
    /// Creates TLS settings for the DNS name expected in the server certificate.
    pub fn new(server_name: impl Into<String>) -> Self {
        Self {
            server_name: server_name.into(),
            root_certificates_der: Vec::new(),
            #[cfg(feature = "aws-efs")]
            client_auth: None,
        }
    }

    /// Returns the configured TLS server name.
    pub fn server_name(&self) -> &str {
        &self.server_name
    }

    /// Adds a DER-encoded root certificate to the default WebPKI trust roots.
    pub fn with_root_certificate_der(mut self, certificate_der: impl Into<Vec<u8>>) -> Self {
        self.root_certificates_der.push(certificate_der.into());
        self
    }

    /// Enables AWS EFS IAM client authorization for this TLS connection.
    ///
    /// This keeps the transport TLS-based and still uses AUTH_SYS for NFS RPC
    /// credentials. It is only needed when the EFS file system policy requires
    /// IAM authorization for NFS clients.
    #[cfg(feature = "aws-efs")]
    pub fn with_efs_iam(mut self, config: EfsIamConfig) -> Self {
        self.client_auth = Some(TlsClientAuth::EfsIam(config));
        self
    }
}

#[derive(Debug, Clone)]
#[cfg(feature = "aws-efs")]
enum TlsClientAuth {
    EfsIam(EfsIamConfig),
}

#[derive(Debug, Clone)]
pub(crate) enum RpcTransport {
    Plaintext,
    Tls(TlsConfig),
}

#[derive(Debug)]
struct PendingCall {
    tx: Option<oneshot::Sender<Result<Bytes, Error>>>,
}

type PendingMap = HashMap<u32, PendingCall>;

fn pending_map_with_capacity(capacity: usize) -> PendingMap {
    HashMap::with_capacity(capacity.max(1))
}

/// Owns one pending-map entry for exactly as long as its call future exists.
///
/// The reader takes the sender but leaves the entry reserved until this guard
/// is dropped, preventing XID reuse while the call future is still alive.
#[derive(Debug)]
struct PendingRegistration {
    pending: Arc<SyncMutex<PendingMap>>,
    xid: u32,
}

impl PendingRegistration {
    fn try_insert(
        pending: &Arc<SyncMutex<PendingMap>>,
        xid: u32,
        tx: oneshot::Sender<Result<Bytes, Error>>,
    ) -> Option<Self> {
        let mut calls = pending.lock();
        if calls.contains_key(&xid) {
            return None;
        }
        calls.insert(xid, PendingCall { tx: Some(tx) });
        drop(calls);
        Some(Self {
            pending: Arc::clone(pending),
            xid,
        })
    }
}

impl Drop for PendingRegistration {
    fn drop(&mut self) {
        self.pending.lock().remove(&self.xid);
    }
}

#[derive(Debug)]
struct RpcRequest {
    header: [u8; RPC_CALL_HEADER_LEN],
    payload: RpcPayload,
    payload_padding_len: usize,
    record_len: usize,
}

/// An RPC procedure body split into a few independently owned byte ranges.
///
/// NFS WRITE compounds use three ranges so the caller's `Bytes` body can sit
/// between encoded XDR prefix and suffix data without being copied. Other RPC
/// calls use a single range.
#[derive(Debug, Clone)]
pub(crate) struct RpcPayload {
    segments: SmallVec<[Bytes; INLINE_RPC_PAYLOAD_SEGMENTS]>,
    len: usize,
}

impl RpcPayload {
    pub(crate) fn from_segments(segments: impl IntoIterator<Item = Bytes>) -> Self {
        let mut out = SmallVec::new();
        let mut len = 0usize;
        for segment in segments {
            if segment.is_empty() {
                continue;
            }
            len = len.saturating_add(segment.len());
            out.push(segment);
        }
        Self { segments: out, len }
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }

    fn segments(&self) -> impl Iterator<Item = &[u8]> {
        self.segments.iter().map(Bytes::as_ref)
    }

    #[cfg(test)]
    pub(crate) fn to_bytes(&self) -> Bytes {
        if let [segment] = self.segments.as_slice() {
            return segment.clone();
        }
        let mut out = BytesMut::with_capacity(self.len);
        for segment in &self.segments {
            out.extend_from_slice(segment);
        }
        out.freeze()
    }

    #[cfg(test)]
    pub(crate) fn segments_for_test(&self) -> &[Bytes] {
        &self.segments
    }
}

impl From<Bytes> for RpcPayload {
    fn from(payload: Bytes) -> Self {
        let len = payload.len();
        let mut segments = SmallVec::new();
        segments.push(payload);
        Self { segments, len }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct RpcClient {
    inner: Arc<RpcClientInner>,
}

#[derive(Debug)]
struct RpcClientInner {
    request_tx: mpsc::Sender<RpcRequest>,
    pending: Arc<SyncMutex<PendingMap>>,
    xid: AtomicU32,
    auth_body: Bytes,
    auth_padding_len: usize,
    call_header_template: [u8; RPC_CALL_HEADER_LEN],
    max_response_bytes: Arc<AtomicUsize>,
    transport_closed: Arc<AtomicBool>,
    reader_task: JoinHandle<()>,
    writer_task: JoinHandle<()>,
}

impl Drop for RpcClientInner {
    fn drop(&mut self) {
        self.reader_task.abort();
        self.writer_task.abort();
    }
}

impl RpcClient {
    pub(crate) async fn connect<A: ToSocketAddrs>(
        addr: A,
        auth: AuthSys,
        max_response_bytes: usize,
        pending_capacity: usize,
        transport: RpcTransport,
    ) -> Result<Self, Error> {
        let auth_body = auth.encode_body()?;
        let auth_padding_len = xdr_padding(auth_body.len());
        let call_header_template = rpc_call_header_template(auth_body.len())?;
        let (reader, writer) = connect_transport(addr, transport).await?;
        let pending = Arc::new(SyncMutex::new(pending_map_with_capacity(pending_capacity)));
        let max_response_bytes = Arc::new(AtomicUsize::new(max_response_bytes));
        let transport_closed = Arc::new(AtomicBool::new(false));
        let reader_pending = Arc::clone(&pending);
        let reader_max_response_bytes = Arc::clone(&max_response_bytes);
        let transport_closed_for_task = Arc::clone(&transport_closed);
        let transport_closed_for_loop = Arc::clone(&transport_closed);
        let reader_task = tokio::spawn(async move {
            read_loop(
                reader,
                reader_pending,
                transport_closed_for_loop,
                reader_max_response_bytes,
            )
            .await;
            transport_closed_for_task.store(true, Ordering::Release);
        });
        // Requests in flight are bounded by session slots, so the writer queue
        // never fills in practice; the capacity only bounds pathological cases.
        let (request_tx, request_rx) = mpsc::channel(pending_capacity.max(1) * 2);
        let writer_task = tokio::spawn(write_loop(
            writer,
            request_rx,
            Arc::clone(&pending),
            Arc::clone(&transport_closed),
            auth_body.clone(),
            auth_padding_len,
        ));

        Ok(Self {
            inner: Arc::new(RpcClientInner {
                request_tx,
                pending,
                xid: AtomicU32::new(auth_stamp()),
                auth_body,
                auth_padding_len,
                call_header_template,
                max_response_bytes,
                transport_closed,
                reader_task,
                writer_task,
            }),
        })
    }

    pub(crate) async fn call<P>(
        &self,
        program: u32,
        version: u32,
        procedure: u32,
        payload: P,
    ) -> Result<Bytes, Error>
    where
        P: Into<RpcPayload>,
    {
        self.call_with_timeout(program, version, procedure, payload, None)
            .await
    }

    pub(crate) async fn call_with_timeout<P>(
        &self,
        program: u32,
        version: u32,
        procedure: u32,
        payload: P,
        timeout: Option<Duration>,
    ) -> Result<Bytes, Error>
    where
        P: Into<RpcPayload>,
    {
        if self.transport_closed() {
            return Err(rpc_request_not_sent_error());
        }

        let (xid, rx, _pending_registration) = loop {
            let xid = self.next_xid();
            let (tx, rx) = oneshot::channel();
            if let Some(registration) =
                PendingRegistration::try_insert(&self.inner.pending, xid, tx)
            {
                break (xid, rx, registration);
            }
        };
        let request = self.encode_call(xid, program, version, procedure, payload.into());
        if self.transport_closed() {
            return Err(rpc_request_not_sent_error());
        }

        if self.inner.request_tx.send(request).await.is_err() {
            return Err(rpc_request_not_sent_error());
        }
        if self.transport_closed() {
            return Err(rpc_transport_stopped_error());
        }

        if let Some(timeout) = timeout {
            match tokio::time::timeout(timeout, rx).await {
                Ok(result) => result
                    .map_err(|_| Error::rpc("RPC reader task stopped before reply arrived"))?,
                Err(_) => Err(Error::Timeout(timeout)),
            }
        } else {
            rx.await
                .map_err(|_| Error::rpc("RPC reader task stopped before reply arrived"))?
        }
    }

    fn transport_closed(&self) -> bool {
        self.inner.transport_closed.load(Ordering::Acquire)
    }

    /// Tightens the transport record limit after channel negotiation.
    pub(crate) fn reduce_max_response_bytes(&self, max_response_bytes: usize) {
        self.inner
            .max_response_bytes
            .fetch_min(max_response_bytes, Ordering::AcqRel);
    }

    fn encode_call<P>(
        &self,
        xid: u32,
        program: u32,
        version: u32,
        procedure: u32,
        payload: P,
    ) -> RpcRequest
    where
        P: Into<RpcPayload>,
    {
        let payload = payload.into();
        let payload_padding_len = xdr_padding(payload.len());
        let record_len = self.encoded_call_len(payload.len());
        RpcRequest {
            header: encode_call_header_from_template(
                &self.inner.call_header_template,
                xid,
                program,
                version,
                procedure,
            ),
            payload,
            payload_padding_len,
            record_len,
        }
    }

    /// Returns the RPC record length, excluding the 4-byte transport marker,
    /// for an NFS payload of `payload_len` bytes.
    pub(crate) fn encoded_call_len(&self, payload_len: usize) -> usize {
        [
            RPC_CALL_HEADER_LEN,
            self.inner.auth_body.len(),
            self.inner.auth_padding_len,
            RPC_AUTH_NONE_VERIFIER.len(),
            payload_len,
            xdr_padding(payload_len),
        ]
        .into_iter()
        .fold(0, usize::saturating_add)
    }

    fn next_xid(&self) -> u32 {
        let xid = self
            .inner
            .xid
            .fetch_add(1, Ordering::Relaxed)
            .wrapping_add(1);
        if xid == 0 {
            self.inner
                .xid
                .fetch_add(1, Ordering::Relaxed)
                .wrapping_add(1)
        } else {
            xid
        }
    }
}

fn rpc_transport_stopped_error() -> Error {
    Error::connection_lost("RPC transport is closed")
}

fn rpc_request_not_sent_error() -> Error {
    Error::request_not_sent(rpc_transport_stopped_error())
}

async fn connect_transport<A: ToSocketAddrs>(
    addr: A,
    transport: RpcTransport,
) -> Result<(BoxedReader, BoxedWriter), Error> {
    let stream = TcpStream::connect(addr).await?;
    stream.set_nodelay(true)?;

    match transport {
        RpcTransport::Plaintext => {
            let (reader, writer) = stream.into_split();
            Ok((buffered_reader(reader), Box::new(writer)))
        }
        RpcTransport::Tls(config) => {
            let server_name = tls_server_name(&config)?;
            let tls_config = tls_client_config(&config).await?;
            let connector = TlsConnector::from(Arc::new(tls_config));
            let stream = connector.connect(server_name, stream).await?;
            Ok(split_transport(stream))
        }
    }
}

fn split_transport<T>(stream: T) -> (BoxedReader, BoxedWriter)
where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (reader, writer) = tokio::io::split(stream);
    (buffered_reader(reader), Box::new(writer))
}

fn buffered_reader<R>(reader: R) -> BoxedReader
where
    R: AsyncRead + Unpin + Send + 'static,
{
    Box::new(BufReader::with_capacity(READER_BUFFER_SIZE, reader))
}

fn tls_server_name(config: &TlsConfig) -> Result<ServerName<'static>, Error> {
    ServerName::try_from(config.server_name.clone()).map_err(|_| {
        Error::invalid_config(format!("invalid TLS server name: {}", config.server_name))
    })
}

async fn tls_client_config(config: &TlsConfig) -> Result<ClientConfig, Error> {
    let mut roots = RootCertStore {
        roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
    };
    for certificate_der in &config.root_certificates_der {
        roots
            .add(CertificateDer::from(certificate_der.clone()))
            .map_err(|err| Error::invalid_config(format!("invalid TLS root certificate: {err}")))?;
    }
    let builder = ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::aws_lc_rs::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .map_err(|err| Error::invalid_config(format!("invalid TLS protocol versions: {err}")))?
    .with_root_certificates(roots);

    #[cfg(feature = "aws-efs")]
    if let Some(client_auth) = &config.client_auth {
        let material = match client_auth {
            TlsClientAuth::EfsIam(efs_config) => {
                efs_config
                    .tls_client_auth_material(config.server_name())
                    .await?
            }
        };
        let certified_key = material.into_rustls()?;
        return Ok(builder.with_client_cert_resolver(Arc::new(
            rustls::sign::SingleCertAndKey::from(certified_key),
        )));
    }

    Ok(builder.with_no_client_auth())
}

/// Drains queued requests and writes each batch with one vectored write, so
/// concurrent callers share syscalls and TCP segments instead of writing one
/// record at a time. A write error poisons the framing of every request on
/// the connection, so it fails all pending calls and ends the transport.
async fn write_loop(
    mut writer: BoxedWriter,
    mut requests: mpsc::Receiver<RpcRequest>,
    pending: Arc<SyncMutex<PendingMap>>,
    transport_closed: Arc<AtomicBool>,
    auth_body: Bytes,
    auth_padding_len: usize,
) {
    let mut batch = Vec::with_capacity(MAX_WRITE_BATCH_RECORDS);
    while let Some(request) = requests.recv().await {
        batch.clear();
        batch.push(request);
        while batch.len() < MAX_WRITE_BATCH_RECORDS {
            match requests.try_recv() {
                Ok(request) => batch.push(request),
                Err(_) => break,
            }
        }

        if let Err(err) =
            write_record_batch(&mut writer, &batch, &auth_body, auth_padding_len).await
        {
            transport_closed.store(true, Ordering::Release);
            fail_all(&pending, err);
            return;
        }
    }
}

async fn write_record_batch<W>(
    writer: &mut W,
    batch: &[RpcRequest],
    auth_body: &[u8],
    auth_padding_len: usize,
) -> Result<(), Error>
where
    W: AsyncWrite + Unpin,
{
    let mut markers = [[0u8; 4]; MAX_WRITE_BATCH_RECORDS];
    for (request, marker) in batch.iter().zip(markers.iter_mut()) {
        if request.record_len > FRAGMENT_SIZE_MASK as usize {
            return Err(Error::rpc(format!(
                "RPC payload too large: {} bytes",
                request.record_len
            )));
        }
        *marker = (LAST_FRAGMENT | request.record_len as u32).to_be_bytes();
    }

    let auth_padding = &ZERO_PADDING[..auth_padding_len];
    let mut slices = SmallVec::<[IoSlice<'_>; MAX_WRITE_BATCH_SLICES]>::new();
    for (request, marker) in batch.iter().zip(markers.iter()) {
        let record = [
            &marker[..],
            &request.header,
            auth_body,
            auth_padding,
            &RPC_AUTH_NONE_VERIFIER,
            &ZERO_PADDING[..request.payload_padding_len],
        ];
        slices.extend(
            record[..5]
                .iter()
                .filter(|part| !part.is_empty())
                .map(|part| IoSlice::new(part)),
        );
        slices.extend(request.payload.segments().map(IoSlice::new));
        if !record[5].is_empty() {
            slices.push(IoSlice::new(record[5]));
        }
    }

    let mut remaining = &mut slices[..];
    while !remaining.is_empty() {
        let written = writer.write_vectored(remaining).await?;
        if written == 0 {
            return Err(
                io::Error::new(io::ErrorKind::WriteZero, "failed to write RPC record").into(),
            );
        }
        IoSlice::advance_slices(&mut remaining, written);
    }

    Ok(())
}

async fn read_loop(
    mut reader: BoxedReader,
    pending: Arc<SyncMutex<PendingMap>>,
    reader_closed: Arc<AtomicBool>,
    max_response_bytes: Arc<AtomicUsize>,
) {
    loop {
        let response = match read_record(&mut reader, &max_response_bytes).await {
            Ok(response) => response,
            Err(err) => {
                reader_closed.store(true, Ordering::Release);
                fail_all(&pending, err);
                return;
            }
        };

        let xid = match peek_xid(&response) {
            Ok(xid) => xid,
            Err(err) => {
                reader_closed.store(true, Ordering::Release);
                fail_all(&pending, err);
                return;
            }
        };

        let tx = pending.lock().get_mut(&xid).and_then(|call| call.tx.take());
        let Some(tx) = tx else {
            continue;
        };

        let decoded = decode_reply(xid, response);
        let _ = tx.send(decoded);
    }
}

async fn read_record<R>(reader: &mut R, max_response_bytes: &AtomicUsize) -> Result<Bytes, Error>
where
    R: AsyncRead + Unpin,
{
    let marker = reader.read_u32().await?;
    let last = marker & LAST_FRAGMENT != 0;
    let size = (marker & FRAGMENT_SIZE_MASK) as usize;

    let response_limit = max_response_bytes.load(Ordering::Acquire);
    if size > response_limit {
        return Err(Error::rpc(format!(
            "RPC response exceeded {response_limit} bytes"
        )));
    }

    let mut out = BytesMut::with_capacity(size);
    read_exact_into_buf(reader, &mut out, size).await?;
    if last {
        return Ok(out.freeze());
    }

    loop {
        let marker = reader.read_u32().await?;
        let last = marker & LAST_FRAGMENT != 0;
        let size = (marker & FRAGMENT_SIZE_MASK) as usize;

        let response_limit = max_response_bytes.load(Ordering::Acquire);
        if out.len().saturating_add(size) > response_limit {
            return Err(Error::rpc(format!(
                "RPC response exceeded {response_limit} bytes"
            )));
        }

        read_exact_into_buf(reader, &mut out, size).await?;

        if last {
            return Ok(out.freeze());
        }
    }
}

async fn read_exact_into_buf<R>(reader: &mut R, out: &mut BytesMut, len: usize) -> Result<(), Error>
where
    R: AsyncRead + Unpin,
{
    let target_len = out
        .len()
        .checked_add(len)
        .ok_or_else(|| Error::rpc("RPC response length overflow"))?;
    out.reserve(len);

    while out.len() < target_len {
        let remaining = target_len - out.len();
        let read = reader.read_buf(&mut out.limit(remaining)).await?;
        if read == 0 {
            return Err(
                io::Error::new(io::ErrorKind::UnexpectedEof, "short RPC record fragment").into(),
            );
        }
    }
    Ok(())
}

fn fail_all(pending: &SyncMutex<PendingMap>, err: Error) {
    let message: Arc<str> = err.to_string().into();
    let mut pending = pending.lock();
    for (_, call) in pending.drain() {
        if let Some(tx) = call.tx {
            let _ = tx.send(Err(Error::ConnectionLost(Arc::clone(&message))));
        }
    }
}

fn peek_xid(response: &Bytes) -> Result<u32, Error> {
    if response.len() < 4 {
        return Err(Error::rpc("short RPC reply without xid"));
    }
    Ok(u32::from_be_bytes([
        response[0],
        response[1],
        response[2],
        response[3],
    ]))
}

fn decode_reply(expected_xid: u32, response: Bytes) -> Result<Bytes, Error> {
    let mut dec = XdrDecoder::new(response);
    let (xid, message_type, reply_stat) = dec.read_u32_triple()?;
    if xid != expected_xid {
        return Err(Error::rpc(format!(
            "RPC xid mismatch: expected {expected_xid}, got {xid}"
        )));
    }

    if message_type != REPLY {
        return Err(Error::rpc(format!(
            "expected RPC reply message, got type {message_type}"
        )));
    }

    match reply_stat {
        MSG_ACCEPTED => {}
        MSG_DENIED => return Err(Error::rpc("RPC call was denied by server")),
        other => return Err(Error::rpc(format!("unknown RPC reply status {other}"))),
    }

    let (_verifier_flavor, accept_stat) = dec.read_u32_skip_opaque_and_read_u32()?;
    match accept_stat {
        SUCCESS => Ok(dec.into_remaining()),
        1 => Err(Error::rpc("RPC program unavailable")),
        2 => {
            let (low, high) = dec.read_u32_pair()?;
            Err(Error::rpc(format!(
                "RPC program version mismatch: server supports {low}..={high}"
            )))
        }
        3 => Err(Error::rpc("RPC procedure unavailable")),
        4 => Err(Error::rpc("RPC server could not decode arguments")),
        5 => Err(Error::rpc("RPC system error")),
        other => Err(Error::rpc(format!("unknown RPC accept status {other}"))),
    }
}

#[cfg(test)]
fn encode_auth_none(enc: &mut XdrEncoder) {
    enc.put_u32(AUTH_NONE);
    enc.put_opaque(&[]);
}

#[cfg(test)]
fn encode_auth_sys(enc: &mut XdrEncoder, body: &Bytes) {
    enc.put_u32(AUTH_SYS);
    enc.put_opaque(body);
}

fn rpc_call_header_template(auth_body_len: usize) -> Result<[u8; RPC_CALL_HEADER_LEN], Error> {
    let auth_body_len = u32::try_from(auth_body_len)
        .map_err(|_| Error::invalid_config("AUTH_SYS credential length does not fit in u32"))?;
    let mut header = [0; RPC_CALL_HEADER_LEN];
    put_header_u32_at(&mut header, 4, CALL);
    put_header_u32_at(&mut header, 8, RPC_VERSION);
    put_header_u32_at(&mut header, 24, AUTH_SYS);
    put_header_u32_at(&mut header, 28, auth_body_len);
    Ok(header)
}

fn encode_call_header_from_template(
    template: &[u8; RPC_CALL_HEADER_LEN],
    xid: u32,
    program: u32,
    version: u32,
    procedure: u32,
) -> [u8; RPC_CALL_HEADER_LEN] {
    let mut header = *template;
    put_header_u32_at(&mut header, 0, xid);
    put_header_u32_at(&mut header, 12, program);
    put_header_u32_at(&mut header, 16, version);
    put_header_u32_at(&mut header, 20, procedure);
    header
}

#[cfg(test)]
fn encode_call_header(
    xid: u32,
    program: u32,
    version: u32,
    procedure: u32,
    auth_body_len: usize,
) -> Result<[u8; RPC_CALL_HEADER_LEN], Error> {
    Ok(encode_call_header_from_template(
        &rpc_call_header_template(auth_body_len)?,
        xid,
        program,
        version,
        procedure,
    ))
}

fn put_header_u32_at(out: &mut [u8; RPC_CALL_HEADER_LEN], offset: usize, value: u32) {
    out[offset..offset + 4].copy_from_slice(&value.to_be_bytes());
}

#[cfg(test)]
fn decode_opaque_auth(dec: &mut XdrDecoder) -> Result<(), Error> {
    dec.skip_bytes(RPC_AUTH_FLAVOR_LEN)?;
    dec.skip_opaque()
}

fn auth_stamp() -> u32 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0));
    (now.as_nanos() as u64 ^ u64::from(process::id())).wrapping_add(now.as_secs()) as u32
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    use bytes::BufMut;
    use rustls::ServerConfig;
    use rustls_pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer};
    use tokio::net::TcpListener;
    use tokio_rustls::TlsAcceptor;

    fn test_request(auth_body: &[u8], payload: &'static [u8]) -> RpcRequest {
        RpcRequest {
            header: encode_call_header(99, 100003, 4, 1, auth_body.len()).unwrap(),
            payload: Bytes::from_static(payload).into(),
            payload_padding_len: xdr_padding(payload.len()),
            record_len: RPC_CALL_HEADER_LEN
                + auth_body.len()
                + xdr_padding(auth_body.len())
                + RPC_AUTH_NONE_VERIFIER.len()
                + payload.len()
                + xdr_padding(payload.len()),
        }
    }

    fn expected_record_bytes(request: &RpcRequest, auth_body: &[u8]) -> Vec<u8> {
        let mut expected = Vec::new();
        expected.extend_from_slice(&(LAST_FRAGMENT | request.record_len as u32).to_be_bytes());
        expected.extend_from_slice(&request.header);
        expected.extend_from_slice(auth_body);
        expected.extend_from_slice(&ZERO_PADDING[..xdr_padding(auth_body.len())]);
        expected.extend_from_slice(&RPC_AUTH_NONE_VERIFIER);
        for segment in request.payload.segments() {
            expected.extend_from_slice(segment);
        }
        expected.extend_from_slice(&ZERO_PADDING[..request.payload_padding_len]);
        expected
    }

    #[tokio::test]
    async fn record_batch_writes_header_auth_payload_and_padding() {
        let auth_body = Bytes::from_static(b"xyz");
        let request = test_request(&auth_body, b"abc");
        let mut writer = ShortWriter::new(usize::MAX);

        write_record_batch(
            &mut writer,
            std::slice::from_ref(&request),
            &auth_body,
            xdr_padding(auth_body.len()),
        )
        .await
        .unwrap();

        assert_eq!(writer.bytes, expected_record_bytes(&request, &auth_body));
        assert_eq!(writer.vectored_calls, 1);
    }

    #[tokio::test]
    async fn record_batch_coalesces_records_into_one_vectored_write() {
        let auth_body = Bytes::from_static(b"auth"); // 4-aligned: no auth padding slice
        let batch = [
            test_request(&auth_body, b"first"),
            test_request(&auth_body, b""),
            test_request(&auth_body, b"third!"),
        ];
        let mut writer = ShortWriter::new(usize::MAX);

        write_record_batch(
            &mut writer,
            &batch,
            &auth_body,
            xdr_padding(auth_body.len()),
        )
        .await
        .unwrap();

        let expected: Vec<u8> = batch
            .iter()
            .flat_map(|request| expected_record_bytes(request, &auth_body))
            .collect();
        assert_eq!(writer.bytes, expected);
        assert_eq!(writer.vectored_calls, 1);
    }

    #[tokio::test]
    async fn record_batch_handles_short_vectored_writes() {
        let auth_body = Bytes::from_static(b"xyz");
        let batch = [
            test_request(&auth_body, b"abcdef"),
            test_request(&auth_body, b"ghi"),
        ];
        let mut writer = ShortWriter::new(3);

        write_record_batch(
            &mut writer,
            &batch,
            &auth_body,
            xdr_padding(auth_body.len()),
        )
        .await
        .unwrap();

        let expected: Vec<u8> = batch
            .iter()
            .flat_map(|request| expected_record_bytes(request, &auth_body))
            .collect();
        assert_eq!(writer.bytes, expected);
        assert!(writer.vectored_calls > 1);
    }

    #[tokio::test]
    async fn record_batch_rejects_write_zero() {
        let auth_body = Bytes::from_static(b"xyz");
        let request = test_request(&auth_body, b"abc");
        let mut writer = ShortWriter::new(0);

        let err = write_record_batch(
            &mut writer,
            std::slice::from_ref(&request),
            &auth_body,
            xdr_padding(auth_body.len()),
        )
        .await
        .unwrap_err();

        assert!(matches!(err, Error::Io(err) if err.kind() == io::ErrorKind::WriteZero));
    }

    #[tokio::test]
    async fn rpc_request_caches_record_length_and_auth_padding() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (_stream, _) = listener.accept().await.unwrap();
        });

        let client = RpcClient::connect(
            addr,
            AuthSys::new("host", 1, 2),
            1024 * 1024,
            1,
            RpcTransport::Plaintext,
        )
        .await
        .unwrap();
        let request = client.encode_call(99, 100003, 4, 1, Bytes::from_static(b"abc"));

        assert_eq!(
            client.inner.auth_padding_len,
            xdr_padding(client.inner.auth_body.len())
        );
        assert_eq!(
            client.inner.call_header_template,
            rpc_call_header_template(client.inner.auth_body.len()).unwrap()
        );
        assert_eq!(
            request.record_len,
            RPC_CALL_HEADER_LEN
                + client.inner.auth_body.len()
                + client.inner.auth_padding_len
                + RPC_AUTH_NONE_VERIFIER.len()
                + 3
                + xdr_padding(3)
        );
        assert_eq!(request.record_len, client.encoded_call_len(3));
        assert_eq!(client.encoded_call_len(usize::MAX), usize::MAX);

        server.await.unwrap();
    }

    #[test]
    fn rpc_call_header_template_caches_static_words() {
        let template = rpc_call_header_template(17).unwrap();
        let header = encode_call_header_from_template(&template, 99, 100003, 4, 1);

        assert_eq!(&header[0..4], &99u32.to_be_bytes());
        assert_eq!(&header[4..8], &CALL.to_be_bytes());
        assert_eq!(&header[8..12], &RPC_VERSION.to_be_bytes());
        assert_eq!(&header[12..16], &100003u32.to_be_bytes());
        assert_eq!(&header[16..20], &4u32.to_be_bytes());
        assert_eq!(&header[20..24], &1u32.to_be_bytes());
        assert_eq!(&header[24..28], &AUTH_SYS.to_be_bytes());
        assert_eq!(&header[28..32], &17u32.to_be_bytes());
    }

    #[test]
    fn auth_sys_enforces_rfc_limits() {
        let largest = AuthSys::new("x".repeat(AUTH_SYS_MAX_MACHINE_NAME_LEN), 1, 2)
            .with_gids((0..AUTH_SYS_MAX_GIDS as u32).collect::<Vec<_>>());

        largest.validate().unwrap();
        assert!(largest.encoded_body_len().unwrap() <= AUTH_SYS_MAX_BODY_LEN);
        assert_eq!(largest.machine_name().len(), AUTH_SYS_MAX_MACHINE_NAME_LEN);
        assert_eq!(largest.uid(), 1);
        assert_eq!(largest.gid(), 2);
        assert_eq!(largest.gids().len(), AUTH_SYS_MAX_GIDS);

        let long_name = AuthSys::new("x".repeat(AUTH_SYS_MAX_MACHINE_NAME_LEN + 1), 1, 2);
        assert!(matches!(
            long_name.validate(),
            Err(Error::InvalidConfig(message)) if message.contains("machine name")
        ));

        let too_many_groups = AuthSys::new("host", 1, 2).with_gids(vec![3; AUTH_SYS_MAX_GIDS + 1]);
        assert!(matches!(
            too_many_groups.validate(),
            Err(Error::InvalidConfig(message)) if message.contains("additional groups")
        ));
    }

    #[cfg(unix)]
    #[test]
    fn current_user_uses_effective_ids_and_supplementary_groups() {
        let auth = AuthSys::current_user().unwrap();

        // SAFETY: geteuid and getegid have no preconditions and cannot fail.
        let expected_uid = checked_unix_id(unsafe { libc::geteuid() }, "effective uid").unwrap();
        // SAFETY: geteuid and getegid have no preconditions and cannot fail.
        let expected_gid = checked_unix_id(unsafe { libc::getegid() }, "effective gid").unwrap();
        assert_eq!(auth.uid(), expected_uid);
        assert_eq!(auth.gid(), expected_gid);
        assert_eq!(
            auth.gids(),
            current_supplementary_groups(auth.gid()).unwrap()
        );
        auth.validate().unwrap();
    }

    #[cfg(not(unix))]
    #[test]
    fn current_user_requires_explicit_credentials() {
        assert!(matches!(
            AuthSys::current_user(),
            Err(Error::InvalidConfig(message)) if message.contains("provide AuthSys explicitly")
        ));
    }

    #[tokio::test]
    async fn invalid_auth_sys_is_rejected_before_opening_transport() {
        let auth = AuthSys::new("host", 1, 2).with_gids(vec![3; AUTH_SYS_MAX_GIDS + 1]);

        let result =
            RpcClient::connect("127.0.0.1:0", auth, 1024, 1, RpcTransport::Plaintext).await;

        assert!(matches!(result, Err(Error::InvalidConfig(_))));
    }

    #[test]
    fn auth_sys_encoder_uses_preencoded_body() {
        let body = Bytes::from_static(b"auth-body");
        let mut enc = XdrEncoder::new();

        encode_auth_sys(&mut enc, &body);

        let mut dec = XdrDecoder::new(enc.freeze());
        assert_eq!(dec.read_u32().unwrap(), AUTH_SYS);
        assert_eq!(dec.read_opaque().unwrap(), body);
        assert_eq!(dec.remaining(), 0);
    }

    #[test]
    fn opaque_auth_decoder_skips_unused_flavor_and_body() {
        let mut enc = XdrEncoder::new();
        encode_auth_sys(&mut enc, &Bytes::from_static(b"auth-body"));
        enc.put_u32(99);

        let mut dec = XdrDecoder::new(enc.freeze());
        decode_opaque_auth(&mut dec).unwrap();
        assert_eq!(dec.read_u32().unwrap(), 99);
        assert_eq!(dec.remaining(), 0);
    }

    #[test]
    fn rpc_call_header_matches_wire_layout_without_copying_auth_body() {
        let auth = AuthSys::new("host", 1, 2).with_gids(vec![3, 4]);
        let auth_body = auth.encode_body().unwrap();
        assert_eq!(auth_body.len(), auth.encoded_body_len().unwrap());

        let header = encode_call_header(99, 100003, 4, 1, auth_body.len()).unwrap();

        assert_eq!(header.len(), RPC_CALL_HEADER_LEN);
        let mut dec = XdrDecoder::new(Bytes::copy_from_slice(&header));
        assert_eq!(dec.read_u32().unwrap(), 99);
        assert_eq!(dec.read_u32().unwrap(), CALL);
        assert_eq!(dec.read_u32().unwrap(), RPC_VERSION);
        assert_eq!(dec.read_u32().unwrap(), 100003);
        assert_eq!(dec.read_u32().unwrap(), 4);
        assert_eq!(dec.read_u32().unwrap(), 1);
        assert_eq!(dec.read_u32().unwrap(), AUTH_SYS);
        assert_eq!(dec.read_u32().unwrap(), auth_body.len() as u32);
        assert_eq!(dec.remaining(), 0);
    }

    #[tokio::test]
    async fn concurrent_calls_are_routed_by_xid() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let first = read_server_record(&mut stream).await;
            let second = read_server_record(&mut stream).await;
            let first_xid = xid(&first);
            let second_xid = xid(&second);

            write_server_record(&mut stream, rpc_success_reply(second_xid, b"two!")).await;
            write_server_record(&mut stream, rpc_success_reply(first_xid, b"one!")).await;
        });

        let client = RpcClient::connect(
            addr,
            AuthSys::new("test", 0, 0),
            1024 * 1024,
            2,
            RpcTransport::Plaintext,
        )
        .await
        .unwrap();
        assert!(client.inner.pending.lock().capacity() >= 2);
        let one = client.call(1, 1, 1, Bytes::from_static(b"first"));
        let two = client.call(1, 1, 1, Bytes::from_static(b"second"));
        let (one, two) = tokio::join!(one, two);

        assert_eq!(&one.unwrap()[..], b"one!");
        assert_eq!(&two.unwrap()[..], b"two!");
        server.await.unwrap();
    }

    #[tokio::test]
    async fn tls_transport_verifies_server_name_and_carries_rpc_records() {
        let certified = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
        let cert_der = certified.cert.der().clone();
        let root_der = cert_der.to_vec();
        let key_der = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(
            certified.signing_key.serialize_der(),
        ));
        let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
        let server_config = ServerConfig::builder_with_provider(Arc::clone(&provider))
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_no_client_auth()
            .with_single_cert(vec![cert_der], key_der)
            .unwrap();
        let acceptor = TlsAcceptor::from(Arc::new(server_config));

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut stream = acceptor.accept(stream).await.unwrap();
            let request = read_server_record(&mut stream).await;
            let xid = xid(&request);
            write_server_record(&mut stream, rpc_success_reply(xid, b"tls!")).await;
        });

        let tls = TlsConfig::new("localhost").with_root_certificate_der(root_der);
        let client = RpcClient::connect(
            addr,
            AuthSys::new("test", 0, 0),
            1024 * 1024,
            1,
            RpcTransport::Tls(tls),
        )
        .await
        .unwrap();
        let response = client.call(1, 1, 1, Bytes::from_static(b"request")).await;

        assert_eq!(&response.unwrap()[..], b"tls!");
        server.await.unwrap();
    }

    #[tokio::test]
    async fn call_with_timeout_returns_timeout_and_removes_pending_call() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let _ = read_server_record(&mut stream).await;
            tokio::time::sleep(Duration::from_millis(100)).await;
        });

        let client = RpcClient::connect(
            addr,
            AuthSys::new("test", 0, 0),
            1024 * 1024,
            1,
            RpcTransport::Plaintext,
        )
        .await
        .unwrap();
        let result = client
            .call_with_timeout(
                1,
                1,
                1,
                Bytes::from_static(b"slow"),
                Some(Duration::from_millis(10)),
            )
            .await;

        assert!(matches!(result, Err(Error::Timeout(_))));
        assert!(client.inner.pending.lock().is_empty());
        server.await.unwrap();
    }

    #[tokio::test]
    async fn cancelling_call_removes_pending_registration_and_ignores_late_reply() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (received_tx, received_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let cancelled = read_server_record(&mut stream).await;
            received_tx.send(()).unwrap();
            release_rx.await.unwrap();
            write_server_record(&mut stream, rpc_success_reply(xid(&cancelled), b"old!")).await;

            let next = read_server_record(&mut stream).await;
            write_server_record(&mut stream, rpc_success_reply(xid(&next), b"next")).await;
        });

        let client = RpcClient::connect(
            addr,
            AuthSys::new("test", 0, 0),
            1024 * 1024,
            1,
            RpcTransport::Plaintext,
        )
        .await
        .unwrap();
        let call_client = client.clone();
        let cancelled = tokio::spawn(async move {
            call_client
                .call(1, 1, 1, Bytes::from_static(b"cancelled"))
                .await
        });

        received_rx.await.unwrap();
        assert_eq!(client.inner.pending.lock().len(), 1);
        cancelled.abort();
        assert!(cancelled.await.unwrap_err().is_cancelled());
        assert!(client.inner.pending.lock().is_empty());

        release_tx.send(()).unwrap();
        let response = tokio::time::timeout(
            Duration::from_secs(1),
            client.call(1, 1, 1, Bytes::from_static(b"next")),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(&response[..], b"next");
        assert!(client.inner.pending.lock().is_empty());
        server.await.unwrap();
    }

    #[tokio::test]
    async fn response_larger_than_reduced_limit_fails_pending_call() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (received_tx, received_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let request = read_server_record(&mut stream).await;
            let xid = xid(&request);
            received_tx.send(()).unwrap();
            release_rx.await.unwrap();
            write_server_record(&mut stream, rpc_success_reply(xid, b"too-large")).await;
        });

        let client = RpcClient::connect(
            addr,
            AuthSys::new("test", 0, 0),
            1024,
            1,
            RpcTransport::Plaintext,
        )
        .await
        .unwrap();
        let call_client = client.clone();
        let call = tokio::spawn(async move {
            call_client
                .call_with_timeout(
                    1,
                    1,
                    1,
                    Bytes::from_static(b"request"),
                    Some(Duration::from_secs(30)),
                )
                .await
        });

        received_rx.await.unwrap();
        client.reduce_max_response_bytes(16);
        release_tx.send(()).unwrap();
        let result = tokio::time::timeout(Duration::from_secs(1), call)
            .await
            .unwrap()
            .unwrap();

        assert!(matches!(result, Err(Error::ConnectionLost(_))));
        assert!(client.inner.pending.lock().is_empty());
        server.await.unwrap();
    }

    #[tokio::test]
    async fn read_record_appends_fragment_data_without_prezeroing() {
        let (mut client, mut server) = tokio::io::duplex(64);
        let writer = tokio::spawn(async move {
            server.write_u32(3).await.unwrap();
            server.write_all(b"abc").await.unwrap();
            server.write_u32(LAST_FRAGMENT | 3).await.unwrap();
            server.write_all(b"def").await.unwrap();
        });

        let max_response_bytes = AtomicUsize::new(1024);
        let record = read_record(&mut client, &max_response_bytes).await.unwrap();

        assert_eq!(&record[..], b"abcdef");
        writer.await.unwrap();
    }

    #[tokio::test]
    async fn read_record_rejects_oversized_first_fragment_before_body_read() {
        let (mut client, mut server) = tokio::io::duplex(64);
        server.write_u32(LAST_FRAGMENT | 17).await.unwrap();

        let max_response_bytes = AtomicUsize::new(16);
        let err = read_record(&mut client, &max_response_bytes)
            .await
            .unwrap_err();

        assert!(matches!(err, Error::Rpc(message) if message.contains("exceeded 16 bytes")));
    }

    #[tokio::test]
    async fn pending_call_fails_fast_when_reader_observes_eof() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let _ = read_server_record(&mut stream).await;
        });

        let client = RpcClient::connect(
            addr,
            AuthSys::new("test", 0, 0),
            1024 * 1024,
            1,
            RpcTransport::Plaintext,
        )
        .await
        .unwrap();
        let result = tokio::time::timeout(Duration::from_secs(1), async {
            client
                .call_with_timeout(
                    1,
                    1,
                    1,
                    Bytes::from_static(b"pending"),
                    Some(Duration::from_secs(30)),
                )
                .await
        })
        .await
        .unwrap();

        assert!(matches!(result, Err(Error::ConnectionLost(_))));
        assert!(client.inner.pending.lock().is_empty());
        server.await.unwrap();
    }

    #[tokio::test]
    async fn fail_all_reuses_connection_lost_message_for_pending_calls() {
        let pending = SyncMutex::new(pending_map_with_capacity(0));
        let (first_tx, first_rx) = oneshot::channel();
        let (second_tx, second_rx) = oneshot::channel();
        {
            let mut calls = pending.lock();
            calls.insert(1, PendingCall { tx: Some(first_tx) });
            calls.insert(
                2,
                PendingCall {
                    tx: Some(second_tx),
                },
            );
        }

        fail_all(&pending, Error::rpc("reader stopped"));

        let first = first_rx.await.unwrap().unwrap_err();
        let second = second_rx.await.unwrap().unwrap_err();
        let (Error::ConnectionLost(first), Error::ConnectionLost(second)) = (first, second) else {
            panic!("pending calls should fail with connection loss");
        };
        assert!(Arc::ptr_eq(&first, &second));
        assert!(pending.lock().is_empty());
    }

    #[tokio::test]
    async fn call_fails_fast_after_reader_task_observes_eof() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (_stream, _) = listener.accept().await.unwrap();
        });

        let client = RpcClient::connect(
            addr,
            AuthSys::new("test", 0, 0),
            1024 * 1024,
            1,
            RpcTransport::Plaintext,
        )
        .await
        .unwrap();
        server.await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while !client.inner.transport_closed.load(Ordering::Acquire) {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();

        let result = client
            .call_with_timeout(
                1,
                1,
                1,
                Bytes::from_static(b"after-eof"),
                Some(Duration::from_secs(30)),
            )
            .await;

        assert!(result.is_err_and(|err| err.is_request_not_sent() && err.is_retryable()));
        assert!(client.inner.pending.lock().is_empty());
    }

    #[test]
    fn rpc_reply_header_uses_fixed_shape_decode() {
        let reply = rpc_success_reply(7, b"body");
        assert_eq!(&decode_reply(7, reply).unwrap()[..], b"body");

        let mut enc = XdrEncoder::new();
        enc.put_u32(7);
        enc.put_u32(REPLY);
        let err = decode_reply(7, enc.freeze()).unwrap_err();
        assert!(matches!(err, Error::Xdr(_)));

        let mut enc = XdrEncoder::new();
        enc.put_u32(7);
        enc.put_u32(REPLY);
        enc.put_u32(MSG_ACCEPTED);
        enc.put_u32(AUTH_NONE);
        enc.put_u32(0);
        let err = decode_reply(7, enc.freeze()).unwrap_err();
        assert!(matches!(err, Error::Xdr(_)));
    }

    async fn read_server_record<S>(stream: &mut S) -> Bytes
    where
        S: AsyncRead + Unpin,
    {
        let marker = stream.read_u32().await.unwrap();
        let size = (marker & FRAGMENT_SIZE_MASK) as usize;
        let mut bytes = BytesMut::zeroed(size);
        stream.read_exact(&mut bytes).await.unwrap();
        bytes.freeze()
    }

    async fn write_server_record<S>(stream: &mut S, payload: Bytes)
    where
        S: AsyncWrite + Unpin,
    {
        let mut frame = BytesMut::with_capacity(4 + payload.len());
        frame.put_u32(LAST_FRAGMENT | payload.len() as u32);
        frame.extend_from_slice(&payload);
        stream.write_all(&frame).await.unwrap();
    }

    fn xid(record: &Bytes) -> u32 {
        u32::from_be_bytes([record[0], record[1], record[2], record[3]])
    }

    fn rpc_success_reply(xid: u32, payload: &[u8]) -> Bytes {
        let mut enc = XdrEncoder::new();
        enc.put_u32(xid);
        enc.put_u32(REPLY);
        enc.put_u32(MSG_ACCEPTED);
        encode_auth_none(&mut enc);
        enc.put_u32(SUCCESS);
        enc.put_fixed_opaque(payload);
        enc.freeze()
    }

    struct ShortWriter {
        bytes: Vec<u8>,
        max_write: usize,
        vectored_calls: usize,
    }

    impl ShortWriter {
        fn new(max_write: usize) -> Self {
            Self {
                bytes: Vec::new(),
                max_write,
                vectored_calls: 0,
            }
        }
    }

    impl AsyncWrite for ShortWriter {
        fn poll_write(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            let len = buf.len().min(self.max_write);
            self.bytes.extend_from_slice(&buf[..len]);
            Poll::Ready(Ok(len))
        }

        fn poll_write_vectored(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            bufs: &[IoSlice<'_>],
        ) -> Poll<io::Result<usize>> {
            self.vectored_calls += 1;
            let mut remaining = self.max_write;
            let mut written = 0;
            for buf in bufs {
                if remaining == 0 {
                    break;
                }
                let len = buf.len().min(remaining);
                self.bytes.extend_from_slice(&buf[..len]);
                remaining -= len;
                written += len;
            }
            Poll::Ready(Ok(written))
        }

        fn is_write_vectored(&self) -> bool {
            true
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }
}
