use std::future::Future;
use std::net::SocketAddr;
use std::ops::Range;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::{Bytes, BytesMut};
use futures_util::stream::{FuturesUnordered, StreamExt};
use parking_lot::RwLock;
use smallvec::SmallVec;
use tokio::net::{ToSocketAddrs, lookup_host};
use tokio::sync::{Mutex, watch};
use tokio::time::Instant;
use ulid::Ulid;

use crate::error::Error;
use crate::nfs4::{
    AttrRequest, ClientOwner, DirPage, FileHandle, FileType, NfsConfig, NfsSession, NfsStatus,
    OpCode, OpenOutcome, OpenedFile, ReadData, RemoveOutcome, WriteData, ensure_write_verifier,
};
use crate::path::{NfsPath, TEMP_FILE_PREFIX};
use crate::rpc::{AuthSys, RpcTransport, TlsConfig};

const TEMP_FILE_CREATE_RETRIES: usize = 8;
const DEFAULT_MAX_BUFFERED_READ_SIZE: u64 = 128 * 1024 * 1024;
const MAX_OPERATION_PIPELINE_DEPTH: usize = 16;
const MAX_CONFIGURED_SESSION_SLOTS: u32 = 1024;
// Direct same-AZ EFS testing found the first repeatable multi-second stalls at
// 507 KiB. Use the largest 4 KiB-aligned size below that boundary; other NFS
// servers retain their negotiated maximum.
const AWS_EFS_MAX_WRITE_CHUNK_SIZE: u32 = 504 * 1024;
const RECONNECT_RETRIES: u32 = 1;
const MIN_RECONNECT_FAILURE_BACKOFF: Duration = Duration::from_millis(250);

#[derive(Debug, Clone)]
struct AddressResolver {
    endpoint: Arc<str>,
}

impl AddressResolver {
    fn new(endpoint: String) -> Self {
        Self {
            endpoint: endpoint.into(),
        }
    }

    async fn resolve(&self) -> Result<Vec<SocketAddr>, Error> {
        resolve_addrs(self.endpoint.as_ref()).await
    }
}

#[derive(Debug, Clone)]
enum ReconnectFailure {
    Timeout(Duration),
    Other(Arc<str>),
}

impl ReconnectFailure {
    fn from_error(error: &Error) -> Self {
        match error {
            Error::Timeout(timeout) => Self::Timeout(*timeout),
            _ => Self::Other(format!("NFS session reconnect failed: {error}").into()),
        }
    }

    fn to_error(&self) -> Error {
        match self {
            Self::Timeout(timeout) => Error::Timeout(*timeout),
            Self::Other(message) => Error::connection_lost(message.clone()),
        }
    }
}

type ReconnectOutcome = Result<(), ReconnectFailure>;

#[derive(Debug, Default)]
struct ReconnectCoordinator {
    state: ReconnectState,
}

#[derive(Debug, Default)]
enum ReconnectState {
    #[default]
    Idle,
    Connecting {
        generation: u64,
        outcome: watch::Receiver<Option<ReconnectOutcome>>,
    },
    Failed {
        generation: u64,
        retry_after: Option<Instant>,
        error: ReconnectFailure,
    },
}

#[derive(Debug, Clone)]
/// A cloneable asynchronous NFSv4.1 client for one export.
pub struct NfsClient {
    inner: Arc<ClientInner>,
}

#[derive(Debug)]
struct ClientInner {
    resolver: AddressResolver,
    client_owner: ClientOwner,
    auth: AuthSys,
    transport: RpcTransport,
    config: NfsConfig,
    connect_timeout: Option<Duration>,
    max_buffered_read_size: Option<u64>,
    session: RwLock<ClientSession>,
    reconnect: Mutex<ReconnectCoordinator>,
}

#[derive(Debug, Clone)]
struct ClientSession {
    session: NfsSession,
    generation: u64,
}

impl NfsClient {
    /// Creates a builder for a client connected to `export` on `addr`.
    pub fn builder(addr: impl Into<String>, export: impl Into<String>) -> NfsClientBuilder {
        NfsClientBuilder::new(addr, export)
    }

    /// Stores a complete file from a single byte buffer.
    ///
    /// The body is written to a uniquely named temporary file in the
    /// destination directory and atomically published at its final name, so
    /// concurrent readers never observe a partial destination and a crash
    /// never leaves a torn file there. Parent directories are created as
    /// needed. The data is committed durably and its size verified
    /// server-side before it is published. Small bodies publish in a single
    /// compound. Cleanup of the returned open state is queued on a bounded
    /// worker and may be batched with other `CLOSE` operations.
    ///
    /// [`PutMode::Overwrite`] atomically creates or replaces the
    /// destination. [`PutMode::IfNotExists`] publishes only when the
    /// destination does not already exist, failing with
    /// [`Error::AlreadyExists`] otherwise.
    ///
    /// A client that dies mid-put can leave a temporary file behind in the
    /// destination directory; [`NfsClient::sweep_temp_files`] removes stale
    /// ones. The `.nfs-crust-tmp-` path-component prefix is reserved for
    /// these internal files and is rejected in public API paths.
    ///
    /// If the atomic publish loses its definitive reply, this returns
    /// [`Error::OutcomeUnknown`] and does not replay the publish. The
    /// destination may or may not contain this body; callers must inspect or
    /// reconcile that path explicitly.
    pub async fn put(
        &self,
        path: impl AsRef<str> + Send,
        body: Bytes,
        mode: PutMode,
    ) -> Result<(), Error> {
        let raw_path = path.as_ref();
        let path = NfsPath::file(raw_path)?;
        self.retrying_session_operation(|snapshot| {
            let path = &path;
            let body = &body;
            async move { self.put_once(snapshot, path, raw_path, body, mode).await }
        })
        .await
    }

    async fn put_once(
        &self,
        snapshot: ClientSession,
        path: &NfsPath<'_>,
        raw_path: &str,
        data: &Bytes,
        mode: PutMode,
    ) -> Result<(), Error> {
        let session = &snapshot.session;
        let (parent, name) = path.parent_and_name()?;

        if let Some(result) = self
            .put_once_fused(&snapshot, parent, name, raw_path, data, mode)
            .await?
        {
            return Ok(result);
        }

        let (temp_name, mut opened) = match open_temp_file(session, parent).await {
            Ok(opened) => opened,
            Err(err) if err.is_nfs_error(NfsStatus::NOENT, OpCode::Lookup) => {
                session.create_parent_dirs(parent).await?;
                open_temp_file(session, parent).await?
            }
            Err(err) => return Err(err),
        };

        let write_result = write_bytes(session, &opened, data).await;

        let outcome = match write_result {
            Ok(outcome) => outcome,
            Err(err) => {
                let close_result = session.close_file(&mut opened).await;
                let _ = session.remove(parent, &temp_name).await;
                return match close_result {
                    Ok(()) => Err(err),
                    Err(close_err) => Err(err.with_close_failure(close_err)),
                };
            }
        };
        let bytes_written = outcome.bytes_written;

        match session
            .close_publish(
                &mut opened,
                parent,
                &temp_name,
                name,
                bytes_written,
                outcome.unstable_verifier,
                mode == PutMode::IfNotExists,
            )
            .await
        {
            Ok(()) => Ok(()),
            Err(err) => {
                let _ = session.remove(parent, &temp_name).await;
                publish_result(mode, err, raw_path)
            }
        }
    }

    /// Attempts the single-compound put; `Ok(None)` means the caller should
    /// use the multi-compound path instead (body over negotiated limits,
    /// missing parent directories, transient statuses, or a server that
    /// lacks part of the fused compound shape).
    async fn put_once_fused(
        &self,
        snapshot: &ClientSession,
        parent: &[&str],
        name: &str,
        raw_path: &str,
        data: &Bytes,
        mode: PutMode,
    ) -> Result<Option<()>, Error> {
        let session = &snapshot.session;
        if !session.fused_put_viable(data.len()) {
            return Ok(None);
        }
        for _ in 0..TEMP_FILE_CREATE_RETRIES {
            let temp_name = temp_file_name();
            match session
                .try_put_fused(
                    parent,
                    &temp_name,
                    name,
                    data.clone(),
                    mode == PutMode::IfNotExists,
                )
                .await
            {
                Ok(Some(OpenOutcome::Opened((_, opened)))) => {
                    if let Err(mut opened) = session.defer_close(opened).await {
                        tracing::warn!(
                            path = raw_path,
                            "deferred CLOSE worker unavailable; closing published file inline"
                        );
                        if let Err(close_error) = session.close_file(&mut opened).await {
                            tracing::warn!(
                                path = raw_path,
                                error = %close_error,
                                "published NFS file is durable, but its open state could not be closed"
                            );
                        }
                    }
                    return Ok(Some(()));
                }
                Ok(Some(OpenOutcome::Exists)) => continue,
                Ok(None) => return Ok(None),
                Err(err) => {
                    let _ = session.remove(parent, &temp_name).await;
                    return match classify_publish_error(mode, &err) {
                        PublishError::Published => Ok(Some(())),
                        PublishError::AlreadyExists => Err(Error::already_exists(raw_path)),
                        PublishError::NoPublish if err.is_retryable() => Err(err),
                        PublishError::NoPublish => Ok(None),
                        PublishError::OutcomeUnknown => Err(outcome_unknown_error(
                            put_operation_name(mode),
                            raw_path,
                            &err,
                        )),
                    };
                }
            }
        }
        Ok(None)
    }

    /// Reads a complete file into a single [`Bytes`] value.
    pub async fn get(&self, path: impl AsRef<str> + Send) -> Result<Bytes, Error> {
        let path = NfsPath::file(path.as_ref())?;
        self.retrying_session_operation(|snapshot| {
            let path = &path;
            async move { self.get_once(&snapshot.session, path).await }
        })
        .await
    }

    async fn get_once(&self, session: &NfsSession, path: &NfsPath<'_>) -> Result<Bytes, Error> {
        let limit = self.inner.max_buffered_read_size;
        let count = small_get_probe_count(session, limit);
        let (fh, size, read) = session
            .read_path_anonymous_with_size(path.components(), count)
            .await?;
        validate_read_reply(&read, count)?;
        ensure_known_size_within_limit(size, limit)?;

        let read_len = read.data.len() as u64;
        if read_len == size {
            validate_eof_expectation(ExpectedEof::ResolvedWithFile(size), read_len, read.eof)?;
            return Ok(read.data);
        }
        if read_len > size {
            return Err(size_expectation_error(
                ExpectedEof::ResolvedWithFile(size),
                read.eof.then_some(read_len),
            ));
        }
        if read.eof {
            return Err(size_expectation_error(
                ExpectedEof::ResolvedWithFile(size),
                Some(read_len),
            ));
        }
        let remaining = size - read_len;
        read_handle_to_bytes_after_prefix(
            session,
            &fh,
            read.data,
            read_len,
            remaining,
            ReadRequirements::resolved_exact(size, limit),
        )
        .await
    }

    /// Reads a complete file using a caller-supplied size.
    ///
    /// This avoids a size lookup when the caller already has a fresh size, such
    /// as one obtained from nearby directory metadata. The read is bounded to
    /// `size` bytes and requires the file to end exactly there. A stale size
    /// returns [`Error::FileSizeMismatch`] rather than truncated data.
    ///
    /// This is intended for immutable records: once a path is written with
    /// [`NfsClient::put`], its contents and size are not modified. Use
    /// [`NfsClient::get`] when the current size must be resolved as part of
    /// the read operation.
    ///
    /// A size of zero still sends a one-byte `READ` probe, validating that the
    /// path exists, is readable, names a regular file, and is actually empty.
    pub async fn get_known_size(
        &self,
        path: impl AsRef<str> + Send,
        size: u64,
    ) -> Result<Bytes, Error> {
        let path = NfsPath::file(path.as_ref())?;
        self.retrying_session_operation(|snapshot| {
            let path = &path;
            async move {
                self.get_known_size_once(&snapshot.session, path, size)
                    .await
            }
        })
        .await
    }

    async fn get_known_size_once(
        &self,
        session: &NfsSession,
        path: &NfsPath<'_>,
        size: u64,
    ) -> Result<Bytes, Error> {
        let limit = self.inner.max_buffered_read_size;
        ensure_known_size_within_limit(size, limit)?;
        if size == 0 {
            let read = session.read_path_anonymous(path.components(), 0, 1).await?;
            validate_read_reply(&read, 1)?;
            validate_expected_eof(0, read.data.len() as u64, read.eof)?;
            return Ok(Bytes::new());
        }

        read_range_anonymous(
            session,
            path.components(),
            0,
            size,
            ReadRequirements::exact(size, limit),
        )
        .await
    }

    /// Reads file type and size metadata for one path.
    ///
    /// Use this when a lean directory listing is not enough and the caller
    /// needs to distinguish files from directories or obtain a known size for
    /// [`NfsClient::get_known_size`].
    pub async fn entry_info(&self, path: impl AsRef<str> + Send) -> Result<EntryInfo, Error> {
        let path = NfsPath::file(path.as_ref())?;
        self.retrying_session_operation(|snapshot| {
            let path = &path;
            async move { self.entry_info_once(&snapshot.session, path).await }
        })
        .await
    }

    async fn entry_info_once(
        &self,
        session: &NfsSession,
        path: &NfsPath<'_>,
    ) -> Result<EntryInfo, Error> {
        let (_fh, attrs) = session
            .lookup_file_handle_attrs(path.components(), AttrRequest::TypeAndSize)
            .await?;
        Ok(EntryInfo {
            kind: EntryKind::from_file_type(attrs.file_type),
            size: attrs
                .size
                .expect("TypeAndSize responses are validated before returning"),
        })
    }

    /// Reads a byte range from a file into a single [`Bytes`] value.
    pub async fn get_range(
        &self,
        path: impl AsRef<str> + Send,
        range: Range<u64>,
    ) -> Result<Bytes, Error> {
        let path = NfsPath::file(path.as_ref())?;
        if range.end <= range.start {
            return Ok(Bytes::new());
        }

        self.retrying_session_operation(|snapshot| {
            let path = &path;
            let range = &range;
            async move { self.get_range_once(&snapshot.session, path, range).await }
        })
        .await
    }

    async fn get_range_once(
        &self,
        session: &NfsSession,
        path: &NfsPath<'_>,
        range: &Range<u64>,
    ) -> Result<Bytes, Error> {
        let limit = self.inner.max_buffered_read_size;
        let requested_len = range.end - range.start;
        if !range_read_needs_size(requested_len, limit) {
            return read_range_anonymous(
                session,
                path.components(),
                range.start,
                requested_len,
                ReadRequirements::new(Some(requested_len), limit),
            )
            .await;
        }

        let (fh, attrs) = session
            .lookup_file_handle_attrs(path.components(), AttrRequest::Size)
            .await?;
        let expected_len = buffered_range_len(range, attrs.size);
        ensure_known_size_within_limit(expected_len, limit)?;
        let remaining = expected_len.min(requested_len);
        if remaining == 0 {
            return Ok(Bytes::new());
        }
        read_handle_to_bytes(
            session,
            &fh,
            range.start,
            remaining,
            ReadRequirements::new(Some(expected_len), limit),
        )
        .await
    }

    /// Removes a file or empty directory.
    ///
    /// Missing paths are treated as already deleted.
    /// If the remove reply is lost, returns [`Error::OutcomeUnknown`] rather
    /// than retrying and risking deletion of a newly recreated path.
    pub async fn delete(&self, path: impl AsRef<str> + Send) -> Result<(), Error> {
        let raw_path = path.as_ref();
        let path = NfsPath::file(raw_path)?;
        self.retrying_session_operation(|snapshot| {
            let path = &path;
            async move { self.delete_once(&snapshot.session, path, raw_path).await }
        })
        .await
    }

    async fn delete_once(
        &self,
        session: &NfsSession,
        path: &NfsPath<'_>,
        raw_path: &str,
    ) -> Result<(), Error> {
        let (parent, name) = path.parent_and_name()?;
        delete_result(session.remove(parent, name).await, raw_path)
    }

    /// Removes stale temporary files left in a directory by dead clients.
    ///
    /// [`NfsClient::put`] publishes through uniquely named temporary files in
    /// the destination directory; a client that dies between creating one
    /// and publishing it leaves the temporary file behind. This lists the
    /// direct children of `directory` and removes the crate-named temporary
    /// files created at least `older_than` ago, judged from the creation
    /// time embedded in each temporary file name. Returns the number of
    /// files removed. A missing directory counts as already swept.
    /// The `.nfs-crust-tmp-` namespace is reserved; entries created through
    /// another client with that prefix can be mistaken for abandoned files.
    ///
    /// Choose `older_than` comfortably larger than the longest plausible
    /// `put` duration. Sweeping a temporary file that a slow writer is still
    /// using makes that writer's publish fail with a not-found error; it
    /// does not corrupt published data.
    ///
    /// The returned count includes only files this call actually removed;
    /// concurrent sweepers that observe the same name do not both count it.
    /// Once removal begins, any failed or lost reply returns
    /// [`Error::OutcomeUnknown`] because some candidates may already be gone.
    pub async fn sweep_temp_files(
        &self,
        directory: impl AsRef<str> + Send,
        older_than: Duration,
    ) -> Result<u64, Error> {
        let raw_directory = directory.as_ref();
        let path = NfsPath::directory(raw_directory)?;
        let older_than_ms = u64::try_from(older_than.as_millis()).unwrap_or(u64::MAX);
        let cutoff_ms = unix_now_millis().saturating_sub(older_than_ms);
        let result = self
            .retrying_session_operation(|snapshot| {
                let path = &path;
                async move {
                    self.sweep_temp_files_once(&snapshot.session, path, raw_directory, cutoff_ms)
                        .await
                }
            })
            .await;
        match result {
            Err(err) if err.is_not_found() => Ok(0),
            other => other,
        }
    }

    async fn sweep_temp_files_once(
        &self,
        session: &NfsSession,
        path: &NfsPath<'_>,
        raw_directory: &str,
        cutoff_ms: u64,
    ) -> Result<u64, Error> {
        let components = path.components();
        let mut removed = 0u64;
        'restart_after_removal: loop {
            let mut cookie = 0;
            let mut verifier = [0; 8];
            let (dir_fh, mut page) = session
                .read_dir_path_page(components, cookie, verifier, None)
                .await?;

            loop {
                let stale: Vec<_> = std::mem::take(&mut page.entries)
                    .into_iter()
                    .filter_map(|entry| {
                        parse_temp_file_created_ms(&entry.name)
                            .is_some_and(|created| created <= cutoff_ms)
                            .then_some(entry.name)
                    })
                    .collect();
                if !stale.is_empty() {
                    let error = remove_temp_names(session, &dir_fh, &stale, &mut removed).await;
                    if error.is_some() {
                        return sweep_result(removed, error, raw_directory);
                    }
                    // Removing entries invalidates directory cookies on some
                    // servers. Restart from a freshly resolved directory
                    // instead of retaining names from later pages in memory.
                    continue 'restart_after_removal;
                }
                if page.eof {
                    return Ok(removed);
                }
                advance_dir_cursor(&mut cookie, &mut verifier, &page)?;
                page = session
                    .read_dir_page(&dir_fh, cookie, verifier, None)
                    .await?;
            }
        }
    }

    /// Lists at most one bounded server page of direct children.
    ///
    /// Each successful attempt fetches one `READDIR` page and never follows
    /// its continuation internally. Pass the returned
    /// [`ListResult::next_token`] to the next call to continue listing.
    pub async fn list_page(
        &self,
        directory: impl AsRef<str> + Send,
        max_entries: usize,
        continuation_token: Option<ContinuationToken>,
    ) -> Result<ListResult, Error> {
        if max_entries == 0 {
            return Err(Error::invalid_config(
                "list max_entries must be greater than zero",
            ));
        }

        let path = NfsPath::directory(directory.as_ref())?;
        self.retrying_session_operation(|snapshot| {
            let path = &path;
            let continuation_token = continuation_token.as_ref();
            async move {
                self.list_page_once(&snapshot.session, path, max_entries, continuation_token)
                    .await
            }
        })
        .await
    }

    async fn list_page_once(
        &self,
        session: &NfsSession,
        path: &NfsPath<'_>,
        max_entries: usize,
        continuation_token: Option<&ContinuationToken>,
    ) -> Result<ListResult, Error> {
        let base_components = path.components();
        let base_path = base_components.join("/");
        let entry_path_prefix = if base_path.is_empty() {
            String::new()
        } else {
            format!("{base_path}/")
        };
        let (cookie, verifier) = match continuation_token {
            Some(token) => token.list_position(&base_path)?,
            None => (0, [0; 8]),
        };
        let page = session
            .read_dir_path_page_without_handle(base_components, cookie, verifier, Some(max_entries))
            .await?;
        let next_token = if page.eof {
            None
        } else {
            if page.last_cookie == cookie {
                return Err(Error::protocol(
                    "READDIR did not advance the cookie and was not EOF",
                ));
            }
            Some(ContinuationToken::from_list_position(
                &base_path,
                page.last_cookie,
                page.verifier,
            ))
        };
        let entries = page
            .entries
            .into_iter()
            .map(|entry| ListEntry {
                path: format!("{entry_path_prefix}{}", entry.name),
                name: entry.name,
            })
            .collect();
        Ok(ListResult {
            entries,
            next_token,
        })
    }

    async fn retrying_session_operation<T, F, Fut>(&self, mut operation: F) -> Result<T, Error>
    where
        F: FnMut(ClientSession) -> Fut,
        Fut: Future<Output = Result<T, Error>>,
    {
        let mut attempts = 0;
        loop {
            let snapshot = self.current_session();
            let generation = snapshot.generation;
            match operation(snapshot).await {
                Ok(result) => return Ok(result),
                Err(err) if self.should_reconnect(&err, attempts) => {
                    retry_reconnect_attempts(
                        &mut attempts,
                        RECONNECT_RETRIES,
                        reconnect_failure_backoff(&self.inner.config),
                        |attempt| self.reconnect(generation, attempt, &err),
                    )
                    .await?;
                }
                Err(err) => return Err(err),
            }
        }
    }

    fn current_session(&self) -> ClientSession {
        self.inner.session.read().clone()
    }

    fn should_reconnect(&self, err: &Error, attempts: u32) -> bool {
        err.requires_session_rebuild() && attempts < RECONNECT_RETRIES
    }

    async fn reconnect(
        &self,
        failed_generation: u64,
        attempt: u32,
        cause: &Error,
    ) -> Result<(), Error> {
        let mut coordinator = self.inner.reconnect.lock().await;
        let current_generation = self.inner.session.read().generation;
        if !session_generation_needs_reconnect(current_generation, failed_generation) {
            tracing::debug!(
                failed_generation,
                current_generation,
                attempt,
                error = %cause,
                "skipping NFS session reconnect; another task already reconnected",
            );
            return Ok(());
        }

        let outcome = match &coordinator.state {
            ReconnectState::Connecting {
                generation,
                outcome,
            } if *generation == failed_generation => outcome.clone(),
            ReconnectState::Failed {
                generation,
                retry_after,
                error,
            } if *generation == failed_generation
                && retry_after.is_none_or(|deadline| Instant::now() < deadline) =>
            {
                return Err(error.to_error());
            }
            _ => {
                tracing::warn!(
                    failed_generation,
                    attempt,
                    error = %cause,
                    "reconnecting NFS session after retryable failure",
                );
                let (sender, outcome) = watch::channel(None);
                coordinator.state = ReconnectState::Connecting {
                    generation: failed_generation,
                    outcome: outcome.clone(),
                };
                let inner = Arc::clone(&self.inner);
                let cause: Arc<str> = cause.to_string().into();
                tokio::spawn(async move {
                    Self::run_reconnect(inner, failed_generation, attempt, cause, sender).await;
                });
                outcome
            }
        };
        drop(coordinator);
        await_reconnect_outcome(outcome).await
    }

    async fn run_reconnect(
        inner: Arc<ClientInner>,
        failed_generation: u64,
        attempt: u32,
        cause: Arc<str>,
        outcome_sender: watch::Sender<Option<ReconnectOutcome>>,
    ) {
        let reconnect_result = async {
            resolve_and_connect_session(
                &inner.resolver,
                inner.auth.clone(),
                inner.transport.clone(),
                inner.config.clone(),
                inner.connect_timeout,
                &inner.client_owner,
            )
            .await
        }
        .await;

        let outcome = match reconnect_result {
            Ok(session) => {
                let server_addr = session.server_addr();
                let mut current = inner.session.write();
                if session_generation_needs_reconnect(current.generation, failed_generation) {
                    current.session = session;
                    current.generation = current.generation.wrapping_add(1);
                }
                let new_generation = current.generation;
                drop(current);
                tracing::info!(
                    failed_generation,
                    new_generation,
                    server_addr = %server_addr,
                    "NFS session reconnected",
                );
                Ok(())
            }
            Err(error) => {
                tracing::warn!(
                    failed_generation,
                    attempt,
                    error = %error,
                    original_error = %cause,
                    "NFS session reconnect failed",
                );
                Err(ReconnectFailure::from_error(&error))
            }
        };

        let mut coordinator = inner.reconnect.lock().await;
        finish_reconnect_attempt(
            &mut coordinator,
            failed_generation,
            outcome.clone(),
            reconnect_failure_backoff(&inner.config),
        );
        drop(coordinator);
        let _ = outcome_sender.send(Some(outcome));
    }
}

#[derive(Debug)]
/// Builder for configuring and connecting an [`NfsClient`].
pub struct NfsClientBuilder {
    endpoint: String,
    auth: Result<AuthSys, Error>,
    transport: RpcTransport,
    config: NfsConfig,
    connect_timeout: Option<Duration>,
    max_buffered_read_size: Option<u64>,
}

impl NfsClientBuilder {
    fn new(addr: impl Into<String>, export: impl Into<String>) -> Self {
        Self {
            endpoint: addr.into(),
            auth: AuthSys::current_user(),
            transport: RpcTransport::Plaintext,
            config: NfsConfig::new(export),
            connect_timeout: Some(Duration::from_secs(30)),
            max_buffered_read_size: Some(DEFAULT_MAX_BUFFERED_READ_SIZE),
        }
    }

    /// Sets AUTH_SYS credentials for RPC calls.
    pub fn auth_sys(mut self, auth: AuthSys) -> Self {
        self.auth = Ok(auth);
        self
    }

    /// Enables TLS for RPC connections.
    ///
    /// For DNS-validated servers, including encrypted EFS endpoints, the server
    /// name must match the certificate presented by the NFS endpoint.
    pub fn tls(mut self, config: TlsConfig) -> Self {
        self.transport = RpcTransport::Tls(config);
        self
    }

    /// Sets the requested maximum NFS `READ` payload size.
    pub fn read_chunk_size(mut self, bytes: u32) -> Self {
        self.config.read_chunk_size = bytes;
        self
    }

    /// Sets the READ size used for size-probing and pipelined read chunks.
    ///
    /// Reads that cannot be sized in advance — the first READ of a
    /// [`NfsClient::get`], and each chunk of a multi-chunk read pipeline —
    /// request this many bytes. Some servers (AWS EFS) price a READ by its
    /// requested count rather than the bytes returned, so a small granularity
    /// keeps small-object reads cheap, while larger bodies are fetched as
    /// several granules pipelined across session slots. Reads whose exact
    /// remaining size is already known and fits one granule are still issued
    /// at that exact size. Values above `read_chunk_size` are clamped to it;
    /// raise it to `read_chunk_size` to read in single maximal chunks.
    pub fn read_granularity(mut self, bytes: u32) -> Self {
        self.config.read_granularity = bytes;
        self
    }

    /// Sets the requested maximum NFS `WRITE` payload size.
    ///
    /// TLS connections using an AWS EFS certificate name cap the effective
    /// value at the largest payload verified stable on EFS. Larger bodies are
    /// split and sent in offset order; other NFS servers retain this requested
    /// maximum and their normal same-file write pipelining.
    pub fn write_chunk_size(mut self, bytes: u32) -> Self {
        self.config.write_chunk_size = bytes;
        self
    }

    /// Sets the POSIX mode used when creating files.
    pub fn file_mode(mut self, mode: u32) -> Self {
        self.config.file_mode = mode;
        self
    }

    /// Sets the POSIX mode used when creating parent directories.
    pub fn dir_mode(mut self, mode: u32) -> Self {
        self.config.dir_mode = mode;
        self
    }

    /// Sets the requested number of concurrent NFSv4.1 session slots.
    ///
    /// Values above 1024 are rejected to bound client-side queues allocated
    /// before the server negotiates its own, usually much smaller, limit.
    pub fn session_slots(mut self, slots: u32) -> Self {
        self.config.session_slots = slots;
        self
    }

    /// Sets one total timeout budget for address resolution and session
    /// establishment across all resolved server addresses.
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = Some(timeout);
        self
    }

    /// Sets or disables the timeout for individual RPC-backed operations.
    pub fn operation_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.config.operation_timeout = timeout;
        self
    }

    /// Sets or disables the maximum bytes that buffered reads may materialize.
    pub fn max_buffered_read_size(mut self, bytes: Option<u64>) -> Self {
        self.max_buffered_read_size = bytes;
        self
    }

    /// Connects to the server and establishes an NFSv4.1 session.
    pub async fn connect(mut self) -> Result<NfsClient, Error> {
        validate_config(&self.config)?;
        validate_buffered_read_limit(self.max_buffered_read_size)?;
        apply_transport_write_cap(&mut self.config, &self.transport);
        let auth = self.auth?;
        let client_owner = ClientOwner::new();
        let resolver = AddressResolver::new(self.endpoint);
        let session = resolve_and_connect_session(
            &resolver,
            auth.clone(),
            self.transport.clone(),
            self.config.clone(),
            self.connect_timeout,
            &client_owner,
        )
        .await?;
        Ok(NfsClient {
            inner: Arc::new(ClientInner {
                resolver,
                client_owner,
                auth,
                transport: self.transport,
                config: self.config,
                connect_timeout: self.connect_timeout,
                max_buffered_read_size: self.max_buffered_read_size,
                session: RwLock::new(ClientSession {
                    session,
                    generation: 0,
                }),
                reconnect: Mutex::new(ReconnectCoordinator::default()),
            }),
        })
    }
}

fn apply_transport_write_cap(config: &mut NfsConfig, transport: &RpcTransport) {
    let RpcTransport::Tls(tls) = transport else {
        return;
    };
    if is_aws_efs_server_name(tls.server_name()) {
        config.write_chunk_size = config.write_chunk_size.min(AWS_EFS_MAX_WRITE_CHUNK_SIZE);
        config.pipeline_write_chunks = false;
    }
}

fn is_aws_efs_server_name(server_name: &str) -> bool {
    let Some((file_system, rest)) = server_name.split_once('.') else {
        return false;
    };
    let Some((service, domain)) = rest.split_once('.') else {
        return false;
    };
    let region = domain
        .strip_suffix(".amazonaws.com")
        .or_else(|| domain.strip_suffix(".amazonaws.com.cn"));

    file_system
        .strip_prefix("fs-")
        .is_some_and(|id| !id.is_empty())
        && matches!(service, "efs" | "efs-fips")
        && region.is_some_and(|region| !region.is_empty() && !region.contains('.'))
}

async fn resolve_addrs<A: ToSocketAddrs>(addr: A) -> Result<Vec<SocketAddr>, Error> {
    let addrs: Vec<_> = lookup_host(addr).await?.collect();
    if addrs.is_empty() {
        return Err(Error::protocol(
            "NFS endpoint did not resolve to any address",
        ));
    }
    Ok(addrs)
}

fn validate_config(config: &NfsConfig) -> Result<(), Error> {
    validate_nonzero(config.read_chunk_size, "read_chunk_size")?;
    validate_nonzero(config.read_granularity, "read_granularity")?;
    validate_nonzero(config.write_chunk_size, "write_chunk_size")?;
    validate_nonzero(config.readdir_dircount, "readdir dircount")?;
    if config.readdir_maxcount < config.readdir_dircount {
        return Err(Error::invalid_config(
            "readdir maxcount must be greater than or equal to dircount",
        ));
    }
    validate_nonzero(config.session_slots, "session_slots")?;
    if config.session_slots > MAX_CONFIGURED_SESSION_SLOTS {
        return Err(Error::invalid_config(format!(
            "session_slots must not exceed {MAX_CONFIGURED_SESSION_SLOTS}"
        )));
    }
    validate_min_bytes(config.max_request_size, "max_request_size", 4096)?;
    validate_min_bytes(config.max_response_size, "max_response_size", 4096)?;
    Ok(())
}

fn validate_buffered_read_limit(limit: Option<u64>) -> Result<(), Error> {
    if limit == Some(0) {
        return Err(Error::invalid_config(
            "max_buffered_read_size must be greater than zero",
        ));
    }
    Ok(())
}

fn validate_nonzero(value: u32, name: &str) -> Result<(), Error> {
    if value == 0 {
        return Err(Error::invalid_config(format!(
            "{name} must be greater than zero"
        )));
    }
    Ok(())
}

fn validate_min_bytes(value: u32, name: &str, minimum: u32) -> Result<(), Error> {
    if value < minimum {
        return Err(Error::invalid_config(format!(
            "{name} must be at least {minimum} bytes"
        )));
    }
    Ok(())
}

async fn connect_session(
    addrs: &[SocketAddr],
    auth: AuthSys,
    transport: RpcTransport,
    config: NfsConfig,
    client_owner: &ClientOwner,
) -> Result<NfsSession, Error> {
    let mut last_error = None;
    for addr in addrs {
        match NfsSession::connect(
            *addr,
            auth.clone(),
            config.clone(),
            transport.clone(),
            client_owner,
        )
        .await
        {
            Ok(session) => return Ok(session),
            Err(err) => last_error = Some(err),
        }
    }

    Err(last_error
        .unwrap_or_else(|| Error::protocol("NFS endpoint did not resolve to any address")))
}

async fn resolve_and_connect_session(
    resolver: &AddressResolver,
    auth: AuthSys,
    transport: RpcTransport,
    config: NfsConfig,
    timeout: Option<Duration>,
    client_owner: &ClientOwner,
) -> Result<NfsSession, Error> {
    let connect = async {
        let addrs = resolver.resolve().await?;
        connect_session(&addrs, auth, transport, config, client_owner).await
    };
    with_optional_timeout(timeout, connect).await
}

async fn with_optional_timeout<T>(
    timeout: Option<Duration>,
    operation: impl Future<Output = Result<T, Error>>,
) -> Result<T, Error> {
    if let Some(timeout) = timeout {
        tokio::time::timeout(timeout, operation)
            .await
            .map_err(|_| Error::Timeout(timeout))?
    } else {
        operation.await
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Publish behavior for [`NfsClient::put`] when the destination exists.
pub enum PutMode {
    /// Create or atomically replace the destination.
    Overwrite,
    /// Create the destination only if it does not already exist.
    IfNotExists,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
/// Opaque continuation token returned by paged directory requests.
pub struct ContinuationToken {
    directory: Box<str>,
    cookie: u64,
    verifier: [u8; 8],
}

impl ContinuationToken {
    fn from_list_position(directory: &str, cookie: u64, verifier: [u8; 8]) -> Self {
        Self {
            directory: directory.into(),
            cookie,
            verifier,
        }
    }

    fn list_position(&self, directory: &str) -> Result<(u64, [u8; 8]), Error> {
        if self.directory.as_ref() != directory {
            return Err(invalid_continuation_token(
                "list continuation token does not match the requested directory",
            ));
        }
        Ok((self.cookie, self.verifier))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Directory entry returned by [`NfsClient::list_page`].
pub struct ListEntry {
    /// Root-relative path of the entry inside the export.
    pub path: String,
    /// Final path component of the entry.
    pub name: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// File type for a directory entry.
pub enum EntryKind {
    /// Regular file.
    File,
    /// Directory.
    Directory,
    /// Other file type or unknown type.
    Other,
}

impl EntryKind {
    fn from_file_type(file_type: Option<FileType>) -> Self {
        match file_type {
            Some(FileType::Regular) => Self::File,
            Some(FileType::Directory) => Self::Directory,
            _ => Self::Other,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Metadata returned by [`NfsClient::entry_info`].
pub struct EntryInfo {
    /// File type reported by the server.
    pub kind: EntryKind,
    /// File size in bytes.
    pub size: u64,
}

impl EntryInfo {
    /// Returns true when the entry is a regular file.
    pub fn is_file(&self) -> bool {
        self.kind == EntryKind::File
    }

    /// Returns true when the entry is a directory.
    pub fn is_dir(&self) -> bool {
        self.kind == EntryKind::Directory
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// One page of directory entries.
pub struct ListResult {
    /// Entries returned by this page.
    pub entries: Vec<ListEntry>,
    /// Token for the next page, if more entries remain.
    pub next_token: Option<ContinuationToken>,
}

#[derive(Debug, Clone, Copy)]
struct ReadRequirements {
    expected_len: Option<u64>,
    expected_eof: Option<ExpectedEof>,
    limit: Option<u64>,
}

#[derive(Debug, Clone, Copy)]
enum ExpectedEof {
    CallerSupplied(u64),
    ResolvedWithFile(u64),
}

impl ReadRequirements {
    fn new(expected_len: Option<u64>, limit: Option<u64>) -> Self {
        Self {
            expected_len,
            expected_eof: None,
            limit,
        }
    }

    fn exact(size: u64, limit: Option<u64>) -> Self {
        Self {
            expected_len: Some(size),
            expected_eof: Some(ExpectedEof::CallerSupplied(size)),
            limit,
        }
    }

    fn resolved_exact(size: u64, limit: Option<u64>) -> Self {
        Self {
            expected_len: Some(size),
            expected_eof: Some(ExpectedEof::ResolvedWithFile(size)),
            limit,
        }
    }
}

fn operation_pipeline_depth(session: &NfsSession) -> usize {
    operation_pipeline_depth_for_slots(session.session_slot_count())
}

fn operation_pipeline_depth_for_slots(slot_count: usize) -> usize {
    slot_count.clamp(1, MAX_OPERATION_PIPELINE_DEPTH)
}

async fn read_range_anonymous<S>(
    session: &NfsSession,
    components: &[S],
    start: u64,
    requested_len: u64,
    requirements: ReadRequirements,
) -> Result<Bytes, Error>
where
    S: AsRef<str>,
{
    if requested_len <= u64::from(session.read_granularity()) {
        let count = requested_len as u32;
        let read = session
            .read_path_anonymous(components, start, count)
            .await?;
        validate_read_reply(&read, count)?;
        ensure_buffered_read_within_limit(
            0,
            read.data.len(),
            requirements.expected_len,
            requirements.limit,
        )?;
        if !read_needs_retry(&read, count) {
            validate_optional_expected_eof(
                requirements.expected_eof,
                start + read.data.len() as u64,
                read.eof,
            )?;
            return Ok(read.data);
        }

        let fh = session.lookup_file_handle(components).await?;
        return read_handle_to_bytes(session, &fh, start, requested_len, requirements).await;
    }

    let count = session.read_granularity();
    let (fh, read) = session
        .read_path_anonymous_with_handle(components, start, count)
        .await?;
    validate_read_reply(&read, count)?;
    ensure_buffered_read_within_limit(
        0,
        read.data.len(),
        requirements.expected_len,
        requirements.limit,
    )?;
    if read.eof {
        validate_optional_expected_eof(
            requirements.expected_eof,
            start + read.data.len() as u64,
            true,
        )?;
        return Ok(read.data);
    }

    let read_len = read.data.len() as u64;
    read_handle_to_bytes_after_prefix(
        session,
        &fh,
        read.data,
        start + read_len,
        requested_len - read_len,
        requirements,
    )
    .await
}

/// The first READ of an unknown-size `get` is issued before the size is known,
/// so its count is what the server bills for: probe at the read granularity,
/// not the full chunk size. Servers whose READ latency scales with the
/// requested count (AWS EFS) make a full-chunk probe expensive for small
/// files, and bodies larger than one granule recover the difference through
/// the pipelined continuation.
fn small_get_probe_count(session: &NfsSession, limit: Option<u64>) -> u32 {
    limit
        .map(|limit| limit.min(u64::from(session.read_granularity())) as u32)
        .unwrap_or_else(|| session.read_granularity())
}

fn read_needs_retry(read: &ReadData, count: u32) -> bool {
    read.data.len() < count as usize && !read.eof
}

fn validate_read_reply(read: &ReadData, count: u32) -> Result<(), Error> {
    if read.data.len() > count as usize {
        return Err(Error::protocol(format!(
            "NFS READ returned {} bytes for a {} byte request",
            read.data.len(),
            count
        )));
    }
    if count > 0 && read.data.is_empty() && !read.eof {
        return Err(Error::protocol(
            "NFS READ returned no data for a nonzero request without EOF",
        ));
    }
    Ok(())
}

fn validate_expected_eof(expected: u64, observed_end: u64, eof: bool) -> Result<(), Error> {
    validate_eof_expectation(ExpectedEof::CallerSupplied(expected), observed_end, eof)
}

fn validate_eof_expectation(
    expectation: ExpectedEof,
    observed_end: u64,
    eof: bool,
) -> Result<(), Error> {
    let expected = expectation.offset();
    if eof {
        if observed_end == expected {
            Ok(())
        } else {
            Err(size_expectation_error(expectation, Some(observed_end)))
        }
    } else if observed_end >= expected {
        Err(size_expectation_error(expectation, None))
    } else {
        Ok(())
    }
}

impl ExpectedEof {
    fn offset(self) -> u64 {
        match self {
            Self::CallerSupplied(offset) | Self::ResolvedWithFile(offset) => offset,
        }
    }
}

fn size_expectation_error(expectation: ExpectedEof, actual: Option<u64>) -> Error {
    let expected = expectation.offset();
    match expectation {
        ExpectedEof::CallerSupplied(_) => Error::file_size_mismatch(expected, actual),
        ExpectedEof::ResolvedWithFile(_) => match actual {
            Some(actual) => Error::protocol(format!(
                "READ reached EOF at byte {actual}, but the size resolved with the file was {expected}"
            )),
            None => Error::protocol(format!(
                "READ reached resolved size {expected} without the required EOF indication"
            )),
        },
    }
}

fn validate_optional_expected_eof(
    expected: Option<ExpectedEof>,
    observed_end: u64,
    eof: bool,
) -> Result<(), Error> {
    match expected {
        Some(expected) => validate_eof_expectation(expected, observed_end, eof),
        None => Ok(()),
    }
}

fn validate_optional_observed_eof(
    expected: Option<ExpectedEof>,
    observed_eof: Option<u64>,
) -> Result<(), Error> {
    match (expected, observed_eof) {
        (Some(expected), Some(actual)) if expected.offset() != actual => {
            Err(size_expectation_error(expected, Some(actual)))
        }
        (Some(_), Some(_)) | (None, _) => Ok(()),
        (Some(expected), None) => Err(size_expectation_error(expected, None)),
    }
}

fn buffered_read_capacity(
    expected_len: Option<u64>,
    requested_len: u64,
    limit: Option<u64>,
) -> usize {
    let size = expected_len.unwrap_or(requested_len);
    usize::try_from(size.min(limit.unwrap_or(DEFAULT_MAX_BUFFERED_READ_SIZE))).unwrap_or_default()
}

fn ensure_known_size_within_limit(size: u64, limit: Option<u64>) -> Result<(), Error> {
    if let Some(limit) = limit
        && size > limit
    {
        return Err(Error::BufferedReadTooLarge {
            size: Some(size),
            limit,
        });
    }
    Ok(())
}

fn ensure_buffered_read_within_limit(
    current_len: usize,
    additional_len: usize,
    expected_len: Option<u64>,
    limit: Option<u64>,
) -> Result<(), Error> {
    let Some(limit) = limit else {
        return Ok(());
    };
    if (current_len as u64).saturating_add(additional_len as u64) > limit {
        return Err(Error::BufferedReadTooLarge {
            size: expected_len,
            limit,
        });
    }
    Ok(())
}

async fn read_handle_to_bytes_after_prefix(
    session: &NfsSession,
    fh: &FileHandle,
    prefix: Bytes,
    resume_offset: u64,
    remaining: u64,
    requirements: ReadRequirements,
) -> Result<Bytes, Error> {
    debug_assert!(remaining > 0);
    let total_len = prefix.len() as u64 + remaining;
    let mut out = BytesMut::with_capacity(buffered_read_capacity(
        requirements.expected_len,
        total_len,
        requirements.limit,
    ));
    out.extend_from_slice(&prefix);

    if remaining <= u64::from(session.read_granularity()) || operation_pipeline_depth(session) <= 1
    {
        read_handle_to_bytes_sequential_into(
            session,
            fh,
            &mut out,
            resume_offset,
            remaining,
            requirements,
        )
        .await?;
    } else {
        read_handle_to_bytes_pipelined_into(
            session,
            fh,
            &mut out,
            resume_offset,
            remaining,
            requirements,
        )
        .await?;
    }

    Ok(out.freeze())
}

async fn read_handle_to_bytes(
    session: &NfsSession,
    fh: &FileHandle,
    offset: u64,
    remaining: u64,
    requirements: ReadRequirements,
) -> Result<Bytes, Error> {
    if remaining == 0 {
        return Ok(Bytes::new());
    }
    if remaining <= u64::from(session.read_granularity()) {
        return read_handle_single_chunk_or_continue(
            session,
            fh,
            offset,
            remaining as u32,
            requirements,
        )
        .await;
    }
    let mut out = BytesMut::with_capacity(buffered_read_capacity(
        requirements.expected_len,
        remaining,
        requirements.limit,
    ));
    if operation_pipeline_depth(session) <= 1 {
        read_handle_to_bytes_sequential_into(
            session,
            fh,
            &mut out,
            offset,
            remaining,
            requirements,
        )
        .await?;
    } else {
        read_handle_to_bytes_pipelined_into(session, fh, &mut out, offset, remaining, requirements)
            .await?;
    }
    Ok(out.freeze())
}

async fn read_handle_single_chunk_or_continue(
    session: &NfsSession,
    fh: &FileHandle,
    offset: u64,
    count: u32,
    requirements: ReadRequirements,
) -> Result<Bytes, Error> {
    let read = session.read_anonymous(fh, offset, count).await?;
    validate_read_reply(&read, count)?;
    ensure_buffered_read_within_limit(
        0,
        read.data.len(),
        requirements.expected_len,
        requirements.limit,
    )?;
    if !read_needs_retry(&read, count) {
        validate_optional_expected_eof(
            requirements.expected_eof,
            offset + read.data.len() as u64,
            read.eof,
        )?;
        return Ok(read.data);
    }

    let read_len = read.data.len();
    let mut out = BytesMut::with_capacity(buffered_read_capacity(
        requirements.expected_len,
        u64::from(count),
        requirements.limit,
    ));
    out.extend_from_slice(&read.data);
    let remaining = u64::from(count).saturating_sub(read_len as u64);
    read_handle_to_bytes_sequential_into(
        session,
        fh,
        &mut out,
        offset + read_len as u64,
        remaining,
        requirements,
    )
    .await?;
    Ok(out.freeze())
}

async fn read_handle_to_bytes_sequential_into(
    session: &NfsSession,
    fh: &FileHandle,
    out: &mut BytesMut,
    mut offset: u64,
    mut remaining: u64,
    requirements: ReadRequirements,
) -> Result<(), Error> {
    let chunk_size = u64::from(session.read_chunk_size());
    let mut observed_eof = None;

    while remaining > 0 {
        let count = remaining.min(chunk_size) as u32;
        let read = session.read_anonymous(fh, offset, count).await?;
        validate_read_reply(&read, count)?;
        if read.eof {
            observed_eof = Some(offset + read.data.len() as u64);
        }
        if read.data.is_empty() {
            break;
        }
        ensure_buffered_read_within_limit(
            out.len(),
            read.data.len(),
            requirements.expected_len,
            requirements.limit,
        )?;

        offset += read.data.len() as u64;
        remaining = remaining.saturating_sub(read.data.len() as u64);
        out.extend_from_slice(&read.data);
        if read.eof {
            break;
        }
    }

    validate_optional_observed_eof(requirements.expected_eof, observed_eof)?;
    Ok(())
}

async fn read_handle_to_bytes_pipelined_into(
    session: &NfsSession,
    fh: &FileHandle,
    out: &mut BytesMut,
    start: u64,
    total_len: u64,
    requirements: ReadRequirements,
) -> Result<(), Error> {
    let depth = operation_pipeline_depth(session);
    let chunk_size = u64::from(session.read_granularity());
    let mut scheduler = ChunkScheduler::new(start, start.saturating_add(total_len), chunk_size);
    let mut emit = PipelinedEmit::new(
        out,
        start,
        requirements.expected_len,
        requirements.expected_eof,
        requirements.limit,
    );
    let mut reads = FuturesUnordered::new();

    loop {
        while emit.can_schedule_more() && reads.len() < depth {
            let Some((offset, count)) = scheduler.next_range(emit.eof_observed()) else {
                break;
            };
            reads.push(read_chunk(session, fh, offset, count));
        }

        let Some(result) = reads.next().await else {
            break;
        };
        match result {
            Ok(read) => {
                if let Some((gap_offset, gap_len)) = read.short_read_gap() {
                    scheduler.push_gap(gap_offset, gap_len);
                }
                emit.record(read);
            }
            Err(err) => emit.record_error(err),
        }
    }

    emit.finish()
}

#[derive(Debug)]
struct ReadTaskOutput {
    offset: u64,
    requested: u32,
    data: Bytes,
    eof: bool,
}

type ReadCompletions = SmallVec<[ReadTaskOutput; MAX_OPERATION_PIPELINE_DEPTH]>;
type ReadRanges = SmallVec<[(u64, u64); MAX_OPERATION_PIPELINE_DEPTH]>;

impl ReadTaskOutput {
    /// A non-EOF short read leaves a gap that must be requested again.
    fn short_read_gap(&self) -> Option<(u64, u64)> {
        let read_len = self.data.len() as u64;
        let gap_len = u64::from(self.requested).saturating_sub(read_len);
        (!self.eof && gap_len > 0).then(|| (self.offset + read_len, gap_len))
    }
}

/// Chunk scheduling for a pipelined read: gaps left by short reads take
/// priority, then fresh ranges advance sequentially to `end`.
struct ChunkScheduler {
    gaps: ReadRanges,
    next_offset: u64,
    end: u64,
    chunk_size: u64,
}

impl ChunkScheduler {
    fn new(start: u64, end: u64, chunk_size: u64) -> Self {
        Self {
            gaps: ReadRanges::new(),
            next_offset: start,
            end,
            chunk_size,
        }
    }

    fn push_gap(&mut self, offset: u64, len: u64) {
        self.gaps.push((offset, len));
    }

    fn next_range(&mut self, eof_observed: bool) -> Option<(u64, u32)> {
        let (offset, len) = if let Some((offset, len)) = self.gaps.pop() {
            (offset, len.min(self.chunk_size))
        } else {
            if eof_observed || self.next_offset >= self.end {
                return None;
            }
            let offset = self.next_offset;
            let len = (self.end - offset).min(self.chunk_size);
            self.next_offset += len;
            (offset, len)
        };
        Some((offset, len as u32))
    }
}

/// Reassembles out-of-order chunk completions into strictly ordered output,
/// tracking the first error and the server-reported end of file.
struct PipelinedEmit<'a> {
    out: &'a mut BytesMut,
    completed: ReadCompletions,
    next_offset: u64,
    /// Exact EOF once proven by a data-bearing EOF read or by an empty EOF at
    /// the next contiguous emit offset.
    eof_at: Option<u64>,
    /// Lowest offset from any EOF response. Empty out-of-order EOF reads only
    /// prove that EOF is at or before their offset.
    eof_upper_bound: Option<u64>,
    first_error: Option<Error>,
    expected_len: Option<u64>,
    expected_eof: Option<ExpectedEof>,
    limit: Option<u64>,
}

impl<'a> PipelinedEmit<'a> {
    fn new(
        out: &'a mut BytesMut,
        start: u64,
        expected_len: Option<u64>,
        expected_eof: Option<ExpectedEof>,
        limit: Option<u64>,
    ) -> Self {
        Self {
            out,
            completed: ReadCompletions::new(),
            next_offset: start,
            eof_at: None,
            eof_upper_bound: None,
            first_error: None,
            expected_len,
            expected_eof,
            limit,
        }
    }

    fn eof_observed(&self) -> bool {
        self.eof_upper_bound.is_some()
    }

    fn can_schedule_more(&self) -> bool {
        self.first_error.is_none()
    }

    /// Emits a completed chunk in offset order; chunks completing ahead of
    /// the emit offset queue until the gap before them closes.
    fn record(&mut self, read: ReadTaskOutput) {
        if read.eof {
            self.observe_read_eof(&read);
        }
        if self.eof_at.is_some_and(|eof_at| {
            !read.data.is_empty() && read.offset.saturating_add(read.data.len() as u64) > eof_at
        }) {
            self.record_error(Error::protocol(
                "pipelined READ returned data beyond a previously reported EOF",
            ));
            return;
        }
        if read.offset == self.next_offset {
            self.emit(read);
            self.drain_queued();
        } else {
            self.completed.push(read);
        }
    }

    fn record_error(&mut self, err: Error) {
        self.first_error.get_or_insert(err);
    }

    fn observe_read_eof(&mut self, read: &ReadTaskOutput) {
        let eof_at = read.offset + read.data.len() as u64;
        if read.data.is_empty() && read.offset != self.next_offset {
            self.observe_eof_upper_bound(eof_at);
        } else {
            self.observe_exact_eof(eof_at);
        }
    }

    fn observe_eof_upper_bound(&mut self, eof_at_or_before: u64) {
        if let Some(exact) = self.eof_at
            && exact > eof_at_or_before
        {
            self.record_error(Error::protocol(format!(
                "pipelined READ reported EOF upper bound {eof_at_or_before} below exact EOF offset {exact}"
            )));
        }
        self.eof_upper_bound = Some(match self.eof_upper_bound {
            Some(current) => current.min(eof_at_or_before),
            None => eof_at_or_before,
        });
    }

    fn observe_exact_eof(&mut self, eof_at: u64) {
        if let Some(upper_bound) = self.eof_upper_bound
            && eof_at > upper_bound
        {
            self.record_error(Error::protocol(format!(
                "pipelined READ reported exact EOF offset {eof_at} beyond earlier EOF upper bound {upper_bound}"
            )));
        }
        match self.eof_at {
            Some(current) if current == eof_at => {}
            Some(current) => self.record_error(Error::protocol(format!(
                "pipelined READ reported conflicting EOF offsets: {current} and {eof_at}"
            ))),
            None => self.eof_at = Some(eof_at),
        }
        self.observe_eof_upper_bound(eof_at);
    }

    fn finish(self) -> Result<(), Error> {
        if let Some(err) = self.first_error {
            return Err(err);
        }
        if self.eof_at.is_some_and(|eof_at| {
            self.completed.iter().any(|read| {
                !read.data.is_empty() && read.offset.saturating_add(read.data.len() as u64) > eof_at
            })
        }) {
            return Err(Error::protocol(
                "pipelined READ returned queued data beyond EOF",
            ));
        }
        match self.eof_at {
            Some(eof_at) if self.next_offset < eof_at => {
                return Err(Error::protocol(
                    "pipelined READ completed with a hole below EOF",
                ));
            }
            None if self
                .eof_upper_bound
                .is_some_and(|upper_bound| self.next_offset < upper_bound) =>
            {
                return Err(Error::protocol(
                    "pipelined READ completed before resolving EOF offset",
                ));
            }
            _ => {}
        }
        validate_optional_observed_eof(self.expected_eof, self.eof_at)
    }

    fn drain_queued(&mut self) {
        while self
            .eof_at
            .is_none_or(|eof_offset| self.next_offset < eof_offset)
        {
            let Some(read) = self.take_completed() else {
                break;
            };
            self.emit(read);
            if self.first_error.is_some()
                || self
                    .eof_at
                    .is_some_and(|eof_offset| self.next_offset >= eof_offset)
            {
                break;
            }
        }
    }

    fn take_completed(&mut self) -> Option<ReadTaskOutput> {
        let index = self
            .completed
            .iter()
            .position(|read| read.offset == self.next_offset)?;
        Some(self.completed.swap_remove(index))
    }

    fn emit(&mut self, read: ReadTaskOutput) {
        let read_len = read.data.len();
        if read_len == 0 {
            self.observe_exact_eof(read.offset);
            return;
        }
        if let Err(err) = ensure_buffered_read_within_limit(
            self.out.len(),
            read_len,
            self.expected_len,
            self.limit,
        ) {
            self.first_error.get_or_insert(err);
            return;
        }
        self.next_offset += read_len as u64;
        self.out.extend_from_slice(&read.data);
        if read.eof {
            self.observe_exact_eof(self.next_offset);
        }
    }
}

async fn read_chunk(
    session: &NfsSession,
    fh: &FileHandle,
    offset: u64,
    count: u32,
) -> Result<ReadTaskOutput, Error> {
    let read = session.read_anonymous(fh, offset, count).await?;
    validate_read_reply(&read, count)?;
    Ok(ReadTaskOutput {
        offset,
        requested: count,
        data: read.data,
        eof: read.eof,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PublishError {
    Published,
    AlreadyExists,
    NoPublish,
    OutcomeUnknown,
}

/// Classifies only errors returned after issuing a publish-capable compound.
/// A concrete NFS error names the operation that stopped the compound, while
/// a transport or decoding failure cannot prove whether LINK ran.
fn classify_publish_error(mode: PutMode, err: &Error) -> PublishError {
    if mode == PutMode::IfNotExists && err.is_nfs_operation(OpCode::Remove) {
        PublishError::Published
    } else if mode == PutMode::IfNotExists && err.is_nfs_error(NfsStatus::EXIST, OpCode::Link) {
        PublishError::AlreadyExists
    } else if error_proves_operation_not_run(err) {
        PublishError::NoPublish
    } else {
        PublishError::OutcomeUnknown
    }
}

fn error_proves_operation_not_run(err: &Error) -> bool {
    err.is_request_not_sent()
        || (err.nfs_status_code().is_some() && !err.is_nfs_operation(OpCode::Illegal))
}

fn delete_result(result: Result<RemoveOutcome, Error>, raw_path: &str) -> Result<(), Error> {
    match result {
        Ok(RemoveOutcome::Removed | RemoveOutcome::Missing) => Ok(()),
        Err(err) if error_proves_operation_not_run(&err) => Err(err),
        Err(err) => Err(outcome_unknown_error("delete", raw_path, &err)),
    }
}

fn tally_remove_result(
    result: Result<RemoveOutcome, Error>,
    removed: &mut u64,
    first_error: &mut Option<Error>,
) {
    match result {
        Ok(RemoveOutcome::Removed) => *removed += 1,
        Ok(RemoveOutcome::Missing) => {}
        Err(err) => {
            first_error.get_or_insert(err);
        }
    }
}

async fn remove_temp_names(
    session: &NfsSession,
    dir_fh: &FileHandle,
    names: &[String],
    removed: &mut u64,
) -> Option<Error> {
    let depth = operation_pipeline_depth(session);
    let mut names = names.iter();
    let mut removes = FuturesUnordered::new();
    let mut first_error = None;
    loop {
        while first_error.is_none() && removes.len() < depth {
            let Some(name) = names.next() else { break };
            removes.push(session.remove_at(dir_fh, name));
        }
        let Some(result) = removes.next().await else {
            break;
        };
        tally_remove_result(result, removed, &mut first_error);
    }
    first_error
}

fn sweep_result(removed: u64, error: Option<Error>, raw_directory: &str) -> Result<u64, Error> {
    match error {
        Some(error) => Err(outcome_unknown_error(
            "sweep temporary files",
            raw_directory,
            &error,
        )),
        None => Ok(removed),
    }
}

fn publish_result(mode: PutMode, err: Error, raw_path: &str) -> Result<(), Error> {
    match classify_publish_error(mode, &err) {
        PublishError::Published => Ok(()),
        PublishError::AlreadyExists => Err(Error::already_exists(raw_path)),
        PublishError::NoPublish => Err(err),
        PublishError::OutcomeUnknown => Err(outcome_unknown_error(
            put_operation_name(mode),
            raw_path,
            &err,
        )),
    }
}

fn put_operation_name(mode: PutMode) -> &'static str {
    match mode {
        PutMode::Overwrite => "overwrite put",
        PutMode::IfNotExists => "create-new put",
    }
}

async fn open_temp_file(
    session: &NfsSession,
    parent: &[&str],
) -> Result<(String, OpenedFile), Error> {
    for _ in 0..TEMP_FILE_CREATE_RETRIES {
        let temp_name = temp_file_name();
        match session.open_file(parent, &temp_name).await? {
            OpenOutcome::Opened(opened) => return Ok((temp_name, opened)),
            OpenOutcome::Exists => continue,
        }
    }

    Err(Error::protocol(format!(
        "failed to allocate a unique temporary file after {TEMP_FILE_CREATE_RETRIES} attempts"
    )))
}

/// Temporary names are the shared prefix plus a ULID, so a sweep can judge
/// staleness from a directory listing alone.
fn temp_file_name() -> String {
    let mut name = String::with_capacity(TEMP_FILE_PREFIX.len() + ulid::ULID_LEN);
    name.push_str(TEMP_FILE_PREFIX);
    name.push_str(Ulid::new().array_to_str(&mut [0; ulid::ULID_LEN]));
    name
}

fn parse_temp_file_created_ms(name: &str) -> Option<u64> {
    let ulid = name.strip_prefix(TEMP_FILE_PREFIX)?;
    Some(Ulid::from_string(ulid).ok()?.timestamp_ms())
}

fn unix_now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| u64::try_from(duration.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or_default()
}

/// Uncommitted writes are not committed here; the caller carries the pending
/// verifier into the verified-close compound, which commits and validates it.
async fn write_bytes(
    session: &NfsSession,
    opened: &OpenedFile,
    body: &Bytes,
) -> Result<WriteOutcome, Error> {
    let body_len = body.len();
    let write_chunk_size = session.write_chunk_size() as usize;
    if body_len == 0 {
        return Ok(WriteOutcome::default());
    }
    if body_len <= write_chunk_size {
        return write_single_chunk_committed(session, opened, 0, body.clone()).await;
    }
    let depth = if session.pipeline_write_chunks() {
        operation_pipeline_depth(session)
    } else {
        1
    };
    if depth <= 1 {
        write_chunk_all(session, opened, 0, body.clone()).await
    } else {
        write_chunk_all_pipelined(session, opened, body, depth).await
    }
}

async fn write_single_chunk_committed(
    session: &NfsSession,
    opened: &OpenedFile,
    mut offset: u64,
    chunk: Bytes,
) -> Result<WriteOutcome, Error> {
    let requested_len = chunk.len();
    let mut written_total = 0usize;
    while written_total < requested_len {
        let remaining = chunk.slice(written_total..);
        let requested_remaining = remaining.len();
        let (write, commit_verifier) = session.write_and_commit(opened, offset, remaining).await?;
        let written = validate_write_reply(&write, requested_remaining)?;
        if write.requires_commit() {
            ensure_write_verifier(write.verifier, commit_verifier)?;
        }
        written_total += written;
        offset += written as u64;
    }
    Ok(WriteOutcome {
        bytes_written: written_total as u64,
        unstable_verifier: None,
    })
}

async fn write_chunk_all_pipelined(
    session: &NfsSession,
    opened: &OpenedFile,
    body: &Bytes,
    depth: usize,
) -> Result<WriteOutcome, Error> {
    let max_write_size = session.write_chunk_size() as usize;
    let mut writes = FuturesUnordered::new();
    let body_len = body.len();
    let mut next_offset = 0usize;
    let mut outcome = WriteOutcome::default();
    let mut first_error = None;

    loop {
        while first_error.is_none() && writes.len() < depth && next_offset < body_len {
            let start = next_offset;
            let end = (start + max_write_size).min(body_len);
            next_offset = end;
            writes.push(write_chunk(
                session,
                opened,
                start as u64,
                body.slice(start..end),
            ));
        }

        if writes.is_empty() {
            break;
        }

        match writes.next().await {
            Some(Ok(written)) => {
                if let Err(err) = outcome.merge(written) {
                    first_error.get_or_insert(err);
                }
            }
            Some(Err(err)) => {
                first_error.get_or_insert(err);
            }
            None => break,
        }
    }

    if let Some(err) = first_error {
        return Err(err);
    }
    Ok(outcome)
}

async fn write_chunk(
    session: &NfsSession,
    opened: &OpenedFile,
    offset: u64,
    chunk: Bytes,
) -> Result<WriteOutcome, Error> {
    let chunk_len = chunk.len();
    let write = session.write(opened, offset, chunk.clone()).await?;
    let written = validate_write_reply(&write, chunk_len)?;
    let mut outcome = WriteOutcome::from_write(&write, written);
    if written == chunk_len {
        return Ok(outcome);
    }

    let rest = write_chunk_all(
        session,
        opened,
        offset + written as u64,
        chunk.slice(written..),
    )
    .await?;
    outcome.merge(rest)?;
    Ok(outcome)
}

async fn write_chunk_all(
    session: &NfsSession,
    opened: &OpenedFile,
    mut offset: u64,
    chunk: Bytes,
) -> Result<WriteOutcome, Error> {
    let mut outcome = WriteOutcome::default();
    let max_write_size = session.write_chunk_size() as usize;
    let chunk_len = chunk.len();
    let mut written_total = 0usize;
    while written_total < chunk_len {
        let end = (written_total + max_write_size).min(chunk_len);
        let remaining = chunk.slice(written_total..end);
        let requested_len = remaining.len();
        let write_offset = offset;
        let write = session.write(opened, write_offset, remaining).await?;
        let written = validate_write_reply(&write, requested_len)?;
        outcome.merge(WriteOutcome::from_write(&write, written))?;
        written_total += written;
        offset += written as u64;
    }
    Ok(outcome)
}

fn validate_write_reply(write: &WriteData, requested_len: usize) -> Result<usize, Error> {
    let written = write.count as usize;
    if written == 0 {
        return Err(Error::protocol("NFS WRITE made no progress"));
    }
    if written > requested_len {
        return Err(Error::protocol(format!(
            "NFS WRITE reported {} bytes for a {} byte request",
            write.count, requested_len
        )));
    }
    Ok(written)
}

#[derive(Debug, Default)]
struct WriteOutcome {
    bytes_written: u64,
    unstable_verifier: Option<[u8; 8]>,
}

impl WriteOutcome {
    fn from_write(write: &WriteData, bytes_written: usize) -> Self {
        Self {
            bytes_written: bytes_written as u64,
            unstable_verifier: if write.requires_commit() {
                Some(write.verifier)
            } else {
                None
            },
        }
    }

    fn merge(&mut self, other: Self) -> Result<(), Error> {
        self.bytes_written += other.bytes_written;
        if let Some(verifier) = other.unstable_verifier {
            self.record_unstable(verifier)?;
        }
        Ok(())
    }

    fn record_unstable(&mut self, verifier: [u8; 8]) -> Result<(), Error> {
        match self.unstable_verifier {
            Some(existing) => ensure_write_verifier(existing, verifier),
            None => {
                self.unstable_verifier = Some(verifier);
                Ok(())
            }
        }
    }
}

/// Advances the READDIR cursor to a page's continuation point, rejecting a
/// server that neither reports EOF nor moves the cookie forward.
fn advance_dir_cursor(
    cookie: &mut u64,
    verifier: &mut [u8; 8],
    page: &DirPage,
) -> Result<(), Error> {
    if page.last_cookie == *cookie {
        return Err(Error::protocol(
            "READDIR did not advance the cookie and was not EOF",
        ));
    }
    *cookie = page.last_cookie;
    *verifier = page.verifier;
    Ok(())
}

fn session_generation_needs_reconnect(current_generation: u64, failed_generation: u64) -> bool {
    current_generation == failed_generation
}

fn reconnect_failure_backoff(config: &NfsConfig) -> Duration {
    config
        .transient_retry_delay
        .max(MIN_RECONNECT_FAILURE_BACKOFF)
}

async fn retry_reconnect_attempts<F, Fut>(
    attempts: &mut u32,
    max_attempts: u32,
    failure_backoff: Duration,
    mut reconnect: F,
) -> Result<(), Error>
where
    F: FnMut(u32) -> Fut,
    Fut: Future<Output = Result<(), Error>>,
{
    debug_assert!(*attempts < max_attempts);
    loop {
        *attempts += 1;
        match reconnect(*attempts).await {
            Ok(()) => return Ok(()),
            Err(_) if *attempts < max_attempts => {
                tokio::time::sleep(failure_backoff).await;
            }
            Err(error) => return Err(error),
        }
    }
}

fn finish_reconnect_attempt(
    coordinator: &mut ReconnectCoordinator,
    failed_generation: u64,
    outcome: ReconnectOutcome,
    failure_backoff: Duration,
) {
    if !matches!(
        &coordinator.state,
        ReconnectState::Connecting { generation, .. }
            if *generation == failed_generation
    ) {
        return;
    }
    coordinator.state = match outcome {
        Ok(()) => ReconnectState::Idle,
        Err(error) => ReconnectState::Failed {
            generation: failed_generation,
            retry_after: Instant::now().checked_add(failure_backoff),
            error,
        },
    };
}

async fn await_reconnect_outcome(
    mut outcome: watch::Receiver<Option<ReconnectOutcome>>,
) -> Result<(), Error> {
    loop {
        if let Some(outcome) = outcome.borrow().clone() {
            return outcome.map_err(|error| error.to_error());
        }
        if outcome.changed().await.is_err() {
            return Err(Error::connection_lost(
                "NFS reconnect task ended before reporting an outcome",
            ));
        }
    }
}

fn outcome_unknown_error(operation: &'static str, path: &str, operation_error: &Error) -> Error {
    let reason: Arc<str> =
        format!("the operation failed after it may have reached the server: {operation_error}")
            .into();
    Error::outcome_unknown(operation, path, reason)
}

fn range_read_needs_size(requested_len: u64, limit: Option<u64>) -> bool {
    limit.is_some_and(|limit| requested_len > limit)
}

fn buffered_range_len(range: &Range<u64>, file_size: Option<u64>) -> u64 {
    match file_size {
        Some(size) if range.start >= size => 0,
        Some(size) => range.end.min(size).saturating_sub(range.start),
        None => range.end - range.start,
    }
}

fn invalid_continuation_token(message: impl Into<String>) -> Error {
    Error::invalid_config(message)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_config() -> NfsConfig {
        NfsConfig {
            read_chunk_size: 1024,
            write_chunk_size: 1024,
            readdir_dircount: 1024,
            readdir_maxcount: 4096,
            file_mode: 0o644,
            dir_mode: 0o755,
            session_slots: 4,
            max_request_size: 4096,
            max_response_size: 4096,
            operation_timeout: Some(Duration::from_secs(1)),
            transient_retries: 1,
            transient_retry_delay: Duration::from_millis(1),
            ..NfsConfig::new("/")
        }
    }

    #[test]
    fn invalid_builder_values_are_reported_instead_of_clamped() {
        let mut config = valid_config();
        config.read_chunk_size = 0;
        assert!(matches!(
            validate_config(&config),
            Err(Error::InvalidConfig(_))
        ));

        let mut config = valid_config();
        config.read_granularity = 0;
        assert!(matches!(
            validate_config(&config),
            Err(Error::InvalidConfig(_))
        ));

        let mut config = valid_config();
        config.readdir_maxcount = config.readdir_dircount - 1;
        assert!(matches!(
            validate_config(&config),
            Err(Error::InvalidConfig(_))
        ));

        let mut config = valid_config();
        config.session_slots = MAX_CONFIGURED_SESSION_SLOTS + 1;
        assert!(matches!(
            validate_config(&config),
            Err(Error::InvalidConfig(message))
                if message.contains("session_slots") && message.contains("1024")
        ));
        assert!(matches!(
            validate_buffered_read_limit(Some(0)),
            Err(Error::InvalidConfig(_))
        ));
        assert!(validate_buffered_read_limit(None).is_ok());
    }

    #[test]
    fn aws_efs_write_cap_is_tls_name_scoped() {
        let mut efs = NfsConfig::new("/");
        efs.write_chunk_size = 1024 * 1024;
        apply_transport_write_cap(
            &mut efs,
            &RpcTransport::Tls(TlsConfig::new("fs-01234567.efs.us-east-1.amazonaws.com")),
        );
        assert_eq!(efs.write_chunk_size, AWS_EFS_MAX_WRITE_CHUNK_SIZE);
        assert!(!efs.pipeline_write_chunks);

        let mut efs_fips = NfsConfig::new("/");
        efs_fips.write_chunk_size = 1024 * 1024;
        apply_transport_write_cap(
            &mut efs_fips,
            &RpcTransport::Tls(TlsConfig::new(
                "fs-01234567.efs-fips.us-gov-west-1.amazonaws.com",
            )),
        );
        assert_eq!(efs_fips.write_chunk_size, AWS_EFS_MAX_WRITE_CHUNK_SIZE);
        assert!(!efs_fips.pipeline_write_chunks);

        assert!(is_aws_efs_server_name(
            "fs-01234567.efs.cn-north-1.amazonaws.com.cn"
        ));
        assert!(!is_aws_efs_server_name("fs-.efs.us-east-1.amazonaws.com"));

        let mut generic_tls = NfsConfig::new("/");
        generic_tls.write_chunk_size = 1024 * 1024;
        apply_transport_write_cap(
            &mut generic_tls,
            &RpcTransport::Tls(TlsConfig::new("nfs.example.com")),
        );
        assert_eq!(generic_tls.write_chunk_size, 1024 * 1024);
        assert!(generic_tls.pipeline_write_chunks);

        let mut plaintext = NfsConfig::new("/");
        plaintext.write_chunk_size = 1024 * 1024;
        apply_transport_write_cap(&mut plaintext, &RpcTransport::Plaintext);
        assert_eq!(plaintext.write_chunk_size, 1024 * 1024);
        assert!(plaintext.pipeline_write_chunks);
    }

    #[test]
    fn temporary_file_names_embed_creation_time_and_stay_unique() {
        let before = unix_now_millis();
        let first_name = temp_file_name();
        let second_name = temp_file_name();
        let after = unix_now_millis();
        assert_ne!(first_name, second_name);
        assert!(first_name.starts_with(TEMP_FILE_PREFIX));

        let created = parse_temp_file_created_ms(&first_name).unwrap();
        assert!((before..=after).contains(&created));
    }

    #[test]
    fn temp_file_parser_only_accepts_crate_named_temps() {
        assert_eq!(parse_temp_file_created_ms("data.bin"), None);
        assert_eq!(parse_temp_file_created_ms(TEMP_FILE_PREFIX), None);
        assert_eq!(
            parse_temp_file_created_ms(&format!("{TEMP_FILE_PREFIX}not-a-ulid")),
            None
        );
        assert_eq!(
            parse_temp_file_created_ms(&format!("{TEMP_FILE_PREFIX}{}", Ulid::from_parts(7, 1))),
            Some(7)
        );
    }

    #[test]
    fn reconnect_is_needed_only_for_the_failed_session_generation() {
        assert!(session_generation_needs_reconnect(0, 0));
        assert!(session_generation_needs_reconnect(u64::MAX, u64::MAX));
        assert!(!session_generation_needs_reconnect(1, 0));
        assert!(!session_generation_needs_reconnect(0, 1));
    }

    #[test]
    fn create_new_publish_errors_preserve_the_publish_boundary() {
        let cleanup_failure = Error::nfs(NfsStatus::DELAY, OpCode::Remove);
        assert_eq!(
            classify_publish_error(PutMode::IfNotExists, &cleanup_failure),
            PublishError::Published
        );

        let conflict = Error::nfs(NfsStatus::EXIST, OpCode::Link);
        assert_eq!(
            classify_publish_error(PutMode::IfNotExists, &conflict),
            PublishError::AlreadyExists
        );

        let rejected_sequence = Error::nfs(NfsStatus::BADSESSION, OpCode::Sequence);
        assert_eq!(
            classify_publish_error(PutMode::IfNotExists, &rejected_sequence),
            PublishError::NoPublish
        );
        assert!(
            publish_result(PutMode::IfNotExists, rejected_sequence, "record")
                .unwrap_err()
                .is_retryable()
        );

        let not_sent = Error::request_not_sent(Error::connection_lost("closed before send"));
        assert_eq!(
            classify_publish_error(PutMode::Overwrite, &not_sent),
            PublishError::NoPublish
        );
        let not_sent = publish_result(PutMode::Overwrite, not_sent, "record").unwrap_err();
        assert!(not_sent.is_request_not_sent());
        assert!(not_sent.is_retryable());

        let invalid_before_send = Error::request_not_sent(Error::protocol("request too large"));
        assert_eq!(
            classify_publish_error(PutMode::IfNotExists, &invalid_before_send),
            PublishError::NoPublish
        );
        assert!(!invalid_before_send.is_retryable());

        for ambiguous in [
            Error::connection_lost("lost after send"),
            Error::Timeout(Duration::from_secs(1)),
            Error::protocol("malformed fused reply"),
            Error::xdr("truncated fused reply"),
            Error::Unsupported("unknown OPEN result".to_owned()),
            Error::nfs(NfsStatus(1), OpCode::Illegal),
        ] {
            for mode in [PutMode::IfNotExists, PutMode::Overwrite] {
                assert_eq!(
                    classify_publish_error(mode, &ambiguous),
                    PublishError::OutcomeUnknown
                );
            }
            assert!(
                publish_result(PutMode::IfNotExists, ambiguous, "record")
                    .unwrap_err()
                    .is_outcome_unknown()
            );
        }

        let overwrite_stopped = Error::nfs(NfsStatus::NOENT, OpCode::Lookup);
        assert_eq!(
            classify_publish_error(PutMode::Overwrite, &overwrite_stopped),
            PublishError::NoPublish
        );
        assert!(
            publish_result(
                PutMode::Overwrite,
                Error::protocol("malformed rename reply"),
                "record",
            )
            .unwrap_err()
            .is_outcome_unknown()
        );
    }

    #[test]
    fn delete_only_retries_errors_that_prove_remove_did_not_run() {
        assert!(delete_result(Ok(RemoveOutcome::Removed), "record").is_ok());
        assert!(delete_result(Ok(RemoveOutcome::Missing), "record").is_ok());

        let rejected_sequence = Error::nfs(NfsStatus::BADSESSION, OpCode::Sequence);
        assert!(
            delete_result(Err(rejected_sequence), "record")
                .unwrap_err()
                .is_retryable()
        );

        let not_sent = Error::request_not_sent(Error::connection_lost("closed before send"));
        let not_sent = delete_result(Err(not_sent), "record").unwrap_err();
        assert!(not_sent.is_request_not_sent());
        assert!(not_sent.is_retryable());

        for ambiguous in [
            Error::connection_lost("lost after send"),
            Error::Timeout(Duration::from_secs(1)),
            Error::protocol("malformed remove reply"),
            Error::xdr("truncated remove reply"),
            Error::nfs(NfsStatus(1), OpCode::Illegal),
        ] {
            assert!(
                delete_result(Err(ambiguous), "record")
                    .unwrap_err()
                    .is_outcome_unknown()
            );
        }
    }

    #[test]
    fn sweep_counts_only_confirmed_removals_and_preserves_first_error() {
        let mut removed = 0;
        let mut first_error = None;
        tally_remove_result(Ok(RemoveOutcome::Removed), &mut removed, &mut first_error);
        tally_remove_result(Ok(RemoveOutcome::Missing), &mut removed, &mut first_error);
        tally_remove_result(
            Err(Error::request_not_sent(Error::connection_lost("first"))),
            &mut removed,
            &mut first_error,
        );
        tally_remove_result(
            Err(Error::connection_lost("second")),
            &mut removed,
            &mut first_error,
        );

        assert_eq!(removed, 1);
        let error = sweep_result(removed, first_error, "temp").unwrap_err();
        assert!(error.is_outcome_unknown());
        assert!(error.to_string().contains("first"));
    }

    #[test]
    fn ambiguous_create_new_error_preserves_path_and_cause() {
        let operation_error = Error::Timeout(Duration::from_secs(1));

        let error = outcome_unknown_error("create-new put", "records/one", &operation_error);

        assert!(matches!(
            error,
            Error::OutcomeUnknown {
                operation: "create-new put",
                path,
                reason,
            } if path == "records/one"
                && reason.contains("operation timed out")
        ));
    }

    #[tokio::test]
    async fn cancelled_reconnect_waiter_does_not_cancel_shared_outcome() {
        let (sender, outcome) = watch::channel(None);
        let cancelled = tokio::spawn(await_reconnect_outcome(outcome.clone()));
        let first = tokio::spawn(await_reconnect_outcome(outcome.clone()));
        let second = tokio::spawn(await_reconnect_outcome(outcome));

        cancelled.abort();
        sender
            .send(Some(Err(ReconnectFailure::Timeout(Duration::from_secs(3)))))
            .unwrap();

        for waiter in [first, second] {
            assert!(matches!(
                waiter.await.unwrap(),
                Err(Error::Timeout(timeout)) if timeout == Duration::from_secs(3)
            ));
        }
    }

    #[test]
    fn reconnect_backoff_has_a_nonzero_floor() {
        let mut config = valid_config();
        config.transient_retry_delay = Duration::ZERO;
        assert_eq!(
            reconnect_failure_backoff(&config),
            MIN_RECONNECT_FAILURE_BACKOFF
        );
        config.transient_retry_delay = Duration::from_secs(2);
        assert_eq!(reconnect_failure_backoff(&config), Duration::from_secs(2));
    }

    #[tokio::test]
    async fn reconnect_budget_recovers_after_a_failed_rebuild() {
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut attempts = 0;
        retry_reconnect_attempts(&mut attempts, 3, Duration::ZERO, {
            let calls = Arc::clone(&calls);
            move |attempt| {
                let calls = Arc::clone(&calls);
                async move {
                    calls.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    if attempt == 1 {
                        Err(Error::connection_lost("first rebuild failed"))
                    } else {
                        Ok(())
                    }
                }
            }
        })
        .await
        .unwrap();

        assert_eq!(attempts, 2);
        assert_eq!(calls.load(std::sync::atomic::Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn reconnect_budget_returns_the_last_failure_when_exhausted() {
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut attempts = 0;
        let error = retry_reconnect_attempts(&mut attempts, 3, Duration::ZERO, {
            let calls = Arc::clone(&calls);
            move |attempt| {
                let calls = Arc::clone(&calls);
                async move {
                    calls.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    Err(Error::connection_lost(format!("rebuild {attempt} failed")))
                }
            }
        })
        .await
        .unwrap_err();

        assert_eq!(attempts, 3);
        assert_eq!(calls.load(std::sync::atomic::Ordering::Relaxed), 3);
        assert!(error.to_string().contains("rebuild 3 failed"));
    }

    #[test]
    fn stale_reconnect_completion_does_not_overwrite_a_new_attempt() {
        let (_sender, outcome) = watch::channel(None);
        let mut coordinator = ReconnectCoordinator {
            state: ReconnectState::Connecting {
                generation: 2,
                outcome,
            },
        };

        finish_reconnect_attempt(&mut coordinator, 1, Ok(()), Duration::from_secs(1));

        assert!(matches!(
            coordinator.state,
            ReconnectState::Connecting { generation: 2, .. }
        ));
    }

    #[test]
    fn reconnect_completion_updates_only_its_generation() {
        let (_sender, outcome) = watch::channel(None);
        let mut coordinator = ReconnectCoordinator {
            state: ReconnectState::Connecting {
                generation: 7,
                outcome,
            },
        };
        finish_reconnect_attempt(
            &mut coordinator,
            7,
            Err(ReconnectFailure::Other("failed".into())),
            Duration::from_secs(1),
        );

        assert!(matches!(
            coordinator.state,
            ReconnectState::Failed {
                generation: 7,
                error: ReconnectFailure::Other(ref message),
                ..
            } if message.as_ref() == "failed"
        ));
    }

    #[test]
    fn reconnect_failure_deadline_handles_unbounded_delays() {
        let (_sender, outcome) = watch::channel(None);
        let mut coordinator = ReconnectCoordinator {
            state: ReconnectState::Connecting {
                generation: 9,
                outcome,
            },
        };

        finish_reconnect_attempt(
            &mut coordinator,
            9,
            Err(ReconnectFailure::Other("failed".into())),
            Duration::MAX,
        );

        assert!(matches!(
            coordinator.state,
            ReconnectState::Failed {
                generation: 9,
                retry_after: None,
                ..
            }
        ));
    }

    #[test]
    fn buffered_range_len_uses_effective_file_size_when_known() {
        assert_eq!(buffered_range_len(&(2..8), Some(10)), 6);
        assert_eq!(buffered_range_len(&(2..20), Some(10)), 8);
        assert_eq!(buffered_range_len(&(20..30), Some(10)), 0);
        assert_eq!(buffered_range_len(&(2..8), None), 6);
    }

    #[test]
    fn list_tokens_round_trip_nfs_position() {
        let token = ContinuationToken::from_list_position("data", 42, [7; 8]);
        assert_eq!(token.directory.as_ref(), "data");
        assert_eq!(token.cookie, 42);
        assert_eq!(token.verifier, [7; 8]);
        assert_eq!(token.list_position("data").unwrap(), (42, [7; 8]));
        assert!(token.list_position("other").is_err());
    }

    #[test]
    fn list_tokens_keep_directories_with_separators_readable() {
        let token = ContinuationToken::from_list_position("dir:with:colon", 1, [0; 8]);

        assert_eq!(token.list_position("dir:with:colon").unwrap(), (1, [0; 8]));
        assert_eq!(token.directory.as_ref(), "dir:with:colon");

        let root = ContinuationToken::from_list_position("", 2, [3; 8]);
        assert_eq!(root.list_position("").unwrap(), (2, [3; 8]));
    }

    #[test]
    fn range_reads_fetch_size_only_when_needed_for_limit() {
        assert!(!range_read_needs_size(4, Some(4)));
        assert!(!range_read_needs_size(4, Some(10)));
        assert!(!range_read_needs_size(4, None));
        assert!(range_read_needs_size(5, Some(4)));
    }

    #[test]
    fn buffered_read_capacity_uses_known_size_and_limit() {
        assert_eq!(buffered_read_capacity(Some(16), 32, Some(8)), 8);
        assert_eq!(buffered_read_capacity(Some(16), 32, None), 16);
        assert_eq!(buffered_read_capacity(None, 16, Some(8)), 8);
        assert_eq!(buffered_read_capacity(None, 16, None), 16);
    }

    #[test]
    fn buffered_read_limit_counts_existing_output() {
        assert!(ensure_buffered_read_within_limit(3, 2, Some(5), Some(5)).is_ok());
        let err = ensure_buffered_read_within_limit(3, 3, Some(6), Some(5)).unwrap_err();
        assert!(matches!(
            err,
            Error::BufferedReadTooLarge {
                size: Some(6),
                limit: 5,
            }
        ));
        assert!(ensure_buffered_read_within_limit(usize::MAX, 1, None, None).is_ok());
    }

    #[test]
    fn read_size_validation_rejects_oversized_reply() {
        let read = ReadData {
            eof: false,
            data: Bytes::from_static(b"abc"),
        };

        let err = validate_read_reply(&read, 2).unwrap_err();
        assert!(matches!(
            err,
            Error::Protocol(message)
                if message == "NFS READ returned 3 bytes for a 2 byte request"
        ));
    }

    #[test]
    fn read_reply_validation_rejects_empty_non_eof_progress() {
        let read = ReadData {
            eof: false,
            data: Bytes::new(),
        };

        let err = validate_read_reply(&read, 4).unwrap_err();
        assert!(matches!(
            err,
            Error::Protocol(message)
                if message == "NFS READ returned no data for a nonzero request without EOF"
        ));
        assert!(validate_read_reply(&read, 1).is_err());
    }

    #[test]
    fn short_non_eof_read_is_marked_for_retry() {
        let short = ReadData {
            eof: false,
            data: Bytes::from_static(b"ab"),
        };
        let eof = ReadData {
            eof: true,
            data: Bytes::from_static(b"ab"),
        };
        let full = ReadData {
            eof: false,
            data: Bytes::from_static(b"abcd"),
        };

        assert!(read_needs_retry(&short, 4));
        assert!(!read_needs_retry(&eof, 4));
        assert!(!read_needs_retry(&full, 4));
    }

    #[test]
    fn exact_reads_require_eof_at_the_expected_offset() {
        assert!(validate_expected_eof(8, 8, true).is_ok());
        assert!(matches!(
            validate_expected_eof(8, 5, true),
            Err(Error::FileSizeMismatch {
                expected: 8,
                actual: Some(5),
            })
        ));
        assert!(matches!(
            validate_expected_eof(8, 8, false),
            Err(Error::FileSizeMismatch {
                expected: 8,
                actual: None,
            })
        ));
        assert!(validate_expected_eof(8, 5, false).is_ok());
    }

    fn read_output(offset: u64, data: &'static [u8], eof: bool) -> ReadTaskOutput {
        ReadTaskOutput {
            offset,
            requested: data.len().max(1) as u32,
            data: Bytes::from_static(data),
            eof,
        }
    }

    #[test]
    fn pipelined_read_state_stays_inline_at_max_depth() {
        let mut out = BytesMut::new();
        let mut emit = PipelinedEmit::new(&mut out, usize::MAX as u64, None, None, None);
        for offset in 0..MAX_OPERATION_PIPELINE_DEPTH {
            emit.record(read_output(offset as u64, b"x", false));
        }
        assert_eq!(emit.completed.len(), MAX_OPERATION_PIPELINE_DEPTH);
        assert!(!emit.completed.spilled());

        let mut scheduler = ChunkScheduler::new(0, 0, 1);
        for offset in 0..MAX_OPERATION_PIPELINE_DEPTH {
            scheduler.push_gap(offset as u64, 1);
        }
        assert_eq!(scheduler.gaps.len(), MAX_OPERATION_PIPELINE_DEPTH);
        assert!(!scheduler.gaps.spilled());
    }

    #[test]
    fn pipelined_read_completion_emits_directly() {
        let mut out = BytesMut::new();
        let mut emit = PipelinedEmit::new(&mut out, 10, None, None, None);

        emit.record(read_output(10, b"abc", false));

        assert_eq!(emit.next_offset, 13);
        assert_eq!(emit.eof_at, None);
        assert!(emit.first_error.is_none());
        assert!(emit.completed.is_empty());
        assert_eq!(&out[..], b"abc");
    }

    #[test]
    fn pipelined_exact_read_validates_the_final_eof() {
        let mut exact = BytesMut::new();
        let mut emit = PipelinedEmit::new(
            &mut exact,
            0,
            Some(4),
            Some(ExpectedEof::CallerSupplied(4)),
            None,
        );
        emit.record(read_output(0, b"abcd", true));
        assert!(emit.finish().is_ok());

        let mut larger = BytesMut::new();
        let mut emit = PipelinedEmit::new(
            &mut larger,
            0,
            Some(4),
            Some(ExpectedEof::CallerSupplied(4)),
            None,
        );
        emit.record(read_output(0, b"abcd", false));
        assert!(matches!(
            emit.finish(),
            Err(Error::FileSizeMismatch {
                expected: 4,
                actual: None,
            })
        ));

        let mut smaller = BytesMut::new();
        let mut emit = PipelinedEmit::new(
            &mut smaller,
            0,
            Some(4),
            Some(ExpectedEof::CallerSupplied(4)),
            None,
        );
        emit.record(read_output(0, b"abc", true));
        assert!(matches!(
            emit.finish(),
            Err(Error::FileSizeMismatch {
                expected: 4,
                actual: Some(3),
            })
        ));
    }

    #[test]
    fn pipelined_read_rejects_queued_data_beyond_exact_eof() {
        let mut out = BytesMut::new();
        let mut emit = PipelinedEmit::new(&mut out, 0, None, None, None);
        emit.record(read_output(5, b"x", false));
        emit.record(read_output(0, b"abcde", true));

        let result = emit.finish();
        assert_eq!(&out[..], b"abcde");
        assert!(matches!(
            result,
            Err(Error::Protocol(message))
                if message == "pipelined READ returned queued data beyond EOF"
        ));
    }

    #[test]
    fn queued_pipelined_read_completions_drain_after_gap_closes() {
        let mut out = BytesMut::new();
        let mut emit = PipelinedEmit::new(&mut out, 10, None, None, None);

        emit.record(read_output(13, b"def", false));
        assert_eq!(emit.completed.len(), 1);
        assert_eq!(emit.next_offset, 10);

        emit.record(read_output(10, b"abc", false));

        assert!(emit.completed.is_empty());
        assert_eq!(emit.next_offset, 16);
        assert_eq!(emit.eof_at, None);
        assert!(emit.first_error.is_none());
        assert_eq!(&out[..], b"abcdef");
    }

    #[test]
    fn queued_pipelined_reads_continue_until_observed_eof_offset() {
        let mut out = BytesMut::new();
        let mut emit = PipelinedEmit::new(&mut out, 0, Some(9), None, None);

        emit.record(read_output(3, b"def", true));
        assert_eq!(emit.eof_at, Some(6));

        emit.record(read_output(0, b"abc", false));

        assert!(emit.completed.is_empty());
        assert_eq!(emit.next_offset, 6);
        assert_eq!(emit.eof_at, Some(6));
        assert!(emit.first_error.is_none());
        assert_eq!(&out[..], b"abcdef");
    }

    #[test]
    fn repeated_observed_eof_at_the_same_offset_is_a_noop() {
        let mut out = BytesMut::new();
        let mut emit = PipelinedEmit::new(&mut out, 0, None, None, None);

        emit.observe_exact_eof(12);
        emit.observe_exact_eof(12);

        assert_eq!(emit.eof_at, Some(12));
        assert_eq!(emit.eof_upper_bound, Some(12));
        assert!(emit.first_error.is_none());
    }

    #[test]
    fn conflicting_observed_eof_offsets_become_a_protocol_error() {
        let mut out = BytesMut::new();
        let mut emit = PipelinedEmit::new(&mut out, 0, Some(15), None, None);

        emit.record(read_output(10, b"klmno", true));
        emit.record(read_output(0, b"abcde", false));
        emit.record(ReadTaskOutput {
            offset: 5,
            requested: 5,
            data: Bytes::from_static(b"fghij"),
            eof: true,
        });

        assert!(matches!(
            emit.first_error,
            Some(Error::Protocol(ref message))
                if message == "pipelined READ reported conflicting EOF offsets: 15 and 10"
        ));
        let err = emit.finish().unwrap_err();
        assert!(matches!(
            err,
            Error::Protocol(ref message)
                if message == "pipelined READ reported conflicting EOF offsets: 15 and 10"
        ));
    }

    #[test]
    fn empty_out_of_order_eof_is_an_upper_bound_not_an_exact_offset() {
        let mut out = BytesMut::new();
        let mut emit = PipelinedEmit::new(&mut out, 0, Some(15), None, None);

        emit.record(ReadTaskOutput {
            offset: 20,
            requested: 5,
            data: Bytes::new(),
            eof: true,
        });
        assert_eq!(emit.eof_at, None);
        assert_eq!(emit.eof_upper_bound, Some(20));

        emit.record(read_output(10, b"klmno", true));
        emit.record(read_output(0, b"abcdefghij", false));

        assert_eq!(emit.completed.len(), 1);
        assert_eq!(emit.next_offset, 15);
        assert_eq!(emit.eof_at, Some(15));
        assert_eq!(emit.eof_upper_bound, Some(15));
        assert!(emit.first_error.is_none());
        assert!(emit.finish().is_ok());
        assert_eq!(&out[..], b"abcdefghijklmno");
    }

    #[test]
    fn short_reads_leave_a_gap_and_eof_reads_do_not() {
        let short = ReadTaskOutput {
            offset: 10,
            requested: 8,
            data: Bytes::from_static(b"abc"),
            eof: false,
        };
        assert_eq!(short.short_read_gap(), Some((13, 5)));

        let eof = ReadTaskOutput {
            offset: 10,
            requested: 8,
            data: Bytes::from_static(b"abc"),
            eof: true,
        };
        assert_eq!(eof.short_read_gap(), None);

        let full = ReadTaskOutput {
            offset: 10,
            requested: 3,
            data: Bytes::from_static(b"abc"),
            eof: false,
        };
        assert_eq!(full.short_read_gap(), None);
    }

    #[test]
    fn eof_before_gap_still_schedules_the_gap_and_reassembles_full_output() {
        let mut out = BytesMut::new();
        let mut emit = PipelinedEmit::new(&mut out, 0, Some(15), None, None);
        let mut scheduler = ChunkScheduler::new(0, 15, 10);

        assert_eq!(scheduler.next_range(false), Some((0, 10)));
        assert_eq!(scheduler.next_range(false), Some((10, 5)));
        assert_eq!(scheduler.next_range(false), None);

        emit.record(ReadTaskOutput {
            offset: 10,
            requested: 5,
            data: Bytes::from_static(b"klmno"),
            eof: true,
        });
        assert_eq!(emit.eof_at, Some(15));

        let short = ReadTaskOutput {
            offset: 0,
            requested: 10,
            data: Bytes::from_static(b"abcde"),
            eof: false,
        };
        if let Some((gap_offset, gap_len)) = short.short_read_gap() {
            scheduler.push_gap(gap_offset, gap_len);
        }
        emit.record(short);

        assert_eq!(scheduler.next_range(emit.eof_observed()), Some((5, 5)));

        emit.record(ReadTaskOutput {
            offset: 5,
            requested: 5,
            data: Bytes::from_static(b"fghij"),
            eof: false,
        });

        assert!(emit.completed.is_empty());
        assert_eq!(emit.next_offset, 15);
        assert_eq!(emit.eof_at, Some(15));
        assert!(emit.first_error.is_none());
        let result = emit.finish();
        assert!(result.is_ok());
        assert_eq!(&out[..], b"abcdefghijklmno");
    }

    #[test]
    fn finish_rejects_a_hole_below_observed_eof() {
        let mut out = BytesMut::new();
        let mut emit = PipelinedEmit::new(&mut out, 0, Some(15), None, None);

        emit.record(ReadTaskOutput {
            offset: 10,
            requested: 5,
            data: Bytes::from_static(b"klmno"),
            eof: true,
        });

        let err = emit.finish().unwrap_err();
        assert!(matches!(
            err,
            Error::Protocol(message)
                if message == "pipelined READ completed with a hole below EOF"
        ));
    }

    #[test]
    fn operation_pipeline_depth_tracks_slots_with_internal_cap() {
        assert_eq!(operation_pipeline_depth_for_slots(0), 1);
        assert_eq!(operation_pipeline_depth_for_slots(1), 1);
        assert_eq!(operation_pipeline_depth_for_slots(4), 4);
        assert_eq!(
            operation_pipeline_depth_for_slots(MAX_OPERATION_PIPELINE_DEPTH),
            MAX_OPERATION_PIPELINE_DEPTH
        );
        assert_eq!(
            operation_pipeline_depth_for_slots(MAX_OPERATION_PIPELINE_DEPTH + 1),
            MAX_OPERATION_PIPELINE_DEPTH
        );
    }

    #[test]
    fn write_reply_validation_rejects_no_progress_and_oversized_count() {
        let no_progress = WriteData::test_unstable(0, [1; 8]);
        let err = validate_write_reply(&no_progress, 4).unwrap_err();
        assert!(matches!(
            err,
            Error::Protocol(message) if message == "NFS WRITE made no progress"
        ));

        let oversized = WriteData::test_unstable(5, [1; 8]);
        let err = validate_write_reply(&oversized, 4).unwrap_err();
        assert!(matches!(
            err,
            Error::Protocol(message)
                if message == "NFS WRITE reported 5 bytes for a 4 byte request"
        ));
    }

    #[test]
    fn write_outcome_tracks_unstable_write_verifier() {
        let write = WriteData::test_unstable(4, [7; 8]);

        let outcome = WriteOutcome::from_write(&write, 4);

        assert_eq!(outcome.bytes_written, 4);
        assert_eq!(outcome.unstable_verifier, Some([7; 8]));
    }

    #[test]
    fn write_outcome_merge_rejects_changed_unstable_verifier() {
        let mut first = WriteOutcome::from_write(&WriteData::test_unstable(4, [7; 8]), 4);
        let second = WriteOutcome::from_write(&WriteData::test_unstable(4, [8; 8]), 4);

        let err = first.merge(second).unwrap_err();

        assert!(err.is_retryable());
    }

    #[test]
    fn gap_ranges_are_scheduled_before_new_ranges_without_shifting() {
        let mut scheduler = ChunkScheduler::new(0, 100, 10);
        scheduler.push_gap(40, 5);
        scheduler.push_gap(60, 7);

        assert_eq!(scheduler.next_range(false), Some((60, 7)));
        assert_eq!(scheduler.next_range(false), Some((40, 5)));
        assert_eq!(scheduler.next_range(false), Some((0, 10)));
        assert_eq!(scheduler.next_offset, 10);

        let mut exhausted = ChunkScheduler::new(95, 100, 10);
        assert_eq!(exhausted.next_range(false), Some((95, 5)));
        assert_eq!(exhausted.next_range(false), None);

        let mut eof_observed = ChunkScheduler::new(0, 100, 10);
        eof_observed.push_gap(20, 3);
        assert_eq!(eof_observed.next_range(true), Some((20, 3)));
        assert_eq!(eof_observed.next_range(true), None);
    }

    #[test]
    fn public_operation_futures_are_send_for_common_path_types() {
        fn assert_send<T: Send>(_: T) {}

        fn compile_check(client: NfsClient, path: String, body: Bytes) {
            let put_client = client.clone();
            assert_send(put_client.put(path.clone(), body.clone(), PutMode::Overwrite));

            let sweep_client = client.clone();
            assert_send(sweep_client.sweep_temp_files(path.clone(), Duration::ZERO));

            let get_client = client.clone();
            assert_send(get_client.get(path.clone()));

            let get_known_size_client = client.clone();
            assert_send(get_known_size_client.get_known_size(path.clone(), 1));

            let entry_info_client = client.clone();
            assert_send(entry_info_client.entry_info(path.clone()));

            let get_range_client = client.clone();
            assert_send(get_range_client.get_range(path.clone(), 0..1));

            let delete_client = client.clone();
            assert_send(delete_client.delete(path.clone()));

            assert_send(client.list_page(path, 100, None));
        }

        let _ = compile_check as fn(NfsClient, String, Bytes);
    }
}
