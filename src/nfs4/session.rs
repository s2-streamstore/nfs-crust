use super::*;

#[derive(Debug, Clone)]
pub(crate) struct NfsSession {
    inner: Arc<NfsSessionInner>,
}

#[derive(Debug)]
struct NfsSessionInner {
    rpc: RpcClient,
    server_addr: SocketAddr,
    export_root: FileHandle,
    clientid: u64,
    sessionid: [u8; 16],
    slots: SlotTable,
    client_owner: ClientOwner,
    client_verifier: u64,
    fused_put_supported: AtomicBool,
    reclaim_complete: AtomicBool,
    fatal_sequence_flags: AtomicU32,
    deferred_close_tx: mpsc::Sender<OpenedFile>,
    config: NfsConfig,
}

impl NfsSession {
    pub(crate) async fn connect(
        addr: SocketAddr,
        auth: AuthSys,
        config: NfsConfig,
        transport: RpcTransport,
        client_owner: &ClientOwner,
    ) -> Result<Self, Error> {
        let rpc = RpcClient::connect(
            addr,
            auth,
            config.max_response_size as usize,
            config.session_slots as usize,
            transport,
        )
        .await?;
        let client_verifier = client_owner.verifier_value();
        let exchange = exchange_id(&rpc, client_owner, client_verifier).await?;
        let created = create_session(&rpc, exchange.clientid, exchange.sequenceid, &config).await?;
        rpc.reduce_max_response_bytes(created.fore_attrs.max_response_size as usize);
        let slots = created
            .fore_attrs
            .maxrequests
            .min(config.session_slots)
            .max(1);
        let config = negotiated_config(config, &created.fore_attrs);
        tracing::debug!(
            server_addr = %addr,
            session_slots = slots,
            max_request_size = config.max_request_size,
            max_response_size = config.max_response_size,
            max_compound_ops = config.max_compound_ops,
            "NFS session established",
        );

        let (deferred_close_tx, deferred_close_rx) = mpsc::channel(DEFERRED_CLOSE_QUEUE_CAPACITY);
        let mut session = Self {
            inner: Arc::new(NfsSessionInner {
                rpc,
                server_addr: addr,
                export_root: FileHandle(Bytes::new()),
                clientid: exchange.clientid,
                sessionid: created.sessionid,
                slots: SlotTable::new(slots),
                client_owner: client_owner.clone(),
                client_verifier,
                fused_put_supported: AtomicBool::new(true),
                reclaim_complete: AtomicBool::new(false),
                fatal_sequence_flags: AtomicU32::new(0),
                deferred_close_tx,
                config,
            }),
        };

        match session.reclaim_complete().await {
            Ok(()) => {}
            Err(err) if err.is_nfs_status(NfsStatus::COMPLETE_ALREADY) => {}
            Err(err) => return Err(err),
        }
        session
            .inner
            .reclaim_complete
            .store(true, Ordering::Release);
        let export_root = {
            let export_path = parse_export(&session.inner.config.export)?;
            session
                .resolve_export_root(export_path.components())
                .await?
        };
        let inner = Arc::get_mut(&mut session.inner).ok_or_else(|| {
            Error::protocol("new NFS session was unexpectedly shared during connection setup")
        })?;
        inner.export_root = export_root;
        tokio::spawn(run_deferred_close_worker(
            Arc::downgrade(&session.inner),
            deferred_close_rx,
        ));
        Ok(session)
    }

    pub(crate) fn read_chunk_size(&self) -> u32 {
        self.inner.config.read_chunk_size
    }

    pub(crate) fn read_granularity(&self) -> u32 {
        self.inner.config.read_granularity
    }

    pub(crate) fn write_chunk_size(&self) -> u32 {
        self.inner.config.write_chunk_size
    }

    pub(crate) fn pipeline_write_chunks(&self) -> bool {
        self.inner.config.pipeline_write_chunks
    }

    pub(crate) fn session_slot_count(&self) -> usize {
        self.inner.slots.effective_len()
    }

    pub(crate) fn server_addr(&self) -> SocketAddr {
        self.inner.server_addr
    }

    pub(crate) async fn create_parent_dirs<S>(&self, parent: &[S]) -> Result<(), Error>
    where
        S: AsRef<str>,
    {
        let mut parent_fh = None;
        for depth in 0..parent.len() {
            let name = parent[depth].as_ref();
            let needs_child_handle = depth + 1 < parent.len();

            match (&parent_fh, needs_child_handle) {
                (Some(fh), true) => {
                    parent_fh = Some(self.ensure_child_dir_handle(fh, name).await?);
                }
                (Some(fh), false) => {
                    self.create_dir_at_handle(fh, name).await?;
                }
                (None, true) => match self.create_dir_with_handle(&parent[..depth], name).await? {
                    CreateDirHandleOutcome::Created(fh) => parent_fh = Some(fh),
                    CreateDirHandleOutcome::Exists => {}
                },
                (None, false) => {
                    self.create_dir(&parent[..depth], name).await?;
                }
            }
        }
        Ok(())
    }

    /// Opens `name` under `parent` for writing, creating it guarded: an
    /// already-present name returns [`OpenOutcome::Exists`].
    pub(crate) async fn open_file<S>(
        &self,
        parent: &[S],
        name: &str,
    ) -> Result<OpenOutcome<OpenedFile>, Error>
    where
        S: AsRef<str>,
    {
        let owner = self.next_owner();
        let mut ops = self.path_ops_with_extra(parent, 2);
        ops.push(NfsOp::Open {
            seqid: 0,
            clientid: self.inner.clientid,
            owner: owner.as_bytes(),
            name,
            file_mode: self.inner.config.file_mode,
        });
        ops.push(NfsOp::GetFh);
        match self
            .compound_decode("open", &ops, |bytes| {
                decode_open_file_compound(bytes, parent.len())
            })
            .await?
        {
            OpenOutcome::Exists => Ok(OpenOutcome::Exists),
            OpenOutcome::Opened((stateid, fh)) => Ok(OpenOutcome::Opened(OpenedFile {
                fh,
                stateid,
                close_seqid: 1,
            })),
        }
    }

    pub(crate) async fn close_file(&self, opened: &mut OpenedFile) -> Result<(), Error> {
        let ops = [
            NfsOp::PutFh(&opened.fh),
            NfsOp::Close {
                seqid: opened.close_seqid,
                stateid: opened.stateid,
            },
        ];
        self.compound_decode("close", &ops, |bytes| {
            decode_close_compound(bytes, CloseShape::File, None)
        })
        .await?;
        opened.close_seqid = opened.close_seqid.wrapping_add(1);
        Ok(())
    }

    /// Queues cleanup of an already-published open state. Queue capacity is
    /// bounded, so sustained publication applies backpressure without waiting
    /// for the CLOSE round trip itself.
    pub(crate) async fn defer_close(&self, opened: OpenedFile) -> Result<(), OpenedFile> {
        self.inner
            .deferred_close_tx
            .send(opened)
            .await
            .map_err(|err| err.0)
    }

    async fn close_batch(&self, opened: &[OpenedFile]) -> Result<(), Error> {
        let max_files_by_ops = (self.inner.config.max_compound_ops as usize)
            .saturating_sub(1)
            .checked_div(2)
            .unwrap_or_default()
            .clamp(1, MAX_DEFERRED_CLOSE_BATCH);
        let mut start = 0usize;
        while start < opened.len() {
            let mut end = (start + max_files_by_ops).min(opened.len());
            let ops = loop {
                let mut ops = CompoundOps::with_capacity((end - start).saturating_mul(2));
                for file in &opened[start..end] {
                    ops.push(NfsOp::PutFh(&file.fh));
                    ops.push(NfsOp::Close {
                        seqid: file.close_seqid,
                        stateid: file.stateid,
                    });
                }
                if end == start + 1 || self.compound_request_fits("close-batch", &ops) {
                    break ops;
                }
                end -= 1;
            };
            let close_count = end - start;
            self.compound_decode_no_transient_retry("close-batch", &ops, |bytes| {
                decode_close_batch_compound(bytes, close_count)
            })
            .await?;
            start = end;
        }
        Ok(())
    }

    /// Closes a written temporary file and atomically publishes it over the
    /// destination name, committing any unstable bytes and verifying the
    /// final size in the same compound. The VERIFY halts the compound before
    /// the publish if the file size does not match what was written. The
    /// publish is `RENAME`, or for `create_new` a `LINK` — which fails with
    /// `EXIST` when the destination is already present — followed by
    /// `REMOVE` of the temporary name.
    #[expect(
        clippy::too_many_arguments,
        reason = "fixed-shape compound: open state, directory, both names, size, verifier, mode"
    )]
    pub(crate) async fn close_publish<S>(
        &self,
        opened: &mut OpenedFile,
        parent: &[S],
        from_name: &str,
        to_name: &str,
        size: u64,
        unstable_verifier: Option<[u8; 8]>,
        create_new: bool,
    ) -> Result<(), Error>
    where
        S: AsRef<str>,
    {
        let mut ops = CompoundOps::with_capacity(8 + parent.len());
        ops.push(NfsOp::PutFh(&opened.fh));
        if unstable_verifier.is_some() {
            ops.push(NfsOp::Commit {
                offset: 0,
                count: commit_count(size),
            });
        }
        ops.push(NfsOp::Verify { size });
        ops.push(NfsOp::Close {
            seqid: opened.close_seqid,
            stateid: opened.stateid,
        });
        let (tag, shape) = if create_new {
            ops.push(NfsOp::SaveFh);
            extend_export_path_ops(&mut ops, &self.inner.export_root, parent);
            ops.push(NfsOp::Link(to_name));
            ops.push(NfsOp::Remove(from_name));
            (
                "close-link-remove",
                CloseShape::LinkRemove {
                    parent_lookup_count: parent.len(),
                },
            )
        } else {
            extend_export_path_ops(&mut ops, &self.inner.export_root, parent);
            ops.push(NfsOp::SaveFh);
            ops.push(NfsOp::Rename {
                old_name: from_name,
                new_name: to_name,
            });
            (
                "close-rename",
                CloseShape::Rename {
                    parent_lookup_count: parent.len(),
                },
            )
        };

        let mut transient_attempts = 0;
        match self
            .compound_decode_detailed(
                tag,
                &ops,
                |bytes| decode_close_compound(bytes, shape, unstable_verifier),
                true,
                &mut transient_attempts,
            )
            .await
        {
            Ok(()) => {}
            Err(err)
                if err.completed_operation(&ops, OpCode::Close)
                    && !err.completed_operation(&ops, OpCode::Link)
                    && !err.completed_operation(&ops, OpCode::Rename)
                    && err.transient_status().is_some() =>
            {
                drop(ops);
                if !self
                    .retry_transient_compound_error(&err, &mut transient_attempts)
                    .await
                {
                    return Err(err.into_error());
                }
                self.publish_after_close(
                    &opened.fh,
                    parent,
                    from_name,
                    to_name,
                    create_new,
                    &mut transient_attempts,
                )
                .await?;
            }
            Err(err) => return Err(err.into_error()),
        }
        opened.close_seqid = opened.close_seqid.wrapping_add(1);
        Ok(())
    }

    async fn publish_after_close<S>(
        &self,
        source_fh: &FileHandle,
        parent: &[S],
        from_name: &str,
        to_name: &str,
        create_new: bool,
        transient_attempts: &mut u32,
    ) -> Result<(), Error>
    where
        S: AsRef<str>,
    {
        let mut ops = CompoundOps::with_capacity(5 + parent.len());
        let (tag, decode_link_remove) = if create_new {
            ops.push(NfsOp::PutFh(source_fh));
            ops.push(NfsOp::SaveFh);
            extend_export_path_ops(&mut ops, &self.inner.export_root, parent);
            ops.push(NfsOp::Link(to_name));
            ops.push(NfsOp::Remove(from_name));
            ("publish-link-remove", true)
        } else {
            extend_export_path_ops(&mut ops, &self.inner.export_root, parent);
            ops.push(NfsOp::SaveFh);
            ops.push(NfsOp::Rename {
                old_name: from_name,
                new_name: to_name,
            });
            ("publish-rename", false)
        };
        self.compound_decode_detailed(
            tag,
            &ops,
            |bytes| {
                if decode_link_remove {
                    decode_link_remove_publish_compound(bytes, parent.len())
                } else {
                    decode_rename_publish_compound(bytes, parent.len())
                }
            },
            true,
            transient_attempts,
        )
        .await
        .map_err(CompoundDecodeError::into_error)
    }

    pub(crate) async fn lookup_file_handle<S>(&self, components: &[S]) -> Result<FileHandle, Error>
    where
        S: AsRef<str>,
    {
        let mut ops = self.path_ops_with_extra(components, 1);
        ops.push(NfsOp::GetFh);
        self.compound_getfh("lookup-file", &ops, OpCode::PutFh, components.len())
            .await
    }

    pub(crate) async fn lookup_file_handle_attrs<S>(
        &self,
        components: &[S],
        attr_request: AttrRequest,
    ) -> Result<(FileHandle, FileAttrs), Error>
    where
        S: AsRef<str>,
    {
        let mut ops = self.path_ops_with_extra(components, 2);
        ops.push(NfsOp::GetFh);
        ops.push(NfsOp::GetAttr(attr_request.bitmap()));
        self.compound_decode("lookup-file-attrs", &ops, |bytes| {
            decode_getfh_attrs_compound(bytes, components.len(), attr_request)
        })
        .await
    }

    pub(crate) async fn read_anonymous(
        &self,
        fh: &FileHandle,
        offset: u64,
        count: u32,
    ) -> Result<ReadData, Error> {
        let ops = [
            NfsOp::PutFh(fh),
            NfsOp::Read {
                stateid: StateId::anonymous(),
                offset,
                count,
            },
        ];
        self.compound_decode("read-anon", &ops, decode_read_compound)
            .await
    }

    pub(crate) async fn read_path_anonymous<S>(
        &self,
        components: &[S],
        offset: u64,
        count: u32,
    ) -> Result<ReadData, Error>
    where
        S: AsRef<str>,
    {
        let mut ops = self.path_ops_with_extra(components, 1);
        ops.push(NfsOp::Read {
            stateid: StateId::anonymous(),
            offset,
            count,
        });
        self.compound_decode("read-path-anon", &ops, |bytes| {
            decode_read_path_compound(bytes, components.len())
        })
        .await
    }

    /// Resolves the file handle and reads `count` bytes at `offset` in one
    /// compound, so a multi-chunk range read needs no separate lookup round
    /// trip before its first chunk.
    pub(crate) async fn read_path_anonymous_with_handle<S>(
        &self,
        components: &[S],
        offset: u64,
        count: u32,
    ) -> Result<(FileHandle, ReadData), Error>
    where
        S: AsRef<str>,
    {
        let mut ops = self.path_ops_with_extra(components, 2);
        ops.push(NfsOp::GetFh);
        ops.push(NfsOp::Read {
            stateid: StateId::anonymous(),
            offset,
            count,
        });
        self.compound_decode("read-path-handle", &ops, |bytes| {
            decode_read_path_handle_compound(bytes, components.len())
        })
        .await
    }

    /// Resolves the file handle and size and reads the first `count` bytes in
    /// one compound, so a `get` needs no separate lookup round trip and later
    /// chunk reads stay on the file resolved together with the size.
    pub(crate) async fn read_path_anonymous_with_size<S>(
        &self,
        components: &[S],
        count: u32,
    ) -> Result<(FileHandle, u64, ReadData), Error>
    where
        S: AsRef<str>,
    {
        let mut ops = self.path_ops_with_extra(components, 3);
        ops.push(NfsOp::GetFh);
        ops.push(NfsOp::GetAttr(Bitmap::size_attr()));
        ops.push(NfsOp::Read {
            stateid: StateId::anonymous(),
            offset: 0,
            count,
        });
        self.compound_decode("read-path-size", &ops, |bytes| {
            decode_read_path_size_compound(bytes, components.len())
        })
        .await
    }

    pub(crate) async fn write(
        &self,
        opened: &OpenedFile,
        offset: u64,
        data: Bytes,
    ) -> Result<WriteData, Error> {
        let ops = [
            NfsOp::PutFh(&opened.fh),
            NfsOp::Write {
                stateid: opened.stateid,
                offset,
                data,
            },
        ];
        self.compound_decode("write", &ops, decode_write_compound)
            .await
    }

    pub(crate) async fn write_and_commit(
        &self,
        opened: &OpenedFile,
        offset: u64,
        data: Bytes,
    ) -> Result<(WriteData, [u8; 8]), Error> {
        let ops = write_and_commit_ops(opened, offset, data);
        self.compound_decode("write-commit", &ops, decode_write_commit_compound)
            .await
    }

    pub(crate) async fn remove<S>(&self, parent: &[S], name: &str) -> Result<RemoveOutcome, Error>
    where
        S: AsRef<str>,
    {
        let mut ops = self.path_ops_with_extra(parent, 1);
        ops.push(NfsOp::Remove(name));
        self.compound_decode("remove", &ops, |bytes| {
            decode_remove_compound(bytes, parent.len())
        })
        .await
    }

    pub(crate) async fn remove_at(
        &self,
        dir: &FileHandle,
        name: &str,
    ) -> Result<RemoveOutcome, Error> {
        let ops = [NfsOp::PutFh(dir), NfsOp::Remove(name)];
        self.compound_decode("remove-at", &ops, |bytes| decode_remove_compound(bytes, 0))
            .await
    }

    /// Returns true until this session sees evidence that the server rejects
    /// the fused put compound shape.
    fn fused_put_supported(&self) -> bool {
        self.inner.fused_put_supported.load(Ordering::Relaxed)
    }

    /// Returns true when a body of `len` bytes could be attempted as a
    /// fused put: it fits the conservative fused-size bound and fused support
    /// has not been latched off. Callers use this to skip fused-only setup
    /// work.
    pub(crate) fn fused_put_viable(&self, len: usize) -> bool {
        fused_put_size_viable(len, self.write_chunk_size()) && self.fused_put_supported()
    }

    /// Latches fused-put support off when a failure indicates the server does
    /// not implement part of the fused shape, rather than a data-path error.
    /// INVAL and BAD_STATEID count as shape rejection only from ops whose
    /// arguments are entirely client-computed; from a name-taking op they
    /// can be about the caller-supplied name instead.
    fn note_fused_put_failure(&self, err: &Error) {
        let unsupported = matches!(err, Error::Unsupported(_))
            || err.nfs_status_code().is_some_and(|code| match code {
                status::NOTSUPP | status::OP_ILLEGAL => true,
                status::INVAL | status::BAD_STATEID => {
                    err.is_nfs_operation(OpCode::Write)
                        || err.is_nfs_operation(OpCode::Commit)
                        || err.is_nfs_operation(OpCode::Verify)
                }
                _ => false,
            });
        if unsupported {
            self.inner
                .fused_put_supported
                .store(false, Ordering::Relaxed);
        }
    }

    /// Writes and atomically publishes a complete file in one compound:
    /// guarded `OPEN`(create) of a temporary name, `WRITE` with the
    /// compound's current stateid, `COMMIT`, a size `VERIFY` that halts the
    /// compound before publish on a short write, then the publish — `RENAME`
    /// onto the destination, or for create-new `LINK` to the destination
    /// (failing with `EXIST` when it is already present) plus `REMOVE` of
    /// the temporary name.
    ///
    /// `OpenOutcome::Exists` reports a temporary-name collision; the caller
    /// retries with a fresh name. The open state is returned for a follow-up
    /// close: some servers (AWS EFS) reject `CLOSE` with the special current
    /// stateid inside the same compound. Returns `Ok(None)` without issuing
    /// an RPC when the body exceeds the conservative fused-size bound, when
    /// this session has latched fused-put support off, or when the compound
    /// would exceed the negotiated request size or operation count.
    pub(crate) async fn try_put_fused<S>(
        &self,
        parent: &[S],
        temp_name: &str,
        dest_name: &str,
        data: Bytes,
        create_new: bool,
    ) -> Result<Option<OpenOutcome<(u64, OpenedFile)>>, Error>
    where
        S: AsRef<str>,
    {
        if !self.fused_put_viable(data.len()) {
            return Ok(None);
        }
        let size = data.len() as u64;
        let count = commit_count(size);
        let owner = self.next_owner();
        let parent_occurrences = if create_new { 2 } else { 1 };
        let mut ops = CompoundOps::with_capacity(10 + parent.len() * parent_occurrences);
        extend_export_path_ops(&mut ops, &self.inner.export_root, parent);
        if !create_new {
            ops.push(NfsOp::SaveFh);
        }
        ops.push(NfsOp::Open {
            seqid: 0,
            clientid: self.inner.clientid,
            owner: owner.as_bytes(),
            name: temp_name,
            file_mode: self.inner.config.file_mode,
        });
        ops.push(NfsOp::GetFh);
        ops.push(NfsOp::Write {
            stateid: StateId::current(),
            offset: 0,
            data: data.clone(),
        });
        ops.push(NfsOp::Commit { offset: 0, count });
        ops.push(NfsOp::Verify { size });
        if create_new {
            ops.push(NfsOp::SaveFh);
            extend_export_path_ops(&mut ops, &self.inner.export_root, parent);
            ops.push(NfsOp::Link(dest_name));
            ops.push(NfsOp::Remove(temp_name));
        } else {
            ops.push(NfsOp::RestoreFh);
            ops.push(NfsOp::Rename {
                old_name: temp_name,
                new_name: dest_name,
            });
        }

        if !self.fused_put_fits(&ops) {
            return Ok(None);
        }

        // Transient statuses are not retried at the compound level: a replay
        // after guarded OPEN created the temporary would misread EXIST as a
        // name collision.
        let mut transient_attempts = 0;
        let outcome = match self
            .compound_decode_detailed(
                "put-fused",
                &ops,
                |bytes| decode_fused_put_compound(bytes, parent.len(), create_new),
                false,
                &mut transient_attempts,
            )
            .await
        {
            Ok(value) => value,
            Err(err) => {
                let err = err.into_error();
                self.note_fused_put_failure(&err);
                return Err(err);
            }
        };
        let (stateid, fh, write) = match outcome {
            OpenOutcome::Opened(parts) => parts,
            OpenOutcome::Exists => return Ok(Some(OpenOutcome::Exists)),
        };
        if u64::from(write.count) != size {
            return Err(Error::protocol(format!(
                "fused put wrote {} of {} bytes but passed size verification",
                write.count, size
            )));
        }
        let opened = OpenedFile {
            fh,
            stateid,
            close_seqid: 1,
        };
        Ok(Some(OpenOutcome::Opened((u64::from(write.count), opened))))
    }

    fn fused_put_fits(&self, ops: &CompoundOps<'_>) -> bool {
        self.compound_request_fits("put-fused", ops)
    }

    async fn resolve_export_root<S>(&self, components: &[S]) -> Result<FileHandle, Error>
    where
        S: AsRef<str>,
    {
        let mut ops = root_path_ops_with_extra(components, 1);
        ops.push(NfsOp::GetFh);
        self.compound_getfh("resolve-export", &ops, OpCode::PutRootFh, components.len())
            .await
    }

    pub(crate) async fn read_dir_page(
        &self,
        dir: &FileHandle,
        cookie: u64,
        verifier: [u8; 8],
        max_entries_hint: Option<usize>,
    ) -> Result<DirPage, Error> {
        let ops = [
            NfsOp::PutFh(dir),
            readdir_op(&self.inner.config, cookie, verifier, max_entries_hint),
        ];
        self.compound_decode("readdir", &ops, |bytes| {
            decode_readdir_compound(bytes, max_entries_hint)
        })
        .await
    }

    pub(crate) async fn read_dir_path_page<S>(
        &self,
        components: &[S],
        cookie: u64,
        verifier: [u8; 8],
        max_entries_hint: Option<usize>,
    ) -> Result<(FileHandle, DirPage), Error>
    where
        S: AsRef<str>,
    {
        let mut ops = self.path_ops_with_extra(components, 2);
        ops.push(NfsOp::GetFh);
        ops.push(readdir_op(
            &self.inner.config,
            cookie,
            verifier,
            max_entries_hint,
        ));
        self.compound_decode("readdir-path", &ops, |bytes| {
            decode_readdir_path_compound(bytes, max_entries_hint, components.len())
        })
        .await
    }

    pub(crate) async fn read_dir_path_page_without_handle<S>(
        &self,
        components: &[S],
        cookie: u64,
        verifier: [u8; 8],
        max_entries_hint: Option<usize>,
    ) -> Result<DirPage, Error>
    where
        S: AsRef<str>,
    {
        let mut ops = self.path_ops_with_extra(components, 1);
        ops.push(readdir_op(
            &self.inner.config,
            cookie,
            verifier,
            max_entries_hint,
        ));
        self.compound_decode("readdir-path", &ops, |bytes| {
            decode_readdir_path_without_handle_compound(bytes, max_entries_hint, components.len())
        })
        .await
    }

    async fn create_dir<S>(&self, parent: &[S], name: &str) -> Result<(), Error>
    where
        S: AsRef<str>,
    {
        let mut ops = self.path_ops_with_extra(parent, 1);
        ops.push(NfsOp::CreateDir {
            name,
            mode: self.inner.config.dir_mode,
        });
        self.compound_create_dir("mkdir", &ops, parent.len()).await
    }

    async fn create_dir_with_handle<S>(
        &self,
        parent: &[S],
        name: &str,
    ) -> Result<CreateDirHandleOutcome, Error>
    where
        S: AsRef<str>,
    {
        let mut ops = self.path_ops_with_extra(parent, 2);
        ops.push(NfsOp::CreateDir {
            name,
            mode: self.inner.config.dir_mode,
        });
        ops.push(NfsOp::GetFh);
        self.compound_create_dir_handle("mkdir-handle", &ops, parent.len())
            .await
    }

    async fn create_dir_at_handle(&self, parent_fh: &FileHandle, name: &str) -> Result<(), Error> {
        let ops = [
            NfsOp::PutFh(parent_fh),
            NfsOp::CreateDir {
                name,
                mode: self.inner.config.dir_mode,
            },
        ];
        self.compound_create_dir("mkdir", &ops, 0).await
    }

    async fn ensure_child_dir_handle(
        &self,
        parent_fh: &FileHandle,
        name: &str,
    ) -> Result<FileHandle, Error> {
        match self.create_child_dir_with_handle(parent_fh, name).await? {
            CreateDirHandleOutcome::Created(fh) => Ok(fh),
            CreateDirHandleOutcome::Exists => self.lookup_child_handle(parent_fh, name).await,
        }
    }

    async fn create_child_dir_with_handle(
        &self,
        parent_fh: &FileHandle,
        name: &str,
    ) -> Result<CreateDirHandleOutcome, Error> {
        let ops = [
            NfsOp::PutFh(parent_fh),
            NfsOp::CreateDir {
                name,
                mode: self.inner.config.dir_mode,
            },
            NfsOp::GetFh,
        ];
        self.compound_create_dir_handle("mkdir-handle", &ops, 0)
            .await
    }

    async fn lookup_child_handle(
        &self,
        parent_fh: &FileHandle,
        name: &str,
    ) -> Result<FileHandle, Error> {
        let ops = [NfsOp::PutFh(parent_fh), NfsOp::Lookup(name), NfsOp::GetFh];
        self.compound_getfh("lookup-child", &ops, OpCode::PutFh, 1)
            .await
    }

    async fn reclaim_complete(&self) -> Result<(), Error> {
        let ops = [NfsOp::ReclaimComplete { one_fs: false }];
        self.compound_decode("reclaim-complete", &ops, |bytes| {
            decode_no_result_compound(bytes, OpCode::ReclaimComplete)
        })
        .await
    }

    async fn compound_getfh(
        &self,
        tag: &str,
        ops: &[NfsOp<'_>],
        start_op: OpCode,
        lookup_count: usize,
    ) -> Result<FileHandle, Error> {
        self.compound_decode(tag, ops, |bytes| {
            decode_getfh_compound(bytes, start_op, lookup_count)
        })
        .await
    }

    async fn compound_create_dir(
        &self,
        tag: &str,
        ops: &[NfsOp<'_>],
        parent_lookup_count: usize,
    ) -> Result<(), Error> {
        let _outcome = self
            .compound_decode(tag, ops, |bytes| {
                decode_create_dir_compound(bytes, parent_lookup_count)
            })
            .await?;
        Ok(())
    }

    async fn compound_create_dir_handle(
        &self,
        tag: &str,
        ops: &[NfsOp<'_>],
        parent_lookup_count: usize,
    ) -> Result<CreateDirHandleOutcome, Error> {
        self.compound_decode(tag, ops, |bytes| {
            decode_create_dir_handle_compound(bytes, parent_lookup_count)
        })
        .await
    }

    async fn compound_decode<T, F>(
        &self,
        tag: &str,
        ops: &[NfsOp<'_>],
        decode: F,
    ) -> Result<T, Error>
    where
        F: FnMut(Bytes) -> Result<T, CompoundDecodeError>,
    {
        let mut attempts = 0;
        self.compound_decode_detailed(tag, ops, decode, true, &mut attempts)
            .await
            .map_err(CompoundDecodeError::into_error)
    }

    /// Issues a compound without transient-status retries, for compounds
    /// that are not safe to replay at this level.
    async fn compound_decode_no_transient_retry<T, F>(
        &self,
        tag: &str,
        ops: &[NfsOp<'_>],
        decode: F,
    ) -> Result<T, Error>
    where
        F: FnMut(Bytes) -> Result<T, CompoundDecodeError>,
    {
        let mut attempts = 0;
        self.compound_decode_detailed(tag, ops, decode, false, &mut attempts)
            .await
            .map_err(CompoundDecodeError::into_error)
    }

    async fn compound_decode_detailed<T, F>(
        &self,
        tag: &str,
        ops: &[NfsOp<'_>],
        mut decode: F,
        retry_transients: bool,
        attempts: &mut u32,
    ) -> Result<T, CompoundDecodeError>
    where
        F: FnMut(Bytes) -> Result<T, CompoundDecodeError>,
    {
        loop {
            let response = self
                .compound_raw_once(tag, ops)
                .await
                .map_err(CompoundDecodeError::from)?;
            match decode(response) {
                Ok(result) => return Ok(result),
                Err(err) => {
                    if retry_transients
                        && transient_compound_retry_is_safe(ops, &err)
                        && self.retry_transient_compound_error(&err, attempts).await
                    {
                        continue;
                    }
                    return Err(err);
                }
            }
        }
    }

    async fn retry_transient_compound_error(
        &self,
        err: &CompoundDecodeError,
        attempts: &mut u32,
    ) -> bool {
        let Some(status) = err.transient_status() else {
            return false;
        };
        let Some(attempt) = claim_transient_retry(attempts, self.inner.config.transient_retries)
        else {
            return false;
        };
        self.sleep_before_transient_retry(status, attempt).await;
        true
    }

    async fn sleep_before_transient_retry(&self, status: NfsStatus, attempts: u32) {
        let delay = transient_retry_delay(self.inner.config.transient_retry_delay, attempts);
        tracing::debug!(
            status_code = status.code(),
            attempt = attempts,
            delay_ms = delay.as_millis() as u64,
            "retrying transient NFS status"
        );
        tokio::time::sleep(delay).await;
    }

    async fn compound_raw_once(&self, tag: &str, ops: &[NfsOp<'_>]) -> Result<Bytes, Error> {
        self.validate_compound_request(tag, ops)
            .map_err(Error::request_not_sent)?;
        self.ensure_sequence_status_usable()
            .map_err(Error::request_not_sent)?;
        let (mut slot, highest_slotid) = self
            .inner
            .slots
            .acquire_for_request()
            .await
            .map_err(Error::request_not_sent)?;
        let sequenceid = slot.sequenceid;
        let slotid = slot.id;
        self.ensure_sequence_status_usable()
            .map_err(Error::request_not_sent)?;
        let payload = encode_compound_with_sequence(
            tag,
            SequenceArgs {
                sessionid: self.inner.sessionid,
                sequenceid,
                slotid,
                highest_slotid,
                cachethis: false,
            },
            ops,
        );
        let span = tracing::debug_span!(
            "nfs_compound",
            tag = %tag,
            slot_id = slotid,
            sequence_id = sequenceid,
            op_count = ops.len(),
            request_len = payload.len(),
            response_len = tracing::field::Empty,
            sequence_advanced = false,
        );
        // From this point until a matching successful SEQUENCE result is
        // decoded, cancellation or any transport/decode error leaves the
        // server-side state of this slot unknown. The guard retires it on
        // drop so another operation can never reuse an uncertain sequence.
        slot.mark_request_outstanding();
        let response = match self
            .inner
            .rpc
            .call_with_timeout(
                NFS_PROGRAM,
                NFS_VERSION,
                NFSPROC4_COMPOUND,
                payload,
                self.inner.config.operation_timeout,
            )
            .instrument(span.clone())
            .await
        {
            Ok(response) => response,
            Err(err) => {
                if err.is_request_not_sent() {
                    slot.disarm_request();
                }
                tracing::debug!(parent: &span, error = %err, "compound request failed");
                return Err(err);
            }
        };
        span.record("response_len", response.len());
        let sequence = decode_sequence_result(&response, self.inner.sessionid, sequenceid, slotid)
            .inspect_err(|err| {
                tracing::debug!(parent: &span, error = %err, "compound sequence validation failed");
            })?;
        self.inner
            .slots
            .update_limits(sequence.highest_slotid, sequence.target_highest_slotid);
        slot.advance_sequence();
        slot.disarm_request();
        span.record("sequence_advanced", true);
        if let Err(err) = validate_sequence_status_flags(
            sequence.status_flags,
            self.inner.reclaim_complete.load(Ordering::Acquire),
        ) {
            let rotated_client_verifier = latch_fatal_sequence_flags(
                &self.inner.fatal_sequence_flags,
                &self.inner.client_owner,
                self.inner.client_verifier,
                sequence.status_flags,
            );
            tracing::warn!(
                parent: &span,
                status_flags = format_args!("0x{:08x}", sequence.status_flags),
                rotated_client_verifier,
                error = %err,
                "NFS session requires recovery after this compound"
            );
        }
        Ok(response)
    }

    fn ensure_sequence_status_usable(&self) -> Result<(), Error> {
        let status_flags = self.inner.fatal_sequence_flags.load(Ordering::Acquire);
        validate_sequence_status_flags(
            status_flags,
            self.inner.reclaim_complete.load(Ordering::Acquire),
        )
    }

    fn validate_compound_request(&self, tag: &str, ops: &[NfsOp<'_>]) -> Result<(), Error> {
        validate_compound_request_limits(
            tag,
            ops,
            self.inner.config.max_compound_ops,
            self.inner.config.max_request_size,
            self.inner
                .rpc
                .encoded_call_len(compound_with_sequence_encoded_len(tag, ops)),
        )
    }

    fn compound_request_fits(&self, tag: &str, ops: &[NfsOp<'_>]) -> bool {
        self.validate_compound_request(tag, ops).is_ok()
    }

    fn path_ops_with_extra<'a, S>(
        &'a self,
        components: &'a [S],
        extra_ops: usize,
    ) -> CompoundOps<'a>
    where
        S: AsRef<str> + 'a,
    {
        export_path_ops_with_extra(&self.inner.export_root, components, extra_ops)
    }

    fn next_owner(&self) -> String {
        self.inner.client_owner.next_open_owner()
    }
}

async fn run_deferred_close_worker(
    session: std::sync::Weak<NfsSessionInner>,
    mut deferred_closes: mpsc::Receiver<OpenedFile>,
) {
    let mut batch = Vec::with_capacity(MAX_DEFERRED_CLOSE_BATCH);
    while let Some(opened) = deferred_closes.recv().await {
        batch.push(opened);

        // Give concurrently completing puts one scheduler turn to enqueue
        // their state before draining, improving compound density without
        // delaying the foreground operations.
        tokio::task::yield_now().await;

        while batch.len() < MAX_DEFERRED_CLOSE_BATCH {
            match deferred_closes.try_recv() {
                Ok(opened) => batch.push(opened),
                Err(_) => break,
            }
        }

        if !batch.is_empty() {
            let Some(inner) = session.upgrade() else {
                break;
            };
            let nfs = NfsSession { inner };
            if let Err(batch_error) = nfs.close_batch(&batch).await {
                tracing::warn!(
                    close_count = batch.len(),
                    error = %batch_error,
                    "batched cleanup of published NFS open states failed"
                );
                for opened in &mut batch {
                    if let Err(close_error) = nfs.close_file(opened).await {
                        tracing::warn!(
                            error = %close_error,
                            "individual cleanup of published NFS open state failed"
                        );
                    }
                }
            }
            batch.clear();
        }
    }
}

pub(super) fn claim_transient_retry(attempts: &mut u32, retry_limit: u32) -> Option<u32> {
    if *attempts >= retry_limit {
        return None;
    }
    *attempts += 1;
    Some(*attempts)
}

pub(super) fn transient_retry_delay(base: Duration, attempt: u32) -> Duration {
    base.checked_mul(attempt).unwrap_or(Duration::MAX)
}
