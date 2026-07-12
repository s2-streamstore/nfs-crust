#![allow(dead_code)]

use std::collections::HashSet;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt, copy_bidirectional};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, Notify};
use tokio::task::JoinHandle;

/// Listener, accept-loop, and bridge-task bookkeeping shared by the proxies.
struct ProxyCore {
    task: JoinHandle<()>,
    bridges: Arc<Mutex<Vec<JoinHandle<()>>>>,
}

type BridgeHandler =
    dyn Fn(TcpStream, TcpStream) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync;

impl ProxyCore {
    async fn start(target: String, handler: Arc<BridgeHandler>) -> io::Result<(String, Self)> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?.to_string();
        let bridges = Arc::new(Mutex::new(Vec::new()));
        let task_bridges = Arc::clone(&bridges);
        let task = tokio::spawn(async move {
            while let Ok((downstream, _)) = listener.accept().await {
                let target = target.clone();
                let handler = Arc::clone(&handler);
                let bridge = tokio::spawn(async move {
                    let Ok(upstream) = TcpStream::connect(target).await else {
                        return;
                    };
                    handler(downstream, upstream).await;
                });
                task_bridges.lock().await.push(bridge);
            }
        });

        Ok((addr, Self { task, bridges }))
    }

    async fn drop_connections(&self) {
        let mut bridges = self.bridges.lock().await;
        for bridge in bridges.drain(..) {
            bridge.abort();
        }
    }
}

impl Drop for ProxyCore {
    fn drop(&mut self) {
        self.task.abort();
        if let Ok(mut bridges) = self.bridges.try_lock() {
            for bridge in bridges.drain(..) {
                bridge.abort();
            }
        }
    }
}

pub(crate) struct TcpProxy {
    pub(crate) addr: String,
    core: ProxyCore,
}

impl TcpProxy {
    pub(crate) async fn start(target: String) -> io::Result<Self> {
        let (addr, core) = ProxyCore::start(
            target,
            Arc::new(|mut downstream, mut upstream| {
                Box::pin(async move {
                    let _ = copy_bidirectional(&mut downstream, &mut upstream).await;
                })
            }),
        )
        .await?;
        Ok(Self { addr, core })
    }

    pub(crate) async fn drop_connections(&self) {
        self.core.drop_connections().await;
    }
}

const RPC_LAST_FRAGMENT: u32 = 0x8000_0000;
const RPC_FRAGMENT_SIZE_MASK: u32 = 0x7fff_ffff;
const RPC_CALL: u32 = 0;
const RPC_REPLY: u32 = 1;
const RPC_VERSION: u32 = 2;
const RPC_MSG_ACCEPTED: u32 = 0;
const RPC_ACCEPT_SUCCESS: u32 = 0;
const NFS_PROGRAM: u32 = 100003;
const NFS_VERSION: u32 = 4;
const NFS_COMPOUND_PROCEDURE: u32 = 1;

pub(crate) struct ObservedProxy {
    pub(crate) addr: String,
    core: ProxyCore,
    observer: Arc<PipelineObserver>,
}

impl ObservedProxy {
    pub(crate) async fn start(target: String, watched_tag: &'static str) -> Self {
        let observer = Arc::new(PipelineObserver::new(watched_tag));
        let bridge_observer = Arc::clone(&observer);
        let (addr, core) = ProxyCore::start(
            target,
            Arc::new(move |downstream, upstream| {
                let observer = Arc::clone(&bridge_observer);
                Box::pin(bridge_observed_connection(downstream, upstream, observer))
            }),
        )
        .await
        .unwrap();

        Self {
            addr,
            core,
            observer,
        }
    }

    pub(crate) async fn reset(&self) {
        self.observer.reset().await;
    }

    pub(crate) fn hold_watched_responses_until(&self, target_in_flight: usize) {
        self.observer.hold_watched_responses_until(target_in_flight);
    }

    pub(crate) fn release_response_hold(&self) {
        self.observer.release_response_hold();
    }

    pub(crate) fn drop_watched_responses(&self) {
        self.observer
            .drop_watched_responses
            .store(true, Ordering::Release);
    }

    pub(crate) async fn wait_for_dropped_successful_response(&self, timeout: Duration) -> bool {
        self.observer
            .wait_for_dropped_successful_response(timeout)
            .await
    }

    pub(crate) async fn wait_for_max_in_flight(&self, target: usize, timeout: Duration) -> bool {
        self.observer.wait_for_max_in_flight(target, timeout).await
    }

    pub(crate) fn max_in_flight(&self) -> usize {
        self.observer.max_in_flight()
    }

    pub(crate) fn observed_count(&self) -> usize {
        self.observer.observed_count()
    }
}

#[derive(Debug)]
struct PipelineObserver {
    watched_tag: &'static str,
    watched_xids: Mutex<HashSet<u32>>,
    observed_count: AtomicUsize,
    in_flight: AtomicUsize,
    max_in_flight: AtomicUsize,
    hold_until: AtomicUsize,
    hold_released: AtomicBool,
    drop_watched_responses: AtomicBool,
    dropped_watched_responses: AtomicUsize,
    dropped_successful_responses: AtomicUsize,
    notify: Notify,
}

impl PipelineObserver {
    fn new(watched_tag: &'static str) -> Self {
        Self {
            watched_tag,
            watched_xids: Mutex::new(HashSet::new()),
            observed_count: AtomicUsize::new(0),
            in_flight: AtomicUsize::new(0),
            max_in_flight: AtomicUsize::new(0),
            hold_until: AtomicUsize::new(0),
            hold_released: AtomicBool::new(true),
            drop_watched_responses: AtomicBool::new(false),
            dropped_watched_responses: AtomicUsize::new(0),
            dropped_successful_responses: AtomicUsize::new(0),
            notify: Notify::new(),
        }
    }

    async fn reset(&self) {
        self.watched_xids.lock().await.clear();
        self.observed_count.store(0, Ordering::Release);
        self.in_flight.store(0, Ordering::Release);
        self.max_in_flight.store(0, Ordering::Release);
        self.hold_until.store(0, Ordering::Release);
        self.hold_released.store(true, Ordering::Release);
        self.drop_watched_responses.store(false, Ordering::Release);
        self.dropped_watched_responses.store(0, Ordering::Release);
        self.dropped_successful_responses
            .store(0, Ordering::Release);
        self.notify.notify_waiters();
    }

    fn hold_watched_responses_until(&self, target_in_flight: usize) {
        self.hold_released.store(false, Ordering::Release);
        self.hold_until.store(target_in_flight, Ordering::Release);
        self.notify.notify_waiters();
    }

    fn release_response_hold(&self) {
        self.hold_until.store(0, Ordering::Release);
        self.hold_released.store(true, Ordering::Release);
        self.notify.notify_waiters();
    }

    async fn record_client_request(&self, body: &[u8]) {
        if rpc_compound_tag(body).as_deref() != Some(self.watched_tag) {
            return;
        }
        let Some(xid) = rpc_xid(body) else {
            return;
        };
        self.observed_count.fetch_add(1, Ordering::AcqRel);
        self.watched_xids.lock().await.insert(xid);
        let in_flight = self.in_flight.fetch_add(1, Ordering::AcqRel) + 1;
        self.update_max_in_flight(in_flight);
        self.notify.notify_waiters();
    }

    async fn before_server_response_forwarded(&self, body: &[u8]) -> bool {
        let Some(xid) = rpc_xid(body) else {
            return false;
        };
        let watched = self.watched_xids.lock().await.remove(&xid);
        if !watched {
            return false;
        }

        self.wait_if_response_held().await;
        self.in_flight
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                Some(current.saturating_sub(1))
            })
            .ok();
        let drop_response = self.drop_watched_responses.load(Ordering::Acquire);
        if drop_response {
            self.dropped_watched_responses
                .fetch_add(1, Ordering::AcqRel);
            if rpc_nfs_compound_status(body) == Some(0) {
                self.dropped_successful_responses
                    .fetch_add(1, Ordering::AcqRel);
            }
        }
        self.notify.notify_waiters();
        drop_response
    }

    async fn wait_if_response_held(&self) {
        loop {
            let target = self.hold_until.load(Ordering::Acquire);
            if target == 0
                || self.hold_released.load(Ordering::Acquire)
                || self.max_in_flight.load(Ordering::Acquire) >= target
            {
                return;
            }
            self.notify.notified().await;
        }
    }

    async fn wait_for_max_in_flight(&self, target: usize, timeout: Duration) -> bool {
        tokio::time::timeout(timeout, async {
            loop {
                if self.max_in_flight.load(Ordering::Acquire) >= target {
                    return;
                }
                self.notify.notified().await;
            }
        })
        .await
        .is_ok()
    }

    async fn wait_for_dropped_successful_response(&self, timeout: Duration) -> bool {
        tokio::time::timeout(timeout, async {
            loop {
                let notified = self.notify.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if self.dropped_successful_responses.load(Ordering::Acquire) > 0 {
                    return;
                }
                notified.await;
            }
        })
        .await
        .is_ok()
    }

    fn max_in_flight(&self) -> usize {
        self.max_in_flight.load(Ordering::Acquire)
    }

    fn observed_count(&self) -> usize {
        self.observed_count.load(Ordering::Acquire)
    }

    fn update_max_in_flight(&self, candidate: usize) {
        let mut current = self.max_in_flight.load(Ordering::Acquire);
        while candidate > current {
            match self.max_in_flight.compare_exchange_weak(
                current,
                candidate,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(observed) => current = observed,
            }
        }
    }
}

async fn bridge_observed_connection(
    downstream: TcpStream,
    upstream: TcpStream,
    observer: Arc<PipelineObserver>,
) {
    let (mut downstream_read, mut downstream_write) = downstream.into_split();
    let (mut upstream_read, mut upstream_write) = upstream.into_split();
    let request_observer = Arc::clone(&observer);
    let client_to_server = tokio::spawn(async move {
        while let Ok(record) = read_rpc_record(&mut downstream_read).await {
            request_observer.record_client_request(&record.body).await;
            if upstream_write.write_all(&record.frame).await.is_err() {
                break;
            }
        }
    });
    let server_to_client = tokio::spawn(async move {
        while let Ok(record) = read_rpc_record(&mut upstream_read).await {
            if observer
                .before_server_response_forwarded(&record.body)
                .await
            {
                break;
            }
            if downstream_write.write_all(&record.frame).await.is_err() {
                break;
            }
        }
    });

    let _ = tokio::join!(client_to_server, server_to_client);
}

#[derive(Debug)]
struct RpcRecord {
    frame: Bytes,
    body: Bytes,
}

async fn read_rpc_record<R>(reader: &mut R) -> io::Result<RpcRecord>
where
    R: AsyncRead + Unpin,
{
    let mut frame = BytesMut::new();
    let mut body = BytesMut::new();
    loop {
        let marker = reader.read_u32().await?;
        let size = (marker & RPC_FRAGMENT_SIZE_MASK) as usize;
        frame.extend_from_slice(&marker.to_be_bytes());
        let frame_start = frame.len();
        let body_start = body.len();
        frame.resize(frame_start + size, 0);
        body.resize(body_start + size, 0);
        reader.read_exact(&mut frame[frame_start..]).await?;
        body[body_start..].copy_from_slice(&frame[frame_start..]);
        if marker & RPC_LAST_FRAGMENT != 0 {
            return Ok(RpcRecord {
                frame: frame.freeze(),
                body: body.freeze(),
            });
        }
    }
}

fn rpc_xid(body: &[u8]) -> Option<u32> {
    read_be_u32(body, 0)
}

fn rpc_nfs_compound_status(body: &[u8]) -> Option<u32> {
    let mut cursor = 0usize;
    let _xid = read_u32(body, &mut cursor)?;
    let message_type = read_u32(body, &mut cursor)?;
    let reply_status = read_u32(body, &mut cursor)?;
    if message_type != RPC_REPLY || reply_status != RPC_MSG_ACCEPTED {
        return None;
    }
    let _verifier_flavor = read_u32(body, &mut cursor)?;
    skip_xdr_opaque(body, &mut cursor)?;
    let accept_status = read_u32(body, &mut cursor)?;
    if accept_status != RPC_ACCEPT_SUCCESS {
        return None;
    }
    read_u32(body, &mut cursor)
}

fn rpc_compound_tag(body: &[u8]) -> Option<String> {
    let mut cursor = 0usize;
    let _xid = read_u32(body, &mut cursor)?;
    let message_type = read_u32(body, &mut cursor)?;
    let rpc_version = read_u32(body, &mut cursor)?;
    let program = read_u32(body, &mut cursor)?;
    let version = read_u32(body, &mut cursor)?;
    let procedure = read_u32(body, &mut cursor)?;
    if message_type != RPC_CALL
        || rpc_version != RPC_VERSION
        || program != NFS_PROGRAM
        || version != NFS_VERSION
        || procedure != NFS_COMPOUND_PROCEDURE
    {
        return None;
    }

    let _cred_flavor = read_u32(body, &mut cursor)?;
    skip_xdr_opaque(body, &mut cursor)?;
    let _verifier_flavor = read_u32(body, &mut cursor)?;
    skip_xdr_opaque(body, &mut cursor)?;
    read_xdr_string(body, &mut cursor)
}

fn read_u32(body: &[u8], cursor: &mut usize) -> Option<u32> {
    let value = read_be_u32(body, *cursor)?;
    *cursor = cursor.checked_add(4)?;
    Some(value)
}

fn read_be_u32(body: &[u8], offset: usize) -> Option<u32> {
    let bytes = body.get(offset..offset.checked_add(4)?)?;
    Some(u32::from_be_bytes(bytes.try_into().ok()?))
}

fn skip_xdr_opaque(body: &[u8], cursor: &mut usize) -> Option<()> {
    let len = read_u32(body, cursor)? as usize;
    let padded_len = xdr_padded_len(len)?;
    let end = cursor.checked_add(padded_len)?;
    body.get(*cursor..end)?;
    *cursor = end;
    Some(())
}

fn read_xdr_string(body: &[u8], cursor: &mut usize) -> Option<String> {
    let len = read_u32(body, cursor)? as usize;
    let start = *cursor;
    let end = start.checked_add(len)?;
    let padded_end = start.checked_add(xdr_padded_len(len)?)?;
    let bytes = body.get(start..end)?;
    body.get(end..padded_end)?;
    *cursor = padded_end;
    std::str::from_utf8(bytes).ok().map(str::to_owned)
}

fn xdr_padded_len(len: usize) -> Option<usize> {
    len.checked_add((4 - (len % 4)) % 4)
}
