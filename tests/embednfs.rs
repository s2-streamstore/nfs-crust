use std::time::Duration;

use bytes::Bytes;
use embednfs::{
    CreateKind, CreateRequest, FileSystem, MemFs, NfsServer, RequestContext, SetAttrs,
    WriteStability,
};
use nfs_crust::{EntryKind, Error, NfsClient, PutMode};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use ulid::Ulid;

mod support;
use support::{ObservedProxy, TcpProxy};

struct TestServer {
    addr: String,
    task: JoinHandle<()>,
}

impl TestServer {
    async fn start() -> Self {
        Self::start_with_files(&[]).await
    }

    async fn start_with_files(files: &[(&str, &str, Bytes)]) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let fs = MemFs::new();
        let ctx = RequestContext::anonymous();
        for (directory, name, body) in files {
            let parent = match fs.lookup(&ctx, &fs.root(), directory).await {
                Ok(handle) => handle,
                Err(_) => {
                    fs.create(
                        &ctx,
                        &fs.root(),
                        directory,
                        CreateRequest {
                            kind: CreateKind::Directory,
                            attrs: SetAttrs::default(),
                        },
                    )
                    .await
                    .unwrap()
                    .handle
                }
            };
            let file = fs
                .create(
                    &ctx,
                    &parent,
                    name,
                    CreateRequest {
                        kind: CreateKind::File,
                        attrs: SetAttrs::default(),
                    },
                )
                .await
                .unwrap();
            fs.write(
                &ctx,
                &file.handle,
                0,
                body.clone(),
                WriteStability::FileSync,
            )
            .await
            .unwrap();
        }
        let server = NfsServer::new(fs);
        let task = tokio::spawn(async move {
            let _ = server.serve(listener).await;
        });
        Self { addr, task }
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn client_builder(addr: &str) -> nfs_crust::NfsClientBuilder {
    NfsClient::builder(addr, "/")
}

async fn connect_client(addr: &str) -> NfsClient {
    client_builder(addr).connect().await.unwrap()
}

#[tokio::test]
async fn operation_api_round_trips_against_embedded_nfsv41_server() {
    let server = TestServer::start().await;
    let client = client_builder(server.addr.as_str())
        .session_slots(4)
        .connect()
        .await
        .unwrap();

    client
        .put(
            "alpha/bravo.txt",
            Bytes::from_static(b"0123456789"),
            PutMode::Overwrite,
        )
        .await
        .unwrap();

    assert_eq!(
        &client.get("alpha/bravo.txt").await.unwrap()[..],
        b"0123456789"
    );
    assert_eq!(
        &client.get_range("alpha/bravo.txt", 3..7).await.unwrap()[..],
        b"3456"
    );

    let listed = client.list_page("alpha/", 100, None).await.unwrap();
    assert_eq!(listed.entries.len(), 1);
    assert_eq!(listed.entries[0].path, "alpha/bravo.txt");
    assert_eq!(listed.entries[0].name, "bravo.txt");
    let info = client.entry_info("alpha/bravo.txt").await.unwrap();
    assert_eq!(info.kind, EntryKind::File);
    assert_eq!(info.size, 10);

    client.delete("alpha/bravo.txt").await.unwrap();
    client.delete("alpha/bravo.txt").await.unwrap();
    assert!(
        client
            .list_page("alpha/", 100, None)
            .await
            .unwrap()
            .entries
            .is_empty()
    );
    client.delete("alpha").await.unwrap();
    assert!(
        client
            .list_page("/", 100, None)
            .await
            .unwrap()
            .entries
            .iter()
            .all(|entry| entry.name != "alpha")
    );
}

#[tokio::test]
async fn put_overwrites_atomically_and_does_not_leave_temp_files() {
    let server = TestServer::start().await;
    let client = client_builder(server.addr.as_str())
        .session_slots(4)
        .connect()
        .await
        .unwrap();

    client
        .put(
            "atomic/item.bin",
            Bytes::from_static(b"old"),
            PutMode::Overwrite,
        )
        .await
        .unwrap();
    client
        .put(
            "atomic/item.bin",
            Bytes::from_static(b"new-value"),
            PutMode::Overwrite,
        )
        .await
        .unwrap();

    assert_eq!(
        &client.get("atomic/item.bin").await.unwrap()[..],
        b"new-value"
    );
    let listed = client.list_page("atomic/", 100, None).await.unwrap();
    assert_eq!(listed.entries.len(), 1);
    assert_eq!(listed.entries[0].path, "atomic/item.bin");
}

#[tokio::test]
async fn repeated_mutations_resolve_parents_inside_mutation_compounds() {
    let server = TestServer::start().await;
    let bootstrap = connect_client(server.addr.as_str()).await;
    bootstrap
        .put(
            "cached-parent/seed.bin",
            Bytes::from_static(b"seed"),
            PutMode::Overwrite,
        )
        .await
        .unwrap();
    let proxy = ObservedProxy::start(server.addr.clone(), "lookup-file").await;
    let client = connect_client(proxy.addr.as_str()).await;
    proxy.reset().await;

    for (name, body) in [("one.bin", b"one" as &[u8]), ("two.bin", b"two")] {
        client
            .put(
                format!("cached-parent/{name}"),
                Bytes::copy_from_slice(body),
                PutMode::Overwrite,
            )
            .await
            .unwrap();
    }
    assert_eq!(
        proxy.observed_count(),
        0,
        "parent traversal should not require a separate lookup compound"
    );
}

#[tokio::test]
async fn put_if_not_exists_fails_clearly_when_destination_exists() {
    let server = TestServer::start().await;
    let client = client_builder(server.addr.as_str())
        .session_slots(4)
        .connect()
        .await
        .unwrap();

    let body = Bytes::from_static(b"first");
    client
        .put("create-new/item.bin", body.clone(), PutMode::IfNotExists)
        .await
        .unwrap();

    let err = client
        .put(
            "create-new/item.bin",
            Bytes::from_static(b"second"),
            PutMode::IfNotExists,
        )
        .await
        .unwrap_err();
    assert!(matches!(err, Error::AlreadyExists(_)));
    assert_eq!(client.get("create-new/item.bin").await.unwrap(), body);

    client
        .put("create-new/plug", Bytes::new(), PutMode::IfNotExists)
        .await
        .unwrap();
    assert_eq!(client.get("create-new/plug").await.unwrap(), Bytes::new());

    let listed = client.list_page("create-new/", 100, None).await.unwrap();
    assert_eq!(listed.entries.len(), 2);
}

#[tokio::test]
async fn put_creates_deep_parent_directories() {
    let server = TestServer::start().await;
    let client = client_builder(server.addr.as_str())
        .session_slots(4)
        .connect()
        .await
        .unwrap();

    client
        .put(
            "deep/a/b/c/item.bin",
            Bytes::from_static(b"nested"),
            PutMode::Overwrite,
        )
        .await
        .unwrap();

    assert_eq!(
        client.get("deep/a/b/c/item.bin").await.unwrap(),
        Bytes::from_static(b"nested")
    );
    let listed = client.list_page("deep/a/b/c", 100, None).await.unwrap();
    assert_eq!(listed.entries.len(), 1);
    assert_eq!(listed.entries[0].path, "deep/a/b/c/item.bin");
}

#[tokio::test]
async fn small_put_publishes_in_a_single_fused_compound() {
    let server = TestServer::start().await;
    let proxy = ObservedProxy::start(server.addr.clone(), "put-fused").await;
    let client = connect_client(proxy.addr.as_str()).await;

    client
        .put(
            "fused/seed.bin",
            Bytes::from_static(b"seed"),
            PutMode::Overwrite,
        )
        .await
        .unwrap();
    proxy.reset().await;

    client
        .put(
            "fused/one.bin",
            Bytes::from_static(b"hello"),
            PutMode::Overwrite,
        )
        .await
        .unwrap();
    assert_eq!(proxy.observed_count(), 1);

    client
        .put(
            "fused/two.bin",
            Bytes::from_static(b"world"),
            PutMode::IfNotExists,
        )
        .await
        .unwrap();
    assert_eq!(proxy.observed_count(), 2);

    let err = client
        .put(
            "fused/two.bin",
            Bytes::from_static(b"again"),
            PutMode::IfNotExists,
        )
        .await
        .unwrap_err();
    assert!(matches!(err, Error::AlreadyExists(_)));

    assert_eq!(
        client.get("fused/one.bin").await.unwrap(),
        Bytes::from_static(b"hello")
    );
    assert_eq!(
        client.get("fused/two.bin").await.unwrap(),
        Bytes::from_static(b"world")
    );
    let listed = client.list_page("fused/", 100, None).await.unwrap();
    assert_eq!(listed.entries.len(), 3);
}

#[tokio::test]
async fn small_put_returns_before_deferred_close_completes() {
    let server = TestServer::start().await;
    let proxy = ObservedProxy::start(server.addr.clone(), "close-batch").await;
    let client = connect_client(proxy.addr.as_str()).await;

    client
        .put(
            "fused-close/seed.bin",
            Bytes::from_static(b"seed"),
            PutMode::Overwrite,
        )
        .await
        .unwrap();
    proxy.reset().await;
    proxy.hold_watched_responses_until(2);

    let put_client = client.clone();
    let put = tokio::spawn(async move {
        put_client
            .put(
                "fused-close/data.bin",
                Bytes::from_static(b"payload"),
                PutMode::Overwrite,
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), put)
        .await
        .expect("put waited for deferred CLOSE")
        .unwrap()
        .unwrap();
    assert!(
        proxy
            .wait_for_max_in_flight(1, Duration::from_secs(2))
            .await
    );

    proxy.release_response_hold();
    assert_eq!(proxy.observed_count(), 1);
}

#[tokio::test]
async fn deferred_closes_are_batched_after_the_worker_falls_behind() {
    let server = TestServer::start().await;
    let proxy = ObservedProxy::start(server.addr.clone(), "close-batch").await;
    let client = connect_client(proxy.addr.as_str()).await;

    client
        .put(
            "close-batch/seed.bin",
            Bytes::from_static(b"seed"),
            PutMode::Overwrite,
        )
        .await
        .unwrap();
    proxy.reset().await;

    let put_count = 16;
    let mut puts = Vec::with_capacity(put_count);
    for index in 0..put_count {
        let client = client.clone();
        puts.push(tokio::spawn(async move {
            client
                .put(
                    format!("close-batch/{index}.bin"),
                    Bytes::from_static(b"data"),
                    PutMode::Overwrite,
                )
                .await
        }));
    }
    for put in puts {
        put.await.unwrap().unwrap();
    }
    let close_count = tokio::time::timeout(Duration::from_secs(2), async {
        let mut previous = 0;
        loop {
            tokio::time::sleep(Duration::from_millis(25)).await;
            let current = proxy.observed_count();
            if current > 0 && current == previous {
                break current;
            }
            previous = current;
        }
    })
    .await
    .expect("background CLOSE processing did not settle");
    assert!(
        close_count < put_count,
        "at least two queued states should share one CLOSE compound"
    );
}

#[tokio::test]
async fn published_fused_put_survives_indeterminate_close_cleanup() {
    let server = TestServer::start().await;
    let proxy = ObservedProxy::start(server.addr.clone(), "close-batch").await;
    let client = client_builder(proxy.addr.as_str())
        .operation_timeout(Some(Duration::from_millis(250)))
        .connect()
        .await
        .unwrap();

    client
        .put(
            "fused-close-recovery/seed.bin",
            Bytes::from_static(b"seed"),
            PutMode::Overwrite,
        )
        .await
        .unwrap();
    proxy.reset().await;
    proxy.drop_watched_responses();

    tokio::time::timeout(
        Duration::from_millis(200),
        client.put(
            "fused-close-recovery/data.bin",
            Bytes::from_static(b"payload"),
            PutMode::Overwrite,
        ),
    )
    .await
    .expect("put waited for indeterminate deferred CLOSE cleanup")
    .unwrap();
    assert!(
        proxy
            .wait_for_dropped_successful_response(Duration::from_secs(2))
            .await
    );

    let verifier = connect_client(server.addr.as_str()).await;
    assert_eq!(
        verifier.get("fused-close-recovery/data.bin").await.unwrap(),
        Bytes::from_static(b"payload")
    );
}

#[tokio::test]
async fn small_put_does_not_use_the_multi_compound_path() {
    let server = TestServer::start().await;
    let proxy = ObservedProxy::start(server.addr.clone(), "open").await;
    let client = connect_client(proxy.addr.as_str()).await;

    client
        .put(
            "fused-only/seed.bin",
            Bytes::from_static(b"seed"),
            PutMode::Overwrite,
        )
        .await
        .unwrap();
    proxy.reset().await;

    client
        .put(
            "fused-only/data.bin",
            Bytes::from_static(b"payload"),
            PutMode::Overwrite,
        )
        .await
        .unwrap();

    assert_eq!(proxy.observed_count(), 0);
}

#[tokio::test]
async fn medium_put_uses_the_verified_multi_compound_path() {
    let server = TestServer::start().await;
    let proxy = ObservedProxy::start(server.addr.clone(), "close-rename").await;
    let client = connect_client(proxy.addr.as_str()).await;
    let body = Bytes::from(vec![0x5a; 512 * 1024]);

    client
        .put("medium/data.bin", body.clone(), PutMode::Overwrite)
        .await
        .unwrap();

    assert_eq!(proxy.observed_count(), 1);
    assert_eq!(client.get("medium/data.bin").await.unwrap(), body);
}

#[tokio::test]
async fn large_put_publishes_with_close_rename_compound() {
    let server = TestServer::start().await;
    let proxy = ObservedProxy::start(server.addr.clone(), "close-rename").await;
    let client = client_builder(proxy.addr.as_str())
        .write_chunk_size(4)
        .connect()
        .await
        .unwrap();

    let body = Bytes::from_static(b"0123456789abcdef");
    client
        .put("large/chunk", body.clone(), PutMode::Overwrite)
        .await
        .unwrap();
    assert_eq!(proxy.observed_count(), 1);
    assert_eq!(client.get("large/chunk").await.unwrap(), body);

    let listed = client.list_page("large/", 100, None).await.unwrap();
    assert_eq!(listed.entries.len(), 1);
}

#[tokio::test]
async fn large_put_if_not_exists_publishes_with_link_and_rejects_existing() {
    let server = TestServer::start().await;
    let proxy = ObservedProxy::start(server.addr.clone(), "close-link-remove").await;
    let client = client_builder(proxy.addr.as_str())
        .write_chunk_size(4)
        .connect()
        .await
        .unwrap();

    let body = Bytes::from_static(b"0123456789abcdef");
    client
        .put("large-new/chunk", body.clone(), PutMode::IfNotExists)
        .await
        .unwrap();
    assert_eq!(proxy.observed_count(), 1);
    assert_eq!(client.get("large-new/chunk").await.unwrap(), body);

    let err = client
        .put("large-new/chunk", body.clone(), PutMode::IfNotExists)
        .await
        .unwrap_err();
    assert!(matches!(err, Error::AlreadyExists(_)));
    assert_eq!(client.get("large-new/chunk").await.unwrap(), body);

    let listed = client.list_page("large-new/", 100, None).await.unwrap();
    assert_eq!(listed.entries.len(), 1);
}

#[tokio::test]
async fn put_splits_single_bytes_value_into_configured_write_chunks() {
    let server = TestServer::start().await;
    let client = client_builder(server.addr.as_str())
        .write_chunk_size(3)
        .connect()
        .await
        .unwrap();

    let body = Bytes::from_static(b"0123456789");
    client
        .put("chunked-write/item.bin", body.clone(), PutMode::Overwrite)
        .await
        .unwrap();
    assert_eq!(client.get("chunked-write/item.bin").await.unwrap(), body);
}

#[tokio::test]
async fn sweep_temp_files_removes_only_stale_temps() {
    let stale_name = format!(".nfs-crust-tmp-{}", Ulid::from_parts(1, 1));
    let fresh_name = format!(".nfs-crust-tmp-{}", Ulid::new());
    let server = TestServer::start_with_files(&[
        ("sweep", &stale_name, Bytes::from_static(b"body")),
        ("sweep", &fresh_name, Bytes::from_static(b"body")),
    ])
    .await;
    let client = connect_client(server.addr.as_str()).await;
    client
        .put(
            "sweep/data.bin",
            Bytes::from_static(b"body"),
            PutMode::Overwrite,
        )
        .await
        .unwrap();

    let removed = client
        .sweep_temp_files("sweep/", Duration::from_secs(3600))
        .await
        .unwrap();
    assert_eq!(removed, 1);

    let names: Vec<_> = client
        .list_page("sweep/", 100, None)
        .await
        .unwrap()
        .entries
        .into_iter()
        .map(|entry| entry.name)
        .collect();
    assert!(names.iter().any(|name| name == "data.bin"));
    assert!(names.contains(&fresh_name));
    assert!(!names.contains(&stale_name));

    assert_eq!(
        client
            .sweep_temp_files("sweep/", Duration::ZERO)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        client
            .sweep_temp_files("never-written/", Duration::ZERO)
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn concurrent_sweepers_count_one_removal_once() {
    let stale_name = format!(".nfs-crust-tmp-{}", Ulid::from_parts(1, 1));
    let server = TestServer::start_with_files(&[(
        "concurrent-sweep",
        &stale_name,
        Bytes::from_static(b"stale"),
    )])
    .await;
    let proxy = ObservedProxy::start(server.addr.clone(), "remove-at").await;
    let client = client_builder(proxy.addr.as_str())
        .session_slots(4)
        .connect()
        .await
        .unwrap();
    proxy.reset().await;
    proxy.hold_watched_responses_until(2);
    let first = {
        let client = client.clone();
        tokio::spawn(async move {
            client
                .sweep_temp_files("concurrent-sweep", Duration::ZERO)
                .await
        })
    };
    let second = {
        let client = client.clone();
        tokio::spawn(async move {
            client
                .sweep_temp_files("concurrent-sweep", Duration::ZERO)
                .await
        })
    };

    assert!(
        proxy
            .wait_for_max_in_flight(2, Duration::from_secs(2))
            .await
    );
    proxy.release_response_hold();
    let first = first.await.unwrap().unwrap();
    let second = second.await.unwrap().unwrap();
    assert_eq!(first + second, 1);
}

#[tokio::test]
async fn get_reads_using_configured_read_chunks() {
    let server = TestServer::start().await;
    let client = client_builder(server.addr.as_str())
        .read_chunk_size(4)
        .connect()
        .await
        .unwrap();

    client
        .put(
            "chunked-read/data.bin",
            Bytes::from_static(b"abcdefghij"),
            PutMode::Overwrite,
        )
        .await
        .unwrap();

    assert_eq!(
        client.get("chunked-read/data.bin").await.unwrap(),
        Bytes::from_static(b"abcdefghij")
    );
}

#[tokio::test]
async fn get_reuses_initial_path_read_prefix() {
    let server = TestServer::start().await;
    let proxy = ObservedProxy::start(server.addr.clone(), "read-anon").await;
    let client = client_builder(proxy.addr.as_str())
        .read_chunk_size(4)
        .connect()
        .await
        .unwrap();

    let body = Bytes::from_static(b"abcdefghij");
    client
        .put("prefix-reuse/data.bin", body.clone(), PutMode::Overwrite)
        .await
        .unwrap();
    proxy.reset().await;

    assert_eq!(client.get("prefix-reuse/data.bin").await.unwrap(), body);
    assert_eq!(proxy.observed_count(), 2);
}

#[tokio::test]
async fn unknown_size_get_probes_and_pipelines_at_read_granularity() {
    let server = TestServer::start().await;
    let proxy = ObservedProxy::start(server.addr.clone(), "read-anon").await;
    let client = client_builder(proxy.addr.as_str())
        .read_granularity(4)
        .connect()
        .await
        .unwrap();

    let body = Bytes::from_static(b"abcdefghij");
    client
        .put("granularity/data.bin", body.clone(), PutMode::Overwrite)
        .await
        .unwrap();
    proxy.reset().await;

    // With the default 1 MiB read_chunk_size, the probe and the continuation
    // chunks must still follow the 4-byte granularity: a 4-byte probe inside
    // the lookup compound plus two follow-up READ compounds.
    assert_eq!(client.get("granularity/data.bin").await.unwrap(), body);
    assert_eq!(proxy.observed_count(), 2);
}

#[tokio::test]
async fn read_granularity_clamps_to_read_chunk_size() {
    let server = TestServer::start().await;
    let proxy = ObservedProxy::start(server.addr.clone(), "read-anon").await;
    let client = client_builder(proxy.addr.as_str())
        .read_chunk_size(4)
        .read_granularity(1024)
        .connect()
        .await
        .unwrap();

    let body = Bytes::from_static(b"abcdefghij");
    client
        .put(
            "granularity-clamp/data.bin",
            body.clone(),
            PutMode::Overwrite,
        )
        .await
        .unwrap();
    proxy.reset().await;

    assert_eq!(
        client.get("granularity-clamp/data.bin").await.unwrap(),
        body
    );
    assert_eq!(proxy.observed_count(), 2);
}

#[tokio::test]
async fn chunked_get_resolves_size_without_separate_lookup() {
    let server = TestServer::start().await;
    let proxy = ObservedProxy::start(server.addr.clone(), "lookup-file-attrs").await;
    let client = client_builder(proxy.addr.as_str())
        .read_chunk_size(4)
        .connect()
        .await
        .unwrap();

    let body = Bytes::from_static(b"abcdefghij");
    client
        .put("sized-get/data.bin", body.clone(), PutMode::Overwrite)
        .await
        .unwrap();
    proxy.reset().await;

    assert_eq!(client.get("sized-get/data.bin").await.unwrap(), body);
    assert_eq!(proxy.observed_count(), 0);
}

#[tokio::test]
async fn chunked_get_range_reads_first_chunk_while_resolving_handle() {
    let server = TestServer::start().await;
    let proxy = ObservedProxy::start(server.addr.clone(), "lookup-file").await;
    let client = client_builder(proxy.addr.as_str())
        .read_chunk_size(4)
        .connect()
        .await
        .unwrap();

    let body = Bytes::from_static(b"abcdefghij");
    client
        .put("handle-range/data.bin", body.clone(), PutMode::Overwrite)
        .await
        .unwrap();
    proxy.reset().await;

    assert_eq!(
        client
            .get_range("handle-range/data.bin", 0..10)
            .await
            .unwrap(),
        body
    );
    assert_eq!(proxy.observed_count(), 0);
}

#[tokio::test]
async fn small_get_reads_path_without_size_lookup() {
    let server = TestServer::start().await;
    let proxy = ObservedProxy::start(server.addr.clone(), "lookup-file-attrs").await;
    let writer = connect_client(proxy.addr.as_str()).await;
    writer
        .put(
            "small-get/data.bin",
            Bytes::from_static(b"small"),
            PutMode::Overwrite,
        )
        .await
        .unwrap();

    let reader = connect_client(proxy.addr.as_str()).await;
    proxy.reset().await;

    assert_eq!(
        reader.get("small-get/data.bin").await.unwrap(),
        Bytes::from_static(b"small")
    );
    assert_eq!(proxy.max_in_flight(), 0);
}

#[tokio::test]
async fn known_size_get_uses_listing_size_without_size_lookup() {
    let server = TestServer::start().await;
    let proxy = ObservedProxy::start(server.addr.clone(), "lookup-file-attrs").await;
    let client = client_builder(proxy.addr.as_str())
        .read_chunk_size(4)
        .connect()
        .await
        .unwrap();
    client
        .put(
            "known-size/data.bin",
            Bytes::from_static(b"abcdefghij"),
            PutMode::Overwrite,
        )
        .await
        .unwrap();
    let listed = client.list_page("known-size", 100, None).await.unwrap();
    let entry = listed
        .entries
        .iter()
        .find(|entry| entry.name == "data.bin")
        .unwrap();
    let size = client.entry_info(&entry.path).await.unwrap().size;
    proxy.reset().await;

    assert_eq!(
        client
            .get_known_size("known-size/data.bin", size)
            .await
            .unwrap(),
        Bytes::from_static(b"abcdefghij")
    );
    assert_eq!(proxy.max_in_flight(), 0);
}

#[tokio::test]
async fn known_size_get_rejects_stale_sizes() {
    let server = TestServer::start().await;
    let client = client_builder(server.addr.as_str())
        .session_slots(4)
        .read_granularity(4)
        .connect()
        .await
        .unwrap();
    client
        .put(
            "known-size-mismatch/data.bin",
            Bytes::from_static(b"abcdefghij"),
            PutMode::Overwrite,
        )
        .await
        .unwrap();

    assert_eq!(
        client
            .get_known_size("known-size-mismatch/data.bin", 10)
            .await
            .unwrap(),
        Bytes::from_static(b"abcdefghij")
    );

    let too_small = client
        .get_known_size("known-size-mismatch/data.bin", 9)
        .await
        .unwrap_err();
    assert!(matches!(
        too_small,
        Error::FileSizeMismatch {
            expected: 9,
            actual: None,
        }
    ));

    let too_large = client
        .get_known_size("known-size-mismatch/data.bin", 11)
        .await
        .unwrap_err();
    assert!(matches!(
        too_large,
        Error::FileSizeMismatch {
            expected: 11,
            actual: Some(10),
        }
    ));

    let zero = client
        .get_known_size("known-size-mismatch/data.bin", 0)
        .await
        .unwrap_err();
    assert!(matches!(
        zero,
        Error::FileSizeMismatch {
            expected: 0,
            actual: None,
        }
    ));
}

#[tokio::test]
async fn zero_size_get_still_validates_the_remote_file() {
    let server = TestServer::start().await;
    let client = connect_client(server.addr.as_str()).await;
    client
        .put("zero-size/empty.bin", Bytes::new(), PutMode::Overwrite)
        .await
        .unwrap();

    assert_eq!(
        client
            .get_known_size("zero-size/empty.bin", 0)
            .await
            .unwrap(),
        Bytes::new()
    );

    let missing = client
        .get_known_size("zero-size/missing.bin", 0)
        .await
        .unwrap_err();
    assert!(missing.is_not_found());

    let directory = client.get_known_size("zero-size", 0).await.unwrap_err();
    assert_eq!(directory.nfs_operation(), Some("READ"));
}

#[tokio::test]
async fn put_pipelines_large_write_chunks_over_session_slots() {
    let server = TestServer::start().await;
    let proxy = ObservedProxy::start(server.addr.clone(), "write").await;
    let client = client_builder(proxy.addr.as_str())
        .session_slots(4)
        .write_chunk_size(1024)
        .connect()
        .await
        .unwrap();
    proxy.reset().await;
    proxy.hold_watched_responses_until(2);

    let body = Bytes::from(vec![7; 16 * 1024]);
    let put_client = client.clone();
    let put_task = tokio::spawn(async move {
        put_client
            .put("pipeline/write.bin", body, PutMode::Overwrite)
            .await
    });

    let pipelined = proxy
        .wait_for_max_in_flight(2, Duration::from_secs(2))
        .await;
    proxy.release_response_hold();
    tokio::time::timeout(Duration::from_secs(5), put_task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(
        pipelined,
        "large put issued at most {} plain WRITE chunk RPCs concurrently",
        proxy.max_in_flight()
    );
}

#[tokio::test]
async fn get_range_pipelines_large_read_chunks_over_session_slots() {
    let server = TestServer::start().await;
    let proxy = ObservedProxy::start(server.addr.clone(), "read-anon").await;
    let client = client_builder(proxy.addr.as_str())
        .session_slots(4)
        .read_chunk_size(1024)
        .write_chunk_size(1024)
        .connect()
        .await
        .unwrap();

    let body = Bytes::from(vec![11; 16 * 1024]);
    client
        .put("pipeline/read.bin", body.clone(), PutMode::Overwrite)
        .await
        .unwrap();
    proxy.reset().await;
    proxy.hold_watched_responses_until(2);

    let get_client = client.clone();
    let len = body.len() as u64;
    let get_task =
        tokio::spawn(async move { get_client.get_range("pipeline/read.bin", 0..len).await });

    let pipelined = proxy
        .wait_for_max_in_flight(2, Duration::from_secs(2))
        .await;
    proxy.release_response_hold();
    let read = tokio::time::timeout(Duration::from_secs(5), get_task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();

    assert_eq!(read, body);
    assert!(
        pipelined,
        "large get_range issued at most {} plain READ chunk RPCs concurrently",
        proxy.max_in_flight()
    );
}

#[tokio::test]
async fn pipelined_get_range_stops_at_eof_with_outstanding_reads_beyond_eof() {
    let server = TestServer::start().await;
    let proxy = ObservedProxy::start(server.addr.clone(), "read-anon").await;
    let client = client_builder(proxy.addr.as_str())
        .session_slots(4)
        .read_chunk_size(1024)
        .connect()
        .await
        .unwrap();

    let body = Bytes::from(vec![13; 1500]);
    client
        .put(
            "pipeline/range-past-eof.bin",
            body.clone(),
            PutMode::Overwrite,
        )
        .await
        .unwrap();
    proxy.reset().await;
    proxy.hold_watched_responses_until(3);

    let get_client = client.clone();
    let get_task = tokio::spawn(async move {
        get_client
            .get_range("pipeline/range-past-eof.bin", 0..4096)
            .await
    });

    let pipelined = proxy
        .wait_for_max_in_flight(3, Duration::from_secs(2))
        .await;
    proxy.release_response_hold();
    let read = tokio::time::timeout(Duration::from_secs(5), get_task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();

    assert_eq!(read, body);
    assert!(
        pipelined,
        "range past EOF issued at most {} plain READ chunk RPCs concurrently",
        proxy.max_in_flight()
    );
}

#[tokio::test]
async fn get_enforces_configured_materialization_limit() {
    let server = TestServer::start().await;
    let writer = client_builder(server.addr.as_str())
        .connect()
        .await
        .unwrap();
    writer
        .put(
            "read-limit/data.bin",
            Bytes::from_static(b"0123456789"),
            PutMode::Overwrite,
        )
        .await
        .unwrap();

    let client = client_builder(server.addr.as_str())
        .max_buffered_read_size(Some(4))
        .connect()
        .await
        .unwrap();

    let err = client.get("read-limit/data.bin").await.unwrap_err();
    assert!(matches!(
        err,
        Error::BufferedReadTooLarge {
            size: Some(10),
            limit: 4,
        }
    ));
}

#[tokio::test]
async fn get_range_enforces_limit_and_stops_at_eof() {
    let server = TestServer::start().await;
    let writer = client_builder(server.addr.as_str())
        .connect()
        .await
        .unwrap();
    writer
        .put(
            "range-limit/data.bin",
            Bytes::from_static(b"0123456789"),
            PutMode::Overwrite,
        )
        .await
        .unwrap();

    let client = client_builder(server.addr.as_str())
        .max_buffered_read_size(Some(4))
        .connect()
        .await
        .unwrap();

    assert_eq!(
        client
            .get_range("range-limit/data.bin", 2..6)
            .await
            .unwrap(),
        Bytes::from_static(b"2345")
    );
    assert_eq!(
        client
            .get_range("range-limit/data.bin", 8..20)
            .await
            .unwrap(),
        Bytes::from_static(b"89")
    );

    let err = client
        .get_range("range-limit/data.bin", 2..8)
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        Error::BufferedReadTooLarge {
            size: Some(6),
            limit: 4,
        }
    ));
}

#[tokio::test]
async fn get_range_size_validation_does_not_open_file() {
    let server = TestServer::start().await;
    let proxy = ObservedProxy::start(server.addr.clone(), "open").await;
    let client = client_builder(proxy.addr.as_str())
        .max_buffered_read_size(Some(4))
        .connect()
        .await
        .unwrap();

    client
        .put(
            "range-no-open/tiny.bin",
            Bytes::from_static(b"xy"),
            PutMode::Overwrite,
        )
        .await
        .unwrap();
    proxy.reset().await;

    assert_eq!(
        client
            .get_range("range-no-open/tiny.bin", 0..8)
            .await
            .unwrap(),
        Bytes::from_static(b"xy")
    );
    assert_eq!(proxy.max_in_flight(), 0);
}

#[tokio::test]
async fn empty_known_reads_return_without_read_compounds() {
    let server = TestServer::start().await;
    let proxy = ObservedProxy::start(server.addr.clone(), "read-anon").await;
    let client = client_builder(proxy.addr.as_str())
        .max_buffered_read_size(Some(4))
        .connect()
        .await
        .unwrap();

    client
        .put("empty/read.bin", Bytes::new(), PutMode::Overwrite)
        .await
        .unwrap();
    client
        .put(
            "empty/small.bin",
            Bytes::from_static(b"0123456789"),
            PutMode::Overwrite,
        )
        .await
        .unwrap();
    proxy.reset().await;

    assert_eq!(client.get("empty/read.bin").await.unwrap(), Bytes::new());
    assert_eq!(
        client.get_range("empty/small.bin", 20..30).await.unwrap(),
        Bytes::new()
    );
    assert_eq!(proxy.max_in_flight(), 0);
}

#[tokio::test]
#[allow(clippy::reversed_empty_ranges)]
async fn invalid_inputs_are_rejected_before_trivial_results() {
    let server = TestServer::start().await;
    let client = connect_client(server.addr.as_str()).await;

    let err = client.get_range("bad//path.bin", 5..2).await.unwrap_err();
    assert!(matches!(err, Error::InvalidPath(_)));

    let err = client.list_page("pages/", 0, None).await.unwrap_err();
    assert!(matches!(err, Error::InvalidConfig(_)));
}

#[tokio::test]
async fn list_supports_continuation_tokens() {
    let server = TestServer::start().await;
    let proxy = ObservedProxy::start(server.addr.clone(), "readdir-path").await;
    let client = client_builder(proxy.addr.as_str())
        .session_slots(4)
        .connect()
        .await
        .unwrap();

    for index in 0..5 {
        client
            .put(
                format!("pages/{index}.txt"),
                Bytes::from(format!("body-{index}")),
                PutMode::Overwrite,
            )
            .await
            .unwrap();
    }

    let first = client.list_page("pages/", 2, None).await.unwrap();
    assert_eq!(first.entries.len(), 2);
    assert!(first.next_token.is_some());
    assert_eq!(proxy.observed_count(), 1);

    let second = client
        .list_page("pages/", 10, first.next_token)
        .await
        .unwrap();
    assert_eq!(second.entries.len(), 3);
    assert!(second.next_token.is_none());
    assert_eq!(proxy.observed_count(), 2);
}

#[tokio::test]
async fn list_page_is_explicit_and_easy_to_continue() {
    let server = TestServer::start().await;
    let client = client_builder(server.addr.as_str())
        .session_slots(4)
        .connect()
        .await
        .unwrap();

    for index in 0..4 {
        client
            .put(
                format!("builder/{index}.txt"),
                Bytes::from(format!("body-{index}")),
                PutMode::Overwrite,
            )
            .await
            .unwrap();
    }

    let first = client.list_page("builder/", 2, None).await.unwrap();
    assert_eq!(first.entries.len(), 2);
    let second = client
        .list_page("builder/", 10, first.next_token)
        .await
        .unwrap();
    assert_eq!(second.entries.len(), 2);
}

#[tokio::test]
async fn list_is_directory_based_not_prefix_or_recursive() {
    let server = TestServer::start().await;
    let client = client_builder(server.addr.as_str())
        .session_slots(4)
        .connect()
        .await
        .unwrap();

    client
        .put(
            "tree/file.txt",
            Bytes::from_static(b"file"),
            PutMode::Overwrite,
        )
        .await
        .unwrap();
    client
        .put(
            "tree/nested/file.bin",
            Bytes::from_static(b"file"),
            PutMode::Overwrite,
        )
        .await
        .unwrap();
    client
        .put(
            "treehouse/not-in-tree.txt",
            Bytes::from_static(b"outside"),
            PutMode::Overwrite,
        )
        .await
        .unwrap();

    let listed = client.list_page("tree", 100, None).await.unwrap();
    assert_eq!(listed.entries.len(), 2);
    assert_eq!(listed.entries[0].path, "tree/file.txt");
    assert_eq!(listed.entries[1].path, "tree/nested");
    assert_eq!(
        client.entry_info("tree/file.txt").await.unwrap().kind,
        EntryKind::File
    );
    assert_eq!(
        client.entry_info("tree/nested").await.unwrap().kind,
        EntryKind::Directory
    );
}

#[tokio::test]
async fn cloneable_client_supports_parallel_puts_and_gets() {
    let server = TestServer::start().await;
    let client = client_builder(server.addr.as_str())
        .session_slots(8)
        .connect()
        .await
        .unwrap();

    let mut tasks = Vec::new();
    for index in 0..16 {
        let client = client.clone();
        tasks.push(tokio::spawn(async move {
            let path = format!("parallel/{index:02}.bin");
            let body = Bytes::from(format!("payload-{index:02}"));
            client
                .put(&path, body.clone(), PutMode::Overwrite)
                .await
                .unwrap();
            assert_eq!(client.get(&path).await.unwrap(), body);
        }));
    }

    for task in tasks {
        task.await.unwrap();
    }

    let listed = client.list_page("parallel/", 100, None).await.unwrap();
    assert_eq!(listed.entries.len(), 16);
}

#[tokio::test]
async fn replay_safe_operation_reconnects_after_connection_drop() {
    let server = TestServer::start().await;
    let proxy = TcpProxy::start(server.addr.clone()).await.unwrap();
    let client = client_builder(proxy.addr.as_str())
        .session_slots(4)
        .operation_timeout(Some(Duration::from_millis(250)))
        .connect()
        .await
        .unwrap();

    client
        .put(
            "reconnect/before.bin",
            Bytes::from_static(b"before"),
            PutMode::Overwrite,
        )
        .await
        .unwrap();
    assert_eq!(
        client.get("reconnect/before.bin").await.unwrap(),
        Bytes::from_static(b"before")
    );

    proxy.drop_connections().await;

    assert_eq!(
        client.get("reconnect/before.bin").await.unwrap(),
        Bytes::from_static(b"before")
    );

    client
        .put(
            "reconnect/after.bin",
            Bytes::from_static(b"after"),
            PutMode::Overwrite,
        )
        .await
        .unwrap();
    assert_eq!(
        client.get("reconnect/after.bin").await.unwrap(),
        Bytes::from_static(b"after")
    );
}

#[tokio::test]
async fn post_dispatch_publish_reply_loss_is_not_replayed() {
    let server = TestServer::start().await;
    let proxy = ObservedProxy::start(server.addr.clone(), "put-fused").await;
    let client = client_builder(proxy.addr.as_str())
        .operation_timeout(Some(Duration::from_millis(250)))
        .connect()
        .await
        .unwrap();

    proxy.drop_watched_responses();

    let put = tokio::spawn(async move {
        client
            .put(
                "ambiguous.bin",
                Bytes::from_static(b"body"),
                PutMode::Overwrite,
            )
            .await
    });
    assert!(
        proxy
            .wait_for_dropped_successful_response(Duration::from_secs(2))
            .await
    );
    let error = put.await.unwrap().unwrap_err();
    assert!(error.is_outcome_unknown());

    let verifier = connect_client(server.addr.as_str()).await;
    assert_eq!(
        verifier.get("ambiguous.bin").await.unwrap(),
        Bytes::from_static(b"body")
    );
}
