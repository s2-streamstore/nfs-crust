use std::env;
use std::io;
use std::process;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[cfg(feature = "aws-efs")]
use aws_credential_types::Credentials;
use bytes::Bytes;
#[cfg(feature = "aws-efs")]
use nfs_crust::EfsIamConfig;
use nfs_crust::{EntryKind, Error, NfsClient, NfsClientBuilder, PutMode, TlsConfig};

mod support;
use support::{ObservedProxy, TcpProxy};

#[derive(Debug, Clone)]
struct ExternalConfig {
    endpoint: String,
    export: String,
    tls_server_name: Option<String>,
    #[cfg(feature = "aws-efs")]
    efs_iam: Option<ExternalEfsIam>,
    prefix: String,
    run_id: String,
    connect_timeout: Option<Duration>,
    operation_timeout: Option<Duration>,
}

impl ExternalConfig {
    fn from_env() -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let endpoint = required_env("NFS_CRUST_EXTERNAL_ENDPOINT")?;
        let export = env::var("NFS_CRUST_EXTERNAL_EXPORT").unwrap_or_else(|_| "/".to_owned());
        let tls_server_name = optional_nonempty_env("NFS_CRUST_EXTERNAL_TLS_SERVER_NAME")?;
        #[cfg(feature = "aws-efs")]
        let efs_iam = ExternalEfsIam::from_env()?;
        #[cfg(not(feature = "aws-efs"))]
        if env_bool("NFS_CRUST_EXTERNAL_EFS_IAM", false)? {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "NFS_CRUST_EXTERNAL_EFS_IAM requires cargo test --features aws-efs",
            )
            .into());
        }
        let prefix = env::var("NFS_CRUST_EXTERNAL_PREFIX")
            .unwrap_or_else(|_| "nfs-crust-external-tests".to_owned());
        let prefix = prefix.trim_matches('/').to_owned();
        if prefix.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "NFS_CRUST_EXTERNAL_PREFIX must not be empty",
            )
            .into());
        }
        let operation_timeout = env_duration_secs("NFS_CRUST_EXTERNAL_OPERATION_TIMEOUT_SECONDS")?;
        let connect_timeout = env_duration_secs("NFS_CRUST_EXTERNAL_CONNECT_TIMEOUT_SECONDS")?;

        Ok(Self {
            endpoint,
            export,
            tls_server_name,
            #[cfg(feature = "aws-efs")]
            efs_iam,
            prefix,
            run_id: unique_run_id(),
            connect_timeout,
            operation_timeout,
        })
    }

    async fn connect(&self) -> Result<NfsClient, Error> {
        self.builder()?.session_slots(16).connect().await
    }

    fn builder(&self) -> Result<NfsClientBuilder, Error> {
        self.builder_for_endpoint(self.endpoint.as_str())
    }

    fn builder_for_endpoint(&self, endpoint: &str) -> Result<NfsClientBuilder, Error> {
        let builder = NfsClient::builder(endpoint, self.export.as_str());
        self.configure_builder(builder)
    }

    fn configure_builder(&self, mut builder: NfsClientBuilder) -> Result<NfsClientBuilder, Error> {
        if let Some(server_name) = &self.tls_server_name {
            let tls = TlsConfig::new(server_name.clone());
            #[cfg(feature = "aws-efs")]
            let tls = if let Some(efs_iam) = &self.efs_iam {
                tls.with_efs_iam(efs_iam.to_config())
            } else {
                tls
            };
            builder = builder.tls(tls);
        } else {
            #[cfg(feature = "aws-efs")]
            if self.efs_iam.is_some() {
                return Err(Error::InvalidConfig(
                    "NFS_CRUST_EXTERNAL_EFS_IAM requires NFS_CRUST_EXTERNAL_TLS_SERVER_NAME"
                        .to_owned(),
                ));
            }
        }
        if let Some(timeout) = self.operation_timeout {
            builder = builder.operation_timeout(Some(timeout));
        }
        if let Some(timeout) = self.connect_timeout {
            builder = builder.connect_timeout(timeout);
        }
        Ok(builder)
    }

    fn path(&self, suffix: &str) -> String {
        format!(
            "{}/{}/{}",
            self.prefix,
            self.run_id,
            suffix.trim_matches('/')
        )
    }
}

#[cfg(feature = "aws-efs")]
#[derive(Debug, Clone)]
struct ExternalEfsIam {
    file_system_id: String,
    region: String,
    access_point_id: Option<String>,
    credentials: Credentials,
}

#[cfg(feature = "aws-efs")]
impl ExternalEfsIam {
    fn from_env() -> Result<Option<Self>, Box<dyn std::error::Error + Send + Sync>> {
        if !env_bool("NFS_CRUST_EXTERNAL_EFS_IAM", false)? {
            return Ok(None);
        }
        let file_system_id = required_env("NFS_CRUST_EXTERNAL_EFS_FILE_SYSTEM_ID")?;
        let region = optional_nonempty_env("NFS_CRUST_EXTERNAL_AWS_REGION")?
            .or(optional_nonempty_env("AWS_REGION")?)
            .or(optional_nonempty_env("AWS_DEFAULT_REGION")?)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "NFS_CRUST_EXTERNAL_AWS_REGION or AWS_REGION must be set for EFS IAM tests",
                )
            })?;
        let access_point_id = optional_nonempty_env("NFS_CRUST_EXTERNAL_EFS_ACCESS_POINT_ID")?;
        let access_key_id = required_env("AWS_ACCESS_KEY_ID")?;
        let secret_access_key = required_env("AWS_SECRET_ACCESS_KEY")?;
        let session_token = optional_nonempty_env("AWS_SESSION_TOKEN")?;
        let credentials = Credentials::new(
            access_key_id,
            secret_access_key,
            session_token,
            None,
            "nfs-crust-external-test",
        );

        Ok(Some(Self {
            file_system_id,
            region,
            access_point_id,
            credentials,
        }))
    }

    fn to_config(&self) -> EfsIamConfig {
        let mut config = EfsIamConfig::new(
            self.file_system_id.clone(),
            self.region.clone(),
            self.credentials.clone(),
        );
        if let Some(access_point_id) = &self.access_point_id {
            config = config.with_access_point_id(access_point_id.clone());
        }
        config
    }
}

fn optional_nonempty_env(
    name: &str,
) -> Result<Option<String>, Box<dyn std::error::Error + Send + Sync>> {
    match env::var(name) {
        Ok(value) if !value.is_empty() => Ok(Some(value)),
        Ok(_) => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{name} must not be empty when set"),
        )
        .into()),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(err) => Err(err.into()),
    }
}

fn required_env(name: &str) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    match env::var(name) {
        Ok(value) if !value.is_empty() => Ok(value),
        _ => Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("{name} must be set to run ignored external NFS tests"),
        )
        .into()),
    }
}

fn env_bool(name: &str, default: bool) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
    match env::var(name) {
        Ok(value) => match value.as_str() {
            "1" | "true" | "TRUE" | "yes" | "YES" => Ok(true),
            "0" | "false" | "FALSE" | "no" | "NO" => Ok(false),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("{name} must be true/false or 1/0"),
            )
            .into()),
        },
        Err(env::VarError::NotPresent) => Ok(default),
        Err(err) => Err(err.into()),
    }
}

fn env_usize(
    name: &str,
    default: usize,
) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
    match env::var(name) {
        Ok(value) => Ok(value.parse()?),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(err) => Err(err.into()),
    }
}

fn env_duration_secs(
    name: &str,
) -> Result<Option<Duration>, Box<dyn std::error::Error + Send + Sync>> {
    let Some(value) = optional_nonempty_env(name)? else {
        return Ok(None);
    };
    let seconds = value.parse::<u64>()?;
    if seconds == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{name} must be greater than zero"),
        )
        .into());
    }
    Ok(Some(Duration::from_secs(seconds)))
}

fn unique_run_id() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is before UNIX_EPOCH")
        .as_nanos();
    format!("run-{}-{nanos}", process::id())
}

async fn delete_files(client: &NfsClient, paths: &[String]) {
    for path in paths.iter().rev() {
        let _ = client.delete(path).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires NFS_CRUST_EXTERNAL_ENDPOINT pointing at a real NFSv4.1 server"]
async fn external_file_directory_and_range_round_trip()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let config = ExternalConfig::from_env()?;
    let client = config.connect().await?;
    let file = config.path("round-trip/hello.txt");

    client
        .put(&file, Bytes::from_static(b"abcdef"), PutMode::Overwrite)
        .await?;

    let all = client.get(&file).await?;
    let range = client.get_range(&file, 2..5).await?;
    let dir = config.path("round-trip");
    let listed = client.list_page(&dir, 100, None).await?;

    assert_eq!(&all[..], b"abcdef");
    assert_eq!(&range[..], b"cde");
    assert!(
        listed
            .entries
            .iter()
            .any(|entry| entry.path == file && entry.name == "hello.txt")
    );
    assert_eq!(client.entry_info(&file).await?.kind, EntryKind::File);

    delete_files(&client, &[file]).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires NFS_CRUST_EXTERNAL_ENDPOINT pointing at a real NFSv4.1 server"]
async fn external_large_file_get_and_ranges() -> Result<(), Box<dyn std::error::Error + Send + Sync>>
{
    let config = ExternalConfig::from_env()?;
    let file_size = env_usize("NFS_CRUST_EXTERNAL_LARGE_BYTES", 8 * 1024 * 1024)?.max(1);
    let client = config
        .builder()?
        .session_slots(16)
        .write_chunk_size(128 * 1024)
        .read_chunk_size(96 * 1024)
        .connect()
        .await?;
    let file = config.path("large/data.bin");
    let body = pattern_bytes(0, file_size);

    client.put(&file, body.clone(), PutMode::Overwrite).await?;

    let limited = config
        .builder()?
        .max_buffered_read_size(Some((file_size / 2).max(1) as u64))
        .connect()
        .await?;
    let err = limited.get(&file).await.unwrap_err();
    assert!(err.is_buffered_read_too_large());

    let start = file_size / 3;
    let end = (start + 4096).min(file_size);
    let range = client.get_range(&file, start as u64..end as u64).await?;
    assert_bytes_eq(&range, &pattern_bytes(start, end - start), "range read");
    assert_bytes_eq(&client.get(&file).await?, &body, "full read");

    delete_files(&client, &[file]).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires NFS_CRUST_EXTERNAL_ENDPOINT pointing at a real NFSv4.1 server"]
async fn external_large_directory_paginates() -> Result<(), Box<dyn std::error::Error + Send + Sync>>
{
    let config = ExternalConfig::from_env()?;
    let entry_count = env_usize("NFS_CRUST_EXTERNAL_DIRECTORY_ENTRIES", 300)?.max(2);
    let client = config.connect().await?;
    let dir = config.path("directory");
    let nested_dir = config.path("directory/nested");
    let nested_file = config.path("directory/nested/child.txt");
    let mut files = Vec::with_capacity(entry_count + 1);

    for index in 0..entry_count {
        let path = config.path(&format!("directory/file-{index:04}.txt"));
        client
            .put(
                &path,
                Bytes::from(format!("body-{index}")),
                PutMode::Overwrite,
            )
            .await?;
        files.push(path);
    }
    client
        .put(
            &nested_file,
            Bytes::from_static(b"nested"),
            PutMode::Overwrite,
        )
        .await?;
    files.push(nested_file.clone());

    let mut token = None;
    let mut direct_paths = Vec::new();
    loop {
        let page = client.list_page(&dir, 37, token.take()).await?;
        assert!(page.entries.len() <= 37);
        token = page.next_token;
        direct_paths.extend(page.entries.into_iter().map(|entry| entry.path));
        if token.is_none() {
            break;
        }
    }

    assert_eq!(direct_paths.len(), entry_count + 1);
    assert!(direct_paths.iter().any(|path| path == &nested_dir));
    assert!(!direct_paths.iter().any(|path| path == &nested_file));

    delete_files(&client, &files).await;
    Ok(())
}

#[tokio::test]
#[ignore = "requires NFS_CRUST_EXTERNAL_ENDPOINT pointing at a real NFSv4.1 server"]
async fn external_small_put_avoids_multi_compound_fallback()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let config = ExternalConfig::from_env()?;
    if config.tls_server_name.is_some() {
        // The observing proxy reads RPC compound tags and cannot see through
        // TLS; the plaintext external run covers this assertion.
        return Ok(());
    }

    let proxy = ObservedProxy::start(config.endpoint.clone(), "open").await;
    let client = config
        .builder_for_endpoint(proxy.addr.as_str())?
        .connect()
        .await?;
    let seed = config.path("fused/seed.bin");
    let file = config.path("fused/data.bin");

    client
        .put(&seed, Bytes::from_static(b"seed"), PutMode::Overwrite)
        .await?;
    proxy.reset().await;

    client
        .put(&file, Bytes::from_static(b"payload"), PutMode::Overwrite)
        .await?;

    assert_eq!(
        proxy.observed_count(),
        0,
        "small put fell back to the multi-compound path"
    );
    assert_eq!(&client.get(&file).await?[..], b"payload");

    delete_files(&client, &[seed, file]).await;
    Ok(())
}

#[tokio::test]
#[ignore = "requires NFS_CRUST_EXTERNAL_ENDPOINT pointing at a real NFSv4.1 server"]
async fn external_medium_put_uses_multi_compound_fallback()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let config = ExternalConfig::from_env()?;
    if config.tls_server_name.is_some() {
        return Ok(());
    }

    let proxy = ObservedProxy::start(config.endpoint.clone(), "put-fused").await;
    let client = config
        .builder_for_endpoint(proxy.addr.as_str())?
        .connect()
        .await?;
    let file = config.path("medium/data.bin");
    let body = pattern_bytes(0, 512 * 1024);

    client.put(&file, body.clone(), PutMode::Overwrite).await?;

    assert_eq!(
        proxy.observed_count(),
        0,
        "512 KiB put used the EFS-pathological fused compound"
    );
    assert_bytes_eq(&client.get(&file).await?, &body, "medium put");
    delete_files(&client, &[file]).await;
    Ok(())
}

#[tokio::test]
#[ignore = "requires NFS_CRUST_EXTERNAL_ENDPOINT pointing at a real NFSv4.1 server"]
async fn external_write_size_boundary_sweep() -> Result<(), Box<dyn std::error::Error + Send + Sync>>
{
    let config = ExternalConfig::from_env()?;
    let mut sizes = vec![256, 320, 384, 448];
    sizes.extend((480..=544).step_by(4));
    sizes.extend([576, 640, 768, 896, 1024]);
    let mut failures = Vec::new();

    for size_kib in sizes {
        let client = config
            .builder()?
            .operation_timeout(Some(Duration::from_secs(3)))
            .connect()
            .await?;
        let file = config.path(&format!("write-size-sweep/{size_kib}-kib.bin"));
        let body = pattern_bytes(size_kib, size_kib * 1024);
        let started = Instant::now();
        let mut successes = 0;
        let mut error = None;
        for _ in 0..12 {
            match client.put(&file, body.clone(), PutMode::Overwrite).await {
                Ok(()) => successes += 1,
                Err(err) => {
                    error = Some(err.to_string());
                    break;
                }
            }
        }
        eprintln!(
            "WRITE_SIZE_SWEEP size_kib={size_kib} successes={successes} elapsed_ms={} error={:?}",
            started.elapsed().as_millis(),
            error
        );
        if let Some(error) = error {
            failures.push(format!(
                "{size_kib} KiB after {successes} successes: {error}"
            ));
        } else {
            assert_bytes_eq(&client.get(&file).await?, &body, "size-sweep body");
        }
    }

    assert!(
        failures.is_empty(),
        "write size sweep failures: {failures:?}"
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires NFS_CRUST_EXTERNAL_ENDPOINT pointing at a real NFSv4.1 server"]
async fn external_put_if_not_exists_uses_one_fused_compound_and_rejects_existing()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let config = ExternalConfig::from_env()?;
    let client = config.connect().await?;
    // Keep the destination directly under the per-run directory. EFS grants
    // 16 operations per compound; create-new resolves the parent twice, so an
    // extra directory level would correctly make the fused shape ineligible.
    let file = config.path("chunk-0");
    let body = Bytes::from_static(b"direct-payload");

    client
        .put(&file, body.clone(), PutMode::IfNotExists)
        .await?;
    assert_eq!(client.get(&file).await?, body);

    let err = client
        .put(&file, Bytes::from_static(b"other"), PutMode::IfNotExists)
        .await
        .unwrap_err();
    assert!(
        matches!(err, nfs_crust::Error::AlreadyExists(_)),
        "expected AlreadyExists, got {err:?}"
    );
    assert_eq!(client.get(&file).await?, body);
    delete_files(&client, &[file]).await;

    if config.tls_server_name.is_some() {
        // The observing proxy reads RPC compound tags and cannot see through
        // TLS; the plaintext external run covers the fused-shape assertion.
        return Ok(());
    }

    let proxy = ObservedProxy::start(config.endpoint.clone(), "put-fused").await;
    let observed_client = config
        .builder_for_endpoint(proxy.addr.as_str())?
        .connect()
        .await?;
    let fused_file = config.path("chunk-1");
    observed_client
        .put(&fused_file, body.clone(), PutMode::IfNotExists)
        .await?;
    assert_eq!(
        proxy.observed_count(),
        1,
        "put fell back to the multi-compound path"
    );
    assert_eq!(observed_client.get(&fused_file).await?, body);
    delete_files(&observed_client, &[fused_file]).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires NFS_CRUST_EXTERNAL_ENDPOINT pointing at a real NFSv4.1 server"]
async fn external_concurrent_clients_round_trip()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let config = ExternalConfig::from_env()?;
    let concurrency = env_usize("NFS_CRUST_EXTERNAL_CONCURRENCY", 32)?.max(1);
    let client = config.connect().await?;
    let mut tasks = Vec::with_capacity(concurrency);

    for index in 0..concurrency {
        let client = client.clone();
        let path = config.path(&format!("concurrent/file-{index:04}.bin"));
        tasks.push(tokio::spawn(async move {
            let body = pattern_bytes(index * 8192, 8192);
            client.put(&path, body.clone(), PutMode::Overwrite).await?;
            let read = client.get(&path).await?;
            assert_eq!(read, body);
            Ok::<String, Error>(path)
        }));
    }

    let mut files = Vec::with_capacity(concurrency);
    for task in tasks {
        files.push(task.await??);
    }
    let listed = client
        .list_page(config.path("concurrent"), concurrency + 1, None)
        .await?;
    assert_eq!(listed.entries.len(), concurrency);

    delete_files(&client, &files).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires NFS_CRUST_EXTERNAL_ENDPOINT pointing at a real NFSv4.1 server"]
async fn external_public_operations_reconnect_after_tcp_drop()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let config = ExternalConfig::from_env()?;
    let proxy = TcpProxy::start(config.endpoint.clone()).await?;
    let client = config
        .builder_for_endpoint(proxy.addr.as_str())?
        .session_slots(4)
        .connect()
        .await?;
    let before = config.path("reconnect/before.bin");
    let after = config.path("reconnect/after.bin");

    client
        .put(&before, Bytes::from_static(b"before"), PutMode::Overwrite)
        .await?;
    assert_eq!(client.get(&before).await?, Bytes::from_static(b"before"));

    proxy.drop_connections().await;

    // First use a replay-safe operation to observe the dead socket and rebuild
    // the session. Sending a mutation first would be genuinely ambiguous if
    // the local TCP stack accepted it while the disconnect was propagating.
    assert_eq!(client.get(&before).await?, Bytes::from_static(b"before"));

    client
        .put(&after, Bytes::from_static(b"after"), PutMode::Overwrite)
        .await?;
    assert_eq!(client.get(&before).await?, Bytes::from_static(b"before"));
    assert_eq!(client.get(&after).await?, Bytes::from_static(b"after"));

    delete_files(&client, &[before, after]).await;
    Ok(())
}

fn pattern_bytes(offset: usize, len: usize) -> Bytes {
    let mut bytes = Vec::with_capacity(len);
    for index in 0..len {
        bytes.push(pattern_byte(offset + index));
    }
    Bytes::from(bytes)
}

fn assert_bytes_eq(actual: &Bytes, expected: &Bytes, context: &str) {
    if actual == expected {
        return;
    }

    let mismatch = actual
        .iter()
        .zip(expected.iter())
        .position(|(actual, expected)| actual != expected);
    panic!(
        "{context} bytes differ: actual_len={}, expected_len={}, first_mismatch={:?}, actual_byte={:?}, expected_byte={:?}",
        actual.len(),
        expected.len(),
        mismatch,
        mismatch.and_then(|index| actual.get(index).copied()),
        mismatch.and_then(|index| expected.get(index).copied()),
    );
}

fn pattern_byte(offset: usize) -> u8 {
    ((offset.wrapping_mul(31).wrapping_add(17)) % 251) as u8
}
