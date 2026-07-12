# nfs-crust

[![CI](https://github.com/s2-streamstore/nfs-crust/actions/workflows/ci.yml/badge.svg)](https://github.com/s2-streamstore/nfs-crust/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/s2-streamstore/nfs-crust/blob/main/LICENSE)

`nfs-crust` lets Tokio applications read and atomically publish whole files on
an NFSv4.1 export without creating an OS mount point. It is designed for
immutable data such as log chunks, segments, and content-addressed blobs.

> [!WARNING]
> This project is experimental. Its authors do not use it in production, and
> the implementation has not yet received a full human review.

## When It Fits

`nfs-crust` is a good fit when:

- files are written once and then read without modification;
- each file can be buffered in memory;
- the application needs low-overhead access to an NFS export from Tokio; and
- atomic publication matters more than a general-purpose filesystem API.

It is not a POSIX filesystem implementation. There is no public streaming API,
random-write API, or access to NFS file handles and stateids.

## Quick Start

Add the crate to your application:

```toml
[dependencies]
nfs-crust = "0.1"
```

The minimum supported Rust version is 1.97. Your application supplies the
Tokio runtime.

```rust,no_run
use bytes::Bytes;
use nfs_crust::{Error, NfsClient, PutMode};

#[tokio::main]
async fn main() -> Result<(), Error> {
    let client = NfsClient::builder("127.0.0.1:2049", "/")
        .connect()
        .await?;

    client
        .put(
            "chunks/00000001",
            Bytes::from_static(b"abcdef"),
            PutMode::IfNotExists,
        )
        .await?;

    let body = client.get("chunks/00000001").await?;
    assert_eq!(&body[..], b"abcdef");

    Ok(())
}
```

The first builder argument is the NFS server address and the second is the
export path. The resulting `NfsClient` acts as a virtual mount. It is cloneable
and can be shared across Tokio tasks.

## API

All paths are relative to the configured export.

| Method | Behavior |
| --- | --- |
| `put(path, body, mode)` | Atomically publishes a complete file, creating parent directories as needed. |
| `get(path)` | Reads a complete file into `Bytes`. |
| `get_known_size(path, size)` | Reads a complete file without a separate size lookup. |
| `get_range(path, range)` | Reads a byte range into `Bytes`. |
| `entry_info(path)` | Returns the file type and size. |
| `delete(path)` | Removes a file or empty directory. Missing paths are accepted. |
| `list_page(directory, limit, token)` | Returns one page of direct children. |
| `sweep_temp_files(directory, age)` | Removes stale temporary files left by interrupted puts. |

Leading and trailing slashes are ignored. Empty interior components, NUL,
`.` and `..` are rejected rather than normalized, so paths cannot escape the
export root. Directory operations may address the root; file operations may
not.

### Writes

`put` writes to a uniquely named temporary file in the destination directory,
commits and verifies it, then publishes it atomically.

- `PutMode::Overwrite` creates or replaces the destination.
- `PutMode::IfNotExists` returns `Error::AlreadyExists` if the destination is
  occupied.

If the client loses the server's reply during publication, it returns
`Error::OutcomeUnknown`. The operation is not replayed because it may already
have succeeded, and replaying it could overwrite another writer. Applications
should reconcile the destination using their own record identity or content
checks.

A process that dies during `put` may leave a temporary file. Use
`sweep_temp_files` with an age greater than the longest expected write plus
clock skew. A sweep can partially succeed; if a reply is lost after removal
starts, it also returns `Error::OutcomeUnknown`.

### Reads and Listing

Reads return buffered `Bytes` and are capped at 128 MiB by default.
`get_known_size` avoids the size lookup but returns
`Error::FileSizeMismatch` if the supplied size is stale. `get_range` stops at
EOF when the requested range extends beyond the file.

`list_page` lists direct children only. It does not recurse or treat paths as
key prefixes. Pass the returned continuation token to the next call; the
client never follows pagination implicitly. List entries contain only a name
and root-relative path, so use `entry_info` when type or size is needed.

## TLS and AWS EFS

Use `TlsConfig` when the server supports NFS over TLS:

```rust,no_run
use nfs_crust::{NfsClient, TlsConfig};

# async fn run() -> Result<(), nfs_crust::Error> {
let hostname = "fs-1234567890abcdef0.efs.us-east-1.amazonaws.com";
let client = NfsClient::builder(format!("{hostname}:2049"), "/")
    .tls(TlsConfig::new(hostname))
    .connect()
    .await?;
# Ok(())
# }
```

The TLS server name must match the endpoint certificate. If the connection
address is an IP address, still pass the certificate's DNS name to
`TlsConfig::new`.

AWS EFS IAM client authorization is available behind the `aws-efs` feature:

```toml
nfs-crust = { version = "0.1", features = ["aws-efs"] }
```

Applications load AWS credentials, usually with `aws-config`, and supply the
credential provider through `EfsIamConfig`. The client uses it to generate a
short-lived EFS client certificate for each TLS connection. NFS operations
continue to use AUTH_SYS credentials.

## Configuration

The defaults use 1 MiB transfer chunks, a 128 MiB buffered-read limit, and
30-second connect and operation timeouts. The builder exposes these limits,
along with file and directory modes.

One `NfsClient` uses one TCP connection. Applications that reach a
per-connection throughput limit can create multiple clients and distribute
work across them. Benchmark against the server and network path used in
production.

## Security

AUTH_SYS provides numeric user and group identity, not cryptographic
authentication. Use plaintext NFS only on a trusted, access-controlled network,
or use TLS when the server supports it. Keep filesystem permissions and export
policy restrictive.

On Unix, the default AUTH_SYS credential uses the process's effective user and
group IDs plus supplementary groups. Other platforms must configure an
explicit `AuthSys` value.

## Learn More

- [Architecture notes](https://github.com/s2-streamstore/nfs-crust/blob/main/docs/architecture.md)
  explain the protocol design, concurrency model, and reconnect behavior.
- [Benchmark results](https://s2-streamstore.github.io/nfs-crust/) compare
  `nfs-crust` with the Linux NFSv4.1 client on AWS EFS.
- [Testing notes](https://github.com/s2-streamstore/nfs-crust/blob/main/docs/testing.md)
  explain how to run the test suites when contributing.

Licensed under the [MIT License](https://github.com/s2-streamstore/nfs-crust/blob/main/LICENSE).
