# Testing

## Local suite

The default suite runs against the in-process `embednfs` NFSv4.1 server and
needs no mount point or external daemon:

```sh
cargo test --locked --workspace --all-targets --all-features
cargo test --locked --workspace --doc --all-features
```

It covers public operations, atomic publication, ambiguous outcomes, paging,
chunking, pipelining, reconnects, cancellation, and protocol-limit validation.
CI owns the complete formatting, lint, MSRV, dependency-policy, and test
checks.

Before publishing a release, verify the crate package manually:

```sh
cargo package --locked -p nfs-crust
```

## External NFS server

Run the ignored interoperability suite against a reachable NFSv4.1 export:

```sh
NFS_CRUST_EXTERNAL_ENDPOINT=127.0.0.1:2049 \
NFS_CRUST_EXTERNAL_EXPORT=/ \
cargo test --test external_nfs -- --ignored --nocapture
```

Set `NFS_CRUST_EXTERNAL_TLS_SERVER_NAME` to enable TLS. For EFS IAM
authorization, also enable the `aws-efs` feature and set:

- `NFS_CRUST_EXTERNAL_EFS_IAM=true`
- `NFS_CRUST_EXTERNAL_EFS_FILE_SYSTEM_ID`
- `NFS_CRUST_EXTERNAL_AWS_REGION`
- standard AWS credential environment variables
- optionally `NFS_CRUST_EXTERNAL_EFS_ACCESS_POINT_ID`

The harness also accepts:

- `NFS_CRUST_EXTERNAL_PREFIX`
- `NFS_CRUST_EXTERNAL_CONNECT_TIMEOUT_SECONDS`
- `NFS_CRUST_EXTERNAL_OPERATION_TIMEOUT_SECONDS`
- `NFS_CRUST_EXTERNAL_LARGE_BYTES`
- `NFS_CRUST_EXTERNAL_DIRECTORY_ENTRIES`
- `NFS_CRUST_EXTERNAL_CONCURRENCY`

## AWS EFS interop

Use the project `efs-test` skill for the repeatable throwaway-EFS workflow. It
creates the AWS resources, reaches EFS through an SSM port-forward, runs the
ignored suite, and tears the resources down. The manual protocol remains in the
skill rather than being duplicated here.

EFS rejects `CLOSE` with the NFSv4.1 current-stateid sentinel even though it
accepts that sentinel for `WRITE`; the external suite covers the follow-up
deferred `CLOSE` using the stateid returned by `OPEN`.
