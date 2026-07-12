# Architecture Notes

`nfs-crust` presents a small operation API while keeping the protocol machinery
inside the client. Public callers ask for `put`, `get`, `get_range`, `delete`,
and `list_page`; internal code decides how to use NFSv4.1 `COMPOUND`, sessions,
stateids, file handles, `COMMIT`, and `CLOSE`.

## Client Model

An `NfsClient` is a virtual mount for one export. During connection setup the
client resolves the export root file handle, negotiates an NFSv4.1 session, and
uses session slots to bound concurrent protocol work.

RPC transport can be plaintext TCP or Rustls-backed TLS. TLS uses WebPKI trust
roots by default and can be extended with DER-encoded root certificates for
private CAs or tests. The TLS server name is explicit so callers can connect to
a resolved address while still verifying the DNS name in the server certificate.

AWS EFS IAM authorization is feature-gated behind `aws-efs`. It is implemented
as TLS client authentication: the application supplies an AWS credential
provider, and the transport generates a short-lived EFS client certificate for
each TLS connection. The NFS RPC layer remains AUTH_SYS, and non-EFS servers use
the same plaintext or generic TLS paths as before.

The client is cloneable. Public operations take a snapshot of the current
session handle for one attempt. Replay-safe phases reconnect and retry after a
transport or session failure. A publish-capable or public-remove compound is
not replayed when its reply may have been lost; it returns
`Error::OutcomeUnknown`, since replay could overwrite a concurrent writer,
turn a successful create into a false conflict, or delete a newly recreated
path. A pre-dispatch failure is instead marked `Error::RequestNotSent`, which
preserves the underlying retryability while proving that replay cannot duplicate
the operation. When multiple tasks notice the same lost generation, one detached
reconnect attempt at a time serves all of them, including the failure result;
configured rebuild retries share the same backoff. Reconnects resolve the
configured endpoint again and normally retain the same NFS client owner identity
while establishing the replacement session. If `SEQUENCE` reports revoked
client state, the shared owner advances to one new verifier/incarnation exactly
once while retaining its owner ID and monotonically unique open-owner counter.

## Operation Strategy

Operations are shaped around NFSv4.1 compounds:

- `get` resolves the file handle, the size, and the first `READ` in one
  compound. Files that fit the first read complete in that single round trip.
  Larger files keep the first chunk as a prefix, enforce the buffered-read cap
  against the resolved size, and read the remaining bytes from the same file
  handle with anonymous-state `READ` compounds, chunked and pipelined across
  available session slots. Continuing from the handle resolved alongside the
  size keeps one `get` on one file even if the path is concurrently replaced.
  Reads issued before the size is known — the `get` probe and the pipeline
  chunks — request `read_granularity` bytes (default 128 KiB, clamped to
  `read_chunk_size`) instead of a full `read_chunk_size` `READ`, because some
  servers (AWS EFS) price a `READ` by its requested count rather than the
  bytes returned. Known-size reads that fit one granule are issued at their
  exact size.
- `get_known_size` uses a caller-supplied size to enforce the buffered-read cap
  and read the file without a separate size lookup. This relies on the
  write-once invariant: once a path is written, its contents and size are
  immutable. A supplied zero size still resolves the path so it cannot turn a
  missing remote object into a successful empty read. The final `READ` EOF bit
  must agree with the supplied size; stale metadata produces
  `Error::FileSizeMismatch` rather than a partial success.
- `entry_info` resolves file handle, type, and size for a single path when
  callers need metadata outside the lean listing path.
- `get_range` avoids a size lookup when the requested range already fits the
  configured buffered-read limit. It first tries NFS anonymous-state `READ`:
  single-chunk ranges use a path lookup plus `READ` in one compound, while
  larger in-limit ranges resolve the file handle together with the first chunk
  `READ` in one compound and pipeline the remaining `PUTFH+READ` compounds.
  Ranges that need size validation resolve file handle and size in a single
  lookup compound, then use the same anonymous-state read path. Range reads
  stop at EOF and return the available bytes in the requested range.
- `put` writes to a guarded, uniquely named temporary file in the destination
  directory and atomically publishes it at the final name: `RENAME` for
  overwrite, or `LINK`+`REMOVE` for if-not-exists, where the `LINK` fails
  with `EXIST` if the destination is already present. Bodies that fit one
  write chunk use a single fused compound: guarded `OPEN` of the temporary,
  `GETFH`, `WRITE` through the compound's current stateid, `COMMIT`, a size
  `VERIFY` that halts the compound before publish on a short write, and the
  publish ops — durability, a server-side size check, and the atomic publish
  all in one round trip. The open state is closed by a follow-up compound
  using the stateid decoded from the reply, because some servers (AWS EFS)
  reject `CLOSE` with the special current stateid. Once publication succeeds,
  the state is queued on a bounded session worker; queued states are combined
  into multi-file `CLOSE` compounds, and a failed batch falls back to
  best-effort individual cleanup. Put completion does not wait for that cleanup
  because publication is already known to be durable. Deferred close processing
  is entirely internal to the session.
  Larger bodies open the temporary guarded, use the
  chunked/pipelined write path with unstable writes, and publish through a
  `COMMIT`+`VERIFY`+`CLOSE`+publish compound that also validates the unstable
  write verifier — no separate `COMMIT` round trip. Servers that lack part of
  the fused shape, bodies over negotiated request limits, missing parent
  directories, and transient statuses all fall back to the multi-compound
  path; unsupported-shape failures latch the fused shape off for the session.
  Transient status retries are limited to compounds whose completed prefix is
  safe to replay; a compound that may already have mutated server state is not
  blindly issued again.
  Failures after the temporary is created remove it, and temporaries orphaned
  by dead clients are named for discovery: `sweep_temp_files` judges age from
  the ULID embedded in each name using the directory listing alone.
- `delete` resolves the parent and removes the final component, which may be a
  file or empty directory. Missing paths are treated as already deleted. A lost
  REMOVE result is not replayed because the name could have been recreated
  concurrently.
- `list_page` fetches one directory-native `READDIR` page per successful
  attempt, with an empty attribute request, so entries carry only names and
  derived root-relative paths. It returns a directory-bound continuation token
  when the server or caller limit leaves more entries; callers drive all
  iteration.

Large reads and writes are chunked and pipelined across available session slots,
with an internal cap on per-operation pipeline depth. The public API remains
buffered: callers pass and receive `Bytes`, not protocol streams.

### AWS EFS Write Payload Boundary

TLS connections whose verified server name is an official AWS EFS or EFS FIPS
DNS name cap individual `WRITE` payloads at 504 KiB. Chunks for one logical file
are issued serially in offset order; independent operations still use the full
session slot table. Plaintext connections and generic TLS NFS servers retain
their negotiated write size and same-file chunk pipelining.

## Safety And Robustness Boundaries

The implementation treats raw file handles, stateids, open owners, sequence IDs,
and close sequencing as internal protocol state.

Important guardrails:

- Buffered reads are capped by default.
- RPC records plus read, write, and directory page sizes are bounded by
  negotiated channel limits.
- Timed-out or cancelled in-flight slots are retired rather than reused with
  uncertain sequence state. Cancellation while waiting for a slot does not
  alter session sequence state.
- Pending RPC calls fail promptly when the connection reader observes EOF or a
  malformed response, and cancelling an RPC removes its pending transaction ID.
- `SEQUENCE` replies are checked against the request and negotiated slot window;
  sequence IDs use the protocol's full wrapping `u32` space.
- Retryable NFS statuses such as `NFS4ERR_DELAY` and `NFS4ERR_GRACE` use bounded
  backoff only where replay is valid for the operation phase.
- Malformed reply lengths and counts are checked before allocation or repeated
  decoding. Negotiated request, operation-count, and 128-byte file-handle limits
  are enforced on every compound shape.
- Writes verify unstable-write verifiers before publishing data.

NFS RPC authentication support is AUTH_SYS. TLS protects the transport when
configured. EFS IAM authorization, when the `aws-efs` feature is enabled, is
handled as EFS-specific TLS client certificate material rather than as a public
NFS security flavor.

## Implementation Boundaries

The NFS implementation is divided by responsibility: `session` owns session
lifecycle and operations, `slots` owns NFSv4.1 sequencing, `ops` defines wire
operations, and `codec` handles compound encoding and decoding.

The code favors fewer round trips on known operation shapes, bounded allocation,
and predictable concurrency. Hot paths use fixed-shape compound encoders and
decoders where practical, reuse parsed path components by reference, and
resolve mutation parents inside the compound so path identity stays current
without adding a separate round trip. WRITE
requests retain the caller's `Bytes` as a scatter/gather payload between encoded
XDR prefix and suffix segments instead of copying it into the encoder. A
dedicated writer task per connection drains queued requests with one vectored
write per batch, so concurrent small operations share syscalls and TCP segments.

These are implementation choices, not public API commitments. The README and
crate documentation define the public contract.
