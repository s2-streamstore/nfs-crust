use std::io;
use std::sync::Arc;
use std::time::Duration;

use crate::nfs4::{NfsStatus, OpCode, status};

#[derive(Debug, thiserror::Error)]
/// Errors returned by the NFS client.
pub enum Error {
    /// Local I/O error from socket or Tokio I/O operations.
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    /// ONC RPC framing or reply error.
    #[error("RPC error: {0}")]
    Rpc(String),

    /// The transport or NFS session was lost and may need to be rebuilt.
    #[error("connection lost: {0}")]
    ConnectionLost(Arc<str>),

    /// The request failed before it was handed to the RPC transport.
    ///
    /// Retrying is safe when the wrapped error is retryable because the
    /// server cannot have executed the operation.
    #[error("request was not sent: {source}")]
    RequestNotSent {
        /// Pre-dispatch failure that prevented the request from being sent.
        #[source]
        source: Box<Error>,
    },

    /// XDR decoding failed.
    #[error("XDR decode error: {0}")]
    Xdr(String),

    /// The NFS server returned a non-success status for an operation.
    #[error("NFS server returned status {status_code} during {operation}")]
    Nfs {
        /// Numeric NFSv4.1 status code returned by the server.
        status_code: u32,
        /// NFS operation that returned the status.
        operation: &'static str,
    },

    /// Protocol-level invariant violation.
    #[error("protocol error: {0}")]
    Protocol(String),

    /// Path input was invalid for a root-relative export path.
    #[error("invalid path: {0}")]
    InvalidPath(String),

    /// A create-new operation found an existing path.
    #[error("path already exists: {0}")]
    AlreadyExists(String),

    /// A mutating operation lost its reply, so its server-side outcome is unknown.
    ///
    /// Retrying such an operation could change the requested semantics. Callers
    /// should inspect the remote state or resolve the ambiguity explicitly.
    #[error("outcome of {operation} for {path:?} is unknown: {reason}")]
    OutcomeUnknown {
        /// Name of the operation whose result was lost.
        operation: &'static str,
        /// Root-relative path affected by the operation.
        path: String,
        /// Transport or session failure that made the outcome ambiguous.
        reason: Arc<str>,
    },

    /// A caller-supplied file size did not match the remote file.
    #[error("remote file size does not match caller-supplied size {expected} bytes")]
    FileSizeMismatch {
        /// Size supplied by the caller.
        expected: u64,
        /// Exact smaller size when EOF revealed it. `None` means the remote
        /// file was proven larger but its total size was not requested.
        actual: Option<u64>,
    },

    /// Client configuration failed validation.
    #[error("invalid client configuration: {0}")]
    InvalidConfig(String),

    /// The server returned a valid but unsupported response.
    #[error("unsupported server response: {0}")]
    Unsupported(String),

    /// A configured operation timeout elapsed.
    #[error("operation timed out after {0:?}")]
    Timeout(Duration),

    /// A buffered read would exceed the configured materialization limit.
    #[error("buffered read is too large: size {size:?} exceeds limit {limit} bytes")]
    BufferedReadTooLarge {
        /// Known or requested read size, when available.
        size: Option<u64>,
        /// Configured buffered read limit in bytes.
        limit: u64,
    },
}

impl Error {
    pub(crate) fn protocol(message: impl Into<String>) -> Self {
        Self::Protocol(message.into())
    }

    pub(crate) fn rpc(message: impl Into<String>) -> Self {
        Self::Rpc(message.into())
    }

    pub(crate) fn connection_lost(message: impl Into<Arc<str>>) -> Self {
        Self::ConnectionLost(message.into())
    }

    pub(crate) fn request_not_sent(source: Self) -> Self {
        match source {
            Self::RequestNotSent { .. } => source,
            source => Self::RequestNotSent {
                source: Box::new(source),
            },
        }
    }

    pub(crate) fn xdr(message: impl Into<String>) -> Self {
        Self::Xdr(message.into())
    }

    pub(crate) fn invalid_config(message: impl Into<String>) -> Self {
        Self::InvalidConfig(message.into())
    }

    pub(crate) fn already_exists(path: impl Into<String>) -> Self {
        Self::AlreadyExists(path.into())
    }

    pub(crate) fn outcome_unknown(
        operation: &'static str,
        path: impl Into<String>,
        reason: impl Into<Arc<str>>,
    ) -> Self {
        Self::OutcomeUnknown {
            operation,
            path: path.into(),
            reason: reason.into(),
        }
    }

    pub(crate) fn file_size_mismatch(expected: u64, actual: Option<u64>) -> Self {
        Self::FileSizeMismatch { expected, actual }
    }

    pub(crate) fn nfs(status: NfsStatus, op: OpCode) -> Self {
        Self::Nfs {
            status_code: status.code(),
            operation: op.name(),
        }
    }

    pub(crate) fn with_close_failure(self, close_err: Self) -> Self {
        let message = format!("{self}; additionally failed to close NFS file: {close_err}");
        if self.requires_session_rebuild() || close_err.requires_session_rebuild() {
            Self::connection_lost(message)
        } else if self.is_retryable() {
            self
        } else if close_err.is_retryable() {
            close_err
        } else {
            Self::protocol(message)
        }
    }

    pub(crate) fn is_nfs_status(&self, status: NfsStatus) -> bool {
        matches!(self, Self::Nfs { status_code, .. } if *status_code == status.code())
    }

    pub(crate) fn is_nfs_error(&self, status: NfsStatus, op: OpCode) -> bool {
        matches!(
            self,
            Self::Nfs {
                status_code,
                operation,
            } if *status_code == status.code() && *operation == op.name()
        )
    }

    pub(crate) fn is_nfs_operation(&self, op: OpCode) -> bool {
        matches!(
            self,
            Self::Nfs { operation, .. } if *operation == op.name()
        )
    }

    /// Returns true when retrying the operation may succeed.
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Io(_) | Self::ConnectionLost(_) | Self::Timeout(_) => true,
            Self::RequestNotSent { source } => source.is_retryable(),
            Self::Nfs { status_code, .. } => {
                matches!(
                    *status_code,
                    status::DELAY
                        | status::GRACE
                        | status::STALE_CLIENTID
                        | status::BADSESSION
                        | status::BADSLOT
                        | status::SEQ_MISORDERED
                        | status::BAD_HIGH_SLOT
                        | status::DEADSESSION
                )
            }
            _ => false,
        }
    }

    /// Returns true when retry requires replacing the current transport or
    /// NFS session, rather than waiting and issuing the operation again.
    pub(crate) fn requires_session_rebuild(&self) -> bool {
        match self {
            Self::Io(_) | Self::ConnectionLost(_) | Self::Timeout(_) => true,
            Self::RequestNotSent { source } => source.requires_session_rebuild(),
            Self::Nfs { status_code, .. } => matches!(
                *status_code,
                status::STALE_CLIENTID
                    | status::BADSESSION
                    | status::BADSLOT
                    | status::SEQ_MISORDERED
                    | status::BAD_HIGH_SLOT
                    | status::DEADSESSION
            ),
            _ => false,
        }
    }

    /// Returns true when the error represents a missing path.
    pub fn is_not_found(&self) -> bool {
        self.is_nfs_error(NfsStatus::NOENT, OpCode::Lookup)
            || self.is_nfs_error(NfsStatus::NOENT, OpCode::Open)
            || self.is_nfs_error(NfsStatus::NOENT, OpCode::Remove)
    }

    /// Returns true when the server denied access.
    pub fn is_permission_denied(&self) -> bool {
        matches!(
            self,
            Self::Nfs { status_code, .. }
                if matches!(*status_code, status::PERM | status::ACCESS | status::WRONGSEC)
        )
    }

    /// Returns true when a mutation may have succeeded but its reply was lost.
    pub fn is_outcome_unknown(&self) -> bool {
        matches!(self, Self::OutcomeUnknown { .. })
    }

    /// Returns true when the operation failed before RPC dispatch.
    pub fn is_request_not_sent(&self) -> bool {
        matches!(self, Self::RequestNotSent { .. })
    }

    /// Returns true when a buffered read exceeded the configured limit.
    pub fn is_buffered_read_too_large(&self) -> bool {
        matches!(self, Self::BufferedReadTooLarge { .. })
    }

    /// Returns the numeric NFSv4.1 status code for server-status errors.
    pub fn nfs_status_code(&self) -> Option<u32> {
        match self {
            Self::Nfs { status_code, .. } => Some(*status_code),
            _ => None,
        }
    }

    /// Returns the NFS operation name for server-status errors.
    pub fn nfs_operation(&self) -> Option<&'static str> {
        match self {
            Self::Nfs { operation, .. } => Some(*operation),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retryable_errors_are_classified() {
        assert!(Error::connection_lost("eof").is_retryable());
        assert!(Error::Timeout(Duration::from_secs(1)).is_retryable());
        assert!(Error::nfs(NfsStatus(10052), OpCode::Sequence).is_retryable());
        assert!(Error::nfs(NfsStatus::DELAY, OpCode::Open).is_retryable());
        assert!(Error::nfs(NfsStatus::GRACE, OpCode::Open).is_retryable());
        assert!(!Error::nfs(NfsStatus::DELAY, OpCode::Open).requires_session_rebuild());
        assert!(!Error::nfs(NfsStatus::GRACE, OpCode::Open).requires_session_rebuild());
        assert!(!Error::nfs(NfsStatus(2), OpCode::Lookup).is_retryable());

        let retryable = Error::request_not_sent(Error::connection_lost("closed"));
        assert!(retryable.is_request_not_sent());
        assert!(retryable.is_retryable());
        let terminal = Error::request_not_sent(Error::protocol("too large"));
        assert!(terminal.is_request_not_sent());
        assert!(!terminal.is_retryable());
    }

    #[test]
    fn close_failures_preserve_retryability() {
        let retryable =
            Error::connection_lost("read eof").with_close_failure(Error::protocol("close failed"));
        assert!(retryable.is_retryable());

        let retryable =
            Error::protocol("read failed").with_close_failure(Error::connection_lost("close eof"));
        assert!(retryable.is_retryable());

        let non_retryable =
            Error::protocol("read failed").with_close_failure(Error::protocol("close failed"));
        assert!(!non_retryable.is_retryable());

        let retryable = Error::nfs(NfsStatus::GRACE, OpCode::Write)
            .with_close_failure(Error::protocol("close failed"));
        assert!(retryable.is_retryable());
        assert!(!retryable.requires_session_rebuild());
    }

    #[test]
    fn common_error_helpers_hide_protocol_details() {
        let not_found = Error::nfs(NfsStatus(2), OpCode::Lookup);
        assert!(not_found.is_not_found());
        assert_eq!(not_found.nfs_status_code(), Some(2));
        assert_eq!(not_found.nfs_operation(), Some("LOOKUP"));

        let denied = Error::nfs(NfsStatus(13), OpCode::Open);
        assert!(denied.is_permission_denied());
        assert!(matches!(
            Error::InvalidPath("bad".to_owned()),
            Error::InvalidPath(_)
        ));
        let unknown = Error::outcome_unknown("create", "objects/new", "connection closed");
        assert!(unknown.is_outcome_unknown());
        assert!(!unknown.is_retryable());
        assert!(matches!(
            Error::file_size_mismatch(8, Some(5)),
            Error::FileSizeMismatch { .. }
        ));
        assert!(
            Error::BufferedReadTooLarge {
                size: Some(8),
                limit: 4,
            }
            .is_buffered_read_too_large()
        );
    }
}
