#![doc = include_str!("../README.md")]
#![warn(missing_docs)]

#[cfg(feature = "aws-efs")]
mod aws_efs;
mod client;
mod error;
mod nfs4;
mod path;
mod rpc;
mod xdr;

#[cfg(feature = "aws-efs")]
pub use aws_efs::EfsIamConfig;
pub use client::{
    ContinuationToken, EntryInfo, EntryKind, ListEntry, ListResult, NfsClient, NfsClientBuilder,
    PutMode,
};
pub use error::Error;
pub use rpc::{AuthSys, TlsConfig};
