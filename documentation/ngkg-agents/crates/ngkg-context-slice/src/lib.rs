//! Tenant-isolated immutable context slices and verified locator indexes.
//!
//! This crate is deliberately independent of the MCP gateway and NGKG query
//! implementation. The broker alone receives object-store credentials.

#![allow(missing_docs)]

mod capability;
mod index;
mod repository;
mod storage;

pub use capability::{CapabilityClaims, CapabilityIssuer, CapabilityRequest};
pub use index::{ChunkLocator, IndexLimits, VerifiedLocatorIndex, build_index};
pub use repository::{
    ChunkRecord, CreateSliceRequest, SliceManifest, SliceRepository, SliceState, SliceView,
};
pub use storage::{ContextObjectStore, ContextStoreConfiguration};

use thiserror::Error;

#[derive(Debug, Error)]
pub enum SliceError {
    #[error("context-slice configuration is invalid: {0}")]
    Configuration(&'static str),
    #[error("context-slice request is invalid: {0}")]
    Invalid(&'static str),
    #[error("context slice was not found")]
    NotFound,
    #[error("context slice state does not permit this operation")]
    State,
    #[error("context-slice authorization failed")]
    Unauthorized,
    #[error("context-slice checksum verification failed")]
    Checksum,
    #[error("context-slice integrity validation failed: {0}")]
    Integrity(&'static str),
    #[error("context-slice resource limit exceeded")]
    Limit,
    #[error("database operation failed")]
    Database(#[from] sqlx::Error),
    #[error("database migration failed")]
    Migration(#[from] sqlx::migrate::MigrateError),
    #[error("object-store operation failed")]
    Store(#[from] object_store::Error),
    #[error("object path is invalid")]
    ObjectPath(#[from] object_store::path::Error),
    #[error("capability processing failed")]
    Capability(#[from] jsonwebtoken::errors::Error),
    #[error("JSON processing failed")]
    Json(#[from] serde_json::Error),
    #[error("local index I/O failed")]
    Io(#[from] std::io::Error),
    #[error("bounded compute task failed")]
    ComputeTask,
}

pub fn sha256(bytes: &[u8]) -> String {
    use sha2::Digest;
    hex::encode(sha2::Sha256::digest(bytes))
}

pub fn valid_hash(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}
