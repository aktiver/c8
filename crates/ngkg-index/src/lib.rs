//! Versioned semantic-index roots and fail-closed open validation.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use uuid::Uuid;

/// Header that binds an index payload to its complete semantic context.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct IndexHeader {
    pub format_name: String,
    pub format_version: u32,
    pub dataset_id: Uuid,
    pub snapshot_id: Uuid,
    pub ontology_hash: [u8; 32],
    pub dictionary_hash: [u8; 32],
    pub policy_hash: [u8; 32],
    pub schema_hash: [u8; 32],
    pub partition_key: String,
    pub entry_count: u64,
    pub payload_sha256: [u8; 32],
}

/// Ordered families protect reducers from opening incomplete prerequisites.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum IndexFamily {
    Dictionaries,
    ClassExtents,
    PropertyExtents,
    HierarchyAndChains,
    GraphRouting,
    VirtualPlanDirectory,
    ProofAndDependency,
    GlobalLocator,
    Statistics,
}

/// Reasons an index cannot enter the query path.
#[derive(Debug, Error, Eq, PartialEq)]
pub enum IndexOpenError {
    #[error("snapshot mismatch: requested {requested}, found {found}")]
    SnapshotMismatch { requested: Uuid, found: Uuid },
    #[error("dataset mismatch: requested {requested}, found {found}")]
    DatasetMismatch { requested: Uuid, found: Uuid },
    #[error("unsupported index format {name} version {version}")]
    UnsupportedFormat { name: String, version: u32 },
    #[error("index payload checksum mismatch")]
    ChecksumMismatch,
    #[error("index semantic dependency hash mismatch: {0}")]
    SemanticDependency(&'static str),
}

/// Verify identity, semantic dependencies, format and bytes before serving.
pub fn verify_open(
    header: &IndexHeader,
    payload: &[u8],
    requested_dataset: Uuid,
    requested_snapshot: Uuid,
    ontology_hash: [u8; 32],
    dictionary_hash: [u8; 32],
    policy_hash: [u8; 32],
) -> Result<(), IndexOpenError> {
    if header.dataset_id != requested_dataset {
        return Err(IndexOpenError::DatasetMismatch { requested: requested_dataset, found: header.dataset_id });
    }
    if header.snapshot_id != requested_snapshot {
        return Err(IndexOpenError::SnapshotMismatch { requested: requested_snapshot, found: header.snapshot_id });
    }
    if header.format_name != "ngkg-index" || header.format_version != 1 {
        return Err(IndexOpenError::UnsupportedFormat { name: header.format_name.clone(), version: header.format_version });
    }
    if header.ontology_hash != ontology_hash {
        return Err(IndexOpenError::SemanticDependency("ontology"));
    }
    if header.dictionary_hash != dictionary_hash {
        return Err(IndexOpenError::SemanticDependency("dictionary"));
    }
    if header.policy_hash != policy_hash {
        return Err(IndexOpenError::SemanticDependency("policy"));
    }
    let observed: [u8; 32] = Sha256::digest(payload).into();
    if observed != header.payload_sha256 {
        return Err(IndexOpenError::ChecksumMismatch);
    }
    Ok(())
}

