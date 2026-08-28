//! Atomic logical database manifests and publication compare-and-swap.

use ngkg_catalog::{CatalogError, OperationRepository};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use thiserror::Error;
use uuid::Uuid;

/// Immutable artifact reference.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HashedArtifact {
    pub uri: String,
    pub sha256: [u8; 32],
}

/// Exact physical table snapshot bound into NGKG publication.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TableSnapshot {
    pub table_uri: String,
    pub iceberg_snapshot_id: Option<i64>,
    pub metadata: HashedArtifact,
}

/// Complete bill of materials for one logical database.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotManifest {
    pub dataset_id: Uuid,
    pub snapshot_id: Uuid,
    pub parent_snapshot_id: Option<Uuid>,
    pub source_manifest: HashedArtifact,
    pub ontology_bundle: HashedArtifact,
    pub mapping_bundle: HashedArtifact,
    pub compiled_plan_directory: HashedArtifact,
    pub identity_registry_version: String,
    pub table_snapshots: Vec<TableSnapshot>,
    pub semantic_spine_root: HashedArtifact,
    pub dictionary_root: HashedArtifact,
    pub semantic_index_root: HashedArtifact,
    pub locator_root: HashedArtifact,
    pub proof_root: HashedArtifact,
    pub coverage_root: HashedArtifact,
    pub graph_routing_root: HashedArtifact,
    pub policy_hash: [u8; 32],
    pub expected_stage_manifests: Vec<HashedArtifact>,
    pub verification_report: HashedArtifact,
}

/// Publication failures leave the previous active snapshot untouched.
#[derive(Debug, Error)]
pub enum SnapshotError {
    #[error("snapshot manifest is incomplete: {0}")]
    Incomplete(&'static str),
    #[error("catalog dependency failed: {0}")]
    Catalog(#[source] CatalogError),
    #[error("active snapshot changed; publication compare-and-swap lost")]
    PublicationConflict,
    #[error("target snapshot is not certified")]
    NotCertified,
}

/// Validate required non-empty collections and identities before artifact verification.
pub fn validate_manifest(manifest: &SnapshotManifest) -> Result<(), SnapshotError> {
    if manifest.identity_registry_version.is_empty() {
        return Err(SnapshotError::Incomplete("identity_registry_version"));
    }
    if manifest.table_snapshots.is_empty() {
        return Err(SnapshotError::Incomplete("table_snapshots"));
    }
    if manifest.expected_stage_manifests.is_empty() {
        return Err(SnapshotError::Incomplete("expected_stage_manifests"));
    }
    Ok(())
}

/// Atomically expose a pre-certified manifest.
pub async fn publish(
    pool: &PgPool,
    tenant_id: Uuid,
    manifest: &SnapshotManifest,
) -> Result<(), SnapshotError> {
    validate_manifest(manifest)?;
    let repository = OperationRepository::new(pool.clone());
    match repository
        .publish_snapshot(
            tenant_id,
            manifest.dataset_id,
            manifest.snapshot_id,
            manifest.parent_snapshot_id,
            "ngkg-snapshot",
        )
        .await
    {
        Ok(_) => Ok(()),
        Err(CatalogError::PublicationConflict) => Err(SnapshotError::PublicationConflict),
        Err(
            CatalogError::NotFound
            | CatalogError::CertificationConflict
            | CatalogError::IllegalTransition { .. },
        ) => Err(SnapshotError::NotCertified),
        Err(error) => Err(SnapshotError::Catalog(error)),
    }
}
