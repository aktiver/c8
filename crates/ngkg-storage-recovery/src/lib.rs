//! Deterministic, fail-closed storage replication and recovery primitives.
//!
//! Snapshot artifacts are immutable. Recovery creates and verifies replacement
//! replicas before a catalog barrier can make them readable; it never repairs by
//! mutating a published object in place.

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    path::{Path, PathBuf},
};

use ngkg_artifact_store::{ArtifactStore, ArtifactStoreError};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Row};
use thiserror::Error;
use uuid::Uuid;

/// Version of the storage recovery wire contracts.
pub const STORAGE_RECOVERY_FORMAT_VERSION: u32 = 1;

/// A trusted operator-configured storage destination.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct StorageTarget {
    /// Stable DNS-label target name, never a credential or user supplied URL.
    pub name: String,
    /// Zone, rack, region, or independent object-store failure domain.
    pub failure_domain: String,
    /// Operator-owned object-store base URL.
    pub base_url: String,
    /// False targets remain readable but receive no new replicas.
    pub writable: bool,
}

/// One immutable object reachable from a certified snapshot root.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct SnapshotArtifact {
    /// Normalized source object key.
    pub object_key: String,
    /// Exact lowercase SHA-256 digest.
    pub sha256: String,
    /// Exact byte length.
    pub bytes: u64,
}

/// Checksum-bound closure of every object needed to restore one snapshot.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct SnapshotStorageManifest {
    /// Contract version.
    pub format_version: u32,
    /// Tenant boundary.
    pub tenant_id: Uuid,
    /// Dataset boundary.
    pub dataset_id: Uuid,
    /// Immutable snapshot identity.
    pub snapshot_id: Uuid,
    /// Catalog snapshot-manifest digest.
    pub snapshot_manifest_sha256: String,
    /// Snapshot activation digest when the cloud compiler path produced the snapshot.
    pub activation_manifest_sha256: Option<String>,
    /// Complete, sorted, duplicate-free artifact closure.
    pub artifacts: Vec<SnapshotArtifact>,
}

/// Why a deterministic transfer exists.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum TransferReason {
    /// Establish the requested replica count.
    Replication,
    /// Replace a replica before retiring its former location.
    Relocation,
    /// Repair capacity after a storage node or failure domain disappeared.
    NodeLoss,
    /// Replace a quarantined corrupt replica.
    ChecksumRepair,
    /// Copy snapshot artifacts into an independent backup target.
    Backup,
    /// Copy a verified backup into an inactive restore namespace.
    Restore,
}

/// Durable storage operation type.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum StorageOperationKind {
    /// Establish the configured replica factor.
    Replicate,
    /// Move a healthy replica to a new target.
    Relocate,
    /// Repair replicas after a node or failure-domain loss.
    NodeLoss,
    /// Create an independent backup.
    Backup,
    /// Restore a backup to an inactive snapshot.
    Restore,
}

impl StorageOperationKind {
    fn as_db(self) -> &'static str {
        match self {
            Self::Replicate => "REPLICATE",
            Self::Relocate => "RELOCATE",
            Self::NodeLoss => "NODE_LOSS",
            Self::Backup => "BACKUP",
            Self::Restore => "RESTORE",
        }
    }
}

/// Durable projection returned for idempotent API retries.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct StorageOperationRecord {
    /// Retry-stable operation ID.
    pub operation_id: Uuid,
    /// Dataset ID.
    pub dataset_id: Uuid,
    /// Source snapshot.
    pub source_snapshot_id: Uuid,
    /// New snapshot for restore, otherwise absent.
    pub restored_snapshot_id: Option<Uuid>,
    /// Operation kind.
    pub kind: StorageOperationKind,
    /// Durable state.
    pub state: String,
    /// Exact plan key.
    pub plan_object_key: String,
    /// Exact plan digest.
    pub plan_sha256: String,
    /// Dense work count.
    pub task_count: u32,
    /// Revision incremented on state transitions.
    pub revision: i64,
    /// Non-sensitive terminal failure code, when failed.
    pub error_code: Option<String>,
}

/// Immutable registration written before Kubernetes desired state is created.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RegisterStorageOperation {
    /// Operation ID derived from the full idempotent request.
    pub operation_id: Uuid,
    /// Dataset ID.
    pub dataset_id: Uuid,
    /// Source snapshot.
    pub source_snapshot_id: Uuid,
    /// Restore destination identity.
    pub restored_snapshot_id: Option<Uuid>,
    /// Operation kind.
    pub kind: StorageOperationKind,
    /// Exact plan key.
    pub plan_object_key: String,
    /// Exact plan checksum.
    pub plan_sha256: [u8; 32],
    /// Dense transfer count.
    pub task_count: u32,
    /// Shared transfer byte ceiling.
    pub max_in_flight_bytes: u64,
}

/// Catalog errors specific to storage recovery registration.
#[derive(Debug, Error)]
pub enum StorageCatalogError {
    /// PostgreSQL failed.
    #[error("storage catalog failed: {0}")]
    Database(#[from] sqlx::Error),
    /// Dataset or snapshot was not found under the tenant.
    #[error("storage recovery source does not exist")]
    NotFound,
    /// The idempotency key is bound to different request bytes.
    #[error("storage recovery idempotency conflict")]
    IdempotencyConflict,
    /// A catalog value is outside the closed vocabulary.
    #[error("storage recovery catalog state is invalid")]
    InvalidCatalogState,
}

/// Tenant-isolated durable recovery operation repository.
#[derive(Clone)]
pub struct StorageRecoveryRepository {
    pool: PgPool,
}

/// Catalog pointer to one fully certified backup manifest.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BackupRecord {
    /// Backup identity.
    pub backup_id: Uuid,
    /// Owning dataset.
    pub dataset_id: Uuid,
    /// Original snapshot.
    pub source_snapshot_id: Uuid,
    /// Registered storage target holding the backup.
    pub destination_target: String,
    /// Exact backup manifest key.
    pub backup_manifest_object_key: String,
    /// Exact backup manifest digest.
    pub backup_manifest_sha256: String,
}

impl StorageRecoveryRepository {
    /// Construct the repository.
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Register or return the exact prior request for an idempotent retry.
    pub async fn create_or_get(
        &self,
        tenant_id: Uuid,
        idempotency_key: &str,
        request_sha256: &[u8; 32],
        request: &RegisterStorageOperation,
    ) -> Result<StorageOperationRecord, StorageCatalogError> {
        let task_count = i32::try_from(request.task_count)
            .map_err(|_| StorageCatalogError::InvalidCatalogState)?;
        let max_in_flight_bytes = i64::try_from(request.max_in_flight_bytes)
            .map_err(|_| StorageCatalogError::InvalidCatalogState)?;
        let mut tx = self.pool.begin().await?;
        sqlx::query("SELECT set_config('ngkg.tenant_id', $1, true)")
            .bind(tenant_id.to_string())
            .execute(&mut *tx)
            .await?;
        let source_exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM snapshot WHERE tenant_id=$1 AND dataset_id=$2 AND snapshot_id=$3)",
        )
        .bind(tenant_id)
        .bind(request.dataset_id)
        .bind(request.source_snapshot_id)
        .fetch_one(&mut *tx)
        .await?;
        if !source_exists {
            return Err(StorageCatalogError::NotFound);
        }
        sqlx::query(
            "INSERT INTO storage_recovery_operation \
             (tenant_id,operation_id,dataset_id,source_snapshot_id,restored_snapshot_id,kind, \
              idempotency_key,request_sha256,plan_object_key,plan_sha256,task_count,max_in_flight_bytes,state) \
             VALUES ($1,$2,$3,$4,$5,$6::ngkg_storage_operation_kind,$7,$8,$9,$10,$11,$12,'PLANNED') \
             ON CONFLICT (tenant_id,idempotency_key) DO NOTHING",
        )
        .bind(tenant_id)
        .bind(request.operation_id)
        .bind(request.dataset_id)
        .bind(request.source_snapshot_id)
        .bind(request.restored_snapshot_id)
        .bind(request.kind.as_db())
        .bind(idempotency_key)
        .bind(request_sha256.as_slice())
        .bind(&request.plan_object_key)
        .bind(request.plan_sha256.as_slice())
        .bind(task_count)
        .bind(max_in_flight_bytes)
        .execute(&mut *tx)
        .await?;
        let row = sqlx::query(
            "SELECT operation_id,dataset_id,source_snapshot_id,restored_snapshot_id,kind::text AS kind, \
                    state::text AS state,plan_object_key,encode(plan_sha256,'hex') AS plan_sha256, \
                    task_count,revision,error_code,encode(request_sha256,'hex') AS request_sha256 \
             FROM storage_recovery_operation WHERE tenant_id=$1 AND idempotency_key=$2",
        )
        .bind(tenant_id)
        .bind(idempotency_key)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(StorageCatalogError::NotFound)?;
        let observed_request_sha256: String = row.try_get("request_sha256")?;
        if observed_request_sha256 != hex::encode(request_sha256)
            || row.try_get::<Uuid, _>("operation_id")? != request.operation_id
            || row.try_get::<String, _>("plan_object_key")? != request.plan_object_key
            || row.try_get::<String, _>("plan_sha256")? != hex::encode(request.plan_sha256)
        {
            return Err(StorageCatalogError::IdempotencyConflict);
        }
        let record = storage_record_from_row(&row)?;
        tx.commit().await?;
        Ok(record)
    }

    /// Advance a recovery operation through the closed nonterminal state machine.
    pub async fn transition(
        &self,
        tenant_id: Uuid,
        operation_id: Uuid,
        expected: &str,
        next: &str,
    ) -> Result<(), StorageCatalogError> {
        if !matches!(
            (expected, next),
            ("PLANNED", "RUNNING") | ("RUNNING", "VERIFYING") | ("PLANNED", "VERIFYING")
        ) {
            return Err(StorageCatalogError::InvalidCatalogState);
        }
        let mut tx = self.pool.begin().await?;
        sqlx::query("SELECT set_config('ngkg.tenant_id', $1, true)")
            .bind(tenant_id.to_string())
            .execute(&mut *tx)
            .await?;
        let affected = sqlx::query(
            "UPDATE storage_recovery_operation SET state=$1::ngkg_storage_operation_state, \
                    revision=revision+1,updated_at=now() \
             WHERE tenant_id=$2 AND operation_id=$3 AND state=$4::ngkg_storage_operation_state",
        )
        .bind(next)
        .bind(tenant_id)
        .bind(operation_id)
        .bind(expected)
        .execute(&mut *tx)
        .await?
        .rows_affected();
        if affected == 0 {
            let state: Option<String> = sqlx::query_scalar(
                "SELECT state::text FROM storage_recovery_operation WHERE tenant_id=$1 AND operation_id=$2",
            )
            .bind(tenant_id)
            .bind(operation_id)
            .fetch_optional(&mut *tx)
            .await?;
            if state.as_deref() != Some(next) {
                return Err(state.map_or(StorageCatalogError::NotFound, |_| {
                    StorageCatalogError::IdempotencyConflict
                }));
            }
        }
        tx.commit().await?;
        Ok(())
    }

    /// Persist a terminal fail-closed outcome. Reconciliation retries with the
    /// same error are idempotent; a different terminal outcome is rejected.
    pub async fn fail(
        &self,
        tenant_id: Uuid,
        operation_id: Uuid,
        error_code: &str,
    ) -> Result<(), StorageCatalogError> {
        if error_code.is_empty()
            || error_code.len() > 128
            || !error_code
                .bytes()
                .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
        {
            return Err(StorageCatalogError::InvalidCatalogState);
        }
        let mut tx = self.pool.begin().await?;
        sqlx::query("SELECT set_config('ngkg.tenant_id', $1, true)")
            .bind(tenant_id.to_string())
            .execute(&mut *tx)
            .await?;
        let affected = sqlx::query(
            "UPDATE storage_recovery_operation SET state='FAILED',error_code=$1, \
                    revision=revision+1,updated_at=now() \
             WHERE tenant_id=$2 AND operation_id=$3 \
               AND state IN ('PLANNED','RUNNING','VERIFYING')",
        )
        .bind(error_code)
        .bind(tenant_id)
        .bind(operation_id)
        .execute(&mut *tx)
        .await?
        .rows_affected();
        if affected == 0 {
            let row = sqlx::query(
                "SELECT state::text AS state,error_code FROM storage_recovery_operation \
                 WHERE tenant_id=$1 AND operation_id=$2",
            )
            .bind(tenant_id)
            .bind(operation_id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(StorageCatalogError::NotFound)?;
            let state: String = row.try_get("state")?;
            let observed: Option<String> = row.try_get("error_code")?;
            if state != "FAILED" || observed.as_deref() != Some(error_code) {
                return Err(StorageCatalogError::IdempotencyConflict);
            }
        }
        tx.commit().await?;
        Ok(())
    }

    /// Resolve a completed backup inside the authenticated tenant.
    pub async fn get_backup(
        &self,
        tenant_id: Uuid,
        backup_id: Uuid,
    ) -> Result<BackupRecord, StorageCatalogError> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("SELECT set_config('ngkg.tenant_id', $1, true)")
            .bind(tenant_id.to_string())
            .execute(&mut *tx)
            .await?;
        let row = sqlx::query(
            "SELECT backup_id,dataset_id,source_snapshot_id,destination_target, \
                    backup_manifest_object_key,encode(backup_manifest_sha256,'hex') AS backup_manifest_sha256 \
             FROM snapshot_backup WHERE tenant_id=$1 AND backup_id=$2",
        )
        .bind(tenant_id)
        .bind(backup_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(StorageCatalogError::NotFound)?;
        let record = BackupRecord {
            backup_id: row.try_get("backup_id")?,
            dataset_id: row.try_get("dataset_id")?,
            source_snapshot_id: row.try_get("source_snapshot_id")?,
            destination_target: row.try_get("destination_target")?,
            backup_manifest_object_key: row.try_get("backup_manifest_object_key")?,
            backup_manifest_sha256: row.try_get("backup_manifest_sha256")?,
        };
        tx.commit().await?;
        Ok(record)
    }

    /// Read one tenant-scoped durable operation without depending on Kubernetes state.
    pub async fn get_operation(
        &self,
        tenant_id: Uuid,
        operation_id: Uuid,
    ) -> Result<StorageOperationRecord, StorageCatalogError> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("SELECT set_config('ngkg.tenant_id', $1, true)")
            .bind(tenant_id.to_string())
            .execute(&mut *tx)
            .await?;
        let row = sqlx::query(
            "SELECT operation_id,dataset_id,source_snapshot_id,restored_snapshot_id,kind::text AS kind, \
                    state::text AS state,plan_object_key,encode(plan_sha256,'hex') AS plan_sha256, \
                    task_count,revision,error_code \
             FROM storage_recovery_operation WHERE tenant_id=$1 AND operation_id=$2",
        )
        .bind(tenant_id)
        .bind(operation_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(StorageCatalogError::NotFound)?;
        let record = storage_record_from_row(&row)?;
        tx.commit().await?;
        Ok(record)
    }

    /// Record the already checksum-verified primary objects that seed a recovery plan.
    pub async fn register_primary_replicas(
        &self,
        tenant_id: Uuid,
        operation_id: Uuid,
        manifest: &SnapshotStorageManifest,
        target: &StorageTarget,
    ) -> Result<(), StorageCatalogError> {
        validate_storage_manifest(manifest).map_err(|_| StorageCatalogError::InvalidCatalogState)?;
        let mut tx = self.pool.begin().await?;
        sqlx::query("SELECT set_config('ngkg.tenant_id', $1, true)")
            .bind(tenant_id.to_string())
            .execute(&mut *tx)
            .await?;
        for artifact in &manifest.artifacts {
            let digest = decode_digest(&artifact.sha256)?;
            let bytes = i64::try_from(artifact.bytes)
                .map_err(|_| StorageCatalogError::InvalidCatalogState)?;
            sqlx::query(
                "INSERT INTO snapshot_artifact_replica \
                 (tenant_id,dataset_id,snapshot_id,artifact_sha256,artifact_bytes,storage_target, \
                  failure_domain,object_key,state,verified_at,recovery_operation_id) \
                 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'READY',now(),$9) ON CONFLICT DO NOTHING",
            )
            .bind(tenant_id)
            .bind(manifest.dataset_id)
            .bind(manifest.snapshot_id)
            .bind(digest.as_slice())
            .bind(bytes)
            .bind(&target.name)
            .bind(&target.failure_domain)
            .bind(&artifact.object_key)
            .bind(operation_id)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    /// Atomically expose verified replica rows and the all-partitions certificate.
    pub async fn commit_success(
        &self,
        tenant_id: Uuid,
        operation_id: Uuid,
        certificate_object_key: &str,
        certificate_sha256: &[u8; 32],
        plan: &RecoveryPlan,
        targets: &[StorageTarget],
        backup: Option<(&str, &[u8; 32])>,
    ) -> Result<(), StorageCatalogError> {
        let domains = targets
            .iter()
            .map(|target| (target.name.as_str(), target.failure_domain.as_str()))
            .collect::<BTreeMap<_, _>>();
        let mut tx = self.pool.begin().await?;
        sqlx::query("SELECT set_config('ngkg.tenant_id', $1, true)")
            .bind(tenant_id.to_string())
            .execute(&mut *tx)
            .await?;
        let row = sqlx::query(
            "SELECT state::text AS state,dataset_id,source_snapshot_id,kind::text AS kind, \
                    recovery_certificate_object_key,encode(recovery_certificate_sha256,'hex') AS recovery_certificate_sha256 \
             FROM storage_recovery_operation WHERE tenant_id=$1 AND operation_id=$2 FOR UPDATE",
        )
        .bind(tenant_id)
        .bind(operation_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(StorageCatalogError::NotFound)?;
        let state: String = row.try_get("state")?;
        if state == "SUCCEEDED" {
            let existing_key: Option<String> = row.try_get("recovery_certificate_object_key")?;
            let existing_sha: Option<String> = row.try_get("recovery_certificate_sha256")?;
            if existing_key.as_deref() == Some(certificate_object_key)
                && existing_sha.as_deref() == Some(hex::encode(certificate_sha256).as_str())
            {
                tx.commit().await?;
                return Ok(());
            }
            return Err(StorageCatalogError::IdempotencyConflict);
        }
        if state != "VERIFYING" {
            return Err(StorageCatalogError::IdempotencyConflict);
        }
        let dataset_id: Uuid = row.try_get("dataset_id")?;
        let source_snapshot_id: Uuid = row.try_get("source_snapshot_id")?;
        let kind: String = row.try_get("kind")?;
        if !matches!(kind.as_str(), "BACKUP" | "RESTORE") {
            for task in &plan.tasks {
                let domain = domains
                    .get(task.destination_target.as_str())
                    .ok_or(StorageCatalogError::InvalidCatalogState)?;
                let digest = decode_digest(&task.sha256)?;
                let bytes = i64::try_from(task.bytes)
                    .map_err(|_| StorageCatalogError::InvalidCatalogState)?;
                sqlx::query(
                    "INSERT INTO snapshot_artifact_replica \
                     (tenant_id,dataset_id,snapshot_id,artifact_sha256,artifact_bytes,storage_target, \
                      failure_domain,object_key,state,verified_at,recovery_operation_id) \
                     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'READY',now(),$9) \
                     ON CONFLICT DO NOTHING",
                )
                .bind(tenant_id)
                .bind(dataset_id)
                .bind(source_snapshot_id)
                .bind(digest.as_slice())
                .bind(bytes)
                .bind(&task.destination_target)
                .bind(*domain)
                .bind(&task.destination_object_key)
                .bind(operation_id)
                .execute(&mut *tx)
                .await?;
            }
        }
        if kind == "RELOCATE" {
            for task in &plan.tasks {
                let digest = decode_digest(&task.sha256)?;
                sqlx::query(
                    "UPDATE snapshot_artifact_replica old SET state='RETIRING' \
                     WHERE old.tenant_id=$1 AND old.dataset_id=$2 AND old.snapshot_id=$3 \
                       AND old.artifact_sha256=$4 AND old.storage_target=$5 AND old.state='READY' \
                       AND EXISTS (SELECT 1 FROM snapshot_artifact_replica replacement \
                         WHERE replacement.tenant_id=old.tenant_id \
                           AND replacement.dataset_id=old.dataset_id \
                           AND replacement.snapshot_id=old.snapshot_id \
                           AND replacement.artifact_sha256=old.artifact_sha256 \
                           AND replacement.storage_target=$6 AND replacement.state='READY')",
                )
                .bind(tenant_id)
                .bind(dataset_id)
                .bind(source_snapshot_id)
                .bind(digest.as_slice())
                .bind(&task.source_target)
                .bind(&task.destination_target)
                .execute(&mut *tx)
                .await?;
            }
        }
        if let Some((manifest_key, manifest_sha256)) = backup {
            sqlx::query(
                "INSERT INTO snapshot_backup \
                 (tenant_id,backup_id,dataset_id,source_snapshot_id,operation_id,destination_target, \
                  backup_manifest_object_key,backup_manifest_sha256) \
                 VALUES ($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT (tenant_id,operation_id) DO NOTHING",
            )
            .bind(tenant_id)
            .bind(operation_id)
            .bind(dataset_id)
            .bind(source_snapshot_id)
            .bind(operation_id)
            .bind(
                plan.tasks
                    .first()
                    .map(|task| task.destination_target.as_str())
                    .ok_or(StorageCatalogError::InvalidCatalogState)?,
            )
            .bind(manifest_key)
            .bind(manifest_sha256.as_slice())
            .execute(&mut *tx)
            .await?;
        }
        sqlx::query(
            "UPDATE storage_recovery_operation SET state='SUCCEEDED',revision=revision+1,updated_at=now(), \
                    recovery_certificate_object_key=$1,recovery_certificate_sha256=$2 \
             WHERE tenant_id=$3 AND operation_id=$4 AND state='VERIFYING'",
        )
        .bind(certificate_object_key)
        .bind(certificate_sha256.as_slice())
        .bind(tenant_id)
        .bind(operation_id)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }
}

fn decode_digest(value: &str) -> Result<[u8; 32], StorageCatalogError> {
    if !valid_sha256(value) {
        return Err(StorageCatalogError::InvalidCatalogState);
    }
    let decoded = hex::decode(value).map_err(|_| StorageCatalogError::InvalidCatalogState)?;
    let mut digest = [0_u8; 32];
    digest.copy_from_slice(&decoded);
    Ok(digest)
}

fn storage_record_from_row(
    row: &sqlx::postgres::PgRow,
) -> Result<StorageOperationRecord, StorageCatalogError> {
    let kind = match row.try_get::<String, _>("kind")?.as_str() {
        "REPLICATE" => StorageOperationKind::Replicate,
        "RELOCATE" => StorageOperationKind::Relocate,
        "NODE_LOSS" => StorageOperationKind::NodeLoss,
        "BACKUP" => StorageOperationKind::Backup,
        "RESTORE" => StorageOperationKind::Restore,
        _ => return Err(StorageCatalogError::InvalidCatalogState),
    };
    let task_count = u32::try_from(row.try_get::<i32, _>("task_count")?)
        .map_err(|_| StorageCatalogError::InvalidCatalogState)?;
    Ok(StorageOperationRecord {
        operation_id: row.try_get("operation_id")?,
        dataset_id: row.try_get("dataset_id")?,
        source_snapshot_id: row.try_get("source_snapshot_id")?,
        restored_snapshot_id: row.try_get("restored_snapshot_id")?,
        kind,
        state: row.try_get("state")?,
        plan_object_key: row.try_get("plan_object_key")?,
        plan_sha256: row.try_get("plan_sha256")?,
        task_count,
        revision: row.try_get("revision")?,
        error_code: row.try_get("error_code")?,
    })
}

/// One retry-stable, independently executable transfer.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct TransferTask {
    /// Dense completion index used by a Kubernetes Indexed Job.
    pub task_index: u32,
    /// BLAKE3 identity over every semantic input to this task.
    pub stable_work_id: String,
    /// Transfer intent.
    pub reason: TransferReason,
    /// Trusted source target name.
    pub source_target: String,
    /// Trusted destination target name.
    pub destination_target: String,
    /// Immutable source object key.
    pub source_object_key: String,
    /// Immutable destination object key. It includes the recovery operation ID.
    pub destination_object_key: String,
    /// Expected content digest at both ends.
    pub sha256: String,
    /// Exact object size and per-task transfer ceiling.
    pub bytes: u64,
}

/// Immutable distributed transfer plan.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct RecoveryPlan {
    /// Contract version.
    pub format_version: u32,
    /// Retry-stable operation identity.
    pub operation_id: Uuid,
    /// Tenant boundary.
    pub tenant_id: Uuid,
    /// Dataset boundary.
    pub dataset_id: Uuid,
    /// Source snapshot.
    pub snapshot_id: Uuid,
    /// Exact storage manifest digest.
    pub storage_manifest_sha256: String,
    /// Required ready replicas per artifact after the completion barrier.
    pub replication_factor: u16,
    /// Maximum aggregate bytes admitted across concurrently running tasks.
    pub max_in_flight_bytes: u64,
    /// Dense, retry-stable Indexed Job work.
    pub tasks: Vec<TransferTask>,
}

/// Terminal state reported by one transfer attempt.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TransferState {
    /// Destination bytes and checksum were verified.
    Succeeded,
    /// Destination or source returned bytes with the wrong checksum.
    Quarantined,
    /// A retryable infrastructure failure occurred; no success is implied.
    RetryableFailure,
    /// A deterministic contract failure occurred.
    PermanentFailure,
}

/// Checksum-bound completion for one partition.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct TransferResult {
    /// Plan operation identity.
    pub operation_id: Uuid,
    /// Dense task index.
    pub task_index: u32,
    /// Stable work identity copied from the plan.
    pub stable_work_id: String,
    /// Terminal state.
    pub state: TransferState,
    /// Observed destination digest only after a complete read-back.
    pub observed_sha256: Option<String>,
    /// Exact bytes copied when successful.
    pub copied_bytes: u64,
    /// Non-sensitive machine-readable failure code.
    pub error_code: Option<String>,
}

/// Aggregate barrier proving that every transfer partition completed exactly once.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct RecoveryCertificate {
    /// Contract version.
    pub format_version: u32,
    /// Recovery operation.
    pub operation_id: Uuid,
    /// Exact plan digest.
    pub plan_sha256: String,
    /// Snapshot protected by the operation.
    pub snapshot_id: Uuid,
    /// Complete number of verified tasks.
    pub verified_task_count: u32,
    /// Total verified bytes.
    pub verified_bytes: u64,
    /// Digest of ordered transfer-result digests.
    pub result_set_sha256: String,
    /// Always true; partial certificates are rejected.
    pub complete: bool,
}

/// Constant-memory, dense-order completion barrier for very large recovery plans.
pub struct RecoveryCertificationAccumulator {
    operation_id: Uuid,
    snapshot_id: Uuid,
    plan_sha256: String,
    expected_task_count: u32,
    next_task_index: u32,
    verified_bytes: u64,
    result_hash: Sha256,
}

impl RecoveryCertificationAccumulator {
    /// Start verification for one exact immutable plan.
    pub fn new(plan: &RecoveryPlan, plan_sha256: &str) -> Result<Self, RecoveryError> {
        validate_plan(plan)?;
        if !valid_sha256(plan_sha256) {
            return Err(RecoveryError::InvalidContract(
                "recovery plan checksum is invalid".to_owned(),
            ));
        }
        let expected_task_count = u32::try_from(plan.tasks.len()).map_err(|_| {
            RecoveryError::InvalidContract("verified task count exceeds u32".to_owned())
        })?;
        let mut result_hash = Sha256::new();
        result_hash.update(b"ngkg-storage-recovery-results-v1\0");
        Ok(Self {
            operation_id: plan.operation_id,
            snapshot_id: plan.snapshot_id,
            plan_sha256: plan_sha256.to_owned(),
            expected_task_count,
            next_task_index: 0,
            verified_bytes: 0,
            result_hash,
        })
    }

    /// Verify one exact dense result and fold its digest into the certificate.
    pub fn observe(
        &mut self,
        task: &TransferTask,
        result: &TransferResult,
    ) -> Result<(), RecoveryError> {
        if task.task_index != self.next_task_index
            || result.task_index != task.task_index
            || result.operation_id != self.operation_id
            || result.stable_work_id != task.stable_work_id
            || result.state != TransferState::Succeeded
            || result.observed_sha256.as_deref() != Some(task.sha256.as_str())
            || result.copied_bytes != task.bytes
            || result.error_code.is_some()
        {
            return Err(RecoveryError::Incomplete(format!(
                "task {} is not a verified dense success",
                task.task_index
            )));
        }
        self.verified_bytes = self
            .verified_bytes
            .checked_add(result.copied_bytes)
            .ok_or_else(|| {
                RecoveryError::InvalidContract("verified byte total overflow".to_owned())
            })?;
        let encoded = serde_json::to_vec(result).map_err(|error| {
            RecoveryError::InvalidContract(format!("result cannot be encoded: {error}"))
        })?;
        self.result_hash.update(Sha256::digest(encoded));
        self.next_task_index = self.next_task_index.checked_add(1).ok_or_else(|| {
            RecoveryError::InvalidContract("verified task index overflow".to_owned())
        })?;
        Ok(())
    }

    /// Complete only after every planned partition has been observed exactly once.
    pub fn finish(self) -> Result<RecoveryCertificate, RecoveryError> {
        if self.next_task_index != self.expected_task_count {
            return Err(RecoveryError::Incomplete(
                "recovery result stream ended before the complete plan".to_owned(),
            ));
        }
        Ok(RecoveryCertificate {
            format_version: STORAGE_RECOVERY_FORMAT_VERSION,
            operation_id: self.operation_id,
            plan_sha256: self.plan_sha256,
            snapshot_id: self.snapshot_id,
            verified_task_count: self.expected_task_count,
            verified_bytes: self.verified_bytes,
            result_set_sha256: hex::encode(self.result_hash.finalize()),
            complete: true,
        })
    }
}

/// One immutable object mapping in a portable backup manifest.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct BackupArtifact {
    /// Original snapshot object key.
    pub source_object_key: String,
    /// Immutable key in the backup target.
    pub backup_object_key: String,
    /// Exact content digest.
    pub sha256: String,
    /// Exact byte length.
    pub bytes: u64,
}

/// Complete backup bill of materials, emitted only after the transfer barrier.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct SnapshotBackupManifest {
    /// Contract version.
    pub format_version: u32,
    /// Retry-stable backup identity.
    pub backup_id: Uuid,
    /// Tenant boundary.
    pub tenant_id: Uuid,
    /// Dataset boundary.
    pub dataset_id: Uuid,
    /// Original immutable snapshot.
    pub source_snapshot_id: Uuid,
    /// Exact source storage manifest digest.
    pub source_storage_manifest_sha256: String,
    /// Exact recovery certificate digest.
    pub recovery_certificate_sha256: String,
    /// Registered backup target.
    pub destination_target: String,
    /// Ordered complete artifact map.
    pub artifacts: Vec<BackupArtifact>,
    /// Total bytes across the artifact map.
    pub total_bytes: u64,
    /// Partial backup manifests are forbidden.
    pub complete: bool,
}

/// Certificate for a byte-complete restore. Storage verification precedes the
/// separate catalog import/publication transaction.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct SnapshotRestoreCertificate {
    /// Contract version.
    pub format_version: u32,
    /// Restore operation identity.
    pub restore_id: Uuid,
    /// Exact backup manifest digest.
    pub backup_manifest_sha256: String,
    /// Original snapshot identity.
    pub source_snapshot_id: Uuid,
    /// Requested restored snapshot identity.
    pub restored_snapshot_id: Uuid,
    /// Plan input identity.
    pub storage_manifest_sha256: String,
    /// All-partitions recovery certificate digest.
    pub recovery_certificate_sha256: String,
    /// False until a separate catalog transaction imports the restored snapshot.
    pub catalog_imported: bool,
    /// `storage-verified` until catalog import, then `certified-inactive`.
    pub publication_state: String,
    /// Always true for a published certificate.
    pub complete: bool,
}

/// Bind a successful restore barrier to its requested inactive identity.
pub fn build_restore_certificate(
    plan: &RecoveryPlan,
    restored_snapshot_id: Uuid,
    backup_manifest_sha256: &str,
    recovery_certificate: &RecoveryCertificate,
    recovery_certificate_sha256: &str,
) -> Result<SnapshotRestoreCertificate, RecoveryError> {
    if restored_snapshot_id.is_nil()
        || !valid_sha256(backup_manifest_sha256)
        || !valid_sha256(recovery_certificate_sha256)
        || !recovery_certificate.complete
        || recovery_certificate.operation_id != plan.operation_id
        || recovery_certificate.snapshot_id != plan.snapshot_id
        || plan.tasks.is_empty()
        || plan.tasks.iter().any(|task| task.reason != TransferReason::Restore)
    {
        return Err(RecoveryError::Incomplete(
            "restore certificate requires a complete restore-only plan".to_owned(),
        ));
    }
    Ok(SnapshotRestoreCertificate {
        format_version: STORAGE_RECOVERY_FORMAT_VERSION,
        restore_id: plan.operation_id,
        backup_manifest_sha256: backup_manifest_sha256.to_owned(),
        source_snapshot_id: plan.snapshot_id,
        restored_snapshot_id,
        storage_manifest_sha256: plan.storage_manifest_sha256.clone(),
        recovery_certificate_sha256: recovery_certificate_sha256.to_owned(),
        catalog_imported: false,
        publication_state: "storage-verified".to_owned(),
        complete: true,
    })
}

/// Build the portable backup manifest only from a complete certified plan.
pub fn build_backup_manifest(
    plan: &RecoveryPlan,
    certificate: &RecoveryCertificate,
    certificate_sha256: &str,
) -> Result<SnapshotBackupManifest, RecoveryError> {
    validate_plan(plan)?;
    if !certificate.complete
        || certificate.operation_id != plan.operation_id
        || certificate.snapshot_id != plan.snapshot_id
        || certificate.verified_task_count != u32::try_from(plan.tasks.len()).unwrap_or(u32::MAX)
        || !valid_sha256(certificate_sha256)
        || plan.tasks.is_empty()
        || plan.tasks.iter().any(|task| task.reason != TransferReason::Backup)
    {
        return Err(RecoveryError::Incomplete(
            "backup requires one complete certified backup plan".to_owned(),
        ));
    }
    let destinations = plan
        .tasks
        .iter()
        .map(|task| task.destination_target.as_str())
        .collect::<BTreeSet<_>>();
    if destinations.len() != 1 {
        return Err(RecoveryError::InvalidContract(
            "one backup manifest must have exactly one destination target".to_owned(),
        ));
    }
    let artifacts = plan
        .tasks
        .iter()
        .map(|task| BackupArtifact {
            source_object_key: task.source_object_key.clone(),
            backup_object_key: task.destination_object_key.clone(),
            sha256: task.sha256.clone(),
            bytes: task.bytes,
        })
        .collect::<Vec<_>>();
    let total_bytes = artifacts.iter().try_fold(0_u64, |total, artifact| {
        total.checked_add(artifact.bytes).ok_or_else(|| {
            RecoveryError::InvalidContract("backup byte total overflow".to_owned())
        })
    })?;
    Ok(SnapshotBackupManifest {
        format_version: STORAGE_RECOVERY_FORMAT_VERSION,
        backup_id: plan.operation_id,
        tenant_id: plan.tenant_id,
        dataset_id: plan.dataset_id,
        source_snapshot_id: plan.snapshot_id,
        source_storage_manifest_sha256: plan.storage_manifest_sha256.clone(),
        recovery_certificate_sha256: certificate_sha256.to_owned(),
        destination_target: destinations
            .into_iter()
            .next()
            .ok_or_else(|| RecoveryError::Incomplete("backup destination is absent".to_owned()))?
            .to_owned(),
        artifacts,
        total_bytes,
        complete: true,
    })
}

/// Build deterministic restore work from a certified backup. Restored objects
/// are staged below a new inactive snapshot namespace; publication is a separate
/// catalog compare-and-swap after the restore certificate is verified.
#[allow(clippy::too_many_arguments)]
pub fn build_restore_plan(
    operation_id: Uuid,
    restored_snapshot_id: Uuid,
    backup: &SnapshotBackupManifest,
    backup_manifest_sha256: &str,
    destination_target: &str,
    targets: &[StorageTarget],
    max_parallelism_bytes: u64,
) -> Result<RecoveryPlan, RecoveryError> {
    validate_backup_manifest(backup)?;
    validate_targets(targets)?;
    if operation_id.is_nil()
        || restored_snapshot_id.is_nil()
        || !valid_sha256(backup_manifest_sha256)
        || max_parallelism_bytes == 0
        || backup.destination_target == destination_target
        || !targets.iter().any(|target| target.name == destination_target && target.writable)
        || !targets.iter().any(|target| target.name == backup.destination_target)
    {
        return Err(RecoveryError::InvalidContract("restore request is invalid".to_owned()));
    }
    let mut tasks = Vec::with_capacity(backup.artifacts.len());
    for artifact in &backup.artifacts {
        let task_index = u32::try_from(tasks.len()).map_err(|_| {
            RecoveryError::InvalidContract("restore task count exceeds u32".to_owned())
        })?;
        let snapshot_artifact = SnapshotArtifact {
            object_key: artifact.backup_object_key.clone(),
            sha256: artifact.sha256.clone(),
            bytes: artifact.bytes,
        };
        let destination_object_key = format!(
            "restores/{}/{}/{}",
            restored_snapshot_id.simple(),
            operation_id.simple(),
            artifact.source_object_key
        );
        let stable_work_id = stable_work_id(
            operation_id,
            task_index,
            &backup.destination_target,
            destination_target,
            &snapshot_artifact,
            &destination_object_key,
            TransferReason::Restore,
        );
        tasks.push(TransferTask {
            task_index,
            stable_work_id,
            reason: TransferReason::Restore,
            source_target: backup.destination_target.clone(),
            destination_target: destination_target.to_owned(),
            source_object_key: artifact.backup_object_key.clone(),
            destination_object_key,
            sha256: artifact.sha256.clone(),
            bytes: artifact.bytes,
        });
    }
    Ok(RecoveryPlan {
        format_version: STORAGE_RECOVERY_FORMAT_VERSION,
        operation_id,
        tenant_id: backup.tenant_id,
        dataset_id: backup.dataset_id,
        snapshot_id: backup.source_snapshot_id,
        storage_manifest_sha256: backup_manifest_sha256.to_owned(),
        replication_factor: 1,
        max_in_flight_bytes: max_parallelism_bytes,
        tasks,
    })
}

/// Validate a recovery plan before any worker performs I/O.
pub fn validate_recovery_plan(plan: &RecoveryPlan) -> Result<(), RecoveryError> {
    validate_plan(plan)
}

/// Validate a portable backup bill of materials before catalog exposure or restore.
pub fn validate_backup_manifest(backup: &SnapshotBackupManifest) -> Result<(), RecoveryError> {
    if backup.format_version != STORAGE_RECOVERY_FORMAT_VERSION
        || backup.backup_id.is_nil()
        || backup.tenant_id.is_nil()
        || backup.dataset_id.is_nil()
        || backup.source_snapshot_id.is_nil()
        || !valid_sha256(&backup.source_storage_manifest_sha256)
        || !valid_sha256(&backup.recovery_certificate_sha256)
        || backup.destination_target.is_empty()
        || backup.artifacts.is_empty()
        || !backup.complete
    {
        return Err(RecoveryError::InvalidContract("backup manifest header is invalid".to_owned()));
    }
    let mut previous: Option<(&str, &str)> = None;
    let total = backup.artifacts.iter().try_fold(0_u64, |sum, artifact| {
        if !valid_object_key(&artifact.source_object_key)
            || !valid_object_key(&artifact.backup_object_key)
            || !valid_sha256(&artifact.sha256)
        {
                return Err(RecoveryError::InvalidContract("backup artifact is invalid".to_owned()));
        }
        let identity = (artifact.source_object_key.as_str(), artifact.backup_object_key.as_str());
        if previous.is_some_and(|prior| prior >= identity) {
            return Err(RecoveryError::InvalidContract(
                "backup artifacts must be sorted and duplicate-free".to_owned(),
            ));
        }
        previous = Some(identity);
        sum.checked_add(artifact.bytes)
            .ok_or_else(|| RecoveryError::InvalidContract("backup bytes overflow".to_owned()))
    })?;
    if total != backup.total_bytes {
        return Err(RecoveryError::InvalidContract("backup byte total mismatch".to_owned()));
    }
    Ok(())
}

/// Storage recovery failures never degrade into empty or partially restored data.
#[derive(Debug, Error)]
pub enum RecoveryError {
    /// An identity, checksum, target, or ordering invariant is invalid.
    #[error("invalid recovery contract: {0}")]
    InvalidContract(String),
    /// There are too few independent writable failure domains.
    #[error("replication factor {requested} cannot be placed across {available} writable failure domains")]
    InsufficientFailureDomains { requested: u16, available: usize },
    /// One or more partitions are absent, duplicated, corrupt, or unsuccessful.
    #[error("recovery completion barrier failed: {0}")]
    Incomplete(String),
    /// Artifact storage failed.
    #[error("artifact transfer failed: {0}")]
    Artifact(#[from] ArtifactStoreError),
    /// Local bounded scratch failed.
    #[error("recovery scratch failed: {0}")]
    Io(#[from] std::io::Error),
}

/// Validate and canonicalize the complete storage manifest.
pub fn validate_storage_manifest(manifest: &SnapshotStorageManifest) -> Result<(), RecoveryError> {
    if manifest.format_version != STORAGE_RECOVERY_FORMAT_VERSION
        || manifest.tenant_id.is_nil()
        || manifest.dataset_id.is_nil()
        || manifest.snapshot_id.is_nil()
        || !valid_sha256(&manifest.snapshot_manifest_sha256)
        || manifest
            .activation_manifest_sha256
            .as_ref()
            .is_some_and(|digest| !valid_sha256(digest))
        || manifest.artifacts.is_empty()
    {
        return Err(RecoveryError::InvalidContract(
            "storage-manifest identity or digest is invalid".to_owned(),
        ));
    }
    let mut previous: Option<&SnapshotArtifact> = None;
    for artifact in &manifest.artifacts {
        validate_artifact(artifact)?;
        if previous.is_some_and(|value| value >= artifact) {
            return Err(RecoveryError::InvalidContract(
                "artifacts must be sorted and duplicate-free".to_owned(),
            ));
        }
        previous = Some(artifact);
    }
    Ok(())
}

/// Walk checksum-bound JSON roots and return the verified transitive artifact closure.
/// Workers never list an object-store prefix; every discovered child is an exact key/hash pair.
pub async fn discover_artifact_closure(
    store: &ArtifactStore,
    roots: &[(String, String)],
    scratch_root: &Path,
    max_json_bytes: u64,
    max_artifact_bytes: u64,
    max_artifacts: usize,
) -> Result<Vec<SnapshotArtifact>, RecoveryError> {
    if roots.is_empty() || max_json_bytes == 0 || max_artifact_bytes == 0 || max_artifacts == 0 {
        return Err(RecoveryError::InvalidContract(
            "artifact closure ceilings and roots must be non-empty".to_owned(),
        ));
    }
    tokio::fs::create_dir_all(scratch_root).await?;
    let mut queue = roots
        .iter()
        .cloned()
        .map(|(key, sha256)| PendingArtifact { key, sha256, bytes: None })
        .collect::<VecDeque<_>>();
    let mut artifacts = BTreeMap::<String, SnapshotArtifact>::new();
    while let Some(pending) = queue.pop_front() {
        if !valid_object_key(&pending.key) || !valid_sha256(&pending.sha256) {
            return Err(RecoveryError::InvalidContract(
                "artifact closure contains an unsafe key or digest".to_owned(),
            ));
        }
        if let Some(existing) = artifacts.get(&pending.key) {
            if existing.sha256 != pending.sha256 {
                return Err(RecoveryError::InvalidContract(format!(
                    "object key {} has conflicting checksums",
                    pending.key
                )));
            }
            continue;
        }
        if artifacts.len() >= max_artifacts {
            return Err(RecoveryError::InvalidContract(
                "artifact closure exceeds the object-count ceiling".to_owned(),
            ));
        }
        let is_json = likely_json_manifest(&pending.key);
        let observed_bytes = if is_json {
            let local = scratch_root.join(format!("{}.json", pending.sha256));
            match tokio::fs::remove_file(&local).await {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
            let bytes = store
                .materialize_verified(&pending.key, &pending.sha256, max_json_bytes, &local)
                .await?;
            let document: serde_json::Value = serde_json::from_slice(&tokio::fs::read(&local).await?)
                .map_err(|error| {
                    RecoveryError::InvalidContract(format!(
                        "checksum-bound JSON artifact {} is invalid: {error}",
                        pending.key
                    ))
                })?;
            let parent = pending.key.rsplit_once('/').map_or("", |value| value.0);
            let mut children = Vec::new();
            collect_json_references(&document, parent, &mut children)?;
            for child in children {
                queue.push_back(child);
            }
            bytes
        } else {
            let ceiling = pending.bytes.unwrap_or(max_artifact_bytes).min(max_artifact_bytes);
            store.verify_remote(&pending.key, &pending.sha256, ceiling).await?
        };
        if pending.bytes.is_some_and(|expected| expected != observed_bytes) {
            return Err(RecoveryError::Incomplete(format!(
                "artifact {} byte length differs from its manifest",
                pending.key
            )));
        }
        artifacts.insert(
            pending.key.clone(),
            SnapshotArtifact {
                object_key: pending.key,
                sha256: pending.sha256,
                bytes: observed_bytes,
            },
        );
    }
    let mut output = artifacts.into_values().collect::<Vec<_>>();
    output.sort();
    Ok(output)
}

#[derive(Clone, Debug)]
struct PendingArtifact {
    key: String,
    sha256: String,
    bytes: Option<u64>,
}

fn collect_json_references(
    value: &serde_json::Value,
    parent: &str,
    output: &mut Vec<PendingArtifact>,
) -> Result<(), RecoveryError> {
    match value {
        serde_json::Value::Object(object) => {
            if let (Some(relative), Some(sha256), Some(bytes)) = (
                object.get("relativePath").and_then(serde_json::Value::as_str),
                object.get("sha256").and_then(serde_json::Value::as_str),
                object.get("bytes").and_then(serde_json::Value::as_u64),
            ) {
                let key = if parent.is_empty() {
                    relative.to_owned()
                } else {
                    format!("{parent}/{relative}")
                };
                output.push(PendingArtifact { key, sha256: sha256.to_owned(), bytes: Some(bytes) });
            }
            for (field, candidate) in object {
                let Some(key) = candidate.as_str() else { continue };
                let (sha_field, bytes_field) = if field == "objectKey" {
                    (Some("sha256".to_owned()), Some("bytes".to_owned()))
                } else if let Some(stem) = field.strip_suffix("ObjectKey") {
                    (Some(format!("{stem}Sha256")), Some(format!("{stem}Bytes")))
                } else if field == "manifestPath" {
                    (Some("manifestSha256".to_owned()), Some("manifestBytes".to_owned()))
                } else if let Some(stem) = field.strip_suffix("ManifestPath") {
                    (Some(format!("{stem}ManifestSha256")), Some(format!("{stem}ManifestBytes")))
                } else {
                    (None, None)
                };
                let Some(sha_field) = sha_field else { continue };
                let Some(sha256) = object.get(&sha_field).and_then(serde_json::Value::as_str) else {
                    continue;
                };
                let bytes = bytes_field
                    .as_ref()
                    .and_then(|name| object.get(name))
                    .and_then(serde_json::Value::as_u64);
                output.push(PendingArtifact { key: key.to_owned(), sha256: sha256.to_owned(), bytes });
            }
            for child in object.values() {
                collect_json_references(child, parent, output)?;
            }
        }
        serde_json::Value::Array(values) => {
            for child in values {
                collect_json_references(child, parent, output)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn likely_json_manifest(key: &str) -> bool {
    let name = key.rsplit('/').next().unwrap_or(key);
    name.ends_with(".json")
        || name == "root"
        || name == "manifest"
        || name.ends_with("-root")
        || name.ends_with("-manifest")
}

/// Select replica targets with deterministic rendezvous hashing and strict
/// failure-domain diversity.
pub fn select_replica_targets(
    snapshot_id: Uuid,
    artifact: &SnapshotArtifact,
    targets: &[StorageTarget],
    replication_factor: u16,
) -> Result<Vec<String>, RecoveryError> {
    validate_artifact(artifact)?;
    if replication_factor == 0 {
        return Err(RecoveryError::InvalidContract(
            "replication factor must be positive".to_owned(),
        ));
    }
    validate_targets(targets)?;
    let writable_domains = targets
        .iter()
        .filter(|target| target.writable)
        .map(|target| target.failure_domain.as_str())
        .collect::<BTreeSet<_>>();
    if writable_domains.len() < usize::from(replication_factor) {
        return Err(RecoveryError::InsufficientFailureDomains {
            requested: replication_factor,
            available: writable_domains.len(),
        });
    }
    let mut scored = targets
        .iter()
        .filter(|target| target.writable)
        .map(|target| {
            let mut hash = Sha256::new();
            hash.update(b"ngkg-storage-rendezvous-v1\0");
            hash.update(snapshot_id.as_bytes());
            hash.update(artifact.sha256.as_bytes());
            hash.update(target.name.as_bytes());
            (hash.finalize().to_vec(), target)
        })
        .collect::<Vec<_>>();
    scored.sort_by(|left, right| right.0.cmp(&left.0).then_with(|| left.1.name.cmp(&right.1.name)));
    let mut domains = BTreeSet::new();
    let selected = scored
        .into_iter()
        .filter_map(|(_, target)| {
            domains
                .insert(target.failure_domain.clone())
                .then(|| target.name.clone())
        })
        .take(usize::from(replication_factor))
        .collect::<Vec<_>>();
    if selected.len() != usize::from(replication_factor) {
        return Err(RecoveryError::InsufficientFailureDomains {
            requested: replication_factor,
            available: selected.len(),
        });
    }
    Ok(selected)
}

/// Build retry-stable fan-out tasks. Existing verified target names suppress
/// duplicate work; quarantined targets must not be passed as verified.
#[allow(clippy::too_many_arguments)]
pub fn build_recovery_plan(
    operation_id: Uuid,
    manifest: &SnapshotStorageManifest,
    storage_manifest_sha256: &str,
    source_target: &str,
    targets: &[StorageTarget],
    existing_verified: &BTreeMap<String, BTreeSet<String>>,
    replication_factor: u16,
    max_in_flight_bytes: u64,
    reason: TransferReason,
) -> Result<RecoveryPlan, RecoveryError> {
    validate_storage_manifest(manifest)?;
    validate_targets(targets)?;
    if operation_id.is_nil() || !valid_sha256(storage_manifest_sha256) || max_in_flight_bytes == 0 {
        return Err(RecoveryError::InvalidContract(
            "plan identity, manifest digest, or byte budget is invalid".to_owned(),
        ));
    }
    let target_names = targets.iter().map(|target| target.name.as_str()).collect::<BTreeSet<_>>();
    if !target_names.contains(source_target) {
        return Err(RecoveryError::InvalidContract("source target is not registered".to_owned()));
    }
    let mut tasks = Vec::new();
    for artifact in &manifest.artifacts {
        let selected = select_replica_targets(manifest.snapshot_id, artifact, targets, replication_factor)?;
        let verified = existing_verified.get(&artifact.sha256);
        for destination in selected {
            if destination == source_target
                || verified.is_some_and(|names| names.contains(&destination))
            {
                continue;
            }
            let task_index = u32::try_from(tasks.len()).map_err(|_| {
                RecoveryError::InvalidContract("task count exceeds u32".to_owned())
            })?;
            let destination_object_key = format!(
                "replicas/sha256/{}/{}/{}/{}",
                &artifact.sha256[..2],
                artifact.sha256,
                operation_id.simple(),
                file_component(&artifact.object_key)
            );
            let stable_work_id = stable_work_id(
                operation_id,
                task_index,
                source_target,
                &destination,
                artifact,
                &destination_object_key,
                reason,
            );
            tasks.push(TransferTask {
                task_index,
                stable_work_id,
                reason,
                source_target: source_target.to_owned(),
                destination_target: destination,
                source_object_key: artifact.object_key.clone(),
                destination_object_key,
                sha256: artifact.sha256.clone(),
                bytes: artifact.bytes,
            });
        }
    }
    Ok(RecoveryPlan {
        format_version: STORAGE_RECOVERY_FORMAT_VERSION,
        operation_id,
        tenant_id: manifest.tenant_id,
        dataset_id: manifest.dataset_id,
        snapshot_id: manifest.snapshot_id,
        storage_manifest_sha256: storage_manifest_sha256.to_owned(),
        replication_factor,
        max_in_flight_bytes,
        tasks,
    })
}

/// Execute one transfer through bounded scratch and verify both source and
/// destination. A checksum mismatch is returned and must quarantine the replica.
pub async fn execute_transfer(
    operation_id: Uuid,
    task: &TransferTask,
    source: &ArtifactStore,
    destination: &ArtifactStore,
    scratch_root: &Path,
    single_put_max_bytes: u64,
    multipart_buffer_bytes: usize,
    multipart_concurrency: usize,
) -> Result<TransferResult, RecoveryError> {
    if operation_id.is_nil() {
        return Err(RecoveryError::InvalidContract(
            "operation ID must be non-nil".to_owned(),
        ));
    }
    validate_task(task)?;
    let scratch = scratch_path(scratch_root, &task.stable_work_id)?;
    if let Some(parent) = scratch.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let transfer = async {
        let copied = source
            .materialize_verified(
                &task.source_object_key,
                &task.sha256,
                task.bytes,
                &scratch,
            )
            .await?;
        if copied != task.bytes {
            return Err(RecoveryError::Incomplete(
                "source byte length differs from the recovery plan".to_owned(),
            ));
        }
        destination
            .put_file_immutable(
                &task.destination_object_key,
                &task.sha256,
                &scratch,
                single_put_max_bytes,
                multipart_buffer_bytes,
                multipart_concurrency,
            )
            .await?;
        destination
            .verify_remote(&task.destination_object_key, &task.sha256, task.bytes)
            .await?;
        Ok(TransferResult {
            operation_id,
            task_index: task.task_index,
            stable_work_id: task.stable_work_id.clone(),
            state: TransferState::Succeeded,
            observed_sha256: Some(task.sha256.clone()),
            copied_bytes: copied,
            error_code: None,
        })
    }
    .await;
    let _ = tokio::fs::remove_file(&scratch).await;
    transfer
}

/// Enforce the all-partitions barrier and return a deterministic certificate.
pub fn certify_recovery(
    plan: &RecoveryPlan,
    plan_sha256: &str,
    results: &[TransferResult],
) -> Result<RecoveryCertificate, RecoveryError> {
    validate_plan(plan)?;
    if !valid_sha256(plan_sha256) || results.len() != plan.tasks.len() {
        return Err(RecoveryError::Incomplete(
            "result cardinality does not match the plan".to_owned(),
        ));
    }
    let by_index = results
        .iter()
        .map(|result| (result.task_index, result))
        .collect::<BTreeMap<_, _>>();
    if by_index.len() != results.len() {
        return Err(RecoveryError::Incomplete("duplicate task result".to_owned()));
    }
    let mut accumulator = RecoveryCertificationAccumulator::new(plan, plan_sha256)?;
    for task in &plan.tasks {
        let result = by_index.get(&task.task_index).ok_or_else(|| {
            RecoveryError::Incomplete(format!("missing task {}", task.task_index))
        })?;
        accumulator.observe(task, result)?;
    }
    accumulator.finish()
}

/// Derive the same operation identity for every delivery of an identical request.
#[must_use]
pub fn derive_operation_id(
    tenant_id: Uuid,
    dataset_id: Uuid,
    idempotency_key: &str,
    request_sha256: &str,
) -> Uuid {
    let mut bytes = Vec::with_capacity(256);
    bytes.extend_from_slice(b"ngkg-storage-recovery-operation-v1\0");
    bytes.extend_from_slice(tenant_id.as_bytes());
    bytes.extend_from_slice(dataset_id.as_bytes());
    bytes.extend_from_slice(idempotency_key.as_bytes());
    bytes.extend_from_slice(request_sha256.as_bytes());
    Uuid::new_v5(&dataset_id, &bytes)
}

fn validate_plan(plan: &RecoveryPlan) -> Result<(), RecoveryError> {
    if plan.format_version != STORAGE_RECOVERY_FORMAT_VERSION
        || plan.operation_id.is_nil()
        || plan.tenant_id.is_nil()
        || plan.dataset_id.is_nil()
        || plan.snapshot_id.is_nil()
        || !valid_sha256(&plan.storage_manifest_sha256)
        || plan.replication_factor == 0
        || plan.max_in_flight_bytes == 0
    {
        return Err(RecoveryError::InvalidContract("plan header is invalid".to_owned()));
    }
    for (index, task) in plan.tasks.iter().enumerate() {
        validate_task(task)?;
        let source = SnapshotArtifact {
            object_key: task.source_object_key.clone(),
            sha256: task.sha256.clone(),
            bytes: task.bytes,
        };
        let expected_work_id = stable_work_id(
            plan.operation_id,
            task.task_index,
            &task.source_target,
            &task.destination_target,
            &source,
            &task.destination_object_key,
            task.reason,
        );
        if usize::try_from(task.task_index).ok() != Some(index)
            || task.bytes > plan.max_in_flight_bytes
            || task.stable_work_id != expected_work_id
        {
            return Err(RecoveryError::InvalidContract(
                "task index, byte budget, or stable work identity is invalid".to_owned(),
            ));
        }
    }
    Ok(())
}

fn validate_task(task: &TransferTask) -> Result<(), RecoveryError> {
    if task.stable_work_id.len() != 71
        || !task.stable_work_id.starts_with("blake3:")
        || !task.stable_work_id[7..].bytes().all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
        || task.source_target.is_empty()
        || task.destination_target.is_empty()
        || task.source_target == task.destination_target
        || !valid_object_key(&task.source_object_key)
        || !valid_object_key(&task.destination_object_key)
        || !valid_sha256(&task.sha256)
    {
        return Err(RecoveryError::InvalidContract(format!(
            "transfer task {} is invalid",
            task.task_index
        )));
    }
    Ok(())
}

fn validate_artifact(artifact: &SnapshotArtifact) -> Result<(), RecoveryError> {
    if !valid_object_key(&artifact.object_key) || !valid_sha256(&artifact.sha256) {
        return Err(RecoveryError::InvalidContract(
            "artifact key or checksum is invalid".to_owned(),
        ));
    }
    Ok(())
}

fn validate_targets(targets: &[StorageTarget]) -> Result<(), RecoveryError> {
    let mut names = BTreeSet::new();
    for target in targets {
        if !valid_dns_label(&target.name)
            || target.failure_domain.is_empty()
            || target.failure_domain.len() > 253
            || target.base_url.contains('@')
            || !matches!(target.base_url.split(':').next(), Some("s3" | "file"))
            || !names.insert(&target.name)
        {
            return Err(RecoveryError::InvalidContract(
                "storage target registry is invalid".to_owned(),
            ));
        }
    }
    Ok(())
}

fn stable_work_id(
    operation_id: Uuid,
    task_index: u32,
    source: &str,
    destination: &str,
    artifact: &SnapshotArtifact,
    destination_key: &str,
    reason: TransferReason,
) -> String {
    let mut hash = blake3::Hasher::new();
    hash.update(b"ngkg-storage-transfer-v1\0");
    hash.update(operation_id.as_bytes());
    hash.update(&task_index.to_be_bytes());
    hash.update(source.as_bytes());
    hash.update(destination.as_bytes());
    hash.update(artifact.object_key.as_bytes());
    hash.update(destination_key.as_bytes());
    hash.update(artifact.sha256.as_bytes());
    hash.update(&artifact.bytes.to_be_bytes());
    hash.update(format!("{reason:?}").as_bytes());
    format!("blake3:{}", hash.finalize().to_hex())
}

fn scratch_path(root: &Path, work_id: &str) -> Result<PathBuf, RecoveryError> {
    let digest = work_id.strip_prefix("blake3:").ok_or_else(|| {
        RecoveryError::InvalidContract("work ID lacks BLAKE3 prefix".to_owned())
    })?;
    Ok(root.join(format!("{digest}.partial")))
}

fn file_component(key: &str) -> String {
    key.rsplit('/').next().unwrap_or("artifact").to_owned()
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

fn valid_dns_label(value: &str) -> bool {
    (1..=63).contains(&value.len())
        && value.bytes().all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        && value.as_bytes().first().is_some_and(u8::is_ascii_alphanumeric)
        && value.as_bytes().last().is_some_and(u8::is_ascii_alphanumeric)
}

fn valid_object_key(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 1024
        && !value.starts_with('/')
        && !value.ends_with('/')
        && !value.contains('\\')
        && value.split('/').all(|segment| {
            !segment.is_empty()
                && segment != "."
                && segment != ".."
                && segment.len() <= 255
                && segment.bytes().all(|byte| {
                    byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-')
                })
        })
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, BTreeSet},
        time::{SystemTime, UNIX_EPOCH},
    };

    use super::*;

    fn artifact() -> SnapshotArtifact {
        SnapshotArtifact {
            object_key: "snapshots/a/part-000.parquet".to_owned(),
            sha256: "a".repeat(64),
            bytes: 42,
        }
    }

    fn targets() -> Vec<StorageTarget> {
        ["zone-a", "zone-b", "zone-c"]
            .into_iter()
            .map(|zone| StorageTarget {
                name: zone.to_owned(),
                failure_domain: zone.to_owned(),
                base_url: format!("s3://ngkg-{zone}"),
                writable: true,
            })
            .collect()
    }

    #[test]
    fn rendezvous_placement_is_deterministic_and_failure_domain_distinct() {
        let snapshot = Uuid::new_v4();
        let left = select_replica_targets(snapshot, &artifact(), &targets(), 3);
        let right = select_replica_targets(snapshot, &artifact(), &targets(), 3);
        assert!(left.is_ok());
        assert_eq!(left.ok(), right.ok());
    }

    #[test]
    fn placement_fails_closed_without_enough_domains() {
        let mut same_domain = targets();
        for target in &mut same_domain {
            target.failure_domain = "one-domain".to_owned();
        }
        assert!(matches!(
            select_replica_targets(Uuid::new_v4(), &artifact(), &same_domain, 2),
            Err(RecoveryError::InsufficientFailureDomains { .. })
        ));
    }

    #[test]
    fn verified_replicas_make_retry_plan_idempotently_empty() -> Result<(), RecoveryError> {
        let tenant = Uuid::new_v4();
        let dataset = Uuid::new_v4();
        let snapshot = Uuid::new_v4();
        let item = artifact();
        let manifest = SnapshotStorageManifest {
            format_version: 1,
            tenant_id: tenant,
            dataset_id: dataset,
            snapshot_id: snapshot,
            snapshot_manifest_sha256: "b".repeat(64),
            activation_manifest_sha256: None,
            artifacts: vec![item.clone()],
        };
        let selected = select_replica_targets(snapshot, &item, &targets(), 2)?;
        let existing = BTreeMap::from([(
            item.sha256.clone(),
            selected.into_iter().collect::<BTreeSet<_>>(),
        )]);
        let plan = build_recovery_plan(
            Uuid::new_v4(),
            &manifest,
            &"c".repeat(64),
            "zone-a",
            &targets(),
            &existing,
            2,
            1024,
            TransferReason::Replication,
        )?;
        assert!(plan.tasks.is_empty());
        Ok(())
    }

    #[test]
    fn completion_barrier_rejects_partial_or_quarantined_results() -> Result<(), RecoveryError> {
        let manifest = SnapshotStorageManifest {
            format_version: 1,
            tenant_id: Uuid::new_v4(),
            dataset_id: Uuid::new_v4(),
            snapshot_id: Uuid::new_v4(),
            snapshot_manifest_sha256: "b".repeat(64),
            activation_manifest_sha256: None,
            artifacts: vec![artifact()],
        };
        let plan = build_recovery_plan(
            Uuid::new_v4(),
            &manifest,
            &"c".repeat(64),
            "zone-a",
            &targets(),
            &BTreeMap::new(),
            2,
            1024,
            TransferReason::NodeLoss,
        )?;
        assert!(certify_recovery(&plan, &"d".repeat(64), &[]).is_err());
        let results = plan
            .tasks
            .iter()
            .map(|task| TransferResult {
                operation_id: plan.operation_id,
                task_index: task.task_index,
                stable_work_id: task.stable_work_id.clone(),
                state: TransferState::Quarantined,
                observed_sha256: None,
                copied_bytes: 0,
                error_code: Some("CHECKSUM_MISMATCH".to_owned()),
            })
            .collect::<Vec<_>>();
        assert!(certify_recovery(&plan, &"d".repeat(64), &results).is_err());
        Ok(())
    }

    #[tokio::test]
    async fn artifact_closure_follows_checksum_bound_manifest_paths(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let root = std::env::temp_dir().join(format!(
            "ngkg-storage-closure-test-{}-{nonce}",
            std::process::id()
        ));
        let objects = root.join("objects");
        let child_path = objects.join("snapshots/a/child.json");
        let leaf_path = objects.join("snapshots/a/leaf.bin");
        std::fs::create_dir_all(child_path.parent().ok_or("child has no parent")?)?;
        let leaf = b"rdf-artifact";
        std::fs::write(&leaf_path, leaf)?;
        let leaf_sha = hex::encode(Sha256::digest(leaf));
        let child = serde_json::to_vec(&serde_json::json!({
            "artifacts": [{
                "relativePath": "leaf.bin",
                "sha256": leaf_sha,
                "bytes": leaf.len()
            }]
        }))?;
        std::fs::write(&child_path, &child)?;
        let child_sha = hex::encode(Sha256::digest(&child));
        let root_path = objects.join("roots/root.json");
        std::fs::create_dir_all(root_path.parent().ok_or("root has no parent")?)?;
        let root_manifest = serde_json::to_vec(&serde_json::json!({
            "manifestPath": "snapshots/a/child.json",
            "manifestSha256": child_sha
        }))?;
        std::fs::write(&root_path, &root_manifest)?;
        let root_sha = hex::encode(Sha256::digest(&root_manifest));
        let store = ArtifactStore::from_base_url(&format!("file://{}", objects.display()))?;
        let closure = discover_artifact_closure(
            &store,
            &[("roots/root.json".to_owned(), root_sha)],
            &root.join("scratch"),
            1024 * 1024,
            1024 * 1024,
            16,
        )
        .await?;
        assert_eq!(closure.len(), 3);
        assert!(closure.iter().any(|artifact| artifact.object_key == "snapshots/a/leaf.bin"));
        std::fs::remove_dir_all(root)?;
        Ok(())
    }
}
