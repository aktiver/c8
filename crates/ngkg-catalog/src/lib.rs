//! Tenant-isolated durable catalog, operation state, and snapshot publication.

use ngkg_types::PublicationPolicy;
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, Postgres, Row, Transaction};
use thiserror::Error;
use uuid::Uuid;

/// Closed job-state vocabulary persisted as PostgreSQL enum labels.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum JobState {
    /// Durable request exists.
    Registered,
    /// Immutable source bundle was verified.
    SourcePlanned,
    /// Projection policy was validated.
    MappingValidated,
    /// Deterministic work was partitioned.
    Partitioned,
    /// RDF projection completed.
    Projected,
    /// GUID and FactID assignment completed.
    Identified,
    /// Semantic spine and payload Parquet completed.
    SpineWritten,
    /// Direct indexes and locator completed.
    Indexed,
    /// Offline reasoner completed consistently.
    Reasoned,
    /// Expected result equality and coverage checks passed.
    Certified,
    /// The active-snapshot compare-and-swap succeeded.
    Published,
    /// A deterministic or exhausted infrastructure failure occurred.
    Failed,
    /// An authorized caller cancelled before publication.
    Cancelled,
}

/// Closed distributed work vocabulary introduced by migrations 3 and 4.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum DistributedWorkKind {
    /// Syntax-safe RDF projection partition.
    Projection,
    /// Deterministic dictionary/fact range reducer.
    Reducer,
    /// Columnar semantic-artifact partition.
    Artifact,
}

impl DistributedWorkKind {
    /// PostgreSQL enum representation.
    #[must_use]
    pub const fn as_db(self) -> &'static str {
        match self {
            Self::Projection => "PROJECTION",
            Self::Reducer => "REDUCER",
            Self::Artifact => "ARTIFACT",
        }
    }
}

/// Immutable catalog registration for one distributed completion index.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RegisterDistributedWork {
    /// Dense completion index in its work kind.
    pub work_index: i32,
    /// Stable content-independent BLAKE3 work identity.
    pub stable_work_id: String,
    /// Exact input object key; workers never list a prefix.
    pub input_object_key: String,
    /// Exact input SHA-256.
    pub input_sha256: [u8; 32],
}

/// Immutable source/reducer plan committed before Indexed Jobs are admitted.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RegisterDistributedPlan {
    /// Exact source plan object key.
    pub source_plan_object_key: String,
    /// Exact source plan SHA-256.
    pub source_plan_sha256: [u8; 32],
    /// Stable logical partition count.
    pub logical_partition_count: i32,
    /// Deterministic reducer range count.
    pub reducer_count: i32,
    /// Source-plan fact count.
    pub fact_count: i64,
    /// Versioned partition layout policy.
    pub layout_profile: String,
    /// Dense projection work.
    pub projections: Vec<RegisterDistributedWork>,
    /// Dense reducer work.
    pub reducers: Vec<RegisterDistributedWork>,
}

/// Durable distributed plan summary used by the operator.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DistributedPlanSummary {
    /// Exact source plan key.
    pub source_plan_object_key: String,
    /// Lowercase source plan SHA-256.
    pub source_plan_sha256: String,
    /// Logical source partitions.
    pub logical_partition_count: i32,
    /// Reducer ranges.
    pub reducer_count: i32,
    /// Expected logical facts.
    pub fact_count: i64,
    /// Versioned layout policy.
    pub layout_profile: String,
    /// Successful projection partitions.
    pub succeeded_projections: i64,
    /// Successful reducer ranges.
    pub succeeded_reducers: i64,
    /// Terminal failed work items.
    pub failed_work: i64,
}

/// Exact work row selected by a Kubernetes completion index.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DistributedWorkItem {
    /// Work category.
    pub work_kind: DistributedWorkKind,
    /// Dense index.
    pub work_index: i32,
    /// Stable work identity.
    pub stable_work_id: String,
    /// Exact input key.
    pub input_object_key: String,
    /// Lowercase input checksum.
    pub input_sha256: String,
    /// PENDING, SUCCEEDED, or FAILED.
    pub state: String,
    /// Immutable output manifest key after success.
    pub output_manifest_object_key: Option<String>,
    /// Immutable output manifest checksum after success.
    pub output_manifest_sha256: Option<String>,
}

/// Immutable globally reduced source registered before final OWL compilation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DistributedRoot {
    /// Root manifest key.
    pub root_manifest_object_key: String,
    /// Root manifest SHA-256.
    pub root_manifest_sha256: String,
    /// Canonical N-Quads key.
    pub canonical_source_object_key: String,
    /// Canonical N-Quads SHA-256.
    pub canonical_source_sha256: String,
    /// Dense dictionary key.
    pub dictionary_object_key: String,
    /// Dense dictionary SHA-256.
    pub dictionary_sha256: String,
    /// Topology-independent logical content hash.
    pub semantic_content_sha256: String,
}

/// Immutable registration for the distributed semantic-artifact stage.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RegisterArtifactPlan {
    /// Exact source-plan key.
    pub source_plan_object_key: String,
    /// Exact source-plan checksum.
    pub source_plan_sha256: [u8; 32],
    /// Exact Phase 15 dictionary key.
    pub dictionary_object_key: String,
    /// Exact dictionary checksum.
    pub dictionary_sha256: [u8; 32],
    /// Exact artifact-plan manifest key.
    pub artifact_plan_object_key: String,
    /// Exact artifact-plan manifest checksum.
    pub artifact_plan_sha256: [u8; 32],
    /// Dense logical partition count.
    pub partition_count: i32,
    /// Immutable Parquet row-group size.
    pub row_group_rows: i32,
    /// One exact work item per logical source partition.
    pub work: Vec<RegisterDistributedWork>,
}

/// Durable artifact-stage counts used by level-based reconciliation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ArtifactPlanSummary {
    /// Exact source-plan key.
    pub source_plan_object_key: String,
    /// Lowercase source-plan checksum.
    pub source_plan_sha256: String,
    /// Exact dictionary key.
    pub dictionary_object_key: String,
    /// Lowercase dictionary checksum.
    pub dictionary_sha256: String,
    /// Exact artifact-plan key.
    pub artifact_plan_object_key: String,
    /// Lowercase artifact-plan checksum.
    pub artifact_plan_sha256: String,
    /// Logical artifact partitions.
    pub partition_count: i32,
    /// Immutable row-group size.
    pub row_group_rows: i32,
    /// Successful artifact partitions.
    pub succeeded_artifacts: i64,
    /// Failed artifact partitions.
    pub failed_artifacts: i64,
}

/// Immutable global artifact root committed after the exact partition barrier.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DistributedArtifactRoot {
    /// Root manifest object key.
    pub root_manifest_object_key: String,
    /// Root manifest SHA-256.
    pub root_manifest_sha256: String,
    /// Global direct locator key.
    pub locator_object_key: String,
    /// Global locator SHA-256.
    pub locator_sha256: String,
    /// Topology-stable semantic content root.
    pub semantic_content_sha256: String,
    /// Exact source facts.
    pub fact_count: i64,
    /// Non-payload semantic rows.
    pub semantic_row_count: i64,
    /// Payload rows.
    pub payload_row_count: i64,
    /// Direct locator records.
    pub locator_record_count: i64,
}

/// Immutable mmap locator and sharded payload serving bill of materials.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DistributedServingRoot {
    /// Serving-root manifest object key.
    pub serving_root_object_key: String,
    /// Serving-root manifest SHA-256.
    pub serving_root_sha256: String,
    /// Fixed-width binary locator object key.
    pub binary_locator_object_key: String,
    /// Fixed-width binary locator SHA-256.
    pub binary_locator_sha256: String,
    /// Source TSV locator SHA-256 bound into the binary header.
    pub source_locator_sha256: String,
    /// Topology-stable semantic content root.
    pub semantic_content_sha256: String,
    /// Logical payload partition count.
    pub partition_count: i32,
    /// Immutable Parquet row-group size.
    pub row_group_rows: i32,
    /// Exact binary locator records.
    pub locator_record_count: i64,
}

/// Reference-versus-sharded hydration evidence required before publication.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ServingCertification {
    /// Immutable equivalence report object key.
    pub report_object_key: String,
    /// Equivalence report SHA-256.
    pub report_sha256: String,
    /// Exact serving-root SHA-256 evaluated by the report.
    pub serving_root_sha256: String,
    /// Exact binary locator SHA-256 evaluated by the report.
    pub binary_locator_sha256: String,
    /// Reference snapshot manifest object key compared by the report.
    pub reference_manifest_object_key: String,
    /// Reference snapshot manifest SHA-256 compared by the report.
    pub reference_manifest_sha256: String,
    /// Exact certified query count.
    pub certified_query_count: i32,
    /// Total reference hydration rows compared.
    pub hydrated_row_count: i64,
}

/// Immutable cloud compiler/reasoner barrier admitted for scalar online serving.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CloudSnapshotActivation {
    pub activation_manifest_object_key: String,
    pub activation_manifest_sha256: String,
    pub semantic_root_object_key: String,
    pub semantic_root_sha256: String,
    pub qualification_root_object_key: String,
    pub qualification_root_sha256: String,
    pub offline_root_object_key: String,
    pub offline_root_sha256: String,
    pub semantic_content_sha256: String,
    pub authorized_graph_set_sha256: String,
    pub datatype_policy_sha256: String,
    pub ontology_sha256: String,
    pub finite_closure_sha256: String,
    pub proof_support_root_sha256: String,
    pub query_dataset_sha256: String,
    pub query_dataset_bytes: i64,
    pub fact_count: i64,
    pub consequence_count: i64,
    pub semantic_partition_count: i32,
    pub reasoning_partition_count: i32,
}

/// Request to atomically certify a complete cloud snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommitCloudSnapshotActivation {
    pub activation: CloudSnapshotActivation,
    pub reference_manifest_object_key: String,
    pub reference_manifest_sha256: [u8; 32],
}

/// Complete catalog truth required by an online replica before it serves traffic.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ActiveServingSnapshot {
    /// Active published snapshot.
    pub snapshot: Snapshot,
    /// Dataset-wide namespace used to reproduce canonical GUIDs.
    pub identity_namespace: Uuid,
    /// Immutable physical serving root.
    pub serving_root: Option<DistributedServingRoot>,
    /// Reference-versus-sharded admission evidence.
    pub serving_certification: Option<ServingCertification>,
    /// Present for the Phase 40.13.15 cloud compiler path.
    pub cloud_activation: Option<CloudSnapshotActivation>,
}

/// Catalog-authoritative roots from which storage recovery computes a complete artifact closure.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SnapshotRecoveryRoots {
    /// Immutable snapshot record.
    pub snapshot: Snapshot,
    /// Cloud compiler activation roots when applicable.
    pub cloud_activation: Option<CloudSnapshotActivation>,
    /// Distributed serving root when applicable.
    pub serving_root: Option<DistributedServingRoot>,
}

impl JobState {
    /// PostgreSQL enum representation.
    #[must_use]
    pub const fn as_db(self) -> &'static str {
        match self {
            Self::Registered => "REGISTERED",
            Self::SourcePlanned => "SOURCE_PLANNED",
            Self::MappingValidated => "MAPPING_VALIDATED",
            Self::Partitioned => "PARTITIONED",
            Self::Projected => "PROJECTED",
            Self::Identified => "IDENTIFIED",
            Self::SpineWritten => "SPINE_WRITTEN",
            Self::Indexed => "INDEXED",
            Self::Reasoned => "REASONED",
            Self::Certified => "CERTIFIED",
            Self::Published => "PUBLISHED",
            Self::Failed => "FAILED",
            Self::Cancelled => "CANCELLED",
        }
    }
}

/// Return true only for explicitly authorized state edges.
#[must_use]
pub const fn may_transition(from: JobState, to: JobState) -> bool {
    use JobState::{
        Cancelled, Certified, Failed, Identified, Indexed, MappingValidated, Partitioned,
        Projected, Published, Reasoned, Registered, SourcePlanned, SpineWritten,
    };
    matches!(
        (from, to),
        (Registered, SourcePlanned)
            | (SourcePlanned, MappingValidated)
            | (MappingValidated, Partitioned)
            | (Partitioned, Projected)
            | (Projected, Identified)
            | (Identified, SpineWritten)
            | (SpineWritten, Indexed)
            | (Indexed, Reasoned)
            | (Reasoned, Certified)
            | (Certified, Published)
            | (
                Registered
                    | SourcePlanned
                    | MappingValidated
                    | Partitioned
                    | Projected
                    | Identified
                    | SpineWritten
                    | Indexed
                    | Reasoned,
                Failed | Cancelled
            )
    )
}

/// Immutable request fields stored beside a compilation operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateCompilation {
    /// Normalized object key below the operator-owned storage root.
    pub bundle_object_key: String,
    /// SHA-256 of the bundle bytes.
    pub bundle_sha256: [u8; 32],
    /// Snapshot expected to be active if publication is requested.
    pub parent_snapshot_id: Option<Uuid>,
    /// Preallocated immutable snapshot identifier.
    pub target_snapshot_id: Uuid,
    /// Manual or automatic guarded publication.
    pub publication_policy: PublicationPolicy,
    /// Operator-owned hardware/resource profile.
    pub resource_profile: String,
}

/// Durable operation projection returned to API, operator, and worker.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Operation {
    /// Tenant boundary applied through PostgreSQL row-level security.
    pub tenant_id: Uuid,
    /// Durable operation identity.
    pub operation_id: Uuid,
    /// Dataset being compiled.
    pub dataset_id: Uuid,
    /// Current closed state.
    pub state: JobState,
    /// Compare-and-swap revision.
    pub revision: i64,
    /// Immutable target snapshot.
    pub target_snapshot_id: Uuid,
    /// Optional machine-readable terminal error code.
    pub error_code: Option<String>,
    /// Optional immutable error artifact object key.
    pub error_artifact_uri: Option<String>,
}

/// Operation plus immutable request data used to validate CR and worker input.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompilationOperation {
    /// Durable state.
    pub operation: Operation,
    /// Checksum-bound compilation request.
    pub request: CreateCompilation,
    /// Dataset-wide namespace used for deterministic identity assignment.
    pub identity_namespace: Uuid,
    /// Dataset projection-policy identifier required by the bundle.
    pub policy_version: String,
}

/// Certified or published immutable snapshot record.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Snapshot {
    /// Tenant owner.
    pub tenant_id: Uuid,
    /// Dataset owner.
    pub dataset_id: Uuid,
    /// Immutable snapshot ID.
    pub snapshot_id: Uuid,
    /// Publication compare-and-swap predecessor.
    pub parent_snapshot_id: Option<Uuid>,
    /// Producing operation.
    pub operation_id: Uuid,
    /// Exact object key of `snapshot-manifest.json`.
    pub manifest_object_key: String,
    /// Manifest SHA-256.
    pub manifest_sha256: String,
    /// `CERTIFIED`, `PUBLISHED`, or `RETIRED`.
    pub state: String,
}

/// Immutable identity recorded before a SPARQL execution starts.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BeginQueryExecutionLog {
    pub query_execution_id: Uuid,
    pub dataset_id: Uuid,
    pub principal_id: String,
    pub request_id: String,
    pub query_sha256: [u8; 32],
    pub query_text: Option<String>,
    pub start_time_epoch_ms: i64,
}

/// Terminal resource and timing evidence for one SPARQL execution.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FinalizeQueryExecutionLog {
    pub snapshot_id: Option<Uuid>,
    pub query_form: Option<String>,
    pub execution_mode: Option<String>,
    pub status: String,
    pub participating_nodes: Option<i32>,
    pub allocated_cpu_millis: Option<i64>,
    pub allocated_memory_bytes: Option<i64>,
    pub result_rows: Option<i64>,
    pub result_bytes: Option<i64>,
    pub cache_hit: Option<bool>,
    pub end_time_epoch_ms: i64,
    pub total_duration_ms: i64,
    pub error_code: Option<String>,
}

/// Tenant-scoped query-log filters. A principal filter is mandatory for callers
/// without the tenant-wide `query-logs:read` scope.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct QueryExecutionLogFilter {
    pub dataset_id: Option<Uuid>,
    pub principal_id: Option<String>,
    pub status: Option<String>,
    pub started_after_epoch_ms: Option<i64>,
    pub started_before_epoch_ms: Option<i64>,
    pub min_duration_ms: Option<i64>,
    pub limit: i64,
    pub offset: i64,
}

/// Durable query execution record returned through the enterprise audit API.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryExecutionLogRecord {
    pub tenant_id: Uuid,
    pub query_execution_id: Uuid,
    pub dataset_id: Uuid,
    pub snapshot_id: Option<Uuid>,
    pub principal_id: String,
    pub request_id: String,
    pub query_sha256: String,
    pub query_text: Option<String>,
    pub query_form: Option<String>,
    pub execution_mode: Option<String>,
    pub status: String,
    pub participating_nodes: Option<i32>,
    pub allocated_cpu_millis: Option<i64>,
    pub allocated_memory_bytes: Option<i64>,
    pub result_rows: Option<i64>,
    pub result_bytes: Option<i64>,
    pub cache_hit: Option<bool>,
    pub start_time_epoch_ms: i64,
    pub end_time_epoch_ms: Option<i64>,
    pub total_duration_ms: Option<i64>,
    pub error_code: Option<String>,
}

/// Result of certification plus optional automatic publication.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CertificationOutcome {
    /// Durable snapshot record after the operation.
    pub snapshot: Snapshot,
    /// True only if active-snapshot CAS succeeded.
    pub published: bool,
    /// True when automatic publication was requested but lost its CAS.
    pub publication_conflict: bool,
}

/// Catalog failures remain distinct so public boundaries can fail precisely.
#[derive(Debug, Error)]
pub enum CatalogError {
    /// PostgreSQL dependency or invariant failure.
    #[error("catalog dependency failed: {0}")]
    Database(#[from] sqlx::Error),
    /// Idempotency key was reused for different canonical request bytes.
    #[error("idempotency key was reused with a different request")]
    IdempotencyConflict,
    /// Dataset or operation does not exist inside the authorized tenant.
    #[error("catalog resource was not found")]
    NotFound,
    /// Client-selected dataset identity conflicts with durable fields.
    #[error("dataset identity conflicts with the durable catalog record")]
    DatasetConflict,
    /// Target snapshot already belongs to another operation.
    #[error("target snapshot identity conflicts with a durable request")]
    SnapshotConflict,
    /// Closed state machine rejected an edge.
    #[error("illegal state transition from {from:?} to {to:?}")]
    IllegalTransition { from: JobState, to: JobState },
    /// Optimistic concurrency lost.
    #[error("compare-and-swap lost: expected revision {expected}")]
    RevisionConflict { expected: i64 },
    /// Publication predecessor was no longer active.
    #[error("active snapshot changed before publication")]
    PublicationConflict,
    /// Catalog contains a state outside the closed application vocabulary.
    #[error("catalog contains unknown job state {0}")]
    UnknownState(String),
    /// Existing certified content differs from a retry.
    #[error("certified snapshot content conflicts with the retry")]
    CertificationConflict,
}

/// PostgreSQL-backed source of operation and publication truth.
#[derive(Clone)]
pub struct OperationRepository {
    pool: PgPool,
}

/// Human-addressable dataset identity backed by an opaque internal UUID.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DatasetRecord {
    /// Opaque internal machine identity.
    pub dataset_id: Uuid,
    /// Tenant-scoped human-readable name such as `supply_chain`.
    pub dataset_name: String,
    /// Stable namespace used to derive entity GUIDs.
    pub identity_namespace: Uuid,
    /// Immutable projection policy identity.
    pub policy_version: String,
}

impl OperationRepository {
    /// Construct a repository over a validated pool.
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Verify PostgreSQL and the latest catalog schema before readiness succeeds.
    pub async fn ready(&self) -> Result<(), CatalogError> {
        let current: Option<i64> =
            sqlx::query_scalar("SELECT max(version) FROM _sqlx_migrations WHERE success = true")
                .fetch_one(&self.pool)
                .await?;
        // Enterprise query audit raises the live schema floor to version 9.
        if current.unwrap_or_default() < 9 {
            return Err(CatalogError::Database(sqlx::Error::Protocol(
                "catalog migrations through version 9 are required".to_owned(),
            )));
        }
        Ok(())
    }

    /// Create an idempotent client-selected dataset.
    pub async fn create_dataset(
        &self,
        tenant_id: Uuid,
        dataset_id: Uuid,
        identity_namespace: Uuid,
        policy_version: &str,
    ) -> Result<(), CatalogError> {
        let mut tx = self.pool.begin().await?;
        set_tenant(&mut tx, tenant_id).await?;
        let inserted = sqlx::query(
            "INSERT INTO dataset (tenant_id, dataset_id, dataset_name, identity_namespace, policy_version) \
             VALUES ($1, $2, $3, $4, $5) ON CONFLICT (tenant_id, dataset_id) DO NOTHING",
        )
        .bind(tenant_id)
        .bind(dataset_id)
        .bind(dataset_id.to_string())
        .bind(identity_namespace)
        .bind(policy_version)
        .execute(&mut *tx)
        .await;
        if let Err(error) = inserted {
            if is_unique_violation(&error) {
                return Err(CatalogError::DatasetConflict);
            }
            return Err(error.into());
        }
        let row = sqlx::query(
            "SELECT identity_namespace, policy_version FROM dataset \
             WHERE tenant_id = $1 AND dataset_id = $2",
        )
        .bind(tenant_id)
        .bind(dataset_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(CatalogError::NotFound)?;
        let observed_namespace: Uuid = row.try_get("identity_namespace")?;
        let observed_policy: String = row.try_get("policy_version")?;
        if observed_namespace != identity_namespace || observed_policy != policy_version {
            return Err(CatalogError::DatasetConflict);
        }
        tx.commit().await?;
        Ok(())
    }

    /// Create or return one tenant-scoped human-named dataset with a server-generated UUID.
    pub async fn create_or_get_named_dataset(
        &self,
        tenant_id: Uuid,
        dataset_name: &str,
        identity_namespace: Uuid,
        policy_version: &str,
    ) -> Result<DatasetRecord, CatalogError> {
        let mut tx = self.pool.begin().await?;
        set_tenant(&mut tx, tenant_id).await?;
        let generated_id = Uuid::new_v4();
        let inserted = sqlx::query(
            "INSERT INTO dataset (tenant_id, dataset_id, dataset_name, identity_namespace, policy_version) \
             VALUES ($1, $2, $3, $4, $5) ON CONFLICT (tenant_id, dataset_name) DO NOTHING",
        )
        .bind(tenant_id)
        .bind(generated_id)
        .bind(dataset_name)
        .bind(identity_namespace)
        .bind(policy_version)
        .execute(&mut *tx)
        .await;
        if let Err(error) = inserted {
            if is_unique_violation(&error) {
                return Err(CatalogError::DatasetConflict);
            }
            return Err(error.into());
        }
        let row = sqlx::query(
            "SELECT dataset_id, dataset_name, identity_namespace, policy_version FROM dataset \
             WHERE tenant_id = $1 AND dataset_name = $2",
        )
        .bind(tenant_id)
        .bind(dataset_name)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(CatalogError::NotFound)?;
        let record = DatasetRecord {
            dataset_id: row.try_get("dataset_id")?,
            dataset_name: row.try_get("dataset_name")?,
            identity_namespace: row.try_get("identity_namespace")?,
            policy_version: row.try_get("policy_version")?,
        };
        if record.identity_namespace != identity_namespace || record.policy_version != policy_version {
            return Err(CatalogError::DatasetConflict);
        }
        tx.commit().await?;
        Ok(record)
    }

    /// Resolve a tenant-scoped human dataset name to its opaque internal UUID.
    pub async fn resolve_dataset_name(
        &self,
        tenant_id: Uuid,
        dataset_name: &str,
    ) -> Result<Uuid, CatalogError> {
        let mut tx = self.pool.begin().await?;
        set_tenant(&mut tx, tenant_id).await?;
        let dataset_id = sqlx::query_scalar(
            "SELECT dataset_id FROM dataset WHERE tenant_id = $1 AND dataset_name = $2",
        )
        .bind(tenant_id)
        .bind(dataset_name)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(CatalogError::NotFound)?;
        tx.commit().await?;
        Ok(dataset_id)
    }

    /// Return whether a tenant-scoped dataset exists.
    pub async fn dataset_exists(
        &self,
        tenant_id: Uuid,
        dataset_id: Uuid,
    ) -> Result<bool, CatalogError> {
        let mut tx = self.pool.begin().await?;
        set_tenant(&mut tx, tenant_id).await?;
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM dataset WHERE tenant_id = $1 AND dataset_id = $2)",
        )
        .bind(tenant_id)
        .bind(dataset_id)
        .fetch_one(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(exists)
    }

    /// Create a compilation or return its durable identity for an exact retry.
    pub async fn create_or_get_compilation(
        &self,
        tenant_id: Uuid,
        dataset_id: Uuid,
        idempotency_key: &str,
        request_hash: &[u8; 32],
        request: &CreateCompilation,
        actor: &str,
    ) -> Result<CompilationOperation, CatalogError> {
        self.create_or_get_compilation_with_operation_id(
            tenant_id,
            dataset_id,
            Uuid::new_v4(),
            idempotency_key,
            request_hash,
            request,
            actor,
        )
        .await
    }

    /// Register a control-plane-derived operation identity for a cloud import.
    /// Retries still resolve by tenant idempotency key and verify every request byte.
    pub async fn create_or_get_compilation_with_operation_id(
        &self,
        tenant_id: Uuid,
        dataset_id: Uuid,
        operation_id: Uuid,
        idempotency_key: &str,
        request_hash: &[u8; 32],
        request: &CreateCompilation,
        actor: &str,
    ) -> Result<CompilationOperation, CatalogError> {
        if operation_id.is_nil() {
            return Err(CatalogError::IdempotencyConflict);
        }
        let mut tx = self.pool.begin().await?;
        set_tenant(&mut tx, tenant_id).await?;
        let dataset_exists: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM dataset WHERE tenant_id = $1 AND dataset_id = $2)",
        )
        .bind(tenant_id)
        .bind(dataset_id)
        .fetch_one(&mut *tx)
        .await?;
        if !dataset_exists {
            return Err(CatalogError::NotFound);
        }
        if let Some(parent_snapshot_id) = request.parent_snapshot_id {
            let parent_exists: bool = sqlx::query_scalar(
                "SELECT EXISTS (SELECT 1 FROM snapshot \
                 WHERE tenant_id = $1 AND dataset_id = $2 AND snapshot_id = $3)",
            )
            .bind(tenant_id)
            .bind(dataset_id)
            .bind(parent_snapshot_id)
            .fetch_one(&mut *tx)
            .await?;
            if !parent_exists {
                return Err(CatalogError::SnapshotConflict);
            }
        }
        let inserted = sqlx::query(
            "INSERT INTO operation \
             (tenant_id, operation_id, dataset_id, idempotency_key, request_hash, state, target_snapshot_id) \
             VALUES ($1, $2, $3, $4, $5, 'REGISTERED', $6) \
             ON CONFLICT (tenant_id, idempotency_key) DO NOTHING",
        )
        .bind(tenant_id)
        .bind(operation_id)
        .bind(dataset_id)
        .bind(idempotency_key)
        .bind(request_hash.as_slice())
        .bind(request.target_snapshot_id)
        .execute(&mut *tx)
        .await?;
        if inserted.rows_affected() == 1 {
            let result = sqlx::query(
                "INSERT INTO compilation_request \
                 (tenant_id, operation_id, bundle_object_key, bundle_sha256, parent_snapshot_id, \
                  target_snapshot_id, publication_policy, resource_profile) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7::ngkg_publication_policy, $8)",
            )
            .bind(tenant_id)
            .bind(operation_id)
            .bind(&request.bundle_object_key)
            .bind(request.bundle_sha256.as_slice())
            .bind(request.parent_snapshot_id)
            .bind(request.target_snapshot_id)
            .bind(request.publication_policy.as_db())
            .bind(&request.resource_profile)
            .execute(&mut *tx)
            .await;
            if let Err(error) = result {
                if is_unique_violation(&error) {
                    return Err(CatalogError::SnapshotConflict);
                }
                return Err(error.into());
            }
            sqlx::query(
                "INSERT INTO operation_audit \
                 (tenant_id, operation_id, revision, previous_state, new_state, actor) \
                 VALUES ($1, $2, 0, NULL, 'REGISTERED', $3)",
            )
            .bind(tenant_id)
            .bind(operation_id)
            .bind(actor)
            .execute(&mut *tx)
            .await?;
        }
        let (durable, observed_hash) =
            load_compilation_by_idempotency(&mut tx, tenant_id, idempotency_key).await?;
        if observed_hash.as_slice() != request_hash
            || durable.operation.dataset_id != dataset_id
            || durable.request != *request
        {
            return Err(CatalogError::IdempotencyConflict);
        }
        tx.commit().await?;
        Ok(durable)
    }

    /// Load one authorized operation and its immutable request.
    pub async fn get_compilation(
        &self,
        tenant_id: Uuid,
        operation_id: Uuid,
    ) -> Result<CompilationOperation, CatalogError> {
        let mut tx = self.pool.begin().await?;
        set_tenant(&mut tx, tenant_id).await?;
        let value = load_compilation(&mut tx, tenant_id, operation_id).await?;
        tx.commit().await?;
        Ok(value)
    }

    /// Atomically register exact projection/reducer work and advance to PARTITIONED.
    pub async fn register_distributed_plan(
        &self,
        tenant_id: Uuid,
        operation_id: Uuid,
        plan: &RegisterDistributedPlan,
        actor: &str,
    ) -> Result<DistributedPlanSummary, CatalogError> {
        validate_distributed_registration(plan)?;
        let mut tx = self.pool.begin().await?;
        set_tenant(&mut tx, tenant_id).await?;
        let mut operation = load_operation_for_update(&mut tx, tenant_id, operation_id).await?;
        let inserted = sqlx::query(
            "INSERT INTO distributed_plan \
             (tenant_id, operation_id, source_plan_object_key, source_plan_sha256, \
              logical_partition_count, reducer_count, fact_count, layout_profile) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8) \
             ON CONFLICT (tenant_id, operation_id) DO NOTHING",
        )
        .bind(tenant_id)
        .bind(operation_id)
        .bind(&plan.source_plan_object_key)
        .bind(plan.source_plan_sha256.as_slice())
        .bind(plan.logical_partition_count)
        .bind(plan.reducer_count)
        .bind(plan.fact_count)
        .bind(&plan.layout_profile)
        .execute(&mut *tx)
        .await?
        .rows_affected();
        if inserted == 1 {
            for (kind, work) in [
                (DistributedWorkKind::Projection, &plan.projections),
                (DistributedWorkKind::Reducer, &plan.reducers),
            ] {
                for item in work {
                    sqlx::query(
                        "INSERT INTO distributed_work \
                         (tenant_id, operation_id, work_kind, work_index, stable_work_id, \
                          input_object_key, input_sha256) \
                         VALUES ($1, $2, $3::ngkg_work_kind, $4, $5, $6, $7)",
                    )
                    .bind(tenant_id)
                    .bind(operation_id)
                    .bind(kind.as_db())
                    .bind(item.work_index)
                    .bind(&item.stable_work_id)
                    .bind(&item.input_object_key)
                    .bind(item.input_sha256.as_slice())
                    .execute(&mut *tx)
                    .await?;
                }
            }
            if operation.state != JobState::Registered {
                return Err(CatalogError::CertificationConflict);
            }
            for next in [
                JobState::SourcePlanned,
                JobState::MappingValidated,
                JobState::Partitioned,
            ] {
                transition_locked(&mut tx, &mut operation, next, actor, None, None).await?;
            }
        } else {
            verify_distributed_registration(&mut tx, tenant_id, operation_id, plan).await?;
            if !matches!(
                operation.state,
                JobState::Partitioned
                    | JobState::Projected
                    | JobState::Identified
                    | JobState::SpineWritten
                    | JobState::Indexed
                    | JobState::Reasoned
                    | JobState::Certified
                    | JobState::Published
            ) {
                return Err(CatalogError::CertificationConflict);
            }
        }
        let summary = load_distributed_summary(&mut tx, tenant_id, operation_id).await?;
        tx.commit().await?;
        Ok(summary)
    }

    /// Return stage counts for level-based operator reconciliation.
    pub async fn get_distributed_plan(
        &self,
        tenant_id: Uuid,
        operation_id: Uuid,
    ) -> Result<DistributedPlanSummary, CatalogError> {
        let mut tx = self.pool.begin().await?;
        set_tenant(&mut tx, tenant_id).await?;
        let summary = load_distributed_summary(&mut tx, tenant_id, operation_id).await?;
        tx.commit().await?;
        Ok(summary)
    }

    /// Resolve one completion index to its immutable input without storage listing.
    pub async fn get_distributed_work(
        &self,
        tenant_id: Uuid,
        operation_id: Uuid,
        work_kind: DistributedWorkKind,
        work_index: i32,
    ) -> Result<DistributedWorkItem, CatalogError> {
        let mut tx = self.pool.begin().await?;
        set_tenant(&mut tx, tenant_id).await?;
        let item =
            load_distributed_work(&mut tx, tenant_id, operation_id, work_kind, work_index).await?;
        tx.commit().await?;
        Ok(item)
    }

    /// Return exact successful output manifests in completion-index order.
    pub async fn list_distributed_outputs(
        &self,
        tenant_id: Uuid,
        operation_id: Uuid,
        work_kind: DistributedWorkKind,
    ) -> Result<Vec<DistributedWorkItem>, CatalogError> {
        let mut tx = self.pool.begin().await?;
        set_tenant(&mut tx, tenant_id).await?;
        let rows = sqlx::query(
            "SELECT work_index, stable_work_id, input_object_key, \
                    encode(input_sha256, 'hex') AS input_sha256, state::text AS state, \
                    output_manifest_object_key, \
                    CASE WHEN output_manifest_sha256 IS NULL THEN NULL \
                         ELSE encode(output_manifest_sha256, 'hex') END AS output_manifest_sha256 \
             FROM distributed_work WHERE tenant_id = $1 AND operation_id = $2 \
               AND work_kind = $3::ngkg_work_kind AND state = 'SUCCEEDED' \
             ORDER BY work_index",
        )
        .bind(tenant_id)
        .bind(operation_id)
        .bind(work_kind.as_db())
        .fetch_all(&mut *tx)
        .await?;
        let values = rows
            .iter()
            .map(|row| distributed_work_from_row(work_kind, row))
            .collect::<Result<Vec<_>, _>>()?;
        tx.commit().await?;
        Ok(values)
    }

    /// Commit one immutable output. The last projection/reducer advances its barrier.
    pub async fn commit_distributed_work(
        &self,
        tenant_id: Uuid,
        operation_id: Uuid,
        work_kind: DistributedWorkKind,
        work_index: i32,
        output_manifest_object_key: &str,
        output_manifest_sha256: &[u8; 32],
        actor: &str,
    ) -> Result<DistributedPlanSummary, CatalogError> {
        let mut tx = self.pool.begin().await?;
        set_tenant(&mut tx, tenant_id).await?;
        let mut operation = load_operation_for_update(&mut tx, tenant_id, operation_id).await?;
        let existing = load_distributed_work_for_update(
            &mut tx,
            tenant_id,
            operation_id,
            work_kind,
            work_index,
        )
        .await?;
        if existing.state == "SUCCEEDED" {
            let expected_manifest_sha256 = hex::encode(output_manifest_sha256);
            if existing.output_manifest_object_key.as_deref() != Some(output_manifest_object_key)
                || existing.output_manifest_sha256.as_deref()
                    != Some(expected_manifest_sha256.as_str())
            {
                return Err(CatalogError::CertificationConflict);
            }
        } else if existing.state == "PENDING" {
            let required_state = match work_kind {
                DistributedWorkKind::Projection => JobState::Partitioned,
                DistributedWorkKind::Reducer => JobState::Projected,
                DistributedWorkKind::Artifact => JobState::Indexed,
            };
            if operation.state != required_state {
                return Err(CatalogError::IllegalTransition {
                    from: operation.state,
                    to: required_state,
                });
            }
            let changed = sqlx::query(
                "UPDATE distributed_work SET state = 'SUCCEEDED', \
                        output_manifest_object_key = $1, output_manifest_sha256 = $2, completed_at = now() \
                 WHERE tenant_id = $3 AND operation_id = $4 \
                   AND work_kind = $5::ngkg_work_kind AND work_index = $6 AND state = 'PENDING'",
            )
            .bind(output_manifest_object_key)
            .bind(output_manifest_sha256.as_slice())
            .bind(tenant_id)
            .bind(operation_id)
            .bind(work_kind.as_db())
            .bind(work_index)
            .execute(&mut *tx)
            .await?
            .rows_affected();
            if changed != 1 {
                return Err(CatalogError::CertificationConflict);
            }
        } else {
            return Err(CatalogError::CertificationConflict);
        }
        let summary = load_distributed_summary(&mut tx, tenant_id, operation_id).await?;
        match work_kind {
            DistributedWorkKind::Projection
                if operation.state == JobState::Partitioned
                    && summary.succeeded_projections
                        == i64::from(summary.logical_partition_count) =>
            {
                transition_locked(
                    &mut tx,
                    &mut operation,
                    JobState::Projected,
                    actor,
                    None,
                    None,
                )
                .await?;
            }
            DistributedWorkKind::Reducer
                if operation.state == JobState::Projected
                    && summary.succeeded_reducers == i64::from(summary.reducer_count) =>
            {
                for next in [
                    JobState::Identified,
                    JobState::SpineWritten,
                    JobState::Indexed,
                ] {
                    transition_locked(&mut tx, &mut operation, next, actor, None, None).await?;
                }
            }
            DistributedWorkKind::Artifact => {}
            _ => {}
        }
        let summary = load_distributed_summary(&mut tx, tenant_id, operation_id).await?;
        tx.commit().await?;
        Ok(summary)
    }

    /// Publish the immutable reducer root once every reducer has succeeded.
    pub async fn commit_distributed_root(
        &self,
        tenant_id: Uuid,
        operation_id: Uuid,
        root: &DistributedRoot,
    ) -> Result<DistributedRoot, CatalogError> {
        let root_manifest_sha256 = decode_catalog_sha256(&root.root_manifest_sha256)?;
        let canonical_source_sha256 = decode_catalog_sha256(&root.canonical_source_sha256)?;
        let dictionary_sha256 = decode_catalog_sha256(&root.dictionary_sha256)?;
        let semantic_content_sha256 = decode_catalog_sha256(&root.semantic_content_sha256)?;
        let mut tx = self.pool.begin().await?;
        set_tenant(&mut tx, tenant_id).await?;
        let operation = load_operation_for_update(&mut tx, tenant_id, operation_id).await?;
        if operation.state != JobState::Indexed {
            return Err(CatalogError::IllegalTransition {
                from: operation.state,
                to: JobState::Indexed,
            });
        }
        let summary = load_distributed_summary(&mut tx, tenant_id, operation_id).await?;
        if summary.failed_work != 0
            || summary.succeeded_reducers != i64::from(summary.reducer_count)
        {
            return Err(CatalogError::CertificationConflict);
        }
        sqlx::query(
            "INSERT INTO distributed_root \
             (tenant_id, operation_id, root_manifest_object_key, root_manifest_sha256, \
              canonical_source_object_key, canonical_source_sha256, dictionary_object_key, \
              dictionary_sha256, semantic_content_sha256) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) \
             ON CONFLICT (tenant_id, operation_id) DO NOTHING",
        )
        .bind(tenant_id)
        .bind(operation_id)
        .bind(&root.root_manifest_object_key)
        .bind(root_manifest_sha256.as_slice())
        .bind(&root.canonical_source_object_key)
        .bind(canonical_source_sha256.as_slice())
        .bind(&root.dictionary_object_key)
        .bind(dictionary_sha256.as_slice())
        .bind(semantic_content_sha256.as_slice())
        .execute(&mut *tx)
        .await?;
        let observed = load_distributed_root(&mut tx, tenant_id, operation_id).await?;
        if observed != *root {
            return Err(CatalogError::CertificationConflict);
        }
        tx.commit().await?;
        Ok(observed)
    }

    /// Load the exact global reducer root; absence means finalization is incomplete.
    pub async fn get_distributed_root(
        &self,
        tenant_id: Uuid,
        operation_id: Uuid,
    ) -> Result<DistributedRoot, CatalogError> {
        let mut tx = self.pool.begin().await?;
        set_tenant(&mut tx, tenant_id).await?;
        let root = load_distributed_root(&mut tx, tenant_id, operation_id).await?;
        tx.commit().await?;
        Ok(root)
    }

    /// Register one immutable artifact plan and every exact completion index.
    pub async fn register_artifact_plan(
        &self,
        tenant_id: Uuid,
        operation_id: Uuid,
        plan: &RegisterArtifactPlan,
    ) -> Result<ArtifactPlanSummary, CatalogError> {
        if plan.partition_count <= 0
            || plan.partition_count > 65_536
            || plan.row_group_rows <= 0
            || plan.source_plan_object_key.is_empty()
            || plan.dictionary_object_key.is_empty()
            || plan.artifact_plan_object_key.is_empty()
            || usize::try_from(plan.partition_count).ok() != Some(plan.work.len())
            || plan.work.iter().enumerate().any(|(index, item)| {
                usize::try_from(item.work_index).ok() != Some(index)
                    || item.stable_work_id.len() != 71
                    || !item.stable_work_id.starts_with("blake3:")
                    || !item.stable_work_id[7..]
                        .bytes()
                        .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
                    || item.input_object_key.is_empty()
            })
        {
            return Err(CatalogError::CertificationConflict);
        }
        let mut tx = self.pool.begin().await?;
        set_tenant(&mut tx, tenant_id).await?;
        let operation = load_operation_for_update(&mut tx, tenant_id, operation_id).await?;
        if operation.state != JobState::Indexed {
            return Err(CatalogError::IllegalTransition {
                from: operation.state,
                to: JobState::Indexed,
            });
        }
        let distributed_plan = load_distributed_summary(&mut tx, tenant_id, operation_id).await?;
        let distributed_root = load_distributed_root(&mut tx, tenant_id, operation_id).await?;
        if distributed_plan.source_plan_object_key != plan.source_plan_object_key
            || distributed_plan.source_plan_sha256 != hex::encode(plan.source_plan_sha256)
            || distributed_root.dictionary_object_key != plan.dictionary_object_key
            || distributed_root.dictionary_sha256 != hex::encode(plan.dictionary_sha256)
            || distributed_plan.logical_partition_count != plan.partition_count
        {
            return Err(CatalogError::CertificationConflict);
        }
        let inserted = sqlx::query(
            "INSERT INTO distributed_artifact_plan \
             (tenant_id, operation_id, source_plan_object_key, source_plan_sha256, \
              dictionary_object_key, dictionary_sha256, artifact_plan_object_key, \
              artifact_plan_sha256, partition_count, row_group_rows) \
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) \
             ON CONFLICT (tenant_id, operation_id) DO NOTHING",
        )
        .bind(tenant_id)
        .bind(operation_id)
        .bind(&plan.source_plan_object_key)
        .bind(plan.source_plan_sha256.as_slice())
        .bind(&plan.dictionary_object_key)
        .bind(plan.dictionary_sha256.as_slice())
        .bind(&plan.artifact_plan_object_key)
        .bind(plan.artifact_plan_sha256.as_slice())
        .bind(plan.partition_count)
        .bind(plan.row_group_rows)
        .execute(&mut *tx)
        .await?
        .rows_affected();
        if inserted == 1 {
            for item in &plan.work {
                sqlx::query(
                    "INSERT INTO distributed_work \
                     (tenant_id, operation_id, work_kind, work_index, stable_work_id, \
                      input_object_key, input_sha256) \
                     VALUES ($1,$2,'ARTIFACT'::ngkg_work_kind,$3,$4,$5,$6)",
                )
                .bind(tenant_id)
                .bind(operation_id)
                .bind(item.work_index)
                .bind(&item.stable_work_id)
                .bind(&item.input_object_key)
                .bind(item.input_sha256.as_slice())
                .execute(&mut *tx)
                .await?;
            }
        } else {
            let observed = load_artifact_plan_summary(&mut tx, tenant_id, operation_id).await?;
            if observed.source_plan_object_key != plan.source_plan_object_key
                || observed.source_plan_sha256 != hex::encode(plan.source_plan_sha256)
                || observed.dictionary_object_key != plan.dictionary_object_key
                || observed.dictionary_sha256 != hex::encode(plan.dictionary_sha256)
                || observed.artifact_plan_object_key != plan.artifact_plan_object_key
                || observed.artifact_plan_sha256 != hex::encode(plan.artifact_plan_sha256)
                || observed.partition_count != plan.partition_count
                || observed.row_group_rows != plan.row_group_rows
            {
                return Err(CatalogError::CertificationConflict);
            }
            let rows = sqlx::query(
                "SELECT work_index, stable_work_id, input_object_key, \
                        encode(input_sha256, 'hex') AS input_sha256, state::text AS state, \
                        output_manifest_object_key, \
                        CASE WHEN output_manifest_sha256 IS NULL THEN NULL \
                             ELSE encode(output_manifest_sha256, 'hex') END AS output_manifest_sha256 \
                 FROM distributed_work WHERE tenant_id=$1 AND operation_id=$2 \
                   AND work_kind='ARTIFACT'::ngkg_work_kind ORDER BY work_index",
            )
            .bind(tenant_id)
            .bind(operation_id)
            .fetch_all(&mut *tx)
            .await?;
            if rows.len() != plan.work.len() {
                return Err(CatalogError::CertificationConflict);
            }
            for (row, expected) in rows.iter().zip(plan.work.iter()) {
                let observed = distributed_work_from_row(DistributedWorkKind::Artifact, row)?;
                if observed.work_index != expected.work_index
                    || observed.stable_work_id != expected.stable_work_id
                    || observed.input_object_key != expected.input_object_key
                    || observed.input_sha256 != hex::encode(expected.input_sha256)
                {
                    return Err(CatalogError::CertificationConflict);
                }
            }
        }
        let summary = load_artifact_plan_summary(&mut tx, tenant_id, operation_id).await?;
        tx.commit().await?;
        Ok(summary)
    }

    /// Return durable artifact-stage counts and immutable roots.
    pub async fn get_artifact_plan(
        &self,
        tenant_id: Uuid,
        operation_id: Uuid,
    ) -> Result<ArtifactPlanSummary, CatalogError> {
        let mut tx = self.pool.begin().await?;
        set_tenant(&mut tx, tenant_id).await?;
        let summary = load_artifact_plan_summary(&mut tx, tenant_id, operation_id).await?;
        tx.commit().await?;
        Ok(summary)
    }

    /// Commit the immutable global artifact root after every artifact work item succeeds.
    pub async fn commit_artifact_root(
        &self,
        tenant_id: Uuid,
        operation_id: Uuid,
        root: &DistributedArtifactRoot,
    ) -> Result<DistributedArtifactRoot, CatalogError> {
        let root_sha = decode_catalog_sha256(&root.root_manifest_sha256)?;
        let locator_sha = decode_catalog_sha256(&root.locator_sha256)?;
        let semantic_sha = decode_catalog_sha256(&root.semantic_content_sha256)?;
        if root.fact_count < 0
            || root.root_manifest_object_key.is_empty()
            || root.locator_object_key.is_empty()
            || root.semantic_row_count < 0
            || root.payload_row_count < 0
            || root.locator_record_count < 0
            || root.semantic_row_count.checked_add(root.payload_row_count) != Some(root.fact_count)
            || root.payload_row_count != root.locator_record_count
        {
            return Err(CatalogError::CertificationConflict);
        }
        let mut tx = self.pool.begin().await?;
        set_tenant(&mut tx, tenant_id).await?;
        let operation = load_operation_for_update(&mut tx, tenant_id, operation_id).await?;
        if operation.state != JobState::Indexed {
            return Err(CatalogError::IllegalTransition {
                from: operation.state,
                to: JobState::Indexed,
            });
        }
        let plan = load_artifact_plan_summary(&mut tx, tenant_id, operation_id).await?;
        if plan.failed_artifacts != 0 || plan.succeeded_artifacts != i64::from(plan.partition_count)
        {
            return Err(CatalogError::CertificationConflict);
        }
        sqlx::query(
            "INSERT INTO distributed_artifact_root \
             (tenant_id,operation_id,root_manifest_object_key,root_manifest_sha256, \
              locator_object_key,locator_sha256,semantic_content_sha256,fact_count, \
              semantic_row_count,payload_row_count,locator_record_count) \
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) \
             ON CONFLICT (tenant_id,operation_id) DO NOTHING",
        )
        .bind(tenant_id)
        .bind(operation_id)
        .bind(&root.root_manifest_object_key)
        .bind(root_sha.as_slice())
        .bind(&root.locator_object_key)
        .bind(locator_sha.as_slice())
        .bind(semantic_sha.as_slice())
        .bind(root.fact_count)
        .bind(root.semantic_row_count)
        .bind(root.payload_row_count)
        .bind(root.locator_record_count)
        .execute(&mut *tx)
        .await?;
        let observed = load_artifact_root(&mut tx, tenant_id, operation_id).await?;
        if observed != *root {
            return Err(CatalogError::CertificationConflict);
        }
        tx.commit().await?;
        Ok(observed)
    }

    /// Load the immutable global artifact root.
    pub async fn get_artifact_root(
        &self,
        tenant_id: Uuid,
        operation_id: Uuid,
    ) -> Result<DistributedArtifactRoot, CatalogError> {
        let mut tx = self.pool.begin().await?;
        set_tenant(&mut tx, tenant_id).await?;
        let root = load_artifact_root(&mut tx, tenant_id, operation_id).await?;
        tx.commit().await?;
        Ok(root)
    }

    /// Commit the immutable serving root after artifact finalization.
    pub async fn commit_serving_root(
        &self,
        tenant_id: Uuid,
        operation_id: Uuid,
        root: &DistributedServingRoot,
    ) -> Result<DistributedServingRoot, CatalogError> {
        let serving_sha = decode_catalog_sha256(&root.serving_root_sha256)?;
        let binary_sha = decode_catalog_sha256(&root.binary_locator_sha256)?;
        let source_sha = decode_catalog_sha256(&root.source_locator_sha256)?;
        let semantic_sha = decode_catalog_sha256(&root.semantic_content_sha256)?;
        if root.serving_root_object_key.is_empty()
            || root.binary_locator_object_key.is_empty()
            || root.partition_count <= 0
            || root.row_group_rows <= 0
            || root.locator_record_count < 0
        {
            return Err(CatalogError::CertificationConflict);
        }
        let mut tx = self.pool.begin().await?;
        set_tenant(&mut tx, tenant_id).await?;
        let operation = load_operation_for_update(&mut tx, tenant_id, operation_id).await?;
        if operation.state != JobState::Indexed {
            return Err(CatalogError::IllegalTransition {
                from: operation.state,
                to: JobState::Indexed,
            });
        }
        let artifact = load_artifact_root(&mut tx, tenant_id, operation_id).await?;
        let plan = load_artifact_plan_summary(&mut tx, tenant_id, operation_id).await?;
        if artifact.locator_sha256 != root.source_locator_sha256
            || artifact.semantic_content_sha256 != root.semantic_content_sha256
            || artifact.locator_record_count != root.locator_record_count
            || plan.partition_count != root.partition_count
            || plan.row_group_rows != root.row_group_rows
        {
            return Err(CatalogError::CertificationConflict);
        }
        sqlx::query(
            "INSERT INTO distributed_serving_root \
             (tenant_id,operation_id,serving_root_object_key,serving_root_sha256, \
              binary_locator_object_key,binary_locator_sha256,source_locator_sha256, \
              semantic_content_sha256,partition_count,row_group_rows,locator_record_count) \
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) \
             ON CONFLICT (tenant_id,operation_id) DO NOTHING",
        )
        .bind(tenant_id)
        .bind(operation_id)
        .bind(&root.serving_root_object_key)
        .bind(serving_sha.as_slice())
        .bind(&root.binary_locator_object_key)
        .bind(binary_sha.as_slice())
        .bind(source_sha.as_slice())
        .bind(semantic_sha.as_slice())
        .bind(root.partition_count)
        .bind(root.row_group_rows)
        .bind(root.locator_record_count)
        .execute(&mut *tx)
        .await?;
        let observed = load_serving_root(&mut tx, tenant_id, operation_id).await?;
        if observed != *root {
            return Err(CatalogError::CertificationConflict);
        }
        tx.commit().await?;
        Ok(observed)
    }

    /// Load the immutable serving root; absence prevents query-serving cutover.
    pub async fn get_serving_root(
        &self,
        tenant_id: Uuid,
        operation_id: Uuid,
    ) -> Result<DistributedServingRoot, CatalogError> {
        let mut tx = self.pool.begin().await?;
        set_tenant(&mut tx, tenant_id).await?;
        let root = load_serving_root(&mut tx, tenant_id, operation_id).await?;
        tx.commit().await?;
        Ok(root)
    }

    /// Commit exact reference-versus-sharded hydration evidence idempotently.
    pub async fn commit_serving_certification(
        &self,
        tenant_id: Uuid,
        operation_id: Uuid,
        certification: &ServingCertification,
    ) -> Result<ServingCertification, CatalogError> {
        let report_sha = decode_catalog_sha256(&certification.report_sha256)?;
        let serving_sha = decode_catalog_sha256(&certification.serving_root_sha256)?;
        let binary_sha = decode_catalog_sha256(&certification.binary_locator_sha256)?;
        let reference_sha = decode_catalog_sha256(&certification.reference_manifest_sha256)?;
        if certification.report_object_key.is_empty()
            || certification.reference_manifest_object_key.is_empty()
            || certification.certified_query_count <= 0
            || certification.hydrated_row_count < 0
        {
            return Err(CatalogError::CertificationConflict);
        }
        let mut tx = self.pool.begin().await?;
        set_tenant(&mut tx, tenant_id).await?;
        let operation = load_operation_for_update(&mut tx, tenant_id, operation_id).await?;
        if !matches!(
            operation.state,
            JobState::Indexed | JobState::Reasoned | JobState::Certified | JobState::Published
        ) {
            return Err(CatalogError::CertificationConflict);
        }
        let serving_root = load_serving_root(&mut tx, tenant_id, operation_id).await?;
        if serving_root.serving_root_sha256 != certification.serving_root_sha256
            || serving_root.binary_locator_sha256 != certification.binary_locator_sha256
        {
            return Err(CatalogError::CertificationConflict);
        }
        sqlx::query(
            "INSERT INTO distributed_serving_certification \
             (tenant_id,operation_id,report_object_key,report_sha256, \
              serving_root_sha256,binary_locator_sha256, \
              reference_manifest_object_key,reference_manifest_sha256, \
              certified_query_count,hydrated_row_count) \
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) \
             ON CONFLICT (tenant_id,operation_id) DO NOTHING",
        )
        .bind(tenant_id)
        .bind(operation_id)
        .bind(&certification.report_object_key)
        .bind(report_sha.as_slice())
        .bind(serving_sha.as_slice())
        .bind(binary_sha.as_slice())
        .bind(&certification.reference_manifest_object_key)
        .bind(reference_sha.as_slice())
        .bind(certification.certified_query_count)
        .bind(certification.hydrated_row_count)
        .execute(&mut *tx)
        .await?;
        let observed = load_serving_certification(&mut tx, tenant_id, operation_id).await?;
        if observed != *certification {
            return Err(CatalogError::CertificationConflict);
        }
        tx.commit().await?;
        Ok(observed)
    }

    /// Load the immutable serving equivalence evidence.
    pub async fn get_serving_certification(
        &self,
        tenant_id: Uuid,
        operation_id: Uuid,
    ) -> Result<ServingCertification, CatalogError> {
        let mut tx = self.pool.begin().await?;
        set_tenant(&mut tx, tenant_id).await?;
        let certification = load_serving_certification(&mut tx, tenant_id, operation_id).await?;
        tx.commit().await?;
        Ok(certification)
    }

    /// Resolve only the active published snapshot with matching serving evidence.
    pub async fn get_active_serving_snapshot(
        &self,
        tenant_id: Uuid,
        dataset_id: Uuid,
    ) -> Result<ActiveServingSnapshot, CatalogError> {
        let mut tx = self.pool.begin().await?;
        set_tenant(&mut tx, tenant_id).await?;
        let row = sqlx::query(
            "SELECT d.identity_namespace, \
                    s.tenant_id,s.dataset_id,s.snapshot_id,s.parent_snapshot_id,s.operation_id, \
                    s.manifest_object_key,encode(s.manifest_sha256,'hex') AS manifest_sha256, \
                    s.state::text AS state \
             FROM dataset d JOIN snapshot s \
               ON s.tenant_id=d.tenant_id AND s.dataset_id=d.dataset_id \
              AND s.snapshot_id=d.active_snapshot_id \
             WHERE d.tenant_id=$1 AND d.dataset_id=$2 AND s.state='PUBLISHED'",
        )
        .bind(tenant_id)
        .bind(dataset_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(CatalogError::NotFound)?;
        let identity_namespace: Uuid = row.try_get("identity_namespace")?;
        let snapshot = snapshot_from_row(&row)?;
        let cloud_activation = match load_cloud_activation(&mut tx, tenant_id, snapshot.operation_id).await {
            Ok(value) => Some(value),
            Err(CatalogError::NotFound) => None,
            Err(error) => return Err(error),
        };
        if cloud_activation.is_some() {
            let bound: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM cloud_snapshot_activation \
                 WHERE tenant_id=$1 AND operation_id=$2 \
                   AND reference_manifest_object_key=$3 \
                   AND encode(reference_manifest_sha256,'hex')=$4 \
                   AND dataset_id=$5 AND snapshot_id=$6)",
            )
            .bind(tenant_id)
            .bind(snapshot.operation_id)
            .bind(&snapshot.manifest_object_key)
            .bind(&snapshot.manifest_sha256)
            .bind(snapshot.dataset_id)
            .bind(snapshot.snapshot_id)
            .fetch_one(&mut *tx)
            .await?;
            if !bound {
                return Err(CatalogError::CertificationConflict);
            }
        }
        let (serving_root, serving_certification) = if cloud_activation.is_some() {
            (None, None)
        } else {
            let root = load_serving_root(&mut tx, tenant_id, snapshot.operation_id).await?;
            let certification =
                load_serving_certification(&mut tx, tenant_id, snapshot.operation_id).await?;
            if certification.reference_manifest_object_key != snapshot.manifest_object_key
                || certification.reference_manifest_sha256 != snapshot.manifest_sha256
                || certification.serving_root_sha256 != root.serving_root_sha256
                || certification.binary_locator_sha256 != root.binary_locator_sha256
            {
                return Err(CatalogError::CertificationConflict);
            }
            (Some(root), Some(certification))
        };
        tx.commit().await?;
        Ok(ActiveServingSnapshot {
            snapshot,
            identity_namespace,
            serving_root,
            serving_certification,
            cloud_activation,
        })
    }

    /// Resolve every catalog root needed to back up a certified or published snapshot.
    pub async fn get_snapshot_recovery_roots(
        &self,
        tenant_id: Uuid,
        dataset_id: Uuid,
        snapshot_id: Uuid,
    ) -> Result<SnapshotRecoveryRoots, CatalogError> {
        let mut tx = self.pool.begin().await?;
        set_tenant(&mut tx, tenant_id).await?;
        let row = sqlx::query(
            "SELECT tenant_id,dataset_id,snapshot_id,parent_snapshot_id,operation_id, \
                    manifest_object_key,encode(manifest_sha256,'hex') AS manifest_sha256,state::text AS state \
             FROM snapshot WHERE tenant_id=$1 AND dataset_id=$2 AND snapshot_id=$3",
        )
        .bind(tenant_id)
        .bind(dataset_id)
        .bind(snapshot_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(CatalogError::NotFound)?;
        let snapshot = snapshot_from_row(&row)?;
        let cloud_activation = match load_cloud_activation(&mut tx, tenant_id, snapshot.operation_id).await {
            Ok(value) => Some(value),
            Err(CatalogError::NotFound) => None,
            Err(error) => return Err(error),
        };
        let serving_root = if cloud_activation.is_none() {
            match load_serving_root(&mut tx, tenant_id, snapshot.operation_id).await {
                Ok(value) => Some(value),
                Err(CatalogError::NotFound) => None,
                Err(error) => return Err(error),
            }
        } else {
            None
        };
        tx.commit().await?;
        Ok(SnapshotRecoveryRoots { snapshot, cloud_activation, serving_root })
    }

    /// Resolve the active serving snapshot from an owned repository handle.
    ///
    /// Axum requires handler futures to be `Send + 'static`. Moving the cheap,
    /// cloneable repository handle into this future prevents a request future
    /// from retaining a borrow of application state while PostgreSQL is
    /// awaited.
    pub async fn get_active_serving_snapshot_owned(
        self,
        tenant_id: Uuid,
        dataset_id: Uuid,
    ) -> Result<ActiveServingSnapshot, CatalogError> {
        self.get_active_serving_snapshot(tenant_id, dataset_id)
            .await
    }

    /// Cancel a nonterminal operation with revision compare-and-swap.
    pub async fn cancel(
        &self,
        tenant_id: Uuid,
        operation_id: Uuid,
        expected_revision: i64,
        actor: &str,
    ) -> Result<Operation, CatalogError> {
        let mut tx = self.pool.begin().await?;
        set_tenant(&mut tx, tenant_id).await?;
        let mut operation = load_operation_for_update(&mut tx, tenant_id, operation_id).await?;
        if operation.revision != expected_revision {
            return Err(CatalogError::RevisionConflict {
                expected: expected_revision,
            });
        }
        transition_locked(
            &mut tx,
            &mut operation,
            JobState::Cancelled,
            actor,
            None,
            None,
        )
        .await?;
        tx.commit().await?;
        Ok(operation)
    }

    /// Mark a deterministic terminal failure without overwriting prior terminal state.
    pub async fn fail(
        &self,
        tenant_id: Uuid,
        operation_id: Uuid,
        error_code: &str,
        error_artifact_uri: Option<&str>,
        actor: &str,
    ) -> Result<Operation, CatalogError> {
        let mut tx = self.pool.begin().await?;
        set_tenant(&mut tx, tenant_id).await?;
        let mut operation = load_operation_for_update(&mut tx, tenant_id, operation_id).await?;
        if operation.state == JobState::Failed {
            if operation.error_code.as_deref() != Some(error_code)
                || operation.error_artifact_uri.as_deref() != error_artifact_uri
            {
                return Err(CatalogError::CertificationConflict);
            }
            tx.commit().await?;
            return Ok(operation);
        }
        transition_locked(
            &mut tx,
            &mut operation,
            JobState::Failed,
            actor,
            Some(error_code),
            error_artifact_uri,
        )
        .await?;
        tx.commit().await?;
        Ok(operation)
    }

    /// Atomically record all verified reference stages and create a certified snapshot.
    ///
    /// Catalog visibility is all-or-nothing: no intermediate state is committed until
    /// every artifact, reasoner result, expected answer, and remote upload verifies.
    pub async fn commit_reference_certification(
        &self,
        tenant_id: Uuid,
        operation_id: Uuid,
        manifest_object_key: &str,
        manifest_sha256: &[u8; 32],
        actor: &str,
    ) -> Result<CertificationOutcome, CatalogError> {
        let mut tx = self.pool.begin().await?;
        set_tenant(&mut tx, tenant_id).await?;
        let request = load_compilation(&mut tx, tenant_id, operation_id)
            .await?
            .request;
        let mut operation = load_operation_for_update(&mut tx, tenant_id, operation_id).await?;
        let serving_root_exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM distributed_serving_root \
             WHERE tenant_id=$1 AND operation_id=$2)",
        )
        .bind(tenant_id)
        .bind(operation_id)
        .fetch_one(&mut *tx)
        .await?;
        if serving_root_exists {
            let certification =
                load_serving_certification(&mut tx, tenant_id, operation_id).await?;
            if certification.reference_manifest_object_key != manifest_object_key
                || certification.reference_manifest_sha256 != hex::encode(manifest_sha256)
            {
                return Err(CatalogError::CertificationConflict);
            }
        }
        if matches!(operation.state, JobState::Certified | JobState::Published) {
            let snapshot = load_snapshot_by_operation(&mut tx, tenant_id, operation_id).await?;
            if snapshot.manifest_object_key != manifest_object_key
                || snapshot.manifest_sha256 != hex::encode(manifest_sha256)
            {
                return Err(CatalogError::CertificationConflict);
            }
            let should_publish = operation.state == JobState::Certified
                && request.publication_policy == PublicationPolicy::AutomaticAfterCertification;
            let dataset_id = operation.dataset_id;
            let target_snapshot_id = request.target_snapshot_id;
            let parent_snapshot_id = request.parent_snapshot_id;
            tx.commit().await?;
            let mut publication_conflict = false;
            if should_publish {
                match self
                    .publish_snapshot(
                        tenant_id,
                        dataset_id,
                        target_snapshot_id,
                        parent_snapshot_id,
                        actor,
                    )
                    .await
                {
                    Ok(_) => {}
                    Err(CatalogError::PublicationConflict) => publication_conflict = true,
                    Err(error) => return Err(error),
                }
            }
            let snapshot = self
                .get_snapshot(tenant_id, dataset_id, target_snapshot_id)
                .await?;
            return Ok(CertificationOutcome {
                published: snapshot.state == "PUBLISHED",
                snapshot,
                publication_conflict,
            });
        }
        if !matches!(
            operation.state,
            JobState::Registered
                | JobState::SourcePlanned
                | JobState::MappingValidated
                | JobState::Partitioned
                | JobState::Projected
                | JobState::Identified
                | JobState::SpineWritten
                | JobState::Indexed
                | JobState::Reasoned
        ) {
            return Err(CatalogError::CertificationConflict);
        }
        for state in [
            JobState::SourcePlanned,
            JobState::MappingValidated,
            JobState::Partitioned,
            JobState::Projected,
            JobState::Identified,
            JobState::SpineWritten,
            JobState::Indexed,
            JobState::Reasoned,
            JobState::Certified,
        ] {
            if may_transition(operation.state, state) {
                transition_locked(&mut tx, &mut operation, state, actor, None, None).await?;
            }
        }
        sqlx::query(
            "INSERT INTO snapshot \
             (tenant_id, dataset_id, snapshot_id, parent_snapshot_id, operation_id, \
              manifest_object_key, manifest_sha256, state) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, 'CERTIFIED')",
        )
        .bind(tenant_id)
        .bind(operation.dataset_id)
        .bind(request.target_snapshot_id)
        .bind(request.parent_snapshot_id)
        .bind(operation_id)
        .bind(manifest_object_key)
        .bind(manifest_sha256.as_slice())
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;

        let mut publication_conflict = false;
        if request.publication_policy == PublicationPolicy::AutomaticAfterCertification {
            match self
                .publish_snapshot(
                    tenant_id,
                    operation.dataset_id,
                    request.target_snapshot_id,
                    request.parent_snapshot_id,
                    actor,
                )
                .await
            {
                Ok(_) => {}
                Err(CatalogError::PublicationConflict) => publication_conflict = true,
                Err(error) => return Err(error),
            }
        }
        let snapshot = self
            .get_snapshot(tenant_id, operation.dataset_id, request.target_snapshot_id)
            .await?;
        Ok(CertificationOutcome {
            published: snapshot.state == "PUBLISHED",
            snapshot,
            publication_conflict,
        })
    }

    /// Atomically bind every cloud compiler/reasoner root and create one certified snapshot.
    ///
    /// The activation row and snapshot are committed in the same PostgreSQL transaction.
    /// Publication remains a compare-and-swap against the expected active parent.
    pub async fn commit_cloud_snapshot_activation(
        &self,
        tenant_id: Uuid,
        operation_id: Uuid,
        request: &CommitCloudSnapshotActivation,
        actor: &str,
    ) -> Result<CertificationOutcome, CatalogError> {
        let activation = &request.activation;
        let digests = [
            &activation.activation_manifest_sha256,
            &activation.semantic_root_sha256,
            &activation.qualification_root_sha256,
            &activation.offline_root_sha256,
            &activation.semantic_content_sha256,
            &activation.authorized_graph_set_sha256,
            &activation.datatype_policy_sha256,
            &activation.ontology_sha256,
            &activation.finite_closure_sha256,
            &activation.proof_support_root_sha256,
            &activation.query_dataset_sha256,
        ]
        .map(|value| decode_catalog_sha256(value))
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;
        if activation.query_dataset_bytes < 0
            || activation.fact_count < 0
            || activation.consequence_count < 0
            || !(1..=65_536).contains(&activation.semantic_partition_count)
            || !(1..=65_536).contains(&activation.reasoning_partition_count)
            || request.reference_manifest_object_key.is_empty()
            || activation.activation_manifest_object_key.is_empty()
        {
            return Err(CatalogError::CertificationConflict);
        }
        let mut tx = self.pool.begin().await?;
        set_tenant(&mut tx, tenant_id).await?;
        let compilation = load_compilation(&mut tx, tenant_id, operation_id).await?;
        let durable = compilation.request;
        let mut operation = load_operation_for_update(&mut tx, tenant_id, operation_id).await?;
        if operation.target_snapshot_id != durable.target_snapshot_id {
            return Err(CatalogError::CertificationConflict);
        }
        let inserted = sqlx::query(
            "INSERT INTO cloud_snapshot_activation \
             (tenant_id,operation_id,dataset_id,snapshot_id,activation_manifest_object_key, \
              activation_manifest_sha256,reference_manifest_object_key,reference_manifest_sha256, \
              semantic_root_object_key,semantic_root_sha256,qualification_root_object_key, \
              qualification_root_sha256,offline_root_object_key,offline_root_sha256, \
              semantic_content_sha256,authorized_graph_set_sha256,datatype_policy_sha256, \
              ontology_sha256,finite_closure_sha256,proof_support_root_sha256,query_dataset_sha256, \
              query_dataset_bytes,fact_count,consequence_count,semantic_partition_count,reasoning_partition_count) \
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26) \
             ON CONFLICT (tenant_id,operation_id) DO NOTHING",
        )
        .bind(tenant_id)
        .bind(operation_id)
        .bind(operation.dataset_id)
        .bind(durable.target_snapshot_id)
        .bind(&activation.activation_manifest_object_key)
        .bind(digests[0].as_slice())
        .bind(&request.reference_manifest_object_key)
        .bind(request.reference_manifest_sha256.as_slice())
        .bind(&activation.semantic_root_object_key)
        .bind(digests[1].as_slice())
        .bind(&activation.qualification_root_object_key)
        .bind(digests[2].as_slice())
        .bind(&activation.offline_root_object_key)
        .bind(digests[3].as_slice())
        .bind(digests[4].as_slice())
        .bind(digests[5].as_slice())
        .bind(digests[6].as_slice())
        .bind(digests[7].as_slice())
        .bind(digests[8].as_slice())
        .bind(digests[9].as_slice())
        .bind(digests[10].as_slice())
        .bind(activation.query_dataset_bytes)
        .bind(activation.fact_count)
        .bind(activation.consequence_count)
        .bind(activation.semantic_partition_count)
        .bind(activation.reasoning_partition_count)
        .execute(&mut *tx)
        .await?
        .rows_affected();
        let observed = load_cloud_activation(&mut tx, tenant_id, operation_id).await?;
        if observed != *activation {
            return Err(CatalogError::CertificationConflict);
        }
        if inserted == 1 {
            if !matches!(
                operation.state,
                JobState::Registered
                    | JobState::SourcePlanned
                    | JobState::MappingValidated
                    | JobState::Partitioned
                    | JobState::Projected
                    | JobState::Identified
                    | JobState::SpineWritten
                    | JobState::Indexed
                    | JobState::Reasoned
            ) {
                return Err(CatalogError::CertificationConflict);
            }
            for state in [
                JobState::SourcePlanned,
                JobState::MappingValidated,
                JobState::Partitioned,
                JobState::Projected,
                JobState::Identified,
                JobState::SpineWritten,
                JobState::Indexed,
                JobState::Reasoned,
                JobState::Certified,
            ] {
                if may_transition(operation.state, state) {
                    transition_locked(&mut tx, &mut operation, state, actor, None, None).await?;
                }
            }
            sqlx::query(
                "INSERT INTO snapshot \
                 (tenant_id,dataset_id,snapshot_id,parent_snapshot_id,operation_id,manifest_object_key,manifest_sha256,state) \
                 VALUES ($1,$2,$3,$4,$5,$6,$7,'CERTIFIED')",
            )
            .bind(tenant_id)
            .bind(operation.dataset_id)
            .bind(durable.target_snapshot_id)
            .bind(durable.parent_snapshot_id)
            .bind(operation_id)
            .bind(&request.reference_manifest_object_key)
            .bind(request.reference_manifest_sha256.as_slice())
            .execute(&mut *tx)
            .await?;
        } else {
            let snapshot = load_snapshot_by_operation(&mut tx, tenant_id, operation_id).await?;
            if snapshot.manifest_object_key != request.reference_manifest_object_key
                || snapshot.manifest_sha256 != hex::encode(request.reference_manifest_sha256)
            {
                return Err(CatalogError::CertificationConflict);
            }
        }
        let dataset_id = operation.dataset_id;
        let snapshot_id = durable.target_snapshot_id;
        let parent_snapshot_id = durable.parent_snapshot_id;
        let automatic = durable.publication_policy == PublicationPolicy::AutomaticAfterCertification;
        tx.commit().await?;
        let mut publication_conflict = false;
        if automatic {
            match self
                .publish_snapshot(tenant_id, dataset_id, snapshot_id, parent_snapshot_id, actor)
                .await
            {
                Ok(_) => {}
                Err(CatalogError::PublicationConflict) => publication_conflict = true,
                Err(error) => return Err(error),
            }
        }
        let snapshot = self.get_snapshot(tenant_id, dataset_id, snapshot_id).await?;
        Ok(CertificationOutcome {
            published: snapshot.state == "PUBLISHED",
            snapshot,
            publication_conflict,
        })
    }

    /// Inspect an immutable snapshot inside the authorized tenant.
    pub async fn get_snapshot(
        &self,
        tenant_id: Uuid,
        dataset_id: Uuid,
        snapshot_id: Uuid,
    ) -> Result<Snapshot, CatalogError> {
        let mut tx = self.pool.begin().await?;
        set_tenant(&mut tx, tenant_id).await?;
        let row = sqlx::query(
            "SELECT tenant_id, dataset_id, snapshot_id, parent_snapshot_id, operation_id, \
                    manifest_object_key, encode(manifest_sha256, 'hex') AS manifest_sha256, state::text AS state \
             FROM snapshot WHERE tenant_id = $1 AND dataset_id = $2 AND snapshot_id = $3",
        )
        .bind(tenant_id)
        .bind(dataset_id)
        .bind(snapshot_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(CatalogError::NotFound)?;
        let snapshot = snapshot_from_row(&row)?;
        tx.commit().await?;
        Ok(snapshot)
    }

    /// Publish a certified snapshot only if its recorded parent is still active.
    pub async fn publish_snapshot(
        &self,
        tenant_id: Uuid,
        dataset_id: Uuid,
        snapshot_id: Uuid,
        expected_parent_snapshot_id: Option<Uuid>,
        actor: &str,
    ) -> Result<Snapshot, CatalogError> {
        let mut tx = self.pool.begin().await?;
        set_tenant(&mut tx, tenant_id).await?;
        let row = sqlx::query(
            "SELECT tenant_id, dataset_id, snapshot_id, parent_snapshot_id, operation_id, \
                    manifest_object_key, encode(manifest_sha256, 'hex') AS manifest_sha256, state::text AS state \
             FROM snapshot WHERE tenant_id = $1 AND dataset_id = $2 AND snapshot_id = $3 FOR UPDATE",
        )
        .bind(tenant_id)
        .bind(dataset_id)
        .bind(snapshot_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(CatalogError::NotFound)?;
        let mut snapshot = snapshot_from_row(&row)?;
        if snapshot.parent_snapshot_id != expected_parent_snapshot_id {
            return Err(CatalogError::PublicationConflict);
        }
        if snapshot.state == "PUBLISHED" {
            tx.commit().await?;
            return Ok(snapshot);
        }
        if snapshot.state != "CERTIFIED" {
            return Err(CatalogError::CertificationConflict);
        }
        let activation_ready: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM cloud_snapshot_activation a \
             WHERE a.tenant_id=$1 AND a.operation_id=$2 \
               AND a.reference_manifest_object_key=$3 \
               AND a.reference_manifest_sha256=decode($4,'hex'))",
        )
        .bind(tenant_id)
        .bind(snapshot.operation_id)
        .bind(&snapshot.manifest_object_key)
        .bind(&snapshot.manifest_sha256)
        .fetch_one(&mut *tx)
        .await?;
        let legacy_ready: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM distributed_serving_certification c \
             WHERE c.tenant_id=$1 AND c.operation_id=$2 \
               AND c.reference_manifest_object_key=$3 \
               AND c.reference_manifest_sha256=decode($4,'hex'))",
        )
        .bind(tenant_id)
        .bind(snapshot.operation_id)
        .bind(&snapshot.manifest_object_key)
        .bind(&snapshot.manifest_sha256)
        .fetch_one(&mut *tx)
        .await?;
        if !activation_ready && !legacy_ready {
            return Err(CatalogError::CertificationConflict);
        }
        let changed = sqlx::query(
            "UPDATE dataset SET active_snapshot_id = $1 \
             WHERE tenant_id = $2 AND dataset_id = $3 \
               AND active_snapshot_id IS NOT DISTINCT FROM $4",
        )
        .bind(snapshot_id)
        .bind(tenant_id)
        .bind(dataset_id)
        .bind(expected_parent_snapshot_id)
        .execute(&mut *tx)
        .await?
        .rows_affected();
        if changed != 1 {
            return Err(CatalogError::PublicationConflict);
        }
        if let Some(parent_snapshot_id) = expected_parent_snapshot_id {
            let retired = sqlx::query(
                "UPDATE snapshot SET state = 'RETIRED' \
                 WHERE tenant_id = $1 AND dataset_id = $2 AND snapshot_id = $3 \
                   AND state = 'PUBLISHED'",
            )
            .bind(tenant_id)
            .bind(dataset_id)
            .bind(parent_snapshot_id)
            .execute(&mut *tx)
            .await?
            .rows_affected();
            if retired != 1 {
                return Err(CatalogError::PublicationConflict);
            }
        }
        let mut operation =
            load_operation_for_update(&mut tx, tenant_id, snapshot.operation_id).await?;
        transition_locked(
            &mut tx,
            &mut operation,
            JobState::Published,
            actor,
            None,
            None,
        )
        .await?;
        sqlx::query(
            "UPDATE snapshot SET state = 'PUBLISHED', published_at = now() \
             WHERE tenant_id = $1 AND dataset_id = $2 AND snapshot_id = $3 AND state = 'CERTIFIED'",
        )
        .bind(tenant_id)
        .bind(dataset_id)
        .bind(snapshot_id)
        .execute(&mut *tx)
        .await?;
        snapshot.state = "PUBLISHED".to_owned();
        tx.commit().await?;
        Ok(snapshot)
    }

    /// Insert the RUNNING record before distributed execution is admitted.
    pub async fn begin_query_execution_log(
        &self,
        tenant_id: Uuid,
        begin: &BeginQueryExecutionLog,
    ) -> Result<(), CatalogError> {
        let mut tx = self.pool.begin().await?;
        set_tenant(&mut tx, tenant_id).await?;
        sqlx::query(
            "INSERT INTO query_execution_log \
             (tenant_id, query_execution_id, dataset_id, principal_id, request_id, query_sha256, query_text, start_time_epoch_ms) \
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
        )
        .bind(tenant_id)
        .bind(begin.query_execution_id)
        .bind(begin.dataset_id)
        .bind(&begin.principal_id)
        .bind(&begin.request_id)
        .bind(begin.query_sha256.as_slice())
        .bind(&begin.query_text)
        .bind(begin.start_time_epoch_ms)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }

    /// Finalize a RUNNING record exactly once.
    pub async fn finalize_query_execution_log(
        &self,
        tenant_id: Uuid,
        query_execution_id: Uuid,
        finish: &FinalizeQueryExecutionLog,
    ) -> Result<(), CatalogError> {
        let mut tx = self.pool.begin().await?;
        set_tenant(&mut tx, tenant_id).await?;
        let changed = sqlx::query(
            "UPDATE query_execution_log SET snapshot_id=$3, query_form=$4, execution_mode=$5, \
             status=$6::ngkg_query_execution_status, participating_nodes=$7, allocated_cpu_millis=$8, \
             allocated_memory_bytes=$9, result_rows=$10, result_bytes=$11, cache_hit=$12, \
             end_time_epoch_ms=$13, total_duration_ms=$14, error_code=$15, finalized_at=now() \
             WHERE tenant_id=$1 AND query_execution_id=$2 AND status='RUNNING'",
        )
        .bind(tenant_id)
        .bind(query_execution_id)
        .bind(finish.snapshot_id)
        .bind(&finish.query_form)
        .bind(&finish.execution_mode)
        .bind(&finish.status)
        .bind(finish.participating_nodes)
        .bind(finish.allocated_cpu_millis)
        .bind(finish.allocated_memory_bytes)
        .bind(finish.result_rows)
        .bind(finish.result_bytes)
        .bind(finish.cache_hit)
        .bind(finish.end_time_epoch_ms)
        .bind(finish.total_duration_ms)
        .bind(&finish.error_code)
        .execute(&mut *tx)
        .await?
        .rows_affected();
        if changed != 1 {
            return Err(CatalogError::NotFound);
        }
        tx.commit().await?;
        Ok(())
    }

    /// Read one exact record inside the active tenant RLS boundary.
    pub async fn get_query_execution_log(
        &self,
        tenant_id: Uuid,
        query_execution_id: Uuid,
    ) -> Result<QueryExecutionLogRecord, CatalogError> {
        let mut tx = self.pool.begin().await?;
        set_tenant(&mut tx, tenant_id).await?;
        let row = sqlx::query(QUERY_EXECUTION_LOG_SELECT)
            .bind(tenant_id)
            .bind(query_execution_id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(CatalogError::NotFound)?;
        let record = query_execution_log_from_row(&row)?;
        tx.commit().await?;
        Ok(record)
    }

    /// List a bounded, deterministic page inside the active tenant RLS boundary.
    pub async fn list_query_execution_logs(
        &self,
        tenant_id: Uuid,
        filter: &QueryExecutionLogFilter,
    ) -> Result<Vec<QueryExecutionLogRecord>, CatalogError> {
        let mut tx = self.pool.begin().await?;
        set_tenant(&mut tx, tenant_id).await?;
        let rows = sqlx::query(
            "SELECT tenant_id, query_execution_id, dataset_id, snapshot_id, principal_id, request_id, \
                    encode(query_sha256,'hex') AS query_sha256, query_text, query_form, execution_mode, \
                    status::text AS status, participating_nodes, allocated_cpu_millis, allocated_memory_bytes, \
                    result_rows, result_bytes, cache_hit, start_time_epoch_ms, end_time_epoch_ms, \
                    total_duration_ms, error_code \
             FROM query_execution_log WHERE tenant_id=$1 \
               AND ($2::uuid IS NULL OR dataset_id=$2) \
               AND ($3::text IS NULL OR principal_id=$3) \
               AND ($4::text IS NULL OR status::text=$4) \
               AND ($5::bigint IS NULL OR start_time_epoch_ms >= $5) \
               AND ($6::bigint IS NULL OR start_time_epoch_ms <= $6) \
               AND ($7::bigint IS NULL OR total_duration_ms >= $7) \
             ORDER BY start_time_epoch_ms DESC, query_execution_id DESC LIMIT $8 OFFSET $9",
        )
        .bind(tenant_id)
        .bind(filter.dataset_id)
        .bind(&filter.principal_id)
        .bind(&filter.status)
        .bind(filter.started_after_epoch_ms)
        .bind(filter.started_before_epoch_ms)
        .bind(filter.min_duration_ms)
        .bind(filter.limit)
        .bind(filter.offset)
        .fetch_all(&mut *tx)
        .await?;
        let records = rows
            .iter()
            .map(query_execution_log_from_row)
            .collect::<Result<Vec<_>, _>>()?;
        tx.commit().await?;
        Ok(records)
    }
}

const QUERY_EXECUTION_LOG_SELECT: &str =
    "SELECT tenant_id, query_execution_id, dataset_id, snapshot_id, principal_id, request_id, \
            encode(query_sha256,'hex') AS query_sha256, query_text, query_form, execution_mode, \
            status::text AS status, participating_nodes, allocated_cpu_millis, allocated_memory_bytes, \
            result_rows, result_bytes, cache_hit, start_time_epoch_ms, end_time_epoch_ms, \
            total_duration_ms, error_code \
     FROM query_execution_log WHERE tenant_id=$1 AND query_execution_id=$2";

fn query_execution_log_from_row(
    row: &sqlx::postgres::PgRow,
) -> Result<QueryExecutionLogRecord, CatalogError> {
    Ok(QueryExecutionLogRecord {
        tenant_id: row.try_get("tenant_id")?,
        query_execution_id: row.try_get("query_execution_id")?,
        dataset_id: row.try_get("dataset_id")?,
        snapshot_id: row.try_get("snapshot_id")?,
        principal_id: row.try_get("principal_id")?,
        request_id: row.try_get("request_id")?,
        query_sha256: row.try_get("query_sha256")?,
        query_text: row.try_get("query_text")?,
        query_form: row.try_get("query_form")?,
        execution_mode: row.try_get("execution_mode")?,
        status: row.try_get("status")?,
        participating_nodes: row.try_get("participating_nodes")?,
        allocated_cpu_millis: row.try_get("allocated_cpu_millis")?,
        allocated_memory_bytes: row.try_get("allocated_memory_bytes")?,
        result_rows: row.try_get("result_rows")?,
        result_bytes: row.try_get("result_bytes")?,
        cache_hit: row.try_get("cache_hit")?,
        start_time_epoch_ms: row.try_get("start_time_epoch_ms")?,
        end_time_epoch_ms: row.try_get("end_time_epoch_ms")?,
        total_duration_ms: row.try_get("total_duration_ms")?,
        error_code: row.try_get("error_code")?,
    })
}

async fn set_tenant(
    tx: &mut Transaction<'_, Postgres>,
    tenant_id: Uuid,
) -> Result<(), CatalogError> {
    sqlx::query("SELECT set_config('ngkg.tenant_id', $1, true)")
        .bind(tenant_id.to_string())
        .execute(&mut **tx)
        .await?;
    Ok(())
}

fn validate_distributed_registration(plan: &RegisterDistributedPlan) -> Result<(), CatalogError> {
    if plan.logical_partition_count <= 0
        || plan.logical_partition_count > 65_536
        || plan.reducer_count <= 0
        || plan.reducer_count > plan.logical_partition_count
        || plan.fact_count < 0
        || usize::try_from(plan.logical_partition_count).ok() != Some(plan.projections.len())
        || usize::try_from(plan.reducer_count).ok() != Some(plan.reducers.len())
        || plan.layout_profile.is_empty()
    {
        return Err(CatalogError::CertificationConflict);
    }
    for work in [&plan.projections, &plan.reducers] {
        for (expected, item) in work.iter().enumerate() {
            if usize::try_from(item.work_index).ok() != Some(expected)
                || item.stable_work_id.len() != 71
                || !item.stable_work_id.starts_with("blake3:")
                || !item.stable_work_id[7..]
                    .bytes()
                    .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
                || item.input_object_key.is_empty()
            {
                return Err(CatalogError::CertificationConflict);
            }
        }
    }
    Ok(())
}

async fn verify_distributed_registration(
    tx: &mut Transaction<'_, Postgres>,
    tenant_id: Uuid,
    operation_id: Uuid,
    expected: &RegisterDistributedPlan,
) -> Result<(), CatalogError> {
    let summary = load_distributed_summary(tx, tenant_id, operation_id).await?;
    if summary.source_plan_object_key != expected.source_plan_object_key
        || summary.source_plan_sha256 != hex::encode(expected.source_plan_sha256)
        || summary.logical_partition_count != expected.logical_partition_count
        || summary.reducer_count != expected.reducer_count
        || summary.fact_count != expected.fact_count
        || summary.layout_profile != expected.layout_profile
    {
        return Err(CatalogError::CertificationConflict);
    }
    for (kind, work) in [
        (DistributedWorkKind::Projection, &expected.projections),
        (DistributedWorkKind::Reducer, &expected.reducers),
    ] {
        let rows = sqlx::query(
            "SELECT work_index, stable_work_id, input_object_key, \
                    encode(input_sha256, 'hex') AS input_sha256, state::text AS state, \
                    output_manifest_object_key, \
                    CASE WHEN output_manifest_sha256 IS NULL THEN NULL \
                         ELSE encode(output_manifest_sha256, 'hex') END AS output_manifest_sha256 \
             FROM distributed_work WHERE tenant_id = $1 AND operation_id = $2 \
               AND work_kind = $3::ngkg_work_kind ORDER BY work_index",
        )
        .bind(tenant_id)
        .bind(operation_id)
        .bind(kind.as_db())
        .fetch_all(&mut **tx)
        .await?;
        if rows.len() != work.len() {
            return Err(CatalogError::CertificationConflict);
        }
        for (row, expected_item) in rows.iter().zip(work.iter()) {
            let item = distributed_work_from_row(kind, row)?;
            if item.work_index != expected_item.work_index
                || item.stable_work_id != expected_item.stable_work_id
                || item.input_object_key != expected_item.input_object_key
                || item.input_sha256 != hex::encode(expected_item.input_sha256)
            {
                return Err(CatalogError::CertificationConflict);
            }
        }
    }
    Ok(())
}

async fn load_distributed_summary(
    tx: &mut Transaction<'_, Postgres>,
    tenant_id: Uuid,
    operation_id: Uuid,
) -> Result<DistributedPlanSummary, CatalogError> {
    let row = sqlx::query(
        "SELECT p.source_plan_object_key, encode(p.source_plan_sha256, 'hex') AS source_plan_sha256, \
                p.logical_partition_count, p.reducer_count, p.fact_count, p.layout_profile, \
                count(*) FILTER (WHERE w.work_kind = 'PROJECTION' AND w.state = 'SUCCEEDED') AS succeeded_projections, \
                count(*) FILTER (WHERE w.work_kind = 'REDUCER' AND w.state = 'SUCCEEDED') AS succeeded_reducers, \
                count(*) FILTER (WHERE w.state = 'FAILED') AS failed_work \
         FROM distributed_plan p LEFT JOIN distributed_work w \
           ON w.tenant_id = p.tenant_id AND w.operation_id = p.operation_id \
         WHERE p.tenant_id = $1 AND p.operation_id = $2 \
         GROUP BY p.tenant_id, p.operation_id, p.source_plan_object_key, p.source_plan_sha256, \
                  p.logical_partition_count, p.reducer_count, p.fact_count, p.layout_profile",
    )
    .bind(tenant_id)
    .bind(operation_id)
    .fetch_optional(&mut **tx)
    .await?
    .ok_or(CatalogError::NotFound)?;
    Ok(DistributedPlanSummary {
        source_plan_object_key: row.try_get("source_plan_object_key")?,
        source_plan_sha256: row.try_get("source_plan_sha256")?,
        logical_partition_count: row.try_get("logical_partition_count")?,
        reducer_count: row.try_get("reducer_count")?,
        fact_count: row.try_get("fact_count")?,
        layout_profile: row.try_get("layout_profile")?,
        succeeded_projections: row.try_get("succeeded_projections")?,
        succeeded_reducers: row.try_get("succeeded_reducers")?,
        failed_work: row.try_get("failed_work")?,
    })
}

async fn load_distributed_work(
    tx: &mut Transaction<'_, Postgres>,
    tenant_id: Uuid,
    operation_id: Uuid,
    work_kind: DistributedWorkKind,
    work_index: i32,
) -> Result<DistributedWorkItem, CatalogError> {
    let row = sqlx::query(
        "SELECT work_index, stable_work_id, input_object_key, \
                encode(input_sha256, 'hex') AS input_sha256, state::text AS state, \
                output_manifest_object_key, \
                CASE WHEN output_manifest_sha256 IS NULL THEN NULL \
                     ELSE encode(output_manifest_sha256, 'hex') END AS output_manifest_sha256 \
         FROM distributed_work WHERE tenant_id = $1 AND operation_id = $2 \
           AND work_kind = $3::ngkg_work_kind AND work_index = $4",
    )
    .bind(tenant_id)
    .bind(operation_id)
    .bind(work_kind.as_db())
    .bind(work_index)
    .fetch_optional(&mut **tx)
    .await?
    .ok_or(CatalogError::NotFound)?;
    distributed_work_from_row(work_kind, &row)
}

async fn load_distributed_work_for_update(
    tx: &mut Transaction<'_, Postgres>,
    tenant_id: Uuid,
    operation_id: Uuid,
    work_kind: DistributedWorkKind,
    work_index: i32,
) -> Result<DistributedWorkItem, CatalogError> {
    let row = sqlx::query(
        "SELECT work_index, stable_work_id, input_object_key, \
                encode(input_sha256, 'hex') AS input_sha256, state::text AS state, \
                output_manifest_object_key, \
                CASE WHEN output_manifest_sha256 IS NULL THEN NULL \
                     ELSE encode(output_manifest_sha256, 'hex') END AS output_manifest_sha256 \
         FROM distributed_work WHERE tenant_id = $1 AND operation_id = $2 \
           AND work_kind = $3::ngkg_work_kind AND work_index = $4 FOR UPDATE",
    )
    .bind(tenant_id)
    .bind(operation_id)
    .bind(work_kind.as_db())
    .bind(work_index)
    .fetch_optional(&mut **tx)
    .await?
    .ok_or(CatalogError::NotFound)?;
    distributed_work_from_row(work_kind, &row)
}

fn distributed_work_from_row(
    work_kind: DistributedWorkKind,
    row: &sqlx::postgres::PgRow,
) -> Result<DistributedWorkItem, CatalogError> {
    Ok(DistributedWorkItem {
        work_kind,
        work_index: row.try_get("work_index")?,
        stable_work_id: row.try_get("stable_work_id")?,
        input_object_key: row.try_get("input_object_key")?,
        input_sha256: row.try_get("input_sha256")?,
        state: row.try_get("state")?,
        output_manifest_object_key: row.try_get("output_manifest_object_key")?,
        output_manifest_sha256: row.try_get("output_manifest_sha256")?,
    })
}

async fn load_distributed_root(
    tx: &mut Transaction<'_, Postgres>,
    tenant_id: Uuid,
    operation_id: Uuid,
) -> Result<DistributedRoot, CatalogError> {
    let row = sqlx::query(
        "SELECT root_manifest_object_key, encode(root_manifest_sha256, 'hex') AS root_manifest_sha256, \
                canonical_source_object_key, encode(canonical_source_sha256, 'hex') AS canonical_source_sha256, \
                dictionary_object_key, encode(dictionary_sha256, 'hex') AS dictionary_sha256, \
                encode(semantic_content_sha256, 'hex') AS semantic_content_sha256 \
         FROM distributed_root WHERE tenant_id = $1 AND operation_id = $2",
    )
    .bind(tenant_id)
    .bind(operation_id)
    .fetch_optional(&mut **tx)
    .await?
    .ok_or(CatalogError::NotFound)?;
    Ok(DistributedRoot {
        root_manifest_object_key: row.try_get("root_manifest_object_key")?,
        root_manifest_sha256: row.try_get("root_manifest_sha256")?,
        canonical_source_object_key: row.try_get("canonical_source_object_key")?,
        canonical_source_sha256: row.try_get("canonical_source_sha256")?,
        dictionary_object_key: row.try_get("dictionary_object_key")?,
        dictionary_sha256: row.try_get("dictionary_sha256")?,
        semantic_content_sha256: row.try_get("semantic_content_sha256")?,
    })
}

async fn load_artifact_plan_summary(
    tx: &mut Transaction<'_, Postgres>,
    tenant_id: Uuid,
    operation_id: Uuid,
) -> Result<ArtifactPlanSummary, CatalogError> {
    let row = sqlx::query(
        "SELECT p.source_plan_object_key, encode(p.source_plan_sha256,'hex') AS source_plan_sha256, \
                p.dictionary_object_key, encode(p.dictionary_sha256,'hex') AS dictionary_sha256, \
                p.artifact_plan_object_key, encode(p.artifact_plan_sha256,'hex') AS artifact_plan_sha256, \
                p.partition_count, p.row_group_rows, \
                count(*) FILTER (WHERE w.state='SUCCEEDED') AS succeeded_artifacts, \
                count(*) FILTER (WHERE w.state='FAILED') AS failed_artifacts \
         FROM distributed_artifact_plan p LEFT JOIN distributed_work w \
           ON w.tenant_id=p.tenant_id AND w.operation_id=p.operation_id \
          AND w.work_kind='ARTIFACT'::ngkg_work_kind \
         WHERE p.tenant_id=$1 AND p.operation_id=$2 \
         GROUP BY p.tenant_id,p.operation_id,p.source_plan_object_key,p.source_plan_sha256, \
                  p.dictionary_object_key,p.dictionary_sha256,p.artifact_plan_object_key, \
                  p.artifact_plan_sha256,p.partition_count,p.row_group_rows",
    )
    .bind(tenant_id)
    .bind(operation_id)
    .fetch_optional(&mut **tx)
    .await?
    .ok_or(CatalogError::NotFound)?;
    Ok(ArtifactPlanSummary {
        source_plan_object_key: row.try_get("source_plan_object_key")?,
        source_plan_sha256: row.try_get("source_plan_sha256")?,
        dictionary_object_key: row.try_get("dictionary_object_key")?,
        dictionary_sha256: row.try_get("dictionary_sha256")?,
        artifact_plan_object_key: row.try_get("artifact_plan_object_key")?,
        artifact_plan_sha256: row.try_get("artifact_plan_sha256")?,
        partition_count: row.try_get("partition_count")?,
        row_group_rows: row.try_get("row_group_rows")?,
        succeeded_artifacts: row.try_get("succeeded_artifacts")?,
        failed_artifacts: row.try_get("failed_artifacts")?,
    })
}

async fn load_artifact_root(
    tx: &mut Transaction<'_, Postgres>,
    tenant_id: Uuid,
    operation_id: Uuid,
) -> Result<DistributedArtifactRoot, CatalogError> {
    let row = sqlx::query(
        "SELECT root_manifest_object_key, encode(root_manifest_sha256,'hex') AS root_manifest_sha256, \
                locator_object_key, encode(locator_sha256,'hex') AS locator_sha256, \
                encode(semantic_content_sha256,'hex') AS semantic_content_sha256, \
                fact_count,semantic_row_count,payload_row_count,locator_record_count \
         FROM distributed_artifact_root WHERE tenant_id=$1 AND operation_id=$2",
    )
    .bind(tenant_id)
    .bind(operation_id)
    .fetch_optional(&mut **tx)
    .await?
    .ok_or(CatalogError::NotFound)?;
    Ok(DistributedArtifactRoot {
        root_manifest_object_key: row.try_get("root_manifest_object_key")?,
        root_manifest_sha256: row.try_get("root_manifest_sha256")?,
        locator_object_key: row.try_get("locator_object_key")?,
        locator_sha256: row.try_get("locator_sha256")?,
        semantic_content_sha256: row.try_get("semantic_content_sha256")?,
        fact_count: row.try_get("fact_count")?,
        semantic_row_count: row.try_get("semantic_row_count")?,
        payload_row_count: row.try_get("payload_row_count")?,
        locator_record_count: row.try_get("locator_record_count")?,
    })
}

async fn load_serving_root(
    tx: &mut Transaction<'_, Postgres>,
    tenant_id: Uuid,
    operation_id: Uuid,
) -> Result<DistributedServingRoot, CatalogError> {
    let row = sqlx::query(
        "SELECT serving_root_object_key, \
                encode(serving_root_sha256,'hex') AS serving_root_sha256, \
                binary_locator_object_key, \
                encode(binary_locator_sha256,'hex') AS binary_locator_sha256, \
                encode(source_locator_sha256,'hex') AS source_locator_sha256, \
                encode(semantic_content_sha256,'hex') AS semantic_content_sha256, \
                partition_count,row_group_rows,locator_record_count \
         FROM distributed_serving_root WHERE tenant_id=$1 AND operation_id=$2",
    )
    .bind(tenant_id)
    .bind(operation_id)
    .fetch_optional(&mut **tx)
    .await?
    .ok_or(CatalogError::NotFound)?;
    Ok(DistributedServingRoot {
        serving_root_object_key: row.try_get("serving_root_object_key")?,
        serving_root_sha256: row.try_get("serving_root_sha256")?,
        binary_locator_object_key: row.try_get("binary_locator_object_key")?,
        binary_locator_sha256: row.try_get("binary_locator_sha256")?,
        source_locator_sha256: row.try_get("source_locator_sha256")?,
        semantic_content_sha256: row.try_get("semantic_content_sha256")?,
        partition_count: row.try_get("partition_count")?,
        row_group_rows: row.try_get("row_group_rows")?,
        locator_record_count: row.try_get("locator_record_count")?,
    })
}

async fn load_serving_certification(
    tx: &mut Transaction<'_, Postgres>,
    tenant_id: Uuid,
    operation_id: Uuid,
) -> Result<ServingCertification, CatalogError> {
    let row = sqlx::query(
        "SELECT report_object_key,encode(report_sha256,'hex') AS report_sha256, \
                encode(serving_root_sha256,'hex') AS serving_root_sha256, \
                encode(binary_locator_sha256,'hex') AS binary_locator_sha256, \
                reference_manifest_object_key, \
                encode(reference_manifest_sha256,'hex') AS reference_manifest_sha256, \
                certified_query_count,hydrated_row_count \
         FROM distributed_serving_certification WHERE tenant_id=$1 AND operation_id=$2",
    )
    .bind(tenant_id)
    .bind(operation_id)
    .fetch_optional(&mut **tx)
    .await?
    .ok_or(CatalogError::NotFound)?;
    Ok(ServingCertification {
        report_object_key: row.try_get("report_object_key")?,
        report_sha256: row.try_get("report_sha256")?,
        serving_root_sha256: row.try_get("serving_root_sha256")?,
        binary_locator_sha256: row.try_get("binary_locator_sha256")?,
        reference_manifest_object_key: row.try_get("reference_manifest_object_key")?,
        reference_manifest_sha256: row.try_get("reference_manifest_sha256")?,
        certified_query_count: row.try_get("certified_query_count")?,
        hydrated_row_count: row.try_get("hydrated_row_count")?,
    })
}

async fn load_cloud_activation(
    tx: &mut Transaction<'_, Postgres>,
    tenant_id: Uuid,
    operation_id: Uuid,
) -> Result<CloudSnapshotActivation, CatalogError> {
    let row = sqlx::query(
        "SELECT activation_manifest_object_key, \
                encode(activation_manifest_sha256,'hex') AS activation_manifest_sha256, \
                semantic_root_object_key,encode(semantic_root_sha256,'hex') AS semantic_root_sha256, \
                qualification_root_object_key,encode(qualification_root_sha256,'hex') AS qualification_root_sha256, \
                offline_root_object_key,encode(offline_root_sha256,'hex') AS offline_root_sha256, \
                encode(semantic_content_sha256,'hex') AS semantic_content_sha256, \
                encode(authorized_graph_set_sha256,'hex') AS authorized_graph_set_sha256, \
                encode(datatype_policy_sha256,'hex') AS datatype_policy_sha256, \
                encode(ontology_sha256,'hex') AS ontology_sha256, \
                encode(finite_closure_sha256,'hex') AS finite_closure_sha256, \
                encode(proof_support_root_sha256,'hex') AS proof_support_root_sha256, \
                encode(query_dataset_sha256,'hex') AS query_dataset_sha256, \
                query_dataset_bytes,fact_count,consequence_count,semantic_partition_count,reasoning_partition_count \
         FROM cloud_snapshot_activation WHERE tenant_id=$1 AND operation_id=$2",
    )
    .bind(tenant_id)
    .bind(operation_id)
    .fetch_optional(&mut **tx)
    .await?
    .ok_or(CatalogError::NotFound)?;
    Ok(CloudSnapshotActivation {
        activation_manifest_object_key: row.try_get("activation_manifest_object_key")?,
        activation_manifest_sha256: row.try_get("activation_manifest_sha256")?,
        semantic_root_object_key: row.try_get("semantic_root_object_key")?,
        semantic_root_sha256: row.try_get("semantic_root_sha256")?,
        qualification_root_object_key: row.try_get("qualification_root_object_key")?,
        qualification_root_sha256: row.try_get("qualification_root_sha256")?,
        offline_root_object_key: row.try_get("offline_root_object_key")?,
        offline_root_sha256: row.try_get("offline_root_sha256")?,
        semantic_content_sha256: row.try_get("semantic_content_sha256")?,
        authorized_graph_set_sha256: row.try_get("authorized_graph_set_sha256")?,
        datatype_policy_sha256: row.try_get("datatype_policy_sha256")?,
        ontology_sha256: row.try_get("ontology_sha256")?,
        finite_closure_sha256: row.try_get("finite_closure_sha256")?,
        proof_support_root_sha256: row.try_get("proof_support_root_sha256")?,
        query_dataset_sha256: row.try_get("query_dataset_sha256")?,
        query_dataset_bytes: row.try_get("query_dataset_bytes")?,
        fact_count: row.try_get("fact_count")?,
        consequence_count: row.try_get("consequence_count")?,
        semantic_partition_count: row.try_get("semantic_partition_count")?,
        reasoning_partition_count: row.try_get("reasoning_partition_count")?,
    })
}

fn decode_catalog_sha256(value: &str) -> Result<[u8; 32], CatalogError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(CatalogError::CertificationConflict);
    }
    let bytes = hex::decode(value).map_err(|_| CatalogError::CertificationConflict)?;
    bytes
        .try_into()
        .map_err(|_| CatalogError::CertificationConflict)
}

async fn load_compilation_by_idempotency(
    tx: &mut Transaction<'_, Postgres>,
    tenant_id: Uuid,
    idempotency_key: &str,
) -> Result<(CompilationOperation, Vec<u8>), CatalogError> {
    let row = sqlx::query(
        "SELECT o.operation_id, o.dataset_id, o.request_hash, o.state::text AS state, o.revision, \
                o.target_snapshot_id, o.error_code, o.error_artifact_uri, c.bundle_object_key, \
                c.bundle_sha256, c.parent_snapshot_id, c.publication_policy::text AS publication_policy, \
                c.resource_profile, d.identity_namespace, d.policy_version \
         FROM operation o JOIN compilation_request c \
           ON c.tenant_id = o.tenant_id AND c.operation_id = o.operation_id \
         JOIN dataset d ON d.tenant_id = o.tenant_id AND d.dataset_id = o.dataset_id \
         WHERE o.tenant_id = $1 AND o.idempotency_key = $2 FOR SHARE",
    )
    .bind(tenant_id)
    .bind(idempotency_key)
    .fetch_optional(&mut **tx)
    .await?
    .ok_or(CatalogError::NotFound)?;
    let request_hash: Vec<u8> = row.try_get("request_hash")?;
    Ok((compilation_from_row(tenant_id, &row)?, request_hash))
}

async fn load_compilation(
    tx: &mut Transaction<'_, Postgres>,
    tenant_id: Uuid,
    operation_id: Uuid,
) -> Result<CompilationOperation, CatalogError> {
    let row = sqlx::query(
        "SELECT o.operation_id, o.dataset_id, o.state::text AS state, o.revision, o.target_snapshot_id, \
                o.error_code, o.error_artifact_uri, c.bundle_object_key, c.bundle_sha256, \
                c.parent_snapshot_id, c.publication_policy::text AS publication_policy, c.resource_profile, \
                d.identity_namespace, d.policy_version \
         FROM operation o JOIN compilation_request c \
           ON c.tenant_id = o.tenant_id AND c.operation_id = o.operation_id \
         JOIN dataset d ON d.tenant_id = o.tenant_id AND d.dataset_id = o.dataset_id \
         WHERE o.tenant_id = $1 AND o.operation_id = $2",
    )
    .bind(tenant_id)
    .bind(operation_id)
    .fetch_optional(&mut **tx)
    .await?
    .ok_or(CatalogError::NotFound)?;
    compilation_from_row(tenant_id, &row)
}

fn compilation_from_row(
    tenant_id: Uuid,
    row: &sqlx::postgres::PgRow,
) -> Result<CompilationOperation, CatalogError> {
    let operation = operation_from_row(tenant_id, row)?;
    let sha: Vec<u8> = row.try_get("bundle_sha256")?;
    let bundle_sha256 = array_32(&sha)?;
    let policy: String = row.try_get("publication_policy")?;
    let publication_policy = match policy.as_str() {
        "MANUAL_AFTER_CERTIFICATION" => PublicationPolicy::ManualAfterCertification,
        "AUTOMATIC_AFTER_CERTIFICATION" => PublicationPolicy::AutomaticAfterCertification,
        _ => {
            return Err(CatalogError::Database(sqlx::Error::Decode(
                "unknown publication policy".into(),
            )));
        }
    };
    Ok(CompilationOperation {
        request: CreateCompilation {
            bundle_object_key: row.try_get("bundle_object_key")?,
            bundle_sha256,
            parent_snapshot_id: row.try_get("parent_snapshot_id")?,
            target_snapshot_id: operation.target_snapshot_id,
            publication_policy,
            resource_profile: row.try_get("resource_profile")?,
        },
        identity_namespace: row.try_get("identity_namespace")?,
        policy_version: row.try_get("policy_version")?,
        operation,
    })
}

async fn load_operation_for_update(
    tx: &mut Transaction<'_, Postgres>,
    tenant_id: Uuid,
    operation_id: Uuid,
) -> Result<Operation, CatalogError> {
    let row = sqlx::query(
        "SELECT operation_id, dataset_id, state::text AS state, revision, target_snapshot_id, \
                error_code, error_artifact_uri \
         FROM operation WHERE tenant_id = $1 AND operation_id = $2 FOR UPDATE",
    )
    .bind(tenant_id)
    .bind(operation_id)
    .fetch_optional(&mut **tx)
    .await?
    .ok_or(CatalogError::NotFound)?;
    operation_from_row(tenant_id, &row)
}

fn operation_from_row(
    tenant_id: Uuid,
    row: &sqlx::postgres::PgRow,
) -> Result<Operation, CatalogError> {
    let state_text: String = row.try_get("state")?;
    Ok(Operation {
        tenant_id,
        operation_id: row.try_get("operation_id")?,
        dataset_id: row.try_get("dataset_id")?,
        state: parse_state(&state_text)?,
        revision: row.try_get("revision")?,
        target_snapshot_id: row.try_get("target_snapshot_id")?,
        error_code: row.try_get("error_code")?,
        error_artifact_uri: row.try_get("error_artifact_uri")?,
    })
}

async fn transition_locked(
    tx: &mut Transaction<'_, Postgres>,
    operation: &mut Operation,
    next: JobState,
    actor: &str,
    error_code: Option<&str>,
    error_artifact_uri: Option<&str>,
) -> Result<(), CatalogError> {
    if !may_transition(operation.state, next) {
        return Err(CatalogError::IllegalTransition {
            from: operation.state,
            to: next,
        });
    }
    let previous = operation.state;
    let next_revision = operation.revision + 1;
    let changed = sqlx::query(
        "UPDATE operation SET state = $1::ngkg_job_state, revision = $2, error_code = $3, \
                error_artifact_uri = $4, updated_at = now() \
         WHERE tenant_id = $5 AND operation_id = $6 AND revision = $7 AND state = $8::ngkg_job_state",
    )
    .bind(next.as_db())
    .bind(next_revision)
    .bind(error_code)
    .bind(error_artifact_uri)
    .bind(operation.tenant_id)
    .bind(operation.operation_id)
    .bind(operation.revision)
    .bind(previous.as_db())
    .execute(&mut **tx)
    .await?
    .rows_affected();
    if changed != 1 {
        return Err(CatalogError::RevisionConflict {
            expected: operation.revision,
        });
    }
    sqlx::query(
        "INSERT INTO operation_audit \
         (tenant_id, operation_id, revision, previous_state, new_state, actor) \
         VALUES ($1, $2, $3, $4::ngkg_job_state, $5::ngkg_job_state, $6)",
    )
    .bind(operation.tenant_id)
    .bind(operation.operation_id)
    .bind(next_revision)
    .bind(previous.as_db())
    .bind(next.as_db())
    .bind(actor)
    .execute(&mut **tx)
    .await?;
    operation.state = next;
    operation.revision = next_revision;
    operation.error_code = error_code.map(ToOwned::to_owned);
    operation.error_artifact_uri = error_artifact_uri.map(ToOwned::to_owned);
    Ok(())
}

async fn load_snapshot_by_operation(
    tx: &mut Transaction<'_, Postgres>,
    tenant_id: Uuid,
    operation_id: Uuid,
) -> Result<Snapshot, CatalogError> {
    let row = sqlx::query(
        "SELECT tenant_id, dataset_id, snapshot_id, parent_snapshot_id, operation_id, \
                manifest_object_key, encode(manifest_sha256, 'hex') AS manifest_sha256, state::text AS state \
         FROM snapshot WHERE tenant_id = $1 AND operation_id = $2",
    )
    .bind(tenant_id)
    .bind(operation_id)
    .fetch_optional(&mut **tx)
    .await?
    .ok_or(CatalogError::NotFound)?;
    snapshot_from_row(&row)
}

fn snapshot_from_row(row: &sqlx::postgres::PgRow) -> Result<Snapshot, CatalogError> {
    Ok(Snapshot {
        tenant_id: row.try_get("tenant_id")?,
        dataset_id: row.try_get("dataset_id")?,
        snapshot_id: row.try_get("snapshot_id")?,
        parent_snapshot_id: row.try_get("parent_snapshot_id")?,
        operation_id: row.try_get("operation_id")?,
        manifest_object_key: row.try_get("manifest_object_key")?,
        manifest_sha256: row.try_get("manifest_sha256")?,
        state: row.try_get("state")?,
    })
}

fn parse_state(value: &str) -> Result<JobState, CatalogError> {
    match value {
        "REGISTERED" => Ok(JobState::Registered),
        "SOURCE_PLANNED" => Ok(JobState::SourcePlanned),
        "MAPPING_VALIDATED" => Ok(JobState::MappingValidated),
        "PARTITIONED" => Ok(JobState::Partitioned),
        "PROJECTED" => Ok(JobState::Projected),
        "IDENTIFIED" => Ok(JobState::Identified),
        "SPINE_WRITTEN" => Ok(JobState::SpineWritten),
        "INDEXED" => Ok(JobState::Indexed),
        "REASONED" => Ok(JobState::Reasoned),
        "CERTIFIED" => Ok(JobState::Certified),
        "PUBLISHED" => Ok(JobState::Published),
        "FAILED" => Ok(JobState::Failed),
        "CANCELLED" => Ok(JobState::Cancelled),
        other => Err(CatalogError::UnknownState(other.to_owned())),
    }
}

fn array_32(bytes: &[u8]) -> Result<[u8; 32], CatalogError> {
    bytes.try_into().map_err(|_| {
        CatalogError::Database(sqlx::Error::Decode(
            "catalog checksum is not 32 bytes".into(),
        ))
    })
}

fn is_unique_violation(error: &sqlx::Error) -> bool {
    error
        .as_database_error()
        .and_then(|database_error| database_error.code())
        .is_some_and(|code| code == "23505")
}

#[cfg(test)]
mod tests {
    use super::{DistributedWorkKind, JobState, may_transition};

    #[test]
    fn state_machine_has_no_skipping_edge() {
        assert!(may_transition(
            JobState::Registered,
            JobState::SourcePlanned
        ));
        assert!(!may_transition(JobState::Registered, JobState::Projected));
        assert!(!may_transition(JobState::Certified, JobState::Cancelled));
        assert!(!may_transition(JobState::Published, JobState::Failed));
    }

    #[test]
    fn cancellation_cannot_retract_publication() {
        assert!(may_transition(JobState::Reasoned, JobState::Cancelled));
        assert!(!may_transition(JobState::Published, JobState::Cancelled));
    }

    #[test]
    fn artifact_work_kind_has_one_catalog_encoding() {
        assert_eq!(DistributedWorkKind::Artifact.as_db(), "ARTIFACT");
    }
}
