//! Shared Kubernetes API contracts used by the control plane and operator.

use kube::CustomResource;
pub use ngkg_types::PublicationPolicy;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use uuid::Uuid;

/// Desired state for one immutable reference compilation.
#[derive(Clone, CustomResource, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[kube(
    group = "ngkg.io",
    version = "v1alpha1",
    kind = "NgkgCompilation",
    plural = "ngkgcompilations",
    namespaced,
    status = "NgkgCompilationStatus",
    shortname = "ngkgc"
)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct NgkgCompilationSpec {
    /// Tenant established by the authenticated API identity.
    pub tenant_id: Uuid,
    /// Catalog dataset that owns the target snapshot.
    pub dataset_id: Uuid,
    /// Durable operation created before this resource.
    pub operation_id: Uuid,
    /// Relative object key of the immutable compilation bundle.
    pub bundle_object_key: String,
    /// Lowercase SHA-256 of the bundle bytes.
    pub bundle_sha256: String,
    /// Optional active snapshot expected during publication.
    pub parent_snapshot_id: Option<Uuid>,
    /// Immutable snapshot identity chosen before work starts.
    pub target_snapshot_id: Uuid,
    /// Manual or guarded automatic publication.
    pub publication_policy: PublicationPolicy,
    /// Operator-owned resource profile name.
    pub resource_profile: String,
}

/// Concise observed state; detailed work remains in PostgreSQL and object storage.
#[derive(Clone, Debug, Default, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct NgkgCompilationStatus {
    /// Kubernetes generation already reconciled.
    pub observed_generation: Option<i64>,
    /// Last durable catalog state observed by the operator.
    pub catalog_state: Option<String>,
    /// Deterministic Kubernetes Job name, when scheduled.
    pub job_name: Option<String>,
    /// Human-readable, non-sensitive reconciliation condition.
    pub condition: Option<String>,
}

/// Supported object-storage providers for an existing-source import.
#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum CloudObjectProvider {
    /// Amazon S3 through the Mountpoint CSI driver.
    AwsS3,
    /// Azure Blob Storage through the Blob CSI driver.
    AzureBlob,
    /// Google Cloud Storage through the GCS FUSE CSI driver.
    Gcs,
}

/// Policy used to freeze mutable object names into one immutable import.
#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum CloudObjectVersionPolicy {
    /// Every selected object must expose a provider version or generation.
    RequireVersionedObjects,
    /// Permit an immutable bucket policy plus a strong checksum when versions are unavailable.
    RequireImmutableChecksum,
}

/// Desired state for importing existing TriG objects from a read-only cloud bucket.
#[derive(Clone, CustomResource, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[kube(
    group = "ngkg.io",
    version = "v1alpha1",
    kind = "NgkgSourceImport",
    plural = "ngkgsourceimports",
    namespaced,
    status = "NgkgSourceImportStatus",
    shortname = "ngkgsi"
)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct NgkgSourceImportSpec {
    /// Tenant established by the authenticated API identity.
    pub tenant_id: Uuid,
    /// Catalog dataset that will own the target snapshot.
    pub dataset_id: Uuid,
    /// Deterministic operation identity derived from the idempotent request.
    pub operation_id: Uuid,
    /// Existing cloud-object provider.
    pub provider: CloudObjectProvider,
    /// Existing bucket or Azure container name.
    pub bucket: String,
    /// Optional Azure storage-account name; rejected for other providers.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub account_name: Option<String>,
    /// Optional normalized object prefix used for bounded discovery.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
    /// Exact normalized object keys. When non-empty, prefix discovery is disabled.
    pub object_keys: Vec<String>,
    /// Operator-approved include patterns. Phase 40.13.10 permits only `**/*.trig`.
    pub include_patterns: Vec<String>,
    /// Path-segment exclusions; alignment, closure, and provenance are mandatory.
    pub exclude_segments: Vec<String>,
    /// Existing workload-identity-enabled Kubernetes ServiceAccount name.
    pub identity_ref: String,
    /// Object version/generation policy used by the discovery manifest.
    pub version_policy: CloudObjectVersionPolicy,
    /// Immutable snapshot identity selected before discovery starts.
    pub target_snapshot_id: Uuid,
    /// Optional active snapshot expected during publication.
    pub parent_snapshot_id: Option<Uuid>,
    /// Manual or guarded automatic publication after full qualification.
    pub publication_policy: PublicationPolicy,
    /// Operator-owned resource profile name.
    pub resource_profile: String,
    /// Total selected-source byte ceiling.
    pub max_source_bytes: u64,
    /// Selected-object count ceiling.
    pub max_source_objects: u32,
    /// Stable logical partition count; independent of current pod count.
    pub logical_partitions: u32,
    /// Control-plane-produced authorization/import/datatype qualification request object key.
    pub ontology_qualification_request_object_key: String,
    /// SHA-256 of the exact OWL 2 DL qualification request bytes.
    pub ontology_qualification_request_sha256: String,
}

/// Checksum-bound observed state for an existing cloud-source import.
#[derive(Clone, Debug, Default, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct NgkgSourceImportStatus {
    /// Kubernetes generation already reconciled.
    pub observed_generation: Option<i64>,
    /// Deterministic Kubernetes Job name, when scheduled.
    pub job_name: Option<String>,
    /// Deterministic artifact-store key of the frozen source manifest.
    pub source_manifest_object_key: Option<String>,
    /// SHA-256 of the exact source-manifest bytes.
    pub source_manifest_sha256: Option<String>,
    /// Deterministic artifact-store key of the immutable whole-object decode plan.
    pub decode_plan_object_key: Option<String>,
    /// SHA-256 of the exact decode-plan bytes.
    pub decode_plan_sha256: Option<String>,
    /// Number of syntax-safe Indexed Job completions in the decode barrier.
    pub decode_work_item_count: Option<u32>,
    /// Kubernetes name of the distributed decode job.
    pub decode_job_name: Option<String>,
    /// Kubernetes name of the all-partitions verification job.
    pub finalize_job_name: Option<String>,
    /// Immutable compiler-handoff manifest, present only after the full barrier verifies.
    pub compiler_handoff_object_key: Option<String>,
    /// SHA-256 of the exact compiler-handoff manifest bytes.
    pub compiler_handoff_sha256: Option<String>,
    /// Kubernetes name of the fragment-map Indexed Job.
    pub semantic_map_job_name: Option<String>,
    /// Kubernetes name of the global dictionary barrier Job.
    pub semantic_dictionary_job_name: Option<String>,
    /// Checksum-bound global RDF dictionary manifest key.
    pub semantic_dictionary_object_key: Option<String>,
    /// SHA-256 of the global RDF dictionary manifest.
    pub semantic_dictionary_sha256: Option<String>,
    /// Kubernetes name of the logical-partition Indexed Job.
    pub semantic_partition_job_name: Option<String>,
    /// Kubernetes name of the all-partitions semantic finalizer.
    pub semantic_finalize_job_name: Option<String>,
    /// Inactive semantic compilation root object key.
    pub semantic_compilation_root_object_key: Option<String>,
    /// SHA-256 of the inactive semantic compilation root.
    pub semantic_compilation_root_sha256: Option<String>,
    /// Unique logical facts compiled after RDF set deduplication.
    pub compiled_fact_count: Option<u64>,
    /// Kubernetes name of the asserted-ontology projection Indexed Job.
    pub ontology_projection_job_name: Option<String>,
    /// Kubernetes name of the deterministic ontology assembly barrier.
    pub ontology_assembly_job_name: Option<String>,
    /// Object key of the complete local-import-closure assembly manifest.
    pub ontology_assembly_object_key: Option<String>,
    /// SHA-256 of the ontology assembly manifest.
    pub ontology_assembly_sha256: Option<String>,
    /// Kubernetes name of the exact global HermiT qualification Job.
    pub ontology_qualification_job_name: Option<String>,
    /// Inactive exact OWL 2 DL qualification root object key.
    pub ontology_qualification_root_object_key: Option<String>,
    /// SHA-256 of the exact OWL 2 DL qualification root.
    pub ontology_qualification_root_sha256: Option<String>,
    /// Kubernetes name of the deterministic HermiT-closure fan-out planner.
    pub offline_reasoning_plan_job_name: Option<String>,
    /// Immutable offline reasoning plan object key.
    pub offline_reasoning_plan_object_key: Option<String>,
    /// SHA-256 of the offline reasoning plan.
    pub offline_reasoning_plan_sha256: Option<String>,
    /// Stable logical partition count in the plan.
    pub offline_reasoning_partition_count: Option<u32>,
    /// Kubernetes name of the offline Indexed partition Job.
    pub offline_reasoning_partition_job_name: Option<String>,
    /// Kubernetes name of the all-partitions offline finalizer.
    pub offline_reasoning_finalize_job_name: Option<String>,
    /// Inactive exact finite-consequence root object key.
    pub offline_reasoning_root_object_key: Option<String>,
    /// SHA-256 of the inactive finite-consequence root.
    pub offline_reasoning_root_sha256: Option<String>,
    /// Kubernetes name of the all-roots activation barrier.
    pub snapshot_activation_job_name: Option<String>,
    /// Immutable Phase 40.13.15 activation manifest object key.
    pub snapshot_activation_manifest_object_key: Option<String>,
    /// SHA-256 of the exact activation manifest.
    pub snapshot_activation_manifest_sha256: Option<String>,
    /// Catalog snapshot state after atomic certification/publication.
    pub snapshot_publication_state: Option<String>,
    /// Number of immutable TriG objects frozen by discovery.
    pub selected_object_count: Option<u32>,
    /// Total selected source bytes.
    pub selected_source_bytes: Option<u64>,
    /// Total parsed RDF quad count.
    pub parsed_quad_count: Option<u64>,
    /// Non-sensitive fail-closed state.
    pub condition: Option<String>,
}

/// Build a server-side-apply status document containing only fields owned by one writer.
///
/// Kubernetes status is shared by the control-plane operator and several stage workers.
/// Serializing a complete stale status as a JSON merge patch can delete or overwrite a
/// concurrently published checksum. This helper deliberately emits only non-null fields
/// from the caller's explicit ownership set. Separate field-manager names then make the
/// API server reject accidental cross-writer ownership instead of silently losing data.
pub fn source_import_status_apply_document(
    name: &str,
    status: &NgkgSourceImportStatus,
    owned_camel_case_fields: &[&str],
) -> Result<Value, serde_json::Error> {
    let serialized = serde_json::to_value(status)?;
    let source = serialized.as_object().cloned().unwrap_or_default();
    let mut owned = Map::new();
    for field in owned_camel_case_fields {
        if let Some(value) = source.get(*field).filter(|value| !value.is_null()) {
            owned.insert((*field).to_owned(), value.clone());
        }
    }
    Ok(json!({
        "apiVersion": "ngkg.io/v1alpha1",
        "kind": "NgkgSourceImport",
        "metadata": {"name": name},
        "status": owned
    }))
}

/// Closed storage-maintenance intent. None of these operations change RDF or OWL semantics.
#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum StorageRecoveryKind {
    /// Establish or repair the configured replica factor.
    Replicate,
    /// Move replicas while retaining the old copy through the verification barrier.
    Relocate,
    /// Replace replicas lost with a node or failure domain.
    NodeLoss,
    /// Create a checksum-bound independent backup.
    Backup,
    /// Restore a backup into a new certified-inactive snapshot identity.
    Restore,
}

/// Desired state for a checksum-bound distributed storage operation.
#[derive(Clone, CustomResource, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[kube(
    group = "ngkg.io",
    version = "v1alpha1",
    kind = "NgkgStorageRecovery",
    plural = "ngkgstoragerecoveries",
    namespaced,
    status = "NgkgStorageRecoveryStatus",
    shortname = "ngkgsr"
)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct NgkgStorageRecoverySpec {
    /// Authenticated tenant boundary.
    pub tenant_id: Uuid,
    /// Dataset that owns the snapshot.
    pub dataset_id: Uuid,
    /// Retry-stable operation identity.
    pub operation_id: Uuid,
    /// Source snapshot. Restore never overwrites this identity.
    pub source_snapshot_id: Uuid,
    /// New inactive snapshot identity for restore operations.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub restored_snapshot_id: Option<Uuid>,
    /// Storage intent.
    pub kind: StorageRecoveryKind,
    /// Exact immutable recovery plan key.
    pub plan_object_key: String,
    /// SHA-256 of the exact recovery plan bytes.
    pub plan_sha256: String,
    /// Dense Indexed Job completion count.
    pub task_count: u32,
    /// Bounded concurrent completions, independent of logical task count.
    pub max_parallelism: u32,
    /// Largest artifact in this exact plan, used to enforce aggregate scratch admission.
    pub largest_task_bytes: u64,
    /// Maximum aggregate bytes admitted by the operation.
    pub max_in_flight_bytes: u64,
    /// Operator-owned resource profile.
    pub resource_profile: String,
}

/// Fail-closed observed state for distributed storage recovery.
#[derive(Clone, Debug, Default, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct NgkgStorageRecoveryStatus {
    /// Kubernetes generation already reconciled.
    pub observed_generation: Option<i64>,
    /// Indexed transfer Job.
    pub transfer_job_name: Option<String>,
    /// All-partitions finalizer Job.
    pub finalize_job_name: Option<String>,
    /// Exact recovery certificate key, present only after every partition verifies.
    pub recovery_certificate_object_key: Option<String>,
    /// SHA-256 of the exact recovery certificate bytes.
    pub recovery_certificate_sha256: Option<String>,
    /// Exact backup manifest key for successful backup operations.
    pub backup_manifest_object_key: Option<String>,
    /// SHA-256 of the exact backup manifest.
    pub backup_manifest_sha256: Option<String>,
    /// Exact restore certificate key for successful restore operations.
    pub restore_certificate_object_key: Option<String>,
    /// SHA-256 of the exact restore certificate.
    pub restore_certificate_sha256: Option<String>,
    /// Number of checksum-quarantined replicas observed by reconciliation.
    pub quarantined_replica_count: u32,
    /// Non-sensitive fail-closed condition.
    pub condition: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::{NgkgCompilationSpec, NgkgSourceImportSpec, PublicationPolicy};
    use uuid::Uuid;

    #[test]
    fn compilation_spec_rejects_unknown_json_fields() {
        let value = serde_json::json!({
            "tenantId": Uuid::nil(),
            "datasetId": Uuid::nil(),
            "operationId": Uuid::nil(),
            "bundleObjectKey": "bundles/example.json",
            "bundleSha256": "0".repeat(64),
            "parentSnapshotId": null,
            "targetSnapshotId": Uuid::nil(),
            "publicationPolicy": "manual-after-certification",
            "resourceProfile": "reference-balanced",
            "unexpected": true
        });
        assert!(serde_json::from_value::<NgkgCompilationSpec>(value).is_err());
    }

    #[test]
    fn publication_policy_has_stable_catalog_value() {
        assert_eq!(
            PublicationPolicy::AutomaticAfterCertification.as_db(),
            "AUTOMATIC_AFTER_CERTIFICATION"
        );
    }

    #[test]
    fn cloud_import_spec_rejects_inline_credentials_and_unknown_fields() {
        let value = serde_json::json!({
            "tenantId": Uuid::nil(),
            "datasetId": Uuid::nil(),
            "operationId": Uuid::nil(),
            "provider": "aws-s3",
            "bucket": "existing-bucket",
            "accountName": null,
            "prefix": "production/",
            "objectKeys": [],
            "includePatterns": ["**/*.trig"],
            "excludeSegments": ["alignment", "closure", "provenance"],
            "identityRef": "ngkg-import-reader",
            "versionPolicy": "require-versioned-objects",
            "targetSnapshotId": Uuid::nil(),
            "parentSnapshotId": null,
            "publicationPolicy": "manual-after-certification",
            "resourceProfile": "distributed-enterprise",
            "maxSourceBytes": 1_000_000,
            "maxSourceObjects": 10,
            "logicalPartitions": 64,
            "ontologyQualificationRequestObjectKey": "imports/qualification.json",
            "ontologyQualificationRequestSha256": "1".repeat(64),
            "accessKey": "must-never-enter-the-contract"
        });
        assert!(serde_json::from_value::<NgkgSourceImportSpec>(value).is_err());
    }
}
