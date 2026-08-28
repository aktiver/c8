//! Deterministic source manifests and syntax/row-group-safe work envelopes.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use uuid::Uuid;

/// An immutable, completely identified unit of distributed work.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct WorkEnvelope {
    pub dataset_id: Uuid,
    pub target_snapshot_id: Uuid,
    pub stage: Stage,
    pub partition_id: String,
    pub source: SourceRange,
    pub input_sha256: [u8; 32],
    pub mapping_version: String,
    pub ontology_bundle_hash: [u8; 32],
    pub expected_schema_hash: [u8; 32],
    pub output_contract_hash: [u8; 32],
    pub retry_class: RetryClass,
    pub resource_class: ResourceClass,
}

/// Safe source granularity. Arbitrary TriG byte ranges are intentionally absent.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SourceRange {
    /// A canonical N-Quads shard produced by a validated TriG safe-scan stage.
    TrigShard {
        object_uri: String,
        shard_manifest_uri: String,
        statement_count: u64,
    },
    /// Exact Parquet row groups discovered from file metadata.
    ParquetRowGroups {
        object_uri: String,
        row_groups: Vec<u32>,
        required_columns: Vec<String>,
    },
    /// Exact Iceberg snapshot manifest entries.
    IcebergEntries {
        table_uri: String,
        snapshot_id: i64,
        manifest_uri: String,
        entry_ordinals: Vec<u32>,
    },
}

/// Closed stage vocabulary.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Stage {
    SafeScan,
    Projection,
    DictionaryRun,
    IndexRun,
    Validation,
    Export,
}

/// Retry classification separates infrastructure failure from bad data.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RetryClass {
    Idempotent,
    DeterministicFailure,
}

/// Hardware responsibility requested from the scheduler.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ResourceClass {
    SemanticProjection,
    Reasoning,
    IndexBuild,
    SparqlQueryProcessing,
    ParquetHydration,
    MaintenanceExport,
}

/// Inputs shared by every deterministic envelope.
#[derive(Clone, Debug)]
pub struct EnvelopeContext {
    pub dataset_id: Uuid,
    pub target_snapshot_id: Uuid,
    pub input_sha256: [u8; 32],
    pub mapping_version: String,
    pub ontology_bundle_hash: [u8; 32],
    pub expected_schema_hash: [u8; 32],
    pub output_contract_hash: [u8; 32],
}

/// Invalid partition policies are rejected instead of repaired silently.
#[derive(Debug, Error, Eq, PartialEq)]
pub enum PlanningError {
    #[error("row group target must be greater than zero")]
    ZeroTarget,
    #[error("a projection requires at least one source row group")]
    EmptySource,
    #[error("required column names must be non-empty")]
    EmptyColumn,
    #[error("a cloud decode plan requires at least one frozen TriG object")]
    EmptyCloudSource,
    #[error("cloud decode target bytes and maximum work items must be positive")]
    InvalidCloudLimits,
    #[error("frozen cloud source ordinals must be contiguous and totals must match")]
    InvalidCloudManifest,
}

/// One immutable TriG object frozen by cloud discovery.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct FrozenCloudSourceObject {
    /// Contiguous source-manifest ordinal.
    pub ordinal: u32,
    /// Normalized path relative to the read-only source mount.
    pub object_key: String,
    /// Frozen source byte count.
    pub bytes: u64,
    /// SHA-256 of the exact source bytes.
    pub sha256: String,
    /// Strictly parsed RDF quad count.
    pub parsed_quad_count: u64,
    /// Quads parsed in the default graph.
    pub default_graph_quad_count: u64,
    /// Per-named-graph quad counts.
    pub named_graph_quad_counts: std::collections::BTreeMap<String, u64>,
    /// Deterministic scope applied to this object's blank-node labels downstream.
    pub blank_node_scope: String,
}

/// Checksum-bound discovery result consumed by the cloud compiler planner.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct FrozenCloudSourceManifest {
    /// Contract version.
    pub format_version: u32,
    /// Authenticated tenant identity.
    pub tenant_id: Uuid,
    /// Catalog dataset identity.
    pub dataset_id: Uuid,
    /// Idempotent cloud-import operation identity.
    pub operation_id: Uuid,
    /// Immutable target snapshot identity.
    pub target_snapshot_id: Uuid,
    /// Closed cloud-provider value.
    pub provider: String,
    /// Existing bucket or container name.
    pub bucket: String,
    /// Azure storage account, when applicable.
    pub account_name: Option<String>,
    /// Operator-controlled read-only source mount.
    pub source_mount: String,
    /// Object immutability policy applied during discovery.
    pub version_policy: String,
    /// Stable downstream semantic partition count.
    pub logical_partitions: u32,
    /// Complete ordered frozen object set.
    pub objects: Vec<FrozenCloudSourceObject>,
    /// Declared object total.
    pub total_objects: u32,
    /// Declared source byte total.
    pub total_bytes: u64,
    /// Declared parsed quad total.
    pub total_quads: u64,
    /// Aggregate identity over ordered object names, sizes, and hashes.
    pub aggregate_source_sha256: String,
}

/// Deterministic, syntax-safe unit selected by a Kubernetes completion index.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct CloudDecodeWorkItem {
    /// Kubernetes completion index; not a semantic partition identity.
    pub completion_index: u32,
    /// Checksum-derived immutable work identity.
    pub work_id: String,
    /// Sum of frozen input bytes assigned to this completion.
    pub total_bytes: u64,
    /// Sum of frozen quads assigned to this completion.
    pub total_quads: u64,
    /// Complete TriG objects assigned without byte splitting.
    pub objects: Vec<FrozenCloudSourceObject>,
}

/// Immutable bridge from a frozen bucket manifest to distributed RDF decoding.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct CloudDecodePlan {
    /// Contract version.
    pub format_version: u32,
    /// Closed deterministic planner algorithm.
    pub planner: String,
    /// Authenticated tenant identity.
    pub tenant_id: Uuid,
    /// Catalog dataset identity.
    pub dataset_id: Uuid,
    /// Idempotent operation identity.
    pub operation_id: Uuid,
    /// Immutable target snapshot identity.
    pub target_snapshot_id: Uuid,
    /// Artifact-store key of the frozen input manifest.
    pub source_manifest_object_key: String,
    /// SHA-256 of the exact frozen input manifest.
    pub source_manifest_sha256: String,
    /// Aggregate source identity inherited from discovery.
    pub aggregate_source_sha256: String,
    /// Stable downstream semantic partition count.
    pub logical_partitions: u32,
    /// Largest-first scheduler target; complete objects may exceed it.
    pub target_work_bytes: u64,
    /// Complete ordered Indexed Job work set.
    pub work_items: Vec<CloudDecodeWorkItem>,
    /// Exact expected completion barrier size.
    pub total_work_items: u32,
    /// Exact frozen object count.
    pub total_objects: u32,
    /// Exact frozen source byte count.
    pub total_bytes: u64,
    /// Exact frozen quad count.
    pub total_quads: u64,
}

/// Build a deterministic largest-first schedule without splitting TriG byte streams.
pub fn plan_cloud_decode(
    manifest: &FrozenCloudSourceManifest,
    source_manifest_object_key: &str,
    source_manifest_sha256: &str,
    target_work_bytes: u64,
    max_work_items: u32,
) -> Result<CloudDecodePlan, PlanningError> {
    if manifest.objects.is_empty() {
        return Err(PlanningError::EmptyCloudSource);
    }
    if target_work_bytes == 0 || max_work_items == 0 {
        return Err(PlanningError::InvalidCloudLimits);
    }
    let object_count = u32::try_from(manifest.objects.len())
        .map_err(|_| PlanningError::InvalidCloudManifest)?;
    let totals = manifest.objects.iter().enumerate().try_fold(
        (0_u64, 0_u64),
        |(bytes, quads), (ordinal, object)| {
            if object.ordinal
                != u32::try_from(ordinal).map_err(|_| PlanningError::InvalidCloudManifest)?
            {
                return Err(PlanningError::InvalidCloudManifest);
            }
            Ok((
                bytes
                    .checked_add(object.bytes)
                    .ok_or(PlanningError::InvalidCloudManifest)?,
                quads
                    .checked_add(object.parsed_quad_count)
                    .ok_or(PlanningError::InvalidCloudManifest)?,
            ))
        },
    )?;
    if object_count != manifest.total_objects
        || totals != (manifest.total_bytes, manifest.total_quads)
    {
        return Err(PlanningError::InvalidCloudManifest);
    }
    let target_count = manifest.total_bytes.div_ceil(target_work_bytes).max(1);
    let work_count = u32::try_from(target_count)
        .unwrap_or(u32::MAX)
        .min(max_work_items)
        .min(object_count)
        .max(1);
    let mut sorted = manifest.objects.clone();
    sorted.sort_unstable_by(|left, right| {
        right
            .bytes
            .cmp(&left.bytes)
            .then_with(|| left.ordinal.cmp(&right.ordinal))
    });
    let group_count =
        usize::try_from(work_count).map_err(|_| PlanningError::InvalidCloudLimits)?;
    let mut groups = vec![Vec::<FrozenCloudSourceObject>::new(); group_count];
    let mut group_bytes = vec![0_u64; groups.len()];
    for object in sorted {
        let index = group_bytes
            .iter()
            .enumerate()
            .min_by_key(|(index, bytes)| (**bytes, *index))
            .map(|(index, _)| index)
            .ok_or(PlanningError::InvalidCloudLimits)?;
        group_bytes[index] = group_bytes[index]
            .checked_add(object.bytes)
            .ok_or(PlanningError::InvalidCloudManifest)?;
        groups[index].push(object);
    }
    let mut work_items = Vec::with_capacity(groups.len());
    for (index, mut objects) in groups.into_iter().enumerate() {
        objects.sort_unstable_by_key(|object| object.ordinal);
        let total_bytes = objects.iter().map(|object| object.bytes).sum();
        let total_quads = objects.iter().map(|object| object.parsed_quad_count).sum();
        let mut identity = Sha256::new();
        identity.update(b"ngkg-cloud-decode-work-v1\0");
        identity.update(manifest.operation_id.as_bytes());
        for object in &objects {
            identity.update(object.ordinal.to_be_bytes());
            identity.update(object.sha256.as_bytes());
        }
        work_items.push(CloudDecodeWorkItem {
            completion_index: u32::try_from(index).map_err(|_| PlanningError::InvalidCloudLimits)?,
            work_id: format!("sha256:{}", hex::encode(identity.finalize())),
            total_bytes,
            total_quads,
            objects,
        });
    }
    Ok(CloudDecodePlan {
        format_version: 1,
        planner: "whole-trig-lpt-v1".to_owned(),
        tenant_id: manifest.tenant_id,
        dataset_id: manifest.dataset_id,
        operation_id: manifest.operation_id,
        target_snapshot_id: manifest.target_snapshot_id,
        source_manifest_object_key: source_manifest_object_key.to_owned(),
        source_manifest_sha256: source_manifest_sha256.to_owned(),
        aggregate_source_sha256: manifest.aggregate_source_sha256.clone(),
        logical_partitions: manifest.logical_partitions,
        target_work_bytes,
        total_work_items: work_count,
        total_objects: manifest.total_objects,
        total_bytes: manifest.total_bytes,
        total_quads: manifest.total_quads,
        work_items,
    })
}

/// Deterministically group exact Parquet row groups into independent envelopes.
pub fn plan_parquet_row_groups(
    context: &EnvelopeContext,
    object_uri: &str,
    row_group_count: u32,
    row_groups_per_partition: u32,
    required_columns: &[String],
) -> Result<Vec<WorkEnvelope>, PlanningError> {
    if row_groups_per_partition == 0 {
        return Err(PlanningError::ZeroTarget);
    }
    if row_group_count == 0 {
        return Err(PlanningError::EmptySource);
    }
    if required_columns.iter().any(String::is_empty) {
        return Err(PlanningError::EmptyColumn);
    }
    let groups: Vec<u32> = (0..row_group_count).collect();
    let partition_size = usize::try_from(row_groups_per_partition).map_err(|_| PlanningError::ZeroTarget)?;
    Ok(groups
        .chunks(partition_size)
        .map(|chunk| {
            let source = SourceRange::ParquetRowGroups {
                object_uri: object_uri.to_owned(),
                row_groups: chunk.to_vec(),
                required_columns: required_columns.to_vec(),
            };
            envelope(context, Stage::Projection, source)
        })
        .collect())
}

fn envelope(context: &EnvelopeContext, stage: Stage, source: SourceRange) -> WorkEnvelope {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"ngkg-work-envelope-v1");
    hasher.update(context.dataset_id.as_bytes());
    hasher.update(context.target_snapshot_id.as_bytes());
    hasher.update(&context.input_sha256);
    hasher.update(format!("{stage:?}:{source:?}").as_bytes());
    WorkEnvelope {
        dataset_id: context.dataset_id,
        target_snapshot_id: context.target_snapshot_id,
        stage,
        partition_id: format!("blake3:{}", hasher.finalize().to_hex()),
        source,
        input_sha256: context.input_sha256,
        mapping_version: context.mapping_version.clone(),
        ontology_bundle_hash: context.ontology_bundle_hash,
        expected_schema_hash: context.expected_schema_hash,
        output_contract_hash: context.output_contract_hash,
        retry_class: RetryClass::Idempotent,
        resource_class: ResourceClass::SemanticProjection,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::{
        EnvelopeContext, FrozenCloudSourceManifest, FrozenCloudSourceObject, SourceRange,
        plan_cloud_decode, plan_parquet_row_groups,
    };
    use uuid::Uuid;

    #[test]
    fn partitions_cover_each_row_group_once() {
        let context = EnvelopeContext {
            dataset_id: Uuid::from_u128(1),
            target_snapshot_id: Uuid::from_u128(2),
            input_sha256: [3; 32],
            mapping_version: "v1".to_owned(),
            ontology_bundle_hash: [4; 32],
            expected_schema_hash: [5; 32],
            output_contract_hash: [6; 32],
        };
        let columns = vec!["entity_guid".to_owned(), "metric_value".to_owned()];
        let first = plan_parquet_row_groups(&context, "s3://bucket/f.parquet", 7, 3, &columns);
        let second = plan_parquet_row_groups(&context, "s3://bucket/f.parquet", 7, 3, &columns);
        assert!(first.is_ok());
        assert_eq!(first, second);
        let mut observed = Vec::new();
        if let Ok(envelopes) = first {
            for envelope in envelopes {
                if let SourceRange::ParquetRowGroups { row_groups, .. } = envelope.source {
                    observed.extend(row_groups);
                }
            }
        }
        assert_eq!(observed, (0..7).collect::<Vec<_>>());
    }

    #[test]
    fn cloud_plan_is_deterministic_and_never_splits_an_object() {
        let objects = [9_u64, 8, 2, 1]
            .into_iter()
            .enumerate()
            .map(|(ordinal, bytes)| FrozenCloudSourceObject {
                ordinal: u32::try_from(ordinal).unwrap_or_default(),
                object_key: format!("asserted/{ordinal}.trig"),
                bytes,
                sha256: format!("{ordinal:064x}"),
                parsed_quad_count: bytes,
                default_graph_quad_count: bytes,
                named_graph_quad_counts: BTreeMap::new(),
                blank_node_scope: format!("object-{ordinal:08}-{:064x}", ordinal),
            })
            .collect::<Vec<_>>();
        let manifest = FrozenCloudSourceManifest {
            format_version: 1,
            tenant_id: Uuid::from_u128(1),
            dataset_id: Uuid::from_u128(2),
            operation_id: Uuid::from_u128(3),
            target_snapshot_id: Uuid::from_u128(4),
            provider: "aws-s3".to_owned(),
            bucket: "bucket".to_owned(),
            account_name: None,
            source_mount: "/source".to_owned(),
            version_policy: "require-immutable-checksum".to_owned(),
            logical_partitions: 256,
            objects,
            total_objects: 4,
            total_bytes: 20,
            total_quads: 20,
            aggregate_source_sha256: "a".repeat(64),
        };
        let first = plan_cloud_decode(&manifest, "source.json", &"b".repeat(64), 10, 8);
        let second = plan_cloud_decode(&manifest, "source.json", &"b".repeat(64), 10, 8);
        assert_eq!(first, second);
        assert!(first.is_ok());
        if let Ok(plan) = first {
            assert_eq!(plan.total_work_items, 2);
            assert_eq!(
                plan.work_items
                    .iter()
                    .map(|item| item.objects.len())
                    .sum::<usize>(),
                4
            );
            assert_eq!(plan.work_items[0].total_bytes, 10);
            assert_eq!(plan.work_items[1].total_bytes, 10);
        }
    }
}
