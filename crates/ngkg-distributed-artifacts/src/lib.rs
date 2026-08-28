//! Topology-stable distributed construction of NGKG columnar semantic artifacts.
//!
//! Each logical source partition is independently encoded with the immutable
//! global dictionary produced by the Phase 15 reducer. Kubernetes pod count and
//! completion order affect scheduling only. A bounded-memory finalizer validates
//! exact partition coverage and merges sorted locator runs without reading any
//! Parquet payload row.

use std::{
    cmp::Reverse,
    collections::{BTreeMap, BTreeSet, BinaryHeap},
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, BufWriter, Write},
    path::{Component, Path, PathBuf},
    sync::Arc,
};

use arrow_array::{
    ArrayRef, BooleanArray, RecordBatch, StringArray, UInt8Array, UInt64Array,
    builder::FixedSizeBinaryBuilder,
};
use arrow_schema::{DataType, Field, Schema};
use ngkg_distributed_build::SourcePlan;
use ngkg_reference::{
    NormalizedFact, NormalizedObject, ProjectionPolicy, Treatment, nquad_line, ntriple_line,
    parse_nquads, public_resource_lexical, sha256_path,
};
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use uuid::Uuid;

/// Phase 16 artifact contract version.
pub const DISTRIBUTED_ARTIFACT_FORMAT_VERSION: u32 = 1;

/// Immutable inputs needed to encode one logical partition.
#[derive(Clone, Debug)]
pub struct ArtifactPartitionRequest<'a> {
    /// Original source identity used for deterministic FactIDs.
    pub source_sha256: &'a str,
    /// Dataset GUID namespace.
    pub dataset_namespace: Uuid,
    /// Source GUID.
    pub source_guid: Uuid,
    /// Source version identity.
    pub source_snapshot: &'a str,
    /// Exhaustive projection policy.
    pub projection_policy: &'a ProjectionPolicy,
    /// Maximum facts accepted by one partition.
    pub max_quads: u64,
    /// Maximum rows in one Parquet row group.
    pub row_group_rows: usize,
}

/// One immutable artifact produced by a partition worker.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ShardArtifact {
    /// Path relative to the partition manifest directory.
    pub relative_path: String,
    /// Lowercase SHA-256.
    pub sha256: String,
    /// Exact stored bytes.
    pub bytes: u64,
}

/// Completion evidence for one independently scheduled artifact partition.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ArtifactPartitionManifest {
    /// Contract version.
    pub format_version: u32,
    /// Dataset identity.
    pub dataset_id: Uuid,
    /// Snapshot identity.
    pub snapshot_id: Uuid,
    /// Source-plan checksum.
    pub source_plan_sha256: String,
    /// Global dictionary checksum.
    pub dictionary_sha256: String,
    /// Dense logical partition index.
    pub partition_index: u32,
    /// Stable Phase 15 work identity.
    pub partition_id: String,
    /// All output files, excluding this manifest.
    pub artifacts: Vec<ShardArtifact>,
    /// Total input facts.
    pub fact_count: u64,
    /// Non-payload semantic rows.
    pub semantic_row_count: u64,
    /// Payload rows.
    pub payload_row_count: u64,
    /// Query-visible RDF facts.
    pub queryable_fact_count: u64,
    /// Reasoning-visible RDF facts.
    pub reasoning_fact_count: u64,
    /// Sorted locator rows.
    pub locator_record_count: u64,
    /// Hash over canonical logical facts and treatments, independent of Parquet metadata.
    pub semantic_content_sha256: String,
}

/// One partition reference in the global artifact root.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ArtifactPartitionReference {
    /// Logical partition index.
    pub partition_index: u32,
    /// Stable partition identity.
    pub partition_id: String,
    /// Immutable manifest path supplied to the finalizer.
    pub manifest_path: String,
    /// Manifest checksum.
    pub manifest_sha256: String,
    /// Logical input facts.
    pub fact_count: u64,
    /// Semantic rows.
    pub semantic_row_count: u64,
    /// Payload rows.
    pub payload_row_count: u64,
}

/// Complete distributed semantic-artifact bill of materials.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DistributedArtifactRootManifest {
    /// Contract version.
    pub format_version: u32,
    /// Dataset identity.
    pub dataset_id: Uuid,
    /// Snapshot identity.
    pub snapshot_id: Uuid,
    /// Source-plan checksum.
    pub source_plan_sha256: String,
    /// Global dictionary checksum.
    pub dictionary_sha256: String,
    /// Every logical partition exactly once.
    pub partitions: Vec<ArtifactPartitionReference>,
    /// Globally sorted direct locator.
    pub locator_path: String,
    /// Locator checksum.
    pub locator_sha256: String,
    /// Input fact count.
    pub fact_count: u64,
    /// Semantic row count.
    pub semantic_row_count: u64,
    /// Payload row count.
    pub payload_row_count: u64,
    /// Locator record count.
    pub locator_record_count: u64,
    /// Topology-independent root over partition semantic hashes.
    pub semantic_content_sha256: String,
}

/// Result of comparing two physical executions of the same logical artifact plan.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ArtifactEquivalenceReport {
    /// Contract version.
    pub format_version: u32,
    /// Baseline root checksum.
    pub baseline_root_sha256: String,
    /// Candidate root checksum.
    pub candidate_root_sha256: String,
    /// True only when all topology-independent fields and partition manifests agree.
    pub equivalent: bool,
    /// Human-readable differences.
    pub mismatches: Vec<String>,
}

/// Artifact compilation always rejects missing or ambiguous inputs.
#[derive(Debug, Error)]
pub enum DistributedArtifactError {
    /// Local file operation failed.
    #[error("distributed artifact I/O failed: {0}")]
    Io(#[from] std::io::Error),
    /// JSON contract failed.
    #[error("distributed artifact JSON failed: {0}")]
    Json(#[from] serde_json::Error),
    /// RDF normalization failed.
    #[error("distributed artifact RDF failed: {0}")]
    Rdf(#[from] ngkg_reference::RdfCompileError),
    /// Arrow batch construction failed.
    #[error("distributed artifact Arrow failed: {0}")]
    Arrow(#[from] arrow_schema::ArrowError),
    /// Parquet encoding failed.
    #[error("distributed artifact Parquet failed: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
    /// Immutable contract violation.
    #[error("distributed artifact contract failed: {0}")]
    Contract(String),
    /// An output path was already occupied.
    #[error("immutable output already exists: {0}")]
    ImmutableConflict(PathBuf),
}

/// Encode one exact source-plan shard into Parquet and semantic sidecars.
#[allow(clippy::too_many_arguments)]
pub fn materialize_artifact_partition(
    source_plan_path: &Path,
    expected_source_plan_sha256: &str,
    dictionary_path: &Path,
    expected_dictionary_sha256: &str,
    partition_index: u32,
    output_root: &Path,
    request: &ArtifactPartitionRequest<'_>,
) -> Result<PathBuf, DistributedArtifactError> {
    verify_file(source_plan_path, expected_source_plan_sha256)?;
    verify_file(dictionary_path, expected_dictionary_sha256)?;
    require_sha256(request.source_sha256)?;
    if request.max_quads == 0 || request.row_group_rows == 0 {
        return Err(DistributedArtifactError::Contract(
            "maxQuads and rowGroupRows must be positive".to_owned(),
        ));
    }
    if output_root.exists() {
        return Err(DistributedArtifactError::ImmutableConflict(
            output_root.to_owned(),
        ));
    }
    let plan: SourcePlan = serde_json::from_slice(&fs::read(source_plan_path)?)?;
    if plan.format_version != 1 || plan.source_sha256 != request.source_sha256 {
        return Err(DistributedArtifactError::Contract(
            "source plan does not match the artifact request".to_owned(),
        ));
    }
    let shard = plan
        .shards
        .get(usize::try_from(partition_index).map_err(|_| {
            DistributedArtifactError::Contract("partition index overflow".to_owned())
        })?)
        .filter(|value| value.partition_index == partition_index)
        .ok_or_else(|| {
            DistributedArtifactError::Contract("partition is absent from source plan".to_owned())
        })?;
    let plan_root = source_plan_path.parent().ok_or_else(|| {
        DistributedArtifactError::Contract("source plan has no parent".to_owned())
    })?;
    let shard_path = safe_join(plan_root, &shard.relative_path)?;
    verify_file(&shard_path, &shard.sha256)?;
    let dictionary = read_dictionary(dictionary_path)?;
    let source_hash = decode_sha256(request.source_sha256)?;
    let mut facts = parse_nquads(
        &shard_path,
        source_hash,
        request.dataset_namespace,
        request.source_guid,
        request.source_snapshot,
        request.projection_policy,
        request.max_quads,
    )?;
    facts.sort_unstable_by_key(|fact| fact.fact_hash);
    if u64::try_from(facts.len()).unwrap_or(u64::MAX) != shard.fact_count {
        return Err(DistributedArtifactError::Contract(
            "partition fact count differs from the source plan".to_owned(),
        ));
    }
    fs::create_dir_all(output_root)?;
    let spine_path = output_root.join("semantic-spine.parquet");
    let payload_path = output_root.join("payload.parquet");
    let queryable_path = output_root.join("queryable.nq");
    let reasoning_path = output_root.join("reasoner-core.nt");
    let locator_path = output_root.join("locator-run.tsv");
    let semantic_rows = write_spine(
        &spine_path,
        &facts,
        &dictionary,
        request.source_guid,
        plan.snapshot_id,
        request.row_group_rows,
    )?;
    let (payload_rows, locator_rows) = write_payload_and_locator(
        &payload_path,
        &locator_path,
        &facts,
        &dictionary,
        partition_index,
        request.source_guid,
        plan.snapshot_id,
        request.row_group_rows,
    )?;
    let (queryable_count, reasoning_count) =
        write_semantic_exports(&facts, &queryable_path, &reasoning_path)?;
    let semantic_content_sha256 = semantic_hash(&facts);
    let artifacts = [
        "semantic-spine.parquet",
        "payload.parquet",
        "queryable.nq",
        "reasoner-core.nt",
        "locator-run.tsv",
    ]
    .into_iter()
    .map(|relative_path| artifact(output_root, relative_path))
    .collect::<Result<Vec<_>, _>>()?;
    let manifest = ArtifactPartitionManifest {
        format_version: DISTRIBUTED_ARTIFACT_FORMAT_VERSION,
        dataset_id: plan.dataset_id,
        snapshot_id: plan.snapshot_id,
        source_plan_sha256: expected_source_plan_sha256.to_owned(),
        dictionary_sha256: expected_dictionary_sha256.to_owned(),
        partition_index,
        partition_id: shard.partition_id.clone(),
        artifacts,
        fact_count: shard.fact_count,
        semantic_row_count: semantic_rows,
        payload_row_count: payload_rows,
        queryable_fact_count: queryable_count,
        reasoning_fact_count: reasoning_count,
        locator_record_count: locator_rows,
        semantic_content_sha256,
    };
    let manifest_path = output_root.join("artifact-partition.json");
    write_json_new(&manifest_path, &manifest)?;
    sync_directory(output_root)?;
    Ok(manifest_path)
}

/// Verify exact partition coverage and build one globally sorted direct locator.
pub fn finalize_artifact_partitions(
    source_plan_path: &Path,
    expected_source_plan_sha256: &str,
    dictionary_path: &Path,
    expected_dictionary_sha256: &str,
    partition_manifest_paths: &[PathBuf],
    output_root: &Path,
) -> Result<PathBuf, DistributedArtifactError> {
    finalize_artifact_partitions_inner(
        source_plan_path,
        expected_source_plan_sha256,
        dictionary_path,
        expected_dictionary_sha256,
        partition_manifest_paths,
        output_root,
        true,
    )
}

/// Finalize manifests already committed by checksum to a durable catalog.
///
/// Only locator runs need to be materialized locally. Parquet and semantic
/// sidecars remain immutable exact-key objects described by the committed
/// partition manifests.
pub fn finalize_catalog_artifact_partitions(
    source_plan_path: &Path,
    expected_source_plan_sha256: &str,
    dictionary_path: &Path,
    expected_dictionary_sha256: &str,
    partition_manifest_paths: &[PathBuf],
    output_root: &Path,
) -> Result<PathBuf, DistributedArtifactError> {
    finalize_artifact_partitions_inner(
        source_plan_path,
        expected_source_plan_sha256,
        dictionary_path,
        expected_dictionary_sha256,
        partition_manifest_paths,
        output_root,
        false,
    )
}

#[allow(clippy::too_many_arguments)]
fn finalize_artifact_partitions_inner(
    source_plan_path: &Path,
    expected_source_plan_sha256: &str,
    dictionary_path: &Path,
    expected_dictionary_sha256: &str,
    partition_manifest_paths: &[PathBuf],
    output_root: &Path,
    verify_all_artifacts: bool,
) -> Result<PathBuf, DistributedArtifactError> {
    verify_file(source_plan_path, expected_source_plan_sha256)?;
    verify_file(dictionary_path, expected_dictionary_sha256)?;
    if output_root.exists() {
        return Err(DistributedArtifactError::ImmutableConflict(
            output_root.to_owned(),
        ));
    }
    let plan: SourcePlan = serde_json::from_slice(&fs::read(source_plan_path)?)?;
    if partition_manifest_paths.len()
        != usize::try_from(plan.logical_partition_count).map_err(|_| {
            DistributedArtifactError::Contract("partition count overflow".to_owned())
        })?
    {
        return Err(DistributedArtifactError::Contract(
            "artifact partition barrier is incomplete".to_owned(),
        ));
    }
    let mut observed = BTreeSet::new();
    let mut manifests = Vec::with_capacity(partition_manifest_paths.len());
    let mut locator_inputs = Vec::with_capacity(partition_manifest_paths.len());
    for path in partition_manifest_paths {
        let manifest: ArtifactPartitionManifest = serde_json::from_slice(&fs::read(path)?)?;
        validate_partition_manifest(
            &manifest,
            path,
            &plan,
            expected_source_plan_sha256,
            expected_dictionary_sha256,
            verify_all_artifacts,
        )?;
        if !observed.insert(manifest.partition_index) {
            return Err(DistributedArtifactError::Contract(
                "duplicate artifact partition manifest".to_owned(),
            ));
        }
        let root = path.parent().ok_or_else(|| {
            DistributedArtifactError::Contract("partition manifest has no parent".to_owned())
        })?;
        locator_inputs.push(root.join("locator-run.tsv"));
        manifests.push((path.clone(), manifest));
    }
    let expected = (0..plan.logical_partition_count).collect::<BTreeSet<_>>();
    if observed != expected {
        return Err(DistributedArtifactError::Contract(
            "artifact partitions do not exactly cover the source plan".to_owned(),
        ));
    }
    manifests.sort_unstable_by_key(|(_, manifest)| manifest.partition_index);
    fs::create_dir_all(output_root)?;
    let locator_path = output_root.join("locator.tsv");
    let locator_record_count = merge_sorted_unique(&locator_inputs, &locator_path)?;
    let fact_count = checked_sum(manifests.iter().map(|(_, value)| value.fact_count), "fact")?;
    let semantic_row_count = checked_sum(
        manifests.iter().map(|(_, value)| value.semantic_row_count),
        "semantic row",
    )?;
    let payload_row_count = checked_sum(
        manifests.iter().map(|(_, value)| value.payload_row_count),
        "payload row",
    )?;
    let expected_locator = checked_sum(
        manifests
            .iter()
            .map(|(_, value)| value.locator_record_count),
        "locator",
    )?;
    if fact_count != plan.fact_count || locator_record_count != expected_locator {
        return Err(DistributedArtifactError::Contract(
            "artifact aggregate counts differ from worker manifests".to_owned(),
        ));
    }
    let mut semantic_hasher = Sha256::new();
    semantic_hasher.update(b"ngkg-distributed-artifacts-v1\0");
    for (_, manifest) in &manifests {
        semantic_hasher.update(manifest.partition_index.to_be_bytes());
        semantic_hasher.update(decode_sha256(&manifest.semantic_content_sha256)?);
    }
    let partitions = manifests
        .iter()
        .map(|(path, manifest)| {
            Ok(ArtifactPartitionReference {
                partition_index: manifest.partition_index,
                partition_id: manifest.partition_id.clone(),
                manifest_path: path.to_string_lossy().into_owned(),
                manifest_sha256: sha256_path(path)?,
                fact_count: manifest.fact_count,
                semantic_row_count: manifest.semantic_row_count,
                payload_row_count: manifest.payload_row_count,
            })
        })
        .collect::<Result<Vec<_>, DistributedArtifactError>>()?;
    let root = DistributedArtifactRootManifest {
        format_version: DISTRIBUTED_ARTIFACT_FORMAT_VERSION,
        dataset_id: plan.dataset_id,
        snapshot_id: plan.snapshot_id,
        source_plan_sha256: expected_source_plan_sha256.to_owned(),
        dictionary_sha256: expected_dictionary_sha256.to_owned(),
        partitions,
        locator_path: "locator.tsv".to_owned(),
        locator_sha256: sha256_path(&locator_path)?,
        fact_count,
        semantic_row_count,
        payload_row_count,
        locator_record_count,
        semantic_content_sha256: hex::encode(semantic_hasher.finalize()),
    };
    let root_path = output_root.join("distributed-artifact-root.json");
    write_json_new(&root_path, &root)?;
    sync_directory(output_root)?;
    Ok(root_path)
}

/// Compare logical artifact content while deliberately ignoring local manifest paths.
pub fn compare_artifact_roots(
    baseline_path: &Path,
    candidate_path: &Path,
    report_path: &Path,
) -> Result<ArtifactEquivalenceReport, DistributedArtifactError> {
    let baseline: DistributedArtifactRootManifest =
        serde_json::from_slice(&fs::read(baseline_path)?)?;
    let candidate: DistributedArtifactRootManifest =
        serde_json::from_slice(&fs::read(candidate_path)?)?;
    validate_artifact_root(&baseline, baseline_path)?;
    validate_artifact_root(&candidate, candidate_path)?;
    let mut mismatches = Vec::new();
    macro_rules! compare_field {
        ($field:ident) => {
            if baseline.$field != candidate.$field {
                mismatches.push(stringify!($field).to_owned());
            }
        };
    }
    compare_field!(format_version);
    compare_field!(dataset_id);
    compare_field!(snapshot_id);
    compare_field!(source_plan_sha256);
    compare_field!(dictionary_sha256);
    compare_field!(locator_sha256);
    compare_field!(fact_count);
    compare_field!(semantic_row_count);
    compare_field!(payload_row_count);
    compare_field!(locator_record_count);
    compare_field!(semantic_content_sha256);
    let baseline_partitions = baseline
        .partitions
        .iter()
        .map(|value| {
            (
                value.partition_index,
                &value.partition_id,
                &value.manifest_sha256,
                value.fact_count,
                value.semantic_row_count,
                value.payload_row_count,
            )
        })
        .collect::<Vec<_>>();
    let candidate_partitions = candidate
        .partitions
        .iter()
        .map(|value| {
            (
                value.partition_index,
                &value.partition_id,
                &value.manifest_sha256,
                value.fact_count,
                value.semantic_row_count,
                value.payload_row_count,
            )
        })
        .collect::<Vec<_>>();
    if baseline_partitions != candidate_partitions {
        mismatches.push("partitions".to_owned());
    }
    let report = ArtifactEquivalenceReport {
        format_version: DISTRIBUTED_ARTIFACT_FORMAT_VERSION,
        baseline_root_sha256: sha256_path(baseline_path)?,
        candidate_root_sha256: sha256_path(candidate_path)?,
        equivalent: mismatches.is_empty(),
        mismatches,
    };
    write_json_new(report_path, &report)?;
    Ok(report)
}

fn validate_artifact_root(
    root: &DistributedArtifactRootManifest,
    root_path: &Path,
) -> Result<(), DistributedArtifactError> {
    if root.format_version != DISTRIBUTED_ARTIFACT_FORMAT_VERSION || root.partitions.is_empty() {
        return Err(DistributedArtifactError::Contract(
            "distributed artifact root header is invalid".to_owned(),
        ));
    }
    for value in [
        &root.source_plan_sha256,
        &root.dictionary_sha256,
        &root.locator_sha256,
        &root.semantic_content_sha256,
    ] {
        require_sha256(value)?;
    }
    let root_directory = root_path.parent().ok_or_else(|| {
        DistributedArtifactError::Contract("artifact root manifest has no parent".to_owned())
    })?;
    verify_file(
        &safe_join(root_directory, &root.locator_path)?,
        &root.locator_sha256,
    )?;
    let mut fact_count = 0_u64;
    let mut semantic_row_count = 0_u64;
    let mut payload_row_count = 0_u64;
    for (expected_index, reference) in root.partitions.iter().enumerate() {
        if usize::try_from(reference.partition_index).ok() != Some(expected_index) {
            return Err(DistributedArtifactError::Contract(
                "artifact root partition indexes are not dense and sorted".to_owned(),
            ));
        }
        require_sha256(&reference.manifest_sha256)?;
        let manifest_path = PathBuf::from(&reference.manifest_path);
        verify_file(&manifest_path, &reference.manifest_sha256)?;
        let manifest: ArtifactPartitionManifest =
            serde_json::from_slice(&fs::read(&manifest_path)?)?;
        if manifest.partition_index != reference.partition_index
            || manifest.partition_id != reference.partition_id
            || manifest.fact_count != reference.fact_count
            || manifest.semantic_row_count != reference.semantic_row_count
            || manifest.payload_row_count != reference.payload_row_count
            || manifest.dataset_id != root.dataset_id
            || manifest.snapshot_id != root.snapshot_id
            || manifest.source_plan_sha256 != root.source_plan_sha256
            || manifest.dictionary_sha256 != root.dictionary_sha256
        {
            return Err(DistributedArtifactError::Contract(
                "artifact root partition reference differs from its manifest".to_owned(),
            ));
        }
        fact_count = fact_count
            .checked_add(reference.fact_count)
            .ok_or_else(|| {
                DistributedArtifactError::Contract("artifact root fact count overflow".to_owned())
            })?;
        semantic_row_count = semantic_row_count
            .checked_add(reference.semantic_row_count)
            .ok_or_else(|| {
                DistributedArtifactError::Contract(
                    "artifact root semantic row count overflow".to_owned(),
                )
            })?;
        payload_row_count = payload_row_count
            .checked_add(reference.payload_row_count)
            .ok_or_else(|| {
                DistributedArtifactError::Contract(
                    "artifact root payload row count overflow".to_owned(),
                )
            })?;
    }
    if fact_count != root.fact_count
        || semantic_row_count != root.semantic_row_count
        || payload_row_count != root.payload_row_count
        || payload_row_count != root.locator_record_count
    {
        return Err(DistributedArtifactError::Contract(
            "artifact root aggregate counts are invalid".to_owned(),
        ));
    }
    Ok(())
}

fn write_spine(
    path: &Path,
    facts: &[NormalizedFact],
    dictionary: &BTreeMap<String, u64>,
    source_guid: Uuid,
    snapshot_id: Uuid,
    row_group_rows: usize,
) -> Result<u64, DistributedArtifactError> {
    let facts = facts
        .iter()
        .filter(|fact| fact.treatment != Treatment::Payload)
        .collect::<Vec<_>>();
    let mut fact_ids = FixedSizeBinaryBuilder::new(16);
    let mut fact_hashes = FixedSizeBinaryBuilder::new(32);
    let mut subject_guids = FixedSizeBinaryBuilder::new(16);
    let mut source_guids = FixedSizeBinaryBuilder::new(16);
    let mut snapshot_ids = FixedSizeBinaryBuilder::new(16);
    let mut subjects = Vec::with_capacity(facts.len());
    let mut predicates = Vec::with_capacity(facts.len());
    let mut objects = Vec::with_capacity(facts.len());
    let mut object_kinds = Vec::with_capacity(facts.len());
    let mut graphs = Vec::with_capacity(facts.len());
    let mut treatments = Vec::with_capacity(facts.len());
    let mut reasoning = Vec::with_capacity(facts.len());
    let mut queryable = Vec::with_capacity(facts.len());
    for fact in &facts {
        fact_ids.append_value(fact.fact_id)?;
        fact_hashes.append_value(fact.fact_hash)?;
        subject_guids.append_value(fact.subject_guid.as_bytes())?;
        source_guids.append_value(source_guid.as_bytes())?;
        snapshot_ids.append_value(snapshot_id.as_bytes())?;
        subjects.push(term_id(
            dictionary,
            fact.subject_term_kind.dictionary_tag(),
            &fact.subject_iri,
        )?);
        predicates.push(term_id(dictionary, 'I', &fact.predicate_iri)?);
        graphs.push(term_id(dictionary, 'I', &fact.graph_iri)?);
        treatments.push(fact.treatment.code());
        reasoning.push(fact.participates_in_reasoning);
        queryable.push(fact.queryable_as_rdf);
        match &fact.object {
            NormalizedObject::Entity { iri, term_kind, .. } => {
                object_kinds.push(1_u8);
                objects.push(term_id(dictionary, term_kind.dictionary_tag(), iri)?);
            }
            NormalizedObject::Literal { ntriples, .. } => {
                object_kinds.push(2_u8);
                objects.push(term_id(dictionary, 'L', ntriples)?);
            }
        }
    }
    let schema = Arc::new(Schema::new(vec![
        Field::new("fact_id128", DataType::FixedSizeBinary(16), false),
        Field::new("fact_hash256", DataType::FixedSizeBinary(32), false),
        Field::new("subject_id64", DataType::UInt64, false),
        Field::new("predicate_id64", DataType::UInt64, false),
        Field::new("object_kind", DataType::UInt8, false),
        Field::new("object_term_id64", DataType::UInt64, false),
        Field::new("graph_id64", DataType::UInt64, false),
        Field::new("subject_guid128", DataType::FixedSizeBinary(16), false),
        Field::new("source_guid128", DataType::FixedSizeBinary(16), false),
        Field::new("treatment", DataType::UInt8, false),
        Field::new("participates_in_reasoning", DataType::Boolean, false),
        Field::new("queryable_as_rdf", DataType::Boolean, false),
        Field::new("snapshot_id128", DataType::FixedSizeBinary(16), false),
    ]));
    let columns: Vec<ArrayRef> = vec![
        Arc::new(fact_ids.finish()),
        Arc::new(fact_hashes.finish()),
        Arc::new(UInt64Array::from(subjects)),
        Arc::new(UInt64Array::from(predicates)),
        Arc::new(UInt8Array::from(object_kinds)),
        Arc::new(UInt64Array::from(objects)),
        Arc::new(UInt64Array::from(graphs)),
        Arc::new(subject_guids.finish()),
        Arc::new(source_guids.finish()),
        Arc::new(UInt8Array::from(treatments)),
        Arc::new(BooleanArray::from(reasoning)),
        Arc::new(BooleanArray::from(queryable)),
        Arc::new(snapshot_ids.finish()),
    ];
    write_batch(path, RecordBatch::try_new(schema, columns)?, row_group_rows)?;
    u64::try_from(facts.len())
        .map_err(|_| DistributedArtifactError::Contract("semantic row count overflow".to_owned()))
}

#[allow(clippy::too_many_arguments)]
fn write_payload_and_locator(
    payload_path: &Path,
    locator_path: &Path,
    facts: &[NormalizedFact],
    dictionary: &BTreeMap<String, u64>,
    partition_index: u32,
    source_guid: Uuid,
    snapshot_id: Uuid,
    row_group_rows: usize,
) -> Result<(u64, u64), DistributedArtifactError> {
    let facts = facts
        .iter()
        .filter(|fact| fact.treatment == Treatment::Payload)
        .collect::<Vec<_>>();
    let mut fact_ids = FixedSizeBinaryBuilder::new(16);
    let mut fact_hashes = FixedSizeBinaryBuilder::new(32);
    let mut subject_guids = FixedSizeBinaryBuilder::new(16);
    let mut source_guids = FixedSizeBinaryBuilder::new(16);
    let mut snapshot_ids = FixedSizeBinaryBuilder::new(16);
    let mut subject_terms = Vec::with_capacity(facts.len());
    let mut subject_resource_kinds = Vec::with_capacity(facts.len());
    let mut predicate_ids = Vec::with_capacity(facts.len());
    let mut graph_ids = Vec::with_capacity(facts.len());
    let mut graph_scopes = Vec::with_capacity(facts.len());
    let mut lexical = Vec::with_capacity(facts.len());
    let mut datatypes = Vec::with_capacity(facts.len());
    let mut languages = Vec::with_capacity(facts.len());
    let mut locators = Vec::with_capacity(facts.len());
    for (row, fact) in facts.iter().enumerate() {
        let NormalizedObject::Literal {
            lexical_value,
            datatype_iri,
            language,
            ..
        } = &fact.object
        else {
            return Err(DistributedArtifactError::Contract(
                "payload treatment requires a literal object".to_owned(),
            ));
        };
        fact_ids.append_value(fact.fact_id)?;
        fact_hashes.append_value(fact.fact_hash)?;
        subject_guids.append_value(fact.subject_guid.as_bytes())?;
        source_guids.append_value(source_guid.as_bytes())?;
        snapshot_ids.append_value(snapshot_id.as_bytes())?;
        subject_terms.push(public_resource_lexical(
            fact.subject_term_kind,
            &fact.subject_iri,
        ));
        subject_resource_kinds.push(fact.subject_term_kind.code());
        let predicate = term_id(dictionary, 'I', &fact.predicate_iri)?;
        let graph = term_id(dictionary, 'I', &fact.graph_iri)?;
        predicate_ids.push(predicate);
        graph_ids.push(graph);
        graph_scopes.push(fact.graph_scope.code());
        lexical.push(lexical_value.as_str());
        datatypes.push(datatype_iri.as_str());
        languages.push(language.as_deref());
        let row_group = row / row_group_rows;
        let row_in_group = row % row_group_rows;
        locators.push(format!(
            "{}\t{partition_index:05}\t{row_group:010}\t{row_in_group:010}\t{graph:020}\t{predicate:020}",
            hex::encode(fact.subject_guid.as_bytes()),
        ));
    }
    locators.sort_unstable();
    if locators.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(DistributedArtifactError::Contract(
            "duplicate physical locator row".to_owned(),
        ));
    }
    write_lines(locator_path, locators.iter().map(String::as_str))?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("fact_id128", DataType::FixedSizeBinary(16), false),
        Field::new("fact_hash256", DataType::FixedSizeBinary(32), false),
        Field::new("subject_guid128", DataType::FixedSizeBinary(16), false),
        Field::new("source_guid128", DataType::FixedSizeBinary(16), false),
        Field::new("subject_term", DataType::Utf8, false),
        Field::new("subject_resource_kind", DataType::UInt8, false),
        Field::new("predicate_id64", DataType::UInt64, false),
        Field::new("graph_id64", DataType::UInt64, false),
        Field::new("graph_scope", DataType::UInt8, false),
        Field::new("lexical_value", DataType::Utf8, false),
        Field::new("datatype_iri", DataType::Utf8, false),
        Field::new("language", DataType::Utf8, true),
        Field::new("snapshot_id128", DataType::FixedSizeBinary(16), false),
    ]));
    let columns: Vec<ArrayRef> = vec![
        Arc::new(fact_ids.finish()),
        Arc::new(fact_hashes.finish()),
        Arc::new(subject_guids.finish()),
        Arc::new(source_guids.finish()),
        Arc::new(StringArray::from(subject_terms)),
        Arc::new(UInt8Array::from(subject_resource_kinds)),
        Arc::new(UInt64Array::from(predicate_ids)),
        Arc::new(UInt64Array::from(graph_ids)),
        Arc::new(UInt8Array::from(graph_scopes)),
        Arc::new(StringArray::from(lexical)),
        Arc::new(StringArray::from(datatypes)),
        Arc::new(StringArray::from(languages)),
        Arc::new(snapshot_ids.finish()),
    ];
    write_batch(
        payload_path,
        RecordBatch::try_new(schema, columns)?,
        row_group_rows,
    )?;
    let count = u64::try_from(facts.len())
        .map_err(|_| DistributedArtifactError::Contract("payload row count overflow".to_owned()))?;
    Ok((count, count))
}

fn write_semantic_exports(
    facts: &[NormalizedFact],
    queryable_path: &Path,
    reasoning_path: &Path,
) -> Result<(u64, u64), DistributedArtifactError> {
    let mut queryable = BufWriter::new(create_new(queryable_path)?);
    let mut reasoning = BufWriter::new(create_new(reasoning_path)?);
    let mut queryable_count = 0_u64;
    let mut reasoning_count = 0_u64;
    for fact in facts {
        if fact.queryable_as_rdf || fact.treatment == Treatment::Core {
            queryable.write_all(nquad_line(fact).as_bytes())?;
            queryable_count = queryable_count.checked_add(1).ok_or_else(|| {
                DistributedArtifactError::Contract("queryable count overflow".to_owned())
            })?;
        }
        if fact.treatment == Treatment::Core && fact.participates_in_reasoning {
            reasoning.write_all(ntriple_line(fact).as_bytes())?;
            reasoning_count = reasoning_count.checked_add(1).ok_or_else(|| {
                DistributedArtifactError::Contract("reasoning count overflow".to_owned())
            })?;
        }
    }
    queryable.flush()?;
    reasoning.flush()?;
    queryable.get_ref().sync_all()?;
    reasoning.get_ref().sync_all()?;
    Ok((queryable_count, reasoning_count))
}

fn write_batch(
    path: &Path,
    batch: RecordBatch,
    row_group_rows: usize,
) -> Result<(), DistributedArtifactError> {
    let properties = WriterProperties::builder()
        .set_max_row_group_size(row_group_rows)
        .build();
    let mut writer = ArrowWriter::try_new(create_new(path)?, batch.schema(), Some(properties))?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}

fn read_dictionary(path: &Path) -> Result<BTreeMap<String, u64>, DistributedArtifactError> {
    let mut dictionary = BTreeMap::new();
    let mut expected_id = 0_u64;
    for line in BufReader::new(File::open(path)?).lines() {
        let line = line?;
        let (id, term) = line.split_once('\t').ok_or_else(|| {
            DistributedArtifactError::Contract("dictionary row has no ID separator".to_owned())
        })?;
        if id.parse::<u64>().ok() != Some(expected_id)
            || !(term.starts_with("I\t") || term.starts_with("L\t"))
            || dictionary.insert(term.to_owned(), expected_id).is_some()
        {
            return Err(DistributedArtifactError::Contract(
                "dictionary is not canonical, dense, or unique".to_owned(),
            ));
        }
        expected_id = expected_id.checked_add(1).ok_or_else(|| {
            DistributedArtifactError::Contract("dictionary ID overflow".to_owned())
        })?;
    }
    Ok(dictionary)
}

fn term_id(
    dictionary: &BTreeMap<String, u64>,
    kind: char,
    term: &str,
) -> Result<u64, DistributedArtifactError> {
    dictionary
        .get(&format!("{kind}\t{term}"))
        .copied()
        .ok_or_else(|| {
            DistributedArtifactError::Contract(format!(
                "global dictionary is missing {kind} term {term}"
            ))
        })
}

fn semantic_hash(facts: &[NormalizedFact]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"ngkg-artifact-partition-v1\0");
    for fact in facts {
        hasher.update(fact.fact_hash);
        hasher.update([fact.treatment.code()]);
        hasher.update([u8::from(fact.participates_in_reasoning)]);
        hasher.update([u8::from(fact.queryable_as_rdf)]);
    }
    hex::encode(hasher.finalize())
}

fn validate_partition_manifest(
    manifest: &ArtifactPartitionManifest,
    manifest_path: &Path,
    plan: &SourcePlan,
    source_plan_sha256: &str,
    dictionary_sha256: &str,
    verify_all_artifacts: bool,
) -> Result<(), DistributedArtifactError> {
    let shard = plan
        .shards
        .get(usize::try_from(manifest.partition_index).map_err(|_| {
            DistributedArtifactError::Contract("partition index overflow".to_owned())
        })?)
        .filter(|value| value.partition_index == manifest.partition_index)
        .ok_or_else(|| {
            DistributedArtifactError::Contract("manifest partition is unplanned".to_owned())
        })?;
    if manifest.format_version != DISTRIBUTED_ARTIFACT_FORMAT_VERSION
        || manifest.dataset_id != plan.dataset_id
        || manifest.snapshot_id != plan.snapshot_id
        || manifest.source_plan_sha256 != source_plan_sha256
        || manifest.dictionary_sha256 != dictionary_sha256
        || manifest.partition_id != shard.partition_id
        || manifest.fact_count != shard.fact_count
        || manifest
            .semantic_row_count
            .saturating_add(manifest.payload_row_count)
            != manifest.fact_count
        || manifest.locator_record_count != manifest.payload_row_count
    {
        return Err(DistributedArtifactError::Contract(
            "artifact partition manifest header or counts are invalid".to_owned(),
        ));
    }
    require_sha256(&manifest.semantic_content_sha256)?;
    let expected_names = BTreeSet::from([
        "semantic-spine.parquet",
        "payload.parquet",
        "queryable.nq",
        "reasoner-core.nt",
        "locator-run.tsv",
    ]);
    let root = manifest_path.parent().ok_or_else(|| {
        DistributedArtifactError::Contract("partition manifest has no parent".to_owned())
    })?;
    let mut observed_names = BTreeSet::new();
    for artifact in &manifest.artifacts {
        if !expected_names.contains(artifact.relative_path.as_str())
            || !observed_names.insert(artifact.relative_path.as_str())
        {
            return Err(DistributedArtifactError::Contract(
                "artifact manifest contains an unexpected or duplicate path".to_owned(),
            ));
        }
        if verify_all_artifacts || artifact.relative_path == "locator-run.tsv" {
            let path = safe_join(root, &artifact.relative_path)?;
            let metadata = fs::metadata(&path)?;
            if metadata.len() != artifact.bytes {
                return Err(DistributedArtifactError::Contract(
                    "artifact byte count mismatch".to_owned(),
                ));
            }
            verify_file(&path, &artifact.sha256)?;
        }
    }
    if observed_names != expected_names {
        return Err(DistributedArtifactError::Contract(
            "artifact partition manifest is incomplete".to_owned(),
        ));
    }
    Ok(())
}

fn artifact(root: &Path, relative_path: &str) -> Result<ShardArtifact, DistributedArtifactError> {
    let path = root.join(relative_path);
    Ok(ShardArtifact {
        relative_path: relative_path.to_owned(),
        sha256: sha256_path(&path)?,
        bytes: fs::metadata(path)?.len(),
    })
}

fn merge_sorted_unique(inputs: &[PathBuf], output: &Path) -> Result<u64, DistributedArtifactError> {
    let mut readers = inputs
        .iter()
        .map(|path| File::open(path).map(BufReader::new))
        .collect::<Result<Vec<_>, _>>()?;
    let mut heap = BinaryHeap::<Reverse<(String, usize)>>::new();
    let mut previous_by_reader = vec![None::<String>; readers.len()];
    for (index, reader) in readers.iter_mut().enumerate() {
        if let Some(line) = next_sorted_line(reader, &mut previous_by_reader[index])? {
            heap.push(Reverse((line, index)));
        }
    }
    let mut writer = BufWriter::new(create_new(output)?);
    let mut previous = None::<String>;
    let mut count = 0_u64;
    while let Some(Reverse((line, index))) = heap.pop() {
        if previous.as_ref() == Some(&line) {
            return Err(DistributedArtifactError::Contract(
                "duplicate physical locator row across partitions".to_owned(),
            ));
        }
        writer.write_all(line.as_bytes())?;
        writer.write_all(b"\n")?;
        previous = Some(line);
        count = count.checked_add(1).ok_or_else(|| {
            DistributedArtifactError::Contract("locator count overflow".to_owned())
        })?;
        if let Some(next) = next_sorted_line(&mut readers[index], &mut previous_by_reader[index])? {
            heap.push(Reverse((next, index)));
        }
    }
    writer.flush()?;
    writer.get_ref().sync_all()?;
    Ok(count)
}

fn next_sorted_line<R: BufRead>(
    reader: &mut R,
    previous: &mut Option<String>,
) -> Result<Option<String>, DistributedArtifactError> {
    let mut line = String::new();
    if reader.read_line(&mut line)? == 0 {
        return Ok(None);
    }
    while line.ends_with('\n') || line.ends_with('\r') {
        line.pop();
    }
    if line.is_empty() || previous.as_ref().is_some_and(|value| value >= &line) {
        return Err(DistributedArtifactError::Contract(
            "locator run is empty, unsorted, or contains duplicates".to_owned(),
        ));
    }
    *previous = Some(line.clone());
    Ok(Some(line))
}

fn checked_sum(
    mut values: impl Iterator<Item = u64>,
    label: &str,
) -> Result<u64, DistributedArtifactError> {
    values.try_fold(0_u64, |total, value| {
        total
            .checked_add(value)
            .ok_or_else(|| DistributedArtifactError::Contract(format!("{label} count overflow")))
    })
}

fn write_lines<'a>(
    path: &Path,
    lines: impl IntoIterator<Item = &'a str>,
) -> Result<(), DistributedArtifactError> {
    let mut writer = BufWriter::new(create_new(path)?);
    for line in lines {
        if line.contains('\n') || line.contains('\r') {
            return Err(DistributedArtifactError::Contract(
                "line output contains an embedded newline".to_owned(),
            ));
        }
        writer.write_all(line.as_bytes())?;
        writer.write_all(b"\n")?;
    }
    writer.flush()?;
    writer.get_ref().sync_all()?;
    Ok(())
}

fn write_json_new<T: Serialize>(path: &Path, value: &T) -> Result<(), DistributedArtifactError> {
    let bytes = serde_json::to_vec_pretty(value)?;
    let mut file = create_new(path)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    Ok(())
}

fn create_new(path: &Path) -> Result<File, DistributedArtifactError> {
    OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)
        .map_err(DistributedArtifactError::Io)
}

fn safe_join(root: &Path, relative: &str) -> Result<PathBuf, DistributedArtifactError> {
    let path = Path::new(relative);
    if path.is_absolute()
        || path
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(DistributedArtifactError::Contract(
            "artifact path is not a safe relative path".to_owned(),
        ));
    }
    Ok(root.join(path))
}

fn verify_file(path: &Path, expected: &str) -> Result<(), DistributedArtifactError> {
    require_sha256(expected)?;
    if sha256_path(path)? != expected {
        return Err(DistributedArtifactError::Contract(format!(
            "checksum mismatch for {}",
            path.display()
        )));
    }
    Ok(())
}

fn require_sha256(value: &str) -> Result<(), DistributedArtifactError> {
    if value.len() != 64
        || value
            .bytes()
            .any(|byte| !byte.is_ascii_hexdigit() || byte.is_ascii_uppercase())
    {
        return Err(DistributedArtifactError::Contract(
            "SHA-256 must be 64 lowercase hexadecimal characters".to_owned(),
        ));
    }
    Ok(())
}

fn decode_sha256(value: &str) -> Result<[u8; 32], DistributedArtifactError> {
    require_sha256(value)?;
    let bytes = hex::decode(value).map_err(|_| {
        DistributedArtifactError::Contract("invalid SHA-256 hexadecimal".to_owned())
    })?;
    bytes
        .try_into()
        .map_err(|_| DistributedArtifactError::Contract("invalid SHA-256 byte length".to_owned()))
}

fn sync_directory(path: &Path) -> Result<(), DistributedArtifactError> {
    File::open(path)?.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{fs, path::Path};

    use uuid::Uuid;

    use super::{merge_sorted_unique, read_dictionary};

    #[test]
    fn dictionary_requires_dense_unique_ids() {
        let root =
            std::env::temp_dir().join(format!("ngkg-artifact-dictionary-{}", Uuid::new_v4()));
        assert!(fs::create_dir(&root).is_ok());
        let path = root.join("dictionary.tsv");
        assert!(fs::write(&path, "0\tI\turn:a\n2\tI\turn:b\n").is_ok());
        assert!(read_dictionary(Path::new(&path)).is_err());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn locator_merge_rejects_duplicate_physical_rows() {
        let root = std::env::temp_dir().join(format!("ngkg-artifact-locator-{}", Uuid::new_v4()));
        assert!(fs::create_dir(&root).is_ok());
        let left = root.join("left.tsv");
        let right = root.join("right.tsv");
        let output = root.join("output.tsv");
        let row = "00000000000000000000000000000001\t00000\t0000000000\t0000000000\t00000000000000000001\t00000000000000000002\n";
        assert!(fs::write(&left, row).is_ok());
        assert!(fs::write(&right, row).is_ok());
        assert!(merge_sorted_unique(&[left, right], &output).is_err());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn locator_merge_rejects_unsorted_worker_run() {
        let root = std::env::temp_dir().join(format!("ngkg-artifact-unsorted-{}", Uuid::new_v4()));
        assert!(fs::create_dir(&root).is_ok());
        let input = root.join("input.tsv");
        let output = root.join("output.tsv");
        assert!(fs::write(&input, "b\na\n").is_ok());
        assert!(merge_sorted_unique(&[input], &output).is_err());
        let _ = fs::remove_dir_all(root);
    }
}
