//! Deterministic, content-addressed source partitioning and reduction.
//!
//! The module deliberately separates *logical partitions* from Kubernetes pods.
//! A retry or a different pod count may change scheduling, but it cannot change
//! the partition IDs, normalized facts, reducer inputs, or final logical source.

use std::{
    cmp::Reverse,
    collections::{BTreeMap, BTreeSet, BinaryHeap},
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, BufWriter, Read, Write},
    path::{Component, Path, PathBuf},
};

use ngkg_reference::{
    NormalizedFact, NormalizedObject, ProjectionPolicy, nquad_line, parse_nquads, parse_trig,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use uuid::Uuid;

/// Current source-partition and reducer contract version.
pub const DISTRIBUTED_BUILD_FORMAT_VERSION: u32 = 1;

/// Immutable inputs that determine all safe-scan output bytes.
#[derive(Clone, Debug)]
pub struct SafeScanRequest<'a> {
    /// Catalog dataset identity.
    pub dataset_id: Uuid,
    /// Immutable target snapshot.
    pub snapshot_id: Uuid,
    /// Namespace used for deterministic entity GUIDs.
    pub dataset_namespace: Uuid,
    /// Original source identity used by FactID construction.
    pub source_guid: Uuid,
    /// Original source version used by FactID construction.
    pub source_snapshot: &'a str,
    /// Lowercase SHA-256 of the original TriG bytes.
    pub source_sha256: &'a str,
    /// Lowercase SHA-256 of canonical projection-policy JSON.
    pub projection_policy_sha256: &'a str,
    /// Validated exhaustive projection policy.
    pub projection_policy: &'a ProjectionPolicy,
    /// Stable logical partition count for this layout profile.
    pub logical_partition_count: u32,
    /// Maximum accepted source quads.
    pub max_quads: u64,
}

/// One canonical N-Quads shard and its exact logical coverage.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct SourceShard {
    /// Stable ordinal in `[0, logicalPartitionCount)`.
    pub partition_index: u32,
    /// Content-independent work identity derived from immutable request context.
    pub partition_id: String,
    /// Relative path below the plan directory.
    pub relative_path: String,
    /// SHA-256 of canonical N-Quads bytes.
    pub sha256: String,
    /// Stored bytes.
    pub bytes: u64,
    /// Number of unique logical RDF facts in this shard.
    pub fact_count: u64,
    /// Exact named-graph counts for this shard.
    pub graph_counts: BTreeMap<String, u64>,
    /// Minimum full FactID fingerprint, when non-empty.
    pub min_fact_hash: Option<String>,
    /// Maximum full FactID fingerprint, when non-empty.
    pub max_fact_hash: Option<String>,
}

/// Bill of materials for syntax-safe distributed input.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct SourcePlan {
    /// Contract version.
    pub format_version: u32,
    /// Dataset identity.
    pub dataset_id: Uuid,
    /// Target snapshot.
    pub snapshot_id: Uuid,
    /// Original TriG SHA-256.
    pub source_sha256: String,
    /// Projection-policy SHA-256.
    pub projection_policy_sha256: String,
    /// Stable layout label. Changing this requires an explicit equivalence test.
    pub layout_profile: String,
    /// Stable logical bucket count; unrelated to current pod parallelism.
    pub logical_partition_count: u32,
    /// Unique logical fact total.
    pub fact_count: u64,
    /// Exact graph counts across all shards.
    pub graph_counts: BTreeMap<String, u64>,
    /// Every partition, including deterministic empty partitions.
    pub shards: Vec<SourceShard>,
}

/// Immutable evidence emitted by one projection completion index.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ProjectionRunManifest {
    /// Contract version.
    pub format_version: u32,
    /// Dataset identity.
    pub dataset_id: Uuid,
    /// Snapshot identity.
    pub snapshot_id: Uuid,
    /// Source-plan SHA-256.
    pub source_plan_sha256: String,
    /// Source logical partition ordinal.
    pub partition_index: u32,
    /// Stable partition ID.
    pub partition_id: String,
    /// Verified canonical fact-run path.
    pub fact_run_path: String,
    /// Fact-run SHA-256.
    pub fact_run_sha256: String,
    /// Verified sorted term-run path.
    pub term_run_path: String,
    /// Term-run SHA-256.
    pub term_run_sha256: String,
    /// Exact fact count.
    pub fact_count: u64,
    /// Exact unique term count contributed by this partition.
    pub term_count: u64,
    /// Exact graph counts.
    pub graph_counts: BTreeMap<String, u64>,
}

/// Immutable output of one deterministic reducer range.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ReducerRunManifest {
    /// Contract version.
    pub format_version: u32,
    /// Dataset identity.
    pub dataset_id: Uuid,
    /// Snapshot identity.
    pub snapshot_id: Uuid,
    /// Source-plan SHA-256.
    pub source_plan_sha256: String,
    /// Reducer ordinal.
    pub reducer_index: u32,
    /// Total reducers in this execution layout.
    pub reducer_count: u32,
    /// Sorted logical source partitions owned by this reducer.
    pub partition_indexes: Vec<u32>,
    /// Canonical merged N-Quads run.
    pub fact_run_path: String,
    /// Fact-run SHA-256.
    pub fact_run_sha256: String,
    /// Sorted deduplicated dictionary term run.
    pub term_run_path: String,
    /// Term-run SHA-256.
    pub term_run_sha256: String,
    /// Exact fact count.
    pub fact_count: u64,
    /// Exact reducer-local unique term count.
    pub term_count: u64,
}

/// Global deterministic reducer result consumed by the existing exact compiler.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DistributedRootManifest {
    /// Contract version.
    pub format_version: u32,
    /// Dataset identity.
    pub dataset_id: Uuid,
    /// Snapshot identity.
    pub snapshot_id: Uuid,
    /// Source-plan SHA-256.
    pub source_plan_sha256: String,
    /// Canonical globally merged N-Quads input.
    pub canonical_source_path: String,
    /// Canonical source SHA-256.
    pub canonical_source_sha256: String,
    /// Globally sorted dictionary with stable dense IDs equal to line ordinals.
    pub dictionary_path: String,
    /// Dictionary SHA-256.
    pub dictionary_sha256: String,
    /// Unique fact total.
    pub fact_count: u64,
    /// Unique dictionary term total.
    pub term_count: u64,
    /// Reducer count used to build this physical layout.
    pub reducer_count: u32,
    /// Hash of logical facts independent of reducer count and file layout.
    pub semantic_content_sha256: String,
}

/// Semantic comparison report for the mandatory one-node/N-node gate.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct BuildEquivalenceReport {
    /// Contract version.
    pub format_version: u32,
    /// Baseline root manifest hash.
    pub baseline_root_sha256: String,
    /// Candidate root manifest hash.
    pub candidate_root_sha256: String,
    /// True only when logical fact and dictionary bytes are identical.
    pub equivalent: bool,
    /// Fields compared by the gate.
    pub compared: Vec<String>,
    /// Human-readable mismatch details, empty on success.
    pub mismatches: Vec<String>,
}

/// Distributed compilation rejects ambiguity instead of repairing it.
#[derive(Debug, Error)]
pub enum DistributedBuildError {
    /// Local artifact I/O failed.
    #[error("distributed build I/O failed: {0}")]
    Io(#[from] std::io::Error),
    /// JSON contract failed.
    #[error("distributed manifest JSON failed: {0}")]
    Json(#[from] serde_json::Error),
    /// RDF normalization failed.
    #[error("distributed RDF normalization failed: {0}")]
    Rdf(#[from] ngkg_reference::RdfCompileError),
    /// Request or artifact violated an immutable contract.
    #[error("distributed build contract failed: {0}")]
    Contract(String),
    /// A supposedly immutable object differs from existing bytes.
    #[error("immutable output conflict: {0}")]
    ImmutableConflict(PathBuf),
}

/// Parse the complete TriG grammar once, source-scope blank nodes, and emit
/// canonical N-Quads shards selected by the full FactID fingerprint.
pub fn safe_scan_trig(
    source_path: &Path,
    output_root: &Path,
    request: &SafeScanRequest<'_>,
) -> Result<PathBuf, DistributedBuildError> {
    require_sha256(request.source_sha256)?;
    require_sha256(request.projection_policy_sha256)?;
    if request.logical_partition_count == 0
        || request.logical_partition_count > 65_536
        || request.max_quads == 0
    {
        return Err(DistributedBuildError::Contract(
            "logical partition count must be in 1..=65536 and maxQuads must be positive".to_owned(),
        ));
    }
    if output_root.exists() {
        return Err(DistributedBuildError::ImmutableConflict(
            output_root.to_owned(),
        ));
    }
    let source_sha = decode_sha256(request.source_sha256)?;
    let facts = parse_trig(
        source_path,
        source_sha,
        request.dataset_namespace,
        request.source_guid,
        request.source_snapshot,
        request.projection_policy,
        request.max_quads,
    )?;
    fs::create_dir_all(output_root.join("shards"))?;
    let partition_count = usize::try_from(request.logical_partition_count)
        .map_err(|_| DistributedBuildError::Contract("partition count overflow".to_owned()))?;
    let mut buckets = vec![Vec::<&NormalizedFact>::new(); partition_count];
    for fact in &facts {
        let index = bucket_for(&fact.fact_hash, request.logical_partition_count)?;
        buckets[index].push(fact);
    }
    let mut graph_counts = BTreeMap::new();
    for fact in &facts {
        *graph_counts.entry(fact.graph_iri.clone()).or_insert(0_u64) += 1;
    }
    let mut shards = Vec::with_capacity(partition_count);
    for (partition_index, mut partition_facts) in buckets.into_iter().enumerate() {
        partition_facts.sort_unstable_by_key(|fact| fact.fact_hash);
        let ordinal = u32::try_from(partition_index).map_err(|_| {
            DistributedBuildError::Contract("partition ordinal overflow".to_owned())
        })?;
        let relative_path = format!("shards/part-{ordinal:05}.nq");
        let path = output_root.join(&relative_path);
        let mut writer = BufWriter::new(create_new(&path)?);
        let mut local_graph_counts = BTreeMap::new();
        for fact in &partition_facts {
            writer.write_all(nquad_line(fact).as_bytes())?;
            *local_graph_counts
                .entry(fact.graph_iri.clone())
                .or_insert(0_u64) += 1;
        }
        writer.flush()?;
        writer.get_ref().sync_all()?;
        let first = partition_facts
            .first()
            .map(|fact| hex::encode(fact.fact_hash));
        let last = partition_facts
            .last()
            .map(|fact| hex::encode(fact.fact_hash));
        let partition_id = partition_id(request, ordinal);
        shards.push(SourceShard {
            partition_index: ordinal,
            partition_id,
            relative_path,
            sha256: sha256_path(&path)?,
            bytes: fs::metadata(&path)?.len(),
            fact_count: u64::try_from(partition_facts.len())
                .map_err(|_| DistributedBuildError::Contract("fact count overflow".to_owned()))?,
            graph_counts: local_graph_counts,
            min_fact_hash: first,
            max_fact_hash: last,
        });
    }
    let plan = SourcePlan {
        format_version: DISTRIBUTED_BUILD_FORMAT_VERSION,
        dataset_id: request.dataset_id,
        snapshot_id: request.snapshot_id,
        source_sha256: request.source_sha256.to_owned(),
        projection_policy_sha256: request.projection_policy_sha256.to_owned(),
        layout_profile: format!("fact-hash-mod-{}-v1", request.logical_partition_count),
        logical_partition_count: request.logical_partition_count,
        fact_count: u64::try_from(facts.len())
            .map_err(|_| DistributedBuildError::Contract("fact count overflow".to_owned()))?,
        graph_counts,
        shards,
    };
    validate_source_plan(&plan, output_root)?;
    let plan_path = output_root.join("source-plan.json");
    write_json_new(&plan_path, &plan)?;
    sync_directory(output_root)?;
    Ok(plan_path)
}

/// Validate one safe shard and emit a canonical fact run plus sorted term run.
#[allow(clippy::too_many_arguments)]
pub fn project_partition(
    plan_path: &Path,
    expected_plan_sha256: &str,
    partition_index: u32,
    dataset_namespace: Uuid,
    source_guid: Uuid,
    source_snapshot: &str,
    projection_policy: &ProjectionPolicy,
    output_root: &Path,
    max_quads: u64,
) -> Result<PathBuf, DistributedBuildError> {
    verify_file(plan_path, expected_plan_sha256, None)?;
    let plan: SourcePlan = serde_json::from_slice(&fs::read(plan_path)?)?;
    let plan_root = plan_path
        .parent()
        .ok_or_else(|| DistributedBuildError::Contract("source plan has no parent".to_owned()))?;
    validate_source_plan_metadata(&plan)?;
    let shard =
        plan.shards
            .get(usize::try_from(partition_index).map_err(|_| {
                DistributedBuildError::Contract("partition index overflow".to_owned())
            })?)
            .filter(|shard| shard.partition_index == partition_index)
            .ok_or_else(|| {
                DistributedBuildError::Contract("partition index is not in plan".to_owned())
            })?;
    if output_root.exists() {
        return Err(DistributedBuildError::ImmutableConflict(
            output_root.to_owned(),
        ));
    }
    fs::create_dir_all(output_root)?;
    let shard_path = safe_join(plan_root, &shard.relative_path)?;
    verify_file(&shard_path, &shard.sha256, Some(shard.bytes))?;
    let source_sha = decode_sha256(&plan.source_sha256)?;
    let facts = parse_nquads(
        &shard_path,
        source_sha,
        dataset_namespace,
        source_guid,
        source_snapshot,
        projection_policy,
        max_quads,
    )?;
    if u64::try_from(facts.len()).unwrap_or(u64::MAX) != shard.fact_count {
        return Err(DistributedBuildError::Contract(
            "projection fact count differs from source plan".to_owned(),
        ));
    }
    let fact_run_path = output_root.join("facts.nq");
    let mut fact_lines = facts
        .iter()
        .map(|fact| nquad_line(fact).trim_end().to_owned())
        .collect::<Vec<_>>();
    fact_lines.sort_unstable();
    if fact_lines.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(DistributedBuildError::Contract(
            "projection emitted duplicate logical facts".to_owned(),
        ));
    }
    let mut terms = BTreeSet::new();
    let mut graph_counts = BTreeMap::new();
    for fact in &facts {
        collect_terms(fact, &mut terms);
        *graph_counts.entry(fact.graph_iri.clone()).or_insert(0_u64) += 1;
    }
    write_lines(&fact_run_path, fact_lines.iter().map(String::as_str))?;
    if graph_counts != shard.graph_counts {
        return Err(DistributedBuildError::Contract(
            "projection graph counts differ from source plan".to_owned(),
        ));
    }
    let term_run_path = output_root.join("terms.txt");
    write_lines(&term_run_path, terms.iter().map(String::as_str))?;
    let manifest = ProjectionRunManifest {
        format_version: DISTRIBUTED_BUILD_FORMAT_VERSION,
        dataset_id: plan.dataset_id,
        snapshot_id: plan.snapshot_id,
        source_plan_sha256: expected_plan_sha256.to_owned(),
        partition_index,
        partition_id: shard.partition_id.clone(),
        fact_run_path: "facts.nq".to_owned(),
        fact_run_sha256: sha256_path(&fact_run_path)?,
        term_run_path: "terms.txt".to_owned(),
        term_run_sha256: sha256_path(&term_run_path)?,
        fact_count: shard.fact_count,
        term_count: u64::try_from(terms.len())
            .map_err(|_| DistributedBuildError::Contract("term count overflow".to_owned()))?,
        graph_counts,
    };
    let manifest_path = output_root.join("projection-run.json");
    write_json_new(&manifest_path, &manifest)?;
    sync_directory(output_root)?;
    Ok(manifest_path)
}

/// Merge assigned projection runs into one immutable reducer range.
pub fn reduce_projection_runs(
    plan_path: &Path,
    expected_plan_sha256: &str,
    projection_manifest_paths: &[PathBuf],
    reducer_index: u32,
    reducer_count: u32,
    output_root: &Path,
) -> Result<PathBuf, DistributedBuildError> {
    if reducer_count == 0 || reducer_index >= reducer_count {
        return Err(DistributedBuildError::Contract(
            "reducer index must be smaller than non-zero reducer count".to_owned(),
        ));
    }
    verify_file(plan_path, expected_plan_sha256, None)?;
    let plan: SourcePlan = serde_json::from_slice(&fs::read(plan_path)?)?;
    validate_source_plan_metadata(&plan)?;
    let mut by_partition = BTreeMap::new();
    for path in projection_manifest_paths {
        let manifest: ProjectionRunManifest = serde_json::from_slice(&fs::read(path)?)?;
        validate_projection_manifest(&manifest, path, &plan, expected_plan_sha256)?;
        if by_partition
            .insert(manifest.partition_index, (path, manifest))
            .is_some()
        {
            return Err(DistributedBuildError::Contract(
                "duplicate projection partition manifest".to_owned(),
            ));
        }
    }
    let expected = plan
        .shards
        .iter()
        .filter(|shard| shard.partition_index % reducer_count == reducer_index)
        .map(|shard| shard.partition_index)
        .collect::<Vec<_>>();
    if expected
        .iter()
        .any(|index| !by_partition.contains_key(index))
    {
        return Err(DistributedBuildError::Contract(
            "reducer is missing an assigned projection run".to_owned(),
        ));
    }
    if output_root.exists() {
        return Err(DistributedBuildError::ImmutableConflict(
            output_root.to_owned(),
        ));
    }
    fs::create_dir_all(output_root)?;
    let mut fact_paths = Vec::new();
    let mut term_paths = Vec::new();
    let mut expected_fact_count = 0_u64;
    for partition_index in &expected {
        let (manifest_path, manifest) = &by_partition[partition_index];
        let root = manifest_path.parent().ok_or_else(|| {
            DistributedBuildError::Contract("projection manifest has no parent".to_owned())
        })?;
        expected_fact_count = expected_fact_count
            .checked_add(manifest.fact_count)
            .ok_or_else(|| DistributedBuildError::Contract("fact count overflow".to_owned()))?;
        fact_paths.push(safe_join(root, &manifest.fact_run_path)?);
        term_paths.push(safe_join(root, &manifest.term_run_path)?);
    }
    let fact_run_path = output_root.join("facts.nq");
    let fact_count = merge_sorted_files(&fact_paths, &fact_run_path, DuplicatePolicy::Reject)?;
    if fact_count != expected_fact_count {
        return Err(DistributedBuildError::Contract(
            "duplicate or missing fact detected during range reduction".to_owned(),
        ));
    }
    let term_run_path = output_root.join("terms.txt");
    let term_count = merge_sorted_files(&term_paths, &term_run_path, DuplicatePolicy::Deduplicate)?;
    let manifest = ReducerRunManifest {
        format_version: DISTRIBUTED_BUILD_FORMAT_VERSION,
        dataset_id: plan.dataset_id,
        snapshot_id: plan.snapshot_id,
        source_plan_sha256: expected_plan_sha256.to_owned(),
        reducer_index,
        reducer_count,
        partition_indexes: expected,
        fact_run_path: "facts.nq".to_owned(),
        fact_run_sha256: sha256_path(&fact_run_path)?,
        term_run_path: "terms.txt".to_owned(),
        term_run_sha256: sha256_path(&term_run_path)?,
        fact_count,
        term_count,
    };
    let manifest_path = output_root.join("reducer-run.json");
    write_json_new(&manifest_path, &manifest)?;
    sync_directory(output_root)?;
    Ok(manifest_path)
}

/// Validate all reducer ranges, prove exact partition coverage, and publish the
/// globally canonical N-Quads source and dense dictionary.
pub fn finalize_reducers(
    plan_path: &Path,
    expected_plan_sha256: &str,
    reducer_manifest_paths: &[PathBuf],
    output_root: &Path,
) -> Result<PathBuf, DistributedBuildError> {
    verify_file(plan_path, expected_plan_sha256, None)?;
    let plan: SourcePlan = serde_json::from_slice(&fs::read(plan_path)?)?;
    validate_source_plan_metadata(&plan)?;
    if reducer_manifest_paths.is_empty() {
        return Err(DistributedBuildError::Contract(
            "no reducer manifests".to_owned(),
        ));
    }
    let mut reducers = BTreeMap::new();
    let mut reducer_count = None;
    let mut covered = BTreeSet::new();
    let mut fact_paths = Vec::new();
    let mut term_paths = Vec::new();
    let mut expected_fact_count = 0_u64;
    for path in reducer_manifest_paths {
        let manifest: ReducerRunManifest = serde_json::from_slice(&fs::read(path)?)?;
        validate_reducer_manifest(&manifest, path, &plan, expected_plan_sha256)?;
        if reducer_count
            .replace(manifest.reducer_count)
            .is_some_and(|value| value != manifest.reducer_count)
        {
            return Err(DistributedBuildError::Contract(
                "inconsistent reducer count".to_owned(),
            ));
        }
        if reducers
            .insert(manifest.reducer_index, manifest.clone())
            .is_some()
        {
            return Err(DistributedBuildError::Contract(
                "duplicate reducer manifest".to_owned(),
            ));
        }
        for partition in &manifest.partition_indexes {
            if !covered.insert(*partition) {
                return Err(DistributedBuildError::Contract(
                    "source partition appears in more than one reducer".to_owned(),
                ));
            }
        }
        let root = path.parent().ok_or_else(|| {
            DistributedBuildError::Contract("reducer manifest has no parent".to_owned())
        })?;
        expected_fact_count = expected_fact_count
            .checked_add(manifest.fact_count)
            .ok_or_else(|| DistributedBuildError::Contract("fact count overflow".to_owned()))?;
        fact_paths.push(safe_join(root, &manifest.fact_run_path)?);
        term_paths.push(safe_join(root, &manifest.term_run_path)?);
    }
    let reducer_count = reducer_count.unwrap_or(0);
    if u32::try_from(reducers.len()).ok() != Some(reducer_count)
        || reducers.keys().copied().collect::<Vec<_>>() != (0..reducer_count).collect::<Vec<_>>()
    {
        return Err(DistributedBuildError::Contract(
            "reducer ordinal coverage is incomplete".to_owned(),
        ));
    }
    let expected_partitions = (0..plan.logical_partition_count).collect::<BTreeSet<_>>();
    if covered != expected_partitions {
        return Err(DistributedBuildError::Contract(
            "logical source partition coverage is incomplete".to_owned(),
        ));
    }
    if expected_fact_count != plan.fact_count {
        return Err(DistributedBuildError::Contract(
            "global fact coverage differs from source plan".to_owned(),
        ));
    }
    if output_root.exists() {
        return Err(DistributedBuildError::ImmutableConflict(
            output_root.to_owned(),
        ));
    }
    fs::create_dir_all(output_root)?;
    let source_path = output_root.join("canonical-source.nq");
    let fact_count = merge_sorted_files(&fact_paths, &source_path, DuplicatePolicy::Reject)?;
    if fact_count != plan.fact_count {
        return Err(DistributedBuildError::Contract(
            "global fact coverage differs from source plan".to_owned(),
        ));
    }
    let dictionary_terms_path = output_root.join("dictionary-terms.txt");
    let term_count = merge_sorted_files(
        &term_paths,
        &dictionary_terms_path,
        DuplicatePolicy::Deduplicate,
    )?;
    let dictionary_path = output_root.join("dictionary.tsv");
    let mut dictionary = BufWriter::new(create_new(&dictionary_path)?);
    for (dense_id, term) in BufReader::new(File::open(&dictionary_terms_path)?)
        .lines()
        .enumerate()
    {
        let term = term?;
        writeln!(dictionary, "{dense_id}\t{term}")?;
    }
    dictionary.flush()?;
    dictionary.get_ref().sync_all()?;
    let root = DistributedRootManifest {
        format_version: DISTRIBUTED_BUILD_FORMAT_VERSION,
        dataset_id: plan.dataset_id,
        snapshot_id: plan.snapshot_id,
        source_plan_sha256: expected_plan_sha256.to_owned(),
        canonical_source_path: "canonical-source.nq".to_owned(),
        canonical_source_sha256: sha256_path(&source_path)?,
        dictionary_path: "dictionary.tsv".to_owned(),
        dictionary_sha256: sha256_path(&dictionary_path)?,
        fact_count: plan.fact_count,
        term_count,
        reducer_count,
        semantic_content_sha256: semantic_content_hash_path(&source_path)?,
    };
    let root_path = output_root.join("distributed-root.json");
    write_json_new(&root_path, &root)?;
    validate_root_manifest(&root, &root_path)?;
    sync_directory(output_root)?;
    Ok(root_path)
}

/// Compare two physical distributed builds at the logical contract boundary.
pub fn compare_roots(
    baseline_path: &Path,
    candidate_path: &Path,
    report_path: &Path,
) -> Result<BuildEquivalenceReport, DistributedBuildError> {
    let baseline: DistributedRootManifest = serde_json::from_slice(&fs::read(baseline_path)?)?;
    let candidate: DistributedRootManifest = serde_json::from_slice(&fs::read(candidate_path)?)?;
    validate_root_manifest(&baseline, baseline_path)?;
    validate_root_manifest(&candidate, candidate_path)?;
    let mut mismatches = Vec::new();
    compare_field(
        "datasetId",
        baseline.dataset_id,
        candidate.dataset_id,
        &mut mismatches,
    );
    compare_field(
        "snapshotId",
        baseline.snapshot_id,
        candidate.snapshot_id,
        &mut mismatches,
    );
    compare_field(
        "factCount",
        baseline.fact_count,
        candidate.fact_count,
        &mut mismatches,
    );
    compare_field(
        "termCount",
        baseline.term_count,
        candidate.term_count,
        &mut mismatches,
    );
    compare_field(
        "semanticContentSha256",
        &baseline.semantic_content_sha256,
        &candidate.semantic_content_sha256,
        &mut mismatches,
    );
    compare_file_bytes(
        "canonicalSource",
        baseline_path,
        &baseline.canonical_source_path,
        candidate_path,
        &candidate.canonical_source_path,
        &mut mismatches,
    )?;
    compare_file_bytes(
        "dictionary",
        baseline_path,
        &baseline.dictionary_path,
        candidate_path,
        &candidate.dictionary_path,
        &mut mismatches,
    )?;
    let report = BuildEquivalenceReport {
        format_version: DISTRIBUTED_BUILD_FORMAT_VERSION,
        baseline_root_sha256: sha256_path(baseline_path)?,
        candidate_root_sha256: sha256_path(candidate_path)?,
        equivalent: mismatches.is_empty(),
        compared: vec![
            "datasetId".to_owned(),
            "snapshotId".to_owned(),
            "factCount".to_owned(),
            "termCount".to_owned(),
            "semanticContentSha256".to_owned(),
            "canonicalSourceBytes".to_owned(),
            "dictionaryBytes".to_owned(),
        ],
        mismatches,
    };
    write_json_new(report_path, &report)?;
    Ok(report)
}

fn validate_source_plan(plan: &SourcePlan, root: &Path) -> Result<(), DistributedBuildError> {
    validate_source_plan_metadata(plan)?;
    for shard in &plan.shards {
        let path = safe_join(root, &shard.relative_path)?;
        verify_file(&path, &shard.sha256, Some(shard.bytes))?;
    }
    Ok(())
}

fn validate_source_plan_metadata(plan: &SourcePlan) -> Result<(), DistributedBuildError> {
    require_sha256(&plan.source_sha256)?;
    require_sha256(&plan.projection_policy_sha256)?;
    if plan.format_version != DISTRIBUTED_BUILD_FORMAT_VERSION
        || plan.logical_partition_count == 0
        || usize::try_from(plan.logical_partition_count).ok() != Some(plan.shards.len())
        || plan.layout_profile != format!("fact-hash-mod-{}-v1", plan.logical_partition_count)
    {
        return Err(DistributedBuildError::Contract(
            "invalid source plan header".to_owned(),
        ));
    }
    let mut fact_count = 0_u64;
    let mut graph_counts = BTreeMap::new();
    for (expected_index, shard) in plan.shards.iter().enumerate() {
        if usize::try_from(shard.partition_index).ok() != Some(expected_index) {
            return Err(DistributedBuildError::Contract(
                "source shard ordinals are not dense".to_owned(),
            ));
        }
        require_sha256(&shard.sha256)?;
        if shard.partition_id.len() != 71
            || !shard.partition_id.starts_with("blake3:")
            || !shard.partition_id[7..]
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
            || shard.relative_path != format!("shards/part-{:05}.nq", shard.partition_index)
            || shard
                .graph_counts
                .values()
                .try_fold(0_u64, |total, count| total.checked_add(*count))
                != Some(shard.fact_count)
        {
            return Err(DistributedBuildError::Contract(
                "source shard metadata is inconsistent".to_owned(),
            ));
        }
        match (
            &shard.min_fact_hash,
            &shard.max_fact_hash,
            shard.fact_count,
            shard.bytes,
        ) {
            (None, None, 0, 0) => {}
            (Some(minimum), Some(maximum), count, bytes) if count > 0 && bytes > 0 => {
                require_sha256(minimum)?;
                require_sha256(maximum)?;
                if minimum > maximum {
                    return Err(DistributedBuildError::Contract(
                        "source shard fact-hash bounds are reversed".to_owned(),
                    ));
                }
            }
            _ => {
                return Err(DistributedBuildError::Contract(
                    "source shard fact-hash bounds disagree with its fact count".to_owned(),
                ));
            }
        }
        let _ = safe_join(Path::new("."), &shard.relative_path)?;
        fact_count = fact_count
            .checked_add(shard.fact_count)
            .ok_or_else(|| DistributedBuildError::Contract("fact count overflow".to_owned()))?;
        add_counts(&mut graph_counts, &shard.graph_counts)?;
    }
    if fact_count != plan.fact_count || graph_counts != plan.graph_counts {
        return Err(DistributedBuildError::Contract(
            "source-plan coverage mismatch".to_owned(),
        ));
    }
    Ok(())
}

fn validate_projection_manifest(
    manifest: &ProjectionRunManifest,
    path: &Path,
    plan: &SourcePlan,
    plan_sha256: &str,
) -> Result<(), DistributedBuildError> {
    if manifest.format_version != DISTRIBUTED_BUILD_FORMAT_VERSION
        || manifest.dataset_id != plan.dataset_id
        || manifest.snapshot_id != plan.snapshot_id
        || manifest.source_plan_sha256 != plan_sha256
    {
        return Err(DistributedBuildError::Contract(
            "projection manifest header mismatch".to_owned(),
        ));
    }
    let shard = plan
        .shards
        .get(usize::try_from(manifest.partition_index).map_err(|_| {
            DistributedBuildError::Contract("projection partition index overflow".to_owned())
        })?)
        .ok_or_else(|| {
            DistributedBuildError::Contract("projection partition is unknown".to_owned())
        })?;
    if manifest.partition_id != shard.partition_id
        || manifest.fact_count != shard.fact_count
        || manifest.graph_counts != shard.graph_counts
    {
        return Err(DistributedBuildError::Contract(
            "projection coverage mismatch".to_owned(),
        ));
    }
    let root = path.parent().ok_or_else(|| {
        DistributedBuildError::Contract("projection manifest has no parent".to_owned())
    })?;
    verify_file(
        &safe_join(root, &manifest.fact_run_path)?,
        &manifest.fact_run_sha256,
        None,
    )?;
    verify_file(
        &safe_join(root, &manifest.term_run_path)?,
        &manifest.term_run_sha256,
        None,
    )?;
    Ok(())
}

fn validate_reducer_manifest(
    manifest: &ReducerRunManifest,
    path: &Path,
    plan: &SourcePlan,
    plan_sha256: &str,
) -> Result<(), DistributedBuildError> {
    if manifest.format_version != DISTRIBUTED_BUILD_FORMAT_VERSION
        || manifest.dataset_id != plan.dataset_id
        || manifest.snapshot_id != plan.snapshot_id
        || manifest.source_plan_sha256 != plan_sha256
        || manifest.reducer_count == 0
        || manifest.reducer_index >= manifest.reducer_count
    {
        return Err(DistributedBuildError::Contract(
            "reducer manifest header mismatch".to_owned(),
        ));
    }
    if manifest.partition_indexes.iter().any(|partition| {
        *partition >= plan.logical_partition_count
            || *partition % manifest.reducer_count != manifest.reducer_index
    }) || manifest
        .partition_indexes
        .windows(2)
        .any(|pair| pair[0] >= pair[1])
    {
        return Err(DistributedBuildError::Contract(
            "reducer owns an invalid partition".to_owned(),
        ));
    }
    let root = path.parent().ok_or_else(|| {
        DistributedBuildError::Contract("reducer manifest has no parent".to_owned())
    })?;
    verify_file(
        &safe_join(root, &manifest.fact_run_path)?,
        &manifest.fact_run_sha256,
        None,
    )?;
    verify_file(
        &safe_join(root, &manifest.term_run_path)?,
        &manifest.term_run_sha256,
        None,
    )?;
    Ok(())
}

fn validate_root_manifest(
    manifest: &DistributedRootManifest,
    path: &Path,
) -> Result<(), DistributedBuildError> {
    if manifest.format_version != DISTRIBUTED_BUILD_FORMAT_VERSION
        || manifest.reducer_count == 0
        || manifest.canonical_source_path != "canonical-source.nq"
        || manifest.dictionary_path != "dictionary.tsv"
    {
        return Err(DistributedBuildError::Contract(
            "distributed root header is invalid".to_owned(),
        ));
    }
    require_sha256(&manifest.source_plan_sha256)?;
    require_sha256(&manifest.canonical_source_sha256)?;
    require_sha256(&manifest.dictionary_sha256)?;
    require_sha256(&manifest.semantic_content_sha256)?;
    let root = path.parent().ok_or_else(|| {
        DistributedBuildError::Contract("distributed root has no parent".to_owned())
    })?;
    let source = safe_join(root, &manifest.canonical_source_path)?;
    let dictionary = safe_join(root, &manifest.dictionary_path)?;
    verify_file(&source, &manifest.canonical_source_sha256, None)?;
    verify_file(&dictionary, &manifest.dictionary_sha256, None)?;
    if semantic_content_hash_path(&source)? != manifest.semantic_content_sha256
        || count_lines(&source)? != manifest.fact_count
        || validate_dictionary(&dictionary)? != manifest.term_count
    {
        return Err(DistributedBuildError::Contract(
            "distributed root content or counts are invalid".to_owned(),
        ));
    }
    Ok(())
}

fn count_lines(path: &Path) -> Result<u64, DistributedBuildError> {
    BufReader::new(File::open(path)?)
        .lines()
        .try_fold(0_u64, |count, line| {
            line?;
            count
                .checked_add(1)
                .ok_or_else(|| DistributedBuildError::Contract("line count overflow".to_owned()))
        })
}

fn validate_dictionary(path: &Path) -> Result<u64, DistributedBuildError> {
    let mut count = 0_u64;
    for line in BufReader::new(File::open(path)?).lines() {
        let line = line?;
        let (dense_id, term) = line.split_once('\t').ok_or_else(|| {
            DistributedBuildError::Contract("dictionary row has no tab separator".to_owned())
        })?;
        if dense_id.parse::<u64>().ok() != Some(count) || term.is_empty() {
            return Err(DistributedBuildError::Contract(
                "dictionary dense IDs or terms are invalid".to_owned(),
            ));
        }
        count = count.checked_add(1).ok_or_else(|| {
            DistributedBuildError::Contract("dictionary count overflow".to_owned())
        })?;
    }
    Ok(count)
}

fn partition_id(request: &SafeScanRequest<'_>, partition_index: u32) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"ngkg-trig-safe-shard-v1");
    hasher.update(request.dataset_id.as_bytes());
    hasher.update(request.snapshot_id.as_bytes());
    hasher.update(request.source_sha256.as_bytes());
    hasher.update(request.projection_policy_sha256.as_bytes());
    hasher.update(&request.logical_partition_count.to_be_bytes());
    hasher.update(&partition_index.to_be_bytes());
    format!("blake3:{}", hasher.finalize().to_hex())
}

fn bucket_for(hash: &[u8; 32], partition_count: u32) -> Result<usize, DistributedBuildError> {
    let prefix = u64::from_be_bytes(
        hash[0..8]
            .try_into()
            .map_err(|_| DistributedBuildError::Contract("invalid fact hash".to_owned()))?,
    );
    usize::try_from(prefix % u64::from(partition_count))
        .map_err(|_| DistributedBuildError::Contract("bucket index overflow".to_owned()))
}

fn collect_terms(fact: &NormalizedFact, terms: &mut BTreeSet<String>) {
    terms.insert(format!(
        "{}\t{}",
        fact.subject_term_kind.dictionary_tag(),
        fact.subject_iri
    ));
    terms.insert(format!("I\t{}", fact.predicate_iri));
    terms.insert(format!("I\t{}", fact.graph_iri));
    match &fact.object {
        NormalizedObject::Entity { iri, term_kind, .. } => {
            terms.insert(format!("{}\t{iri}", term_kind.dictionary_tag()));
        }
        NormalizedObject::Literal { ntriples, .. } => {
            terms.insert(format!("L\t{ntriples}"));
        }
    }
}

fn semantic_content_hash_path(path: &Path) -> Result<String, DistributedBuildError> {
    let mut hasher = Sha256::new();
    hasher.update(b"ngkg-semantic-content-v1\0");
    for fact in BufReader::new(File::open(path)?).lines() {
        let fact = fact?;
        hasher.update((fact.len() as u64).to_be_bytes());
        hasher.update(fact.as_bytes());
    }
    Ok(hex::encode(hasher.finalize()))
}

#[derive(Clone, Copy)]
enum DuplicatePolicy {
    Reject,
    Deduplicate,
}

/// K-way merge already-sorted worker runs with O(number_of_runs) resident rows.
fn merge_sorted_files(
    inputs: &[PathBuf],
    output: &Path,
    duplicate_policy: DuplicatePolicy,
) -> Result<u64, DistributedBuildError> {
    let mut readers = inputs
        .iter()
        .map(|path| File::open(path).map(BufReader::new))
        .collect::<Result<Vec<_>, _>>()?;
    let mut heap = BinaryHeap::<Reverse<(String, usize)>>::new();
    let mut previous_by_reader = vec![None::<String>; readers.len()];
    for (index, reader) in readers.iter_mut().enumerate() {
        if let Some(line) =
            next_canonical_line(reader, &inputs[index], &mut previous_by_reader[index])?
        {
            heap.push(Reverse((line, index)));
        }
    }
    let mut writer = BufWriter::new(create_new(output)?);
    let mut previous: Option<String> = None;
    let mut count = 0_u64;
    while let Some(Reverse((line, index))) = heap.pop() {
        if previous.as_deref() == Some(&line) {
            if matches!(duplicate_policy, DuplicatePolicy::Reject) {
                return Err(DistributedBuildError::Contract(format!(
                    "duplicate logical fact encountered while merging {}",
                    output.display()
                )));
            }
        } else {
            writer.write_all(line.as_bytes())?;
            writer.write_all(b"\n")?;
            count = count.checked_add(1).ok_or_else(|| {
                DistributedBuildError::Contract("merged row count overflow".to_owned())
            })?;
            previous = Some(line);
        }
        if let Some(next) = next_canonical_line(
            &mut readers[index],
            &inputs[index],
            &mut previous_by_reader[index],
        )? {
            heap.push(Reverse((next, index)));
        }
    }
    writer.flush()?;
    writer.get_ref().sync_all()?;
    Ok(count)
}

fn next_canonical_line(
    reader: &mut BufReader<File>,
    path: &Path,
    previous: &mut Option<String>,
) -> Result<Option<String>, DistributedBuildError> {
    let mut line = String::new();
    let bytes = reader.read_line(&mut line)?;
    if bytes == 0 {
        return Ok(None);
    }
    if !line.ends_with('\n') || line.ends_with("\r\n") {
        return Err(DistributedBuildError::Contract(format!(
            "run is not canonical LF-delimited text: {}",
            path.display()
        )));
    }
    line.pop();
    if line.is_empty() {
        return Err(DistributedBuildError::Contract(format!(
            "run contains an empty row: {}",
            path.display()
        )));
    }
    if previous.as_ref().is_some_and(|value| value >= &line) {
        return Err(DistributedBuildError::Contract(format!(
            "run is not strictly sorted: {}",
            path.display()
        )));
    }
    *previous = Some(line.clone());
    Ok(Some(line))
}

fn write_lines<'a>(
    path: &Path,
    lines: impl Iterator<Item = &'a str>,
) -> Result<(), DistributedBuildError> {
    let mut writer = BufWriter::new(create_new(path)?);
    for line in lines {
        writer.write_all(line.as_bytes())?;
        writer.write_all(b"\n")?;
    }
    writer.flush()?;
    writer.get_ref().sync_all()?;
    Ok(())
}

fn write_json_new(path: &Path, value: &impl Serialize) -> Result<(), DistributedBuildError> {
    let bytes = serde_json::to_vec_pretty(value)?;
    let mut file = create_new(path)?;
    file.write_all(&bytes)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    Ok(())
}

fn create_new(path: &Path) -> Result<File, DistributedBuildError> {
    OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)
        .map_err(Into::into)
}

fn verify_file(
    path: &Path,
    expected_sha256: &str,
    expected_bytes: Option<u64>,
) -> Result<(), DistributedBuildError> {
    require_sha256(expected_sha256)?;
    let metadata = fs::metadata(path)?;
    if expected_bytes.is_some_and(|bytes| bytes != metadata.len())
        || sha256_path(path)? != expected_sha256
    {
        return Err(DistributedBuildError::Contract(format!(
            "artifact checksum or size mismatch: {}",
            path.display()
        )));
    }
    Ok(())
}

fn sha256_path(path: &Path) -> Result<String, DistributedBuildError> {
    let mut reader = BufReader::new(File::open(path)?);
    let mut hasher = Sha256::new();
    let mut buffer = vec![0_u8; 1024 * 1024];
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hex::encode(hasher.finalize()))
}

fn require_sha256(value: &str) -> Result<(), DistributedBuildError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(DistributedBuildError::Contract(
            "SHA-256 must be 64 lowercase hexadecimal characters".to_owned(),
        ));
    }
    Ok(())
}

fn decode_sha256(value: &str) -> Result<[u8; 32], DistributedBuildError> {
    require_sha256(value)?;
    let bytes = hex::decode(value)
        .map_err(|_| DistributedBuildError::Contract("invalid SHA-256".to_owned()))?;
    bytes
        .try_into()
        .map_err(|_| DistributedBuildError::Contract("invalid SHA-256 length".to_owned()))
}

fn safe_join(root: &Path, relative: &str) -> Result<PathBuf, DistributedBuildError> {
    let path = Path::new(relative);
    if path.is_absolute()
        || path
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(DistributedBuildError::Contract(format!(
            "unsafe relative path: {relative}"
        )));
    }
    Ok(root.join(path))
}

fn add_counts(
    target: &mut BTreeMap<String, u64>,
    source: &BTreeMap<String, u64>,
) -> Result<(), DistributedBuildError> {
    for (key, value) in source {
        let entry = target.entry(key.clone()).or_insert(0);
        *entry = entry
            .checked_add(*value)
            .ok_or_else(|| DistributedBuildError::Contract("graph count overflow".to_owned()))?;
    }
    Ok(())
}

fn sync_directory(path: &Path) -> Result<(), DistributedBuildError> {
    File::open(path)?.sync_all()?;
    Ok(())
}

fn compare_field<T: PartialEq + std::fmt::Display>(
    name: &str,
    baseline: T,
    candidate: T,
    mismatches: &mut Vec<String>,
) {
    if baseline != candidate {
        mismatches.push(format!(
            "{name}: baseline={baseline}, candidate={candidate}"
        ));
    }
}

fn compare_file_bytes(
    name: &str,
    baseline_manifest: &Path,
    baseline_relative: &str,
    candidate_manifest: &Path,
    candidate_relative: &str,
    mismatches: &mut Vec<String>,
) -> Result<(), DistributedBuildError> {
    let baseline_root = baseline_manifest
        .parent()
        .ok_or_else(|| DistributedBuildError::Contract("baseline root missing".to_owned()))?;
    let candidate_root = candidate_manifest
        .parent()
        .ok_or_else(|| DistributedBuildError::Contract("candidate root missing".to_owned()))?;
    let baseline = fs::read(safe_join(baseline_root, baseline_relative)?)?;
    let candidate = fs::read(safe_join(candidate_root, candidate_relative)?)?;
    if baseline != candidate {
        mismatches.push(format!("{name} bytes differ"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        path::{Path, PathBuf},
    };

    use super::{
        SafeScanRequest, bucket_for, compare_roots, finalize_reducers, partition_id,
        project_partition, reduce_projection_runs, safe_scan_trig, sha256_path,
    };
    use ngkg_reference::ProjectionPolicy;
    use uuid::Uuid;

    #[test]
    fn bucket_assignment_is_stable_and_bounded() {
        let hash = [255_u8; 32];
        let first = bucket_for(&hash, 17);
        let second = bucket_for(&hash, 17);
        assert_eq!(first.as_ref().ok(), second.as_ref().ok());
        assert!(first.is_ok_and(|value| value < 17));
    }

    #[test]
    fn partition_identity_includes_snapshot_and_layout() {
        let policy = ProjectionPolicy {
            policy_id: "urn:test:policy".to_owned(),
            reject_default_graph: true,
            rules: Vec::new(),
        };
        let request = SafeScanRequest {
            dataset_id: Uuid::from_u128(1),
            snapshot_id: Uuid::from_u128(2),
            dataset_namespace: Uuid::from_u128(3),
            source_guid: Uuid::from_u128(4),
            source_snapshot: "v1",
            source_sha256: "00f1f0b83878c28b231db15f3b9b502b4c6918a71f15d87e57acc1b9622788e0",
            projection_policy_sha256: "11f1f0b83878c28b231db15f3b9b502b4c6918a71f15d87e57acc1b9622788e0",
            projection_policy: &policy,
            logical_partition_count: 8,
            max_quads: 1,
        };
        assert_ne!(partition_id(&request, 0), partition_id(&request, 1));
    }

    #[test]
    fn topology_layouts_reduce_to_identical_logical_bytes() -> Result<(), Box<dyn std::error::Error>>
    {
        let repository = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let source = repository.join("test-corpus/datasets/cross-domain.trig");
        let policy_path = repository.join("test-corpus/reference/projection-policy.json");
        let policy: ProjectionPolicy = serde_json::from_slice(&fs::read(&policy_path)?)?;
        let source_sha256 = sha256_path(&source)?;
        let policy_sha256 = sha256_path(&policy_path)?;
        let output = std::env::temp_dir().join(format!("ngkg-distributed-test-{}", Uuid::new_v4()));
        fs::create_dir(&output)?;

        let baseline = build_layout(
            &source,
            &policy,
            &source_sha256,
            &policy_sha256,
            &output.join("baseline"),
            1,
            1,
        )?;
        let distributed = build_layout(
            &source,
            &policy,
            &source_sha256,
            &policy_sha256,
            &output.join("distributed"),
            8,
            3,
        )?;
        let report = compare_roots(&baseline, &distributed, &output.join("report.json"))?;
        assert!(report.equivalent, "{}", report.mismatches.join("; "));
        fs::remove_dir_all(output)?;
        Ok(())
    }

    fn build_layout(
        source: &Path,
        policy: &ProjectionPolicy,
        source_sha256: &str,
        policy_sha256: &str,
        output: &Path,
        partition_count: u32,
        reducer_count: u32,
    ) -> Result<PathBuf, Box<dyn std::error::Error>> {
        let dataset_id = Uuid::from_u128(1);
        let snapshot_id = Uuid::from_u128(2);
        let namespace = Uuid::from_u128(3);
        let source_guid = Uuid::from_u128(4);
        let plan_root = output.join("plan");
        let plan = safe_scan_trig(
            source,
            &plan_root,
            &SafeScanRequest {
                dataset_id,
                snapshot_id,
                dataset_namespace: namespace,
                source_guid,
                source_snapshot: "test-v1",
                source_sha256,
                projection_policy_sha256: policy_sha256,
                projection_policy: policy,
                logical_partition_count: partition_count,
                max_quads: 100_000,
            },
        )?;
        let plan_sha256 = sha256_path(&plan)?;
        let mut projections = Vec::new();
        for partition_index in 0..partition_count {
            projections.push(project_partition(
                &plan,
                &plan_sha256,
                partition_index,
                namespace,
                source_guid,
                "test-v1",
                policy,
                &output.join(format!("projection-{partition_index:05}")),
                100_000,
            )?);
        }
        let mut reducers = Vec::new();
        for reducer_index in 0..reducer_count {
            reducers.push(reduce_projection_runs(
                &plan,
                &plan_sha256,
                &projections,
                reducer_index,
                reducer_count,
                &output.join(format!("reducer-{reducer_index:05}")),
            )?);
        }
        Ok(finalize_reducers(
            &plan,
            &plan_sha256,
            &reducers,
            &output.join("root"),
        )?)
    }
}
