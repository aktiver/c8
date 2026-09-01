//! Deterministic, bounded-memory physical compilation of exact HermiT consequences.
//!
//! This crate never derives OWL consequences itself. Its only semantic input is the
//! checksum-bound `finite-closure.nt` emitted by the pinned HermiT qualification
//! job. It distributes sorting, partitioning and index construction without making
//! pod topology part of result identity. Arbitrary OWL 2 DL requests not covered by
//! the finite named-consequence certificate must continue to exact online HermiT.

use std::{
    collections::{BTreeMap, BinaryHeap},
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, BufWriter, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

use arrow_array::{ArrayRef, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use ngkg_ontology_qualifier::OntologyQualificationRoot;
use oxigraph::io::{RdfFormat, RdfParser};
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};
use thiserror::Error;
use uuid::Uuid;

/// Phase 40.13.14 contract version.
pub const OFFLINE_REASONING_FORMAT_VERSION: u32 = 1;
const MERGE_FAN_IN: usize = 256;
const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
const RDFS_SUBCLASS: &str = "http://www.w3.org/2000/01/rdf-schema#subClassOf";
const RDFS_SUBPROPERTY: &str = "http://www.w3.org/2000/01/rdf-schema#subPropertyOf";
const OWL_EQUIVALENT_CLASS: &str = "http://www.w3.org/2002/07/owl#equivalentClass";
const OWL_EQUIVALENT_PROPERTY: &str = "http://www.w3.org/2002/07/owl#equivalentProperty";
const OWL_SAME_AS: &str = "http://www.w3.org/2002/07/owl#sameAs";

/// Offline materialization failures are explicit and fail closed.
#[derive(Debug, Error)]
pub enum OfflineReasoningError {
    /// Local staging failed.
    #[error("offline reasoning I/O failed: {0}")]
    Io(#[from] std::io::Error),
    /// A manifest was malformed.
    #[error("offline reasoning JSON failed: {0}")]
    Json(#[from] serde_json::Error),
    /// RDF input was not valid N-Triples.
    #[error("offline reasoning RDF failed: {0}")]
    Rdf(String),
    /// An immutable identity, ceiling, or barrier was violated.
    #[error("offline reasoning contract failed: {0}")]
    Contract(String),
    /// Arrow/Parquet output failed.
    #[error("offline reasoning Parquet failed: {0}")]
    Parquet(String),
}

/// Resource bounds for topology-independent plan construction.
#[derive(Clone, Copy, Debug)]
pub struct PlanLimits {
    /// Stable logical partition count, unrelated to current pod count.
    pub logical_partitions: u32,
    /// Maximum exact consequences accepted from HermiT.
    pub max_consequences: u64,
    /// Maximum canonical rows buffered before a sorted spill.
    pub rows_in_memory: usize,
    /// Maximum bytes accepted for one spill run.
    pub max_run_bytes: u64,
}

/// One immutable sorted input run for a logical partition.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ConsequenceRun {
    /// Path relative to the plan output root.
    pub relative_path: String,
    /// SHA-256 of the exact bytes.
    pub sha256: String,
    /// File size.
    pub bytes: u64,
    /// Number of canonical rows.
    pub row_count: u64,
}

/// Checksum-bound fan-out plan built from the exact HermiT closure.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct OfflineReasoningPlan {
    /// Contract version.
    pub format_version: u32,
    /// Dataset identity.
    pub dataset_id: Uuid,
    /// Snapshot identity.
    pub snapshot_id: Uuid,
    /// SHA-256 of the Phase 40.13.13 qualification root.
    pub ontology_qualification_root_sha256: String,
    /// Exact HermiT finite-closure digest bound by that root.
    pub finite_closure_sha256: String,
    /// Stable logical fan-out.
    pub logical_partitions: u32,
    /// Exact parsed consequence count before set deduplication.
    pub input_consequence_count: u64,
    /// Sorted runs by logical partition.
    pub partition_runs: BTreeMap<u32, Vec<ConsequenceRun>>,
    /// Semantic authority statement.
    pub authority: String,
    /// Publication state; Phase 40.13.14 never activates a snapshot.
    pub publication_state: String,
}

/// One partition's immutable output artifact.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct OfflineArtifact {
    /// Relative path.
    pub relative_path: String,
    /// SHA-256 digest.
    pub sha256: String,
    /// File size.
    pub bytes: u64,
    /// Logical row count.
    pub row_count: u64,
}

/// Evidence emitted by one Indexed Job completion.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct OfflinePartitionManifest {
    /// Contract version.
    pub format_version: u32,
    /// Dataset identity.
    pub dataset_id: Uuid,
    /// Snapshot identity.
    pub snapshot_id: Uuid,
    /// Plan digest.
    pub offline_reasoning_plan_sha256: String,
    /// Qualification root digest.
    pub ontology_qualification_root_sha256: String,
    /// Logical partition index.
    pub partition_index: u32,
    /// Unique exact consequences in this partition.
    pub consequence_count: u64,
    /// One support record per exact consequence.
    pub proof_support_count: u64,
    /// Output files.
    pub artifacts: Vec<OfflineArtifact>,
    /// Digest over canonical consequences and support identities.
    pub semantic_content_sha256: String,
}

/// Checksum-bound reference to a completed logical partition.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct OfflinePartitionReference {
    /// Partition index.
    pub partition_index: u32,
    /// Manifest location supplied by the object-store stage.
    pub manifest_path: String,
    /// Manifest SHA-256.
    pub manifest_sha256: String,
    /// Consequence count.
    pub consequence_count: u64,
}

/// One completion whose manifest and artifacts were independently verified by
/// the object-store finalizer without staging every large artifact locally.
#[derive(Clone, Debug)]
pub struct VerifiedPartitionInput {
    /// Parsed, identity-checked completion manifest.
    pub manifest: OfflinePartitionManifest,
    /// Immutable object-store manifest key.
    pub manifest_object_key: String,
    /// Digest observed after bounded materialization.
    pub manifest_sha256: String,
    /// Locally materialized and checksum-verified equality membership artifact.
    pub same_as_membership_path: PathBuf,
}

/// Complete, exact, but inactive finite named-consequence materialization.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct OfflineReasoningRoot {
    /// Contract version.
    pub format_version: u32,
    /// Tenant identity.
    pub tenant_id: Uuid,
    /// Dataset identity.
    pub dataset_id: Uuid,
    /// Operation identity.
    pub operation_id: Uuid,
    /// Snapshot identity.
    pub snapshot_id: Uuid,
    /// Exact Phase 40.13.13 root digest.
    pub ontology_qualification_root_sha256: String,
    /// Exact HermiT closure digest.
    pub finite_closure_sha256: String,
    /// Exact HermiT implementation.
    pub reasoner_name: String,
    /// Exact HermiT version.
    pub reasoner_version: String,
    /// Stable partition count.
    pub logical_partitions: u32,
    /// All partition references; no missing completion is allowed.
    pub partitions: Vec<OfflinePartitionReference>,
    /// Set cardinality of finite named consequences.
    pub consequence_count: u64,
    /// Digest of the equality-component table.
    pub equality_components_sha256: String,
    /// Number of non-singleton equality members recorded.
    pub equality_member_count: u64,
    /// Digest over the complete ordered support set.
    pub proof_support_root_sha256: String,
    /// Precisely limited coverage label.
    pub coverage: String,
    /// This materialization never claims arbitrary OWL 2 DL completeness.
    pub arbitrary_owl2_dl_complete: bool,
    /// Uncovered requests must route to exact HermiT.
    pub unknown_routes_to_exact_hermit: bool,
    /// Phase 40.13.15 is responsible for publication.
    pub publication_state: String,
}

/// Validate a qualified, consistent, inactive HermiT root.
pub fn validate_qualification(
    root: &OntologyQualificationRoot,
) -> Result<(), OfflineReasoningError> {
    validate_sha256(&root.finite_closure_sha256)?;
    if root.format_version != 1
        || root.reasoner_name != "HermiT"
        || root.reasoner_version != "1.4.5.519"
        || !root.profile_valid
        || !root.consistency_checked
        || !root.consistent
        || root.qualification_state != "owl2-dl-qualified"
        || root.publication_state != "inactive"
    {
        return Err(OfflineReasoningError::Contract(
            "exact OWL 2 DL qualification is missing, inconsistent, or active".to_owned(),
        ));
    }
    Ok(())
}

/// Build stable external-sort runs from a verified HermiT finite closure.
pub fn plan_exact_consequences(
    qualification_root: &OntologyQualificationRoot,
    qualification_root_sha256: &str,
    finite_closure_path: &Path,
    output_root: &Path,
    limits: PlanLimits,
) -> Result<PathBuf, OfflineReasoningError> {
    validate_qualification(qualification_root)?;
    validate_sha256(qualification_root_sha256)?;
    if sha256_path(finite_closure_path)? != qualification_root.finite_closure_sha256 {
        return Err(OfflineReasoningError::Contract(
            "finite closure checksum mismatch".to_owned(),
        ));
    }
    if limits.logical_partitions == 0 || limits.rows_in_memory == 0 || limits.max_run_bytes == 0 {
        return Err(OfflineReasoningError::Contract(
            "planner ceilings must be positive".to_owned(),
        ));
    }
    create_new_root(output_root)?;
    let spool_root = output_root.join("planner-spool");
    fs::create_dir(&spool_root)?;
    let shard_count = limits.logical_partitions.min(256);
    let mut spools = (0..shard_count)
        .map(|index| {
            create_new(&spool_root.join(format!("shard-{index:04}.spool"))).map(BufWriter::new)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let mut runs = BTreeMap::<u32, Vec<ConsequenceRun>>::new();
    let mut parsed_count = 0_u64;
    let parser = RdfParser::from_format(RdfFormat::NTriples)
        .for_reader(BufReader::new(File::open(finite_closure_path)?));
    for parsed in parser {
        let quad = parsed.map_err(|error| OfflineReasoningError::Rdf(error.to_string()))?;
        parsed_count = checked_add(parsed_count, 1, "input consequence")?;
        if parsed_count > limits.max_consequences {
            return Err(OfflineReasoningError::Contract(
                "HermiT consequence ceiling exceeded".to_owned(),
            ));
        }
        let canonical = format!("{} {} {} .", quad.subject, quad.predicate, quad.object);
        let partition = partition_for(&canonical, limits.logical_partitions);
        let shard = usize::try_from(partition % shard_count).map_err(|_| {
            OfflineReasoningError::Contract("planner shard index overflow".to_owned())
        })?;
        writeln!(spools[shard], "{partition:010}\t{canonical}")?;
    }
    for spool in &mut spools {
        spool.flush()?;
        spool.get_ref().sync_all()?;
    }
    drop(spools);
    for shard in 0..shard_count {
        sort_spool_into_partition_runs(
            &spool_root.join(format!("shard-{shard:04}.spool")),
            output_root,
            limits.rows_in_memory,
            limits.max_run_bytes,
            &mut runs,
        )?;
    }
    fs::remove_dir_all(&spool_root)?;
    if parsed_count != qualification_root.finite_closure_axiom_count {
        return Err(OfflineReasoningError::Contract(
            "HermiT report and finite closure counts differ".to_owned(),
        ));
    }
    let plan = OfflineReasoningPlan {
        format_version: OFFLINE_REASONING_FORMAT_VERSION,
        dataset_id: qualification_root.dataset_id,
        snapshot_id: qualification_root.snapshot_id,
        ontology_qualification_root_sha256: qualification_root_sha256.to_owned(),
        finite_closure_sha256: qualification_root.finite_closure_sha256.clone(),
        logical_partitions: limits.logical_partitions,
        input_consequence_count: parsed_count,
        partition_runs: runs,
        authority: "exact-hermit-finite-named-consequences".to_owned(),
        publication_state: "inactive".to_owned(),
    };
    let path = output_root.join("offline-reasoning-plan.json");
    write_json_new(&path, &plan)?;
    sync_directory(output_root)?;
    Ok(path)
}

/// Merge and compile one logical partition using bounded memory and fan-in.
pub fn reduce_exact_partition(
    plan_path: &Path,
    plan_sha256: &str,
    qualification_root: &OntologyQualificationRoot,
    qualification_root_sha256: &str,
    downloaded_run_root: &Path,
    partition_index: u32,
    output_root: &Path,
    row_group_rows: usize,
) -> Result<PathBuf, OfflineReasoningError> {
    validate_qualification(qualification_root)?;
    verify_file(plan_path, plan_sha256)?;
    let plan: OfflineReasoningPlan = read_json(plan_path)?;
    validate_plan(&plan, qualification_root, qualification_root_sha256)?;
    if partition_index >= plan.logical_partitions || row_group_rows == 0 {
        return Err(OfflineReasoningError::Contract(
            "invalid reducer partition or row-group ceiling".to_owned(),
        ));
    }
    create_new_root(output_root)?;
    let expected_runs = plan
        .partition_runs
        .get(&partition_index)
        .cloned()
        .unwrap_or_default();
    let mut local_runs = Vec::new();
    for run in &expected_runs {
        let path = downloaded_run_root.join(&run.relative_path);
        verify_file(&path, &run.sha256)?;
        if fs::metadata(&path)?.len() != run.bytes {
            return Err(OfflineReasoningError::Contract(
                "run byte count mismatch".to_owned(),
            ));
        }
        local_runs.push(path);
    }
    let merge_root = output_root.join("merge");
    fs::create_dir(&merge_root)?;
    let canonical = output_root.join("closure.nt");
    merge_runs_hierarchical(&local_runs, &merge_root, &canonical)?;
    let graph = format!(
        "https://c8-next-generation.io/{}/{}/closure",
        qualification_root.dataset_id, qualification_root.snapshot_id
    );
    let artifacts = build_partition_artifacts(
        &canonical,
        output_root,
        &graph,
        qualification_root_sha256,
        partition_index,
        row_group_rows,
    )?;
    fs::remove_dir_all(&merge_root)?;
    let consequence_count = line_count(&canonical)?;
    let semantic_content_sha256 =
        semantic_digest(&canonical, &output_root.join("proof-support.tsv"))?;
    let manifest = OfflinePartitionManifest {
        format_version: OFFLINE_REASONING_FORMAT_VERSION,
        dataset_id: plan.dataset_id,
        snapshot_id: plan.snapshot_id,
        offline_reasoning_plan_sha256: plan_sha256.to_owned(),
        ontology_qualification_root_sha256: qualification_root_sha256.to_owned(),
        partition_index,
        consequence_count,
        proof_support_count: consequence_count,
        artifacts,
        semantic_content_sha256,
    };
    let path = output_root.join("offline-partition.json");
    write_json_new(&path, &manifest)?;
    sync_directory(output_root)?;
    Ok(path)
}

/// Verify every Indexed completion and emit an inactive completeness root.
pub fn finalize_offline_reasoning(
    plan_path: &Path,
    plan_sha256: &str,
    qualification_root: &OntologyQualificationRoot,
    qualification_root_sha256: &str,
    partition_manifests: &[PathBuf],
    output_root: &Path,
) -> Result<PathBuf, OfflineReasoningError> {
    let mut verified = Vec::with_capacity(partition_manifests.len());
    for path in partition_manifests {
        let manifest: OfflinePartitionManifest = read_json(path)?;
        let parent = path
            .parent()
            .ok_or_else(|| OfflineReasoningError::Contract("manifest has no parent".to_owned()))?;
        for artifact in &manifest.artifacts {
            verify_file(&parent.join(&artifact.relative_path), &artifact.sha256)?;
        }
        verified.push(VerifiedPartitionInput {
            manifest,
            manifest_object_key: path.to_string_lossy().into_owned(),
            manifest_sha256: sha256_path(path)?,
            same_as_membership_path: parent.join("sameas-membership.tsv"),
        });
    }
    finalize_offline_reasoning_verified(
        plan_path,
        plan_sha256,
        qualification_root,
        qualification_root_sha256,
        &verified,
        output_root,
    )
}

/// Finalize after an object-store caller has checksum-verified every referenced
/// artifact remotely and materialized only the much smaller equality rows.
pub fn finalize_offline_reasoning_verified(
    plan_path: &Path,
    plan_sha256: &str,
    qualification_root: &OntologyQualificationRoot,
    qualification_root_sha256: &str,
    partitions: &[VerifiedPartitionInput],
    output_root: &Path,
) -> Result<PathBuf, OfflineReasoningError> {
    validate_qualification(qualification_root)?;
    verify_file(plan_path, plan_sha256)?;
    let plan: OfflineReasoningPlan = read_json(plan_path)?;
    validate_plan(&plan, qualification_root, qualification_root_sha256)?;
    if partitions.len()
        != usize::try_from(plan.logical_partitions).map_err(|_| {
            OfflineReasoningError::Contract("logical partition count overflow".to_owned())
        })?
    {
        return Err(OfflineReasoningError::Contract(
            "partition completion barrier is incomplete".to_owned(),
        ));
    }
    create_new_root(output_root)?;
    let mut ordered = partitions.to_vec();
    ordered.sort_by_key(|input| input.manifest.partition_index);
    let mut seen = vec![false; ordered.len()];
    let mut references = Vec::with_capacity(ordered.len());
    let mut consequence_count = 0_u64;
    let mut proof_root = Sha256::new();
    let mut same_as_paths = Vec::new();
    for input in &ordered {
        let manifest = &input.manifest;
        let index = usize::try_from(manifest.partition_index)
            .map_err(|_| OfflineReasoningError::Contract("partition index overflow".to_owned()))?;
        if index >= seen.len() || seen[index] {
            return Err(OfflineReasoningError::Contract(
                "duplicate or out-of-range partition completion".to_owned(),
            ));
        }
        if manifest.format_version != OFFLINE_REASONING_FORMAT_VERSION
            || manifest.dataset_id != plan.dataset_id
            || manifest.snapshot_id != plan.snapshot_id
            || manifest.offline_reasoning_plan_sha256 != plan_sha256
            || manifest.ontology_qualification_root_sha256 != qualification_root_sha256
            || manifest.consequence_count != manifest.proof_support_count
        {
            return Err(OfflineReasoningError::Contract(
                "partition completion identity mismatch".to_owned(),
            ));
        }
        seen[index] = true;
        consequence_count =
            checked_add(consequence_count, manifest.consequence_count, "consequence")?;
        proof_root.update(manifest.semantic_content_sha256.as_bytes());
        let same_as = manifest
            .artifacts
            .iter()
            .find(|artifact| artifact.relative_path == "sameas-membership.tsv")
            .ok_or_else(|| {
                OfflineReasoningError::Contract(
                    "partition lacks equality membership artifact".to_owned(),
                )
            })?;
        verify_file(&input.same_as_membership_path, &same_as.sha256)?;
        same_as_paths.push(input.same_as_membership_path.clone());
        references.push(OfflinePartitionReference {
            partition_index: manifest.partition_index,
            manifest_path: input.manifest_object_key.clone(),
            manifest_sha256: input.manifest_sha256.clone(),
            consequence_count: manifest.consequence_count,
        });
    }
    if seen.iter().any(|value| !*value) {
        return Err(OfflineReasoningError::Contract(
            "missing partition completion".to_owned(),
        ));
    }
    references.sort_by_key(|value| value.partition_index);
    let equality_path = output_root.join("equality-components.tsv");
    let equality_member_count = merge_equality_membership(&same_as_paths, &equality_path)?;
    let root = OfflineReasoningRoot {
        format_version: OFFLINE_REASONING_FORMAT_VERSION,
        tenant_id: qualification_root.tenant_id,
        dataset_id: qualification_root.dataset_id,
        operation_id: qualification_root.operation_id,
        snapshot_id: qualification_root.snapshot_id,
        ontology_qualification_root_sha256: qualification_root_sha256.to_owned(),
        finite_closure_sha256: qualification_root.finite_closure_sha256.clone(),
        reasoner_name: qualification_root.reasoner_name.clone(),
        reasoner_version: qualification_root.reasoner_version.clone(),
        logical_partitions: plan.logical_partitions,
        partitions: references,
        consequence_count,
        equality_components_sha256: sha256_path(&equality_path)?,
        equality_member_count,
        proof_support_root_sha256: hex::encode(proof_root.finalize()),
        coverage: "finite-named-consequences-emitted-by-exact-hermit".to_owned(),
        arbitrary_owl2_dl_complete: false,
        unknown_routes_to_exact_hermit: true,
        publication_state: "inactive".to_owned(),
    };
    let path = output_root.join("offline-reasoning-root.json");
    write_json_new(&path, &root)?;
    sync_directory(output_root)?;
    Ok(path)
}

fn validate_plan(
    plan: &OfflineReasoningPlan,
    root: &OntologyQualificationRoot,
    root_sha256: &str,
) -> Result<(), OfflineReasoningError> {
    if plan.format_version != OFFLINE_REASONING_FORMAT_VERSION
        || plan.dataset_id != root.dataset_id
        || plan.snapshot_id != root.snapshot_id
        || plan.ontology_qualification_root_sha256 != root_sha256
        || plan.finite_closure_sha256 != root.finite_closure_sha256
        || plan.authority != "exact-hermit-finite-named-consequences"
        || plan.publication_state != "inactive"
        || plan.logical_partitions == 0
    {
        return Err(OfflineReasoningError::Contract(
            "offline plan identity mismatch".to_owned(),
        ));
    }
    Ok(())
}

fn sort_spool_into_partition_runs(
    spool: &Path,
    output_root: &Path,
    rows_in_memory: usize,
    max_run_bytes: u64,
    runs: &mut BTreeMap<u32, Vec<ConsequenceRun>>,
) -> Result<(), OfflineReasoningError> {
    let scratch = spool.with_extension("sort");
    fs::create_dir(&scratch)?;
    let mut chunks = Vec::new();
    let mut buffer = Vec::with_capacity(rows_in_memory);
    for row in BufReader::new(File::open(spool)?).lines() {
        buffer.push(row?);
        if buffer.len() >= rows_in_memory {
            flush_sort_run(&scratch, &mut buffer, &mut chunks)?;
        }
    }
    flush_sort_run(&scratch, &mut buffer, &mut chunks)?;
    if chunks.len() > MERGE_FAN_IN {
        let collapsed = scratch.join("collapsed.spool");
        merge_runs_hierarchical(&chunks, &scratch, &collapsed)?;
        chunks = vec![collapsed];
    }
    let mut readers = chunks
        .iter()
        .map(|path| File::open(path).map(BufReader::new))
        .collect::<Result<Vec<_>, _>>()?;
    let mut heap = BinaryHeap::new();
    for (index, reader) in readers.iter_mut().enumerate() {
        if let Some(row) = next_row(reader)? {
            heap.push(HeapRow { row, reader: index });
        }
    }
    let mut previous = None::<String>;
    let mut active = None::<ActiveRun>;
    while let Some(item) = heap.pop() {
        if previous.as_deref() != Some(&item.row) {
            let (prefix, triple) = item.row.split_once('\t').ok_or_else(|| {
                OfflineReasoningError::Contract("planner spool row is malformed".to_owned())
            })?;
            let partition = prefix.parse::<u32>().map_err(|_| {
                OfflineReasoningError::Contract("planner partition is malformed".to_owned())
            })?;
            let row_bytes = u64::try_from(triple.len() + 1).map_err(|_| {
                OfflineReasoningError::Contract("planner row length overflow".to_owned())
            })?;
            if row_bytes > max_run_bytes {
                return Err(OfflineReasoningError::Contract(
                    "one consequence exceeds run byte ceiling".to_owned(),
                ));
            }
            let rotate = active.as_ref().is_none_or(|run| {
                run.partition != partition || run.bytes.saturating_add(row_bytes) > max_run_bytes
            });
            if rotate {
                if let Some(run) = active.take() {
                    finish_active_run(run, runs)?;
                }
                active = Some(start_active_run(
                    output_root,
                    partition,
                    runs.get(&partition).map_or(0, Vec::len),
                )?);
            }
            let run = active.as_mut().ok_or_else(|| {
                OfflineReasoningError::Contract("planner run is absent".to_owned())
            })?;
            writeln!(run.writer, "{triple}")?;
            run.bytes = checked_add(run.bytes, row_bytes, "run byte")?;
            run.rows = checked_add(run.rows, 1, "run row")?;
            previous = Some(item.row.clone());
        }
        if let Some(row) = next_row(&mut readers[item.reader])? {
            heap.push(HeapRow {
                row,
                reader: item.reader,
            });
        }
    }
    if let Some(run) = active {
        finish_active_run(run, runs)?;
    }
    fs::remove_dir_all(&scratch)?;
    Ok(())
}

struct ActiveRun {
    partition: u32,
    relative_path: String,
    path: PathBuf,
    writer: BufWriter<File>,
    bytes: u64,
    rows: u64,
}

fn start_active_run(
    output_root: &Path,
    partition: u32,
    ordinal: usize,
) -> Result<ActiveRun, OfflineReasoningError> {
    let relative_path = format!("runs/{partition:05}/run-{ordinal:08}.nt");
    let path = output_root.join(&relative_path);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    Ok(ActiveRun {
        partition,
        relative_path,
        writer: BufWriter::new(create_new(&path)?),
        path,
        bytes: 0,
        rows: 0,
    })
}

fn finish_active_run(
    mut run: ActiveRun,
    runs: &mut BTreeMap<u32, Vec<ConsequenceRun>>,
) -> Result<(), OfflineReasoningError> {
    run.writer.flush()?;
    run.writer.get_ref().sync_all()?;
    drop(run.writer);
    runs.entry(run.partition).or_default().push(ConsequenceRun {
        relative_path: run.relative_path,
        sha256: sha256_path(&run.path)?,
        bytes: run.bytes,
        row_count: run.rows,
    });
    Ok(())
}

fn merge_runs_hierarchical(
    inputs: &[PathBuf],
    scratch: &Path,
    output: &Path,
) -> Result<(), OfflineReasoningError> {
    if inputs.is_empty() {
        create_new(output)?.sync_all()?;
        return Ok(());
    }
    let mut generation = inputs.to_vec();
    let mut level = 0_u32;
    while generation.len() > MERGE_FAN_IN {
        let mut next = Vec::new();
        for (index, chunk) in generation.chunks(MERGE_FAN_IN).enumerate() {
            let path = scratch.join(format!("merge-{level:04}-{index:08}.run"));
            merge_sorted_unique(chunk, &path)?;
            next.push(path);
        }
        if level > 0 {
            for path in generation {
                fs::remove_file(path)?;
            }
        }
        generation = next;
        level = level
            .checked_add(1)
            .ok_or_else(|| OfflineReasoningError::Contract("merge level overflow".to_owned()))?;
    }
    merge_sorted_unique(&generation, output)?;
    if level > 0 {
        for path in generation {
            fs::remove_file(path)?;
        }
    }
    Ok(())
}

#[derive(Eq, PartialEq)]
struct HeapRow {
    row: String,
    reader: usize,
}
impl Ord for HeapRow {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other
            .row
            .cmp(&self.row)
            .then_with(|| other.reader.cmp(&self.reader))
    }
}
impl PartialOrd for HeapRow {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

fn merge_sorted_unique(inputs: &[PathBuf], output: &Path) -> Result<(), OfflineReasoningError> {
    let mut readers = inputs
        .iter()
        .map(|path| File::open(path).map(BufReader::new))
        .collect::<Result<Vec<_>, _>>()?;
    let mut heap = BinaryHeap::new();
    for (index, reader) in readers.iter_mut().enumerate() {
        if let Some(row) = next_row(reader)? {
            heap.push(HeapRow { row, reader: index });
        }
    }
    let mut writer = BufWriter::new(create_new(output)?);
    let mut previous = None::<String>;
    while let Some(item) = heap.pop() {
        if previous.as_deref() != Some(&item.row) {
            writeln!(writer, "{}", item.row)?;
            previous = Some(item.row.clone());
        }
        if let Some(row) = next_row(&mut readers[item.reader])? {
            heap.push(HeapRow {
                row,
                reader: item.reader,
            });
        }
    }
    writer.flush()?;
    writer.get_ref().sync_all()?;
    Ok(())
}

fn next_row(reader: &mut BufReader<File>) -> Result<Option<String>, OfflineReasoningError> {
    let mut row = String::new();
    if reader.read_line(&mut row)? == 0 {
        return Ok(None);
    }
    while row.ends_with('\n') || row.ends_with('\r') {
        row.pop();
    }
    Ok(Some(row))
}

fn build_partition_artifacts(
    canonical: &Path,
    output_root: &Path,
    closure_graph: &str,
    qualification_root_sha256: &str,
    partition_index: u32,
    row_group_rows: usize,
) -> Result<Vec<OfflineArtifact>, OfflineReasoningError> {
    let nq_path = output_root.join("closure.nq");
    let proof_path = output_root.join("proof-support.tsv");
    let proof_unsorted_path = output_root.join("proof-support.unsorted");
    let class_path = output_root.join("class-extents.tsv");
    let class_unsorted_path = output_root.join("class-extents.unsorted");
    let property_path = output_root.join("property-extents.tsv");
    let property_unsorted_path = output_root.join("property-extents.unsorted");
    let class_hierarchy_path = output_root.join("class-hierarchy.tsv");
    let class_hierarchy_unsorted_path = output_root.join("class-hierarchy.unsorted");
    let property_hierarchy_path = output_root.join("property-hierarchy.tsv");
    let property_hierarchy_unsorted_path = output_root.join("property-hierarchy.unsorted");
    let sameas_path = output_root.join("sameas-membership.tsv");
    let sameas_unsorted_path = output_root.join("sameas-membership.unsorted");
    let parquet_path = output_root.join("closure.parquet");
    let mut nq = BufWriter::new(create_new(&nq_path)?);
    let mut proof = BufWriter::new(create_new(&proof_unsorted_path)?);
    let mut class = BufWriter::new(create_new(&class_unsorted_path)?);
    let mut property = BufWriter::new(create_new(&property_unsorted_path)?);
    let mut class_hierarchy = BufWriter::new(create_new(&class_hierarchy_unsorted_path)?);
    let mut property_hierarchy = BufWriter::new(create_new(&property_hierarchy_unsorted_path)?);
    let mut sameas = BufWriter::new(create_new(&sameas_unsorted_path)?);
    let schema = Arc::new(Schema::new(vec![
        Field::new("canonical_triple", DataType::Utf8, false),
        Field::new("closure_graph", DataType::Utf8, false),
        Field::new("support_id", DataType::Utf8, false),
        Field::new("semantic_kind", DataType::Utf8, false),
    ]));
    let properties = WriterProperties::builder()
        .set_max_row_group_row_count(Some(row_group_rows))
        .build();
    let mut parquet = ArrowWriter::try_new(
        create_new(&parquet_path)?,
        Arc::clone(&schema),
        Some(properties),
    )
    .map_err(|error| OfflineReasoningError::Parquet(error.to_string()))?;
    let mut triples = Vec::with_capacity(row_group_rows);
    let mut graphs = Vec::with_capacity(row_group_rows);
    let mut supports = Vec::with_capacity(row_group_rows);
    let mut kinds = Vec::with_capacity(row_group_rows);
    let parser = RdfParser::from_format(RdfFormat::NTriples)
        .for_reader(BufReader::new(File::open(canonical)?));
    for parsed in parser {
        let quad = parsed.map_err(|error| OfflineReasoningError::Rdf(error.to_string()))?;
        let triple = format!("{} {} {} .", quad.subject, quad.predicate, quad.object);
        let support = support_id(qualification_root_sha256, partition_index, &triple);
        let kind = semantic_kind(quad.predicate.as_str());
        let without_dot = triple.strip_suffix(" .").ok_or_else(|| {
            OfflineReasoningError::Contract("canonical triple lacks terminator".to_owned())
        })?;
        writeln!(nq, "{without_dot} <{closure_graph}> .")?;
        writeln!(proof, "{support}\t{partition_index}\t{triple}")?;
        match quad.predicate.as_str() {
            RDF_TYPE => writeln!(class, "{}\t{}\t{support}", quad.object, quad.subject)?,
            RDFS_SUBCLASS | OWL_EQUIVALENT_CLASS => writeln!(
                class_hierarchy,
                "{}\t{}\t{support}",
                quad.subject, quad.object
            )?,
            RDFS_SUBPROPERTY | OWL_EQUIVALENT_PROPERTY => writeln!(
                property_hierarchy,
                "{}\t{}\t{support}",
                quad.subject, quad.object
            )?,
            OWL_SAME_AS => {
                let left = quad.subject.to_string();
                let right = quad.object.to_string();
                let representative = if left <= right {
                    left.clone()
                } else {
                    right.clone()
                };
                writeln!(sameas, "{left}\t{representative}\t{support}")?;
                writeln!(sameas, "{right}\t{representative}\t{support}")?;
            }
            _ => writeln!(
                property,
                "{}\t{}\t{}\t{support}",
                quad.predicate, quad.subject, quad.object
            )?,
        }
        triples.push(triple);
        graphs.push(closure_graph.to_owned());
        supports.push(support);
        kinds.push(kind.to_owned());
        if triples.len() >= row_group_rows {
            flush_parquet(
                &mut parquet,
                &schema,
                &mut triples,
                &mut graphs,
                &mut supports,
                &mut kinds,
            )?;
        }
    }
    flush_parquet(
        &mut parquet,
        &schema,
        &mut triples,
        &mut graphs,
        &mut supports,
        &mut kinds,
    )?;
    parquet
        .close()
        .map_err(|error| OfflineReasoningError::Parquet(error.to_string()))?;
    for writer in [
        &mut nq,
        &mut proof,
        &mut class,
        &mut property,
        &mut class_hierarchy,
        &mut property_hierarchy,
        &mut sameas,
    ] {
        writer.flush()?;
        writer.get_ref().sync_all()?;
    }
    drop((
        proof,
        class,
        property,
        class_hierarchy,
        property_hierarchy,
        sameas,
    ));
    for (unsorted, sorted) in [
        (&proof_unsorted_path, &proof_path),
        (&class_unsorted_path, &class_path),
        (&property_unsorted_path, &property_path),
        (&class_hierarchy_unsorted_path, &class_hierarchy_path),
        (&property_hierarchy_unsorted_path, &property_hierarchy_path),
        (&sameas_unsorted_path, &sameas_path),
    ] {
        external_sort_unique(unsorted, sorted, row_group_rows)?;
        fs::remove_file(unsorted)?;
    }
    let mut artifacts = Vec::new();
    for (name, rows) in [
        ("closure.nt", line_count(canonical)?),
        ("closure.nq", line_count(&nq_path)?),
        ("proof-support.tsv", line_count(&proof_path)?),
        ("class-extents.tsv", line_count(&class_path)?),
        ("property-extents.tsv", line_count(&property_path)?),
        ("class-hierarchy.tsv", line_count(&class_hierarchy_path)?),
        (
            "property-hierarchy.tsv",
            line_count(&property_hierarchy_path)?,
        ),
        ("sameas-membership.tsv", line_count(&sameas_path)?),
        ("closure.parquet", line_count(canonical)?),
    ] {
        let path = output_root.join(name);
        artifacts.push(OfflineArtifact {
            relative_path: name.to_owned(),
            sha256: sha256_path(&path)?,
            bytes: fs::metadata(&path)?.len(),
            row_count: rows,
        });
    }
    Ok(artifacts)
}

fn flush_parquet(
    writer: &mut ArrowWriter<File>,
    schema: &Arc<Schema>,
    triples: &mut Vec<String>,
    graphs: &mut Vec<String>,
    supports: &mut Vec<String>,
    kinds: &mut Vec<String>,
) -> Result<(), OfflineReasoningError> {
    if triples.is_empty() {
        return Ok(());
    }
    let columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(std::mem::take(triples))),
        Arc::new(StringArray::from(std::mem::take(graphs))),
        Arc::new(StringArray::from(std::mem::take(supports))),
        Arc::new(StringArray::from(std::mem::take(kinds))),
    ];
    let batch = RecordBatch::try_new(Arc::clone(schema), columns)
        .map_err(|error| OfflineReasoningError::Parquet(error.to_string()))?;
    writer
        .write(&batch)
        .map_err(|error| OfflineReasoningError::Parquet(error.to_string()))?;
    Ok(())
}

fn semantic_kind(predicate: &str) -> &'static str {
    match predicate {
        RDF_TYPE => "class-extent",
        RDFS_SUBCLASS | OWL_EQUIVALENT_CLASS => "class-hierarchy",
        RDFS_SUBPROPERTY | OWL_EQUIVALENT_PROPERTY => "property-hierarchy",
        OWL_SAME_AS => "equality",
        _ => "property-extent",
    }
}

fn support_id(root: &str, partition: u32, triple: &str) -> String {
    let mut hash = Sha256::new();
    hash.update(b"ngkg-exact-support-v1\0");
    hash.update(root.as_bytes());
    hash.update(b"\0");
    hash.update(partition.to_be_bytes());
    hash.update(b"\0");
    hash.update(triple.as_bytes());
    format!("urn:ngkg:support:sha256:{}", hex::encode(hash.finalize()))
}

fn merge_equality_membership(
    inputs: &[PathBuf],
    output: &Path,
) -> Result<u64, OfflineReasoningError> {
    let parent = output.parent().ok_or_else(|| {
        OfflineReasoningError::Contract("equality output has no parent".to_owned())
    })?;
    let scratch = parent.join("equality-merge");
    fs::create_dir(&scratch)?;
    let sorted = scratch.join("all-memberships.tsv");
    merge_runs_hierarchical(inputs, &scratch, &sorted)?;
    let mut writer = BufWriter::new(create_new(output)?);
    let mut current_member = None::<String>;
    let mut representative = String::new();
    let mut count = 0_u64;
    for row in BufReader::new(File::open(&sorted)?).lines() {
        let row = row?;
        let mut columns = row.splitn(3, '\t');
        let member = columns
            .next()
            .ok_or_else(|| OfflineReasoningError::Contract("invalid equality row".to_owned()))?;
        let candidate = columns
            .next()
            .ok_or_else(|| OfflineReasoningError::Contract("invalid equality row".to_owned()))?;
        if current_member.as_deref() != Some(member) {
            if let Some(previous) = current_member.replace(member.to_owned()) {
                writeln!(writer, "{previous}\t{representative}")?;
                count = checked_add(count, 1, "equality member")?;
            }
            candidate.clone_into(&mut representative);
        } else if candidate < representative.as_str() {
            candidate.clone_into(&mut representative);
        }
    }
    if let Some(previous) = current_member {
        writeln!(writer, "{previous}\t{representative}")?;
        count = checked_add(count, 1, "equality member")?;
    }
    writer.flush()?;
    writer.get_ref().sync_all()?;
    fs::remove_dir_all(&scratch)?;
    Ok(count)
}

fn external_sort_unique(
    input: &Path,
    output: &Path,
    rows_in_memory: usize,
) -> Result<(), OfflineReasoningError> {
    let parent = output
        .parent()
        .ok_or_else(|| OfflineReasoningError::Contract("sort output has no parent".to_owned()))?;
    let scratch = parent.join("external-sort");
    fs::create_dir(&scratch)?;
    let mut runs = Vec::new();
    let mut buffer = Vec::with_capacity(rows_in_memory);
    for row in BufReader::new(File::open(input)?).lines() {
        buffer.push(row?);
        if buffer.len() >= rows_in_memory {
            flush_sort_run(&scratch, &mut buffer, &mut runs)?;
        }
    }
    flush_sort_run(&scratch, &mut buffer, &mut runs)?;
    merge_runs_hierarchical(&runs, &scratch, output)?;
    fs::remove_dir_all(&scratch)?;
    Ok(())
}

fn flush_sort_run(
    root: &Path,
    buffer: &mut Vec<String>,
    runs: &mut Vec<PathBuf>,
) -> Result<(), OfflineReasoningError> {
    if buffer.is_empty() {
        return Ok(());
    }
    buffer.sort();
    buffer.dedup();
    let path = root.join(format!("sort-{:08}.run", runs.len()));
    let mut writer = BufWriter::new(create_new(&path)?);
    for row in buffer.iter() {
        writeln!(writer, "{row}")?;
    }
    writer.flush()?;
    writer.get_ref().sync_all()?;
    buffer.clear();
    runs.push(path);
    Ok(())
}

fn semantic_digest(first: &Path, second: &Path) -> Result<String, OfflineReasoningError> {
    let mut hash = Sha256::new();
    for path in [first, second] {
        hash.update(fs::metadata(path)?.len().to_be_bytes());
        let mut reader = BufReader::new(File::open(path)?);
        std::io::copy(&mut reader, &mut HashWriter(&mut hash))?;
    }
    Ok(hex::encode(hash.finalize()))
}

struct HashWriter<'a>(&'a mut Sha256);
impl Write for HashWriter<'_> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.update(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn partition_for(row: &str, partitions: u32) -> u32 {
    let digest = Sha256::digest(row.as_bytes());
    u32::from_be_bytes([digest[0], digest[1], digest[2], digest[3]]) % partitions
}

fn line_count(path: &Path) -> Result<u64, OfflineReasoningError> {
    let mut count = 0_u64;
    for row in BufReader::new(File::open(path)?).lines() {
        row?;
        count = checked_add(count, 1, "line")?;
    }
    Ok(count)
}

fn checked_add(value: u64, increment: u64, label: &str) -> Result<u64, OfflineReasoningError> {
    value
        .checked_add(increment)
        .ok_or_else(|| OfflineReasoningError::Contract(format!("{label} count overflow")))
}

fn validate_sha256(value: &str) -> Result<(), OfflineReasoningError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(OfflineReasoningError::Contract(
            "invalid SHA-256".to_owned(),
        ));
    }
    Ok(())
}

/// Compute SHA-256 without loading the file into memory.
pub fn sha256_path(path: &Path) -> Result<String, OfflineReasoningError> {
    let mut hash = Sha256::new();
    let mut reader = BufReader::new(File::open(path)?);
    std::io::copy(&mut reader, &mut HashWriter(&mut hash))?;
    Ok(hex::encode(hash.finalize()))
}

fn verify_file(path: &Path, expected: &str) -> Result<(), OfflineReasoningError> {
    validate_sha256(expected)?;
    if sha256_path(path)? != expected {
        return Err(OfflineReasoningError::Contract(format!(
            "checksum mismatch for {}",
            path.display()
        )));
    }
    Ok(())
}

fn read_json<T: DeserializeOwned>(path: &Path) -> Result<T, OfflineReasoningError> {
    Ok(serde_json::from_slice(&fs::read(path)?)?)
}
fn write_json_new<T: Serialize>(path: &Path, value: &T) -> Result<(), OfflineReasoningError> {
    let mut bytes = serde_json::to_vec_pretty(value)?;
    bytes.push(b'\n');
    let mut file = create_new(path)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    Ok(())
}
fn create_new(path: &Path) -> Result<File, std::io::Error> {
    OpenOptions::new().create_new(true).write(true).open(path)
}
fn create_new_root(path: &Path) -> Result<(), OfflineReasoningError> {
    if path.exists() {
        return Err(OfflineReasoningError::Contract(format!(
            "output root already exists: {}",
            path.display()
        )));
    }
    fs::create_dir_all(path)?;
    Ok(())
}
fn sync_directory(path: &Path) -> Result<(), OfflineReasoningError> {
    File::open(path)?.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn qualified() -> OntologyQualificationRoot {
        OntologyQualificationRoot {
            format_version: 1,
            tenant_id: Uuid::nil(),
            dataset_id: Uuid::nil(),
            operation_id: Uuid::nil(),
            snapshot_id: Uuid::nil(),
            semantic_compilation_root_sha256: "1".repeat(64),
            qualification_request_sha256: "2".repeat(64),
            assembly_manifest_sha256: "3".repeat(64),
            authorized_graph_set_sha256: "4".repeat(64),
            datatype_policy_sha256: "5".repeat(64),
            synthetic_snapshot_ontology_sha256: "6".repeat(64),
            owl_signature_sha256: "7".repeat(64),
            owl_profile_qualification_sha256: "8".repeat(64),
            owl_consistency_qualification_sha256: "9".repeat(64),
            reasoner_report_sha256: "a".repeat(64),
            finite_closure_sha256: "b".repeat(64),
            finite_closure_axiom_count: 2,
            reasoner_name: "HermiT".to_owned(),
            reasoner_version: "1.4.5.519".to_owned(),
            profile_valid: true,
            consistency_checked: true,
            consistent: true,
            qualification_state: "owl2-dl-qualified".to_owned(),
            publication_state: "inactive".to_owned(),
        }
    }

    #[test]
    fn topology_does_not_change_logical_partitioning() {
        assert_eq!(
            partition_for("<urn:a> <urn:p> <urn:b> .", 64),
            partition_for("<urn:a> <urn:p> <urn:b> .", 64)
        );
    }

    #[test]
    fn rejects_unqualified_or_active_roots() {
        let mut root = qualified();
        root.publication_state = "active".to_owned();
        assert!(validate_qualification(&root).is_err());
    }

    #[test]
    fn exact_closure_round_trip_is_complete_and_inactive() -> Result<(), Box<dyn std::error::Error>>
    {
        let base = std::env::temp_dir().join(format!("ngkg-offline-reasoning-{}", Uuid::new_v4()));
        fs::create_dir(&base)?;
        let closure = base.join("finite-closure.nt");
        fs::write(
            &closure,
            concat!(
                "<urn:a> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <urn:C> .\n",
                "<urn:a> <http://www.w3.org/2002/07/owl#sameAs> <urn:b> .\n",
                "<urn:a> <urn:p> <urn:b> .\n",
            ),
        )?;
        let mut root = qualified();
        root.finite_closure_sha256 = sha256_path(&closure)?;
        root.finite_closure_axiom_count = 3;
        let root_sha256 = "c".repeat(64);
        let plan_root = base.join("plan");
        let plan_path = plan_exact_consequences(
            &root,
            &root_sha256,
            &closure,
            &plan_root,
            PlanLimits {
                logical_partitions: 4,
                max_consequences: 10,
                rows_in_memory: 2,
                max_run_bytes: 4096,
            },
        )?;
        let plan_sha256 = sha256_path(&plan_path)?;
        let mut manifests = Vec::new();
        for partition in 0..4 {
            let output = base.join(format!("partition-{partition}"));
            manifests.push(reduce_exact_partition(
                &plan_path,
                &plan_sha256,
                &root,
                &root_sha256,
                &plan_root,
                partition,
                &output,
                2,
            )?);
        }
        let final_path = finalize_offline_reasoning(
            &plan_path,
            &plan_sha256,
            &root,
            &root_sha256,
            &manifests,
            &base.join("final"),
        )?;
        let result: OfflineReasoningRoot = read_json(&final_path)?;
        assert_eq!(result.consequence_count, 3);
        assert_eq!(result.publication_state, "inactive");
        assert!(!result.arbitrary_owl2_dl_complete);
        assert!(result.unknown_routes_to_exact_hermit);
        fs::remove_dir_all(base)?;
        Ok(())
    }
}
