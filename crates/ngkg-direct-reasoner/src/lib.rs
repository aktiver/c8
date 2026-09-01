//! Phase 40.8 exhaustive OWL 2 Direct-Semantics fallback coordinator.
//!
//! This crate is intentionally correctness-first. It consumes a Phase 40.7 legality decision,
//! extracts the exact typed BGP template, partitions the finite candidate space deterministically,
//! launches checksum-pinned HermiT adapter processes, and merges only complete partition reports.
//! Any timeout, process failure, counter mismatch, identity mismatch, or resource ceiling failure
//! returns an error; no partial answer is promoted to exact/complete.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use ngkg_owl_direct::extract_direct_bgp_template;
use ngkg_sparql_compiler::CompiledSparqlQuery;
use ngkg_types::{
    DIRECT_EXACT_ENGINE_V1, DIRECT_EXACT_FORMAT_VERSION, DirectBgpCompleteness, DirectBgpExactness,
    DirectBgpGraphContext, DirectBgpLegalityRecord, DirectBgpOutcome, DirectBgpResult,
    DirectBgpSolution, DirectBgpStatus, DirectCertificate, DirectCertifiedOutcome,
    DirectCompletenessEvidence, DirectCompletenessMethod, DirectExactOntologyInput,
    DirectExactPartition, DirectExactPartitionResult, DirectExactRequest, DirectProofCoverage,
    DirectProofManifest, DirectProofOntologyInput, DirectReasonerCheckProof,
    DirectReasonerIdentity, DirectSupportKind, DirectSupportReference, EntailmentRegime,
    direct_bgp_result_sha256, direct_binding_sha256, direct_completion_support_id,
    direct_reasoner_support_id, validate_direct_certificate_result,
    validate_direct_exact_partition_result, validate_direct_exact_request,
    validate_direct_proof_bundle, validate_direct_proof_manifest,
};
use sha2::{Digest, Sha256};
use thiserror::Error;
use uuid::Uuid;

const MAX_LOCAL_REASONER_LANES: usize = 8;
const MAX_EXACT_PARTITIONS: u64 = 4096;

/// Exact execution ceilings. Phase 40.10 moves these into the authoritative Helm contract.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DirectExactLimits {
    pub max_candidate_bindings: u64,
    pub max_partition_candidates: u64,
    pub max_exact_partitions: u64,
    pub max_grounded_axioms_per_candidate: u64,
    pub max_grounded_rdf_bytes_per_candidate: u64,
    pub max_local_reasoner_lanes: usize,
    pub reasoner_heap_mib_per_lane: u64,
    pub reasoner_timeout: Duration,
    pub max_certificate_bytes: u64,
    pub max_proof_support_ids: u64,
}

impl Default for DirectExactLimits {
    fn default() -> Self {
        Self {
            max_candidate_bindings: 10_000_000,
            max_partition_candidates: 250_000,
            max_exact_partitions: MAX_EXACT_PARTITIONS,
            max_grounded_axioms_per_candidate: 65_536,
            max_grounded_rdf_bytes_per_candidate: 16 * 1024 * 1024,
            max_local_reasoner_lanes: MAX_LOCAL_REASONER_LANES,
            reasoner_heap_mib_per_lane: 4096,
            reasoner_timeout: Duration::from_secs(300),
            max_certificate_bytes: 512 * 1024 * 1024,
            max_proof_support_ids: 1_000_000,
        }
    }
}

#[derive(Clone, Debug)]
pub struct DirectExactBindings {
    pub dataset_id: Uuid,
    pub snapshot_id: Uuid,
    pub query_sha256: String,
    pub sparql_algebra_sha256: String,
    pub active_dataset_sha256: String,
    pub authorized_graph_set_sha256: String,
    pub owl_signature_sha256: String,
    pub datatype_policy_sha256: String,
    pub owl_profile_qualification_sha256: String,
    pub owl_consistency_qualification_sha256: String,
    pub graph_context: DirectBgpGraphContext,
}

#[derive(Clone, Debug)]
pub struct DirectExactAdapter {
    pub java_executable: PathBuf,
    pub adapter_jar: PathBuf,
    pub adapter_sha256: String,
    pub adapter_version: String,
    pub reasoner_version: String,
}

#[derive(Clone, Debug)]
pub struct DirectExactOntologyBundle {
    pub inputs: Vec<DirectExactOntologyInput>,
    pub aggregate_input_sha256: String,
    /// Exact active graph represented by the appended ABox input.
    pub graph_context: DirectBgpGraphContext,
    /// SHA-256 of the exact RDF scoping graph materialized for this BGP.
    pub scoped_graph_sha256: String,
}

/// Immutable partition requests prepared for either local lanes or distributed workers.
#[derive(Clone, Debug)]
pub struct PreparedDirectExactBgp {
    pub requests: Vec<DirectExactRequest>,
    pub request_set_sha256: String,
}

#[derive(Debug, Error)]
pub enum DirectExactError {
    #[error("Phase 40.7 BGP template extraction failed: {0}")]
    Template(String),
    #[error("exact adapter artifact is missing or has the wrong SHA-256")]
    AdapterIntegrity,
    #[error("exact fallback I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("exact fallback JSON failed: {0}")]
    Json(#[from] serde_json::Error),
    #[error("exact fallback request is invalid: {0}")]
    InvalidRequest(String),
    #[error("exact reasoner partition {0} failed or timed out")]
    PartitionFailure(u32),
    #[error("exact reasoner partition result is invalid: {0}")]
    InvalidPartition(String),
    #[error("exact reasoner partitions disagree on candidate-space identity")]
    PartitionMismatch,
    #[error("exact reasoner returned duplicate candidate ordinals")]
    DuplicateCandidate,
    #[error("exact partition plan exceeds bounded Phase 40.8 limits")]
    PartitionPlan,
    #[error("Direct result/certificate construction failed")]
    Certificate,
    #[error("Phase 40.11 trusted runtime ceiling exceeded: {0}")]
    ResourceCeiling(&'static str),
}

/// Execute one admitted BGP exhaustively. The caller supplies the exact active ontology bundle;
/// therefore authorization/dataset selection has already happened before any candidate exists.
pub fn execute_exact_direct_bgp(
    query: &CompiledSparqlQuery,
    legality: &DirectBgpLegalityRecord,
    bindings: &DirectExactBindings,
    ontology: &DirectExactOntologyBundle,
    adapter: &DirectExactAdapter,
    work_dir: &Path,
    limits: DirectExactLimits,
) -> Result<(DirectBgpResult, DirectCertificate, DirectProofManifest), DirectExactError> {
    verify_adapter(adapter)?;
    fs::create_dir_all(work_dir)?;
    let available = thread::available_parallelism().map_or(1, |count| count.get());
    let concurrent_lanes = available
        .min(limits.max_local_reasoner_lanes.max(1))
        .clamp(1, MAX_LOCAL_REASONER_LANES);
    let request_root = work_dir.join("direct-exact");
    if request_root.exists() {
        fs::remove_dir_all(&request_root)?;
    }
    fs::create_dir_all(&request_root)?;
    let prepared = prepare_exact_direct_bgp_requests(
        query,
        legality,
        bindings,
        ontology,
        &request_root,
        limits,
    )?;
    let mut requests = Vec::with_capacity(prepared.requests.len());
    for request in prepared.requests {
        let index = request.partition.index;
        let output_path = PathBuf::from(&request.output_path);
        if let Some(parent) = output_path.parent() {
            fs::create_dir_all(parent)?;
        }
        let request_path = request_root.join(format!("partition-{index:04}.request.json"));
        write_atomic_bytes(&request_path, &serde_json::to_vec_pretty(&request)?)?;
        requests.push((request, request_path, output_path));
    }

    let request_set_sha256 = prepared.request_set_sha256;
    let mut results = Vec::with_capacity(requests.len());
    for wave in requests.chunks(concurrent_lanes) {
        let cancel = Arc::new(AtomicBool::new(false));
        let mut wave_results = Vec::with_capacity(wave.len());
        thread::scope(|scope| {
            let mut handles = Vec::with_capacity(wave.len());
            for (request, request_path, output_path) in wave {
                let adapter = adapter.clone();
                let request = request.clone();
                let request_path = request_path.clone();
                let output_path = output_path.clone();
                let cancel = Arc::clone(&cancel);
                handles.push(scope.spawn(move || {
                    let outcome = run_partition(
                        &adapter,
                        &request,
                        &request_path,
                        &output_path,
                        limits.reasoner_timeout,
                        limits.reasoner_heap_mib_per_lane,
                        &cancel,
                    );
                    if outcome.is_err() {
                        cancel.store(true, Ordering::Release);
                    }
                    outcome
                }));
            }
            for handle in handles {
                wave_results.push(
                    handle
                        .join()
                        .map_err(|_| DirectExactError::PartitionMismatch)??,
                );
            }
            Ok::<(), DirectExactError>(())
        })?;
        results.extend(wave_results);
    }
    results.sort_by_key(|result| result.partition.index);
    merge_partition_results(
        results,
        bindings,
        legality,
        ontology,
        adapter,
        &request_set_sha256,
        &limits,
    )
}

/// Prepare the immutable request set consumed by distributed reasoner workers.
///
/// Partition identity depends only on admitted ceilings and query/snapshot hashes, never on the
/// current Kubernetes replica count. `output_root` must name the shared worker-side work root.
pub fn prepare_exact_direct_bgp_requests(
    query: &CompiledSparqlQuery,
    legality: &DirectBgpLegalityRecord,
    bindings: &DirectExactBindings,
    ontology: &DirectExactOntologyBundle,
    output_root: &Path,
    limits: DirectExactLimits,
) -> Result<PreparedDirectExactBgp, DirectExactError> {
    if legality.status != ngkg_types::DirectBgpLegalityStatus::Legal
        || !legality.grounded_owl2dl_check_required
    {
        return Err(DirectExactError::Template(
            "Phase 40.7 did not admit this BGP".to_owned(),
        ));
    }
    match (&legality.graph_scope, &bindings.graph_context) {
        (ngkg_types::DirectBgpScope::Default, DirectBgpGraphContext::Default { .. }) => {}
        (
            ngkg_types::DirectBgpScope::Named { graph_iri },
            DirectBgpGraphContext::Named { graph_iri: active },
        ) if graph_iri == active => {}
        (ngkg_types::DirectBgpScope::NamedVariable { .. }, DirectBgpGraphContext::Named { .. }) => {
        }
        _ => {
            return Err(DirectExactError::Template(
                "active graph context does not satisfy Phase 40.7 BGP scope".to_owned(),
            ));
        }
    }
    if ontology.graph_context != bindings.graph_context {
        return Err(DirectExactError::Template(
            "active ontology graph context differs from exact bindings".to_owned(),
        ));
    }
    let template = extract_direct_bgp_template(query, legality)
        .map_err(|error| DirectExactError::Template(error.to_string()))?;
    if template.triples.iter().any(|triple| {
        [&triple.subject, &triple.predicate, &triple.object]
            .into_iter()
            .any(|term| matches!(term, ngkg_types::DirectExactTermPattern::BlankNode { .. }))
    }) {
        return Err(DirectExactError::Template(
            "anonymous-individual instance mappings are not yet qualified for the exact path"
                .to_owned(),
        ));
    }
    if limits.max_partition_candidates == 0
        || limits.max_exact_partitions == 0
        || limits.reasoner_heap_mib_per_lane == 0
        || limits.max_certificate_bytes == 0
        || limits.max_proof_support_ids == 0
    {
        return Err(DirectExactError::PartitionPlan);
    }
    let partition_count = limits
        .max_candidate_bindings
        .div_ceil(limits.max_partition_candidates)
        .max(1);
    if partition_count > limits.max_exact_partitions || partition_count > MAX_EXACT_PARTITIONS {
        return Err(DirectExactError::PartitionPlan);
    }
    let partition_count_u32 =
        u32::try_from(partition_count).map_err(|_| DirectExactError::PartitionPlan)?;
    let mut requests = Vec::with_capacity(
        usize::try_from(partition_count).map_err(|_| DirectExactError::PartitionPlan)?,
    );
    for index in 0..partition_count_u32 {
        let request = DirectExactRequest {
            format_version: DIRECT_EXACT_FORMAT_VERSION,
            dataset_id: bindings.dataset_id,
            snapshot_id: bindings.snapshot_id,
            query_sha256: bindings.query_sha256.clone(),
            sparql_algebra_sha256: bindings.sparql_algebra_sha256.clone(),
            bgp_sha256: legality.bgp_sha256.clone(),
            active_dataset_sha256: bindings.active_dataset_sha256.clone(),
            authorized_graph_set_sha256: bindings.authorized_graph_set_sha256.clone(),
            owl_signature_sha256: bindings.owl_signature_sha256.clone(),
            datatype_policy_sha256: bindings.datatype_policy_sha256.clone(),
            owl_profile_qualification_sha256: bindings.owl_profile_qualification_sha256.clone(),
            owl_consistency_qualification_sha256: bindings
                .owl_consistency_qualification_sha256
                .clone(),
            engine: DIRECT_EXACT_ENGINE_V1.to_owned(),
            inputs: ontology.inputs.clone(),
            aggregate_input_sha256: ontology.aggregate_input_sha256.clone(),
            template: template.clone(),
            partition: DirectExactPartition {
                index,
                count: partition_count_u32,
            },
            max_candidate_bindings: limits.max_candidate_bindings,
            max_partition_candidates: limits.max_partition_candidates,
            max_grounded_axioms_per_candidate: limits.max_grounded_axioms_per_candidate,
            max_grounded_rdf_bytes_per_candidate: limits.max_grounded_rdf_bytes_per_candidate,
            output_path: output_root
                .join(bindings.dataset_id.to_string())
                .join(bindings.snapshot_id.to_string())
                .join(&bindings.query_sha256)
                .join(&legality.bgp_sha256)
                .join(format!("partition-{index:04}"))
                .join("result.json")
                .to_string_lossy()
                .into_owned(),
        };
        validate_direct_exact_request(&request)
            .map_err(|error| DirectExactError::InvalidRequest(error.to_string()))?;
        requests.push(request);
    }
    let request_set_sha256 = direct_exact_request_set_sha256(&requests)?;
    Ok(PreparedDirectExactBgp {
        requests,
        request_set_sha256,
    })
}

fn run_partition(
    adapter: &DirectExactAdapter,
    request: &DirectExactRequest,
    request_path: &Path,
    output_path: &Path,
    timeout: Duration,
    heap_mib: u64,
    cancel: &AtomicBool,
) -> Result<DirectExactPartitionResult, DirectExactError> {
    let partition_dir = request_path
        .parent()
        .ok_or(DirectExactError::PartitionMismatch)?
        .join(format!("partition-{:04}-scratch", request.partition.index));
    fs::create_dir_all(&partition_dir)?;
    let stderr_path = request_path.with_extension("stderr.log");
    let stderr = fs::File::create(&stderr_path)?;
    let mut child = Command::new(&adapter.java_executable)
        .arg("-XX:+ExitOnOutOfMemoryError")
        .arg("-XX:ActiveProcessorCount=1")
        .arg("-XX:ParallelGCThreads=1")
        .arg("-XX:ConcGCThreads=1")
        .arg(format!("-Xmx{heap_mib}m"))
        .arg(format!("-Djava.io.tmpdir={}", partition_dir.display()))
        .arg("-jar")
        .arg(&adapter.adapter_jar)
        .arg("--direct-request")
        .arg(request_path)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::from(stderr))
        .spawn()?;
    let deadline = Instant::now() + timeout;
    loop {
        if cancel.load(Ordering::Acquire) {
            let _ = child.kill();
            let _ = child.wait();
            return Err(DirectExactError::PartitionFailure(request.partition.index));
        }
        if let Some(status) = child.try_wait()? {
            if !status.success() {
                return Err(DirectExactError::PartitionFailure(request.partition.index));
            }
            break;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            return Err(DirectExactError::PartitionFailure(request.partition.index));
        }
        thread::sleep(Duration::from_millis(25));
    }
    let result: DirectExactPartitionResult = serde_json::from_slice(&fs::read(output_path)?)?;
    validate_direct_exact_partition_result(&result)
        .map_err(|error| DirectExactError::InvalidPartition(error.to_string()))?;
    if result.dataset_id != request.dataset_id
        || result.snapshot_id != request.snapshot_id
        || result.query_sha256 != request.query_sha256
        || result.bgp_sha256 != request.bgp_sha256
        || result.partition != request.partition
        || result.aggregate_input_sha256 != request.aggregate_input_sha256
        || result.request_sha256 != hex::encode(sha256_file(request_path)?)
    {
        return Err(DirectExactError::PartitionMismatch);
    }
    Ok(result)
}

/// Execute one immutable exact-reasoner partition in an online worker.
///
/// The service boundary must independently constrain ontology input paths and `work_dir` to its
/// operator-owned roots before calling this function. The request is serialized canonically by
/// this worker so the returned `requestSha256` is bound to the bytes HermiT actually consumed.
pub fn execute_exact_direct_partition(
    adapter: &DirectExactAdapter,
    request: &DirectExactRequest,
    work_dir: &Path,
    limits: DirectExactLimits,
) -> Result<DirectExactPartitionResult, DirectExactError> {
    verify_adapter(adapter)?;
    validate_direct_exact_request(request)
        .map_err(|error| DirectExactError::InvalidRequest(error.to_string()))?;
    if limits.reasoner_heap_mib_per_lane == 0 || limits.reasoner_timeout.is_zero() {
        return Err(DirectExactError::PartitionPlan);
    }
    fs::create_dir_all(work_dir)?;
    let request_path = work_dir.join(format!(
        "partition-{:04}.request.json",
        request.partition.index
    ));
    let output_path = PathBuf::from(&request.output_path);
    write_atomic_bytes(&request_path, &serde_json::to_vec_pretty(request)?)?;
    let cancel = AtomicBool::new(false);
    run_partition(
        adapter,
        request,
        &request_path,
        &output_path,
        limits.reasoner_timeout,
        limits.reasoner_heap_mib_per_lane,
        &cancel,
    )
}

/// Merge a complete set of exact HermiT partition reports into the result, proof-support
/// manifest, and completeness certificate. Any missing range or identity disagreement fails.
pub fn merge_partition_results(
    results: Vec<DirectExactPartitionResult>,
    bindings: &DirectExactBindings,
    legality: &DirectBgpLegalityRecord,
    ontology: &DirectExactOntologyBundle,
    adapter: &DirectExactAdapter,
    request_set_sha256: &str,
    limits: &DirectExactLimits,
) -> Result<(DirectBgpResult, DirectCertificate, DirectProofManifest), DirectExactError> {
    let first = results.first().ok_or(DirectExactError::PartitionMismatch)?;
    let candidate_count = first.candidate_binding_count;
    if results.iter().any(|result| {
        result.candidate_binding_count != candidate_count
            || result.candidate_space_sha256 != first.candidate_space_sha256
            || result.aggregate_input_sha256 != first.aggregate_input_sha256
            || result.reasoner_name != "HermiT"
            || result.reasoner_version != adapter.reasoner_version
            || result.adapter_version != adapter.adapter_version
    }) {
        return Err(DirectExactError::PartitionMismatch);
    }
    let expected_partition_count =
        u32::try_from(results.len()).map_err(|_| DirectExactError::PartitionMismatch)?;
    let mut cursor = 0_u64;
    for (index, result) in results.iter().enumerate() {
        if result.partition.count != expected_partition_count
            || result.partition.index
                != u32::try_from(index).map_err(|_| DirectExactError::PartitionMismatch)?
            || result.partition_start_ordinal != cursor
        {
            return Err(DirectExactError::PartitionMismatch);
        }
        cursor = result.partition_end_ordinal_exclusive;
    }
    if cursor != candidate_count {
        return Err(DirectExactError::PartitionMismatch);
    }
    let checked = results.iter().try_fold(0_u64, |total, result| {
        total
            .checked_add(result.checked_candidate_count)
            .ok_or(DirectExactError::PartitionMismatch)
    })?;
    if checked != candidate_count {
        return Err(DirectExactError::PartitionMismatch);
    }

    let mut by_binding: BTreeMap<String, DirectBgpSolution> = BTreeMap::new();
    let mut candidate_ordinals = std::collections::BTreeSet::new();
    for result in &results {
        for entailed in &result.entailed {
            if !candidate_ordinals.insert(entailed.candidate_ordinal) {
                return Err(DirectExactError::DuplicateCandidate);
            }
            let key = serde_json::to_string(&entailed.bindings)?;
            let entry = by_binding.entry(key).or_insert(DirectBgpSolution {
                bindings: entailed.bindings.clone(),
                multiplicity: 0,
            });
            entry.multiplicity = entry
                .multiplicity
                .checked_add(1)
                .ok_or(DirectExactError::PartitionMismatch)?;
        }
    }
    let solutions = by_binding.into_values().collect::<Vec<_>>();
    let multiplicity = solutions.iter().try_fold(0_u64, |total, solution| {
        total
            .checked_add(solution.multiplicity)
            .ok_or(DirectExactError::PartitionMismatch)
    })?;
    let required_support_ids = multiplicity
        .checked_add(1)
        .ok_or(DirectExactError::PartitionMismatch)?;
    enforce_proof_support_limit(required_support_ids, limits)?;
    let variables = legality
        .variables
        .iter()
        .map(|typing| typing.variable.clone())
        .collect::<Vec<_>>();
    let result = DirectBgpResult {
        format_version: 1,
        dataset_id: bindings.dataset_id,
        snapshot_id: bindings.snapshot_id,
        query_sha256: bindings.query_sha256.clone(),
        bgp_sha256: legality.bgp_sha256.clone(),
        active_dataset_sha256: bindings.active_dataset_sha256.clone(),
        authorized_graph_set_sha256: bindings.authorized_graph_set_sha256.clone(),
        owl_signature_sha256: bindings.owl_signature_sha256.clone(),
        datatype_policy_sha256: bindings.datatype_policy_sha256.clone(),
        entailment_regime: EntailmentRegime::Owl2Direct,
        graph_context: bindings.graph_context.clone(),
        variables,
        candidate_binding_count: candidate_count,
        solution_multiplicity_total: multiplicity,
        solutions,
        outcome: DirectBgpOutcome {
            status: DirectBgpStatus::Complete,
            exactness: DirectBgpExactness::Exact,
            completeness: DirectBgpCompleteness::Complete,
        },
        error: None,
    };
    let result_hash =
        direct_bgp_result_sha256(&result).map_err(|_| DirectExactError::Certificate)?;
    let candidate_space_sha256 = first.candidate_space_sha256.clone();
    let execution_root_sha256 = hash_partition_results(&results)?;
    let request_sha256 = request_set_sha256.to_owned();
    let partition_count =
        u32::try_from(results.len()).map_err(|_| DirectExactError::PartitionMismatch)?;
    let reasoner_requests = results.iter().try_fold(0_u64, |total, result| {
        total
            .checked_add(result.reasoner_request_count)
            .ok_or(DirectExactError::PartitionMismatch)
    })?;

    let mut ontology_input_iris = BTreeMap::<String, BTreeSet<String>>::new();
    for input in &ontology.inputs {
        ontology_input_iris
            .entry(input.sha256.clone())
            .or_default()
            .extend(input.ontology_iris.iter().cloned());
    }
    let ontology_inputs = ontology_input_iris
        .into_iter()
        .map(|(sha256, ontology_iris)| DirectProofOntologyInput {
            sha256,
            ontology_iris: ontology_iris.into_iter().collect(),
        })
        .collect::<Vec<_>>();

    let mut proof_manifest = DirectProofManifest {
        format_version: 1,
        dataset_id: bindings.dataset_id,
        snapshot_id: bindings.snapshot_id,
        query_sha256: bindings.query_sha256.clone(),
        bgp_sha256: legality.bgp_sha256.clone(),
        active_dataset_sha256: bindings.active_dataset_sha256.clone(),
        authorized_graph_set_sha256: bindings.authorized_graph_set_sha256.clone(),
        owl_signature_sha256: bindings.owl_signature_sha256.clone(),
        datatype_policy_sha256: bindings.datatype_policy_sha256.clone(),
        entailment_regime: EntailmentRegime::Owl2Direct,
        graph_context: bindings.graph_context.clone(),
        direct_bgp_result_sha256: result_hash.clone(),
        candidate_space_sha256: candidate_space_sha256.clone(),
        execution_root_sha256: execution_root_sha256.clone(),
        reasoner_engine: "HermiT".to_owned(),
        reasoner_version: adapter.reasoner_version.clone(),
        adapter_version: adapter.adapter_version.clone(),
        completion_support_id: String::new(),
        ontology_inputs,
        answer_proofs: Vec::with_capacity(usize::try_from(multiplicity).unwrap_or(0)),
    };
    for partition in &results {
        for entailed in &partition.entailed {
            let mut proof = DirectReasonerCheckProof {
                support_id: String::new(),
                candidate_ordinal: entailed.candidate_ordinal,
                partition_index: partition.partition.index,
                request_sha256: partition.request_sha256.clone(),
                binding_sha256: direct_binding_sha256(&entailed.bindings),
                grounded_rdf_sha256: entailed.grounded_rdf_sha256.clone(),
                logical_axioms_sha256: entailed.logical_axioms_sha256.clone(),
                logical_axiom_count: entailed.logical_axiom_count,
            };
            proof.support_id = direct_reasoner_support_id(&proof_manifest, &proof);
            proof_manifest.answer_proofs.push(proof);
        }
    }
    proof_manifest
        .answer_proofs
        .sort_by_key(|proof| proof.candidate_ordinal);
    proof_manifest.completion_support_id = direct_completion_support_id(&proof_manifest);
    validate_direct_proof_manifest(&proof_manifest).map_err(|_| DirectExactError::Certificate)?;
    let proof_manifest_bytes = serde_json::to_vec_pretty(&proof_manifest)?;
    let proof_manifest_sha256 = hex::encode(Sha256::digest(&proof_manifest_bytes));

    let source_graph_iri = match &bindings.graph_context {
        DirectBgpGraphContext::Named { graph_iri } => Some(graph_iri.clone()),
        DirectBgpGraphContext::Default { .. } => None,
    };
    let mut support_references = std::iter::once(DirectSupportReference {
        support_id: proof_manifest.completion_support_id.clone(),
        kind: DirectSupportKind::ReasonerCheck,
        artifact_sha256: Some(proof_manifest_sha256.clone()),
        source_graph_iri: source_graph_iri.clone(),
    })
    .chain(
        proof_manifest
            .answer_proofs
            .iter()
            .map(|proof| DirectSupportReference {
                support_id: proof.support_id.clone(),
                kind: DirectSupportKind::ReasonerCheck,
                artifact_sha256: Some(proof_manifest_sha256.clone()),
                source_graph_iri: source_graph_iri.clone(),
            }),
    )
    .collect::<Vec<_>>();
    support_references.sort_by(|a, b| a.support_id.cmp(&b.support_id));

    let certificate = DirectCertificate {
        format_version: 2,
        dataset_id: bindings.dataset_id,
        snapshot_id: bindings.snapshot_id,
        query_sha256: bindings.query_sha256.clone(),
        bgp_sha256: legality.bgp_sha256.clone(),
        active_dataset_sha256: bindings.active_dataset_sha256.clone(),
        authorized_graph_set_sha256: bindings.authorized_graph_set_sha256.clone(),
        owl_signature_sha256: bindings.owl_signature_sha256.clone(),
        datatype_policy_sha256: bindings.datatype_policy_sha256.clone(),
        entailment_regime: EntailmentRegime::Owl2Direct,
        graph_context: bindings.graph_context.clone(),
        certified_outcome: DirectCertifiedOutcome::ExactComplete,
        direct_bgp_result_sha256: result_hash,
        proof_manifest_sha256: Some(proof_manifest_sha256.clone()),
        reasoner: DirectReasonerIdentity {
            engine: "HermiT".to_owned(),
            engine_version: adapter.reasoner_version.clone(),
            adapter_name: "ngkg-hermit-adapter".to_owned(),
            adapter_version: adapter.adapter_version.clone(),
            request_sha256,
        },
        completeness: DirectCompletenessEvidence {
            method: DirectCompletenessMethod::ExhaustiveCandidateEntailment,
            candidate_space_sha256,
            candidate_binding_count: candidate_count,
            checked_candidate_binding_count: checked,
            partition_count,
            completed_partition_count: partition_count,
            reasoner_request_count: reasoner_requests,
            successful_reasoner_request_count: reasoner_requests,
            execution_root_sha256,
        },
        proof_coverage: DirectProofCoverage::Complete,
        support_references,
    };
    validate_direct_certificate_result(&certificate, &result)
        .map_err(|_| DirectExactError::Certificate)?;
    validate_direct_proof_bundle(
        &proof_manifest,
        &result,
        &certificate,
        &proof_manifest_sha256,
    )
    .map_err(|_| DirectExactError::Certificate)?;
    let certificate_bytes = serde_json::to_vec(&certificate)?;
    enforce_certificate_size_limit(certificate_bytes.len(), limits)?;
    Ok((result, certificate, proof_manifest))
}

fn enforce_proof_support_limit(
    required_support_ids: u64,
    limits: &DirectExactLimits,
) -> Result<(), DirectExactError> {
    if required_support_ids > limits.max_proof_support_ids {
        return Err(DirectExactError::ResourceCeiling("maxProofSupportIds"));
    }
    Ok(())
}

fn enforce_certificate_size_limit(
    certificate_bytes: usize,
    limits: &DirectExactLimits,
) -> Result<(), DirectExactError> {
    let certificate_bytes =
        u64::try_from(certificate_bytes).map_err(|_| DirectExactError::Certificate)?;
    if certificate_bytes > limits.max_certificate_bytes {
        return Err(DirectExactError::ResourceCeiling("maxCertificateBytes"));
    }
    Ok(())
}

fn verify_adapter(adapter: &DirectExactAdapter) -> Result<(), DirectExactError> {
    if !adapter.java_executable.is_file() || !adapter.adapter_jar.is_file() {
        return Err(DirectExactError::AdapterIntegrity);
    }
    let observed = hex::encode(sha256_file(&adapter.adapter_jar)?);
    if observed != adapter.adapter_sha256 {
        return Err(DirectExactError::AdapterIntegrity);
    }
    Ok(())
}

fn sha256_file(path: &Path) -> Result<[u8; 32], std::io::Error> {
    use std::io::Read;
    let mut file = fs::File::open(path)?;
    let mut hash = Sha256::new();
    let mut buf = vec![0_u8; 1024 * 1024].into_boxed_slice();
    loop {
        let read = file.read(&mut buf)?;
        if read == 0 {
            break;
        }
        hash.update(&buf[..read]);
    }
    Ok(hash.finalize().into())
}

fn write_atomic_bytes(path: &Path, bytes: &[u8]) -> Result<(), std::io::Error> {
    use std::io::Write as _;
    let parent = path
        .parent()
        .ok_or_else(|| std::io::Error::other("atomic request path has no parent"))?;
    fs::create_dir_all(parent)?;
    let temporary = parent.join(format!(".request-{}.tmp", Uuid::new_v4()));
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)?;
    if let Err(error) = (|| {
        file.write_all(bytes)?;
        file.sync_all()?;
        drop(file);
        fs::rename(&temporary, path)?;
        Ok::<(), std::io::Error>(())
    })() {
        let _ = fs::remove_file(&temporary);
        return Err(error);
    }
    Ok(())
}

fn hash_partition_results(
    results: &[DirectExactPartitionResult],
) -> Result<String, DirectExactError> {
    let mut digests = Vec::with_capacity(results.len());
    for result in results {
        let bytes = serde_json::to_vec(result)?;
        digests.push(Sha256::digest(bytes));
    }
    digests.sort();
    let mut hash = Sha256::new();
    hash.update(b"ngkg-direct-exact-execution-root-v1\0");
    for digest in digests {
        hash.update(digest);
    }
    Ok(hex::encode(hash.finalize()))
}

/// Hash an immutable cross-pod request set exactly as the local coordinator does.
pub fn direct_exact_request_set_sha256(
    requests: &[DirectExactRequest],
) -> Result<String, DirectExactError> {
    let mut digests = Vec::with_capacity(requests.len());
    for request in requests {
        digests.push(Sha256::digest(serde_json::to_vec_pretty(request)?));
    }
    digests.sort();
    let mut hash = Sha256::new();
    hash.update(b"ngkg-direct-exact-request-set-v1\0");
    for digest in digests {
        hash.update(digest);
    }
    Ok(hex::encode(hash.finalize()))
}

#[cfg(test)]
mod tests {
    use super::{
        DirectExactError, DirectExactLimits, enforce_certificate_size_limit,
        enforce_proof_support_limit,
    };

    #[test]
    fn merge_proof_support_ceiling_is_enforced_at_the_exact_boundary() {
        let limits = DirectExactLimits {
            max_proof_support_ids: 2,
            ..DirectExactLimits::default()
        };
        assert!(enforce_proof_support_limit(2, &limits).is_ok());
        assert!(matches!(
            enforce_proof_support_limit(3, &limits),
            Err(DirectExactError::ResourceCeiling("maxProofSupportIds"))
        ));
    }

    #[test]
    fn merge_certificate_ceiling_is_enforced_at_the_exact_boundary() {
        let limits = DirectExactLimits {
            max_certificate_bytes: 4,
            ..DirectExactLimits::default()
        };
        assert!(enforce_certificate_size_limit(4, &limits).is_ok());
        assert!(matches!(
            enforce_certificate_size_limit(5, &limits),
            Err(DirectExactError::ResourceCeiling("maxCertificateBytes"))
        ));
    }
}
