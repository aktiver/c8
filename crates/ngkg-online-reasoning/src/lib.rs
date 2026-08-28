//! Phase 40.13.6 online OWL 2 Direct-Semantics routing and scheduling contracts.
//!
//! The input to this crate is already ontology-grounded RDF. It deliberately contains no
//! vocabulary matching, lexical similarity, embeddings, candidate correspondences, or ontology
//! alignment jobs. Its first boundary is authorization: only asserted `*/semkg` ontology modules
//! may enter an exact OWL snapshot. Closure and provenance graphs remain derived acceleration and
//! evidence artifacts.

use std::collections::{BTreeMap, BTreeSet};

use futures::{StreamExt, TryStreamExt, stream};
use ngkg_types::{
    DirectBgpLegalityRecord, DirectBgpLegalityStatus, DirectBgpResult, DirectCertificate,
    DirectExactPartition, DirectExactPartitionResult, DirectExactRequest, DirectProofManifest,
    direct_bgp_result_sha256, validate_direct_exact_partition_result,
};
use ngkg_direct_reasoner::{
    DirectExactAdapter, DirectExactBindings, DirectExactError, DirectExactLimits,
    DirectExactOntologyBundle, direct_exact_request_set_sha256,
    merge_partition_results,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use uuid::Uuid;

mod algebra;

pub use algebra::{ExactAlgebraError, substitute_exact_bgp_results};

/// Stable format identifier for the online snapshot and partition contracts.
pub const ONLINE_DIRECT_FORMAT_VERSION: u32 = 1;
/// Exact reasoner version qualified by this phase.
pub const HERMIT_VERSION: &str = "1.4.5.519";
const SEMKG_GRAPH_PREFIX: &str = "https://c8-next-generation.io/";

/// Closed graph role under NGKG's ontology-grounded TriG convention.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum OntologyGraphRole {
    /// Authorized asserted TBox, RBox, and ABox module.
    AssertedOntology,
    /// Derived finite materialization cache; never an asserted input.
    FiniteClosure,
    /// Evidence/proof metadata; never an asserted input.
    Provenance,
}

/// One named graph made visible by the authorization and dataset resolver.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct AuthorizedGraph {
    pub graph_iri: String,
    pub content_sha256: String,
    pub authorization_labels: BTreeSet<String>,
}

/// One immutable asserted ontology module selected for exact reasoning.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct AssertedOntologyModule {
    pub graph_iri: String,
    pub content_sha256: String,
}

/// One import resolved only through an operator-pinned immutable document.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct PinnedImport {
    pub ontology_iri: String,
    pub version_iri: String,
    pub document_sha256: String,
}

/// Complete request identity for one deterministic synthetic OWL snapshot.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct OntologySnapshotBinding {
    pub format_version: u32,
    pub dataset_id: Uuid,
    pub snapshot_id: Uuid,
    pub authorized_graph_set_sha256: String,
    pub active_dataset_sha256: String,
    pub datatype_policy_sha256: String,
    pub owl_signature_sha256: String,
    pub owl_profile_qualification_sha256: String,
    pub owl_consistency_qualification_sha256: String,
    pub asserted_modules: Vec<AssertedOntologyModule>,
    pub pinned_imports: Vec<PinnedImport>,
    pub synthetic_ontology_sha256: String,
}

/// Whether an acceleration lane is independently certified complete for this exact request.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum CoverageState {
    CertifiedComplete,
    Incomplete,
    Unknown,
}

/// Fail-closed online route selected for one legal BGP.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum EntailmentRoute {
    CertifiedSemanticIndex,
    CertifiedFiniteClosure,
    ExactHermit,
    IllegalOwlDirect,
}

/// Evidence available to the route selector.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EntailmentRoutingInput {
    pub legality: DirectBgpLegalityStatus,
    pub semantic_index: CoverageState,
    pub finite_closure: CoverageState,
}

/// Immutable plan for distributing one finite candidate space across reasoner pods.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DistributedReasonerPlan {
    pub format_version: u32,
    pub dataset_id: Uuid,
    pub snapshot_id: Uuid,
    pub query_sha256: String,
    pub bgp_sha256: String,
    pub ontology_snapshot_sha256: String,
    pub candidate_binding_ceiling: u64,
    pub max_candidates_per_partition: u64,
    pub partitions: Vec<DirectExactPartition>,
    pub plan_sha256: String,
}

/// A complete set of partition outputs admitted for final exact merge.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompletePartitionSet {
    pub plan_sha256: String,
    pub candidate_space_sha256: String,
    pub candidate_binding_count: u64,
    pub checked_candidate_count: u64,
    pub results: Vec<DirectExactPartitionResult>,
}

/// Runtime observations used by an external metrics adapter/HPA or KEDA scaler.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ReasonerScalingObservation {
    pub queued_partitions: u64,
    pub estimated_axioms: u64,
    pub memory_pressure_percent: u8,
    pub p95_latency_milliseconds: u64,
    pub oldest_queue_age_milliseconds: u64,
}

/// Trusted scaling policy. It is deployment state, never supplied by a query.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReasonerScalingPolicy {
    pub min_replicas: u32,
    pub max_replicas: u32,
    pub partitions_per_replica: u64,
    pub axioms_per_replica: u64,
    pub latency_target_milliseconds: u64,
    pub backlog_age_target_milliseconds: u64,
    pub durable_queue_qualified: bool,
    pub cold_start_slo_qualified: bool,
}

/// Exact online-reasoning contract failure.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum OnlineReasoningError {
    #[error("graph IRI is outside the NGKG asserted/closure/provenance convention")]
    GraphConvention,
    #[error("an asserted ontology graph is not authorized")]
    UnauthorizedAssertedGraph,
    #[error("snapshot identity or hash binding is invalid")]
    SnapshotBinding,
    #[error("pinned imports are ambiguous, mutable, or not canonical")]
    PinnedImports,
    #[error("distributed reasoner limits or plan are invalid")]
    PartitionPlan,
    #[error("a reasoner partition is missing, duplicated, invalid, or mismatched")]
    PartitionSet,
    #[error("reasoner scaling policy is invalid")]
    ScalingPolicy,
    #[error("distributed reasoner worker pool or concurrency is invalid")]
    WorkerPool,
    #[error("a reasoner worker failed, timed out, overflowed, or returned invalid bytes")]
    WorkerResponse,
    #[error("an index or closure answer differs from exact HermiT")]
    CoverageMismatch,
}

/// Classify one graph without guessing from its RDF contents.
pub fn ontology_graph_role(graph_iri: &str) -> Result<OntologyGraphRole, OnlineReasoningError> {
    let suffix = graph_iri
        .strip_prefix(SEMKG_GRAPH_PREFIX)
        .ok_or(OnlineReasoningError::GraphConvention)?;
    let mut parts = suffix.split('/');
    let scope = parts.next().filter(|value| !value.is_empty());
    let subdomain = parts.next().filter(|value| !value.is_empty());
    let role = parts.next();
    if scope.is_none() || subdomain.is_none() || parts.next().is_some() {
        return Err(OnlineReasoningError::GraphConvention);
    }
    match role {
        Some("semkg") => Ok(OntologyGraphRole::AssertedOntology),
        Some("closure") => Ok(OntologyGraphRole::FiniteClosure),
        Some("provenance") => Ok(OntologyGraphRole::Provenance),
        _ => Err(OnlineReasoningError::GraphConvention),
    }
}

/// Select asserted ontology modules from an already authorized, ontology-grounded dataset.
///
/// Closure and provenance graphs are intentionally ignored. If the caller asks to include a
/// `semkg` graph not present in the authorized set, selection fails instead of silently omitting
/// it. This operation is ontology loading, not ontology alignment.
pub fn select_authorized_asserted_modules(
    visible_graphs: &[AuthorizedGraph],
    requested_graph_iris: &BTreeSet<String>,
    principal_labels: &BTreeSet<String>,
) -> Result<Vec<AssertedOntologyModule>, OnlineReasoningError> {
    let by_iri = visible_graphs
        .iter()
        .map(|graph| (graph.graph_iri.as_str(), graph))
        .collect::<BTreeMap<_, _>>();
    let mut modules = Vec::new();
    for graph_iri in requested_graph_iris {
        if ontology_graph_role(graph_iri)? != OntologyGraphRole::AssertedOntology {
            continue;
        }
        let graph = by_iri
            .get(graph_iri.as_str())
            .ok_or(OnlineReasoningError::UnauthorizedAssertedGraph)?;
        if !graph.authorization_labels.is_subset(principal_labels)
            || !is_sha256(&graph.content_sha256)
        {
            return Err(OnlineReasoningError::UnauthorizedAssertedGraph);
        }
        modules.push(AssertedOntologyModule {
            graph_iri: graph.graph_iri.clone(),
            content_sha256: graph.content_sha256.clone(),
        });
    }
    modules.sort_by(|left, right| left.graph_iri.cmp(&right.graph_iri));
    if modules.is_empty()
        || modules
            .windows(2)
            .any(|window| window[0].graph_iri == window[1].graph_iri)
    {
        return Err(OnlineReasoningError::UnauthorizedAssertedGraph);
    }
    Ok(modules)
}

/// Validate the complete deterministic synthetic-ontology identity.
pub fn validate_ontology_snapshot_binding(
    binding: &OntologySnapshotBinding,
) -> Result<(), OnlineReasoningError> {
    if binding.format_version != ONLINE_DIRECT_FORMAT_VERSION
        || binding.dataset_id.is_nil()
        || binding.snapshot_id.is_nil()
        || binding.asserted_modules.is_empty()
        || [
            binding.authorized_graph_set_sha256.as_str(),
            binding.active_dataset_sha256.as_str(),
            binding.datatype_policy_sha256.as_str(),
            binding.owl_signature_sha256.as_str(),
            binding.owl_profile_qualification_sha256.as_str(),
            binding.owl_consistency_qualification_sha256.as_str(),
            binding.synthetic_ontology_sha256.as_str(),
        ]
        .into_iter()
        .any(|value| !is_sha256(value))
    {
        return Err(OnlineReasoningError::SnapshotBinding);
    }
    if binding.asserted_modules.iter().any(|module| {
        ontology_graph_role(&module.graph_iri) != Ok(OntologyGraphRole::AssertedOntology)
            || !is_sha256(&module.content_sha256)
    }) || !binding
        .asserted_modules
        .windows(2)
        .all(|window| window[0].graph_iri < window[1].graph_iri)
    {
        return Err(OnlineReasoningError::SnapshotBinding);
    }
    if binding.pinned_imports.iter().any(|import| {
        import.ontology_iri.is_empty()
            || import.version_iri.is_empty()
            || import.ontology_iri == import.version_iri
            || !is_sha256(&import.document_sha256)
    }) || !binding
        .pinned_imports
        .windows(2)
        .all(|window| window[0].ontology_iri < window[1].ontology_iri)
    {
        return Err(OnlineReasoningError::PinnedImports);
    }
    Ok(())
}

/// Select the fastest lane whose completeness is independently certified.
#[must_use]
pub const fn route_entailment(input: EntailmentRoutingInput) -> EntailmentRoute {
    if matches!(input.legality, DirectBgpLegalityStatus::Illegal) {
        EntailmentRoute::IllegalOwlDirect
    } else if matches!(input.semantic_index, CoverageState::CertifiedComplete) {
        EntailmentRoute::CertifiedSemanticIndex
    } else if matches!(input.finite_closure, CoverageState::CertifiedComplete) {
        EntailmentRoute::CertifiedFiniteClosure
    } else {
        // Incomplete and unknown are not false. Both require exact reasoning.
        EntailmentRoute::ExactHermit
    }
}

/// Admit a semantic-index or finite-closure completeness claim only when its exact SPARQL
/// multiset relation is byte-independently equal to the HermiT oracle result.
pub fn require_acceleration_equivalence(
    accelerated: &DirectBgpResult,
    exact: &DirectBgpResult,
) -> Result<(), OnlineReasoningError> {
    let accelerated_sha256 = direct_bgp_result_sha256(accelerated)
        .map_err(|_| OnlineReasoningError::CoverageMismatch)?;
    let exact_sha256 = direct_bgp_result_sha256(exact)
        .map_err(|_| OnlineReasoningError::CoverageMismatch)?;
    if accelerated.dataset_id != exact.dataset_id
        || accelerated.snapshot_id != exact.snapshot_id
        || accelerated.query_sha256 != exact.query_sha256
        || accelerated.bgp_sha256 != exact.bgp_sha256
        || accelerated.active_dataset_sha256 != exact.active_dataset_sha256
        || accelerated.authorized_graph_set_sha256 != exact.authorized_graph_set_sha256
        || accelerated.entailment_regime != exact.entailment_regime
        || accelerated_sha256 != exact_sha256
    {
        return Err(OnlineReasoningError::CoverageMismatch);
    }
    Ok(())
}

/// Build a deterministic finite candidate partition plan independent of pod count.
pub fn build_distributed_reasoner_plan(
    dataset_id: Uuid,
    snapshot_id: Uuid,
    query_sha256: String,
    bgp_sha256: String,
    ontology_snapshot_sha256: String,
    candidate_binding_ceiling: u64,
    max_candidates_per_partition: u64,
    max_partitions: u32,
) -> Result<DistributedReasonerPlan, OnlineReasoningError> {
    if dataset_id.is_nil()
        || snapshot_id.is_nil()
        || !is_sha256(&query_sha256)
        || !is_sha256(&bgp_sha256)
        || !is_sha256(&ontology_snapshot_sha256)
        || candidate_binding_ceiling == 0
        || max_candidates_per_partition == 0
        || max_partitions == 0
    {
        return Err(OnlineReasoningError::PartitionPlan);
    }
    let count = candidate_binding_ceiling.div_ceil(max_candidates_per_partition);
    let count = u32::try_from(count).map_err(|_| OnlineReasoningError::PartitionPlan)?;
    if count == 0 || count > max_partitions {
        return Err(OnlineReasoningError::PartitionPlan);
    }
    let partitions = (0..count)
        .map(|index| DirectExactPartition { index, count })
        .collect::<Vec<_>>();
    let mut plan = DistributedReasonerPlan {
        format_version: ONLINE_DIRECT_FORMAT_VERSION,
        dataset_id,
        snapshot_id,
        query_sha256,
        bgp_sha256,
        ontology_snapshot_sha256,
        candidate_binding_ceiling,
        max_candidates_per_partition,
        partitions,
        plan_sha256: String::new(),
    };
    plan.plan_sha256 = plan_hash(&plan)?;
    Ok(plan)
}

/// Admit a partition set only after every planned partition reports one complete range.
pub fn require_complete_partition_set(
    plan: &DistributedReasonerPlan,
    mut results: Vec<DirectExactPartitionResult>,
) -> Result<CompletePartitionSet, OnlineReasoningError> {
    if plan.plan_sha256 != plan_hash(plan)? || results.len() != plan.partitions.len() {
        return Err(OnlineReasoningError::PartitionSet);
    }
    results.sort_by_key(|result| result.partition.index);
    let first = results.first().ok_or(OnlineReasoningError::PartitionSet)?;
    let mut cursor = 0_u64;
    let mut checked = 0_u64;
    for (expected, result) in plan.partitions.iter().zip(&results) {
        validate_direct_exact_partition_result(result)
            .map_err(|_| OnlineReasoningError::PartitionSet)?;
        if result.dataset_id != plan.dataset_id
            || result.snapshot_id != plan.snapshot_id
            || result.query_sha256 != plan.query_sha256
            || result.bgp_sha256 != plan.bgp_sha256
            || result.partition != *expected
            || result.reasoner_name != "HermiT"
            || result.reasoner_version != HERMIT_VERSION
            || result.candidate_binding_count != first.candidate_binding_count
            || result.candidate_space_sha256 != first.candidate_space_sha256
            || result.partition_start_ordinal != cursor
        {
            return Err(OnlineReasoningError::PartitionSet);
        }
        cursor = result.partition_end_ordinal_exclusive;
        checked = checked
            .checked_add(result.checked_candidate_count)
            .ok_or(OnlineReasoningError::PartitionSet)?;
    }
    if cursor != first.candidate_binding_count || checked != first.candidate_binding_count {
        return Err(OnlineReasoningError::PartitionSet);
    }
    Ok(CompletePartitionSet {
        plan_sha256: plan.plan_sha256.clone(),
        candidate_space_sha256: first.candidate_space_sha256.clone(),
        candidate_binding_count: first.candidate_binding_count,
        checked_candidate_count: checked,
        results,
    })
}

/// Apply the distributed completeness barrier and then construct the exact proof/certificate
/// bundle with the same merger used by local HermiT execution.
pub fn complete_distributed_exact_bgp(
    plan: &DistributedReasonerPlan,
    requests: &[DirectExactRequest],
    results: Vec<DirectExactPartitionResult>,
    bindings: &DirectExactBindings,
    legality: &DirectBgpLegalityRecord,
    ontology: &DirectExactOntologyBundle,
    adapter: &DirectExactAdapter,
    limits: &DirectExactLimits,
) -> Result<(DirectBgpResult, DirectCertificate, DirectProofManifest), DirectExactError> {
    let complete = require_complete_partition_set(plan, results)
        .map_err(|_| DirectExactError::PartitionMismatch)?;
    if requests.len() != plan.partitions.len()
        || requests.iter().zip(&plan.partitions).any(|(request, partition)| {
            request.dataset_id != plan.dataset_id
                || request.snapshot_id != plan.snapshot_id
                || request.query_sha256 != plan.query_sha256
                || request.bgp_sha256 != plan.bgp_sha256
                || request.aggregate_input_sha256 != plan.ontology_snapshot_sha256
                || request.partition != *partition
        })
    {
        return Err(DirectExactError::PartitionMismatch);
    }
    let request_set_sha256 = direct_exact_request_set_sha256(requests)?;
    merge_partition_results(
        complete.results,
        bindings,
        legality,
        ontology,
        adapter,
        &request_set_sha256,
        limits,
    )
}

/// Dispatch immutable exact partitions across ready reasoner pod addresses with bounded
/// concurrency and response bytes. Any transport error, non-success status, missing response, or
/// identity mismatch aborts the entire set; callers must not merge a partial vector.
pub async fn dispatch_exact_partitions(
    client: &reqwest::Client,
    worker_base_urls: &[String],
    bearer_token: &str,
    requests: Vec<DirectExactRequest>,
    max_concurrency: usize,
    max_response_bytes: usize,
) -> Result<Vec<DirectExactPartitionResult>, OnlineReasoningError> {
    dispatch_exact_partitions_with_retry(
        client,
        worker_base_urls,
        bearer_token,
        requests,
        max_concurrency,
        max_response_bytes,
        1,
    )
    .await
}

/// Dispatch immutable partitions with bounded at-least-once retry.
///
/// A ClusterIP service may be the single configured URL: Kubernetes then balances concurrent
/// immutable requests over the ready reasoner pods. Retries rotate explicit pod URLs when more
/// than one is supplied. Each accepted response must match the checksum of the exact request bytes;
/// duplicate delivery is harmless because only one identity-equal result per partition is merged.
pub async fn dispatch_exact_partitions_with_retry(
    client: &reqwest::Client,
    worker_base_urls: &[String],
    bearer_token: &str,
    requests: Vec<DirectExactRequest>,
    max_concurrency: usize,
    max_response_bytes: usize,
    max_attempts: usize,
) -> Result<Vec<DirectExactPartitionResult>, OnlineReasoningError> {
    if worker_base_urls.is_empty()
        || bearer_token.is_empty()
        || requests.is_empty()
        || max_concurrency == 0
        || max_response_bytes == 0
        || max_attempts == 0
    {
        return Err(OnlineReasoningError::WorkerPool);
    }
    let response_total = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let jobs = requests.into_iter().enumerate().map(|(ordinal, request)| {
        let client = client.clone();
        let worker_base_urls = worker_base_urls.to_vec();
        let token = bearer_token.to_owned();
        let response_total = std::sync::Arc::clone(&response_total);
        async move {
            let expected_request_sha256 = hex::encode(Sha256::digest(
                serde_json::to_vec_pretty(&request)
                    .map_err(|_| OnlineReasoningError::WorkerResponse)?,
            ));
            let mut accepted = None;
            for attempt in 0..max_attempts {
                let base = worker_base_urls[(ordinal + attempt) % worker_base_urls.len()]
                    .trim_end_matches('/');
                let response = client
                    .post(format!("{base}/v1/direct/partitions/execute"))
                    .bearer_auth(&token)
                    .json(&request)
                    .send()
                    .await;
                let Ok(response) = response else {
                    continue;
                };
                if !response.status().is_success() {
                    continue;
                }
                let bytes = response
                    .bytes_stream()
                    .map_err(|_| OnlineReasoningError::WorkerResponse)
                    .try_fold(Vec::new(), |mut output, chunk| async move {
                        if output
                            .len()
                            .checked_add(chunk.len())
                            .is_none_or(|length| length > max_response_bytes)
                        {
                            return Err(OnlineReasoningError::WorkerResponse);
                        }
                        output.extend_from_slice(&chunk);
                        Ok(output)
                    })
                    .await;
                let Ok(bytes) = bytes else {
                    continue;
                };
                let result = serde_json::from_slice::<DirectExactPartitionResult>(&bytes)
                    .map_err(|_| OnlineReasoningError::WorkerResponse);
                let Ok(result) = result else {
                    continue;
                };
                if validate_direct_exact_partition_result(&result).is_err()
                    || result.dataset_id != request.dataset_id
                    || result.snapshot_id != request.snapshot_id
                    || result.query_sha256 != request.query_sha256
                    || result.bgp_sha256 != request.bgp_sha256
                    || result.partition != request.partition
                    || result.aggregate_input_sha256 != request.aggregate_input_sha256
                    || result.request_sha256 != expected_request_sha256
                {
                    continue;
                }
                accepted = Some((result, bytes.len()));
                break;
            }
            let (result, response_bytes) =
                accepted.ok_or(OnlineReasoningError::WorkerResponse)?;
            response_total
                .fetch_update(
                    std::sync::atomic::Ordering::AcqRel,
                    std::sync::atomic::Ordering::Acquire,
                    |current| {
                        current
                            .checked_add(response_bytes)
                            .filter(|total| *total <= max_response_bytes)
                    },
                )
                .map_err(|_| OnlineReasoningError::WorkerResponse)?;
            Ok(result)
        }
    });
    let mut results = stream::iter(jobs)
        .buffer_unordered(max_concurrency)
        .try_collect::<Vec<_>>()
        .await?;
    results.sort_by_key(|result| result.partition.index);
    Ok(results)
}

/// Compute a bounded desired replica count from workload rather than CPU alone.
pub fn desired_reasoner_replicas(
    policy: ReasonerScalingPolicy,
    observation: ReasonerScalingObservation,
) -> Result<u32, OnlineReasoningError> {
    if policy.max_replicas == 0
        || policy.min_replicas > policy.max_replicas
        || policy.partitions_per_replica == 0
        || policy.axioms_per_replica == 0
        || policy.latency_target_milliseconds == 0
        || policy.backlog_age_target_milliseconds == 0
        || observation.memory_pressure_percent > 100
        || (policy.min_replicas == 0
            && !(policy.durable_queue_qualified && policy.cold_start_slo_qualified))
    {
        return Err(OnlineReasoningError::ScalingPolicy);
    }
    if observation.queued_partitions == 0
        && observation.estimated_axioms == 0
        && observation.oldest_queue_age_milliseconds == 0
    {
        return Ok(policy.min_replicas);
    }
    let queue = observation
        .queued_partitions
        .div_ceil(policy.partitions_per_replica);
    let axioms = observation
        .estimated_axioms
        .div_ceil(policy.axioms_per_replica);
    let latency = observation
        .p95_latency_milliseconds
        .div_ceil(policy.latency_target_milliseconds);
    let age = observation
        .oldest_queue_age_milliseconds
        .div_ceil(policy.backlog_age_target_milliseconds);
    let memory = u64::from(observation.memory_pressure_percent >= 80);
    let desired = queue.max(axioms).max(latency).max(age).max(memory).max(1);
    Ok(u32::try_from(desired)
        .unwrap_or(u32::MAX)
        .clamp(policy.min_replicas, policy.max_replicas))
}

fn plan_hash(plan: &DistributedReasonerPlan) -> Result<String, OnlineReasoningError> {
    let mut value = plan.clone();
    value.plan_sha256.clear();
    let bytes = serde_json::to_vec(&value).map_err(|_| OnlineReasoningError::PartitionPlan)?;
    let mut hash = Sha256::new();
    hash.update(b"ngkg-online-direct-reasoner-plan-v1\0");
    hash.update(bytes);
    Ok(hex_lower(hash.finalize().as_slice()))
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn hex_lower(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(char::from(DIGITS[usize::from(byte >> 4)]));
        output.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sha(ch: char) -> String {
        std::iter::repeat_n(ch, 64).collect()
    }

    #[test]
    fn closure_and_provenance_never_enter_asserted_snapshot(
    ) -> Result<(), OnlineReasoningError> {
        let labels = BTreeSet::from(["finance".to_owned()]);
        let graphs = [
            AuthorizedGraph {
                graph_iri: "https://c8-next-generation.io/acme/orders/semkg".to_owned(),
                content_sha256: sha('a'),
                authorization_labels: labels.clone(),
            },
            AuthorizedGraph {
                graph_iri: "https://c8-next-generation.io/acme/orders/closure".to_owned(),
                content_sha256: sha('b'),
                authorization_labels: labels.clone(),
            },
            AuthorizedGraph {
                graph_iri: "https://c8-next-generation.io/acme/orders/provenance".to_owned(),
                content_sha256: sha('c'),
                authorization_labels: labels.clone(),
            },
        ];
        let requested = graphs
            .iter()
            .map(|graph| graph.graph_iri.clone())
            .collect::<BTreeSet<_>>();
        let selected = select_authorized_asserted_modules(&graphs, &requested, &labels)?;
        assert_eq!(selected.len(), 1);
        assert!(selected[0].graph_iri.ends_with("/semkg"));
        Ok(())
    }

    #[test]
    fn unknown_coverage_routes_to_exact_hermit() {
        assert_eq!(
            route_entailment(EntailmentRoutingInput {
                legality: DirectBgpLegalityStatus::Legal,
                semantic_index: CoverageState::Unknown,
                finite_closure: CoverageState::Incomplete,
            }),
            EntailmentRoute::ExactHermit
        );
    }

    #[test]
    fn illegal_direct_bgp_never_reaches_an_execution_lane() {
        assert_eq!(
            route_entailment(EntailmentRoutingInput {
                legality: DirectBgpLegalityStatus::Illegal,
                semantic_index: CoverageState::CertifiedComplete,
                finite_closure: CoverageState::CertifiedComplete,
            }),
            EntailmentRoute::IllegalOwlDirect
        );
    }

    #[test]
    fn partition_count_is_independent_of_current_replica_count(
    ) -> Result<(), OnlineReasoningError> {
        let plan = build_distributed_reasoner_plan(
            Uuid::new_v4(),
            Uuid::new_v4(),
            sha('a'),
            sha('b'),
            sha('c'),
            1_000_000,
            125_000,
            32,
        )?;
        assert_eq!(plan.partitions.len(), 8);
        assert!(plan.partitions.iter().all(|partition| partition.count == 8));
        Ok(())
    }

    #[test]
    fn missing_reasoner_partition_fails_closed() -> Result<(), OnlineReasoningError> {
        let plan = build_distributed_reasoner_plan(
            Uuid::new_v4(),
            Uuid::new_v4(),
            sha('a'),
            sha('b'),
            sha('c'),
            1_000,
            500,
            8,
        )?;
        assert_eq!(
            require_complete_partition_set(&plan, Vec::new()),
            Err(OnlineReasoningError::PartitionSet)
        );
        Ok(())
    }

    #[test]
    fn scale_to_zero_requires_durable_queue_and_cold_start_evidence() {
        let policy = ReasonerScalingPolicy {
            min_replicas: 0,
            max_replicas: 20,
            partitions_per_replica: 2,
            axioms_per_replica: 500_000,
            latency_target_milliseconds: 5_000,
            backlog_age_target_milliseconds: 2_000,
            durable_queue_qualified: false,
            cold_start_slo_qualified: true,
        };
        assert_eq!(
            desired_reasoner_replicas(
                policy,
                ReasonerScalingObservation {
                    queued_partitions: 0,
                    estimated_axioms: 0,
                    memory_pressure_percent: 0,
                    p95_latency_milliseconds: 0,
                    oldest_queue_age_milliseconds: 0,
                }
            ),
            Err(OnlineReasoningError::ScalingPolicy)
        );
    }

    #[test]
    fn workload_signals_scale_reasoners_within_bounds() -> Result<(), OnlineReasoningError> {
        let policy = ReasonerScalingPolicy {
            min_replicas: 2,
            max_replicas: 20,
            partitions_per_replica: 2,
            axioms_per_replica: 500_000,
            latency_target_milliseconds: 5_000,
            backlog_age_target_milliseconds: 2_000,
            durable_queue_qualified: false,
            cold_start_slo_qualified: false,
        };
        let desired = desired_reasoner_replicas(
            policy,
            ReasonerScalingObservation {
                queued_partitions: 13,
                estimated_axioms: 2_000_000,
                memory_pressure_percent: 82,
                p95_latency_milliseconds: 11_000,
                oldest_queue_age_milliseconds: 4_500,
            },
        )?;
        assert_eq!(desired, 7);
        Ok(())
    }
}
