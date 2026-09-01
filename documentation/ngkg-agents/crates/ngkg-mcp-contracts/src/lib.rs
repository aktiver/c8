//! Versioned MCP semantic evidence contracts.
//!
//! These types adapt already-authorized NGKG responses. They do not evaluate
//! SPARQL, perform OWL reasoning, or invent proof identifiers.

use std::collections::BTreeMap;

use ngkg_api_client::{ExactEntailmentEvidence, QueryForm, QueryOutcome, QueryResponse};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use uuid::Uuid;

/// Current reasoned-context envelope format.
pub const CONTEXT_FORMAT_VERSION: u32 = 1;
const RESULT_DOMAIN: &[u8] = b"ngkg-agent-semantic-result-v1\0";
const STATEMENT_DOMAIN: &[u8] = b"ngkg-agent-context-statement-v1\0";

/// Gateway output ceilings recorded in every result.
#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct EnvelopeLimits {
    /// Maximum SELECT binding rows.
    pub maximum_rows: usize,
    /// Maximum graph statements.
    pub maximum_triples: usize,
    /// Maximum encoded semantic payload bytes.
    pub maximum_payload_bytes: usize,
}

impl Default for EnvelopeLimits {
    fn default() -> Self {
        Self {
            maximum_rows: 100_000,
            maximum_triples: 10_000,
            maximum_payload_bytes: 8_388_608,
        }
    }
}

/// Semantic trust classification derived from NGKG evidence.
#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SemanticStatus {
    /// Complete local result served by a certified route.
    CertifiedComplete,
    /// Complete local exact OWL 2 Direct-Semantics result.
    ExactComplete,
    /// Complete SPARQL result without claim-level exact proof.
    SparqlComplete,
    /// Complete SPARQL federation result outside local snapshot certification.
    FederatedVolatile,
}

/// Tool-facing query form.
#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum EnvelopeQueryForm {
    /// SPARQL SELECT.
    Select,
    /// SPARQL ASK.
    Ask,
    /// SPARQL CONSTRUCT.
    Construct,
    /// SPARQL DESCRIBE.
    Describe,
}

impl From<QueryForm> for EnvelopeQueryForm {
    fn from(value: QueryForm) -> Self {
        match value {
            QueryForm::Select => Self::Select,
            QueryForm::Ask => Self::Ask,
            QueryForm::Construct => Self::Construct,
            QueryForm::Describe => Self::Describe,
        }
    }
}

/// Bounded semantic payload preserving SPARQL bag and graph result order.
#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields, tag = "kind", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SemanticPayload {
    /// SELECT variables and ordered bag of bindings.
    Select {
        /// Variable order.
        head: Vec<String>,
        /// Ordered result bindings; duplicates remain duplicates.
        bindings: Vec<serde_json::Value>,
    },
    /// ASK Boolean.
    Ask {
        /// Boolean result.
        value: bool,
    },
    /// CONSTRUCT or DESCRIBE lexical N-Triples and local statement IDs.
    Graph {
        /// N-Triples in exact upstream result order.
        ntriples: Vec<String>,
        /// Envelope-local statement references.
        statement_ids: Vec<String>,
    },
}

/// Evidence references safe for model/tool consumption.
#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct SemanticEvidence {
    /// Routing decision.
    pub routing_mode: String,
    /// Execution mode.
    pub execution_mode: String,
    /// Optional distributed plan checksum.
    pub plan_sha256: Option<String>,
    /// Exact result checksums.
    pub exact_result_sha256s: Vec<String>,
    /// Exact certificate checksums.
    pub certificate_sha256s: Vec<String>,
    /// Exact proof-manifest checksums.
    pub proof_manifest_sha256s: Vec<String>,
    /// Validated upstream proof/support IDs only.
    pub proof_ids: Vec<String>,
    /// Property-path plan-set checksum.
    pub property_path_plan_set_sha256: Option<String>,
    /// Federation registry checksum when remote SERVICE participated.
    pub federation_registry_sha256: Option<String>,
    /// Remote endpoint-set checksum when remote SERVICE participated.
    pub federation_endpoint_set_sha256: Option<String>,
}

/// Explicit OWL interpretation metadata.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ReasoningMetadata {
    /// Declared NGKG entailment profile.
    pub profile: String,
    /// Execution mode copied from NGKG evidence.
    pub execution_mode: String,
    /// True only after all relevant barriers are complete.
    pub complete: bool,
    /// Always false under OWL open-world semantics.
    pub unknown_is_false: bool,
    /// True when remote state prevents local-snapshot OWL certification.
    pub federated: bool,
}

/// Limits actually applied to this envelope.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct AppliedLimits {
    /// Maximum rows.
    pub maximum_rows: usize,
    /// Maximum triples.
    pub maximum_triples: usize,
    /// Maximum payload bytes.
    pub maximum_payload_bytes: usize,
    /// This implementation fails instead of truncating authoritative results.
    pub truncated: bool,
}

/// Complete, snapshot-bound semantic result returned by MCP tools.
#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ReasonedContextEnvelope {
    /// Contract version.
    pub format_version: u32,
    /// Dataset ID.
    pub dataset_id: Uuid,
    /// Active snapshot ID.
    pub snapshot_id: Uuid,
    /// Immutable NGKG query ledger ID.
    pub query_execution_id: Uuid,
    /// Query-text checksum.
    pub query_sha256: String,
    /// Serving-root checksum.
    pub serving_root_sha256: String,
    /// Authorized graph-set checksum.
    pub authorized_graph_set_sha256: String,
    /// Active dataset checksum.
    pub active_dataset_sha256: String,
    /// Domain-separated checksum of the complete envelope payload/evidence.
    pub semantic_result_sha256: String,
    /// Semantic trust classification.
    pub semantic_status: SemanticStatus,
    /// SPARQL query form.
    pub query_form: EnvelopeQueryForm,
    /// Typed bounded result.
    pub payload: SemanticPayload,
    /// Proof, routing, path, and federation references.
    pub evidence: SemanticEvidence,
    /// Explicit reasoning semantics.
    pub reasoning: ReasoningMetadata,
    /// Enforced limits.
    pub limits: AppliedLimits,
}

/// Fail-closed envelope validation error.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum EnvelopeError {
    /// A configured limit is zero.
    #[error("semantic envelope limits must be positive")]
    InvalidLimits,
    /// The upstream result exceeds a hard ceiling.
    #[error("semantic result exceeds the configured {0} ceiling")]
    Limit(&'static str),
    /// Query-form fields are inconsistent.
    #[error("NGKG response fields do not match the declared query form")]
    QueryForm,
    /// Required completion evidence is inconsistent.
    #[error("NGKG completion or proof evidence is inconsistent")]
    Evidence,
    /// Result hashing failed.
    #[error("semantic result canonicalization failed")]
    Canonicalization,
}

/// Validate a public NGKG result and construct its MCP envelope.
pub fn build_reasoned_context_envelope(
    outcome: QueryOutcome,
    limits: EnvelopeLimits,
) -> Result<ReasonedContextEnvelope, EnvelopeError> {
    validate_limits(limits)?;
    let response = outcome.response;
    validate_evidence(&response)?;
    let status = semantic_status(&response)?;
    let query_form = EnvelopeQueryForm::from(response.query_form);
    let mut payload = payload(&response, limits)?;
    let evidence = semantic_evidence(&response);
    let reasoning = ReasoningMetadata {
        profile: "OWL_2_DL".to_owned(),
        execution_mode: response.execution.mode.clone(),
        complete: true,
        unknown_is_false: false,
        federated: response.federation.is_some(),
    };
    let applied = AppliedLimits {
        maximum_rows: limits.maximum_rows,
        maximum_triples: limits.maximum_triples,
        maximum_payload_bytes: limits.maximum_payload_bytes,
        truncated: false,
    };
    let result_hash = semantic_result_sha256(
        outcome.query_execution_id,
        &response,
        status,
        &payload,
        &evidence,
        &reasoning,
        &applied,
    )?;
    if let SemanticPayload::Graph {
        ntriples,
        statement_ids,
    } = &mut payload
    {
        *statement_ids = ntriples
            .iter()
            .enumerate()
            .map(|(ordinal, statement)| statement_id(&result_hash, ordinal, statement.as_bytes()))
            .collect();
    }
    Ok(ReasonedContextEnvelope {
        format_version: CONTEXT_FORMAT_VERSION,
        dataset_id: response.dataset_id,
        snapshot_id: response.snapshot_id,
        query_execution_id: outcome.query_execution_id,
        query_sha256: response.query_sha256,
        serving_root_sha256: response.serving_root_sha256,
        authorized_graph_set_sha256: response.authorized_graph_set_sha256,
        active_dataset_sha256: response.active_dataset_sha256,
        semantic_result_sha256: result_hash,
        semantic_status: status,
        query_form,
        payload,
        evidence,
        reasoning,
        limits: applied,
    })
}

fn validate_limits(limits: EnvelopeLimits) -> Result<(), EnvelopeError> {
    if limits.maximum_rows == 0 || limits.maximum_triples == 0 || limits.maximum_payload_bytes == 0
    {
        return Err(EnvelopeError::InvalidLimits);
    }
    Ok(())
}

fn validate_evidence(response: &QueryResponse) -> Result<(), EnvelopeError> {
    if !response.complete
        || response.active_dataset_sha256 != response.routing.active_dataset_sha256
        || response
            .entailment
            .as_ref()
            .is_some_and(|item| !item.complete || item.regime != "owl2-direct")
        || response
            .property_path_execution
            .as_ref()
            .is_some_and(|item| !item.complete)
        || response
            .federation
            .as_ref()
            .is_some_and(|item| !item.complete)
    {
        return Err(EnvelopeError::Evidence);
    }
    Ok(())
}

fn semantic_status(response: &QueryResponse) -> Result<SemanticStatus, EnvelopeError> {
    if response.federation.is_some() {
        return Ok(SemanticStatus::FederatedVolatile);
    }
    if response.entailment.is_some() {
        return Ok(SemanticStatus::ExactComplete);
    }
    if matches!(
        response.execution.mode.as_str(),
        "certified_local_route"
            | "certified_distributed_fragments"
            | "certified_partitioned_shuffle"
    ) {
        return Ok(SemanticStatus::CertifiedComplete);
    }
    if response.execution.mode.is_empty() {
        return Err(EnvelopeError::Evidence);
    }
    Ok(SemanticStatus::SparqlComplete)
}

fn payload(
    response: &QueryResponse,
    limits: EnvelopeLimits,
) -> Result<SemanticPayload, EnvelopeError> {
    let payload = match response.query_form {
        QueryForm::Select => {
            if response.bindings.len() > limits.maximum_rows
                || response.boolean_result.is_some()
                || !response.graph_ntriples.is_empty()
            {
                return Err(EnvelopeError::QueryForm);
            }
            SemanticPayload::Select {
                head: response.head.clone(),
                bindings: response.bindings.clone(),
            }
        }
        QueryForm::Ask => {
            if !response.bindings.is_empty() || !response.graph_ntriples.is_empty() {
                return Err(EnvelopeError::QueryForm);
            }
            SemanticPayload::Ask {
                value: response.boolean_result.ok_or(EnvelopeError::QueryForm)?,
            }
        }
        QueryForm::Construct | QueryForm::Describe => {
            if !response.bindings.is_empty()
                || response.boolean_result.is_some()
                || response.graph_ntriples.len() > limits.maximum_triples
                || response
                    .graph_ntriples
                    .iter()
                    .any(|line| line.is_empty() || !line.trim_end().ends_with('.'))
            {
                return Err(EnvelopeError::QueryForm);
            }
            SemanticPayload::Graph {
                ntriples: response.graph_ntriples.clone(),
                statement_ids: Vec::new(),
            }
        }
    };
    let bytes = serde_json::to_vec(&payload).map_err(|_| EnvelopeError::Canonicalization)?;
    if bytes.len() > limits.maximum_payload_bytes {
        return Err(EnvelopeError::Limit("payload-byte"));
    }
    Ok(payload)
}

fn semantic_evidence(response: &QueryResponse) -> SemanticEvidence {
    let (exact_result_sha256s, certificate_sha256s, proof_manifest_sha256s, proof_ids) =
        response.entailment.as_ref().map_or_else(
            || (Vec::new(), Vec::new(), Vec::new(), Vec::new()),
            |exact| {
                (
                    exact.result_sha256s.clone(),
                    exact.certificate_sha256s.clone(),
                    exact.proof_manifest_sha256s.clone(),
                    proof_ids(exact),
                )
            },
        );
    SemanticEvidence {
        routing_mode: response.routing.selection_mode.clone(),
        execution_mode: response.execution.mode.clone(),
        plan_sha256: response.execution.plan_sha256.clone(),
        exact_result_sha256s,
        certificate_sha256s,
        proof_manifest_sha256s,
        proof_ids,
        property_path_plan_set_sha256: response
            .property_path_execution
            .as_ref()
            .map(|value| value.plan_set_sha256.clone()),
        federation_registry_sha256: response
            .federation
            .as_ref()
            .map(|value| value.registry_sha256.clone()),
        federation_endpoint_set_sha256: response
            .federation
            .as_ref()
            .map(|value| value.endpoint_set_sha256.clone()),
    }
}

fn proof_ids(exact: &ExactEntailmentEvidence) -> Vec<String> {
    let mut ids = Vec::new();
    for manifest in &exact.proof_manifests {
        if let Some(value) = manifest
            .get("completionSupportId")
            .and_then(serde_json::Value::as_str)
        {
            ids.push(value.to_owned());
        }
        if let Some(proofs) = manifest
            .get("answerProofs")
            .and_then(serde_json::Value::as_array)
        {
            for proof in proofs {
                if let Some(value) = proof.get("supportId").and_then(serde_json::Value::as_str) {
                    ids.push(value.to_owned());
                }
            }
        }
    }
    ids.sort();
    ids.dedup();
    ids
}

#[allow(clippy::too_many_arguments)]
fn semantic_result_sha256(
    query_execution_id: Uuid,
    response: &QueryResponse,
    status: SemanticStatus,
    payload: &SemanticPayload,
    evidence: &SemanticEvidence,
    reasoning: &ReasoningMetadata,
    limits: &AppliedLimits,
) -> Result<String, EnvelopeError> {
    let mut digest = Sha256::new();
    digest.update(RESULT_DOMAIN);
    digest.update(CONTEXT_FORMAT_VERSION.to_be_bytes());
    digest.update(response.dataset_id.as_bytes());
    digest.update(response.snapshot_id.as_bytes());
    digest.update(query_execution_id.as_bytes());
    update_string(&mut digest, &response.serving_root_sha256);
    update_string(&mut digest, &response.query_sha256);
    update_string(&mut digest, &response.authorized_graph_set_sha256);
    update_string(&mut digest, &response.active_dataset_sha256);
    update_json(&mut digest, &status)?;
    update_json(&mut digest, &EnvelopeQueryForm::from(response.query_form))?;
    update_json(&mut digest, payload)?;
    update_json(&mut digest, evidence)?;
    update_json(&mut digest, reasoning)?;
    update_json(&mut digest, limits)?;
    Ok(hex::encode(digest.finalize()))
}

fn statement_id(result_sha256: &str, ordinal: usize, statement: &[u8]) -> String {
    let mut digest = Sha256::new();
    digest.update(STATEMENT_DOMAIN);
    update_bytes(&mut digest, result_sha256.as_bytes());
    digest.update(u64::try_from(ordinal).unwrap_or(u64::MAX).to_be_bytes());
    update_bytes(&mut digest, statement);
    format!("sha256:{}", hex::encode(digest.finalize()))
}

fn update_json<T: Serialize>(digest: &mut Sha256, value: &T) -> Result<(), EnvelopeError> {
    let value = serde_json::to_value(value).map_err(|_| EnvelopeError::Canonicalization)?;
    update_canonical_value(digest, &value);
    Ok(())
}

fn update_canonical_value(digest: &mut Sha256, value: &serde_json::Value) {
    match value {
        serde_json::Value::Null => digest.update([0]),
        serde_json::Value::Bool(item) => digest.update([1, u8::from(*item)]),
        serde_json::Value::Number(item) => {
            digest.update([2]);
            update_string(digest, &item.to_string());
        }
        serde_json::Value::String(item) => {
            digest.update([3]);
            update_string(digest, item);
        }
        serde_json::Value::Array(items) => {
            digest.update([4]);
            digest.update(u64::try_from(items.len()).unwrap_or(u64::MAX).to_be_bytes());
            for item in items {
                update_canonical_value(digest, item);
            }
        }
        serde_json::Value::Object(items) => {
            digest.update([5]);
            let ordered = items.iter().collect::<BTreeMap<_, _>>();
            digest.update(
                u64::try_from(ordered.len())
                    .unwrap_or(u64::MAX)
                    .to_be_bytes(),
            );
            for (key, item) in ordered {
                update_string(digest, key);
                update_canonical_value(digest, item);
            }
        }
    }
}

fn update_string(digest: &mut Sha256, value: &str) {
    update_bytes(digest, value.as_bytes());
}

fn update_bytes(digest: &mut Sha256, value: &[u8]) {
    digest.update(u64::try_from(value.len()).unwrap_or(u64::MAX).to_be_bytes());
    digest.update(value);
}

#[cfg(test)]
mod tests {
    use ngkg_api_client::{
        ExecutionEvidence, QueryForm, QueryOutcome, QueryResponse, RoutingEvidence,
    };
    use uuid::Uuid;

    use super::{EnvelopeLimits, SemanticPayload, SemanticStatus, build_reasoned_context_envelope};

    fn response(form: QueryForm) -> QueryResponse {
        QueryResponse {
            dataset_id: Uuid::from_u128(1),
            snapshot_id: Uuid::from_u128(2),
            serving_root_sha256: "1".repeat(64),
            query_sha256: "2".repeat(64),
            query_form: form,
            authorized_graph_set_sha256: "3".repeat(64),
            active_dataset_sha256: "4".repeat(64),
            coverage_scope: "test".to_owned(),
            complete: true,
            routing: RoutingEvidence {
                selection_mode: "typed_active_dataset".to_owned(),
                dataset_selection_source: "service_default".to_owned(),
                default_graph_iris: Vec::new(),
                named_graph_iris: Vec::new(),
                active_dataset_sha256: "4".repeat(64),
                include_internal_closure: false,
                selected_graph_iris: Vec::new(),
                selected_graph_count: 0,
                total_graph_count: 1,
                capability_index_sha256: "5".repeat(64),
                routed_dataset_sha256: "6".repeat(64),
            },
            execution: ExecutionEvidence {
                mode: "certified_local_route".to_owned(),
                exchange_format: "none".to_owned(),
                fragment_ingress_mode: "none".to_owned(),
                fragment_ingress_bytes: 0,
                fragment_materialization_mode: "none".to_owned(),
                fragment_owned_rows: 0,
                shuffle_result_ingress_mode: "none".to_owned(),
                shuffle_result_ingress_bytes: 0,
                intermediate_result_mode: "none".to_owned(),
                assembled_intermediate_owned_rows: 0,
                fragment_count: 0,
                worker_count: 0,
                shuffle_partition_count: 0,
                shuffle_worker_count: 0,
                shuffle_spill_mode: "none".to_owned(),
                shuffle_spill_bytes: 0,
                shuffle_cache_mode: "none".to_owned(),
                shuffle_cache_hits: 0,
                worker_join_mode: "none".to_owned(),
                worker_join_spill_bytes: 0,
                worker_join_grace_partitions: 0,
                worker_join_max_build_rows: 0,
                worker_input_mode: "none".to_owned(),
                worker_input_bytes: 0,
                coordinator_request_mode: "none".to_owned(),
                coordinator_request_bytes: 0,
                plan_sha256: None,
            },
            head: Vec::new(),
            bindings: Vec::new(),
            boolean_result: None,
            graph_ntriples: Vec::new(),
            qualified_entities: Vec::new(),
            hydrated_payload: Vec::new(),
            entailment: None,
            property_path_execution: None,
            federation: None,
        }
    }

    #[test]
    fn graph_statement_ids_are_result_local_and_stable() -> Result<(), super::EnvelopeError> {
        let mut value = response(QueryForm::Construct);
        value.graph_ntriples = vec!["<urn:s> <urn:p> <urn:o> .".to_owned()];
        let execution = Uuid::from_u128(3);
        let first = build_reasoned_context_envelope(
            QueryOutcome {
                query_execution_id: execution,
                response: value.clone(),
            },
            EnvelopeLimits::default(),
        )?;
        let second = build_reasoned_context_envelope(
            QueryOutcome {
                query_execution_id: execution,
                response: value,
            },
            EnvelopeLimits::default(),
        )?;
        assert_eq!(first.semantic_result_sha256, second.semantic_result_sha256);
        assert!(matches!(&first.payload, SemanticPayload::Graph { .. }));
        let statement_ids = match first.payload {
            SemanticPayload::Graph { statement_ids, .. } => statement_ids,
            SemanticPayload::Select { .. } | SemanticPayload::Ask { .. } => Vec::new(),
        };
        assert_eq!(statement_ids.len(), 1);
        assert!(statement_ids[0].starts_with("sha256:"));
        Ok(())
    }

    #[test]
    fn certified_local_route_is_classified_strictly() -> Result<(), super::EnvelopeError> {
        let mut value = response(QueryForm::Ask);
        value.boolean_result = Some(true);
        let envelope = build_reasoned_context_envelope(
            QueryOutcome {
                query_execution_id: Uuid::from_u128(3),
                response: value,
            },
            EnvelopeLimits::default(),
        )?;
        assert_eq!(envelope.semantic_status, SemanticStatus::CertifiedComplete);
        assert!(!envelope.reasoning.unknown_is_false);
        Ok(())
    }

    #[test]
    fn authoritative_results_are_rejected_instead_of_truncated() {
        let mut value = response(QueryForm::Select);
        value.head = vec!["s".to_owned()];
        value.bindings = vec![
            serde_json::json!({"s": "urn:one"}),
            serde_json::json!({"s": "urn:two"}),
        ];
        let result = build_reasoned_context_envelope(
            QueryOutcome {
                query_execution_id: Uuid::from_u128(3),
                response: value,
            },
            EnvelopeLimits {
                maximum_rows: 1,
                ..EnvelopeLimits::default()
            },
        );
        assert!(result.is_err());
    }
}
