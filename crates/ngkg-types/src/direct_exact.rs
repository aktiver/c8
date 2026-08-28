//! Phase 40.8 exact OWL 2 Direct-Semantics fallback transport contracts.
//!
//! These objects are internal cross-language contracts between the Rust semantic planner and the
//! checksum-pinned HermiT adapter. They are intentionally partitionable: Phase 40.8 can execute
//! independent candidate ordinal ranges in bounded local CPU lanes, while Phase 40.15 can move
//! those same immutable partitions across Kubernetes nodes without changing semantics.

use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

use crate::{DirectBgpRdfTerm, DirectBgpScope, DirectVariableRole, DirectVariableRoleSource};

pub const DIRECT_EXACT_FORMAT_VERSION: u32 = 1;
pub const DIRECT_EXACT_ENGINE_V1: &str = "hermit-grounded-owl2dl-isentailed-v1";

/// One RDF/SPARQL term position in a Direct-BGP template.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase", tag = "termType")]
pub enum DirectExactTermPattern {
    #[serde(rename = "variable")]
    Variable { name: String },
    #[serde(rename = "iri")]
    Iri { value: String },
    #[serde(rename = "blankNode")]
    BlankNode { value: String },
    #[serde(rename = "literal")]
    Literal {
        lexical_form: String,
        datatype_iri: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        language: Option<String>,
    },
}

/// One typed triple pattern retained exactly enough for Java/OWLAPI grounding.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DirectExactTriplePattern {
    pub subject: DirectExactTermPattern,
    pub predicate: DirectExactTermPattern,
    pub object: DirectExactTermPattern,
}

/// One BGP-local finite-domain variable.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DirectExactVariable {
    pub name: String,
    pub role: DirectVariableRole,
    /// Retained so the exact engine can distinguish an explicit owl:NamedIndividual declaration
    /// from an undeclared variable merely occurring in an Individual structural position.
    pub source: DirectVariableRoleSource,
}

/// Typed BGP template handed to the exact reasoner only after Phase 40.7 admission.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DirectExactBgpTemplate {
    pub ordinal: u64,
    pub bgp_sha256: String,
    pub graph_scope: DirectBgpScope,
    pub variables: Vec<DirectExactVariable>,
    pub triples: Vec<DirectExactTriplePattern>,
}

/// Exact checksum-bound ontology document used by one scoped reasoner execution.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DirectExactOntologyInput {
    pub path: String,
    pub sha256: String,
    pub ontology_iris: Vec<String>,
}

/// One deterministic candidate ordinal partition.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DirectExactPartition {
    pub index: u32,
    pub count: u32,
}

/// Request consumed by `ngkg-hermit-adapter --direct-request`.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DirectExactRequest {
    pub format_version: u32,
    pub dataset_id: Uuid,
    pub snapshot_id: Uuid,
    pub query_sha256: String,
    pub sparql_algebra_sha256: String,
    pub bgp_sha256: String,
    pub active_dataset_sha256: String,
    pub authorized_graph_set_sha256: String,
    pub owl_signature_sha256: String,
    pub datatype_policy_sha256: String,
    pub owl_profile_qualification_sha256: String,
    pub owl_consistency_qualification_sha256: String,
    pub engine: String,
    pub inputs: Vec<DirectExactOntologyInput>,
    pub aggregate_input_sha256: String,
    pub template: DirectExactBgpTemplate,
    pub partition: DirectExactPartition,
    pub max_candidate_bindings: u64,
    pub max_partition_candidates: u64,
    pub max_grounded_axioms_per_candidate: u64,
    pub max_grounded_rdf_bytes_per_candidate: u64,
    pub output_path: String,
}

/// One entailed candidate returned by a partition. Non-entailed candidates are not serialized.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DirectExactEntailedBinding {
    pub candidate_ordinal: u64,
    pub bindings: std::collections::BTreeMap<String, DirectBgpRdfTerm>,
    /// SHA-256 of the exact deterministic grounded RDF bytes parsed by OWLAPI for this candidate.
    pub grounded_rdf_sha256: String,
    /// SHA-256 over the sorted logical OWL axioms actually submitted to HermiT isEntailed.
    pub logical_axioms_sha256: String,
    pub logical_axiom_count: u64,
}

/// Exact result for one candidate partition.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DirectExactPartitionResult {
    pub format_version: u32,
    pub dataset_id: Uuid,
    pub snapshot_id: Uuid,
    pub query_sha256: String,
    pub bgp_sha256: String,
    pub engine: String,
    pub reasoner_name: String,
    pub reasoner_version: String,
    pub adapter_version: String,
    pub request_sha256: String,
    pub aggregate_input_sha256: String,
    pub candidate_space_sha256: String,
    pub partition: DirectExactPartition,
    pub candidate_binding_count: u64,
    pub partition_start_ordinal: u64,
    pub partition_end_ordinal_exclusive: u64,
    pub checked_candidate_count: u64,
    pub grounded_owl2dl_candidate_count: u64,
    pub entailed_candidate_count: u64,
    pub reasoner_request_count: u64,
    pub entailed: Vec<DirectExactEntailedBinding>,
    pub complete: bool,
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum DirectExactValidationError {
    #[error("unsupported Phase 40.8 exact-reasoner format/engine")]
    Version,
    #[error("partition is invalid")]
    Partition,
    #[error("exact-reasoner request/result identity hash is invalid: {0}")]
    Hash(&'static str),
    #[error("candidate ceilings must be positive")]
    Ceiling,
    #[error("BGP template does not match the admitted BGP")]
    Template,
    #[error("partition result counters are inconsistent")]
    Counters,
    #[error("entailed candidate ordinals or binding keys are invalid")]
    Entailed,
}

pub fn validate_direct_exact_request(request: &DirectExactRequest) -> Result<(), DirectExactValidationError> {
    if request.format_version != DIRECT_EXACT_FORMAT_VERSION || request.engine != DIRECT_EXACT_ENGINE_V1 {
        return Err(DirectExactValidationError::Version);
    }
    for (name, value) in [
        ("querySha256", request.query_sha256.as_str()),
        ("sparqlAlgebraSha256", request.sparql_algebra_sha256.as_str()),
        ("bgpSha256", request.bgp_sha256.as_str()),
        ("activeDatasetSha256", request.active_dataset_sha256.as_str()),
        ("authorizedGraphSetSha256", request.authorized_graph_set_sha256.as_str()),
        ("owlSignatureSha256", request.owl_signature_sha256.as_str()),
        ("datatypePolicySha256", request.datatype_policy_sha256.as_str()),
        ("owlProfileQualificationSha256", request.owl_profile_qualification_sha256.as_str()),
        ("owlConsistencyQualificationSha256", request.owl_consistency_qualification_sha256.as_str()),
        ("aggregateInputSha256", request.aggregate_input_sha256.as_str()),
    ] {
        if !is_lower_sha256(value) { return Err(DirectExactValidationError::Hash(name)); }
    }
    if request.partition.count == 0 || request.partition.index >= request.partition.count {
        return Err(DirectExactValidationError::Partition);
    }
    if request.max_candidate_bindings == 0 || request.max_partition_candidates == 0
        || request.max_grounded_axioms_per_candidate == 0
        || request.max_grounded_rdf_bytes_per_candidate == 0 {
        return Err(DirectExactValidationError::Ceiling);
    }
    if request.template.bgp_sha256 != request.bgp_sha256 || request.template.triples.is_empty() {
        return Err(DirectExactValidationError::Template);
    }
    Ok(())
}

pub fn validate_direct_exact_partition_result(result: &DirectExactPartitionResult) -> Result<(), DirectExactValidationError> {
    if result.format_version != DIRECT_EXACT_FORMAT_VERSION || result.engine != DIRECT_EXACT_ENGINE_V1 {
        return Err(DirectExactValidationError::Version);
    }
    for (name, value) in [("requestSha256", result.request_sha256.as_str()), ("aggregateInputSha256", result.aggregate_input_sha256.as_str()), ("candidateSpaceSha256", result.candidate_space_sha256.as_str())] {
        if !is_lower_sha256(value) { return Err(DirectExactValidationError::Hash(name)); }
    }
    if result.reasoner_name != "HermiT" || result.reasoner_version.is_empty() || result.adapter_version.is_empty() { return Err(DirectExactValidationError::Version); }
    if result.partition.count == 0 || result.partition.index >= result.partition.count
        || result.partition_start_ordinal > result.partition_end_ordinal_exclusive {
        return Err(DirectExactValidationError::Partition);
    }
    let expected = result.partition_end_ordinal_exclusive.saturating_sub(result.partition_start_ordinal);
    if !result.complete || result.checked_candidate_count != expected
        || result.grounded_owl2dl_candidate_count > result.checked_candidate_count
        || result.entailed_candidate_count > result.grounded_owl2dl_candidate_count
        || result.entailed_candidate_count != u64::try_from(result.entailed.len()).unwrap_or(u64::MAX)
        || result.reasoner_request_count != result.grounded_owl2dl_candidate_count {
        return Err(DirectExactValidationError::Counters);
    }
    let mut previous = None;
    for entailed in &result.entailed {
        if entailed.candidate_ordinal < result.partition_start_ordinal
            || entailed.candidate_ordinal >= result.partition_end_ordinal_exclusive
            || previous.is_some_and(|value| entailed.candidate_ordinal <= value)
            || entailed.bindings.keys().any(|name| name.is_empty() || name.starts_with('?') || name.starts_with('$'))
            || !is_lower_sha256(&entailed.grounded_rdf_sha256)
            || !is_lower_sha256(&entailed.logical_axioms_sha256)
        {
            return Err(DirectExactValidationError::Entailed);
        }
        previous = Some(entailed.candidate_ordinal);
    }
    Ok(())
}

fn is_lower_sha256(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}
