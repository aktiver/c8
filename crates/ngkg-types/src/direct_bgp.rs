//! Phase 40.3 exact Direct-BGP result contract.
//!
//! This module defines the closed runtime object that later OWL Direct legality and exact
//! reasoner phases populate. Phase 40.3 does not itself claim that arbitrary BGPs are legal or
//! complete; it makes any result that crosses that future boundary snapshot-, dataset-, graph-,
//! policy-, and RDF-term-exact and independently validateable.

use std::{collections::{BTreeMap, BTreeSet}, thread};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

use crate::EntailmentRegime;

const FORMAT_VERSION: u32 = 1;
const MAX_VALIDATION_LANES: usize = 32;
const RDF_LANG_STRING: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString";

/// Active RDF graph against which the OWL Direct BGP leaf was evaluated.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase", tag = "scope")]
pub enum DirectBgpGraphContext {
    /// The query's active default graph; its exact bytes/meaning are bound by this hash.
    #[serde(rename = "default")]
    Default {
        active_default_graph_sha256: String,
    },
    /// One independently queryable named graph.
    #[serde(rename = "named")]
    Named { graph_iri: String },
}

/// Exact RDF term identity carried by a Direct-BGP solution mapping.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase", tag = "termType")]
pub enum DirectBgpRdfTerm {
    /// Absolute RDF IRI.
    #[serde(rename = "iri")]
    Iri { value: String },
    /// Dataset-scoped RDF blank-node identifier.
    #[serde(rename = "blankNode")]
    BlankNode { value: String },
    /// RDF literal with an explicit datatype IRI and optional language tag.
    #[serde(rename = "literal")]
    Literal {
        lexical_form: String,
        datatype_iri: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        language: Option<String>,
    },
}

/// One distinct SPARQL solution mapping compressed with exact bag multiplicity.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DirectBgpSolution {
    /// Variable name without `?` or `$` mapped to one exact RDF term.
    pub bindings: BTreeMap<String, DirectBgpRdfTerm>,
    /// Number of identical mappings in the exact SPARQL multiset.
    pub multiplicity: u64,
}

/// Whether the result object represents a successful exact answer or a fail-closed attempt.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum DirectBgpStatus {
    Complete,
    Failed,
}

/// Exactness evidence carried by the result.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum DirectBgpExactness {
    Exact,
    NotEstablished,
}

/// Completeness evidence carried by the result.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum DirectBgpCompleteness {
    Complete,
    Incomplete,
    NotEstablished,
}

/// Closed outcome tuple. Successful results must be both exact and complete.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DirectBgpOutcome {
    pub status: DirectBgpStatus,
    pub exactness: DirectBgpExactness,
    pub completeness: DirectBgpCompleteness,
}

/// Stable machine-readable failure families for an unsuccessful Direct-BGP attempt.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum DirectBgpFailureCode {
    IllegalBgp,
    InconsistentOntology,
    UnsupportedDatatype,
    ResourceExhausted,
    Timeout,
    ReasonerFailure,
    IntegrityFailure,
    NotCovered,
}

/// Bounded diagnostic for a fail-closed Direct-BGP attempt.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DirectBgpFailure {
    pub code: DirectBgpFailureCode,
    pub retryable: bool,
    pub detail: String,
}

/// Snapshot-bound, graph-sensitive exact result contract for one OWL Direct BGP leaf.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DirectBgpResult {
    pub format_version: u32,
    pub dataset_id: Uuid,
    pub snapshot_id: Uuid,
    /// SHA-256 of the exact SPARQL query bytes that owns this BGP.
    pub query_sha256: String,
    /// SHA-256 of the canonical typed BGP representation.
    pub bgp_sha256: String,
    /// SHA-256 of the exact active default/named RDF dataset.
    pub active_dataset_sha256: String,
    /// SHA-256 of the graph authorization set applied before evaluation.
    pub authorized_graph_set_sha256: String,
    /// SHA-256 of the Phase 40.1 merged-ontology signature.
    pub owl_signature_sha256: String,
    /// SHA-256 of the Phase 40.2 datatype policy.
    pub datatype_policy_sha256: String,
    pub entailment_regime: EntailmentRegime,
    pub graph_context: DirectBgpGraphContext,
    /// Canonical sorted unique result variables without `?`/`$` prefixes.
    pub variables: Vec<String>,
    /// Number of candidate ground bindings considered by the exact path.
    pub candidate_binding_count: u64,
    /// Sum of `solutions[].multiplicity`; allows bag validation without expansion.
    pub solution_multiplicity_total: u64,
    /// Distinct solution mappings; duplicate mappings are represented by multiplicity.
    pub solutions: Vec<DirectBgpSolution>,
    pub outcome: DirectBgpOutcome,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<DirectBgpFailure>,
}

/// Fail-closed contract validation error.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum DirectBgpValidationError {
    #[error("unsupported Direct-BGP result format version")]
    FormatVersion,
    #[error("{0} must be lowercase hexadecimal SHA-256")]
    InvalidSha256(&'static str),
    #[error("result variables must be sorted, unique, non-empty names without ?/$ prefixes or whitespace")]
    InvalidVariables,
    #[error("graph context is invalid: {0}")]
    InvalidGraphContext(String),
    #[error("outcome/error state is invalid: {0}")]
    InvalidOutcome(String),
    #[error("candidateBindingCount is smaller than the number of distinct successful solutions")]
    CandidateCount,
    #[error("solutionMultiplicityTotal does not equal the checked sum of solution multiplicities")]
    MultiplicityTotal,
    #[error("solution {index} is invalid: {detail}")]
    InvalidSolution { index: usize, detail: String },
    #[error("parallel Direct-BGP result validation worker failed")]
    ValidationWorkerFailure,
}

/// Validate one Direct-BGP result without expanding bag duplicates.
///
/// Large solution vectors are validated in deterministic bounded CPU lanes. The lowest solution
/// index always wins if more than one lane observes invalid input, so worker scheduling cannot
/// change the reported failure.
pub fn validate_direct_bgp_result(result: &DirectBgpResult) -> Result<(), DirectBgpValidationError> {
    if result.format_version != FORMAT_VERSION {
        return Err(DirectBgpValidationError::FormatVersion);
    }
    for (name, value) in [
        ("querySha256", result.query_sha256.as_str()),
        ("bgpSha256", result.bgp_sha256.as_str()),
        ("activeDatasetSha256", result.active_dataset_sha256.as_str()),
        ("authorizedGraphSetSha256", result.authorized_graph_set_sha256.as_str()),
        ("owlSignatureSha256", result.owl_signature_sha256.as_str()),
        ("datatypePolicySha256", result.datatype_policy_sha256.as_str()),
    ] {
        if !is_lower_sha256(value) {
            return Err(DirectBgpValidationError::InvalidSha256(name));
        }
    }
    validate_graph_context(&result.graph_context)?;
    validate_variables(&result.variables)?;
    validate_outcome(result)?;

    let distinct_solutions = u64::try_from(result.solutions.len())
        .map_err(|_| DirectBgpValidationError::CandidateCount)?;
    if matches!(result.outcome.status, DirectBgpStatus::Complete)
        && result.candidate_binding_count < distinct_solutions
    {
        return Err(DirectBgpValidationError::CandidateCount);
    }

    let variable_set = result.variables.iter().map(String::as_str).collect::<BTreeSet<_>>();
    validate_solutions_parallel(&result.solutions, &variable_set)?;

    let mut multiplicity_total = 0_u64;
    for solution in &result.solutions {
        multiplicity_total = multiplicity_total
            .checked_add(solution.multiplicity)
            .ok_or(DirectBgpValidationError::MultiplicityTotal)?;
    }
    if multiplicity_total != result.solution_multiplicity_total {
        return Err(DirectBgpValidationError::MultiplicityTotal);
    }
    Ok(())
}

fn validate_solutions_parallel(
    solutions: &[DirectBgpSolution],
    variables: &BTreeSet<&str>,
) -> Result<(), DirectBgpValidationError> {
    if solutions.is_empty() {
        return Ok(());
    }
    let available = thread::available_parallelism().map_or(1, |count| count.get());
    let lanes = available.min(MAX_VALIDATION_LANES).min(solutions.len()).max(1);
    let chunk_size = solutions.len().div_ceil(lanes);
    let mut first_error: Option<(usize, String)> = None;
    let worker_result = thread::scope(|scope| {
        let mut handles = Vec::with_capacity(lanes);
        for (chunk_index, chunk) in solutions.chunks(chunk_size).enumerate() {
            let start = chunk_index * chunk_size;
            handles.push(scope.spawn(move || {
                for (offset, solution) in chunk.iter().enumerate() {
                    if let Err(detail) = validate_solution(solution, variables) {
                        return Some((start + offset, detail));
                    }
                }
                None
            }));
        }
        for handle in handles {
            match handle.join() {
                Ok(Some(observed)) => {
                    if first_error.as_ref().is_none_or(|current| observed.0 < current.0) {
                        first_error = Some(observed);
                    }
                }
                Ok(None) => {}
                Err(_) => return Err(DirectBgpValidationError::ValidationWorkerFailure),
            }
        }
        Ok(())
    });
    worker_result?;
    if let Some((index, detail)) = first_error {
        return Err(DirectBgpValidationError::InvalidSolution { index, detail });
    }
    Ok(())
}

fn validate_solution(solution: &DirectBgpSolution, variables: &BTreeSet<&str>) -> Result<(), String> {
    if solution.multiplicity == 0 {
        return Err("multiplicity must be greater than zero".to_owned());
    }
    for (variable, term) in &solution.bindings {
        if !variables.contains(variable.as_str()) {
            return Err(format!("binding variable {variable:?} is absent from variables"));
        }
        validate_rdf_term(term)?;
    }
    Ok(())
}

fn validate_rdf_term(term: &DirectBgpRdfTerm) -> Result<(), String> {
    match term {
        DirectBgpRdfTerm::Iri { value } => {
            if !is_absolute_iri(value) {
                return Err("IRI term is not an absolute whitespace-free IRI".to_owned());
            }
        }
        DirectBgpRdfTerm::BlankNode { value } => {
            if value.is_empty() || value.len() > 1024 || value.chars().any(char::is_whitespace) {
                return Err("blank-node identifier is empty, oversized, or contains whitespace".to_owned());
            }
        }
        DirectBgpRdfTerm::Literal { lexical_form, datatype_iri, language } => {
            if lexical_form.len() > 1_048_576 {
                return Err("literal lexical form exceeds the Phase 40.3 contract bound".to_owned());
            }
            if !is_absolute_iri(datatype_iri) {
                return Err("literal datatype is not an absolute whitespace-free IRI".to_owned());
            }
            if let Some(language) = language {
                if language.is_empty()
                    || language.len() > 63
                    || language.chars().any(char::is_whitespace)
                    || datatype_iri != RDF_LANG_STRING
                {
                    return Err("language literal must use rdf:langString and a bounded non-empty tag".to_owned());
                }
            }
        }
    }
    Ok(())
}

fn validate_graph_context(context: &DirectBgpGraphContext) -> Result<(), DirectBgpValidationError> {
    match context {
        DirectBgpGraphContext::Default { active_default_graph_sha256 } => {
            if !is_lower_sha256(active_default_graph_sha256) {
                return Err(DirectBgpValidationError::InvalidGraphContext(
                    "default graph hash is not lowercase SHA-256".to_owned(),
                ));
            }
        }
        DirectBgpGraphContext::Named { graph_iri } => {
            if !is_absolute_iri(graph_iri) {
                return Err(DirectBgpValidationError::InvalidGraphContext(
                    "named graph IRI is not absolute".to_owned(),
                ));
            }
        }
    }
    Ok(())
}

fn validate_variables(variables: &[String]) -> Result<(), DirectBgpValidationError> {
    if variables.windows(2).any(|window| window[0] >= window[1])
        || variables.iter().any(|variable| {
            variable.is_empty()
                || variable.len() > 1024
                || variable.starts_with('?') || variable.starts_with('$')
                || variable.chars().any(char::is_whitespace)
        })
    {
        return Err(DirectBgpValidationError::InvalidVariables);
    }
    Ok(())
}

fn validate_outcome(result: &DirectBgpResult) -> Result<(), DirectBgpValidationError> {
    match result.outcome.status {
        DirectBgpStatus::Complete => {
            if result.outcome.exactness != DirectBgpExactness::Exact
                || result.outcome.completeness != DirectBgpCompleteness::Complete
                || result.error.is_some()
            {
                return Err(DirectBgpValidationError::InvalidOutcome(
                    "complete requires exact + complete and forbids error".to_owned(),
                ));
            }
        }
        DirectBgpStatus::Failed => {
            if result.outcome.exactness != DirectBgpExactness::NotEstablished
                || result.outcome.completeness == DirectBgpCompleteness::Complete
                || result.error.is_none()
                || !result.solutions.is_empty()
                || result.solution_multiplicity_total != 0
            {
                return Err(DirectBgpValidationError::InvalidOutcome(
                    "failed requires no successful solutions, not-established exactness, non-complete completeness, and an error".to_owned(),
                ));
            }
            if let Some(error) = &result.error
                && (error.detail.is_empty() || error.detail.len() > 4096)
            {
                return Err(DirectBgpValidationError::InvalidOutcome(
                    "failure detail must contain 1..4096 bytes".to_owned(),
                ));
            }
        }
    }
    Ok(())
}

fn is_lower_sha256(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

fn is_absolute_iri(value: &str) -> bool {
    let Some(colon) = value.find(':') else {
        return false;
    };
    if colon == 0 || value.chars().any(char::is_whitespace) {
        return false;
    }
    value[..colon].chars().enumerate().all(|(index, character)| {
        if index == 0 {
            character.is_ascii_alphabetic()
        } else {
            character.is_ascii_alphanumeric() || matches!(character, '+' | '-' | '.')
        }
    })
}

#[cfg(test)]
mod phase40_3_tests {
    use super::{
        DirectBgpCompleteness, DirectBgpExactness, DirectBgpFailure, DirectBgpFailureCode,
        DirectBgpGraphContext, DirectBgpOutcome, DirectBgpRdfTerm, DirectBgpResult,
        DirectBgpSolution, DirectBgpStatus, DirectBgpValidationError, validate_direct_bgp_result,
    };
    use crate::EntailmentRegime;
    use std::collections::BTreeMap;
    use uuid::Uuid;

    fn complete_result() -> DirectBgpResult {
        let mut bindings = BTreeMap::new();
        bindings.insert(
            "x".to_owned(),
            DirectBgpRdfTerm::Iri { value: "https://example.test/a".to_owned() },
        );
        DirectBgpResult {
            format_version: 1,
            dataset_id: Uuid::from_u128(1),
            snapshot_id: Uuid::from_u128(2),
            query_sha256: "11".repeat(32),
            bgp_sha256: "22".repeat(32),
            active_dataset_sha256: "33".repeat(32),
            authorized_graph_set_sha256: "44".repeat(32),
            owl_signature_sha256: "55".repeat(32),
            datatype_policy_sha256: "66".repeat(32),
            entailment_regime: EntailmentRegime::Owl2Direct,
            graph_context: DirectBgpGraphContext::Named {
                graph_iri: "https://example.test/graph/a".to_owned(),
            },
            variables: vec!["x".to_owned()],
            candidate_binding_count: 8,
            solution_multiplicity_total: 2,
            solutions: vec![DirectBgpSolution { bindings, multiplicity: 2 }],
            outcome: DirectBgpOutcome {
                status: DirectBgpStatus::Complete,
                exactness: DirectBgpExactness::Exact,
                completeness: DirectBgpCompleteness::Complete,
            },
            error: None,
        }
    }

    #[test]
    fn exact_complete_result_validates_without_expanding_bag_duplicates() {
        assert_eq!(validate_direct_bgp_result(&complete_result()), Ok(()));
    }

    #[test]
    fn failed_result_cannot_carry_partial_solutions() {
        let mut result = complete_result();
        result.outcome = DirectBgpOutcome {
            status: DirectBgpStatus::Failed,
            exactness: DirectBgpExactness::NotEstablished,
            completeness: DirectBgpCompleteness::Incomplete,
        };
        result.error = Some(DirectBgpFailure {
            code: DirectBgpFailureCode::Timeout,
            retryable: true,
            detail: "deadline exceeded".to_owned(),
        });
        assert!(matches!(
            validate_direct_bgp_result(&result),
            Err(DirectBgpValidationError::InvalidOutcome(_))
        ));
    }

    #[test]
    fn multiplicity_total_is_checked_exactly() {
        let mut result = complete_result();
        result.solution_multiplicity_total = 1;
        assert_eq!(
            validate_direct_bgp_result(&result),
            Err(DirectBgpValidationError::MultiplicityTotal)
        );
    }

    #[test]
    fn variables_must_be_canonical_sorted_unique_names() {
        let mut result = complete_result();
        result.variables = vec!["z".to_owned(), "x".to_owned()];
        assert_eq!(
            validate_direct_bgp_result(&result),
            Err(DirectBgpValidationError::InvalidVariables)
        );
    }
}
