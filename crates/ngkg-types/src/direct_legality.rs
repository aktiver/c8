//! Phase 40.7 OWL 2 Direct-Semantics BGP legality contract.
//!
//! The contract is deliberately separate from Phase 40.3 result and Phase 40.4 certificate
//! objects. Phase 40.7 only admits or rejects a typed basic graph pattern. It never claims an
//! entailment answer. Phase 40.8 consumes only `Legal` records and remains responsible for the
//! exact, grounded OWL 2 DL entailment path.

use std::{collections::BTreeSet, thread};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

use crate::EntailmentRegime;

const FORMAT_VERSION: u32 = 1;
const MAX_VALIDATION_LANES: usize = 32;

/// Active graph scope of one SPARQL basic graph pattern.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase", tag = "scope")]
pub enum DirectBgpScope {
    /// Pattern executes against the active query default graph.
    #[serde(rename = "default")]
    Default,
    /// Pattern executes against one explicit named graph.
    #[serde(rename = "named")]
    Named { graph_iri: String },
    /// Pattern executes once per available named graph and binds this graph variable.
    #[serde(rename = "namedVariable")]
    NamedVariable { variable: String },
}

/// OWL structural role assigned to a SPARQL variable within one BGP only.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum DirectVariableRole {
    Class,
    ObjectProperty,
    DataProperty,
    AnnotationProperty,
    Datatype,
    NamedIndividual,
    Literal,
}

/// How a variable received its role.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum DirectVariableRoleSource {
    /// BGP-local `?x rdf:type owl:*` declaration required by the W3C Direct regime.
    ExplicitDeclaration,
    /// Role follows uniquely from an OWL structural position plus declarations in the active
    /// qualified ontology signature.
    StructuralPosition,
}

/// Canonical variable typing evidence local to a single BGP.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DirectVariableTyping {
    /// Variable without `?`/`$` prefix.
    pub variable: String,
    pub role: DirectVariableRole,
    pub source: DirectVariableRoleSource,
}

/// Query-level legality state of a BGP before exact entailment execution.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum DirectBgpLegalityStatus {
    Legal,
    Illegal,
}

/// Stable fail-closed reason codes for illegal Direct BGPs.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum DirectBgpLegalityFailureCode {
    ConflictingVariableType,
    AmbiguousVariableRole,
    UndeclaredEntityVariable,
    UnknownPredicate,
    InvalidStructuralShape,
    UnsupportedOwlStructure,
    InvalidGraphScope,
    ResourceLimitExceeded,
}

/// Bounded diagnostic for one rejected BGP.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DirectBgpLegalityFailure {
    pub code: DirectBgpLegalityFailureCode,
    pub detail: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub triple_ordinal: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub variable: Option<String>,
}

/// One deterministic legality decision for one typed SPARQL BGP leaf.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DirectBgpLegalityRecord {
    /// Stable query-local preorder ordinal.
    pub ordinal: u64,
    /// SHA-256 of the canonical, order-independent triple-pattern multiset plus graph scope.
    pub bgp_sha256: String,
    pub graph_scope: DirectBgpScope,
    pub triple_count: u64,
    /// Canonical sorted unique OWL axiom/structure families recognized while mapping the BGP.
    pub recognized_forms: Vec<String>,
    /// Canonical sorted unique variable-typing evidence.
    pub variables: Vec<DirectVariableTyping>,
    pub status: DirectBgpLegalityStatus,
    /// W3C query-level legality does not replace the per-grounding OWL 2 DL check required by
    /// Phase 40.8 before a candidate solution may be accepted.
    pub grounded_owl2dl_check_required: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure: Option<DirectBgpLegalityFailure>,
}

/// Snapshot-bound result of classifying every BGP in one parsed SPARQL query.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DirectBgpLegalityReport {
    pub format_version: u32,
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
    pub entailment_regime: EntailmentRegime,
    /// Versioned W3C-derived admission algorithm. It covers query/BGP legality; exact candidate
    /// grounding and entailment remain Phase 40.8 responsibilities.
    pub classifier: String,
    pub bgp_count: u64,
    pub all_bgps_legal: bool,
    /// SPARQL property paths are algebra operators, not BGP triple patterns under the Direct
    /// entailment extension; this flag makes that boundary machine-visible.
    pub property_paths_outside_direct_bgps: bool,
    pub bgps: Vec<DirectBgpLegalityRecord>,
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum DirectBgpLegalityValidationError {
    #[error("unsupported Direct-BGP legality format version")]
    FormatVersion,
    #[error("{0} must be lowercase hexadecimal SHA-256")]
    InvalidSha256(&'static str),
    #[error("classifier identifier is not the Phase 40.7 classifier")]
    Classifier,
    #[error("BGP count or ordinals are inconsistent")]
    BgpCount,
    #[error("allBgpsLegal disagrees with the individual BGP decisions")]
    AggregateLegality,
    #[error("BGP {ordinal} is invalid: {detail}")]
    InvalidBgp { ordinal: u64, detail: String },
    #[error("parallel Direct-BGP legality validation worker failed")]
    ValidationWorkerFailure,
}

/// Closed identifier for the Phase 40.7 admission algorithm.
pub const DIRECT_BGP_CLASSIFIER_V1: &str = "w3c-owl2-direct-bgp-cp1-cp4-v1";

/// Independently validate a snapshot-bound legality report.
pub fn validate_direct_bgp_legality_report(
    report: &DirectBgpLegalityReport,
) -> Result<(), DirectBgpLegalityValidationError> {
    if report.format_version != FORMAT_VERSION {
        return Err(DirectBgpLegalityValidationError::FormatVersion);
    }
    for (field, value) in [
        ("querySha256", report.query_sha256.as_str()),
        ("sparqlAlgebraSha256", report.sparql_algebra_sha256.as_str()),
        ("activeDatasetSha256", report.active_dataset_sha256.as_str()),
        ("authorizedGraphSetSha256", report.authorized_graph_set_sha256.as_str()),
        ("owlSignatureSha256", report.owl_signature_sha256.as_str()),
        ("datatypePolicySha256", report.datatype_policy_sha256.as_str()),
        ("owlProfileQualificationSha256", report.owl_profile_qualification_sha256.as_str()),
        ("owlConsistencyQualificationSha256", report.owl_consistency_qualification_sha256.as_str()),
    ] {
        if !is_lower_sha256(value) {
            return Err(DirectBgpLegalityValidationError::InvalidSha256(field));
        }
    }
    if report.classifier != DIRECT_BGP_CLASSIFIER_V1 {
        return Err(DirectBgpLegalityValidationError::Classifier);
    }
    let expected_count = u64::try_from(report.bgps.len())
        .map_err(|_| DirectBgpLegalityValidationError::BgpCount)?;
    if report.bgp_count != expected_count {
        return Err(DirectBgpLegalityValidationError::BgpCount);
    }
    for (index, bgp) in report.bgps.iter().enumerate() {
        if bgp.ordinal != u64::try_from(index).map_err(|_| DirectBgpLegalityValidationError::BgpCount)? {
            return Err(DirectBgpLegalityValidationError::BgpCount);
        }
    }
    validate_bgps_parallel(&report.bgps)?;
    let aggregate = report
        .bgps
        .iter()
        .all(|bgp| bgp.status == DirectBgpLegalityStatus::Legal);
    if aggregate != report.all_bgps_legal {
        return Err(DirectBgpLegalityValidationError::AggregateLegality);
    }
    Ok(())
}

fn validate_bgps_parallel(
    bgps: &[DirectBgpLegalityRecord],
) -> Result<(), DirectBgpLegalityValidationError> {
    if bgps.is_empty() {
        return Ok(());
    }
    let available = thread::available_parallelism().map_or(1, |count| count.get());
    let lanes = available.min(MAX_VALIDATION_LANES).min(bgps.len()).max(1);
    let chunk_size = bgps.len().div_ceil(lanes);
    let mut first: Option<(u64, String)> = None;
    let worker_result = thread::scope(|scope| {
        let mut handles = Vec::with_capacity(lanes);
        for chunk in bgps.chunks(chunk_size) {
            handles.push(scope.spawn(move || {
                for bgp in chunk {
                    if let Err(detail) = validate_bgp(bgp) {
                        return Some((bgp.ordinal, detail));
                    }
                }
                None
            }));
        }
        for handle in handles {
            let observed = handle
                .join()
                .map_err(|_| DirectBgpLegalityValidationError::ValidationWorkerFailure)?;
            if let Some(candidate) = observed
                && first.as_ref().is_none_or(|existing| candidate.0 < existing.0)
            {
                first = Some(candidate);
            }
        }
        Ok::<(), DirectBgpLegalityValidationError>(())
    });
    worker_result?;
    if let Some((ordinal, detail)) = first {
        return Err(DirectBgpLegalityValidationError::InvalidBgp { ordinal, detail });
    }
    Ok(())
}

fn validate_bgp(bgp: &DirectBgpLegalityRecord) -> Result<(), String> {
    if !is_lower_sha256(&bgp.bgp_sha256) {
        return Err("bgpSha256 is not lowercase SHA-256".to_owned());
    }
    validate_scope(&bgp.graph_scope)?;
    if bgp.triple_count == 0 && !bgp.recognized_forms.is_empty() {
        return Err("empty BGP cannot advertise recognized OWL forms".to_owned());
    }
    if !sorted_unique_strings(&bgp.recognized_forms) {
        return Err("recognizedForms must be sorted and unique".to_owned());
    }
    let mut variable_names = BTreeSet::new();
    let mut prior: Option<&str> = None;
    for typing in &bgp.variables {
        if !valid_variable_name(&typing.variable) || !variable_names.insert(typing.variable.as_str()) {
            return Err("variable typing names must be valid and unique".to_owned());
        }
        if prior.is_some_and(|p| p >= typing.variable.as_str()) {
            return Err("variable typings must be sorted by variable name".to_owned());
        }
        prior = Some(typing.variable.as_str());
    }
    match bgp.status {
        DirectBgpLegalityStatus::Legal => {
            if bgp.failure.is_some() {
                return Err("legal BGP cannot carry a failure".to_owned());
            }
            if !bgp.grounded_owl2dl_check_required {
                return Err("legal Direct BGP must retain the Phase 40.8 grounded OWL 2 DL check".to_owned());
            }
        }
        DirectBgpLegalityStatus::Illegal => {
            let failure = bgp.failure.as_ref().ok_or_else(|| "illegal BGP requires a failure".to_owned())?;
            if failure.detail.is_empty() || failure.detail.len() > 2048 {
                return Err("failure detail must be bounded and non-empty".to_owned());
            }
            if failure.variable.as_deref().is_some_and(|v| !valid_variable_name(v)) {
                return Err("failure variable is invalid".to_owned());
            }
        }
    }
    Ok(())
}

fn validate_scope(scope: &DirectBgpScope) -> Result<(), String> {
    match scope {
        DirectBgpScope::Default => Ok(()),
        DirectBgpScope::Named { graph_iri } => {
            if absolute_iri(graph_iri) { Ok(()) } else { Err("named graph IRI is not absolute".to_owned()) }
        }
        DirectBgpScope::NamedVariable { variable } => {
            if valid_variable_name(variable) { Ok(()) } else { Err("graph variable is invalid".to_owned()) }
        }
    }
}

fn sorted_unique_strings(values: &[String]) -> bool {
    values.windows(2).all(|pair| pair[0] < pair[1])
}

fn valid_variable_name(value: &str) -> bool {
    !value.is_empty()
        && !value.starts_with('?')
        && !value.starts_with('$')
        && !value.chars().any(char::is_whitespace)
}

fn absolute_iri(value: &str) -> bool {
    value.contains(':') && !value.chars().any(char::is_whitespace)
}

fn is_lower_sha256(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hash() -> String { "11".repeat(32) }

    #[test]
    fn complete_legal_report_validates() {
        let report = DirectBgpLegalityReport {
            format_version: 1,
            dataset_id: Uuid::from_u128(1),
            snapshot_id: Uuid::from_u128(2),
            query_sha256: hash(),
            sparql_algebra_sha256: hash(),
            active_dataset_sha256: hash(),
            authorized_graph_set_sha256: hash(),
            owl_signature_sha256: hash(),
            datatype_policy_sha256: hash(),
            owl_profile_qualification_sha256: hash(),
            owl_consistency_qualification_sha256: hash(),
            entailment_regime: EntailmentRegime::Owl2Direct,
            classifier: DIRECT_BGP_CLASSIFIER_V1.to_owned(),
            bgp_count: 1,
            all_bgps_legal: true,
            property_paths_outside_direct_bgps: true,
            bgps: vec![DirectBgpLegalityRecord {
                ordinal: 0,
                bgp_sha256: hash(),
                graph_scope: DirectBgpScope::Default,
                triple_count: 1,
                recognized_forms: vec!["ObjectPropertyAssertion".to_owned()],
                variables: vec![DirectVariableTyping {
                    variable: "x".to_owned(),
                    role: DirectVariableRole::NamedIndividual,
                    source: DirectVariableRoleSource::StructuralPosition,
                }],
                status: DirectBgpLegalityStatus::Legal,
                grounded_owl2dl_check_required: true,
                failure: None,
            }],
        };
        assert_eq!(validate_direct_bgp_legality_report(&report), Ok(()));
    }
}
