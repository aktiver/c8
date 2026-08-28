//! Phase 40.4 Direct certificate contract.
//!
//! The certificate is the immutable evidence envelope that later exact OWL Direct execution
//! attaches to one successful [`DirectBgpResult`]. Phase 40.4 defines and validates the contract;
//! it does not yet claim that arbitrary Direct-BGP execution or proof-DAG production is available.

use std::collections::BTreeSet;

use hex::encode as hex_encode;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use uuid::Uuid;

use crate::{
    DirectBgpCompleteness, DirectBgpExactness, DirectBgpGraphContext, DirectBgpRdfTerm,
    DirectBgpResult, DirectBgpSolution, DirectBgpStatus, EntailmentRegime,
    validate_direct_bgp_result,
};

const LEGACY_FORMAT_VERSION: u32 = 1;
const PROOF_FORMAT_VERSION: u32 = 2;
const MAX_SUPPORT_REFERENCES: usize = 1_000_000;
const RESULT_DIGEST_DOMAIN: &[u8] = b"ngkg-direct-bgp-result-v1\0";
const SOLUTION_DIGEST_DOMAIN: &[u8] = b"ngkg-direct-bgp-solution-v1\0";

/// The only result state a Direct certificate may certify.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum DirectCertifiedOutcome {
    /// The referenced Direct-BGP result is exact and complete.
    ExactComplete,
}

/// Exact completeness method represented by the Phase 40 Direct certificate.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum DirectCompletenessMethod {
    /// Every binding in the finite candidate space was checked by the exact reasoner path.
    ExhaustiveCandidateEntailment,
}

/// Provenance/proof coverage currently attached to the certificate.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum DirectProofCoverage {
    /// No proof/support graph has yet been attached; Phase 40.9 owns that runtime wiring.
    NotAvailable,
    /// Some support identifiers are present but full proof coverage is not established.
    Partial,
    /// Every certified answer is covered by support/proof references.
    Complete,
}

/// Stable support-reference families used by future proof/provenance wiring.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum DirectSupportKind {
    OntologyInput,
    AssertedAxiom,
    EntailedAxiom,
    OntologyImport,
    ClosureArtifact,
    ReasonerCheck,
}

/// One deterministic support/provenance reference.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DirectSupportReference {
    /// Stable lowercase SHA-256 identifier of the support/proof object.
    pub support_id: String,
    pub kind: DirectSupportKind,
    /// Optional checksum of the immutable artifact carrying the support object.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_sha256: Option<String>,
    /// Optional source graph that owns the asserted/provenance support.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_graph_iri: Option<String>,
}

/// Exact reasoner implementation identity bound into the certificate.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DirectReasonerIdentity {
    pub engine: String,
    pub engine_version: String,
    pub adapter_name: String,
    pub adapter_version: String,
    /// SHA-256 of the immutable exact-reasoner request envelope.
    pub request_sha256: String,
}

/// Evidence that the finite candidate space was exhausted without partial-success semantics.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DirectCompletenessEvidence {
    pub method: DirectCompletenessMethod,
    /// SHA-256 of the canonical finite candidate-binding inventory.
    pub candidate_space_sha256: String,
    pub candidate_binding_count: u64,
    pub checked_candidate_binding_count: u64,
    /// Distributed partition count used by the exact path; scalar execution uses one partition.
    pub partition_count: u32,
    pub completed_partition_count: u32,
    pub reasoner_request_count: u64,
    pub successful_reasoner_request_count: u64,
    /// Merkle/root-style checksum over completed exact-reasoner partition evidence.
    pub execution_root_sha256: String,
}

/// Snapshot-bound certificate for one successful exact Direct-BGP result.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DirectCertificate {
    pub format_version: u32,
    pub dataset_id: Uuid,
    pub snapshot_id: Uuid,
    pub query_sha256: String,
    pub bgp_sha256: String,
    pub active_dataset_sha256: String,
    pub authorized_graph_set_sha256: String,
    pub owl_signature_sha256: String,
    pub datatype_policy_sha256: String,
    pub entailment_regime: EntailmentRegime,
    pub graph_context: DirectBgpGraphContext,
    pub certified_outcome: DirectCertifiedOutcome,
    /// Scheduling-independent digest of the exact Direct-BGP result object.
    pub direct_bgp_result_sha256: String,
    /// SHA-256 of the Phase 40.9 immutable proof/support manifest. Required for formatVersion 2.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proof_manifest_sha256: Option<String>,
    pub reasoner: DirectReasonerIdentity,
    pub completeness: DirectCompletenessEvidence,
    pub proof_coverage: DirectProofCoverage,
    /// Strictly sorted unique support references. Phase 40.9 will require full answer coverage.
    pub support_references: Vec<DirectSupportReference>,
}

/// Direct-certificate contract failures.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum DirectCertificateValidationError {
    #[error("unsupported Direct certificate format version")]
    FormatVersion,
    #[error("{0} must be lowercase hexadecimal SHA-256")]
    InvalidSha256(&'static str),
    #[error("Direct certificate graph context is invalid")]
    InvalidGraphContext,
    #[error("reasoner identity contains an empty, oversized, or control-character field")]
    InvalidReasonerIdentity,
    #[error("completeness evidence does not prove exhaustive successful evaluation")]
    InvalidCompletenessEvidence,
    #[error("support references must be strictly sorted by supportId, unique, bounded, and valid")]
    InvalidSupportReferences,
    #[error("complete proof coverage requires at least one support reference")]
    InvalidProofCoverage,
    #[error("referenced Direct-BGP result is not exact + complete")]
    ResultNotExactComplete,
    #[error("Direct-BGP result contract is invalid")]
    InvalidDirectBgpResult,
}

/// Validate the closed Phase 40.4 Direct certificate contract.
pub fn validate_direct_certificate(
    certificate: &DirectCertificate,
) -> Result<(), DirectCertificateValidationError> {
    if !matches!(certificate.format_version, LEGACY_FORMAT_VERSION | PROOF_FORMAT_VERSION) {
        return Err(DirectCertificateValidationError::FormatVersion);
    }
    for (name, value) in [
        ("querySha256", certificate.query_sha256.as_str()),
        ("bgpSha256", certificate.bgp_sha256.as_str()),
        ("activeDatasetSha256", certificate.active_dataset_sha256.as_str()),
        ("authorizedGraphSetSha256", certificate.authorized_graph_set_sha256.as_str()),
        ("owlSignatureSha256", certificate.owl_signature_sha256.as_str()),
        ("datatypePolicySha256", certificate.datatype_policy_sha256.as_str()),
        ("directBgpResultSha256", certificate.direct_bgp_result_sha256.as_str()),
    ] {
        require_sha256(name, value)?;
    }
    validate_graph_context(&certificate.graph_context)?;
    validate_reasoner(&certificate.reasoner)?;
    validate_completeness(&certificate.completeness)?;
    validate_support_references(&certificate.support_references)?;
    match certificate.format_version {
        LEGACY_FORMAT_VERSION => {
            if certificate.proof_manifest_sha256.is_some() {
                return Err(DirectCertificateValidationError::InvalidProofCoverage);
            }
            if certificate.proof_coverage == DirectProofCoverage::Complete
                && certificate.support_references.is_empty()
            {
                return Err(DirectCertificateValidationError::InvalidProofCoverage);
            }
        }
        PROOF_FORMAT_VERSION => {
            let proof_manifest_sha256 = certificate.proof_manifest_sha256.as_deref()
                .ok_or(DirectCertificateValidationError::InvalidProofCoverage)?;
            require_sha256("proofManifestSha256", proof_manifest_sha256)?;
            if certificate.proof_coverage != DirectProofCoverage::Complete
                || certificate.support_references.is_empty()
                || certificate.support_references.iter().any(|reference| {
                    reference.kind != DirectSupportKind::ReasonerCheck
                        || reference.artifact_sha256.as_deref() != Some(proof_manifest_sha256)
                })
            {
                return Err(DirectCertificateValidationError::InvalidProofCoverage);
            }
        }
        _ => return Err(DirectCertificateValidationError::FormatVersion),
    }
    Ok(())
}

/// Produce the scheduling-independent SHA-256 bound by a Direct certificate.
///
/// Distinct solution rows are independently digested and sorted by digest before the outer hash
/// is finalized. Therefore distributed worker completion order cannot change the certificate.
pub fn direct_bgp_result_sha256(
    result: &DirectBgpResult,
) -> Result<String, DirectCertificateValidationError> {
    validate_direct_bgp_result(result)
        .map_err(|_| DirectCertificateValidationError::InvalidDirectBgpResult)?;

    let mut hash = Sha256::new();
    hash.update(RESULT_DIGEST_DOMAIN);
    hash.update(result.format_version.to_be_bytes());
    hash.update(result.dataset_id.as_bytes());
    hash.update(result.snapshot_id.as_bytes());
    update_string(&mut hash, &result.query_sha256);
    update_string(&mut hash, &result.bgp_sha256);
    update_string(&mut hash, &result.active_dataset_sha256);
    update_string(&mut hash, &result.authorized_graph_set_sha256);
    update_string(&mut hash, &result.owl_signature_sha256);
    update_string(&mut hash, &result.datatype_policy_sha256);
    hash.update([0x01]); // EntailmentRegime::Owl2Direct is the only admitted value.
    update_graph_context(&mut hash, &result.graph_context);

    update_usize(&mut hash, result.variables.len());
    for variable in &result.variables {
        update_string(&mut hash, variable);
    }
    hash.update(result.candidate_binding_count.to_be_bytes());
    hash.update(result.solution_multiplicity_total.to_be_bytes());

    let mut solution_hashes = result
        .solutions
        .iter()
        .map(digest_solution)
        .collect::<Vec<_>>();
    solution_hashes.sort_unstable();
    update_usize(&mut hash, solution_hashes.len());
    for solution_hash in solution_hashes {
        hash.update(solution_hash);
    }

    hash.update([match result.outcome.status {
        DirectBgpStatus::Complete => 0x01,
        DirectBgpStatus::Failed => 0x02,
    }]);
    hash.update([match result.outcome.exactness {
        DirectBgpExactness::Exact => 0x01,
        DirectBgpExactness::NotEstablished => 0x02,
    }]);
    hash.update([match result.outcome.completeness {
        DirectBgpCompleteness::Complete => 0x01,
        DirectBgpCompleteness::Incomplete => 0x02,
        DirectBgpCompleteness::NotEstablished => 0x03,
    }]);
    if let Some(error) = &result.error {
        hash.update([0x01]);
        hash.update([failure_code_tag(error.code)]);
        hash.update([u8::from(error.retryable)]);
        update_string(&mut hash, &error.detail);
    } else {
        hash.update([0x00]);
    }
    Ok(hex_encode(hash.finalize()))
}

/// Require that a certificate refers to this exact complete result and candidate inventory.
pub fn validate_direct_certificate_result(
    certificate: &DirectCertificate,
    result: &DirectBgpResult,
) -> Result<(), DirectCertificateValidationError> {
    validate_direct_certificate(certificate)?;
    validate_direct_bgp_result(result)
        .map_err(|_| DirectCertificateValidationError::InvalidDirectBgpResult)?;
    if result.outcome.status != DirectBgpStatus::Complete
        || result.outcome.exactness != DirectBgpExactness::Exact
        || result.outcome.completeness != DirectBgpCompleteness::Complete
        || result.error.is_some()
    {
        return Err(DirectCertificateValidationError::ResultNotExactComplete);
    }
    if certificate.dataset_id != result.dataset_id
        || certificate.snapshot_id != result.snapshot_id
        || certificate.query_sha256 != result.query_sha256
        || certificate.bgp_sha256 != result.bgp_sha256
        || certificate.active_dataset_sha256 != result.active_dataset_sha256
        || certificate.authorized_graph_set_sha256 != result.authorized_graph_set_sha256
        || certificate.owl_signature_sha256 != result.owl_signature_sha256
        || certificate.datatype_policy_sha256 != result.datatype_policy_sha256
        || certificate.entailment_regime != result.entailment_regime
        || certificate.graph_context != result.graph_context
        || certificate.direct_bgp_result_sha256 != direct_bgp_result_sha256(result)?
        || certificate.completeness.candidate_binding_count != result.candidate_binding_count
    {
        return Err(DirectCertificateValidationError::InvalidCompletenessEvidence);
    }
    Ok(())
}

fn digest_solution(solution: &DirectBgpSolution) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(SOLUTION_DIGEST_DOMAIN);
    update_usize(&mut hash, solution.bindings.len());
    for (variable, term) in &solution.bindings {
        update_string(&mut hash, variable);
        update_term(&mut hash, term);
    }
    hash.update(solution.multiplicity.to_be_bytes());
    hash.finalize().into()
}

fn update_term(hash: &mut Sha256, term: &DirectBgpRdfTerm) {
    match term {
        DirectBgpRdfTerm::Iri { value } => {
            hash.update([0x01]);
            update_string(hash, value);
        }
        DirectBgpRdfTerm::BlankNode { value } => {
            hash.update([0x02]);
            update_string(hash, value);
        }
        DirectBgpRdfTerm::Literal { lexical_form, datatype_iri, language } => {
            hash.update([0x03]);
            update_string(hash, lexical_form);
            update_string(hash, datatype_iri);
            if let Some(language) = language {
                hash.update([0x01]);
                update_string(hash, language);
            } else {
                hash.update([0x00]);
            }
        }
    }
}

fn update_graph_context(hash: &mut Sha256, context: &DirectBgpGraphContext) {
    match context {
        DirectBgpGraphContext::Default { active_default_graph_sha256 } => {
            hash.update([0x01]);
            update_string(hash, active_default_graph_sha256);
        }
        DirectBgpGraphContext::Named { graph_iri } => {
            hash.update([0x02]);
            update_string(hash, graph_iri);
        }
    }
}

fn update_string(hash: &mut Sha256, value: &str) {
    hash.update(u64::try_from(value.len()).unwrap_or(u64::MAX).to_be_bytes());
    hash.update(value.as_bytes());
}

fn update_usize(hash: &mut Sha256, value: usize) {
    hash.update(u64::try_from(value).unwrap_or(u64::MAX).to_be_bytes());
}

fn failure_code_tag(code: crate::DirectBgpFailureCode) -> u8 {
    match code {
        crate::DirectBgpFailureCode::IllegalBgp => 0x01,
        crate::DirectBgpFailureCode::InconsistentOntology => 0x02,
        crate::DirectBgpFailureCode::UnsupportedDatatype => 0x03,
        crate::DirectBgpFailureCode::ResourceExhausted => 0x04,
        crate::DirectBgpFailureCode::Timeout => 0x05,
        crate::DirectBgpFailureCode::ReasonerFailure => 0x06,
        crate::DirectBgpFailureCode::IntegrityFailure => 0x07,
        crate::DirectBgpFailureCode::NotCovered => 0x08,
    }
}

fn validate_reasoner(identity: &DirectReasonerIdentity) -> Result<(), DirectCertificateValidationError> {
    for value in [
        identity.engine.as_str(),
        identity.engine_version.as_str(),
        identity.adapter_name.as_str(),
        identity.adapter_version.as_str(),
    ] {
        if value.is_empty() || value.len() > 256 || value.chars().any(char::is_control) {
            return Err(DirectCertificateValidationError::InvalidReasonerIdentity);
        }
    }
    require_sha256("reasoner.requestSha256", &identity.request_sha256)
}

fn validate_completeness(
    evidence: &DirectCompletenessEvidence,
) -> Result<(), DirectCertificateValidationError> {
    require_sha256("completeness.candidateSpaceSha256", &evidence.candidate_space_sha256)?;
    require_sha256("completeness.executionRootSha256", &evidence.execution_root_sha256)?;
    if evidence.partition_count == 0
        || evidence.partition_count > 1_000_000
        || evidence.completed_partition_count != evidence.partition_count
        || evidence.checked_candidate_binding_count != evidence.candidate_binding_count
        || evidence.successful_reasoner_request_count != evidence.reasoner_request_count
    {
        return Err(DirectCertificateValidationError::InvalidCompletenessEvidence);
    }
    Ok(())
}

fn validate_support_references(
    references: &[DirectSupportReference],
) -> Result<(), DirectCertificateValidationError> {
    if references.len() > MAX_SUPPORT_REFERENCES {
        return Err(DirectCertificateValidationError::InvalidSupportReferences);
    }
    let mut previous: Option<&str> = None;
    let mut seen = BTreeSet::new();
    for reference in references {
        require_sha256("supportReferences.supportId", &reference.support_id)?;
        if previous.is_some_and(|value| value >= reference.support_id.as_str())
            || !seen.insert(reference.support_id.as_str())
        {
            return Err(DirectCertificateValidationError::InvalidSupportReferences);
        }
        if let Some(artifact_sha256) = &reference.artifact_sha256 {
            require_sha256("supportReferences.artifactSha256", artifact_sha256)?;
        }
        if let Some(graph_iri) = &reference.source_graph_iri
            && !is_absolute_iri(graph_iri)
        {
            return Err(DirectCertificateValidationError::InvalidSupportReferences);
        }
        previous = Some(&reference.support_id);
    }
    Ok(())
}

fn validate_graph_context(context: &DirectBgpGraphContext) -> Result<(), DirectCertificateValidationError> {
    match context {
        DirectBgpGraphContext::Default { active_default_graph_sha256 } => {
            require_sha256("graphContext.activeDefaultGraphSha256", active_default_graph_sha256)?;
        }
        DirectBgpGraphContext::Named { graph_iri } => {
            if !is_absolute_iri(graph_iri) {
                return Err(DirectCertificateValidationError::InvalidGraphContext);
            }
        }
    }
    Ok(())
}

fn require_sha256(
    name: &'static str,
    value: &str,
) -> Result<(), DirectCertificateValidationError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(DirectCertificateValidationError::InvalidSha256(name));
    }
    Ok(())
}

fn is_absolute_iri(value: &str) -> bool {
    let Some(colon) = value.find(':') else {
        return false;
    };
    colon > 0
        && !value.chars().any(char::is_whitespace)
        && value[..colon].chars().enumerate().all(|(index, character)| {
            if index == 0 {
                character.is_ascii_alphabetic()
            } else {
                character.is_ascii_alphanumeric() || matches!(character, '+' | '-' | '.')
            }
        })
}

#[cfg(test)]
mod phase40_4_tests {
    use std::collections::BTreeMap;

    use uuid::Uuid;

    use super::{
        DirectCertificate, DirectCertifiedOutcome, DirectCompletenessEvidence,
        DirectCompletenessMethod, DirectProofCoverage, DirectReasonerIdentity,
        DirectSupportKind, DirectSupportReference, direct_bgp_result_sha256,
        validate_direct_certificate, validate_direct_certificate_result,
    };
    use crate::{
        DirectBgpCompleteness, DirectBgpExactness, DirectBgpGraphContext, DirectBgpOutcome,
        DirectBgpRdfTerm, DirectBgpResult, DirectBgpSolution, DirectBgpStatus, EntailmentRegime,
    };

    fn result() -> DirectBgpResult {
        let mut a = BTreeMap::new();
        a.insert("x".to_owned(), DirectBgpRdfTerm::Iri { value: "https://example.test/a".to_owned() });
        let mut b = BTreeMap::new();
        b.insert("x".to_owned(), DirectBgpRdfTerm::Iri { value: "https://example.test/b".to_owned() });
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
            graph_context: DirectBgpGraphContext::Named { graph_iri: "https://example.test/g".to_owned() },
            variables: vec!["x".to_owned()],
            candidate_binding_count: 8,
            solution_multiplicity_total: 3,
            solutions: vec![
                DirectBgpSolution { bindings: a, multiplicity: 2 },
                DirectBgpSolution { bindings: b, multiplicity: 1 },
            ],
            outcome: DirectBgpOutcome {
                status: DirectBgpStatus::Complete,
                exactness: DirectBgpExactness::Exact,
                completeness: DirectBgpCompleteness::Complete,
            },
            error: None,
        }
    }

    fn certificate(result: &DirectBgpResult) -> DirectCertificate {
        DirectCertificate {
            format_version: 1,
            dataset_id: result.dataset_id,
            snapshot_id: result.snapshot_id,
            query_sha256: result.query_sha256.clone(),
            bgp_sha256: result.bgp_sha256.clone(),
            active_dataset_sha256: result.active_dataset_sha256.clone(),
            authorized_graph_set_sha256: result.authorized_graph_set_sha256.clone(),
            owl_signature_sha256: result.owl_signature_sha256.clone(),
            datatype_policy_sha256: result.datatype_policy_sha256.clone(),
            entailment_regime: EntailmentRegime::Owl2Direct,
            graph_context: result.graph_context.clone(),
            certified_outcome: DirectCertifiedOutcome::ExactComplete,
            direct_bgp_result_sha256: direct_bgp_result_sha256(result).unwrap_or_default(),
            proof_manifest_sha256: None,
            reasoner: DirectReasonerIdentity {
                engine: "HermiT".to_owned(),
                engine_version: "1.4.3.517".to_owned(),
                adapter_name: "ngkg-hermit-adapter".to_owned(),
                adapter_version: "40.4".to_owned(),
                request_sha256: "77".repeat(32),
            },
            completeness: DirectCompletenessEvidence {
                method: DirectCompletenessMethod::ExhaustiveCandidateEntailment,
                candidate_space_sha256: "88".repeat(32),
                candidate_binding_count: result.candidate_binding_count,
                checked_candidate_binding_count: result.candidate_binding_count,
                partition_count: 4,
                completed_partition_count: 4,
                reasoner_request_count: 8,
                successful_reasoner_request_count: 8,
                execution_root_sha256: "99".repeat(32),
            },
            proof_coverage: DirectProofCoverage::Partial,
            support_references: vec![DirectSupportReference {
                support_id: "aa".repeat(32),
                kind: DirectSupportKind::ReasonerCheck,
                artifact_sha256: Some("bb".repeat(32)),
                source_graph_iri: None,
            }],
        }
    }

    #[test]
    fn certificate_binds_exact_complete_result() {
        let result = result();
        let certificate = certificate(&result);
        assert_eq!(validate_direct_certificate(&certificate), Ok(()));
        assert_eq!(validate_direct_certificate_result(&certificate, &result), Ok(()));
    }

    #[test]
    fn result_digest_is_independent_of_parallel_solution_completion_order() {
        let first = result();
        let mut second = first.clone();
        second.solutions.reverse();
        assert_eq!(direct_bgp_result_sha256(&first), direct_bgp_result_sha256(&second));
    }

    #[test]
    fn result_digest_matches_cross_language_fixture_vector() {
        assert_eq!(
            direct_bgp_result_sha256(&result()),
            Ok("35e90b74cd86849ed8ed5877088ef32ffdac9642c11fab422c470ff31171475f".to_owned())
        );
    }

    #[test]
    fn incomplete_partition_evidence_is_rejected() {
        let result = result();
        let mut certificate = certificate(&result);
        certificate.completeness.completed_partition_count = 3;
        assert!(validate_direct_certificate(&certificate).is_err());
    }

    #[test]
    fn support_references_must_be_sorted_unique() {
        let result = result();
        let mut certificate = certificate(&result);
        certificate.support_references.push(certificate.support_references[0].clone());
        assert!(validate_direct_certificate(&certificate).is_err());
    }
}
