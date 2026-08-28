//! Phase 40.9 exact Direct-BGP support/proof manifest.
//!
//! HermiT does not expose a derivation DAG through the adapter used by NGKG. Phase 40.9 therefore
//! records a stronger auditable runtime invariant that is available today: every returned SPARQL
//! solution multiplicity is backed by one immutable grounded OWL 2 DL reasoner check, and the
//! global exhaustive-completion barrier is backed by its own support identifier. These records do
//! not claim to be a minimal logical derivation; they are checksum-bound evidence of exactly what
//! was grounded and checked by the pinned exact reasoner.

use std::collections::{BTreeMap, BTreeSet};

use hex::encode as hex_encode;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use uuid::Uuid;

use crate::{
    DirectBgpGraphContext, DirectBgpRdfTerm, DirectBgpResult, DirectCertificate,
    DirectProofCoverage, DirectSupportKind, EntailmentRegime, direct_bgp_result_sha256,
    validate_direct_bgp_result, validate_direct_certificate_result,
};

pub const DIRECT_PROOF_FORMAT_VERSION: u32 = 1;
const MAX_PROOF_RECORDS: usize = 1_000_000;
const BINDING_DOMAIN: &[u8] = b"ngkg-direct-proof-binding-v1\0";
const SUPPORT_DOMAIN: &[u8] = b"ngkg-direct-reasoner-check-support-v1\0";
const COMPLETION_DOMAIN: &[u8] = b"ngkg-direct-completion-support-v1\0";

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DirectProofOntologyInput {
    pub sha256: String,
    pub ontology_iris: Vec<String>,
}

/// One exact HermiT check that produced one entailed candidate solution.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DirectReasonerCheckProof {
    pub support_id: String,
    pub candidate_ordinal: u64,
    pub partition_index: u32,
    pub request_sha256: String,
    pub binding_sha256: String,
    pub grounded_rdf_sha256: String,
    pub logical_axioms_sha256: String,
    pub logical_axiom_count: u64,
}

/// Immutable support manifest for one exact + complete Direct-BGP result.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DirectProofManifest {
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
    pub direct_bgp_result_sha256: String,
    pub candidate_space_sha256: String,
    pub execution_root_sha256: String,
    pub reasoner_engine: String,
    pub reasoner_version: String,
    pub adapter_version: String,
    /// Stable support object proving the exhaustive partition completion barrier, including empty answers.
    pub completion_support_id: String,
    pub ontology_inputs: Vec<DirectProofOntologyInput>,
    /// Strictly increasing by candidateOrdinal; each record corresponds to one entailed candidate.
    pub answer_proofs: Vec<DirectReasonerCheckProof>,
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum DirectProofValidationError {
    #[error("unsupported Direct proof manifest version")]
    Version,
    #[error("invalid SHA-256 field: {0}")]
    Hash(&'static str),
    #[error("invalid reasoner identity")]
    Reasoner,
    #[error("ontology-input proof inventory is not sorted, unique, or valid")]
    OntologyInputs,
    #[error("answer proof records are not sorted, unique, bounded, or valid")]
    AnswerProofs,
    #[error("answer proof supportId is not reproducible")]
    SupportId,
    #[error("completion supportId is not reproducible")]
    CompletionSupportId,
    #[error("Direct proof manifest does not cover the exact result multiset")]
    ResultCoverage,
    #[error("Direct certificate does not exactly bind the proof manifest")]
    CertificateBinding,
    #[error("Direct result/certificate contract is invalid")]
    BaseContract,
}

pub fn direct_binding_sha256(bindings: &BTreeMap<String, DirectBgpRdfTerm>) -> String {
    let mut hash = Sha256::new();
    hash.update(BINDING_DOMAIN);
    hash.update(u64::try_from(bindings.len()).unwrap_or(u64::MAX).to_be_bytes());
    for (variable, term) in bindings {
        update_string(&mut hash, variable);
        update_term(&mut hash, term);
    }
    hex_encode(hash.finalize())
}

pub fn direct_reasoner_support_id(
    manifest: &DirectProofManifest,
    proof: &DirectReasonerCheckProof,
) -> String {
    let mut hash = Sha256::new();
    hash.update(SUPPORT_DOMAIN);
    hash.update(manifest.dataset_id.as_bytes());
    hash.update(manifest.snapshot_id.as_bytes());
    update_string(&mut hash, &manifest.query_sha256);
    update_string(&mut hash, &manifest.bgp_sha256);
    update_string(&mut hash, &manifest.active_dataset_sha256);
    update_string(&mut hash, &manifest.authorized_graph_set_sha256);
    update_string(&mut hash, &manifest.owl_signature_sha256);
    update_string(&mut hash, &manifest.datatype_policy_sha256);
    update_graph(&mut hash, &manifest.graph_context);
    hash.update(proof.candidate_ordinal.to_be_bytes());
    hash.update(proof.partition_index.to_be_bytes());
    update_string(&mut hash, &proof.request_sha256);
    update_string(&mut hash, &proof.binding_sha256);
    update_string(&mut hash, &proof.grounded_rdf_sha256);
    update_string(&mut hash, &proof.logical_axioms_sha256);
    hash.update(proof.logical_axiom_count.to_be_bytes());
    hex_encode(hash.finalize())
}

pub fn direct_completion_support_id(manifest: &DirectProofManifest) -> String {
    let mut hash = Sha256::new();
    hash.update(COMPLETION_DOMAIN);
    hash.update(manifest.dataset_id.as_bytes());
    hash.update(manifest.snapshot_id.as_bytes());
    update_string(&mut hash, &manifest.query_sha256);
    update_string(&mut hash, &manifest.bgp_sha256);
    update_string(&mut hash, &manifest.direct_bgp_result_sha256);
    update_string(&mut hash, &manifest.candidate_space_sha256);
    update_string(&mut hash, &manifest.execution_root_sha256);
    update_string(&mut hash, &manifest.reasoner_engine);
    update_string(&mut hash, &manifest.reasoner_version);
    update_string(&mut hash, &manifest.adapter_version);
    hex_encode(hash.finalize())
}

pub fn validate_direct_proof_manifest(
    manifest: &DirectProofManifest,
) -> Result<(), DirectProofValidationError> {
    if manifest.format_version != DIRECT_PROOF_FORMAT_VERSION
        || manifest.entailment_regime != EntailmentRegime::Owl2Direct
    {
        return Err(DirectProofValidationError::Version);
    }
    for (name, value) in [
        ("querySha256", manifest.query_sha256.as_str()),
        ("bgpSha256", manifest.bgp_sha256.as_str()),
        ("activeDatasetSha256", manifest.active_dataset_sha256.as_str()),
        ("authorizedGraphSetSha256", manifest.authorized_graph_set_sha256.as_str()),
        ("owlSignatureSha256", manifest.owl_signature_sha256.as_str()),
        ("datatypePolicySha256", manifest.datatype_policy_sha256.as_str()),
        ("directBgpResultSha256", manifest.direct_bgp_result_sha256.as_str()),
        ("candidateSpaceSha256", manifest.candidate_space_sha256.as_str()),
        ("executionRootSha256", manifest.execution_root_sha256.as_str()),
        ("completionSupportId", manifest.completion_support_id.as_str()),
    ] {
        require_sha(name, value)?;
    }
    for value in [&manifest.reasoner_engine, &manifest.reasoner_version, &manifest.adapter_version] {
        if value.is_empty() || value.len() > 256 || value.chars().any(char::is_control) {
            return Err(DirectProofValidationError::Reasoner);
        }
    }
    validate_ontology_inputs(&manifest.ontology_inputs)?;
    if manifest.answer_proofs.len() > MAX_PROOF_RECORDS {
        return Err(DirectProofValidationError::AnswerProofs);
    }
    let mut previous_ordinal = None;
    let mut support_ids = BTreeSet::new();
    for proof in &manifest.answer_proofs {
        for (name, value) in [
            ("answerProof.requestSha256", proof.request_sha256.as_str()),
            ("answerProof.bindingSha256", proof.binding_sha256.as_str()),
            ("answerProof.groundedRdfSha256", proof.grounded_rdf_sha256.as_str()),
            ("answerProof.logicalAxiomsSha256", proof.logical_axioms_sha256.as_str()),
            ("answerProof.supportId", proof.support_id.as_str()),
        ] {
            require_sha(name, value)?;
        }
        if previous_ordinal.is_some_and(|value| proof.candidate_ordinal <= value)
            || !support_ids.insert(proof.support_id.as_str())
        {
            return Err(DirectProofValidationError::AnswerProofs);
        }
        if direct_reasoner_support_id(manifest, proof) != proof.support_id {
            return Err(DirectProofValidationError::SupportId);
        }
        previous_ordinal = Some(proof.candidate_ordinal);
    }
    if direct_completion_support_id(manifest) != manifest.completion_support_id {
        return Err(DirectProofValidationError::CompletionSupportId);
    }
    Ok(())
}

/// Verify that proof records cover every exact answer multiplicity and that the certificate exposes
/// exactly those support IDs plus the global completion barrier support.
pub fn validate_direct_proof_bundle(
    manifest: &DirectProofManifest,
    result: &DirectBgpResult,
    certificate: &DirectCertificate,
    manifest_sha256: &str,
) -> Result<(), DirectProofValidationError> {
    validate_direct_proof_manifest(manifest)?;
    validate_direct_bgp_result(result).map_err(|_| DirectProofValidationError::BaseContract)?;
    validate_direct_certificate_result(certificate, result)
        .map_err(|_| DirectProofValidationError::BaseContract)?;
    require_sha("proofManifestSha256", manifest_sha256)?;
    let result_sha = direct_bgp_result_sha256(result).map_err(|_| DirectProofValidationError::BaseContract)?;
    if manifest.direct_bgp_result_sha256 != result_sha
        || manifest.dataset_id != result.dataset_id
        || manifest.snapshot_id != result.snapshot_id
        || manifest.query_sha256 != result.query_sha256
        || manifest.bgp_sha256 != result.bgp_sha256
        || manifest.active_dataset_sha256 != result.active_dataset_sha256
        || manifest.authorized_graph_set_sha256 != result.authorized_graph_set_sha256
        || manifest.owl_signature_sha256 != result.owl_signature_sha256
        || manifest.datatype_policy_sha256 != result.datatype_policy_sha256
        || manifest.graph_context != result.graph_context
        || manifest.candidate_space_sha256 != certificate.completeness.candidate_space_sha256
        || manifest.execution_root_sha256 != certificate.completeness.execution_root_sha256
    {
        return Err(DirectProofValidationError::ResultCoverage);
    }

    let mut expected = BTreeMap::<String, u64>::new();
    for solution in &result.solutions {
        let binding_sha256 = direct_binding_sha256(&solution.bindings);
        if expected.insert(binding_sha256, solution.multiplicity).is_some() {
            // The exact result contract uses compressed bag rows. Two rows with the same canonical
            // binding would make proof multiplicity coverage ambiguous, so reject rather than merge.
            return Err(DirectProofValidationError::ResultCoverage);
        }
    }
    let mut observed = BTreeMap::<String, u64>::new();
    for proof in &manifest.answer_proofs {
        let entry = observed.entry(proof.binding_sha256.clone()).or_default();
        *entry = entry.checked_add(1).ok_or(DirectProofValidationError::ResultCoverage)?;
    }
    if expected != observed
        || u64::try_from(manifest.answer_proofs.len()).unwrap_or(u64::MAX)
            != result.solution_multiplicity_total
    {
        return Err(DirectProofValidationError::ResultCoverage);
    }

    if certificate.format_version != 2
        || certificate.proof_coverage != DirectProofCoverage::Complete
        || certificate.proof_manifest_sha256.as_deref() != Some(manifest_sha256)
        || manifest.reasoner_engine != certificate.reasoner.engine
        || manifest.reasoner_version != certificate.reasoner.engine_version
        || manifest.adapter_version != certificate.reasoner.adapter_version
    {
        return Err(DirectProofValidationError::CertificateBinding);
    }
    let expected_ids = std::iter::once(manifest.completion_support_id.clone())
        .chain(manifest.answer_proofs.iter().map(|proof| proof.support_id.clone()))
        .collect::<BTreeSet<_>>();
    let actual_ids = certificate.support_references.iter()
        .map(|reference| reference.support_id.clone())
        .collect::<BTreeSet<_>>();
    if expected_ids != actual_ids || certificate.support_references.iter().any(|reference| {
        reference.kind != DirectSupportKind::ReasonerCheck
            || reference.artifact_sha256.as_deref() != Some(manifest_sha256)
    }) {
        return Err(DirectProofValidationError::CertificateBinding);
    }
    Ok(())
}

fn validate_ontology_inputs(inputs: &[DirectProofOntologyInput]) -> Result<(), DirectProofValidationError> {
    let mut previous: Option<&str> = None;
    for input in inputs {
        require_sha("ontologyInputs.sha256", &input.sha256)?;
        if previous.is_some_and(|value| value >= input.sha256.as_str()) {
            return Err(DirectProofValidationError::OntologyInputs);
        }
        let mut iri_previous: Option<&str> = None;
        for iri in &input.ontology_iris {
            if !absolute_iri(iri) || iri_previous.is_some_and(|value| value >= iri.as_str()) {
                return Err(DirectProofValidationError::OntologyInputs);
            }
            iri_previous = Some(iri);
        }
        previous = Some(&input.sha256);
    }
    Ok(())
}

fn require_sha(name: &'static str, value: &str) -> Result<(), DirectProofValidationError> {
    if value.len() != 64 || !value.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)) {
        return Err(DirectProofValidationError::Hash(name));
    }
    Ok(())
}

fn absolute_iri(value: &str) -> bool {
    let Some(colon) = value.find(':') else { return false; };
    colon > 0 && !value.chars().any(char::is_whitespace)
}

fn update_string(hash: &mut Sha256, value: &str) {
    hash.update(u64::try_from(value.len()).unwrap_or(u64::MAX).to_be_bytes());
    hash.update(value.as_bytes());
}

fn update_graph(hash: &mut Sha256, graph: &DirectBgpGraphContext) {
    match graph {
        DirectBgpGraphContext::Default { active_default_graph_sha256 } => {
            hash.update([1]); update_string(hash, active_default_graph_sha256);
        }
        DirectBgpGraphContext::Named { graph_iri } => {
            hash.update([2]); update_string(hash, graph_iri);
        }
    }
}

fn update_term(hash: &mut Sha256, term: &DirectBgpRdfTerm) {
    match term {
        DirectBgpRdfTerm::Iri { value } => { hash.update([1]); update_string(hash, value); }
        DirectBgpRdfTerm::BlankNode { value } => { hash.update([2]); update_string(hash, value); }
        DirectBgpRdfTerm::Literal { lexical_form, datatype_iri, language } => {
            hash.update([3]); update_string(hash, lexical_form); update_string(hash, datatype_iri);
            match language { Some(value) => { hash.update([1]); update_string(hash, value); }, None => hash.update([0]) }
        }
    }
}
