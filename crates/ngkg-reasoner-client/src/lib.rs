//! Versioned mTLS boundary to a certified OWL 2 DL reasoner.

use std::{collections::BTreeSet, time::Duration};

use ngkg_types::{
    DirectBgpLegalityReport, DirectBgpLegalityStatus, DirectBgpLegalityValidationError,
    DirectBgpResult, DirectBgpValidationError, DirectCertificate, DirectCertificateValidationError,
    DirectProofManifest, DirectProofValidationError, validate_direct_bgp_legality_report,
    validate_direct_bgp_result, validate_direct_certificate_result, validate_direct_proof_bundle,
};
use reqwest::{Certificate, Client, Identity, Url};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

/// Exact reasoner request bound to one immutable module and snapshot.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReasoningRequest {
    pub dataset_id: Uuid,
    pub snapshot_id: Uuid,
    pub ontology_bundle_hash: [u8; 32],
    pub module_manifest_uri: String,
    pub entailment_regime: EntailmentRegime,
    pub requested_artifacts: Vec<ReasonerArtifactKind>,
    pub memory_budget_bytes: u64,
    pub deadline_unix_ms: i64,
}

/// Only exact OWL 2 Direct Semantics is accepted on this path.
#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum EntailmentRegime {
    Owl2Direct,
}

/// Immutable reasoner output families.
#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReasonerArtifactKind {
    ConsistencyReport,
    ClassExtents,
    PropertyExtents,
    SelectedClosure,
    ProofDag,
    DependencyDag,
    CoverageContribution,
}

/// Signed/hashed adapter result metadata.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ReasoningResult {
    pub snapshot_id: Uuid,
    pub reasoner_name: String,
    pub reasoner_version: String,
    pub input_hash: [u8; 32],
    pub consistent: bool,
    pub artifact_manifest_uri: String,
    pub artifact_manifest_sha256: [u8; 32],
}

/// Query-plan-specific proof that the compiled path is exact.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct CoverageCertificate {
    pub certificate_id: String,
    pub snapshot_id: Uuid,
    pub entailment_regime: String,
    pub authorized_graph_set_hash: [u8; 32],
    pub selected_graph_set_hash: [u8; 32],
    pub query_algebra_hash: [u8; 32],
    pub covered_operator_hashes: Vec<[u8; 32]>,
    pub ontology_module_hashes: Vec<[u8; 32]>,
    pub closure_root_hash: [u8; 32],
    pub proof_root_hash: [u8; 32],
    pub dictionary_hash: [u8; 32],
    pub policy_hash: [u8; 32],
    pub expires_with_snapshot: bool,
}

/// Explicit planner decision; there is no best-effort exact branch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CoverageDecision {
    CertifiedCompiled,
    ExpandAndRecertify,
    ExactReasoner,
}

/// Expected immutable identities for one Direct-BGP result crossing the reasoner boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DirectBgpExpectedBinding {
    pub dataset_id: Uuid,
    pub snapshot_id: Uuid,
    pub query_sha256: String,
    pub bgp_sha256: String,
    pub active_dataset_sha256: String,
    pub authorized_graph_set_sha256: String,
    pub owl_signature_sha256: String,
    pub datatype_policy_sha256: String,
}

/// Expected immutable identities for the Phase 40.7 legality report consumed by Phase 40.8.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DirectBgpLegalityExpectedBinding {
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
}

/// Certified remote dependency.
#[derive(Clone)]
pub struct ReasonerClient {
    client: Client,
    endpoint: Url,
    expected_name: String,
    expected_version: String,
}

/// Reasoner boundary failures.
#[derive(Debug, Error)]
pub enum ReasonerError {
    #[error("reasoner endpoint must be a valid HTTPS URL")]
    InvalidEndpoint,
    #[error("mTLS client identity is invalid: {0}")]
    InvalidIdentity(reqwest::Error),
    #[error("reasoner CA bundle is invalid: {0}")]
    InvalidCa(reqwest::Error),
    #[error("reasoner client construction failed: {0}")]
    Client(reqwest::Error),
    #[error("reasoner request failed: {0}")]
    Request(reqwest::Error),
    #[error("reasoner returned snapshot mismatch")]
    SnapshotMismatch,
    #[error("reasoner implementation/version mismatch")]
    VersionMismatch,
    #[error("reasoner input hash mismatch")]
    InputMismatch,
    #[error("ontology is inconsistent under the configured exact policy")]
    InconsistentOntology,
    #[error("coverage certificate is not bound to the requested snapshot/algebra/policy")]
    InvalidCoverage,
    #[error("Direct-BGP result contract is invalid: {0}")]
    InvalidDirectBgpContract(#[from] DirectBgpValidationError),
    #[error(
        "Direct-BGP result is not bound to the requested dataset/snapshot/query/BGP/authorization/policy identities"
    )]
    DirectBgpBindingMismatch,
    #[error("Direct certificate contract is invalid: {0}")]
    InvalidDirectCertificateContract(#[from] DirectCertificateValidationError),
    #[error(
        "Direct certificate does not bind the exact requested result/dataset/snapshot/query/BGP/authorization/policy identities"
    )]
    DirectCertificateBindingMismatch,
    #[error("Direct-BGP legality report contract is invalid: {0}")]
    InvalidDirectBgpLegalityContract(#[from] DirectBgpLegalityValidationError),
    #[error("Direct-BGP legality report is not bound to the requested semantic identities")]
    DirectBgpLegalityBindingMismatch,
    #[error("Direct-BGP ordinal is absent or was rejected by the Phase 40.7 legality classifier")]
    IllegalDirectBgp,
    #[error("Direct proof/support manifest is invalid: {0}")]
    InvalidDirectProofContract(#[from] DirectProofValidationError),
    #[error("Direct proof/support manifest or certificate is not bound to the exact result")]
    DirectProofBindingMismatch,
}

/// Validate and bind one Phase 40.7 legality decision before any Phase 40.8 exact-reasoner work.
///
/// This is the fail-closed semantic handoff: an exact-reasoner worker may not manufacture its
/// own interpretation of a BGP or skip the query-level W3C admission decision.
pub fn require_legal_direct_bgp<'a>(
    report: &'a DirectBgpLegalityReport,
    bgp_ordinal: u64,
    expected: &DirectBgpLegalityExpectedBinding,
) -> Result<&'a ngkg_types::DirectBgpLegalityRecord, ReasonerError> {
    validate_direct_bgp_legality_report(report)?;
    if report.dataset_id != expected.dataset_id
        || report.snapshot_id != expected.snapshot_id
        || report.query_sha256 != expected.query_sha256
        || report.sparql_algebra_sha256 != expected.sparql_algebra_sha256
        || report.active_dataset_sha256 != expected.active_dataset_sha256
        || report.authorized_graph_set_sha256 != expected.authorized_graph_set_sha256
        || report.owl_signature_sha256 != expected.owl_signature_sha256
        || report.datatype_policy_sha256 != expected.datatype_policy_sha256
        || report.owl_profile_qualification_sha256 != expected.owl_profile_qualification_sha256
        || report.owl_consistency_qualification_sha256
            != expected.owl_consistency_qualification_sha256
    {
        return Err(ReasonerError::DirectBgpLegalityBindingMismatch);
    }
    let Some(record) = report
        .bgps
        .iter()
        .find(|record| record.ordinal == bgp_ordinal)
    else {
        return Err(ReasonerError::IllegalDirectBgp);
    };
    if record.status != DirectBgpLegalityStatus::Legal || !record.grounded_owl2dl_check_required {
        return Err(ReasonerError::IllegalDirectBgp);
    }
    Ok(record)
}

/// Validate the Phase 40.3 result contract and every immutable identity expected by the caller.
pub fn validate_direct_bgp_result_binding(
    result: &DirectBgpResult,
    expected: &DirectBgpExpectedBinding,
) -> Result<(), ReasonerError> {
    validate_direct_bgp_result(result)?;
    if result.dataset_id != expected.dataset_id
        || result.snapshot_id != expected.snapshot_id
        || result.query_sha256 != expected.query_sha256
        || result.bgp_sha256 != expected.bgp_sha256
        || result.active_dataset_sha256 != expected.active_dataset_sha256
        || result.authorized_graph_set_sha256 != expected.authorized_graph_set_sha256
        || result.owl_signature_sha256 != expected.owl_signature_sha256
        || result.datatype_policy_sha256 != expected.datatype_policy_sha256
    {
        return Err(ReasonerError::DirectBgpBindingMismatch);
    }
    Ok(())
}

/// Validate the Phase 40.4 certificate and bind it to the exact complete Direct-BGP result.
pub fn validate_direct_certificate_binding(
    certificate: &DirectCertificate,
    result: &DirectBgpResult,
    expected: &DirectBgpExpectedBinding,
) -> Result<(), ReasonerError> {
    validate_direct_bgp_result_binding(result, expected)?;
    if validate_direct_certificate_result(certificate, result).is_err() {
        return Err(ReasonerError::DirectCertificateBindingMismatch);
    }
    Ok(())
}

/// Validate the Phase 40.9 proof/support manifest and require the certificate to cover every
/// exact solution multiplicity plus the global exhaustive-completion barrier.
pub fn validate_direct_proof_binding(
    proof_manifest: &DirectProofManifest,
    proof_manifest_sha256: &str,
    certificate: &DirectCertificate,
    result: &DirectBgpResult,
    expected: &DirectBgpExpectedBinding,
) -> Result<(), ReasonerError> {
    validate_direct_bgp_result_binding(result, expected)?;
    validate_direct_proof_bundle(proof_manifest, result, certificate, proof_manifest_sha256)
        .map_err(|_| ReasonerError::DirectProofBindingMismatch)?;
    Ok(())
}

impl ReasonerClient {
    /// Build a strict mTLS client. Plain HTTP and system-only trust are rejected.
    pub fn new(
        endpoint: &str,
        client_identity_pem: &[u8],
        ca_pem: &[u8],
        expected_name: String,
        expected_version: String,
    ) -> Result<Self, ReasonerError> {
        let endpoint = Url::parse(endpoint).map_err(|_| ReasonerError::InvalidEndpoint)?;
        if endpoint.scheme() != "https" {
            return Err(ReasonerError::InvalidEndpoint);
        }
        let identity =
            Identity::from_pem(client_identity_pem).map_err(ReasonerError::InvalidIdentity)?;
        let ca = Certificate::from_pem(ca_pem).map_err(ReasonerError::InvalidCa)?;
        let client = Client::builder()
            .identity(identity)
            .add_root_certificate(ca)
            .https_only(true)
            .timeout(Duration::from_secs(300))
            .build()
            .map_err(ReasonerError::Client)?;
        Ok(Self {
            client,
            endpoint,
            expected_name,
            expected_version,
        })
    }

    /// Execute and validate one immutable reasoner operation.
    pub async fn reason(
        &self,
        request: &ReasoningRequest,
        expected_input_hash: [u8; 32],
    ) -> Result<ReasoningResult, ReasonerError> {
        let endpoint = self
            .endpoint
            .join("v1/reason")
            .map_err(|_| ReasonerError::InvalidEndpoint)?;
        let result = self
            .client
            .post(endpoint)
            .json(request)
            .send()
            .await
            .map_err(ReasonerError::Request)?
            .error_for_status()
            .map_err(ReasonerError::Request)?
            .json::<ReasoningResult>()
            .await
            .map_err(ReasonerError::Request)?;
        if result.snapshot_id != request.snapshot_id {
            return Err(ReasonerError::SnapshotMismatch);
        }
        if result.reasoner_name != self.expected_name
            || result.reasoner_version != self.expected_version
        {
            return Err(ReasonerError::VersionMismatch);
        }
        if result.input_hash != expected_input_hash {
            return Err(ReasonerError::InputMismatch);
        }
        if !result.consistent {
            return Err(ReasonerError::InconsistentOntology);
        }
        Ok(result)
    }
}

/// Decide whether all required operators are covered by this exact certificate.
pub fn decide_coverage(
    certificate: Option<&CoverageCertificate>,
    snapshot_id: Uuid,
    query_algebra_hash: [u8; 32],
    policy_hash: [u8; 32],
    required_operators: &BTreeSet<[u8; 32]>,
    expandable_dependencies_exist: bool,
) -> CoverageDecision {
    if let Some(certificate) = certificate {
        let covered = certificate
            .covered_operator_hashes
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        if certificate.snapshot_id == snapshot_id
            && certificate.query_algebra_hash == query_algebra_hash
            && certificate.policy_hash == policy_hash
            && certificate.expires_with_snapshot
            && required_operators.is_subset(&covered)
        {
            return CoverageDecision::CertifiedCompiled;
        }
    }
    if expandable_dependencies_exist {
        CoverageDecision::ExpandAndRecertify
    } else {
        CoverageDecision::ExactReasoner
    }
}
