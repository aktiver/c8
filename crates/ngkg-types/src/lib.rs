//! Shared, closed-vocabulary contracts used by every NGKG phase.

pub mod direct_bgp;
pub mod direct_certificate;
pub mod direct_legality;
pub mod direct_exact;
pub mod direct_proof;

pub use direct_bgp::{
    DirectBgpCompleteness, DirectBgpExactness, DirectBgpFailure, DirectBgpFailureCode,
    DirectBgpGraphContext, DirectBgpOutcome, DirectBgpRdfTerm, DirectBgpResult,
    DirectBgpSolution, DirectBgpStatus, DirectBgpValidationError, validate_direct_bgp_result,
};
pub use direct_certificate::{
    DirectCertificate, DirectCertificateValidationError, DirectCertifiedOutcome,
    DirectCompletenessEvidence, DirectCompletenessMethod, DirectProofCoverage,
    DirectReasonerIdentity, DirectSupportKind, DirectSupportReference,
    direct_bgp_result_sha256, validate_direct_certificate, validate_direct_certificate_result,
};

pub use direct_legality::{
    DIRECT_BGP_CLASSIFIER_V1, DirectBgpLegalityFailure, DirectBgpLegalityFailureCode,
    DirectBgpLegalityRecord, DirectBgpLegalityReport, DirectBgpLegalityStatus, DirectBgpScope,
    DirectBgpLegalityValidationError, DirectVariableRole, DirectVariableRoleSource,
    DirectVariableTyping, validate_direct_bgp_legality_report,
};


pub use direct_proof::{
    DIRECT_PROOF_FORMAT_VERSION, DirectProofManifest, DirectProofOntologyInput,
    DirectProofValidationError, DirectReasonerCheckProof, direct_binding_sha256,
    direct_completion_support_id, direct_reasoner_support_id,
    validate_direct_proof_bundle, validate_direct_proof_manifest,
};

pub use direct_exact::{
    DIRECT_EXACT_ENGINE_V1, DIRECT_EXACT_FORMAT_VERSION, DirectExactBgpTemplate,
    DirectExactEntailedBinding, DirectExactOntologyInput, DirectExactPartition,
    DirectExactPartitionResult, DirectExactRequest, DirectExactTermPattern,
    DirectExactTriplePattern, DirectExactValidationError, DirectExactVariable,
    validate_direct_exact_partition_result, validate_direct_exact_request,
};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

/// The only exact entailment regime exposed by the first NGKG release.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum EntailmentRegime {
    /// OWL 2 Direct Semantics.
    Owl2Direct,
}

/// Publication behavior requested for a certified snapshot.
#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum PublicationPolicy {
    /// Certification creates an immutable unpublished snapshot.
    ManualAfterCertification,
    /// Certification also performs a guarded active-snapshot compare-and-swap.
    AutomaticAfterCertification,
}

impl PublicationPolicy {
    /// Stable catalog representation.
    #[must_use]
    pub const fn as_db(self) -> &'static str {
        match self {
            Self::ManualAfterCertification => "MANUAL_AFTER_CERTIFICATION",
            Self::AutomaticAfterCertification => "AUTOMATIC_AFTER_CERTIFICATION",
        }
    }
}

/// Immutable identity of a logical database read.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotRef {
    /// Dataset being read.
    pub dataset_id: Uuid,
    /// Exact immutable snapshot.
    pub snapshot_id: Uuid,
}

/// Metadata every snapshot-bound artifact must expose before it can be opened.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactIdentity {
    /// Dataset that owns the artifact.
    pub dataset_id: Uuid,
    /// Snapshot that gives the artifact meaning.
    pub snapshot_id: Uuid,
    /// Lowercase hexadecimal SHA-256 of the immutable bytes.
    pub sha256: String,
}

/// Fail-closed snapshot compatibility error.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum SnapshotError {
    /// A component came from a different logical snapshot.
    #[error("snapshot mismatch: requested {requested}, component contains {found}")]
    Mismatch { requested: Uuid, found: Uuid },
}

/// Require an artifact to belong to the requested snapshot.
pub fn require_snapshot(identity: &ArtifactIdentity, requested: Uuid) -> Result<(), SnapshotError> {
    if identity.snapshot_id != requested {
        return Err(SnapshotError::Mismatch {
            requested,
            found: identity.snapshot_id,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{ArtifactIdentity, SnapshotError, require_snapshot};
    use uuid::Uuid;

    #[test]
    fn mixed_snapshots_fail_closed() {
        let requested = Uuid::from_u128(1);
        let identity = ArtifactIdentity {
            dataset_id: Uuid::from_u128(2),
            snapshot_id: Uuid::from_u128(3),
            sha256: "00".repeat(32),
        };
        assert_eq!(
            require_snapshot(&identity, requested),
            Err(SnapshotError::Mismatch {
                requested,
                found: Uuid::from_u128(3)
            })
        );
    }
}
