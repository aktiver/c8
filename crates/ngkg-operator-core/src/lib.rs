//! Level-based compilation reconciliation decisions.

use ngkg_catalog::JobState;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Durable stage resources the Kubernetes operator may reconcile.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StageAction {
    PlanSource,
    ValidateMapping,
    CreateProjectionJob,
    CreateIdentityReducers,
    CreateIndexReducers,
    CreateReasoningJob,
    CreateCertificationJob,
    CreateUnpublishedSnapshot,
    ObservePublished,
    TerminalFailure,
    TerminalCancellation,
}

/// Concise observed stage data loaded from catalog, not inferred from pod history.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct StageObservation {
    pub job_state: JobState,
    pub expected_partitions: u32,
    pub succeeded_partitions: u32,
    pub failed_partitions: u32,
    pub reducer_manifests_valid: bool,
}

/// Reconciliation refuses to advance incomplete or corrupt stages.
#[derive(Debug, Error, Eq, PartialEq)]
pub enum ReconcileError {
    #[error("partition count is inconsistent with catalog truth")]
    PartitionCount,
    #[error("a stage contains failed deterministic partitions")]
    DeterministicPartitionFailure,
    #[error("required reducer manifests are invalid or missing")]
    InvalidReducers,
}

/// Calculate the next idempotent action from durable state.
pub fn next_action(observed: &StageObservation) -> Result<StageAction, ReconcileError> {
    if observed.succeeded_partitions > observed.expected_partitions {
        return Err(ReconcileError::PartitionCount);
    }
    if observed.failed_partitions > 0 {
        return Err(ReconcileError::DeterministicPartitionFailure);
    }
    let complete = observed.expected_partitions > 0
        && observed.succeeded_partitions == observed.expected_partitions;
    use JobState::{
        Cancelled, Certified, Failed, Identified, Indexed, MappingValidated, Partitioned,
        Projected, Published, Reasoned, Registered, SourcePlanned, SpineWritten,
    };
    match observed.job_state {
        Registered => Ok(StageAction::PlanSource),
        SourcePlanned => Ok(StageAction::ValidateMapping),
        MappingValidated => Ok(StageAction::CreateProjectionJob),
        Partitioned if complete => Ok(StageAction::CreateIdentityReducers),
        Partitioned => Ok(StageAction::CreateProjectionJob),
        Projected if observed.reducer_manifests_valid => Ok(StageAction::CreateIdentityReducers),
        Projected => Err(ReconcileError::InvalidReducers),
        Identified | SpineWritten if observed.reducer_manifests_valid => {
            Ok(StageAction::CreateIndexReducers)
        }
        Identified | SpineWritten => Err(ReconcileError::InvalidReducers),
        Indexed => Ok(StageAction::CreateReasoningJob),
        Reasoned => Ok(StageAction::CreateCertificationJob),
        Certified => Ok(StageAction::CreateUnpublishedSnapshot),
        Published => Ok(StageAction::ObservePublished),
        Failed => Ok(StageAction::TerminalFailure),
        Cancelled => Ok(StageAction::TerminalCancellation),
    }
}

/// Environment variable names for the immutable Phase 40 exact-reasoner ceiling bundle.
pub const PHASE40_ENV_MAX_CANDIDATE_BINDINGS: &str = "NGKG_PHASE40_DIRECT_MAX_CANDIDATE_BINDINGS";
pub const PHASE40_ENV_MAX_PARTITION_CANDIDATES: &str =
    "NGKG_PHASE40_DIRECT_MAX_PARTITION_CANDIDATES";
pub const PHASE40_ENV_MAX_EXACT_PARTITIONS: &str = "NGKG_PHASE40_DIRECT_MAX_EXACT_PARTITIONS";
pub const PHASE40_ENV_MAX_GROUNDED_AXIOMS_PER_CANDIDATE: &str =
    "NGKG_PHASE40_DIRECT_MAX_GROUNDED_AXIOMS_PER_CANDIDATE";
pub const PHASE40_ENV_MAX_GROUNDED_RDF_BYTES_PER_CANDIDATE: &str =
    "NGKG_PHASE40_DIRECT_MAX_GROUNDED_RDF_BYTES_PER_CANDIDATE";
pub const PHASE40_ENV_REASONER_CONCURRENCY: &str = "NGKG_PHASE40_DIRECT_REASONER_CONCURRENCY";
pub const PHASE40_ENV_REASONER_HEAP_MIB_PER_LANE: &str =
    "NGKG_PHASE40_DIRECT_REASONER_HEAP_MIB_PER_LANE";
pub const PHASE40_ENV_REASONER_TIMEOUT_SECONDS: &str =
    "NGKG_PHASE40_DIRECT_REASONER_TIMEOUT_SECONDS";
pub const PHASE40_ENV_MAX_CERTIFICATE_BYTES: &str = "NGKG_PHASE40_DIRECT_MAX_CERTIFICATE_BYTES";
pub const PHASE40_ENV_MAX_PROOF_SUPPORT_IDS: &str = "NGKG_PHASE40_DIRECT_MAX_PROOF_SUPPORT_IDS";

const PHASE40_MAX_REASONER_LANES_REVIEWED: u64 = 32;
const PHASE40_MAX_EXACT_PARTITIONS_REVIEWED: u64 = 4096;
const PHASE40_MAX_PROOF_SUPPORT_IDS_REVIEWED: u64 = 1_000_000;

/// Immutable Phase 40 exact-reasoner ceilings loaded by an operator.
///
/// Operators validate this bundle once at startup and copy the exact values into every
/// generated reference/reasoner Job.  The SHA uses the same domain and field order as the
/// reference worker so the operator annotation and the worker's locally recomputed policy
/// identity are byte-for-byte comparable.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Phase40DirectCeilings {
    pub max_candidate_bindings: u64,
    pub max_partition_candidates: u64,
    pub max_exact_partitions: u64,
    pub max_grounded_axioms_per_candidate: u64,
    pub max_grounded_rdf_bytes_per_candidate: u64,
    pub reasoner_concurrency: u64,
    pub reasoner_heap_mib_per_lane: u64,
    pub reasoner_timeout_seconds: u64,
    pub max_certificate_bytes: u64,
    pub max_proof_support_ids: u64,
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum Phase40CeilingError {
    #[error("required Phase 40 operator ceiling {0} is missing or empty")]
    Missing(&'static str),
    #[error("Phase 40 operator ceiling {0} must be a positive unsigned integer")]
    Invalid(&'static str),
    #[error("Phase 40 direct maxPartitionCandidates exceeds maxCandidateBindings")]
    PartitionExceedsCandidateSpace,
    #[error("Phase 40 direct maxExactPartitions cannot cover the candidate space")]
    PartitionCoverage,
    #[error("Phase 40 direct reasonerConcurrency exceeds reviewed cap")]
    ReasonerConcurrency,
    #[error("Phase 40 direct maxExactPartitions exceeds reviewed cap")]
    ExactPartitions,
    #[error("Phase 40 direct maxProofSupportIds exceeds reviewed cap")]
    ProofSupportIds,
}

impl Phase40DirectCeilings {
    /// Load and validate the operator's immutable Phase 40 bundle from the environment.
    pub fn from_env() -> Result<Self, Phase40CeilingError> {
        let value = Self {
            max_candidate_bindings: phase40_required_u64(PHASE40_ENV_MAX_CANDIDATE_BINDINGS)?,
            max_partition_candidates: phase40_required_u64(PHASE40_ENV_MAX_PARTITION_CANDIDATES)?,
            max_exact_partitions: phase40_required_u64(PHASE40_ENV_MAX_EXACT_PARTITIONS)?,
            max_grounded_axioms_per_candidate: phase40_required_u64(
                PHASE40_ENV_MAX_GROUNDED_AXIOMS_PER_CANDIDATE,
            )?,
            max_grounded_rdf_bytes_per_candidate: phase40_required_u64(
                PHASE40_ENV_MAX_GROUNDED_RDF_BYTES_PER_CANDIDATE,
            )?,
            reasoner_concurrency: phase40_required_u64(PHASE40_ENV_REASONER_CONCURRENCY)?,
            reasoner_heap_mib_per_lane: phase40_required_u64(
                PHASE40_ENV_REASONER_HEAP_MIB_PER_LANE,
            )?,
            reasoner_timeout_seconds: phase40_required_u64(PHASE40_ENV_REASONER_TIMEOUT_SECONDS)?,
            max_certificate_bytes: phase40_required_u64(PHASE40_ENV_MAX_CERTIFICATE_BYTES)?,
            max_proof_support_ids: phase40_required_u64(PHASE40_ENV_MAX_PROOF_SUPPORT_IDS)?,
        };
        value.validate()?;
        Ok(value)
    }

    /// Validate cross-field invariants that must hold before an operator can create Jobs.
    pub fn validate(&self) -> Result<(), Phase40CeilingError> {
        if self.max_partition_candidates > self.max_candidate_bindings {
            return Err(Phase40CeilingError::PartitionExceedsCandidateSpace);
        }
        if self
            .max_candidate_bindings
            .div_ceil(self.max_partition_candidates)
            > self.max_exact_partitions
        {
            return Err(Phase40CeilingError::PartitionCoverage);
        }
        if self.reasoner_concurrency > PHASE40_MAX_REASONER_LANES_REVIEWED {
            return Err(Phase40CeilingError::ReasonerConcurrency);
        }
        if self.max_exact_partitions > PHASE40_MAX_EXACT_PARTITIONS_REVIEWED {
            return Err(Phase40CeilingError::ExactPartitions);
        }
        if self.max_proof_support_ids > PHASE40_MAX_PROOF_SUPPORT_IDS_REVIEWED {
            return Err(Phase40CeilingError::ProofSupportIds);
        }
        Ok(())
    }

    /// Ordered environment pairs copied verbatim into generated reference/reasoner Jobs.
    pub fn env_pairs(&self) -> Vec<(&'static str, String)> {
        vec![
            (
                PHASE40_ENV_MAX_CANDIDATE_BINDINGS,
                self.max_candidate_bindings.to_string(),
            ),
            (
                PHASE40_ENV_MAX_PARTITION_CANDIDATES,
                self.max_partition_candidates.to_string(),
            ),
            (
                PHASE40_ENV_MAX_EXACT_PARTITIONS,
                self.max_exact_partitions.to_string(),
            ),
            (
                PHASE40_ENV_MAX_GROUNDED_AXIOMS_PER_CANDIDATE,
                self.max_grounded_axioms_per_candidate.to_string(),
            ),
            (
                PHASE40_ENV_MAX_GROUNDED_RDF_BYTES_PER_CANDIDATE,
                self.max_grounded_rdf_bytes_per_candidate.to_string(),
            ),
            (
                PHASE40_ENV_REASONER_CONCURRENCY,
                self.reasoner_concurrency.to_string(),
            ),
            (
                PHASE40_ENV_REASONER_HEAP_MIB_PER_LANE,
                self.reasoner_heap_mib_per_lane.to_string(),
            ),
            (
                PHASE40_ENV_REASONER_TIMEOUT_SECONDS,
                self.reasoner_timeout_seconds.to_string(),
            ),
            (
                PHASE40_ENV_MAX_CERTIFICATE_BYTES,
                self.max_certificate_bytes.to_string(),
            ),
            (
                PHASE40_ENV_MAX_PROOF_SUPPORT_IDS,
                self.max_proof_support_ids.to_string(),
            ),
        ]
    }

    /// SHA-256 identity shared with `TrustedPhase40DirectCeilings::bundle_sha256`.
    pub fn bundle_sha256(&self) -> String {
        use sha2::{Digest, Sha256};
        let mut hash = Sha256::new();
        hash.update(b"ngkg-phase40-reference-worker-ceilings-v1\0");
        for (name, value) in self.env_pairs() {
            hash.update(name.as_bytes());
            hash.update(b"=");
            hash.update(value.as_bytes());
            hash.update(b"\n");
        }
        hex::encode(hash.finalize())
    }
}

fn phase40_required_u64(name: &'static str) -> Result<u64, Phase40CeilingError> {
    let raw = std::env::var(name).map_err(|_| Phase40CeilingError::Missing(name))?;
    if raw.trim().is_empty() {
        return Err(Phase40CeilingError::Missing(name));
    }
    raw.parse::<u64>()
        .ok()
        .filter(|value| *value > 0)
        .ok_or(Phase40CeilingError::Invalid(name))
}

#[cfg(test)]
mod phase40_13_tests {
    use super::*;

    fn ceilings() -> Phase40DirectCeilings {
        Phase40DirectCeilings {
            max_candidate_bindings: 10_000_000,
            max_partition_candidates: 250_000,
            max_exact_partitions: 4096,
            max_grounded_axioms_per_candidate: 65_536,
            max_grounded_rdf_bytes_per_candidate: 16_777_216,
            reasoner_concurrency: 8,
            reasoner_heap_mib_per_lane: 4096,
            reasoner_timeout_seconds: 300,
            max_certificate_bytes: 536_870_912,
            max_proof_support_ids: 1_000_000,
        }
    }

    #[test]
    fn deterministic_bundle_has_all_ten_environment_values() {
        let value = ceilings();
        assert_eq!(value.env_pairs().len(), 10);
        assert_eq!(value.bundle_sha256().len(), 64);
        assert!(value.validate().is_ok());
    }

    #[test]
    fn impossible_partition_coverage_is_rejected() {
        let mut value = ceilings();
        value.max_exact_partitions = 1;
        assert_eq!(
            value.validate(),
            Err(Phase40CeilingError::PartitionCoverage)
        );
    }
}
