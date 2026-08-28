//! Phase 40.11 trusted reference-worker ceiling ingestion.
//!
//! The Helm chart is the authority for these values.  The reference worker never trusts
//! per-job JSON to raise a ceiling: a job may request a smaller budget, but every requested
//! value is checked against this environment bundle before any ontology or JVM work begins.

use std::{env, fs, thread};

use crate::direct_job::DirectJobLimits;
use sha2::{Digest, Sha256};

const MAX_REASONER_LANES_REVIEWED: usize = 32;
const MAX_EXACT_PARTITIONS_REVIEWED: u64 = 4096;
const MAX_PROOF_SUPPORT_IDS_REVIEWED: u64 = 1_000_000;
const MIB: u64 = 1024 * 1024;

pub const ENV_MAX_CANDIDATE_BINDINGS: &str = "NGKG_PHASE40_DIRECT_MAX_CANDIDATE_BINDINGS";
pub const ENV_MAX_PARTITION_CANDIDATES: &str = "NGKG_PHASE40_DIRECT_MAX_PARTITION_CANDIDATES";
pub const ENV_MAX_EXACT_PARTITIONS: &str = "NGKG_PHASE40_DIRECT_MAX_EXACT_PARTITIONS";
pub const ENV_MAX_GROUNDED_AXIOMS_PER_CANDIDATE: &str = "NGKG_PHASE40_DIRECT_MAX_GROUNDED_AXIOMS_PER_CANDIDATE";
pub const ENV_MAX_GROUNDED_RDF_BYTES_PER_CANDIDATE: &str = "NGKG_PHASE40_DIRECT_MAX_GROUNDED_RDF_BYTES_PER_CANDIDATE";
pub const ENV_REASONER_CONCURRENCY: &str = "NGKG_PHASE40_DIRECT_REASONER_CONCURRENCY";
pub const ENV_REASONER_HEAP_MIB_PER_LANE: &str = "NGKG_PHASE40_DIRECT_REASONER_HEAP_MIB_PER_LANE";
pub const ENV_REASONER_TIMEOUT_SECONDS: &str = "NGKG_PHASE40_DIRECT_REASONER_TIMEOUT_SECONDS";
pub const ENV_MAX_CERTIFICATE_BYTES: &str = "NGKG_PHASE40_DIRECT_MAX_CERTIFICATE_BYTES";
pub const ENV_MAX_PROOF_SUPPORT_IDS: &str = "NGKG_PHASE40_DIRECT_MAX_PROOF_SUPPORT_IDS";
pub const ENV_CEILINGS_SHA256: &str = "NGKG_PHASE40_DIRECT_CEILINGS_SHA256";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TrustedPhase40DirectCeilings {
    pub max_candidate_bindings: u64,
    pub max_partition_candidates: u64,
    pub max_exact_partitions: u64,
    pub max_grounded_axioms_per_candidate: u64,
    pub max_grounded_rdf_bytes_per_candidate: u64,
    pub reasoner_concurrency: usize,
    pub reasoner_heap_mib_per_lane: u64,
    pub reasoner_timeout_seconds: u64,
    pub max_certificate_bytes: u64,
    pub max_proof_support_ids: u64,
}

impl TrustedPhase40DirectCeilings {
    pub fn from_env() -> Result<Self, String> {
        let ceilings = Self {
            max_candidate_bindings: required_u64(ENV_MAX_CANDIDATE_BINDINGS)?,
            max_partition_candidates: required_u64(ENV_MAX_PARTITION_CANDIDATES)?,
            max_exact_partitions: required_u64(ENV_MAX_EXACT_PARTITIONS)?,
            max_grounded_axioms_per_candidate: required_u64(ENV_MAX_GROUNDED_AXIOMS_PER_CANDIDATE)?,
            max_grounded_rdf_bytes_per_candidate: required_u64(ENV_MAX_GROUNDED_RDF_BYTES_PER_CANDIDATE)?,
            reasoner_concurrency: required_usize(ENV_REASONER_CONCURRENCY)?,
            reasoner_heap_mib_per_lane: required_u64(ENV_REASONER_HEAP_MIB_PER_LANE)?,
            reasoner_timeout_seconds: required_u64(ENV_REASONER_TIMEOUT_SECONDS)?,
            max_certificate_bytes: required_u64(ENV_MAX_CERTIFICATE_BYTES)?,
            max_proof_support_ids: required_u64(ENV_MAX_PROOF_SUPPORT_IDS)?,
        };
        ceilings.validate_cross_fields()?;
        ceilings.validate_local_resources()?;
        let expected_sha256 = required(ENV_CEILINGS_SHA256)?;
        let observed_sha256 = ceilings.bundle_sha256();
        if expected_sha256 != observed_sha256 {
            return Err(format!(
                "Phase 40 direct ceiling bundle SHA mismatch: expected {expected_sha256}, computed {observed_sha256}"
            ));
        }
        Ok(ceilings)
    }

    fn validate_cross_fields(&self) -> Result<(), String> {
        if self.max_partition_candidates > self.max_candidate_bindings {
            return Err("Phase 40 direct maxPartitionCandidates exceeds maxCandidateBindings".to_owned());
        }
        let required_partitions = self
            .max_candidate_bindings
            .div_ceil(self.max_partition_candidates);
        if required_partitions > self.max_exact_partitions {
            return Err(format!(
                "Phase 40 direct maxExactPartitions {} cannot cover {} required partitions",
                self.max_exact_partitions, required_partitions
            ));
        }
        if self.reasoner_concurrency > MAX_REASONER_LANES_REVIEWED {
            return Err(format!(
                "Phase 40 direct reasonerConcurrency exceeds reviewed cap {MAX_REASONER_LANES_REVIEWED}"
            ));
        }
        if self.max_exact_partitions > MAX_EXACT_PARTITIONS_REVIEWED {
            return Err(format!(
                "Phase 40 direct maxExactPartitions exceeds runtime cap {MAX_EXACT_PARTITIONS_REVIEWED}"
            ));
        }
        if self.max_proof_support_ids > MAX_PROOF_SUPPORT_IDS_REVIEWED {
            return Err(format!(
                "Phase 40 direct maxProofSupportIds exceeds runtime cap {MAX_PROOF_SUPPORT_IDS_REVIEWED}"
            ));
        }
        Ok(())
    }

    fn validate_local_resources(&self) -> Result<(), String> {
        let available = thread::available_parallelism().map_or(1, |value| value.get());
        if self.reasoner_concurrency > available {
            return Err(format!(
                "Phase 40 direct reasonerConcurrency {} exceeds CPU lanes visible to this worker {}",
                self.reasoner_concurrency, available
            ));
        }
        if let Some(memory_limit) = cgroup_memory_limit_bytes() {
            let heap_budget = (self.reasoner_concurrency as u64)
                .checked_mul(self.reasoner_heap_mib_per_lane)
                .and_then(|value| value.checked_mul(MIB))
                .ok_or_else(|| "Phase 40 reasoner heap budget overflow".to_owned())?;
            // Preserve at least 20% for OWLAPI/HermiT non-heap, Rust, mmap/file buffers and OS work.
            if heap_budget > memory_limit.saturating_mul(80) / 100 {
                return Err(format!(
                    "Phase 40 direct JVM heap budget {heap_budget} exceeds 80% of cgroup memory limit {memory_limit}"
                ));
            }
        }
        Ok(())
    }

    pub fn bundle_sha256(&self) -> String {
        let mut hash = Sha256::new();
        hash.update(b"ngkg-phase40-reference-worker-ceilings-v1\0");
        for (name, value) in [
            (ENV_MAX_CANDIDATE_BINDINGS, self.max_candidate_bindings.to_string()),
            (ENV_MAX_PARTITION_CANDIDATES, self.max_partition_candidates.to_string()),
            (ENV_MAX_EXACT_PARTITIONS, self.max_exact_partitions.to_string()),
            (ENV_MAX_GROUNDED_AXIOMS_PER_CANDIDATE, self.max_grounded_axioms_per_candidate.to_string()),
            (ENV_MAX_GROUNDED_RDF_BYTES_PER_CANDIDATE, self.max_grounded_rdf_bytes_per_candidate.to_string()),
            (ENV_REASONER_CONCURRENCY, self.reasoner_concurrency.to_string()),
            (ENV_REASONER_HEAP_MIB_PER_LANE, self.reasoner_heap_mib_per_lane.to_string()),
            (ENV_REASONER_TIMEOUT_SECONDS, self.reasoner_timeout_seconds.to_string()),
            (ENV_MAX_CERTIFICATE_BYTES, self.max_certificate_bytes.to_string()),
            (ENV_MAX_PROOF_SUPPORT_IDS, self.max_proof_support_ids.to_string()),
        ] {
            hash.update(name.as_bytes());
            hash.update(b"=");
            hash.update(value.as_bytes());
            hash.update(b"\n");
        }
        hex::encode(hash.finalize())
    }

    pub fn enforce_job(&self, requested: &DirectJobLimits) -> Result<(), String> {
        cap_u64("maxCandidateBindings", requested.max_candidate_bindings, self.max_candidate_bindings)?;
        cap_u64("maxPartitionCandidates", requested.max_partition_candidates, self.max_partition_candidates)?;
        cap_u64(
            "maxGroundedAxiomsPerCandidate",
            requested.max_grounded_axioms_per_candidate,
            self.max_grounded_axioms_per_candidate,
        )?;
        cap_u64(
            "maxGroundedRdfBytesPerCandidate",
            requested.max_grounded_rdf_bytes_per_candidate,
            self.max_grounded_rdf_bytes_per_candidate,
        )?;
        if requested.max_local_reasoner_lanes == 0 || requested.max_local_reasoner_lanes > self.reasoner_concurrency {
            return Err(format!(
                "job maxLocalReasonerLanes {} exceeds trusted Phase 40 reasonerConcurrency {}",
                requested.max_local_reasoner_lanes, self.reasoner_concurrency
            ));
        }
        cap_u64(
            "reasonerHeapMibPerLane",
            requested.reasoner_heap_mib_per_lane,
            self.reasoner_heap_mib_per_lane,
        )?;
        cap_u64(
            "reasonerTimeoutSeconds",
            requested.reasoner_timeout_seconds,
            self.reasoner_timeout_seconds,
        )?;
        Ok(())
    }
}

fn cap_u64(name: &str, requested: u64, trusted: u64) -> Result<(), String> {
    if requested == 0 {
        return Err(format!("job {name} must be positive"));
    }
    if requested > trusted {
        return Err(format!("job {name} {requested} exceeds trusted Phase 40 ceiling {trusted}"));
    }
    Ok(())
}

fn required_u64(name: &str) -> Result<u64, String> {
    let raw = required(name)?;
    let value = raw.parse::<u64>().map_err(|_| format!("{name} must be an unsigned integer"))?;
    if value == 0 { return Err(format!("{name} must be positive")); }
    Ok(value)
}

fn required_usize(name: &str) -> Result<usize, String> {
    let value = required_u64(name)?;
    usize::try_from(value).map_err(|_| format!("{name} exceeds this platform's usize range"))
}

fn required(name: &str) -> Result<String, String> {
    let value = env::var(name).map_err(|_| format!("required Phase 40 reference-worker ceiling {name} is missing"))?;
    if value.trim().is_empty() { return Err(format!("required Phase 40 reference-worker ceiling {name} is empty")); }
    Ok(value)
}

fn cgroup_memory_limit_bytes() -> Option<u64> {
    for path in ["/sys/fs/cgroup/memory.max", "/sys/fs/cgroup/memory/memory.limit_in_bytes"] {
        let Ok(raw) = fs::read_to_string(path) else { continue; };
        let raw = raw.trim();
        if raw == "max" { return None; }
        let Ok(value) = raw.parse::<u64>() else { continue; };
        // Ignore the very large sentinel used by some cgroup-v1 runtimes for "unlimited".
        if value > 0 && value < (1_u64 << 60) { return Some(value); }
    }
    None
}

#[cfg(test)]
mod phase40_11_tests {
    use super::*;

    fn trusted() -> TrustedPhase40DirectCeilings {
        TrustedPhase40DirectCeilings {
            max_candidate_bindings: 10_000_000,
            max_partition_candidates: 250_000,
            max_exact_partitions: 4096,
            max_grounded_axioms_per_candidate: 65_536,
            max_grounded_rdf_bytes_per_candidate: 16 * 1024 * 1024,
            reasoner_concurrency: 8,
            reasoner_heap_mib_per_lane: 4096,
            reasoner_timeout_seconds: 300,
            max_certificate_bytes: 512 * 1024 * 1024,
            max_proof_support_ids: 1_000_000,
        }
    }

    #[test]
    fn lower_per_job_limits_are_allowed_but_escalation_is_rejected() {
        let mut job = DirectJobLimits {
            max_candidate_bindings: 1_000,
            max_partition_candidates: 100,
            max_grounded_axioms_per_candidate: 100,
            max_grounded_rdf_bytes_per_candidate: 1024,
            max_local_reasoner_lanes: 2,
            reasoner_heap_mib_per_lane: 512,
            reasoner_timeout_seconds: 60,
        };
        assert!(trusted().enforce_job(&job).is_ok());
        job.max_candidate_bindings = 10_000_001;
        assert!(trusted().enforce_job(&job).is_err());
    }

    #[test]
    fn impossible_partition_bundle_is_rejected() {
        let mut limits = trusted();
        limits.max_exact_partitions = 1;
        assert!(limits.validate_cross_fields().is_err());
    }
}
