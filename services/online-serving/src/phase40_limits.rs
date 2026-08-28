//! Trusted Phase 40.12 online admission ceilings.
//!
//! Helm is the authority for these values. Every online-serving role validates the same
//! immutable environment bundle so query coordinators and distributed fragment workers can be
//! proven to have started under one resource policy. Only the query role consumes the BGP count
//! and triple ceilings semantically; the fragment role records the same bundle identity for
//! distributed-runtime coherence.

use std::{env, thread};

use anyhow::{Context, Result, bail};
use ngkg_owl_direct::DirectBgpClassificationLimits;
use serde::Serialize;
use sha2::{Digest, Sha256};

pub const ENV_MAX_BGPS: &str = "NGKG_PHASE40_DIRECT_ADMISSION_MAX_BGPS";
pub const ENV_MAX_TRIPLES_PER_BGP: &str = "NGKG_PHASE40_DIRECT_ADMISSION_MAX_TRIPLES_PER_BGP";
pub const ENV_MAX_CLASSIFICATION_CPU_LANES: &str =
    "NGKG_PHASE40_DIRECT_ADMISSION_MAX_CLASSIFICATION_CPU_LANES";

const HARD_MAX_BGPS: usize = 4096;
const HARD_MAX_TRIPLES_PER_BGP: usize = 65_536;
const HARD_MAX_CLASSIFICATION_CPU_LANES: usize = 32;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TrustedPhase40AdmissionCeilings {
    pub max_bgps: usize,
    pub max_triples_per_bgp: usize,
    pub max_classification_cpu_lanes: usize,
}

impl TrustedPhase40AdmissionCeilings {
    pub fn from_env() -> Result<Self> {
        let value = Self {
            max_bgps: positive_usize(ENV_MAX_BGPS)?,
            max_triples_per_bgp: positive_usize(ENV_MAX_TRIPLES_PER_BGP)?,
            max_classification_cpu_lanes: positive_usize(ENV_MAX_CLASSIFICATION_CPU_LANES)?,
        };
        value.validate_hard_bounds()?;
        Ok(value)
    }

    fn validate_hard_bounds(self) -> Result<()> {
        if self.max_bgps > HARD_MAX_BGPS {
            bail!("{ENV_MAX_BGPS} exceeds reviewed Phase 40.7 hard cap {HARD_MAX_BGPS}");
        }
        if self.max_triples_per_bgp > HARD_MAX_TRIPLES_PER_BGP {
            bail!(
                "{ENV_MAX_TRIPLES_PER_BGP} exceeds reviewed Phase 40.7 hard cap {HARD_MAX_TRIPLES_PER_BGP}"
            );
        }
        if self.max_classification_cpu_lanes > HARD_MAX_CLASSIFICATION_CPU_LANES {
            bail!(
                "{ENV_MAX_CLASSIFICATION_CPU_LANES} exceeds reviewed Phase 40.7 hard cap {HARD_MAX_CLASSIFICATION_CPU_LANES}"
            );
        }
        Ok(())
    }

    /// Return the exact classifier limits for this process. The Helm lane value is an upper
    /// bound, not a command to oversubscribe the pod. We additionally cap it by visible CPU and
    /// the Rust compute-thread budget while leaving the semantic BGP/triple ceilings unchanged.
    pub fn classifier_limits(
        self,
        rust_compute_threads: usize,
    ) -> Result<DirectBgpClassificationLimits> {
        if rust_compute_threads == 0 {
            bail!("NGKG_RUST_COMPUTE_THREADS must be positive");
        }
        let visible = thread::available_parallelism().map_or(1, |count| count.get());
        let effective_lanes = self
            .max_classification_cpu_lanes
            .min(visible)
            .min(rust_compute_threads)
            .max(1);
        Ok(DirectBgpClassificationLimits {
            max_bgps: self.max_bgps,
            max_triples_per_bgp: self.max_triples_per_bgp,
            max_cpu_lanes: effective_lanes,
        })
    }

    pub fn bundle_sha256(self) -> Result<String> {
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct Envelope {
            contract: &'static str,
            max_bgps: usize,
            max_triples_per_bgp: usize,
            max_classification_cpu_lanes: usize,
        }
        let bytes = serde_json::to_vec(&Envelope {
            contract: "ngkg-phase40-online-admission-ceilings-v1",
            max_bgps: self.max_bgps,
            max_triples_per_bgp: self.max_triples_per_bgp,
            max_classification_cpu_lanes: self.max_classification_cpu_lanes,
        })?;
        Ok(hex::encode(Sha256::digest(bytes)))
    }
}

fn positive_usize(name: &str) -> Result<usize> {
    let raw = env::var(name).with_context(|| format!("{name} is required"))?;
    let value = raw
        .parse::<usize>()
        .with_context(|| format!("{name} must be a positive integer"))?;
    if value == 0 {
        bail!("{name} must be positive");
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifier_limits_never_oversubscribe_compute_budget() -> Result<()> {
        let trusted = TrustedPhase40AdmissionCeilings {
            max_bgps: 4096,
            max_triples_per_bgp: 65_536,
            max_classification_cpu_lanes: 32,
        };
        let limits = trusted.classifier_limits(4)?;
        assert_eq!(limits.max_bgps, 4096);
        assert_eq!(limits.max_triples_per_bgp, 65_536);
        assert!(limits.max_cpu_lanes <= 4);
        assert!(limits.max_cpu_lanes >= 1);
        Ok(())
    }

    #[test]
    fn hard_bounds_reject_unreviewed_values() {
        let trusted = TrustedPhase40AdmissionCeilings {
            max_bgps: HARD_MAX_BGPS + 1,
            max_triples_per_bgp: HARD_MAX_TRIPLES_PER_BGP,
            max_classification_cpu_lanes: HARD_MAX_CLASSIFICATION_CPU_LANES,
        };
        assert!(trusted.validate_hard_bounds().is_err());
    }
}
