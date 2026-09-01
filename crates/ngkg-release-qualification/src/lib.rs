//! Fail-closed Phase 40.13.24 Kubernetes release qualification contracts.
//!
//! NGKG remains a Rust production system. Apache Jena and other external
//! standards oracles never enter this runtime. This crate certifies immutable evidence produced by
//! isolated RKE/RKE2, EKS, AKS, and GKE qualification clusters.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

/// Phase 1.0.0-RC1 release-freeze and publication contracts.
pub mod rc1;

/// Phase 1.0.0 General Availability go/no-go and publication contracts.
pub mod ga;

/// Phase 40.13.24 evidence format.
pub const RELEASE_QUALIFICATION_FORMAT_VERSION: u32 = 1;

/// Supported Kubernetes distributions/providers.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum KubernetesProvider {
    /// Rancher Kubernetes Engine.
    Rke,
    /// Rancher Kubernetes Engine 2.
    Rke2,
    /// Amazon Elastic Kubernetes Service.
    Eks,
    /// Azure Kubernetes Service.
    Aks,
    /// Google Kubernetes Engine.
    Gke,
}

/// Closed release gate set. Every gate is mandatory.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ReleaseGate {
    /// Cross-domain OWL 2 DL multi-hop graph-result prerequisite.
    SemanticContextGraph,
    /// Sustained multi-node, multi-core operation.
    MultinodeSoak,
    /// Pod and node loss while work is active.
    ComputeChaos,
    /// Bounded network disruption and recovery.
    NetworkChaos,
    /// Storage/CSI/object corruption and recovery.
    StorageChaos,
    /// Supported in-place upgrade path.
    Upgrade,
    /// Fail-closed rollback to the preceding release.
    Rollback,
    /// Backup, restore, RPO, and RTO evidence.
    BackupRestore,
    /// Helm lint, render, install, and upgrade evidence.
    Helm,
    /// Image digests, signatures, provenance, and runtime hardening.
    ImageProvenance,
    /// SPDX and CycloneDX software bills of materials.
    Sbom,
    /// CVE scan and signed exception policy.
    Cve,
    /// Dependency and container license policy.
    License,
    /// Two isolated builders produce identical release identities.
    ReproducibleBuild,
    /// Provider-neutral behavior and bucket/identity integration.
    ProviderPortability,
}

/// Immutable proof that the semantic release prerequisite completed.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct SemanticContextEvidence {
    /// OWL profile qualification identity.
    pub owl2_dl_qualification_sha256: String,
    /// Snapshot identity.
    pub snapshot_sha256: String,
    /// Authorized named-graph set identity.
    pub authorized_graph_set_sha256: String,
    /// Canonical query identity.
    pub query_sha256: String,
    /// Canonical graph-result identity.
    pub result_graph_sha256: String,
    /// Independently certified scalar result identity.
    pub scalar_oracle_graph_sha256: String,
    /// Exact reasoner/completeness certificate identity.
    pub reasoning_certificate_sha256: String,
    /// Number of separately authorized semantic domains used.
    pub domain_count: u32,
    /// Longest asserted/inferred hop chain exercised.
    pub hop_count: u32,
    /// Number of output triples supported by reasoned consequences.
    pub reasoned_output_triples: u64,
    /// Distinct Kubernetes nodes involved in the execution.
    pub activated_nodes: u32,
    /// Aggregate activated CPU millicores.
    pub activated_cpu_millis: u64,
    /// Aggregate activated memory.
    pub activated_memory_bytes: u64,
    /// Query form, restricted to graph-producing forms.
    pub query_form: String,
    /// Completeness barrier reached.
    pub complete: bool,
    /// Every returned inferred fact has certified answer support.
    pub proof_coverage: String,
}

/// One release scenario assigned to an Indexed Job partition.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ReleaseScenario {
    /// Stable scenario identifier.
    pub scenario_id: String,
    /// Release gate.
    pub gate: ReleaseGate,
    /// Target Kubernetes provider.
    pub provider: KubernetesProvider,
    /// Content hash of the closed driver descriptor.
    pub input_sha256: String,
    /// Expected canonical result/artifact identity.
    pub expected_output_sha256: String,
    /// Dense partition assignment.
    pub partition: u32,
    /// Minimum distinct worker nodes.
    pub minimum_nodes: u32,
    /// Minimum aggregate CPU millicores.
    pub minimum_cpu_millis: u64,
    /// Minimum aggregate RAM.
    pub minimum_memory_bytes: u64,
    /// Minimum retained run duration.
    pub minimum_duration_seconds: u64,
    /// True only for deliberately disruptive scenarios.
    pub disruptive: bool,
    /// Hash of explicit isolated-cluster approval evidence when disruptive.
    pub approval_evidence_sha256: Option<String>,
}

/// Content-bound distributed release plan.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ReleaseQualificationPlan {
    /// Format version.
    pub format_version: u32,
    /// Unique release qualification run.
    pub run_id: String,
    /// Candidate release identity.
    pub release_sha256: String,
    /// Phase 40.13.23 certificate identity.
    pub performance_certificate_sha256: String,
    /// Semantic prerequisite evidence identity.
    pub semantic_evidence_sha256: String,
    /// Closed inventory identity.
    pub inventory_sha256: String,
    /// Dense partition count.
    pub partition_count: u32,
    /// Sorted unique scenario list.
    pub scenarios: Vec<ReleaseScenario>,
}

/// One terminal scenario observation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ReleaseObservation {
    /// Scenario identifier.
    pub scenario_id: String,
    /// Worker-observed provider.
    pub provider: KubernetesProvider,
    /// Gate exercised.
    pub gate: ReleaseGate,
    /// Canonical result/artifact hash.
    pub output_sha256: String,
    /// Content hash of retained raw evidence.
    pub evidence_sha256: String,
    /// Distinct nodes actually used.
    pub activated_nodes: u32,
    /// CPU actually activated.
    pub activated_cpu_millis: u64,
    /// Memory actually activated.
    pub activated_memory_bytes: u64,
    /// Retained execution duration.
    pub duration_seconds: u64,
    /// Number of injected failures.
    pub injected_failures: u32,
    /// Number of recovered failures.
    pub recovered_failures: u32,
    /// Exact semantic response identity after recovery.
    pub post_recovery_result_sha256: String,
    /// True only after durable terminal evidence exists.
    pub complete: bool,
}

/// Atomic output from one dense Kubernetes Indexed Job completion.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ReleasePartitionReport {
    /// Format version.
    pub format_version: u32,
    /// Exact plan identity.
    pub plan_sha256: String,
    /// Dense completion index.
    pub partition: u32,
    /// Unique pod/worker identity.
    pub worker_id: String,
    /// Sorted scenario observations.
    pub observations: Vec<ReleaseObservation>,
    /// Durable partition barrier.
    pub complete: bool,
}

/// Final fail-closed release qualification certificate.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ReleaseQualificationCertificate {
    /// Format version.
    pub format_version: u32,
    /// Plan identity.
    pub plan_sha256: String,
    /// Candidate release identity.
    pub release_sha256: String,
    /// Performance prerequisite identity.
    pub performance_certificate_sha256: String,
    /// Semantic prerequisite identity.
    pub semantic_evidence_sha256: String,
    /// Providers with complete coverage.
    pub qualified_providers: Vec<KubernetesProvider>,
    /// Gates with complete coverage.
    pub qualified_gates: Vec<ReleaseGate>,
    /// Deterministic evidence Merkle-like root.
    pub evidence_root_sha256: String,
    /// Count of failed or missing scenarios; must be zero.
    pub failure_count: u32,
    /// True only after every dense partition and scenario succeeds.
    pub complete: bool,
}

/// Release qualification rejection.
#[derive(Debug, Error, Eq, PartialEq)]
pub enum QualificationError {
    /// A digest or identity is invalid.
    #[error("release qualification identity is invalid")]
    InvalidIdentity,
    /// The semantic prerequisite is incomplete or semantically unequal.
    #[error("cross-domain OWL 2 DL context-graph prerequisite is not certified")]
    SemanticPrerequisite,
    /// Plan invariants are invalid.
    #[error("release qualification plan is incomplete or non-canonical")]
    InvalidPlan,
    /// Reports do not form an exact dense barrier.
    #[error("release report barrier is missing, duplicated, partial, or unequal")]
    InvalidReportBarrier,
}

fn valid_sha(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

/// Validate the graph-producing, distributed semantic prerequisite.
pub fn validate_semantic_context_evidence(
    evidence: &SemanticContextEvidence,
) -> Result<(), QualificationError> {
    let hashes = [
        &evidence.owl2_dl_qualification_sha256,
        &evidence.snapshot_sha256,
        &evidence.authorized_graph_set_sha256,
        &evidence.query_sha256,
        &evidence.result_graph_sha256,
        &evidence.scalar_oracle_graph_sha256,
        &evidence.reasoning_certificate_sha256,
    ];
    if hashes.iter().any(|value| !valid_sha(value))
        || evidence.result_graph_sha256 != evidence.scalar_oracle_graph_sha256
        || evidence.domain_count < 3
        || evidence.hop_count < 2
        || evidence.reasoned_output_triples == 0
        || evidence.activated_nodes < 2
        || evidence.activated_cpu_millis < 2_000
        || evidence.activated_memory_bytes == 0
        || !matches!(evidence.query_form.as_str(), "CONSTRUCT" | "DESCRIBE")
        || !evidence.complete
        || evidence.proof_coverage != "complete"
    {
        return Err(QualificationError::SemanticPrerequisite);
    }
    Ok(())
}

/// Stable, scheduling-independent scenario partition.
#[must_use]
pub fn stable_partition(scenario_id: &str, input_sha256: &str, partition_count: u32) -> u32 {
    if partition_count == 0 {
        return 0;
    }
    let digest = Sha256::digest(format!("{scenario_id}\0{input_sha256}"));
    let mut prefix = [0_u8; 8];
    prefix.copy_from_slice(&digest[..8]);
    (u64::from_be_bytes(prefix) % u64::from(partition_count)) as u32
}

/// Validate the immutable execution plan before any driver is invoked.
pub fn validate_plan(plan: &ReleaseQualificationPlan) -> Result<(), QualificationError> {
    if plan.format_version != RELEASE_QUALIFICATION_FORMAT_VERSION
        || plan.run_id.is_empty()
        || !valid_sha(&plan.release_sha256)
        || !valid_sha(&plan.performance_certificate_sha256)
        || !valid_sha(&plan.semantic_evidence_sha256)
        || !valid_sha(&plan.inventory_sha256)
        || plan.partition_count == 0
        || plan.scenarios.is_empty()
    {
        return Err(QualificationError::InvalidPlan);
    }
    let mut ids = BTreeSet::new();
    for scenario in &plan.scenarios {
        if !ids.insert(&scenario.scenario_id)
            || scenario.scenario_id.is_empty()
            || !valid_sha(&scenario.input_sha256)
            || !valid_sha(&scenario.expected_output_sha256)
            || scenario.partition >= plan.partition_count
            || scenario.partition
                != stable_partition(
                    &scenario.scenario_id,
                    &scenario.input_sha256,
                    plan.partition_count,
                )
            || scenario.minimum_nodes < 3
            || scenario.minimum_cpu_millis < 3_000
            || scenario.minimum_memory_bytes == 0
            || (scenario.disruptive
                && scenario
                    .approval_evidence_sha256
                    .as_deref()
                    .is_none_or(|value| !valid_sha(value)))
            || (!scenario.disruptive && scenario.approval_evidence_sha256.is_some())
        {
            return Err(QualificationError::InvalidPlan);
        }
    }
    Ok(())
}

/// Merge an exact dense report set into a release certificate.
pub fn certify_release(
    plan: &ReleaseQualificationPlan,
    reports: &[ReleasePartitionReport],
) -> Result<ReleaseQualificationCertificate, QualificationError> {
    validate_plan(plan)?;
    if reports.len() != plan.partition_count as usize {
        return Err(QualificationError::InvalidReportBarrier);
    }
    let plan_sha = canonical_plan_sha256(plan);
    let mut partitions = BTreeSet::new();
    let mut worker_ids = BTreeSet::new();
    let expected = plan
        .scenarios
        .iter()
        .map(|scenario| (scenario.scenario_id.as_str(), scenario))
        .collect::<BTreeMap<_, _>>();
    let mut observed = BTreeMap::new();
    for report in reports {
        if report.format_version != RELEASE_QUALIFICATION_FORMAT_VERSION
            || report.plan_sha256 != plan_sha
            || report.partition >= plan.partition_count
            || !report.complete
            || !partitions.insert(report.partition)
            || report.worker_id.is_empty()
            || !worker_ids.insert(&report.worker_id)
        {
            return Err(QualificationError::InvalidReportBarrier);
        }
        for observation in &report.observations {
            let Some(scenario) = expected.get(observation.scenario_id.as_str()) else {
                return Err(QualificationError::InvalidReportBarrier);
            };
            if scenario.partition != report.partition
                || observation.provider != scenario.provider
                || observation.gate != scenario.gate
                || !observation.complete
                || observation.output_sha256 != scenario.expected_output_sha256
                || observation.post_recovery_result_sha256 != scenario.expected_output_sha256
                || !valid_sha(&observation.evidence_sha256)
                || observation.activated_nodes < scenario.minimum_nodes
                || observation.activated_cpu_millis < scenario.minimum_cpu_millis
                || observation.activated_memory_bytes < scenario.minimum_memory_bytes
                || observation.duration_seconds < scenario.minimum_duration_seconds
                || observation.injected_failures != observation.recovered_failures
                || observed
                    .insert(&observation.scenario_id, observation)
                    .is_some()
            {
                return Err(QualificationError::InvalidReportBarrier);
            }
        }
    }
    if observed.len() != expected.len()
        || partitions != (0..plan.partition_count).collect::<BTreeSet<_>>()
    {
        return Err(QualificationError::InvalidReportBarrier);
    }
    let all_providers = [
        KubernetesProvider::Rke,
        KubernetesProvider::Rke2,
        KubernetesProvider::Eks,
        KubernetesProvider::Aks,
        KubernetesProvider::Gke,
    ]
    .into_iter()
    .collect::<BTreeSet<_>>();
    let all_gates = [
        ReleaseGate::SemanticContextGraph,
        ReleaseGate::MultinodeSoak,
        ReleaseGate::ComputeChaos,
        ReleaseGate::NetworkChaos,
        ReleaseGate::StorageChaos,
        ReleaseGate::Upgrade,
        ReleaseGate::Rollback,
        ReleaseGate::BackupRestore,
        ReleaseGate::Helm,
        ReleaseGate::ImageProvenance,
        ReleaseGate::Sbom,
        ReleaseGate::Cve,
        ReleaseGate::License,
        ReleaseGate::ReproducibleBuild,
        ReleaseGate::ProviderPortability,
    ]
    .into_iter()
    .collect::<BTreeSet<_>>();
    let providers = observed
        .values()
        .map(|item| item.provider)
        .collect::<BTreeSet<_>>();
    let gates = observed
        .values()
        .map(|item| item.gate)
        .collect::<BTreeSet<_>>();
    if providers != all_providers || gates != all_gates {
        return Err(QualificationError::InvalidReportBarrier);
    }
    let mut evidence = Sha256::new();
    for (scenario_id, observation) in &observed {
        evidence.update(scenario_id.as_bytes());
        evidence.update([0]);
        evidence.update(observation.evidence_sha256.as_bytes());
        evidence.update([0]);
    }
    Ok(ReleaseQualificationCertificate {
        format_version: RELEASE_QUALIFICATION_FORMAT_VERSION,
        plan_sha256: plan_sha,
        release_sha256: plan.release_sha256.clone(),
        performance_certificate_sha256: plan.performance_certificate_sha256.clone(),
        semantic_evidence_sha256: plan.semantic_evidence_sha256.clone(),
        qualified_providers: providers.into_iter().collect(),
        qualified_gates: gates.into_iter().collect(),
        evidence_root_sha256: format!("{:x}", evidence.finalize()),
        failure_count: 0,
        complete: true,
    })
}

fn canonical_plan_sha256(plan: &ReleaseQualificationPlan) -> String {
    let bytes = serde_json::to_value(plan)
        .and_then(|value| serde_json::to_vec(&value))
        .unwrap_or_default();
    format!("{:x}", Sha256::digest(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn semantic_context_requires_distributed_graph_equivalence() {
        let evidence = SemanticContextEvidence {
            owl2_dl_qualification_sha256: "1".repeat(64),
            snapshot_sha256: "2".repeat(64),
            authorized_graph_set_sha256: "3".repeat(64),
            query_sha256: "4".repeat(64),
            result_graph_sha256: "5".repeat(64),
            scalar_oracle_graph_sha256: "5".repeat(64),
            reasoning_certificate_sha256: "6".repeat(64),
            domain_count: 4,
            hop_count: 3,
            reasoned_output_triples: 1,
            activated_nodes: 3,
            activated_cpu_millis: 12_000,
            activated_memory_bytes: 32 * 1024 * 1024 * 1024,
            query_form: "CONSTRUCT".into(),
            complete: true,
            proof_coverage: "complete".into(),
        };
        assert_eq!(validate_semantic_context_evidence(&evidence), Ok(()));
        let mut unequal = evidence;
        unequal.scalar_oracle_graph_sha256 = "7".repeat(64);
        assert_eq!(
            validate_semantic_context_evidence(&unequal),
            Err(QualificationError::SemanticPrerequisite)
        );
    }
}
