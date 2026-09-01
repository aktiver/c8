//! Deterministic autoscaling policy and qualification evidence.
//!
//! Kubernetes HPA/KEDA create pod demand. The cluster node provisioner then
//! expands the one labelled responsibility pool on unschedulable demand. This
//! crate makes the 80-percent CPU-or-memory trigger, scale-from-zero behavior,
//! checkpoint-safe scale-down, and deterministic evidence executable rather
//! than leaving them as prose in Helm values.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

/// Wire format for Phase 40.13.20 autoscaling evidence.
pub const AUTOSCALING_FORMAT_VERSION: u32 = 1;

/// Required steady-state ceiling. Twenty percent remains failure headroom.
pub const PRODUCTION_SATURATION_TARGET_PERCENT: u8 = 80;

/// A separately schedulable c8 responsibility pool.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum WorkloadClass {
    SourceIngestion,
    SemanticProjection,
    SemanticArtifactBuild,
    IndexBuild,
    OfflineReasoning,
    OnlineReasoning,
    SparqlQuery,
    SparqlFragment,
    ParquetHydration,
    StorageRecovery,
}

/// The unique owner that creates workload pod demand.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum DemandOwner {
    Hpa,
    Keda,
    Operator,
}

/// Reviewed policy for one node pool.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct PoolPolicy {
    pub workload: WorkloadClass,
    pub pool_name: String,
    pub node_label: String,
    pub demand_owner: DemandOwner,
    pub min_nodes: u32,
    pub max_nodes: u32,
    pub scale_up_step: u32,
    pub scale_down_step: u32,
    pub cpu_target_percent: u8,
    pub memory_target_percent: u8,
    pub scale_from_zero: bool,
    pub drain_requires_checkpoint: bool,
}

/// One node's scheduler and live metrics snapshot.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct NodeResourceSample {
    pub node_name: String,
    pub ready: bool,
    pub allocatable_cpu_millis: u64,
    pub requested_cpu_millis: u64,
    pub used_cpu_millis: u64,
    pub allocatable_memory_bytes: u64,
    pub requested_memory_bytes: u64,
    pub used_memory_bytes: u64,
}

/// Exact pool observation used by one decision.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct PoolObservation {
    pub observed_unix_millis: u64,
    pub current_nodes: u32,
    pub pending_pods: u32,
    pub active_work_items: u64,
    pub active_checkpoint_bytes: u64,
    pub active_spill_bytes: u64,
    pub nodes: Vec<NodeResourceSample>,
}

/// Closed autoscaling outcome. Node count remains owned by the installed node
/// provisioner; this decision proves why pod demand must rise, hold, or drain.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ScalingAction {
    ScaleFromZero,
    ScaleOut,
    Hold,
    ScaleIn,
    ScaleInBlocked,
}

/// Deterministic, checksum-bindable decision.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ScalingDecision {
    pub format_version: u32,
    pub workload: WorkloadClass,
    pub pool_name: String,
    pub action: ScalingAction,
    pub current_nodes: u32,
    pub desired_nodes: u32,
    pub ready_nodes: u32,
    pub pending_pods: u32,
    pub cpu_saturation_percent: u8,
    pub memory_saturation_percent: u8,
    pub trigger: String,
    pub observation_sha256: String,
}

/// Cross-scale semantic invariance evidence. Autoscaling cannot certify unless
/// the same immutable inputs produced exactly the same result and artifact IDs.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DeterminismEvidence {
    pub snapshot_id: String,
    pub workload: WorkloadClass,
    pub baseline_result_sha256: String,
    pub scaled_result_sha256: String,
    pub baseline_artifact_root_sha256: String,
    pub scaled_artifact_root_sha256: String,
    pub node_loss_injected: bool,
    pub retry_injected: bool,
}

/// Complete Phase 40.13.20 qualification certificate.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct AutoscalingQualificationCertificate {
    pub format_version: u32,
    pub policy_bundle_sha256: String,
    pub decisions_sha256: String,
    pub determinism_sha256: String,
    pub qualified_workloads: Vec<WorkloadClass>,
    pub cpu_target_percent: u8,
    pub memory_target_percent: u8,
    pub kueue_admission_observed: bool,
    pub node_provisioner_observed: bool,
    pub scale_from_zero_observed: bool,
    pub node_loss_observed: bool,
    pub complete: bool,
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum AutoscalingError {
    #[error("invalid autoscaling contract: {0}")]
    InvalidContract(String),
    #[error("autoscaling qualification is incomplete: {0}")]
    Incomplete(String),
}

/// Evaluate one exact pool observation. Requested and live consumption are
/// both charged so delayed metrics cannot permit scheduler overcommit.
pub fn evaluate_pool(
    policy: &PoolPolicy,
    observation: &PoolObservation,
) -> Result<ScalingDecision, AutoscalingError> {
    validate_policy(policy)?;
    validate_observation(observation)?;
    if observation.current_nodes
        != u32::try_from(observation.nodes.len())
            .map_err(|_| AutoscalingError::InvalidContract("node count exceeds u32".to_owned()))?
    {
        return Err(AutoscalingError::InvalidContract(
            "declared and observed node counts differ".to_owned(),
        ));
    }
    let ready_nodes = u32::try_from(observation.nodes.iter().filter(|node| node.ready).count())
        .map_err(|_| {
            AutoscalingError::InvalidContract("ready node count exceeds u32".to_owned())
        })?;
    let (cpu_percent, memory_percent) = maximum_node_saturation(observation)?;
    let pending = observation.pending_pods > 0;
    let saturated =
        cpu_percent >= policy.cpu_target_percent || memory_percent >= policy.memory_target_percent;
    let (action, desired_nodes, trigger) = if observation.current_nodes == 0 {
        if pending || observation.active_work_items > 0 {
            if !policy.scale_from_zero {
                return Err(AutoscalingError::Incomplete(
                    "work exists in a zero-sized pool without scale-from-zero".to_owned(),
                ));
            }
            (
                ScalingAction::ScaleFromZero,
                policy.min_nodes.max(1).min(policy.max_nodes),
                "pending-work-at-zero".to_owned(),
            )
        } else {
            (ScalingAction::Hold, 0, "idle-zero-pool".to_owned())
        }
    } else if saturated || pending {
        let desired = observation
            .current_nodes
            .saturating_add(policy.scale_up_step)
            .min(policy.max_nodes);
        let reason = match (
            cpu_percent >= policy.cpu_target_percent,
            memory_percent >= policy.memory_target_percent,
            pending,
        ) {
            (true, true, _) => "cpu-and-memory-at-80-percent",
            (true, false, _) => "cpu-at-80-percent",
            (false, true, _) => "memory-at-80-percent",
            (false, false, true) => "unschedulable-pod-demand",
            _ => "capacity-demand",
        };
        if desired == observation.current_nodes {
            (
                ScalingAction::Hold,
                desired,
                format!("maximum-nodes:{reason}"),
            )
        } else {
            (ScalingAction::ScaleOut, desired, reason.to_owned())
        }
    } else if observation.current_nodes > policy.min_nodes && observation.active_work_items == 0 {
        let unsafe_to_drain = observation.active_spill_bytes > 0
            || (policy.drain_requires_checkpoint && observation.active_checkpoint_bytes > 0);
        if unsafe_to_drain {
            (
                ScalingAction::ScaleInBlocked,
                observation.current_nodes,
                "checkpoint-or-spill-active".to_owned(),
            )
        } else {
            (
                ScalingAction::ScaleIn,
                observation
                    .current_nodes
                    .saturating_sub(policy.scale_down_step)
                    .max(policy.min_nodes),
                "idle-and-drain-safe".to_owned(),
            )
        }
    } else {
        (
            ScalingAction::Hold,
            observation.current_nodes,
            "below-target".to_owned(),
        )
    };
    let observation_sha256 = canonical_sha256(observation)?;
    Ok(ScalingDecision {
        format_version: AUTOSCALING_FORMAT_VERSION,
        workload: policy.workload,
        pool_name: policy.pool_name.clone(),
        action,
        current_nodes: observation.current_nodes,
        desired_nodes,
        ready_nodes,
        pending_pods: observation.pending_pods,
        cpu_saturation_percent: cpu_percent,
        memory_saturation_percent: memory_percent,
        trigger,
        observation_sha256,
    })
}

/// Certify only a complete workload matrix with live Kueue, node provisioning,
/// node loss, scale-from-zero and checksum-identical semantic results.
pub fn certify_autoscaling(
    policies: &[PoolPolicy],
    decisions: &[ScalingDecision],
    determinism: &[DeterminismEvidence],
    kueue_admission_observed: bool,
    node_provisioner_observed: bool,
    scale_from_zero_observed: bool,
    node_loss_observed: bool,
) -> Result<AutoscalingQualificationCertificate, AutoscalingError> {
    if policies.is_empty() || decisions.is_empty() || determinism.is_empty() {
        return Err(AutoscalingError::Incomplete(
            "policy, decision, and determinism evidence are required".to_owned(),
        ));
    }
    for policy in policies {
        validate_policy(policy)?;
    }
    let policy_workloads = policies
        .iter()
        .map(|value| value.workload)
        .collect::<BTreeSet<_>>();
    let decision_workloads = decisions
        .iter()
        .map(|value| value.workload)
        .collect::<BTreeSet<_>>();
    let deterministic_workloads = determinism
        .iter()
        .map(|value| value.workload)
        .collect::<BTreeSet<_>>();
    if policy_workloads != decision_workloads || policy_workloads != deterministic_workloads {
        return Err(AutoscalingError::Incomplete(
            "qualification does not cover the exact policy workload set".to_owned(),
        ));
    }
    if decisions.iter().any(|decision| {
        decision.format_version != AUTOSCALING_FORMAT_VERSION
            || decision.cpu_saturation_percent > 100
            || decision.memory_saturation_percent > 100
            || !valid_sha256(&decision.observation_sha256)
    }) {
        return Err(AutoscalingError::InvalidContract(
            "scaling decision evidence is invalid".to_owned(),
        ));
    }
    if determinism.iter().any(|evidence| {
        evidence.snapshot_id.is_empty()
            || !valid_sha256(&evidence.baseline_result_sha256)
            || evidence.baseline_result_sha256 != evidence.scaled_result_sha256
            || !valid_sha256(&evidence.baseline_artifact_root_sha256)
            || evidence.baseline_artifact_root_sha256 != evidence.scaled_artifact_root_sha256
            || !evidence.node_loss_injected
            || !evidence.retry_injected
    }) {
        return Err(AutoscalingError::Incomplete(
            "scaled execution is not checksum-identical after loss and retry".to_owned(),
        ));
    }
    if !kueue_admission_observed
        || !node_provisioner_observed
        || !scale_from_zero_observed
        || !node_loss_observed
    {
        return Err(AutoscalingError::Incomplete(
            "live Kubernetes autoscaling observations are incomplete".to_owned(),
        ));
    }
    Ok(AutoscalingQualificationCertificate {
        format_version: AUTOSCALING_FORMAT_VERSION,
        policy_bundle_sha256: canonical_sha256(policies)?,
        decisions_sha256: canonical_sha256(decisions)?,
        determinism_sha256: canonical_sha256(determinism)?,
        qualified_workloads: policy_workloads.into_iter().collect(),
        cpu_target_percent: PRODUCTION_SATURATION_TARGET_PERCENT,
        memory_target_percent: PRODUCTION_SATURATION_TARGET_PERCENT,
        kueue_admission_observed,
        node_provisioner_observed,
        scale_from_zero_observed,
        node_loss_observed,
        complete: true,
    })
}

fn maximum_node_saturation(observation: &PoolObservation) -> Result<(u8, u8), AutoscalingError> {
    if observation.nodes.is_empty() {
        return Ok((0, 0));
    }
    let mut maximum_cpu = 0_u8;
    let mut maximum_memory = 0_u8;
    let mut ready = 0_u32;
    for node in observation.nodes.iter().filter(|node| node.ready) {
        ready = ready.saturating_add(1);
        maximum_cpu = maximum_cpu.max(percent(
            node.requested_cpu_millis.max(node.used_cpu_millis),
            node.allocatable_cpu_millis,
        )?);
        maximum_memory = maximum_memory.max(percent(
            node.requested_memory_bytes.max(node.used_memory_bytes),
            node.allocatable_memory_bytes,
        )?);
    }
    if ready == 0 {
        return Err(AutoscalingError::InvalidContract(
            "pool has nodes but no ready capacity".to_owned(),
        ));
    }
    Ok((maximum_cpu, maximum_memory))
}

fn percent(used: u64, capacity: u64) -> Result<u8, AutoscalingError> {
    if capacity == 0 {
        return Err(AutoscalingError::InvalidContract(
            "ready capacity is zero".to_owned(),
        ));
    }
    let value = u128::from(used)
        .saturating_mul(100)
        .div_ceil(u128::from(capacity))
        .min(100);
    u8::try_from(value)
        .map_err(|_| AutoscalingError::InvalidContract("percent exceeds u8".to_owned()))
}

fn validate_policy(policy: &PoolPolicy) -> Result<(), AutoscalingError> {
    if policy.pool_name.is_empty()
        || policy.node_label != format!("ngkg.io/workload={}", workload_label(policy.workload))
        || policy.max_nodes == 0
        || policy.min_nodes > policy.max_nodes
        || policy.scale_up_step == 0
        || policy.scale_down_step == 0
        || policy.cpu_target_percent != PRODUCTION_SATURATION_TARGET_PERCENT
        || policy.memory_target_percent != PRODUCTION_SATURATION_TARGET_PERCENT
        || (policy.min_nodes == 0 && !policy.scale_from_zero)
    {
        return Err(AutoscalingError::InvalidContract(
            "pool policy violates the production 80-percent ownership envelope".to_owned(),
        ));
    }
    Ok(())
}

fn validate_observation(observation: &PoolObservation) -> Result<(), AutoscalingError> {
    let mut names = BTreeSet::new();
    for node in &observation.nodes {
        if node.node_name.is_empty()
            || !names.insert(node.node_name.as_str())
            || node.allocatable_cpu_millis == 0
            || node.allocatable_memory_bytes == 0
            || node.requested_cpu_millis > node.allocatable_cpu_millis
            || node.requested_memory_bytes > node.allocatable_memory_bytes
        {
            return Err(AutoscalingError::InvalidContract(
                "node observation is invalid or over-requested".to_owned(),
            ));
        }
    }
    Ok(())
}

fn workload_label(workload: WorkloadClass) -> &'static str {
    match workload {
        WorkloadClass::SourceIngestion => "source-ingestion",
        WorkloadClass::SemanticProjection => "semantic-projection",
        WorkloadClass::SemanticArtifactBuild => "semantic-artifact-build",
        WorkloadClass::IndexBuild => "index-build",
        WorkloadClass::OfflineReasoning => "reasoning",
        WorkloadClass::OnlineReasoning => "online-reasoning",
        WorkloadClass::SparqlQuery => "sparql-query-processing",
        WorkloadClass::SparqlFragment => "sparql-fragment-processing",
        WorkloadClass::ParquetHydration => "parquet-hydration",
        WorkloadClass::StorageRecovery => "storage-recovery",
    }
}

fn canonical_sha256<T: Serialize + ?Sized>(value: &T) -> Result<String, AutoscalingError> {
    let bytes = serde_json::to_vec(value).map_err(|error| {
        AutoscalingError::InvalidContract(format!("evidence cannot be encoded: {error}"))
    })?;
    Ok(hex::encode(Sha256::digest(bytes)))
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy() -> PoolPolicy {
        PoolPolicy {
            workload: WorkloadClass::StorageRecovery,
            pool_name: "storage-recovery".to_owned(),
            node_label: "ngkg.io/workload=storage-recovery".to_owned(),
            demand_owner: DemandOwner::Operator,
            min_nodes: 0,
            max_nodes: 32,
            scale_up_step: 1,
            scale_down_step: 1,
            cpu_target_percent: 80,
            memory_target_percent: 80,
            scale_from_zero: true,
            drain_requires_checkpoint: true,
        }
    }

    fn observation(cpu: u64, memory: u64) -> PoolObservation {
        PoolObservation {
            observed_unix_millis: 1,
            current_nodes: 1,
            pending_pods: 0,
            active_work_items: 1,
            active_checkpoint_bytes: 0,
            active_spill_bytes: 0,
            nodes: vec![NodeResourceSample {
                node_name: "node-a".to_owned(),
                ready: true,
                allocatable_cpu_millis: 10_000,
                requested_cpu_millis: cpu,
                used_cpu_millis: cpu,
                allocatable_memory_bytes: 10_000,
                requested_memory_bytes: memory,
                used_memory_bytes: memory,
            }],
        }
    }

    #[test]
    fn seventy_nine_percent_holds() -> Result<(), AutoscalingError> {
        assert_eq!(
            evaluate_pool(&policy(), &observation(7_900, 7_900))?.action,
            ScalingAction::Hold
        );
        Ok(())
    }

    #[test]
    fn exactly_eighty_percent_scales_out() -> Result<(), AutoscalingError> {
        let decision = evaluate_pool(&policy(), &observation(8_000, 1_000))?;
        assert_eq!(decision.action, ScalingAction::ScaleOut);
        assert_eq!(decision.desired_nodes, 2);
        Ok(())
    }

    #[test]
    fn memory_alone_triggers_scale_out() -> Result<(), AutoscalingError> {
        assert_eq!(
            evaluate_pool(&policy(), &observation(1_000, 8_000))?.trigger,
            "memory-at-80-percent"
        );
        Ok(())
    }

    #[test]
    fn one_saturated_node_cannot_be_hidden_by_an_idle_node() -> Result<(), AutoscalingError> {
        let mut value = observation(8_000, 1_000);
        value.nodes.push(NodeResourceSample {
            node_name: "node-b".to_owned(),
            ready: true,
            allocatable_cpu_millis: 10_000,
            requested_cpu_millis: 0,
            used_cpu_millis: 0,
            allocatable_memory_bytes: 10_000,
            requested_memory_bytes: 0,
            used_memory_bytes: 0,
        });
        value.current_nodes = 2;
        assert_eq!(
            evaluate_pool(&policy(), &value)?.action,
            ScalingAction::ScaleOut
        );
        Ok(())
    }

    #[test]
    fn pending_work_scales_from_zero() -> Result<(), AutoscalingError> {
        let mut value = observation(1_000, 1_000);
        value.current_nodes = 0;
        value.nodes.clear();
        value.pending_pods = 1;
        assert_eq!(
            evaluate_pool(&policy(), &value)?.action,
            ScalingAction::ScaleFromZero
        );
        Ok(())
    }

    #[test]
    fn active_checkpoint_blocks_scale_in() -> Result<(), AutoscalingError> {
        let mut value = observation(1_000, 1_000);
        value.active_work_items = 0;
        value.active_checkpoint_bytes = 42;
        let mut reviewed = policy();
        reviewed.min_nodes = 0;
        assert_eq!(
            evaluate_pool(&reviewed, &value)?.action,
            ScalingAction::ScaleInBlocked
        );
        Ok(())
    }
}
