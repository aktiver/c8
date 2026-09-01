//! Deterministic Phase 40.13.23 performance and capacity certification.
//!
//! This crate is part of the Rust qualification plane. Apache Jena may provide
//! an external same-hardware comparison sample, but it is never linked into an
//! NGKG service, operator, storage component, or query runtime.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

/// Performance evidence wire version.
pub const PERFORMANCE_FORMAT_VERSION: u32 = 1;

/// Closed workload families required by the capacity qualification.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum PerformanceFamily {
    /// Whole-TriG cloud ingestion and syntax-aware decode.
    TrigIngestion,
    /// GUID, Parquet, adjacency, index, and snapshot compilation.
    SemanticCompilation,
    /// Finite closure construction plus exact OWL 2 DL verification.
    OfflineReasoning,
    /// Distributed property-path frontier traversal.
    PropertyPath,
    /// Scalar and distributed SPARQL query forms.
    SparqlQuery,
    /// Sustained multi-user SPARQL load.
    ConcurrentSparql,
    /// Checkpoint, node-loss, retry, relocation, backup, and restore overhead.
    Recovery,
}

/// Engine process that emitted one observation.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum BenchmarkEngine {
    /// The production implementation: NGKG Rust release images only.
    NgkgRust,
    /// An isolated competitor/reference process, never a runtime dependency.
    ExternalApacheJena,
}

/// Controlled cache state.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum CacheState {
    Cold,
    Warm,
    Hot,
}

/// Warm-up observations are retained but cannot enter measured statistics.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum TrialPhase {
    Warmup,
    Measured,
}

/// Immutable policy and resource point for one benchmark scenario.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct PerformanceScenario {
    /// Stable scenario identity.
    pub scenario_id: String,
    /// Workload family.
    pub family: PerformanceFamily,
    /// Immutable input descriptor digest.
    pub input_sha256: String,
    /// Canonical semantic result required at every trial and scale point.
    pub expected_result_sha256: String,
    /// Scenarios that differ only by resources share this capacity group.
    pub capacity_group: String,
    /// Dense zero-based resource scale point within the capacity group.
    pub scale_ordinal: u32,
    /// Cache state.
    pub cache_state: CacheState,
    /// Client-side concurrency created by the driver.
    pub concurrency: u32,
    /// Maximum admitted worker nodes for this scale point.
    pub requested_nodes: u32,
    /// Requested aggregate CPU cores in millicores.
    pub requested_cpu_millis: u64,
    /// Requested aggregate RAM.
    pub requested_memory_bytes: u64,
    /// Retained but statistically excluded warm-up trials per engine.
    pub warmup_trials: u32,
    /// Required measured trials per engine. No failed trial may be dropped.
    pub measured_trials: u32,
    /// True when an isolated same-hardware Jena comparison is applicable.
    pub require_external_jena: bool,
    /// Maximum allowed p95 latency; zero disables this threshold.
    pub maximum_p95_nanoseconds: u64,
    /// Minimum median throughput; zero disables this threshold.
    pub minimum_throughput_per_second: u64,
    /// Minimum Jena/NGKG median latency ratio in thousandths; zero disables it.
    pub minimum_speedup_milli_x: u64,
    /// Maximum normalized cost in micro-US dollars per million operations; zero disables it.
    pub maximum_cost_micro_usd_per_million: u64,
    /// Stable dense Indexed Job partition.
    pub partition: u32,
}

/// Content-bound benchmark execution plan.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct PerformancePlan {
    /// Wire version.
    pub format_version: u32,
    /// Unique run identity.
    pub run_id: String,
    /// Digest of the workload/dataset/policy inventory.
    pub benchmark_inventory_sha256: String,
    /// Release NGKG image digest.
    pub ngkg_image_sha256: String,
    /// Optional external Jena image digest.
    pub external_jena_image_sha256: Option<String>,
    /// Exact hardware fingerprint.
    pub hardware_sha256: String,
    /// Exact pricing fingerprint.
    pub pricing_sha256: String,
    /// Phase 40.13.20 live autoscaling evidence.
    pub autoscaling_evidence_sha256: String,
    /// Dense partition count.
    pub partition_count: u32,
    /// Strictly sorted unique scenarios.
    pub scenarios: Vec<PerformanceScenario>,
}

/// One retained warm-up or measured engine observation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct PerformanceObservation {
    /// Scenario identity.
    pub scenario_id: String,
    /// Engine identity.
    pub engine: BenchmarkEngine,
    /// Warm-up or measured phase.
    pub trial_phase: TrialPhase,
    /// Dense zero-based trial index within phase and engine.
    pub trial: u32,
    /// End-to-end monotonic duration.
    pub duration_nanoseconds: u64,
    /// Completed operations represented by the duration.
    pub operations: u64,
    /// Family-specific throughput units (bytes, triples, axioms, edges, or queries).
    pub work_items: u64,
    /// Input bytes consumed.
    pub input_bytes: u64,
    /// Output rows, triples, edges, or artifacts.
    pub output_items: u64,
    /// Process/container CPU time.
    pub cpu_time_nanoseconds: u64,
    /// Peak resident bytes.
    pub peak_rss_bytes: u64,
    /// Storage/network bytes read.
    pub bytes_read: u64,
    /// Storage/network bytes written.
    pub bytes_written: u64,
    /// Distinct nodes activated by this execution.
    pub nodes_activated: u32,
    /// Activated requested CPU in millicores.
    pub cpu_millis_activated: u64,
    /// Activated requested RAM.
    pub ram_bytes_activated: u64,
    /// Canonical semantic result digest.
    pub result_sha256: String,
    /// Optional produced artifact-root digest.
    pub artifact_root_sha256: Option<String>,
    /// Exact Phase 40.13.20 scaling evidence digest.
    pub autoscaling_evidence_sha256: String,
    /// Trial cost in micro-US dollars.
    pub cost_micro_usd: u64,
    /// True only for a non-partial terminal result.
    pub complete: bool,
}

/// Atomic output of one Indexed Job completion.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct PerformancePartitionReport {
    /// Wire version.
    pub format_version: u32,
    /// Exact plan digest.
    pub plan_sha256: String,
    /// Dense completion index.
    pub partition: u32,
    /// Unique worker/pod identity.
    pub worker_id: String,
    /// Observations sorted by scenario, engine, phase, and trial.
    pub observations: Vec<PerformanceObservation>,
    /// True only after durable completion.
    pub complete: bool,
}

/// Deterministic summary for one scenario and engine.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct EngineStatistics {
    /// Engine.
    pub engine: BenchmarkEngine,
    /// Count of measured trials.
    pub measured_trials: u32,
    /// Nearest-rank p50 duration.
    pub p50_nanoseconds: u64,
    /// Nearest-rank p95 duration.
    pub p95_nanoseconds: u64,
    /// Nearest-rank p99 duration.
    pub p99_nanoseconds: u64,
    /// Median integer operations per second.
    pub median_throughput_per_second: u64,
    /// Median normalized cost per million operations.
    pub median_cost_micro_usd_per_million: u64,
    /// Maximum activated nodes.
    pub maximum_nodes_activated: u32,
    /// Maximum peak RSS.
    pub maximum_peak_rss_bytes: u64,
}

/// Scenario statistics included in the certificate.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ScenarioStatistics {
    /// Scenario identity.
    pub scenario_id: String,
    /// NGKG statistics.
    pub ngkg: EngineStatistics,
    /// Optional external Jena statistics.
    pub external_jena: Option<EngineStatistics>,
    /// Jena/NGKG median latency ratio in thousandths.
    pub speedup_milli_x: Option<u64>,
}

/// Release-bound performance and capacity certificate.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct PerformanceQualificationCertificate {
    /// Wire version.
    pub format_version: u32,
    /// Plan digest.
    pub plan_sha256: String,
    /// Ordered report-set digest.
    pub report_set_sha256: String,
    /// Scenario summaries.
    pub scenarios: Vec<ScenarioStatistics>,
    /// Qualified families.
    pub qualified_families: Vec<PerformanceFamily>,
    /// All results and artifact identities remained exact.
    pub deterministic_results: bool,
    /// The 80-percent autoscaling evidence remained bound.
    pub autoscaling_evidence_bound: bool,
    /// No failed or incomplete trial was omitted.
    pub no_excluded_trials: bool,
    /// Zero for a valid certificate.
    pub failed_threshold_count: u64,
    /// True only after the dense all-scenario barrier.
    pub complete: bool,
}

/// Fail-closed performance qualification errors.
#[derive(Debug, Error, Eq, PartialEq)]
pub enum PerformanceError {
    /// Malformed identity, policy, resource, or sample.
    #[error("invalid performance qualification contract: {0}")]
    InvalidContract(String),
    /// Missing, duplicate, partial, unequal, or below-threshold evidence.
    #[error("performance qualification is incomplete: {0}")]
    Incomplete(String),
    /// Canonical encoding failed.
    #[error("performance qualification serialization failed")]
    Serialization,
}

/// Assign a stable topology-independent completion index.
pub fn stable_partition(
    scenario_id: &str,
    input_sha256: &str,
    partition_count: u32,
) -> Result<u32, PerformanceError> {
    if scenario_id.is_empty() || !is_sha256(input_sha256) || partition_count == 0 {
        return Err(PerformanceError::InvalidContract(
            "stable partition input is invalid".to_owned(),
        ));
    }
    let bytes = Sha256::digest(format!("{scenario_id}\0{input_sha256}").as_bytes());
    let prefix: [u8; 8] = bytes[..8].try_into().map_err(|_| {
        PerformanceError::InvalidContract("partition digest is truncated".to_owned())
    })?;
    u32::try_from(u64::from_be_bytes(prefix) % u64::from(partition_count))
        .map_err(|_| PerformanceError::InvalidContract("partition index exceeds u32".to_owned()))
}

/// Validate and certify a complete dense report set.
pub fn certify_performance(
    plan: &PerformancePlan,
    reports: &[PerformancePartitionReport],
) -> Result<PerformanceQualificationCertificate, PerformanceError> {
    validate_plan(plan)?;
    let plan_sha256 = canonical_sha256(plan)?;
    if reports.len()
        != usize::try_from(plan.partition_count).map_err(|_| {
            PerformanceError::InvalidContract("partition count exceeds usize".to_owned())
        })?
    {
        return Err(PerformanceError::Incomplete(
            "one report per dense partition is required".to_owned(),
        ));
    }
    let expected = plan
        .scenarios
        .iter()
        .map(|scenario| (scenario.scenario_id.as_str(), scenario))
        .collect::<BTreeMap<_, _>>();
    let mut reports = reports.to_vec();
    reports.sort_by_key(|report| report.partition);
    let mut observations = BTreeMap::<&str, Vec<&PerformanceObservation>>::new();
    let mut workers = BTreeSet::new();
    for (index, report) in reports.iter().enumerate() {
        if report.format_version != PERFORMANCE_FORMAT_VERSION
            || report.plan_sha256 != plan_sha256
            || usize::try_from(report.partition).map_err(|_| {
                PerformanceError::InvalidContract("partition index exceeds usize".to_owned())
            })? != index
            || report.worker_id.is_empty()
            || !workers.insert(report.worker_id.as_str())
            || !report.complete
        {
            return Err(PerformanceError::Incomplete(
                "partition identity, worker identity, or completion failed".to_owned(),
            ));
        }
        for observation in &report.observations {
            let scenario = expected
                .get(observation.scenario_id.as_str())
                .ok_or_else(|| {
                    PerformanceError::Incomplete("observation is outside the plan".to_owned())
                })?;
            if scenario.partition != report.partition {
                return Err(PerformanceError::Incomplete(
                    "observation was delivered by the wrong partition".to_owned(),
                ));
            }
            validate_observation(plan, scenario, observation)?;
            observations
                .entry(observation.scenario_id.as_str())
                .or_default()
                .push(observation);
        }
    }
    let mut summaries = Vec::with_capacity(plan.scenarios.len());
    let mut families = BTreeSet::new();
    let mut capacity_artifacts = BTreeMap::<&str, BTreeSet<&str>>::new();
    for scenario in &plan.scenarios {
        let rows = observations
            .get(scenario.scenario_id.as_str())
            .ok_or_else(|| {
                PerformanceError::Incomplete("scenario has no observations".to_owned())
            })?;
        let measured_ngkg = rows.iter().filter(|item| {
            item.engine == BenchmarkEngine::NgkgRust && item.trial_phase == TrialPhase::Measured
        });
        let mut artifact_values = BTreeSet::new();
        let mut artifact_missing = false;
        for observation in measured_ngkg {
            match observation.artifact_root_sha256.as_deref() {
                Some(value) if is_sha256(value) => {
                    artifact_values.insert(value);
                }
                Some(_) => {
                    return Err(PerformanceError::Incomplete(
                        "artifact-root digest is invalid".to_owned(),
                    ));
                }
                None => artifact_missing = true,
            }
        }
        if artifact_values.len() > 1 || (artifact_missing && !artifact_values.is_empty()) {
            return Err(PerformanceError::Incomplete(
                "artifact identity changed or disappeared between measured trials".to_owned(),
            ));
        }
        if let Some(value) = artifact_values.first() {
            capacity_artifacts
                .entry(scenario.capacity_group.as_str())
                .or_default()
                .insert(*value);
        }
        let ngkg = statistics(scenario, rows, BenchmarkEngine::NgkgRust)?;
        let external_jena = if scenario.require_external_jena {
            Some(statistics(
                scenario,
                rows,
                BenchmarkEngine::ExternalApacheJena,
            )?)
        } else {
            None
        };
        if (ngkg.p95_nanoseconds > scenario.maximum_p95_nanoseconds
            && scenario.maximum_p95_nanoseconds > 0)
            || ngkg.median_throughput_per_second < scenario.minimum_throughput_per_second
            || (scenario.maximum_cost_micro_usd_per_million > 0
                && ngkg.median_cost_micro_usd_per_million
                    > scenario.maximum_cost_micro_usd_per_million)
        {
            return Err(PerformanceError::Incomplete(
                "NGKG latency, throughput, or cost threshold failed".to_owned(),
            ));
        }
        let speedup = external_jena.as_ref().map(|jena| {
            jena.p50_nanoseconds
                .saturating_mul(1_000)
                .checked_div(ngkg.p50_nanoseconds)
                .unwrap_or(0)
        });
        if scenario.minimum_speedup_milli_x > 0
            && speedup.is_none_or(|value| value < scenario.minimum_speedup_milli_x)
        {
            return Err(PerformanceError::Incomplete(
                "external Jena comparison threshold failed".to_owned(),
            ));
        }
        summaries.push(ScenarioStatistics {
            scenario_id: scenario.scenario_id.clone(),
            ngkg,
            external_jena,
            speedup_milli_x: speedup,
        });
        families.insert(scenario.family);
    }
    if capacity_artifacts.values().any(|values| values.len() > 1) {
        return Err(PerformanceError::Incomplete(
            "artifact identity changed across capacity scale points".to_owned(),
        ));
    }
    let summary_by_id = summaries
        .iter()
        .map(|summary| (summary.scenario_id.as_str(), summary))
        .collect::<BTreeMap<_, _>>();
    let mut capacity_groups = BTreeMap::<&str, Vec<&PerformanceScenario>>::new();
    for scenario in &plan.scenarios {
        capacity_groups
            .entry(scenario.capacity_group.as_str())
            .or_default()
            .push(scenario);
    }
    for scenarios in capacity_groups.values_mut() {
        scenarios.sort_by_key(|scenario| scenario.scale_ordinal);
        if scenarios.len() > 1 {
            let first = summary_by_id[scenarios[0].scenario_id.as_str()];
            let last = summary_by_id[scenarios[scenarios.len() - 1].scenario_id.as_str()];
            if last.ngkg.median_throughput_per_second < first.ngkg.median_throughput_per_second {
                return Err(PerformanceError::Incomplete(
                    "throughput regressed at the largest capacity point".to_owned(),
                ));
            }
        }
    }
    Ok(PerformanceQualificationCertificate {
        format_version: PERFORMANCE_FORMAT_VERSION,
        plan_sha256,
        report_set_sha256: canonical_sha256(&reports)?,
        scenarios: summaries,
        qualified_families: families.into_iter().collect(),
        deterministic_results: true,
        autoscaling_evidence_bound: true,
        no_excluded_trials: true,
        failed_threshold_count: 0,
        complete: true,
    })
}

fn validate_plan(plan: &PerformancePlan) -> Result<(), PerformanceError> {
    if plan.format_version != PERFORMANCE_FORMAT_VERSION
        || plan.run_id.is_empty()
        || plan.partition_count == 0
        || plan.partition_count > 65_536
        || plan.scenarios.is_empty()
        || !is_sha256(&plan.benchmark_inventory_sha256)
        || !is_sha256(&plan.ngkg_image_sha256)
        || plan
            .external_jena_image_sha256
            .as_ref()
            .is_some_and(|value| !is_sha256(value))
        || !is_sha256(&plan.hardware_sha256)
        || !is_sha256(&plan.pricing_sha256)
        || !is_sha256(&plan.autoscaling_evidence_sha256)
    {
        return Err(PerformanceError::InvalidContract(
            "plan header is invalid".to_owned(),
        ));
    }
    let mut previous: Option<&str> = None;
    let mut capacity_groups = BTreeMap::<&str, Vec<&PerformanceScenario>>::new();
    for scenario in &plan.scenarios {
        if previous.is_some_and(|value| value >= scenario.scenario_id.as_str())
            || scenario.scenario_id.is_empty()
            || !is_sha256(&scenario.input_sha256)
            || !is_sha256(&scenario.expected_result_sha256)
            || scenario.capacity_group.is_empty()
            || scenario.concurrency == 0
            || scenario.requested_nodes == 0
            || scenario.requested_cpu_millis == 0
            || scenario.requested_memory_bytes == 0
            || scenario.measured_trials < 3
            || scenario.partition
                != stable_partition(
                    &scenario.scenario_id,
                    &scenario.input_sha256,
                    plan.partition_count,
                )?
            || (scenario.require_external_jena && plan.external_jena_image_sha256.is_none())
            || (!scenario.require_external_jena && scenario.minimum_speedup_milli_x > 0)
        {
            return Err(PerformanceError::InvalidContract(
                "scenario identity, resources, trials, or threshold is invalid".to_owned(),
            ));
        }
        capacity_groups
            .entry(scenario.capacity_group.as_str())
            .or_default()
            .push(scenario);
        previous = Some(&scenario.scenario_id);
    }
    for scenarios in capacity_groups.values_mut() {
        scenarios.sort_by_key(|scenario| scenario.scale_ordinal);
        let first = scenarios[0];
        for (ordinal, scenario) in scenarios.iter().enumerate() {
            if usize::try_from(scenario.scale_ordinal).map_err(|_| {
                PerformanceError::InvalidContract("scale ordinal exceeds usize".to_owned())
            })? != ordinal
                || scenario.family != first.family
                || scenario.expected_result_sha256 != first.expected_result_sha256
                || (ordinal > 0
                    && (scenario.requested_nodes <= scenarios[ordinal - 1].requested_nodes
                        || scenario.requested_cpu_millis
                            <= scenarios[ordinal - 1].requested_cpu_millis
                        || scenario.requested_memory_bytes
                            <= scenarios[ordinal - 1].requested_memory_bytes))
            {
                return Err(PerformanceError::InvalidContract(
                    "capacity scale points are not dense, equivalent, and increasing".to_owned(),
                ));
            }
        }
    }
    Ok(())
}

fn validate_observation(
    plan: &PerformancePlan,
    scenario: &PerformanceScenario,
    observation: &PerformanceObservation,
) -> Result<(), PerformanceError> {
    if !observation.complete
        || observation.duration_nanoseconds == 0
        || observation.operations == 0
        || observation.work_items == 0
        || observation.nodes_activated == 0
        || observation.nodes_activated > scenario.requested_nodes
        || observation.cpu_millis_activated == 0
        || observation.cpu_millis_activated > scenario.requested_cpu_millis
        || observation.ram_bytes_activated == 0
        || observation.ram_bytes_activated > scenario.requested_memory_bytes
        || observation.result_sha256 != scenario.expected_result_sha256
        || observation.autoscaling_evidence_sha256 != plan.autoscaling_evidence_sha256
        || (observation.engine == BenchmarkEngine::ExternalApacheJena
            && !scenario.require_external_jena)
    {
        return Err(PerformanceError::Incomplete(
            "partial, unequal, unbound, or over-envelope observation".to_owned(),
        ));
    }
    Ok(())
}

fn statistics(
    scenario: &PerformanceScenario,
    observations: &[&PerformanceObservation],
    engine: BenchmarkEngine,
) -> Result<EngineStatistics, PerformanceError> {
    let warmups = observations
        .iter()
        .filter(|item| item.engine == engine && item.trial_phase == TrialPhase::Warmup)
        .copied()
        .collect::<Vec<_>>();
    let measured = observations
        .iter()
        .filter(|item| item.engine == engine && item.trial_phase == TrialPhase::Measured)
        .copied()
        .collect::<Vec<_>>();
    validate_dense_trials(&warmups, scenario.warmup_trials)?;
    validate_dense_trials(&measured, scenario.measured_trials)?;
    let mut durations = measured
        .iter()
        .map(|item| item.duration_nanoseconds)
        .collect::<Vec<_>>();
    let mut throughput = measured
        .iter()
        .map(|item| {
            item.work_items
                .saturating_mul(1_000_000_000)
                .checked_div(item.duration_nanoseconds)
                .unwrap_or(0)
        })
        .collect::<Vec<_>>();
    let mut normalized_cost = measured
        .iter()
        .map(|item| {
            item.cost_micro_usd
                .saturating_mul(1_000_000)
                .checked_div(item.work_items)
                .unwrap_or(u64::MAX)
        })
        .collect::<Vec<_>>();
    durations.sort_unstable();
    throughput.sort_unstable();
    normalized_cost.sort_unstable();
    Ok(EngineStatistics {
        engine,
        measured_trials: scenario.measured_trials,
        p50_nanoseconds: nearest_rank(&durations, 50)?,
        p95_nanoseconds: nearest_rank(&durations, 95)?,
        p99_nanoseconds: nearest_rank(&durations, 99)?,
        median_throughput_per_second: nearest_rank(&throughput, 50)?,
        median_cost_micro_usd_per_million: nearest_rank(&normalized_cost, 50)?,
        maximum_nodes_activated: measured
            .iter()
            .map(|item| item.nodes_activated)
            .max()
            .unwrap_or(0),
        maximum_peak_rss_bytes: measured
            .iter()
            .map(|item| item.peak_rss_bytes)
            .max()
            .unwrap_or(0),
    })
}

fn validate_dense_trials(
    observations: &[&PerformanceObservation],
    expected: u32,
) -> Result<(), PerformanceError> {
    let indices = observations
        .iter()
        .map(|item| item.trial)
        .collect::<BTreeSet<_>>();
    if observations.len()
        != usize::try_from(expected).map_err(|_| {
            PerformanceError::InvalidContract("trial count exceeds usize".to_owned())
        })?
        || indices != (0..expected).collect::<BTreeSet<_>>()
    {
        return Err(PerformanceError::Incomplete(
            "trials are missing, duplicated, or excluded".to_owned(),
        ));
    }
    Ok(())
}

fn nearest_rank(values: &[u64], percentile: usize) -> Result<u64, PerformanceError> {
    if values.is_empty() || !(1..=100).contains(&percentile) {
        return Err(PerformanceError::InvalidContract(
            "percentile input is invalid".to_owned(),
        ));
    }
    let rank = values.len().saturating_mul(percentile).div_ceil(100);
    values
        .get(rank.saturating_sub(1))
        .copied()
        .ok_or_else(|| PerformanceError::InvalidContract("percentile rank is invalid".to_owned()))
}

fn canonical_sha256<T: Serialize>(value: &T) -> Result<String, PerformanceError> {
    serde_json::to_vec(value)
        .map(|bytes| hex::encode(Sha256::digest(bytes)))
        .map_err(|_| PerformanceError::Serialization)
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

#[cfg(test)]
mod tests {
    use super::{PerformanceError, nearest_rank, stable_partition};

    #[test]
    fn nearest_rank_is_deterministic_for_small_trial_sets() -> Result<(), PerformanceError> {
        let values = [10, 20, 30, 40, 50];
        assert_eq!(nearest_rank(&values, 50)?, 30);
        assert_eq!(nearest_rank(&values, 95)?, 50);
        assert_eq!(nearest_rank(&values, 99)?, 50);
        Ok(())
    }

    #[test]
    fn stable_partition_is_repeatable() -> Result<(), PerformanceError> {
        let first = stable_partition("scenario", &"a".repeat(64), 17)?;
        let second = stable_partition("scenario", &"a".repeat(64), 17)?;
        assert_eq!(first, second);
        assert!(first < 17);
        Ok(())
    }
}
