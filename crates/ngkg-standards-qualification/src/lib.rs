//! Deterministic Phase 40.13.22 standards and differential qualification.
//!
//! Independent cases can execute across cores and Kubernetes Indexed Job pods. This crate owns
//! stable case partitioning and the exact all-partitions merge barrier. A certificate is emitted
//! only when every required W3C/product case agrees with its declared Apache Jena, HermiT, or
//! normative-result oracle; missing work, partial output, duplicate delivery, and mismatches fail.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

/// Phase 40.13.22 wire-format version.
pub const STANDARDS_QUALIFICATION_FORMAT_VERSION: u32 = 1;

/// Closed standards families covered by the release gate.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum StandardsFamily {
    /// RDF 1.1 TriG positive and negative syntax.
    TrigSyntax,
    /// RDF 1.1 TriG dataset equivalence.
    TrigEvaluation,
    /// SPARQL 1.1 query positive and negative syntax.
    SparqlSyntax,
    /// SPARQL 1.1 algebra and query-form evaluation.
    SparqlEvaluation,
    /// SPARQL result formats and RDF graph serialization.
    ResultFormat,
    /// SPARQL Protocol dataset parameters and negotiation.
    SparqlProtocol,
    /// SPARQL Service Description claims.
    ServiceDescription,
    /// Secured SERVICE and SERVICE SILENT behavior.
    Federation,
    /// OWL 2 Direct-Semantics query evaluation.
    OwlDirect,
    /// Required fail-closed behavior under malformed input or dependency failure.
    Failure,
}

/// Independent authority used for a case.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum OracleEngine {
    /// Normative result distributed in the pinned W3C test suite.
    W3cExpected,
    /// Pinned Apache Jena ARQ/RIOT differential implementation.
    ApacheJena,
    /// Pinned OWLAPI/HermiT Direct-Semantics implementation.
    Hermit,
}

/// Expected terminal outcome.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ExpectedOutcome {
    /// A complete canonical answer is required.
    Success,
    /// Both implementations must reject with the expected stable class.
    Failure,
}

/// Terminal outcome observed from one engine.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ObservedOutcome {
    /// Complete result emitted.
    Success,
    /// Fail-closed terminal error emitted.
    Failure,
}

/// Immutable qualification case identity.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct QualificationCase {
    /// Stable W3C IRI or product case identifier.
    pub case_id: String,
    /// Standards family.
    pub family: StandardsFamily,
    /// SHA-256 of the canonical input descriptor and every referenced input digest.
    pub input_sha256: String,
    /// Independent authority.
    pub oracle: OracleEngine,
    /// Expected terminal outcome.
    pub expected_outcome: ExpectedOutcome,
    /// Normative canonical result digest when one is independently available.
    pub expected_result_sha256: Option<String>,
    /// Stable error class required for negative/failure cases.
    pub expected_error_class: Option<String>,
    /// Dense stable partition derived from `case_id` and `input_sha256`.
    pub partition: u32,
}

/// Immutable global case plan.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct StandardsQualificationPlan {
    /// Wire-format version.
    pub format_version: u32,
    /// SHA-256 of the pinned suite/provider inventory.
    pub suite_inventory_sha256: String,
    /// Dense Indexed Job partition count.
    pub partition_count: u32,
    /// Unique cases sorted by `case_id`.
    pub cases: Vec<QualificationCase>,
}

/// One engine-pair observation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DifferentialObservation {
    /// Stable case identifier.
    pub case_id: String,
    /// Standards family repeated for local validation.
    pub family: StandardsFamily,
    /// Stable plan partition.
    pub partition: u32,
    /// NGKG terminal outcome.
    pub ngkg_outcome: ObservedOutcome,
    /// Oracle terminal outcome.
    pub oracle_outcome: ObservedOutcome,
    /// Canonical NGKG result digest for a successful case.
    pub ngkg_result_sha256: Option<String>,
    /// Canonical oracle result digest for a successful case.
    pub oracle_result_sha256: Option<String>,
    /// NGKG stable failure class for a negative case.
    pub ngkg_error_class: Option<String>,
    /// Oracle stable failure class for a negative case.
    pub oracle_error_class: Option<String>,
    /// True only when the engine explicitly certified a non-partial terminal result.
    pub complete: bool,
    /// Bounded execution duration.
    pub duration_milliseconds: u64,
}

/// One atomically written Indexed Job output.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct StandardsPartitionReport {
    /// Wire-format version.
    pub format_version: u32,
    /// Exact plan digest.
    pub plan_sha256: String,
    /// Dense completion index.
    pub partition: u32,
    /// Kubernetes pod/worker identity.
    pub worker_id: String,
    /// Observations sorted by case ID.
    pub observations: Vec<DifferentialObservation>,
    /// True only after every assigned case was durably recorded.
    pub complete: bool,
}

/// Final zero-mismatch evidence bound to the exact plan and partition set.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct StandardsQualificationCertificate {
    /// Wire-format version.
    pub format_version: u32,
    /// Exact plan digest.
    pub plan_sha256: String,
    /// Digest of reports sorted by partition.
    pub report_set_sha256: String,
    /// Total required cases.
    pub case_count: u64,
    /// Exact coverage by family.
    pub cases_by_family: BTreeMap<StandardsFamily, u64>,
    /// Exact independent authorities observed.
    pub oracle_engines: Vec<OracleEngine>,
    /// Always zero for a valid certificate.
    pub mismatch_count: u64,
    /// Always zero for a valid certificate.
    pub missing_case_count: u64,
    /// True only after the complete dense barrier passes.
    pub complete: bool,
}

/// Fail-closed qualification errors.
#[derive(Debug, Error, Eq, PartialEq)]
pub enum StandardsQualificationError {
    /// Invalid schema-level or identity contract.
    #[error("invalid standards qualification contract: {0}")]
    InvalidContract(String),
    /// Missing, partial, duplicate, or mismatched evidence.
    #[error("standards qualification is incomplete: {0}")]
    Incomplete(String),
    /// Canonical serialization failed.
    #[error("standards qualification serialization failed")]
    Serialization,
}

/// Build a topology-independent plan, assigning cases to stable dense partitions.
pub fn build_plan(
    suite_inventory_sha256: String,
    partition_count: u32,
    mut cases: Vec<QualificationCase>,
) -> Result<StandardsQualificationPlan, StandardsQualificationError> {
    if !is_sha256(&suite_inventory_sha256) || partition_count == 0 || partition_count > 65_536 {
        return Err(StandardsQualificationError::InvalidContract(
            "suite digest or partition count is invalid".to_owned(),
        ));
    }
    cases.sort_by(|left, right| left.case_id.cmp(&right.case_id));
    for case in &mut cases {
        case.partition = stable_partition(&case.case_id, &case.input_sha256, partition_count)?;
    }
    let plan = StandardsQualificationPlan {
        format_version: STANDARDS_QUALIFICATION_FORMAT_VERSION,
        suite_inventory_sha256,
        partition_count,
        cases,
    };
    validate_plan(&plan)?;
    Ok(plan)
}

/// Validate a plan before any work is admitted.
pub fn validate_plan(
    plan: &StandardsQualificationPlan,
) -> Result<(), StandardsQualificationError> {
    if plan.format_version != STANDARDS_QUALIFICATION_FORMAT_VERSION
        || !is_sha256(&plan.suite_inventory_sha256)
        || plan.partition_count == 0
        || plan.partition_count > 65_536
        || plan.cases.is_empty()
    {
        return Err(StandardsQualificationError::InvalidContract(
            "plan header or case set is invalid".to_owned(),
        ));
    }
    let mut previous: Option<&str> = None;
    for case in &plan.cases {
        validate_case(case, plan.partition_count)?;
        if previous.is_some_and(|value| value >= case.case_id.as_str()) {
            return Err(StandardsQualificationError::InvalidContract(
                "case IDs must be unique and strictly sorted".to_owned(),
            ));
        }
        previous = Some(&case.case_id);
    }
    Ok(())
}

/// Merge a complete dense report set and emit a zero-mismatch certificate.
pub fn certify_standards(
    plan: &StandardsQualificationPlan,
    reports: &[StandardsPartitionReport],
) -> Result<StandardsQualificationCertificate, StandardsQualificationError> {
    validate_plan(plan)?;
    let plan_sha256 = canonical_sha256(plan)?;
    if reports.len() != usize::try_from(plan.partition_count).unwrap_or(usize::MAX) {
        return Err(StandardsQualificationError::Incomplete(
            "one complete report per dense partition is required".to_owned(),
        ));
    }
    let expected = plan
        .cases
        .iter()
        .map(|case| (case.case_id.as_str(), case))
        .collect::<BTreeMap<_, _>>();
    let mut reports_sorted = reports.to_vec();
    reports_sorted.sort_by_key(|report| report.partition);
    let mut observed_ids = BTreeSet::new();
    let mut workers = BTreeSet::new();
    for (expected_partition, report) in reports_sorted.iter().enumerate() {
        if report.format_version != STANDARDS_QUALIFICATION_FORMAT_VERSION
            || report.plan_sha256 != plan_sha256
            || report.partition != u32::try_from(expected_partition).unwrap_or(u32::MAX)
            || report.worker_id.is_empty()
            || !report.complete
            || !workers.insert(report.worker_id.as_str())
        {
            return Err(StandardsQualificationError::Incomplete(
                "partition identity, worker identity, or completion barrier failed".to_owned(),
            ));
        }
        let assigned = plan
            .cases
            .iter()
            .filter(|case| case.partition == report.partition)
            .map(|case| case.case_id.as_str())
            .collect::<Vec<_>>();
        if assigned.len() != report.observations.len() {
            return Err(StandardsQualificationError::Incomplete(
                "partition observation count differs from its exact assignment".to_owned(),
            ));
        }
        let mut previous: Option<&str> = None;
        for observation in &report.observations {
            if previous.is_some_and(|value| value >= observation.case_id.as_str())
                || !observed_ids.insert(observation.case_id.as_str())
            {
                return Err(StandardsQualificationError::Incomplete(
                    "observations must be globally unique and sorted within partitions".to_owned(),
                ));
            }
            previous = Some(&observation.case_id);
            let case = expected.get(observation.case_id.as_str()).ok_or_else(|| {
                StandardsQualificationError::Incomplete(
                    "report contains a case outside the immutable plan".to_owned(),
                )
            })?;
            validate_observation(case, observation)?;
        }
    }
    if observed_ids.len() != expected.len() {
        return Err(StandardsQualificationError::Incomplete(
            "the all-cases completion barrier is incomplete".to_owned(),
        ));
    }
    let mut cases_by_family = BTreeMap::new();
    let mut oracle_engines = BTreeSet::new();
    for case in &plan.cases {
        *cases_by_family.entry(case.family).or_insert(0_u64) += 1;
        oracle_engines.insert(case.oracle);
    }
    Ok(StandardsQualificationCertificate {
        format_version: STANDARDS_QUALIFICATION_FORMAT_VERSION,
        plan_sha256,
        report_set_sha256: canonical_sha256(&reports_sorted)?,
        case_count: u64::try_from(plan.cases.len()).map_err(|_| {
            StandardsQualificationError::InvalidContract("case count exceeds u64".to_owned())
        })?,
        cases_by_family,
        oracle_engines: oracle_engines.into_iter().collect(),
        mismatch_count: 0,
        missing_case_count: 0,
        complete: true,
    })
}

fn validate_case(
    case: &QualificationCase,
    partition_count: u32,
) -> Result<(), StandardsQualificationError> {
    if case.case_id.is_empty()
        || case.case_id.len() > 2048
        || !is_sha256(&case.input_sha256)
        || case.partition != stable_partition(&case.case_id, &case.input_sha256, partition_count)?
    {
        return Err(StandardsQualificationError::InvalidContract(
            "case identity, input digest, or stable partition is invalid".to_owned(),
        ));
    }
    match case.expected_outcome {
        ExpectedOutcome::Success => {
            if case.expected_error_class.is_some()
                || case
                    .expected_result_sha256
                    .as_ref()
                    .is_some_and(|value| !is_sha256(value))
            {
                return Err(StandardsQualificationError::InvalidContract(
                    "successful case has invalid expected evidence".to_owned(),
                ));
            }
        }
        ExpectedOutcome::Failure => {
            if case.expected_result_sha256.is_some()
                || case
                    .expected_error_class
                    .as_ref()
                    .is_none_or(|value| value.is_empty() || value.len() > 128)
            {
                return Err(StandardsQualificationError::InvalidContract(
                    "failure case lacks one bounded error class".to_owned(),
                ));
            }
        }
    }
    Ok(())
}

fn validate_observation(
    case: &QualificationCase,
    observation: &DifferentialObservation,
) -> Result<(), StandardsQualificationError> {
    if observation.case_id != case.case_id
        || observation.family != case.family
        || observation.partition != case.partition
        || !observation.complete
    {
        return Err(StandardsQualificationError::Incomplete(
            "observation identity or completeness differs from the plan".to_owned(),
        ));
    }
    match case.expected_outcome {
        ExpectedOutcome::Success => {
            let ngkg = observation.ngkg_result_sha256.as_deref();
            let oracle = observation.oracle_result_sha256.as_deref();
            if observation.ngkg_outcome != ObservedOutcome::Success
                || observation.oracle_outcome != ObservedOutcome::Success
                || observation.ngkg_error_class.is_some()
                || observation.oracle_error_class.is_some()
                || ngkg.is_none_or(|value| !is_sha256(value))
                || oracle.is_none_or(|value| !is_sha256(value))
                || ngkg != oracle
                || case
                    .expected_result_sha256
                    .as_deref()
                    .is_some_and(|expected| Some(expected) != ngkg)
            {
                return Err(StandardsQualificationError::Incomplete(
                    "successful NGKG and oracle canonical results differ".to_owned(),
                ));
            }
        }
        ExpectedOutcome::Failure => {
            if observation.ngkg_outcome != ObservedOutcome::Failure
                || observation.oracle_outcome != ObservedOutcome::Failure
                || observation.ngkg_result_sha256.is_some()
                || observation.oracle_result_sha256.is_some()
                || observation.ngkg_error_class != case.expected_error_class
                || observation.oracle_error_class != case.expected_error_class
            {
                return Err(StandardsQualificationError::Incomplete(
                    "negative-case failure classes differ or a partial result escaped".to_owned(),
                ));
            }
        }
    }
    Ok(())
}

fn stable_partition(
    case_id: &str,
    input_sha256: &str,
    partition_count: u32,
) -> Result<u32, StandardsQualificationError> {
    if case_id.is_empty() || !is_sha256(input_sha256) || partition_count == 0 {
        return Err(StandardsQualificationError::InvalidContract(
            "stable partition input is invalid".to_owned(),
        ));
    }
    let digest = Sha256::digest(format!("{case_id}\0{input_sha256}").as_bytes());
    let prefix: [u8; 8] = digest[..8].try_into().map_err(|_| {
        StandardsQualificationError::InvalidContract("partition digest is truncated".to_owned())
    })?;
    Ok(u32::try_from(u64::from_be_bytes(prefix) % u64::from(partition_count))
        .unwrap_or(u32::MAX))
}

fn canonical_sha256<T: Serialize>(value: &T) -> Result<String, StandardsQualificationError> {
    serde_json::to_vec(value)
        .map(|bytes| hex::encode(Sha256::digest(bytes)))
        .map_err(|_| StandardsQualificationError::Serialization)
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cases() -> Vec<QualificationCase> {
        vec![
            QualificationCase {
                case_id: "case-a".to_owned(),
                family: StandardsFamily::SparqlEvaluation,
                input_sha256: "1".repeat(64),
                oracle: OracleEngine::ApacheJena,
                expected_outcome: ExpectedOutcome::Success,
                expected_result_sha256: None,
                expected_error_class: None,
                partition: 0,
            },
            QualificationCase {
                case_id: "case-b".to_owned(),
                family: StandardsFamily::Failure,
                input_sha256: "2".repeat(64),
                oracle: OracleEngine::W3cExpected,
                expected_outcome: ExpectedOutcome::Failure,
                expected_result_sha256: None,
                expected_error_class: Some("MALFORMED_QUERY".to_owned()),
                partition: 0,
            },
        ]
    }

    #[test]
    fn stable_partition_is_topology_bound_but_order_independent(
    ) -> Result<(), StandardsQualificationError> {
        let mut reversed = cases();
        reversed.reverse();
        let left = build_plan("a".repeat(64), 2, cases())?;
        let right = build_plan("a".repeat(64), 2, reversed)?;
        assert_eq!(left, right);
        Ok(())
    }

    #[test]
    fn complete_zero_mismatch_reports_certify() -> Result<(), StandardsQualificationError> {
        let plan = build_plan("a".repeat(64), 2, cases())?;
        let digest = canonical_sha256(&plan)?;
        let reports = (0..2)
            .map(|partition| StandardsPartitionReport {
                format_version: 1,
                plan_sha256: digest.clone(),
                partition,
                worker_id: format!("worker-{partition}"),
                observations: plan
                    .cases
                    .iter()
                    .filter(|case| case.partition == partition)
                    .map(|case| match case.expected_outcome {
                        ExpectedOutcome::Success => DifferentialObservation {
                            case_id: case.case_id.clone(),
                            family: case.family,
                            partition,
                            ngkg_outcome: ObservedOutcome::Success,
                            oracle_outcome: ObservedOutcome::Success,
                            ngkg_result_sha256: Some("b".repeat(64)),
                            oracle_result_sha256: Some("b".repeat(64)),
                            ngkg_error_class: None,
                            oracle_error_class: None,
                            complete: true,
                            duration_milliseconds: 1,
                        },
                        ExpectedOutcome::Failure => DifferentialObservation {
                            case_id: case.case_id.clone(),
                            family: case.family,
                            partition,
                            ngkg_outcome: ObservedOutcome::Failure,
                            oracle_outcome: ObservedOutcome::Failure,
                            ngkg_result_sha256: None,
                            oracle_result_sha256: None,
                            ngkg_error_class: case.expected_error_class.clone(),
                            oracle_error_class: case.expected_error_class.clone(),
                            complete: true,
                            duration_milliseconds: 1,
                        },
                    })
                    .collect(),
                complete: true,
            })
            .collect::<Vec<_>>();
        let certificate = certify_standards(&plan, &reports)?;
        assert!(certificate.complete);
        assert_eq!(certificate.case_count, 2);
        assert_eq!(certificate.mismatch_count, 0);
        Ok(())
    }

    #[test]
    fn one_mismatch_blocks_the_global_certificate() -> Result<(), StandardsQualificationError> {
        let mut source_cases = cases();
        let plan = build_plan("a".repeat(64), 1, vec![source_cases.remove(0)])?;
        let report = StandardsPartitionReport {
            format_version: 1,
            plan_sha256: canonical_sha256(&plan)?,
            partition: 0,
            worker_id: "worker-0".to_owned(),
            observations: vec![DifferentialObservation {
                case_id: plan.cases[0].case_id.clone(),
                family: plan.cases[0].family,
                partition: 0,
                ngkg_outcome: ObservedOutcome::Success,
                oracle_outcome: ObservedOutcome::Success,
                ngkg_result_sha256: Some("b".repeat(64)),
                oracle_result_sha256: Some("c".repeat(64)),
                ngkg_error_class: None,
                oracle_error_class: None,
                complete: true,
                duration_milliseconds: 1,
            }],
            complete: true,
        };
        assert!(certify_standards(&plan, &[report]).is_err());
        Ok(())
    }
}
