//! NGKG 1.0.0 General Availability certification boundary.
//!
//! This module adds no data-plane feature. It admits a GA publication only
//! when live, same-release qualification, defect closure, immutable signed
//! artifacts, production-runtime isolation, and reproducible builds agree on
//! one release identity.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

/// Exact General Availability product version.
pub const GA_VERSION: &str = "1.0.0";
/// GA evidence wire format.
pub const GA_FORMAT_VERSION: u32 = 1;

/// Mandatory live GA qualification areas.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum GaQualificationKind {
    /// Final RC1 acceptance on real HA clusters.
    Rc1Acceptance,
    /// Complete SPARQL 1.1 and distributed/scalar equivalence.
    SparqlCorrectness,
    /// Authorized cross-domain OWL 2 DL context graph.
    CrossDomainOwl2Dl,
    /// Profile, consistency, closure, proofs, and exact fallback.
    ReasoningCorrectness,
    /// Multinode and multicore distributed execution.
    MultinodeHpc,
    /// CPU-or-memory autoscaling at the 80-percent boundary.
    Autoscaling,
    /// RKE/RKE2, EKS, AKS, and GKE support matrix.
    KubernetesMatrix,
    /// S3, Azure Blob, GCS, and S3-compatible TriG ingestion.
    CloudTrigIngestion,
    /// High availability and destructive chaos qualification.
    HaChaos,
    /// Backup, restore, and disaster recovery.
    BackupRestore,
    /// Installation, upgrade, migration, and rollback.
    UpgradeRollback,
    /// Identity, authorization, encryption, isolation, and audit.
    EnterpriseSecurity,
    /// Tenant-safe query resource accounting and query logs.
    QueryLogs,
    /// Correctness-gated performance and capacity evidence.
    PerformanceCapacity,
    /// SLOs, dashboards, alerts, traces, and runbooks.
    OperationalReadiness,
    /// Rust production boundary and external-oracle isolation.
    ProductionRuntimeAudit,
    /// CVE, license, secret, and image-hardening gate.
    SecurityLicense,
    /// Two isolated, identical final builds.
    ReproducibleBuild,
    /// Final public contract and storage-format freeze.
    ContractFreeze,
    /// Signed artifact publication and independent verification.
    ArtifactPublication,
}

/// One retained live qualification certificate.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GaQualificationEvidence {
    /// Closed qualification identity.
    pub kind: GaQualificationKind,
    /// Exact retained certificate bytes.
    pub certificate_sha256: String,
    /// Exact GA release or image subject.
    pub subject_sha256: String,
    /// True only for real infrastructure and production binaries.
    pub live: bool,
    /// Synthetic/static evidence is never publishable.
    pub synthetic: bool,
    /// Terminal failures, mismatches, missing cells, or waivers.
    pub failure_count: u32,
    /// Terminal certificate marker.
    pub complete: bool,
}

/// Same-subject GA evidence ledger.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GaQualificationLedger {
    /// Wire version.
    pub format_version: u32,
    /// Product version.
    pub release_version: String,
    /// Release subject shared by every evidence item.
    pub release_sha256: String,
    /// Sorted unique mandatory evidence.
    pub qualifications: Vec<GaQualificationEvidence>,
    /// True only after the ledger is durably finalized.
    pub complete: bool,
}

/// Defect severity used by the GA barrier.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum DefectSeverity {
    /// Release-blocking correctness, isolation, security, or integrity defect.
    Critical,
    /// High-impact supported-production defect.
    High,
    /// Bounded defect with an accepted operational workaround.
    Medium,
    /// Low-impact documented defect.
    Low,
}

/// Reviewed RC defect disposition.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DefectDisposition {
    /// Stable defect identity.
    pub defect_id: String,
    /// Reviewed severity.
    pub severity: DefectSeverity,
    /// True if the issue remains unresolved.
    pub unresolved: bool,
    /// True if release management classified it as blocking.
    pub release_blocking: bool,
    /// Fix regression passed when the issue was resolved.
    pub regression_passed: bool,
    /// Compatibility impact was reviewed.
    pub compatibility_reviewed: bool,
    /// Exact review/fix evidence.
    pub evidence_sha256: String,
}

/// Complete known-defect ledger.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DefectLedger {
    /// Wire version.
    pub format_version: u32,
    /// Exact release subject.
    pub release_sha256: String,
    /// All accepted and resolved RC defects.
    pub defects: Vec<DefectDisposition>,
    /// Terminal ledger marker.
    pub complete: bool,
}

/// Production dependency/runtime isolation evidence.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ProductionRuntimeAudit {
    /// Exact release subject.
    pub release_sha256: String,
    /// The production runtime is the Rust implementation.
    pub rust_production_runtime: bool,
    /// Apache Jena is not linked, embedded, or deployed in production.
    pub apache_jena_in_production: bool,
    /// HermiT is constrained to pinned exact qualification/fallback workers.
    pub hermit_isolated_exact_boundary: bool,
    /// Dependency lock, image, and process inspection report.
    pub report_sha256: String,
    /// Terminal audit marker.
    pub complete: bool,
}

/// Mandatory published GA artifact families.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum GaArtifactClass {
    /// Deterministic source archive.
    SourceArchive,
    /// Immutable multi-architecture OCI index.
    ImageIndex,
    /// Helm chart packages.
    HelmCharts,
    /// Rendered Kubernetes installation bundle.
    KubernetesBundle,
    /// CustomResourceDefinition bundle.
    Crds,
    /// Ordered PostgreSQL migrations.
    Migrations,
    /// CLI and qualification utilities.
    Utilities,
    /// OpenAPI and JSON Schema bundle.
    ApiSchemas,
    /// SPDX SBOM.
    SbomSpdx,
    /// CycloneDX SBOM.
    SbomCycloneDx,
    /// SLSA-style provenance.
    Provenance,
    /// Complete qualification evidence bundle.
    QualificationEvidence,
    /// Operator and support documentation.
    Documentation,
    /// Top-level checksum manifest.
    Checksums,
    /// Detached signatures and transparency-log bundles.
    Signatures,
}

/// One immutable, signed GA artifact.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GaArtifact {
    /// Required artifact family.
    pub class: GaArtifactClass,
    /// Release-relative path or immutable OCI digest reference.
    pub path: String,
    /// Exact bytes or OCI manifest digest.
    pub sha256: String,
    /// Detached signature or verification-bundle digest.
    pub signature_sha256: String,
    /// Artifact media type.
    pub media_type: String,
    /// True only when mutable resolution is impossible.
    pub immutable: bool,
}

/// Final GA go/no-go certificate.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GaCertificate {
    /// Wire version.
    pub format_version: u32,
    /// Exact product version.
    pub release_version: String,
    /// Qualified release subject.
    pub release_sha256: String,
    /// Qualification ledger identity.
    pub qualification_ledger_sha256: String,
    /// Defect ledger identity.
    pub defect_ledger_sha256: String,
    /// Final contract freeze identity.
    pub freeze_manifest_sha256: String,
    /// Runtime audit identity.
    pub runtime_audit_sha256: String,
    /// Deterministic artifact root.
    pub artifact_root_sha256: String,
    /// Exact support matrix.
    pub support_matrix_sha256: String,
    /// Published known-issues declaration.
    pub known_issues_sha256: String,
    /// Final acceptance procedure.
    pub acceptance_plan_sha256: String,
    /// Zero for GA publication.
    pub failure_count: u32,
    /// Explicit production go decision.
    pub decision: String,
    /// Publication barrier.
    pub publishable: bool,
    /// Terminal certificate marker.
    pub complete: bool,
}

/// GA certification failure.
#[derive(Debug, Error, Eq, PartialEq)]
pub enum GaError {
    /// Version, checksum, or subject identity is invalid.
    #[error("GA release identity is invalid")]
    Identity,
    /// Live same-subject qualification is incomplete.
    #[error("GA live qualification ledger is incomplete")]
    Qualification,
    /// A release-blocking or unreviewed defect remains.
    #[error("GA defect closure gate failed")]
    Defects,
    /// Production dependencies violate the Rust/oracle boundary.
    #[error("GA production runtime audit failed")]
    Runtime,
    /// Artifact coverage, immutability, or signatures are incomplete.
    #[error("GA signed artifact set is incomplete")]
    Artifacts,
}

fn valid_sha(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

fn all_qualifications() -> BTreeSet<GaQualificationKind> {
    [
        GaQualificationKind::Rc1Acceptance,
        GaQualificationKind::SparqlCorrectness,
        GaQualificationKind::CrossDomainOwl2Dl,
        GaQualificationKind::ReasoningCorrectness,
        GaQualificationKind::MultinodeHpc,
        GaQualificationKind::Autoscaling,
        GaQualificationKind::KubernetesMatrix,
        GaQualificationKind::CloudTrigIngestion,
        GaQualificationKind::HaChaos,
        GaQualificationKind::BackupRestore,
        GaQualificationKind::UpgradeRollback,
        GaQualificationKind::EnterpriseSecurity,
        GaQualificationKind::QueryLogs,
        GaQualificationKind::PerformanceCapacity,
        GaQualificationKind::OperationalReadiness,
        GaQualificationKind::ProductionRuntimeAudit,
        GaQualificationKind::SecurityLicense,
        GaQualificationKind::ReproducibleBuild,
        GaQualificationKind::ContractFreeze,
        GaQualificationKind::ArtifactPublication,
    ]
    .into_iter()
    .collect()
}

fn all_artifacts() -> BTreeSet<GaArtifactClass> {
    [
        GaArtifactClass::SourceArchive,
        GaArtifactClass::ImageIndex,
        GaArtifactClass::HelmCharts,
        GaArtifactClass::KubernetesBundle,
        GaArtifactClass::Crds,
        GaArtifactClass::Migrations,
        GaArtifactClass::Utilities,
        GaArtifactClass::ApiSchemas,
        GaArtifactClass::SbomSpdx,
        GaArtifactClass::SbomCycloneDx,
        GaArtifactClass::Provenance,
        GaArtifactClass::QualificationEvidence,
        GaArtifactClass::Documentation,
        GaArtifactClass::Checksums,
        GaArtifactClass::Signatures,
    ]
    .into_iter()
    .collect()
}

/// Validate complete live same-subject qualification.
pub fn validate_ga_qualifications(ledger: &GaQualificationLedger) -> Result<(), GaError> {
    if ledger.format_version != GA_FORMAT_VERSION
        || ledger.release_version != GA_VERSION
        || !valid_sha(&ledger.release_sha256)
        || !ledger.complete
    {
        return Err(GaError::Qualification);
    }
    let mut seen = BTreeSet::new();
    for evidence in &ledger.qualifications {
        if !seen.insert(evidence.kind)
            || !valid_sha(&evidence.certificate_sha256)
            || evidence.subject_sha256 != ledger.release_sha256
            || !evidence.live
            || evidence.synthetic
            || evidence.failure_count != 0
            || !evidence.complete
        {
            return Err(GaError::Qualification);
        }
    }
    if seen != all_qualifications() {
        return Err(GaError::Qualification);
    }
    Ok(())
}

/// Reject unresolved release blockers and unreviewed defect dispositions.
pub fn validate_defects(ledger: &DefectLedger, release_sha256: &str) -> Result<(), GaError> {
    if ledger.format_version != GA_FORMAT_VERSION
        || ledger.release_sha256 != release_sha256
        || !ledger.complete
    {
        return Err(GaError::Defects);
    }
    let mut ids = BTreeSet::new();
    for defect in &ledger.defects {
        if defect.defect_id.is_empty()
            || !ids.insert(defect.defect_id.as_str())
            || !valid_sha(&defect.evidence_sha256)
            || !defect.compatibility_reviewed
            || defect.release_blocking
            || (defect.unresolved
                && matches!(defect.severity, DefectSeverity::Critical | DefectSeverity::High))
            || (!defect.unresolved && !defect.regression_passed)
        {
            return Err(GaError::Defects);
        }
    }
    Ok(())
}

/// Enforce Rust production isolation from Jena and the bounded HermiT role.
pub fn validate_runtime(audit: &ProductionRuntimeAudit, release_sha256: &str) -> Result<(), GaError> {
    if audit.release_sha256 != release_sha256
        || !audit.rust_production_runtime
        || audit.apache_jena_in_production
        || !audit.hermit_isolated_exact_boundary
        || !valid_sha(&audit.report_sha256)
        || !audit.complete
    {
        return Err(GaError::Runtime);
    }
    Ok(())
}

/// Issue the final go certificate only after every GA barrier succeeds.
pub fn certify_ga(
    qualifications: &GaQualificationLedger,
    defects: &DefectLedger,
    runtime: &ProductionRuntimeAudit,
    artifacts: &[GaArtifact],
    freeze_manifest_sha256: &str,
    support_matrix_sha256: &str,
    known_issues_sha256: &str,
    acceptance_plan_sha256: &str,
) -> Result<GaCertificate, GaError> {
    validate_ga_qualifications(qualifications)?;
    validate_defects(defects, &qualifications.release_sha256)?;
    validate_runtime(runtime, &qualifications.release_sha256)?;
    if !valid_sha(freeze_manifest_sha256)
        || !valid_sha(support_matrix_sha256)
        || !valid_sha(known_issues_sha256)
        || !valid_sha(acceptance_plan_sha256)
    {
        return Err(GaError::Identity);
    }
    let mut classes = BTreeSet::new();
    let mut paths = BTreeSet::new();
    let mut root = Sha256::new();
    for artifact in artifacts {
        if !classes.insert(artifact.class)
            || !paths.insert(artifact.path.as_str())
            || artifact.path.is_empty()
            || artifact.path.contains("..")
            || !valid_sha(&artifact.sha256)
            || !valid_sha(&artifact.signature_sha256)
            || artifact.media_type.is_empty()
            || !artifact.immutable
        {
            return Err(GaError::Artifacts);
        }
        root.update(artifact.path.as_bytes());
        root.update([0]);
        root.update(artifact.sha256.as_bytes());
        root.update([0]);
        root.update(artifact.signature_sha256.as_bytes());
        root.update([0]);
    }
    if classes != all_artifacts() {
        return Err(GaError::Artifacts);
    }
    Ok(GaCertificate {
        format_version: GA_FORMAT_VERSION,
        release_version: GA_VERSION.to_owned(),
        release_sha256: qualifications.release_sha256.clone(),
        qualification_ledger_sha256: canonical_sha256(qualifications)?,
        defect_ledger_sha256: canonical_sha256(defects)?,
        freeze_manifest_sha256: freeze_manifest_sha256.to_owned(),
        runtime_audit_sha256: canonical_sha256(runtime)?,
        artifact_root_sha256: format!("{:x}", root.finalize()),
        support_matrix_sha256: support_matrix_sha256.to_owned(),
        known_issues_sha256: known_issues_sha256.to_owned(),
        acceptance_plan_sha256: acceptance_plan_sha256.to_owned(),
        failure_count: 0,
        decision: "go".to_owned(),
        publishable: true,
        complete: true,
    })
}

fn canonical_sha256<T: Serialize>(value: &T) -> Result<String, GaError> {
    let bytes = serde_json::to_vec(value).map_err(|_| GaError::Identity)?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn synthetic_or_static_ga_evidence_is_rejected() {
        let ledger = GaQualificationLedger {
            format_version: 1,
            release_version: GA_VERSION.to_owned(),
            release_sha256: "1".repeat(64),
            qualifications: vec![GaQualificationEvidence {
                kind: GaQualificationKind::Rc1Acceptance,
                certificate_sha256: "2".repeat(64),
                subject_sha256: "1".repeat(64),
                live: false,
                synthetic: true,
                failure_count: 0,
                complete: true,
            }],
            complete: true,
        };
        assert_eq!(validate_ga_qualifications(&ledger), Err(GaError::Qualification));
    }

    #[test]
    fn apache_jena_in_production_is_rejected() {
        let audit = ProductionRuntimeAudit {
            release_sha256: "1".repeat(64),
            rust_production_runtime: true,
            apache_jena_in_production: true,
            hermit_isolated_exact_boundary: true,
            report_sha256: "2".repeat(64),
            complete: true,
        };
        assert_eq!(validate_runtime(&audit, &"1".repeat(64)), Err(GaError::Runtime));
    }
}
