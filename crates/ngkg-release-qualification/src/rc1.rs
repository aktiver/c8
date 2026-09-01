//! Phase 1.0.0-RC1 immutable freeze and publication certification.
//!
//! This module adds no query, reasoning, storage, or autoscaling behavior. It
//! closes the release boundary around already-qualified production behavior.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

/// Exact product version represented by this release line.
pub const RC1_VERSION: &str = "1.0.0-rc.1";
/// RC1 wire format.
pub const RC1_FORMAT_VERSION: u32 = 1;

/// Mandatory prior qualification identities.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum PrerequisiteKind {
    /// Exact SPARQL 1.1 forms and algebra.
    Sparql11,
    /// Authorized union-default and named graph semantics.
    AuthorizedRdfDataset,
    /// OWL 2 DL profile and global consistency.
    Owl2Dl,
    /// Distributed finite closure and exact fallback.
    DistributedReasoning,
    /// Distributed joins, paths, spill, checkpoints, and termination.
    DistributedQueryRuntime,
    /// Atomic snapshot publication.
    AtomicPublication,
    /// Secured federation and SPARQL Protocol.
    Federation,
    /// Replication, relocation, backup, restore, and recovery.
    StorageRecovery,
    /// Live 80-percent CPU-or-memory autoscaling.
    Autoscaling,
    /// Enterprise security and operations.
    EnterpriseSecurity,
    /// W3C and differential qualification.
    Standards,
    /// Enterprise performance and capacity qualification.
    PerformanceCapacity,
    /// RKE/RKE2, EKS, AKS, and GKE release qualification.
    KubernetesRelease,
    /// Cross-domain reasoned context graph equivalence.
    SemanticContextGraph,
}

/// Origin and strength of one evidence item.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum EvidenceClass {
    /// Retained observation from the required real system and infrastructure.
    LiveProductionQualification,
    /// Source/static validation only; never publishable.
    StaticOnly,
    /// Synthetic harness validation only; never publishable.
    SyntheticOnly,
}

/// One mandatory prerequisite certificate reference.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct PrerequisiteEvidence {
    /// Closed prerequisite identity.
    pub kind: PrerequisiteKind,
    /// Evidence strength.
    pub evidence_class: EvidenceClass,
    /// Exact retained certificate bytes.
    pub certificate_sha256: String,
    /// Exact qualified release/image identity.
    pub subject_sha256: String,
    /// True only for a terminal zero-failure certificate.
    pub complete: bool,
    /// Number of failures, mismatches, missing partitions, or waivers.
    pub failure_count: u32,
    /// Explicit flag preventing synthetic evidence from being relabeled.
    pub synthetic: bool,
}

/// Closed prerequisite ledger.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct PrerequisiteLedger {
    /// Wire version.
    pub format_version: u32,
    /// Candidate release version.
    pub release_version: String,
    /// Exact candidate identity shared by every prerequisite.
    pub release_sha256: String,
    /// Sorted unique mandatory evidence.
    pub prerequisites: Vec<PrerequisiteEvidence>,
    /// True only after the entire ledger is durable.
    pub complete: bool,
}

/// Public or operational surface families frozen for the 1.0 line.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum FreezeSurface {
    /// REST routes, methods, operation IDs, media types, and errors.
    OpenApi,
    /// JSON Schemas and wire formats.
    JsonSchema,
    /// Kubernetes CustomResourceDefinitions.
    Crd,
    /// Helm values, chart metadata, and rendered template sources.
    Helm,
    /// Public configuration environment variables.
    Environment,
    /// Ordered PostgreSQL migrations.
    DatabaseMigration,
    /// Object-store and immutable artifact layouts.
    ObjectLayout,
    /// Snapshot, proof, checkpoint, and certificate formats.
    SemanticArtifact,
}

/// One frozen file or extracted interface inventory.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct FrozenEntry {
    /// Surface family.
    pub surface: FreezeSurface,
    /// Repository-relative canonical path or extracted inventory key.
    pub path: String,
    /// Exact canonical bytes.
    pub sha256: String,
    /// Number of public items represented by the entry.
    pub item_count: u64,
}

/// Immutable compatibility freeze.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct FreezeManifest {
    /// Wire version.
    pub format_version: u32,
    /// Frozen release version.
    pub release_version: String,
    /// Exact source manifest identity.
    pub source_manifest_sha256: String,
    /// Canonical sorted entries.
    pub entries: Vec<FrozenEntry>,
    /// Compatibility changes require a reviewed RC defect.
    pub changes_require_rc_defect: bool,
    /// True only after every required surface is present.
    pub complete: bool,
}

/// Required deliverable classes.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ArtifactClass {
    /// Deterministic source archive.
    SourceArchive,
    /// Multi-architecture immutable image index.
    ImageIndex,
    /// Helm chart packages.
    HelmCharts,
    /// Rendered Kubernetes installation bundle.
    KubernetesBundle,
    /// Versioned CRD bundle.
    Crds,
    /// Ordered database migrations.
    Migrations,
    /// CLI and qualification utilities.
    Utilities,
    /// OpenAPI and JSON Schema bundle.
    ApiSchemas,
    /// SPDX software bill of materials.
    SbomSpdx,
    /// CycloneDX software bill of materials.
    SbomCycloneDx,
    /// Build provenance/attestation.
    Provenance,
    /// Complete prior qualification evidence.
    QualificationEvidence,
    /// Installation, operation, upgrade, and support documentation.
    Documentation,
    /// Top-level checksum manifest.
    Checksums,
}

/// One signed, immutable release artifact.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ReleaseArtifact {
    /// Required artifact family.
    pub class: ArtifactClass,
    /// Repository/release-relative path or OCI digest reference.
    pub path: String,
    /// Exact bytes or OCI manifest identity.
    pub sha256: String,
    /// Detached signature or transparency-log bundle identity.
    pub signature_sha256: String,
    /// Media type.
    pub media_type: String,
}

/// Supply-chain and security release gate.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct SupplyChainEvidence {
    /// Signature-verification report.
    pub signature_report_sha256: String,
    /// Provenance-verification report.
    pub provenance_report_sha256: String,
    /// Secret scan report.
    pub secret_scan_sha256: String,
    /// License policy report.
    pub license_report_sha256: String,
    /// Vulnerability scan report.
    pub vulnerability_report_sha256: String,
    /// Unapproved critical CVEs.
    pub unapproved_critical_cves: u32,
    /// Unapproved high CVEs.
    pub unapproved_high_cves: u32,
    /// Long-lived or embedded credentials found.
    pub embedded_credentials: u32,
    /// License policy passed.
    pub license_policy_complete: bool,
    /// Image/runtime hardening policy passed.
    pub runtime_hardening_complete: bool,
    /// Workload identity policy passed on all providers.
    pub workload_identity_complete: bool,
    /// Default-deny network policy qualification passed.
    pub network_policy_complete: bool,
    /// Complete terminal evidence.
    pub complete: bool,
}

/// Two isolated release builders must produce the same identity set.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ReproducibleBuildEvidence {
    /// First isolated builder output manifest.
    pub builder_a_manifest_sha256: String,
    /// Second isolated builder output manifest.
    pub builder_b_manifest_sha256: String,
    /// Normalized source input identity.
    pub source_sha256: String,
    /// Network access was denied after dependency hydration.
    pub network_controlled: bool,
    /// Dependency resolution used immutable locks.
    pub dependencies_locked: bool,
    /// Timestamps and archive metadata were normalized.
    pub timestamps_normalized: bool,
    /// Exact binary/image/chart/bundle equivalence.
    pub complete: bool,
}

/// Final RC1 publication certificate.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Rc1Certificate {
    /// Wire version.
    pub format_version: u32,
    /// Frozen release version.
    pub release_version: String,
    /// Candidate release identity.
    pub release_sha256: String,
    /// Prerequisite ledger identity.
    pub prerequisite_ledger_sha256: String,
    /// Compatibility freeze identity.
    pub freeze_manifest_sha256: String,
    /// Artifact inventory identity.
    pub artifact_manifest_sha256: String,
    /// Deterministic root over every artifact and signature.
    pub artifact_root_sha256: String,
    /// Supported Kubernetes matrix identity.
    pub support_matrix_sha256: String,
    /// Known-issues identity.
    pub known_issues_sha256: String,
    /// Final acceptance-test plan identity.
    pub acceptance_plan_sha256: String,
    /// Zero for a publishable RC1.
    pub failure_count: u32,
    /// True only when release publication is allowed.
    pub publishable: bool,
    /// Terminal certificate barrier.
    pub complete: bool,
}

/// RC1 certification failure.
#[derive(Debug, Error, Eq, PartialEq)]
pub enum Rc1Error {
    /// Identity or header is invalid.
    #[error("RC1 identity or version is invalid")]
    InvalidIdentity,
    /// Prior qualification is missing, static, synthetic, failed, or mismatched.
    #[error("RC1 prerequisite ledger is not production-qualified")]
    Prerequisites,
    /// Frozen interface coverage is incomplete.
    #[error("RC1 compatibility freeze is incomplete")]
    Freeze,
    /// Artifact, signature, SBOM, or documentation coverage is incomplete.
    #[error("RC1 signed artifact inventory is incomplete")]
    Artifacts,
    /// Build equivalence or security evidence is incomplete.
    #[error("RC1 reproducibility or supply-chain gate failed")]
    SupplyChain,
}

fn valid_sha(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

fn all_prerequisites() -> BTreeSet<PrerequisiteKind> {
    [
        PrerequisiteKind::Sparql11,
        PrerequisiteKind::AuthorizedRdfDataset,
        PrerequisiteKind::Owl2Dl,
        PrerequisiteKind::DistributedReasoning,
        PrerequisiteKind::DistributedQueryRuntime,
        PrerequisiteKind::AtomicPublication,
        PrerequisiteKind::Federation,
        PrerequisiteKind::StorageRecovery,
        PrerequisiteKind::Autoscaling,
        PrerequisiteKind::EnterpriseSecurity,
        PrerequisiteKind::Standards,
        PrerequisiteKind::PerformanceCapacity,
        PrerequisiteKind::KubernetesRelease,
        PrerequisiteKind::SemanticContextGraph,
    ]
    .into_iter()
    .collect()
}

fn all_surfaces() -> BTreeSet<FreezeSurface> {
    [
        FreezeSurface::OpenApi,
        FreezeSurface::JsonSchema,
        FreezeSurface::Crd,
        FreezeSurface::Helm,
        FreezeSurface::Environment,
        FreezeSurface::DatabaseMigration,
        FreezeSurface::ObjectLayout,
        FreezeSurface::SemanticArtifact,
    ]
    .into_iter()
    .collect()
}

fn all_artifacts() -> BTreeSet<ArtifactClass> {
    [
        ArtifactClass::SourceArchive,
        ArtifactClass::ImageIndex,
        ArtifactClass::HelmCharts,
        ArtifactClass::KubernetesBundle,
        ArtifactClass::Crds,
        ArtifactClass::Migrations,
        ArtifactClass::Utilities,
        ArtifactClass::ApiSchemas,
        ArtifactClass::SbomSpdx,
        ArtifactClass::SbomCycloneDx,
        ArtifactClass::Provenance,
        ArtifactClass::QualificationEvidence,
        ArtifactClass::Documentation,
        ArtifactClass::Checksums,
    ]
    .into_iter()
    .collect()
}

/// Verify that only real, complete, same-subject prerequisite evidence is admitted.
pub fn validate_prerequisites(ledger: &PrerequisiteLedger) -> Result<(), Rc1Error> {
    if ledger.format_version != RC1_FORMAT_VERSION
        || ledger.release_version != RC1_VERSION
        || !valid_sha(&ledger.release_sha256)
        || !ledger.complete
    {
        return Err(Rc1Error::Prerequisites);
    }
    let mut kinds = BTreeSet::new();
    for evidence in &ledger.prerequisites {
        if !kinds.insert(evidence.kind)
            || evidence.evidence_class != EvidenceClass::LiveProductionQualification
            || !valid_sha(&evidence.certificate_sha256)
            || evidence.subject_sha256 != ledger.release_sha256
            || !evidence.complete
            || evidence.failure_count != 0
            || evidence.synthetic
        {
            return Err(Rc1Error::Prerequisites);
        }
    }
    if kinds != all_prerequisites() {
        return Err(Rc1Error::Prerequisites);
    }
    Ok(())
}

/// Verify complete, canonical interface-freeze coverage.
pub fn validate_freeze(freeze: &FreezeManifest) -> Result<(), Rc1Error> {
    if freeze.format_version != RC1_FORMAT_VERSION
        || freeze.release_version != RC1_VERSION
        || !valid_sha(&freeze.source_manifest_sha256)
        || !freeze.changes_require_rc_defect
        || !freeze.complete
    {
        return Err(Rc1Error::Freeze);
    }
    let mut identities = BTreeSet::new();
    let mut surfaces = BTreeSet::new();
    for entry in &freeze.entries {
        if entry.path.is_empty()
            || entry.path.starts_with('/')
            || entry.path.contains("..")
            || !valid_sha(&entry.sha256)
            || entry.item_count == 0
            || !identities.insert((entry.surface, entry.path.as_str()))
        {
            return Err(Rc1Error::Freeze);
        }
        surfaces.insert(entry.surface);
    }
    if surfaces != all_surfaces() {
        return Err(Rc1Error::Freeze);
    }
    Ok(())
}

/// Issue a publication certificate only after every release barrier succeeds.
pub fn certify_rc1(
    ledger: &PrerequisiteLedger,
    freeze: &FreezeManifest,
    artifacts: &[ReleaseArtifact],
    supply_chain: &SupplyChainEvidence,
    reproducible: &ReproducibleBuildEvidence,
    support_matrix_sha256: &str,
    known_issues_sha256: &str,
    acceptance_plan_sha256: &str,
) -> Result<Rc1Certificate, Rc1Error> {
    validate_prerequisites(ledger)?;
    validate_freeze(freeze)?;
    if ![
        support_matrix_sha256,
        known_issues_sha256,
        acceptance_plan_sha256,
    ]
    .iter()
    .all(|value| valid_sha(value))
    {
        return Err(Rc1Error::InvalidIdentity);
    }
    let mut classes = BTreeSet::new();
    let mut paths = BTreeSet::new();
    let mut root = Sha256::new();
    for artifact in artifacts {
        if artifact.path.is_empty()
            || artifact.path.contains("..")
            || !classes.insert(artifact.class)
            || !paths.insert(artifact.path.as_str())
            || !valid_sha(&artifact.sha256)
            || !valid_sha(&artifact.signature_sha256)
            || artifact.media_type.is_empty()
        {
            return Err(Rc1Error::Artifacts);
        }
        root.update(artifact.path.as_bytes());
        root.update([0]);
        root.update(artifact.sha256.as_bytes());
        root.update([0]);
        root.update(artifact.signature_sha256.as_bytes());
        root.update([0]);
    }
    if classes != all_artifacts() {
        return Err(Rc1Error::Artifacts);
    }
    let supply_hashes = [
        &supply_chain.signature_report_sha256,
        &supply_chain.provenance_report_sha256,
        &supply_chain.secret_scan_sha256,
        &supply_chain.license_report_sha256,
        &supply_chain.vulnerability_report_sha256,
    ];
    if supply_hashes.iter().any(|value| !valid_sha(value))
        || supply_chain.unapproved_critical_cves != 0
        || supply_chain.unapproved_high_cves != 0
        || supply_chain.embedded_credentials != 0
        || !supply_chain.license_policy_complete
        || !supply_chain.runtime_hardening_complete
        || !supply_chain.workload_identity_complete
        || !supply_chain.network_policy_complete
        || !supply_chain.complete
        || !valid_sha(&reproducible.builder_a_manifest_sha256)
        || reproducible.builder_a_manifest_sha256 != reproducible.builder_b_manifest_sha256
        || !valid_sha(&reproducible.source_sha256)
        || !reproducible.network_controlled
        || !reproducible.dependencies_locked
        || !reproducible.timestamps_normalized
        || !reproducible.complete
    {
        return Err(Rc1Error::SupplyChain);
    }
    let ledger_sha256 = canonical_sha256(ledger)?;
    let freeze_sha256 = canonical_sha256(freeze)?;
    let artifact_manifest_sha256 = canonical_sha256(&artifacts)?;
    Ok(Rc1Certificate {
        format_version: RC1_FORMAT_VERSION,
        release_version: RC1_VERSION.to_owned(),
        release_sha256: ledger.release_sha256.clone(),
        prerequisite_ledger_sha256: ledger_sha256,
        freeze_manifest_sha256: freeze_sha256,
        artifact_manifest_sha256,
        artifact_root_sha256: format!("{:x}", root.finalize()),
        support_matrix_sha256: support_matrix_sha256.to_owned(),
        known_issues_sha256: known_issues_sha256.to_owned(),
        acceptance_plan_sha256: acceptance_plan_sha256.to_owned(),
        failure_count: 0,
        publishable: true,
        complete: true,
    })
}

fn canonical_sha256<T: Serialize>(value: &T) -> Result<String, Rc1Error> {
    let object = serde_json::to_value(value).map_err(|_| Rc1Error::InvalidIdentity)?;
    let bytes = serde_json::to_vec(&object).map_err(|_| Rc1Error::InvalidIdentity)?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn synthetic_prerequisite_is_rejected() {
        let ledger = PrerequisiteLedger {
            format_version: 1,
            release_version: RC1_VERSION.into(),
            release_sha256: "1".repeat(64),
            prerequisites: vec![PrerequisiteEvidence {
                kind: PrerequisiteKind::Sparql11,
                evidence_class: EvidenceClass::SyntheticOnly,
                certificate_sha256: "2".repeat(64),
                subject_sha256: "1".repeat(64),
                complete: true,
                failure_count: 0,
                synthetic: true,
            }],
            complete: true,
        };
        assert_eq!(
            validate_prerequisites(&ledger),
            Err(Rc1Error::Prerequisites)
        );
    }
}
