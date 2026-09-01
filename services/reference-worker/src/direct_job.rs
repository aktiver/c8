use std::{
    fs,
    path::{Component, Path, PathBuf},
    time::Duration,
};

use ngkg_dataset::{GraphCatalog, ResolvedDataset, validate_resolved_dataset};
use ngkg_direct_reasoner::{
    DirectExactAdapter, DirectExactBindings, DirectExactLimits, execute_exact_direct_bgp,
};
use ngkg_reference::{
    CompiledSparqlQuery, OwlSignature, ReferenceSnapshotManifest,
    build_direct_active_ontology_bundle,
};
use ngkg_types::{
    DirectBgpLegalityReport, DirectBgpLegalityStatus, DirectBgpScope,
    validate_direct_bgp_legality_report,
};
use serde::Deserialize;
use sha2::{Digest, Sha256};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DirectJob {
    format_version: u32,
    /// Existing read-only snapshot directory containing the manifest payload tree.
    snapshot_root: PathBuf,
    snapshot_manifest_path: PathBuf,
    snapshot_manifest_sha256: String,
    /// Existing directory containing immutable request/query/legality inputs.
    request_root: PathBuf,
    query_path: PathBuf,
    query_sha256: String,
    legality_report_path: PathBuf,
    /// Exact graph-set envelope produced by the Phase 37 resolver used by Phase 40.7.
    resolved_dataset: ResolvedDataset,
    bgp_ordinal: u64,
    /// Required only for a Phase 40.7 `GRAPH ?g` BGP. Constant/default scopes derive themselves.
    #[serde(default)]
    graph_binding_iri: Option<String>,
    java_executable: PathBuf,
    reasoner_adapter_jar: PathBuf,
    reasoner_adapter_sha256: String,
    reasoner_adapter_version: String,
    reasoner_version: String,
    /// Existing or creatable writable root. Every transient/output file must remain underneath it.
    work_root: PathBuf,
    work_dir: PathBuf,
    output_result_path: PathBuf,
    output_certificate_path: PathBuf,
    output_proof_manifest_path: PathBuf,
    limits: DirectJobLimits,
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct DirectJobLimits {
    pub(crate) max_candidate_bindings: u64,
    pub(crate) max_partition_candidates: u64,
    pub(crate) max_grounded_axioms_per_candidate: u64,
    pub(crate) max_grounded_rdf_bytes_per_candidate: u64,
    pub(crate) max_local_reasoner_lanes: usize,
    pub(crate) reasoner_heap_mib_per_lane: u64,
    pub(crate) reasoner_timeout_seconds: u64,
}

pub fn execute(path: &Path) -> Result<String, String> {
    let job: DirectJob = serde_json::from_slice(&fs::read(path).map_err(|e| e.to_string())?)
        .map_err(|e| e.to_string())?;
    if job.format_version != 1 {
        return Err("unsupported direct-job formatVersion".to_owned());
    }
    if job.reasoner_adapter_version != "40.9" || job.reasoner_version != "1.4.5.519" {
        return Err("direct-job reasoner/adapter version does not match the Phase 40.9 proof-enabled exact engine".to_owned());
    }
    let trusted_phase40 = crate::phase40_limits::TrustedPhase40DirectCeilings::from_env()?;
    trusted_phase40.enforce_job(&job.limits)?;
    if job.limits.max_local_reasoner_lanes == 0 || job.limits.max_local_reasoner_lanes > 32 {
        return Err("maxLocalReasonerLanes must be between 1 and 32".to_owned());
    }
    if job.limits.reasoner_heap_mib_per_lane < 256 {
        return Err("reasonerHeapMibPerLane must be at least 256 MiB".to_owned());
    }

    let snapshot_root = canonical_existing_dir(&job.snapshot_root, "snapshotRoot")?;
    let request_root = canonical_existing_dir(&job.request_root, "requestRoot")?;
    fs::create_dir_all(&job.work_root).map_err(|e| e.to_string())?;
    let work_root = canonical_existing_dir(&job.work_root, "workRoot")?;
    require_existing_descendant(
        &snapshot_root,
        &job.snapshot_manifest_path,
        "snapshotManifestPath",
    )?;
    require_existing_descendant(&request_root, &job.query_path, "queryPath")?;
    require_existing_descendant(
        &request_root,
        &job.legality_report_path,
        "legalityReportPath",
    )?;
    require_output_descendant(&work_root, &job.work_dir, "workDir")?;
    require_output_descendant(&work_root, &job.output_result_path, "outputResultPath")?;
    require_output_descendant(
        &work_root,
        &job.output_certificate_path,
        "outputCertificatePath",
    )?;
    require_output_descendant(
        &work_root,
        &job.output_proof_manifest_path,
        "outputProofManifestPath",
    )?;

    verify_hash(&job.snapshot_manifest_path, &job.snapshot_manifest_sha256)?;
    verify_hash(&job.query_path, &job.query_sha256)?;
    verify_hash(&job.reasoner_adapter_jar, &job.reasoner_adapter_sha256)?;

    let manifest: ReferenceSnapshotManifest =
        serde_json::from_slice(&fs::read(&job.snapshot_manifest_path).map_err(|e| e.to_string())?)
            .map_err(|e| e.to_string())?;
    let signature_sha = manifest
        .owl_signature_sha256
        .clone()
        .ok_or_else(|| "snapshot lacks Phase 40.1 OWL signature".to_owned())?;
    let datatype_sha = manifest
        .datatype_policy_sha256
        .clone()
        .ok_or_else(|| "snapshot lacks Phase 40.2 datatype policy".to_owned())?;
    let profile_sha = manifest
        .owl_profile_qualification_sha256
        .clone()
        .ok_or_else(|| "snapshot lacks Phase 40.5 profile qualification".to_owned())?;
    let consistency_sha = manifest
        .owl_consistency_qualification_sha256
        .clone()
        .ok_or_else(|| "snapshot lacks Phase 40.6 consistency qualification".to_owned())?;

    let signature_path = snapshot_root.join("reasoner/owl-signature.json");
    verify_hash(&signature_path, &signature_sha)?;
    let signature: OwlSignature =
        serde_json::from_slice(&fs::read(&signature_path).map_err(|e| e.to_string())?)
            .map_err(|e| e.to_string())?;
    if signature.dataset_id != manifest.dataset_id || signature.snapshot_id != manifest.snapshot_id
    {
        return Err("OWL signature does not belong to snapshot".to_owned());
    }

    let graph_catalog_path = snapshot_root.join("indexes/rdf-dataset-catalog.json");
    verify_manifest_artifact(
        &manifest,
        &graph_catalog_path,
        "indexes/rdf-dataset-catalog.json",
    )?;
    let graph_catalog: GraphCatalog =
        serde_json::from_slice(&fs::read(&graph_catalog_path).map_err(|e| e.to_string())?)
            .map_err(|e| e.to_string())?;
    validate_resolved_dataset(&graph_catalog, &job.resolved_dataset).map_err(|e| e.to_string())?;

    let query_dataset_path = snapshot_root.join("data/query-dataset.nq");
    verify_manifest_artifact(&manifest, &query_dataset_path, "data/query-dataset.nq")?;

    let query_text = fs::read_to_string(&job.query_path).map_err(|e| e.to_string())?;
    let compiled = CompiledSparqlQuery::parse(&query_text).map_err(|e| e.to_string())?;
    let report: DirectBgpLegalityReport =
        serde_json::from_slice(&fs::read(&job.legality_report_path).map_err(|e| e.to_string())?)
            .map_err(|e| e.to_string())?;
    validate_direct_bgp_legality_report(&report).map_err(|e| e.to_string())?;
    if report.dataset_id != manifest.dataset_id
        || report.snapshot_id != manifest.snapshot_id
        || report.query_sha256 != job.query_sha256
        || report.sparql_algebra_sha256 != compiled.canonical_sse_sha256()
        || report.active_dataset_sha256 != job.resolved_dataset.active_dataset_sha256
        || report.authorized_graph_set_sha256 != job.resolved_dataset.authorized_graph_set_sha256
        || report.owl_signature_sha256 != signature_sha
        || report.datatype_policy_sha256 != datatype_sha
        || report.owl_profile_qualification_sha256 != profile_sha
        || report.owl_consistency_qualification_sha256 != consistency_sha
    {
        return Err(
            "Phase 40.7 legality report is not bound to the exact execution snapshot/query/dataset"
                .to_owned(),
        );
    }
    let legality = report
        .bgps
        .iter()
        .find(|record| record.ordinal == job.bgp_ordinal)
        .ok_or_else(|| "requested BGP ordinal is absent from legality report".to_owned())?;
    if legality.status != DirectBgpLegalityStatus::Legal || !legality.grounded_owl2dl_check_required
    {
        return Err("requested BGP was not admitted by Phase 40.7".to_owned());
    }
    match (&legality.graph_scope, job.graph_binding_iri.as_deref()) {
        (DirectBgpScope::Default | DirectBgpScope::Named { .. }, None) => {}
        (DirectBgpScope::NamedVariable { .. }, Some(value)) if absolute_iri(value) => {}
        _ => {
            return Err(
                "graphBindingIri is inconsistent with the Phase 40.7 graph scope".to_owned(),
            );
        }
    }

    let active_bundle = build_direct_active_ontology_bundle(
        &snapshot_root,
        &manifest,
        &signature,
        &graph_catalog,
        &job.resolved_dataset,
        &query_dataset_path,
        &legality.graph_scope,
        job.graph_binding_iri.as_deref(),
        &job.work_dir,
    )
    .map_err(|e| e.to_string())?;

    let bindings = DirectExactBindings {
        dataset_id: manifest.dataset_id,
        snapshot_id: manifest.snapshot_id,
        query_sha256: job.query_sha256.clone(),
        sparql_algebra_sha256: compiled.canonical_sse_sha256().to_owned(),
        active_dataset_sha256: job.resolved_dataset.active_dataset_sha256.clone(),
        authorized_graph_set_sha256: job.resolved_dataset.authorized_graph_set_sha256.clone(),
        owl_signature_sha256: signature_sha,
        datatype_policy_sha256: datatype_sha,
        owl_profile_qualification_sha256: profile_sha,
        owl_consistency_qualification_sha256: consistency_sha,
        graph_context: active_bundle.graph_context.clone(),
    };
    let adapter = DirectExactAdapter {
        java_executable: job.java_executable.clone(),
        adapter_jar: job.reasoner_adapter_jar.clone(),
        adapter_sha256: job.reasoner_adapter_sha256.clone(),
        adapter_version: job.reasoner_adapter_version.clone(),
        reasoner_version: job.reasoner_version.clone(),
    };
    let limits = DirectExactLimits {
        max_candidate_bindings: positive(
            job.limits.max_candidate_bindings,
            "maxCandidateBindings",
        )?,
        max_partition_candidates: positive(
            job.limits.max_partition_candidates,
            "maxPartitionCandidates",
        )?,
        max_exact_partitions: trusted_phase40.max_exact_partitions,
        max_grounded_axioms_per_candidate: positive(
            job.limits.max_grounded_axioms_per_candidate,
            "maxGroundedAxiomsPerCandidate",
        )?,
        max_grounded_rdf_bytes_per_candidate: positive(
            job.limits.max_grounded_rdf_bytes_per_candidate,
            "maxGroundedRdfBytesPerCandidate",
        )?,
        max_local_reasoner_lanes: job.limits.max_local_reasoner_lanes,
        reasoner_heap_mib_per_lane: positive(
            job.limits.reasoner_heap_mib_per_lane,
            "reasonerHeapMiBPerLane",
        )?,
        reasoner_timeout: Duration::from_secs(positive(
            job.limits.reasoner_timeout_seconds,
            "reasonerTimeoutSeconds",
        )?),
        max_certificate_bytes: trusted_phase40.max_certificate_bytes,
        max_proof_support_ids: trusted_phase40.max_proof_support_ids,
    };
    let (result, certificate, proof_manifest) = execute_exact_direct_bgp(
        &compiled,
        legality,
        &bindings,
        &active_bundle,
        &adapter,
        &job.work_dir,
        limits,
    )
    .map_err(|e| e.to_string())?;
    atomic_json(&job.output_result_path, &result)?;
    if u64::try_from(proof_manifest.answer_proofs.len())
        .map_err(|_| "proof-support count overflow".to_owned())?
        .checked_add(1)
        .ok_or_else(|| "proof-support count overflow".to_owned())?
        > trusted_phase40.max_proof_support_ids
    {
        return Err("Phase 40 proof-support output exceeds trusted maxProofSupportIds".to_owned());
    }
    atomic_json(&job.output_proof_manifest_path, &proof_manifest)?;
    atomic_json_bounded(
        &job.output_certificate_path,
        &certificate,
        trusted_phase40.max_certificate_bytes,
        "Direct certificate",
    )?;
    verify_hash(
        &job.output_proof_manifest_path,
        certificate
            .proof_manifest_sha256
            .as_deref()
            .ok_or_else(|| "Phase 40.9 certificate omits proofManifestSha256".to_owned())?,
    )?;
    Ok(serde_json::json!({
        "status":"exact-direct-bgp-complete",
        "candidateBindingCount": result.candidate_binding_count,
        "solutionMultiplicityTotal": result.solution_multiplicity_total,
        "scopedGraphSha256": active_bundle.scoped_graph_sha256,
        "result": job.output_result_path,
        "certificate": job.output_certificate_path,
        "proofManifest": job.output_proof_manifest_path,
        "trustedPhase40CeilingsSha256": trusted_phase40.bundle_sha256()
    })
    .to_string())
}

fn positive(value: u64, name: &str) -> Result<u64, String> {
    if value == 0 {
        Err(format!("{name} must be positive"))
    } else {
        Ok(value)
    }
}

fn verify_manifest_artifact(
    manifest: &ReferenceSnapshotManifest,
    path: &Path,
    relative: &str,
) -> Result<(), String> {
    let artifact = manifest
        .artifacts
        .iter()
        .find(|artifact| artifact.relative_path == relative)
        .ok_or_else(|| format!("snapshot manifest omits {relative}"))?;
    verify_hash(path, &artifact.sha256)?;
    let bytes = fs::metadata(path).map_err(|e| e.to_string())?.len();
    if bytes != artifact.bytes {
        return Err(format!(
            "snapshot artifact byte count mismatch for {relative}"
        ));
    }
    Ok(())
}

fn verify_hash(path: &Path, expected: &str) -> Result<(), String> {
    if expected.len() != 64
        || !expected
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(format!("invalid SHA-256 for {}", path.display()));
    }
    use std::io::Read;
    let mut file = fs::File::open(path).map_err(|e| e.to_string())?;
    let mut digest = Sha256::new();
    let mut buffer = vec![0_u8; 1024 * 1024].into_boxed_slice();
    loop {
        let read = file.read(&mut buffer).map_err(|e| e.to_string())?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    let observed = hex::encode(digest.finalize());
    if observed != expected {
        return Err(format!("SHA-256 mismatch for {}", path.display()));
    }
    Ok(())
}

fn canonical_existing_dir(path: &Path, name: &str) -> Result<PathBuf, String> {
    let canonical = fs::canonicalize(path).map_err(|e| format!("{name}: {e}"))?;
    if !canonical.is_dir() {
        return Err(format!("{name} is not a directory"));
    }
    Ok(canonical)
}

fn require_existing_descendant(root: &Path, path: &Path, name: &str) -> Result<(), String> {
    let canonical = fs::canonicalize(path).map_err(|e| format!("{name}: {e}"))?;
    if !canonical.starts_with(root) || !canonical.is_file() {
        return Err(format!("{name} escapes its allowed root or is not a file"));
    }
    Ok(())
}

fn require_output_descendant(root: &Path, path: &Path, name: &str) -> Result<(), String> {
    if path
        .components()
        .any(|component| matches!(component, Component::ParentDir))
    {
        return Err(format!("{name} contains parent traversal"));
    }
    let parent = path
        .parent()
        .ok_or_else(|| format!("{name} has no parent"))?;
    fs::create_dir_all(parent).map_err(|e| format!("{name}: {e}"))?;
    let canonical_parent = fs::canonicalize(parent).map_err(|e| format!("{name}: {e}"))?;
    if !canonical_parent.starts_with(root) {
        return Err(format!("{name} escapes workRoot"));
    }
    Ok(())
}

fn absolute_iri(value: &str) -> bool {
    value.contains(':') && !value.chars().any(char::is_whitespace)
}

fn atomic_json_bounded<T: serde::Serialize>(
    path: &Path,
    value: &T,
    max_bytes: u64,
    label: &str,
) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    }
    let bytes = serde_json::to_vec_pretty(value).map_err(|e| e.to_string())?;
    let len = u64::try_from(bytes.len()).map_err(|_| format!("{label} byte count overflow"))?;
    if len > max_bytes {
        return Err(format!(
            "{label} bytes {len} exceed trusted Phase 40 ceiling {max_bytes}"
        ));
    }
    let tmp = path.with_extension(format!("tmp-{}", uuid::Uuid::new_v4()));
    fs::write(&tmp, bytes).map_err(|e| e.to_string())?;
    fs::rename(tmp, path).map_err(|e| e.to_string())?;
    Ok(())
}

fn atomic_json<T: serde::Serialize>(path: &Path, value: &T) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    }
    let tmp = path.with_extension(format!("tmp-{}", uuid::Uuid::new_v4()));
    fs::write(
        &tmp,
        serde_json::to_vec_pretty(value).map_err(|e| e.to_string())?,
    )
    .map_err(|e| e.to_string())?;
    fs::rename(tmp, path).map_err(|e| e.to_string())?;
    Ok(())
}
