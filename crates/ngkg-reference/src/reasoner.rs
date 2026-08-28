//! Fail-closed invocation of the version-pinned offline OWL 2 DL adapter.

use std::{
    fs::{self, File},
    path::Path,
    process::{Command, Stdio},
    thread,
    time::{Duration, Instant},
};

use sha2::Digest;
use thiserror::Error;

use crate::{
    datatype_policy::read_policy,
    model::{
        OwlConsistencyQualification, OwlProfileQualification, OwlSignature,
        OwlSignatureOntologyDocument, ReasonerReport, ReasonerRequest, TrustedReasonerConfig,
    },
};

#[derive(Debug, Error)]
pub enum ReasonerInvocationError {
    #[error("reasoner command I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("reasoner process exceeded the configured deadline")]
    Timeout,
    #[error("reasoner process exited unsuccessfully: {0}")]
    ProcessFailure(String),
    #[error("reasoner report is invalid: {0}")]
    InvalidReport(String),
    #[error("reasoner name or version differs from the manifest")]
    VersionMismatch,
    #[error("reasoner report belongs to a different request or snapshot")]
    RequestMismatch,
    #[error("combined ontology is outside OWL 2 DL: {0}")]
    ProfileInvalid(String),
    #[error("ontology is inconsistent under the configured exact policy")]
    Inconsistent,
    #[error("reasoner did not produce its declared closure output")]
    ClosureMissing,
    #[error("reasoner did not produce its declared OWL signature output")]
    OwlSignatureMissing,
    #[error("OWL signature is invalid: {0}")]
    InvalidOwlSignature(String),
    #[error("reasoner did not produce its declared OWL profile/import qualification output")]
    OwlProfileQualificationMissing,
    #[error("OWL profile/import qualification is invalid: {0}")]
    InvalidOwlProfileQualification(String),
    #[error("reasoner did not produce its declared OWL consistency qualification output")]
    OwlConsistencyQualificationMissing,
    #[error("OWL consistency qualification is invalid: {0}")]
    InvalidOwlConsistencyQualification(String),
}

const OWL_SIGNATURE_FORMAT_VERSION: u32 = 1;
const OWL_PROFILE_QUALIFICATION_FORMAT_VERSION: u32 = 1;
const OWL_CONSISTENCY_QUALIFICATION_FORMAT_VERSION: u32 = 1;

fn is_lower_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn require_sorted_unique_iris(
    label: &str,
    values: &[String],
) -> Result<(), ReasonerInvocationError> {
    if values.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(ReasonerInvocationError::InvalidOwlSignature(format!(
            "{label} must be strictly sorted and unique"
        )));
    }
    for value in values {
        oxigraph::model::NamedNode::new(value.clone()).map_err(|error| {
            ReasonerInvocationError::InvalidOwlSignature(format!(
                "{label} contains invalid IRI {value:?}: {error}"
            ))
        })?;
    }
    Ok(())
}

fn expected_signature_documents(request: &ReasonerRequest) -> Vec<OwlSignatureOntologyDocument> {
    let mut documents = request
        .inputs
        .iter()
        .map(|input| {
            let mut ontology_iris = input.ontology_iris.clone();
            ontology_iris.sort_unstable();
            ontology_iris.dedup();
            OwlSignatureOntologyDocument {
                sha256: input.sha256.clone(),
                ontology_iris,
            }
        })
        .collect::<Vec<_>>();
    documents.sort_unstable_by(|left, right| {
        left.sha256
            .cmp(&right.sha256)
            .then_with(|| left.ontology_iris.cmp(&right.ontology_iris))
    });
    documents
}

fn read_and_validate_owl_signature(
    request: &ReasonerRequest,
) -> Result<(OwlSignature, String), ReasonerInvocationError> {
    if !request.output_owl_signature_path.is_file() {
        return Err(ReasonerInvocationError::OwlSignatureMissing);
    }
    let bytes = fs::read(&request.output_owl_signature_path)?;
    let signature: OwlSignature = serde_json::from_slice(&bytes)
        .map_err(|error| ReasonerInvocationError::InvalidOwlSignature(error.to_string()))?;
    if signature.format_version != OWL_SIGNATURE_FORMAT_VERSION
        || signature.dataset_id != request.dataset_id
        || signature.snapshot_id != request.snapshot_id
        || signature.aggregate_input_sha256 != request.aggregate_input_sha256
    {
        return Err(ReasonerInvocationError::InvalidOwlSignature(
            "signature identity differs from reasoner request".to_owned(),
        ));
    }
    if !is_lower_sha256(&signature.aggregate_input_sha256) {
        return Err(ReasonerInvocationError::InvalidOwlSignature(
            "aggregateInputSha256 must be lowercase SHA-256".to_owned(),
        ));
    }
    if signature.ontology_documents != expected_signature_documents(request) {
        return Err(ReasonerInvocationError::InvalidOwlSignature(
            "ontologyDocuments do not exactly match checksum-bound reasoner inputs".to_owned(),
        ));
    }
    for document in &signature.ontology_documents {
        if !is_lower_sha256(&document.sha256) {
            return Err(ReasonerInvocationError::InvalidOwlSignature(
                "ontology document SHA-256 is invalid".to_owned(),
            ));
        }
        require_sorted_unique_iris("ontologyDocuments[].ontologyIris", &document.ontology_iris)?;
    }
    for (label, values) in [
        ("imports", &signature.imports),
        ("classes", &signature.classes),
        ("objectProperties", &signature.object_properties),
        ("dataProperties", &signature.data_properties),
        ("annotationProperties", &signature.annotation_properties),
        ("namedIndividuals", &signature.named_individuals),
        ("datatypes", &signature.datatypes),
    ] {
        require_sorted_unique_iris(label, values)?;
    }
    let digest = sha2::Sha256::digest(&bytes);
    Ok((signature, hex::encode(digest)))
}

fn read_and_validate_owl_profile_qualification(
    request: &ReasonerRequest,
    owl_signature_sha256: &str,
) -> Result<(OwlProfileQualification, String), ReasonerInvocationError> {
    if !request.output_owl_profile_qualification_path.is_file() {
        return Err(ReasonerInvocationError::OwlProfileQualificationMissing);
    }
    let bytes = fs::read(&request.output_owl_profile_qualification_path)?;
    let qualification: OwlProfileQualification =
        serde_json::from_slice(&bytes).map_err(|error| {
            ReasonerInvocationError::InvalidOwlProfileQualification(error.to_string())
        })?;
    if qualification.format_version != OWL_PROFILE_QUALIFICATION_FORMAT_VERSION
        || qualification.dataset_id != request.dataset_id
        || qualification.snapshot_id != request.snapshot_id
        || qualification.aggregate_input_sha256 != request.aggregate_input_sha256
        || qualification.owl_signature_sha256 != owl_signature_sha256
        || qualification.datatype_policy_sha256 != request.datatype_policy_sha256
        || qualification.owl_profile != "OWL 2 DL"
        || !qualification.direct_semantics
    {
        return Err(ReasonerInvocationError::InvalidOwlProfileQualification(
            "qualification identity differs from reasoner request".to_owned(),
        ));
    }
    let input_count = u64::try_from(request.inputs.len()).map_err(|_| {
        ReasonerInvocationError::InvalidOwlProfileQualification("input count overflow".to_owned())
    })?;
    let ontology_count = u64::try_from(
        request
            .inputs
            .iter()
            .filter(|input| !input.ontology_iris.is_empty())
            .count(),
    )
    .map_err(|_| {
        ReasonerInvocationError::InvalidOwlProfileQualification(
            "ontology count overflow".to_owned(),
        )
    })?;
    let abox_count = input_count.checked_sub(ontology_count).ok_or_else(|| {
        ReasonerInvocationError::InvalidOwlProfileQualification("ABox count underflow".to_owned())
    })?;
    if qualification.input_document_count != input_count
        || qualification.ontology_document_count != ontology_count
        || qualification.abox_document_count != abox_count
        || qualification.loaded_ontology_count != qualification.input_document_count
        || qualification.ontology_documents.len()
            != usize::try_from(ontology_count).unwrap_or(usize::MAX)
        || qualification.import_declaration_count != qualification.resolved_import_count
        || qualification.resolved_import_count
            != u64::try_from(qualification.import_resolutions.len()).unwrap_or(u64::MAX)
        || !qualification.complete_local_import_closure
    {
        return Err(ReasonerInvocationError::InvalidOwlProfileQualification(
            "document/import closure counts are inconsistent".to_owned(),
        ));
    }
    let mut previous_doc: Option<(String, String, String)> = None;
    for document in &qualification.ontology_documents {
        if !is_lower_sha256(&document.sha256) {
            return Err(ReasonerInvocationError::InvalidOwlProfileQualification(
                "ontology document SHA-256 is invalid".to_owned(),
            ));
        }
        oxigraph::model::NamedNode::new(document.ontology_iri.clone()).map_err(|error| {
            ReasonerInvocationError::InvalidOwlProfileQualification(format!(
                "ontologyIri is invalid: {error}"
            ))
        })?;
        if let Some(version) = &document.version_iri {
            oxigraph::model::NamedNode::new(version.clone()).map_err(|error| {
                ReasonerInvocationError::InvalidOwlProfileQualification(format!(
                    "versionIri is invalid: {error}"
                ))
            })?;
        }
        let key = (
            document.ontology_iri.clone(),
            document.version_iri.clone().unwrap_or_default(),
            document.sha256.clone(),
        );
        if previous_doc
            .as_ref()
            .is_some_and(|previous| previous >= &key)
        {
            return Err(ReasonerInvocationError::InvalidOwlProfileQualification(
                "ontologyDocuments must be strictly sorted and unique".to_owned(),
            ));
        }
        previous_doc = Some(key);
        let input = request
            .inputs
            .iter()
            .find(|input| input.sha256 == document.sha256)
            .ok_or_else(|| {
                ReasonerInvocationError::InvalidOwlProfileQualification(
                    "qualification references an ontology document outside the request".to_owned(),
                )
            })?;
        let mut expected_aliases = input.ontology_iris.clone();
        expected_aliases.sort_unstable();
        expected_aliases.dedup();
        let mut observed_aliases = vec![document.ontology_iri.clone()];
        if let Some(version) = &document.version_iri {
            observed_aliases.push(version.clone());
        }
        observed_aliases.sort_unstable();
        if observed_aliases != expected_aliases {
            return Err(ReasonerInvocationError::InvalidOwlProfileQualification(
                "OWLAPI ontology/version identity differs from checksum-bound preflight aliases"
                    .to_owned(),
            ));
        }
    }
    let source_ontology_iris = qualification
        .ontology_documents
        .iter()
        .map(|document| document.ontology_iri.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    let mut previous_import: Option<(String, String, String)> = None;
    for resolution in &qualification.import_resolutions {
        if !source_ontology_iris.contains(resolution.source_ontology_iri.as_str()) {
            return Err(ReasonerInvocationError::InvalidOwlProfileQualification(
                "import source is not one of the checksum-bound ontology documents".to_owned(),
            ));
        }
        for iri in [&resolution.source_ontology_iri, &resolution.imported_iri] {
            oxigraph::model::NamedNode::new(iri.as_str()).map_err(|error| {
                ReasonerInvocationError::InvalidOwlProfileQualification(format!(
                    "import IRI is invalid: {error}"
                ))
            })?;
        }
        if !is_lower_sha256(&resolution.resolved_document_sha256) {
            return Err(ReasonerInvocationError::InvalidOwlProfileQualification(
                "resolved import document SHA-256 is invalid".to_owned(),
            ));
        }
        let key = (
            resolution.source_ontology_iri.clone(),
            resolution.imported_iri.clone(),
            resolution.resolved_document_sha256.clone(),
        );
        if previous_import
            .as_ref()
            .is_some_and(|previous| previous >= &key)
        {
            return Err(ReasonerInvocationError::InvalidOwlProfileQualification(
                "importResolutions must be strictly sorted and unique".to_owned(),
            ));
        }
        previous_import = Some(key);
        let target = request.inputs.iter().find(|input| {
            input.sha256 == resolution.resolved_document_sha256
                && input
                    .ontology_iris
                    .iter()
                    .any(|iri| iri == &resolution.imported_iri)
        });
        if target.is_none() {
            return Err(ReasonerInvocationError::InvalidOwlProfileQualification(
                "resolved import is not bound to the declared local ontology document".to_owned(),
            ));
        }
    }
    if qualification.profile_violation_samples.len() > 100
        || qualification
            .profile_violation_samples
            .iter()
            .any(|sample| sample.is_empty() || sample.len() > 4097)
        || (qualification.profile_valid
            && (qualification.profile_violation_count != 0
                || !qualification.profile_violation_samples.is_empty()))
        || (!qualification.profile_valid && qualification.profile_violation_count == 0)
    {
        return Err(ReasonerInvocationError::InvalidOwlProfileQualification(
            "profile evidence is internally inconsistent".to_owned(),
        ));
    }
    let digest = sha2::Sha256::digest(&bytes);
    Ok((qualification, hex::encode(digest)))
}

fn read_and_validate_owl_consistency_qualification(
    command: &TrustedReasonerConfig,
    request: &ReasonerRequest,
    owl_signature_sha256: &str,
    owl_profile_qualification: &OwlProfileQualification,
    owl_profile_qualification_sha256: &str,
) -> Result<(OwlConsistencyQualification, String), ReasonerInvocationError> {
    if !request.output_owl_consistency_qualification_path.is_file() {
        return Err(ReasonerInvocationError::OwlConsistencyQualificationMissing);
    }
    let bytes = fs::read(&request.output_owl_consistency_qualification_path)?;
    let qualification: OwlConsistencyQualification =
        serde_json::from_slice(&bytes).map_err(|error| {
            ReasonerInvocationError::InvalidOwlConsistencyQualification(error.to_string())
        })?;
    let input_count = u64::try_from(request.inputs.len()).map_err(|_| {
        ReasonerInvocationError::InvalidOwlConsistencyQualification(
            "input count overflow".to_owned(),
        )
    })?;
    if qualification.format_version != OWL_CONSISTENCY_QUALIFICATION_FORMAT_VERSION
        || qualification.dataset_id != request.dataset_id
        || qualification.snapshot_id != request.snapshot_id
        || qualification.aggregate_input_sha256 != request.aggregate_input_sha256
        || qualification.owl_signature_sha256 != owl_signature_sha256
        || qualification.datatype_policy_sha256 != request.datatype_policy_sha256
        || qualification.owl_profile_qualification_sha256 != owl_profile_qualification_sha256
        || qualification.owl_profile != "OWL 2 DL"
        || !qualification.direct_semantics
        || qualification.reasoner_name != command.expected_name
        || qualification.reasoner_version != command.expected_version
        || qualification.consistency_method != "OWLReasoner.isConsistent"
        || qualification.input_document_count != input_count
        || qualification.loaded_ontology_count != owl_profile_qualification.loaded_ontology_count
        || qualification.merged_axiom_count != owl_profile_qualification.merged_axiom_count
        || qualification.inconsistent_ontology_handling != "reject_snapshot"
    {
        return Err(ReasonerInvocationError::InvalidOwlConsistencyQualification(
            "consistency qualification identity differs from the exact qualified ontology/reasoner"
                .to_owned(),
        ));
    }
    if qualification.loaded_ontology_count != qualification.input_document_count {
        return Err(ReasonerInvocationError::InvalidOwlConsistencyQualification(
            "consistency check did not cover the complete checksum-bound document set".to_owned(),
        ));
    }
    if !qualification.consistency_checked
        || qualification.publication_permitted != qualification.consistent
    {
        return Err(ReasonerInvocationError::InvalidOwlConsistencyQualification(
            "completed OWL 2 DL consistency evidence must be checked and publication must equal consistency".to_owned(),
        ));
    }
    let digest = sha2::Sha256::digest(&bytes);
    Ok((qualification, hex::encode(digest)))
}

pub fn invoke_reasoner(
    command: &TrustedReasonerConfig,
    request: &ReasonerRequest,
    request_path: &Path,
    stdout_path: &Path,
    stderr_path: &Path,
    timeout_seconds: u64,
) -> Result<ReasonerReport, ReasonerInvocationError> {
    if !command.java_executable.is_file() {
        return Err(ReasonerInvocationError::InvalidReport(
            "javaExecutable is not an existing file".to_owned(),
        ));
    }
    if !command.adapter_jar.path.is_file() {
        return Err(ReasonerInvocationError::InvalidReport(
            "adapterJar is not an existing file".to_owned(),
        ));
    }
    if !request.datatype_policy_path.is_file() {
        return Err(ReasonerInvocationError::InvalidReport(
            "datatypePolicyPath is not an existing file".to_owned(),
        ));
    }
    let (_, observed_datatype_policy_sha256) = read_policy(&request.datatype_policy_path)
        .map_err(|error| ReasonerInvocationError::InvalidReport(error.to_string()))?;
    if observed_datatype_policy_sha256 != request.datatype_policy_sha256 {
        return Err(ReasonerInvocationError::RequestMismatch);
    }
    let request_bytes = serde_json::to_vec_pretty(request)
        .map_err(|error| ReasonerInvocationError::InvalidReport(error.to_string()))?;
    fs::write(request_path, request_bytes)?;
    let mut child = Command::new(&command.java_executable)
        .arg("-jar")
        .arg(&command.adapter_jar.path)
        .arg("--request")
        .arg(request_path)
        .stdin(Stdio::null())
        .stdout(Stdio::from(File::create(stdout_path)?))
        .stderr(Stdio::from(File::create(stderr_path)?))
        .spawn()?;
    let started = Instant::now();
    let status = loop {
        if let Some(status) = child.try_wait()? {
            break status;
        }
        if started.elapsed() >= Duration::from_secs(timeout_seconds) {
            child.kill()?;
            let _ = child.wait();
            return Err(ReasonerInvocationError::Timeout);
        }
        thread::sleep(Duration::from_millis(100));
    };
    if !status.success() {
        if request.output_report_path.is_file() {
            let report_bytes = fs::read(&request.output_report_path)?;
            if let Ok(report) = serde_json::from_slice::<ReasonerReport>(&report_bytes)
                && report.format_version == 5
                && report.dataset_id == request.dataset_id
                && report.snapshot_id == request.snapshot_id
                && report.aggregate_input_sha256 == request.aggregate_input_sha256
                && report.datatype_policy_sha256 == request.datatype_policy_sha256
                && !report.profile_valid
            {
                let (_signature, signature_sha256) = read_and_validate_owl_signature(request)?;
                let (qualification, qualification_sha256) =
                    read_and_validate_owl_profile_qualification(request, &signature_sha256)?;
                if qualification.profile_valid
                    || report.owl_signature_sha256 != signature_sha256
                    || report.owl_profile_qualification_sha256 != qualification_sha256
                    || report.profile_violation_count != qualification.profile_violation_count
                    || report.profile_violation_samples != qualification.profile_violation_samples
                {
                    return Err(ReasonerInvocationError::InvalidReport(
                        "failed profile report is not bound to Phase 40.5 qualification evidence"
                            .to_owned(),
                    ));
                }
                let detail = if report.profile_violation_samples.is_empty() {
                    format!("{} profile violations", report.profile_violation_count)
                } else {
                    format!(
                        "{} profile violations; first: {}",
                        report.profile_violation_count, report.profile_violation_samples[0]
                    )
                };
                return Err(ReasonerInvocationError::ProfileInvalid(detail));
            }
        }
        return Err(ReasonerInvocationError::ProcessFailure(status.to_string()));
    }
    if !request.output_closure_path.is_file() {
        return Err(ReasonerInvocationError::ClosureMissing);
    }
    let (_owl_signature, owl_signature_sha256) = read_and_validate_owl_signature(request)?;
    let (owl_profile_qualification, owl_profile_qualification_sha256) =
        read_and_validate_owl_profile_qualification(request, &owl_signature_sha256)?;
    let (owl_consistency_qualification, owl_consistency_qualification_sha256) =
        read_and_validate_owl_consistency_qualification(
            command,
            request,
            &owl_signature_sha256,
            &owl_profile_qualification,
            &owl_profile_qualification_sha256,
        )?;
    let report_bytes = fs::read(&request.output_report_path)?;
    let report: ReasonerReport = serde_json::from_slice(&report_bytes)
        .map_err(|error| ReasonerInvocationError::InvalidReport(error.to_string()))?;
    if report.reasoner_name != command.expected_name
        || report.reasoner_version != command.expected_version
    {
        return Err(ReasonerInvocationError::VersionMismatch);
    }
    if report.materialization_scope.is_empty()
        || report.owl_profile != "OWL 2 DL"
        || !report.direct_semantics
        || report.profile_violation_samples.len() > 100
        || report
            .profile_violation_samples
            .iter()
            .any(|sample| sample.is_empty() || sample.len() > 4097)
    {
        return Err(ReasonerInvocationError::InvalidReport(
            "reasoner semantic declaration or bounded profile evidence is invalid".to_owned(),
        ));
    }
    if report.format_version != 5
        || report.dataset_id != request.dataset_id
        || report.snapshot_id != request.snapshot_id
        || report.aggregate_input_sha256 != request.aggregate_input_sha256
        || report.owl_signature_sha256 != owl_signature_sha256
        || report.datatype_policy_sha256 != request.datatype_policy_sha256
        || report.owl_profile_qualification_sha256 != owl_profile_qualification_sha256
        || report.owl_consistency_qualification_sha256 != owl_consistency_qualification_sha256
        || report.profile_valid != owl_profile_qualification.profile_valid
        || report.profile_violation_count != owl_profile_qualification.profile_violation_count
        || report.profile_violation_samples != owl_profile_qualification.profile_violation_samples
        || report.named_individual_count > request.max_named_individuals
    {
        return Err(ReasonerInvocationError::RequestMismatch);
    }
    if !report.profile_valid
        || report.profile_violation_count != 0
        || !report.profile_violation_samples.is_empty()
    {
        return Err(ReasonerInvocationError::ProfileInvalid(format!(
            "{} profile violations",
            report.profile_violation_count
        )));
    }
    if report.consistency_checked != owl_consistency_qualification.consistency_checked
        || report.consistent != owl_consistency_qualification.consistent
    {
        return Err(ReasonerInvocationError::RequestMismatch);
    }
    if !report.consistency_checked {
        return Err(ReasonerInvocationError::InvalidReport(
            "reasoner did not check consistency".to_owned(),
        ));
    }
    if !report.consistent {
        return Err(ReasonerInvocationError::Inconsistent);
    }
    Ok(report)
}

#[cfg(test)]
mod phase40_1_tests {
    use super::read_and_validate_owl_signature;
    use crate::model::{ReasonerInputArtifact, ReasonerRequest};
    use std::fs;
    use uuid::Uuid;

    fn request(root: &std::path::Path) -> ReasonerRequest {
        ReasonerRequest {
            format_version: 4,
            dataset_id: Uuid::from_u128(1),
            snapshot_id: Uuid::from_u128(2),
            inputs: vec![ReasonerInputArtifact {
                path: root.join("ontology.ttl"),
                sha256: "11".repeat(32),
                ontology_iris: vec!["https://example.test/ontology".to_owned()],
            }],
            aggregate_input_sha256: "22".repeat(32),
            output_closure_path: root.join("closure.nt"),
            output_report_path: root.join("report.json"),
            output_owl_signature_path: root.join("owl-signature.json"),
            output_owl_profile_qualification_path: root.join("owl-profile-qualification.json"),
            output_owl_consistency_qualification_path: root
                .join("owl-consistency-qualification.json"),
            datatype_policy_path: root.join("datatype-policy.json"),
            datatype_policy_sha256: "33".repeat(32),
            max_named_individuals: 10,
            max_properties: 10,
        }
    }

    #[test]
    fn owl_signature_is_bound_to_request_and_checksum_inputs()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = std::env::temp_dir().join(format!("ngkg-owl-signature-{}", Uuid::new_v4()));
        fs::create_dir_all(&root)?;
        let request = request(&root);
        let value = serde_json::json!({
            "formatVersion": 1,
            "datasetId": request.dataset_id,
            "snapshotId": request.snapshot_id,
            "aggregateInputSha256": request.aggregate_input_sha256,
            "ontologyDocuments": [{"sha256": "11".repeat(32), "ontologyIris": ["https://example.test/ontology"]}],
            "imports": [],
            "classes": ["https://example.test/A"],
            "objectProperties": [],
            "dataProperties": [],
            "annotationProperties": [],
            "namedIndividuals": [],
            "datatypes": []
        });
        fs::write(
            &request.output_owl_signature_path,
            serde_json::to_vec_pretty(&value)?,
        )?;
        let (signature, digest) = read_and_validate_owl_signature(&request)?;
        assert_eq!(signature.classes, vec!["https://example.test/A"]);
        assert_eq!(digest.len(), 64);
        fs::remove_dir_all(root).ok();
        Ok(())
    }

    #[test]
    fn owl_signature_rejects_unsorted_entities() -> Result<(), Box<dyn std::error::Error>> {
        let root = std::env::temp_dir().join(format!("ngkg-owl-signature-{}", Uuid::new_v4()));
        fs::create_dir_all(&root)?;
        let request = request(&root);
        let value = serde_json::json!({
            "formatVersion": 1,
            "datasetId": request.dataset_id,
            "snapshotId": request.snapshot_id,
            "aggregateInputSha256": request.aggregate_input_sha256,
            "ontologyDocuments": [{"sha256": "11".repeat(32), "ontologyIris": ["https://example.test/ontology"]}],
            "imports": [],
            "classes": ["https://example.test/B", "https://example.test/A"],
            "objectProperties": [],
            "dataProperties": [],
            "annotationProperties": [],
            "namedIndividuals": [],
            "datatypes": []
        });
        fs::write(
            &request.output_owl_signature_path,
            serde_json::to_vec_pretty(&value)?,
        )?;
        assert!(read_and_validate_owl_signature(&request).is_err());
        fs::remove_dir_all(root).ok();
        Ok(())
    }
}

#[cfg(test)]
mod phase40_5_tests {
    use super::read_and_validate_owl_profile_qualification;
    use crate::model::{ReasonerInputArtifact, ReasonerRequest};
    use std::fs;
    use uuid::Uuid;

    fn request(root: &std::path::Path) -> ReasonerRequest {
        ReasonerRequest {
            format_version: 4,
            dataset_id: Uuid::from_u128(5),
            snapshot_id: Uuid::from_u128(6),
            inputs: vec![
                ReasonerInputArtifact {
                    path: root.join("child.ttl"),
                    sha256: "44".repeat(32),
                    ontology_iris: vec![
                        "https://example.test/child".to_owned(),
                        "https://example.test/child/2026".to_owned(),
                    ],
                },
                ReasonerInputArtifact {
                    path: root.join("root.ttl"),
                    sha256: "55".repeat(32),
                    ontology_iris: vec!["https://example.test/root".to_owned()],
                },
                ReasonerInputArtifact {
                    path: root.join("core-abox.nt"),
                    sha256: "66".repeat(32),
                    ontology_iris: Vec::new(),
                },
            ],
            aggregate_input_sha256: "11".repeat(32),
            output_closure_path: root.join("closure.nt"),
            output_report_path: root.join("report.json"),
            output_owl_signature_path: root.join("owl-signature.json"),
            output_owl_profile_qualification_path: root.join("owl-profile-qualification.json"),
            output_owl_consistency_qualification_path: root
                .join("owl-consistency-qualification.json"),
            datatype_policy_path: root.join("datatype-policy.json"),
            datatype_policy_sha256: "33".repeat(32),
            max_named_individuals: 10,
            max_properties: 10,
        }
    }

    #[test]
    fn profile_qualification_binds_version_iri_import_to_local_document()
    -> Result<(), Box<dyn std::error::Error>> {
        let root =
            std::env::temp_dir().join(format!("ngkg-profile-qualification-{}", Uuid::new_v4()));
        fs::create_dir_all(&root)?;
        let request = request(&root);
        let value = serde_json::json!({
            "formatVersion": 1,
            "datasetId": request.dataset_id,
            "snapshotId": request.snapshot_id,
            "aggregateInputSha256": request.aggregate_input_sha256,
            "owlSignatureSha256": "22".repeat(32),
            "datatypePolicySha256": request.datatype_policy_sha256,
            "owlProfile": "OWL 2 DL",
            "directSemantics": true,
            "inputDocumentCount": 3,
            "ontologyDocumentCount": 2,
            "aboxDocumentCount": 1,
            "loadedOntologyCount": 3,
            "importDeclarationCount": 1,
            "resolvedImportCount": 1,
            "completeLocalImportClosure": true,
            "mergedAxiomCount": 8,
            "ontologyDocuments": [
                {"sha256": "44".repeat(32), "ontologyIri": "https://example.test/child", "versionIri": "https://example.test/child/2026"},
                {"sha256": "55".repeat(32), "ontologyIri": "https://example.test/root"}
            ],
            "importResolutions": [
                {"sourceOntologyIri": "https://example.test/root", "importedIri": "https://example.test/child/2026", "resolvedDocumentSha256": "44".repeat(32)}
            ],
            "profileValid": true,
            "profileViolationCount": 0,
            "profileViolationSamples": []
        });
        fs::write(
            &request.output_owl_profile_qualification_path,
            serde_json::to_vec_pretty(&value)?,
        )?;
        let (evidence, digest) =
            read_and_validate_owl_profile_qualification(&request, &"22".repeat(32))?;
        assert!(evidence.complete_local_import_closure);
        assert_eq!(evidence.resolved_import_count, 1);
        assert_eq!(digest.len(), 64);
        fs::remove_dir_all(root).ok();
        Ok(())
    }

    #[test]
    fn profile_qualification_rejects_wrong_import_document_hash()
    -> Result<(), Box<dyn std::error::Error>> {
        let root =
            std::env::temp_dir().join(format!("ngkg-profile-qualification-{}", Uuid::new_v4()));
        fs::create_dir_all(&root)?;
        let request = request(&root);
        let value = serde_json::json!({
            "formatVersion": 1,
            "datasetId": request.dataset_id,
            "snapshotId": request.snapshot_id,
            "aggregateInputSha256": request.aggregate_input_sha256,
            "owlSignatureSha256": "22".repeat(32),
            "datatypePolicySha256": request.datatype_policy_sha256,
            "owlProfile": "OWL 2 DL",
            "directSemantics": true,
            "inputDocumentCount": 3,
            "ontologyDocumentCount": 2,
            "aboxDocumentCount": 1,
            "loadedOntologyCount": 3,
            "importDeclarationCount": 1,
            "resolvedImportCount": 1,
            "completeLocalImportClosure": true,
            "mergedAxiomCount": 8,
            "ontologyDocuments": [
                {"sha256": "44".repeat(32), "ontologyIri": "https://example.test/child", "versionIri": "https://example.test/child/2026"},
                {"sha256": "55".repeat(32), "ontologyIri": "https://example.test/root"}
            ],
            "importResolutions": [
                {"sourceOntologyIri": "https://example.test/root", "importedIri": "https://example.test/child/2026", "resolvedDocumentSha256": "77".repeat(32)}
            ],
            "profileValid": true,
            "profileViolationCount": 0,
            "profileViolationSamples": []
        });
        fs::write(
            &request.output_owl_profile_qualification_path,
            serde_json::to_vec_pretty(&value)?,
        )?;
        assert!(read_and_validate_owl_profile_qualification(&request, &"22".repeat(32)).is_err());
        fs::remove_dir_all(root).ok();
        Ok(())
    }
}

#[cfg(test)]
mod phase40_6_tests {
    use super::read_and_validate_owl_consistency_qualification;
    use crate::model::{
        InputArtifact, OwlProfileQualification, ReasonerInputArtifact, ReasonerRequest,
        TrustedReasonerConfig,
    };
    use std::fs;
    use uuid::Uuid;

    fn request(root: &std::path::Path) -> ReasonerRequest {
        ReasonerRequest {
            format_version: 4,
            dataset_id: Uuid::from_u128(7),
            snapshot_id: Uuid::from_u128(8),
            inputs: vec![ReasonerInputArtifact {
                path: root.join("ontology.ttl"),
                sha256: "11".repeat(32),
                ontology_iris: vec!["https://example.test/ontology".to_owned()],
            }],
            aggregate_input_sha256: "22".repeat(32),
            output_closure_path: root.join("closure.nt"),
            output_report_path: root.join("report.json"),
            output_owl_signature_path: root.join("owl-signature.json"),
            output_owl_profile_qualification_path: root.join("owl-profile-qualification.json"),
            output_owl_consistency_qualification_path: root
                .join("owl-consistency-qualification.json"),
            datatype_policy_path: root.join("datatype-policy.json"),
            datatype_policy_sha256: "33".repeat(32),
            max_named_individuals: 10,
            max_properties: 10,
        }
    }

    fn profile(request: &ReasonerRequest) -> OwlProfileQualification {
        OwlProfileQualification {
            format_version: 1,
            dataset_id: request.dataset_id,
            snapshot_id: request.snapshot_id,
            aggregate_input_sha256: request.aggregate_input_sha256.clone(),
            owl_signature_sha256: "44".repeat(32),
            datatype_policy_sha256: request.datatype_policy_sha256.clone(),
            owl_profile: "OWL 2 DL".to_owned(),
            direct_semantics: true,
            input_document_count: 1,
            ontology_document_count: 1,
            abox_document_count: 0,
            loaded_ontology_count: 1,
            import_declaration_count: 0,
            resolved_import_count: 0,
            complete_local_import_closure: true,
            merged_axiom_count: 9,
            ontology_documents: Vec::new(),
            import_resolutions: Vec::new(),
            profile_valid: true,
            profile_violation_count: 0,
            profile_violation_samples: Vec::new(),
        }
    }

    fn command(root: &std::path::Path) -> TrustedReasonerConfig {
        TrustedReasonerConfig {
            java_executable: root.join("java"),
            adapter_jar: InputArtifact {
                path: root.join("adapter.jar"),
                sha256: "55".repeat(32),
            },
            expected_name: "HermiT".to_owned(),
            expected_version: "1.4.5.519".to_owned(),
        }
    }

    #[test]
    fn consistency_qualification_binds_global_reasoner_decision()
    -> Result<(), Box<dyn std::error::Error>> {
        let root =
            std::env::temp_dir().join(format!("ngkg-consistency-qualification-{}", Uuid::new_v4()));
        fs::create_dir_all(&root)?;
        let request = request(&root);
        let profile = profile(&request);
        let value = serde_json::json!({
            "formatVersion": 1,
            "datasetId": request.dataset_id,
            "snapshotId": request.snapshot_id,
            "aggregateInputSha256": request.aggregate_input_sha256,
            "owlSignatureSha256": "44".repeat(32),
            "datatypePolicySha256": request.datatype_policy_sha256,
            "owlProfileQualificationSha256": "66".repeat(32),
            "owlProfile": "OWL 2 DL",
            "directSemantics": true,
            "reasonerName": "HermiT",
            "reasonerVersion": "1.4.5.519",
            "consistencyMethod": "OWLReasoner.isConsistent",
            "inputDocumentCount": 1,
            "loadedOntologyCount": 1,
            "mergedAxiomCount": 9,
            "consistencyChecked": true,
            "consistent": true,
            "publicationPermitted": true,
            "inconsistentOntologyHandling": "reject_snapshot"
        });
        fs::write(
            &request.output_owl_consistency_qualification_path,
            serde_json::to_vec_pretty(&value)?,
        )?;
        let (evidence, digest) = read_and_validate_owl_consistency_qualification(
            &command(&root),
            &request,
            &"44".repeat(32),
            &profile,
            &"66".repeat(32),
        )?;
        assert!(evidence.consistent && evidence.publication_permitted);
        assert_eq!(digest.len(), 64);
        fs::remove_dir_all(root).ok();
        Ok(())
    }

    #[test]
    fn consistency_qualification_rejects_publishable_inconsistency()
    -> Result<(), Box<dyn std::error::Error>> {
        let root =
            std::env::temp_dir().join(format!("ngkg-consistency-qualification-{}", Uuid::new_v4()));
        fs::create_dir_all(&root)?;
        let request = request(&root);
        let profile = profile(&request);
        let value = serde_json::json!({
            "formatVersion": 1,
            "datasetId": request.dataset_id,
            "snapshotId": request.snapshot_id,
            "aggregateInputSha256": request.aggregate_input_sha256,
            "owlSignatureSha256": "44".repeat(32),
            "datatypePolicySha256": request.datatype_policy_sha256,
            "owlProfileQualificationSha256": "66".repeat(32),
            "owlProfile": "OWL 2 DL",
            "directSemantics": true,
            "reasonerName": "HermiT",
            "reasonerVersion": "1.4.5.519",
            "consistencyMethod": "OWLReasoner.isConsistent",
            "inputDocumentCount": 1,
            "loadedOntologyCount": 1,
            "mergedAxiomCount": 9,
            "consistencyChecked": true,
            "consistent": false,
            "publicationPermitted": true,
            "inconsistentOntologyHandling": "reject_snapshot"
        });
        fs::write(
            &request.output_owl_consistency_qualification_path,
            serde_json::to_vec_pretty(&value)?,
        )?;
        assert!(
            read_and_validate_owl_consistency_qualification(
                &command(&root),
                &request,
                &"44".repeat(32),
                &profile,
                &"66".repeat(32)
            )
            .is_err()
        );
        fs::remove_dir_all(root).ok();
        Ok(())
    }
}
