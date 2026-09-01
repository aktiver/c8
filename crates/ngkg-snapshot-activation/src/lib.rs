//! Fail-closed atomic activation contracts for cloud-compiled RDF snapshots.
//!
//! Phase 40.13.15 binds the complete semantic compiler, OWL 2 DL
//! qualification, and exact offline-reasoning roots into one immutable serving
//! identity. It performs no ontology alignment and no raw-data mapping.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, BufWriter, Write},
    path::{Path, PathBuf},
};

use ngkg_dataset::{GraphCatalog, GraphRecord, LogicalGraphName, SOURCE_DEFAULT_GRAPH_ROLE};
use ngkg_offline_reasoner::OfflineReasoningRoot;
use ngkg_ontology_qualifier::{OntologyQualificationRequest, OntologyQualificationRoot};
use ngkg_reference::{
    ArtifactRecord, GraphCapabilityIndexFile, GraphCapabilityRecord, ReferenceSnapshotManifest,
};
use ngkg_semantic_compiler::{GraphRole, SemanticCompilationRoot};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use uuid::Uuid;

/// Activation manifest contract version.
pub const SNAPSHOT_ACTIVATION_FORMAT_VERSION: u32 = 1;

/// Exact immutable object-store reference.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ActivationRootReference {
    pub object_key: String,
    pub sha256: String,
}

/// One activation input already checksum-verified by the object-store worker.
pub struct ActivationInputs<'a> {
    pub semantic_root: &'a SemanticCompilationRoot,
    pub semantic_root_ref: ActivationRootReference,
    pub qualification_request: &'a OntologyQualificationRequest,
    pub qualification_request_ref: ActivationRootReference,
    pub qualification_root: &'a OntologyQualificationRoot,
    pub qualification_root_ref: ActivationRootReference,
    pub offline_root: &'a OfflineReasoningRoot,
    pub offline_root_ref: ActivationRootReference,
    pub identity_namespace: Uuid,
    pub parent_snapshot_id: Option<Uuid>,
}

/// Checksum-bound publication barrier consumed by PostgreSQL and online serving.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct SnapshotActivationManifest {
    pub format_version: u32,
    pub tenant_id: Uuid,
    pub dataset_id: Uuid,
    pub operation_id: Uuid,
    pub snapshot_id: Uuid,
    pub parent_snapshot_id: Option<Uuid>,
    pub identity_namespace: Uuid,
    pub semantic_root: ActivationRootReference,
    pub qualification_request: ActivationRootReference,
    pub qualification_root: ActivationRootReference,
    pub offline_reasoning_root: ActivationRootReference,
    pub semantic_content_sha256: String,
    pub authorized_graph_set_sha256: String,
    pub datatype_policy_sha256: String,
    pub synthetic_snapshot_ontology_sha256: String,
    pub finite_closure_sha256: String,
    pub proof_support_root_sha256: String,
    pub fact_count: u64,
    pub consequence_count: u64,
    pub semantic_partition_count: u32,
    pub reasoning_partition_count: u32,
    pub reference_manifest_sha256: String,
    pub query_dataset_sha256: String,
    pub query_dataset_bytes: u64,
    pub graph_catalog_sha256: String,
    pub capability_index_sha256: String,
    pub exact_reasoner: String,
    pub exact_reasoner_version: String,
    pub unknown_routes_to_exact_hermit: bool,
    pub all_partitions_verified: bool,
    pub publication_state: String,
}

/// Locally assembled compatibility artifacts used by the scalar correctness path.
pub struct ServingArtifacts {
    pub reference_manifest_path: PathBuf,
    pub activation_manifest_path: PathBuf,
}

#[derive(Debug, Error)]
pub enum ActivationError {
    #[error("snapshot activation I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("snapshot activation JSON failed: {0}")]
    Json(#[from] serde_json::Error),
    #[error("snapshot activation contract failed: {0}")]
    Contract(String),
    #[error("snapshot graph catalog failed: {0}")]
    Dataset(#[from] ngkg_dataset::DatasetError),
}

/// Validate every transitive identity and completeness edge before creating bytes.
pub fn validate_inputs(inputs: &ActivationInputs<'_>) -> Result<(), ActivationError> {
    let semantic = inputs.semantic_root;
    let request = inputs.qualification_request;
    let qualification = inputs.qualification_root;
    let offline = inputs.offline_root;
    for reference in [
        &inputs.semantic_root_ref,
        &inputs.qualification_request_ref,
        &inputs.qualification_root_ref,
        &inputs.offline_root_ref,
    ] {
        validate_object_key(&reference.object_key)?;
        require_sha256(&reference.sha256)?;
    }
    for digest in [
        semantic.semantic_content_sha256.as_str(),
        request.authorized_graph_set_sha256.as_str(),
        request.datatype_policy_sha256.as_str(),
        qualification.synthetic_snapshot_ontology_sha256.as_str(),
        qualification.finite_closure_sha256.as_str(),
        offline.proof_support_root_sha256.as_str(),
    ] {
        require_sha256(digest)?;
    }
    if semantic.tenant_id != request.tenant_id
        || semantic.dataset_id != request.dataset_id
        || semantic.operation_id != request.operation_id
        || semantic.snapshot_id != request.snapshot_id
        || qualification.tenant_id != request.tenant_id
        || qualification.dataset_id != request.dataset_id
        || qualification.operation_id != request.operation_id
        || qualification.snapshot_id != request.snapshot_id
        || offline.tenant_id != request.tenant_id
        || offline.dataset_id != request.dataset_id
        || offline.operation_id != request.operation_id
        || offline.snapshot_id != request.snapshot_id
        || request.semantic_compilation_root_sha256 != inputs.semantic_root_ref.sha256
        || qualification.semantic_compilation_root_sha256 != inputs.semantic_root_ref.sha256
        || qualification.qualification_request_sha256 != inputs.qualification_request_ref.sha256
        || offline.ontology_qualification_root_sha256 != inputs.qualification_root_ref.sha256
        || offline.finite_closure_sha256 != qualification.finite_closure_sha256
    {
        return Err(ActivationError::Contract(
            "semantic, qualification, and reasoning roots do not form one snapshot".to_owned(),
        ));
    }
    if semantic.publication_state != "inactive"
        || qualification.publication_state != "inactive"
        || offline.publication_state != "inactive"
        || semantic.logical_partitions == 0
        || semantic.partitions.len() != semantic.logical_partitions as usize
        || offline.logical_partitions == 0
        || offline.partitions.len() != offline.logical_partitions as usize
        || !qualification.profile_valid
        || !qualification.consistency_checked
        || !qualification.consistent
        || qualification.reasoner_name != "HermiT"
        || qualification.reasoner_version != "1.4.5.519"
        || offline.reasoner_name != "HermiT"
        || offline.reasoner_version != "1.4.5.519"
        || offline.arbitrary_owl2_dl_complete
        || !offline.unknown_routes_to_exact_hermit
    {
        return Err(ActivationError::Contract(
            "snapshot is incomplete, active, inconsistent, or not exact-HermiT qualified"
                .to_owned(),
        ));
    }
    let semantic_indexes = semantic
        .partitions
        .iter()
        .map(|partition| partition.partition_index)
        .collect::<BTreeSet<_>>();
    let reasoning_indexes = offline
        .partitions
        .iter()
        .map(|partition| partition.partition_index)
        .collect::<BTreeSet<_>>();
    if semantic_indexes != (0..semantic.logical_partitions).collect()
        || reasoning_indexes != (0..offline.logical_partitions).collect()
    {
        return Err(ActivationError::Contract(
            "partition barrier has a duplicate or missing completion".to_owned(),
        ));
    }
    for graph in &request.authorized_asserted_graphs {
        if !graph
            .graph_iri
            .starts_with("https://c8-next-generation.io/")
            || !graph.graph_iri.ends_with("/semkg")
            || graph.graph_iri.contains("/alignment")
            || graph.graph_iri.contains("/closure")
            || graph.graph_iri.contains("/provenance")
            || graph.authorization_labels.is_empty()
        {
            return Err(ActivationError::Contract(
                "only explicitly authorized asserted */semkg graphs may activate".to_owned(),
            ));
        }
    }
    Ok(())
}

/// Assemble the scalar serving image using bounded, ordered file streaming.
///
/// Logical partitions were produced in parallel across the cluster. This final
/// compatibility image is topology-independent and never affects the Phase 16
/// distributed storage layout.
pub fn build_serving_artifacts(
    inputs: &ActivationInputs<'_>,
    semantic_fact_partitions: &[PathBuf],
    finite_closure_path: &Path,
    owl_signature_path: &Path,
    datatype_policy_path: &Path,
    owl_profile_path: &Path,
    owl_consistency_path: &Path,
    output_root: &Path,
) -> Result<ServingArtifacts, ActivationError> {
    validate_inputs(inputs)?;
    if semantic_fact_partitions.len() != inputs.semantic_root.logical_partitions as usize {
        return Err(ActivationError::Contract(
            "query dataset partition barrier is incomplete".to_owned(),
        ));
    }
    create_new_root(output_root)?;
    for relative in ["data", "reasoner", "indexes", "activation"] {
        fs::create_dir(output_root.join(relative))?;
    }
    let query_dataset = output_root.join("data/query-dataset.nq");
    concatenate_lines(semantic_fact_partitions, &query_dataset)?;
    copy_new(
        finite_closure_path,
        &output_root.join("reasoner/closure.nt"),
    )?;
    copy_new(
        owl_signature_path,
        &output_root.join("reasoner/owl-signature.json"),
    )?;
    copy_new(
        datatype_policy_path,
        &output_root.join("reasoner/datatype-policy.json"),
    )?;
    copy_new(
        owl_profile_path,
        &output_root.join("reasoner/owl-profile-qualification.json"),
    )?;
    copy_new(
        owl_consistency_path,
        &output_root.join("reasoner/owl-consistency-qualification.json"),
    )?;

    let graph_catalog = build_graph_catalog(inputs)?;
    let graph_catalog_path = output_root.join("indexes/rdf-dataset-catalog.json");
    write_json_new(&graph_catalog_path, &graph_catalog)?;
    let graph_catalog_sha256 = sha256_path(&graph_catalog_path)?;
    let capabilities = build_capability_index(inputs, &graph_catalog, &graph_catalog_sha256)?;
    let capability_path = output_root.join("indexes/graph-capabilities.json");
    write_json_new(&capability_path, &capabilities)?;

    let artifact_names = [
        "data/query-dataset.nq",
        "reasoner/closure.nt",
        "reasoner/owl-signature.json",
        "reasoner/datatype-policy.json",
        "reasoner/owl-profile-qualification.json",
        "reasoner/owl-consistency-qualification.json",
        "indexes/rdf-dataset-catalog.json",
        "indexes/graph-capabilities.json",
    ];
    let artifacts = artifact_names
        .iter()
        .map(|relative| artifact(output_root, relative))
        .collect::<Result<Vec<_>, _>>()?;
    let reference_manifest = ReferenceSnapshotManifest {
        format_version: 1,
        dataset_id: inputs.semantic_root.dataset_id,
        snapshot_id: inputs.semantic_root.snapshot_id,
        parent_snapshot_id: inputs.parent_snapshot_id,
        dataset_namespace: inputs.identity_namespace,
        source_sha256: inputs.semantic_root.aggregate_source_sha256.clone(),
        ontology_bundle_sha256: inputs
            .qualification_root
            .synthetic_snapshot_ontology_sha256
            .clone(),
        projection_policy_sha256: inputs
            .qualification_request
            .authorization_policy_sha256
            .clone(),
        dictionary_root_sha256: inputs.semantic_root.dictionary_manifest_sha256.clone(),
        artifacts,
        certified_queries: Vec::new(),
        reasoner_name: "HermiT".to_owned(),
        reasoner_version: "1.4.5.519".to_owned(),
        owl_signature_sha256: Some(inputs.qualification_root.owl_signature_sha256.clone()),
        datatype_policy_sha256: Some(inputs.qualification_root.datatype_policy_sha256.clone()),
        owl_profile_qualification_sha256: Some(
            inputs
                .qualification_root
                .owl_profile_qualification_sha256
                .clone(),
        ),
        owl_consistency_qualification_sha256: Some(
            inputs
                .qualification_root
                .owl_consistency_qualification_sha256
                .clone(),
        ),
        closure_graph_iri: format!(
            "https://c8-next-generation.io/system/{}/closure",
            inputs.semantic_root.dataset_id
        ),
        reasoning_scope: "owl2-direct-semantics-exact-hermit-with-certified-finite-closure"
            .to_owned(),
        publication: "certified-inactive".to_owned(),
    };
    let reference_manifest_path = output_root.join("snapshot-manifest.json");
    write_json_new(&reference_manifest_path, &reference_manifest)?;

    let activation = SnapshotActivationManifest {
        format_version: SNAPSHOT_ACTIVATION_FORMAT_VERSION,
        tenant_id: inputs.semantic_root.tenant_id,
        dataset_id: inputs.semantic_root.dataset_id,
        operation_id: inputs.semantic_root.operation_id,
        snapshot_id: inputs.semantic_root.snapshot_id,
        parent_snapshot_id: inputs.parent_snapshot_id,
        identity_namespace: inputs.identity_namespace,
        semantic_root: inputs.semantic_root_ref.clone(),
        qualification_request: inputs.qualification_request_ref.clone(),
        qualification_root: inputs.qualification_root_ref.clone(),
        offline_reasoning_root: inputs.offline_root_ref.clone(),
        semantic_content_sha256: inputs.semantic_root.semantic_content_sha256.clone(),
        authorized_graph_set_sha256: inputs
            .qualification_root
            .authorized_graph_set_sha256
            .clone(),
        datatype_policy_sha256: inputs.qualification_root.datatype_policy_sha256.clone(),
        synthetic_snapshot_ontology_sha256: inputs
            .qualification_root
            .synthetic_snapshot_ontology_sha256
            .clone(),
        finite_closure_sha256: inputs.qualification_root.finite_closure_sha256.clone(),
        proof_support_root_sha256: inputs.offline_root.proof_support_root_sha256.clone(),
        fact_count: inputs.semantic_root.fact_count,
        consequence_count: inputs.offline_root.consequence_count,
        semantic_partition_count: inputs.semantic_root.logical_partitions,
        reasoning_partition_count: inputs.offline_root.logical_partitions,
        reference_manifest_sha256: sha256_path(&reference_manifest_path)?,
        query_dataset_sha256: sha256_path(&query_dataset)?,
        query_dataset_bytes: fs::metadata(&query_dataset)?.len(),
        graph_catalog_sha256,
        capability_index_sha256: sha256_path(&capability_path)?,
        exact_reasoner: "HermiT".to_owned(),
        exact_reasoner_version: "1.4.5.519".to_owned(),
        unknown_routes_to_exact_hermit: true,
        all_partitions_verified: true,
        publication_state: "certified-inactive".to_owned(),
    };
    validate_activation_manifest(&activation)?;
    let activation_manifest_path = output_root.join("activation/snapshot-activation.json");
    write_json_new(&activation_manifest_path, &activation)?;
    sync_tree(output_root)?;
    Ok(ServingArtifacts {
        reference_manifest_path,
        activation_manifest_path,
    })
}

pub fn validate_activation_manifest(
    manifest: &SnapshotActivationManifest,
) -> Result<(), ActivationError> {
    if manifest.format_version != SNAPSHOT_ACTIVATION_FORMAT_VERSION
        || manifest.tenant_id.is_nil()
        || manifest.dataset_id.is_nil()
        || manifest.operation_id.is_nil()
        || manifest.snapshot_id.is_nil()
        || manifest.identity_namespace.is_nil()
        || !manifest.all_partitions_verified
        || !manifest.unknown_routes_to_exact_hermit
        || manifest.semantic_partition_count == 0
        || manifest.reasoning_partition_count == 0
        || manifest.exact_reasoner != "HermiT"
        || manifest.exact_reasoner_version != "1.4.5.519"
        || manifest.publication_state != "certified-inactive"
    {
        return Err(ActivationError::Contract(
            "activation manifest is incomplete or not publication-safe".to_owned(),
        ));
    }
    for digest in [
        manifest.semantic_content_sha256.as_str(),
        manifest.authorized_graph_set_sha256.as_str(),
        manifest.datatype_policy_sha256.as_str(),
        manifest.synthetic_snapshot_ontology_sha256.as_str(),
        manifest.finite_closure_sha256.as_str(),
        manifest.proof_support_root_sha256.as_str(),
        manifest.reference_manifest_sha256.as_str(),
        manifest.query_dataset_sha256.as_str(),
        manifest.graph_catalog_sha256.as_str(),
        manifest.capability_index_sha256.as_str(),
    ] {
        require_sha256(digest)?;
    }
    Ok(())
}

fn build_graph_catalog(inputs: &ActivationInputs<'_>) -> Result<GraphCatalog, ActivationError> {
    let counts = inputs
        .semantic_root
        .graph_inventory
        .iter()
        .filter(|graph| graph.role == GraphRole::AssertedOntologyCandidate)
        .map(|graph| (strip_graph_term(&graph.graph_term), graph.quad_count))
        .collect::<BTreeMap<_, _>>();
    let mut graphs = vec![GraphRecord {
        graph_id: 0,
        name: LogicalGraphName::Default,
        role: SOURCE_DEFAULT_GRAPH_ROLE.to_owned(),
        authorization_labels: BTreeSet::new(),
        query_visible: false,
        reasoning_visible: false,
        asserted_quad_count: 0,
    }];
    let mut authorized = inputs
        .qualification_request
        .authorized_asserted_graphs
        .clone();
    authorized.sort_by(|left, right| left.graph_iri.cmp(&right.graph_iri));
    for (ordinal, graph) in authorized.into_iter().enumerate() {
        graphs.push(GraphRecord {
            graph_id: u32::try_from(ordinal + 1)
                .map_err(|_| ActivationError::Contract("graph ID overflow".to_owned()))?,
            name: LogicalGraphName::Named {
                iri: graph.graph_iri.clone(),
            },
            role: "semkg".to_owned(),
            authorization_labels: graph.authorization_labels.into_iter().collect(),
            query_visible: true,
            reasoning_visible: true,
            asserted_quad_count: counts.get(&graph.graph_iri).copied().unwrap_or(0),
        });
    }
    let catalog = GraphCatalog {
        format_version: 1,
        dataset_id: inputs.semantic_root.dataset_id,
        snapshot_id: inputs.semantic_root.snapshot_id,
        graphs,
    };
    catalog.validate()?;
    Ok(catalog)
}

fn build_capability_index(
    inputs: &ActivationInputs<'_>,
    catalog: &GraphCatalog,
    graph_catalog_sha256: &str,
) -> Result<GraphCapabilityIndexFile, ActivationError> {
    let mut graphs = Vec::new();
    let mut dependencies = BTreeMap::new();
    for graph in &catalog.graphs {
        if let LogicalGraphName::Named { iri } = &graph.name {
            graphs.push(GraphCapabilityRecord {
                graph_id: graph.graph_id,
                graph_iri: iri.clone(),
                role: graph.role.clone(),
                authorization_labels: graph.authorization_labels.clone(),
                reasoning_visible: graph.reasoning_visible,
                queryable_fact_count: graph.asserted_quad_count,
            });
            dependencies.insert(iri.clone(), Vec::new());
        }
    }
    Ok(GraphCapabilityIndexFile {
        format_version: 2,
        dataset_id: inputs.semantic_root.dataset_id,
        snapshot_id: inputs.semantic_root.snapshot_id,
        graph_catalog_sha256: graph_catalog_sha256.to_owned(),
        graphs,
        predicate_to_graphs: BTreeMap::new(),
        class_to_graphs: BTreeMap::new(),
        dependencies,
    })
}

fn concatenate_lines(inputs: &[PathBuf], output: &Path) -> Result<(), ActivationError> {
    let file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(output)?;
    let mut writer = BufWriter::new(file);
    for input in inputs {
        let mut reader = BufReader::new(File::open(input)?);
        let mut line = Vec::new();
        while reader.read_until(b'\n', &mut line)? != 0 {
            writer.write_all(&line)?;
            if !line.ends_with(b"\n") {
                writer.write_all(b"\n")?;
            }
            line.clear();
        }
    }
    writer.flush()?;
    writer.get_ref().sync_all()?;
    Ok(())
}

fn copy_new(source: &Path, target: &Path) -> Result<(), ActivationError> {
    let mut reader = BufReader::new(File::open(source)?);
    let file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(target)?;
    let mut writer = BufWriter::new(file);
    std::io::copy(&mut reader, &mut writer)?;
    writer.flush()?;
    writer.get_ref().sync_all()?;
    Ok(())
}

fn artifact(root: &Path, relative: &str) -> Result<ArtifactRecord, ActivationError> {
    let path = root.join(relative);
    Ok(ArtifactRecord {
        relative_path: relative.to_owned(),
        sha256: sha256_path(&path)?,
        bytes: fs::metadata(path)?.len(),
    })
}

fn create_new_root(path: &Path) -> Result<(), ActivationError> {
    if path.exists() {
        return Err(ActivationError::Contract(
            "activation output already exists".to_owned(),
        ));
    }
    fs::create_dir_all(path)?;
    Ok(())
}

fn write_json_new<T: Serialize>(path: &Path, value: &T) -> Result<(), ActivationError> {
    let file = OpenOptions::new().create_new(true).write(true).open(path)?;
    let mut writer = BufWriter::new(file);
    serde_json::to_writer_pretty(&mut writer, value)?;
    writer.write_all(b"\n")?;
    writer.flush()?;
    writer.get_ref().sync_all()?;
    Ok(())
}

fn sync_tree(root: &Path) -> Result<(), ActivationError> {
    for relative in ["data", "reasoner", "indexes", "activation"] {
        File::open(root.join(relative))?.sync_all()?;
    }
    File::open(root)?.sync_all()?;
    Ok(())
}

fn sha256_path(path: &Path) -> Result<String, ActivationError> {
    let mut reader = BufReader::new(File::open(path)?);
    let mut digest = Sha256::new();
    std::io::copy(&mut reader, &mut digest)?;
    Ok(hex::encode(digest.finalize()))
}

fn strip_graph_term(value: &str) -> String {
    value
        .strip_prefix('<')
        .and_then(|inner| inner.strip_suffix('>'))
        .unwrap_or(value)
        .to_owned()
}

fn require_sha256(value: &str) -> Result<(), ActivationError> {
    if value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        Ok(())
    } else {
        Err(ActivationError::Contract("invalid SHA-256".to_owned()))
    }
}

fn validate_object_key(value: &str) -> Result<(), ActivationError> {
    if value.is_empty()
        || value.len() > 1024
        || value.starts_with('/')
        || value.split('/').any(|segment| {
            segment.is_empty()
                || segment == "."
                || segment == ".."
                || segment.len() > 255
                || !segment
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
        })
    {
        return Err(ActivationError::Contract(
            "unsafe activation object key".to_owned(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_alignment_graph_activation() {
        assert!(validate_object_key("imports/a/b/root.json").is_ok());
        assert!(validate_object_key("../root.json").is_err());
        assert!(require_sha256(&"a".repeat(64)).is_ok());
        assert!(require_sha256(&"A".repeat(64)).is_err());
    }
}
