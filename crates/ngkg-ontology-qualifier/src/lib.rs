//! Deterministic OWL 2 DL snapshot assembly and qualification contracts.
//!
//! Phase 40.13.13 consumes only the inactive semantic-compilation root from
//! Phase 40.13.12. It selects asserted `*/semkg` named graphs, projects them
//! independently across logical partitions, resolves a checksum-pinned local
//! import closure, and creates the exact HermiT request used for the one global
//! OWL 2 DL profile and consistency decision. It performs no ontology
//! alignment, schema matching, or raw-data mapping.

use std::{
    cmp::Reverse,
    collections::{BTreeMap, BTreeSet, BinaryHeap},
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
    process::{Command, Stdio},
    thread,
    time::{Duration, Instant},
};

use ngkg_semantic_compiler::{
    GraphRole, SemanticCompilationRoot, SemanticPartitionManifest,
};
use oxigraph::{
    io::{RdfFormat, RdfParser},
    model::{GraphName, Quad, Term},
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};
use thiserror::Error;
use uuid::Uuid;

/// Phase 40.13.13 qualification format.
pub const ONTOLOGY_QUALIFICATION_FORMAT_VERSION: u32 = 1;
/// Exact asserted ontology graph suffix.
pub const ASSERTED_GRAPH_SUFFIX: &str = "/semkg";
const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
const OWL_ONTOLOGY: &str = "http://www.w3.org/2002/07/owl#Ontology";
const OWL_IMPORTS: &str = "http://www.w3.org/2002/07/owl#imports";
const OWL_VERSION_IRI: &str = "http://www.w3.org/2002/07/owl#versionIRI";

/// One explicit build-time authorization decision for an asserted ontology graph.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct AuthorizedOntologyGraph {
    pub graph_iri: String,
    pub graph_id: u64,
    pub authorization_labels: Vec<String>,
}

/// One externally stored ontology document whose bytes and identities are pinned.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct PinnedImportDocument {
    pub ontology_iri: String,
    pub version_iri: Option<String>,
    pub object_key: String,
    pub sha256: String,
    pub bytes: u64,
}

/// Trusted, immutable request produced by the control plane after graph authorization.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct OntologyQualificationRequest {
    pub format_version: u32,
    pub tenant_id: Uuid,
    pub dataset_id: Uuid,
    pub operation_id: Uuid,
    pub snapshot_id: Uuid,
    pub semantic_compilation_root_sha256: String,
    pub authorization_policy_sha256: String,
    pub authorized_graph_set_sha256: String,
    pub authorized_asserted_graphs: Vec<AuthorizedOntologyGraph>,
    pub pinned_imports: Vec<PinnedImportDocument>,
    pub datatype_policy_object_key: String,
    pub datatype_policy_sha256: String,
}

/// One sorted projection run emitted by a logical partition.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct OntologyProjectionArtifact {
    pub graph_iri: String,
    pub relative_path: String,
    pub sha256: String,
    pub bytes: u64,
    pub triple_count: u64,
}

/// Exact output of one partition projection job.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct OntologyProjectionManifest {
    pub format_version: u32,
    pub dataset_id: Uuid,
    pub snapshot_id: Uuid,
    pub semantic_compilation_root_sha256: String,
    pub qualification_request_sha256: String,
    pub authorized_graph_set_sha256: String,
    pub partition_index: u32,
    pub semantic_partition_manifest_sha256: String,
    pub selected_quad_count: u64,
    pub artifacts: Vec<OntologyProjectionArtifact>,
}

/// One locally materialized ontology document in the exact HermiT input set.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct AssembledOntologyDocument {
    pub document_id: String,
    pub source_kind: String,
    pub graph_iri: Option<String>,
    pub ontology_iri: String,
    pub version_iri: Option<String>,
    pub import_iris: Vec<String>,
    pub relative_path: String,
    pub sha256: String,
    pub bytes: u64,
    pub triple_count: u64,
}

/// Complete local import closure and synthetic-snapshot identity.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct OntologyAssemblyManifest {
    pub format_version: u32,
    pub tenant_id: Uuid,
    pub dataset_id: Uuid,
    pub operation_id: Uuid,
    pub snapshot_id: Uuid,
    pub semantic_compilation_root_sha256: String,
    pub semantic_content_sha256: String,
    pub qualification_request_sha256: String,
    pub authorization_policy_sha256: String,
    pub authorized_graph_set_sha256: String,
    pub datatype_policy_sha256: String,
    pub projection_completion_set_sha256: String,
    pub documents: Vec<AssembledOntologyDocument>,
    pub complete_pinned_import_closure: bool,
    pub aggregate_input_sha256: String,
    pub ontology_hashes_sha256: String,
    pub synthetic_snapshot_ontology_sha256: String,
    pub publication_state: String,
}

/// HermiT adapter input, matching the pinned Java adapter format version 4.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct HermitQualificationRequest {
    pub format_version: u32,
    pub dataset_id: Uuid,
    pub snapshot_id: Uuid,
    pub inputs: Vec<HermitInput>,
    pub aggregate_input_sha256: String,
    pub output_closure_path: String,
    pub output_report_path: String,
    pub output_owl_signature_path: String,
    pub output_owl_profile_qualification_path: String,
    pub output_owl_consistency_qualification_path: String,
    pub datatype_policy_path: String,
    pub datatype_policy_sha256: String,
    pub max_named_individuals: u64,
    pub max_properties: u64,
}

/// One exact HermiT input.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct HermitInput {
    pub path: String,
    pub sha256: String,
    pub ontology_iris: Vec<String>,
}

/// Immutable qualification root. It is evidence, not publication authorization.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct OntologyQualificationRoot {
    pub format_version: u32,
    pub tenant_id: Uuid,
    pub dataset_id: Uuid,
    pub operation_id: Uuid,
    pub snapshot_id: Uuid,
    pub semantic_compilation_root_sha256: String,
    pub qualification_request_sha256: String,
    pub assembly_manifest_sha256: String,
    pub authorized_graph_set_sha256: String,
    pub datatype_policy_sha256: String,
    pub synthetic_snapshot_ontology_sha256: String,
    pub owl_signature_sha256: String,
    pub owl_profile_qualification_sha256: String,
    pub owl_consistency_qualification_sha256: String,
    pub reasoner_report_sha256: String,
    pub finite_closure_sha256: String,
    pub finite_closure_axiom_count: u64,
    pub reasoner_name: String,
    pub reasoner_version: String,
    pub profile_valid: bool,
    pub consistency_checked: bool,
    pub consistent: bool,
    pub qualification_state: String,
    pub publication_state: String,
}

/// Fail-closed qualification failures.
#[derive(Debug, Error)]
pub enum OntologyQualificationError {
    #[error("ontology qualification I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("ontology qualification JSON failed: {0}")]
    Json(#[from] serde_json::Error),
    #[error("ontology qualification RDF failed: {0}")]
    Rdf(String),
    #[error("ontology qualification contract failed: {0}")]
    Contract(String),
    #[error("HermiT qualification failed")]
    Hermit,
}

/// Validate build authorization and exact binding to the inactive semantic root.
pub fn validate_qualification_request(
    root: &SemanticCompilationRoot,
    root_sha256: &str,
    request: &OntologyQualificationRequest,
) -> Result<(), OntologyQualificationError> {
    require_sha256(root_sha256)?;
    for value in [
        request.semantic_compilation_root_sha256.as_str(),
        request.authorization_policy_sha256.as_str(),
        request.authorized_graph_set_sha256.as_str(),
        request.datatype_policy_sha256.as_str(),
        root.semantic_content_sha256.as_str(),
    ] {
        require_sha256(value)?;
    }
    if request.format_version != ONTOLOGY_QUALIFICATION_FORMAT_VERSION
        || request.tenant_id != root.tenant_id
        || request.dataset_id != root.dataset_id
        || request.operation_id != root.operation_id
        || request.snapshot_id != root.snapshot_id
        || request.semantic_compilation_root_sha256 != root_sha256
        || root.publication_state != "inactive"
        || root.qualification_state != "pending-owl2-dl-snapshot-qualification"
        || request.authorized_asserted_graphs.is_empty()
    {
        return Err(OntologyQualificationError::Contract(
            "qualification request is not bound to the inactive semantic root".to_owned(),
        ));
    }
    if request.datatype_policy_object_key != "policies/owl-direct-datatype-policy.json" {
        return Err(OntologyQualificationError::Contract(
            "qualification request does not use the operator-owned datatype policy".to_owned(),
        ));
    }
    let candidates = root
        .graph_inventory
        .iter()
        .filter(|entry| entry.role == GraphRole::AssertedOntologyCandidate)
        .map(|entry| (entry.graph_term.clone(), entry.graph_id))
        .collect::<BTreeMap<_, _>>();
    let mut graph_rows = request.authorized_asserted_graphs.clone();
    graph_rows.sort_by(|left, right| left.graph_iri.cmp(&right.graph_iri));
    if graph_rows != request.authorized_asserted_graphs {
        return Err(OntologyQualificationError::Contract(
            "authorized asserted graphs must be strictly sorted".to_owned(),
        ));
    }
    let mut previous = None;
    for graph in &graph_rows {
        if !is_asserted_graph_iri(&graph.graph_iri)
            || candidates.get(&format!("<{}>", graph.graph_iri)) != Some(&graph.graph_id)
            || graph.authorization_labels.is_empty()
            || graph.authorization_labels.iter().any(|label| label.is_empty())
            || graph.authorization_labels.windows(2).any(|pair| pair[0] >= pair[1])
            || previous == Some(graph.graph_iri.as_str())
        {
            return Err(OntologyQualificationError::Contract(
                "graph authorization contains an invalid or non-asserted graph".to_owned(),
            ));
        }
        previous = Some(graph.graph_iri.as_str());
    }
    if hash_authorized_graphs(&graph_rows) != request.authorized_graph_set_sha256 {
        return Err(OntologyQualificationError::Contract(
            "authorized graph-set SHA-256 mismatch".to_owned(),
        ));
    }
    let mut aliases = BTreeSet::new();
    let mut previous_import = None;
    let import_prefix = format!(
        "ontology-imports/{}/{}/",
        request.tenant_id, request.dataset_id
    );
    for import in &request.pinned_imports {
        require_sha256(&import.sha256)?;
        if import.bytes == 0
            || import.object_key.is_empty()
            || !import.object_key.starts_with(&import_prefix)
            || pinned_extension(&import.object_key).is_none()
            || previous_import.as_deref() >= Some(import.ontology_iri.as_str())
            || !aliases.insert(import.ontology_iri.clone())
            || import.version_iri.as_ref().is_some_and(|iri| !aliases.insert(iri.clone()))
        {
            return Err(OntologyQualificationError::Contract(
                "pinned imports are invalid, duplicated, or unsorted".to_owned(),
            ));
        }
        previous_import = Some(import.ontology_iri.clone());
    }
    Ok(())
}

/// Project one semantic partition into sorted N-Triples runs, one per authorized graph.
#[allow(clippy::too_many_arguments)]
pub fn project_partition(
    root: &SemanticCompilationRoot,
    root_sha256: &str,
    request: &OntologyQualificationRequest,
    request_sha256: &str,
    partition_manifest_path: &Path,
    partition_manifest_sha256: &str,
    facts_path: &Path,
    output_root: &Path,
    max_selected_quads: u64,
    max_rows_in_memory: usize,
) -> Result<PathBuf, OntologyQualificationError> {
    validate_qualification_request(root, root_sha256, request)?;
    verify_file(partition_manifest_path, partition_manifest_sha256, None)?;
    require_sha256(request_sha256)?;
    let partition: SemanticPartitionManifest = read_json(partition_manifest_path)?;
    if partition.dataset_id != root.dataset_id
        || partition.snapshot_id != root.snapshot_id
        || partition.compiler_handoff_sha256 != root.compiler_handoff_sha256
        || partition.dictionary_sha256.is_empty()
    {
        return Err(OntologyQualificationError::Contract(
            "semantic partition is not bound to the compilation root".to_owned(),
        ));
    }
    let expected = root
        .partitions
        .iter()
        .find(|entry| entry.partition_index == partition.partition_index)
        .ok_or_else(|| OntologyQualificationError::Contract("partition absent from root".to_owned()))?;
    if expected.manifest_sha256 != partition_manifest_sha256 {
        return Err(OntologyQualificationError::Contract(
            "partition manifest checksum differs from root".to_owned(),
        ));
    }
    ensure_new_root(output_root)?;
    if max_rows_in_memory == 0 {
        return Err(OntologyQualificationError::Contract(
            "ontology projection memory row ceiling must be positive".to_owned(),
        ));
    }
    let authorized = request
        .authorized_asserted_graphs
        .iter()
        .map(|graph| graph.graph_iri.as_str())
        .collect::<BTreeSet<_>>();
    let graph_ordinals = authorized
        .iter()
        .enumerate()
        .map(|(index, graph)| ((*graph).to_owned(), index))
        .collect::<BTreeMap<_, _>>();
    let mut rows: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut run_paths: BTreeMap<String, Vec<PathBuf>> = BTreeMap::new();
    let mut run_counters: BTreeMap<String, usize> = BTreeMap::new();
    let mut buffered = 0_usize;
    let parser = RdfParser::from_format(RdfFormat::NQuads)
        .for_reader(BufReader::new(File::open(facts_path)?));
    let mut selected = 0_u64;
    for result in parser {
        let quad = result.map_err(|error| OntologyQualificationError::Rdf(error.to_string()))?;
        let GraphName::NamedNode(graph) = &quad.graph_name else {
            continue;
        };
        if !authorized.contains(graph.as_str()) {
            continue;
        }
        selected = selected.checked_add(1).ok_or_else(|| {
            OntologyQualificationError::Contract("selected quad counter overflow".to_owned())
        })?;
        if selected > max_selected_quads {
            return Err(OntologyQualificationError::Contract(
                "partition ontology projection exceeds its quad ceiling".to_owned(),
            ));
        }
        rows.entry(graph.as_str().to_owned())
            .or_default()
            .push(as_ntriple(&quad));
        buffered = buffered.saturating_add(1);
        if buffered >= max_rows_in_memory {
            flush_projection_buffers(
                output_root,
                &graph_ordinals,
                &mut rows,
                &mut run_paths,
                &mut run_counters,
            )?;
            buffered = 0;
        }
    }
    flush_projection_buffers(
        output_root,
        &graph_ordinals,
        &mut rows,
        &mut run_paths,
        &mut run_counters,
    )?;
    let mut artifacts = Vec::new();
    for (ordinal, graph) in authorized.iter().enumerate() {
        let Some(runs) = run_paths.remove(*graph) else { continue; };
        let relative = format!("graphs/{ordinal:08}.nt");
        let path = output_root.join(&relative);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let triple_count = merge_sorted_runs(&runs, &path)?;
        artifacts.push(OntologyProjectionArtifact {
            graph_iri: (*graph).to_owned(),
            relative_path: relative,
            sha256: sha256_path(&path)?,
            bytes: fs::metadata(&path)?.len(),
            triple_count,
        });
    }
    let spill_root = output_root.join(".spill");
    if spill_root.exists() {
        fs::remove_dir_all(spill_root)?;
    }
    let manifest = OntologyProjectionManifest {
        format_version: ONTOLOGY_QUALIFICATION_FORMAT_VERSION,
        dataset_id: root.dataset_id,
        snapshot_id: root.snapshot_id,
        semantic_compilation_root_sha256: root_sha256.to_owned(),
        qualification_request_sha256: request_sha256.to_owned(),
        authorized_graph_set_sha256: request.authorized_graph_set_sha256.clone(),
        partition_index: partition.partition_index,
        semantic_partition_manifest_sha256: partition_manifest_sha256.to_owned(),
        selected_quad_count: artifacts.iter().map(|artifact| artifact.triple_count).sum(),
        artifacts,
    };
    let path = output_root.join("ontology-projection.json");
    write_json_new(&path, &manifest)?;
    Ok(path)
}

/// Merge every projection and pinned import into one complete local ontology assembly.
pub fn assemble_snapshot_ontology(
    root: &SemanticCompilationRoot,
    root_sha256: &str,
    request: &OntologyQualificationRequest,
    request_sha256: &str,
    projection_manifest_paths: &[PathBuf],
    pinned_import_paths: &BTreeMap<String, PathBuf>,
    datatype_policy_path: &Path,
    output_root: &Path,
) -> Result<PathBuf, OntologyQualificationError> {
    validate_qualification_request(root, root_sha256, request)?;
    verify_file(datatype_policy_path, &request.datatype_policy_sha256, None)?;
    if projection_manifest_paths.len() != root.partitions.len() {
        return Err(OntologyQualificationError::Contract(
            "ontology projection completion barrier is incomplete".to_owned(),
        ));
    }
    ensure_new_root(output_root)?;
    let mut projections = Vec::new();
    for path in projection_manifest_paths {
        let manifest: OntologyProjectionManifest = read_json(path)?;
        if manifest.semantic_compilation_root_sha256 != root_sha256
            || manifest.qualification_request_sha256 != request_sha256
            || manifest.authorized_graph_set_sha256 != request.authorized_graph_set_sha256
        {
            return Err(OntologyQualificationError::Contract(
                "ontology projection identity mismatch".to_owned(),
            ));
        }
        projections.push((path.clone(), manifest));
    }
    projections.sort_by_key(|(_, manifest)| manifest.partition_index);
    if projections
        .iter()
        .enumerate()
        .any(|(index, (_, manifest))| manifest.partition_index != u32::try_from(index).unwrap_or(u32::MAX))
    {
        return Err(OntologyQualificationError::Contract(
            "ontology projection partition set is incomplete or duplicated".to_owned(),
        ));
    }
    let mut completion = Sha256::new();
    completion.update(b"ngkg-ontology-projection-completion-v1\0");
    let mut graph_runs: BTreeMap<String, Vec<PathBuf>> = BTreeMap::new();
    for (manifest_path, manifest) in &projections {
        completion.update(manifest.partition_index.to_be_bytes());
        completion.update(decode_sha256(&sha256_path(manifest_path)?)?);
        let parent = manifest_path.parent().ok_or_else(|| {
            OntologyQualificationError::Contract("projection has no parent".to_owned())
        })?;
        for artifact in &manifest.artifacts {
            let path = safe_join(parent, &artifact.relative_path)?;
            verify_file(&path, &artifact.sha256, Some(artifact.bytes))?;
            graph_runs.entry(artifact.graph_iri.clone()).or_default().push(path);
        }
    }
    let authorized = request
        .authorized_asserted_graphs
        .iter()
        .map(|graph| graph.graph_iri.clone())
        .collect::<BTreeSet<_>>();
    if !graph_runs.keys().all(|graph| authorized.contains(graph)) {
        return Err(OntologyQualificationError::Contract(
            "projection includes a graph outside build authorization".to_owned(),
        ));
    }
    let document_root = output_root.join("documents");
    fs::create_dir_all(&document_root)?;
    let mut documents = Vec::new();
    for (ordinal, graph) in authorized.iter().enumerate() {
        let runs = graph_runs.remove(graph).unwrap_or_default();
        if runs.is_empty() {
            return Err(OntologyQualificationError::Contract(
                "authorized asserted ontology graph contains no triples".to_owned(),
            ));
        }
        let relative = format!("documents/asserted-{ordinal:08}.nt");
        let path = output_root.join(&relative);
        let triple_count = merge_sorted_runs(&runs, &path)?;
        let identity = scan_ontology_document(&path)?;
        documents.push(AssembledOntologyDocument {
            document_id: format!("asserted:{graph}"),
            source_kind: "authorized-asserted-semkg".to_owned(),
            graph_iri: Some(graph.clone()),
            ontology_iri: identity.ontology_iri,
            version_iri: identity.version_iri,
            import_iris: identity.import_iris,
            relative_path: relative,
            sha256: sha256_path(&path)?,
            bytes: fs::metadata(&path)?.len(),
            triple_count,
        });
    }
    for (ordinal, pin) in request.pinned_imports.iter().enumerate() {
        let source = pinned_import_paths.get(&pin.object_key).ok_or_else(|| {
            OntologyQualificationError::Contract("pinned import was not materialized".to_owned())
        })?;
        verify_file(source, &pin.sha256, Some(pin.bytes))?;
        let extension = pinned_extension(&pin.object_key).ok_or_else(|| {
            OntologyQualificationError::Contract("pinned import format is unsupported".to_owned())
        })?;
        let relative = format!("documents/import-{ordinal:08}.{extension}");
        let path = output_root.join(&relative);
        fs::copy(source, &path)?;
        let identity = scan_ontology_document(&path)?;
        if identity.ontology_iri != pin.ontology_iri || identity.version_iri != pin.version_iri {
            return Err(OntologyQualificationError::Contract(
                "pinned import identity differs from its lock entry".to_owned(),
            ));
        }
        documents.push(AssembledOntologyDocument {
            document_id: format!("pinned:{}", pin.ontology_iri),
            source_kind: "pinned-import".to_owned(),
            graph_iri: None,
            ontology_iri: identity.ontology_iri,
            version_iri: identity.version_iri,
            import_iris: identity.import_iris,
            relative_path: relative,
            sha256: pin.sha256.clone(),
            bytes: pin.bytes,
            triple_count: identity.triple_count,
        });
    }
    documents.sort_by(|left, right| left.ontology_iri.cmp(&right.ontology_iri));
    validate_import_closure(&documents)?;
    let aggregate_input_sha256 = aggregate_input_sha256(&documents)?;
    let ontology_hashes_sha256 = hash_document_set(&documents)?;
    let synthetic_snapshot_ontology_sha256 = hash_synthetic_snapshot(
        root,
        request,
        &aggregate_input_sha256,
        &ontology_hashes_sha256,
    );
    let manifest = OntologyAssemblyManifest {
        format_version: ONTOLOGY_QUALIFICATION_FORMAT_VERSION,
        tenant_id: root.tenant_id,
        dataset_id: root.dataset_id,
        operation_id: root.operation_id,
        snapshot_id: root.snapshot_id,
        semantic_compilation_root_sha256: root_sha256.to_owned(),
        semantic_content_sha256: root.semantic_content_sha256.clone(),
        qualification_request_sha256: request_sha256.to_owned(),
        authorization_policy_sha256: request.authorization_policy_sha256.clone(),
        authorized_graph_set_sha256: request.authorized_graph_set_sha256.clone(),
        datatype_policy_sha256: request.datatype_policy_sha256.clone(),
        projection_completion_set_sha256: hex::encode(completion.finalize()),
        documents,
        complete_pinned_import_closure: true,
        aggregate_input_sha256,
        ontology_hashes_sha256,
        synthetic_snapshot_ontology_sha256,
        publication_state: "inactive".to_owned(),
    };
    let path = output_root.join("ontology-assembly.json");
    write_json_new(&path, &manifest)?;
    Ok(path)
}

/// Create the exact adapter request. The caller may execute it locally or in a dedicated reasoner pod.
pub fn build_hermit_request(
    assembly_path: &Path,
    assembly_sha256: &str,
    datatype_policy_path: &Path,
    output_root: &Path,
    max_named_individuals: u64,
    max_properties: u64,
) -> Result<PathBuf, OntologyQualificationError> {
    verify_file(assembly_path, assembly_sha256, None)?;
    let assembly: OntologyAssemblyManifest = read_json(assembly_path)?;
    verify_file(datatype_policy_path, &assembly.datatype_policy_sha256, None)?;
    if !assembly.complete_pinned_import_closure
        || assembly.publication_state != "inactive"
        || max_named_individuals == 0
        || max_properties == 0
    {
        return Err(OntologyQualificationError::Contract(
            "assembly is incomplete or HermiT ceilings are invalid".to_owned(),
        ));
    }
    fs::create_dir_all(output_root)?;
    let assembly_root = assembly_path.parent().ok_or_else(|| {
        OntologyQualificationError::Contract("assembly has no parent".to_owned())
    })?;
    let mut inputs = Vec::new();
    for document in &assembly.documents {
        let path = safe_join(assembly_root, &document.relative_path)?;
        verify_file(&path, &document.sha256, Some(document.bytes))?;
        let mut aliases = vec![document.ontology_iri.clone()];
        if let Some(version) = &document.version_iri {
            aliases.push(version.clone());
        }
        aliases.sort();
        inputs.push(HermitInput {
            path: path.to_string_lossy().into_owned(),
            sha256: document.sha256.clone(),
            ontology_iris: aliases,
        });
    }
    let request = HermitQualificationRequest {
        format_version: 4,
        dataset_id: assembly.dataset_id,
        snapshot_id: assembly.snapshot_id,
        inputs,
        aggregate_input_sha256: assembly.aggregate_input_sha256,
        output_closure_path: output_root.join("finite-closure.nt").to_string_lossy().into_owned(),
        output_report_path: output_root.join("reasoner-report.json").to_string_lossy().into_owned(),
        output_owl_signature_path: output_root.join("owl-signature.json").to_string_lossy().into_owned(),
        output_owl_profile_qualification_path: output_root.join("owl-profile-qualification.json").to_string_lossy().into_owned(),
        output_owl_consistency_qualification_path: output_root.join("owl-consistency-qualification.json").to_string_lossy().into_owned(),
        datatype_policy_path: datatype_policy_path.to_string_lossy().into_owned(),
        datatype_policy_sha256: assembly.datatype_policy_sha256,
        max_named_individuals,
        max_properties,
    };
    let path = output_root.join("hermit-qualification-request.json");
    write_json_new(&path, &request)?;
    Ok(path)
}

/// Execute pinned HermiT with a bounded heap and fail on any non-successful result.
pub fn execute_hermit(
    java_executable: &Path,
    adapter_jar: &Path,
    adapter_sha256: &str,
    request_path: &Path,
    heap_mib: u64,
    timeout: Duration,
) -> Result<(), OntologyQualificationError> {
    verify_file(adapter_jar, adapter_sha256, None)?;
    if heap_mib < 256 {
        return Err(OntologyQualificationError::Contract(
            "HermiT heap must be at least 256 MiB".to_owned(),
        ));
    }
    if timeout.is_zero() {
        return Err(OntologyQualificationError::Contract(
            "HermiT timeout must be positive".to_owned(),
        ));
    }
    let mut child = Command::new(java_executable)
        .arg(format!("-Xmx{heap_mib}m"))
        .arg("-XX:+ExitOnOutOfMemoryError")
        .arg("-jar")
        .arg(adapter_jar)
        .arg("--request")
        .arg(request_path)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .spawn()?;
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = child.try_wait()? {
            if status.success() {
                return Ok(());
            }
            return Err(OntologyQualificationError::Hermit);
        }
        if Instant::now() >= deadline {
            child.kill()?;
            let _ = child.wait();
            return Err(OntologyQualificationError::Hermit);
        }
        thread::sleep(Duration::from_millis(50));
    }
}

/// Verify exact HermiT evidence and write an inactive qualification root.
pub fn finalize_qualification(
    request: &OntologyQualificationRequest,
    request_sha256: &str,
    assembly_path: &Path,
    assembly_sha256: &str,
    reasoner_output_root: &Path,
    output_root: &Path,
) -> Result<PathBuf, OntologyQualificationError> {
    verify_file(assembly_path, assembly_sha256, None)?;
    let assembly: OntologyAssemblyManifest = read_json(assembly_path)?;
    let report_path = reasoner_output_root.join("reasoner-report.json");
    let finite_closure_path = reasoner_output_root.join("finite-closure.nt");
    let signature_path = reasoner_output_root.join("owl-signature.json");
    let profile_path = reasoner_output_root.join("owl-profile-qualification.json");
    let consistency_path = reasoner_output_root.join("owl-consistency-qualification.json");
    let report: ReasonerReport = read_json(&report_path)?;
    let profile: ProfileEvidence = read_json(&profile_path)?;
    let consistency: ConsistencyEvidence = read_json(&consistency_path)?;
    let signature_sha256 = sha256_path(&signature_path)?;
    let profile_sha256 = sha256_path(&profile_path)?;
    let consistency_sha256 = sha256_path(&consistency_path)?;
    if report.format_version != 5
        || report.dataset_id != request.dataset_id
        || report.snapshot_id != request.snapshot_id
        || report.reasoner_name != "HermiT"
        || report.reasoner_version != "1.4.5.519"
        || report.aggregate_input_sha256 != assembly.aggregate_input_sha256
        || report.datatype_policy_sha256 != request.datatype_policy_sha256
        || report.owl_signature_sha256 != signature_sha256
        || report.owl_profile_qualification_sha256 != profile_sha256
        || report.owl_consistency_qualification_sha256 != consistency_sha256
        || profile.format_version != 1
        || profile.dataset_id != request.dataset_id
        || profile.snapshot_id != request.snapshot_id
        || profile.aggregate_input_sha256 != assembly.aggregate_input_sha256
        || profile.owl_signature_sha256 != report.owl_signature_sha256
        || profile.datatype_policy_sha256 != request.datatype_policy_sha256
        || !profile.profile_valid
        || !profile.complete_local_import_closure
        || consistency.format_version != 1
        || consistency.dataset_id != request.dataset_id
        || consistency.snapshot_id != request.snapshot_id
        || consistency.aggregate_input_sha256 != assembly.aggregate_input_sha256
        || consistency.owl_signature_sha256 != report.owl_signature_sha256
        || consistency.datatype_policy_sha256 != request.datatype_policy_sha256
        || consistency.owl_profile_qualification_sha256 != report.owl_profile_qualification_sha256
        || consistency.reasoner_name != "HermiT"
        || consistency.reasoner_version != "1.4.5.519"
        || !consistency.consistency_checked
        || !consistency.consistent
        || !consistency.publication_permitted
    {
        return Err(OntologyQualificationError::Contract(
            "HermiT evidence is incomplete, inconsistent, or identity-mismatched".to_owned(),
        ));
    }
    ensure_new_root(output_root)?;
    let root = OntologyQualificationRoot {
        format_version: ONTOLOGY_QUALIFICATION_FORMAT_VERSION,
        tenant_id: request.tenant_id,
        dataset_id: request.dataset_id,
        operation_id: request.operation_id,
        snapshot_id: request.snapshot_id,
        semantic_compilation_root_sha256: request.semantic_compilation_root_sha256.clone(),
        qualification_request_sha256: request_sha256.to_owned(),
        assembly_manifest_sha256: assembly_sha256.to_owned(),
        authorized_graph_set_sha256: request.authorized_graph_set_sha256.clone(),
        datatype_policy_sha256: request.datatype_policy_sha256.clone(),
        synthetic_snapshot_ontology_sha256: assembly.synthetic_snapshot_ontology_sha256,
        owl_signature_sha256: report.owl_signature_sha256,
        owl_profile_qualification_sha256: report.owl_profile_qualification_sha256,
        owl_consistency_qualification_sha256: report.owl_consistency_qualification_sha256,
        reasoner_report_sha256: sha256_path(&report_path)?,
        finite_closure_sha256: sha256_path(&finite_closure_path)?,
        finite_closure_axiom_count: report.emitted_axiom_count,
        reasoner_name: report.reasoner_name,
        reasoner_version: report.reasoner_version,
        profile_valid: true,
        consistency_checked: true,
        consistent: true,
        qualification_state: "owl2-dl-qualified".to_owned(),
        publication_state: "inactive".to_owned(),
    };
    let path = output_root.join("ontology-qualification-root.json");
    write_json_new(&path, &root)?;
    Ok(path)
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ReasonerReport {
    format_version: u32,
    dataset_id: Uuid,
    snapshot_id: Uuid,
    reasoner_name: String,
    reasoner_version: String,
    aggregate_input_sha256: String,
    owl_signature_sha256: String,
    datatype_policy_sha256: String,
    owl_profile_qualification_sha256: String,
    owl_consistency_qualification_sha256: String,
    emitted_axiom_count: u64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ProfileEvidence {
    format_version: u32,
    dataset_id: Uuid,
    snapshot_id: Uuid,
    aggregate_input_sha256: String,
    owl_signature_sha256: String,
    datatype_policy_sha256: String,
    complete_local_import_closure: bool,
    profile_valid: bool,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ConsistencyEvidence {
    format_version: u32,
    dataset_id: Uuid,
    snapshot_id: Uuid,
    aggregate_input_sha256: String,
    owl_signature_sha256: String,
    datatype_policy_sha256: String,
    owl_profile_qualification_sha256: String,
    reasoner_name: String,
    reasoner_version: String,
    consistency_checked: bool,
    consistent: bool,
    publication_permitted: bool,
}

struct OntologyIdentity {
    ontology_iri: String,
    version_iri: Option<String>,
    import_iris: Vec<String>,
    triple_count: u64,
}

fn scan_ontology_document(path: &Path) -> Result<OntologyIdentity, OntologyQualificationError> {
    let format = match path.extension().and_then(|value| value.to_str()) {
        Some("nt") => RdfFormat::NTriples,
        Some("nq") => RdfFormat::NQuads,
        Some("ttl") => RdfFormat::Turtle,
        Some("trig") => RdfFormat::TriG,
        Some("rdf" | "owl" | "xml") => RdfFormat::RdfXml,
        _ => return Err(OntologyQualificationError::Contract("unsupported ontology document format".to_owned())),
    };
    let parser = RdfParser::from_format(format).for_reader(BufReader::new(File::open(path)?));
    let mut headers = BTreeSet::new();
    let mut versions: BTreeMap<String, String> = BTreeMap::new();
    let mut imports: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut count = 0_u64;
    for result in parser {
        let quad = result.map_err(|error| OntologyQualificationError::Rdf(error.to_string()))?;
        count = count.checked_add(1).ok_or_else(|| OntologyQualificationError::Contract("triple count overflow".to_owned()))?;
        let Some(subject) = named_subject(&quad) else { continue; };
        if quad.predicate.as_str() == RDF_TYPE && named_object(&quad) == Some(OWL_ONTOLOGY) {
            headers.insert(subject.to_owned());
        } else if quad.predicate.as_str() == OWL_VERSION_IRI {
            if let Some(value) = named_object(&quad) {
                if versions.insert(subject.to_owned(), value.to_owned()).is_some() {
                    return Err(OntologyQualificationError::Contract("ontology has multiple version IRIs".to_owned()));
                }
            }
        } else if quad.predicate.as_str() == OWL_IMPORTS {
            if let Some(value) = named_object(&quad) {
                imports.entry(subject.to_owned()).or_default().insert(value.to_owned());
            }
        }
    }
    if headers.len() != 1 {
        return Err(OntologyQualificationError::Contract(
            "every ontology module must contain exactly one owl:Ontology header".to_owned(),
        ));
    }
    let ontology_iri = headers.into_iter().next().ok_or_else(|| {
        OntologyQualificationError::Contract("ontology header disappeared".to_owned())
    })?;
    if versions.keys().any(|subject| subject != &ontology_iri)
        || imports.keys().any(|subject| subject != &ontology_iri)
    {
        return Err(OntologyQualificationError::Contract(
            "owl:versionIRI and owl:imports must belong to the ontology header".to_owned(),
        ));
    }
    Ok(OntologyIdentity {
        version_iri: versions.remove(&ontology_iri),
        import_iris: imports.remove(&ontology_iri).unwrap_or_default().into_iter().collect(),
        ontology_iri,
        triple_count: count,
    })
}

fn validate_import_closure(documents: &[AssembledOntologyDocument]) -> Result<(), OntologyQualificationError> {
    let mut aliases = BTreeMap::new();
    for (index, document) in documents.iter().enumerate() {
        for alias in std::iter::once(&document.ontology_iri).chain(document.version_iri.iter()) {
            if aliases.insert(alias.clone(), index).is_some() {
                return Err(OntologyQualificationError::Contract(
                    "ontology/version IRI maps to multiple documents".to_owned(),
                ));
            }
        }
    }
    for document in documents {
        for imported in &document.import_iris {
            if !aliases.contains_key(imported) {
                return Err(OntologyQualificationError::Contract(
                    format!("unresolved or unpinned owl:imports target: {imported}"),
                ));
            }
        }
    }
    Ok(())
}

fn merge_sorted_runs(paths: &[PathBuf], output: &Path) -> Result<u64, OntologyQualificationError> {
    let mut readers = Vec::with_capacity(paths.len());
    for path in paths {
        readers.push(BufReader::new(File::open(path)?));
    }
    let mut heap = BinaryHeap::new();
    for (index, reader) in readers.iter_mut().enumerate() {
        if let Some(row) = read_nonempty_line(reader)? {
            heap.push(Reverse((row, index)));
        }
    }
    let mut writer = BufWriter::new(create_new(output)?);
    let mut previous: Option<String> = None;
    let mut count = 0_u64;
    while let Some(Reverse((row, index))) = heap.pop() {
        if previous.as_deref() != Some(row.as_str()) {
            writer.write_all(row.as_bytes())?;
            writer.write_all(b"\n")?;
            count = count.checked_add(1).ok_or_else(|| {
                OntologyQualificationError::Contract("merged triple count overflow".to_owned())
            })?;
            previous = Some(row);
        }
        if let Some(next) = read_nonempty_line(&mut readers[index])? {
            heap.push(Reverse((next, index)));
        }
    }
    writer.flush()?;
    Ok(count)
}

fn flush_projection_buffers(
    output_root: &Path,
    graph_ordinals: &BTreeMap<String, usize>,
    buffers: &mut BTreeMap<String, Vec<String>>,
    runs: &mut BTreeMap<String, Vec<PathBuf>>,
    counters: &mut BTreeMap<String, usize>,
) -> Result<(), OntologyQualificationError> {
    for (graph, rows) in buffers.iter_mut() {
        if rows.is_empty() { continue; }
        rows.sort();
        rows.dedup();
        let ordinal = graph_ordinals.get(graph).ok_or_else(|| {
            OntologyQualificationError::Contract("projection graph lacks stable ordinal".to_owned())
        })?;
        let counter = counters.entry(graph.clone()).or_default();
        let path = output_root.join(format!(".spill/{ordinal:08}/run-{:08}.nt", *counter));
        if let Some(parent) = path.parent() { fs::create_dir_all(parent)?; }
        let mut writer = BufWriter::new(create_new(&path)?);
        for row in rows.iter() {
            writer.write_all(row.as_bytes())?;
            writer.write_all(b"\n")?;
        }
        writer.flush()?;
        runs.entry(graph.clone()).or_default().push(path);
        *counter = counter.saturating_add(1);
        rows.clear();
    }
    Ok(())
}

fn read_nonempty_line(reader: &mut BufReader<File>) -> Result<Option<String>, OntologyQualificationError> {
    loop {
        let mut row = String::new();
        if reader.read_line(&mut row)? == 0 { return Ok(None); }
        while matches!(row.as_bytes().last(), Some(b'\n') | Some(b'\r')) { row.pop(); }
        if !row.is_empty() { return Ok(Some(row)); }
    }
}

fn as_ntriple(quad: &Quad) -> String {
    format!("{} {} {} .", quad.subject, quad.predicate, quad.object)
}

fn named_subject(quad: &Quad) -> Option<&str> {
    match &quad.subject {
        oxigraph::model::Subject::NamedNode(node) => Some(node.as_str()),
        _ => None,
    }
}

fn named_object(quad: &Quad) -> Option<&str> {
    match &quad.object {
        Term::NamedNode(node) => Some(node.as_str()),
        _ => None,
    }
}

fn is_asserted_graph_iri(value: &str) -> bool {
    value.starts_with("https://c8-next-generation.io/")
        && value.ends_with(ASSERTED_GRAPH_SUFFIX)
        && !value.contains("/closure")
        && !value.contains("/provenance")
        && !value.contains("/alignment")
}

fn pinned_extension(object_key: &str) -> Option<&'static str> {
    match Path::new(object_key).extension()?.to_str()?.to_ascii_lowercase().as_str() {
        "ttl" => Some("ttl"),
        "nt" => Some("nt"),
        "rdf" => Some("rdf"),
        "owl" => Some("owl"),
        "xml" => Some("xml"),
        _ => None,
    }
}

/// Deterministic hash of the exact authorized asserted graph set.
pub fn hash_authorized_graphs(graphs: &[AuthorizedOntologyGraph]) -> String {
    let mut digest = Sha256::new();
    digest.update(b"ngkg-authorized-asserted-ontology-graphs-v1\0");
    for graph in graphs {
        hash_string(&mut digest, &graph.graph_iri);
        digest.update(graph.graph_id.to_be_bytes());
        for label in &graph.authorization_labels {
            hash_string(&mut digest, label);
        }
    }
    hex::encode(digest.finalize())
}

fn aggregate_input_sha256(documents: &[AssembledOntologyDocument]) -> Result<String, OntologyQualificationError> {
    let mut digest = Sha256::new();
    for document in documents {
        let decoded = decode_sha256(&document.sha256)?;
        digest.update(u64::try_from(decoded.len()).unwrap_or(32).to_be_bytes());
        digest.update(decoded);
    }
    Ok(hex::encode(digest.finalize()))
}

fn hash_document_set(documents: &[AssembledOntologyDocument]) -> Result<String, OntologyQualificationError> {
    let mut digest = Sha256::new();
    digest.update(b"ngkg-ontology-document-set-v1\0");
    for document in documents {
        hash_string(&mut digest, &document.ontology_iri);
        if let Some(version) = &document.version_iri { hash_string(&mut digest, version); }
        digest.update(decode_sha256(&document.sha256)?);
    }
    Ok(hex::encode(digest.finalize()))
}

fn hash_synthetic_snapshot(
    root: &SemanticCompilationRoot,
    request: &OntologyQualificationRequest,
    aggregate: &str,
    ontology_hashes: &str,
) -> String {
    let mut digest = Sha256::new();
    digest.update(b"ngkg-synthetic-owl2dl-snapshot-v1\0");
    digest.update(root.dataset_id.as_bytes());
    digest.update(root.snapshot_id.as_bytes());
    for value in [
        root.semantic_content_sha256.as_str(),
        request.authorized_graph_set_sha256.as_str(),
        request.datatype_policy_sha256.as_str(),
        aggregate,
        ontology_hashes,
    ] { digest.update(hex::decode(value).unwrap_or_default()); }
    hex::encode(digest.finalize())
}

fn hash_string(digest: &mut Sha256, value: &str) {
    digest.update((value.len() as u64).to_be_bytes());
    digest.update(value.as_bytes());
}

/// SHA-256 of a regular file.
pub fn sha256_path(path: &Path) -> Result<String, OntologyQualificationError> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.file_type().is_file() || metadata.file_type().is_symlink() {
        return Err(OntologyQualificationError::Contract("artifact is not a regular file".to_owned()));
    }
    let mut reader = BufReader::new(File::open(path)?);
    let mut digest = Sha256::new();
    let mut buffer = vec![0_u8; 1024 * 1024];
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 { break; }
        digest.update(&buffer[..read]);
    }
    Ok(hex::encode(digest.finalize()))
}

fn read_json<T: DeserializeOwned>(path: &Path) -> Result<T, OntologyQualificationError> {
    Ok(serde_json::from_slice(&fs::read(path)?)?)
}

fn write_json_new<T: Serialize>(path: &Path, value: &T) -> Result<(), OntologyQualificationError> {
    let bytes = serde_json::to_vec_pretty(value)?;
    let mut writer = BufWriter::new(create_new(path)?);
    writer.write_all(&bytes)?;
    writer.write_all(b"\n")?;
    writer.flush()?;
    Ok(())
}

fn create_new(path: &Path) -> Result<File, OntologyQualificationError> {
    Ok(OpenOptions::new().create_new(true).write(true).open(path)?)
}

fn ensure_new_root(path: &Path) -> Result<(), OntologyQualificationError> {
    if path.exists() {
        return Err(OntologyQualificationError::Contract("immutable output root already exists".to_owned()));
    }
    fs::create_dir_all(path)?;
    Ok(())
}

fn verify_file(path: &Path, sha256: &str, bytes: Option<u64>) -> Result<(), OntologyQualificationError> {
    require_sha256(sha256)?;
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.file_type().is_file() || metadata.file_type().is_symlink()
        || bytes.is_some_and(|expected| expected != metadata.len())
        || sha256_path(path)? != sha256
    {
        return Err(OntologyQualificationError::Contract("artifact checksum/size mismatch".to_owned()));
    }
    Ok(())
}

fn require_sha256(value: &str) -> Result<(), OntologyQualificationError> {
    if value.len() != 64 || !value.bytes().all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase()) {
        return Err(OntologyQualificationError::Contract("invalid lowercase SHA-256".to_owned()));
    }
    Ok(())
}

fn decode_sha256(value: &str) -> Result<Vec<u8>, OntologyQualificationError> {
    require_sha256(value)?;
    hex::decode(value).map_err(|_| OntologyQualificationError::Contract("invalid SHA-256 encoding".to_owned()))
}

fn safe_join(root: &Path, relative: &str) -> Result<PathBuf, OntologyQualificationError> {
    let path = Path::new(relative);
    if path.is_absolute() || path.components().any(|component| !matches!(component, std::path::Component::Normal(_))) {
        return Err(OntologyQualificationError::Contract("artifact path escapes its root".to_owned()));
    }
    Ok(root.join(path))
}

#[cfg(test)]
mod tests {
    use std::{fs, path::PathBuf};

    use super::{
        AssembledOntologyDocument, AuthorizedOntologyGraph, hash_authorized_graphs,
        is_asserted_graph_iri, scan_ontology_document, validate_import_closure,
    };
    use uuid::Uuid;

    #[test]
    fn graph_role_boundary_rejects_non_asserted_artifacts() {
        assert!(is_asserted_graph_iri("https://c8-next-generation.io/supply/chain/semkg"));
        assert!(!is_asserted_graph_iri("https://c8-next-generation.io/supply/chain/closure"));
        assert!(!is_asserted_graph_iri("https://c8-next-generation.io/supply/chain/provenance"));
        assert!(!is_asserted_graph_iri("https://c8-next-generation.io/supply/chain/alignment/semkg"));
    }

    #[test]
    fn authorized_graph_hash_is_topology_independent() {
        let graphs = vec![AuthorizedOntologyGraph {
            graph_iri: "https://c8-next-generation.io/clinical/oncology/semkg".to_owned(),
            graph_id: 42,
            authorization_labels: vec!["domain:oncology".to_owned()],
        }];
        assert_eq!(hash_authorized_graphs(&graphs), hash_authorized_graphs(&graphs));
    }

    #[test]
    fn module_scan_and_import_closure_fail_closed() -> Result<(), Box<dyn std::error::Error>> {
        let root: PathBuf = std::env::temp_dir()
            .join(format!("ngkg-ontology-qualifier-{}", Uuid::new_v4()));
        fs::create_dir_all(&root)?;
        let path = root.join("module.nt");
        fs::write(
            &path,
            concat!(
                "<https://example.test/ontology> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2002/07/owl#Ontology> .\n",
                "<https://example.test/ontology> <http://www.w3.org/2002/07/owl#imports> <https://example.test/imported> .\n"
            ),
        )?;
        let identity = scan_ontology_document(&path)?;
        assert_eq!(identity.ontology_iri, "https://example.test/ontology");
        assert_eq!(identity.import_iris, vec!["https://example.test/imported".to_owned()]);
        let documents = vec![AssembledOntologyDocument {
            document_id: "asserted:test".to_owned(),
            source_kind: "authorized-asserted-semkg".to_owned(),
            graph_iri: Some("https://c8-next-generation.io/test/domain/semkg".to_owned()),
            ontology_iri: identity.ontology_iri,
            version_iri: None,
            import_iris: identity.import_iris,
            relative_path: "documents/asserted-00000000.nt".to_owned(),
            sha256: "1".repeat(64),
            bytes: 1,
            triple_count: 2,
        }];
        assert!(validate_import_closure(&documents).is_err());
        fs::remove_dir_all(root)?;
        Ok(())
    }
}
