//! Object-store execution for distributed OWL 2 DL snapshot qualification.

use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use futures::{StreamExt, TryStreamExt, stream};
use kube::{Api, Client, api::{Patch, PatchParams}};
use ngkg_artifact_store::ArtifactStore;
use ngkg_kube::{NgkgSourceImport, NgkgSourceImportStatus};
use ngkg_ontology_qualifier::{
    OntologyAssemblyManifest, OntologyProjectionManifest, OntologyQualificationRequest,
    assemble_snapshot_ontology, build_hermit_request, execute_hermit,
    finalize_qualification, project_partition, sha256_path,
};
use ngkg_semantic_compiler::{SemanticCompilationRoot, SemanticPartitionManifest};
use serde::de::DeserializeOwned;
use thiserror::Error;

use super::{reject_unknown, required_path, required_u64, required_usize, required_value};

#[derive(Debug, Error)]
pub enum CloudOntologyError {
    #[error("cloud ontology configuration is invalid: {0}")]
    Config(String),
    #[error("cloud ontology I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("cloud ontology artifact access failed: {0}")]
    Artifact(#[from] ngkg_artifact_store::ArtifactStoreError),
    #[error("cloud ontology qualification failed: {0}")]
    Qualification(#[from] ngkg_ontology_qualifier::OntologyQualificationError),
    #[error("cloud ontology JSON failed: {0}")]
    Json(#[from] serde_json::Error),
    #[error("cloud ontology Kubernetes status failed: {0}")]
    Kubernetes(#[from] kube::Error),
}

pub async fn execute_project(
    options: &BTreeMap<String, String>,
) -> Result<String, CloudOntologyError> {
    reject_unknown(options, &[
        "namespace", "import-name", "artifact-base-url", "scratch-root",
        "semantic-compilation-root-object-key", "semantic-compilation-root-sha256",
        "ontology-qualification-request-object-key", "ontology-qualification-request-sha256",
        "completion-index", "ontology-max-manifest-bytes", "ontology-max-partition-quads",
        "ontology-projection-rows-in-memory",
        "ontology-max-artifact-bytes", "single-put-max-bytes", "multipart-buffer-bytes",
        "multipart-concurrency",
    ]).map_err(CloudOntologyError::Config)?;
    let context = Context::new(options).await?;
    let partition_index = required_value(options, "completion-index")
        .map_err(CloudOntologyError::Config)?
        .parse::<u32>()
        .map_err(|_| CloudOntologyError::Config("completion-index must be a u32".to_owned()))?;
    let reference = context
        .root
        .partitions
        .iter()
        .find(|entry| entry.partition_index == partition_index)
        .ok_or_else(|| CloudOntologyError::Config("partition is absent from semantic root".to_owned()))?;
    let partition_manifest_path = context.scratch.join("semantic-partition.json");
    context.store.materialize_verified(
        &reference.manifest_path,
        &reference.manifest_sha256,
        limit(options, "ontology-max-manifest-bytes")?,
        &partition_manifest_path,
    ).await?;
    let partition: SemanticPartitionManifest = read_json(&partition_manifest_path)?;
    let facts = partition.artifacts.iter().find(|artifact| artifact.relative_path == "facts.nq")
        .ok_or_else(|| CloudOntologyError::Config("semantic partition lacks canonical facts.nq".to_owned()))?;
    let facts_key = parent_object_key(&reference.manifest_path)? + "/facts.nq";
    let facts_path = context.scratch.join("facts.nq");
    context.store.materialize_verified(
        &facts_key,
        &facts.sha256,
        limit(options, "ontology-max-artifact-bytes")?.min(facts.bytes),
        &facts_path,
    ).await?;
    let output = context.scratch.join("projection-output");
    let manifest = project_partition(
        &context.root,
        &context.root_sha256,
        &context.request,
        &context.request_sha256,
        &partition_manifest_path,
        &reference.manifest_sha256,
        &facts_path,
        &output,
        limit(options, "ontology-max-partition-quads")?,
        required_usize(options, "ontology-projection-rows-in-memory")
            .map_err(CloudOntologyError::Config)?,
    )?;
    upload_tree(
        &context.store,
        &output,
        &format!("{}/projections/{partition_index:05}", context.prefix),
        options,
    ).await?;
    Ok(serde_json::json!({
        "status": "ontology-projection-complete",
        "partitionIndex": partition_index,
        "manifestSha256": sha256_path(&manifest)?
    }).to_string())
}

pub async fn execute_assemble(
    options: &BTreeMap<String, String>,
) -> Result<String, CloudOntologyError> {
    reject_unknown(options, &[
        "namespace", "import-name", "artifact-base-url", "scratch-root",
        "semantic-compilation-root-object-key", "semantic-compilation-root-sha256",
        "ontology-qualification-request-object-key", "ontology-qualification-request-sha256",
        "ontology-max-manifest-bytes", "ontology-max-artifact-bytes",
        "ontology-download-concurrency", "single-put-max-bytes", "multipart-buffer-bytes",
        "multipart-concurrency",
    ]).map_err(CloudOntologyError::Config)?;
    let context = Context::new(options).await?;
    let manifest_limit = limit(options, "ontology-max-manifest-bytes")?;
    let artifact_limit = limit(options, "ontology-max-artifact-bytes")?;
    let concurrency = required_usize(options, "ontology-download-concurrency")
        .map_err(CloudOntologyError::Config)?;
    if concurrency == 0 {
        return Err(CloudOntologyError::Config("download concurrency must be positive".to_owned()));
    }
    let mut projection_paths = Vec::new();
    let mut downloads = Vec::new();
    for partition in 0..context.root.logical_partitions {
        let local_root = context.scratch.join(format!("projections/{partition:05}"));
        let manifest_path = local_root.join("ontology-projection.json");
        context.store.materialize_unverified_bounded(
            &format!("{}/projections/{partition:05}/ontology-projection.json", context.prefix),
            manifest_limit,
            &manifest_path,
        ).await?;
        let manifest: OntologyProjectionManifest = read_json(&manifest_path)?;
        for artifact in manifest.artifacts {
            if artifact.bytes > artifact_limit {
                return Err(CloudOntologyError::Config("ontology projection artifact exceeds ceiling".to_owned()));
            }
            downloads.push((
                format!("{}/projections/{partition:05}/{}", context.prefix, artifact.relative_path),
                artifact.sha256,
                artifact.bytes,
                local_root.join(artifact.relative_path),
            ));
        }
        projection_paths.push(manifest_path);
    }
    stream::iter(downloads.into_iter().map(|(key, sha256, bytes, path)| {
        let store = Arc::clone(&context.store);
        async move {
            store.materialize_verified(&key, &sha256, bytes, &path).await?;
            Ok::<_, CloudOntologyError>(())
        }
    }))
    .buffer_unordered(concurrency)
    .try_collect::<Vec<_>>()
    .await?;
    let mut pinned = BTreeMap::new();
    for (ordinal, import) in context.request.pinned_imports.iter().enumerate() {
        if import.bytes > artifact_limit {
            return Err(CloudOntologyError::Config("pinned import exceeds ontology artifact ceiling".to_owned()));
        }
        let path = context.scratch.join(format!("pinned/{ordinal:08}.rdf"));
        context.store.materialize_verified(&import.object_key, &import.sha256, import.bytes, &path).await?;
        pinned.insert(import.object_key.clone(), path);
    }
    let datatype_policy = context.scratch.join("datatype-policy.json");
    context.store.materialize_verified(
        &context.request.datatype_policy_object_key,
        &context.request.datatype_policy_sha256,
        manifest_limit,
        &datatype_policy,
    ).await?;
    let output = context.scratch.join("assembly-output");
    let assembly = assemble_snapshot_ontology(
        &context.root,
        &context.root_sha256,
        &context.request,
        &context.request_sha256,
        &projection_paths,
        &pinned,
        &datatype_policy,
        &output,
    )?;
    upload_tree(&context.store, &output, &format!("{}/assembly", context.prefix), options).await?;
    let assembly_sha256 = sha256_path(&assembly)?;
    patch_status(
        options,
        NgkgSourceImportStatus {
            ontology_assembly_object_key: Some(format!("{}/assembly/ontology-assembly.json", context.prefix)),
            ontology_assembly_sha256: Some(assembly_sha256.clone()),
            condition: Some("OntologyAssemblyCompleteInactive".to_owned()),
            ..load_status(options).await?
        },
    ).await?;
    Ok(serde_json::json!({
        "status": "ontology-assembly-complete-inactive",
        "assemblySha256": assembly_sha256
    }).to_string())
}

pub async fn execute_qualify(
    options: &BTreeMap<String, String>,
) -> Result<String, CloudOntologyError> {
    reject_unknown(options, &[
        "namespace", "import-name", "artifact-base-url", "scratch-root",
        "semantic-compilation-root-object-key", "semantic-compilation-root-sha256",
        "ontology-qualification-request-object-key", "ontology-qualification-request-sha256",
        "ontology-assembly-object-key", "ontology-assembly-sha256",
        "ontology-max-manifest-bytes", "ontology-max-artifact-bytes",
        "java-executable", "reasoner-adapter-jar", "reasoner-adapter-sha256",
        "reasoner-name", "reasoner-version", "ontology-reasoner-heap-mib",
        "ontology-reasoner-timeout-seconds", "ontology-max-named-individuals",
        "ontology-max-properties", "single-put-max-bytes", "multipart-buffer-bytes",
        "multipart-concurrency",
    ]).map_err(CloudOntologyError::Config)?;
    if required_value(options, "reasoner-name").map_err(CloudOntologyError::Config)? != "HermiT"
        || required_value(options, "reasoner-version").map_err(CloudOntologyError::Config)? != "1.4.5.519"
    {
        return Err(CloudOntologyError::Config("qualification requires pinned HermiT 1.4.5.519".to_owned()));
    }
    let context = Context::new(options).await?;
    let assembly_key = required_value(options, "ontology-assembly-object-key")
        .map_err(CloudOntologyError::Config)?;
    let assembly_sha256 = required_value(options, "ontology-assembly-sha256")
        .map_err(CloudOntologyError::Config)?;
    let assembly_root = context.scratch.join("assembly");
    let assembly_path = assembly_root.join("ontology-assembly.json");
    context.store.materialize_verified(
        &assembly_key,
        &assembly_sha256,
        limit(options, "ontology-max-manifest-bytes")?,
        &assembly_path,
    ).await?;
    let assembly: OntologyAssemblyManifest = read_json(&assembly_path)?;
    let assembly_prefix = parent_object_key(&assembly_key)?;
    let artifact_limit = limit(options, "ontology-max-artifact-bytes")?;
    for document in &assembly.documents {
        if document.bytes > artifact_limit {
            return Err(CloudOntologyError::Config("assembled ontology document exceeds ceiling".to_owned()));
        }
        context.store.materialize_verified(
            &format!("{assembly_prefix}/{}", document.relative_path),
            &document.sha256,
            document.bytes,
            &assembly_root.join(&document.relative_path),
        ).await?;
    }
    let datatype_policy = context.scratch.join("datatype-policy.json");
    context.store.materialize_verified(
        &context.request.datatype_policy_object_key,
        &context.request.datatype_policy_sha256,
        limit(options, "ontology-max-manifest-bytes")?,
        &datatype_policy,
    ).await?;
    let reasoner_output = context.scratch.join("reasoner-output");
    let reasoner_request = build_hermit_request(
        &assembly_path,
        &assembly_sha256,
        &datatype_policy,
        &reasoner_output,
        limit(options, "ontology-max-named-individuals")?,
        limit(options, "ontology-max-properties")?,
    )?;
    execute_hermit(
        &required_path(options, "java-executable").map_err(CloudOntologyError::Config)?,
        &required_path(options, "reasoner-adapter-jar").map_err(CloudOntologyError::Config)?,
        &required_value(options, "reasoner-adapter-sha256").map_err(CloudOntologyError::Config)?,
        &reasoner_request,
        limit(options, "ontology-reasoner-heap-mib")?,
        Duration::from_secs(limit(options, "ontology-reasoner-timeout-seconds")?),
    )?;
    for path in collect_files(&reasoner_output)? {
        if fs::metadata(&path)?.len() > artifact_limit {
            return Err(CloudOntologyError::Config("reasoner output exceeds ontology artifact ceiling".to_owned()));
        }
    }
    let root_output = context.scratch.join("qualification-root");
    let qualification_root = finalize_qualification(
        &context.request,
        &context.request_sha256,
        &assembly_path,
        &assembly_sha256,
        &reasoner_output,
        &root_output,
    )?;
    upload_tree(&context.store, &reasoner_output, &format!("{}/reasoner", context.prefix), options).await?;
    upload_tree(&context.store, &root_output, &format!("{}/root", context.prefix), options).await?;
    let qualification_sha256 = sha256_path(&qualification_root)?;
    let qualification_key = format!("{}/root/ontology-qualification-root.json", context.prefix);
    patch_status(
        options,
        NgkgSourceImportStatus {
            ontology_qualification_root_object_key: Some(qualification_key.clone()),
            ontology_qualification_root_sha256: Some(qualification_sha256.clone()),
            condition: Some("Owl2DlSnapshotQualifiedInactive".to_owned()),
            ..load_status(options).await?
        },
    ).await?;
    Ok(serde_json::json!({
        "status": "owl2-dl-snapshot-qualified-inactive",
        "ontologyQualificationRootObjectKey": qualification_key,
        "ontologyQualificationRootSha256": qualification_sha256
    }).to_string())
}

struct Context {
    store: Arc<ArtifactStore>,
    scratch: PathBuf,
    root: SemanticCompilationRoot,
    root_sha256: String,
    request: OntologyQualificationRequest,
    request_sha256: String,
    prefix: String,
}

impl Context {
    async fn new(options: &BTreeMap<String, String>) -> Result<Self, CloudOntologyError> {
        let scratch = required_path(options, "scratch-root").map_err(CloudOntologyError::Config)?;
        fs::create_dir_all(&scratch)?;
        let store = Arc::new(ArtifactStore::from_base_url(
            &required_value(options, "artifact-base-url").map_err(CloudOntologyError::Config)?,
        )?);
        let root_key = required_value(options, "semantic-compilation-root-object-key")
            .map_err(CloudOntologyError::Config)?;
        let root_sha256 = required_value(options, "semantic-compilation-root-sha256")
            .map_err(CloudOntologyError::Config)?;
        let root_path = scratch.join("semantic-compilation-root.json");
        store.materialize_verified(
            &root_key,
            &root_sha256,
            limit(options, "ontology-max-manifest-bytes")?,
            &root_path,
        ).await?;
        let root: SemanticCompilationRoot = read_json(&root_path)?;
        let request_key = required_value(options, "ontology-qualification-request-object-key")
            .map_err(CloudOntologyError::Config)?;
        let request_sha256 = required_value(options, "ontology-qualification-request-sha256")
            .map_err(CloudOntologyError::Config)?;
        let request_path = scratch.join("ontology-qualification-request.json");
        store.materialize_verified(
            &request_key,
            &request_sha256,
            limit(options, "ontology-max-manifest-bytes")?,
            &request_path,
        ).await?;
        let request: OntologyQualificationRequest = read_json(&request_path)?;
        ngkg_ontology_qualifier::validate_qualification_request(&root, &root_sha256, &request)?;
        let prefix = format!(
            "imports/{}/{}/{}/ontology-qualification",
            root.tenant_id, root.dataset_id, root.operation_id
        );
        Ok(Self { store, scratch, root, root_sha256, request, request_sha256, prefix })
    }
}

async fn upload_tree(
    store: &ArtifactStore,
    root: &Path,
    prefix: &str,
    options: &BTreeMap<String, String>,
) -> Result<(), CloudOntologyError> {
    for path in collect_files(root)? {
        let relative = path.strip_prefix(root)
            .map_err(|_| CloudOntologyError::Config("output escaped its root".to_owned()))?
            .to_str()
            .ok_or_else(|| CloudOntologyError::Config("output path is not UTF-8".to_owned()))?;
        store.put_file_immutable(
            &format!("{prefix}/{relative}"),
            &sha256_path(&path)?,
            &path,
            limit(options, "single-put-max-bytes")?,
            required_usize(options, "multipart-buffer-bytes").map_err(CloudOntologyError::Config)?,
            required_usize(options, "multipart-concurrency").map_err(CloudOntologyError::Config)?,
        ).await?;
    }
    Ok(())
}

fn collect_files(root: &Path) -> Result<Vec<PathBuf>, CloudOntologyError> {
    let mut pending = vec![root.to_owned()];
    let mut files = Vec::new();
    while let Some(directory) = pending.pop() {
        for entry in fs::read_dir(directory)? {
            let entry = entry?;
            let kind = entry.file_type()?;
            if kind.is_symlink() {
                return Err(CloudOntologyError::Config("output contains a symlink".to_owned()));
            }
            if kind.is_dir() { pending.push(entry.path()); }
            else if kind.is_file() { files.push(entry.path()); }
        }
    }
    files.sort();
    Ok(files)
}

fn parent_object_key(key: &str) -> Result<String, CloudOntologyError> {
    let (parent, _) = key.rsplit_once('/').ok_or_else(|| {
        CloudOntologyError::Config("object key has no parent".to_owned())
    })?;
    if parent.is_empty() || parent.contains("..") {
        return Err(CloudOntologyError::Config("unsafe object-key parent".to_owned()));
    }
    Ok(parent.to_owned())
}

fn limit(options: &BTreeMap<String, String>, name: &str) -> Result<u64, CloudOntologyError> {
    required_u64(options, name).map_err(CloudOntologyError::Config)
}

fn read_json<T: DeserializeOwned>(path: &Path) -> Result<T, CloudOntologyError> {
    Ok(serde_json::from_slice(&fs::read(path)?)?)
}

async fn load_status(options: &BTreeMap<String, String>) -> Result<NgkgSourceImportStatus, CloudOntologyError> {
    let namespace = required_value(options, "namespace").map_err(CloudOntologyError::Config)?;
    let name = required_value(options, "import-name").map_err(CloudOntologyError::Config)?;
    let api: Api<NgkgSourceImport> = Api::namespaced(Client::try_default().await?, &namespace);
    Ok(api.get(&name).await?.status.unwrap_or_default())
}

async fn patch_status(
    options: &BTreeMap<String, String>,
    status: NgkgSourceImportStatus,
) -> Result<(), CloudOntologyError> {
    let namespace = required_value(options, "namespace").map_err(CloudOntologyError::Config)?;
    let name = required_value(options, "import-name").map_err(CloudOntologyError::Config)?;
    let api: Api<NgkgSourceImport> = Api::namespaced(Client::try_default().await?, &namespace);
    api.patch_status(
        &name,
        &PatchParams::default(),
        &Patch::Merge(serde_json::json!({
            "apiVersion": "ngkg.io/v1alpha1",
            "kind": "NgkgSourceImport",
            "status": status
        })),
    ).await?;
    Ok(())
}
