//! Phase 40.13.15 all-roots verification, certification, and atomic activation.

use std::{collections::BTreeMap, env, fs, path::{Path, PathBuf}, sync::Arc};

use futures::{StreamExt, TryStreamExt, stream};
use kube::{Api, Client, api::{Patch, PatchParams}};
use ngkg_artifact_store::ArtifactStore;
use ngkg_catalog::{
    CloudSnapshotActivation, CommitCloudSnapshotActivation, OperationRepository,
};
use ngkg_kube::{NgkgSourceImport, NgkgSourceImportStatus};
use ngkg_offline_reasoner::{OfflinePartitionManifest, OfflineReasoningRoot};
use ngkg_ontology_qualifier::{OntologyQualificationRequest, OntologyQualificationRoot};
use ngkg_semantic_compiler::{SemanticCompilationRoot, SemanticPartitionManifest};
use ngkg_snapshot_activation::{
    ActivationInputs, ActivationRootReference, SnapshotActivationManifest,
    build_serving_artifacts,
};
use serde::de::DeserializeOwned;
use sqlx::postgres::PgPoolOptions;
use thiserror::Error;

use super::{reject_unknown, required_path, required_u64, required_usize, required_value};

#[derive(Debug, Error)]
pub enum CloudActivationError {
    #[error("cloud activation configuration is invalid: {0}")]
    Config(String),
    #[error("cloud activation I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("cloud activation artifact access failed: {0}")]
    Artifact(#[from] ngkg_artifact_store::ArtifactStoreError),
    #[error("cloud activation contract failed: {0}")]
    Activation(#[from] ngkg_snapshot_activation::ActivationError),
    #[error("cloud activation JSON failed: {0}")]
    Json(#[from] serde_json::Error),
    #[error("cloud activation catalog failed: {0}")]
    Catalog(#[from] ngkg_catalog::CatalogError),
    #[error("cloud activation database failed: {0}")]
    Database(#[from] sqlx::Error),
    #[error("cloud activation Kubernetes status failed: {0}")]
    Kubernetes(#[from] kube::Error),
}

const ALLOWED: &[&str] = &[
    "namespace", "import-name", "artifact-base-url", "scratch-root",
    "semantic-compilation-root-object-key", "semantic-compilation-root-sha256",
    "ontology-qualification-request-object-key", "ontology-qualification-request-sha256",
    "ontology-qualification-root-object-key", "ontology-qualification-root-sha256",
    "offline-reasoning-root-object-key", "offline-reasoning-root-sha256",
    "activation-max-manifest-bytes", "activation-max-partition-bytes",
    "activation-max-query-dataset-bytes", "activation-verify-concurrency",
    "single-put-max-bytes", "multipart-buffer-bytes", "multipart-concurrency",
];

pub async fn execute(options: &BTreeMap<String, String>) -> Result<String, CloudActivationError> {
    reject_unknown(options, ALLOWED).map_err(CloudActivationError::Config)?;
    let scratch = required_path(options, "scratch-root").map_err(CloudActivationError::Config)?;
    fs::create_dir_all(&scratch)?;
    let store = Arc::new(ArtifactStore::from_base_url(
        &required_value(options, "artifact-base-url").map_err(CloudActivationError::Config)?,
    )?);
    let manifest_limit = limit(options, "activation-max-manifest-bytes")?;
    let semantic_ref = root_ref(options, "semantic-compilation-root")?;
    let request_ref = root_ref(options, "ontology-qualification-request")?;
    let qualification_ref = root_ref(options, "ontology-qualification-root")?;
    let offline_ref = root_ref(options, "offline-reasoning-root")?;
    let semantic: SemanticCompilationRoot = materialize_json(
        &store, &semantic_ref, manifest_limit, &scratch.join("semantic-root.json"),
    ).await?;
    let qualification_request: OntologyQualificationRequest = materialize_json(
        &store, &request_ref, manifest_limit, &scratch.join("qualification-request.json"),
    ).await?;
    let qualification: OntologyQualificationRoot = materialize_json(
        &store, &qualification_ref, manifest_limit, &scratch.join("qualification-root.json"),
    ).await?;
    let offline: OfflineReasoningRoot = materialize_json(
        &store, &offline_ref, manifest_limit, &scratch.join("offline-root.json"),
    ).await?;

    let database_url = env::var("NGKG_DATABASE_URL")
        .map_err(|_| CloudActivationError::Config("NGKG_DATABASE_URL is required".to_owned()))?;
    let pool = PgPoolOptions::new().max_connections(2).connect(&database_url).await?;
    let catalog = OperationRepository::new(pool);
    catalog.ready().await?;
    let durable = catalog.get_compilation(semantic.tenant_id, semantic.operation_id).await?;
    if durable.operation.dataset_id != semantic.dataset_id
        || durable.operation.target_snapshot_id != semantic.snapshot_id
        || durable.request.parent_snapshot_id != qualification_request_parent(options).await?
        || durable.request.bundle_object_key.as_str() != request_ref.object_key.as_str()
        || hex::encode(durable.request.bundle_sha256) != request_ref.sha256
    {
        return Err(CloudActivationError::Config(
            "catalog operation differs from the cloud compiler snapshot".to_owned(),
        ));
    }
    let inputs = ActivationInputs {
        semantic_root: &semantic,
        semantic_root_ref: semantic_ref,
        qualification_request: &qualification_request,
        qualification_request_ref: request_ref,
        qualification_root: &qualification,
        qualification_root_ref: qualification_ref,
        offline_root: &offline,
        offline_root_ref: offline_ref,
        identity_namespace: durable.identity_namespace,
        parent_snapshot_id: durable.request.parent_snapshot_id,
    };
    ngkg_snapshot_activation::validate_inputs(&inputs)?;
    let concurrency = required_usize(options, "activation-verify-concurrency")
        .map_err(CloudActivationError::Config)?;
    if concurrency == 0 {
        return Err(CloudActivationError::Config(
            "activation verification concurrency must be positive".to_owned(),
        ));
    }
    let facts = materialize_semantic_partitions(
        Arc::clone(&store), &semantic, &scratch, manifest_limit,
        limit(options, "activation-max-partition-bytes")?, concurrency,
    ).await?;
    verify_offline_partitions(
        Arc::clone(&store), &offline, &scratch, manifest_limit,
        limit(options, "activation-max-partition-bytes")?, concurrency,
    ).await?;

    let qualification_prefix = parent_object_key(&inputs.qualification_root_ref.object_key)?
        .strip_suffix("/root")
        .ok_or_else(|| CloudActivationError::Config("qualification root key is noncanonical".to_owned()))?
        .to_owned();
    let reasoner_root = scratch.join("reasoner-input");
    fs::create_dir_all(&reasoner_root)?;
    let reasoner_files = [
        ("finite-closure.nt", &qualification.finite_closure_sha256),
        ("owl-signature.json", &qualification.owl_signature_sha256),
        ("owl-profile-qualification.json", &qualification.owl_profile_qualification_sha256),
        ("owl-consistency-qualification.json", &qualification.owl_consistency_qualification_sha256),
    ];
    for (name, sha256) in reasoner_files {
        store.materialize_verified(
            &format!("{qualification_prefix}/reasoner/{name}"), sha256,
            limit(options, "activation-max-partition-bytes")?, &reasoner_root.join(name),
        ).await?;
    }
    let datatype_policy = reasoner_root.join("datatype-policy.json");
    store.materialize_verified(
        &qualification_request.datatype_policy_object_key,
        &qualification_request.datatype_policy_sha256,
        manifest_limit,
        &datatype_policy,
    ).await?;
    let output = scratch.join("activation-output");
    let artifacts = build_serving_artifacts(
        &inputs, &facts, &reasoner_root.join("finite-closure.nt"),
        &reasoner_root.join("owl-signature.json"), &datatype_policy,
        &reasoner_root.join("owl-profile-qualification.json"),
        &reasoner_root.join("owl-consistency-qualification.json"), &output,
    )?;
    let query_bytes = fs::metadata(output.join("data/query-dataset.nq"))?.len();
    if query_bytes > limit(options, "activation-max-query-dataset-bytes")? {
        return Err(CloudActivationError::Config(
            "scalar compatibility dataset exceeds its activation ceiling; Phase 16 distributed query activation is required"
                .to_owned(),
        ));
    }
    let prefix = format!(
        "imports/{}/{}/{}/activation/snapshot",
        semantic.tenant_id, semantic.dataset_id, semantic.operation_id
    );
    upload_tree(&store, &output, &prefix, options).await?;
    let activation: SnapshotActivationManifest = read_json(&artifacts.activation_manifest_path)?;
    let activation_key = format!("{prefix}/activation/snapshot-activation.json");
    let activation_sha256 = sha256_path(&artifacts.activation_manifest_path)?;
    let reference_key = format!("{prefix}/snapshot-manifest.json");
    let reference_sha256 = decode_sha256(&activation.reference_manifest_sha256)?;
    let outcome = catalog.commit_cloud_snapshot_activation(
        semantic.tenant_id,
        semantic.operation_id,
        &CommitCloudSnapshotActivation {
            activation: CloudSnapshotActivation {
                activation_manifest_object_key: activation_key.clone(),
                activation_manifest_sha256: activation_sha256.clone(),
                semantic_root_object_key: activation.semantic_root.object_key,
                semantic_root_sha256: activation.semantic_root.sha256,
                qualification_root_object_key: activation.qualification_root.object_key,
                qualification_root_sha256: activation.qualification_root.sha256,
                offline_root_object_key: activation.offline_reasoning_root.object_key,
                offline_root_sha256: activation.offline_reasoning_root.sha256,
                semantic_content_sha256: activation.semantic_content_sha256,
                authorized_graph_set_sha256: activation.authorized_graph_set_sha256,
                datatype_policy_sha256: activation.datatype_policy_sha256,
                ontology_sha256: activation.synthetic_snapshot_ontology_sha256,
                finite_closure_sha256: activation.finite_closure_sha256,
                proof_support_root_sha256: activation.proof_support_root_sha256,
                query_dataset_sha256: activation.query_dataset_sha256,
                query_dataset_bytes: i64::try_from(activation.query_dataset_bytes)
                    .map_err(|_| CloudActivationError::Config("query dataset size overflow".to_owned()))?,
                fact_count: i64::try_from(activation.fact_count)
                    .map_err(|_| CloudActivationError::Config("fact count overflow".to_owned()))?,
                consequence_count: i64::try_from(activation.consequence_count)
                    .map_err(|_| CloudActivationError::Config("consequence count overflow".to_owned()))?,
                semantic_partition_count: i32::try_from(activation.semantic_partition_count)
                    .map_err(|_| CloudActivationError::Config("semantic partition count overflow".to_owned()))?,
                reasoning_partition_count: i32::try_from(activation.reasoning_partition_count)
                    .map_err(|_| CloudActivationError::Config("reasoning partition count overflow".to_owned()))?,
            },
            reference_manifest_object_key: reference_key,
            reference_manifest_sha256: reference_sha256,
        },
        "cloud-snapshot-activation-worker",
    ).await?;
    patch_status(options, NgkgSourceImportStatus {
        snapshot_activation_manifest_object_key: Some(activation_key),
        snapshot_activation_manifest_sha256: Some(activation_sha256),
        snapshot_publication_state: Some(outcome.snapshot.state.clone()),
        condition: Some(if outcome.published {
            "SnapshotPublishedAtomically"
        } else if outcome.publication_conflict {
            "SnapshotCertifiedPublicationConflict"
        } else {
            "SnapshotCertifiedInactive"
        }.to_owned()),
        ..load_status(options).await?
    }).await?;
    Ok(serde_json::json!({
        "status": outcome.snapshot.state,
        "snapshotId": outcome.snapshot.snapshot_id,
        "activationManifestObjectKey": activation_key,
        "activationManifestSha256": activation_sha256,
        "published": outcome.published,
        "publicationConflict": outcome.publication_conflict
    }).to_string())
}

async fn materialize_semantic_partitions(
    store: Arc<ArtifactStore>, root: &SemanticCompilationRoot, scratch: &Path,
    manifest_limit: u64, artifact_limit: u64, concurrency: usize,
) -> Result<Vec<PathBuf>, CloudActivationError> {
    let jobs = root.partitions.iter().map(|reference| {
        let store = Arc::clone(&store);
        let local = scratch.join(format!("semantic/{:05}", reference.partition_index));
        let manifest_key = reference.manifest_path.clone();
        let manifest_sha = reference.manifest_sha256.clone();
        async move {
            fs::create_dir_all(&local)?;
            let manifest_path = local.join("semantic-partition.json");
            store.materialize_verified(&manifest_key, &manifest_sha, manifest_limit, &manifest_path).await?;
            let manifest: SemanticPartitionManifest = read_json(&manifest_path)?;
            let facts = manifest.artifacts.iter().find(|artifact| artifact.relative_path == "facts.nq")
                .ok_or_else(|| CloudActivationError::Config("semantic partition lacks facts.nq".to_owned()))?;
            if facts.bytes > artifact_limit {
                return Err(CloudActivationError::Config("semantic facts partition exceeds ceiling".to_owned()));
            }
            let prefix = parent_object_key(&manifest_key)?;
            let path = local.join("facts.nq");
            store.materialize_verified(&format!("{prefix}/facts.nq"), &facts.sha256, facts.bytes, &path).await?;
            Ok::<_, CloudActivationError>((manifest.partition_index, path))
        }
    });
    let mut files = stream::iter(jobs).buffer_unordered(concurrency).try_collect::<Vec<_>>().await?;
    files.sort_by_key(|(index, _)| *index);
    if files.iter().enumerate().any(|(index, (actual, _))| usize::try_from(*actual).ok() != Some(index)) {
        return Err(CloudActivationError::Config("semantic partition materialization is incomplete".to_owned()));
    }
    Ok(files.into_iter().map(|(_, path)| path).collect())
}

async fn verify_offline_partitions(
    store: Arc<ArtifactStore>, root: &OfflineReasoningRoot, scratch: &Path,
    manifest_limit: u64, artifact_limit: u64, concurrency: usize,
) -> Result<(), CloudActivationError> {
    let mut checks = Vec::new();
    for reference in &root.partitions {
        let local = scratch.join(format!("offline/{:05}.json", reference.partition_index));
        if let Some(parent) = local.parent() { fs::create_dir_all(parent)?; }
        store.materialize_verified(&reference.manifest_path, &reference.manifest_sha256, manifest_limit, &local).await?;
        let manifest: OfflinePartitionManifest = read_json(&local)?;
        if manifest.partition_index != reference.partition_index
            || manifest.consequence_count != reference.consequence_count
        {
            return Err(CloudActivationError::Config("offline partition identity mismatch".to_owned()));
        }
        let prefix = parent_object_key(&reference.manifest_path)?;
        for artifact in manifest.artifacts {
            if artifact.bytes > artifact_limit {
                return Err(CloudActivationError::Config("offline artifact exceeds ceiling".to_owned()));
            }
            checks.push((format!("{prefix}/{}", artifact.relative_path), artifact.sha256, artifact.bytes));
        }
    }
    stream::iter(checks.into_iter().map(|(key, sha256, bytes)| {
        let store = Arc::clone(&store);
        async move { store.verify_remote(&key, &sha256, bytes).await?; Ok::<_, CloudActivationError>(()) }
    })).buffer_unordered(concurrency).try_collect::<Vec<_>>().await?;
    Ok(())
}

async fn materialize_json<T: DeserializeOwned>(
    store: &ArtifactStore, reference: &ActivationRootReference, limit: u64, path: &Path,
) -> Result<T, CloudActivationError> {
    store.materialize_verified(&reference.object_key, &reference.sha256, limit, path).await?;
    read_json(path)
}

fn root_ref(options: &BTreeMap<String, String>, prefix: &str) -> Result<ActivationRootReference, CloudActivationError> {
    Ok(ActivationRootReference {
        object_key: required_value(options, &format!("{prefix}-object-key")).map_err(CloudActivationError::Config)?,
        sha256: required_value(options, &format!("{prefix}-sha256")).map_err(CloudActivationError::Config)?,
    })
}

async fn qualification_request_parent(options: &BTreeMap<String, String>) -> Result<Option<uuid::Uuid>, CloudActivationError> {
    let status = load_status(options).await?;
    let _ = status;
    let api: Api<NgkgSourceImport> = Api::namespaced(
        Client::try_default().await?,
        &required_value(options, "namespace").map_err(CloudActivationError::Config)?,
    );
    Ok(api.get(&required_value(options, "import-name").map_err(CloudActivationError::Config)?).await?.spec.parent_snapshot_id)
}

fn limit(options: &BTreeMap<String, String>, name: &str) -> Result<u64, CloudActivationError> {
    required_u64(options, name).map_err(CloudActivationError::Config)
}
fn read_json<T: DeserializeOwned>(path: &Path) -> Result<T, CloudActivationError> {
    Ok(serde_json::from_slice(&fs::read(path)?)?)
}
fn decode_sha256(value: &str) -> Result<[u8; 32], CloudActivationError> {
    let bytes = hex::decode(value).map_err(|_| CloudActivationError::Config("invalid SHA-256".to_owned()))?;
    bytes.try_into().map_err(|_| CloudActivationError::Config("invalid SHA-256 length".to_owned()))
}
fn sha256_path(path: &Path) -> Result<String, CloudActivationError> {
    ngkg_offline_reasoner::sha256_path(path).map_err(|error| CloudActivationError::Config(error.to_string()))
}
fn parent_object_key(key: &str) -> Result<String, CloudActivationError> {
    let (parent, _) = key.rsplit_once('/').ok_or_else(|| CloudActivationError::Config("object key has no parent".to_owned()))?;
    if parent.is_empty() || parent.contains("..") { return Err(CloudActivationError::Config("unsafe object-key parent".to_owned())); }
    Ok(parent.to_owned())
}
fn collect_files(root: &Path) -> Result<Vec<PathBuf>, CloudActivationError> {
    let mut pending = vec![root.to_owned()]; let mut files = Vec::new();
    while let Some(directory) = pending.pop() {
        for entry in fs::read_dir(directory)? { let entry = entry?; let kind = entry.file_type()?;
            if kind.is_symlink() { return Err(CloudActivationError::Config("activation output contains a symlink".to_owned())); }
            if kind.is_dir() { pending.push(entry.path()); } else if kind.is_file() { files.push(entry.path()); }
        }
    }
    files.sort(); Ok(files)
}
async fn upload_tree(store: &ArtifactStore, root: &Path, prefix: &str, options: &BTreeMap<String, String>) -> Result<(), CloudActivationError> {
    for path in collect_files(root)? {
        let relative = path.strip_prefix(root).map_err(|_| CloudActivationError::Config("activation output escaped root".to_owned()))?
            .to_str().ok_or_else(|| CloudActivationError::Config("activation output path is not UTF-8".to_owned()))?;
        store.put_file_immutable(&format!("{prefix}/{relative}"), &sha256_path(&path)?, &path,
            limit(options, "single-put-max-bytes")?, required_usize(options, "multipart-buffer-bytes").map_err(CloudActivationError::Config)?,
            required_usize(options, "multipart-concurrency").map_err(CloudActivationError::Config)?).await?;
    }
    Ok(())
}
async fn load_status(options: &BTreeMap<String, String>) -> Result<NgkgSourceImportStatus, CloudActivationError> {
    let api: Api<NgkgSourceImport> = Api::namespaced(Client::try_default().await?, &required_value(options, "namespace").map_err(CloudActivationError::Config)?);
    Ok(api.get(&required_value(options, "import-name").map_err(CloudActivationError::Config)?).await?.status.unwrap_or_default())
}
async fn patch_status(options: &BTreeMap<String, String>, status: NgkgSourceImportStatus) -> Result<(), CloudActivationError> {
    let namespace = required_value(options, "namespace").map_err(CloudActivationError::Config)?;
    let name = required_value(options, "import-name").map_err(CloudActivationError::Config)?;
    let api: Api<NgkgSourceImport> = Api::namespaced(Client::try_default().await?, &namespace);
    api.patch_status(&name, &PatchParams::default(), &Patch::Merge(serde_json::json!({
        "apiVersion": "ngkg.io/v1alpha1", "kind": "NgkgSourceImport", "status": status
    }))).await?; Ok(())
}
