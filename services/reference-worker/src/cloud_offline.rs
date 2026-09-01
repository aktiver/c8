//! Object-store and Kubernetes stages for exact offline OWL materialization.

use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};

use futures::{StreamExt, TryStreamExt, stream};
use kube::{
    Api, Client,
    api::{Patch, PatchParams},
};
use ngkg_artifact_store::ArtifactStore;
use ngkg_kube::{
    NgkgSourceImport, NgkgSourceImportStatus, source_import_status_apply_document,
};
use ngkg_offline_reasoner::{
    OfflinePartitionManifest, OfflineReasoningPlan, PlanLimits, VerifiedPartitionInput,
    finalize_offline_reasoning_verified, plan_exact_consequences, reduce_exact_partition,
    sha256_path,
};
use ngkg_ontology_qualifier::OntologyQualificationRoot;
use serde::de::DeserializeOwned;
use thiserror::Error;

use super::{reject_unknown, required_path, required_u64, required_usize, required_value};

#[derive(Debug, Error)]
pub enum CloudOfflineError {
    #[error("cloud offline reasoning configuration is invalid: {0}")]
    Config(String),
    #[error("cloud offline reasoning I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("cloud offline reasoning artifact access failed: {0}")]
    Artifact(#[from] ngkg_artifact_store::ArtifactStoreError),
    #[error("cloud offline reasoning failed: {0}")]
    Reasoning(#[from] ngkg_offline_reasoner::OfflineReasoningError),
    #[error("cloud offline reasoning JSON failed: {0}")]
    Json(#[from] serde_json::Error),
    #[error("cloud offline reasoning Kubernetes status failed: {0}")]
    Kubernetes(#[from] kube::Error),
}

const COMMON: &[&str] = &[
    "namespace",
    "import-name",
    "artifact-base-url",
    "scratch-root",
    "ontology-qualification-root-object-key",
    "ontology-qualification-root-sha256",
    "offline-finite-closure-object-key",
    "offline-max-manifest-bytes",
    "offline-max-artifact-bytes",
    "offline-max-finalizer-local-bytes",
    "single-put-max-bytes",
    "multipart-buffer-bytes",
    "multipart-concurrency",
];

pub async fn execute_plan(options: &BTreeMap<String, String>) -> Result<String, CloudOfflineError> {
    reject_stage_options(
        options,
        &[
            "offline-logical-partitions",
            "offline-max-consequences",
            "offline-plan-rows-in-memory",
            "offline-max-run-bytes",
        ],
    )?;
    let context = Context::new(options).await?;
    let closure = context.scratch.join("finite-closure.nt");
    context
        .store
        .materialize_verified(
            &required_value(options, "offline-finite-closure-object-key")
                .map_err(CloudOfflineError::Config)?,
            &context.root.finite_closure_sha256,
            limit(options, "offline-max-artifact-bytes")?,
            &closure,
        )
        .await?;
    let output = context.scratch.join("plan-output");
    let plan_path = plan_exact_consequences(
        &context.root,
        &context.root_sha256,
        &closure,
        &output,
        PlanLimits {
            logical_partitions: required_value(options, "offline-logical-partitions")
                .map_err(CloudOfflineError::Config)?
                .parse()
                .map_err(|_| {
                    CloudOfflineError::Config("offline-logical-partitions must be a u32".to_owned())
                })?,
            max_consequences: limit(options, "offline-max-consequences")?,
            rows_in_memory: required_usize(options, "offline-plan-rows-in-memory")
                .map_err(CloudOfflineError::Config)?,
            max_run_bytes: limit(options, "offline-max-run-bytes")?,
        },
    )?;
    upload_tree(
        &context.store,
        &output,
        &format!("{}/plan", context.prefix),
        options,
    )
    .await?;
    let plan_sha256 = sha256_path(&plan_path)?;
    let plan: OfflineReasoningPlan = read_json(&plan_path)?;
    let plan_key = format!("{}/plan/offline-reasoning-plan.json", context.prefix);
    patch_status(
        options,
        NgkgSourceImportStatus {
            offline_reasoning_plan_object_key: Some(plan_key.clone()),
            offline_reasoning_plan_sha256: Some(plan_sha256.clone()),
            offline_reasoning_partition_count: Some(plan.logical_partitions),
            condition: Some("OfflineReasoningPlanCompleteInactive".to_owned()),
            ..load_status(options).await?
        },
    )
    .await?;
    Ok(serde_json::json!({
        "status": "offline-reasoning-plan-complete-inactive",
        "planObjectKey": plan_key, "planSha256": plan_sha256,
        "logicalPartitions": plan.logical_partitions
    })
    .to_string())
}

pub async fn execute_partition(
    options: &BTreeMap<String, String>,
) -> Result<String, CloudOfflineError> {
    reject_stage_options(
        options,
        &[
            "offline-reasoning-plan-object-key",
            "offline-reasoning-plan-sha256",
            "completion-index",
            "offline-parquet-row-group-rows",
        ],
    )?;
    let context = Context::new(options).await?;
    let plan_path = context.scratch.join("offline-reasoning-plan.json");
    let plan_key = required_value(options, "offline-reasoning-plan-object-key")
        .map_err(CloudOfflineError::Config)?;
    let plan_sha256 = required_value(options, "offline-reasoning-plan-sha256")
        .map_err(CloudOfflineError::Config)?;
    context
        .store
        .materialize_verified(
            &plan_key,
            &plan_sha256,
            limit(options, "offline-max-manifest-bytes")?,
            &plan_path,
        )
        .await?;
    let plan: OfflineReasoningPlan = read_json(&plan_path)?;
    let partition = required_value(options, "completion-index")
        .map_err(CloudOfflineError::Config)?
        .parse::<u32>()
        .map_err(|_| CloudOfflineError::Config("completion-index must be a u32".to_owned()))?;
    if partition >= plan.logical_partitions {
        return Err(CloudOfflineError::Config(
            "completion-index is outside the plan".to_owned(),
        ));
    }
    let plan_prefix = parent_object_key(&plan_key)?;
    let run_root = context.scratch.join("downloaded-runs");
    for run in plan
        .partition_runs
        .get(&partition)
        .cloned()
        .unwrap_or_default()
    {
        context
            .store
            .materialize_verified(
                &format!("{plan_prefix}/{}", run.relative_path),
                &run.sha256,
                limit(options, "offline-max-artifact-bytes")?.min(run.bytes),
                &run_root.join(run.relative_path),
            )
            .await?;
    }
    let output = context.scratch.join("partition-output");
    let manifest = reduce_exact_partition(
        &plan_path,
        &plan_sha256,
        &context.root,
        &context.root_sha256,
        &run_root,
        partition,
        &output,
        required_usize(options, "offline-parquet-row-group-rows")
            .map_err(CloudOfflineError::Config)?,
    )?;
    upload_tree(
        &context.store,
        &output,
        &format!("{}/partitions/{partition:05}", context.prefix),
        options,
    )
    .await?;
    Ok(serde_json::json!({
        "status": "offline-reasoning-partition-complete-inactive",
        "partitionIndex": partition, "manifestSha256": sha256_path(&manifest)?
    })
    .to_string())
}

pub async fn execute_finalize(
    options: &BTreeMap<String, String>,
) -> Result<String, CloudOfflineError> {
    reject_stage_options(
        options,
        &[
            "offline-reasoning-plan-object-key",
            "offline-reasoning-plan-sha256",
            "offline-finalize-concurrency",
        ],
    )?;
    let context = Context::new(options).await?;
    let plan_path = context.scratch.join("offline-reasoning-plan.json");
    let plan_key = required_value(options, "offline-reasoning-plan-object-key")
        .map_err(CloudOfflineError::Config)?;
    let plan_sha256 = required_value(options, "offline-reasoning-plan-sha256")
        .map_err(CloudOfflineError::Config)?;
    context
        .store
        .materialize_verified(
            &plan_key,
            &plan_sha256,
            limit(options, "offline-max-manifest-bytes")?,
            &plan_path,
        )
        .await?;
    let plan: OfflineReasoningPlan = read_json(&plan_path)?;
    let concurrency = required_usize(options, "offline-finalize-concurrency")
        .map_err(CloudOfflineError::Config)?;
    if concurrency == 0 {
        return Err(CloudOfflineError::Config(
            "finalize concurrency must be positive".to_owned(),
        ));
    }
    let mut inputs = Vec::new();
    let mut remote_verifications = Vec::new();
    let mut local_equality_bytes = 0_u64;
    for partition in 0..plan.logical_partitions {
        let manifest_key = format!(
            "{}/partitions/{partition:05}/offline-partition.json",
            context.prefix
        );
        let local_root = context.scratch.join(format!("partitions/{partition:05}"));
        let manifest_path = local_root.join("offline-partition.json");
        context
            .store
            .materialize_unverified_bounded(
                &manifest_key,
                limit(options, "offline-max-manifest-bytes")?,
                &manifest_path,
            )
            .await?;
        let manifest: OfflinePartitionManifest = read_json(&manifest_path)?;
        let manifest_sha256 = sha256_path(&manifest_path)?;
        let mut same_as_path = None;
        for artifact in &manifest.artifacts {
            let key = format!(
                "{}/partitions/{partition:05}/{}",
                context.prefix, artifact.relative_path
            );
            if artifact.relative_path == "sameas-membership.tsv" {
                local_equality_bytes = local_equality_bytes
                    .checked_add(artifact.bytes)
                    .ok_or_else(|| {
                        CloudOfflineError::Config("finalizer local byte count overflow".to_owned())
                    })?;
                if local_equality_bytes > limit(options, "offline-max-finalizer-local-bytes")? {
                    return Err(CloudOfflineError::Config(
                        "equality finalizer scratch ceiling exceeded".to_owned(),
                    ));
                }
                let path = local_root.join(&artifact.relative_path);
                context
                    .store
                    .materialize_verified(&key, &artifact.sha256, artifact.bytes, &path)
                    .await?;
                same_as_path = Some(path);
            } else {
                remote_verifications.push((key, artifact.sha256.clone(), artifact.bytes));
            }
        }
        inputs.push(VerifiedPartitionInput {
            manifest,
            manifest_object_key: manifest_key,
            manifest_sha256,
            same_as_membership_path: same_as_path.ok_or_else(|| {
                CloudOfflineError::Config("partition lacks equality membership".to_owned())
            })?,
        });
    }
    stream::iter(
        remote_verifications
            .into_iter()
            .map(|(key, sha256, bytes)| {
                let store = Arc::clone(&context.store);
                async move {
                    store.verify_remote(&key, &sha256, bytes).await?;
                    Ok::<_, CloudOfflineError>(())
                }
            }),
    )
    .buffer_unordered(concurrency)
    .try_collect::<Vec<_>>()
    .await?;
    let output = context.scratch.join("final-output");
    let root_path = finalize_offline_reasoning_verified(
        &plan_path,
        &plan_sha256,
        &context.root,
        &context.root_sha256,
        &inputs,
        &output,
    )?;
    upload_tree(
        &context.store,
        &output,
        &format!("{}/root", context.prefix),
        options,
    )
    .await?;
    let root_sha256 = sha256_path(&root_path)?;
    let root_key = format!("{}/root/offline-reasoning-root.json", context.prefix);
    patch_status(
        options,
        NgkgSourceImportStatus {
            offline_reasoning_root_object_key: Some(root_key.clone()),
            offline_reasoning_root_sha256: Some(root_sha256.clone()),
            condition: Some("OfflineReasoningCompleteInactive".to_owned()),
            ..load_status(options).await?
        },
    )
    .await?;
    Ok(serde_json::json!({
        "status": "offline-reasoning-complete-inactive",
        "rootObjectKey": root_key, "rootSha256": root_sha256
    })
    .to_string())
}

struct Context {
    store: Arc<ArtifactStore>,
    scratch: PathBuf,
    root: OntologyQualificationRoot,
    root_sha256: String,
    prefix: String,
}

impl Context {
    async fn new(options: &BTreeMap<String, String>) -> Result<Self, CloudOfflineError> {
        let scratch = required_path(options, "scratch-root").map_err(CloudOfflineError::Config)?;
        fs::create_dir_all(&scratch)?;
        let store = Arc::new(ArtifactStore::from_base_url(
            &required_value(options, "artifact-base-url").map_err(CloudOfflineError::Config)?,
        )?);
        let root_key = required_value(options, "ontology-qualification-root-object-key")
            .map_err(CloudOfflineError::Config)?;
        let root_sha256 = required_value(options, "ontology-qualification-root-sha256")
            .map_err(CloudOfflineError::Config)?;
        let root_path = scratch.join("ontology-qualification-root.json");
        store
            .materialize_verified(
                &root_key,
                &root_sha256,
                limit(options, "offline-max-manifest-bytes")?,
                &root_path,
            )
            .await?;
        let root: OntologyQualificationRoot = read_json(&root_path)?;
        ngkg_offline_reasoner::validate_qualification(&root)?;
        let prefix = format!(
            "imports/{}/{}/{}/offline-reasoning",
            root.tenant_id, root.dataset_id, root.operation_id
        );
        Ok(Self {
            store,
            scratch,
            root,
            root_sha256,
            prefix,
        })
    }
}

fn reject_stage_options(
    options: &BTreeMap<String, String>,
    stage: &[&str],
) -> Result<(), CloudOfflineError> {
    let mut allowed = COMMON.to_vec();
    allowed.extend_from_slice(stage);
    reject_unknown(options, &allowed).map_err(CloudOfflineError::Config)
}
fn limit(options: &BTreeMap<String, String>, name: &str) -> Result<u64, CloudOfflineError> {
    required_u64(options, name).map_err(CloudOfflineError::Config)
}
fn read_json<T: DeserializeOwned>(path: &Path) -> Result<T, CloudOfflineError> {
    Ok(serde_json::from_slice(&fs::read(path)?)?)
}
fn parent_object_key(key: &str) -> Result<String, CloudOfflineError> {
    let (parent, _) = key
        .rsplit_once('/')
        .ok_or_else(|| CloudOfflineError::Config("object key has no parent".to_owned()))?;
    if parent.is_empty() || parent.contains("..") {
        return Err(CloudOfflineError::Config(
            "unsafe object-key parent".to_owned(),
        ));
    }
    Ok(parent.to_owned())
}
fn collect_files(root: &Path) -> Result<Vec<PathBuf>, CloudOfflineError> {
    let mut pending = vec![root.to_owned()];
    let mut files = Vec::new();
    while let Some(directory) = pending.pop() {
        for entry in fs::read_dir(directory)? {
            let entry = entry?;
            let kind = entry.file_type()?;
            if kind.is_symlink() {
                return Err(CloudOfflineError::Config(
                    "output contains a symlink".to_owned(),
                ));
            }
            if kind.is_dir() {
                pending.push(entry.path());
            } else if kind.is_file() {
                files.push(entry.path());
            }
        }
    }
    files.sort();
    Ok(files)
}
async fn upload_tree(
    store: &ArtifactStore,
    root: &Path,
    prefix: &str,
    options: &BTreeMap<String, String>,
) -> Result<(), CloudOfflineError> {
    for path in collect_files(root)? {
        let relative = path
            .strip_prefix(root)
            .map_err(|_| CloudOfflineError::Config("output escaped its root".to_owned()))?
            .to_str()
            .ok_or_else(|| CloudOfflineError::Config("output path is not UTF-8".to_owned()))?;
        store
            .put_file_immutable(
                &format!("{prefix}/{relative}"),
                &sha256_path(&path)?,
                &path,
                limit(options, "single-put-max-bytes")?,
                required_usize(options, "multipart-buffer-bytes")
                    .map_err(CloudOfflineError::Config)?,
                required_usize(options, "multipart-concurrency")
                    .map_err(CloudOfflineError::Config)?,
            )
            .await?;
    }
    Ok(())
}
async fn load_status(
    options: &BTreeMap<String, String>,
) -> Result<NgkgSourceImportStatus, CloudOfflineError> {
    let api: Api<NgkgSourceImport> = Api::namespaced(
        Client::try_default().await?,
        &required_value(options, "namespace").map_err(CloudOfflineError::Config)?,
    );
    Ok(api
        .get(&required_value(options, "import-name").map_err(CloudOfflineError::Config)?)
        .await?
        .status
        .unwrap_or_default())
}
async fn patch_status(
    options: &BTreeMap<String, String>,
    status: NgkgSourceImportStatus,
) -> Result<(), CloudOfflineError> {
    let namespace = required_value(options, "namespace").map_err(CloudOfflineError::Config)?;
    let name = required_value(options, "import-name").map_err(CloudOfflineError::Config)?;
    let api: Api<NgkgSourceImport> = Api::namespaced(Client::try_default().await?, &namespace);
    let document = source_import_status_apply_document(
        &name,
        &status,
        &[
            "offlineReasoningPlanObjectKey", "offlineReasoningPlanSha256",
            "offlineReasoningPartitionCount", "offlineReasoningRootObjectKey",
            "offlineReasoningRootSha256",
        ],
    )?;
    api.patch_status(
        &name,
        &PatchParams::apply("ngkg-offline-reasoning-worker"),
        &Patch::Apply(document),
    )
    .await?;
    Ok(())
}
