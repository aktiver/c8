//! Object-store stages for distributed semantic compilation of cloud RDF fragments.

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
use ngkg_semantic_compiler::{
    CompilerHandoffManifest, DictionaryManifest, FragmentMapManifest, MapLimits, ReduceLimits,
    SemanticCompilationRoot, SemanticPartitionManifest, finalize_committed_semantic_root,
    finalize_dictionary, map_fragment, reduce_partition, sha256_path,
};
use serde::de::DeserializeOwned;
use thiserror::Error;

use super::{reject_unknown, required_path, required_u64, required_usize, required_value};

#[derive(Debug, Error)]
pub enum CloudSemanticError {
    #[error("cloud semantic configuration is invalid: {0}")]
    Config(String),
    #[error("cloud semantic I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("cloud semantic artifact access failed: {0}")]
    Artifact(#[from] ngkg_artifact_store::ArtifactStoreError),
    #[error("cloud semantic compilation failed: {0}")]
    Compiler(#[from] ngkg_semantic_compiler::SemanticCompilerError),
    #[error("cloud semantic JSON failed: {0}")]
    Json(#[from] serde_json::Error),
    #[error("cloud semantic Kubernetes status failed: {0}")]
    Kubernetes(#[from] kube::Error),
}

pub async fn execute_map(options: &BTreeMap<String, String>) -> Result<String, CloudSemanticError> {
    reject_unknown(
        options,
        &[
            "namespace",
            "import-name",
            "artifact-base-url",
            "scratch-root",
            "compiler-handoff-object-key",
            "compiler-handoff-sha256",
            "semantic-max-manifest-bytes",
            "single-put-max-bytes",
            "multipart-buffer-bytes",
            "multipart-concurrency",
            "completion-index",
            "semantic-max-fragment-bytes",
            "semantic-max-fragment-quads",
            "semantic-map-rows-in-memory",
            "semantic-map-worker-threads",
            "semantic-max-run-bytes",
        ],
    )
    .map_err(CloudSemanticError::Config)?;
    let context = Context::new(options).await?;
    let ordinal = required_value(options, "completion-index")
        .map_err(CloudSemanticError::Config)?
        .parse::<u32>()
        .map_err(|_| CloudSemanticError::Config("completion-index must be a u32".to_owned()))?;
    let fragment = context
        .handoff
        .fragments
        .get(usize::try_from(ordinal).unwrap_or(usize::MAX))
        .filter(|value| value.ordinal == ordinal)
        .ok_or_else(|| CloudSemanticError::Config("completion index is absent".to_owned()))?;
    let fragment_path = context.scratch.join("decoded-fragment.nq");
    context
        .store
        .materialize_verified(
            &fragment.object_key,
            &fragment.sha256,
            required_u64(options, "semantic-max-fragment-bytes")
                .map_err(CloudSemanticError::Config)?,
            &fragment_path,
        )
        .await?;
    let output = context.scratch.join("map-output");
    let manifest = map_fragment(
        &context.handoff,
        &context.handoff_sha256,
        ordinal,
        &fragment_path,
        &output,
        MapLimits {
            max_fragment_bytes: required_u64(options, "semantic-max-fragment-bytes")
                .map_err(CloudSemanticError::Config)?,
            max_fragment_quads: required_u64(options, "semantic-max-fragment-quads")
                .map_err(CloudSemanticError::Config)?,
            max_rows_in_memory: required_usize(options, "semantic-map-rows-in-memory")
                .map_err(CloudSemanticError::Config)?,
            max_run_bytes: required_u64(options, "semantic-max-run-bytes")
                .map_err(CloudSemanticError::Config)?,
            worker_threads: required_usize(options, "semantic-map-worker-threads")
                .map_err(CloudSemanticError::Config)?,
        },
    )?;
    upload_tree(
        &context,
        &output,
        &format!("{}/maps/{ordinal:08}", context.prefix),
        options,
    )
    .await?;
    Ok(serde_json::json!({
        "status": "cloud-semantic-map-complete",
        "fragmentOrdinal": ordinal,
        "manifestObjectKey": format!("{}/maps/{ordinal:08}/fragment-map.json", context.prefix),
        "manifestSha256": sha256_path(&manifest)?
    })
    .to_string())
}

pub async fn execute_dictionary(
    options: &BTreeMap<String, String>,
) -> Result<String, CloudSemanticError> {
    reject_unknown(
        options,
        &[
            "namespace",
            "import-name",
            "artifact-base-url",
            "scratch-root",
            "compiler-handoff-object-key",
            "compiler-handoff-sha256",
            "semantic-max-manifest-bytes",
            "single-put-max-bytes",
            "multipart-buffer-bytes",
            "multipart-concurrency",
            "semantic-max-run-bytes",
            "semantic-max-dictionary-bytes",
        ],
    )
    .map_err(CloudSemanticError::Config)?;
    let context = Context::new(options).await?;
    let maps = materialize_maps(&context, options, None).await?;
    let output = context.scratch.join("dictionary-output");
    let manifest = finalize_dictionary(&context.handoff, &context.handoff_sha256, &maps, &output)?;
    let dictionary_limit = required_u64(options, "semantic-max-dictionary-bytes")
        .map_err(CloudSemanticError::Config)?;
    for file in [
        output.join("dictionary.tsv"),
        output.join("guid-dictionary.tsv"),
    ] {
        if fs::metadata(&file)?.len() > dictionary_limit {
            return Err(CloudSemanticError::Config(format!(
                "semantic dictionary {} exceeds its byte ceiling",
                file.display()
            )));
        }
    }
    upload_tree(
        &context,
        &output,
        &format!("{}/dictionary", context.prefix),
        options,
    )
    .await?;
    let sha256 = sha256_path(&manifest)?;
    patch_status(
        options,
        NgkgSourceImportStatus {
            semantic_dictionary_object_key: Some(format!(
                "{}/dictionary/dictionary-manifest.json",
                context.prefix
            )),
            semantic_dictionary_sha256: Some(sha256.clone()),
            condition: Some("SemanticDictionaryComplete".to_owned()),
            ..load_status(options).await?
        },
    )
    .await?;
    Ok(serde_json::json!({
        "status": "cloud-semantic-dictionary-complete",
        "dictionaryManifestSha256": sha256
    })
    .to_string())
}

pub async fn execute_partition(
    options: &BTreeMap<String, String>,
) -> Result<String, CloudSemanticError> {
    reject_unknown(
        options,
        &[
            "namespace",
            "import-name",
            "artifact-base-url",
            "scratch-root",
            "compiler-handoff-object-key",
            "compiler-handoff-sha256",
            "semantic-max-manifest-bytes",
            "single-put-max-bytes",
            "multipart-buffer-bytes",
            "multipart-concurrency",
            "completion-index",
            "semantic-max-run-bytes",
            "semantic-max-dictionary-bytes",
            "semantic-max-input-runs",
            "semantic-max-partition-quads",
            "semantic-parquet-row-group-rows",
        ],
    )
    .map_err(CloudSemanticError::Config)?;
    let context = Context::new(options).await?;
    let partition = required_value(options, "completion-index")
        .map_err(CloudSemanticError::Config)?
        .parse::<u32>()
        .map_err(|_| CloudSemanticError::Config("completion-index must be a u32".to_owned()))?;
    if partition >= context.handoff.logical_partitions {
        return Err(CloudSemanticError::Config(
            "partition completion index is outside logical layout".to_owned(),
        ));
    }
    let maps = materialize_maps(&context, options, Some(partition)).await?;
    let dictionary_manifest_path = context.scratch.join("dictionary/dictionary-manifest.json");
    let dictionary_manifest_key = format!("{}/dictionary/dictionary-manifest.json", context.prefix);
    context
        .store
        .materialize_unverified_bounded(
            &dictionary_manifest_key,
            required_u64(options, "semantic-max-manifest-bytes")
                .map_err(CloudSemanticError::Config)?,
            &dictionary_manifest_path,
        )
        .await?;
    let dictionary: DictionaryManifest = read_json(&dictionary_manifest_path)?;
    let dictionary_path = context.scratch.join("dictionary/dictionary.tsv");
    context
        .store
        .materialize_verified(
            &format!("{}/dictionary/dictionary.tsv", context.prefix),
            &dictionary.dictionary_sha256,
            required_u64(options, "semantic-max-dictionary-bytes")
                .map_err(CloudSemanticError::Config)?,
            &dictionary_path,
        )
        .await?;
    let output = context.scratch.join("partition-output");
    let manifest_sha256 = sha256_path(&dictionary_manifest_path)?;
    let manifest = reduce_partition(
        &context.handoff,
        &context.handoff_sha256,
        &maps,
        &dictionary_manifest_path,
        &manifest_sha256,
        partition,
        &output,
        ReduceLimits {
            max_input_runs: required_usize(options, "semantic-max-input-runs")
                .map_err(CloudSemanticError::Config)?,
            max_partition_quads: required_u64(options, "semantic-max-partition-quads")
                .map_err(CloudSemanticError::Config)?,
            parquet_row_group_rows: required_usize(options, "semantic-parquet-row-group-rows")
                .map_err(CloudSemanticError::Config)?,
        },
    )?;
    upload_tree(
        &context,
        &output,
        &format!("{}/partitions/{partition:05}", context.prefix),
        options,
    )
    .await?;
    Ok(serde_json::json!({
        "status": "cloud-semantic-partition-complete",
        "partitionIndex": partition,
        "manifestSha256": sha256_path(&manifest)?
    })
    .to_string())
}

pub async fn execute_finalize(
    options: &BTreeMap<String, String>,
) -> Result<String, CloudSemanticError> {
    reject_unknown(
        options,
        &[
            "namespace",
            "import-name",
            "artifact-base-url",
            "scratch-root",
            "compiler-handoff-object-key",
            "compiler-handoff-sha256",
            "semantic-max-manifest-bytes",
            "single-put-max-bytes",
            "multipart-buffer-bytes",
            "multipart-concurrency",
            "semantic-max-dictionary-bytes",
            "semantic-max-artifact-bytes",
            "semantic-finalize-concurrency",
        ],
    )
    .map_err(CloudSemanticError::Config)?;
    let context = Context::new(options).await?;
    let dictionary_manifest_path = context.scratch.join("dictionary/dictionary-manifest.json");
    context
        .store
        .materialize_unverified_bounded(
            &format!("{}/dictionary/dictionary-manifest.json", context.prefix),
            required_u64(options, "semantic-max-manifest-bytes")
                .map_err(CloudSemanticError::Config)?,
            &dictionary_manifest_path,
        )
        .await?;
    let dictionary: DictionaryManifest = read_json(&dictionary_manifest_path)?;
    context
        .store
        .verify_remote(
            &format!("{}/dictionary/dictionary.tsv", context.prefix),
            &dictionary.dictionary_sha256,
            required_u64(options, "semantic-max-dictionary-bytes")
                .map_err(CloudSemanticError::Config)?,
        )
        .await?;
    context
        .store
        .verify_remote(
            &format!("{}/dictionary/guid-dictionary.tsv", context.prefix),
            &dictionary.guid_dictionary_sha256,
            required_u64(options, "semantic-max-dictionary-bytes")
                .map_err(CloudSemanticError::Config)?,
        )
        .await?;
    let manifest_limit =
        required_u64(options, "semantic-max-manifest-bytes").map_err(CloudSemanticError::Config)?;
    let artifact_limit =
        required_u64(options, "semantic-max-artifact-bytes").map_err(CloudSemanticError::Config)?;
    let verify_concurrency = required_usize(options, "semantic-finalize-concurrency")
        .map_err(CloudSemanticError::Config)?;
    let mut partition_paths = Vec::new();
    let mut artifacts = Vec::new();
    for partition in 0..context.handoff.logical_partitions {
        let path = context
            .scratch
            .join(format!("partition-manifests/{partition:05}.json"));
        context
            .store
            .materialize_unverified_bounded(
                &format!(
                    "{}/partitions/{partition:05}/semantic-partition.json",
                    context.prefix
                ),
                manifest_limit,
                &path,
            )
            .await?;
        let manifest: SemanticPartitionManifest = read_json(&path)?;
        for artifact in manifest.artifacts {
            artifacts.push((
                format!(
                    "{}/partitions/{partition:05}/{}",
                    context.prefix, artifact.relative_path
                ),
                artifact.sha256,
                artifact.bytes,
            ));
        }
        partition_paths.push(path);
    }
    stream::iter(artifacts.into_iter().map(|(key, sha256, bytes)| {
        let store = Arc::clone(&context.store);
        async move {
            if bytes > artifact_limit {
                return Err(CloudSemanticError::Config(format!(
                    "semantic artifact {key} exceeds finalizer ceiling"
                )));
            }
            store.verify_remote(&key, &sha256, bytes).await?;
            Ok::<_, CloudSemanticError>(())
        }
    }))
    .buffer_unordered(verify_concurrency)
    .try_collect::<Vec<_>>()
    .await?;
    let output = context.scratch.join("root-output");
    let dictionary_manifest_sha256 = sha256_path(&dictionary_manifest_path)?;
    let root = finalize_committed_semantic_root(
        &context.handoff,
        &context.handoff_sha256,
        &dictionary_manifest_path,
        &dictionary_manifest_sha256,
        &partition_paths,
        &output,
    )?;
    let mut compiled_root: SemanticCompilationRoot = read_json(&root)?;
    compiled_root.dictionary_manifest_path =
        format!("{}/dictionary/dictionary-manifest.json", context.prefix);
    for partition in &mut compiled_root.partitions {
        partition.manifest_path = format!(
            "{}/partitions/{:05}/semantic-partition.json",
            context.prefix, partition.partition_index
        );
    }
    let compiled_fact_count = compiled_root.fact_count;
    fs::write(&root, serde_json::to_vec_pretty(&compiled_root)?)?;
    upload_tree(
        &context,
        &output,
        &format!("{}/root", context.prefix),
        options,
    )
    .await?;
    let root_sha256 = sha256_path(&root)?;
    let root_key = format!("{}/root/semantic-compilation-root.json", context.prefix);
    patch_status(
        options,
        NgkgSourceImportStatus {
            semantic_compilation_root_object_key: Some(root_key.clone()),
            semantic_compilation_root_sha256: Some(root_sha256.clone()),
            compiled_fact_count: Some(compiled_fact_count),
            condition: Some("SemanticCompilationCompleteInactive".to_owned()),
            ..load_status(options).await?
        },
    )
    .await?;
    Ok(serde_json::json!({
        "status": "cloud-semantic-compilation-complete-inactive",
        "semanticCompilationRootObjectKey": root_key,
        "semanticCompilationRootSha256": root_sha256
    })
    .to_string())
}

struct Context {
    store: Arc<ArtifactStore>,
    scratch: PathBuf,
    handoff: CompilerHandoffManifest,
    handoff_sha256: String,
    prefix: String,
}

impl Context {
    async fn new(options: &BTreeMap<String, String>) -> Result<Self, CloudSemanticError> {
        let scratch = required_path(options, "scratch-root").map_err(CloudSemanticError::Config)?;
        fs::create_dir_all(&scratch)?;
        let store = Arc::new(ArtifactStore::from_base_url(
            &required_value(options, "artifact-base-url").map_err(CloudSemanticError::Config)?,
        )?);
        let handoff_key = required_value(options, "compiler-handoff-object-key")
            .map_err(CloudSemanticError::Config)?;
        let handoff_sha256 = required_value(options, "compiler-handoff-sha256")
            .map_err(CloudSemanticError::Config)?;
        let path = scratch.join("compiler-handoff.json");
        store
            .materialize_verified(
                &handoff_key,
                &handoff_sha256,
                required_u64(options, "semantic-max-manifest-bytes")
                    .map_err(CloudSemanticError::Config)?,
                &path,
            )
            .await?;
        let handoff: CompilerHandoffManifest = read_json(&path)?;
        ngkg_semantic_compiler::validate_handoff(&handoff)?;
        let prefix = format!(
            "imports/{}/{}/{}/semantic-compilation",
            handoff.tenant_id, handoff.dataset_id, handoff.operation_id
        );
        Ok(Self {
            store,
            scratch,
            handoff,
            handoff_sha256,
            prefix,
        })
    }
}

async fn materialize_maps(
    context: &Context,
    options: &BTreeMap<String, String>,
    partition: Option<u32>,
) -> Result<Vec<PathBuf>, CloudSemanticError> {
    let manifest_limit =
        required_u64(options, "semantic-max-manifest-bytes").map_err(CloudSemanticError::Config)?;
    let run_limit =
        required_u64(options, "semantic-max-run-bytes").map_err(CloudSemanticError::Config)?;
    let mut paths = Vec::new();
    for ordinal in 0..context.handoff.total_objects {
        let root = context.scratch.join(format!("maps/{ordinal:08}"));
        let path = root.join("fragment-map.json");
        context
            .store
            .materialize_unverified_bounded(
                &format!("{}/maps/{ordinal:08}/fragment-map.json", context.prefix),
                manifest_limit,
                &path,
            )
            .await?;
        let manifest: FragmentMapManifest = read_json(&path)?;
        let artifacts = if let Some(partition) = partition {
            manifest
                .fact_runs
                .get(&partition)
                .cloned()
                .unwrap_or_default()
        } else {
            manifest.term_runs.clone()
        };
        for artifact in artifacts {
            context
                .store
                .materialize_verified(
                    &format!(
                        "{}/maps/{ordinal:08}/{}",
                        context.prefix, artifact.relative_path
                    ),
                    &artifact.sha256,
                    run_limit.min(artifact.bytes),
                    &root.join(&artifact.relative_path),
                )
                .await?;
        }
        paths.push(path);
    }
    Ok(paths)
}

async fn upload_tree(
    context: &Context,
    root: &Path,
    object_prefix: &str,
    options: &BTreeMap<String, String>,
) -> Result<(), CloudSemanticError> {
    let files = collect_files(root)?;
    for path in files {
        let relative = path.strip_prefix(root).map_err(|_| {
            CloudSemanticError::Config("compiler output escaped its root".to_owned())
        })?;
        let relative = relative.to_str().ok_or_else(|| {
            CloudSemanticError::Config("compiler output path is not UTF-8".to_owned())
        })?;
        context
            .store
            .put_file_immutable(
                &format!("{object_prefix}/{relative}"),
                &sha256_path(&path)?,
                &path,
                required_u64(options, "single-put-max-bytes")
                    .map_err(CloudSemanticError::Config)?,
                required_usize(options, "multipart-buffer-bytes")
                    .map_err(CloudSemanticError::Config)?,
                required_usize(options, "multipart-concurrency")
                    .map_err(CloudSemanticError::Config)?,
            )
            .await?;
    }
    Ok(())
}

fn collect_files(root: &Path) -> Result<Vec<PathBuf>, CloudSemanticError> {
    let mut pending = vec![root.to_owned()];
    let mut files = Vec::new();
    while let Some(directory) = pending.pop() {
        for entry in fs::read_dir(directory)? {
            let entry = entry?;
            let kind = entry.file_type()?;
            if kind.is_symlink() {
                return Err(CloudSemanticError::Config(
                    "compiler output contains a symlink".to_owned(),
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

fn read_json<T: DeserializeOwned>(path: &Path) -> Result<T, CloudSemanticError> {
    Ok(serde_json::from_slice(&fs::read(path)?)?)
}

async fn load_status(
    options: &BTreeMap<String, String>,
) -> Result<NgkgSourceImportStatus, CloudSemanticError> {
    let namespace = required_value(options, "namespace").map_err(CloudSemanticError::Config)?;
    let name = required_value(options, "import-name").map_err(CloudSemanticError::Config)?;
    let api: Api<NgkgSourceImport> = Api::namespaced(Client::try_default().await?, &namespace);
    Ok(api.get(&name).await?.status.unwrap_or_default())
}

async fn patch_status(
    options: &BTreeMap<String, String>,
    status: NgkgSourceImportStatus,
) -> Result<(), CloudSemanticError> {
    let namespace = required_value(options, "namespace").map_err(CloudSemanticError::Config)?;
    let name = required_value(options, "import-name").map_err(CloudSemanticError::Config)?;
    let api: Api<NgkgSourceImport> = Api::namespaced(Client::try_default().await?, &namespace);
    let document = source_import_status_apply_document(
        &name,
        &status,
        &[
            "semanticDictionaryObjectKey", "semanticDictionarySha256",
            "semanticCompilationRootObjectKey", "semanticCompilationRootSha256",
            "compiledFactCount",
        ],
    )?;
    api.patch_status(
        &name,
        &PatchParams::apply("ngkg-semantic-worker"),
        &Patch::Apply(document),
    )
    .await?;
    Ok(())
}
