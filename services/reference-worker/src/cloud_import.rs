//! Read-only cloud-bucket TriG discovery and checksum-bound source-manifest publication.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File},
    io::{BufReader, Read},
    path::{Component, Path, PathBuf},
    sync::{Arc, Mutex},
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
use ngkg_source_planner::{FrozenCloudSourceManifest, FrozenCloudSourceObject, plan_cloud_decode};
use oxigraph::{
    io::{RdfFormat, RdfParser},
    model::GraphName,
};
use sha2::{Digest, Sha256};
use thiserror::Error;

use super::{required_path, required_u64, required_usize, required_value};

const SOURCE_MOUNT: &str = "/source";

#[derive(Debug, Error)]
pub enum CloudImportError {
    #[error("cloud import configuration is invalid: {0}")]
    Config(String),
    #[error("cloud import filesystem failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("cloud import Kubernetes state failed: {0}")]
    Kubernetes(#[from] kube::Error),
    #[error("cloud import artifact publication failed: {0}")]
    Artifact(#[from] ngkg_artifact_store::ArtifactStoreError),
    #[error("cloud import JSON failed: {0}")]
    Json(#[from] serde_json::Error),
    #[error("TriG source {object_key} is invalid: {detail}")]
    Trig { object_key: String, detail: String },
}

struct HashingReader<R> {
    inner: R,
    state: Arc<Mutex<HashState>>,
}

#[derive(Clone)]
struct HashState {
    hash: Sha256,
    bytes: u64,
}

impl<R> HashingReader<R> {
    fn new(inner: R, state: Arc<Mutex<HashState>>) -> Self {
        Self { inner, state }
    }
}

impl<R: Read> Read for HashingReader<R> {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        let count = self.inner.read(buffer)?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| std::io::Error::other("source hash state lock was poisoned"))?;
        state.hash.update(&buffer[..count]);
        state.bytes = state
            .bytes
            .checked_add(u64::try_from(count).unwrap_or(u64::MAX))
            .ok_or_else(|| std::io::Error::other("source byte count overflow"))?;
        Ok(count)
    }
}

pub async fn execute(options: &BTreeMap<String, String>) -> Result<String, CloudImportError> {
    let namespace = required_value(options, "namespace").map_err(CloudImportError::Config)?;
    let import_name = required_value(options, "import-name").map_err(CloudImportError::Config)?;
    let source_root = required_path(options, "source-root").map_err(CloudImportError::Config)?;
    let scratch_root = required_path(options, "scratch-root").map_err(CloudImportError::Config)?;
    if source_root != Path::new(SOURCE_MOUNT) || !source_root.is_absolute() {
        return Err(CloudImportError::Config(
            "source-root must be the operator-controlled /source mount".to_owned(),
        ));
    }
    fs::create_dir_all(&scratch_root)?;
    let client = Client::try_default().await?;
    let imports: Api<NgkgSourceImport> = Api::namespaced(client, &namespace);
    let import = imports.get(&import_name).await?;
    if import.spec.version_policy != ngkg_kube::CloudObjectVersionPolicy::RequireImmutableChecksum {
        return Err(CloudImportError::Config(
            "provider-version proof is not implemented; require-immutable-checksum is mandatory"
                .to_owned(),
        ));
    }
    let paths = discover_paths(&source_root, &import)?;
    let concurrency = required_usize(options, "scan-concurrency")
        .map_err(CloudImportError::Config)?
        .min(paths.len().max(1));
    let root = Arc::new(source_root);
    let scans = stream::iter(paths.into_iter().enumerate().map(|(ordinal, relative)| {
        let root = Arc::clone(&root);
        async move {
            tokio::task::spawn_blocking(move || scan_trig(&root, ordinal, relative))
                .await
                .map_err(|error| {
                    CloudImportError::Config(format!("TriG scan worker failed: {error}"))
                })?
        }
    }))
    .buffer_unordered(concurrency)
    .try_collect::<Vec<_>>()
    .await?;
    let mut objects = scans;
    objects.sort_unstable_by_key(|object| object.ordinal);
    let total_bytes = objects.iter().try_fold(0_u64, |total, object| {
        total.checked_add(object.bytes).ok_or_else(|| {
            CloudImportError::Config("selected source byte count overflow".to_owned())
        })
    })?;
    let total_quads = objects.iter().try_fold(0_u64, |total, object| {
        total.checked_add(object.parsed_quad_count).ok_or_else(|| {
            CloudImportError::Config("selected source quad count overflow".to_owned())
        })
    })?;
    if total_bytes > import.spec.max_source_bytes {
        return Err(CloudImportError::Config(format!(
            "selected sources contain {total_bytes} bytes, exceeding maxSourceBytes"
        )));
    }
    let mut aggregate = Sha256::new();
    aggregate.update(b"ngkg-cloud-source-aggregate-v1\0");
    for object in &objects {
        aggregate.update(object.ordinal.to_be_bytes());
        aggregate.update((object.object_key.len() as u64).to_be_bytes());
        aggregate.update(object.object_key.as_bytes());
        aggregate.update(object.bytes.to_be_bytes());
        aggregate.update(object.sha256.as_bytes());
    }
    let provider = serde_json::to_value(import.spec.provider)?
        .as_str()
        .ok_or_else(|| CloudImportError::Config("cloud provider is not textual".to_owned()))?
        .to_owned();
    let version_policy = serde_json::to_value(import.spec.version_policy)?
        .as_str()
        .ok_or_else(|| CloudImportError::Config("version policy is not textual".to_owned()))?
        .to_owned();
    let manifest = FrozenCloudSourceManifest {
        format_version: 1,
        tenant_id: import.spec.tenant_id,
        dataset_id: import.spec.dataset_id,
        operation_id: import.spec.operation_id,
        target_snapshot_id: import.spec.target_snapshot_id,
        provider,
        bucket: import.spec.bucket.clone(),
        account_name: import.spec.account_name.clone(),
        source_mount: SOURCE_MOUNT.to_owned(),
        version_policy,
        logical_partitions: import.spec.logical_partitions,
        total_objects: u32::try_from(objects.len())
            .map_err(|_| CloudImportError::Config("selected object count overflow".to_owned()))?,
        total_bytes,
        total_quads,
        aggregate_source_sha256: hex::encode(aggregate.finalize()),
        objects,
    };
    let manifest_bytes = serde_json::to_vec_pretty(&manifest)?;
    let manifest_sha256 = hex::encode(Sha256::digest(&manifest_bytes));
    let manifest_path = scratch_root.join("source-manifest.json");
    tokio::fs::write(&manifest_path, &manifest_bytes).await?;
    let manifest_key = format!(
        "imports/{}/{}/{}/source-manifest.json",
        import.spec.tenant_id, import.spec.dataset_id, import.spec.operation_id
    );
    let store = ArtifactStore::from_base_url(
        &required_value(options, "artifact-base-url").map_err(CloudImportError::Config)?,
    )?;
    store
        .put_file_immutable(
            &manifest_key,
            &manifest_sha256,
            &manifest_path,
            required_u64(options, "single-put-max-bytes").map_err(CloudImportError::Config)?,
            required_usize(options, "multipart-buffer-bytes").map_err(CloudImportError::Config)?,
            required_usize(options, "multipart-concurrency").map_err(CloudImportError::Config)?,
        )
        .await?;
    let decode_plan = plan_cloud_decode(
        &manifest,
        &manifest_key,
        &manifest_sha256,
        required_u64(options, "decode-target-work-bytes").map_err(CloudImportError::Config)?,
        u32::try_from(
            required_usize(options, "decode-max-work-items").map_err(CloudImportError::Config)?,
        )
        .map_err(|_| CloudImportError::Config("decode-max-work-items exceeds u32".to_owned()))?,
    )
    .map_err(|error| CloudImportError::Config(error.to_string()))?;
    let decode_plan_bytes = serde_json::to_vec_pretty(&decode_plan)?;
    let max_plan_bytes =
        required_u64(options, "decode-max-plan-bytes").map_err(CloudImportError::Config)?;
    if u64::try_from(decode_plan_bytes.len()).unwrap_or(u64::MAX) > max_plan_bytes {
        return Err(CloudImportError::Config(
            "cloud decode plan exceeds decode-max-plan-bytes".to_owned(),
        ));
    }
    let decode_plan_sha256 = hex::encode(Sha256::digest(&decode_plan_bytes));
    let decode_plan_path = scratch_root.join("source-decode-plan.json");
    tokio::fs::write(&decode_plan_path, &decode_plan_bytes).await?;
    let decode_plan_key = format!(
        "imports/{}/{}/{}/source-decode-plan.json",
        import.spec.tenant_id, import.spec.dataset_id, import.spec.operation_id
    );
    store
        .put_file_immutable(
            &decode_plan_key,
            &decode_plan_sha256,
            &decode_plan_path,
            required_u64(options, "single-put-max-bytes").map_err(CloudImportError::Config)?,
            required_usize(options, "multipart-buffer-bytes").map_err(CloudImportError::Config)?,
            required_usize(options, "multipart-concurrency").map_err(CloudImportError::Config)?,
        )
        .await?;
    // Phase 40.13.10's SourceManifestPublished barrier remains satisfied; Phase 40.13.11
    // advances the externally observed condition only after its derived plan is immutable.
    let status = NgkgSourceImportStatus {
        observed_generation: import.metadata.generation,
        job_name: import
            .status
            .as_ref()
            .and_then(|value| value.job_name.clone()),
        source_manifest_object_key: Some(manifest_key.clone()),
        source_manifest_sha256: Some(manifest_sha256.clone()),
        decode_plan_object_key: Some(decode_plan_key.clone()),
        decode_plan_sha256: Some(decode_plan_sha256.clone()),
        decode_work_item_count: Some(decode_plan.total_work_items),
        selected_object_count: Some(manifest.total_objects),
        selected_source_bytes: Some(total_bytes),
        parsed_quad_count: Some(total_quads),
        condition: Some("SourceDecodePlanPublished".to_owned()),
        ..NgkgSourceImportStatus::default()
    };
    let document = source_import_status_apply_document(
        &import_name,
        &status,
        &[
            "observedGeneration", "sourceManifestObjectKey", "sourceManifestSha256",
            "decodePlanObjectKey", "decodePlanSha256", "decodeWorkItemCount",
            "selectedObjectCount", "selectedSourceBytes", "parsedQuadCount",
        ],
    )?;
    imports
        .patch_status(
            &import_name,
            &PatchParams::apply("ngkg-source-discovery-worker"),
            &Patch::Apply(document),
        )
        .await?;
    Ok(serde_json::json!({
        "status": "source-decode-plan-published",
        "operationId": import.spec.operation_id,
        "sourceManifestObjectKey": manifest_key,
        "sourceManifestSha256": manifest_sha256,
        "decodePlanObjectKey": decode_plan_key,
        "decodePlanSha256": decode_plan_sha256,
        "decodeWorkItemCount": decode_plan.total_work_items,
        "selectedObjectCount": manifest.total_objects,
        "selectedSourceBytes": total_bytes,
        "parsedQuadCount": total_quads
    })
    .to_string())
}

fn discover_paths(
    source_root: &Path,
    import: &NgkgSourceImport,
) -> Result<Vec<PathBuf>, CloudImportError> {
    let max_objects = usize::try_from(import.spec.max_source_objects)
        .map_err(|_| CloudImportError::Config("maxSourceObjects overflow".to_owned()))?;
    let excluded = import
        .spec
        .exclude_segments
        .iter()
        .map(|value| value.to_ascii_lowercase())
        .collect::<BTreeSet<_>>();
    let mut selected = Vec::new();
    if !import.spec.object_keys.is_empty() {
        for object_key in &import.spec.object_keys {
            let relative = safe_relative_path(object_key)?;
            require_selected_trig(&relative, &excluded)?;
            let path = source_root.join(&relative);
            require_regular_file(&path)?;
            selected.push(relative);
        }
    } else {
        let prefix = import
            .spec
            .prefix
            .as_deref()
            .ok_or_else(|| CloudImportError::Config("prefix is absent".to_owned()))?;
        let relative_prefix = safe_relative_path(prefix.trim_end_matches('/'))?;
        let base = source_root.join(&relative_prefix);
        let mut pending = vec![base];
        while let Some(directory) = pending.pop() {
            let metadata = fs::symlink_metadata(&directory)?;
            if metadata.file_type().is_symlink() || !metadata.is_dir() {
                return Err(CloudImportError::Config(format!(
                    "discovery path {} is not a real directory",
                    directory.display()
                )));
            }
            for entry in fs::read_dir(&directory)? {
                let entry = entry?;
                let path = entry.path();
                let metadata = fs::symlink_metadata(&path)?;
                if metadata.file_type().is_symlink() {
                    return Err(CloudImportError::Config(format!(
                        "source mount contains a forbidden symlink: {}",
                        path.display()
                    )));
                }
                if metadata.is_dir() {
                    pending.push(path);
                } else if metadata.is_file() {
                    let relative = path.strip_prefix(source_root).map_err(|_| {
                        CloudImportError::Config("discovered path escaped source root".to_owned())
                    })?;
                    if is_selected_trig(relative, &excluded) {
                        selected.push(relative.to_owned());
                    }
                }
            }
        }
    }
    selected.sort_unstable();
    selected.dedup();
    if selected.is_empty() || selected.len() > max_objects {
        return Err(CloudImportError::Config(
            "selected TriG object count is outside 1..=maxSourceObjects".to_owned(),
        ));
    }
    Ok(selected)
}

fn safe_relative_path(value: &str) -> Result<PathBuf, CloudImportError> {
    let path = Path::new(value);
    if value.is_empty()
        || value.contains('\\')
        || value.split('/').any(str::is_empty)
        || path.is_absolute()
        || path
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(CloudImportError::Config(format!(
            "unsafe source object key: {value}"
        )));
    }
    Ok(path.to_owned())
}

fn is_selected_trig(path: &Path, excluded: &BTreeSet<String>) -> bool {
    path.extension()
        .and_then(|value| value.to_str())
        .is_some_and(|value| value.eq_ignore_ascii_case("trig"))
        && !path.components().any(|component| {
            let Component::Normal(value) = component else {
                return true;
            };
            excluded.contains(&value.to_string_lossy().to_ascii_lowercase())
        })
}

fn require_selected_trig(path: &Path, excluded: &BTreeSet<String>) -> Result<(), CloudImportError> {
    if !is_selected_trig(path, excluded) {
        return Err(CloudImportError::Config(format!(
            "object is not an allowed TriG source: {}",
            path.display()
        )));
    }
    Ok(())
}

fn require_regular_file(path: &Path) -> Result<(), CloudImportError> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(CloudImportError::Config(format!(
            "source object is not a real regular file: {}",
            path.display()
        )));
    }
    Ok(())
}

fn scan_trig(
    source_root: &Path,
    ordinal: usize,
    relative: PathBuf,
) -> Result<FrozenCloudSourceObject, CloudImportError> {
    let path = source_root.join(&relative);
    require_regular_file(&path)?;
    let before = fs::metadata(&path)?;
    let hash_state = Arc::new(Mutex::new(HashState {
        hash: Sha256::new(),
        bytes: 0,
    }));
    let reader = HashingReader::new(BufReader::new(File::open(&path)?), Arc::clone(&hash_state));
    let mut parser = RdfParser::from_format(RdfFormat::TriG).for_reader(reader);
    let mut parsed_quad_count = 0_u64;
    let mut default_graph_quad_count = 0_u64;
    let mut named_graph_quad_counts = BTreeMap::<String, u64>::new();
    for parsed in &mut parser {
        let quad = parsed.map_err(|error| CloudImportError::Trig {
            object_key: relative.display().to_string(),
            detail: error.to_string(),
        })?;
        parsed_quad_count = parsed_quad_count
            .checked_add(1)
            .ok_or_else(|| CloudImportError::Config("source quad count overflow".to_owned()))?;
        match quad.graph_name {
            GraphName::DefaultGraph => {
                default_graph_quad_count =
                    default_graph_quad_count.checked_add(1).ok_or_else(|| {
                        CloudImportError::Config("default graph quad count overflow".to_owned())
                    })?;
            }
            GraphName::NamedNode(graph) => {
                let count = named_graph_quad_counts
                    .entry(graph.into_string())
                    .or_default();
                *count = count.checked_add(1).ok_or_else(|| {
                    CloudImportError::Config("named graph quad count overflow".to_owned())
                })?;
            }
            GraphName::BlankNode(_) => {
                return Err(CloudImportError::Trig {
                    object_key: relative.display().to_string(),
                    detail: "blank-node graph names are forbidden".to_owned(),
                });
            }
        }
    }
    if parsed_quad_count == 0 {
        return Err(CloudImportError::Trig {
            object_key: relative.display().to_string(),
            detail: "empty TriG sources are forbidden".to_owned(),
        });
    }
    drop(parser);
    let after = fs::metadata(&path)?;
    let state = hash_state
        .lock()
        .map_err(|_| CloudImportError::Config("source hash state lock was poisoned".to_owned()))?
        .clone();
    if before.len() != after.len()
        || before.modified().ok() != after.modified().ok()
        || state.bytes != after.len()
    {
        return Err(CloudImportError::Config(format!(
            "source object changed while it was being frozen: {}",
            relative.display()
        )));
    }
    let sha256 = hex::encode(state.hash.finalize());
    let ordinal = u32::try_from(ordinal)
        .map_err(|_| CloudImportError::Config("source ordinal overflow".to_owned()))?;
    Ok(FrozenCloudSourceObject {
        ordinal,
        object_key: relative.to_string_lossy().replace('\\', "/"),
        bytes: after.len(),
        sha256: sha256.clone(),
        parsed_quad_count,
        default_graph_quad_count,
        named_graph_quad_counts,
        blank_node_scope: format!("object-{ordinal:08}-{sha256}"),
    })
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeSet, path::Path};

    use super::{is_selected_trig, safe_relative_path};

    #[test]
    fn source_keys_cannot_escape_the_read_only_mount() {
        assert!(safe_relative_path("published/clinical_nutrition.trig").is_ok());
        assert!(safe_relative_path("../private.trig").is_err());
        assert!(safe_relative_path("/absolute.trig").is_err());
        assert!(safe_relative_path("published//empty.trig").is_err());
    }

    #[test]
    fn discovery_selects_only_trig_and_excludes_non_asserted_artifact_paths() {
        let excluded = BTreeSet::from([
            "alignment".to_owned(),
            "closure".to_owned(),
            "provenance".to_owned(),
        ]);
        assert!(is_selected_trig(
            Path::new("oncology/semkg/asserted.trig"),
            &excluded
        ));
        assert!(!is_selected_trig(
            Path::new("oncology/closure/materialized.trig"),
            &excluded
        ));
        assert!(!is_selected_trig(
            Path::new("oncology/semkg/data.ttl"),
            &excluded
        ));
    }
}
