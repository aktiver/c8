//! Syntax-safe distributed decode and all-completions compiler handoff.

use std::{
    collections::BTreeMap,
    fs::{self, File},
    io::{BufReader, BufWriter, Read, Write},
    path::{Component, Path, PathBuf},
    sync::{Arc, Mutex},
};

use futures::{StreamExt, TryStreamExt, stream};
use kube::{Api, Client, api::{Patch, PatchParams}};
use ngkg_artifact_store::ArtifactStore;
use ngkg_kube::{NgkgSourceImport, NgkgSourceImportStatus};
use ngkg_source_planner::{CloudDecodePlan, CloudDecodeWorkItem, FrozenCloudSourceObject};
use oxigraph::io::{RdfFormat, RdfParser, RdfSerializer};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use super::{required_path, required_u64, required_usize, required_value};

const SOURCE_MOUNT: &str = "/source";

#[derive(Debug, Error)]
pub enum CloudDecodeError {
    #[error("cloud decode configuration is invalid: {0}")]
    Config(String),
    #[error("cloud decode I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("cloud decode artifact access failed: {0}")]
    Artifact(#[from] ngkg_artifact_store::ArtifactStoreError),
    #[error("cloud decode JSON failed: {0}")]
    Json(#[from] serde_json::Error),
    #[error("cloud decode Kubernetes state failed: {0}")]
    Kubernetes(#[from] kube::Error),
    #[error("TriG decode failed for {object_key}: {detail}")]
    Trig { object_key: String, detail: String },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct DecodedObjectFragment {
    ordinal: u32,
    source_object_key: String,
    source_sha256: String,
    blank_node_scope: String,
    object_key: String,
    sha256: String,
    bytes: u64,
    quad_count: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct DecodeCompletionManifest {
    format_version: u32,
    tenant_id: uuid::Uuid,
    dataset_id: uuid::Uuid,
    operation_id: uuid::Uuid,
    target_snapshot_id: uuid::Uuid,
    decode_plan_sha256: String,
    completion_index: u32,
    work_id: String,
    total_bytes: u64,
    total_quads: u64,
    fragments: Vec<DecodedObjectFragment>,
}

#[derive(Debug, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct CompilerHandoffManifest {
    format_version: u32,
    tenant_id: uuid::Uuid,
    dataset_id: uuid::Uuid,
    operation_id: uuid::Uuid,
    target_snapshot_id: uuid::Uuid,
    source_manifest_object_key: String,
    source_manifest_sha256: String,
    decode_plan_object_key: String,
    decode_plan_sha256: String,
    aggregate_source_sha256: String,
    logical_partitions: u32,
    decoded_format: String,
    blank_node_policy: String,
    total_objects: u32,
    total_bytes: u64,
    total_quads: u64,
    expected_completions: u32,
    verified_completions: u32,
    completion_set_sha256: String,
    fragments: Vec<DecodedObjectFragment>,
}

struct HashingReader<R> {
    inner: R,
    state: Arc<Mutex<(Sha256, u64)>>,
}

impl<R: Read> Read for HashingReader<R> {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        let count = self.inner.read(buffer)?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| std::io::Error::other("source hash lock was poisoned"))?;
        state.0.update(&buffer[..count]);
        state.1 = state
            .1
            .checked_add(u64::try_from(count).unwrap_or(u64::MAX))
            .ok_or_else(|| std::io::Error::other("source byte count overflow"))?;
        Ok(count)
    }
}

struct BoundedWriter<W> {
    inner: W,
    written: u64,
    maximum: u64,
}

impl<W: Write> Write for BoundedWriter<W> {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        let requested = u64::try_from(buffer.len())
            .map_err(|_| std::io::Error::other("decoded fragment byte count overflow"))?;
        if self.written.checked_add(requested).is_none_or(|next| next > self.maximum) {
            return Err(std::io::Error::other("decoded fragment exceeds configured byte ceiling"));
        }
        let count = self.inner.write(buffer)?;
        self.written = self
            .written
            .checked_add(u64::try_from(count).unwrap_or(u64::MAX))
            .ok_or_else(|| std::io::Error::other("decoded fragment byte count overflow"))?;
        Ok(count)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

pub async fn execute_decode(
    options: &BTreeMap<String, String>,
) -> Result<String, CloudDecodeError> {
    let source_root = required_path(options, "source-root").map_err(CloudDecodeError::Config)?;
    if source_root != Path::new(SOURCE_MOUNT) || !source_root.is_absolute() {
        return Err(CloudDecodeError::Config(
            "source-root must be the operator-controlled /source mount".to_owned(),
        ));
    }
    let scratch_root = required_path(options, "scratch-root").map_err(CloudDecodeError::Config)?;
    fs::create_dir_all(&scratch_root)?;
    let store = Arc::new(ArtifactStore::from_base_url(
        &required_value(options, "artifact-base-url").map_err(CloudDecodeError::Config)?,
    )?);
    let plan_key = required_value(options, "decode-plan-object-key")
        .map_err(CloudDecodeError::Config)?;
    let plan_sha256 = required_value(options, "decode-plan-sha256")
        .map_err(CloudDecodeError::Config)?;
    let plan_path = scratch_root.join("source-decode-plan.json");
    store
        .materialize_verified(
            &plan_key,
            &plan_sha256,
            required_u64(options, "decode-max-plan-bytes").map_err(CloudDecodeError::Config)?,
            &plan_path,
        )
        .await?;
    let plan: CloudDecodePlan = serde_json::from_slice(&fs::read(&plan_path)?)?;
    validate_plan_identity(&plan, &plan_key, &plan_sha256)?;
    let completion_index = required_value(options, "completion-index")
        .map_err(CloudDecodeError::Config)?
        .parse::<u32>()
        .map_err(|_| CloudDecodeError::Config("completion-index must be a u32".to_owned()))?;
    let work = plan
        .work_items
        .get(usize::try_from(completion_index).map_err(|_| {
            CloudDecodeError::Config("completion-index exceeds platform size".to_owned())
        })?)
        .filter(|work| work.completion_index == completion_index)
        .cloned()
        .ok_or_else(|| CloudDecodeError::Config("completion-index is absent from plan".to_owned()))?;
    let maximum_fragment_bytes = required_u64(options, "decode-max-fragment-bytes")
        .map_err(CloudDecodeError::Config)?;
    let object_concurrency = required_usize(options, "decode-object-concurrency")
        .map_err(CloudDecodeError::Config)?
        .min(work.objects.len().max(1));
    let single_put_max_bytes = required_u64(options, "single-put-max-bytes")
        .map_err(CloudDecodeError::Config)?;
    let multipart_buffer_bytes = required_usize(options, "multipart-buffer-bytes")
        .map_err(CloudDecodeError::Config)?;
    let multipart_concurrency = required_usize(options, "multipart-concurrency")
        .map_err(CloudDecodeError::Config)?;
    let plan = Arc::new(plan);
    let source_root = Arc::new(source_root);
    let scratch_root = Arc::new(scratch_root);
    let fragments = stream::iter(work.objects.iter().cloned().map(|object| {
        let store = Arc::clone(&store);
        let plan = Arc::clone(&plan);
        let source_root = Arc::clone(&source_root);
        let scratch_root = Arc::clone(&scratch_root);
        async move {
            let ordinal = object.ordinal;
            let decode_source_root = Arc::clone(&source_root);
            let decode_scratch_root = Arc::clone(&scratch_root);
            let decode_plan = Arc::clone(&plan);
            let fragment = tokio::task::spawn_blocking(move || {
                decode_object(
                    &decode_source_root,
                    &decode_scratch_root,
                    &decode_plan,
                    &object,
                    maximum_fragment_bytes,
                )
            })
            .await
            .map_err(|error| {
                CloudDecodeError::Config(format!("TriG decode lane failed: {error}"))
            })??;
            let local_path = scratch_root.join(fragment_file_name(ordinal));
            store
                .put_file_immutable(
                    &fragment.object_key,
                    &fragment.sha256,
                    &local_path,
                    single_put_max_bytes,
                    multipart_buffer_bytes,
                    multipart_concurrency,
                )
                .await?;
            tokio::fs::remove_file(&local_path).await?;
            Ok::<_, CloudDecodeError>(fragment)
        }
    }))
    .buffer_unordered(object_concurrency)
    .try_collect::<Vec<_>>()
    .await?;
    let mut fragments = fragments;
    fragments.sort_unstable_by_key(|fragment| fragment.ordinal);
    let completion = DecodeCompletionManifest {
        format_version: 1,
        tenant_id: plan.tenant_id,
        dataset_id: plan.dataset_id,
        operation_id: plan.operation_id,
        target_snapshot_id: plan.target_snapshot_id,
        decode_plan_sha256: plan_sha256,
        completion_index,
        work_id: work.work_id.clone(),
        total_bytes: work.total_bytes,
        total_quads: work.total_quads,
        fragments,
    };
    let completion_bytes = serde_json::to_vec_pretty(&completion)?;
    let completion_sha256 = hex::encode(Sha256::digest(&completion_bytes));
    let completion_path = scratch_root.join("decode-completion.json");
    fs::write(&completion_path, &completion_bytes)?;
    let completion_key = completion_key(&plan, completion_index);
    store
        .put_file_immutable(
            &completion_key,
            &completion_sha256,
            &completion_path,
            required_u64(options, "single-put-max-bytes").map_err(CloudDecodeError::Config)?,
            required_usize(options, "multipart-buffer-bytes")
                .map_err(CloudDecodeError::Config)?,
            required_usize(options, "multipart-concurrency")
                .map_err(CloudDecodeError::Config)?,
        )
        .await?;
    Ok(serde_json::json!({
        "status": "cloud-decode-complete",
        "completionIndex": completion_index,
        "workId": work.work_id,
        "completionManifestObjectKey": completion_key,
        "completionManifestSha256": completion_sha256,
        "decodedObjects": completion.fragments.len(),
        "decodedQuads": completion.total_quads
    })
    .to_string())
}

pub async fn execute_finalize(
    options: &BTreeMap<String, String>,
) -> Result<String, CloudDecodeError> {
    let scratch_root = required_path(options, "scratch-root").map_err(CloudDecodeError::Config)?;
    fs::create_dir_all(&scratch_root)?;
    let store = Arc::new(ArtifactStore::from_base_url(
        &required_value(options, "artifact-base-url").map_err(CloudDecodeError::Config)?,
    )?);
    let plan_key = required_value(options, "decode-plan-object-key")
        .map_err(CloudDecodeError::Config)?;
    let plan_sha256 = required_value(options, "decode-plan-sha256")
        .map_err(CloudDecodeError::Config)?;
    let plan_path = scratch_root.join("source-decode-plan.json");
    store
        .materialize_verified(
            &plan_key,
            &plan_sha256,
            required_u64(options, "decode-max-plan-bytes").map_err(CloudDecodeError::Config)?,
            &plan_path,
        )
        .await?;
    let plan: CloudDecodePlan = serde_json::from_slice(&fs::read(&plan_path)?)?;
    validate_plan_identity(&plan, &plan_key, &plan_sha256)?;
    let completion_limit = required_u64(options, "decode-max-completion-manifest-bytes")
        .map_err(CloudDecodeError::Config)?;
    let fragment_limit = required_u64(options, "decode-max-fragment-bytes")
        .map_err(CloudDecodeError::Config)?;
    let verify_concurrency = required_usize(options, "decode-finalize-concurrency")
        .map_err(CloudDecodeError::Config)?;
    let root = Arc::new(scratch_root.clone());
    let completions = stream::iter(plan.work_items.iter().cloned().map(|work| {
        let store = Arc::clone(&store);
        let root = Arc::clone(&root);
        let plan = plan.clone();
        let plan_sha256 = plan_sha256.clone();
        async move {
            load_and_verify_completion(
                &store,
                &root,
                &plan,
                &plan_sha256,
                &work,
                completion_limit,
                fragment_limit,
            )
            .await
        }
    }))
    .buffer_unordered(verify_concurrency)
    .try_collect::<Vec<_>>()
    .await?;
    let mut completions = completions;
    completions.sort_unstable_by_key(|completion| completion.completion_index);
    let mut aggregate = Sha256::new();
    aggregate.update(b"ngkg-cloud-decode-completion-set-v1\0");
    let mut fragments = Vec::new();
    for completion in completions {
        aggregate.update(completion.completion_index.to_be_bytes());
        aggregate.update(completion.work_id.as_bytes());
        for fragment in completion.fragments {
            aggregate.update(fragment.ordinal.to_be_bytes());
            aggregate.update(fragment.sha256.as_bytes());
            fragments.push(fragment);
        }
    }
    fragments.sort_unstable_by_key(|fragment| fragment.ordinal);
    if fragments.len() != usize::try_from(plan.total_objects).unwrap_or(usize::MAX)
        || fragments.iter().enumerate().any(|(ordinal, fragment)| {
            fragment.ordinal != u32::try_from(ordinal).unwrap_or(u32::MAX)
        })
    {
        return Err(CloudDecodeError::Config(
            "verified completion set does not cover every source object exactly once".to_owned(),
        ));
    }
    let handoff = CompilerHandoffManifest {
        format_version: 1,
        tenant_id: plan.tenant_id,
        dataset_id: plan.dataset_id,
        operation_id: plan.operation_id,
        target_snapshot_id: plan.target_snapshot_id,
        source_manifest_object_key: plan.source_manifest_object_key.clone(),
        source_manifest_sha256: plan.source_manifest_sha256.clone(),
        decode_plan_object_key: plan_key,
        decode_plan_sha256: plan_sha256,
        aggregate_source_sha256: plan.aggregate_source_sha256.clone(),
        logical_partitions: plan.logical_partitions,
        decoded_format: "application/n-quads".to_owned(),
        blank_node_policy: "object-scoped-label-v1".to_owned(),
        total_objects: plan.total_objects,
        total_bytes: fragments.iter().map(|fragment| fragment.bytes).sum(),
        total_quads: plan.total_quads,
        expected_completions: plan.total_work_items,
        verified_completions: plan.total_work_items,
        completion_set_sha256: hex::encode(aggregate.finalize()),
        fragments,
    };
    let bytes = serde_json::to_vec_pretty(&handoff)?;
    let sha256 = hex::encode(Sha256::digest(&bytes));
    let path = scratch_root.join("compiler-handoff.json");
    fs::write(&path, &bytes)?;
    let key = format!(
        "imports/{}/{}/{}/compiler-handoff.json",
        plan.tenant_id, plan.dataset_id, plan.operation_id
    );
    store
        .put_file_immutable(
            &key,
            &sha256,
            &path,
            required_u64(options, "single-put-max-bytes").map_err(CloudDecodeError::Config)?,
            required_usize(options, "multipart-buffer-bytes")
                .map_err(CloudDecodeError::Config)?,
            required_usize(options, "multipart-concurrency")
                .map_err(CloudDecodeError::Config)?,
        )
        .await?;
    let namespace = required_value(options, "namespace").map_err(CloudDecodeError::Config)?;
    let import_name = required_value(options, "import-name").map_err(CloudDecodeError::Config)?;
    let imports: Api<NgkgSourceImport> = Api::namespaced(Client::try_default().await?, &namespace);
    let import = imports.get(&import_name).await?;
    let status = NgkgSourceImportStatus {
        compiler_handoff_object_key: Some(key.clone()),
        compiler_handoff_sha256: Some(sha256.clone()),
        condition: Some("CompilerHandoffPublished".to_owned()),
        ..import.status.unwrap_or_default()
    };
    imports
        .patch_status(
            &import_name,
            &PatchParams::default(),
            &Patch::Merge(serde_json::json!({"status": status})),
        )
        .await?;
    Ok(serde_json::json!({
        "status": "compiler-handoff-published",
        "compilerHandoffObjectKey": key,
        "compilerHandoffSha256": sha256,
        "verifiedCompletions": plan.total_work_items,
        "decodedObjects": plan.total_objects,
        "decodedQuads": plan.total_quads
    })
    .to_string())
}

fn decode_object(
    source_root: &Path,
    scratch_root: &Path,
    plan: &CloudDecodePlan,
    object: &FrozenCloudSourceObject,
    maximum_fragment_bytes: u64,
) -> Result<DecodedObjectFragment, CloudDecodeError> {
    let relative = safe_relative_path(&object.object_key)?;
    let source = source_root.join(&relative);
    let before = fs::symlink_metadata(&source)?;
    if before.file_type().is_symlink() || !before.is_file() || before.len() != object.bytes {
        return Err(CloudDecodeError::Config(format!(
            "frozen source object changed before decode: {}",
            object.object_key
        )));
    }
    let state = Arc::new(Mutex::new((Sha256::new(), 0_u64)));
    let reader = HashingReader {
        inner: BufReader::new(File::open(&source)?),
        state: Arc::clone(&state),
    };
    let output_path = scratch_root.join(fragment_file_name(object.ordinal));
    let writer = BoundedWriter {
        inner: BufWriter::new(File::create(&output_path)?),
        written: 0,
        maximum: maximum_fragment_bytes,
    };
    let mut serializer = RdfSerializer::from_format(RdfFormat::NQuads).for_writer(writer);
    let mut quad_count = 0_u64;
    for parsed in RdfParser::from_format(RdfFormat::TriG).for_reader(reader) {
        let quad = parsed.map_err(|error| CloudDecodeError::Trig {
            object_key: object.object_key.clone(),
            detail: error.to_string(),
        })?;
        serializer
            .serialize_quad(&quad)
            .map_err(|error| CloudDecodeError::Trig {
                object_key: object.object_key.clone(),
                detail: error.to_string(),
            })?;
        quad_count = quad_count
            .checked_add(1)
            .ok_or_else(|| CloudDecodeError::Config("decoded quad count overflow".to_owned()))?;
    }
    let mut writer = serializer.finish().map_err(|error| CloudDecodeError::Trig {
        object_key: object.object_key.clone(),
        detail: error.to_string(),
    })?;
    writer.flush()?;
    writer.inner.get_ref().sync_all()?;
    let after = fs::symlink_metadata(&source)?;
    let observed = state
        .lock()
        .map_err(|_| CloudDecodeError::Config("source hash lock was poisoned".to_owned()))?;
    let observed_sha256 = hex::encode(observed.0.clone().finalize());
    if before.len() != after.len()
        || before.modified().ok() != after.modified().ok()
        || observed.1 != object.bytes
        || observed_sha256 != object.sha256
        || quad_count != object.parsed_quad_count
    {
        return Err(CloudDecodeError::Config(format!(
            "frozen source identity or quad count changed during decode: {}",
            object.object_key
        )));
    }
    drop(observed);
    let bytes = fs::metadata(&output_path)?.len();
    let sha256 = hash_path(&output_path)?;
    Ok(DecodedObjectFragment {
        ordinal: object.ordinal,
        source_object_key: object.object_key.clone(),
        source_sha256: object.sha256.clone(),
        blank_node_scope: object.blank_node_scope.clone(),
        object_key: format!(
            "imports/{}/{}/{}/decoded/objects/{}",
            plan.tenant_id,
            plan.dataset_id,
            plan.operation_id,
            fragment_file_name(object.ordinal)
        ),
        sha256,
        bytes,
        quad_count,
    })
}

async fn load_and_verify_completion(
    store: &ArtifactStore,
    scratch_root: &Path,
    plan: &CloudDecodePlan,
    plan_sha256: &str,
    work: &CloudDecodeWorkItem,
    completion_limit: u64,
    fragment_limit: u64,
) -> Result<DecodeCompletionManifest, CloudDecodeError> {
    let key = completion_key(plan, work.completion_index);
    let path = scratch_root.join(format!("completion-{:08}.json", work.completion_index));
    let bytes = store
        .materialize_unverified_bounded(&key, completion_limit, &path)
        .await?;
    if bytes == 0 {
        return Err(CloudDecodeError::Config("empty completion manifest".to_owned()));
    }
    let completion: DecodeCompletionManifest = serde_json::from_slice(&fs::read(&path)?)?;
    if completion.format_version != 1
        || completion.tenant_id != plan.tenant_id
        || completion.dataset_id != plan.dataset_id
        || completion.operation_id != plan.operation_id
        || completion.target_snapshot_id != plan.target_snapshot_id
        || completion.decode_plan_sha256 != plan_sha256
        || completion.completion_index != work.completion_index
        || completion.work_id != work.work_id
        || completion.total_bytes != work.total_bytes
        || completion.total_quads != work.total_quads
        || completion.fragments.len() != work.objects.len()
    {
        return Err(CloudDecodeError::Config(format!(
            "decode completion {} does not match its immutable work item",
            work.completion_index
        )));
    }
    for (fragment, source) in completion.fragments.iter().zip(&work.objects) {
        if fragment.ordinal != source.ordinal
            || fragment.source_object_key != source.object_key
            || fragment.source_sha256 != source.sha256
            || fragment.blank_node_scope != source.blank_node_scope
            || fragment.quad_count != source.parsed_quad_count
        {
            return Err(CloudDecodeError::Config(format!(
                "decoded fragment {} does not match its frozen source",
                fragment.ordinal
            )));
        }
        let verified_bytes = store
            .verify_remote(&fragment.object_key, &fragment.sha256, fragment_limit)
            .await?;
        if verified_bytes != fragment.bytes {
            return Err(CloudDecodeError::Config(format!(
                "decoded fragment {} byte count mismatch",
                fragment.ordinal
            )));
        }
    }
    Ok(completion)
}

fn validate_plan_identity(
    plan: &CloudDecodePlan,
    plan_key: &str,
    plan_sha256: &str,
) -> Result<(), CloudDecodeError> {
    if plan.format_version != 1
        || plan.planner != "whole-trig-lpt-v1"
        || plan.total_work_items == 0
        || plan.work_items.len() != usize::try_from(plan.total_work_items).unwrap_or(usize::MAX)
        || plan.source_manifest_object_key.is_empty()
        || plan.source_manifest_sha256.len() != 64
        || plan_key.is_empty()
        || plan_sha256.len() != 64
    {
        return Err(CloudDecodeError::Config(
            "decode plan identity or barrier is invalid".to_owned(),
        ));
    }
    Ok(())
}

fn completion_key(plan: &CloudDecodePlan, index: u32) -> String {
    format!(
        "imports/{}/{}/{}/decode-completions/{index:08}.json",
        plan.tenant_id, plan.dataset_id, plan.operation_id
    )
}

fn fragment_file_name(ordinal: u32) -> String {
    format!("object-{ordinal:08}.nq")
}

fn safe_relative_path(value: &str) -> Result<PathBuf, CloudDecodeError> {
    let path = Path::new(value);
    if value.is_empty()
        || value.contains('\\')
        || value.split('/').any(str::is_empty)
        || path.is_absolute()
        || path.components().any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(CloudDecodeError::Config(format!(
            "unsafe frozen object key: {value}"
        )));
    }
    Ok(path.to_owned())
}

fn hash_path(path: &Path) -> Result<String, CloudDecodeError> {
    let mut input = BufReader::new(File::open(path)?);
    let mut hash = Sha256::new();
    let mut buffer = vec![0_u8; 1024 * 1024];
    loop {
        let count = input.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        hash.update(&buffer[..count]);
    }
    Ok(hex::encode(hash.finalize()))
}

#[cfg(test)]
mod tests {
    use super::safe_relative_path;

    #[test]
    fn decode_never_accepts_a_path_outside_the_operator_mount() {
        assert!(safe_relative_path("asserted/oncology.trig").is_ok());
        assert!(safe_relative_path("../secrets.trig").is_err());
        assert!(safe_relative_path("asserted//bad.trig").is_err());
    }
}
