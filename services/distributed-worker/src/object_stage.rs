//! Object-store and PostgreSQL execution path for Kubernetes distributed stages.

use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

use ngkg_artifact_store::{ArtifactStore, ArtifactStoreError};
use ngkg_catalog::{
    CatalogError, DistributedArtifactRoot, DistributedRoot, DistributedServingRoot,
    DistributedWorkKind, OperationRepository, RegisterArtifactPlan, RegisterDistributedPlan,
    RegisterDistributedWork,
};
use ngkg_distributed_artifacts::{
    ArtifactPartitionManifest, ArtifactPartitionRequest, DistributedArtifactRootManifest,
    finalize_catalog_artifact_partitions, materialize_artifact_partition,
};
use ngkg_distributed_build::{
    DistributedRootManifest, ProjectionRunManifest, ReducerRunManifest, SafeScanRequest,
    SourcePlan, finalize_reducers, project_partition, reduce_projection_runs, safe_scan_trig,
};
use ngkg_hydration::{
    HydrationError, SERVING_ROOT_FORMAT_VERSION, ServingPayloadPartition, ServingRootManifest,
};
use ngkg_locator::compile_sharded_locator;
use ngkg_reference::{CompilationBundle, sha256_path};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};
use sqlx::postgres::PgPoolOptions;
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Error)]
pub(crate) enum StageError {
    #[error("distributed stage configuration failed: {0}")]
    Config(String),
    #[error("distributed stage catalog failed: {0}")]
    Catalog(#[from] CatalogError),
    #[error("distributed stage object store failed: {0}")]
    Store(#[from] ArtifactStoreError),
    #[error("distributed stage I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("distributed stage JSON failed: {0}")]
    Json(#[from] serde_json::Error),
    #[error("distributed build failed: {0}")]
    Build(#[from] ngkg_distributed_build::DistributedBuildError),
    #[error("distributed semantic artifacts failed: {0}")]
    Artifact(#[from] ngkg_distributed_artifacts::DistributedArtifactError),
    #[error("distributed serving root failed: {0}")]
    Hydration(#[from] HydrationError),
    #[error("binary locator compilation failed: {0}")]
    Locator(#[from] ngkg_locator::LocatorError),
    #[error("distributed stage PostgreSQL connection failed: {0}")]
    Database(#[from] sqlx::Error),
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct ArtifactObjectPlan {
    format_version: u32,
    dataset_id: Uuid,
    snapshot_id: Uuid,
    source_plan_object_key: String,
    source_plan_sha256: String,
    dictionary_object_key: String,
    dictionary_sha256: String,
    partition_count: u32,
    row_group_rows: usize,
    work: Vec<ArtifactObjectWork>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct ArtifactObjectWork {
    partition_index: u32,
    partition_id: String,
    source_shard_object_key: String,
    source_shard_sha256: String,
}

struct Runtime {
    tenant_id: Uuid,
    operation_id: Uuid,
    scratch: PathBuf,
    store: ArtifactStore,
    catalog: OperationRepository,
    max_object_bytes: u64,
    single_put_max_bytes: u64,
    multipart_buffer_bytes: usize,
    multipart_concurrency: usize,
}

pub(crate) async fn plan(options: &BTreeMap<String, String>) -> Result<String, StageError> {
    reject_unknown(
        options,
        &[
            "tenant-id",
            "operation-id",
            "dataset-id",
            "target-snapshot-id",
            "bundle-object-key",
            "bundle-sha256",
            "scratch-root",
            "logical-partitions",
            "reducer-count",
            "max-quads",
            "max-object-bytes",
            "single-put-max-bytes",
            "multipart-buffer-bytes",
            "multipart-concurrency",
        ],
    )?;
    let runtime = Runtime::new(options, "plan").await?;
    let dataset_id = uuid(options, "dataset-id")?;
    let snapshot_id = uuid(options, "target-snapshot-id")?;
    let bundle = load_bundle(&runtime, options, dataset_id, snapshot_id).await?;
    let input = runtime.scratch.join("input");
    let output = runtime.scratch.join("output");
    tokio::fs::create_dir_all(&input).await?;
    let source = input.join("source.trig");
    runtime
        .store
        .materialize_verified(
            &bundle.source.object_key,
            &bundle.source.sha256,
            runtime.max_object_bytes,
            &source,
        )
        .await?;
    let policy_path = input.join("projection-policy.json");
    let policy_bytes = serde_json::to_vec(&bundle.projection)?;
    write_new(&policy_path, &policy_bytes).await?;
    let policy_sha256 = sha256_bytes(&policy_bytes);
    let logical_partition_count = positive_u32(options, "logical-partitions")?;
    let reducer_count = positive_i32(options, "reducer-count")?;
    if reducer_count
        > i32::try_from(logical_partition_count).map_err(|_| {
            StageError::Config("logical partition count does not fit i32".to_owned())
        })?
    {
        return Err(StageError::Config(
            "reducer count exceeds logical partitions".to_owned(),
        ));
    }
    let request = SafeScanRequest {
        dataset_id,
        snapshot_id,
        dataset_namespace: bundle.dataset_namespace,
        source_guid: bundle.source_guid,
        source_snapshot: &bundle.source_snapshot,
        source_sha256: &bundle.source.sha256,
        projection_policy_sha256: &policy_sha256,
        projection_policy: &bundle.projection,
        logical_partition_count,
        max_quads: positive_u64(options, "max-quads")?,
    };
    let plan_path = safe_scan_trig(&source, &output, &request)?;
    let source_plan: SourcePlan = read_json(&plan_path)?;
    let plan_sha256 = sha256_path(&plan_path)?;
    let prefix = distributed_prefix(runtime.tenant_id, dataset_id, snapshot_id);
    for shard in &source_plan.shards {
        let local = output.join(&shard.relative_path);
        runtime
            .put(
                &format!("{prefix}/source/{}", shard.relative_path),
                &shard.sha256,
                &local,
            )
            .await?;
    }
    let source_plan_key = format!("{prefix}/source/source-plan.json");
    runtime
        .put(&source_plan_key, &plan_sha256, &plan_path)
        .await?;
    let projections = source_plan
        .shards
        .iter()
        .map(|shard| {
            Ok(RegisterDistributedWork {
                work_index: i32::try_from(shard.partition_index)
                    .map_err(|_| StageError::Config("partition index overflow".to_owned()))?,
                stable_work_id: shard.partition_id.clone(),
                input_object_key: format!("{prefix}/source/{}", shard.relative_path),
                input_sha256: decode_sha256(&shard.sha256)?,
            })
        })
        .collect::<Result<Vec<_>, StageError>>()?;
    let plan_hash_bytes = decode_sha256(&plan_sha256)?;
    let reducers = (0..reducer_count)
        .map(|index| RegisterDistributedWork {
            work_index: index,
            stable_work_id: reducer_work_id(
                dataset_id,
                snapshot_id,
                index,
                reducer_count,
                &plan_sha256,
            ),
            input_object_key: source_plan_key.clone(),
            input_sha256: plan_hash_bytes,
        })
        .collect();
    let registration = RegisterDistributedPlan {
        source_plan_object_key: source_plan_key.clone(),
        source_plan_sha256: decode_sha256(&plan_sha256)?,
        logical_partition_count: i32::try_from(logical_partition_count)
            .map_err(|_| StageError::Config("partition count overflow".to_owned()))?,
        reducer_count,
        fact_count: i64::try_from(source_plan.fact_count)
            .map_err(|_| StageError::Config("fact count overflow".to_owned()))?,
        layout_profile: source_plan.layout_profile,
        projections,
        reducers,
    };
    let summary = runtime
        .catalog
        .register_distributed_plan(
            runtime.tenant_id,
            runtime.operation_id,
            &registration,
            &format!("distributed-planner:{}", runtime.operation_id),
        )
        .await?;
    Ok(serde_json::json!({
        "status": "partitioned",
        "operationId": runtime.operation_id,
        "sourcePlanObjectKey": source_plan_key,
        "sourcePlanSha256": plan_sha256,
        "logicalPartitionCount": summary.logical_partition_count,
        "reducerCount": summary.reducer_count,
        "factCount": summary.fact_count
    })
    .to_string())
}

pub(crate) async fn project(options: &BTreeMap<String, String>) -> Result<String, StageError> {
    reject_unknown(
        options,
        &[
            "tenant-id",
            "operation-id",
            "work-index",
            "scratch-root",
            "max-quads",
            "max-object-bytes",
            "single-put-max-bytes",
            "multipart-buffer-bytes",
            "multipart-concurrency",
        ],
    )?;
    let runtime = Runtime::new(options, "projection").await?;
    let index = nonnegative_i32(options, "work-index")?;
    let work = runtime
        .catalog
        .get_distributed_work(
            runtime.tenant_id,
            runtime.operation_id,
            DistributedWorkKind::Projection,
            index,
        )
        .await?;
    if work.state == "SUCCEEDED" {
        return Ok(serde_json::json!({"status":"already-projected","workIndex":index}).to_string());
    }
    let durable = runtime
        .catalog
        .get_compilation(runtime.tenant_id, runtime.operation_id)
        .await?;
    let bundle = materialize_durable_bundle(
        &runtime,
        &durable.request.bundle_object_key,
        &hex::encode(durable.request.bundle_sha256),
    )
    .await?;
    validate_bundle_identity(&bundle, &durable)?;
    let summary = runtime
        .catalog
        .get_distributed_plan(runtime.tenant_id, runtime.operation_id)
        .await?;
    let input = runtime.scratch.join("input");
    let output = runtime.scratch.join("output");
    tokio::fs::create_dir_all(input.join("shards")).await?;
    let plan_path = input.join("source-plan.json");
    runtime
        .store
        .materialize_verified(
            &summary.source_plan_object_key,
            &summary.source_plan_sha256,
            runtime.max_object_bytes,
            &plan_path,
        )
        .await?;
    let plan: SourcePlan = read_json(&plan_path)?;
    let shard = plan
        .shards
        .get(
            usize::try_from(index)
                .map_err(|_| StageError::Config("work index overflow".to_owned()))?,
        )
        .ok_or_else(|| StageError::Config("work index is absent from source plan".to_owned()))?;
    if work.stable_work_id != shard.partition_id || work.input_sha256 != shard.sha256 {
        return Err(StageError::Config(
            "catalog work differs from source plan".to_owned(),
        ));
    }
    let shard_path = input.join(&shard.relative_path);
    runtime
        .store
        .materialize_verified(
            &work.input_object_key,
            &work.input_sha256,
            runtime.max_object_bytes,
            &shard_path,
        )
        .await?;
    let policy_sha256 = sha256_bytes(&serde_json::to_vec(&bundle.projection)?);
    if policy_sha256 != plan.projection_policy_sha256 {
        return Err(StageError::Config(
            "bundle policy differs from source plan".to_owned(),
        ));
    }
    let manifest_path = project_partition(
        &plan_path,
        &summary.source_plan_sha256,
        u32::try_from(index).map_err(|_| StageError::Config("work index overflow".to_owned()))?,
        bundle.dataset_namespace,
        bundle.source_guid,
        &bundle.source_snapshot,
        &bundle.projection,
        &output,
        positive_u64(options, "max-quads")?,
    )?;
    let manifest: ProjectionRunManifest = read_json(&manifest_path)?;
    let prefix = format!(
        "{}/projection/{index:05}",
        distributed_prefix(
            runtime.tenant_id,
            durable.operation.dataset_id,
            durable.operation.target_snapshot_id
        )
    );
    runtime
        .put(
            &format!("{prefix}/facts.nq"),
            &manifest.fact_run_sha256,
            &output.join(&manifest.fact_run_path),
        )
        .await?;
    runtime
        .put(
            &format!("{prefix}/terms.txt"),
            &manifest.term_run_sha256,
            &output.join(&manifest.term_run_path),
        )
        .await?;
    let manifest_sha256 = sha256_path(&manifest_path)?;
    let manifest_key = format!("{prefix}/projection-run.json");
    runtime
        .put(&manifest_key, &manifest_sha256, &manifest_path)
        .await?;
    runtime
        .catalog
        .commit_distributed_work(
            runtime.tenant_id,
            runtime.operation_id,
            DistributedWorkKind::Projection,
            index,
            &manifest_key,
            &decode_sha256(&manifest_sha256)?,
            &format!("projection-worker:{}:{index}", runtime.operation_id),
        )
        .await?;
    Ok(serde_json::json!({"status":"projected","workIndex":index,"manifestObjectKey":manifest_key,"manifestSha256":manifest_sha256}).to_string())
}

pub(crate) async fn reduce(options: &BTreeMap<String, String>) -> Result<String, StageError> {
    reject_unknown(
        options,
        &[
            "tenant-id",
            "operation-id",
            "work-index",
            "scratch-root",
            "max-object-bytes",
            "single-put-max-bytes",
            "multipart-buffer-bytes",
            "multipart-concurrency",
        ],
    )?;
    let runtime = Runtime::new(options, "reducer").await?;
    let index = nonnegative_i32(options, "work-index")?;
    let work = runtime
        .catalog
        .get_distributed_work(
            runtime.tenant_id,
            runtime.operation_id,
            DistributedWorkKind::Reducer,
            index,
        )
        .await?;
    if work.state == "SUCCEEDED" {
        return Ok(serde_json::json!({"status":"already-reduced","workIndex":index}).to_string());
    }
    let durable = runtime
        .catalog
        .get_compilation(runtime.tenant_id, runtime.operation_id)
        .await?;
    let summary = runtime
        .catalog
        .get_distributed_plan(runtime.tenant_id, runtime.operation_id)
        .await?;
    let projections = runtime
        .catalog
        .list_distributed_outputs(
            runtime.tenant_id,
            runtime.operation_id,
            DistributedWorkKind::Projection,
        )
        .await?;
    if projections.len()
        != usize::try_from(summary.logical_partition_count)
            .map_err(|_| StageError::Config("partition count overflow".to_owned()))?
    {
        return Err(StageError::Config(
            "projection barrier is incomplete".to_owned(),
        ));
    }
    let input = runtime.scratch.join("input");
    let output = runtime.scratch.join("output");
    tokio::fs::create_dir_all(&input).await?;
    let plan_path = input.join("source-plan.json");
    runtime
        .store
        .materialize_verified(
            &summary.source_plan_object_key,
            &summary.source_plan_sha256,
            runtime.max_object_bytes,
            &plan_path,
        )
        .await?;
    let mut manifest_paths = Vec::with_capacity(projections.len());
    for projection in projections {
        if projection.work_index % summary.reducer_count != index {
            continue;
        }
        let key = projection.output_manifest_object_key.ok_or_else(|| {
            StageError::Config("successful projection lacks manifest key".to_owned())
        })?;
        let sha = projection.output_manifest_sha256.ok_or_else(|| {
            StageError::Config("successful projection lacks manifest hash".to_owned())
        })?;
        let root = input.join(format!("projection-{:05}", projection.work_index));
        tokio::fs::create_dir_all(&root).await?;
        let path = root.join("projection-run.json");
        runtime
            .store
            .materialize_verified(&key, &sha, runtime.max_object_bytes, &path)
            .await?;
        let manifest: ProjectionRunManifest = read_json(&path)?;
        let object_prefix = object_parent(&key)?;
        runtime
            .store
            .materialize_verified(
                &format!("{object_prefix}/{}", manifest.fact_run_path),
                &manifest.fact_run_sha256,
                runtime.max_object_bytes,
                &root.join(&manifest.fact_run_path),
            )
            .await?;
        runtime
            .store
            .materialize_verified(
                &format!("{object_prefix}/{}", manifest.term_run_path),
                &manifest.term_run_sha256,
                runtime.max_object_bytes,
                &root.join(&manifest.term_run_path),
            )
            .await?;
        manifest_paths.push(path);
    }
    let manifest_path = reduce_projection_runs(
        &plan_path,
        &summary.source_plan_sha256,
        &manifest_paths,
        u32::try_from(index).map_err(|_| StageError::Config("work index overflow".to_owned()))?,
        u32::try_from(summary.reducer_count)
            .map_err(|_| StageError::Config("reducer count overflow".to_owned()))?,
        &output,
    )?;
    let manifest: ReducerRunManifest = read_json(&manifest_path)?;
    let prefix = format!(
        "{}/reducer/{index:05}",
        distributed_prefix(
            runtime.tenant_id,
            durable.operation.dataset_id,
            durable.operation.target_snapshot_id
        )
    );
    runtime
        .put(
            &format!("{prefix}/facts.nq"),
            &manifest.fact_run_sha256,
            &output.join(&manifest.fact_run_path),
        )
        .await?;
    runtime
        .put(
            &format!("{prefix}/terms.txt"),
            &manifest.term_run_sha256,
            &output.join(&manifest.term_run_path),
        )
        .await?;
    let manifest_sha256 = sha256_path(&manifest_path)?;
    let manifest_key = format!("{prefix}/reducer-run.json");
    runtime
        .put(&manifest_key, &manifest_sha256, &manifest_path)
        .await?;
    runtime
        .catalog
        .commit_distributed_work(
            runtime.tenant_id,
            runtime.operation_id,
            DistributedWorkKind::Reducer,
            index,
            &manifest_key,
            &decode_sha256(&manifest_sha256)?,
            &format!("reducer-worker:{}:{index}", runtime.operation_id),
        )
        .await?;
    Ok(serde_json::json!({"status":"reduced","workIndex":index,"manifestObjectKey":manifest_key,"manifestSha256":manifest_sha256}).to_string())
}

pub(crate) async fn finalize(options: &BTreeMap<String, String>) -> Result<String, StageError> {
    reject_unknown(
        options,
        &[
            "tenant-id",
            "operation-id",
            "scratch-root",
            "max-object-bytes",
            "single-put-max-bytes",
            "multipart-buffer-bytes",
            "multipart-concurrency",
        ],
    )?;
    let runtime = Runtime::new(options, "finalize").await?;
    match runtime
        .catalog
        .get_distributed_root(runtime.tenant_id, runtime.operation_id)
        .await
    {
        Ok(root) => {
            return Ok(serde_json::json!({"status":"already-finalized","root":root}).to_string());
        }
        Err(CatalogError::NotFound) => {}
        Err(error) => return Err(error.into()),
    }
    let durable = runtime
        .catalog
        .get_compilation(runtime.tenant_id, runtime.operation_id)
        .await?;
    let summary = runtime
        .catalog
        .get_distributed_plan(runtime.tenant_id, runtime.operation_id)
        .await?;
    let reducers = runtime
        .catalog
        .list_distributed_outputs(
            runtime.tenant_id,
            runtime.operation_id,
            DistributedWorkKind::Reducer,
        )
        .await?;
    if reducers.len()
        != usize::try_from(summary.reducer_count)
            .map_err(|_| StageError::Config("reducer count overflow".to_owned()))?
    {
        return Err(StageError::Config(
            "reducer barrier is incomplete".to_owned(),
        ));
    }
    let input = runtime.scratch.join("input");
    let output = runtime.scratch.join("output");
    tokio::fs::create_dir_all(&input).await?;
    let plan_path = input.join("source-plan.json");
    runtime
        .store
        .materialize_verified(
            &summary.source_plan_object_key,
            &summary.source_plan_sha256,
            runtime.max_object_bytes,
            &plan_path,
        )
        .await?;
    let mut manifest_paths = Vec::with_capacity(reducers.len());
    for reducer in reducers {
        let key = reducer.output_manifest_object_key.ok_or_else(|| {
            StageError::Config("successful reducer lacks manifest key".to_owned())
        })?;
        let sha = reducer.output_manifest_sha256.ok_or_else(|| {
            StageError::Config("successful reducer lacks manifest hash".to_owned())
        })?;
        let root = input.join(format!("reducer-{:05}", reducer.work_index));
        tokio::fs::create_dir_all(&root).await?;
        let path = root.join("reducer-run.json");
        runtime
            .store
            .materialize_verified(&key, &sha, runtime.max_object_bytes, &path)
            .await?;
        let manifest: ReducerRunManifest = read_json(&path)?;
        let object_prefix = object_parent(&key)?;
        runtime
            .store
            .materialize_verified(
                &format!("{object_prefix}/{}", manifest.fact_run_path),
                &manifest.fact_run_sha256,
                runtime.max_object_bytes,
                &root.join(&manifest.fact_run_path),
            )
            .await?;
        runtime
            .store
            .materialize_verified(
                &format!("{object_prefix}/{}", manifest.term_run_path),
                &manifest.term_run_sha256,
                runtime.max_object_bytes,
                &root.join(&manifest.term_run_path),
            )
            .await?;
        manifest_paths.push(path);
    }
    let root_path = finalize_reducers(
        &plan_path,
        &summary.source_plan_sha256,
        &manifest_paths,
        &output,
    )?;
    let root: DistributedRootManifest = read_json(&root_path)?;
    let prefix = format!(
        "{}/root",
        distributed_prefix(
            runtime.tenant_id,
            durable.operation.dataset_id,
            durable.operation.target_snapshot_id
        )
    );
    let source_key = format!("{prefix}/canonical-source.nq");
    let dictionary_key = format!("{prefix}/dictionary.tsv");
    runtime
        .put(
            &source_key,
            &root.canonical_source_sha256,
            &output.join(&root.canonical_source_path),
        )
        .await?;
    runtime
        .put(
            &dictionary_key,
            &root.dictionary_sha256,
            &output.join(&root.dictionary_path),
        )
        .await?;
    let root_sha256 = sha256_path(&root_path)?;
    let root_key = format!("{prefix}/distributed-root.json");
    runtime.put(&root_key, &root_sha256, &root_path).await?;
    let registered = runtime
        .catalog
        .commit_distributed_root(
            runtime.tenant_id,
            runtime.operation_id,
            &DistributedRoot {
                root_manifest_object_key: root_key,
                root_manifest_sha256: root_sha256,
                canonical_source_object_key: source_key,
                canonical_source_sha256: root.canonical_source_sha256,
                dictionary_object_key: dictionary_key,
                dictionary_sha256: root.dictionary_sha256,
                semantic_content_sha256: root.semantic_content_sha256,
            },
        )
        .await?;
    Ok(serde_json::json!({"status":"finalized","root":registered}).to_string())
}

pub(crate) async fn prepare_artifacts(
    options: &BTreeMap<String, String>,
) -> Result<String, StageError> {
    reject_unknown(
        options,
        &[
            "tenant-id",
            "operation-id",
            "scratch-root",
            "row-group-rows",
            "max-object-bytes",
            "single-put-max-bytes",
            "multipart-buffer-bytes",
            "multipart-concurrency",
        ],
    )?;
    let runtime = Runtime::new(options, "artifact-plan").await?;
    if let Ok(plan) = runtime
        .catalog
        .get_artifact_plan(runtime.tenant_id, runtime.operation_id)
        .await
    {
        return Ok(serde_json::json!({"status":"artifact-plan-exists","plan":plan}).to_string());
    }
    let durable = runtime
        .catalog
        .get_compilation(runtime.tenant_id, runtime.operation_id)
        .await?;
    let distributed = runtime
        .catalog
        .get_distributed_plan(runtime.tenant_id, runtime.operation_id)
        .await?;
    let root = runtime
        .catalog
        .get_distributed_root(runtime.tenant_id, runtime.operation_id)
        .await?;
    let projections = runtime
        .catalog
        .list_distributed_outputs(
            runtime.tenant_id,
            runtime.operation_id,
            DistributedWorkKind::Projection,
        )
        .await?;
    if projections.len()
        != usize::try_from(distributed.logical_partition_count)
            .map_err(|_| StageError::Config("artifact partition count overflow".to_owned()))?
    {
        return Err(StageError::Config(
            "projection barrier is incomplete".to_owned(),
        ));
    }
    let row_group_rows = positive_usize(options, "row-group-rows")?;
    let row_group_rows_identity = u64::try_from(row_group_rows)
        .map_err(|_| StageError::Config("row-group size exceeds work identity".to_owned()))?;
    let work = projections
        .iter()
        .map(|item| {
            let partition_index = u32::try_from(item.work_index)
                .map_err(|_| StageError::Config("artifact work index overflow".to_owned()))?;
            Ok(ArtifactObjectWork {
                partition_index,
                partition_id: artifact_work_id(
                    durable.operation.dataset_id,
                    durable.operation.target_snapshot_id,
                    item.work_index,
                    &item.stable_work_id,
                    &root.dictionary_sha256,
                    row_group_rows_identity,
                ),
                source_shard_object_key: item.input_object_key.clone(),
                source_shard_sha256: item.input_sha256.clone(),
            })
        })
        .collect::<Result<Vec<_>, StageError>>()?;
    let plan = ArtifactObjectPlan {
        format_version: 1,
        dataset_id: durable.operation.dataset_id,
        snapshot_id: durable.operation.target_snapshot_id,
        source_plan_object_key: distributed.source_plan_object_key.clone(),
        source_plan_sha256: distributed.source_plan_sha256.clone(),
        dictionary_object_key: root.dictionary_object_key.clone(),
        dictionary_sha256: root.dictionary_sha256.clone(),
        partition_count: u32::try_from(distributed.logical_partition_count)
            .map_err(|_| StageError::Config("artifact partition count overflow".to_owned()))?,
        row_group_rows,
        work,
    };
    let output = runtime.scratch.join("output");
    tokio::fs::create_dir_all(&output).await?;
    let plan_path = output.join("artifact-plan.json");
    write_new(&plan_path, &serde_json::to_vec_pretty(&plan)?).await?;
    let plan_sha256 = sha256_path(&plan_path)?;
    let prefix = format!(
        "{}/artifacts",
        distributed_prefix(
            runtime.tenant_id,
            durable.operation.dataset_id,
            durable.operation.target_snapshot_id,
        )
    );
    let plan_key = format!("{prefix}/artifact-plan.json");
    runtime.put(&plan_key, &plan_sha256, &plan_path).await?;
    let registration = RegisterArtifactPlan {
        source_plan_object_key: plan.source_plan_object_key.clone(),
        source_plan_sha256: decode_sha256(&plan.source_plan_sha256)?,
        dictionary_object_key: plan.dictionary_object_key.clone(),
        dictionary_sha256: decode_sha256(&plan.dictionary_sha256)?,
        artifact_plan_object_key: plan_key.clone(),
        artifact_plan_sha256: decode_sha256(&plan_sha256)?,
        partition_count: i32::try_from(plan.partition_count)
            .map_err(|_| StageError::Config("artifact partition count overflow".to_owned()))?,
        row_group_rows: i32::try_from(plan.row_group_rows).map_err(|_| {
            StageError::Config("row-group size exceeds catalog encoding".to_owned())
        })?,
        work: plan
            .work
            .iter()
            .map(|item| {
                Ok(RegisterDistributedWork {
                    work_index: i32::try_from(item.partition_index).map_err(|_| {
                        StageError::Config("artifact work index overflow".to_owned())
                    })?,
                    stable_work_id: item.partition_id.clone(),
                    input_object_key: item.source_shard_object_key.clone(),
                    input_sha256: decode_sha256(&item.source_shard_sha256)?,
                })
            })
            .collect::<Result<Vec<_>, StageError>>()?,
    };
    let summary = runtime
        .catalog
        .register_artifact_plan(runtime.tenant_id, runtime.operation_id, &registration)
        .await?;
    Ok(serde_json::json!({
        "status":"artifact-plan-registered",
        "artifactPlanObjectKey":plan_key,
        "artifactPlanSha256":plan_sha256,
        "plan":summary
    })
    .to_string())
}

pub(crate) async fn materialize_artifact_object_store(
    options: &BTreeMap<String, String>,
) -> Result<String, StageError> {
    reject_unknown(
        options,
        &[
            "tenant-id",
            "operation-id",
            "work-index",
            "scratch-root",
            "max-quads",
            "max-object-bytes",
            "single-put-max-bytes",
            "multipart-buffer-bytes",
            "multipart-concurrency",
        ],
    )?;
    let runtime = Runtime::new(options, "artifact").await?;
    let index = nonnegative_i32(options, "work-index")?;
    let work = runtime
        .catalog
        .get_distributed_work(
            runtime.tenant_id,
            runtime.operation_id,
            DistributedWorkKind::Artifact,
            index,
        )
        .await?;
    if work.state == "SUCCEEDED" {
        return Ok(
            serde_json::json!({"status":"already-materialized","workIndex":index}).to_string(),
        );
    }
    let durable = runtime
        .catalog
        .get_compilation(runtime.tenant_id, runtime.operation_id)
        .await?;
    let plan_summary = runtime
        .catalog
        .get_artifact_plan(runtime.tenant_id, runtime.operation_id)
        .await?;
    let bundle = materialize_durable_bundle(
        &runtime,
        &durable.request.bundle_object_key,
        &hex::encode(durable.request.bundle_sha256),
    )
    .await?;
    validate_bundle_identity(&bundle, &durable)?;
    let input = runtime.scratch.join("input");
    let output = runtime.scratch.join("output");
    tokio::fs::create_dir_all(input.join("shards")).await?;
    let artifact_plan_path = input.join("artifact-plan.json");
    runtime
        .store
        .materialize_verified(
            &plan_summary.artifact_plan_object_key,
            &plan_summary.artifact_plan_sha256,
            runtime.max_object_bytes,
            &artifact_plan_path,
        )
        .await?;
    let artifact_plan: ArtifactObjectPlan = read_json(&artifact_plan_path)?;
    let artifact_item = artifact_plan
        .work
        .get(
            usize::try_from(index)
                .map_err(|_| StageError::Config("artifact index overflow".to_owned()))?,
        )
        .filter(|item| i32::try_from(item.partition_index).ok() == Some(index))
        .ok_or_else(|| {
            StageError::Config("artifact index is absent from artifact plan".to_owned())
        })?;
    if artifact_plan.format_version != 1
        || artifact_plan.dataset_id != durable.operation.dataset_id
        || artifact_plan.snapshot_id != durable.operation.target_snapshot_id
        || artifact_plan.source_plan_object_key != plan_summary.source_plan_object_key
        || artifact_plan.source_plan_sha256 != plan_summary.source_plan_sha256
        || artifact_plan.dictionary_object_key != plan_summary.dictionary_object_key
        || artifact_plan.dictionary_sha256 != plan_summary.dictionary_sha256
        || i32::try_from(artifact_plan.partition_count).ok() != Some(plan_summary.partition_count)
        || artifact_plan.work.len()
            != usize::try_from(plan_summary.partition_count)
                .map_err(|_| StageError::Config("artifact partition count overflow".to_owned()))?
        || i32::try_from(artifact_plan.row_group_rows).ok() != Some(plan_summary.row_group_rows)
        || artifact_item.partition_id != work.stable_work_id
        || artifact_item.source_shard_object_key != work.input_object_key
        || artifact_item.source_shard_sha256 != work.input_sha256
    {
        return Err(StageError::Config(
            "artifact plan or work item differs from catalog truth".to_owned(),
        ));
    }
    let plan_path = input.join("source-plan.json");
    runtime
        .store
        .materialize_verified(
            &plan_summary.source_plan_object_key,
            &plan_summary.source_plan_sha256,
            runtime.max_object_bytes,
            &plan_path,
        )
        .await?;
    let source_plan: SourcePlan = read_json(&plan_path)?;
    let shard = source_plan
        .shards
        .get(
            usize::try_from(index)
                .map_err(|_| StageError::Config("artifact index overflow".to_owned()))?,
        )
        .filter(|item| i32::try_from(item.partition_index).ok() == Some(index))
        .ok_or_else(|| {
            StageError::Config("artifact index is absent from source plan".to_owned())
        })?;
    if work.input_sha256 != shard.sha256 {
        return Err(StageError::Config(
            "artifact work checksum differs from source plan".to_owned(),
        ));
    }
    let shard_path = input.join(&shard.relative_path);
    runtime
        .store
        .materialize_verified(
            &work.input_object_key,
            &work.input_sha256,
            runtime.max_object_bytes,
            &shard_path,
        )
        .await?;
    let dictionary_path = input.join("dictionary.tsv");
    runtime
        .store
        .materialize_verified(
            &plan_summary.dictionary_object_key,
            &plan_summary.dictionary_sha256,
            runtime.max_object_bytes,
            &dictionary_path,
        )
        .await?;
    let request = ArtifactPartitionRequest {
        source_sha256: &source_plan.source_sha256,
        dataset_namespace: bundle.dataset_namespace,
        source_guid: bundle.source_guid,
        source_snapshot: &bundle.source_snapshot,
        projection_policy: &bundle.projection,
        max_quads: positive_u64(options, "max-quads")?,
        row_group_rows: usize::try_from(plan_summary.row_group_rows)
            .map_err(|_| StageError::Config("row-group size overflow".to_owned()))?,
    };
    let manifest_path = materialize_artifact_partition(
        &plan_path,
        &plan_summary.source_plan_sha256,
        &dictionary_path,
        &plan_summary.dictionary_sha256,
        u32::try_from(index)
            .map_err(|_| StageError::Config("artifact index overflow".to_owned()))?,
        &output,
        &request,
    )?;
    let manifest: ArtifactPartitionManifest = read_json(&manifest_path)?;
    let prefix = format!(
        "{}/artifacts/partition-{index:05}",
        distributed_prefix(
            runtime.tenant_id,
            durable.operation.dataset_id,
            durable.operation.target_snapshot_id,
        )
    );
    for artifact in &manifest.artifacts {
        runtime
            .put(
                &format!("{prefix}/{}", artifact.relative_path),
                &artifact.sha256,
                &output.join(&artifact.relative_path),
            )
            .await?;
    }
    let manifest_sha256 = sha256_path(&manifest_path)?;
    let manifest_key = format!("{prefix}/artifact-partition.json");
    runtime
        .put(&manifest_key, &manifest_sha256, &manifest_path)
        .await?;
    runtime
        .catalog
        .commit_distributed_work(
            runtime.tenant_id,
            runtime.operation_id,
            DistributedWorkKind::Artifact,
            index,
            &manifest_key,
            &decode_sha256(&manifest_sha256)?,
            &format!("artifact-worker:{}:{index}", runtime.operation_id),
        )
        .await?;
    Ok(serde_json::json!({
        "status":"artifact-partition-committed",
        "workIndex":index,
        "manifestObjectKey":manifest_key,
        "manifestSha256":manifest_sha256
    })
    .to_string())
}

pub(crate) async fn finalize_artifacts_object_store(
    options: &BTreeMap<String, String>,
) -> Result<String, StageError> {
    reject_unknown(
        options,
        &[
            "tenant-id",
            "operation-id",
            "scratch-root",
            "max-object-bytes",
            "single-put-max-bytes",
            "multipart-buffer-bytes",
            "multipart-concurrency",
        ],
    )?;
    let runtime = Runtime::new(options, "artifact-finalize").await?;
    if let Ok(root) = runtime
        .catalog
        .get_artifact_root(runtime.tenant_id, runtime.operation_id)
        .await
    {
        return Ok(serde_json::json!({"status":"artifact-root-exists","root":root}).to_string());
    }
    let durable = runtime
        .catalog
        .get_compilation(runtime.tenant_id, runtime.operation_id)
        .await?;
    let plan_summary = runtime
        .catalog
        .get_artifact_plan(runtime.tenant_id, runtime.operation_id)
        .await?;
    let outputs = runtime
        .catalog
        .list_distributed_outputs(
            runtime.tenant_id,
            runtime.operation_id,
            DistributedWorkKind::Artifact,
        )
        .await?;
    if outputs.len()
        != usize::try_from(plan_summary.partition_count)
            .map_err(|_| StageError::Config("artifact partition count overflow".to_owned()))?
    {
        return Err(StageError::Config(
            "artifact barrier is incomplete".to_owned(),
        ));
    }
    let input = runtime.scratch.join("input");
    let output = runtime.scratch.join("output");
    tokio::fs::create_dir_all(&input).await?;
    let plan_path = input.join("source-plan.json");
    runtime
        .store
        .materialize_verified(
            &plan_summary.source_plan_object_key,
            &plan_summary.source_plan_sha256,
            runtime.max_object_bytes,
            &plan_path,
        )
        .await?;
    let dictionary_path = input.join("dictionary.tsv");
    runtime
        .store
        .materialize_verified(
            &plan_summary.dictionary_object_key,
            &plan_summary.dictionary_sha256,
            runtime.max_object_bytes,
            &dictionary_path,
        )
        .await?;
    let mut manifest_paths = Vec::with_capacity(outputs.len());
    let mut manifest_keys = BTreeMap::new();
    for item in outputs {
        let manifest_key = item.output_manifest_object_key.ok_or_else(|| {
            StageError::Config("successful artifact work lacks manifest key".to_owned())
        })?;
        let manifest_sha = item.output_manifest_sha256.ok_or_else(|| {
            StageError::Config("successful artifact work lacks manifest checksum".to_owned())
        })?;
        let root = input.join(format!("partition-{:05}", item.work_index));
        tokio::fs::create_dir_all(&root).await?;
        let manifest_path = root.join("artifact-partition.json");
        runtime
            .store
            .materialize_verified(
                &manifest_key,
                &manifest_sha,
                runtime.max_object_bytes,
                &manifest_path,
            )
            .await?;
        let manifest: ArtifactPartitionManifest = read_json(&manifest_path)?;
        let locator = manifest
            .artifacts
            .iter()
            .find(|artifact| artifact.relative_path == "locator-run.tsv")
            .ok_or_else(|| StageError::Config("artifact manifest has no locator run".to_owned()))?;
        runtime
            .store
            .materialize_verified(
                &format!(
                    "{}/{}",
                    object_parent(&manifest_key)?,
                    locator.relative_path
                ),
                &locator.sha256,
                runtime.max_object_bytes,
                &root.join("locator-run.tsv"),
            )
            .await?;
        manifest_keys.insert(manifest.partition_index, manifest_key);
        manifest_paths.push(manifest_path);
    }
    let local_root_path = finalize_catalog_artifact_partitions(
        &plan_path,
        &plan_summary.source_plan_sha256,
        &dictionary_path,
        &plan_summary.dictionary_sha256,
        &manifest_paths,
        &output,
    )?;
    let mut root: DistributedArtifactRootManifest = read_json(&local_root_path)?;
    for partition in &mut root.partitions {
        partition.manifest_path = manifest_keys
            .get(&partition.partition_index)
            .cloned()
            .ok_or_else(|| {
                StageError::Config("artifact root references an unknown partition".to_owned())
            })?;
    }
    let prefix = format!(
        "{}/artifacts/root",
        distributed_prefix(
            runtime.tenant_id,
            durable.operation.dataset_id,
            durable.operation.target_snapshot_id,
        )
    );
    let locator_key = format!("{prefix}/locator.tsv");
    root.locator_path = locator_key.clone();
    let catalog_root_path = runtime.scratch.join("distributed-artifact-root.json");
    write_new(&catalog_root_path, &serde_json::to_vec_pretty(&root)?).await?;
    runtime
        .put(
            &locator_key,
            &root.locator_sha256,
            &output.join("locator.tsv"),
        )
        .await?;
    let root_sha256 = sha256_path(&catalog_root_path)?;
    let root_key = format!("{prefix}/distributed-artifact-root.json");
    runtime
        .put(&root_key, &root_sha256, &catalog_root_path)
        .await?;
    let registered = runtime
        .catalog
        .commit_artifact_root(
            runtime.tenant_id,
            runtime.operation_id,
            &DistributedArtifactRoot {
                root_manifest_object_key: root_key,
                root_manifest_sha256: root_sha256,
                locator_object_key: locator_key,
                locator_sha256: root.locator_sha256,
                semantic_content_sha256: root.semantic_content_sha256,
                fact_count: i64::try_from(root.fact_count)
                    .map_err(|_| StageError::Config("artifact fact count overflow".to_owned()))?,
                semantic_row_count: i64::try_from(root.semantic_row_count)
                    .map_err(|_| StageError::Config("semantic row count overflow".to_owned()))?,
                payload_row_count: i64::try_from(root.payload_row_count)
                    .map_err(|_| StageError::Config("payload row count overflow".to_owned()))?,
                locator_record_count: i64::try_from(root.locator_record_count)
                    .map_err(|_| StageError::Config("locator count overflow".to_owned()))?,
            },
        )
        .await?;
    Ok(serde_json::json!({"status":"artifact-root-committed","root":registered}).to_string())
}

pub(crate) async fn prepare_serving_root_object_store(
    options: &BTreeMap<String, String>,
) -> Result<String, StageError> {
    reject_unknown(
        options,
        &[
            "tenant-id",
            "operation-id",
            "scratch-root",
            "max-object-bytes",
            "single-put-max-bytes",
            "multipart-buffer-bytes",
            "multipart-concurrency",
        ],
    )?;
    let runtime = Runtime::new(options, "serving-root").await?;
    if let Ok(root) = runtime
        .catalog
        .get_serving_root(runtime.tenant_id, runtime.operation_id)
        .await
    {
        return Ok(serde_json::json!({"status":"serving-root-exists","root":root}).to_string());
    }
    let durable = runtime
        .catalog
        .get_compilation(runtime.tenant_id, runtime.operation_id)
        .await?;
    let plan = runtime
        .catalog
        .get_artifact_plan(runtime.tenant_id, runtime.operation_id)
        .await?;
    let artifact = runtime
        .catalog
        .get_artifact_root(runtime.tenant_id, runtime.operation_id)
        .await?;
    let outputs = runtime
        .catalog
        .list_distributed_outputs(
            runtime.tenant_id,
            runtime.operation_id,
            DistributedWorkKind::Artifact,
        )
        .await?;
    if outputs.len()
        != usize::try_from(plan.partition_count)
            .map_err(|_| StageError::Config("serving partition count overflow".to_owned()))?
    {
        return Err(StageError::Config(
            "serving root requires every artifact completion".to_owned(),
        ));
    }
    let input = runtime.scratch.join("input");
    let output = runtime.scratch.join("output");
    tokio::fs::create_dir_all(&input).await?;
    tokio::fs::create_dir_all(&output).await?;
    let artifact_root_path = input.join("distributed-artifact-root.json");
    runtime
        .store
        .materialize_verified(
            &artifact.root_manifest_object_key,
            &artifact.root_manifest_sha256,
            runtime.max_object_bytes,
            &artifact_root_path,
        )
        .await?;
    let artifact_manifest: DistributedArtifactRootManifest = read_json(&artifact_root_path)?;
    if artifact_manifest.dataset_id != durable.operation.dataset_id
        || artifact_manifest.snapshot_id != durable.operation.target_snapshot_id
        || artifact_manifest.locator_path != artifact.locator_object_key
        || artifact_manifest.locator_sha256 != artifact.locator_sha256
        || artifact_manifest.semantic_content_sha256 != artifact.semantic_content_sha256
        || i64::try_from(artifact_manifest.locator_record_count).ok()
            != Some(artifact.locator_record_count)
        || artifact_manifest.partitions.len() != outputs.len()
    {
        return Err(StageError::Config(
            "artifact root differs from durable catalog truth".to_owned(),
        ));
    }
    let locator_tsv = input.join("locator.tsv");
    runtime
        .store
        .materialize_verified(
            &artifact.locator_object_key,
            &artifact.locator_sha256,
            runtime.max_object_bytes,
            &locator_tsv,
        )
        .await?;
    let locator_binary = output.join("locator.bin");
    let compiled_count = compile_sharded_locator(
        &locator_tsv,
        &artifact.locator_sha256,
        durable.operation.target_snapshot_id,
        &locator_binary,
    )?;
    if i64::try_from(compiled_count).ok() != Some(artifact.locator_record_count) {
        return Err(StageError::Config(
            "compiled locator count differs from artifact root".to_owned(),
        ));
    }
    let prefix = format!(
        "{}/serving/root",
        distributed_prefix(
            runtime.tenant_id,
            durable.operation.dataset_id,
            durable.operation.target_snapshot_id,
        )
    );
    let binary_locator_object_key = format!("{prefix}/locator.bin");
    let binary_locator_sha256 = sha256_path(&locator_binary)?;
    let mut partitions = Vec::with_capacity(outputs.len());
    for (reference, catalog_output) in artifact_manifest.partitions.iter().zip(outputs.iter()) {
        let manifest_object_key = catalog_output
            .output_manifest_object_key
            .as_deref()
            .ok_or_else(|| StageError::Config("artifact output omits manifest key".to_owned()))?;
        let manifest_sha256 = catalog_output
            .output_manifest_sha256
            .as_deref()
            .ok_or_else(|| StageError::Config("artifact output omits manifest hash".to_owned()))?;
        if i32::try_from(reference.partition_index).ok() != Some(catalog_output.work_index)
            || reference.manifest_path != manifest_object_key
            || reference.manifest_sha256 != manifest_sha256
        {
            return Err(StageError::Config(
                "artifact reference differs from completion-index truth".to_owned(),
            ));
        }
        let manifest_path = input.join(format!(
            "artifact-partition-{:05}.json",
            reference.partition_index
        ));
        runtime
            .store
            .materialize_verified(
                manifest_object_key,
                manifest_sha256,
                runtime.max_object_bytes,
                &manifest_path,
            )
            .await?;
        let manifest: ArtifactPartitionManifest = read_json(&manifest_path)?;
        if manifest.dataset_id != durable.operation.dataset_id
            || manifest.snapshot_id != durable.operation.target_snapshot_id
            || manifest.partition_index != reference.partition_index
            || manifest.payload_row_count != reference.payload_row_count
            || manifest.locator_record_count != manifest.payload_row_count
        {
            return Err(StageError::Config(
                "artifact partition cannot be admitted to the serving root".to_owned(),
            ));
        }
        let payload = manifest
            .artifacts
            .iter()
            .find(|value| value.relative_path == "payload.parquet")
            .ok_or_else(|| StageError::Config("artifact partition omits payload".to_owned()))?;
        partitions.push(ServingPayloadPartition {
            partition_index: reference.partition_index,
            manifest_object_key: manifest_object_key.to_owned(),
            manifest_sha256: manifest_sha256.to_owned(),
            payload_object_key: format!(
                "{}/{}",
                object_parent(manifest_object_key)?,
                payload.relative_path
            ),
            payload_sha256: payload.sha256.clone(),
            payload_bytes: payload.bytes,
            payload_row_count: manifest.payload_row_count,
        });
    }
    let serving = ServingRootManifest {
        format_version: SERVING_ROOT_FORMAT_VERSION,
        dataset_id: durable.operation.dataset_id,
        snapshot_id: durable.operation.target_snapshot_id,
        artifact_root_object_key: artifact.root_manifest_object_key.clone(),
        artifact_root_sha256: artifact.root_manifest_sha256.clone(),
        dictionary_object_key: plan.dictionary_object_key.clone(),
        dictionary_sha256: plan.dictionary_sha256.clone(),
        source_locator_object_key: artifact.locator_object_key.clone(),
        source_locator_sha256: artifact.locator_sha256.clone(),
        binary_locator_object_key: binary_locator_object_key.clone(),
        binary_locator_sha256: binary_locator_sha256.clone(),
        semantic_content_sha256: artifact.semantic_content_sha256.clone(),
        row_group_rows: u32::try_from(plan.row_group_rows)
            .map_err(|_| StageError::Config("row-group size overflow".to_owned()))?,
        locator_record_count: compiled_count,
        partitions,
    };
    serving.validate()?;
    let serving_path = output.join("serving-root.json");
    write_new(&serving_path, &serde_json::to_vec_pretty(&serving)?).await?;
    runtime
        .put(
            &binary_locator_object_key,
            &binary_locator_sha256,
            &locator_binary,
        )
        .await?;
    let serving_root_sha256 = sha256_path(&serving_path)?;
    let serving_root_object_key = format!("{prefix}/serving-root.json");
    runtime
        .put(
            &serving_root_object_key,
            &serving_root_sha256,
            &serving_path,
        )
        .await?;
    let registered = runtime
        .catalog
        .commit_serving_root(
            runtime.tenant_id,
            runtime.operation_id,
            &DistributedServingRoot {
                serving_root_object_key,
                serving_root_sha256,
                binary_locator_object_key,
                binary_locator_sha256,
                source_locator_sha256: artifact.locator_sha256,
                semantic_content_sha256: artifact.semantic_content_sha256,
                partition_count: plan.partition_count,
                row_group_rows: plan.row_group_rows,
                locator_record_count: artifact.locator_record_count,
            },
        )
        .await?;
    Ok(serde_json::json!({"status":"serving-root-committed","root":registered}).to_string())
}

impl Runtime {
    async fn new(options: &BTreeMap<String, String>, stage: &str) -> Result<Self, StageError> {
        let tenant_id = uuid(options, "tenant-id")?;
        let operation_id = uuid(options, "operation-id")?;
        let scratch_root = path(options, "scratch-root")?;
        let scratch = scratch_root.join(format!("{operation_id}-{stage}"));
        if scratch.exists() {
            return Err(StageError::Config(format!(
                "scratch path already exists: {}",
                scratch.display()
            )));
        }
        tokio::fs::create_dir_all(&scratch).await?;
        let database_url = required_env("NGKG_DATABASE_URL")?;
        let pool = PgPoolOptions::new()
            .max_connections(4)
            .connect(&database_url)
            .await?;
        Ok(Self {
            tenant_id,
            operation_id,
            scratch,
            store: ArtifactStore::from_base_url(&required_env("NGKG_ARTIFACT_BASE_URL")?)?,
            catalog: OperationRepository::new(pool),
            max_object_bytes: positive_u64(options, "max-object-bytes")?,
            single_put_max_bytes: positive_u64(options, "single-put-max-bytes")?,
            multipart_buffer_bytes: positive_usize(options, "multipart-buffer-bytes")?,
            multipart_concurrency: positive_usize(options, "multipart-concurrency")?,
        })
    }

    async fn put(&self, key: &str, sha256: &str, source: &Path) -> Result<(), StageError> {
        self.store
            .put_file_immutable(
                key,
                sha256,
                source,
                self.single_put_max_bytes,
                self.multipart_buffer_bytes,
                self.multipart_concurrency,
            )
            .await?;
        Ok(())
    }
}

async fn load_bundle(
    runtime: &Runtime,
    options: &BTreeMap<String, String>,
    dataset_id: Uuid,
    snapshot_id: Uuid,
) -> Result<CompilationBundle, StageError> {
    let durable = runtime
        .catalog
        .get_compilation(runtime.tenant_id, runtime.operation_id)
        .await?;
    let key = value(options, "bundle-object-key")?;
    let sha = value(options, "bundle-sha256")?;
    if durable.operation.dataset_id != dataset_id
        || durable.operation.target_snapshot_id != snapshot_id
        || durable.request.bundle_object_key != key
        || hex::encode(durable.request.bundle_sha256) != sha
    {
        return Err(StageError::Config(
            "planner arguments differ from catalog request".to_owned(),
        ));
    }
    let bundle = materialize_durable_bundle(runtime, &key, &sha).await?;
    validate_bundle_identity(&bundle, &durable)?;
    Ok(bundle)
}

async fn materialize_durable_bundle(
    runtime: &Runtime,
    key: &str,
    sha: &str,
) -> Result<CompilationBundle, StageError> {
    let path = runtime.scratch.join("compilation-bundle.json");
    runtime
        .store
        .materialize_verified(key, sha, runtime.max_object_bytes, &path)
        .await?;
    read_json(&path)
}

fn validate_bundle_identity(
    bundle: &CompilationBundle,
    durable: &ngkg_catalog::CompilationOperation,
) -> Result<(), StageError> {
    if bundle.format_version != 1
        || bundle.dataset_id != durable.operation.dataset_id
        || bundle.snapshot_id != durable.operation.target_snapshot_id
        || bundle.parent_snapshot_id != durable.request.parent_snapshot_id
        || bundle.dataset_namespace != durable.identity_namespace
        || bundle.projection.policy_id != durable.policy_version
    {
        return Err(StageError::Config(
            "bundle identity differs from catalog truth".to_owned(),
        ));
    }
    Ok(())
}

fn reducer_work_id(
    dataset_id: Uuid,
    snapshot_id: Uuid,
    index: i32,
    count: i32,
    plan_sha256: &str,
) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"ngkg-reducer-range-v1");
    hasher.update(dataset_id.as_bytes());
    hasher.update(snapshot_id.as_bytes());
    hasher.update(&index.to_be_bytes());
    hasher.update(&count.to_be_bytes());
    hasher.update(plan_sha256.as_bytes());
    format!("blake3:{}", hasher.finalize().to_hex())
}

fn artifact_work_id(
    dataset_id: Uuid,
    snapshot_id: Uuid,
    index: i32,
    projection_work_id: &str,
    dictionary_sha256: &str,
    row_group_rows: u64,
) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"ngkg-artifact-partition-v1");
    hasher.update(dataset_id.as_bytes());
    hasher.update(snapshot_id.as_bytes());
    hasher.update(&index.to_be_bytes());
    hasher.update(projection_work_id.as_bytes());
    hasher.update(dictionary_sha256.as_bytes());
    hasher.update(&row_group_rows.to_be_bytes());
    format!("blake3:{}", hasher.finalize().to_hex())
}

fn distributed_prefix(tenant_id: Uuid, dataset_id: Uuid, snapshot_id: Uuid) -> String {
    format!("distributed/{tenant_id}/{dataset_id}/{snapshot_id}")
}

fn object_parent(key: &str) -> Result<&str, StageError> {
    key.rsplit_once('/')
        .map(|(parent, _)| parent)
        .filter(|parent| !parent.is_empty())
        .ok_or_else(|| StageError::Config("manifest object key has no parent".to_owned()))
}

fn read_json<T: DeserializeOwned>(path: &Path) -> Result<T, StageError> {
    serde_json::from_slice(&std::fs::read(path)?).map_err(Into::into)
}

async fn write_new(path: &Path, bytes: &[u8]) -> Result<(), StageError> {
    let mut file = tokio::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)
        .await?;
    tokio::io::AsyncWriteExt::write_all(&mut file, bytes).await?;
    file.sync_all().await?;
    Ok(())
}

fn sha256_bytes(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn decode_sha256(value: &str) -> Result<[u8; 32], StageError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(StageError::Config("invalid SHA-256".to_owned()));
    }
    let bytes = hex::decode(value).map_err(|_| StageError::Config("invalid SHA-256".to_owned()))?;
    bytes
        .try_into()
        .map_err(|_| StageError::Config("invalid SHA-256 length".to_owned()))
}

fn reject_unknown(options: &BTreeMap<String, String>, allowed: &[&str]) -> Result<(), StageError> {
    if let Some(key) = options.keys().find(|key| !allowed.contains(&key.as_str())) {
        return Err(StageError::Config(format!("unknown option --{key}")));
    }
    Ok(())
}

fn value(options: &BTreeMap<String, String>, name: &str) -> Result<String, StageError> {
    options
        .get(name)
        .filter(|value| !value.is_empty())
        .cloned()
        .ok_or_else(|| StageError::Config(format!("--{name} is required")))
}

fn path(options: &BTreeMap<String, String>, name: &str) -> Result<PathBuf, StageError> {
    value(options, name).map(PathBuf::from)
}

fn uuid(options: &BTreeMap<String, String>, name: &str) -> Result<Uuid, StageError> {
    value(options, name)?
        .parse::<Uuid>()
        .map_err(|_| StageError::Config(format!("--{name} must be a UUID")))
}

fn positive_u64(options: &BTreeMap<String, String>, name: &str) -> Result<u64, StageError> {
    value(options, name)?
        .parse::<u64>()
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| StageError::Config(format!("--{name} must be a positive integer")))
}

fn positive_u32(options: &BTreeMap<String, String>, name: &str) -> Result<u32, StageError> {
    value(options, name)?
        .parse::<u32>()
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| StageError::Config(format!("--{name} must be a positive 32-bit integer")))
}

fn positive_i32(options: &BTreeMap<String, String>, name: &str) -> Result<i32, StageError> {
    value(options, name)?
        .parse::<i32>()
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| StageError::Config(format!("--{name} must be a positive 32-bit integer")))
}

fn nonnegative_i32(options: &BTreeMap<String, String>, name: &str) -> Result<i32, StageError> {
    value(options, name)?
        .parse::<i32>()
        .ok()
        .filter(|value| *value >= 0)
        .ok_or_else(|| {
            StageError::Config(format!("--{name} must be a non-negative 32-bit integer"))
        })
}

fn positive_usize(options: &BTreeMap<String, String>, name: &str) -> Result<usize, StageError> {
    value(options, name)?
        .parse::<usize>()
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| StageError::Config(format!("--{name} must be a positive integer")))
}

fn required_env(name: &str) -> Result<String, StageError> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| StageError::Config(format!("{name} is required")))
}
