//! Restart-safe Kubernetes controller for the real atomic reference compiler.

use std::{collections::BTreeMap, env, sync::Arc, time::Duration};

use anyhow::{Context as AnyhowContext, Result};
use futures::StreamExt;
use k8s_openapi::{
    api::{
        batch::v1::{Job, JobSpec},
        core::v1::{
            CSIPersistentVolumeSource, Capabilities, Container, EmptyDirVolumeSource,
            EnvFromSource, EnvVar, EnvVarSource, ObjectFieldSelector, ObjectReference,
            PersistentVolume, PersistentVolumeClaim, PersistentVolumeClaimSpec,
            PersistentVolumeClaimVolumeSource, PersistentVolumeSpec, PodSecurityContext, PodSpec,
            PodTemplateSpec, ResourceRequirements, SeccompProfile, SecretEnvSource,
            SecretKeySelector, SecurityContext, ServiceAccount, Toleration, Volume, VolumeMount,
            VolumeResourceRequirements,
        },
        rbac::v1::{PolicyRule, Role, RoleBinding, RoleRef, Subject},
        storage::v1::CSIDriver,
    },
    apimachinery::pkg::api::resource::Quantity,
};
use kube::{
    Api, Client, Resource, ResourceExt,
    api::{DeleteParams, ObjectMeta, Patch, PatchParams, PostParams, Preconditions},
    runtime::{Controller, controller::Action, finalizer::{Event, finalizer}, watcher},
};
use ngkg_catalog::{CatalogError, JobState, OperationRepository};
use ngkg_kube::{
    CloudObjectProvider, NgkgCompilation, NgkgCompilationSpec, NgkgCompilationStatus,
    NgkgSourceImport, NgkgSourceImportStatus, source_import_status_apply_document,
};
use ngkg_operator_core::Phase40DirectCeilings;
use serde_json::json;
use sha2::{Digest, Sha256};
use sqlx::postgres::PgPoolOptions;
use thiserror::Error;

#[derive(Clone)]
struct Context {
    client: Client,
    catalog: OperationRepository,
    namespace: String,
    worker: WorkerConfig,
}

#[derive(Clone)]
struct WorkerConfig {
    image: String,
    service_account: String,
    database_secret: String,
    artifact_base_url: String,
    object_store_credentials_secret: Option<String>,
    require_source_csi_driver_discovery: bool,
    aws_s3_csi_driver: String,
    azure_blob_csi_driver: String,
    gcs_csi_driver: String,
    resource_profile: String,
    queue_name: String,
    cpu: String,
    memory: String,
    scratch_size: String,
    semantic_scratch_size: String,
    active_deadline_seconds: i64,
    ttl_seconds_after_finished: i32,
    java_tool_options: String,
    automount_service_account_token: bool,
    phase40_direct: Phase40DirectCeilings,
    options: Vec<(String, String)>,
}

const SOURCE_IMPORT_FINALIZER: &str = "ngkg.io/source-import-runtime-cleanup";

// Keep the reference Job command line identical to the worker's
// `compile-object-store` allowlist. Other values in WorkerConfig are used by
// cloud/distributed stages and must never be forwarded to this command.
const REFERENCE_COMPILE_OPTION_NAMES: &[&str] = &[
    "java-executable",
    "reasoner-adapter-jar",
    "reasoner-adapter-sha256",
    "reasoner-name",
    "reasoner-version",
    "ceiling-bundle-bytes",
    "ceiling-staged-object-bytes",
    "ceiling-staged-total-bytes",
    "ceiling-staged-artifacts",
    "ceiling-output-bytes",
    "ceiling-output-artifacts",
    "ceiling-input-bytes",
    "ceiling-quads",
    "ceiling-dictionary-terms",
    "ceiling-reasoner-seconds",
    "ceiling-parquet-row-group-rows",
    "ceiling-named-individuals",
    "ceiling-properties",
    "download-concurrency",
    "upload-concurrency",
    "single-put-max-bytes",
    "multipart-buffer-bytes",
    "multipart-concurrency",
    "hydration-worker-threads",
    "ceiling-hydration-rows",
];

#[derive(Debug, Error)]
enum OperatorError {
    #[error("catalog operation failed: {0}")]
    Catalog(#[from] CatalogError),
    #[error("Kubernetes operation failed: {0}")]
    Kubernetes(#[from] kube::Error),
    #[error("status serialization failed: {0}")]
    StatusSerialization(#[from] serde_json::Error),
    #[error("source-import finalization failed: {0}")]
    Finalization(String),
    #[error("compilation resource conflicts with durable catalog request: {0}")]
    SpecConflict(&'static str),
    #[error("resource profile is not served by this operator")]
    ResourceProfile,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter("info")
        .init();
    let namespace = required("NGKG_NAMESPACE")?;
    let database_url = required("NGKG_DATABASE_URL")?;
    let client = Client::try_default().await?;
    let pool = PgPoolOptions::new()
        .max_connections(16)
        .connect(&database_url)
        .await?;
    let worker = WorkerConfig::from_env()?;
    let context = Arc::new(Context {
        client: client.clone(),
        catalog: OperationRepository::new(pool),
        namespace: namespace.clone(),
        worker,
    });
    let compilations: Api<NgkgCompilation> = Api::namespaced(client.clone(), &namespace);
    let imports: Api<NgkgSourceImport> = Api::namespaced(client.clone(), &namespace);
    let jobs: Api<Job> = Api::namespaced(client, &namespace);
    let compilation_controller = Controller::new(compilations, watcher::Config::default())
        .owns(jobs.clone(), watcher::Config::default())
        .run(reconcile, error_policy, Arc::clone(&context))
        .for_each(|result| async move {
            if let Err(error) = result {
                tracing::error!(%error, "reconciliation stream failed");
            }
        });
    let import_controller = Controller::new(imports, watcher::Config::default())
        .owns(jobs, watcher::Config::default())
        .run(reconcile_import, import_error_policy, context)
        .for_each(|result| async move {
            if let Err(error) = result {
                tracing::error!(%error, "source-import reconciliation stream failed");
            }
        });
    futures::future::join(compilation_controller, import_controller).await;
    Ok(())
}

impl WorkerConfig {
    fn from_env() -> Result<Self> {
        let active_deadline_seconds = positive_i64("NGKG_REFERENCE_ACTIVE_DEADLINE_SECONDS")?;
        let ttl_seconds_after_finished = positive_i32("NGKG_REFERENCE_TTL_SECONDS_AFTER_FINISHED")?;
        let options = [
            ("java-executable", "NGKG_REFERENCE_JAVA_EXECUTABLE"),
            ("reasoner-adapter-jar", "NGKG_REASONER_ADAPTER_JAR"),
            ("reasoner-adapter-sha256", "NGKG_REASONER_ADAPTER_SHA256"),
            ("reasoner-name", "NGKG_REASONER_NAME"),
            ("reasoner-version", "NGKG_REASONER_VERSION"),
            ("ceiling-bundle-bytes", "NGKG_CEILING_BUNDLE_BYTES"),
            (
                "ceiling-staged-object-bytes",
                "NGKG_CEILING_STAGED_OBJECT_BYTES",
            ),
            (
                "ceiling-staged-total-bytes",
                "NGKG_CEILING_STAGED_TOTAL_BYTES",
            ),
            ("ceiling-staged-artifacts", "NGKG_CEILING_STAGED_ARTIFACTS"),
            ("ceiling-output-bytes", "NGKG_CEILING_OUTPUT_BYTES"),
            ("ceiling-output-artifacts", "NGKG_CEILING_OUTPUT_ARTIFACTS"),
            ("ceiling-input-bytes", "NGKG_CEILING_INPUT_BYTES"),
            ("ceiling-quads", "NGKG_CEILING_QUADS"),
            ("ceiling-dictionary-terms", "NGKG_CEILING_DICTIONARY_TERMS"),
            ("ceiling-reasoner-seconds", "NGKG_CEILING_REASONER_SECONDS"),
            (
                "ceiling-parquet-row-group-rows",
                "NGKG_CEILING_PARQUET_ROW_GROUP_ROWS",
            ),
            (
                "ceiling-named-individuals",
                "NGKG_CEILING_NAMED_INDIVIDUALS",
            ),
            ("ceiling-properties", "NGKG_CEILING_PROPERTIES"),
            ("download-concurrency", "NGKG_DOWNLOAD_CONCURRENCY"),
            ("upload-concurrency", "NGKG_UPLOAD_CONCURRENCY"),
            ("single-put-max-bytes", "NGKG_SINGLE_PUT_MAX_BYTES"),
            ("multipart-buffer-bytes", "NGKG_MULTIPART_BUFFER_BYTES"),
            ("multipart-concurrency", "NGKG_MULTIPART_CONCURRENCY"),
            (
                "decode-target-work-bytes",
                "NGKG_CLOUD_DECODE_TARGET_WORK_BYTES",
            ),
            ("decode-max-work-items", "NGKG_CLOUD_DECODE_MAX_WORK_ITEMS"),
            ("decode-max-plan-bytes", "NGKG_CLOUD_DECODE_MAX_PLAN_BYTES"),
            (
                "decode-max-completion-manifest-bytes",
                "NGKG_CLOUD_DECODE_MAX_COMPLETION_MANIFEST_BYTES",
            ),
            (
                "decode-max-fragment-bytes",
                "NGKG_CLOUD_DECODE_MAX_FRAGMENT_BYTES",
            ),
            (
                "decode-finalize-concurrency",
                "NGKG_CLOUD_DECODE_FINALIZE_CONCURRENCY",
            ),
            (
                "decode-object-concurrency",
                "NGKG_CLOUD_DECODE_OBJECT_CONCURRENCY",
            ),
            (
                "decode-max-parallelism",
                "NGKG_CLOUD_DECODE_MAX_PARALLELISM",
            ),
            (
                "semantic-map-max-parallelism",
                "NGKG_SEMANTIC_MAP_MAX_PARALLELISM",
            ),
            (
                "semantic-partition-max-parallelism",
                "NGKG_SEMANTIC_PARTITION_MAX_PARALLELISM",
            ),
            (
                "semantic-max-manifest-bytes",
                "NGKG_SEMANTIC_MAX_MANIFEST_BYTES",
            ),
            (
                "semantic-max-fragment-bytes",
                "NGKG_SEMANTIC_MAX_FRAGMENT_BYTES",
            ),
            (
                "semantic-max-fragment-quads",
                "NGKG_SEMANTIC_MAX_FRAGMENT_QUADS",
            ),
            (
                "semantic-map-rows-in-memory",
                "NGKG_SEMANTIC_MAP_ROWS_IN_MEMORY",
            ),
            ("semantic-max-run-bytes", "NGKG_SEMANTIC_MAX_RUN_BYTES"),
            (
                "semantic-max-dictionary-bytes",
                "NGKG_SEMANTIC_MAX_DICTIONARY_BYTES",
            ),
            ("semantic-max-input-runs", "NGKG_SEMANTIC_MAX_INPUT_RUNS"),
            (
                "semantic-max-partition-quads",
                "NGKG_SEMANTIC_MAX_PARTITION_QUADS",
            ),
            (
                "semantic-parquet-row-group-rows",
                "NGKG_SEMANTIC_PARQUET_ROW_GROUP_ROWS",
            ),
            (
                "semantic-max-artifact-bytes",
                "NGKG_SEMANTIC_MAX_ARTIFACT_BYTES",
            ),
            (
                "semantic-finalize-concurrency",
                "NGKG_SEMANTIC_FINALIZE_CONCURRENCY",
            ),
            (
                "ontology-project-max-parallelism",
                "NGKG_ONTOLOGY_PROJECT_MAX_PARALLELISM",
            ),
            (
                "ontology-max-manifest-bytes",
                "NGKG_ONTOLOGY_MAX_MANIFEST_BYTES",
            ),
            (
                "ontology-max-partition-quads",
                "NGKG_ONTOLOGY_MAX_PARTITION_QUADS",
            ),
            (
                "ontology-projection-rows-in-memory",
                "NGKG_ONTOLOGY_PROJECTION_ROWS_IN_MEMORY",
            ),
            (
                "ontology-max-artifact-bytes",
                "NGKG_ONTOLOGY_MAX_ARTIFACT_BYTES",
            ),
            (
                "ontology-download-concurrency",
                "NGKG_ONTOLOGY_DOWNLOAD_CONCURRENCY",
            ),
            (
                "ontology-reasoner-heap-mib",
                "NGKG_ONTOLOGY_REASONER_HEAP_MIB",
            ),
            (
                "ontology-reasoner-timeout-seconds",
                "NGKG_ONTOLOGY_REASONER_TIMEOUT_SECONDS",
            ),
            (
                "ontology-max-named-individuals",
                "NGKG_ONTOLOGY_MAX_NAMED_INDIVIDUALS",
            ),
            ("ontology-max-properties", "NGKG_ONTOLOGY_MAX_PROPERTIES"),
            (
                "offline-partition-max-parallelism",
                "NGKG_OFFLINE_PARTITION_MAX_PARALLELISM",
            ),
            ("offline-worker-cpu", "NGKG_OFFLINE_WORKER_CPU"),
            ("offline-worker-memory", "NGKG_OFFLINE_WORKER_MEMORY"),
            ("offline-scratch-size", "NGKG_OFFLINE_SCRATCH_SIZE"),
            (
                "offline-logical-partitions",
                "NGKG_OFFLINE_LOGICAL_PARTITIONS",
            ),
            (
                "offline-max-manifest-bytes",
                "NGKG_OFFLINE_MAX_MANIFEST_BYTES",
            ),
            (
                "offline-max-artifact-bytes",
                "NGKG_OFFLINE_MAX_ARTIFACT_BYTES",
            ),
            (
                "offline-max-finalizer-local-bytes",
                "NGKG_OFFLINE_MAX_FINALIZER_LOCAL_BYTES",
            ),
            ("offline-max-consequences", "NGKG_OFFLINE_MAX_CONSEQUENCES"),
            (
                "offline-plan-rows-in-memory",
                "NGKG_OFFLINE_PLAN_ROWS_IN_MEMORY",
            ),
            ("offline-max-run-bytes", "NGKG_OFFLINE_MAX_RUN_BYTES"),
            (
                "offline-parquet-row-group-rows",
                "NGKG_OFFLINE_PARQUET_ROW_GROUP_ROWS",
            ),
            (
                "offline-finalize-concurrency",
                "NGKG_OFFLINE_FINALIZE_CONCURRENCY",
            ),
            (
                "activation-max-manifest-bytes",
                "NGKG_ACTIVATION_MAX_MANIFEST_BYTES",
            ),
            (
                "activation-max-partition-bytes",
                "NGKG_ACTIVATION_MAX_PARTITION_BYTES",
            ),
            (
                "activation-max-query-dataset-bytes",
                "NGKG_ACTIVATION_MAX_QUERY_DATASET_BYTES",
            ),
            (
                "activation-verify-concurrency",
                "NGKG_ACTIVATION_VERIFY_CONCURRENCY",
            ),
        ]
        .into_iter()
        .map(|(option, variable)| Ok((option.to_owned(), required(variable)?)))
        .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            image: required_digest_image("NGKG_REFERENCE_WORKER_IMAGE")?,
            service_account: required("NGKG_REFERENCE_SERVICE_ACCOUNT")?,
            database_secret: required("NGKG_DATABASE_SECRET")?,
            artifact_base_url: required("NGKG_ARTIFACT_BASE_URL")?,
            object_store_credentials_secret: optional("NGKG_OBJECT_STORE_CREDENTIALS_SECRET"),
            require_source_csi_driver_discovery: required_bool(
                "NGKG_REQUIRE_SOURCE_CSI_DRIVER_DISCOVERY",
            )?,
            aws_s3_csi_driver: required_exact("NGKG_AWS_S3_CSI_DRIVER", "s3.csi.aws.com")?,
            azure_blob_csi_driver: required_exact(
                "NGKG_AZURE_BLOB_CSI_DRIVER",
                "blob.csi.azure.com",
            )?,
            gcs_csi_driver: required_exact("NGKG_GCS_CSI_DRIVER", "gcsfuse.csi.storage.gke.io")?,
            resource_profile: required("NGKG_REFERENCE_RESOURCE_PROFILE")?,
            queue_name: required("NGKG_REFERENCE_QUEUE")?,
            cpu: required("NGKG_REFERENCE_CPU")?,
            memory: required("NGKG_REFERENCE_MEMORY")?,
            scratch_size: required("NGKG_REFERENCE_SCRATCH_SIZE")?,
            semantic_scratch_size: required("NGKG_SEMANTIC_SCRATCH_SIZE")?,
            active_deadline_seconds,
            ttl_seconds_after_finished,
            java_tool_options: required("NGKG_REFERENCE_JAVA_TOOL_OPTIONS")?,
            automount_service_account_token: required_bool(
                "NGKG_REFERENCE_AUTOMOUNT_SERVICE_ACCOUNT_TOKEN",
            )?,
            phase40_direct: Phase40DirectCeilings::from_env()
                .context("invalid Phase 40 direct ceiling bundle")?,
            options,
        })
    }
}

async fn reconcile(
    compilation: Arc<NgkgCompilation>,
    context: Arc<Context>,
) -> Result<Action, OperatorError> {
    let durable = context
        .catalog
        .get_compilation(compilation.spec.tenant_id, compilation.spec.operation_id)
        .await?;
    verify_spec(&compilation.spec, &durable)?;
    if compilation.spec.resource_profile != context.worker.resource_profile {
        return Ok(Action::await_change());
    }
    let job_name = format!("{}-reference", compilation.name_any());
    match durable.operation.state {
        JobState::Registered => {
            ensure_reference_job(&compilation, &context, &job_name).await?;
            if let Some((error_code, condition)) =
                reference_job_terminal_without_catalog_commit(&context, &job_name).await?
            {
                let failed = context
                    .catalog
                    .fail(
                        compilation.spec.tenant_id,
                        compilation.spec.operation_id,
                        error_code,
                        None,
                        "ngkg-operator",
                    )
                    .await?;
                patch_status(
                    &compilation,
                    &context,
                    Some(job_name),
                    failed.state,
                    condition,
                )
                .await?;
                return Ok(Action::await_change());
            }
            patch_status(
                &compilation,
                &context,
                Some(job_name),
                durable.operation.state,
                "Scheduled",
            )
            .await?;
            Ok(Action::requeue(Duration::from_secs(15)))
        }
        JobState::Cancelled => {
            delete_job_if_present(&context, &job_name).await?;
            patch_status(
                &compilation,
                &context,
                None,
                durable.operation.state,
                "Cancelled",
            )
            .await?;
            Ok(Action::await_change())
        }
        JobState::Failed => {
            patch_status(
                &compilation,
                &context,
                Some(job_name),
                durable.operation.state,
                "TerminalFailure",
            )
            .await?;
            Ok(Action::await_change())
        }
        JobState::Certified | JobState::Published => {
            patch_status(
                &compilation,
                &context,
                Some(job_name),
                durable.operation.state,
                durable.operation.state.as_db(),
            )
            .await?;
            Ok(Action::await_change())
        }
        _ => {
            patch_status(
                &compilation,
                &context,
                Some(job_name),
                durable.operation.state,
                "AtomicCommitInProgress",
            )
            .await?;
            Ok(Action::requeue(Duration::from_secs(5)))
        }
    }
}

fn error_policy(
    _object: Arc<NgkgCompilation>,
    error: &OperatorError,
    _context: Arc<Context>,
) -> Action {
    tracing::error!(%error, "reconciliation failed closed");
    Action::requeue(Duration::from_secs(30))
}

async fn reconcile_import(
    import: Arc<NgkgSourceImport>,
    context: Arc<Context>,
) -> Result<Action, OperatorError> {
    let imports: Api<NgkgSourceImport> =
        Api::namespaced(context.client.clone(), &context.namespace);
    finalizer(&imports, SOURCE_IMPORT_FINALIZER, import, move |event| {
        let context = context.clone();
        async move {
            match event {
                Event::Apply(import) => reconcile_import_apply(import, context).await,
                Event::Cleanup(import) => cleanup_import_runtime(import, context).await,
            }
        }
    })
    .await
    .map_err(|error| OperatorError::Finalization(error.to_string()))
}

async fn reconcile_import_apply(
    import: Arc<NgkgSourceImport>,
    context: Arc<Context>,
) -> Result<Action, OperatorError> {
    if import.spec.resource_profile != context.worker.resource_profile {
        return Ok(Action::await_change());
    }
    let current = import.status.clone().unwrap_or_default();
    if current.snapshot_activation_manifest_sha256.is_some() {
        return Ok(Action::await_change());
    }
    let base = format!("ngkg-import-{}", import.spec.operation_id.simple());
    if current.offline_reasoning_root_sha256.is_some() {
        return reconcile_import_activation(&import, &context, &base, current).await;
    }
    if current.ontology_qualification_root_sha256.is_some() {
        return reconcile_import_offline_reasoning(&import, &context, &base, current).await;
    }
    if current.semantic_compilation_root_sha256.is_some() {
        return reconcile_import_ontology(&import, &context, &base, current).await;
    }
    if current.compiler_handoff_sha256.is_some() {
        return reconcile_import_semantic(&import, &context, &base, current).await;
    }
    ensure_import_volume(&import, &context, &base).await?;
    ensure_import_worker_rbac(&import, &context, &base).await?;
    if current.decode_plan_sha256.is_some() {
        return reconcile_import_decode(&import, &context, &base, current).await;
    }
    ensure_import_job(&import, &context, &base).await?;
    let jobs: Api<Job> = Api::namespaced(context.client.clone(), &context.namespace);
    if let Some(job) = jobs.get_opt(&base).await? {
        if job_condition_is_true(&job, "Failed") {
            patch_import_status(
                &import,
                &context,
                NgkgSourceImportStatus {
                    observed_generation: import.metadata.generation,
                    job_name: Some(base),
                    condition: Some("SourceDiscoveryFailed".to_owned()),
                    ..current.clone()
                },
            )
            .await?;
            return Ok(Action::await_change());
        }
        if job_condition_is_true(&job, "Complete") {
            let imports: Api<NgkgSourceImport> =
                Api::namespaced(context.client.clone(), &context.namespace);
            let refreshed = imports.get(&import.name_any()).await?;
            if refreshed
                .status
                .as_ref()
                .and_then(|status| status.source_manifest_sha256.as_deref())
                .is_some()
            {
                return Ok(Action::await_change());
            }
            patch_import_status(
                &import,
                &context,
                NgkgSourceImportStatus {
                    observed_generation: import.metadata.generation,
                    job_name: Some(base),
                    condition: Some("SourceManifestMissing".to_owned()),
                    ..current.clone()
                },
            )
            .await?;
            return Ok(Action::await_change());
        }
    }
    patch_import_status(
        &import,
        &context,
        NgkgSourceImportStatus {
            observed_generation: import.metadata.generation,
            job_name: Some(base.clone()),
            condition: Some("SourceDiscoveryScheduled".to_owned()),
            ..current
        },
    )
    .await?;
    Ok(Action::requeue(Duration::from_secs(15)))
}

async fn cleanup_import_runtime(
    import: Arc<NgkgSourceImport>,
    context: Arc<Context>,
) -> Result<Action, OperatorError> {
    let base = format!("ngkg-import-{}", import.spec.operation_id.simple());
    let jobs: Api<Job> = Api::namespaced(context.client.clone(), &context.namespace);
    let selector = format!("ngkg.io/operation-id={}", import.spec.operation_id);
    for job in jobs.list(&kube::api::ListParams::default().labels(&selector)).await? {
        if let Some(name) = job.metadata.name.as_deref() {
            match jobs.delete(name, &DeleteParams::default()).await {
                Ok(_) => {}
                Err(kube::Error::Api(kube_error)) if kube_error.code == 404 => {}
                Err(error) => return Err(error.into()),
            }
        }
    }
    let claims: Api<PersistentVolumeClaim> =
        Api::namespaced(context.client.clone(), &context.namespace);
    if let Some(claim) = claims.get_opt(&base).await? {
        let owned = claim.metadata.labels.as_ref().and_then(|labels| labels.get("ngkg.io/operation-id"))
            == Some(&import.spec.operation_id.to_string())
            && claim.spec.as_ref().and_then(|spec| spec.volume_name.as_deref()) == Some(base.as_str());
        if !owned {
            return Err(OperatorError::Finalization("refusing to delete an unowned source-import PVC".to_owned()));
        }
        let uid = claim.metadata.uid.ok_or_else(|| OperatorError::Finalization("source-import PVC has no UID".to_owned()))?;
        let params = DeleteParams { preconditions: Some(Preconditions { uid: Some(uid), resource_version: claim.metadata.resource_version }), ..DeleteParams::default() };
        match claims.delete(&base, &params).await {
            Ok(_) => {}
            Err(kube::Error::Api(kube_error)) if kube_error.code == 404 => {}
            Err(error) => return Err(error.into()),
        }
    }
    let volumes: Api<PersistentVolume> = Api::all(context.client.clone());
    if let Some(volume) = volumes.get_opt(&base).await? {
        validate_import_volume(&volume, &import, &context, &base).await?;
        let uid = volume.metadata.uid.ok_or_else(|| OperatorError::Finalization("source-import PV has no UID".to_owned()))?;
        let params = DeleteParams { preconditions: Some(Preconditions { uid: Some(uid), resource_version: volume.metadata.resource_version }), ..DeleteParams::default() };
        match volumes.delete(&base, &params).await {
            Ok(_) => {}
            Err(kube::Error::Api(kube_error)) if kube_error.code == 404 => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(Action::await_change())
}

async fn reconcile_import_offline_reasoning(
    import: &NgkgSourceImport,
    context: &Context,
    base: &str,
    current: NgkgSourceImportStatus,
) -> Result<Action, OperatorError> {
    let jobs: Api<Job> = Api::namespaced(context.client.clone(), &context.namespace);
    let plan_name = format!("{base}-offline-plan");
    ensure_offline_plan_job(import, context, &plan_name, &current).await?;
    let Some(plan_job) = jobs.get_opt(&plan_name).await? else {
        return Ok(Action::requeue(Duration::from_secs(5)));
    };
    if job_condition_is_true(&plan_job, "Failed") {
        patch_import_status(
            import,
            context,
            NgkgSourceImportStatus {
                offline_reasoning_plan_job_name: Some(plan_name),
                condition: Some("OfflineReasoningPlanFailed".to_owned()),
                ..current
            },
        )
        .await?;
        return Ok(Action::await_change());
    }
    if !job_condition_is_true(&plan_job, "Complete")
        || current.offline_reasoning_plan_sha256.is_none()
        || current.offline_reasoning_partition_count.is_none()
    {
        patch_import_status(
            import,
            context,
            NgkgSourceImportStatus {
                offline_reasoning_plan_job_name: Some(plan_name),
                condition: Some("OfflineReasoningPlanRunning".to_owned()),
                ..current
            },
        )
        .await?;
        return Ok(Action::requeue(Duration::from_secs(15)));
    }
    let completions = current
        .offline_reasoning_partition_count
        .filter(|value| *value > 0)
        .ok_or(OperatorError::SpecConflict(
            "offlineReasoningPartitionCount",
        ))?;
    let partition_name = format!("{base}-offline-part");
    ensure_offline_partition_job(import, context, &partition_name, completions, &current).await?;
    let Some(partition_job) = jobs.get_opt(&partition_name).await? else {
        return Ok(Action::requeue(Duration::from_secs(5)));
    };
    if job_condition_is_true(&partition_job, "Failed") {
        patch_import_status(
            import,
            context,
            NgkgSourceImportStatus {
                offline_reasoning_plan_job_name: Some(plan_name),
                offline_reasoning_partition_job_name: Some(partition_name),
                condition: Some("OfflineReasoningPartitionFailed".to_owned()),
                ..current
            },
        )
        .await?;
        return Ok(Action::await_change());
    }
    if !job_condition_is_true(&partition_job, "Complete") {
        patch_import_status(
            import,
            context,
            NgkgSourceImportStatus {
                offline_reasoning_plan_job_name: Some(plan_name),
                offline_reasoning_partition_job_name: Some(partition_name),
                condition: Some("OfflineReasoningPartitionsRunning".to_owned()),
                ..current
            },
        )
        .await?;
        return Ok(Action::requeue(Duration::from_secs(15)));
    }
    let finalize_name = format!("{base}-offline-final");
    ensure_offline_finalize_job(import, context, &finalize_name, &current).await?;
    let Some(finalize_job) = jobs.get_opt(&finalize_name).await? else {
        return Ok(Action::requeue(Duration::from_secs(5)));
    };
    if job_condition_is_true(&finalize_job, "Failed") {
        patch_import_status(
            import,
            context,
            NgkgSourceImportStatus {
                offline_reasoning_plan_job_name: Some(plan_name),
                offline_reasoning_partition_job_name: Some(partition_name),
                offline_reasoning_finalize_job_name: Some(finalize_name),
                condition: Some("OfflineReasoningFinalizeFailed".to_owned()),
                ..current
            },
        )
        .await?;
        return Ok(Action::await_change());
    }
    patch_import_status(
        import,
        context,
        NgkgSourceImportStatus {
            offline_reasoning_plan_job_name: Some(plan_name),
            offline_reasoning_partition_job_name: Some(partition_name),
            offline_reasoning_finalize_job_name: Some(finalize_name),
            condition: Some("OfflineReasoningFinalizeRunning".to_owned()),
            ..current
        },
    )
    .await?;
    Ok(Action::requeue(Duration::from_secs(15)))
}

async fn reconcile_import_activation(
    import: &NgkgSourceImport,
    context: &Context,
    base: &str,
    current: NgkgSourceImportStatus,
) -> Result<Action, OperatorError> {
    let name = format!("{base}-activate");
    ensure_snapshot_activation_job(import, context, &name, &current).await?;
    let jobs: Api<Job> = Api::namespaced(context.client.clone(), &context.namespace);
    let Some(job) = jobs.get_opt(&name).await? else {
        return Ok(Action::requeue(Duration::from_secs(5)));
    };
    if job_condition_is_true(&job, "Failed") {
        patch_import_status(
            import,
            context,
            NgkgSourceImportStatus {
                snapshot_activation_job_name: Some(name),
                condition: Some("SnapshotActivationFailedClosed".to_owned()),
                ..current
            },
        )
        .await?;
        return Ok(Action::await_change());
    }
    patch_import_status(
        import,
        context,
        NgkgSourceImportStatus {
            snapshot_activation_job_name: Some(name),
            condition: Some("SnapshotActivationBarrierRunning".to_owned()),
            ..current
        },
    )
    .await?;
    Ok(Action::requeue(Duration::from_secs(10)))
}

async fn reconcile_import_ontology(
    import: &NgkgSourceImport,
    context: &Context,
    base: &str,
    current: NgkgSourceImportStatus,
) -> Result<Action, OperatorError> {
    let jobs: Api<Job> = Api::namespaced(context.client.clone(), &context.namespace);
    let projection_name = format!("{base}-owl-project");
    ensure_ontology_projection_job(import, context, &projection_name, &current).await?;
    let Some(projection) = jobs.get_opt(&projection_name).await? else {
        return Ok(Action::requeue(Duration::from_secs(5)));
    };
    if job_condition_is_true(&projection, "Failed") {
        patch_import_status(
            import,
            context,
            NgkgSourceImportStatus {
                ontology_projection_job_name: Some(projection_name),
                condition: Some("OntologyProjectionFailed".to_owned()),
                ..current
            },
        )
        .await?;
        return Ok(Action::await_change());
    }
    if !job_condition_is_true(&projection, "Complete") {
        patch_import_status(
            import,
            context,
            NgkgSourceImportStatus {
                ontology_projection_job_name: Some(projection_name),
                condition: Some("OntologyProjectionRunning".to_owned()),
                ..current
            },
        )
        .await?;
        return Ok(Action::requeue(Duration::from_secs(15)));
    }
    let assembly_name = format!("{base}-owl-assemble");
    ensure_ontology_assembly_job(import, context, &assembly_name, &current).await?;
    let Some(assembly) = jobs.get_opt(&assembly_name).await? else {
        return Ok(Action::requeue(Duration::from_secs(5)));
    };
    if job_condition_is_true(&assembly, "Failed") {
        patch_import_status(
            import,
            context,
            NgkgSourceImportStatus {
                ontology_projection_job_name: Some(projection_name),
                ontology_assembly_job_name: Some(assembly_name),
                condition: Some("OntologyAssemblyFailed".to_owned()),
                ..current
            },
        )
        .await?;
        return Ok(Action::await_change());
    }
    if !job_condition_is_true(&assembly, "Complete") || current.ontology_assembly_sha256.is_none() {
        patch_import_status(
            import,
            context,
            NgkgSourceImportStatus {
                ontology_projection_job_name: Some(projection_name),
                ontology_assembly_job_name: Some(assembly_name),
                condition: Some("OntologyAssemblyRunning".to_owned()),
                ..current
            },
        )
        .await?;
        return Ok(Action::requeue(Duration::from_secs(15)));
    }
    let qualify_name = format!("{base}-owl-qualify");
    ensure_ontology_qualification_job(import, context, &qualify_name, &current).await?;
    let Some(qualify) = jobs.get_opt(&qualify_name).await? else {
        return Ok(Action::requeue(Duration::from_secs(5)));
    };
    if job_condition_is_true(&qualify, "Failed") {
        patch_import_status(
            import,
            context,
            NgkgSourceImportStatus {
                ontology_projection_job_name: Some(projection_name),
                ontology_assembly_job_name: Some(assembly_name),
                ontology_qualification_job_name: Some(qualify_name),
                condition: Some("Owl2DlQualificationFailed".to_owned()),
                ..current
            },
        )
        .await?;
        return Ok(Action::await_change());
    }
    patch_import_status(
        import,
        context,
        NgkgSourceImportStatus {
            ontology_projection_job_name: Some(projection_name),
            ontology_assembly_job_name: Some(assembly_name),
            ontology_qualification_job_name: Some(qualify_name),
            condition: Some("Owl2DlQualificationRunning".to_owned()),
            ..current
        },
    )
    .await?;
    Ok(Action::requeue(Duration::from_secs(15)))
}

async fn reconcile_import_semantic(
    import: &NgkgSourceImport,
    context: &Context,
    base: &str,
    current: NgkgSourceImportStatus,
) -> Result<Action, OperatorError> {
    let jobs: Api<Job> = Api::namespaced(context.client.clone(), &context.namespace);
    let object_count = current
        .selected_object_count
        .filter(|count| *count > 0)
        .ok_or(OperatorError::SpecConflict("selectedObjectCount"))?;
    let map_name = format!("{base}-sem-map");
    ensure_semantic_map_job(import, context, &map_name, object_count, &current).await?;
    let Some(map_job) = jobs.get_opt(&map_name).await? else {
        return Ok(Action::requeue(Duration::from_secs(5)));
    };
    if job_condition_is_true(&map_job, "Failed") {
        patch_import_status(
            import,
            context,
            NgkgSourceImportStatus {
                semantic_map_job_name: Some(map_name),
                condition: Some("SemanticMapFailed".to_owned()),
                ..current
            },
        )
        .await?;
        return Ok(Action::await_change());
    }
    if !job_condition_is_true(&map_job, "Complete") {
        patch_import_status(
            import,
            context,
            NgkgSourceImportStatus {
                semantic_map_job_name: Some(map_name),
                condition: Some("SemanticMapRunning".to_owned()),
                ..current
            },
        )
        .await?;
        return Ok(Action::requeue(Duration::from_secs(15)));
    }

    let dictionary_name = format!("{base}-sem-dict");
    ensure_semantic_dictionary_job(import, context, &dictionary_name, &current).await?;
    let Some(dictionary_job) = jobs.get_opt(&dictionary_name).await? else {
        return Ok(Action::requeue(Duration::from_secs(5)));
    };
    if job_condition_is_true(&dictionary_job, "Failed") {
        patch_import_status(
            import,
            context,
            NgkgSourceImportStatus {
                semantic_map_job_name: Some(map_name),
                semantic_dictionary_job_name: Some(dictionary_name),
                condition: Some("SemanticDictionaryFailed".to_owned()),
                ..current
            },
        )
        .await?;
        return Ok(Action::await_change());
    }
    if !job_condition_is_true(&dictionary_job, "Complete") {
        patch_import_status(
            import,
            context,
            NgkgSourceImportStatus {
                semantic_map_job_name: Some(map_name),
                semantic_dictionary_job_name: Some(dictionary_name),
                condition: Some("SemanticDictionaryRunning".to_owned()),
                ..current
            },
        )
        .await?;
        return Ok(Action::requeue(Duration::from_secs(15)));
    }
    if current.semantic_dictionary_sha256.is_none() {
        return Ok(Action::requeue(Duration::from_secs(5)));
    }

    let partition_name = format!("{base}-sem-part");
    ensure_semantic_partition_job(
        import,
        context,
        &partition_name,
        import.spec.logical_partitions,
        &current,
    )
    .await?;
    let Some(partition_job) = jobs.get_opt(&partition_name).await? else {
        return Ok(Action::requeue(Duration::from_secs(5)));
    };
    if job_condition_is_true(&partition_job, "Failed") {
        patch_import_status(
            import,
            context,
            NgkgSourceImportStatus {
                semantic_map_job_name: Some(map_name),
                semantic_dictionary_job_name: Some(dictionary_name),
                semantic_partition_job_name: Some(partition_name),
                condition: Some("SemanticPartitionFailed".to_owned()),
                ..current
            },
        )
        .await?;
        return Ok(Action::await_change());
    }
    if !job_condition_is_true(&partition_job, "Complete") {
        patch_import_status(
            import,
            context,
            NgkgSourceImportStatus {
                semantic_map_job_name: Some(map_name),
                semantic_dictionary_job_name: Some(dictionary_name),
                semantic_partition_job_name: Some(partition_name),
                condition: Some("SemanticPartitionRunning".to_owned()),
                ..current
            },
        )
        .await?;
        return Ok(Action::requeue(Duration::from_secs(15)));
    }

    let finalize_name = format!("{base}-sem-final");
    ensure_semantic_finalize_job(import, context, &finalize_name, &current).await?;
    let Some(finalize_job) = jobs.get_opt(&finalize_name).await? else {
        return Ok(Action::requeue(Duration::from_secs(5)));
    };
    if job_condition_is_true(&finalize_job, "Failed") {
        patch_import_status(
            import,
            context,
            NgkgSourceImportStatus {
                semantic_map_job_name: Some(map_name),
                semantic_dictionary_job_name: Some(dictionary_name),
                semantic_partition_job_name: Some(partition_name),
                semantic_finalize_job_name: Some(finalize_name),
                condition: Some("SemanticFinalizationFailed".to_owned()),
                ..current
            },
        )
        .await?;
        return Ok(Action::await_change());
    }
    if job_condition_is_true(&finalize_job, "Complete") {
        let imports: Api<NgkgSourceImport> =
            Api::namespaced(context.client.clone(), &context.namespace);
        let refreshed = imports.get(&import.name_any()).await?;
        if refreshed
            .status
            .as_ref()
            .and_then(|status| status.semantic_compilation_root_sha256.as_deref())
            .is_some()
        {
            return Ok(Action::await_change());
        }
    }
    patch_import_status(
        import,
        context,
        NgkgSourceImportStatus {
            semantic_map_job_name: Some(map_name),
            semantic_dictionary_job_name: Some(dictionary_name),
            semantic_partition_job_name: Some(partition_name),
            semantic_finalize_job_name: Some(finalize_name),
            condition: Some("SemanticFinalizationRunning".to_owned()),
            ..current
        },
    )
    .await?;
    Ok(Action::requeue(Duration::from_secs(15)))
}

async fn reconcile_import_decode(
    import: &NgkgSourceImport,
    context: &Context,
    base: &str,
    current: NgkgSourceImportStatus,
) -> Result<Action, OperatorError> {
    let count = current
        .decode_work_item_count
        .filter(|count| *count > 0)
        .ok_or(OperatorError::SpecConflict("decodeWorkItemCount"))?;
    let decode_name = format!("{base}-decode");
    ensure_decode_job(import, context, base, &decode_name, count, &current).await?;
    let jobs: Api<Job> = Api::namespaced(context.client.clone(), &context.namespace);
    let Some(decode_job) = jobs.get_opt(&decode_name).await? else {
        return Ok(Action::requeue(Duration::from_secs(5)));
    };
    if job_condition_is_true(&decode_job, "Failed") {
        patch_import_status(
            import,
            context,
            NgkgSourceImportStatus {
                observed_generation: import.metadata.generation,
                decode_job_name: Some(decode_name),
                condition: Some("DistributedDecodeFailed".to_owned()),
                ..current
            },
        )
        .await?;
        return Ok(Action::await_change());
    }
    if !job_condition_is_true(&decode_job, "Complete") {
        patch_import_status(
            import,
            context,
            NgkgSourceImportStatus {
                observed_generation: import.metadata.generation,
                decode_job_name: Some(decode_name),
                condition: Some("DistributedDecodeRunning".to_owned()),
                ..current
            },
        )
        .await?;
        return Ok(Action::requeue(Duration::from_secs(15)));
    }
    let finalize_name = format!("{base}-finalize");
    ensure_decode_finalize_job(import, context, &finalize_name, &current).await?;
    let Some(finalize_job) = jobs.get_opt(&finalize_name).await? else {
        return Ok(Action::requeue(Duration::from_secs(5)));
    };
    if job_condition_is_true(&finalize_job, "Failed") {
        patch_import_status(
            import,
            context,
            NgkgSourceImportStatus {
                observed_generation: import.metadata.generation,
                decode_job_name: Some(decode_name),
                finalize_job_name: Some(finalize_name),
                condition: Some("CompilerHandoffFailed".to_owned()),
                ..current
            },
        )
        .await?;
        return Ok(Action::await_change());
    }
    if job_condition_is_true(&finalize_job, "Complete") {
        let imports: Api<NgkgSourceImport> =
            Api::namespaced(context.client.clone(), &context.namespace);
        let refreshed = imports.get(&import.name_any()).await?;
        if refreshed
            .status
            .as_ref()
            .and_then(|status| status.compiler_handoff_sha256.as_deref())
            .is_some()
        {
            return Ok(Action::await_change());
        }
        patch_import_status(
            import,
            context,
            NgkgSourceImportStatus {
                observed_generation: import.metadata.generation,
                decode_job_name: Some(decode_name),
                finalize_job_name: Some(finalize_name),
                condition: Some("CompilerHandoffMissing".to_owned()),
                ..current
            },
        )
        .await?;
        return Ok(Action::await_change());
    }
    patch_import_status(
        import,
        context,
        NgkgSourceImportStatus {
            observed_generation: import.metadata.generation,
            decode_job_name: Some(decode_name),
            finalize_job_name: Some(finalize_name),
            condition: Some("CompilerHandoffVerifying".to_owned()),
            ..current
        },
    )
    .await?;
    Ok(Action::requeue(Duration::from_secs(15)))
}

fn job_condition_is_true(job: &Job, condition_type: &str) -> bool {
    job.status
        .as_ref()
        .and_then(|status| status.conditions.as_ref())
        .is_some_and(|conditions| {
            conditions
                .iter()
                .any(|condition| condition.type_ == condition_type && condition.status == "True")
        })
}

async fn ensure_import_worker_rbac(
    import: &NgkgSourceImport,
    context: &Context,
    name: &str,
) -> Result<(), OperatorError> {
    let roles: Api<Role> = Api::namespaced(context.client.clone(), &context.namespace);
    if roles.get_opt(name).await?.is_none() {
        let role = Role {
            metadata: ObjectMeta {
                name: Some(name.to_owned()),
                namespace: Some(context.namespace.clone()),
                owner_references: import.controller_owner_ref(&()).map(|owner| vec![owner]),
                ..ObjectMeta::default()
            },
            rules: Some(vec![PolicyRule {
                api_groups: Some(vec!["ngkg.io".to_owned()]),
                resources: Some(vec![
                    "ngkgsourceimports".to_owned(),
                    "ngkgsourceimports/status".to_owned(),
                ]),
                resource_names: Some(vec![import.name_any()]),
                verbs: vec!["get".to_owned(), "patch".to_owned()],
                ..PolicyRule::default()
            }]),
        };
        match roles.create(&PostParams::default(), &role).await {
            Ok(_) => {}
            Err(kube::Error::Api(response)) if response.code == 409 => {}
            Err(error) => return Err(error.into()),
        }
    }
    let bindings: Api<RoleBinding> = Api::namespaced(context.client.clone(), &context.namespace);
    if bindings.get_opt(name).await?.is_none() {
        let binding = RoleBinding {
            metadata: ObjectMeta {
                name: Some(name.to_owned()),
                namespace: Some(context.namespace.clone()),
                owner_references: import.controller_owner_ref(&()).map(|owner| vec![owner]),
                ..ObjectMeta::default()
            },
            role_ref: RoleRef {
                api_group: Some("rbac.authorization.k8s.io".to_owned()),
                kind: "Role".to_owned(),
                name: name.to_owned(),
            },
            subjects: Some(vec![Subject {
                kind: "ServiceAccount".to_owned(),
                name: import.spec.identity_ref.clone(),
                namespace: Some(context.namespace.clone()),
                ..Subject::default()
            }]),
        };
        match bindings.create(&PostParams::default(), &binding).await {
            Ok(_) => {}
            Err(kube::Error::Api(response)) if response.code == 409 => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

fn import_error_policy(
    _object: Arc<NgkgSourceImport>,
    error: &OperatorError,
    _context: Arc<Context>,
) -> Action {
    tracing::error!(%error, "source-import reconciliation failed closed");
    Action::requeue(Duration::from_secs(30))
}

async fn ensure_import_volume(
    import: &NgkgSourceImport,
    context: &Context,
    name: &str,
) -> Result<(), OperatorError> {
    // Static bucket volumes are declarative mount handles, not one-PiB allocations.  Use the
    // admitted import ceiling so schedulers and quota engines see a bounded, honest request.
    let capacity_bytes = import.spec.max_source_bytes.max(1_048_576);
    let capacity = Quantity(capacity_bytes.to_string());
    let persistent_volumes: Api<PersistentVolume> = Api::all(context.client.clone());
    if persistent_volumes.get_opt(name).await?.is_none() {
        let service_accounts: Api<ServiceAccount> =
            Api::namespaced(context.client.clone(), &context.namespace);
        let source_identity = service_accounts.get(&import.spec.identity_ref).await?;
        let azure_client_id = source_identity
            .metadata
            .annotations
            .as_ref()
            .and_then(|annotations| annotations.get("azure.workload.identity/client-id"));
        if import.spec.provider == CloudObjectProvider::AzureBlob && azure_client_id.is_none() {
            return Err(OperatorError::SpecConflict(
                "Azure source identity lacks azure.workload.identity/client-id",
            ));
        }
        let (driver, volume_handle, attributes) =
            import_csi_contract(import, azure_client_id.map(String::as_str), &context.worker);
        if context.worker.require_source_csi_driver_discovery {
            let drivers: Api<CSIDriver> = Api::all(context.client.clone());
            if drivers.get_opt(&driver).await?.is_none() {
                return Err(OperatorError::SpecConflict(
                    "required cloud source CSI driver is not registered",
                ));
            }
        }
        let volume = PersistentVolume {
            metadata: ObjectMeta {
                name: Some(name.to_owned()),
                labels: Some(BTreeMap::from([
                    ("app.kubernetes.io/name".to_owned(), "ngkg".to_owned()),
                    (
                        "app.kubernetes.io/component".to_owned(),
                        "source-import".to_owned(),
                    ),
                    (
                        "ngkg.io/operation-id".to_owned(),
                        import.spec.operation_id.to_string(),
                    ),
                ])),
                annotations: Some(BTreeMap::from([(
                    "ngkg.io/source-spec-sha256".to_owned(),
                    import_spec_hash(import),
                ), (
                    "ngkg.io/lifecycle-owner".to_owned(),
                    format!("{}/{}", context.namespace, import.name_any()),
                )])),
                ..ObjectMeta::default()
            },
            spec: Some(PersistentVolumeSpec {
                access_modes: Some(vec!["ReadOnlyMany".to_owned()]),
                capacity: Some(BTreeMap::from([(
                    "storage".to_owned(),
                    capacity.clone(),
                )])),
                claim_ref: Some(ObjectReference {
                    name: Some(name.to_owned()),
                    namespace: Some(context.namespace.clone()),
                    ..ObjectReference::default()
                }),
                csi: Some(CSIPersistentVolumeSource {
                    driver,
                    read_only: Some(true),
                    volume_attributes: Some(attributes),
                    volume_handle,
                    ..CSIPersistentVolumeSource::default()
                }),
                // All three supported CSI drivers treat deletion of the static mount handle as
                // unmount/reclamation; source buckets remain protected by read-only identity.
                persistent_volume_reclaim_policy: Some("Delete".to_owned()),
                storage_class_name: Some(String::new()),
                volume_mode: Some("Filesystem".to_owned()),
                ..PersistentVolumeSpec::default()
            }),
            status: None,
        };
        match persistent_volumes
            .create(&PostParams::default(), &volume)
            .await
        {
            Ok(_) => {}
            Err(kube::Error::Api(kube_error)) if kube_error.code == 409 => {}
            Err(error) => return Err(error.into()),
        }
    }
    let existing = persistent_volumes
        .get(name)
        .await?;
    validate_import_volume(&existing, import, context, name).await?;
    let claims: Api<PersistentVolumeClaim> =
        Api::namespaced(context.client.clone(), &context.namespace);
    if claims.get_opt(name).await?.is_none() {
        let claim = PersistentVolumeClaim {
            metadata: ObjectMeta {
                name: Some(name.to_owned()),
                namespace: Some(context.namespace.clone()),
                labels: Some(BTreeMap::from([(
                    "ngkg.io/operation-id".to_owned(),
                    import.spec.operation_id.to_string(),
                )])),
                owner_references: import.controller_owner_ref(&()).map(|owner| vec![owner]),
                ..ObjectMeta::default()
            },
            spec: Some(PersistentVolumeClaimSpec {
                access_modes: Some(vec!["ReadOnlyMany".to_owned()]),
                resources: Some(VolumeResourceRequirements {
                    requests: Some(BTreeMap::from([(
                        "storage".to_owned(),
                        capacity,
                    )])),
                    ..VolumeResourceRequirements::default()
                }),
                storage_class_name: Some(String::new()),
                volume_mode: Some("Filesystem".to_owned()),
                volume_name: Some(name.to_owned()),
                ..PersistentVolumeClaimSpec::default()
            }),
            status: None,
        };
        match claims.create(&PostParams::default(), &claim).await {
            Ok(_) => {}
            Err(kube::Error::Api(kube_error)) if kube_error.code == 409 => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

async fn validate_import_volume(
    volume: &PersistentVolume,
    import: &NgkgSourceImport,
    context: &Context,
    name: &str,
) -> Result<(), OperatorError> {
    let expected_owner = format!("{}/{}", context.namespace, import.name_any());
    let annotations = volume.metadata.annotations.as_ref();
    let labels = volume.metadata.labels.as_ref();
    if volume.metadata.name.as_deref() != Some(name)
        || annotations.and_then(|items| items.get("ngkg.io/source-spec-sha256")) != Some(&import_spec_hash(import))
        || annotations.and_then(|items| items.get("ngkg.io/lifecycle-owner")) != Some(&expected_owner)
        || labels.and_then(|items| items.get("ngkg.io/operation-id")) != Some(&import.spec.operation_id.to_string())
    {
        return Err(OperatorError::SpecConflict("existing source import PV ownership"));
    }
    let service_accounts: Api<ServiceAccount> =
        Api::namespaced(context.client.clone(), &context.namespace);
    let identity = service_accounts.get(&import.spec.identity_ref).await?;
    let azure_client_id = identity.metadata.annotations.as_ref()
        .and_then(|items| items.get("azure.workload.identity/client-id"))
        .map(String::as_str);
    if import.spec.provider == CloudObjectProvider::AzureBlob && azure_client_id.is_none() {
        return Err(OperatorError::SpecConflict(
            "Azure source identity lacks azure.workload.identity/client-id",
        ));
    }
    let (driver, handle, attributes) = import_csi_contract(import, azure_client_id, &context.worker);
    let spec = volume.spec.as_ref().ok_or(OperatorError::SpecConflict("existing source import PV spec"))?;
    let csi = spec.csi.as_ref().ok_or(OperatorError::SpecConflict("existing source import PV CSI"))?;
    if csi.driver != driver
        || csi.volume_handle != handle
        || csi.read_only != Some(true)
        || csi.volume_attributes.as_ref() != Some(&attributes)
        || spec.persistent_volume_reclaim_policy.as_deref() != Some("Delete")
        || spec.claim_ref.as_ref().and_then(|claim| claim.name.as_deref()) != Some(name)
        || spec.claim_ref.as_ref().and_then(|claim| claim.namespace.as_deref()) != Some(context.namespace.as_str())
    {
        return Err(OperatorError::SpecConflict("existing source import PV storage contract"));
    }
    Ok(())
}

fn import_csi_contract(
    import: &NgkgSourceImport,
    azure_client_id: Option<&str>,
    config: &WorkerConfig,
) -> (String, String, BTreeMap<String, String>) {
    match import.spec.provider {
        CloudObjectProvider::AwsS3 => (
            config.aws_s3_csi_driver.clone(),
            import.spec.operation_id.to_string(),
            BTreeMap::from([
                ("authenticationSource".to_owned(), "pod".to_owned()),
                ("bucketName".to_owned(), import.spec.bucket.clone()),
            ]),
        ),
        CloudObjectProvider::AzureBlob => (
            config.azure_blob_csi_driver.clone(),
            import.spec.operation_id.to_string(),
            BTreeMap::from([
                ("containerName".to_owned(), import.spec.bucket.clone()),
                (
                    "AzureStorageAuthType".to_owned(),
                    "workloadidentity".to_owned(),
                ),
                (
                    "clientID".to_owned(),
                    azure_client_id.unwrap_or_default().to_owned(),
                ),
                (
                    "storageAccount".to_owned(),
                    import.spec.account_name.clone().unwrap_or_default(),
                ),
                ("protocol".to_owned(), "fuse2".to_owned()),
            ]),
        ),
        CloudObjectProvider::Gcs => (
            config.gcs_csi_driver.clone(),
            import.spec.bucket.clone(),
            BTreeMap::new(),
        ),
    }
}

fn source_pod_annotations(
    import: &NgkgSourceImport,
    spec_hash: &str,
    source_mounted: bool,
) -> BTreeMap<String, String> {
    let mut annotations = BTreeMap::from([(
        "ngkg.io/source-spec-sha256".to_owned(),
        spec_hash.to_owned(),
    )]);
    if source_mounted && import.spec.provider == CloudObjectProvider::Gcs {
        annotations.insert("gke-gcsfuse/volumes".to_owned(), "true".to_owned());
    }
    annotations
}

async fn ensure_import_job(
    import: &NgkgSourceImport,
    context: &Context,
    name: &str,
) -> Result<(), OperatorError> {
    let jobs: Api<Job> = Api::namespaced(context.client.clone(), &context.namespace);
    if let Some(existing) = jobs.get_opt(name).await? {
        let observed = existing
            .metadata
            .annotations
            .as_ref()
            .and_then(|values| values.get("ngkg.io/source-spec-sha256"));
        if observed != Some(&import_spec_hash(import)) {
            return Err(OperatorError::SpecConflict("existing source import Job"));
        }
        return Ok(());
    }
    let mut labels = BTreeMap::from([
        ("app.kubernetes.io/name".to_owned(), "ngkg".to_owned()),
        (
            "app.kubernetes.io/component".to_owned(),
            "source-import".to_owned(),
        ),
        (
            "ngkg.io/responsibility".to_owned(),
            "source-ingestion".to_owned(),
        ),
        (
            "ngkg.io/operation-id".to_owned(),
            import.spec.operation_id.to_string(),
        ),
        (
            "kueue.x-k8s.io/queue-name".to_owned(),
            context.worker.queue_name.clone(),
        ),
    ]);
    if import.spec.provider == CloudObjectProvider::AzureBlob {
        labels.insert("azure.workload.identity/use".to_owned(), "true".to_owned());
    }
    let source_spec_sha256 = import_spec_hash(import);
    let scratch_size = Quantity(context.worker.scratch_size.clone());
    let resources = ResourceRequirements {
        limits: Some(BTreeMap::from([
            ("cpu".to_owned(), Quantity(context.worker.cpu.clone())),
            ("memory".to_owned(), Quantity(context.worker.memory.clone())),
            ("ephemeral-storage".to_owned(), scratch_size.clone()),
        ])),
        requests: Some(BTreeMap::from([
            ("cpu".to_owned(), Quantity(context.worker.cpu.clone())),
            ("memory".to_owned(), Quantity(context.worker.memory.clone())),
            ("ephemeral-storage".to_owned(), scratch_size.clone()),
        ])),
        ..ResourceRequirements::default()
    };
    let cpu_threads = context.worker.cpu.parse::<u32>().unwrap_or(1).max(1);
    let arguments = vec![
        "cloud-import".to_owned(),
        "--namespace".to_owned(),
        context.namespace.clone(),
        "--import-name".to_owned(),
        import.name_any(),
        "--source-root".to_owned(),
        "/source".to_owned(),
        "--artifact-base-url".to_owned(),
        context.worker.artifact_base_url.clone(),
        "--scratch-root".to_owned(),
        "/scratch".to_owned(),
        "--single-put-max-bytes".to_owned(),
        option_value(&context.worker.options, "single-put-max-bytes"),
        "--multipart-buffer-bytes".to_owned(),
        option_value(&context.worker.options, "multipart-buffer-bytes"),
        "--multipart-concurrency".to_owned(),
        option_value(&context.worker.options, "multipart-concurrency"),
        "--scan-concurrency".to_owned(),
        cpu_threads.to_string(),
        "--decode-target-work-bytes".to_owned(),
        option_value(&context.worker.options, "decode-target-work-bytes"),
        "--decode-max-work-items".to_owned(),
        option_value(&context.worker.options, "decode-max-work-items"),
        "--decode-max-plan-bytes".to_owned(),
        option_value(&context.worker.options, "decode-max-plan-bytes"),
    ];
    let mut env_from = Vec::new();
    if let Some(secret) = &context.worker.object_store_credentials_secret {
        env_from.push(EnvFromSource {
            secret_ref: Some(SecretEnvSource {
                name: secret.clone(),
                optional: Some(false),
            }),
            ..EnvFromSource::default()
        });
    }
    let pod_annotations = source_pod_annotations(import, &source_spec_sha256, true);
    let job = Job {
        metadata: ObjectMeta {
            name: Some(name.to_owned()),
            namespace: Some(context.namespace.clone()),
            labels: Some(labels.clone()),
            annotations: Some(BTreeMap::from([(
                "ngkg.io/source-spec-sha256".to_owned(),
                source_spec_sha256.clone(),
            )])),
            owner_references: import.controller_owner_ref(&()).map(|owner| vec![owner]),
            ..ObjectMeta::default()
        },
        spec: Some(JobSpec {
            active_deadline_seconds: Some(context.worker.active_deadline_seconds),
            backoff_limit: Some(3),
            completions: Some(1),
            parallelism: Some(1),
            ttl_seconds_after_finished: Some(context.worker.ttl_seconds_after_finished),
            template: PodTemplateSpec {
                metadata: Some(ObjectMeta {
                    labels: Some(labels),
                    annotations: Some(pod_annotations),
                    ..ObjectMeta::default()
                }),
                spec: Some(PodSpec {
                    automount_service_account_token: Some(true),
                    containers: vec![Container {
                        name: "loader".to_owned(),
                        image: Some(context.worker.image.clone()),
                        args: Some(arguments),
                        env_from: Some(env_from),
                        resources: Some(resources),
                        security_context: Some(SecurityContext {
                            allow_privilege_escalation: Some(false),
                            capabilities: Some(Capabilities {
                                add: None,
                                drop: Some(vec!["ALL".to_owned()]),
                            }),
                            read_only_root_filesystem: Some(true),
                            run_as_non_root: Some(true),
                            ..SecurityContext::default()
                        }),
                        volume_mounts: Some(vec![
                            VolumeMount {
                                mount_path: "/source".to_owned(),
                                name: "source".to_owned(),
                                read_only: Some(true),
                                ..VolumeMount::default()
                            },
                            VolumeMount {
                                mount_path: "/scratch".to_owned(),
                                name: "scratch".to_owned(),
                                ..VolumeMount::default()
                            },
                        ]),
                        ..Container::default()
                    }],
                    node_selector: Some(BTreeMap::from([(
                        "ngkg.io/workload".to_owned(),
                        "source-ingestion".to_owned(),
                    )])),
                    restart_policy: Some("Never".to_owned()),
                    security_context: Some(PodSecurityContext {
                        run_as_non_root: Some(true),
                        seccomp_profile: Some(SeccompProfile {
                            localhost_profile: None,
                            type_: "RuntimeDefault".to_owned(),
                        }),
                        ..PodSecurityContext::default()
                    }),
                    service_account_name: Some(import.spec.identity_ref.clone()),
                    tolerations: Some(vec![Toleration {
                        effect: Some("NoSchedule".to_owned()),
                        key: Some("ngkg.io/workload".to_owned()),
                        operator: Some("Equal".to_owned()),
                        value: Some("source-ingestion".to_owned()),
                        ..Toleration::default()
                    }]),
                    volumes: Some(vec![
                        Volume {
                            name: "source".to_owned(),
                            persistent_volume_claim: Some(PersistentVolumeClaimVolumeSource {
                                claim_name: name.to_owned(),
                                read_only: Some(true),
                            }),
                            ..Volume::default()
                        },
                        Volume {
                            name: "scratch".to_owned(),
                            empty_dir: Some(EmptyDirVolumeSource {
                                size_limit: Some(scratch_size),
                                ..EmptyDirVolumeSource::default()
                            }),
                            ..Volume::default()
                        },
                    ]),
                    ..PodSpec::default()
                }),
            },
            ..JobSpec::default()
        }),
        ..Job::default()
    };
    match jobs.create(&PostParams::default(), &job).await {
        Ok(_) => Ok(()),
        Err(kube::Error::Api(kube_error)) if kube_error.code == 409 => Ok(()),
        Err(error) => Err(error.into()),
    }
}

async fn ensure_decode_job(
    import: &NgkgSourceImport,
    context: &Context,
    volume_name: &str,
    name: &str,
    work_item_count: u32,
    status: &NgkgSourceImportStatus,
) -> Result<(), OperatorError> {
    let plan_key = status
        .decode_plan_object_key
        .as_deref()
        .ok_or(OperatorError::SpecConflict("decodePlanObjectKey"))?;
    let plan_sha256 = status
        .decode_plan_sha256
        .as_deref()
        .ok_or(OperatorError::SpecConflict("decodePlanSha256"))?;
    let arguments = vec![
        "cloud-decode".to_owned(),
        "--source-root".to_owned(),
        "/source".to_owned(),
        "--artifact-base-url".to_owned(),
        context.worker.artifact_base_url.clone(),
        "--scratch-root".to_owned(),
        "/scratch".to_owned(),
        "--decode-plan-object-key".to_owned(),
        plan_key.to_owned(),
        "--decode-plan-sha256".to_owned(),
        plan_sha256.to_owned(),
        "--decode-max-plan-bytes".to_owned(),
        option_value(&context.worker.options, "decode-max-plan-bytes"),
        "--decode-max-fragment-bytes".to_owned(),
        option_value(&context.worker.options, "decode-max-fragment-bytes"),
        "--decode-object-concurrency".to_owned(),
        option_value(&context.worker.options, "decode-object-concurrency"),
        "--completion-index".to_owned(),
        "$(JOB_COMPLETION_INDEX)".to_owned(),
        "--single-put-max-bytes".to_owned(),
        option_value(&context.worker.options, "single-put-max-bytes"),
        "--multipart-buffer-bytes".to_owned(),
        option_value(&context.worker.options, "multipart-buffer-bytes"),
        "--multipart-concurrency".to_owned(),
        option_value(&context.worker.options, "multipart-concurrency"),
    ];
    let maximum = option_value(&context.worker.options, "decode-max-parallelism")
        .parse::<u32>()
        .map_err(|_| OperatorError::SpecConflict("decodeMaxParallelism"))?
        .max(1);
    ensure_cloud_stage_job(
        import,
        context,
        volume_name,
        name,
        "source-decode",
        arguments,
        work_item_count,
        work_item_count.min(maximum),
        true,
        true,
    )
    .await
}

async fn ensure_decode_finalize_job(
    import: &NgkgSourceImport,
    context: &Context,
    name: &str,
    status: &NgkgSourceImportStatus,
) -> Result<(), OperatorError> {
    let plan_key = status
        .decode_plan_object_key
        .as_deref()
        .ok_or(OperatorError::SpecConflict("decodePlanObjectKey"))?;
    let plan_sha256 = status
        .decode_plan_sha256
        .as_deref()
        .ok_or(OperatorError::SpecConflict("decodePlanSha256"))?;
    let arguments = vec![
        "cloud-decode-finalize".to_owned(),
        "--namespace".to_owned(),
        context.namespace.clone(),
        "--import-name".to_owned(),
        import.name_any(),
        "--artifact-base-url".to_owned(),
        context.worker.artifact_base_url.clone(),
        "--scratch-root".to_owned(),
        "/scratch".to_owned(),
        "--decode-plan-object-key".to_owned(),
        plan_key.to_owned(),
        "--decode-plan-sha256".to_owned(),
        plan_sha256.to_owned(),
        "--decode-max-plan-bytes".to_owned(),
        option_value(&context.worker.options, "decode-max-plan-bytes"),
        "--decode-max-completion-manifest-bytes".to_owned(),
        option_value(
            &context.worker.options,
            "decode-max-completion-manifest-bytes",
        ),
        "--decode-max-fragment-bytes".to_owned(),
        option_value(&context.worker.options, "decode-max-fragment-bytes"),
        "--decode-finalize-concurrency".to_owned(),
        option_value(&context.worker.options, "decode-finalize-concurrency"),
        "--single-put-max-bytes".to_owned(),
        option_value(&context.worker.options, "single-put-max-bytes"),
        "--multipart-buffer-bytes".to_owned(),
        option_value(&context.worker.options, "multipart-buffer-bytes"),
        "--multipart-concurrency".to_owned(),
        option_value(&context.worker.options, "multipart-concurrency"),
    ];
    ensure_cloud_stage_job(
        import,
        context,
        name,
        name,
        "source-decode-finalize",
        arguments,
        1,
        1,
        false,
        false,
    )
    .await
}

fn semantic_common_arguments(
    import: &NgkgSourceImport,
    context: &Context,
    status: &NgkgSourceImportStatus,
) -> Result<Vec<String>, OperatorError> {
    let handoff_key = status
        .compiler_handoff_object_key
        .as_deref()
        .ok_or(OperatorError::SpecConflict("compilerHandoffObjectKey"))?;
    let handoff_sha256 = status
        .compiler_handoff_sha256
        .as_deref()
        .ok_or(OperatorError::SpecConflict("compilerHandoffSha256"))?;
    Ok(vec![
        "--namespace".to_owned(),
        context.namespace.clone(),
        "--import-name".to_owned(),
        import.name_any(),
        "--artifact-base-url".to_owned(),
        context.worker.artifact_base_url.clone(),
        "--scratch-root".to_owned(),
        "/scratch".to_owned(),
        "--compiler-handoff-object-key".to_owned(),
        handoff_key.to_owned(),
        "--compiler-handoff-sha256".to_owned(),
        handoff_sha256.to_owned(),
        "--semantic-max-manifest-bytes".to_owned(),
        option_value(&context.worker.options, "semantic-max-manifest-bytes"),
        "--single-put-max-bytes".to_owned(),
        option_value(&context.worker.options, "single-put-max-bytes"),
        "--multipart-buffer-bytes".to_owned(),
        option_value(&context.worker.options, "multipart-buffer-bytes"),
        "--multipart-concurrency".to_owned(),
        option_value(&context.worker.options, "multipart-concurrency"),
    ])
}

fn ontology_common_arguments(
    import: &NgkgSourceImport,
    context: &Context,
    status: &NgkgSourceImportStatus,
) -> Result<Vec<String>, OperatorError> {
    let root_key = status
        .semantic_compilation_root_object_key
        .as_deref()
        .ok_or(OperatorError::SpecConflict(
            "semanticCompilationRootObjectKey",
        ))?;
    let root_sha256 = status
        .semantic_compilation_root_sha256
        .as_deref()
        .ok_or(OperatorError::SpecConflict("semanticCompilationRootSha256"))?;
    Ok(vec![
        "--namespace".to_owned(),
        context.namespace.clone(),
        "--import-name".to_owned(),
        import.name_any(),
        "--artifact-base-url".to_owned(),
        context.worker.artifact_base_url.clone(),
        "--scratch-root".to_owned(),
        "/scratch".to_owned(),
        "--semantic-compilation-root-object-key".to_owned(),
        root_key.to_owned(),
        "--semantic-compilation-root-sha256".to_owned(),
        root_sha256.to_owned(),
        "--ontology-qualification-request-object-key".to_owned(),
        import
            .spec
            .ontology_qualification_request_object_key
            .clone(),
        "--ontology-qualification-request-sha256".to_owned(),
        import.spec.ontology_qualification_request_sha256.clone(),
        "--ontology-max-manifest-bytes".to_owned(),
        option_value(&context.worker.options, "ontology-max-manifest-bytes"),
        "--ontology-max-artifact-bytes".to_owned(),
        option_value(&context.worker.options, "ontology-max-artifact-bytes"),
        "--single-put-max-bytes".to_owned(),
        option_value(&context.worker.options, "single-put-max-bytes"),
        "--multipart-buffer-bytes".to_owned(),
        option_value(&context.worker.options, "multipart-buffer-bytes"),
        "--multipart-concurrency".to_owned(),
        option_value(&context.worker.options, "multipart-concurrency"),
    ])
}

async fn ensure_ontology_projection_job(
    import: &NgkgSourceImport,
    context: &Context,
    name: &str,
    status: &NgkgSourceImportStatus,
) -> Result<(), OperatorError> {
    let mut arguments = vec!["cloud-ontology-project".to_owned()];
    arguments.extend(ontology_common_arguments(import, context, status)?);
    arguments.extend([
        "--completion-index".to_owned(),
        "$(JOB_COMPLETION_INDEX)".to_owned(),
        "--ontology-max-partition-quads".to_owned(),
        option_value(&context.worker.options, "ontology-max-partition-quads"),
        "--ontology-projection-rows-in-memory".to_owned(),
        option_value(
            &context.worker.options,
            "ontology-projection-rows-in-memory",
        ),
    ]);
    let maximum = option_value(&context.worker.options, "ontology-project-max-parallelism")
        .parse::<u32>()
        .map_err(|_| OperatorError::SpecConflict("ontologyProjectMaxParallelism"))?
        .max(1);
    let completions = import.spec.logical_partitions;
    ensure_cloud_stage_job(
        import,
        context,
        name,
        name,
        "ontology-project",
        arguments,
        completions,
        completions.min(maximum),
        true,
        false,
    )
    .await
}

async fn ensure_ontology_assembly_job(
    import: &NgkgSourceImport,
    context: &Context,
    name: &str,
    status: &NgkgSourceImportStatus,
) -> Result<(), OperatorError> {
    let mut arguments = vec!["cloud-ontology-assemble".to_owned()];
    arguments.extend(ontology_common_arguments(import, context, status)?);
    arguments.extend([
        "--ontology-download-concurrency".to_owned(),
        option_value(&context.worker.options, "ontology-download-concurrency"),
    ]);
    ensure_cloud_stage_job(
        import,
        context,
        name,
        name,
        "ontology-assemble",
        arguments,
        1,
        1,
        false,
        false,
    )
    .await
}

async fn ensure_ontology_qualification_job(
    import: &NgkgSourceImport,
    context: &Context,
    name: &str,
    status: &NgkgSourceImportStatus,
) -> Result<(), OperatorError> {
    let assembly_key = status
        .ontology_assembly_object_key
        .as_deref()
        .ok_or(OperatorError::SpecConflict("ontologyAssemblyObjectKey"))?;
    let assembly_sha256 = status
        .ontology_assembly_sha256
        .as_deref()
        .ok_or(OperatorError::SpecConflict("ontologyAssemblySha256"))?;
    let mut arguments = vec!["cloud-ontology-qualify".to_owned()];
    arguments.extend(ontology_common_arguments(import, context, status)?);
    arguments.extend([
        "--ontology-assembly-object-key".to_owned(),
        assembly_key.to_owned(),
        "--ontology-assembly-sha256".to_owned(),
        assembly_sha256.to_owned(),
        "--java-executable".to_owned(),
        option_value(&context.worker.options, "java-executable"),
        "--reasoner-adapter-jar".to_owned(),
        option_value(&context.worker.options, "reasoner-adapter-jar"),
        "--reasoner-adapter-sha256".to_owned(),
        option_value(&context.worker.options, "reasoner-adapter-sha256"),
        "--reasoner-name".to_owned(),
        option_value(&context.worker.options, "reasoner-name"),
        "--reasoner-version".to_owned(),
        option_value(&context.worker.options, "reasoner-version"),
        "--ontology-reasoner-heap-mib".to_owned(),
        option_value(&context.worker.options, "ontology-reasoner-heap-mib"),
        "--ontology-reasoner-timeout-seconds".to_owned(),
        option_value(&context.worker.options, "ontology-reasoner-timeout-seconds"),
        "--ontology-max-named-individuals".to_owned(),
        option_value(&context.worker.options, "ontology-max-named-individuals"),
        "--ontology-max-properties".to_owned(),
        option_value(&context.worker.options, "ontology-max-properties"),
    ]);
    ensure_cloud_stage_job(
        import,
        context,
        name,
        name,
        "ontology-qualify",
        arguments,
        1,
        1,
        false,
        false,
    )
    .await
}

fn offline_common_arguments(
    import: &NgkgSourceImport,
    context: &Context,
    status: &NgkgSourceImportStatus,
) -> Result<Vec<String>, OperatorError> {
    let root_key = status
        .ontology_qualification_root_object_key
        .as_deref()
        .ok_or(OperatorError::SpecConflict(
            "ontologyQualificationRootObjectKey",
        ))?;
    let root_sha256 =
        status
            .ontology_qualification_root_sha256
            .as_deref()
            .ok_or(OperatorError::SpecConflict(
                "ontologyQualificationRootSha256",
            ))?;
    let qualification_prefix = root_key
        .strip_suffix("/root/ontology-qualification-root.json")
        .ok_or(OperatorError::SpecConflict(
            "ontologyQualificationRootObjectKey",
        ))?;
    Ok(vec![
        "--namespace".to_owned(),
        context.namespace.clone(),
        "--import-name".to_owned(),
        import.name_any(),
        "--artifact-base-url".to_owned(),
        context.worker.artifact_base_url.clone(),
        "--scratch-root".to_owned(),
        "/scratch".to_owned(),
        "--ontology-qualification-root-object-key".to_owned(),
        root_key.to_owned(),
        "--ontology-qualification-root-sha256".to_owned(),
        root_sha256.to_owned(),
        "--offline-finite-closure-object-key".to_owned(),
        format!("{qualification_prefix}/reasoner/finite-closure.nt"),
        "--offline-max-manifest-bytes".to_owned(),
        option_value(&context.worker.options, "offline-max-manifest-bytes"),
        "--offline-max-artifact-bytes".to_owned(),
        option_value(&context.worker.options, "offline-max-artifact-bytes"),
        "--offline-max-finalizer-local-bytes".to_owned(),
        option_value(&context.worker.options, "offline-max-finalizer-local-bytes"),
        "--single-put-max-bytes".to_owned(),
        option_value(&context.worker.options, "single-put-max-bytes"),
        "--multipart-buffer-bytes".to_owned(),
        option_value(&context.worker.options, "multipart-buffer-bytes"),
        "--multipart-concurrency".to_owned(),
        option_value(&context.worker.options, "multipart-concurrency"),
    ])
}

async fn ensure_offline_plan_job(
    import: &NgkgSourceImport,
    context: &Context,
    name: &str,
    status: &NgkgSourceImportStatus,
) -> Result<(), OperatorError> {
    let mut arguments = vec!["cloud-offline-plan".to_owned()];
    arguments.extend(offline_common_arguments(import, context, status)?);
    arguments.extend([
        "--offline-logical-partitions".to_owned(),
        option_value(&context.worker.options, "offline-logical-partitions"),
        "--offline-max-consequences".to_owned(),
        option_value(&context.worker.options, "offline-max-consequences"),
        "--offline-plan-rows-in-memory".to_owned(),
        option_value(&context.worker.options, "offline-plan-rows-in-memory"),
        "--offline-max-run-bytes".to_owned(),
        option_value(&context.worker.options, "offline-max-run-bytes"),
    ]);
    ensure_cloud_stage_job(
        import,
        context,
        name,
        name,
        "offline-plan",
        arguments,
        1,
        1,
        false,
        false,
    )
    .await
}

async fn ensure_offline_partition_job(
    import: &NgkgSourceImport,
    context: &Context,
    name: &str,
    completions: u32,
    status: &NgkgSourceImportStatus,
) -> Result<(), OperatorError> {
    let plan_key = status
        .offline_reasoning_plan_object_key
        .as_deref()
        .ok_or(OperatorError::SpecConflict("offlineReasoningPlanObjectKey"))?;
    let plan_sha256 = status
        .offline_reasoning_plan_sha256
        .as_deref()
        .ok_or(OperatorError::SpecConflict("offlineReasoningPlanSha256"))?;
    let mut arguments = vec!["cloud-offline-partition".to_owned()];
    arguments.extend(offline_common_arguments(import, context, status)?);
    arguments.extend([
        "--offline-reasoning-plan-object-key".to_owned(),
        plan_key.to_owned(),
        "--offline-reasoning-plan-sha256".to_owned(),
        plan_sha256.to_owned(),
        "--completion-index".to_owned(),
        "$(JOB_COMPLETION_INDEX)".to_owned(),
        "--offline-parquet-row-group-rows".to_owned(),
        option_value(&context.worker.options, "offline-parquet-row-group-rows"),
    ]);
    let maximum = option_value(&context.worker.options, "offline-partition-max-parallelism")
        .parse::<u32>()
        .map_err(|_| OperatorError::SpecConflict("offlinePartitionMaxParallelism"))?
        .max(1);
    ensure_cloud_stage_job(
        import,
        context,
        name,
        name,
        "offline-partition",
        arguments,
        completions,
        completions.min(maximum),
        true,
        false,
    )
    .await
}

async fn ensure_offline_finalize_job(
    import: &NgkgSourceImport,
    context: &Context,
    name: &str,
    status: &NgkgSourceImportStatus,
) -> Result<(), OperatorError> {
    let plan_key = status
        .offline_reasoning_plan_object_key
        .as_deref()
        .ok_or(OperatorError::SpecConflict("offlineReasoningPlanObjectKey"))?;
    let plan_sha256 = status
        .offline_reasoning_plan_sha256
        .as_deref()
        .ok_or(OperatorError::SpecConflict("offlineReasoningPlanSha256"))?;
    let mut arguments = vec!["cloud-offline-finalize".to_owned()];
    arguments.extend(offline_common_arguments(import, context, status)?);
    arguments.extend([
        "--offline-reasoning-plan-object-key".to_owned(),
        plan_key.to_owned(),
        "--offline-reasoning-plan-sha256".to_owned(),
        plan_sha256.to_owned(),
        "--offline-finalize-concurrency".to_owned(),
        option_value(&context.worker.options, "offline-finalize-concurrency"),
    ]);
    ensure_cloud_stage_job(
        import,
        context,
        name,
        name,
        "offline-finalize",
        arguments,
        1,
        1,
        false,
        false,
    )
    .await
}

async fn ensure_snapshot_activation_job(
    import: &NgkgSourceImport,
    context: &Context,
    name: &str,
    status: &NgkgSourceImportStatus,
) -> Result<(), OperatorError> {
    let value = |field: Option<&str>, name| {
        field
            .map(str::to_owned)
            .ok_or(OperatorError::SpecConflict(name))
    };
    let mut arguments = vec![
        "cloud-snapshot-activate".to_owned(),
        "--namespace".to_owned(),
        context.namespace.clone(),
        "--import-name".to_owned(),
        import.name_any(),
        "--artifact-base-url".to_owned(),
        context.worker.artifact_base_url.clone(),
        "--scratch-root".to_owned(),
        "/scratch".to_owned(),
        "--semantic-compilation-root-object-key".to_owned(),
        value(
            status.semantic_compilation_root_object_key.as_deref(),
            "semanticCompilationRootObjectKey",
        )?,
        "--semantic-compilation-root-sha256".to_owned(),
        value(
            status.semantic_compilation_root_sha256.as_deref(),
            "semanticCompilationRootSha256",
        )?,
        "--ontology-qualification-request-object-key".to_owned(),
        import
            .spec
            .ontology_qualification_request_object_key
            .clone(),
        "--ontology-qualification-request-sha256".to_owned(),
        import.spec.ontology_qualification_request_sha256.clone(),
        "--ontology-qualification-root-object-key".to_owned(),
        value(
            status.ontology_qualification_root_object_key.as_deref(),
            "ontologyQualificationRootObjectKey",
        )?,
        "--ontology-qualification-root-sha256".to_owned(),
        value(
            status.ontology_qualification_root_sha256.as_deref(),
            "ontologyQualificationRootSha256",
        )?,
        "--offline-reasoning-root-object-key".to_owned(),
        value(
            status.offline_reasoning_root_object_key.as_deref(),
            "offlineReasoningRootObjectKey",
        )?,
        "--offline-reasoning-root-sha256".to_owned(),
        value(
            status.offline_reasoning_root_sha256.as_deref(),
            "offlineReasoningRootSha256",
        )?,
        "--activation-max-manifest-bytes".to_owned(),
        option_value(&context.worker.options, "activation-max-manifest-bytes"),
        "--activation-max-partition-bytes".to_owned(),
        option_value(&context.worker.options, "activation-max-partition-bytes"),
        "--activation-max-query-dataset-bytes".to_owned(),
        option_value(
            &context.worker.options,
            "activation-max-query-dataset-bytes",
        ),
        "--activation-verify-concurrency".to_owned(),
        option_value(&context.worker.options, "activation-verify-concurrency"),
        "--single-put-max-bytes".to_owned(),
        option_value(&context.worker.options, "single-put-max-bytes"),
        "--multipart-buffer-bytes".to_owned(),
        option_value(&context.worker.options, "multipart-buffer-bytes"),
        "--multipart-concurrency".to_owned(),
        option_value(&context.worker.options, "multipart-concurrency"),
    ];
    arguments.shrink_to_fit();
    ensure_cloud_stage_job(
        import,
        context,
        name,
        name,
        "snapshot-activation",
        arguments,
        1,
        1,
        false,
        false,
    )
    .await
}

async fn ensure_semantic_map_job(
    import: &NgkgSourceImport,
    context: &Context,
    name: &str,
    completions: u32,
    status: &NgkgSourceImportStatus,
) -> Result<(), OperatorError> {
    let mut arguments = vec!["cloud-semantic-map".to_owned()];
    arguments.extend(semantic_common_arguments(import, context, status)?);
    let cpu_threads = context.worker.cpu.parse::<u32>().unwrap_or(1).max(1);
    arguments.extend([
        "--completion-index".to_owned(),
        "$(JOB_COMPLETION_INDEX)".to_owned(),
        "--semantic-max-fragment-bytes".to_owned(),
        option_value(&context.worker.options, "semantic-max-fragment-bytes"),
        "--semantic-max-fragment-quads".to_owned(),
        option_value(&context.worker.options, "semantic-max-fragment-quads"),
        "--semantic-map-rows-in-memory".to_owned(),
        option_value(&context.worker.options, "semantic-map-rows-in-memory"),
        "--semantic-max-run-bytes".to_owned(),
        option_value(&context.worker.options, "semantic-max-run-bytes"),
        "--semantic-map-worker-threads".to_owned(),
        cpu_threads.saturating_sub(2).max(1).to_string(),
    ]);
    let maximum = option_value(&context.worker.options, "semantic-map-max-parallelism")
        .parse::<u32>()
        .map_err(|_| OperatorError::SpecConflict("semanticMapMaxParallelism"))?
        .max(1);
    ensure_cloud_stage_job(
        import,
        context,
        name,
        name,
        "semantic-map",
        arguments,
        completions,
        completions.min(maximum),
        true,
        false,
    )
    .await
}

async fn ensure_semantic_dictionary_job(
    import: &NgkgSourceImport,
    context: &Context,
    name: &str,
    status: &NgkgSourceImportStatus,
) -> Result<(), OperatorError> {
    let mut arguments = vec!["cloud-semantic-dictionary".to_owned()];
    arguments.extend(semantic_common_arguments(import, context, status)?);
    arguments.extend([
        "--semantic-max-run-bytes".to_owned(),
        option_value(&context.worker.options, "semantic-max-run-bytes"),
        "--semantic-max-dictionary-bytes".to_owned(),
        option_value(&context.worker.options, "semantic-max-dictionary-bytes"),
    ]);
    ensure_cloud_stage_job(
        import,
        context,
        name,
        name,
        "semantic-dictionary",
        arguments,
        1,
        1,
        false,
        false,
    )
    .await
}

async fn ensure_semantic_partition_job(
    import: &NgkgSourceImport,
    context: &Context,
    name: &str,
    completions: u32,
    status: &NgkgSourceImportStatus,
) -> Result<(), OperatorError> {
    let mut arguments = vec!["cloud-semantic-partition".to_owned()];
    arguments.extend(semantic_common_arguments(import, context, status)?);
    arguments.extend([
        "--completion-index".to_owned(),
        "$(JOB_COMPLETION_INDEX)".to_owned(),
        "--semantic-max-run-bytes".to_owned(),
        option_value(&context.worker.options, "semantic-max-run-bytes"),
        "--semantic-max-dictionary-bytes".to_owned(),
        option_value(&context.worker.options, "semantic-max-dictionary-bytes"),
        "--semantic-max-input-runs".to_owned(),
        option_value(&context.worker.options, "semantic-max-input-runs"),
        "--semantic-max-partition-quads".to_owned(),
        option_value(&context.worker.options, "semantic-max-partition-quads"),
        "--semantic-parquet-row-group-rows".to_owned(),
        option_value(&context.worker.options, "semantic-parquet-row-group-rows"),
    ]);
    let maximum = option_value(
        &context.worker.options,
        "semantic-partition-max-parallelism",
    )
    .parse::<u32>()
    .map_err(|_| OperatorError::SpecConflict("semanticPartitionMaxParallelism"))?
    .max(1);
    ensure_cloud_stage_job(
        import,
        context,
        name,
        name,
        "semantic-partition",
        arguments,
        completions,
        completions.min(maximum),
        true,
        false,
    )
    .await
}

async fn ensure_semantic_finalize_job(
    import: &NgkgSourceImport,
    context: &Context,
    name: &str,
    status: &NgkgSourceImportStatus,
) -> Result<(), OperatorError> {
    let mut arguments = vec!["cloud-semantic-finalize".to_owned()];
    arguments.extend(semantic_common_arguments(import, context, status)?);
    arguments.extend([
        "--semantic-max-dictionary-bytes".to_owned(),
        option_value(&context.worker.options, "semantic-max-dictionary-bytes"),
        "--semantic-max-artifact-bytes".to_owned(),
        option_value(&context.worker.options, "semantic-max-artifact-bytes"),
        "--semantic-finalize-concurrency".to_owned(),
        option_value(&context.worker.options, "semantic-finalize-concurrency"),
    ]);
    ensure_cloud_stage_job(
        import,
        context,
        name,
        name,
        "semantic-finalize",
        arguments,
        1,
        1,
        false,
        false,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn ensure_cloud_stage_job(
    import: &NgkgSourceImport,
    context: &Context,
    volume_name: &str,
    name: &str,
    component: &str,
    arguments: Vec<String>,
    completions: u32,
    parallelism: u32,
    indexed: bool,
    mount_source: bool,
) -> Result<(), OperatorError> {
    let jobs: Api<Job> = Api::namespaced(context.client.clone(), &context.namespace);
    let stage_cpu = if component.starts_with("offline-") {
        option_value(&context.worker.options, "offline-worker-cpu")
    } else {
        context.worker.cpu.clone()
    };
    let stage_memory = if component.starts_with("offline-") {
        option_value(&context.worker.options, "offline-worker-memory")
    } else {
        context.worker.memory.clone()
    };
    let stage_scratch = if component.starts_with("offline-") {
        option_value(&context.worker.options, "offline-scratch-size")
    } else if component.starts_with("semantic-")
        || component.starts_with("ontology-")
        || component == "snapshot-activation"
    {
        context.worker.semantic_scratch_size.clone()
    } else {
        context.worker.scratch_size.clone()
    };
    let stage_spec_digest: [u8; 32] = Sha256::digest(
        serde_json::to_vec(&json!({
            "importSpec": &import.spec,
            "component": component,
            "arguments": &arguments,
            "workerImage": &context.worker.image,
            "queue": &context.worker.queue_name,
            "cpu": &stage_cpu,
            "memory": &stage_memory,
            "scratch": &stage_scratch,
            "completions": completions,
            "parallelism": parallelism,
            "indexed": indexed,
            "mountSource": mount_source
        }))
        .unwrap_or_default(),
    )
    .into();
    let stage_spec_sha256 = hex::encode(stage_spec_digest);
    let durable_stage = context
        .catalog
        .reserve_orchestration_stage(
            import.spec.tenant_id,
            import.spec.operation_id,
            component,
            &stage_spec_digest,
            &context.namespace,
            name,
        )
        .await?;
    if durable_stage.state == "SUCCEEDED" {
        return Ok(());
    }
    if let Some(existing) = jobs.get_opt(name).await? {
        let observed = existing
            .metadata
            .annotations
            .as_ref()
            .and_then(|values| values.get("ngkg.io/source-spec-sha256"));
        let observed_stage = existing
            .metadata
            .annotations
            .as_ref()
            .and_then(|values| values.get("ngkg.io/stage-spec-sha256"));
        if observed != Some(&import_spec_hash(import))
            || observed_stage.is_some_and(|value| value != &stage_spec_sha256)
            || ((component.starts_with("semantic-")
                || component.starts_with("ontology-")
                || component.starts_with("offline-")
                || component == "snapshot-activation")
                && observed_stage.is_none())
        {
            return Err(OperatorError::SpecConflict("existing cloud compiler Job"));
        }
        let succeeded = existing.status.as_ref().is_some_and(|status| {
            status
                .conditions
                .as_ref()
                .is_some_and(|conditions| conditions.iter().any(|condition| {
                    condition.type_ == "Complete" && condition.status == "True"
                }))
        });
        let failed = existing.status.as_ref().is_some_and(|status| {
            status
                .conditions
                .as_ref()
                .is_some_and(|conditions| conditions.iter().any(|condition| {
                    condition.type_ == "Failed" && condition.status == "True"
                }))
        });
        if succeeded || failed {
            context
                .catalog
                .complete_orchestration_stage(
                    import.spec.tenant_id,
                    import.spec.operation_id,
                    component,
                    succeeded,
                    &json!({
                        "jobName": name,
                        "jobUid": existing.metadata.uid,
                        "stageSpecSha256": stage_spec_sha256,
                        "succeeded": succeeded
                    }),
                )
                .await?;
            if succeeded {
                return Ok(());
            }
            return Err(OperatorError::SpecConflict("cloud compiler Job failed"));
        }
        if let Some(uid) = existing.metadata.uid.as_deref() {
            context
                .catalog
                .mark_orchestration_stage_running(
                    import.spec.tenant_id,
                    import.spec.operation_id,
                    component,
                    uid,
                )
                .await?;
        }
        return Ok(());
    }
    let responsibility = if component.starts_with("ontology-") || component.starts_with("offline-")
    {
        "reasoning"
    } else if component.starts_with("semantic-") || component == "snapshot-activation" {
        "semantic-projection"
    } else {
        "source-ingestion"
    };
    let mut labels = BTreeMap::from([
        ("app.kubernetes.io/name".to_owned(), "ngkg".to_owned()),
        (
            "app.kubernetes.io/component".to_owned(),
            component.to_owned(),
        ),
        (
            "ngkg.io/responsibility".to_owned(),
            responsibility.to_owned(),
        ),
        (
            "ngkg.io/operation-id".to_owned(),
            import.spec.operation_id.to_string(),
        ),
        (
            "kueue.x-k8s.io/queue-name".to_owned(),
            context.worker.queue_name.clone(),
        ),
    ]);
    if import.spec.provider == CloudObjectProvider::AzureBlob {
        labels.insert("azure.workload.identity/use".to_owned(), "true".to_owned());
    }
    let scratch_size = Quantity(stage_scratch);
    let resources = ResourceRequirements {
        limits: Some(BTreeMap::from([
            ("cpu".to_owned(), Quantity(stage_cpu.clone())),
            ("memory".to_owned(), Quantity(stage_memory.clone())),
            ("ephemeral-storage".to_owned(), scratch_size.clone()),
        ])),
        requests: Some(BTreeMap::from([
            ("cpu".to_owned(), Quantity(stage_cpu.clone())),
            ("memory".to_owned(), Quantity(stage_memory)),
            ("ephemeral-storage".to_owned(), scratch_size.clone()),
        ])),
        ..ResourceRequirements::default()
    };
    let cpu_threads = stage_cpu.parse::<u32>().unwrap_or(1).max(1);
    let tokio_threads = if component == "semantic-map" {
        2_u32.min(cpu_threads)
    } else if component == "ontology-qualify" {
        2_u32.min(cpu_threads)
    } else {
        cpu_threads
    };
    let mut environment = vec![
        EnvVar {
            name: "TOKIO_WORKER_THREADS".to_owned(),
            value: Some(tokio_threads.to_string()),
            ..EnvVar::default()
        },
        EnvVar {
            name: "RAYON_NUM_THREADS".to_owned(),
            value: Some("1".to_owned()),
            ..EnvVar::default()
        },
        EnvVar {
            name: "OMP_NUM_THREADS".to_owned(),
            value: Some("1".to_owned()),
            ..EnvVar::default()
        },
        EnvVar {
            name: "OPENBLAS_NUM_THREADS".to_owned(),
            value: Some("1".to_owned()),
            ..EnvVar::default()
        },
        EnvVar {
            name: "MKL_NUM_THREADS".to_owned(),
            value: Some("1".to_owned()),
            ..EnvVar::default()
        },
    ];
    if component == "snapshot-activation" {
        environment.push(EnvVar {
            name: "NGKG_DATABASE_URL".to_owned(),
            value_from: Some(EnvVarSource {
                secret_key_ref: Some(SecretKeySelector {
                    key: "database-url".to_owned(),
                    name: context.worker.database_secret.clone(),
                    optional: Some(false),
                }),
                ..EnvVarSource::default()
            }),
            ..EnvVar::default()
        });
    }
    if indexed {
        environment.push(EnvVar {
            name: "JOB_COMPLETION_INDEX".to_owned(),
            value_from: Some(EnvVarSource {
                field_ref: Some(ObjectFieldSelector {
                    api_version: Some("v1".to_owned()),
                    field_path: "metadata.annotations['batch.kubernetes.io/job-completion-index']"
                        .to_owned(),
                }),
                ..EnvVarSource::default()
            }),
            ..EnvVar::default()
        });
    }
    let mut mounts = vec![VolumeMount {
        mount_path: "/scratch".to_owned(),
        name: "scratch".to_owned(),
        ..VolumeMount::default()
    }];
    let mut volumes = vec![Volume {
        name: "scratch".to_owned(),
        empty_dir: Some(EmptyDirVolumeSource {
            size_limit: Some(scratch_size),
            ..EmptyDirVolumeSource::default()
        }),
        ..Volume::default()
    }];
    if mount_source {
        mounts.push(VolumeMount {
            mount_path: "/source".to_owned(),
            name: "source".to_owned(),
            read_only: Some(true),
            ..VolumeMount::default()
        });
        volumes.push(Volume {
            name: "source".to_owned(),
            persistent_volume_claim: Some(PersistentVolumeClaimVolumeSource {
                claim_name: volume_name.to_owned(),
                read_only: Some(true),
            }),
            ..Volume::default()
        });
    }
    let env_from = context
        .worker
        .object_store_credentials_secret
        .as_ref()
        .map(|secret| {
            vec![EnvFromSource {
                secret_ref: Some(SecretEnvSource {
                    name: secret.clone(),
                    optional: Some(false),
                }),
                ..EnvFromSource::default()
            }]
        })
        .unwrap_or_default();
    let spec_hash = import_spec_hash(import);
    let mut stage_pod_annotations = source_pod_annotations(import, &spec_hash, mount_source);
    stage_pod_annotations.insert(
        "ngkg.io/stage-spec-sha256".to_owned(),
        stage_spec_sha256.clone(),
    );
    let job = Job {
        metadata: ObjectMeta {
            name: Some(name.to_owned()),
            namespace: Some(context.namespace.clone()),
            labels: Some(labels.clone()),
            annotations: Some(BTreeMap::from([
                ("ngkg.io/source-spec-sha256".to_owned(), spec_hash.clone()),
                (
                    "ngkg.io/stage-spec-sha256".to_owned(),
                    stage_spec_sha256.clone(),
                ),
            ])),
            owner_references: import.controller_owner_ref(&()).map(|owner| vec![owner]),
            ..ObjectMeta::default()
        },
        spec: Some(JobSpec {
            active_deadline_seconds: Some(context.worker.active_deadline_seconds),
            backoff_limit: (!indexed).then_some(3),
            backoff_limit_per_index: indexed.then_some(3),
            completion_mode: indexed.then(|| "Indexed".to_owned()),
            completions: Some(
                i32::try_from(completions)
                    .map_err(|_| OperatorError::SpecConflict("cloud compiler completions"))?,
            ),
            max_failed_indexes: indexed.then_some(0),
            parallelism: Some(
                i32::try_from(parallelism)
                    .map_err(|_| OperatorError::SpecConflict("cloud compiler parallelism"))?,
            ),
            ttl_seconds_after_finished: Some(context.worker.ttl_seconds_after_finished),
            template: PodTemplateSpec {
                metadata: Some(ObjectMeta {
                    labels: Some(labels),
                    annotations: Some(stage_pod_annotations),
                    ..ObjectMeta::default()
                }),
                spec: Some(PodSpec {
                    automount_service_account_token: Some(true),
                    containers: vec![Container {
                        name: "decoder".to_owned(),
                        image: Some(context.worker.image.clone()),
                        args: Some(arguments),
                        env: Some(environment),
                        env_from: Some(env_from),
                        resources: Some(resources),
                        security_context: Some(SecurityContext {
                            allow_privilege_escalation: Some(false),
                            capabilities: Some(Capabilities {
                                add: None,
                                drop: Some(vec!["ALL".to_owned()]),
                            }),
                            read_only_root_filesystem: Some(true),
                            run_as_non_root: Some(true),
                            ..SecurityContext::default()
                        }),
                        volume_mounts: Some(mounts),
                        ..Container::default()
                    }],
                    node_selector: Some(BTreeMap::from([(
                        "ngkg.io/workload".to_owned(),
                        responsibility.to_owned(),
                    )])),
                    restart_policy: Some("Never".to_owned()),
                    security_context: Some(PodSecurityContext {
                        run_as_non_root: Some(true),
                        seccomp_profile: Some(SeccompProfile {
                            localhost_profile: None,
                            type_: "RuntimeDefault".to_owned(),
                        }),
                        ..PodSecurityContext::default()
                    }),
                    service_account_name: Some(import.spec.identity_ref.clone()),
                    tolerations: Some(vec![Toleration {
                        effect: Some("NoSchedule".to_owned()),
                        key: Some("ngkg.io/workload".to_owned()),
                        operator: Some("Equal".to_owned()),
                        value: Some(responsibility.to_owned()),
                        ..Toleration::default()
                    }]),
                    volumes: Some(volumes),
                    ..PodSpec::default()
                }),
            },
            ..JobSpec::default()
        }),
        ..Job::default()
    };
    match jobs.create(&PostParams::default(), &job).await {
        Ok(created) => {
            if let Some(uid) = created.metadata.uid.as_deref() {
                context
                    .catalog
                    .mark_orchestration_stage_running(
                        import.spec.tenant_id,
                        import.spec.operation_id,
                        component,
                        uid,
                    )
                    .await?;
            }
            Ok(())
        }
        Err(kube::Error::Api(error)) if error.code == 409 => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn option_value(options: &[(String, String)], name: &str) -> String {
    options
        .iter()
        .find(|(key, _)| key == name)
        .map(|(_, value)| value.clone())
        .unwrap_or_else(|| "1".to_owned())
}

fn import_spec_hash(import: &NgkgSourceImport) -> String {
    hex::encode(Sha256::digest(
        serde_json::to_vec(&import.spec).unwrap_or_default(),
    ))
}

async fn patch_import_status(
    import: &NgkgSourceImport,
    context: &Context,
    status: NgkgSourceImportStatus,
) -> Result<(), OperatorError> {
    let imports: Api<NgkgSourceImport> =
        Api::namespaced(context.client.clone(), &context.namespace);
    let name = import.name_any();
    let document = source_import_status_apply_document(
        &name,
        &status,
        &[
            "observedGeneration", "jobName", "decodeJobName", "finalizeJobName",
            "semanticMapJobName", "semanticDictionaryJobName",
            "semanticPartitionJobName", "semanticFinalizeJobName",
            "ontologyProjectionJobName", "ontologyAssemblyJobName",
            "ontologyQualificationJobName", "offlineReasoningPlanJobName",
            "offlineReasoningPartitionJobName", "offlineReasoningFinalizeJobName",
            "snapshotActivationJobName", "condition",
        ],
    )?;
    imports
        .patch_status(
            &name,
            &PatchParams::apply("ngkg-source-import-operator"),
            &Patch::Apply(document),
        )
        .await?;
    Ok(())
}

fn verify_spec(
    spec: &NgkgCompilationSpec,
    durable: &ngkg_catalog::CompilationOperation,
) -> Result<(), OperatorError> {
    if spec.dataset_id != durable.operation.dataset_id {
        return Err(OperatorError::SpecConflict("datasetId"));
    }
    if spec.target_snapshot_id != durable.operation.target_snapshot_id {
        return Err(OperatorError::SpecConflict("targetSnapshotId"));
    }
    if spec.bundle_object_key != durable.request.bundle_object_key {
        return Err(OperatorError::SpecConflict("bundleObjectKey"));
    }
    if spec.bundle_sha256 != hex::encode(durable.request.bundle_sha256) {
        return Err(OperatorError::SpecConflict("bundleSha256"));
    }
    if spec.parent_snapshot_id != durable.request.parent_snapshot_id {
        return Err(OperatorError::SpecConflict("parentSnapshotId"));
    }
    if spec.publication_policy != durable.request.publication_policy {
        return Err(OperatorError::SpecConflict("publicationPolicy"));
    }
    if spec.resource_profile != durable.request.resource_profile {
        return Err(OperatorError::SpecConflict("resourceProfile"));
    }
    Ok(())
}

async fn ensure_reference_job(
    compilation: &NgkgCompilation,
    context: &Context,
    name: &str,
) -> Result<(), OperatorError> {
    if compilation.spec.resource_profile != context.worker.resource_profile {
        return Err(OperatorError::ResourceProfile);
    }
    let jobs: Api<Job> = Api::namespaced(context.client.clone(), &context.namespace);
    if let Some(existing) = jobs.get_opt(name).await? {
        let expected_operation = existing
            .metadata
            .labels
            .as_ref()
            .and_then(|labels| labels.get("ngkg.io/operation-id"));
        if expected_operation != Some(&compilation.spec.operation_id.to_string()) {
            return Err(OperatorError::SpecConflict("existing Job operation label"));
        }
        let expected_work_spec = work_spec_hash(compilation, context);
        let observed_work_spec = existing
            .metadata
            .annotations
            .as_ref()
            .and_then(|annotations| annotations.get("ngkg.io/work-spec-sha256"));
        if observed_work_spec != Some(&expected_work_spec) {
            return Err(OperatorError::SpecConflict("existing Job work-spec hash"));
        }
        let expected_phase40 = context.worker.phase40_direct.bundle_sha256();
        let observed_phase40 = existing
            .metadata
            .annotations
            .as_ref()
            .and_then(|annotations| annotations.get("ngkg.io/phase40-direct-ceilings-sha256"));
        if observed_phase40 != Some(&expected_phase40) {
            return Err(OperatorError::SpecConflict(
                "existing Job Phase 40 ceiling bundle hash",
            ));
        }
        let expected_uid = compilation
            .metadata
            .uid
            .as_deref()
            .ok_or(OperatorError::SpecConflict("compilation UID"))?;
        let owned = existing
            .metadata
            .owner_references
            .as_ref()
            .is_some_and(|owners| {
                owners.iter().any(|owner| {
                    owner.controller == Some(true)
                        && owner.kind == "NgkgCompilation"
                        && owner.uid == expected_uid
                })
            });
        if !owned {
            return Err(OperatorError::SpecConflict("existing Job owner"));
        }
        return Ok(());
    }
    let job = reference_job(compilation, context, name)?;
    match jobs.create(&PostParams::default(), &job).await {
        Ok(_) => Ok(()),
        Err(kube::Error::Api(response)) if response.code == 409 => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn reference_job(
    compilation: &NgkgCompilation,
    context: &Context,
    name: &str,
) -> Result<Job, OperatorError> {
    let cpu = Quantity(context.worker.cpu.clone());
    let memory = Quantity(context.worker.memory.clone());
    let scratch_size = Quantity(context.worker.scratch_size.clone());
    let resources = ResourceRequirements {
        limits: Some(BTreeMap::from([
            ("cpu".to_owned(), cpu.clone()),
            ("memory".to_owned(), memory.clone()),
            ("ephemeral-storage".to_owned(), scratch_size.clone()),
        ])),
        requests: Some(BTreeMap::from([
            ("cpu".to_owned(), cpu),
            ("memory".to_owned(), memory),
            ("ephemeral-storage".to_owned(), scratch_size.clone()),
        ])),
        ..ResourceRequirements::default()
    };
    let mut arguments = vec![
        "compile-object-store".to_owned(),
        "--tenant-id".to_owned(),
        compilation.spec.tenant_id.to_string(),
        "--operation-id".to_owned(),
        compilation.spec.operation_id.to_string(),
        "--dataset-id".to_owned(),
        compilation.spec.dataset_id.to_string(),
        "--target-snapshot-id".to_owned(),
        compilation.spec.target_snapshot_id.to_string(),
        "--bundle-object-key".to_owned(),
        compilation.spec.bundle_object_key.clone(),
        "--bundle-sha256".to_owned(),
        compilation.spec.bundle_sha256.clone(),
        "--scratch-root".to_owned(),
        "/scratch".to_owned(),
    ];
    for (option, value) in context
        .worker
        .options
        .iter()
        .filter(|(option, _)| REFERENCE_COMPILE_OPTION_NAMES.contains(&option.as_str()))
    {
        arguments.push(format!("--{option}"));
        arguments.push(value.clone());
    }
    let mut env_from = Vec::new();
    if let Some(secret) = &context.worker.object_store_credentials_secret {
        env_from.push(EnvFromSource {
            secret_ref: Some(SecretEnvSource {
                name: secret.clone(),
                optional: Some(false),
            }),
            ..EnvFromSource::default()
        });
    }
    let work_spec_sha256 = work_spec_hash(compilation, context);
    let phase40_direct_ceiling_sha256 = context.worker.phase40_direct.bundle_sha256();
    let mut environment = vec![
        EnvVar {
            name: "NGKG_DATABASE_URL".to_owned(),
            value_from: Some(EnvVarSource {
                secret_key_ref: Some(SecretKeySelector {
                    key: "database-url".to_owned(),
                    name: context.worker.database_secret.clone(),
                    optional: Some(false),
                }),
                ..EnvVarSource::default()
            }),
            ..EnvVar::default()
        },
        EnvVar {
            name: "NGKG_ARTIFACT_BASE_URL".to_owned(),
            value: Some(context.worker.artifact_base_url.clone()),
            ..EnvVar::default()
        },
        EnvVar {
            name: "JAVA_TOOL_OPTIONS".to_owned(),
            value: Some(context.worker.java_tool_options.clone()),
            ..EnvVar::default()
        },
        EnvVar {
            name: "OMP_NUM_THREADS".to_owned(),
            value: Some("1".to_owned()),
            ..EnvVar::default()
        },
        EnvVar {
            name: "OPENBLAS_NUM_THREADS".to_owned(),
            value: Some("1".to_owned()),
            ..EnvVar::default()
        },
        EnvVar {
            name: "MKL_NUM_THREADS".to_owned(),
            value: Some("1".to_owned()),
            ..EnvVar::default()
        },
        EnvVar {
            name: "OMP_DYNAMIC".to_owned(),
            value: Some("FALSE".to_owned()),
            ..EnvVar::default()
        },
        EnvVar {
            name: "OMP_MAX_ACTIVE_LEVELS".to_owned(),
            value: Some("1".to_owned()),
            ..EnvVar::default()
        },
        EnvVar {
            name: "NGKG_POD_NAME".to_owned(),
            value_from: Some(EnvVarSource {
                field_ref: Some(ObjectFieldSelector {
                    api_version: Some("v1".to_owned()),
                    field_path: "metadata.name".to_owned(),
                }),
                ..EnvVarSource::default()
            }),
            ..EnvVar::default()
        },
        EnvVar {
            name: "NGKG_PHASE40_DIRECT_CEILINGS_SHA256".to_owned(),
            value: Some(phase40_direct_ceiling_sha256.clone()),
            ..EnvVar::default()
        },
    ];
    environment.extend(context.worker.phase40_direct.env_pairs().into_iter().map(
        |(name, value)| EnvVar {
            name: name.to_owned(),
            value: Some(value),
            ..EnvVar::default()
        },
    ));

    let labels = BTreeMap::from([
        ("app.kubernetes.io/name".to_owned(), "ngkg".to_owned()),
        (
            "app.kubernetes.io/component".to_owned(),
            "reference-compilation".to_owned(),
        ),
        (
            "ngkg.io/responsibility".to_owned(),
            "semantic-projection".to_owned(),
        ),
        (
            "ngkg.io/operation-id".to_owned(),
            compilation.spec.operation_id.to_string(),
        ),
        (
            "kueue.x-k8s.io/queue-name".to_owned(),
            context.worker.queue_name.clone(),
        ),
    ]);
    Ok(Job {
        metadata: ObjectMeta {
            name: Some(name.to_owned()),
            namespace: Some(context.namespace.clone()),
            labels: Some(labels.clone()),
            annotations: Some(BTreeMap::from([
                (
                    "ngkg.io/work-spec-sha256".to_owned(),
                    work_spec_sha256.clone(),
                ),
                (
                    "ngkg.io/phase40-direct-ceilings-sha256".to_owned(),
                    phase40_direct_ceiling_sha256.clone(),
                ),
            ])),
            owner_references: compilation
                .controller_owner_ref(&())
                .map(|owner| vec![owner]),
            ..ObjectMeta::default()
        },
        spec: Some(JobSpec {
            active_deadline_seconds: Some(context.worker.active_deadline_seconds),
            backoff_limit: Some(3),
            completions: Some(1),
            parallelism: Some(1),
            ttl_seconds_after_finished: Some(context.worker.ttl_seconds_after_finished),
            template: PodTemplateSpec {
                metadata: Some(ObjectMeta {
                    labels: Some(labels),
                    annotations: Some(BTreeMap::from([
                        ("ngkg.io/work-spec-sha256".to_owned(), work_spec_sha256),
                        (
                            "ngkg.io/phase40-direct-ceilings-sha256".to_owned(),
                            phase40_direct_ceiling_sha256,
                        ),
                    ])),
                    ..ObjectMeta::default()
                }),
                spec: Some(PodSpec {
                    automount_service_account_token: Some(
                        context.worker.automount_service_account_token,
                    ),
                    containers: vec![Container {
                        name: "worker".to_owned(),
                        image: Some(context.worker.image.clone()),
                        args: Some(arguments),
                        env: Some(environment),
                        env_from: Some(env_from),
                        resources: Some(resources),
                        security_context: Some(SecurityContext {
                            allow_privilege_escalation: Some(false),
                            capabilities: Some(Capabilities {
                                add: None,
                                drop: Some(vec!["ALL".to_owned()]),
                            }),
                            read_only_root_filesystem: Some(true),
                            run_as_non_root: Some(true),
                            ..SecurityContext::default()
                        }),
                        volume_mounts: Some(vec![VolumeMount {
                            mount_path: "/scratch".to_owned(),
                            name: "scratch".to_owned(),
                            ..VolumeMount::default()
                        }]),
                        ..Container::default()
                    }],
                    node_selector: Some(BTreeMap::from([(
                        "ngkg.io/workload".to_owned(),
                        "semantic-projection".to_owned(),
                    )])),
                    restart_policy: Some("Never".to_owned()),
                    security_context: Some(PodSecurityContext {
                        run_as_non_root: Some(true),
                        seccomp_profile: Some(SeccompProfile {
                            localhost_profile: None,
                            type_: "RuntimeDefault".to_owned(),
                        }),
                        ..PodSecurityContext::default()
                    }),
                    service_account_name: Some(context.worker.service_account.clone()),
                    tolerations: Some(vec![Toleration {
                        effect: Some("NoSchedule".to_owned()),
                        key: Some("ngkg.io/workload".to_owned()),
                        operator: Some("Equal".to_owned()),
                        value: Some("semantic-projection".to_owned()),
                        ..Toleration::default()
                    }]),
                    volumes: Some(vec![Volume {
                        name: "scratch".to_owned(),
                        empty_dir: Some(EmptyDirVolumeSource {
                            size_limit: Some(scratch_size),
                            ..EmptyDirVolumeSource::default()
                        }),
                        ..Volume::default()
                    }]),
                    ..PodSpec::default()
                }),
            },
            ..JobSpec::default()
        }),
        ..Job::default()
    })
}

fn work_spec_hash(compilation: &NgkgCompilation, context: &Context) -> String {
    let canonical = json!({
        "spec": &compilation.spec,
        "workerImage": &context.worker.image,
        "serviceAccount": &context.worker.service_account,
        "databaseSecret": &context.worker.database_secret,
        "objectStoreCredentialsSecret": &context.worker.object_store_credentials_secret,
        "artifactBaseUrl": &context.worker.artifact_base_url,
        "resourceProfile": &context.worker.resource_profile,
        "queueName": &context.worker.queue_name,
        "cpu": &context.worker.cpu,
        "memory": &context.worker.memory,
        "scratchSize": &context.worker.scratch_size,
        "activeDeadlineSeconds": context.worker.active_deadline_seconds,
        "ttlSecondsAfterFinished": context.worker.ttl_seconds_after_finished,
        "javaToolOptions": &context.worker.java_tool_options,
        "automountServiceAccountToken": context.worker.automount_service_account_token,
        "phase40DirectCeilings": &context.worker.phase40_direct,
        "phase40DirectCeilingsSha256": context.worker.phase40_direct.bundle_sha256(),
        "options": &context.worker.options,
    });
    hex::encode(Sha256::digest(canonical.to_string().as_bytes()))
}

async fn delete_job_if_present(context: &Context, name: &str) -> Result<(), OperatorError> {
    let jobs: Api<Job> = Api::namespaced(context.client.clone(), &context.namespace);
    if jobs.get_opt(name).await?.is_some() {
        jobs.delete(name, &DeleteParams::default()).await?;
    }
    Ok(())
}

async fn reference_job_terminal_without_catalog_commit(
    context: &Context,
    name: &str,
) -> Result<Option<(&'static str, &'static str)>, OperatorError> {
    let jobs: Api<Job> = Api::namespaced(context.client.clone(), &context.namespace);
    let Some(job) = jobs.get_opt(name).await? else {
        return Ok(None);
    };
    let conditions = job
        .status
        .and_then(|status| status.conditions)
        .unwrap_or_default();
    if conditions
        .iter()
        .any(|condition| condition.type_ == "Failed" && condition.status == "True")
    {
        return Ok(Some((
            "INFRASTRUCTURE_RETRY_EXHAUSTED",
            "InfrastructureRetryExhausted",
        )));
    }
    if conditions
        .iter()
        .any(|condition| condition.type_ == "Complete" && condition.status == "True")
    {
        return Ok(Some((
            "JOB_COMPLETED_WITHOUT_CERTIFICATION",
            "JobCompletedWithoutCertification",
        )));
    }
    Ok(None)
}

async fn patch_status(
    compilation: &NgkgCompilation,
    context: &Context,
    job_name: Option<String>,
    state: JobState,
    condition: &str,
) -> Result<(), OperatorError> {
    let compilations: Api<NgkgCompilation> =
        Api::namespaced(context.client.clone(), &context.namespace);
    let status = NgkgCompilationStatus {
        observed_generation: compilation.metadata.generation,
        catalog_state: Some(state.as_db().to_owned()),
        job_name,
        condition: Some(condition.to_owned()),
    };
    compilations
        .patch_status(
            &compilation.name_any(),
            &PatchParams::default(),
            &Patch::Merge(json!({"status": status})),
        )
        .await?;
    Ok(())
}

fn required(name: &str) -> Result<String> {
    env::var(name).with_context(|| format!("{name} is required"))
}

fn optional(name: &str) -> Option<String> {
    env::var(name).ok().filter(|value| !value.is_empty())
}

fn positive_i64(name: &str) -> Result<i64> {
    required(name)?
        .parse::<i64>()
        .ok()
        .filter(|value| *value > 0)
        .with_context(|| format!("{name} must be a positive integer"))
}

fn positive_i32(name: &str) -> Result<i32> {
    required(name)?
        .parse::<i32>()
        .ok()
        .filter(|value| *value > 0)
        .with_context(|| format!("{name} must be a positive 32-bit integer"))
}

fn required_bool(name: &str) -> Result<bool> {
    required(name)?
        .parse::<bool>()
        .with_context(|| format!("{name} must be true or false"))
}

fn required_exact(name: &str, expected: &str) -> Result<String> {
    let value = required(name)?;
    if value != expected {
        anyhow::bail!("{name} must equal the qualified CSI driver {expected}");
    }
    Ok(value)
}

fn required_digest_image(name: &str) -> Result<String> {
    let value = required(name)?;
    let Some((repository, digest)) = value.rsplit_once("@sha256:") else {
        anyhow::bail!("{name} must be pinned as repository@sha256:<64 lowercase hex>");
    };
    if repository.is_empty()
        || digest.len() != 64
        || !digest
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        anyhow::bail!("{name} must be pinned as repository@sha256:<64 lowercase hex>");
    }
    Ok(value)
}
