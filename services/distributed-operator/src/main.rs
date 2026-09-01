//! Level-based Kubernetes controller for one distributed compilation DAG.

use std::{collections::BTreeMap, env, sync::Arc, time::Duration};

use anyhow::{Context as AnyhowContext, Result};
use futures::StreamExt;
use k8s_openapi::{
    api::{
        batch::v1::{Job, JobSpec},
        core::v1::{
            Capabilities, Container, EmptyDirVolumeSource, EnvFromSource, EnvVar, EnvVarSource,
            ObjectFieldSelector, PodSecurityContext, PodSpec, PodTemplateSpec,
            ResourceRequirements, SeccompProfile, SecretEnvSource, SecretKeySelector,
            SecurityContext, Toleration, Volume, VolumeMount,
        },
    },
    apimachinery::pkg::api::resource::Quantity,
};
use kube::{
    Api, Client, Resource, ResourceExt,
    api::{DeleteParams, ObjectMeta, Patch, PatchParams, PostParams},
    runtime::{Controller, controller::Action, watcher},
};
use ngkg_catalog::{CatalogError, JobState, OperationRepository};
use ngkg_kube::{NgkgCompilation, NgkgCompilationSpec, NgkgCompilationStatus};
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
    config: Config,
}

#[derive(Clone)]
struct Config {
    resource_profile: String,
    distributed_image: String,
    reference_image: String,
    distributed_service_account: String,
    reference_service_account: String,
    database_secret: String,
    artifact_base_url: String,
    object_store_credentials_secret: Option<String>,
    distributed_automount_service_account_token: bool,
    reference_automount_service_account_token: bool,
    logical_partitions: i32,
    reducer_count: i32,
    max_quads: u64,
    artifact_row_group_rows: usize,
    artifact_options: Vec<(String, String)>,
    reference_options: Vec<(String, String)>,
    java_tool_options: String,
    phase40_direct: Phase40DirectCeilings,
    ttl_seconds_after_finished: i32,
    stages: BTreeMap<Stage, StageResources>,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum Stage {
    Plan,
    Projection,
    Reducer,
    Finalize,
    ArtifactPlan,
    Artifact,
    ArtifactFinalize,
    ServingRoot,
    Reasoner,
}

#[derive(Clone)]
struct StageResources {
    responsibility: String,
    queue: String,
    cpu: String,
    memory: String,
    scratch: String,
    max_parallelism: i32,
    active_deadline_seconds: i64,
}

#[derive(Debug, Error)]
enum OperatorError {
    #[error("catalog failed: {0}")]
    Catalog(#[from] CatalogError),
    #[error("Kubernetes failed: {0}")]
    Kubernetes(#[from] kube::Error),
    #[error("CR differs from durable catalog field {0}")]
    SpecConflict(&'static str),
    #[error("distributed operator configuration is inconsistent: {0}")]
    Config(String),
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
    let context = Arc::new(Context {
        client: client.clone(),
        catalog: OperationRepository::new(pool),
        namespace: namespace.clone(),
        config: Config::from_env()?,
    });
    let compilations: Api<NgkgCompilation> = Api::namespaced(client.clone(), &namespace);
    let jobs: Api<Job> = Api::namespaced(client, &namespace);
    Controller::new(compilations, watcher::Config::default())
        .owns(jobs, watcher::Config::default())
        .run(reconcile, error_policy, context)
        .for_each(|result| async move {
            if let Err(error) = result {
                tracing::error!(%error, "distributed reconciliation stream failed");
            }
        })
        .await;
    Ok(())
}

impl Config {
    fn from_env() -> Result<Self> {
        let logical_partitions = positive_i32("NGKG_DISTRIBUTED_LOGICAL_PARTITIONS")?;
        let reducer_count = positive_i32("NGKG_DISTRIBUTED_REDUCER_COUNT")?;
        if reducer_count > logical_partitions {
            anyhow::bail!("NGKG_DISTRIBUTED_REDUCER_COUNT cannot exceed logical partitions");
        }
        let stages = [
            (Stage::Plan, "PLAN", "semantic-projection"),
            (Stage::Projection, "PROJECTION", "semantic-projection"),
            (Stage::Reducer, "REDUCER", "index-build"),
            (Stage::Finalize, "FINALIZE", "index-build"),
            (
                Stage::ArtifactPlan,
                "ARTIFACT_PLAN",
                "semantic-artifact-build",
            ),
            (Stage::Artifact, "ARTIFACT", "semantic-artifact-build"),
            (Stage::ArtifactFinalize, "ARTIFACT_FINALIZE", "index-build"),
            (Stage::ServingRoot, "SERVING_ROOT", "index-build"),
            (Stage::Reasoner, "REASONER", "reasoning"),
        ]
        .into_iter()
        .map(|(stage, prefix, responsibility)| {
            Ok((
                stage,
                StageResources {
                    responsibility: responsibility.to_owned(),
                    queue: required(&format!("NGKG_{prefix}_QUEUE"))?,
                    cpu: required(&format!("NGKG_{prefix}_CPU"))?,
                    memory: required(&format!("NGKG_{prefix}_MEMORY"))?,
                    scratch: required(&format!("NGKG_{prefix}_SCRATCH"))?,
                    max_parallelism: positive_i32(&format!("NGKG_{prefix}_MAX_PARALLELISM"))?,
                    active_deadline_seconds: positive_i64(&format!(
                        "NGKG_{prefix}_ACTIVE_DEADLINE_SECONDS"
                    ))?,
                },
            ))
        })
        .collect::<Result<BTreeMap<_, _>>>()?;
        let artifact_options = [
            ("max-object-bytes", "NGKG_DISTRIBUTED_MAX_OBJECT_BYTES"),
            ("single-put-max-bytes", "NGKG_SINGLE_PUT_MAX_BYTES"),
            ("multipart-buffer-bytes", "NGKG_MULTIPART_BUFFER_BYTES"),
            ("multipart-concurrency", "NGKG_MULTIPART_CONCURRENCY"),
        ]
        .into_iter()
        .map(|(option, variable)| Ok((option.to_owned(), required(variable)?)))
        .collect::<Result<Vec<_>>>()?;
        let reference_options = [
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
            ("ceiling-hydration-rows", "NGKG_CEILING_HYDRATION_ROWS"),
            ("hydration-worker-threads", "NGKG_HYDRATION_WORKER_THREADS"),
            ("download-concurrency", "NGKG_DOWNLOAD_CONCURRENCY"),
            ("upload-concurrency", "NGKG_UPLOAD_CONCURRENCY"),
            ("single-put-max-bytes", "NGKG_SINGLE_PUT_MAX_BYTES"),
            ("multipart-buffer-bytes", "NGKG_MULTIPART_BUFFER_BYTES"),
            ("multipart-concurrency", "NGKG_MULTIPART_CONCURRENCY"),
        ]
        .into_iter()
        .map(|(option, variable)| Ok((option.to_owned(), required(variable)?)))
        .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            resource_profile: required("NGKG_DISTRIBUTED_RESOURCE_PROFILE")?,
            distributed_image: required_digest_image("NGKG_DISTRIBUTED_WORKER_IMAGE")?,
            reference_image: required_digest_image("NGKG_REFERENCE_WORKER_IMAGE")?,
            distributed_service_account: required("NGKG_DISTRIBUTED_WORKER_SERVICE_ACCOUNT")?,
            reference_service_account: required("NGKG_REFERENCE_SERVICE_ACCOUNT")?,
            database_secret: required("NGKG_DATABASE_SECRET")?,
            artifact_base_url: required("NGKG_ARTIFACT_BASE_URL")?,
            object_store_credentials_secret: optional("NGKG_OBJECT_STORE_CREDENTIALS_SECRET"),
            distributed_automount_service_account_token: required_bool(
                "NGKG_DISTRIBUTED_AUTOMOUNT_SERVICE_ACCOUNT_TOKEN",
            )?,
            reference_automount_service_account_token: required_bool(
                "NGKG_REFERENCE_AUTOMOUNT_SERVICE_ACCOUNT_TOKEN",
            )?,
            logical_partitions,
            reducer_count,
            max_quads: positive_u64("NGKG_DISTRIBUTED_MAX_QUADS")?,
            artifact_row_group_rows: positive_usize("NGKG_DISTRIBUTED_ARTIFACT_ROW_GROUP_ROWS")?,
            artifact_options,
            reference_options,
            java_tool_options: required("NGKG_REFERENCE_JAVA_TOOL_OPTIONS")?,
            phase40_direct: Phase40DirectCeilings::from_env()
                .context("invalid Phase 40 direct ceiling bundle")?,
            ttl_seconds_after_finished: positive_i32(
                "NGKG_DISTRIBUTED_TTL_SECONDS_AFTER_FINISHED",
            )?,
            stages,
        })
    }

    fn stage(&self, stage: Stage) -> &StageResources {
        &self.stages[&stage]
    }
}

async fn reconcile(
    compilation: Arc<NgkgCompilation>,
    context: Arc<Context>,
) -> Result<Action, OperatorError> {
    if compilation.spec.resource_profile != context.config.resource_profile {
        return Ok(Action::await_change());
    }
    let durable = context
        .catalog
        .get_compilation(compilation.spec.tenant_id, compilation.spec.operation_id)
        .await?;
    verify_spec(&compilation.spec, &durable)?;
    if durable.operation.state == JobState::Cancelled {
        delete_stage_jobs(&compilation, &context).await?;
        patch_status(
            &compilation,
            &context,
            None,
            durable.operation.state,
            "Cancelled",
        )
        .await?;
        return Ok(Action::await_change());
    }
    if durable.operation.state == JobState::Failed {
        patch_status(
            &compilation,
            &context,
            None,
            durable.operation.state,
            "TerminalFailure",
        )
        .await?;
        return Ok(Action::await_change());
    }
    if matches!(
        durable.operation.state,
        JobState::Certified | JobState::Published
    ) {
        patch_status(
            &compilation,
            &context,
            Some(stage_name(&compilation, Stage::Reasoner)),
            durable.operation.state,
            durable.operation.state.as_db(),
        )
        .await?;
        return Ok(Action::await_change());
    }
    match durable.operation.state {
        JobState::Registered => {
            let args = planner_args(&compilation, &context.config);
            schedule_stage(&compilation, &context, Stage::Plan, 1, 1, args, false).await
        }
        JobState::Partitioned => {
            let plan = context
                .catalog
                .get_distributed_plan(compilation.spec.tenant_id, compilation.spec.operation_id)
                .await?;
            let args = indexed_args(&compilation, &context.config, Stage::Projection);
            let parallelism = plan
                .logical_partition_count
                .min(context.config.stage(Stage::Projection).max_parallelism);
            schedule_stage(
                &compilation,
                &context,
                Stage::Projection,
                plan.logical_partition_count,
                parallelism,
                args,
                true,
            )
            .await
        }
        JobState::Projected => {
            let plan = context
                .catalog
                .get_distributed_plan(compilation.spec.tenant_id, compilation.spec.operation_id)
                .await?;
            let args = indexed_args(&compilation, &context.config, Stage::Reducer);
            let parallelism = plan
                .reducer_count
                .min(context.config.stage(Stage::Reducer).max_parallelism);
            schedule_stage(
                &compilation,
                &context,
                Stage::Reducer,
                plan.reducer_count,
                parallelism,
                args,
                true,
            )
            .await
        }
        JobState::Indexed => {
            match context
                .catalog
                .get_distributed_root(compilation.spec.tenant_id, compilation.spec.operation_id)
                .await
            {
                Ok(root) => reconcile_artifact_barrier(&compilation, &context, &root).await,
                Err(CatalogError::NotFound) => {
                    let args = indexed_args(&compilation, &context.config, Stage::Finalize);
                    schedule_stage(&compilation, &context, Stage::Finalize, 1, 1, args, false).await
                }
                Err(error) => Err(error.into()),
            }
        }
        JobState::SourcePlanned
        | JobState::MappingValidated
        | JobState::Identified
        | JobState::SpineWritten
        | JobState::Reasoned => {
            patch_status(
                &compilation,
                &context,
                None,
                durable.operation.state,
                "CatalogBarrier",
            )
            .await?;
            Ok(Action::requeue(Duration::from_secs(3)))
        }
        JobState::Certified | JobState::Published | JobState::Failed | JobState::Cancelled => {
            Ok(Action::await_change())
        }
    }
}

async fn reconcile_artifact_barrier(
    compilation: &NgkgCompilation,
    context: &Context,
    distributed_root: &ngkg_catalog::DistributedRoot,
) -> Result<Action, OperatorError> {
    match context
        .catalog
        .get_artifact_plan(compilation.spec.tenant_id, compilation.spec.operation_id)
        .await
    {
        Err(CatalogError::NotFound) => {
            let args = indexed_args(compilation, &context.config, Stage::ArtifactPlan);
            schedule_stage(compilation, context, Stage::ArtifactPlan, 1, 1, args, false).await
        }
        Err(error) => Err(error.into()),
        Ok(plan) => match context
            .catalog
            .get_artifact_root(compilation.spec.tenant_id, compilation.spec.operation_id)
            .await
        {
            Ok(artifact_root) => {
                match context
                    .catalog
                    .get_serving_root(compilation.spec.tenant_id, compilation.spec.operation_id)
                    .await
                {
                    Ok(serving_root) => {
                        let args = reasoner_args(
                            compilation,
                            &context.config,
                            distributed_root,
                            &artifact_root,
                            &serving_root,
                        );
                        schedule_stage(compilation, context, Stage::Reasoner, 1, 1, args, false)
                            .await
                    }
                    Err(CatalogError::NotFound) => {
                        let args = indexed_args(compilation, &context.config, Stage::ServingRoot);
                        schedule_stage(compilation, context, Stage::ServingRoot, 1, 1, args, false)
                            .await
                    }
                    Err(error) => Err(error.into()),
                }
            }
            Err(CatalogError::NotFound) if plan.failed_artifacts != 0 => Err(
                OperatorError::Config("artifact completion index contains failures".to_owned()),
            ),
            Err(CatalogError::NotFound)
                if plan.succeeded_artifacts == i64::from(plan.partition_count) =>
            {
                let args = indexed_args(compilation, &context.config, Stage::ArtifactFinalize);
                schedule_stage(
                    compilation,
                    context,
                    Stage::ArtifactFinalize,
                    1,
                    1,
                    args,
                    false,
                )
                .await
            }
            Err(CatalogError::NotFound) => {
                let args = indexed_args(compilation, &context.config, Stage::Artifact);
                let parallelism = plan
                    .partition_count
                    .min(context.config.stage(Stage::Artifact).max_parallelism);
                schedule_stage(
                    compilation,
                    context,
                    Stage::Artifact,
                    plan.partition_count,
                    parallelism,
                    args,
                    true,
                )
                .await
            }
            Err(error) => Err(error.into()),
        },
    }
}

async fn artifact_plan_missing(
    catalog: &OperationRepository,
    compilation: &NgkgCompilation,
) -> Result<bool, OperatorError> {
    match catalog
        .get_artifact_plan(compilation.spec.tenant_id, compilation.spec.operation_id)
        .await
    {
        Ok(_) => Ok(false),
        Err(CatalogError::NotFound) => Ok(true),
        Err(error) => Err(error.into()),
    }
}

async fn artifact_work_pending(
    catalog: &OperationRepository,
    compilation: &NgkgCompilation,
) -> Result<bool, OperatorError> {
    let plan = catalog
        .get_artifact_plan(compilation.spec.tenant_id, compilation.spec.operation_id)
        .await?;
    Ok(plan.failed_artifacts == 0 && plan.succeeded_artifacts < i64::from(plan.partition_count))
}

async fn artifact_root_missing(
    catalog: &OperationRepository,
    compilation: &NgkgCompilation,
) -> Result<bool, OperatorError> {
    match catalog
        .get_artifact_root(compilation.spec.tenant_id, compilation.spec.operation_id)
        .await
    {
        Ok(_) => Ok(false),
        Err(CatalogError::NotFound) => Ok(true),
        Err(error) => Err(error.into()),
    }
}

async fn serving_root_missing(
    catalog: &OperationRepository,
    compilation: &NgkgCompilation,
) -> Result<bool, OperatorError> {
    match catalog
        .get_serving_root(compilation.spec.tenant_id, compilation.spec.operation_id)
        .await
    {
        Ok(_) => Ok(false),
        Err(CatalogError::NotFound) => Ok(true),
        Err(error) => Err(error.into()),
    }
}

async fn schedule_stage(
    compilation: &NgkgCompilation,
    context: &Context,
    stage: Stage,
    completions: i32,
    parallelism: i32,
    args: Vec<String>,
    indexed: bool,
) -> Result<Action, OperatorError> {
    if completions <= 0 || parallelism <= 0 || parallelism > completions {
        return Err(OperatorError::Config(
            "invalid Job completion or parallelism count".to_owned(),
        ));
    }
    let name = stage_name(compilation, stage);
    let job = stage_job(
        compilation,
        context,
        stage,
        &name,
        completions,
        parallelism,
        args,
        indexed,
    )?;
    ensure_job(context, compilation, &job).await?;
    if let Some(terminal) = job_terminal(context, &name).await? {
        let latest = context
            .catalog
            .get_compilation(compilation.spec.tenant_id, compilation.spec.operation_id)
            .await?;
        let still_expected = match stage {
            Stage::Plan => latest.operation.state == JobState::Registered,
            Stage::Projection => latest.operation.state == JobState::Partitioned,
            Stage::Reducer => latest.operation.state == JobState::Projected,
            Stage::Finalize => {
                if latest.operation.state != JobState::Indexed {
                    false
                } else {
                    match context
                        .catalog
                        .get_distributed_root(
                            compilation.spec.tenant_id,
                            compilation.spec.operation_id,
                        )
                        .await
                    {
                        Ok(_) => false,
                        Err(CatalogError::NotFound) => true,
                        Err(error) => return Err(error.into()),
                    }
                }
            }
            Stage::ArtifactPlan => artifact_plan_missing(&context.catalog, compilation).await?,
            Stage::Artifact => artifact_work_pending(&context.catalog, compilation).await?,
            Stage::ArtifactFinalize => artifact_root_missing(&context.catalog, compilation).await?,
            Stage::ServingRoot => serving_root_missing(&context.catalog, compilation).await?,
            Stage::Reasoner => latest.operation.state == JobState::Indexed,
        };
        if !still_expected {
            return Ok(Action::requeue(Duration::from_secs(1)));
        }
        let code = match terminal {
            JobTerminal::Failed => format!("{}_RETRY_EXHAUSTED", stage_label(stage).to_uppercase()),
            JobTerminal::Complete => format!(
                "{}_COMPLETED_WITHOUT_CATALOG_COMMIT",
                stage_label(stage).to_uppercase()
            ),
        };
        let failed = context
            .catalog
            .fail(
                compilation.spec.tenant_id,
                compilation.spec.operation_id,
                &code,
                None,
                "ngkg-distributed-operator",
            )
            .await?;
        patch_status(compilation, context, Some(name), failed.state, &code).await?;
        return Ok(Action::await_change());
    }
    patch_status(
        compilation,
        context,
        Some(name),
        state_for_stage(stage),
        &format!("{}Scheduled", stage_label(stage)),
    )
    .await?;
    Ok(Action::requeue(Duration::from_secs(10)))
}

fn planner_args(compilation: &NgkgCompilation, config: &Config) -> Vec<String> {
    let mut args = vec![
        "plan-object-store".to_owned(),
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
        "--logical-partitions".to_owned(),
        config.logical_partitions.to_string(),
        "--reducer-count".to_owned(),
        config.reducer_count.to_string(),
        "--max-quads".to_owned(),
        config.max_quads.to_string(),
    ];
    append_options(&mut args, &config.artifact_options);
    args
}

fn indexed_args(compilation: &NgkgCompilation, config: &Config, stage: Stage) -> Vec<String> {
    let command = match stage {
        Stage::Projection => "project-object-store",
        Stage::Reducer => "reduce-object-store",
        Stage::Finalize => "finalize-object-store",
        Stage::ArtifactPlan => "prepare-artifacts-object-store",
        Stage::Artifact => "materialize-artifact-object-store",
        Stage::ArtifactFinalize => "finalize-artifacts-object-store",
        Stage::ServingRoot => "prepare-serving-root-object-store",
        Stage::Plan | Stage::Reasoner => "invalid",
    };
    let mut args = vec![
        command.to_owned(),
        "--tenant-id".to_owned(),
        compilation.spec.tenant_id.to_string(),
        "--operation-id".to_owned(),
        compilation.spec.operation_id.to_string(),
    ];
    if matches!(stage, Stage::Projection | Stage::Reducer | Stage::Artifact) {
        args.extend([
            "--work-index".to_owned(),
            "$(JOB_COMPLETION_INDEX)".to_owned(),
        ]);
    }
    args.extend(["--scratch-root".to_owned(), "/scratch".to_owned()]);
    if stage == Stage::Projection {
        args.extend(["--max-quads".to_owned(), config.max_quads.to_string()]);
    }
    if stage == Stage::ArtifactPlan {
        args.extend([
            "--row-group-rows".to_owned(),
            config.artifact_row_group_rows.to_string(),
        ]);
    }
    if stage == Stage::Artifact {
        args.extend(["--max-quads".to_owned(), config.max_quads.to_string()]);
    }
    append_options(&mut args, &config.artifact_options);
    args
}

fn reasoner_args(
    compilation: &NgkgCompilation,
    config: &Config,
    root: &ngkg_catalog::DistributedRoot,
    artifact_root: &ngkg_catalog::DistributedArtifactRoot,
    serving_root: &ngkg_catalog::DistributedServingRoot,
) -> Vec<String> {
    let mut args = vec![
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
        "--distributed-root-object-key".to_owned(),
        root.root_manifest_object_key.clone(),
        "--distributed-root-sha256".to_owned(),
        root.root_manifest_sha256.clone(),
        "--distributed-artifact-root-object-key".to_owned(),
        artifact_root.root_manifest_object_key.clone(),
        "--distributed-artifact-root-sha256".to_owned(),
        artifact_root.root_manifest_sha256.clone(),
        "--distributed-serving-root-object-key".to_owned(),
        serving_root.serving_root_object_key.clone(),
        "--distributed-serving-root-sha256".to_owned(),
        serving_root.serving_root_sha256.clone(),
    ];
    append_options(&mut args, &config.reference_options);
    args
}

fn append_options(args: &mut Vec<String>, options: &[(String, String)]) {
    for (name, value) in options {
        args.push(format!("--{name}"));
        args.push(value.clone());
    }
}

#[allow(clippy::too_many_arguments)]
fn stage_job(
    compilation: &NgkgCompilation,
    context: &Context,
    stage: Stage,
    name: &str,
    completions: i32,
    parallelism: i32,
    args: Vec<String>,
    indexed: bool,
) -> Result<Job, OperatorError> {
    let config = &context.config;
    let stage_config = config.stage(stage);
    let cpu = Quantity(stage_config.cpu.clone());
    let memory = Quantity(stage_config.memory.clone());
    let scratch = Quantity(stage_config.scratch.clone());
    let image = if stage == Stage::Reasoner {
        &config.reference_image
    } else {
        &config.distributed_image
    };
    let service_account = if stage == Stage::Reasoner {
        &config.reference_service_account
    } else {
        &config.distributed_service_account
    };
    let labels = BTreeMap::from([
        ("app.kubernetes.io/name".to_owned(), "ngkg".to_owned()),
        (
            "app.kubernetes.io/component".to_owned(),
            format!("distributed-{}", stage_label(stage)),
        ),
        ("ngkg.io/network-plane".to_owned(), "batch".to_owned()),
        (
            "ngkg.io/responsibility".to_owned(),
            stage_config.responsibility.clone(),
        ),
        (
            "ngkg.io/operation-id".to_owned(),
            compilation.spec.operation_id.to_string(),
        ),
        ("ngkg.io/stage".to_owned(), stage_label(stage).to_owned()),
        (
            "kueue.x-k8s.io/queue-name".to_owned(),
            stage_config.queue.clone(),
        ),
    ]);
    let hash = work_spec_hash(compilation, config, stage, &args, completions, parallelism);
    let phase40_direct_ceiling_sha256 =
        (stage == Stage::Reasoner).then(|| config.phase40_direct.bundle_sha256());
    let mut annotations = BTreeMap::from([("ngkg.io/work-spec-sha256".to_owned(), hash.clone())]);
    if let Some(value) = &phase40_direct_ceiling_sha256 {
        annotations.insert(
            "ngkg.io/phase40-direct-ceilings-sha256".to_owned(),
            value.clone(),
        );
    }
    let resources = ResourceRequirements {
        limits: Some(BTreeMap::from([
            ("cpu".to_owned(), cpu.clone()),
            ("memory".to_owned(), memory.clone()),
            ("ephemeral-storage".to_owned(), scratch.clone()),
        ])),
        requests: Some(BTreeMap::from([
            ("cpu".to_owned(), cpu),
            ("memory".to_owned(), memory),
            ("ephemeral-storage".to_owned(), scratch.clone()),
        ])),
        ..ResourceRequirements::default()
    };
    let mut env_from = Vec::new();
    if let Some(secret) = &config.object_store_credentials_secret {
        env_from.push(EnvFromSource {
            secret_ref: Some(SecretEnvSource {
                name: secret.clone(),
                optional: Some(false),
            }),
            ..EnvFromSource::default()
        });
    }
    let mut environment = vec![
        EnvVar {
            name: "NGKG_DATABASE_URL".to_owned(),
            value_from: Some(EnvVarSource {
                secret_key_ref: Some(SecretKeySelector {
                    key: "database-url".to_owned(),
                    name: config.database_secret.clone(),
                    optional: Some(false),
                }),
                ..EnvVarSource::default()
            }),
            ..EnvVar::default()
        },
        EnvVar {
            name: "NGKG_ARTIFACT_BASE_URL".to_owned(),
            value: Some(config.artifact_base_url.clone()),
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
    ];
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
    if stage == Stage::Reasoner {
        environment.push(EnvVar {
            name: "JAVA_TOOL_OPTIONS".to_owned(),
            value: Some(config.java_tool_options.clone()),
            ..EnvVar::default()
        });
        environment.push(EnvVar {
            name: "NGKG_PHASE40_DIRECT_CEILINGS_SHA256".to_owned(),
            value: phase40_direct_ceiling_sha256.clone(),
            ..EnvVar::default()
        });
        environment.extend(
            config
                .phase40_direct
                .env_pairs()
                .into_iter()
                .map(|(name, value)| EnvVar {
                    name: name.to_owned(),
                    value: Some(value),
                    ..EnvVar::default()
                }),
        );
    }
    Ok(Job {
        metadata: ObjectMeta {
            name: Some(name.to_owned()),
            namespace: Some(context.namespace.clone()),
            labels: Some(labels.clone()),
            annotations: Some(annotations.clone()),
            owner_references: compilation
                .controller_owner_ref(&())
                .map(|owner| vec![owner]),
            ..ObjectMeta::default()
        },
        spec: Some(JobSpec {
            active_deadline_seconds: Some(stage_config.active_deadline_seconds),
            backoff_limit: (!indexed).then_some(3),
            backoff_limit_per_index: indexed.then_some(3),
            completion_mode: indexed.then(|| "Indexed".to_owned()),
            completions: Some(completions),
            max_failed_indexes: indexed.then_some(0),
            parallelism: Some(parallelism),
            ttl_seconds_after_finished: Some(config.ttl_seconds_after_finished),
            template: PodTemplateSpec {
                metadata: Some(ObjectMeta {
                    labels: Some(labels),
                    annotations: Some(annotations),
                    ..ObjectMeta::default()
                }),
                spec: Some(PodSpec {
                    automount_service_account_token: Some(if stage == Stage::Reasoner {
                        config.reference_automount_service_account_token
                    } else {
                        config.distributed_automount_service_account_token
                    }),
                    containers: vec![Container {
                        name: "worker".to_owned(),
                        image: Some(image.clone()),
                        args: Some(args),
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
                            name: "scratch".to_owned(),
                            mount_path: "/scratch".to_owned(),
                            ..VolumeMount::default()
                        }]),
                        ..Container::default()
                    }],
                    node_selector: Some(BTreeMap::from([(
                        "ngkg.io/workload".to_owned(),
                        stage_config.responsibility.clone(),
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
                    service_account_name: Some(service_account.clone()),
                    tolerations: Some(vec![Toleration {
                        key: Some("ngkg.io/workload".to_owned()),
                        operator: Some("Equal".to_owned()),
                        value: Some(stage_config.responsibility.clone()),
                        effect: Some("NoSchedule".to_owned()),
                        ..Toleration::default()
                    }]),
                    volumes: Some(vec![Volume {
                        name: "scratch".to_owned(),
                        empty_dir: Some(EmptyDirVolumeSource {
                            size_limit: Some(scratch),
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

async fn ensure_job(
    context: &Context,
    compilation: &NgkgCompilation,
    expected: &Job,
) -> Result<(), OperatorError> {
    let name = expected
        .metadata
        .name
        .as_deref()
        .ok_or(OperatorError::SpecConflict("Job name"))?;
    let jobs: Api<Job> = Api::namespaced(context.client.clone(), &context.namespace);
    if let Some(existing) = jobs.get_opt(name).await? {
        return verify_existing_job(compilation, expected, &existing);
    }
    match jobs.create(&PostParams::default(), expected).await {
        Ok(_) => Ok(()),
        Err(kube::Error::Api(response)) if response.code == 409 => {
            let existing = jobs.get(name).await?;
            verify_existing_job(compilation, expected, &existing)
        }
        Err(error) => Err(error.into()),
    }
}

fn verify_existing_job(
    compilation: &NgkgCompilation,
    expected: &Job,
    existing: &Job,
) -> Result<(), OperatorError> {
    let expected_hash = expected
        .metadata
        .annotations
        .as_ref()
        .and_then(|values| values.get("ngkg.io/work-spec-sha256"));
    let observed_hash = existing
        .metadata
        .annotations
        .as_ref()
        .and_then(|values| values.get("ngkg.io/work-spec-sha256"));
    let operation = existing
        .metadata
        .labels
        .as_ref()
        .and_then(|values| values.get("ngkg.io/operation-id"));
    let operation_id = compilation.spec.operation_id.to_string();
    let uid = compilation
        .metadata
        .uid
        .as_deref()
        .ok_or(OperatorError::SpecConflict("CR UID"))?;
    let owned = existing
        .metadata
        .owner_references
        .as_ref()
        .is_some_and(|owners| {
            owners.iter().any(|owner| {
                owner.controller == Some(true)
                    && owner.kind == "NgkgCompilation"
                    && owner.uid == uid
            })
        });
    let expected_phase40 = expected
        .metadata
        .annotations
        .as_ref()
        .and_then(|values| values.get("ngkg.io/phase40-direct-ceilings-sha256"));
    let observed_phase40 = existing
        .metadata
        .annotations
        .as_ref()
        .and_then(|values| values.get("ngkg.io/phase40-direct-ceilings-sha256"));
    if expected_hash != observed_hash
        || expected_phase40 != observed_phase40
        || operation != Some(&operation_id)
        || !owned
    {
        return Err(OperatorError::SpecConflict("existing distributed Job"));
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum JobTerminal {
    Failed,
    Complete,
}

async fn job_terminal(context: &Context, name: &str) -> Result<Option<JobTerminal>, OperatorError> {
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
        return Ok(Some(JobTerminal::Failed));
    }
    if conditions
        .iter()
        .any(|condition| condition.type_ == "Complete" && condition.status == "True")
    {
        return Ok(Some(JobTerminal::Complete));
    }
    Ok(None)
}

async fn delete_stage_jobs(
    compilation: &NgkgCompilation,
    context: &Context,
) -> Result<(), OperatorError> {
    let jobs: Api<Job> = Api::namespaced(context.client.clone(), &context.namespace);
    for stage in [
        Stage::Plan,
        Stage::Projection,
        Stage::Reducer,
        Stage::Finalize,
        Stage::ArtifactPlan,
        Stage::Artifact,
        Stage::ArtifactFinalize,
        Stage::ServingRoot,
        Stage::Reasoner,
    ] {
        let name = stage_name(compilation, stage);
        if jobs.get_opt(&name).await?.is_some() {
            jobs.delete(&name, &DeleteParams::default()).await?;
        }
    }
    Ok(())
}

fn stage_name(compilation: &NgkgCompilation, stage: Stage) -> String {
    let suffix = match stage {
        Stage::Plan => "plan",
        Stage::Projection => "project",
        Stage::Reducer => "reduce",
        Stage::Finalize => "finalize",
        Stage::ArtifactPlan => "artifact-plan",
        Stage::Artifact => "artifact",
        Stage::ArtifactFinalize => "artifact-finalize",
        Stage::ServingRoot => "serving-root",
        Stage::Reasoner => "reason",
    };
    format!("{}-{suffix}", compilation.name_any())
}

fn stage_label(stage: Stage) -> &'static str {
    match stage {
        Stage::Plan => "plan",
        Stage::Projection => "projection",
        Stage::Reducer => "reducer",
        Stage::Finalize => "finalize",
        Stage::ArtifactPlan => "artifact-plan",
        Stage::Artifact => "artifact",
        Stage::ArtifactFinalize => "artifact-finalize",
        Stage::ServingRoot => "serving-root",
        Stage::Reasoner => "reasoner",
    }
}

const fn state_for_stage(stage: Stage) -> JobState {
    match stage {
        Stage::Plan => JobState::Registered,
        Stage::Projection => JobState::Partitioned,
        Stage::Reducer => JobState::Projected,
        Stage::Finalize
        | Stage::ArtifactPlan
        | Stage::Artifact
        | Stage::ArtifactFinalize
        | Stage::ServingRoot
        | Stage::Reasoner => JobState::Indexed,
    }
}

fn work_spec_hash(
    compilation: &NgkgCompilation,
    config: &Config,
    stage: Stage,
    args: &[String],
    completions: i32,
    parallelism: i32,
) -> String {
    let resources = config.stage(stage);
    let service_account = if stage == Stage::Reasoner {
        &config.reference_service_account
    } else {
        &config.distributed_service_account
    };
    let automount_token = if stage == Stage::Reasoner {
        config.reference_automount_service_account_token
    } else {
        config.distributed_automount_service_account_token
    };
    let canonical = json!({
        "spec": &compilation.spec, "stage": stage_label(stage), "args": args,
        "distributedImage": &config.distributed_image, "referenceImage": &config.reference_image,
        "queue": &resources.queue, "responsibility": &resources.responsibility,
        "cpu": &resources.cpu, "memory": &resources.memory, "scratch": &resources.scratch,
        "serviceAccount": service_account, "automountServiceAccountToken": automount_token,
        "databaseSecret": &config.database_secret, "artifactBaseUrl": &config.artifact_base_url,
        "objectStoreCredentialsSecret": &config.object_store_credentials_secret,
        "javaToolOptions": &config.java_tool_options,
        "phase40DirectCeilings": (stage == Stage::Reasoner).then_some(&config.phase40_direct),
        "phase40DirectCeilingsSha256": (stage == Stage::Reasoner)
            .then(|| config.phase40_direct.bundle_sha256()),
        "activeDeadlineSeconds": resources.active_deadline_seconds,
        "ttlSecondsAfterFinished": config.ttl_seconds_after_finished,
        "completions": completions, "parallelism": parallelism,
    });
    hex::encode(Sha256::digest(canonical.to_string().as_bytes()))
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
            &Patch::Merge(json!({"status":status})),
        )
        .await?;
    Ok(())
}

fn error_policy(
    _object: Arc<NgkgCompilation>,
    error: &OperatorError,
    _context: Arc<Context>,
) -> Action {
    tracing::error!(%error, "distributed reconciliation failed closed");
    Action::requeue(Duration::from_secs(30))
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
fn positive_u64(name: &str) -> Result<u64> {
    required(name)?
        .parse::<u64>()
        .ok()
        .filter(|value| *value > 0)
        .with_context(|| format!("{name} must be a positive integer"))
}
fn positive_usize(name: &str) -> Result<usize> {
    required(name)?
        .parse::<usize>()
        .ok()
        .filter(|value| *value > 0)
        .with_context(|| format!("{name} must be a positive integer"))
}
fn required_bool(name: &str) -> Result<bool> {
    required(name)?
        .parse::<bool>()
        .with_context(|| format!("{name} must be true or false"))
}
fn required_digest_image(name: &str) -> Result<String> {
    let value = required(name)?;
    let Some((repository, digest)) = value.rsplit_once("@sha256:") else {
        anyhow::bail!("{name} must be repository@sha256:<64 lowercase hex>");
    };
    if repository.is_empty()
        || digest.len() != 64
        || !digest
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        anyhow::bail!("{name} must be repository@sha256:<64 lowercase hex>");
    }
    Ok(value)
}
