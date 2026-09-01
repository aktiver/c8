//! Restart-safe controller for distributed immutable snapshot recovery.

use std::{collections::BTreeMap, env, path::PathBuf, sync::Arc, time::Duration};

use anyhow::{Context as AnyhowContext, Result};
use futures::StreamExt;
use k8s_openapi::{
    api::{
        batch::v1::{Job, JobSpec},
        core::v1::{
            Capabilities, Container, EmptyDirVolumeSource, EnvFromSource, EnvVar, EnvVarSource,
            ObjectFieldSelector, PodSecurityContext, PodSpec, PodTemplateSpec,
            ResourceRequirements, SeccompProfile, SecretEnvSource, SecurityContext, Volume,
            VolumeMount,
        },
    },
    apimachinery::pkg::api::resource::Quantity,
};
use kube::{
    Api, Client, Resource, ResourceExt,
    api::{ObjectMeta, Patch, PatchParams, PostParams},
    runtime::{Controller, controller::Action, watcher},
};
use ngkg_artifact_store::ArtifactStore;
use ngkg_kube::{NgkgStorageRecovery, NgkgStorageRecoveryStatus, StorageRecoveryKind};
use ngkg_storage_recovery::{
    RecoveryCertificate, RecoveryPlan, SnapshotBackupManifest, StorageCatalogError,
    StorageRecoveryRepository, StorageTarget, TransferReason, build_restore_certificate,
    validate_backup_manifest, validate_recovery_plan,
};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use sqlx::postgres::PgPoolOptions;
use thiserror::Error;

#[derive(Clone)]
struct Context {
    client: Client,
    namespace: String,
    catalog: StorageRecoveryRepository,
    artifact_store: ArtifactStore,
    config: Config,
}

#[derive(Clone)]
struct Config {
    worker_image: String,
    service_account: String,
    queue_name: String,
    resource_profile: String,
    cpu: String,
    memory: String,
    scratch_size: String,
    active_deadline_seconds: i64,
    ttl_seconds_after_finished: i32,
    max_plan_bytes: u64,
    max_result_bytes: u64,
    max_task_bytes: u64,
    task_timeout_seconds: u64,
    single_put_max_bytes: u64,
    multipart_buffer_bytes: usize,
    multipart_concurrency: usize,
    node_saturation_target_percent: u8,
    target_registry_json: String,
    targets: Vec<StorageTarget>,
    object_store_credentials_secret: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct TargetRegistry {
    format_version: u32,
    targets: Vec<StorageTarget>,
}

#[derive(Debug, Error)]
enum OperatorError {
    #[error("Kubernetes operation failed: {0}")]
    Kubernetes(#[from] kube::Error),
    #[error("artifact verification failed: {0}")]
    Artifact(#[from] ngkg_artifact_store::ArtifactStoreError),
    #[error("catalog operation failed: {0}")]
    Catalog(#[from] StorageCatalogError),
    #[error("recovery resource is invalid: {0}")]
    Contract(String),
    #[error("local recovery verification failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("recovery evidence JSON is invalid: {0}")]
    Json(#[from] serde_json::Error),
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter("info")
        .init();
    let namespace = required("NGKG_NAMESPACE")?;
    let database_url = required("NGKG_DATABASE_URL")?;
    let artifact_base_url = required("NGKG_CONTROL_ARTIFACT_BASE_URL")?;
    let client = Client::try_default().await?;
    let pool = PgPoolOptions::new()
        .max_connections(16)
        .connect(&database_url)
        .await?;
    let context = Arc::new(Context {
        client: client.clone(),
        namespace: namespace.clone(),
        catalog: StorageRecoveryRepository::new(pool),
        artifact_store: ArtifactStore::from_base_url(&artifact_base_url)?,
        config: Config::from_env()?,
    });
    let resources: Api<NgkgStorageRecovery> = Api::namespaced(client.clone(), &namespace);
    let jobs: Api<Job> = Api::namespaced(client, &namespace);
    Controller::new(resources, watcher::Config::default())
        .owns(jobs, watcher::Config::default())
        .run(reconcile, error_policy, context)
        .for_each(|result| async move {
            if let Err(error) = result {
                tracing::error!(%error, "storage recovery reconciliation failed");
            }
        })
        .await;
    Ok(())
}

impl Config {
    fn from_env() -> Result<Self> {
        let target_registry_json = required("NGKG_STORAGE_TARGETS_JSON")?;
        let registry: TargetRegistry = serde_json::from_str(&target_registry_json)
            .context("NGKG_STORAGE_TARGETS_JSON is invalid")?;
        if registry.format_version != 1 || registry.targets.len() < 2 {
            anyhow::bail!("at least two registered storage targets are required");
        }
        Ok(Self {
            worker_image: required_digest_image("NGKG_STORAGE_RECOVERY_WORKER_IMAGE")?,
            service_account: required("NGKG_STORAGE_RECOVERY_SERVICE_ACCOUNT")?,
            queue_name: required("NGKG_STORAGE_RECOVERY_QUEUE")?,
            resource_profile: required("NGKG_STORAGE_RECOVERY_RESOURCE_PROFILE")?,
            cpu: required("NGKG_STORAGE_RECOVERY_CPU")?,
            memory: required("NGKG_STORAGE_RECOVERY_MEMORY")?,
            scratch_size: required("NGKG_STORAGE_RECOVERY_SCRATCH_SIZE")?,
            active_deadline_seconds: positive_i64("NGKG_STORAGE_RECOVERY_ACTIVE_DEADLINE_SECONDS")?,
            ttl_seconds_after_finished: positive_i32(
                "NGKG_STORAGE_RECOVERY_TTL_SECONDS_AFTER_FINISHED",
            )?,
            max_plan_bytes: positive_u64("NGKG_STORAGE_RECOVERY_MAX_PLAN_BYTES")?,
            max_result_bytes: positive_u64("NGKG_STORAGE_RECOVERY_MAX_RESULT_BYTES")?,
            max_task_bytes: positive_u64("NGKG_STORAGE_RECOVERY_MAX_TASK_BYTES")?,
            task_timeout_seconds: positive_u64("NGKG_STORAGE_RECOVERY_TASK_TIMEOUT_SECONDS")?,
            single_put_max_bytes: positive_u64("NGKG_SINGLE_PUT_MAX_BYTES")?,
            multipart_buffer_bytes: positive_usize("NGKG_MULTIPART_BUFFER_BYTES")?,
            multipart_concurrency: positive_usize("NGKG_MULTIPART_CONCURRENCY")?,
            node_saturation_target_percent: production_saturation_target(
                "NGKG_NODE_SATURATION_TARGET_PERCENT",
            )?,
            target_registry_json,
            targets: registry.targets,
            object_store_credentials_secret: optional("NGKG_OBJECT_STORE_CREDENTIALS_SECRET"),
        })
    }
}

async fn reconcile(
    recovery: Arc<NgkgStorageRecovery>,
    context: Arc<Context>,
) -> Result<Action, OperatorError> {
    validate_resource(&recovery, &context.config)?;
    if recovery
        .status
        .as_ref()
        .and_then(|status| status.recovery_certificate_sha256.as_ref())
        .is_some()
    {
        return Ok(Action::await_change());
    }
    let jobs: Api<Job> = Api::namespaced(context.client.clone(), &context.namespace);
    let base = format!("ngkg-recovery-{}", recovery.spec.operation_id.simple());
    let transfer_name = format!("{base}-copy");
    if recovery.spec.task_count > 0 {
        ensure_job(&jobs, transfer_job(&recovery, &context, &transfer_name)?).await?;
        match context
            .catalog
            .transition(
                recovery.spec.tenant_id,
                recovery.spec.operation_id,
                "PLANNED",
                "RUNNING",
            )
            .await
        {
            Ok(()) | Err(StorageCatalogError::IdempotencyConflict) => {}
            Err(error) => return Err(error.into()),
        }
        let transfer = jobs.get(&transfer_name).await?;
        if condition_true(&transfer, "Failed") {
            context
                .catalog
                .fail(
                    recovery.spec.tenant_id,
                    recovery.spec.operation_id,
                    "TRANSFER_JOB_FAILED",
                )
                .await?;
            patch_status(
                &recovery,
                &context,
                NgkgStorageRecoveryStatus {
                    observed_generation: recovery.metadata.generation,
                    transfer_job_name: Some(transfer_name),
                    condition: Some("TransferFailedClosed".to_owned()),
                    ..recovery.status.clone().unwrap_or_default()
                },
            )
            .await?;
            return Ok(Action::await_change());
        }
        if !condition_true(&transfer, "Complete") {
            patch_status(
                &recovery,
                &context,
                NgkgStorageRecoveryStatus {
                    observed_generation: recovery.metadata.generation,
                    transfer_job_name: Some(transfer_name),
                    condition: Some("DistributedTransferRunning".to_owned()),
                    ..recovery.status.clone().unwrap_or_default()
                },
            )
            .await?;
            return Ok(Action::requeue(Duration::from_secs(10)));
        }
    }
    transition_to_verifying(&context.catalog, &recovery).await?;
    let finalize_name = format!("{base}-verify");
    ensure_job(&jobs, finalize_job(&recovery, &context, &finalize_name)?).await?;
    let finalize = jobs.get(&finalize_name).await?;
    if condition_true(&finalize, "Failed") {
        context
            .catalog
            .fail(
                recovery.spec.tenant_id,
                recovery.spec.operation_id,
                "VERIFICATION_BARRIER_FAILED",
            )
            .await?;
        patch_status(
            &recovery,
            &context,
            NgkgStorageRecoveryStatus {
                observed_generation: recovery.metadata.generation,
                transfer_job_name: (recovery.spec.task_count > 0).then_some(transfer_name),
                finalize_job_name: Some(finalize_name),
                condition: Some("VerificationBarrierFailedClosed".to_owned()),
                ..recovery.status.clone().unwrap_or_default()
            },
        )
        .await?;
        return Ok(Action::await_change());
    }
    if !condition_true(&finalize, "Complete") {
        patch_status(
            &recovery,
            &context,
            NgkgStorageRecoveryStatus {
                observed_generation: recovery.metadata.generation,
                transfer_job_name: (recovery.spec.task_count > 0).then_some(transfer_name),
                finalize_job_name: Some(finalize_name),
                condition: Some("VerificationBarrierRunning".to_owned()),
                ..recovery.status.clone().unwrap_or_default()
            },
        )
        .await?;
        return Ok(Action::requeue(Duration::from_secs(10)));
    }
    commit_verified(&recovery, &context, &transfer_name, &finalize_name).await?;
    Ok(Action::await_change())
}

async fn transition_to_verifying(
    catalog: &StorageRecoveryRepository,
    recovery: &NgkgStorageRecovery,
) -> Result<(), OperatorError> {
    let expected = if recovery.spec.task_count == 0 {
        "PLANNED"
    } else {
        "RUNNING"
    };
    match catalog
        .transition(
            recovery.spec.tenant_id,
            recovery.spec.operation_id,
            expected,
            "VERIFYING",
        )
        .await
    {
        Ok(()) | Err(StorageCatalogError::IdempotencyConflict) => Ok(()),
        Err(error) => Err(error.into()),
    }
}

async fn commit_verified(
    recovery: &NgkgStorageRecovery,
    context: &Context,
    transfer_name: &str,
    finalize_name: &str,
) -> Result<(), OperatorError> {
    let scratch = PathBuf::from("/tmp/ngkg-storage-recovery-operator")
        .join(recovery.spec.operation_id.simple().to_string());
    tokio::fs::create_dir_all(&scratch).await?;
    let plan_path = scratch.join("plan.json");
    remove_if_present(&plan_path).await?;
    context
        .artifact_store
        .materialize_verified(
            &recovery.spec.plan_object_key,
            &recovery.spec.plan_sha256,
            context.config.max_plan_bytes,
            &plan_path,
        )
        .await?;
    let plan: RecoveryPlan = serde_json::from_slice(&tokio::fs::read(&plan_path).await?)?;
    validate_recovery_plan(&plan).map_err(|error| OperatorError::Contract(error.to_string()))?;
    if plan.operation_id != recovery.spec.operation_id
        || plan.tenant_id != recovery.spec.tenant_id
        || plan.dataset_id != recovery.spec.dataset_id
        || plan.snapshot_id != recovery.spec.source_snapshot_id
        || plan.max_in_flight_bytes != recovery.spec.max_in_flight_bytes
        || u32::try_from(plan.tasks.len()).ok() != Some(recovery.spec.task_count)
        || !plan_reason_matches(recovery.spec.kind, &plan)
    {
        return Err(OperatorError::Contract(
            "recovery plan identity differs from its Kubernetes resource".to_owned(),
        ));
    }
    let certificate_key = format!(
        "storage-recovery/{}/recovery-certificate.json",
        recovery.spec.operation_id.simple()
    );
    let certificate_path = scratch.join("certificate.json");
    remove_if_present(&certificate_path).await?;
    context
        .artifact_store
        .materialize_unverified_bounded(
            &certificate_key,
            context.config.max_result_bytes,
            &certificate_path,
        )
        .await?;
    let certificate_bytes = tokio::fs::read(&certificate_path).await?;
    let certificate_sha256 = Sha256::digest(&certificate_bytes);
    let certificate_digest: [u8; 32] = certificate_sha256.into();
    let certificate: RecoveryCertificate = serde_json::from_slice(&certificate_bytes)?;
    if !certificate.complete
        || certificate.operation_id != recovery.spec.operation_id
        || certificate.snapshot_id != recovery.spec.source_snapshot_id
        || certificate.plan_sha256 != recovery.spec.plan_sha256
        || certificate.verified_task_count != recovery.spec.task_count
    {
        return Err(OperatorError::Contract(
            "recovery certificate is not bound to the complete CR plan".to_owned(),
        ));
    }
    let backup = if recovery.spec.kind == StorageRecoveryKind::Backup {
        let key = format!(
            "storage-recovery/{}/backup-manifest.json",
            recovery.spec.operation_id.simple()
        );
        let path = scratch.join("backup.json");
        remove_if_present(&path).await?;
        context
            .artifact_store
            .materialize_unverified_bounded(&key, context.config.max_plan_bytes, &path)
            .await?;
        let bytes = tokio::fs::read(path).await?;
        let manifest: SnapshotBackupManifest = serde_json::from_slice(&bytes)?;
        validate_backup_manifest(&manifest)
            .map_err(|error| OperatorError::Contract(error.to_string()))?;
        if manifest.backup_id != recovery.spec.operation_id
            || manifest.tenant_id != recovery.spec.tenant_id
            || manifest.dataset_id != recovery.spec.dataset_id
            || manifest.source_snapshot_id != recovery.spec.source_snapshot_id
            || manifest.source_storage_manifest_sha256 != plan.storage_manifest_sha256
            || manifest.recovery_certificate_sha256 != hex::encode(certificate_digest)
            || !manifest.complete
        {
            return Err(OperatorError::Contract(
                "backup manifest is not bound to the certified recovery".to_owned(),
            ));
        }
        let digest: [u8; 32] = Sha256::digest(&bytes).into();
        Some((key, digest))
    } else {
        None
    };
    let restore = if recovery.spec.kind == StorageRecoveryKind::Restore {
        let restored_snapshot_id = recovery.spec.restored_snapshot_id.ok_or_else(|| {
            OperatorError::Contract("restore lacks restored snapshot identity".to_owned())
        })?;
        let certificate = build_restore_certificate(
            &plan,
            restored_snapshot_id,
            &plan.storage_manifest_sha256,
            &certificate,
            &hex::encode(certificate_digest),
        )
        .map_err(|error| OperatorError::Contract(error.to_string()))?;
        let bytes = serde_json::to_vec(&certificate)?;
        let digest: [u8; 32] = Sha256::digest(&bytes).into();
        let path = scratch.join("restore-certificate.json");
        tokio::fs::write(&path, bytes).await?;
        let key = format!(
            "storage-recovery/{}/restore-certificate.json",
            recovery.spec.operation_id.simple()
        );
        context
            .artifact_store
            .put_file_immutable(
                &key,
                &hex::encode(digest),
                &path,
                context.config.single_put_max_bytes,
                context.config.multipart_buffer_bytes,
                context.config.multipart_concurrency,
            )
            .await?;
        Some((key, digest))
    } else {
        None
    };
    context
        .catalog
        .commit_success(
            recovery.spec.tenant_id,
            recovery.spec.operation_id,
            &certificate_key,
            &certificate_digest,
            &plan,
            &context.config.targets,
            backup.as_ref().map(|(key, digest)| (key.as_str(), digest)),
        )
        .await?;
    patch_status(
        recovery,
        context,
        NgkgStorageRecoveryStatus {
            observed_generation: recovery.metadata.generation,
            transfer_job_name: (recovery.spec.task_count > 0).then(|| transfer_name.to_owned()),
            finalize_job_name: Some(finalize_name.to_owned()),
            recovery_certificate_object_key: Some(certificate_key),
            recovery_certificate_sha256: Some(hex::encode(certificate_digest)),
            backup_manifest_object_key: backup.as_ref().map(|value| value.0.clone()),
            backup_manifest_sha256: backup.as_ref().map(|value| hex::encode(value.1)),
            restore_certificate_object_key: restore.as_ref().map(|value| value.0.clone()),
            restore_certificate_sha256: restore.as_ref().map(|value| hex::encode(value.1)),
            quarantined_replica_count: 0,
            condition: Some("RecoveryCertifiedComplete".to_owned()),
        },
    )
    .await?;
    Ok(())
}

fn plan_reason_matches(kind: StorageRecoveryKind, plan: &RecoveryPlan) -> bool {
    let expected = match kind {
        StorageRecoveryKind::Replicate => TransferReason::Replication,
        StorageRecoveryKind::Relocate => TransferReason::Relocation,
        StorageRecoveryKind::NodeLoss => TransferReason::NodeLoss,
        StorageRecoveryKind::Backup => TransferReason::Backup,
        StorageRecoveryKind::Restore => TransferReason::Restore,
    };
    plan.tasks.iter().all(|task| task.reason == expected)
}

fn transfer_job(
    recovery: &NgkgStorageRecovery,
    context: &Context,
    name: &str,
) -> Result<Job, OperatorError> {
    let parallelism = recovery.spec.task_count.min(recovery.spec.max_parallelism);
    job(
        recovery,
        context,
        name,
        "transfer",
        Some(recovery.spec.task_count),
        Some(parallelism),
    )
}

fn finalize_job(
    recovery: &NgkgStorageRecovery,
    context: &Context,
    name: &str,
) -> Result<Job, OperatorError> {
    job(recovery, context, name, "finalize", None, None)
}

fn job(
    recovery: &NgkgStorageRecovery,
    context: &Context,
    name: &str,
    mode: &str,
    completions: Option<u32>,
    parallelism: Option<u32>,
) -> Result<Job, OperatorError> {
    let config = &context.config;
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
    let mut env = [
        ("NGKG_RECOVERY_MODE", mode.to_owned()),
        (
            "NGKG_CONTROL_ARTIFACT_BASE_URL",
            context.artifact_store.base_url().to_string(),
        ),
        (
            "NGKG_RECOVERY_PLAN_OBJECT_KEY",
            recovery.spec.plan_object_key.clone(),
        ),
        (
            "NGKG_RECOVERY_PLAN_SHA256",
            recovery.spec.plan_sha256.clone(),
        ),
        ("NGKG_RECOVERY_SCRATCH_ROOT", "/scratch/recovery".to_owned()),
        (
            "NGKG_RECOVERY_MAX_PLAN_BYTES",
            config.max_plan_bytes.to_string(),
        ),
        (
            "NGKG_RECOVERY_MAX_RESULT_BYTES",
            config.max_result_bytes.to_string(),
        ),
        (
            "NGKG_RECOVERY_MAX_TASK_BYTES",
            config.max_task_bytes.to_string(),
        ),
        (
            "NGKG_RECOVERY_TASK_TIMEOUT_SECONDS",
            config.task_timeout_seconds.to_string(),
        ),
        (
            "NGKG_SINGLE_PUT_MAX_BYTES",
            config.single_put_max_bytes.to_string(),
        ),
        (
            "NGKG_MULTIPART_BUFFER_BYTES",
            config.multipart_buffer_bytes.to_string(),
        ),
        (
            "NGKG_MULTIPART_CONCURRENCY",
            config.multipart_concurrency.to_string(),
        ),
        (
            "NGKG_NODE_SATURATION_TARGET_PERCENT",
            config.node_saturation_target_percent.to_string(),
        ),
        (
            "NGKG_STORAGE_TARGETS_JSON",
            config.target_registry_json.clone(),
        ),
        ("RAYON_NUM_THREADS", config.cpu.clone()),
        ("OMP_NUM_THREADS", "1".to_owned()),
        ("OPENBLAS_NUM_THREADS", "1".to_owned()),
        ("MKL_NUM_THREADS", "1".to_owned()),
    ]
    .into_iter()
    .map(|(name, value)| EnvVar {
        name: name.to_owned(),
        value: Some(value),
        ..EnvVar::default()
    })
    .collect::<Vec<_>>();
    env.push(EnvVar {
        name: "NGKG_RECOVERY_ATTEMPT_ID".to_owned(),
        value_from: Some(EnvVarSource {
            field_ref: Some(ObjectFieldSelector {
                api_version: Some("v1".to_owned()),
                field_path: "metadata.uid".to_owned(),
            }),
            ..EnvVarSource::default()
        }),
        ..EnvVar::default()
    });
    if completions.is_some() {
        env.push(EnvVar {
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
    let resources = ResourceRequirements {
        requests: Some(BTreeMap::from([
            ("cpu".to_owned(), Quantity(config.cpu.clone())),
            ("memory".to_owned(), Quantity(config.memory.clone())),
            (
                "ephemeral-storage".to_owned(),
                Quantity(config.scratch_size.clone()),
            ),
        ])),
        limits: Some(BTreeMap::from([
            ("cpu".to_owned(), Quantity(config.cpu.clone())),
            ("memory".to_owned(), Quantity(config.memory.clone())),
            (
                "ephemeral-storage".to_owned(),
                Quantity(config.scratch_size.clone()),
            ),
        ])),
        ..ResourceRequirements::default()
    };
    let pod = PodTemplateSpec {
        metadata: Some(ObjectMeta {
            labels: Some(BTreeMap::from([
                ("app.kubernetes.io/name".to_owned(), "ngkg".to_owned()),
                (
                    "app.kubernetes.io/component".to_owned(),
                    "storage-recovery-worker".to_owned(),
                ),
                (
                    "ngkg.io/operation-id".to_owned(),
                    recovery.spec.operation_id.to_string(),
                ),
                ("ngkg.io/network-plane".to_owned(), "batch".to_owned()),
            ])),
            annotations: Some(BTreeMap::from([(
                "kueue.x-k8s.io/queue-name".to_owned(),
                config.queue_name.clone(),
            )])),
            ..ObjectMeta::default()
        }),
        spec: Some(PodSpec {
            service_account_name: Some(config.service_account.clone()),
            automount_service_account_token: Some(false),
            restart_policy: Some("Never".to_owned()),
            node_selector: Some(BTreeMap::from([(
                "ngkg.io/workload".to_owned(),
                "storage-recovery".to_owned(),
            )])),
            security_context: Some(PodSecurityContext {
                run_as_non_root: Some(true),
                seccomp_profile: Some(SeccompProfile {
                    type_: "RuntimeDefault".to_owned(),
                    localhost_profile: None,
                }),
                ..PodSecurityContext::default()
            }),
            containers: vec![Container {
                name: "recovery".to_owned(),
                image: Some(config.worker_image.clone()),
                env: Some(env),
                env_from: (!env_from.is_empty()).then_some(env_from),
                resources: Some(resources),
                volume_mounts: Some(vec![VolumeMount {
                    name: "scratch".to_owned(),
                    mount_path: "/scratch".to_owned(),
                    ..VolumeMount::default()
                }]),
                security_context: Some(SecurityContext {
                    allow_privilege_escalation: Some(false),
                    read_only_root_filesystem: Some(true),
                    run_as_non_root: Some(true),
                    capabilities: Some(Capabilities {
                        drop: Some(vec!["ALL".to_owned()]),
                        ..Capabilities::default()
                    }),
                    ..SecurityContext::default()
                }),
                ..Container::default()
            }],
            volumes: Some(vec![Volume {
                name: "scratch".to_owned(),
                empty_dir: Some(EmptyDirVolumeSource {
                    size_limit: Some(Quantity(config.scratch_size.clone())),
                    ..EmptyDirVolumeSource::default()
                }),
                ..Volume::default()
            }]),
            ..PodSpec::default()
        }),
    };
    Ok(Job {
        metadata: ObjectMeta {
            name: Some(name.to_owned()),
            namespace: Some(context.namespace.clone()),
            owner_references: recovery
                .controller_owner_ref(&())
                .map(|reference| vec![reference]),
            annotations: Some(BTreeMap::from([(
                "kueue.x-k8s.io/queue-name".to_owned(),
                config.queue_name.clone(),
            )])),
            labels: Some(BTreeMap::from([(
                "app.kubernetes.io/component".to_owned(),
                "storage-recovery".to_owned(),
            )])),
            ..ObjectMeta::default()
        },
        spec: Some(JobSpec {
            template: pod,
            completions: completions
                .map(i32::try_from)
                .transpose()
                .map_err(|_| OperatorError::Contract("task count exceeds i32".to_owned()))?,
            parallelism: parallelism
                .map(i32::try_from)
                .transpose()
                .map_err(|_| OperatorError::Contract("parallelism exceeds i32".to_owned()))?,
            completion_mode: completions.map(|_| "Indexed".to_owned()),
            backoff_limit: completions.is_none().then_some(6),
            backoff_limit_per_index: completions.is_some().then_some(6),
            max_failed_indexes: completions.is_some().then_some(0),
            active_deadline_seconds: Some(config.active_deadline_seconds),
            ttl_seconds_after_finished: Some(config.ttl_seconds_after_finished),
            ..JobSpec::default()
        }),
        status: None,
    })
}

async fn ensure_job(api: &Api<Job>, job: Job) -> Result<(), kube::Error> {
    let name = job.name_any();
    if api.get_opt(&name).await?.is_none() {
        match api.create(&PostParams::default(), &job).await {
            Ok(_) => {}
            Err(kube::Error::Api(response)) if response.code == 409 => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

async fn patch_status(
    recovery: &NgkgStorageRecovery,
    context: &Context,
    status: NgkgStorageRecoveryStatus,
) -> Result<(), kube::Error> {
    let api: Api<NgkgStorageRecovery> = Api::namespaced(context.client.clone(), &context.namespace);
    api.patch_status(
        &recovery.name_any(),
        &PatchParams::default(),
        &Patch::Merge(serde_json::json!({"status": status})),
    )
    .await?;
    Ok(())
}

fn validate_resource(recovery: &NgkgStorageRecovery, config: &Config) -> Result<(), OperatorError> {
    let spec = &recovery.spec;
    if spec.tenant_id.is_nil()
        || spec.dataset_id.is_nil()
        || spec.operation_id.is_nil()
        || spec.source_snapshot_id.is_nil()
        || spec.plan_sha256.len() != 64
        || !spec
            .plan_sha256
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
        || spec.task_count > 10_000_000
        || spec.max_parallelism == 0
        || spec.max_parallelism > 4096
        || spec.largest_task_bytes > config.max_task_bytes
        || spec.max_in_flight_bytes == 0
        || spec.resource_profile != config.resource_profile
        || (spec.kind == StorageRecoveryKind::Restore) != spec.restored_snapshot_id.is_some()
    {
        return Err(OperatorError::Contract(
            "spec violates the recovery ceiling bundle".to_owned(),
        ));
    }
    let admitted_bytes = u64::from(spec.max_parallelism)
        .checked_mul(spec.largest_task_bytes)
        .ok_or_else(|| {
            OperatorError::Contract("aggregate transfer byte budget overflows".to_owned())
        })?;
    if admitted_bytes > spec.max_in_flight_bytes {
        return Err(OperatorError::Contract(
            "parallel storage work exceeds maxInFlightBytes".to_owned(),
        ));
    }
    Ok(())
}

fn condition_true(job: &Job, type_: &str) -> bool {
    job.status
        .as_ref()
        .and_then(|status| status.conditions.as_ref())
        .is_some_and(|conditions| {
            conditions
                .iter()
                .any(|condition| condition.type_ == type_ && condition.status == "True")
        })
}

async fn remove_if_present(path: &std::path::Path) -> Result<(), std::io::Error> {
    match tokio::fs::remove_file(path).await {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

fn error_policy(
    _recovery: Arc<NgkgStorageRecovery>,
    error: &OperatorError,
    _context: Arc<Context>,
) -> Action {
    tracing::error!(%error, "storage recovery reconcile error");
    Action::requeue(Duration::from_secs(30))
}

fn required(name: &'static str) -> Result<String> {
    env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .with_context(|| format!("{name} is required"))
}

fn optional(name: &'static str) -> Option<String> {
    env::var(name).ok().filter(|value| !value.trim().is_empty())
}

fn required_digest_image(name: &'static str) -> Result<String> {
    let value = required(name)?;
    if !value.contains("@sha256:") || value.ends_with("@sha256:") {
        anyhow::bail!("{name} must be pinned by sha256 digest");
    }
    Ok(value)
}

fn positive_u64(name: &'static str) -> Result<u64> {
    required(name)?
        .parse::<u64>()
        .ok()
        .filter(|value| *value > 0)
        .with_context(|| format!("{name} must be positive"))
}

fn positive_usize(name: &'static str) -> Result<usize> {
    required(name)?
        .parse::<usize>()
        .ok()
        .filter(|value| *value > 0)
        .with_context(|| format!("{name} must be positive"))
}

fn positive_i64(name: &'static str) -> Result<i64> {
    required(name)?
        .parse::<i64>()
        .ok()
        .filter(|value| *value > 0)
        .with_context(|| format!("{name} must be positive"))
}

fn positive_i32(name: &'static str) -> Result<i32> {
    required(name)?
        .parse::<i32>()
        .ok()
        .filter(|value| *value > 0)
        .with_context(|| format!("{name} must be positive"))
}

fn production_saturation_target(name: &'static str) -> Result<u8> {
    let value = required(name)?
        .parse::<u8>()
        .with_context(|| format!("{name} must be an integer"))?;
    if value != 80 {
        anyhow::bail!("{name} must equal the production 80-percent headroom target");
    }
    Ok(value)
}
