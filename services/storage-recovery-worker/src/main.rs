//! One bounded Kubernetes Indexed Job completion for snapshot replication.

use std::{collections::BTreeMap, env, path::PathBuf, time::Duration};

use anyhow::{Context, Result};
use ngkg_artifact_store::{ArtifactStore, ArtifactStoreError};
use ngkg_hpc_runtime::{resource_envelope_report, validate_buffer_budget};
use ngkg_storage_recovery::{
    RecoveryCertificationAccumulator, RecoveryError, RecoveryPlan, StorageTarget, TransferReason,
    TransferResult, TransferState, build_backup_manifest, execute_transfer, validate_recovery_plan,
};
use serde::Deserialize;
use sha2::{Digest, Sha256};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct TargetRegistry {
    format_version: u32,
    targets: Vec<StorageTarget>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter("info")
        .init();
    run().await
}

async fn run() -> Result<()> {
    match required("NGKG_RECOVERY_MODE")?.as_str() {
        "transfer" => run_transfer().await,
        "finalize" => run_finalize().await,
        value => anyhow::bail!("unsupported NGKG_RECOVERY_MODE {value}"),
    }
}

async fn run_transfer() -> Result<()> {
    let control_store = ArtifactStore::from_base_url(&required("NGKG_CONTROL_ARTIFACT_BASE_URL")?)?;
    let plan_key = required("NGKG_RECOVERY_PLAN_OBJECT_KEY")?;
    let plan_sha256 = required_sha256("NGKG_RECOVERY_PLAN_SHA256")?;
    let scratch_root = PathBuf::from(required("NGKG_RECOVERY_SCRATCH_ROOT")?);
    let task_index = required_u32("JOB_COMPLETION_INDEX")?;
    let max_plan_bytes = required_u64("NGKG_RECOVERY_MAX_PLAN_BYTES")?;
    let max_task_bytes = required_u64("NGKG_RECOVERY_MAX_TASK_BYTES")?;
    let timeout_seconds = required_u64("NGKG_RECOVERY_TASK_TIMEOUT_SECONDS")?;
    let single_put_max_bytes = required_u64("NGKG_SINGLE_PUT_MAX_BYTES")?;
    let multipart_buffer_bytes = required_usize("NGKG_MULTIPART_BUFFER_BYTES")?;
    let multipart_concurrency = required_usize("NGKG_MULTIPART_CONCURRENCY")?;
    validate_runtime_envelope(multipart_buffer_bytes, multipart_concurrency)?;
    let registry = load_registry()?;

    tokio::fs::create_dir_all(&scratch_root).await?;
    let plan_path = scratch_root.join("recovery-plan.json");
    control_store
        .materialize_verified(&plan_key, &plan_sha256, max_plan_bytes, &plan_path)
        .await
        .context("recovery plan verification failed")?;
    let plan: RecoveryPlan = serde_json::from_slice(&tokio::fs::read(&plan_path).await?)
        .context("recovery plan JSON is invalid")?;
    validate_recovery_plan(&plan).context("recovery plan contract is invalid")?;
    let task = plan
        .tasks
        .get(usize::try_from(task_index)?)
        .filter(|task| task.task_index == task_index)
        .cloned()
        .context("JOB_COMPLETION_INDEX is not a dense plan task")?;
    if task.bytes > max_task_bytes || task.bytes > plan.max_in_flight_bytes {
        anyhow::bail!("task byte ceiling exceeds worker or plan budget");
    }
    let source = target_store(&registry, &task.source_target)?;
    let destination = target_store(&registry, &task.destination_target)?;
    let execution = tokio::time::timeout(
        Duration::from_secs(timeout_seconds),
        execute_transfer(
            plan.operation_id,
            &task,
            &source,
            &destination,
            &scratch_root,
            single_put_max_bytes,
            multipart_buffer_bytes,
            multipart_concurrency,
        ),
    )
    .await;
    let result = match execution {
        Ok(Ok(result)) => result,
        Ok(Err(error)) => failure_result(&plan, task_index, &task.stable_work_id, &error),
        Err(_) => TransferResult {
            operation_id: plan.operation_id,
            task_index,
            stable_work_id: task.stable_work_id.clone(),
            state: TransferState::RetryableFailure,
            observed_sha256: None,
            copied_bytes: 0,
            error_code: Some("TASK_TIMEOUT".to_owned()),
        },
    };
    let result_bytes = serde_json::to_vec(&result)?;
    let result_sha256 = hex::encode(Sha256::digest(&result_bytes));
    let result_path = scratch_root.join(format!("result-{task_index}.json"));
    tokio::fs::write(&result_path, &result_bytes).await?;
    let result_key = if result.state == TransferState::Succeeded {
        format!(
            "storage-recovery/{}/results/{task_index:010}.json",
            plan.operation_id.simple()
        )
    } else {
        let attempt_id = required("NGKG_RECOVERY_ATTEMPT_ID")?;
        if attempt_id.len() > 128
            || !attempt_id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        {
            anyhow::bail!("NGKG_RECOVERY_ATTEMPT_ID is invalid");
        }
        format!(
            "storage-recovery/{}/attempts/{task_index:010}/{attempt_id}.json",
            plan.operation_id.simple()
        )
    };
    control_store
        .put_file_immutable(
            &result_key,
            &result_sha256,
            &result_path,
            single_put_max_bytes,
            multipart_buffer_bytes,
            multipart_concurrency,
        )
        .await
        .context("partition result publication failed")?;
    tracing::info!(
        operation_id = %plan.operation_id,
        task_index,
        state = ?result.state,
        result_key,
        result_sha256,
        "storage recovery partition completed"
    );
    if result.state != TransferState::Succeeded {
        anyhow::bail!(
            "storage recovery partition failed closed: {:?}",
            result.state
        );
    }
    Ok(())
}

async fn run_finalize() -> Result<()> {
    let control_store = ArtifactStore::from_base_url(&required("NGKG_CONTROL_ARTIFACT_BASE_URL")?)?;
    let plan_key = required("NGKG_RECOVERY_PLAN_OBJECT_KEY")?;
    let plan_sha256 = required_sha256("NGKG_RECOVERY_PLAN_SHA256")?;
    let scratch_root = PathBuf::from(required("NGKG_RECOVERY_SCRATCH_ROOT")?);
    let max_plan_bytes = required_u64("NGKG_RECOVERY_MAX_PLAN_BYTES")?;
    let max_result_bytes = required_u64("NGKG_RECOVERY_MAX_RESULT_BYTES")?;
    let single_put_max_bytes = required_u64("NGKG_SINGLE_PUT_MAX_BYTES")?;
    let multipart_buffer_bytes = required_usize("NGKG_MULTIPART_BUFFER_BYTES")?;
    let multipart_concurrency = required_usize("NGKG_MULTIPART_CONCURRENCY")?;
    validate_runtime_envelope(multipart_buffer_bytes, multipart_concurrency)?;
    tokio::fs::create_dir_all(&scratch_root).await?;
    let plan_path = scratch_root.join("recovery-plan.json");
    control_store
        .materialize_verified(&plan_key, &plan_sha256, max_plan_bytes, &plan_path)
        .await?;
    let plan: RecoveryPlan = serde_json::from_slice(&tokio::fs::read(&plan_path).await?)?;
    validate_recovery_plan(&plan)?;
    let mut accumulator = RecoveryCertificationAccumulator::new(&plan, &plan_sha256)?;
    for task in &plan.tasks {
        let path = scratch_root.join(format!("result-{:010}.json", task.task_index));
        let key = format!(
            "storage-recovery/{}/results/{:010}.json",
            plan.operation_id.simple(),
            task.task_index
        );
        control_store
            .materialize_unverified_bounded(&key, max_result_bytes, &path)
            .await
            .with_context(|| format!("missing recovery partition {}", task.task_index))?;
        let bytes = tokio::fs::read(&path).await?;
        let result = serde_json::from_slice::<TransferResult>(&bytes)?;
        accumulator.observe(task, &result)?;
    }
    let certificate = accumulator.finish()?;
    let certificate_bytes = serde_json::to_vec(&certificate)?;
    let certificate_sha256 = hex::encode(Sha256::digest(&certificate_bytes));
    let certificate_path = scratch_root.join("recovery-certificate.json");
    tokio::fs::write(&certificate_path, &certificate_bytes).await?;
    let certificate_key = format!(
        "storage-recovery/{}/recovery-certificate.json",
        plan.operation_id.simple()
    );
    control_store
        .put_file_immutable(
            &certificate_key,
            &certificate_sha256,
            &certificate_path,
            single_put_max_bytes,
            multipart_buffer_bytes,
            multipart_concurrency,
        )
        .await?;
    let backup = if plan
        .tasks
        .iter()
        .all(|task| task.reason == TransferReason::Backup)
        && !plan.tasks.is_empty()
    {
        let manifest = build_backup_manifest(&plan, &certificate, &certificate_sha256)?;
        let bytes = serde_json::to_vec(&manifest)?;
        let sha256 = hex::encode(Sha256::digest(&bytes));
        let path = scratch_root.join("backup-manifest.json");
        tokio::fs::write(&path, bytes).await?;
        let key = format!(
            "storage-recovery/{}/backup-manifest.json",
            plan.operation_id.simple()
        );
        control_store
            .put_file_immutable(
                &key,
                &sha256,
                &path,
                single_put_max_bytes,
                multipart_buffer_bytes,
                multipart_concurrency,
            )
            .await?;
        Some((key, sha256))
    } else {
        None
    };
    tracing::info!(
        operation_id = %plan.operation_id,
        certificate_key,
        certificate_sha256,
        task_count = plan.tasks.len(),
        result_digest_count = plan.tasks.len(),
        backup_manifest_key = backup.as_ref().map(|value| value.0.as_str()),
        backup_manifest_sha256 = backup.as_ref().map(|value| value.1.as_str()),
        "all storage recovery partitions certified"
    );
    Ok(())
}

fn load_registry() -> Result<TargetRegistry> {
    let value = required("NGKG_STORAGE_TARGETS_JSON")?;
    let registry: TargetRegistry =
        serde_json::from_str(&value).context("NGKG_STORAGE_TARGETS_JSON is invalid")?;
    if registry.format_version != 1 || registry.targets.is_empty() {
        anyhow::bail!("storage target registry is empty or version-incompatible");
    }
    let names = registry
        .targets
        .iter()
        .map(|target| target.name.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    if names.len() != registry.targets.len() {
        anyhow::bail!("storage target names are not unique");
    }
    Ok(registry)
}

fn target_store(registry: &TargetRegistry, name: &str) -> Result<ArtifactStore> {
    let targets = registry
        .targets
        .iter()
        .map(|target| (target.name.as_str(), target))
        .collect::<BTreeMap<_, _>>();
    let target = targets
        .get(name)
        .context("plan references an unregistered storage target")?;
    ArtifactStore::from_base_url(&target.base_url).map_err(Into::into)
}

fn validate_runtime_envelope(buffer_bytes: usize, concurrency: usize) -> Result<()> {
    let envelope = resource_envelope_report()
        .context("storage recovery requires a finite cgroup CPU/memory envelope")?;
    let admitted = validate_buffer_budget(buffer_bytes, concurrency, &envelope)
        .context("multipart buffers exceed the 80-percent cgroup memory envelope")?;
    tracing::info!(
        cpuset_cores = envelope.cpuset_cores,
        memory_limit_bytes = envelope.memory_limit_bytes,
        memory_current_bytes = envelope.memory_current_bytes,
        usable_memory_bytes = envelope.usable_memory_bytes,
        memory_headroom_bytes = envelope.memory_headroom_bytes,
        saturation_target_percent = envelope.saturation_target_percent,
        admitted_multipart_bytes = admitted,
        "storage recovery cgroup envelope admitted"
    );
    Ok(())
}

fn failure_result(
    plan: &RecoveryPlan,
    task_index: u32,
    stable_work_id: &str,
    error: &RecoveryError,
) -> TransferResult {
    let (state, code) = match error {
        RecoveryError::Artifact(ArtifactStoreError::ChecksumMismatch { .. })
        | RecoveryError::Artifact(ArtifactStoreError::ImmutableConflict(_)) => {
            (TransferState::Quarantined, "CHECKSUM_MISMATCH")
        }
        RecoveryError::Artifact(ArtifactStoreError::Store(_))
        | RecoveryError::Artifact(ArtifactStoreError::Io(_))
        | RecoveryError::Io(_) => (TransferState::RetryableFailure, "STORAGE_UNAVAILABLE"),
        RecoveryError::Artifact(_)
        | RecoveryError::InvalidContract(_)
        | RecoveryError::InsufficientFailureDomains { .. }
        | RecoveryError::Incomplete(_) => {
            (TransferState::PermanentFailure, "RECOVERY_CONTRACT_FAILED")
        }
    };
    TransferResult {
        operation_id: plan.operation_id,
        task_index,
        stable_work_id: stable_work_id.to_owned(),
        state,
        observed_sha256: None,
        copied_bytes: 0,
        error_code: Some(code.to_owned()),
    }
}

fn required(name: &'static str) -> Result<String> {
    env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .with_context(|| format!("{name} is required"))
}

fn required_u64(name: &'static str) -> Result<u64> {
    required(name)?
        .parse::<u64>()
        .ok()
        .filter(|value| *value > 0)
        .with_context(|| format!("{name} must be a positive integer"))
}

fn required_u32(name: &'static str) -> Result<u32> {
    required(name)?
        .parse::<u32>()
        .with_context(|| format!("{name} must be a non-negative integer"))
}

fn required_usize(name: &'static str) -> Result<usize> {
    required(name)?
        .parse::<usize>()
        .ok()
        .filter(|value| *value > 0)
        .with_context(|| format!("{name} must be a positive integer"))
}

fn required_sha256(name: &'static str) -> Result<String> {
    let value = required(name)?;
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        anyhow::bail!("{name} must be lowercase SHA-256");
    }
    Ok(value)
}
