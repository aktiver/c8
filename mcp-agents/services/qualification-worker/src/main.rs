//! Kubernetes-native CPU qualification worker. PostgreSQL leases distribute
//! immutable partitions across nodes while each pod uses its cgroup CPU quota.

use std::{
    env, fs,
    path::{Path, PathBuf},
    str::FromStr,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use ngkg_agent_input::{InputObjectStore, ObjectStoreConfiguration};
use ngkg_cpu_work_plane::{ClaimedPartition, CpuWorkRepository, PartitionCompletion};
use ngkg_hpc_runtime::{ResourceBudget, canonical_lineset};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,ngkg_qualification_worker=info".into()),
        )
        .init();
    let repository = CpuWorkRepository::connect(
        &required("NGKG_AGENT_DATABASE_URL")?,
        positive_u32("NGKG_AGENT_DATABASE_MAX_CONNECTIONS", 8)?,
        Duration::from_millis(positive_u64(
            "NGKG_AGENT_DATABASE_ACQUIRE_TIMEOUT_MS",
            5_000,
        )?),
    )
    .await?;
    repository.ready().await?;
    let store = InputObjectStore::build(ObjectStoreConfiguration::from_environment()?)?;
    let worker_id = env::var("NGKG_QUALIFICATION_WORKER_ID")
        .unwrap_or_else(|_| format!("qualification-{}", uuid::Uuid::new_v4()));
    let lease_ms = i64::try_from(positive_u64("NGKG_QUALIFICATION_LEASE_MS", 300_000)?)?;
    let idle = Duration::from_millis(positive_u64("NGKG_QUALIFICATION_IDLE_POLL_MS", 250)?);
    let memory_fraction = positive_u64("NGKG_QUALIFICATION_MEMORY_FRACTION_PERCENT", 70)?;
    let configured_spill =
        positive_u64("NGKG_QUALIFICATION_MAX_SPILL_BYTES", 8 * 1024 * 1024 * 1024)?;
    let spill_root = absolute_path(
        "NGKG_QUALIFICATION_SPILL_ROOT",
        "/var/lib/ngkg-qualification-spill",
    )?;
    fs::create_dir_all(&spill_root)?;

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => break,
            claimed = repository.claim(&worker_id, lease_ms) => {
                let Some(claim) = claimed? else {
                    tokio::time::sleep(idle).await;
                    continue;
                };
                process_partition(&repository, &store, &spill_root, memory_fraction, configured_spill, lease_ms, claim).await;
            }
        }
    }
    Ok(())
}

async fn process_partition(
    repository: &CpuWorkRepository,
    store: &InputObjectStore,
    spill_root: &Path,
    memory_fraction: u64,
    configured_spill: u64,
    lease_ms: i64,
    claim: ClaimedPartition,
) {
    let directory = spill_root
        .join(claim.workload_id.to_string())
        .join(format!("{:010}", claim.partition_ordinal))
        .join(claim.lease_token.to_string());
    let result = async {
        anyhow::ensure!(claim.kernel == "CANONICAL_LINESET_V1", "unsupported kernel");
        let maximum_bytes = usize::try_from(claim.maximum_partition_bytes.min(claim.byte_length))?;
        let source = store.get_verified(&claim.object_reference, &claim.source_sha256, maximum_bytes).await?;
        let maximum_spill = configured_spill.min(u64::try_from(claim.maximum_spill_bytes)?);
        let budget = ResourceBudget::from_cgroup(memory_fraction, maximum_spill)?;
        let source = source.to_vec();
        let kernel_directory = directory.clone();
        let handle = tokio::task::spawn_blocking(move || canonical_lineset(&source, &kernel_directory, budget));
        tokio::pin!(handle);
        let heartbeat_ms = u64::try_from((lease_ms / 3).max(1_000))?;
        let mut heartbeat = tokio::time::interval(Duration::from_millis(heartbeat_ms));
        let receipt = loop {
            tokio::select! {
                outcome = &mut handle => break outcome.context("CPU kernel task failed")??,
                _ = heartbeat.tick() => {
                    repository.checkpoint(&claim, &claim.source_sha256, 0, 0, 0, lease_ms, epoch_ms()?).await?;
                }
            }
        };
        let completed_at = epoch_ms()?;
        repository.checkpoint(
            &claim,
            &receipt.result_sha256,
            i64::try_from(receipt.records)?,
            i64::try_from(receipt.input_bytes)?,
            i64::try_from(receipt.spilled_bytes)?,
            lease_ms,
            completed_at,
        ).await?;
        repository.complete(&claim, &PartitionCompletion {
            result_sha256: receipt.result_sha256,
            records_completed: i64::try_from(receipt.records)?,
            bytes_completed: i64::try_from(receipt.input_bytes)?,
            spill_bytes: i64::try_from(receipt.spilled_bytes)?,
            threads_used: i32::try_from(receipt.threads)?,
            peak_memory_bytes: i64::try_from(budget.memory_bytes)?,
            completed_at_epoch_ms: completed_at,
        }).await?;
        Result::<()>::Ok(())
    }.await;
    if directory.starts_with(spill_root) && directory != spill_root {
        let _ = fs::remove_dir_all(&directory);
    }
    match result {
        Ok(()) => {
            tracing::info!(workload_id=%claim.workload_id, partition=claim.partition_ordinal, attempt=claim.attempt, "CPU partition completed");
        }
        Err(error) => {
            tracing::error!(%error, workload_id=%claim.workload_id, partition=claim.partition_ordinal, attempt=claim.attempt, "CPU partition failed");
            let _ = repository
                .fail(&claim, failure_code(&error), epoch_ms().unwrap_or_default())
                .await;
        }
    }
}

fn failure_code(error: &anyhow::Error) -> &'static str {
    let message = error.to_string();
    if message.contains("checksum") {
        "SOURCE_CHECKSUM_FAILED"
    } else if message.contains("spill") {
        "SPILL_LIMIT_EXCEEDED"
    } else if message.contains("UTF-8") {
        "SOURCE_ENCODING_INVALID"
    } else if message.contains("lease") {
        "LEASE_LOST"
    } else {
        "CPU_KERNEL_FAILED"
    }
}

fn epoch_ms() -> Result<i64> {
    Ok(i64::try_from(
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis(),
    )?)
}
fn required(name: &'static str) -> Result<String> {
    env::var(name)
        .with_context(|| format!("{name} is required"))
        .and_then(|v| {
            anyhow::ensure!(!v.is_empty(), "{name} must not be empty");
            Ok(v)
        })
}
fn positive_u64(name: &'static str, default: u64) -> Result<u64> {
    let value = env::var(name)
        .ok()
        .map_or(Ok(default), |v| u64::from_str(&v))?;
    anyhow::ensure!(value > 0, "{name} must be positive");
    Ok(value)
}
fn positive_u32(name: &'static str, default: u32) -> Result<u32> {
    Ok(u32::try_from(positive_u64(name, u64::from(default))?)?)
}
fn absolute_path(name: &'static str, default: &str) -> Result<PathBuf> {
    let path = PathBuf::from(env::var(name).unwrap_or_else(|_| default.to_owned()));
    anyhow::ensure!(path.is_absolute(), "{name} must be absolute");
    Ok(path)
}
