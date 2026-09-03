//! Idempotent lease-based context-slice garbage collector.

use std::{
    env,
    str::FromStr,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use ngkg_context_slice::{ContextObjectStore, ContextStoreConfiguration, SliceRepository};
use ngkg_hpc_runtime::ResourceBudget;
use sha2::{Digest, Sha256};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let repository = SliceRepository::connect(
        &required("NGKG_AGENT_DATABASE_URL")?,
        positive_u32("NGKG_CONTEXT_GC_DATABASE_MAX_CONNECTIONS", 8)?,
        Duration::from_millis(positive_u64(
            "NGKG_CONTEXT_GC_DATABASE_ACQUIRE_TIMEOUT_MS",
            5000,
        )?),
    )
    .await?;
    let cgroup_threads = ResourceBudget::from_cgroup(50, 1)?.threads.max(1);
    let configured = usize::try_from(positive_u64("NGKG_CONTEXT_MAX_HASH_TASKS", 16)?)?;
    let store = ContextObjectStore::build(
        ContextStoreConfiguration::from_environment()?,
        configured.min(cgroup_threads),
    )?;
    let worker = env::var("NGKG_CONTEXT_GC_WORKER_ID")
        .unwrap_or_else(|_| format!("context-gc-{}", uuid::Uuid::new_v4()));
    let lease = i64::try_from(positive_u64("NGKG_CONTEXT_GC_LEASE_MS", 300_000)?)?;
    let idle = Duration::from_millis(positive_u64("NGKG_CONTEXT_GC_IDLE_POLL_MS", 1000)?);
    loop {
        tokio::select! {
            _=tokio::signal::ctrl_c()=>break,
            claim=repository.claim_gc(&worker,lease,epoch_ms()?)=>{
                let Some((tenant,slice,token))=claim? else {tokio::time::sleep(idle).await;continue};
                match delete_slice(&repository,&store,tenant,slice,token).await {
                    Ok(())=>tracing::info!(%tenant,%slice,"context slice deleted and tombstoned"),
                    Err(error)=>tracing::error!(%error,%tenant,%slice,"context slice deletion will retry after lease expiry"),
                }
            }
        }
    }
    Ok(())
}

async fn delete_slice(
    repository: &SliceRepository,
    store: &ContextObjectStore,
    tenant: uuid::Uuid,
    slice: uuid::Uuid,
    token: uuid::Uuid,
) -> Result<()> {
    let references = repository.internal_refs(tenant, slice).await?;
    let mut evidence = Sha256::new();
    evidence.update(b"ngkg-context-slice-tombstone-v1\0");
    evidence.update(tenant.as_bytes());
    evidence.update(slice.as_bytes());
    for reference in &references {
        store.delete(reference).await?;
        evidence.update(u64::try_from(reference.len())?.to_le_bytes());
        evidence.update(reference.as_bytes());
    }
    repository
        .finish_gc(
            tenant,
            slice,
            token,
            &hex::encode(evidence.finalize()),
            i32::try_from(references.len())?,
            epoch_ms()?,
        )
        .await?;
    Ok(())
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
    let v = env::var(name)
        .ok()
        .map_or(Ok(default), |v| u64::from_str(&v))?;
    anyhow::ensure!(v > 0, "{name} must be positive");
    Ok(v)
}
fn positive_u32(name: &'static str, default: u32) -> Result<u32> {
    Ok(u32::try_from(positive_u64(name, u64::from(default))?)?)
}
