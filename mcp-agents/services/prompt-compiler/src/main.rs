//! Distributed prompt compiler worker. PostgreSQL leases distribute immutable
//! parts across nodes; Rayon processes each part across the worker's CPU quota.

use anyhow::{Context, Result};
use ngkg_agent_input::{
    CompileLimits, InputObjectStore, InputRepository, ObjectStoreConfiguration, compile_part,
};
use std::{
    env, fs,
    str::FromStr,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,ngkg_prompt_compiler=info".into()),
        )
        .init();
    let database_url = required("NGKG_AGENT_DATABASE_URL")?;
    let repository = InputRepository::connect(
        &database_url,
        positive_u32("NGKG_AGENT_DATABASE_MAX_CONNECTIONS", 8)?,
        Duration::from_millis(positive_u64(
            "NGKG_AGENT_DATABASE_ACQUIRE_TIMEOUT_MS",
            5000,
        )?),
    )
    .await?;
    let store = InputObjectStore::build(ObjectStoreConfiguration::from_environment()?)?;
    let worker_id = env::var("NGKG_PROMPT_WORKER_ID")
        .unwrap_or_else(|_| format!("worker-{}", uuid::Uuid::new_v4()));
    let threads = positive_usize("NGKG_PROMPT_COMPILER_THREADS", cgroup_cpu_count())?;
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(threads)
        .thread_name(|i| format!("prompt-cpu-{i}"))
        .build()?;
    let limits = CompileLimits {
        target_chunk_bytes: positive_usize("NGKG_PROMPT_TARGET_CHUNK_BYTES", 16_384)?,
        maximum_chunk_bytes: positive_usize("NGKG_PROMPT_MAX_CHUNK_BYTES", 65_536)?,
        maximum_chunks: positive_usize("NGKG_PROMPT_MAX_CHUNKS_PER_PART", 100_000)?,
        maximum_requirements: positive_usize("NGKG_PROMPT_MAX_REQUIREMENTS_PER_PART", 100_000)?,
    };
    let maximum_part_bytes = positive_usize("NGKG_PROMPT_MAX_PART_BYTES", 67_108_864)?;
    let lease_ms =
        i64::try_from(positive_u64("NGKG_PROMPT_LEASE_MS", 300_000)?).context("lease overflow")?;
    let idle = Duration::from_millis(positive_u64("NGKG_PROMPT_IDLE_POLL_MS", 250)?);
    repository.ready().await?;
    loop {
        tokio::select! {
            _=tokio::signal::ctrl_c()=>break,
            claimed=repository.claim_shard(&worker_id,lease_ms)=>{
                let Some(shard)=claimed? else{tokio::time::sleep(idle).await;continue};
                let bytes=match store.get_verified(&shard.object_reference,&shard.source_sha256,maximum_part_bytes).await{Ok(value)=>value,Err(error)=>{tracing::error!(%error,input_id=%shard.input_id,part=shard.part_ordinal,"verified source load failed");repository.fail_shard(&shard,"SOURCE_VERIFICATION_FAILED").await?;continue}};
                let input_id=shard.input_id;let ordinal=u32::try_from(shard.part_ordinal).context("negative part ordinal")?;
                let compiled=pool.install(||compile_part(input_id,ordinal,&bytes,limits));
                match compiled{Ok(value)=>{let now=epoch_ms()?;repository.complete_shard(&shard,&value,now).await?;repository.finalize_compilation(shard.tenant_id,shard.input_id,now).await?;tracing::info!(input_id=%shard.input_id,part=shard.part_ordinal,chunks=value.chunks.len(),requirements=value.requirements.len(),"prompt shard compiled")},Err(error)=>{tracing::error!(%error,input_id=%shard.input_id,part=shard.part_ordinal,"prompt compile failed");repository.fail_shard(&shard,"STRUCTURAL_COMPILE_FAILED").await?}}
            }
        }
    }
    Ok(())
}

fn cgroup_cpu_count() -> usize {
    let available = std::thread::available_parallelism().map_or(1, usize::from);
    let quota = fs::read_to_string("/sys/fs/cgroup/cpu.max")
        .ok()
        .and_then(|s| {
            let mut p = s.split_whitespace();
            let q = p.next()?;
            let period = p.next()?.parse::<u64>().ok()?;
            if q == "max" {
                None
            } else {
                let quota = q.parse::<u64>().ok()?;
                usize::try_from(quota.saturating_add(period - 1) / period).ok()
            }
        })
        .unwrap_or(available);
    available.min(quota).max(1)
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
fn positive_usize(name: &'static str, default: usize) -> Result<usize> {
    Ok(usize::try_from(positive_u64(
        name,
        u64::try_from(default)?,
    )?)?)
}
