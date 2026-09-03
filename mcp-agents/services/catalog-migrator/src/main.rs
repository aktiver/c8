//! Dedicated, one-shot migration entry point for the `ngkg_agents` schema.

use std::{env, str::FromStr, time::Duration};

use anyhow::{Context, Result};
use ngkg_agent_catalog::{AgentCatalog, CatalogOptions};

#[tokio::main]
async fn main() -> Result<()> {
    let database_url =
        env::var("NGKG_AGENT_DATABASE_URL").context("NGKG_AGENT_DATABASE_URL is required")?;
    let maximum_connections = env::var("NGKG_AGENT_DATABASE_MAX_CONNECTIONS")
        .ok()
        .map_or(Ok(2), |value| u32::from_str(&value))?;
    let acquire_timeout = env::var("NGKG_AGENT_DATABASE_ACQUIRE_TIMEOUT_MS")
        .ok()
        .map_or(Ok(5_000), |value| u64::from_str(&value))?;
    let allow_insecure_loopback = env::var("NGKG_AGENT_DATABASE_ALLOW_INSECURE_LOOPBACK")
        .ok()
        .map_or(Ok(false), |value| bool::from_str(&value))?;
    let catalog = AgentCatalog::connect(
        &database_url,
        CatalogOptions {
            maximum_connections,
            acquire_timeout: Duration::from_millis(acquire_timeout),
            allow_insecure_loopback,
        },
    )
    .await?;
    catalog.migrate().await?;
    let runtime_role = env::var("NGKG_AGENT_RUNTIME_DATABASE_ROLE")
        .context("NGKG_AGENT_RUNTIME_DATABASE_ROLE is required")?;
    catalog.grant_runtime_role(&runtime_role).await?;
    catalog.ready().await?;
    Ok(())
}
