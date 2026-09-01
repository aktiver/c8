//! One-shot PostgreSQL migration binary used by the Helm pre-install/pre-upgrade Job.

use std::{env, path::Path};

use anyhow::{Context, Result};
use sqlx::{PgPool, migrate::Migrator};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter("info")
        .init();
    let database_url = env::var("NGKG_DATABASE_URL").context("NGKG_DATABASE_URL is required")?;
    let migration_directory =
        env::var("NGKG_MIGRATION_DIRECTORY").context("NGKG_MIGRATION_DIRECTORY is required")?;
    let pool = PgPool::connect(&database_url).await?;
    let migrator = Migrator::new(Path::new(&migration_directory)).await?;
    migrator.run(&pool).await?;
    tracing::info!(directory = %migration_directory, "catalog migrations completed");
    Ok(())
}
