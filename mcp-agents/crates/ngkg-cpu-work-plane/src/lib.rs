//! Durable tenant-scoped multinode CPU work and checkpoint repository.

#![allow(missing_docs)]

use std::time::Duration;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Row, postgres::PgPoolOptions};
use thiserror::Error;
use uuid::Uuid;

const REQUEST_DOMAIN: &[u8] = b"ngkg-cpu-work-request-v1\0";

#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CpuKernel {
    CanonicalLinesetV1,
}

impl CpuKernel {
    fn as_str(self) -> &'static str {
        match self {
            Self::CanonicalLinesetV1 => "CANONICAL_LINESET_V1",
        }
    }
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct CpuPartitionInput {
    pub ordinal: u32,
    pub object_reference: String,
    pub source_sha256: String,
    pub byte_length: u64,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct CreateQualificationWorkload {
    pub kernel: CpuKernel,
    pub partitions: Vec<CpuPartitionInput>,
    pub maximum_attempts: u32,
    pub maximum_partition_bytes: u64,
    pub maximum_spill_bytes: u64,
    pub idempotency_key: String,
}

#[derive(Clone, Debug, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkloadView {
    pub workload_id: Uuid,
    pub component: String,
    pub kernel: String,
    pub state: String,
    pub state_version: i64,
    pub total_partitions: i32,
    pub completed_partitions: i32,
    pub failed_partitions: i32,
    pub result_root_sha256: Option<String>,
    pub created_at_epoch_ms: i64,
    pub updated_at_epoch_ms: i64,
}

#[derive(Clone, Debug, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CheckpointView {
    pub partition_ordinal: i32,
    pub sequence: i64,
    pub checkpoint_sha256: String,
    pub records_completed: i64,
    pub bytes_completed: i64,
    pub spill_bytes: i64,
    pub created_at_epoch_ms: i64,
}

#[derive(Clone, Debug)]
pub struct ClaimedPartition {
    pub tenant_id: Uuid,
    pub workload_id: Uuid,
    pub partition_ordinal: i32,
    pub kernel: String,
    pub object_reference: String,
    pub source_sha256: String,
    pub byte_length: i64,
    pub maximum_partition_bytes: i64,
    pub maximum_spill_bytes: i64,
    pub lease_token: Uuid,
    pub attempt: i32,
}

#[derive(Clone, Debug)]
pub struct PartitionCompletion {
    pub result_sha256: String,
    pub records_completed: i64,
    pub bytes_completed: i64,
    pub spill_bytes: i64,
    pub threads_used: i32,
    pub peak_memory_bytes: i64,
    pub completed_at_epoch_ms: i64,
}

#[derive(Clone)]
pub struct CpuWorkRepository {
    pool: PgPool,
}

impl CpuWorkRepository {
    pub async fn connect(
        database_url: &str,
        maximum_connections: u32,
        acquire_timeout: Duration,
    ) -> Result<Self, WorkError> {
        if maximum_connections == 0 {
            return Err(WorkError::Invalid);
        }
        let pool = PgPoolOptions::new()
            .max_connections(maximum_connections)
            .acquire_timeout(acquire_timeout)
            .connect(database_url)
            .await?;
        Ok(Self { pool })
    }

    pub async fn ready(&self) -> Result<(), WorkError> {
        let ready: bool = sqlx::query_scalar(
            "SELECT COALESCE((SELECT relrowsecurity AND relforcerowsecurity FROM pg_class WHERE oid=to_regclass('ngkg_agents.cpu_workload')),false)",
        )
        .fetch_one(&self.pool)
        .await?;
        if !ready {
            return Err(WorkError::State);
        }
        Ok(())
    }

    pub async fn create_qualification(
        &self,
        tenant_id: Uuid,
        subject: &str,
        request: &CreateQualificationWorkload,
        now: i64,
    ) -> Result<WorkloadView, WorkError> {
        validate_request(tenant_id, subject, request)?;
        let request_bytes = serde_json::to_vec(request)?;
        let request_sha = domain_hash(REQUEST_DOMAIN, &request_bytes);
        let idempotency_sha = idempotency_hash(tenant_id, subject, &request.idempotency_key);
        let mut tx = self.tenant_tx(tenant_id).await?;
        if let Some(row) = sqlx::query(
            "SELECT workload_id,request_sha256 FROM ngkg_agents.cpu_workload WHERE tenant_id=$1 AND idempotency_sha256=$2",
        )
        .bind(tenant_id)
        .bind(idempotency_sha.as_slice())
        .fetch_optional(&mut *tx)
        .await?
        {
            let observed: Vec<u8> = row.try_get("request_sha256")?;
            if observed != request_sha {
                return Err(WorkError::Conflict);
            }
            let workload_id: Uuid = row.try_get("workload_id")?;
            tx.commit().await?;
            return self.get(tenant_id, workload_id).await;
        }
        let workload_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO ngkg_agents.cpu_workload(tenant_id,workload_id,component,kernel,subject,state,state_version,total_partitions,completed_partitions,failed_partitions,maximum_attempts,maximum_partition_bytes,maximum_spill_bytes,idempotency_sha256,request_sha256,created_at_epoch_ms,updated_at_epoch_ms) VALUES($1,$2,'QUALIFICATION',$3,$4,'READY',0,$5,0,0,$6,$7,$8,$9,$10,$11,$11)",
        )
        .bind(tenant_id)
        .bind(workload_id)
        .bind(request.kernel.as_str())
        .bind(subject)
        .bind(i32::try_from(request.partitions.len()).map_err(|_| WorkError::Limit)?)
        .bind(i32::try_from(request.maximum_attempts).map_err(|_| WorkError::Limit)?)
        .bind(i64::try_from(request.maximum_partition_bytes).map_err(|_| WorkError::Limit)?)
        .bind(i64::try_from(request.maximum_spill_bytes).map_err(|_| WorkError::Limit)?)
        .bind(idempotency_sha.as_slice())
        .bind(request_sha)
        .bind(now)
        .execute(&mut *tx)
        .await?;
        for partition in &request.partitions {
            sqlx::query(
                "INSERT INTO ngkg_agents.cpu_work_partition(tenant_id,workload_id,partition_ordinal,state,attempt,object_reference,source_sha256,byte_length) VALUES($1,$2,$3,'READY',0,$4,decode($5,'hex'),$6)",
            )
            .bind(tenant_id)
            .bind(workload_id)
            .bind(i32::try_from(partition.ordinal).map_err(|_| WorkError::Limit)?)
            .bind(&partition.object_reference)
            .bind(&partition.source_sha256)
            .bind(i64::try_from(partition.byte_length).map_err(|_| WorkError::Limit)?)
            .execute(&mut *tx)
            .await?;
        }
        sqlx::query("SELECT ngkg_agents.enqueue_cpu_workload($1,$2)")
            .bind(tenant_id)
            .bind(workload_id)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        self.get(tenant_id, workload_id).await
    }

    pub async fn get(&self, tenant_id: Uuid, workload_id: Uuid) -> Result<WorkloadView, WorkError> {
        let mut tx = self.tenant_tx(tenant_id).await?;
        let row = sqlx::query(
            "SELECT workload_id,component,kernel,state,state_version,total_partitions,completed_partitions,failed_partitions,result_root_sha256,created_at_epoch_ms,updated_at_epoch_ms FROM ngkg_agents.cpu_workload WHERE tenant_id=$1 AND workload_id=$2",
        )
        .bind(tenant_id)
        .bind(workload_id)
        .fetch_one(&mut *tx)
        .await?;
        tx.commit().await?;
        row_to_workload(&row)
    }

    pub async fn checkpoints(
        &self,
        tenant_id: Uuid,
        workload_id: Uuid,
    ) -> Result<Vec<CheckpointView>, WorkError> {
        let mut tx = self.tenant_tx(tenant_id).await?;
        let rows = sqlx::query(
            "SELECT partition_ordinal,sequence,checkpoint_sha256,records_completed,bytes_completed,spill_bytes,created_at_epoch_ms FROM ngkg_agents.cpu_checkpoint WHERE tenant_id=$1 AND workload_id=$2 ORDER BY partition_ordinal,sequence",
        )
        .bind(tenant_id)
        .bind(workload_id)
        .fetch_all(&mut *tx)
        .await?;
        tx.commit().await?;
        rows.iter().map(row_to_checkpoint).collect()
    }

    pub async fn cancel(
        &self,
        tenant_id: Uuid,
        workload_id: Uuid,
        now: i64,
    ) -> Result<WorkloadView, WorkError> {
        let mut tx = self.tenant_tx(tenant_id).await?;
        let changed = sqlx::query(
            "UPDATE ngkg_agents.cpu_workload SET state='CANCELLED',state_version=state_version+1,updated_at_epoch_ms=$3 WHERE tenant_id=$1 AND workload_id=$2 AND state IN ('READY','RUNNING')",
        )
        .bind(tenant_id)
        .bind(workload_id)
        .bind(now)
        .execute(&mut *tx)
        .await?;
        if changed.rows_affected() != 1 {
            return Err(WorkError::State);
        }
        sqlx::query("SELECT ngkg_agents.cancel_cpu_workload($1,$2)")
            .bind(tenant_id)
            .bind(workload_id)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        self.get(tenant_id, workload_id).await
    }

    pub async fn claim(
        &self,
        worker_id: &str,
        lease_ms: i64,
    ) -> Result<Option<ClaimedPartition>, WorkError> {
        let lease_token = Uuid::new_v4();
        let row =
            sqlx::query("SELECT * FROM ngkg_agents.claim_cpu_partition($1,$2,$3,'QUALIFICATION')")
                .bind(worker_id)
                .bind(lease_token)
                .bind(lease_ms)
                .fetch_optional(&self.pool)
                .await?;
        let Some(row) = row else { return Ok(None) };
        Ok(Some(ClaimedPartition {
            tenant_id: row.try_get("tenant_id")?,
            workload_id: row.try_get("workload_id")?,
            partition_ordinal: row.try_get("partition_ordinal")?,
            kernel: row.try_get("kernel")?,
            object_reference: row.try_get("object_reference")?,
            source_sha256: hex::encode(row.try_get::<Vec<u8>, _>("source_sha256")?),
            byte_length: row.try_get("byte_length")?,
            maximum_partition_bytes: row.try_get("maximum_partition_bytes")?,
            maximum_spill_bytes: row.try_get("maximum_spill_bytes")?,
            lease_token,
            attempt: row.try_get("attempt")?,
        }))
    }

    pub async fn checkpoint(
        &self,
        claim: &ClaimedPartition,
        checkpoint_sha256: &str,
        records: i64,
        bytes: i64,
        spill: i64,
        lease_ms: i64,
        now: i64,
    ) -> Result<(), WorkError> {
        let mut tx = self.tenant_tx(claim.tenant_id).await?;
        sqlx::query("SELECT ngkg_agents.checkpoint_cpu_partition($1,$2,$3,$4,decode($5,'hex'),$6,$7,$8,$9,$10)")
            .bind(claim.tenant_id)
            .bind(claim.workload_id)
            .bind(claim.partition_ordinal)
            .bind(claim.lease_token)
            .bind(checkpoint_sha256)
            .bind(records)
            .bind(bytes)
            .bind(spill)
            .bind(lease_ms)
            .bind(now)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn complete(
        &self,
        claim: &ClaimedPartition,
        completion: &PartitionCompletion,
    ) -> Result<(), WorkError> {
        let mut tx = self.tenant_tx(claim.tenant_id).await?;
        sqlx::query("SELECT ngkg_agents.finish_cpu_partition($1,$2,$3,$4,'COMPLETED',decode($5,'hex'),NULL,$6,$7,$8,$9,$10,$11)")
            .bind(claim.tenant_id)
            .bind(claim.workload_id)
            .bind(claim.partition_ordinal)
            .bind(claim.lease_token)
            .bind(&completion.result_sha256)
            .bind(completion.records_completed)
            .bind(completion.bytes_completed)
            .bind(completion.spill_bytes)
            .bind(completion.threads_used)
            .bind(completion.peak_memory_bytes)
            .bind(completion.completed_at_epoch_ms)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        self.finalize_if_complete(
            claim.tenant_id,
            claim.workload_id,
            completion.completed_at_epoch_ms,
        )
        .await?;
        Ok(())
    }

    async fn finalize_if_complete(
        &self,
        tenant_id: Uuid,
        workload_id: Uuid,
        now: i64,
    ) -> Result<(), WorkError> {
        let mut tx = self.tenant_tx(tenant_id).await?;
        let row = sqlx::query("SELECT state,total_partitions,completed_partitions,failed_partitions FROM ngkg_agents.cpu_workload WHERE tenant_id=$1 AND workload_id=$2 FOR UPDATE")
            .bind(tenant_id)
            .bind(workload_id)
            .fetch_one(&mut *tx)
            .await?;
        let state: String = row.try_get("state")?;
        let total: i32 = row.try_get("total_partitions")?;
        let completed: i32 = row.try_get("completed_partitions")?;
        let failed: i32 = row.try_get("failed_partitions")?;
        if state == "RUNNING" && failed == 0 && completed == total {
            let rows = sqlx::query("SELECT partition_ordinal,result_sha256 FROM ngkg_agents.cpu_work_partition WHERE tenant_id=$1 AND workload_id=$2 ORDER BY partition_ordinal")
                .bind(tenant_id)
                .bind(workload_id)
                .fetch_all(&mut *tx)
                .await?;
            let hashes = rows
                .iter()
                .map(|partition| {
                    let ordinal: i32 = partition.try_get("partition_ordinal")?;
                    let hash: Vec<u8> = partition.try_get("result_sha256")?;
                    Ok((
                        u32::try_from(ordinal).map_err(|_| WorkError::State)?,
                        hex::encode(hash),
                    ))
                })
                .collect::<Result<Vec<_>, WorkError>>()?;
            let root = ngkg_hpc_runtime::deterministic_partition_root(&hashes)
                .map_err(|_| WorkError::State)?;
            sqlx::query("UPDATE ngkg_agents.cpu_workload SET state='COMPLETED',state_version=state_version+1,result_root_sha256=decode($3,'hex'),updated_at_epoch_ms=$4 WHERE tenant_id=$1 AND workload_id=$2 AND state='RUNNING'")
                .bind(tenant_id)
                .bind(workload_id)
                .bind(root)
                .bind(now)
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    pub async fn fail(
        &self,
        claim: &ClaimedPartition,
        code: &str,
        now: i64,
    ) -> Result<(), WorkError> {
        if code.is_empty() || code.len() > 128 {
            return Err(WorkError::Invalid);
        }
        let mut tx = self.tenant_tx(claim.tenant_id).await?;
        sqlx::query(
            "SELECT ngkg_agents.finish_cpu_partition($1,$2,$3,$4,'FAILED',NULL,$5,0,0,0,0,0,$6)",
        )
        .bind(claim.tenant_id)
        .bind(claim.workload_id)
        .bind(claim.partition_ordinal)
        .bind(claim.lease_token)
        .bind(code)
        .bind(now)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn ready_partitions(&self) -> Result<i64, WorkError> {
        Ok(
            sqlx::query_scalar("SELECT ngkg_agents.cpu_ready_partition_count('QUALIFICATION')")
                .fetch_one(&self.pool)
                .await?,
        )
    }

    async fn tenant_tx(
        &self,
        tenant_id: Uuid,
    ) -> Result<sqlx::Transaction<'_, sqlx::Postgres>, WorkError> {
        if tenant_id.is_nil() {
            return Err(WorkError::Invalid);
        }
        let mut tx = self.pool.begin().await?;
        sqlx::query("SELECT set_config('ngkg.tenant_id',$1,true)")
            .bind(tenant_id.to_string())
            .execute(&mut *tx)
            .await?;
        Ok(tx)
    }
}

fn validate_request(
    tenant_id: Uuid,
    subject: &str,
    request: &CreateQualificationWorkload,
) -> Result<(), WorkError> {
    if tenant_id.is_nil()
        || subject.is_empty()
        || subject.len() > 256
        || request.partitions.is_empty()
        || request.partitions.len() > 100_000
        || !(1..=20).contains(&request.maximum_attempts)
        || request.maximum_partition_bytes == 0
        || request.maximum_partition_bytes > 1_073_741_824
        || request.maximum_spill_bytes == 0
        || request.maximum_spill_bytes > 4_398_046_511_104
        || request.idempotency_key.len() < 8
        || request.idempotency_key.len() > 256
    {
        return Err(WorkError::Invalid);
    }
    let prefix = format!("tenants/{tenant_id}/");
    for (index, partition) in request.partitions.iter().enumerate() {
        if usize::try_from(partition.ordinal).ok() != Some(index)
            || !partition.object_reference.starts_with(&prefix)
            || partition.object_reference.contains("..")
            || partition.source_sha256.len() != 64
            || hex::decode(&partition.source_sha256).map_or(true, |bytes| bytes.len() != 32)
            || partition.byte_length == 0
            || partition.byte_length > request.maximum_partition_bytes
        {
            return Err(WorkError::Invalid);
        }
    }
    Ok(())
}

fn row_to_workload(row: &sqlx::postgres::PgRow) -> Result<WorkloadView, WorkError> {
    let root: Option<Vec<u8>> = row.try_get("result_root_sha256")?;
    Ok(WorkloadView {
        workload_id: row.try_get("workload_id")?,
        component: row.try_get("component")?,
        kernel: row.try_get("kernel")?,
        state: row.try_get("state")?,
        state_version: row.try_get("state_version")?,
        total_partitions: row.try_get("total_partitions")?,
        completed_partitions: row.try_get("completed_partitions")?,
        failed_partitions: row.try_get("failed_partitions")?,
        result_root_sha256: root.map(hex::encode),
        created_at_epoch_ms: row.try_get("created_at_epoch_ms")?,
        updated_at_epoch_ms: row.try_get("updated_at_epoch_ms")?,
    })
}

fn row_to_checkpoint(row: &sqlx::postgres::PgRow) -> Result<CheckpointView, WorkError> {
    Ok(CheckpointView {
        partition_ordinal: row.try_get("partition_ordinal")?,
        sequence: row.try_get("sequence")?,
        checkpoint_sha256: hex::encode(row.try_get::<Vec<u8>, _>("checkpoint_sha256")?),
        records_completed: row.try_get("records_completed")?,
        bytes_completed: row.try_get("bytes_completed")?,
        spill_bytes: row.try_get("spill_bytes")?,
        created_at_epoch_ms: row.try_get("created_at_epoch_ms")?,
    })
}

fn idempotency_hash(tenant: Uuid, subject: &str, key: &str) -> Vec<u8> {
    let mut digest = Sha256::new();
    digest.update(b"ngkg-cpu-work-idempotency-v1\0");
    digest.update(tenant.as_bytes());
    digest.update(subject.as_bytes());
    digest.update([0]);
    digest.update(key.as_bytes());
    digest.finalize().to_vec()
}

fn domain_hash(domain: &[u8], value: &[u8]) -> Vec<u8> {
    let mut digest = Sha256::new();
    digest.update(domain);
    digest.update(value);
    digest.finalize().to_vec()
}

#[derive(Debug, Error)]
pub enum WorkError {
    #[error("work request is invalid")]
    Invalid,
    #[error("work state is invalid")]
    State,
    #[error("idempotency conflict")]
    Conflict,
    #[error("configured limit exceeded")]
    Limit,
    #[error("database failed")]
    Database(#[from] sqlx::Error),
    #[error("JSON failed")]
    Json(#[from] serde_json::Error),
}
