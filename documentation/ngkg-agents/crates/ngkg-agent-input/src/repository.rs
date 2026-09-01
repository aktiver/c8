use std::time::Duration;

use serde::{Deserialize, Serialize};
use sqlx::{PgPool, Row, postgres::PgPoolOptions};
use thiserror::Error;
use uuid::Uuid;

use crate::{CompiledPart, PromptChunk, PromptRequirement};

#[derive(Clone)]
pub struct InputRepository {
    pool: PgPool,
}

#[derive(Clone, Debug)]
pub struct CreateInput {
    pub input_id: Uuid,
    pub subject: String,
    pub actor: Option<String>,
    pub source_name: String,
    pub media_type: String,
    pub maximum_parts: i32,
    pub maximum_bytes: i64,
    pub created_at_epoch_ms: i64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InputPart {
    pub ordinal: i32,
    pub byte_length: i64,
    pub media_type: String,
    pub source_sha256: String,
    pub object_reference: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InputManifest {
    pub input_id: Uuid,
    pub state: String,
    pub state_version: i64,
    pub expected_parts: Option<i32>,
    pub total_bytes: Option<i64>,
    pub source_root_sha256: Option<String>,
    pub compiled_root_sha256: Option<String>,
    pub requirement_root_sha256: Option<String>,
    pub parts: Vec<InputPart>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InputStatus {
    pub input_id: Uuid,
    pub state: String,
    pub state_version: i64,
    pub completed_shards: i64,
    pub total_shards: i64,
    pub failure_code: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RequirementRecord {
    pub requirement_id: String,
    pub part_ordinal: i32,
    pub source_chunk_id: String,
    pub kind: String,
    pub mandatory: bool,
    pub byte_start: i64,
    pub byte_end: i64,
    pub normalized_text: String,
    pub text_sha256: String,
}

#[derive(Clone, Debug)]
pub struct ClaimedShard {
    pub tenant_id: Uuid,
    pub input_id: Uuid,
    pub part_ordinal: i32,
    pub source_sha256: String,
    pub object_reference: String,
    pub lease_token: Uuid,
}

impl InputRepository {
    pub async fn connect(
        database_url: &str,
        maximum_connections: u32,
        acquire_timeout: Duration,
    ) -> Result<Self, RepositoryError> {
        let pool = PgPoolOptions::new()
            .max_connections(maximum_connections)
            .acquire_timeout(acquire_timeout)
            .connect(database_url)
            .await?;
        Ok(Self { pool })
    }

    pub async fn ready(&self) -> Result<(), RepositoryError> {
        let ready: bool = sqlx::query_scalar("SELECT COALESCE((SELECT relrowsecurity AND relforcerowsecurity FROM pg_class WHERE oid=to_regclass('ngkg_agents.prompt_input')), false)").fetch_one(&self.pool).await?;
        if !ready {
            return Err(RepositoryError::State);
        }
        Ok(())
    }

    pub async fn create_input(
        &self,
        tenant_id: Uuid,
        input: &CreateInput,
    ) -> Result<InputManifest, RepositoryError> {
        let mut tx = self.tenant_tx(tenant_id).await?;
        sqlx::query("INSERT INTO ngkg_agents.prompt_input (tenant_id,input_id,subject,actor,source_name,media_type,state,state_version,maximum_parts,maximum_bytes,created_at_epoch_ms) VALUES ($1,$2,$3,$4,$5,$6,'UPLOADING',0,$7,$8,$9)")
            .bind(tenant_id).bind(input.input_id).bind(&input.subject).bind(&input.actor).bind(&input.source_name).bind(&input.media_type).bind(input.maximum_parts).bind(input.maximum_bytes).bind(input.created_at_epoch_ms).execute(&mut *tx).await?;
        tx.commit().await?;
        self.manifest(tenant_id, input.input_id).await
    }

    pub async fn record_part(
        &self,
        tenant_id: Uuid,
        input_id: Uuid,
        part: &InputPart,
    ) -> Result<(), RepositoryError> {
        let mut tx = self.tenant_tx(tenant_id).await?;
        let input_row = sqlx::query("SELECT state,maximum_parts,maximum_bytes FROM ngkg_agents.prompt_input WHERE tenant_id=$1 AND input_id=$2 FOR UPDATE").bind(tenant_id).bind(input_id).fetch_one(&mut *tx).await?;
        let state: String = input_row.try_get("state")?;
        if state != "UPLOADING" {
            return Err(RepositoryError::State);
        }
        if part.ordinal >= input_row.try_get::<i32, _>("maximum_parts")? {
            return Err(RepositoryError::Limit);
        }
        let existing = sqlx::query("SELECT byte_length,source_sha256,object_reference FROM ngkg_agents.prompt_part WHERE tenant_id=$1 AND input_id=$2 AND ordinal=$3").bind(tenant_id).bind(input_id).bind(part.ordinal).fetch_optional(&mut *tx).await?;
        if let Some(row) = existing {
            let hash: Vec<u8> = row.try_get("source_sha256")?;
            if row.try_get::<i64, _>("byte_length")? != part.byte_length
                || hex::encode(hash) != part.source_sha256
                || row.try_get::<String, _>("object_reference")? != part.object_reference
            {
                return Err(RepositoryError::Conflict);
            }
        } else {
            let uploaded:i64=sqlx::query_scalar("SELECT COALESCE(sum(byte_length),0) FROM ngkg_agents.prompt_part WHERE tenant_id=$1 AND input_id=$2").bind(tenant_id).bind(input_id).fetch_one(&mut *tx).await?;
            if uploaded
                .checked_add(part.byte_length)
                .ok_or(RepositoryError::Limit)?
                > input_row.try_get::<i64, _>("maximum_bytes")?
            {
                return Err(RepositoryError::Limit);
            }
            sqlx::query("INSERT INTO ngkg_agents.prompt_part (tenant_id,input_id,ordinal,byte_length,media_type,source_sha256,object_reference) VALUES ($1,$2,$3,$4,$5,decode($6,'hex'),$7)").bind(tenant_id).bind(input_id).bind(part.ordinal).bind(part.byte_length).bind(&part.media_type).bind(&part.source_sha256).bind(&part.object_reference).execute(&mut *tx).await?;
        }
        tx.commit().await?;
        Ok(())
    }

    pub async fn finalize_input(
        &self,
        tenant_id: Uuid,
        input_id: Uuid,
        expected_parts: i32,
        expected_bytes: i64,
        expected_root: &str,
        epoch_ms: i64,
    ) -> Result<InputManifest, RepositoryError> {
        let mut tx = self.tenant_tx(tenant_id).await?;
        let row = sqlx::query("SELECT state,state_version,maximum_parts,maximum_bytes FROM ngkg_agents.prompt_input WHERE tenant_id=$1 AND input_id=$2 FOR UPDATE").bind(tenant_id).bind(input_id).fetch_one(&mut *tx).await?;
        let state: String = row.try_get("state")?;
        if state != "UPLOADING" && state != "FINALIZED" && state != "COMPILING" {
            return Err(RepositoryError::State);
        }
        let parts = sqlx::query("SELECT ordinal,byte_length,encode(source_sha256,'hex') AS source_sha256 FROM ngkg_agents.prompt_part WHERE tenant_id=$1 AND input_id=$2 ORDER BY ordinal").bind(tenant_id).bind(input_id).fetch_all(&mut *tx).await?;
        if parts.len() != usize::try_from(expected_parts).map_err(|_| RepositoryError::Limit)?
            || parts
                .iter()
                .enumerate()
                .any(|(i, row)| row.try_get::<i32, _>("ordinal").ok() != i32::try_from(i).ok())
        {
            return Err(RepositoryError::Incomplete);
        }
        let total = parts.iter().try_fold(0_i64, |sum, row| {
            sum.checked_add(row.try_get::<i64, _>("byte_length")?)
                .ok_or(RepositoryError::Limit)
        })?;
        if total != expected_bytes
            || total > row.try_get::<i64, _>("maximum_bytes")?
            || expected_parts > row.try_get::<i32, _>("maximum_parts")?
        {
            return Err(RepositoryError::Limit);
        }
        let root = source_root(parts.iter().map(|row| {
            (
                row.try_get::<i32, _>("ordinal").unwrap_or_default(),
                row.try_get::<i64, _>("byte_length").unwrap_or_default(),
                row.try_get::<String, _>("source_sha256")
                    .unwrap_or_default(),
            )
        }));
        if root != expected_root {
            return Err(RepositoryError::Conflict);
        }
        if state == "UPLOADING" {
            sqlx::query("UPDATE ngkg_agents.prompt_input SET state='COMPILING',state_version=state_version+1,expected_parts=$3,total_bytes=$4,source_root_sha256=decode($5,'hex'),finalized_at_epoch_ms=$6 WHERE tenant_id=$1 AND input_id=$2").bind(tenant_id).bind(input_id).bind(expected_parts).bind(expected_bytes).bind(expected_root).bind(epoch_ms).execute(&mut *tx).await?;
            sqlx::query("INSERT INTO ngkg_agents.prompt_compilation_shard (tenant_id,input_id,part_ordinal,state,attempt) SELECT tenant_id,input_id,ordinal,'READY',0 FROM ngkg_agents.prompt_part WHERE tenant_id=$1 AND input_id=$2 ON CONFLICT DO NOTHING").bind(tenant_id).bind(input_id).execute(&mut *tx).await?;
            sqlx::query("SELECT ngkg_agents.enqueue_prompt_compilation_shards($1,$2)")
                .bind(tenant_id)
                .bind(input_id)
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
        self.manifest(tenant_id, input_id).await
    }

    pub async fn manifest(
        &self,
        tenant_id: Uuid,
        input_id: Uuid,
    ) -> Result<InputManifest, RepositoryError> {
        let mut tx = self.tenant_tx(tenant_id).await?;
        let row=sqlx::query("SELECT state,state_version,expected_parts,total_bytes,encode(source_root_sha256,'hex') source_root,encode(compiled_root_sha256,'hex') compiled_root,encode(requirement_root_sha256,'hex') requirement_root FROM ngkg_agents.prompt_input WHERE tenant_id=$1 AND input_id=$2").bind(tenant_id).bind(input_id).fetch_one(&mut *tx).await?;
        let rows=sqlx::query("SELECT ordinal,byte_length,media_type,encode(source_sha256,'hex') source_sha256,object_reference FROM ngkg_agents.prompt_part WHERE tenant_id=$1 AND input_id=$2 ORDER BY ordinal").bind(tenant_id).bind(input_id).fetch_all(&mut *tx).await?;
        let parts = rows
            .into_iter()
            .map(|p| {
                Ok(InputPart {
                    ordinal: p.try_get("ordinal")?,
                    byte_length: p.try_get("byte_length")?,
                    media_type: p.try_get("media_type")?,
                    source_sha256: p.try_get("source_sha256")?,
                    object_reference: p.try_get("object_reference")?,
                })
            })
            .collect::<Result<Vec<_>, sqlx::Error>>()?;
        tx.commit().await?;
        Ok(InputManifest {
            input_id,
            state: row.try_get("state")?,
            state_version: row.try_get("state_version")?,
            expected_parts: row.try_get("expected_parts")?,
            total_bytes: row.try_get("total_bytes")?,
            source_root_sha256: row.try_get("source_root")?,
            compiled_root_sha256: row.try_get("compiled_root")?,
            requirement_root_sha256: row.try_get("requirement_root")?,
            parts,
        })
    }

    pub async fn status(
        &self,
        tenant_id: Uuid,
        input_id: Uuid,
    ) -> Result<InputStatus, RepositoryError> {
        let mut tx = self.tenant_tx(tenant_id).await?;
        let row=sqlx::query("SELECT p.state,p.state_version,p.failure_code,count(s.*) total_shards,count(s.*) FILTER (WHERE s.state='COMPLETED') completed_shards FROM ngkg_agents.prompt_input p LEFT JOIN ngkg_agents.prompt_compilation_shard s ON s.tenant_id=p.tenant_id AND s.input_id=p.input_id WHERE p.tenant_id=$1 AND p.input_id=$2 GROUP BY p.state,p.state_version,p.failure_code").bind(tenant_id).bind(input_id).fetch_one(&mut *tx).await?;
        tx.commit().await?;
        Ok(InputStatus {
            input_id,
            state: row.try_get("state")?,
            state_version: row.try_get("state_version")?,
            completed_shards: row.try_get("completed_shards")?,
            total_shards: row.try_get("total_shards")?,
            failure_code: row.try_get("failure_code")?,
        })
    }

    pub async fn requirements(
        &self,
        tenant_id: Uuid,
        input_id: Uuid,
    ) -> Result<Vec<RequirementRecord>, RepositoryError> {
        let mut tx = self.tenant_tx(tenant_id).await?;
        let rows=sqlx::query("SELECT requirement_id,part_ordinal,source_chunk_id,kind,mandatory,byte_start,byte_end,normalized_text,encode(text_sha256,'hex') text_sha256 FROM ngkg_agents.prompt_requirement WHERE tenant_id=$1 AND input_id=$2 ORDER BY part_ordinal,byte_start,requirement_id").bind(tenant_id).bind(input_id).fetch_all(&mut *tx).await?;
        let result = rows
            .into_iter()
            .map(|r| {
                Ok(RequirementRecord {
                    requirement_id: r.try_get("requirement_id")?,
                    part_ordinal: r.try_get("part_ordinal")?,
                    source_chunk_id: r.try_get("source_chunk_id")?,
                    kind: r.try_get("kind")?,
                    mandatory: r.try_get("mandatory")?,
                    byte_start: r.try_get("byte_start")?,
                    byte_end: r.try_get("byte_end")?,
                    normalized_text: r.try_get("normalized_text")?,
                    text_sha256: r.try_get("text_sha256")?,
                })
            })
            .collect::<Result<Vec<_>, sqlx::Error>>()?;
        tx.commit().await?;
        Ok(result)
    }

    pub async fn claim_shard(
        &self,
        worker_id: &str,
        lease_ms: i64,
    ) -> Result<Option<ClaimedShard>, RepositoryError> {
        let lease_token = Uuid::new_v4();
        let row=sqlx::query("SELECT tenant_id,input_id,part_ordinal FROM ngkg_agents.claim_prompt_compilation_shard($1,$2,$3)").bind(worker_id).bind(lease_token).bind(lease_ms).fetch_optional(&self.pool).await?;
        let Some(row) = row else { return Ok(None) };
        let tenant_id = row.try_get("tenant_id")?;
        let input_id = row.try_get("input_id")?;
        let part_ordinal = row.try_get("part_ordinal")?;
        let mut tx = self.tenant_tx(tenant_id).await?;
        let p=sqlx::query("SELECT encode(source_sha256,'hex') source_sha256,object_reference FROM ngkg_agents.prompt_part WHERE tenant_id=$1 AND input_id=$2 AND ordinal=$3").bind(tenant_id).bind(input_id).bind(part_ordinal).fetch_one(&mut *tx).await?;
        tx.commit().await?;
        Ok(Some(ClaimedShard {
            tenant_id,
            input_id,
            part_ordinal,
            source_sha256: p.try_get("source_sha256")?,
            object_reference: p.try_get("object_reference")?,
            lease_token,
        }))
    }

    pub async fn complete_shard(
        &self,
        shard: &ClaimedShard,
        compiled: &CompiledPart,
        epoch_ms: i64,
    ) -> Result<(), RepositoryError> {
        let mut tx = self.tenant_tx(shard.tenant_id).await?;
        let owned:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM ngkg_agents.prompt_compilation_shard WHERE tenant_id=$1 AND input_id=$2 AND part_ordinal=$3 AND state='LEASED' AND lease_token=$4)").bind(shard.tenant_id).bind(shard.input_id).bind(shard.part_ordinal).bind(shard.lease_token).fetch_one(&mut *tx).await?;
        if !owned {
            return Err(RepositoryError::Lease);
        }
        for c in &compiled.chunks {
            insert_chunk(&mut tx, shard, c).await?;
        }
        for r in &compiled.requirements {
            insert_requirement(&mut tx, shard, r).await?;
        }
        sqlx::query("UPDATE ngkg_agents.prompt_compilation_shard SET state='COMPLETED',compiled_sha256=decode($5,'hex'),completed_at_epoch_ms=$6,lease_owner=NULL,lease_token=NULL,lease_expires_at_epoch_ms=NULL WHERE tenant_id=$1 AND input_id=$2 AND part_ordinal=$3 AND lease_token=$4").bind(shard.tenant_id).bind(shard.input_id).bind(shard.part_ordinal).bind(shard.lease_token).bind(&compiled.compiled_sha256).bind(epoch_ms).execute(&mut *tx).await?;
        sqlx::query("SELECT ngkg_agents.finish_prompt_compilation_claim($1,$2,$3,$4,'COMPLETED')")
            .bind(shard.tenant_id)
            .bind(shard.input_id)
            .bind(shard.part_ordinal)
            .bind(shard.lease_token)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    /// Atomically publish deterministic aggregate roots after every part shard
    /// is complete. Concurrent workers are harmless: only one state CAS wins.
    pub async fn finalize_compilation(
        &self,
        tenant_id: Uuid,
        input_id: Uuid,
        epoch_ms: i64,
    ) -> Result<bool, RepositoryError> {
        let mut tx = self.tenant_tx(tenant_id).await?;
        let locked=sqlx::query("SELECT state FROM ngkg_agents.prompt_input WHERE tenant_id=$1 AND input_id=$2 FOR UPDATE").bind(tenant_id).bind(input_id).fetch_one(&mut *tx).await?;
        let state: String = locked.try_get("state")?;
        if state == "COMPILED" {
            tx.commit().await?;
            return Ok(true);
        }
        if state != "COMPILING" {
            return Err(RepositoryError::State);
        }
        let pending:i64=sqlx::query_scalar("SELECT count(*) FROM ngkg_agents.prompt_compilation_shard WHERE tenant_id=$1 AND input_id=$2 AND state<>'COMPLETED'").bind(tenant_id).bind(input_id).fetch_one(&mut *tx).await?;
        if pending > 0 {
            tx.commit().await?;
            return Ok(false);
        }
        let shard_hashes:Vec<Vec<u8>>=sqlx::query_scalar("SELECT compiled_sha256 FROM ngkg_agents.prompt_compilation_shard WHERE tenant_id=$1 AND input_id=$2 ORDER BY part_ordinal").bind(tenant_id).bind(input_id).fetch_all(&mut *tx).await?;
        let requirement_hashes:Vec<Vec<u8>>=sqlx::query_scalar("SELECT text_sha256 FROM ngkg_agents.prompt_requirement WHERE tenant_id=$1 AND input_id=$2 ORDER BY part_ordinal,byte_start,requirement_id").bind(tenant_id).bind(input_id).fetch_all(&mut *tx).await?;
        let compiled_root = hash_list(b"ngkg-prompt-compiled-root-v1\0", &shard_hashes);
        let requirement_root = hash_list(b"ngkg-prompt-requirement-root-v1\0", &requirement_hashes);
        sqlx::query("UPDATE ngkg_agents.prompt_input SET state='COMPILED',state_version=state_version+1,compiled_root_sha256=$3,requirement_root_sha256=$4,completed_at_epoch_ms=$5 WHERE tenant_id=$1 AND input_id=$2 AND state='COMPILING'").bind(tenant_id).bind(input_id).bind(compiled_root).bind(requirement_root).bind(epoch_ms).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(true)
    }

    pub async fn fail_shard(
        &self,
        shard: &ClaimedShard,
        failure_code: &str,
    ) -> Result<(), RepositoryError> {
        let mut tx = self.tenant_tx(shard.tenant_id).await?;
        sqlx::query("UPDATE ngkg_agents.prompt_compilation_shard SET state='FAILED',failure_code=$5,lease_owner=NULL,lease_token=NULL,lease_expires_at_epoch_ms=NULL WHERE tenant_id=$1 AND input_id=$2 AND part_ordinal=$3 AND lease_token=$4").bind(shard.tenant_id).bind(shard.input_id).bind(shard.part_ordinal).bind(shard.lease_token).bind(failure_code).execute(&mut *tx).await?;
        sqlx::query("SELECT ngkg_agents.finish_prompt_compilation_claim($1,$2,$3,$4,'FAILED')")
            .bind(shard.tenant_id)
            .bind(shard.input_id)
            .bind(shard.part_ordinal)
            .bind(shard.lease_token)
            .execute(&mut *tx)
            .await?;
        sqlx::query("UPDATE ngkg_agents.prompt_input SET state='FAILED',state_version=state_version+1,failure_code=$3 WHERE tenant_id=$1 AND input_id=$2 AND state='COMPILING'").bind(shard.tenant_id).bind(shard.input_id).bind(failure_code).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(())
    }

    async fn tenant_tx(
        &self,
        tenant_id: Uuid,
    ) -> Result<sqlx::Transaction<'_, sqlx::Postgres>, RepositoryError> {
        if tenant_id.is_nil() {
            return Err(RepositoryError::Tenant);
        }
        let mut tx = self.pool.begin().await?;
        sqlx::query("SELECT set_config('ngkg.tenant_id',$1,true)")
            .bind(tenant_id.to_string())
            .execute(&mut *tx)
            .await?;
        Ok(tx)
    }
}

async fn insert_chunk(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    s: &ClaimedShard,
    c: &PromptChunk,
) -> Result<(), RepositoryError> {
    sqlx::query("INSERT INTO ngkg_agents.prompt_chunk (tenant_id,input_id,part_ordinal,chunk_id,ordinal,byte_start,byte_end,heading_path,text_sha256) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,decode($9,'hex')) ON CONFLICT DO NOTHING").bind(s.tenant_id).bind(s.input_id).bind(s.part_ordinal).bind(&c.chunk_id).bind(i32::try_from(c.ordinal).map_err(|_|RepositoryError::Limit)?).bind(i64::try_from(c.byte_start).map_err(|_|RepositoryError::Limit)?).bind(i64::try_from(c.byte_end).map_err(|_|RepositoryError::Limit)?).bind(serde_json::to_value(&c.heading_path)?).bind(&c.text_sha256).execute(&mut **tx).await?;
    Ok(())
}
async fn insert_requirement(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    s: &ClaimedShard,
    r: &PromptRequirement,
) -> Result<(), RepositoryError> {
    sqlx::query("INSERT INTO ngkg_agents.prompt_requirement (tenant_id,input_id,requirement_id,part_ordinal,source_chunk_id,kind,mandatory,byte_start,byte_end,normalized_text,text_sha256) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,decode($11,'hex')) ON CONFLICT DO NOTHING").bind(s.tenant_id).bind(s.input_id).bind(&r.requirement_id).bind(s.part_ordinal).bind(&r.source_chunk_id).bind(format!("{:?}",r.kind).to_ascii_uppercase()).bind(r.mandatory).bind(i64::try_from(r.byte_start).map_err(|_|RepositoryError::Limit)?).bind(i64::try_from(r.byte_end).map_err(|_|RepositoryError::Limit)?).bind(&r.normalized_text).bind(&r.text_sha256).execute(&mut **tx).await?;
    Ok(())
}
fn source_root(parts: impl Iterator<Item = (i32, i64, String)>) -> String {
    use sha2::{Digest, Sha256};
    let mut d = Sha256::new();
    d.update(b"ngkg-prompt-source-root-v1\0");
    for (o, l, h) in parts {
        d.update(o.to_be_bytes());
        d.update(l.to_be_bytes());
        d.update(h.as_bytes());
    }
    hex::encode(d.finalize())
}
fn hash_list(domain: &[u8], values: &[Vec<u8>]) -> Vec<u8> {
    use sha2::{Digest, Sha256};
    let mut d = Sha256::new();
    d.update(domain);
    for value in values {
        d.update(u64::try_from(value.len()).unwrap_or(u64::MAX).to_be_bytes());
        d.update(value);
    }
    d.finalize().to_vec()
}

#[derive(Debug, Error)]
pub enum RepositoryError {
    #[error("database operation failed")]
    Sql(#[from] sqlx::Error),
    #[error("JSON operation failed")]
    Json(#[from] serde_json::Error),
    #[error("tenant is invalid")]
    Tenant,
    #[error("state transition is invalid")]
    State,
    #[error("idempotency conflict")]
    Conflict,
    #[error("input is incomplete")]
    Incomplete,
    #[error("configured limit exceeded")]
    Limit,
    #[error("worker lease was lost")]
    Lease,
}
