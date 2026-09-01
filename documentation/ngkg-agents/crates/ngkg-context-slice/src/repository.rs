use std::time::Duration;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, Postgres, Row, Transaction, postgres::PgPoolOptions};
use uuid::Uuid;

use crate::{SliceError, valid_hash};

#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SliceState {
    Uploading,
    Abandoned,
    Active,
    ExpiredPendingDelete,
    Deleting,
    Deleted,
    Corrupt,
}

impl SliceState {
    fn parse(value: &str) -> Result<Self, SliceError> {
        match value {
            "UPLOADING" => Ok(Self::Uploading),
            "ABANDONED" => Ok(Self::Abandoned),
            "ACTIVE" => Ok(Self::Active),
            "EXPIRED_PENDING_DELETE" => Ok(Self::ExpiredPendingDelete),
            "DELETING" => Ok(Self::Deleting),
            "DELETED" => Ok(Self::Deleted),
            "CORRUPT" => Ok(Self::Corrupt),
            _ => Err(SliceError::Integrity("slice state")),
        }
    }
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct CreateSliceRequest {
    pub dataset_id: Uuid,
    pub snapshot_id: Uuid,
    pub authorized_graph_set_sha256: String,
    pub semantic_result_sha256: String,
    pub media_type: String,
    pub chunk_size_bytes: u64,
    pub expected_total_bytes: u64,
    pub total_triples: u64,
    pub ttl_seconds: u64,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ChunkRecord {
    pub ordinal: u32,
    pub byte_start: u64,
    pub byte_end_exclusive: u64,
    pub chunk_sha256: String,
    #[serde(skip_serializing)]
    #[schemars(skip)]
    pub object_reference: String,
}

#[derive(Clone, Debug, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SliceManifest {
    pub version: &'static str,
    pub slice_id: Uuid,
    pub dataset_id: Uuid,
    pub snapshot_id: Uuid,
    pub authorized_graph_set_sha256: String,
    pub semantic_result_sha256: String,
    pub content_sha256: String,
    pub media_type: String,
    pub total_bytes: u64,
    pub total_triples: u64,
    pub chunks: Vec<ChunkRecord>,
    pub index_sha256: String,
    pub index_bytes: u64,
    pub expires_at_epoch_ms: i64,
}

#[derive(Clone, Debug, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SliceView {
    pub slice_id: Uuid,
    pub subject: String,
    pub dataset_id: Uuid,
    pub snapshot_id: Uuid,
    pub authorized_graph_set_sha256: String,
    pub semantic_result_sha256: String,
    pub media_type: String,
    pub state: SliceState,
    pub state_version: i64,
    pub chunk_size_bytes: u64,
    pub expected_total_bytes: u64,
    pub total_bytes: Option<u64>,
    pub total_triples: u64,
    pub content_sha256: Option<String>,
    pub manifest_sha256: Option<String>,
    pub index_sha256: Option<String>,
    pub index_bytes: Option<u64>,
    pub created_at_epoch_ms: i64,
    pub expires_at_epoch_ms: i64,
    pub delete_after_epoch_ms: i64,
}

#[derive(Clone)]
pub struct SliceRepository {
    pool: PgPool,
}

impl SliceRepository {
    pub async fn connect(
        database_url: &str,
        maximum_connections: u32,
        timeout: Duration,
    ) -> Result<Self, SliceError> {
        let pool = PgPoolOptions::new()
            .max_connections(maximum_connections)
            .acquire_timeout(timeout)
            .connect(database_url)
            .await?;
        sqlx::migrate!("../../migrations-agents").run(&pool).await?;
        Ok(Self { pool })
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn create(
        &self,
        tenant_id: Uuid,
        subject: &str,
        request: &CreateSliceRequest,
        maximum_ttl_seconds: u64,
        recovery_window_seconds: u64,
        kms_key_id_sha256: &str,
        now: i64,
    ) -> Result<SliceView, SliceError> {
        if subject.is_empty()
            || subject.len() > 256
            || request.media_type.is_empty()
            || request.media_type.len() > 128
            || !valid_hash(&request.authorized_graph_set_sha256)
            || !valid_hash(&request.semantic_result_sha256)
            || request.chunk_size_bytes < 65_536
            || request.chunk_size_bytes > 268_435_456
            || request.expected_total_bytes == 0
            || request.expected_total_bytes > 10 * 1024_u64.pow(4)
            || request.ttl_seconds == 0
            || request.ttl_seconds > maximum_ttl_seconds
            || !valid_hash(kms_key_id_sha256)
        {
            return Err(SliceError::Invalid("create slice"));
        }
        let slice_id = Uuid::new_v4();
        let expires = now
            .checked_add(
                i64::try_from(request.ttl_seconds)
                    .map_err(|_| SliceError::Limit)?
                    .saturating_mul(1000),
            )
            .ok_or(SliceError::Limit)?;
        let delete_after = expires
            .checked_add(
                i64::try_from(recovery_window_seconds)
                    .map_err(|_| SliceError::Limit)?
                    .saturating_mul(1000),
            )
            .ok_or(SliceError::Limit)?;
        let mut tx = self.tenant_tx(tenant_id).await?;
        sqlx::query("INSERT INTO ngkg_agents.context_slice(tenant_id,slice_id,subject,dataset_id,snapshot_id,authorized_graph_set_sha256,semantic_result_sha256,media_type,state,state_version,chunk_size_bytes,expected_total_bytes,total_triples,kms_key_id_sha256,created_at_epoch_ms,expires_at_epoch_ms,delete_after_epoch_ms,updated_at_epoch_ms) VALUES($1,$2,$3,$4,$5,decode($6,'hex'),decode($7,'hex'),$8,'UPLOADING',1,$9,$10,$11,decode($12,'hex'),$13,$14,$15,$13)")
            .bind(tenant_id).bind(slice_id).bind(subject).bind(request.dataset_id).bind(request.snapshot_id)
            .bind(&request.authorized_graph_set_sha256).bind(&request.semantic_result_sha256).bind(&request.media_type)
            .bind(i64::try_from(request.chunk_size_bytes).map_err(|_|SliceError::Limit)?)
            .bind(i64::try_from(request.expected_total_bytes).map_err(|_|SliceError::Limit)?)
            .bind(i64::try_from(request.total_triples).map_err(|_|SliceError::Limit)?)
            .bind(kms_key_id_sha256).bind(now).bind(expires).bind(delete_after).execute(&mut *tx).await?;
        sqlx::query("SELECT ngkg_agents.schedule_context_slice_gc($1,$2,$3)")
            .bind(tenant_id)
            .bind(slice_id)
            .bind(delete_after)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        self.get(tenant_id, slice_id).await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn add_chunk(
        &self,
        tenant_id: Uuid,
        slice_id: Uuid,
        ordinal: u32,
        byte_start: u64,
        byte_end: u64,
        digest: &str,
        reference: &str,
        now: i64,
    ) -> Result<(), SliceError> {
        if !valid_hash(digest)
            || reference.is_empty()
            || reference.len() > 2048
            || byte_start >= byte_end
        {
            return Err(SliceError::Invalid("chunk"));
        }
        let mut tx = self.tenant_tx(tenant_id).await?;
        let row=sqlx::query("SELECT state,chunk_size_bytes,expected_total_bytes FROM ngkg_agents.context_slice WHERE tenant_id=$1 AND slice_id=$2 FOR UPDATE").bind(tenant_id).bind(slice_id).fetch_optional(&mut *tx).await?.ok_or(SliceError::NotFound)?;
        if row.try_get::<String, _>("state")? != "UPLOADING" {
            return Err(SliceError::State);
        }
        let chunk_size: u64 = u64::try_from(row.try_get::<i64, _>("chunk_size_bytes")?)
            .map_err(|_| SliceError::Integrity("chunk size"))?;
        let total: u64 = u64::try_from(row.try_get::<i64, _>("expected_total_bytes")?)
            .map_err(|_| SliceError::Integrity("slice size"))?;
        if byte_start
            != u64::from(ordinal)
                .checked_mul(chunk_size)
                .ok_or(SliceError::Limit)?
            || byte_end > total
            || byte_end - byte_start > chunk_size
        {
            return Err(SliceError::Invalid("chunk range"));
        }
        sqlx::query("INSERT INTO ngkg_agents.context_slice_chunk(tenant_id,slice_id,ordinal,byte_start,byte_end_exclusive,chunk_sha256,object_reference,created_at_epoch_ms) VALUES($1,$2,$3,$4,$5,decode($6,'hex'),$7,$8) ON CONFLICT(tenant_id,slice_id,ordinal) DO NOTHING")
            .bind(tenant_id).bind(slice_id).bind(i32::try_from(ordinal).map_err(|_|SliceError::Limit)?)
            .bind(i64::try_from(byte_start).map_err(|_|SliceError::Limit)?).bind(i64::try_from(byte_end).map_err(|_|SliceError::Limit)?)
            .bind(digest).bind(reference).bind(now).execute(&mut *tx).await?;
        let matches:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM ngkg_agents.context_slice_chunk WHERE tenant_id=$1 AND slice_id=$2 AND ordinal=$3 AND byte_start=$4 AND byte_end_exclusive=$5 AND chunk_sha256=decode($6,'hex') AND object_reference=$7)")
            .bind(tenant_id).bind(slice_id).bind(i32::try_from(ordinal).map_err(|_|SliceError::Limit)?).bind(i64::try_from(byte_start).map_err(|_|SliceError::Limit)?).bind(i64::try_from(byte_end).map_err(|_|SliceError::Limit)?).bind(digest).bind(reference).fetch_one(&mut *tx).await?;
        if !matches {
            return Err(SliceError::Integrity("chunk idempotency collision"));
        }
        tx.commit().await?;
        Ok(())
    }

    pub async fn chunks(
        &self,
        tenant_id: Uuid,
        slice_id: Uuid,
    ) -> Result<Vec<ChunkRecord>, SliceError> {
        let mut tx = self.tenant_tx(tenant_id).await?;
        let rows=sqlx::query("SELECT ordinal,byte_start,byte_end_exclusive,encode(chunk_sha256,'hex') digest,object_reference FROM ngkg_agents.context_slice_chunk WHERE tenant_id=$1 AND slice_id=$2 ORDER BY ordinal").bind(tenant_id).bind(slice_id).fetch_all(&mut *tx).await?;
        tx.commit().await?;
        rows.into_iter()
            .map(|row| {
                Ok(ChunkRecord {
                    ordinal: u32::try_from(row.try_get::<i32, _>("ordinal")?)
                        .map_err(|_| SliceError::Integrity("ordinal"))?,
                    byte_start: u64::try_from(row.try_get::<i64, _>("byte_start")?)
                        .map_err(|_| SliceError::Integrity("start"))?,
                    byte_end_exclusive: u64::try_from(row.try_get::<i64, _>("byte_end_exclusive")?)
                        .map_err(|_| SliceError::Integrity("end"))?,
                    chunk_sha256: row.try_get("digest")?,
                    object_reference: row.try_get("object_reference")?,
                })
            })
            .collect()
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn activate(
        &self,
        tenant_id: Uuid,
        slice_id: Uuid,
        content: &str,
        manifest: &str,
        manifest_ref: &str,
        index: &str,
        index_bytes: u64,
        index_ref: &str,
        total: u64,
        now: i64,
    ) -> Result<SliceView, SliceError> {
        if !valid_hash(content) || !valid_hash(manifest) || !valid_hash(index) {
            return Err(SliceError::Checksum);
        }
        let mut tx = self.tenant_tx(tenant_id).await?;
        let changed=sqlx::query("UPDATE ngkg_agents.context_slice SET state='ACTIVE',state_version=state_version+1,total_bytes=$3,content_sha256=decode($4,'hex'),manifest_sha256=decode($5,'hex'),manifest_object_reference=$6,index_sha256=decode($7,'hex'),index_bytes=$8,index_object_reference=$9,updated_at_epoch_ms=$10 WHERE tenant_id=$1 AND slice_id=$2 AND state='UPLOADING'")
            .bind(tenant_id).bind(slice_id).bind(i64::try_from(total).map_err(|_|SliceError::Limit)?).bind(content).bind(manifest).bind(manifest_ref).bind(index).bind(i64::try_from(index_bytes).map_err(|_|SliceError::Limit)?).bind(index_ref).bind(now).execute(&mut *tx).await?;
        if changed.rows_affected() != 1 {
            return Err(SliceError::State);
        }
        tx.commit().await?;
        self.get(tenant_id, slice_id).await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn record_capability(
        &self,
        tenant_id: Uuid,
        slice_id: Uuid,
        subject: &str,
        audience: &str,
        nonce: Uuid,
        start: u64,
        end: u64,
        token_sha: &[u8],
        policy_sha: &str,
        issued: i64,
        expires: i64,
    ) -> Result<Uuid, SliceError> {
        let id = Uuid::new_v4();
        let mut tx = self.tenant_tx(tenant_id).await?;
        let changed=sqlx::query("INSERT INTO ngkg_agents.context_slice_capability(tenant_id,capability_id,slice_id,subject,audience,nonce,range_start,range_end_exclusive,token_sha256,policy_version_sha256,issued_at_epoch_ms,expires_at_epoch_ms) SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,decode($10,'hex'),$11,$12 FROM ngkg_agents.context_slice WHERE tenant_id=$1 AND slice_id=$3 AND state='ACTIVE' AND expires_at_epoch_ms>=$12")
            .bind(tenant_id).bind(id).bind(slice_id).bind(subject).bind(audience).bind(nonce).bind(i64::try_from(start).map_err(|_|SliceError::Limit)?).bind(i64::try_from(end).map_err(|_|SliceError::Limit)?).bind(token_sha).bind(policy_sha).bind(issued).bind(expires).execute(&mut *tx).await?;
        if changed.rows_affected() != 1 {
            return Err(SliceError::State);
        }
        tx.commit().await?;
        Ok(id)
    }

    pub async fn capability_valid(
        &self,
        tenant: Uuid,
        slice: Uuid,
        nonce: Uuid,
        token_sha: &[u8],
        now: i64,
    ) -> Result<bool, SliceError> {
        let mut tx = self.tenant_tx(tenant).await?;
        let valid:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM ngkg_agents.context_slice_capability c JOIN ngkg_agents.context_slice s USING(tenant_id,slice_id) WHERE c.tenant_id=$1 AND c.slice_id=$2 AND c.nonce=$3 AND c.token_sha256=$4 AND c.revoked_at_epoch_ms IS NULL AND c.expires_at_epoch_ms>$5 AND s.state='ACTIVE' AND s.expires_at_epoch_ms>$5)").bind(tenant).bind(slice).bind(nonce).bind(token_sha).bind(now).fetch_one(&mut *tx).await?;
        tx.commit().await?;
        Ok(valid)
    }

    pub async fn mark_expired(
        &self,
        tenant: Uuid,
        slice: Uuid,
        now: i64,
    ) -> Result<SliceView, SliceError> {
        let mut tx = self.tenant_tx(tenant).await?;
        sqlx::query("UPDATE ngkg_agents.context_slice SET state='EXPIRED_PENDING_DELETE',state_version=state_version+1,updated_at_epoch_ms=$3 WHERE tenant_id=$1 AND slice_id=$2 AND state='ACTIVE'").bind(tenant).bind(slice).bind(now).execute(&mut *tx).await?;
        tx.commit().await?;
        self.get(tenant, slice).await
    }

    pub async fn get(&self, tenant_id: Uuid, slice_id: Uuid) -> Result<SliceView, SliceError> {
        let mut tx = self.tenant_tx(tenant_id).await?;
        let row=sqlx::query("SELECT *,encode(authorized_graph_set_sha256,'hex') graph_sha,encode(semantic_result_sha256,'hex') result_sha,encode(content_sha256,'hex') content_hex,encode(manifest_sha256,'hex') manifest_hex,encode(index_sha256,'hex') index_hex FROM ngkg_agents.context_slice WHERE tenant_id=$1 AND slice_id=$2").bind(tenant_id).bind(slice_id).fetch_optional(&mut *tx).await?.ok_or(SliceError::NotFound)?;
        tx.commit().await?;
        row_to_view(&row)
    }

    pub async fn internal_refs(
        &self,
        tenant: Uuid,
        slice: Uuid,
    ) -> Result<Vec<String>, SliceError> {
        let mut tx = self.tenant_tx(tenant).await?;
        let mut refs=sqlx::query_scalar::<_,String>("SELECT object_reference FROM ngkg_agents.context_slice_chunk WHERE tenant_id=$1 AND slice_id=$2 ORDER BY ordinal").bind(tenant).bind(slice).fetch_all(&mut *tx).await?;
        let row=sqlx::query("SELECT manifest_object_reference,index_object_reference FROM ngkg_agents.context_slice WHERE tenant_id=$1 AND slice_id=$2 AND state='DELETING'").bind(tenant).bind(slice).fetch_one(&mut *tx).await?;
        if let Some(value) = row.try_get::<Option<String>, _>("manifest_object_reference")? {
            refs.push(value);
        }
        if let Some(value) = row.try_get::<Option<String>, _>("index_object_reference")? {
            refs.push(value);
        }
        tx.commit().await?;
        Ok(refs)
    }

    pub async fn index_material(
        &self,
        tenant: Uuid,
        slice: Uuid,
    ) -> Result<(String, String, usize), SliceError> {
        let mut tx = self.tenant_tx(tenant).await?;
        let row=sqlx::query("SELECT index_object_reference,encode(index_sha256,'hex') index_sha,index_bytes FROM ngkg_agents.context_slice WHERE tenant_id=$1 AND slice_id=$2 AND state='ACTIVE'").bind(tenant).bind(slice).fetch_optional(&mut *tx).await?.ok_or(SliceError::NotFound)?;
        tx.commit().await?;
        let reference = row
            .try_get::<Option<String>, _>("index_object_reference")?
            .ok_or(SliceError::Integrity("index reference"))?;
        let digest = row
            .try_get::<Option<String>, _>("index_sha")?
            .ok_or(SliceError::Integrity("index digest"))?;
        let length = usize::try_from(
            row.try_get::<Option<i64>, _>("index_bytes")?
                .ok_or(SliceError::Integrity("index length"))?,
        )
        .map_err(|_| SliceError::Limit)?;
        Ok((reference, digest, length))
    }

    pub async fn claim_gc(
        &self,
        worker: &str,
        lease_ms: i64,
        now: i64,
    ) -> Result<Option<(Uuid, Uuid, Uuid)>, SliceError> {
        let token = Uuid::new_v4();
        let row = sqlx::query("SELECT * FROM ngkg_agents.claim_context_slice_gc($1,$2,$3,$4)")
            .bind(worker)
            .bind(token)
            .bind(lease_ms)
            .bind(now)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row
            .map(|r| {
                Ok::<(Uuid, Uuid, Uuid), sqlx::Error>((
                    r.try_get("tenant_id")?,
                    r.try_get("slice_id")?,
                    token,
                ))
            })
            .transpose()?)
    }

    pub async fn finish_gc(
        &self,
        tenant: Uuid,
        slice: Uuid,
        token: Uuid,
        evidence: &str,
        count: i32,
        now: i64,
    ) -> Result<(), SliceError> {
        let mut tx = self.tenant_tx(tenant).await?;
        sqlx::query(
            "SELECT ngkg_agents.finish_context_slice_gc($1,$2,$3,$4,decode($5,'hex'),$6,$7)",
        )
        .bind(tenant)
        .bind(slice)
        .bind(token)
        .bind(Uuid::new_v4())
        .bind(evidence)
        .bind(count)
        .bind(now)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn tenant_tx(&self, tenant: Uuid) -> Result<Transaction<'_, Postgres>, SliceError> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("SELECT set_config('ngkg.tenant_id',$1,true)")
            .bind(tenant.to_string())
            .execute(&mut *tx)
            .await?;
        Ok(tx)
    }
}

fn row_to_view(row: &sqlx::postgres::PgRow) -> Result<SliceView, SliceError> {
    Ok(SliceView {
        slice_id: row.try_get("slice_id")?,
        subject: row.try_get("subject")?,
        dataset_id: row.try_get("dataset_id")?,
        snapshot_id: row.try_get("snapshot_id")?,
        authorized_graph_set_sha256: row.try_get("graph_sha")?,
        semantic_result_sha256: row.try_get("result_sha")?,
        media_type: row.try_get("media_type")?,
        state: SliceState::parse(&row.try_get::<String, _>("state")?)?,
        state_version: row.try_get("state_version")?,
        chunk_size_bytes: u64::try_from(row.try_get::<i64, _>("chunk_size_bytes")?)
            .map_err(|_| SliceError::Integrity("chunk size"))?,
        expected_total_bytes: u64::try_from(row.try_get::<i64, _>("expected_total_bytes")?)
            .map_err(|_| SliceError::Integrity("total size"))?,
        total_bytes: row
            .try_get::<Option<i64>, _>("total_bytes")?
            .map(u64::try_from)
            .transpose()
            .map_err(|_| SliceError::Integrity("total bytes"))?,
        total_triples: u64::try_from(row.try_get::<i64, _>("total_triples")?)
            .map_err(|_| SliceError::Integrity("triple count"))?,
        content_sha256: row.try_get("content_hex")?,
        manifest_sha256: row.try_get("manifest_hex")?,
        index_sha256: row.try_get("index_hex")?,
        index_bytes: row
            .try_get::<Option<i64>, _>("index_bytes")?
            .map(u64::try_from)
            .transpose()
            .map_err(|_| SliceError::Integrity("index bytes"))?,
        created_at_epoch_ms: row.try_get("created_at_epoch_ms")?,
        expires_at_epoch_ms: row.try_get("expires_at_epoch_ms")?,
        delete_after_epoch_ms: row.try_get("delete_after_epoch_ms")?,
    })
}
