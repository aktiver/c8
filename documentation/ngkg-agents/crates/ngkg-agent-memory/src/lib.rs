//! Evidence-bound five-class agent memory above the public NGKG API.
//! Models may propose memory; NGKG entailment and an externally qualified,
//! atomically published snapshot remain authoritative for semantic memory.

#![allow(missing_docs)]

use http::HeaderValue;
use ngkg_agent_catalog::Hash32;
use ngkg_api_client::{NgkgQueryClient, QueryRequest};
use ngkg_mcp_contracts::{
    EnvelopeLimits, EnvelopeQueryForm, SemanticPayload, SemanticStatus,
    build_reasoned_context_envelope,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Postgres, Row, Transaction, postgres::PgPoolOptions};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use thiserror::Error;
use uuid::Uuid;

const CONTENT_DOMAIN: &[u8] = b"ngkg-agent-memory-content-v1\0";
const IDEMPOTENCY_DOMAIN: &[u8] = b"ngkg-agent-memory-idempotency-v1\0";
const EVIDENCE_DOMAIN: &[u8] = b"ngkg-agent-memory-evidence-v1\0";

#[derive(Clone, Copy, Debug)]
pub struct MemoryLimits {
    pub maximum_content_bytes: usize,
    pub maximum_search_results: i64,
    pub maximum_statements: usize,
    pub maximum_working_ttl: Duration,
    pub maximum_retention_days: i32,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum MemoryClass {
    Working,
    Episodic,
    Semantic,
    Procedural,
    Evidence,
}
impl MemoryClass {
    fn as_str(self) -> &'static str {
        match self {
            Self::Working => "WORKING",
            Self::Episodic => "EPISODIC",
            Self::Semantic => "SEMANTIC",
            Self::Procedural => "PROCEDURAL",
            Self::Evidence => "EVIDENCE",
        }
    }
    fn parse(value: &str) -> Result<Self, MemoryError> {
        match value {
            "WORKING" => Ok(Self::Working),
            "EPISODIC" => Ok(Self::Episodic),
            "SEMANTIC" => Ok(Self::Semantic),
            "PROCEDURAL" => Ok(Self::Procedural),
            "EVIDENCE" => Ok(Self::Evidence),
            _ => Err(MemoryError::Evidence),
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum MemoryAudience {
    Owner,
    Tenant,
}
impl MemoryAudience {
    fn as_str(self) -> &'static str {
        match self {
            Self::Owner => "OWNER",
            Self::Tenant => "TENANT",
        }
    }
    fn parse(value: &str) -> Result<Self, MemoryError> {
        match value {
            "OWNER" => Ok(Self::Owner),
            "TENANT" => Ok(Self::Tenant),
            _ => Err(MemoryError::Evidence),
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum MemoryState {
    Proposed,
    Validating,
    Validated,
    Entailed,
    Contradicted,
    Unknown,
    ApprovalRequired,
    Approved,
    Published,
    Superseded,
    Revoked,
    Rejected,
    Expired,
}
impl MemoryState {
    fn as_str(self) -> &'static str {
        match self {
            Self::Proposed => "PROPOSED",
            Self::Validating => "VALIDATING",
            Self::Validated => "VALIDATED",
            Self::Entailed => "ENTAILED",
            Self::Contradicted => "CONTRADICTED",
            Self::Unknown => "UNKNOWN",
            Self::ApprovalRequired => "APPROVAL_REQUIRED",
            Self::Approved => "APPROVED",
            Self::Published => "PUBLISHED",
            Self::Superseded => "SUPERSEDED",
            Self::Revoked => "REVOKED",
            Self::Rejected => "REJECTED",
            Self::Expired => "EXPIRED",
        }
    }
    fn parse(value: &str) -> Result<Self, MemoryError> {
        match value {
            "PROPOSED" => Ok(Self::Proposed),
            "VALIDATING" => Ok(Self::Validating),
            "VALIDATED" => Ok(Self::Validated),
            "ENTAILED" => Ok(Self::Entailed),
            "CONTRADICTED" => Ok(Self::Contradicted),
            "UNKNOWN" => Ok(Self::Unknown),
            "APPROVAL_REQUIRED" => Ok(Self::ApprovalRequired),
            "APPROVED" => Ok(Self::Approved),
            "PUBLISHED" => Ok(Self::Published),
            "SUPERSEDED" => Ok(Self::Superseded),
            "REVOKED" => Ok(Self::Revoked),
            "REJECTED" => Ok(Self::Rejected),
            "EXPIRED" => Ok(Self::Expired),
            _ => Err(MemoryError::Evidence),
        }
    }
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ProposeMemoryRequest {
    pub memory_class: MemoryClass,
    pub audience: MemoryAudience,
    pub content_type: String,
    pub content: String,
    #[serde(default)]
    pub source_execution_id: Option<Uuid>,
    #[serde(default)]
    pub dataset_id: Option<Uuid>,
    #[serde(default)]
    pub snapshot_id: Option<Uuid>,
    #[serde(default)]
    pub authorized_graph_set_sha256: Option<String>,
    #[serde(default)]
    pub semantic_result_sha256: Option<String>,
    #[serde(default)]
    pub answer_certificate_sha256: Option<String>,
    #[serde(default)]
    pub provenance: serde_json::Value,
    pub retention_days: i32,
    #[serde(default)]
    pub legal_hold: bool,
    #[serde(default)]
    pub expires_at_epoch_ms: Option<i64>,
    pub idempotency_key: String,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct MemorySearchRequest {
    #[serde(default)]
    pub query: String,
    #[serde(default)]
    pub memory_class: Option<MemoryClass>,
    #[serde(default = "default_limit")]
    pub limit: i64,
}
fn default_limit() -> i64 {
    20
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct MemoryPublishRequest {
    pub ngkg_operation_id: Uuid,
    pub published_snapshot_id: Uuid,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct MemorySupersedeRequest {
    pub superseding_memory_id: Uuid,
    pub reason_code: String,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct MemoryReasonRequest {
    pub reason_code: String,
}

#[derive(Clone, Debug, JsonSchema, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct MemoryView {
    pub memory_id: Uuid,
    pub memory_class: MemoryClass,
    pub audience: MemoryAudience,
    pub state: MemoryState,
    pub state_version: i64,
    pub version: i64,
    pub owner_subject: String,
    pub content_type: String,
    pub content: String,
    pub content_sha256: String,
    pub source_execution_id: Option<Uuid>,
    pub dataset_id: Option<Uuid>,
    pub snapshot_id: Option<Uuid>,
    pub authorized_graph_set_sha256: Option<String>,
    pub semantic_result_sha256: Option<String>,
    pub answer_certificate_sha256: Option<String>,
    pub provenance: serde_json::Value,
    pub retention_days: i32,
    pub legal_hold: bool,
    pub expires_at_epoch_ms: Option<i64>,
    pub created_at_epoch_ms: i64,
    pub updated_at_epoch_ms: i64,
}

#[derive(Clone, Debug, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MemoryValidationReceipt {
    pub memory: MemoryView,
    pub query_execution_ids: Vec<Uuid>,
    pub proof_support_ids: Vec<String>,
    pub evidence_sha256: String,
}

#[derive(Clone, Debug, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MemoryPublicationReceipt {
    pub publication_id: Uuid,
    pub memory: MemoryView,
    pub ngkg_operation_id: Uuid,
    pub published_snapshot_id: Uuid,
    pub query_execution_ids: Vec<Uuid>,
    pub proof_support_ids: Vec<String>,
    pub evidence_sha256: String,
}

#[derive(Clone, Debug, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MemoryTransitionView {
    pub transition_id: Uuid,
    pub state_version: i64,
    pub from_state: MemoryState,
    pub to_state: MemoryState,
    pub actor: String,
    pub reason_code: String,
    pub evidence_sha256: String,
    pub query_execution_ids: Vec<Uuid>,
    pub proof_support_ids: Vec<String>,
    pub created_at_epoch_ms: i64,
}

#[derive(Clone, Debug, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MemoryEdgeView {
    pub edge_id: Uuid,
    pub source_memory_id: Uuid,
    pub source_version: i64,
    pub target_memory_id: Uuid,
    pub target_version: i64,
    pub edge_type: String,
    pub evidence_sha256: String,
    pub created_at_epoch_ms: i64,
}

#[derive(Clone, Debug, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MemoryExplanation {
    pub memory: MemoryView,
    pub transitions: Vec<MemoryTransitionView>,
    pub edges: Vec<MemoryEdgeView>,
    pub inclusion_rule: String,
}

#[derive(Clone)]
pub struct MemoryService {
    pool: PgPool,
    query: NgkgQueryClient,
    envelope_limits: EnvelopeLimits,
    limits: MemoryLimits,
}

impl MemoryService {
    pub async fn connect(
        database_url: &str,
        maximum_connections: u32,
        acquire_timeout: Duration,
        query: NgkgQueryClient,
        envelope_limits: EnvelopeLimits,
        limits: MemoryLimits,
    ) -> Result<Self, MemoryError> {
        if maximum_connections == 0
            || limits.maximum_content_bytes == 0
            || limits.maximum_content_bytes > 1_048_576
            || limits.maximum_search_results < 1
            || limits.maximum_statements == 0
            || limits.maximum_retention_days < 1
        {
            return Err(MemoryError::Invalid);
        }
        let pool = PgPoolOptions::new()
            .max_connections(maximum_connections)
            .acquire_timeout(acquire_timeout)
            .connect(database_url)
            .await?;
        Ok(Self {
            pool,
            query,
            envelope_limits,
            limits,
        })
    }

    pub async fn ready(&self) -> Result<(), MemoryError> {
        let ready:bool=sqlx::query_scalar("SELECT COALESCE((SELECT relrowsecurity AND relforcerowsecurity FROM pg_class WHERE oid=to_regclass('ngkg_agents.agent_memory')),false)").fetch_one(&self.pool).await?;
        if !ready {
            return Err(MemoryError::Evidence);
        }
        Ok(())
    }

    pub async fn propose(
        &self,
        tenant_id: Uuid,
        subject: &str,
        request: ProposeMemoryRequest,
    ) -> Result<MemoryView, MemoryError> {
        validate_proposal(&request, self.limits)?;
        let now = epoch_ms()?;
        let idempotency = idempotency_hash(tenant_id, subject, &request.idempotency_key);
        let request_sha = domain_hash(
            b"ngkg-agent-memory-proposal-v1\0",
            &serde_json::to_vec(&request)?,
        );
        let content_sha = domain_hash(CONTENT_DOMAIN, request.content.as_bytes());
        let mut tx = self.tenant_tx(tenant_id).await?;
        if let Some(row)=sqlx::query("SELECT memory_id,idempotency_request_sha256 FROM ngkg_agents.agent_memory WHERE tenant_id=$1 AND idempotency_sha256=$2").bind(tenant_id).bind(idempotency.0.as_slice()).fetch_optional(&mut *tx).await?{let memory_id:Uuid=row.try_get("memory_id")?;if hash_vec(row.try_get("idempotency_request_sha256")?)?!=request_sha{return Err(MemoryError::Conflict)}tx.commit().await?;return self.get(tenant_id,subject,memory_id).await}
        if matches!(request.memory_class, MemoryClass::Semantic) {
            self.verify_semantic_source(&mut tx, tenant_id, &request)
                .await?;
        } else if let Some(execution_id) = request.source_execution_id {
            let exists:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM ngkg_agents.agent_execution WHERE tenant_id=$1 AND execution_id=$2)").bind(tenant_id).bind(execution_id).fetch_one(&mut *tx).await?;
            if !exists {
                return Err(MemoryError::Evidence);
            }
        }
        let memory_id = Uuid::new_v4();
        sqlx::query("INSERT INTO ngkg_agents.agent_memory(tenant_id,memory_id,memory_class,owner_subject,audience,state,state_version,current_version,retention_days,legal_hold,expires_at_epoch_ms,idempotency_sha256,idempotency_request_sha256,created_at_epoch_ms,updated_at_epoch_ms) VALUES($1,$2,$3,$4,$5,'PROPOSED',0,1,$6,$7,$8,$9,$10,$11,$11)").bind(tenant_id).bind(memory_id).bind(request.memory_class.as_str()).bind(subject).bind(request.audience.as_str()).bind(request.retention_days).bind(request.legal_hold).bind(request.expires_at_epoch_ms).bind(idempotency.0.as_slice()).bind(request_sha.0.as_slice()).bind(now).execute(&mut *tx).await?;
        sqlx::query("INSERT INTO ngkg_agents.agent_memory_version(tenant_id,memory_id,version,content_type,content,content_sha256,source_execution_id,dataset_id,snapshot_id,authorized_graph_set_sha256,semantic_result_sha256,answer_certificate_sha256,provenance,created_by,created_at_epoch_ms) VALUES($1,$2,1,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)").bind(tenant_id).bind(memory_id).bind(&request.content_type).bind(&request.content).bind(content_sha.0.as_slice()).bind(request.source_execution_id).bind(request.dataset_id).bind(request.snapshot_id).bind(optional_hash_bytes(&request.authorized_graph_set_sha256)?).bind(optional_hash_bytes(&request.semantic_result_sha256)?).bind(optional_hash_bytes(&request.answer_certificate_sha256)?).bind(&request.provenance).bind(subject).bind(now).execute(&mut *tx).await?;
        tx.commit().await?;
        self.get(tenant_id, subject, memory_id).await
    }

    pub async fn get(
        &self,
        tenant_id: Uuid,
        subject: &str,
        memory_id: Uuid,
    ) -> Result<MemoryView, MemoryError> {
        let mut tx = self.tenant_tx(tenant_id).await?;
        let row = sqlx::query(MEMORY_SELECT)
            .bind(tenant_id)
            .bind(memory_id)
            .fetch_one(&mut *tx)
            .await?;
        let view = row_to_memory(&row)?;
        authorize_view(&view, subject)?;
        tx.commit().await?;
        Ok(view)
    }

    pub async fn search(
        &self,
        tenant_id: Uuid,
        subject: &str,
        request: MemorySearchRequest,
    ) -> Result<Vec<MemoryView>, MemoryError> {
        if request.query.len() > 4096
            || request.limit < 1
            || request.limit > self.limits.maximum_search_results
        {
            return Err(MemoryError::Invalid);
        }
        let class = request.memory_class.map(MemoryClass::as_str);
        let now = epoch_ms()?;
        let mut tx = self.tenant_tx(tenant_id).await?;
        let rows=sqlx::query("SELECT m.memory_id,m.memory_class,m.owner_subject,m.audience,m.state,m.state_version,m.current_version,m.retention_days,m.legal_hold,m.expires_at_epoch_ms,m.created_at_epoch_ms,m.updated_at_epoch_ms,v.content_type,v.content,v.content_sha256,v.source_execution_id,v.dataset_id,v.snapshot_id,v.authorized_graph_set_sha256,v.semantic_result_sha256,v.answer_certificate_sha256,v.provenance FROM ngkg_agents.agent_memory m JOIN ngkg_agents.agent_memory_version v ON v.tenant_id=m.tenant_id AND v.memory_id=m.memory_id AND v.version=m.current_version WHERE m.tenant_id=$1 AND (m.audience='TENANT' OR m.owner_subject=$2) AND ($3::text IS NULL OR m.memory_class=$3) AND ((m.memory_class='SEMANTIC' AND m.state='PUBLISHED') OR (m.memory_class<>'SEMANTIC' AND m.state IN ('VALIDATED','APPROVED','PUBLISHED'))) AND (m.expires_at_epoch_ms IS NULL OR m.expires_at_epoch_ms>$4) AND ($5='' OR v.search_vector @@ plainto_tsquery('simple',$5)) ORDER BY CASE WHEN $5='' THEN 0 ELSE ts_rank_cd(v.search_vector,plainto_tsquery('simple',$5)) END DESC,m.updated_at_epoch_ms DESC,m.memory_id LIMIT $6").bind(tenant_id).bind(subject).bind(class).bind(now).bind(request.query.trim()).bind(request.limit).fetch_all(&mut *tx).await?;
        tx.commit().await?;
        rows.iter().map(row_to_memory).collect()
    }

    pub async fn validate(
        &self,
        tenant_id: Uuid,
        subject: &str,
        authorization: &HeaderValue,
        memory_id: Uuid,
        request_id: &str,
    ) -> Result<MemoryValidationReceipt, MemoryError> {
        let mut memory = self.get(tenant_id, subject, memory_id).await?;
        if memory.state == MemoryState::Proposed {
            let started = memory_evidence(&memory, MemoryState::Validating, &[], &[]);
            memory = self
                .transition(
                    tenant_id,
                    subject,
                    &memory,
                    MemoryState::Validating,
                    "VALIDATION_STARTED",
                    started,
                    Vec::new(),
                    Vec::new(),
                )
                .await?;
        } else if memory.state != MemoryState::Validating {
            return Err(MemoryError::State);
        }
        if memory.memory_class != MemoryClass::Semantic {
            let evidence = memory_evidence(&memory, MemoryState::Validated, &[], &[]);
            let final_memory = self
                .transition(
                    tenant_id,
                    subject,
                    &memory,
                    MemoryState::Validated,
                    "STRUCTURAL_VALIDATION_PASSED",
                    evidence,
                    Vec::new(),
                    Vec::new(),
                )
                .await?;
            return Ok(MemoryValidationReceipt {
                memory: final_memory,
                query_execution_ids: Vec::new(),
                proof_support_ids: Vec::new(),
                evidence_sha256: evidence.to_lower_hex(),
            });
        }
        let snapshot = memory.snapshot_id.ok_or(MemoryError::Evidence)?;
        let dataset = memory.dataset_id.ok_or(MemoryError::Evidence)?;
        let expected_graph = memory
            .authorized_graph_set_sha256
            .clone()
            .ok_or(MemoryError::Evidence)?;
        let (entailed, queries, proofs) = self
            .entail_all(
                authorization,
                dataset,
                snapshot,
                &expected_graph,
                &memory.content,
                request_id,
            )
            .await?;
        let state = if entailed {
            MemoryState::Entailed
        } else {
            MemoryState::Unknown
        };
        let reason = if entailed {
            "OWL2_DL_ENTAILED"
        } else {
            "OPEN_WORLD_UNKNOWN"
        };
        let evidence = memory_evidence(&memory, state, &queries, &proofs);
        let mut final_memory = self
            .transition(
                tenant_id,
                subject,
                &memory,
                state,
                reason,
                evidence,
                queries.clone(),
                proofs.clone(),
            )
            .await?;
        if entailed {
            final_memory = self
                .transition(
                    tenant_id,
                    subject,
                    &final_memory,
                    MemoryState::ApprovalRequired,
                    "SEMANTIC_PUBLICATION_APPROVAL_REQUIRED",
                    evidence,
                    queries.clone(),
                    proofs.clone(),
                )
                .await?;
        }
        Ok(MemoryValidationReceipt {
            memory: final_memory,
            query_execution_ids: queries,
            proof_support_ids: proofs,
            evidence_sha256: evidence.to_lower_hex(),
        })
    }

    pub async fn approve(
        &self,
        tenant_id: Uuid,
        subject: &str,
        memory_id: Uuid,
        reason: &str,
    ) -> Result<MemoryView, MemoryError> {
        validate_reason(reason)?;
        let memory = self.get(tenant_id, subject, memory_id).await?;
        if memory.state != MemoryState::ApprovalRequired {
            return Err(MemoryError::State);
        }
        let evidence = memory_evidence(&memory, MemoryState::Approved, &[], &[]);
        self.transition(
            tenant_id,
            subject,
            &memory,
            MemoryState::Approved,
            reason,
            evidence,
            Vec::new(),
            Vec::new(),
        )
        .await
    }

    pub async fn publish(
        &self,
        tenant_id: Uuid,
        subject: &str,
        authorization: &HeaderValue,
        memory_id: Uuid,
        request: MemoryPublishRequest,
        request_id: &str,
    ) -> Result<MemoryPublicationReceipt, MemoryError> {
        let memory = self.get(tenant_id, subject, memory_id).await?;
        if memory.memory_class != MemoryClass::Semantic
            || memory.state != MemoryState::Approved
            || request.ngkg_operation_id.is_nil()
            || request.published_snapshot_id.is_nil()
        {
            return Err(MemoryError::State);
        }
        let dataset = memory.dataset_id.ok_or(MemoryError::Evidence)?;
        let expected_graph = memory
            .authorized_graph_set_sha256
            .clone()
            .ok_or(MemoryError::Evidence)?;
        let (entailed, queries, proofs) = self
            .entail_all(
                authorization,
                dataset,
                request.published_snapshot_id,
                &expected_graph,
                &memory.content,
                request_id,
            )
            .await?;
        if !entailed {
            return Err(MemoryError::Unknown);
        }
        let evidence = memory_evidence(&memory, MemoryState::Published, &queries, &proofs);
        let publication_id = Uuid::new_v4();
        let now = epoch_ms()?;
        let mut tx = self.tenant_tx(tenant_id).await?;
        let result=sqlx::query("UPDATE ngkg_agents.agent_memory SET state='PUBLISHED',state_version=state_version+1,updated_at_epoch_ms=$1 WHERE tenant_id=$2 AND memory_id=$3 AND state='APPROVED' AND state_version=$4").bind(now).bind(tenant_id).bind(memory_id).bind(memory.state_version).execute(&mut *tx).await?;
        if result.rows_affected() != 1 {
            return Err(MemoryError::Conflict);
        }
        sqlx::query("INSERT INTO ngkg_agents.agent_memory_transition(tenant_id,transition_id,memory_id,state_version,from_state,to_state,actor,reason_code,evidence_sha256,query_execution_ids,proof_support_ids,created_at_epoch_ms) VALUES($1,$2,$3,$4,'APPROVED','PUBLISHED',$5,'PUBLISHED_SNAPSHOT_REENTAILMENT_PASSED',$6,$7,$8,$9)").bind(tenant_id).bind(Uuid::new_v4()).bind(memory_id).bind(memory.state_version+1).bind(subject).bind(evidence.0.as_slice()).bind(serde_json::to_value(&queries)?).bind(serde_json::to_value(&proofs)?).bind(now).execute(&mut *tx).await?;
        sqlx::query("INSERT INTO ngkg_agents.agent_memory_publication(tenant_id,publication_id,memory_id,memory_version,ngkg_operation_id,published_snapshot_id,validation_evidence_sha256,query_execution_ids,proof_support_ids,published_by,published_at_epoch_ms) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)").bind(tenant_id).bind(publication_id).bind(memory_id).bind(memory.version).bind(request.ngkg_operation_id).bind(request.published_snapshot_id).bind(evidence.0.as_slice()).bind(serde_json::to_value(&queries)?).bind(serde_json::to_value(&proofs)?).bind(subject).bind(now).execute(&mut *tx).await?;
        tx.commit().await?;
        let final_memory = self.get(tenant_id, subject, memory_id).await?;
        Ok(MemoryPublicationReceipt {
            publication_id,
            memory: final_memory,
            ngkg_operation_id: request.ngkg_operation_id,
            published_snapshot_id: request.published_snapshot_id,
            query_execution_ids: queries,
            proof_support_ids: proofs,
            evidence_sha256: evidence.to_lower_hex(),
        })
    }

    pub async fn revoke(
        &self,
        tenant_id: Uuid,
        subject: &str,
        memory_id: Uuid,
        reason: &str,
    ) -> Result<MemoryView, MemoryError> {
        validate_reason(reason)?;
        let memory = self.get(tenant_id, subject, memory_id).await?;
        if matches!(
            memory.state,
            MemoryState::Revoked
                | MemoryState::Superseded
                | MemoryState::Rejected
                | MemoryState::Expired
        ) {
            return Err(MemoryError::State);
        }
        let evidence = memory_evidence(&memory, MemoryState::Revoked, &[], &[]);
        self.transition(
            tenant_id,
            subject,
            &memory,
            MemoryState::Revoked,
            reason,
            evidence,
            Vec::new(),
            Vec::new(),
        )
        .await
    }

    pub async fn supersede(
        &self,
        tenant_id: Uuid,
        subject: &str,
        memory_id: Uuid,
        request: MemorySupersedeRequest,
    ) -> Result<MemoryView, MemoryError> {
        validate_reason(&request.reason_code)?;
        let memory = self.get(tenant_id, subject, memory_id).await?;
        let replacement = self
            .get(tenant_id, subject, request.superseding_memory_id)
            .await?;
        let replacement_ready = if memory.memory_class == MemoryClass::Semantic {
            replacement.state == MemoryState::Published
        } else {
            matches!(
                replacement.state,
                MemoryState::Validated | MemoryState::Approved | MemoryState::Published
            )
        };
        if memory.memory_class != replacement.memory_class
            || memory.memory_id == replacement.memory_id
            || !matches!(
                memory.state,
                MemoryState::Validated | MemoryState::Approved | MemoryState::Published
            )
            || !replacement_ready
        {
            return Err(MemoryError::State);
        }
        let evidence = memory_edge_evidence(&memory, &replacement, "SUPERSEDES");
        let now = epoch_ms()?;
        let mut tx = self.tenant_tx(tenant_id).await?;
        sqlx::query("INSERT INTO ngkg_agents.agent_memory_edge(tenant_id,edge_id,source_memory_id,source_version,target_memory_id,target_version,edge_type,actor,evidence_sha256,created_at_epoch_ms) VALUES($1,$2,$3,$4,$5,$6,'SUPERSEDES',$7,$8,$9)").bind(tenant_id).bind(Uuid::new_v4()).bind(replacement.memory_id).bind(replacement.version).bind(memory.memory_id).bind(memory.version).bind(subject).bind(evidence.0.as_slice()).bind(now).execute(&mut *tx).await?;
        let result=sqlx::query("UPDATE ngkg_agents.agent_memory SET state='SUPERSEDED',state_version=state_version+1,updated_at_epoch_ms=$1 WHERE tenant_id=$2 AND memory_id=$3 AND state=$4 AND state_version=$5").bind(now).bind(tenant_id).bind(memory_id).bind(memory.state.as_str()).bind(memory.state_version).execute(&mut *tx).await?;
        if result.rows_affected() != 1 {
            return Err(MemoryError::Conflict);
        }
        sqlx::query("INSERT INTO ngkg_agents.agent_memory_transition(tenant_id,transition_id,memory_id,state_version,from_state,to_state,actor,reason_code,evidence_sha256,query_execution_ids,proof_support_ids,created_at_epoch_ms) VALUES($1,$2,$3,$4,$5,'SUPERSEDED',$6,$7,$8,'[]','[]',$9)").bind(tenant_id).bind(Uuid::new_v4()).bind(memory_id).bind(memory.state_version+1).bind(memory.state.as_str()).bind(subject).bind(&request.reason_code).bind(evidence.0.as_slice()).bind(now).execute(&mut *tx).await?;
        tx.commit().await?;
        self.get(tenant_id, subject, memory_id).await
    }

    pub async fn explain(
        &self,
        tenant_id: Uuid,
        subject: &str,
        memory_id: Uuid,
    ) -> Result<MemoryExplanation, MemoryError> {
        let memory = self.get(tenant_id, subject, memory_id).await?;
        let mut tx = self.tenant_tx(tenant_id).await?;
        let transitions=sqlx::query("SELECT transition_id,state_version,from_state,to_state,actor,reason_code,evidence_sha256,query_execution_ids,proof_support_ids,created_at_epoch_ms FROM ngkg_agents.agent_memory_transition WHERE tenant_id=$1 AND memory_id=$2 ORDER BY state_version").bind(tenant_id).bind(memory_id).fetch_all(&mut *tx).await?.iter().map(row_to_transition).collect::<Result<Vec<_>,_>>()?;
        let edges=sqlx::query("SELECT edge_id,source_memory_id,source_version,target_memory_id,target_version,edge_type,evidence_sha256,created_at_epoch_ms FROM ngkg_agents.agent_memory_edge WHERE tenant_id=$1 AND (source_memory_id=$2 OR target_memory_id=$2) ORDER BY created_at_epoch_ms,edge_id").bind(tenant_id).bind(memory_id).fetch_all(&mut *tx).await?.iter().map(row_to_edge).collect::<Result<Vec<_>,_>>()?;
        tx.commit().await?;
        let inclusion_rule=if memory.memory_class==MemoryClass::Semantic{"Returned as current factual memory only in PUBLISHED state after published-snapshot re-entailment."}else{"Returned only while validated, authorized, unexpired, and neither revoked nor superseded."}.to_owned();
        Ok(MemoryExplanation {
            memory,
            transitions,
            edges,
            inclusion_rule,
        })
    }

    async fn transition(
        &self,
        tenant_id: Uuid,
        subject: &str,
        memory: &MemoryView,
        next: MemoryState,
        reason: &str,
        evidence: Hash32,
        queries: Vec<Uuid>,
        proofs: Vec<String>,
    ) -> Result<MemoryView, MemoryError> {
        let now = epoch_ms()?;
        let mut tx = self.tenant_tx(tenant_id).await?;
        let result=sqlx::query("UPDATE ngkg_agents.agent_memory SET state=$1,state_version=state_version+1,updated_at_epoch_ms=$2 WHERE tenant_id=$3 AND memory_id=$4 AND state=$5 AND state_version=$6").bind(next.as_str()).bind(now).bind(tenant_id).bind(memory.memory_id).bind(memory.state.as_str()).bind(memory.state_version).execute(&mut *tx).await?;
        if result.rows_affected() != 1 {
            return Err(MemoryError::Conflict);
        }
        sqlx::query("INSERT INTO ngkg_agents.agent_memory_transition(tenant_id,transition_id,memory_id,state_version,from_state,to_state,actor,reason_code,evidence_sha256,query_execution_ids,proof_support_ids,created_at_epoch_ms) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)").bind(tenant_id).bind(Uuid::new_v4()).bind(memory.memory_id).bind(memory.state_version+1).bind(memory.state.as_str()).bind(next.as_str()).bind(subject).bind(reason).bind(evidence.0.as_slice()).bind(serde_json::to_value(queries)?).bind(serde_json::to_value(proofs)?).bind(now).execute(&mut *tx).await?;
        tx.commit().await?;
        self.get(tenant_id, subject, memory.memory_id).await
    }

    async fn verify_semantic_source(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        tenant_id: Uuid,
        request: &ProposeMemoryRequest,
    ) -> Result<(), MemoryError> {
        let execution = request.source_execution_id.ok_or(MemoryError::Evidence)?;
        let certificate =
            optional_hash(&request.answer_certificate_sha256)?.ok_or(MemoryError::Evidence)?;
        let dataset = request.dataset_id.ok_or(MemoryError::Evidence)?;
        let snapshot = request.snapshot_id.ok_or(MemoryError::Evidence)?;
        let graph =
            optional_hash(&request.authorized_graph_set_sha256)?.ok_or(MemoryError::Evidence)?;
        let semantic =
            optional_hash(&request.semantic_result_sha256)?.ok_or(MemoryError::Evidence)?;
        let row=sqlx::query("SELECT e.state,e.result_sha256,c.dataset_id,c.snapshot_id,c.authorized_graph_set_sha256,c.semantic_result_sha256,c.certificate_sha256,c.certificate FROM ngkg_agents.agent_execution e JOIN ngkg_agents.agent_answer_certificate c ON c.tenant_id=e.tenant_id AND c.execution_id=e.execution_id WHERE e.tenant_id=$1 AND e.execution_id=$2").bind(tenant_id).bind(execution).fetch_one(&mut **tx).await?;
        if row.try_get::<String, _>("state")? != "COMPLETED"
            || hash_vec(row.try_get("result_sha256")?)? != certificate
            || row.try_get::<Uuid, _>("dataset_id")? != dataset
            || row.try_get::<Uuid, _>("snapshot_id")? != snapshot
            || hash_vec(row.try_get("authorized_graph_set_sha256")?)? != graph
            || hash_vec(row.try_get("semantic_result_sha256")?)? != semantic
            || hash_vec(row.try_get("certificate_sha256")?)? != certificate
        {
            return Err(MemoryError::Evidence);
        }
        let certificate_json: serde_json::Value = row.try_get("certificate")?;
        let certified = certificate_json
            .get("claims")
            .and_then(serde_json::Value::as_array)
            .ok_or(MemoryError::Evidence)?
            .iter()
            .filter_map(|claim| {
                claim
                    .get("canonicalNtriple")
                    .and_then(serde_json::Value::as_str)
            })
            .collect::<std::collections::BTreeSet<_>>();
        if semantic_statements(&request.content, self.limits.maximum_statements)?
            .iter()
            .any(|statement| !certified.contains(statement.as_str()))
        {
            return Err(MemoryError::Evidence);
        }
        Ok(())
    }

    async fn entail_all(
        &self,
        authorization: &HeaderValue,
        dataset_id: Uuid,
        snapshot_id: Uuid,
        expected_graph: &str,
        content: &str,
        request_id: &str,
    ) -> Result<(bool, Vec<Uuid>, Vec<String>), MemoryError> {
        let statements = semantic_statements(content, self.limits.maximum_statements)?;
        let mut query_ids = Vec::new();
        let mut proofs = Vec::new();
        for (statement_index, statement) in statements.iter().enumerate() {
            let ask = canonical_ntriple_to_ask(statement)?;
            let child_request = child_request_id(request_id, statement_index);
            let outcome = self
                .query
                .query(
                    authorization,
                    dataset_id,
                    &QueryRequest {
                        query: ask,
                        snapshot_id: Some(snapshot_id),
                        hydrate: false,
                        default_graph_uris: Vec::new(),
                        named_graph_uris: Vec::new(),
                    },
                    &child_request,
                )
                .await?;
            let envelope = build_reasoned_context_envelope(outcome, self.envelope_limits)?;
            if envelope.dataset_id != dataset_id
                || envelope.snapshot_id != snapshot_id
                || envelope.authorized_graph_set_sha256 != expected_graph
                || envelope.reasoning.federated
                || !envelope.reasoning.complete
                || envelope.reasoning.unknown_is_false
                || !matches!(envelope.query_form, EnvelopeQueryForm::Ask)
                || matches!(envelope.semantic_status, SemanticStatus::FederatedVolatile)
            {
                return Err(MemoryError::Evidence);
            }
            query_ids.push(envelope.query_execution_id);
            proofs.extend(envelope.evidence.proof_ids);
            if !matches!(envelope.payload, SemanticPayload::Ask { value: true }) {
                return Ok((false, query_ids, proofs));
            }
        }
        proofs.sort();
        proofs.dedup();
        Ok((true, query_ids, proofs))
    }

    async fn tenant_tx(&self, tenant_id: Uuid) -> Result<Transaction<'_, Postgres>, MemoryError> {
        if tenant_id.is_nil() {
            return Err(MemoryError::Invalid);
        }
        let mut tx = self.pool.begin().await?;
        let installed: bool = sqlx::query_scalar("SELECT set_config('ngkg.tenant_id',$1,true)=$1")
            .bind(tenant_id.to_string())
            .fetch_one(&mut *tx)
            .await?;
        if !installed {
            return Err(MemoryError::Evidence);
        }
        Ok(tx)
    }
}

const MEMORY_SELECT: &str = "SELECT m.memory_id,m.memory_class,m.owner_subject,m.audience,m.state,m.state_version,m.current_version,m.retention_days,m.legal_hold,m.expires_at_epoch_ms,m.created_at_epoch_ms,m.updated_at_epoch_ms,v.content_type,v.content,v.content_sha256,v.source_execution_id,v.dataset_id,v.snapshot_id,v.authorized_graph_set_sha256,v.semantic_result_sha256,v.answer_certificate_sha256,v.provenance FROM ngkg_agents.agent_memory m JOIN ngkg_agents.agent_memory_version v ON v.tenant_id=m.tenant_id AND v.memory_id=m.memory_id AND v.version=m.current_version WHERE m.tenant_id=$1 AND m.memory_id=$2";

fn row_to_memory(row: &sqlx::postgres::PgRow) -> Result<MemoryView, MemoryError> {
    Ok(MemoryView {
        memory_id: row.try_get("memory_id")?,
        memory_class: MemoryClass::parse(&row.try_get::<String, _>("memory_class")?)?,
        audience: MemoryAudience::parse(&row.try_get::<String, _>("audience")?)?,
        state: MemoryState::parse(&row.try_get::<String, _>("state")?)?,
        state_version: row.try_get("state_version")?,
        version: row.try_get("current_version")?,
        owner_subject: row.try_get("owner_subject")?,
        content_type: row.try_get("content_type")?,
        content: row.try_get("content")?,
        content_sha256: hash_vec(row.try_get("content_sha256")?)?.to_lower_hex(),
        source_execution_id: row.try_get("source_execution_id")?,
        dataset_id: row.try_get("dataset_id")?,
        snapshot_id: row.try_get("snapshot_id")?,
        authorized_graph_set_sha256: optional_hash_vec(
            row.try_get("authorized_graph_set_sha256")?,
        )?,
        semantic_result_sha256: optional_hash_vec(row.try_get("semantic_result_sha256")?)?,
        answer_certificate_sha256: optional_hash_vec(row.try_get("answer_certificate_sha256")?)?,
        provenance: row.try_get("provenance")?,
        retention_days: row.try_get("retention_days")?,
        legal_hold: row.try_get("legal_hold")?,
        expires_at_epoch_ms: row.try_get("expires_at_epoch_ms")?,
        created_at_epoch_ms: row.try_get("created_at_epoch_ms")?,
        updated_at_epoch_ms: row.try_get("updated_at_epoch_ms")?,
    })
}
fn row_to_transition(row: &sqlx::postgres::PgRow) -> Result<MemoryTransitionView, MemoryError> {
    Ok(MemoryTransitionView {
        transition_id: row.try_get("transition_id")?,
        state_version: row.try_get("state_version")?,
        from_state: MemoryState::parse(&row.try_get::<String, _>("from_state")?)?,
        to_state: MemoryState::parse(&row.try_get::<String, _>("to_state")?)?,
        actor: row.try_get("actor")?,
        reason_code: row.try_get("reason_code")?,
        evidence_sha256: hash_vec(row.try_get("evidence_sha256")?)?.to_lower_hex(),
        query_execution_ids: serde_json::from_value(row.try_get("query_execution_ids")?)?,
        proof_support_ids: serde_json::from_value(row.try_get("proof_support_ids")?)?,
        created_at_epoch_ms: row.try_get("created_at_epoch_ms")?,
    })
}
fn row_to_edge(row: &sqlx::postgres::PgRow) -> Result<MemoryEdgeView, MemoryError> {
    Ok(MemoryEdgeView {
        edge_id: row.try_get("edge_id")?,
        source_memory_id: row.try_get("source_memory_id")?,
        source_version: row.try_get("source_version")?,
        target_memory_id: row.try_get("target_memory_id")?,
        target_version: row.try_get("target_version")?,
        edge_type: row.try_get("edge_type")?,
        evidence_sha256: hash_vec(row.try_get("evidence_sha256")?)?.to_lower_hex(),
        created_at_epoch_ms: row.try_get("created_at_epoch_ms")?,
    })
}

fn validate_proposal(
    request: &ProposeMemoryRequest,
    limits: MemoryLimits,
) -> Result<(), MemoryError> {
    if request.content.is_empty()
        || request.content.len() > limits.maximum_content_bytes
        || request.retention_days < 1
        || request.retention_days > limits.maximum_retention_days
        || request.idempotency_key.len() < 8
        || request.idempotency_key.len() > 256
        || !request.provenance.is_object()
    {
        return Err(MemoryError::Invalid);
    }
    if contains_secret(&request.content) {
        return Err(MemoryError::Poisoned);
    }
    match request.memory_class {
        MemoryClass::Working => {
            let now = epoch_ms()?;
            let expires = request.expires_at_epoch_ms.ok_or(MemoryError::Invalid)?;
            let max = now
                .checked_add(
                    i64::try_from(limits.maximum_working_ttl.as_millis())
                        .map_err(|_| MemoryError::Invalid)?,
                )
                .ok_or(MemoryError::Invalid)?;
            if expires <= now || expires > max {
                return Err(MemoryError::Invalid);
            }
        }
        MemoryClass::Semantic => {
            if request.content_type != "application/n-triples" {
                return Err(MemoryError::Invalid);
            }
            semantic_statements(&request.content, limits.maximum_statements)?;
        }
        _ => {
            if !matches!(
                request.content_type.as_str(),
                "text/plain" | "application/json"
            ) {
                return Err(MemoryError::Invalid);
            }
            if request.content_type == "application/json" {
                let _: serde_json::Value = serde_json::from_str(&request.content)?;
            }
        }
    }
    Ok(())
}
fn contains_secret(value: &str) -> bool {
    let upper = value.to_ascii_uppercase();
    upper.contains("-----BEGIN PRIVATE KEY-----")
        || upper.contains("-----BEGIN OPENSSH PRIVATE KEY-----")
        || upper.contains("AWS_SECRET_ACCESS_KEY")
        || upper.contains("BEARER EYJ")
        || upper.contains("PASSWORD=")
        || upper.contains("IGNORE PREVIOUS INSTRUCTIONS")
        || upper.contains("REVEAL THE SYSTEM PROMPT")
        || upper.contains("<|IM_START|>")
        || upper.contains("[INST]")
}
fn semantic_statements(content: &str, maximum: usize) -> Result<Vec<String>, MemoryError> {
    let lines = content
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    if lines.is_empty() || lines.len() > maximum {
        return Err(MemoryError::Invalid);
    }
    for line in &lines {
        canonical_ntriple_to_ask(line)?;
    }
    Ok(lines)
}
fn canonical_ntriple_to_ask(line: &str) -> Result<String, MemoryError> {
    if line.len() > 65_536
        || line.contains(['\r', '\n', '\0'])
        || !line.ends_with(" .")
        || line.contains("SERVICE")
        || line.contains("GRAPH")
        || line.contains('?')
        || line.contains('$')
        || line.contains("_:")
    {
        return Err(MemoryError::InvalidRdf);
    }
    let body = &line[..line.len() - 2];
    let (subject, rest) = iri_token(body)?;
    let rest = rest.strip_prefix(' ').ok_or(MemoryError::InvalidRdf)?;
    let (predicate, object) = iri_token(rest)?;
    let object = object.strip_prefix(' ').ok_or(MemoryError::InvalidRdf)?;
    if object.is_empty()
        || object.starts_with('<') && !valid_iri_token(object)
        || object.starts_with('"') && !valid_literal_token(object)
        || (!object.starts_with('<') && !object.starts_with('"'))
    {
        return Err(MemoryError::InvalidRdf);
    }
    Ok(format!("ASK {{ {subject} {predicate} {object} . }}"))
}
fn iri_token(value: &str) -> Result<(&str, &str), MemoryError> {
    let end = value.find('>').ok_or(MemoryError::InvalidRdf)?;
    let token = &value[..=end];
    if !valid_iri_token(token) {
        return Err(MemoryError::InvalidRdf);
    }
    Ok((token, &value[end + 1..]))
}
fn valid_iri_token(value: &str) -> bool {
    if !value.starts_with('<') || !value.ends_with('>') || value.len() <= 3 {
        return false;
    }
    let inner = &value[1..value.len() - 1];
    let Some(colon) = inner.find(':') else {
        return false;
    };
    let scheme = &inner[..colon];
    !scheme.is_empty()
        && scheme.as_bytes()[0].is_ascii_alphabetic()
        && scheme
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'+' | b'-' | b'.'))
        && !inner.bytes().any(|b| {
            b <= 0x20
                || matches!(
                    b,
                    b'<' | b'>' | b'"' | b'{' | b'}' | b'|' | b'^' | b'`' | b'\\'
                )
        })
}
fn valid_literal_token(value: &str) -> bool {
    let bytes = value.as_bytes();
    if bytes.len() < 2 || bytes[0] != b'"' {
        return false;
    }
    let mut escaped = false;
    let mut close = None;
    for (i, b) in bytes.iter().enumerate().skip(1) {
        if escaped {
            escaped = false;
            continue;
        }
        if *b == b'\\' {
            escaped = true;
            continue;
        }
        if *b == b'"' {
            close = Some(i);
            break;
        }
        if *b < 0x20 {
            return false;
        }
    }
    let Some(end) = close else { return false };
    let suffix = &value[end + 1..];
    suffix.is_empty()
        || suffix.starts_with('@')
            && suffix[1..]
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-')
        || suffix.starts_with("^^") && valid_iri_token(&suffix[2..])
}
fn authorize_view(view: &MemoryView, subject: &str) -> Result<(), MemoryError> {
    if view.audience == MemoryAudience::Owner && view.owner_subject != subject {
        return Err(MemoryError::NotAllowed);
    }
    Ok(())
}
fn validate_reason(value: &str) -> Result<(), MemoryError> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_')
    {
        return Err(MemoryError::Invalid);
    }
    Ok(())
}
fn optional_hash(value: &Option<String>) -> Result<Option<Hash32>, MemoryError> {
    value
        .as_deref()
        .map(|value| Hash32::from_lower_hex(value).map_err(MemoryError::from))
        .transpose()
}
fn optional_hash_bytes(value: &Option<String>) -> Result<Option<Vec<u8>>, MemoryError> {
    Ok(optional_hash(value)?.map(|hash| hash.0.to_vec()))
}
fn optional_hash_vec(value: Option<Vec<u8>>) -> Result<Option<String>, MemoryError> {
    value
        .map(hash_vec)
        .transpose()
        .map(|value| value.map(Hash32::to_lower_hex))
}
fn hash_vec(value: Vec<u8>) -> Result<Hash32, MemoryError> {
    let bytes: [u8; 32] = value.try_into().map_err(|_| MemoryError::Evidence)?;
    Ok(Hash32(bytes))
}
fn domain_hash(domain: &[u8], bytes: &[u8]) -> Hash32 {
    let mut digest = Sha256::new();
    digest.update(domain);
    digest.update(bytes);
    Hash32(digest.finalize().into())
}
fn idempotency_hash(tenant: Uuid, subject: &str, key: &str) -> Hash32 {
    let mut digest = Sha256::new();
    digest.update(IDEMPOTENCY_DOMAIN);
    digest.update(tenant.as_bytes());
    digest.update(subject.as_bytes());
    digest.update([0]);
    digest.update(key.as_bytes());
    Hash32(digest.finalize().into())
}
fn memory_evidence(
    memory: &MemoryView,
    next: MemoryState,
    queries: &[Uuid],
    proofs: &[String],
) -> Hash32 {
    let mut digest = Sha256::new();
    digest.update(EVIDENCE_DOMAIN);
    digest.update(memory.memory_id.as_bytes());
    digest.update(memory.version.to_be_bytes());
    digest.update(memory.content_sha256.as_bytes());
    digest.update(next.as_str().as_bytes());
    for id in queries {
        digest.update(id.as_bytes());
    }
    for proof in proofs {
        digest.update(proof.as_bytes());
        digest.update([0]);
    }
    Hash32(digest.finalize().into())
}
fn memory_edge_evidence(source: &MemoryView, target: &MemoryView, kind: &str) -> Hash32 {
    let mut digest = Sha256::new();
    digest.update(EVIDENCE_DOMAIN);
    digest.update(kind.as_bytes());
    digest.update(source.memory_id.as_bytes());
    digest.update(source.version.to_be_bytes());
    digest.update(target.memory_id.as_bytes());
    digest.update(target.version.to_be_bytes());
    Hash32(digest.finalize().into())
}
fn child_request_id(parent: &str, ordinal: usize) -> String {
    let mut digest = Sha256::new();
    digest.update(b"ngkg-memory-child-request-v1\0");
    digest.update(parent.as_bytes());
    digest.update(u64::try_from(ordinal).unwrap_or(u64::MAX).to_be_bytes());
    format!("memory-{}", &hex::encode(digest.finalize())[..32])
}
fn epoch_ms() -> Result<i64, MemoryError> {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| MemoryError::Clock)?
            .as_millis(),
    )
    .map_err(|_| MemoryError::Clock)
}

#[derive(Debug, Error)]
pub enum MemoryError {
    #[error("invalid memory request")]
    Invalid,
    #[error("memory contains credential-like material")]
    Poisoned,
    #[error("memory RDF is invalid or unsafe")]
    InvalidRdf,
    #[error("memory access denied")]
    NotAllowed,
    #[error("memory lifecycle state is invalid")]
    State,
    #[error("memory state changed concurrently")]
    Conflict,
    #[error("memory evidence does not match")]
    Evidence,
    #[error("semantic memory remains unknown")]
    Unknown,
    #[error("clock failed")]
    Clock,
    #[error("database failed")]
    Database(#[from] sqlx::Error),
    #[error("JSON failed")]
    Json(#[from] serde_json::Error),
    #[error("catalog hash failed")]
    Catalog(#[from] ngkg_agent_catalog::CatalogError),
    #[error("NGKG query failed")]
    Query(#[from] ngkg_api_client::ClientError),
    #[error("semantic envelope failed")]
    Envelope(#[from] ngkg_mcp_contracts::EnvelopeError),
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn semantic_parser_rejects_variables_and_service() {
        assert!(canonical_ntriple_to_ask("<https://s> <https://p> <https://o> .").is_ok());
        assert!(canonical_ntriple_to_ask("<https://s> <https://p> ?o .").is_err());
        assert!(canonical_ntriple_to_ask("<https://s> <https://p> SERVICE .").is_err());
    }
    #[test]
    fn poison_filter_blocks_keys_and_instruction_override() {
        assert!(contains_secret("-----BEGIN PRIVATE KEY-----"));
        assert!(contains_secret("ignore previous instructions"));
        assert!(!contains_secret("ordinary evidence"));
    }
    #[test]
    fn domain_hashes_are_separate() {
        assert_ne!(
            domain_hash(CONTENT_DOMAIN, b"x"),
            domain_hash(EVIDENCE_DOMAIN, b"x")
        );
    }
}
