//! Tenant-isolated PostgreSQL repository and tamper-evident audit chain.
//!
//! Every operation starts a transaction, installs the tenant with
//! `set_config(..., true)`, and relies on forced PostgreSQL row-level security.
//! Audit append operations are serialized per tenant with an advisory
//! transaction lock and are idempotent by event UUID.

#![allow(missing_docs)]

use std::time::Duration;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Postgres, Row, Transaction, postgres::PgPoolOptions};
use thiserror::Error;
use url::Url;
use uuid::Uuid;

const AUDIT_DOMAIN: &[u8] = b"ngkg-agent-audit-event-v1\0";
const ZERO_HASH: Hash32 = Hash32([0; 32]);

/// Validated SHA-256 value.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub struct Hash32(#[serde(with = "hash_serde")] pub [u8; 32]);

impl Hash32 {
    /// Parse canonical lowercase hexadecimal SHA-256.
    pub fn from_lower_hex(value: &str) -> Result<Self, CatalogError> {
        if value.len() != 64
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
        {
            return Err(CatalogError::Invalid("SHA-256 is not canonical"));
        }
        let bytes = hex::decode(value).map_err(|_| CatalogError::Invalid("SHA-256 is invalid"))?;
        let decoded = bytes
            .try_into()
            .map_err(|_| CatalogError::Invalid("SHA-256 decoded length is invalid"))?;
        Ok(Self(decoded))
    }

    /// Encode canonical lowercase hexadecimal SHA-256.
    #[must_use]
    pub fn to_lower_hex(self) -> String {
        hex::encode(self.0)
    }
}

mod hash_serde {
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(value: &[u8; 32], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&hex::encode(value))
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<[u8; 32], D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        super::Hash32::from_lower_hex(&value)
            .map(|hash| hash.0)
            .map_err(serde::de::Error::custom)
    }
}

/// Pool and transport bounds.
#[derive(Clone, Copy, Debug)]
pub struct CatalogOptions {
    /// Maximum database connections in one process.
    pub maximum_connections: u32,
    /// Time allowed to acquire or establish a connection.
    pub acquire_timeout: Duration,
    /// Allow unencrypted PostgreSQL only on an exact loopback host for tests.
    pub allow_insecure_loopback: bool,
}

impl Default for CatalogOptions {
    fn default() -> Self {
        Self {
            maximum_connections: 16,
            acquire_timeout: Duration::from_secs(5),
            allow_insecure_loopback: false,
        }
    }
}

/// PostgreSQL-backed agent catalog.
#[derive(Clone)]
pub struct AgentCatalog {
    pool: PgPool,
}

impl AgentCatalog {
    /// Connect after validating transport and pool limits.
    pub async fn connect(
        database_url: &str,
        options: CatalogOptions,
    ) -> Result<Self, CatalogError> {
        validate_database_url(database_url, options)?;
        let pool = PgPoolOptions::new()
            .max_connections(options.maximum_connections)
            .acquire_timeout(options.acquire_timeout)
            .connect(database_url)
            .await?;
        Ok(Self { pool })
    }

    /// Run the add-on migration set. Production uses the dedicated migrator job.
    pub async fn migrate(&self) -> Result<(), CatalogError> {
        sqlx::migrate!("../../migrations-agents")
            .run(&self.pool)
            .await?;
        Ok(())
    }

    /// Grant the minimum runtime privileges to a pre-created, non-privileged role.
    pub async fn grant_runtime_role(&self, role_name: &str) -> Result<(), CatalogError> {
        if role_name.is_empty()
            || role_name.len() > 63
            || role_name.starts_with("pg_")
            || !role_name.bytes().enumerate().all(|(index, byte)| {
                byte.is_ascii_lowercase() || byte == b'_' || (index > 0 && byte.is_ascii_digit())
            })
        {
            return Err(CatalogError::Invalid(
                "runtime database role name is invalid",
            ));
        }
        let mut transaction = self.pool.begin().await?;
        let safe: Option<String> = sqlx::query_scalar(
            "SELECT quote_ident(r.rolname) FROM pg_roles r \
             WHERE r.rolname=$1 AND r.rolcanlogin AND NOT r.rolsuper \
               AND NOT r.rolbypassrl \
               AND NOT EXISTS (SELECT 1 FROM pg_auth_members m WHERE m.member=r.oid) \
               AND NOT EXISTS (SELECT 1 FROM pg_namespace n WHERE n.nspname='ngkg_agents' AND n.nspowner=r.oid) \
               AND NOT EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace \
                                WHERE n.nspname='ngkg_agents' AND c.relowner=r.oid)",
        )
        .bind(role_name)
        .fetch_optional(&mut *transaction)
        .await?;
        let safe = safe.ok_or(CatalogError::Invalid(
            "runtime database role is missing, privileged, inherited, or owns agent objects",
        ))?;
        for statement in [
            format!("GRANT USAGE ON SCHEMA ngkg_agents TO {safe}"),
            format!("GRANT EXECUTE ON FUNCTION ngkg_agents.current_tenant_id() TO {safe}"),
            format!(
                "GRANT EXECUTE ON FUNCTION ngkg_agents.claim_prompt_compilation_shard(text,uuid,bigint) TO {safe}"
            ),
            format!(
                "GRANT EXECUTE ON FUNCTION ngkg_agents.enqueue_prompt_compilation_shards(uuid,uuid) TO {safe}"
            ),
            format!(
                "GRANT EXECUTE ON FUNCTION ngkg_agents.finish_prompt_compilation_claim(uuid,uuid,integer,uuid,text) TO {safe}"
            ),
            format!(
                "GRANT EXECUTE ON FUNCTION ngkg_agents.enqueue_cpu_workload(uuid,uuid) TO {safe}"
            ),
            format!(
                "GRANT EXECUTE ON FUNCTION ngkg_agents.claim_cpu_partition(text,uuid,bigint,text) TO {safe}"
            ),
            format!(
                "GRANT EXECUTE ON FUNCTION ngkg_agents.cpu_ready_partition_count(text) TO {safe}"
            ),
            format!(
                "GRANT EXECUTE ON FUNCTION ngkg_agents.checkpoint_cpu_partition(uuid,uuid,integer,uuid,bytea,bigint,bigint,bigint,bigint,bigint) TO {safe}"
            ),
            format!(
                "GRANT EXECUTE ON FUNCTION ngkg_agents.finish_cpu_partition(uuid,uuid,integer,uuid,text,bytea,text,bigint,bigint,bigint,integer,bigint,bigint) TO {safe}"
            ),
            format!(
                "GRANT EXECUTE ON FUNCTION ngkg_agents.cancel_cpu_workload(uuid,uuid) TO {safe}"
            ),
            format!(
                "GRANT EXECUTE ON FUNCTION ngkg_agents.claim_context_slice_gc(text,uuid,bigint,bigint) TO {safe}"
            ),
            format!(
                "GRANT EXECUTE ON FUNCTION ngkg_agents.schedule_context_slice_gc(uuid,uuid,bigint) TO {safe}"
            ),
            format!(
                "GRANT EXECUTE ON FUNCTION ngkg_agents.finish_context_slice_gc(uuid,uuid,uuid,uuid,bytea,integer,bigint) TO {safe}"
            ),
            format!(
                "GRANT EXECUTE ON FUNCTION ngkg_agents.validate_context_slice_transition() TO {safe}"
            ),
            format!(
                "GRANT EXECUTE ON FUNCTION ngkg_agents.validate_context_capability_update() TO {safe}"
            ),
            format!(
                "GRANT EXECUTE ON FUNCTION ngkg_agents.validate_agent_memory_transition() TO {safe}"
            ),
            format!("GRANT SELECT, INSERT ON ALL TABLES IN SCHEMA ngkg_agents TO {safe}"),
            format!("REVOKE ALL ON ngkg_agents.prompt_compilation_queue FROM {safe}"),
            format!("REVOKE ALL ON ngkg_agents.cpu_work_queue FROM {safe}"),
            format!(
                "GRANT UPDATE ON ngkg_agents.agent_execution, ngkg_agents.model_call, ngkg_agents.tool_call, ngkg_agents.prompt_input, ngkg_agents.prompt_compilation_shard, ngkg_agents.agent_memory, ngkg_agents.cpu_workload, ngkg_agents.cpu_work_partition, ngkg_agents.context_slice, ngkg_agents.context_slice_capability TO {safe}"
            ),
            format!("REVOKE ALL ON ngkg_agents.context_slice_gc_queue FROM {safe}"),
            format!(
                "REVOKE DELETE, TRUNCATE, REFERENCES, TRIGGER ON ALL TABLES IN SCHEMA ngkg_agents FROM {safe}"
            ),
        ] {
            sqlx::query(&statement).execute(&mut *transaction).await?;
        }
        transaction.commit().await?;
        Ok(())
    }

    /// Verify database liveness without setting tenant state.
    pub async fn ready(&self) -> Result<(), CatalogError> {
        let ready: bool = sqlx::query_scalar(
            "SELECT COALESCE((SELECT relrowsecurity AND relforcerowsecurity \
             FROM pg_class WHERE oid = to_regclass('ngkg_agents.agent_audit_chain')), false)",
        )
        .fetch_one(&self.pool)
        .await?;
        if !ready {
            return Err(CatalogError::Evidence(
                "agent audit schema is absent or forced RLS is disabled",
            ));
        }
        Ok(())
    }

    /// Append one idempotent, tenant-serialized audit event.
    pub async fn append_audit_event(
        &self,
        tenant_id: Uuid,
        input: &AuditEventInput,
    ) -> Result<AuditChainRecord, CatalogError> {
        validate_audit_input(tenant_id, input)?;
        let mut transaction = self.tenant_transaction(tenant_id).await?;
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
            .bind(tenant_id.to_string())
            .execute(&mut *transaction)
            .await?;

        if let Some(existing) =
            existing_audit_event(&mut transaction, tenant_id, input.event_id).await?
        {
            validate_idempotent_event(tenant_id, &existing, input)?;
            transaction.commit().await?;
            return Ok(AuditChainRecord {
                tenant_id,
                sequence: existing.sequence,
                event_id: input.event_id,
                previous_event_sha256: existing.previous_event_sha256,
                event_sha256: existing.event_sha256,
            });
        }

        let prior = sqlx::query(
            "SELECT sequence, event_sha256 FROM ngkg_agents.agent_audit_chain \
             WHERE tenant_id = $1 ORDER BY sequence DESC LIMIT 1 FOR UPDATE",
        )
        .bind(tenant_id)
        .fetch_optional(&mut *transaction)
        .await?;
        let (sequence, previous) = if let Some(row) = prior {
            let prior_sequence: i64 = row.try_get("sequence")?;
            let prior_hash: Vec<u8> = row.try_get("event_sha256")?;
            (
                prior_sequence
                    .checked_add(1)
                    .ok_or(CatalogError::Overflow)?,
                hash_from_database(prior_hash)?,
            )
        } else {
            (0, ZERO_HASH)
        };
        let event_sha256 = audit_event_sha256(tenant_id, sequence, previous, input)?;
        sqlx::query(
            "INSERT INTO ngkg_agents.agent_audit_chain \
             (tenant_id, sequence, event_id, event_type, subject, actor, request_id, outcome, \
              policy_version_sha256, service_build_sha256, redacted_payload_sha256, \
              previous_event_sha256, event_sha256, event_time_epoch_ms) \
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)",
        )
        .bind(tenant_id)
        .bind(sequence)
        .bind(input.event_id)
        .bind(&input.event_type)
        .bind(&input.subject)
        .bind(&input.actor)
        .bind(&input.request_id)
        .bind(input.outcome.as_str())
        .bind(input.policy_version_sha256.0.as_slice())
        .bind(input.service_build_sha256.0.as_slice())
        .bind(input.redacted_payload_sha256.0.as_slice())
        .bind(previous.0.as_slice())
        .bind(event_sha256.0.as_slice())
        .bind(input.event_time_epoch_ms)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(AuditChainRecord {
            tenant_id,
            sequence,
            event_id: input.event_id,
            previous_event_sha256: previous,
            event_sha256,
        })
    }

    /// Record an immutable external WORM receipt for an exact tenant chain head.
    pub async fn record_audit_seal(&self, input: &AuditSeal) -> Result<(), CatalogError> {
        validate_audit_seal(input)?;
        let mut transaction = self.tenant_transaction(input.tenant_id).await?;
        let observed: Option<Vec<u8>> = sqlx::query_scalar(
            "SELECT event_sha256 FROM ngkg_agents.agent_audit_chain \
             WHERE tenant_id=$1 AND sequence=$2",
        )
        .bind(input.tenant_id)
        .bind(input.through_sequence)
        .fetch_optional(&mut *transaction)
        .await?;
        let observed =
            observed.ok_or(CatalogError::Evidence("audit seal chain head is missing"))?;
        if hash_from_database(observed)? != input.chain_head_sha256 {
            return Err(CatalogError::Evidence(
                "audit seal chain head does not match",
            ));
        }
        sqlx::query(
            "INSERT INTO ngkg_agents.audit_seal \
             (tenant_id, seal_id, through_sequence, chain_head_sha256, external_target, \
              external_receipt_sha256, sealed_by, sealed_at_epoch_ms) \
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
        )
        .bind(input.tenant_id)
        .bind(input.seal_id)
        .bind(input.through_sequence)
        .bind(input.chain_head_sha256.0.as_slice())
        .bind(&input.external_target)
        .bind(input.external_receipt_sha256.0.as_slice())
        .bind(&input.sealed_by)
        .bind(input.sealed_at_epoch_ms)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    /// Insert one immutable provider version. State changes create a new version row.
    pub async fn record_tool_provider(
        &self,
        input: &ToolProviderVersion,
    ) -> Result<(), CatalogError> {
        validate_provider(input)?;
        let mut transaction = self.tenant_transaction(input.tenant_id).await?;
        sqlx::query(
            "INSERT INTO ngkg_agents.tool_provider \
             (tenant_id, provider_id, version, name, endpoint, auth_reference, policy, state, \
              spec_sha256, created_by, created_at_epoch_ms) \
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)",
        )
        .bind(input.tenant_id)
        .bind(input.provider_id)
        .bind(input.version)
        .bind(&input.name)
        .bind(&input.endpoint)
        .bind(&input.auth_reference)
        .bind(&input.policy)
        .bind(input.state.as_str())
        .bind(input.spec_sha256.0.as_slice())
        .bind(&input.created_by)
        .bind(input.created_at_epoch_ms)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    /// Insert one immutable, qualification-bound tool catalog.
    pub async fn record_tool_catalog(
        &self,
        input: &ToolCatalogVersion,
    ) -> Result<(), CatalogError> {
        validate_tool_catalog(input)?;
        let mut transaction = self.tenant_transaction(input.tenant_id).await?;
        sqlx::query(
            "INSERT INTO ngkg_agents.tool_catalog \
             (tenant_id, provider_id, provider_version, catalog_sha256, protocol_version, \
              discovered_tools, qualification_evidence_sha256, created_at_epoch_ms) \
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
        )
        .bind(input.tenant_id)
        .bind(input.provider_id)
        .bind(input.provider_version)
        .bind(input.catalog_sha256.0.as_slice())
        .bind(&input.protocol_version)
        .bind(&input.discovered_tools)
        .bind(input.qualification_evidence_sha256.0.as_slice())
        .bind(input.created_at_epoch_ms)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    /// Atomically publish a qualified provider version and its immutable catalog.
    /// A crash can therefore never expose a `QUALIFIED` provider without the
    /// qualification evidence that makes that state meaningful.
    pub async fn record_qualified_tool_provider_and_catalog(
        &self,
        provider: &ToolProviderVersion,
        catalog: &ToolCatalogVersion,
    ) -> Result<(), CatalogError> {
        validate_provider(provider)?;
        validate_tool_catalog(catalog)?;
        if provider.state != ProviderState::Qualified
            || provider.tenant_id != catalog.tenant_id
            || provider.provider_id != catalog.provider_id
            || provider.version != catalog.provider_version
        {
            return Err(CatalogError::Invalid(
                "qualified provider and catalog identities do not match",
            ));
        }
        let mut transaction = self.tenant_transaction(provider.tenant_id).await?;
        sqlx::query(
            "INSERT INTO ngkg_agents.tool_provider \
             (tenant_id, provider_id, version, name, endpoint, auth_reference, policy, state, \
              spec_sha256, created_by, created_at_epoch_ms) \
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)",
        )
        .bind(provider.tenant_id)
        .bind(provider.provider_id)
        .bind(provider.version)
        .bind(&provider.name)
        .bind(&provider.endpoint)
        .bind(&provider.auth_reference)
        .bind(&provider.policy)
        .bind(provider.state.as_str())
        .bind(provider.spec_sha256.0.as_slice())
        .bind(&provider.created_by)
        .bind(provider.created_at_epoch_ms)
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "INSERT INTO ngkg_agents.tool_catalog \
             (tenant_id, provider_id, provider_version, catalog_sha256, protocol_version, \
              discovered_tools, qualification_evidence_sha256, created_at_epoch_ms) \
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
        )
        .bind(catalog.tenant_id)
        .bind(catalog.provider_id)
        .bind(catalog.provider_version)
        .bind(catalog.catalog_sha256.0.as_slice())
        .bind(&catalog.protocol_version)
        .bind(&catalog.discovered_tools)
        .bind(catalog.qualification_evidence_sha256.0.as_slice())
        .bind(catalog.created_at_epoch_ms)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    /// Insert one immutable agent-profile version.
    pub async fn record_agent_profile(
        &self,
        input: &AgentProfileVersion,
    ) -> Result<(), CatalogError> {
        validate_profile(input)?;
        let mut transaction = self.tenant_transaction(input.tenant_id).await?;
        sqlx::query(
            "INSERT INTO ngkg_agents.agent_profile \
             (tenant_id, profile_id, version, name, dataset_constraints, model_allowlist, \
              tool_catalog_sha256s, limits, approval_policy, profile_sha256, created_by, created_at_epoch_ms) \
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)",
        )
        .bind(input.tenant_id)
        .bind(input.profile_id)
        .bind(input.version)
        .bind(&input.name)
        .bind(&input.dataset_constraints)
        .bind(&input.model_allowlist)
        .bind(&input.tool_catalog_sha256s)
        .bind(&input.limits)
        .bind(&input.approval_policy)
        .bind(input.profile_sha256.0.as_slice())
        .bind(&input.created_by)
        .bind(input.created_at_epoch_ms)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    /// Load an exact immutable profile version under forced tenant RLS.
    pub async fn load_agent_profile(
        &self,
        tenant_id: Uuid,
        profile_id: Uuid,
        version: i64,
    ) -> Result<AgentProfileVersion, CatalogError> {
        let mut transaction = self.tenant_transaction(tenant_id).await?;
        let row=sqlx::query("SELECT name,dataset_constraints,model_allowlist,tool_catalog_sha256s,limits,approval_policy,profile_sha256,created_by,created_at_epoch_ms FROM ngkg_agents.agent_profile WHERE tenant_id=$1 AND profile_id=$2 AND version=$3").bind(tenant_id).bind(profile_id).bind(version).fetch_one(&mut *transaction).await?;
        transaction.commit().await?;
        Ok(AgentProfileVersion {
            tenant_id,
            profile_id,
            version,
            name: row.try_get("name")?,
            dataset_constraints: row.try_get("dataset_constraints")?,
            model_allowlist: row.try_get("model_allowlist")?,
            tool_catalog_sha256s: row.try_get("tool_catalog_sha256s")?,
            limits: row.try_get("limits")?,
            approval_policy: row.try_get("approval_policy")?,
            profile_sha256: hash_from_database(row.try_get("profile_sha256")?)?,
            created_by: row.try_get("created_by")?,
            created_at_epoch_ms: row.try_get("created_at_epoch_ms")?,
        })
    }

    /// Load one exact immutable tool-provider version under tenant RLS.
    pub async fn load_tool_provider(
        &self,
        tenant_id: Uuid,
        provider_id: Uuid,
        version: i64,
    ) -> Result<ToolProviderVersion, CatalogError> {
        let mut transaction = self.tenant_transaction(tenant_id).await?;
        let row=sqlx::query("SELECT name,endpoint,auth_reference,policy,state,spec_sha256,created_by,created_at_epoch_ms FROM ngkg_agents.tool_provider WHERE tenant_id=$1 AND provider_id=$2 AND version=$3").bind(tenant_id).bind(provider_id).bind(version).fetch_one(&mut *transaction).await?;
        transaction.commit().await?;
        let state = match row.try_get::<String, _>("state")?.as_str() {
            "PENDING" => ProviderState::Pending,
            "QUALIFIED" => ProviderState::Qualified,
            "DISABLED" => ProviderState::Disabled,
            "REVOKED" => ProviderState::Revoked,
            _ => return Err(CatalogError::Evidence("provider state is invalid")),
        };
        Ok(ToolProviderVersion {
            tenant_id,
            provider_id,
            version,
            name: row.try_get("name")?,
            endpoint: row.try_get("endpoint")?,
            auth_reference: row.try_get("auth_reference")?,
            policy: row.try_get("policy")?,
            state,
            spec_sha256: hash_from_database(row.try_get("spec_sha256")?)?,
            created_by: row.try_get("created_by")?,
            created_at_epoch_ms: row.try_get("created_at_epoch_ms")?,
        })
    }

    /// Load one exact qualification-bound catalog.
    pub async fn load_tool_catalog(
        &self,
        tenant_id: Uuid,
        provider_id: Uuid,
        catalog_sha256: Hash32,
    ) -> Result<ToolCatalogVersion, CatalogError> {
        let mut transaction = self.tenant_transaction(tenant_id).await?;
        let row=sqlx::query("SELECT provider_version,protocol_version,discovered_tools,qualification_evidence_sha256,created_at_epoch_ms FROM ngkg_agents.tool_catalog WHERE tenant_id=$1 AND provider_id=$2 AND catalog_sha256=$3").bind(tenant_id).bind(provider_id).bind(catalog_sha256.0.as_slice()).fetch_one(&mut *transaction).await?;
        transaction.commit().await?;
        Ok(ToolCatalogVersion {
            tenant_id,
            provider_id,
            provider_version: row.try_get("provider_version")?,
            catalog_sha256,
            protocol_version: row.try_get("protocol_version")?,
            discovered_tools: row.try_get("discovered_tools")?,
            qualification_evidence_sha256: hash_from_database(
                row.try_get("qualification_evidence_sha256")?,
            )?,
            created_at_epoch_ms: row.try_get("created_at_epoch_ms")?,
        })
    }

    /// Load one immutable approval decision.
    pub async fn load_approval(
        &self,
        tenant_id: Uuid,
        approval_id: Uuid,
    ) -> Result<ApprovalRecord, CatalogError> {
        let mut transaction = self.tenant_transaction(tenant_id).await?;
        let row=sqlx::query("SELECT execution_id,tool_name,approver,policy_sha256,catalog_sha256,decision,expires_at_epoch_ms,created_at_epoch_ms FROM ngkg_agents.approval WHERE tenant_id=$1 AND approval_id=$2").bind(tenant_id).bind(approval_id).fetch_one(&mut *transaction).await?;
        transaction.commit().await?;
        let decision = match row.try_get::<String, _>("decision")?.as_str() {
            "APPROVED" => ApprovalDecision::Approved,
            "DENIED" => ApprovalDecision::Denied,
            _ => return Err(CatalogError::Evidence("approval decision is invalid")),
        };
        let catalog: Option<Vec<u8>> = row.try_get("catalog_sha256")?;
        Ok(ApprovalRecord {
            tenant_id,
            approval_id,
            execution_id: row.try_get("execution_id")?,
            tool_name: row.try_get("tool_name")?,
            approver: row.try_get("approver")?,
            policy_sha256: hash_from_database(row.try_get("policy_sha256")?)?,
            catalog_sha256: catalog.map(hash_from_database).transpose()?,
            decision,
            expires_at_epoch_ms: row.try_get("expires_at_epoch_ms")?,
            created_at_epoch_ms: row.try_get("created_at_epoch_ms")?,
        })
    }

    /// Resolve the immutable agent profile and certificate identity for a tool call.
    pub async fn load_tool_execution_context(
        &self,
        tenant_id: Uuid,
        execution_id: Uuid,
    ) -> Result<ToolExecutionContext, CatalogError> {
        let mut transaction = self.tenant_transaction(tenant_id).await?;
        let row=sqlx::query("SELECT profile_id,profile_version,state,result_sha256 FROM ngkg_agents.agent_execution WHERE tenant_id=$1 AND execution_id=$2").bind(tenant_id).bind(execution_id).fetch_one(&mut *transaction).await?;
        transaction.commit().await?;
        let result: Option<Vec<u8>> = row.try_get("result_sha256")?;
        Ok(ToolExecutionContext {
            tenant_id,
            execution_id,
            profile_id: row.try_get("profile_id")?,
            profile_version: row.try_get("profile_version")?,
            state: row.try_get("state")?,
            result_sha256: result.map(hash_from_database).transpose()?,
        })
    }

    /// Bind one frozen prompt manifest to an execution.
    pub async fn record_execution_input(
        &self,
        input: &ExecutionInputRecord,
    ) -> Result<(), CatalogError> {
        let mut transaction = self.tenant_transaction(input.tenant_id).await?;
        sqlx::query("INSERT INTO ngkg_agents.agent_execution_input(tenant_id,input_id,execution_id,source_root_sha256,compiled_root_sha256,requirement_root_sha256,context_query_sha256,created_at_epoch_ms) VALUES($1,$2,$3,$4,$5,$6,$7,$8)").bind(input.tenant_id).bind(input.input_id).bind(input.execution_id).bind(input.source_root_sha256.0.as_slice()).bind(input.compiled_root_sha256.0.as_slice()).bind(input.requirement_root_sha256.0.as_slice()).bind(input.context_query_sha256.0.as_slice()).bind(input.created_at_epoch_ms).execute(&mut *transaction).await?;
        transaction.commit().await?;
        Ok(())
    }

    /// Store one immutable answer certificate after all claims are entailed.
    pub async fn record_answer_certificate(
        &self,
        input: &AnswerCertificateRecord,
    ) -> Result<(), CatalogError> {
        let mut transaction = self.tenant_transaction(input.tenant_id).await?;
        sqlx::query("INSERT INTO ngkg_agents.agent_answer_certificate(tenant_id,certificate_id,execution_id,dataset_id,snapshot_id,query_execution_id,authorized_graph_set_sha256,active_dataset_sha256,serving_root_sha256,semantic_result_sha256,source_root_sha256,compiled_root_sha256,requirement_root_sha256,model_request_sha256,model_response_sha256,answer_sha256,certificate_sha256,claim_validation_ids,proof_support_ids,certificate,issued_at_epoch_ms) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21)").bind(input.tenant_id).bind(input.certificate_id).bind(input.execution_id).bind(input.dataset_id).bind(input.snapshot_id).bind(input.query_execution_id).bind(input.authorized_graph_set_sha256.0.as_slice()).bind(input.active_dataset_sha256.0.as_slice()).bind(input.serving_root_sha256.0.as_slice()).bind(input.semantic_result_sha256.0.as_slice()).bind(input.source_root_sha256.0.as_slice()).bind(input.compiled_root_sha256.0.as_slice()).bind(input.requirement_root_sha256.0.as_slice()).bind(input.model_request_sha256.0.as_slice()).bind(input.model_response_sha256.0.as_slice()).bind(input.answer_sha256.0.as_slice()).bind(input.certificate_sha256.0.as_slice()).bind(serde_json::to_value(&input.claim_validation_ids)?).bind(serde_json::to_value(&input.proof_support_ids)?).bind(&input.certificate).bind(input.issued_at_epoch_ms).execute(&mut *transaction).await?;
        transaction.commit().await?;
        Ok(())
    }

    /// Atomically insert an answer certificate and complete its execution CAS.
    pub async fn complete_answer_certificate(
        &self,
        input: &AnswerCertificateRecord,
        transition: &ExecutionTransition,
    ) -> Result<i64, CatalogError> {
        validate_transition(transition)?;
        if transition.tenant_id != input.tenant_id
            || transition.execution_id != input.execution_id
            || transition.expected_state != ExecutionState::Validating
            || transition.next_state != ExecutionState::Completed
            || transition.result_sha256 != Some(input.certificate_sha256)
        {
            return Err(CatalogError::Invalid(
                "certificate completion transition is inconsistent",
            ));
        }
        let next_version = transition
            .expected_state_version
            .checked_add(1)
            .ok_or(CatalogError::Overflow)?;
        let mut transaction = self.tenant_transaction(input.tenant_id).await?;
        sqlx::query("INSERT INTO ngkg_agents.agent_answer_certificate(tenant_id,certificate_id,execution_id,dataset_id,snapshot_id,query_execution_id,authorized_graph_set_sha256,active_dataset_sha256,serving_root_sha256,semantic_result_sha256,source_root_sha256,compiled_root_sha256,requirement_root_sha256,model_request_sha256,model_response_sha256,answer_sha256,certificate_sha256,claim_validation_ids,proof_support_ids,certificate,issued_at_epoch_ms) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21)").bind(input.tenant_id).bind(input.certificate_id).bind(input.execution_id).bind(input.dataset_id).bind(input.snapshot_id).bind(input.query_execution_id).bind(input.authorized_graph_set_sha256.0.as_slice()).bind(input.active_dataset_sha256.0.as_slice()).bind(input.serving_root_sha256.0.as_slice()).bind(input.semantic_result_sha256.0.as_slice()).bind(input.source_root_sha256.0.as_slice()).bind(input.compiled_root_sha256.0.as_slice()).bind(input.requirement_root_sha256.0.as_slice()).bind(input.model_request_sha256.0.as_slice()).bind(input.model_response_sha256.0.as_slice()).bind(input.answer_sha256.0.as_slice()).bind(input.certificate_sha256.0.as_slice()).bind(serde_json::to_value(&input.claim_validation_ids)?).bind(serde_json::to_value(&input.proof_support_ids)?).bind(&input.certificate).bind(input.issued_at_epoch_ms).execute(&mut *transaction).await?;
        let row=sqlx::query("UPDATE ngkg_agents.agent_execution SET state='COMPLETED',state_version=$1,ended_at_epoch_ms=$2,result_sha256=$3,failure_code=NULL WHERE tenant_id=$4 AND execution_id=$5 AND state='VALIDATING' AND state_version=$6 RETURNING state_version").bind(next_version).bind(transition.ended_at_epoch_ms).bind(input.certificate_sha256.0.as_slice()).bind(input.tenant_id).bind(input.execution_id).bind(transition.expected_state_version).fetch_optional(&mut *transaction).await?;
        let Some(row) = row else {
            return Err(CatalogError::Conflict("certificate completion CAS failed"));
        };
        let observed: i64 = row.try_get("state_version")?;
        transaction.commit().await?;
        Ok(observed)
    }

    /// Insert one immutable retention-policy version.
    pub async fn record_retention_policy(
        &self,
        input: &RetentionPolicyVersion,
    ) -> Result<(), CatalogError> {
        validate_retention_policy(input)?;
        let mut transaction = self.tenant_transaction(input.tenant_id).await?;
        sqlx::query(
            "INSERT INTO ngkg_agents.retention_policy \
             (tenant_id, policy_id, version, minimum_retention_days, legal_hold, external_worm_required, \
              policy_sha256, created_by, created_at_epoch_ms) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)",
        )
        .bind(input.tenant_id)
        .bind(input.policy_id)
        .bind(input.version)
        .bind(input.minimum_retention_days)
        .bind(input.legal_hold)
        .bind(input.external_worm_required)
        .bind(input.policy_sha256.0.as_slice())
        .bind(&input.created_by)
        .bind(input.created_at_epoch_ms)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    /// Insert an immutable agent execution identity in `ADMITTED` state.
    pub async fn begin_execution(&self, input: &AgentExecutionStart) -> Result<(), CatalogError> {
        validate_execution_start(input)?;
        let mut transaction = self.tenant_transaction(input.tenant_id).await?;
        sqlx::query(
            "INSERT INTO ngkg_agents.agent_execution \
             (tenant_id, execution_id, subject, actor, dataset_id, profile_id, profile_version, \
              model_provider, model_id, state, state_version, started_at_epoch_ms) \
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,'ADMITTED',0,$10)",
        )
        .bind(input.tenant_id)
        .bind(input.execution_id)
        .bind(&input.subject)
        .bind(&input.actor)
        .bind(input.dataset_id)
        .bind(input.profile_id)
        .bind(input.profile_version)
        .bind(&input.model_provider)
        .bind(&input.model_id)
        .bind(input.started_at_epoch_ms)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    /// Compare-and-swap one legal execution transition.
    pub async fn transition_execution(
        &self,
        transition: &ExecutionTransition,
    ) -> Result<i64, CatalogError> {
        validate_transition(transition)?;
        let mut transaction = self.tenant_transaction(transition.tenant_id).await?;
        let next_version = transition
            .expected_state_version
            .checked_add(1)
            .ok_or(CatalogError::Overflow)?;
        let row = sqlx::query(
            "UPDATE ngkg_agents.agent_execution SET state=$1, state_version=$2, snapshot_id=COALESCE($3,snapshot_id), \
             authorized_graph_set_sha256=COALESCE($4,authorized_graph_set_sha256), \
             active_dataset_sha256=COALESCE($5,active_dataset_sha256), \
             serving_root_sha256=COALESCE($6,serving_root_sha256), \
             ended_at_epoch_ms=$7, result_sha256=$8, failure_code=$9 \
             WHERE tenant_id=$10 AND execution_id=$11 AND state_version=$12 AND state=$13 \
             RETURNING state_version",
        )
        .bind(transition.next_state.as_str())
        .bind(next_version)
        .bind(transition.snapshot_id)
        .bind(hash_option_bytes(transition.authorized_graph_set_sha256))
        .bind(hash_option_bytes(transition.active_dataset_sha256))
        .bind(hash_option_bytes(transition.serving_root_sha256))
        .bind(transition.ended_at_epoch_ms)
        .bind(hash_option_bytes(transition.result_sha256))
        .bind(&transition.failure_code)
        .bind(transition.tenant_id)
        .bind(transition.execution_id)
        .bind(transition.expected_state_version)
        .bind(transition.expected_state.as_str())
        .fetch_optional(&mut *transaction)
        .await?;
        let Some(row) = row else {
            return Err(CatalogError::Conflict("agent execution CAS failed"));
        };
        let observed: i64 = row.try_get("state_version")?;
        transaction.commit().await?;
        Ok(observed)
    }

    /// Begin a finalize-once model call.
    pub async fn begin_model_call(&self, input: &ModelCallStart) -> Result<(), CatalogError> {
        validate_model_call_start(input)?;
        let mut transaction = self.tenant_transaction(input.tenant_id).await?;
        sqlx::query(
            "INSERT INTO ngkg_agents.model_call \
             (tenant_id, model_call_id, execution_id, ordinal, provider, model_id, request_sha256, started_at_epoch_ms) \
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
        )
        .bind(input.tenant_id)
        .bind(input.model_call_id)
        .bind(input.execution_id)
        .bind(input.ordinal)
        .bind(&input.provider)
        .bind(&input.model_id)
        .bind(input.request_sha256.0.as_slice())
        .bind(input.started_at_epoch_ms)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    /// Finalize a model call exactly once.
    pub async fn finalize_model_call(&self, input: &ModelCallFinish) -> Result<(), CatalogError> {
        validate_model_call_finish(input)?;
        let mut transaction = self.tenant_transaction(input.tenant_id).await?;
        let result = sqlx::query(
            "UPDATE ngkg_agents.model_call SET response_sha256=$1, input_tokens=$2, output_tokens=$3, \
             ended_at_epoch_ms=$4, outcome=$5, error_code=$6 \
             WHERE tenant_id=$7 AND model_call_id=$8 AND ended_at_epoch_ms IS NULL",
        )
        .bind(hash_option_bytes(input.response_sha256))
        .bind(input.input_tokens)
        .bind(input.output_tokens)
        .bind(input.ended_at_epoch_ms)
        .bind(input.outcome.as_str())
        .bind(&input.error_code)
        .bind(input.tenant_id)
        .bind(input.model_call_id)
        .execute(&mut *transaction)
        .await?;
        if result.rows_affected() != 1 {
            return Err(CatalogError::Conflict(
                "model call already finalized or missing",
            ));
        }
        transaction.commit().await?;
        Ok(())
    }

    /// Begin a finalize-once tool call.
    pub async fn begin_tool_call(&self, input: &ToolCallStart) -> Result<(), CatalogError> {
        validate_tool_call_start(input)?;
        let mut transaction = self.tenant_transaction(input.tenant_id).await?;
        sqlx::query(
            "INSERT INTO ngkg_agents.tool_call \
             (tenant_id, tool_call_id, execution_id, ordinal, provider_id, tool_name, catalog_sha256, \
              arguments_sha256, approval_id, started_at_epoch_ms) \
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)",
        )
        .bind(input.tenant_id)
        .bind(input.tool_call_id)
        .bind(input.execution_id)
        .bind(input.ordinal)
        .bind(input.provider_id)
        .bind(&input.tool_name)
        .bind(hash_option_bytes(input.catalog_sha256))
        .bind(input.arguments_sha256.0.as_slice())
        .bind(input.approval_id)
        .bind(input.started_at_epoch_ms)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    /// Finalize a tool call exactly once.
    pub async fn finalize_tool_call(&self, input: &ToolCallFinish) -> Result<(), CatalogError> {
        validate_tool_call_finish(input)?;
        let mut transaction = self.tenant_transaction(input.tenant_id).await?;
        let result = sqlx::query(
            "UPDATE ngkg_agents.tool_call SET result_sha256=$1, query_execution_id=$2, \
             ended_at_epoch_ms=$3, outcome=$4, error_code=$5 \
             WHERE tenant_id=$6 AND tool_call_id=$7 AND ended_at_epoch_ms IS NULL",
        )
        .bind(hash_option_bytes(input.result_sha256))
        .bind(input.query_execution_id)
        .bind(input.ended_at_epoch_ms)
        .bind(input.outcome.as_str())
        .bind(&input.error_code)
        .bind(input.tenant_id)
        .bind(input.tool_call_id)
        .execute(&mut *transaction)
        .await?;
        if result.rows_affected() != 1 {
            return Err(CatalogError::Conflict(
                "tool call already finalized or missing",
            ));
        }
        transaction.commit().await?;
        Ok(())
    }

    /// Store one immutable claim-validation verdict.
    pub async fn record_claim_validation(
        &self,
        input: &ClaimValidation,
    ) -> Result<(), CatalogError> {
        validate_claim(input)?;
        let mut transaction = self.tenant_transaction(input.tenant_id).await?;
        sqlx::query(
            "INSERT INTO ngkg_agents.claim_validation \
             (tenant_id, validation_id, execution_id, claim_sha256, verdict, query_execution_id, \
              proof_support_ids, reason_code, evidence_sha256, created_at_epoch_ms) \
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)",
        )
        .bind(input.tenant_id)
        .bind(input.validation_id)
        .bind(input.execution_id)
        .bind(input.claim_sha256.0.as_slice())
        .bind(input.verdict.as_str())
        .bind(input.query_execution_id)
        .bind(serde_json::to_value(&input.proof_support_ids)?)
        .bind(&input.reason_code)
        .bind(input.evidence_sha256.0.as_slice())
        .bind(input.created_at_epoch_ms)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    /// Store one immutable and explicitly classified resource observation.
    pub async fn record_resource_observation(
        &self,
        input: &ExecutionResourceObservation,
    ) -> Result<(), CatalogError> {
        validate_resource_observation(input)?;
        let mut transaction = self.tenant_transaction(input.tenant_id).await?;
        sqlx::query(
            "INSERT INTO ngkg_agents.execution_resource_observation \
             (tenant_id, observation_id, execution_id, resource_semantics, source, \
              participating_pods, distinct_physical_nodes, cpu_millicores, memory_bytes, \
              interval_start_epoch_ms, interval_end_epoch_ms, evidence_sha256) \
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)",
        )
        .bind(input.tenant_id)
        .bind(input.observation_id)
        .bind(input.execution_id)
        .bind(input.resource_semantics.as_str())
        .bind(&input.source)
        .bind(input.participating_pods)
        .bind(input.distinct_physical_nodes)
        .bind(input.cpu_millicores)
        .bind(input.memory_bytes)
        .bind(input.interval_start_epoch_ms)
        .bind(input.interval_end_epoch_ms)
        .bind(input.evidence_sha256.0.as_slice())
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    /// Store one immutable approval decision.
    pub async fn record_approval(&self, input: &ApprovalRecord) -> Result<(), CatalogError> {
        validate_approval(input)?;
        let mut transaction = self.tenant_transaction(input.tenant_id).await?;
        sqlx::query(
            "INSERT INTO ngkg_agents.approval \
             (tenant_id, approval_id, execution_id, tool_name, approver, policy_sha256, catalog_sha256, \
              decision, expires_at_epoch_ms, created_at_epoch_ms) \
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)",
        )
        .bind(input.tenant_id)
        .bind(input.approval_id)
        .bind(input.execution_id)
        .bind(&input.tool_name)
        .bind(&input.approver)
        .bind(input.policy_sha256.0.as_slice())
        .bind(hash_option_bytes(input.catalog_sha256))
        .bind(input.decision.as_str())
        .bind(input.expires_at_epoch_ms)
        .bind(input.created_at_epoch_ms)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    async fn tenant_transaction(
        &self,
        tenant_id: Uuid,
    ) -> Result<Transaction<'_, Postgres>, CatalogError> {
        if tenant_id.is_nil() {
            return Err(CatalogError::Invalid("tenant UUID is nil"));
        }
        let mut transaction = self.pool.begin().await?;
        let installed: String = sqlx::query_scalar("SELECT set_config('ngkg.tenant_id', $1, true)")
            .bind(tenant_id.to_string())
            .fetch_one(&mut *transaction)
            .await?;
        if installed != tenant_id.to_string() {
            return Err(CatalogError::Evidence("tenant session binding failed"));
        }
        Ok(transaction)
    }
}

/// Immutable audit event input. Raw prompts, queries, tokens, and credentials are prohibited.
#[derive(Clone, Debug)]
pub struct AuditEventInput {
    pub event_id: Uuid,
    pub event_type: String,
    pub subject: String,
    pub actor: Option<String>,
    pub request_id: String,
    pub outcome: AuditOutcome,
    pub policy_version_sha256: Hash32,
    pub service_build_sha256: Hash32,
    pub redacted_payload_sha256: Hash32,
    pub event_time_epoch_ms: i64,
}

/// Audit outcome vocabulary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AuditOutcome {
    Started,
    Completed,
    Failed,
    Denied,
    Cancelled,
}

impl AuditOutcome {
    /// Stable database and audit-hash representation.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Started => "STARTED",
            Self::Completed => "COMPLETED",
            Self::Failed => "FAILED",
            Self::Denied => "DENIED",
            Self::Cancelled => "CANCELLED",
        }
    }
}

/// Appended chain identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AuditChainRecord {
    pub tenant_id: Uuid,
    pub sequence: i64,
    pub event_id: Uuid,
    pub previous_event_sha256: Hash32,
    pub event_sha256: Hash32,
}

/// Immutable external WORM sealing receipt.
#[derive(Clone, Debug)]
pub struct AuditSeal {
    pub tenant_id: Uuid,
    pub seal_id: Uuid,
    pub through_sequence: i64,
    pub chain_head_sha256: Hash32,
    pub external_target: String,
    pub external_receipt_sha256: Hash32,
    pub sealed_by: String,
    pub sealed_at_epoch_ms: i64,
}

/// Provider lifecycle state. Every change is a new immutable version.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProviderState {
    Pending,
    Qualified,
    Disabled,
    Revoked,
}

impl ProviderState {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "PENDING",
            Self::Qualified => "QUALIFIED",
            Self::Disabled => "DISABLED",
            Self::Revoked => "REVOKED",
        }
    }
}

/// Immutable user-tool provider version.
#[derive(Clone, Debug)]
pub struct ToolProviderVersion {
    pub tenant_id: Uuid,
    pub provider_id: Uuid,
    pub version: i64,
    pub name: String,
    pub endpoint: String,
    pub auth_reference: String,
    pub policy: serde_json::Value,
    pub state: ProviderState,
    pub spec_sha256: Hash32,
    pub created_by: String,
    pub created_at_epoch_ms: i64,
}

/// Immutable discovered and qualified MCP catalog.
#[derive(Clone, Debug)]
pub struct ToolCatalogVersion {
    pub tenant_id: Uuid,
    pub provider_id: Uuid,
    pub provider_version: i64,
    pub catalog_sha256: Hash32,
    pub protocol_version: String,
    pub discovered_tools: serde_json::Value,
    pub qualification_evidence_sha256: Hash32,
    pub created_at_epoch_ms: i64,
}

#[derive(Clone, Debug)]
pub struct ToolExecutionContext {
    pub tenant_id: Uuid,
    pub execution_id: Uuid,
    pub profile_id: Uuid,
    pub profile_version: i64,
    pub state: String,
    pub result_sha256: Option<Hash32>,
}

/// Immutable agent-profile version.
#[derive(Clone, Debug)]
pub struct AgentProfileVersion {
    pub tenant_id: Uuid,
    pub profile_id: Uuid,
    pub version: i64,
    pub name: String,
    pub dataset_constraints: serde_json::Value,
    pub model_allowlist: serde_json::Value,
    pub tool_catalog_sha256s: serde_json::Value,
    pub limits: serde_json::Value,
    pub approval_policy: serde_json::Value,
    pub profile_sha256: Hash32,
    pub created_by: String,
    pub created_at_epoch_ms: i64,
}

/// Immutable audit-retention policy version.
#[derive(Clone, Debug)]
pub struct RetentionPolicyVersion {
    pub tenant_id: Uuid,
    pub policy_id: Uuid,
    pub version: i64,
    pub minimum_retention_days: i32,
    pub legal_hold: bool,
    pub external_worm_required: bool,
    pub policy_sha256: Hash32,
    pub created_by: String,
    pub created_at_epoch_ms: i64,
}

#[derive(Clone, Debug)]
pub struct ExecutionInputRecord {
    pub tenant_id: Uuid,
    pub input_id: Uuid,
    pub execution_id: Uuid,
    pub source_root_sha256: Hash32,
    pub compiled_root_sha256: Hash32,
    pub requirement_root_sha256: Hash32,
    pub context_query_sha256: Hash32,
    pub created_at_epoch_ms: i64,
}

#[derive(Clone, Debug)]
pub struct AnswerCertificateRecord {
    pub tenant_id: Uuid,
    pub certificate_id: Uuid,
    pub execution_id: Uuid,
    pub dataset_id: Uuid,
    pub snapshot_id: Uuid,
    pub query_execution_id: Uuid,
    pub authorized_graph_set_sha256: Hash32,
    pub active_dataset_sha256: Hash32,
    pub serving_root_sha256: Hash32,
    pub semantic_result_sha256: Hash32,
    pub source_root_sha256: Hash32,
    pub compiled_root_sha256: Hash32,
    pub requirement_root_sha256: Hash32,
    pub model_request_sha256: Hash32,
    pub model_response_sha256: Hash32,
    pub answer_sha256: Hash32,
    pub certificate_sha256: Hash32,
    pub claim_validation_ids: Vec<Uuid>,
    pub proof_support_ids: Vec<String>,
    pub certificate: serde_json::Value,
    pub issued_at_epoch_ms: i64,
}

#[derive(Debug)]
struct ExistingAuditEvent {
    sequence: i64,
    event_sha256: Hash32,
    previous_event_sha256: Hash32,
    event_type: String,
    subject: String,
    actor: Option<String>,
    request_id: String,
    outcome: String,
    policy_version_sha256: Hash32,
    service_build_sha256: Hash32,
    redacted_payload_sha256: Hash32,
    event_time_epoch_ms: i64,
}

/// Immutable execution identity.
#[derive(Clone, Debug)]
pub struct AgentExecutionStart {
    pub tenant_id: Uuid,
    pub execution_id: Uuid,
    pub subject: String,
    pub actor: Option<String>,
    pub dataset_id: Uuid,
    pub profile_id: Uuid,
    pub profile_version: i64,
    pub model_provider: String,
    pub model_id: String,
    pub started_at_epoch_ms: i64,
}

/// Agent execution state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExecutionState {
    Admitted,
    Running,
    WaitingApproval,
    Validating,
    Completed,
    Failed,
    Cancelled,
}

impl ExecutionState {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Admitted => "ADMITTED",
            Self::Running => "RUNNING",
            Self::WaitingApproval => "WAITING_APPROVAL",
            Self::Validating => "VALIDATING",
            Self::Completed => "COMPLETED",
            Self::Failed => "FAILED",
            Self::Cancelled => "CANCELLED",
        }
    }

    const fn terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled)
    }
}

/// Compare-and-swap execution update.
#[derive(Clone, Debug)]
pub struct ExecutionTransition {
    pub tenant_id: Uuid,
    pub execution_id: Uuid,
    pub expected_state: ExecutionState,
    pub expected_state_version: i64,
    pub next_state: ExecutionState,
    pub snapshot_id: Option<Uuid>,
    pub authorized_graph_set_sha256: Option<Hash32>,
    pub active_dataset_sha256: Option<Hash32>,
    pub serving_root_sha256: Option<Hash32>,
    pub ended_at_epoch_ms: Option<i64>,
    pub result_sha256: Option<Hash32>,
    pub failure_code: Option<String>,
}

/// Initial model-call fields.
#[derive(Clone, Debug)]
pub struct ModelCallStart {
    pub tenant_id: Uuid,
    pub model_call_id: Uuid,
    pub execution_id: Uuid,
    pub ordinal: i32,
    pub provider: String,
    pub model_id: String,
    pub request_sha256: Hash32,
    pub started_at_epoch_ms: i64,
}

/// Terminal call outcome.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CallOutcome {
    Completed,
    Failed,
    Cancelled,
    Denied,
}

impl CallOutcome {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "COMPLETED",
            Self::Failed => "FAILED",
            Self::Cancelled => "CANCELLED",
            Self::Denied => "DENIED",
        }
    }
}

/// Model-call finalization.
#[derive(Clone, Debug)]
pub struct ModelCallFinish {
    pub tenant_id: Uuid,
    pub model_call_id: Uuid,
    pub response_sha256: Option<Hash32>,
    pub input_tokens: Option<i64>,
    pub output_tokens: Option<i64>,
    pub ended_at_epoch_ms: i64,
    pub outcome: CallOutcome,
    pub error_code: Option<String>,
}

/// Initial tool-call fields.
#[derive(Clone, Debug)]
pub struct ToolCallStart {
    pub tenant_id: Uuid,
    pub tool_call_id: Uuid,
    pub execution_id: Uuid,
    pub ordinal: i32,
    pub provider_id: Option<Uuid>,
    pub tool_name: String,
    pub catalog_sha256: Option<Hash32>,
    pub arguments_sha256: Hash32,
    pub approval_id: Option<Uuid>,
    pub started_at_epoch_ms: i64,
}

/// Tool-call finalization.
#[derive(Clone, Debug)]
pub struct ToolCallFinish {
    pub tenant_id: Uuid,
    pub tool_call_id: Uuid,
    pub result_sha256: Option<Hash32>,
    pub query_execution_id: Option<Uuid>,
    pub ended_at_epoch_ms: i64,
    pub outcome: CallOutcome,
    pub error_code: Option<String>,
}

/// Claim-validation verdict.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClaimVerdict {
    Entailed,
    Contradicted,
    Unknown,
    Invalid,
}

impl ClaimVerdict {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Entailed => "ENTAILED",
            Self::Contradicted => "CONTRADICTED",
            Self::Unknown => "UNKNOWN",
            Self::Invalid => "INVALID",
        }
    }
}

/// Immutable claim-validation record.
#[derive(Clone, Debug)]
pub struct ClaimValidation {
    pub tenant_id: Uuid,
    pub validation_id: Uuid,
    pub execution_id: Uuid,
    pub claim_sha256: Hash32,
    pub verdict: ClaimVerdict,
    pub query_execution_id: Option<Uuid>,
    pub proof_support_ids: Vec<String>,
    pub reason_code: String,
    pub evidence_sha256: Hash32,
    pub created_at_epoch_ms: i64,
}

/// Semantics of one resource record. Allocation must never be reported as observation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResourceSemantics {
    ConfiguredAllocation,
    ObservedUsage,
}

impl ResourceSemantics {
    const fn as_str(self) -> &'static str {
        match self {
            Self::ConfiguredAllocation => "CONFIGURED_ALLOCATION",
            Self::ObservedUsage => "OBSERVED_USAGE",
        }
    }
}

/// Immutable execution resource record with evidence identity.
#[derive(Clone, Debug)]
pub struct ExecutionResourceObservation {
    pub tenant_id: Uuid,
    pub observation_id: Uuid,
    pub execution_id: Uuid,
    pub resource_semantics: ResourceSemantics,
    pub source: String,
    pub participating_pods: i32,
    pub distinct_physical_nodes: Option<i32>,
    pub cpu_millicores: i64,
    pub memory_bytes: i64,
    pub interval_start_epoch_ms: i64,
    pub interval_end_epoch_ms: i64,
    pub evidence_sha256: Hash32,
}

/// Approval decision.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ApprovalDecision {
    Approved,
    Denied,
}

impl ApprovalDecision {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Approved => "APPROVED",
            Self::Denied => "DENIED",
        }
    }
}

/// Immutable approval record.
#[derive(Clone, Debug)]
pub struct ApprovalRecord {
    pub tenant_id: Uuid,
    pub approval_id: Uuid,
    pub execution_id: Uuid,
    pub tool_name: String,
    pub approver: String,
    pub policy_sha256: Hash32,
    pub catalog_sha256: Option<Hash32>,
    pub decision: ApprovalDecision,
    pub expires_at_epoch_ms: i64,
    pub created_at_epoch_ms: i64,
}

/// Catalog failure.
#[derive(Debug, Error)]
pub enum CatalogError {
    #[error("invalid agent catalog input: {0}")]
    Invalid(&'static str),
    #[error("agent catalog evidence mismatch: {0}")]
    Evidence(&'static str),
    #[error("agent catalog conflict: {0}")]
    Conflict(&'static str),
    #[error("agent catalog arithmetic overflow")]
    Overflow,
    #[error("agent catalog database operation failed: {0}")]
    Database(#[from] sqlx::Error),
    #[error("agent catalog migration failed: {0}")]
    Migration(#[from] sqlx::migrate::MigrateError),
    #[error("agent catalog JSON operation failed: {0}")]
    Json(#[from] serde_json::Error),
    #[error("agent catalog URL is invalid: {0}")]
    Url(#[from] url::ParseError),
}

fn validate_database_url(database_url: &str, options: CatalogOptions) -> Result<(), CatalogError> {
    if options.maximum_connections == 0 || options.acquire_timeout.is_zero() {
        return Err(CatalogError::Invalid("database pool limits are invalid"));
    }
    let url = Url::parse(database_url)?;
    if !matches!(url.scheme(), "postgres" | "postgresql") {
        return Err(CatalogError::Invalid("database URL scheme is invalid"));
    }
    let host = url
        .host_str()
        .ok_or(CatalogError::Invalid("database URL host is missing"))?;
    let loopback = matches!(host, "127.0.0.1" | "localhost" | "::1");
    let encrypted = url.query_pairs().any(|(key, value)| {
        key == "sslmode" && matches!(value.as_ref(), "require" | "verify-ca" | "verify-full")
    });
    if !(encrypted || options.allow_insecure_loopback && loopback) {
        return Err(CatalogError::Invalid(
            "encrypted PostgreSQL transport is required",
        ));
    }
    Ok(())
}

async fn existing_audit_event(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: Uuid,
    event_id: Uuid,
) -> Result<Option<ExistingAuditEvent>, CatalogError> {
    let row = sqlx::query(
        "SELECT sequence, event_sha256, previous_event_sha256, event_type, subject, actor, request_id, outcome, \
         policy_version_sha256, service_build_sha256, redacted_payload_sha256, event_time_epoch_ms \
         FROM ngkg_agents.agent_audit_chain WHERE tenant_id=$1 AND event_id=$2",
    )
    .bind(tenant_id)
    .bind(event_id)
    .fetch_optional(&mut **transaction)
    .await?;
    row.map(|value| {
        Ok(ExistingAuditEvent {
            sequence: value.try_get("sequence")?,
            event_sha256: hash_from_database(value.try_get("event_sha256")?)?,
            previous_event_sha256: hash_from_database(value.try_get("previous_event_sha256")?)?,
            event_type: value.try_get("event_type")?,
            subject: value.try_get("subject")?,
            actor: value.try_get("actor")?,
            request_id: value.try_get("request_id")?,
            outcome: value.try_get("outcome")?,
            policy_version_sha256: hash_from_database(value.try_get("policy_version_sha256")?)?,
            service_build_sha256: hash_from_database(value.try_get("service_build_sha256")?)?,
            redacted_payload_sha256: hash_from_database(value.try_get("redacted_payload_sha256")?)?,
            event_time_epoch_ms: value.try_get("event_time_epoch_ms")?,
        })
    })
    .transpose()
}

fn validate_idempotent_event(
    tenant_id: Uuid,
    existing: &ExistingAuditEvent,
    input: &AuditEventInput,
) -> Result<(), CatalogError> {
    if existing.event_type != input.event_type
        || existing.subject != input.subject
        || existing.actor != input.actor
        || existing.request_id != input.request_id
        || existing.outcome != input.outcome.as_str()
        || existing.policy_version_sha256 != input.policy_version_sha256
        || existing.service_build_sha256 != input.service_build_sha256
        || existing.redacted_payload_sha256 != input.redacted_payload_sha256
    {
        return Err(CatalogError::Conflict(
            "audit event UUID was reused with different content",
        ));
    }
    let mut canonical = input.clone();
    canonical.event_time_epoch_ms = existing.event_time_epoch_ms;
    let recomputed = audit_event_sha256(
        tenant_id,
        existing.sequence,
        existing.previous_event_sha256,
        &canonical,
    )?;
    if recomputed != existing.event_sha256 {
        return Err(CatalogError::Evidence(
            "stored audit event hash does not verify",
        ));
    }
    Ok(())
}

fn audit_event_sha256(
    tenant_id: Uuid,
    sequence: i64,
    previous: Hash32,
    input: &AuditEventInput,
) -> Result<Hash32, CatalogError> {
    let sequence = u64::try_from(sequence).map_err(|_| CatalogError::Overflow)?;
    let event_time =
        u64::try_from(input.event_time_epoch_ms).map_err(|_| CatalogError::Overflow)?;
    let mut digest = Sha256::new();
    digest.update(AUDIT_DOMAIN);
    digest.update(tenant_id.as_bytes());
    digest.update(sequence.to_be_bytes());
    digest.update(input.event_id.as_bytes());
    update_bytes(&mut digest, input.event_type.as_bytes())?;
    update_bytes(&mut digest, input.subject.as_bytes())?;
    match &input.actor {
        Some(actor) => {
            digest.update([1]);
            update_bytes(&mut digest, actor.as_bytes())?;
        }
        None => digest.update([0]),
    }
    update_bytes(&mut digest, input.request_id.as_bytes())?;
    update_bytes(&mut digest, input.outcome.as_str().as_bytes())?;
    digest.update(input.policy_version_sha256.0);
    digest.update(input.service_build_sha256.0);
    digest.update(input.redacted_payload_sha256.0);
    digest.update(previous.0);
    digest.update(event_time.to_be_bytes());
    Ok(Hash32(digest.finalize().into()))
}

fn update_bytes(digest: &mut Sha256, bytes: &[u8]) -> Result<(), CatalogError> {
    let length = u64::try_from(bytes.len()).map_err(|_| CatalogError::Overflow)?;
    digest.update(length.to_be_bytes());
    digest.update(bytes);
    Ok(())
}

fn hash_from_database(bytes: Vec<u8>) -> Result<Hash32, CatalogError> {
    bytes
        .try_into()
        .map(Hash32)
        .map_err(|_| CatalogError::Evidence("database SHA-256 length changed"))
}

fn hash_option_bytes(value: Option<Hash32>) -> Option<Vec<u8>> {
    value.map(|hash| hash.0.to_vec())
}

fn validate_audit_input(tenant_id: Uuid, input: &AuditEventInput) -> Result<(), CatalogError> {
    if tenant_id.is_nil()
        || input.event_id.is_nil()
        || input.event_type.is_empty()
        || input.event_type.len() > 128
        || !input
            .event_type
            .bytes()
            .next()
            .is_some_and(|byte| byte.is_ascii_uppercase())
        || !input
            .event_type
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
        || !bounded(&input.subject, 256)
        || input
            .actor
            .as_ref()
            .is_some_and(|value| !bounded(value, 256))
        || !bounded(&input.request_id, 128)
        || input.event_time_epoch_ms < 0
    {
        return Err(CatalogError::Invalid(
            "audit event fields are outside bounds",
        ));
    }
    Ok(())
}

fn validate_audit_seal(input: &AuditSeal) -> Result<(), CatalogError> {
    if input.tenant_id.is_nil()
        || input.seal_id.is_nil()
        || input.through_sequence < 0
        || !bounded(&input.external_target, 1024)
        || !bounded(&input.sealed_by, 256)
        || input.sealed_at_epoch_ms < 0
    {
        return Err(CatalogError::Invalid(
            "audit seal fields are outside bounds",
        ));
    }
    Ok(())
}

fn validate_provider(input: &ToolProviderVersion) -> Result<(), CatalogError> {
    let endpoint = Url::parse(&input.endpoint)?;
    if input.tenant_id.is_nil()
        || input.provider_id.is_nil()
        || input.version <= 0
        || input.created_at_epoch_ms < 0
        || !bounded(&input.name, 256)
        || !bounded(&input.auth_reference, 1024)
        || !bounded(&input.created_by, 256)
        || endpoint.scheme() != "https"
        || endpoint.host_str().is_none()
        || !endpoint.username().is_empty()
        || endpoint.password().is_some()
        || endpoint.query().is_some()
        || endpoint.fragment().is_some()
        || encoded_json_size(&input.policy)? > 1_048_576
    {
        return Err(CatalogError::Invalid(
            "tool provider fields are outside bounds",
        ));
    }
    Ok(())
}

fn validate_tool_catalog(input: &ToolCatalogVersion) -> Result<(), CatalogError> {
    if input.tenant_id.is_nil()
        || input.provider_id.is_nil()
        || input.provider_version <= 0
        || input.created_at_epoch_ms < 0
        || !bounded(&input.protocol_version, 64)
        || !input.discovered_tools.is_array()
        || encoded_json_size(&input.discovered_tools)? > 8_388_608
    {
        return Err(CatalogError::Invalid(
            "tool catalog fields are outside bounds",
        ));
    }
    Ok(())
}

fn validate_profile(input: &AgentProfileVersion) -> Result<(), CatalogError> {
    if input.tenant_id.is_nil()
        || input.profile_id.is_nil()
        || input.version <= 0
        || input.created_at_epoch_ms < 0
        || !bounded(&input.name, 256)
        || !bounded(&input.created_by, 256)
    {
        return Err(CatalogError::Invalid(
            "agent profile fields are outside bounds",
        ));
    }
    for value in [
        &input.dataset_constraints,
        &input.model_allowlist,
        &input.tool_catalog_sha256s,
        &input.limits,
        &input.approval_policy,
    ] {
        if encoded_json_size(value)? > 1_048_576 {
            return Err(CatalogError::Invalid("agent profile JSON exceeds bounds"));
        }
    }
    Ok(())
}

fn validate_retention_policy(input: &RetentionPolicyVersion) -> Result<(), CatalogError> {
    if input.tenant_id.is_nil()
        || input.policy_id.is_nil()
        || input.version <= 0
        || input.minimum_retention_days < 1
        || input.created_at_epoch_ms < 0
        || !bounded(&input.created_by, 256)
    {
        return Err(CatalogError::Invalid(
            "retention policy fields are outside bounds",
        ));
    }
    Ok(())
}

fn validate_execution_start(input: &AgentExecutionStart) -> Result<(), CatalogError> {
    if input.tenant_id.is_nil()
        || input.execution_id.is_nil()
        || input.dataset_id.is_nil()
        || input.profile_id.is_nil()
        || input.profile_version <= 0
        || input.started_at_epoch_ms < 0
        || !bounded(&input.subject, 256)
        || input
            .actor
            .as_ref()
            .is_some_and(|value| !bounded(value, 256))
        || !bounded(&input.model_provider, 128)
        || !bounded(&input.model_id, 512)
    {
        return Err(CatalogError::Invalid(
            "execution identity fields are outside bounds",
        ));
    }
    Ok(())
}

fn validate_transition(input: &ExecutionTransition) -> Result<(), CatalogError> {
    let legal = match input.expected_state {
        ExecutionState::Admitted => matches!(
            input.next_state,
            ExecutionState::Running | ExecutionState::Failed | ExecutionState::Cancelled
        ),
        ExecutionState::Running => matches!(
            input.next_state,
            ExecutionState::WaitingApproval
                | ExecutionState::Validating
                | ExecutionState::Failed
                | ExecutionState::Cancelled
        ),
        ExecutionState::WaitingApproval => matches!(
            input.next_state,
            ExecutionState::Running | ExecutionState::Failed | ExecutionState::Cancelled
        ),
        ExecutionState::Validating => matches!(
            input.next_state,
            ExecutionState::Completed | ExecutionState::Failed | ExecutionState::Cancelled
        ),
        ExecutionState::Completed | ExecutionState::Failed | ExecutionState::Cancelled => false,
    };
    if input.tenant_id.is_nil()
        || input.execution_id.is_nil()
        || input.expected_state_version < 0
        || !legal
        || input.next_state.terminal() != input.ended_at_epoch_ms.is_some()
        || (matches!(input.next_state, ExecutionState::Completed) && input.result_sha256.is_none())
        || (matches!(input.next_state, ExecutionState::Failed) && input.failure_code.is_none())
        || input.ended_at_epoch_ms.is_some_and(|value| value < 0)
        || input
            .failure_code
            .as_ref()
            .is_some_and(|value| !bounded(value, 128))
    {
        return Err(CatalogError::Invalid("execution transition is invalid"));
    }
    Ok(())
}

fn validate_model_call_start(input: &ModelCallStart) -> Result<(), CatalogError> {
    if input.tenant_id.is_nil()
        || input.model_call_id.is_nil()
        || input.execution_id.is_nil()
        || input.ordinal < 0
        || input.started_at_epoch_ms < 0
        || !bounded(&input.provider, 128)
        || !bounded(&input.model_id, 512)
    {
        return Err(CatalogError::Invalid(
            "model call fields are outside bounds",
        ));
    }
    Ok(())
}

fn validate_tool_call_start(input: &ToolCallStart) -> Result<(), CatalogError> {
    if input.tenant_id.is_nil()
        || input.tool_call_id.is_nil()
        || input.execution_id.is_nil()
        || input.provider_id.is_some_and(|value| value.is_nil())
        || input.approval_id.is_some_and(|value| value.is_nil())
        || input.ordinal < 0
        || input.started_at_epoch_ms < 0
        || !bounded(&input.tool_name, 512)
    {
        return Err(CatalogError::Invalid("tool call fields are outside bounds"));
    }
    Ok(())
}

fn validate_call_finish(
    ended_at_epoch_ms: i64,
    outcome: CallOutcome,
    error_code: &Option<String>,
    allow_denied: bool,
) -> Result<(), CatalogError> {
    if ended_at_epoch_ms < 0
        || (!allow_denied && matches!(outcome, CallOutcome::Denied))
        || error_code
            .as_ref()
            .is_some_and(|value| !bounded(value, 128))
    {
        return Err(CatalogError::Invalid(
            "call finalization fields are invalid",
        ));
    }
    Ok(())
}

fn validate_model_call_finish(input: &ModelCallFinish) -> Result<(), CatalogError> {
    if input.tenant_id.is_nil() || input.model_call_id.is_nil() {
        return Err(CatalogError::Invalid("model call identity is invalid"));
    }
    validate_call_finish(
        input.ended_at_epoch_ms,
        input.outcome,
        &input.error_code,
        false,
    )?;
    if input.input_tokens.is_some_and(|value| value < 0)
        || input.output_tokens.is_some_and(|value| value < 0)
        || (matches!(input.outcome, CallOutcome::Completed) && input.response_sha256.is_none())
    {
        return Err(CatalogError::Invalid(
            "model call result fields are invalid",
        ));
    }
    Ok(())
}

fn validate_tool_call_finish(input: &ToolCallFinish) -> Result<(), CatalogError> {
    if input.tenant_id.is_nil()
        || input.tool_call_id.is_nil()
        || input.query_execution_id.is_some_and(|value| value.is_nil())
    {
        return Err(CatalogError::Invalid("tool call identity is invalid"));
    }
    validate_call_finish(
        input.ended_at_epoch_ms,
        input.outcome,
        &input.error_code,
        true,
    )?;
    if matches!(input.outcome, CallOutcome::Completed) && input.result_sha256.is_none() {
        return Err(CatalogError::Invalid(
            "completed tool call requires a result hash",
        ));
    }
    Ok(())
}

fn validate_claim(input: &ClaimValidation) -> Result<(), CatalogError> {
    if input.tenant_id.is_nil()
        || input.validation_id.is_nil()
        || input.execution_id.is_nil()
        || input.created_at_epoch_ms < 0
        || !bounded(&input.reason_code, 128)
        || input.proof_support_ids.len() > 10_000
        || input
            .proof_support_ids
            .iter()
            .any(|value| !bounded(value, 1024))
    {
        return Err(CatalogError::Invalid(
            "claim-validation fields are outside bounds",
        ));
    }
    Ok(())
}

fn validate_resource_observation(input: &ExecutionResourceObservation) -> Result<(), CatalogError> {
    if input.tenant_id.is_nil()
        || input.observation_id.is_nil()
        || input.execution_id.is_nil()
        || !bounded(&input.source, 128)
        || input.participating_pods < 0
        || input.distinct_physical_nodes.is_some_and(|value| value < 0)
        || (matches!(
            input.resource_semantics,
            ResourceSemantics::ConfiguredAllocation
        ) && input.distinct_physical_nodes.is_some())
        || input.cpu_millicores < 0
        || input.memory_bytes < 0
        || input.interval_start_epoch_ms < 0
        || input.interval_end_epoch_ms < input.interval_start_epoch_ms
    {
        return Err(CatalogError::Invalid(
            "resource observation fields are outside bounds",
        ));
    }
    Ok(())
}

fn validate_approval(input: &ApprovalRecord) -> Result<(), CatalogError> {
    if input.tenant_id.is_nil()
        || input.approval_id.is_nil()
        || input.execution_id.is_nil()
        || input.created_at_epoch_ms < 0
        || input.expires_at_epoch_ms < input.created_at_epoch_ms
        || !bounded(&input.tool_name, 512)
        || !bounded(&input.approver, 256)
    {
        return Err(CatalogError::Invalid("approval fields are outside bounds"));
    }
    Ok(())
}

fn bounded(value: &str, maximum: usize) -> bool {
    !value.is_empty() && value.len() <= maximum
}

fn encoded_json_size(value: &serde_json::Value) -> Result<usize, CatalogError> {
    Ok(serde_json::to_vec(value)?.len())
}

#[cfg(test)]
mod tests {
    use super::{
        AuditEventInput, AuditOutcome, CatalogError, ExistingAuditEvent, Hash32, ZERO_HASH,
        audit_event_sha256, validate_idempotent_event,
    };
    use uuid::Uuid;

    fn input() -> AuditEventInput {
        AuditEventInput {
            event_id: Uuid::from_u128(2),
            event_type: "MCP_TOOL_STARTED".to_owned(),
            subject: "principal".to_owned(),
            actor: Some("ngkg-mcp-gateway".to_owned()),
            request_id: "request".to_owned(),
            outcome: AuditOutcome::Started,
            policy_version_sha256: Hash32([1; 32]),
            service_build_sha256: Hash32([2; 32]),
            redacted_payload_sha256: Hash32([3; 32]),
            event_time_epoch_ms: 1_000,
        }
    }

    #[test]
    fn audit_hash_is_stable_and_domain_bound() -> Result<(), CatalogError> {
        let first = audit_event_sha256(Uuid::from_u128(1), 0, ZERO_HASH, &input())?;
        let second = audit_event_sha256(Uuid::from_u128(1), 0, ZERO_HASH, &input())?;
        let other = audit_event_sha256(Uuid::from_u128(3), 0, ZERO_HASH, &input())?;
        assert_eq!(first, second);
        assert_ne!(first, other);
        assert_eq!(
            first.to_lower_hex(),
            "8f060b1265dabfb7f7f137db0a8b83c3f8a2de3435cf53cb24e69240aadb788b"
        );
        Ok(())
    }

    #[test]
    fn canonical_hash_parser_rejects_uppercase() {
        assert!(Hash32::from_lower_hex(&"a".repeat(64)).is_ok());
        assert!(Hash32::from_lower_hex(&"A".repeat(64)).is_err());
    }

    #[test]
    fn audit_retry_reuses_persisted_event_time() -> Result<(), CatalogError> {
        let tenant_id = Uuid::from_u128(1);
        let original = input();
        let event_sha256 = audit_event_sha256(tenant_id, 0, ZERO_HASH, &original)?;
        let existing = ExistingAuditEvent {
            sequence: 0,
            event_sha256,
            previous_event_sha256: ZERO_HASH,
            event_type: original.event_type.clone(),
            subject: original.subject.clone(),
            actor: original.actor.clone(),
            request_id: original.request_id.clone(),
            outcome: original.outcome.as_str().to_owned(),
            policy_version_sha256: original.policy_version_sha256,
            service_build_sha256: original.service_build_sha256,
            redacted_payload_sha256: original.redacted_payload_sha256,
            event_time_epoch_ms: original.event_time_epoch_ms,
        };
        let mut retry = original;
        retry.event_time_epoch_ms += 500;
        validate_idempotent_event(tenant_id, &existing, &retry)
    }
}
