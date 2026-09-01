//! Redacted, fail-closed gateway audit adapter.

use std::time::{SystemTime, UNIX_EPOCH};

use ngkg_agent_catalog::{AgentCatalog, AuditEventInput, AuditOutcome, CatalogError, Hash32};
use serde::Serialize;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::auth::GatewayIdentity;

const PAYLOAD_DOMAIN: &[u8] = b"ngkg-mcp-redacted-payload-v1\0";

#[derive(Clone)]
pub(crate) struct GatewayAudit {
    catalog: AgentCatalog,
    service_build_sha256: Hash32,
}

impl GatewayAudit {
    pub(crate) const fn new(catalog: AgentCatalog, service_build_sha256: Hash32) -> Self {
        Self {
            catalog,
            service_build_sha256,
        }
    }

    pub(crate) async fn ready(&self) -> Result<(), CatalogError> {
        self.catalog.ready().await
    }

    pub(crate) async fn append(
        &self,
        identity: &GatewayIdentity,
        request_id: &str,
        outcome: AuditOutcome,
        redacted_payload_sha256: Hash32,
    ) -> Result<(), CatalogError> {
        self.append_operation(
            "MCP_TOOL_CALL",
            identity,
            request_id,
            outcome,
            redacted_payload_sha256,
        )
        .await
    }

    pub(crate) async fn append_operation(
        &self,
        event_type: &'static str,
        identity: &GatewayIdentity,
        request_id: &str,
        outcome: AuditOutcome,
        redacted_payload_sha256: Hash32,
    ) -> Result<(), CatalogError> {
        let event = AuditEventInput {
            event_id: deterministic_event_id(identity.tenant_id, event_type, request_id, outcome),
            event_type: event_type.to_owned(),
            subject: identity.subject.clone(),
            actor: identity.actor.clone(),
            request_id: request_id.to_owned(),
            outcome,
            policy_version_sha256: Hash32(identity.policy_version_sha256),
            service_build_sha256: self.service_build_sha256,
            redacted_payload_sha256,
            event_time_epoch_ms: epoch_milliseconds()?,
        };
        self.catalog
            .append_audit_event(identity.tenant_id, &event)
            .await?;
        Ok(())
    }
}

fn deterministic_event_id(
    tenant_id: Uuid,
    event_type: &str,
    request_id: &str,
    outcome: AuditOutcome,
) -> Uuid {
    let mut digest = Sha256::new();
    digest.update(b"ngkg-mcp-audit-event-id-v1\0");
    digest.update(tenant_id.as_bytes());
    digest.update(event_type.as_bytes());
    digest.update([0]);
    digest.update(request_id.as_bytes());
    digest.update([0]);
    digest.update(outcome.as_str().as_bytes());
    let hash = digest.finalize();
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&hash[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x80;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}

pub(crate) fn redacted_payload_sha256<T: Serialize>(
    tool_name: &str,
    value: &T,
) -> Result<Hash32, CatalogError> {
    let encoded = serde_json::to_vec(value)?;
    let mut digest = Sha256::new();
    digest.update(PAYLOAD_DOMAIN);
    update_bytes(&mut digest, tool_name.as_bytes())?;
    update_bytes(&mut digest, &Sha256::digest(encoded))?;
    Ok(Hash32(digest.finalize().into()))
}

fn update_bytes(digest: &mut Sha256, bytes: &[u8]) -> Result<(), CatalogError> {
    let length = u64::try_from(bytes.len()).map_err(|_| CatalogError::Overflow)?;
    digest.update(length.to_be_bytes());
    digest.update(bytes);
    Ok(())
}

fn epoch_milliseconds() -> Result<i64, CatalogError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| CatalogError::Invalid("system clock is before the Unix epoch"))?;
    i64::try_from(duration.as_millis()).map_err(|_| CatalogError::Overflow)
}
