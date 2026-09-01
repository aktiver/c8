//! Tenant-isolated, qualification-bound client for user-supplied MCP tools.
//! Remote output is volatile and untrusted; it never becomes NGKG semantic
//! evidence or an answer certificate without a later claim-validation pass.

#![allow(missing_docs)]

use futures_util::StreamExt;
use http::HeaderValue;
use ngkg_agent_catalog::{
    AgentCatalog, ApprovalDecision, ApprovalRecord, CallOutcome, Hash32, ProviderState,
    ToolCallFinish, ToolCallStart, ToolCatalogVersion, ToolProviderVersion,
};
use reqwest::{Client, redirect::Policy as RedirectPolicy};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    net::IpAddr,
    path::PathBuf,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use thiserror::Error;
use tokio::{net::lookup_host, sync::Semaphore};
use url::Url;
use uuid::Uuid;

const SPEC_DOMAIN: &[u8] = b"ngkg-tool-provider-spec-v1\0";
const CATALOG_DOMAIN: &[u8] = b"ngkg-qualified-mcp-catalog-v1\0";
const QUALIFICATION_DOMAIN: &[u8] = b"ngkg-mcp-qualification-evidence-v1\0";

#[derive(Clone, Copy, Debug)]
pub struct BrokerLimits {
    pub maximum_tools: usize,
    pub maximum_schema_depth: usize,
    pub maximum_catalog_bytes: usize,
    pub maximum_request_bytes: usize,
    pub maximum_response_bytes: usize,
    pub maximum_pages: usize,
    pub maximum_in_flight: usize,
    pub connect_timeout: Duration,
    pub request_timeout: Duration,
    pub allow_cluster_private_endpoints: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ProviderPolicy {
    #[serde(default)]
    pub allowed_tools: Vec<String>,
    pub requires_approval: bool,
    pub allow_side_effects: bool,
    pub maximum_request_bytes: usize,
    pub maximum_response_bytes: usize,
    pub request_timeout_milliseconds: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct RegisterProviderRequest {
    pub name: String,
    pub endpoint: String,
    #[serde(default = "no_auth")]
    pub auth_reference: String,
    pub policy: ProviderPolicy,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProviderReceipt {
    pub provider_id: Uuid,
    pub provider_version: i64,
    pub state: &'static str,
    pub spec_sha256: String,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QualificationReceipt {
    pub provider_id: Uuid,
    pub provider_version: i64,
    pub protocol_version: String,
    pub catalog_sha256: String,
    pub qualification_evidence_sha256: String,
    pub tools: Vec<QualifiedTool>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct QualifiedTool {
    pub name: String,
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    pub input_schema: serde_json::Value,
    #[serde(default)]
    pub output_schema: Option<serde_json::Value>,
    #[serde(default)]
    pub annotations: serde_json::Value,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ToolInvocationRequest {
    pub execution_id: Uuid,
    pub provider_id: Uuid,
    pub provider_version: i64,
    pub catalog_sha256: String,
    pub tool_name: String,
    pub arguments: serde_json::Value,
    pub context_certificate_sha256: String,
    #[serde(default)]
    pub approval_id: Option<Uuid>,
    #[serde(default)]
    pub ordinal: i32,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ToolInvocationResult {
    pub tool_call_id: Uuid,
    pub trust_classification: &'static str,
    pub context_certificate_sha256: String,
    pub catalog_sha256: String,
    pub result_sha256: String,
    pub content: serde_json::Value,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct CredentialFileEntry {
    kind: String,
    file: PathBuf,
    file_sha256: String,
}

#[derive(Clone, Default)]
pub struct CredentialRegistry {
    headers: Arc<BTreeMap<String, HeaderValue>>,
}

impl CredentialRegistry {
    pub fn from_checksum_bound_file(path: PathBuf, expected: &str) -> Result<Self, BrokerError> {
        let canonical_manifest = fs::canonicalize(&path)?;
        let credential_root = canonical_manifest
            .parent()
            .ok_or(BrokerError::Credential)?
            .to_path_buf();
        let metadata = fs::metadata(&canonical_manifest)?;
        if !metadata.is_file() || metadata.len() > 1_048_576 {
            return Err(BrokerError::Configuration);
        }
        let bytes = fs::read(&canonical_manifest)?;
        if expected.len() != 64 || hex::encode(Sha256::digest(&bytes)) != expected {
            return Err(BrokerError::Credential);
        }
        let entries: BTreeMap<String, CredentialFileEntry> = serde_json::from_slice(&bytes)?;
        let mut headers = BTreeMap::new();
        for (reference, entry) in entries {
            if reference.is_empty() || entry.kind != "bearer" {
                return Err(BrokerError::Credential);
            }
            let canonical_secret = fs::canonicalize(&entry.file)?;
            if canonical_secret.parent() != Some(credential_root.as_path()) {
                return Err(BrokerError::Credential);
            }
            let meta = fs::metadata(&canonical_secret)?;
            if !meta.is_file() || meta.len() > 16_384 {
                return Err(BrokerError::Credential);
            }
            let secret = fs::read(&canonical_secret)?;
            if hex::encode(Sha256::digest(&secret)) != entry.file_sha256 {
                return Err(BrokerError::Credential);
            }
            let token = std::str::from_utf8(&secret)
                .map_err(|_| BrokerError::Credential)?
                .trim();
            if token.is_empty() || token.len() > 16_384 {
                return Err(BrokerError::Credential);
            }
            headers.insert(
                reference,
                HeaderValue::from_str(&format!("Bearer {token}"))
                    .map_err(|_| BrokerError::Credential)?,
            );
        }
        Ok(Self {
            headers: Arc::new(headers),
        })
    }
    fn authorization(&self, reference: &str) -> Result<Option<HeaderValue>, BrokerError> {
        if reference == "none" {
            Ok(None)
        } else {
            self.headers
                .get(reference)
                .cloned()
                .map(Some)
                .ok_or(BrokerError::Credential)
        }
    }
}

#[derive(Clone)]
pub struct ToolBroker {
    catalog: AgentCatalog,
    credentials: CredentialRegistry,
    limits: BrokerLimits,
    lanes: Arc<Semaphore>,
    protocol_versions: Arc<BTreeSet<String>>,
}

impl ToolBroker {
    pub fn new(
        catalog: AgentCatalog,
        credentials: CredentialRegistry,
        limits: BrokerLimits,
        protocol_versions: BTreeSet<String>,
    ) -> Result<Self, BrokerError> {
        if limits.maximum_tools == 0
            || limits.maximum_schema_depth == 0
            || limits.maximum_catalog_bytes == 0
            || limits.maximum_request_bytes == 0
            || limits.maximum_response_bytes == 0
            || limits.maximum_pages == 0
            || limits.maximum_in_flight == 0
            || protocol_versions.is_empty()
        {
            return Err(BrokerError::Configuration);
        }
        Ok(Self {
            catalog,
            credentials,
            limits,
            lanes: Arc::new(Semaphore::new(limits.maximum_in_flight)),
            protocol_versions: Arc::new(protocol_versions),
        })
    }

    pub async fn register(
        &self,
        tenant_id: Uuid,
        created_by: &str,
        request: RegisterProviderRequest,
    ) -> Result<ProviderReceipt, BrokerError> {
        validate_registration(&request, self.limits)?;
        let provider_id = Uuid::new_v4();
        let version = 1_i64;
        let spec_sha256 = provider_spec_sha256(&request)?;
        let created = epoch_ms()?;
        self.catalog
            .record_tool_provider(&ToolProviderVersion {
                tenant_id,
                provider_id,
                version,
                name: request.name,
                endpoint: request.endpoint,
                auth_reference: request.auth_reference,
                policy: serde_json::to_value(request.policy)?,
                state: ProviderState::Pending,
                spec_sha256,
                created_by: created_by.to_owned(),
                created_at_epoch_ms: created,
            })
            .await?;
        Ok(ProviderReceipt {
            provider_id,
            provider_version: version,
            state: "PENDING",
            spec_sha256: spec_sha256.to_lower_hex(),
        })
    }

    pub async fn qualify(
        &self,
        tenant_id: Uuid,
        provider_id: Uuid,
        pending_version: i64,
        qualified_by: &str,
    ) -> Result<QualificationReceipt, BrokerError> {
        let pending = self
            .catalog
            .load_tool_provider(tenant_id, provider_id, pending_version)
            .await?;
        if pending.state != ProviderState::Pending {
            return Err(BrokerError::State);
        }
        let policy: ProviderPolicy = serde_json::from_value(pending.policy.clone())?;
        let mut session = self.session(&pending, &policy).await?;
        let initialized=session.rpc("initialize",serde_json::json!({"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"ngkg-tool-broker","version":"0.7.0"}}),self.limits.maximum_response_bytes).await?;
        let protocol = initialized
            .pointer("/protocolVersion")
            .and_then(|v| v.as_str())
            .ok_or(BrokerError::Protocol)?
            .to_owned();
        if !self.protocol_versions.contains(&protocol) {
            return Err(BrokerError::Protocol);
        }
        session.protocol_version =
            Some(HeaderValue::from_str(&protocol).map_err(|_| BrokerError::Protocol)?);
        session
            .notification("notifications/initialized", serde_json::json!({}))
            .await?;
        let mut tools = Vec::new();
        let mut cursor = None;
        let mut seen = BTreeSet::new();
        for _ in 0..self.limits.maximum_pages {
            let result = session
                .rpc(
                    "tools/list",
                    cursor.as_ref().map_or_else(
                        || serde_json::json!({}),
                        |value| serde_json::json!({"cursor":value}),
                    ),
                    self.limits.maximum_catalog_bytes,
                )
                .await?;
            let page = result
                .get("tools")
                .and_then(|v| v.as_array())
                .ok_or(BrokerError::Protocol)?;
            for raw in page {
                let tool = parse_tool(raw, self.limits.maximum_schema_depth)?;
                if !seen.insert(tool.name.clone()) {
                    return Err(BrokerError::Protocol);
                }
                tools.push(tool);
                if tools.len() > self.limits.maximum_tools {
                    return Err(BrokerError::Limit);
                }
            }
            cursor = result
                .get("nextCursor")
                .and_then(|v| v.as_str())
                .map(ToOwned::to_owned);
            if cursor.is_none() {
                break;
            }
        }
        if cursor.is_some() || tools.is_empty() {
            return Err(BrokerError::Limit);
        }
        tools.sort_by(|a, b| a.name.cmp(&b.name));
        let catalog_bytes = serde_json::to_vec(&tools)?;
        if catalog_bytes.len() > self.limits.maximum_catalog_bytes {
            return Err(BrokerError::Limit);
        }
        let catalog_sha256 = domain_hash(CATALOG_DOMAIN, &catalog_bytes);
        let qualification_evidence_sha256 = qualification_hash(&pending, &protocol, catalog_sha256);
        let qualified_version = pending_version.checked_add(1).ok_or(BrokerError::Limit)?;
        let qualified_at = epoch_ms()?;
        let qualified_provider = ToolProviderVersion {
            tenant_id,
            provider_id,
            version: qualified_version,
            name: pending.name,
            endpoint: pending.endpoint,
            auth_reference: pending.auth_reference,
            policy: pending.policy,
            state: ProviderState::Qualified,
            spec_sha256: pending.spec_sha256,
            created_by: qualified_by.to_owned(),
            created_at_epoch_ms: qualified_at,
        };
        let qualified_catalog = ToolCatalogVersion {
            tenant_id,
            provider_id,
            provider_version: qualified_version,
            catalog_sha256,
            protocol_version: protocol.clone(),
            discovered_tools: serde_json::to_value(&tools)?,
            qualification_evidence_sha256,
            created_at_epoch_ms: qualified_at,
        };
        self.catalog
            .record_qualified_tool_provider_and_catalog(&qualified_provider, &qualified_catalog)
            .await?;
        Ok(QualificationReceipt {
            provider_id,
            provider_version: qualified_version,
            protocol_version: protocol,
            catalog_sha256: catalog_sha256.to_lower_hex(),
            qualification_evidence_sha256: qualification_evidence_sha256.to_lower_hex(),
            tools,
        })
    }

    pub async fn invoke(
        &self,
        tenant_id: Uuid,
        request: ToolInvocationRequest,
    ) -> Result<ToolInvocationResult, BrokerError> {
        if request.execution_id.is_nil()
            || request.provider_id.is_nil()
            || request.provider_version < 1
            || request.ordinal < 0
            || request.tool_name.is_empty()
            || request.tool_name.len() > 512
            || !request.arguments.is_object()
        {
            return Err(BrokerError::Invalid);
        }
        let catalog_hash = Hash32::from_lower_hex(&request.catalog_sha256)?;
        let context_hash = Hash32::from_lower_hex(&request.context_certificate_sha256)?;
        let execution = self
            .catalog
            .load_tool_execution_context(tenant_id, request.execution_id)
            .await?;
        if execution.result_sha256 != Some(context_hash) || execution.state != "COMPLETED" {
            return Err(BrokerError::Context);
        }
        let profile = self
            .catalog
            .load_agent_profile(tenant_id, execution.profile_id, execution.profile_version)
            .await?;
        if !json_hash_allowlist_contains(&profile.tool_catalog_sha256s, &request.catalog_sha256) {
            return Err(BrokerError::NotAllowed);
        }
        let catalog = self
            .catalog
            .load_tool_catalog(tenant_id, request.provider_id, catalog_hash)
            .await?;
        if catalog.provider_version != request.provider_version {
            return Err(BrokerError::Evidence);
        }
        let provider = self
            .catalog
            .load_tool_provider(tenant_id, request.provider_id, request.provider_version)
            .await?;
        if provider.state != ProviderState::Qualified {
            return Err(BrokerError::State);
        }
        let policy: ProviderPolicy = serde_json::from_value(provider.policy.clone())?;
        let tools: Vec<QualifiedTool> = serde_json::from_value(catalog.discovered_tools)?;
        let tool = tools
            .iter()
            .find(|tool| tool.name == request.tool_name)
            .ok_or(BrokerError::NotAllowed)?;
        if !policy.allowed_tools.is_empty()
            && !policy
                .allowed_tools
                .iter()
                .any(|name| name == &request.tool_name)
        {
            return Err(BrokerError::NotAllowed);
        }
        let read_only = tool
            .annotations
            .get("readOnlyHint")
            .and_then(serde_json::Value::as_bool)
            == Some(true);
        if !read_only && !policy.allow_side_effects {
            return Err(BrokerError::NotAllowed);
        }
        validate_value(
            &tool.input_schema,
            &request.arguments,
            0,
            self.limits.maximum_schema_depth,
        )?;
        let policy_hash = domain_hash(SPEC_DOMAIN, &serde_json::to_vec(&profile.approval_policy)?);
        if !read_only
            || policy.requires_approval
            || profile_requires_approval(&profile.approval_policy, &request.tool_name)
        {
            let approval_id = request.approval_id.ok_or(BrokerError::ApprovalRequired)?;
            let approval = self.catalog.load_approval(tenant_id, approval_id).await?;
            if approval.execution_id != request.execution_id
                || approval.tool_name != request.tool_name
                || approval.policy_sha256 != policy_hash
                || approval.catalog_sha256 != Some(catalog_hash)
                || approval.decision != ApprovalDecision::Approved
                || approval.expires_at_epoch_ms < epoch_ms()?
            {
                return Err(BrokerError::ApprovalDenied);
            }
        }
        let arguments_sha256 = domain_hash(SPEC_DOMAIN, &serde_json::to_vec(&request.arguments)?);
        let tool_call_id = Uuid::new_v4();
        self.catalog
            .begin_tool_call(&ToolCallStart {
                tenant_id,
                tool_call_id,
                execution_id: request.execution_id,
                ordinal: request.ordinal,
                provider_id: Some(request.provider_id),
                tool_name: request.tool_name.clone(),
                catalog_sha256: Some(catalog_hash),
                arguments_sha256,
                approval_id: request.approval_id,
                started_at_epoch_ms: epoch_ms()?,
            })
            .await?;
        let outcome = self
            .invoke_remote(&provider, &policy, &request.tool_name, request.arguments)
            .await;
        match outcome {
            Ok(content) => {
                let bytes = serde_json::to_vec(&content)?;
                let result_sha256 = domain_hash(CATALOG_DOMAIN, &bytes);
                self.catalog
                    .finalize_tool_call(&ToolCallFinish {
                        tenant_id,
                        tool_call_id,
                        result_sha256: Some(result_sha256),
                        query_execution_id: None,
                        ended_at_epoch_ms: epoch_ms()?,
                        outcome: CallOutcome::Completed,
                        error_code: None,
                    })
                    .await?;
                Ok(ToolInvocationResult {
                    tool_call_id,
                    trust_classification: "UNTRUSTED_EXTERNAL_TOOL",
                    context_certificate_sha256: request.context_certificate_sha256,
                    catalog_sha256: request.catalog_sha256,
                    result_sha256: result_sha256.to_lower_hex(),
                    content,
                })
            }
            Err(error) => {
                self.catalog
                    .finalize_tool_call(&ToolCallFinish {
                        tenant_id,
                        tool_call_id,
                        result_sha256: None,
                        query_execution_id: None,
                        ended_at_epoch_ms: epoch_ms()?,
                        outcome: CallOutcome::Failed,
                        error_code: Some(error.code().to_owned()),
                    })
                    .await?;
                Err(error)
            }
        }
    }

    pub async fn approve(
        &self,
        tenant_id: Uuid,
        approver: &str,
        execution_id: Uuid,
        tool_name: String,
        catalog_sha256: String,
        approved: bool,
        expires_at_epoch_ms: i64,
    ) -> Result<Uuid, BrokerError> {
        let context = self
            .catalog
            .load_tool_execution_context(tenant_id, execution_id)
            .await?;
        let profile = self
            .catalog
            .load_agent_profile(tenant_id, context.profile_id, context.profile_version)
            .await?;
        let catalog = Hash32::from_lower_hex(&catalog_sha256)?;
        if !json_hash_allowlist_contains(&profile.tool_catalog_sha256s, &catalog_sha256) {
            return Err(BrokerError::NotAllowed);
        }
        let approval_id = Uuid::new_v4();
        self.catalog
            .record_approval(&ApprovalRecord {
                tenant_id,
                approval_id,
                execution_id,
                tool_name,
                approver: approver.to_owned(),
                policy_sha256: domain_hash(
                    SPEC_DOMAIN,
                    &serde_json::to_vec(&profile.approval_policy)?,
                ),
                catalog_sha256: Some(catalog),
                decision: if approved {
                    ApprovalDecision::Approved
                } else {
                    ApprovalDecision::Denied
                },
                expires_at_epoch_ms,
                created_at_epoch_ms: epoch_ms()?,
            })
            .await?;
        Ok(approval_id)
    }

    async fn invoke_remote(
        &self,
        provider: &ToolProviderVersion,
        policy: &ProviderPolicy,
        tool_name: &str,
        arguments: serde_json::Value,
    ) -> Result<serde_json::Value, BrokerError> {
        let _permit = tokio::time::timeout(
            Duration::from_secs(1),
            Arc::clone(&self.lanes).acquire_owned(),
        )
        .await
        .map_err(|_| BrokerError::Admission)?
        .map_err(|_| BrokerError::Admission)?;
        let mut session = self.session(provider, policy).await?;
        let initialized=session.rpc("initialize",serde_json::json!({"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"ngkg-tool-broker","version":"0.7.0"}}),policy.maximum_response_bytes.min(self.limits.maximum_response_bytes)).await?;
        let protocol = initialized
            .pointer("/protocolVersion")
            .and_then(|v| v.as_str())
            .ok_or(BrokerError::Protocol)?;
        if !self.protocol_versions.contains(protocol) {
            return Err(BrokerError::Protocol);
        }
        session.protocol_version =
            Some(HeaderValue::from_str(protocol).map_err(|_| BrokerError::Protocol)?);
        session
            .notification("notifications/initialized", serde_json::json!({}))
            .await?;
        let result = session
            .rpc(
                "tools/call",
                serde_json::json!({"name":tool_name,"arguments":arguments}),
                policy
                    .maximum_response_bytes
                    .min(self.limits.maximum_response_bytes),
            )
            .await?;
        if result.get("isError").and_then(serde_json::Value::as_bool) == Some(true) {
            return Err(BrokerError::Remote);
        }
        Ok(result)
    }

    async fn session(
        &self,
        provider: &ToolProviderVersion,
        policy: &ProviderPolicy,
    ) -> Result<McpSession, BrokerError> {
        let endpoint = Url::parse(&provider.endpoint)?;
        let host = endpoint.host_str().ok_or(BrokerError::Endpoint)?.to_owned();
        let port = endpoint
            .port_or_known_default()
            .ok_or(BrokerError::Endpoint)?;
        let addresses = lookup_host((host.as_str(), port))
            .await?
            .collect::<Vec<_>>();
        if addresses.is_empty()
            || addresses.iter().any(|address| {
                !safe_address(
                    address.ip(),
                    self.limits.allow_cluster_private_endpoints,
                    &host,
                )
            })
        {
            return Err(BrokerError::Endpoint);
        }
        let pinned = addresses[0];
        let authorization = self.credentials.authorization(&provider.auth_reference)?;
        let timeout = Duration::from_millis(policy.request_timeout_milliseconds)
            .min(self.limits.request_timeout);
        let client = Client::builder()
            .connect_timeout(self.limits.connect_timeout)
            .timeout(timeout)
            .redirect(RedirectPolicy::none())
            .https_only(true)
            .resolve(&host, pinned)
            .build()?;
        Ok(McpSession {
            client,
            endpoint,
            authorization,
            session_id: None,
            protocol_version: None,
            next_id: 1,
            maximum_request_bytes: policy
                .maximum_request_bytes
                .min(self.limits.maximum_request_bytes),
        })
    }
}

struct McpSession {
    client: Client,
    endpoint: Url,
    authorization: Option<HeaderValue>,
    session_id: Option<HeaderValue>,
    protocol_version: Option<HeaderValue>,
    next_id: u64,
    maximum_request_bytes: usize,
}
impl McpSession {
    async fn rpc(
        &mut self,
        method: &str,
        params: serde_json::Value,
        maximum_response: usize,
    ) -> Result<serde_json::Value, BrokerError> {
        let id = self.next_id;
        self.next_id = self.next_id.checked_add(1).ok_or(BrokerError::Limit)?;
        let body = serde_json::to_vec(
            &serde_json::json!({"jsonrpc":"2.0","id":id,"method":method,"params":params}),
        )?;
        let value = self.send(body, maximum_response).await?;
        if value.get("id").and_then(serde_json::Value::as_u64) != Some(id)
            || value.get("error").is_some()
        {
            return Err(BrokerError::Remote);
        }
        value.get("result").cloned().ok_or(BrokerError::Protocol)
    }
    async fn notification(
        &mut self,
        method: &str,
        params: serde_json::Value,
    ) -> Result<(), BrokerError> {
        let body = serde_json::to_vec(
            &serde_json::json!({"jsonrpc":"2.0","method":method,"params":params}),
        )?;
        let _ = self.send_optional(body, 65_536).await?;
        Ok(())
    }
    async fn send(
        &mut self,
        body: Vec<u8>,
        maximum: usize,
    ) -> Result<serde_json::Value, BrokerError> {
        self.send_optional(body, maximum)
            .await?
            .ok_or(BrokerError::Protocol)
    }
    async fn send_optional(
        &mut self,
        body: Vec<u8>,
        maximum: usize,
    ) -> Result<Option<serde_json::Value>, BrokerError> {
        if body.len() > self.maximum_request_bytes {
            return Err(BrokerError::Limit);
        }
        let mut request = self
            .client
            .post(self.endpoint.clone())
            .header("accept", "application/json, text/event-stream")
            .header("content-type", "application/json")
            .body(body);
        if let Some(value) = &self.authorization {
            request = request.header("authorization", value.clone());
        }
        if let Some(value) = &self.session_id {
            request = request.header("mcp-session-id", value.clone());
        }
        if let Some(value) = &self.protocol_version {
            request = request.header("mcp-protocol-version", value.clone());
        }
        let response = request.send().await?;
        if let Some(value) = response.headers().get("mcp-session-id") {
            self.session_id = Some(value.clone());
        }
        if response.status() == reqwest::StatusCode::ACCEPTED
            || response.status() == reqwest::StatusCode::NO_CONTENT
        {
            return Ok(None);
        }
        if !response.status().is_success() {
            return Err(BrokerError::Remote);
        }
        let content_type = response
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_owned();
        let bytes = bounded_body(response, maximum).await?;
        if content_type.starts_with("application/json") {
            return Ok(Some(serde_json::from_slice(&bytes)?));
        }
        if content_type.starts_with("text/event-stream") {
            return parse_sse(&bytes).map(Some);
        }
        Err(BrokerError::Protocol)
    }
}

async fn bounded_body(
    response: reqwest::Response,
    maximum: usize,
) -> Result<bytes::Bytes, BrokerError> {
    let mut stream = response.bytes_stream();
    let mut body = bytes::BytesMut::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        if body
            .len()
            .checked_add(chunk.len())
            .is_none_or(|length| length > maximum)
        {
            return Err(BrokerError::Limit);
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body.freeze())
}
fn parse_sse(bytes: &[u8]) -> Result<serde_json::Value, BrokerError> {
    let text = std::str::from_utf8(bytes)
        .map_err(|_| BrokerError::Protocol)?
        .replace("\r\n", "\n");
    for event in text.split("\n\n") {
        let data = event
            .lines()
            .filter_map(|line| line.strip_prefix("data:"))
            .map(str::trim)
            .collect::<Vec<_>>()
            .join("\n");
        if !data.is_empty() {
            let value: serde_json::Value = serde_json::from_str(&data)?;
            if value.get("jsonrpc").is_some() {
                return Ok(value);
            }
        }
    }
    Err(BrokerError::Protocol)
}

fn validate_registration(
    request: &RegisterProviderRequest,
    limits: BrokerLimits,
) -> Result<(), BrokerError> {
    let endpoint = Url::parse(&request.endpoint)?;
    if request.name.is_empty()
        || request.name.len() > 256
        || request.auth_reference.is_empty()
        || request.auth_reference.len() > 1024
        || endpoint.scheme() != "https"
        || endpoint.username() != ""
        || endpoint.password().is_some()
        || endpoint.query().is_some()
        || endpoint.fragment().is_some()
        || request.policy.maximum_request_bytes == 0
        || request.policy.maximum_request_bytes > limits.maximum_request_bytes
        || request.policy.maximum_response_bytes == 0
        || request.policy.maximum_response_bytes > limits.maximum_response_bytes
        || request.policy.request_timeout_milliseconds == 0
        || request.policy.request_timeout_milliseconds
            > u64::try_from(limits.request_timeout.as_millis()).unwrap_or(u64::MAX)
        || request.policy.allowed_tools.len() > limits.maximum_tools
    {
        return Err(BrokerError::Invalid);
    }
    for name in &request.policy.allowed_tools {
        if !valid_tool_name(name) {
            return Err(BrokerError::Invalid);
        }
    }
    Ok(())
}
fn parse_tool(
    value: &serde_json::Value,
    maximum_depth: usize,
) -> Result<QualifiedTool, BrokerError> {
    let object = value.as_object().ok_or(BrokerError::Protocol)?;
    let name = object
        .get("name")
        .and_then(|v| v.as_str())
        .filter(|v| valid_tool_name(v))
        .ok_or(BrokerError::Protocol)?
        .to_owned();
    let title = bounded_optional(object.get("title"), 256)?;
    let description = bounded_optional(object.get("description"), 4096)?;
    let input_schema = object
        .get("inputSchema")
        .cloned()
        .ok_or(BrokerError::Protocol)?;
    validate_schema(&input_schema, 0, maximum_depth)?;
    let output_schema = object.get("outputSchema").cloned();
    if let Some(schema) = &output_schema {
        validate_schema(schema, 0, maximum_depth)?;
    }
    let annotations = object
        .get("annotations")
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}));
    if !annotations.is_object() || serde_json::to_vec(&annotations)?.len() > 65_536 {
        return Err(BrokerError::Protocol);
    }
    Ok(QualifiedTool {
        name,
        title,
        description,
        input_schema,
        output_schema,
        annotations,
    })
}
fn bounded_optional(
    value: Option<&serde_json::Value>,
    maximum: usize,
) -> Result<Option<String>, BrokerError> {
    match value {
        None => Ok(None),
        Some(value) => value
            .as_str()
            .filter(|text| text.len() <= maximum)
            .map(|text| Some(text.to_owned()))
            .ok_or(BrokerError::Protocol),
    }
}
fn valid_tool_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.' | b'/'))
}
fn validate_schema(
    schema: &serde_json::Value,
    depth: usize,
    maximum: usize,
) -> Result<(), BrokerError> {
    if depth > maximum {
        return Err(BrokerError::Limit);
    }
    let object = schema.as_object().ok_or(BrokerError::Schema)?;
    if object.keys().any(|key| {
        matches!(
            key.as_str(),
            "$ref" | "$dynamicRef" | "allOf" | "anyOf" | "oneOf" | "not" | "if" | "then" | "else"
        )
    }) {
        return Err(BrokerError::Schema);
    }
    let kind = object
        .get("type")
        .and_then(|v| v.as_str())
        .ok_or(BrokerError::Schema)?;
    if !matches!(
        kind,
        "object" | "array" | "string" | "number" | "integer" | "boolean" | "null"
    ) {
        return Err(BrokerError::Schema);
    }
    if let Some(properties) = object.get("properties") {
        for child in properties.as_object().ok_or(BrokerError::Schema)?.values() {
            validate_schema(child, depth + 1, maximum)?;
        }
    }
    if let Some(items) = object.get("items") {
        validate_schema(items, depth + 1, maximum)?;
    }
    Ok(())
}
fn validate_value(
    schema: &serde_json::Value,
    value: &serde_json::Value,
    depth: usize,
    maximum: usize,
) -> Result<(), BrokerError> {
    if depth > maximum {
        return Err(BrokerError::Limit);
    }
    let object = schema.as_object().ok_or(BrokerError::Schema)?;
    match object
        .get("type")
        .and_then(|v| v.as_str())
        .ok_or(BrokerError::Schema)?
    {
        "object" => {
            let values = value.as_object().ok_or(BrokerError::Arguments)?;
            let properties = object.get("properties").and_then(|v| v.as_object());
            if let Some(required) = object.get("required").and_then(|v| v.as_array()) {
                for name in required.iter().filter_map(|v| v.as_str()) {
                    if !values.contains_key(name) {
                        return Err(BrokerError::Arguments);
                    }
                }
            }
            for (name, child) in values {
                match properties.and_then(|map| map.get(name)) {
                    Some(schema) => validate_value(schema, child, depth + 1, maximum)?,
                    None if object
                        .get("additionalProperties")
                        .and_then(serde_json::Value::as_bool)
                        == Some(false) =>
                    {
                        return Err(BrokerError::Arguments);
                    }
                    None => {}
                }
            }
        }
        "array" => {
            let values = value.as_array().ok_or(BrokerError::Arguments)?;
            if let Some(items) = object.get("items") {
                for child in values {
                    validate_value(items, child, depth + 1, maximum)?;
                }
            }
        }
        "string" if !value.is_string() => return Err(BrokerError::Arguments),
        "number" if !value.is_number() => return Err(BrokerError::Arguments),
        "integer" if value.as_i64().is_none() && value.as_u64().is_none() => {
            return Err(BrokerError::Arguments);
        }
        "boolean" if !value.is_boolean() => return Err(BrokerError::Arguments),
        "null" if !value.is_null() => return Err(BrokerError::Arguments),
        _ => {}
    }
    if let Some(choices) = object.get("enum").and_then(|v| v.as_array())
        && !choices.contains(value)
    {
        return Err(BrokerError::Arguments);
    }
    Ok(())
}
fn safe_address(address: IpAddr, allow_cluster: bool, host: &str) -> bool {
    let cluster_name = host.ends_with(".svc") || host.contains(".svc.") || !host.contains('.');
    match address {
        IpAddr::V4(ip) => {
            let octets = ip.octets();
            let rfc1918 = ip.is_private();
            let special = octets[0] == 0
                || octets[0] == 127
                || (octets[0] == 100 && (64..=127).contains(&octets[1]))
                || (octets[0] == 169 && octets[1] == 254)
                || (octets[0] == 192 && octets[1] == 0 && octets[2] == 0)
                || (octets[0] == 198 && (18..=19).contains(&octets[1]))
                || ip.is_documentation()
                || octets[0] >= 224;
            if special {
                return false;
            }
            !rfc1918 || allow_cluster && cluster_name
        }
        IpAddr::V6(ip) => {
            if let Some(mapped) = ip.to_ipv4_mapped() {
                return safe_address(IpAddr::V4(mapped), allow_cluster, host);
            }
            let segments = ip.segments();
            let private = ip.is_unique_local();
            let special = ip.is_unspecified()
                || ip.is_loopback()
                || ip.is_multicast()
                || ip.is_unicast_link_local()
                || (segments[0] == 0x2001 && segments[1] == 0x0db8);
            if special {
                return false;
            }
            !private || allow_cluster && cluster_name
        }
    }
}
fn profile_requires_approval(policy: &serde_json::Value, tool: &str) -> bool {
    policy
        .get("requiredTools")
        .and_then(|v| v.as_array())
        .is_some_and(|tools| {
            tools
                .iter()
                .filter_map(|v| v.as_str())
                .any(|name| name == tool)
        })
}
fn json_hash_allowlist_contains(value: &serde_json::Value, hash: &str) -> bool {
    value.as_array().is_some_and(|entries| {
        entries
            .iter()
            .filter_map(|v| v.as_str())
            .any(|entry| entry == hash)
    })
}
fn provider_spec_sha256(request: &RegisterProviderRequest) -> Result<Hash32, BrokerError> {
    Ok(domain_hash(SPEC_DOMAIN, &serde_json::to_vec(request)?))
}
fn qualification_hash(provider: &ToolProviderVersion, protocol: &str, catalog: Hash32) -> Hash32 {
    let mut digest = Sha256::new();
    digest.update(QUALIFICATION_DOMAIN);
    digest.update(provider.provider_id.as_bytes());
    digest.update(provider.version.to_be_bytes());
    digest.update(provider.spec_sha256.0);
    digest.update(protocol.as_bytes());
    digest.update(catalog.0);
    Hash32(digest.finalize().into())
}
fn domain_hash(domain: &[u8], bytes: &[u8]) -> Hash32 {
    let mut digest = Sha256::new();
    digest.update(domain);
    digest.update(bytes);
    Hash32(digest.finalize().into())
}
fn epoch_ms() -> Result<i64, BrokerError> {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| BrokerError::Clock)?
            .as_millis(),
    )
    .map_err(|_| BrokerError::Clock)
}
fn no_auth() -> String {
    "none".to_owned()
}

#[derive(Debug, Error)]
pub enum BrokerError {
    #[error("broker configuration is invalid")]
    Configuration,
    #[error("provider request is invalid")]
    Invalid,
    #[error("provider endpoint is unsafe")]
    Endpoint,
    #[error("credential is invalid")]
    Credential,
    #[error("provider state is invalid")]
    State,
    #[error("protocol response is invalid")]
    Protocol,
    #[error("remote tool failed")]
    Remote,
    #[error("schema is unsupported")]
    Schema,
    #[error("arguments do not satisfy the qualified schema")]
    Arguments,
    #[error("operation is not allowed")]
    NotAllowed,
    #[error("approval is required")]
    ApprovalRequired,
    #[error("approval was denied or expired")]
    ApprovalDenied,
    #[error("context certificate mismatch")]
    Context,
    #[error("qualification evidence mismatch")]
    Evidence,
    #[error("broker limit exceeded")]
    Limit,
    #[error("broker admission timed out")]
    Admission,
    #[error("clock failed")]
    Clock,
    #[error("catalog failed")]
    Catalog(#[from] ngkg_agent_catalog::CatalogError),
    #[error("URL failed")]
    Url(#[from] url::ParseError),
    #[error("HTTP failed")]
    Http(#[from] reqwest::Error),
    #[error("I/O or DNS failed")]
    Io(#[from] std::io::Error),
    #[error("JSON failed")]
    Json(#[from] serde_json::Error),
}
impl BrokerError {
    pub const fn code(&self) -> &'static str {
        match self {
            Self::Configuration => "TOOL_BROKER_CONFIGURATION",
            Self::Invalid => "TOOL_REQUEST_INVALID",
            Self::Endpoint => "TOOL_ENDPOINT_UNSAFE",
            Self::Credential => "TOOL_CREDENTIAL_INVALID",
            Self::State => "TOOL_PROVIDER_STATE",
            Self::Protocol => "MCP_PROTOCOL_INVALID",
            Self::Remote => "MCP_REMOTE_FAILED",
            Self::Schema => "TOOL_SCHEMA_UNSUPPORTED",
            Self::Arguments => "TOOL_ARGUMENTS_INVALID",
            Self::NotAllowed => "TOOL_POLICY_DENIED",
            Self::ApprovalRequired => "TOOL_APPROVAL_REQUIRED",
            Self::ApprovalDenied => "TOOL_APPROVAL_DENIED",
            Self::Context => "TOOL_CONTEXT_MISMATCH",
            Self::Evidence => "TOOL_EVIDENCE_MISMATCH",
            Self::Limit => "TOOL_LIMIT",
            Self::Admission => "TOOL_ADMISSION",
            Self::Clock => "CLOCK_FAILED",
            Self::Catalog(_) => "CATALOG_FAILED",
            Self::Url(_) => "TOOL_URL_INVALID",
            Self::Http(_) => "TOOL_TRANSPORT_FAILED",
            Self::Io(_) => "TOOL_CREDENTIAL_IO_OR_DNS",
            Self::Json(_) => "TOOL_JSON_INVALID",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, Ipv6Addr};

    fn limits() -> BrokerLimits {
        BrokerLimits {
            maximum_tools: 10,
            maximum_schema_depth: 8,
            maximum_catalog_bytes: 65_536,
            maximum_request_bytes: 65_536,
            maximum_response_bytes: 65_536,
            maximum_pages: 4,
            maximum_in_flight: 2,
            connect_timeout: Duration::from_secs(1),
            request_timeout: Duration::from_secs(2),
            allow_cluster_private_endpoints: false,
        }
    }

    fn registration(endpoint: &str) -> RegisterProviderRequest {
        RegisterProviderRequest {
            name: "test-provider".to_owned(),
            endpoint: endpoint.to_owned(),
            auth_reference: "none".to_owned(),
            policy: ProviderPolicy {
                allowed_tools: vec!["read.graph".to_owned()],
                requires_approval: false,
                allow_side_effects: false,
                maximum_request_bytes: 4096,
                maximum_response_bytes: 4096,
                request_timeout_milliseconds: 1000,
            },
        }
    }

    #[test]
    fn registration_requires_https_without_url_credentials() {
        assert!(
            validate_registration(&registration("https://tools.example/mcp"), limits()).is_ok()
        );
        assert!(
            validate_registration(&registration("http://tools.example/mcp"), limits()).is_err()
        );
        assert!(
            validate_registration(
                &registration("https://user:pass@tools.example/mcp"),
                limits()
            )
            .is_err()
        );
    }

    #[test]
    fn schema_subset_rejects_remote_or_combinatorial_references() {
        let safe = serde_json::json!({"type":"object","properties":{"id":{"type":"string"}},"required":["id"],"additionalProperties":false});
        let remote = serde_json::json!({"type":"object","properties":{"id":{"$ref":"https://example/schema"}}});
        assert!(validate_schema(&safe, 0, 8).is_ok());
        assert!(validate_schema(&remote, 0, 8).is_err());
        assert!(validate_value(&safe, &serde_json::json!({"id":"x"}), 0, 8).is_ok());
        assert!(validate_value(&safe, &serde_json::json!({}), 0, 8).is_err());
        assert!(validate_value(&safe, &serde_json::json!({"id":"x","extra":true}), 0, 8).is_err());
    }

    #[test]
    fn network_policy_rejects_metadata_and_non_cluster_private_addresses() {
        assert!(!safe_address(
            IpAddr::V4(Ipv4Addr::new(169, 254, 169, 254)),
            false,
            "metadata"
        ));
        assert!(!safe_address(
            IpAddr::V4(Ipv4Addr::new(10, 0, 0, 4)),
            false,
            "tools.ns.svc"
        ));
        assert!(safe_address(
            IpAddr::V4(Ipv4Addr::new(10, 0, 0, 4)),
            true,
            "tools.ns.svc.cluster.local"
        ));
        assert!(!safe_address(
            IpAddr::V6(Ipv6Addr::LOCALHOST),
            true,
            "tools.ns.svc"
        ));
        assert!(safe_address(
            IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8)),
            false,
            "tools.example"
        ));
    }

    #[test]
    fn sse_parser_accepts_crlf_and_multiline_json_data() {
        let payload = b"event: message\r\ndata: {\"jsonrpc\":\"2.0\",\r\ndata: \"id\":1,\"result\":{}}\r\n\r\n";
        let parsed = parse_sse(payload);
        assert!(parsed.is_ok());
        assert_eq!(
            parsed
                .ok()
                .and_then(|value| value.get("id").and_then(serde_json::Value::as_u64)),
            Some(1)
        );
    }

    #[test]
    fn provider_hash_is_domain_separated_and_deterministic() {
        let request = registration("https://tools.example/mcp");
        let first = provider_spec_sha256(&request);
        let second = provider_spec_sha256(&request);
        assert!(first.is_ok());
        assert_eq!(first.ok(), second.ok());
        assert_ne!(
            domain_hash(SPEC_DOMAIN, b"x"),
            domain_hash(CATALOG_DOMAIN, b"x")
        );
    }
}
