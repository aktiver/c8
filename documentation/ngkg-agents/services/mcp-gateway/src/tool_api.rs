//! OAuth-scoped REST control and execution surface for tenant MCP tools.

use crate::{SharedState, auth::GatewayIdentity};
use axum::{
    Json, Router,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::post,
};
use ngkg_agent_catalog::{AuditOutcome, Hash32};
use ngkg_tool_broker::{BrokerError, RegisterProviderRequest, ToolInvocationRequest};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

pub(crate) fn router() -> Router<SharedState> {
    Router::new()
        .route("/v1/tool-providers", post(register))
        .route(
            "/v1/tool-providers/{provider_id}/versions/{version}/qualify",
            post(qualify),
        )
        .route("/v1/tool-approvals", post(approve))
        .route("/v1/tool-calls", post(invoke))
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct ApprovalRequest {
    execution_id: Uuid,
    tool_name: String,
    catalog_sha256: String,
    approved: bool,
    expires_at_epoch_ms: i64,
}
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ApprovalReceipt {
    approval_id: Uuid,
}
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ErrorBody {
    code: &'static str,
    message: &'static str,
}

async fn register(
    State(state): State<SharedState>,
    headers: HeaderMap,
    axum::Extension(identity): axum::Extension<GatewayIdentity>,
    Json(request): Json<RegisterProviderRequest>,
) -> Response {
    if let Err(response) = authorize(&identity, "tools:providers:write") {
        return response;
    }
    let Some(broker) = &state.tool_broker else {
        return disabled();
    };
    let payload = payload_hash(&request);
    let request_id = request_id(&headers);
    if audit(
        &state,
        &identity,
        &request_id,
        AuditOutcome::Started,
        payload,
    )
    .await
    .is_err()
    {
        return audit_failed();
    }
    match broker
        .register(identity.tenant_id, &identity.subject, request)
        .await
    {
        Ok(result) => {
            complete(
                &state,
                &identity,
                &request_id,
                payload,
                StatusCode::CREATED,
                result,
            )
            .await
        }
        Err(error) => failed(&state, &identity, &request_id, payload, error).await,
    }
}
async fn qualify(
    State(state): State<SharedState>,
    Path((provider_id, version)): Path<(Uuid, i64)>,
    headers: HeaderMap,
    axum::Extension(identity): axum::Extension<GatewayIdentity>,
) -> Response {
    if let Err(response) = authorize(&identity, "tools:providers:write") {
        return response;
    }
    let Some(broker) = &state.tool_broker else {
        return disabled();
    };
    let payload = Hash32(Sha256::digest(format!("{provider_id}:{version}").as_bytes()).into());
    let request_id = request_id(&headers);
    if audit(
        &state,
        &identity,
        &request_id,
        AuditOutcome::Started,
        payload,
    )
    .await
    .is_err()
    {
        return audit_failed();
    }
    match broker
        .qualify(identity.tenant_id, provider_id, version, &identity.subject)
        .await
    {
        Ok(result) => {
            complete(
                &state,
                &identity,
                &request_id,
                payload,
                StatusCode::OK,
                result,
            )
            .await
        }
        Err(error) => failed(&state, &identity, &request_id, payload, error).await,
    }
}
async fn approve(
    State(state): State<SharedState>,
    headers: HeaderMap,
    axum::Extension(identity): axum::Extension<GatewayIdentity>,
    Json(request): Json<ApprovalRequest>,
) -> Response {
    if let Err(response) = authorize(&identity, "tools:approve") {
        return response;
    }
    let Some(broker) = &state.tool_broker else {
        return disabled();
    };
    let payload = payload_hash(&request);
    let request_id = request_id(&headers);
    if audit(
        &state,
        &identity,
        &request_id,
        AuditOutcome::Started,
        payload,
    )
    .await
    .is_err()
    {
        return audit_failed();
    }
    match broker
        .approve(
            identity.tenant_id,
            &identity.subject,
            request.execution_id,
            request.tool_name,
            request.catalog_sha256,
            request.approved,
            request.expires_at_epoch_ms,
        )
        .await
    {
        Ok(approval_id) => {
            complete(
                &state,
                &identity,
                &request_id,
                payload,
                StatusCode::CREATED,
                ApprovalReceipt { approval_id },
            )
            .await
        }
        Err(error) => failed(&state, &identity, &request_id, payload, error).await,
    }
}
async fn invoke(
    State(state): State<SharedState>,
    headers: HeaderMap,
    axum::Extension(identity): axum::Extension<GatewayIdentity>,
    Json(request): Json<ToolInvocationRequest>,
) -> Response {
    if let Err(response) = authorize(&identity, "tools:execute") {
        return response;
    }
    let Some(broker) = &state.tool_broker else {
        return disabled();
    };
    let payload = payload_hash(&request);
    let request_id = request_id(&headers);
    if audit(
        &state,
        &identity,
        &request_id,
        AuditOutcome::Started,
        payload,
    )
    .await
    .is_err()
    {
        return audit_failed();
    }
    match broker.invoke(identity.tenant_id, request).await {
        Ok(result) => {
            complete(
                &state,
                &identity,
                &request_id,
                payload,
                StatusCode::OK,
                result,
            )
            .await
        }
        Err(error) => failed(&state, &identity, &request_id, payload, error).await,
    }
}

async fn complete<T: Serialize>(
    state: &SharedState,
    identity: &GatewayIdentity,
    request_id: &str,
    payload: Hash32,
    status: StatusCode,
    result: T,
) -> Response {
    if audit(
        state,
        identity,
        request_id,
        AuditOutcome::Completed,
        payload,
    )
    .await
    .is_err()
    {
        audit_failed()
    } else {
        (status, Json(result)).into_response()
    }
}
async fn failed(
    state: &SharedState,
    identity: &GatewayIdentity,
    request_id: &str,
    payload: Hash32,
    error_value: BrokerError,
) -> Response {
    let outcome = if matches!(
        error_value,
        BrokerError::NotAllowed
            | BrokerError::ApprovalRequired
            | BrokerError::ApprovalDenied
            | BrokerError::Context
    ) {
        AuditOutcome::Denied
    } else {
        AuditOutcome::Failed
    };
    let _ = audit(state, identity, request_id, outcome, payload).await;
    broker_error(&error_value)
}
fn authorize(identity: &GatewayIdentity, scope: &str) -> Result<(), Response> {
    if identity.tenant_id.is_nil() || identity.subject.is_empty() {
        return Err(error(
            StatusCode::UNAUTHORIZED,
            "unauthenticated",
            "valid bearer authentication is required",
        ));
    }
    if !identity.scopes.contains(scope) {
        return Err(error(
            StatusCode::FORBIDDEN,
            "forbidden",
            "required tool scope is missing",
        ));
    }
    Ok(())
}
async fn audit(
    state: &SharedState,
    identity: &GatewayIdentity,
    request_id: &str,
    outcome: AuditOutcome,
    payload: Hash32,
) -> Result<(), ngkg_agent_catalog::CatalogError> {
    state
        .audit
        .append_operation("USER_TOOL", identity, request_id, outcome, payload)
        .await
}
fn payload_hash<T: Serialize>(value: &T) -> Hash32 {
    Hash32(Sha256::digest(serde_json::to_vec(value).unwrap_or_default()).into())
}
fn request_id(headers: &HeaderMap) -> String {
    headers
        .get("x-request-id")
        .and_then(|v| v.to_str().ok())
        .filter(|v| !v.is_empty() && v.len() <= 128)
        .map_or_else(|| Uuid::new_v4().to_string(), ToOwned::to_owned)
}
fn broker_error(value: &BrokerError) -> Response {
    match value {
        BrokerError::Invalid | BrokerError::Arguments | BrokerError::Schema => error(
            StatusCode::BAD_REQUEST,
            "tool_request_invalid",
            "tool request or schema is invalid",
        ),
        BrokerError::NotAllowed => error(
            StatusCode::FORBIDDEN,
            "tool_policy_denied",
            "profile or provider policy denied the tool",
        ),
        BrokerError::ApprovalRequired => error(
            StatusCode::CONFLICT,
            "tool_approval_required",
            "an immutable approval is required",
        ),
        BrokerError::ApprovalDenied => error(
            StatusCode::FORBIDDEN,
            "tool_approval_denied",
            "approval was denied, expired, or mismatched",
        ),
        BrokerError::Context | BrokerError::Evidence => error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "tool_evidence_mismatch",
            "execution, certificate, or catalog evidence does not match",
        ),
        BrokerError::Endpoint => error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "tool_endpoint_unsafe",
            "tool endpoint failed network safety policy",
        ),
        BrokerError::Remote | BrokerError::Http(_) | BrokerError::Io(_) => error(
            StatusCode::BAD_GATEWAY,
            "tool_remote_failed",
            "bounded MCP dependency failed",
        ),
        BrokerError::Limit => error(
            StatusCode::PAYLOAD_TOO_LARGE,
            "tool_limit",
            "tool operation exceeded a configured bound",
        ),
        _ => error(
            StatusCode::SERVICE_UNAVAILABLE,
            "tool_broker_failed",
            "tool broker failed closed",
        ),
    }
}
fn disabled() -> Response {
    error(
        StatusCode::SERVICE_UNAVAILABLE,
        "tool_broker_disabled",
        "user tool broker is disabled",
    )
}
fn audit_failed() -> Response {
    error(
        StatusCode::INTERNAL_SERVER_ERROR,
        "audit_failed",
        "required audit append failed",
    )
}
fn error(status: StatusCode, code: &'static str, message: &'static str) -> Response {
    (status, Json(ErrorBody { code, message })).into_response()
}
