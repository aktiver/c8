//! Authenticated REST admission for managed, reasoning-bound agent execution.

use crate::{SharedState, auth::GatewayIdentity};
use axum::{
    Json, Router,
    extract::State,
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::post,
};
use ngkg_agent_catalog::{AuditOutcome, Hash32};
use ngkg_agent_orchestrator::{AgentExecutionRequest, ExecutionIdentity, OrchestratorError};
use serde::Serialize;
use sha2::{Digest, Sha256};
use uuid::Uuid;

pub(crate) fn router() -> Router<SharedState> {
    Router::new().route("/v1/agent-executions", post(execute))
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ErrorBody {
    code: &'static str,
    message: &'static str,
}

async fn execute(
    State(state): State<SharedState>,
    headers: HeaderMap,
    axum::Extension(identity): axum::Extension<GatewayIdentity>,
    Json(request): Json<AgentExecutionRequest>,
) -> Response {
    if identity.tenant_id.is_nil() || identity.subject.is_empty() {
        return error(
            StatusCode::UNAUTHORIZED,
            "unauthenticated",
            "valid bearer authentication is required",
        );
    }
    if !identity.scopes.contains("agents:execute") {
        return error(
            StatusCode::FORBIDDEN,
            "forbidden",
            "agents:execute scope is required",
        );
    }
    let Some(orchestrator) = &state.orchestrator else {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "managed_agents_disabled",
            "managed agent execution is disabled",
        );
    };
    let Some(authorization) = headers.get(header::AUTHORIZATION).cloned() else {
        return error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "delegation_unavailable",
            "internal delegation token is unavailable",
        );
    };
    let request_id = headers
        .get("x-request-id")
        .and_then(|v| v.to_str().ok())
        .filter(|v| !v.is_empty() && v.len() <= 128)
        .map_or_else(|| Uuid::new_v4().to_string(), ToOwned::to_owned);
    let payload = match serde_json::to_vec(&request) {
        Ok(bytes) => Hash32(Sha256::digest(bytes).into()),
        Err(_) => {
            return error(
                StatusCode::BAD_REQUEST,
                "invalid_request",
                "request encoding failed",
            );
        }
    };
    if state
        .audit
        .append_operation(
            "AGENT_EXECUTION",
            &identity,
            &request_id,
            AuditOutcome::Started,
            payload,
        )
        .await
        .is_err()
    {
        return error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "audit_failed",
            "required audit append failed",
        );
    }
    let execution_identity = ExecutionIdentity {
        tenant_id: identity.tenant_id,
        subject: identity.subject.clone(),
        actor: identity.actor.clone(),
    };
    match orchestrator
        .execute(&execution_identity, &authorization, request, &request_id)
        .await
    {
        Ok(outcome) => {
            let result_hash =
                Hash32::from_lower_hex(&outcome.certificate.certificate_sha256).unwrap_or(payload);
            if state
                .audit
                .append_operation(
                    "AGENT_EXECUTION",
                    &identity,
                    &request_id,
                    AuditOutcome::Completed,
                    result_hash,
                )
                .await
                .is_err()
            {
                return error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "audit_failed",
                    "required audit append failed",
                );
            }
            (StatusCode::OK, Json(outcome)).into_response()
        }
        Err(failure) => {
            let _ = state
                .audit
                .append_operation(
                    "AGENT_EXECUTION",
                    &identity,
                    &request_id,
                    AuditOutcome::Failed,
                    payload,
                )
                .await;
            orchestrator_error(&failure)
        }
    }
}

fn orchestrator_error(error_value: &OrchestratorError) -> Response {
    match error_value {
        OrchestratorError::InvalidRequest => error(
            StatusCode::BAD_REQUEST,
            "invalid_request",
            "agent execution request is invalid",
        ),
        OrchestratorError::NotAllowed => error(
            StatusCode::FORBIDDEN,
            "policy_denied",
            "agent profile does not allow this execution",
        ),
        OrchestratorError::InputNotCompiled => error(
            StatusCode::CONFLICT,
            "input_not_compiled",
            "agent input has not completed deterministic compilation",
        ),
        OrchestratorError::Limit => error(
            StatusCode::PAYLOAD_TOO_LARGE,
            "resource_limit",
            "agent execution exceeds a configured bound",
        ),
        OrchestratorError::UncertifiedContext
        | OrchestratorError::UncertifiedClaim
        | OrchestratorError::EvidenceMismatch => error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "answer_not_certified",
            "no answer was returned because semantic certification failed",
        ),
        OrchestratorError::InvalidClaim => error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "model_claim_invalid",
            "the provider proposed invalid RDF",
        ),
        OrchestratorError::Provider(_) => error(
            StatusCode::BAD_GATEWAY,
            "model_provider_failed",
            "the bounded model provider call failed",
        ),
        _ => error(
            StatusCode::SERVICE_UNAVAILABLE,
            "execution_failed",
            "managed agent execution failed closed",
        ),
    }
}
fn error(status: StatusCode, code: &'static str, message: &'static str) -> Response {
    (status, Json(ErrorBody { code, message })).into_response()
}
