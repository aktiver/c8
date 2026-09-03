//! Authenticated REST surface for every evidence-bound memory operation.

use crate::{SharedState, auth::GatewayIdentity};
use axum::{
    Json, Router,
    extract::{Path, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use ngkg_agent_catalog::{AuditOutcome, Hash32};
use ngkg_agent_memory::{
    MemoryError, MemoryPublishRequest, MemoryReasonRequest, MemorySearchRequest,
    MemorySupersedeRequest, ProposeMemoryRequest,
};
use serde::Serialize;
use sha2::{Digest, Sha256};
use uuid::Uuid;

pub(crate) fn router() -> Router<SharedState> {
    Router::new()
        .route("/v1/memories", post(propose))
        .route("/v1/memories/search", post(search))
        .route("/v1/memories/{memory_id}", get(get_memory))
        .route("/v1/memories/{memory_id}/explain", get(explain))
        .route("/v1/memories/{memory_id}/validate", post(validate))
        .route("/v1/memories/{memory_id}/approve", post(approve))
        .route("/v1/memories/{memory_id}/publish", post(publish))
        .route("/v1/memories/{memory_id}/supersede", post(supersede))
        .route("/v1/memories/{memory_id}/revoke", post(revoke))
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ErrorBody {
    code: &'static str,
    message: &'static str,
}

async fn propose(
    State(state): State<SharedState>,
    headers: HeaderMap,
    axum::Extension(identity): axum::Extension<GatewayIdentity>,
    Json(request): Json<ProposeMemoryRequest>,
) -> Response {
    if let Err(response) = authorize(&identity, "memory:write") {
        return response;
    }
    let Some(memory) = &state.memory else {
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
    match memory
        .propose(identity.tenant_id, &identity.subject, request)
        .await
    {
        Ok(value) => {
            complete(
                &state,
                &identity,
                &request_id,
                payload,
                StatusCode::CREATED,
                value,
            )
            .await
        }
        Err(failure) => failed(&state, &identity, &request_id, payload, failure).await,
    }
}
async fn search(
    State(state): State<SharedState>,
    headers: HeaderMap,
    axum::Extension(identity): axum::Extension<GatewayIdentity>,
    Json(request): Json<MemorySearchRequest>,
) -> Response {
    if let Err(response) = authorize(&identity, "memory:read") {
        return response;
    }
    let Some(memory) = &state.memory else {
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
    match memory
        .search(identity.tenant_id, &identity.subject, request)
        .await
    {
        Ok(value) => {
            complete(
                &state,
                &identity,
                &request_id,
                payload,
                StatusCode::OK,
                value,
            )
            .await
        }
        Err(failure) => failed(&state, &identity, &request_id, payload, failure).await,
    }
}
async fn get_memory(
    State(state): State<SharedState>,
    Path(memory_id): Path<Uuid>,
    headers: HeaderMap,
    axum::Extension(identity): axum::Extension<GatewayIdentity>,
) -> Response {
    if let Err(response) = authorize(&identity, "memory:read") {
        return response;
    }
    let Some(memory) = &state.memory else {
        return disabled();
    };
    let payload = id_hash(memory_id);
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
    match memory
        .get(identity.tenant_id, &identity.subject, memory_id)
        .await
    {
        Ok(value) => {
            complete(
                &state,
                &identity,
                &request_id,
                payload,
                StatusCode::OK,
                value,
            )
            .await
        }
        Err(failure) => failed(&state, &identity, &request_id, payload, failure).await,
    }
}
async fn explain(
    State(state): State<SharedState>,
    Path(memory_id): Path<Uuid>,
    headers: HeaderMap,
    axum::Extension(identity): axum::Extension<GatewayIdentity>,
) -> Response {
    if let Err(response) = authorize(&identity, "memory:read") {
        return response;
    }
    let Some(memory) = &state.memory else {
        return disabled();
    };
    let payload = id_hash(memory_id);
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
    match memory
        .explain(identity.tenant_id, &identity.subject, memory_id)
        .await
    {
        Ok(value) => {
            complete(
                &state,
                &identity,
                &request_id,
                payload,
                StatusCode::OK,
                value,
            )
            .await
        }
        Err(failure) => failed(&state, &identity, &request_id, payload, failure).await,
    }
}
async fn validate(
    State(state): State<SharedState>,
    Path(memory_id): Path<Uuid>,
    headers: HeaderMap,
    axum::Extension(identity): axum::Extension<GatewayIdentity>,
) -> Response {
    if let Err(response) = authorize(&identity, "memory:validate") {
        return response;
    }
    let Some(memory) = &state.memory else {
        return disabled();
    };
    let Some(authorization) = headers.get(header::AUTHORIZATION).cloned() else {
        return delegation_failed();
    };
    let payload = id_hash(memory_id);
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
    match memory
        .validate(
            identity.tenant_id,
            &identity.subject,
            &authorization,
            memory_id,
            &request_id,
        )
        .await
    {
        Ok(value) => {
            complete(
                &state,
                &identity,
                &request_id,
                payload,
                StatusCode::OK,
                value,
            )
            .await
        }
        Err(failure) => failed(&state, &identity, &request_id, payload, failure).await,
    }
}
async fn approve(
    State(state): State<SharedState>,
    Path(memory_id): Path<Uuid>,
    headers: HeaderMap,
    axum::Extension(identity): axum::Extension<GatewayIdentity>,
    Json(request): Json<MemoryReasonRequest>,
) -> Response {
    if let Err(response) = authorize(&identity, "memory:approve") {
        return response;
    }
    let Some(memory) = &state.memory else {
        return disabled();
    };
    let payload = payload_hash(&(memory_id, request.reason_code.clone()));
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
    match memory
        .approve(
            identity.tenant_id,
            &identity.subject,
            memory_id,
            &request.reason_code,
        )
        .await
    {
        Ok(value) => {
            complete(
                &state,
                &identity,
                &request_id,
                payload,
                StatusCode::OK,
                value,
            )
            .await
        }
        Err(failure) => failed(&state, &identity, &request_id, payload, failure).await,
    }
}
async fn publish(
    State(state): State<SharedState>,
    Path(memory_id): Path<Uuid>,
    headers: HeaderMap,
    axum::Extension(identity): axum::Extension<GatewayIdentity>,
    Json(request): Json<MemoryPublishRequest>,
) -> Response {
    if let Err(response) = authorize(&identity, "memory:publish") {
        return response;
    }
    let Some(memory) = &state.memory else {
        return disabled();
    };
    let Some(authorization) = headers.get(header::AUTHORIZATION).cloned() else {
        return delegation_failed();
    };
    let payload = payload_hash(&(memory_id, &request));
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
    match memory
        .publish(
            identity.tenant_id,
            &identity.subject,
            &authorization,
            memory_id,
            request,
            &request_id,
        )
        .await
    {
        Ok(value) => {
            complete(
                &state,
                &identity,
                &request_id,
                payload,
                StatusCode::OK,
                value,
            )
            .await
        }
        Err(failure) => failed(&state, &identity, &request_id, payload, failure).await,
    }
}
async fn supersede(
    State(state): State<SharedState>,
    Path(memory_id): Path<Uuid>,
    headers: HeaderMap,
    axum::Extension(identity): axum::Extension<GatewayIdentity>,
    Json(request): Json<MemorySupersedeRequest>,
) -> Response {
    if let Err(response) = authorize(&identity, "memory:write") {
        return response;
    }
    let Some(memory) = &state.memory else {
        return disabled();
    };
    let payload = payload_hash(&(memory_id, &request));
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
    match memory
        .supersede(identity.tenant_id, &identity.subject, memory_id, request)
        .await
    {
        Ok(value) => {
            complete(
                &state,
                &identity,
                &request_id,
                payload,
                StatusCode::OK,
                value,
            )
            .await
        }
        Err(failure) => failed(&state, &identity, &request_id, payload, failure).await,
    }
}
async fn revoke(
    State(state): State<SharedState>,
    Path(memory_id): Path<Uuid>,
    headers: HeaderMap,
    axum::Extension(identity): axum::Extension<GatewayIdentity>,
    Json(request): Json<MemoryReasonRequest>,
) -> Response {
    if let Err(response) = authorize(&identity, "memory:write") {
        return response;
    }
    let Some(memory) = &state.memory else {
        return disabled();
    };
    let payload = payload_hash(&(memory_id, request.reason_code.clone()));
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
    match memory
        .revoke(
            identity.tenant_id,
            &identity.subject,
            memory_id,
            &request.reason_code,
        )
        .await
    {
        Ok(value) => {
            complete(
                &state,
                &identity,
                &request_id,
                payload,
                StatusCode::OK,
                value,
            )
            .await
        }
        Err(failure) => failed(&state, &identity, &request_id, payload, failure).await,
    }
}

async fn complete<T: Serialize>(
    state: &SharedState,
    identity: &GatewayIdentity,
    request_id: &str,
    payload: Hash32,
    status: StatusCode,
    value: T,
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
        (status, Json(value)).into_response()
    }
}
async fn failed(
    state: &SharedState,
    identity: &GatewayIdentity,
    request_id: &str,
    payload: Hash32,
    failure: MemoryError,
) -> Response {
    let outcome = if matches!(
        failure,
        MemoryError::NotAllowed
            | MemoryError::State
            | MemoryError::Unknown
            | MemoryError::Evidence
            | MemoryError::Poisoned
    ) {
        AuditOutcome::Denied
    } else {
        AuditOutcome::Failed
    };
    let _ = audit(state, identity, request_id, outcome, payload).await;
    memory_error(&failure)
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
            "required memory scope is missing",
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
        .append_operation("AGENT_MEMORY", identity, request_id, outcome, payload)
        .await
}
fn payload_hash<T: Serialize>(value: &T) -> Hash32 {
    match serde_json::to_vec(value) {
        Ok(bytes) => Hash32(Sha256::digest(bytes).into()),
        Err(_) => Hash32([0; 32]),
    }
}
fn id_hash(value: Uuid) -> Hash32 {
    Hash32(Sha256::digest(value.as_bytes()).into())
}
fn request_id(headers: &HeaderMap) -> String {
    headers
        .get("x-request-id")
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty() && value.len() <= 128)
        .map_or_else(|| Uuid::new_v4().to_string(), ToOwned::to_owned)
}
fn memory_error(value: &MemoryError) -> Response {
    match value {
        MemoryError::Invalid | MemoryError::InvalidRdf | MemoryError::Json(_) => error(
            StatusCode::BAD_REQUEST,
            "memory_request_invalid",
            "memory request is invalid",
        ),
        MemoryError::Poisoned => error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "memory_poisoning_blocked",
            "credential-like or policy-poisoning content was blocked",
        ),
        MemoryError::NotAllowed => error(
            StatusCode::FORBIDDEN,
            "memory_access_denied",
            "memory access is denied",
        ),
        MemoryError::State | MemoryError::Conflict => error(
            StatusCode::CONFLICT,
            "memory_state_conflict",
            "memory lifecycle state changed or is invalid",
        ),
        MemoryError::Evidence => error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "memory_evidence_mismatch",
            "memory provenance or semantic evidence does not match",
        ),
        MemoryError::Unknown => error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "memory_not_entailed",
            "semantic memory is unknown under OWL open-world semantics",
        ),
        MemoryError::Query(_) | MemoryError::Envelope(_) => error(
            StatusCode::BAD_GATEWAY,
            "memory_validation_failed",
            "NGKG validation dependency failed closed",
        ),
        _ => error(
            StatusCode::SERVICE_UNAVAILABLE,
            "memory_service_failed",
            "memory service failed closed",
        ),
    }
}
fn disabled() -> Response {
    error(
        StatusCode::SERVICE_UNAVAILABLE,
        "memory_disabled",
        "agent memory is disabled",
    )
}
fn delegation_failed() -> Response {
    error(
        StatusCode::INTERNAL_SERVER_ERROR,
        "delegation_unavailable",
        "internal NGKG delegation is unavailable",
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
