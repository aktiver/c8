//! Authenticated REST surface for resumable long prompts and attachments.

use crate::{SharedState, audit::redacted_payload_sha256, auth::GatewayIdentity};
use axum::{
    Json, Router,
    body::Bytes,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post, put},
};
use ngkg_agent_catalog::{AuditOutcome, Hash32};
use ngkg_agent_input::{CreateInput, InputPart};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

pub(crate) fn router() -> Router<SharedState> {
    Router::new()
        .route("/v1/agent-inputs", post(create_input))
        .route(
            "/v1/agent-inputs/{input_id}/parts/{ordinal}",
            put(upload_part),
        )
        .route("/v1/agent-inputs/{input_id}/finalize", post(finalize_input))
        .route("/v1/agent-inputs/{input_id}", get(status))
        .route("/v1/agent-inputs/{input_id}/manifest", get(manifest))
        .route(
            "/v1/agent-inputs/{input_id}/requirements",
            get(requirements),
        )
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct CreateRequest {
    source_name: String,
    media_type: String,
    #[serde(default)]
    maximum_parts: Option<i32>,
    #[serde(default)]
    maximum_bytes: Option<i64>,
}
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct FinalizeRequest {
    expected_parts: i32,
    expected_bytes: i64,
    source_root_sha256: String,
}
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct PartReceipt {
    input_id: Uuid,
    ordinal: i32,
    byte_length: i64,
    source_sha256: String,
}
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ErrorBody {
    code: &'static str,
    message: &'static str,
}

async fn create_input(
    State(state): State<SharedState>,
    headers: HeaderMap,
    axum::Extension(identity): axum::Extension<GatewayIdentity>,
    Json(request): Json<CreateRequest>,
) -> Response {
    if let Err(r) = authorize(&identity, "agent-inputs:write") {
        return r;
    }
    if request.source_name.is_empty()
        || request.source_name.len() > 1024
        || request.media_type.is_empty()
        || request.media_type.len() > 256
        || request.maximum_parts.is_some_and(|v| v < 1)
        || request.maximum_bytes.is_some_and(|v| v < 1)
    {
        return error(
            StatusCode::BAD_REQUEST,
            "invalid_request",
            "sourceName, mediaType, or limit is invalid",
        );
    }
    let input = CreateInput {
        input_id: Uuid::new_v4(),
        subject: identity.subject.clone(),
        actor: identity.actor.clone(),
        source_name: request.source_name,
        media_type: request.media_type,
        maximum_parts: request
            .maximum_parts
            .unwrap_or(state.maximum_input_parts)
            .min(state.maximum_input_parts),
        maximum_bytes: request
            .maximum_bytes
            .unwrap_or(state.maximum_input_bytes)
            .min(state.maximum_input_bytes),
        created_at_epoch_ms: match epoch_ms() {
            Ok(v) => v,
            Err(_) => {
                return error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "clock_failed",
                    "system clock is unavailable",
                );
            }
        },
    };
    let request_id = request_id(&headers);
    let payload = match redacted_payload_sha256(
        "agent_input_create",
        &(input.input_id, input.maximum_parts, input.maximum_bytes),
    ) {
        Ok(v) => v,
        Err(_) => {
            return error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "audit_failed",
                "audit encoding failed",
            );
        }
    };
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
        return error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "audit_failed",
            "required audit append failed",
        );
    }
    if let Ok(mut result) = state
        .input_repository
        .create_input(identity.tenant_id, &input)
        .await
    {
        redact(&mut result);
        if audit(
            &state,
            &identity,
            &request_id,
            AuditOutcome::Completed,
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
        (StatusCode::CREATED, Json(result)).into_response()
    } else {
        let _ = audit(
            &state,
            &identity,
            &request_id,
            AuditOutcome::Failed,
            payload,
        )
        .await;
        error(
            StatusCode::CONFLICT,
            "input_create_failed",
            "input could not be created",
        )
    }
}

async fn upload_part(
    State(state): State<SharedState>,
    Path((input_id, ordinal)): Path<(Uuid, i32)>,
    headers: HeaderMap,
    axum::Extension(identity): axum::Extension<GatewayIdentity>,
    body: Bytes,
) -> Response {
    if let Err(r) = authorize(&identity, "agent-inputs:write") {
        return r;
    }
    if ordinal < 0
        || ordinal >= state.maximum_input_parts
        || body.is_empty()
        || body.len() > state.maximum_input_part_bytes
    {
        return error(
            StatusCode::PAYLOAD_TOO_LARGE,
            "part_limit",
            "part exceeds the configured limit",
        );
    }
    let expected = match header(&headers, "x-ngkg-content-sha256") {
        Some(v) if canonical_sha(v) => v.to_owned(),
        _ => {
            return error(
                StatusCode::BAD_REQUEST,
                "checksum_required",
                "x-ngkg-content-sha256 must be lowercase SHA-256",
            );
        }
    };
    let observed = hex::encode(Sha256::digest(&body));
    if observed != expected {
        return error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "checksum_mismatch",
            "part checksum does not match",
        );
    }
    let media_type = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/octet-stream")
        .to_owned();
    if media_type.len() > 256 {
        return error(
            StatusCode::BAD_REQUEST,
            "media_type_invalid",
            "content-type is too long",
        );
    }
    let request_id = request_id(&headers);
    let payload =
        Hash32(Sha256::digest(format!("{input_id}:{ordinal}:{expected}").as_bytes()).into());
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
        return error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "audit_failed",
            "required audit append failed",
        );
    }
    let part_ordinal = match u32::try_from(ordinal) {
        Ok(v) => v,
        Err(_) => {
            return error(
                StatusCode::BAD_REQUEST,
                "part_limit",
                "part ordinal is invalid",
            );
        }
    };
    let object_reference = if let Ok(v) = state
        .input_object_store
        .put_part(
            identity.tenant_id,
            input_id,
            part_ordinal,
            &expected,
            body.clone(),
        )
        .await
    {
        v
    } else {
        let _ = audit(
            &state,
            &identity,
            &request_id,
            AuditOutcome::Failed,
            payload,
        )
        .await;
        return error(
            StatusCode::BAD_GATEWAY,
            "object_store_failed",
            "part could not be stored",
        );
    };
    let byte_length = match i64::try_from(body.len()) {
        Ok(v) => v,
        Err(_) => {
            return error(
                StatusCode::PAYLOAD_TOO_LARGE,
                "part_limit",
                "part length overflow",
            );
        }
    };
    let part = InputPart {
        ordinal,
        byte_length,
        media_type,
        source_sha256: expected.clone(),
        object_reference,
    };
    if let Ok(()) = state
        .input_repository
        .record_part(identity.tenant_id, input_id, &part)
        .await
    {
        if audit(
            &state,
            &identity,
            &request_id,
            AuditOutcome::Completed,
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
        (
            StatusCode::OK,
            Json(PartReceipt {
                input_id,
                ordinal,
                byte_length: part.byte_length,
                source_sha256: expected,
            }),
        )
            .into_response()
    } else {
        let _ = audit(
            &state,
            &identity,
            &request_id,
            AuditOutcome::Failed,
            payload,
        )
        .await;
        error(
            StatusCode::CONFLICT,
            "part_conflict",
            "part conflicts with immutable input state",
        )
    }
}

async fn finalize_input(
    State(state): State<SharedState>,
    Path(input_id): Path<Uuid>,
    headers: HeaderMap,
    axum::Extension(identity): axum::Extension<GatewayIdentity>,
    Json(request): Json<FinalizeRequest>,
) -> Response {
    if let Err(r) = authorize(&identity, "agent-inputs:write") {
        return r;
    }
    if request.expected_parts < 1
        || request.expected_parts > state.maximum_input_parts
        || request.expected_bytes < 1
        || request.expected_bytes > state.maximum_input_bytes
        || !canonical_sha(&request.source_root_sha256)
    {
        return error(
            StatusCode::BAD_REQUEST,
            "invalid_manifest",
            "final manifest is invalid",
        );
    }
    let request_id = request_id(&headers);
    let payload = Hash32(
        Sha256::digest(
            format!(
                "{input_id}:{}:{}:{}",
                request.expected_parts, request.expected_bytes, request.source_root_sha256
            )
            .as_bytes(),
        )
        .into(),
    );
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
        return error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "audit_failed",
            "required audit append failed",
        );
    }
    let finalized_at = match epoch_ms() {
        Ok(v) => v,
        Err(_) => {
            return error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "clock_failed",
                "system clock is unavailable",
            );
        }
    };
    if let Ok(mut result) = state
        .input_repository
        .finalize_input(
            identity.tenant_id,
            input_id,
            request.expected_parts,
            request.expected_bytes,
            &request.source_root_sha256,
            finalized_at,
        )
        .await
    {
        redact(&mut result);
        if audit(
            &state,
            &identity,
            &request_id,
            AuditOutcome::Completed,
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
        (StatusCode::ACCEPTED, Json(result)).into_response()
    } else {
        let _ = audit(
            &state,
            &identity,
            &request_id,
            AuditOutcome::Failed,
            payload,
        )
        .await;
        error(
            StatusCode::CONFLICT,
            "finalize_failed",
            "parts do not match the immutable manifest",
        )
    }
}

async fn status(
    State(state): State<SharedState>,
    Path(input_id): Path<Uuid>,
    headers: HeaderMap,
    axum::Extension(identity): axum::Extension<GatewayIdentity>,
) -> Response {
    if let Err(r) = authorize(&identity, "agent-inputs:read") {
        return r;
    }
    match state
        .input_repository
        .status(identity.tenant_id, input_id)
        .await
    {
        Ok(v) => {
            if audit_read(&state, &identity, &headers, input_id, "AGENT_INPUT_STATUS")
                .await
                .is_err()
            {
                return error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "audit_failed",
                    "required audit append failed",
                );
            }
            Json(v).into_response()
        }
        Err(_) => error(StatusCode::NOT_FOUND, "not_found", "input was not found"),
    }
}
async fn manifest(
    State(state): State<SharedState>,
    Path(input_id): Path<Uuid>,
    headers: HeaderMap,
    axum::Extension(identity): axum::Extension<GatewayIdentity>,
) -> Response {
    if let Err(r) = authorize(&identity, "agent-inputs:read") {
        return r;
    }
    match state
        .input_repository
        .manifest(identity.tenant_id, input_id)
        .await
    {
        Ok(mut v) => {
            redact(&mut v);
            if audit_read(
                &state,
                &identity,
                &headers,
                input_id,
                "AGENT_INPUT_MANIFEST_READ",
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
            Json(v).into_response()
        }
        Err(_) => error(StatusCode::NOT_FOUND, "not_found", "input was not found"),
    }
}
async fn requirements(
    State(state): State<SharedState>,
    Path(input_id): Path<Uuid>,
    headers: HeaderMap,
    axum::Extension(identity): axum::Extension<GatewayIdentity>,
) -> Response {
    if let Err(r) = authorize(&identity, "agent-inputs:read") {
        return r;
    }
    match state
        .input_repository
        .requirements(identity.tenant_id, input_id)
        .await
    {
        Ok(v) => {
            if audit_read(
                &state,
                &identity,
                &headers,
                input_id,
                "AGENT_REQUIREMENTS_READ",
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
            Json(v).into_response()
        }
        Err(_) => error(StatusCode::NOT_FOUND, "not_found", "input was not found"),
    }
}

fn redact(manifest: &mut ngkg_agent_input::InputManifest) {
    for part in &mut manifest.parts {
        "REDACTED".clone_into(&mut part.object_reference);
    }
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
            "required input scope is missing",
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
        .append_operation("AGENT_INPUT", identity, request_id, outcome, payload)
        .await
}
async fn audit_read(
    state: &SharedState,
    identity: &GatewayIdentity,
    headers: &HeaderMap,
    input_id: Uuid,
    event_type: &'static str,
) -> Result<(), ngkg_agent_catalog::CatalogError> {
    let payload = Hash32(Sha256::digest(input_id.as_bytes()).into());
    state
        .audit
        .append_operation(
            event_type,
            identity,
            &request_id(headers),
            AuditOutcome::Completed,
            payload,
        )
        .await
}
fn header<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name)?.to_str().ok().filter(|v| !v.is_empty())
}
fn request_id(headers: &HeaderMap) -> String {
    header(headers, "x-request-id")
        .filter(|v| v.len() <= 128)
        .map_or_else(|| Uuid::new_v4().to_string(), ToOwned::to_owned)
}
fn canonical_sha(v: &str) -> bool {
    v.len() == 64
        && v.bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
}
fn epoch_ms() -> Result<i64, std::num::TryFromIntError> {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
}
fn error(status: StatusCode, code: &'static str, message: &'static str) -> Response {
    (status, Json(ErrorBody { code, message })).into_response()
}
