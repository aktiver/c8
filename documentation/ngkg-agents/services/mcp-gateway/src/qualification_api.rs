//! REST control plane for deterministic, multinode CPU qualification work.

use std::time::{SystemTime, UNIX_EPOCH};

use axum::{
    Json, Router,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use ngkg_agent_catalog::{AuditOutcome, Hash32};
use ngkg_cpu_work_plane::{CpuKernel, CpuPartitionInput, CreateQualificationWorkload};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{SharedState, auth::GatewayIdentity};

pub(crate) fn router() -> Router<SharedState> {
    Router::new()
        .route("/v1/qualification-workloads", post(create))
        .route(
            "/v1/qualification-workloads/{workload_id}",
            get(get_workload),
        )
        .route(
            "/v1/qualification-workloads/{workload_id}/checkpoints",
            get(checkpoints),
        )
        .route(
            "/v1/qualification-workloads/{workload_id}/cancel",
            post(cancel),
        )
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct CreateRequest {
    input_id: Uuid,
    idempotency_key: String,
    #[serde(default = "default_attempts")]
    maximum_attempts: u32,
    #[serde(default = "default_partition_bytes")]
    maximum_partition_bytes: u64,
    #[serde(default = "default_spill_bytes")]
    maximum_spill_bytes: u64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ErrorBody {
    code: &'static str,
    message: &'static str,
}

async fn create(
    State(state): State<SharedState>,
    headers: HeaderMap,
    axum::Extension(identity): axum::Extension<GatewayIdentity>,
    Json(request): Json<CreateRequest>,
) -> Response {
    if let Err(response) = authorize(&identity, "qualification:write") {
        return response;
    }
    if request.idempotency_key.len() < 8
        || request.idempotency_key.len() > 256
        || request.maximum_attempts == 0
        || request.maximum_attempts > 20
        || request.maximum_partition_bytes == 0
        || request.maximum_spill_bytes == 0
    {
        return error(
            StatusCode::BAD_REQUEST,
            "invalid_request",
            "qualification limits or idempotency key are invalid",
        );
    }
    let manifest = match state
        .input_repository
        .manifest(identity.tenant_id, request.input_id)
        .await
    {
        Ok(value) if value.state != "UPLOADING" && !value.parts.is_empty() => value,
        _ => {
            return error(
                StatusCode::CONFLICT,
                "input_not_frozen",
                "input must be checksum-frozen before qualification",
            );
        }
    };
    let partitions = match manifest
        .parts
        .into_iter()
        .map(|part| {
            Ok(CpuPartitionInput {
                ordinal: u32::try_from(part.ordinal)?,
                object_reference: part.object_reference,
                source_sha256: part.source_sha256,
                byte_length: u64::try_from(part.byte_length)?,
            })
        })
        .collect::<Result<Vec<_>, std::num::TryFromIntError>>()
    {
        Ok(value)
            if value
                .iter()
                .all(|part| part.byte_length <= request.maximum_partition_bytes) =>
        {
            value
        }
        _ => {
            return error(
                StatusCode::PAYLOAD_TOO_LARGE,
                "partition_limit",
                "an immutable input partition exceeds maximumPartitionBytes",
            );
        }
    };
    let command = CreateQualificationWorkload {
        kernel: CpuKernel::CanonicalLinesetV1,
        partitions,
        maximum_attempts: request.maximum_attempts,
        maximum_partition_bytes: request.maximum_partition_bytes,
        maximum_spill_bytes: request.maximum_spill_bytes,
        idempotency_key: request.idempotency_key,
    };
    let request_id = request_id(&headers);
    let payload = Hash32(
        Sha256::digest(format!("{}:{}", request.input_id, command.idempotency_key).as_bytes())
            .into(),
    );
    if state
        .audit
        .append_operation(
            "CPU_QUALIFICATION_CREATE",
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
    if let Ok(value) = state
        .cpu_work
        .create_qualification(identity.tenant_id, &identity.subject, &command, epoch_ms())
        .await
    {
        if state
            .audit
            .append_operation(
                "CPU_QUALIFICATION_CREATE",
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
        (StatusCode::ACCEPTED, Json(value)).into_response()
    } else {
        let _ = state
            .audit
            .append_operation(
                "CPU_QUALIFICATION_CREATE",
                &identity,
                &request_id,
                AuditOutcome::Failed,
                payload,
            )
            .await;
        error(
            StatusCode::CONFLICT,
            "workload_create_failed",
            "qualification workload could not be created",
        )
    }
}

async fn get_workload(
    State(state): State<SharedState>,
    Path(workload_id): Path<Uuid>,
    axum::Extension(identity): axum::Extension<GatewayIdentity>,
) -> Response {
    if let Err(response) = authorize(&identity, "qualification:read") {
        return response;
    }
    match state.cpu_work.get(identity.tenant_id, workload_id).await {
        Ok(value) => Json(value).into_response(),
        Err(_) => error(
            StatusCode::NOT_FOUND,
            "not_found",
            "qualification workload was not found",
        ),
    }
}

async fn checkpoints(
    State(state): State<SharedState>,
    Path(workload_id): Path<Uuid>,
    axum::Extension(identity): axum::Extension<GatewayIdentity>,
) -> Response {
    if let Err(response) = authorize(&identity, "qualification:read") {
        return response;
    }
    match state
        .cpu_work
        .checkpoints(identity.tenant_id, workload_id)
        .await
    {
        Ok(value) => Json(value).into_response(),
        Err(_) => error(
            StatusCode::NOT_FOUND,
            "not_found",
            "qualification workload was not found",
        ),
    }
}

async fn cancel(
    State(state): State<SharedState>,
    Path(workload_id): Path<Uuid>,
    axum::Extension(identity): axum::Extension<GatewayIdentity>,
) -> Response {
    if let Err(response) = authorize(&identity, "qualification:cancel") {
        return response;
    }
    match state
        .cpu_work
        .cancel(identity.tenant_id, workload_id, epoch_ms())
        .await
    {
        Ok(value) => Json(value).into_response(),
        Err(_) => error(
            StatusCode::CONFLICT,
            "cancel_failed",
            "workload is terminal or could not be cancelled",
        ),
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
            "required qualification scope is missing",
        ));
    }
    Ok(())
}
fn request_id(headers: &HeaderMap) -> String {
    headers
        .get("x-request-id")
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty() && value.len() <= 128)
        .map_or_else(|| Uuid::new_v4().to_string(), ToOwned::to_owned)
}
fn epoch_ms() -> i64 {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(i64::MAX)
}
fn default_attempts() -> u32 {
    5
}
fn default_partition_bytes() -> u64 {
    67_108_864
}
fn default_spill_bytes() -> u64 {
    8_589_934_592
}
fn error(status: StatusCode, code: &'static str, message: &'static str) -> Response {
    (status, Json(ErrorBody { code, message })).into_response()
}
