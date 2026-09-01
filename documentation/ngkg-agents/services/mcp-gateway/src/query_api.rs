//! REST parity for every built-in semantic MCP query capability.

use axum::{
    Json, Router,
    extract::{Path, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use ngkg_agent_catalog::{AuditOutcome, Hash32};
use ngkg_api_client::QueryRequest;
use ngkg_mcp_contracts::build_reasoned_context_envelope;
use serde::Serialize;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{SharedState, auth::GatewayIdentity};

pub(crate) fn router() -> Router<SharedState> {
    Router::new()
        .route("/v1/datasets/{dataset_id}/query", post(query))
        .route(
            "/v1/datasets/{dataset_id}/active-snapshot",
            get(active_snapshot),
        )
        .route("/v1/query_logs/{query_execution_id}", get(query_log))
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct QueryLogResponse {
    log: ngkg_api_client::QueryLog,
    resource_semantics: &'static str,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ErrorBody {
    code: &'static str,
    message: &'static str,
}

async fn query(
    State(state): State<SharedState>,
    Path(dataset_id): Path<Uuid>,
    headers: HeaderMap,
    axum::Extension(identity): axum::Extension<GatewayIdentity>,
    Json(request): Json<QueryRequest>,
) -> Response {
    if let Err(response) = authorize(&identity) {
        return response;
    }
    let request_id = request_id(&headers);
    let payload = payload_hash(&(dataset_id, &request));
    if !state.query_tools_enabled {
        if audit(
            &state,
            &identity,
            &request_id,
            AuditOutcome::Denied,
            payload,
        )
        .await
        .is_err()
        {
            return audit_failed();
        }
        return disabled();
    }
    let Some(authorization) = headers.get(header::AUTHORIZATION).cloned() else {
        return error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "delegation_unavailable",
            "internal NGKG delegation is unavailable",
        );
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
        return audit_failed();
    }
    match state
        .query_client
        .query(&authorization, dataset_id, &request, &request_id)
        .await
    {
        Ok(outcome) => match build_reasoned_context_envelope(outcome, state.envelope_limits) {
            Ok(value) => complete(&state, &identity, &request_id, payload, value).await,
            Err(_) => failed(&state, &identity, &request_id, payload).await,
        },
        Err(_) => failed(&state, &identity, &request_id, payload).await,
    }
}

async fn active_snapshot(
    State(state): State<SharedState>,
    Path(dataset_id): Path<Uuid>,
    headers: HeaderMap,
    axum::Extension(identity): axum::Extension<GatewayIdentity>,
) -> Response {
    if let Err(response) = authorize(&identity) {
        return response;
    }
    let request_id = request_id(&headers);
    let payload = id_hash(dataset_id);
    if !state.query_tools_enabled {
        if audit(
            &state,
            &identity,
            &request_id,
            AuditOutcome::Denied,
            payload,
        )
        .await
        .is_err()
        {
            return audit_failed();
        }
        return disabled();
    }
    let Some(authorization) = headers.get(header::AUTHORIZATION).cloned() else {
        return error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "delegation_unavailable",
            "internal NGKG delegation is unavailable",
        );
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
        return audit_failed();
    }
    let request = QueryRequest {
        query: "ASK { }".to_owned(),
        snapshot_id: None,
        hydrate: false,
        default_graph_uris: Vec::new(),
        named_graph_uris: Vec::new(),
    };
    match state
        .query_client
        .query(&authorization, dataset_id, &request, &request_id)
        .await
    {
        Ok(outcome) => match build_reasoned_context_envelope(outcome, state.envelope_limits) {
            Ok(value) => complete(&state, &identity, &request_id, payload, value).await,
            Err(_) => failed(&state, &identity, &request_id, payload).await,
        },
        Err(_) => failed(&state, &identity, &request_id, payload).await,
    }
}

async fn query_log(
    State(state): State<SharedState>,
    Path(query_execution_id): Path<Uuid>,
    headers: HeaderMap,
    axum::Extension(identity): axum::Extension<GatewayIdentity>,
) -> Response {
    if let Err(response) = authorize(&identity) {
        return response;
    }
    let request_id = request_id(&headers);
    let payload = id_hash(query_execution_id);
    if !state.query_tools_enabled {
        if audit(
            &state,
            &identity,
            &request_id,
            AuditOutcome::Denied,
            payload,
        )
        .await
        .is_err()
        {
            return audit_failed();
        }
        return disabled();
    }
    let Some(authorization) = headers.get(header::AUTHORIZATION).cloned() else {
        return error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "delegation_unavailable",
            "internal NGKG delegation is unavailable",
        );
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
        return audit_failed();
    }
    match state
        .query_client
        .query_log(&authorization, query_execution_id, &request_id)
        .await
    {
        Ok(log) => {
            complete(
                &state,
                &identity,
                &request_id,
                payload,
                QueryLogResponse {
                    log,
                    resource_semantics: "CONFIGURED_ALLOCATION_ESTIMATES_NOT_OBSERVED_USAGE",
                },
            )
            .await
        }
        Err(_) => failed(&state, &identity, &request_id, payload).await,
    }
}

async fn complete<T: Serialize>(
    state: &SharedState,
    identity: &GatewayIdentity,
    request_id: &str,
    payload: Hash32,
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
        (StatusCode::OK, Json(value)).into_response()
    }
}

async fn failed(
    state: &SharedState,
    identity: &GatewayIdentity,
    request_id: &str,
    payload: Hash32,
) -> Response {
    if audit(state, identity, request_id, AuditOutcome::Failed, payload)
        .await
        .is_err()
    {
        return audit_failed();
    }
    error(
        StatusCode::BAD_GATEWAY,
        "ngkg_query_failed",
        "NGKG query failed closed",
    )
}

fn authorize(identity: &GatewayIdentity) -> Result<(), Response> {
    if identity.tenant_id.is_nil() || identity.subject.is_empty() {
        return Err(error(
            StatusCode::UNAUTHORIZED,
            "unauthenticated",
            "valid bearer authentication is required",
        ));
    }
    if !identity.scopes.contains("queries:execute") {
        return Err(error(
            StatusCode::FORBIDDEN,
            "forbidden",
            "queries:execute scope is required",
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
        .append_operation(
            "SEMANTIC_QUERY_REST",
            identity,
            request_id,
            outcome,
            payload,
        )
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

fn disabled() -> Response {
    error(
        StatusCode::SERVICE_UNAVAILABLE,
        "semantic_query_tools_disabled",
        "semantic query tools are disabled by deployment policy",
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
