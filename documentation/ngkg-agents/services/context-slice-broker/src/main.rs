//! REST-driven large context-slice broker. Object credentials terminate here.

#![allow(missing_docs)]

use std::{
    collections::BTreeSet,
    env,
    fs::OpenOptions,
    io::Write,
    net::SocketAddr,
    os::unix::fs::OpenOptionsExt,
    path::PathBuf,
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use axum::{
    Json, Router,
    body::Bytes,
    extract::{DefaultBodyLimit, Path, Request, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post, put},
};
use ngkg_auth::{
    AuthenticationConfiguration, Authenticator, DelegationConfiguration, Identity,
    OpaqueConfiguration,
};
use ngkg_context_slice::{
    CapabilityIssuer, CapabilityRequest, ChunkLocator, ContextObjectStore,
    ContextStoreConfiguration, CreateSliceRequest, IndexLimits, SliceError, SliceManifest,
    SliceRepository, SliceState, VerifiedLocatorIndex, build_index, sha256,
};
use ngkg_hpc_runtime::ResourceBudget;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::sync::Semaphore;
use tower_http::{request_id::MakeRequestUuid, trace::TraceLayer};
use url::Url;
use uuid::Uuid;

#[derive(Clone)]
struct AppState {
    repository: SliceRepository,
    store: ContextObjectStore,
    signer: CapabilityIssuer,
    authenticator: Authenticator,
    limits: Limits,
    allowed_audiences: Arc<BTreeSet<String>>,
    kms_key_id_sha256: String,
    index_stage: PathBuf,
    index_owner_uid: u32,
    range_admission: Arc<Semaphore>,
    metrics: Arc<Metrics>,
}

#[derive(Clone, Copy)]
struct Limits {
    maximum_ttl_seconds: u64,
    recovery_window_seconds: u64,
    maximum_chunk_bytes: usize,
    maximum_range_bytes: usize,
    maximum_chunks: usize,
    maximum_index_bytes: usize,
}

#[derive(Default)]
struct Metrics {
    active_reads: AtomicU64,
    bytes_served: AtomicU64,
    checksum_failures: AtomicU64,
    capability_denials: AtomicU64,
    mapped_bytes: AtomicU64,
    read_milliseconds: AtomicU64,
    reads: AtomicU64,
    expirations: AtomicU64,
}
struct MappedCharge {
    metrics: Arc<Metrics>,
    bytes: u64,
}
impl MappedCharge {
    fn new(metrics: Arc<Metrics>, bytes: usize) -> Self {
        let bytes = u64::try_from(bytes).unwrap_or(u64::MAX);
        metrics.mapped_bytes.fetch_add(bytes, Ordering::Relaxed);
        Self { metrics, bytes }
    }
}
impl Drop for MappedCharge {
    fn drop(&mut self) {
        self.metrics
            .mapped_bytes
            .fetch_sub(self.bytes, Ordering::Relaxed);
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct FinalizeRequest {
    content_sha256: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CapabilityResponse {
    capability_id: Uuid,
    token: String,
    expires_at_epoch: u64,
    range_start: u64,
    range_end_exclusive: u64,
    audience: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ErrorBody {
    code: &'static str,
    message: &'static str,
}
struct AuthIdentity(Identity);

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let bind = required("NGKG_CONTEXT_BIND")?.parse::<SocketAddr>()?;
    let database = required("NGKG_AGENT_DATABASE_URL")?;
    let authenticator = Authenticator::build(authentication_configuration()?).await?;
    let repository = SliceRepository::connect(
        &database,
        positive_u32("NGKG_CONTEXT_DATABASE_MAX_CONNECTIONS", 32)?,
        Duration::from_millis(positive_u64(
            "NGKG_CONTEXT_DATABASE_ACQUIRE_TIMEOUT_MS",
            5000,
        )?),
    )
    .await?;
    let cgroup_threads = ResourceBudget::from_cgroup(50, 1)?.threads.max(1);
    let hash_tasks = positive_usize("NGKG_CONTEXT_MAX_HASH_TASKS", 16)?.min(cgroup_threads);
    let store =
        ContextObjectStore::build(ContextStoreConfiguration::from_environment()?, hash_tasks)?;
    let maximum_capability_ttl_seconds =
        positive_u64("NGKG_CONTEXT_CAPABILITY_MAX_TTL_SECONDS", 300)?;
    let signer = CapabilityIssuer::load(
        required("NGKG_CONTEXT_CAPABILITY_ISSUER")?,
        &PathBuf::from(required("NGKG_CONTEXT_CAPABILITY_KEY_FILE")?),
        &required("NGKG_CONTEXT_CAPABILITY_KEY_SHA256")?,
        Duration::from_secs(maximum_capability_ttl_seconds),
        Duration::from_secs(positive_u64(
            "NGKG_CONTEXT_CAPABILITY_CLOCK_SKEW_SECONDS",
            15,
        )?),
    )?;
    let stage = PathBuf::from(required("NGKG_CONTEXT_INDEX_STAGE_DIR")?);
    std::fs::create_dir_all(&stage)?;
    let audiences = comma_set("NGKG_CONTEXT_CAPABILITY_AUDIENCES")?;
    let maximum_in_flight = positive_usize("NGKG_CONTEXT_MAX_IN_FLIGHT_READS", 64)?;
    let state = AppState {
        repository,
        store,
        signer,
        authenticator,
        limits: Limits {
            maximum_ttl_seconds: positive_u64("NGKG_CONTEXT_MAX_TTL_SECONDS", 604_800)?,
            recovery_window_seconds: positive_u64("NGKG_CONTEXT_RECOVERY_WINDOW_SECONDS", 86_400)?,
            maximum_chunk_bytes: positive_usize("NGKG_CONTEXT_MAX_CHUNK_BYTES", 67_108_864)?,
            maximum_range_bytes: positive_usize("NGKG_CONTEXT_MAX_RANGE_BYTES", 67_108_864)?,
            maximum_chunks: positive_usize("NGKG_CONTEXT_MAX_CHUNKS", 1_000_000)?,
            maximum_index_bytes: positive_usize("NGKG_CONTEXT_MAX_INDEX_BYTES", 268_435_456)?,
        },
        allowed_audiences: Arc::new(audiences),
        kms_key_id_sha256: required("NGKG_CONTEXT_KMS_KEY_ID_SHA256")?,
        index_stage: stage,
        index_owner_uid: positive_u32("NGKG_CONTEXT_INDEX_OWNER_UID", 65532)?,
        range_admission: Arc::new(Semaphore::new(maximum_in_flight)),
        metrics: Arc::new(Metrics::default()),
    };
    let protected = Router::new()
        .route("/v1/context-slices", post(create_slice))
        .route("/v1/context-slices/{slice_id}", get(get_slice))
        .route(
            "/v1/context-slices/{slice_id}/chunks/{ordinal}",
            put(put_chunk),
        )
        .route(
            "/v1/context-slices/{slice_id}/finalize",
            post(finalize_slice),
        )
        .route(
            "/v1/context-slices/{slice_id}/capabilities",
            post(issue_capability),
        )
        .route("/v1/context-slices/{slice_id}/expire", post(expire_slice))
        .layer(middleware::from_fn_with_state(state.clone(), require_auth));
    let app = Router::new()
        .merge(protected)
        .route("/v1/context-slices/{slice_id}/content", get(read_range))
        .route("/health/live", get(|| async { StatusCode::NO_CONTENT }))
        .route("/health/ready", get(ready))
        .route("/metrics", get(metrics))
        .route(
            "/openapi.yaml",
            get(|| async {
                (
                    [("content-type", "application/yaml")],
                    include_str!("../../../contracts/context-slice-openapi.yaml"),
                )
            }),
        )
        .route("/swagger-ui", get(swagger))
        .with_state(state)
        .layer(DefaultBodyLimit::max(268_435_456))
        .layer(TraceLayer::new_for_http())
        .layer(tower_http::request_id::SetRequestIdLayer::new(
            header::HeaderName::from_static("x-request-id"),
            MakeRequestUuid,
        ));
    let listener = tokio::net::TcpListener::bind(bind).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn require_auth(State(state): State<AppState>, mut request: Request, next: Next) -> Response {
    match state.authenticator.authenticate(request.headers()).await {
        Ok(auth) => {
            request.extensions_mut().insert(auth.identity);
            next.run(request).await
        }
        Err(_) => (
            StatusCode::UNAUTHORIZED,
            Json(ErrorBody {
                code: "unauthenticated",
                message: "valid bearer authentication is required",
            }),
        )
            .into_response(),
    }
}

async fn create_slice(
    State(state): State<AppState>,
    AuthIdentity(identity): AuthIdentity,
    Json(body): Json<CreateSliceRequest>,
) -> Result<impl IntoResponse, ApiError> {
    require_scope(&identity, "context-slices:write")?;
    let now = now_ms()?;
    let view = state
        .repository
        .create(
            identity.tenant_id,
            &identity.subject,
            &body,
            state.limits.maximum_ttl_seconds,
            state.limits.recovery_window_seconds,
            &state.kms_key_id_sha256,
            now,
        )
        .await?;
    Ok((StatusCode::CREATED, Json(view)))
}

async fn get_slice(
    State(state): State<AppState>,
    AuthIdentity(identity): AuthIdentity,
    Path(slice): Path<Uuid>,
) -> Result<Json<ngkg_context_slice::SliceView>, ApiError> {
    require_scope(&identity, "context-slices:read")?;
    Ok(Json(state.repository.get(identity.tenant_id, slice).await?))
}

async fn put_chunk(
    State(state): State<AppState>,
    AuthIdentity(identity): AuthIdentity,
    Path((slice, ordinal)): Path<(Uuid, u32)>,
    headers: HeaderMap,
    bytes: Bytes,
) -> Result<StatusCode, ApiError> {
    require_scope(&identity, "context-slices:write")?;
    if bytes.is_empty() || bytes.len() > state.limits.maximum_chunk_bytes {
        return Err(SliceError::Limit.into());
    }
    let digest = header_value(&headers, "x-ngkg-content-sha256")?;
    if state.store.digest_bytes(bytes.clone()).await? != digest {
        return Err(SliceError::Checksum.into());
    }
    let view = state.repository.get(identity.tenant_id, slice).await?;
    if view.state != SliceState::Uploading {
        return Err(SliceError::State.into());
    }
    let start = u64::from(ordinal)
        .checked_mul(view.chunk_size_bytes)
        .ok_or(SliceError::Limit)?;
    let end = start
        .checked_add(u64::try_from(bytes.len()).map_err(|_| SliceError::Limit)?)
        .ok_or(SliceError::Limit)?;
    let reference =
        ContextObjectStore::chunk_reference(identity.tenant_id, slice, ordinal, &digest)?;
    state
        .repository
        .add_chunk(
            identity.tenant_id,
            slice,
            ordinal,
            start,
            end,
            &digest,
            &reference,
            now_ms()?,
        )
        .await?;
    state
        .store
        .put_chunk(identity.tenant_id, slice, ordinal, &digest, bytes)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn finalize_slice(
    State(state): State<AppState>,
    AuthIdentity(identity): AuthIdentity,
    Path(slice): Path<Uuid>,
    Json(request): Json<FinalizeRequest>,
) -> Result<Json<ngkg_context_slice::SliceView>, ApiError> {
    require_scope(&identity, "context-slices:write")?;
    let view = state.repository.get(identity.tenant_id, slice).await?;
    if view.state != SliceState::Uploading {
        return Err(SliceError::State.into());
    }
    let chunks = state.repository.chunks(identity.tenant_id, slice).await?;
    if chunks.is_empty() || chunks.len() > state.limits.maximum_chunks {
        return Err(SliceError::Limit.into());
    }
    let mut content = Sha256::new();
    let mut cursor = 0_u64;
    for chunk in &chunks {
        if usize::try_from(chunk.ordinal).map_err(|_| SliceError::Limit)?
            >= state.limits.maximum_chunks
            || chunk.byte_start != cursor
        {
            return Err(SliceError::Integrity("chunk continuity").into());
        }
        let bytes = state
            .store
            .get_verified(
                &chunk.object_reference,
                &chunk.chunk_sha256,
                state.limits.maximum_chunk_bytes,
            )
            .await
            .inspect_err(|_e| {
                state
                    .metrics
                    .checksum_failures
                    .fetch_add(1, Ordering::Relaxed);
            })?;
        tokio::task::block_in_place(|| content.update(&bytes));
        cursor = chunk.byte_end_exclusive;
    }
    let observed = hex::encode(content.finalize());
    if observed != request.content_sha256 || cursor != view.expected_total_bytes {
        return Err(SliceError::Checksum.into());
    }
    let locators = chunks
        .iter()
        .map(|c| ChunkLocator {
            chunk_sha256: c.chunk_sha256.clone(),
            ordinal: c.ordinal,
            byte_start: c.byte_start,
            byte_end_exclusive: c.byte_end_exclusive,
        })
        .collect();
    let expected_total = view.expected_total_bytes;
    let expected_content = observed.clone();
    let index_bytes = tokio::task::spawn_blocking(move || {
        build_index(locators, &expected_content, expected_total)
    })
    .await
    .map_err(|_| SliceError::Integrity("index worker"))??;
    if index_bytes.len() > state.limits.maximum_index_bytes {
        return Err(SliceError::Limit.into());
    }
    let (index_ref, index_sha) = state
        .store
        .put_index(identity.tenant_id, slice, Bytes::from(index_bytes.clone()))
        .await?;
    let index_length = u64::try_from(index_bytes.len()).map_err(|_| SliceError::Limit)?;
    let manifest = SliceManifest {
        version: "ngkg-context-slice-v1",
        slice_id: slice,
        dataset_id: view.dataset_id,
        snapshot_id: view.snapshot_id,
        authorized_graph_set_sha256: view.authorized_graph_set_sha256.clone(),
        semantic_result_sha256: view.semantic_result_sha256.clone(),
        content_sha256: observed.clone(),
        media_type: view.media_type.clone(),
        total_bytes: view.expected_total_bytes,
        total_triples: view.total_triples,
        chunks: chunks.clone(),
        index_sha256: index_sha.clone(),
        index_bytes: index_length,
        expires_at_epoch_ms: view.expires_at_epoch_ms,
    };
    let manifest_bytes = serde_json::to_vec(&manifest)?;
    let manifest_sha = sha256(&manifest_bytes);
    let manifest_ref = state
        .store
        .put_manifest(
            identity.tenant_id,
            slice,
            &manifest_sha,
            Bytes::from(manifest_bytes),
        )
        .await?;
    Ok(Json(
        state
            .repository
            .activate(
                identity.tenant_id,
                slice,
                &observed,
                &manifest_sha,
                &manifest_ref,
                &index_sha,
                index_length,
                &index_ref,
                view.expected_total_bytes,
                now_ms()?,
            )
            .await?,
    ))
}

async fn issue_capability(
    State(state): State<AppState>,
    AuthIdentity(identity): AuthIdentity,
    Path(slice): Path<Uuid>,
    Json(request): Json<CapabilityRequest>,
) -> Result<Json<CapabilityResponse>, ApiError> {
    require_scope(&identity, "context-slices:read")?;
    if !state.allowed_audiences.contains(&request.audience) {
        return Err(SliceError::Unauthorized.into());
    }
    let view = state.repository.get(identity.tenant_id, slice).await?;
    if view.state != SliceState::Active {
        return Err(SliceError::State.into());
    }
    let now = u64::try_from(now_ms()? / 1000).map_err(|_| SliceError::Integrity("clock"))?;
    let manifest = view
        .manifest_sha256
        .as_deref()
        .ok_or(SliceError::Integrity("manifest identity"))?;
    let policy = hex::encode(identity.policy_version_sha256);
    let (token, claims) = state.signer.issue(
        identity.tenant_id,
        slice,
        &identity.subject,
        manifest,
        &policy,
        &request,
        view.total_bytes
            .ok_or(SliceError::Integrity("total bytes"))?,
        now,
    )?;
    let nonce =
        Uuid::parse_str(&claims.jti).map_err(|_| SliceError::Integrity("capability nonce"))?;
    if claims.exp.saturating_mul(1000)
        > u64::try_from(view.expires_at_epoch_ms)
            .map_err(|_| SliceError::Integrity("slice expiry"))?
    {
        return Err(SliceError::Invalid("capability exceeds slice expiry").into());
    }
    let issued_ms = i64::try_from(claims.iat.checked_mul(1000).ok_or(SliceError::Limit)?)
        .map_err(|_| SliceError::Limit)?;
    let expires_ms = i64::try_from(claims.exp.checked_mul(1000).ok_or(SliceError::Limit)?)
        .map_err(|_| SliceError::Limit)?;
    let id = state
        .repository
        .record_capability(
            identity.tenant_id,
            slice,
            &identity.subject,
            &request.audience,
            nonce,
            request.range_start,
            request.range_end_exclusive,
            &CapabilityIssuer::token_sha256(&token),
            &policy,
            issued_ms,
            expires_ms,
        )
        .await?;
    Ok(Json(CapabilityResponse {
        capability_id: id,
        token,
        expires_at_epoch: claims.exp,
        range_start: claims.range_start,
        range_end_exclusive: claims.range_end_exclusive,
        audience: claims.aud,
    }))
}

async fn read_range(
    State(state): State<AppState>,
    Path(slice): Path<Uuid>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let permit = state
        .range_admission
        .clone()
        .try_acquire_owned()
        .map_err(|_| SliceError::Limit)?;
    state.metrics.active_reads.fetch_add(1, Ordering::Relaxed);
    let started = Instant::now();
    let result = read_range_inner(&state, slice, &headers).await;
    drop(permit);
    state.metrics.active_reads.fetch_sub(1, Ordering::Relaxed);
    state.metrics.reads.fetch_add(1, Ordering::Relaxed);
    let elapsed = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
    state
        .metrics
        .read_milliseconds
        .fetch_add(elapsed, Ordering::Relaxed);
    result
}

async fn read_range_inner(
    state: &AppState,
    slice: Uuid,
    headers: &HeaderMap,
) -> Result<Response, ApiError> {
    let token = header_value(headers, "x-ngkg-slice-capability")?;
    let audience = header_value(headers, "x-ngkg-capability-audience")?;
    let now = now_ms()?;
    let claims = state
        .signer
        .verify(
            &token,
            &audience,
            u64::try_from(now / 1000).map_err(|_| SliceError::Integrity("clock"))?,
        )
        .inspect_err(|_e| {
            state
                .metrics
                .capability_denials
                .fetch_add(1, Ordering::Relaxed);
        })?;
    if claims.slice_id != slice
        || !state.allowed_audiences.contains(&audience)
        || (claims.range_end_exclusive - claims.range_start)
            > u64::try_from(state.limits.maximum_range_bytes).map_err(|_| SliceError::Limit)?
    {
        return Err(SliceError::Unauthorized.into());
    }
    let nonce = Uuid::parse_str(&claims.jti).map_err(|_| SliceError::Unauthorized)?;
    if !state
        .repository
        .capability_valid(
            claims.tenant_id,
            slice,
            nonce,
            &CapabilityIssuer::token_sha256(&token),
            now,
        )
        .await?
    {
        return Err(SliceError::Unauthorized.into());
    }
    let view = state.repository.get(claims.tenant_id, slice).await?;
    if view.manifest_sha256.as_deref() != Some(&claims.manifest_sha256) {
        return Err(SliceError::Integrity("manifest binding").into());
    }
    let (index_ref, index_sha, index_len) = state
        .repository
        .index_material(claims.tenant_id, slice)
        .await?;
    let bytes = state
        .store
        .get_verified(&index_ref, &index_sha, state.limits.maximum_index_bytes)
        .await?;
    let stage = state
        .index_stage
        .join(format!("{}.{}.idx", slice, Uuid::new_v4()));
    let mut options = OpenOptions::new();
    options.write(true).create_new(true).mode(0o600);
    let mut file = options.open(&stage)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    drop(file);
    let index_result = VerifiedLocatorIndex::from_staged_file(
        &stage,
        &index_sha,
        index_len,
        IndexLimits {
            maximum_records: state.limits.maximum_chunks,
            maximum_mapped_bytes: state.limits.maximum_index_bytes,
            expected_owner_uid: state.index_owner_uid,
        },
    );
    let _ = std::fs::remove_file(&stage);
    let index = index_result?;
    if index.content_sha256()
        != view
            .content_sha256
            .as_deref()
            .ok_or(SliceError::Integrity("content identity"))?
        || index.total_bytes()
            != view
                .total_bytes
                .ok_or(SliceError::Integrity("total bytes"))?
    {
        return Err(SliceError::Integrity("index content binding").into());
    }
    let _mapped_charge = MappedCharge::new(Arc::clone(&state.metrics), index.mapped_bytes());
    let range_length = claims.range_end_exclusive - claims.range_start;
    let chunks = state.repository.chunks(claims.tenant_id, slice).await?;
    let mut output =
        Vec::with_capacity(usize::try_from(range_length).map_err(|_| SliceError::Limit)?);
    for chunk in chunks.iter().filter(|c| {
        c.byte_end_exclusive > claims.range_start && c.byte_start < claims.range_end_exclusive
    }) {
        let indexed = index
            .locate(&chunk.chunk_sha256, chunk.ordinal)?
            .ok_or(SliceError::Integrity("index locator absent"))?;
        if indexed.ordinal != chunk.ordinal
            || indexed.byte_start != chunk.byte_start
            || indexed.byte_end_exclusive != chunk.byte_end_exclusive
        {
            return Err(SliceError::Integrity("index locator mismatch").into());
        }
        let object = state
            .store
            .get_verified(
                &chunk.object_reference,
                &chunk.chunk_sha256,
                state.limits.maximum_chunk_bytes,
            )
            .await?;
        let from = usize::try_from(claims.range_start.saturating_sub(chunk.byte_start))
            .map_err(|_| SliceError::Limit)?;
        let to = usize::try_from(
            claims.range_end_exclusive.min(chunk.byte_end_exclusive) - chunk.byte_start,
        )
        .map_err(|_| SliceError::Limit)?;
        output.extend_from_slice(&object[from..to]);
    }
    let output_length = u64::try_from(output.len()).map_err(|_| SliceError::Limit)?;
    if output_length != range_length {
        return Err(SliceError::Integrity("range assembly").into());
    }
    state
        .metrics
        .bytes_served
        .fetch_add(output_length, Ordering::Relaxed);
    Ok((
        StatusCode::PARTIAL_CONTENT,
        [
            (
                header::CONTENT_TYPE,
                HeaderValue::from_str(&view.media_type)
                    .map_err(|_| SliceError::Integrity("media type"))?,
            ),
            (
                header::CONTENT_RANGE,
                HeaderValue::from_str(&format!(
                    "bytes {}-{}/{}",
                    claims.range_start,
                    claims.range_end_exclusive - 1,
                    view.total_bytes.unwrap_or_default()
                ))
                .map_err(|_| SliceError::Integrity("content range"))?,
            ),
        ],
        output,
    )
        .into_response())
}

async fn expire_slice(
    State(state): State<AppState>,
    AuthIdentity(identity): AuthIdentity,
    Path(slice): Path<Uuid>,
) -> Result<Json<ngkg_context_slice::SliceView>, ApiError> {
    require_scope(&identity, "context-slices:write")?;
    let view = state
        .repository
        .mark_expired(identity.tenant_id, slice, now_ms()?)
        .await?;
    state.metrics.expirations.fetch_add(1, Ordering::Relaxed);
    Ok(Json(view))
}
async fn ready(State(state): State<AppState>) -> StatusCode {
    if state.authenticator.ready().await.is_ok() {
        StatusCode::NO_CONTENT
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}
async fn metrics(State(state): State<AppState>) -> String {
    let mapped = state.metrics.mapped_bytes.load(Ordering::Relaxed);
    format!(
        "# TYPE ngkg_context_active_reads gauge\nngkg_context_active_reads {}\n# TYPE ngkg_context_bytes_served_total counter\nngkg_context_bytes_served_total {}\n# TYPE ngkg_context_checksum_failures_total counter\nngkg_context_checksum_failures_total {}\n# TYPE ngkg_context_capability_denials_total counter\nngkg_context_capability_denials_total {}\n# TYPE ngkg_context_mapped_bytes gauge\nngkg_context_mapped_bytes {mapped}\n# TYPE ngkg_context_resident_estimate_bytes gauge\nngkg_context_resident_estimate_bytes {mapped}\n# TYPE ngkg_context_read_milliseconds_total counter\nngkg_context_read_milliseconds_total {}\n# TYPE ngkg_context_reads_total counter\nngkg_context_reads_total {}\n# TYPE ngkg_context_expirations_total counter\nngkg_context_expirations_total {}\n",
        state.metrics.active_reads.load(Ordering::Relaxed),
        state.metrics.bytes_served.load(Ordering::Relaxed),
        state.metrics.checksum_failures.load(Ordering::Relaxed),
        state.metrics.capability_denials.load(Ordering::Relaxed),
        state.metrics.read_milliseconds.load(Ordering::Relaxed),
        state.metrics.reads.load(Ordering::Relaxed),
        state.metrics.expirations.load(Ordering::Relaxed)
    )
}
async fn swagger() -> impl IntoResponse {
    axum::response::Html(
        r#"<!doctype html><html><body><div id="swagger-ui"></div><script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js"></script><script>SwaggerUIBundle({url:'/openapi.yaml',dom_id:'#swagger-ui'});</script></body></html>"#,
    )
}

fn require_scope(identity: &Identity, scope: &str) -> Result<(), ApiError> {
    if identity.scopes.contains(scope) {
        Ok(())
    } else {
        Err(SliceError::Unauthorized.into())
    }
}
fn header_value(headers: &HeaderMap, name: &'static str) -> Result<String, ApiError> {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .filter(|v| !v.is_empty() && v.len() <= 16_384)
        .map(ToOwned::to_owned)
        .ok_or_else(|| SliceError::Invalid("required header").into())
}
fn now_ms() -> Result<i64, SliceError> {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| SliceError::Integrity("clock"))?
            .as_millis(),
    )
    .map_err(|_| SliceError::Integrity("clock"))
}

struct ApiError(SliceError);
impl From<SliceError> for ApiError {
    fn from(e: SliceError) -> Self {
        Self(e)
    }
}
impl From<serde_json::Error> for ApiError {
    fn from(e: serde_json::Error) -> Self {
        Self(e.into())
    }
}
impl From<std::io::Error> for ApiError {
    fn from(e: std::io::Error) -> Self {
        Self(e.into())
    }
}
impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, code, message) = match self.0 {
            SliceError::Unauthorized => (
                StatusCode::FORBIDDEN,
                "forbidden",
                "context-slice access denied",
            ),
            SliceError::NotFound => (
                StatusCode::NOT_FOUND,
                "not_found",
                "context slice not found",
            ),
            SliceError::Invalid(_) => (
                StatusCode::BAD_REQUEST,
                "invalid_request",
                "context-slice request is invalid",
            ),
            SliceError::State => (
                StatusCode::CONFLICT,
                "invalid_state",
                "context-slice state conflict",
            ),
            SliceError::Limit => (
                StatusCode::PAYLOAD_TOO_LARGE,
                "limit_exceeded",
                "context-slice limit exceeded",
            ),
            SliceError::Checksum | SliceError::Integrity(_) => (
                StatusCode::UNPROCESSABLE_ENTITY,
                "integrity_failure",
                "context-slice integrity verification failed",
            ),
            _ => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal_error",
                "context-slice operation failed",
            ),
        };
        (status, Json(ErrorBody { code, message })).into_response()
    }
}

impl axum::extract::FromRequestParts<AppState> for AuthIdentity {
    type Rejection = ApiError;
    fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        _: &AppState,
    ) -> impl std::future::Future<Output = Result<Self, Self::Rejection>> + Send {
        let identity = parts.extensions.get::<Identity>().cloned();
        async move {
            identity
                .map(AuthIdentity)
                .ok_or_else(|| SliceError::Unauthorized.into())
        }
    }
}

fn authentication_configuration() -> Result<AuthenticationConfiguration> {
    match required("NGKG_AUTH_MODE")?.as_str() {
        "opaque" => Ok(AuthenticationConfiguration::Opaque(OpaqueConfiguration {
            token_file: PathBuf::from(required("NGKG_AUTH_TOKEN_FILE")?),
            token_file_sha256: required("NGKG_AUTH_TOKEN_FILE_SHA256")?,
        })),
        "delegation" => Ok(AuthenticationConfiguration::Delegation(Box::new(
            DelegationConfiguration {
                issuer: required("NGKG_AUTH_ISSUER")?,
                audience: required("NGKG_AUTH_AUDIENCE")?,
                jwks_url: Url::parse(&required("NGKG_AUTH_JWKS_URL")?)?,
                allowed_algorithms: comma_set("NGKG_AUTH_ALLOWED_ALGORITHMS")?,
                required_typ: env::var("NGKG_AUTH_REQUIRED_TYP")
                    .unwrap_or_else(|_| "at+jwt".into()),
                maximum_token_lifetime: Duration::from_secs(positive_u64(
                    "NGKG_AUTH_MAX_TOKEN_LIFETIME_SECONDS",
                    300,
                )?),
                clock_skew: Duration::from_secs(positive_u64("NGKG_AUTH_CLOCK_SKEW_SECONDS", 30)?),
                jwks_cache_ttl: Duration::from_secs(positive_u64(
                    "NGKG_AUTH_JWKS_CACHE_TTL_SECONDS",
                    300,
                )?),
                jwks_last_known_good_grace: Duration::from_secs(positive_u64(
                    "NGKG_AUTH_JWKS_LAST_KNOWN_GOOD_SECONDS",
                    300,
                )?),
                connect_timeout: Duration::from_millis(positive_u64(
                    "NGKG_AUTH_CONNECT_TIMEOUT_MS",
                    2000,
                )?),
                request_timeout: Duration::from_millis(positive_u64(
                    "NGKG_AUTH_REQUEST_TIMEOUT_MS",
                    5000,
                )?),
                allow_loopback: false,
            },
        ))),
        _ => anyhow::bail!("NGKG_AUTH_MODE must be opaque or delegation"),
    }
}
fn required(name: &'static str) -> Result<String> {
    env::var(name)
        .with_context(|| format!("{name} is required"))
        .and_then(|v| {
            anyhow::ensure!(!v.is_empty(), "{name} must not be empty");
            Ok(v)
        })
}
fn positive_u64(name: &'static str, default: u64) -> Result<u64> {
    let v = env::var(name)
        .ok()
        .map_or(Ok(default), |v| u64::from_str(&v))?;
    anyhow::ensure!(v > 0, "{name} must be positive");
    Ok(v)
}
fn positive_usize(name: &'static str, default: usize) -> Result<usize> {
    usize::try_from(positive_u64(name, u64::try_from(default)?)?).context("usize conversion")
}
fn positive_u32(name: &'static str, default: u32) -> Result<u32> {
    u32::try_from(positive_u64(name, u64::from(default))?).context("u32 conversion")
}
fn comma_set(name: &'static str) -> Result<BTreeSet<String>> {
    let values: BTreeSet<String> = required(name)?
        .split(',')
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(ToOwned::to_owned)
        .collect();
    anyhow::ensure!(!values.is_empty(), "{name} must not be empty");
    Ok(values)
}
