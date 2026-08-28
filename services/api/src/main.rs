//! NGKG authenticated asynchronous control-plane REST API.

use std::{
    collections::{BTreeMap, BTreeSet},
    env,
    fs::File,
    io::BufReader,
    net::SocketAddr,
    path::PathBuf,
    sync::{Arc, OnceLock},
};

use anyhow::{Context, Result};
use axum::{
    body::Body,
    Json, Router,
    extract::{Path, State},
    http::{
        HeaderMap, HeaderValue, StatusCode,
        header::{CONTENT_LENGTH, CONTENT_TYPE},
    },
    response::{IntoResponse, Response},
    routing::{get, post, put},
};
use futures::StreamExt;
use kube::{
    Api, Client,
    api::{ListParams, ObjectMeta, PostParams},
};
use ngkg_artifact_store::{ArtifactStore, ArtifactStoreError};
use ngkg_catalog::{
    CatalogError, CompilationOperation, CreateCompilation, DatasetRecord, OperationRepository,
};
use ngkg_kube::{
    CloudObjectProvider, CloudObjectVersionPolicy, NgkgCompilation, NgkgCompilationSpec,
    NgkgSourceImport, NgkgSourceImportSpec, NgkgSourceImportStatus, NgkgStorageRecovery,
    NgkgStorageRecoverySpec, NgkgStorageRecoveryStatus, StorageRecoveryKind,
};
use ngkg_storage_recovery::{
    RecoveryPlan, RegisterStorageOperation, SnapshotBackupManifest,
    SnapshotStorageManifest, StorageOperationKind, StorageRecoveryRepository, StorageTarget,
    TransferReason, build_recovery_plan, build_restore_plan, derive_operation_id,
    discover_artifact_closure, validate_backup_manifest,
};
use ngkg_types::PublicationPolicy;
use oxigraph::{
    io::{RdfFormat, RdfParser},
    model::GraphName,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::postgres::PgPoolOptions;
use tokio::io::AsyncWriteExt;
use tokio::sync::{Semaphore, TryAcquireError};
use tower_http::trace::TraceLayer;
use utoipa_swagger_ui::{Config as SwaggerConfig, serve as serve_swagger_ui};
use uuid::Uuid;

mod auth;

use auth::{AuthError, Identity, TokenAuthorizer};

#[derive(Clone)]
struct ApiState {
    catalog: Arc<OperationRepository>,
    storage_catalog: Arc<StorageRecoveryRepository>,
    authorizer: Arc<TokenAuthorizer>,
    compilations: Api<NgkgCompilation>,
    source_imports: Api<NgkgSourceImport>,
    storage_recoveries: Api<NgkgStorageRecovery>,
    kube_client: Client,
    allowed_resource_profiles: Arc<BTreeSet<String>>,
    artifact_store: Arc<ArtifactStore>,
    source_upload: Arc<SourceUploadConfig>,
    source_upload_slots: Arc<Semaphore>,
    storage_recovery: Arc<StorageRecoveryApiConfig>,
}

#[derive(Debug)]
struct SourceUploadConfig {
    scratch_root: PathBuf,
    object_prefix: String,
    max_bytes: u64,
    max_quads: u64,
    max_named_graphs: usize,
    single_put_max_bytes: u64,
    multipart_buffer_bytes: usize,
    multipart_concurrency: usize,
}

#[derive(Debug)]
struct StorageRecoveryApiConfig {
    scratch_root: PathBuf,
    source_target: String,
    targets: Vec<StorageTarget>,
    max_manifest_bytes: u64,
    max_artifact_bytes: u64,
    max_artifacts: usize,
    max_task_bytes: u64,
    max_parallelism: u32,
    max_in_flight_bytes: u64,
    resource_profile: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct StorageTargetRegistry {
    format_version: u32,
    targets: Vec<StorageTarget>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct NamedGraphSummary {
    graph_iri: String,
    parsed_quad_count: u64,
}

#[derive(Clone, Debug, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct TrigUploadMetadata {
    format_version: u32,
    tenant_id: Uuid,
    dataset_id: Uuid,
    source_id: Uuid,
    source_sha256: String,
    source_bytes: u64,
    parsed_quad_count: u64,
    default_graph_quad_count: u64,
    named_graphs: Vec<NamedGraphSummary>,
}

#[derive(Debug, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct TrigUploadResponse {
    source_id: Uuid,
    dataset_id: Uuid,
    object_key: String,
    sha256: String,
    bytes: u64,
    parsed_quad_count: u64,
    default_graph_quad_count: u64,
    named_graphs: Vec<NamedGraphSummary>,
    metadata_object_key: String,
    metadata_sha256: String,
}

#[derive(Debug)]
struct TrigScan {
    parsed_quad_count: u64,
    default_graph_quad_count: u64,
    named_graphs: Vec<NamedGraphSummary>,
}

const SOURCE_UPLOAD_SCRATCH_MARKER: &str = ".ngkg-source-upload-scratch-v1";
const SOURCE_UPLOAD_SCRATCH_MARKER_BYTES: &[u8] = b"ngkg-source-upload-scratch-v1\n";

struct ScratchLease {
    path: PathBuf,
}

impl Drop for ScratchLease {
    fn drop(&mut self) {
        if let Err(error) = std::fs::remove_file(&self.path)
            && error.kind() != std::io::ErrorKind::NotFound
        {
            tracing::error!(path = %self.path.display(), %error, "source upload scratch cleanup failed");
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct CreateDatasetRequest {
    identity_namespace: Uuid,
    policy_version: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct CreateNamedDatasetRequest {
    name: String,
    policy_version: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct CreateIngestionRequest {
    bundle_object_key: String,
    bundle_sha256: String,
    parent_snapshot_id: Option<Uuid>,
    target_snapshot_id: Option<Uuid>,
    publication_policy: PublicationPolicy,
    resource_profile: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct CreateCloudImportRequest {
    provider: CloudObjectProvider,
    bucket: String,
    account_name: Option<String>,
    prefix: Option<String>,
    #[serde(default)]
    object_keys: Vec<String>,
    #[serde(default = "default_trig_include_patterns")]
    include_patterns: Vec<String>,
    #[serde(default = "mandatory_excluded_graph_roles")]
    exclude_segments: Vec<String>,
    identity_ref: String,
    version_policy: CloudObjectVersionPolicy,
    target_snapshot_id: Uuid,
    parent_snapshot_id: Option<Uuid>,
    publication_policy: PublicationPolicy,
    resource_profile: String,
    max_source_bytes: u64,
    max_source_objects: u32,
    logical_partitions: u32,
    ontology_qualification_request_object_key: String,
    ontology_qualification_request_sha256: String,
}

#[derive(Debug, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct CloudImportAccepted {
    operation_id: Uuid,
    target_snapshot_id: Uuid,
    state: &'static str,
}

#[derive(Debug, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct CloudImportStatusResponse {
    operation_id: Uuid,
    dataset_id: Uuid,
    target_snapshot_id: Uuid,
    status: NgkgSourceImportStatus,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct OperationAccepted {
    operation_id: Uuid,
    state: ngkg_catalog::JobState,
    revision: i64,
    target_snapshot_id: Uuid,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct JobResponse {
    operation: ngkg_catalog::Operation,
    bundle_object_key: String,
    bundle_sha256: String,
    parent_snapshot_id: Option<Uuid>,
    publication_policy: PublicationPolicy,
    resource_profile: String,
    distributed_build: Option<ngkg_catalog::DistributedPlanSummary>,
    distributed_artifacts: Option<ngkg_catalog::ArtifactPlanSummary>,
    distributed_artifact_root: Option<ngkg_catalog::DistributedArtifactRoot>,
    distributed_serving_root: Option<ngkg_catalog::DistributedServingRoot>,
    distributed_serving_certification: Option<ngkg_catalog::ServingCertification>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct CancelRequest {
    expected_revision: i64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct PublishRequest {
    expected_parent_snapshot_id: Option<Uuid>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct CreateStorageOperationRequest {
    kind: StorageRecoveryKind,
    destination_target: Option<String>,
    replication_factor: u16,
    max_parallelism: Option<u32>,
    max_in_flight_bytes: Option<u64>,
    resource_profile: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct CreateRestoreRequest {
    backup_id: Uuid,
    destination_target: String,
    restored_snapshot_id: Option<Uuid>,
    max_parallelism: Option<u32>,
    max_in_flight_bytes: Option<u64>,
    resource_profile: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct StorageOperationAccepted {
    operation_id: Uuid,
    source_snapshot_id: Uuid,
    restored_snapshot_id: Option<Uuid>,
    kind: StorageRecoveryKind,
    task_count: u32,
    state: String,
}

#[derive(Debug, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct StorageOperationStatusResponse {
    operation_id: Uuid,
    dataset_id: Uuid,
    source_snapshot_id: Uuid,
    restored_snapshot_id: Option<Uuid>,
    kind: StorageRecoveryKind,
    state: String,
    error_code: Option<String>,
    status: NgkgStorageRecoveryStatus,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ErrorBody {
    code: &'static str,
    message: String,
}

struct ApiError {
    status: StatusCode,
    code: &'static str,
    message: String,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.status, Json(ErrorBody { code: self.code, message: self.message })).into_response()
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().json().with_env_filter("info").init();
    let database_url = required("NGKG_DATABASE_URL")?;
    let bind: SocketAddr = required("NGKG_BIND_ADDR")?
        .parse()
        .context("NGKG_BIND_ADDR must be a socket address")?;
    let namespace = required("NGKG_WORKLOAD_NAMESPACE")?;
    let token_file = PathBuf::from(required("NGKG_AUTH_TOKENS_FILE")?);
    let authorizer = TokenAuthorizer::load(
        &token_file,
        &required("NGKG_AUTH_TOKENS_FILE_SHA256")?,
    )
    .map_err(anyhow::Error::msg)?;
    let profiles = required("NGKG_ALLOWED_RESOURCE_PROFILES")?
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .collect::<BTreeSet<_>>();
    if profiles.is_empty() {
        anyhow::bail!("NGKG_ALLOWED_RESOURCE_PROFILES must contain at least one profile");
    }
    let artifact_store = Arc::new(ArtifactStore::from_base_url(&required("NGKG_ARTIFACT_BASE_URL")?)?);
    let storage_registry_json = required("NGKG_STORAGE_TARGETS_JSON")?;
    let storage_registry: StorageTargetRegistry = serde_json::from_str(&storage_registry_json)
        .context("NGKG_STORAGE_TARGETS_JSON is invalid")?;
    if storage_registry.format_version != 1 || storage_registry.targets.len() < 2 {
        anyhow::bail!("storage recovery requires at least two registered targets");
    }
    let storage_recovery = Arc::new(StorageRecoveryApiConfig {
        scratch_root: absolute_path("NGKG_STORAGE_RECOVERY_API_SCRATCH_ROOT")?,
        source_target: required("NGKG_PRIMARY_STORAGE_TARGET")?,
        targets: storage_registry.targets,
        max_manifest_bytes: positive_u64("NGKG_STORAGE_RECOVERY_MAX_MANIFEST_BYTES")?,
        max_artifact_bytes: positive_u64("NGKG_STORAGE_RECOVERY_MAX_ARTIFACT_BYTES")?,
        max_artifacts: positive_usize("NGKG_STORAGE_RECOVERY_MAX_ARTIFACTS")?,
        max_task_bytes: positive_u64("NGKG_STORAGE_RECOVERY_MAX_TASK_BYTES")?,
        max_parallelism: positive_u32("NGKG_STORAGE_RECOVERY_MAX_PARALLELISM")?,
        max_in_flight_bytes: positive_u64("NGKG_STORAGE_RECOVERY_MAX_IN_FLIGHT_BYTES")?,
        resource_profile: required("NGKG_STORAGE_RECOVERY_RESOURCE_PROFILE")?,
    });
    let primary_target = storage_recovery
        .targets
        .iter()
        .find(|target| target.name == storage_recovery.source_target)
        .context("NGKG_PRIMARY_STORAGE_TARGET is absent from the target registry")?;
    let primary_store = ArtifactStore::from_base_url(&primary_target.base_url)
        .context("primary storage target URL is invalid")?;
    if primary_store.base_url() != artifact_store.base_url() {
        anyhow::bail!(
            "NGKG_ARTIFACT_BASE_URL must identify the registered primary storage target"
        );
    }
    prepare_storage_recovery_scratch(&storage_recovery.scratch_root)?;
    let source_upload = Arc::new(SourceUploadConfig {
        scratch_root: absolute_path("NGKG_SOURCE_UPLOAD_SCRATCH_ROOT")?,
        object_prefix: required_object_prefix("NGKG_SOURCE_UPLOAD_OBJECT_PREFIX")?,
        max_bytes: positive_u64("NGKG_MAX_TRIG_UPLOAD_BYTES")?,
        max_quads: positive_u64("NGKG_MAX_TRIG_UPLOAD_QUADS")?,
        max_named_graphs: positive_usize("NGKG_MAX_TRIG_NAMED_GRAPHS")?,
        single_put_max_bytes: positive_u64("NGKG_SOURCE_UPLOAD_SINGLE_PUT_MAX_BYTES")?,
        multipart_buffer_bytes: positive_usize("NGKG_SOURCE_UPLOAD_MULTIPART_BUFFER_BYTES")?,
        multipart_concurrency: positive_usize("NGKG_SOURCE_UPLOAD_MULTIPART_CONCURRENCY")?,
    });
    prepare_source_upload_scratch(&source_upload.scratch_root)?;
    let max_source_uploads_in_flight = positive_usize("NGKG_MAX_TRIG_UPLOADS_IN_FLIGHT")?;
    if max_source_uploads_in_flight > Semaphore::MAX_PERMITS {
        anyhow::bail!("NGKG_MAX_TRIG_UPLOADS_IN_FLIGHT exceeds the Tokio semaphore ceiling");
    }
    let pool = PgPoolOptions::new().max_connections(32).connect(&database_url).await?;
    let kube_client = Client::try_default().await?;
    let state = ApiState {
        catalog: Arc::new(OperationRepository::new(pool.clone())),
        storage_catalog: Arc::new(StorageRecoveryRepository::new(pool)),
        authorizer: Arc::new(authorizer),
        compilations: Api::namespaced(kube_client.clone(), &namespace),
        source_imports: Api::namespaced(kube_client.clone(), &namespace),
        storage_recoveries: Api::namespaced(kube_client.clone(), &namespace),
        kube_client,
        allowed_resource_profiles: Arc::new(profiles),
        artifact_store,
        source_upload,
        source_upload_slots: Arc::new(Semaphore::new(max_source_uploads_in_flight)),
        storage_recovery,
    };
    let app = Router::new()
        .route("/health/live", get(|| async { StatusCode::NO_CONTENT }))
        .route("/health/ready", get(ready))
        .route("/openapi.yaml", get(openapi))
        .route("/openapi.json", get(openapi_json))
        .route("/docs", get(swagger_ui_root))
        .route("/docs/{*asset}", get(swagger_ui_asset))
        .route("/v1/datasets", post(create_named_dataset))
        .route("/v1/datasets/{dataset_id}", put(create_dataset))
        .route(
            "/v1/datasets/{dataset_id}/sources/{source_id}",
            put(upload_trig_source),
        )
        .route("/v1/datasets/{dataset_id}/ingestions", post(create_ingestion))
        .route(
            "/v1/datasets/{dataset_name}/imports",
            post(create_cloud_import_by_name),
        )
        .route(
            "/v1/datasets/{dataset_name}/imports/{operation_id}",
            get(get_cloud_import_by_name),
        )
        .route(
            "/v1/datasets/by-id/{dataset_id}/imports",
            post(create_cloud_import),
        )
        .route("/v1/jobs/{operation_id}", get(get_job))
        .route("/v1/jobs/{operation_id}/cancel", post(cancel_job))
        .route(
            "/v1/datasets/{dataset_id}/snapshots/{snapshot_id}",
            get(get_snapshot),
        )
        .route(
            "/v1/datasets/{dataset_id}/snapshots/{snapshot_id}/publish",
            post(publish_snapshot),
        )
        .route(
            "/v1/datasets/{dataset_name}/snapshots/{snapshot_id}/storage-operations",
            post(create_storage_operation),
        )
        .route(
            "/v1/datasets/{dataset_name}/restores",
            post(create_restore),
        )
        .route(
            "/v1/storage-operations/{operation_id}",
            get(get_storage_operation),
        )
        .layer(TraceLayer::new_for_http())
        .with_state(state);
    let listener = tokio::net::TcpListener::bind(bind).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn ready(State(state): State<ApiState>) -> StatusCode {
    let catalog = state.catalog.ready().await;
    let kubernetes = state.kube_client.apiserver_version().await;
    let source_import_crd = state
        .source_imports
        .list(&ListParams::default().limit(1))
        .await;
    let storage_recovery_crd = state
        .storage_recoveries
        .list(&ListParams::default().limit(1))
        .await;
    match (catalog, kubernetes, source_import_crd, storage_recovery_crd) {
        (Ok(()), Ok(_), Ok(_), Ok(_)) => StatusCode::NO_CONTENT,
        (catalog, kubernetes, source_import_crd, storage_recovery_crd) => {
            tracing::error!(
                ?catalog,
                ?kubernetes,
                ?source_import_crd,
                ?storage_recovery_crd,
                "API readiness dependency failed"
            );
            StatusCode::SERVICE_UNAVAILABLE
        }
    }
}

async fn openapi() -> impl IntoResponse {
    (
        [(CONTENT_TYPE, "application/yaml; charset=utf-8")],
        include_str!("../../../api/openapi.yaml"),
    )
}

async fn openapi_json() -> Result<Response, ApiError> {
    let document = serde_yaml::from_str::<serde_json::Value>(include_str!(
        "../../../api/openapi.yaml"
    ))
    .map_err(|error| internal_contract_error("embedded OpenAPI YAML is invalid", error))?;
    let bytes = serde_json::to_vec(&document)
        .map_err(|error| internal_contract_error("OpenAPI JSON serialization failed", error))?;
    Ok(([(CONTENT_TYPE, "application/json; charset=utf-8")], bytes).into_response())
}

static SWAGGER_CONFIG: OnceLock<Arc<SwaggerConfig<'static>>> = OnceLock::new();

fn swagger_config() -> Arc<SwaggerConfig<'static>> {
    Arc::clone(SWAGGER_CONFIG.get_or_init(|| {
        Arc::new(
            SwaggerConfig::from("/openapi.json")
                .display_request_duration(true)
                .persist_authorization(false),
        )
    }))
}

async fn swagger_ui_root() -> Result<Response, ApiError> {
    swagger_ui_response("")
}

async fn swagger_ui_asset(Path(asset): Path<String>) -> Result<Response, ApiError> {
    swagger_ui_response(&asset)
}

fn swagger_ui_response(asset: &str) -> Result<Response, ApiError> {
    let Some(file) = serve_swagger_ui(asset, swagger_config())
        .map_err(|error| internal_contract_error("vendored Swagger UI failed", error))?
    else {
        return Ok(StatusCode::NOT_FOUND.into_response());
    };
    let content_type = HeaderValue::from_str(&file.content_type).map_err(|error| {
        internal_contract_error("vendored Swagger UI returned an invalid content type", error)
    })?;
    let mut response = (StatusCode::OK, file.bytes.into_owned()).into_response();
    response.headers_mut().insert(CONTENT_TYPE, content_type);
    response.headers_mut().insert(
        "content-security-policy",
        HeaderValue::from_static(
            "default-src 'self'; style-src 'self' 'unsafe-inline'; script-src 'self'; img-src 'self' data:; connect-src 'self'; font-src 'self' data:",
        ),
    );
    response.headers_mut().insert(
        "x-content-type-options",
        HeaderValue::from_static("nosniff"),
    );
    Ok(response)
}

async fn create_dataset(
    State(state): State<ApiState>,
    Path(dataset_id): Path<Uuid>,
    headers: HeaderMap,
    Json(request): Json<CreateDatasetRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let identity = authorize(&state, &headers, "datasets:write")?;
    if request.policy_version.is_empty() || request.policy_version.len() > 128 {
        return Err(unprocessable("policyVersion must contain 1..128 characters"));
    }
    state
        .catalog
        .create_dataset(
            identity.tenant_id,
            dataset_id,
            request.identity_namespace,
            &request.policy_version,
        )
        .await
        .map_err(catalog_error)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn create_named_dataset(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(request): Json<CreateNamedDatasetRequest>,
) -> Result<Json<DatasetRecord>, ApiError> {
    let identity = authorize(&state, &headers, "datasets:write")?;
    validate_dataset_name(&request.name)?;
    if request.policy_version.is_empty() || request.policy_version.len() > 128 {
        return Err(unprocessable("policyVersion must contain 1..128 characters"));
    }
    let identity_namespace = Uuid::new_v5(
        &identity.tenant_id,
        format!("ngkg-dataset-identity-v1:{}", request.name).as_bytes(),
    );
    let dataset = state
        .catalog
        .create_or_get_named_dataset(
            identity.tenant_id,
            &request.name,
            identity_namespace,
            &request.policy_version,
        )
        .await
        .map_err(catalog_error)?;
    Ok(Json(dataset))
}

fn validate_dataset_name(value: &str) -> Result<(), ApiError> {
    let valid = (1..=63).contains(&value.len())
        && value
            .bytes()
            .next()
            .is_some_and(|byte| byte.is_ascii_lowercase())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_');
    if !valid {
        return Err(unprocessable(
            "dataset name must match ^[a-z][a-z0-9_]{0,62}$, for example supply_chain",
        ));
    }
    Ok(())
}

async fn upload_trig_source(
    State(state): State<ApiState>,
    Path((dataset_id, source_id)): Path<(Uuid, Uuid)>,
    headers: HeaderMap,
    body: Body,
) -> Result<(StatusCode, Json<TrigUploadResponse>), ApiError> {
    let identity = authorize(&state, &headers, "sources:write")?;
    if dataset_id.is_nil() || source_id.is_nil() {
        return Err(unprocessable("datasetId and sourceId must be non-nil UUIDs"));
    }
    require_trig_content_type(&headers)?;
    if !state
        .catalog
        .dataset_exists(identity.tenant_id, dataset_id)
        .await
        .map_err(catalog_error)?
    {
        return Err(ApiError {
            status: StatusCode::NOT_FOUND,
            code: "DATASET_NOT_FOUND",
            message: "the tenant-scoped dataset does not exist".to_owned(),
        });
    }
    let expected_sha256 = content_sha256(&headers)?;
    if let Some(content_length) = content_length(&headers)?
        && content_length > state.source_upload.max_bytes
    {
        return Err(payload_too_large("TriG upload exceeds the configured byte ceiling"));
    }
    let _permit = Arc::clone(&state.source_upload_slots)
        .try_acquire_owned()
        .map_err(|error| match error {
            TryAcquireError::NoPermits => ApiError {
                status: StatusCode::TOO_MANY_REQUESTS,
                code: "SOURCE_UPLOAD_CAPACITY_EXHAUSTED",
                message: "all bounded source-upload lanes are busy; retry after an active upload completes"
                    .to_owned(),
            },
            TryAcquireError::Closed => ApiError {
                status: StatusCode::SERVICE_UNAVAILABLE,
                code: "SOURCE_UPLOAD_UNAVAILABLE",
                message: "source-upload admission is unavailable".to_owned(),
            },
        })?;

    let scratch_path = state
        .source_upload
        .scratch_root
        .join(format!("upload-{}.trig", Uuid::new_v4()));
    let scratch = ScratchLease {
        path: scratch_path.clone(),
    };
    let file = tokio::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&scratch.path)
        .await
        .map_err(upload_io_error)?;
    let mut writer = tokio::io::BufWriter::new(file);
    let mut stream = body.into_data_stream();
    let mut observed_bytes = 0_u64;
    let mut hasher = Sha256::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|error| ApiError {
            status: StatusCode::BAD_REQUEST,
            code: "TRIG_UPLOAD_STREAM_FAILED",
            message: format!("TriG request body terminated before completion: {error}"),
        })?;
        observed_bytes = observed_bytes
            .checked_add(u64::try_from(chunk.len()).map_err(|_| {
                payload_too_large("TriG upload chunk exceeds this platform")
            })?)
            .filter(|bytes| *bytes <= state.source_upload.max_bytes)
            .ok_or_else(|| payload_too_large("TriG upload exceeds the configured byte ceiling"))?;
        hasher.update(&chunk);
        writer.write_all(&chunk).await.map_err(upload_io_error)?;
    }
    writer.flush().await.map_err(upload_io_error)?;
    writer.get_ref().sync_all().await.map_err(upload_io_error)?;
    drop(writer);

    if let Some(content_length) = content_length(&headers)?
        && content_length != observed_bytes
    {
        return Err(ApiError {
            status: StatusCode::BAD_REQUEST,
            code: "TRIG_CONTENT_LENGTH_MISMATCH",
            message: "Content-Length differs from the received TriG byte count".to_owned(),
        });
    }
    let observed_sha256 = hex::encode(hasher.finalize());
    if observed_sha256 != expected_sha256 {
        return Err(ApiError {
            status: StatusCode::UNPROCESSABLE_ENTITY,
            code: "TRIG_SHA256_MISMATCH",
            message: "received TriG bytes do not match X-NGKG-Content-SHA256".to_owned(),
        });
    }

    let scan_path = scratch.path.clone();
    let max_quads = state.source_upload.max_quads;
    let max_named_graphs = state.source_upload.max_named_graphs;
    let scan = tokio::task::spawn_blocking(move || {
        inspect_trig(&scan_path, max_quads, max_named_graphs)
    })
    .await
    .map_err(|error| ApiError {
        status: StatusCode::SERVICE_UNAVAILABLE,
        code: "TRIG_VALIDATION_WORKER_FAILED",
        message: format!("TriG validation worker failed: {error}"),
    })??;

    let object_key = source_object_key(
        &state.source_upload.object_prefix,
        identity.tenant_id,
        dataset_id,
        source_id,
        &expected_sha256,
        "source.trig",
    );
    state
        .artifact_store
        .put_file_immutable(
            &object_key,
            &expected_sha256,
            &scratch.path,
            state.source_upload.single_put_max_bytes,
            state.source_upload.multipart_buffer_bytes,
            state.source_upload.multipart_concurrency,
        )
        .await
        .map_err(artifact_upload_error)?;

    let metadata = TrigUploadMetadata {
        format_version: 1,
        tenant_id: identity.tenant_id,
        dataset_id,
        source_id,
        source_sha256: expected_sha256.clone(),
        source_bytes: observed_bytes,
        parsed_quad_count: scan.parsed_quad_count,
        default_graph_quad_count: scan.default_graph_quad_count,
        named_graphs: scan.named_graphs.clone(),
    };
    let metadata_bytes = serde_json::to_vec_pretty(&metadata).map_err(|error| ApiError {
        status: StatusCode::SERVICE_UNAVAILABLE,
        code: "SOURCE_METADATA_SERIALIZATION_FAILED",
        message: format!("validated source metadata could not be serialized: {error}"),
    })?;
    let metadata_sha256 = hex::encode(Sha256::digest(&metadata_bytes));
    let metadata_path = state
        .source_upload
        .scratch_root
        .join(format!("metadata-{}.json", Uuid::new_v4()));
    let metadata_scratch = ScratchLease {
        path: metadata_path.clone(),
    };
    write_synced_file(&metadata_scratch.path, &metadata_bytes).await?;
    let metadata_object_key = source_object_key(
        &state.source_upload.object_prefix,
        identity.tenant_id,
        dataset_id,
        source_id,
        &expected_sha256,
        "source-metadata.json",
    );
    state
        .artifact_store
        .put_file_immutable(
            &metadata_object_key,
            &metadata_sha256,
            &metadata_scratch.path,
            state.source_upload.single_put_max_bytes,
            state.source_upload.multipart_buffer_bytes,
            state.source_upload.multipart_concurrency,
        )
        .await
        .map_err(artifact_upload_error)?;

    Ok((
        StatusCode::CREATED,
        Json(TrigUploadResponse {
            source_id,
            dataset_id,
            object_key,
            sha256: expected_sha256,
            bytes: observed_bytes,
            parsed_quad_count: scan.parsed_quad_count,
            default_graph_quad_count: scan.default_graph_quad_count,
            named_graphs: scan.named_graphs,
            metadata_object_key,
            metadata_sha256,
        }),
    ))
}

fn inspect_trig(
    path: &std::path::Path,
    max_quads: u64,
    max_named_graphs: usize,
) -> Result<TrigScan, ApiError> {
    let input = BufReader::new(File::open(path).map_err(upload_io_error)?);
    let mut parsed_quad_count = 0_u64;
    let mut default_graph_quad_count = 0_u64;
    let mut named_graph_counts = BTreeMap::<String, u64>::new();
    for parsed in RdfParser::from_format(RdfFormat::TriG).for_reader(input) {
        let quad = parsed.map_err(|error| ApiError {
            status: StatusCode::UNPROCESSABLE_ENTITY,
            code: "INVALID_TRIG",
            message: format!("TriG parser rejected the RDF dataset: {error}"),
        })?;
        parsed_quad_count = parsed_quad_count
            .checked_add(1)
            .filter(|count| *count <= max_quads)
            .ok_or_else(|| payload_too_large("TriG upload exceeds the configured quad ceiling"))?;
        match quad.graph_name {
            GraphName::DefaultGraph => {
                default_graph_quad_count = default_graph_quad_count
                    .checked_add(1)
                    .ok_or_else(|| payload_too_large("default-graph quad count overflow"))?;
            }
            GraphName::NamedNode(graph) => {
                if !named_graph_counts.contains_key(graph.as_str())
                    && named_graph_counts.len() >= max_named_graphs
                {
                    return Err(payload_too_large(
                        "TriG upload exceeds the configured named-graph ceiling",
                    ));
                }
                let count = named_graph_counts.entry(graph.as_str().to_owned()).or_default();
                *count = count
                    .checked_add(1)
                    .ok_or_else(|| payload_too_large("named-graph quad count overflow"))?;
            }
            GraphName::BlankNode(_) => {
                return Err(ApiError {
                    status: StatusCode::UNPROCESSABLE_ENTITY,
                    code: "BLANK_GRAPH_NAME_REJECTED",
                    message: "NGKG subdomain graphs must use absolute IRI graph names".to_owned(),
                });
            }
        }
    }
    if named_graph_counts.is_empty() {
        return Err(ApiError {
            status: StatusCode::UNPROCESSABLE_ENTITY,
            code: "NAMED_SUBDOMAIN_GRAPH_REQUIRED",
            message: "NGKG source TriG must declare at least one IRI-named subdomain graph"
                .to_owned(),
        });
    }
    let named_graphs = named_graph_counts
        .into_iter()
        .map(|(graph_iri, parsed_quad_count)| NamedGraphSummary {
            graph_iri,
            parsed_quad_count,
        })
        .collect();
    Ok(TrigScan {
        parsed_quad_count,
        default_graph_quad_count,
        named_graphs,
    })
}

fn require_trig_content_type(headers: &HeaderMap) -> Result<(), ApiError> {
    let value = headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| ApiError {
            status: StatusCode::UNSUPPORTED_MEDIA_TYPE,
            code: "TRIG_CONTENT_TYPE_REQUIRED",
            message: "Content-Type application/trig is required".to_owned(),
        })?;
    let mut parts = value.split(';');
    if !parts
        .next()
        .is_some_and(|media_type| media_type.trim().eq_ignore_ascii_case("application/trig"))
    {
        return Err(ApiError {
            status: StatusCode::UNSUPPORTED_MEDIA_TYPE,
            code: "UNSUPPORTED_SOURCE_MEDIA_TYPE",
            message: "source uploads accept only application/trig".to_owned(),
        });
    }
    for parameter in parts {
        let Some((name, value)) = parameter.trim().split_once('=') else {
            return Err(ApiError {
                status: StatusCode::UNSUPPORTED_MEDIA_TYPE,
                code: "INVALID_TRIG_CONTENT_TYPE",
                message: "application/trig parameters must use name=value syntax".to_owned(),
            });
        };
        if !name.trim().eq_ignore_ascii_case("charset")
            || !value.trim().trim_matches('"').eq_ignore_ascii_case("utf-8")
        {
            return Err(ApiError {
                status: StatusCode::UNSUPPORTED_MEDIA_TYPE,
                code: "INVALID_TRIG_CHARSET",
                message: "application/trig source uploads must be UTF-8".to_owned(),
            });
        }
    }
    Ok(())
}

fn content_sha256(headers: &HeaderMap) -> Result<String, ApiError> {
    let value = headers
        .get("x-ngkg-content-sha256")
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| unprocessable("X-NGKG-Content-SHA256 is required"))?;
    if value.len() != 64
        || value
            .bytes()
            .any(|byte| !byte.is_ascii_hexdigit() || byte.is_ascii_uppercase())
    {
        return Err(unprocessable(
            "X-NGKG-Content-SHA256 must be 64 lowercase hexadecimal characters",
        ));
    }
    Ok(value.to_owned())
}

fn content_length(headers: &HeaderMap) -> Result<Option<u64>, ApiError> {
    headers
        .get(CONTENT_LENGTH)
        .map(|value| {
            value
                .to_str()
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .ok_or_else(|| ApiError {
                    status: StatusCode::BAD_REQUEST,
                    code: "INVALID_CONTENT_LENGTH",
                    message: "Content-Length must be an unsigned decimal byte count".to_owned(),
                })
        })
        .transpose()
}

fn source_object_key(
    prefix: &str,
    tenant_id: Uuid,
    dataset_id: Uuid,
    source_id: Uuid,
    sha256: &str,
    file_name: &str,
) -> String {
    format!(
        "{prefix}/{tenant_id}/{dataset_id}/{source_id}/{sha256}/{file_name}"
    )
}

async fn write_synced_file(path: &std::path::Path, bytes: &[u8]) -> Result<(), ApiError> {
    let file = tokio::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)
        .await
        .map_err(upload_io_error)?;
    let mut writer = tokio::io::BufWriter::new(file);
    writer.write_all(bytes).await.map_err(upload_io_error)?;
    writer.flush().await.map_err(upload_io_error)?;
    writer.get_ref().sync_all().await.map_err(upload_io_error)?;
    Ok(())
}

async fn create_ingestion(
    State(state): State<ApiState>,
    Path(dataset_id): Path<Uuid>,
    headers: HeaderMap,
    Json(request): Json<CreateIngestionRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let identity = authorize(&state, &headers, "ingestions:create")?;
    let key = idempotency_key(&headers)?;
    let bundle_sha256 = decode_sha256(&request.bundle_sha256)?;
    validate_object_key(&request.bundle_object_key)?;
    if request.parent_snapshot_id == Some(request.target_snapshot_id) {
        return Err(unprocessable(
            "targetSnapshotId must differ from parentSnapshotId",
        ));
    }
    if !state.allowed_resource_profiles.contains(&request.resource_profile) {
        return Err(unprocessable("resourceProfile is not enabled by the operator"));
    }
    let request_bytes = serde_json::to_vec(&request)
        .map_err(|_| unprocessable("request cannot be canonicalized"))?;
    let request_hash = *blake3::hash(&request_bytes).as_bytes();
    let durable_request = CreateCompilation {
        bundle_object_key: request.bundle_object_key.clone(),
        bundle_sha256,
        parent_snapshot_id: request.parent_snapshot_id,
        target_snapshot_id: request.target_snapshot_id,
        publication_policy: request.publication_policy,
        resource_profile: request.resource_profile.clone(),
    };
    let operation = state
        .catalog
        .create_or_get_compilation(
            identity.tenant_id,
            dataset_id,
            key,
            &request_hash,
            &durable_request,
            &identity.principal_id,
        )
        .await
        .map_err(catalog_error)?;
    ensure_compilation_resource(&state, &operation).await?;
    Ok((
        StatusCode::ACCEPTED,
        Json(OperationAccepted {
            operation_id: operation.operation.operation_id,
            state: operation.operation.state,
            revision: operation.operation.revision,
            target_snapshot_id: operation.operation.target_snapshot_id,
        }),
    ))
}

fn default_trig_include_patterns() -> Vec<String> {
    vec!["**/*.trig".to_owned()]
}

fn mandatory_excluded_graph_roles() -> Vec<String> {
    vec![
        "alignment".to_owned(),
        "closure".to_owned(),
        "provenance".to_owned(),
    ]
}

async fn create_cloud_import_by_name(
    State(state): State<ApiState>,
    Path(dataset_name): Path<String>,
    headers: HeaderMap,
    Json(request): Json<CreateCloudImportRequest>,
) -> Result<(StatusCode, Json<CloudImportAccepted>), ApiError> {
    validate_dataset_name(&dataset_name)?;
    let identity = authorize(&state, &headers, "imports:create")?;
    let dataset_id = state
        .catalog
        .resolve_dataset_name(identity.tenant_id, &dataset_name)
        .await
        .map_err(catalog_error)?;
    create_cloud_import(
        State(state),
        Path(dataset_id),
        headers,
        Json(request),
    )
    .await
}

async fn get_cloud_import_by_name(
    State(state): State<ApiState>,
    Path((dataset_name, operation_id)): Path<(String, Uuid)>,
    headers: HeaderMap,
) -> Result<Json<CloudImportStatusResponse>, ApiError> {
    validate_dataset_name(&dataset_name)?;
    let identity = authorize(&state, &headers, "imports:read")?;
    let dataset_id = state
        .catalog
        .resolve_dataset_name(identity.tenant_id, &dataset_name)
        .await
        .map_err(catalog_error)?;
    let name = format!("ngkg-import-{}", operation_id.simple());
    let resource = state
        .source_imports
        .get_opt(&name)
        .await
        .map_err(kubernetes_api_error)?
        .ok_or_else(|| ApiError {
            status: StatusCode::NOT_FOUND,
            code: "IMPORT_NOT_FOUND",
            message: "the tenant-scoped cloud import does not exist".to_owned(),
        })?;
    if resource.spec.tenant_id != identity.tenant_id
        || resource.spec.dataset_id != dataset_id
        || resource.spec.operation_id != operation_id
    {
        return Err(ApiError {
            status: StatusCode::NOT_FOUND,
            code: "IMPORT_NOT_FOUND",
            message: "the tenant-scoped cloud import does not exist".to_owned(),
        });
    }
    Ok(Json(CloudImportStatusResponse {
        operation_id,
        dataset_id,
        target_snapshot_id: resource.spec.target_snapshot_id,
        status: resource.status.unwrap_or_default(),
    }))
}

async fn create_cloud_import(
    State(state): State<ApiState>,
    Path(dataset_id): Path<Uuid>,
    headers: HeaderMap,
    Json(request): Json<CreateCloudImportRequest>,
) -> Result<(StatusCode, Json<CloudImportAccepted>), ApiError> {
    let identity = authorize(&state, &headers, "imports:create")?;
    let key = idempotency_key(&headers)?;
    if dataset_id.is_nil()
        || !state
            .catalog
            .dataset_exists(identity.tenant_id, dataset_id)
            .await
            .map_err(catalog_error)?
    {
        return Err(ApiError {
            status: StatusCode::NOT_FOUND,
            code: "DATASET_NOT_FOUND",
            message: "the tenant-scoped dataset does not exist".to_owned(),
        });
    }
    validate_cloud_import_request(&request, &state.allowed_resource_profiles)?;

    let request_bytes = serde_json::to_vec(&request)
        .map_err(|_| unprocessable("cloud import request cannot be canonicalized"))?;
    let request_hash = *blake3::hash(&request_bytes).as_bytes();
    let qualification_request_sha256 = decode_sha256(
        &request.ontology_qualification_request_sha256,
    )?;
    let mut operation_name = Vec::with_capacity(256);
    operation_name.extend_from_slice(identity.tenant_id.as_bytes());
    operation_name.extend_from_slice(dataset_id.as_bytes());
    operation_name.extend_from_slice(key.as_bytes());
    let deterministic_request_id = Uuid::new_v5(&dataset_id, &operation_name);
    // Phase 40.13.10 used `Uuid::new_v5(&operation_id, b"ngkg-cloud-import-target-snapshot-v1")`.
    // The request-stable namespace is retained while PostgreSQL now owns operation identity.
    let target_snapshot_id = request.target_snapshot_id.unwrap_or_else(|| {
        Uuid::new_v5(&deterministic_request_id, b"ngkg-cloud-import-target-snapshot-v1")
    });
    let durable = state
        .catalog
        .create_or_get_compilation_with_operation_id(
            identity.tenant_id,
            dataset_id,
            deterministic_request_id,
            key,
            &request_hash,
            &CreateCompilation {
                bundle_object_key: request.ontology_qualification_request_object_key.clone(),
                bundle_sha256: qualification_request_sha256,
                parent_snapshot_id: request.parent_snapshot_id,
                target_snapshot_id,
                publication_policy: request.publication_policy,
                resource_profile: request.resource_profile.clone(),
            },
            &identity.principal_id,
        )
        .await
        .map_err(catalog_error)?;
    let operation_id = durable.operation.operation_id;
    let spec = NgkgSourceImportSpec {
        tenant_id: identity.tenant_id,
        dataset_id,
        operation_id,
        provider: request.provider,
        bucket: request.bucket,
        account_name: request.account_name,
        prefix: request.prefix,
        object_keys: request.object_keys,
        include_patterns: request.include_patterns,
        exclude_segments: request.exclude_segments,
        identity_ref: request.identity_ref,
        version_policy: request.version_policy,
        target_snapshot_id,
        parent_snapshot_id: request.parent_snapshot_id,
        publication_policy: request.publication_policy,
        resource_profile: request.resource_profile,
        max_source_bytes: request.max_source_bytes,
        max_source_objects: request.max_source_objects,
        logical_partitions: request.logical_partitions,
        ontology_qualification_request_object_key: request.ontology_qualification_request_object_key,
        ontology_qualification_request_sha256: request.ontology_qualification_request_sha256,
    };
    let name = format!("ngkg-import-{}", operation_id.simple());
    if let Some(existing) = state
        .source_imports
        .get_opt(&name)
        .await
        .map_err(kubernetes_api_error)?
    {
        if existing.spec != spec {
            return Err(ApiError {
                status: StatusCode::CONFLICT,
                code: "IDEMPOTENCY_CONFLICT",
                message: "Idempotency-Key is already bound to a different cloud import"
                    .to_owned(),
            });
        }
    } else {
        let resource = NgkgSourceImport::new(
            &name,
            spec,
        );
        match state
            .source_imports
            .create(&PostParams::default(), &resource)
            .await
        {
            Ok(_) => {}
            Err(kube::Error::Api(response)) if response.code == 409 => {}
            Err(error) => return Err(kubernetes_api_error(error)),
        }
    }
    Ok((
        StatusCode::ACCEPTED,
        Json(CloudImportAccepted {
            operation_id,
            target_snapshot_id,
            state: "SOURCE_DISCOVERY_PENDING",
        }),
    ))
}

fn validate_cloud_import_request(
    request: &CreateCloudImportRequest,
    profiles: &BTreeSet<String>,
) -> Result<(), ApiError> {
    if request.version_policy != CloudObjectVersionPolicy::RequireImmutableChecksum {
        return Err(unprocessable(
            "Phase 40.13.10 accepts require-immutable-checksum only; provider-version proof is not yet implemented",
        ));
    }
    if request.target_snapshot_id.is_some_and(|target| target.is_nil())
        || request
            .target_snapshot_id
            .is_some_and(|target| request.parent_snapshot_id == Some(target))
    {
        return Err(unprocessable(
            "when supplied, targetSnapshotId must be non-nil and differ from parentSnapshotId",
        ));
    }
    if !profiles.contains(&request.resource_profile) {
        return Err(unprocessable("resourceProfile is not enabled by the operator"));
    }
    validate_dns_label(&request.identity_ref, "identityRef")?;
    validate_bucket_name(&request.bucket)?;
    match request.provider {
        CloudObjectProvider::AzureBlob => {
            let account = request.account_name.as_deref().ok_or_else(|| {
                unprocessable("accountName is required for provider azure-blob")
            })?;
            validate_dns_label(account, "accountName")?;
        }
        CloudObjectProvider::AwsS3 | CloudObjectProvider::Gcs => {
            if request.account_name.is_some() {
                return Err(unprocessable(
                    "accountName is permitted only for provider azure-blob",
                ));
            }
        }
    }
    if request.object_keys.is_empty() == request.prefix.is_none() {
        return Err(unprocessable(
            "provide either non-empty objectKeys or one prefix, but not both",
        ));
    }
    if let Some(prefix) = &request.prefix {
        validate_cloud_prefix(prefix)?;
    }
    if request.object_keys.len()
        > usize::try_from(request.max_source_objects).unwrap_or(usize::MAX)
    {
        return Err(unprocessable("objectKeys exceeds maxSourceObjects"));
    }
    let mut keys = BTreeSet::new();
    for key in &request.object_keys {
        validate_object_key(key)?;
        if !key.to_ascii_lowercase().ends_with(".trig") || !keys.insert(key) {
            return Err(unprocessable(
                "objectKeys must be unique normalized paths ending in .trig",
            ));
        }
    }
    if request.include_patterns != ["**/*.trig"] {
        return Err(unprocessable(
            "includePatterns currently permits only the deterministic **/*.trig pattern",
        ));
    }
    let excluded = request
        .exclude_segments
        .iter()
        .map(|value| value.to_ascii_lowercase())
        .collect::<BTreeSet<_>>();
    if !["alignment", "closure", "provenance"]
        .iter()
        .all(|required| excluded.contains(*required))
        || excluded.contains("semkg")
    {
        return Err(unprocessable(
            "excludeSegments must include alignment, closure, and provenance and must not exclude semkg",
        ));
    }
    if request.max_source_bytes == 0
        || request.max_source_bytes > (1_u64 << 50)
        || request.max_source_objects == 0
        || request.max_source_objects > 1_000_000
        || !(1..=65_536).contains(&request.logical_partitions)
    {
        return Err(unprocessable(
            "cloud import ceilings exceed the platform contract",
        ));
    }
    validate_object_key(&request.ontology_qualification_request_object_key)?;
    if request.ontology_qualification_request_sha256.len() != 64
        || !request.ontology_qualification_request_sha256.bytes().all(|byte| {
            byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase()
        })
    {
        return Err(unprocessable(
            "ontologyQualificationRequestSha256 must be lowercase SHA-256",
        ));
    }
    Ok(())
}

fn validate_bucket_name(value: &str) -> Result<(), ApiError> {
    if !(3..=255).contains(&value.len())
        || value.starts_with(['.', '-'])
        || value.ends_with(['.', '-'])
        || value.bytes().any(|byte| {
            !(byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_'))
        })
    {
        return Err(unprocessable("bucket must be a normalized cloud bucket/container name"));
    }
    Ok(())
}

fn validate_dns_label(value: &str, field: &str) -> Result<(), ApiError> {
    let valid = (1..=63).contains(&value.len())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        && value
            .as_bytes()
            .first()
            .is_some_and(u8::is_ascii_alphanumeric)
        && value
            .as_bytes()
            .last()
            .is_some_and(u8::is_ascii_alphanumeric);
    if !valid {
        return Err(unprocessable(&format!(
            "{field} must be a lowercase Kubernetes DNS label"
        )));
    }
    Ok(())
}

fn validate_cloud_prefix(value: &str) -> Result<(), ApiError> {
    if value.is_empty()
        || value.len() > 1024
        || value.starts_with('/')
        || value.contains('\\')
        || value.contains("//")
        || value.split('/').any(|segment| segment == "." || segment == "..")
    {
        return Err(unprocessable("prefix must be a normalized relative object prefix"));
    }
    Ok(())
}

fn kubernetes_api_error(error: kube::Error) -> ApiError {
    tracing::error!(%error, "Kubernetes desired-state operation failed");
    ApiError {
        status: StatusCode::SERVICE_UNAVAILABLE,
        code: "KUBERNETES_UNAVAILABLE",
        message: "Kubernetes desired-state dependency is unavailable".to_owned(),
    }
}

async fn get_job(
    State(state): State<ApiState>,
    Path(operation_id): Path<Uuid>,
    headers: HeaderMap,
) -> Result<Json<JobResponse>, ApiError> {
    let identity = authorize(&state, &headers, "jobs:read")?;
    let value = state
        .catalog
        .get_compilation(identity.tenant_id, operation_id)
        .await
        .map_err(catalog_error)?;
    let distributed_build = match state
        .catalog
        .get_distributed_plan(identity.tenant_id, operation_id)
        .await
    {
        Ok(summary) => Some(summary),
        Err(CatalogError::NotFound) => None,
        Err(error) => return Err(catalog_error(error)),
    };
    let distributed_artifacts = match state
        .catalog
        .get_artifact_plan(identity.tenant_id, operation_id)
        .await
    {
        Ok(summary) => Some(summary),
        Err(CatalogError::NotFound) => None,
        Err(error) => return Err(catalog_error(error)),
    };
    let distributed_artifact_root = match state
        .catalog
        .get_artifact_root(identity.tenant_id, operation_id)
        .await
    {
        Ok(root) => Some(root),
        Err(CatalogError::NotFound) => None,
        Err(error) => return Err(catalog_error(error)),
    };
    let distributed_serving_root = match state
        .catalog
        .get_serving_root(identity.tenant_id, operation_id)
        .await
    {
        Ok(root) => Some(root),
        Err(CatalogError::NotFound) => None,
        Err(error) => return Err(catalog_error(error)),
    };
    let distributed_serving_certification = match state
        .catalog
        .get_serving_certification(identity.tenant_id, operation_id)
        .await
    {
        Ok(certification) => Some(certification),
        Err(CatalogError::NotFound) => None,
        Err(error) => return Err(catalog_error(error)),
    };
    Ok(Json(job_response(
        value,
        distributed_build,
        distributed_artifacts,
        distributed_artifact_root,
        distributed_serving_root,
        distributed_serving_certification,
    )))
}

async fn cancel_job(
    State(state): State<ApiState>,
    Path(operation_id): Path<Uuid>,
    headers: HeaderMap,
    Json(request): Json<CancelRequest>,
) -> Result<Json<ngkg_catalog::Operation>, ApiError> {
    let identity = authorize(&state, &headers, "jobs:cancel")?;
    if request.expected_revision < 0 {
        return Err(unprocessable("expectedRevision must be non-negative"));
    }
    let operation = state
        .catalog
        .cancel(
            identity.tenant_id,
            operation_id,
            request.expected_revision,
            &identity.principal_id,
        )
        .await
        .map_err(catalog_error)?;
    Ok(Json(operation))
}

async fn create_storage_operation(
    State(state): State<ApiState>,
    Path((dataset_name, snapshot_id)): Path<(String, Uuid)>,
    headers: HeaderMap,
    Json(request): Json<CreateStorageOperationRequest>,
) -> Result<(StatusCode, Json<StorageOperationAccepted>), ApiError> {
    validate_dataset_name(&dataset_name)?;
    let identity = authorize(&state, &headers, "storage:write")?;
    let idempotency_key = idempotency_key(&headers)?;
    if snapshot_id.is_nil() || request.kind == StorageRecoveryKind::Restore {
        return Err(unprocessable(
            "snapshot storage operations require a non-nil snapshot and a non-restore kind",
        ));
    }
    let dataset_id = state
        .catalog
        .resolve_dataset_name(identity.tenant_id, &dataset_name)
        .await
        .map_err(catalog_error)?;
    let roots = state
        .catalog
        .get_snapshot_recovery_roots(identity.tenant_id, dataset_id, snapshot_id)
        .await
        .map_err(catalog_error)?;
    let (max_parallelism, max_in_flight_bytes, resource_profile) =
        validate_storage_request(&state, request.max_parallelism, request.max_in_flight_bytes, request.resource_profile.as_deref())?;
    let request_bytes = serde_json::to_vec(&request)
        .map_err(|_| unprocessable("storage request cannot be canonicalized"))?;
    let request_digest: [u8; 32] = Sha256::digest(&request_bytes).into();
    let request_sha256 = hex::encode(request_digest);
    let operation_id = derive_operation_id(
        identity.tenant_id,
        dataset_id,
        idempotency_key,
        &request_sha256,
    );
    let (manifest, manifest_sha256) = build_and_publish_storage_manifest(
        &state,
        identity.tenant_id,
        dataset_id,
        &roots,
        operation_id,
    ).await?;
    let (targets, replication_factor, reason) = storage_plan_inputs(&state, &request)?;
    let existing = manifest
        .artifacts
        .iter()
        .map(|artifact| {
            (
                artifact.sha256.clone(),
                BTreeSet::from([state.storage_recovery.source_target.clone()]),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let plan = build_recovery_plan(
        operation_id,
        &manifest,
        &manifest_sha256,
        &state.storage_recovery.source_target,
        &targets,
        &existing,
        replication_factor,
        max_in_flight_bytes,
        reason,
    )
    .map_err(|error| unprocessable(&error.to_string()))?;
    validate_storage_task_sizes(&state, &plan)?;
    let (plan_key, plan_sha256) = publish_recovery_plan(&state, &plan).await?;
    let durable_kind = storage_catalog_kind(request.kind)?;
    let durable = state
        .storage_catalog
        .create_or_get(
            identity.tenant_id,
            idempotency_key,
            &request_digest,
            &RegisterStorageOperation {
                operation_id,
                dataset_id,
                source_snapshot_id: snapshot_id,
                restored_snapshot_id: None,
                kind: durable_kind,
                plan_object_key: plan_key.clone(),
                plan_sha256: decode_sha256(&plan_sha256)?,
                task_count: u32::try_from(plan.tasks.len())
                    .map_err(|_| unprocessable("storage task count exceeds u32"))?,
                max_in_flight_bytes,
            },
        )
        .await
        .map_err(storage_catalog_error)?;
    let primary_target = state.storage_recovery.targets.iter()
        .find(|target| target.name == state.storage_recovery.source_target)
        .ok_or_else(|| unprocessable("primary storage target is not registered"))?;
    state.storage_catalog.register_primary_replicas(
        identity.tenant_id,
        operation_id,
        &manifest,
        primary_target,
    ).await.map_err(storage_catalog_error)?;
    let max_parallelism = cap_storage_parallelism(
        max_parallelism,
        max_in_flight_bytes,
        &plan,
    );
    let spec = NgkgStorageRecoverySpec {
        tenant_id: identity.tenant_id,
        dataset_id,
        operation_id,
        source_snapshot_id: snapshot_id,
        restored_snapshot_id: None,
        kind: request.kind,
        plan_object_key: plan_key,
        plan_sha256,
        task_count: durable.task_count,
        max_parallelism,
        largest_task_bytes: largest_storage_task(&plan),
        max_in_flight_bytes,
        resource_profile,
    };
    ensure_storage_recovery_resource(&state, &spec).await?;
    Ok((
        StatusCode::ACCEPTED,
        Json(StorageOperationAccepted {
            operation_id,
            source_snapshot_id: snapshot_id,
            restored_snapshot_id: None,
            kind: request.kind,
            task_count: durable.task_count,
            state: durable.state,
        }),
    ))
}

async fn create_restore(
    State(state): State<ApiState>,
    Path(dataset_name): Path<String>,
    headers: HeaderMap,
    Json(request): Json<CreateRestoreRequest>,
) -> Result<(StatusCode, Json<StorageOperationAccepted>), ApiError> {
    validate_dataset_name(&dataset_name)?;
    let identity = authorize(&state, &headers, "storage:restore")?;
    let idempotency_key = idempotency_key(&headers)?;
    let dataset_id = state
        .catalog
        .resolve_dataset_name(identity.tenant_id, &dataset_name)
        .await
        .map_err(catalog_error)?;
    let backup = state
        .storage_catalog
        .get_backup(identity.tenant_id, request.backup_id)
        .await
        .map_err(storage_catalog_error)?;
    if backup.dataset_id != dataset_id {
        return Err(ApiError {
            status: StatusCode::NOT_FOUND,
            code: "BACKUP_NOT_FOUND",
            message: "the tenant dataset does not own this backup".to_owned(),
        });
    }
    let (max_parallelism, max_in_flight_bytes, resource_profile) =
        validate_storage_request(&state, request.max_parallelism, request.max_in_flight_bytes, request.resource_profile.as_deref())?;
    if request.destination_target == backup.destination_target
        || !state.storage_recovery.targets.iter().any(|target| {
            target.name == request.destination_target && target.writable
        })
    {
        return Err(unprocessable(
            "restore destination must be a different writable registered target",
        ));
    }
    let request_bytes = serde_json::to_vec(&request)
        .map_err(|_| unprocessable("restore request cannot be canonicalized"))?;
    let request_digest: [u8; 32] = Sha256::digest(&request_bytes).into();
    let operation_id = derive_operation_id(
        identity.tenant_id,
        dataset_id,
        idempotency_key,
        &hex::encode(request_digest),
    );
    let restored_snapshot_id = request.restored_snapshot_id.unwrap_or_else(|| {
        Uuid::new_v5(&operation_id, b"ngkg-storage-restored-snapshot-v1")
    });
    if restored_snapshot_id.is_nil() {
        return Err(unprocessable("restoredSnapshotId must be non-nil"));
    }
    let scratch = recovery_scratch(&state, operation_id)?;
    let backup_path = scratch.join("backup-manifest.json");
    remove_file_if_present(&backup_path).await?;
    state.artifact_store.materialize_verified(
        &backup.backup_manifest_object_key,
        &backup.backup_manifest_sha256,
        state.storage_recovery.max_manifest_bytes,
        &backup_path,
    ).await.map_err(artifact_error)?;
    let backup_manifest: SnapshotBackupManifest = serde_json::from_slice(
        &tokio::fs::read(&backup_path).await.map_err(io_error)?,
    ).map_err(|_| unprocessable("backup manifest is invalid"))?;
    validate_backup_manifest(&backup_manifest)
        .map_err(|error| unprocessable(&error.to_string()))?;
    if backup_manifest.backup_id != request.backup_id
        || backup_manifest.tenant_id != identity.tenant_id
        || backup_manifest.dataset_id != dataset_id
        || backup_manifest.source_snapshot_id != backup.source_snapshot_id
        || backup_manifest.destination_target != backup.destination_target
        || !backup_manifest.complete
    {
        return Err(unprocessable(
            "backup manifest identity differs from its tenant-scoped catalog record",
        ));
    }
    let plan = build_restore_plan(
        operation_id,
        restored_snapshot_id,
        &backup_manifest,
        &backup.backup_manifest_sha256,
        &request.destination_target,
        &state.storage_recovery.targets,
        max_in_flight_bytes,
    ).map_err(|error| unprocessable(&error.to_string()))?;
    validate_storage_task_sizes(&state, &plan)?;
    let (plan_key, plan_sha256) = publish_recovery_plan(&state, &plan).await?;
    let durable = state.storage_catalog.create_or_get(
        identity.tenant_id,
        idempotency_key,
        &request_digest,
        &RegisterStorageOperation {
            operation_id,
            dataset_id,
            source_snapshot_id: backup.source_snapshot_id,
            restored_snapshot_id: Some(restored_snapshot_id),
            kind: StorageOperationKind::Restore,
            plan_object_key: plan_key.clone(),
            plan_sha256: decode_sha256(&plan_sha256)?,
            task_count: u32::try_from(plan.tasks.len())
                .map_err(|_| unprocessable("restore task count exceeds u32"))?,
            max_in_flight_bytes,
        },
    ).await.map_err(storage_catalog_error)?;
    let max_parallelism = cap_storage_parallelism(
        max_parallelism,
        max_in_flight_bytes,
        &plan,
    );
    let spec = NgkgStorageRecoverySpec {
        tenant_id: identity.tenant_id,
        dataset_id,
        operation_id,
        source_snapshot_id: backup.source_snapshot_id,
        restored_snapshot_id: Some(restored_snapshot_id),
        kind: StorageRecoveryKind::Restore,
        plan_object_key: plan_key,
        plan_sha256,
        task_count: durable.task_count,
        max_parallelism,
        largest_task_bytes: largest_storage_task(&plan),
        max_in_flight_bytes,
        resource_profile,
    };
    ensure_storage_recovery_resource(&state, &spec).await?;
    Ok((StatusCode::ACCEPTED, Json(StorageOperationAccepted {
        operation_id,
        source_snapshot_id: backup.source_snapshot_id,
        restored_snapshot_id: Some(restored_snapshot_id),
        kind: StorageRecoveryKind::Restore,
        task_count: durable.task_count,
        state: durable.state,
    })))
}

async fn get_storage_operation(
    State(state): State<ApiState>,
    Path(operation_id): Path<Uuid>,
    headers: HeaderMap,
) -> Result<Json<StorageOperationStatusResponse>, ApiError> {
    let identity = authorize(&state, &headers, "storage:read")?;
    let durable = state
        .storage_catalog
        .get_operation(identity.tenant_id, operation_id)
        .await
        .map_err(storage_catalog_error)?;
    let name = format!("ngkg-storage-{}", operation_id.simple());
    let resource = state.storage_recoveries.get_opt(&name).await.map_err(kubernetes_api_error)?;
    let status = if let Some(resource) = resource {
        if resource.spec.tenant_id != identity.tenant_id
            || resource.spec.operation_id != operation_id
            || resource.spec.dataset_id != durable.dataset_id
            || resource.spec.source_snapshot_id != durable.source_snapshot_id
        {
            return Err(ApiError {
                status: StatusCode::CONFLICT,
                code: "STORAGE_CONTROL_STATE_CONFLICT",
                message: "durable catalog and Kubernetes storage operation differ".to_owned(),
            });
        }
        resource.status.unwrap_or_default()
    } else {
        NgkgStorageRecoveryStatus {
            condition: Some("CatalogDurableKubernetesResourcePending".to_owned()),
            ..NgkgStorageRecoveryStatus::default()
        }
    };
    Ok(Json(StorageOperationStatusResponse {
        operation_id,
        dataset_id: durable.dataset_id,
        source_snapshot_id: durable.source_snapshot_id,
        restored_snapshot_id: durable.restored_snapshot_id,
        kind: recovery_kind_from_catalog(durable.kind),
        state: durable.state,
        error_code: durable.error_code,
        status,
    }))
}

async fn build_and_publish_storage_manifest(
    state: &ApiState,
    tenant_id: Uuid,
    dataset_id: Uuid,
    roots: &ngkg_catalog::SnapshotRecoveryRoots,
    operation_id: Uuid,
) -> Result<(SnapshotStorageManifest, String), ApiError> {
    let scratch = recovery_scratch(state, operation_id)?;
    let snapshot = &roots.snapshot;
    let mut root_objects = vec![(
        snapshot.manifest_object_key.clone(),
        snapshot.manifest_sha256.clone(),
    )];
    if let Some(activation) = &roots.cloud_activation {
        root_objects.extend([
            (activation.activation_manifest_object_key.clone(), activation.activation_manifest_sha256.clone()),
            (activation.semantic_root_object_key.clone(), activation.semantic_root_sha256.clone()),
            (activation.qualification_root_object_key.clone(), activation.qualification_root_sha256.clone()),
            (activation.offline_root_object_key.clone(), activation.offline_root_sha256.clone()),
        ]);
    }
    if let Some(serving) = &roots.serving_root {
        root_objects.push((serving.serving_root_object_key.clone(), serving.serving_root_sha256.clone()));
    }
    let artifacts = discover_artifact_closure(
        &state.artifact_store,
        &root_objects,
        &scratch.join("closure"),
        state.storage_recovery.max_manifest_bytes,
        state.storage_recovery.max_artifact_bytes,
        state.storage_recovery.max_artifacts,
    ).await.map_err(|error| artifact_closure_error(&error.to_string()))?;
    let manifest = SnapshotStorageManifest {
        format_version: 1,
        tenant_id,
        dataset_id,
        snapshot_id: snapshot.snapshot_id,
        snapshot_manifest_sha256: snapshot.manifest_sha256.clone(),
        activation_manifest_sha256: roots.cloud_activation.as_ref()
            .map(|activation| activation.activation_manifest_sha256.clone()),
        artifacts,
    };
    let bytes = serde_json::to_vec(&manifest)
        .map_err(|_| unprocessable("storage manifest cannot be encoded"))?;
    let sha256 = hex::encode(Sha256::digest(&bytes));
    let path = scratch.join("snapshot-storage-manifest.json");
    tokio::fs::write(&path, bytes).await.map_err(io_error)?;
    let key = format!(
        "storage-recovery/{}/snapshot-storage-manifest.json",
        operation_id.simple()
    );
    state.artifact_store.put_file_immutable(
        &key,
        &sha256,
        &path,
        state.source_upload.single_put_max_bytes,
        state.source_upload.multipart_buffer_bytes,
        state.source_upload.multipart_concurrency,
    ).await.map_err(artifact_error)?;
    Ok((manifest, sha256))
}

async fn publish_recovery_plan(
    state: &ApiState,
    plan: &RecoveryPlan,
) -> Result<(String, String), ApiError> {
    let bytes = serde_json::to_vec(plan)
        .map_err(|_| unprocessable("recovery plan cannot be encoded"))?;
    if u64::try_from(bytes.len()).unwrap_or(u64::MAX) > state.storage_recovery.max_manifest_bytes {
        return Err(unprocessable("recovery plan exceeds the operator byte ceiling"));
    }
    let sha256 = hex::encode(Sha256::digest(&bytes));
    let scratch = recovery_scratch(state, plan.operation_id)?;
    let path = scratch.join("recovery-plan.json");
    tokio::fs::write(&path, bytes).await.map_err(io_error)?;
    let key = format!("storage-recovery/{}/recovery-plan.json", plan.operation_id.simple());
    state.artifact_store.put_file_immutable(
        &key,
        &sha256,
        &path,
        state.source_upload.single_put_max_bytes,
        state.source_upload.multipart_buffer_bytes,
        state.source_upload.multipart_concurrency,
    ).await.map_err(artifact_error)?;
    Ok((key, sha256))
}

fn storage_plan_inputs(
    state: &ApiState,
    request: &CreateStorageOperationRequest,
) -> Result<(Vec<StorageTarget>, u16, TransferReason), ApiError> {
    if request.replication_factor == 0 || request.replication_factor > 16 {
        return Err(unprocessable("replicationFactor must be 1..16"));
    }
    let (reason, targets, factor) = match request.kind {
        StorageRecoveryKind::Backup => {
            let destination = request.destination_target.as_deref()
                .ok_or_else(|| unprocessable("backup requires destinationTarget"))?;
            if destination == state.storage_recovery.source_target {
                return Err(unprocessable("backup destination must differ from primary storage"));
            }
            let selected = state.storage_recovery.targets.iter().filter(|target| {
                target.name == state.storage_recovery.source_target || target.name == destination
            }).cloned().collect::<Vec<_>>();
            if selected.len() != 2 || !selected.iter().any(|target| target.name == destination && target.writable) {
                return Err(unprocessable("backup destination is not a writable registered target"));
            }
            (TransferReason::Backup, selected, 2)
        }
        StorageRecoveryKind::Replicate => (
            TransferReason::Replication,
            state.storage_recovery.targets.clone(),
            request.replication_factor,
        ),
        StorageRecoveryKind::Relocate => (
            TransferReason::Relocation,
            {
                let destination = request.destination_target.as_deref()
                    .ok_or_else(|| unprocessable("relocation requires destinationTarget"))?;
                let selected = state.storage_recovery.targets.iter().filter(|target| {
                    target.name == state.storage_recovery.source_target || target.name == destination
                }).cloned().collect::<Vec<_>>();
                if selected.len() != 2 || !selected.iter().any(|target| target.name == destination && target.writable) {
                    return Err(unprocessable("relocation destination is not a writable registered target"));
                }
                selected
            },
            2,
        ),
        StorageRecoveryKind::NodeLoss => (
            TransferReason::NodeLoss,
            state.storage_recovery.targets.clone(),
            request.replication_factor,
        ),
        StorageRecoveryKind::Restore => return Err(unprocessable("use the restore route")),
    };
    Ok((targets, factor, reason))
}

fn validate_storage_request(
    state: &ApiState,
    parallelism: Option<u32>,
    bytes: Option<u64>,
    resource_profile: Option<&str>,
) -> Result<(u32, u64, String), ApiError> {
    let parallelism = parallelism.unwrap_or(state.storage_recovery.max_parallelism);
    let bytes = bytes.unwrap_or(state.storage_recovery.max_in_flight_bytes);
    let profile = resource_profile.unwrap_or(&state.storage_recovery.resource_profile);
    if parallelism == 0
        || parallelism > state.storage_recovery.max_parallelism
        || bytes == 0
        || bytes > state.storage_recovery.max_in_flight_bytes
        || profile != state.storage_recovery.resource_profile
    {
        return Err(unprocessable("storage request exceeds the trusted recovery profile"));
    }
    Ok((parallelism, bytes, profile.to_owned()))
}

fn storage_catalog_kind(kind: StorageRecoveryKind) -> Result<StorageOperationKind, ApiError> {
    match kind {
        StorageRecoveryKind::Replicate => Ok(StorageOperationKind::Replicate),
        StorageRecoveryKind::Relocate => Ok(StorageOperationKind::Relocate),
        StorageRecoveryKind::NodeLoss => Ok(StorageOperationKind::NodeLoss),
        StorageRecoveryKind::Backup => Ok(StorageOperationKind::Backup),
        StorageRecoveryKind::Restore => Err(unprocessable("use the restore route")),
    }
}

const fn recovery_kind_from_catalog(kind: StorageOperationKind) -> StorageRecoveryKind {
    match kind {
        StorageOperationKind::Replicate => StorageRecoveryKind::Replicate,
        StorageOperationKind::Relocate => StorageRecoveryKind::Relocate,
        StorageOperationKind::NodeLoss => StorageRecoveryKind::NodeLoss,
        StorageOperationKind::Backup => StorageRecoveryKind::Backup,
        StorageOperationKind::Restore => StorageRecoveryKind::Restore,
    }
}

async fn ensure_storage_recovery_resource(
    state: &ApiState,
    spec: &NgkgStorageRecoverySpec,
) -> Result<(), ApiError> {
    let name = format!("ngkg-storage-{}", spec.operation_id.simple());
    if let Some(existing) = state.storage_recoveries.get_opt(&name).await.map_err(kubernetes_api_error)? {
        if existing.spec == *spec {
            return Ok(());
        }
        return Err(ApiError {
            status: StatusCode::CONFLICT,
            code: "IDEMPOTENCY_CONFLICT",
            message: "storage operation desired state differs from the durable request".to_owned(),
        });
    }
    let resource = NgkgStorageRecovery::new(&name, spec.clone());
    match state.storage_recoveries.create(&PostParams::default(), &resource).await {
        Ok(_) => Ok(()),
        Err(kube::Error::Api(response)) if response.code == 409 => Ok(()),
        Err(error) => Err(kubernetes_api_error(error)),
    }
}

fn recovery_scratch(state: &ApiState, operation_id: Uuid) -> Result<PathBuf, ApiError> {
    let path = state.storage_recovery.scratch_root.join(format!("storage-{}", operation_id.simple()));
    std::fs::create_dir_all(&path).map_err(io_error)?;
    Ok(path)
}

async fn remove_file_if_present(path: &std::path::Path) -> Result<(), ApiError> {
    match tokio::fs::remove_file(path).await {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(io_error(error)),
    }
}

async fn get_snapshot(
    State(state): State<ApiState>,
    Path((dataset_id, snapshot_id)): Path<(Uuid, Uuid)>,
    headers: HeaderMap,
) -> Result<Json<ngkg_catalog::Snapshot>, ApiError> {
    let identity = authorize(&state, &headers, "snapshots:read")?;
    let snapshot = state
        .catalog
        .get_snapshot(identity.tenant_id, dataset_id, snapshot_id)
        .await
        .map_err(catalog_error)?;
    Ok(Json(snapshot))
}

async fn publish_snapshot(
    State(state): State<ApiState>,
    Path((dataset_id, snapshot_id)): Path<(Uuid, Uuid)>,
    headers: HeaderMap,
    Json(request): Json<PublishRequest>,
) -> Result<Json<ngkg_catalog::Snapshot>, ApiError> {
    let identity = authorize(&state, &headers, "snapshots:publish")?;
    let snapshot = state
        .catalog
        .publish_snapshot(
            identity.tenant_id,
            dataset_id,
            snapshot_id,
            request.expected_parent_snapshot_id,
            &identity.principal_id,
        )
        .await
        .map_err(catalog_error)?;
    Ok(Json(snapshot))
}

async fn ensure_compilation_resource(
    state: &ApiState,
    value: &CompilationOperation,
) -> Result<(), ApiError> {
    let operation = &value.operation;
    let name = format!("ngkg-{}", operation.operation_id.simple());
    let spec = NgkgCompilationSpec {
        tenant_id: operation.tenant_id,
        dataset_id: operation.dataset_id,
        operation_id: operation.operation_id,
        bundle_object_key: value.request.bundle_object_key.clone(),
        bundle_sha256: hex::encode(value.request.bundle_sha256),
        parent_snapshot_id: value.request.parent_snapshot_id,
        target_snapshot_id: value.request.target_snapshot_id,
        publication_policy: value.request.publication_policy,
        resource_profile: value.request.resource_profile.clone(),
    };
    if let Some(existing) = state
        .compilations
        .get_opt(&name)
        .await
        .map_err(kubernetes_error)?
    {
        if existing.spec != spec {
            return Err(ApiError {
                status: StatusCode::CONFLICT,
                code: "KUBERNETES_RESOURCE_CONFLICT",
                message: "durable operation conflicts with the existing compilation resource".to_owned(),
            });
        }
        return Ok(());
    }
    let resource = NgkgCompilation {
        metadata: ObjectMeta {
            name: Some(name.clone()),
            labels: Some(std::collections::BTreeMap::from([
                ("app.kubernetes.io/name".to_owned(), "ngkg".to_owned()),
                ("app.kubernetes.io/component".to_owned(), "compilation".to_owned()),
                ("ngkg.io/operation-id".to_owned(), operation.operation_id.to_string()),
            ])),
            ..ObjectMeta::default()
        },
        spec,
        status: None,
    };
    match state.compilations.create(&PostParams::default(), &resource).await {
        Ok(_) => Ok(()),
        Err(kube::Error::Api(response)) if response.code == 409 => {
            let existing = state.compilations.get(&name).await.map_err(kubernetes_error)?;
            if existing.spec == resource.spec {
                Ok(())
            } else {
                Err(ApiError {
                    status: StatusCode::CONFLICT,
                    code: "KUBERNETES_RESOURCE_CONFLICT",
                    message: "concurrent compilation resource has different immutable content".to_owned(),
                })
            }
        }
        Err(error) => Err(kubernetes_error(error)),
    }
}

fn job_response(
    value: CompilationOperation,
    distributed_build: Option<ngkg_catalog::DistributedPlanSummary>,
    distributed_artifacts: Option<ngkg_catalog::ArtifactPlanSummary>,
    distributed_artifact_root: Option<ngkg_catalog::DistributedArtifactRoot>,
    distributed_serving_root: Option<ngkg_catalog::DistributedServingRoot>,
    distributed_serving_certification: Option<ngkg_catalog::ServingCertification>,
) -> JobResponse {
    JobResponse {
        operation: value.operation,
        bundle_object_key: value.request.bundle_object_key,
        bundle_sha256: hex::encode(value.request.bundle_sha256),
        parent_snapshot_id: value.request.parent_snapshot_id,
        publication_policy: value.request.publication_policy,
        resource_profile: value.request.resource_profile,
        distributed_build,
        distributed_artifacts,
        distributed_artifact_root,
        distributed_serving_root,
        distributed_serving_certification,
    }
}

fn authorize(state: &ApiState, headers: &HeaderMap, scope: &str) -> Result<Identity, ApiError> {
    state.authorizer.authorize(headers, scope).map_err(|error| match error {
        AuthError::Unauthenticated => ApiError {
            status: StatusCode::UNAUTHORIZED,
            code: "UNAUTHENTICATED",
            message: "a valid bearer token is required".to_owned(),
        },
        AuthError::Forbidden => ApiError {
            status: StatusCode::FORBIDDEN,
            code: "FORBIDDEN",
            message: "the authenticated principal lacks the required scope".to_owned(),
        },
    })
}

fn idempotency_key(headers: &HeaderMap) -> Result<&str, ApiError> {
    let key = headers
        .get("idempotency-key")
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| unprocessable("valid Idempotency-Key is required"))?;
    if !(16..=128).contains(&key.len()) {
        return Err(unprocessable("Idempotency-Key length must be 16..128"));
    }
    Ok(key)
}

fn decode_sha256(value: &str) -> Result<[u8; 32], ApiError> {
    if value.len() != 64
        || value.bytes().any(|byte| !byte.is_ascii_hexdigit() || byte.is_ascii_uppercase())
    {
        return Err(unprocessable("bundleSha256 must be 64 lowercase hexadecimal characters"));
    }
    let bytes = hex::decode(value).map_err(|_| unprocessable("bundleSha256 is invalid"))?;
    let mut output = [0_u8; 32];
    output.copy_from_slice(&bytes);
    Ok(output)
}

fn validate_object_key(value: &str) -> Result<(), ApiError> {
    if value.is_empty()
        || value.len() > 1024
        || value.starts_with('/')
        || value.ends_with('/')
        || value.contains('\\')
        || value.split('/').any(|segment| {
            if segment.len() > 255 {
                return true;
            }
            let mut bytes = segment.bytes();
            !bytes
                .next()
                .is_some_and(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
                || bytes.any(|byte| {
                    !(byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
                })
        })
    {
        return Err(unprocessable(
            "bundleObjectKey must use normalized ASCII path segments beginning with a letter, digit, or underscore",
        ));
    }
    Ok(())
}

fn catalog_error(error: CatalogError) -> ApiError {
    match error {
        CatalogError::IdempotencyConflict
        | CatalogError::DatasetConflict
        | CatalogError::SnapshotConflict
        | CatalogError::CertificationConflict => ApiError {
            status: StatusCode::CONFLICT,
            code: "CATALOG_CONFLICT",
            message: error.to_string(),
        },
        CatalogError::RevisionConflict { .. }
        | CatalogError::IllegalTransition { .. }
        | CatalogError::PublicationConflict => ApiError {
            status: StatusCode::CONFLICT,
            code: "STATE_CONFLICT",
            message: error.to_string(),
        },
        CatalogError::NotFound => ApiError {
            status: StatusCode::NOT_FOUND,
            code: "NOT_FOUND",
            message: "resource was not found".to_owned(),
        },
        CatalogError::Database(_) | CatalogError::UnknownState(_) => {
            tracing::error!(%error, "catalog operation failed");
            ApiError {
                status: StatusCode::SERVICE_UNAVAILABLE,
                code: "CATALOG_UNAVAILABLE",
                message: "durable catalog is unavailable".to_owned(),
            }
        }
    }
}

fn storage_catalog_error(error: ngkg_storage_recovery::StorageCatalogError) -> ApiError {
    match error {
        ngkg_storage_recovery::StorageCatalogError::NotFound => ApiError {
            status: StatusCode::NOT_FOUND,
            code: "STORAGE_SOURCE_NOT_FOUND",
            message: "the tenant-scoped snapshot or backup was not found".to_owned(),
        },
        ngkg_storage_recovery::StorageCatalogError::IdempotencyConflict => ApiError {
            status: StatusCode::CONFLICT,
            code: "IDEMPOTENCY_CONFLICT",
            message: "Idempotency-Key is already bound to different storage recovery inputs".to_owned(),
        },
        ngkg_storage_recovery::StorageCatalogError::Database(_)
        | ngkg_storage_recovery::StorageCatalogError::InvalidCatalogState => {
            tracing::error!("storage recovery catalog operation failed");
            ApiError {
                status: StatusCode::SERVICE_UNAVAILABLE,
                code: "STORAGE_CATALOG_UNAVAILABLE",
                message: "storage recovery catalog is unavailable".to_owned(),
            }
        }
    }
}

fn cap_storage_parallelism(
    requested: u32,
    max_in_flight_bytes: u64,
    plan: &RecoveryPlan,
) -> u32 {
    let largest = plan.tasks.iter().map(|task| task.bytes).max().unwrap_or(1).max(1);
    let byte_limited = (max_in_flight_bytes / largest).max(1);
    let byte_limited = u32::try_from(byte_limited).unwrap_or(u32::MAX);
    requested.min(byte_limited).max(1)
}

fn largest_storage_task(plan: &RecoveryPlan) -> u64 {
    plan.tasks.iter().map(|task| task.bytes).max().unwrap_or(0)
}

fn validate_storage_task_sizes(state: &ApiState, plan: &RecoveryPlan) -> Result<(), ApiError> {
    if plan
        .tasks
        .iter()
        .any(|task| task.bytes > state.storage_recovery.max_task_bytes)
    {
        return Err(ApiError {
            status: StatusCode::PAYLOAD_TOO_LARGE,
            code: "STORAGE_RECOVERY_TASK_TOO_LARGE",
            message: "a snapshot artifact exceeds the trusted worker scratch ceiling".to_owned(),
        });
    }
    Ok(())
}

fn artifact_error(error: ArtifactStoreError) -> ApiError {
    match error {
        ArtifactStoreError::ChecksumMismatch { .. } | ArtifactStoreError::ImmutableConflict(_) => ApiError {
            status: StatusCode::CONFLICT,
            code: "STORAGE_CHECKSUM_CONFLICT",
            message: "immutable storage evidence failed checksum verification".to_owned(),
        },
        ArtifactStoreError::SizeLimit { .. } | ArtifactStoreError::AggregateSizeLimit { .. } => ApiError {
            status: StatusCode::PAYLOAD_TOO_LARGE,
            code: "STORAGE_RECOVERY_LIMIT_EXCEEDED",
            message: "storage recovery evidence exceeds its trusted byte ceiling".to_owned(),
        },
        ArtifactStoreError::BaseUrl(_)
        | ArtifactStoreError::UnsafeKey(_)
        | ArtifactStoreError::InvalidSha256
        | ArtifactStoreError::Io(_)
        | ArtifactStoreError::Store(_) => {
            tracing::error!(%error, "storage recovery artifact operation failed");
            ApiError {
                status: StatusCode::SERVICE_UNAVAILABLE,
                code: "STORAGE_RECOVERY_UNAVAILABLE",
                message: "storage recovery object storage is unavailable".to_owned(),
            }
        }
    }
}

fn artifact_closure_error(message: &str) -> ApiError {
    ApiError {
        status: StatusCode::CONFLICT,
        code: "SNAPSHOT_STORAGE_CLOSURE_INVALID",
        message: format!("snapshot storage closure failed verification: {message}"),
    }
}

fn io_error(error: std::io::Error) -> ApiError {
    tracing::error!(%error, "storage recovery API scratch failed");
    ApiError {
        status: StatusCode::SERVICE_UNAVAILABLE,
        code: "STORAGE_RECOVERY_SCRATCH_UNAVAILABLE",
        message: "storage recovery scratch is unavailable".to_owned(),
    }
}

fn kubernetes_error(error: kube::Error) -> ApiError {
    tracing::error!(%error, "Kubernetes desired-state write failed");
    ApiError {
        status: StatusCode::SERVICE_UNAVAILABLE,
        code: "SCHEDULER_UNAVAILABLE",
        message: "operation is durable but scheduling is temporarily unavailable; retry with the same idempotency key"
            .to_owned(),
    }
}

fn internal_contract_error(context: &str, error: impl std::fmt::Display) -> ApiError {
    tracing::error!(%error, %context, "embedded API contract failure");
    ApiError {
        status: StatusCode::INTERNAL_SERVER_ERROR,
        code: "API_CONTRACT_INVALID",
        message: context.to_owned(),
    }
}

fn unprocessable(message: &str) -> ApiError {
    ApiError {
        status: StatusCode::UNPROCESSABLE_ENTITY,
        code: "INVALID_REQUEST",
        message: message.to_owned(),
    }
}

fn artifact_upload_error(error: ArtifactStoreError) -> ApiError {
    match error {
        ArtifactStoreError::SizeLimit { .. } | ArtifactStoreError::AggregateSizeLimit { .. } => {
            payload_too_large("source object exceeds the configured storage byte ceiling")
        }
        ArtifactStoreError::ImmutableConflict(_) | ArtifactStoreError::ChecksumMismatch { .. } => {
            ApiError {
                status: StatusCode::CONFLICT,
                code: "IMMUTABLE_SOURCE_CONFLICT",
                message: "an immutable object already exists at the content-addressed source key with different bytes"
                    .to_owned(),
            }
        }
        ArtifactStoreError::UnsafeKey(_) | ArtifactStoreError::InvalidSha256 | ArtifactStoreError::BaseUrl(_) => {
            tracing::error!(%error, "source upload storage configuration is invalid");
            ApiError {
                status: StatusCode::SERVICE_UNAVAILABLE,
                code: "SOURCE_STORAGE_CONFIGURATION_INVALID",
                message: "source object storage configuration is invalid".to_owned(),
            }
        }
        ArtifactStoreError::Io(_) | ArtifactStoreError::Store(_) => {
            tracing::error!(%error, "source upload storage operation failed");
            ApiError {
                status: StatusCode::SERVICE_UNAVAILABLE,
                code: "SOURCE_STORAGE_UNAVAILABLE",
                message: "source object storage is unavailable".to_owned(),
            }
        }
    }
}

fn upload_io_error(error: std::io::Error) -> ApiError {
    tracing::error!(%error, "source upload scratch I/O failed");
    ApiError {
        status: StatusCode::SERVICE_UNAVAILABLE,
        code: "SOURCE_UPLOAD_SCRATCH_UNAVAILABLE",
        message: "source upload scratch storage is unavailable".to_owned(),
    }
}

fn payload_too_large(message: &str) -> ApiError {
    ApiError {
        status: StatusCode::PAYLOAD_TOO_LARGE,
        code: "SOURCE_UPLOAD_LIMIT_EXCEEDED",
        message: message.to_owned(),
    }
}

fn absolute_path(name: &str) -> Result<PathBuf> {
    let path = PathBuf::from(required(name)?);
    if !path.is_absolute() {
        anyhow::bail!("{name} must be an absolute path");
    }
    Ok(path)
}

fn positive_u64(name: &str) -> Result<u64> {
    let value = required(name)?
        .parse::<u64>()
        .with_context(|| format!("{name} must be a positive integer"))?;
    if value == 0 {
        anyhow::bail!("{name} must be positive");
    }
    Ok(value)
}

fn positive_u32(name: &str) -> Result<u32> {
    let value = required(name)?
        .parse::<u32>()
        .with_context(|| format!("{name} must be a positive integer"))?;
    if value == 0 {
        anyhow::bail!("{name} must be positive");
    }
    Ok(value)
}

fn positive_usize(name: &str) -> Result<usize> {
    let value = required(name)?
        .parse::<usize>()
        .with_context(|| format!("{name} must be a positive integer"))?;
    if value == 0 {
        anyhow::bail!("{name} must be positive");
    }
    Ok(value)
}

fn required_object_prefix(name: &str) -> Result<String> {
    let value = required(name)?;
    if !safe_object_path(&value) {
        anyhow::bail!("{name} must use normalized ASCII object-store path segments");
    }
    Ok(value)
}

fn safe_object_path(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 1024
        && !value.starts_with('/')
        && !value.ends_with('/')
        && !value.contains('\\')
        && !value.split('/').any(|segment| {
            if segment.len() > 255 {
                return true;
            }
            let mut bytes = segment.bytes();
            !bytes
                .next()
                .is_some_and(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
                || bytes.any(|byte| {
                    !(byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
                })
        })
}

fn prepare_source_upload_scratch(path: &std::path::Path) -> Result<()> {
    if path.exists() {
        let metadata = std::fs::symlink_metadata(path)?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            anyhow::bail!("NGKG_SOURCE_UPLOAD_SCRATCH_ROOT must be a real directory");
        }
    } else {
        std::fs::create_dir_all(path)?;
    }
    let marker = path.join(SOURCE_UPLOAD_SCRATCH_MARKER);
    if marker.exists() {
        if std::fs::symlink_metadata(&marker)?.file_type().is_symlink()
            || std::fs::read(&marker)? != SOURCE_UPLOAD_SCRATCH_MARKER_BYTES
        {
            anyhow::bail!("source upload scratch marker is invalid");
        }
    } else {
        if std::fs::read_dir(path)?.next().transpose()?.is_some() {
            anyhow::bail!("uninitialized source upload scratch must be empty");
        }
        let mut file = std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&marker)?;
        use std::io::Write as _;
        file.write_all(SOURCE_UPLOAD_SCRATCH_MARKER_BYTES)?;
        file.sync_all()?;
        std::fs::File::open(path)?.sync_all()?;
    }
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        let name = entry.file_name();
        let name = name
            .to_str()
            .ok_or_else(|| anyhow::anyhow!("source upload scratch contains a non-UTF-8 entry"))?;
        if name == SOURCE_UPLOAD_SCRATCH_MARKER {
            continue;
        }
        let managed = (name.starts_with("upload-") && name.ends_with(".trig"))
            || (name.starts_with("metadata-") && name.ends_with(".json"));
        if !managed || file_type.is_symlink() || !file_type.is_file() {
            anyhow::bail!("source upload scratch contains unmanaged entry {name}");
        }
        std::fs::remove_file(entry.path())?;
    }
    std::fs::File::open(path)?.sync_all()?;
    Ok(())
}

fn prepare_storage_recovery_scratch(path: &std::path::Path) -> Result<()> {
    if path.exists() {
        let metadata = std::fs::symlink_metadata(path)?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            anyhow::bail!("NGKG_STORAGE_RECOVERY_API_SCRATCH_ROOT must be a real directory");
        }
    } else {
        std::fs::create_dir_all(path)?;
    }
    Ok(())
}

fn required(name: &str) -> Result<String> {
    env::var(name).with_context(|| format!("{name} is required"))
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeSet, fs};

    use axum::http::{HeaderMap, HeaderValue, header::CONTENT_TYPE};
    use uuid::Uuid;

    use super::{
        CloudObjectProvider, CloudObjectVersionPolicy, CreateCloudImportRequest,
        PublicationPolicy, inspect_trig, require_trig_content_type, safe_object_path,
        validate_cloud_import_request, validate_dataset_name,
    };

    fn trig_file(contents: &str) -> std::io::Result<std::path::PathBuf> {
        let path = std::env::temp_dir().join(format!("ngkg-api-upload-{}.trig", Uuid::new_v4()));
        fs::write(&path, contents)?;
        Ok(path)
    }

    #[test]
    fn trig_upload_profile_preserves_default_and_named_graph_counts(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let path = trig_file(
            concat!(
                "<https://example.test/meta> <https://example.test/version> \"1\" .\n",
                "<https://example.test/g1> { <https://example.test/s> <https://example.test/p> <https://example.test/o> . }\n",
                "<https://example.test/g2> { <https://example.test/s2> <https://example.test/p> <https://example.test/o2> . }\n",
            ),
        )?;
        let scan = inspect_trig(&path, 10, 10)
            .map_err(|_| std::io::Error::other("valid TriG fixture was rejected"))?;
        fs::remove_file(path)?;
        assert_eq!(scan.parsed_quad_count, 3);
        assert_eq!(scan.default_graph_quad_count, 1);
        assert_eq!(scan.named_graphs.len(), 2);
        assert_eq!(scan.named_graphs[0].graph_iri, "https://example.test/g1");
        assert_eq!(scan.named_graphs[0].parsed_quad_count, 1);
        Ok(())
    }

    #[test]
    fn trig_upload_rejects_blank_graph_names() -> Result<(), Box<dyn std::error::Error>> {
        let path = trig_file(
            "_:graph { <https://example.test/s> <https://example.test/p> <https://example.test/o> . }\n",
        )?;
        let result = inspect_trig(&path, 10, 10);
        fs::remove_file(path)?;
        assert!(result.is_err());
        Ok(())
    }

    #[test]
    fn trig_upload_requires_an_iri_named_subdomain_graph(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let path = trig_file(
            "<https://example.test/s> <https://example.test/p> <https://example.test/o> .\n",
        )?;
        let result = inspect_trig(&path, 10, 10);
        fs::remove_file(path)?;
        assert!(result.is_err());
        Ok(())
    }

    #[test]
    fn trig_content_type_is_utf8_and_exact_media_type() {
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/trig"));
        assert!(require_trig_content_type(&headers).is_ok());
        headers.insert(
            CONTENT_TYPE,
            HeaderValue::from_static("application/trig; charset=utf-8"),
        );
        assert!(require_trig_content_type(&headers).is_ok());
        headers.insert(
            CONTENT_TYPE,
            HeaderValue::from_static("application/trig; charset=iso-8859-1"),
        );
        assert!(require_trig_content_type(&headers).is_err());
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("text/turtle"));
        assert!(require_trig_content_type(&headers).is_err());
    }

    #[test]
    fn upload_object_prefix_uses_normalized_object_store_segments() {
        assert!(safe_object_path("sources"));
        assert!(safe_object_path("tenant-sources/v1"));
        assert!(!safe_object_path("/sources"));
        assert!(!safe_object_path("sources/../escape"));
        assert!(!safe_object_path("sources//empty"));
    }

    #[test]
    fn dataset_names_are_generic_human_slugs() {
        for name in [
            "supply_chain",
            "clinical_nutrition",
            "oncology",
            "loyalty_graph",
        ] {
            assert!(validate_dataset_name(name).is_ok());
        }
        for name in ["Supply Chain", "6f0b2e4e-6aaa", "../oncology", ""] {
            assert!(validate_dataset_name(name).is_err());
        }
    }

    #[test]
    fn cloud_import_accepts_identity_reference_but_not_provider_version_claims() {
        let base = CreateCloudImportRequest {
            provider: CloudObjectProvider::AwsS3,
            bucket: "existing-trig-bucket".to_owned(),
            account_name: None,
            prefix: Some("published".to_owned()),
            object_keys: Vec::new(),
            include_patterns: vec!["**/*.trig".to_owned()],
            exclude_segments: vec![
                "alignment".to_owned(),
                "closure".to_owned(),
                "provenance".to_owned(),
            ],
            identity_ref: "ngkg-source-reader".to_owned(),
            version_policy: CloudObjectVersionPolicy::RequireImmutableChecksum,
            target_snapshot_id: None,
            parent_snapshot_id: None,
            publication_policy: PublicationPolicy::ManualAfterCertification,
            resource_profile: "distributed-hpc-v1".to_owned(),
            max_source_bytes: 500 * 1024 * 1024 * 1024,
            max_source_objects: 10_000,
            logical_partitions: 256,
            ontology_qualification_request_object_key:
                "imports/qualification-request.json".to_owned(),
            ontology_qualification_request_sha256: "1".repeat(64),
        };
        let profiles = BTreeSet::from(["distributed-hpc-v1".to_owned()]);
        assert!(validate_cloud_import_request(&base, &profiles).is_ok());
        let mut unsupported = base;
        unsupported.version_policy = CloudObjectVersionPolicy::RequireVersionedObjects;
        assert!(validate_cloud_import_request(&unsupported, &profiles).is_err());
    }

    #[test]
    fn embedded_openapi_exposes_swagger_and_every_control_plane_operation(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let document = serde_yaml::from_str::<serde_json::Value>(include_str!(
            "../../../api/openapi.yaml"
        ))?;
        let required = [
            "/docs",
            "/openapi.yaml",
            "/openapi.json",
            "/health/live",
            "/health/ready",
            "/v1/datasets",
            "/v1/datasets/{datasetId}",
            "/v1/datasets/{datasetId}/sources/{sourceId}",
            "/v1/datasets/{datasetId}/ingestions",
            "/v1/datasets/{datasetName}/imports",
            "/v1/datasets/{datasetName}/imports/{operationId}",
            "/v1/datasets/by-id/{datasetId}/imports",
            "/v1/jobs/{operationId}",
            "/v1/jobs/{operationId}/cancel",
            "/v1/datasets/{datasetId}/snapshots/{snapshotId}",
            "/v1/datasets/{datasetId}/snapshots/{snapshotId}/publish",
        ];
        for path in required {
            assert!(
                document.pointer(&format!("/paths/{}", path.replace('~', "~0").replace('/', "~1"))).is_some(),
                "missing OpenAPI path {path}"
            );
        }
        Ok(())
    }
}
