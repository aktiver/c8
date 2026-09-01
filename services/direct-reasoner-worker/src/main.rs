//! Dedicated online HermiT partition worker used by Phase 40.13.7.
//!
//! The coordinator sends immutable candidate partitions. This process verifies every ontology,
//! adapter, request, path, and resource boundary before launching a one-CPU HermiT child. It never
//! selects graphs or aligns data: authorization and asserted-module selection happen before a
//! partition is admitted, and the worker accepts only checksum-bound files under its read-only
//! ontology root.

use std::{
    env, fs,
    net::SocketAddr,
    path::{Component, Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, State},
    http::{HeaderMap, StatusCode, header::AUTHORIZATION},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use ngkg_direct_reasoner::{DirectExactAdapter, DirectExactLimits, execute_exact_direct_partition};
use ngkg_hpc_runtime::{ThreadBudget, capability_report};
use ngkg_types::{DirectExactPartitionResult, DirectExactRequest};
use serde::Serialize;
use sha2::{Digest, Sha256};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tower_http::trace::TraceLayer;

#[derive(Clone)]
struct AppState {
    ontology_root: PathBuf,
    work_root: PathBuf,
    adapter: DirectExactAdapter,
    limits: DirectExactLimits,
    token_sha256: [u8; 32],
    lanes: Arc<Semaphore>,
    max_pending: u64,
    metrics: Arc<Metrics>,
}

#[derive(Default)]
struct Metrics {
    queued_partitions: AtomicU64,
    in_flight_partitions: AtomicU64,
    completed_partitions: AtomicU64,
    failed_partitions: AtomicU64,
    estimated_axioms: AtomicU64,
    oldest_queue_epoch_milliseconds: AtomicU64,
    service_nanoseconds: AtomicU64,
}

struct PendingGuard {
    metrics: Arc<Metrics>,
    estimated_axioms: u64,
    active: bool,
}

struct InFlightGuard {
    metrics: Arc<Metrics>,
    _permit: OwnedSemaphorePermit,
    started: Instant,
    failed: bool,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ErrorBody {
    code: &'static str,
    message: String,
}

#[derive(Debug)]
enum WorkerError {
    Unauthorized,
    QueueFull,
    Invalid(String),
    Reasoner(String),
    Join(String),
}

impl IntoResponse for WorkerError {
    fn into_response(self) -> Response {
        let (status, code, message) = match self {
            Self::Unauthorized => (
                StatusCode::UNAUTHORIZED,
                "REASONER_UNAUTHORIZED",
                "a valid internal reasoner bearer token is required".to_owned(),
            ),
            Self::QueueFull => (
                StatusCode::TOO_MANY_REQUESTS,
                "REASONER_QUEUE_FULL",
                "the bounded reasoner partition queue is full".to_owned(),
            ),
            Self::Invalid(message) => (
                StatusCode::UNPROCESSABLE_ENTITY,
                "REASONER_PARTITION_INVALID",
                message,
            ),
            Self::Reasoner(message) | Self::Join(message) => (
                StatusCode::SERVICE_UNAVAILABLE,
                "REASONER_PARTITION_FAILED",
                message,
            ),
        };
        (status, Json(ErrorBody { code, message })).into_response()
    }
}

impl Drop for PendingGuard {
    fn drop(&mut self) {
        if self.active {
            let previous = self
                .metrics
                .queued_partitions
                .fetch_sub(1, Ordering::AcqRel);
            if previous == 1 {
                self.metrics
                    .oldest_queue_epoch_milliseconds
                    .store(0, Ordering::Release);
            }
            self.metrics
                .estimated_axioms
                .fetch_sub(self.estimated_axioms, Ordering::AcqRel);
        }
    }
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        self.metrics
            .in_flight_partitions
            .fetch_sub(1, Ordering::AcqRel);
        if self.failed {
            self.metrics
                .failed_partitions
                .fetch_add(1, Ordering::Relaxed);
        } else {
            self.metrics
                .completed_partitions
                .fetch_add(1, Ordering::Relaxed);
        }
        self.metrics.service_nanoseconds.fetch_add(
            u64::try_from(self.started.elapsed().as_nanos()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
    }
}

fn main() -> Result<()> {
    let control_threads = positive_usize("NGKG_CONTROL_THREADS")?;
    let reasoner_lanes = positive_usize("NGKG_RUST_COMPUTE_THREADS")?;
    let blocking_io = positive_usize("NGKG_BLOCKING_IO_THREADS")?;
    let capabilities = capability_report(ThreadBudget {
        rust_compute: reasoner_lanes,
        blocking_io,
        openmp: positive_usize("OMP_NUM_THREADS")?,
        blas: positive_usize("OPENBLAS_NUM_THREADS")?,
        control: control_threads,
    })?;
    let max_in_flight = positive_usize("NGKG_REASONER_MAX_IN_FLIGHT")?;
    if max_in_flight > reasoner_lanes {
        anyhow::bail!("NGKG_REASONER_MAX_IN_FLIGHT cannot exceed the cgroup CPU lane budget");
    }
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(control_threads)
        .max_blocking_threads(
            reasoner_lanes
                .checked_add(blocking_io)
                .context("reasoner blocking-thread budget overflow")?,
        )
        .enable_all()
        .build()?
        .block_on(async_main(max_in_flight, capabilities.cpuset_cores))
}

async fn async_main(max_in_flight: usize, cpuset_cores: usize) -> Result<()> {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter("info")
        .init();
    let bind = required("NGKG_BIND_ADDR")?
        .parse::<SocketAddr>()
        .context("NGKG_BIND_ADDR must be a socket address")?;
    let ontology_root = canonical_dir(&required_path("NGKG_REASONER_ONTOLOGY_ROOT")?)?;
    let work_root_input = required_path("NGKG_REASONER_WORK_ROOT")?;
    fs::create_dir_all(&work_root_input)?;
    let work_root = canonical_dir(&work_root_input)?;
    let adapter_jar = required_path("NGKG_REASONER_ADAPTER_JAR")?;
    let adapter = DirectExactAdapter {
        java_executable: required_path("NGKG_JAVA_EXECUTABLE")?,
        adapter_jar,
        adapter_sha256: required("NGKG_REASONER_ADAPTER_SHA256")?,
        adapter_version: required("NGKG_REASONER_ADAPTER_VERSION")?,
        reasoner_version: required("NGKG_HERMIT_VERSION")?,
    };
    if adapter.reasoner_version != "1.4.5.519" {
        anyhow::bail!("NGKG_HERMIT_VERSION must be the pinned version 1.4.5.519");
    }
    let token = decode_sha256(&required("NGKG_REASONER_SHARED_TOKEN_SHA256")?)?;
    let state = AppState {
        ontology_root,
        work_root,
        adapter,
        limits: DirectExactLimits {
            max_candidate_bindings: positive_u64("NGKG_DIRECT_MAX_CANDIDATE_BINDINGS")?,
            max_partition_candidates: positive_u64("NGKG_DIRECT_MAX_PARTITION_CANDIDATES")?,
            max_exact_partitions: positive_u64("NGKG_DIRECT_MAX_EXACT_PARTITIONS")?,
            max_grounded_axioms_per_candidate: positive_u64(
                "NGKG_DIRECT_MAX_GROUNDED_AXIOMS_PER_CANDIDATE",
            )?,
            max_grounded_rdf_bytes_per_candidate: positive_u64(
                "NGKG_DIRECT_MAX_GROUNDED_RDF_BYTES_PER_CANDIDATE",
            )?,
            max_local_reasoner_lanes: max_in_flight,
            reasoner_heap_mib_per_lane: positive_u64("NGKG_REASONER_HEAP_MIB_PER_LANE")?,
            reasoner_timeout: Duration::from_secs(positive_u64(
                "NGKG_REASONER_PARTITION_TIMEOUT_SECONDS",
            )?),
            max_certificate_bytes: positive_u64("NGKG_DIRECT_MAX_CERTIFICATE_BYTES")?,
            max_proof_support_ids: positive_u64("NGKG_DIRECT_MAX_PROOF_SUPPORT_IDS")?,
        },
        token_sha256: token,
        lanes: Arc::new(Semaphore::new(max_in_flight)),
        max_pending: positive_u64("NGKG_REASONER_MAX_PENDING")?,
        metrics: Arc::new(Metrics::default()),
    };
    validate_heap_budget(max_in_flight, state.limits.reasoner_heap_mib_per_lane)?;
    tracing::info!(
        %bind,
        cpuset_cores,
        max_in_flight,
        reasoner_version = %state.adapter.reasoner_version,
        "exact HermiT partition worker is ready"
    );
    let app = Router::new()
        .route("/health/live", get(|| async { StatusCode::NO_CONTENT }))
        .route("/health/ready", get(|| async { StatusCode::NO_CONTENT }))
        .route("/metrics", get(metrics))
        .route("/v1/direct/partitions/execute", post(execute_partition))
        .layer(DefaultBodyLimit::max(64 * 1024 * 1024))
        .layer(TraceLayer::new_for_http())
        .with_state(state);
    let listener = tokio::net::TcpListener::bind(bind).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn execute_partition(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<DirectExactRequest>,
) -> Result<Json<DirectExactPartitionResult>, WorkerError> {
    authorize(&headers, &state.token_sha256)?;
    validate_partition_paths(&state, &request)?;
    if let Some(existing) = existing_partition_result(&request)? {
        return Ok(Json(existing));
    }
    let estimated_axioms = request
        .max_grounded_axioms_per_candidate
        .saturating_mul(request.max_partition_candidates);
    let queued = state
        .metrics
        .queued_partitions
        .fetch_add(1, Ordering::AcqRel)
        .saturating_add(1);
    state
        .metrics
        .estimated_axioms
        .fetch_add(estimated_axioms, Ordering::AcqRel);
    if queued == 1 {
        state
            .metrics
            .oldest_queue_epoch_milliseconds
            .store(epoch_milliseconds(), Ordering::Release);
    }
    let mut pending = PendingGuard {
        metrics: Arc::clone(&state.metrics),
        estimated_axioms,
        active: true,
    };
    if queued > state.max_pending {
        return Err(WorkerError::QueueFull);
    }
    let permit = Arc::clone(&state.lanes)
        .acquire_owned()
        .await
        .map_err(|_| WorkerError::Reasoner("reasoner admission controller is closed".to_owned()))?;
    pending.active = false;
    let previous = state
        .metrics
        .queued_partitions
        .fetch_sub(1, Ordering::AcqRel);
    if previous == 1 {
        state
            .metrics
            .oldest_queue_epoch_milliseconds
            .store(0, Ordering::Release);
    }
    state
        .metrics
        .estimated_axioms
        .fetch_sub(estimated_axioms, Ordering::AcqRel);
    state
        .metrics
        .in_flight_partitions
        .fetch_add(1, Ordering::AcqRel);
    let mut active = InFlightGuard {
        metrics: Arc::clone(&state.metrics),
        _permit: permit,
        started: Instant::now(),
        failed: true,
    };
    let adapter = state.adapter.clone();
    let limits = state.limits;
    let work_dir = partition_work_dir(&state.work_root, &request);
    let result = tokio::task::spawn_blocking(move || {
        execute_exact_direct_partition(&adapter, &request, &work_dir, limits)
    })
    .await
    .map_err(|error| WorkerError::Join(error.to_string()))?
    .map_err(|error| WorkerError::Reasoner(error.to_string()))?;
    active.failed = false;
    Ok(Json(result))
}

fn existing_partition_result(
    request: &DirectExactRequest,
) -> Result<Option<DirectExactPartitionResult>, WorkerError> {
    let output = Path::new(&request.output_path);
    if !output.exists() {
        return Ok(None);
    }
    let bytes = fs::read(output)
        .map_err(|error| WorkerError::Invalid(format!("cached partition read failed: {error}")))?;
    let result: DirectExactPartitionResult = serde_json::from_slice(&bytes)
        .map_err(|_| WorkerError::Invalid("cached partition result is malformed".to_owned()))?;
    ngkg_types::validate_direct_exact_partition_result(&result)
        .map_err(|_| WorkerError::Invalid("cached partition result is invalid".to_owned()))?;
    let request_sha256 =
        hex::encode(Sha256::digest(serde_json::to_vec_pretty(request).map_err(
            |_| WorkerError::Invalid("partition request is not serializable".to_owned()),
        )?));
    if result.dataset_id != request.dataset_id
        || result.snapshot_id != request.snapshot_id
        || result.query_sha256 != request.query_sha256
        || result.bgp_sha256 != request.bgp_sha256
        || result.partition != request.partition
        || result.aggregate_input_sha256 != request.aggregate_input_sha256
        || result.request_sha256 != request_sha256
    {
        return Err(WorkerError::Invalid(
            "cached partition result has a conflicting immutable identity".to_owned(),
        ));
    }
    Ok(Some(result))
}

async fn metrics(State(state): State<AppState>) -> impl IntoResponse {
    let metrics = &state.metrics;
    let oldest = metrics
        .oldest_queue_epoch_milliseconds
        .load(Ordering::Acquire);
    let oldest_age = if oldest == 0 {
        0
    } else {
        epoch_milliseconds().saturating_sub(oldest)
    };
    let completed = metrics.completed_partitions.load(Ordering::Relaxed);
    let failed = metrics.failed_partitions.load(Ordering::Relaxed);
    let service_count = completed.saturating_add(failed);
    let service_nanoseconds = metrics.service_nanoseconds.load(Ordering::Relaxed);
    let mean_latency_milliseconds = if service_count == 0 {
        0.0
    } else {
        service_nanoseconds as f64 / service_count as f64 / 1_000_000.0
    };
    let body = format!(
        "# TYPE ngkg_reasoner_queued_candidate_partitions gauge\nngkg_reasoner_queued_candidate_partitions {}\n\
# TYPE ngkg_reasoner_in_flight_partitions gauge\nngkg_reasoner_in_flight_partitions {}\n\
# TYPE ngkg_reasoner_estimated_axioms gauge\nngkg_reasoner_estimated_axioms {}\n\
# TYPE ngkg_reasoner_oldest_queue_age_milliseconds gauge\nngkg_reasoner_oldest_queue_age_milliseconds {}\n\
# TYPE ngkg_reasoner_completed_partitions_total counter\nngkg_reasoner_completed_partitions_total {}\n\
# TYPE ngkg_reasoner_failed_partitions_total counter\nngkg_reasoner_failed_partitions_total {}\n\
# TYPE ngkg_reasoner_service_seconds_total counter\nngkg_reasoner_service_seconds_total {:.6}\n\
# TYPE ngkg_reasoner_service_count_total counter\nngkg_reasoner_service_count_total {}\n\
# TYPE ngkg_reasoner_mean_service_latency_milliseconds gauge\nngkg_reasoner_mean_service_latency_milliseconds {:.6}\n",
        metrics.queued_partitions.load(Ordering::Relaxed),
        metrics.in_flight_partitions.load(Ordering::Relaxed),
        metrics.estimated_axioms.load(Ordering::Relaxed),
        oldest_age,
        completed,
        failed,
        service_nanoseconds as f64 / 1_000_000_000.0,
        service_count,
        mean_latency_milliseconds,
    );
    (
        [("content-type", "text/plain; version=0.0.4; charset=utf-8")],
        body,
    )
}

fn validate_partition_paths(
    state: &AppState,
    request: &DirectExactRequest,
) -> Result<(), WorkerError> {
    if request.reasoner_identity_mismatch() {
        return Err(WorkerError::Invalid(
            "request engine does not identify the pinned exact HermiT adapter".to_owned(),
        ));
    }
    for input in &request.inputs {
        let path = canonical_file(Path::new(&input.path))?;
        if !path.starts_with(&state.ontology_root) || sha256_path(&path)? != input.sha256 {
            return Err(WorkerError::Invalid(
                "ontology input escapes its read-only root or has the wrong checksum".to_owned(),
            ));
        }
    }
    let output = PathBuf::from(&request.output_path);
    if output
        .components()
        .any(|component| matches!(component, Component::ParentDir))
        || output != partition_work_dir(&state.work_root, request).join("result.json")
    {
        return Err(WorkerError::Invalid(
            "partition output path is not the deterministic worker-owned result path".to_owned(),
        ));
    }
    Ok(())
}

trait DirectRequestIdentity {
    fn reasoner_identity_mismatch(&self) -> bool;
}

impl DirectRequestIdentity for DirectExactRequest {
    fn reasoner_identity_mismatch(&self) -> bool {
        self.engine != ngkg_types::DIRECT_EXACT_ENGINE_V1
    }
}

fn partition_work_dir(root: &Path, request: &DirectExactRequest) -> PathBuf {
    root.join(request.dataset_id.to_string())
        .join(request.snapshot_id.to_string())
        .join(&request.query_sha256)
        .join(&request.bgp_sha256)
        .join(format!("partition-{:04}", request.partition.index))
}

fn authorize(headers: &HeaderMap, expected: &[u8; 32]) -> Result<(), WorkerError> {
    let token = headers
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .filter(|value| !value.is_empty())
        .ok_or(WorkerError::Unauthorized)?;
    let observed: [u8; 32] = Sha256::digest(token.as_bytes()).into();
    let different = observed
        .iter()
        .zip(expected)
        .fold(0_u8, |difference, (left, right)| {
            difference | (left ^ right)
        });
    if different != 0 {
        return Err(WorkerError::Unauthorized);
    }
    Ok(())
}

fn canonical_dir(path: &Path) -> Result<PathBuf> {
    let canonical = fs::canonicalize(path)?;
    if !canonical.is_dir() {
        anyhow::bail!("{} is not a directory", path.display());
    }
    Ok(canonical)
}

fn canonical_file(path: &Path) -> Result<PathBuf, WorkerError> {
    let metadata =
        fs::symlink_metadata(path).map_err(|error| WorkerError::Invalid(error.to_string()))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(WorkerError::Invalid(
            "ontology inputs must be non-symlink regular files".to_owned(),
        ));
    }
    fs::canonicalize(path).map_err(|error| WorkerError::Invalid(error.to_string()))
}

fn sha256_path(path: &Path) -> Result<String, WorkerError> {
    use std::io::Read;
    let mut file = fs::File::open(path).map_err(|error| WorkerError::Invalid(error.to_string()))?;
    let mut hash = Sha256::new();
    let mut buffer = vec![0_u8; 1024 * 1024].into_boxed_slice();
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|error| WorkerError::Invalid(error.to_string()))?;
        if read == 0 {
            break;
        }
        hash.update(&buffer[..read]);
    }
    Ok(hex::encode(hash.finalize()))
}

fn required(name: &str) -> Result<String> {
    env::var(name)
        .with_context(|| format!("{name} is required"))
        .and_then(|value| {
            if value.is_empty() {
                anyhow::bail!("{name} cannot be empty");
            }
            Ok(value)
        })
}

fn required_path(name: &str) -> Result<PathBuf> {
    let path = PathBuf::from(required(name)?);
    if !path.is_absolute() {
        anyhow::bail!("{name} must be absolute");
    }
    Ok(path)
}

fn positive_usize(name: &str) -> Result<usize> {
    required(name)?
        .parse::<usize>()
        .with_context(|| format!("{name} must be a positive integer"))
        .and_then(|value| {
            if value == 0 {
                anyhow::bail!("{name} must be positive");
            }
            Ok(value)
        })
}

fn positive_u64(name: &str) -> Result<u64> {
    required(name)?
        .parse::<u64>()
        .with_context(|| format!("{name} must be a positive integer"))
        .and_then(|value| {
            if value == 0 {
                anyhow::bail!("{name} must be positive");
            }
            Ok(value)
        })
}

fn decode_sha256(value: &str) -> Result<[u8; 32]> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        anyhow::bail!("SHA-256 values must be 64 lowercase hexadecimal characters");
    }
    let bytes = hex::decode(value)?;
    bytes
        .try_into()
        .map_err(|_| anyhow::anyhow!("SHA-256 length is invalid"))
}

fn validate_heap_budget(lanes: usize, heap_mib_per_lane: u64) -> Result<()> {
    let Some(memory_limit) = cgroup_memory_limit_bytes() else {
        return Ok(());
    };
    let heap_bytes = u64::try_from(lanes)
        .ok()
        .and_then(|value| value.checked_mul(heap_mib_per_lane))
        .and_then(|value| value.checked_mul(1024 * 1024))
        .context("reasoner JVM heap budget overflow")?;
    if heap_bytes > memory_limit.saturating_mul(80) / 100 {
        anyhow::bail!(
            "aggregate HermiT heap budget {heap_bytes} exceeds 80% of cgroup memory limit {memory_limit}"
        );
    }
    Ok(())
}

fn cgroup_memory_limit_bytes() -> Option<u64> {
    for path in [
        "/sys/fs/cgroup/memory.max",
        "/sys/fs/cgroup/memory/memory.limit_in_bytes",
    ] {
        let Ok(raw) = fs::read_to_string(path) else {
            continue;
        };
        let raw = raw.trim();
        if raw == "max" {
            return None;
        }
        let Ok(value) = raw.parse::<u64>() else {
            continue;
        };
        if value > 0 && value < (1_u64 << 60) {
            return Some(value);
        }
    }
    None
}

fn epoch_milliseconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| u64::try_from(duration.as_millis()).ok())
        .unwrap_or(u64::MAX)
}
