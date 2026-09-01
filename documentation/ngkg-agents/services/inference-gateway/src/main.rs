//! Always-available CPU admission gateway for scale-from-zero GPU inference.
//!
//! Requests wait here, under explicit tenant-independent infrastructure bounds,
//! while Kubernetes provisions a GPU node and vLLM becomes ready. A request is
//! sent to a backend exactly once; ambiguous upstream failures are never retried.

use std::{
    env,
    net::SocketAddr,
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use axum::{
    Router,
    body::Body,
    extract::{DefaultBodyLimit, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use bytes::Bytes;
use futures_util::StreamExt;
use serde::Serialize;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use url::Url;

const OPENAPI: &str = include_str!("../../../contracts/inference-gateway-openapi.yaml");

#[derive(Clone)]
struct AppState {
    inner: Arc<Inner>,
}

struct Inner {
    client: reqwest::Client,
    backend: Url,
    execution_lanes: Arc<Semaphore>,
    queue_lanes: Arc<Semaphore>,
    cold_start_timeout: Duration,
    readiness_poll: Duration,
    upstream_timeout: Duration,
    maximum_response_bytes: usize,
    served_model: String,
    draining: AtomicBool,
    backend_ready: AtomicBool,
    waiting: AtomicUsize,
    in_flight: AtomicUsize,
    admitted_total: AtomicU64,
    rejected_total: AtomicU64,
    failed_total: AtomicU64,
}

enum Gauge {
    Waiting,
    InFlight,
}

struct GaugeGuard {
    inner: Arc<Inner>,
    gauge: Gauge,
}

impl GaugeGuard {
    fn new(inner: Arc<Inner>, gauge: Gauge) -> Self {
        match gauge {
            Gauge::Waiting => inner.waiting.fetch_add(1, Ordering::AcqRel),
            Gauge::InFlight => inner.in_flight.fetch_add(1, Ordering::AcqRel),
        };
        Self { inner, gauge }
    }
}

impl Drop for GaugeGuard {
    fn drop(&mut self) {
        match self.gauge {
            Gauge::Waiting => self.inner.waiting.fetch_sub(1, Ordering::AcqRel),
            Gauge::InFlight => self.inner.in_flight.fetch_sub(1, Ordering::AcqRel),
        };
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct StatusPayload {
    scope: &'static str,
    ready: bool,
    draining: bool,
    backend_ready: bool,
    waiting_requests: usize,
    in_flight_requests: usize,
    admitted_total: u64,
    rejected_total: u64,
    failed_total: u64,
    served_model: String,
    observed_at_epoch_ms: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,ngkg_inference_gateway=info".into()),
        )
        .init();

    let bind = socket("NGKG_INFERENCE_BIND", "0.0.0.0:8080")?;
    let backend = Url::parse(&required("NGKG_INFERENCE_BACKEND_URL")?)?;
    anyhow::ensure!(
        backend.scheme() == "http" && is_cluster_service(&backend),
        "NGKG_INFERENCE_BACKEND_URL must be an in-cluster http service name"
    );
    let maximum_request_bytes = positive_usize("NGKG_INFERENCE_MAX_REQUEST_BYTES", 16_777_216)?;
    let state = AppState {
        inner: Arc::new(Inner {
            client: reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .connect_timeout(duration_ms("NGKG_INFERENCE_CONNECT_TIMEOUT_MS", 2_000)?)
                .timeout(duration_ms("NGKG_INFERENCE_UPSTREAM_TIMEOUT_MS", 900_000)?)
                .build()?,
            backend,
            execution_lanes: Arc::new(Semaphore::new(positive_usize(
                "NGKG_INFERENCE_MAX_IN_FLIGHT",
                256,
            )?)),
            queue_lanes: Arc::new(Semaphore::new(positive_usize(
                "NGKG_INFERENCE_MAX_WAITING",
                4_096,
            )?)),
            cold_start_timeout: duration_ms("NGKG_INFERENCE_COLD_START_TIMEOUT_MS", 900_000)?,
            readiness_poll: duration_ms("NGKG_INFERENCE_READINESS_POLL_MS", 1_000)?,
            upstream_timeout: duration_ms("NGKG_INFERENCE_UPSTREAM_TIMEOUT_MS", 900_000)?,
            maximum_response_bytes: positive_usize(
                "NGKG_INFERENCE_MAX_RESPONSE_BYTES",
                67_108_864,
            )?,
            served_model: required("NGKG_INFERENCE_SERVED_MODEL")?,
            draining: AtomicBool::new(false),
            backend_ready: AtomicBool::new(false),
            waiting: AtomicUsize::new(0),
            in_flight: AtomicUsize::new(0),
            admitted_total: AtomicU64::new(0),
            rejected_total: AtomicU64::new(0),
            failed_total: AtomicU64::new(0),
        }),
    };

    let app = Router::new()
        .route("/health/live", get(live))
        .route("/health/ready", get(ready))
        .route("/v1/status", get(status))
        .route("/v1/chat/completions", post(chat_completions))
        .route("/metrics", get(metrics))
        .route("/openapi.yaml", get(openapi))
        .layer(DefaultBodyLimit::max(maximum_request_bytes))
        .with_state(state.clone());

    tokio::spawn(backend_observer(state.clone()));
    let listener = tokio::net::TcpListener::bind(bind).await?;
    tracing::info!(%bind, backend=%state.inner.backend, "inference admission gateway ready");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown(state))
        .await?;
    Ok(())
}

async fn live() -> StatusCode {
    StatusCode::NO_CONTENT
}

async fn ready(State(state): State<AppState>) -> StatusCode {
    if state.inner.draining.load(Ordering::Acquire) {
        StatusCode::SERVICE_UNAVAILABLE
    } else {
        // Backend readiness is intentionally not required: this CPU service is
        // where callers wait while the GPU deployment scales from zero.
        StatusCode::NO_CONTENT
    }
}

async fn status(State(state): State<AppState>) -> impl IntoResponse {
    let inner = &state.inner;
    axum::Json(StatusPayload {
        scope: "INSTANCE",
        ready: !inner.draining.load(Ordering::Acquire),
        draining: inner.draining.load(Ordering::Acquire),
        backend_ready: inner.backend_ready.load(Ordering::Acquire),
        waiting_requests: inner.waiting.load(Ordering::Acquire),
        in_flight_requests: inner.in_flight.load(Ordering::Acquire),
        admitted_total: inner.admitted_total.load(Ordering::Relaxed),
        rejected_total: inner.rejected_total.load(Ordering::Relaxed),
        failed_total: inner.failed_total.load(Ordering::Relaxed),
        served_model: inner.served_model.clone(),
        observed_at_epoch_ms: epoch_ms(),
    })
}

async fn openapi() -> impl IntoResponse {
    ([(header::CONTENT_TYPE, "application/yaml")], OPENAPI)
}

async fn chat_completions(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if state.inner.draining.load(Ordering::Acquire) {
        return problem(
            StatusCode::SERVICE_UNAVAILABLE,
            "INFERENCE_GATEWAY_DRAINING",
        );
    }
    let Ok(payload) = serde_json::from_slice::<serde_json::Value>(&body) else {
        return problem(StatusCode::UNSUPPORTED_MEDIA_TYPE, "JSON_BODY_REQUIRED");
    };
    if !is_json(&headers) {
        return problem(StatusCode::UNSUPPORTED_MEDIA_TYPE, "JSON_BODY_REQUIRED");
    }
    if payload.get("model").and_then(serde_json::Value::as_str)
        != Some(state.inner.served_model.as_str())
    {
        return problem(StatusCode::UNPROCESSABLE_ENTITY, "SERVED_MODEL_MISMATCH");
    }
    let queue_permit = if let Ok(permit) = Arc::clone(&state.inner.queue_lanes).try_acquire_owned()
    {
        permit
    } else {
        state.inner.rejected_total.fetch_add(1, Ordering::Relaxed);
        return problem(StatusCode::TOO_MANY_REQUESTS, "INFERENCE_QUEUE_FULL");
    };
    let waiting = GaugeGuard::new(Arc::clone(&state.inner), Gauge::Waiting);
    let outcome = admit_and_forward(&state, &headers, body, queue_permit, waiting).await;
    if outcome.status().is_server_error() {
        state.inner.failed_total.fetch_add(1, Ordering::Relaxed);
    }
    outcome
}

async fn admit_and_forward(
    state: &AppState,
    headers: &HeaderMap,
    body: Bytes,
    _queue_permit: OwnedSemaphorePermit,
    waiting: GaugeGuard,
) -> Response {
    let deadline = Instant::now() + state.inner.cold_start_timeout;
    loop {
        if state.inner.draining.load(Ordering::Acquire) {
            return problem(
                StatusCode::SERVICE_UNAVAILABLE,
                "INFERENCE_GATEWAY_DRAINING",
            );
        }
        match backend_ready(state).await {
            Ok(true) => break,
            Ok(false) | Err(_) if Instant::now() < deadline => {
                tokio::time::sleep(state.inner.readiness_poll).await;
            }
            Ok(false) | Err(_) => {
                state.inner.rejected_total.fetch_add(1, Ordering::Relaxed);
                return problem(StatusCode::GATEWAY_TIMEOUT, "GPU_COLD_START_TIMEOUT");
            }
        }
    }

    let remaining = deadline.saturating_duration_since(Instant::now());
    let execution_permit = if let Ok(Ok(permit)) = tokio::time::timeout(
        remaining,
        Arc::clone(&state.inner.execution_lanes).acquire_owned(),
    )
    .await
    {
        permit
    } else {
        state.inner.rejected_total.fetch_add(1, Ordering::Relaxed);
        return problem(StatusCode::GATEWAY_TIMEOUT, "INFERENCE_ADMISSION_TIMEOUT");
    };
    drop(waiting);
    let _in_flight = GaugeGuard::new(Arc::clone(&state.inner), Gauge::InFlight);
    state.inner.admitted_total.fetch_add(1, Ordering::Relaxed);

    let response = forward_once(state, headers, body).await;
    drop(execution_permit);
    response
}

async fn backend_ready(state: &AppState) -> Result<bool> {
    let endpoint = state.inner.backend.join("health/ready")?;
    let ready = state
        .inner
        .client
        .get(endpoint)
        .timeout(Duration::from_secs(2))
        .send()
        .await
        .is_ok_and(|response| response.status().is_success());
    state.inner.backend_ready.store(ready, Ordering::Release);
    Ok(ready)
}

async fn backend_observer(state: AppState) {
    loop {
        let _ = backend_ready(&state).await;
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

async fn forward_once(state: &AppState, headers: &HeaderMap, body: Bytes) -> Response {
    let endpoint = match state.inner.backend.join("v1/chat/completions") {
        Ok(value) => value,
        Err(_) => return problem(StatusCode::BAD_GATEWAY, "INVALID_BACKEND_URL"),
    };
    let mut request = state
        .inner
        .client
        .post(endpoint)
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::ACCEPT, "application/json")
        .timeout(state.inner.upstream_timeout)
        .body(body);
    if let Some(request_id) = headers.get("x-request-id") {
        request = request.header("x-request-id", request_id);
    }
    // A POST is deliberately attempted once. Retrying an ambiguous transport
    // failure could duplicate billable inference or tool-bearing output.
    let upstream = match request.send().await {
        Ok(value) => value,
        Err(error) => {
            tracing::warn!(%error, "single inference attempt failed");
            return problem(StatusCode::BAD_GATEWAY, "INFERENCE_UPSTREAM_FAILED");
        }
    };
    bounded_response(upstream, state.inner.maximum_response_bytes).await
}

async fn bounded_response(upstream: reqwest::Response, maximum: usize) -> Response {
    let status = upstream.status();
    let content_type = upstream
        .headers()
        .get(header::CONTENT_TYPE)
        .cloned()
        .unwrap_or_else(|| HeaderValue::from_static("application/json"));
    let mut stream = upstream.bytes_stream();
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = match chunk {
            Ok(value) => value,
            Err(_) => return problem(StatusCode::BAD_GATEWAY, "INFERENCE_RESPONSE_FAILED"),
        };
        if bytes.len().saturating_add(chunk.len()) > maximum {
            return problem(StatusCode::BAD_GATEWAY, "INFERENCE_RESPONSE_TOO_LARGE");
        }
        bytes.extend_from_slice(&chunk);
    }
    let mut response = Response::new(Body::from(bytes));
    *response.status_mut() = status;
    response
        .headers_mut()
        .insert(header::CONTENT_TYPE, content_type);
    response
}

async fn metrics(State(state): State<AppState>) -> impl IntoResponse {
    let inner = &state.inner;
    let output = format!(
        concat!(
            "# TYPE ngkg_inference_waiting_requests gauge\n",
            "ngkg_inference_waiting_requests {}\n",
            "# TYPE ngkg_inference_in_flight_requests gauge\n",
            "ngkg_inference_in_flight_requests {}\n",
            "# TYPE ngkg_inference_backend_ready gauge\n",
            "ngkg_inference_backend_ready {}\n",
            "# TYPE ngkg_inference_draining gauge\n",
            "ngkg_inference_draining {}\n",
            "# TYPE ngkg_inference_admitted_total counter\n",
            "ngkg_inference_admitted_total {}\n",
            "# TYPE ngkg_inference_rejected_total counter\n",
            "ngkg_inference_rejected_total {}\n",
            "# TYPE ngkg_inference_failed_total counter\n",
            "ngkg_inference_failed_total {}\n"
        ),
        inner.waiting.load(Ordering::Acquire),
        inner.in_flight.load(Ordering::Acquire),
        usize::from(inner.backend_ready.load(Ordering::Acquire)),
        usize::from(inner.draining.load(Ordering::Acquire)),
        inner.admitted_total.load(Ordering::Relaxed),
        inner.rejected_total.load(Ordering::Relaxed),
        inner.failed_total.load(Ordering::Relaxed),
    );
    (
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        output,
    )
}

async fn shutdown(state: AppState) {
    #[cfg(unix)]
    {
        let terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate());
        match terminate {
            Ok(mut signal) => {
                tokio::select! { _ = signal.recv() => {}, _ = tokio::signal::ctrl_c() => {} }
            }
            Err(_) => {
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
    state.inner.draining.store(true, Ordering::Release);
    let deadline = Instant::now() + Duration::from_secs(110);
    while (state.inner.waiting.load(Ordering::Acquire) > 0
        || state.inner.in_flight.load(Ordering::Acquire) > 0)
        && Instant::now() < deadline
    {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn problem(status: StatusCode, code: &'static str) -> Response {
    let payload = serde_json::json!({"code": code, "status": status.as_u16()});
    (status, axum::Json(payload)).into_response()
}

fn is_json(headers: &HeaderMap) -> bool {
    headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| {
            value.split(';').next().is_some_and(|mime| {
                mime.trim().eq_ignore_ascii_case("application/json")
                    || mime.trim().ends_with("+json")
            })
        })
}

fn is_cluster_service(url: &Url) -> bool {
    url.host_str().is_some_and(|host| {
        !host.eq_ignore_ascii_case("localhost")
            && host.parse::<std::net::IpAddr>().is_err()
            && (host.ends_with(".svc")
                || host.ends_with(".svc.cluster.local")
                || !host.contains('.'))
    })
}

fn required(name: &'static str) -> Result<String> {
    env::var(name)
        .with_context(|| format!("{name} is required"))
        .and_then(|value| {
            anyhow::ensure!(!value.trim().is_empty(), "{name} must not be empty");
            Ok(value)
        })
}

fn positive_usize(name: &'static str, default: usize) -> Result<usize> {
    let value = env::var(name)
        .ok()
        .map_or(Ok(default), |value| usize::from_str(&value))?;
    anyhow::ensure!(value > 0, "{name} must be positive");
    Ok(value)
}

fn duration_ms(name: &'static str, default: u64) -> Result<Duration> {
    let value = env::var(name)
        .ok()
        .map_or(Ok(default), |value| u64::from_str(&value))?;
    anyhow::ensure!(value > 0, "{name} must be positive");
    Ok(Duration::from_millis(value))
}

fn socket(name: &'static str, default: &'static str) -> Result<SocketAddr> {
    env::var(name)
        .unwrap_or_else(|_| default.to_owned())
        .parse()
        .with_context(|| format!("{name} must be a socket address"))
}

fn epoch_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |value| {
            u64::try_from(value.as_millis()).unwrap_or(u64::MAX)
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_cluster_service_backends_are_accepted() -> Result<()> {
        assert!(is_cluster_service(&Url::parse(
            "http://ngkg-vllm-backend:8081/"
        )?));
        assert!(is_cluster_service(&Url::parse(
            "http://backend.ns.svc:8081/"
        )?));
        assert!(!is_cluster_service(&Url::parse("http://127.0.0.1:8081/")?));
        assert!(!is_cluster_service(&Url::parse("https://example.com/")?));
        Ok(())
    }
}
