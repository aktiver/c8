//! Per-GPU-pod readiness and drain proxy.
//!
//! The vLLM engine listens only on loopback. This process is the sole backend
//! endpoint, verifies the expected served model, rejects new work while
//! draining, and waits for admitted requests before allowing pod termination.

use std::{
    env,
    net::SocketAddr,
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
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
use serde::Deserialize;
use tokio::sync::Semaphore;
use url::Url;

const OPENAPI: &str = include_str!("../../../contracts/vllm-pod-agent-openapi.yaml");

#[derive(Clone)]
struct AppState {
    inner: Arc<Inner>,
}

struct Inner {
    client: reqwest::Client,
    upstream: Url,
    served_model: String,
    lanes: Arc<Semaphore>,
    maximum_response_bytes: usize,
    upstream_timeout: Duration,
    drain_timeout: Duration,
    draining: AtomicBool,
    upstream_ready: AtomicBool,
    in_flight: AtomicUsize,
    admitted_total: AtomicU64,
    failed_total: AtomicU64,
}

struct InFlightGuard {
    inner: Arc<Inner>,
}

impl InFlightGuard {
    fn new(inner: Arc<Inner>) -> Self {
        inner.in_flight.fetch_add(1, Ordering::AcqRel);
        Self { inner }
    }
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        self.inner.in_flight.fetch_sub(1, Ordering::AcqRel);
    }
}

#[derive(Deserialize)]
struct Models {
    data: Vec<Model>,
}

#[derive(Deserialize)]
struct Model {
    id: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,ngkg_vllm_pod_agent=info".into()),
        )
        .init();

    if env::args().nth(1).as_deref() == Some("drain") {
        return request_drain().await;
    }

    let bind = socket("NGKG_VLLM_AGENT_BIND", "0.0.0.0:8081")?;
    let admin_bind = socket("NGKG_VLLM_AGENT_ADMIN_BIND", "127.0.0.1:8082")?;
    anyhow::ensure!(
        admin_bind.ip().is_loopback(),
        "admin listener must be loopback-only"
    );
    let upstream = Url::parse(&required("NGKG_VLLM_UPSTREAM_URL")?)?;
    anyhow::ensure!(
        upstream.scheme() == "http" && upstream.host_str() == Some("127.0.0.1"),
        "vLLM upstream must use loopback http"
    );
    let maximum_request_bytes = positive_usize("NGKG_VLLM_AGENT_MAX_REQUEST_BYTES", 16_777_216)?;
    let state = AppState {
        inner: Arc::new(Inner {
            client: reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .connect_timeout(duration_ms("NGKG_VLLM_AGENT_CONNECT_TIMEOUT_MS", 2_000)?)
                .timeout(duration_ms("NGKG_VLLM_AGENT_UPSTREAM_TIMEOUT_MS", 900_000)?)
                .build()?,
            upstream,
            served_model: required("NGKG_VLLM_SERVED_MODEL")?,
            lanes: Arc::new(Semaphore::new(positive_usize(
                "NGKG_VLLM_AGENT_MAX_IN_FLIGHT",
                256,
            )?)),
            maximum_response_bytes: positive_usize(
                "NGKG_VLLM_AGENT_MAX_RESPONSE_BYTES",
                67_108_864,
            )?,
            upstream_timeout: duration_ms("NGKG_VLLM_AGENT_UPSTREAM_TIMEOUT_MS", 900_000)?,
            drain_timeout: duration_ms("NGKG_VLLM_AGENT_DRAIN_TIMEOUT_MS", 600_000)?,
            draining: AtomicBool::new(false),
            upstream_ready: AtomicBool::new(false),
            in_flight: AtomicUsize::new(0),
            admitted_total: AtomicU64::new(0),
            failed_total: AtomicU64::new(0),
        }),
    };

    tokio::spawn(readiness_loop(state.clone()));
    let public = Router::new()
        .route("/health/live", get(live))
        .route("/health/ready", get(ready))
        .route("/v1/chat/completions", post(chat_completions))
        .route("/metrics", get(metrics))
        .route("/openapi.yaml", get(openapi))
        .layer(DefaultBodyLimit::max(maximum_request_bytes))
        .with_state(state.clone());
    let admin = Router::new()
        .route("/admin/drain", post(drain))
        .with_state(state.clone());
    let public_listener = tokio::net::TcpListener::bind(bind).await?;
    let admin_listener = tokio::net::TcpListener::bind(admin_bind).await?;
    tracing::info!(%bind, %admin_bind, upstream=%state.inner.upstream, "vLLM pod agent ready");

    let public_server =
        axum::serve(public_listener, public).with_graceful_shutdown(shutdown(state));
    let admin_server = axum::serve(admin_listener, admin);
    tokio::spawn(async move {
        if let Err(error) = admin_server.await {
            tracing::error!(%error, "vLLM pod-agent admin listener failed");
        }
    });
    public_server.await?;
    Ok(())
}

async fn request_drain() -> Result<()> {
    let endpoint = env::var("NGKG_VLLM_AGENT_DRAIN_URL")
        .unwrap_or_else(|_| "http://127.0.0.1:8082/admin/drain".to_owned());
    let timeout =
        duration_ms("NGKG_VLLM_AGENT_DRAIN_TIMEOUT_MS", 600_000)? + Duration::from_secs(5);
    let response = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(timeout)
        .build()?
        .post(endpoint)
        .send()
        .await?;
    anyhow::ensure!(
        response.status().is_success(),
        "pod-agent drain failed: {}",
        response.status()
    );
    if let Ok(path) = env::var("NGKG_VLLM_DRAIN_COMPLETE_FILE") {
        tokio::fs::write(path, b"complete\n").await?;
    }
    Ok(())
}

async fn live() -> StatusCode {
    StatusCode::NO_CONTENT
}

async fn ready(State(state): State<AppState>) -> StatusCode {
    if !state.inner.draining.load(Ordering::Acquire)
        && state.inner.upstream_ready.load(Ordering::Acquire)
    {
        StatusCode::NO_CONTENT
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

async fn openapi() -> impl IntoResponse {
    ([(header::CONTENT_TYPE, "application/yaml")], OPENAPI)
}

async fn chat_completions(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if state.inner.draining.load(Ordering::Acquire)
        || !state.inner.upstream_ready.load(Ordering::Acquire)
    {
        return problem(StatusCode::SERVICE_UNAVAILABLE, "VLLM_BACKEND_NOT_READY");
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
    let permit = match Arc::clone(&state.inner.lanes).try_acquire_owned() {
        Ok(value) => value,
        Err(_) => return problem(StatusCode::TOO_MANY_REQUESTS, "VLLM_BACKEND_BUSY"),
    };
    let _in_flight = InFlightGuard::new(Arc::clone(&state.inner));
    state.inner.admitted_total.fetch_add(1, Ordering::Relaxed);
    let result = forward_once(&state, &headers, body).await;
    drop(permit);
    if result.status().is_server_error() {
        state.inner.failed_total.fetch_add(1, Ordering::Relaxed);
    }
    result
}

async fn forward_once(state: &AppState, headers: &HeaderMap, body: Bytes) -> Response {
    let endpoint = match state.inner.upstream.join("v1/chat/completions") {
        Ok(value) => value,
        Err(_) => return problem(StatusCode::BAD_GATEWAY, "INVALID_VLLM_URL"),
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
    let upstream = match request.send().await {
        Ok(value) => value,
        Err(error) => {
            tracing::warn!(%error, "vLLM request failed without retry");
            return problem(StatusCode::BAD_GATEWAY, "VLLM_REQUEST_FAILED");
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
            Err(_) => return problem(StatusCode::BAD_GATEWAY, "VLLM_RESPONSE_FAILED"),
        };
        if bytes.len().saturating_add(chunk.len()) > maximum {
            return problem(StatusCode::BAD_GATEWAY, "VLLM_RESPONSE_TOO_LARGE");
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

async fn readiness_loop(state: AppState) {
    let interval =
        duration_ms("NGKG_VLLM_AGENT_READINESS_POLL_MS", 2_000).unwrap_or(Duration::from_secs(2));
    loop {
        if state.inner.draining.load(Ordering::Acquire) {
            state.inner.upstream_ready.store(false, Ordering::Release);
        } else {
            let ready = verify_upstream(&state).await.unwrap_or(false);
            state.inner.upstream_ready.store(ready, Ordering::Release);
        }
        tokio::time::sleep(interval).await;
    }
}

async fn verify_upstream(state: &AppState) -> Result<bool> {
    let health = state
        .inner
        .client
        .get(state.inner.upstream.join("health")?)
        .timeout(Duration::from_secs(2))
        .send()
        .await?;
    if !health.status().is_success() {
        return Ok(false);
    }
    let response = state
        .inner
        .client
        .get(state.inner.upstream.join("v1/models")?)
        .timeout(Duration::from_secs(5))
        .send()
        .await?;
    if !response.status().is_success() {
        return Ok(false);
    }
    let models: Models = response.json().await?;
    Ok(models
        .data
        .iter()
        .any(|model| model.id == state.inner.served_model))
}

async fn drain(State(state): State<AppState>) -> impl IntoResponse {
    state.inner.draining.store(true, Ordering::Release);
    state.inner.upstream_ready.store(false, Ordering::Release);
    let deadline = Instant::now() + state.inner.drain_timeout;
    while state.inner.in_flight.load(Ordering::Acquire) > 0 && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    if state.inner.in_flight.load(Ordering::Acquire) == 0 {
        StatusCode::NO_CONTENT
    } else {
        StatusCode::REQUEST_TIMEOUT
    }
}

async fn metrics(State(state): State<AppState>) -> impl IntoResponse {
    let inner = &state.inner;
    let output = format!(
        concat!(
            "# TYPE ngkg_vllm_backend_ready gauge\n",
            "ngkg_vllm_backend_ready {}\n",
            "# TYPE ngkg_vllm_backend_draining gauge\n",
            "ngkg_vllm_backend_draining {}\n",
            "# TYPE ngkg_vllm_backend_in_flight_requests gauge\n",
            "ngkg_vllm_backend_in_flight_requests {}\n",
            "# TYPE ngkg_vllm_backend_admitted_total counter\n",
            "ngkg_vllm_backend_admitted_total {}\n",
            "# TYPE ngkg_vllm_backend_failed_total counter\n",
            "ngkg_vllm_backend_failed_total {}\n"
        ),
        usize::from(inner.upstream_ready.load(Ordering::Acquire)),
        usize::from(inner.draining.load(Ordering::Acquire)),
        inner.in_flight.load(Ordering::Acquire),
        inner.admitted_total.load(Ordering::Relaxed),
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
    state.inner.upstream_ready.store(false, Ordering::Release);
    let deadline = Instant::now() + state.inner.drain_timeout;
    while state.inner.in_flight.load(Ordering::Acquire) > 0 && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn problem(status: StatusCode, code: &'static str) -> Response {
    (
        status,
        axum::Json(serde_json::json!({"code": code, "status": status.as_u16()})),
    )
        .into_response()
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
