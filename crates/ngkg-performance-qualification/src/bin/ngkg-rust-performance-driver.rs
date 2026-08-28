//! Rust-only live HTTP driver for Phase 40.13.23 NGKG measurements.

use std::{
    env, fs,
    io::Read,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use reqwest::{
    Url,
    blocking::{Client, Response},
    redirect::Policy,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

const MAX_CONCURRENCY: u32 = 10_000;
const MAX_REQUEST_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct DriverRequest {
    format_version: u32,
    run_id: String,
    scenario_id: String,
    family: String,
    trial_phase: String,
    trial: u32,
    cache_state: String,
    concurrency: u32,
    resource_envelope: ResourceEnvelope,
    hardware_sha256: String,
    pricing_sha256: String,
    pricing: PricingEvidence,
    autoscaling_evidence_sha256: String,
    descriptor: Descriptor,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct ResourceEnvelope {
    nodes: u32,
    cpu_millis: u64,
    memory_bytes: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct PricingEvidence {
    format_version: u32,
    observed_epoch_seconds: u64,
    provider: String,
    region: String,
    currency: String,
    node_micro_usd_per_hour: u64,
    object_read_micro_usd_per_million: u64,
    object_write_micro_usd_per_million: u64,
    object_storage_micro_usd_per_gib_month: u64,
    egress_micro_usd_per_gib: u64,
    source_url_sha256: String,
    complete: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct Descriptor {
    dataset: Value,
    operation: HttpOperation,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct HttpOperation {
    endpoint_env: String,
    bearer_token_env: String,
    path: String,
    body_path: PathBuf,
    content_type: String,
    accept: String,
    maximum_response_bytes: u64,
    result_sha256_pointer: String,
    artifact_root_sha256_pointer: Option<String>,
    work_items_per_operation: u64,
    object_read_operations_per_request: u64,
    object_write_operations_per_request: u64,
    egress_bytes_per_request: u64,
    query_log_endpoint_env: Option<String>,
    query_log_token_env: Option<String>,
    cache_control: Option<CacheControl>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct CacheControl {
    endpoint_env: String,
    bearer_token_env: String,
    path: String,
}

#[derive(Debug)]
struct CallEvidence {
    result_sha256: String,
    artifact_root_sha256: Option<String>,
    response_bytes: u64,
    output_items: u64,
    nodes_activated: u32,
    cpu_millis_activated: u64,
    ram_bytes_activated: u64,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct DriverObservation {
    format_version: u32,
    engine: &'static str,
    engine_version: &'static str,
    scenario_id: String,
    trial_phase: String,
    trial: u32,
    duration_nanoseconds: u64,
    operations: u64,
    work_items: u64,
    input_bytes: u64,
    output_items: u64,
    cpu_time_nanoseconds: u64,
    peak_rss_bytes: u64,
    bytes_read: u64,
    bytes_written: u64,
    nodes_activated: u32,
    cpu_millis_activated: u64,
    ram_bytes_activated: u64,
    result_sha256: String,
    artifact_root_sha256: Option<String>,
    autoscaling_evidence_sha256: String,
    cost_micro_usd: u64,
    complete: bool,
    error_class: Option<String>,
}

#[derive(Debug, Error)]
enum DriverError {
    #[error("invalid benchmark request: {0}")]
    Invalid(String),
    #[error("benchmark I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("benchmark HTTP failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error("benchmark JSON failed: {0}")]
    Json(#[from] serde_json::Error),
    #[error("benchmark worker thread failed")]
    Worker,
}

fn main() {
    if let Err(error) = run() {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), DriverError> {
    let mut arguments = env::args_os();
    let _program = arguments.next();
    let request_path = arguments
        .next()
        .ok_or_else(|| DriverError::Invalid("request path is required".to_owned()))?;
    if arguments.next().is_some() {
        return Err(DriverError::Invalid(
            "exactly one request path is required".to_owned(),
        ));
    }
    let request: DriverRequest = serde_json::from_slice(&fs::read(request_path)?)?;
    validate_request(&request)?;
    let body = read_bounded_regular(&request.descriptor.operation.body_path, MAX_REQUEST_BYTES)?;
    let client = Client::builder()
        .redirect(Policy::none())
        .connect_timeout(Duration::from_secs(30))
        .timeout(Duration::from_secs(3600))
        .build()?;
    reset_cache(&client, &request)?;
    let cpu_before = cgroup_cpu_nanoseconds().unwrap_or(0);
    let started = Instant::now();
    let calls = std::thread::scope(|scope| {
        let mut handles = Vec::new();
        for _ in 0..request.concurrency {
            let client = client.clone();
            let operation = request.descriptor.operation.clone();
            let body = body.clone();
            handles.push(scope.spawn(move || execute_call(&client, &operation, &body)));
        }
        handles
            .into_iter()
            .map(|handle| handle.join().map_err(|_| DriverError::Worker)?)
            .collect::<Result<Vec<_>, DriverError>>()
    })?;
    let elapsed = u64::try_from(started.elapsed().as_nanos()).map_err(|_| {
        DriverError::Invalid("duration exceeds u64 nanoseconds".to_owned())
    })?;
    let cpu_after = cgroup_cpu_nanoseconds().unwrap_or(cpu_before);
    let first = calls
        .first()
        .ok_or_else(|| DriverError::Invalid("no completed calls".to_owned()))?;
    if calls.iter().any(|call| {
        call.result_sha256 != first.result_sha256
            || call.artifact_root_sha256 != first.artifact_root_sha256
    }) {
        return Err(DriverError::Invalid(
            "concurrent calls returned unequal semantic or artifact identities".to_owned(),
        ));
    }
    let result_sha256 = first.result_sha256.clone();
    let artifact_root_sha256 = first.artifact_root_sha256.clone();
    let operations = u64::from(request.concurrency);
    let input_bytes = u64::try_from(body.len())
        .map_err(|_| DriverError::Invalid("request body exceeds u64".to_owned()))?
        .saturating_mul(operations);
    let response_bytes = checked_sum(calls.iter().map(|call| call.response_bytes), "response bytes")?;
    let nodes = calls.iter().try_fold(0_u32, |total, call| {
        total
            .checked_add(call.nodes_activated)
            .ok_or_else(|| DriverError::Invalid("activated node count overflow".to_owned()))
    })?;
    let cpu_millis = checked_sum(
        calls.iter().map(|call| call.cpu_millis_activated),
        "activated CPU",
    )?;
    let ram_bytes = checked_sum(
        calls.iter().map(|call| call.ram_bytes_activated),
        "activated RAM",
    )?;
    let output_items = checked_sum(calls.iter().map(|call| call.output_items), "output items")?;
    let cost = trial_cost(&request, elapsed, nodes)?;
    let observation = DriverObservation {
        format_version: 1,
        engine: "ngkg-rust",
        engine_version: env!("CARGO_PKG_VERSION"),
        scenario_id: request.scenario_id,
        trial_phase: request.trial_phase,
        trial: request.trial,
        duration_nanoseconds: elapsed,
        operations,
        work_items: request
            .descriptor
            .operation
            .work_items_per_operation
            .saturating_mul(operations),
        input_bytes,
        output_items,
        cpu_time_nanoseconds: cpu_after.saturating_sub(cpu_before),
        peak_rss_bytes: cgroup_memory_peak_bytes().unwrap_or(0),
        bytes_read: input_bytes,
        bytes_written: response_bytes,
        nodes_activated: nodes,
        cpu_millis_activated: cpu_millis,
        ram_bytes_activated: ram_bytes,
        result_sha256,
        artifact_root_sha256,
        autoscaling_evidence_sha256: request.autoscaling_evidence_sha256,
        cost_micro_usd: cost,
        complete: true,
        error_class: None,
    };
    println!("{}", serde_json::to_string(&observation)?);
    Ok(())
}

fn validate_request(request: &DriverRequest) -> Result<(), DriverError> {
    let sha = |value: &str| {
        value.len() == 64
            && value
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    };
    if request.format_version != 1
        || request.run_id.is_empty()
        || request.scenario_id.is_empty()
        || request.family.is_empty()
        || !matches!(request.trial_phase.as_str(), "warmup" | "measured")
        || !matches!(request.cache_state.as_str(), "cold" | "warm" | "hot")
        || request.concurrency == 0
        || request.concurrency > MAX_CONCURRENCY
        || request.resource_envelope.nodes == 0
        || request.resource_envelope.cpu_millis == 0
        || request.resource_envelope.memory_bytes == 0
        || !sha(&request.hardware_sha256)
        || !sha(&request.pricing_sha256)
        || !sha(&request.autoscaling_evidence_sha256)
        || request.pricing.format_version != 1
        || request.pricing.currency != "USD"
        || !request.pricing.complete
        || request.descriptor.operation.work_items_per_operation == 0
        || request.descriptor.operation.maximum_response_bytes == 0
        || request.descriptor.operation.maximum_response_bytes > 1024 * 1024 * 1024
        || !request.descriptor.dataset.is_object()
    {
        return Err(DriverError::Invalid(
            "identity, resource, pricing, or operation contract is invalid".to_owned(),
        ));
    }
    Ok(())
}

fn execute_call(
    client: &Client,
    operation: &HttpOperation,
    body: &[u8],
) -> Result<CallEvidence, DriverError> {
    let url = trusted_url(&operation.endpoint_env, &operation.path)?;
    let token = secret_environment(&operation.bearer_token_env)?;
    let response = client
        .post(url)
        .bearer_auth(token)
        .header("content-type", &operation.content_type)
        .header("accept", &operation.accept)
        .body(body.to_vec())
        .send()?;
    let query_execution_id = response
        .headers()
        .get("x-ngkg-query-execution-id")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let value = bounded_json(response, operation.maximum_response_bytes)?;
    let result_sha256 = pointer_string(&value, &operation.result_sha256_pointer)?;
    let artifact_root_sha256 = operation
        .artifact_root_sha256_pointer
        .as_ref()
        .map(|pointer| pointer_string(&value, pointer))
        .transpose()?;
    let response_bytes = u64::try_from(serde_json::to_vec(&value)?.len())
        .map_err(|_| DriverError::Invalid("response size exceeds u64".to_owned()))?;
    let (nodes, cpu, ram, output_items) = match (
        &operation.query_log_endpoint_env,
        &operation.query_log_token_env,
        query_execution_id,
    ) {
        (Some(endpoint), Some(token_env), Some(execution_id)) => {
            query_log_resources(client, endpoint, token_env, &execution_id)?
        }
        (None, None, None) => (1, 1, 1, 1),
        _ => {
            return Err(DriverError::Invalid(
                "query-log endpoint, token, and execution ID must be supplied together".to_owned(),
            ));
        }
    };
    Ok(CallEvidence {
        result_sha256,
        artifact_root_sha256,
        response_bytes,
        output_items,
        nodes_activated: nodes,
        cpu_millis_activated: cpu,
        ram_bytes_activated: ram,
    })
}

fn query_log_resources(
    client: &Client,
    endpoint_env: &str,
    token_env: &str,
    execution_id: &str,
) -> Result<(u32, u64, u64, u64), DriverError> {
    let path = format!("/v1/query_logs/{execution_id}");
    let url = trusted_url(endpoint_env, &path)?;
    let value = bounded_json(
        client
            .get(url)
            .bearer_auth(secret_environment(token_env)?)
            .header("accept", "application/json")
            .send()?,
        1024 * 1024,
    )?;
    if value.pointer("/status").and_then(Value::as_str) != Some("COMPLETED") {
        return Err(DriverError::Invalid(
            "query log is not a completed execution".to_owned(),
        ));
    }
    let nodes = pointer_u64(&value, "/resources/nodesActivated")?;
    Ok((
        u32::try_from(nodes)
            .map_err(|_| DriverError::Invalid("node count exceeds u32".to_owned()))?,
        pointer_u64(&value, "/resources/cpuMillicores")?,
        pointer_u64(&value, "/resources/ramBytes")?,
        value.pointer("/resultRows").and_then(Value::as_u64).unwrap_or(0),
    ))
}

fn reset_cache(client: &Client, request: &DriverRequest) -> Result<(), DriverError> {
    let Some(control) = &request.descriptor.operation.cache_control else {
        if request.cache_state == "cold" {
            return Err(DriverError::Invalid(
                "cold trial lacks an explicit cache controller".to_owned(),
            ));
        }
        return Ok(());
    };
    let response = client
        .post(trusted_url(&control.endpoint_env, &control.path)?)
        .bearer_auth(secret_environment(&control.bearer_token_env)?)
        .json(&serde_json::json!({"cacheState": request.cache_state}))
        .send()?;
    let value = bounded_json(response, 1024 * 1024)?;
    if value.pointer("/status").and_then(Value::as_str) != Some("ready") {
        return Err(DriverError::Invalid(
            "cache controller did not certify readiness".to_owned(),
        ));
    }
    Ok(())
}

fn bounded_json(mut response: Response, maximum: u64) -> Result<Value, DriverError> {
    if !response.status().is_success() {
        return Err(DriverError::Invalid(format!(
            "endpoint returned HTTP {}",
            response.status()
        )));
    }
    let mut bytes = Vec::new();
    response.by_ref().take(maximum.saturating_add(1)).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > maximum {
        return Err(DriverError::Invalid(
            "endpoint response exceeded its byte ceiling".to_owned(),
        ));
    }
    serde_json::from_slice(&bytes).map_err(DriverError::from)
}

fn trusted_url(environment: &str, path: &str) -> Result<Url, DriverError> {
    if !valid_environment_name(environment) || !path.starts_with('/') || path.starts_with("//") {
        return Err(DriverError::Invalid("endpoint reference is invalid".to_owned()));
    }
    let base = env::var(environment)
        .map_err(|_| DriverError::Invalid(format!("missing endpoint environment {environment}")))?;
    let base = Url::parse(&base)
        .map_err(|_| DriverError::Invalid("endpoint URL is invalid".to_owned()))?;
    if base.scheme() != "https" || base.username() != "" || base.password().is_some() {
        return Err(DriverError::Invalid(
            "benchmark endpoints require credential-free HTTPS URLs".to_owned(),
        ));
    }
    base.join(path)
        .map_err(|_| DriverError::Invalid("endpoint path is invalid".to_owned()))
}

fn secret_environment(name: &str) -> Result<String, DriverError> {
    if !valid_environment_name(name) {
        return Err(DriverError::Invalid("token environment name is invalid".to_owned()));
    }
    env::var(name)
        .ok()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| DriverError::Invalid(format!("missing token environment {name}")))
}

fn valid_environment_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
}

fn read_bounded_regular(path: &Path, maximum: u64) -> Result<Vec<u8>, DriverError> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.file_type().is_file() || metadata.file_type().is_symlink() || metadata.len() > maximum {
        return Err(DriverError::Invalid(
            "request body is not one bounded regular file".to_owned(),
        ));
    }
    fs::read(path).map_err(DriverError::from)
}

fn pointer_string(value: &Value, pointer: &str) -> Result<String, DriverError> {
    let output = value
        .pointer(pointer)
        .and_then(Value::as_str)
        .ok_or_else(|| DriverError::Invalid(format!("missing string at JSON pointer {pointer}")))?;
    if output.len() != 64
        || !output
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(DriverError::Invalid(format!(
            "JSON pointer {pointer} is not a SHA-256"
        )));
    }
    Ok(output.to_owned())
}

fn pointer_u64(value: &Value, pointer: &str) -> Result<u64, DriverError> {
    value
        .pointer(pointer)
        .and_then(Value::as_u64)
        .ok_or_else(|| DriverError::Invalid(format!("missing u64 at JSON pointer {pointer}")))
}

fn cgroup_cpu_nanoseconds() -> Option<u64> {
    fs::read_to_string("/sys/fs/cgroup/cpu.stat")
        .ok()?
        .lines()
        .find_map(|line| line.strip_prefix("usage_usec "))?
        .parse::<u64>()
        .ok()
        .map(|value| value.saturating_mul(1_000))
}

fn cgroup_memory_peak_bytes() -> Option<u64> {
    fs::read_to_string("/sys/fs/cgroup/memory.peak")
        .ok()?
        .trim()
        .parse()
        .ok()
}

fn trial_cost(request: &DriverRequest, duration_ns: u64, nodes: u32) -> Result<u64, DriverError> {
    let pricing = &request.pricing;
    let operation = &request.descriptor.operation;
    let node = u128::from(pricing.node_micro_usd_per_hour)
        .saturating_mul(u128::from(nodes))
        .saturating_mul(u128::from(duration_ns))
        .div_ceil(3_600_000_000_000);
    let operations = u128::from(request.concurrency);
    let reads = u128::from(pricing.object_read_micro_usd_per_million)
        .saturating_mul(u128::from(operation.object_read_operations_per_request))
        .saturating_mul(operations)
        .div_ceil(1_000_000);
    let writes = u128::from(pricing.object_write_micro_usd_per_million)
        .saturating_mul(u128::from(operation.object_write_operations_per_request))
        .saturating_mul(operations)
        .div_ceil(1_000_000);
    let egress = u128::from(pricing.egress_micro_usd_per_gib)
        .saturating_mul(u128::from(operation.egress_bytes_per_request))
        .saturating_mul(operations)
        .div_ceil(1024 * 1024 * 1024);
    u64::try_from(node.saturating_add(reads).saturating_add(writes).saturating_add(egress))
        .map_err(|_| DriverError::Invalid("trial cost exceeds u64".to_owned()))
}

fn checked_sum(
    values: impl IntoIterator<Item = u64>,
    label: &str,
) -> Result<u64, DriverError> {
    values.into_iter().try_fold(0_u64, |total, value| {
        total
            .checked_add(value)
            .ok_or_else(|| DriverError::Invalid(format!("{label} counter overflow")))
    })
}
