//! Policy-controlled SPARQL 1.1 federated query execution.
//!
//! The default Oxigraph HTTP handler is intentionally not used. Every remote
//! endpoint must be present in a checksum-bound registry and is executed with
//! DNS/private-address validation, address pinning, no redirects, bounded
//! concurrency, response ceilings, and secret references resolved at startup.

use std::{
    collections::{BTreeMap, BTreeSet},
    env, fs,
    io::{Cursor, Read},
    net::{IpAddr, SocketAddr, ToSocketAddrs},
    path::Path,
    sync::{Arc, Condvar, Mutex},
    time::{Duration, Instant},
};

use oxigraph::{
    model::NamedNode,
    sparql::{
        DefaultServiceHandler, QueryEvaluationError, QuerySolutionIter,
        results::{QueryResultsFormat, QueryResultsParser, ReaderQueryResultsParserOutput},
    },
};
use oxiri::Iri;
use reqwest::{
    blocking::Client,
    header::{ACCEPT, AUTHORIZATION, CONTENT_LENGTH, CONTENT_TYPE, HeaderValue},
    redirect::Policy,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use spargebra::{Query, algebra::GraphPattern};
use thiserror::Error;
use url::Url;

/// Version of the checksum-bound endpoint registry contract.
pub const FEDERATION_REGISTRY_FORMAT_VERSION: u32 = 1;

/// A checked, non-secret description of one allowed endpoint.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct FederationEndpointConfig {
    /// Exact endpoint IRI accepted by fixed and variable SERVICE clauses.
    pub iri: String,
    /// Tenants allowed to invoke this endpoint.
    pub tenant_ids: Vec<String>,
    /// Optional environment variable containing a bearer token.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bearer_token_env: Option<String>,
}

/// Bounded remote-execution ceilings shared by all queries in one pod.
#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct FederationLimits {
    /// Maximum SERVICE calls made by a single query.
    pub max_calls_per_query: usize,
    /// Maximum concurrent remote calls in one query pod.
    pub max_concurrent_calls: usize,
    /// Maximum calls waiting for a concurrency lane.
    pub max_pending_calls: usize,
    /// Maximum time spent waiting for a lane.
    pub queue_timeout_millis: u64,
    /// TCP connection timeout.
    pub connect_timeout_millis: u64,
    /// Whole remote request timeout.
    pub request_timeout_millis: u64,
    /// Maximum response body size.
    pub max_response_bytes: usize,
}

/// On-disk endpoint allowlist.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct FederationRegistryFile {
    /// Registry contract version.
    pub format_version: u32,
    /// Global resource ceilings.
    pub limits: FederationLimits,
    /// Exact endpoint entries.
    pub endpoints: Vec<FederationEndpointConfig>,
}

#[derive(Clone)]
struct RuntimeEndpoint {
    url: Url,
    bearer_token: Option<HeaderValue>,
    tenant_ids: BTreeSet<String>,
}

/// Process-wide counters suitable for Prometheus export and HPA inputs.
#[derive(Default)]
pub struct FederationMetrics {
    pending: std::sync::atomic::AtomicU64,
    active: std::sync::atomic::AtomicU64,
    completed: std::sync::atomic::AtomicU64,
    failed: std::sync::atomic::AtomicU64,
    response_bytes: std::sync::atomic::AtomicU64,
}

/// Stable metrics snapshot.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FederationMetricsSnapshot {
    /// Calls waiting for a lane.
    pub pending: u64,
    /// Calls holding a lane.
    pub active: u64,
    /// Successfully parsed calls.
    pub completed: u64,
    /// Failed calls, including failures hidden by SERVICE SILENT.
    pub failed: u64,
    /// Accepted response bytes.
    pub response_bytes: u64,
}

impl FederationMetrics {
    /// Read all counters with relaxed ordering; they are observability only.
    #[must_use]
    pub fn snapshot(&self) -> FederationMetricsSnapshot {
        use std::sync::atomic::Ordering;
        FederationMetricsSnapshot {
            pending: self.pending.load(Ordering::Relaxed),
            active: self.active.load(Ordering::Relaxed),
            completed: self.completed.load(Ordering::Relaxed),
            failed: self.failed.load(Ordering::Relaxed),
            response_bytes: self.response_bytes.load(Ordering::Relaxed),
        }
    }
}

struct BlockingLimiter {
    maximum: usize,
    state: Mutex<usize>,
    available: Condvar,
}

struct BlockingPermit<'a>(&'a BlockingLimiter);

impl Drop for BlockingPermit<'_> {
    fn drop(&mut self) {
        if let Ok(mut active) = self.0.state.lock() {
            *active = active.saturating_sub(1);
            self.0.available.notify_one();
        }
    }
}

impl BlockingLimiter {
    fn acquire(&self, timeout: Duration) -> Result<BlockingPermit<'_>, FederationError> {
        let deadline = Instant::now() + timeout;
        let mut active = self.state.lock().map_err(|_| FederationError::LimiterPoisoned)?;
        while *active >= self.maximum {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(FederationError::QueueTimeout);
            }
            let (next, wait) = self
                .available
                .wait_timeout(active, remaining)
                .map_err(|_| FederationError::LimiterPoisoned)?;
            active = next;
            if wait.timed_out() && *active >= self.maximum {
                return Err(FederationError::QueueTimeout);
            }
        }
        *active += 1;
        Ok(BlockingPermit(self))
    }
}

/// Validated endpoint registry shared across queries in a serving pod.
#[derive(Clone)]
pub struct FederationRegistry {
    sha256: String,
    limits: FederationLimits,
    endpoints: Arc<BTreeMap<String, RuntimeEndpoint>>,
    limiter: Arc<BlockingLimiter>,
    metrics: Arc<FederationMetrics>,
}

impl FederationRegistry {
    /// Load, checksum, validate, and resolve all secret references.
    pub fn load(path: &Path, expected_sha256: &str) -> Result<Self, FederationError> {
        let bytes = fs::read(path)?;
        let observed = hex::encode(Sha256::digest(&bytes));
        if observed != expected_sha256.to_ascii_lowercase() {
            return Err(FederationError::ChecksumMismatch);
        }
        let file: FederationRegistryFile = serde_json::from_slice(&bytes)?;
        validate_limits(file.limits)?;
        if file.format_version != FEDERATION_REGISTRY_FORMAT_VERSION {
            return Err(FederationError::FormatVersion(file.format_version));
        }
        if file.endpoints.is_empty() {
            return Err(FederationError::NoEndpoints);
        }
        let mut endpoints = BTreeMap::new();
        for endpoint in file.endpoints {
            let url = validate_endpoint_url(&endpoint.iri)?;
            if endpoint.tenant_ids.is_empty()
                || endpoint
                    .tenant_ids
                    .iter()
                    .any(|value| uuid::Uuid::parse_str(value).is_err())
            {
                return Err(FederationError::InvalidTenantScope(endpoint.iri));
            }
            let bearer_token = endpoint
                .bearer_token_env
                .as_deref()
                .map(load_bearer_token)
                .transpose()?;
            if endpoints
                .insert(
                    endpoint.iri.clone(),
                    RuntimeEndpoint {
                        url,
                        bearer_token,
                        tenant_ids: endpoint.tenant_ids.into_iter().collect(),
                    },
                )
                .is_some()
            {
                return Err(FederationError::DuplicateEndpoint(endpoint.iri));
            }
        }
        Ok(Self {
            sha256: observed,
            limits: file.limits,
            endpoints: Arc::new(endpoints),
            limiter: Arc::new(BlockingLimiter {
                maximum: file.limits.max_concurrent_calls,
                state: Mutex::new(0),
                available: Condvar::new(),
            }),
            metrics: Arc::new(FederationMetrics::default()),
        })
    }

    /// Registry identity bound to request evidence and pod annotations.
    #[must_use]
    pub fn sha256(&self) -> &str {
        &self.sha256
    }

    /// Process metrics shared by all query-scoped handlers.
    #[must_use]
    pub fn metrics(&self) -> Arc<FederationMetrics> {
        Arc::clone(&self.metrics)
    }

    /// Configured outbound lane count for cgroup/thread-budget validation.
    #[must_use]
    pub const fn max_concurrent_calls(&self) -> usize {
        self.limits.max_concurrent_calls
    }

    /// Create an isolated per-query call budget over the process-wide lane pool.
    #[must_use]
    pub fn query_handler(&self, tenant_id: &str) -> FederationServiceHandler {
        FederationServiceHandler {
            registry: self.clone(),
            tenant_id: tenant_id.to_owned(),
            audit: Arc::new(Mutex::new(FederationQueryAudit::default())),
        }
    }
}

/// Complete audit for one federated query execution.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FederationQueryEvidence {
    /// Checksum-bound registry used by the query.
    pub registry_sha256: String,
    /// Number of SERVICE calls attempted.
    pub service_call_count: u64,
    /// Total response bytes accepted.
    pub response_bytes: u64,
    /// SHA-256 over the sorted exact endpoint IRIs used.
    pub endpoint_set_sha256: String,
    /// True only after the scalar evaluator consumed all SERVICE results.
    pub complete: bool,
}

#[derive(Default)]
struct FederationQueryAudit {
    calls: u64,
    response_bytes: u64,
    endpoints: BTreeSet<String>,
}

/// Query-scoped custom Oxigraph SERVICE handler.
#[derive(Clone)]
pub struct FederationServiceHandler {
    registry: FederationRegistry,
    tenant_id: String,
    audit: Arc<Mutex<FederationQueryAudit>>,
}

impl FederationServiceHandler {
    /// Produce deterministic successful-query evidence after evaluation completes.
    pub fn evidence(&self) -> Result<FederationQueryEvidence, FederationError> {
        let audit = self.audit.lock().map_err(|_| FederationError::AuditPoisoned)?;
        let mut digest = Sha256::new();
        digest.update(b"ngkg-federation-endpoint-set-v1\0");
        for endpoint in &audit.endpoints {
            digest.update(endpoint.as_bytes());
            digest.update(b"\0");
        }
        Ok(FederationQueryEvidence {
            registry_sha256: self.registry.sha256.clone(),
            service_call_count: audit.calls,
            response_bytes: audit.response_bytes,
            endpoint_set_sha256: hex::encode(digest.finalize()),
            complete: true,
        })
    }

    fn execute(
        &self,
        service_name: &NamedNode,
        pattern: &GraphPattern,
        base_iri: Option<&Iri<String>>,
    ) -> Result<QuerySolutionIter<'static>, FederationError> {
        use std::sync::atomic::Ordering;
        let endpoint_iri = service_name.as_str();
        {
            let mut audit = self.audit.lock().map_err(|_| FederationError::AuditPoisoned)?;
            if usize::try_from(audit.calls).unwrap_or(usize::MAX)
                >= self.registry.limits.max_calls_per_query
            {
                self.registry.metrics.failed.fetch_add(1, Ordering::Relaxed);
                return Err(FederationError::CallLimit);
            }
            audit.calls += 1;
            audit.endpoints.insert(endpoint_iri.to_owned());
        }
        let Some(endpoint) = self.registry.endpoints.get(endpoint_iri) else {
            self.registry.metrics.failed.fetch_add(1, Ordering::Relaxed);
            return Err(FederationError::EndpointDenied(endpoint_iri.to_owned()));
        };
        if !endpoint.tenant_ids.contains(&self.tenant_id) {
            self.registry.metrics.failed.fetch_add(1, Ordering::Relaxed);
            return Err(FederationError::EndpointDenied(endpoint_iri.to_owned()));
        }
        let pending = self.registry.metrics.pending.fetch_add(1, Ordering::Relaxed) + 1;
        if usize::try_from(pending).unwrap_or(usize::MAX)
            > self.registry.limits.max_pending_calls
        {
            self.registry.metrics.pending.fetch_sub(1, Ordering::Relaxed);
            self.registry.metrics.failed.fetch_add(1, Ordering::Relaxed);
            return Err(FederationError::PendingLimit);
        }
        let permit = self.registry.limiter.acquire(Duration::from_millis(
            self.registry.limits.queue_timeout_millis,
        ));
        self.registry.metrics.pending.fetch_sub(1, Ordering::Relaxed);
        let _permit = match permit {
            Ok(permit) => permit,
            Err(error) => {
                self.registry.metrics.failed.fetch_add(1, Ordering::Relaxed);
                return Err(error);
            }
        };
        self.registry.metrics.active.fetch_add(1, Ordering::Relaxed);
        let result = self.execute_with_lane(endpoint, pattern, base_iri);
        self.registry.metrics.active.fetch_sub(1, Ordering::Relaxed);
        match &result {
            Ok((_, bytes)) => {
                self.registry.metrics.completed.fetch_add(1, Ordering::Relaxed);
                self.registry.metrics.response_bytes.fetch_add(*bytes, Ordering::Relaxed);
                if let Ok(mut audit) = self.audit.lock() {
                    audit.response_bytes = audit.response_bytes.saturating_add(*bytes);
                }
            }
            Err(_) => {
                self.registry.metrics.failed.fetch_add(1, Ordering::Relaxed);
            }
        }
        result.map(|(solutions, _)| solutions)
    }

    fn execute_with_lane(
        &self,
        endpoint: &RuntimeEndpoint,
        pattern: &GraphPattern,
        base_iri: Option<&Iri<String>>,
    ) -> Result<(QuerySolutionIter<'static>, u64), FederationError> {
        let host = endpoint
            .url
            .host_str()
            .ok_or(FederationError::EndpointHasNoHost)?;
        let port = endpoint.url.port_or_known_default().ok_or(FederationError::EndpointHasNoPort)?;
        let addresses = resolve_public_addresses(host, port)?;
        let pinned = *addresses.first().ok_or(FederationError::DnsEmpty)?;
        let client = Client::builder()
            .connect_timeout(Duration::from_millis(
                self.registry.limits.connect_timeout_millis,
            ))
            .timeout(Duration::from_millis(
                self.registry.limits.request_timeout_millis,
            ))
            .redirect(Policy::none())
            .resolve(host, pinned)
            .build()?;
        let query = Query::Select {
            dataset: None,
            pattern: pattern.clone(),
            base_iri: base_iri.cloned(),
        }
        .to_string();
        let mut request = client
            .post(endpoint.url.clone())
            .header(CONTENT_TYPE, "application/sparql-query")
            .header(
                ACCEPT,
                "application/sparql-results+json, application/sparql-results+xml;q=0.9",
            )
            .body(query);
        if let Some(value) = &endpoint.bearer_token {
            request = request.header(AUTHORIZATION, value.clone());
        }
        let mut response = request.send()?;
        if !response.status().is_success() {
            return Err(FederationError::HttpStatus(response.status().as_u16()));
        }
        let limit = u64::try_from(self.registry.limits.max_response_bytes)
            .map_err(|_| FederationError::ResponseTooLarge)?;
        if let Some(length) = response
            .headers()
            .get(CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok())
            && length > limit
        {
            return Err(FederationError::ResponseTooLarge);
        }
        let content_type = response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .and_then(QueryResultsFormat::from_media_type)
            .ok_or(FederationError::UnsupportedResultFormat)?;
        let mut body = Vec::new();
        response.by_ref().take(limit.saturating_add(1)).read_to_end(&mut body)?;
        if body.len() > self.registry.limits.max_response_bytes {
            return Err(FederationError::ResponseTooLarge);
        }
        let byte_count = u64::try_from(body.len()).map_err(|_| FederationError::ResponseTooLarge)?;
        let ReaderQueryResultsParserOutput::Solutions(reader) =
            QueryResultsParser::from_format(content_type)
                .for_reader(Cursor::new(body))
                .map_err(|error| FederationError::Results(error.to_string()))?
        else {
            return Err(FederationError::SolutionsRequired);
        };
        let variables = reader.variables().into();
        let solutions = reader
            .map(|solution| {
                solution.map_err(|error| QueryEvaluationError::Service(Box::new(error)))
            })
            .collect::<Vec<_>>();
        Ok((QuerySolutionIter::new(variables, solutions.into_iter()), byte_count))
    }
}

impl DefaultServiceHandler for FederationServiceHandler {
    type Error = FederationError;

    fn handle(
        &self,
        service_name: &NamedNode,
        pattern: &GraphPattern,
        base_iri: Option<&Iri<String>>,
    ) -> Result<QuerySolutionIter<'static>, Self::Error> {
        self.execute(service_name, pattern, base_iri)
    }
}

/// Federation policy or transport failure.
#[derive(Debug, Error)]
pub enum FederationError {
    /// Registry file I/O.
    #[error("federation registry I/O failed: {0}")]
    Io(#[from] std::io::Error),
    /// Registry JSON.
    #[error("federation registry JSON failed: {0}")]
    Json(#[from] serde_json::Error),
    /// URL parsing.
    #[error("federation endpoint URL is invalid: {0}")]
    Url(#[from] url::ParseError),
    /// HTTP client error.
    #[error("federated HTTP request failed: {0}")]
    Http(#[from] reqwest::Error),
    /// SPARQL result parsing.
    #[error("federated SPARQL result parsing failed: {0}")]
    Results(String),
    /// Registry checksum mismatch.
    #[error("federation registry SHA-256 mismatch")]
    ChecksumMismatch,
    /// Unsupported registry version.
    #[error("unsupported federation registry version {0}")]
    FormatVersion(u32),
    /// Empty endpoint registry.
    #[error("federation registry contains no endpoints")]
    NoEndpoints,
    /// Duplicate exact endpoint.
    #[error("duplicate federation endpoint {0}")]
    DuplicateEndpoint(String),
    /// Missing or invalid endpoint tenant scope.
    #[error("federation endpoint has no valid tenant scope: {0}")]
    InvalidTenantScope(String),
    /// Endpoint not present in the allowlist.
    #[error("federation endpoint is not allowlisted: {0}")]
    EndpointDenied(String),
    /// Endpoint scheme or authority violates policy.
    #[error("federation endpoint must be an HTTPS URL without userinfo, query, or fragment")]
    UnsafeEndpoint,
    /// Environment secret reference is invalid.
    #[error("federation bearer-token environment reference is invalid or absent: {0}")]
    SecretReference(String),
    /// Limits are invalid.
    #[error("federation resource limits must all be positive and internally bounded")]
    InvalidLimits,
    /// Per-query call ceiling.
    #[error("federation call ceiling exceeded")]
    CallLimit,
    /// Pending queue ceiling.
    #[error("federation pending-call ceiling exceeded")]
    PendingLimit,
    /// Queue timeout.
    #[error("federation concurrency-lane wait timed out")]
    QueueTimeout,
    /// Synchronization was poisoned.
    #[error("federation concurrency limiter is unavailable")]
    LimiterPoisoned,
    /// Audit synchronization was poisoned.
    #[error("federation query audit is unavailable")]
    AuditPoisoned,
    /// Endpoint host absent.
    #[error("federation endpoint has no host")]
    EndpointHasNoHost,
    /// Endpoint port absent.
    #[error("federation endpoint has no effective port")]
    EndpointHasNoPort,
    /// DNS returned nothing.
    #[error("federation endpoint DNS returned no addresses")]
    DnsEmpty,
    /// DNS resolved to a forbidden network.
    #[error("federation endpoint resolved to a private, local, multicast, or reserved address: {0}")]
    ForbiddenAddress(IpAddr),
    /// Remote HTTP status.
    #[error("federation endpoint returned HTTP status {0}")]
    HttpStatus(u16),
    /// Response too large.
    #[error("federation response exceeded its byte ceiling")]
    ResponseTooLarge,
    /// Unsupported content type.
    #[error("federation endpoint did not return a supported SPARQL result media type")]
    UnsupportedResultFormat,
    /// SERVICE requires a solution sequence.
    #[error("federation endpoint returned a boolean instead of solutions")]
    SolutionsRequired,
}

fn validate_limits(limits: FederationLimits) -> Result<(), FederationError> {
    if limits.max_calls_per_query == 0
        || limits.max_concurrent_calls == 0
        || limits.max_pending_calls < limits.max_concurrent_calls
        || limits.queue_timeout_millis == 0
        || limits.connect_timeout_millis == 0
        || limits.request_timeout_millis < limits.connect_timeout_millis
        || limits.max_response_bytes == 0
    {
        return Err(FederationError::InvalidLimits);
    }
    Ok(())
}

fn validate_endpoint_url(value: &str) -> Result<Url, FederationError> {
    let url = Url::parse(value)?;
    if url.scheme() != "https"
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(FederationError::UnsafeEndpoint);
    }
    Ok(url)
}

fn load_bearer_token(variable: &str) -> Result<HeaderValue, FederationError> {
    if variable.is_empty()
        || !variable
            .bytes()
            .all(|value| value.is_ascii_uppercase() || value.is_ascii_digit() || value == b'_')
    {
        return Err(FederationError::SecretReference(variable.to_owned()));
    }
    let token = env::var(variable)
        .map_err(|_| FederationError::SecretReference(variable.to_owned()))?;
    HeaderValue::from_str(&format!("Bearer {token}"))
        .map_err(|_| FederationError::SecretReference(variable.to_owned()))
}

fn resolve_public_addresses(host: &str, port: u16) -> Result<Vec<SocketAddr>, FederationError> {
    let mut addresses = (host, port).to_socket_addrs()?.collect::<Vec<_>>();
    addresses.sort_unstable();
    addresses.dedup();
    if addresses.is_empty() {
        return Err(FederationError::DnsEmpty);
    }
    for address in &addresses {
        if forbidden_ip(address.ip()) {
            return Err(FederationError::ForbiddenAddress(address.ip()));
        }
    }
    Ok(addresses)
}

fn forbidden_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => {
            let octets = ip.octets();
            ip.is_private()
                || ip.is_loopback()
                || ip.is_link_local()
                || ip.is_broadcast()
                || ip.is_multicast()
                || ip.is_unspecified()
                || octets[0] == 0
                || octets[0] >= 240
                || (octets[0] == 100 && (64..=127).contains(&octets[1]))
                || (octets[0] == 192 && octets[1] == 0 && octets[2] == 0)
                || (octets[0] == 192 && octets[1] == 0 && octets[2] == 2)
                || (octets[0] == 198 && (octets[1] == 18 || octets[1] == 19))
                || (octets[0] == 198 && octets[1] == 51 && octets[2] == 100)
                || (octets[0] == 203 && octets[1] == 0 && octets[2] == 113)
        }
        IpAddr::V6(ip) => {
            let segments = ip.segments();
            ip.is_loopback()
                || ip.is_unspecified()
                || ip.is_multicast()
                || (segments[0] & 0xfe00) == 0xfc00
                || (segments[0] & 0xffc0) == 0xfe80
                || (segments[0] == 0x2001 && segments[1] == 0x0db8)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{FederationEndpointConfig, FederationLimits, FederationRegistryFile, forbidden_ip, validate_endpoint_url, validate_limits};
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

    #[test]
    fn private_and_reserved_networks_are_rejected() {
        assert!(forbidden_ip(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))));
        assert!(forbidden_ip(IpAddr::V4(Ipv4Addr::new(169, 254, 169, 254))));
        assert!(forbidden_ip(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1))));
        assert!(forbidden_ip(IpAddr::V6(Ipv6Addr::LOCALHOST)));
        assert!(!forbidden_ip(IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8))));
    }

    #[test]
    fn endpoint_policy_requires_credential_free_https() {
        assert!(validate_endpoint_url("https://query.example/sparql").is_ok());
        assert!(validate_endpoint_url("http://query.example/sparql").is_err());
        assert!(validate_endpoint_url("https://user:secret@query.example/sparql").is_err());
        assert!(validate_endpoint_url("https://query.example/sparql?api_key=secret").is_err());
        assert!(validate_endpoint_url("https://query.example/sparql#fragment").is_err());
    }

    #[test]
    fn registry_contract_is_strict_and_serializable() {
        let limits = FederationLimits {
            max_calls_per_query: 32,
            max_concurrent_calls: 8,
            max_pending_calls: 16,
            queue_timeout_millis: 500,
            connect_timeout_millis: 1_000,
            request_timeout_millis: 5_000,
            max_response_bytes: 1_048_576,
        };
        assert!(validate_limits(limits).is_ok());
        let file = FederationRegistryFile {
            format_version: 1,
            limits,
            endpoints: vec![FederationEndpointConfig {
                iri: "https://query.example/sparql".to_owned(),
                tenant_ids: vec!["00000000-0000-0000-0000-000000000001".to_owned()],
                bearer_token_env: Some("NGKG_FEDERATION_EXAMPLE_TOKEN".to_owned()),
            }],
        };
        assert!(serde_json::to_vec(&file).is_ok());
    }
}
