//! Bounded client for the frozen NGKG 1.0 public online API.
//!
//! This crate deliberately exposes no fragment, shuffle, algebra, property-path,
//! locator, hydration, reasoner, catalog, object-store, or Kubernetes methods.

use std::{sync::Arc, time::Duration};

use bytes::BytesMut;
use futures_util::StreamExt;
use reqwest::{
    Client, Response, StatusCode,
    header::{AUTHORIZATION, CONTENT_LENGTH, CONTENT_TYPE, HeaderValue},
    redirect::Policy,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::sync::Semaphore;
use url::Url;
use uuid::Uuid;

const QUERY_EXECUTION_HEADER: &str = "x-ngkg-query-execution-id";
const REQUEST_ID_HEADER: &str = "x-request-id";

/// Resource and admission ceilings applied before contacting NGKG.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClientLimits {
    /// Maximum encoded request body size.
    pub maximum_request_bytes: usize,
    /// Maximum accepted response body size.
    pub maximum_response_bytes: usize,
    /// Maximum simultaneous upstream requests in one gateway pod.
    pub maximum_in_flight: usize,
    /// Time allowed to acquire an upstream lane.
    pub admission_timeout: Duration,
    /// TCP connect timeout.
    pub connect_timeout: Duration,
    /// Whole upstream request timeout.
    pub request_timeout: Duration,
}

impl Default for ClientLimits {
    fn default() -> Self {
        Self {
            maximum_request_bytes: 1_048_576,
            maximum_response_bytes: 67_108_864,
            maximum_in_flight: 32,
            admission_timeout: Duration::from_millis(250),
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_mins(2),
        }
    }
}

/// Strict query request matching `api/online-openapi.yaml`.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct QueryRequest {
    /// Complete SPARQL query text.
    pub query: String,
    /// Optional active-snapshot pin.
    pub snapshot_id: Option<Uuid>,
    /// Whether NGKG should hydrate qualified entities.
    pub hydrate: bool,
    /// Optional protocol-equivalent default graph override.
    #[serde(default)]
    pub default_graph_uris: Vec<String>,
    /// Optional protocol-equivalent named graph override.
    #[serde(default)]
    pub named_graph_uris: Vec<String>,
}

/// Supported SPARQL result form.
#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum QueryForm {
    /// SPARQL SELECT.
    Select,
    /// SPARQL ASK.
    Ask,
    /// SPARQL CONSTRUCT.
    Construct,
    /// SPARQL DESCRIBE.
    Describe,
}

/// Entity selected for optional payload hydration.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct QualifiedEntity {
    /// Stable result ordinal.
    pub query_ordinal: u64,
    /// Entity IRI.
    pub iri: String,
    /// NGKG entity GUID.
    pub guid: Uuid,
    /// SPARQL bag multiplicity.
    pub multiplicity: u64,
}

/// Hydrated row returned by the public query coordinator.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct PayloadRow {
    /// Result ordinal.
    pub query_ordinal: u64,
    /// SPARQL bag multiplicity.
    pub multiplicity: u64,
    /// Entity GUID.
    pub entity_guid: Uuid,
    /// RDF subject term.
    pub subject_term: String,
    /// `named_node` or `blank_node`.
    pub subject_resource_kind: String,
    /// Predicate IRI.
    pub predicate_iri: String,
    /// Literal or object lexical form.
    pub lexical_value: String,
    /// Optional datatype IRI.
    pub datatype_iri: Option<String>,
    /// Optional language tag.
    pub language: Option<String>,
    /// Source graph IRI.
    pub graph_iri: String,
}

/// Snapshot and graph routing evidence.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct RoutingEvidence {
    /// Typed selection decision.
    pub selection_mode: String,
    /// Dataset source: service, query, or protocol.
    pub dataset_selection_source: String,
    /// Default graph IRIs.
    pub default_graph_iris: Vec<String>,
    /// Named graph IRIs.
    pub named_graph_iris: Vec<String>,
    /// Active dataset hash repeated inside routing evidence.
    pub active_dataset_sha256: String,
    /// Whether internal closure was selected.
    pub include_internal_closure: bool,
    /// Exact selected graph set.
    pub selected_graph_iris: Vec<String>,
    /// Selected graph count.
    pub selected_graph_count: u32,
    /// Total authorized graph count.
    pub total_graph_count: u32,
    /// Capability-index checksum.
    pub capability_index_sha256: String,
    /// Routed dataset checksum.
    pub routed_dataset_sha256: String,
}

/// Distributed query execution evidence.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ExecutionEvidence {
    /// Execution mode.
    pub mode: String,
    /// Exchange encoding.
    pub exchange_format: String,
    /// Fragment ingress mode.
    pub fragment_ingress_mode: String,
    /// Fragment ingress bytes.
    pub fragment_ingress_bytes: u64,
    /// Fragment materialization mode.
    pub fragment_materialization_mode: String,
    /// Owned fragment rows.
    pub fragment_owned_rows: u64,
    /// Shuffle ingress mode.
    pub shuffle_result_ingress_mode: String,
    /// Shuffle ingress bytes.
    pub shuffle_result_ingress_bytes: u64,
    /// Intermediate-result mode.
    pub intermediate_result_mode: String,
    /// Owned intermediate rows.
    pub assembled_intermediate_owned_rows: u64,
    /// Fragment count.
    pub fragment_count: u32,
    /// Worker count.
    pub worker_count: u32,
    /// Shuffle partition count.
    pub shuffle_partition_count: u32,
    /// Shuffle worker count.
    pub shuffle_worker_count: u32,
    /// Shuffle spill mode.
    pub shuffle_spill_mode: String,
    /// Shuffle spill bytes.
    pub shuffle_spill_bytes: u64,
    /// Shuffle cache mode.
    pub shuffle_cache_mode: String,
    /// Shuffle cache hits.
    pub shuffle_cache_hits: u32,
    /// Worker join mode.
    pub worker_join_mode: String,
    /// Worker join spill bytes.
    pub worker_join_spill_bytes: u64,
    /// Grace hash partition count.
    pub worker_join_grace_partitions: u32,
    /// Maximum build rows observed.
    pub worker_join_max_build_rows: u64,
    /// Worker input mode.
    pub worker_input_mode: String,
    /// Worker input bytes.
    pub worker_input_bytes: u64,
    /// Coordinator request mode.
    pub coordinator_request_mode: String,
    /// Coordinator request bytes.
    pub coordinator_request_bytes: u64,
    /// Optional distributed plan checksum.
    pub plan_sha256: Option<String>,
}

/// Distributed property-path completion evidence.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct PropertyPathEvidence {
    /// Runtime mode.
    pub mode: String,
    /// Plan-set checksum.
    pub plan_set_sha256: String,
    /// Property-path count.
    pub path_count: u64,
    /// Semantic partitions.
    pub semantic_partition_count: u32,
    /// Completed frontier iterations.
    pub completed_iterations: u64,
    /// Completed work items.
    pub completed_work_items: u64,
    /// Accepted endpoint count.
    pub accepted_endpoint_count: u64,
    /// Endpoint-set checksums.
    pub endpoint_set_sha256s: Vec<String>,
    /// Scanned adjacency rows.
    pub scanned_adjacency_rows: u64,
    /// Hot-vertex split work items.
    pub hot_split_work_items: u64,
    /// Checkpoint bytes.
    pub checkpoint_bytes: u64,
    /// Participating worker IDs.
    pub worker_ids: Vec<String>,
    /// Scalar equivalence requirement.
    pub scalar_oracle_equivalence_required: bool,
    /// Completion barrier.
    pub complete: bool,
}

/// Local exact OWL 2 Direct-Semantics evidence.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ExactEntailmentEvidence {
    /// Must be `owl2-direct`.
    pub regime: String,
    /// Direct BGP count.
    pub bgp_count: u64,
    /// Exact result checksums.
    pub result_sha256s: Vec<String>,
    /// Certificate checksums.
    pub certificate_sha256s: Vec<String>,
    /// Proof-manifest checksums.
    pub proof_manifest_sha256s: Vec<String>,
    /// Typed upstream certificates are preserved as closed server-owned JSON.
    pub certificates: Vec<serde_json::Value>,
    /// Typed upstream proof manifests are preserved as closed server-owned JSON.
    pub proof_manifests: Vec<serde_json::Value>,
    /// Distributed algebra plan checksum.
    pub distributed_algebra_plan_sha256: String,
    /// Distributed algebra stages.
    pub distributed_algebra_stage_count: u64,
    /// Distributed algebra waves.
    pub distributed_algebra_wave_count: u64,
    /// Distributed algebra work items.
    pub distributed_algebra_work_item_count: u64,
    /// Distributed algebra partitions.
    pub distributed_algebra_partition_count: u32,
    /// Scalar equivalence requirement.
    pub distributed_algebra_scalar_equivalence_required: bool,
    /// Distributed path plan checksum.
    pub distributed_property_path_plan_sha256: String,
    /// Distributed path count.
    pub distributed_property_path_count: u64,
    /// Property-path automaton checksums.
    pub distributed_property_path_automaton_sha256s: Vec<String>,
    /// Property-path partitions.
    pub distributed_property_path_partition_count: u32,
    /// Scalar path equivalence requirement.
    pub distributed_property_path_scalar_equivalence_required: bool,
    /// Exact completion barrier.
    pub complete: bool,
}

/// Volatile remote SPARQL SERVICE evidence.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct FederationEvidence {
    /// Registry checksum.
    pub registry_sha256: String,
    /// SERVICE calls attempted.
    pub service_call_count: u64,
    /// Remote response bytes.
    pub response_bytes: u64,
    /// Endpoint-set checksum.
    pub endpoint_set_sha256: String,
    /// SPARQL federation completion, not local OWL certification.
    pub complete: bool,
}

/// Strict public NGKG query response.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct QueryResponse {
    /// Dataset ID.
    pub dataset_id: Uuid,
    /// Active published snapshot ID.
    pub snapshot_id: Uuid,
    /// Serving-root checksum.
    pub serving_root_sha256: String,
    /// Query-text checksum.
    pub query_sha256: String,
    /// Query form.
    pub query_form: QueryForm,
    /// Authorized graph-set checksum.
    pub authorized_graph_set_sha256: String,
    /// Active dataset checksum.
    pub active_dataset_sha256: String,
    /// Offline coverage description.
    pub coverage_scope: String,
    /// Must be true for a successful public response.
    pub complete: bool,
    /// Routing evidence.
    pub routing: RoutingEvidence,
    /// Execution evidence.
    pub execution: ExecutionEvidence,
    /// SELECT variable order.
    pub head: Vec<String>,
    /// SELECT bindings preserving SPARQL bag order.
    pub bindings: Vec<serde_json::Value>,
    /// ASK result.
    #[serde(default)]
    pub boolean_result: Option<bool>,
    /// CONSTRUCT/DESCRIBE N-Triples in result order.
    #[serde(default)]
    pub graph_ntriples: Vec<String>,
    /// Qualified entity references.
    pub qualified_entities: Vec<QualifiedEntity>,
    /// Optional payload rows.
    pub hydrated_payload: Vec<PayloadRow>,
    /// Exact entailment evidence.
    #[serde(default)]
    pub entailment: Option<ExactEntailmentEvidence>,
    /// Distributed property-path evidence.
    #[serde(default)]
    pub property_path_execution: Option<PropertyPathEvidence>,
    /// Volatile federation evidence.
    #[serde(default)]
    pub federation: Option<FederationEvidence>,
}

/// Query response plus immutable transport identity.
#[derive(Clone, Debug)]
pub struct QueryOutcome {
    /// NGKG query execution ledger ID.
    pub query_execution_id: Uuid,
    /// Validated response.
    pub response: QueryResponse,
}

/// Query-log view returned by NGKG.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct QueryLog {
    /// Query execution ID.
    pub query_execution_id: Uuid,
    /// Dataset ID.
    pub dataset_id: Uuid,
    /// Snapshot ID when terminal.
    pub snapshot_id: Option<Uuid>,
    /// Request correlation ID.
    pub request_id: String,
    /// Principal view.
    pub user: QueryLogUser,
    /// Query text when authorized.
    pub sparql_query: Option<String>,
    /// Query checksum.
    pub query_sha256: String,
    /// Query form.
    pub query_form: Option<String>,
    /// Execution mode.
    pub execution_mode: Option<String>,
    /// Runtime status.
    pub status: String,
    /// Configured allocation estimates.
    pub resources: QueryLogResources,
    /// Timing evidence.
    pub timing: QueryLogTiming,
    /// Result rows.
    pub result_rows: Option<i64>,
    /// Result bytes.
    pub result_bytes: Option<i64>,
    /// Cache hit.
    pub cache_hit: Option<bool>,
    /// Terminal error code.
    pub error_code: Option<String>,
}

/// Query-log principal.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct QueryLogUser {
    /// Authenticated principal ID.
    pub principal_id: String,
}

/// Allocation estimates in the existing NGKG query log.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct QueryLogResources {
    /// Participating configured replicas; not verified physical nodes.
    pub nodes_activated: Option<i32>,
    /// Whole cores from configured millicores.
    pub cores_activated: Option<i64>,
    /// Allocated CPU millicores.
    pub cpu_millicores: Option<i64>,
    /// Allocated RAM bytes.
    pub ram_bytes: Option<i64>,
    /// Allocated RAM GiB.
    pub ram_gib: Option<i64>,
}

/// Query-log timing.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct QueryLogTiming {
    /// Start epoch seconds.
    pub start_time_epoch: i64,
    /// Start epoch milliseconds.
    pub start_time_epoch_ms: i64,
    /// End epoch seconds.
    pub end_time_epoch: Option<i64>,
    /// End epoch milliseconds.
    pub end_time_epoch_ms: Option<i64>,
    /// Total milliseconds.
    pub total_time_ms: Option<i64>,
    /// Human-readable duration.
    pub total_time: Option<String>,
}

/// Bounded public NGKG query client.
#[derive(Clone)]
pub struct NgkgQueryClient {
    base_url: Url,
    http: Client,
    limits: ClientLimits,
    lanes: Arc<Semaphore>,
}

impl NgkgQueryClient {
    /// Build a client after validating URL and all resource ceilings.
    pub fn new(
        base_url: Url,
        limits: ClientLimits,
        allow_http_loopback: bool,
    ) -> Result<Self, ClientError> {
        validate_limits(limits)?;
        validate_base_url(&base_url, allow_http_loopback)?;
        let http = Client::builder()
            .connect_timeout(limits.connect_timeout)
            .timeout(limits.request_timeout)
            .redirect(Policy::none())
            .https_only(!allow_http_loopback)
            .build()?;
        Ok(Self {
            base_url,
            http,
            limits,
            lanes: Arc::new(Semaphore::new(limits.maximum_in_flight)),
        })
    }

    /// Execute one public enriched query and validate its immutable identity.
    pub async fn query(
        &self,
        authorization: &HeaderValue,
        dataset_id: Uuid,
        request: &QueryRequest,
        request_id: &str,
    ) -> Result<QueryOutcome, ClientError> {
        validate_authorization(authorization)?;
        validate_request(request, self.limits.maximum_request_bytes)?;
        let encoded = serde_json::to_vec(request)?;
        if encoded.len() > self.limits.maximum_request_bytes {
            return Err(ClientError::RequestTooLarge);
        }
        let _permit = tokio::time::timeout(
            self.limits.admission_timeout,
            Arc::clone(&self.lanes).acquire_owned(),
        )
        .await
        .map_err(|_| ClientError::AdmissionTimeout)?
        .map_err(|_| ClientError::Closed)?;
        let url = self.endpoint(&format!("v1/datasets/{dataset_id}/query"))?;
        let response = self
            .http
            .post(url)
            .header(AUTHORIZATION, authorization.clone())
            .header(REQUEST_ID_HEADER, request_id)
            .header(CONTENT_TYPE, "application/json")
            .body(encoded)
            .send()
            .await?;
        let headers = response.headers().clone();
        let status = response.status();
        let bytes = collect_response(response, self.limits.maximum_response_bytes).await?;
        if !status.is_success() {
            return Err(ClientError::HttpStatus {
                status,
                body: bounded_error(&bytes),
            });
        }
        require_json(&headers)?;
        let query_execution_id = header_uuid(&headers, QUERY_EXECUTION_HEADER)?;
        let decoded: QueryResponse = serde_json::from_slice(&bytes)?;
        validate_query_response(dataset_id, request, &decoded)?;
        Ok(QueryOutcome {
            query_execution_id,
            response: decoded,
        })
    }

    /// Retrieve one immutable query-log record.
    pub async fn query_log(
        &self,
        authorization: &HeaderValue,
        query_execution_id: Uuid,
        request_id: &str,
    ) -> Result<QueryLog, ClientError> {
        validate_authorization(authorization)?;
        let _permit = tokio::time::timeout(
            self.limits.admission_timeout,
            Arc::clone(&self.lanes).acquire_owned(),
        )
        .await
        .map_err(|_| ClientError::AdmissionTimeout)?
        .map_err(|_| ClientError::Closed)?;
        let url = self.endpoint(&format!("v1/query_logs/{query_execution_id}"))?;
        let response = self
            .http
            .get(url)
            .header(AUTHORIZATION, authorization.clone())
            .header(REQUEST_ID_HEADER, request_id)
            .send()
            .await?;
        let headers = response.headers().clone();
        let status = response.status();
        let bytes = collect_response(response, self.limits.maximum_response_bytes).await?;
        if !status.is_success() {
            return Err(ClientError::HttpStatus {
                status,
                body: bounded_error(&bytes),
            });
        }
        require_json(&headers)?;
        let log: QueryLog = serde_json::from_slice(&bytes)?;
        if log.query_execution_id != query_execution_id || !lower_sha256(&log.query_sha256) {
            return Err(ClientError::Evidence("query-log identity mismatch"));
        }
        Ok(log)
    }

    /// Check the public query service readiness endpoint.
    pub async fn ready(&self) -> Result<(), ClientError> {
        let url = self.endpoint("health/ready")?;
        let response = self.http.get(url).send().await?;
        if response.status() != StatusCode::NO_CONTENT {
            return Err(ClientError::HttpStatus {
                status: response.status(),
                body: String::new(),
            });
        }
        Ok(())
    }

    fn endpoint(&self, path: &str) -> Result<Url, ClientError> {
        self.base_url.join(path).map_err(ClientError::Url)
    }
}

/// Fail-closed public-client error.
#[derive(Debug, Error)]
pub enum ClientError {
    /// Invalid base URL.
    #[error("invalid NGKG base URL: {0}")]
    Url(#[from] url::ParseError),
    /// URL violates transport policy.
    #[error("NGKG base URL must be HTTPS without credentials, query, or fragment")]
    UnsafeUrl,
    /// Limits are invalid.
    #[error("NGKG client limits are invalid")]
    InvalidLimits,
    /// Bearer authorization is absent or malformed.
    #[error("NGKG bearer authorization is missing or malformed")]
    Authorization,
    /// Request violates configured bounds.
    #[error("NGKG query request violates configured bounds")]
    RequestTooLarge,
    /// No upstream lane became available.
    #[error("NGKG upstream admission timed out")]
    AdmissionTimeout,
    /// Admission pool was closed.
    #[error("NGKG upstream admission is closed")]
    Closed,
    /// HTTP client failure.
    #[error("NGKG HTTP request failed: {0}")]
    Http(#[from] reqwest::Error),
    /// Upstream returned a non-success status.
    #[error("NGKG returned HTTP {status}: {body}")]
    HttpStatus {
        /// HTTP status.
        status: StatusCode,
        /// Bounded redacted body excerpt.
        body: String,
    },
    /// Response exceeded its byte ceiling.
    #[error("NGKG response exceeds the configured byte ceiling")]
    ResponseTooLarge,
    /// Required header is missing or malformed.
    #[error("NGKG response header is missing or malformed: {0}")]
    Header(&'static str),
    /// Response content type is not JSON.
    #[error("NGKG response content type is not application/json")]
    ContentType,
    /// Response JSON does not match the frozen wire type.
    #[error("NGKG response JSON is invalid: {0}")]
    Json(#[from] serde_json::Error),
    /// Semantic identity or completion evidence is inconsistent.
    #[error("NGKG response evidence is invalid: {0}")]
    Evidence(&'static str),
}

fn validate_limits(limits: ClientLimits) -> Result<(), ClientError> {
    if limits.maximum_request_bytes == 0
        || limits.maximum_response_bytes == 0
        || limits.maximum_in_flight == 0
        || limits.admission_timeout.is_zero()
        || limits.connect_timeout.is_zero()
        || limits.request_timeout.is_zero()
    {
        return Err(ClientError::InvalidLimits);
    }
    Ok(())
}

fn validate_base_url(url: &Url, allow_http_loopback: bool) -> Result<(), ClientError> {
    let safe_https = url.scheme() == "https";
    let safe_loopback = allow_http_loopback
        && url.scheme() == "http"
        && url
            .host_str()
            .is_some_and(|host| matches!(host, "127.0.0.1" | "localhost" | "::1"));
    if (!safe_https && !safe_loopback)
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || url.path() != "/"
    {
        return Err(ClientError::UnsafeUrl);
    }
    Ok(())
}

fn validate_authorization(value: &HeaderValue) -> Result<(), ClientError> {
    value
        .to_str()
        .ok()
        .and_then(|text| text.strip_prefix("Bearer "))
        .filter(|token| !token.is_empty() && token.len() <= 8_192)
        .ok_or(ClientError::Authorization)
        .map(|_| ())
}

fn validate_request(request: &QueryRequest, maximum: usize) -> Result<(), ClientError> {
    if request.query.is_empty()
        || request.query.len() > maximum
        || request.default_graph_uris.len() > 4_096
        || request.named_graph_uris.len() > 4_096
        || request
            .default_graph_uris
            .iter()
            .chain(&request.named_graph_uris)
            .any(|value| value.is_empty() || value.len() > 8_192)
    {
        return Err(ClientError::RequestTooLarge);
    }
    Ok(())
}

fn validate_query_response(
    dataset_id: Uuid,
    request: &QueryRequest,
    response: &QueryResponse,
) -> Result<(), ClientError> {
    if response.dataset_id != dataset_id
        || request
            .snapshot_id
            .is_some_and(|snapshot| snapshot != response.snapshot_id)
        || !response.complete
        || response.routing.active_dataset_sha256 != response.active_dataset_sha256
        || response.query_sha256 != hex::encode(Sha256::digest(request.query.as_bytes()))
        || usize::try_from(response.routing.selected_graph_count).ok()
            != Some(response.routing.selected_graph_iris.len())
        || response.routing.selected_graph_count > response.routing.total_graph_count
    {
        return Err(ClientError::Evidence(
            "query identity or completion mismatch",
        ));
    }
    for hash in [
        &response.serving_root_sha256,
        &response.query_sha256,
        &response.authorized_graph_set_sha256,
        &response.active_dataset_sha256,
        &response.routing.capability_index_sha256,
        &response.routing.routed_dataset_sha256,
    ] {
        if !lower_sha256(hash) {
            return Err(ClientError::Evidence("invalid SHA-256 evidence"));
        }
    }
    if response
        .entailment
        .as_ref()
        .is_some_and(|evidence| !evidence.complete || evidence.regime != "owl2-direct")
        || response
            .property_path_execution
            .as_ref()
            .is_some_and(|evidence| !evidence.complete)
        || response
            .federation
            .as_ref()
            .is_some_and(|evidence| !evidence.complete)
    {
        return Err(ClientError::Evidence(
            "subordinate completion barrier failed",
        ));
    }
    if response
        .execution
        .plan_sha256
        .as_ref()
        .is_some_and(|value| !lower_sha256(value))
        || response
            .property_path_execution
            .as_ref()
            .is_some_and(|value| {
                !lower_sha256(&value.plan_set_sha256)
                    || value
                        .endpoint_set_sha256s
                        .iter()
                        .any(|hash| !lower_sha256(hash))
            })
        || response.entailment.as_ref().is_some_and(|value| {
            !lower_sha256(&value.distributed_algebra_plan_sha256)
                || !lower_sha256(&value.distributed_property_path_plan_sha256)
                || value
                    .result_sha256s
                    .iter()
                    .chain(&value.certificate_sha256s)
                    .chain(&value.proof_manifest_sha256s)
                    .chain(&value.distributed_property_path_automaton_sha256s)
                    .any(|hash| !lower_sha256(hash))
        })
        || response.federation.as_ref().is_some_and(|value| {
            !lower_sha256(&value.registry_sha256) || !lower_sha256(&value.endpoint_set_sha256)
        })
    {
        return Err(ClientError::Evidence(
            "invalid subordinate SHA-256 evidence",
        ));
    }
    Ok(())
}

async fn collect_response(response: Response, maximum: usize) -> Result<Vec<u8>, ClientError> {
    if response
        .headers()
        .get(CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .is_some_and(|value| value > u64::try_from(maximum).unwrap_or(u64::MAX))
    {
        return Err(ClientError::ResponseTooLarge);
    }
    let mut stream = response.bytes_stream();
    let mut output = BytesMut::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        let next = output
            .len()
            .checked_add(chunk.len())
            .ok_or(ClientError::ResponseTooLarge)?;
        if next > maximum {
            return Err(ClientError::ResponseTooLarge);
        }
        output.extend_from_slice(&chunk);
    }
    Ok(output.to_vec())
}

fn require_json(headers: &reqwest::header::HeaderMap) -> Result<(), ClientError> {
    headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .filter(|value| value.split(';').next() == Some("application/json"))
        .ok_or(ClientError::ContentType)
        .map(|_| ())
}

fn header_uuid(
    headers: &reqwest::header::HeaderMap,
    name: &'static str,
) -> Result<Uuid, ClientError> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| Uuid::parse_str(value).ok())
        .ok_or(ClientError::Header(name))
}

fn lower_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

fn bounded_error(bytes: &[u8]) -> String {
    const MAXIMUM: usize = 2_048;
    String::from_utf8_lossy(&bytes[..bytes.len().min(MAXIMUM)])
        .chars()
        .filter(|character| !character.is_control() || *character == '\n')
        .collect()
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{ClientLimits, NgkgQueryClient, lower_sha256};
    use url::Url;

    #[test]
    fn only_lowercase_sha256_is_accepted() {
        assert!(lower_sha256(&"a".repeat(64)));
        assert!(!lower_sha256(&"A".repeat(64)));
        assert!(!lower_sha256("abc"));
    }

    #[test]
    fn production_client_rejects_plain_http() -> Result<(), url::ParseError> {
        let result = NgkgQueryClient::new(
            Url::parse("http://ngkg-query.default.svc:32010/")?,
            ClientLimits::default(),
            false,
        );
        assert!(result.is_err());
        Ok(())
    }

    #[test]
    fn loopback_test_mode_is_narrow() -> Result<(), url::ParseError> {
        let limits = ClientLimits {
            request_timeout: Duration::from_secs(1),
            ..ClientLimits::default()
        };
        assert!(
            NgkgQueryClient::new(Url::parse("http://127.0.0.1:32010/")?, limits, true,).is_ok()
        );
        assert!(NgkgQueryClient::new(Url::parse("http://example.test/")?, limits, true,).is_err());
        Ok(())
    }
}
