//! Certified online query, locator, and Parquet hydration replicas.

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    env,
    fs::{self, File, OpenOptions},
    io::{BufReader, BufWriter, Cursor, Read, Seek, SeekFrom, Write},
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{
        Arc, OnceLock,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use axum::middleware::Next;
use axum::{
    Json, Router,
    body::{Body, to_bytes},
    extract::{DefaultBodyLimit, Path as AxumPath, Query as AxumQuery, RawQuery, Request, State},
    http::{
        HeaderMap, HeaderValue, StatusCode,
        header::{ACCEPT, CONTENT_LENGTH, CONTENT_TYPE, VARY},
    },
    middleware,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt, stream};
use ngkg_artifact_store::{ArtifactStore, ArtifactStoreError};
use ngkg_catalog::{
    ActiveServingSnapshot, BeginQueryExecutionLog, CatalogError, DistributedServingRoot,
    FinalizeQueryExecutionLog, OperationRepository, QueryExecutionLogFilter,
    QueryExecutionLogRecord,
};
use ngkg_dataset::{restrict_resolved_dataset_to_roles, validate_resolved_dataset};
use ngkg_direct_reasoner::{
    DirectExactAdapter, DirectExactBindings, DirectExactLimits, DirectExactOntologyBundle,
    prepare_exact_direct_bgp_requests,
};
use ngkg_federation::{FederationQueryEvidence, FederationRegistry};
use ngkg_grace_join::{GraceJoinEngine, GraceJoinError, GraceJoinIdentity, GraceJoinSide};
use ngkg_hpc_runtime::{
    CapabilityReport, ResourceEnvelopeReport, ThreadBudget, capability_report,
    resource_envelope_report,
};
use ngkg_hydration::{
    HydratedShardRow, HydrationError, ServingRootManifest, ShardedQualifiedGuid,
    VerifiedPayloadShard, hydrate_sharded_payload_for_graphs, verify_payload_shard,
};
use ngkg_identity::{IdentityError, guid_for_canonical_iri};
use ngkg_locator::{LocatorError, MmapLocatorIndex, ShardLocatorRecord};
use ngkg_native_runtime::{
    LeafExecutionMode, LeafPredicate, LeafScanLimits, LeafScanResult, NativeCutoverMode,
    scan_verified_parquet_leaf,
};
use ngkg_online_reasoning::{
    CoverageState, EntailmentRoute, EntailmentRoutingInput, build_distributed_reasoner_plan,
    complete_distributed_exact_bgp, dispatch_exact_partitions_with_retry, route_entailment,
    substitute_exact_bgp_results,
};
use ngkg_owl_direct::{DirectBgpClassificationLimits, OwlSignatureIndex, classify_direct_bgps};
use ngkg_query_cache::{
    QUERY_CACHE_HEADER_BYTES, QueryCacheError, QueryCacheKey, QueryCacheLookup, QueryResultCache,
};
use ngkg_query_executor::{
    ARROW_STREAM_EOS, ARROW_STREAM_MEDIA_TYPE, AdjacencyArtifactIdentity, ExecutionError,
    FragmentBatchMetadata, FragmentBindingStream, PartitionAdjacencyIndex, PartitionPathBatch,
    PathEndpoint, PathFrontierKey, PathGraphScope, ShuffleJoinMetadata, ShuffleJoinStream,
    ShuffleJoinStreamHeader, complete_path_iteration, distinct_sparql_json,
    execute_partition_path_batch, global_slice_sparql_json, inner_join_sparql_json,
    lookup_dictionary_id_optional, lookup_dictionary_ids_available, minus_sparql_json,
    project_sparql_json, shuffle_partition_for_binding, union_sparql_json,
    write_checkpoint_atomic, write_fragment_arrow_stream, write_shuffle_join_stream_iter,
};
use ngkg_query_planner::{
    AlgebraExecutionLane, DistributedAlgebraLimits, DistributedAlgebraOperator,
    DistributedPropertyPathLimits, DistributedPropertyPathPlan, algebra_execution_waves,
};
use ngkg_reference::{
    CERTIFIED_QUERY_RESULT_HASH_VERSION, CertifiedFragmentRuntime, CertifiedQueryExecutionLimits,
    CertifiedSemanticResult, CertifiedSemanticRuntime, DatasetError, DatasetSelectionSource,
    DistributedQueryCertificate, DistributedQueryPlanFile, GraphCapabilityIndexFile, GraphCatalog,
    LogicalGraphName, OwlSignature, ProtocolDatasetSpecification, QueryDatasetSpecification,
    QueryRoutingCertificate, ReferenceRuntimeError, ReferenceSnapshotManifest, ResolvedDataset,
    build_direct_active_ontology_bundle, canonical_query_payload_sha256,
    canonical_sparql_multiset_sha256, resolve_dataset, sha256_path,
};
use ngkg_shuffle_cache::{CacheLookup, ShuffleCacheError, ShuffleCacheKey, ShuffleResultCache};
use ngkg_sparql_compiler::{
    CompiledSparqlQuery, QueryForm, SPARQL_ALGEBRA_FORMAT_VERSION, SparqlCompileError,
};
use ngkg_types::{
    DIRECT_BGP_CLASSIFIER_V1, DirectBgpCompleteness, DirectBgpExactness, DirectBgpGraphContext,
    DirectBgpLegalityReport, DirectBgpOutcome, DirectBgpRdfTerm, DirectBgpResult, DirectBgpScope,
    DirectBgpSolution, DirectBgpStatus, DirectCertificate, DirectProofManifest, EntailmentRegime,
    direct_bgp_result_sha256, validate_direct_bgp_legality_report,
};
use oxigraph::{
    io::{RdfFormat, RdfParser, RdfSerializer},
    model::{BlankNode, Literal, NamedNode, Term, Variable},
    sparql::{
        CancellationToken,
        results::{QueryResultsFormat, QueryResultsSerializer},
    },
};
use reqwest::{Client as HttpClient, Response as HttpResponse};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use spargebra::{Query as SparqlQuery, algebra::GraphPattern};
use sqlx::postgres::PgPoolOptions;
use thiserror::Error;
use tokio::io::AsyncWriteExt;
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore, TryAcquireError, mpsc};
use tower_http::trace::TraceLayer;
use utoipa_swagger_ui::{Config as SwaggerConfig, serve as serve_swagger_ui};
use uuid::Uuid;

mod auth;
mod phase40_limits;
mod tenant_admission;

use auth::{AuthError, Identity, TokenAuthorizer, bearer};
use phase40_limits::TrustedPhase40AdmissionCeilings;
use tenant_admission::TenantAdmissionRegistry;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Role {
    Query,
    Fragment,
    Locator,
    Hydration,
}

#[derive(Clone)]
struct AppState {
    role: Role,
    authorizer: Arc<TokenAuthorizer>,
    manager: Arc<ServingStateManager>,
    http: HttpClient,
    fragment_http: HttpClient,
    reasoner_http: HttpClient,
    hydration_url: Option<String>,
    fragment_service: Option<String>,
    max_request_bytes: usize,
    max_query_bytes: usize,
    max_query_response_bytes: usize,
    max_query_result_rows: usize,
    max_query_graph_triples: usize,
    max_query_graph_blank_nodes: usize,
    query_timeout: Duration,
    max_qualified_entities: usize,
    max_hydration_rows: u64,
    max_hydration_response_bytes: usize,
    hydration_worker_threads: usize,
    max_distributed_fragments: usize,
    max_distributed_intermediate_rows: usize,
    max_distributed_exchange_bytes: usize,
    max_fragment_response_bytes: usize,
    fragment_response_spool: Option<Arc<FragmentResponseSpool>>,
    fragment_arrow_batch_rows: usize,
    fragment_arrow_http_chunk_bytes: usize,
    fragment_arrow_channel_capacity: usize,
    fragment_exchange_concurrency: usize,
    distributed_algebra_enabled: bool,
    distributed_algebra_replicas: usize,
    native_cutover_mode: NativeCutoverMode,
    shuffle_partition_count: u32,
    max_shuffle_request_bytes: usize,
    shuffle_request_spool: Option<Arc<StreamingRequestSpool>>,
    max_shuffle_response_bytes: usize,
    max_shuffle_exchange_bytes: usize,
    shuffle_exchange_concurrency: usize,
    shuffle_spill_root: Option<PathBuf>,
    max_shuffle_spill_bytes: u64,
    max_shuffle_open_files: usize,
    property_path_max_iterations: u32,
    property_path_max_frontier_items: u64,
    property_path_max_visited_items: u64,
    property_path_max_checkpoint_bytes: u64,
    property_path_max_spill_bytes: u64,
    property_path_hot_vertex_degree: u64,
    property_path_max_hot_vertex_splits: u32,
    partition_native_paths_enabled: bool,
    property_path_worker_threads: usize,
    property_path_max_scan_rows: u64,
    property_path_core_lanes: Arc<Semaphore>,
    shuffle_result_cache: Option<Arc<ShuffleResultCache>>,
    shuffle_cache_flights: Arc<Mutex<BTreeMap<String, Arc<Mutex<()>>>>>,
    max_shuffle_cache_entry_bytes: usize,
    grace_join_engine: Option<Arc<GraceJoinEngine>>,
    worker_join_bucket_count: u32,
    max_worker_join_build_rows: usize,
    in_memory_join_build_rows: usize,
    query_result_cache: Option<Arc<QueryResultCache>>,
    query_cache_flights: Arc<Mutex<BTreeMap<String, Arc<Mutex<()>>>>>,
    max_query_cache_entry_bytes: usize,
    admission: Arc<AdmissionController>,
    worker_id: String,
    standards_features: StandardsFeatureGates,
    direct_bgp_classification_limits: DirectBgpClassificationLimits,
    phase40_admission_ceiling_sha256: String,
    online_direct: Option<Arc<OnlineDirectConfig>>,
    federation: Option<Arc<FederationRegistry>>,
    query_logs: QueryLogConfig,
    runtime_capabilities: CapabilityReport,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct HpcParquetCapabilities {
    projected_columns: bool,
    bounded_arrow_batches: bool,
    deterministic_rank_receipts: bool,
    execution_modes: Vec<&'static str>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct HpcMpiCapabilities {
    online_query_participant: bool,
    finite_batch_supported: bool,
    one_rank_per_pod: bool,
    elastic_rank_resize: bool,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct HpcCapabilitiesResponse {
    format_version: u32,
    role: String,
    worker_id: String,
    local: CapabilityReport,
    memory: ResourceEnvelopeReport,
    parquet: HpcParquetCapabilities,
    mpi: HpcMpiCapabilities,
    autoscaling_target_percent: u8,
}

#[derive(Clone, Copy, Debug)]
struct QueryLogConfig {
    store_query_text: bool,
    max_page_size: usize,
    coordinator_cpu_millis: u64,
    coordinator_memory_bytes: u64,
    fragment_cpu_millis: u64,
    fragment_memory_bytes: u64,
    hydration_cpu_millis: u64,
    hydration_memory_bytes: u64,
}

#[derive(Clone)]
struct OnlineDirectConfig {
    worker_base_urls: Vec<String>,
    bearer_token: String,
    ontology_root: PathBuf,
    work_root: PathBuf,
    dispatch_concurrency: usize,
    dispatch_attempts: usize,
    max_partition_response_bytes: usize,
    limits: DirectExactLimits,
    adapter: DirectExactAdapter,
}

/// Standards declarations are release gates, not optimistic configuration.
/// Every value defaults to false and is advertised only after its independent
/// conformance gate has been completed for the deployed build and dataset.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct StandardsFeatureGates {
    sparql_11_query: bool,
    union_default_graph: bool,
    owl_direct: bool,
    owl_dl: bool,
}

#[derive(Clone, Copy, Debug)]
enum AdmissionClass {
    Query = 0,
    Fragment = 1,
    Shuffle = 2,
    Locator = 3,
    Hydration = 4,
}

impl AdmissionClass {
    const ALL: [Self; 5] = [
        Self::Query,
        Self::Fragment,
        Self::Shuffle,
        Self::Locator,
        Self::Hydration,
    ];

    const fn index(self) -> usize {
        match self {
            Self::Query => 0,
            Self::Fragment => 1,
            Self::Shuffle => 2,
            Self::Locator => 3,
            Self::Hydration => 4,
        }
    }

    const fn label(self) -> &'static str {
        match self {
            Self::Query => "query",
            Self::Fragment => "fragment",
            Self::Shuffle => "shuffle",
            Self::Locator => "locator",
            Self::Hydration => "hydration",
        }
    }
}

#[derive(Default)]
struct AdmissionCounters {
    admitted: AtomicU64,
    rejected: AtomicU64,
    in_flight: AtomicU64,
    completed: AtomicU64,
    failed: AtomicU64,
    wait_nanoseconds: AtomicU64,
    service_nanoseconds: AtomicU64,
}

struct AdmissionController {
    semaphores: [Arc<Semaphore>; 5],
    limits: [usize; 5],
    pending_semaphores: [Arc<Semaphore>; 5],
    pending_limits: [usize; 5],
    fragment_worker: Arc<Semaphore>,
    fragment_worker_limit: usize,
    tenants: TenantAdmissionRegistry,
    wait: Duration,
    counters: [AdmissionCounters; 5],
    cache_hits: AtomicU64,
    cache_misses: AtomicU64,
    cache_invalid: AtomicU64,
    cache_errors: AtomicU64,
    query_cache_hits: AtomicU64,
    query_cache_misses: AtomicU64,
    query_cache_invalid: AtomicU64,
    query_cache_errors: AtomicU64,
    worker_join_in_memory: AtomicU64,
    worker_join_grace: AtomicU64,
    worker_join_spill_bytes: AtomicU64,
    coordinator_streamed_requests: AtomicU64,
    coordinator_streamed_bytes: AtomicU64,
    coordinator_direct_partition_fragments: AtomicU64,
    coordinator_direct_partition_rows: AtomicU64,
    coordinator_spooled_shuffle_responses: AtomicU64,
    coordinator_spooled_shuffle_response_bytes: AtomicU64,
    property_path_pending_work_items: AtomicU64,
    property_path_active_frontier_items: AtomicU64,
    property_path_checkpoint_bytes: AtomicU64,
    tenant_rejections: [AtomicU64; 5],
}

struct AdmissionLease {
    controller: Arc<AdmissionController>,
    class: AdmissionClass,
    _permits: Vec<OwnedSemaphorePermit>,
    started: Instant,
    failed: bool,
}

struct PropertyPathMetricLease {
    controller: Arc<AdmissionController>,
    pending: u64,
    frontier: u64,
    checkpoint: u64,
}

impl PropertyPathMetricLease {
    fn new(controller: Arc<AdmissionController>) -> Self {
        Self {
            controller,
            pending: 0,
            frontier: 0,
            checkpoint: 0,
        }
    }

    fn set_pending(&mut self, value: u64) {
        replace_metric_contribution(
            &self.controller.property_path_pending_work_items,
            &mut self.pending,
            value,
        );
    }

    fn set_frontier(&mut self, value: u64) {
        replace_metric_contribution(
            &self.controller.property_path_active_frontier_items,
            &mut self.frontier,
            value,
        );
    }

    fn add_checkpoint(&mut self, value: u64) -> Result<(), OnlineError> {
        let next = self
            .checkpoint
            .checked_add(value)
            .ok_or_else(|| OnlineError::Request("checkpoint metric overflow".to_owned()))?;
        replace_metric_contribution(
            &self.controller.property_path_checkpoint_bytes,
            &mut self.checkpoint,
            next,
        );
        Ok(())
    }
}

impl Drop for PropertyPathMetricLease {
    fn drop(&mut self) {
        self.controller
            .property_path_pending_work_items
            .fetch_sub(self.pending, Ordering::Relaxed);
        self.controller
            .property_path_active_frontier_items
            .fetch_sub(self.frontier, Ordering::Relaxed);
        self.controller
            .property_path_checkpoint_bytes
            .fetch_sub(self.checkpoint, Ordering::Relaxed);
    }
}

fn replace_metric_contribution(metric: &AtomicU64, current: &mut u64, value: u64) {
    if value >= *current {
        metric.fetch_add(value - *current, Ordering::Relaxed);
    } else {
        metric.fetch_sub(*current - value, Ordering::Relaxed);
    }
    *current = value;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AdmissionScope {
    Global,
    Tenant,
}

impl AdmissionScope {
    const fn label(self) -> &'static str {
        match self {
            Self::Global => "global",
            Self::Tenant => "tenant",
        }
    }
}

enum AdmissionFailure {
    TimedOut(AdmissionScope),
    Closed(AdmissionScope),
    UnknownTenant,
}

enum SemaphoreAcquireFailure {
    TimedOut,
    Closed,
}

impl SemaphoreAcquireFailure {
    const fn with_scope(self, scope: AdmissionScope) -> AdmissionFailure {
        match self {
            Self::TimedOut => AdmissionFailure::TimedOut(scope),
            Self::Closed => AdmissionFailure::Closed(scope),
        }
    }
}

struct ServingStateManager {
    catalog: OperationRepository,
    store: ArtifactStore,
    cache_root: PathBuf,
    max_object_bytes: u64,
    max_payload_cache_bytes: u64,
    max_resident_query_routes: usize,
    max_resident_fragment_runtimes: usize,
    authorization: Mutex<BTreeMap<(Uuid, Uuid), Arc<GraphAuthorizationState>>>,
    semantic: Mutex<BTreeMap<(Uuid, Uuid), Arc<SemanticState>>>,
    physical: Mutex<BTreeMap<(Uuid, Uuid), Arc<PhysicalState>>>,
    authorization_loads: Mutex<BTreeMap<(Uuid, Uuid), Arc<Mutex<()>>>>,
    semantic_loads: Mutex<BTreeMap<(Uuid, Uuid), Arc<Mutex<()>>>>,
    physical_loads: Mutex<BTreeMap<(Uuid, Uuid), Arc<Mutex<()>>>>,
}

struct SemanticState {
    active: ActiveServingSnapshot,
    manifest: Arc<ReferenceSnapshotManifest>,
    manifest_path: PathBuf,
    query_dataset_path: PathBuf,
    query_dataset_sha256: String,
    closure_path: PathBuf,
    capability_index_sha256: String,
    owl_signature: Arc<OwlSignatureIndex>,
    owl_signature_document: Arc<OwlSignature>,
    owl_signature_sha256: String,
    datatype_policy_sha256: String,
    owl_profile_qualification_sha256: String,
    owl_consistency_qualification_sha256: String,
    graph_catalog: Arc<GraphCatalog>,
    full_runtime: Mutex<Option<Arc<CertifiedSemanticRuntime>>>,
    full_runtime_load: Arc<Mutex<()>>,
    runtimes: Mutex<BoundedLruCache<CertifiedSemanticRuntime>>,
    runtime_loads: Mutex<BTreeMap<String, Arc<Mutex<()>>>>,
    fragment_runtimes: Mutex<BoundedLruCache<CertifiedFragmentRuntime>>,
    fragment_runtime_loads: Mutex<BTreeMap<String, Arc<Mutex<()>>>>,
    distributed_plans: Mutex<BoundedLruCache<DistributedQueryPlanFile>>,
    distributed_plan_loads: Mutex<BTreeMap<String, Arc<Mutex<()>>>>,
}

struct SemanticPartitionFiles {
    semantic_root_sha256: String,
    partition_manifest_sha256: String,
    facts_path: PathBuf,
    facts_sha256: String,
    facts_bytes: u64,
    facts_rows: u64,
    forward: AdjacencyArtifactIdentity,
    reverse: AdjacencyArtifactIdentity,
    dictionary_path: PathBuf,
    dictionary_sha256: String,
}

/// Read-plane view of a checksum-bound semantic compilation root. Unknown
/// compiler evidence remains covered by the root digest but is intentionally
/// not linked into the latency-sensitive online binary.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SemanticCompilationRootView {
    format_version: u32,
    tenant_id: Uuid,
    dataset_id: Uuid,
    snapshot_id: Uuid,
    dictionary_manifest_path: String,
    dictionary_manifest_sha256: String,
    logical_partitions: u32,
    partitions: Vec<SemanticPartitionReferenceView>,
    fact_count: u64,
    edge_count: u64,
    semantic_content_sha256: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SemanticPartitionReferenceView {
    partition_index: u32,
    partition_id: String,
    manifest_path: String,
    manifest_sha256: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SemanticPartitionManifestView {
    format_version: u32,
    dataset_id: Uuid,
    snapshot_id: Uuid,
    dictionary_sha256: String,
    partition_index: u32,
    partition_id: String,
    fact_count: u64,
    edge_count: u64,
    artifacts: Vec<SemanticRunArtifactView>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SemanticRunArtifactView {
    relative_path: String,
    sha256: String,
    bytes: u64,
    row_count: u64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SemanticDictionaryManifestView {
    format_version: u32,
    dataset_id: Uuid,
    snapshot_id: Uuid,
    dictionary_sha256: String,
}

struct GraphAuthorizationState {
    active: ActiveServingSnapshot,
    graph_catalog: Arc<GraphCatalog>,
}

struct AuthorizedQueryGraphs {
    graph_set_sha256: String,
    graph_iris: BTreeSet<String>,
}

struct BoundedLruCache<T> {
    entries: BTreeMap<String, Arc<T>>,
    least_to_most_recent: VecDeque<String>,
}

struct BoundedBuffer {
    bytes: Vec<u8>,
    max_bytes: usize,
}

const REQUEST_SPOOL_MARKER: &str = ".ngkg-request-spool-v1";
const REQUEST_SPOOL_MARKER_BYTES: &[u8] = b"ngkg-streaming-request-spool-v1\n";
const FRAGMENT_RESPONSE_SPOOL_MARKER: &str = ".ngkg-fragment-response-spool-v1";
const FRAGMENT_RESPONSE_SPOOL_MARKER_BYTES: &[u8] = b"ngkg-fragment-response-spool-v1\n";

/// Process-wide, fail-closed local-NVMe budget for streamed worker requests.
struct StreamingRequestSpool {
    root: PathBuf,
    max_active_bytes: u64,
    active_bytes: AtomicU64,
}

/// One checksum-verified request file. Drop removes it and releases its reservation.
struct StreamingRequestLease {
    owner: Arc<StreamingRequestSpool>,
    path: PathBuf,
    bytes: u64,
    sha256: String,
    released: bool,
}

/// Process-wide local-NVMe budget for certified fragment response streams.
struct FragmentResponseSpool {
    root: PathBuf,
    max_active_bytes: u64,
    active_bytes: AtomicU64,
}

/// Immutable response body retained until its certified rows are decoded.
struct FragmentResponseLease {
    owner: Arc<FragmentResponseSpool>,
    path: PathBuf,
    bytes: u64,
    sha256: [u8; 32],
    released: bool,
}

/// A fully consumed fragment stream whose encoded rows remain on local NVMe
/// until a following physical operator consumes them.
struct ValidatedFragmentSpool {
    lease: FragmentResponseLease,
    metadata: FragmentBatchMetadata,
    head: Vec<String>,
    row_count: u64,
    always_bound_variables: BTreeSet<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct FragmentBindingSummary {
    head: Vec<String>,
    always_bound_variables: BTreeSet<String>,
}

struct ShuffleSpoolRelation {
    spools: Vec<ValidatedFragmentSpool>,
    original_fragment_count: u64,
    original_fragment_rows: u64,
}

struct FragmentReplay {
    lease: FragmentResponseLease,
    stream: FragmentBindingStream<BufReader<File>>,
    expected_rows: u64,
    observed_rows: u64,
}

struct ValidatedFragmentSpoolSequence {
    pending: VecDeque<ValidatedFragmentSpool>,
    current: Option<FragmentReplay>,
    max_rows: usize,
}

const SPILL_MAGIC: &[u8; 8] = b"NGKGSP25";
const SPILL_HEADER_BYTES: usize = 85;
const SPILL_ROOT_MARKER: &str = ".ngkg-shuffle-root-v1";

#[derive(Clone)]
struct SpillIdentity {
    dataset_id: Uuid,
    snapshot_id: Uuid,
    query_sha256: [u8; 32],
    stage: u32,
    partition_count: u32,
}

#[derive(Clone)]
struct SpillPartition {
    path: PathBuf,
    side: u8,
    partition: u32,
    rows: usize,
    bytes: u64,
    sha256: [u8; 32],
}

struct ShuffleSpillStage {
    root: PathBuf,
    cleaned: bool,
    identity: SpillIdentity,
    left: Vec<SpillPartition>,
    right: Vec<SpillPartition>,
    total_bytes: u64,
}

struct SpillWriter {
    writer: BufWriter<File>,
    hasher: Sha256,
    path: PathBuf,
    side: u8,
    partition: u32,
    rows: usize,
    bytes: u64,
}

struct SpillPartitionReader {
    reader: BufReader<File>,
    rows_remaining: usize,
    declared_rows: usize,
    expected_bytes: u64,
    bytes_read: u64,
    expected_sha256: [u8; 32],
    hasher: Sha256,
    key_variables: Vec<String>,
    partition: u32,
    partition_count: u32,
    terminal: bool,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct CachedShuffleResult {
    format_version: u32,
    output_head: Vec<String>,
    bindings: Vec<serde_json::Value>,
    multiset_sha256: String,
    join_mode: String,
    join_spill_bytes: u64,
    join_bucket_count: u32,
    join_max_build_rows: u64,
}

struct ValidatedShuffleSpool {
    header: ShuffleJoinStreamHeader,
    left_stream_sha256: String,
    right_stream_sha256: String,
}

#[derive(Clone, Copy, Debug, Default)]
struct WorkerJoinSummary {
    grace_partitions: u32,
    spill_bytes: u64,
    max_build_rows: u64,
    streamed_input_bytes: u64,
}

struct ShuffleCacheFlightLease {
    registry: Arc<Mutex<BTreeMap<String, Arc<Mutex<()>>>>>,
    digest: String,
    flight: Arc<Mutex<()>>,
}

struct QueryCacheFlightLease {
    registry: Arc<Mutex<BTreeMap<String, Arc<Mutex<()>>>>>,
    digest: String,
    flight: Arc<Mutex<()>>,
}

impl AdmissionController {
    fn new(
        limits: [usize; 5],
        pending_limits: [usize; 5],
        fragment_worker_limit: usize,
        tenants: TenantAdmissionRegistry,
        wait: Duration,
    ) -> Self {
        Self {
            semaphores: std::array::from_fn(|index| Arc::new(Semaphore::new(limits[index]))),
            limits,
            pending_semaphores: std::array::from_fn(|index| {
                Arc::new(Semaphore::new(pending_limits[index]))
            }),
            pending_limits,
            fragment_worker: Arc::new(Semaphore::new(fragment_worker_limit)),
            fragment_worker_limit,
            tenants,
            wait,
            counters: std::array::from_fn(|_| AdmissionCounters::default()),
            cache_hits: AtomicU64::new(0),
            cache_misses: AtomicU64::new(0),
            cache_invalid: AtomicU64::new(0),
            cache_errors: AtomicU64::new(0),
            query_cache_hits: AtomicU64::new(0),
            query_cache_misses: AtomicU64::new(0),
            query_cache_invalid: AtomicU64::new(0),
            query_cache_errors: AtomicU64::new(0),
            worker_join_in_memory: AtomicU64::new(0),
            worker_join_grace: AtomicU64::new(0),
            worker_join_spill_bytes: AtomicU64::new(0),
            coordinator_streamed_requests: AtomicU64::new(0),
            coordinator_streamed_bytes: AtomicU64::new(0),
            coordinator_direct_partition_fragments: AtomicU64::new(0),
            coordinator_direct_partition_rows: AtomicU64::new(0),
            coordinator_spooled_shuffle_responses: AtomicU64::new(0),
            coordinator_spooled_shuffle_response_bytes: AtomicU64::new(0),
            property_path_pending_work_items: AtomicU64::new(0),
            property_path_active_frontier_items: AtomicU64::new(0),
            property_path_checkpoint_bytes: AtomicU64::new(0),
            tenant_rejections: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }

    async fn acquire(
        self: &Arc<Self>,
        class: AdmissionClass,
        tenant_id: Uuid,
    ) -> Result<AdmissionLease, AdmissionFailure> {
        let started_wait = Instant::now();
        let deadline = started_wait + self.wait;
        let tenant = self
            .tenants
            .lanes(tenant_id)
            .ok_or(AdmissionFailure::UnknownTenant)?;
        let tenant_pending = match tenant.pending(class).try_acquire_owned() {
            Ok(permit) => permit,
            Err(TryAcquireError::NoPermits) => {
                return Err(AdmissionFailure::TimedOut(AdmissionScope::Tenant));
            }
            Err(TryAcquireError::Closed) => {
                return Err(AdmissionFailure::Closed(AdmissionScope::Tenant));
            }
        };
        let global_pending =
            match Arc::clone(&self.pending_semaphores[class.index()]).try_acquire_owned() {
                Ok(permit) => permit,
                Err(TryAcquireError::NoPermits) => {
                    return Err(AdmissionFailure::TimedOut(AdmissionScope::Global));
                }
                Err(TryAcquireError::Closed) => {
                    return Err(AdmissionFailure::Closed(AdmissionScope::Global));
                }
            };
        let mut permits = Vec::with_capacity(4);
        permits.push(
            acquire_before(tenant.execution(class), deadline)
                .await
                .map_err(|failure| failure.with_scope(AdmissionScope::Tenant))?,
        );
        if matches!(class, AdmissionClass::Fragment | AdmissionClass::Shuffle) {
            permits.push(
                acquire_before(tenant.fragment_worker(), deadline)
                    .await
                    .map_err(|failure| failure.with_scope(AdmissionScope::Tenant))?,
            );
        }
        permits.push(
            acquire_before(Arc::clone(&self.semaphores[class.index()]), deadline)
                .await
                .map_err(|failure| failure.with_scope(AdmissionScope::Global))?,
        );
        if matches!(class, AdmissionClass::Fragment | AdmissionClass::Shuffle) {
            permits.push(
                acquire_before(Arc::clone(&self.fragment_worker), deadline)
                    .await
                    .map_err(|failure| failure.with_scope(AdmissionScope::Global))?,
            );
        }
        drop(tenant_pending);
        drop(global_pending);
        let counters = &self.counters[class.index()];
        counters.admitted.fetch_add(1, Ordering::Relaxed);
        counters.in_flight.fetch_add(1, Ordering::Relaxed);
        add_duration(&counters.wait_nanoseconds, started_wait.elapsed());
        Ok(AdmissionLease {
            controller: Arc::clone(self),
            class,
            _permits: permits,
            started: Instant::now(),
            failed: false,
        })
    }

    fn reject(&self, class: AdmissionClass, scope: AdmissionScope, waited: Duration) {
        let counters = &self.counters[class.index()];
        counters.rejected.fetch_add(1, Ordering::Relaxed);
        if scope == AdmissionScope::Tenant {
            self.tenant_rejections[class.index()].fetch_add(1, Ordering::Relaxed);
        }
        add_duration(&counters.wait_nanoseconds, waited);
    }

    fn render(
        &self,
        role: Role,
        query_cache: Option<&QueryResultCache>,
        shuffle_cache: Option<&ShuffleResultCache>,
        grace_join: Option<&GraceJoinEngine>,
    ) -> String {
        let mut output = String::from(
            "# HELP ngkg_admission_limit Configured request concurrency ceiling.\n\
# TYPE ngkg_admission_limit gauge\n\
# HELP ngkg_admission_in_flight Requests admitted and not fully transmitted.\n\
# TYPE ngkg_admission_in_flight gauge\n\
# HELP ngkg_admission_pending_limit Maximum requests allowed to wait for an execution lane.\n\
# TYPE ngkg_admission_pending_limit gauge\n\
# HELP ngkg_admission_pending Requests currently waiting for an execution lane.\n\
# TYPE ngkg_admission_pending gauge\n\
# HELP ngkg_admission_admitted_total Requests admitted for execution.\n\
# TYPE ngkg_admission_admitted_total counter\n\
# HELP ngkg_admission_rejected_total Requests rejected after the bounded admission wait.\n\
# TYPE ngkg_admission_rejected_total counter\n\
# HELP ngkg_admission_completed_total Admitted responses whose bodies completed or were dropped.\n\
# TYPE ngkg_admission_completed_total counter\n\
# HELP ngkg_admission_failed_total Admitted requests returning non-success status.\n\
# TYPE ngkg_admission_failed_total counter\n\
# HELP ngkg_admission_wait_seconds_total Total time spent waiting for admission.\n\
# TYPE ngkg_admission_wait_seconds_total counter\n\
# HELP ngkg_admission_service_seconds_total Total admitted request lifetime through response body.\n\
# TYPE ngkg_admission_service_seconds_total counter\n\
# HELP ngkg_admission_rejections_by_scope_total Admission rejections by bounded global or tenant scope.\n\
# TYPE ngkg_admission_rejections_by_scope_total counter\n",
        );
        for class in AdmissionClass::ALL {
            let counters = &self.counters[class.index()];
            let labels = format!("role=\"{role:?}\",class=\"{}\"", class.label());
            push_metric(
                &mut output,
                "ngkg_admission_limit",
                &labels,
                self.limits[class.index()],
            );
            push_metric(
                &mut output,
                "ngkg_admission_in_flight",
                &labels,
                counters.in_flight.load(Ordering::Relaxed),
            );
            push_metric(
                &mut output,
                "ngkg_admission_pending_limit",
                &labels,
                self.pending_limits[class.index()],
            );
            push_metric(
                &mut output,
                "ngkg_admission_pending",
                &labels,
                self.pending_limits[class.index()]
                    .saturating_sub(self.pending_semaphores[class.index()].available_permits()),
            );
            push_metric(
                &mut output,
                "ngkg_admission_admitted_total",
                &labels,
                counters.admitted.load(Ordering::Relaxed),
            );
            push_metric(
                &mut output,
                "ngkg_admission_rejected_total",
                &labels,
                counters.rejected.load(Ordering::Relaxed),
            );
            push_metric(
                &mut output,
                "ngkg_admission_completed_total",
                &labels,
                counters.completed.load(Ordering::Relaxed),
            );
            push_metric(
                &mut output,
                "ngkg_admission_failed_total",
                &labels,
                counters.failed.load(Ordering::Relaxed),
            );
            push_seconds_metric(
                &mut output,
                "ngkg_admission_wait_seconds_total",
                &labels,
                counters.wait_nanoseconds.load(Ordering::Relaxed),
            );
            push_seconds_metric(
                &mut output,
                "ngkg_admission_service_seconds_total",
                &labels,
                counters.service_nanoseconds.load(Ordering::Relaxed),
            );
            let tenant_rejections = self.tenant_rejections[class.index()].load(Ordering::Relaxed);
            let all_rejections = counters.rejected.load(Ordering::Relaxed);
            for (scope, value) in [
                (
                    AdmissionScope::Global,
                    all_rejections.saturating_sub(tenant_rejections),
                ),
                (AdmissionScope::Tenant, tenant_rejections),
            ] {
                push_metric(
                    &mut output,
                    "ngkg_admission_rejections_by_scope_total",
                    &format!("{labels},scope=\"{}\"", scope.label()),
                    value,
                );
            }
        }
        output.push_str(
            "# HELP ngkg_tenant_admission_configured Number of checksum-bound tenant policies loaded by this replica.\n\
# TYPE ngkg_tenant_admission_configured gauge\n",
        );
        push_metric(
            &mut output,
            "ngkg_tenant_admission_configured",
            &format!("role=\"{role:?}\""),
            self.tenants.tenant_count(),
        );
        output.push_str(
            "# HELP ngkg_fragment_worker_admission_limit Shared fragment and shuffle concurrency ceiling.\n\
# TYPE ngkg_fragment_worker_admission_limit gauge\n",
        );
        push_metric(
            &mut output,
            "ngkg_fragment_worker_admission_limit",
            &format!("role=\"{role:?}\""),
            self.fragment_worker_limit,
        );
        output.push_str(
            "# HELP ngkg_query_cache_events_total Query coordinator complete-result cache outcomes.\n\
# TYPE ngkg_query_cache_events_total counter\n",
        );
        for (outcome, value) in [
            ("hit", self.query_cache_hits.load(Ordering::Relaxed)),
            ("miss", self.query_cache_misses.load(Ordering::Relaxed)),
            ("invalid", self.query_cache_invalid.load(Ordering::Relaxed)),
            ("error", self.query_cache_errors.load(Ordering::Relaxed)),
        ] {
            push_metric(
                &mut output,
                "ngkg_query_cache_events_total",
                &format!("role=\"{role:?}\",outcome=\"{outcome}\""),
                value,
            );
        }
        output.push_str(
            "# HELP ngkg_query_cache_entries Current query-node complete-result cache entries.\n\
# TYPE ngkg_query_cache_entries gauge\n\
# HELP ngkg_query_cache_bytes Current query-node complete-result cache bytes.\n\
# TYPE ngkg_query_cache_bytes gauge\n",
        );
        if let Some(cache) = query_cache
            && let Ok((entries, bytes)) = cache.usage()
        {
            push_metric(
                &mut output,
                "ngkg_query_cache_entries",
                &format!("role=\"{role:?}\""),
                entries,
            );
            push_metric(
                &mut output,
                "ngkg_query_cache_bytes",
                &format!("role=\"{role:?}\""),
                bytes,
            );
        }
        output.push_str(
            "# HELP ngkg_shuffle_cache_events_total Worker-side shuffle cache outcomes.\n\
# TYPE ngkg_shuffle_cache_events_total counter\n",
        );
        for (outcome, value) in [
            ("hit", self.cache_hits.load(Ordering::Relaxed)),
            ("miss", self.cache_misses.load(Ordering::Relaxed)),
            ("invalid", self.cache_invalid.load(Ordering::Relaxed)),
            ("error", self.cache_errors.load(Ordering::Relaxed)),
        ] {
            push_metric(
                &mut output,
                "ngkg_shuffle_cache_events_total",
                &format!("role=\"{role:?}\",outcome=\"{outcome}\""),
                value,
            );
        }
        output.push_str(
            "# HELP ngkg_shuffle_cache_entries Current worker-local shuffle cache entries.\n\
# TYPE ngkg_shuffle_cache_entries gauge\n\
# HELP ngkg_shuffle_cache_bytes Current worker-local shuffle cache bytes.\n\
# TYPE ngkg_shuffle_cache_bytes gauge\n",
        );
        if let Some(cache) = shuffle_cache
            && let Ok((entries, bytes)) = cache.usage()
        {
            push_metric(
                &mut output,
                "ngkg_shuffle_cache_entries",
                &format!("role=\"{role:?}\""),
                entries,
            );
            push_metric(
                &mut output,
                "ngkg_shuffle_cache_bytes",
                &format!("role=\"{role:?}\""),
                bytes,
            );
        }
        output.push_str(
            "# HELP ngkg_worker_join_executions_total Worker partition joins computed by bounded mode.\n\
# TYPE ngkg_worker_join_executions_total counter\n\
# HELP ngkg_worker_join_spill_bytes_total Bytes written to worker-local Grace spill.\n\
# TYPE ngkg_worker_join_spill_bytes_total counter\n\
# HELP ngkg_worker_join_active_spill_bytes Bytes currently reserved by live Grace joins.\n\
# TYPE ngkg_worker_join_active_spill_bytes gauge\n",
        );
        for (mode, value) in [
            (
                "in_memory_hash_v1",
                self.worker_join_in_memory.load(Ordering::Relaxed),
            ),
            (
                "grace_hash_nvme_v1",
                self.worker_join_grace.load(Ordering::Relaxed),
            ),
        ] {
            push_metric(
                &mut output,
                "ngkg_worker_join_executions_total",
                &format!("role=\"{role:?}\",mode=\"{mode}\""),
                value,
            );
        }
        push_metric(
            &mut output,
            "ngkg_worker_join_spill_bytes_total",
            &format!("role=\"{role:?}\""),
            self.worker_join_spill_bytes.load(Ordering::Relaxed),
        );
        if let Some(engine) = grace_join
            && let Ok(bytes) = engine.active_spill_bytes()
        {
            push_metric(
                &mut output,
                "ngkg_worker_join_active_spill_bytes",
                &format!("role=\"{role:?}\""),
                bytes,
            );
        }
        output.push_str(
            "# HELP ngkg_coordinator_streamed_shuffle_requests_total Shuffle requests encoded directly from coordinator spill files.\n\
# TYPE ngkg_coordinator_streamed_shuffle_requests_total counter\n\
# HELP ngkg_coordinator_streamed_shuffle_bytes_total Arrow request bytes streamed from coordinator spill files.\n\
# TYPE ngkg_coordinator_streamed_shuffle_bytes_total counter\n\
# HELP ngkg_coordinator_direct_partition_fragments_total Certified fragment spools replayed directly into primary hash partitions.\n\
# TYPE ngkg_coordinator_direct_partition_fragments_total counter\n\
# HELP ngkg_coordinator_direct_partition_rows_total Certified fragment rows replayed directly into primary hash partitions.\n\
# TYPE ngkg_coordinator_direct_partition_rows_total counter\n\
# HELP ngkg_coordinator_spooled_shuffle_responses_total Shuffle worker responses admitted to checksum-verified coordinator NVMe spools.\n\
# TYPE ngkg_coordinator_spooled_shuffle_responses_total counter\n\
# HELP ngkg_coordinator_spooled_shuffle_response_bytes_total Encoded shuffle worker response bytes admitted to coordinator NVMe spools.\n\
# TYPE ngkg_coordinator_spooled_shuffle_response_bytes_total counter\n",
        );
        push_metric(
            &mut output,
            "ngkg_coordinator_streamed_shuffle_requests_total",
            &format!("role=\"{role:?}\""),
            self.coordinator_streamed_requests.load(Ordering::Relaxed),
        );
        push_metric(
            &mut output,
            "ngkg_coordinator_streamed_shuffle_bytes_total",
            &format!("role=\"{role:?}\""),
            self.coordinator_streamed_bytes.load(Ordering::Relaxed),
        );
        push_metric(
            &mut output,
            "ngkg_coordinator_direct_partition_fragments_total",
            &format!("role=\"{role:?}\""),
            self.coordinator_direct_partition_fragments
                .load(Ordering::Relaxed),
        );
        push_metric(
            &mut output,
            "ngkg_coordinator_direct_partition_rows_total",
            &format!("role=\"{role:?}\""),
            self.coordinator_direct_partition_rows
                .load(Ordering::Relaxed),
        );
        push_metric(
            &mut output,
            "ngkg_coordinator_spooled_shuffle_responses_total",
            &format!("role=\"{role:?}\""),
            self.coordinator_spooled_shuffle_responses
                .load(Ordering::Relaxed),
        );
        push_metric(
            &mut output,
            "ngkg_coordinator_spooled_shuffle_response_bytes_total",
            &format!("role=\"{role:?}\""),
            self.coordinator_spooled_shuffle_response_bytes
                .load(Ordering::Relaxed),
        );
        output.push_str(
            "# HELP ngkg_property_path_pending_work_items Semantic partition expansions awaiting the global frontier barrier.\n\
# TYPE ngkg_property_path_pending_work_items gauge\n\
# HELP ngkg_property_path_active_frontier_items Origin-preserving property-path states owned by live queries.\n\
# TYPE ngkg_property_path_active_frontier_items gauge\n\
# HELP ngkg_property_path_checkpoint_bytes Checksum-bound property-path checkpoint bytes retained by live queries.\n\
# TYPE ngkg_property_path_checkpoint_bytes gauge\n",
        );
        for (name, value) in [
            (
                "ngkg_property_path_pending_work_items",
                self.property_path_pending_work_items
                    .load(Ordering::Relaxed),
            ),
            (
                "ngkg_property_path_active_frontier_items",
                self.property_path_active_frontier_items
                    .load(Ordering::Relaxed),
            ),
            (
                "ngkg_property_path_checkpoint_bytes",
                self.property_path_checkpoint_bytes.load(Ordering::Relaxed),
            ),
        ] {
            push_metric(&mut output, name, &format!("role=\"{role:?}\""), value);
        }
        output
    }
}

impl Drop for AdmissionLease {
    fn drop(&mut self) {
        let counters = &self.controller.counters[self.class.index()];
        counters.in_flight.fetch_sub(1, Ordering::Relaxed);
        counters.completed.fetch_add(1, Ordering::Relaxed);
        if self.failed {
            counters.failed.fetch_add(1, Ordering::Relaxed);
        }
        add_duration(&counters.service_nanoseconds, self.started.elapsed());
    }
}

async fn acquire_before(
    semaphore: Arc<Semaphore>,
    deadline: Instant,
) -> Result<OwnedSemaphorePermit, SemaphoreAcquireFailure> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    tokio::time::timeout(remaining, semaphore.acquire_owned())
        .await
        .map_err(|_| SemaphoreAcquireFailure::TimedOut)?
        .map_err(|_| SemaphoreAcquireFailure::Closed)
}

fn add_duration(counter: &AtomicU64, duration: Duration) {
    let value = u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX);
    let _previous = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_add(value))
    });
}

fn push_metric(output: &mut String, name: &str, labels: &str, value: impl std::fmt::Display) {
    use std::fmt::Write as _;
    let _result = writeln!(output, "{name}{{{labels}}} {value}");
}

fn push_seconds_metric(output: &mut String, name: &str, labels: &str, nanoseconds: u64) {
    use std::fmt::Write as _;
    let seconds = nanoseconds / 1_000_000_000;
    let fraction = nanoseconds % 1_000_000_000;
    let _result = writeln!(output, "{name}{{{labels}}} {seconds}.{fraction:09}");
}

impl Drop for ShuffleCacheFlightLease {
    fn drop(&mut self) {
        let registry = Arc::clone(&self.registry);
        let digest = self.digest.clone();
        let flight = Arc::clone(&self.flight);
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let _cleanup = runtime.spawn(async move {
            tokio::task::yield_now().await;
            let mut flights = registry.lock().await;
            if Arc::strong_count(&flight) == 2
                && flights
                    .get(&digest)
                    .is_some_and(|current| Arc::ptr_eq(current, &flight))
            {
                flights.remove(&digest);
            }
        });
    }
}

impl Drop for QueryCacheFlightLease {
    fn drop(&mut self) {
        let registry = Arc::clone(&self.registry);
        let digest = self.digest.clone();
        let flight = Arc::clone(&self.flight);
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let _cleanup = runtime.spawn(async move {
            tokio::task::yield_now().await;
            let mut flights = registry.lock().await;
            if Arc::strong_count(&flight) == 2
                && flights
                    .get(&digest)
                    .is_some_and(|current| Arc::ptr_eq(current, &flight))
            {
                flights.remove(&digest);
            }
        });
    }
}

impl StreamingRequestSpool {
    fn open(root: &Path, max_active_bytes: u64) -> Result<Self> {
        if max_active_bytes == 0 {
            anyhow::bail!("NGKG_MAX_STREAMING_REQUEST_SPOOL_BYTES must be positive");
        }
        prepare_streaming_request_root(root)?;
        Ok(Self {
            root: root.to_path_buf(),
            max_active_bytes,
            active_bytes: AtomicU64::new(0),
        })
    }

    async fn receive(
        self: &Arc<Self>,
        body: Body,
        max_request_bytes: usize,
    ) -> Result<StreamingRequestLease, OnlineError> {
        let path = self.root.join(format!("request-{}.arrow", Uuid::new_v4()));
        let file = tokio::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&path)
            .await?;
        let mut lease = StreamingRequestLease {
            owner: Arc::clone(self),
            path,
            bytes: 0,
            sha256: String::new(),
            released: false,
        };
        let mut writer = tokio::io::BufWriter::new(file);
        let mut stream = body.into_data_stream();
        let mut hasher = Sha256::new();
        let mut tail = VecDeque::with_capacity(ARROW_STREAM_EOS.len());
        let maximum = u64::try_from(max_request_bytes).map_err(|_| {
            OnlineError::Request("shuffle request ceiling exceeds this platform".to_owned())
        })?;
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|error| {
                OnlineError::Request(format!("shuffle request stream failed: {error}"))
            })?;
            let chunk_bytes = u64::try_from(chunk.len()).map_err(|_| {
                OnlineError::Request("shuffle request chunk exceeds this platform".to_owned())
            })?;
            let next = lease.bytes.checked_add(chunk_bytes).ok_or_else(|| {
                OnlineError::Request("shuffle request byte count overflow".to_owned())
            })?;
            if next > maximum {
                return Err(OnlineError::Request(
                    "shuffle request exceeds its byte ceiling".to_owned(),
                ));
            }
            self.reserve(chunk_bytes)?;
            lease.bytes = next;
            hasher.update(&chunk);
            for byte in chunk.iter() {
                if tail.len() == ARROW_STREAM_EOS.len() {
                    tail.pop_front();
                }
                tail.push_back(*byte);
            }
            writer.write_all(&chunk).await?;
        }
        writer.flush().await?;
        writer.get_ref().sync_all().await?;
        drop(writer);
        if lease.bytes <= u64::try_from(ARROW_STREAM_EOS.len()).unwrap_or(8)
            || !tail.iter().copied().eq(ARROW_STREAM_EOS)
        {
            return Err(OnlineError::SnapshotConflict(
                "shuffle Arrow stream is truncated or lacks its end marker".to_owned(),
            ));
        }
        lease.sha256 = hex::encode(hasher.finalize());
        Ok(lease)
    }

    fn reserve(&self, bytes: u64) -> Result<(), OnlineError> {
        self.active_bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |active| {
                active
                    .checked_add(bytes)
                    .filter(|next| *next <= self.max_active_bytes)
            })
            .map(|_| ())
            .map_err(|_| {
                OnlineError::Request(
                    "fragment worker streaming-request spool capacity is exhausted".to_owned(),
                )
            })
    }
}

impl Drop for StreamingRequestLease {
    fn drop(&mut self) {
        if self.released {
            return;
        }
        match fs::remove_file(&self.path) {
            Ok(()) => {
                self.owner
                    .active_bytes
                    .fetch_sub(self.bytes, Ordering::AcqRel);
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                self.owner
                    .active_bytes
                    .fetch_sub(self.bytes, Ordering::AcqRel);
            }
            Err(error) => {
                tracing::error!(path = %self.path.display(), %error, "streaming request spool cleanup failed; byte reservation retained");
                return;
            }
        }
        self.released = true;
    }
}

impl FragmentResponseSpool {
    fn open(root: &Path, max_active_bytes: u64) -> Result<Self> {
        if max_active_bytes == 0 {
            anyhow::bail!("NGKG_MAX_FRAGMENT_RESPONSE_SPOOL_BYTES must be positive");
        }
        prepare_fragment_response_spool_root(root)?;
        Ok(Self {
            root: root.to_path_buf(),
            max_active_bytes,
            active_bytes: AtomicU64::new(0),
        })
    }

    async fn receive(
        self: &Arc<Self>,
        response: HttpResponse,
        max_response_bytes: usize,
    ) -> Result<FragmentResponseLease, OnlineError> {
        let maximum = u64::try_from(max_response_bytes).map_err(|_| {
            OnlineError::Request("fragment response ceiling exceeds this platform".to_owned())
        })?;
        if response
            .content_length()
            .is_some_and(|bytes| bytes > maximum)
        {
            return Err(OnlineError::Request(
                "fragment response exceeds its byte ceiling".to_owned(),
            ));
        }
        let path = self.root.join(format!("response-{}.arrow", Uuid::new_v4()));
        let file = tokio::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&path)
            .await?;
        let mut lease = FragmentResponseLease {
            owner: Arc::clone(self),
            path,
            bytes: 0,
            sha256: [0; 32],
            released: false,
        };
        let mut writer = tokio::io::BufWriter::new(file);
        let mut stream = response.bytes_stream();
        let mut hasher = Sha256::new();
        let mut tail = VecDeque::with_capacity(ARROW_STREAM_EOS.len());
        while let Some(chunk) = stream.next().await {
            let chunk = chunk
                .map_err(|error| upstream_transport_error("fragment response stream", error))?;
            let chunk_bytes = u64::try_from(chunk.len()).map_err(|_| {
                OnlineError::Request("fragment response chunk exceeds this platform".to_owned())
            })?;
            let next = lease.bytes.checked_add(chunk_bytes).ok_or_else(|| {
                OnlineError::Request("fragment response byte count overflow".to_owned())
            })?;
            if next > maximum {
                return Err(OnlineError::Request(
                    "fragment response exceeds its byte ceiling".to_owned(),
                ));
            }
            self.reserve(chunk_bytes)?;
            lease.bytes = next;
            hasher.update(&chunk);
            for byte in chunk.iter() {
                if tail.len() == ARROW_STREAM_EOS.len() {
                    tail.pop_front();
                }
                tail.push_back(*byte);
            }
            writer.write_all(&chunk).await?;
        }
        writer.flush().await?;
        writer.get_ref().sync_all().await?;
        drop(writer);
        if lease.bytes <= u64::try_from(ARROW_STREAM_EOS.len()).unwrap_or(8)
            || !tail.iter().copied().eq(ARROW_STREAM_EOS)
        {
            return Err(OnlineError::SnapshotConflict(
                "fragment Arrow response is truncated or lacks its end marker".to_owned(),
            ));
        }
        lease.sha256 = hasher.finalize().into();
        Ok(lease)
    }

    fn reserve(&self, bytes: u64) -> Result<(), OnlineError> {
        self.active_bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |active| {
                active
                    .checked_add(bytes)
                    .filter(|next| *next <= self.max_active_bytes)
            })
            .map(|_| ())
            .map_err(|_| {
                OnlineError::Request(
                    "query fragment-response spool capacity is exhausted".to_owned(),
                )
            })
    }
}

impl FragmentResponseLease {
    fn open_stream(
        &self,
        max_rows: usize,
    ) -> Result<FragmentBindingStream<BufReader<File>>, OnlineError> {
        let metadata = fs::symlink_metadata(&self.path)?;
        if metadata.file_type().is_symlink() || !metadata.is_file() || metadata.len() != self.bytes
        {
            return Err(OnlineError::SnapshotConflict(
                "fragment response spool file identity changed".to_owned(),
            ));
        }
        let mut file = File::open(&self.path)?;
        let mut hasher = Sha256::new();
        let mut buffer = vec![0_u8; 1024 * 1024].into_boxed_slice();
        loop {
            let read = file.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            hasher.update(&buffer[..read]);
        }
        if <[u8; 32]>::from(hasher.finalize()) != self.sha256 {
            return Err(OnlineError::SnapshotConflict(
                "fragment response spool checksum changed".to_owned(),
            ));
        }
        file.seek(SeekFrom::Start(0))?;
        FragmentBindingStream::try_new(BufReader::new(file), max_rows)
            .map_err(distributed_execution_error)
    }
}

impl ValidatedFragmentSpool {
    fn validate(lease: FragmentResponseLease, max_rows: usize) -> Result<Self, OnlineError> {
        let mut stream = lease.open_stream(max_rows)?;
        let metadata = stream.metadata().clone();
        let head = stream.head().to_vec();
        let mut always_bound_variables = head.iter().cloned().collect::<BTreeSet<_>>();
        let mut row_count = 0_u64;
        for row in &mut stream {
            let row = row.map_err(distributed_execution_error)?;
            let binding = row.as_object().ok_or_else(|| {
                OnlineError::SnapshotConflict(
                    "fragment stream contains a non-object binding".to_owned(),
                )
            })?;
            always_bound_variables.retain(|variable| binding.contains_key(variable));
            row_count = row_count.checked_add(1).ok_or_else(|| {
                OnlineError::Request("fragment response row count overflow".to_owned())
            })?;
        }
        Ok(Self {
            lease,
            metadata,
            head,
            row_count,
            always_bound_variables,
        })
    }

    fn summary(&self) -> FragmentBindingSummary {
        FragmentBindingSummary {
            head: self.head.clone(),
            always_bound_variables: self.always_bound_variables.clone(),
        }
    }

    fn materialize(self, max_rows: usize) -> Result<Vec<serde_json::Value>, OnlineError> {
        let response = self
            .lease
            .open_stream(max_rows)?
            .into_batch()
            .map_err(distributed_execution_error)?;
        if response.metadata != self.metadata
            || response.head != self.head
            || u64::try_from(response.bindings.len()).ok() != Some(self.row_count)
        {
            return Err(OnlineError::SnapshotConflict(
                "fragment response changed between validation and replay".to_owned(),
            ));
        }
        Ok(response.bindings)
    }
}

impl ValidatedFragmentSpoolSequence {
    fn new(spools: Vec<ValidatedFragmentSpool>, max_rows: usize) -> Self {
        Self {
            pending: spools.into(),
            current: None,
            max_rows,
        }
    }

    fn open_next(&mut self) -> Result<bool, OnlineError> {
        let Some(spool) = self.pending.pop_front() else {
            return Ok(false);
        };
        let stream = spool.lease.open_stream(self.max_rows)?;
        if stream.metadata() != &spool.metadata || stream.head() != spool.head.as_slice() {
            return Err(OnlineError::SnapshotConflict(
                "fragment spool identity changed before replay".to_owned(),
            ));
        }
        self.current = Some(FragmentReplay {
            lease: spool.lease,
            stream,
            expected_rows: spool.row_count,
            observed_rows: 0,
        });
        Ok(true)
    }
}

impl Iterator for ValidatedFragmentSpoolSequence {
    type Item = Result<serde_json::Value, OnlineError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.current.is_none() {
                match self.open_next() {
                    Ok(true) => {}
                    Ok(false) => return None,
                    Err(error) => return Some(Err(error)),
                }
            }
            let next = self.current.as_mut()?.stream.next();
            match next {
                Some(Ok(row)) => {
                    let replay = self.current.as_mut()?;
                    replay.observed_rows = match replay.observed_rows.checked_add(1) {
                        Some(rows) if rows <= replay.expected_rows => rows,
                        _ => {
                            self.current.take();
                            return Some(Err(OnlineError::SnapshotConflict(
                                "fragment spool replay exceeded its validated row count".to_owned(),
                            )));
                        }
                    };
                    return Some(Ok(row));
                }
                Some(Err(error)) => {
                    self.current.take();
                    return Some(Err(distributed_execution_error(error)));
                }
                None => {
                    let replay = self.current.take()?;
                    if replay.observed_rows != replay.expected_rows {
                        return Some(Err(OnlineError::SnapshotConflict(
                            "fragment spool replay differs from its validated row count".to_owned(),
                        )));
                    }
                    let FragmentReplay { lease, stream, .. } = replay;
                    drop(stream);
                    drop(lease);
                }
            }
        }
    }
}

impl Drop for FragmentResponseLease {
    fn drop(&mut self) {
        if self.released {
            return;
        }
        match fs::remove_file(&self.path) {
            Ok(()) => {
                self.owner
                    .active_bytes
                    .fetch_sub(self.bytes, Ordering::AcqRel);
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                self.owner
                    .active_bytes
                    .fetch_sub(self.bytes, Ordering::AcqRel);
            }
            Err(error) => {
                tracing::error!(path = %self.path.display(), %error, "fragment response spool cleanup failed; byte reservation retained");
                return;
            }
        }
        self.released = true;
    }
}

fn prepare_fragment_response_spool_root(root: &Path) -> Result<()> {
    if root.exists() {
        let metadata = fs::symlink_metadata(root)?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            anyhow::bail!("fragment response spool root must be a real directory");
        }
    } else {
        fs::create_dir_all(root)?;
    }
    let marker = root.join(FRAGMENT_RESPONSE_SPOOL_MARKER);
    if marker.exists() {
        if fs::symlink_metadata(&marker)?.file_type().is_symlink()
            || fs::read(&marker)? != FRAGMENT_RESPONSE_SPOOL_MARKER_BYTES
        {
            anyhow::bail!("fragment response spool root marker is invalid");
        }
    } else {
        if fs::read_dir(root)?.next().transpose()?.is_some() {
            anyhow::bail!("uninitialized fragment response spool root must be empty");
        }
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&marker)?;
        file.write_all(FRAGMENT_RESPONSE_SPOOL_MARKER_BYTES)?;
        file.sync_all()?;
        File::open(root)?.sync_all()?;
    }
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name
            .to_str()
            .ok_or_else(|| anyhow::anyhow!("fragment response spool entry is not UTF-8"))?;
        if name == FRAGMENT_RESPONSE_SPOOL_MARKER {
            continue;
        }
        let identifier = name
            .strip_prefix("response-")
            .and_then(|value| value.strip_suffix(".arrow"))
            .ok_or_else(|| anyhow::anyhow!("unmanaged fragment response spool entry {name}"))?;
        if identifier.parse::<Uuid>().is_err()
            || entry.file_type()?.is_symlink()
            || !entry.file_type()?.is_file()
        {
            anyhow::bail!("invalid fragment response spool entry {name}");
        }
        fs::remove_file(entry.path())?;
    }
    Ok(())
}

fn prepare_streaming_request_root(root: &Path) -> Result<()> {
    if root.exists() {
        let metadata = fs::symlink_metadata(root)?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            anyhow::bail!("streaming request spool root must be a real directory");
        }
    } else {
        fs::create_dir_all(root)?;
    }
    let marker = root.join(REQUEST_SPOOL_MARKER);
    if marker.exists() {
        if fs::symlink_metadata(&marker)?.file_type().is_symlink()
            || fs::read(&marker)? != REQUEST_SPOOL_MARKER_BYTES
        {
            anyhow::bail!("streaming request spool root marker is invalid");
        }
    } else {
        if fs::read_dir(root)?.next().transpose()?.is_some() {
            anyhow::bail!("uninitialized streaming request spool root must be empty");
        }
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&marker)?;
        file.write_all(REQUEST_SPOOL_MARKER_BYTES)?;
        file.sync_all()?;
        File::open(root)?.sync_all()?;
    }
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name
            .to_str()
            .ok_or_else(|| anyhow::anyhow!("streaming request spool entry is not UTF-8"))?;
        if name == REQUEST_SPOOL_MARKER {
            continue;
        }
        let identifier = name
            .strip_prefix("request-")
            .and_then(|value| value.strip_suffix(".arrow"))
            .ok_or_else(|| anyhow::anyhow!("unmanaged streaming request spool entry {name}"))?;
        if identifier.parse::<Uuid>().is_err()
            || entry.file_type()?.is_symlink()
            || !entry.file_type()?.is_file()
        {
            anyhow::bail!("invalid streaming request spool entry {name}");
        }
        fs::remove_file(entry.path())?;
    }
    Ok(())
}

impl BoundedBuffer {
    fn new(max_bytes: usize) -> Self {
        Self {
            bytes: Vec::with_capacity(max_bytes.min(1024 * 1024)),
            max_bytes,
        }
    }

    fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }
}

impl Write for BoundedBuffer {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        if self
            .bytes
            .len()
            .checked_add(buffer.len())
            .is_none_or(|total| total > self.max_bytes)
        {
            return Err(std::io::Error::other(
                "serialized response exceeds its byte ceiling",
            ));
        }
        self.bytes.extend_from_slice(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl SpillWriter {
    fn create(
        root: &Path,
        identity: &SpillIdentity,
        side: u8,
        partition: u32,
        total_bytes: &mut u64,
        max_bytes: u64,
    ) -> Result<Self, OnlineError> {
        let path = root.join(format!("side-{side}-partition-{partition:08}.spill"));
        let file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&path)?;
        let mut value = Self {
            writer: BufWriter::new(file),
            hasher: Sha256::new(),
            path,
            side,
            partition,
            rows: 0,
            bytes: 0,
        };
        let header = spill_header(identity, side, partition);
        reserve_spill_bytes(total_bytes, header.len(), max_bytes)?;
        value.write_hashed(&header)?;
        Ok(value)
    }

    fn append(
        &mut self,
        row: &serde_json::Value,
        total_bytes: &mut u64,
        max_bytes: u64,
    ) -> Result<(), OnlineError> {
        let encoded = serde_json::to_vec(row).map_err(|_| {
            OnlineError::SnapshotConflict("shuffle spill row is not serializable".to_owned())
        })?;
        let length = u64::try_from(encoded.len()).map_err(|_| {
            OnlineError::Request("shuffle spill row exceeds this platform".to_owned())
        })?;
        let record_bytes = encoded
            .len()
            .checked_add(8)
            .ok_or_else(|| OnlineError::Request("shuffle spill record size overflow".to_owned()))?;
        reserve_spill_bytes(total_bytes, record_bytes, max_bytes)?;
        self.write_hashed(&length.to_be_bytes())?;
        self.write_hashed(&encoded)?;
        self.rows = self
            .rows
            .checked_add(1)
            .ok_or_else(|| OnlineError::Request("shuffle spill row count overflow".to_owned()))?;
        Ok(())
    }

    fn write_hashed(&mut self, bytes: &[u8]) -> Result<(), OnlineError> {
        self.writer.write_all(bytes)?;
        self.hasher.update(bytes);
        self.bytes = self
            .bytes
            .checked_add(u64::try_from(bytes.len()).map_err(|_| {
                OnlineError::Request("shuffle spill byte count overflow".to_owned())
            })?)
            .ok_or_else(|| OnlineError::Request("shuffle spill byte count overflow".to_owned()))?;
        Ok(())
    }

    fn finish(mut self) -> Result<SpillPartition, OnlineError> {
        self.writer.flush()?;
        self.writer.get_ref().sync_all()?;
        let sha256: [u8; 32] = self.hasher.finalize().into();
        Ok(SpillPartition {
            path: self.path,
            side: self.side,
            partition: self.partition,
            rows: self.rows,
            bytes: self.bytes,
            sha256,
        })
    }
}

impl ShuffleSpillStage {
    #[cfg(test)]
    #[allow(clippy::too_many_arguments)]
    fn create(
        base: &Path,
        identity: SpillIdentity,
        left_rows: Vec<serde_json::Value>,
        right_rows: Vec<serde_json::Value>,
        key_variables: &[String],
        max_bytes: u64,
        max_open_files: usize,
    ) -> Result<Self, OnlineError> {
        Self::create_iter(
            base,
            identity,
            left_rows.into_iter().map(Ok),
            right_rows.into_iter().map(Ok),
            key_variables,
            max_bytes,
            max_open_files,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn create_iter<L, R>(
        base: &Path,
        identity: SpillIdentity,
        left_rows: L,
        right_rows: R,
        key_variables: &[String],
        max_bytes: u64,
        max_open_files: usize,
    ) -> Result<Self, OnlineError>
    where
        L: IntoIterator<Item = Result<serde_json::Value, OnlineError>>,
        R: IntoIterator<Item = Result<serde_json::Value, OnlineError>>,
    {
        let partition_count = usize::try_from(identity.partition_count).map_err(|_| {
            OnlineError::Request("shuffle partition count exceeds this platform".to_owned())
        })?;
        if partition_count
            .checked_mul(2)
            .is_none_or(|files| files > max_open_files)
        {
            return Err(OnlineError::Request(
                "shuffle partition writers exceed the open-file ceiling".to_owned(),
            ));
        }
        let root = base.join(format!("stage-{}", Uuid::new_v4()));
        fs::create_dir(&root)?;
        let result = (|| {
            let mut total_bytes = 0_u64;
            let mut left = (0..identity.partition_count)
                .map(|partition| {
                    SpillWriter::create(&root, &identity, 0, partition, &mut total_bytes, max_bytes)
                })
                .collect::<Result<Vec<_>, _>>()?;
            let mut right = (0..identity.partition_count)
                .map(|partition| {
                    SpillWriter::create(&root, &identity, 1, partition, &mut total_bytes, max_bytes)
                })
                .collect::<Result<Vec<_>, _>>()?;
            spill_rows(
                left_rows,
                &mut left,
                key_variables,
                identity.partition_count,
                &mut total_bytes,
                max_bytes,
            )?;
            spill_rows(
                right_rows,
                &mut right,
                key_variables,
                identity.partition_count,
                &mut total_bytes,
                max_bytes,
            )?;
            let left = left
                .into_iter()
                .map(SpillWriter::finish)
                .collect::<Result<Vec<_>, _>>()?;
            let right = right
                .into_iter()
                .map(SpillWriter::finish)
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Self {
                root: root.clone(),
                cleaned: false,
                identity,
                left,
                right,
                total_bytes,
            })
        })();
        if result.is_err() {
            let _cleanup = fs::remove_dir_all(&root);
        }
        result
    }

    #[cfg(test)]
    fn read_pair(
        &self,
        partition: u32,
        key_variables: &[String],
        max_rows: usize,
    ) -> Result<(Vec<serde_json::Value>, Vec<serde_json::Value>), OnlineError> {
        let index = usize::try_from(partition).map_err(|_| {
            OnlineError::Request("shuffle partition exceeds this platform".to_owned())
        })?;
        let left = self.left.get(index).ok_or_else(|| {
            OnlineError::SnapshotConflict("left spill partition is absent".to_owned())
        })?;
        let right = self.right.get(index).ok_or_else(|| {
            OnlineError::SnapshotConflict("right spill partition is absent".to_owned())
        })?;
        Ok((
            read_spill_partition(left, &self.identity, key_variables, max_rows)?,
            read_spill_partition(right, &self.identity, key_variables, max_rows)?,
        ))
    }

    fn open_pair(
        &self,
        partition: u32,
        key_variables: &[String],
        max_rows: usize,
    ) -> Result<(SpillPartitionReader, SpillPartitionReader), OnlineError> {
        let index = usize::try_from(partition).map_err(|_| {
            OnlineError::Request("shuffle partition exceeds this platform".to_owned())
        })?;
        let left = self.left.get(index).ok_or_else(|| {
            OnlineError::SnapshotConflict("left spill partition is absent".to_owned())
        })?;
        let right = self.right.get(index).ok_or_else(|| {
            OnlineError::SnapshotConflict("right spill partition is absent".to_owned())
        })?;
        Ok((
            SpillPartitionReader::open(left, &self.identity, key_variables, max_rows)?,
            SpillPartitionReader::open(right, &self.identity, key_variables, max_rows)?,
        ))
    }

    fn cleanup(mut self) -> Result<(), OnlineError> {
        fs::remove_dir_all(&self.root)?;
        self.cleaned = true;
        Ok(())
    }
}

impl Drop for ShuffleSpillStage {
    fn drop(&mut self) {
        if !self.cleaned {
            if let Err(error) = fs::remove_dir_all(&self.root) {
                tracing::error!(path = %self.root.display(), %error, "shuffle spill cleanup failed");
            }
        }
    }
}

fn spill_rows<I>(
    rows: I,
    writers: &mut [SpillWriter],
    key_variables: &[String],
    partition_count: u32,
    total_bytes: &mut u64,
    max_bytes: u64,
) -> Result<(), OnlineError>
where
    I: IntoIterator<Item = Result<serde_json::Value, OnlineError>>,
{
    for row in rows {
        let row = row?;
        let partition = shuffle_partition_for_binding(&row, key_variables, partition_count)
            .map_err(distributed_execution_error)?;
        let writer = writers
            .get_mut(usize::try_from(partition).map_err(|_| {
                OnlineError::Request("shuffle partition exceeds this platform".to_owned())
            })?)
            .ok_or_else(|| {
                OnlineError::SnapshotConflict("shuffle spill owner is absent".to_owned())
            })?;
        writer.append(&row, total_bytes, max_bytes)?;
    }
    Ok(())
}

#[cfg(test)]
fn read_spill_partition(
    partition: &SpillPartition,
    identity: &SpillIdentity,
    key_variables: &[String],
    max_rows: usize,
) -> Result<Vec<serde_json::Value>, OnlineError> {
    SpillPartitionReader::open(partition, identity, key_variables, max_rows)?.collect()
}

impl SpillPartitionReader {
    fn open(
        partition: &SpillPartition,
        identity: &SpillIdentity,
        key_variables: &[String],
        max_rows: usize,
    ) -> Result<Self, OnlineError> {
        let metadata = fs::symlink_metadata(&partition.path)?;
        if partition.rows > max_rows
            || metadata.file_type().is_symlink()
            || !metadata.is_file()
            || metadata.len() != partition.bytes
        {
            return Err(OnlineError::SnapshotConflict(
                "shuffle spill size, type, or row contract is invalid".to_owned(),
            ));
        }
        let mut reader = BufReader::new(File::open(&partition.path)?);
        let mut hasher = Sha256::new();
        let mut header = [0_u8; SPILL_HEADER_BYTES];
        read_hashed_exact(&mut reader, &mut hasher, &mut header)?;
        if header.as_slice()
            != spill_header(identity, partition.side, partition.partition).as_slice()
        {
            return Err(OnlineError::SnapshotConflict(
                "shuffle spill header differs from its stage identity".to_owned(),
            ));
        }
        Ok(Self {
            reader,
            rows_remaining: partition.rows,
            declared_rows: partition.rows,
            expected_bytes: partition.bytes,
            bytes_read: u64::try_from(SPILL_HEADER_BYTES).map_err(|_| {
                OnlineError::SnapshotConflict("shuffle spill header size overflow".to_owned())
            })?,
            expected_sha256: partition.sha256,
            hasher,
            key_variables: key_variables.to_vec(),
            partition: partition.partition,
            partition_count: identity.partition_count,
            terminal: false,
        })
    }

    const fn declared_rows(&self) -> usize {
        self.declared_rows
    }

    fn read_next(&mut self) -> Result<Option<serde_json::Value>, OnlineError> {
        if self.rows_remaining == 0 {
            let mut trailing = [0_u8; 1];
            if self.reader.read(&mut trailing)? != 0
                || self.bytes_read != self.expected_bytes
                || <[u8; 32]>::from(self.hasher.clone().finalize()) != self.expected_sha256
            {
                return Err(OnlineError::SnapshotConflict(
                    "shuffle spill checksum or file boundary is invalid".to_owned(),
                ));
            }
            return Ok(None);
        }
        let mut length_bytes = [0_u8; 8];
        read_hashed_exact(&mut self.reader, &mut self.hasher, &mut length_bytes)?;
        let length = usize::try_from(u64::from_be_bytes(length_bytes)).map_err(|_| {
            OnlineError::SnapshotConflict("shuffle spill record length overflow".to_owned())
        })?;
        let record_bytes = u64::try_from(length)
            .ok()
            .and_then(|value| value.checked_add(8))
            .ok_or_else(|| {
                OnlineError::SnapshotConflict("shuffle spill length overflow".to_owned())
            })?;
        self.bytes_read = self.bytes_read.checked_add(record_bytes).ok_or_else(|| {
            OnlineError::SnapshotConflict("shuffle spill length overflow".to_owned())
        })?;
        if length == 0 || self.bytes_read > self.expected_bytes {
            return Err(OnlineError::SnapshotConflict(
                "shuffle spill record exceeds the certified file".to_owned(),
            ));
        }
        let mut encoded = vec![0_u8; length];
        read_hashed_exact(&mut self.reader, &mut self.hasher, &mut encoded)?;
        let row: serde_json::Value = serde_json::from_slice(&encoded).map_err(|_| {
            OnlineError::SnapshotConflict("shuffle spill row is invalid JSON".to_owned())
        })?;
        if shuffle_partition_for_binding(&row, &self.key_variables, self.partition_count)
            .map_err(distributed_execution_error)?
            != self.partition
        {
            return Err(OnlineError::SnapshotConflict(
                "shuffle spill row belongs to another partition".to_owned(),
            ));
        }
        self.rows_remaining -= 1;
        Ok(Some(row))
    }
}

impl Iterator for SpillPartitionReader {
    type Item = Result<serde_json::Value, OnlineError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.terminal {
            return None;
        }
        match self.read_next() {
            Ok(Some(row)) => Some(Ok(row)),
            Ok(None) => {
                self.terminal = true;
                None
            }
            Err(error) => {
                self.terminal = true;
                Some(Err(error))
            }
        }
    }
}

fn read_hashed_exact(
    reader: &mut impl Read,
    hasher: &mut Sha256,
    buffer: &mut [u8],
) -> Result<(), OnlineError> {
    reader.read_exact(buffer)?;
    hasher.update(buffer);
    Ok(())
}

fn spill_header(identity: &SpillIdentity, side: u8, partition: u32) -> Vec<u8> {
    let mut header = Vec::with_capacity(SPILL_HEADER_BYTES);
    header.extend_from_slice(SPILL_MAGIC);
    header.extend_from_slice(identity.dataset_id.as_bytes());
    header.extend_from_slice(identity.snapshot_id.as_bytes());
    header.extend_from_slice(&identity.query_sha256);
    header.extend_from_slice(&identity.stage.to_be_bytes());
    header.push(side);
    header.extend_from_slice(&partition.to_be_bytes());
    header.extend_from_slice(&identity.partition_count.to_be_bytes());
    header
}

fn prepare_shuffle_spill_root(path: &Path) -> Result<(), OnlineError> {
    if path.exists() {
        let metadata = fs::symlink_metadata(path)?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(OnlineError::Request(
                "shuffle spill root must be a real directory".to_owned(),
            ));
        }
    } else {
        fs::create_dir_all(path)?;
    }
    let marker = path.join(SPILL_ROOT_MARKER);
    if marker.exists() {
        if fs::symlink_metadata(&marker)?.file_type().is_symlink()
            || fs::read(&marker)? != b"ngkg-shuffle-spill-v1\n"
        {
            return Err(OnlineError::Request(
                "shuffle spill root marker is invalid".to_owned(),
            ));
        }
    } else {
        let mut entries = fs::read_dir(path)?;
        if entries.next().transpose()?.is_some() {
            return Err(OnlineError::Request(
                "uninitialized shuffle spill root must be empty".to_owned(),
            ));
        }
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&marker)?;
        file.write_all(b"ngkg-shuffle-spill-v1\n")?;
        file.sync_all()?;
    }
    let mut stale_stages = Vec::new();
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_str().ok_or_else(|| {
            OnlineError::Request("shuffle spill root contains a non-UTF-8 entry".to_owned())
        })?;
        if name == SPILL_ROOT_MARKER {
            continue;
        }
        let file_type = entry.file_type()?;
        if !name
            .strip_prefix("stage-")
            .is_some_and(|value| value.parse::<Uuid>().is_ok())
            || file_type.is_symlink()
            || !file_type.is_dir()
        {
            return Err(OnlineError::Request(
                "shuffle spill root contains an unmanaged entry".to_owned(),
            ));
        }
        stale_stages.push(entry.path());
    }
    for stale in stale_stages {
        fs::remove_dir_all(stale)?;
    }
    Ok(())
}

fn reserve_spill_bytes(total: &mut u64, bytes: usize, maximum: u64) -> Result<(), OnlineError> {
    let bytes = u64::try_from(bytes)
        .map_err(|_| OnlineError::Request("shuffle spill size overflow".to_owned()))?;
    *total = (*total)
        .checked_add(bytes)
        .filter(|value| *value <= maximum)
        .ok_or_else(|| OnlineError::Request("shuffle spill exceeds its byte ceiling".to_owned()))?;
    Ok(())
}

struct ArrowBodyWriter {
    sender: mpsc::Sender<Result<Bytes, std::io::Error>>,
    pending: Vec<u8>,
    chunk_bytes: usize,
    max_bytes: usize,
    written: usize,
}

struct ArrowRequestWriter {
    body: ArrowBodyWriter,
    exchange_bytes: Arc<AtomicUsize>,
    max_exchange_bytes: usize,
    hasher: Sha256,
    written: u64,
}

struct ArrowRequestEvidence {
    bytes: u64,
    sha256: String,
}

impl ArrowBodyWriter {
    fn new(
        sender: mpsc::Sender<Result<Bytes, std::io::Error>>,
        chunk_bytes: usize,
        max_bytes: usize,
    ) -> Self {
        Self {
            sender,
            pending: Vec::with_capacity(chunk_bytes),
            chunk_bytes,
            max_bytes,
            written: 0,
        }
    }

    fn send_pending(&mut self) -> std::io::Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let bytes = Bytes::from(std::mem::replace(
            &mut self.pending,
            Vec::with_capacity(self.chunk_bytes),
        ));
        self.sender.blocking_send(Ok(bytes)).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "fragment client disconnected",
            )
        })
    }

    fn fail(&mut self, message: String) {
        self.pending.clear();
        let _ = self
            .sender
            .blocking_send(Err(std::io::Error::other(message)));
    }
}

impl Write for ArrowBodyWriter {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        self.written = self
            .written
            .checked_add(buffer.len())
            .filter(|total| *total <= self.max_bytes)
            .ok_or_else(|| {
                std::io::Error::other("Arrow fragment stream exceeds its byte ceiling")
            })?;
        let mut remaining = buffer;
        while !remaining.is_empty() {
            let available = self.chunk_bytes.saturating_sub(self.pending.len());
            let take = available.min(remaining.len());
            self.pending.extend_from_slice(&remaining[..take]);
            remaining = &remaining[take..];
            if self.pending.len() == self.chunk_bytes {
                self.send_pending()?;
            }
        }
        Ok(buffer.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.send_pending()
    }
}

impl ArrowRequestWriter {
    fn new(
        sender: mpsc::Sender<Result<Bytes, std::io::Error>>,
        chunk_bytes: usize,
        max_request_bytes: usize,
        exchange_bytes: Arc<AtomicUsize>,
        max_exchange_bytes: usize,
    ) -> Self {
        Self {
            body: ArrowBodyWriter::new(sender, chunk_bytes, max_request_bytes),
            exchange_bytes,
            max_exchange_bytes,
            hasher: Sha256::new(),
            written: 0,
        }
    }

    fn complete(&mut self) -> Result<ArrowRequestEvidence, std::io::Error> {
        self.flush()?;
        Ok(ArrowRequestEvidence {
            bytes: self.written,
            sha256: hex::encode(self.hasher.clone().finalize()),
        })
    }

    fn fail(&mut self, message: String) {
        self.body.fail(message);
    }
}

impl Write for ArrowRequestWriter {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        reserve_exchange_bytes(&self.exchange_bytes, buffer.len(), self.max_exchange_bytes)
            .map_err(|error| std::io::Error::other(error.to_string()))?;
        let written = self.body.write(buffer)?;
        self.hasher.update(buffer);
        self.written = self
            .written
            .checked_add(
                u64::try_from(buffer.len())
                    .map_err(|_| std::io::Error::other("streamed request byte count overflow"))?,
            )
            .ok_or_else(|| std::io::Error::other("streamed request byte count overflow"))?;
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.body.flush()
    }
}

impl<T> BoundedLruCache<T> {
    fn new() -> Self {
        Self {
            entries: BTreeMap::new(),
            least_to_most_recent: VecDeque::new(),
        }
    }

    fn get(&mut self, query_sha256: &str) -> Option<Arc<T>> {
        let runtime = self.entries.get(query_sha256).cloned()?;
        self.touch(query_sha256);
        Some(runtime)
    }

    fn contains(&self, query_sha256: &str) -> bool {
        self.entries.contains_key(query_sha256)
    }

    fn insert(&mut self, query_sha256: String, runtime: Arc<T>, capacity: usize) -> Option<String> {
        if capacity == 0 {
            self.entries.clear();
            self.least_to_most_recent.clear();
            return None;
        }
        if self.entries.contains_key(&query_sha256) {
            self.entries.insert(query_sha256.clone(), runtime);
            self.touch(&query_sha256);
            return None;
        }
        let mut evicted_query = None;
        while self.entries.len() >= capacity {
            if let Some(evicted) = self.least_to_most_recent.pop_front() {
                self.entries.remove(&evicted);
                evicted_query = Some(evicted);
            }
        }
        self.entries.insert(query_sha256.clone(), runtime);
        self.least_to_most_recent.push_back(query_sha256);
        evicted_query
    }

    fn touch(&mut self, query_sha256: &str) {
        if let Some(position) = self
            .least_to_most_recent
            .iter()
            .position(|value| value == query_sha256)
        {
            self.least_to_most_recent.remove(position);
        }
        self.least_to_most_recent.push_back(query_sha256.to_owned());
    }
}

struct PhysicalState {
    active: ActiveServingSnapshot,
    manifest: ServingRootManifest,
    locator: Arc<MmapLocatorIndex>,
    dictionary: Arc<BTreeMap<u64, String>>,
    authorization: Arc<GraphAuthorizationState>,
    payloads: Mutex<BTreeMap<u32, VerifiedPayloadShard>>,
    payload_load: Arc<Mutex<()>>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct QueryRequest {
    query: String,
    snapshot_id: Option<Uuid>,
    hydrate: bool,
    #[serde(default)]
    default_graph_uris: Vec<String>,
    #[serde(default)]
    named_graph_uris: Vec<String>,
}

/// Internal, authenticated native leaf-scan request. Dictionary IDs are resolved by the
/// coordinator from the same checksum-bound semantic dictionary used by this worker.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct NativeLeafScanRequest {
    snapshot_id: Uuid,
    manifest_sha256: String,
    semantic_root_sha256: String,
    active_dataset: ResolvedDataset,
    predicate: LeafPredicate,
    limits: LeafScanLimits,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct NativeLeafScanResponse {
    dataset_id: Uuid,
    snapshot_id: Uuid,
    query_sha256: String,
    partition: u32,
    partition_manifest_sha256: String,
    worker_id: String,
    result: LeafScanResult,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct DirectBgpValidationRequest {
    query: String,
    snapshot_id: Option<Uuid>,
    #[serde(default)]
    default_graph_uris: Vec<String>,
    #[serde(default)]
    named_graph_uris: Vec<String>,
}

#[derive(Debug, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct DirectBgpRoutingRecord {
    ordinal: u64,
    bgp_sha256: String,
    route: EntailmentRoute,
}

#[derive(Debug, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct DirectEntailmentRoutingResponse {
    legality: DirectBgpLegalityReport,
    routes: Vec<DirectBgpRoutingRecord>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SparqlProtocolRequest {
    query: String,
    default_graph_uris: Vec<String>,
    named_graph_uris: Vec<String>,
}

#[derive(Default)]
struct ParsedProtocolParameters {
    query: Option<String>,
    default_graph_uris: Vec<String>,
    named_graph_uris: Vec<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SparqlSolutionFormat {
    Json,
    Xml,
    Tsv,
    Csv,
}

const SPARQL_SOLUTION_FORMATS: [SparqlSolutionFormat; 4] = [
    SparqlSolutionFormat::Json,
    SparqlSolutionFormat::Xml,
    SparqlSolutionFormat::Tsv,
    SparqlSolutionFormat::Csv,
];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SparqlGraphFormat {
    Turtle,
    NTriples,
    RdfXml,
}

const SPARQL_GRAPH_FORMATS: [SparqlGraphFormat; 3] = [
    SparqlGraphFormat::Turtle,
    SparqlGraphFormat::NTriples,
    SparqlGraphFormat::RdfXml,
];

impl SparqlGraphFormat {
    const fn rdf_format(self) -> RdfFormat {
        match self {
            Self::Turtle => RdfFormat::Turtle,
            Self::NTriples => RdfFormat::NTriples,
            Self::RdfXml => RdfFormat::RdfXml,
        }
    }

    const fn media_type(self) -> &'static str {
        match self {
            Self::Turtle => "text/turtle",
            Self::NTriples => "application/n-triples",
            Self::RdfXml => "application/rdf+xml",
        }
    }

    const fn content_type(self) -> &'static str {
        match self {
            Self::Turtle => "text/turtle; charset=utf-8",
            Self::NTriples => "application/n-triples; charset=utf-8",
            Self::RdfXml => "application/rdf+xml; charset=utf-8",
        }
    }

    const fn service_description_token(self) -> &'static str {
        match self {
            Self::Turtle => "formats:Turtle",
            Self::NTriples => "formats:N-Triples",
            Self::RdfXml => "formats:RDF_XML",
        }
    }
}

impl SparqlSolutionFormat {
    const fn result_format(self) -> QueryResultsFormat {
        match self {
            Self::Json => QueryResultsFormat::Json,
            Self::Xml => QueryResultsFormat::Xml,
            Self::Tsv => QueryResultsFormat::Tsv,
            Self::Csv => QueryResultsFormat::Csv,
        }
    }

    const fn media_type(self) -> &'static str {
        match self {
            Self::Json => "application/sparql-results+json",
            Self::Xml => "application/sparql-results+xml",
            Self::Tsv => "text/tab-separated-values",
            Self::Csv => "text/csv",
        }
    }

    const fn content_type(self) -> &'static str {
        match self {
            Self::Json => "application/sparql-results+json; charset=utf-8",
            Self::Xml => "application/sparql-results+xml; charset=utf-8",
            Self::Tsv => "text/tab-separated-values; charset=utf-8",
            Self::Csv => "text/csv; charset=utf-8",
        }
    }

    const fn service_description_token(self) -> &'static str {
        match self {
            Self::Json => "formats:SPARQL_Results_JSON",
            Self::Xml => "formats:SPARQL_Results_XML",
            Self::Tsv => "formats:SPARQL_Results_TSV",
            Self::Csv => "formats:SPARQL_Results_CSV",
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct QualifiedEntity {
    query_ordinal: u64,
    iri: String,
    guid: Uuid,
    multiplicity: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct HydrationRequest {
    snapshot_id: Uuid,
    serving_root_sha256: String,
    entities: Vec<QualifiedEntity>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct LocatorRequest {
    snapshot_id: Uuid,
    serving_root_sha256: String,
    entities: Vec<QualifiedEntity>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct QueryResponse {
    dataset_id: Uuid,
    snapshot_id: Uuid,
    serving_root_sha256: String,
    query_sha256: String,
    query_form: QueryForm,
    authorized_graph_set_sha256: String,
    active_dataset_sha256: String,
    coverage_scope: String,
    complete: bool,
    routing: RoutingResponse,
    execution: ExecutionResponse,
    head: Vec<String>,
    bindings: Vec<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    boolean_result: Option<bool>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    graph_ntriples: Vec<String>,
    qualified_entities: Vec<QualifiedEntity>,
    hydrated_payload: Vec<OnlinePayloadRow>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    entailment: Option<ExactEntailmentEvidence>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    property_path_execution: Option<PropertyPathExecutionEvidence>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    federation: Option<FederationQueryEvidence>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct QueryLogParameters {
    dataset_id: Option<Uuid>,
    user_id: Option<String>,
    status: Option<String>,
    started_after_epoch_ms: Option<i64>,
    started_before_epoch_ms: Option<i64>,
    min_duration_ms: Option<i64>,
    limit: Option<usize>,
    offset: Option<usize>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct QueryLogPage {
    items: Vec<QueryLogView>,
    limit: usize,
    offset: usize,
    has_more: bool,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct QueryLogView {
    query_execution_id: Uuid,
    dataset_id: Uuid,
    snapshot_id: Option<Uuid>,
    request_id: String,
    user: QueryLogUser,
    sparql_query: Option<String>,
    query_sha256: String,
    query_form: Option<String>,
    execution_mode: Option<String>,
    status: String,
    resources: QueryLogResources,
    timing: QueryLogTiming,
    result_rows: Option<i64>,
    result_bytes: Option<i64>,
    cache_hit: Option<bool>,
    error_code: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct QueryLogUser {
    principal_id: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct QueryLogResources {
    participating_pods: Vec<String>,
    participating_nodes: Vec<String>,
    requested_cpu_millicores: Option<i64>,
    requested_ram_bytes: Option<i64>,
    allocated_cpu_millicores: Option<i64>,
    allocated_ram_bytes: Option<i64>,
    measured_cpu_time_ms: Option<i64>,
    measured_peak_rss_bytes: Option<i64>,
    measured_gpu_time_ms: Option<i64>,
    measured_gpu_peak_memory_bytes: Option<i64>,
    measurement_scope: String,
    autoscaling_events: serde_json::Value,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct QueryLogTiming {
    start_time_epoch: i64,
    start_time_epoch_ms: i64,
    end_time_epoch: Option<i64>,
    end_time_epoch_ms: Option<i64>,
    total_time_ms: Option<i64>,
    total_time: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct PropertyPathExecutionEvidence {
    mode: String,
    plan_set_sha256: String,
    path_count: u64,
    semantic_partition_count: u32,
    completed_iterations: u64,
    completed_work_items: u64,
    accepted_endpoint_count: u64,
    endpoint_set_sha256s: Vec<String>,
    scanned_adjacency_rows: u64,
    hot_split_work_items: u64,
    checkpoint_bytes: u64,
    worker_ids: Vec<String>,
    scalar_oracle_equivalence_required: bool,
    complete: bool,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct ExactEntailmentEvidence {
    regime: EntailmentRegime,
    bgp_count: u64,
    result_sha256s: Vec<String>,
    certificate_sha256s: Vec<String>,
    proof_manifest_sha256s: Vec<String>,
    certificates: Vec<DirectCertificate>,
    proof_manifests: Vec<DirectProofManifest>,
    distributed_algebra_plan_sha256: String,
    distributed_algebra_stage_count: u64,
    distributed_algebra_wave_count: u64,
    distributed_algebra_work_item_count: u64,
    distributed_algebra_partition_count: u32,
    distributed_algebra_scalar_equivalence_required: bool,
    distributed_property_path_plan_sha256: String,
    distributed_property_path_count: u64,
    distributed_property_path_automaton_sha256s: Vec<String>,
    distributed_property_path_partition_count: u32,
    distributed_property_path_scalar_equivalence_required: bool,
    complete: bool,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct RoutingResponse {
    selection_mode: String,
    dataset_selection_source: DatasetSelectionSource,
    default_graph_iris: Vec<String>,
    named_graph_iris: Vec<String>,
    active_dataset_sha256: String,
    include_internal_closure: bool,
    selected_graph_iris: Vec<String>,
    selected_graph_count: u32,
    total_graph_count: u32,
    capability_index_sha256: String,
    routed_dataset_sha256: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct ExecutionResponse {
    mode: String,
    exchange_format: String,
    fragment_ingress_mode: String,
    fragment_ingress_bytes: u64,
    fragment_materialization_mode: String,
    fragment_owned_rows: u64,
    shuffle_result_ingress_mode: String,
    shuffle_result_ingress_bytes: u64,
    intermediate_result_mode: String,
    assembled_intermediate_owned_rows: u64,
    fragment_count: u32,
    worker_count: u32,
    shuffle_partition_count: u32,
    shuffle_worker_count: u32,
    shuffle_spill_mode: String,
    shuffle_spill_bytes: u64,
    shuffle_cache_mode: String,
    shuffle_cache_hits: u32,
    worker_join_mode: String,
    worker_join_spill_bytes: u64,
    worker_join_grace_partitions: u32,
    worker_join_max_build_rows: u64,
    worker_input_mode: String,
    worker_input_bytes: u64,
    coordinator_request_mode: String,
    coordinator_request_bytes: u64,
    plan_sha256: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct FragmentExecutionRequest {
    snapshot_id: Uuid,
    manifest_sha256: String,
}

/// One exact, snapshot-bound scalar-oracle task executed by a fragment worker.
///
/// Phase 40.13.16 transports the complete rewritten SPARQL query rather than an approximate SQL
/// expression. Every requested replica evaluates the same typed algebra with the pinned scalar
/// oracle. The coordinator returns a result only after a dense set of distinct workers reports the
/// same canonical result checksum.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct DistributedAlgebraExecutionRequest {
    snapshot_id: Uuid,
    manifest_sha256: String,
    original_query: String,
    original_query_sha256: String,
    rewritten_query: String,
    rewritten_query_sha256: String,
    active_dataset: ResolvedDataset,
    max_solution_rows: usize,
    max_graph_triples: usize,
    max_graph_blank_nodes: usize,
    ordered: bool,
    replica: u32,
    replica_count: u32,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct DistributedAlgebraResultPayload {
    query_form: QueryForm,
    head: Vec<String>,
    bindings: Vec<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    boolean_result: Option<bool>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    graph_ntriples: Vec<String>,
    qualified_entity_iris: Vec<String>,
    coverage_scope: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct DistributedAlgebraExecutionResponse {
    dataset_id: Uuid,
    snapshot_id: Uuid,
    manifest_sha256: String,
    original_query_sha256: String,
    rewritten_query_sha256: String,
    result_sha256: String,
    replica: u32,
    replica_count: u32,
    worker_id: String,
    complete: bool,
    result: DistributedAlgebraResultPayload,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum PartitionPathAction {
    Seed,
    Expand,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct PartitionPathExecutionRequest {
    snapshot_id: Uuid,
    manifest_sha256: String,
    semantic_root_sha256: String,
    active_dataset: ResolvedDataset,
    plan_sha256: String,
    plan: DistributedPropertyPathPlan,
    iteration: u32,
    storage_partition: u32,
    action: PartitionPathAction,
    frontier: Vec<PathFrontierKey>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct PartitionPathExecutionResponse {
    dataset_id: Uuid,
    snapshot_id: Uuid,
    semantic_root_sha256: String,
    partition_manifest_sha256: String,
    forward_adjacency_sha256: String,
    reverse_adjacency_sha256: String,
    dictionary_sha256: String,
    plan_sha256: String,
    storage_partition: u32,
    iteration: u32,
    worker_id: String,
    response_sha256: String,
    batch: PartitionPathBatch,
    complete: bool,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct HydrationResponse {
    dataset_id: Uuid,
    snapshot_id: Uuid,
    serving_root_sha256: String,
    rows: Vec<OnlinePayloadRow>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct LocatorResponse {
    dataset_id: Uuid,
    snapshot_id: Uuid,
    serving_root_sha256: String,
    entities: Vec<LocatedEntity>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct LocatedEntity {
    query_ordinal: u64,
    guid: Uuid,
    records: Vec<ShardLocatorRecord>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct OnlinePayloadRow {
    query_ordinal: u64,
    multiplicity: u64,
    entity_guid: Uuid,
    subject_term: String,
    subject_resource_kind: ngkg_hydration::RdfResourceKind,
    predicate_iri: String,
    lexical_value: String,
    datatype_iri: Option<String>,
    language: Option<String>,
    graph_iri: String,
}

#[derive(Debug, Error)]
enum OnlineError {
    #[error("authentication is required")]
    Unauthenticated,
    #[error("the principal lacks the required permission")]
    Forbidden,
    #[error("query execution log was not found")]
    QueryLogNotFound,
    #[error(
        "the principal is not authorized for every graph required by this exact query and its inference proofs"
    )]
    GraphForbidden,
    #[error("request is invalid: {0}")]
    Request(String),
    #[error("SPARQL Protocol request is malformed: {0}")]
    MalformedProtocol(String),
    #[error("SPARQL query is malformed: {0}")]
    MalformedSparql(String),
    #[error("SPARQL query exceeds the configured byte ceiling")]
    QueryTooLarge,
    #[error("the request media type is not supported: {0}")]
    UnsupportedMediaType(String),
    #[error("none of the requested response media types is supported")]
    NotAcceptable,
    #[error("the resolved active SPARQL dataset is not certified for this query")]
    ActiveDatasetNotCertified,
    #[error("the active serving snapshot conflicts with the request: {0}")]
    SnapshotConflict(String),
    #[error("native distributed execution is unavailable: {0}")]
    NativeCutoverUnavailable(String),
    #[error("catalog access failed: {0}")]
    Catalog(#[from] CatalogError),
    #[error("object storage failed: {0}")]
    Store(#[from] ArtifactStoreError),
    #[error("semantic execution failed: {0}")]
    Reference(#[from] ReferenceRuntimeError),
    #[error("locator access failed: {0}")]
    Locator(#[from] LocatorError),
    #[error("payload hydration failed: {0}")]
    Hydration(#[from] HydrationError),
    #[error("identity resolution failed: {0}")]
    Identity(#[from] IdentityError),
    #[error("shuffle result cache failed: {0}")]
    ShuffleCache(#[from] ShuffleCacheError),
    #[error("worker Grace join failed: {0}")]
    GraceJoin(#[from] GraceJoinError),
    #[error("certified query result cache failed: {0}")]
    QueryCache(#[from] QueryCacheError),
    #[error("cache I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("manifest JSON failed: {0}")]
    Json(#[from] serde_json::Error),
    #[error("blocking execution failed: {0}")]
    Join(#[from] tokio::task::JoinError),
    #[error("upstream dependency failed: {0}")]
    Upstream(String),
    #[error("upstream dependency timed out: {0}")]
    GatewayTimeout(String),
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ErrorBody {
    code: &'static str,
    message: String,
}

impl OnlineError {
    const fn audit_code(&self) -> &'static str {
        match self {
            Self::Unauthenticated => "UNAUTHENTICATED",
            Self::Forbidden => "FORBIDDEN",
            Self::QueryLogNotFound => "QUERY_LOG_NOT_FOUND",
            Self::GraphForbidden => "GRAPH_FORBIDDEN",
            Self::Request(_) => "INVALID_QUERY_REQUEST",
            Self::MalformedProtocol(_) => "MALFORMED_SPARQL_PROTOCOL_REQUEST",
            Self::MalformedSparql(_) => "MALFORMED_SPARQL_QUERY",
            Self::QueryTooLarge => "SPARQL_QUERY_TOO_LARGE",
            Self::UnsupportedMediaType(_) => "UNSUPPORTED_SPARQL_MEDIA_TYPE",
            Self::NotAcceptable => "SPARQL_RESULT_FORMAT_NOT_ACCEPTABLE",
            Self::ActiveDatasetNotCertified => "ACTIVE_DATASET_NOT_CERTIFIED",
            Self::SnapshotConflict(_) => "SNAPSHOT_CONFLICT",
            Self::NativeCutoverUnavailable(_) => "NATIVE_CUTOVER_UNAVAILABLE",
            Self::Reference(ReferenceRuntimeError::UncertifiedQuery) => "UNCERTIFIED_QUERY",
            Self::GatewayTimeout(_) => "UPSTREAM_TIMEOUT",
            _ => "SERVING_DEPENDENCY_FAILED",
        }
    }
}

impl IntoResponse for OnlineError {
    fn into_response(self) -> Response {
        let (status, code, message) = match &self {
            Self::Unauthenticated => (
                StatusCode::UNAUTHORIZED,
                "UNAUTHENTICATED",
                "authentication is required".to_owned(),
            ),
            Self::Forbidden => (
                StatusCode::FORBIDDEN,
                "FORBIDDEN",
                "the principal lacks the required permission".to_owned(),
            ),
            Self::QueryLogNotFound => (
                StatusCode::NOT_FOUND,
                "QUERY_LOG_NOT_FOUND",
                "query execution log was not found".to_owned(),
            ),
            Self::GraphForbidden => (
                StatusCode::FORBIDDEN,
                "GRAPH_FORBIDDEN",
                "the principal is not authorized for every graph required by this exact query and its inference proofs"
                    .to_owned(),
            ),
            Self::Request(message) => (
                StatusCode::UNPROCESSABLE_ENTITY,
                "INVALID_QUERY_REQUEST",
                message.clone(),
            ),
            Self::MalformedProtocol(message) => (
                StatusCode::BAD_REQUEST,
                "MALFORMED_SPARQL_PROTOCOL_REQUEST",
                message.clone(),
            ),
            Self::MalformedSparql(message) => (
                StatusCode::BAD_REQUEST,
                "MALFORMED_SPARQL_QUERY",
                message.clone(),
            ),
            Self::QueryTooLarge => (
                StatusCode::PAYLOAD_TOO_LARGE,
                "SPARQL_QUERY_TOO_LARGE",
                "SPARQL query exceeds the configured byte ceiling".to_owned(),
            ),
            Self::UnsupportedMediaType(message) => (
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "UNSUPPORTED_SPARQL_MEDIA_TYPE",
                message.clone(),
            ),
            Self::NotAcceptable => (
                StatusCode::NOT_ACCEPTABLE,
                "SPARQL_RESULT_FORMAT_NOT_ACCEPTABLE",
                "none of the requested media ranges allow a supported SPARQL 1.1 result representation"
                    .to_owned(),
            ),
            Self::ActiveDatasetNotCertified => (
                StatusCode::UNPROCESSABLE_ENTITY,
                "ACTIVE_DATASET_NOT_CERTIFIED",
                "the authorized dataset produced by protocol-over-query precedence does not match the offline query certificate"
                    .to_owned(),
            ),
            Self::GatewayTimeout(message) => (
                StatusCode::GATEWAY_TIMEOUT,
                "UPSTREAM_TIMEOUT",
                message.clone(),
            ),
            Self::SnapshotConflict(message) => (
                StatusCode::CONFLICT,
                "SNAPSHOT_CONFLICT",
                message.clone(),
            ),
            Self::NativeCutoverUnavailable(message) => (
                StatusCode::SERVICE_UNAVAILABLE,
                "NATIVE_CUTOVER_UNAVAILABLE",
                message.clone(),
            ),
            Self::Catalog(CatalogError::NotFound) => {
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    "NO_ACTIVE_SERVING_SNAPSHOT",
                    "no active certified serving snapshot is available".to_owned(),
                )
            }
            Self::Reference(ReferenceRuntimeError::UncertifiedQuery) => {
                (
                    StatusCode::UNPROCESSABLE_ENTITY,
                    "UNCERTIFIED_QUERY",
                    "the exact query bytes are not certified for the active snapshot".to_owned(),
                )
            }
            _ => (
                StatusCode::SERVICE_UNAVAILABLE,
                "SERVING_DEPENDENCY_FAILED",
                "a required serving dependency failed".to_owned(),
            ),
        };
        if status.is_server_error() {
            tracing::error!(error = %self, %code, "online serving request failed");
        }
        (status, Json(ErrorBody { code, message })).into_response()
    }
}

impl From<AuthError> for OnlineError {
    fn from(value: AuthError) -> Self {
        match value {
            AuthError::Unauthenticated => Self::Unauthenticated,
            AuthError::Forbidden => Self::Forbidden,
        }
    }
}

fn main() -> Result<()> {
    let role = parse_role(env::args().nth(1).as_deref())?;
    let control_threads = positive_usize("NGKG_CONTROL_THREADS")?;
    let compute_threads = positive_usize("NGKG_RUST_COMPUTE_THREADS")?;
    let blocking_io_threads = positive_usize("NGKG_BLOCKING_IO_THREADS")?;
    let runtime_capabilities = capability_report(ThreadBudget {
        rust_compute: compute_threads,
        blocking_io: blocking_io_threads,
        openmp: positive_usize("OMP_NUM_THREADS")?,
        blas: positive_usize("OPENBLAS_NUM_THREADS")?,
        control: control_threads,
    })?;
    let blocking_threads = if role == Role::Hydration {
        blocking_io_threads
    } else {
        compute_threads
            .checked_add(blocking_io_threads)
            .context("Rust blocking thread budget overflow")?
    };
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(control_threads)
        .max_blocking_threads(blocking_threads)
        .enable_all()
        .build()?
        .block_on(async_main(role, compute_threads, runtime_capabilities))
}

async fn async_main(
    role: Role,
    rust_compute_threads: usize,
    runtime_capabilities: CapabilityReport,
) -> Result<()> {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter("info")
        .init();
    tracing::info!(
        ?role,
        cpuset_cores = runtime_capabilities.cpuset_cores,
        cpuset_source = %runtime_capabilities.cpuset_source,
        rust_compute_threads = runtime_capabilities.budget.rust_compute,
        blocking_io_threads = runtime_capabilities.budget.blocking_io,
        openmp_threads = runtime_capabilities.budget.openmp,
        blas_threads = runtime_capabilities.budget.blas,
        control_threads = runtime_capabilities.budget.control,
        node_saturation_target_percent = runtime_capabilities.node_saturation_target_percent,
        "validated shared Rust/OpenMP/BLAS Kubernetes CPU budget"
    );
    let phase40_admission = TrustedPhase40AdmissionCeilings::from_env()?;
    let direct_bgp_classification_limits =
        phase40_admission.classifier_limits(rust_compute_threads)?;
    let phase40_admission_ceiling_sha256 = phase40_admission.bundle_sha256()?;
    tracing::info!(
        ?role,
        configured_max_bgps = phase40_admission.max_bgps,
        configured_max_triples_per_bgp = phase40_admission.max_triples_per_bgp,
        configured_max_classification_cpu_lanes = phase40_admission.max_classification_cpu_lanes,
        effective_classification_cpu_lanes = direct_bgp_classification_limits.max_cpu_lanes,
        phase40_admission_ceiling_sha256 = %phase40_admission_ceiling_sha256,
        "trusted Phase 40 online admission ceilings loaded"
    );
    let online_direct = load_online_direct_config(role)?;
    let federation = load_federation_registry(role)?;
    if federation.as_ref().is_some_and(|registry| {
        registry.max_concurrent_calls()
            > rust_compute_threads.saturating_add(runtime_capabilities.budget.blocking_io)
    }) {
        anyhow::bail!(
            "federation concurrency exceeds the cgroup-aware Rust compute plus blocking-I/O lane budget"
        );
    }
    let bind: SocketAddr = required("NGKG_BIND_ADDR")?
        .parse()
        .context("NGKG_BIND_ADDR must be a socket address")?;
    let pool = PgPoolOptions::new()
        .max_connections(positive_u32("NGKG_DATABASE_MAX_CONNECTIONS")?)
        .connect(&required("NGKG_DATABASE_URL")?)
        .await?;
    let manager = Arc::new(ServingStateManager {
        catalog: OperationRepository::new(pool),
        store: ArtifactStore::from_base_url(&required("NGKG_ARTIFACT_BASE_URL")?)?,
        cache_root: absolute_path("NGKG_CACHE_ROOT")?,
        max_object_bytes: positive_u64("NGKG_MAX_OBJECT_BYTES")?,
        max_payload_cache_bytes: positive_u64("NGKG_MAX_PAYLOAD_CACHE_BYTES")?,
        max_resident_query_routes: positive_usize("NGKG_MAX_RESIDENT_QUERY_ROUTES")?,
        max_resident_fragment_runtimes: positive_usize("NGKG_MAX_RESIDENT_FRAGMENT_RUNTIMES")?,
        authorization: Mutex::new(BTreeMap::new()),
        semantic: Mutex::new(BTreeMap::new()),
        physical: Mutex::new(BTreeMap::new()),
        authorization_loads: Mutex::new(BTreeMap::new()),
        semantic_loads: Mutex::new(BTreeMap::new()),
        physical_loads: Mutex::new(BTreeMap::new()),
    });
    tokio::fs::create_dir_all(&manager.cache_root).await?;
    let hydration_url = optional("NGKG_HYDRATION_URL");
    let fragment_service = optional("NGKG_FRAGMENT_SERVICE");
    if role == Role::Query && hydration_url.is_none() {
        anyhow::bail!("NGKG_HYDRATION_URL is required for the query role");
    }
    if role == Role::Query && fragment_service.is_none() {
        anyhow::bail!("NGKG_FRAGMENT_SERVICE is required for the query role");
    }
    let shuffle_spill_root = if role == Role::Query {
        let root = absolute_path("NGKG_SHUFFLE_SPILL_ROOT")?;
        prepare_shuffle_spill_root(&root).map_err(anyhow::Error::new)?;
        Some(root)
    } else {
        None
    };
    let fragment_response_spool = if role == Role::Query {
        Some(Arc::new(FragmentResponseSpool::open(
            &absolute_path("NGKG_FRAGMENT_RESPONSE_SPOOL_ROOT")?,
            positive_u64("NGKG_MAX_FRAGMENT_RESPONSE_SPOOL_BYTES")?,
        )?))
    } else {
        None
    };
    let (shuffle_result_cache, max_shuffle_cache_entry_bytes) = if role == Role::Fragment {
        let root = absolute_path("NGKG_SHUFFLE_CACHE_ROOT")?;
        let max_bytes = positive_u64("NGKG_MAX_SHUFFLE_CACHE_BYTES")?;
        let max_entries = positive_usize("NGKG_MAX_SHUFFLE_CACHE_ENTRIES")?;
        let max_entry_bytes = positive_usize("NGKG_MAX_SHUFFLE_CACHE_ENTRY_BYTES")?;
        let cache = ShuffleResultCache::open(
            &root,
            max_bytes,
            max_entries,
            u64::try_from(max_entry_bytes)
                .context("shuffle cache entry ceiling exceeds this platform")?,
        )
        .map_err(anyhow::Error::new)?;
        (Some(Arc::new(cache)), max_entry_bytes)
    } else {
        (None, 0)
    };
    let shuffle_request_spool = if role == Role::Fragment {
        Some(Arc::new(StreamingRequestSpool::open(
            &absolute_path("NGKG_STREAMING_REQUEST_SPOOL_ROOT")?,
            positive_u64("NGKG_MAX_STREAMING_REQUEST_SPOOL_BYTES")?,
        )?))
    } else {
        None
    };
    let (worker_join_bucket_count, max_worker_join_build_rows) =
        if matches!(role, Role::Query | Role::Fragment) {
            (
                positive_u32("NGKG_WORKER_JOIN_BUCKETS")?,
                positive_usize("NGKG_MAX_WORKER_JOIN_BUILD_ROWS")?,
            )
        } else {
            (2, 1)
        };
    let in_memory_join_build_rows = if role == Role::Fragment {
        positive_usize("NGKG_IN_MEMORY_JOIN_BUILD_ROWS")?
    } else {
        1
    };
    let grace_join_engine = if role == Role::Fragment {
        let engine = GraceJoinEngine::open(
            &absolute_path("NGKG_WORKER_JOIN_SPILL_ROOT")?,
            positive_u64("NGKG_MAX_WORKER_JOIN_SPILL_BYTES")?,
            positive_u64("NGKG_MAX_WORKER_JOIN_SPILL_BYTES_PER_REQUEST")?,
            worker_join_bucket_count,
            positive_usize("NGKG_MAX_WORKER_JOIN_OPEN_FILES")?,
            max_worker_join_build_rows,
            positive_usize("NGKG_MAX_WORKER_JOIN_PROBE_ROWS")?,
            positive_usize("NGKG_MAX_WORKER_JOIN_ROW_BYTES")?,
            in_memory_join_build_rows,
        )
        .map_err(anyhow::Error::new)?;
        Some(Arc::new(engine))
    } else {
        None
    };
    let (query_result_cache, max_query_cache_entry_bytes) = if role == Role::Query {
        let root = absolute_path("NGKG_QUERY_RESULT_CACHE_ROOT")?;
        let max_bytes = positive_u64("NGKG_MAX_QUERY_RESULT_CACHE_BYTES")?;
        let max_entries = positive_usize("NGKG_MAX_QUERY_RESULT_CACHE_ENTRIES")?;
        let max_entry_bytes = positive_usize("NGKG_MAX_QUERY_RESULT_CACHE_ENTRY_BYTES")?;
        let cache = QueryResultCache::open(
            &root,
            max_bytes,
            max_entries,
            u64::try_from(max_entry_bytes)
                .context("query cache entry ceiling exceeds this platform")?,
        )
        .map_err(anyhow::Error::new)?;
        (Some(Arc::new(cache)), max_entry_bytes)
    } else {
        (None, 0)
    };
    let fragment_timeout_seconds = positive_u64("NGKG_FRAGMENT_TIMEOUT_SECONDS")?;
    let hydration_timeout_seconds = positive_u64("NGKG_HYDRATION_TIMEOUT_SECONDS")?;
    let admission_limits = [
        positive_usize("NGKG_MAX_QUERY_IN_FLIGHT")?,
        positive_usize("NGKG_MAX_FRAGMENT_IN_FLIGHT")?,
        positive_usize("NGKG_MAX_SHUFFLE_IN_FLIGHT")?,
        positive_usize("NGKG_MAX_LOCATOR_IN_FLIGHT")?,
        positive_usize("NGKG_MAX_HYDRATION_IN_FLIGHT")?,
    ];
    let admission_pending_limits = [
        positive_usize("NGKG_MAX_QUERY_PENDING")?,
        positive_usize("NGKG_MAX_FRAGMENT_PENDING")?,
        positive_usize("NGKG_MAX_SHUFFLE_PENDING")?,
        positive_usize("NGKG_MAX_LOCATOR_PENDING")?,
        positive_usize("NGKG_MAX_HYDRATION_PENDING")?,
    ];
    let fragment_worker_limit = positive_usize("NGKG_MAX_FRAGMENT_WORKER_IN_FLIGHT")?;
    if admission_limits
        .iter()
        .chain(admission_pending_limits.iter())
        .chain(std::iter::once(&fragment_worker_limit))
        .any(|limit| *limit > Semaphore::MAX_PERMITS)
    {
        anyhow::bail!("an admission limit exceeds the Tokio semaphore ceiling");
    }
    if admission_limits[AdmissionClass::Fragment.index()] > fragment_worker_limit
        || admission_limits[AdmissionClass::Shuffle.index()] > fragment_worker_limit
    {
        anyhow::bail!(
            "fragment and shuffle admission limits cannot exceed the shared fragment-worker limit"
        );
    }
    let admission_wait_milliseconds = positive_u64("NGKG_ADMISSION_WAIT_MILLISECONDS")?;
    if admission_wait_milliseconds > 5000 {
        anyhow::bail!("NGKG_ADMISSION_WAIT_MILLISECONDS cannot exceed 5000");
    }
    let authorizer = Arc::new(
        TokenAuthorizer::load(
            &absolute_path("NGKG_AUTH_TOKENS_FILE")?,
            &required("NGKG_AUTH_TOKENS_FILE_SHA256")?,
        )
        .map_err(anyhow::Error::msg)?,
    );
    let tenant_admission = TenantAdmissionRegistry::load(
        &absolute_path("NGKG_TENANT_ADMISSION_POLICY_FILE")?,
        &required("NGKG_TENANT_ADMISSION_POLICY_SHA256")?,
        positive_usize("NGKG_MAX_ADMISSION_TENANTS")?,
        &authorizer.query_tenant_ids(),
        admission_limits,
        admission_pending_limits,
        fragment_worker_limit,
    )
    .map_err(anyhow::Error::msg)?;
    tracing::info!(
        tenant_count = tenant_admission.tenant_count(),
        policy_sha256 = tenant_admission.policy_sha256(),
        "checksum-bound tenant admission policy loaded"
    );
    let admission = Arc::new(AdmissionController::new(
        admission_limits,
        admission_pending_limits,
        fragment_worker_limit,
        tenant_admission,
        Duration::from_millis(admission_wait_milliseconds),
    ));
    let query_logs = load_query_log_config(role)?;
    let max_request_bytes = positive_usize("NGKG_MAX_REQUEST_BYTES")?;
    let state = AppState {
        role,
        authorizer,
        manager,
        http: HttpClient::builder()
            .timeout(Duration::from_secs(hydration_timeout_seconds))
            .build()?,
        fragment_http: HttpClient::builder()
            .timeout(Duration::from_secs(fragment_timeout_seconds))
            .build()?,
        reasoner_http: HttpClient::builder()
            .timeout(
                online_direct
                    .as_ref()
                    .map_or(Duration::from_secs(1), |config| {
                        config
                            .limits
                            .reasoner_timeout
                            .saturating_add(Duration::from_secs(10))
                    }),
            )
            .build()?,
        hydration_url,
        fragment_service,
        max_request_bytes,
        max_query_bytes: positive_usize("NGKG_MAX_QUERY_BYTES")?,
        max_query_response_bytes: positive_usize("NGKG_MAX_QUERY_RESPONSE_BYTES")?,
        max_query_result_rows: positive_usize("NGKG_MAX_QUERY_RESULT_ROWS")?,
        max_query_graph_triples: positive_usize("NGKG_MAX_QUERY_GRAPH_TRIPLES")?,
        max_query_graph_blank_nodes: positive_usize("NGKG_MAX_QUERY_GRAPH_BLANK_NODES")?,
        query_timeout: Duration::from_secs(positive_u64("NGKG_QUERY_TIMEOUT_SECONDS")?),
        max_qualified_entities: positive_usize("NGKG_MAX_QUALIFIED_ENTITIES")?,
        max_hydration_rows: positive_u64("NGKG_MAX_HYDRATION_ROWS")?,
        max_hydration_response_bytes: positive_usize("NGKG_MAX_HYDRATION_RESPONSE_BYTES")?,
        hydration_worker_threads: positive_usize("NGKG_HYDRATION_WORKER_THREADS")?,
        max_distributed_fragments: positive_usize("NGKG_MAX_DISTRIBUTED_FRAGMENTS")?,
        max_distributed_intermediate_rows: positive_usize(
            "NGKG_MAX_DISTRIBUTED_INTERMEDIATE_ROWS",
        )?,
        max_distributed_exchange_bytes: positive_usize("NGKG_MAX_DISTRIBUTED_EXCHANGE_BYTES")?,
        max_fragment_response_bytes: positive_usize("NGKG_MAX_FRAGMENT_RESPONSE_BYTES")?,
        fragment_response_spool,
        fragment_arrow_batch_rows: positive_usize("NGKG_FRAGMENT_ARROW_BATCH_ROWS")?,
        fragment_arrow_http_chunk_bytes: positive_usize("NGKG_FRAGMENT_ARROW_HTTP_CHUNK_BYTES")?,
        fragment_arrow_channel_capacity: positive_usize("NGKG_FRAGMENT_ARROW_CHANNEL_CAPACITY")?,
        fragment_exchange_concurrency: positive_usize("NGKG_FRAGMENT_EXCHANGE_CONCURRENCY")?,
        distributed_algebra_enabled: required_bool("NGKG_DISTRIBUTED_ALGEBRA_ENABLED")?,
        distributed_algebra_replicas: positive_usize("NGKG_DISTRIBUTED_ALGEBRA_REPLICAS")?,
        native_cutover_mode: required("NGKG_NATIVE_CUTOVER_MODE")?.parse()?,
        shuffle_partition_count: positive_u32("NGKG_SHUFFLE_PARTITIONS")?,
        max_shuffle_request_bytes: positive_usize("NGKG_MAX_SHUFFLE_REQUEST_BYTES")?,
        shuffle_request_spool,
        max_shuffle_response_bytes: positive_usize("NGKG_MAX_SHUFFLE_RESPONSE_BYTES")?,
        max_shuffle_exchange_bytes: positive_usize("NGKG_MAX_SHUFFLE_EXCHANGE_BYTES")?,
        shuffle_exchange_concurrency: positive_usize("NGKG_SHUFFLE_EXCHANGE_CONCURRENCY")?,
        shuffle_spill_root,
        max_shuffle_spill_bytes: positive_u64("NGKG_MAX_SHUFFLE_SPILL_BYTES")?,
        max_shuffle_open_files: positive_usize("NGKG_MAX_SHUFFLE_OPEN_FILES")?,
        property_path_max_iterations: positive_u32("NGKG_PROPERTY_PATH_MAX_ITERATIONS")?,
        property_path_max_frontier_items: positive_u64("NGKG_PROPERTY_PATH_MAX_FRONTIER_ITEMS")?,
        property_path_max_visited_items: positive_u64("NGKG_PROPERTY_PATH_MAX_VISITED_ITEMS")?,
        property_path_max_checkpoint_bytes: positive_u64(
            "NGKG_PROPERTY_PATH_MAX_CHECKPOINT_BYTES",
        )?,
        property_path_max_spill_bytes: positive_u64("NGKG_PROPERTY_PATH_MAX_SPILL_BYTES")?,
        property_path_hot_vertex_degree: positive_u64("NGKG_PROPERTY_PATH_HOT_VERTEX_DEGREE")?,
        property_path_max_hot_vertex_splits: positive_u32(
            "NGKG_PROPERTY_PATH_MAX_HOT_VERTEX_SPLITS",
        )?,
        partition_native_paths_enabled: required_bool("NGKG_PARTITION_NATIVE_PATHS_ENABLED")?,
        property_path_worker_threads: positive_usize("NGKG_PROPERTY_PATH_WORKER_THREADS")?,
        property_path_max_scan_rows: positive_u64("NGKG_PROPERTY_PATH_MAX_SCAN_ROWS")?,
        property_path_core_lanes: Arc::new(Semaphore::new(rust_compute_threads)),
        shuffle_result_cache,
        shuffle_cache_flights: Arc::new(Mutex::new(BTreeMap::new())),
        max_shuffle_cache_entry_bytes,
        grace_join_engine,
        worker_join_bucket_count,
        max_worker_join_build_rows,
        in_memory_join_build_rows,
        query_result_cache,
        query_cache_flights: Arc::new(Mutex::new(BTreeMap::new())),
        max_query_cache_entry_bytes,
        admission,
        worker_id: optional("NGKG_WORKER_ID").unwrap_or_else(|| format!("{role:?}")),
        // Phase 36 deliberately has no environment-variable switch for claims.
        // A later qualification phase must load these gates from checksum-bound
        // conformance evidence instead of trusting operator-provided booleans.
        standards_features: StandardsFeatureGates::default(),
        direct_bgp_classification_limits,
        phase40_admission_ceiling_sha256,
        online_direct,
        federation,
        query_logs,
        runtime_capabilities,
    };
    validate_standards_feature_implications(state.standards_features)
        .map_err(anyhow::Error::msg)?;
    if state.max_fragment_response_bytes > state.max_distributed_exchange_bytes {
        anyhow::bail!(
            "NGKG_MAX_FRAGMENT_RESPONSE_BYTES cannot exceed NGKG_MAX_DISTRIBUTED_EXCHANGE_BYTES"
        );
    }
    if state.fragment_response_spool.as_ref().is_some_and(|spool| {
        u64::try_from(state.max_distributed_exchange_bytes)
            .ok()
            .is_none_or(|maximum| maximum > spool.max_active_bytes)
    }) {
        anyhow::bail!(
            "NGKG_MAX_DISTRIBUTED_EXCHANGE_BYTES cannot exceed NGKG_MAX_FRAGMENT_RESPONSE_SPOOL_BYTES"
        );
    }
    if state.fragment_exchange_concurrency > state.max_distributed_fragments {
        anyhow::bail!(
            "NGKG_FRAGMENT_EXCHANGE_CONCURRENCY cannot exceed NGKG_MAX_DISTRIBUTED_FRAGMENTS"
        );
    }
    if state.distributed_algebra_enabled && state.distributed_algebra_replicas < 2 {
        anyhow::bail!("distributed algebra requires at least two scalar-oracle replicas");
    }
    if state.native_cutover_mode.requires_native()
        && (!state.distributed_algebra_enabled || !state.partition_native_paths_enabled)
    {
        anyhow::bail!(
            "required native cutover needs distributed algebra and partition-native paths"
        );
    }
    if state.distributed_algebra_replicas > state.max_distributed_fragments
        || state.distributed_algebra_replicas > state.fragment_exchange_concurrency
    {
        anyhow::bail!(
            "distributed algebra replicas must fit fragment and exchange concurrency ceilings"
        );
    }
    if state.fragment_arrow_batch_rows > state.max_distributed_intermediate_rows {
        anyhow::bail!(
            "NGKG_FRAGMENT_ARROW_BATCH_ROWS cannot exceed NGKG_MAX_DISTRIBUTED_INTERMEDIATE_ROWS"
        );
    }
    if state.max_query_result_rows > state.max_distributed_intermediate_rows {
        anyhow::bail!(
            "NGKG_MAX_QUERY_RESULT_ROWS cannot exceed NGKG_MAX_DISTRIBUTED_INTERMEDIATE_ROWS"
        );
    }
    if state.max_query_graph_blank_nodes > state.max_query_graph_triples.saturating_mul(2) {
        anyhow::bail!(
            "NGKG_MAX_QUERY_GRAPH_BLANK_NODES cannot exceed twice NGKG_MAX_QUERY_GRAPH_TRIPLES"
        );
    }
    if state.fragment_arrow_http_chunk_bytes > state.max_fragment_response_bytes {
        anyhow::bail!(
            "NGKG_FRAGMENT_ARROW_HTTP_CHUNK_BYTES cannot exceed NGKG_MAX_FRAGMENT_RESPONSE_BYTES"
        );
    }
    if state
        .fragment_arrow_http_chunk_bytes
        .checked_mul(state.fragment_arrow_channel_capacity)
        .is_none_or(|buffered| buffered > state.max_fragment_response_bytes)
    {
        anyhow::bail!(
            "Arrow HTTP chunk bytes multiplied by channel capacity cannot exceed the fragment response ceiling"
        );
    }
    if state.max_query_response_bytes < state.max_hydration_response_bytes {
        anyhow::bail!(
            "NGKG_MAX_QUERY_RESPONSE_BYTES cannot be smaller than NGKG_MAX_HYDRATION_RESPONSE_BYTES"
        );
    }
    if role == Role::Query
        && state
            .max_query_response_bytes
            .checked_add(QUERY_CACHE_HEADER_BYTES)
            .is_none_or(|bytes| bytes > state.max_query_cache_entry_bytes)
    {
        anyhow::bail!(
            "NGKG_MAX_QUERY_RESULT_CACHE_ENTRY_BYTES must cover the query response plus cache header"
        );
    }
    if state.shuffle_partition_count < 2 {
        anyhow::bail!("NGKG_SHUFFLE_PARTITIONS must be at least two");
    }
    if state.property_path_max_hot_vertex_splits < 2 {
        anyhow::bail!("NGKG_PROPERTY_PATH_MAX_HOT_VERTEX_SPLITS must be at least two");
    }
    if state.partition_native_paths_enabled
        && (state.property_path_worker_threads > state.fragment_exchange_concurrency
            || state.property_path_worker_threads > rust_compute_threads)
    {
        anyhow::bail!(
            "property-path worker threads must fit the fragment exchange and cgroup Rust budgets"
        );
    }
    if state.property_path_max_visited_items < state.property_path_max_frontier_items {
        anyhow::bail!(
            "NGKG_PROPERTY_PATH_MAX_VISITED_ITEMS cannot be smaller than the frontier ceiling"
        );
    }
    if state
        .property_path_max_frontier_items
        .checked_mul(128)
        .is_none_or(|bytes| {
            usize::try_from(bytes)
                .ok()
                .is_none_or(|bytes| bytes > state.max_shuffle_request_bytes)
        })
    {
        anyhow::bail!(
            "property-path frontier ceiling cannot fit the bounded internal request envelope"
        );
    }
    if state.property_path_max_checkpoint_bytes > state.property_path_max_spill_bytes
        || state.property_path_max_spill_bytes > state.max_shuffle_spill_bytes
    {
        anyhow::bail!(
            "property-path checkpoint/spill ceilings must fit the shared bounded shuffle spill budget"
        );
    }
    if state.worker_join_bucket_count < 2 {
        anyhow::bail!("NGKG_WORKER_JOIN_BUCKETS must be at least two");
    }
    if state.max_worker_join_build_rows > state.max_distributed_intermediate_rows {
        anyhow::bail!(
            "NGKG_MAX_WORKER_JOIN_BUILD_ROWS cannot exceed the distributed intermediate row ceiling"
        );
    }
    if state.max_shuffle_request_bytes > state.max_shuffle_exchange_bytes
        || state.max_shuffle_response_bytes > state.max_shuffle_exchange_bytes
    {
        anyhow::bail!(
            "shuffle request and response byte ceilings cannot exceed the total exchange ceiling"
        );
    }
    if state.shuffle_request_spool.as_ref().is_some_and(|spool| {
        u64::try_from(state.max_shuffle_request_bytes)
            .ok()
            .is_none_or(|maximum| maximum > spool.max_active_bytes)
    }) {
        anyhow::bail!(
            "NGKG_MAX_SHUFFLE_REQUEST_BYTES cannot exceed NGKG_MAX_STREAMING_REQUEST_SPOOL_BYTES"
        );
    }
    if state.shuffle_exchange_concurrency
        > usize::try_from(state.shuffle_partition_count).unwrap_or(usize::MAX)
    {
        anyhow::bail!("shuffle exchange concurrency cannot exceed the partition count");
    }
    if usize::try_from(state.shuffle_partition_count)
        .ok()
        .and_then(|partitions| partitions.checked_mul(2))
        .is_none_or(|files| files > state.max_shuffle_open_files)
    {
        anyhow::bail!("two shuffle spill files per partition must fit the open-file ceiling");
    }
    if state.max_shuffle_request_bytes > max_request_bytes {
        anyhow::bail!("NGKG_MAX_SHUFFLE_REQUEST_BYTES cannot exceed NGKG_MAX_REQUEST_BYTES");
    }
    if state
        .fragment_arrow_http_chunk_bytes
        .checked_mul(state.fragment_arrow_channel_capacity)
        .is_none_or(|buffered| buffered > state.max_shuffle_response_bytes)
    {
        anyhow::bail!(
            "Arrow HTTP chunk bytes multiplied by channel capacity cannot exceed the shuffle response ceiling"
        );
    }
    if state
        .fragment_arrow_http_chunk_bytes
        .checked_mul(state.fragment_arrow_channel_capacity)
        .is_none_or(|buffered| buffered > state.max_shuffle_request_bytes)
    {
        anyhow::bail!(
            "Arrow HTTP chunk bytes multiplied by channel capacity cannot exceed the shuffle request ceiling"
        );
    }
    let app = role_router(role)
        .route("/health/live", get(|| async { StatusCode::NO_CONTENT }))
        .route("/health/ready", get(ready))
        .route("/metrics", get(metrics))
        .route("/openapi.yaml", get(openapi))
        .route("/openapi.json", get(openapi_json))
        .route("/docs", get(swagger_ui_root))
        .route("/docs/{*asset}", get(swagger_ui_asset))
        .layer(DefaultBodyLimit::max(max_request_bytes))
        .layer(TraceLayer::new_for_http())
        .layer(middleware::from_fn_with_state(
            state.clone(),
            admission_middleware,
        ))
        .with_state(state);
    let listener = tokio::net::TcpListener::bind(bind).await?;
    tracing::info!(?role, %bind, "NGKG online serving replica is listening");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    Ok(())
}

fn role_router(role: Role) -> Router<AppState> {
    match role {
        Role::Query => Router::new()
            .route("/v1/hpc/capabilities", get(get_hpc_capabilities))
            .route("/v1/query_logs", get(list_query_logs))
            .route("/v1/query_logs/{query_execution_id}", get(get_query_log))
            .route("/v1/datasets/{dataset_id}/query", post(query))
            .route(
                "/v1/datasets/{dataset_id}/sparql/direct/validate",
                post(validate_direct_bgps),
            )
            .route(
                "/v1/datasets/{dataset_id}/sparql/direct/route",
                post(route_direct_bgps),
            )
            .route(
                "/v1/datasets/{dataset_id}/sparql",
                get(sparql_get).post(sparql_post),
            )
            .route(
                "/v1/datasets/{dataset_id}/sparql/service-description",
                get(sparql_service_description),
            ),
        Role::Fragment => Router::new()
            .route(
                "/v1/datasets/{dataset_id}/fragments/{query_sha256}/{fragment_id}/execute",
                post(execute_fragment),
            )
            .route(
                "/v1/datasets/{dataset_id}/shuffles/{query_sha256}/{stage}/{partition}/join",
                post(execute_shuffle_partition),
            )
            .route(
                "/v1/datasets/{dataset_id}/algebra/{query_sha256}/{replica}/execute",
                post(execute_distributed_algebra_replica),
            )
            .route(
                "/v1/datasets/{dataset_id}/paths/{query_sha256}/{path_id}/{iteration}/{partition}/expand",
                post(execute_partition_path),
            )
            .route(
                "/v1/datasets/{dataset_id}/native/leaves/{query_sha256}/{partition}/scan",
                post(execute_native_leaf_scan),
            ),
        Role::Locator => Router::new().route("/v1/datasets/{dataset_id}/locate", post(locate)),
        Role::Hydration => Router::new().route("/v1/datasets/{dataset_id}/hydrate", post(hydrate)),
    }
}

async fn ready(State(state): State<AppState>) -> StatusCode {
    match state.manager.catalog.ready().await {
        Ok(()) => StatusCode::NO_CONTENT,
        Err(error) => {
            tracing::error!(%error, role = ?state.role, "online readiness failed");
            StatusCode::SERVICE_UNAVAILABLE
        }
    }
}

const COMMON_OPENAPI_PATHS: &[&str] = &[
    "/docs",
    "/openapi.yaml",
    "/openapi.json",
    "/health/live",
    "/health/ready",
    "/metrics",
];

fn role_exposes_openapi_path(role: Role, path: &str) -> bool {
    if COMMON_OPENAPI_PATHS.contains(&path) {
        return true;
    }
    match role {
        Role::Query => matches!(
            path,
            "/v1/datasets/{datasetId}/sparql"
                | "/v1/datasets/{datasetId}/sparql/service-description"
                | "/v1/datasets/{datasetId}/sparql/direct/validate"
                | "/v1/datasets/{datasetId}/sparql/direct/route"
                | "/v1/datasets/{datasetId}/query"
                | "/v1/query_logs"
                | "/v1/query_logs/{queryExecutionId}"
                | "/v1/hpc/capabilities"
        ),
        Role::Fragment => matches!(
            path,
            "/v1/datasets/{datasetId}/fragments/{querySha256}/{fragmentId}/execute"
                | "/v1/datasets/{datasetId}/shuffles/{querySha256}/{stage}/{partition}/join"
                | "/v1/datasets/{datasetId}/algebra/{querySha256}/{replica}/execute"
                | "/v1/datasets/{datasetId}/paths/{querySha256}/{pathId}/{iteration}/{partition}/expand"
                | "/v1/datasets/{datasetId}/native/leaves/{querySha256}/{partition}/scan"
        ),
        Role::Locator => path == "/v1/datasets/{datasetId}/locate",
        Role::Hydration => path == "/v1/datasets/{datasetId}/hydrate",
    }
}

async fn get_hpc_capabilities(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<HpcCapabilitiesResponse>, OnlineError> {
    let _identity = state.authorizer.authorize(&headers)?;
    let memory = resource_envelope_report()
        .map_err(|error| OnlineError::Request(error.to_string()))?;
    let openmp_available = openmp_kernel_available();
    let mut execution_modes = vec!["rust"];
    if openmp_available {
        execution_modes.push("openmp");
    }
    Ok(Json(HpcCapabilitiesResponse {
        format_version: 1,
        role: format!("{:?}", state.role).to_ascii_lowercase(),
        worker_id: state.worker_id.clone(),
        local: state.runtime_capabilities.clone(),
        memory,
        parquet: HpcParquetCapabilities {
            projected_columns: true,
            bounded_arrow_batches: true,
            deterministic_rank_receipts: true,
            execution_modes,
        },
        mpi: HpcMpiCapabilities {
            online_query_participant: false,
            finite_batch_supported: true,
            one_rank_per_pod: true,
            elastic_rank_resize: false,
        },
        autoscaling_target_percent: state.runtime_capabilities.node_saturation_target_percent,
    }))
}

fn openmp_kernel_available() -> bool {
    env::var("NGKG_OPENMP_FILTER_EXECUTABLE")
        .ok()
        .map(PathBuf::from)
        .is_some_and(|path| {
            path.is_absolute()
                && fs::symlink_metadata(path)
                    .is_ok_and(|metadata| metadata.is_file() && !metadata.file_type().is_symlink())
        })
}

fn filtered_openapi_document(role: Role) -> Result<serde_json::Value, OnlineError> {
    let mut document =
        serde_yaml::from_str::<serde_json::Value>(include_str!("../../../api/online-openapi.yaml"))
            .map_err(|error| {
                OnlineError::SnapshotConflict(format!("embedded OpenAPI YAML is invalid: {error}"))
            })?;
    let paths = document
        .get_mut("paths")
        .and_then(serde_json::Value::as_object_mut)
        .ok_or_else(|| {
            OnlineError::SnapshotConflict(
                "embedded OpenAPI document does not contain a paths object".to_owned(),
            )
        })?;
    paths.retain(|path, _| role_exposes_openapi_path(role, path));

    let missing_common = COMMON_OPENAPI_PATHS
        .iter()
        .copied()
        .find(|path| !paths.contains_key(*path));
    if let Some(path) = missing_common {
        return Err(OnlineError::SnapshotConflict(format!(
            "embedded OpenAPI document is missing common runtime path {path}"
        )));
    }
    let has_role_path = paths
        .keys()
        .any(|path| !COMMON_OPENAPI_PATHS.contains(&path.as_str()));
    if !has_role_path {
        return Err(OnlineError::SnapshotConflict(format!(
            "embedded OpenAPI document has no operation paths for role {role:?}"
        )));
    }
    Ok(document)
}

async fn openapi(State(state): State<AppState>) -> Result<Response, OnlineError> {
    let document = filtered_openapi_document(state.role)?;
    let bytes = serde_yaml::to_string(&document)
        .map_err(|error| {
            OnlineError::SnapshotConflict(format!(
                "role-filtered OpenAPI YAML could not be serialized: {error}"
            ))
        })?
        .into_bytes();
    Ok(([(CONTENT_TYPE, "application/yaml; charset=utf-8")], bytes).into_response())
}

async fn openapi_json(State(state): State<AppState>) -> Result<Response, OnlineError> {
    let document = filtered_openapi_document(state.role)?;
    let bytes = serde_json::to_vec(&document)?;
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

async fn swagger_ui_root() -> Result<Response, OnlineError> {
    swagger_ui_response("")
}

async fn swagger_ui_asset(AxumPath(asset): AxumPath<String>) -> Result<Response, OnlineError> {
    swagger_ui_response(&asset)
}

fn swagger_ui_response(asset: &str) -> Result<Response, OnlineError> {
    let Some(file) = serve_swagger_ui(asset, swagger_config())
        .map_err(|error| OnlineError::Upstream(format!("vendored Swagger UI failed: {error}")))?
    else {
        return Ok(StatusCode::NOT_FOUND.into_response());
    };
    let content_type = HeaderValue::from_str(&file.content_type).map_err(|_| {
        OnlineError::Upstream("vendored Swagger UI returned an invalid content type".to_owned())
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

async fn metrics(State(state): State<AppState>) -> impl IntoResponse {
    let mut body = state.admission.render(
        state.role,
        state.query_result_cache.as_deref(),
        state.shuffle_result_cache.as_deref(),
        state.grace_join_engine.as_deref(),
    );
    body.push_str(
        "# HELP ngkg_streaming_request_spool_active_bytes Bytes reserved by live streamed Arrow requests.\n\
# TYPE ngkg_streaming_request_spool_active_bytes gauge\n",
    );
    if let Some(spool) = &state.shuffle_request_spool {
        push_metric(
            &mut body,
            "ngkg_streaming_request_spool_active_bytes",
            &format!("role=\"{:?}\"", state.role),
            spool.active_bytes.load(Ordering::Relaxed),
        );
    }
    body.push_str(
        "# HELP ngkg_fragment_response_spool_active_bytes Bytes reserved by live coordinator fragment responses.\n\
# TYPE ngkg_fragment_response_spool_active_bytes gauge\n",
    );
    if let Some(spool) = &state.fragment_response_spool {
        push_metric(
            &mut body,
            "ngkg_fragment_response_spool_active_bytes",
            &format!("role=\"{:?}\"", state.role),
            spool.active_bytes.load(Ordering::Relaxed),
        );
    }
    body.push_str(
        "# HELP ngkg_federation_pending_calls SPARQL SERVICE calls waiting for a bounded outbound lane.\n\
# TYPE ngkg_federation_pending_calls gauge\n\
# HELP ngkg_federation_active_calls SPARQL SERVICE calls holding an outbound lane.\n\
# TYPE ngkg_federation_active_calls gauge\n\
# HELP ngkg_federation_completed_calls_total Successfully parsed SPARQL SERVICE calls.\n\
# TYPE ngkg_federation_completed_calls_total counter\n\
# HELP ngkg_federation_failed_calls_total Failed SPARQL SERVICE calls, including SERVICE SILENT failures.\n\
# TYPE ngkg_federation_failed_calls_total counter\n\
# HELP ngkg_federation_response_bytes_total Accepted SPARQL SERVICE response bytes.\n\
# TYPE ngkg_federation_response_bytes_total counter\n",
    );
    if let Some(registry) = &state.federation {
        let snapshot = registry.metrics().snapshot();
        push_metric(
            &mut body,
            "ngkg_federation_pending_calls",
            "",
            snapshot.pending,
        );
        push_metric(
            &mut body,
            "ngkg_federation_active_calls",
            "",
            snapshot.active,
        );
        push_metric(
            &mut body,
            "ngkg_federation_completed_calls_total",
            "",
            snapshot.completed,
        );
        push_metric(
            &mut body,
            "ngkg_federation_failed_calls_total",
            "",
            snapshot.failed,
        );
        push_metric(
            &mut body,
            "ngkg_federation_response_bytes_total",
            "",
            snapshot.response_bytes,
        );
    }
    (
        [(CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8")],
        body,
    )
}

async fn sparql_get(
    State(state): State<AppState>,
    AxumPath(dataset_id): AxumPath<Uuid>,
    RawQuery(raw_query): RawQuery,
    headers: HeaderMap,
) -> Result<Response, OnlineError> {
    let parameters = parse_protocol_parameters(raw_query.as_deref().unwrap_or(""), true)?;
    let request = finish_protocol_request(parameters, None)?;
    execute_sparql_protocol(state, dataset_id, headers, request).await
}

async fn sparql_post(
    State(state): State<AppState>,
    AxumPath(dataset_id): AxumPath<Uuid>,
    RawQuery(raw_query): RawQuery,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, OnlineError> {
    let media_type = sparql_request_media_type(&headers)?;
    let mut uri_parameters = parse_protocol_parameters(raw_query.as_deref().unwrap_or(""), false)?;
    let request = match media_type.as_str() {
        "application/sparql-query" => {
            let query = std::str::from_utf8(&body).map_err(|_| {
                OnlineError::MalformedProtocol(
                    "application/sparql-query body must be valid UTF-8".to_owned(),
                )
            })?;
            finish_protocol_request(uri_parameters, Some(query.to_owned()))?
        }
        "application/x-www-form-urlencoded" => {
            let form = std::str::from_utf8(&body).map_err(|_| {
                OnlineError::MalformedProtocol(
                    "form body must be valid UTF-8 before percent decoding".to_owned(),
                )
            })?;
            let form_parameters = parse_protocol_parameters(form, true)?;
            if uri_parameters.query.is_some() {
                return Err(OnlineError::MalformedProtocol(
                    "query must occur only once in a protocol request".to_owned(),
                ));
            }
            uri_parameters.query = form_parameters.query;
            uri_parameters
                .default_graph_uris
                .extend(form_parameters.default_graph_uris);
            uri_parameters
                .named_graph_uris
                .extend(form_parameters.named_graph_uris);
            finish_protocol_request(uri_parameters, None)?
        }
        _ => return Err(OnlineError::UnsupportedMediaType(media_type)),
    };
    execute_sparql_protocol(state, dataset_id, headers, request).await
}

fn sparql_request_media_type(headers: &HeaderMap) -> Result<String, OnlineError> {
    let content_type = headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| OnlineError::UnsupportedMediaType("Content-Type is required".to_owned()))?;
    let mut components = content_type.split(';');
    let media_type = components
        .next()
        .map(str::trim)
        .map(str::to_ascii_lowercase)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| OnlineError::UnsupportedMediaType(content_type.to_owned()))?;
    for parameter in components {
        let parameter = parameter.trim();
        if let Some((name, value)) = parameter.split_once('=')
            && name.trim().eq_ignore_ascii_case("charset")
            && !value.trim().trim_matches('"').eq_ignore_ascii_case("utf-8")
        {
            return Err(OnlineError::UnsupportedMediaType(
                "SPARQL Protocol request charset must be UTF-8".to_owned(),
            ));
        }
    }
    Ok(media_type)
}

async fn sparql_service_description(
    State(state): State<AppState>,
    AxumPath(dataset_id): AxumPath<Uuid>,
    headers: HeaderMap,
) -> Result<Response, OnlineError> {
    let identity = state.authorizer.authorize(&headers)?;
    let _semantic = state
        .manager
        .clone()
        .semantic_state(identity.tenant_id, dataset_id)
        .await?;
    let turtle = render_service_description(state.standards_features, state.federation.is_some());
    Ok(([(CONTENT_TYPE, "text/turtle; charset=utf-8")], turtle).into_response())
}

async fn execute_sparql_protocol(
    state: AppState,
    dataset_id: Uuid,
    headers: HeaderMap,
    request: SparqlProtocolRequest,
) -> Result<Response, OnlineError> {
    require_protocol_query_size(&request.query, state.max_query_bytes)?;
    // Negotiate before admission, durable logging, cache lookup, federation, or
    // distributed execution. A representation the client refuses must consume
    // no semantic-query capacity and must never appear as a completed query.
    let protocol_query = compile_certified_query(&request.query)?;
    let protocol_form = protocol_query.form();
    let protocol_ordered = protocol_query.solution_order_is_significant();
    match protocol_form {
        QueryForm::Select | QueryForm::Ask => {
            select_sparql_solution_format(&headers)?;
        }
        QueryForm::Construct | QueryForm::Describe => {
            select_sparql_graph_format(&headers)?;
        }
    }
    let maximum = state.max_query_response_bytes;
    let response_headers = headers.clone();
    let custom = query(
        State(state.clone()),
        AxumPath(dataset_id),
        headers,
        Json(QueryRequest {
            query: request.query,
            snapshot_id: None,
            hydrate: false,
            default_graph_uris: request.default_graph_uris,
            named_graph_uris: request.named_graph_uris,
        }),
    )
    .await?;
    let (parts, body) = custom.into_parts();
    if parts.status != StatusCode::OK {
        return Err(OnlineError::SnapshotConflict(
            "certified query returned a non-success response to the protocol adapter".to_owned(),
        ));
    }
    let cache_header = parts
        .headers
        .get("x-ngkg-query-cache")
        .cloned()
        .ok_or_else(|| {
            OnlineError::SnapshotConflict(
                "certified query response lacks its cache disposition".to_owned(),
            )
        })?;
    let query_execution_header = parts
        .headers
        .get("x-ngkg-query-execution-id")
        .cloned()
        .ok_or_else(|| {
            OnlineError::SnapshotConflict(
                "query response lacks its durable execution identity".to_owned(),
            )
        })?;
    let bytes = to_bytes(body, maximum).await.map_err(|error| {
        OnlineError::SnapshotConflict(format!(
            "certified query response could not be read: {error}"
        ))
    })?;
    let certified = serde_json::from_slice::<QueryResponse>(&bytes)?;
    if !certified.complete {
        return Err(OnlineError::SnapshotConflict(
            "certified query response is not complete".to_owned(),
        ));
    }
    let (content_type, output) = match certified.query_form {
        QueryForm::Select => {
            let format = select_sparql_solution_format(&response_headers)?;
            (
                format.content_type(),
                serialize_sparql_solutions(&certified.head, &certified.bindings, format, maximum)?,
            )
        }
        QueryForm::Ask => {
            let format = select_sparql_solution_format(&response_headers)?;
            let value = certified.boolean_result.ok_or_else(|| {
                OnlineError::SnapshotConflict(
                    "certified ASK response has no boolean result".to_owned(),
                )
            })?;
            (
                format.content_type(),
                serialize_sparql_boolean(value, format, maximum)?,
            )
        }
        QueryForm::Construct | QueryForm::Describe => {
            let format = select_sparql_graph_format(&response_headers)?;
            (
                format.content_type(),
                serialize_sparql_graph(&certified.graph_ntriples, format, maximum)?,
            )
        }
    };
    let semantic_result_sha256 = canonical_query_payload_sha256(
        certified.query_form,
        &certified.head,
        &certified.bindings,
        certified.boolean_result,
        &certified.graph_ntriples,
        protocol_ordered,
        CertifiedQueryExecutionLimits {
            max_solution_rows: state.max_query_result_rows,
            max_graph_triples: state.max_query_graph_triples,
            max_graph_blank_nodes: state.max_query_graph_blank_nodes,
        },
    )
    .map_err(ReferenceRuntimeError::Query)?;
    let mut response = ([(CONTENT_TYPE, content_type)], output).into_response();
    response
        .headers_mut()
        .insert("x-ngkg-query-cache", cache_header);
    response
        .headers_mut()
        .insert("x-ngkg-query-execution-id", query_execution_header);
    response.headers_mut().insert(
        "x-ngkg-snapshot-id",
        header_value(&certified.snapshot_id.to_string())?,
    );
    response.headers_mut().insert(
        "x-ngkg-query-sha256",
        header_value(&certified.query_sha256)?,
    );
    response.headers_mut().insert(
        "x-ngkg-semantic-result-sha256",
        header_value(&semantic_result_sha256)?,
    );
    response.headers_mut().insert(
        "x-ngkg-native-cutover-mode",
        HeaderValue::from_static(state.native_cutover_mode.as_str()),
    );
    response
        .headers_mut()
        .insert("x-ngkg-complete", HeaderValue::from_static("true"));
    if let Some(entailment) = &certified.entailment {
        response.headers_mut().insert(
            "x-ngkg-entailment-regime",
            HeaderValue::from_static("owl2-direct"),
        );
        response.headers_mut().insert(
            "x-ngkg-entailment-evidence-sha256",
            header_value(&sha256_json(entailment)?)?,
        );
    }
    if let Some(federation) = &certified.federation {
        response.headers_mut().insert(
            "x-ngkg-federation-registry-sha256",
            header_value(&federation.registry_sha256)?,
        );
        response.headers_mut().insert(
            "x-ngkg-federation-call-count",
            header_value(&federation.service_call_count.to_string())?,
        );
        response.headers_mut().insert(
            "x-ngkg-federation-endpoint-set-sha256",
            header_value(&federation.endpoint_set_sha256)?,
        );
    }
    response
        .headers_mut()
        .insert(VARY, HeaderValue::from_static("Accept"));
    Ok(response)
}

fn finish_protocol_request(
    parameters: ParsedProtocolParameters,
    body_query: Option<String>,
) -> Result<SparqlProtocolRequest, OnlineError> {
    if parameters.query.is_some() && body_query.is_some() {
        return Err(OnlineError::MalformedProtocol(
            "query must occur only once in a protocol request".to_owned(),
        ));
    }
    let query = parameters.query.or(body_query).ok_or_else(|| {
        OnlineError::MalformedProtocol("exactly one query parameter or body is required".to_owned())
    })?;
    if query.is_empty() {
        return Err(OnlineError::MalformedProtocol(
            "SPARQL query must not be empty".to_owned(),
        ));
    }
    Ok(SparqlProtocolRequest {
        query,
        default_graph_uris: parameters.default_graph_uris,
        named_graph_uris: parameters.named_graph_uris,
    })
}

fn compile_certified_query(query: &str) -> Result<CompiledSparqlQuery, OnlineError> {
    let compiled = match CompiledSparqlQuery::parse(query) {
        Ok(compiled) => compiled,
        Err(SparqlCompileError::Syntax(message)) => {
            return Err(OnlineError::MalformedSparql(message));
        }
    };
    // Phase 40.13.1 contract: `compiled.execution_analysis().has_remote_service` is execution
    // policy, never a syntax error. Its earlier diagnostic began "SPARQL SERVICE parsed successfully";
    // Phase 40.13.18 now applies the secured policy in `query`.
    Ok(compiled)
}

fn require_protocol_query_size(query: &str, maximum: usize) -> Result<(), OnlineError> {
    if query.len() > maximum {
        Err(OnlineError::QueryTooLarge)
    } else {
        Ok(())
    }
}

fn select_sparql_solution_format(headers: &HeaderMap) -> Result<SparqlSolutionFormat, OnlineError> {
    if headers.get_all(ACCEPT).iter().next().is_none() {
        return Ok(SparqlSolutionFormat::Json);
    }
    let mut selected: Option<(SparqlSolutionFormat, f32, u8, usize)> = None;
    for (server_rank, format) in SPARQL_SOLUTION_FORMATS.iter().copied().enumerate() {
        let Some((quality, specificity)) = media_quality(headers, format.media_type())? else {
            continue;
        };
        if quality <= 0.0 {
            continue;
        }
        match selected {
            Some((_, best_quality, best_specificity, best_rank))
                if best_quality > quality
                    || (best_quality == quality && best_specificity > specificity)
                    || (best_quality == quality
                        && best_specificity == specificity
                        && best_rank < server_rank) => {}
            _ => selected = Some((format, quality, specificity, server_rank)),
        }
    }
    selected
        .map(|(format, _, _, _)| format)
        .ok_or(OnlineError::NotAcceptable)
}

fn select_sparql_graph_format(headers: &HeaderMap) -> Result<SparqlGraphFormat, OnlineError> {
    if headers.get_all(ACCEPT).iter().next().is_none() {
        return Ok(SparqlGraphFormat::Turtle);
    }
    let mut selected: Option<(SparqlGraphFormat, f32, u8, usize)> = None;
    for (server_rank, format) in SPARQL_GRAPH_FORMATS.iter().copied().enumerate() {
        let Some((quality, specificity)) = media_quality(headers, format.media_type())? else {
            continue;
        };
        if quality <= 0.0 {
            continue;
        }
        match selected {
            Some((_, best_quality, best_specificity, best_rank))
                if best_quality > quality
                    || (best_quality == quality && best_specificity > specificity)
                    || (best_quality == quality
                        && best_specificity == specificity
                        && best_rank < server_rank) => {}
            _ => selected = Some((format, quality, specificity, server_rank)),
        }
    }
    selected
        .map(|(format, _, _, _)| format)
        .ok_or(OnlineError::NotAcceptable)
}

fn media_quality(headers: &HeaderMap, media_type: &str) -> Result<Option<(f32, u8)>, OnlineError> {
    let (target_type, target_subtype) = media_type
        .split_once('/')
        .ok_or(OnlineError::NotAcceptable)?;
    let mut selected: Option<(f32, u8)> = None;
    for value in headers.get_all(ACCEPT) {
        let value = value.to_str().map_err(|_| OnlineError::NotAcceptable)?;
        for range in value.split(',') {
            let mut components = range.split(';');
            let media_range = components
                .next()
                .map(str::trim)
                .unwrap_or_default()
                .to_ascii_lowercase();
            let Some((range_type, range_subtype)) = media_range.split_once('/') else {
                return Err(OnlineError::NotAcceptable);
            };
            let specificity = if range_type == target_type && range_subtype == target_subtype {
                2
            } else if range_type == target_type && range_subtype == "*" {
                1
            } else if range_type == "*" && range_subtype == "*" {
                0
            } else {
                continue;
            };
            let mut quality = 1.0_f32;
            let mut representation_matches = true;
            for parameter in components {
                let Some((name, value)) = parameter.trim().split_once('=') else {
                    return Err(OnlineError::NotAcceptable);
                };
                if name.trim().eq_ignore_ascii_case("q") {
                    quality = value
                        .trim()
                        .parse::<f32>()
                        .map_err(|_| OnlineError::NotAcceptable)?;
                    if !(0.0..=1.0).contains(&quality) {
                        return Err(OnlineError::NotAcceptable);
                    }
                } else if name.trim().eq_ignore_ascii_case("charset") {
                    representation_matches =
                        value.trim().trim_matches('"').eq_ignore_ascii_case("utf-8");
                } else {
                    representation_matches = false;
                }
            }
            if !representation_matches {
                continue;
            }
            match selected {
                Some((_, best_specificity)) if best_specificity > specificity => {}
                Some((best_quality, best_specificity)) if best_specificity == specificity => {
                    selected = Some((best_quality.max(quality), specificity));
                }
                _ => selected = Some((quality, specificity)),
            }
        }
    }
    Ok(selected)
}

fn serialize_sparql_solutions(
    head: &[String],
    bindings: &[serde_json::Value],
    format: SparqlSolutionFormat,
    maximum: usize,
) -> Result<Bytes, OnlineError> {
    let variables = head
        .iter()
        .map(|name| {
            Variable::new(name.clone()).map_err(|error| {
                OnlineError::SnapshotConflict(format!(
                    "certified SPARQL head contains an invalid variable: {error}"
                ))
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let allowed_variables = head.iter().map(String::as_str).collect::<BTreeSet<_>>();
    let mut output = BoundedBuffer::new(maximum);
    {
        let serializer = QueryResultsSerializer::from_format(format.result_format());
        let mut writer = serializer
            .serialize_solutions_to_writer(&mut output, variables)
            .map_err(sparql_serialization_error)?;
        for binding in bindings {
            let object = binding.as_object().ok_or_else(|| {
                OnlineError::SnapshotConflict(
                    "certified SPARQL solution is not a binding object".to_owned(),
                )
            })?;
            let mut row = Vec::with_capacity(object.len());
            for (name, value) in object {
                if !allowed_variables.contains(name.as_str()) {
                    return Err(OnlineError::SnapshotConflict(format!(
                        "certified SPARQL solution binds undeclared variable {name}"
                    )));
                }
                let variable = Variable::new(name.clone()).map_err(|error| {
                    OnlineError::SnapshotConflict(format!(
                        "certified SPARQL binding variable is invalid: {error}"
                    ))
                })?;
                row.push((variable, sparql_json_term(value)?));
            }
            writer
                .serialize(
                    row.iter()
                        .map(|(variable, term)| (variable.as_ref(), term.as_ref())),
                )
                .map_err(sparql_serialization_error)?;
        }
        writer.finish().map_err(sparql_serialization_error)?;
    }
    Ok(Bytes::from(output.into_bytes()))
}

fn serialize_sparql_boolean(
    value: bool,
    format: SparqlSolutionFormat,
    maximum: usize,
) -> Result<Bytes, OnlineError> {
    let mut output = BoundedBuffer::new(maximum);
    QueryResultsSerializer::from_format(format.result_format())
        .serialize_boolean_to_writer(&mut output, value)
        .map_err(sparql_serialization_error)?;
    Ok(Bytes::from(output.into_bytes()))
}

fn serialize_sparql_graph(
    canonical_ntriples: &[String],
    format: SparqlGraphFormat,
    maximum: usize,
) -> Result<Bytes, OnlineError> {
    let source = canonical_ntriples.concat();
    let mut output = BoundedBuffer::new(maximum);
    {
        let mut serializer =
            RdfSerializer::from_format(format.rdf_format()).for_writer(&mut output);
        for quad in
            RdfParser::from_format(RdfFormat::NTriples).for_reader(Cursor::new(source.as_bytes()))
        {
            let quad = quad.map_err(|error| {
                OnlineError::SnapshotConflict(format!(
                    "certified graph result is not valid canonical N-Triples: {error}"
                ))
            })?;
            serializer
                .serialize_quad(&quad)
                .map_err(sparql_serialization_error)?;
        }
        serializer.finish().map_err(sparql_serialization_error)?;
    }
    Ok(Bytes::from(output.into_bytes()))
}

fn sparql_serialization_error(error: std::io::Error) -> OnlineError {
    OnlineError::Request(format!(
        "SPARQL result serialization failed within its byte ceiling: {error}"
    ))
}

fn sparql_json_term(value: &serde_json::Value) -> Result<Term, OnlineError> {
    let object = value.as_object().ok_or_else(|| {
        OnlineError::SnapshotConflict("SPARQL result term is not an object".to_owned())
    })?;
    let kind = object
        .get("type")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| {
            OnlineError::SnapshotConflict("SPARQL result term has no type".to_owned())
        })?;
    let lexical = object
        .get("value")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| {
            OnlineError::SnapshotConflict("SPARQL result term has no value".to_owned())
        })?;
    match kind {
        "uri" => NamedNode::new(lexical.to_owned())
            .map(Term::NamedNode)
            .map_err(|error| {
                OnlineError::SnapshotConflict(format!("invalid SPARQL result IRI: {error}"))
            }),
        "bnode" => BlankNode::new(lexical.to_owned())
            .map(Term::BlankNode)
            .map_err(|error| {
                OnlineError::SnapshotConflict(format!("invalid SPARQL result blank node: {error}"))
            }),
        "literal" | "typed-literal" => {
            if let Some(language) = object.get("xml:lang").and_then(serde_json::Value::as_str) {
                Literal::new_language_tagged_literal(lexical.to_owned(), language.to_owned())
                    .map(Term::Literal)
                    .map_err(|error| {
                        OnlineError::SnapshotConflict(format!(
                            "invalid SPARQL result language tag: {error}"
                        ))
                    })
            } else if let Some(datatype) =
                object.get("datatype").and_then(serde_json::Value::as_str)
            {
                let datatype = NamedNode::new(datatype.to_owned()).map_err(|error| {
                    OnlineError::SnapshotConflict(format!(
                        "invalid SPARQL result datatype IRI: {error}"
                    ))
                })?;
                Ok(Term::Literal(Literal::new_typed_literal(
                    lexical.to_owned(),
                    datatype,
                )))
            } else {
                Ok(Term::Literal(Literal::new_simple_literal(
                    lexical.to_owned(),
                )))
            }
        }
        other => Err(OnlineError::SnapshotConflict(format!(
            "unsupported certified SPARQL result term type {other}"
        ))),
    }
}

fn parse_protocol_parameters(
    encoded: &str,
    allow_query: bool,
) -> Result<ParsedProtocolParameters, OnlineError> {
    let mut parsed = ParsedProtocolParameters::default();
    for field in encoded.split('&').filter(|field| !field.is_empty()) {
        let (raw_name, raw_value) = field.split_once('=').unwrap_or((field, ""));
        let name = decode_form_component(raw_name)?;
        let value = decode_form_component(raw_value)?;
        match name.as_str() {
            "query" if allow_query => {
                if parsed.query.replace(value).is_some() {
                    return Err(OnlineError::MalformedProtocol(
                        "query parameter must occur exactly once".to_owned(),
                    ));
                }
            }
            "query" => {
                return Err(OnlineError::MalformedProtocol(
                    "query URL parameter cannot accompany an application/sparql-query body"
                        .to_owned(),
                ));
            }
            "default-graph-uri" => parsed.default_graph_uris.push(value),
            "named-graph-uri" => parsed.named_graph_uris.push(value),
            _ => {
                return Err(OnlineError::MalformedProtocol(format!(
                    "unsupported SPARQL Protocol parameter: {name}"
                )));
            }
        }
    }
    Ok(parsed)
}

fn decode_form_component(value: &str) -> Result<String, OnlineError> {
    let bytes = value.as_bytes();
    let mut output = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'+' => {
                output.push(b' ');
                index += 1;
            }
            b'%' => {
                let high = bytes.get(index + 1).and_then(|value| hex_digit(*value));
                let low = bytes.get(index + 2).and_then(|value| hex_digit(*value));
                let (Some(high), Some(low)) = (high, low) else {
                    return Err(OnlineError::MalformedProtocol(
                        "form parameter contains invalid percent encoding".to_owned(),
                    ));
                };
                output.push((high << 4) | low);
                index += 3;
            }
            byte => {
                output.push(byte);
                index += 1;
            }
        }
    }
    String::from_utf8(output).map_err(|_| {
        OnlineError::MalformedProtocol(
            "form parameter is not valid percent-encoded UTF-8".to_owned(),
        )
    })
}

const fn hex_digit(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}

fn render_service_description(features: StandardsFeatureGates, federation_enabled: bool) -> String {
    let mut predicates = vec![
        "sd:endpoint <../sparql>".to_owned(),
        "ngkg:certifiedQueryOnly true".to_owned(),
    ];
    predicates.extend(
        SPARQL_SOLUTION_FORMATS
            .iter()
            .map(|format| format!("sd:resultFormat {}", format.service_description_token())),
    );
    predicates.extend(
        SPARQL_GRAPH_FORMATS
            .iter()
            .map(|format| format!("sd:resultFormat {}", format.service_description_token())),
    );
    if features.sparql_11_query {
        predicates.push("sd:supportedLanguage sd:SPARQL11Query".to_owned());
    }
    if features.union_default_graph {
        predicates.push("sd:feature sd:UnionDefaultGraph".to_owned());
    }
    if features.owl_direct {
        predicates.push(
            "sd:defaultEntailmentRegime <http://www.w3.org/ns/entailment/OWL-Direct>".to_owned(),
        );
    }
    if features.owl_dl {
        predicates.push(
            "sd:defaultSupportedEntailmentProfile <http://www.w3.org/ns/owl-profile/DL>".to_owned(),
        );
    }
    if federation_enabled {
        predicates.push("sd:feature sd:BasicFederatedQuery".to_owned());
        predicates.push("ngkg:securedEndpointRegistry true".to_owned());
    }
    format!(
        "@prefix sd: <http://www.w3.org/ns/sparql-service-description#> .\n\
@prefix formats: <http://www.w3.org/ns/formats/> .\n\
@prefix ngkg: <https://ngkg.io/vocabulary/service#> .\n\n\
[] a sd:Service ;\n   {} .\n",
        predicates.join(" ;\n   ")
    )
}

fn validate_standards_feature_implications(
    features: StandardsFeatureGates,
) -> Result<(), &'static str> {
    if features.owl_dl && !features.owl_direct {
        return Err("OWL DL advertisement requires OWL Direct advertisement");
    }
    if features.owl_direct && !features.sparql_11_query {
        return Err("OWL Direct advertisement requires SPARQL 1.1 Query advertisement");
    }
    if features.union_default_graph && !features.sparql_11_query {
        return Err("union-default advertisement requires SPARQL 1.1 Query advertisement");
    }
    Ok(())
}

fn header_value(value: &str) -> Result<HeaderValue, OnlineError> {
    HeaderValue::from_str(value).map_err(|_| {
        OnlineError::SnapshotConflict("response metadata is not a valid HTTP header".to_owned())
    })
}

async fn admission_middleware(
    State(state): State<AppState>,
    request: Request,
    next: Next,
) -> Response {
    let Some(class) = admission_class(state.role, request.uri().path()) else {
        return next.run(request).await;
    };
    let identity = match if request.uri().path().starts_with("/v1/query_logs") {
        state.authorizer.authorize_query_logs(request.headers())
    } else {
        state.authorizer.authorize(request.headers())
    } {
        Ok(identity) => identity,
        Err(error) => return OnlineError::from(error).into_response(),
    };
    let started_wait = Instant::now();
    let mut lease = match state.admission.acquire(class, identity.tenant_id).await {
        Ok(lease) => lease,
        Err(AdmissionFailure::TimedOut(scope)) => {
            state.admission.reject(class, scope, started_wait.elapsed());
            let (code, message) = if scope == AdmissionScope::Tenant {
                (
                    "TENANT_ADMISSION_CAPACITY_EXHAUSTED",
                    "this tenant's bounded execution lanes are busy; retry after completion",
                )
            } else {
                (
                    "ADMISSION_CAPACITY_EXHAUSTED",
                    "the bounded execution lanes are busy; retry after scaling or completion",
                )
            };
            return admission_rejection(StatusCode::TOO_MANY_REQUESTS, code, message, true);
        }
        Err(AdmissionFailure::Closed(scope)) => {
            state.admission.reject(class, scope, started_wait.elapsed());
            return admission_rejection(
                StatusCode::SERVICE_UNAVAILABLE,
                "ADMISSION_CONTROLLER_CLOSED",
                "the execution admission controller is unavailable",
                false,
            );
        }
        Err(AdmissionFailure::UnknownTenant) => {
            return admission_rejection(
                StatusCode::SERVICE_UNAVAILABLE,
                "TENANT_ADMISSION_POLICY_MISSING",
                "the authenticated tenant is absent from the active admission policy",
                false,
            );
        }
    };
    let response = next.run(request).await;
    lease.failed = !response.status().is_success();
    hold_admission_through_body(response, lease)
}

fn admission_class(role: Role, path: &str) -> Option<AdmissionClass> {
    if !path.starts_with("/v1/") {
        return None;
    }
    Some(match role {
        Role::Query => AdmissionClass::Query,
        Role::Fragment
            if path.contains("/shuffles/")
                || path.contains("/algebra/")
                || path.contains("/paths/") =>
        {
            AdmissionClass::Shuffle
        }
        Role::Fragment => AdmissionClass::Fragment,
        Role::Locator => AdmissionClass::Locator,
        Role::Hydration => AdmissionClass::Hydration,
    })
}

fn admission_rejection(
    status: StatusCode,
    code: &'static str,
    message: &'static str,
    retry: bool,
) -> Response {
    let mut response = (
        status,
        Json(ErrorBody {
            code,
            message: message.to_owned(),
        }),
    )
        .into_response();
    if retry {
        response
            .headers_mut()
            .insert("retry-after", HeaderValue::from_static("1"));
    }
    response
}

fn hold_admission_through_body(response: Response, lease: AdmissionLease) -> Response {
    let (parts, body) = response.into_parts();
    let stream = stream::unfold(
        (body.into_data_stream(), Some(lease)),
        |(mut body, mut lease)| async move {
            let item = body.next().await?;
            if item.is_err()
                && let Some(active) = lease.as_mut()
            {
                active.failed = true;
            }
            Some((item, (body, lease)))
        },
    );
    Response::from_parts(parts, Body::from_stream(stream))
}

async fn validate_direct_bgps(
    State(state): State<AppState>,
    AxumPath(dataset_id): AxumPath<Uuid>,
    headers: HeaderMap,
    Json(request): Json<DirectBgpValidationRequest>,
) -> Result<Json<DirectBgpLegalityReport>, OnlineError> {
    let identity = state.authorizer.authorize(&headers)?;
    let DirectBgpValidationRequest {
        query: query_text,
        snapshot_id: requested_snapshot_id,
        default_graph_uris,
        named_graph_uris,
    } = request;
    if query_text.is_empty() || query_text.len() > state.max_query_bytes {
        return Err(OnlineError::QueryTooLarge);
    }
    let parse_text = query_text.clone();
    let parse_task = tokio::task::spawn_blocking(move || compile_certified_query(&parse_text));
    let compiled_query = match tokio::time::timeout(state.query_timeout, parse_task).await {
        Ok(joined) => joined??,
        Err(_) => {
            return Err(OnlineError::GatewayTimeout(
                "SPARQL parsing exceeded the configured query timeout".to_owned(),
            ));
        }
    };

    // Authorization precedes access to the combined semantic signature. Phase 40.7 uses that
    // signature for declaration disambiguation, so it must never leak entities from a reasoning
    // graph the principal is forbidden to observe.
    let authorization = state
        .manager
        .clone()
        .authorization_state(identity.tenant_id, dataset_id)
        .await?;
    require_reasoning_graph_authorization(&identity, &authorization.graph_catalog)?;
    let semantic = state
        .manager
        .clone()
        .semantic_state(identity.tenant_id, dataset_id)
        .await?;
    require_requested_snapshot(requested_snapshot_id, &semantic.active)?;

    let query_dataset = compiled_query.dataset_specification().clone();
    let protocol_dataset = ProtocolDatasetSpecification {
        default_graph_uris,
        named_graph_uris,
    };
    let active_dataset = resolve_request_dataset(
        &semantic.graph_catalog,
        &identity.graph_authorization_labels,
        &query_dataset,
        &protocol_dataset,
    )?;
    if active_dataset.authorized_graph_set_sha256.is_empty()
        || active_dataset.active_dataset_sha256.is_empty()
    {
        return Err(OnlineError::SnapshotConflict(
            "Direct-BGP validation resolved an unbound active dataset".to_owned(),
        ));
    }

    let signature = Arc::clone(&semantic.owl_signature);
    let classify_query = compiled_query.clone();
    let classify_task = tokio::task::spawn_blocking(move || {
        classify_direct_bgps(
            &classify_query,
            signature.as_ref(),
            state.direct_bgp_classification_limits,
        )
    });
    let classification = match tokio::time::timeout(state.query_timeout, classify_task).await {
        Ok(joined) => joined?.map_err(|error| OnlineError::Request(error.to_string()))?,
        Err(_) => {
            return Err(OnlineError::GatewayTimeout(
                "OWL Direct-BGP classification exceeded the configured query timeout".to_owned(),
            ));
        }
    };

    let bgp_count = u64::try_from(classification.records.len()).map_err(|_| {
        OnlineError::Request("Direct-BGP count exceeds the platform integer range".to_owned())
    })?;
    let all_bgps_legal = classification
        .records
        .iter()
        .all(|record| record.status == ngkg_types::DirectBgpLegalityStatus::Legal);
    let report = DirectBgpLegalityReport {
        format_version: 1,
        dataset_id,
        snapshot_id: semantic.active.snapshot.snapshot_id,
        query_sha256: hex::encode(Sha256::digest(query_text.as_bytes())),
        sparql_algebra_sha256: compiled_query.canonical_sse_sha256().to_owned(),
        active_dataset_sha256: active_dataset.active_dataset_sha256,
        authorized_graph_set_sha256: active_dataset.authorized_graph_set_sha256,
        owl_signature_sha256: semantic.owl_signature_sha256.clone(),
        datatype_policy_sha256: semantic.datatype_policy_sha256.clone(),
        owl_profile_qualification_sha256: semantic.owl_profile_qualification_sha256.clone(),
        owl_consistency_qualification_sha256: semantic.owl_consistency_qualification_sha256.clone(),
        entailment_regime: EntailmentRegime::Owl2Direct,
        classifier: DIRECT_BGP_CLASSIFIER_V1.to_owned(),
        bgp_count,
        all_bgps_legal,
        property_paths_outside_direct_bgps: classification.property_paths_outside_direct_bgps,
        bgps: classification.records,
    };
    validate_direct_bgp_legality_report(&report).map_err(|error| {
        OnlineError::SnapshotConflict(format!(
            "Direct-BGP legality report failed its runtime contract: {error}"
        ))
    })?;
    tracing::info!(
        tenant_id = %identity.tenant_id,
        principal_id = %identity.principal_id,
        %dataset_id,
        snapshot_id = %report.snapshot_id,
        bgp_count = report.bgp_count,
        all_bgps_legal = report.all_bgps_legal,
        phase40_admission_ceiling_sha256 = %state.phase40_admission_ceiling_sha256,
        "OWL Direct-BGP validation completed"
    );
    Ok(Json(report))
}

async fn route_direct_bgps(
    State(state): State<AppState>,
    AxumPath(dataset_id): AxumPath<Uuid>,
    headers: HeaderMap,
    Json(request): Json<DirectBgpValidationRequest>,
) -> Result<Json<DirectEntailmentRoutingResponse>, OnlineError> {
    let Json(legality) =
        validate_direct_bgps(State(state), AxumPath(dataset_id), headers, Json(request)).await?;
    let routes = legality
        .bgps
        .iter()
        .map(|bgp| DirectBgpRoutingRecord {
            ordinal: bgp.ordinal,
            bgp_sha256: bgp.bgp_sha256.clone(),
            // Phase 40.13.7 qualifies index/closure coverage case-by-case. Until that evidence is
            // present, legal BGPs must fall through to HermiT; unknown never becomes false.
            route: route_entailment(EntailmentRoutingInput {
                legality: bgp.status,
                semantic_index: CoverageState::Unknown,
                finite_closure: CoverageState::Unknown,
            }),
        })
        .collect();
    Ok(Json(DirectEntailmentRoutingResponse { legality, routes }))
}

fn epoch_milliseconds() -> Result<i64, OnlineError> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| OnlineError::Request("system clock is before the Unix epoch".to_owned()))?
        .as_millis();
    i64::try_from(millis)
        .map_err(|_| OnlineError::Request("epoch timestamp exceeds the audit ledger".to_owned()))
}

fn request_id(headers: &HeaderMap) -> String {
    headers
        .get("x-request-id")
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty() && value.len() <= 128)
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| Uuid::new_v4().to_string())
}

const fn query_form_name(form: QueryForm) -> &'static str {
    form.as_str()
}

fn query_resource_envelope(
    config: &QueryLogConfig,
    response: &QueryResponse,
    cache_hit: bool,
) -> Result<(i32, i64, i64), OnlineError> {
    let fragment_workers = if cache_hit {
        0_u64
    } else {
        u64::from(
            response
                .execution
                .worker_count
                .max(response.execution.shuffle_worker_count)
                .max(
                    response
                        .property_path_execution
                        .as_ref()
                        .map_or(0, |evidence| {
                            u32::try_from(evidence.worker_ids.len()).unwrap_or(u32::MAX)
                        }),
                ),
        )
    };
    let hydration_workers = u64::from(!response.hydrated_payload.is_empty());
    let nodes = 1_u64
        .checked_add(fragment_workers)
        .and_then(|value| value.checked_add(hydration_workers))
        .ok_or_else(|| OnlineError::Request("query node accounting overflow".to_owned()))?;
    let cpu_millis = config
        .coordinator_cpu_millis
        .checked_add(
            config
                .fragment_cpu_millis
                .checked_mul(fragment_workers)
                .ok_or_else(|| OnlineError::Request("query CPU accounting overflow".to_owned()))?,
        )
        .and_then(|value| {
            value.checked_add(config.hydration_cpu_millis.checked_mul(hydration_workers)?)
        })
        .ok_or_else(|| OnlineError::Request("query CPU accounting overflow".to_owned()))?;
    let memory_bytes = config
        .coordinator_memory_bytes
        .checked_add(
            config
                .fragment_memory_bytes
                .checked_mul(fragment_workers)
                .ok_or_else(|| OnlineError::Request("query RAM accounting overflow".to_owned()))?,
        )
        .and_then(|value| {
            value.checked_add(
                config
                    .hydration_memory_bytes
                    .checked_mul(hydration_workers)?,
            )
        })
        .ok_or_else(|| OnlineError::Request("query RAM accounting overflow".to_owned()))?;
    Ok((
        i32::try_from(nodes)
            .map_err(|_| OnlineError::Request("query node accounting overflow".to_owned()))?,
        i64::try_from(cpu_millis)
            .map_err(|_| OnlineError::Request("query CPU accounting overflow".to_owned()))?,
        i64::try_from(memory_bytes)
            .map_err(|_| OnlineError::Request("query RAM accounting overflow".to_owned()))?,
    ))
}

#[derive(Clone, Copy, Debug, Default)]
struct CgroupResourceSample {
    cpu_usage_micros: Option<u64>,
    memory_peak_bytes: Option<u64>,
}

async fn cgroup_resource_sample() -> CgroupResourceSample {
    tokio::task::spawn_blocking(|| {
        let cpu_usage_micros = fs::read_to_string("/sys/fs/cgroup/cpu.stat")
            .ok()
            .and_then(|value| {
                value.lines().find_map(|line| {
                    let (name, value) = line.split_once(' ')?;
                    (name == "usage_usec").then(|| value.parse::<u64>().ok()).flatten()
                })
            });
        let memory_peak_bytes = fs::read_to_string("/sys/fs/cgroup/memory.peak")
            .ok()
            .and_then(|value| value.trim().parse::<u64>().ok());
        CgroupResourceSample {
            cpu_usage_micros,
            memory_peak_bytes,
        }
    })
    .await
    .unwrap_or_default()
}

fn measured_resource_fields(
    start: CgroupResourceSample,
    end: CgroupResourceSample,
) -> (Option<i64>, Option<i64>, String) {
    let cpu_ms = start
        .cpu_usage_micros
        .zip(end.cpu_usage_micros)
        .and_then(|(start, end)| i64::try_from(end.saturating_sub(start) / 1_000).ok());
    let peak_rss = end
        .memory_peak_bytes
        .and_then(|value| i64::try_from(value).ok());
    let scope = if cpu_ms.is_some() || peak_rss.is_some() {
        "COORDINATOR_CGROUP_INTERVAL"
    } else {
        "UNAVAILABLE"
    };
    (cpu_ms, peak_rss, scope.to_owned())
}

fn kubernetes_resource_identities() -> (Vec<String>, Vec<String>) {
    let pods = env::var("NGKG_POD_UID")
        .ok()
        .filter(|value| !value.is_empty())
        .into_iter()
        .collect();
    let nodes = env::var("NGKG_NODE_UID")
        .ok()
        .filter(|value| !value.is_empty())
        .into_iter()
        .collect();
    (pods, nodes)
}

fn human_duration(duration_ms: i64) -> String {
    let total_seconds = duration_ms.max(0) / 1000;
    format!("{}min {}s", total_seconds / 60, total_seconds % 60)
}

fn query_log_view(record: QueryExecutionLogRecord, identity: &Identity) -> QueryLogView {
    let total_time = record.total_duration_ms.map(human_duration);
    QueryLogView {
        query_execution_id: record.query_execution_id,
        dataset_id: record.dataset_id,
        snapshot_id: record.snapshot_id,
        request_id: record.request_id,
        user: QueryLogUser {
            principal_id: record.principal_id.clone(),
        },
        sparql_query: identity
            .can_read_query_text(&record.principal_id)
            .then_some(record.query_text)
            .flatten(),
        query_sha256: record.query_sha256,
        query_form: record.query_form,
        execution_mode: record.execution_mode,
        status: record.status,
        resources: QueryLogResources {
            participating_pods: record.participating_pod_uids,
            participating_nodes: record.participating_node_uids,
            requested_cpu_millicores: record.requested_cpu_millis,
            requested_ram_bytes: record.requested_memory_bytes,
            allocated_cpu_millicores: record.allocated_cpu_millis,
            allocated_ram_bytes: record.allocated_memory_bytes,
            measured_cpu_time_ms: record.measured_cpu_time_millis,
            measured_peak_rss_bytes: record.measured_peak_rss_bytes,
            measured_gpu_time_ms: record.measured_gpu_time_millis,
            measured_gpu_peak_memory_bytes: record.measured_gpu_peak_memory_bytes,
            measurement_scope: record.measurement_scope,
            autoscaling_events: record.autoscaling_events,
        },
        timing: QueryLogTiming {
            start_time_epoch: record.start_time_epoch_ms / 1000,
            start_time_epoch_ms: record.start_time_epoch_ms,
            end_time_epoch: record.end_time_epoch_ms.map(|value| value / 1000),
            end_time_epoch_ms: record.end_time_epoch_ms,
            total_time_ms: record.total_duration_ms,
            total_time,
        },
        result_rows: record.result_rows,
        result_bytes: record.result_bytes,
        cache_hit: record.cache_hit,
        error_code: record.error_code,
    }
}

async fn list_query_logs(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumQuery(parameters): AxumQuery<QueryLogParameters>,
) -> Result<Json<QueryLogPage>, OnlineError> {
    let identity = state.authorizer.authorize_query_logs(&headers)?;
    let limit = parameters.limit.unwrap_or(100);
    let offset = parameters.offset.unwrap_or(0);
    if limit == 0 || limit > state.query_logs.max_page_size || offset > 10_000_000 {
        return Err(OnlineError::Request(
            "query-log limit or offset is outside the configured bounds".to_owned(),
        ));
    }
    if parameters
        .user_id
        .as_ref()
        .is_some_and(|value| value.is_empty() || value.len() > 256)
        || parameters
            .started_after_epoch_ms
            .is_some_and(|value| value < 0)
        || parameters
            .started_before_epoch_ms
            .is_some_and(|value| value < 0)
        || parameters.min_duration_ms.is_some_and(|value| value < 0)
    {
        return Err(OnlineError::Request(
            "query-log filters are outside the configured bounds".to_owned(),
        ));
    }
    let status = parameters.status.map(|value| value.to_ascii_uppercase());
    if status.as_ref().is_some_and(|value| {
        !matches!(
            value.as_str(),
            "RUNNING" | "COMPLETED" | "FAILED" | "TIMED_OUT" | "CANCELLED"
        )
    }) {
        return Err(OnlineError::Request(
            "query-log status is outside the closed vocabulary".to_owned(),
        ));
    }
    let principal_id = if identity.can_read_all_query_logs() {
        parameters.user_id
    } else {
        if parameters
            .user_id
            .as_ref()
            .is_some_and(|value| value != &identity.principal_id)
        {
            return Err(OnlineError::QueryLogNotFound);
        }
        Some(identity.principal_id.clone())
    };
    let fetch_limit = limit.saturating_add(1);
    let records = state
        .manager
        .catalog
        .list_query_execution_logs(
            identity.tenant_id,
            &QueryExecutionLogFilter {
                dataset_id: parameters.dataset_id,
                principal_id,
                status,
                started_after_epoch_ms: parameters.started_after_epoch_ms,
                started_before_epoch_ms: parameters.started_before_epoch_ms,
                min_duration_ms: parameters.min_duration_ms,
                limit: i64::try_from(fetch_limit).map_err(|_| {
                    OnlineError::Request("query-log page size exceeds this platform".to_owned())
                })?,
                offset: i64::try_from(offset).map_err(|_| {
                    OnlineError::Request("query-log offset exceeds this platform".to_owned())
                })?,
            },
        )
        .await?;
    let has_more = records.len() > limit;
    let items = records
        .into_iter()
        .take(limit)
        .map(|record| query_log_view(record, &identity))
        .collect();
    Ok(Json(QueryLogPage {
        items,
        limit,
        offset,
        has_more,
    }))
}

async fn get_query_log(
    State(state): State<AppState>,
    AxumPath(query_execution_id): AxumPath<Uuid>,
    headers: HeaderMap,
) -> Result<Json<QueryLogView>, OnlineError> {
    let identity = state.authorizer.authorize_query_logs(&headers)?;
    let record = state
        .manager
        .catalog
        .get_query_execution_log(identity.tenant_id, query_execution_id)
        .await
        .map_err(|error| match error {
            CatalogError::NotFound => OnlineError::QueryLogNotFound,
            other => OnlineError::Catalog(other),
        })?;
    if !identity.can_read_all_query_logs() && record.principal_id != identity.principal_id {
        return Err(OnlineError::QueryLogNotFound);
    }
    Ok(Json(query_log_view(record, &identity)))
}

async fn query(
    State(state): State<AppState>,
    AxumPath(dataset_id): AxumPath<Uuid>,
    headers: HeaderMap,
    Json(request): Json<QueryRequest>,
) -> Result<Response, OnlineError> {
    let identity = state.authorizer.authorize(&headers)?;
    if request.query.is_empty() || request.query.len() > state.max_query_bytes {
        return Err(OnlineError::Request(
            "query byte length is outside the configured bounds".to_owned(),
        ));
    }
    let query_execution_id = Uuid::new_v4();
    let start_time_epoch_ms = epoch_milliseconds()?;
    let resource_start = cgroup_resource_sample().await;
    let request_id = request_id(&headers);
    let query_sha256: [u8; 32] = Sha256::digest(request.query.as_bytes()).into();
    state
        .manager
        .catalog
        .begin_query_execution_log(
            identity.tenant_id,
            &BeginQueryExecutionLog {
                query_execution_id,
                dataset_id,
                principal_id: identity.principal_id.clone(),
                request_id,
                query_sha256,
                query_text: state
                    .query_logs
                    .store_query_text
                    .then(|| request.query.clone()),
                start_time_epoch_ms,
            },
        )
        .await?;

    let outcome = query_inner(
        state.clone(),
        dataset_id,
        headers,
        request,
        identity.clone(),
    )
    .await;
    match outcome {
        Ok(response) => {
            let cache_hit = response
                .headers()
                .get("x-ngkg-query-cache")
                .and_then(|value| value.to_str().ok())
                == Some("hit");
            let (parts, body) = response.into_parts();
            let bytes = to_bytes(body, state.max_query_response_bytes)
                .await
                .map_err(|error| {
                    OnlineError::SnapshotConflict(format!(
                        "query audit could not inspect the bounded response: {error}"
                    ))
                })?;
            let completed: QueryResponse = serde_json::from_slice(&bytes)?;
            let end_time_epoch_ms = epoch_milliseconds()?;
            let resource_end = cgroup_resource_sample().await;
            let (measured_cpu_time_millis, measured_peak_rss_bytes, measurement_scope) =
                measured_resource_fields(resource_start, resource_end);
            let (participating_pod_uids, participating_node_uids) =
                kubernetes_resource_identities();
            let (nodes, cpu_millis, memory_bytes) =
                query_resource_envelope(&state.query_logs, &completed, cache_hit)?;
            let result_rows = match completed.query_form {
                QueryForm::Select => completed.bindings.len(),
                QueryForm::Ask => usize::from(completed.boolean_result.is_some()),
                QueryForm::Construct | QueryForm::Describe => completed.graph_ntriples.len(),
            };
            state
                .manager
                .catalog
                .finalize_query_execution_log(
                    identity.tenant_id,
                    query_execution_id,
                    &FinalizeQueryExecutionLog {
                        snapshot_id: Some(completed.snapshot_id),
                        query_form: Some(query_form_name(completed.query_form).to_owned()),
                        execution_mode: Some(completed.execution.mode.clone()),
                        status: "COMPLETED".to_owned(),
                        participating_nodes: Some(nodes),
                        allocated_cpu_millis: Some(cpu_millis),
                        allocated_memory_bytes: Some(memory_bytes),
                        requested_cpu_millis: Some(cpu_millis),
                        requested_memory_bytes: Some(memory_bytes),
                        measured_cpu_time_millis,
                        measured_peak_rss_bytes,
                        measured_gpu_time_millis: None,
                        measured_gpu_peak_memory_bytes: None,
                        participating_pod_uids,
                        participating_node_uids,
                        autoscaling_events: json!([]),
                        measurement_scope,
                        result_rows: Some(i64::try_from(result_rows).map_err(|_| {
                            OnlineError::SnapshotConflict(
                                "query result row count exceeds the audit ledger".to_owned(),
                            )
                        })?),
                        result_bytes: Some(i64::try_from(bytes.len()).map_err(|_| {
                            OnlineError::SnapshotConflict(
                                "query result bytes exceed the audit ledger".to_owned(),
                            )
                        })?),
                        cache_hit: Some(cache_hit),
                        end_time_epoch_ms,
                        total_duration_ms: end_time_epoch_ms.saturating_sub(start_time_epoch_ms),
                        error_code: None,
                    },
                )
                .await?;
            let mut response = Response::from_parts(parts, Body::from(bytes));
            response.headers_mut().insert(
                "x-ngkg-query-execution-id",
                header_value(&query_execution_id.to_string())?,
            );
            Ok(response)
        }
        Err(error) => {
            let end_time_epoch_ms = epoch_milliseconds()?;
            let resource_end = cgroup_resource_sample().await;
            let (measured_cpu_time_millis, measured_peak_rss_bytes, measurement_scope) =
                measured_resource_fields(resource_start, resource_end);
            let (participating_pod_uids, participating_node_uids) =
                kubernetes_resource_identities();
            let status = if matches!(&error, OnlineError::GatewayTimeout(_)) {
                "TIMED_OUT"
            } else {
                "FAILED"
            };
            let error_code = error.audit_code().to_owned();
            state
                .manager
                .catalog
                .finalize_query_execution_log(
                    identity.tenant_id,
                    query_execution_id,
                    &FinalizeQueryExecutionLog {
                        snapshot_id: None,
                        query_form: None,
                        execution_mode: None,
                        status: status.to_owned(),
                        participating_nodes: Some(1),
                        allocated_cpu_millis: Some(
                            i64::try_from(state.query_logs.coordinator_cpu_millis).map_err(
                                |_| OnlineError::Request("CPU audit value overflow".to_owned()),
                            )?,
                        ),
                        allocated_memory_bytes: Some(
                            i64::try_from(state.query_logs.coordinator_memory_bytes).map_err(
                                |_| OnlineError::Request("RAM audit value overflow".to_owned()),
                            )?,
                        ),
                        requested_cpu_millis: Some(
                            i64::try_from(state.query_logs.coordinator_cpu_millis).map_err(
                                |_| OnlineError::Request("CPU audit value overflow".to_owned()),
                            )?,
                        ),
                        requested_memory_bytes: Some(
                            i64::try_from(state.query_logs.coordinator_memory_bytes).map_err(
                                |_| OnlineError::Request("RAM audit value overflow".to_owned()),
                            )?,
                        ),
                        measured_cpu_time_millis,
                        measured_peak_rss_bytes,
                        measured_gpu_time_millis: None,
                        measured_gpu_peak_memory_bytes: None,
                        participating_pod_uids,
                        participating_node_uids,
                        autoscaling_events: json!([]),
                        measurement_scope,
                        result_rows: None,
                        result_bytes: None,
                        cache_hit: None,
                        end_time_epoch_ms,
                        total_duration_ms: end_time_epoch_ms.saturating_sub(start_time_epoch_ms),
                        error_code: Some(error_code),
                    },
                )
                .await?;
            Err(error)
        }
    }
}

async fn query_inner(
    state: AppState,
    dataset_id: Uuid,
    headers: HeaderMap,
    request: QueryRequest,
    identity: Identity,
) -> Result<Response, OnlineError> {
    tracing::info!(
        tenant_id = %identity.tenant_id,
        principal_id = %identity.principal_id,
        %dataset_id,
        "query accepted"
    );
    let QueryRequest {
        query: query_text,
        snapshot_id: requested_snapshot_id,
        hydrate,
        default_graph_uris,
        named_graph_uris,
    } = request;
    if query_text.is_empty() || query_text.len() > state.max_query_bytes {
        return Err(OnlineError::Request(
            "query byte length is outside the configured bounds".to_owned(),
        ));
    }
    let parse_task = tokio::task::spawn_blocking(move || {
        let compiled = compile_certified_query(&query_text)?;
        Ok::<_, OnlineError>((query_text, compiled))
    });
    let (query_text, compiled_query) =
        match tokio::time::timeout(state.query_timeout, parse_task).await {
            Ok(joined) => joined??,
            Err(_) => {
                return Err(OnlineError::GatewayTimeout(
                    "SPARQL parsing exceeded the configured query timeout".to_owned(),
                ));
            }
        };
    if compiled_query.execution_analysis().has_remote_service && state.federation.is_none() {
        return Err(OnlineError::Request(
            "SPARQL SERVICE requires a checksum-bound federation endpoint registry".to_owned(),
        ));
    }
    let authorization = state
        .manager
        .clone()
        .authorization_state(identity.tenant_id, dataset_id)
        .await?;
    // This gate runs before semantic_state is allowed to materialize the shared
    // closure. Phase 37 cannot safely expose a closure compiled from a graph the
    // principal is forbidden to observe.
    let preauthorized_graphs = authorized_service_graphs(&identity, &authorization.graph_catalog)?;
    require_reasoning_graph_authorization(&identity, &authorization.graph_catalog)?;
    let semantic = state
        .manager
        .clone()
        .semantic_state(identity.tenant_id, dataset_id)
        .await?;
    require_requested_snapshot(requested_snapshot_id, &semantic.active)?;
    let query_sha256 = hex::encode(Sha256::digest(query_text.as_bytes()));
    let query_dataset = compiled_query.dataset_specification().clone();
    let protocol_dataset = ProtocolDatasetSpecification {
        default_graph_uris,
        named_graph_uris,
    };
    let active_dataset = resolve_request_dataset(
        &semantic.graph_catalog,
        &identity.graph_authorization_labels,
        &query_dataset,
        &protocol_dataset,
    )?;
    let certificate = if compiled_query.execution_analysis().has_remote_service {
        // Remote state is outside the immutable snapshot trust boundary. Even a malformed legacy
        // manifest must not make a SERVICE query cacheable or snapshot-certifiable.
        None
    } else {
        semantic
            .manifest
            .certified_queries
            .iter()
            .find(|query| query.query_sha256 == query_sha256)
            .cloned()
    };
    let deployment_limits = CertifiedQueryExecutionLimits {
        max_solution_rows: state.max_query_result_rows,
        max_graph_triples: state.max_query_graph_triples,
        max_graph_blank_nodes: state.max_query_graph_blank_nodes,
    };
    require_native_cutover_admission(&state, &compiled_query, certificate.as_ref())?;
    if state.online_direct.is_some() {
        return execute_online_direct_query(
            state.clone(),
            headers.clone(),
            identity.tenant_id,
            dataset_id,
            query_text,
            compiled_query,
            semantic,
            active_dataset,
            preauthorized_graphs,
            hydrate,
            deployment_limits,
        )
        .await;
    }
    let Some(certificate) = certificate else {
        return execute_uncertified_exact_query(
            state.clone(),
            headers.clone(),
            identity.tenant_id,
            dataset_id,
            query_text,
            compiled_query,
            semantic,
            active_dataset,
            preauthorized_graphs,
            hydrate,
            deployment_limits,
            None,
            None,
            None,
        )
        .await;
    };
    require_supported_result_hash_version(certificate.result_hash_version)?;
    if certificate.sparql_algebra_format_version != SPARQL_ALGEBRA_FORMAT_VERSION
        || certificate.sparql_algebra_sha256 != compiled_query.canonical_sse_sha256()
        || certificate.query_form != compiled_query.form()
        || certificate.ordered != compiled_query.solution_order_is_significant()
    {
        return Err(OnlineError::SnapshotConflict(
            "query form, ordering, or algebra differs from its offline semantic certificate"
                .to_owned(),
        ));
    }
    let certified_limits = CertifiedQueryExecutionLimits {
        max_solution_rows: usize::try_from(certificate.max_solution_rows).map_err(|_| {
            OnlineError::SnapshotConflict(
                "certified solution-row limit exceeds this platform".to_owned(),
            )
        })?,
        max_graph_triples: usize::try_from(certificate.max_graph_triples).map_err(|_| {
            OnlineError::SnapshotConflict(
                "certified graph-triple limit exceeds this platform".to_owned(),
            )
        })?,
        max_graph_blank_nodes: usize::try_from(certificate.max_graph_blank_nodes).map_err(
            |_| {
                OnlineError::SnapshotConflict(
                    "certified graph blank-node limit exceeds this platform".to_owned(),
                )
            },
        )?,
    };
    let effective_limits = CertifiedQueryExecutionLimits {
        max_solution_rows: certified_limits
            .max_solution_rows
            .min(deployment_limits.max_solution_rows),
        max_graph_triples: certified_limits
            .max_graph_triples
            .min(deployment_limits.max_graph_triples),
        max_graph_blank_nodes: certified_limits
            .max_graph_blank_nodes
            .min(deployment_limits.max_graph_blank_nodes),
    };
    if effective_limits.max_solution_rows == 0
        || effective_limits.max_graph_triples == 0
        || effective_limits.max_graph_blank_nodes == 0
    {
        return Err(OnlineError::SnapshotConflict(
            "certified query result ceilings are invalid".to_owned(),
        ));
    }
    let routing = certificate
        .routing
        .clone()
        .ok_or(ReferenceRuntimeError::UncertifiedQuery)?;
    if active_dataset.active_dataset_sha256 != routing.active_dataset_sha256 {
        return Err(OnlineError::ActiveDatasetNotCertified);
    }
    let authorized_graphs = authorize_query_graphs(&identity, &semantic, &routing)?;
    if authorized_graphs.graph_set_sha256 != preauthorized_graphs.graph_set_sha256 {
        return Err(OnlineError::SnapshotConflict(
            "graph authorization changed while the semantic snapshot was loading".to_owned(),
        ));
    }
    let cache_key = QueryCacheKey {
        tenant_id: identity.tenant_id,
        dataset_id,
        snapshot_id: semantic.active.snapshot.snapshot_id,
        manifest_sha256: semantic.active.snapshot.manifest_sha256.clone(),
        serving_root_sha256: semantic_serving_identity(&semantic.active)?,
        query_sha256: query_sha256.clone(),
        authorized_graph_set_sha256: authorized_graphs.graph_set_sha256.clone(),
        active_dataset_sha256: active_dataset.active_dataset_sha256.clone(),
        dataset_selection_source: active_dataset.selection_source.code(),
        hydrate,
    };
    let cache_digest = cache_key.digest()?;
    let cache = state.query_result_cache.clone().ok_or_else(|| {
        OnlineError::SnapshotConflict("query role has no certified result cache".to_owned())
    })?;
    let cache_flight =
        query_cache_flight(Arc::clone(&state.query_cache_flights), cache_digest.clone()).await;
    let cache_guard = Arc::clone(&cache_flight.flight).lock_owned().await;
    let cache_for_read = Arc::clone(&cache);
    let key_for_read = cache_key.clone();
    match tokio::task::spawn_blocking(move || cache_for_read.get(&key_for_read)).await {
        Ok(Ok(QueryCacheLookup::Hit(bytes))) => {
            match serde_json::from_slice::<QueryResponse>(&bytes)
                .map_err(OnlineError::Json)
                .and_then(|response| {
                    validate_cached_query_response(
                        &response,
                        &cache_key,
                        &routing,
                        &certificate.scope,
                        certificate.query_form,
                        &certificate.observed_result_sha256,
                        certificate.ordered,
                        effective_limits,
                        semantic.active.identity_namespace,
                        state.max_qualified_entities,
                        state.max_hydration_rows,
                        state.max_distributed_fragments,
                        state.shuffle_partition_count,
                        state.max_worker_join_build_rows,
                        &authorized_graphs.graph_iris,
                    )
                }) {
                Ok(()) => {
                    state
                        .admission
                        .query_cache_hits
                        .fetch_add(1, Ordering::Relaxed);
                    drop(cache_guard);
                    drop(cache_flight);
                    return Ok(query_json_response(bytes, true));
                }
                Err(error) => {
                    state
                        .admission
                        .query_cache_invalid
                        .fetch_add(1, Ordering::Relaxed);
                    tracing::warn!(%error, %cache_digest, "certified query cache entry failed logical validation");
                    let cache_for_invalidate = Arc::clone(&cache);
                    let key_for_invalidate = cache_key.clone();
                    match tokio::task::spawn_blocking(move || {
                        cache_for_invalidate.invalidate(&key_for_invalidate)
                    })
                    .await
                    {
                        Ok(Ok(())) => {}
                        Ok(Err(error)) => {
                            state
                                .admission
                                .query_cache_errors
                                .fetch_add(1, Ordering::Relaxed);
                            tracing::warn!(%error, %cache_digest, "invalid query cache entry could not be removed");
                        }
                        Err(error) => {
                            state
                                .admission
                                .query_cache_errors
                                .fetch_add(1, Ordering::Relaxed);
                            tracing::warn!(%error, %cache_digest, "query cache invalidation task failed");
                        }
                    }
                }
            }
        }
        Ok(Ok(QueryCacheLookup::Miss)) => {}
        Ok(Err(error)) => {
            state
                .admission
                .query_cache_errors
                .fetch_add(1, Ordering::Relaxed);
            tracing::warn!(%error, %cache_digest, "query cache read failed; recomputing certified result");
        }
        Err(error) => {
            state
                .admission
                .query_cache_errors
                .fetch_add(1, Ordering::Relaxed);
            tracing::warn!(%error, %cache_digest, "query cache read task failed; recomputing certified result");
        }
    }
    state
        .admission
        .query_cache_misses
        .fetch_add(1, Ordering::Relaxed);
    let (result, execution) = if let Some(_) = routing.distributed.as_ref() {
        if compiled_query.form() != QueryForm::Select {
            return Err(OnlineError::SnapshotConflict(
                "only SELECT queries may carry a distributed Phase 39 certificate".to_owned(),
            ));
        }
        let token = bearer(&headers)?.to_owned();
        match tokio::time::timeout(
            state.query_timeout,
            execute_distributed_query(
                state.clone(),
                Arc::clone(&semantic),
                routing.clone(),
                query_sha256.clone(),
                token,
            ),
        )
        .await
        {
            Ok(result) => result?,
            Err(_) => {
                return Err(OnlineError::GatewayTimeout(
                    "distributed SPARQL evaluation exceeded the configured query timeout"
                        .to_owned(),
                ));
            }
        }
    } else {
        let (runtime, _) = state
            .manager
            .clone()
            .routed_runtime(Arc::clone(&semantic), query_sha256.clone())
            .await?;
        let active_dataset_for_runtime = active_dataset.clone();
        let graph_catalog_for_runtime = Arc::clone(&semantic.graph_catalog);
        let compiled_for_runtime = compiled_query.clone();
        let query_text_for_runtime = query_text.clone();
        let cancellation = CancellationToken::new();
        let cancellation_for_runtime = cancellation.clone();
        let mut execution_task = tokio::task::spawn_blocking(move || {
            runtime.execute_compiled_with_dataset_bounded_cancellable(
                &query_text_for_runtime,
                &compiled_for_runtime,
                &active_dataset_for_runtime,
                &graph_catalog_for_runtime,
                deployment_limits,
                Some(cancellation_for_runtime),
            )
        });
        let result = match tokio::time::timeout(state.query_timeout, &mut execution_task).await {
            Ok(joined) => joined??,
            Err(_) => {
                cancellation.cancel();
                return Err(OnlineError::GatewayTimeout(
                    "exact SPARQL evaluation exceeded the configured query timeout".to_owned(),
                ));
            }
        };
        (
            result,
            ExecutionResponse {
                mode: "certified_local_route".to_owned(),
                exchange_format: "none".to_owned(),
                fragment_ingress_mode: "none".to_owned(),
                fragment_ingress_bytes: 0,
                fragment_materialization_mode: "none".to_owned(),
                fragment_owned_rows: 0,
                shuffle_result_ingress_mode: "none".to_owned(),
                shuffle_result_ingress_bytes: 0,
                intermediate_result_mode: "none".to_owned(),
                assembled_intermediate_owned_rows: 0,
                fragment_count: 0,
                worker_count: 0,
                shuffle_partition_count: 0,
                shuffle_worker_count: 0,
                shuffle_spill_mode: "none".to_owned(),
                shuffle_spill_bytes: 0,
                shuffle_cache_mode: "none".to_owned(),
                shuffle_cache_hits: 0,
                worker_join_mode: "none".to_owned(),
                worker_join_spill_bytes: 0,
                worker_join_grace_partitions: 0,
                worker_join_max_build_rows: 0,
                worker_input_mode: "none".to_owned(),
                worker_input_bytes: 0,
                coordinator_request_mode: "none".to_owned(),
                coordinator_request_bytes: 0,
                plan_sha256: None,
            },
        )
    };
    if result.query_sha256 != query_sha256 {
        return Err(OnlineError::SnapshotConflict(
            "routed runtime executed a different query certificate".to_owned(),
        ));
    }
    if result.qualified_entity_iris.len() > state.max_qualified_entities {
        return Err(OnlineError::Request(
            "query qualified more entities than the online ceiling".to_owned(),
        ));
    }
    let qualified_entities = qualify_entities(&result, semantic.active.identity_namespace)?;
    let hydrated_payload = if hydrate && !qualified_entities.is_empty() {
        let token = bearer(&headers)?;
        let hydration_request = HydrationRequest {
            snapshot_id: semantic.active.snapshot.snapshot_id,
            serving_root_sha256: required_serving_root(&semantic.active)?
                .serving_root_sha256
                .clone(),
            entities: qualified_entities.clone(),
        };
        let base = state
            .hydration_url
            .as_deref()
            .ok_or_else(|| OnlineError::Upstream("hydration URL is absent".to_owned()))?;
        let response = state
            .http
            .post(format!(
                "{}/v1/datasets/{dataset_id}/hydrate",
                base.trim_end_matches('/')
            ))
            .bearer_auth(token)
            .json(&hydration_request)
            .send()
            .await
            .map_err(|error| upstream_transport_error("hydration request", error))?;
        if !response.status().is_success() {
            return Err(upstream_status_error("hydration", response.status()));
        }
        let bytes = read_bounded_response(response, state.max_hydration_response_bytes).await?;
        let hydrated = serde_json::from_slice::<HydrationResponse>(&bytes)?;
        if hydrated.dataset_id != dataset_id
            || hydrated.snapshot_id != semantic.active.snapshot.snapshot_id
            || hydrated.serving_root_sha256
                != required_serving_root(&semantic.active)?.serving_root_sha256
        {
            return Err(OnlineError::Upstream(
                "hydration response identity differs from the semantic request".to_owned(),
            ));
        }
        validate_hydrated_rows(
            &hydrated.rows,
            &qualified_entities,
            state.max_hydration_rows,
            &authorized_graphs.graph_iris,
        )?;
        hydrated.rows
    } else {
        Vec::new()
    };
    let response = QueryResponse {
        dataset_id,
        snapshot_id: result.snapshot_id,
        serving_root_sha256: semantic_serving_identity(&semantic.active)?,
        query_sha256: result.query_sha256,
        query_form: result.query_form,
        authorized_graph_set_sha256: authorized_graphs.graph_set_sha256.clone(),
        active_dataset_sha256: active_dataset.active_dataset_sha256.clone(),
        coverage_scope: result.coverage_scope,
        complete: true,
        routing: RoutingResponse {
            selection_mode: routing.selection_mode.clone(),
            dataset_selection_source: active_dataset.selection_source,
            default_graph_iris: graph_iris_for_ids(
                &semantic.graph_catalog,
                &active_dataset.default_graph_ids,
            )?,
            named_graph_iris: graph_iris_for_ids(
                &semantic.graph_catalog,
                &active_dataset.named_graph_ids,
            )?,
            active_dataset_sha256: active_dataset.active_dataset_sha256.clone(),
            include_internal_closure: routing.include_internal_closure,
            selected_graph_count: u32::try_from(routing.selected_graph_iris.len()).map_err(
                |_| OnlineError::SnapshotConflict("selected graph count overflow".to_owned()),
            )?,
            selected_graph_iris: routing.selected_graph_iris.clone(),
            total_graph_count: routing.total_graph_count,
            capability_index_sha256: routing.capability_index_sha256.clone(),
            routed_dataset_sha256: routing.route_artifact_sha256.clone(),
        },
        execution,
        head: result.head,
        bindings: result.bindings,
        boolean_result: result.boolean_result,
        graph_ntriples: result.graph_ntriples,
        qualified_entities,
        hydrated_payload,
        entailment: None,
        property_path_execution: None,
        federation: None,
    };
    let mut body = BoundedBuffer::new(state.max_query_response_bytes);
    serde_json::to_writer(&mut body, &response).map_err(|_| {
        OnlineError::Request("serialized query response exceeds its byte ceiling".to_owned())
    })?;
    let bytes = Bytes::from(body.into_bytes());
    let cache_for_write = Arc::clone(&cache);
    let key_for_write = cache_key;
    let bytes_for_write = bytes.clone();
    match tokio::task::spawn_blocking(move || {
        cache_for_write.insert(&key_for_write, &bytes_for_write)
    })
    .await
    {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            state
                .admission
                .query_cache_errors
                .fetch_add(1, Ordering::Relaxed);
            tracing::warn!(%error, %cache_digest, "complete certified query response was not cached");
        }
        Err(error) => {
            state
                .admission
                .query_cache_errors
                .fetch_add(1, Ordering::Relaxed);
            tracing::warn!(%error, %cache_digest, "query cache write task failed");
        }
    }
    drop(cache_guard);
    drop(cache_flight);
    Ok(query_json_response(bytes, false))
}

struct ExactBgpExecution {
    result: DirectBgpResult,
    certificate: DirectCertificate,
    proof_manifest: DirectProofManifest,
}

#[allow(clippy::too_many_arguments)]
async fn execute_online_direct_query(
    state: AppState,
    headers: HeaderMap,
    tenant_id: Uuid,
    dataset_id: Uuid,
    query_text: String,
    compiled_query: CompiledSparqlQuery,
    semantic: Arc<SemanticState>,
    active_dataset: ResolvedDataset,
    authorized_graphs: AuthorizedQueryGraphs,
    hydrate: bool,
    deployment_limits: CertifiedQueryExecutionLimits,
) -> Result<Response, OnlineError> {
    let config = state.online_direct.clone().ok_or_else(|| {
        OnlineError::SnapshotConflict("online Direct configuration disappeared".to_owned())
    })?;
    let algebra_plan = compiled_query
        .distributed_algebra_plan(DistributedAlgebraLimits {
            partition_count: state.shuffle_partition_count,
            max_input_rows: u64::try_from(state.max_distributed_intermediate_rows).map_err(
                |_| OnlineError::Request("algebra input row ceiling overflow".to_owned()),
            )?,
            max_output_rows: u64::try_from(state.max_distributed_intermediate_rows).map_err(
                |_| OnlineError::Request("algebra output row ceiling overflow".to_owned()),
            )?,
            max_exchange_bytes: u64::try_from(state.max_shuffle_exchange_bytes).map_err(|_| {
                OnlineError::Request("algebra exchange ceiling overflow".to_owned())
            })?,
            max_spill_bytes: state.max_shuffle_spill_bytes,
        })
        .map_err(|error| OnlineError::Request(error.to_string()))?;
    let algebra_plan_sha256 = sha256_json(&algebra_plan)?;
    let algebra_stage_count = u64::try_from(algebra_plan.stages.len())
        .map_err(|_| OnlineError::Request("algebra stage count overflow".to_owned()))?;
    let algebra_waves = algebra_execution_waves(&algebra_plan)
        .map_err(|error| OnlineError::Request(error.to_string()))?;
    let algebra_wave_count = u64::try_from(algebra_waves.len())
        .map_err(|_| OnlineError::Request("algebra wave count overflow".to_owned()))?;
    let algebra_work_item_count = algebra_waves.iter().try_fold(0_u64, |total, wave| {
        u64::try_from(wave.work_items.len())
            .ok()
            .and_then(|width| total.checked_add(width))
            .ok_or_else(|| OnlineError::Request("algebra work-item count overflow".to_owned()))
    })?;
    let property_path_plans = compiled_query
        .distributed_property_path_plans(DistributedPropertyPathLimits {
            partition_count: state.shuffle_partition_count,
            max_iterations: state.property_path_max_iterations,
            max_frontier_items: state.property_path_max_frontier_items,
            max_visited_items: state.property_path_max_visited_items,
            max_checkpoint_bytes: state.property_path_max_checkpoint_bytes,
            max_spill_bytes: state.property_path_max_spill_bytes,
            hot_vertex_degree: state.property_path_hot_vertex_degree,
            max_hot_vertex_splits: state.property_path_max_hot_vertex_splits,
        })
        .map_err(|error| OnlineError::Request(error.to_string()))?;
    let property_path_plan_sha256 = sha256_json(&property_path_plans)?;
    let property_path_count = u64::try_from(property_path_plans.len())
        .map_err(|_| OnlineError::Request("property-path count overflow".to_owned()))?;
    let property_path_automaton_sha256s = property_path_plans
        .iter()
        .map(|plan| plan.automaton_sha256.clone())
        .collect::<Vec<_>>();
    let allowed_roles = BTreeSet::from(["semkg".to_owned()]);
    let direct_dataset = restrict_resolved_dataset_to_roles(
        &semantic.graph_catalog,
        &active_dataset,
        &allowed_roles,
    )
    .map_err(|error| {
        OnlineError::Request(format!(
            "OWL Direct execution requires an authorized active */semkg graph: {error}"
        ))
    })?;
    for graph_id in direct_dataset
        .default_graph_ids
        .iter()
        .chain(direct_dataset.named_graph_ids.iter())
    {
        let graph = semantic.graph_catalog.by_id(*graph_id).ok_or_else(|| {
            OnlineError::SnapshotConflict("Direct graph ID is absent from catalog".to_owned())
        })?;
        let LogicalGraphName::Named { iri } = &graph.name else {
            return Err(OnlineError::SnapshotConflict(
                "Direct graph is not named".to_owned(),
            ));
        };
        if graph.role != "semkg" || !iri.ends_with("/semkg") {
            return Err(OnlineError::GraphForbidden);
        }
    }

    let signature = Arc::clone(&semantic.owl_signature);
    let classify_query = compiled_query.clone();
    let classification_limits = state.direct_bgp_classification_limits;
    let classification = tokio::time::timeout(
        state.query_timeout,
        tokio::task::spawn_blocking(move || {
            classify_direct_bgps(&classify_query, signature.as_ref(), classification_limits)
        }),
    )
    .await
    .map_err(|_| {
        OnlineError::GatewayTimeout("OWL Direct-BGP classification timed out".to_owned())
    })??
    .map_err(|error| OnlineError::Request(error.to_string()))?;
    if classification.records.iter().any(|record| {
        route_entailment(EntailmentRoutingInput {
            legality: record.status,
            semantic_index: CoverageState::Unknown,
            finite_closure: CoverageState::Unknown,
        }) == EntailmentRoute::IllegalOwlDirect
    }) {
        return Err(OnlineError::Request(
            "a BGP is illegal under OWL 2 Direct Semantics".to_owned(),
        ));
    }

    let snapshot_root = config
        .ontology_root
        .join(dataset_id.to_string())
        .join(semantic.active.snapshot.snapshot_id.to_string());
    tokio::fs::create_dir_all(snapshot_root.join("data")).await?;
    tokio::fs::create_dir_all(snapshot_root.join("ontology")).await?;
    state
        .manager
        .clone()
        .materialize_snapshot_artifact(
            semantic.active.clone(),
            Arc::clone(&semantic.manifest),
            "data/query-dataset.nq".to_owned(),
            snapshot_root.join("data/query-dataset.nq"),
        )
        .await?;
    let mut ontology_artifacts = BTreeSet::new();
    for document in &semantic.owl_signature_document.ontology_documents {
        let artifact = semantic
            .manifest
            .artifacts
            .iter()
            .find(|artifact| {
                artifact.sha256 == document.sha256
                    && artifact.relative_path.starts_with("ontology/")
            })
            .ok_or_else(|| {
                OnlineError::SnapshotConflict(
                    "pinned OWL import is absent from the snapshot manifest".to_owned(),
                )
            })?;
        ontology_artifacts.insert(artifact.relative_path.clone());
    }
    for relative in ontology_artifacts {
        let destination = snapshot_root.join(&relative);
        if let Some(parent) = destination.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        state
            .manager
            .clone()
            .materialize_snapshot_artifact(
                semantic.active.clone(),
                Arc::clone(&semantic.manifest),
                relative,
                destination,
            )
            .await?;
    }

    let query_sha256 = hex::encode(Sha256::digest(query_text.as_bytes()));
    let mut exact_relations = Vec::with_capacity(classification.records.len());
    let mut evidence_executions = Vec::new();
    for legality in &classification.records {
        match &legality.graph_scope {
            DirectBgpScope::Default | DirectBgpScope::Named { .. } => {
                let execution = execute_one_online_direct_bgp(
                    &state,
                    &config,
                    &semantic,
                    &compiled_query,
                    &direct_dataset,
                    legality,
                    None,
                    &snapshot_root,
                    &query_sha256,
                )
                .await?;
                exact_relations.push(execution.result.clone());
                evidence_executions.push(execution);
            }
            DirectBgpScope::NamedVariable { variable } => {
                let mut scoped = Vec::new();
                for graph_id in &direct_dataset.named_graph_ids {
                    let graph = semantic.graph_catalog.by_id(*graph_id).ok_or_else(|| {
                        OnlineError::SnapshotConflict("named Direct graph ID is absent".to_owned())
                    })?;
                    let LogicalGraphName::Named { iri } = &graph.name else {
                        return Err(OnlineError::SnapshotConflict(
                            "named Direct graph has no IRI".to_owned(),
                        ));
                    };
                    let execution = execute_one_online_direct_bgp(
                        &state,
                        &config,
                        &semantic,
                        &compiled_query,
                        &direct_dataset,
                        legality,
                        Some(iri),
                        &snapshot_root,
                        &query_sha256,
                    )
                    .await?;
                    scoped.push((iri.clone(), execution.result.clone()));
                    evidence_executions.push(execution);
                }
                exact_relations.push(combine_named_graph_results(
                    legality,
                    variable,
                    &scoped,
                    &direct_dataset,
                    &semantic,
                    &query_sha256,
                )?);
            }
        }
    }
    let rewritten = substitute_exact_bgp_results(
        compiled_query.query_clone(),
        &exact_relations,
        deployment_limits.max_solution_rows,
    )
    .map_err(|error| OnlineError::SnapshotConflict(error.to_string()))?;
    let evidence = ExactEntailmentEvidence {
        regime: EntailmentRegime::Owl2Direct,
        bgp_count: u64::try_from(exact_relations.len())
            .map_err(|_| OnlineError::SnapshotConflict("Direct BGP count overflow".to_owned()))?,
        result_sha256s: evidence_executions
            .iter()
            .map(|execution| {
                direct_bgp_result_sha256(&execution.result)
                    .map_err(|error| OnlineError::SnapshotConflict(error.to_string()))
            })
            .collect::<Result<Vec<_>, _>>()?,
        certificate_sha256s: evidence_executions
            .iter()
            .map(|execution| sha256_json(&execution.certificate))
            .collect::<Result<Vec<_>, _>>()?,
        proof_manifest_sha256s: evidence_executions
            .iter()
            .map(|execution| sha256_json(&execution.proof_manifest))
            .collect::<Result<Vec<_>, _>>()?,
        certificates: evidence_executions
            .iter()
            .map(|execution| execution.certificate.clone())
            .collect(),
        proof_manifests: evidence_executions
            .iter()
            .map(|execution| execution.proof_manifest.clone())
            .collect(),
        distributed_algebra_plan_sha256: algebra_plan_sha256.clone(),
        distributed_algebra_stage_count: algebra_stage_count,
        distributed_algebra_wave_count: algebra_wave_count,
        distributed_algebra_work_item_count: algebra_work_item_count,
        distributed_algebra_partition_count: state.shuffle_partition_count,
        distributed_algebra_scalar_equivalence_required: algebra_plan.require_scalar_equivalence,
        distributed_property_path_plan_sha256: property_path_plan_sha256,
        distributed_property_path_count: property_path_count,
        distributed_property_path_automaton_sha256s: property_path_automaton_sha256s,
        distributed_property_path_partition_count: state.shuffle_partition_count,
        distributed_property_path_scalar_equivalence_required: property_path_plans
            .iter()
            .all(|plan| plan.require_scalar_equivalence),
        complete: true,
    };
    let native_precomputed = if state.native_cutover_mode.requires_native() {
        Some(finalize_native_exact_select(
            &compiled_query,
            &exact_relations,
            &semantic,
            &query_sha256,
            deployment_limits,
            &algebra_plan_sha256,
            config.worker_base_urls.len(),
        )?)
    } else {
        None
    };
    execute_uncertified_exact_query(
        state,
        headers,
        tenant_id,
        dataset_id,
        query_text,
        compiled_query,
        semantic,
        direct_dataset,
        authorized_graphs,
        hydrate,
        deployment_limits,
        Some(rewritten),
        Some(evidence),
        native_precomputed,
    )
    .await
}

/// Finalize an exact-HermiT SELECT with only standards-safe native bag operators. The routine
/// consumes the same ordered BGP result vector used by the typed rewrite, expands multiplicities
/// under the query ceiling, and never opens an Oxigraph store. Unsupported algebra fails closed.
fn finalize_native_exact_select(
    compiled: &CompiledSparqlQuery,
    relations: &[DirectBgpResult],
    semantic: &SemanticState,
    query_sha256: &str,
    limits: CertifiedQueryExecutionLimits,
    plan_sha256: &str,
    reasoner_worker_count: usize,
) -> Result<(CertifiedSemanticResult, ExecutionResponse), OnlineError> {
    let SparqlQuery::Select { pattern, .. } = compiled.query() else {
        return Err(OnlineError::NativeCutoverUnavailable(
            "native exact finalization currently requires SELECT algebra".to_owned(),
        ));
    };
    let mut cursor = 0_usize;
    let (head, bindings) = evaluate_native_exact_pattern(
        pattern,
        relations,
        &mut cursor,
        limits.max_solution_rows,
    )?;
    if cursor != relations.len() {
        return Err(OnlineError::SnapshotConflict(
            "native exact finalizer did not consume the complete BGP relation set".to_owned(),
        ));
    }
    let qualified_entity_iris = bindings
        .iter()
        .filter_map(serde_json::Value::as_object)
        .flat_map(|binding| binding.values())
        .filter_map(|term| {
            let object = term.as_object()?;
            (object.get("type")?.as_str()? == "uri")
                .then(|| object.get("value")?.as_str().map(ToOwned::to_owned))
                .flatten()
        })
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let result = CertifiedSemanticResult {
        dataset_id: semantic.active.snapshot.dataset_id,
        snapshot_id: semantic.active.snapshot.snapshot_id,
        query_sha256: query_sha256.to_owned(),
        query_form: QueryForm::Select,
        head,
        bindings,
        boolean_result: None,
        graph_ntriples: Vec::new(),
        qualified_entity_iris,
        coverage_scope: "authorized active */semkg dataset; exact OWL 2 Direct BGPs; native Rust multiset algebra".to_owned(),
    };
    let fragment_count = u32::try_from(relations.len()).map_err(|_| {
        OnlineError::NativeCutoverUnavailable("exact BGP count exceeds u32".to_owned())
    })?;
    let worker_count = u32::try_from(reasoner_worker_count).map_err(|_| {
        OnlineError::NativeCutoverUnavailable("reasoner worker count exceeds u32".to_owned())
    })?;
    let binding_count = u64::try_from(result.bindings.len()).map_err(|_| {
        OnlineError::NativeCutoverUnavailable("native result row count overflow".to_owned())
    })?;
    Ok((
        result,
        ExecutionResponse {
            mode: "native_distributed_exact_bgp_algebra_v1".to_owned(),
            exchange_format: "checksum_bound_direct_bgp_relations_v1".to_owned(),
            fragment_ingress_mode: "exact_reasoner_partition_barrier_v1".to_owned(),
            fragment_ingress_bytes: 0,
            fragment_materialization_mode: "native_rust_binding_multiset_v1".to_owned(),
            fragment_owned_rows: binding_count,
            shuffle_result_ingress_mode: "none".to_owned(),
            shuffle_result_ingress_bytes: 0,
            intermediate_result_mode: "bounded_complete_relations_v1".to_owned(),
            assembled_intermediate_owned_rows: binding_count,
            fragment_count,
            worker_count,
            shuffle_partition_count: 0,
            shuffle_worker_count: 0,
            shuffle_spill_mode: "bounded_native_operator_v1".to_owned(),
            shuffle_spill_bytes: 0,
            shuffle_cache_mode: "none".to_owned(),
            shuffle_cache_hits: 0,
            worker_join_mode: "native_rust_exact_bag_v1".to_owned(),
            worker_join_spill_bytes: 0,
            worker_join_grace_partitions: 0,
            worker_join_max_build_rows: 0,
            worker_input_mode: "proof_verified_exact_relations_v1".to_owned(),
            worker_input_bytes: 0,
            coordinator_request_mode: "native_cutover_required_v1".to_owned(),
            coordinator_request_bytes: 0,
            plan_sha256: Some(plan_sha256.to_owned()),
        },
    ))
}

fn evaluate_native_exact_pattern(
    pattern: &GraphPattern,
    relations: &[DirectBgpResult],
    cursor: &mut usize,
    max_rows: usize,
) -> Result<(Vec<String>, Vec<serde_json::Value>), OnlineError> {
    match pattern {
        GraphPattern::Bgp { .. } => {
            let relation = relations.get(*cursor).ok_or_else(|| {
                OnlineError::SnapshotConflict(
                    "native exact algebra has no result for a BGP leaf".to_owned(),
                )
            })?;
            *cursor = cursor.checked_add(1).ok_or_else(|| {
                OnlineError::NativeCutoverUnavailable("BGP cursor overflow".to_owned())
            })?;
            direct_result_bindings(relation, max_rows)
        }
        GraphPattern::Join { left, right } => {
            let (left_head, left_rows) =
                evaluate_native_exact_pattern(left, relations, cursor, max_rows)?;
            let (right_head, right_rows) =
                evaluate_native_exact_pattern(right, relations, cursor, max_rows)?;
            Ok((
                union_binding_heads(&left_head, &right_head),
                inner_join_sparql_json(&left_rows, &right_rows, max_rows)
                    .map_err(distributed_execution_error)?,
            ))
        }
        GraphPattern::Union { left, right } => {
            let (left_head, left_rows) =
                evaluate_native_exact_pattern(left, relations, cursor, max_rows)?;
            let (right_head, right_rows) =
                evaluate_native_exact_pattern(right, relations, cursor, max_rows)?;
            Ok((
                union_binding_heads(&left_head, &right_head),
                union_sparql_json(&left_rows, &right_rows, max_rows)
                    .map_err(distributed_execution_error)?,
            ))
        }
        GraphPattern::Minus { left, right } => {
            let (head, left_rows) =
                evaluate_native_exact_pattern(left, relations, cursor, max_rows)?;
            let (_, right_rows) =
                evaluate_native_exact_pattern(right, relations, cursor, max_rows)?;
            Ok((
                head,
                minus_sparql_json(&left_rows, &right_rows, max_rows)
                    .map_err(distributed_execution_error)?,
            ))
        }
        GraphPattern::Project { inner, variables } => {
            let (_, rows) = evaluate_native_exact_pattern(inner, relations, cursor, max_rows)?;
            let head = variables
                .iter()
                .map(|variable| variable.as_str().to_owned())
                .collect::<Vec<_>>();
            let rows = project_sparql_json(&rows, &head).map_err(distributed_execution_error)?;
            Ok((head, rows))
        }
        GraphPattern::Distinct { inner } => {
            let (head, rows) = evaluate_native_exact_pattern(inner, relations, cursor, max_rows)?;
            Ok((
                head,
                distinct_sparql_json(&rows, max_rows).map_err(distributed_execution_error)?,
            ))
        }
        GraphPattern::Reduced { inner } => {
            evaluate_native_exact_pattern(inner, relations, cursor, max_rows)
        }
        GraphPattern::Slice {
            inner,
            start,
            length,
        } => {
            let (head, rows) = evaluate_native_exact_pattern(inner, relations, cursor, max_rows)?;
            Ok((
                head,
                global_slice_sparql_json(std::slice::from_ref(&rows), *start, *length, max_rows)
                    .map_err(distributed_execution_error)?,
            ))
        }
        _ => Err(OnlineError::NativeCutoverUnavailable(
            "typed algebra reached an operator without a native exact finalizer".to_owned(),
        )),
    }
}

fn direct_result_bindings(
    result: &DirectBgpResult,
    max_rows: usize,
) -> Result<(Vec<String>, Vec<serde_json::Value>), OnlineError> {
    if result.outcome.status != DirectBgpStatus::Complete
        || result.outcome.exactness != DirectBgpExactness::Exact
        || result.outcome.completeness != DirectBgpCompleteness::Complete
    {
        return Err(OnlineError::SnapshotConflict(
            "native finalization received an incomplete exact-BGP relation".to_owned(),
        ));
    }
    let mut rows = Vec::new();
    for solution in &result.solutions {
        let multiplicity = usize::try_from(solution.multiplicity).map_err(|_| {
            OnlineError::NativeCutoverUnavailable("BGP multiplicity exceeds this platform".to_owned())
        })?;
        if rows
            .len()
            .checked_add(multiplicity)
            .is_none_or(|total| total > max_rows)
        {
            return Err(OnlineError::Request(
                "exact BGP expansion exceeds the native row ceiling".to_owned(),
            ));
        }
        let binding = solution
            .bindings
            .iter()
            .map(|(variable, term)| Ok((variable.clone(), direct_term_json(term)?)))
            .collect::<Result<serde_json::Map<_, _>, OnlineError>>()?;
        rows.extend(std::iter::repeat_n(
            serde_json::Value::Object(binding),
            multiplicity,
        ));
    }
    Ok((result.variables.clone(), rows))
}

fn direct_term_json(term: &DirectBgpRdfTerm) -> Result<serde_json::Value, OnlineError> {
    let value = match term {
        DirectBgpRdfTerm::Iri { value } => {
            serde_json::json!({"type": "uri", "value": value})
        }
        DirectBgpRdfTerm::BlankNode { value } => {
            serde_json::json!({"type": "bnode", "value": value})
        }
        DirectBgpRdfTerm::Literal {
            lexical_form,
            datatype_iri,
            language,
        } => {
            let mut object = serde_json::Map::from_iter([
                ("type".to_owned(), serde_json::Value::String("literal".to_owned())),
                ("value".to_owned(), serde_json::Value::String(lexical_form.clone())),
                ("datatype".to_owned(), serde_json::Value::String(datatype_iri.clone())),
            ]);
            if let Some(language) = language {
                object.insert("xml:lang".to_owned(), serde_json::Value::String(language.clone()));
            }
            serde_json::Value::Object(object)
        }
    };
    Ok(value)
}

#[allow(clippy::too_many_arguments)]
async fn execute_one_online_direct_bgp(
    state: &AppState,
    config: &OnlineDirectConfig,
    semantic: &SemanticState,
    compiled_query: &CompiledSparqlQuery,
    direct_dataset: &ResolvedDataset,
    legality: &ngkg_types::DirectBgpLegalityRecord,
    graph_binding_iri: Option<&str>,
    snapshot_root: &Path,
    query_sha256: &str,
) -> Result<ExactBgpExecution, OnlineError> {
    let scope_token = graph_binding_iri.unwrap_or("default");
    let scope_sha256 = hex::encode(Sha256::digest(scope_token.as_bytes()));
    let bundle_work = snapshot_root
        .join("requests")
        .join(query_sha256)
        .join(&legality.bgp_sha256)
        .join(scope_sha256);
    let manifest = Arc::clone(&semantic.manifest);
    let signature = Arc::clone(&semantic.owl_signature_document);
    let catalog = Arc::clone(&semantic.graph_catalog);
    let resolved = direct_dataset.clone();
    let query_dataset_path = snapshot_root.join("data/query-dataset.nq");
    let snapshot_root_owned = snapshot_root.to_owned();
    let graph_scope = legality.graph_scope.clone();
    let graph_binding = graph_binding_iri.map(ToOwned::to_owned);
    let bundle: DirectExactOntologyBundle = tokio::task::spawn_blocking(move || {
        build_direct_active_ontology_bundle(
            &snapshot_root_owned,
            &manifest,
            &signature,
            &catalog,
            &resolved,
            &query_dataset_path,
            &graph_scope,
            graph_binding.as_deref(),
            &bundle_work,
        )
    })
    .await?
    .map_err(|error| OnlineError::SnapshotConflict(error.to_string()))?;
    let bindings = DirectExactBindings {
        dataset_id: semantic.active.snapshot.dataset_id,
        snapshot_id: semantic.active.snapshot.snapshot_id,
        query_sha256: query_sha256.to_owned(),
        sparql_algebra_sha256: compiled_query.canonical_sse_sha256().to_owned(),
        active_dataset_sha256: direct_dataset.active_dataset_sha256.clone(),
        authorized_graph_set_sha256: direct_dataset.authorized_graph_set_sha256.clone(),
        owl_signature_sha256: semantic.owl_signature_sha256.clone(),
        datatype_policy_sha256: semantic.datatype_policy_sha256.clone(),
        owl_profile_qualification_sha256: semantic.owl_profile_qualification_sha256.clone(),
        owl_consistency_qualification_sha256: semantic.owl_consistency_qualification_sha256.clone(),
        graph_context: bundle.graph_context.clone(),
    };
    let prepared = prepare_exact_direct_bgp_requests(
        compiled_query,
        legality,
        &bindings,
        &bundle,
        &config.work_root,
        config.limits,
    )
    .map_err(|error| OnlineError::SnapshotConflict(error.to_string()))?;
    let plan = build_distributed_reasoner_plan(
        bindings.dataset_id,
        bindings.snapshot_id,
        bindings.query_sha256.clone(),
        legality.bgp_sha256.clone(),
        bundle.aggregate_input_sha256.clone(),
        config.limits.max_candidate_bindings,
        config.limits.max_partition_candidates,
        u32::try_from(config.limits.max_exact_partitions).map_err(|_| {
            OnlineError::SnapshotConflict("exact partition ceiling exceeds u32".to_owned())
        })?,
    )
    .map_err(|error| OnlineError::SnapshotConflict(error.to_string()))?;
    let requests = prepared.requests;
    let results = dispatch_exact_partitions_with_retry(
        &state.reasoner_http,
        &config.worker_base_urls,
        &config.bearer_token,
        requests.clone(),
        config.dispatch_concurrency,
        config.max_partition_response_bytes,
        config.dispatch_attempts,
    )
    .await
    .map_err(|error| OnlineError::Upstream(error.to_string()))?;
    let (result, certificate, proof_manifest) = complete_distributed_exact_bgp(
        &plan,
        &requests,
        results,
        &bindings,
        legality,
        &bundle,
        &config.adapter,
        &config.limits,
    )
    .map_err(|error| OnlineError::SnapshotConflict(error.to_string()))?;
    Ok(ExactBgpExecution {
        result,
        certificate,
        proof_manifest,
    })
}

fn combine_named_graph_results(
    legality: &ngkg_types::DirectBgpLegalityRecord,
    graph_variable: &str,
    scoped: &[(String, DirectBgpResult)],
    direct_dataset: &ResolvedDataset,
    semantic: &SemanticState,
    query_sha256: &str,
) -> Result<DirectBgpResult, OnlineError> {
    let mut variables = BTreeSet::from([graph_variable.to_owned()]);
    let mut solutions = Vec::new();
    let mut candidate_binding_count = 0_u64;
    let mut solution_multiplicity_total = 0_u64;
    let mut graph_hash = Sha256::new();
    graph_hash.update(b"ngkg-direct-named-graph-relation-v1\0");
    for (graph_iri, result) in scoped {
        graph_hash.update(graph_iri.as_bytes());
        graph_hash.update(
            direct_bgp_result_sha256(result)
                .map_err(|error| OnlineError::SnapshotConflict(error.to_string()))?
                .as_bytes(),
        );
        variables.extend(result.variables.iter().cloned());
        candidate_binding_count = candidate_binding_count
            .checked_add(result.candidate_binding_count)
            .ok_or_else(|| OnlineError::SnapshotConflict("candidate count overflow".to_owned()))?;
        solution_multiplicity_total = solution_multiplicity_total
            .checked_add(result.solution_multiplicity_total)
            .ok_or_else(|| {
                OnlineError::SnapshotConflict("solution multiplicity overflow".to_owned())
            })?;
        for solution in &result.solutions {
            let mut bindings = solution.bindings.clone();
            bindings.insert(
                graph_variable.to_owned(),
                DirectBgpRdfTerm::Iri {
                    value: graph_iri.clone(),
                },
            );
            solutions.push(DirectBgpSolution {
                bindings,
                multiplicity: solution.multiplicity,
            });
        }
    }
    Ok(DirectBgpResult {
        format_version: 1,
        dataset_id: semantic.active.snapshot.dataset_id,
        snapshot_id: semantic.active.snapshot.snapshot_id,
        query_sha256: query_sha256.to_owned(),
        bgp_sha256: legality.bgp_sha256.clone(),
        active_dataset_sha256: direct_dataset.active_dataset_sha256.clone(),
        authorized_graph_set_sha256: direct_dataset.authorized_graph_set_sha256.clone(),
        owl_signature_sha256: semantic.owl_signature_sha256.clone(),
        datatype_policy_sha256: semantic.datatype_policy_sha256.clone(),
        entailment_regime: EntailmentRegime::Owl2Direct,
        graph_context: DirectBgpGraphContext::Default {
            active_default_graph_sha256: hex::encode(graph_hash.finalize()),
        },
        variables: variables.into_iter().collect(),
        candidate_binding_count,
        solution_multiplicity_total,
        solutions,
        outcome: DirectBgpOutcome {
            status: DirectBgpStatus::Complete,
            exactness: DirectBgpExactness::Exact,
            completeness: DirectBgpCompleteness::Complete,
        },
        error: None,
    })
}

fn sha256_json<T: Serialize>(value: &T) -> Result<String, OnlineError> {
    Ok(hex::encode(Sha256::digest(serde_json::to_vec(value)?)))
}

#[allow(clippy::too_many_arguments)]
async fn execute_partition_native_path_set(
    state: &AppState,
    headers: &HeaderMap,
    semantic: &SemanticState,
    query_sha256: &str,
    compiled_query: &CompiledSparqlQuery,
    active_dataset: &ResolvedDataset,
) -> Result<Option<PropertyPathExecutionEvidence>, OnlineError> {
    if !state.partition_native_paths_enabled
        || compiled_query.execution_analysis().has_remote_service
    {
        return Ok(None);
    }
    let activation = match semantic.active.cloud_activation.as_ref() {
        Some(value) => value,
        None => return Ok(None),
    };
    let partition_count = u32::try_from(activation.semantic_partition_count).map_err(|_| {
        OnlineError::SnapshotConflict("semantic partition count is invalid".to_owned())
    })?;
    // A legal query must retain the scalar correctness lane when the active
    // snapshot is intentionally single-partitioned (for example a small dev
    // dataset). Distributed execution is an optimization, never a grammar gate.
    if partition_count < 2 {
        return Ok(None);
    }
    let plans = compiled_query
        .distributed_property_path_plans(DistributedPropertyPathLimits {
            partition_count,
            max_iterations: state.property_path_max_iterations,
            max_frontier_items: state.property_path_max_frontier_items,
            max_visited_items: state.property_path_max_visited_items,
            max_checkpoint_bytes: state.property_path_max_checkpoint_bytes,
            max_spill_bytes: state.property_path_max_spill_bytes,
            hot_vertex_degree: state.property_path_hot_vertex_degree,
            max_hot_vertex_splits: state.property_path_max_hot_vertex_splits,
        })
        .map_err(|error| OnlineError::Request(error.to_string()))?;
    if plans.is_empty() {
        return Ok(None);
    }
    let plan_set_sha256 = sha256_json(&plans)?;
    let service = state.fragment_service.as_deref().ok_or_else(|| {
        OnlineError::Upstream("fragment worker service is not configured".to_owned())
    })?;
    let mut workers = tokio::net::lookup_host(service)
        .await
        .map_err(|error| OnlineError::Upstream(format!("fragment DNS failed: {error}")))?
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    workers.sort_unstable();
    if workers.len() < 2 {
        return Err(OnlineError::Upstream(
            "partition-native paths require at least two ready fragment workers".to_owned(),
        ));
    }
    let token = bearer(headers)?.to_owned();
    let checkpoint_base = state
        .shuffle_spill_root
        .as_ref()
        .ok_or_else(|| {
            OnlineError::SnapshotConflict(
                "query role has no property-path checkpoint volume".to_owned(),
            )
        })?
        .join("property-path-checkpoints")
        .join(semantic.active.snapshot.tenant_id.to_string())
        .join(semantic.active.snapshot.dataset_id.to_string())
        .join(semantic.active.snapshot.snapshot_id.to_string())
        .join(query_sha256);
    let mut path_metrics = PropertyPathMetricLease::new(Arc::clone(&state.admission));
    let mut completed_iterations = 0_u64;
    let mut completed_work_items = 0_u64;
    let mut accepted_endpoint_count = 0_u64;
    let mut endpoint_set_sha256s = Vec::new();
    let mut scanned_adjacency_rows = 0_u64;
    let mut hot_split_work_items = 0_u64;
    let mut checkpoint_bytes = 0_u64;
    let mut worker_ids = BTreeSet::new();
    for plan in &plans {
        let plan_sha256 = sha256_json(plan)?;
        path_metrics.set_pending(u64::from(partition_count));
        let seed_responses = dispatch_partition_path_wave(
            state,
            &workers,
            &token,
            semantic,
            query_sha256,
            &plan_sha256,
            plan,
            active_dataset,
            0,
            PartitionPathAction::Seed,
            Vec::new(),
        )
        .await?;
        path_metrics.set_pending(0);
        let mut frontier = BTreeSet::new();
        for response in seed_responses {
            validate_partition_path_response(
                &response,
                semantic,
                activation,
                query_sha256,
                &plan_sha256,
                plan,
                0,
                &[],
            )?;
            worker_ids.insert(response.worker_id);
            scanned_adjacency_rows = scanned_adjacency_rows
                .checked_add(response.batch.adjacency_rows_read)
                .ok_or_else(|| OnlineError::Request("path scan evidence overflow".to_owned()))?;
            frontier.extend(response.batch.seed_frontier);
        }
        if u64::try_from(frontier.len())
            .ok()
            .is_none_or(|count| count > plan.max_frontier_items)
        {
            return Err(OnlineError::Request(
                "partition-native path seed exceeds its frontier ceiling".to_owned(),
            ));
        }
        let mut visited = frontier.clone();
        let mut endpoints = BTreeSet::<PathEndpoint>::new();
        let mut frontier = frontier.into_iter().collect::<Vec<_>>();
        path_metrics.set_frontier(u64::try_from(frontier.len()).map_err(|_| {
            OnlineError::Request("property-path frontier metric overflow".to_owned())
        })?);
        let mut iteration = 0_u32;
        while !frontier.is_empty() {
            if iteration >= plan.max_iterations {
                return Err(OnlineError::GatewayTimeout(
                    "property-path iteration ceiling reached before termination".to_owned(),
                ));
            }
            path_metrics.set_pending(u64::from(partition_count));
            let responses = dispatch_partition_path_wave(
                state,
                &workers,
                &token,
                semantic,
                query_sha256,
                &plan_sha256,
                plan,
                active_dataset,
                iteration,
                PartitionPathAction::Expand,
                frontier.clone(),
            )
            .await?;
            path_metrics.set_pending(0);
            let mut expected = Vec::new();
            let mut results = Vec::new();
            for response in responses {
                validate_partition_path_response(
                    &response,
                    semantic,
                    activation,
                    query_sha256,
                    &plan_sha256,
                    plan,
                    iteration,
                    &frontier,
                )?;
                worker_ids.insert(response.worker_id);
                scanned_adjacency_rows = scanned_adjacency_rows
                    .checked_add(response.batch.adjacency_rows_read)
                    .ok_or_else(|| {
                        OnlineError::Request("path scan evidence overflow".to_owned())
                    })?;
                hot_split_work_items = hot_split_work_items
                    .checked_add(response.batch.hot_split_count)
                    .ok_or_else(|| {
                        OnlineError::Request("hot-split evidence overflow".to_owned())
                    })?;
                expected.extend(response.batch.work);
                results.extend(response.batch.results);
            }
            completed_work_items = completed_work_items
                .checked_add(u64::try_from(expected.len()).map_err(|_| {
                    OnlineError::Request("path work-item count overflow".to_owned())
                })?)
                .ok_or_else(|| OnlineError::Request("path work-item count overflow".to_owned()))?;
            let outcome = complete_path_iteration(
                &expected,
                results,
                &visited,
                &endpoints,
                plan.max_frontier_items,
                plan.max_visited_items,
                plan.max_checkpoint_bytes,
            )
            .map_err(distributed_execution_error)?;
            let checkpoint_root = checkpoint_base.join(&plan.path_id);
            let checkpoint = outcome.checkpoint.clone();
            let maximum = plan.max_checkpoint_bytes;
            let checkpoint_path = tokio::task::spawn_blocking(move || {
                write_checkpoint_atomic(&checkpoint_root, &checkpoint, maximum)
            })
            .await?
            .map_err(partition_path_error)?;
            let checkpoint_sha256 = sha256_path_off_thread(checkpoint_path.clone()).await?;
            state
                .manager
                .store
                .put_file_immutable(
                    &format!(
                        "query-checkpoints/{}/{}/{}/{}/{}/{}",
                        semantic.active.snapshot.tenant_id,
                        semantic.active.snapshot.dataset_id,
                        semantic.active.snapshot.snapshot_id,
                        query_sha256,
                        plan.path_id,
                        checkpoint_path
                            .file_name()
                            .and_then(|value| value.to_str())
                            .ok_or_else(|| {
                                OnlineError::SnapshotConflict(
                                    "checkpoint path is not UTF-8".to_owned(),
                                )
                            })?
                    ),
                    &checkpoint_sha256,
                    &checkpoint_path,
                    plan.max_checkpoint_bytes,
                    usize::try_from(plan.max_checkpoint_bytes.min(8 * 1024 * 1024)).map_err(
                        |_| OnlineError::Request("checkpoint buffer ceiling overflow".to_owned()),
                    )?,
                    1,
                )
                .await?;
            let persisted_checkpoint_bytes = fs::metadata(&checkpoint_path)?.len();
            checkpoint_bytes = checkpoint_bytes
                .checked_add(persisted_checkpoint_bytes)
                .filter(|bytes| *bytes <= plan.max_spill_bytes)
                .ok_or_else(|| {
                    OnlineError::Request(
                        "property-path checkpoint spill exceeded its admitted byte ceiling"
                            .to_owned(),
                    )
                })?;
            path_metrics.add_checkpoint(persisted_checkpoint_bytes)?;
            completed_iterations = completed_iterations.checked_add(1).ok_or_else(|| {
                OnlineError::Request("path iteration evidence overflow".to_owned())
            })?;
            visited = outcome.visited;
            endpoints = outcome.endpoints;
            frontier = outcome.next_frontier;
            path_metrics.set_frontier(u64::try_from(frontier.len()).map_err(|_| {
                OnlineError::Request("property-path frontier metric overflow".to_owned())
            })?);
            if outcome.terminated {
                break;
            }
            iteration = iteration.checked_add(1).ok_or_else(|| {
                OnlineError::Request("property-path iteration overflow".to_owned())
            })?;
        }
        accepted_endpoint_count = accepted_endpoint_count
            .checked_add(u64::try_from(endpoints.len()).map_err(|_| {
                OnlineError::Request("property-path endpoint count overflow".to_owned())
            })?)
            .ok_or_else(|| {
                OnlineError::Request("property-path endpoint count overflow".to_owned())
            })?;
        endpoint_set_sha256s.push(sha256_json(&endpoints)?);
        path_metrics.set_frontier(0);
    }
    Ok(Some(PropertyPathExecutionEvidence {
        mode: "partition_native_distributed_frontier_v1".to_owned(),
        plan_set_sha256,
        path_count: u64::try_from(plans.len())
            .map_err(|_| OnlineError::Request("path count overflow".to_owned()))?,
        semantic_partition_count: partition_count,
        completed_iterations,
        completed_work_items,
        accepted_endpoint_count,
        endpoint_set_sha256s,
        scanned_adjacency_rows,
        hot_split_work_items,
        checkpoint_bytes,
        worker_ids: worker_ids.into_iter().collect(),
        scalar_oracle_equivalence_required: true,
        complete: true,
    }))
}

#[allow(clippy::too_many_arguments)]
async fn dispatch_partition_path_wave(
    state: &AppState,
    workers: &[SocketAddr],
    token: &str,
    semantic: &SemanticState,
    query_sha256: &str,
    plan_sha256: &str,
    plan: &DistributedPropertyPathPlan,
    active_dataset: &ResolvedDataset,
    iteration: u32,
    action: PartitionPathAction,
    frontier: Vec<PathFrontierKey>,
) -> Result<Vec<PartitionPathExecutionResponse>, OnlineError> {
    let activation =
        semantic.active.cloud_activation.as_ref().ok_or_else(|| {
            OnlineError::SnapshotConflict("cloud activation disappeared".to_owned())
        })?;
    let requests = (0..plan.partition_count)
        .map(|partition| {
            let worker = workers
                .get(usize::try_from(partition).unwrap_or(usize::MAX) % workers.len())
                .ok_or_else(|| {
                    OnlineError::Upstream("path worker assignment is empty".to_owned())
                })?;
            let request = PartitionPathExecutionRequest {
                snapshot_id: semantic.active.snapshot.snapshot_id,
                manifest_sha256: semantic.active.snapshot.manifest_sha256.clone(),
                semantic_root_sha256: activation.semantic_root_sha256.clone(),
                active_dataset: active_dataset.clone(),
                plan_sha256: plan_sha256.to_owned(),
                plan: plan.clone(),
                iteration,
                storage_partition: partition,
                action,
                frontier: frontier.clone(),
            };
            let client = state.fragment_http.clone();
            let token = token.to_owned();
            let url = format!(
                "http://{worker}/v1/datasets/{}/paths/{}/{}/{iteration}/{partition}/expand",
                semantic.active.snapshot.dataset_id, query_sha256, plan.path_id
            );
            let max_bytes = state.max_fragment_response_bytes;
            Ok::<_, OnlineError>(async move {
                let response = client
                    .post(url)
                    .bearer_auth(token)
                    .json(&request)
                    .send()
                    .await
                    .map_err(|error| {
                        upstream_transport_error("property-path worker request", error)
                    })?;
                if !response.status().is_success() {
                    return Err(upstream_status_error(
                        "property-path worker",
                        response.status(),
                    ));
                }
                let bytes = read_bounded_response(response, max_bytes).await?;
                serde_json::from_slice::<PartitionPathExecutionResponse>(&bytes)
                    .map_err(OnlineError::Json)
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let mut responses = stream::iter(requests)
        .buffer_unordered(state.fragment_exchange_concurrency)
        .try_collect::<Vec<_>>()
        .await?;
    responses.sort_by_key(|response| response.storage_partition);
    if responses.len() != usize::try_from(plan.partition_count).unwrap_or(usize::MAX)
        || responses.iter().enumerate().any(|(expected, response)| {
            usize::try_from(response.storage_partition).ok() != Some(expected)
        })
    {
        return Err(OnlineError::SnapshotConflict(
            "property-path partition barrier is incomplete or duplicated".to_owned(),
        ));
    }
    Ok(responses)
}

#[allow(clippy::too_many_arguments)]
fn validate_partition_path_response(
    response: &PartitionPathExecutionResponse,
    semantic: &SemanticState,
    activation: &ngkg_catalog::CloudSnapshotActivation,
    query_sha256: &str,
    plan_sha256: &str,
    plan: &DistributedPropertyPathPlan,
    iteration: u32,
    expected_frontier: &[PathFrontierKey],
) -> Result<(), OnlineError> {
    if !response.complete
        || !response.batch.complete
        || response.dataset_id != semantic.active.snapshot.dataset_id
        || response.snapshot_id != semantic.active.snapshot.snapshot_id
        || response.semantic_root_sha256 != activation.semantic_root_sha256
        || response.plan_sha256 != plan_sha256
        || response.iteration != iteration
        || response.storage_partition != response.batch.storage_partition
        || response.storage_partition >= plan.partition_count
        || response.worker_id.is_empty()
        || !is_sha256(&response.partition_manifest_sha256)
        || !is_sha256(&response.forward_adjacency_sha256)
        || !is_sha256(&response.reverse_adjacency_sha256)
        || !is_sha256(&response.dictionary_sha256)
        || response.response_sha256 != sha256_json(&response.batch)?
        || response.batch.work.iter().any(|work| {
            work.identity.query_sha256 != query_sha256
                || work.identity.plan_sha256 != plan_sha256
                || work.identity.path_id != plan.path_id
                || work.identity.iteration != iteration
                || work.identity.storage_partition != response.storage_partition
        })
    {
        return Err(OnlineError::SnapshotConflict(
            "property-path worker returned foreign, partial, or corrupt evidence".to_owned(),
        ));
    }
    if expected_frontier.is_empty() {
        if !response.batch.work.is_empty() || !response.batch.results.is_empty() {
            return Err(OnlineError::SnapshotConflict(
                "property-path seed response contains expansion work".to_owned(),
            ));
        }
        return Ok(());
    }
    let expected_frontier = expected_frontier.iter().copied().collect::<BTreeSet<_>>();
    let mut splits = BTreeMap::<PathFrontierKey, (u32, BTreeSet<u32>)>::new();
    for work in &response.batch.work {
        let entry = splits
            .entry(work.identity.frontier)
            .or_insert_with(|| (work.identity.split_count, BTreeSet::new()));
        if entry.0 != work.identity.split_count || !entry.1.insert(work.identity.split_index) {
            return Err(OnlineError::SnapshotConflict(
                "property-path worker returned a duplicate or inconsistent hot split".to_owned(),
            ));
        }
    }
    if splits.keys().copied().collect::<BTreeSet<_>>() != expected_frontier
        || splits.values().any(|(count, indices)| {
            indices.len() != usize::try_from(*count).unwrap_or(usize::MAX)
                || indices.iter().copied().ne(0..*count)
        })
    {
        return Err(OnlineError::SnapshotConflict(
            "property-path worker omitted a frontier or hot-vertex split".to_owned(),
        ));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn execute_distributed_scalar_oracle(
    state: &AppState,
    headers: &HeaderMap,
    semantic: &SemanticState,
    query_text: &str,
    compiled_query: &CompiledSparqlQuery,
    rewritten_query: &spargebra::Query,
    active_dataset: &ResolvedDataset,
    limits: CertifiedQueryExecutionLimits,
) -> Result<(CertifiedSemanticResult, ExecutionResponse), OnlineError> {
    let service = state.fragment_service.as_deref().ok_or_else(|| {
        OnlineError::Upstream("fragment worker service is not configured".to_owned())
    })?;
    let mut workers = tokio::net::lookup_host(service)
        .await
        .map_err(|error| OnlineError::Upstream(format!("fragment DNS failed: {error}")))?
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    workers.sort_unstable();
    let replica_count = state.distributed_algebra_replicas;
    if workers.len() < replica_count || replica_count < 2 {
        return Err(OnlineError::Upstream(
            "distributed algebra requires its complete distinct-worker replica set".to_owned(),
        ));
    }
    let original_query_sha256 = hex::encode(Sha256::digest(query_text.as_bytes()));
    let rewritten_query = rewritten_query.to_string();
    if rewritten_query.len() > state.max_request_bytes {
        return Err(OnlineError::Request(
            "rewritten distributed algebra exceeds the internal request ceiling".to_owned(),
        ));
    }
    let rewritten_query_sha256 = hex::encode(Sha256::digest(rewritten_query.as_bytes()));
    let replica_count_u32 = u32::try_from(replica_count).map_err(|_| {
        OnlineError::Request("distributed algebra replica count exceeds this platform".to_owned())
    })?;
    let token = bearer(headers)?.to_owned();
    let requests = workers
        .into_iter()
        .take(replica_count)
        .enumerate()
        .map(|(replica, worker)| {
            let replica = u32::try_from(replica).map_err(|_| {
                OnlineError::Request("distributed algebra replica ordinal overflow".to_owned())
            })?;
            let request = DistributedAlgebraExecutionRequest {
                snapshot_id: semantic.active.snapshot.snapshot_id,
                manifest_sha256: semantic.active.snapshot.manifest_sha256.clone(),
                original_query: query_text.to_owned(),
                original_query_sha256: original_query_sha256.clone(),
                rewritten_query: rewritten_query.clone(),
                rewritten_query_sha256: rewritten_query_sha256.clone(),
                active_dataset: active_dataset.clone(),
                max_solution_rows: limits.max_solution_rows,
                max_graph_triples: limits.max_graph_triples,
                max_graph_blank_nodes: limits.max_graph_blank_nodes,
                ordered: compiled_query.solution_order_is_significant(),
                replica,
                replica_count: replica_count_u32,
            };
            let client = state.fragment_http.clone();
            let token = token.clone();
            let url = format!(
                "http://{worker}/v1/datasets/{}/algebra/{}/{replica}/execute",
                semantic.active.snapshot.dataset_id, original_query_sha256
            );
            let max_bytes = state.max_query_response_bytes;
            Ok::<_, OnlineError>(async move {
                let response = client
                    .post(url)
                    .bearer_auth(token)
                    .json(&request)
                    .send()
                    .await
                    .map_err(|error| {
                        upstream_transport_error("distributed algebra worker request", error)
                    })?;
                if !response.status().is_success() {
                    return Err(upstream_status_error(
                        "distributed algebra worker",
                        response.status(),
                    ));
                }
                let bytes = read_bounded_response(response, max_bytes).await?;
                serde_json::from_slice::<DistributedAlgebraExecutionResponse>(&bytes)
                    .map_err(OnlineError::Json)
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let mut responses = stream::iter(requests)
        .buffer_unordered(replica_count)
        .try_collect::<Vec<_>>()
        .await?;
    responses.sort_by_key(|response| response.replica);
    let first = responses.first().cloned().ok_or_else(|| {
        OnlineError::Upstream("distributed algebra returned no replicas".to_owned())
    })?;
    let mut worker_ids = BTreeSet::new();
    for (replica, response) in responses.iter().enumerate() {
        let replica = u32::try_from(replica).map_err(|_| {
            OnlineError::SnapshotConflict("distributed algebra replica overflow".to_owned())
        })?;
        if !response.complete
            || response.dataset_id != semantic.active.snapshot.dataset_id
            || response.snapshot_id != semantic.active.snapshot.snapshot_id
            || response.manifest_sha256 != semantic.active.snapshot.manifest_sha256
            || response.original_query_sha256 != original_query_sha256
            || response.rewritten_query_sha256 != rewritten_query_sha256
            || response.replica != replica
            || response.replica_count != replica_count_u32
            || response.worker_id.is_empty()
            || response.result_sha256 != first.result_sha256
            || response.result.query_form != first.result.query_form
            || response.result.coverage_scope != first.result.coverage_scope
            || (compiled_query.solution_order_is_significant() && response.result != first.result)
        {
            return Err(OnlineError::SnapshotConflict(
                "distributed algebra replica set is incomplete or scalar-unequal".to_owned(),
            ));
        }
        if !worker_ids.insert(response.worker_id.clone()) {
            return Err(OnlineError::Upstream(
                "distributed algebra replicas did not execute on distinct workers".to_owned(),
            ));
        }
    }
    let result = CertifiedSemanticResult {
        dataset_id: first.dataset_id,
        snapshot_id: first.snapshot_id,
        query_sha256: first.original_query_sha256,
        query_form: first.result.query_form,
        head: first.result.head,
        bindings: first.result.bindings,
        boolean_result: first.result.boolean_result,
        graph_ntriples: first.result.graph_ntriples,
        qualified_entity_iris: first.result.qualified_entity_iris,
        coverage_scope: first.result.coverage_scope,
    };
    Ok((
        result,
        ExecutionResponse {
            mode: "distributed_scalar_oracle_equivalence_v1".to_owned(),
            exchange_format: "bounded_json_result_v1".to_owned(),
            fragment_ingress_mode: "none".to_owned(),
            fragment_ingress_bytes: 0,
            fragment_materialization_mode: "snapshot_local_scalar_oracle_v1".to_owned(),
            fragment_owned_rows: 0,
            shuffle_result_ingress_mode: "none".to_owned(),
            shuffle_result_ingress_bytes: 0,
            intermediate_result_mode: "complete_replica_barrier_v1".to_owned(),
            assembled_intermediate_owned_rows: 0,
            fragment_count: replica_count_u32,
            worker_count: replica_count_u32,
            shuffle_partition_count: 0,
            shuffle_worker_count: 0,
            shuffle_spill_mode: "bounded_worker_local_v1".to_owned(),
            shuffle_spill_bytes: 0,
            shuffle_cache_mode: "none".to_owned(),
            shuffle_cache_hits: 0,
            worker_join_mode: "typed_scalar_oracle_algebra_v1".to_owned(),
            worker_join_spill_bytes: 0,
            worker_join_grace_partitions: 0,
            worker_join_max_build_rows: u64::try_from(state.max_worker_join_build_rows)
                .map_err(|_| OnlineError::Request("worker row ceiling overflow".to_owned()))?,
            worker_input_mode: "checksum_bound_rewritten_sparql_v1".to_owned(),
            worker_input_bytes: u64::try_from(rewritten_query.len()).map_err(|_| {
                OnlineError::Request("rewritten query byte count overflow".to_owned())
            })?,
            coordinator_request_mode: "bounded_concurrent_distinct_workers_v1".to_owned(),
            coordinator_request_bytes: u64::try_from(query_text.len())
                .map_err(|_| OnlineError::Request("query byte count overflow".to_owned()))?,
            plan_sha256: Some(compiled_query.canonical_sse_sha256().to_owned()),
        },
    ))
}

#[allow(clippy::too_many_arguments)]
async fn execute_uncertified_exact_query(
    state: AppState,
    headers: HeaderMap,
    tenant_id: Uuid,
    dataset_id: Uuid,
    query_text: String,
    compiled_query: CompiledSparqlQuery,
    semantic: Arc<SemanticState>,
    active_dataset: ResolvedDataset,
    authorized_graphs: AuthorizedQueryGraphs,
    hydrate: bool,
    deployment_limits: CertifiedQueryExecutionLimits,
    rewritten_query: Option<spargebra::Query>,
    entailment: Option<ExactEntailmentEvidence>,
    native_precomputed: Option<(CertifiedSemanticResult, ExecutionResponse)>,
) -> Result<Response, OnlineError> {
    let query_sha256 = hex::encode(Sha256::digest(query_text.as_bytes()));
    let is_exact_entailment = entailment.is_some();
    let federation_handler = if compiled_query.execution_analysis().has_remote_service {
        Some(
            state
                .federation
                .as_ref()
                .ok_or_else(|| {
                    OnlineError::Request(
                        "SPARQL SERVICE requires a checksum-bound federation endpoint registry"
                            .to_owned(),
                    )
                })?
                .query_handler(&tenant_id.to_string()),
        )
    } else {
        None
    };
    let federation_audit = federation_handler.clone();
    let rewritten_for_execution = rewritten_query
        .clone()
        .unwrap_or_else(|| compiled_query.query_clone());
    let property_path_execution = execute_partition_native_path_set(
        &state,
        &headers,
        &semantic,
        &query_sha256,
        &compiled_query,
        &active_dataset,
    )
    .await?;
    // Volatile functions have one query-scoped evaluation context. Running RAND/NOW/UUID/BNODE
    // independently on multiple replicas would manufacture a false mismatch (and NOW could lose
    // query scope), so those legal queries retain the bounded uncached scalar lane.
    let replica_safe = compiled_query.execution_analysis().is_snapshot_cacheable()
        && !compiled_query.execution_analysis().has_remote_service;
    let (result, execution) = if let Some(native) = native_precomputed {
        native
    } else if state.distributed_algebra_enabled && replica_safe {
        execute_distributed_scalar_oracle(
            &state,
            &headers,
            &semantic,
            &query_text,
            &compiled_query,
            &rewritten_for_execution,
            &active_dataset,
            deployment_limits,
        )
        .await?
    } else {
        let runtime = state
            .manager
            .clone()
            .full_runtime(Arc::clone(&semantic))
            .await?;
        let active_dataset_for_runtime = active_dataset.clone();
        let graph_catalog_for_runtime = Arc::clone(&semantic.graph_catalog);
        let compiled_for_runtime = compiled_query.clone();
        let query_for_runtime = query_text.clone();
        let cancellation = CancellationToken::new();
        let cancellation_for_runtime = cancellation.clone();
        let has_rewritten_query = rewritten_query.is_some();
        let mut execution_task =
            tokio::task::spawn_blocking(move || match (has_rewritten_query, federation_handler) {
                (true, Some(handler)) => runtime
                    .execute_exact_entailment_rewritten_federated_with_dataset_bounded_cancellable(
                        &query_for_runtime,
                        &compiled_for_runtime,
                        rewritten_for_execution,
                        &active_dataset_for_runtime,
                        &graph_catalog_for_runtime,
                        deployment_limits,
                        Some(cancellation_for_runtime),
                        handler,
                    ),
                (true, None) => runtime
                    .execute_exact_entailment_rewritten_with_dataset_bounded_cancellable(
                        &query_for_runtime,
                        &compiled_for_runtime,
                        rewritten_for_execution,
                        &active_dataset_for_runtime,
                        &graph_catalog_for_runtime,
                        deployment_limits,
                        Some(cancellation_for_runtime),
                    ),
                (false, Some(handler)) => runtime
                    .execute_uncertified_federated_compiled_with_dataset_bounded_cancellable(
                        &query_for_runtime,
                        &compiled_for_runtime,
                        &active_dataset_for_runtime,
                        &graph_catalog_for_runtime,
                        deployment_limits,
                        Some(cancellation_for_runtime),
                        handler,
                    ),
                (false, None) => runtime
                    .execute_uncertified_compiled_with_dataset_bounded_cancellable(
                        &query_for_runtime,
                        &compiled_for_runtime,
                        &active_dataset_for_runtime,
                        &graph_catalog_for_runtime,
                        deployment_limits,
                        Some(cancellation_for_runtime),
                    ),
            });
        let result = match tokio::time::timeout(state.query_timeout, &mut execution_task).await {
            Ok(joined) => joined??,
            Err(_) => {
                cancellation.cancel();
                return Err(OnlineError::GatewayTimeout(
                    "exact ad-hoc SPARQL evaluation exceeded the configured query timeout"
                        .to_owned(),
                ));
            }
        };
        let execution = ExecutionResponse {
            mode: if is_exact_entailment {
                "exact_distributed_owl2_direct_then_scalar_algebra".to_owned()
            } else {
                "exact_scalar_ad_hoc".to_owned()
            },
            exchange_format: "none".to_owned(),
            fragment_ingress_mode: "none".to_owned(),
            fragment_ingress_bytes: 0,
            fragment_materialization_mode: "none".to_owned(),
            fragment_owned_rows: 0,
            shuffle_result_ingress_mode: "none".to_owned(),
            shuffle_result_ingress_bytes: 0,
            intermediate_result_mode: "none".to_owned(),
            assembled_intermediate_owned_rows: 0,
            fragment_count: 0,
            worker_count: 0,
            shuffle_partition_count: 0,
            shuffle_worker_count: 0,
            shuffle_spill_mode: "none".to_owned(),
            shuffle_spill_bytes: 0,
            shuffle_cache_mode: "none".to_owned(),
            shuffle_cache_hits: 0,
            worker_join_mode: "none".to_owned(),
            worker_join_spill_bytes: 0,
            worker_join_grace_partitions: 0,
            worker_join_max_build_rows: 0,
            worker_input_mode: "none".to_owned(),
            worker_input_bytes: 0,
            coordinator_request_mode: "none".to_owned(),
            coordinator_request_bytes: 0,
            plan_sha256: None,
        };
        (result, execution)
    };
    let federation = federation_audit
        .as_ref()
        .map(|handler| handler.evidence())
        .transpose()
        .map_err(|error| OnlineError::Upstream(error.to_string()))?;
    if result.query_sha256 != query_sha256 {
        return Err(OnlineError::SnapshotConflict(
            "exact ad-hoc runtime returned a different query identity".to_owned(),
        ));
    }
    if result.qualified_entity_iris.len() > state.max_qualified_entities {
        return Err(OnlineError::Request(
            "query qualified more entities than the online ceiling".to_owned(),
        ));
    }
    let qualified_entities = qualify_entities(&result, semantic.active.identity_namespace)?;
    let hydrated_payload = if hydrate && !qualified_entities.is_empty() {
        let token = bearer(&headers)?;
        let hydration_request = HydrationRequest {
            snapshot_id: semantic.active.snapshot.snapshot_id,
            serving_root_sha256: required_serving_root(&semantic.active)?
                .serving_root_sha256
                .clone(),
            entities: qualified_entities.clone(),
        };
        let base = state
            .hydration_url
            .as_deref()
            .ok_or_else(|| OnlineError::Upstream("hydration URL is absent".to_owned()))?;
        let response = state
            .http
            .post(format!(
                "{}/v1/datasets/{dataset_id}/hydrate",
                base.trim_end_matches('/')
            ))
            .bearer_auth(token)
            .json(&hydration_request)
            .send()
            .await
            .map_err(|error| upstream_transport_error("hydration request", error))?;
        if !response.status().is_success() {
            return Err(upstream_status_error("hydration", response.status()));
        }
        let bytes = read_bounded_response(response, state.max_hydration_response_bytes).await?;
        let hydrated = serde_json::from_slice::<HydrationResponse>(&bytes)?;
        if hydrated.dataset_id != dataset_id
            || hydrated.snapshot_id != semantic.active.snapshot.snapshot_id
            || hydrated.serving_root_sha256
                != required_serving_root(&semantic.active)?.serving_root_sha256
        {
            return Err(OnlineError::Upstream(
                "hydration response identity differs from the semantic request".to_owned(),
            ));
        }
        validate_hydrated_rows(
            &hydrated.rows,
            &qualified_entities,
            state.max_hydration_rows,
            &authorized_graphs.graph_iris,
        )?;
        hydrated.rows
    } else {
        Vec::new()
    };
    let default_graph_iris =
        graph_iris_for_ids(&semantic.graph_catalog, &active_dataset.default_graph_ids)?;
    let named_graph_iris =
        graph_iris_for_ids(&semantic.graph_catalog, &active_dataset.named_graph_ids)?;
    let mut selected_graph_iris = default_graph_iris
        .iter()
        .chain(named_graph_iris.iter())
        .cloned()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    selected_graph_iris.sort_unstable();
    let include_internal_closure = !is_exact_entailment
        && matches!(
            active_dataset.selection_source,
            DatasetSelectionSource::ServiceDefault
        );
    let response = QueryResponse {
        dataset_id,
        snapshot_id: result.snapshot_id,
        serving_root_sha256: semantic_serving_identity(&semantic.active)?,
        query_sha256: result.query_sha256,
        query_form: result.query_form,
        authorized_graph_set_sha256: authorized_graphs.graph_set_sha256.clone(),
        active_dataset_sha256: active_dataset.active_dataset_sha256.clone(),
        coverage_scope: result.coverage_scope,
        complete: true,
        routing: RoutingResponse {
            selection_mode: "full_exact_active_dataset_v1".to_owned(),
            dataset_selection_source: active_dataset.selection_source,
            default_graph_iris,
            named_graph_iris,
            active_dataset_sha256: active_dataset.active_dataset_sha256.clone(),
            include_internal_closure,
            selected_graph_count: u32::try_from(selected_graph_iris.len()).map_err(|_| {
                OnlineError::SnapshotConflict("selected graph count overflow".to_owned())
            })?,
            selected_graph_iris,
            total_graph_count: u32::try_from(authorized_graphs.graph_iris.len()).map_err(|_| {
                OnlineError::SnapshotConflict("authorized graph count overflow".to_owned())
            })?,
            capability_index_sha256: semantic.capability_index_sha256.clone(),
            routed_dataset_sha256: semantic.query_dataset_sha256.clone(),
        },
        execution,
        head: result.head,
        bindings: result.bindings,
        boolean_result: result.boolean_result,
        graph_ntriples: result.graph_ntriples,
        qualified_entities,
        hydrated_payload,
        entailment,
        property_path_execution,
        federation,
    };
    let mut body = BoundedBuffer::new(state.max_query_response_bytes);
    serde_json::to_writer(&mut body, &response).map_err(|_| {
        OnlineError::Request("serialized query response exceeds its byte ceiling".to_owned())
    })?;
    Ok(query_json_response(Bytes::from(body.into_bytes()), false))
}

/// Enforce the Phase 5 public-runtime boundary before a query can materialize a local scalar
/// store, invoke an oracle replica, populate a result cache, or consume distributed workers.
/// Shadow mode leaves existing service behavior intact for differential qualification. Required
/// mode accepts only algebra plans composed of native kernels and explicitly covered exact-BGP
/// stages. Any scalar-oracle stage or missing executable route fails closed with 503.
fn require_native_cutover_admission(
    state: &AppState,
    compiled: &CompiledSparqlQuery,
    certificate: Option<&ngkg_reference::CertifiedQueryRecord>,
) -> Result<(), OnlineError> {
    if !state.native_cutover_mode.requires_native() {
        return Ok(());
    }
    if compiled.execution_analysis().has_remote_service {
        return Err(OnlineError::NativeCutoverUnavailable(
            "federated SERVICE remains outside the immutable native snapshot boundary".to_owned(),
        ));
    }
    let plan = compiled
        .distributed_algebra_plan(DistributedAlgebraLimits {
            partition_count: state.shuffle_partition_count,
            max_input_rows: u64::try_from(state.max_distributed_intermediate_rows).map_err(
                |_| OnlineError::NativeCutoverUnavailable("native input ceiling overflow".to_owned()),
            )?,
            max_output_rows: u64::try_from(state.max_distributed_intermediate_rows).map_err(
                |_| OnlineError::NativeCutoverUnavailable("native output ceiling overflow".to_owned()),
            )?,
            max_exchange_bytes: u64::try_from(state.max_distributed_exchange_bytes).map_err(
                |_| OnlineError::NativeCutoverUnavailable("native exchange ceiling overflow".to_owned()),
            )?,
            max_spill_bytes: state.max_shuffle_spill_bytes,
        })
        .map_err(|error| OnlineError::NativeCutoverUnavailable(error.to_string()))?;
    if let Some(stage) = plan
        .stages
        .iter()
        .find(|stage| stage.lane == AlgebraExecutionLane::ScalarOraclePartitioned)
    {
        return Err(OnlineError::NativeCutoverUnavailable(format!(
            "algebra stage {} has no production-native kernel",
            stage.stage_id
        )));
    }
    if let Some(stage) = plan.stages.iter().find(|stage| {
        stage.lane == AlgebraExecutionLane::NativePartitioned
            && !matches!(
                stage.operator,
                DistributedAlgebraOperator::Join
                    | DistributedAlgebraOperator::Union
                    | DistributedAlgebraOperator::Minus
                    | DistributedAlgebraOperator::Project
                    | DistributedAlgebraOperator::Distinct
                    | DistributedAlgebraOperator::Reduced
                    | DistributedAlgebraOperator::Slice
            )
    }) {
        return Err(OnlineError::NativeCutoverUnavailable(format!(
            "native operator {:?} is not enabled on the public cutover path",
            stage.operator
        )));
    }
    let has_exact_stage = plan
        .stages
        .iter()
        .any(|stage| stage.lane == AlgebraExecutionLane::ExactReasonerPartitioned);
    if has_exact_stage && state.online_direct.is_none() {
        return Err(OnlineError::NativeCutoverUnavailable(
            "an uncovered OWL BGP requires the exact reasoner worker pool".to_owned(),
        ));
    }
    let has_certified_distributed_route = certificate
        .and_then(|record| record.routing.as_ref())
        .and_then(|routing| routing.distributed.as_ref())
        .is_some();
    if !has_exact_stage && !has_certified_distributed_route {
        return Err(OnlineError::NativeCutoverUnavailable(
            "the active snapshot has no executable distributed plan for this query hash"
                .to_owned(),
        ));
    }
    Ok(())
}

async fn execute_distributed_query(
    state: AppState,
    semantic: Arc<SemanticState>,
    routing: QueryRoutingCertificate,
    query_sha256: String,
    token: String,
) -> Result<(CertifiedSemanticResult, ExecutionResponse), OnlineError> {
    let distributed = routing.distributed.clone().ok_or_else(|| {
        OnlineError::SnapshotConflict("distributed query certificate is absent".to_owned())
    })?;
    let plan = state
        .manager
        .clone()
        .distributed_plan(Arc::clone(&semantic), query_sha256.clone())
        .await?;
    if plan.fragments.len() > state.max_distributed_fragments {
        return Err(OnlineError::Request(
            "distributed fragment count exceeds the online ceiling".to_owned(),
        ));
    }
    if plan.fragments.iter().any(|fragment| {
        usize::try_from(fragment.row_count)
            .ok()
            .is_none_or(|rows| rows > state.max_distributed_intermediate_rows)
    }) {
        return Err(OnlineError::Request(
            "a certified fragment exceeds the online row ceiling".to_owned(),
        ));
    }
    let service = state.fragment_service.as_deref().ok_or_else(|| {
        OnlineError::Upstream("fragment worker service is not configured".to_owned())
    })?;
    let mut workers = tokio::net::lookup_host(service)
        .await
        .map_err(|error| OnlineError::Upstream(format!("fragment DNS failed: {error}")))?
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    workers.sort_unstable();
    if workers.len() < 2 {
        return Err(OnlineError::Upstream(
            "distributed execution requires at least two ready fragment workers".to_owned(),
        ));
    }
    let request = FragmentExecutionRequest {
        snapshot_id: semantic.active.snapshot.snapshot_id,
        manifest_sha256: semantic.active.snapshot.manifest_sha256.clone(),
    };
    let response_spool = state.fragment_response_spool.clone().ok_or_else(|| {
        OnlineError::SnapshotConflict("query role has no fragment response spool".to_owned())
    })?;
    let max_response_rows = state.max_distributed_intermediate_rows;
    let exchange_bytes = Arc::new(AtomicUsize::new(0));
    let fragments = plan.fragments.clone();
    let responses = stream::iter(fragments.into_iter().enumerate().map(|(index, fragment)| {
        let worker = workers[index % workers.len()];
        let url = format!(
            "http://{worker}/v1/datasets/{}/fragments/{}/{}/execute",
            semantic.active.snapshot.dataset_id, query_sha256, fragment.fragment_id
        );
        let client = state.fragment_http.clone();
        let token = token.to_owned();
        let request = request.clone();
        let max_response_bytes = state.max_fragment_response_bytes;
        let max_exchange_bytes = state.max_distributed_exchange_bytes;
        let exchange_bytes = Arc::clone(&exchange_bytes);
        let response_spool = Arc::clone(&response_spool);
        async move {
            let response = client
                .post(url)
                .bearer_auth(token)
                .header(ACCEPT, ARROW_STREAM_MEDIA_TYPE)
                .json(&request)
                .send()
                .await
                .map_err(|error| upstream_transport_error("fragment worker request", error))?;
            if !response.status().is_success() {
                return Err(upstream_status_error("fragment worker", response.status()));
            }
            require_arrow_content_type(&response)?;
            let lease = response_spool.receive(response, max_response_bytes).await?;
            let byte_count = usize::try_from(lease.bytes).map_err(|_| {
                OnlineError::Request(
                    "fragment response byte count exceeds this platform".to_owned(),
                )
            })?;
            exchange_bytes
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                    current
                        .checked_add(byte_count)
                        .filter(|total| *total <= max_exchange_bytes)
                })
                .map_err(|_| {
                    OnlineError::Request(
                        "distributed exchange exceeds the total byte ceiling".to_owned(),
                    )
                })?;
            Ok::<_, OnlineError>(lease)
        }
    }))
    .buffer_unordered(state.fragment_exchange_concurrency)
    .try_collect::<Vec<_>>()
    .await?;
    let validation_concurrency = state.fragment_exchange_concurrency;
    let responses = stream::iter(responses.into_iter().map(|lease| async move {
        tokio::task::spawn_blocking(move || {
            ValidatedFragmentSpool::validate(lease, max_response_rows)
        })
        .await?
    }))
    .buffer_unordered(validation_concurrency)
    .try_collect::<Vec<_>>()
    .await?;
    let mut by_id = BTreeMap::new();
    let mut worker_ids = BTreeSet::new();
    for response in responses {
        if by_id
            .insert(response.metadata.fragment_id.clone(), response)
            .is_some()
        {
            return Err(OnlineError::SnapshotConflict(
                "fragment worker returned a duplicate fragment".to_owned(),
            ));
        }
    }
    let mut fragment_spools = Vec::with_capacity(plan.join_order.len());
    for fragment_id in &plan.join_order {
        let fragment = plan
            .fragments
            .iter()
            .find(|fragment| &fragment.fragment_id == fragment_id)
            .ok_or_else(|| {
                OnlineError::SnapshotConflict(
                    "join order references an unknown fragment".to_owned(),
                )
            })?;
        let response = by_id
            .remove(fragment_id)
            .ok_or_else(|| OnlineError::Upstream("a fragment result is missing".to_owned()))?;
        if response.metadata.dataset_id != semantic.active.snapshot.dataset_id
            || response.metadata.snapshot_id != semantic.active.snapshot.snapshot_id
            || response.metadata.query_sha256 != query_sha256
            || response.metadata.worker_id.is_empty()
            || response.head != fragment.head
            || response.row_count != fragment.row_count
            || response.metadata.multiset_sha256 != fragment.observed_multiset_sha256
        {
            return Err(OnlineError::SnapshotConflict(
                "fragment response differs from its offline certificate".to_owned(),
            ));
        }
        worker_ids.insert(response.metadata.worker_id.clone());
        fragment_spools.push(response);
    }
    if !by_id.is_empty() {
        return Err(OnlineError::SnapshotConflict(
            "fragment workers returned results outside the join order".to_owned(),
        ));
    }
    if worker_ids.len() < 2 {
        return Err(OnlineError::Upstream(
            "distributed fragments did not execute on at least two workers".to_owned(),
        ));
    }
    let fragment_ingress_bytes =
        u64::try_from(exchange_bytes.load(Ordering::Acquire)).map_err(|_| {
            OnlineError::Request("fragment ingress byte count exceeds this platform".to_owned())
        })?;
    let final_head = plan.final_head.clone();
    let ordered = plan.ordered;
    let max_intermediate_rows = state.max_distributed_intermediate_rows;
    let expected_multiset_sha256 = distributed.distributed_multiset_sha256.clone();
    let fragment_summaries = fragment_spools
        .iter()
        .map(ValidatedFragmentSpool::summary)
        .collect::<Vec<_>>();
    let shuffle_eligible = shuffle_plan_is_eligible(&plan, &fragment_summaries)?;
    let (
        joined,
        shuffle_worker_count,
        shuffle_spill_bytes,
        shuffle_cache_hits,
        worker_join_summary,
        fragment_materialization_mode,
        fragment_owned_rows,
        shuffle_result_ingress_mode,
        shuffle_result_ingress_bytes,
        intermediate_result_mode,
        assembled_intermediate_owned_rows,
    ) = if shuffle_eligible {
        let (joined, worker_count, spill_bytes, cache_hits, join_summary, result_ingress_bytes) =
            execute_partitioned_shuffle(
                state.clone(),
                Arc::clone(&semantic),
                plan.clone(),
                distributed.clone(),
                fragment_spools,
                workers.clone(),
                token.clone(),
            )
            .await?;
        (
            joined,
            Some(worker_count),
            spill_bytes,
            cache_hits,
            join_summary,
            "direct_spool_to_primary_partition_v1".to_owned(),
            0,
            "streamed_nvme_spool_v1".to_owned(),
            result_ingress_bytes,
            "partition_spool_sequence_v1".to_owned(),
            0,
        )
    } else {
        let (joined, owned_rows) = tokio::task::spawn_blocking(move || {
            local_fragment_join_spools(fragment_spools, max_intermediate_rows)
        })
        .await??;
        (
            joined,
            None,
            0,
            0,
            WorkerJoinSummary::default(),
            "bounded_owned_fallback_v1".to_owned(),
            owned_rows,
            "none".to_owned(),
            0,
            "none".to_owned(),
            0,
        )
    };
    let (bindings, qualified_entity_iris) = tokio::task::spawn_blocking(move || {
        let bindings =
            project_sparql_json(&joined, &final_head).map_err(distributed_execution_error)?;
        let observed = canonical_sparql_multiset_sha256(&final_head, &bindings, ordered)
            .map_err(ReferenceRuntimeError::Query)?;
        if observed != expected_multiset_sha256 {
            return Err(OnlineError::SnapshotConflict(
                "distributed final multiset differs from offline certification".to_owned(),
            ));
        }
        let qualified_entity_iris = binding_entity_iris(&bindings);
        Ok((bindings, qualified_entity_iris))
    })
    .await??;
    let coverage_scope = semantic
        .manifest
        .certified_queries
        .iter()
        .find(|query| query.query_sha256 == query_sha256)
        .map(|query| query.scope.clone())
        .ok_or(ReferenceRuntimeError::UncertifiedQuery)?;
    Ok((
        CertifiedSemanticResult {
            dataset_id: semantic.active.snapshot.dataset_id,
            snapshot_id: semantic.active.snapshot.snapshot_id,
            query_sha256: query_sha256.to_owned(),
            query_form: QueryForm::Select,
            head: plan.final_head.clone(),
            bindings,
            boolean_result: None,
            graph_ntriples: Vec::new(),
            qualified_entity_iris,
            coverage_scope,
        },
        ExecutionResponse {
            mode: if shuffle_worker_count.is_some() {
                "certified_partitioned_shuffle"
            } else {
                "certified_distributed_fragments"
            }
            .to_owned(),
            exchange_format: "arrow_ipc_stream_v1".to_owned(),
            fragment_ingress_mode: "streamed_nvme_spool_v1".to_owned(),
            fragment_ingress_bytes,
            fragment_materialization_mode,
            fragment_owned_rows,
            shuffle_result_ingress_mode,
            shuffle_result_ingress_bytes,
            intermediate_result_mode,
            assembled_intermediate_owned_rows,
            fragment_count: distributed.fragment_count,
            worker_count: u32::try_from(worker_ids.len())
                .map_err(|_| OnlineError::SnapshotConflict("worker count overflow".to_owned()))?,
            shuffle_partition_count: if shuffle_worker_count.is_some() {
                state.shuffle_partition_count
            } else {
                0
            },
            shuffle_worker_count: if let Some(count) = shuffle_worker_count {
                u32::try_from(count).map_err(|_| {
                    OnlineError::SnapshotConflict("shuffle worker count overflow".to_owned())
                })?
            } else {
                0
            },
            shuffle_spill_mode: if shuffle_worker_count.is_some() {
                "bounded_local_nvme_v1"
            } else {
                "none"
            }
            .to_owned(),
            shuffle_spill_bytes,
            shuffle_cache_mode: if shuffle_worker_count.is_some() {
                "snapshot_checksum_local_nvme_v1"
            } else {
                "none"
            }
            .to_owned(),
            shuffle_cache_hits,
            worker_join_mode: if shuffle_worker_count.is_none() {
                "none"
            } else if worker_join_summary.grace_partitions == 0 {
                "in_memory_hash_v1"
            } else {
                "grace_hash_nvme_v1"
            }
            .to_owned(),
            worker_join_spill_bytes: worker_join_summary.spill_bytes,
            worker_join_grace_partitions: worker_join_summary.grace_partitions,
            worker_join_max_build_rows: worker_join_summary.max_build_rows,
            worker_input_mode: if shuffle_worker_count.is_some() {
                "streamed_spool_v1"
            } else {
                "none"
            }
            .to_owned(),
            worker_input_bytes: worker_join_summary.streamed_input_bytes,
            coordinator_request_mode: if shuffle_worker_count.is_some() {
                "streamed_from_spill_v1"
            } else {
                "none"
            }
            .to_owned(),
            coordinator_request_bytes: worker_join_summary.streamed_input_bytes,
            plan_sha256: Some(distributed.plan_artifact_sha256.clone()),
        },
    ))
}

fn binding_entity_iris(bindings: &[serde_json::Value]) -> Vec<String> {
    let mut iris = BTreeSet::new();
    for term in bindings
        .iter()
        .filter_map(serde_json::Value::as_object)
        .flat_map(|binding| binding.values())
    {
        if term.get("type").and_then(serde_json::Value::as_str) == Some("uri") {
            if let Some(iri) = term.get("value").and_then(serde_json::Value::as_str) {
                iris.insert(iri.to_owned());
            }
        }
    }
    iris.into_iter().collect()
}

fn local_fragment_join_spools(
    fragment_spools: Vec<ValidatedFragmentSpool>,
    max_rows: usize,
) -> Result<(Vec<serde_json::Value>, u64), OnlineError> {
    let mut fragments = fragment_spools.into_iter();
    let Some(first) = fragments.next() else {
        return Err(OnlineError::SnapshotConflict(
            "distributed plan produced no fragment bindings".to_owned(),
        ));
    };
    let mut owned_rows = first.row_count;
    let mut joined = first.materialize(max_rows)?;
    for fragment in fragments {
        owned_rows = owned_rows.checked_add(fragment.row_count).ok_or_else(|| {
            OnlineError::Request("fragment materialization row evidence overflow".to_owned())
        })?;
        let bindings = fragment.materialize(max_rows)?;
        joined = inner_join_sparql_json(&joined, &bindings, max_rows)
            .map_err(distributed_execution_error)?;
    }
    Ok((joined, owned_rows))
}

fn shuffle_plan_is_eligible(
    plan: &DistributedQueryPlanFile,
    fragment_summaries: &[FragmentBindingSummary],
) -> Result<bool, OnlineError> {
    if fragment_summaries.len() != plan.join_order.len() {
        return Err(OnlineError::SnapshotConflict(
            "fragment summaries do not cover the join order".to_owned(),
        ));
    }
    if fragment_summaries.len() < 2 {
        return Ok(false);
    }
    for stage in 0..plan.join_order.len().saturating_sub(1) {
        let stage_u32 = u32::try_from(stage).map_err(|_| {
            OnlineError::SnapshotConflict("shuffle stage count overflow".to_owned())
        })?;
        let Ok((_, _, keys)) = shuffle_stage_contract(plan, stage_u32) else {
            return Ok(false);
        };
        for key in &keys {
            for summary in fragment_summaries.iter().take(stage + 1) {
                if summary.head.contains(key) && !summary.always_bound_variables.contains(key) {
                    return Ok(false);
                }
            }
            if !fragment_summaries[stage + 1]
                .always_bound_variables
                .contains(key)
            {
                return Ok(false);
            }
        }
    }
    Ok(true)
}

#[allow(clippy::too_many_arguments)]
fn create_primary_shuffle_spill(
    root: &Path,
    identity: SpillIdentity,
    left: ShuffleSpoolRelation,
    right: ValidatedFragmentSpool,
    key_variables: &[String],
    max_bytes: u64,
    max_open_files: usize,
    max_rows: usize,
) -> Result<ShuffleSpillStage, OnlineError> {
    let left_stream = ValidatedFragmentSpoolSequence::new(left.spools, max_rows);
    let right_stream = ValidatedFragmentSpoolSequence::new(vec![right], max_rows);
    ShuffleSpillStage::create_iter(
        root,
        identity,
        left_stream,
        right_stream,
        key_variables,
        max_bytes,
        max_open_files,
    )
}

#[allow(clippy::too_many_arguments)]
fn validate_shuffle_response_spool(
    lease: FragmentResponseLease,
    dataset_id: Uuid,
    snapshot_id: Uuid,
    query_sha256: &str,
    expected_fragment_id: &str,
    expected_head: &[String],
    key_variables: &[String],
    partition_count: u32,
    expected_partition: u32,
    max_rows: usize,
) -> Result<ValidatedFragmentSpool, OnlineError> {
    let decoded = lease
        .open_stream(max_rows)?
        .into_batch()
        .map_err(distributed_execution_error)?;
    if decoded.metadata.dataset_id != dataset_id
        || decoded.metadata.snapshot_id != snapshot_id
        || decoded.metadata.query_sha256 != query_sha256
        || decoded.metadata.fragment_id != expected_fragment_id
        || decoded.metadata.worker_id.is_empty()
        || decoded.head.as_slice() != expected_head
    {
        return Err(OnlineError::SnapshotConflict(
            "shuffle response identity or head is invalid".to_owned(),
        ));
    }
    validate_shuffle_partition_rows(
        &decoded.bindings,
        key_variables,
        partition_count,
        expected_partition,
    )?;
    let observed = canonical_sparql_multiset_sha256(&decoded.head, &decoded.bindings, false)
        .map_err(ReferenceRuntimeError::Query)?;
    if observed != decoded.metadata.multiset_sha256 {
        return Err(OnlineError::SnapshotConflict(
            "shuffle response multiset checksum is invalid".to_owned(),
        ));
    }
    let mut always_bound_variables = decoded.head.iter().cloned().collect::<BTreeSet<_>>();
    for binding in &decoded.bindings {
        let binding = binding.as_object().ok_or_else(|| {
            OnlineError::SnapshotConflict(
                "shuffle response contains a non-object binding".to_owned(),
            )
        })?;
        always_bound_variables.retain(|variable| binding.contains_key(variable));
    }
    Ok(ValidatedFragmentSpool {
        lease,
        metadata: decoded.metadata,
        head: decoded.head,
        row_count: u64::try_from(decoded.bindings.len())
            .map_err(|_| OnlineError::Request("shuffle response row count overflow".to_owned()))?,
        always_bound_variables,
    })
}

fn materialize_fragment_spools(
    spools: Vec<ValidatedFragmentSpool>,
    max_rows: usize,
) -> Result<Vec<serde_json::Value>, OnlineError> {
    let mut rows = Vec::new();
    for spool in spools {
        let decoded = spool.materialize(max_rows)?;
        if rows
            .len()
            .checked_add(decoded.len())
            .is_none_or(|total| total > max_rows)
        {
            return Err(OnlineError::Request(
                "shuffle final result exceeds the intermediate row ceiling".to_owned(),
            ));
        }
        rows.extend(decoded);
    }
    Ok(rows)
}

async fn execute_partitioned_shuffle(
    state: AppState,
    semantic: Arc<SemanticState>,
    plan: Arc<DistributedQueryPlanFile>,
    distributed: DistributedQueryCertificate,
    fragment_spools: Vec<ValidatedFragmentSpool>,
    workers: Vec<SocketAddr>,
    token: String,
) -> Result<
    (
        Vec<serde_json::Value>,
        usize,
        u64,
        u32,
        WorkerJoinSummary,
        u64,
    ),
    OnlineError,
> {
    let mut fragments = fragment_spools.into_iter();
    let Some(first) = fragments.next() else {
        return Err(OnlineError::SnapshotConflict(
            "shuffle plan has no left relation".to_owned(),
        ));
    };
    let mut left_source = ShuffleSpoolRelation {
        original_fragment_count: 1,
        original_fragment_rows: first.row_count,
        spools: vec![first],
    };
    let exchange_bytes = Arc::new(AtomicUsize::new(0));
    let mut worker_ids = BTreeSet::new();
    let mut total_spill_bytes = 0_u64;
    let mut total_cache_hits = 0_u32;
    let mut total_shuffle_response_bytes = 0_u64;
    let mut worker_join_summary = WorkerJoinSummary::default();
    let spill_root = state.shuffle_spill_root.clone().ok_or_else(|| {
        OnlineError::SnapshotConflict("query role has no shuffle spill root".to_owned())
    })?;
    let max_intermediate_rows = state.max_distributed_intermediate_rows;
    let response_spool = state.fragment_response_spool.clone().ok_or_else(|| {
        OnlineError::SnapshotConflict("query role has no shuffle response spool".to_owned())
    })?;
    let query_sha256_bytes: [u8; 32] = hex::decode(&plan.query_sha256)
        .ok()
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or_else(|| {
            OnlineError::SnapshotConflict("shuffle query SHA-256 is invalid".to_owned())
        })?;
    for (stage, right) in fragments.enumerate() {
        let stage_u32 = u32::try_from(stage).map_err(|_| {
            OnlineError::SnapshotConflict("shuffle stage count overflow".to_owned())
        })?;
        let (left_head, right_head, key_variables) =
            shuffle_stage_contract(plan.as_ref(), stage_u32)?;
        let partition_count = state.shuffle_partition_count;
        let keys = key_variables.clone();
        let spill_identity = SpillIdentity {
            dataset_id: semantic.active.snapshot.dataset_id,
            snapshot_id: semantic.active.snapshot.snapshot_id,
            query_sha256: query_sha256_bytes,
            stage: stage_u32,
            partition_count,
        };
        let max_spill_bytes = state.max_shuffle_spill_bytes;
        let max_open_files = state.max_shuffle_open_files;
        let stage_root = spill_root.clone();
        let direct_fragment_count = left_source
            .original_fragment_count
            .checked_add(1)
            .ok_or_else(|| {
                OnlineError::Request("direct fragment partition count evidence overflow".to_owned())
            })?;
        let direct_fragment_rows = left_source
            .original_fragment_rows
            .checked_add(right.row_count)
            .ok_or_else(|| {
                OnlineError::Request("direct fragment partition row evidence overflow".to_owned())
            })?;
        let stage_spill = tokio::task::spawn_blocking(move || {
            create_primary_shuffle_spill(
                &stage_root,
                spill_identity,
                left_source,
                right,
                &keys,
                max_spill_bytes,
                max_open_files,
                max_intermediate_rows,
            )
        })
        .await??;
        state
            .admission
            .coordinator_direct_partition_fragments
            .fetch_add(direct_fragment_count, Ordering::Relaxed);
        state
            .admission
            .coordinator_direct_partition_rows
            .fetch_add(direct_fragment_rows, Ordering::Relaxed);
        total_spill_bytes = total_spill_bytes
            .checked_add(stage_spill.total_bytes)
            .ok_or_else(|| OnlineError::Request("shuffle spill evidence overflow".to_owned()))?;
        let stage_spill = Arc::new(stage_spill);
        let output_head = union_binding_heads(&left_head, &right_head);
        let partition_workers = (0..partition_count)
            .map(|partition_u32| {
                let partition = usize::try_from(partition_u32).map_err(|_| {
                    OnlineError::Request("shuffle partition exceeds this platform".to_owned())
                })?;
                Ok((partition_u32, workers[partition % workers.len()]))
            })
            .collect::<Result<Vec<_>, OnlineError>>()?;
        let responses = stream::iter(partition_workers.into_iter().map(
            |(partition_u32, worker)| {
                let dataset_id = semantic.active.snapshot.dataset_id;
                let snapshot_id = semantic.active.snapshot.snapshot_id;
                let query_sha256 = plan.query_sha256.clone();
                let plan_sha256 = distributed.plan_artifact_sha256.clone();
                let left_head = left_head.clone();
                let right_head = right_head.clone();
                let key_variables = key_variables.clone();
                let client = state.fragment_http.clone();
                let token = token.to_owned();
                let exchange_bytes = Arc::clone(&exchange_bytes);
                let max_request_bytes = state.max_shuffle_request_bytes;
                let max_response_bytes = state.max_shuffle_response_bytes;
                let max_exchange_bytes = state.max_shuffle_exchange_bytes;
                let max_rows = state.max_distributed_intermediate_rows;
                let batch_rows = state.fragment_arrow_batch_rows;
                let chunk_bytes = state.fragment_arrow_http_chunk_bytes;
                let channel_capacity = state.fragment_arrow_channel_capacity;
                let max_worker_buckets = state.worker_join_bucket_count;
                let max_worker_build_rows = state.max_worker_join_build_rows;
                let stage_spill = Arc::clone(&stage_spill);
                let admission = Arc::clone(&state.admission);
                let response_spool = Arc::clone(&response_spool);
                async move {
                    let request_id = Uuid::new_v4();
                    let producer_spill = Arc::clone(&stage_spill);
                    let producer_keys = key_variables.clone();
                    let producer_query_sha256 = query_sha256.clone();
                    let (body_sender, body_receiver) = mpsc::channel(channel_capacity);
                    let (evidence_sender, evidence_receiver) = tokio::sync::oneshot::channel();
                    let producer_exchange = Arc::clone(&exchange_bytes);
                    let _producer = tokio::task::spawn_blocking(move || {
                        let mut output = ArrowRequestWriter::new(
                            body_sender,
                            chunk_bytes,
                            max_request_bytes,
                            producer_exchange,
                            max_exchange_bytes,
                        );
                        let result = (|| {
                            let (left_reader, right_reader) = producer_spill.open_pair(
                                partition_u32,
                                &producer_keys,
                                max_rows,
                            )?;
                            let header = ShuffleJoinStreamHeader {
                                metadata: ShuffleJoinMetadata {
                                    dataset_id,
                                    snapshot_id,
                                    query_sha256: producer_query_sha256,
                                    plan_sha256,
                                    request_id,
                                    stage: stage_u32,
                                    partition: partition_u32,
                                    partition_count,
                                },
                                left_head,
                                right_head,
                                key_variables: producer_keys,
                                left_row_count: u64::try_from(left_reader.declared_rows())
                                    .map_err(|_| {
                                        OnlineError::Request(
                                            "left shuffle partition row count overflow".to_owned(),
                                        )
                                    })?,
                                right_row_count: u64::try_from(right_reader.declared_rows())
                                    .map_err(|_| {
                                        OnlineError::Request(
                                            "right shuffle partition row count overflow".to_owned(),
                                        )
                                    })?,
                            };
                            let left_rows = left_reader.map(|row| {
                                row.map_err(|error| {
                                    ExecutionError::InvalidArrowStream(error.to_string())
                                })
                            });
                            let right_rows = right_reader.map(|row| {
                                row.map_err(|error| {
                                    ExecutionError::InvalidArrowStream(error.to_string())
                                })
                            });
                            write_shuffle_join_stream_iter(
                                &mut output,
                                &header,
                                left_rows,
                                right_rows,
                                batch_rows,
                            )
                            .map_err(distributed_execution_error)?;
                            output.complete().map_err(OnlineError::Io)
                        })();
                        if let Err(error) = &result {
                            output.fail(error.to_string());
                        }
                        let _sent = evidence_sender.send(result);
                    });
                    let request_stream = stream::unfold(body_receiver, |mut receiver| async move {
                        receiver.recv().await.map(|item| (item, receiver))
                    });
                    let url = format!(
                        "http://{worker}/v1/datasets/{dataset_id}/shuffles/\
                             {query_sha256}/{stage_u32}/{partition_u32}/join"
                    );
                    let response_result = client
                        .post(url)
                        .bearer_auth(token)
                        .header(ACCEPT, ARROW_STREAM_MEDIA_TYPE)
                        .header(CONTENT_TYPE, ARROW_STREAM_MEDIA_TYPE)
                        .body(reqwest::Body::wrap_stream(request_stream))
                        .send()
                        .await;
                    let evidence = evidence_receiver.await.map_err(|_| {
                        OnlineError::Upstream(
                            "streamed shuffle request producer terminated without evidence"
                                .to_owned(),
                        )
                    })??;
                    let request_length = evidence.bytes;
                    let request_sha256 = evidence.sha256;
                    admission
                        .coordinator_streamed_requests
                        .fetch_add(1, Ordering::Relaxed);
                    admission
                        .coordinator_streamed_bytes
                        .fetch_add(request_length, Ordering::Relaxed);
                    let response = response_result.map_err(|error| {
                        upstream_transport_error("shuffle worker request", error)
                    })?;
                    if !response.status().is_success() {
                        return Err(upstream_status_error("shuffle worker", response.status()));
                    }
                    require_arrow_content_type(&response)?;
                    let cache_hit = match response
                        .headers()
                        .get("x-ngkg-shuffle-cache")
                        .and_then(|value| value.to_str().ok())
                    {
                        Some("hit") => true,
                        Some("miss") => false,
                        _ => {
                            return Err(OnlineError::Upstream(
                                "shuffle worker omitted a valid cache-status header".to_owned(),
                            ));
                        }
                    };
                    let join_evidence = worker_join_evidence(
                        response.headers(),
                        max_worker_buckets,
                        max_worker_build_rows,
                        request_length,
                        &request_sha256,
                    )?;
                    let lease = response_spool.receive(response, max_response_bytes).await?;
                    let response_bytes = usize::try_from(lease.bytes).map_err(|_| {
                        OnlineError::Request(
                            "shuffle response byte count exceeds this platform".to_owned(),
                        )
                    })?;
                    reserve_exchange_bytes(&exchange_bytes, response_bytes, max_exchange_bytes)?;
                    admission
                        .coordinator_spooled_shuffle_responses
                        .fetch_add(1, Ordering::Relaxed);
                    admission
                        .coordinator_spooled_shuffle_response_bytes
                        .fetch_add(lease.bytes, Ordering::Relaxed);
                    Ok::<_, OnlineError>((partition_u32, lease, cache_hit, join_evidence))
                }
            },
        ))
        .buffer_unordered(state.shuffle_exchange_concurrency)
        .try_collect::<Vec<_>>()
        .await?;
        let mut by_partition = BTreeMap::new();
        let mut stage_row_count = 0_u64;
        for (partition, lease, cache_hit, join_evidence) in responses {
            total_shuffle_response_bytes = total_shuffle_response_bytes
                .checked_add(lease.bytes)
                .ok_or_else(|| {
                    OnlineError::Request("shuffle response ingress evidence overflow".to_owned())
                })?;
            let expected_id = shuffle_partition_id(stage_u32, partition);
            let dataset_id = semantic.active.snapshot.dataset_id;
            let snapshot_id = semantic.active.snapshot.snapshot_id;
            let query_sha256 = plan.query_sha256.clone();
            let expected_head = output_head.clone();
            let ownership_keys = key_variables.clone();
            let spool = tokio::task::spawn_blocking(move || {
                validate_shuffle_response_spool(
                    lease,
                    dataset_id,
                    snapshot_id,
                    &query_sha256,
                    &expected_id,
                    &expected_head,
                    &ownership_keys,
                    partition_count,
                    partition,
                    max_intermediate_rows,
                )
            })
            .await??;
            stage_row_count = stage_row_count
                .checked_add(spool.row_count)
                .filter(|rows| {
                    usize::try_from(*rows)
                        .ok()
                        .is_some_and(|rows| rows <= max_intermediate_rows)
                })
                .ok_or_else(|| {
                    OnlineError::Request(
                        "shuffle stage exceeds the total intermediate row ceiling".to_owned(),
                    )
                })?;
            let worker_id = spool.metadata.worker_id.clone();
            if by_partition.insert(partition, spool).is_some() {
                return Err(OnlineError::SnapshotConflict(
                    "shuffle worker returned a duplicate partition".to_owned(),
                ));
            }
            worker_ids.insert(worker_id);
            if cache_hit {
                total_cache_hits = total_cache_hits.checked_add(1).ok_or_else(|| {
                    OnlineError::Request("shuffle cache hit count overflow".to_owned())
                })?;
            }
            worker_join_summary.spill_bytes = worker_join_summary
                .spill_bytes
                .checked_add(join_evidence.spill_bytes)
                .ok_or_else(|| {
                    OnlineError::Request("worker join spill evidence overflow".to_owned())
                })?;
            worker_join_summary.max_build_rows = worker_join_summary
                .max_build_rows
                .max(join_evidence.max_build_rows);
            worker_join_summary.streamed_input_bytes = worker_join_summary
                .streamed_input_bytes
                .checked_add(join_evidence.streamed_input_bytes)
                .ok_or_else(|| {
                    OnlineError::SnapshotConflict(
                        "worker streamed-input evidence overflow".to_owned(),
                    )
                })?;
            if join_evidence.grace_partitions == 1 {
                worker_join_summary.grace_partitions = worker_join_summary
                    .grace_partitions
                    .checked_add(1)
                    .ok_or_else(|| {
                        OnlineError::Request("worker Grace partition count overflow".to_owned())
                    })?;
            }
        }
        if by_partition.len() != usize::try_from(partition_count).unwrap_or(usize::MAX) {
            return Err(OnlineError::Upstream(
                "shuffle stage is missing a partition".to_owned(),
            ));
        }
        let mut stage_spools = Vec::with_capacity(by_partition.len());
        for partition in 0..partition_count {
            let spool = by_partition
                .remove(&partition)
                .ok_or_else(|| OnlineError::Upstream("shuffle partition is missing".to_owned()))?;
            stage_spools.push(spool);
        }
        left_source = ShuffleSpoolRelation {
            spools: stage_spools,
            original_fragment_count: 0,
            original_fragment_rows: 0,
        };
        Arc::try_unwrap(stage_spill)
            .map_err(|_| {
                OnlineError::SnapshotConflict(
                    "shuffle spill stage still has active readers".to_owned(),
                )
            })?
            .cleanup()?;
    }
    if worker_ids.len() < 2 {
        return Err(OnlineError::Upstream(
            "partitioned shuffle did not execute on at least two workers".to_owned(),
        ));
    }
    let final_spools = left_source.spools;
    let left = tokio::task::spawn_blocking(move || {
        materialize_fragment_spools(final_spools, max_intermediate_rows)
    })
    .await??;
    Ok((
        left,
        worker_ids.len(),
        total_spill_bytes,
        total_cache_hits,
        worker_join_summary,
        total_shuffle_response_bytes,
    ))
}

fn worker_join_evidence(
    headers: &HeaderMap,
    maximum_buckets: u32,
    maximum_build_rows: usize,
    expected_input_bytes: u64,
    expected_input_sha256: &str,
) -> Result<WorkerJoinSummary, OnlineError> {
    let value = |name: &'static str| {
        headers
            .get(name)
            .and_then(|header| header.to_str().ok())
            .ok_or_else(|| {
                OnlineError::Upstream(format!("shuffle worker omitted valid {name} evidence"))
            })
    };
    if value("x-ngkg-worker-input-mode")? != "streamed_spool_v1"
        || value("x-ngkg-worker-input-bytes")?.parse::<u64>().ok() != Some(expected_input_bytes)
        || value("x-ngkg-worker-input-sha256")? != expected_input_sha256
    {
        return Err(OnlineError::Upstream(
            "worker streaming-input evidence differs from the sent Arrow request".to_owned(),
        ));
    }
    let mode = value("x-ngkg-worker-join-mode")?;
    let spill_bytes = value("x-ngkg-worker-join-spill-bytes")?
        .parse::<u64>()
        .map_err(|_| OnlineError::Upstream("worker join spill evidence is invalid".to_owned()))?;
    let buckets = value("x-ngkg-worker-join-buckets")?
        .parse::<u32>()
        .map_err(|_| OnlineError::Upstream("worker join bucket evidence is invalid".to_owned()))?;
    let max_build_rows = value("x-ngkg-worker-join-max-build-rows")?
        .parse::<u64>()
        .map_err(|_| {
            OnlineError::Upstream("worker join build-row evidence is invalid".to_owned())
        })?;
    if max_build_rows
        > u64::try_from(maximum_build_rows).map_err(|_| {
            OnlineError::SnapshotConflict("worker build-row ceiling overflow".to_owned())
        })?
    {
        return Err(OnlineError::Upstream(
            "worker join exceeded the coordinator build-row ceiling".to_owned(),
        ));
    }
    let grace_partitions = match mode {
        "in_memory_hash_v1" if spill_bytes == 0 && buckets == 0 => 0,
        "grace_hash_nvme_v1" if spill_bytes > 0 && buckets > 0 && buckets <= maximum_buckets => 1,
        _ => {
            return Err(OnlineError::Upstream(
                "worker join mode-specific evidence is invalid".to_owned(),
            ));
        }
    };
    Ok(WorkerJoinSummary {
        grace_partitions,
        spill_bytes,
        max_build_rows,
        streamed_input_bytes: expected_input_bytes,
    })
}

fn reserve_exchange_bytes(
    total: &AtomicUsize,
    bytes: usize,
    maximum: usize,
) -> Result<(), OnlineError> {
    total
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
            current.checked_add(bytes).filter(|next| *next <= maximum)
        })
        .map(|_| ())
        .map_err(|_| {
            OnlineError::Request("distributed shuffle exceeds its byte ceiling".to_owned())
        })
}

fn distributed_execution_error(error: ExecutionError) -> OnlineError {
    match error {
        ExecutionError::IntermediateRowLimit => {
            OnlineError::Request("distributed join exceeds the online row ceiling".to_owned())
        }
        _ => OnlineError::SnapshotConflict(
            "distributed bindings violate the certified join contract".to_owned(),
        ),
    }
}

fn upstream_transport_error(operation: &'static str, error: reqwest::Error) -> OnlineError {
    if error.is_timeout() {
        OnlineError::GatewayTimeout(format!("{operation} exceeded its configured deadline"))
    } else {
        OnlineError::Upstream(format!("{operation} failed: {error}"))
    }
}

fn upstream_status_error(operation: &'static str, status: reqwest::StatusCode) -> OnlineError {
    if matches!(status.as_u16(), 408 | 504) {
        OnlineError::GatewayTimeout(format!("{operation} returned timeout status {status}"))
    } else {
        OnlineError::Upstream(format!("{operation} returned {status}"))
    }
}

async fn read_bounded_response(
    response: HttpResponse,
    max_bytes: usize,
) -> Result<Vec<u8>, OnlineError> {
    let max_bytes_u64 = u64::try_from(max_bytes).unwrap_or(u64::MAX);
    if response
        .content_length()
        .is_some_and(|bytes| bytes > max_bytes_u64)
    {
        return Err(OnlineError::Request(
            "fragment response exceeds the byte ceiling".to_owned(),
        ));
    }
    let mut body = Vec::with_capacity(
        response
            .content_length()
            .and_then(|bytes| usize::try_from(bytes).ok())
            .unwrap_or(0)
            .min(max_bytes),
    );
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk
            .map_err(|error| upstream_transport_error("bounded upstream response stream", error))?;
        if body
            .len()
            .checked_add(chunk.len())
            .is_none_or(|total| total > max_bytes)
        {
            return Err(OnlineError::Request(
                "fragment response exceeds the byte ceiling".to_owned(),
            ));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

async fn execute_distributed_algebra_replica(
    State(state): State<AppState>,
    AxumPath((dataset_id, query_sha256, replica)): AxumPath<(Uuid, String, u32)>,
    headers: HeaderMap,
    Json(request): Json<DistributedAlgebraExecutionRequest>,
) -> Result<Response, OnlineError> {
    if state.role != Role::Fragment {
        return Err(OnlineError::SnapshotConflict(
            "distributed algebra route is not hosted by this worker role".to_owned(),
        ));
    }
    let identity = state.authorizer.authorize(&headers)?;
    if request.snapshot_id.is_nil()
        || request.original_query.is_empty()
        || request.original_query.len() > state.max_query_bytes
        || request.rewritten_query.is_empty()
        || request.rewritten_query.len() > state.max_request_bytes
        || request.max_solution_rows == 0
        || request.max_graph_triples == 0
        || request.max_graph_blank_nodes == 0
        || !is_sha256(&query_sha256)
        || query_sha256 != request.original_query_sha256
        || hex::encode(Sha256::digest(request.original_query.as_bytes())) != query_sha256
        || !is_sha256(&request.rewritten_query_sha256)
        || hex::encode(Sha256::digest(request.rewritten_query.as_bytes()))
            != request.rewritten_query_sha256
        || request.replica != replica
        || request.replica_count < 2
        || replica >= request.replica_count
        || usize::try_from(request.replica_count)
            .ok()
            .is_none_or(|count| count > state.distributed_algebra_replicas)
    {
        return Err(OnlineError::Request(
            "distributed algebra identity or replica set is invalid".to_owned(),
        ));
    }
    let compiled = CompiledSparqlQuery::parse(&request.original_query)
        .map_err(|error| OnlineError::MalformedSparql(error.to_string()))?;
    let rewritten = CompiledSparqlQuery::parse(&request.rewritten_query)
        .map_err(|error| OnlineError::MalformedSparql(error.to_string()))?;
    if compiled.form() != rewritten.form()
        || compiled.solution_order_is_significant() != request.ordered
    {
        return Err(OnlineError::Request(
            "rewritten algebra changes the query form or ordering contract".to_owned(),
        ));
    }
    let authorization = state
        .manager
        .clone()
        .authorization_state(identity.tenant_id, dataset_id)
        .await?;
    let preauthorized = authorized_service_graphs(&identity, &authorization.graph_catalog)?;
    let semantic = state
        .manager
        .clone()
        .semantic_state(identity.tenant_id, dataset_id)
        .await?;
    if request.snapshot_id != semantic.active.snapshot.snapshot_id
        || request.manifest_sha256 != semantic.active.snapshot.manifest_sha256
        || request.active_dataset.authorized_graph_set_sha256 != preauthorized.graph_set_sha256
        || validate_resolved_dataset(&semantic.graph_catalog, &request.active_dataset).is_err()
        || !resolved_dataset_is_authorized(
            &request.active_dataset,
            &semantic.graph_catalog,
            &preauthorized.graph_iris,
        )
    {
        return Err(OnlineError::GraphForbidden);
    }
    let limits = CertifiedQueryExecutionLimits {
        max_solution_rows: request.max_solution_rows.min(state.max_query_result_rows),
        max_graph_triples: request.max_graph_triples.min(state.max_query_graph_triples),
        max_graph_blank_nodes: request
            .max_graph_blank_nodes
            .min(state.max_query_graph_blank_nodes),
    };
    let runtime = state
        .manager
        .clone()
        .full_runtime(Arc::clone(&semantic))
        .await?;
    let active_dataset = request.active_dataset.clone();
    let graph_catalog = Arc::clone(&semantic.graph_catalog);
    let original_query = request.original_query.clone();
    let rewritten_query = rewritten.query_clone();
    let cancellation = CancellationToken::new();
    let cancellation_for_runtime = cancellation.clone();
    let mut task = tokio::task::spawn_blocking(move || {
        runtime.execute_exact_entailment_rewritten_with_dataset_bounded_cancellable(
            &original_query,
            &compiled,
            rewritten_query,
            &active_dataset,
            &graph_catalog,
            limits,
            Some(cancellation_for_runtime),
        )
    });
    let result = match tokio::time::timeout(state.query_timeout, &mut task).await {
        Ok(joined) => joined??,
        Err(_) => {
            cancellation.cancel();
            return Err(OnlineError::GatewayTimeout(
                "distributed scalar-oracle replica exceeded the query timeout".to_owned(),
            ));
        }
    };
    if result.dataset_id != dataset_id
        || result.snapshot_id != request.snapshot_id
        || result.query_sha256 != request.original_query_sha256
        || result.query_form != rewritten.form()
    {
        return Err(OnlineError::SnapshotConflict(
            "distributed scalar-oracle result has a different immutable identity".to_owned(),
        ));
    }
    let result_sha256 = canonical_query_payload_sha256(
        result.query_form,
        &result.head,
        &result.bindings,
        result.boolean_result,
        &result.graph_ntriples,
        request.ordered,
        limits,
    )
    .map_err(ReferenceRuntimeError::Query)?;
    let response = DistributedAlgebraExecutionResponse {
        dataset_id,
        snapshot_id: request.snapshot_id,
        manifest_sha256: request.manifest_sha256,
        original_query_sha256: request.original_query_sha256,
        rewritten_query_sha256: request.rewritten_query_sha256,
        result_sha256,
        replica,
        replica_count: request.replica_count,
        worker_id: state.worker_id.clone(),
        complete: true,
        result: DistributedAlgebraResultPayload {
            query_form: result.query_form,
            head: result.head,
            bindings: result.bindings,
            boolean_result: result.boolean_result,
            graph_ntriples: result.graph_ntriples,
            qualified_entity_iris: result.qualified_entity_iris,
            coverage_scope: result.coverage_scope,
        },
    };
    let mut body = BoundedBuffer::new(state.max_query_response_bytes);
    serde_json::to_writer(&mut body, &response).map_err(|_| {
        OnlineError::Request("distributed algebra response exceeds its byte ceiling".to_owned())
    })?;
    Ok(query_json_response(Bytes::from(body.into_bytes()), false))
}

async fn execute_native_leaf_scan(
    State(state): State<AppState>,
    AxumPath((dataset_id, query_sha256, partition)): AxumPath<(Uuid, String, u32)>,
    headers: HeaderMap,
    Json(request): Json<NativeLeafScanRequest>,
) -> Result<Response, OnlineError> {
    if state.role != Role::Fragment || !is_sha256(&query_sha256) {
        return Err(OnlineError::Request(
            "native leaf route identity is invalid".to_owned(),
        ));
    }
    let identity = state.authorizer.authorize(&headers)?;
    let authorization = state
        .manager
        .clone()
        .authorization_state(identity.tenant_id, dataset_id)
        .await?;
    let preauthorized = authorized_service_graphs(&identity, &authorization.graph_catalog)?;
    let semantic = state
        .manager
        .clone()
        .semantic_state(identity.tenant_id, dataset_id)
        .await?;
    if request.snapshot_id != semantic.active.snapshot.snapshot_id
        || request.manifest_sha256 != semantic.active.snapshot.manifest_sha256
        || request.active_dataset.authorized_graph_set_sha256 != preauthorized.graph_set_sha256
        || validate_resolved_dataset(&semantic.graph_catalog, &request.active_dataset).is_err()
        || !resolved_dataset_is_authorized(
            &request.active_dataset,
            &semantic.graph_catalog,
            &preauthorized.graph_iris,
        )
        || request.limits.max_rows == 0
        || request.limits.max_rows > state.max_distributed_intermediate_rows
        || request.limits.max_decoded_bytes == 0
        || usize::try_from(request.limits.max_decoded_bytes)
            .ok()
            .is_none_or(|bytes| bytes > state.max_distributed_exchange_bytes)
        || request.limits.batch_rows == 0
        || request.limits.batch_rows > state.fragment_arrow_batch_rows
        || (request.limits.execution_mode == LeafExecutionMode::OpenMp
            && !openmp_kernel_available())
    {
        return Err(OnlineError::GraphForbidden);
    }
    let files = state
        .manager
        .clone()
        .semantic_partition_files(&semantic.active, partition)
        .await?;
    if request.semantic_root_sha256 != files.semantic_root_sha256
        || request
            .predicate
            .graph_id
            .is_some_and(|_| request.active_dataset.default_graph_ids.is_empty()
                && request.active_dataset.named_graph_ids.is_empty())
    {
        return Err(OnlineError::SnapshotConflict(
            "native leaf request differs from the active semantic root".to_owned(),
        ));
    }
    let active_graph_iris = graph_iris_for_ids(
        &semantic.graph_catalog,
        &request
            .active_dataset
            .default_graph_ids
            .iter()
            .chain(request.active_dataset.named_graph_ids.iter())
            .copied()
            .collect::<Vec<_>>(),
    )?;
    let dictionary_terms = active_graph_iris
        .iter()
        .map(|iri| format!("N\t{iri}"))
        .collect::<BTreeSet<_>>();
    let dictionary_path = files.dictionary_path.clone();
    let allowed_graph_ids = tokio::task::spawn_blocking(move || {
        lookup_dictionary_ids_available(&dictionary_path, &dictionary_terms)
            .map(|ids| ids.into_values().collect::<BTreeSet<_>>())
    })
    .await?
    .map_err(partition_path_error)?;
    if allowed_graph_ids.is_empty() {
        return Err(OnlineError::GraphForbidden);
    }
    let mut predicate = request.predicate;
    predicate.allowed_graph_ids = allowed_graph_ids;
    if predicate
        .graph_id
        .is_some_and(|graph| !predicate.allowed_graph_ids.contains(&graph))
    {
        return Err(OnlineError::GraphForbidden);
    }
    let limits = request.limits;
    let path = files.facts_path.clone();
    let sha256 = files.facts_sha256.clone();
    let bytes = files.facts_bytes;
    let expected_rows = files.facts_rows;
    let cancelled = Arc::new(AtomicBool::new(false));
    let cancelled_for_scan = Arc::clone(&cancelled);
    let mut task = tokio::task::spawn_blocking(move || {
        scan_verified_parquet_leaf(
            &path,
            &sha256,
            bytes,
            partition,
            predicate,
            limits,
            &cancelled_for_scan,
        )
    });
    let result = match tokio::time::timeout(state.query_timeout, &mut task).await {
        Ok(joined) => joined?
            .map_err(|error| OnlineError::SnapshotConflict(error.to_string()))?,
        Err(_) => {
            cancelled.store(true, Ordering::Release);
            return Err(OnlineError::GatewayTimeout(
                "native Parquet leaf scan exceeded the query deadline".to_owned(),
            ));
        }
    };
    if result.scanned_rows != expected_rows {
        return Err(OnlineError::SnapshotConflict(
            "native Parquet scan did not cover the complete certified partition".to_owned(),
        ));
    }
    let response = NativeLeafScanResponse {
        dataset_id,
        snapshot_id: request.snapshot_id,
        query_sha256,
        partition,
        partition_manifest_sha256: files.partition_manifest_sha256,
        worker_id: state.worker_id.clone(),
        result,
    };
    let mut body = BoundedBuffer::new(state.max_fragment_response_bytes);
    serde_json::to_writer(&mut body, &response).map_err(|_| {
        OnlineError::Request("native leaf response exceeds its byte ceiling".to_owned())
    })?;
    Ok(query_json_response(Bytes::from(body.into_bytes()), false))
}

async fn execute_partition_path(
    State(state): State<AppState>,
    AxumPath((dataset_id, query_sha256, path_id, iteration, partition)): AxumPath<(
        Uuid,
        String,
        String,
        u32,
        u32,
    )>,
    headers: HeaderMap,
    Json(request): Json<PartitionPathExecutionRequest>,
) -> Result<Response, OnlineError> {
    if state.role != Role::Fragment || !state.partition_native_paths_enabled {
        return Err(OnlineError::SnapshotConflict(
            "partition-native property-path execution is disabled on this role".to_owned(),
        ));
    }
    let identity = state.authorizer.authorize(&headers)?;
    if !is_sha256(&query_sha256)
        || !is_sha256(&request.plan_sha256)
        || !is_sha256(&request.semantic_root_sha256)
        || request.storage_partition != partition
        || request.iteration != iteration
        || request.plan.path_id != path_id
        || request.plan.partition_count < 2
        || partition >= request.plan.partition_count
        || iteration >= request.plan.max_iterations
        || request.plan.max_iterations != state.property_path_max_iterations
        || request.plan.max_frontier_items != state.property_path_max_frontier_items
        || request.plan.max_visited_items != state.property_path_max_visited_items
        || request.plan.max_checkpoint_bytes != state.property_path_max_checkpoint_bytes
        || request.plan.max_spill_bytes != state.property_path_max_spill_bytes
        || request.plan.hot_vertex_degree != state.property_path_hot_vertex_degree
        || request.plan.max_hot_vertex_splits != state.property_path_max_hot_vertex_splits
        || !request.plan.require_complete_partition_set
        || !request.plan.require_scalar_equivalence
        || sha256_json(&request.plan)? != request.plan_sha256
        || u64::try_from(request.frontier.len())
            .ok()
            .is_none_or(|count| count > state.property_path_max_frontier_items)
    {
        return Err(OnlineError::Request(
            "partition-native property-path identity is invalid".to_owned(),
        ));
    }
    let authorization = state
        .manager
        .clone()
        .authorization_state(identity.tenant_id, dataset_id)
        .await?;
    let preauthorized = authorized_service_graphs(&identity, &authorization.graph_catalog)?;
    let semantic = state
        .manager
        .clone()
        .semantic_state(identity.tenant_id, dataset_id)
        .await?;
    let activation = semantic.active.cloud_activation.as_ref().ok_or_else(|| {
        OnlineError::SnapshotConflict(
            "partition-native paths require an active cloud semantic root".to_owned(),
        )
    })?;
    if request.snapshot_id != semantic.active.snapshot.snapshot_id
        || request.manifest_sha256 != semantic.active.snapshot.manifest_sha256
        || request.semantic_root_sha256 != activation.semantic_root_sha256
        || request.plan.partition_count
            != u32::try_from(activation.semantic_partition_count).map_err(|_| {
                OnlineError::SnapshotConflict("semantic partition count is invalid".to_owned())
            })?
        || request.active_dataset.authorized_graph_set_sha256 != preauthorized.graph_set_sha256
        || validate_resolved_dataset(&semantic.graph_catalog, &request.active_dataset).is_err()
        || !resolved_dataset_is_authorized(
            &request.active_dataset,
            &semantic.graph_catalog,
            &preauthorized.graph_iris,
        )
    {
        return Err(OnlineError::GraphForbidden);
    }
    let files = state
        .manager
        .clone()
        .semantic_partition_files(&semantic.active, partition)
        .await?;
    let dictionary_path = files.dictionary_path.clone();
    let graph_catalog = Arc::clone(&semantic.graph_catalog);
    let active_dataset = request.active_dataset.clone();
    let plan = request.plan.clone();
    let worker_id = state.worker_id.clone();
    let worker_threads = state.property_path_worker_threads;
    let max_rows = state.property_path_max_scan_rows;
    let max_work_items = state
        .max_fragment_response_bytes
        .checked_div(512)
        .ok_or_else(|| {
            OnlineError::Request("property-path response work ceiling is invalid".to_owned())
        })?;
    let action = request.action;
    let frontier = request.frontier.clone();
    let query_sha = query_sha256.clone();
    let plan_sha = request.plan_sha256.clone();
    let forward = files.forward.clone();
    let reverse = files.reverse.clone();
    let mut worker_path_metrics = PropertyPathMetricLease::new(Arc::clone(&state.admission));
    worker_path_metrics.set_pending(1);
    let path_core_permits = u32::try_from(worker_threads)
        .map_err(|_| OnlineError::Request("property-path core lane count overflow".to_owned()))?;
    let _path_core_permits = tokio::time::timeout(
        state.query_timeout,
        Arc::clone(&state.property_path_core_lanes).acquire_many_owned(path_core_permits),
    )
    .await
    .map_err(|_| {
        OnlineError::GatewayTimeout("property-path core lanes remained saturated".to_owned())
    })?
    .map_err(|_| OnlineError::Upstream("property-path core lane pool is closed".to_owned()))?;
    worker_path_metrics.set_pending(0);
    worker_path_metrics.set_frontier(u64::try_from(frontier.len()).map_err(|_| {
        OnlineError::Request("property-path worker frontier metric overflow".to_owned())
    })?);
    let batch = tokio::task::spawn_blocking(move || {
        let index =
            PartitionAdjacencyIndex::open(forward, reverse).map_err(partition_path_error)?;
        let (authorized_graphs, default_graphs, named_graphs, named_graph_iris) =
            dense_path_graph_sets(&dictionary_path, &graph_catalog, &active_dataset)?;
        let scope = resolve_path_graph_scope(
            &plan.graph_scope,
            &dictionary_path,
            &authorized_graphs,
            &default_graphs,
            &named_graphs,
            &named_graph_iris,
        )?;
        let scan_graphs = match &scope {
            PathGraphScope::UnionDefault => default_graphs.clone(),
            PathGraphScope::Named(graph) => BTreeSet::from([*graph]),
            PathGraphScope::NamedVariable(graphs) => graphs.clone(),
        };
        match action {
            PartitionPathAction::Seed => {
                if !frontier.is_empty() {
                    return Err(OnlineError::Request(
                        "property-path seed request must not contain a frontier".to_owned(),
                    ));
                }
                let subject_key = fixed_pattern_dictionary_key(&plan.subject_pattern);
                let subject_filter = subject_key
                    .as_deref()
                    .map(|term| lookup_dictionary_id_optional(&dictionary_path, term))
                    .transpose()
                    .map_err(partition_path_error)?
                    .flatten();
                if subject_key.is_some() && subject_filter.is_none() {
                    return Ok(PartitionPathBatch {
                        storage_partition: partition,
                        work: Vec::new(),
                        results: Vec::new(),
                        seed_frontier: Vec::new(),
                        adjacency_rows_read: 0,
                        hot_split_count: 0,
                        worker_threads: 1,
                        complete: true,
                    });
                }
                let (seed_frontier, adjacency_rows_read) = index
                    .seed_frontier(&plan, &scope, subject_filter, &scan_graphs, max_rows)
                    .map_err(partition_path_error)?;
                Ok(PartitionPathBatch {
                    storage_partition: partition,
                    work: Vec::new(),
                    results: Vec::new(),
                    seed_frontier,
                    adjacency_rows_read,
                    hot_split_count: 0,
                    worker_threads: 1,
                    complete: true,
                })
            }
            PartitionPathAction::Expand => {
                if frontier.is_empty() {
                    return Err(OnlineError::Request(
                        "property-path expansion requires a non-empty frontier".to_owned(),
                    ));
                }
                execute_partition_path_batch(
                    &index,
                    &dictionary_path,
                    &query_sha,
                    &plan_sha,
                    &plan,
                    iteration,
                    partition,
                    &frontier,
                    &scan_graphs,
                    max_rows,
                    max_work_items,
                    worker_threads,
                    &worker_id,
                )
                .map_err(partition_path_error)
            }
        }
    })
    .await??;
    worker_path_metrics.set_frontier(0);
    let response_sha256 = sha256_json(&batch)?;
    let response = PartitionPathExecutionResponse {
        dataset_id,
        snapshot_id: request.snapshot_id,
        semantic_root_sha256: files.semantic_root_sha256,
        partition_manifest_sha256: files.partition_manifest_sha256,
        forward_adjacency_sha256: files.forward.sha256,
        reverse_adjacency_sha256: files.reverse.sha256,
        dictionary_sha256: files.dictionary_sha256,
        plan_sha256: request.plan_sha256,
        storage_partition: partition,
        iteration,
        worker_id: state.worker_id.clone(),
        response_sha256,
        batch,
        complete: true,
    };
    let mut body = BoundedBuffer::new(state.max_fragment_response_bytes);
    serde_json::to_writer(&mut body, &response).map_err(|_| {
        OnlineError::Request("partition path response exceeds its byte ceiling".to_owned())
    })?;
    Ok(query_json_response(Bytes::from(body.into_bytes()), false))
}

fn partition_path_error(error: impl std::fmt::Display) -> OnlineError {
    OnlineError::SnapshotConflict(format!("partition-native property path failed: {error}"))
}

fn fixed_pattern_dictionary_key(pattern: &str) -> Option<String> {
    if pattern.starts_with('?') || pattern.starts_with('$') || pattern.starts_with("_:") {
        return None;
    }
    pattern
        .strip_prefix('<')
        .and_then(|value| value.strip_suffix('>'))
        .map(|iri| format!("N\t{iri}"))
        .or_else(|| pattern.starts_with('"').then(|| format!("L\t{pattern}")))
}

fn dense_path_graph_sets(
    dictionary_path: &Path,
    catalog: &GraphCatalog,
    dataset: &ResolvedDataset,
) -> Result<
    (
        BTreeSet<u64>,
        BTreeSet<u64>,
        BTreeSet<u64>,
        BTreeSet<String>,
    ),
    OnlineError,
> {
    let default_iris = graph_iris_for_ids(catalog, &dataset.default_graph_ids)?;
    let named_iris = graph_iris_for_ids(catalog, &dataset.named_graph_ids)?;
    let keys = default_iris
        .iter()
        .chain(&named_iris)
        .map(|iri| format!("N\t{iri}"))
        .collect::<BTreeSet<_>>();
    let ids =
        lookup_dictionary_ids_available(dictionary_path, &keys).map_err(partition_path_error)?;
    let default_graphs = default_iris
        .iter()
        .filter_map(|iri| ids.get(&format!("N\t{iri}")).copied())
        .collect::<BTreeSet<_>>();
    let named_graphs = named_iris
        .iter()
        .filter_map(|iri| ids.get(&format!("N\t{iri}")).copied())
        .collect::<BTreeSet<_>>();
    let authorized = default_graphs.union(&named_graphs).copied().collect();
    Ok((
        authorized,
        default_graphs,
        named_graphs,
        named_iris.into_iter().collect(),
    ))
}

fn resolve_path_graph_scope(
    scope: &str,
    dictionary_path: &Path,
    authorized_graphs: &BTreeSet<u64>,
    _default_graphs: &BTreeSet<u64>,
    named_graphs: &BTreeSet<u64>,
    named_graph_iris: &BTreeSet<String>,
) -> Result<PathGraphScope, OnlineError> {
    if scope == "active-default" {
        return Ok(PathGraphScope::UnionDefault);
    }
    if scope.starts_with('?') || scope.starts_with('$') {
        return Ok(PathGraphScope::NamedVariable(named_graphs.clone()));
    }
    let iri = scope
        .strip_prefix('<')
        .and_then(|value| value.strip_suffix('>'))
        .ok_or_else(|| OnlineError::Request("property-path graph scope is invalid".to_owned()))?;
    if !named_graph_iris.contains(iri) {
        return Err(OnlineError::GraphForbidden);
    }
    let key = format!("N\t{iri}");
    match lookup_dictionary_id_optional(dictionary_path, &key).map_err(partition_path_error)? {
        Some(graph) if authorized_graphs.contains(&graph) && named_graphs.contains(&graph) => {
            Ok(PathGraphScope::Named(graph))
        }
        Some(_) => Err(OnlineError::GraphForbidden),
        None => Ok(PathGraphScope::NamedVariable(BTreeSet::new())),
    }
}

fn resolved_dataset_is_authorized(
    dataset: &ResolvedDataset,
    graph_catalog: &GraphCatalog,
    authorized_graph_iris: &BTreeSet<String>,
) -> bool {
    dataset
        .default_graph_ids
        .iter()
        .chain(&dataset.named_graph_ids)
        .chain(&dataset.authorized_graph_ids)
        .all(|graph_id| {
            graph_catalog
                .by_id(*graph_id)
                .is_some_and(|graph| match &graph.name {
                    LogicalGraphName::Named { iri } => authorized_graph_iris.contains(iri),
                    LogicalGraphName::Default => false,
                })
        })
}

async fn execute_fragment(
    State(state): State<AppState>,
    AxumPath((dataset_id, query_sha256, fragment_id)): AxumPath<(Uuid, String, String)>,
    headers: HeaderMap,
    Json(request): Json<FragmentExecutionRequest>,
) -> Result<Response, OnlineError> {
    let identity = state.authorizer.authorize(&headers)?;
    require_arrow_accept(&headers)?;
    if !is_sha256(&query_sha256)
        || fragment_id.is_empty()
        || fragment_id.len() > 128
        || !fragment_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
    {
        return Err(OnlineError::Request(
            "fragment query hash or identifier is invalid".to_owned(),
        ));
    }
    // Internal fragment endpoints receive the same end-user identity as the
    // coordinator. Resolve authorization before loading any semantic state so
    // a forged fragment request cannot probe a graph outside that principal's
    // authorized union/named dataset.
    let authorization = state
        .manager
        .clone()
        .authorization_state(identity.tenant_id, dataset_id)
        .await?;
    let preauthorized = authorized_service_graphs(&identity, &authorization.graph_catalog)?;
    let semantic = state
        .manager
        .clone()
        .semantic_state(identity.tenant_id, dataset_id)
        .await?;
    if request.snapshot_id != semantic.active.snapshot.snapshot_id
        || request.manifest_sha256 != semantic.active.snapshot.manifest_sha256
    {
        return Err(OnlineError::SnapshotConflict(
            "fragment request does not address the active manifest".to_owned(),
        ));
    }
    let plan = state
        .manager
        .clone()
        .distributed_plan(Arc::clone(&semantic), query_sha256.clone())
        .await?;
    let fragment = plan
        .fragments
        .iter()
        .find(|fragment| fragment.fragment_id == fragment_id)
        .ok_or_else(|| {
            OnlineError::SnapshotConflict("fragment is absent from its plan".to_owned())
        })?;
    if !preauthorized.graph_iris.contains(&fragment.graph_iri) {
        return Err(OnlineError::GraphForbidden);
    }
    if usize::try_from(fragment.row_count)
        .ok()
        .is_none_or(|rows| rows > state.max_distributed_intermediate_rows)
    {
        return Err(OnlineError::Request(
            "certified fragment exceeds the online row ceiling".to_owned(),
        ));
    }
    let runtime = state
        .manager
        .clone()
        .fragment_runtime(
            Arc::clone(&semantic),
            query_sha256.clone(),
            fragment_id.clone(),
        )
        .await?;
    let result = tokio::task::spawn_blocking(move || runtime.execute()).await??;
    let metadata = FragmentBatchMetadata {
        dataset_id: result.dataset_id,
        snapshot_id: result.snapshot_id,
        query_sha256: result.query_sha256,
        fragment_id: result.fragment_id,
        worker_id: state.worker_id.clone(),
        multiset_sha256: result.multiset_sha256,
    };
    Ok(arrow_binding_response(
        &state,
        metadata,
        result.head,
        result.bindings,
        state.max_fragment_response_bytes,
    ))
}

fn arrow_binding_response(
    state: &AppState,
    metadata: FragmentBatchMetadata,
    head: Vec<String>,
    bindings: Vec<serde_json::Value>,
    max_bytes: usize,
) -> Response {
    let max_batch_rows = state.fragment_arrow_batch_rows;
    let chunk_bytes = state.fragment_arrow_http_chunk_bytes;
    let channel_capacity = state.fragment_arrow_channel_capacity;
    let (sender, receiver) = mpsc::channel(channel_capacity);
    let _encoder = tokio::task::spawn_blocking(move || {
        let mut output = ArrowBodyWriter::new(sender, chunk_bytes, max_bytes);
        let encoded =
            write_fragment_arrow_stream(&mut output, &metadata, &head, &bindings, max_batch_rows)
                .and_then(|()| {
                    output
                        .flush()
                        .map_err(|error| ExecutionError::InvalidArrowStream(error.to_string()))
                });
        if let Err(error) = encoded {
            output.fail(error.to_string());
        }
    });
    let stream = stream::unfold(receiver, |mut receiver| async move {
        receiver.recv().await.map(|item| (item, receiver))
    });
    (
        [(CONTENT_TYPE, ARROW_STREAM_MEDIA_TYPE)],
        Body::from_stream(stream),
    )
        .into_response()
}

async fn execute_shuffle_partition(
    State(state): State<AppState>,
    AxumPath((dataset_id, query_sha256, stage, partition)): AxumPath<(Uuid, String, u32, u32)>,
    headers: HeaderMap,
    body: Body,
) -> Result<Response, OnlineError> {
    let identity = state.authorizer.authorize(&headers)?;
    require_arrow_accept(&headers)?;
    require_arrow_request_content_type(&headers)?;
    let maximum_request = u64::try_from(state.max_shuffle_request_bytes).map_err(|_| {
        OnlineError::Request("shuffle request ceiling exceeds this platform".to_owned())
    })?;
    if !is_sha256(&query_sha256)
        || headers
            .get(CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok())
            .is_some_and(|length| length > maximum_request)
    {
        return Err(OnlineError::Request(
            "shuffle query identity or request size is invalid".to_owned(),
        ));
    }
    let semantic = state
        .manager
        .clone()
        .semantic_state(identity.tenant_id, dataset_id)
        .await?;
    let plan = state
        .manager
        .clone()
        .distributed_plan(Arc::clone(&semantic), query_sha256.clone())
        .await?;
    let certificate = distributed_certificate(&semantic.manifest, &query_sha256)?;
    let max_rows = state.max_distributed_intermediate_rows;
    let request_spool = state.shuffle_request_spool.clone().ok_or_else(|| {
        OnlineError::SnapshotConflict("fragment role has no streaming request spool".to_owned())
    })?;
    let request = request_spool
        .receive(body, state.max_shuffle_request_bytes)
        .await?;
    let request_path = request.path.clone();
    let validated =
        tokio::task::spawn_blocking(move || inspect_shuffle_spool(&request_path, max_rows))
            .await??;
    let (left_head, right_head, key_variables) = shuffle_stage_contract(&plan, stage)?;
    if validated.header.metadata.dataset_id != dataset_id
        || validated.header.metadata.snapshot_id != semantic.active.snapshot.snapshot_id
        || validated.header.metadata.query_sha256 != query_sha256
        || validated.header.metadata.plan_sha256 != certificate.plan_artifact_sha256
        || validated.header.metadata.stage != stage
        || validated.header.metadata.partition != partition
        || validated.header.metadata.partition_count != state.shuffle_partition_count
        || validated.header.left_head != left_head
        || validated.header.right_head != right_head
        || validated.header.key_variables != key_variables
    {
        return Err(OnlineError::SnapshotConflict(
            "shuffle request differs from its snapshot, plan, stage, or partition contract"
                .to_owned(),
        ));
    }
    let output_head =
        union_binding_heads(&validated.header.left_head, &validated.header.right_head);
    let join_keys = validated.header.key_variables.clone();
    let partition_count = validated.header.metadata.partition_count;
    let expected_partition = validated.header.metadata.partition;
    let left_row_count = validated.header.left_row_count;
    let right_row_count = validated.header.right_row_count;
    let left_input_sha256 = validated.left_stream_sha256;
    let right_input_sha256 = validated.right_stream_sha256;
    let cache = state.shuffle_result_cache.clone().ok_or_else(|| {
        OnlineError::SnapshotConflict("fragment role has no shuffle result cache".to_owned())
    })?;
    let grace_join_engine = state.grace_join_engine.clone().ok_or_else(|| {
        OnlineError::SnapshotConflict("fragment role has no worker Grace join engine".to_owned())
    })?;
    let grace_identity = GraceJoinIdentity {
        tenant_id: identity.tenant_id,
        dataset_id,
        snapshot_id: semantic.active.snapshot.snapshot_id,
        query_sha256: query_sha256.clone(),
        plan_sha256: certificate.plan_artifact_sha256.clone(),
        stage,
        partition,
        partition_count,
        left_input_sha256: left_input_sha256.clone(),
        right_input_sha256: right_input_sha256.clone(),
    };
    let cache_key = ShuffleCacheKey {
        tenant_id: identity.tenant_id,
        dataset_id,
        snapshot_id: semantic.active.snapshot.snapshot_id,
        query_sha256: query_sha256.clone(),
        plan_sha256: certificate.plan_artifact_sha256.clone(),
        stage,
        partition,
        partition_count,
        left_input_sha256,
        right_input_sha256,
    };
    let cache_digest = cache_key.digest()?;
    let flight = shuffle_cache_flight(
        Arc::clone(&state.shuffle_cache_flights),
        cache_digest.clone(),
    )
    .await;
    let flight_guard = Arc::clone(&flight.flight).lock_owned().await;
    let request_path_for_join = request.path.clone();
    let cache_result = async {
        let cache_for_read = Arc::clone(&cache);
        let key_for_read = cache_key.clone();
        let lookup = tokio::task::spawn_blocking(move || cache_for_read.get(&key_for_read)).await;
        match lookup {
            Ok(Ok(CacheLookup::Hit(payload))) => {
                let head_for_validation = output_head.clone();
                let keys_for_validation = join_keys.clone();
                let decoded = tokio::task::spawn_blocking(move || {
                    validate_cached_shuffle_result(
                        &payload,
                        &head_for_validation,
                        &keys_for_validation,
                        partition_count,
                        expected_partition,
                        max_rows,
                        state.worker_join_bucket_count,
                        state.max_worker_join_build_rows,
                    )
                })
                .await;
                match decoded {
                    Ok(Ok(Some(result))) => {
                        state.admission.cache_hits.fetch_add(1, Ordering::Relaxed);
                        return Ok::<_, OnlineError>((result, true));
                    }
                    Ok(Ok(None)) => {
                        state
                            .admission
                            .cache_invalid
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    Ok(Err(error)) => {
                        state
                            .admission
                            .cache_invalid
                            .fetch_add(1, Ordering::Relaxed);
                        tracing::warn!(%error, %cache_digest, "shuffle cache logical validation failed; recomputing exact partition");
                    }
                    Err(error) => {
                        state
                            .admission
                            .cache_errors
                            .fetch_add(1, Ordering::Relaxed);
                        tracing::warn!(%error, %cache_digest, "shuffle cache validation task failed; recomputing exact partition");
                    }
                }
                let cache_for_invalidate = Arc::clone(&cache);
                let key_for_invalidate = cache_key.clone();
                match tokio::task::spawn_blocking(move || {
                    cache_for_invalidate.invalidate(&key_for_invalidate)
                })
                .await
                {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => {
                        state
                            .admission
                            .cache_errors
                            .fetch_add(1, Ordering::Relaxed);
                        tracing::warn!(%error, %cache_digest, "invalid shuffle cache entry could not be removed");
                    }
                    Err(error) => {
                        state
                            .admission
                            .cache_errors
                            .fetch_add(1, Ordering::Relaxed);
                        tracing::warn!(%error, %cache_digest, "shuffle cache invalidation task failed");
                    }
                }
            }
            Ok(Ok(CacheLookup::Miss)) => {}
            Ok(Err(error)) => {
                state
                    .admission
                    .cache_errors
                    .fetch_add(1, Ordering::Relaxed);
                tracing::warn!(%error, %cache_digest, "shuffle cache read failed; recomputing exact partition");
            }
            Err(error) => {
                state
                    .admission
                    .cache_errors
                    .fetch_add(1, Ordering::Relaxed);
                tracing::warn!(%error, %cache_digest, "shuffle cache read task failed; recomputing exact partition");
            }
        }

        state
            .admission
            .cache_misses
            .fetch_add(1, Ordering::Relaxed);

        let output_head_for_join = output_head.clone();
        let keys_for_join = join_keys.clone();
        let grace_engine_for_join = Arc::clone(&grace_join_engine);
        let grace_identity_for_join = grace_identity.clone();
        let max_cache_bytes = state.max_shuffle_cache_entry_bytes;
        let in_memory_join_build_rows = state.in_memory_join_build_rows;
        let max_worker_join_build_rows = state.max_worker_join_build_rows;
        let (result, encoded) = tokio::task::spawn_blocking(move || {
            let small_partition = right_row_count
                <= u64::try_from(in_memory_join_build_rows).unwrap_or(u64::MAX)
                && left_row_count
                    <= u64::try_from(max_worker_join_build_rows).unwrap_or(u64::MAX);
            let result = if small_partition {
                let input = ShuffleJoinStream::try_new(
                    File::open(&request_path_for_join)?,
                    max_rows,
                )
                .map_err(distributed_execution_error)?
                .into_input()
                .map_err(distributed_execution_error)?;
                compute_shuffle_result(
                    &grace_engine_for_join,
                    &grace_identity_for_join,
                    input.left_bindings,
                    input.right_bindings,
                    output_head_for_join,
                    keys_for_join,
                    partition_count,
                    expected_partition,
                    max_rows,
                )?
            } else {
                compute_streaming_shuffle_result(
                    &grace_engine_for_join,
                    &grace_identity_for_join,
                    &request_path_for_join,
                    output_head_for_join,
                    keys_for_join,
                    partition_count,
                    expected_partition,
                    max_rows,
                )?
            };
            let mut output = BoundedBuffer::new(max_cache_bytes);
            let encoded = serde_json::to_writer(&mut output, &result)
                .map(|()| output.into_bytes());
            Ok::<_, OnlineError>((result, encoded))
        })
        .await??;
        match result.join_mode.as_str() {
            "in_memory_hash_v1" => {
                state
                    .admission
                    .worker_join_in_memory
                    .fetch_add(1, Ordering::Relaxed);
            }
            "grace_hash_nvme_v1" => {
                state
                    .admission
                    .worker_join_grace
                    .fetch_add(1, Ordering::Relaxed);
                state
                    .admission
                    .worker_join_spill_bytes
                    .fetch_add(result.join_spill_bytes, Ordering::Relaxed);
            }
            _ => {
                return Err(OnlineError::SnapshotConflict(
                    "worker join returned an unknown execution mode".to_owned(),
                ));
            }
        }
        match encoded {
            Ok(payload) => {
                let cache_for_write = Arc::clone(&cache);
                let key_for_write = cache_key.clone();
                match tokio::task::spawn_blocking(move || {
                    cache_for_write.insert(&key_for_write, &payload)
                })
                .await
                {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => {
                        state
                            .admission
                            .cache_errors
                            .fetch_add(1, Ordering::Relaxed);
                        tracing::warn!(%error, %cache_digest, "shuffle cache write was skipped");
                    }
                    Err(error) => {
                        state
                            .admission
                            .cache_errors
                            .fetch_add(1, Ordering::Relaxed);
                        tracing::warn!(%error, %cache_digest, "shuffle cache write task failed");
                    }
                }
            }
            Err(error) => {
                tracing::warn!(%error, %cache_digest, "shuffle result exceeds its cache serialization ceiling");
            }
        }
        Ok((result, false))
    }
    .await;
    drop(flight_guard);
    drop(flight);
    let (result, cache_hit) = cache_result?;
    let metadata = FragmentBatchMetadata {
        dataset_id,
        snapshot_id: semantic.active.snapshot.snapshot_id,
        query_sha256,
        fragment_id: shuffle_partition_id(stage, partition),
        worker_id: state.worker_id.clone(),
        multiset_sha256: result.multiset_sha256,
    };
    let mut response = arrow_binding_response(
        &state,
        metadata,
        result.output_head,
        result.bindings,
        state.max_shuffle_response_bytes,
    );
    response.headers_mut().insert(
        "x-ngkg-shuffle-cache",
        HeaderValue::from_static(if cache_hit { "hit" } else { "miss" }),
    );
    for (name, value) in [
        ("x-ngkg-worker-input-mode", "streamed_spool_v1".to_owned()),
        ("x-ngkg-worker-input-bytes", request.bytes.to_string()),
        ("x-ngkg-worker-input-sha256", request.sha256.clone()),
        ("x-ngkg-worker-join-mode", result.join_mode),
        (
            "x-ngkg-worker-join-spill-bytes",
            result.join_spill_bytes.to_string(),
        ),
        (
            "x-ngkg-worker-join-buckets",
            result.join_bucket_count.to_string(),
        ),
        (
            "x-ngkg-worker-join-max-build-rows",
            result.join_max_build_rows.to_string(),
        ),
    ] {
        response.headers_mut().insert(
            name,
            HeaderValue::from_str(&value).map_err(|_| {
                OnlineError::SnapshotConflict("worker join response evidence is invalid".to_owned())
            })?,
        );
    }
    Ok(response)
}

fn inspect_shuffle_spool(
    path: &Path,
    max_rows: usize,
) -> Result<ValidatedShuffleSpool, OnlineError> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(OnlineError::SnapshotConflict(
            "streaming shuffle request path is not a regular owned file".to_owned(),
        ));
    }
    let mut stream = ShuffleJoinStream::try_new(File::open(path)?, max_rows)
        .map_err(distributed_execution_error)?;
    let header = stream.header().clone();
    let mut left = Sha256::new();
    let mut right = Sha256::new();
    left.update(b"ngkg-shuffle-left-stream-v1\0");
    right.update(b"ngkg-shuffle-right-stream-v1\0");
    for variable in &header.left_head {
        hash_length_prefixed(&mut left, variable.as_bytes())?;
    }
    for variable in &header.right_head {
        hash_length_prefixed(&mut right, variable.as_bytes())?;
    }
    for decoded in &mut stream {
        let (side, row) = decoded.map_err(distributed_execution_error)?;
        match side {
            0 => hash_shuffle_row(&mut left, &row)?,
            1 => hash_shuffle_row(&mut right, &row)?,
            _ => {
                return Err(OnlineError::SnapshotConflict(
                    "shuffle relation code is invalid".to_owned(),
                ));
            }
        }
    }
    left.update(header.left_row_count.to_be_bytes());
    right.update(header.right_row_count.to_be_bytes());
    Ok(ValidatedShuffleSpool {
        header,
        left_stream_sha256: hex::encode(left.finalize()),
        right_stream_sha256: hex::encode(right.finalize()),
    })
}

fn hash_shuffle_row(hash: &mut Sha256, row: &serde_json::Value) -> Result<(), OnlineError> {
    let encoded = serde_json::to_vec(row)?;
    hash_length_prefixed(hash, &encoded)
}

fn hash_length_prefixed(hash: &mut Sha256, bytes: &[u8]) -> Result<(), OnlineError> {
    let length = u64::try_from(bytes.len()).map_err(|_| {
        OnlineError::SnapshotConflict("shuffle digest component is too large".to_owned())
    })?;
    hash.update(length.to_be_bytes());
    hash.update(bytes);
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn compute_streaming_shuffle_result(
    engine: &GraceJoinEngine,
    identity: &GraceJoinIdentity,
    path: &Path,
    output_head: Vec<String>,
    join_keys: Vec<String>,
    partition_count: u32,
    expected_partition: u32,
    max_rows: usize,
) -> Result<CachedShuffleResult, OnlineError> {
    let stream = ShuffleJoinStream::try_new(File::open(path)?, max_rows)
        .map_err(distributed_execution_error)?;
    let rows = stream.map(|decoded| {
        let (side, row) = decoded.map_err(GraceJoinError::Execution)?;
        let side = match side {
            0 => GraceJoinSide::Left,
            1 => GraceJoinSide::Right,
            _ => {
                return Err(GraceJoinError::Execution(
                    ExecutionError::InvalidArrowStream(
                        "shuffle relation code is unknown".to_owned(),
                    ),
                ));
            }
        };
        Ok((side, row))
    });
    let outcome = engine
        .join_stream(identity, rows, &join_keys, max_rows)
        .map_err(|error| match error {
            GraceJoinError::Execution(execution) => distributed_execution_error(execution),
            other => OnlineError::GraceJoin(other),
        })?;
    let bindings = outcome.bindings;
    validate_shuffle_partition_rows(&bindings, &join_keys, partition_count, expected_partition)?;
    let multiset_sha256 = canonical_sparql_multiset_sha256(&output_head, &bindings, false)
        .map_err(ReferenceRuntimeError::Query)?;
    Ok(CachedShuffleResult {
        format_version: 2,
        output_head,
        bindings,
        multiset_sha256,
        join_mode: outcome.mode.to_owned(),
        join_spill_bytes: outcome.spill_bytes,
        join_bucket_count: outcome.buckets_processed,
        join_max_build_rows: outcome.max_build_rows,
    })
}

fn compute_shuffle_result(
    engine: &GraceJoinEngine,
    identity: &GraceJoinIdentity,
    left: Vec<serde_json::Value>,
    right: Vec<serde_json::Value>,
    output_head: Vec<String>,
    join_keys: Vec<String>,
    partition_count: u32,
    expected_partition: u32,
    max_rows: usize,
) -> Result<CachedShuffleResult, OnlineError> {
    let outcome = engine
        .join(identity, left, right, &join_keys, max_rows)
        .map_err(|error| match error {
            GraceJoinError::Execution(execution) => distributed_execution_error(execution),
            other => OnlineError::GraceJoin(other),
        })?;
    let bindings = outcome.bindings;
    validate_shuffle_partition_rows(&bindings, &join_keys, partition_count, expected_partition)?;
    let multiset_sha256 = canonical_sparql_multiset_sha256(&output_head, &bindings, false)
        .map_err(ReferenceRuntimeError::Query)?;
    Ok(CachedShuffleResult {
        format_version: 2,
        output_head,
        bindings,
        multiset_sha256,
        join_mode: outcome.mode.to_owned(),
        join_spill_bytes: outcome.spill_bytes,
        join_bucket_count: outcome.buckets_processed,
        join_max_build_rows: outcome.max_build_rows,
    })
}

fn validate_cached_shuffle_result(
    payload: &[u8],
    expected_head: &[String],
    join_keys: &[String],
    partition_count: u32,
    expected_partition: u32,
    max_rows: usize,
    maximum_buckets: u32,
    maximum_build_rows: usize,
) -> Result<Option<CachedShuffleResult>, OnlineError> {
    let Ok(result) = serde_json::from_slice::<CachedShuffleResult>(payload) else {
        return Ok(None);
    };
    if result.format_version != 2
        || result.output_head != expected_head
        || result.bindings.len() > max_rows
        || !is_sha256(&result.multiset_sha256)
    {
        return Ok(None);
    }
    let maximum_build_rows = u64::try_from(maximum_build_rows).map_err(|_| {
        OnlineError::SnapshotConflict("worker build-row ceiling overflow".to_owned())
    })?;
    let evidence_is_valid = match result.join_mode.as_str() {
        "in_memory_hash_v1" => {
            result.join_spill_bytes == 0
                && result.join_bucket_count == 0
                && result.join_max_build_rows <= maximum_build_rows
        }
        "grace_hash_nvme_v1" => {
            result.join_spill_bytes > 0
                && result.join_bucket_count > 0
                && result.join_bucket_count <= maximum_buckets
                && result.join_max_build_rows > 0
                && result.join_max_build_rows <= maximum_build_rows
        }
        _ => false,
    };
    if !evidence_is_valid {
        return Ok(None);
    }
    if validate_shuffle_partition_rows(
        &result.bindings,
        join_keys,
        partition_count,
        expected_partition,
    )
    .is_err()
    {
        return Ok(None);
    }
    let Ok(observed) =
        canonical_sparql_multiset_sha256(&result.output_head, &result.bindings, false)
    else {
        return Ok(None);
    };
    if observed != result.multiset_sha256 {
        return Ok(None);
    }
    Ok(Some(result))
}

fn validate_shuffle_partition_rows(
    bindings: &[serde_json::Value],
    join_keys: &[String],
    partition_count: u32,
    expected_partition: u32,
) -> Result<(), OnlineError> {
    for binding in bindings {
        if shuffle_partition_for_binding(binding, join_keys, partition_count)
            .map_err(distributed_execution_error)?
            != expected_partition
        {
            return Err(OnlineError::SnapshotConflict(
                "shuffle worker produced a row for another partition".to_owned(),
            ));
        }
    }
    Ok(())
}

async fn shuffle_cache_flight(
    registry: Arc<Mutex<BTreeMap<String, Arc<Mutex<()>>>>>,
    digest: String,
) -> ShuffleCacheFlightLease {
    let mut flights = registry.lock().await;
    let flight = Arc::clone(
        flights
            .entry(digest.clone())
            .or_insert_with(|| Arc::new(Mutex::new(()))),
    );
    drop(flights);
    ShuffleCacheFlightLease {
        registry,
        digest,
        flight,
    }
}

async fn query_cache_flight(
    registry: Arc<Mutex<BTreeMap<String, Arc<Mutex<()>>>>>,
    digest: String,
) -> QueryCacheFlightLease {
    let mut flights = registry.lock().await;
    let flight = Arc::clone(
        flights
            .entry(digest.clone())
            .or_insert_with(|| Arc::new(Mutex::new(()))),
    );
    drop(flights);
    QueryCacheFlightLease {
        registry,
        digest,
        flight,
    }
}

fn query_json_response(bytes: Bytes, cache_hit: bool) -> Response {
    let mut response = ([(CONTENT_TYPE, "application/json")], bytes).into_response();
    response.headers_mut().insert(
        "x-ngkg-query-cache",
        HeaderValue::from_static(if cache_hit { "hit" } else { "miss" }),
    );
    response
}

#[allow(clippy::too_many_arguments)]
fn validate_cached_query_response(
    response: &QueryResponse,
    key: &QueryCacheKey,
    routing: &QueryRoutingCertificate,
    expected_scope: &str,
    expected_form: QueryForm,
    expected_result_sha256: &str,
    expected_ordered: bool,
    result_limits: CertifiedQueryExecutionLimits,
    identity_namespace: Uuid,
    max_qualified_entities: usize,
    max_hydration_rows: u64,
    max_distributed_fragments: usize,
    shuffle_partition_count: u32,
    max_worker_join_build_rows: usize,
    authorized_graph_iris: &BTreeSet<String>,
) -> Result<(), OnlineError> {
    if response.dataset_id != key.dataset_id
        || response.snapshot_id != key.snapshot_id
        || response.serving_root_sha256 != key.serving_root_sha256
        || response.query_sha256 != key.query_sha256
        || response.query_form != expected_form
        || response.authorized_graph_set_sha256 != key.authorized_graph_set_sha256
        || response.active_dataset_sha256 != key.active_dataset_sha256
        || response.coverage_scope != expected_scope
        || !response.complete
        || response.qualified_entities.len() > max_qualified_entities
    {
        return Err(OnlineError::SnapshotConflict(
            "cached query response identity or completeness is invalid".to_owned(),
        ));
    }
    if response.routing.selection_mode != routing.selection_mode
        || response.routing.dataset_selection_source.code() != key.dataset_selection_source
        || response.routing.default_graph_iris != routing.default_graph_iris
        || response.routing.named_graph_iris != routing.named_graph_iris
        || response.routing.active_dataset_sha256 != routing.active_dataset_sha256
        || response.routing.active_dataset_sha256 != key.active_dataset_sha256
        || response.routing.include_internal_closure != routing.include_internal_closure
        || response.routing.selected_graph_iris != routing.selected_graph_iris
        || response.routing.selected_graph_count
            != u32::try_from(routing.selected_graph_iris.len()).map_err(|_| {
                OnlineError::SnapshotConflict("cached selected graph count overflow".to_owned())
            })?
        || response.routing.total_graph_count != routing.total_graph_count
        || response.routing.capability_index_sha256 != routing.capability_index_sha256
        || response.routing.routed_dataset_sha256 != routing.route_artifact_sha256
    {
        return Err(OnlineError::SnapshotConflict(
            "cached query response routing differs from its offline certificate".to_owned(),
        ));
    }
    let observed = canonical_query_payload_sha256(
        response.query_form,
        &response.head,
        &response.bindings,
        response.boolean_result,
        &response.graph_ntriples,
        expected_ordered,
        result_limits,
    )
    .map_err(ReferenceRuntimeError::Query)?;
    if observed != expected_result_sha256 {
        return Err(OnlineError::SnapshotConflict(
            "cached query result differs from the offline form-aware certificate".to_owned(),
        ));
    }
    let result_iris = query_response_entity_iris(response)?;
    let qualified_iris = response
        .qualified_entities
        .iter()
        .map(|entity| entity.iri.clone())
        .collect::<Vec<_>>();
    if qualified_iris != result_iris.into_iter().collect::<Vec<_>>()
        || response
            .qualified_entities
            .iter()
            .enumerate()
            .any(|(ordinal, entity)| {
                u64::try_from(ordinal).ok() != Some(entity.query_ordinal)
                    || entity.multiplicity != 1
            })
    {
        return Err(OnlineError::SnapshotConflict(
            "cached qualified entities differ from the exact SPARQL result".to_owned(),
        ));
    }
    verify_qualified_identities(&response.qualified_entities, identity_namespace)?;
    if !key.hydrate {
        if !response.hydrated_payload.is_empty() {
            return Err(OnlineError::SnapshotConflict(
                "non-hydrated cache entry contains payload".to_owned(),
            ));
        }
    } else {
        validate_hydrated_rows(
            &response.hydrated_payload,
            &response.qualified_entities,
            max_hydration_rows,
            authorized_graph_iris,
        )?;
    }
    if !validate_cached_execution(
        &response.execution,
        routing,
        max_distributed_fragments,
        result_limits.max_solution_rows,
        shuffle_partition_count,
        max_worker_join_build_rows,
    ) {
        return Err(OnlineError::SnapshotConflict(
            "cached query execution metadata is invalid".to_owned(),
        ));
    }
    Ok(())
}

fn query_response_entity_iris(response: &QueryResponse) -> Result<BTreeSet<String>, OnlineError> {
    match response.query_form {
        QueryForm::Ask => Ok(BTreeSet::new()),
        QueryForm::Select => {
            let mut iris = BTreeSet::new();
            for binding in &response.bindings {
                let object = binding.as_object().ok_or_else(|| {
                    OnlineError::SnapshotConflict(
                        "cached SPARQL binding is not an object".to_owned(),
                    )
                })?;
                for term in object.values() {
                    if term.get("type").and_then(serde_json::Value::as_str) == Some("uri") {
                        let iri = term
                            .get("value")
                            .and_then(serde_json::Value::as_str)
                            .ok_or_else(|| {
                                OnlineError::SnapshotConflict(
                                    "cached SPARQL URI binding has no lexical value".to_owned(),
                                )
                            })?;
                        iris.insert(iri.to_owned());
                    }
                }
            }
            Ok(iris)
        }
        QueryForm::Construct | QueryForm::Describe => {
            let mut iris = BTreeSet::new();
            let source = response.graph_ntriples.concat();
            for quad in RdfParser::from_format(RdfFormat::NTriples)
                .for_reader(Cursor::new(source.as_bytes()))
            {
                let quad = quad.map_err(|error| {
                    OnlineError::SnapshotConflict(format!(
                        "cached canonical graph result cannot be parsed: {error}"
                    ))
                })?;
                if let oxigraph::model::NamedOrBlankNode::NamedNode(node) = &quad.subject {
                    iris.insert(node.as_str().to_owned());
                }
                if let Term::NamedNode(node) = &quad.object {
                    iris.insert(node.as_str().to_owned());
                }
            }
            Ok(iris)
        }
    }
}

fn require_supported_result_hash_version(result_hash_version: u32) -> Result<(), OnlineError> {
    if result_hash_version == CERTIFIED_QUERY_RESULT_HASH_VERSION {
        Ok(())
    } else {
        Err(OnlineError::SnapshotConflict(
            "certified query uses an unsupported or legacy result hash version".to_owned(),
        ))
    }
}

fn validate_cached_execution(
    execution: &ExecutionResponse,
    routing: &QueryRoutingCertificate,
    max_distributed_fragments: usize,
    max_result_rows: usize,
    shuffle_partition_count: u32,
    max_worker_join_build_rows: usize,
) -> bool {
    let fragment_count_is_bounded = usize::try_from(execution.fragment_count)
        .ok()
        .is_some_and(|count| count <= max_distributed_fragments);
    let fallback_owned_rows_are_bounded = u64::try_from(max_distributed_fragments)
        .ok()
        .and_then(|fragments| {
            u64::try_from(max_result_rows)
                .ok()
                .and_then(|rows| fragments.checked_mul(rows))
        })
        .is_some_and(|maximum| execution.fragment_owned_rows <= maximum);
    match (&routing.distributed, execution.mode.as_str()) {
        (None, "certified_local_route") => {
            execution.exchange_format == "none"
                && execution.fragment_ingress_mode == "none"
                && execution.fragment_ingress_bytes == 0
                && execution.fragment_materialization_mode == "none"
                && execution.fragment_owned_rows == 0
                && execution.shuffle_result_ingress_mode == "none"
                && execution.shuffle_result_ingress_bytes == 0
                && execution.intermediate_result_mode == "none"
                && execution.assembled_intermediate_owned_rows == 0
                && execution.fragment_count == 0
                && execution.worker_count == 0
                && execution.shuffle_partition_count == 0
                && execution.shuffle_worker_count == 0
                && execution.shuffle_spill_mode == "none"
                && execution.shuffle_spill_bytes == 0
                && execution.shuffle_cache_mode == "none"
                && execution.shuffle_cache_hits == 0
                && execution.worker_join_mode == "none"
                && execution.worker_join_spill_bytes == 0
                && execution.worker_join_grace_partitions == 0
                && execution.worker_join_max_build_rows == 0
                && execution.worker_input_mode == "none"
                && execution.worker_input_bytes == 0
                && execution.coordinator_request_mode == "none"
                && execution.coordinator_request_bytes == 0
                && execution.plan_sha256.is_none()
        }
        (Some(distributed), "certified_distributed_fragments") => {
            fragment_count_is_bounded
                && execution.fragment_count == distributed.fragment_count
                && execution.worker_count >= 2
                && execution.worker_count <= execution.fragment_count
                && execution.exchange_format == "arrow_ipc_stream_v1"
                && execution.fragment_ingress_mode == "streamed_nvme_spool_v1"
                && execution.fragment_ingress_bytes > 0
                && execution.fragment_materialization_mode == "bounded_owned_fallback_v1"
                && fallback_owned_rows_are_bounded
                && execution.shuffle_result_ingress_mode == "none"
                && execution.shuffle_result_ingress_bytes == 0
                && execution.intermediate_result_mode == "none"
                && execution.assembled_intermediate_owned_rows == 0
                && execution.shuffle_partition_count == 0
                && execution.shuffle_worker_count == 0
                && execution.shuffle_spill_mode == "none"
                && execution.shuffle_spill_bytes == 0
                && execution.shuffle_cache_mode == "none"
                && execution.shuffle_cache_hits == 0
                && execution.worker_join_mode == "none"
                && execution.worker_join_spill_bytes == 0
                && execution.worker_join_grace_partitions == 0
                && execution.worker_join_max_build_rows == 0
                && execution.worker_input_mode == "none"
                && execution.worker_input_bytes == 0
                && execution.coordinator_request_mode == "none"
                && execution.coordinator_request_bytes == 0
                && execution.plan_sha256.as_deref()
                    == Some(distributed.plan_artifact_sha256.as_str())
        }
        (Some(distributed), "certified_partitioned_shuffle") => {
            fragment_count_is_bounded
                && execution.fragment_count == distributed.fragment_count
                && execution.worker_count >= 2
                && execution.worker_count <= execution.fragment_count
                && execution.exchange_format == "arrow_ipc_stream_v1"
                && execution.fragment_ingress_mode == "streamed_nvme_spool_v1"
                && execution.fragment_ingress_bytes > 0
                && execution.fragment_materialization_mode == "direct_spool_to_primary_partition_v1"
                && execution.fragment_owned_rows == 0
                && execution.shuffle_result_ingress_mode == "streamed_nvme_spool_v1"
                && execution.shuffle_result_ingress_bytes > 0
                && execution.intermediate_result_mode == "partition_spool_sequence_v1"
                && execution.assembled_intermediate_owned_rows == 0
                && execution.shuffle_partition_count == shuffle_partition_count
                && execution.shuffle_worker_count >= 2
                && execution.shuffle_worker_count <= shuffle_partition_count
                && execution.shuffle_spill_mode == "bounded_local_nvme_v1"
                && execution.shuffle_cache_mode == "snapshot_checksum_local_nvme_v1"
                && execution.worker_input_mode == "streamed_spool_v1"
                && execution.worker_input_bytes > 0
                && execution.coordinator_request_mode == "streamed_from_spill_v1"
                && execution.coordinator_request_bytes == execution.worker_input_bytes
                && worker_join_execution_is_valid(execution, max_worker_join_build_rows)
                && execution.plan_sha256.as_deref()
                    == Some(distributed.plan_artifact_sha256.as_str())
        }
        _ => false,
    }
}

fn worker_join_execution_is_valid(
    execution: &ExecutionResponse,
    maximum_build_rows: usize,
) -> bool {
    let build_is_bounded = u64::try_from(maximum_build_rows)
        .ok()
        .is_some_and(|maximum| execution.worker_join_max_build_rows <= maximum);
    match execution.worker_join_mode.as_str() {
        "in_memory_hash_v1" => {
            build_is_bounded
                && execution.worker_join_spill_bytes == 0
                && execution.worker_join_grace_partitions == 0
        }
        "grace_hash_nvme_v1" => {
            build_is_bounded
                && execution.worker_join_spill_bytes > 0
                && execution.worker_join_grace_partitions > 0
                && execution.worker_join_max_build_rows > 0
        }
        _ => false,
    }
}

fn distributed_certificate<'a>(
    manifest: &'a ReferenceSnapshotManifest,
    query_sha256: &str,
) -> Result<&'a DistributedQueryCertificate, OnlineError> {
    manifest
        .certified_queries
        .iter()
        .find(|query| query.query_sha256 == query_sha256)
        .and_then(|query| query.routing.as_ref())
        .and_then(|routing| routing.distributed.as_ref())
        .ok_or_else(|| {
            OnlineError::SnapshotConflict("shuffle query has no distributed certificate".to_owned())
        })
}

fn shuffle_stage_contract(
    plan: &DistributedQueryPlanFile,
    stage: u32,
) -> Result<(Vec<String>, Vec<String>, Vec<String>), OnlineError> {
    let stage = usize::try_from(stage)
        .map_err(|_| OnlineError::Request("shuffle stage does not fit this platform".to_owned()))?;
    let right_ordinal = stage
        .checked_add(1)
        .ok_or_else(|| OnlineError::Request("shuffle stage overflows the join order".to_owned()))?;
    if right_ordinal >= plan.join_order.len() {
        return Err(OnlineError::Request(
            "shuffle stage is outside the distributed join order".to_owned(),
        ));
    }
    let mut left_head = Vec::new();
    for fragment_id in &plan.join_order[..=stage] {
        let fragment = plan
            .fragments
            .iter()
            .find(|fragment| &fragment.fragment_id == fragment_id)
            .ok_or_else(|| {
                OnlineError::SnapshotConflict(
                    "shuffle stage references an unknown left fragment".to_owned(),
                )
            })?;
        left_head = union_binding_heads(&left_head, &fragment.head);
    }
    let right = plan
        .fragments
        .iter()
        .find(|fragment| fragment.fragment_id == plan.join_order[right_ordinal])
        .ok_or_else(|| {
            OnlineError::SnapshotConflict(
                "shuffle stage references an unknown right fragment".to_owned(),
            )
        })?;
    let right_set = right.head.iter().cloned().collect::<BTreeSet<_>>();
    let key_variables = left_head
        .iter()
        .filter(|variable| right_set.contains(*variable))
        .cloned()
        .collect::<Vec<_>>();
    if key_variables.is_empty() {
        return Err(OnlineError::Request(
            "cross-product stages are not eligible for hash shuffle".to_owned(),
        ));
    }
    Ok((left_head, right.head.clone(), key_variables))
}

fn union_binding_heads(left: &[String], right: &[String]) -> Vec<String> {
    let mut union = left.to_vec();
    let mut present = left.iter().cloned().collect::<BTreeSet<_>>();
    for variable in right {
        if present.insert(variable.clone()) {
            union.push(variable.clone());
        }
    }
    union
}

fn shuffle_partition_id(stage: u32, partition: u32) -> String {
    format!("shuffle-stage-{stage:04}-partition-{partition:04}")
}

fn require_arrow_request_content_type(headers: &HeaderMap) -> Result<(), OnlineError> {
    if headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        != Some(ARROW_STREAM_MEDIA_TYPE)
    {
        return Err(OnlineError::Request(
            "shuffle join requires an Arrow IPC request body".to_owned(),
        ));
    }
    Ok(())
}

fn require_arrow_accept(headers: &HeaderMap) -> Result<(), OnlineError> {
    let accepted = headers
        .get(ACCEPT)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| {
            value
                .split(',')
                .map(str::trim)
                .any(|media_type| media_type == ARROW_STREAM_MEDIA_TYPE)
        });
    if !accepted {
        return Err(OnlineError::Request(
            "fragment execution requires the Arrow IPC stream media type".to_owned(),
        ));
    }
    Ok(())
}

fn require_arrow_content_type(response: &HttpResponse) -> Result<(), OnlineError> {
    let content_type = response
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok());
    if content_type != Some(ARROW_STREAM_MEDIA_TYPE) {
        return Err(OnlineError::Upstream(
            "fragment worker returned an unexpected media type".to_owned(),
        ));
    }
    Ok(())
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

async fn locate(
    State(state): State<AppState>,
    AxumPath(dataset_id): AxumPath<Uuid>,
    headers: HeaderMap,
    Json(request): Json<LocatorRequest>,
) -> Result<Response, OnlineError> {
    let identity = state.authorizer.authorize(&headers)?;
    tracing::info!(
        tenant_id = %identity.tenant_id,
        principal_id = %identity.principal_id,
        %dataset_id,
        "locator request accepted"
    );
    validate_entity_request(&request.entities, state.max_qualified_entities)?;
    let physical = state
        .manager
        .clone()
        .physical_state(identity.tenant_id, dataset_id)
        .await?;
    let authorized = authorized_service_graphs(&identity, &physical.authorization.graph_catalog)?;
    let authorized_graph_ids =
        authorized_physical_graph_ids(&physical.dictionary, &authorized.graph_iris);
    require_physical_request(
        request.snapshot_id,
        &request.serving_root_sha256,
        &physical.active,
    )?;
    verify_qualified_identities(&request.entities, physical.active.identity_namespace)?;
    let mut entities = Vec::with_capacity(request.entities.len());
    for entity in request.entities {
        let records = physical.locator.lookup(entity.guid)?;
        if records.is_empty() {
            return Err(OnlineError::SnapshotConflict(
                "qualified GUID is absent from the active locator".to_owned(),
            ));
        }
        if records
            .iter()
            .any(|record| !physical.dictionary.contains_key(&record.graph_id))
        {
            return Err(OnlineError::SnapshotConflict(
                "locator graph ID is absent from the serving dictionary".to_owned(),
            ));
        }
        let records = records
            .into_iter()
            .filter(|record| authorized_graph_ids.contains(&record.graph_id))
            .collect::<Vec<_>>();
        if records.is_empty() {
            return Err(OnlineError::SnapshotConflict(
                "qualified GUID is absent from the active locator".to_owned(),
            ));
        }
        entities.push(LocatedEntity {
            query_ordinal: entity.query_ordinal,
            guid: entity.guid,
            records,
        });
    }
    let response = LocatorResponse {
        dataset_id,
        snapshot_id: physical.active.snapshot.snapshot_id,
        serving_root_sha256: required_serving_root(&physical.active)?
            .serving_root_sha256
            .clone(),
        entities,
    };
    let mut body = BoundedBuffer::new(state.max_hydration_response_bytes);
    serde_json::to_writer(&mut body, &response).map_err(|_| {
        OnlineError::Request("serialized locator response exceeds its byte ceiling".to_owned())
    })?;
    Ok(([(CONTENT_TYPE, "application/json")], body.into_bytes()).into_response())
}

async fn hydrate(
    State(state): State<AppState>,
    AxumPath(dataset_id): AxumPath<Uuid>,
    headers: HeaderMap,
    Json(request): Json<HydrationRequest>,
) -> Result<Response, OnlineError> {
    let identity = state.authorizer.authorize(&headers)?;
    tracing::info!(
        tenant_id = %identity.tenant_id,
        principal_id = %identity.principal_id,
        %dataset_id,
        "hydration request accepted"
    );
    validate_entity_request(&request.entities, state.max_qualified_entities)?;
    let physical = state
        .manager
        .clone()
        .physical_state(identity.tenant_id, dataset_id)
        .await?;
    let authorized = authorized_service_graphs(&identity, &physical.authorization.graph_catalog)?;
    let authorized_graph_ids =
        authorized_physical_graph_ids(&physical.dictionary, &authorized.graph_iris);
    require_physical_request(
        request.snapshot_id,
        &request.serving_root_sha256,
        &physical.active,
    )?;
    verify_qualified_identities(&request.entities, physical.active.identity_namespace)?;
    let qualified = request
        .entities
        .iter()
        .map(|entity| ShardedQualifiedGuid {
            query_ordinal: entity.query_ordinal,
            entity_guid: entity.guid,
            multiplicity: entity.multiplicity,
        })
        .collect::<Vec<_>>();
    let mut partitions = BTreeSet::new();
    for entity in &request.entities {
        for record in physical.locator.lookup(entity.guid)? {
            if !physical.dictionary.contains_key(&record.graph_id) {
                return Err(OnlineError::SnapshotConflict(
                    "locator graph ID is absent from the serving dictionary".to_owned(),
                ));
            }
            if authorized_graph_ids.contains(&record.graph_id) {
                partitions.insert(record.partition_index);
            }
        }
    }
    let shards = state
        .manager
        .clone()
        .payload_shards(Arc::clone(&physical), partitions)
        .await?;
    let locator = Arc::clone(&physical.locator);
    let snapshot_id = physical.active.snapshot.snapshot_id;
    let worker_threads = state.hydration_worker_threads;
    let max_rows = state.max_hydration_rows;
    let graph_ids = authorized_graph_ids;
    let rows = tokio::task::spawn_blocking(move || {
        hydrate_sharded_payload_for_graphs(
            &locator,
            snapshot_id,
            &qualified,
            &shards,
            worker_threads,
            max_rows,
            &graph_ids,
        )
    })
    .await??;
    let iri_by_guid = request
        .entities
        .iter()
        .map(|entity| (entity.guid, entity.iri.clone()))
        .collect::<BTreeMap<_, _>>();
    let rows = public_payload_rows(&rows, &iri_by_guid, &physical.dictionary)?
        .into_iter()
        .filter(|row| authorized.graph_iris.contains(&row.graph_iri))
        .collect::<Vec<_>>();
    validate_hydrated_rows(
        &rows,
        &request.entities,
        state.max_hydration_rows,
        &authorized.graph_iris,
    )?;
    let response = HydrationResponse {
        dataset_id,
        snapshot_id,
        serving_root_sha256: required_serving_root(&physical.active)?
            .serving_root_sha256
            .clone(),
        rows,
    };
    let mut body = BoundedBuffer::new(state.max_hydration_response_bytes);
    serde_json::to_writer(&mut body, &response).map_err(|_| {
        OnlineError::Request("serialized hydration response exceeds its byte ceiling".to_owned())
    })?;
    Ok(([(CONTENT_TYPE, "application/json")], body.into_bytes()).into_response())
}

impl ServingStateManager {
    async fn authorization_state(
        self: Arc<Self>,
        tenant_id: Uuid,
        dataset_id: Uuid,
    ) -> Result<Arc<GraphAuthorizationState>, OnlineError> {
        let key = (tenant_id, dataset_id);
        let load_lock = {
            let mut loads = self.authorization_loads.lock().await;
            Arc::clone(loads.entry(key).or_insert_with(|| Arc::new(Mutex::new(()))))
        };
        let _load_guard = load_lock.lock_owned().await;
        let active = self
            .catalog
            .clone()
            .get_active_serving_snapshot_owned(tenant_id, dataset_id)
            .await?;
        {
            let cache = self.authorization.lock().await;
            if let Some(existing) = cache.get(&key)
                && same_active(&existing.active, &active)
            {
                return Ok(Arc::clone(existing));
            }
        }
        let root = self.snapshot_cache_root(&active);
        tokio::fs::create_dir_all(&root).await?;
        let manifest_path = root.join("snapshot-manifest.json");
        Arc::clone(&self)
            .materialize_cached(
                active.snapshot.manifest_object_key.clone(),
                active.snapshot.manifest_sha256.clone(),
                manifest_path.clone(),
            )
            .await?;
        let manifest = Arc::new(serde_json::from_slice::<ReferenceSnapshotManifest>(
            &tokio::fs::read(&manifest_path).await?,
        )?);
        if manifest.dataset_id != dataset_id
            || manifest.snapshot_id != active.snapshot.snapshot_id
            || manifest.dataset_namespace != active.identity_namespace
        {
            return Err(OnlineError::SnapshotConflict(
                "reference manifest identity differs from graph authorization snapshot".to_owned(),
            ));
        }
        let graph_catalog_path = root.join("rdf-dataset-catalog.json");
        Arc::clone(&self)
            .materialize_snapshot_artifact(
                active.clone(),
                Arc::clone(&manifest),
                "indexes/rdf-dataset-catalog.json".to_owned(),
                graph_catalog_path.clone(),
            )
            .await?;
        let graph_catalog: GraphCatalog =
            serde_json::from_slice(&tokio::fs::read(&graph_catalog_path).await?)?;
        graph_catalog.validate().map_err(|error| {
            OnlineError::SnapshotConflict(format!("RDF dataset catalog is invalid: {error}"))
        })?;
        if graph_catalog.dataset_id != active.snapshot.dataset_id
            || graph_catalog.snapshot_id != active.snapshot.snapshot_id
        {
            return Err(OnlineError::SnapshotConflict(
                "RDF dataset catalog identity differs from the active snapshot".to_owned(),
            ));
        }
        let state = Arc::new(GraphAuthorizationState {
            active,
            graph_catalog: Arc::new(graph_catalog),
        });
        let mut cache = self.authorization.lock().await;
        if let Some(existing) = cache.get(&key)
            && same_active(&existing.active, &state.active)
        {
            return Ok(Arc::clone(existing));
        }
        cache.insert(key, Arc::clone(&state));
        Ok(state)
    }

    async fn semantic_state(
        self: Arc<Self>,
        tenant_id: Uuid,
        dataset_id: Uuid,
    ) -> Result<Arc<SemanticState>, OnlineError> {
        let key = (tenant_id, dataset_id);
        let load_lock = {
            let mut loads = self.semantic_loads.lock().await;
            Arc::clone(loads.entry(key).or_insert_with(|| Arc::new(Mutex::new(()))))
        };
        let _load_guard = load_lock.lock_owned().await;
        let active = self
            .catalog
            .clone()
            .get_active_serving_snapshot_owned(tenant_id, dataset_id)
            .await?;
        {
            let cache = self.semantic.lock().await;
            if let Some(existing) = cache.get(&key)
                && same_active(&existing.active, &active)
            {
                return Ok(Arc::clone(existing));
            }
        }
        let authorization = Arc::clone(&self)
            .authorization_state(tenant_id, dataset_id)
            .await?;
        if !same_active(&active, &authorization.active) {
            return Err(OnlineError::SnapshotConflict(
                "semantic and graph-authorization snapshots differ".to_owned(),
            ));
        }
        let root = self.snapshot_cache_root(&active);
        tokio::fs::create_dir_all(&root).await?;
        let manifest_path = root.join("snapshot-manifest.json");
        Arc::clone(&self)
            .materialize_cached(
                active.snapshot.manifest_object_key.clone(),
                active.snapshot.manifest_sha256.clone(),
                manifest_path.clone(),
            )
            .await?;
        let manifest = Arc::new(serde_json::from_slice::<ReferenceSnapshotManifest>(
            &tokio::fs::read(&manifest_path).await?,
        )?);
        if manifest.dataset_id != dataset_id
            || manifest.snapshot_id != active.snapshot.snapshot_id
            || manifest.dataset_namespace != active.identity_namespace
        {
            return Err(OnlineError::SnapshotConflict(
                "reference manifest identity differs from catalog".to_owned(),
            ));
        }
        let query_dataset_path = root.join("query-dataset.nq");
        let closure_path = root.join("closure.nt");
        let capability_path = root.join("graph-capabilities.json");
        let graph_catalog_path = root.join("rdf-dataset-catalog.json");
        let owl_signature_path = root.join("owl-signature.json");
        Arc::clone(&self)
            .materialize_snapshot_artifact(
                active.clone(),
                Arc::clone(&manifest),
                "reasoner/owl-signature.json".to_owned(),
                owl_signature_path.clone(),
            )
            .await?;
        Arc::clone(&self)
            .materialize_snapshot_artifact(
                active.clone(),
                Arc::clone(&manifest),
                "indexes/graph-capabilities.json".to_owned(),
                capability_path.clone(),
            )
            .await?;
        Arc::clone(&self)
            .materialize_snapshot_artifact(
                active.clone(),
                Arc::clone(&manifest),
                "reasoner/closure.nt".to_owned(),
                closure_path.clone(),
            )
            .await?;
        let capability_index: GraphCapabilityIndexFile =
            serde_json::from_slice(&tokio::fs::read(&capability_path).await?)?;
        let owl_signature: OwlSignature =
            serde_json::from_slice(&tokio::fs::read(&owl_signature_path).await?)?;
        if owl_signature.dataset_id != dataset_id
            || owl_signature.snapshot_id != active.snapshot.snapshot_id
        {
            return Err(OnlineError::SnapshotConflict(
                "OWL signature identity differs from the active snapshot".to_owned(),
            ));
        }
        let owl_signature_sha256 = manifest.owl_signature_sha256.clone().ok_or_else(|| {
            OnlineError::SnapshotConflict("Phase 40.7 requires owlSignatureSha256".to_owned())
        })?;
        if sha256_path_off_thread(owl_signature_path.clone()).await? != owl_signature_sha256 {
            return Err(OnlineError::SnapshotConflict(
                "OWL signature bytes differ from the snapshot manifest binding".to_owned(),
            ));
        }
        let datatype_policy_sha256 = manifest.datatype_policy_sha256.clone().ok_or_else(|| {
            OnlineError::SnapshotConflict("Phase 40.7 requires datatypePolicySha256".to_owned())
        })?;
        let owl_profile_qualification_sha256 = manifest
            .owl_profile_qualification_sha256
            .clone()
            .ok_or_else(|| {
                OnlineError::SnapshotConflict(
                    "Phase 40.7 requires owlProfileQualificationSha256".to_owned(),
                )
            })?;
        let owl_consistency_qualification_sha256 = manifest
            .owl_consistency_qualification_sha256
            .clone()
            .ok_or_else(|| {
                OnlineError::SnapshotConflict(
                    "Phase 40.7 requires owlConsistencyQualificationSha256".to_owned(),
                )
            })?;
        let owl_signature_index = OwlSignatureIndex {
            classes: owl_signature.classes.iter().cloned().collect(),
            object_properties: owl_signature.object_properties.iter().cloned().collect(),
            data_properties: owl_signature.data_properties.iter().cloned().collect(),
            annotation_properties: owl_signature
                .annotation_properties
                .iter()
                .cloned()
                .collect(),
            named_individuals: owl_signature.named_individuals.iter().cloned().collect(),
            datatypes: owl_signature.datatypes.iter().cloned().collect(),
        }
        .with_builtins();
        let capability_sha256 = sha256_path_off_thread(capability_path.clone()).await?;
        validate_capability_index(
            &active,
            &manifest,
            &capability_index,
            &capability_sha256,
            &hex::encode(Sha256::digest(tokio::fs::read(&graph_catalog_path).await?)),
            &authorization.graph_catalog,
        )?;
        let query_dataset_sha256 = manifest
            .artifacts
            .iter()
            .find(|artifact| artifact.relative_path == "data/query-dataset.nq")
            .ok_or_else(|| {
                OnlineError::SnapshotConflict(
                    "snapshot omits required artifact data/query-dataset.nq".to_owned(),
                )
            })?
            .sha256
            .clone();
        let state = Arc::new(SemanticState {
            active,
            manifest,
            manifest_path,
            query_dataset_path,
            query_dataset_sha256,
            closure_path,
            capability_index_sha256: capability_sha256,
            owl_signature: Arc::new(owl_signature_index),
            owl_signature_document: Arc::new(owl_signature),
            owl_signature_sha256,
            datatype_policy_sha256,
            owl_profile_qualification_sha256,
            owl_consistency_qualification_sha256,
            graph_catalog: Arc::clone(&authorization.graph_catalog),
            full_runtime: Mutex::new(None),
            full_runtime_load: Arc::new(Mutex::new(())),
            runtimes: Mutex::new(BoundedLruCache::new()),
            runtime_loads: Mutex::new(BTreeMap::new()),
            fragment_runtimes: Mutex::new(BoundedLruCache::new()),
            fragment_runtime_loads: Mutex::new(BTreeMap::new()),
            distributed_plans: Mutex::new(BoundedLruCache::new()),
            distributed_plan_loads: Mutex::new(BTreeMap::new()),
        });
        let mut cache = self.semantic.lock().await;
        if let Some(existing) = cache.get(&key)
            && same_active(&existing.active, &state.active)
        {
            return Ok(Arc::clone(existing));
        }
        cache.insert(key, Arc::clone(&state));
        Ok(state)
    }

    async fn full_runtime(
        self: Arc<Self>,
        state: Arc<SemanticState>,
    ) -> Result<Arc<CertifiedSemanticRuntime>, OnlineError> {
        if let Some(runtime) = state.full_runtime.lock().await.as_ref() {
            return Ok(Arc::clone(runtime));
        }
        let _load_guard = Arc::clone(&state.full_runtime_load).lock_owned().await;
        if let Some(runtime) = state.full_runtime.lock().await.as_ref() {
            return Ok(Arc::clone(runtime));
        }
        Arc::clone(&self)
            .materialize_snapshot_artifact(
                state.active.clone(),
                Arc::clone(&state.manifest),
                "data/query-dataset.nq".to_owned(),
                state.query_dataset_path.clone(),
            )
            .await?;
        let manifest_path = state.manifest_path.clone();
        let manifest_sha256 = state.active.snapshot.manifest_sha256.clone();
        let query_dataset_path = state.query_dataset_path.clone();
        let closure_path = state.closure_path.clone();
        let runtime = tokio::task::spawn_blocking(move || {
            CertifiedSemanticRuntime::open(
                &manifest_path,
                &manifest_sha256,
                &query_dataset_path,
                &closure_path,
            )
        })
        .await??;
        let runtime = Arc::new(runtime);
        *state.full_runtime.lock().await = Some(Arc::clone(&runtime));
        tracing::info!(
            snapshot_id = %state.active.snapshot.snapshot_id,
            query_dataset_sha256 = %state.query_dataset_sha256,
            "full exact scalar semantic runtime constructed"
        );
        Ok(runtime)
    }

    async fn routed_runtime(
        self: Arc<Self>,
        state: Arc<SemanticState>,
        query_sha256: String,
    ) -> Result<(Arc<CertifiedSemanticRuntime>, QueryRoutingCertificate), OnlineError> {
        let certificate = state
            .manifest
            .certified_queries
            .iter()
            .find(|query| query.query_sha256 == query_sha256)
            .ok_or(ReferenceRuntimeError::UncertifiedQuery)?;
        let routing = certificate.routing.clone().ok_or_else(|| {
            OnlineError::SnapshotConflict(
                "active query certificate has no relevant-graph routing proof".to_owned(),
            )
        })?;
        let load_lock = {
            let mut loads = state.runtime_loads.lock().await;
            Arc::clone(
                loads
                    .entry(query_sha256.to_owned())
                    .or_insert_with(|| Arc::new(Mutex::new(()))),
            )
        };
        let load_guard = load_lock.lock_owned().await;
        if let Some(runtime) = state.runtimes.lock().await.get(&query_sha256) {
            tracing::debug!(
                %query_sha256,
                snapshot_id = %state.active.snapshot.snapshot_id,
                selected_graph_count = routing.selected_graph_iris.len(),
                "certified routed runtime cache hit"
            );
            return Ok((runtime, routing));
        }
        let route_path = self
            .snapshot_cache_root(&state.active)
            .join("routes")
            .join(format!("{query_sha256}.nq"));
        Arc::clone(&self)
            .materialize_snapshot_artifact(
                state.active.clone(),
                Arc::clone(&state.manifest),
                routing.route_artifact_relative_path.clone(),
                route_path.clone(),
            )
            .await?;
        if sha256_path_off_thread(route_path.clone()).await? != routing.route_artifact_sha256
            || route_path.metadata()?.len() != routing.route_artifact_bytes
        {
            return Err(OnlineError::SnapshotConflict(
                "routed dataset differs from its query certificate".to_owned(),
            ));
        }
        let manifest_path = state.manifest_path.clone();
        let manifest_sha256 = state.active.snapshot.manifest_sha256.clone();
        let closure_path = state.closure_path.clone();
        let expected_query_sha256 = query_sha256.to_owned();
        let runtime = tokio::task::spawn_blocking(move || {
            CertifiedSemanticRuntime::open_routed(
                &manifest_path,
                &manifest_sha256,
                &expected_query_sha256,
                &route_path,
                &closure_path,
            )
        })
        .await??;
        let runtime = Arc::new(runtime);
        tracing::info!(
            %query_sha256,
            snapshot_id = %state.active.snapshot.snapshot_id,
            selected_graph_count = routing.selected_graph_iris.len(),
            total_graph_count = routing.total_graph_count,
            route_bytes = routing.route_artifact_bytes,
            selection_mode = %routing.selection_mode,
            "certified routed runtime constructed"
        );
        let evicted_query = state.runtimes.lock().await.insert(
            query_sha256.to_owned(),
            Arc::clone(&runtime),
            self.max_resident_query_routes,
        );
        drop(load_guard);
        if let Some(evicted_query) = evicted_query {
            tracing::info!(
                query_sha256 = %evicted_query,
                snapshot_id = %state.active.snapshot.snapshot_id,
                "routed runtime evicted from the bounded local cache"
            );
            Arc::clone(&self)
                .remove_evicted_route(Arc::clone(&state), evicted_query)
                .await;
        }
        Ok((runtime, routing))
    }

    async fn distributed_plan(
        self: Arc<Self>,
        state: Arc<SemanticState>,
        query_sha256: String,
    ) -> Result<Arc<DistributedQueryPlanFile>, OnlineError> {
        let load_lock = {
            let mut loads = state.distributed_plan_loads.lock().await;
            Arc::clone(
                loads
                    .entry(query_sha256.to_owned())
                    .or_insert_with(|| Arc::new(Mutex::new(()))),
            )
        };
        let _load_guard = load_lock.lock_owned().await;
        if let Some(plan) = state.distributed_plans.lock().await.get(&query_sha256) {
            return Ok(plan);
        }
        let distributed = state
            .manifest
            .certified_queries
            .iter()
            .find(|query| query.query_sha256 == query_sha256)
            .and_then(|query| query.routing.as_ref())
            .and_then(|routing| routing.distributed.as_ref())
            .cloned()
            .ok_or(ReferenceRuntimeError::UncertifiedQuery)?;
        let plan_path = self
            .snapshot_cache_root(&state.active)
            .join("plans")
            .join(format!("{query_sha256}.json"));
        Arc::clone(&self)
            .materialize_snapshot_artifact(
                state.active.clone(),
                Arc::clone(&state.manifest),
                distributed.plan_artifact_relative_path.clone(),
                plan_path.clone(),
            )
            .await?;
        if sha256_path_off_thread(plan_path.clone()).await? != distributed.plan_artifact_sha256
            || plan_path.metadata()?.len() != distributed.plan_artifact_bytes
        {
            return Err(OnlineError::SnapshotConflict(
                "distributed plan differs from its certificate".to_owned(),
            ));
        }
        let plan: DistributedQueryPlanFile =
            serde_json::from_slice(&tokio::fs::read(plan_path).await?)?;
        validate_distributed_plan(
            &state.active,
            &state.manifest,
            &query_sha256,
            &distributed,
            &plan,
        )?;
        let plan = Arc::new(plan);
        let evicted = state.distributed_plans.lock().await.insert(
            query_sha256.to_owned(),
            Arc::clone(&plan),
            self.max_resident_query_routes,
        );
        if let Some(evicted) = evicted {
            tracing::info!(query_sha256 = %evicted, "distributed plan evicted from parsed cache");
        }
        Ok(plan)
    }

    async fn fragment_runtime(
        self: Arc<Self>,
        state: Arc<SemanticState>,
        query_sha256: String,
        fragment_id: String,
    ) -> Result<Arc<CertifiedFragmentRuntime>, OnlineError> {
        let cache_key = format!("{query_sha256}:{fragment_id}");
        let load_lock = {
            let mut loads = state.fragment_runtime_loads.lock().await;
            Arc::clone(
                loads
                    .entry(cache_key.clone())
                    .or_insert_with(|| Arc::new(Mutex::new(()))),
            )
        };
        let load_guard = load_lock.lock_owned().await;
        if let Some(runtime) = state.fragment_runtimes.lock().await.get(&cache_key) {
            return Ok(runtime);
        }
        let plan = Arc::clone(&self)
            .distributed_plan(Arc::clone(&state), query_sha256.clone())
            .await?;
        let fragment = plan
            .fragments
            .iter()
            .find(|fragment| fragment.fragment_id == fragment_id)
            .cloned()
            .ok_or_else(|| {
                OnlineError::SnapshotConflict("fragment is absent from its plan".to_owned())
            })?;
        let root = self
            .snapshot_cache_root(&state.active)
            .join("fragments")
            .join(&query_sha256)
            .join(&fragment_id);
        let dataset_path = root.join("dataset.nq");
        let query_path = root.join("query.rq");
        Arc::clone(&self)
            .materialize_snapshot_artifact(
                state.active.clone(),
                Arc::clone(&state.manifest),
                fragment.dataset_artifact_relative_path.clone(),
                dataset_path.clone(),
            )
            .await?;
        Arc::clone(&self)
            .materialize_snapshot_artifact(
                state.active.clone(),
                Arc::clone(&state.manifest),
                fragment.query_artifact_relative_path.clone(),
                query_path.clone(),
            )
            .await?;
        let plan_path = self
            .snapshot_cache_root(&state.active)
            .join("plans")
            .join(format!("{query_sha256}.json"));
        let manifest_path = state.manifest_path.clone();
        let manifest_sha256 = state.active.snapshot.manifest_sha256.clone();
        let closure_path = state.closure_path.clone();
        let query_sha256_owned = query_sha256.to_owned();
        let fragment_id_owned = fragment_id.to_owned();
        let runtime = tokio::task::spawn_blocking(move || {
            CertifiedFragmentRuntime::open(
                &manifest_path,
                &manifest_sha256,
                &query_sha256_owned,
                &fragment_id_owned,
                &plan_path,
                &dataset_path,
                &query_path,
                &closure_path,
            )
        })
        .await??;
        let runtime = Arc::new(runtime);
        let evicted = state.fragment_runtimes.lock().await.insert(
            cache_key,
            Arc::clone(&runtime),
            self.max_resident_fragment_runtimes,
        );
        drop(load_guard);
        if let Some(evicted) = evicted {
            tracing::info!(cache_key = %evicted, "fragment runtime evicted");
            Arc::clone(&self)
                .remove_evicted_fragment(Arc::clone(&state), evicted)
                .await;
        }
        Ok(runtime)
    }

    async fn remove_evicted_fragment(
        self: Arc<Self>,
        state: Arc<SemanticState>,
        cache_key: String,
    ) {
        let Some((query_sha256, fragment_id)) = cache_key.split_once(':') else {
            tracing::error!(%cache_key, "invalid fragment cache key was evicted");
            return;
        };
        let load_lock = {
            let mut loads = state.fragment_runtime_loads.lock().await;
            Arc::clone(
                loads
                    .entry(cache_key.clone())
                    .or_insert_with(|| Arc::new(Mutex::new(()))),
            )
        };
        let _load_guard = load_lock.lock_owned().await;
        if state.fragment_runtimes.lock().await.contains(&cache_key) {
            return;
        }
        let evicted_path = self
            .snapshot_cache_root(&state.active)
            .join("fragments")
            .join(query_sha256)
            .join(fragment_id);
        match tokio::fs::remove_dir_all(&evicted_path).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                tracing::warn!(
                    %error,
                    path = %evicted_path.display(),
                    "evicted fragment artifacts could not be removed from the local cache"
                );
            }
        }
    }

    async fn remove_evicted_route(
        self: Arc<Self>,
        state: Arc<SemanticState>,
        query_sha256: String,
    ) {
        let load_lock = {
            let mut loads = state.runtime_loads.lock().await;
            Arc::clone(
                loads
                    .entry(query_sha256.clone())
                    .or_insert_with(|| Arc::new(Mutex::new(()))),
            )
        };
        let _load_guard = load_lock.lock_owned().await;
        if state.runtimes.lock().await.contains(&query_sha256) {
            return;
        }
        let evicted_path = self
            .snapshot_cache_root(&state.active)
            .join("routes")
            .join(format!("{query_sha256}.nq"));
        match tokio::fs::remove_file(&evicted_path).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                tracing::warn!(
                    %error,
                    path = %evicted_path.display(),
                    "evicted routed dataset could not be removed from the local cache"
                );
            }
        }
    }

    async fn physical_state(
        self: Arc<Self>,
        tenant_id: Uuid,
        dataset_id: Uuid,
    ) -> Result<Arc<PhysicalState>, OnlineError> {
        let key = (tenant_id, dataset_id);
        let load_lock = {
            let mut loads = self.physical_loads.lock().await;
            Arc::clone(loads.entry(key).or_insert_with(|| Arc::new(Mutex::new(()))))
        };
        let _load_guard = load_lock.lock_owned().await;
        let active = self
            .catalog
            .clone()
            .get_active_serving_snapshot_owned(tenant_id, dataset_id)
            .await?;
        {
            let cache = self.physical.lock().await;
            if let Some(existing) = cache.get(&key)
                && same_active(&existing.active, &active)
            {
                return Ok(Arc::clone(existing));
            }
        }
        let authorization = Arc::clone(&self)
            .authorization_state(tenant_id, dataset_id)
            .await?;
        if !same_active(&active, &authorization.active) {
            return Err(OnlineError::SnapshotConflict(
                "physical and graph-authorization snapshots differ".to_owned(),
            ));
        }
        let root = self.snapshot_cache_root(&active);
        tokio::fs::create_dir_all(&root).await?;
        let serving_path = root.join("serving-root.json");
        Arc::clone(&self)
            .materialize_cached(
                required_serving_root(&active)?
                    .serving_root_object_key
                    .clone(),
                required_serving_root(&active)?.serving_root_sha256.clone(),
                serving_path.clone(),
            )
            .await?;
        let manifest: ServingRootManifest =
            serde_json::from_slice(&tokio::fs::read(&serving_path).await?)?;
        manifest.validate()?;
        validate_serving_manifest(&active, &manifest)?;
        let locator_path = root.join("locator.bin");
        let dictionary_path = root.join("dictionary.tsv");
        Arc::clone(&self)
            .materialize_cached(
                manifest.binary_locator_object_key.clone(),
                manifest.binary_locator_sha256.clone(),
                locator_path.clone(),
            )
            .await?;
        Arc::clone(&self)
            .materialize_cached(
                manifest.dictionary_object_key.clone(),
                manifest.dictionary_sha256.clone(),
                dictionary_path.clone(),
            )
            .await?;
        let binary_sha = manifest.binary_locator_sha256.clone();
        let source_sha = manifest.source_locator_sha256.clone();
        let snapshot_id = manifest.snapshot_id;
        let locator = tokio::task::spawn_blocking(move || {
            MmapLocatorIndex::open(&locator_path, &binary_sha, snapshot_id, &source_sha)
        })
        .await??;
        if u64::try_from(locator.record_count()).ok() != Some(manifest.locator_record_count) {
            return Err(OnlineError::SnapshotConflict(
                "mmap locator count differs from serving root".to_owned(),
            ));
        }
        let dictionary =
            tokio::task::spawn_blocking(move || read_iri_dictionary(&dictionary_path)).await??;
        let state = Arc::new(PhysicalState {
            active,
            manifest,
            locator: Arc::new(locator),
            dictionary: Arc::new(dictionary),
            authorization,
            payloads: Mutex::new(BTreeMap::new()),
            payload_load: Arc::new(Mutex::new(())),
        });
        let mut cache = self.physical.lock().await;
        if let Some(existing) = cache.get(&key)
            && same_active(&existing.active, &state.active)
        {
            return Ok(Arc::clone(existing));
        }
        cache.insert(key, Arc::clone(&state));
        Ok(state)
    }

    async fn payload_shards(
        self: Arc<Self>,
        state: Arc<PhysicalState>,
        required: BTreeSet<u32>,
    ) -> Result<BTreeMap<u32, VerifiedPayloadShard>, OnlineError> {
        // Serialize population without holding the payload map across object-store or
        // filesystem awaits. Readers only take the map lock long enough to clone an
        // immutable, checksum-verified shard.
        let _load_guard = Arc::clone(&state.payload_load).lock_owned().await;
        for partition_index in &required {
            if state.payloads.lock().await.contains_key(partition_index) {
                continue;
            }
            let partition = state
                .manifest
                .partitions
                .get(usize::try_from(*partition_index).map_err(|_| {
                    OnlineError::SnapshotConflict("partition index overflow".to_owned())
                })?)
                .filter(|value| value.partition_index == *partition_index)
                .ok_or_else(|| {
                    OnlineError::SnapshotConflict(
                        "locator references a partition absent from the serving root".to_owned(),
                    )
                })?;
            let next_bytes = state.payloads.lock().await.values().try_fold(
                partition.payload_bytes,
                |total, shard| {
                    total.checked_add(shard.byte_count()).ok_or_else(|| {
                        OnlineError::SnapshotConflict(
                            "payload cache byte count overflow".to_owned(),
                        )
                    })
                },
            )?;
            if next_bytes > self.max_payload_cache_bytes {
                return Err(OnlineError::Request(
                    "payload cache ceiling would be exceeded".to_owned(),
                ));
            }
            let path = self
                .snapshot_cache_root(&state.active)
                .join(format!("payload-{partition_index:05}.parquet"));
            Arc::clone(&self)
                .materialize_cached(
                    partition.payload_object_key.clone(),
                    partition.payload_sha256.clone(),
                    path.clone(),
                )
                .await?;
            if path.metadata()?.len() != partition.payload_bytes {
                return Err(OnlineError::SnapshotConflict(
                    "payload byte count differs from serving root".to_owned(),
                ));
            }
            let shard = verify_payload_shard(*partition_index, &path, &partition.payload_sha256)?;
            state.payloads.lock().await.insert(*partition_index, shard);
        }
        let cache = state.payloads.lock().await;
        Ok(required
            .iter()
            .filter_map(|index| cache.get(index).cloned().map(|shard| (*index, shard)))
            .collect())
    }

    async fn materialize_snapshot_artifact(
        self: Arc<Self>,
        active: ActiveServingSnapshot,
        manifest: Arc<ReferenceSnapshotManifest>,
        relative_path: String,
        destination: PathBuf,
    ) -> Result<(), OnlineError> {
        let artifact = manifest
            .artifacts
            .iter()
            .find(|artifact| artifact.relative_path == relative_path)
            .ok_or_else(|| {
                OnlineError::SnapshotConflict(format!(
                    "snapshot omits required artifact {relative_path}"
                ))
            })?;
        let prefix = object_parent(&active.snapshot.manifest_object_key)?;
        let key = format!("{prefix}/{relative_path}");
        Arc::clone(&self)
            .materialize_cached(key, artifact.sha256.clone(), destination.clone())
            .await?;
        if destination.metadata()?.len() != artifact.bytes {
            return Err(OnlineError::SnapshotConflict(format!(
                "artifact size differs for {relative_path}"
            )));
        }
        Ok(())
    }

    async fn semantic_partition_files(
        self: Arc<Self>,
        active: &ActiveServingSnapshot,
        storage_partition: u32,
    ) -> Result<SemanticPartitionFiles, OnlineError> {
        let activation = active.cloud_activation.as_ref().ok_or_else(|| {
            OnlineError::SnapshotConflict(
                "partition-native traversal requires a cloud semantic activation".to_owned(),
            )
        })?;
        if storage_partition
            >= u32::try_from(activation.semantic_partition_count).map_err(|_| {
                OnlineError::SnapshotConflict("semantic partition count is invalid".to_owned())
            })?
        {
            return Err(OnlineError::Request(
                "property-path storage partition is outside the active layout".to_owned(),
            ));
        }
        let root = self.snapshot_cache_root(active).join("partition-native");
        tokio::fs::create_dir_all(&root).await?;
        let semantic_root_path = root.join("semantic-compilation-root.json");
        Arc::clone(&self)
            .materialize_cached(
                activation.semantic_root_object_key.clone(),
                activation.semantic_root_sha256.clone(),
                semantic_root_path.clone(),
            )
            .await?;
        let semantic: SemanticCompilationRootView =
            serde_json::from_slice(&tokio::fs::read(&semantic_root_path).await?)?;
        if semantic.format_version != 1
            || semantic.tenant_id != active.snapshot.tenant_id
            || semantic.dataset_id != active.snapshot.dataset_id
            || semantic.snapshot_id != active.snapshot.snapshot_id
            || semantic.semantic_content_sha256 != activation.semantic_content_sha256
            || semantic.edge_count != semantic.fact_count
            || semantic.logical_partitions
                != u32::try_from(activation.semantic_partition_count).map_err(|_| {
                    OnlineError::SnapshotConflict(
                        "semantic partition count exceeds this platform".to_owned(),
                    )
                })?
            || semantic.partitions.len()
                != usize::try_from(semantic.logical_partitions).unwrap_or(usize::MAX)
        {
            return Err(OnlineError::SnapshotConflict(
                "semantic compilation root differs from the active snapshot".to_owned(),
            ));
        }
        let reference = semantic
            .partitions
            .get(usize::try_from(storage_partition).map_err(|_| {
                OnlineError::SnapshotConflict("semantic partition index overflow".to_owned())
            })?)
            .filter(|reference| reference.partition_index == storage_partition)
            .ok_or_else(|| {
                OnlineError::SnapshotConflict(
                    "semantic compilation root has a missing or reordered partition".to_owned(),
                )
            })?;
        let partition_root = root.join(format!("partition-{storage_partition:05}"));
        tokio::fs::create_dir_all(&partition_root).await?;
        let manifest_path = partition_root.join("semantic-partition.json");
        Arc::clone(&self)
            .materialize_cached(
                reference.manifest_path.clone(),
                reference.manifest_sha256.clone(),
                manifest_path.clone(),
            )
            .await?;
        let manifest: SemanticPartitionManifestView =
            serde_json::from_slice(&tokio::fs::read(&manifest_path).await?)?;
        if manifest.format_version != 1
            || manifest.dataset_id != active.snapshot.dataset_id
            || manifest.snapshot_id != active.snapshot.snapshot_id
            || manifest.partition_index != storage_partition
            || manifest.partition_id != reference.partition_id
            || manifest.dictionary_sha256.is_empty()
            || manifest.edge_count != manifest.fact_count
        {
            return Err(OnlineError::SnapshotConflict(
                "semantic partition manifest has a different immutable identity".to_owned(),
            ));
        }
        let manifest_prefix = object_parent(&reference.manifest_path)?;
        let facts_artifact = required_semantic_artifact(&manifest.artifacts, "facts.parquet")?;
        let forward_artifact =
            required_semantic_artifact(&manifest.artifacts, "adjacency-forward.tsv")?;
        let reverse_artifact =
            required_semantic_artifact(&manifest.artifacts, "adjacency-reverse.tsv")?;
        let facts_path = partition_root.join("facts.parquet");
        let forward_path = partition_root.join("adjacency-forward.tsv");
        let reverse_path = partition_root.join("adjacency-reverse.tsv");
        Arc::clone(&self)
            .materialize_cached(
                format!("{manifest_prefix}/{}", facts_artifact.relative_path),
                facts_artifact.sha256.clone(),
                facts_path.clone(),
            )
            .await?;
        Arc::clone(&self)
            .materialize_cached(
                format!("{manifest_prefix}/{}", forward_artifact.relative_path),
                forward_artifact.sha256.clone(),
                forward_path.clone(),
            )
            .await?;
        Arc::clone(&self)
            .materialize_cached(
                format!("{manifest_prefix}/{}", reverse_artifact.relative_path),
                reverse_artifact.sha256.clone(),
                reverse_path.clone(),
            )
            .await?;

        let dictionary_manifest_path = root.join("dictionary-manifest.json");
        Arc::clone(&self)
            .materialize_cached(
                semantic.dictionary_manifest_path.clone(),
                semantic.dictionary_manifest_sha256.clone(),
                dictionary_manifest_path.clone(),
            )
            .await?;
        let dictionary: SemanticDictionaryManifestView =
            serde_json::from_slice(&tokio::fs::read(&dictionary_manifest_path).await?)?;
        if dictionary.format_version != 1
            || dictionary.dataset_id != active.snapshot.dataset_id
            || dictionary.snapshot_id != active.snapshot.snapshot_id
            || dictionary.dictionary_sha256 != manifest.dictionary_sha256
        {
            return Err(OnlineError::SnapshotConflict(
                "semantic dictionary differs from the active partition".to_owned(),
            ));
        }
        let dictionary_prefix = object_parent(&semantic.dictionary_manifest_path)?;
        let dictionary_path = root.join("dictionary.tsv");
        Arc::clone(&self)
            .materialize_cached(
                format!("{dictionary_prefix}/dictionary.tsv"),
                dictionary.dictionary_sha256.clone(),
                dictionary_path.clone(),
            )
            .await?;
        Ok(SemanticPartitionFiles {
            semantic_root_sha256: activation.semantic_root_sha256.clone(),
            partition_manifest_sha256: reference.manifest_sha256.clone(),
            facts_path,
            facts_sha256: facts_artifact.sha256.clone(),
            facts_bytes: facts_artifact.bytes,
            facts_rows: facts_artifact.row_count,
            forward: adjacency_identity(forward_path, forward_artifact),
            reverse: adjacency_identity(reverse_path, reverse_artifact),
            dictionary_path,
            dictionary_sha256: dictionary.dictionary_sha256,
        })
    }

    async fn materialize_cached(
        self: Arc<Self>,
        object_key: String,
        sha256: String,
        destination: PathBuf,
    ) -> Result<(), OnlineError> {
        if destination.exists() {
            if destination.metadata()?.len() <= self.max_object_bytes
                && sha256_path_off_thread(destination.clone()).await? == sha256
            {
                return Ok(());
            }
            tracing::warn!(
                path = %destination.display(),
                "corrupt local cache entry will be replaced from immutable object storage"
            );
            tokio::fs::remove_file(&destination).await?;
        }
        self.store
            .materialize_verified(&object_key, &sha256, self.max_object_bytes, &destination)
            .await?;
        Ok(())
    }

    fn snapshot_cache_root(&self, active: &ActiveServingSnapshot) -> PathBuf {
        self.cache_root
            .join(active.snapshot.tenant_id.to_string())
            .join(active.snapshot.dataset_id.to_string())
            .join(active.snapshot.snapshot_id.to_string())
    }
}

/// Hash a potentially multi-gigabyte artifact outside Tokio's cooperative runtime.
/// The bounded blocking pool prevents one cache validation from stalling unrelated
/// API, MCP, heartbeat, or cancellation futures on the same query replica.
async fn sha256_path_off_thread(path: PathBuf) -> Result<String, OnlineError> {
    Ok(tokio::task::spawn_blocking(move || sha256_path(&path)).await??)
}

fn qualify_entities(
    result: &CertifiedSemanticResult,
    namespace: Uuid,
) -> Result<Vec<QualifiedEntity>, OnlineError> {
    let mut output = Vec::with_capacity(result.qualified_entity_iris.len());
    let mut iri_by_guid = BTreeMap::new();
    for (ordinal, iri) in result.qualified_entity_iris.iter().enumerate() {
        let guid = guid_for_canonical_iri(namespace, iri)?;
        if iri_by_guid.insert(guid, iri.as_str()).is_some() {
            return Err(OnlineError::SnapshotConflict(
                "two qualified IRIs resolve to one GUID".to_owned(),
            ));
        }
        output.push(QualifiedEntity {
            query_ordinal: u64::try_from(ordinal).map_err(|_| {
                OnlineError::Request("qualified entity ordinal overflow".to_owned())
            })?,
            iri: iri.clone(),
            guid,
            multiplicity: 1,
        });
    }
    Ok(output)
}

fn validate_entity_request(
    entities: &[QualifiedEntity],
    maximum: usize,
) -> Result<(), OnlineError> {
    let mut ordinals = BTreeSet::new();
    if entities.is_empty()
        || entities.len() > maximum
        || entities.iter().any(|entity| {
            entity.iri.is_empty()
                || entity.multiplicity == 0
                || !ordinals.insert(entity.query_ordinal)
        })
    {
        return Err(OnlineError::Request(
            "qualified entity envelope is invalid or over budget".to_owned(),
        ));
    }
    Ok(())
}

fn verify_qualified_identities(
    entities: &[QualifiedEntity],
    namespace: Uuid,
) -> Result<(), OnlineError> {
    for entity in entities {
        if guid_for_canonical_iri(namespace, &entity.iri)? != entity.guid {
            return Err(OnlineError::SnapshotConflict(
                "qualified IRI and GUID do not match the active namespace".to_owned(),
            ));
        }
    }
    Ok(())
}

fn validate_hydrated_rows(
    rows: &[OnlinePayloadRow],
    entities: &[QualifiedEntity],
    maximum: u64,
    authorized_graph_iris: &BTreeSet<String>,
) -> Result<(), OnlineError> {
    if u64::try_from(rows.len()).map_or(true, |count| count > maximum) {
        return Err(OnlineError::Upstream(
            "hydration response exceeds the configured row ceiling".to_owned(),
        ));
    }
    let expected = entities
        .iter()
        .map(|entity| (entity.query_ordinal, entity))
        .collect::<BTreeMap<_, _>>();
    if rows.iter().any(|row| {
        !authorized_graph_iris.contains(&row.graph_iri)
            || expected.get(&row.query_ordinal).is_none_or(|entity| {
                row.entity_guid != entity.guid
                    || row.multiplicity != entity.multiplicity
                    || match row.subject_resource_kind {
                        ngkg_hydration::RdfResourceKind::NamedNode => {
                            row.subject_term != entity.iri
                        }
                        ngkg_hydration::RdfResourceKind::BlankNode => true,
                    }
            })
    }) {
        return Err(OnlineError::Upstream(
            "hydration response contains an unqualified entity binding".to_owned(),
        ));
    }
    Ok(())
}

fn require_requested_snapshot(
    requested: Option<Uuid>,
    active: &ActiveServingSnapshot,
) -> Result<(), OnlineError> {
    if requested.is_some_and(|value| value != active.snapshot.snapshot_id) {
        return Err(OnlineError::SnapshotConflict(
            "only the active published snapshot may be queried".to_owned(),
        ));
    }
    Ok(())
}

fn authorize_query_graphs(
    identity: &Identity,
    semantic: &SemanticState,
    routing: &QueryRoutingCertificate,
) -> Result<AuthorizedQueryGraphs, OnlineError> {
    let authorized = authorized_service_graphs(identity, &semantic.graph_catalog)?;
    if routing
        .selected_graph_iris
        .iter()
        .any(|iri| !authorized.graph_iris.contains(iri))
    {
        return Err(OnlineError::GraphForbidden);
    }
    // Phase 37's finite closure is compiled from every reasoning-visible source
    // graph. Until Phase 41 emits proof-filtered closure partitions, a principal
    // must be authorized for every such graph or the shared closure could leak an
    // inference from a forbidden domain. Failing closed preserves exactness.
    require_reasoning_graph_authorization(identity, &semantic.graph_catalog)?;
    Ok(authorized)
}

fn require_reasoning_graph_authorization(
    identity: &Identity,
    graph_catalog: &GraphCatalog,
) -> Result<(), OnlineError> {
    if graph_catalog
        .graphs
        .iter()
        .filter(|graph| graph.reasoning_visible)
        .any(|graph| {
            graph
                .authorization_labels
                .is_disjoint(&identity.graph_authorization_labels)
        })
    {
        return Err(OnlineError::GraphForbidden);
    }
    Ok(())
}

fn resolve_request_dataset(
    graph_catalog: &GraphCatalog,
    principal_labels: &BTreeSet<String>,
    query: &QueryDatasetSpecification,
    protocol: &ProtocolDatasetSpecification,
) -> Result<ResolvedDataset, OnlineError> {
    resolve_dataset(graph_catalog, principal_labels, query, protocol).map_err(|error| match error {
        DatasetError::ForbiddenRequestedGraph(_) | DatasetError::EmptyAuthorizedDataset => {
            OnlineError::GraphForbidden
        }
        other => OnlineError::Request(other.to_string()),
    })
}

fn graph_iris_for_ids(
    graph_catalog: &GraphCatalog,
    graph_ids: &[u32],
) -> Result<Vec<String>, OnlineError> {
    graph_ids
        .iter()
        .map(|graph_id| {
            let graph = graph_catalog.by_id(*graph_id).ok_or_else(|| {
                OnlineError::SnapshotConflict(
                    "active dataset references an unknown graph ID".to_owned(),
                )
            })?;
            let LogicalGraphName::Named { iri } = &graph.name else {
                return Err(OnlineError::SnapshotConflict(
                    "active dataset references the physical source default graph".to_owned(),
                ));
            };
            Ok(iri.clone())
        })
        .collect()
}

fn authorized_service_graphs(
    identity: &Identity,
    graph_catalog: &GraphCatalog,
) -> Result<AuthorizedQueryGraphs, OnlineError> {
    let active = resolve_dataset(
        graph_catalog,
        &identity.graph_authorization_labels,
        &QueryDatasetSpecification::default(),
        &ProtocolDatasetSpecification::default(),
    )
    .map_err(|_| OnlineError::GraphForbidden)?;
    if active.named_graph_ids.is_empty() {
        return Err(OnlineError::GraphForbidden);
    }
    let allowed_iris = graph_catalog
        .graphs
        .iter()
        .filter(|graph| {
            active
                .named_graph_ids
                .binary_search(&graph.graph_id)
                .is_ok()
        })
        .filter_map(|graph| match &graph.name {
            LogicalGraphName::Named { iri } => Some(iri.as_str()),
            LogicalGraphName::Default => None,
        })
        .collect::<BTreeSet<_>>();
    Ok(AuthorizedQueryGraphs {
        graph_set_sha256: active.authorized_graph_set_sha256,
        graph_iris: allowed_iris.into_iter().map(ToOwned::to_owned).collect(),
    })
}

fn authorized_physical_graph_ids(
    dictionary: &BTreeMap<u64, String>,
    authorized_graph_iris: &BTreeSet<String>,
) -> BTreeSet<u64> {
    dictionary
        .iter()
        .filter_map(|(id, term)| authorized_graph_iris.contains(term).then_some(*id))
        .collect()
}

fn require_physical_request(
    snapshot_id: Uuid,
    serving_root_sha256: &str,
    active: &ActiveServingSnapshot,
) -> Result<(), OnlineError> {
    if snapshot_id != active.snapshot.snapshot_id
        || serving_root_sha256 != required_serving_root(active)?.serving_root_sha256
    {
        return Err(OnlineError::SnapshotConflict(
            "physical request is stale or addresses another serving root".to_owned(),
        ));
    }
    Ok(())
}

fn required_serving_root(
    active: &ActiveServingSnapshot,
) -> Result<&DistributedServingRoot, OnlineError> {
    active.serving_root.as_ref().ok_or_else(|| {
        OnlineError::Upstream(
            "the active cloud snapshot supports semantic querying but has no Phase 19 hydration layout"
                .to_owned(),
        )
    })
}

fn semantic_serving_identity(active: &ActiveServingSnapshot) -> Result<String, OnlineError> {
    if let Some(root) = &active.serving_root {
        return Ok(root.serving_root_sha256.clone());
    }
    active
        .cloud_activation
        .as_ref()
        .map(|activation| activation.activation_manifest_sha256.clone())
        .ok_or_else(|| {
            OnlineError::SnapshotConflict(
                "active snapshot has no checksum-bound semantic serving identity".to_owned(),
            )
        })
}

fn required_semantic_artifact<'a>(
    artifacts: &'a [SemanticRunArtifactView],
    relative_path: &str,
) -> Result<&'a SemanticRunArtifactView, OnlineError> {
    artifacts
        .iter()
        .find(|artifact| artifact.relative_path == relative_path)
        .ok_or_else(|| {
            OnlineError::SnapshotConflict(format!("semantic partition omits {relative_path}"))
        })
}

fn adjacency_identity(
    path: PathBuf,
    artifact: &SemanticRunArtifactView,
) -> AdjacencyArtifactIdentity {
    AdjacencyArtifactIdentity {
        path,
        sha256: artifact.sha256.clone(),
        bytes: artifact.bytes,
        rows: artifact.row_count,
    }
}

fn same_active(left: &ActiveServingSnapshot, right: &ActiveServingSnapshot) -> bool {
    left.snapshot.snapshot_id == right.snapshot.snapshot_id
        && left.snapshot.manifest_sha256 == right.snapshot.manifest_sha256
        && left
            .serving_root
            .as_ref()
            .map(|root| &root.serving_root_sha256)
            == right
                .serving_root
                .as_ref()
                .map(|root| &root.serving_root_sha256)
        && left
            .serving_certification
            .as_ref()
            .map(|certificate| &certificate.report_sha256)
            == right
                .serving_certification
                .as_ref()
                .map(|certificate| &certificate.report_sha256)
        && left
            .cloud_activation
            .as_ref()
            .map(|activation| &activation.activation_manifest_sha256)
            == right
                .cloud_activation
                .as_ref()
                .map(|activation| &activation.activation_manifest_sha256)
}

fn validate_capability_index(
    active: &ActiveServingSnapshot,
    manifest: &ReferenceSnapshotManifest,
    index: &GraphCapabilityIndexFile,
    capability_sha256: &str,
    graph_catalog_sha256: &str,
    graph_catalog: &GraphCatalog,
) -> Result<(), OnlineError> {
    if index.format_version != 2
        || index.dataset_id != active.snapshot.dataset_id
        || index.snapshot_id != active.snapshot.snapshot_id
        || index.graph_catalog_sha256 != graph_catalog_sha256
        || index.graphs.is_empty()
    {
        return Err(OnlineError::SnapshotConflict(
            "graph capability index identity or format is invalid".to_owned(),
        ));
    }
    let graph_iris = index
        .graphs
        .iter()
        .map(|graph| {
            if graph.graph_id == 0
                || graph.graph_iri.is_empty()
                || graph.authorization_labels.is_empty()
            {
                return Err(OnlineError::SnapshotConflict(
                    "graph capability records must reference named catalog graphs".to_owned(),
                ));
            }
            Ok(graph.graph_iri.clone())
        })
        .collect::<Result<BTreeSet<_>, OnlineError>>()?;
    if graph_iris.len() != index.graphs.len() {
        return Err(OnlineError::SnapshotConflict(
            "graph capability index contains duplicate graph IRIs".to_owned(),
        ));
    }
    let catalog_graphs = graph_catalog
        .graphs
        .iter()
        .filter_map(|graph| match &graph.name {
            LogicalGraphName::Named { iri } if graph.query_visible => Some((iri, graph)),
            LogicalGraphName::Default | LogicalGraphName::Named { .. } => None,
        })
        .collect::<BTreeMap<_, _>>();
    if catalog_graphs
        .keys()
        .map(|iri| (*iri).clone())
        .collect::<BTreeSet<_>>()
        != graph_iris
    {
        return Err(OnlineError::SnapshotConflict(
            "graph capability index does not cover every query-visible catalog graph".to_owned(),
        ));
    }
    for graph in &index.graphs {
        let catalog = catalog_graphs.get(&graph.graph_iri).ok_or_else(|| {
            OnlineError::SnapshotConflict(
                "capability graph is absent from the RDF dataset catalog".to_owned(),
            )
        })?;
        if graph.graph_id != catalog.graph_id
            || graph.role != catalog.role
            || graph.authorization_labels != catalog.authorization_labels
            || graph.reasoning_visible != catalog.reasoning_visible
            || graph.queryable_fact_count > catalog.asserted_quad_count
        {
            return Err(OnlineError::SnapshotConflict(
                "capability graph metadata differs from the RDF dataset catalog".to_owned(),
            ));
        }
    }
    let canonical_graphs = graph_iris.iter().cloned().collect::<Vec<_>>();
    if index
        .graphs
        .iter()
        .map(|graph| graph.graph_iri.clone())
        .collect::<Vec<_>>()
        != canonical_graphs
    {
        return Err(OnlineError::SnapshotConflict(
            "graph capability records are not in canonical IRI order".to_owned(),
        ));
    }
    validate_capability_map(&index.predicate_to_graphs, &graph_iris, false)?;
    validate_capability_map(&index.class_to_graphs, &graph_iris, false)?;
    validate_capability_map(&index.dependencies, &graph_iris, true)?;
    if index.dependencies.keys().cloned().collect::<BTreeSet<_>>() != graph_iris {
        return Err(OnlineError::SnapshotConflict(
            "graph dependency index does not cover every named graph".to_owned(),
        ));
    }
    let capability_artifact = manifest
        .artifacts
        .iter()
        .find(|artifact| artifact.relative_path == "indexes/graph-capabilities.json")
        .ok_or_else(|| {
            OnlineError::SnapshotConflict("snapshot omits the graph capability artifact".to_owned())
        })?;
    if capability_artifact.sha256 != capability_sha256 {
        return Err(OnlineError::SnapshotConflict(
            "graph capability checksum differs from the snapshot".to_owned(),
        ));
    }
    for query in &manifest.certified_queries {
        require_supported_result_hash_version(query.result_hash_version)?;
        let routing = query.routing.as_ref().ok_or_else(|| {
            OnlineError::SnapshotConflict(
                "certified query omits its relevant-graph routing proof".to_owned(),
            )
        })?;
        let expected_relative_path = format!("data/routes/{}.nq", query.query_sha256);
        let selected = routing
            .selected_graph_iris
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();
        let query_limits_are_valid = query.max_solution_rows > 0
            && query.max_graph_triples > 0
            && query.max_graph_blank_nodes > 0;
        let result_hashes_are_valid = is_sha256(&query.observed_result_sha256)
            && match query.query_form {
                QueryForm::Select => query
                    .observed_multiset_sha256
                    .as_deref()
                    .is_some_and(is_sha256),
                QueryForm::Ask | QueryForm::Construct | QueryForm::Describe => {
                    query.observed_multiset_sha256.is_none()
                }
            };
        if routing.format_version != 1
            || routing.capability_index_sha256 != capability_sha256
            || usize::try_from(routing.total_graph_count).ok() != Some(graph_iris.len())
            || selected.len() != routing.selected_graph_iris.len()
            || !selected.is_subset(&graph_iris)
            || routing.selected_graph_iris != selected.iter().cloned().collect::<Vec<_>>()
            || routing.route_artifact_relative_path != expected_relative_path
            || routing.routed_result_sha256 != query.observed_result_sha256
            || routing.routed_multiset_sha256 != query.observed_multiset_sha256
            || !is_sha256(&routing.active_dataset_sha256)
            || !query_limits_are_valid
            || !result_hashes_are_valid
            || !matches!(
                routing.selection_mode.as_str(),
                "typed_active_dataset"
                    | "typed_declared_graph"
                    | "typed_property_path_full_active_default"
                    | "typed_active_default_no_capability"
                    | "typed_capability_dependency"
                    | "typed_active_dataset_fallback"
            )
            || (query.query_form != QueryForm::Select && routing.distributed.is_some())
        {
            return Err(OnlineError::SnapshotConflict(
                "query routing proof is inconsistent with the capability index".to_owned(),
            ));
        }
        let route_artifact = manifest
            .artifacts
            .iter()
            .find(|artifact| artifact.relative_path == routing.route_artifact_relative_path)
            .ok_or_else(|| {
                OnlineError::SnapshotConflict(
                    "query route artifact is absent from the snapshot".to_owned(),
                )
            })?;
        if route_artifact.sha256 != routing.route_artifact_sha256
            || route_artifact.bytes != routing.route_artifact_bytes
        {
            return Err(OnlineError::SnapshotConflict(
                "query route artifact differs from its certificate".to_owned(),
            ));
        }
        if let Some(distributed) = &routing.distributed {
            let expected_plan = format!("plans/distributed/{}.json", query.query_sha256);
            let plan_artifact = manifest
                .artifacts
                .iter()
                .find(|artifact| artifact.relative_path == distributed.plan_artifact_relative_path)
                .ok_or_else(|| {
                    OnlineError::SnapshotConflict(
                        "distributed plan artifact is absent from the snapshot".to_owned(),
                    )
                })?;
            if distributed.format_version != 1
                || distributed.fragment_count < 2
                || distributed.plan_artifact_relative_path != expected_plan
                || !is_sha256(&distributed.plan_artifact_sha256)
                || !is_sha256(&distributed.distributed_multiset_sha256)
                || distributed.plan_artifact_bytes == 0
                || plan_artifact.sha256 != distributed.plan_artifact_sha256
                || plan_artifact.bytes != distributed.plan_artifact_bytes
            {
                return Err(OnlineError::SnapshotConflict(
                    "distributed certificate differs from the immutable snapshot".to_owned(),
                ));
            }
        }
    }
    Ok(())
}

fn validate_distributed_plan(
    active: &ActiveServingSnapshot,
    manifest: &ReferenceSnapshotManifest,
    query_sha256: &str,
    certificate: &DistributedQueryCertificate,
    plan: &DistributedQueryPlanFile,
) -> Result<(), OnlineError> {
    let query = manifest
        .certified_queries
        .iter()
        .find(|query| query.query_sha256 == query_sha256)
        .ok_or(ReferenceRuntimeError::UncertifiedQuery)?;
    let routing = query.routing.as_ref().ok_or_else(|| {
        OnlineError::SnapshotConflict("distributed plan has no routing proof".to_owned())
    })?;
    let bound_certificate = routing.distributed.as_ref().ok_or_else(|| {
        OnlineError::SnapshotConflict("distributed plan has no certificate".to_owned())
    })?;
    if query.query_form != QueryForm::Select
        || query.observed_multiset_sha256.as_deref()
            != Some(certificate.distributed_multiset_sha256.as_str())
        || bound_certificate.plan_artifact_sha256 != certificate.plan_artifact_sha256
        || bound_certificate.distributed_multiset_sha256 != certificate.distributed_multiset_sha256
        || plan.format_version != 1
        || plan.dataset_id != active.snapshot.dataset_id
        || plan.snapshot_id != active.snapshot.snapshot_id
        || plan.query_sha256 != query_sha256
        || plan.ordered
        || plan.fragments.len() < 2
        || usize::try_from(certificate.fragment_count).ok() != Some(plan.fragments.len())
        || plan.final_head.is_empty()
    {
        return Err(OnlineError::SnapshotConflict(
            "distributed plan identity or coverage is invalid".to_owned(),
        ));
    }
    let final_head = plan
        .final_head
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    if final_head.len() != plan.final_head.len()
        || plan.final_head.iter().any(|variable| variable.is_empty())
    {
        return Err(OnlineError::SnapshotConflict(
            "distributed final projection is invalid".to_owned(),
        ));
    }
    let selected_graphs = routing
        .selected_graph_iris
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    let mut fragment_ids = BTreeSet::new();
    let mut fragment_variables = BTreeSet::new();
    for (ordinal, fragment) in plan.fragments.iter().enumerate() {
        let expected_id = format!("fragment-{ordinal:04}");
        let expected_dataset = format!("data/distributed/{query_sha256}/{expected_id}.nq");
        let expected_query = format!("queries/distributed/{query_sha256}/{expected_id}.rq");
        let head = fragment
            .head
            .iter()
            .map(String::as_str)
            .collect::<BTreeSet<_>>();
        if fragment.fragment_id != expected_id
            || !fragment_ids.insert(fragment.fragment_id.as_str())
            || !selected_graphs.contains(fragment.graph_iri.as_str())
            || fragment.dataset_artifact_relative_path != expected_dataset
            || fragment.query_artifact_relative_path != expected_query
            || fragment.dataset_artifact_bytes == 0
            || fragment.query_artifact_bytes == 0
            || !is_sha256(&fragment.dataset_artifact_sha256)
            || !is_sha256(&fragment.query_artifact_sha256)
            || !is_sha256(&fragment.observed_multiset_sha256)
            || head.len() != fragment.head.len()
            || fragment.head.iter().any(|variable| variable.is_empty())
        {
            return Err(OnlineError::SnapshotConflict(
                "distributed fragment contract is invalid".to_owned(),
            ));
        }
        for (relative_path, sha256, bytes) in [
            (
                fragment.dataset_artifact_relative_path.as_str(),
                fragment.dataset_artifact_sha256.as_str(),
                fragment.dataset_artifact_bytes,
            ),
            (
                fragment.query_artifact_relative_path.as_str(),
                fragment.query_artifact_sha256.as_str(),
                fragment.query_artifact_bytes,
            ),
        ] {
            if !manifest.artifacts.iter().any(|artifact| {
                artifact.relative_path == relative_path
                    && artifact.sha256 == sha256
                    && artifact.bytes == bytes
            }) {
                return Err(OnlineError::SnapshotConflict(
                    "distributed fragment artifact differs from the snapshot".to_owned(),
                ));
            }
        }
        fragment_variables.extend(fragment.head.iter().map(String::as_str));
    }
    let join_order = plan
        .join_order
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    if join_order.len() != plan.join_order.len()
        || join_order != fragment_ids
        || !final_head.is_subset(&fragment_variables)
    {
        return Err(OnlineError::SnapshotConflict(
            "distributed join order or final projection is incomplete".to_owned(),
        ));
    }
    Ok(())
}

fn validate_capability_map(
    map: &BTreeMap<String, Vec<String>>,
    graph_iris: &BTreeSet<String>,
    allow_empty: bool,
) -> Result<(), OnlineError> {
    for (key, values) in map {
        let unique = values.iter().cloned().collect::<BTreeSet<_>>();
        if key.is_empty()
            || (!allow_empty && values.is_empty())
            || unique.len() != values.len()
            || !unique.is_subset(graph_iris)
            || values != &unique.iter().cloned().collect::<Vec<_>>()
            || (allow_empty && unique.contains(key))
        {
            return Err(OnlineError::SnapshotConflict(
                "graph capability map is not canonical or references an unknown graph".to_owned(),
            ));
        }
    }
    Ok(())
}

fn validate_serving_manifest(
    active: &ActiveServingSnapshot,
    manifest: &ServingRootManifest,
) -> Result<(), OnlineError> {
    let serving_root = required_serving_root(active)?;
    if manifest.dataset_id != active.snapshot.dataset_id
        || manifest.snapshot_id != active.snapshot.snapshot_id
        || manifest.binary_locator_object_key != serving_root.binary_locator_object_key
        || manifest.binary_locator_sha256 != serving_root.binary_locator_sha256
        || manifest.source_locator_sha256 != serving_root.source_locator_sha256
        || manifest.semantic_content_sha256 != serving_root.semantic_content_sha256
        || i32::try_from(manifest.partitions.len()).ok() != Some(serving_root.partition_count)
        || i32::try_from(manifest.row_group_rows).ok() != Some(serving_root.row_group_rows)
        || i64::try_from(manifest.locator_record_count).ok()
            != Some(serving_root.locator_record_count)
    {
        return Err(OnlineError::SnapshotConflict(
            "serving manifest differs from catalog truth".to_owned(),
        ));
    }
    Ok(())
}

fn public_payload_rows(
    rows: &[HydratedShardRow],
    iri_by_guid: &BTreeMap<Uuid, String>,
    dictionary: &BTreeMap<u64, String>,
) -> Result<Vec<OnlinePayloadRow>, OnlineError> {
    rows.iter()
        .map(|row| {
            let qualified_subject = iri_by_guid.get(&row.entity_guid).ok_or_else(|| {
                OnlineError::SnapshotConflict(
                    "hydrated GUID has no qualified public IRI".to_owned(),
                )
            })?;
            if row.subject_resource_kind != ngkg_hydration::RdfResourceKind::NamedNode
                || &row.subject_term != qualified_subject
            {
                return Err(OnlineError::SnapshotConflict(
                    "hydrated subject differs from the qualified named-node IRI".to_owned(),
                ));
            }
            if row.graph_scope != ngkg_hydration::RdfGraphScope::Named {
                return Err(OnlineError::SnapshotConflict(
                    "service hydration cannot expose the physical source default graph".to_owned(),
                ));
            }
            Ok(OnlinePayloadRow {
                query_ordinal: row.query_ordinal,
                multiplicity: row.multiplicity,
                entity_guid: row.entity_guid,
                subject_resource_kind: row.subject_resource_kind,
                subject_term: row.subject_term.clone(),
                predicate_iri: dictionary.get(&row.predicate_id).cloned().ok_or_else(|| {
                    OnlineError::SnapshotConflict(
                        "payload predicate is absent from the dictionary".to_owned(),
                    )
                })?,
                lexical_value: row.lexical_value.clone(),
                datatype_iri: (!row.datatype_iri.is_empty()).then(|| row.datatype_iri.clone()),
                language: row.language.clone(),
                graph_iri: dictionary.get(&row.graph_id).cloned().ok_or_else(|| {
                    OnlineError::SnapshotConflict(
                        "payload graph is absent from the dictionary".to_owned(),
                    )
                })?,
            })
        })
        .collect()
}

fn read_iri_dictionary(path: &Path) -> Result<BTreeMap<u64, String>, OnlineError> {
    use std::io::{BufRead, BufReader};

    let mut dictionary = BTreeMap::new();
    let mut expected = 0_u64;
    for line in BufReader::new(std::fs::File::open(path)?).lines() {
        let line = line?;
        let mut fields = line.splitn(3, '\t');
        let id = fields.next().and_then(|value| value.parse::<u64>().ok());
        let kind = fields.next();
        let term = fields.next();
        if id != Some(expected) || kind.is_none() || term.is_none() {
            return Err(OnlineError::SnapshotConflict(
                "dictionary is not dense and canonical".to_owned(),
            ));
        }
        if kind == Some("I") {
            let iri = term.ok_or_else(|| {
                OnlineError::SnapshotConflict("dictionary IRI is absent".to_owned())
            })?;
            if iri.is_empty() || dictionary.insert(expected, iri.to_owned()).is_some() {
                return Err(OnlineError::SnapshotConflict(
                    "dictionary IRI entry is invalid".to_owned(),
                ));
            }
        } else if !matches!(kind, Some("B") | Some("L")) {
            return Err(OnlineError::SnapshotConflict(
                "dictionary term kind is invalid".to_owned(),
            ));
        }
        expected = expected
            .checked_add(1)
            .ok_or_else(|| OnlineError::SnapshotConflict("dictionary ID overflow".to_owned()))?;
    }
    Ok(dictionary)
}

fn object_parent(key: &str) -> Result<&str, OnlineError> {
    key.rsplit_once('/')
        .map(|(parent, _)| parent)
        .filter(|parent| !parent.is_empty())
        .ok_or_else(|| OnlineError::SnapshotConflict("object key has no parent".to_owned()))
}

fn parse_role(value: Option<&str>) -> Result<Role> {
    match value {
        Some("query") => Ok(Role::Query),
        Some("fragment") => Ok(Role::Fragment),
        Some("locator") => Ok(Role::Locator),
        Some("hydration") => Ok(Role::Hydration),
        _ => anyhow::bail!("usage: ngkg-online-serving query|fragment|locator|hydration"),
    }
}

fn required(name: &str) -> Result<String> {
    env::var(name).with_context(|| format!("{name} is required"))
}

fn optional(name: &str) -> Option<String> {
    env::var(name).ok().filter(|value| !value.is_empty())
}

fn load_query_log_config(role: Role) -> Result<QueryLogConfig> {
    if role != Role::Query {
        return Ok(QueryLogConfig {
            store_query_text: false,
            max_page_size: 1,
            coordinator_cpu_millis: 1,
            coordinator_memory_bytes: 1,
            fragment_cpu_millis: 1,
            fragment_memory_bytes: 1,
            hydration_cpu_millis: 1,
            hydration_memory_bytes: 1,
        });
    }
    let config = QueryLogConfig {
        store_query_text: required_bool("NGKG_QUERY_LOG_STORE_QUERY_TEXT")?,
        max_page_size: positive_usize("NGKG_QUERY_LOG_MAX_PAGE_SIZE")?,
        coordinator_cpu_millis: positive_u64("NGKG_QUERY_LOG_COORDINATOR_CPU_MILLIS")?,
        coordinator_memory_bytes: positive_u64("NGKG_QUERY_LOG_COORDINATOR_MEMORY_BYTES")?,
        fragment_cpu_millis: positive_u64("NGKG_QUERY_LOG_FRAGMENT_CPU_MILLIS")?,
        fragment_memory_bytes: positive_u64("NGKG_QUERY_LOG_FRAGMENT_MEMORY_BYTES")?,
        hydration_cpu_millis: positive_u64("NGKG_QUERY_LOG_HYDRATION_CPU_MILLIS")?,
        hydration_memory_bytes: positive_u64("NGKG_QUERY_LOG_HYDRATION_MEMORY_BYTES")?,
    };
    if config.max_page_size > 1000 {
        anyhow::bail!("NGKG_QUERY_LOG_MAX_PAGE_SIZE cannot exceed 1000");
    }
    Ok(config)
}

fn load_federation_registry(role: Role) -> Result<Option<Arc<FederationRegistry>>> {
    if role != Role::Query {
        return Ok(None);
    }
    let path = optional("NGKG_FEDERATION_REGISTRY_FILE");
    let expected_sha256 = optional("NGKG_FEDERATION_REGISTRY_SHA256");
    match (path, expected_sha256) {
        (None, None) => Ok(None),
        (Some(_), None) | (None, Some(_)) => {
            anyhow::bail!(
                "NGKG_FEDERATION_REGISTRY_FILE and NGKG_FEDERATION_REGISTRY_SHA256 must be configured together"
            )
        }
        (Some(path), Some(expected_sha256)) => {
            let path = PathBuf::from(path);
            if !path.is_absolute() {
                anyhow::bail!("NGKG_FEDERATION_REGISTRY_FILE must be an absolute path");
            }
            let registry =
                FederationRegistry::load(&path, &expected_sha256).map_err(anyhow::Error::new)?;
            tracing::info!(
                registry_sha256 = registry.sha256(),
                "checksum-bound SPARQL federation endpoint registry loaded"
            );
            Ok(Some(Arc::new(registry)))
        }
    }
}

fn required_bool(name: &str) -> Result<bool> {
    match required(name)?.as_str() {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => anyhow::bail!("{name} must be exactly true or false"),
    }
}

fn load_online_direct_config(role: Role) -> Result<Option<Arc<OnlineDirectConfig>>> {
    let enabled = optional("NGKG_ONLINE_DIRECT_ENABLED").is_some_and(|value| value == "true");
    if role != Role::Query || !enabled {
        return Ok(None);
    }
    let worker_base_urls = required("NGKG_REASONER_WORKER_URLS")?
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    if worker_base_urls.is_empty()
        || worker_base_urls
            .iter()
            .any(|url| !(url.starts_with("http://") || url.starts_with("https://")))
    {
        anyhow::bail!("NGKG_REASONER_WORKER_URLS must contain bounded HTTP(S) service URLs");
    }
    let config = OnlineDirectConfig {
        worker_base_urls,
        bearer_token: required("NGKG_REASONER_SHARED_TOKEN")?,
        ontology_root: absolute_path("NGKG_REASONER_ONTOLOGY_ROOT")?,
        work_root: absolute_path("NGKG_REASONER_WORK_ROOT")?,
        dispatch_concurrency: positive_usize("NGKG_REASONER_DISPATCH_CONCURRENCY")?,
        dispatch_attempts: positive_usize("NGKG_REASONER_DISPATCH_ATTEMPTS")?,
        max_partition_response_bytes: positive_usize("NGKG_REASONER_MAX_PARTITION_RESPONSE_BYTES")?,
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
            max_local_reasoner_lanes: 1,
            reasoner_heap_mib_per_lane: positive_u64("NGKG_REASONER_HEAP_MIB_PER_LANE")?,
            reasoner_timeout: Duration::from_secs(positive_u64(
                "NGKG_REASONER_PARTITION_TIMEOUT_SECONDS",
            )?),
            max_certificate_bytes: positive_u64("NGKG_DIRECT_MAX_CERTIFICATE_BYTES")?,
            max_proof_support_ids: positive_u64("NGKG_DIRECT_MAX_PROOF_SUPPORT_IDS")?,
        },
        // Only identity fields are consumed by the distributed merger. Worker pods independently
        // verify the executable/JAR bytes before accepting a partition.
        adapter: DirectExactAdapter {
            java_executable: PathBuf::from(required("NGKG_JAVA_EXECUTABLE")?),
            adapter_jar: PathBuf::from(required("NGKG_REASONER_ADAPTER_JAR")?),
            adapter_sha256: required("NGKG_REASONER_ADAPTER_SHA256")?,
            adapter_version: required("NGKG_REASONER_ADAPTER_VERSION")?,
            reasoner_version: required("NGKG_HERMIT_VERSION")?,
        },
    };
    if config.dispatch_concurrency > 256 || config.dispatch_attempts > 8 {
        anyhow::bail!("online Direct dispatch concurrency or retry ceiling is unsafe");
    }
    Ok(Some(Arc::new(config)))
}

fn positive_u64(name: &str) -> Result<u64> {
    required(name)?
        .parse::<u64>()
        .ok()
        .filter(|value| *value > 0)
        .with_context(|| format!("{name} must be a positive integer"))
}

fn positive_usize(name: &str) -> Result<usize> {
    required(name)?
        .parse::<usize>()
        .ok()
        .filter(|value| *value > 0)
        .with_context(|| format!("{name} must be a positive platform-sized integer"))
}

fn positive_u32(name: &str) -> Result<u32> {
    required(name)?
        .parse::<u32>()
        .ok()
        .filter(|value| *value > 0)
        .with_context(|| format!("{name} must be a positive 32-bit integer"))
}

fn absolute_path(name: &str) -> Result<PathBuf> {
    let path = PathBuf::from(required(name)?);
    if !path.is_absolute() {
        anyhow::bail!("{name} must be an absolute path");
    }
    Ok(path)
}

async fn shutdown_signal() {
    let interrupt = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let terminate = async {
        if let Ok(mut signal) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            signal.recv().await;
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        () = interrupt => {},
        () = terminate => {},
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeSet,
        fs::{self, OpenOptions},
        io::Write,
        path::PathBuf,
        sync::{Arc, atomic::AtomicUsize},
    };

    use axum::{
        body::Body,
        http::{HeaderMap, HeaderValue, StatusCode},
        response::IntoResponse,
    };
    use ngkg_grace_join::{GraceJoinEngine, GraceJoinIdentity};
    use ngkg_identity::guid_for_canonical_iri;
    use ngkg_query_cache::QueryCacheKey;
    use ngkg_query_executor::{
        FragmentBatchMetadata, shuffle_partition_for_binding, write_fragment_arrow_stream,
    };
    use ngkg_reference::{
        CERTIFIED_QUERY_RESULT_HASH_VERSION, CertifiedQueryExecutionLimits, DatasetSelectionSource,
        QueryRoutingCertificate, canonical_query_payload_sha256, canonical_sparql_multiset_sha256,
    };
    use serde_json::json;
    use sha2::{Digest, Sha256};

    use super::{
        ARROW_STREAM_EOS, AdmissionClass, AdmissionController, AdmissionFailure, AdmissionScope,
        ArrowBodyWriter, ArrowRequestWriter, BoundedBuffer, BoundedLruCache, ExecutionResponse,
        FragmentResponseLease, FragmentResponseSpool, OnlineError, OnlinePayloadRow,
        ParsedProtocolParameters, QualifiedEntity, QueryForm, QueryResponse, Role, RoutingResponse,
        ShuffleSpillStage, SparqlGraphFormat, SparqlSolutionFormat, SpillIdentity,
        StandardsFeatureGates, StreamingRequestSpool, ValidatedFragmentSpool,
        ValidatedFragmentSpoolSequence, admission_rejection, compile_certified_query,
        compute_shuffle_result, decode_form_component, filtered_openapi_document,
        finish_protocol_request, hold_admission_through_body, parse_protocol_parameters,
        prepare_shuffle_spill_root, render_service_description,
        require_supported_result_hash_version, reserve_exchange_bytes, select_sparql_graph_format,
        select_sparql_solution_format, serialize_sparql_boolean, serialize_sparql_graph,
        serialize_sparql_solutions, sparql_request_media_type, swagger_ui_response,
        union_binding_heads, upstream_status_error, validate_cached_query_response,
        validate_cached_shuffle_result, validate_hydrated_rows, validate_shuffle_response_spool,
        validate_standards_feature_implications, worker_join_evidence,
    };
    use crate::tenant_admission::TenantAdmissionRegistry;
    use uuid::Uuid;

    fn qualified() -> QualifiedEntity {
        QualifiedEntity {
            query_ordinal: 7,
            iri: "https://ngkg.io/id/entity-7".to_owned(),
            guid: Uuid::from_u128(7),
            multiplicity: 2,
        }
    }

    fn spill_root() -> PathBuf {
        std::env::temp_dir().join(format!("ngkg-phase25-test-{}", Uuid::new_v4()))
    }

    fn request_spool_root() -> PathBuf {
        std::env::temp_dir().join(format!("ngkg-phase31-request-test-{}", Uuid::new_v4()))
    }

    fn fragment_response_spool_root() -> PathBuf {
        std::env::temp_dir().join(format!(
            "ngkg-phase33-fragment-response-test-{}",
            Uuid::new_v4()
        ))
    }

    #[test]
    fn sparql_protocol_parameters_are_strict_and_lossless() -> Result<(), OnlineError> {
        let parsed = parse_protocol_parameters(
            "query=SELECT+%3Fs+WHERE+%7B+%3Fs+%3Fp+%3Fo+%7D&default-graph-uri=https%3A%2F%2Fexample.org%2Fg1&default-graph-uri=https%3A%2F%2Fexample.org%2Fg2&named-graph-uri=https%3A%2F%2Fexample.org%2Fn",
            true,
        )?;
        let request = finish_protocol_request(parsed, None)?;
        assert_eq!(request.query, "SELECT ?s WHERE { ?s ?p ?o }");
        assert_eq!(
            request.default_graph_uris,
            vec![
                "https://example.org/g1".to_owned(),
                "https://example.org/g2".to_owned()
            ]
        );
        assert_eq!(
            request.named_graph_uris,
            vec!["https://example.org/n".to_owned()]
        );
        assert!(parse_protocol_parameters("query=a&query=b", true).is_err());
        assert!(parse_protocol_parameters("query=%FF", true).is_err());
        assert!(parse_protocol_parameters("unknown=value", true).is_err());
        assert!(parse_protocol_parameters("query=SELECT", false).is_err());
        assert!(finish_protocol_request(ParsedProtocolParameters::default(), None).is_err());
        assert_eq!(decode_form_component("a%2Bb+c")?, "a+b c");
        Ok(())
    }

    #[test]
    fn sparql_result_content_negotiation_is_standards_aware_and_fail_closed() {
        let mut headers = HeaderMap::new();
        assert_eq!(
            select_sparql_solution_format(&headers).ok(),
            Some(SparqlSolutionFormat::Json)
        );
        headers.insert(
            "accept",
            HeaderValue::from_static(
                "application/sparql-results+xml;q=0.9, application/sparql-results+json;q=0.5",
            ),
        );
        assert_eq!(
            select_sparql_solution_format(&headers).ok(),
            Some(SparqlSolutionFormat::Xml)
        );
        headers.insert("accept", HeaderValue::from_static("text/csv"));
        assert_eq!(
            select_sparql_solution_format(&headers).ok(),
            Some(SparqlSolutionFormat::Csv)
        );
        headers.insert(
            "accept",
            HeaderValue::from_static("text/tab-separated-values"),
        );
        assert_eq!(
            select_sparql_solution_format(&headers).ok(),
            Some(SparqlSolutionFormat::Tsv)
        );
        headers.insert("accept", HeaderValue::from_static("application/json"));
        assert!(select_sparql_solution_format(&headers).is_err());
        headers.insert("accept", HeaderValue::from_static("*/*;q=0"));
        assert!(select_sparql_solution_format(&headers).is_err());
        headers.insert(
            "accept",
            HeaderValue::from_static(
                "application/sparql-results+json;q=0, application/*;q=1, */*;q=1",
            ),
        );
        assert_eq!(
            select_sparql_solution_format(&headers).ok(),
            Some(SparqlSolutionFormat::Xml)
        );
        headers.insert("accept", HeaderValue::from_static("*/*;q=1.1"));
        assert!(select_sparql_solution_format(&headers).is_err());
        headers.insert(
            "accept",
            HeaderValue::from_static("text/csv; charset=iso-8859-1"),
        );
        assert!(select_sparql_solution_format(&headers).is_err());
    }

    #[test]
    fn sparql_protocol_request_media_types_require_utf8() {
        let mut headers = HeaderMap::new();
        assert!(sparql_request_media_type(&headers).is_err());
        headers.insert(
            "content-type",
            HeaderValue::from_static("application/sparql-query; charset=UTF-8"),
        );
        let observed = sparql_request_media_type(&headers);
        assert_eq!(observed.ok().as_deref(), Some("application/sparql-query"));
        headers.insert(
            "content-type",
            HeaderValue::from_static("application/sparql-query; charset=iso-8859-1"),
        );
        assert!(sparql_request_media_type(&headers).is_err());
    }

    #[test]
    fn sparql_is_parsed_once_by_the_shared_typed_compiler_for_all_query_forms() {
        for (query_text, expected_form) in [
            ("SELECT ?s WHERE { ?s ?p ?o }", QueryForm::Select),
            ("ASK { ?s ?p ?o }", QueryForm::Ask),
            (
                "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }",
                QueryForm::Construct,
            ),
            ("DESCRIBE ?s WHERE { ?s ?p ?o }", QueryForm::Describe),
        ] {
            let compiled = compile_certified_query(query_text);
            assert!(
                compiled
                    .as_ref()
                    .is_ok_and(|query| query.form() == expected_form)
            );
        }
        let volatile = compile_certified_query("SELECT (NOW() AS ?now) WHERE {}");
        assert!(
            volatile
                .as_ref()
                .is_ok_and(|query| { !query.execution_analysis().is_snapshot_cacheable() })
        );
        let federated = compile_certified_query(
            "SELECT * WHERE { SERVICE <https://example.test/sparql> { ?s ?p ?o } }",
        );
        assert!(federated.as_ref().is_ok_and(|query| {
            query.execution_analysis().has_remote_service && query.require_certifiable().is_err()
        }));
        assert!(compile_certified_query("SELECT WHERE { ?s ?p }").is_err());
    }

    #[test]
    fn select_response_uses_w3c_solution_serializers() -> Result<(), Box<dyn std::error::Error>> {
        let head = ["asset".to_owned(), "label".to_owned()];
        let bindings = [
            json!({
                "asset": {"type": "uri", "value": "https://example.org/a"},
                "label": {"type": "literal", "value": "A", "xml:lang": "en"}
            }),
            json!({"asset": {"type": "uri", "value": "https://example.org/b"}}),
        ];

        let json_bytes =
            serialize_sparql_solutions(&head, &bindings, SparqlSolutionFormat::Json, 4096)?;
        let value = serde_json::from_slice::<serde_json::Value>(&json_bytes)?;
        assert_eq!(value.pointer("/head/vars/0"), Some(&json!("asset")));
        assert_eq!(
            value.pointer("/results/bindings/0/label/xml:lang"),
            Some(&json!("en"))
        );
        assert!(value.pointer("/results/bindings/1/label").is_none());

        let xml = serialize_sparql_solutions(&head, &bindings, SparqlSolutionFormat::Xml, 4096)?;
        assert!(std::str::from_utf8(&xml)?.contains("<sparql"));
        let tsv = serialize_sparql_solutions(&head, &bindings, SparqlSolutionFormat::Tsv, 4096)?;
        assert!(std::str::from_utf8(&tsv)?.starts_with("?asset\t?label"));
        let csv = serialize_sparql_solutions(&head, &bindings, SparqlSolutionFormat::Csv, 4096)?;
        assert!(std::str::from_utf8(&csv)?.starts_with("asset,label"));
        Ok(())
    }

    #[test]
    fn ask_and_graph_results_use_standard_serializers() -> Result<(), Box<dyn std::error::Error>> {
        let ask_json = serialize_sparql_boolean(true, SparqlSolutionFormat::Json, 4096)?;
        let ask_value = serde_json::from_slice::<serde_json::Value>(&ask_json)?;
        assert_eq!(ask_value.pointer("/boolean"), Some(&json!(true)));
        let ask_xml = serialize_sparql_boolean(false, SparqlSolutionFormat::Xml, 4096)?;
        assert!(std::str::from_utf8(&ask_xml)?.contains("<boolean>false</boolean>"));

        let graph = [
            "<https://example.org/s> <https://example.org/p> <https://example.org/o> .\n"
                .to_owned(),
        ];
        let nt = serialize_sparql_graph(&graph, SparqlGraphFormat::NTriples, 4096)?;
        assert!(std::str::from_utf8(&nt)?.contains("https://example.org/s"));
        let turtle = serialize_sparql_graph(&graph, SparqlGraphFormat::Turtle, 4096)?;
        assert!(std::str::from_utf8(&turtle)?.contains("https://example.org/p"));
        let rdf_xml = serialize_sparql_graph(&graph, SparqlGraphFormat::RdfXml, 16384)?;
        assert!(std::str::from_utf8(&rdf_xml)?.contains("rdf:RDF"));

        let mut headers = HeaderMap::new();
        headers.insert("accept", HeaderValue::from_static("application/n-triples"));
        assert_eq!(
            select_sparql_graph_format(&headers).ok(),
            Some(SparqlGraphFormat::NTriples)
        );
        headers.insert(
            "accept",
            HeaderValue::from_static("application/sparql-results+json"),
        );
        assert!(select_sparql_graph_format(&headers).is_err());
        Ok(())
    }

    #[test]
    fn swagger_ui_is_vendored_and_served_with_hardened_headers() -> Result<(), OnlineError> {
        let index = swagger_ui_response("")?;
        assert_eq!(index.status(), StatusCode::OK);
        assert!(index.headers().get("content-type").is_some());
        assert_eq!(
            index
                .headers()
                .get("x-content-type-options")
                .and_then(|value| value.to_str().ok()),
            Some("nosniff")
        );
        let csp = index
            .headers()
            .get("content-security-policy")
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default();
        assert_eq!(
            csp,
            "default-src 'self'; style-src 'self' 'unsafe-inline'; script-src 'self'; img-src 'self' data:; connect-src 'self'; font-src 'self' data:"
        );

        let css = swagger_ui_response("swagger-ui.css")?;
        assert_eq!(css.status(), StatusCode::OK);
        let missing = swagger_ui_response("ngkg-no-such-swagger-asset")?;
        assert_eq!(missing.status(), StatusCode::NOT_FOUND);
        Ok(())
    }

    #[test]
    fn upstream_timeout_statuses_are_not_collapsed_into_dependency_outages() {
        assert!(matches!(
            upstream_status_error("fragment worker", reqwest::StatusCode::GATEWAY_TIMEOUT),
            OnlineError::GatewayTimeout(_)
        ));
        assert!(matches!(
            upstream_status_error("fragment worker", reqwest::StatusCode::REQUEST_TIMEOUT),
            OnlineError::GatewayTimeout(_)
        ));
        assert!(matches!(
            upstream_status_error("fragment worker", reqwest::StatusCode::SERVICE_UNAVAILABLE),
            OnlineError::Upstream(_)
        ));
    }

    #[test]
    fn standards_are_not_advertised_without_release_gates() {
        let closed = render_service_description(StandardsFeatureGates::default(), false);
        assert!(!closed.contains("SPARQL11Query"));
        assert!(!closed.contains("UnionDefaultGraph"));
        assert!(!closed.contains("OWL-Direct"));
        assert!(!closed.contains("owl-profile/DL"));
        assert!(closed.contains("certifiedQueryOnly true"));

        let open = render_service_description(
            StandardsFeatureGates {
                sparql_11_query: true,
                union_default_graph: true,
                owl_direct: true,
                owl_dl: true,
            },
            true,
        );
        assert!(open.contains("sd:supportedLanguage sd:SPARQL11Query"));
        assert!(open.contains("sd:feature sd:UnionDefaultGraph"));
        assert!(open.contains("entailment/OWL-Direct"));
        assert!(open.contains("owl-profile/DL"));
        assert!(open.contains("sd:feature sd:BasicFederatedQuery"));
        assert!(open.contains("securedEndpointRegistry true"));
    }

    #[test]
    fn standards_feature_implications_fail_closed() {
        assert!(validate_standards_feature_implications(StandardsFeatureGates::default()).is_ok());
        assert!(
            validate_standards_feature_implications(StandardsFeatureGates {
                sparql_11_query: false,
                union_default_graph: true,
                owl_direct: false,
                owl_dl: false,
            })
            .is_err()
        );
        assert!(
            validate_standards_feature_implications(StandardsFeatureGates {
                sparql_11_query: false,
                union_default_graph: false,
                owl_direct: true,
                owl_dl: true,
            })
            .is_err()
        );
        assert!(
            validate_standards_feature_implications(StandardsFeatureGates {
                sparql_11_query: true,
                union_default_graph: true,
                owl_direct: true,
                owl_dl: true,
            })
            .is_ok()
        );
    }

    #[test]
    fn sparql_protocol_errors_use_precise_http_statuses() {
        assert_eq!(
            OnlineError::MalformedSparql("bad grammar".to_owned())
                .into_response()
                .status(),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            OnlineError::UnsupportedMediaType("text/plain".to_owned())
                .into_response()
                .status(),
            StatusCode::UNSUPPORTED_MEDIA_TYPE
        );
        assert_eq!(
            OnlineError::NotAcceptable.into_response().status(),
            StatusCode::NOT_ACCEPTABLE
        );
        assert_eq!(
            OnlineError::ActiveDatasetNotCertified
                .into_response()
                .status(),
            StatusCode::UNPROCESSABLE_ENTITY
        );
        assert_eq!(
            OnlineError::GatewayTimeout("query deadline".to_owned())
                .into_response()
                .status(),
            StatusCode::GATEWAY_TIMEOUT
        );
        assert_eq!(
            OnlineError::QueryTooLarge.into_response().status(),
            StatusCode::PAYLOAD_TOO_LARGE
        );
        assert_eq!(
            OnlineError::NativeCutoverUnavailable("missing native plan".to_owned())
                .into_response()
                .status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
    }

    #[test]
    fn embedded_openapi_describes_all_sparql_protocol_encodings()
    -> Result<(), Box<dyn std::error::Error>> {
        let document = serde_yaml::from_str::<serde_json::Value>(include_str!(
            "../../../api/online-openapi.yaml"
        ))?;
        let sparql = document
            .pointer("/paths/~1v1~1datasets~1{datasetId}~1sparql")
            .ok_or("SPARQL path is absent")?;
        assert!(sparql.get("get").is_some());
        let content = sparql
            .pointer("/post/requestBody/content")
            .and_then(serde_json::Value::as_object)
            .ok_or("SPARQL POST content is absent")?;
        assert!(content.contains_key("application/sparql-query"));
        assert!(content.contains_key("application/x-www-form-urlencoded"));
        assert!(document.pointer("/paths/~1docs/get").is_some());
        assert!(document.pointer("/paths/~1openapi.json/get").is_some());
        Ok(())
    }

    #[test]
    fn runtime_openapi_contains_only_common_and_current_role_paths()
    -> Result<(), Box<dyn std::error::Error>> {
        let common = BTreeSet::from([
            "/docs",
            "/openapi.yaml",
            "/openapi.json",
            "/health/live",
            "/health/ready",
            "/metrics",
        ]);
        let cases = [
            (
                Role::Query,
                BTreeSet::from([
                    "/v1/hpc/capabilities",
                    "/v1/query_logs",
                    "/v1/query_logs/{queryExecutionId}",
                    "/v1/datasets/{datasetId}/sparql",
                    "/v1/datasets/{datasetId}/sparql/direct/validate",
                    "/v1/datasets/{datasetId}/sparql/direct/route",
                    "/v1/datasets/{datasetId}/sparql/service-description",
                    "/v1/datasets/{datasetId}/query",
                ]),
            ),
            (
                Role::Fragment,
                BTreeSet::from([
                    "/v1/datasets/{datasetId}/fragments/{querySha256}/{fragmentId}/execute",
                    "/v1/datasets/{datasetId}/shuffles/{querySha256}/{stage}/{partition}/join",
                    "/v1/datasets/{datasetId}/algebra/{querySha256}/{replica}/execute",
                    "/v1/datasets/{datasetId}/paths/{querySha256}/{pathId}/{iteration}/{partition}/expand",
                    "/v1/datasets/{datasetId}/native/leaves/{querySha256}/{partition}/scan",
                ]),
            ),
            (
                Role::Locator,
                BTreeSet::from(["/v1/datasets/{datasetId}/locate"]),
            ),
            (
                Role::Hydration,
                BTreeSet::from(["/v1/datasets/{datasetId}/hydrate"]),
            ),
        ];

        for (role, role_paths) in cases {
            let document = filtered_openapi_document(role)?;
            let actual = document
                .get("paths")
                .and_then(serde_json::Value::as_object)
                .ok_or("filtered OpenAPI paths are absent")?
                .keys()
                .map(String::as_str)
                .collect::<BTreeSet<_>>();
            let expected = common.union(&role_paths).copied().collect::<BTreeSet<_>>();
            assert_eq!(actual, expected, "unexpected OpenAPI paths for {role:?}");
        }
        Ok(())
    }

    #[test]
    fn fragment_response_spool_streams_exact_rows_and_rejects_corruption()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = fragment_response_spool_root();
        let spool = Arc::new(FragmentResponseSpool::open(&root, 1024 * 1024)?);
        let metadata = FragmentBatchMetadata {
            dataset_id: Uuid::from_u128(1),
            snapshot_id: Uuid::from_u128(2),
            query_sha256: "1".repeat(64),
            fragment_id: "fragment-0001".to_owned(),
            worker_id: "worker-1".to_owned(),
            multiset_sha256: "2".repeat(64),
        };
        let rows = vec![
            json!({"x": {"type": "uri", "value": "urn:x:1"}}),
            json!({"x": {"type": "uri", "value": "urn:x:2"}}),
        ];
        let mut bytes = Vec::new();
        write_fragment_arrow_stream(&mut bytes, &metadata, &["x".to_owned()], &rows, 1)?;
        let path = root.join(format!("response-{}.arrow", Uuid::new_v4()));
        fs::write(&path, &bytes)?;
        spool.active_bytes.store(
            u64::try_from(bytes.len())?,
            std::sync::atomic::Ordering::Release,
        );
        let lease = FragmentResponseLease {
            owner: Arc::clone(&spool),
            path: path.clone(),
            bytes: u64::try_from(bytes.len())?,
            sha256: Sha256::digest(&bytes).into(),
            released: false,
        };
        let validated = ValidatedFragmentSpool::validate(lease, 2)?;
        assert_eq!(validated.row_count, 2);
        assert_eq!(validated.head, vec!["x".to_owned()]);
        assert_eq!(
            validated.always_bound_variables,
            BTreeSet::from(["x".to_owned()])
        );
        let mut corrupted = bytes;
        corrupted[16] ^= 0x01;
        fs::write(&path, corrupted)?;
        assert!(validated.lease.open_stream(2).is_err());
        drop(validated);
        assert_eq!(
            spool
                .active_bytes
                .load(std::sync::atomic::Ordering::Acquire),
            0
        );
        assert!(!path.exists());
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn validated_spool_sequence_replays_multiple_partitions_without_assembly()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = fragment_response_spool_root();
        let spool = Arc::new(FragmentResponseSpool::open(&root, 4 * 1024 * 1024)?);
        let head = vec!["x".to_owned()];
        let expected = vec![
            json!({"x": {"type": "uri", "value": "urn:x:1"}}),
            json!({"x": {"type": "uri", "value": "urn:x:2"}}),
            json!({"x": {"type": "uri", "value": "urn:x:3"}}),
        ];
        let mut validated = Vec::new();
        for (ordinal, rows) in [expected[..2].to_vec(), expected[2..].to_vec()]
            .into_iter()
            .enumerate()
        {
            let metadata = FragmentBatchMetadata {
                dataset_id: Uuid::from_u128(1),
                snapshot_id: Uuid::from_u128(2),
                query_sha256: "1".repeat(64),
                fragment_id: format!("stage-partition-{ordinal}"),
                worker_id: format!("worker-{ordinal}"),
                multiset_sha256: canonical_sparql_multiset_sha256(&head, &rows, false)?,
            };
            let mut bytes = Vec::new();
            write_fragment_arrow_stream(&mut bytes, &metadata, &head, &rows, 1)?;
            let path = root.join(format!("response-{}.arrow", Uuid::new_v4()));
            fs::write(&path, &bytes)?;
            spool.active_bytes.fetch_add(
                u64::try_from(bytes.len())?,
                std::sync::atomic::Ordering::AcqRel,
            );
            validated.push(ValidatedFragmentSpool::validate(
                FragmentResponseLease {
                    owner: Arc::clone(&spool),
                    path,
                    bytes: u64::try_from(bytes.len())?,
                    sha256: Sha256::digest(&bytes).into(),
                    released: false,
                },
                4,
            )?);
        }
        let observed =
            ValidatedFragmentSpoolSequence::new(validated, 4).collect::<Result<Vec<_>, _>>()?;
        assert_eq!(observed, expected);
        assert_eq!(
            spool
                .active_bytes
                .load(std::sync::atomic::Ordering::Acquire),
            0
        );
        assert_eq!(fs::read_dir(&root)?.count(), 1);
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn shuffle_response_spool_validates_exact_multiset_and_partition()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = fragment_response_spool_root();
        let spool = Arc::new(FragmentResponseSpool::open(&root, 4 * 1024 * 1024)?);
        let head = vec!["x".to_owned(), "y".to_owned()];
        let keys = vec!["x".to_owned()];
        let rows = vec![json!({
            "x": {"type": "uri", "value": "urn:x:35"},
            "y": {"type": "literal", "value": "enterprise"}
        })];
        let partition = shuffle_partition_for_binding(&rows[0], &keys, 4)?;
        let metadata = FragmentBatchMetadata {
            dataset_id: Uuid::from_u128(1),
            snapshot_id: Uuid::from_u128(2),
            query_sha256: "3".repeat(64),
            fragment_id: super::shuffle_partition_id(0, partition),
            worker_id: "worker-enterprise".to_owned(),
            multiset_sha256: canonical_sparql_multiset_sha256(&head, &rows, false)?,
        };
        let mut bytes = Vec::new();
        write_fragment_arrow_stream(&mut bytes, &metadata, &head, &rows, 1)?;
        let path = root.join(format!("response-{}.arrow", Uuid::new_v4()));
        fs::write(&path, &bytes)?;
        spool.active_bytes.store(
            u64::try_from(bytes.len())?,
            std::sync::atomic::Ordering::Release,
        );
        let validated = validate_shuffle_response_spool(
            FragmentResponseLease {
                owner: Arc::clone(&spool),
                path,
                bytes: u64::try_from(bytes.len())?,
                sha256: Sha256::digest(&bytes).into(),
                released: false,
            },
            metadata.dataset_id,
            metadata.snapshot_id,
            &metadata.query_sha256,
            &metadata.fragment_id,
            &head,
            &keys,
            4,
            partition,
            4,
        )?;
        assert_eq!(validated.row_count, 1);
        assert_eq!(validated.materialize(4)?, rows);
        assert_eq!(
            spool
                .active_bytes
                .load(std::sync::atomic::Ordering::Acquire),
            0
        );
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[tokio::test]
    async fn streamed_request_spool_verifies_checksum_eos_limits_and_cleanup()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = request_spool_root();
        let spool = Arc::new(StreamingRequestSpool::open(&root, 1024)?);
        let mut payload = b"arrow-schema-and-record-batches".to_vec();
        payload.extend_from_slice(&ARROW_STREAM_EOS);
        let lease = spool.receive(Body::from(payload.clone()), 1024).await?;
        assert_eq!(lease.bytes, u64::try_from(payload.len())?);
        assert_eq!(lease.sha256, hex::encode(Sha256::digest(&payload)));
        assert!(lease.path.is_file());
        drop(lease);
        assert_eq!(
            spool
                .active_bytes
                .load(std::sync::atomic::Ordering::Acquire),
            0
        );
        assert_eq!(fs::read_dir(&root)?.count(), 1);

        assert!(
            spool
                .receive(Body::from(b"truncated".to_vec()), 1024)
                .await
                .is_err()
        );
        assert_eq!(
            spool
                .active_bytes
                .load(std::sync::atomic::Ordering::Acquire),
            0
        );
        assert_eq!(fs::read_dir(&root)?.count(), 1);

        let constrained_root = request_spool_root();
        let constrained = Arc::new(StreamingRequestSpool::open(&constrained_root, 8)?);
        assert!(
            constrained
                .receive(Body::from(payload), 1024)
                .await
                .is_err()
        );
        assert_eq!(
            constrained
                .active_bytes
                .load(std::sync::atomic::Ordering::Acquire),
            0
        );
        assert_eq!(fs::read_dir(&constrained_root)?.count(), 1);
        fs::remove_dir_all(root)?;
        fs::remove_dir_all(constrained_root)?;
        Ok(())
    }

    #[test]
    fn coordinator_rejects_worker_input_evidence_that_differs_from_sent_body()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut headers = HeaderMap::new();
        for (name, value) in [
            ("x-ngkg-worker-input-mode", "streamed_spool_v1"),
            ("x-ngkg-worker-input-bytes", "128"),
            ("x-ngkg-worker-input-sha256", "1"),
            ("x-ngkg-worker-join-mode", "grace_hash_nvme_v1"),
            ("x-ngkg-worker-join-spill-bytes", "1024"),
            ("x-ngkg-worker-join-buckets", "2"),
            ("x-ngkg-worker-join-max-build-rows", "16"),
        ] {
            headers.insert(name, HeaderValue::from_str(value)?);
        }
        assert!(worker_join_evidence(&headers, 64, 64, 128, &"1".repeat(64)).is_err());
        headers.insert(
            "x-ngkg-worker-input-sha256",
            HeaderValue::from_str(&"1".repeat(64))?,
        );
        assert!(worker_join_evidence(&headers, 64, 64, 127, &"1".repeat(64)).is_err());
        let evidence = worker_join_evidence(&headers, 64, 64, 128, &"1".repeat(64))?;
        assert_eq!(evidence.streamed_input_bytes, 128);
        assert_eq!(evidence.grace_partitions, 1);
        Ok(())
    }

    fn admission_controller(
        tenants: &[Uuid],
        limits: [usize; 5],
        pending_limits: [usize; 5],
        fragment_worker_limit: usize,
        wait: std::time::Duration,
    ) -> Result<Arc<AdmissionController>, String> {
        let entries = tenants
            .iter()
            .map(|tenant| {
                json!({
                    "tenantId": tenant,
                    "query": {"maxInFlight": 1, "maxPending": 1},
                    "fragment": {"maxInFlight": 1, "maxPending": 1},
                    "shuffle": {"maxInFlight": 1, "maxPending": 1},
                    "locator": {"maxInFlight": 1, "maxPending": 1},
                    "hydration": {"maxInFlight": 1, "maxPending": 1},
                    "fragmentWorkerMaxInFlight": 1
                })
            })
            .collect::<Vec<_>>();
        let bytes = serde_json::to_vec(&json!({"formatVersion": 1, "tenants": entries}))
            .map_err(|error| error.to_string())?;
        let checksum = hex::encode(Sha256::digest(&bytes));
        let registry = TenantAdmissionRegistry::from_bytes(
            &bytes,
            &checksum,
            tenants.len(),
            &tenants.iter().copied().collect::<BTreeSet<_>>(),
            limits,
            pending_limits,
            fragment_worker_limit,
        )?;
        Ok(Arc::new(AdmissionController::new(
            limits,
            pending_limits,
            fragment_worker_limit,
            registry,
            wait,
        )))
    }

    #[test]
    fn hydration_response_must_preserve_qualified_identity() {
        let entity = qualified();
        let mut row = OnlinePayloadRow {
            query_ordinal: entity.query_ordinal,
            multiplicity: entity.multiplicity,
            entity_guid: entity.guid,
            subject_term: entity.iri.clone(),
            subject_resource_kind: ngkg_hydration::RdfResourceKind::NamedNode,
            predicate_iri: "https://ngkg.io/ontology/value".to_owned(),
            lexical_value: "42".to_owned(),
            datatype_iri: Some("http://www.w3.org/2001/XMLSchema#integer".to_owned()),
            language: None,
            graph_iri: "https://ngkg.io/graph/domain".to_owned(),
        };
        let graphs = BTreeSet::from(["https://ngkg.io/graph/domain".to_owned()]);
        assert!(
            validate_hydrated_rows(
                std::slice::from_ref(&row),
                std::slice::from_ref(&entity),
                1,
                &graphs
            )
            .is_ok()
        );
        row.graph_iri = "https://ngkg.io/graph/forbidden".to_owned();
        assert!(validate_hydrated_rows(&[row], &[entity], 1, &graphs).is_err());
    }

    #[test]
    fn hydration_response_rejects_unqualified_guid() {
        let entity = qualified();
        let row = OnlinePayloadRow {
            query_ordinal: entity.query_ordinal,
            multiplicity: entity.multiplicity,
            entity_guid: Uuid::from_u128(8),
            subject_term: entity.iri.clone(),
            subject_resource_kind: ngkg_hydration::RdfResourceKind::NamedNode,
            predicate_iri: "https://ngkg.io/ontology/value".to_owned(),
            lexical_value: "42".to_owned(),
            datatype_iri: None,
            language: None,
            graph_iri: "https://ngkg.io/graph/domain".to_owned(),
        };
        let graphs = BTreeSet::from(["https://ngkg.io/graph/domain".to_owned()]);
        assert!(validate_hydrated_rows(&[row], &[entity], 1, &graphs).is_err());
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn query_cache_revalidates_form_aware_result_and_guid() -> Result<(), Box<dyn std::error::Error>>
    {
        let tenant_id = Uuid::from_u128(1);
        let dataset_id = Uuid::from_u128(2);
        let snapshot_id = Uuid::from_u128(3);
        let namespace = Uuid::from_u128(4);
        let iri = "https://ngkg.io/id/certified-entity";
        let second_iri = "https://ngkg.io/id/certified-entity-2";
        let head = vec!["entity".to_owned()];
        let bindings = vec![
            json!({"entity": {"type": "uri", "value": iri}}),
            json!({"entity": {"type": "uri", "value": second_iri}}),
        ];
        let limits = CertifiedQueryExecutionLimits {
            max_solution_rows: 10,
            max_graph_triples: 10,
            max_graph_blank_nodes: 10,
        };
        let result_sha256 = canonical_query_payload_sha256(
            QueryForm::Select,
            &head,
            &bindings,
            None,
            &[],
            false,
            limits,
        )?;
        let multiset_sha256 = canonical_sparql_multiset_sha256(&head, &bindings, false)?;
        let key = QueryCacheKey {
            tenant_id,
            dataset_id,
            snapshot_id,
            manifest_sha256: "1".repeat(64),
            serving_root_sha256: "2".repeat(64),
            query_sha256: "3".repeat(64),
            authorized_graph_set_sha256: "7".repeat(64),
            active_dataset_sha256: "8".repeat(64),
            dataset_selection_source: DatasetSelectionSource::ServiceDefault.code(),
            hydrate: false,
        };
        let routing = QueryRoutingCertificate {
            format_version: 1,
            capability_index_sha256: "4".repeat(64),
            selected_graph_iris: vec!["https://ngkg.io/graph/domain".to_owned()],
            total_graph_count: 2,
            selection_mode: "typed_active_dataset".to_owned(),
            dataset_selection_source: DatasetSelectionSource::ServiceDefault,
            default_graph_iris: vec!["https://ngkg.io/graph/domain".to_owned()],
            named_graph_iris: vec!["https://ngkg.io/graph/domain".to_owned()],
            active_dataset_sha256: key.active_dataset_sha256.clone(),
            include_internal_closure: true,
            route_artifact_relative_path: "data/routes/route.nq".to_owned(),
            route_artifact_sha256: "5".repeat(64),
            route_artifact_bytes: 10,
            routed_result_sha256: result_sha256.clone(),
            routed_multiset_sha256: Some(multiset_sha256),
            distributed: None,
        };
        let authorized_graphs = BTreeSet::from(["https://ngkg.io/graph/domain".to_owned()]);
        let mut response = QueryResponse {
            dataset_id,
            snapshot_id,
            serving_root_sha256: key.serving_root_sha256.clone(),
            query_sha256: key.query_sha256.clone(),
            query_form: QueryForm::Select,
            authorized_graph_set_sha256: key.authorized_graph_set_sha256.clone(),
            active_dataset_sha256: key.active_dataset_sha256.clone(),
            coverage_scope: "certified-test".to_owned(),
            complete: true,
            routing: RoutingResponse {
                selection_mode: routing.selection_mode.clone(),
                dataset_selection_source: routing.dataset_selection_source,
                default_graph_iris: routing.default_graph_iris.clone(),
                named_graph_iris: routing.named_graph_iris.clone(),
                active_dataset_sha256: routing.active_dataset_sha256.clone(),
                include_internal_closure: routing.include_internal_closure,
                selected_graph_iris: routing.selected_graph_iris.clone(),
                selected_graph_count: 1,
                total_graph_count: routing.total_graph_count,
                capability_index_sha256: routing.capability_index_sha256.clone(),
                routed_dataset_sha256: routing.route_artifact_sha256.clone(),
            },
            execution: ExecutionResponse {
                mode: "certified_local_route".to_owned(),
                exchange_format: "none".to_owned(),
                fragment_ingress_mode: "none".to_owned(),
                fragment_ingress_bytes: 0,
                fragment_materialization_mode: "none".to_owned(),
                fragment_owned_rows: 0,
                shuffle_result_ingress_mode: "none".to_owned(),
                shuffle_result_ingress_bytes: 0,
                intermediate_result_mode: "none".to_owned(),
                assembled_intermediate_owned_rows: 0,
                fragment_count: 0,
                worker_count: 0,
                shuffle_partition_count: 0,
                shuffle_worker_count: 0,
                shuffle_spill_mode: "none".to_owned(),
                shuffle_spill_bytes: 0,
                shuffle_cache_mode: "none".to_owned(),
                shuffle_cache_hits: 0,
                worker_join_mode: "none".to_owned(),
                worker_join_spill_bytes: 0,
                worker_join_grace_partitions: 0,
                worker_join_max_build_rows: 0,
                worker_input_mode: "none".to_owned(),
                worker_input_bytes: 0,
                coordinator_request_mode: "none".to_owned(),
                coordinator_request_bytes: 0,
                plan_sha256: None,
            },
            head,
            bindings,
            boolean_result: None,
            graph_ntriples: Vec::new(),
            qualified_entities: vec![
                QualifiedEntity {
                    query_ordinal: 0,
                    iri: iri.to_owned(),
                    guid: guid_for_canonical_iri(namespace, iri)?,
                    multiplicity: 1,
                },
                QualifiedEntity {
                    query_ordinal: 1,
                    iri: second_iri.to_owned(),
                    guid: guid_for_canonical_iri(namespace, second_iri)?,
                    multiplicity: 1,
                },
            ],
            hydrated_payload: Vec::new(),
            entailment: None,
            property_path_execution: None,
            federation: None,
        };
        assert!(
            validate_cached_query_response(
                &response,
                &key,
                &routing,
                "certified-test",
                QueryForm::Select,
                &result_sha256,
                false,
                limits,
                namespace,
                10,
                10,
                10,
                64,
                8,
                &authorized_graphs,
            )
            .is_ok()
        );

        response.bindings.reverse();
        assert!(
            validate_cached_query_response(
                &response,
                &key,
                &routing,
                "certified-test",
                QueryForm::Select,
                &result_sha256,
                false,
                limits,
                namespace,
                10,
                10,
                10,
                64,
                8,
                &authorized_graphs,
            )
            .is_ok()
        );
        response.bindings.reverse();
        response.qualified_entities[0].guid = Uuid::from_u128(99);
        assert!(
            validate_cached_query_response(
                &response,
                &key,
                &routing,
                "certified-test",
                QueryForm::Select,
                &result_sha256,
                false,
                limits,
                namespace,
                10,
                10,
                10,
                64,
                8,
                &authorized_graphs,
            )
            .is_err()
        );
        Ok(())
    }

    #[test]
    fn legacy_or_unknown_result_hash_versions_fail_before_cache_use() {
        assert!(require_supported_result_hash_version(CERTIFIED_QUERY_RESULT_HASH_VERSION).is_ok());
        assert!(require_supported_result_hash_version(0).is_err());
        assert!(require_supported_result_hash_version(1).is_err());
        assert!(
            require_supported_result_hash_version(CERTIFIED_QUERY_RESULT_HASH_VERSION + 1).is_err()
        );
    }

    #[test]
    fn routed_runtime_cache_is_bounded_and_lru_ordered() {
        let mut cache = BoundedLruCache::new();
        assert!(cache.insert("a".to_owned(), Arc::new(1_u8), 2).is_none());
        assert!(cache.insert("b".to_owned(), Arc::new(2_u8), 2).is_none());
        assert_eq!(cache.get("a").as_deref(), Some(&1));
        assert_eq!(
            cache.insert("c".to_owned(), Arc::new(3_u8), 2),
            Some("b".to_owned())
        );
        assert!(cache.get("b").is_none());
        assert_eq!(cache.get("a").as_deref(), Some(&1));
        assert_eq!(cache.get("c").as_deref(), Some(&3));
    }

    #[test]
    fn zero_capacity_never_retains_a_runtime() {
        let mut cache = BoundedLruCache::new();
        assert!(cache.insert("a".to_owned(), Arc::new(1_u8), 0).is_none());
        assert!(cache.get("a").is_none());
    }

    #[test]
    fn fragment_serialization_buffer_stops_before_crossing_limit() {
        let mut buffer = BoundedBuffer::new(4);
        assert!(buffer.write_all(b"1234").is_ok());
        assert!(buffer.write_all(b"5").is_err());
        assert_eq!(buffer.into_bytes(), b"1234");
    }

    #[test]
    fn arrow_body_writer_chunks_and_stops_at_the_response_ceiling() {
        let (sender, mut receiver) = tokio::sync::mpsc::channel(2);
        let mut writer = ArrowBodyWriter::new(sender, 4, 5);
        assert!(writer.write_all(b"12345").is_ok());
        assert!(writer.flush().is_ok());
        assert!(writer.write_all(b"6").is_err());
        let first = receiver.try_recv();
        let second = receiver.try_recv();
        assert_eq!(
            first.ok().and_then(Result::ok).as_deref(),
            Some(&b"1234"[..])
        );
        assert_eq!(second.ok().and_then(Result::ok).as_deref(), Some(&b"5"[..]));
    }

    #[test]
    fn arrow_request_writer_streams_exact_chunks_and_emits_online_evidence()
    -> Result<(), Box<dyn std::error::Error>> {
        let (sender, mut receiver) = tokio::sync::mpsc::channel(3);
        let exchange = Arc::new(AtomicUsize::new(0));
        let mut writer = ArrowRequestWriter::new(sender, 4, 8, Arc::clone(&exchange), 8);
        assert!(writer.write_all(b"12345").is_ok());
        let evidence = writer.complete()?;
        assert_eq!(evidence.bytes, 5);
        assert_eq!(evidence.sha256, hex::encode(Sha256::digest(b"12345")));
        assert_eq!(exchange.load(std::sync::atomic::Ordering::Acquire), 5);
        assert_eq!(
            receiver.try_recv().ok().and_then(Result::ok).as_deref(),
            Some(&b"1234"[..])
        );
        assert_eq!(
            receiver.try_recv().ok().and_then(Result::ok).as_deref(),
            Some(&b"5"[..])
        );
        assert!(writer.write_all(b"6789").is_err());
        assert_eq!(exchange.load(std::sync::atomic::Ordering::Acquire), 5);
        Ok(())
    }

    #[test]
    fn shuffle_exchange_reservation_never_crosses_the_query_ceiling() {
        let total = AtomicUsize::new(0);
        assert!(reserve_exchange_bytes(&total, 4, 5).is_ok());
        assert!(reserve_exchange_bytes(&total, 1, 5).is_ok());
        assert!(reserve_exchange_bytes(&total, 1, 5).is_err());
        assert_eq!(total.into_inner(), 5);
    }

    #[test]
    fn shuffle_output_head_preserves_left_order_and_adds_right_variables_once() {
        assert_eq!(
            union_binding_heads(
                &["asset".to_owned(), "event".to_owned()],
                &["event".to_owned(), "failure".to_owned()],
            ),
            vec!["asset".to_owned(), "event".to_owned(), "failure".to_owned(),]
        );
    }

    #[test]
    fn cached_shuffle_result_is_revalidated_before_reuse() -> Result<(), OnlineError> {
        let left = vec![json!({"x": {"type": "uri", "value": "urn:x:1"}})];
        let right = vec![json!({
            "x": {"type": "uri", "value": "urn:x:1"},
            "y": {"type": "literal", "value": "one"}
        })];
        let keys = vec!["x".to_owned()];
        let partition_count = 4;
        let partition = shuffle_partition_for_binding(&left[0], &keys, partition_count)
            .map_err(super::distributed_execution_error)?;
        let head = vec!["x".to_owned(), "y".to_owned()];
        let root =
            std::env::temp_dir().join(format!("ngkg-phase30-online-grace-test-{}", Uuid::new_v4()));
        let engine = GraceJoinEngine::open(&root, 1_000_000, 500_000, 4, 8, 2, 2, 4096, 2)
            .map_err(OnlineError::GraceJoin)?;
        let identity = GraceJoinIdentity {
            tenant_id: Uuid::from_u128(1),
            dataset_id: Uuid::from_u128(2),
            snapshot_id: Uuid::from_u128(3),
            query_sha256: "1".repeat(64),
            plan_sha256: "2".repeat(64),
            stage: 0,
            partition,
            partition_count,
            left_input_sha256: "3".repeat(64),
            right_input_sha256: "4".repeat(64),
        };
        let result = compute_shuffle_result(
            &engine,
            &identity,
            left,
            right,
            head.clone(),
            keys.clone(),
            partition_count,
            partition,
            8,
        )?;
        let payload = serde_json::to_vec(&result)?;
        assert!(
            validate_cached_shuffle_result(
                &payload,
                &head,
                &keys,
                partition_count,
                partition,
                8,
                4,
                2,
            )?
            .is_some()
        );
        let mut corrupt: serde_json::Value = serde_json::from_slice(&payload)?;
        corrupt["multisetSha256"] = serde_json::Value::String("0".repeat(64));
        assert!(
            validate_cached_shuffle_result(
                &serde_json::to_vec(&corrupt)?,
                &head,
                &keys,
                partition_count,
                partition,
                8,
                4,
                2,
            )?
            .is_none()
        );
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[tokio::test]
    async fn admission_is_bounded_and_releases_after_response_body() -> Result<(), OnlineError> {
        let tenant = Uuid::from_u128(1);
        let controller = admission_controller(
            &[tenant],
            [1, 1, 1, 1, 1],
            [1, 1, 1, 1, 1],
            1,
            std::time::Duration::from_millis(1),
        )
        .map_err(OnlineError::Request)?;
        let lease = controller
            .acquire(AdmissionClass::Query, tenant)
            .await
            .map_err(|_| OnlineError::Request("first admission failed".to_owned()))?;
        assert_eq!(
            controller.counters[AdmissionClass::Query.index()]
                .in_flight
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );
        assert!(matches!(
            controller.acquire(AdmissionClass::Query, tenant).await,
            Err(AdmissionFailure::TimedOut(AdmissionScope::Tenant))
        ));
        let response = hold_admission_through_body(
            axum::response::Response::new(Body::from("exact-body")),
            lease,
        );
        let bytes = axum::body::to_bytes(response.into_body(), 1024)
            .await
            .map_err(|error| OnlineError::Upstream(error.to_string()))?;
        assert_eq!(&bytes[..], b"exact-body");
        assert_eq!(
            controller.counters[AdmissionClass::Query.index()]
                .in_flight
                .load(std::sync::atomic::Ordering::Relaxed),
            0
        );
        let _next = controller
            .acquire(AdmissionClass::Query, tenant)
            .await
            .map_err(|_| OnlineError::Request("released lane was not reusable".to_owned()))?;
        Ok(())
    }

    #[tokio::test]
    async fn admission_pending_queue_has_a_hard_count_bound() -> Result<(), OnlineError> {
        let tenant_a = Uuid::from_u128(1);
        let tenant_b = Uuid::from_u128(2);
        let peer_tenant = Uuid::from_u128(3);
        let controller = admission_controller(
            &[tenant_a, tenant_b, peer_tenant],
            [2, 2, 2, 2, 2],
            [2, 2, 2, 2, 2],
            2,
            std::time::Duration::from_millis(100),
        )
        .map_err(OnlineError::Request)?;
        let active_a = controller
            .acquire(AdmissionClass::Query, tenant_a)
            .await
            .map_err(|_| OnlineError::Request("tenant A admission failed".to_owned()))?;
        let active_b = controller
            .acquire(AdmissionClass::Query, tenant_b)
            .await
            .map_err(|_| OnlineError::Request("tenant B admission failed".to_owned()))?;
        let waiting_a = Arc::clone(&controller);
        let waiter_a =
            tokio::spawn(async move { waiting_a.acquire(AdmissionClass::Query, tenant_a).await });
        let waiting_b = Arc::clone(&controller);
        let waiter_b =
            tokio::spawn(async move { waiting_b.acquire(AdmissionClass::Query, tenant_b).await });
        for _attempt in 0..100 {
            if controller.pending_semaphores[AdmissionClass::Query.index()].available_permits() == 0
            {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(
            controller.pending_semaphores[AdmissionClass::Query.index()].available_permits(),
            0
        );
        assert!(matches!(
            controller.acquire(AdmissionClass::Query, peer_tenant).await,
            Err(AdmissionFailure::TimedOut(AdmissionScope::Global))
        ));
        drop(active_a);
        drop(active_b);
        assert!(waiter_a.await?.is_ok());
        assert!(waiter_b.await?.is_ok());
        Ok(())
    }

    #[tokio::test]
    async fn saturated_tenant_cannot_consume_another_tenants_lane() -> Result<(), OnlineError> {
        let tenant_a = Uuid::from_u128(1);
        let tenant_b = Uuid::from_u128(2);
        let controller = admission_controller(
            &[tenant_a, tenant_b],
            [2, 2, 2, 2, 2],
            [2, 2, 2, 2, 2],
            2,
            std::time::Duration::from_millis(1),
        )
        .map_err(OnlineError::Request)?;
        let _tenant_a_active = controller
            .acquire(AdmissionClass::Query, tenant_a)
            .await
            .map_err(|_| OnlineError::Request("tenant A admission failed".to_owned()))?;
        assert!(matches!(
            controller.acquire(AdmissionClass::Query, tenant_a).await,
            Err(AdmissionFailure::TimedOut(AdmissionScope::Tenant))
        ));
        let _tenant_b_active = controller
            .acquire(AdmissionClass::Query, tenant_b)
            .await
            .map_err(|_| OnlineError::Request("tenant B was starved".to_owned()))?;
        Ok(())
    }

    #[test]
    fn admission_overload_is_explicitly_retryable() {
        let response = admission_rejection(
            StatusCode::TOO_MANY_REQUESTS,
            "ADMISSION_CAPACITY_EXHAUSTED",
            "busy",
            true,
        );
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(
            response
                .headers()
                .get("retry-after")
                .and_then(|value| value.to_str().ok()),
            Some("1")
        );
    }

    #[test]
    fn spill_partitions_round_trip_and_cleanup_exact_rows() -> Result<(), OnlineError> {
        let root = spill_root();
        prepare_shuffle_spill_root(&root)?;
        let identity = SpillIdentity {
            dataset_id: Uuid::from_u128(1),
            snapshot_id: Uuid::from_u128(2),
            query_sha256: [3; 32],
            stage: 0,
            partition_count: 4,
        };
        let left = vec![
            json!({"x": {"type": "uri", "value": "urn:x:1"}}),
            json!({"x": {"type": "uri", "value": "urn:x:2"}}),
        ];
        let right = vec![
            json!({"x": {"type": "uri", "value": "urn:x:1"}, "y": {"type": "uri", "value": "urn:y:1"}}),
            json!({"x": {"type": "uri", "value": "urn:x:2"}, "y": {"type": "uri", "value": "urn:y:2"}}),
        ];
        let stage = ShuffleSpillStage::create(
            &root,
            identity,
            left,
            right,
            &["x".to_owned()],
            1024 * 1024,
            8,
        )?;
        let mut observed = 0_usize;
        for partition in 0..4 {
            let (left_rows, right_rows) = stage.read_pair(partition, &["x".to_owned()], 4)?;
            observed = observed
                .checked_add(left_rows.len())
                .and_then(|value| value.checked_add(right_rows.len()))
                .ok_or_else(|| OnlineError::Request("test row count overflow".to_owned()))?;
        }
        assert_eq!(observed, 4);
        assert!(stage.total_bytes > 0);
        stage.cleanup()?;
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn incremental_primary_partitioning_matches_owned_rows_and_cleans_source_failure()
    -> Result<(), OnlineError> {
        let root = spill_root();
        prepare_shuffle_spill_root(&root)?;
        let left = vec![
            json!({"x": {"type": "uri", "value": "urn:x:1"}}),
            json!({"x": {"type": "uri", "value": "urn:x:2"}}),
        ];
        let right = vec![
            json!({"x": {"type": "uri", "value": "urn:x:1"}, "y": {"type": "literal", "value": "one"}}),
            json!({"x": {"type": "uri", "value": "urn:x:2"}, "y": {"type": "literal", "value": "two"}}),
        ];
        let stage = ShuffleSpillStage::create_iter(
            &root,
            SpillIdentity {
                dataset_id: Uuid::from_u128(1),
                snapshot_id: Uuid::from_u128(2),
                query_sha256: [4; 32],
                stage: 0,
                partition_count: 4,
            },
            left.clone().into_iter().map(Ok),
            right.clone().into_iter().map(Ok),
            &["x".to_owned()],
            1024 * 1024,
            8,
        )?;
        let mut replayed_left = Vec::new();
        let mut replayed_right = Vec::new();
        for partition in 0..4 {
            let (mut partition_left, mut partition_right) =
                stage.read_pair(partition, &["x".to_owned()], 4)?;
            replayed_left.append(&mut partition_left);
            replayed_right.append(&mut partition_right);
        }
        replayed_left.sort_by_key(|value| value.to_string());
        replayed_right.sort_by_key(|value| value.to_string());
        let mut expected_left = left;
        let mut expected_right = right;
        expected_left.sort_by_key(|value| value.to_string());
        expected_right.sort_by_key(|value| value.to_string());
        assert_eq!(replayed_left, expected_left);
        assert_eq!(replayed_right, expected_right);
        stage.cleanup()?;

        let failed = ShuffleSpillStage::create_iter(
            &root,
            SpillIdentity {
                dataset_id: Uuid::from_u128(1),
                snapshot_id: Uuid::from_u128(2),
                query_sha256: [5; 32],
                stage: 1,
                partition_count: 2,
            },
            vec![Err(OnlineError::Request(
                "injected source failure".to_owned(),
            ))],
            std::iter::empty::<Result<serde_json::Value, OnlineError>>(),
            &["x".to_owned()],
            1024 * 1024,
            4,
        );
        assert!(failed.is_err());
        assert_eq!(fs::read_dir(&root)?.count(), 1);
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn spill_partition_rejects_post_write_corruption() -> Result<(), OnlineError> {
        let root = spill_root();
        prepare_shuffle_spill_root(&root)?;
        let stage = ShuffleSpillStage::create(
            &root,
            SpillIdentity {
                dataset_id: Uuid::from_u128(1),
                snapshot_id: Uuid::from_u128(2),
                query_sha256: [3; 32],
                stage: 0,
                partition_count: 2,
            },
            vec![json!({"x": {"type": "uri", "value": "urn:x:1"}})],
            Vec::new(),
            &["x".to_owned()],
            1024 * 1024,
            4,
        )?;
        let target = stage.left[0].path.clone();
        OpenOptions::new()
            .append(true)
            .open(target)?
            .write_all(b"corrupt")?;
        assert!(stage.read_pair(0, &["x".to_owned()], 4).is_err());
        drop(stage);
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn query_log_duration_uses_unbounded_minutes_and_seconds() {
        assert_eq!(super::human_duration(90_000), "1min 30s");
        assert_eq!(super::human_duration(13_812_000), "230min 12s");
        assert_eq!(super::human_duration(999), "0min 0s");
    }
}
