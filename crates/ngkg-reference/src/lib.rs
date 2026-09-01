//! Real single-node NGKG reference compiler and certified query runner.
//!
//! This crate is intentionally the semantic reference milestone. It uses real TriG,
//! Parquet, an external OWL 2 DL reasoner adapter, SPARQL evaluation, a direct locator,
//! and immutable checksums. It does not pretend to be the later distributed engine.

mod compiler;
mod datatype_policy;
mod direct_exact;
mod locator;
mod model;
mod parquet_io;
mod query;
mod rdf;
mod reasoner;

use std::{
    collections::BTreeSet,
    fs::{self, File},
    io::{Read, Write},
    path::{Component, Path},
};

pub use compiler::{ReferenceCompileError, compile_from_manifest};
pub use datatype_policy::{
    DatatypeLexicalLimits, DatatypePolicy, DatatypePolicyError, DatatypeValidationSummary,
    SupportedDatatype,
};
pub use direct_exact::{DirectActiveOntologyError, build_direct_active_ontology_bundle};
pub use model::{
    ArtifactRecord, CertifiedFragmentResult, CertifiedQueryInput, CertifiedQueryRecord,
    CertifiedSemanticResult, CompilationBundle, CompileLimits, DistributedQueryCertificate,
    DistributedQueryFragment, DistributedQueryPlanFile, GraphCapabilityIndexFile,
    GraphCapabilityRecord, HydratedPayload, InputArtifact, ObjectArtifact,
    ObjectCertifiedQueryInput, OwlSignature, PredicateRule, ProjectionPolicy,
    QueryRoutingCertificate, ReasoningPolicy, ReferenceCompileManifest, ReferenceQueryResult,
    ReferenceSnapshotManifest, Treatment, TrustedReasonerConfig, TrustedResourceCeilings,
};
pub use ngkg_dataset::{
    DatasetError, DatasetSelectionSource, GraphCatalog, GraphDeclaration, GraphRecord,
    LogicalGraphName, ProtocolDatasetSpecification, QueryDatasetSpecification, ResolvedDataset,
    compile_catalog, resolve_dataset,
};
use ngkg_identity::guid_for_canonical_iri;
pub use ngkg_sparql_compiler::{CompiledSparqlQuery, QueryForm, SPARQL_ALGEBRA_FORMAT_VERSION};
use oxigraph::sparql::CancellationToken;
pub use rdf::{
    GraphScope, NormalizedFact, NormalizedObject, RdfCompileError, ResourceTermKind, nquad_line,
    ntriple_line, parse_nquads, parse_trig, public_resource_lexical,
};
use sha2::{Digest, Sha256};
use thiserror::Error;
use uuid::Uuid;

use crate::{
    locator::{LocatorIndex, LocatorRecord},
    parquet_io::hydrate_rows,
    query::{
        QUERY_RESULT_HASH_VERSION, QueryExecutionLimits, build_store,
        canonical_query_result_sha256, execute_compiled_query,
        execute_compiled_query_with_dataset_cancellable,
        execute_compiled_query_with_dataset_federated_cancellable, execute_select, query_file,
    },
};

pub use crate::query::{
    DefaultDatasetPolicy, ExecutedQueryResult, ExpectedQueryResult,
    QUERY_RESULT_HASH_VERSION as CERTIFIED_QUERY_RESULT_HASH_VERSION,
    QueryExecutionLimits as CertifiedQueryExecutionLimits, canonical_query_payload_sha256,
    canonical_sparql_multiset_sha256, execute_compiled_query_with_default_policy,
    execute_entailment_rewritten_query_with_dataset_cancellable,
    execute_entailment_rewritten_query_with_dataset_federated_cancellable, load_rdf_fixture,
    load_rdf_fixture_with_base_iri, parse_expected, query_dataset_specification, verify_expected,
};

#[derive(Debug, Error)]
pub enum ReferenceRuntimeError {
    #[error("snapshot or query I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("snapshot manifest JSON is invalid: {0}")]
    Manifest(#[from] serde_json::Error),
    #[error("snapshot manifest format version is unsupported")]
    FormatVersion,
    #[error("snapshot artifact path is unsafe: {0}")]
    UnsafeArtifact(String),
    #[error("snapshot artifact checksum or size mismatch: {0}")]
    ArtifactMismatch(String),
    #[error("query path escapes the configured query root")]
    QueryRoot,
    #[error("query hash has no exact certificate for this snapshot")]
    UncertifiedQuery,
    #[error("SPARQL execution failed: {0}")]
    Query(#[from] crate::query::ReferenceQueryError),
    #[error("locator lookup failed: {0}")]
    Locator(#[from] crate::locator::LocatorFileError),
    #[error("payload hydration failed: {0}")]
    Hydration(#[from] crate::parquet_io::ParquetIoError),
    #[error("entity IRI could not be converted to the snapshot GUID namespace: {0}")]
    Identity(String),
}

/// Checksum-verified in-memory semantic replica for exact certified query hashes.
pub struct CertifiedSemanticRuntime {
    manifest: ReferenceSnapshotManifest,
    store: oxigraph::store::Store,
    reasoner_report_sha256: String,
    only_query_sha256: Option<String>,
}

impl CertifiedSemanticRuntime {
    /// Open only the semantic artifacts required by the online query plane.
    pub fn open(
        snapshot_manifest_path: &Path,
        expected_snapshot_manifest_sha256: &str,
        query_dataset_path: &Path,
        closure_path: &Path,
    ) -> Result<Self, ReferenceRuntimeError> {
        let observed = hex::encode(sha256_file(snapshot_manifest_path)?);
        if decode_sha256(expected_snapshot_manifest_sha256).is_none()
            || observed != expected_snapshot_manifest_sha256
        {
            return Err(ReferenceRuntimeError::ArtifactMismatch(
                "snapshot-manifest.json".to_owned(),
            ));
        }
        let manifest: ReferenceSnapshotManifest =
            serde_json::from_slice(&fs::read(snapshot_manifest_path)?)?;
        if manifest.format_version != 1 || manifest.certified_queries.is_empty() {
            return Err(ReferenceRuntimeError::FormatVersion);
        }
        verify_semantic_qualification_bindings(&manifest)?;
        verify_selected_artifact(&manifest, "data/query-dataset.nq", query_dataset_path)?;
        verify_selected_artifact(&manifest, "reasoner/closure.nt", closure_path)?;
        let reasoner_report_sha256 = manifest
            .artifacts
            .iter()
            .find(|artifact| artifact.relative_path == "reasoner/report.json")
            .map(|artifact| artifact.sha256.clone())
            .ok_or_else(|| {
                ReferenceRuntimeError::ArtifactMismatch("reasoner/report.json".to_owned())
            })?;
        if manifest
            .certified_queries
            .iter()
            .any(|query| query.reasoner_report_sha256 != reasoner_report_sha256)
        {
            return Err(ReferenceRuntimeError::ArtifactMismatch(
                "coverage/reasoner report binding".to_owned(),
            ));
        }
        let store = build_store(
            query_dataset_path,
            closure_path,
            &manifest.closure_graph_iri,
        )?;
        Ok(Self {
            manifest,
            store,
            reasoner_report_sha256,
            only_query_sha256: None,
        })
    }

    /// Open a per-query routed dataset only when its offline routing proof is complete.
    pub fn open_routed(
        snapshot_manifest_path: &Path,
        expected_snapshot_manifest_sha256: &str,
        query_sha256: &str,
        routed_dataset_path: &Path,
        closure_path: &Path,
    ) -> Result<Self, ReferenceRuntimeError> {
        let observed = hex::encode(sha256_file(snapshot_manifest_path)?);
        if decode_sha256(expected_snapshot_manifest_sha256).is_none()
            || observed != expected_snapshot_manifest_sha256
        {
            return Err(ReferenceRuntimeError::ArtifactMismatch(
                "snapshot-manifest.json".to_owned(),
            ));
        }
        let manifest: ReferenceSnapshotManifest =
            serde_json::from_slice(&fs::read(snapshot_manifest_path)?)?;
        if manifest.format_version != 1 || manifest.certified_queries.is_empty() {
            return Err(ReferenceRuntimeError::FormatVersion);
        }
        verify_semantic_qualification_bindings(&manifest)?;
        let certificate = manifest
            .certified_queries
            .iter()
            .find(|query| query.query_sha256 == query_sha256)
            .ok_or(ReferenceRuntimeError::UncertifiedQuery)?;
        let routing = certificate.routing.as_ref().ok_or_else(|| {
            ReferenceRuntimeError::ArtifactMismatch("query routing certificate".to_owned())
        })?;
        let expected_route = format!("data/routes/{query_sha256}.nq");
        if routing.format_version != 1
            || usize::try_from(routing.total_graph_count)
                .ok()
                .is_none_or(|count| routing.selected_graph_iris.len() > count)
            || decode_sha256(&routing.active_dataset_sha256).is_none()
            || routing.route_artifact_relative_path != expected_route
            || routing.routed_result_sha256 != certificate.observed_result_sha256
            || routing.routed_multiset_sha256 != certificate.observed_multiset_sha256
            || routing.route_artifact_sha256 != hex::encode(sha256_file(routed_dataset_path)?)
            || routing.route_artifact_bytes != fs::metadata(routed_dataset_path)?.len()
        {
            return Err(ReferenceRuntimeError::ArtifactMismatch(
                routing.route_artifact_relative_path.clone(),
            ));
        }
        verify_selected_artifact(
            &manifest,
            &routing.route_artifact_relative_path,
            routed_dataset_path,
        )?;
        verify_selected_artifact(&manifest, "reasoner/closure.nt", closure_path)?;
        let reasoner_report_sha256 = manifest
            .artifacts
            .iter()
            .find(|artifact| artifact.relative_path == "reasoner/report.json")
            .map(|artifact| artifact.sha256.clone())
            .ok_or_else(|| {
                ReferenceRuntimeError::ArtifactMismatch("reasoner/report.json".to_owned())
            })?;
        if certificate.reasoner_report_sha256 != reasoner_report_sha256 {
            return Err(ReferenceRuntimeError::ArtifactMismatch(
                "coverage/reasoner report binding".to_owned(),
            ));
        }
        let store = build_store(
            routed_dataset_path,
            closure_path,
            &manifest.closure_graph_iri,
        )?;
        Ok(Self {
            manifest,
            store,
            reasoner_report_sha256,
            only_query_sha256: Some(query_sha256.to_owned()),
        })
    }

    /// Execute an exact query only when the immutable snapshot certifies its byte hash.
    pub fn execute(&self, query: &str) -> Result<CertifiedSemanticResult, ReferenceRuntimeError> {
        let compiled =
            CompiledSparqlQuery::parse(query).map_err(crate::query::ReferenceQueryError::from)?;
        self.execute_compiled(query, &compiled)
    }

    /// Execute an already-parsed query using the compile-time certified result ceilings.
    pub fn execute_compiled(
        &self,
        query: &str,
        compiled: &CompiledSparqlQuery,
    ) -> Result<CertifiedSemanticResult, ReferenceRuntimeError> {
        let (query_sha256, certificate) = self.require_certificate(query, compiled)?;
        let limits = certificate_query_limits(certificate)?;
        let executed = execute_compiled_query(&self.store, compiled, limits)?;
        self.certified_result(query_sha256, certificate, executed, limits)
    }

    /// Execute an exact query against a graph-authorized, precedence-resolved dataset.
    pub fn execute_with_dataset(
        &self,
        query: &str,
        dataset: &ResolvedDataset,
        graph_catalog: &GraphCatalog,
    ) -> Result<CertifiedSemanticResult, ReferenceRuntimeError> {
        let compiled =
            CompiledSparqlQuery::parse(query).map_err(crate::query::ReferenceQueryError::from)?;
        self.execute_compiled_with_dataset(query, &compiled, dataset, graph_catalog)
    }

    /// Execute an already-parsed query using its checksum-bound compile-time ceilings.
    pub fn execute_compiled_with_dataset(
        &self,
        query: &str,
        compiled: &CompiledSparqlQuery,
        dataset: &ResolvedDataset,
        graph_catalog: &GraphCatalog,
    ) -> Result<CertifiedSemanticResult, ReferenceRuntimeError> {
        let (_, certificate) = self.require_certificate(query, compiled)?;
        let limits = certificate_query_limits(certificate)?;
        self.execute_compiled_with_dataset_bounded(query, compiled, dataset, graph_catalog, limits)
    }

    /// Execute with deployment limits that may only tighten the checksum-bound snapshot ceilings.
    pub fn execute_compiled_with_dataset_bounded(
        &self,
        query: &str,
        compiled: &CompiledSparqlQuery,
        dataset: &ResolvedDataset,
        graph_catalog: &GraphCatalog,
        deployment_limits: QueryExecutionLimits,
    ) -> Result<CertifiedSemanticResult, ReferenceRuntimeError> {
        self.execute_compiled_with_dataset_bounded_cancellable(
            query,
            compiled,
            dataset,
            graph_catalog,
            deployment_limits,
            None,
        )
    }

    /// Bounded exact execution with cooperative SPARQL evaluator cancellation.
    pub fn execute_compiled_with_dataset_bounded_cancellable(
        &self,
        query: &str,
        compiled: &CompiledSparqlQuery,
        dataset: &ResolvedDataset,
        graph_catalog: &GraphCatalog,
        deployment_limits: QueryExecutionLimits,
        cancellation_token: Option<CancellationToken>,
    ) -> Result<CertifiedSemanticResult, ReferenceRuntimeError> {
        let (query_sha256, certificate) = self.require_certificate(query, compiled)?;
        let certified_limits = certificate_query_limits(certificate)?;
        let deployment_limits = deployment_limits.validate()?;
        let limits = QueryExecutionLimits {
            max_solution_rows: certified_limits
                .max_solution_rows
                .min(deployment_limits.max_solution_rows),
            max_graph_triples: certified_limits
                .max_graph_triples
                .min(deployment_limits.max_graph_triples),
            max_graph_blank_nodes: certified_limits
                .max_graph_blank_nodes
                .min(deployment_limits.max_graph_blank_nodes),
        }
        .validate()?;
        let routing = certificate.routing.as_ref().ok_or_else(|| {
            ReferenceRuntimeError::ArtifactMismatch("query routing certificate".to_owned())
        })?;
        if dataset.active_dataset_sha256 != routing.active_dataset_sha256 {
            return Err(ReferenceRuntimeError::ArtifactMismatch(
                "active SPARQL dataset certificate".to_owned(),
            ));
        }
        let executed = execute_compiled_query_with_dataset_cancellable(
            &self.store,
            compiled,
            dataset,
            graph_catalog,
            routing.include_internal_closure,
            limits,
            cancellation_token,
        )?;
        self.certified_result(query_sha256, certificate, executed, limits)
    }

    fn require_certificate(
        &self,
        query: &str,
        compiled: &CompiledSparqlQuery,
    ) -> Result<(String, &CertifiedQueryRecord), ReferenceRuntimeError> {
        let query_sha256 = hex::encode(Sha256::digest(query.as_bytes()));
        if self
            .only_query_sha256
            .as_ref()
            .is_some_and(|expected| expected != &query_sha256)
        {
            return Err(ReferenceRuntimeError::UncertifiedQuery);
        }
        let certificate = self
            .manifest
            .certified_queries
            .iter()
            .find(|value| value.query_sha256 == query_sha256)
            .ok_or(ReferenceRuntimeError::UncertifiedQuery)?;
        if certificate.result_hash_version != QUERY_RESULT_HASH_VERSION
            || certificate.reasoner_report_sha256 != self.reasoner_report_sha256
            || certificate.sparql_algebra_format_version != SPARQL_ALGEBRA_FORMAT_VERSION
            || certificate.sparql_algebra_sha256 != compiled.canonical_sse_sha256()
            || certificate.query_form != compiled.form()
            || certificate.ordered != compiled.solution_order_is_significant()
        {
            return Err(ReferenceRuntimeError::ArtifactMismatch(
                "coverage/reasoner/result/algebra certificate binding".to_owned(),
            ));
        }
        let _limits = certificate_query_limits(certificate)?;
        Ok((query_sha256, certificate))
    }

    /// Execute a legal parsed SPARQL query without requiring a precompiled query hash.
    ///
    /// This is the Phase 39.4 correctness path for ad-hoc SPARQL. It remains bounded,
    /// authorization/dataset resolved by the caller, and uses the complete scalar RDF
    /// evaluator over the immutable snapshot plus the already-qualified finite closure.
    /// It is deliberately not an OWL 2 Direct-Semantics certificate; Phase 40 adds that
    /// semantic gate and exact reasoner fallback.
    pub fn execute_uncertified_compiled_with_dataset_bounded_cancellable(
        &self,
        query: &str,
        compiled: &CompiledSparqlQuery,
        dataset: &ResolvedDataset,
        graph_catalog: &GraphCatalog,
        deployment_limits: QueryExecutionLimits,
        cancellation_token: Option<CancellationToken>,
    ) -> Result<CertifiedSemanticResult, ReferenceRuntimeError> {
        if self.only_query_sha256.is_some() {
            return Err(ReferenceRuntimeError::UncertifiedQuery);
        }
        let limits = deployment_limits.validate()?;
        let include_internal_closure = matches!(
            dataset.selection_source,
            DatasetSelectionSource::ServiceDefault
        );
        let executed = execute_compiled_query_with_dataset_cancellable(
            &self.store,
            compiled,
            dataset,
            graph_catalog,
            include_internal_closure,
            limits,
            cancellation_token,
        )?;
        self.uncertified_result(
            query,
            executed,
            "phase39-exact-rdf-plus-qualified-finite-closure-v1",
        )
    }

    /// Execute uncached federated SPARQL against the exact authorized active dataset.
    ///
    /// Remote state is never accepted into immutable snapshot certification or result caches.
    #[allow(clippy::too_many_arguments)]
    pub fn execute_uncertified_federated_compiled_with_dataset_bounded_cancellable(
        &self,
        query: &str,
        compiled: &CompiledSparqlQuery,
        dataset: &ResolvedDataset,
        graph_catalog: &GraphCatalog,
        deployment_limits: QueryExecutionLimits,
        cancellation_token: Option<CancellationToken>,
        federation: ngkg_federation::FederationServiceHandler,
    ) -> Result<CertifiedSemanticResult, ReferenceRuntimeError> {
        if self.only_query_sha256.is_some() {
            return Err(ReferenceRuntimeError::UncertifiedQuery);
        }
        let limits = deployment_limits.validate()?;
        let include_internal_closure = matches!(
            dataset.selection_source,
            DatasetSelectionSource::ServiceDefault
        );
        let executed = execute_compiled_query_with_dataset_federated_cancellable(
            &self.store,
            compiled,
            dataset,
            graph_catalog,
            include_internal_closure,
            limits,
            cancellation_token,
            Some(federation),
        )?;
        self.uncertified_result(query, executed, "sparql11-secured-federation-v1")
    }

    fn uncertified_result(
        &self,
        query: &str,
        executed: ExecutedQueryResult,
        coverage_scope: &str,
    ) -> Result<CertifiedSemanticResult, ReferenceRuntimeError> {
        let query_sha256 = hex::encode(Sha256::digest(query.as_bytes()));
        let (query_form, head, bindings, boolean_result, graph_ntriples, qualified_entity_iris) =
            match executed {
                ExecutedQueryResult::Solutions(value) => (
                    QueryForm::Select,
                    value.head,
                    value.bindings,
                    None,
                    Vec::new(),
                    value.entity_iris.into_iter().collect(),
                ),
                ExecutedQueryResult::Boolean(value) => (
                    QueryForm::Ask,
                    Vec::new(),
                    Vec::new(),
                    Some(value),
                    Vec::new(),
                    Vec::new(),
                ),
                ExecutedQueryResult::Graph { form, graph } => (
                    form,
                    Vec::new(),
                    Vec::new(),
                    None,
                    graph.ntriples,
                    graph.entity_iris.into_iter().collect(),
                ),
            };
        Ok(CertifiedSemanticResult {
            dataset_id: self.manifest.dataset_id,
            snapshot_id: self.manifest.snapshot_id,
            query_sha256,
            query_form,
            head,
            bindings,
            boolean_result,
            graph_ntriples,
            qualified_entity_iris,
            coverage_scope: coverage_scope.to_owned(),
        })
    }

    /// Execute the original SPARQL outer algebra after exact OWL Direct BGP substitution.
    ///
    /// The rewritten algebra is produced only after every HermiT partition has passed the
    /// completeness barrier. The physical finite-closure graph is deliberately excluded because
    /// the exact `VALUES` relations already provide the regime-specific BGP semantics.
    pub fn execute_exact_entailment_rewritten_with_dataset_bounded_cancellable(
        &self,
        query: &str,
        compiled: &CompiledSparqlQuery,
        rewritten: spargebra::Query,
        dataset: &ResolvedDataset,
        graph_catalog: &GraphCatalog,
        deployment_limits: QueryExecutionLimits,
        cancellation_token: Option<CancellationToken>,
    ) -> Result<CertifiedSemanticResult, ReferenceRuntimeError> {
        if self.only_query_sha256.is_some() {
            return Err(ReferenceRuntimeError::UncertifiedQuery);
        }
        let limits = deployment_limits.validate()?;
        let executed = crate::query::execute_entailment_rewritten_query_with_dataset_cancellable(
            &self.store,
            compiled,
            rewritten,
            dataset,
            graph_catalog,
            limits,
            cancellation_token,
        )?;
        let query_sha256 = hex::encode(Sha256::digest(query.as_bytes()));
        let (query_form, head, bindings, boolean_result, graph_ntriples, qualified_entity_iris) =
            match executed {
                ExecutedQueryResult::Solutions(value) => (
                    QueryForm::Select,
                    value.head,
                    value.bindings,
                    None,
                    Vec::new(),
                    value.entity_iris.into_iter().collect(),
                ),
                ExecutedQueryResult::Boolean(value) => (
                    QueryForm::Ask,
                    Vec::new(),
                    Vec::new(),
                    Some(value),
                    Vec::new(),
                    Vec::new(),
                ),
                ExecutedQueryResult::Graph { form, graph } => (
                    form,
                    Vec::new(),
                    Vec::new(),
                    None,
                    graph.ntriples,
                    graph.entity_iris.into_iter().collect(),
                ),
            };
        Ok(CertifiedSemanticResult {
            dataset_id: self.manifest.dataset_id,
            snapshot_id: self.manifest.snapshot_id,
            query_sha256,
            query_form,
            head,
            bindings,
            boolean_result,
            graph_ntriples,
            qualified_entity_iris,
            coverage_scope: "owl2-direct-exact-hermit-bgp-substitution-v1".to_owned(),
        })
    }

    /// Execute exact OWL Direct BGP substitution and secured remote SERVICE operators in one
    /// scalar algebra evaluation. Local BGPs are exact HermiT relations; remote BGPs remain
    /// under the remote endpoint's advertised entailment regime.
    #[allow(clippy::too_many_arguments)]
    pub fn execute_exact_entailment_rewritten_federated_with_dataset_bounded_cancellable(
        &self,
        query: &str,
        compiled: &CompiledSparqlQuery,
        rewritten: spargebra::Query,
        dataset: &ResolvedDataset,
        graph_catalog: &GraphCatalog,
        deployment_limits: QueryExecutionLimits,
        cancellation_token: Option<CancellationToken>,
        federation: ngkg_federation::FederationServiceHandler,
    ) -> Result<CertifiedSemanticResult, ReferenceRuntimeError> {
        if self.only_query_sha256.is_some() {
            return Err(ReferenceRuntimeError::UncertifiedQuery);
        }
        let limits = deployment_limits.validate()?;
        let executed =
            crate::query::execute_entailment_rewritten_query_with_dataset_federated_cancellable(
                &self.store,
                compiled,
                rewritten,
                dataset,
                graph_catalog,
                limits,
                cancellation_token,
                Some(federation),
            )?;
        self.uncertified_result(
            query,
            executed,
            "owl2-direct-exact-hermit-plus-secured-federation-v1",
        )
    }

    fn certified_result(
        &self,
        query_sha256: String,
        certificate: &CertifiedQueryRecord,
        executed: ExecutedQueryResult,
        limits: QueryExecutionLimits,
    ) -> Result<CertifiedSemanticResult, ReferenceRuntimeError> {
        let observed = canonical_query_result_sha256(&executed, certificate.ordered, limits)?;
        if observed != certificate.observed_result_sha256 {
            return Err(ReferenceRuntimeError::ArtifactMismatch(
                "fresh semantic result differs from its offline form-aware certificate".to_owned(),
            ));
        }
        let (query_form, head, bindings, boolean_result, graph_ntriples, qualified_entity_iris) =
            match executed {
                ExecutedQueryResult::Solutions(value) => (
                    QueryForm::Select,
                    value.head,
                    value.bindings,
                    None,
                    Vec::new(),
                    value.entity_iris.into_iter().collect(),
                ),
                ExecutedQueryResult::Boolean(value) => (
                    QueryForm::Ask,
                    Vec::new(),
                    Vec::new(),
                    Some(value),
                    Vec::new(),
                    Vec::new(),
                ),
                ExecutedQueryResult::Graph { form, graph } => (
                    form,
                    Vec::new(),
                    Vec::new(),
                    None,
                    graph.ntriples,
                    graph.entity_iris.into_iter().collect(),
                ),
            };
        Ok(CertifiedSemanticResult {
            dataset_id: self.manifest.dataset_id,
            snapshot_id: self.manifest.snapshot_id,
            query_sha256,
            query_form,
            head,
            bindings,
            boolean_result,
            graph_ntriples,
            qualified_entity_iris,
            coverage_scope: certificate.scope.clone(),
        })
    }

    /// Dataset identity namespace bound into the compiled snapshot.
    #[must_use]
    pub const fn dataset_namespace(&self) -> Uuid {
        self.manifest.dataset_namespace
    }

    /// Immutable snapshot identity served by this replica.
    #[must_use]
    pub const fn snapshot_id(&self) -> Uuid {
        self.manifest.snapshot_id
    }
}

/// Checksum-verified runtime for one immutable distributed query fragment.
pub struct CertifiedFragmentRuntime {
    dataset_id: Uuid,
    snapshot_id: Uuid,
    query_sha256: String,
    fragment_id: String,
    query_text: String,
    expected_head: Vec<String>,
    expected_rows: u64,
    expected_multiset_sha256: String,
    store: oxigraph::store::Store,
}

impl CertifiedFragmentRuntime {
    /// Open one fragment only after its plan and all RDF/query inputs verify.
    #[allow(clippy::too_many_arguments)]
    pub fn open(
        snapshot_manifest_path: &Path,
        expected_snapshot_manifest_sha256: &str,
        query_sha256: &str,
        fragment_id: &str,
        plan_path: &Path,
        fragment_dataset_path: &Path,
        fragment_query_path: &Path,
        closure_path: &Path,
    ) -> Result<Self, ReferenceRuntimeError> {
        let observed_manifest = hex::encode(sha256_file(snapshot_manifest_path)?);
        if decode_sha256(expected_snapshot_manifest_sha256).is_none()
            || observed_manifest != expected_snapshot_manifest_sha256
        {
            return Err(ReferenceRuntimeError::ArtifactMismatch(
                "snapshot-manifest.json".to_owned(),
            ));
        }
        let manifest: ReferenceSnapshotManifest =
            serde_json::from_slice(&fs::read(snapshot_manifest_path)?)?;
        if manifest.format_version != 1 {
            return Err(ReferenceRuntimeError::FormatVersion);
        }
        verify_semantic_qualification_bindings(&manifest)?;
        let certificate = manifest
            .certified_queries
            .iter()
            .find(|query| query.query_sha256 == query_sha256)
            .ok_or(ReferenceRuntimeError::UncertifiedQuery)?;
        let distributed = certificate
            .routing
            .as_ref()
            .and_then(|routing| routing.distributed.as_ref())
            .ok_or_else(|| {
                ReferenceRuntimeError::ArtifactMismatch("distributed query certificate".to_owned())
            })?;
        let reasoner_report_sha256 = manifest
            .artifacts
            .iter()
            .find(|artifact| artifact.relative_path == "reasoner/report.json")
            .map(|artifact| artifact.sha256.as_str())
            .ok_or_else(|| {
                ReferenceRuntimeError::ArtifactMismatch("reasoner/report.json".to_owned())
            })?;
        if certificate.reasoner_report_sha256 != reasoner_report_sha256
            || distributed.plan_artifact_relative_path
                != format!("plans/distributed/{query_sha256}.json")
        {
            return Err(ReferenceRuntimeError::ArtifactMismatch(
                "distributed reasoner or plan binding".to_owned(),
            ));
        }
        verify_selected_artifact(
            &manifest,
            &distributed.plan_artifact_relative_path,
            plan_path,
        )?;
        if hex::encode(sha256_file(plan_path)?) != distributed.plan_artifact_sha256
            || fs::metadata(plan_path)?.len() != distributed.plan_artifact_bytes
        {
            return Err(ReferenceRuntimeError::ArtifactMismatch(
                distributed.plan_artifact_relative_path.clone(),
            ));
        }
        let plan: DistributedQueryPlanFile = serde_json::from_slice(&fs::read(plan_path)?)?;
        if plan.format_version != 1
            || plan.dataset_id != manifest.dataset_id
            || plan.snapshot_id != manifest.snapshot_id
            || plan.query_sha256 != query_sha256
            || plan.ordered
            || usize::try_from(distributed.fragment_count).ok() != Some(plan.fragments.len())
            || plan.join_order.len() != plan.fragments.len()
        {
            return Err(ReferenceRuntimeError::ArtifactMismatch(
                "distributed plan identity".to_owned(),
            ));
        }
        let fragment = plan
            .fragments
            .iter()
            .find(|fragment| fragment.fragment_id == fragment_id)
            .ok_or_else(|| {
                ReferenceRuntimeError::ArtifactMismatch("distributed fragment ID".to_owned())
            })?;
        verify_selected_artifact(
            &manifest,
            &fragment.dataset_artifact_relative_path,
            fragment_dataset_path,
        )?;
        verify_selected_artifact(
            &manifest,
            &fragment.query_artifact_relative_path,
            fragment_query_path,
        )?;
        verify_selected_artifact(&manifest, "reasoner/closure.nt", closure_path)?;
        if hex::encode(sha256_file(fragment_dataset_path)?) != fragment.dataset_artifact_sha256
            || fs::metadata(fragment_dataset_path)?.len() != fragment.dataset_artifact_bytes
            || hex::encode(sha256_file(fragment_query_path)?) != fragment.query_artifact_sha256
            || fs::metadata(fragment_query_path)?.len() != fragment.query_artifact_bytes
        {
            return Err(ReferenceRuntimeError::ArtifactMismatch(
                fragment.fragment_id.clone(),
            ));
        }
        let query_text = fs::read_to_string(fragment_query_path)?;
        let store = build_store(
            fragment_dataset_path,
            closure_path,
            &manifest.closure_graph_iri,
        )?;
        Ok(Self {
            dataset_id: manifest.dataset_id,
            snapshot_id: manifest.snapshot_id,
            query_sha256: query_sha256.to_owned(),
            fragment_id: fragment_id.to_owned(),
            query_text,
            expected_head: fragment.head.clone(),
            expected_rows: fragment.row_count,
            expected_multiset_sha256: fragment.observed_multiset_sha256.clone(),
            store,
        })
    }

    /// Execute the immutable fragment and reject any result drift.
    pub fn execute(&self) -> Result<CertifiedFragmentResult, ReferenceRuntimeError> {
        let executed = execute_select(&self.store, &self.query_text)?;
        let observed_rows = u64::try_from(executed.bindings.len()).map_err(|_| {
            ReferenceRuntimeError::ArtifactMismatch("fragment row count overflow".to_owned())
        })?;
        let multiset_sha256 =
            canonical_sparql_multiset_sha256(&executed.head, &executed.bindings, false)?;
        if executed.head != self.expected_head
            || observed_rows != self.expected_rows
            || multiset_sha256 != self.expected_multiset_sha256
        {
            return Err(ReferenceRuntimeError::ArtifactMismatch(
                "fragment result differs from offline certification".to_owned(),
            ));
        }
        Ok(CertifiedFragmentResult {
            dataset_id: self.dataset_id,
            snapshot_id: self.snapshot_id,
            query_sha256: self.query_sha256.clone(),
            fragment_id: self.fragment_id.clone(),
            head: executed.head,
            bindings: executed.bindings,
            multiset_sha256,
        })
    }
}

fn verify_selected_artifact(
    manifest: &ReferenceSnapshotManifest,
    relative_path: &str,
    path: &Path,
) -> Result<(), ReferenceRuntimeError> {
    let artifact = manifest
        .artifacts
        .iter()
        .find(|artifact| artifact.relative_path == relative_path)
        .ok_or_else(|| ReferenceRuntimeError::ArtifactMismatch(relative_path.to_owned()))?;
    let metadata = fs::metadata(path)?;
    if metadata.len() != artifact.bytes || hex::encode(sha256_file(path)?) != artifact.sha256 {
        return Err(ReferenceRuntimeError::ArtifactMismatch(
            relative_path.to_owned(),
        ));
    }
    Ok(())
}

fn certificate_query_limits(
    certificate: &CertifiedQueryRecord,
) -> Result<QueryExecutionLimits, ReferenceRuntimeError> {
    QueryExecutionLimits {
        max_solution_rows: usize::try_from(certificate.max_solution_rows).map_err(|_| {
            ReferenceRuntimeError::ArtifactMismatch("query solution-row ceiling".to_owned())
        })?,
        max_graph_triples: usize::try_from(certificate.max_graph_triples).map_err(|_| {
            ReferenceRuntimeError::ArtifactMismatch("query graph-triple ceiling".to_owned())
        })?,
        max_graph_blank_nodes: usize::try_from(certificate.max_graph_blank_nodes).map_err(
            |_| {
                ReferenceRuntimeError::ArtifactMismatch("query graph blank-node ceiling".to_owned())
            },
        )?,
    }
    .validate()
    .map_err(ReferenceRuntimeError::Query)
}

/// Execute only a query hash certified in the immutable snapshot, then hydrate payload by GUID.
pub fn execute_snapshot_query(
    snapshot_manifest_path: &Path,
    expected_snapshot_manifest_sha256: &str,
    query_path: &Path,
    allowed_query_root: &Path,
    hydrate_payload: bool,
) -> Result<ReferenceQueryResult, ReferenceRuntimeError> {
    let snapshot_manifest_path = fs::canonicalize(snapshot_manifest_path)?;
    let observed_manifest_sha256 = hex::encode(sha256_file(&snapshot_manifest_path)?);
    if decode_sha256(expected_snapshot_manifest_sha256).is_none()
        || observed_manifest_sha256 != expected_snapshot_manifest_sha256
    {
        return Err(ReferenceRuntimeError::ArtifactMismatch(
            "snapshot-manifest.json".to_owned(),
        ));
    }
    let snapshot_root = snapshot_manifest_path
        .parent()
        .ok_or_else(|| std::io::Error::other("snapshot manifest has no parent"))?;
    let manifest: ReferenceSnapshotManifest =
        serde_json::from_slice(&fs::read(&snapshot_manifest_path)?)?;
    if manifest.format_version != 1 {
        return Err(ReferenceRuntimeError::FormatVersion);
    }
    verify_semantic_qualification_bindings(&manifest)?;
    verify_artifacts(snapshot_root, &manifest.artifacts)?;

    let allowed_query_root = fs::canonicalize(allowed_query_root)?;
    let query_path = fs::canonicalize(query_path)?;
    if !query_path.starts_with(&allowed_query_root) {
        return Err(ReferenceRuntimeError::QueryRoot);
    }
    let query_sha256 = hex::encode(sha256_file(&query_path)?);
    let certificate = manifest
        .certified_queries
        .iter()
        .find(|certificate| certificate.query_sha256 == query_sha256)
        .ok_or(ReferenceRuntimeError::UncertifiedQuery)?;
    let reasoner_report = manifest
        .artifacts
        .iter()
        .find(|artifact| artifact.relative_path == "reasoner/report.json")
        .ok_or_else(|| {
            ReferenceRuntimeError::ArtifactMismatch("reasoner/report.json".to_owned())
        })?;
    if certificate.reasoner_report_sha256 != reasoner_report.sha256 {
        return Err(ReferenceRuntimeError::ArtifactMismatch(
            "coverage/reasoner report binding".to_owned(),
        ));
    }
    let store = build_store(
        &snapshot_root.join("data/query-dataset.nq"),
        &snapshot_root.join("reasoner/closure.nt"),
        &manifest.closure_graph_iri,
    )?;
    let query_text = query_file(&query_path)?;
    let compiled =
        CompiledSparqlQuery::parse(&query_text).map_err(crate::query::ReferenceQueryError::from)?;
    if certificate.result_hash_version != QUERY_RESULT_HASH_VERSION
        || certificate.query_form != compiled.form()
        || certificate.sparql_algebra_format_version != SPARQL_ALGEBRA_FORMAT_VERSION
        || certificate.sparql_algebra_sha256 != compiled.canonical_sse_sha256()
        || certificate.ordered != compiled.solution_order_is_significant()
    {
        return Err(ReferenceRuntimeError::ArtifactMismatch(
            "unsupported or mismatched query result/algebra certificate".to_owned(),
        ));
    }
    let limits = certificate_query_limits(certificate)?;
    let executed = execute_compiled_query(&store, &compiled, limits)?;
    let observed = canonical_query_result_sha256(&executed, certificate.ordered, limits)?;
    if observed != certificate.observed_result_sha256 {
        return Err(ReferenceRuntimeError::ArtifactMismatch(
            "fresh snapshot query result differs from its offline certificate".to_owned(),
        ));
    }
    let entity_iris = executed.entity_iris().clone();
    let qualified_entity_iris = entity_iris.iter().cloned().collect();
    let hydrated_payload = if hydrate_payload {
        hydrate_bound_entities(snapshot_root, &manifest, &entity_iris)?
    } else {
        Vec::new()
    };
    let (query_form, head, bindings, boolean_result, graph_ntriples) = match executed {
        ExecutedQueryResult::Solutions(value) => (
            QueryForm::Select,
            value.head,
            value.bindings,
            None,
            Vec::new(),
        ),
        ExecutedQueryResult::Boolean(value) => (
            QueryForm::Ask,
            Vec::new(),
            Vec::new(),
            Some(value),
            Vec::new(),
        ),
        ExecutedQueryResult::Graph { form, graph } => {
            (form, Vec::new(), Vec::new(), None, graph.ntriples)
        }
    };
    Ok(ReferenceQueryResult {
        dataset_id: manifest.dataset_id,
        snapshot_id: manifest.snapshot_id,
        query_sha256,
        query_form,
        head,
        bindings,
        boolean_result,
        graph_ntriples,
        qualified_entity_iris,
        hydrated_payload,
        coverage_scope: certificate.scope.clone(),
    })
}

fn hydrate_bound_entities(
    snapshot_root: &Path,
    manifest: &ReferenceSnapshotManifest,
    entity_iris: &BTreeSet<String>,
) -> Result<Vec<model::HydratedPayload>, ReferenceRuntimeError> {
    let payload_artifact = manifest
        .artifacts
        .iter()
        .find(|artifact| artifact.relative_path == "data/payload.parquet")
        .ok_or_else(|| {
            ReferenceRuntimeError::ArtifactMismatch("data/payload.parquet".to_owned())
        })?;
    let payload_hash = decode_sha256(&payload_artifact.sha256)
        .ok_or_else(|| ReferenceRuntimeError::ArtifactMismatch("payload SHA-256".to_owned()))?;
    let locator = LocatorIndex::open(
        &snapshot_root.join("indexes/locator.bin"),
        manifest.snapshot_id,
        payload_hash,
    )?;
    let mut unique = BTreeSet::new();
    for iri in entity_iris {
        let guid = guid_for_canonical_iri(manifest.dataset_namespace, iri)
            .map_err(|error| ReferenceRuntimeError::Identity(error.to_string()))?;
        for record in locator.lookup(guid) {
            unique.insert((
                *record.entity_guid.as_bytes(),
                record.row_group,
                record.row_in_group,
                record.graph_id,
                record.predicate_id,
            ));
        }
    }
    let records = unique
        .into_iter()
        .map(
            |(guid, row_group, row_in_group, graph_id, predicate_id)| LocatorRecord {
                entity_guid: uuid::Uuid::from_bytes(guid),
                row_group,
                row_in_group,
                graph_id,
                predicate_id,
            },
        )
        .collect::<Vec<_>>();
    hydrate_rows(&snapshot_root.join("data/payload.parquet"), &records)
        .map_err(ReferenceRuntimeError::Hydration)
}

fn verify_semantic_qualification_bindings(
    manifest: &ReferenceSnapshotManifest,
) -> Result<(), ReferenceRuntimeError> {
    let bindings = [
        (
            "reasoner/owl-signature.json",
            manifest.owl_signature_sha256.as_deref(),
        ),
        (
            "reasoner/datatype-policy.json",
            manifest.datatype_policy_sha256.as_deref(),
        ),
        (
            "reasoner/owl-profile-qualification.json",
            manifest.owl_profile_qualification_sha256.as_deref(),
        ),
        (
            "reasoner/owl-consistency-qualification.json",
            manifest.owl_consistency_qualification_sha256.as_deref(),
        ),
    ];
    for (relative_path, expected) in bindings {
        let Some(expected) = expected else { continue };
        if decode_sha256(expected).is_none() {
            return Err(ReferenceRuntimeError::ArtifactMismatch(
                relative_path.to_owned(),
            ));
        }
        let artifact = manifest
            .artifacts
            .iter()
            .find(|artifact| artifact.relative_path == relative_path)
            .ok_or_else(|| ReferenceRuntimeError::ArtifactMismatch(relative_path.to_owned()))?;
        if artifact.sha256 != expected {
            return Err(ReferenceRuntimeError::ArtifactMismatch(
                relative_path.to_owned(),
            ));
        }
    }
    if manifest.owl_consistency_qualification_sha256.is_some()
        && (manifest.owl_signature_sha256.is_none()
            || manifest.datatype_policy_sha256.is_none()
            || manifest.owl_profile_qualification_sha256.is_none())
    {
        return Err(ReferenceRuntimeError::ArtifactMismatch(
            "incomplete semantic qualification binding chain".to_owned(),
        ));
    }
    Ok(())
}

fn verify_artifacts(
    root: &Path,
    artifacts: &[ArtifactRecord],
) -> Result<(), ReferenceRuntimeError> {
    let mut observed_paths = BTreeSet::new();
    for artifact in artifacts {
        if !observed_paths.insert(artifact.relative_path.as_str()) {
            return Err(ReferenceRuntimeError::UnsafeArtifact(
                artifact.relative_path.clone(),
            ));
        }
        let relative = Path::new(&artifact.relative_path);
        if relative.is_absolute()
            || relative
                .components()
                .any(|component| !matches!(component, Component::Normal(_)))
        {
            return Err(ReferenceRuntimeError::UnsafeArtifact(
                artifact.relative_path.clone(),
            ));
        }
        let path = root.join(relative);
        let metadata = fs::metadata(&path)?;
        let observed = hex::encode(sha256_file(&path)?);
        if metadata.len() != artifact.bytes || observed != artifact.sha256 {
            return Err(ReferenceRuntimeError::ArtifactMismatch(
                artifact.relative_path.clone(),
            ));
        }
    }
    for required in [
        "data/query-dataset.nq",
        "data/semantic-spine.parquet",
        "data/payload.parquet",
        "indexes/dictionaries.json",
        "indexes/rdf-dataset-catalog.json",
        "indexes/graph-capabilities.json",
        "indexes/locator.bin",
        "reasoner/closure.nt",
        "reasoner/report.json",
        "certification/coverage.json",
        "certification/verification.json",
    ] {
        if !observed_paths.contains(required) {
            return Err(ReferenceRuntimeError::ArtifactMismatch(required.to_owned()));
        }
    }
    Ok(())
}

pub(crate) fn artifact_record(
    root: &Path,
    relative_path: &str,
) -> Result<ArtifactRecord, std::io::Error> {
    let path = root.join(relative_path);
    let metadata = fs::metadata(&path)?;
    Ok(ArtifactRecord {
        relative_path: relative_path.to_owned(),
        sha256: hex::encode(sha256_file(&path)?),
        bytes: metadata.len(),
    })
}

pub(crate) fn sha256_file(path: &Path) -> Result<[u8; 32], std::io::Error> {
    let mut file = File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0_u8; 1024 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hasher.finalize().into())
}

/// Hash one immutable file for catalog or work-envelope registration.
pub fn sha256_path(path: &Path) -> Result<String, std::io::Error> {
    sha256_file(path).map(hex::encode)
}

pub(crate) fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

pub(crate) fn decode_sha256(value: &str) -> Option<[u8; 32]> {
    if value.len() != 64
        || value
            .chars()
            .any(|character| !character.is_ascii_hexdigit() || character.is_ascii_uppercase())
    {
        return None;
    }
    let bytes = hex::decode(value).ok()?;
    let mut output = [0_u8; 32];
    output.copy_from_slice(&bytes);
    Some(output)
}

/// Write user-facing query output atomically so partial JSON is never presented as success.
pub fn write_query_result(
    path: &Path,
    result: &ReferenceQueryResult,
) -> Result<(), ReferenceRuntimeError> {
    let parent = path
        .parent()
        .ok_or_else(|| std::io::Error::other("result path has no parent"))?;
    if path.exists() {
        return Err(ReferenceRuntimeError::Io(std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            "query result already exists",
        )));
    }
    let temporary = parent.join(format!(
        ".{}.partial",
        path.file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("result")
    ));
    if temporary.exists() {
        return Err(ReferenceRuntimeError::Io(std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            "partial result already exists",
        )));
    }
    let bytes = serde_json::to_vec_pretty(result)?;
    let mut file = File::create(&temporary)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    fs::rename(temporary, path)?;
    File::open(parent)?.sync_all()?;
    Ok(())
}
