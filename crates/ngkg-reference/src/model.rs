//! Strict, versioned contracts for the single-node reference compiler.

use std::path::PathBuf;

use ngkg_dataset::{DatasetSelectionSource, GraphDeclaration};
use ngkg_sparql_compiler::QueryForm;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Immutable local input. The worker verifies the checksum before parsing bytes.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct InputArtifact {
    pub path: PathBuf,
    pub sha256: String,
}

/// Checksum-bound object that is materialized below an operator-owned storage root.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ObjectArtifact {
    pub object_key: String,
    pub sha256: String,
    pub file_name: String,
}

/// How a predicate is represented after aligned TriG is dehydrated.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Treatment {
    Core,
    Virtual,
    Payload,
}

impl Treatment {
    #[must_use]
    pub const fn code(self) -> u8 {
        match self {
            Self::Core => 1,
            Self::Virtual => 2,
            Self::Payload => 3,
        }
    }
}

/// Closed behavior for a predicate encountered during compilation.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct PredicateRule {
    pub predicate_iri: String,
    pub treatment: Treatment,
    pub participates_in_reasoning: bool,
    pub queryable_as_rdf: bool,
}

/// Projection rules are exhaustive: unknown predicates fail compilation.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ProjectionPolicy {
    pub policy_id: String,
    pub reject_default_graph: bool,
    pub rules: Vec<PredicateRule>,
}

/// Hard resource bounds prevent an accidental single-node denial of service.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct CompileLimits {
    pub max_input_bytes: u64,
    pub max_quads: u64,
    pub max_dictionary_terms: u64,
    pub max_reasoner_seconds: u64,
    pub parquet_row_group_rows: usize,
}

/// User-visible reasoning bounds. Executable selection is operator-controlled.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ReasoningPolicy {
    pub closure_graph_iri: String,
    pub max_named_individuals: u64,
    pub max_properties: u64,
}

/// Trusted deployment configuration; this is never read from an uploaded manifest.
#[derive(Clone, Debug)]
pub struct TrustedReasonerConfig {
    pub java_executable: PathBuf,
    pub adapter_jar: InputArtifact,
    pub expected_name: String,
    pub expected_version: String,
}

/// Operator-enforced ceilings. Uploaded manifests may request less, never more.
#[derive(Clone, Copy, Debug)]
pub struct TrustedResourceCeilings {
    pub max_input_bytes: u64,
    pub max_quads: u64,
    pub max_dictionary_terms: u64,
    pub max_reasoner_seconds: u64,
    pub max_parquet_row_group_rows: usize,
    pub max_named_individuals: u64,
    pub max_properties: u64,
}

/// One independently authored expected-answer assertion.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct CertifiedQueryInput {
    pub query_id: String,
    pub ordered: bool,
    pub query: InputArtifact,
    pub expected: InputArtifact,
    pub required_source_iris: Vec<String>,
}

/// Expected query assertion whose bytes live in immutable object storage.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ObjectCertifiedQueryInput {
    pub query_id: String,
    pub ordered: bool,
    pub query: ObjectArtifact,
    pub expected: ObjectArtifact,
    pub required_source_iris: Vec<String>,
}

/// Object-store-native request used by the Phase 14 Kubernetes worker.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct CompilationBundle {
    pub format_version: u32,
    pub dataset_id: Uuid,
    pub snapshot_id: Uuid,
    pub parent_snapshot_id: Option<Uuid>,
    pub dataset_namespace: Uuid,
    pub source_guid: Uuid,
    pub source_snapshot: String,
    pub source: ObjectArtifact,
    pub ontology_bundle: Vec<ObjectArtifact>,
    pub projection: ProjectionPolicy,
    pub reasoning: ReasoningPolicy,
    pub graph_catalog: Vec<GraphDeclaration>,
    pub certified_queries: Vec<ObjectCertifiedQueryInput>,
    pub limits: CompileLimits,
}

/// Complete immutable request consumed by one reference compilation worker.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ReferenceCompileManifest {
    pub format_version: u32,
    pub dataset_id: Uuid,
    pub snapshot_id: Uuid,
    pub parent_snapshot_id: Option<Uuid>,
    pub dataset_namespace: Uuid,
    pub source_guid: Uuid,
    pub source_snapshot: String,
    pub source: InputArtifact,
    /// Original uploaded source hash when `source` is a canonical distributed derivative.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_identity_sha256: Option<String>,
    pub ontology_bundle: Vec<InputArtifact>,
    pub output_directory: PathBuf,
    pub projection: ProjectionPolicy,
    pub reasoning: ReasoningPolicy,
    pub graph_catalog: Vec<GraphDeclaration>,
    pub certified_queries: Vec<CertifiedQueryInput>,
    pub limits: CompileLimits,
}

/// One immutable artifact emitted by the reference compiler.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ArtifactRecord {
    pub relative_path: String,
    pub sha256: String,
    pub bytes: u64,
}

/// Exact query coverage is deliberately per query hash, not a global flag.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct CertifiedQueryRecord {
    pub query_id: String,
    pub query_sha256: String,
    pub expected_sha256: String,
    /// Version of the canonical SPARQL algebra certificate.
    pub sparql_algebra_format_version: u32,
    /// SHA-256 of the parser-normalized SPARQL S-expression.
    pub sparql_algebra_sha256: String,
    /// Standards query form bound into this exact certificate.
    pub query_form: QueryForm,
    /// Version of the canonical form-aware result-hash algorithm.
    pub result_hash_version: u32,
    /// Whether SELECT solution sequence order is part of the certificate.
    pub ordered: bool,
    /// Compile-time ceiling on materialized SELECT solution rows.
    pub max_solution_rows: u64,
    /// Compile-time ceiling on materialized CONSTRUCT/DESCRIBE triples.
    pub max_graph_triples: u64,
    /// Compile-time ceiling on distinct graph-result blank nodes admitted to RDFC canonicalization.
    pub max_graph_blank_nodes: u64,
    /// Canonical form-aware result hash (SELECT bag/sequence, ASK boolean, RDF graph isomorphism).
    pub observed_result_sha256: String,
    /// Legacy SELECT multiset hash retained only for the distributed Phase 35-38 fast path.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_multiset_sha256: Option<String>,
    pub reasoner_report_sha256: String,
    pub scope: String,
    /// Exact relevant-graph route independently re-evaluated during certification.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub routing: Option<QueryRoutingCertificate>,
}

/// Snapshot-bound routing proof for one exact certified query hash.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct QueryRoutingCertificate {
    /// Contract version.
    pub format_version: u32,
    /// SHA-256 of `indexes/graph-capabilities.json`.
    pub capability_index_sha256: String,
    /// Named graphs physically present in the routed query dataset.
    pub selected_graph_iris: Vec<String>,
    /// Complete named-graph count in the source query dataset.
    pub total_graph_count: u32,
    /// Why this exact graph set was selected.
    pub selection_mode: String,
    /// Dataset precedence branch observed during offline certification.
    pub dataset_selection_source: DatasetSelectionSource,
    /// Named graphs merged into the active default graph before physical routing.
    pub default_graph_iris: Vec<String>,
    /// Named graphs available to `GRAPH` before physical routing.
    pub named_graph_iris: Vec<String>,
    /// Hash of the exact active default/named dataset.
    pub active_dataset_sha256: String,
    /// Whether the internal finite reasoner materialization was included in the
    /// active default graph during offline certification.
    pub include_internal_closure: bool,
    /// Snapshot-relative routed N-Quads artifact.
    pub route_artifact_relative_path: String,
    /// Routed artifact SHA-256.
    pub route_artifact_sha256: String,
    /// Routed artifact byte count.
    pub route_artifact_bytes: u64,
    /// Independently verified form-aware result hash for this exact routed dataset.
    pub routed_result_sha256: String,
    /// SELECT-only v1 multiset hash retained for distributed fragment/shuffle equivalence.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub routed_multiset_sha256: Option<String>,
    /// Optional cross-node plan that was independently proven equivalent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub distributed: Option<DistributedQueryCertificate>,
}

/// Snapshot artifact binding for one certified cross-node query plan.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DistributedQueryCertificate {
    /// Contract version.
    pub format_version: u32,
    /// Immutable plan artifact path.
    pub plan_artifact_relative_path: String,
    /// Immutable plan artifact SHA-256.
    pub plan_artifact_sha256: String,
    /// Immutable plan artifact byte count.
    pub plan_artifact_bytes: u64,
    /// Number of independently executable fragments.
    pub fragment_count: u32,
    /// Canonical final multiset produced by offline fragment execution and joins.
    pub distributed_multiset_sha256: String,
}

/// Complete immutable cross-node execution plan for one exact query hash.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DistributedQueryPlanFile {
    /// Contract version.
    pub format_version: u32,
    /// Dataset identity.
    pub dataset_id: Uuid,
    /// Snapshot identity.
    pub snapshot_id: Uuid,
    /// Exact original query byte hash.
    pub query_sha256: String,
    /// Whether final bag rows have significant order.
    pub ordered: bool,
    /// Final projected variables in SPARQL result order.
    pub final_head: Vec<String>,
    /// Stable fragment order used by the certified coordinator join.
    pub join_order: Vec<String>,
    /// Independently executable graph fragments.
    pub fragments: Vec<DistributedQueryFragment>,
}

/// One checksum-bound graph fragment and its certified result contract.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DistributedQueryFragment {
    /// Stable fragment identifier.
    pub fragment_id: String,
    /// Named graph evaluated by this fragment.
    pub graph_iri: String,
    /// Fragment-local N-Quads artifact path.
    pub dataset_artifact_relative_path: String,
    /// Fragment-local N-Quads SHA-256.
    pub dataset_artifact_sha256: String,
    /// Fragment-local N-Quads byte count.
    pub dataset_artifact_bytes: u64,
    /// Fragment SPARQL artifact path.
    pub query_artifact_relative_path: String,
    /// Fragment SPARQL SHA-256.
    pub query_artifact_sha256: String,
    /// Fragment SPARQL byte count.
    pub query_artifact_bytes: u64,
    /// Variables returned by the fragment evaluator.
    pub head: Vec<String>,
    /// Exact certified fragment row count including bag duplicates.
    pub row_count: u64,
    /// Canonical exact fragment result multiset.
    pub observed_multiset_sha256: String,
}

/// One named graph represented in the capability index.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GraphCapabilityRecord {
    /// Dense deterministic graph ID in lexical IRI order.
    pub graph_id: u32,
    /// Absolute named-graph IRI.
    pub graph_iri: String,
    /// Deployment-defined graph role.
    pub role: String,
    /// Authorization labels copied from the immutable graph catalog.
    pub authorization_labels: std::collections::BTreeSet<String>,
    /// Whether offline reasoning consumes this graph.
    pub reasoning_visible: bool,
    /// Query-visible facts assigned to the graph.
    pub queryable_fact_count: u64,
}

/// Offline-compiled graph routing metadata used only with a per-query certificate.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GraphCapabilityIndexFile {
    /// Contract version.
    pub format_version: u32,
    /// Dataset identity.
    pub dataset_id: Uuid,
    /// Snapshot identity.
    pub snapshot_id: Uuid,
    /// SHA-256 of `indexes/rdf-dataset-catalog.json`.
    pub graph_catalog_sha256: String,
    /// Every query-visible named graph exactly once.
    pub graphs: Vec<GraphCapabilityRecord>,
    /// Predicate IRI to candidate graph IRIs.
    pub predicate_to_graphs: std::collections::BTreeMap<String, Vec<String>>,
    /// RDF class IRI to candidate graph IRIs.
    pub class_to_graphs: std::collections::BTreeMap<String, Vec<String>>,
    /// Cross-graph entity-reference dependencies used for conservative expansion.
    pub dependencies: std::collections::BTreeMap<String, Vec<String>>,
}

/// Output of the first real compiler. This is the local atomic snapshot bill of materials.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ReferenceSnapshotManifest {
    pub format_version: u32,
    pub dataset_id: Uuid,
    pub snapshot_id: Uuid,
    pub parent_snapshot_id: Option<Uuid>,
    pub dataset_namespace: Uuid,
    pub source_sha256: String,
    pub ontology_bundle_sha256: String,
    pub projection_policy_sha256: String,
    pub dictionary_root_sha256: String,
    pub artifacts: Vec<ArtifactRecord>,
    pub certified_queries: Vec<CertifiedQueryRecord>,
    pub reasoner_name: String,
    pub reasoner_version: String,
    /// SHA-256 of `reasoner/owl-signature.json` for Phase 40.1+ snapshots.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owl_signature_sha256: Option<String>,
    /// SHA-256 of `reasoner/datatype-policy.json` for Phase 40.2+ snapshots.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub datatype_policy_sha256: Option<String>,
    /// SHA-256 of `reasoner/owl-profile-qualification.json` for Phase 40.5+ snapshots.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owl_profile_qualification_sha256: Option<String>,
    /// SHA-256 of `reasoner/owl-consistency-qualification.json` for Phase 40.6+ snapshots.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owl_consistency_qualification_sha256: Option<String>,
    pub closure_graph_iri: String,
    pub reasoning_scope: String,
    pub publication: String,
}

/// Request passed to the HermiT command adapter.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ReasonerRequest {
    pub format_version: u32,
    pub dataset_id: Uuid,
    pub snapshot_id: Uuid,
    pub inputs: Vec<ReasonerInputArtifact>,
    pub aggregate_input_sha256: String,
    pub output_closure_path: PathBuf,
    pub output_report_path: PathBuf,
    /// Required Phase 40.1 signature artifact emitted from the merged OWL ontology.
    pub output_owl_signature_path: PathBuf,
    /// Required Phase 40.5 profile/import qualification artifact emitted by OWLAPI.
    pub output_owl_profile_qualification_path: PathBuf,
    /// Required Phase 40.6 global consistency qualification artifact emitted by HermiT.
    pub output_owl_consistency_qualification_path: PathBuf,
    /// Trusted Phase 40.2 datatype policy artifact copied into the immutable snapshot.
    pub datatype_policy_path: PathBuf,
    /// SHA-256 of the exact Phase 40.2 datatype policy bytes.
    pub datatype_policy_sha256: String,
    pub max_named_individuals: u64,
    pub max_properties: u64,
}

/// Hashed reasoner input plus every ontology IRI resolved to that local document.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ReasonerInputArtifact {
    pub path: PathBuf,
    pub sha256: String,
    pub ontology_iris: Vec<String>,
}

/// Checksum-bound ontology input represented in the Phase 40.1 OWL signature.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct OwlSignatureOntologyDocument {
    pub sha256: String,
    pub ontology_iris: Vec<String>,
}

/// Deterministic signature of the exact merged ontology presented to OWLAPI/HermiT.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct OwlSignature {
    pub format_version: u32,
    pub dataset_id: Uuid,
    pub snapshot_id: Uuid,
    pub aggregate_input_sha256: String,
    pub ontology_documents: Vec<OwlSignatureOntologyDocument>,
    pub imports: Vec<String>,
    pub classes: Vec<String>,
    pub object_properties: Vec<String>,
    pub data_properties: Vec<String>,
    pub annotation_properties: Vec<String>,
    pub named_individuals: Vec<String>,
    pub datatypes: Vec<String>,
}

/// One ontology document observed by OWLAPI during Phase 40.5 qualification.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct OwlProfileOntologyDocument {
    pub sha256: String,
    pub ontology_iri: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version_iri: Option<String>,
}

/// One locally resolved owl:imports edge in the complete OWLAPI import closure.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct OwlProfileImportResolution {
    pub source_ontology_iri: String,
    pub imported_iri: String,
    pub resolved_document_sha256: String,
}

/// Deterministic Phase 40.5 evidence for import closure and merged OWL 2 DL profile qualification.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct OwlProfileQualification {
    pub format_version: u32,
    pub dataset_id: Uuid,
    pub snapshot_id: Uuid,
    pub aggregate_input_sha256: String,
    pub owl_signature_sha256: String,
    pub datatype_policy_sha256: String,
    pub owl_profile: String,
    pub direct_semantics: bool,
    pub input_document_count: u64,
    pub ontology_document_count: u64,
    pub abox_document_count: u64,
    pub loaded_ontology_count: u64,
    pub import_declaration_count: u64,
    pub resolved_import_count: u64,
    pub complete_local_import_closure: bool,
    pub merged_axiom_count: u64,
    pub ontology_documents: Vec<OwlProfileOntologyDocument>,
    pub import_resolutions: Vec<OwlProfileImportResolution>,
    pub profile_valid: bool,
    pub profile_violation_count: u64,
    pub profile_violation_samples: Vec<String>,
}

/// Deterministic Phase 40.6 evidence that HermiT checked the complete merged ontology for consistency.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct OwlConsistencyQualification {
    pub format_version: u32,
    pub dataset_id: Uuid,
    pub snapshot_id: Uuid,
    pub aggregate_input_sha256: String,
    pub owl_signature_sha256: String,
    pub datatype_policy_sha256: String,
    pub owl_profile_qualification_sha256: String,
    pub owl_profile: String,
    pub direct_semantics: bool,
    pub reasoner_name: String,
    pub reasoner_version: String,
    pub consistency_method: String,
    pub input_document_count: u64,
    pub loaded_ontology_count: u64,
    pub merged_axiom_count: u64,
    pub consistency_checked: bool,
    pub consistent: bool,
    pub publication_permitted: bool,
    pub inconsistent_ontology_handling: String,
}

/// Machine-verifiable response written by the reasoner adapter.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ReasonerReport {
    pub format_version: u32,
    pub dataset_id: Uuid,
    pub snapshot_id: Uuid,
    pub reasoner_name: String,
    pub reasoner_version: String,
    pub aggregate_input_sha256: String,
    /// SHA-256 binding the report to the deterministic Phase 40.1 OWL signature.
    pub owl_signature_sha256: String,
    /// SHA-256 binding the report to the trusted Phase 40.2 datatype policy.
    pub datatype_policy_sha256: String,
    /// SHA-256 binding the report to the Phase 40.5 import/profile qualification evidence.
    pub owl_profile_qualification_sha256: String,
    /// SHA-256 binding the report to the Phase 40.6 global consistency qualification evidence.
    pub owl_consistency_qualification_sha256: String,
    pub owl_profile: String,
    pub direct_semantics: bool,
    pub profile_valid: bool,
    pub profile_violation_count: u64,
    pub profile_violation_samples: Vec<String>,
    pub consistency_checked: bool,
    pub consistent: bool,
    pub named_individual_count: u64,
    pub emitted_axiom_count: u64,
    pub proof_dag_available: bool,
    pub materialization_scope: String,
}

/// Direct-hydration row returned without a broad storage scan.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HydratedPayload {
    pub subject_term: String,
    pub subject_resource_kind: crate::rdf::ResourceTermKind,
    pub predicate_iri: String,
    pub lexical_value: String,
    pub datatype_iri: Option<String>,
    pub language: Option<String>,
    pub graph_scope: crate::rdf::GraphScope,
    pub graph_iri: Option<String>,
}

/// Query output contains exact SPARQL bindings and optional payload hydration.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReferenceQueryResult {
    pub dataset_id: Uuid,
    pub snapshot_id: Uuid,
    pub query_sha256: String,
    pub query_form: QueryForm,
    pub head: Vec<String>,
    pub bindings: Vec<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub boolean_result: Option<bool>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub graph_ntriples: Vec<String>,
    /// Every named entity bound by the certified query, including entities with no payload rows.
    pub qualified_entity_iris: Vec<String>,
    pub hydrated_payload: Vec<HydratedPayload>,
    pub coverage_scope: String,
}

/// Semantically qualified online result before any physical payload hydration.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CertifiedSemanticResult {
    /// Dataset that owns the certified query.
    pub dataset_id: Uuid,
    /// Exact immutable snapshot evaluated by the query.
    pub snapshot_id: Uuid,
    /// SHA-256 of the exact query bytes.
    pub query_sha256: String,
    /// Standards query form.
    pub query_form: QueryForm,
    /// Ordered SPARQL result variables for SELECT; empty otherwise.
    pub head: Vec<String>,
    /// SPARQL solution bindings with bag multiplicity preserved for SELECT.
    pub bindings: Vec<serde_json::Value>,
    /// Boolean result for ASK.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub boolean_result: Option<bool>,
    /// RDFC-1.0 canonical N-Triples for CONSTRUCT/DESCRIBE.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub graph_ntriples: Vec<String>,
    /// Distinct named entities qualified before physical hydration.
    pub qualified_entity_iris: Vec<String>,
    /// Offline reasoner/query coverage boundary.
    pub coverage_scope: String,
}

/// Exact fragment result returned only after snapshot-certificate verification.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CertifiedFragmentResult {
    /// Dataset that owns the fragment plan.
    pub dataset_id: Uuid,
    /// Exact immutable snapshot evaluated by the fragment.
    pub snapshot_id: Uuid,
    /// Exact original query hash that owns this fragment.
    pub query_sha256: String,
    /// Stable fragment identifier.
    pub fragment_id: String,
    /// Ordered fragment result variables.
    pub head: Vec<String>,
    /// SPARQL solution rows with bag multiplicity preserved.
    pub bindings: Vec<serde_json::Value>,
    /// Canonical fragment multiset checksum.
    pub multiset_sha256: String,
}
