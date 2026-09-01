//! Lossless RDF dataset catalog and fail-closed active-dataset resolution.
//!
//! This crate owns graph identity, visibility, authorization, and the precedence
//! between the service dataset, query `FROM` clauses, and SPARQL Protocol dataset
//! parameters. Storage and execution layers consume the resolved dense graph IDs;
//! they do not infer dataset meaning from file names, graph IRI conventions, or
//! query text.

use std::collections::{BTreeMap, BTreeSet};

use oxigraph::model::NamedNode;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use uuid::Uuid;

/// Current graph-catalog contract version.
pub const GRAPH_CATALOG_FORMAT_VERSION: u32 = 1;

/// Reserved role assigned to the physical source default graph.
pub const SOURCE_DEFAULT_GRAPH_ROLE: &str = "source_default";

/// Logical RDF graph identity, independent of a physical dictionary key.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum LogicalGraphName {
    /// The single unlabeled graph in an RDF dataset.
    Default,
    /// A named graph addressed by an absolute IRI.
    Named {
        /// Absolute RDF graph IRI.
        iri: String,
    },
}

/// Operator-authored policy for one named graph.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GraphDeclaration {
    /// Absolute named-graph IRI. No naming convention is inferred.
    pub graph_iri: String,
    /// Stable, deployment-defined role token such as `semkg` or `alignment`.
    pub role: String,
    /// Principal labels that grant access to this graph.
    pub authorization_labels: BTreeSet<String>,
    /// Whether SPARQL may address asserted facts in this graph.
    pub query_visible: bool,
    /// Whether offline semantic reasoning may consume this graph.
    pub reasoning_visible: bool,
}

/// One deterministic graph record in the immutable RDF dataset catalog.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GraphRecord {
    /// Dense graph ID. The physical source default graph is always zero.
    pub graph_id: u32,
    /// Logical RDF graph identity.
    pub name: LogicalGraphName,
    /// Stable graph role. The source default graph uses `source_default`.
    pub role: String,
    /// Authorization labels. The internal source default graph has none because it
    /// is not part of the NGKG service dataset.
    pub authorization_labels: BTreeSet<String>,
    /// Whether the graph is addressable by the SPARQL service.
    pub query_visible: bool,
    /// Whether the graph participates in offline reasoning.
    pub reasoning_visible: bool,
    /// Exact asserted quad count. Zero retains an explicitly declared empty graph.
    pub asserted_quad_count: u64,
}

/// Immutable, checksum-bound catalog for one RDF dataset snapshot.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GraphCatalog {
    /// Contract version.
    pub format_version: u32,
    /// Dataset identity.
    pub dataset_id: Uuid,
    /// Snapshot identity.
    pub snapshot_id: Uuid,
    /// Default graph followed by named graphs in lexical IRI order.
    pub graphs: Vec<GraphRecord>,
}

/// Query-level dataset clauses parsed from typed SPARQL syntax.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct QueryDatasetSpecification {
    /// True only when the query contains at least one `FROM` or `FROM NAMED` clause.
    pub specified: bool,
    /// Graph IRIs merged into the active default graph.
    pub default_graph_iris: Vec<String>,
    /// Graph IRIs available through `GRAPH`.
    pub named_graph_iris: Vec<String>,
}

/// Dataset parameters supplied through the SPARQL Protocol.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ProtocolDatasetSpecification {
    /// Repeated `default-graph-uri` values.
    pub default_graph_uris: Vec<String>,
    /// Repeated `named-graph-uri` values.
    pub named_graph_uris: Vec<String>,
}

impl ProtocolDatasetSpecification {
    /// Whether protocol parameters replace any query dataset clauses.
    #[must_use]
    pub fn is_specified(&self) -> bool {
        !self.default_graph_uris.is_empty() || !self.named_graph_uris.is_empty()
    }
}

/// Source of the resolved active dataset.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DatasetSelectionSource {
    /// No explicit dataset: authorized named graphs form the union-default service dataset.
    ServiceDefault,
    /// Typed query `FROM` and `FROM NAMED` clauses.
    QueryDataset,
    /// SPARQL Protocol parameters, which replace query clauses.
    ProtocolDataset,
}

impl DatasetSelectionSource {
    /// Stable cache/wire code for the precedence branch.
    #[must_use]
    pub const fn code(self) -> u8 {
        match self {
            Self::ServiceDefault => 0,
            Self::QueryDataset => 1,
            Self::ProtocolDataset => 2,
        }
    }
}

/// Dense, authorization-qualified active dataset consumed by execution.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ResolvedDataset {
    /// Dataset-precedence branch used for this request.
    pub selection_source: DatasetSelectionSource,
    /// Named source graphs merged into the active default graph.
    pub default_graph_ids: Vec<u32>,
    /// Named source graphs addressable through `GRAPH`.
    pub named_graph_ids: Vec<u32>,
    /// Every query-visible named graph authorized for the principal.
    pub authorized_graph_ids: Vec<u32>,
    /// Hash of the full authorized graph set, used in caches and worker envelopes.
    pub authorized_graph_set_sha256: String,
    /// Hash of the active default and named graph IDs. Selection source is bound separately.
    pub active_dataset_sha256: String,
}

/// Graph catalog or active-dataset contract failure.
#[derive(Debug, Error, Eq, PartialEq)]
pub enum DatasetError {
    /// Dataset and snapshot identifiers must be non-nil.
    #[error("datasetId and snapshotId must be non-nil UUIDs")]
    NilIdentity,
    /// A graph IRI is not an absolute IRI.
    #[error("graph IRI is invalid: {0}")]
    InvalidGraphIri(String),
    /// The graph role is not a stable lower-snake token.
    #[error("graph role is invalid: {0}")]
    InvalidRole(String),
    /// An authorization label is empty, oversized, or contains unsafe characters.
    #[error("graph authorization label is invalid: {0}")]
    InvalidAuthorizationLabel(String),
    /// A graph declaration occurs more than once.
    #[error("graph declaration is duplicated: {0}")]
    DuplicateGraph(String),
    /// Input data contains an undeclared named graph.
    #[error("source contains an undeclared named graph: {0}")]
    UndeclaredGraph(String),
    /// Graph IDs or ordering are not canonical.
    #[error("graph catalog is not canonical")]
    NonCanonicalCatalog,
    /// Query-visible or reasoning-visible graphs require at least one authorization label.
    #[error("visible graph has no authorization labels: {0}")]
    UnlabeledVisibleGraph(String),
    /// Requested graph is not in this snapshot.
    #[error("requested graph is absent from the active snapshot: {0}")]
    UnknownRequestedGraph(String),
    /// Requested graph is not query-visible.
    #[error("requested graph is not query-visible: {0}")]
    HiddenRequestedGraph(String),
    /// Principal does not have access to a requested graph.
    #[error("requested graph is forbidden: {0}")]
    ForbiddenRequestedGraph(String),
    /// No named graph is available to the service for this principal.
    #[error("principal has no query-visible named graph in the active snapshot")]
    EmptyAuthorizedDataset,
    /// Query dataset declaration is internally inconsistent.
    #[error("query dataset specification is inconsistent")]
    InvalidQueryDatasetSpecification,
    /// Dataset hash serialization failed.
    #[error("dataset hash serialization failed: {0}")]
    HashSerialization(String),
    /// Dense graph ID space exceeds the contract.
    #[error("graph count exceeds the u32 graph ID space")]
    GraphIdOverflow,
    /// A serialized active-dataset envelope does not match the graph catalog or its hashes.
    #[error("resolved active dataset integrity check failed")]
    ResolvedDatasetIntegrity,
}

/// Build a deterministic graph catalog from observed counts and declarations.
///
/// The physical source default graph is retained as graph ID zero but is never
/// query- or reasoning-visible through the NGKG union-default service. Every named
/// source graph must be declared, while declarations with zero observed quads are
/// retained to preserve empty named graphs.
pub fn compile_catalog(
    dataset_id: Uuid,
    snapshot_id: Uuid,
    default_quad_count: u64,
    named_quad_counts: &BTreeMap<String, u64>,
    declarations: &[GraphDeclaration],
) -> Result<GraphCatalog, DatasetError> {
    if dataset_id.is_nil() || snapshot_id.is_nil() {
        return Err(DatasetError::NilIdentity);
    }
    let mut declared = BTreeMap::new();
    for declaration in declarations {
        validate_declaration(declaration)?;
        if declared
            .insert(declaration.graph_iri.clone(), declaration.clone())
            .is_some()
        {
            return Err(DatasetError::DuplicateGraph(declaration.graph_iri.clone()));
        }
    }
    if let Some(iri) = named_quad_counts
        .keys()
        .find(|iri| !declared.contains_key(*iri))
    {
        return Err(DatasetError::UndeclaredGraph(iri.clone()));
    }

    let mut graphs = Vec::with_capacity(declared.len().saturating_add(1));
    graphs.push(GraphRecord {
        graph_id: 0,
        name: LogicalGraphName::Default,
        role: SOURCE_DEFAULT_GRAPH_ROLE.to_owned(),
        authorization_labels: BTreeSet::new(),
        query_visible: false,
        reasoning_visible: false,
        asserted_quad_count: default_quad_count,
    });
    for (ordinal, (iri, declaration)) in declared.into_iter().enumerate() {
        let graph_id =
            u32::try_from(ordinal.saturating_add(1)).map_err(|_| DatasetError::GraphIdOverflow)?;
        graphs.push(GraphRecord {
            graph_id,
            name: LogicalGraphName::Named { iri: iri.clone() },
            role: declaration.role,
            authorization_labels: declaration.authorization_labels,
            query_visible: declaration.query_visible,
            reasoning_visible: declaration.reasoning_visible,
            asserted_quad_count: named_quad_counts.get(&iri).copied().unwrap_or(0),
        });
    }
    let catalog = GraphCatalog {
        format_version: GRAPH_CATALOG_FORMAT_VERSION,
        dataset_id,
        snapshot_id,
        graphs,
    };
    catalog.validate()?;
    Ok(catalog)
}

impl GraphCatalog {
    /// Validate identity, dense IDs, ordering, IRIs, roles, and authorization policy.
    pub fn validate(&self) -> Result<(), DatasetError> {
        if self.format_version != GRAPH_CATALOG_FORMAT_VERSION
            || self.dataset_id.is_nil()
            || self.snapshot_id.is_nil()
            || self.graphs.is_empty()
        {
            return Err(DatasetError::NonCanonicalCatalog);
        }
        let mut previous_iri: Option<&str> = None;
        let mut names = BTreeSet::new();
        for (ordinal, graph) in self.graphs.iter().enumerate() {
            if usize::try_from(graph.graph_id).ok() != Some(ordinal) || !valid_role(&graph.role) {
                return Err(DatasetError::NonCanonicalCatalog);
            }
            for label in &graph.authorization_labels {
                if !valid_authorization_label(label) {
                    return Err(DatasetError::InvalidAuthorizationLabel(label.clone()));
                }
            }
            match (&graph.name, ordinal) {
                (LogicalGraphName::Default, 0)
                    if graph.role == SOURCE_DEFAULT_GRAPH_ROLE
                        && graph.authorization_labels.is_empty()
                        && !graph.query_visible
                        && !graph.reasoning_visible => {}
                (LogicalGraphName::Named { iri }, index) if index > 0 => {
                    validate_graph_iri(iri)?;
                    if previous_iri.is_some_and(|previous| previous >= iri.as_str())
                        || !names.insert(iri.clone())
                    {
                        return Err(DatasetError::NonCanonicalCatalog);
                    }
                    if (graph.query_visible || graph.reasoning_visible)
                        && graph.authorization_labels.is_empty()
                    {
                        return Err(DatasetError::UnlabeledVisibleGraph(iri.clone()));
                    }
                    previous_iri = Some(iri);
                }
                _ => return Err(DatasetError::NonCanonicalCatalog),
            }
        }
        Ok(())
    }

    /// Lookup a graph record by dense ID.
    #[must_use]
    pub fn by_id(&self, graph_id: u32) -> Option<&GraphRecord> {
        self.graphs
            .get(usize::try_from(graph_id).ok()?)
            .filter(|record| record.graph_id == graph_id)
    }

    /// Lookup a named graph by exact IRI.
    #[must_use]
    pub fn named(&self, iri: &str) -> Option<&GraphRecord> {
        self.graphs
            .binary_search_by(|record| match &record.name {
                LogicalGraphName::Default => std::cmp::Ordering::Less,
                LogicalGraphName::Named { iri: candidate } => candidate.as_str().cmp(iri),
            })
            .ok()
            .and_then(|index| self.graphs.get(index))
    }
}

/// Resolve the active dataset with SPARQL Protocol precedence and graph authorization.
pub fn resolve_dataset(
    catalog: &GraphCatalog,
    principal_labels: &BTreeSet<String>,
    query: &QueryDatasetSpecification,
    protocol: &ProtocolDatasetSpecification,
) -> Result<ResolvedDataset, DatasetError> {
    catalog.validate()?;
    if query.specified
        != (!query.default_graph_iris.is_empty() || !query.named_graph_iris.is_empty())
    {
        return Err(DatasetError::InvalidQueryDatasetSpecification);
    }
    let authorized = catalog
        .graphs
        .iter()
        .filter(|graph| graph.query_visible)
        .filter(|graph| !graph.authorization_labels.is_disjoint(principal_labels))
        .map(|graph| graph.graph_id)
        .collect::<Vec<_>>();
    if authorized.is_empty() {
        return Err(DatasetError::EmptyAuthorizedDataset);
    }
    let authorized_set = authorized.iter().copied().collect::<BTreeSet<_>>();
    let authorized_graph_set_sha256 = hash_graph_set(catalog, &authorized)?;

    let (selection_source, default_iris, named_iris) = if protocol.is_specified() {
        (
            DatasetSelectionSource::ProtocolDataset,
            protocol.default_graph_uris.as_slice(),
            protocol.named_graph_uris.as_slice(),
        )
    } else if query.specified {
        (
            DatasetSelectionSource::QueryDataset,
            query.default_graph_iris.as_slice(),
            query.named_graph_iris.as_slice(),
        )
    } else {
        let active_dataset_sha256 = hash_active_dataset(
            DatasetSelectionSource::ServiceDefault,
            &authorized,
            &authorized,
        )?;
        return Ok(ResolvedDataset {
            selection_source: DatasetSelectionSource::ServiceDefault,
            default_graph_ids: authorized.clone(),
            named_graph_ids: authorized.clone(),
            authorized_graph_ids: authorized,
            authorized_graph_set_sha256,
            active_dataset_sha256,
        });
    };

    let default_graph_ids = resolve_requested_graphs(catalog, &authorized_set, default_iris)?;
    let named_graph_ids = resolve_requested_graphs(catalog, &authorized_set, named_iris)?;
    let active_dataset_sha256 =
        hash_active_dataset(selection_source, &default_graph_ids, &named_graph_ids)?;
    Ok(ResolvedDataset {
        selection_source,
        default_graph_ids,
        named_graph_ids,
        authorized_graph_ids: authorized,
        authorized_graph_set_sha256,
        active_dataset_sha256,
    })
}

/// Validate a serialized resolved dataset without relying on the original principal labels.
///
/// This is used at the Phase 40.8 exact-reasoner boundary. The authorization graph set is
/// checksum-bound by the Phase 40.7 report, while this function proves that the graph IDs are
/// canonical query-visible named graphs, the active default/named sets are authorized subsets,
/// and both semantic hashes recompute exactly from the immutable graph catalog.
pub fn validate_resolved_dataset(
    catalog: &GraphCatalog,
    resolved: &ResolvedDataset,
) -> Result<(), DatasetError> {
    fn canonical(ids: &[u32]) -> bool {
        ids.windows(2).all(|pair| pair[0] < pair[1])
    }
    catalog.validate()?;
    if !canonical(&resolved.default_graph_ids)
        || !canonical(&resolved.named_graph_ids)
        || !canonical(&resolved.authorized_graph_ids)
        || resolved.authorized_graph_ids.is_empty()
    {
        return Err(DatasetError::ResolvedDatasetIntegrity);
    }
    let authorized = resolved
        .authorized_graph_ids
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    for graph_id in &resolved.authorized_graph_ids {
        let graph = catalog
            .by_id(*graph_id)
            .ok_or(DatasetError::ResolvedDatasetIntegrity)?;
        if !graph.query_visible || !matches!(graph.name, LogicalGraphName::Named { .. }) {
            return Err(DatasetError::ResolvedDatasetIntegrity);
        }
    }
    if resolved
        .default_graph_ids
        .iter()
        .chain(&resolved.named_graph_ids)
        .any(|id| !authorized.contains(id))
    {
        return Err(DatasetError::ResolvedDatasetIntegrity);
    }
    if resolved.selection_source == DatasetSelectionSource::ServiceDefault
        && (resolved.default_graph_ids != resolved.authorized_graph_ids
            || resolved.named_graph_ids != resolved.authorized_graph_ids)
    {
        return Err(DatasetError::ResolvedDatasetIntegrity);
    }
    if hash_graph_set(catalog, &resolved.authorized_graph_ids)?
        != resolved.authorized_graph_set_sha256
        || hash_active_dataset(
            resolved.selection_source,
            &resolved.default_graph_ids,
            &resolved.named_graph_ids,
        )? != resolved.active_dataset_sha256
    {
        return Err(DatasetError::ResolvedDatasetIntegrity);
    }
    Ok(())
}

/// Restrict an already authorization-qualified dataset to explicitly allowed graph roles.
///
/// This is used by entailment execution to remove closure, provenance and any mapping/alignment
/// graph before ontology assembly. Authorization is never broadened; the returned graph-set and
/// active-dataset hashes are recomputed from the reduced immutable graph IDs.
pub fn restrict_resolved_dataset_to_roles(
    catalog: &GraphCatalog,
    resolved: &ResolvedDataset,
    allowed_roles: &BTreeSet<String>,
) -> Result<ResolvedDataset, DatasetError> {
    validate_resolved_dataset(catalog, resolved)?;
    if allowed_roles.is_empty() {
        return Err(DatasetError::ResolvedDatasetIntegrity);
    }
    let allowed = |graph_id: &u32| {
        catalog
            .by_id(*graph_id)
            .is_some_and(|graph| allowed_roles.contains(&graph.role))
    };
    let default_graph_ids = resolved
        .default_graph_ids
        .iter()
        .copied()
        .filter(allowed)
        .collect::<Vec<_>>();
    let named_graph_ids = resolved
        .named_graph_ids
        .iter()
        .copied()
        .filter(allowed)
        .collect::<Vec<_>>();
    let authorized_graph_ids = resolved
        .authorized_graph_ids
        .iter()
        .copied()
        .filter(allowed)
        .collect::<Vec<_>>();
    if authorized_graph_ids.is_empty()
        || (default_graph_ids.is_empty() && named_graph_ids.is_empty())
    {
        return Err(DatasetError::EmptyAuthorizedDataset);
    }
    let restricted = ResolvedDataset {
        selection_source: resolved.selection_source,
        authorized_graph_set_sha256: hash_graph_set(catalog, &authorized_graph_ids)?,
        active_dataset_sha256: hash_active_dataset(
            resolved.selection_source,
            &default_graph_ids,
            &named_graph_ids,
        )?,
        default_graph_ids,
        named_graph_ids,
        authorized_graph_ids,
    };
    validate_resolved_dataset(catalog, &restricted)?;
    Ok(restricted)
}

fn validate_declaration(declaration: &GraphDeclaration) -> Result<(), DatasetError> {
    validate_graph_iri(&declaration.graph_iri)?;
    if !valid_role(&declaration.role) {
        return Err(DatasetError::InvalidRole(declaration.role.clone()));
    }
    for label in &declaration.authorization_labels {
        if !valid_authorization_label(label) {
            return Err(DatasetError::InvalidAuthorizationLabel(label.clone()));
        }
    }
    if (declaration.query_visible || declaration.reasoning_visible)
        && declaration.authorization_labels.is_empty()
    {
        return Err(DatasetError::UnlabeledVisibleGraph(
            declaration.graph_iri.clone(),
        ));
    }
    Ok(())
}

fn validate_graph_iri(iri: &str) -> Result<(), DatasetError> {
    NamedNode::new(iri.to_owned())
        .map(|_| ())
        .map_err(|_| DatasetError::InvalidGraphIri(iri.to_owned()))
}

fn valid_role(value: &str) -> bool {
    (1..=64).contains(&value.len())
        && value.bytes().enumerate().all(|(index, byte)| {
            if index == 0 {
                byte.is_ascii_lowercase()
            } else {
                byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'_' | b'-')
            }
        })
}

/// Validate one authorization label used by graph catalogs and bearer identities.
#[must_use]
pub fn valid_authorization_label(value: &str) -> bool {
    (1..=128).contains(&value.len())
        && value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.' | b':' | b'/')
        })
}

fn resolve_requested_graphs(
    catalog: &GraphCatalog,
    authorized: &BTreeSet<u32>,
    iris: &[String],
) -> Result<Vec<u32>, DatasetError> {
    let mut ids = BTreeSet::new();
    for iri in iris {
        validate_graph_iri(iri)?;
        let graph = catalog
            .named(iri)
            .ok_or_else(|| DatasetError::UnknownRequestedGraph(iri.clone()))?;
        if !graph.query_visible {
            return Err(DatasetError::HiddenRequestedGraph(iri.clone()));
        }
        if !authorized.contains(&graph.graph_id) {
            return Err(DatasetError::ForbiddenRequestedGraph(iri.clone()));
        }
        ids.insert(graph.graph_id);
    }
    Ok(ids.into_iter().collect())
}

fn hash_graph_set(catalog: &GraphCatalog, graph_ids: &[u32]) -> Result<String, DatasetError> {
    let rows = graph_ids
        .iter()
        .map(|graph_id| {
            let graph = catalog
                .by_id(*graph_id)
                .ok_or(DatasetError::NonCanonicalCatalog)?;
            let LogicalGraphName::Named { iri } = &graph.name else {
                return Err(DatasetError::NonCanonicalCatalog);
            };
            Ok((
                graph.graph_id,
                iri,
                &graph.role,
                &graph.authorization_labels,
                graph.query_visible,
                graph.reasoning_visible,
            ))
        })
        .collect::<Result<Vec<_>, DatasetError>>()?;
    let bytes = serde_json::to_vec(&("ngkg-authorized-graph-set-v1", rows))
        .map_err(|error| DatasetError::HashSerialization(error.to_string()))?;
    Ok(hex_sha256(&bytes))
}

fn hash_active_dataset(
    _source: DatasetSelectionSource,
    default_graph_ids: &[u32],
    named_graph_ids: &[u32],
) -> Result<String, DatasetError> {
    // Selection precedence is retained in `selection_source`; the semantic hash
    // intentionally binds only the resulting active RDF dataset so equivalent
    // query and protocol specifications may share one exact certificate.
    let bytes = serde_json::to_vec(&("ngkg-active-dataset-v1", default_graph_ids, named_graph_ids))
        .map_err(|error| DatasetError::HashSerialization(error.to_string()))?;
    Ok(hex_sha256(&bytes))
}

fn hex_sha256(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

#[cfg(test)]
mod tests {
    use super::{
        DatasetError, DatasetSelectionSource, GraphCatalog, GraphDeclaration, LogicalGraphName,
        ProtocolDatasetSpecification, QueryDatasetSpecification, compile_catalog, resolve_dataset,
        validate_resolved_dataset,
    };
    use std::collections::{BTreeMap, BTreeSet};
    use uuid::Uuid;

    fn declaration(iri: &str, label: &str) -> GraphDeclaration {
        GraphDeclaration {
            graph_iri: iri.to_owned(),
            role: "semkg".to_owned(),
            authorization_labels: BTreeSet::from([label.to_owned()]),
            query_visible: true,
            reasoning_visible: true,
        }
    }

    fn catalog() -> Result<GraphCatalog, DatasetError> {
        compile_catalog(
            Uuid::from_u128(1),
            Uuid::from_u128(2),
            3,
            &BTreeMap::from([
                ("https://example.test/g1".to_owned(), 7),
                ("https://example.test/g2".to_owned(), 11),
            ]),
            &[
                declaration("https://example.test/g2", "team-b"),
                declaration("https://example.test/g1", "team-a"),
                declaration("https://example.test/empty", "team-a"),
            ],
        )
    }

    #[test]
    fn catalog_retains_default_and_empty_graphs_in_canonical_order() -> Result<(), DatasetError> {
        let catalog = catalog()?;
        assert_eq!(catalog.graphs.len(), 4);
        assert!(matches!(catalog.graphs[0].name, LogicalGraphName::Default));
        let names = catalog
            .graphs
            .iter()
            .skip(1)
            .filter_map(|graph| match &graph.name {
                LogicalGraphName::Named { iri } => Some(iri.as_str()),
                LogicalGraphName::Default => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            names,
            vec![
                "https://example.test/empty",
                "https://example.test/g1",
                "https://example.test/g2",
            ]
        );
        assert_eq!(catalog.graphs[1].asserted_quad_count, 0);
        assert_eq!(catalog.graphs[0].asserted_quad_count, 3);
        catalog.validate()
    }

    #[test]
    fn undeclared_source_graph_fails_closed() {
        let result = compile_catalog(
            Uuid::from_u128(1),
            Uuid::from_u128(2),
            0,
            &BTreeMap::from([("https://example.test/unknown".to_owned(), 1)]),
            &[],
        );
        assert!(matches!(result, Err(DatasetError::UndeclaredGraph(_))));
    }

    #[test]
    fn service_default_is_the_authorized_named_graph_union() -> Result<(), DatasetError> {
        let resolved = resolve_dataset(
            &catalog()?,
            &BTreeSet::from(["team-a".to_owned()]),
            &QueryDatasetSpecification::default(),
            &ProtocolDatasetSpecification::default(),
        )?;
        assert_eq!(
            resolved.selection_source,
            DatasetSelectionSource::ServiceDefault
        );
        assert_eq!(resolved.default_graph_ids, vec![1, 2]);
        assert_eq!(resolved.named_graph_ids, vec![1, 2]);
        assert_eq!(resolved.authorized_graph_ids, vec![1, 2]);
        assert_eq!(resolved.authorized_graph_set_sha256.len(), 64);
        assert_eq!(resolved.active_dataset_sha256.len(), 64);
        Ok(())
    }

    #[test]
    fn protocol_dataset_replaces_query_dataset() -> Result<(), DatasetError> {
        let resolved = resolve_dataset(
            &catalog()?,
            &BTreeSet::from(["team-a".to_owned(), "team-b".to_owned()]),
            &QueryDatasetSpecification {
                specified: true,
                default_graph_iris: vec!["https://example.test/g1".to_owned()],
                named_graph_iris: Vec::new(),
            },
            &ProtocolDatasetSpecification {
                default_graph_uris: vec!["https://example.test/g2".to_owned()],
                named_graph_uris: vec!["https://example.test/g1".to_owned()],
            },
        )?;
        assert_eq!(
            resolved.selection_source,
            DatasetSelectionSource::ProtocolDataset
        );
        assert_eq!(resolved.default_graph_ids, vec![3]);
        assert_eq!(resolved.named_graph_ids, vec![2]);
        Ok(())
    }

    #[test]
    fn unauthorized_explicit_graph_is_rejected_not_removed() -> Result<(), DatasetError> {
        let result = resolve_dataset(
            &catalog()?,
            &BTreeSet::from(["team-a".to_owned()]),
            &QueryDatasetSpecification {
                specified: true,
                default_graph_iris: vec!["https://example.test/g2".to_owned()],
                named_graph_iris: Vec::new(),
            },
            &ProtocolDatasetSpecification::default(),
        );
        assert!(matches!(
            result,
            Err(DatasetError::ForbiddenRequestedGraph(_))
        ));
        Ok(())
    }

    #[test]
    fn query_specification_flag_cannot_disagree_with_its_graph_lists() -> Result<(), DatasetError> {
        let result = resolve_dataset(
            &catalog()?,
            &BTreeSet::from(["team-a".to_owned()]),
            &QueryDatasetSpecification {
                specified: false,
                default_graph_iris: vec!["https://example.test/g1".to_owned()],
                named_graph_iris: Vec::new(),
            },
            &ProtocolDatasetSpecification::default(),
        );
        assert_eq!(result, Err(DatasetError::InvalidQueryDatasetSpecification));
        Ok(())
    }

    #[test]
    fn active_dataset_hash_is_semantic_while_selection_source_is_separate()
    -> Result<(), DatasetError> {
        let catalog = catalog()?;
        let labels = BTreeSet::from(["team-a".to_owned(), "team-b".to_owned()]);
        let query = QueryDatasetSpecification {
            specified: true,
            default_graph_iris: vec!["https://example.test/g1".to_owned()],
            named_graph_iris: vec!["https://example.test/g2".to_owned()],
        };
        let query_result = resolve_dataset(
            &catalog,
            &labels,
            &query,
            &ProtocolDatasetSpecification::default(),
        )?;
        let protocol_result = resolve_dataset(
            &catalog,
            &labels,
            &QueryDatasetSpecification::default(),
            &ProtocolDatasetSpecification {
                default_graph_uris: query.default_graph_iris,
                named_graph_uris: query.named_graph_iris,
            },
        )?;
        assert_eq!(
            query_result.active_dataset_sha256,
            protocol_result.active_dataset_sha256
        );
        assert_ne!(
            query_result.selection_source,
            protocol_result.selection_source
        );
        assert_eq!(
            query_result.authorized_graph_set_sha256,
            protocol_result.authorized_graph_set_sha256
        );
        Ok(())
    }
    #[test]
    fn resolved_dataset_round_trips_integrity_hashes() -> Result<(), DatasetError> {
        let dataset_id = Uuid::new_v4();
        let snapshot_id = Uuid::new_v4();
        let mut named = BTreeMap::new();
        named.insert("https://example.test/g1".to_owned(), 1);
        named.insert("https://example.test/g2".to_owned(), 1);
        let catalog = compile_catalog(
            dataset_id,
            snapshot_id,
            0,
            &named,
            &[
                declaration("https://example.test/g1", "team"),
                declaration("https://example.test/g2", "team"),
            ],
        )?;
        let resolved = resolve_dataset(
            &catalog,
            &BTreeSet::from(["team".to_owned()]),
            &QueryDatasetSpecification::default(),
            &ProtocolDatasetSpecification::default(),
        )?;
        validate_resolved_dataset(&catalog, &resolved)?;
        let mut tampered = resolved.clone();
        tampered.active_dataset_sha256 = "0".repeat(64);
        assert_eq!(
            validate_resolved_dataset(&catalog, &tampered),
            Err(DatasetError::ResolvedDatasetIntegrity)
        );
        Ok(())
    }
}
