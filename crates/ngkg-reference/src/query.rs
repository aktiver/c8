//! Reference SPARQL 1.1 execution and form-specific exact result certification.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File},
    io::{BufReader, Cursor},
    path::Path,
};

use ngkg_dataset::{GraphCatalog, LogicalGraphName, QueryDatasetSpecification, ResolvedDataset};
use ngkg_sparql_compiler::{
    CompiledSparqlQuery, QueryForm, SparqlCertificationError, SparqlCompileError,
};
use oxigraph::{
    io::{RdfFormat, RdfParser},
    model::{
        BlankNode, Dataset, GraphName, Literal, NamedNode, NamedOrBlankNode, Quad, Term, Triple,
        dataset::{CanonicalizationAlgorithm, CanonicalizationHashAlgorithm},
    },
    sparql::{CancellationToken, QueryEvaluationError, QueryResults, SparqlEvaluator},
    store::Store,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use spargebra::Query;
use thiserror::Error;

/// Version of NGKG's form-aware exact SPARQL result certificate.
pub const QUERY_RESULT_HASH_VERSION: u32 = 2;

#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum CanonicalTerm {
    NamedNode {
        value: String,
    },
    BlankNode {
        value: String,
    },
    Literal {
        value: String,
        datatype: String,
        language: Option<String>,
    },
}

/// Operator-controlled bounds for one exact scalar SPARQL evaluation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QueryExecutionLimits {
    /// Maximum SELECT solution rows materialized into a certified response.
    pub max_solution_rows: usize,
    /// Maximum CONSTRUCT/DESCRIBE graph triples admitted before canonicalization.
    pub max_graph_triples: usize,
    /// Maximum distinct result blank nodes admitted to RDF canonicalization.
    pub max_graph_blank_nodes: usize,
}

impl QueryExecutionLimits {
    /// Fail closed when a configured execution budget is unusable.
    pub fn validate(self) -> Result<Self, ReferenceQueryError> {
        if self.max_solution_rows == 0
            || self.max_graph_triples == 0
            || self.max_graph_blank_nodes == 0
        {
            return Err(ReferenceQueryError::ResultLimit(
                "query execution limits must all be positive".to_owned(),
            ));
        }
        Ok(self)
    }
}

#[derive(Clone, Debug)]
pub struct ExecutedSolutions {
    pub head: Vec<String>,
    pub bindings: Vec<Value>,
    pub entity_iris: BTreeSet<String>,
    canonical_rows: Vec<BTreeMap<String, CanonicalTerm>>,
}

/// Canonical RDF graph result. `ntriples` is RDFC-1.0 canonicalized and sorted.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutedGraph {
    pub ntriples: Vec<String>,
    pub entity_iris: BTreeSet<String>,
}

/// Exact scalar result for any SPARQL 1.1 query form.
#[derive(Clone, Debug)]
pub enum ExecutedQueryResult {
    /// SELECT solution multiset.
    Solutions(ExecutedSolutions),
    /// ASK boolean.
    Boolean(bool),
    /// CONSTRUCT or DESCRIBE RDF graph.
    Graph {
        /// Original graph-producing query form.
        form: QueryForm,
        /// Canonical graph payload.
        graph: ExecutedGraph,
    },
}

impl ExecutedQueryResult {
    /// Query-result form implied by the evaluator output.
    #[must_use]
    pub const fn form(&self) -> QueryForm {
        match self {
            Self::Solutions(_) => QueryForm::Select,
            Self::Boolean(_) => QueryForm::Ask,
            Self::Graph { form, .. } => *form,
        }
    }

    /// Named IRIs semantically qualified by this result before hydration.
    #[must_use]
    pub fn entity_iris(&self) -> &BTreeSet<String> {
        match self {
            Self::Solutions(value) => &value.entity_iris,
            Self::Boolean(_) => empty_entity_set(),
            Self::Graph { graph, .. } => &graph.entity_iris,
        }
    }
}

fn empty_entity_set() -> &'static BTreeSet<String> {
    static EMPTY: std::sync::OnceLock<BTreeSet<String>> = std::sync::OnceLock::new();
    EMPTY.get_or_init(BTreeSet::new)
}

#[derive(Clone, Debug)]
pub struct ExpectedSolutions {
    pub head: Vec<String>,
    canonical_rows: Vec<BTreeMap<String, CanonicalTerm>>,
}

/// Independently authored expected result for a certified query.
#[derive(Clone, Debug)]
pub enum ExpectedQueryResult {
    Solutions(ExpectedSolutions),
    Boolean(bool),
    Graph(Vec<String>),
}

#[derive(Debug, Error)]
pub enum ReferenceQueryError {
    #[error("RDF/query file I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("RDF loading failed: {0}")]
    Rdf(String),
    #[error("SPARQL execution failed: {0}")]
    Sparql(String),
    #[error("SPARQL compilation failed: {0}")]
    Compile(#[from] SparqlCompileError),
    #[error("SPARQL query is not eligible for immutable snapshot certification: {0}")]
    Certification(#[from] SparqlCertificationError),
    #[error("only SELECT solution results are accepted by this compatibility API")]
    SelectRequired,
    #[error("expected query result is invalid: {0}")]
    InvalidExpected(String),
    #[error("observed SPARQL result differs from the independent expected result: {0}")]
    AnswerMismatch(String),
    #[error("expected provenance/source evidence differs from the certification manifest")]
    SourceEvidenceMismatch,
    #[error("required source evidence is not linked from a result entity: {0}")]
    SourceEvidenceMissing(String),
    #[error("blank-node graph names are outside the NGKG named-subdomain profile")]
    BlankGraphName,
    #[error("SPARQL dataset specification is invalid: {0}")]
    Dataset(String),
    #[error("query result exceeded an operator-controlled exact-execution limit: {0}")]
    ResultLimit(String),
    #[error("expected RDF graph format is unsupported: {0}")]
    ExpectedGraphFormat(String),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
/// Default-graph behavior selected before a certified query is evaluated.
pub enum DefaultDatasetPolicy {
    /// Preserve and query the default graph physically present in the RDF dataset.
    StoredDefault,
    /// Expose the set union of every named graph as the active default graph while
    /// retaining all named graphs for `GRAPH`, `FROM`, and `FROM NAMED` evaluation.
    UnionDefault,
}

pub fn build_store(
    query_dataset_path: &Path,
    closure_path: &Path,
    closure_graph_iri: &str,
) -> Result<Store, ReferenceQueryError> {
    build_store_with_dataset_policy(
        query_dataset_path,
        closure_path,
        closure_graph_iri,
        DefaultDatasetPolicy::UnionDefault,
    )
}

/// Build a reference store with an explicit, fail-closed active-dataset policy.
pub fn build_store_with_dataset_policy(
    query_dataset_path: &Path,
    closure_path: &Path,
    closure_graph_iri: &str,
    default_dataset_policy: DefaultDatasetPolicy,
) -> Result<Store, ReferenceQueryError> {
    let store = Store::new().map_err(|error| ReferenceQueryError::Rdf(error.to_string()))?;
    load_dataset(
        &store,
        query_dataset_path,
        RdfFormat::NQuads,
        None,
        default_dataset_policy,
    )?;
    let closure_graph = NamedNode::new(closure_graph_iri.to_owned())
        .map_err(|error| ReferenceQueryError::Rdf(error.to_string()))?;
    load_dataset(
        &store,
        closure_path,
        RdfFormat::NTriples,
        Some(closure_graph),
        default_dataset_policy,
    )?;
    Ok(store)
}

/// Compatibility SELECT API retained for distributed fragment certification.
pub fn execute_select(
    store: &Store,
    query_text: &str,
) -> Result<ExecutedSolutions, ReferenceQueryError> {
    let compiled = CompiledSparqlQuery::parse(query_text)?;
    execute_compiled_select(store, &compiled)
}

/// Compatibility SELECT API retained for the certified distributed fast path.
pub fn execute_compiled_select(
    store: &Store,
    compiled: &CompiledSparqlQuery,
) -> Result<ExecutedSolutions, ReferenceQueryError> {
    let limits = QueryExecutionLimits {
        max_solution_rows: usize::MAX,
        max_graph_triples: 1,
        max_graph_blank_nodes: 1,
    };
    let result = execute_compiled_query(store, compiled, limits)?;
    match result {
        ExecutedQueryResult::Solutions(value) => Ok(value),
        ExecutedQueryResult::Boolean(_) | ExecutedQueryResult::Graph { .. } => {
            Err(ReferenceQueryError::SelectRequired)
        }
    }
}

/// Execute any SPARQL 1.1 query form using the shared parsed algebra.
pub fn execute_compiled_query(
    store: &Store,
    compiled: &CompiledSparqlQuery,
    limits: QueryExecutionLimits,
) -> Result<ExecutedQueryResult, ReferenceQueryError> {
    execute_compiled_query_with_default_policy(
        store,
        compiled,
        limits,
        DefaultDatasetPolicy::UnionDefault,
    )
}

/// Execute a parsed query with an explicit service-default policy.
///
/// NGKG production uses `UnionDefault`; the W3C conformance driver uses
/// `StoredDefault` so official SPARQL fixtures are evaluated with the dataset
/// described by the test manifest rather than NGKG's service extension.
pub fn execute_compiled_query_with_default_policy(
    store: &Store,
    compiled: &CompiledSparqlQuery,
    limits: QueryExecutionLimits,
    default_dataset_policy: DefaultDatasetPolicy,
) -> Result<ExecutedQueryResult, ReferenceQueryError> {
    execute_compiled_query_with_default_policy_cancellable(
        store,
        compiled,
        limits,
        default_dataset_policy,
        None,
    )
}

/// Cancellable form of `execute_compiled_query_with_default_policy`.
pub fn execute_compiled_query_with_default_policy_cancellable(
    store: &Store,
    compiled: &CompiledSparqlQuery,
    limits: QueryExecutionLimits,
    _default_dataset_policy: DefaultDatasetPolicy,
    cancellation_token: Option<CancellationToken>,
) -> Result<ExecutedQueryResult, ReferenceQueryError> {
    let limits = limits.validate()?;
    let evaluator = if let Some(token) = cancellation_token {
        SparqlEvaluator::new().with_cancellation_token(token)
    } else {
        SparqlEvaluator::new()
    };
    let prepared = evaluator.for_query(compiled.query_clone());
    // `UnionDefault` is materialized as an RDF set in the physical default
    // graph at load time. Oxigraph's dynamic union view preserves one match per
    // source graph, which would incorrectly turn duplicate cross-graph triples
    // into duplicate SPARQL solutions.
    execute_prepared(prepared.on_store(store).execute(), compiled, limits)
}

/// Execute with an optional Oxigraph cancellation token for bounded online requests.
pub fn execute_compiled_query_cancellable(
    store: &Store,
    compiled: &CompiledSparqlQuery,
    limits: QueryExecutionLimits,
    cancellation_token: Option<CancellationToken>,
) -> Result<ExecutedQueryResult, ReferenceQueryError> {
    execute_compiled_query_with_default_policy_cancellable(
        store,
        compiled,
        limits,
        DefaultDatasetPolicy::UnionDefault,
        cancellation_token,
    )
}

/// Return the standards-parser-derived query dataset specification.
pub fn query_dataset_specification(
    query_text: &str,
) -> Result<QueryDatasetSpecification, ReferenceQueryError> {
    Ok(CompiledSparqlQuery::parse(query_text)?
        .dataset_specification()
        .clone())
}

/// Compatibility SELECT API against an authorization-qualified active dataset.
pub fn execute_select_with_dataset(
    store: &Store,
    query_text: &str,
    dataset: &ResolvedDataset,
    catalog: &GraphCatalog,
    include_internal_closure: bool,
) -> Result<ExecutedSolutions, ReferenceQueryError> {
    let compiled = CompiledSparqlQuery::parse(query_text)?;
    execute_compiled_select_with_dataset(
        store,
        &compiled,
        dataset,
        catalog,
        include_internal_closure,
    )
}

/// Compatibility SELECT API against an exact active dataset.
pub fn execute_compiled_select_with_dataset(
    store: &Store,
    compiled: &CompiledSparqlQuery,
    dataset: &ResolvedDataset,
    catalog: &GraphCatalog,
    include_internal_closure: bool,
) -> Result<ExecutedSolutions, ReferenceQueryError> {
    let limits = QueryExecutionLimits {
        max_solution_rows: usize::MAX,
        max_graph_triples: 1,
        max_graph_blank_nodes: 1,
    };
    match execute_compiled_query_with_dataset(
        store,
        compiled,
        dataset,
        catalog,
        include_internal_closure,
        limits,
    )? {
        ExecutedQueryResult::Solutions(value) => Ok(value),
        ExecutedQueryResult::Boolean(_) | ExecutedQueryResult::Graph { .. } => {
            Err(ReferenceQueryError::SelectRequired)
        }
    }
}

/// Execute an already-compiled query of any SPARQL 1.1 form against an exact active dataset.
pub fn execute_compiled_query_with_dataset(
    store: &Store,
    compiled: &CompiledSparqlQuery,
    dataset: &ResolvedDataset,
    catalog: &GraphCatalog,
    include_internal_closure: bool,
    limits: QueryExecutionLimits,
) -> Result<ExecutedQueryResult, ReferenceQueryError> {
    execute_compiled_query_with_dataset_cancellable(
        store,
        compiled,
        dataset,
        catalog,
        include_internal_closure,
        limits,
        None,
    )
}

/// Execute an exact active-dataset query with cooperative cancellation.
pub fn execute_compiled_query_with_dataset_cancellable(
    store: &Store,
    compiled: &CompiledSparqlQuery,
    dataset: &ResolvedDataset,
    catalog: &GraphCatalog,
    include_internal_closure: bool,
    limits: QueryExecutionLimits,
    cancellation_token: Option<CancellationToken>,
) -> Result<ExecutedQueryResult, ReferenceQueryError> {
    execute_compiled_query_with_dataset_federated_cancellable(
        store,
        compiled,
        dataset,
        catalog,
        include_internal_closure,
        limits,
        cancellation_token,
        None,
    )
}

/// Execute an exact active-dataset query with a policy-controlled SPARQL SERVICE handler.
///
/// The handler is query scoped: its call ceiling and evidence cover one complete scalar
/// algebra evaluation, including correlated and variable-endpoint SERVICE operators.
#[allow(clippy::too_many_arguments)]
pub fn execute_compiled_query_with_dataset_federated_cancellable(
    store: &Store,
    compiled: &CompiledSparqlQuery,
    dataset: &ResolvedDataset,
    catalog: &GraphCatalog,
    include_internal_closure: bool,
    limits: QueryExecutionLimits,
    cancellation_token: Option<CancellationToken>,
    federation: Option<ngkg_federation::FederationServiceHandler>,
) -> Result<ExecutedQueryResult, ReferenceQueryError> {
    let limits = limits.validate()?;
    catalog
        .validate()
        .map_err(|error| ReferenceQueryError::Dataset(error.to_string()))?;
    let evaluator = if let Some(token) = cancellation_token {
        SparqlEvaluator::new().with_cancellation_token(token)
    } else {
        SparqlEvaluator::new()
    };
    let evaluator = if let Some(handler) = federation {
        evaluator.with_default_service_handler(handler)
    } else {
        evaluator
    };
    let mut prepared = evaluator.for_query(compiled.query_clone());
    let mut default_graphs = Vec::with_capacity(
        dataset
            .default_graph_ids
            .len()
            .saturating_add(usize::from(include_internal_closure)),
    );
    if include_internal_closure {
        default_graphs.push(GraphName::DefaultGraph);
    }
    for graph_id in &dataset.default_graph_ids {
        default_graphs.push(catalog_named_node(catalog, *graph_id)?.into());
    }
    let named_graphs = dataset
        .named_graph_ids
        .iter()
        .map(|graph_id| catalog_named_node(catalog, *graph_id).map(Into::into))
        .collect::<Result<Vec<NamedOrBlankNode>, _>>()?;
    prepared.dataset_mut().set_default_graph(default_graphs);
    prepared
        .dataset_mut()
        .set_available_named_graphs(named_graphs);
    execute_prepared(prepared.on_store(store).execute(), compiled, limits)
}

/// Execute a query whose BGP leaves were replaced by exact entailment `VALUES` relations.
///
/// `compiled` remains the original query and therefore controls result form, variable ordering,
/// limits and result certification. `rewritten` differs only in BGP evaluation and is evaluated
/// against the authorization-qualified asserted dataset without the finite closure graph.
pub fn execute_entailment_rewritten_query_with_dataset_cancellable(
    store: &Store,
    compiled: &CompiledSparqlQuery,
    rewritten: Query,
    dataset: &ResolvedDataset,
    catalog: &GraphCatalog,
    limits: QueryExecutionLimits,
    cancellation_token: Option<CancellationToken>,
) -> Result<ExecutedQueryResult, ReferenceQueryError> {
    execute_entailment_rewritten_query_with_dataset_federated_cancellable(
        store,
        compiled,
        rewritten,
        dataset,
        catalog,
        limits,
        cancellation_token,
        None,
    )
}

/// Execute exact OWL-rewritten outer algebra with policy-controlled SERVICE evaluation.
#[allow(clippy::too_many_arguments)]
pub fn execute_entailment_rewritten_query_with_dataset_federated_cancellable(
    store: &Store,
    compiled: &CompiledSparqlQuery,
    rewritten: Query,
    dataset: &ResolvedDataset,
    catalog: &GraphCatalog,
    limits: QueryExecutionLimits,
    cancellation_token: Option<CancellationToken>,
    federation: Option<ngkg_federation::FederationServiceHandler>,
) -> Result<ExecutedQueryResult, ReferenceQueryError> {
    let limits = limits.validate()?;
    catalog
        .validate()
        .map_err(|error| ReferenceQueryError::Dataset(error.to_string()))?;
    let evaluator = if let Some(token) = cancellation_token {
        SparqlEvaluator::new().with_cancellation_token(token)
    } else {
        SparqlEvaluator::new()
    };
    let evaluator = if let Some(handler) = federation {
        evaluator.with_default_service_handler(handler)
    } else {
        evaluator
    };
    let mut prepared = evaluator.for_query(rewritten);
    let default_graphs = dataset
        .default_graph_ids
        .iter()
        .map(|graph_id| catalog_named_node(catalog, *graph_id).map(Into::into))
        .collect::<Result<Vec<GraphName>, _>>()?;
    let named_graphs = dataset
        .named_graph_ids
        .iter()
        .map(|graph_id| catalog_named_node(catalog, *graph_id).map(Into::into))
        .collect::<Result<Vec<NamedOrBlankNode>, _>>()?;
    prepared.dataset_mut().set_default_graph(default_graphs);
    prepared
        .dataset_mut()
        .set_available_named_graphs(named_graphs);
    execute_prepared(prepared.on_store(store).execute(), compiled, limits)
}

fn catalog_named_node(
    catalog: &GraphCatalog,
    graph_id: u32,
) -> Result<NamedNode, ReferenceQueryError> {
    let graph = catalog.by_id(graph_id).ok_or_else(|| {
        ReferenceQueryError::Dataset(format!("graph ID {graph_id} is absent from its catalog"))
    })?;
    let LogicalGraphName::Named { iri } = &graph.name else {
        return Err(ReferenceQueryError::Dataset(
            "the physical source default graph cannot enter an active SPARQL dataset".to_owned(),
        ));
    };
    NamedNode::new(iri.clone()).map_err(|error| ReferenceQueryError::Dataset(error.to_string()))
}

fn execute_prepared(
    result: Result<QueryResults<'_>, QueryEvaluationError>,
    compiled: &CompiledSparqlQuery,
    limits: QueryExecutionLimits,
) -> Result<ExecutedQueryResult, ReferenceQueryError> {
    let expected_form = compiled.form();
    let result = result.map_err(|error| ReferenceQueryError::Sparql(error.to_string()))?;
    match result {
        QueryResults::Solutions(mut solutions) => {
            if expected_form != QueryForm::Select {
                return Err(ReferenceQueryError::Sparql(
                    "evaluator returned solution rows for a non-SELECT query".to_owned(),
                ));
            }
            let mut head = solutions
                .variables()
                .iter()
                .map(|variable| variable.as_str().to_owned())
                .collect::<Vec<_>>();
            let source_order = compiled.solution_variable_order();
            head.sort_by_key(|variable| {
                source_order
                    .iter()
                    .position(|candidate| candidate == variable)
                    .unwrap_or(usize::MAX)
            });
            let mut canonical_rows = Vec::new();
            let mut bindings = Vec::new();
            let mut entity_iris = BTreeSet::new();
            for row in &mut solutions {
                if bindings.len() >= limits.max_solution_rows {
                    return Err(ReferenceQueryError::ResultLimit(format!(
                        "SELECT exceeds {} solution rows",
                        limits.max_solution_rows
                    )));
                }
                let row = row.map_err(|error| ReferenceQueryError::Sparql(error.to_string()))?;
                let mut canonical = BTreeMap::new();
                let mut binding = Map::new();
                for (variable, term) in row.iter() {
                    let name = variable.as_str().to_owned();
                    if let Term::NamedNode(node) = term {
                        entity_iris.insert(node.as_str().to_owned());
                    }
                    canonical.insert(name.clone(), canonical_term(term));
                    binding.insert(name, term_to_sparql_json(term));
                }
                canonical_rows.push(canonical);
                bindings.push(Value::Object(binding));
            }
            Ok(ExecutedQueryResult::Solutions(ExecutedSolutions {
                head,
                bindings,
                entity_iris,
                canonical_rows,
            }))
        }
        QueryResults::Boolean(value) => {
            if expected_form != QueryForm::Ask {
                return Err(ReferenceQueryError::Sparql(
                    "evaluator returned a boolean for a non-ASK query".to_owned(),
                ));
            }
            Ok(ExecutedQueryResult::Boolean(value))
        }
        QueryResults::Graph(mut graph) => {
            if !matches!(expected_form, QueryForm::Construct | QueryForm::Describe) {
                return Err(ReferenceQueryError::Sparql(
                    "evaluator returned an RDF graph for a non-graph query".to_owned(),
                ));
            }
            let mut triples = Vec::new();
            for triple in &mut graph {
                if triples.len() >= limits.max_graph_triples {
                    return Err(ReferenceQueryError::ResultLimit(format!(
                        "graph result exceeds {} triples",
                        limits.max_graph_triples
                    )));
                }
                triples
                    .push(triple.map_err(|error| ReferenceQueryError::Sparql(error.to_string()))?);
            }
            Ok(ExecutedQueryResult::Graph {
                form: expected_form,
                graph: canonicalize_graph(triples, limits)?,
            })
        }
    }
}

/// Parse an independently authored expected result using the query form and file format.
pub fn parse_expected(
    path: &Path,
    bytes: &[u8],
    form: QueryForm,
    limits: QueryExecutionLimits,
) -> Result<ExpectedQueryResult, ReferenceQueryError> {
    match form {
        QueryForm::Select => parse_expected_solutions(bytes).map(ExpectedQueryResult::Solutions),
        QueryForm::Ask => parse_expected_boolean(bytes).map(ExpectedQueryResult::Boolean),
        QueryForm::Construct | QueryForm::Describe => {
            parse_expected_graph(path, bytes, limits).map(ExpectedQueryResult::Graph)
        }
    }
}

fn parse_expected_solutions(bytes: &[u8]) -> Result<ExpectedSolutions, ReferenceQueryError> {
    let value: Value = serde_json::from_slice(bytes)
        .map_err(|error| ReferenceQueryError::InvalidExpected(error.to_string()))?;
    let head = value
        .pointer("/head/vars")
        .and_then(Value::as_array)
        .ok_or_else(|| ReferenceQueryError::InvalidExpected("head.vars is required".to_owned()))?
        .iter()
        .map(|value| {
            value.as_str().map(ToOwned::to_owned).ok_or_else(|| {
                ReferenceQueryError::InvalidExpected("head variable is not a string".to_owned())
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let rows = value
        .pointer("/results/bindings")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            ReferenceQueryError::InvalidExpected("results.bindings is required".to_owned())
        })?;
    let mut canonical_rows = Vec::with_capacity(rows.len());
    for row in rows {
        let object = row.as_object().ok_or_else(|| {
            ReferenceQueryError::InvalidExpected("binding row is not an object".to_owned())
        })?;
        let mut canonical = BTreeMap::new();
        for (variable, term) in object {
            canonical.insert(variable.clone(), expected_term(term)?);
        }
        canonical_rows.push(canonical);
    }
    Ok(ExpectedSolutions {
        head,
        canonical_rows,
    })
}

fn parse_expected_boolean(bytes: &[u8]) -> Result<bool, ReferenceQueryError> {
    let value: Value = serde_json::from_slice(bytes)
        .map_err(|error| ReferenceQueryError::InvalidExpected(error.to_string()))?;
    value
        .get("boolean")
        .and_then(Value::as_bool)
        .ok_or_else(|| {
            ReferenceQueryError::InvalidExpected("ASK result requires boolean".to_owned())
        })
}

fn parse_expected_graph(
    path: &Path,
    bytes: &[u8],
    limits: QueryExecutionLimits,
) -> Result<Vec<String>, ReferenceQueryError> {
    let extension = path
        .extension()
        .and_then(|value| value.to_str())
        .ok_or_else(|| {
            ReferenceQueryError::ExpectedGraphFormat(
                "graph expected-result artifact requires a recognized RDF extension".to_owned(),
            )
        })?;
    let format = RdfFormat::from_extension(extension)
        .ok_or_else(|| ReferenceQueryError::ExpectedGraphFormat(extension.to_owned()))?;
    let mut triples = Vec::new();
    let base_iri = url::Url::from_file_path(path)
        .map(String::from)
        .map_err(|()| ReferenceQueryError::ExpectedGraphFormat(path.display().to_string()))?;
    let parser = RdfParser::from_format(format)
        .with_base_iri(base_iri)
        .map_err(|error| ReferenceQueryError::InvalidExpected(error.to_string()))?;
    for parsed in parser.for_reader(Cursor::new(bytes)) {
        if triples.len() >= limits.max_graph_triples {
            return Err(ReferenceQueryError::ResultLimit(format!(
                "expected graph exceeds {} triples",
                limits.max_graph_triples
            )));
        }
        let quad =
            parsed.map_err(|error| ReferenceQueryError::InvalidExpected(error.to_string()))?;
        if !matches!(quad.graph_name, GraphName::DefaultGraph) {
            return Err(ReferenceQueryError::InvalidExpected(
                "CONSTRUCT/DESCRIBE expected artifacts must encode one RDF graph, not named graphs"
                    .to_owned(),
            ));
        }
        triples.push(Triple::new(quad.subject, quad.predicate, quad.object));
    }
    Ok(canonicalize_graph(triples, limits)?.ntriples)
}

/// Compare an observed result using SPARQL-form-specific equality and return its v2 hash.
pub fn verify_expected(
    observed: &ExecutedQueryResult,
    expected: &ExpectedQueryResult,
    ordered: bool,
    limits: QueryExecutionLimits,
) -> Result<String, ReferenceQueryError> {
    match (observed, expected) {
        (ExecutedQueryResult::Solutions(observed), ExpectedQueryResult::Solutions(expected)) => {
            if !solution_results_equivalent(
                &observed.head,
                &observed.canonical_rows,
                &expected.head,
                &expected.canonical_rows,
                ordered,
                limits,
            )? {
                return Err(ReferenceQueryError::AnswerMismatch(format!(
                    "SELECT head observed={:?} expected={:?}; first observed rows={:?}; first expected rows={:?}",
                    observed.head,
                    expected.head,
                    observed.canonical_rows.iter().take(4).collect::<Vec<_>>(),
                    expected.canonical_rows.iter().take(4).collect::<Vec<_>>()
                )));
            }
        }
        (ExecutedQueryResult::Boolean(observed), ExpectedQueryResult::Boolean(expected)) => {
            if observed != expected {
                return Err(ReferenceQueryError::AnswerMismatch(format!(
                    "ASK observed={observed} expected={expected}"
                )));
            }
        }
        (
            ExecutedQueryResult::Graph {
                graph: observed, ..
            },
            ExpectedQueryResult::Graph(expected),
        ) => {
            if observed.ntriples != *expected {
                return Err(ReferenceQueryError::AnswerMismatch(format!(
                    "graph observed triples={} expected triples={}",
                    observed.ntriples.len(),
                    expected.len()
                )));
            }
        }
        _ => {
            return Err(ReferenceQueryError::AnswerMismatch(
                "query-result forms differ".to_owned(),
            ));
        }
    }
    canonical_query_result_sha256(observed, ordered, limits)
}

/// Phase 22+ distributed SELECT fragment multiset hash retained unchanged.
pub fn canonical_sparql_multiset_sha256(
    head: &[String],
    bindings: &[Value],
    ordered: bool,
) -> Result<String, ReferenceQueryError> {
    let mut canonical_rows = canonical_rows_from_bindings(bindings)?;
    if !ordered {
        canonical_rows.sort_unstable();
    }
    let bytes = serde_json::to_vec(&(head, canonical_rows))
        .map_err(|error| ReferenceQueryError::InvalidExpected(error.to_string()))?;
    Ok(crate::sha256_hex(&bytes))
}

/// Canonical v2 result hash covering all four SPARQL query forms.
pub fn canonical_query_result_sha256(
    result: &ExecutedQueryResult,
    ordered: bool,
    limits: QueryExecutionLimits,
) -> Result<String, ReferenceQueryError> {
    let limits = limits.validate()?;
    let bytes = match result {
        ExecutedQueryResult::Solutions(value) => {
            if value.bindings.len() > limits.max_solution_rows {
                return Err(ReferenceQueryError::ResultLimit(format!(
                    "SELECT exceeds {} solution rows",
                    limits.max_solution_rows
                )));
            }
            let mut rows = value.canonical_rows.clone();
            if !ordered {
                rows.sort_unstable();
            }
            serde_json::to_vec(&("select", &value.head, rows))
        }
        ExecutedQueryResult::Boolean(value) => serde_json::to_vec(&("ask", value)),
        ExecutedQueryResult::Graph { form, graph: value } => {
            if value.ntriples.len() > limits.max_graph_triples {
                return Err(ReferenceQueryError::ResultLimit(format!(
                    "graph result exceeds {} triples",
                    limits.max_graph_triples
                )));
            }
            serde_json::to_vec(&(form.as_str(), &value.ntriples))
        }
    }
    .map_err(|error| ReferenceQueryError::InvalidExpected(error.to_string()))?;
    Ok(crate::sha256_hex(&bytes))
}

/// Rebuild and hash a cached/custom API payload before accepting it as certified.
pub fn canonical_query_payload_sha256(
    form: QueryForm,
    head: &[String],
    bindings: &[Value],
    boolean_result: Option<bool>,
    graph_ntriples: &[String],
    ordered: bool,
    limits: QueryExecutionLimits,
) -> Result<String, ReferenceQueryError> {
    let result = match form {
        QueryForm::Select => {
            if boolean_result.is_some() || !graph_ntriples.is_empty() {
                return Err(ReferenceQueryError::InvalidExpected(
                    "SELECT payload contains non-solution result fields".to_owned(),
                ));
            }
            ExecutedQueryResult::Solutions(ExecutedSolutions {
                head: head.to_vec(),
                bindings: bindings.to_vec(),
                entity_iris: binding_entity_iris(bindings),
                canonical_rows: canonical_rows_from_bindings(bindings)?,
            })
        }
        QueryForm::Ask => {
            if !head.is_empty() || !bindings.is_empty() || !graph_ntriples.is_empty() {
                return Err(ReferenceQueryError::InvalidExpected(
                    "ASK payload contains solution or graph fields".to_owned(),
                ));
            }
            ExecutedQueryResult::Boolean(boolean_result.ok_or_else(|| {
                ReferenceQueryError::InvalidExpected("ASK payload has no boolean".to_owned())
            })?)
        }
        QueryForm::Construct | QueryForm::Describe => {
            if !head.is_empty() || !bindings.is_empty() || boolean_result.is_some() {
                return Err(ReferenceQueryError::InvalidExpected(
                    "graph payload contains solution or boolean fields".to_owned(),
                ));
            }
            let graph = graph_from_ntriples(graph_ntriples, limits)?;
            ExecutedQueryResult::Graph { form, graph }
        }
    };
    canonical_query_result_sha256(&result, ordered, limits)
}

pub fn verify_binding_values(
    head: &[String],
    bindings: &[Value],
    expected: &ExpectedSolutions,
    ordered: bool,
) -> Result<String, ReferenceQueryError> {
    let observed_rows = canonical_rows_from_bindings(bindings)?;
    let limits = QueryExecutionLimits {
        max_solution_rows: bindings.len().max(1),
        max_graph_triples: 1,
        max_graph_blank_nodes: bindings.len().saturating_mul(head.len()).max(1),
    };
    if !solution_results_equivalent(
        head,
        &observed_rows,
        &expected.head,
        &expected.canonical_rows,
        ordered,
        limits,
    )? {
        return Err(ReferenceQueryError::AnswerMismatch(format!(
            "SELECT head observed={head:?} expected={:?}; first observed rows={:?}; first expected rows={:?}",
            expected.head,
            observed_rows.iter().take(4).collect::<Vec<_>>(),
            expected.canonical_rows.iter().take(4).collect::<Vec<_>>()
        )));
    }
    canonical_sparql_multiset_sha256(head, bindings, ordered)
}

fn solution_results_equivalent(
    observed_head: &[String],
    observed_rows: &[BTreeMap<String, CanonicalTerm>],
    expected_head: &[String],
    expected_rows: &[BTreeMap<String, CanonicalTerm>],
    ordered: bool,
    limits: QueryExecutionLimits,
) -> Result<bool, ReferenceQueryError> {
    if observed_rows.len() > limits.max_solution_rows
        || expected_rows.len() > limits.max_solution_rows
    {
        return Err(ReferenceQueryError::ResultLimit(format!(
            "solution comparison exceeds {} rows",
            limits.max_solution_rows
        )));
    }
    let observed_variables = observed_head.iter().collect::<BTreeSet<_>>();
    let expected_variables = expected_head.iter().collect::<BTreeSet<_>>();
    if observed_variables != expected_variables || observed_rows.len() != expected_rows.len() {
        return Ok(false);
    }

    let mut observed = observed_rows.iter().map(comparison_row).collect::<Vec<_>>();
    let mut expected = expected_rows.iter().map(comparison_row).collect::<Vec<_>>();
    let has_blank_nodes = observed
        .iter()
        .chain(&expected)
        .flat_map(|row| row.values())
        .any(|term| matches!(term, CanonicalTerm::BlankNode { .. }));
    if has_blank_nodes {
        let blank_node_count = observed
            .iter()
            .chain(&expected)
            .flat_map(|row| row.values())
            .filter(|term| matches!(term, CanonicalTerm::BlankNode { .. }))
            .count();
        if blank_node_count > limits.max_graph_blank_nodes.saturating_mul(2) {
            return Err(ReferenceQueryError::ResultLimit(format!(
                "solution comparison has {blank_node_count} blank-node bindings; ceiling is {} per result",
                limits.max_graph_blank_nodes
            )));
        }
        return Ok(canonical_solution_dataset(&observed, ordered)?
            == canonical_solution_dataset(&expected, ordered)?);
    }
    if !ordered {
        observed.sort_unstable();
        expected.sort_unstable();
    }
    Ok(observed == expected)
}

fn comparison_row(row: &BTreeMap<String, CanonicalTerm>) -> BTreeMap<String, CanonicalTerm> {
    row.iter()
        .map(|(variable, term)| (variable.clone(), comparison_term(term)))
        .collect()
}

fn comparison_term(term: &CanonicalTerm) -> CanonicalTerm {
    let CanonicalTerm::Literal {
        value,
        datatype,
        language,
    } = term
    else {
        return term.clone();
    };
    let normalized_language = language.as_ref().map(|value| value.to_ascii_lowercase());
    let normalized_value = match datatype.as_str() {
        "http://www.w3.org/2001/XMLSchema#boolean" => match value.as_str() {
            "1" | "true" => "true".to_owned(),
            "0" | "false" => "false".to_owned(),
            _ => value.clone(),
        },
        "http://www.w3.org/2001/XMLSchema#integer"
        | "http://www.w3.org/2001/XMLSchema#nonPositiveInteger"
        | "http://www.w3.org/2001/XMLSchema#negativeInteger"
        | "http://www.w3.org/2001/XMLSchema#long"
        | "http://www.w3.org/2001/XMLSchema#int"
        | "http://www.w3.org/2001/XMLSchema#short"
        | "http://www.w3.org/2001/XMLSchema#byte"
        | "http://www.w3.org/2001/XMLSchema#nonNegativeInteger"
        | "http://www.w3.org/2001/XMLSchema#unsignedLong"
        | "http://www.w3.org/2001/XMLSchema#unsignedInt"
        | "http://www.w3.org/2001/XMLSchema#unsignedShort"
        | "http://www.w3.org/2001/XMLSchema#unsignedByte"
        | "http://www.w3.org/2001/XMLSchema#positiveInteger" => {
            canonical_integer_lexical(value).unwrap_or_else(|| value.clone())
        }
        "http://www.w3.org/2001/XMLSchema#decimal" => {
            canonical_decimal_lexical(value).unwrap_or_else(|| value.clone())
        }
        "http://www.w3.org/2001/XMLSchema#float" => {
            canonical_float_lexical(value, false).unwrap_or_else(|| value.clone())
        }
        "http://www.w3.org/2001/XMLSchema#double" => {
            canonical_float_lexical(value, true).unwrap_or_else(|| value.clone())
        }
        "http://www.w3.org/2001/XMLSchema#dayTimeDuration" if duration_is_zero(value) => {
            "PT0S".to_owned()
        }
        _ => value.clone(),
    };
    CanonicalTerm::Literal {
        value: normalized_value,
        datatype: datatype.clone(),
        language: normalized_language,
    }
}

fn canonical_integer_lexical(value: &str) -> Option<String> {
    let (negative, digits) = match value.as_bytes().first() {
        Some(b'+') => (false, &value[1..]),
        Some(b'-') => (true, &value[1..]),
        _ => (false, value),
    };
    if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let digits = digits.trim_start_matches('0');
    if digits.is_empty() {
        return Some("0".to_owned());
    }
    Some(if negative {
        format!("-{digits}")
    } else {
        digits.to_owned()
    })
}

fn canonical_decimal_lexical(value: &str) -> Option<String> {
    let (negative, unsigned) = match value.as_bytes().first() {
        Some(b'+') => (false, &value[1..]),
        Some(b'-') => (true, &value[1..]),
        _ => (false, value),
    };
    let mut pieces = unsigned.split('.');
    let integer = pieces.next()?;
    let fraction = pieces.next().unwrap_or_default();
    if pieces.next().is_some()
        || (integer.is_empty() && fraction.is_empty())
        || !integer.bytes().all(|byte| byte.is_ascii_digit())
        || !fraction.bytes().all(|byte| byte.is_ascii_digit())
    {
        return None;
    }
    let integer = integer.trim_start_matches('0');
    let fraction = fraction.trim_end_matches('0');
    let integer = if integer.is_empty() { "0" } else { integer };
    let zero = integer == "0" && fraction.is_empty();
    let unsigned = if fraction.is_empty() {
        integer.to_owned()
    } else {
        format!("{integer}.{fraction}")
    };
    Some(if negative && !zero {
        format!("-{unsigned}")
    } else {
        unsigned
    })
}

fn canonical_float_lexical(value: &str, double_precision: bool) -> Option<String> {
    let value = match value {
        "INF" => f64::INFINITY,
        "-INF" => f64::NEG_INFINITY,
        "NaN" => return Some("NaN".to_owned()),
        _ => value.parse::<f64>().ok()?,
    };
    if value == 0.0 {
        return Some("0".to_owned());
    }
    if double_precision {
        Some(format!("{:016x}", value.to_bits()))
    } else {
        Some(format!("{:08x}", (value as f32).to_bits()))
    }
}

fn duration_is_zero(value: &str) -> bool {
    let unsigned = value.strip_prefix('-').unwrap_or(value);
    let Some(body) = unsigned.strip_prefix('P') else {
        return false;
    };
    let digits = body
        .chars()
        .filter(|value| value.is_ascii_digit() || *value == '.')
        .collect::<String>();
    !digits.is_empty() && digits.chars().all(|value| matches!(value, '0' | '.'))
}

fn canonical_solution_dataset(
    rows: &[BTreeMap<String, CanonicalTerm>],
    ordered: bool,
) -> Result<Vec<String>, ReferenceQueryError> {
    let root = NamedNode::new_unchecked("urn:ngkg:solution-set");
    let row_predicate = NamedNode::new_unchecked("urn:ngkg:solution-row");
    let index_predicate = NamedNode::new_unchecked("urn:ngkg:solution-index");
    let row_type_predicate =
        NamedNode::new_unchecked("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
    let row_type = NamedNode::new_unchecked("urn:ngkg:SolutionRow");
    let integer_datatype = NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#integer");
    let mut dataset = Dataset::new();
    for (index, row) in rows.iter().enumerate() {
        let row_node = BlankNode::new_unchecked(format!("row{index}"));
        dataset.insert(&Quad::new(
            root.clone(),
            row_predicate.clone(),
            row_node.clone(),
            GraphName::DefaultGraph,
        ));
        dataset.insert(&Quad::new(
            row_node.clone(),
            row_type_predicate.clone(),
            row_type.clone(),
            GraphName::DefaultGraph,
        ));
        if ordered {
            dataset.insert(&Quad::new(
                row_node.clone(),
                index_predicate.clone(),
                Literal::new_typed_literal(index.to_string(), integer_datatype.clone()),
                GraphName::DefaultGraph,
            ));
        }
        for (variable, term) in row {
            let predicate = NamedNode::new_unchecked(format!(
                "urn:ngkg:binding:{}",
                hex::encode(variable.as_bytes())
            ));
            dataset.insert(&Quad::new(
                row_node.clone(),
                predicate,
                comparison_rdf_term(term)?,
                GraphName::DefaultGraph,
            ));
        }
    }
    dataset.canonicalize(CanonicalizationAlgorithm::Rdfc10 {
        hash_algorithm: CanonicalizationHashAlgorithm::Sha256,
    });
    let mut lines = dataset
        .iter()
        .map(|quad| quad.to_string())
        .collect::<Vec<_>>();
    lines.sort_unstable();
    Ok(lines)
}

fn comparison_rdf_term(term: &CanonicalTerm) -> Result<Term, ReferenceQueryError> {
    match term {
        CanonicalTerm::NamedNode { value } => NamedNode::new(value.clone())
            .map(Term::NamedNode)
            .map_err(|error| ReferenceQueryError::InvalidExpected(error.to_string())),
        CanonicalTerm::BlankNode { value } => {
            let mut digest = Sha256::new();
            digest.update(b"ngkg-solution-blank-node-v1\0");
            digest.update(value.as_bytes());
            Ok(Term::BlankNode(BlankNode::new_unchecked(format!(
                "result{}",
                hex::encode(digest.finalize())
            ))))
        }
        CanonicalTerm::Literal {
            value,
            datatype,
            language,
        } => {
            if let Some(language) = language {
                Literal::new_language_tagged_literal(value.clone(), language.clone())
                    .map(Term::Literal)
                    .map_err(|error| ReferenceQueryError::InvalidExpected(error.to_string()))
            } else {
                let datatype = NamedNode::new(datatype.clone())
                    .map_err(|error| ReferenceQueryError::InvalidExpected(error.to_string()))?;
                Ok(Term::Literal(Literal::new_typed_literal(
                    value.clone(),
                    datatype,
                )))
            }
        }
    }
}

fn canonical_rows_from_bindings(
    bindings: &[Value],
) -> Result<Vec<BTreeMap<String, CanonicalTerm>>, ReferenceQueryError> {
    bindings
        .iter()
        .map(|row| {
            row.as_object()
                .ok_or_else(|| {
                    ReferenceQueryError::InvalidExpected("binding row is not an object".to_owned())
                })?
                .iter()
                .map(|(variable, term)| expected_term(term).map(|term| (variable.clone(), term)))
                .collect()
        })
        .collect()
}

fn canonicalize_graph(
    triples: Vec<Triple>,
    limits: QueryExecutionLimits,
) -> Result<ExecutedGraph, ReferenceQueryError> {
    let mut blank_nodes = BTreeSet::new();
    let mut entity_iris = BTreeSet::new();
    for triple in &triples {
        match &triple.subject {
            NamedOrBlankNode::NamedNode(node) => {
                entity_iris.insert(node.as_str().to_owned());
            }
            NamedOrBlankNode::BlankNode(node) => {
                blank_nodes.insert(node.as_str().to_owned());
            }
        }
        if let Term::NamedNode(node) = &triple.object {
            entity_iris.insert(node.as_str().to_owned());
        } else if let Term::BlankNode(node) = &triple.object {
            blank_nodes.insert(node.as_str().to_owned());
        }
    }
    if blank_nodes.len() > limits.max_graph_blank_nodes {
        return Err(ReferenceQueryError::ResultLimit(format!(
            "graph result has {} blank nodes; canonicalization ceiling is {}",
            blank_nodes.len(),
            limits.max_graph_blank_nodes
        )));
    }
    let mut dataset = Dataset::new();
    for triple in triples {
        dataset.insert(&Quad::new(
            triple.subject,
            triple.predicate,
            triple.object,
            GraphName::DefaultGraph,
        ));
    }
    dataset.canonicalize(CanonicalizationAlgorithm::Rdfc10 {
        hash_algorithm: CanonicalizationHashAlgorithm::Sha256,
    });
    let mut ntriples = dataset
        .iter()
        .map(|quad| format!("{quad}\n"))
        .collect::<Vec<_>>();
    ntriples.sort_unstable();
    Ok(ExecutedGraph {
        ntriples,
        entity_iris,
    })
}

fn graph_from_ntriples(
    lines: &[String],
    limits: QueryExecutionLimits,
) -> Result<ExecutedGraph, ReferenceQueryError> {
    if lines.len() > limits.max_graph_triples {
        return Err(ReferenceQueryError::ResultLimit(format!(
            "graph payload exceeds {} triples",
            limits.max_graph_triples
        )));
    }
    let bytes = lines.concat();
    let mut triples = Vec::with_capacity(lines.len());
    for parsed in
        RdfParser::from_format(RdfFormat::NTriples).for_reader(Cursor::new(bytes.as_bytes()))
    {
        let quad =
            parsed.map_err(|error| ReferenceQueryError::InvalidExpected(error.to_string()))?;
        if !matches!(quad.graph_name, GraphName::DefaultGraph) {
            return Err(ReferenceQueryError::InvalidExpected(
                "graph payload unexpectedly contains a named graph".to_owned(),
            ));
        }
        triples.push(Triple::new(quad.subject, quad.predicate, quad.object));
    }
    canonicalize_graph(triples, limits)
}

fn binding_entity_iris(bindings: &[Value]) -> BTreeSet<String> {
    bindings
        .iter()
        .filter_map(Value::as_object)
        .flat_map(|binding| binding.values())
        .filter_map(|term| {
            (term.get("type").and_then(Value::as_str) == Some("uri"))
                .then(|| term.get("value").and_then(Value::as_str))
                .flatten()
                .map(ToOwned::to_owned)
        })
        .collect()
}

/// Verify configured provenance links from all named result entities.
pub fn verify_source_links(
    store: &Store,
    entity_iris: &BTreeSet<String>,
    required_source_iris: &[String],
) -> Result<(), ReferenceQueryError> {
    if required_source_iris.is_empty() {
        return Ok(());
    }
    let entities = entity_iris
        .iter()
        .map(|iri| {
            NamedNode::new(iri.clone())
                .map(|node| format!("<{}>", node.as_str()))
                .map_err(|error| ReferenceQueryError::Sparql(error.to_string()))
        })
        .collect::<Result<Vec<_>, _>>()?;
    if entities.is_empty() {
        return Err(ReferenceQueryError::SourceEvidenceMissing(
            required_source_iris[0].clone(),
        ));
    }
    for source in required_source_iris {
        let source = NamedNode::new(source.clone())
            .map_err(|error| ReferenceQueryError::Sparql(error.to_string()))?;
        let ask = format!(
            "ASK WHERE {{ VALUES ?entity {{ {} }} GRAPH ?graph {{ ?entity <http://www.w3.org/ns/prov#wasDerivedFrom> <{}> }} }}",
            entities.join(" "),
            source.as_str()
        );
        let result = SparqlEvaluator::new()
            .parse_query(&ask)
            .map_err(|error| ReferenceQueryError::Sparql(error.to_string()))?
            .on_store(store)
            .execute()
            .map_err(|error| ReferenceQueryError::Sparql(error.to_string()))?;
        if !matches!(result, QueryResults::Boolean(true)) {
            return Err(ReferenceQueryError::SourceEvidenceMissing(
                source.as_str().to_owned(),
            ));
        }
    }
    Ok(())
}

/// Load an RDF fixture into an existing store using ordinary RDF dataset semantics.
/// A graph override places every parsed triple/quad into one named graph.
pub fn load_rdf_fixture(
    store: &Store,
    path: &Path,
    format: RdfFormat,
    graph_override: Option<NamedNode>,
) -> Result<(), ReferenceQueryError> {
    load_dataset(
        store,
        path,
        format,
        graph_override,
        DefaultDatasetPolicy::StoredDefault,
    )
}

/// Load an RDF fixture with an explicit retrieval IRI for relative IRI resolution.
pub fn load_rdf_fixture_with_base_iri(
    store: &Store,
    path: &Path,
    format: RdfFormat,
    graph_override: Option<NamedNode>,
    base_iri: &str,
) -> Result<(), ReferenceQueryError> {
    load_dataset_with_base_iri(
        store,
        path,
        format,
        graph_override,
        DefaultDatasetPolicy::StoredDefault,
        Some(base_iri),
    )
}

pub fn query_file(path: &Path) -> Result<String, ReferenceQueryError> {
    fs::read_to_string(path).map_err(ReferenceQueryError::Io)
}

fn load_dataset(
    store: &Store,
    path: &Path,
    format: RdfFormat,
    graph_override: Option<NamedNode>,
    default_dataset_policy: DefaultDatasetPolicy,
) -> Result<(), ReferenceQueryError> {
    load_dataset_with_base_iri(
        store,
        path,
        format,
        graph_override,
        default_dataset_policy,
        None,
    )
}

fn load_dataset_with_base_iri(
    store: &Store,
    path: &Path,
    format: RdfFormat,
    graph_override: Option<NamedNode>,
    default_dataset_policy: DefaultDatasetPolicy,
    base_iri: Option<&str>,
) -> Result<(), ReferenceQueryError> {
    let input = BufReader::new(File::open(path)?);
    let parser = match base_iri {
        Some(value) => RdfParser::from_format(format)
            .with_base_iri(value)
            .map_err(|error| ReferenceQueryError::Rdf(error.to_string()))?,
        None => RdfParser::from_format(format),
    };
    for quad in parser.for_reader(input) {
        let quad = quad.map_err(|error| ReferenceQueryError::Rdf(error.to_string()))?;
        let quad = if let Some(graph) = &graph_override {
            Quad::new(
                quad.subject,
                quad.predicate,
                quad.object,
                GraphName::NamedNode(graph.clone()),
            )
        } else {
            quad
        };
        if matches!(&quad.graph_name, GraphName::BlankNode(_)) {
            return Err(ReferenceQueryError::BlankGraphName);
        }
        let quad = standardize_named_graph_blank_nodes(quad)?;
        match default_dataset_policy {
            DefaultDatasetPolicy::StoredDefault => {
                store
                    .insert(&quad)
                    .map_err(|error| ReferenceQueryError::Rdf(error.to_string()))?;
            }
            DefaultDatasetPolicy::UnionDefault => {
                if graph_override.is_some() {
                    // The finite reasoner materialization is retained only in the
                    // physical store default graph. Dataset preparation may include
                    // it in the active default graph but can never expose it through
                    // GRAPH or FROM NAMED.
                    let closure_member = Quad::new(
                        quad.subject,
                        quad.predicate,
                        quad.object,
                        GraphName::DefaultGraph,
                    );
                    store
                        .insert(&closure_member)
                        .map_err(|error| ReferenceQueryError::Rdf(error.to_string()))?;
                } else if matches!(&quad.graph_name, GraphName::DefaultGraph) {
                    // The uploaded physical default graph is preserved in immutable
                    // source and columnar artifacts but excluded from the NGKG service
                    // dataset, whose default is an authorized named-graph union.
                    continue;
                } else {
                    store
                        .insert(&quad)
                        .map_err(|error| ReferenceQueryError::Rdf(error.to_string()))?;
                    let union_member = Quad::new(
                        quad.subject.clone(),
                        quad.predicate.clone(),
                        quad.object.clone(),
                        GraphName::DefaultGraph,
                    );
                    store
                        .insert(&union_member)
                        .map_err(|error| ReferenceQueryError::Rdf(error.to_string()))?;
                }
            }
        }
    }
    Ok(())
}

fn standardize_named_graph_blank_nodes(quad: Quad) -> Result<Quad, ReferenceQueryError> {
    let GraphName::NamedNode(graph) = &quad.graph_name else {
        return Ok(quad);
    };
    let subject = match quad.subject {
        NamedOrBlankNode::NamedNode(node) => NamedOrBlankNode::NamedNode(node),
        NamedOrBlankNode::BlankNode(node) => {
            NamedOrBlankNode::BlankNode(scoped_blank_node(graph, &node)?)
        }
    };
    let object = match quad.object {
        Term::NamedNode(node) => Term::NamedNode(node),
        Term::BlankNode(node) => Term::BlankNode(scoped_blank_node(graph, &node)?),
        Term::Literal(literal) => Term::Literal(literal),
    };
    Ok(Quad::new(subject, quad.predicate, object, quad.graph_name))
}

fn scoped_blank_node(
    graph: &NamedNode,
    node: &BlankNode,
) -> Result<BlankNode, ReferenceQueryError> {
    let mut digest = Sha256::new();
    digest.update(b"ngkg-graph-scoped-blank-node-v1\0");
    digest.update(graph.as_str().as_bytes());
    digest.update(b"\0");
    digest.update(node.as_str().as_bytes());
    BlankNode::new(format!("ngkg{}", hex::encode(digest.finalize())))
        .map_err(|error| ReferenceQueryError::Rdf(error.to_string()))
}

fn canonical_term(term: &Term) -> CanonicalTerm {
    match term {
        Term::NamedNode(node) => CanonicalTerm::NamedNode {
            value: node.as_str().to_owned(),
        },
        Term::BlankNode(node) => CanonicalTerm::BlankNode {
            value: node.as_str().to_owned(),
        },
        Term::Literal(literal) => CanonicalTerm::Literal {
            value: literal.value().to_owned(),
            datatype: literal.datatype().as_str().to_owned(),
            language: literal.language().map(ToOwned::to_owned),
        },
    }
}

fn term_to_sparql_json(term: &Term) -> Value {
    match term {
        Term::NamedNode(node) => json!({"type": "uri", "value": node.as_str()}),
        Term::BlankNode(node) => json!({"type": "bnode", "value": node.as_str()}),
        Term::Literal(literal) => {
            let mut value = Map::new();
            value.insert("type".to_owned(), Value::String("literal".to_owned()));
            value.insert(
                "value".to_owned(),
                Value::String(literal.value().to_owned()),
            );
            if let Some(language) = literal.language() {
                value.insert("xml:lang".to_owned(), Value::String(language.to_owned()));
            } else {
                value.insert(
                    "datatype".to_owned(),
                    Value::String(literal.datatype().as_str().to_owned()),
                );
            }
            Value::Object(value)
        }
    }
}

fn expected_term(value: &Value) -> Result<CanonicalTerm, ReferenceQueryError> {
    let object = value
        .as_object()
        .ok_or_else(|| ReferenceQueryError::InvalidExpected("term is not an object".to_owned()))?;
    let kind = object
        .get("type")
        .and_then(Value::as_str)
        .ok_or_else(|| ReferenceQueryError::InvalidExpected("term type is required".to_owned()))?;
    let term_value = object
        .get("value")
        .and_then(Value::as_str)
        .ok_or_else(|| ReferenceQueryError::InvalidExpected("term value is required".to_owned()))?
        .to_owned();
    match kind {
        "uri" => Ok(CanonicalTerm::NamedNode { value: term_value }),
        "bnode" => Ok(CanonicalTerm::BlankNode { value: term_value }),
        "literal" | "typed-literal" => {
            let language = object
                .get("xml:lang")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned);
            let datatype = object
                .get("datatype")
                .and_then(Value::as_str)
                .unwrap_or(if language.is_some() {
                    "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString"
                } else {
                    "http://www.w3.org/2001/XMLSchema#string"
                })
                .to_owned();
            Ok(CanonicalTerm::Literal {
                value: term_value,
                datatype,
                language,
            })
        }
        other => Err(ReferenceQueryError::InvalidExpected(format!(
            "unsupported term type {other}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, BTreeSet},
        fs,
        path::PathBuf,
    };

    use serde_json::{Value, json};
    use uuid::Uuid;

    use super::{
        CanonicalTerm, DefaultDatasetPolicy, ExecutedQueryResult, QueryExecutionLimits,
        ReferenceQueryError, build_store, build_store_with_dataset_policy,
        canonical_query_payload_sha256, canonical_sparql_multiset_sha256, execute_compiled_query,
        execute_select, solution_results_equivalent,
    };
    use ngkg_dataset::{
        GraphDeclaration, ProtocolDatasetSpecification, QueryDatasetSpecification, compile_catalog,
        resolve_dataset,
    };
    use ngkg_sparql_compiler::{CompiledSparqlQuery, QueryForm};

    fn temporary_file(extension: &str, contents: &str) -> Result<PathBuf, std::io::Error> {
        let path =
            std::env::temp_dir().join(format!("ngkg-query-test-{}.{}", Uuid::new_v4(), extension));
        fs::write(&path, contents)?;
        Ok(path)
    }

    fn graph_declaration(iri: &str, label: &str) -> GraphDeclaration {
        GraphDeclaration {
            graph_iri: iri.to_owned(),
            role: "semkg".to_owned(),
            authorization_labels: BTreeSet::from([label.to_owned()]),
            query_visible: true,
            reasoning_visible: true,
        }
    }

    #[test]
    fn service_default_is_named_graph_union_and_named_graphs_remain_isolated()
    -> Result<(), Box<dyn std::error::Error>> {
        let dataset = temporary_file(
            "nq",
            concat!(
                "<https://example.test/s> <https://example.test/p> <https://example.test/o1> <https://example.test/g1> .\n",
                "<https://example.test/s> <https://example.test/p> <https://example.test/o2> <https://example.test/g2> .\n",
            ),
        )?;
        let closure = temporary_file("nt", "")?;
        let store = build_store(&dataset, &closure, "https://example.test/closure")?;

        let union = execute_select(
            &store,
            "SELECT ?o WHERE { <https://example.test/s> <https://example.test/p> ?o }",
        )?;
        assert_eq!(union.bindings.len(), 2);
        assert!(union.entity_iris.contains("https://example.test/o1"));
        assert!(union.entity_iris.contains("https://example.test/o2"));

        let graph = execute_select(
            &store,
            "SELECT ?o WHERE { GRAPH <https://example.test/g1> { <https://example.test/s> <https://example.test/p> ?o } }",
        )?;
        assert_eq!(graph.bindings.len(), 1);
        assert!(graph.entity_iris.contains("https://example.test/o1"));
        assert!(!graph.entity_iris.contains("https://example.test/o2"));

        let graph_variable = execute_select(
            &store,
            "SELECT DISTINCT ?g WHERE { GRAPH ?g { <https://example.test/s> <https://example.test/p> ?o } }",
        )?;
        assert_eq!(graph_variable.bindings.len(), 2);
        assert!(
            graph_variable
                .entity_iris
                .contains("https://example.test/g1")
        );
        assert!(
            graph_variable
                .entity_iris
                .contains("https://example.test/g2")
        );

        fs::remove_file(dataset)?;
        fs::remove_file(closure)?;
        Ok(())
    }

    #[test]
    fn explicit_dataset_clause_replaces_union_default() -> Result<(), Box<dyn std::error::Error>> {
        let dataset = temporary_file(
            "nq",
            concat!(
                "<https://example.test/s> <https://example.test/p> <https://example.test/o1> <https://example.test/g1> .\n",
                "<https://example.test/s> <https://example.test/p> <https://example.test/o2> <https://example.test/g2> .\n",
            ),
        )?;
        let closure = temporary_file("nt", "")?;
        let store = build_store(&dataset, &closure, "https://example.test/closure")?;

        let from = execute_select(
            &store,
            "SELECT ?o FROM <https://example.test/g1> WHERE { <https://example.test/s> <https://example.test/p> ?o }",
        )?;
        assert_eq!(from.bindings.len(), 1);
        assert!(from.entity_iris.contains("https://example.test/o1"));

        let from_named_only = execute_select(
            &store,
            "SELECT ?o FROM NAMED <https://example.test/g1> WHERE { <https://example.test/s> <https://example.test/p> ?o }",
        )?;
        assert!(from_named_only.bindings.is_empty());

        fs::remove_file(dataset)?;
        fs::remove_file(closure)?;
        Ok(())
    }

    #[test]
    fn multiple_from_rdf_merge_standardizes_blank_nodes_apart()
    -> Result<(), Box<dyn std::error::Error>> {
        // The same dataset-scoped blank node is deliberately present in both source
        // graphs. SPARQL FROM constructs its default graph using RDF merge, which
        // must standardize blank nodes apart per input graph rather than collapse
        // them by their source dataset identity.
        let dataset = temporary_file(
            "nq",
            concat!(
                "_:shared <https://example.test/p> \"one\" <https://example.test/g1> .\n",
                "_:shared <https://example.test/p> \"two\" <https://example.test/g2> .\n",
            ),
        )?;
        let closure = temporary_file("nt", "")?;
        let store = build_store(&dataset, &closure, "https://example.test/closure")?;

        let merged = execute_select(
            &store,
            concat!(
                "SELECT ?s ?o ",
                "FROM <https://example.test/g1> ",
                "FROM <https://example.test/g2> ",
                "WHERE { ?s <https://example.test/p> ?o } ORDER BY ?o",
            ),
        )?;
        assert_eq!(merged.bindings.len(), 2);
        let subjects = merged
            .bindings
            .iter()
            .filter_map(|row| row.get("s"))
            .filter(|term| term.get("type").and_then(Value::as_str) == Some("bnode"))
            .filter_map(|term| term.get("value").and_then(Value::as_str))
            .collect::<BTreeSet<_>>();
        assert_eq!(
            subjects.len(),
            2,
            "RDF merge must standardize FROM-graph blank nodes apart"
        );

        fs::remove_file(dataset)?;
        fs::remove_file(closure)?;
        Ok(())
    }

    #[test]
    fn union_default_uses_rdf_set_union_not_bag_concatenation()
    -> Result<(), Box<dyn std::error::Error>> {
        let dataset = temporary_file(
            "nq",
            concat!(
                "<https://example.test/s> <https://example.test/p> <https://example.test/o> <https://example.test/g1> .\n",
                "<https://example.test/s> <https://example.test/p> <https://example.test/o> <https://example.test/g2> .\n",
            ),
        )?;
        let closure = temporary_file("nt", "")?;
        let store = build_store(&dataset, &closure, "https://example.test/closure")?;

        let union = execute_select(
            &store,
            "SELECT ?o WHERE { <https://example.test/s> <https://example.test/p> ?o }",
        )?;
        assert_eq!(union.bindings.len(), 1);
        let named = execute_select(
            &store,
            "SELECT ?g WHERE { GRAPH ?g { <https://example.test/s> <https://example.test/p> <https://example.test/o> } }",
        )?;
        assert_eq!(named.bindings.len(), 2);

        fs::remove_file(dataset)?;
        fs::remove_file(closure)?;
        Ok(())
    }

    #[test]
    fn phase39_3_graph_variable_values_filter_and_bag_semantics()
    -> Result<(), Box<dyn std::error::Error>> {
        let dataset = temporary_file(
            "nq",
            concat!(
                "<https://example.test/s> <https://example.test/p> <https://example.test/o1> <https://example.test/g1> .\n",
                "<https://example.test/s> <https://example.test/p> <https://example.test/o2> <https://example.test/g1> .\n",
                "<https://example.test/s> <https://example.test/p> <https://example.test/o3> <https://example.test/g2> .\n",
            ),
        )?;
        let closure = temporary_file("nt", "")?;
        let store = build_store(&dataset, &closure, "https://example.test/closure")?;

        let values = execute_select(
            &store,
            concat!(
                "SELECT ?g ?o WHERE { VALUES ?g { <https://example.test/g2> } ",
                "GRAPH ?g { <https://example.test/s> <https://example.test/p> ?o } }",
            ),
        )?;
        assert_eq!(values.bindings.len(), 1);
        assert!(values.entity_iris.contains("https://example.test/g2"));
        assert!(values.entity_iris.contains("https://example.test/o3"));

        let filtered = execute_select(
            &store,
            concat!(
                "SELECT ?g ?o WHERE { GRAPH ?g { <https://example.test/s> <https://example.test/p> ?o } ",
                "FILTER(?g = <https://example.test/g1>) }",
            ),
        )?;
        assert_eq!(filtered.bindings.len(), 2);
        assert!(filtered.bindings.iter().all(|row| {
            row.get("g")
                .and_then(|value| value.get("value"))
                .and_then(Value::as_str)
                == Some("https://example.test/g1")
        }));

        let bag = execute_select(
            &store,
            "SELECT ?g WHERE { GRAPH ?g { <https://example.test/s> <https://example.test/p> ?o } }",
        )?;
        assert_eq!(
            bag.bindings.len(),
            3,
            "GRAPH ?g must preserve SPARQL bag multiplicity"
        );
        let g1_rows = bag
            .bindings
            .iter()
            .filter(|row| {
                row.get("g")
                    .and_then(|value| value.get("value"))
                    .and_then(Value::as_str)
                    == Some("https://example.test/g1")
            })
            .count();
        assert_eq!(g1_rows, 2);

        fs::remove_file(dataset)?;
        fs::remove_file(closure)?;
        Ok(())
    }

    #[test]
    fn phase39_3_from_named_limits_graph_variable_domain() -> Result<(), Box<dyn std::error::Error>>
    {
        let dataset = temporary_file(
            "nq",
            concat!(
                "<https://example.test/s> <https://example.test/p> <https://example.test/o1> <https://example.test/g1> .\n",
                "<https://example.test/s> <https://example.test/p> <https://example.test/o2> <https://example.test/g2> .\n",
            ),
        )?;
        let closure = temporary_file("nt", "")?;
        let store = build_store(&dataset, &closure, "https://example.test/closure")?;
        let selected = execute_select(
            &store,
            concat!(
                "SELECT ?g ?o FROM NAMED <https://example.test/g2> WHERE { ",
                "GRAPH ?g { <https://example.test/s> <https://example.test/p> ?o } }",
            ),
        )?;
        assert_eq!(selected.bindings.len(), 1);
        assert!(selected.entity_iris.contains("https://example.test/g2"));
        assert!(!selected.entity_iris.contains("https://example.test/g1"));
        fs::remove_file(dataset)?;
        fs::remove_file(closure)?;
        Ok(())
    }

    #[test]
    fn phase39_3_reused_graph_variable_joins_inside_the_same_named_graph()
    -> Result<(), Box<dyn std::error::Error>> {
        let dataset = temporary_file(
            "nq",
            concat!(
                "<https://example.test/s> <https://example.test/p1> <https://example.test/a> <https://example.test/g1> .\n",
                "<https://example.test/s> <https://example.test/p2> <https://example.test/b> <https://example.test/g1> .\n",
                "<https://example.test/s> <https://example.test/p1> <https://example.test/c> <https://example.test/g2> .\n",
                "<https://example.test/s> <https://example.test/p2> <https://example.test/d> <https://example.test/g3> .\n",
            ),
        )?;
        let closure = temporary_file("nt", "")?;
        let store = build_store(&dataset, &closure, "https://example.test/closure")?;
        let joined = execute_select(
            &store,
            concat!(
                "SELECT ?g WHERE { ",
                "GRAPH ?g { <https://example.test/s> <https://example.test/p1> ?x } ",
                "GRAPH ?g { <https://example.test/s> <https://example.test/p2> ?y } }",
            ),
        )?;
        assert_eq!(joined.bindings.len(), 1);
        assert!(joined.entity_iris.contains("https://example.test/g1"));
        assert!(!joined.entity_iris.contains("https://example.test/g2"));
        assert!(!joined.entity_iris.contains("https://example.test/g3"));
        fs::remove_file(dataset)?;
        fs::remove_file(closure)?;
        Ok(())
    }

    #[test]
    fn phase39_3_authorization_and_protocol_dataset_bound_graph_variable_visibility()
    -> Result<(), Box<dyn std::error::Error>> {
        let dataset = temporary_file(
            "nq",
            concat!(
                "<https://example.test/s> <https://example.test/p> <https://example.test/o1> <https://example.test/g1> .\n",
                "<https://example.test/s> <https://example.test/p> <https://example.test/o2> <https://example.test/g2> .\n",
            ),
        )?;
        let closure = temporary_file("nt", "")?;
        let store = build_store(&dataset, &closure, "https://example.test/closure")?;
        let catalog = compile_catalog(
            Uuid::from_u128(1),
            Uuid::from_u128(2),
            0,
            &std::collections::BTreeMap::from([
                ("https://example.test/g1".to_owned(), 1),
                ("https://example.test/g2".to_owned(), 1),
            ]),
            &[
                graph_declaration("https://example.test/g1", "team-a"),
                graph_declaration("https://example.test/g2", "team-b"),
            ],
        )?;
        let query = CompiledSparqlQuery::parse(
            "SELECT ?g WHERE { GRAPH ?g { <https://example.test/s> <https://example.test/p> ?o } }",
        )?;
        let limits = QueryExecutionLimits {
            max_solution_rows: 100,
            max_graph_triples: 100,
            max_graph_blank_nodes: 100,
        };
        let authorized = resolve_dataset(
            &catalog,
            &BTreeSet::from(["team-a".to_owned()]),
            &QueryDatasetSpecification::default(),
            &ProtocolDatasetSpecification::default(),
        )?;
        let observed = super::execute_compiled_query_with_dataset(
            &store,
            &query,
            &authorized,
            &catalog,
            false,
            limits,
        )?;
        let super::ExecutedQueryResult::Solutions(rows) = observed else {
            return Err("expected SELECT solutions".into());
        };
        assert_eq!(rows.bindings.len(), 1);
        assert!(rows.entity_iris.contains("https://example.test/g1"));
        assert!(!rows.entity_iris.contains("https://example.test/g2"));

        let protocol = resolve_dataset(
            &catalog,
            &BTreeSet::from(["team-a".to_owned(), "team-b".to_owned()]),
            &QueryDatasetSpecification::default(),
            &ProtocolDatasetSpecification {
                default_graph_uris: Vec::new(),
                named_graph_uris: vec!["https://example.test/g2".to_owned()],
            },
        )?;
        let observed = super::execute_compiled_query_with_dataset(
            &store, &query, &protocol, &catalog, false, limits,
        )?;
        let super::ExecutedQueryResult::Solutions(rows) = observed else {
            return Err("expected SELECT solutions".into());
        };
        assert_eq!(rows.bindings.len(), 1);
        assert!(rows.entity_iris.contains("https://example.test/g2"));
        assert!(!rows.entity_iris.contains("https://example.test/g1"));
        fs::remove_file(dataset)?;
        fs::remove_file(closure)?;
        Ok(())
    }

    #[test]
    fn source_default_graph_is_preserved_but_excluded_from_union_default()
    -> Result<(), Box<dyn std::error::Error>> {
        let dataset = temporary_file(
            "nq",
            "<https://example.test/s> <https://example.test/p> <https://example.test/default> .\n",
        )?;
        let closure = temporary_file("nt", "")?;
        let union_store = build_store(&dataset, &closure, "https://example.test/closure")?;
        let union = execute_select(
            &union_store,
            "SELECT ?o WHERE { <https://example.test/s> <https://example.test/p> ?o }",
        )?;
        assert!(union.bindings.is_empty());

        let store = build_store_with_dataset_policy(
            &dataset,
            &closure,
            "https://example.test/closure",
            DefaultDatasetPolicy::StoredDefault,
        )?;
        let stored = execute_select(
            &store,
            "SELECT ?o WHERE { <https://example.test/s> <https://example.test/p> ?o }",
        )?;
        assert_eq!(stored.bindings.len(), 1);
        assert!(stored.entity_iris.contains("https://example.test/default"));

        fs::remove_file(dataset)?;
        fs::remove_file(closure)?;
        Ok(())
    }

    #[test]
    fn internal_closure_contributes_to_union_without_becoming_a_named_graph()
    -> Result<(), Box<dyn std::error::Error>> {
        let dataset = temporary_file("nq", "")?;
        let closure = temporary_file(
            "nt",
            "<https://example.test/s> <https://example.test/inferred> <https://example.test/o> .\n",
        )?;
        let store = build_store(&dataset, &closure, "https://example.test/closure")?;

        let entailed = execute_select(
            &store,
            "SELECT ?o WHERE { <https://example.test/s> <https://example.test/inferred> ?o }",
        )?;
        assert_eq!(entailed.bindings.len(), 1);
        let internal_graph = execute_select(
            &store,
            "SELECT ?o WHERE { GRAPH <https://example.test/closure> { <https://example.test/s> <https://example.test/inferred> ?o } }",
        )?;
        assert!(internal_graph.bindings.is_empty());

        fs::remove_file(dataset)?;
        fs::remove_file(closure)?;
        Ok(())
    }

    #[test]
    fn blank_node_graph_name_is_rejected_by_service_dataset()
    -> Result<(), Box<dyn std::error::Error>> {
        let dataset = temporary_file(
            "nq",
            "<https://example.test/s> <https://example.test/p> <https://example.test/o> _:graph .\n",
        )?;
        let closure = temporary_file("nt", "")?;
        assert!(matches!(
            build_store(&dataset, &closure, "https://example.test/closure"),
            Err(ReferenceQueryError::BlankGraphName)
        ));
        fs::remove_file(dataset)?;
        fs::remove_file(closure)?;
        Ok(())
    }

    #[test]
    fn certified_result_hash_binds_head_bag_and_order() -> Result<(), ReferenceQueryError> {
        let first = json!({
            "x": {"type": "uri", "value": "https://example.test/first"}
        });
        let second = json!({
            "x": {"type": "uri", "value": "https://example.test/second"}
        });
        let forward = vec![first.clone(), second.clone()];
        let reverse = vec![second, first];
        let head = vec!["x".to_owned()];

        assert_eq!(
            canonical_sparql_multiset_sha256(&head, &forward, false)?,
            canonical_sparql_multiset_sha256(&head, &reverse, false)?
        );
        assert_ne!(
            canonical_sparql_multiset_sha256(&head, &forward, true)?,
            canonical_sparql_multiset_sha256(&head, &reverse, true)?
        );
        assert_ne!(
            canonical_sparql_multiset_sha256(&["x".to_owned()], &[], false)?,
            canonical_sparql_multiset_sha256(&["y".to_owned()], &[], false)?
        );
        Ok(())
    }

    #[test]
    fn phase39_executes_ask_and_full_scalar_algebra() -> Result<(), Box<dyn std::error::Error>> {
        let dataset = temporary_file(
            "nq",
            concat!(
                "<https://example.test/a> <https://example.test/p> \"1\"^^<http://www.w3.org/2001/XMLSchema#integer> <https://example.test/g1> .\n",
                "<https://example.test/b> <https://example.test/p> \"2\"^^<http://www.w3.org/2001/XMLSchema#integer> <https://example.test/g1> .\n",
                "<https://example.test/b> <https://example.test/q> \"3\"^^<http://www.w3.org/2001/XMLSchema#integer> <https://example.test/g1> .\n",
            ),
        )?;
        let closure = temporary_file("nt", "")?;
        let store = build_store(&dataset, &closure, "https://example.test/closure")?;
        let limits = QueryExecutionLimits {
            max_solution_rows: 100,
            max_graph_triples: 100,
            max_graph_blank_nodes: 100,
        };
        let ask = CompiledSparqlQuery::parse(
            "ASK WHERE { ?s <https://example.test/p> ?v FILTER(?v > 1) }",
        )?;
        assert!(matches!(
            execute_compiled_query(&store, &ask, limits)?,
            super::ExecutedQueryResult::Boolean(true)
        ));
        let select = CompiledSparqlQuery::parse(concat!(
            "SELECT ?s (COUNT(?v) AS ?count) WHERE { ",
            "{ ?s <https://example.test/p> ?v OPTIONAL { ?s <https://example.test/q> ?q } } ",
            "UNION { VALUES (?s ?v) { (<https://example.test/c> 4) } } ",
            "BIND((?v + 1) AS ?next) FILTER(?next > 1) } ",
            "GROUP BY ?s HAVING(COUNT(?v) >= 1) ORDER BY ?s LIMIT 10 OFFSET 0"
        ))?;
        let result = execute_compiled_query(&store, &select, limits)?;
        let super::ExecutedQueryResult::Solutions(result) = result else {
            return Err("SELECT did not return solutions".into());
        };
        assert!(!result.bindings.is_empty());
        fs::remove_file(dataset)?;
        fs::remove_file(closure)?;
        Ok(())
    }

    #[test]
    fn phase39_scalar_algebra_preserves_minus_subquery_paths_and_bag_semantics()
    -> Result<(), Box<dyn std::error::Error>> {
        let dataset = temporary_file(
            "nq",
            concat!(
                "<https://example.test/a> <https://example.test/p> <https://example.test/b> <https://example.test/g> .\n",
                "<https://example.test/b> <https://example.test/p> <https://example.test/c> <https://example.test/g> .\n",
                "<https://example.test/c> <https://example.test/blocked> \"true\"^^<http://www.w3.org/2001/XMLSchema#boolean> <https://example.test/g> .\n",
            ),
        )?;
        let closure = temporary_file("nt", "")?;
        let store = build_store(&dataset, &closure, "https://example.test/closure")?;
        let limits = QueryExecutionLimits {
            max_solution_rows: 100,
            max_graph_triples: 100,
            max_graph_blank_nodes: 100,
        };

        let path_query = CompiledSparqlQuery::parse(concat!(
            "SELECT DISTINCT ?x WHERE { ",
            "{ SELECT ?x WHERE { <https://example.test/a> <https://example.test/p>+ ?x } } ",
            "MINUS { ?x <https://example.test/blocked> true } ",
            "} ORDER BY ?x"
        ))?;
        let ExecutedQueryResult::Solutions(path_result) =
            execute_compiled_query(&store, &path_query, limits)?
        else {
            return Err("path/subquery/MINUS query did not return solutions".into());
        };
        assert_eq!(path_result.bindings.len(), 1);
        assert_eq!(
            path_result.bindings[0].pointer("/x/value"),
            Some(&json!("https://example.test/b"))
        );

        let zero_length = CompiledSparqlQuery::parse(
            "SELECT ?x WHERE { <https://example.test/a> <https://example.test/p>* ?x } ORDER BY ?x",
        )?;
        let ExecutedQueryResult::Solutions(zero_result) =
            execute_compiled_query(&store, &zero_length, limits)?
        else {
            return Err("zero-length property path did not return solutions".into());
        };
        assert_eq!(zero_result.bindings.len(), 3);
        assert!(
            zero_result
                .bindings
                .iter()
                .any(|row| { row.pointer("/x/value") == Some(&json!("https://example.test/a")) })
        );

        let bag = CompiledSparqlQuery::parse(concat!(
            "SELECT ?x WHERE { ",
            "{ VALUES ?x { <https://example.test/b> } } UNION ",
            "{ VALUES ?x { <https://example.test/b> } } ",
            "}"
        ))?;
        let ExecutedQueryResult::Solutions(bag_result) =
            execute_compiled_query(&store, &bag, limits)?
        else {
            return Err("UNION bag query did not return solutions".into());
        };
        assert_eq!(
            bag_result.bindings.len(),
            2,
            "UNION must preserve duplicate multiplicity"
        );

        let reduced = CompiledSparqlQuery::parse(concat!(
            "SELECT REDUCED ?x WHERE { ",
            "{ VALUES ?x { <https://example.test/b> } } UNION ",
            "{ VALUES ?x { <https://example.test/b> } } ",
            "}"
        ))?;
        let ExecutedQueryResult::Solutions(reduced_result) =
            execute_compiled_query(&store, &reduced, limits)?
        else {
            return Err("REDUCED query did not return solutions".into());
        };
        assert!((1..=2).contains(&reduced_result.bindings.len()));

        fs::remove_file(dataset)?;
        fs::remove_file(closure)?;
        Ok(())
    }

    #[test]
    fn phase39_optional_keeps_unbound_variables_and_filter_errors_do_not_become_false_data()
    -> Result<(), Box<dyn std::error::Error>> {
        let dataset = temporary_file(
            "nq",
            concat!(
                "<https://example.test/a> <https://example.test/p> \"1\"^^<http://www.w3.org/2001/XMLSchema#integer> <https://example.test/g> .\n",
                "<https://example.test/b> <https://example.test/p> \"2\"^^<http://www.w3.org/2001/XMLSchema#integer> <https://example.test/g> .\n",
                "<https://example.test/b> <https://example.test/q> \"3\"^^<http://www.w3.org/2001/XMLSchema#integer> <https://example.test/g> .\n",
            ),
        )?;
        let closure = temporary_file("nt", "")?;
        let store = build_store(&dataset, &closure, "https://example.test/closure")?;
        let limits = QueryExecutionLimits {
            max_solution_rows: 100,
            max_graph_triples: 100,
            max_graph_blank_nodes: 100,
        };
        let optional = CompiledSparqlQuery::parse(concat!(
            "SELECT ?s ?q WHERE { ?s <https://example.test/p> ?v ",
            "OPTIONAL { ?s <https://example.test/q> ?q } } ORDER BY ?s"
        ))?;
        let ExecutedQueryResult::Solutions(optional_result) =
            execute_compiled_query(&store, &optional, limits)?
        else {
            return Err("OPTIONAL query did not return solutions".into());
        };
        assert_eq!(optional_result.bindings.len(), 2);
        assert!(optional_result.bindings[0].get("q").is_none());
        assert!(optional_result.bindings[1].get("q").is_some());

        let error_filter = CompiledSparqlQuery::parse(concat!(
            "SELECT ?s WHERE { ?s <https://example.test/p> ?v ",
            "FILTER((?v / 0) > 1) }"
        ))?;
        let ExecutedQueryResult::Solutions(error_result) =
            execute_compiled_query(&store, &error_filter, limits)?
        else {
            return Err("FILTER error query did not return solutions".into());
        };
        assert!(error_result.bindings.is_empty());

        fs::remove_file(dataset)?;
        fs::remove_file(closure)?;
        Ok(())
    }

    #[test]
    fn phase39_construct_graph_hash_is_blank_node_isomorphism_stable()
    -> Result<(), Box<dyn std::error::Error>> {
        let limits = QueryExecutionLimits {
            max_solution_rows: 10,
            max_graph_triples: 10,
            max_graph_blank_nodes: 10,
        };
        let first = vec![
            "_:left <https://example.test/p> <https://example.test/o> .\n".to_owned(),
            "<https://example.test/s> <https://example.test/q> _:left .\n".to_owned(),
        ];
        let second = vec![
            "_:right <https://example.test/p> <https://example.test/o> .\n".to_owned(),
            "<https://example.test/s> <https://example.test/q> _:right .\n".to_owned(),
        ];
        assert_eq!(
            canonical_query_payload_sha256(
                QueryForm::Construct,
                &[],
                &[],
                None,
                &first,
                false,
                limits,
            )?,
            canonical_query_payload_sha256(
                QueryForm::Construct,
                &[],
                &[],
                None,
                &second,
                false,
                limits,
            )?
        );
        Ok(())
    }

    #[test]
    fn phase39_construct_and_describe_return_graphs() -> Result<(), Box<dyn std::error::Error>> {
        let dataset = temporary_file(
            "nq",
            "<https://example.test/s> <https://example.test/p> <https://example.test/o> <https://example.test/g> .\n",
        )?;
        let closure = temporary_file("nt", "")?;
        let store = build_store(&dataset, &closure, "https://example.test/closure")?;
        let limits = QueryExecutionLimits {
            max_solution_rows: 10,
            max_graph_triples: 10,
            max_graph_blank_nodes: 10,
        };
        for query in [
            "CONSTRUCT { ?s <https://example.test/copy> ?o } WHERE { ?s <https://example.test/p> ?o }",
            "DESCRIBE <https://example.test/s>",
        ] {
            let compiled = CompiledSparqlQuery::parse(query)?;
            let result = execute_compiled_query(&store, &compiled, limits)?;
            let super::ExecutedQueryResult::Graph { graph, .. } = result else {
                return Err("graph query did not return an RDF graph".into());
            };
            assert!(!graph.ntriples.is_empty());
        }
        fs::remove_file(dataset)?;
        fs::remove_file(closure)?;
        Ok(())
    }

    #[test]
    fn phase40_13_5_group_concat_is_a_simple_literal() -> Result<(), Box<dyn std::error::Error>> {
        let dataset = temporary_file("nq", "")?;
        let closure = temporary_file("nt", "")?;
        let store = build_store(&dataset, &closure, "https://example.test/closure")?;
        let query = CompiledSparqlQuery::parse(concat!(
            "ASK WHERE { { SELECT (GROUP_CONCAT(?value; SEPARATOR=\"|\") AS ?joined) ",
            "WHERE { VALUES ?value { \"first\"@en \"second\"@en } } } ",
            "FILTER(DATATYPE(?joined) = <http://www.w3.org/2001/XMLSchema#string>) }"
        ))?;
        assert!(matches!(
            execute_compiled_query(
                &store,
                &query,
                QueryExecutionLimits {
                    max_solution_rows: 10,
                    max_graph_triples: 10,
                    max_graph_blank_nodes: 10,
                },
            )?,
            ExecutedQueryResult::Boolean(true)
        ));
        fs::remove_file(dataset)?;
        fs::remove_file(closure)?;
        Ok(())
    }

    #[test]
    fn phase40_13_5_graph_scope_reaches_subqueries_and_minus()
    -> Result<(), Box<dyn std::error::Error>> {
        let dataset = temporary_file(
            "nq",
            concat!(
                "<https://example.test/a> <https://example.test/p> <https://example.test/o> <https://example.test/g1> .\n",
                "<https://example.test/b> <https://example.test/p> <https://example.test/x> <https://example.test/g2> .\n",
                "<https://example.test/c> <https://example.test/p> <https://example.test/y> <https://example.test/g2> .\n",
            ),
        )?;
        let closure = temporary_file("nt", "")?;
        let store = build_store(&dataset, &closure, "https://example.test/closure")?;

        let aggregate = execute_select(
            &store,
            concat!(
                "SELECT ?g ?count WHERE { GRAPH ?g { ",
                "{ SELECT (COUNT(*) AS ?count) WHERE { ?s <https://example.test/p> ?o } } ",
                "} } ORDER BY ?g"
            ),
        )?;
        assert_eq!(aggregate.bindings.len(), 2);
        assert_eq!(
            aggregate.bindings[0].pointer("/count/value"),
            Some(&json!("1"))
        );
        assert_eq!(
            aggregate.bindings[1].pointer("/count/value"),
            Some(&json!("2"))
        );

        let minus = execute_select(
            &store,
            concat!(
                "SELECT ?s WHERE { GRAPH <https://example.test/g1> { ",
                "?s <https://example.test/p> ?o MINUS { ?s <https://example.test/q> ?blocked } ",
                "} }"
            ),
        )?;
        assert_eq!(minus.bindings.len(), 1);
        assert_eq!(
            minus.bindings[0].pointer("/s/value"),
            Some(&json!("https://example.test/a"))
        );

        fs::remove_file(dataset)?;
        fs::remove_file(closure)?;
        Ok(())
    }

    #[test]
    fn phase40_13_5_bnode_string_is_solution_scoped() -> Result<(), Box<dyn std::error::Error>> {
        let dataset = temporary_file("nq", "")?;
        let closure = temporary_file("nt", "")?;
        let store = build_store(&dataset, &closure, "https://example.test/closure")?;
        let result = execute_select(
            &store,
            concat!(
                "SELECT ?x (BNODE(?x) AS ?first) (BNODE(?x) AS ?second) WHERE { ",
                "VALUES ?x { \"a\" \"b\" } } ORDER BY ?x"
            ),
        )?;
        assert_eq!(result.bindings.len(), 2);
        let first_a = result.bindings[0]
            .pointer("/first/value")
            .ok_or("first BNODE result is missing")?;
        let second_a = result.bindings[0]
            .pointer("/second/value")
            .ok_or("second BNODE result is missing")?;
        let first_b = result.bindings[1]
            .pointer("/first/value")
            .ok_or("next BNODE result is missing")?;
        assert_eq!(
            first_a, second_a,
            "the same label and solution must be stable"
        );
        assert_ne!(
            first_a, first_b,
            "different solutions need distinct blank nodes"
        );

        fs::remove_file(dataset)?;
        fs::remove_file(closure)?;
        Ok(())
    }

    #[test]
    fn phase40_13_5_zero_length_path_includes_an_absent_constant_node()
    -> Result<(), Box<dyn std::error::Error>> {
        let dataset = temporary_file("nq", "")?;
        let closure = temporary_file("nt", "")?;
        let store = build_store(&dataset, &closure, "https://example.test/closure")?;
        for operator in ["*", "?"] {
            let query = format!(
                "SELECT ?s WHERE {{ ?s <https://example.test/p>{operator} <https://example.test/o> }}"
            );
            let result = execute_select(&store, &query)?;
            assert_eq!(result.bindings.len(), 1);
            assert_eq!(
                result.bindings[0].pointer("/s/value"),
                Some(&json!("https://example.test/o"))
            );
        }

        fs::remove_file(dataset)?;
        fs::remove_file(closure)?;
        Ok(())
    }

    #[test]
    fn phase40_13_5_store_preserves_numeric_lexical_identity_and_derived_datatype()
    -> Result<(), Box<dyn std::error::Error>> {
        let dataset = temporary_file(
            "nq",
            concat!(
                "<https://example.test/s> <https://example.test/double> \"1.0E6\"^^<http://www.w3.org/2001/XMLSchema#double> <https://example.test/g> .\n",
                "<https://example.test/s> <https://example.test/negative> \"-3\"^^<http://www.w3.org/2001/XMLSchema#negativeInteger> <https://example.test/g> .\n",
            ),
        )?;
        let closure = temporary_file("nt", "")?;
        let store = build_store(&dataset, &closure, "https://example.test/closure")?;
        let result = execute_select(
            &store,
            concat!(
                "SELECT ?double ?negative WHERE { ",
                "<https://example.test/s> <https://example.test/double> ?double ; ",
                "<https://example.test/negative> ?negative }"
            ),
        )?;
        assert_eq!(result.bindings.len(), 1);
        assert_eq!(
            result.bindings[0].pointer("/double/value"),
            Some(&json!("1.0E6"))
        );
        assert_eq!(
            result.bindings[0].pointer("/negative/datatype"),
            Some(&json!("http://www.w3.org/2001/XMLSchema#negativeInteger"))
        );

        fs::remove_file(dataset)?;
        fs::remove_file(closure)?;
        Ok(())
    }

    #[test]
    fn result_equivalence_uses_rdf_values_language_case_and_blank_node_isomorphism()
    -> Result<(), Box<dyn std::error::Error>> {
        let literal =
            |value: &str, datatype: &str, language: Option<&str>| CanonicalTerm::Literal {
                value: value.to_owned(),
                datatype: datatype.to_owned(),
                language: language.map(ToOwned::to_owned),
            };
        let observed = vec![BTreeMap::from([
            (
                "number".to_owned(),
                literal("+001", "http://www.w3.org/2001/XMLSchema#integer", None),
            ),
            (
                "label".to_owned(),
                literal(
                    "value",
                    "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString",
                    Some("EN-us"),
                ),
            ),
            (
                "node".to_owned(),
                CanonicalTerm::BlankNode {
                    value: "observed".to_owned(),
                },
            ),
        ])];
        let expected = vec![BTreeMap::from([
            (
                "number".to_owned(),
                literal("1", "http://www.w3.org/2001/XMLSchema#integer", None),
            ),
            (
                "label".to_owned(),
                literal(
                    "value",
                    "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString",
                    Some("en-US"),
                ),
            ),
            (
                "node".to_owned(),
                CanonicalTerm::BlankNode {
                    value: "expected".to_owned(),
                },
            ),
        ])];
        let limits = QueryExecutionLimits {
            max_solution_rows: 10,
            max_graph_triples: 10,
            max_graph_blank_nodes: 10,
        };
        assert!(solution_results_equivalent(
            &["number".to_owned(), "label".to_owned(), "node".to_owned()],
            &observed,
            &["node".to_owned(), "number".to_owned(), "label".to_owned()],
            &expected,
            false,
            limits,
        )?);
        Ok(())
    }
}
