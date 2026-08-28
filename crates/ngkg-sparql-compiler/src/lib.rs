//! Standards-first SPARQL compiler shared by offline certification and online serving.
//!
//! The compiler parses SPARQL once into `spargebra`'s SPARQL 1.1 algebra and derives
//! dataset, routing, deterministic-execution, and safe-distribution contracts only
//! from that typed tree. Storage kernels never reinterpret query text.

use std::collections::BTreeSet;

use ngkg_dataset::QueryDatasetSpecification;
use ngkg_query_planner::{
    AlgebraExecutionLane, AlgebraPlanError, DistributedAlgebraLimits,
    DistributedAlgebraOperator, DistributedAlgebraPlan, DistributedAlgebraStage,
    DistributedPathAutomaton, DistributedPropertyPathLimits, DistributedPropertyPathPlan,
    PathDirection, PathTransition, PathTransitionKind, PropertyPathPlanError,
    validate_distributed_algebra_plan, validate_distributed_property_path_plan,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use spargebra::{Query, SparqlParser};
use spargebra::{
    algebra::{
        AggregateExpression, Expression, Function, GraphPattern, OrderExpression,
        PropertyPathExpression,
    },
    term::{NamedNodePattern, TermPattern, TriplePattern},
};
use thiserror::Error;

/// Version of NGKG's canonical algebra-certificate contract.
pub const SPARQL_ALGEBRA_FORMAT_VERSION: u32 = 1;
/// RDF type predicate used by capability routing.
pub const RDF_TYPE_IRI: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";

/// Parsed SPARQL query form.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum QueryForm {
    /// SPARQL SELECT.
    #[serde(rename = "SELECT")]
    Select,
    /// SPARQL ASK.
    #[serde(rename = "ASK")]
    Ask,
    /// SPARQL CONSTRUCT.
    #[serde(rename = "CONSTRUCT")]
    Construct,
    /// SPARQL DESCRIBE.
    #[serde(rename = "DESCRIBE")]
    Describe,
}

impl QueryForm {
    /// Stable protocol label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Select => "SELECT",
            Self::Ask => "ASK",
            Self::Construct => "CONSTRUCT",
            Self::Describe => "DESCRIBE",
        }
    }
}

/// Typed routing evidence derived from SPARQL algebra.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RouteAnalysis {
    /// Constant semantic IRIs found in predicates, rdf:type objects, and paths.
    pub semantic_iris: BTreeSet<String>,
    /// Constant graph IRIs explicitly addressed by GRAPH.
    pub declared_graph_iris: BTreeSet<String>,
    /// Whether GRAPH uses a variable and therefore ranges across named graphs.
    pub has_graph_variable: bool,
    /// Whether any graph pattern is evaluated against the active default graph.
    pub has_default_graph_pattern: bool,
    /// Whether the algebra contains a property path.
    pub has_property_path: bool,
}

/// Execution properties derived from typed SPARQL algebra after standards parsing.
///
/// These properties select runtime, caching, retry, and certification behavior. They are
/// deliberately not parser errors: SPARQL features remain legal even when an immutable NGKG
/// certificate or a particular execution lane cannot support them.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ExecutionAnalysis {
    /// The query contains one or more SPARQL 1.1 federated SERVICE operators.
    pub has_remote_service: bool,
    /// Standards function names whose results or blank-node identity vary by execution.
    pub volatile_functions: BTreeSet<String>,
}

impl ExecutionAnalysis {
    /// Immutable result certificates require execution-independent inputs and values.
    #[must_use]
    pub fn is_certifiable(&self) -> bool {
        !self.has_remote_service && self.volatile_functions.is_empty()
    }

    /// Snapshot result caches share the same safety boundary as immutable certificates.
    #[must_use]
    pub fn is_snapshot_cacheable(&self) -> bool {
        self.is_certifiable()
    }
}

/// One typed constant-GRAPH fragment eligible for exact distributed certification.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DistributedGraphFragment {
    /// Named graph selected by the algebra leaf.
    pub graph_iri: String,
    /// Standards-rendered standalone SELECT query generated from the typed leaf.
    pub query_text: String,
}

/// Immutable parsed query plus all semantic metadata used by NGKG planning.
#[derive(Clone, Debug)]
pub struct CompiledSparqlQuery {
    query: Query,
    form: QueryForm,
    dataset: QueryDatasetSpecification,
    route: RouteAnalysis,
    execution: ExecutionAnalysis,
    solution_variable_order: Vec<String>,
    canonical_sse: String,
    canonical_sse_sha256: String,
}

/// Fail-closed typed SPARQL compilation error.
#[derive(Debug, Error)]
pub enum SparqlCompileError {
    /// SPARQL grammar violation.
    #[error("SPARQL parsing failed: {0}")]
    Syntax(String),
}

/// A legal SPARQL query that is not eligible for an immutable snapshot certificate.
#[derive(Debug, Error, Eq, PartialEq)]
pub enum SparqlCertificationError {
    /// Federated SERVICE results are controlled by remote state outside the snapshot.
    #[error("remote SPARQL SERVICE execution is not allowed in certified mode")]
    RemoteService,
    /// A volatile function cannot participate in an immutable result certificate.
    #[error("nondeterministic SPARQL function is not allowed in certified mode: {0}")]
    NondeterministicFunction(String),
}

impl CompiledSparqlQuery {
    /// Parse once and derive dataset, routing, and execution-policy metadata from the typed
    /// SPARQL 1.1 algebra. Certification eligibility is checked separately.
    pub fn parse(query_text: &str) -> Result<Self, SparqlCompileError> {
        Self::parse_with_parser(SparqlParser::new(), query_text)
    }

    /// Parse with the retrieval IRI used to resolve relative IRIs when the query
    /// does not declare its own `BASE`.
    pub fn parse_with_base_iri(
        query_text: &str,
        base_iri: &str,
    ) -> Result<Self, SparqlCompileError> {
        let parser = SparqlParser::new()
            .with_base_iri(base_iri)
            .map_err(|error| SparqlCompileError::Syntax(error.to_string()))?;
        Self::parse_with_parser(parser, query_text)
    }

    fn parse_with_parser(
        parser: SparqlParser,
        query_text: &str,
    ) -> Result<Self, SparqlCompileError> {
        let query = match parser.clone().parse_query(query_text) {
            Ok(query) => query,
            Err(original_error) => {
                let normalized = normalize_token_separators(query_text);
                if normalized == query_text {
                    return Err(SparqlCompileError::Syntax(original_error.to_string()));
                }
                parser
                    .parse_query(&normalized)
                    .map_err(|_| SparqlCompileError::Syntax(original_error.to_string()))?
            }
        };
        let form = query_form(&query);
        let dataset = query_dataset(&query);
        let mut route = RouteAnalysis::default();
        let mut execution = ExecutionAnalysis::default();
        inspect_graph_pattern(query_pattern(&query), false, &mut route, &mut execution)?;
        let canonical_sse = query.to_sse();
        let canonical_sse_sha256 = hex::encode(Sha256::digest(canonical_sse.as_bytes()));
        Ok(Self {
            query,
            form,
            dataset,
            route,
            execution,
            solution_variable_order: textual_variable_order(query_text),
            canonical_sse,
            canonical_sse_sha256,
        })
    }

    /// Borrow the parsed standards algebra.
    #[must_use]
    pub const fn query(&self) -> &Query {
        &self.query
    }

    /// Clone the parsed query for evaluators that take ownership.
    #[must_use]
    pub fn query_clone(&self) -> Query {
        self.query.clone()
    }

    /// Query form.
    #[must_use]
    pub const fn form(&self) -> QueryForm {
        self.form
    }

    /// Parsed query-level FROM/FROM NAMED specification.
    #[must_use]
    pub const fn dataset_specification(&self) -> &QueryDatasetSpecification {
        &self.dataset
    }

    /// Typed routing evidence.
    #[must_use]
    pub const fn route_analysis(&self) -> &RouteAnalysis {
        &self.route
    }

    /// Typed execution, cache, retry, and certification policy evidence.
    #[must_use]
    pub const fn execution_analysis(&self) -> &ExecutionAnalysis {
        &self.execution
    }

    /// Variables in their first lexical occurrence order in the submitted query.
    ///
    /// The standards algebra intentionally treats projected variables as a set in
    /// several places. The presentation layer still needs the source order for
    /// `SELECT *` result headers and CSV/TSV serialization, so this metadata is
    /// retained alongside (but never used to change) the parsed algebra.
    #[must_use]
    pub fn solution_variable_order(&self) -> &[String] {
        &self.solution_variable_order
    }

    /// Reject only the immutable certification operation, never standards parsing.
    pub fn require_certifiable(&self) -> Result<(), SparqlCertificationError> {
        if self.execution.has_remote_service {
            return Err(SparqlCertificationError::RemoteService);
        }
        if let Some(function) = self.execution.volatile_functions.iter().next() {
            return Err(SparqlCertificationError::NondeterministicFunction(
                function.clone(),
            ));
        }
        Ok(())
    }

    /// Canonical SPARQL S-expression used as a versioned semantic certificate.
    #[must_use]
    pub fn canonical_sse(&self) -> &str {
        &self.canonical_sse
    }

    /// SHA-256 of the canonical SPARQL S-expression.
    #[must_use]
    pub fn canonical_sse_sha256(&self) -> &str {
        &self.canonical_sse_sha256
    }

    /// Whether SPARQL solution sequence order is semantically constrained by a
    /// top-level ORDER BY. Graph and boolean query forms are never sequence-ordered.
    #[must_use]
    pub fn solution_order_is_significant(&self) -> bool {
        self.form == QueryForm::Select && top_level_has_order_by(query_pattern(&self.query))
    }

    /// Return independent constant-GRAPH leaves only when the SELECT algebra is a
    /// pure inner-join tree. Any other algebra remains on the exact local path.
    #[must_use]
    pub fn distributed_graph_fragments(&self) -> Option<Vec<DistributedGraphFragment>> {
        let Query::Select { pattern, .. } = &self.query else {
            return None;
        };
        let root = strip_projection(pattern);
        let mut leaves = Vec::new();
        if !collect_constant_graph_join_leaves(root, &mut leaves) || leaves.len() < 2 {
            return None;
        }
        let mut fragments = Vec::with_capacity(leaves.len());
        for (graph_iri, graph_pattern) in leaves {
            let query = Query::Select {
                dataset: None,
                pattern: graph_pattern,
                base_iri: None,
            };
            fragments.push(DistributedGraphFragment {
                graph_iri,
                query_text: query.to_string(),
            });
        }
        Some(fragments)
    }

    /// Compile the complete typed algebra into a bounded post-order distributed DAG.
    ///
    /// The plan is intentionally conservative: only operators with exact binding-only kernels
    /// use the native lane. Expression evaluation, RDF-term ordering, aggregation, subquery scope,
    /// graph construction and property paths remain on the pinned scalar-oracle lane. BGP stages
    /// use exact HermiT partitions when the online OWL Direct route is active.
    pub fn distributed_algebra_plan(
        &self,
        limits: DistributedAlgebraLimits,
    ) -> Result<DistributedAlgebraPlan, AlgebraPlanError> {
        let limits = limits.validate()?;
        let mut builder = AlgebraPlanBuilder {
            limits,
            stages: Vec::new(),
        };
        let pattern_root = builder.pattern(query_pattern(&self.query), true)?;
        let root_stage_id = match self.form {
            QueryForm::Select => pattern_root,
            QueryForm::Ask => builder.finalize(
                DistributedAlgebraOperator::AskFinalize,
                pattern_root,
                &self.canonical_sse,
            )?,
            QueryForm::Construct => builder.finalize(
                DistributedAlgebraOperator::ConstructFinalize,
                pattern_root,
                &self.canonical_sse,
            )?,
            QueryForm::Describe => builder.finalize(
                DistributedAlgebraOperator::DescribeFinalize,
                pattern_root,
                &self.canonical_sse,
            )?,
        };
        let plan = DistributedAlgebraPlan {
            format_version: 1,
            query_algebra_sha256: self.canonical_sse_sha256.clone(),
            root_stage_id,
            stages: builder.stages,
            require_complete_partition_set: true,
            require_scalar_equivalence: true,
        };
        validate_distributed_algebra_plan(&plan)?;
        Ok(plan)
    }

    /// Compile every typed SPARQL property-path occurrence into an exact bounded NFA plan.
    pub fn distributed_property_path_plans(
        &self,
        limits: DistributedPropertyPathLimits,
    ) -> Result<Vec<DistributedPropertyPathPlan>, PropertyPathPlanError> {
        let limits = limits.validate()?;
        let mut plans = Vec::new();
        collect_distributed_property_paths(
            query_pattern(&self.query),
            "active-default",
            limits,
            &mut plans,
        )?;
        Ok(plans)
    }
}

struct AlgebraPlanBuilder {
    limits: DistributedAlgebraLimits,
    stages: Vec<DistributedAlgebraStage>,
}

impl AlgebraPlanBuilder {
    fn pattern(
        &mut self,
        pattern: &GraphPattern,
        outer_project: bool,
    ) -> Result<String, AlgebraPlanError> {
        let (operator, lane, inputs) = match pattern {
            GraphPattern::Bgp { .. } => (
                DistributedAlgebraOperator::Bgp,
                AlgebraExecutionLane::ExactReasonerPartitioned,
                Vec::new(),
            ),
            GraphPattern::Path { .. } => (
                DistributedAlgebraOperator::Path,
                AlgebraExecutionLane::ScalarOraclePartitioned,
                Vec::new(),
            ),
            GraphPattern::Join { left, right } => (
                DistributedAlgebraOperator::Join,
                AlgebraExecutionLane::NativePartitioned,
                vec![self.pattern(left, false)?, self.pattern(right, false)?],
            ),
            GraphPattern::Lateral { left, right } => (
                DistributedAlgebraOperator::Lateral,
                AlgebraExecutionLane::ScalarOraclePartitioned,
                vec![self.pattern(left, false)?, self.pattern(right, false)?],
            ),
            GraphPattern::LeftJoin {
                left,
                right,
                expression,
            } => {
                let mut inputs = vec![self.pattern(left, false)?, self.pattern(right, false)?];
                if let Some(expression) = expression {
                    self.expression_dependencies(expression, &mut inputs)?;
                }
                (
                    DistributedAlgebraOperator::LeftJoin,
                    AlgebraExecutionLane::ScalarOraclePartitioned,
                    inputs,
                )
            }
            GraphPattern::Filter { expr, inner } => {
                let mut inputs = vec![self.pattern(inner, outer_project)?];
                self.expression_dependencies(expr, &mut inputs)?;
                (
                    DistributedAlgebraOperator::Filter,
                    AlgebraExecutionLane::ScalarOraclePartitioned,
                    inputs,
                )
            }
            GraphPattern::Union { left, right } => (
                DistributedAlgebraOperator::Union,
                AlgebraExecutionLane::NativePartitioned,
                vec![self.pattern(left, false)?, self.pattern(right, false)?],
            ),
            GraphPattern::Graph { inner, .. } => (
                DistributedAlgebraOperator::Graph,
                AlgebraExecutionLane::ScalarOraclePartitioned,
                vec![self.pattern(inner, outer_project)?],
            ),
            GraphPattern::Extend {
                inner,
                expression,
                ..
            } => {
                let mut inputs = vec![self.pattern(inner, outer_project)?];
                self.expression_dependencies(expression, &mut inputs)?;
                (
                    DistributedAlgebraOperator::Extend,
                    AlgebraExecutionLane::ScalarOraclePartitioned,
                    inputs,
                )
            }
            GraphPattern::Minus { left, right } => (
                DistributedAlgebraOperator::Minus,
                AlgebraExecutionLane::NativePartitioned,
                vec![self.pattern(left, false)?, self.pattern(right, false)?],
            ),
            GraphPattern::Values { .. } => (
                DistributedAlgebraOperator::Values,
                AlgebraExecutionLane::NativePartitioned,
                Vec::new(),
            ),
            GraphPattern::OrderBy { inner, expression } => {
                let mut inputs = vec![self.pattern(inner, outer_project)?];
                for order in expression {
                    match order {
                        OrderExpression::Asc(expression) | OrderExpression::Desc(expression) => {
                            self.expression_dependencies(expression, &mut inputs)?;
                        }
                    }
                }
                (
                    DistributedAlgebraOperator::Order,
                    AlgebraExecutionLane::ScalarOraclePartitioned,
                    inputs,
                )
            }
            GraphPattern::Project { inner, .. } if outer_project => (
                DistributedAlgebraOperator::Project,
                AlgebraExecutionLane::NativePartitioned,
                vec![self.pattern(inner, false)?],
            ),
            GraphPattern::Project { inner, .. } => (
                DistributedAlgebraOperator::Subquery,
                AlgebraExecutionLane::ScalarOraclePartitioned,
                vec![self.pattern(inner, false)?],
            ),
            GraphPattern::Distinct { inner } => (
                DistributedAlgebraOperator::Distinct,
                AlgebraExecutionLane::NativePartitioned,
                vec![self.pattern(inner, outer_project)?],
            ),
            GraphPattern::Reduced { inner } => (
                DistributedAlgebraOperator::Reduced,
                AlgebraExecutionLane::NativePartitioned,
                vec![self.pattern(inner, outer_project)?],
            ),
            GraphPattern::Slice { inner, .. } => (
                DistributedAlgebraOperator::Slice,
                AlgebraExecutionLane::NativePartitioned,
                vec![self.pattern(inner, outer_project)?],
            ),
            GraphPattern::Group {
                inner,
                aggregates,
                ..
            } => {
                let mut inputs = vec![self.pattern(inner, outer_project)?];
                for (_, aggregate) in aggregates {
                    if let AggregateExpression::FunctionCall { expr, .. } = aggregate {
                        self.expression_dependencies(expr, &mut inputs)?;
                    }
                }
                (
                    DistributedAlgebraOperator::Group,
                    AlgebraExecutionLane::ScalarOraclePartitioned,
                    inputs,
                )
            }
            GraphPattern::Service { inner, .. } => (
                DistributedAlgebraOperator::Service,
                AlgebraExecutionLane::ScalarOraclePartitioned,
                vec![self.pattern(inner, false)?],
            ),
        };
        self.push(operator, lane, inputs, wrapped_pattern_sse(pattern))
    }

    fn expression_dependencies(
        &mut self,
        expression: &Expression,
        inputs: &mut Vec<String>,
    ) -> Result<(), AlgebraPlanError> {
        match expression {
            Expression::NamedNode(_)
            | Expression::Literal(_)
            | Expression::Variable(_)
            | Expression::Bound(_) => {}
            Expression::Or(left, right)
            | Expression::And(left, right)
            | Expression::Equal(left, right)
            | Expression::SameTerm(left, right)
            | Expression::Greater(left, right)
            | Expression::GreaterOrEqual(left, right)
            | Expression::Less(left, right)
            | Expression::LessOrEqual(left, right)
            | Expression::Add(left, right)
            | Expression::Subtract(left, right)
            | Expression::Multiply(left, right)
            | Expression::Divide(left, right) => {
                self.expression_dependencies(left, inputs)?;
                self.expression_dependencies(right, inputs)?;
            }
            Expression::In(left, values) => {
                self.expression_dependencies(left, inputs)?;
                for value in values {
                    self.expression_dependencies(value, inputs)?;
                }
            }
            Expression::UnaryPlus(inner)
            | Expression::UnaryMinus(inner)
            | Expression::Not(inner) => self.expression_dependencies(inner, inputs)?,
            Expression::Exists(pattern) => inputs.push(self.pattern(pattern, false)?),
            Expression::If(condition, yes, no) => {
                self.expression_dependencies(condition, inputs)?;
                self.expression_dependencies(yes, inputs)?;
                self.expression_dependencies(no, inputs)?;
            }
            Expression::Coalesce(values) | Expression::FunctionCall(_, values) => {
                for value in values {
                    self.expression_dependencies(value, inputs)?;
                }
            }
        }
        Ok(())
    }

    fn finalize(
        &mut self,
        operator: DistributedAlgebraOperator,
        input: String,
        complete_sse: &str,
    ) -> Result<String, AlgebraPlanError> {
        self.push(
            operator,
            AlgebraExecutionLane::ScalarOraclePartitioned,
            vec![input],
            complete_sse.to_owned(),
        )
    }

    fn push(
        &mut self,
        operator: DistributedAlgebraOperator,
        lane: AlgebraExecutionLane,
        inputs: Vec<String>,
        algebra_sse: String,
    ) -> Result<String, AlgebraPlanError> {
        let ordinal = self.stages.len();
        let stage_id = format!("algebra-stage-{ordinal:05}");
        let algebra_sha256 = hex::encode(Sha256::digest(algebra_sse.as_bytes()));
        self.stages.push(DistributedAlgebraStage {
            stage_id: stage_id.clone(),
            operator,
            inputs,
            lane,
            algebra_sha256,
            partition_count: self.limits.partition_count,
            max_input_rows: self.limits.max_input_rows,
            max_output_rows: self.limits.max_output_rows,
            max_exchange_bytes: self.limits.max_exchange_bytes,
            max_spill_bytes: self.limits.max_spill_bytes,
        });
        Ok(stage_id)
    }
}

fn wrapped_pattern_sse(pattern: &GraphPattern) -> String {
    Query::Select {
        dataset: None,
        pattern: pattern.clone(),
        base_iri: None,
    }
    .to_sse()
}

struct PathAutomatonBuilder {
    state_count: u32,
    transitions: Vec<PathTransition>,
}

impl PathAutomatonBuilder {
    fn new_state(&mut self) -> Result<u32, PropertyPathPlanError> {
        let state = self.state_count;
        self.state_count = self
            .state_count
            .checked_add(1)
            .ok_or(PropertyPathPlanError::InvalidAutomaton)?;
        Ok(state)
    }

    fn epsilon(&mut self, from_state: u32, to_state: u32) {
        self.transitions.push(PathTransition {
            from_state,
            to_state,
            transition: PathTransitionKind::Epsilon,
        });
    }

    fn fragment(
        &mut self,
        path: &PropertyPathExpression,
        reverse: bool,
    ) -> Result<(u32, u32), PropertyPathPlanError> {
        match path {
            PropertyPathExpression::NamedNode(node) => {
                let start = self.new_state()?;
                let end = self.new_state()?;
                self.transitions.push(PathTransition {
                    from_state: start,
                    to_state: end,
                    transition: PathTransitionKind::Predicate {
                        direction: if reverse {
                            PathDirection::Reverse
                        } else {
                            PathDirection::Forward
                        },
                        predicate_iri: node.as_str().to_owned(),
                    },
                });
                Ok((start, end))
            }
            PropertyPathExpression::Reverse(inner) => self.fragment(inner, !reverse),
            PropertyPathExpression::Sequence(left, right) => {
                let (first, second) = if reverse { (right, left) } else { (left, right) };
                let (start, middle_left) = self.fragment(first, reverse)?;
                let (middle_right, end) = self.fragment(second, reverse)?;
                self.epsilon(middle_left, middle_right);
                Ok((start, end))
            }
            PropertyPathExpression::Alternative(left, right) => {
                let start = self.new_state()?;
                let end = self.new_state()?;
                let (left_start, left_end) = self.fragment(left, reverse)?;
                let (right_start, right_end) = self.fragment(right, reverse)?;
                self.epsilon(start, left_start);
                self.epsilon(start, right_start);
                self.epsilon(left_end, end);
                self.epsilon(right_end, end);
                Ok((start, end))
            }
            PropertyPathExpression::ZeroOrMore(inner) => {
                let start = self.new_state()?;
                let end = self.new_state()?;
                let (inner_start, inner_end) = self.fragment(inner, reverse)?;
                self.epsilon(start, end);
                self.epsilon(start, inner_start);
                self.epsilon(inner_end, inner_start);
                self.epsilon(inner_end, end);
                Ok((start, end))
            }
            PropertyPathExpression::OneOrMore(inner) => {
                let start = self.new_state()?;
                let end = self.new_state()?;
                let (inner_start, inner_end) = self.fragment(inner, reverse)?;
                self.epsilon(start, inner_start);
                self.epsilon(inner_end, inner_start);
                self.epsilon(inner_end, end);
                Ok((start, end))
            }
            PropertyPathExpression::ZeroOrOne(inner) => {
                let start = self.new_state()?;
                let end = self.new_state()?;
                let (inner_start, inner_end) = self.fragment(inner, reverse)?;
                self.epsilon(start, end);
                self.epsilon(start, inner_start);
                self.epsilon(inner_end, end);
                Ok((start, end))
            }
            PropertyPathExpression::NegatedPropertySet(nodes) => {
                let start = self.new_state()?;
                let end = self.new_state()?;
                let mut excluded_predicate_iris = nodes
                    .iter()
                    .map(|node| node.as_str().to_owned())
                    .collect::<Vec<_>>();
                excluded_predicate_iris.sort();
                excluded_predicate_iris.dedup();
                self.transitions.push(PathTransition {
                    from_state: start,
                    to_state: end,
                    transition: PathTransitionKind::NegatedPropertySet {
                        direction: if reverse {
                            PathDirection::Reverse
                        } else {
                            PathDirection::Forward
                        },
                        excluded_predicate_iris,
                    },
                });
                Ok((start, end))
            }
        }
    }
}

fn compile_path_automaton(
    path: &PropertyPathExpression,
) -> Result<DistributedPathAutomaton, PropertyPathPlanError> {
    let mut builder = PathAutomatonBuilder {
        state_count: 0,
        transitions: Vec::new(),
    };
    let (start_state, accept_state) = builder.fragment(path, false)?;
    builder.transitions.sort();
    Ok(DistributedPathAutomaton {
        format_version: 1,
        state_count: builder.state_count,
        start_state,
        accept_states: vec![accept_state],
        transitions: builder.transitions,
    })
}

fn collect_distributed_property_paths(
    pattern: &GraphPattern,
    graph_scope: &str,
    limits: DistributedPropertyPathLimits,
    output: &mut Vec<DistributedPropertyPathPlan>,
) -> Result<(), PropertyPathPlanError> {
    match pattern {
        GraphPattern::Path {
            subject,
            path,
            object,
        } => {
            let path_ordinal =
                u32::try_from(output.len()).map_err(|_| PropertyPathPlanError::InvalidIdentity)?;
            let automaton = compile_path_automaton(path)?;
            let automaton_bytes = serde_json::to_vec(&automaton)
                .map_err(|_| PropertyPathPlanError::InvalidAutomaton)?;
            let plan = DistributedPropertyPathPlan {
                path_id: format!("property-path-{path_ordinal:05}"),
                path_ordinal,
                graph_scope: graph_scope.to_owned(),
                subject_pattern: subject.to_string(),
                path_sparql: path.to_string(),
                object_pattern: object.to_string(),
                automaton,
                automaton_sha256: hex::encode(Sha256::digest(automaton_bytes)),
                partition_count: limits.partition_count,
                max_iterations: limits.max_iterations,
                max_frontier_items: limits.max_frontier_items,
                max_visited_items: limits.max_visited_items,
                max_checkpoint_bytes: limits.max_checkpoint_bytes,
                max_spill_bytes: limits.max_spill_bytes,
                hot_vertex_degree: limits.hot_vertex_degree,
                max_hot_vertex_splits: limits.max_hot_vertex_splits,
                require_complete_partition_set: true,
                require_scalar_equivalence: true,
            };
            validate_distributed_property_path_plan(&plan)?;
            output.push(plan);
        }
        GraphPattern::Join { left, right }
        | GraphPattern::Lateral { left, right }
        | GraphPattern::Union { left, right }
        | GraphPattern::Minus { left, right } => {
            collect_distributed_property_paths(left, graph_scope, limits, output)?;
            collect_distributed_property_paths(right, graph_scope, limits, output)?;
        }
        GraphPattern::LeftJoin {
            left,
            right,
            expression,
        } => {
            collect_distributed_property_paths(left, graph_scope, limits, output)?;
            collect_distributed_property_paths(right, graph_scope, limits, output)?;
            if let Some(expression) = expression {
                collect_expression_property_paths(expression, graph_scope, limits, output)?;
            }
        }
        GraphPattern::Filter { expr, inner } => {
            collect_distributed_property_paths(inner, graph_scope, limits, output)?;
            collect_expression_property_paths(expr, graph_scope, limits, output)?;
        }
        GraphPattern::Graph { name, inner } => {
            collect_distributed_property_paths(inner, &name.to_string(), limits, output)?;
        }
        GraphPattern::Extend {
            inner, expression, ..
        } => {
            collect_distributed_property_paths(inner, graph_scope, limits, output)?;
            collect_expression_property_paths(expression, graph_scope, limits, output)?;
        }
        GraphPattern::OrderBy { inner, expression } => {
            collect_distributed_property_paths(inner, graph_scope, limits, output)?;
            for order in expression {
                match order {
                    OrderExpression::Asc(expression) | OrderExpression::Desc(expression) => {
                        collect_expression_property_paths(
                            expression,
                            graph_scope,
                            limits,
                            output,
                        )?;
                    }
                }
            }
        }
        GraphPattern::Project { inner, .. }
        | GraphPattern::Distinct { inner }
        | GraphPattern::Reduced { inner }
        | GraphPattern::Slice { inner, .. } => {
            collect_distributed_property_paths(inner, graph_scope, limits, output)?;
        }
        GraphPattern::Group {
            inner, aggregates, ..
        } => {
            collect_distributed_property_paths(inner, graph_scope, limits, output)?;
            for (_, aggregate) in aggregates {
                if let AggregateExpression::FunctionCall { expr, .. } = aggregate {
                    collect_expression_property_paths(expr, graph_scope, limits, output)?;
                }
            }
        }
        GraphPattern::Service { inner, .. } => {
            collect_distributed_property_paths(inner, graph_scope, limits, output)?;
        }
        GraphPattern::Bgp { .. } | GraphPattern::Values { .. } => {}
    }
    Ok(())
}

fn collect_expression_property_paths(
    expression: &Expression,
    graph_scope: &str,
    limits: DistributedPropertyPathLimits,
    output: &mut Vec<DistributedPropertyPathPlan>,
) -> Result<(), PropertyPathPlanError> {
    match expression {
        Expression::NamedNode(_)
        | Expression::Literal(_)
        | Expression::Variable(_)
        | Expression::Bound(_) => {}
        Expression::Or(left, right)
        | Expression::And(left, right)
        | Expression::Equal(left, right)
        | Expression::SameTerm(left, right)
        | Expression::Greater(left, right)
        | Expression::GreaterOrEqual(left, right)
        | Expression::Less(left, right)
        | Expression::LessOrEqual(left, right)
        | Expression::Add(left, right)
        | Expression::Subtract(left, right)
        | Expression::Multiply(left, right)
        | Expression::Divide(left, right) => {
            collect_expression_property_paths(left, graph_scope, limits, output)?;
            collect_expression_property_paths(right, graph_scope, limits, output)?;
        }
        Expression::In(left, values) => {
            collect_expression_property_paths(left, graph_scope, limits, output)?;
            for value in values {
                collect_expression_property_paths(value, graph_scope, limits, output)?;
            }
        }
        Expression::UnaryPlus(inner)
        | Expression::UnaryMinus(inner)
        | Expression::Not(inner) => {
            collect_expression_property_paths(inner, graph_scope, limits, output)?;
        }
        Expression::Exists(pattern) => {
            collect_distributed_property_paths(pattern, graph_scope, limits, output)?;
        }
        Expression::If(condition, yes, no) => {
            collect_expression_property_paths(condition, graph_scope, limits, output)?;
            collect_expression_property_paths(yes, graph_scope, limits, output)?;
            collect_expression_property_paths(no, graph_scope, limits, output)?;
        }
        Expression::Coalesce(values) | Expression::FunctionCall(_, values) => {
            for value in values {
                collect_expression_property_paths(value, graph_scope, limits, output)?;
            }
        }
    }
    Ok(())
}

fn textual_variable_order(query_text: &str) -> Vec<String> {
    #[derive(Clone, Copy, Eq, PartialEq)]
    enum LexicalState {
        Normal,
        Comment,
        Iri,
        String { quote: char, triple: bool },
    }

    let chars = query_text.chars().collect::<Vec<_>>();
    let mut state = LexicalState::Normal;
    let mut order = Vec::new();
    let mut seen = BTreeSet::new();
    let mut index = 0;
    while index < chars.len() {
        match state {
            LexicalState::Comment => {
                if matches!(chars[index], '\n' | '\r') {
                    state = LexicalState::Normal;
                }
                index += 1;
            }
            LexicalState::Iri => {
                if chars[index] == '\\' {
                    index = (index + 2).min(chars.len());
                } else {
                    if chars[index] == '>' {
                        state = LexicalState::Normal;
                    }
                    index += 1;
                }
            }
            LexicalState::String { quote, triple } => {
                if chars[index] == '\\' {
                    index = (index + 2).min(chars.len());
                } else if chars[index] == quote {
                    if triple
                        && index + 2 < chars.len()
                        && chars[index + 1] == quote
                        && chars[index + 2] == quote
                    {
                        index += 3;
                        state = LexicalState::Normal;
                    } else if !triple {
                        index += 1;
                        state = LexicalState::Normal;
                    } else {
                        index += 1;
                    }
                } else {
                    index += 1;
                }
            }
            LexicalState::Normal => match chars[index] {
                '#' => {
                    state = LexicalState::Comment;
                    index += 1;
                }
                '<' => {
                    state = LexicalState::Iri;
                    index += 1;
                }
                quote @ ('\'' | '"') => {
                    let triple = index + 2 < chars.len()
                        && chars[index + 1] == quote
                        && chars[index + 2] == quote;
                    index += if triple { 3 } else { 1 };
                    state = LexicalState::String { quote, triple };
                }
                '?' | '$'
                    if index + 1 < chars.len()
                        && (chars[index + 1] == '_' || chars[index + 1].is_alphanumeric()) =>
                {
                    let start = index + 1;
                    let mut end = start + 1;
                    while end < chars.len()
                        && (chars[end] == '_'
                            || chars[end] == '\u{00B7}'
                            || chars[end].is_alphanumeric()
                            || ('\u{0300}'..='\u{036F}').contains(&chars[end])
                            || ('\u{203F}'..='\u{2040}').contains(&chars[end]))
                    {
                        end += 1;
                    }
                    let variable = chars[start..end].iter().collect::<String>();
                    if seen.insert(variable.clone()) {
                        order.push(variable);
                    }
                    index = end;
                }
                _ => index += 1,
            },
        }
    }
    order
}

fn normalize_token_separators(query_text: &str) -> String {
    #[derive(Clone, Copy, Eq, PartialEq)]
    enum LexicalState {
        Normal,
        Comment,
        Iri,
        String { quote: char, triple: bool },
    }

    let chars = query_text.chars().collect::<Vec<_>>();
    let mut output = String::with_capacity(query_text.len() + 8);
    let mut state = LexicalState::Normal;
    let mut index = 0;
    while index < chars.len() {
        let value = chars[index];
        output.push(value);
        match state {
            LexicalState::Comment => {
                if matches!(value, '\n' | '\r') {
                    state = LexicalState::Normal;
                }
                index += 1;
            }
            LexicalState::Iri => {
                if value == '\\' && index + 1 < chars.len() {
                    index += 1;
                    output.push(chars[index]);
                } else if value == '>' {
                    state = LexicalState::Normal;
                }
                index += 1;
            }
            LexicalState::String { quote, triple } => {
                if value == '\\' && index + 1 < chars.len() {
                    index += 1;
                    output.push(chars[index]);
                } else if value == quote {
                    if triple
                        && index + 2 < chars.len()
                        && chars[index + 1] == quote
                        && chars[index + 2] == quote
                    {
                        output.push(quote);
                        output.push(quote);
                        index += 3;
                        state = LexicalState::Normal;
                        continue;
                    }
                    if !triple {
                        state = LexicalState::Normal;
                    }
                }
                index += 1;
            }
            LexicalState::Normal => {
                match value {
                    '#' => state = LexicalState::Comment,
                    '<' => state = LexicalState::Iri,
                    quote @ ('\'' | '"') => {
                        let triple = index + 2 < chars.len()
                            && chars[index + 1] == quote
                            && chars[index + 2] == quote;
                        if triple {
                            output.push(quote);
                            output.push(quote);
                            index += 2;
                        }
                        state = LexicalState::String { quote, triple };
                    }
                    ',' if index + 1 < chars.len() && !chars[index + 1].is_whitespace() => {
                        output.push(' ');
                    }
                    _ => {}
                }
                index += 1;
            }
        }
    }
    output
}

fn query_form(query: &Query) -> QueryForm {
    match query {
        Query::Select { .. } => QueryForm::Select,
        Query::Ask { .. } => QueryForm::Ask,
        Query::Construct { .. } => QueryForm::Construct,
        Query::Describe { .. } => QueryForm::Describe,
    }
}

fn query_pattern(query: &Query) -> &GraphPattern {
    match query {
        Query::Select { pattern, .. }
        | Query::Ask { pattern, .. }
        | Query::Construct { pattern, .. }
        | Query::Describe { pattern, .. } => pattern,
    }
}

fn query_dataset(query: &Query) -> QueryDatasetSpecification {
    let Some(dataset) = query.dataset() else {
        return QueryDatasetSpecification::default();
    };
    QueryDatasetSpecification {
        specified: true,
        default_graph_iris: dataset
            .default
            .iter()
            .map(|graph| graph.as_str().to_owned())
            .collect(),
        named_graph_iris: dataset
            .named
            .as_ref()
            .map(|graphs| {
                graphs
                    .iter()
                    .map(|graph| graph.as_str().to_owned())
                    .collect()
            })
            .unwrap_or_default(),
    }
}

fn inspect_graph_pattern(
    pattern: &GraphPattern,
    inside_graph: bool,
    route: &mut RouteAnalysis,
    execution: &mut ExecutionAnalysis,
) -> Result<(), SparqlCompileError> {
    match pattern {
        GraphPattern::Bgp { patterns } => {
            if !inside_graph && !patterns.is_empty() {
                route.has_default_graph_pattern = true;
            }
            for triple in patterns {
                inspect_triple_pattern(triple, route);
            }
        }
        GraphPattern::Path {
            subject: _,
            path,
            object: _,
        } => {
            if !inside_graph {
                route.has_default_graph_pattern = true;
            }
            route.has_property_path = true;
            collect_path_iris(path, &mut route.semantic_iris);
        }
        GraphPattern::Join { left, right }
        | GraphPattern::Lateral { left, right }
        | GraphPattern::Union { left, right }
        | GraphPattern::Minus { left, right } => {
            inspect_graph_pattern(left, inside_graph, route, execution)?;
            inspect_graph_pattern(right, inside_graph, route, execution)?;
        }
        GraphPattern::LeftJoin {
            left,
            right,
            expression,
        } => {
            inspect_graph_pattern(left, inside_graph, route, execution)?;
            inspect_graph_pattern(right, inside_graph, route, execution)?;
            if let Some(expression) = expression {
                inspect_expression(expression, inside_graph, route, execution)?;
            }
        }
        GraphPattern::Filter { expr, inner } => {
            inspect_graph_pattern(inner, inside_graph, route, execution)?;
            inspect_expression(expr, inside_graph, route, execution)?;
        }
        GraphPattern::Graph { name, inner } => {
            match name {
                NamedNodePattern::NamedNode(node) => {
                    route.declared_graph_iris.insert(node.as_str().to_owned());
                }
                NamedNodePattern::Variable(_) => route.has_graph_variable = true,
            }
            inspect_graph_pattern(inner, true, route, execution)?;
        }
        GraphPattern::Extend {
            inner,
            variable: _,
            expression,
        } => {
            inspect_graph_pattern(inner, inside_graph, route, execution)?;
            inspect_expression(expression, inside_graph, route, execution)?;
        }
        GraphPattern::Values { .. } => {}
        GraphPattern::OrderBy { inner, expression } => {
            inspect_graph_pattern(inner, inside_graph, route, execution)?;
            for order in expression {
                match order {
                    OrderExpression::Asc(expression) | OrderExpression::Desc(expression) => {
                        inspect_expression(expression, inside_graph, route, execution)?;
                    }
                }
            }
        }
        GraphPattern::Project {
            inner,
            variables: _,
        }
        | GraphPattern::Distinct { inner }
        | GraphPattern::Reduced { inner }
        | GraphPattern::Slice { inner, .. } => {
            inspect_graph_pattern(inner, inside_graph, route, execution)?;
        }
        GraphPattern::Group {
            inner,
            variables: _,
            aggregates,
        } => {
            inspect_graph_pattern(inner, inside_graph, route, execution)?;
            for (_, aggregate) in aggregates {
                inspect_aggregate(aggregate, inside_graph, route, execution)?;
            }
        }
        GraphPattern::Service { inner, .. } => {
            execution.has_remote_service = true;
            inspect_graph_pattern(inner, inside_graph, route, execution)?;
        }
    }
    Ok(())
}

fn inspect_triple_pattern(pattern: &TriplePattern, route: &mut RouteAnalysis) {
    if let NamedNodePattern::NamedNode(predicate) = &pattern.predicate {
        route.semantic_iris.insert(predicate.as_str().to_owned());
        if predicate.as_str() == RDF_TYPE_IRI {
            if let TermPattern::NamedNode(class) = &pattern.object {
                route.semantic_iris.insert(class.as_str().to_owned());
            }
        }
    }
}

fn collect_path_iris(path: &PropertyPathExpression, iris: &mut BTreeSet<String>) {
    match path {
        PropertyPathExpression::NamedNode(node) => {
            iris.insert(node.as_str().to_owned());
        }
        PropertyPathExpression::Reverse(inner)
        | PropertyPathExpression::ZeroOrMore(inner)
        | PropertyPathExpression::OneOrMore(inner)
        | PropertyPathExpression::ZeroOrOne(inner) => collect_path_iris(inner, iris),
        PropertyPathExpression::Sequence(left, right)
        | PropertyPathExpression::Alternative(left, right) => {
            collect_path_iris(left, iris);
            collect_path_iris(right, iris);
        }
        PropertyPathExpression::NegatedPropertySet(nodes) => {
            iris.extend(nodes.iter().map(|node| node.as_str().to_owned()));
        }
    }
}

fn inspect_aggregate(
    aggregate: &AggregateExpression,
    inside_graph: bool,
    route: &mut RouteAnalysis,
    execution: &mut ExecutionAnalysis,
) -> Result<(), SparqlCompileError> {
    match aggregate {
        AggregateExpression::CountSolutions { .. } => Ok(()),
        AggregateExpression::FunctionCall { expr, .. } => {
            inspect_expression(expr, inside_graph, route, execution)
        }
    }
}

fn inspect_expression(
    expression: &Expression,
    inside_graph: bool,
    route: &mut RouteAnalysis,
    execution: &mut ExecutionAnalysis,
) -> Result<(), SparqlCompileError> {
    match expression {
        Expression::NamedNode(_)
        | Expression::Literal(_)
        | Expression::Variable(_)
        | Expression::Bound(_) => Ok(()),
        Expression::Or(left, right)
        | Expression::And(left, right)
        | Expression::Equal(left, right)
        | Expression::SameTerm(left, right)
        | Expression::Greater(left, right)
        | Expression::GreaterOrEqual(left, right)
        | Expression::Less(left, right)
        | Expression::LessOrEqual(left, right)
        | Expression::Add(left, right)
        | Expression::Subtract(left, right)
        | Expression::Multiply(left, right)
        | Expression::Divide(left, right) => {
            inspect_expression(left, inside_graph, route, execution)?;
            inspect_expression(right, inside_graph, route, execution)
        }
        Expression::In(left, values) => {
            inspect_expression(left, inside_graph, route, execution)?;
            for value in values {
                inspect_expression(value, inside_graph, route, execution)?;
            }
            Ok(())
        }
        Expression::UnaryPlus(inner) | Expression::UnaryMinus(inner) | Expression::Not(inner) => {
            inspect_expression(inner, inside_graph, route, execution)
        }
        Expression::Exists(pattern) => {
            inspect_graph_pattern(pattern, inside_graph, route, execution)
        }
        Expression::If(condition, yes, no) => {
            inspect_expression(condition, inside_graph, route, execution)?;
            inspect_expression(yes, inside_graph, route, execution)?;
            inspect_expression(no, inside_graph, route, execution)
        }
        Expression::Coalesce(values) => {
            for value in values {
                inspect_expression(value, inside_graph, route, execution)?;
            }
            Ok(())
        }
        Expression::FunctionCall(function, arguments) => {
            let volatile_name = match function {
                Function::BNode => Some("BNODE"),
                Function::Rand => Some("RAND"),
                Function::Now => Some("NOW"),
                Function::Uuid => Some("UUID"),
                Function::StrUuid => Some("STRUUID"),
                _ => None,
            };
            if let Some(name) = volatile_name {
                execution.volatile_functions.insert(name.to_owned());
            }
            for argument in arguments {
                inspect_expression(argument, inside_graph, route, execution)?;
            }
            Ok(())
        }
    }
}

fn top_level_has_order_by(pattern: &GraphPattern) -> bool {
    top_level_has_order_by_after_projection(pattern, false)
}

fn top_level_has_order_by_after_projection(pattern: &GraphPattern, projected: bool) -> bool {
    match pattern {
        GraphPattern::OrderBy { .. } => true,
        // A second Project is a subquery boundary. Its ORDER BY constrains only
        // that subquery and must not turn the outer SELECT into an ordered result.
        GraphPattern::Project { inner, .. } if !projected => {
            top_level_has_order_by_after_projection(inner, true)
        }
        GraphPattern::Distinct { inner }
        | GraphPattern::Reduced { inner }
        | GraphPattern::Slice { inner, .. } => {
            top_level_has_order_by_after_projection(inner, projected)
        }
        _ => false,
    }
}

fn strip_projection(pattern: &GraphPattern) -> &GraphPattern {
    if let GraphPattern::Project { inner, .. } = pattern {
        inner
    } else {
        pattern
    }
}

fn collect_constant_graph_join_leaves(
    pattern: &GraphPattern,
    output: &mut Vec<(String, GraphPattern)>,
) -> bool {
    match pattern {
        GraphPattern::Join { left, right } => {
            collect_constant_graph_join_leaves(left, output)
                && collect_constant_graph_join_leaves(right, output)
        }
        GraphPattern::Graph {
            name: NamedNodePattern::NamedNode(node),
            inner,
        } => {
            output.push((
                node.as_str().to_owned(),
                GraphPattern::Graph {
                    name: NamedNodePattern::NamedNode(node.clone()),
                    inner: inner.clone(),
                },
            ));
            true
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use ngkg_query_planner::{
        DistributedAlgebraLimits, DistributedAlgebraOperator, DistributedPropertyPathLimits,
        PathDirection, PathTransitionKind,
    };

    use super::{CompiledSparqlQuery, QueryForm, SparqlCertificationError, SparqlCompileError};

    #[test]
    fn dataset_and_routes_come_from_typed_algebra() -> Result<(), SparqlCompileError> {
        let query = CompiledSparqlQuery::parse(
            "PREFIX ex: <https://example.test/> SELECT ?s FROM ex:g1 FROM NAMED ex:g2 WHERE { GRAPH ex:g2 { ?s a ex:Asset } }",
        )?;
        assert_eq!(query.form(), QueryForm::Select);
        assert_eq!(
            query.dataset_specification().default_graph_iris,
            ["https://example.test/g1"]
        );
        assert_eq!(
            query.dataset_specification().named_graph_iris,
            ["https://example.test/g2"]
        );
        assert!(
            query
                .route_analysis()
                .declared_graph_iris
                .contains("https://example.test/g2")
        );
        assert!(
            query
                .route_analysis()
                .semantic_iris
                .contains("https://example.test/Asset")
        );
        Ok(())
    }

    #[test]
    fn volatile_and_remote_features_parse_then_receive_execution_policy()
    -> Result<(), SparqlCompileError> {
        let volatile = CompiledSparqlQuery::parse("SELECT (NOW() AS ?now) WHERE {}")?;
        assert!(
            volatile
                .execution_analysis()
                .volatile_functions
                .contains("NOW")
        );
        assert!(!volatile.execution_analysis().is_snapshot_cacheable());
        assert!(matches!(
            volatile.require_certifiable(),
            Err(SparqlCertificationError::NondeterministicFunction(_))
        ));

        let service = CompiledSparqlQuery::parse(
            "SELECT * WHERE { SERVICE <https://example.test/sparql> { ?s ?p ?o } }",
        )?;
        assert!(service.execution_analysis().has_remote_service);
        assert!(!service.execution_analysis().is_certifiable());
        assert!(matches!(
            service.require_certifiable(),
            Err(SparqlCertificationError::RemoteService)
        ));

        assert!(CompiledSparqlQuery::parse("SELECT * WHERE { BIND(\"NOW()\" AS ?text) }").is_ok());
        Ok(())
    }

    #[test]
    fn all_standard_volatile_functions_are_classified_without_parser_rejection()
    -> Result<(), SparqlCompileError> {
        let query = CompiledSparqlQuery::parse(
            "SELECT (RAND() AS ?r) (NOW() AS ?n) (UUID() AS ?u) (STRUUID() AS ?su) (BNODE() AS ?b) WHERE {}",
        )?;
        for function in ["RAND", "NOW", "UUID", "STRUUID", "BNODE"] {
            assert!(
                query
                    .execution_analysis()
                    .volatile_functions
                    .contains(function)
            );
        }
        assert!(!query.execution_analysis().is_certifiable());
        Ok(())
    }

    #[test]
    fn only_pure_constant_graph_inner_join_is_distributable() -> Result<(), SparqlCompileError> {
        let query = CompiledSparqlQuery::parse(
            "SELECT ?s ?o WHERE { GRAPH <https://example.test/g1> { ?s <https://example.test/p> ?x } GRAPH <https://example.test/g2> { ?x <https://example.test/q> ?o } }",
        )?;
        let fragments = query.distributed_graph_fragments().ok_or_else(|| {
            SparqlCompileError::Syntax("expected distributed fragments".to_owned())
        })?;
        assert_eq!(fragments.len(), 2);
        assert_eq!(fragments[0].graph_iri, "https://example.test/g1");
        assert!(fragments[0].query_text.starts_with("SELECT"));

        let filtered = CompiledSparqlQuery::parse(
            "SELECT ?s WHERE { GRAPH <https://example.test/g1> { ?s <https://example.test/p> ?x } GRAPH <https://example.test/g2> { ?x <https://example.test/q> ?o } FILTER(?o > 1) }",
        )?;
        assert!(filtered.distributed_graph_fragments().is_none());
        Ok(())
    }

    #[test]
    fn graph_variable_and_property_path_force_conservative_routing_evidence()
    -> Result<(), SparqlCompileError> {
        let graph_variable = CompiledSparqlQuery::parse(
            "SELECT ?g ?s WHERE { GRAPH ?g { ?s <https://example.test/p> ?o } }",
        )?;
        assert!(graph_variable.route_analysis().has_graph_variable);
        assert!(!graph_variable.route_analysis().has_default_graph_pattern);

        let path = CompiledSparqlQuery::parse(
            "SELECT ?s ?o WHERE { ?s <https://example.test/p>/<https://example.test/q>+ ?o }",
        )?;
        assert!(path.route_analysis().has_property_path);
        assert!(path.route_analysis().has_default_graph_pattern);
        assert!(
            path.route_analysis()
                .semantic_iris
                .contains("https://example.test/p")
        );
        assert!(
            path.route_analysis()
                .semantic_iris
                .contains("https://example.test/q")
        );
        assert!(path.distributed_graph_fragments().is_none());
        Ok(())
    }

    #[test]
    fn non_inner_join_algebra_never_enters_distributed_fast_path() -> Result<(), SparqlCompileError>
    {
        for query_text in [
            "SELECT ?s WHERE { GRAPH <https://example.test/g1> { ?s <https://example.test/p> ?x } OPTIONAL { GRAPH <https://example.test/g2> { ?x <https://example.test/q> ?o } } }",
            "SELECT ?s WHERE { { GRAPH <https://example.test/g1> { ?s <https://example.test/p> ?x } } UNION { GRAPH <https://example.test/g2> { ?s <https://example.test/q> ?o } } }",
            "SELECT ?s WHERE { GRAPH <https://example.test/g1> { ?s <https://example.test/p> ?x } MINUS { GRAPH <https://example.test/g2> { ?s <https://example.test/q> ?o } } }",
        ] {
            let compiled = CompiledSparqlQuery::parse(query_text)?;
            assert!(compiled.distributed_graph_fragments().is_none());
        }
        Ok(())
    }

    #[test]
    fn all_query_forms_are_typed_for_phase39_scalar_execution() -> Result<(), SparqlCompileError> {
        let cases = [
            ("ASK { ?s ?p ?o }", QueryForm::Ask),
            (
                "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }",
                QueryForm::Construct,
            ),
            ("DESCRIBE ?s WHERE { ?s ?p ?o }", QueryForm::Describe),
        ];
        for (query_text, expected) in cases {
            assert_eq!(CompiledSparqlQuery::parse(query_text)?.form(), expected);
        }
        Ok(())
    }

    #[test]
    fn only_top_level_order_by_marks_select_sequence_as_significant()
    -> Result<(), SparqlCompileError> {
        let ordered = CompiledSparqlQuery::parse(
            "SELECT ?s WHERE { ?s <https://example.test/p> ?o } ORDER BY ?s",
        )?;
        assert!(ordered.solution_order_is_significant());
        let unordered = CompiledSparqlQuery::parse(
            "SELECT ?s WHERE { { SELECT ?s WHERE { ?s <https://example.test/p> ?o } ORDER BY ?s } }",
        )?;
        assert!(!unordered.solution_order_is_significant());
        assert!(!CompiledSparqlQuery::parse("ASK { ?s ?p ?o }")?.solution_order_is_significant());
        Ok(())
    }

    #[test]
    fn canonical_algebra_hash_is_stable_across_surface_whitespace() -> Result<(), SparqlCompileError>
    {
        let left =
            CompiledSparqlQuery::parse("SELECT ?s WHERE { ?s <https://example.test/p> ?o }")?;
        let right = CompiledSparqlQuery::parse("SELECT ?s WHERE{?s <https://example.test/p> ?o}")?;
        assert_eq!(left.canonical_sse(), right.canonical_sse());
        assert_eq!(left.canonical_sse_sha256(), right.canonical_sse_sha256());
        Ok(())
    }

    #[test]
    fn retrieval_base_resolves_relative_query_iris() -> Result<(), SparqlCompileError> {
        for base in [
            "https://example.test/query.rq",
            "file:///tmp/ngkg-tests/query.rq",
        ] {
            let query =
                CompiledSparqlQuery::parse_with_base_iri("SELECT * WHERE { <s> <p> <o> }", base)?;
            assert!(query.canonical_sse().contains("/s"));
            assert!(query.canonical_sse().contains("/p"));
        }
        Ok(())
    }

    #[test]
    fn legal_adjacent_comma_tokens_are_retried_without_changing_literals()
    -> Result<(), SparqlCompileError> {
        let query = CompiledSparqlQuery::parse(
            r#"SELECT ?s WHERE { VALUES ?s { 1 2 } FILTER(?s IN(1,2)) BIND("a,b" AS ?text) }"#,
        )?;
        assert_eq!(query.form(), QueryForm::Select);
        assert_eq!(query.solution_variable_order(), ["s", "text"]);
        Ok(())
    }

    #[test]
    fn lexical_variable_order_ignores_comments_iris_and_strings() -> Result<(), SparqlCompileError>
    {
        let query = CompiledSparqlQuery::parse(concat!(
            "SELECT ?second ?first WHERE {\n",
            "  # ?commented\n",
            "  ?first <https://example.test/?not-a-variable> \"?also-not\" .\n",
            "  BIND(?first AS ?second)\n",
            "}",
        ))?;
        assert_eq!(query.solution_variable_order(), ["second", "first"]);
        Ok(())
    }

    #[test]
    fn complete_algebra_plan_covers_optional_union_minus_group_order_and_subquery()
    -> Result<(), SparqlCompileError> {
        let query = CompiledSparqlQuery::parse(
            "SELECT DISTINCT ?s (COUNT(?o) AS ?count) WHERE { \
             { ?s <https://example.test/p> ?o OPTIONAL { ?s <https://example.test/q> ?x } } \
             UNION { { SELECT ?s ?o WHERE { ?s <https://example.test/r> ?o } } \
             MINUS { ?s <https://example.test/blocked> true } } } \
             GROUP BY ?s ORDER BY DESC(?count) LIMIT 10",
        )?;
        let plan = query
            .distributed_algebra_plan(DistributedAlgebraLimits {
                partition_count: 8,
                max_input_rows: 1_000_000,
                max_output_rows: 1_000_000,
                max_exchange_bytes: 1 << 30,
                max_spill_bytes: 1 << 32,
            })
            .map_err(|error| SparqlCompileError::Syntax(error.to_string()))?;
        for operator in [
            DistributedAlgebraOperator::LeftJoin,
            DistributedAlgebraOperator::Union,
            DistributedAlgebraOperator::Minus,
            DistributedAlgebraOperator::Group,
            DistributedAlgebraOperator::Order,
            DistributedAlgebraOperator::Distinct,
            DistributedAlgebraOperator::Subquery,
            DistributedAlgebraOperator::Slice,
        ] {
            assert!(plan.stages.iter().any(|stage| stage.operator == operator));
        }
        assert!(plan.require_complete_partition_set);
        assert!(plan.require_scalar_equivalence);
        Ok(())
    }

    #[test]
    fn property_paths_compile_to_bounded_forward_and_reverse_automata()
    -> Result<(), SparqlCompileError> {
        let query = CompiledSparqlQuery::parse(
            "SELECT ?s ?o WHERE { ?s (^<urn:p>/<urn:q>)+|!<urn:blocked> ?o }",
        )?;
        let plans = query
            .distributed_property_path_plans(DistributedPropertyPathLimits {
                partition_count: 8,
                max_iterations: 10_000,
                max_frontier_items: 1_000_000,
                max_visited_items: 10_000_000,
                max_checkpoint_bytes: 1 << 30,
                max_spill_bytes: 1 << 32,
                hot_vertex_degree: 100_000,
                max_hot_vertex_splits: 64,
            })
            .map_err(|error| SparqlCompileError::Syntax(error.to_string()))?;
        assert_eq!(plans.len(), 1);
        let plan = &plans[0];
        assert!(plan.automaton.state_count > 2);
        assert!(plan.automaton.transitions.iter().any(|transition| matches!(
            &transition.transition,
            PathTransitionKind::Predicate {
                direction: PathDirection::Reverse,
                predicate_iri,
            } if predicate_iri == "urn:p"
        )));
        assert!(plan.automaton.transitions.iter().any(|transition| matches!(
            &transition.transition,
            PathTransitionKind::NegatedPropertySet { .. }
        )));
        Ok(())
    }
}
