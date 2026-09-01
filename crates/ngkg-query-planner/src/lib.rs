//! Authorization-first relevant-graph routing and distributed plan contracts.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

/// Capability token extracted from parsed SPARQL algebra.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum QueryCapability {
    ClassIri(String),
    PropertyIri(String),
    BoundGuid(Uuid),
    GraphRole(String),
    VirtualPredicate(String),
}

/// Snapshot-bound graph routing metadata.
#[derive(Clone, Debug, Default)]
pub struct GraphRoutingIndex {
    pub capabilities: BTreeMap<QueryCapability, BTreeSet<u32>>,
    pub dependencies: BTreeMap<u32, BTreeSet<u32>>,
}

/// One inspectable distributed plan.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DistributedPlan {
    pub plan_id: String,
    pub dataset_id: Uuid,
    pub snapshot_id: Uuid,
    pub authorized_graph_set_hash: [u8; 32],
    pub selected_graph_ids: Vec<u32>,
    pub fragments: Vec<QueryFragment>,
    pub exchanges: Vec<Exchange>,
    pub final_operators: Vec<CoordinatorOperator>,
    pub coverage_requirement: CoverageRequirement,
    pub hydration: HydrationRequirement,
}

/// One independently executable graph/table/reasoner fragment.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryFragment {
    pub fragment_id: String,
    pub owner: FragmentOwner,
    pub algebra_hash: [u8; 32],
    pub graph_ids: Vec<u32>,
    pub required_index_roots: Vec<[u8; 32]>,
    pub deadline_unix_ms: i64,
    pub memory_limit_bytes: u64,
}

/// Physical owner of a query fragment.
#[derive(Clone, Debug, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum FragmentOwner {
    SemanticShard { shard_id: String },
    VirtualRdf { compiled_plan_id: String },
    ExactReasoner { module_manifest_uri: String },
}

/// Cross-node Arrow exchange strategy.
#[derive(Clone, Debug, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Exchange {
    BroadcastExactKeys {
        source: String,
        targets: Vec<String>,
        key_columns: Vec<u16>,
    },
    BloomThenExact {
        source: String,
        targets: Vec<String>,
        key_columns: Vec<u16>,
    },
    HashShuffle {
        sources: Vec<String>,
        targets: Vec<String>,
        key_columns: Vec<u16>,
    },
    OrderedMerge {
        sources: Vec<String>,
        target: String,
        order_columns: Vec<u16>,
    },
}

/// Coordinator operations that cannot be hidden in unordered joins.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CoordinatorOperator {
    LeftJoin,
    Minus,
    NotExists,
    Aggregate,
    Distinct,
    Order,
    Slice,
    ProofGate,
}

/// Phase 40.13.8 execution lane for one typed SPARQL algebra stage.
///
/// Native stages are limited to operators whose multiset semantics are fully represented by
/// bindings. Operators that depend on SPARQL expression evaluation, aggregate error behavior,
/// RDF-term ordering, subquery scope, or graph construction remain on the qualified scalar-oracle
/// lane, but the complete stage is assigned to a worker and may execute concurrently with other
/// ready stages.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AlgebraExecutionLane {
    /// Exact native Rust multiset kernel.
    NativePartitioned,
    /// Pinned standards evaluator running on a fragment worker.
    ScalarOraclePartitioned,
    /// Exact HermiT-backed BGP worker lane.
    ExactReasonerPartitioned,
}

/// Closed Phase 40.13.8 SPARQL algebra vocabulary.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DistributedAlgebraOperator {
    Bgp,
    Path,
    Join,
    Lateral,
    LeftJoin,
    Union,
    Minus,
    Filter,
    Extend,
    Values,
    Graph,
    Project,
    Distinct,
    Reduced,
    Group,
    Order,
    Slice,
    Subquery,
    Service,
    AskFinalize,
    ConstructFinalize,
    DescribeFinalize,
}

/// One immutable node in a post-order distributed algebra DAG.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DistributedAlgebraStage {
    /// Stable post-order stage identifier.
    pub stage_id: String,
    /// Typed operator represented by this node.
    pub operator: DistributedAlgebraOperator,
    /// Stage IDs that must complete before this stage is admitted.
    pub inputs: Vec<String>,
    /// Execution lane selected from standards-preserving rules.
    pub lane: AlgebraExecutionLane,
    /// SHA-256 of the exact wrapped algebra subtree.
    pub algebra_sha256: String,
    /// Stable number of logical output partitions.
    pub partition_count: u32,
    /// Maximum admitted input rows across all partitions.
    pub max_input_rows: u64,
    /// Maximum admitted output rows across all partitions.
    pub max_output_rows: u64,
    /// Maximum Arrow exchange bytes across all attempts.
    pub max_exchange_bytes: u64,
    /// Maximum local spill bytes across all attempts.
    pub max_spill_bytes: u64,
}

/// Snapshot-independent typed algebra DAG produced from one parsed SPARQL query.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DistributedAlgebraPlan {
    /// Contract version. Phase 40.13.8 emits version 1.
    pub format_version: u32,
    /// SHA-256 of the canonical complete query algebra.
    pub query_algebra_sha256: String,
    /// Root stage whose complete output determines the query result.
    pub root_stage_id: String,
    /// Post-order nodes. Inputs must always precede their consumers.
    pub stages: Vec<DistributedAlgebraStage>,
    /// All partitions must complete before the result may be returned.
    pub require_complete_partition_set: bool,
    /// Optimized output must compare equal to the scalar oracle before certification.
    pub require_scalar_equivalence: bool,
}

/// Deployment ceilings copied into every immutable algebra stage.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DistributedAlgebraLimits {
    pub partition_count: u32,
    pub max_input_rows: u64,
    pub max_output_rows: u64,
    pub max_exchange_bytes: u64,
    pub max_spill_bytes: u64,
}

/// One immutable worker-owned partition from a topological execution wave.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AlgebraWorkItem {
    pub stage_id: String,
    pub partition: u32,
    pub partition_count: u32,
}

/// Independent stages and all their partitions that may run concurrently.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AlgebraExecutionWave {
    pub ordinal: u32,
    pub work_items: Vec<AlgebraWorkItem>,
}

/// Direction in which a property-path transition reads the adjacency index.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PathDirection {
    Forward,
    Reverse,
}

/// One edge or epsilon transition in a Thompson-style SPARQL property-path NFA.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields, tag = "kind", rename_all = "snake_case")]
pub enum PathTransitionKind {
    Epsilon,
    Predicate {
        direction: PathDirection,
        predicate_iri: String,
    },
    NegatedPropertySet {
        direction: PathDirection,
        excluded_predicate_iris: Vec<String>,
    },
}

/// Immutable NFA transition with dense state identifiers.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct PathTransition {
    pub from_state: u32,
    pub to_state: u32,
    pub transition: PathTransitionKind,
}

/// Exact automaton compiled from one typed SPARQL 1.1 property-path expression.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DistributedPathAutomaton {
    pub format_version: u32,
    pub state_count: u32,
    pub start_state: u32,
    pub accept_states: Vec<u32>,
    pub transitions: Vec<PathTransition>,
}

/// Immutable distributed execution contract for one property-path occurrence.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DistributedPropertyPathPlan {
    pub path_id: String,
    pub path_ordinal: u32,
    pub graph_scope: String,
    pub subject_pattern: String,
    /// Canonical SPARQL serialization retained for scalar differential checks
    /// and the later typed outer-algebra substitution gate.
    pub path_sparql: String,
    pub object_pattern: String,
    pub automaton: DistributedPathAutomaton,
    pub automaton_sha256: String,
    pub partition_count: u32,
    pub max_iterations: u32,
    pub max_frontier_items: u64,
    pub max_visited_items: u64,
    pub max_checkpoint_bytes: u64,
    pub max_spill_bytes: u64,
    pub hot_vertex_degree: u64,
    pub max_hot_vertex_splits: u32,
    pub require_complete_partition_set: bool,
    pub require_scalar_equivalence: bool,
}

/// Deployment ceilings applied to every compiled property-path occurrence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DistributedPropertyPathLimits {
    pub partition_count: u32,
    pub max_iterations: u32,
    pub max_frontier_items: u64,
    pub max_visited_items: u64,
    pub max_checkpoint_bytes: u64,
    pub max_spill_bytes: u64,
    pub hot_vertex_degree: u64,
    pub max_hot_vertex_splits: u32,
}

/// Fail-closed property-path planning error.
#[derive(Debug, Error, Eq, PartialEq)]
pub enum PropertyPathPlanError {
    #[error("distributed property-path identity or limits are invalid")]
    InvalidIdentity,
    #[error("distributed property-path automaton is invalid")]
    InvalidAutomaton,
}

impl DistributedPropertyPathLimits {
    pub fn validate(self) -> Result<Self, PropertyPathPlanError> {
        if self.partition_count < 2
            || self.max_iterations == 0
            || self.max_frontier_items == 0
            || self.max_visited_items < self.max_frontier_items
            || self.max_checkpoint_bytes == 0
            || self.max_spill_bytes < self.max_checkpoint_bytes
            || self.hot_vertex_degree == 0
            || self.max_hot_vertex_splits < 2
        {
            return Err(PropertyPathPlanError::InvalidIdentity);
        }
        Ok(self)
    }
}

/// Validate the complete property-path plan before any frontier is admitted.
pub fn validate_distributed_property_path_plan(
    plan: &DistributedPropertyPathPlan,
) -> Result<(), PropertyPathPlanError> {
    if plan.path_id.is_empty()
        || plan.graph_scope.is_empty()
        || plan.subject_pattern.is_empty()
        || plan.path_sparql.is_empty()
        || plan.object_pattern.is_empty()
        || !lower_hex_sha256(&plan.automaton_sha256)
        || plan.partition_count < 2
        || plan.max_iterations == 0
        || plan.max_frontier_items == 0
        || plan.max_visited_items < plan.max_frontier_items
        || plan.max_checkpoint_bytes == 0
        || plan.max_spill_bytes < plan.max_checkpoint_bytes
        || plan.hot_vertex_degree == 0
        || plan.max_hot_vertex_splits < 2
        || !plan.require_complete_partition_set
        || !plan.require_scalar_equivalence
    {
        return Err(PropertyPathPlanError::InvalidIdentity);
    }
    let automaton = &plan.automaton;
    if automaton.format_version != 1
        || automaton.state_count == 0
        || automaton.start_state >= automaton.state_count
        || automaton.accept_states.is_empty()
        || automaton
            .accept_states
            .windows(2)
            .any(|pair| pair[0] >= pair[1])
        || automaton
            .accept_states
            .iter()
            .any(|state| *state >= automaton.state_count)
        || automaton.transitions.iter().any(|transition| {
            transition.from_state >= automaton.state_count
                || transition.to_state >= automaton.state_count
                || match &transition.transition {
                    PathTransitionKind::Epsilon => false,
                    PathTransitionKind::Predicate { predicate_iri, .. } => predicate_iri.is_empty(),
                    PathTransitionKind::NegatedPropertySet {
                        excluded_predicate_iris,
                        ..
                    } => {
                        excluded_predicate_iris.is_empty()
                            || excluded_predicate_iris
                                .iter()
                                .any(std::string::String::is_empty)
                    }
                }
        })
    {
        return Err(PropertyPathPlanError::InvalidAutomaton);
    }
    Ok(())
}

impl DistributedAlgebraLimits {
    /// Reject zero or single-partition configurations that cannot establish distributed work.
    pub fn validate(self) -> Result<Self, AlgebraPlanError> {
        if self.partition_count < 2
            || self.max_input_rows == 0
            || self.max_output_rows == 0
            || self.max_exchange_bytes == 0
            || self.max_spill_bytes == 0
        {
            return Err(AlgebraPlanError::InvalidIdentity);
        }
        Ok(self)
    }
}

/// Fail-closed distributed-algebra plan error.
#[derive(Debug, Error, Eq, PartialEq)]
pub enum AlgebraPlanError {
    #[error("distributed algebra plan identity or limits are invalid")]
    InvalidIdentity,
    #[error("distributed algebra plan is not a valid post-order DAG")]
    InvalidDag,
    #[error("distributed algebra stage uses an unsafe execution lane")]
    UnsafeLane,
}

/// Validate the complete Phase 40.13.8 distributed algebra contract.
pub fn validate_distributed_algebra_plan(
    plan: &DistributedAlgebraPlan,
) -> Result<(), AlgebraPlanError> {
    if plan.format_version != 1
        || !lower_hex_sha256(&plan.query_algebra_sha256)
        || plan.root_stage_id.is_empty()
        || plan.stages.is_empty()
        || !plan.require_complete_partition_set
        || !plan.require_scalar_equivalence
    {
        return Err(AlgebraPlanError::InvalidIdentity);
    }
    let mut seen = BTreeSet::new();
    for stage in &plan.stages {
        if stage.stage_id.is_empty()
            || seen.contains(&stage.stage_id)
            || !lower_hex_sha256(&stage.algebra_sha256)
            || stage.partition_count < 2
            || stage.max_input_rows == 0
            || stage.max_output_rows == 0
            || stage.max_exchange_bytes == 0
            || stage.max_spill_bytes == 0
            || stage.inputs.iter().any(|input| !seen.contains(input))
        {
            return Err(AlgebraPlanError::InvalidDag);
        }
        if !lane_is_safe(stage.operator, stage.lane) {
            return Err(AlgebraPlanError::UnsafeLane);
        }
        seen.insert(stage.stage_id.clone());
    }
    if !seen.contains(&plan.root_stage_id)
        || plan.stages.last().map(|stage| stage.stage_id.as_str())
            != Some(plan.root_stage_id.as_str())
    {
        return Err(AlgebraPlanError::InvalidDag);
    }
    Ok(())
}

/// Build deterministic dependency waves for bounded multicore and multinode dispatch.
///
/// Every partition of a ready stage appears in the same logical wave. Runtime concurrency may be
/// lower than the wave width, but the next wave cannot begin until the complete-partition barrier
/// has accepted every work item in the preceding wave.
pub fn algebra_execution_waves(
    plan: &DistributedAlgebraPlan,
) -> Result<Vec<AlgebraExecutionWave>, AlgebraPlanError> {
    validate_distributed_algebra_plan(plan)?;
    let mut completed = BTreeSet::new();
    let mut remaining = plan
        .stages
        .iter()
        .map(|stage| stage.stage_id.clone())
        .collect::<BTreeSet<_>>();
    let by_id = plan
        .stages
        .iter()
        .map(|stage| (stage.stage_id.clone(), stage))
        .collect::<BTreeMap<_, _>>();
    let mut waves = Vec::new();
    while !remaining.is_empty() {
        let ready = remaining
            .iter()
            .filter_map(|stage_id| {
                let stage = by_id.get(stage_id)?;
                stage
                    .inputs
                    .iter()
                    .all(|input| completed.contains(input))
                    .then_some(*stage)
            })
            .collect::<Vec<_>>();
        if ready.is_empty() {
            return Err(AlgebraPlanError::InvalidDag);
        }
        let ordinal = u32::try_from(waves.len()).map_err(|_| AlgebraPlanError::InvalidDag)?;
        let mut work_items = Vec::new();
        for stage in &ready {
            for partition in 0..stage.partition_count {
                work_items.push(AlgebraWorkItem {
                    stage_id: stage.stage_id.clone(),
                    partition,
                    partition_count: stage.partition_count,
                });
            }
        }
        waves.push(AlgebraExecutionWave {
            ordinal,
            work_items,
        });
        for stage in ready {
            remaining.remove(&stage.stage_id);
            completed.insert(stage.stage_id.clone());
        }
    }
    Ok(waves)
}

fn lane_is_safe(operator: DistributedAlgebraOperator, lane: AlgebraExecutionLane) -> bool {
    use AlgebraExecutionLane::{
        ExactReasonerPartitioned, NativePartitioned, ScalarOraclePartitioned,
    };
    use DistributedAlgebraOperator::{
        AskFinalize, Bgp, ConstructFinalize, DescribeFinalize, Distinct, Extend, Filter, Graph,
        Group, Join, Lateral, LeftJoin, Minus, Order, Path, Project, Reduced, Service, Slice,
        Subquery, Union, Values,
    };
    match operator {
        Bgp => matches!(lane, ExactReasonerPartitioned | ScalarOraclePartitioned),
        Join | Union | Minus | Project | Distinct | Reduced | Slice | Values => {
            lane == NativePartitioned
        }
        Path | Lateral | LeftJoin | Filter | Extend | Graph | Group | Order | Subquery
        | Service | AskFinalize | ConstructFinalize | DescribeFinalize => {
            lane == ScalarOraclePartitioned
        }
    }
}

fn lower_hex_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

/// Plan-specific exactness obligation.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CoverageRequirement {
    pub algebra_hash: [u8; 32],
    pub required_operator_hashes: Vec<[u8; 32]>,
    pub exact_fallback_allowed: bool,
}

/// Payload is presentation work, never semantic qualification.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HydrationRequirement {
    pub predicate_ids: Vec<u32>,
    pub include_proofs: bool,
    pub max_payload_bytes: u64,
}

/// Routing errors block planning before any forbidden metadata is exposed.
#[derive(Debug, Error, Eq, PartialEq)]
pub enum RoutingError {
    #[error("query capability is absent from this snapshot: {0:?}")]
    UnknownCapability(QueryCapability),
    #[error("required graph dependency {dependency} is not authorized for graph {source_graph}")]
    ForbiddenDependency { source_graph: u32, dependency: u32 },
    #[error("query selects no graph in the declared dataset")]
    EmptyDataset,
}

/// Resolve capability graphs and their complete dependency closure inside authorization.
pub fn route_relevant_graphs(
    index: &GraphRoutingIndex,
    capabilities: &[QueryCapability],
    declared_dataset: &BTreeSet<u32>,
    authorized_graphs: &BTreeSet<u32>,
) -> Result<BTreeSet<u32>, RoutingError> {
    let allowed = declared_dataset
        .intersection(authorized_graphs)
        .copied()
        .collect::<BTreeSet<_>>();
    if allowed.is_empty() {
        return Err(RoutingError::EmptyDataset);
    }
    let mut selected = BTreeSet::new();
    for capability in capabilities {
        let graphs = index
            .capabilities
            .get(capability)
            .ok_or_else(|| RoutingError::UnknownCapability(capability.clone()))?;
        selected.extend(graphs.intersection(&allowed).copied());
    }
    if selected.is_empty() {
        return Err(RoutingError::EmptyDataset);
    }
    let mut queue = selected.iter().copied().collect::<VecDeque<_>>();
    while let Some(graph) = queue.pop_front() {
        if let Some(dependencies) = index.dependencies.get(&graph) {
            for dependency in dependencies {
                if !allowed.contains(dependency) {
                    return Err(RoutingError::ForbiddenDependency {
                        source_graph: graph,
                        dependency: *dependency,
                    });
                }
                if selected.insert(*dependency) {
                    queue.push_back(*dependency);
                }
            }
        }
    }
    Ok(selected)
}

/// Choose a lossless exchange from measured relation sizes and explicit budgets.
#[must_use]
pub fn choose_exchange(
    left_bytes: u64,
    right_bytes: u64,
    broadcast_budget: u64,
    bloom_budget: u64,
) -> &'static str {
    let smaller = left_bytes.min(right_bytes);
    if smaller <= broadcast_budget {
        "broadcast_exact_keys"
    } else if smaller <= bloom_budget {
        "bloom_then_exact"
    } else {
        "hash_shuffle"
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use super::{
        AlgebraExecutionLane, AlgebraPlanError, DistributedAlgebraOperator, DistributedAlgebraPlan,
        DistributedAlgebraStage, GraphRoutingIndex, QueryCapability, RoutingError,
        route_relevant_graphs, validate_distributed_algebra_plan,
    };

    #[test]
    fn unauthorized_dependency_fails_instead_of_disappearing() {
        let capability = QueryCapability::PropertyIri("urn:p".to_owned());
        let index = GraphRoutingIndex {
            capabilities: BTreeMap::from([(capability.clone(), BTreeSet::from([1]))]),
            dependencies: BTreeMap::from([(1, BTreeSet::from([2]))]),
        };
        assert_eq!(
            route_relevant_graphs(
                &index,
                &[capability],
                &BTreeSet::from([1, 2]),
                &BTreeSet::from([1])
            ),
            Err(RoutingError::ForbiddenDependency {
                source_graph: 1,
                dependency: 2,
            })
        );
    }

    #[test]
    fn unsafe_native_aggregate_is_rejected() {
        let digest = "0".repeat(64);
        let plan = DistributedAlgebraPlan {
            format_version: 1,
            query_algebra_sha256: digest.clone(),
            root_stage_id: "stage-0".to_owned(),
            stages: vec![DistributedAlgebraStage {
                stage_id: "stage-0".to_owned(),
                operator: DistributedAlgebraOperator::Group,
                inputs: vec![],
                lane: AlgebraExecutionLane::NativePartitioned,
                algebra_sha256: digest,
                partition_count: 8,
                max_input_rows: 100,
                max_output_rows: 100,
                max_exchange_bytes: 1024,
                max_spill_bytes: 1024,
            }],
            require_complete_partition_set: true,
            require_scalar_equivalence: true,
        };
        assert_eq!(
            validate_distributed_algebra_plan(&plan),
            Err(AlgebraPlanError::UnsafeLane)
        );
    }
}
