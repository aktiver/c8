//! Exact bounded distributed property-path frontier primitives for Phase 40.13.9.
//!
//! Property paths return endpoint pairs, not one row per route. State therefore includes the
//! originating entity and deduplicates `(origin, entity, automaton_state)` globally. Workers may
//! split a high-degree vertex into deterministic edge shards, but a coordinator may advance or
//! terminate an iteration only after the exact expected work-item set is complete and verified.

use std::collections::{BTreeSet, VecDeque};

use ngkg_query_planner::{
    DistributedPathAutomaton, DistributedPropertyPathPlan, PathDirection, PathTransitionKind,
    validate_distributed_property_path_plan,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::ExecutionError;

/// One directed RDF adjacency edge in the authorized active graph scope.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct PathEdge {
    pub source_entity_id: u64,
    pub predicate_iri: String,
    pub target_entity_id: u64,
    /// Dense graph-term ID from the immutable snapshot dictionary.
    pub graph_id: u64,
}

/// Distributed path state. The origin is required for SPARQL endpoint-set semantics.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct PathFrontierKey {
    pub origin_entity_id: u64,
    pub entity_id: u64,
    pub automaton_state: u32,
    /// `None` is NGKG's authorized union-default graph. `Some` keeps a
    /// `GRAPH <iri>` or `GRAPH ?g` traversal inside one named graph.
    pub graph_id: Option<u64>,
}

/// One deduplicated SPARQL property-path endpoint pair.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct PathEndpoint {
    pub subject_entity_id: u64,
    pub object_entity_id: u64,
    pub graph_id: Option<u64>,
}

/// Stable identity of one vertex or hot-vertex sub-work item.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct PathWorkIdentity {
    pub query_sha256: String,
    pub plan_sha256: String,
    pub path_id: String,
    pub automaton_sha256: String,
    pub iteration: u32,
    pub owner_partition: u32,
    pub partition_count: u32,
    /// Immutable Phase 40.13.12 semantic partition whose adjacency bytes are scanned.
    pub storage_partition: u32,
    pub frontier: PathFrontierKey,
    pub split_index: u32,
    pub split_count: u32,
}

/// Deterministic assignment used by bounded core and pod schedulers.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct PathExpansionWorkItem {
    pub identity: PathWorkIdentity,
}

/// Complete input for one worker-owned adjacency subrange.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct PathExpansionTask {
    pub work: PathExpansionWorkItem,
    pub automaton: DistributedPathAutomaton,
    pub edges: Vec<PathEdge>,
    pub max_frontier_items: u64,
    pub max_visited_items: u64,
}

/// Checksum-bound worker result. Partial responses are never treated as an empty frontier.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct PathExpansionResult {
    pub identity: PathWorkIdentity,
    pub next_frontier: Vec<PathFrontierKey>,
    pub accepting_endpoints: Vec<PathEndpoint>,
    pub scanned_edges: u64,
    pub output_sha256: String,
    pub worker_id: String,
    pub complete: bool,
}

/// Durable, snapshot-externalized state after a globally complete iteration.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct PathCheckpointState {
    pub query_sha256: String,
    pub plan_sha256: String,
    pub path_id: String,
    pub automaton_sha256: String,
    pub completed_iteration: u32,
    pub partition_count: u32,
    pub visited: Vec<PathFrontierKey>,
    pub next_frontier: Vec<PathFrontierKey>,
    pub endpoints: Vec<PathEndpoint>,
    pub terminated: bool,
}

/// Canonical checkpoint bytes and their SHA-256 identity.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct PathCheckpoint {
    pub state: PathCheckpointState,
    pub state_sha256: String,
    pub encoded_bytes: u64,
}

/// Globally merged outcome for one exact complete frontier iteration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PathIterationOutcome {
    pub visited: BTreeSet<PathFrontierKey>,
    pub next_frontier: Vec<PathFrontierKey>,
    pub endpoints: BTreeSet<PathEndpoint>,
    pub terminated: bool,
    pub scanned_edges: u64,
    pub checkpoint: PathCheckpoint,
}

/// Create one origin-preserving initial NFA state per distinct admitted subject entity.
pub fn seed_path_frontier(
    origins: impl IntoIterator<Item = u64>,
    automaton: &DistributedPathAutomaton,
    max_frontier_items: u64,
) -> Result<Vec<PathFrontierKey>, ExecutionError> {
    validate_automaton(automaton)?;
    let origins = origins.into_iter().collect::<BTreeSet<_>>();
    if max_frontier_items == 0
        || u64::try_from(origins.len())
            .ok()
            .is_none_or(|count| count > max_frontier_items)
    {
        return Err(ExecutionError::PropertyPathFrontierLimit);
    }
    Ok(origins
        .into_iter()
        .map(|entity_id| PathFrontierKey {
            origin_entity_id: entity_id,
            entity_id,
            automaton_state: automaton.start_state,
            graph_id: None,
        })
        .collect())
}

/// Create initial states whose named-graph scope has already been authorization checked.
pub fn seed_scoped_path_frontier(
    origins: impl IntoIterator<Item = (u64, Option<u64>)>,
    automaton: &DistributedPathAutomaton,
    max_frontier_items: u64,
) -> Result<Vec<PathFrontierKey>, ExecutionError> {
    validate_automaton(automaton)?;
    let origins = origins.into_iter().collect::<BTreeSet<_>>();
    if max_frontier_items == 0
        || u64::try_from(origins.len())
            .ok()
            .is_none_or(|count| count > max_frontier_items)
    {
        return Err(ExecutionError::PropertyPathFrontierLimit);
    }
    Ok(origins
        .into_iter()
        .map(|(entity_id, graph_id)| PathFrontierKey {
            origin_entity_id: entity_id,
            entity_id,
            automaton_state: automaton.start_state,
            graph_id,
        })
        .collect())
}

/// Stable owner independent of the current replica or node count.
pub fn path_partition_owner(
    query_sha256: &str,
    path_id: &str,
    key: PathFrontierKey,
    partition_count: u32,
) -> Result<u32, ExecutionError> {
    if !lower_hex_sha256(query_sha256) || path_id.is_empty() || partition_count < 2 {
        return Err(ExecutionError::InvalidPropertyPathIdentity);
    }
    let mut hash = Sha256::new();
    hash.update(b"ngkg-property-path-owner-v1\0");
    hash_component(&mut hash, query_sha256.as_bytes())?;
    hash_component(&mut hash, path_id.as_bytes())?;
    hash.update(key.origin_entity_id.to_be_bytes());
    hash.update(key.entity_id.to_be_bytes());
    hash.update(key.automaton_state.to_be_bytes());
    hash.update(key.graph_id.unwrap_or(u64::MAX).to_be_bytes());
    let digest = hash.finalize();
    let prefix = u64::from_be_bytes(
        digest[..8]
            .try_into()
            .map_err(|_| ExecutionError::InvalidPropertyPathIdentity)?,
    );
    u32::try_from(prefix % u64::from(partition_count))
        .map_err(|_| ExecutionError::InvalidPropertyPathIdentity)
}

/// Create deterministic work assignments and split only adjacency-heavy vertices.
pub fn path_expansion_work_items(
    query_sha256: &str,
    plan_sha256: &str,
    plan: &DistributedPropertyPathPlan,
    iteration: u32,
    frontier: &[PathFrontierKey],
    edges: &[PathEdge],
) -> Result<Vec<PathExpansionWorkItem>, ExecutionError> {
    path_partition_expansion_work_items(
        query_sha256,
        plan_sha256,
        plan,
        iteration,
        0,
        frontier,
        edges,
    )
}

/// Create work for one immutable storage partition. Every storage partition
/// must report before the global iteration barrier can advance.
pub fn path_partition_expansion_work_items(
    query_sha256: &str,
    plan_sha256: &str,
    plan: &DistributedPropertyPathPlan,
    iteration: u32,
    storage_partition: u32,
    frontier: &[PathFrontierKey],
    edges: &[PathEdge],
) -> Result<Vec<PathExpansionWorkItem>, ExecutionError> {
    validate_plan(plan)?;
    if automaton_sha256(&plan.automaton)? != plan.automaton_sha256 {
        return Err(ExecutionError::InvalidPropertyPathIdentity);
    }
    if !lower_hex_sha256(query_sha256)
        || !lower_hex_sha256(plan_sha256)
        || iteration >= plan.max_iterations
        || storage_partition >= plan.partition_count
        || u64::try_from(frontier.len()).ok().is_none_or(|count| count > plan.max_frontier_items)
    {
        return Err(ExecutionError::PropertyPathIterationLimit);
    }
    validate_edges(edges)?;
    let unique = frontier.iter().copied().collect::<BTreeSet<_>>();
    if unique.len() != frontier.len() {
        return Err(ExecutionError::InvalidPropertyPathIdentity);
    }
    let mut work = Vec::new();
    for key in unique {
        if key.automaton_state >= plan.automaton.state_count {
            return Err(ExecutionError::InvalidPropertyPathIdentity);
        }
        let degree = incident_degree(key.entity_id, edges)?;
        let required = degree
            .checked_add(plan.hot_vertex_degree.saturating_sub(1))
            .and_then(|value| value.checked_div(plan.hot_vertex_degree))
            .unwrap_or(1)
            .max(1);
        let split_count = u32::try_from(required)
            .unwrap_or(plan.max_hot_vertex_splits)
            .min(plan.max_hot_vertex_splits)
            .max(1);
        let owner_partition =
            path_partition_owner(query_sha256, &plan.path_id, key, plan.partition_count)?;
        for split_index in 0..split_count {
            work.push(PathExpansionWorkItem {
                identity: PathWorkIdentity {
                    query_sha256: query_sha256.to_owned(),
                    plan_sha256: plan_sha256.to_owned(),
                    path_id: plan.path_id.clone(),
                    automaton_sha256: plan.automaton_sha256.clone(),
                    iteration,
                    owner_partition,
                    partition_count: plan.partition_count,
                    storage_partition,
                    frontier: key,
                    split_index,
                    split_count,
                },
            });
        }
    }
    Ok(work)
}

/// Expand one deterministic vertex shard through the property-path NFA.
pub fn expand_path_work_item(
    task: &PathExpansionTask,
    worker_id: &str,
) -> Result<PathExpansionResult, ExecutionError> {
    expand_path_work_item_borrowed(
        &task.work,
        &task.automaton,
        &task.edges,
        task.max_frontier_items,
        task.max_visited_items,
        worker_id,
    )
}

/// Expand a work item without cloning the immutable adjacency slice for every
/// hot-vertex split running inside the worker's bounded CPU pool.
pub fn expand_path_work_item_borrowed(
    work: &PathExpansionWorkItem,
    automaton: &DistributedPathAutomaton,
    edges: &[PathEdge],
    max_frontier_items: u64,
    max_visited_items: u64,
    worker_id: &str,
) -> Result<PathExpansionResult, ExecutionError> {
    validate_work_identity(&work.identity)?;
    validate_automaton(automaton)?;
    if automaton_sha256(automaton)? != work.identity.automaton_sha256 {
        return Err(ExecutionError::InvalidPropertyPathIdentity);
    }
    validate_edges(edges)?;
    if worker_id.is_empty() || max_frontier_items == 0 || max_visited_items == 0 {
        return Err(ExecutionError::InvalidPropertyPathIdentity);
    }
    let identity = &work.identity;
    if path_partition_owner(
        &identity.query_sha256,
        &identity.path_id,
        identity.frontier,
        identity.partition_count,
    )? != identity.owner_partition
    {
        return Err(ExecutionError::InvalidPropertyPathIdentity);
    }

    let closure = epsilon_closure(
        automaton,
        identity.frontier.automaton_state,
    )?;
    let mut next = BTreeSet::new();
    let mut endpoints = BTreeSet::new();
    let mut scanned_edges = 0_u64;
    for state in closure {
        if automaton.accept_states.binary_search(&state).is_ok() {
            endpoints.insert(PathEndpoint {
                subject_entity_id: identity.frontier.origin_entity_id,
                object_entity_id: identity.frontier.entity_id,
                graph_id: identity.frontier.graph_id,
            });
        }
        for transition in automaton
            .transitions
            .iter()
            .filter(|transition| transition.from_state == state)
        {
            let direction = match &transition.transition {
                PathTransitionKind::Predicate { direction, .. }
                | PathTransitionKind::NegatedPropertySet { direction, .. } => *direction,
                PathTransitionKind::Epsilon => continue,
            };
            for edge in edges {
                if !edge_in_split(edge, identity)?
                    || !edge_touches(edge, identity.frontier.entity_id, direction)
                    || identity
                        .frontier
                        .graph_id
                        .is_some_and(|graph_id| graph_id != edge.graph_id)
                {
                    continue;
                }
                scanned_edges = scanned_edges
                    .checked_add(1)
                    .ok_or(ExecutionError::PropertyPathFrontierLimit)?;
                let predicate_matches = match &transition.transition {
                    PathTransitionKind::Predicate { predicate_iri, .. } => {
                        edge.predicate_iri.as_str() == predicate_iri
                    }
                    PathTransitionKind::NegatedPropertySet {
                        excluded_predicate_iris,
                        ..
                    } => excluded_predicate_iris.binary_search(&edge.predicate_iri).is_err(),
                    PathTransitionKind::Epsilon => false,
                };
                if !predicate_matches {
                    continue;
                }
                let entity_id = match direction {
                    PathDirection::Forward => edge.target_entity_id,
                    PathDirection::Reverse => edge.source_entity_id,
                };
                for next_state in epsilon_closure(automaton, transition.to_state)? {
                    let key = PathFrontierKey {
                        origin_entity_id: identity.frontier.origin_entity_id,
                        entity_id,
                        automaton_state: next_state,
                        graph_id: identity.frontier.graph_id,
                    };
                    if has_consuming_transition(automaton, next_state) {
                        next.insert(key);
                    }
                    if automaton.accept_states.binary_search(&next_state).is_ok() {
                        endpoints.insert(PathEndpoint {
                            subject_entity_id: key.origin_entity_id,
                            object_entity_id: key.entity_id,
                            graph_id: key.graph_id,
                        });
                    }
                }
            }
        }
    }
    if u64::try_from(next.len()).ok().is_none_or(|count| count > max_frontier_items)
        || u64::try_from(next.len()).ok().is_none_or(|count| count > max_visited_items)
    {
        return Err(ExecutionError::PropertyPathFrontierLimit);
    }
    let next_frontier = next.into_iter().collect::<Vec<_>>();
    let accepting_endpoints = endpoints.into_iter().collect::<Vec<_>>();
    let output_sha256 = result_sha256(&next_frontier, &accepting_endpoints, scanned_edges)?;
    Ok(PathExpansionResult {
        identity: identity.clone(),
        next_frontier,
        accepting_endpoints,
        scanned_edges,
        output_sha256,
        worker_id: worker_id.to_owned(),
        complete: true,
    })
}

/// Verify all expected work, merge a global visited set and establish exact termination.
#[allow(clippy::too_many_arguments)]
pub fn complete_path_iteration(
    expected_work: &[PathExpansionWorkItem],
    results: Vec<PathExpansionResult>,
    prior_visited: &BTreeSet<PathFrontierKey>,
    prior_endpoints: &BTreeSet<PathEndpoint>,
    max_frontier_items: u64,
    max_visited_items: u64,
    max_checkpoint_bytes: u64,
) -> Result<PathIterationOutcome, ExecutionError> {
    if expected_work.is_empty() || expected_work.len() != results.len() {
        return Err(ExecutionError::InvalidPropertyPathIdentity);
    }
    let reference = &expected_work
        .first()
        .ok_or(ExecutionError::InvalidPropertyPathIdentity)?
        .identity;
    validate_work_identity(reference)?;
    for work in expected_work {
        validate_work_identity(&work.identity)?;
        if work.identity.query_sha256 != reference.query_sha256
            || work.identity.plan_sha256 != reference.plan_sha256
            || work.identity.path_id != reference.path_id
            || work.identity.automaton_sha256 != reference.automaton_sha256
            || work.identity.iteration != reference.iteration
            || work.identity.partition_count != reference.partition_count
        {
            return Err(ExecutionError::InvalidPropertyPathIdentity);
        }
    }
    let expected = expected_work
        .iter()
        .map(|work| work.identity.clone())
        .collect::<BTreeSet<_>>();
    if expected.len() != expected_work.len() {
        return Err(ExecutionError::InvalidPropertyPathIdentity);
    }
    let mut actual = BTreeSet::new();
    let mut candidates = BTreeSet::new();
    let mut endpoints = prior_endpoints.clone();
    let mut scanned_edges = 0_u64;
    for result in results {
        validate_work_identity(&result.identity)?;
        if !result.complete
            || result.worker_id.is_empty()
            || !lower_hex_sha256(&result.output_sha256)
            || result.output_sha256
                != result_sha256(
                    &result.next_frontier,
                    &result.accepting_endpoints,
                    result.scanned_edges,
                )?
            || !actual.insert(result.identity.clone())
        {
            return Err(ExecutionError::InvalidPropertyPathIdentity);
        }
        candidates.extend(result.next_frontier);
        endpoints.extend(result.accepting_endpoints);
        scanned_edges = scanned_edges
            .checked_add(result.scanned_edges)
            .ok_or(ExecutionError::PropertyPathFrontierLimit)?;
    }
    if actual != expected {
        return Err(ExecutionError::InvalidPropertyPathIdentity);
    }
    let mut visited = prior_visited.clone();
    let mut next_frontier = Vec::new();
    for candidate in candidates {
        if visited.insert(candidate) {
            next_frontier.push(candidate);
        }
    }
    if u64::try_from(next_frontier.len())
        .ok()
        .is_none_or(|count| count > max_frontier_items)
    {
        return Err(ExecutionError::PropertyPathFrontierLimit);
    }
    if u64::try_from(visited.len())
        .ok()
        .is_none_or(|count| count > max_visited_items)
    {
        return Err(ExecutionError::PropertyPathVisitedLimit);
    }
    let terminated = next_frontier.is_empty();
    let checkpoint = build_path_checkpoint(
        PathCheckpointState {
            query_sha256: reference.query_sha256.clone(),
            plan_sha256: reference.plan_sha256.clone(),
            path_id: reference.path_id.clone(),
            automaton_sha256: reference.automaton_sha256.clone(),
            completed_iteration: reference.iteration,
            partition_count: reference.partition_count,
            visited: visited.iter().copied().collect(),
            next_frontier: next_frontier.clone(),
            endpoints: endpoints.iter().copied().collect(),
            terminated,
        },
        max_checkpoint_bytes,
    )?;
    Ok(PathIterationOutcome {
        visited,
        next_frontier,
        endpoints,
        terminated,
        scanned_edges,
        checkpoint,
    })
}

/// Build a bounded canonical checkpoint suitable for atomic NVMe/object-store publication.
pub fn build_path_checkpoint(
    state: PathCheckpointState,
    max_checkpoint_bytes: u64,
) -> Result<PathCheckpoint, ExecutionError> {
    validate_checkpoint_state(&state)?;
    let bytes = serde_json::to_vec(&state)
        .map_err(|_| ExecutionError::InvalidPropertyPathIdentity)?;
    let encoded_bytes =
        u64::try_from(bytes.len()).map_err(|_| ExecutionError::PropertyPathCheckpointLimit)?;
    if max_checkpoint_bytes == 0 || encoded_bytes > max_checkpoint_bytes {
        return Err(ExecutionError::PropertyPathCheckpointLimit);
    }
    Ok(PathCheckpoint {
        state,
        state_sha256: hex_encode(&Sha256::digest(bytes)),
        encoded_bytes,
    })
}

/// Validate a checkpoint before resuming a traversal after retry or pod replacement.
pub fn validate_path_checkpoint(
    checkpoint: &PathCheckpoint,
    max_checkpoint_bytes: u64,
) -> Result<(), ExecutionError> {
    validate_checkpoint_state(&checkpoint.state)?;
    let bytes = serde_json::to_vec(&checkpoint.state)
        .map_err(|_| ExecutionError::InvalidPropertyPathIdentity)?;
    let encoded_bytes =
        u64::try_from(bytes.len()).map_err(|_| ExecutionError::PropertyPathCheckpointLimit)?;
    if encoded_bytes != checkpoint.encoded_bytes
        || encoded_bytes > max_checkpoint_bytes
        || hex_encode(&Sha256::digest(bytes)) != checkpoint.state_sha256
    {
        return Err(ExecutionError::PropertyPathCheckpointLimit);
    }
    Ok(())
}

fn validate_plan(plan: &DistributedPropertyPathPlan) -> Result<(), ExecutionError> {
    validate_distributed_property_path_plan(plan)
        .map_err(|_| ExecutionError::InvalidPropertyPathIdentity)
}

fn validate_automaton(automaton: &DistributedPathAutomaton) -> Result<(), ExecutionError> {
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
    {
        return Err(ExecutionError::InvalidPropertyPathIdentity);
    }
    Ok(())
}

fn validate_work_identity(identity: &PathWorkIdentity) -> Result<(), ExecutionError> {
    if !lower_hex_sha256(&identity.query_sha256)
        || !lower_hex_sha256(&identity.plan_sha256)
        || !lower_hex_sha256(&identity.automaton_sha256)
        || identity.path_id.is_empty()
        || identity.partition_count < 2
        || identity.owner_partition >= identity.partition_count
        || identity.storage_partition >= identity.partition_count
        || identity.split_count == 0
        || identity.split_index >= identity.split_count
    {
        return Err(ExecutionError::InvalidPropertyPathIdentity);
    }
    Ok(())
}

fn validate_edges(edges: &[PathEdge]) -> Result<(), ExecutionError> {
    if edges.iter().any(|edge| edge.predicate_iri.is_empty()) {
        return Err(ExecutionError::InvalidPropertyPathIdentity);
    }
    Ok(())
}

fn epsilon_closure(
    automaton: &DistributedPathAutomaton,
    initial: u32,
) -> Result<Vec<u32>, ExecutionError> {
    if initial >= automaton.state_count {
        return Err(ExecutionError::InvalidPropertyPathIdentity);
    }
    let mut closure = BTreeSet::from([initial]);
    let mut queue = VecDeque::from([initial]);
    while let Some(state) = queue.pop_front() {
        for transition in automaton.transitions.iter().filter(|transition| {
            transition.from_state == state
                && matches!(&transition.transition, PathTransitionKind::Epsilon)
        }) {
            if transition.to_state >= automaton.state_count {
                return Err(ExecutionError::InvalidPropertyPathIdentity);
            }
            if closure.insert(transition.to_state) {
                queue.push_back(transition.to_state);
            }
        }
    }
    Ok(closure.into_iter().collect())
}

fn has_consuming_transition(automaton: &DistributedPathAutomaton, state: u32) -> bool {
    automaton.transitions.iter().any(|transition| {
        transition.from_state == state
            && !matches!(&transition.transition, PathTransitionKind::Epsilon)
    })
}

fn incident_degree(entity_id: u64, edges: &[PathEdge]) -> Result<u64, ExecutionError> {
    edges.iter().try_fold(0_u64, |degree, edge| {
        if edge.source_entity_id == entity_id || edge.target_entity_id == entity_id {
            degree
                .checked_add(1)
                .ok_or(ExecutionError::PropertyPathFrontierLimit)
        } else {
            Ok(degree)
        }
    })
}

fn edge_touches(edge: &PathEdge, entity_id: u64, direction: PathDirection) -> bool {
    match direction {
        PathDirection::Forward => edge.source_entity_id == entity_id,
        PathDirection::Reverse => edge.target_entity_id == entity_id,
    }
}

fn edge_in_split(edge: &PathEdge, identity: &PathWorkIdentity) -> Result<bool, ExecutionError> {
    if identity.split_count == 1 {
        return Ok(true);
    }
    let mut hash = Sha256::new();
    hash.update(b"ngkg-property-path-hot-edge-v1\0");
    hash.update(edge.source_entity_id.to_be_bytes());
    hash_component(&mut hash, edge.predicate_iri.as_bytes())?;
    hash.update(edge.target_entity_id.to_be_bytes());
    let digest = hash.finalize();
    let prefix = u64::from_be_bytes(
        digest[..8]
            .try_into()
            .map_err(|_| ExecutionError::InvalidPropertyPathIdentity)?,
    );
    Ok(prefix % u64::from(identity.split_count) == u64::from(identity.split_index))
}

fn result_sha256(
    frontier: &[PathFrontierKey],
    endpoints: &[PathEndpoint],
    scanned_edges: u64,
) -> Result<String, ExecutionError> {
    let mut canonical_frontier = frontier.to_vec();
    canonical_frontier.sort_unstable();
    canonical_frontier.dedup();
    let mut canonical_endpoints = endpoints.to_vec();
    canonical_endpoints.sort_unstable();
    canonical_endpoints.dedup();
    let bytes = serde_json::to_vec(&(canonical_frontier, canonical_endpoints, scanned_edges))
        .map_err(|_| ExecutionError::InvalidPropertyPathIdentity)?;
    Ok(hex_encode(&Sha256::digest(bytes)))
}

fn automaton_sha256(
    automaton: &DistributedPathAutomaton,
) -> Result<String, ExecutionError> {
    let bytes = serde_json::to_vec(automaton)
        .map_err(|_| ExecutionError::InvalidPropertyPathIdentity)?;
    Ok(hex_encode(&Sha256::digest(bytes)))
}

fn validate_checkpoint_state(state: &PathCheckpointState) -> Result<(), ExecutionError> {
    if !lower_hex_sha256(&state.query_sha256)
        || !lower_hex_sha256(&state.plan_sha256)
        || !lower_hex_sha256(&state.automaton_sha256)
        || state.path_id.is_empty()
        || state.partition_count < 2
        || !is_sorted_unique(&state.visited)
        || !is_sorted_unique(&state.next_frontier)
        || !is_sorted_unique(&state.endpoints)
        || state
            .next_frontier
            .iter()
            .any(|key| state.visited.binary_search(key).is_err())
        || state.terminated != state.next_frontier.is_empty()
    {
        return Err(ExecutionError::InvalidPropertyPathIdentity);
    }
    Ok(())
}

fn is_sorted_unique<T: Ord>(values: &[T]) -> bool {
    values.windows(2).all(|pair| pair[0] < pair[1])
}

fn hash_component(hash: &mut Sha256, bytes: &[u8]) -> Result<(), ExecutionError> {
    let length =
        u64::try_from(bytes.len()).map_err(|_| ExecutionError::PropertyPathCheckpointLimit)?;
    hash.update(length.to_be_bytes());
    hash.update(bytes);
    Ok(())
}

fn lower_hex_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use ngkg_query_planner::{
        DistributedPathAutomaton, DistributedPropertyPathPlan, PathDirection, PathTransition,
        PathTransitionKind,
    };

    use super::{
        PathEdge, PathExpansionTask, PathFrontierKey, complete_path_iteration,
        expand_path_work_item, path_expansion_work_items,
    };

    fn digest() -> String {
        "a".repeat(64)
    }

    fn plan() -> Result<DistributedPropertyPathPlan, super::ExecutionError> {
        let automaton = DistributedPathAutomaton {
            format_version: 1,
            state_count: 2,
            start_state: 0,
            accept_states: vec![1],
            transitions: vec![PathTransition {
                from_state: 0,
                to_state: 1,
                transition: PathTransitionKind::Predicate {
                    direction: PathDirection::Forward,
                    predicate_iri: "urn:p".to_owned(),
                },
            }],
        };
        let automaton_sha256 = super::automaton_sha256(&automaton)?;
        Ok(DistributedPropertyPathPlan {
            path_id: "property-path-00000".to_owned(),
            path_ordinal: 0,
            graph_scope: "active-default".to_owned(),
            subject_pattern: "?s".to_owned(),
            path_sparql: "<urn:p>".to_owned(),
            object_pattern: "?o".to_owned(),
            automaton,
            automaton_sha256,
            partition_count: 4,
            max_iterations: 10,
            max_frontier_items: 100,
            max_visited_items: 1_000,
            max_checkpoint_bytes: 1_000_000,
            max_spill_bytes: 2_000_000,
            hot_vertex_degree: 2,
            max_hot_vertex_splits: 8,
            require_complete_partition_set: true,
            require_scalar_equivalence: true,
        })
    }

    #[test]
    fn hot_vertices_split_and_complete_only_after_all_work() -> Result<(), super::ExecutionError> {
        let plan = plan()?;
        let seed = PathFrontierKey {
            origin_entity_id: 1,
            entity_id: 1,
            automaton_state: 0,
            graph_id: None,
        };
        let edges = (2..=6)
            .map(|target| PathEdge {
                source_entity_id: 1,
                predicate_iri: "urn:p".to_owned(),
                target_entity_id: target,
                graph_id: 7,
            })
            .collect::<Vec<_>>();
        let work = path_expansion_work_items(&digest(), &digest(), &plan, 0, &[seed], &edges)?;
        assert_eq!(work.len(), 3);
        let results = work
            .iter()
            .map(|work| {
                expand_path_work_item(
                    &PathExpansionTask {
                        work: work.clone(),
                        automaton: plan.automaton.clone(),
                        edges: edges.clone(),
                        max_frontier_items: 100,
                        max_visited_items: 1_000,
                    },
                    "worker-a",
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        let outcome = complete_path_iteration(
            &work,
            results,
            &BTreeSet::from([seed]),
            &BTreeSet::new(),
            100,
            1_000,
            1_000_000,
        )?;
        assert_eq!(outcome.endpoints.len(), 5);
        assert!(outcome.terminated);
        Ok(())
    }

    #[test]
    fn a_missing_hot_split_cannot_be_declared_complete() -> Result<(), super::ExecutionError> {
        let plan = plan()?;
        let seed = PathFrontierKey {
            origin_entity_id: 1,
            entity_id: 1,
            automaton_state: 0,
            graph_id: None,
        };
        let edges = vec![PathEdge {
            source_entity_id: 1,
            predicate_iri: "urn:p".to_owned(),
            target_entity_id: 2,
            graph_id: 7,
        }];
        let work = path_expansion_work_items(&digest(), &digest(), &plan, 0, &[seed], &edges)?;
        let first = work
            .first()
            .cloned()
            .ok_or(super::ExecutionError::InvalidPropertyPathIdentity)?;
        let result = expand_path_work_item(
            &PathExpansionTask {
                work: first.clone(),
                automaton: plan.automaton,
                edges,
                max_frontier_items: 100,
                max_visited_items: 1_000,
            },
            "worker-a",
        )?;
        let mut duplicated = work.clone();
        duplicated.push(first);
        assert!(complete_path_iteration(
            &duplicated,
            vec![result],
            &BTreeSet::from([seed]),
            &BTreeSet::new(),
            100,
            1_000,
            1_000_000,
        )
        .is_err());
        Ok(())
    }

    #[test]
    fn named_graph_frontier_never_crosses_graphs() -> Result<(), super::ExecutionError> {
        let plan = plan()?;
        let seed = PathFrontierKey {
            origin_entity_id: 1,
            entity_id: 1,
            automaton_state: 0,
            graph_id: Some(7),
        };
        let edges = vec![
            PathEdge {
                source_entity_id: 1,
                predicate_iri: "urn:p".to_owned(),
                target_entity_id: 2,
                graph_id: 7,
            },
            PathEdge {
                source_entity_id: 1,
                predicate_iri: "urn:p".to_owned(),
                target_entity_id: 3,
                graph_id: 8,
            },
        ];
        let work = path_expansion_work_items(&digest(), &digest(), &plan, 0, &[seed], &edges)?;
        let results = work
            .iter()
            .map(|work| {
                expand_path_work_item(
                    &PathExpansionTask {
                        work: work.clone(),
                        automaton: plan.automaton.clone(),
                        edges: edges.clone(),
                        max_frontier_items: 100,
                        max_visited_items: 1_000,
                    },
                    "worker-a",
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        let endpoints = results
            .iter()
            .flat_map(|result| &result.accepting_endpoints)
            .copied()
            .collect::<BTreeSet<_>>();
        assert_eq!(endpoints.len(), 1);
        assert!(endpoints.iter().all(|endpoint| {
            endpoint.object_entity_id == 2 && endpoint.graph_id == Some(7)
        }));
        Ok(())
    }
}
