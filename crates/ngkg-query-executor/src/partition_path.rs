//! Partition-native property-path scans over Phase 40.13.12 adjacency artifacts.
//!
//! Adjacency rows are fixed width and sorted by their first dense RDF-term ID.
//! Workers therefore binary-seek only the vertices in the current frontier. A
//! complete iteration still fans out to every immutable semantic partition;
//! no partition miss is interpreted as an empty answer.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

use ngkg_query_planner::{DistributedPropertyPathPlan, PathDirection, PathTransitionKind};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use super::{
    ExecutionError, PathCheckpoint, PathEdge, PathExpansionResult, PathExpansionWorkItem,
    PathFrontierKey, expand_path_work_item_borrowed,
    path_partition_expansion_work_items, seed_scoped_path_frontier,
};

/// Four 20-digit IDs, three tabs and one newline.
pub const ADJACENCY_RECORD_BYTES: u64 = 84;

/// Checksum and row boundary for one immutable adjacency artifact.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct AdjacencyArtifactIdentity {
    pub path: PathBuf,
    pub sha256: String,
    pub bytes: u64,
    pub rows: u64,
}

/// Named-graph semantics carried by a path frontier.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PathGraphScope {
    UnionDefault,
    Named(u64),
    NamedVariable(BTreeSet<u64>),
}

/// Complete worker output for one storage partition and frontier iteration.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct PartitionPathBatch {
    pub storage_partition: u32,
    pub work: Vec<PathExpansionWorkItem>,
    pub results: Vec<PathExpansionResult>,
    pub seed_frontier: Vec<PathFrontierKey>,
    pub adjacency_rows_read: u64,
    pub hot_split_count: u64,
    pub worker_threads: u32,
    pub complete: bool,
}

/// Partition-native scan failures always abort the global path barrier.
#[derive(Debug, Error)]
pub enum PartitionPathError {
    #[error("partition adjacency I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("partition adjacency identity or row framing is invalid")]
    AdjacencyIdentity,
    #[error("partition dictionary is invalid or incomplete")]
    Dictionary,
    #[error("partition scan exceeded its admitted row ceiling")]
    ScanLimit,
    #[error("partition path execution failed: {0}")]
    Execution(#[from] ExecutionError),
    #[error("partition checkpoint is invalid or exceeds its byte ceiling")]
    Checkpoint,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct AdjacencyRow {
    anchor: u64,
    predicate: u64,
    other: u64,
    graph: u64,
}

/// Verified forward and reverse fixed-record adjacency files.
pub struct PartitionAdjacencyIndex {
    forward: AdjacencyArtifactIdentity,
    reverse: AdjacencyArtifactIdentity,
}

impl PartitionAdjacencyIndex {
    /// Open only real, checksum-valid fixed-record files.
    pub fn open(
        forward: AdjacencyArtifactIdentity,
        reverse: AdjacencyArtifactIdentity,
    ) -> Result<Self, PartitionPathError> {
        validate_artifact(&forward)?;
        validate_artifact(&reverse)?;
        Ok(Self { forward, reverse })
    }

    /// Seed all RDF nodes visible in the selected graph scope. Phase 40.13.17
    /// adjacency includes literal objects, which are valid one-step endpoints.
    pub fn seed_frontier(
        &self,
        plan: &DistributedPropertyPathPlan,
        scope: &PathGraphScope,
        subject_filter: Option<u64>,
        authorized_graphs: &BTreeSet<u64>,
        max_rows_read: u64,
    ) -> Result<(Vec<PathFrontierKey>, u64), PartitionPathError> {
        let mut rows_read = 0_u64;
        let mut origins = BTreeSet::new();
        if let Some(subject) = subject_filter {
            for artifact in [&self.forward, &self.reverse] {
                for row in read_anchor_rows(artifact, subject, max_rows_read, &mut rows_read)? {
                    if let Some(graph) = graph_for_scope(scope, row.graph, authorized_graphs) {
                        origins.insert((subject, graph));
                    }
                }
            }
            return Ok((
                seed_scoped_path_frontier(origins, &plan.automaton, plan.max_frontier_items)?,
                rows_read,
            ));
        }
        let mut reader = BufReader::new(File::open(&self.forward.path)?);
        let mut record = [0_u8; ADJACENCY_RECORD_BYTES as usize];
        loop {
            match reader.read_exact(&mut record) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(error) => return Err(error.into()),
            }
            rows_read = checked_row(rows_read, max_rows_read)?;
            let row = parse_row(&record)?;
            let Some(graph) = graph_for_scope(scope, row.graph, authorized_graphs) else {
                continue;
            };
            for entity in [row.anchor, row.other] {
                if subject_filter.is_none_or(|required| required == entity) {
                    origins.insert((entity, graph));
                }
            }
        }
        Ok((
            seed_scoped_path_frontier(origins, &plan.automaton, plan.max_frontier_items)?,
            rows_read,
        ))
    }

    fn edges(
        &self,
        frontier: &[PathFrontierKey],
        plan: &DistributedPropertyPathPlan,
        authorized_graphs: &BTreeSet<u64>,
        dictionary_path: &Path,
        max_rows_read: u64,
    ) -> Result<(Vec<PathEdge>, u64), PartitionPathError> {
        let needs_forward = plan.automaton.transitions.iter().any(|transition| {
            matches!(
                transition.transition,
                PathTransitionKind::Predicate { direction: PathDirection::Forward, .. }
                    | PathTransitionKind::NegatedPropertySet { direction: PathDirection::Forward, .. }
            )
        });
        let needs_reverse = plan.automaton.transitions.iter().any(|transition| {
            matches!(
                transition.transition,
                PathTransitionKind::Predicate { direction: PathDirection::Reverse, .. }
                    | PathTransitionKind::NegatedPropertySet { direction: PathDirection::Reverse, .. }
            )
        });
        let anchors = frontier.iter().map(|key| key.entity_id).collect::<BTreeSet<_>>();
        let mut indexed = BTreeSet::new();
        let mut rows_read = 0_u64;
        if needs_forward {
            for anchor in &anchors {
                for row in read_anchor_rows(&self.forward, *anchor, max_rows_read, &mut rows_read)? {
                    if authorized_graphs.contains(&row.graph) {
                        indexed.insert((row.anchor, row.predicate, row.other, row.graph));
                    }
                }
            }
        }
        if needs_reverse {
            for anchor in &anchors {
                for row in read_anchor_rows(&self.reverse, *anchor, max_rows_read, &mut rows_read)? {
                    if authorized_graphs.contains(&row.graph) {
                        indexed.insert((row.other, row.predicate, row.anchor, row.graph));
                    }
                }
            }
        }
        let predicate_ids = indexed.iter().map(|edge| edge.1).collect::<BTreeSet<_>>();
        let predicates = lookup_dictionary_terms(dictionary_path, &predicate_ids)?;
        indexed
            .into_iter()
            .map(|(source, predicate, target, graph)| {
                Ok(PathEdge {
                    source_entity_id: source,
                    predicate_iri: predicates
                        .get(&predicate)
                        .and_then(|term| term.strip_prefix("N\t"))
                        .ok_or(PartitionPathError::Dictionary)?
                        .to_owned(),
                    target_entity_id: target,
                    graph_id: graph,
                })
            })
            .collect::<Result<Vec<_>, _>>()
            .map(|edges| (edges, rows_read))
    }
}

fn graph_for_scope(
    scope: &PathGraphScope,
    graph: u64,
    authorized_graphs: &BTreeSet<u64>,
) -> Option<Option<u64>> {
    if !authorized_graphs.contains(&graph) {
        return None;
    }
    match scope {
        PathGraphScope::UnionDefault => Some(None),
        PathGraphScope::Named(required) if *required == graph => Some(Some(graph)),
        PathGraphScope::Named(_) => None,
        PathGraphScope::NamedVariable(graphs) if graphs.contains(&graph) => Some(Some(graph)),
        PathGraphScope::NamedVariable(_) => None,
    }
}

/// Execute all hot splits for one immutable storage partition across a bounded
/// number of native Rust threads.
#[allow(clippy::too_many_arguments)]
pub fn execute_partition_path_batch(
    index: &PartitionAdjacencyIndex,
    dictionary_path: &Path,
    query_sha256: &str,
    plan_sha256: &str,
    plan: &DistributedPropertyPathPlan,
    iteration: u32,
    storage_partition: u32,
    frontier: &[PathFrontierKey],
    authorized_graphs: &BTreeSet<u64>,
    max_rows_read: u64,
    max_work_items: usize,
    worker_threads: usize,
    worker_id: &str,
) -> Result<PartitionPathBatch, PartitionPathError> {
    if worker_threads == 0 || worker_id.is_empty() {
        return Err(PartitionPathError::AdjacencyIdentity);
    }
    let (edges, rows_read) = index.edges(
        frontier,
        plan,
        authorized_graphs,
        dictionary_path,
        max_rows_read,
    )?;
    let work = path_partition_expansion_work_items(
        query_sha256,
        plan_sha256,
        plan,
        iteration,
        storage_partition,
        frontier,
        &edges,
    )?;
    if work.len() > max_work_items {
        return Err(PartitionPathError::ScanLimit);
    }
    let lanes = worker_threads.min(work.len()).max(1);
    let chunk = work.len().div_ceil(lanes);
    let results = std::thread::scope(|scope| {
        let automaton = &plan.automaton;
        let shared_edges = &edges;
        let max_frontier_items = plan.max_frontier_items;
        let max_visited_items = plan.max_visited_items;
        let handles = work
            .chunks(chunk)
            .map(|items| {
                scope.spawn(move || {
                    items
                        .iter()
                        .map(|item| {
                            expand_path_work_item_borrowed(
                                item,
                                automaton,
                                shared_edges,
                                max_frontier_items,
                                max_visited_items,
                                worker_id,
                            )
                        })
                        .collect::<Result<Vec<_>, _>>()
                })
            })
            .collect::<Vec<_>>();
        handles
            .into_iter()
            .map(|handle| {
                handle
                    .join()
                    .map_err(|_| PartitionPathError::AdjacencyIdentity)?
                    .map_err(PartitionPathError::Execution)
            })
            .collect::<Result<Vec<_>, PartitionPathError>>()
    })?
    .into_iter()
    .flatten()
    .collect::<Vec<_>>();
    let hot_split_count = work
        .iter()
        .filter(|item| item.identity.split_count > 1)
        .count()
        .try_into()
        .map_err(|_| PartitionPathError::ScanLimit)?;
    Ok(PartitionPathBatch {
        storage_partition,
        work,
        results,
        seed_frontier: Vec::new(),
        adjacency_rows_read: rows_read,
        hot_split_count,
        worker_threads: u32::try_from(lanes).map_err(|_| PartitionPathError::ScanLimit)?,
        complete: true,
    })
}

/// Resolve a bounded set of canonical dictionary terms without retaining the
/// complete enterprise dictionary in RAM.
pub fn lookup_dictionary_ids(
    path: &Path,
    terms: &BTreeSet<String>,
) -> Result<BTreeMap<String, u64>, PartitionPathError> {
    let mut output = BTreeMap::new();
    if terms.is_empty() {
        return Ok(output);
    }
    for line in BufReader::new(File::open(path)?).lines() {
        let line = line?;
        let (id, term) = line.split_once('\t').ok_or(PartitionPathError::Dictionary)?;
        if terms.contains(term) {
            output.insert(
                term.to_owned(),
                id.parse::<u64>().map_err(|_| PartitionPathError::Dictionary)?,
            );
            if output.len() == terms.len() {
                break;
            }
        }
    }
    if output.len() != terms.len() {
        return Err(PartitionPathError::Dictionary);
    }
    Ok(output)
}

/// Resolve the subset of terms present in a snapshot dictionary. This is used
/// for authorized logical graphs that may legitimately contain zero triples
/// and therefore have no dense dictionary term in the compiled snapshot.
pub fn lookup_dictionary_ids_available(
    path: &Path,
    terms: &BTreeSet<String>,
) -> Result<BTreeMap<String, u64>, PartitionPathError> {
    let mut output = BTreeMap::new();
    if terms.is_empty() {
        return Ok(output);
    }
    for line in BufReader::new(File::open(path)?).lines() {
        let line = line?;
        let (id, term) = line.split_once('\t').ok_or(PartitionPathError::Dictionary)?;
        if terms.contains(term) {
            output.insert(
                term.to_owned(),
                id.parse::<u64>().map_err(|_| PartitionPathError::Dictionary)?,
            );
            if output.len() == terms.len() {
                break;
            }
        }
    }
    Ok(output)
}

/// Resolve one term when present. Absence is a valid empty RDF match, whereas
/// malformed dictionary rows still fail closed.
pub fn lookup_dictionary_id_optional(
    path: &Path,
    required: &str,
) -> Result<Option<u64>, PartitionPathError> {
    for line in BufReader::new(File::open(path)?).lines() {
        let line = line?;
        let (id, term) = line.split_once('\t').ok_or(PartitionPathError::Dictionary)?;
        if term == required {
            return id
                .parse::<u64>()
                .map(Some)
                .map_err(|_| PartitionPathError::Dictionary);
        }
    }
    Ok(None)
}

/// Resolve endpoint/predicate IDs by streaming the immutable dictionary once.
pub fn lookup_dictionary_terms(
    path: &Path,
    ids: &BTreeSet<u64>,
) -> Result<BTreeMap<u64, String>, PartitionPathError> {
    let mut output = BTreeMap::new();
    if ids.is_empty() {
        return Ok(output);
    }
    for line in BufReader::new(File::open(path)?).lines() {
        let line = line?;
        let (id, term) = line.split_once('\t').ok_or(PartitionPathError::Dictionary)?;
        let id = id.parse::<u64>().map_err(|_| PartitionPathError::Dictionary)?;
        if ids.contains(&id) {
            output.insert(id, term.to_owned());
            if output.len() == ids.len() {
                break;
            }
        }
    }
    if output.len() != ids.len() {
        return Err(PartitionPathError::Dictionary);
    }
    Ok(output)
}

/// Atomically persist one checksum-bound checkpoint on marker-owned local
/// NVMe. The caller may then publish the same immutable bytes to object storage.
pub fn write_checkpoint_atomic(
    root: &Path,
    checkpoint: &PathCheckpoint,
    max_bytes: u64,
) -> Result<PathBuf, PartitionPathError> {
    let bytes = serde_json::to_vec(checkpoint).map_err(|_| PartitionPathError::Checkpoint)?;
    // `checkpoint.encoded_bytes` binds the inner state; the persisted envelope
    // also includes the digest and therefore has its own independent ceiling.
    if u64::try_from(bytes.len())
        .ok()
        .is_none_or(|size| size == 0 || size > max_bytes)
    {
        return Err(PartitionPathError::Checkpoint);
    }
    fs::create_dir_all(root)?;
    let final_path = root.join(format!(
        "{}-{:08}-{}.json",
        checkpoint.state.path_id,
        checkpoint.state.completed_iteration,
        checkpoint.state_sha256
    ));
    if final_path.exists() {
        if sha256_file(&final_path)? == hex_encode(&Sha256::digest(&bytes)) {
            return Ok(final_path);
        }
        return Err(PartitionPathError::Checkpoint);
    }
    let temporary = root.join(format!(".{}.partial", checkpoint.state_sha256));
    let mut file = OpenOptions::new().create_new(true).write(true).open(&temporary)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    fs::rename(&temporary, &final_path)?;
    File::open(root)?.sync_all()?;
    Ok(final_path)
}

fn validate_artifact(artifact: &AdjacencyArtifactIdentity) -> Result<(), PartitionPathError> {
    let metadata = fs::symlink_metadata(&artifact.path)?;
    if metadata.file_type().is_symlink()
        || !metadata.is_file()
        || metadata.len() != artifact.bytes
        || artifact.bytes != artifact.rows.saturating_mul(ADJACENCY_RECORD_BYTES)
        || !lower_hex_sha256(&artifact.sha256)
        || sha256_file(&artifact.path)? != artifact.sha256
    {
        return Err(PartitionPathError::AdjacencyIdentity);
    }
    Ok(())
}

fn read_anchor_rows(
    artifact: &AdjacencyArtifactIdentity,
    anchor: u64,
    maximum: u64,
    total: &mut u64,
) -> Result<Vec<AdjacencyRow>, PartitionPathError> {
    let mut reader = File::open(&artifact.path)?;
    let mut low = 0_u64;
    let mut high = artifact.rows;
    let mut record = [0_u8; ADJACENCY_RECORD_BYTES as usize];
    while low < high {
        let middle = low + (high - low) / 2;
        read_record(&mut reader, middle, &mut record)?;
        *total = checked_row(*total, maximum)?;
        if parse_row(&record)?.anchor < anchor {
            low = middle + 1;
        } else {
            high = middle;
        }
    }
    let mut rows = Vec::new();
    while low < artifact.rows {
        read_record(&mut reader, low, &mut record)?;
        let row = parse_row(&record)?;
        *total = checked_row(*total, maximum)?;
        if row.anchor != anchor {
            break;
        }
        rows.push(row);
        low += 1;
    }
    Ok(rows)
}

fn read_record(
    reader: &mut File,
    index: u64,
    record: &mut [u8; ADJACENCY_RECORD_BYTES as usize],
) -> Result<(), PartitionPathError> {
    reader.seek(SeekFrom::Start(
        index
            .checked_mul(ADJACENCY_RECORD_BYTES)
            .ok_or(PartitionPathError::AdjacencyIdentity)?,
    ))?;
    reader.read_exact(record)?;
    Ok(())
}

fn parse_row(record: &[u8; ADJACENCY_RECORD_BYTES as usize]) -> Result<AdjacencyRow, PartitionPathError> {
    if record[20] != b'\t' || record[41] != b'\t' || record[62] != b'\t' || record[83] != b'\n' {
        return Err(PartitionPathError::AdjacencyIdentity);
    }
    Ok(AdjacencyRow {
        anchor: parse_id(&record[0..20])?,
        predicate: parse_id(&record[21..41])?,
        other: parse_id(&record[42..62])?,
        graph: parse_id(&record[63..83])?,
    })
}

fn parse_id(bytes: &[u8]) -> Result<u64, PartitionPathError> {
    std::str::from_utf8(bytes)
        .map_err(|_| PartitionPathError::AdjacencyIdentity)?
        .parse::<u64>()
        .map_err(|_| PartitionPathError::AdjacencyIdentity)
}

fn checked_row(current: u64, maximum: u64) -> Result<u64, PartitionPathError> {
    current
        .checked_add(1)
        .filter(|next| *next <= maximum)
        .ok_or(PartitionPathError::ScanLimit)
}

fn sha256_file(path: &Path) -> Result<String, PartitionPathError> {
    let mut reader = BufReader::new(File::open(path)?);
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 128 * 1024];
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hex_encode(&hasher.finalize()))
}

fn lower_hex_sha256(value: &str) -> bool {
    value.len() == 64
        && value.bytes().all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
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
    use std::{collections::BTreeSet, fs};

    use ngkg_query_planner::{
        DistributedPathAutomaton, DistributedPropertyPathPlan, PathDirection, PathTransition,
        PathTransitionKind,
    };
    use sha2::Digest;
    use uuid::Uuid;

    use super::{
        ADJACENCY_RECORD_BYTES, AdjacencyArtifactIdentity, PartitionAdjacencyIndex,
        PathGraphScope, execute_partition_path_batch, hex_encode, sha256_file,
    };
    use crate::PathFrontierKey;

    fn plan() -> Result<DistributedPropertyPathPlan, Box<dyn std::error::Error>> {
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
        let automaton_sha256 = hex_encode(&sha2::Sha256::digest(
            serde_json::to_vec(&automaton)?,
        ));
        Ok(DistributedPropertyPathPlan {
            path_id: "property-path-00000".to_owned(),
            path_ordinal: 0,
            graph_scope: "active-default".to_owned(),
            subject_pattern: "?s".to_owned(),
            path_sparql: "<urn:p>".to_owned(),
            object_pattern: "?o".to_owned(),
            automaton,
            automaton_sha256,
            partition_count: 2,
            max_iterations: 8,
            max_frontier_items: 100,
            max_visited_items: 1_000,
            max_checkpoint_bytes: 1_000_000,
            max_spill_bytes: 2_000_000,
            hot_vertex_degree: 1,
            max_hot_vertex_splits: 8,
            require_complete_partition_set: true,
            require_scalar_equivalence: true,
        })
    }

    fn row(anchor: u64, predicate: u64, other: u64, graph: u64) -> String {
        format!("{anchor:020}\t{predicate:020}\t{other:020}\t{graph:020}\n")
    }

    fn identity(path: std::path::PathBuf, rows: u64) -> Result<AdjacencyArtifactIdentity, Box<dyn std::error::Error>> {
        Ok(AdjacencyArtifactIdentity {
            sha256: sha256_file(&path)?,
            bytes: rows * ADJACENCY_RECORD_BYTES,
            rows,
            path,
        })
    }

    #[test]
    fn partition_scan_binary_seeks_and_hot_splits_literal_endpoints(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let root = std::env::temp_dir().join(format!("ngkg-path-{}", Uuid::new_v4()));
        fs::create_dir_all(&root)?;
        let forward = root.join("forward.tsv");
        let reverse = root.join("reverse.tsv");
        let dictionary = root.join("dictionary.tsv");
        fs::write(&forward, format!("{}{}", row(2, 1, 3, 0), row(2, 1, 4, 0)))?;
        fs::write(&reverse, format!("{}{}", row(3, 1, 2, 0), row(4, 1, 2, 0)))?;
        fs::write(
            &dictionary,
            "0\tN\turn:g\n1\tN\turn:p\n2\tN\turn:a\n3\tN\turn:b\n4\tL\t\"literal\"\n",
        )?;
        let index = PartitionAdjacencyIndex::open(
            identity(forward, 2)?,
            identity(reverse, 2)?,
        )?;
        let plan = plan()?;
        let allowed = BTreeSet::from([0]);
        let (seed, rows) = index.seed_frontier(
            &plan,
            &PathGraphScope::UnionDefault,
            Some(2),
            &allowed,
            10,
        )?;
        assert!((2..=10).contains(&rows));
        assert_eq!(seed.len(), 1);
        let batch = execute_partition_path_batch(
            &index,
            &dictionary,
            &"a".repeat(64),
            &"b".repeat(64),
            &plan,
            0,
            0,
            &[PathFrontierKey {
                origin_entity_id: 2,
                entity_id: 2,
                automaton_state: 0,
                graph_id: None,
            }],
            &allowed,
            10,
            100,
            2,
            "worker-a",
        )?;
        assert!((2..=10).contains(&batch.adjacency_rows_read));
        assert_eq!(batch.work.len(), 2);
        assert_eq!(batch.results.iter().map(|result| result.accepting_endpoints.len()).sum::<usize>(), 2);
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn corrupt_adjacency_checksum_fails_closed() -> Result<(), Box<dyn std::error::Error>> {
        let root = std::env::temp_dir().join(format!("ngkg-path-{}", Uuid::new_v4()));
        fs::create_dir_all(&root)?;
        let path = root.join("adjacency.tsv");
        fs::write(&path, row(1, 2, 3, 4))?;
        let mut artifact = identity(path.clone(), 1)?;
        artifact.sha256 = "0".repeat(64);
        assert!(PartitionAdjacencyIndex::open(artifact.clone(), artifact).is_err());
        fs::remove_dir_all(root)?;
        Ok(())
    }
}
