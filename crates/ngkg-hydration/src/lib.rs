//! Policy-bounded contextual assembly and direct payload hydration planning.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs::File,
    io::{BufReader, Read},
    path::{Path, PathBuf},
};

use arrow_array::{Array, FixedSizeBinaryArray, RecordBatch, StringArray, UInt8Array, UInt64Array};
use ngkg_locator::{
    LocatorClient, LocatorError, LocatorKey, MmapLocatorIndex, PhysicalRange, ShardLocatorRecord,
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use uuid::Uuid;

/// Required and optional context rules.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ContextPolicy {
    pub policy_id: String,
    pub include_query_edges: bool,
    pub include_proof_edges: bool,
    pub include_named_graph_identity: bool,
    pub include_provenance: bool,
    pub allowed_optional_predicate_ids: BTreeSet<u32>,
    pub max_optional_entities: u64,
    pub max_optional_edges: u64,
    pub allowed_hydration_predicate_ids: BTreeSet<u32>,
    pub max_payload_bytes: u64,
    pub allow_optional_context_truncation: bool,
}

/// One semantically qualified key with its original solution ordinal.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct QualifiedKey {
    pub query_ordinal: u64,
    pub key: LocatorKey,
    pub multiplicity: u64,
}

/// Compact result handed to the representation-only hydration plane.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DehydratedSemanticResult {
    pub snapshot_id: Uuid,
    pub plan_id: String,
    pub coverage_certificate_id: String,
    pub qualified_keys: Vec<QualifiedKey>,
    pub query_edge_ids: Vec<u64>,
    pub named_graph_ids: Vec<u32>,
    pub proof_ids: Vec<u64>,
    pub requested_predicate_ids: BTreeSet<u32>,
}

/// Range lookup retains every key/ordinal association before I/O coalescing.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ResolvedHydrationRange {
    pub query_ordinal: u64,
    pub multiplicity: u64,
    pub key: LocatorKey,
    pub range: PhysicalRange,
}

/// Immutable, bounded plan for hydration workers.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct HydrationPlan {
    pub snapshot_id: Uuid,
    pub required_columns: BTreeSet<u32>,
    pub grouped_ranges: BTreeMap<(String, u32, u32), Vec<ResolvedHydrationRange>>,
}

/// Context/hydration failures cannot change answer eligibility.
#[derive(Debug, Error)]
pub enum HydrationError {
    #[error("requested hydration predicate {0} is not authorized by context policy")]
    PredicateForbidden(u32),
    #[error("semantically qualified key is missing from the locator: {0:?}")]
    QualifiedKeyMissing(LocatorKey),
    #[error("locator returned a different snapshot")]
    SnapshotMismatch,
    #[error("locator failed: {0}")]
    Locator(#[from] LocatorError),
    #[error("hydration request has no qualified keys")]
    EmptyResult,
    #[error("payload shard {0} is absent from the immutable serving root")]
    MissingPayloadShard(u32),
    #[error("payload shard checksum does not match the immutable serving root")]
    PayloadChecksumMismatch,
    #[error("payload row does not match its locator or snapshot")]
    InvalidPayloadRow,
    #[error("hydration row budget of {maximum} would be exceeded by {requested} rows")]
    RowBudgetExceeded { requested: u64, maximum: u64 },
    #[error("hydration worker thread count must be positive")]
    InvalidWorkerCount,
    #[error("a hydration worker terminated unexpectedly")]
    WorkerTerminated,
    #[error("payload I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("Parquet hydration failed: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
    #[error("Arrow hydration failed: {0}")]
    Arrow(#[from] arrow_schema::ArrowError),
    #[error("distributed serving-root contract is invalid")]
    InvalidServingRoot,
}

/// Phase 19 immutable serving-root contract version.
pub const SERVING_ROOT_FORMAT_VERSION: u32 = 1;

/// Exact payload object owned by one logical artifact partition.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ServingPayloadPartition {
    /// Dense logical partition index.
    pub partition_index: u32,
    /// Phase 17 partition manifest object key.
    pub manifest_object_key: String,
    /// Phase 17 partition manifest SHA-256.
    pub manifest_sha256: String,
    /// Exact payload Parquet object key.
    pub payload_object_key: String,
    /// Exact payload Parquet SHA-256.
    pub payload_sha256: String,
    /// Compressed Parquet bytes.
    pub payload_bytes: u64,
    /// Payload rows addressed by the global locator.
    pub payload_row_count: u64,
}

/// Immutable bill of materials consumed by locator and hydration replicas.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ServingRootManifest {
    /// Contract version.
    pub format_version: u32,
    /// Dataset identity.
    pub dataset_id: Uuid,
    /// Snapshot identity.
    pub snapshot_id: Uuid,
    /// Phase 17 artifact-root object key.
    pub artifact_root_object_key: String,
    /// Phase 17 artifact-root SHA-256.
    pub artifact_root_sha256: String,
    /// Dense dictionary object key.
    pub dictionary_object_key: String,
    /// Dense dictionary SHA-256.
    pub dictionary_sha256: String,
    /// Canonical TSV locator object key.
    pub source_locator_object_key: String,
    /// Canonical TSV locator SHA-256.
    pub source_locator_sha256: String,
    /// Fixed-width binary locator object key.
    pub binary_locator_object_key: String,
    /// Fixed-width binary locator SHA-256.
    pub binary_locator_sha256: String,
    /// Topology-independent semantic content root.
    pub semantic_content_sha256: String,
    /// Immutable Parquet row-group size.
    pub row_group_rows: u32,
    /// Binary locator record count.
    pub locator_record_count: u64,
    /// Every logical payload partition exactly once.
    pub partitions: Vec<ServingPayloadPartition>,
}

/// One certified query's reference-versus-sharded hydration comparison.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ServingQueryEquivalence {
    /// Stable certified query identity.
    pub query_id: String,
    /// Exact certified query SHA-256.
    pub query_sha256: String,
    /// Canonical reference hydration rows.
    pub reference_row_count: u64,
    /// Canonical sharded hydration rows.
    pub sharded_row_count: u64,
    /// SHA-256 of the canonical hydration multiset.
    pub canonical_rows_sha256: String,
}

/// Publication evidence proving the serving root matches the reference path.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ServingEquivalenceReport {
    /// Report contract version.
    pub format_version: u32,
    /// Dataset identity.
    pub dataset_id: Uuid,
    /// Snapshot identity.
    pub snapshot_id: Uuid,
    /// Certified serving-root SHA-256.
    pub serving_root_sha256: String,
    /// Certified binary locator SHA-256.
    pub binary_locator_sha256: String,
    /// Exact query comparisons in query-ID order.
    pub queries: Vec<ServingQueryEquivalence>,
    /// True only when every comparison passed.
    pub equivalent: bool,
}

impl ServingRootManifest {
    /// Reject incomplete, non-dense, mismatched, or malformed serving roots.
    pub fn validate(&self) -> Result<(), HydrationError> {
        if self.format_version != SERVING_ROOT_FORMAT_VERSION
            || self.artifact_root_object_key.is_empty()
            || self.dictionary_object_key.is_empty()
            || self.source_locator_object_key.is_empty()
            || self.binary_locator_object_key.is_empty()
            || self.partitions.is_empty()
            || self.row_group_rows == 0
            || !is_sha256(&self.artifact_root_sha256)
            || !is_sha256(&self.dictionary_sha256)
            || !is_sha256(&self.source_locator_sha256)
            || !is_sha256(&self.binary_locator_sha256)
            || !is_sha256(&self.semantic_content_sha256)
        {
            return Err(HydrationError::InvalidServingRoot);
        }
        let mut rows = 0_u64;
        for (expected, partition) in self.partitions.iter().enumerate() {
            if usize::try_from(partition.partition_index).ok() != Some(expected)
                || partition.manifest_object_key.is_empty()
                || partition.payload_object_key.is_empty()
                || !is_sha256(&partition.manifest_sha256)
                || !is_sha256(&partition.payload_sha256)
            {
                return Err(HydrationError::InvalidServingRoot);
            }
            rows = rows
                .checked_add(partition.payload_row_count)
                .ok_or(HydrationError::InvalidServingRoot)?;
        }
        if rows != self.locator_record_count {
            return Err(HydrationError::InvalidServingRoot);
        }
        Ok(())
    }
}

/// One checksum-qualified immutable payload shard cached on a hydration node.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VerifiedPayloadShard {
    partition_index: u32,
    path: PathBuf,
    sha256: [u8; 32],
    byte_count: u64,
}

/// One semantic result key whose payload must be reconstructed without scanning.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ShardedQualifiedGuid {
    /// Stable ordinal of the semantic solution row.
    pub query_ordinal: u64,
    /// Qualified entity to resolve through the locator.
    pub entity_guid: Uuid,
    /// SPARQL bag multiplicity carried without physical duplication.
    pub multiplicity: u64,
}

/// Public RDF resource kind retained by the payload plane.
///
/// The physical dictionary and GUID are lookup aids only. They never change a
/// blank node into an IRI in a hydrated response.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RdfResourceKind {
    /// Absolute IRI RDF term.
    NamedNode,
    /// Dataset-scoped RDF blank-node term.
    BlankNode,
}

impl RdfResourceKind {
    const fn from_code(value: u8) -> Option<Self> {
        match value {
            1 => Some(Self::NamedNode),
            2 => Some(Self::BlankNode),
            _ => None,
        }
    }
}

/// Logical RDF graph kind retained independently from the dense graph ID.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RdfGraphScope {
    /// The physical source default graph.
    Default,
    /// An RDF named graph.
    Named,
}

impl RdfGraphScope {
    const fn from_code(value: u8) -> Option<Self> {
        match value {
            0 => Some(Self::Default),
            1 => Some(Self::Named),
            _ => None,
        }
    }
}

/// One hydrated payload value with its semantic solution identity preserved.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HydratedShardRow {
    /// Stable ordinal of the semantic solution row.
    pub query_ordinal: u64,
    /// SPARQL bag multiplicity.
    pub multiplicity: u64,
    /// Qualified subject identity.
    pub entity_guid: Uuid,
    /// Exact public RDF lexical term (`IRI` text or `_:` blank-node label).
    pub subject_term: String,
    /// RDF term kind retained independently from the GUID.
    pub subject_resource_kind: RdfResourceKind,
    /// Payload artifact partition.
    pub partition_index: u32,
    /// Parquet row group.
    pub row_group: u32,
    /// Row offset within the Parquet row group.
    pub row_in_group: u32,
    /// Dense predicate dictionary ID.
    pub predicate_id: u64,
    /// Dense named-graph dictionary ID.
    pub graph_id: u64,
    /// Logical default-versus-named graph identity.
    pub graph_scope: RdfGraphScope,
    /// Literal lexical form.
    pub lexical_value: String,
    /// Literal datatype IRI.
    pub datatype_iri: String,
    /// Optional normalized language tag.
    pub language: Option<String>,
}

#[derive(Clone, Copy, Debug)]
struct PendingRow {
    qualified: ShardedQualifiedGuid,
    locator: ShardLocatorRecord,
}

type HydrationGroup = ((u32, u32), Vec<PendingRow>);

/// Verify an immutable payload shard once before admitting it to the node cache.
pub fn verify_payload_shard(
    partition_index: u32,
    path: &Path,
    expected_sha256: &str,
) -> Result<VerifiedPayloadShard, HydrationError> {
    let expected = decode_sha256(expected_sha256)?;
    let observed = sha256_path(path)?;
    if observed != expected {
        return Err(HydrationError::PayloadChecksumMismatch);
    }
    Ok(VerifiedPayloadShard {
        partition_index,
        path: path.to_owned(),
        sha256: observed,
        byte_count: path.metadata()?.len(),
    })
}

impl VerifiedPayloadShard {
    /// Logical artifact partition served by this file.
    #[must_use]
    pub const fn partition_index(&self) -> u32 {
        self.partition_index
    }

    /// Immutable local cache path.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Content checksum committed by the serving root.
    #[must_use]
    pub const fn sha256(&self) -> [u8; 32] {
        self.sha256
    }

    /// Compressed Parquet size used for admission and cache accounting.
    #[must_use]
    pub const fn byte_count(&self) -> u64 {
        self.byte_count
    }
}

/// Directly hydrate exact Parquet row groups in bounded parallel worker lanes.
///
/// The locator lookup happens before any Parquet file is opened. Work is then
/// partitioned by payload shard and row group, so each lane reads only exact
/// immutable objects and the operating-system page cache can reuse hot groups.
pub fn hydrate_sharded_payload(
    locator: &MmapLocatorIndex,
    expected_snapshot_id: Uuid,
    qualified: &[ShardedQualifiedGuid],
    shards: &BTreeMap<u32, VerifiedPayloadShard>,
    worker_threads: usize,
    max_rows: u64,
) -> Result<Vec<HydratedShardRow>, HydrationError> {
    hydrate_sharded_payload_internal(
        locator,
        expected_snapshot_id,
        qualified,
        shards,
        worker_threads,
        max_rows,
        None,
    )
}

/// Hydrate only locator records whose graph IDs are authorized for the caller.
///
/// Filtering occurs before payload shard selection and before row-budget
/// accounting. Consequently, a caller cannot make an unauthorized graph consume
/// I/O, worker slots, or the authorized response budget.
pub fn hydrate_sharded_payload_for_graphs(
    locator: &MmapLocatorIndex,
    expected_snapshot_id: Uuid,
    qualified: &[ShardedQualifiedGuid],
    shards: &BTreeMap<u32, VerifiedPayloadShard>,
    worker_threads: usize,
    max_rows: u64,
    authorized_graph_ids: &BTreeSet<u64>,
) -> Result<Vec<HydratedShardRow>, HydrationError> {
    if authorized_graph_ids.is_empty() {
        return Err(HydrationError::EmptyResult);
    }
    hydrate_sharded_payload_internal(
        locator,
        expected_snapshot_id,
        qualified,
        shards,
        worker_threads,
        max_rows,
        Some(authorized_graph_ids),
    )
}

fn hydrate_sharded_payload_internal(
    locator: &MmapLocatorIndex,
    expected_snapshot_id: Uuid,
    qualified: &[ShardedQualifiedGuid],
    shards: &BTreeMap<u32, VerifiedPayloadShard>,
    worker_threads: usize,
    max_rows: u64,
    authorized_graph_ids: Option<&BTreeSet<u64>>,
) -> Result<Vec<HydratedShardRow>, HydrationError> {
    if worker_threads == 0 {
        return Err(HydrationError::InvalidWorkerCount);
    }
    if locator.snapshot_id() != expected_snapshot_id {
        return Err(HydrationError::SnapshotMismatch);
    }
    if qualified.is_empty() {
        return Err(HydrationError::EmptyResult);
    }
    let mut grouped: BTreeMap<(u32, u32), Vec<PendingRow>> = BTreeMap::new();
    let mut requested = 0_u64;
    for value in qualified {
        let mut records = locator.lookup(value.entity_guid)?;
        if records.is_empty() {
            return Err(HydrationError::QualifiedKeyMissing(LocatorKey::Entity(
                value.entity_guid,
            )));
        }
        if let Some(authorized) = authorized_graph_ids {
            records.retain(|record| authorized.contains(&record.graph_id));
            // Do not reveal whether an otherwise valid key existed only in a
            // graph outside the caller's authorization set.
            if records.is_empty() {
                return Err(HydrationError::QualifiedKeyMissing(LocatorKey::Entity(
                    value.entity_guid,
                )));
            }
        }
        requested = requested
            .checked_add(u64::try_from(records.len()).map_err(|_| {
                HydrationError::RowBudgetExceeded {
                    requested: u64::MAX,
                    maximum: max_rows,
                }
            })?)
            .ok_or(HydrationError::RowBudgetExceeded {
                requested: u64::MAX,
                maximum: max_rows,
            })?;
        if requested > max_rows {
            return Err(HydrationError::RowBudgetExceeded {
                requested,
                maximum: max_rows,
            });
        }
        for record in records {
            if !shards.contains_key(&record.partition_index) {
                return Err(HydrationError::MissingPayloadShard(record.partition_index));
            }
            grouped
                .entry((record.partition_index, record.row_group))
                .or_default()
                .push(PendingRow {
                    qualified: *value,
                    locator: record,
                });
        }
    }
    let groups = grouped.into_iter().collect::<Vec<_>>();
    let lane_count = worker_threads.min(groups.len()).max(1);
    let mut lanes = vec![Vec::<HydrationGroup>::new(); lane_count];
    for (index, group) in groups.into_iter().enumerate() {
        lanes[index % lane_count].push(group);
    }
    let lane_results = std::thread::scope(|scope| {
        let mut handles = Vec::with_capacity(lane_count);
        for lane in lanes {
            handles.push(
                scope.spawn(move || process_hydration_lane(lane, shards, expected_snapshot_id)),
            );
        }
        handles
            .into_iter()
            .map(|handle| {
                handle
                    .join()
                    .map_err(|_| HydrationError::WorkerTerminated)?
            })
            .collect::<Result<Vec<_>, HydrationError>>()
    })?;
    let mut output = lane_results.into_iter().flatten().collect::<Vec<_>>();
    output.sort_unstable_by_key(|row| {
        (
            row.query_ordinal,
            row.entity_guid,
            row.partition_index,
            row.row_group,
            row.row_in_group,
            row.predicate_id,
            row.graph_id,
        )
    });
    Ok(output)
}

/// Resolve all qualified keys to exact ranges and preserve ordinals/multiplicity.
pub async fn plan_hydration<C: LocatorClient>(
    client: &C,
    result: &DehydratedSemanticResult,
    policy: &ContextPolicy,
) -> Result<HydrationPlan, HydrationError> {
    if result.qualified_keys.is_empty() {
        return Err(HydrationError::EmptyResult);
    }
    if let Some(predicate) = result
        .requested_predicate_ids
        .difference(&policy.allowed_hydration_predicate_ids)
        .next()
    {
        return Err(HydrationError::PredicateForbidden(*predicate));
    }
    let keys = result
        .qualified_keys
        .iter()
        .map(|qualified| qualified.key)
        .collect::<Vec<_>>();
    let entries = client.lookup_batch(result.snapshot_id, &keys).await?;
    let mut grouped: BTreeMap<(String, u32, u32), Vec<ResolvedHydrationRange>> = BTreeMap::new();
    for qualified in &result.qualified_keys {
        let entry = entries
            .get(&qualified.key)
            .ok_or(HydrationError::QualifiedKeyMissing(qualified.key))?;
        if entry.snapshot_id != result.snapshot_id {
            return Err(HydrationError::SnapshotMismatch);
        }
        for range in &entry.ranges {
            grouped
                .entry((
                    range.object_uri.clone(),
                    range.row_group,
                    range.column_mask_id,
                ))
                .or_default()
                .push(ResolvedHydrationRange {
                    query_ordinal: qualified.query_ordinal,
                    multiplicity: qualified.multiplicity,
                    key: qualified.key,
                    range: range.clone(),
                });
        }
    }
    for ranges in grouped.values_mut() {
        ranges.sort_by_key(|value| (value.range.first_row, value.query_ordinal));
    }
    Ok(HydrationPlan {
        snapshot_id: result.snapshot_id,
        required_columns: result.requested_predicate_ids.clone(),
        grouped_ranges: grouped,
    })
}

/// Deterministically budget optional context without touching required edges.
#[must_use]
pub fn truncate_optional_context<T: Ord>(mut optional: Vec<T>, limit: u64) -> (Vec<T>, bool) {
    optional.sort_unstable();
    let requested = optional.len();
    let limit = usize::try_from(limit).unwrap_or(usize::MAX);
    optional.truncate(limit);
    let truncated = optional.len() < requested;
    (optional, truncated)
}

fn process_hydration_lane(
    groups: Vec<HydrationGroup>,
    shards: &BTreeMap<u32, VerifiedPayloadShard>,
    expected_snapshot_id: Uuid,
) -> Result<Vec<HydratedShardRow>, HydrationError> {
    let mut output = Vec::new();
    for ((partition_index, row_group), mut pending) in groups {
        pending.sort_unstable_by_key(|row| (row.locator.row_in_group, row.qualified.query_ordinal));
        let shard = shards
            .get(&partition_index)
            .ok_or(HydrationError::MissingPayloadShard(partition_index))?;
        if shard.partition_index != partition_index {
            return Err(HydrationError::MissingPayloadShard(partition_index));
        }
        let file = File::open(&shard.path)?;
        let mut reader = ParquetRecordBatchReaderBuilder::try_new(file)?
            .with_row_groups(vec![
                usize::try_from(row_group).map_err(|_| HydrationError::InvalidPayloadRow)?,
            ])
            .build()?;
        let mut pending_index = 0_usize;
        let mut row_offset = 0_u32;
        for batch in &mut reader {
            let batch = batch?;
            let batch_rows =
                u32::try_from(batch.num_rows()).map_err(|_| HydrationError::InvalidPayloadRow)?;
            let batch_end = row_offset
                .checked_add(batch_rows)
                .ok_or(HydrationError::InvalidPayloadRow)?;
            while pending_index < pending.len()
                && pending[pending_index].locator.row_in_group < batch_end
            {
                let wanted = pending[pending_index];
                if wanted.locator.row_in_group < row_offset {
                    return Err(HydrationError::InvalidPayloadRow);
                }
                let row = usize::try_from(wanted.locator.row_in_group - row_offset)
                    .map_err(|_| HydrationError::InvalidPayloadRow)?;
                output.push(payload_row(&batch, row, wanted, expected_snapshot_id)?);
                pending_index += 1;
            }
            row_offset = batch_end;
        }
        if pending_index != pending.len() {
            return Err(HydrationError::InvalidPayloadRow);
        }
    }
    Ok(output)
}

fn payload_row(
    batch: &RecordBatch,
    row: usize,
    wanted: PendingRow,
    expected_snapshot_id: Uuid,
) -> Result<HydratedShardRow, HydrationError> {
    if row >= batch.num_rows() {
        return Err(HydrationError::InvalidPayloadRow);
    }
    let subject = fixed_binary_column(batch, "subject_guid128")?;
    let subject_term = string_column(batch, "subject_term")?;
    let subject_resource_kind = u8_column(batch, "subject_resource_kind")?;
    let snapshot = fixed_binary_column(batch, "snapshot_id128")?;
    let predicate = u64_column(batch, "predicate_id64")?;
    let graph = u64_column(batch, "graph_id64")?;
    let graph_scope = u8_column(batch, "graph_scope")?;
    let lexical = string_column(batch, "lexical_value")?;
    let datatype = string_column(batch, "datatype_iri")?;
    let language = string_column(batch, "language")?;
    let entity_guid =
        Uuid::from_slice(subject.value(row)).map_err(|_| HydrationError::InvalidPayloadRow)?;
    let snapshot_id =
        Uuid::from_slice(snapshot.value(row)).map_err(|_| HydrationError::InvalidPayloadRow)?;
    let subject_term = subject_term.value(row);
    let subject_resource_kind = RdfResourceKind::from_code(subject_resource_kind.value(row))
        .ok_or(HydrationError::InvalidPayloadRow)?;
    let graph_scope = RdfGraphScope::from_code(graph_scope.value(row))
        .ok_or(HydrationError::InvalidPayloadRow)?;
    let subject_term_is_valid = match subject_resource_kind {
        RdfResourceKind::NamedNode => !subject_term.is_empty() && !subject_term.starts_with("_:"),
        RdfResourceKind::BlankNode => subject_term
            .strip_prefix("_:")
            .is_some_and(|label| !label.is_empty()),
    };
    if entity_guid != wanted.qualified.entity_guid
        || snapshot_id != expected_snapshot_id
        || predicate.value(row) != wanted.locator.predicate_id
        || graph.value(row) != wanted.locator.graph_id
        || !subject_term_is_valid
    {
        return Err(HydrationError::InvalidPayloadRow);
    }
    Ok(HydratedShardRow {
        query_ordinal: wanted.qualified.query_ordinal,
        multiplicity: wanted.qualified.multiplicity,
        entity_guid,
        subject_term: subject_term.to_owned(),
        subject_resource_kind,
        partition_index: wanted.locator.partition_index,
        row_group: wanted.locator.row_group,
        row_in_group: wanted.locator.row_in_group,
        predicate_id: wanted.locator.predicate_id,
        graph_id: wanted.locator.graph_id,
        graph_scope,
        lexical_value: lexical.value(row).to_owned(),
        datatype_iri: datatype.value(row).to_owned(),
        language: (!language.is_null(row)).then(|| language.value(row).to_owned()),
    })
}

fn fixed_binary_column<'a>(
    batch: &'a RecordBatch,
    name: &'static str,
) -> Result<&'a FixedSizeBinaryArray, HydrationError> {
    batch
        .column_by_name(name)
        .and_then(|column| column.as_any().downcast_ref::<FixedSizeBinaryArray>())
        .ok_or(HydrationError::InvalidPayloadRow)
}

fn u64_column<'a>(
    batch: &'a RecordBatch,
    name: &'static str,
) -> Result<&'a UInt64Array, HydrationError> {
    batch
        .column_by_name(name)
        .and_then(|column| column.as_any().downcast_ref::<UInt64Array>())
        .ok_or(HydrationError::InvalidPayloadRow)
}

fn u8_column<'a>(
    batch: &'a RecordBatch,
    name: &'static str,
) -> Result<&'a UInt8Array, HydrationError> {
    batch
        .column_by_name(name)
        .and_then(|column| column.as_any().downcast_ref::<UInt8Array>())
        .ok_or(HydrationError::InvalidPayloadRow)
}

fn string_column<'a>(
    batch: &'a RecordBatch,
    name: &'static str,
) -> Result<&'a StringArray, HydrationError> {
    batch
        .column_by_name(name)
        .and_then(|column| column.as_any().downcast_ref::<StringArray>())
        .ok_or(HydrationError::InvalidPayloadRow)
}

fn decode_sha256(value: &str) -> Result<[u8; 32], HydrationError> {
    if !is_sha256(value) {
        return Err(HydrationError::PayloadChecksumMismatch);
    }
    hex::decode(value)
        .map_err(|_| HydrationError::PayloadChecksumMismatch)?
        .try_into()
        .map_err(|_| HydrationError::PayloadChecksumMismatch)
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

fn sha256_path(path: &Path) -> Result<[u8; 32], HydrationError> {
    let mut hasher = Sha256::new();
    let mut reader = BufReader::new(File::open(path)?);
    let mut block = [0_u8; 1024 * 1024];
    loop {
        let read = reader.read(&mut block)?;
        if read == 0 {
            break;
        }
        hasher.update(&block[..read]);
    }
    Ok(hasher.finalize().into())
}

#[cfg(test)]
mod sharded_tests {
    use std::{
        collections::{BTreeMap, BTreeSet},
        fs,
        fs::File,
        sync::Arc,
    };

    use arrow_array::{
        ArrayRef, RecordBatch, StringArray, UInt8Array, UInt64Array,
        builder::FixedSizeBinaryBuilder,
    };
    use arrow_schema::{DataType, Field, Schema};
    use ngkg_locator::{MmapLocatorIndex, compile_sharded_locator};
    use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
    use sha2::{Digest, Sha256};
    use uuid::Uuid;

    use super::{
        RdfGraphScope, RdfResourceKind, SERVING_ROOT_FORMAT_VERSION, ServingPayloadPartition,
        ServingRootManifest, ShardedQualifiedGuid, hydrate_sharded_payload,
        hydrate_sharded_payload_for_graphs, verify_payload_shard,
    };

    #[test]
    fn serving_root_requires_dense_complete_payload_partitions() {
        let hash = "a".repeat(64);
        let mut root = ServingRootManifest {
            format_version: SERVING_ROOT_FORMAT_VERSION,
            dataset_id: Uuid::from_u128(1),
            snapshot_id: Uuid::from_u128(2),
            artifact_root_object_key: "distributed/artifact-root.json".to_owned(),
            artifact_root_sha256: hash.clone(),
            dictionary_object_key: "distributed/dictionary.tsv".to_owned(),
            dictionary_sha256: hash.clone(),
            source_locator_object_key: "distributed/locator.tsv".to_owned(),
            source_locator_sha256: hash.clone(),
            binary_locator_object_key: "distributed/locator.bin".to_owned(),
            binary_locator_sha256: hash.clone(),
            semantic_content_sha256: hash.clone(),
            row_group_rows: 1,
            locator_record_count: 3,
            partitions: vec![ServingPayloadPartition {
                partition_index: 0,
                manifest_object_key: "distributed/partition-0/manifest.json".to_owned(),
                manifest_sha256: hash.clone(),
                payload_object_key: "distributed/partition-0/payload.parquet".to_owned(),
                payload_sha256: hash,
                payload_bytes: 64,
                payload_row_count: 3,
            }],
        };
        assert!(root.validate().is_ok());
        root.partitions[0].partition_index = 1;
        assert!(root.validate().is_err());
        root.partitions.clear();
        root.locator_record_count = 0;
        assert!(root.validate().is_err());
    }

    #[test]
    fn direct_lookup_reads_only_qualified_rows() -> Result<(), Box<dyn std::error::Error>> {
        let root = std::env::temp_dir().join(format!("ngkg-hydration-{}", Uuid::new_v4()));
        fs::create_dir(&root)?;
        let snapshot = Uuid::from_u128(9);
        let entity = Uuid::from_u128(11);
        let payload = root.join("payload.parquet");
        let locator_tsv = root.join("locator.tsv");
        let locator_binary = root.join("locator.bin");
        write_payload(&payload, snapshot, entity)?;
        let locator_text = format!(
            "{}\t00000\t0000000000\t0000000000\t00000000000000000031\t00000000000000000021\n{}\t00000\t0000000001\t0000000000\t00000000000000000032\t00000000000000000022\n",
            hex::encode(entity.as_bytes()),
            hex::encode(entity.as_bytes()),
        );
        fs::write(&locator_tsv, locator_text.as_bytes())?;
        let locator_sha = hex::encode(Sha256::digest(locator_text.as_bytes()));
        assert_eq!(
            compile_sharded_locator(&locator_tsv, &locator_sha, snapshot, &locator_binary)?,
            2
        );
        let binary_sha = hex::encode(Sha256::digest(fs::read(&locator_binary)?));
        let index = MmapLocatorIndex::open(&locator_binary, &binary_sha, snapshot, &locator_sha)?;
        let payload_sha = hex::encode(Sha256::digest(fs::read(&payload)?));
        let shard = verify_payload_shard(0, &payload, &payload_sha)?;
        let shards = BTreeMap::from([(0, shard)]);
        let rows = hydrate_sharded_payload(
            &index,
            snapshot,
            &[ShardedQualifiedGuid {
                query_ordinal: 4,
                entity_guid: entity,
                multiplicity: 3,
            }],
            &shards,
            2,
            10,
        )?;
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].lexical_value, "alpha");
        assert_eq!(rows[1].lexical_value, "beta");
        assert_eq!(rows[0].subject_term, "https://example.test/entity");
        assert_eq!(rows[0].subject_resource_kind, RdfResourceKind::NamedNode);
        assert_eq!(rows[0].graph_scope, RdfGraphScope::Named);
        assert!(
            rows.iter()
                .all(|row| row.query_ordinal == 4 && row.multiplicity == 3)
        );

        let authorized = BTreeSet::from([31_u64]);
        let authorized_rows = hydrate_sharded_payload_for_graphs(
            &index,
            snapshot,
            &[ShardedQualifiedGuid {
                query_ordinal: 4,
                entity_guid: entity,
                multiplicity: 3,
            }],
            &shards,
            2,
            10,
            &authorized,
        )?;
        assert_eq!(authorized_rows.len(), 1);
        assert_eq!(authorized_rows[0].graph_id, 31);
        let _ = fs::remove_dir_all(root);
        Ok(())
    }

    fn write_payload(
        path: &std::path::Path,
        snapshot: Uuid,
        entity: Uuid,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("subject_guid128", DataType::FixedSizeBinary(16), false),
            Field::new("subject_term", DataType::Utf8, false),
            Field::new("subject_resource_kind", DataType::UInt8, false),
            Field::new("predicate_id64", DataType::UInt64, false),
            Field::new("graph_id64", DataType::UInt64, false),
            Field::new("graph_scope", DataType::UInt8, false),
            Field::new("lexical_value", DataType::Utf8, false),
            Field::new("datatype_iri", DataType::Utf8, false),
            Field::new("language", DataType::Utf8, true),
            Field::new("snapshot_id128", DataType::FixedSizeBinary(16), false),
        ]));
        let mut subjects = FixedSizeBinaryBuilder::new(16);
        subjects.append_value(entity.as_bytes())?;
        subjects.append_value(entity.as_bytes())?;
        let mut snapshots = FixedSizeBinaryBuilder::new(16);
        snapshots.append_value(snapshot.as_bytes())?;
        snapshots.append_value(snapshot.as_bytes())?;
        let columns: Vec<ArrayRef> = vec![
            Arc::new(subjects.finish()),
            Arc::new(StringArray::from(vec![
                "https://example.test/entity",
                "https://example.test/entity",
            ])),
            Arc::new(UInt8Array::from(vec![1_u8, 1_u8])),
            Arc::new(UInt64Array::from(vec![21, 22])),
            Arc::new(UInt64Array::from(vec![31, 32])),
            Arc::new(UInt8Array::from(vec![1_u8, 1_u8])),
            Arc::new(StringArray::from(vec!["alpha", "beta"])),
            Arc::new(StringArray::from(vec!["urn:datatype", "urn:datatype"])),
            Arc::new(StringArray::from(vec![None::<&str>, Some("en")])),
            Arc::new(snapshots.finish()),
        ];
        let batch = RecordBatch::try_new(schema.clone(), columns)?;
        let properties = WriterProperties::builder()
            .set_max_row_group_size(1)
            .build();
        let mut writer = ArrowWriter::try_new(File::create(path)?, schema, Some(properties))?;
        writer.write(&batch)?;
        writer.close()?;
        Ok(())
    }
}
