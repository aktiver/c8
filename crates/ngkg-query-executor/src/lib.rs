//! Exact encoded binding, semijoin and property-frontier primitives.

mod distributed_algebra;
mod distributed_path;
mod partition_path;

pub use distributed_algebra::{
    AlgebraPartitionIdentity, AlgebraPartitionResult, NativeAlgebraTask,
    complete_algebra_partition_set, distinct_sparql_json, execute_native_algebra_task,
    global_slice_sparql_json, group_owned_partitions, left_join_sparql_json,
    merge_ordered_partitions_by, minus_sparql_json, union_sparql_json,
};
pub use distributed_path::{
    PathCheckpoint, PathCheckpointState, PathEdge, PathEndpoint, PathExpansionResult,
    PathExpansionTask, PathExpansionWorkItem, PathFrontierKey, PathIterationOutcome,
    PathWorkIdentity, build_path_checkpoint, complete_path_iteration, expand_path_work_item,
    expand_path_work_item_borrowed, path_expansion_work_items, path_partition_expansion_work_items,
    path_partition_owner, seed_path_frontier, seed_scoped_path_frontier, validate_path_checkpoint,
};
pub use partition_path::{
    AdjacencyArtifactIdentity, PartitionAdjacencyIndex, PartitionPathBatch, PartitionPathError,
    PathGraphScope, execute_partition_path_batch, lookup_dictionary_id_optional,
    lookup_dictionary_ids, lookup_dictionary_ids_available, lookup_dictionary_terms,
    write_checkpoint_atomic,
};

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    io::{Cursor, Read, Write},
    sync::Arc,
};

use arrow_array::{Array, ArrayRef, RecordBatch, StringArray, UInt8Array};
use arrow_ipc::{reader::StreamReader, writer::StreamWriter};
use arrow_schema::{DataType, Field, Schema};
use serde::Serialize;
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use thiserror::Error;
use uuid::Uuid;

/// Media type used by the authenticated Phase 23 fragment data plane.
pub const ARROW_STREAM_MEDIA_TYPE: &str = "application/vnd.apache.arrow.stream";

const ARROW_BINDING_FORMAT_VERSION: &str = "ngkg.fragment-bindings.v1";
const META_FORMAT: &str = "ngkg.format";
const META_DATASET_ID: &str = "ngkg.dataset-id";
const META_SNAPSHOT_ID: &str = "ngkg.snapshot-id";
const META_QUERY_SHA256: &str = "ngkg.query-sha256";
const META_FRAGMENT_ID: &str = "ngkg.fragment-id";
const META_WORKER_ID: &str = "ngkg.worker-id";
const META_MULTISET_SHA256: &str = "ngkg.multiset-sha256";
const META_VARIABLE_COUNT: &str = "ngkg.variable-count";
/// Required Arrow IPC stream end marker used by bounded HTTP spooling.
pub const ARROW_STREAM_EOS: [u8; 8] = [0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0];
const SHUFFLE_FORMAT_VERSION: &str = "ngkg.shuffle-join.v2";

/// Encoded RDF term preserves term family instead of treating all IDs as entities.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum EncodedTerm {
    Entity(u64),
    Literal(u64),
    Blank(u64),
}

/// Compact distributed solution row with explicit bag multiplicity.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct BindingRow {
    pub query_ordinal: u64,
    pub values: BTreeMap<u16, EncodedTerm>,
    pub multiplicity: u64,
    pub named_graph_id: Option<u32>,
    pub proof_ids: Vec<u64>,
}

/// Property-path state is entity plus automaton state.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct FrontierKey {
    pub entity_id: u64,
    pub automaton_state: u32,
}

/// One exact distributed frontier iteration.
#[derive(Clone, Debug, Serialize)]
pub struct FrontierBatch {
    pub query_id: String,
    pub iteration: u32,
    pub owner_partition: u32,
    pub keys: Vec<FrontierKey>,
    pub proof_path_refs: Vec<u64>,
}

/// Execution failures do not permit partial streams to become empty results.
#[derive(Debug, Error, Eq, PartialEq)]
pub enum ExecutionError {
    #[error("join multiplicity overflow")]
    MultiplicityOverflow,
    #[error("binding rows disagree on shared variable {0}")]
    IncompatibleBinding(u16),
    #[error("SPARQL JSON binding is not an object")]
    InvalidSparqlBinding,
    #[error("distributed intermediate row ceiling exceeded")]
    IntermediateRowLimit,
    #[error("Arrow IPC fragment stream is invalid: {0}")]
    InvalidArrowStream(String),
    #[error("Arrow IPC fragment metadata is invalid: {0}")]
    InvalidArrowMetadata(String),
    #[error("SPARQL JSON term is invalid: {0}")]
    InvalidSparqlTerm(String),
    #[error("shuffle partition count must be positive")]
    InvalidPartitionCount,
    #[error("shuffle join key must contain at least one variable")]
    EmptyShuffleKey,
    #[error("shuffle join key {0} is unbound")]
    UnboundShuffleKey(String),
    #[error("SPARQL algebra operator is not safe for the native partition lane")]
    UnsafeNativeAlgebraOperator,
    #[error("distributed property-path identity or checksum is invalid")]
    InvalidPropertyPathIdentity,
    #[error("distributed property-path frontier ceiling exceeded")]
    PropertyPathFrontierLimit,
    #[error("distributed property-path visited-set ceiling exceeded")]
    PropertyPathVisitedLimit,
    #[error("distributed property-path iteration ceiling exceeded")]
    PropertyPathIterationLimit,
    #[error("distributed property-path checkpoint ceiling exceeded")]
    PropertyPathCheckpointLimit,
}

/// Snapshot- and certificate-bound metadata carried in Arrow schema metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FragmentBatchMetadata {
    /// Tenant-scoped dataset whose immutable fragment was executed.
    pub dataset_id: Uuid,
    /// Published snapshot that owns the plan and closure.
    pub snapshot_id: Uuid,
    /// SHA-256 of the exact certified query bytes.
    pub query_sha256: String,
    /// Fragment identifier from the immutable distributed plan.
    pub fragment_id: String,
    /// Distinct serving worker identity.
    pub worker_id: String,
    /// Canonical multiset checksum certified offline for this fragment.
    pub multiset_sha256: String,
}

/// A decoded Arrow fragment stream with exact SPARQL solution bindings.
#[derive(Clone, Debug, PartialEq)]
pub struct FragmentBindingBatch {
    /// Stream-level immutable identity and certificate fields.
    pub metadata: FragmentBatchMetadata,
    /// Ordered SPARQL result variables represented by the column groups.
    pub head: Vec<String>,
    /// Exact decoded SPARQL JSON bindings with bag order preserved.
    pub bindings: Vec<Value>,
}

/// Incremental decoder for one certified fragment response.
///
/// Only the current Arrow record batch is retained. Callers can therefore
/// validate or route rows directly from a file or network spool without first
/// constructing a complete encoded response buffer.
pub struct FragmentBindingStream<R: Read> {
    reader: StreamReader<R>,
    metadata: FragmentBatchMetadata,
    head: Vec<String>,
    current_batch: Option<RecordBatch>,
    current_row: usize,
    decoded_rows: usize,
    max_rows: usize,
    terminal: bool,
}

/// Identity and ownership of one partitioned shuffle-join request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ShuffleJoinMetadata {
    /// Dataset that owns the certified distributed plan.
    pub dataset_id: Uuid,
    /// Immutable published snapshot.
    pub snapshot_id: Uuid,
    /// Exact certified query hash.
    pub query_sha256: String,
    /// Checksum of the immutable distributed query plan.
    pub plan_sha256: String,
    /// Unique coordinator-generated request identity.
    pub request_id: Uuid,
    /// Zero-based sequential join stage.
    pub stage: u32,
    /// Hash partition owned by this request.
    pub partition: u32,
    /// Total stable partition count for the stage.
    pub partition_count: u32,
}

/// Decoded left and right relations for one exact shuffle partition.
#[derive(Clone, Debug, PartialEq)]
pub struct ShuffleJoinInput {
    /// Snapshot, plan, stage and partition identity.
    pub metadata: ShuffleJoinMetadata,
    /// Ordered variables in the left relation.
    pub left_head: Vec<String>,
    /// Ordered variables in the right relation.
    pub right_head: Vec<String>,
    /// Exact shared variables used to assign both relations.
    pub key_variables: Vec<String>,
    /// Left SPARQL bag rows belonging to the partition.
    pub left_bindings: Vec<Value>,
    /// Right SPARQL bag rows belonging to the partition.
    pub right_bindings: Vec<Value>,
}

/// Validated immutable header for a streaming shuffle request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ShuffleJoinStreamHeader {
    /// Snapshot, plan, stage and partition identity.
    pub metadata: ShuffleJoinMetadata,
    /// Ordered variables in the left relation.
    pub left_head: Vec<String>,
    /// Ordered variables in the right relation.
    pub right_head: Vec<String>,
    /// Exact shared variables used for primary ownership.
    pub key_variables: Vec<String>,
    /// Declared and subsequently verified left bag row count.
    pub left_row_count: u64,
    /// Declared and subsequently verified right bag row count.
    pub right_row_count: u64,
}

/// Incremental Arrow IPC decoder that never retains a complete relation.
pub struct ShuffleJoinStream<R: Read> {
    reader: StreamReader<R>,
    header: ShuffleJoinStreamHeader,
    union_head: Vec<String>,
    current_batch: Option<RecordBatch>,
    current_row: usize,
    left_rows: u64,
    right_rows: u64,
    seen_right: bool,
    terminal: bool,
}

/// Encode one certified fragment result as a bounded Arrow IPC stream.
///
/// Every SPARQL variable becomes four typed columns (term kind, lexical value,
/// datatype IRI, language). Unbound values are represented by four nulls. This
/// avoids per-row JSON objects on the wire while preserving RDF term identity
/// and SPARQL bag order exactly.
///
/// # Errors
///
/// Returns an error for invalid certificate metadata, invalid RDF JSON terms,
/// an empty/duplicate head, a zero batch bound, Arrow failures, or a bounded
/// writer that refuses additional bytes.
pub fn write_fragment_arrow_stream(
    output: &mut impl Write,
    metadata: &FragmentBatchMetadata,
    head: &[String],
    bindings: &[Value],
    max_batch_rows: usize,
) -> Result<(), ExecutionError> {
    validate_fragment_metadata(metadata)?;
    validate_head(head)?;
    if max_batch_rows == 0 {
        return Err(ExecutionError::InvalidArrowMetadata(
            "Arrow batch row ceiling must be positive".to_owned(),
        ));
    }
    let mut fields = Vec::with_capacity(head.len().saturating_mul(4));
    for (ordinal, _) in head.iter().enumerate() {
        let prefix = format!("v{ordinal:04}");
        fields.extend([
            Field::new(format!("{prefix}.kind"), DataType::UInt8, true),
            Field::new(format!("{prefix}.value"), DataType::Utf8, true),
            Field::new(format!("{prefix}.datatype"), DataType::Utf8, true),
            Field::new(format!("{prefix}.language"), DataType::Utf8, true),
        ]);
    }
    let mut schema_metadata = HashMap::from([
        (
            META_FORMAT.to_owned(),
            ARROW_BINDING_FORMAT_VERSION.to_owned(),
        ),
        (META_DATASET_ID.to_owned(), metadata.dataset_id.to_string()),
        (
            META_SNAPSHOT_ID.to_owned(),
            metadata.snapshot_id.to_string(),
        ),
        (META_QUERY_SHA256.to_owned(), metadata.query_sha256.clone()),
        (META_FRAGMENT_ID.to_owned(), metadata.fragment_id.clone()),
        (META_WORKER_ID.to_owned(), metadata.worker_id.clone()),
        (
            META_MULTISET_SHA256.to_owned(),
            metadata.multiset_sha256.clone(),
        ),
        (META_VARIABLE_COUNT.to_owned(), head.len().to_string()),
    ]);
    for (ordinal, variable) in head.iter().enumerate() {
        schema_metadata.insert(format!("ngkg.variable.{ordinal:04}"), variable.clone());
    }
    let schema = Arc::new(Schema::new_with_metadata(fields, schema_metadata));
    let mut writer = StreamWriter::try_new(output, &schema)
        .map_err(|error| ExecutionError::InvalidArrowStream(error.to_string()))?;
    for rows in bindings.chunks(max_batch_rows) {
        let arrays = binding_arrays(head, rows)?;
        let batch = RecordBatch::try_new(Arc::clone(&schema), arrays)
            .map_err(|error| ExecutionError::InvalidArrowStream(error.to_string()))?;
        writer
            .write(&batch)
            .map_err(|error| ExecutionError::InvalidArrowStream(error.to_string()))?;
    }
    writer
        .finish()
        .map_err(|error| ExecutionError::InvalidArrowStream(error.to_string()))
}

fn binding_arrays(head: &[String], bindings: &[Value]) -> Result<Vec<ArrayRef>, ExecutionError> {
    let rows = bindings.iter().collect::<Vec<_>>();
    binding_arrays_from_refs(head, &rows)
}

fn binding_arrays_from_refs(
    head: &[String],
    bindings: &[&Value],
) -> Result<Vec<ArrayRef>, ExecutionError> {
    let mut arrays = Vec::<ArrayRef>::with_capacity(head.len().saturating_mul(4));
    for variable in head {
        let mut kinds = Vec::with_capacity(bindings.len());
        let mut values = Vec::with_capacity(bindings.len());
        let mut datatypes = Vec::with_capacity(bindings.len());
        let mut languages = Vec::with_capacity(bindings.len());
        for binding in bindings {
            let row = binding
                .as_object()
                .ok_or(ExecutionError::InvalidSparqlBinding)?;
            match row.get(variable) {
                None => {
                    kinds.push(None);
                    values.push(None);
                    datatypes.push(None);
                    languages.push(None);
                }
                Some(term) => {
                    let parsed = parse_sparql_term(term)?;
                    kinds.push(Some(parsed.kind));
                    values.push(Some(parsed.value.to_owned()));
                    datatypes.push(parsed.datatype.map(ToOwned::to_owned));
                    languages.push(parsed.language.map(ToOwned::to_owned));
                }
            }
        }
        arrays.extend([
            Arc::new(UInt8Array::from(kinds)) as ArrayRef,
            Arc::new(StringArray::from(values)) as ArrayRef,
            Arc::new(StringArray::from(datatypes)) as ArrayRef,
            Arc::new(StringArray::from(languages)) as ArrayRef,
        ]);
    }
    Ok(arrays)
}

/// Encode both sides of one hash-owned join partition as Arrow IPC.
///
/// # Errors
///
/// Returns an error for invalid plan metadata, invalid heads or join keys,
/// malformed RDF terms, a zero batch bound, or Arrow/writer failures.
pub fn write_shuffle_join_stream(
    output: &mut impl Write,
    input: &ShuffleJoinInput,
    max_batch_rows: usize,
) -> Result<(), ExecutionError> {
    validate_shuffle_input(input)?;
    let header = ShuffleJoinStreamHeader {
        metadata: input.metadata.clone(),
        left_head: input.left_head.clone(),
        right_head: input.right_head.clone(),
        key_variables: input.key_variables.clone(),
        left_row_count: u64::try_from(input.left_bindings.len())
            .map_err(|_| ExecutionError::IntermediateRowLimit)?,
        right_row_count: u64::try_from(input.right_bindings.len())
            .map_err(|_| ExecutionError::IntermediateRowLimit)?,
    };
    write_shuffle_join_stream_iter(
        output,
        &header,
        input.left_bindings.iter().cloned().map(Ok),
        input.right_bindings.iter().cloned().map(Ok),
        max_batch_rows,
    )
}

/// Encode a hash-owned shuffle request from incremental relation iterators.
///
/// At most `max_batch_rows` decoded bindings are retained. Declared relation
/// counts, RDF term validity and primary partition ownership are verified while
/// rows are consumed, so an incomplete or foreign iterator cannot produce a
/// successful Arrow stream.
///
/// # Errors
///
/// Returns an error for invalid identity/schema, a source iterator failure,
/// count mismatch, foreign partition row, malformed RDF term, or writer failure.
pub fn write_shuffle_join_stream_iter<L, R>(
    output: &mut impl Write,
    header: &ShuffleJoinStreamHeader,
    left_rows: L,
    right_rows: R,
    max_batch_rows: usize,
) -> Result<(), ExecutionError>
where
    L: IntoIterator<Item = Result<Value, ExecutionError>>,
    R: IntoIterator<Item = Result<Value, ExecutionError>>,
{
    validate_shuffle_header(
        &header.metadata,
        &header.left_head,
        &header.right_head,
        &header.key_variables,
    )?;
    if max_batch_rows == 0 {
        return Err(ExecutionError::InvalidArrowMetadata(
            "Arrow batch row ceiling must be positive".to_owned(),
        ));
    }
    let union_head = union_head(&header.left_head, &header.right_head);
    let mut fields = Vec::with_capacity(1 + union_head.len().saturating_mul(4));
    fields.push(Field::new("relation", DataType::UInt8, false));
    for (ordinal, _) in union_head.iter().enumerate() {
        let prefix = format!("v{ordinal:04}");
        fields.extend([
            Field::new(format!("{prefix}.kind"), DataType::UInt8, true),
            Field::new(format!("{prefix}.value"), DataType::Utf8, true),
            Field::new(format!("{prefix}.datatype"), DataType::Utf8, true),
            Field::new(format!("{prefix}.language"), DataType::Utf8, true),
        ]);
    }
    let mut metadata = HashMap::from([
        (META_FORMAT.to_owned(), SHUFFLE_FORMAT_VERSION.to_owned()),
        (
            META_DATASET_ID.to_owned(),
            header.metadata.dataset_id.to_string(),
        ),
        (
            META_SNAPSHOT_ID.to_owned(),
            header.metadata.snapshot_id.to_string(),
        ),
        (
            META_QUERY_SHA256.to_owned(),
            header.metadata.query_sha256.clone(),
        ),
        (
            "ngkg.plan-sha256".to_owned(),
            header.metadata.plan_sha256.clone(),
        ),
        (
            "ngkg.request-id".to_owned(),
            header.metadata.request_id.to_string(),
        ),
        ("ngkg.stage".to_owned(), header.metadata.stage.to_string()),
        (
            "ngkg.partition".to_owned(),
            header.metadata.partition.to_string(),
        ),
        (
            "ngkg.partition-count".to_owned(),
            header.metadata.partition_count.to_string(),
        ),
        (
            "ngkg.left-row-count".to_owned(),
            header.left_row_count.to_string(),
        ),
        (
            "ngkg.right-row-count".to_owned(),
            header.right_row_count.to_string(),
        ),
    ]);
    insert_variable_metadata(&mut metadata, "ngkg.variable", &union_head);
    insert_variable_metadata(&mut metadata, "ngkg.left-variable", &header.left_head);
    insert_variable_metadata(&mut metadata, "ngkg.right-variable", &header.right_head);
    insert_variable_metadata(&mut metadata, "ngkg.key-variable", &header.key_variables);
    let schema = Arc::new(Schema::new_with_metadata(fields, metadata));
    let mut writer = StreamWriter::try_new(output, &schema)
        .map_err(|error| ExecutionError::InvalidArrowStream(error.to_string()))?;
    write_shuffle_relation(
        &mut writer,
        &schema,
        &union_head,
        0,
        left_rows,
        header.left_row_count,
        header,
        max_batch_rows,
    )?;
    write_shuffle_relation(
        &mut writer,
        &schema,
        &union_head,
        1,
        right_rows,
        header.right_row_count,
        header,
        max_batch_rows,
    )?;
    writer
        .finish()
        .map_err(|error| ExecutionError::InvalidArrowStream(error.to_string()))
}

#[allow(clippy::too_many_arguments)]
fn write_shuffle_relation<W, I>(
    writer: &mut StreamWriter<W>,
    schema: &Arc<Schema>,
    union_head: &[String],
    side: u8,
    rows: I,
    declared_rows: u64,
    header: &ShuffleJoinStreamHeader,
    max_batch_rows: usize,
) -> Result<(), ExecutionError>
where
    W: Write,
    I: IntoIterator<Item = Result<Value, ExecutionError>>,
{
    let mut observed = 0_u64;
    let mut chunk = Vec::with_capacity(max_batch_rows);
    for row in rows {
        let row = row?;
        if shuffle_partition_for_binding(
            &row,
            &header.key_variables,
            header.metadata.partition_count,
        )? != header.metadata.partition
        {
            return Err(ExecutionError::InvalidArrowMetadata(
                "shuffle row was sent to the wrong partition".to_owned(),
            ));
        }
        observed = observed
            .checked_add(1)
            .ok_or(ExecutionError::IntermediateRowLimit)?;
        if observed > declared_rows {
            return Err(ExecutionError::InvalidArrowMetadata(
                "shuffle relation exceeds its declared row count".to_owned(),
            ));
        }
        chunk.push(row);
        if chunk.len() == max_batch_rows {
            write_shuffle_batch(writer, schema, union_head, side, &chunk)?;
            chunk.clear();
        }
    }
    if !chunk.is_empty() {
        write_shuffle_batch(writer, schema, union_head, side, &chunk)?;
    }
    if observed != declared_rows {
        return Err(ExecutionError::InvalidArrowMetadata(
            "shuffle relation count differs from its declaration".to_owned(),
        ));
    }
    Ok(())
}

fn write_shuffle_batch<W: Write>(
    writer: &mut StreamWriter<W>,
    schema: &Arc<Schema>,
    union_head: &[String],
    side: u8,
    rows: &[Value],
) -> Result<(), ExecutionError> {
    let mut arrays = Vec::with_capacity(schema.fields().len());
    arrays.push(Arc::new(UInt8Array::from(vec![side; rows.len()])) as ArrayRef);
    let binding_refs = rows.iter().collect::<Vec<_>>();
    arrays.extend(binding_arrays_from_refs(union_head, &binding_refs)?);
    let batch = RecordBatch::try_new(Arc::clone(schema), arrays)
        .map_err(|error| ExecutionError::InvalidArrowStream(error.to_string()))?;
    writer
        .write(&batch)
        .map_err(|error| ExecutionError::InvalidArrowStream(error.to_string()))
}

/// Decode one complete partitioned shuffle-join request.
///
/// # Errors
///
/// Returns an error for a partial stream, invalid schema/metadata, malformed RDF
/// columns, or a relation whose decoded rows exceed `max_rows_per_side`.
pub fn read_shuffle_join_stream(
    bytes: &[u8],
    max_rows_per_side: usize,
) -> Result<ShuffleJoinInput, ExecutionError> {
    if !bytes.ends_with(&ARROW_STREAM_EOS) {
        return Err(ExecutionError::InvalidArrowStream(
            "stream end marker is absent".to_owned(),
        ));
    }
    ShuffleJoinStream::try_new(Cursor::new(bytes), max_rows_per_side)?.into_input()
}

impl<R: Read> ShuffleJoinStream<R> {
    /// Open and validate a shuffle schema before yielding any relation row.
    ///
    /// # Errors
    ///
    /// Returns an error for malformed metadata/schema or declared relation counts
    /// above `max_rows_per_side`.
    pub fn try_new(input: R, max_rows_per_side: usize) -> Result<Self, ExecutionError> {
        if max_rows_per_side == 0 {
            return Err(ExecutionError::IntermediateRowLimit);
        }
        let reader = StreamReader::try_new(input, None)
            .map_err(|error| ExecutionError::InvalidArrowStream(error.to_string()))?;
        let schema = reader.schema();
        if required_metadata(schema.metadata(), META_FORMAT)? != SHUFFLE_FORMAT_VERSION {
            return Err(ExecutionError::InvalidArrowMetadata(
                "shuffle format version is unsupported".to_owned(),
            ));
        }
        let union_variables = read_variable_metadata(schema.metadata(), "ngkg.variable")?;
        let left_head = read_variable_metadata(schema.metadata(), "ngkg.left-variable")?;
        let right_head = read_variable_metadata(schema.metadata(), "ngkg.right-variable")?;
        let key_variables = read_variable_metadata(schema.metadata(), "ngkg.key-variable")?;
        validate_shuffle_metadata_keys(
            schema.metadata(),
            union_variables.len(),
            left_head.len(),
            right_head.len(),
            key_variables.len(),
        )?;
        if union_variables != union_head(&left_head, &right_head)
            || schema.fields().len() != 1 + union_variables.len().saturating_mul(4)
            || schema.field(0).name() != "relation"
            || schema.field(0).data_type() != &DataType::UInt8
            || schema.field(0).is_nullable()
        {
            return Err(ExecutionError::InvalidArrowMetadata(
                "shuffle relation schema is invalid".to_owned(),
            ));
        }
        validate_arrow_schema_with_offset(&schema, union_variables.len(), 1)?;
        let left_row_count = parse_u64_metadata(schema.metadata(), "ngkg.left-row-count")?;
        let right_row_count = parse_u64_metadata(schema.metadata(), "ngkg.right-row-count")?;
        let maximum =
            u64::try_from(max_rows_per_side).map_err(|_| ExecutionError::IntermediateRowLimit)?;
        if left_row_count > maximum || right_row_count > maximum {
            return Err(ExecutionError::IntermediateRowLimit);
        }
        let metadata = ShuffleJoinMetadata {
            dataset_id: parse_uuid_metadata(schema.metadata(), META_DATASET_ID)?,
            snapshot_id: parse_uuid_metadata(schema.metadata(), META_SNAPSHOT_ID)?,
            query_sha256: required_metadata(schema.metadata(), META_QUERY_SHA256)?,
            plan_sha256: required_metadata(schema.metadata(), "ngkg.plan-sha256")?,
            request_id: parse_uuid_metadata(schema.metadata(), "ngkg.request-id")?,
            stage: parse_u32_metadata(schema.metadata(), "ngkg.stage")?,
            partition: parse_u32_metadata(schema.metadata(), "ngkg.partition")?,
            partition_count: parse_u32_metadata(schema.metadata(), "ngkg.partition-count")?,
        };
        validate_shuffle_header(&metadata, &left_head, &right_head, &key_variables)?;
        Ok(Self {
            reader,
            header: ShuffleJoinStreamHeader {
                metadata,
                left_head,
                right_head,
                key_variables,
                left_row_count,
                right_row_count,
            },
            union_head: union_variables,
            current_batch: None,
            current_row: 0,
            left_rows: 0,
            right_rows: 0,
            seen_right: false,
            terminal: false,
        })
    }

    /// Return the validated immutable stream header.
    #[must_use]
    pub const fn header(&self) -> &ShuffleJoinStreamHeader {
        &self.header
    }

    /// Decode the complete stream into the legacy owned input representation.
    ///
    /// This is retained only for explicitly memory-bounded partitions and tests.
    ///
    /// # Errors
    ///
    /// Returns an error for any invalid batch, row, ownership key, or row count.
    pub fn into_input(mut self) -> Result<ShuffleJoinInput, ExecutionError> {
        let header = self.header.clone();
        let mut left_bindings = Vec::with_capacity(
            usize::try_from(header.left_row_count)
                .map_err(|_| ExecutionError::IntermediateRowLimit)?,
        );
        let mut right_bindings = Vec::with_capacity(
            usize::try_from(header.right_row_count)
                .map_err(|_| ExecutionError::IntermediateRowLimit)?,
        );
        for decoded in &mut self {
            let (side, binding) = decoded?;
            match side {
                0 => left_bindings.push(binding),
                1 => right_bindings.push(binding),
                _ => {
                    return Err(ExecutionError::InvalidArrowStream(
                        "shuffle relation code is unknown".to_owned(),
                    ));
                }
            }
        }
        Ok(ShuffleJoinInput {
            metadata: header.metadata,
            left_head: header.left_head,
            right_head: header.right_head,
            key_variables: header.key_variables,
            left_bindings,
            right_bindings,
        })
    }

    fn next_decoded(&mut self) -> Result<Option<(u8, Value)>, ExecutionError> {
        loop {
            if let Some(batch) = &self.current_batch
                && self.current_row < batch.num_rows()
            {
                let row_index = self.current_row;
                self.current_row += 1;
                let (side, binding) = {
                    let relations = batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<UInt8Array>()
                        .ok_or_else(|| {
                            ExecutionError::InvalidArrowStream(
                                "relation column type changed".to_owned(),
                            )
                        })?;
                    if relations.is_null(row_index) {
                        return Err(ExecutionError::InvalidArrowStream(
                            "shuffle relation is null".to_owned(),
                        ));
                    }
                    let side = relations.value(row_index);
                    let mut binding = Map::new();
                    for (ordinal, variable) in self.union_head.iter().enumerate() {
                        if let Some(term) =
                            decode_arrow_term_at(batch, 1 + ordinal.saturating_mul(4), row_index)?
                        {
                            binding.insert(variable.clone(), term);
                        }
                    }
                    (side, Value::Object(binding))
                };
                if shuffle_partition_for_binding(
                    &binding,
                    &self.header.key_variables,
                    self.header.metadata.partition_count,
                )? != self.header.metadata.partition
                {
                    return Err(ExecutionError::InvalidArrowMetadata(
                        "shuffle row was sent to the wrong partition".to_owned(),
                    ));
                }
                match side {
                    0 => {
                        if self.seen_right {
                            return Err(ExecutionError::InvalidArrowStream(
                                "left relation row follows the right relation".to_owned(),
                            ));
                        }
                        self.left_rows = self
                            .left_rows
                            .checked_add(1)
                            .ok_or(ExecutionError::IntermediateRowLimit)?;
                        if self.left_rows > self.header.left_row_count {
                            return Err(ExecutionError::InvalidArrowMetadata(
                                "left relation exceeds its declared row count".to_owned(),
                            ));
                        }
                    }
                    1 => {
                        self.seen_right = true;
                        self.right_rows = self
                            .right_rows
                            .checked_add(1)
                            .ok_or(ExecutionError::IntermediateRowLimit)?;
                        if self.right_rows > self.header.right_row_count {
                            return Err(ExecutionError::InvalidArrowMetadata(
                                "right relation exceeds its declared row count".to_owned(),
                            ));
                        }
                    }
                    _ => {
                        return Err(ExecutionError::InvalidArrowStream(
                            "shuffle relation code is unknown".to_owned(),
                        ));
                    }
                }
                return Ok(Some((side, binding)));
            }
            match self.reader.next() {
                Some(Ok(batch)) => {
                    self.current_batch = Some(batch);
                    self.current_row = 0;
                }
                Some(Err(error)) => {
                    return Err(ExecutionError::InvalidArrowStream(error.to_string()));
                }
                None => {
                    if self.left_rows != self.header.left_row_count
                        || self.right_rows != self.header.right_row_count
                    {
                        return Err(ExecutionError::InvalidArrowMetadata(
                            "decoded relation counts differ from Arrow metadata".to_owned(),
                        ));
                    }
                    return Ok(None);
                }
            }
        }
    }
}

impl<R: Read> Iterator for ShuffleJoinStream<R> {
    type Item = Result<(u8, Value), ExecutionError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.terminal {
            return None;
        }
        match self.next_decoded() {
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

/// Assign exact SPARQL bindings to stable hash partitions.
///
/// # Errors
///
/// Returns an error for a zero partition count, malformed bindings, invalid RDF
/// terms, or an unbound join-key variable.
pub fn partition_sparql_json(
    rows: &[Value],
    key_variables: &[String],
    partition_count: u32,
) -> Result<Vec<Vec<Value>>, ExecutionError> {
    if partition_count == 0 {
        return Err(ExecutionError::InvalidPartitionCount);
    }
    if key_variables.is_empty() {
        return Err(ExecutionError::EmptyShuffleKey);
    }
    let count =
        usize::try_from(partition_count).map_err(|_| ExecutionError::InvalidPartitionCount)?;
    let mut partitions = vec![Vec::new(); count];
    for row in rows {
        let partition = shuffle_partition_for_binding(row, key_variables, partition_count)?;
        let index =
            usize::try_from(partition).map_err(|_| ExecutionError::InvalidPartitionCount)?;
        partitions[index].push(row.clone());
    }
    Ok(partitions)
}

/// Return the stable owner partition for one completely bound join key.
///
/// # Errors
///
/// Returns an error for malformed bindings, invalid terms, unbound key variables,
/// or a zero partition count.
pub fn shuffle_partition_for_binding(
    row: &Value,
    key_variables: &[String],
    partition_count: u32,
) -> Result<u32, ExecutionError> {
    partition_for_binding(
        row,
        key_variables,
        partition_count,
        b"ngkg-shuffle-key-v1\0",
    )
}

/// Assign a completely bound join key to a deterministic worker-local Grace bucket.
///
/// This uses a domain distinct from the cross-node shuffle hash. Reusing the
/// primary hash would send every row in a power-of-two primary partition back
/// to the same local bucket when both partition counts match.
///
/// # Errors
///
/// Returns an error for malformed bindings, invalid terms, unbound key variables,
/// or a zero bucket count.
pub fn grace_partition_for_binding(
    row: &Value,
    key_variables: &[String],
    bucket_count: u32,
) -> Result<u32, ExecutionError> {
    partition_for_binding(
        row,
        key_variables,
        bucket_count,
        b"ngkg-worker-grace-key-v1\0",
    )
}

fn partition_for_binding(
    row: &Value,
    key_variables: &[String],
    partition_count: u32,
    domain: &[u8],
) -> Result<u32, ExecutionError> {
    if partition_count == 0 {
        return Err(ExecutionError::InvalidPartitionCount);
    }
    if key_variables.is_empty() {
        return Err(ExecutionError::EmptyShuffleKey);
    }
    let binding = row
        .as_object()
        .ok_or(ExecutionError::InvalidSparqlBinding)?;
    let mut hash = Sha256::new();
    hash.update(domain);
    for variable in key_variables {
        let term = binding
            .get(variable)
            .ok_or_else(|| ExecutionError::UnboundShuffleKey(variable.clone()))?;
        let parsed = parse_sparql_term(term)?;
        let variable_bytes = variable.as_bytes();
        update_length_prefixed_hash(&mut hash, variable_bytes)?;
        hash.update([parsed.kind]);
        update_length_prefixed_hash(&mut hash, parsed.value.as_bytes())?;
        update_optional_hash(&mut hash, parsed.datatype)?;
        update_optional_hash(&mut hash, parsed.language)?;
    }
    let digest = hash.finalize();
    let prefix = u64::from_be_bytes(digest[..8].try_into().map_err(|_| {
        ExecutionError::InvalidArrowStream("shuffle hash prefix is unavailable".to_owned())
    })?);
    u32::try_from(prefix % u64::from(partition_count))
        .map_err(|_| ExecutionError::InvalidPartitionCount)
}

fn update_optional_hash(hash: &mut Sha256, value: Option<&str>) -> Result<(), ExecutionError> {
    match value {
        Some(value) => {
            hash.update([1]);
            update_length_prefixed_hash(hash, value.as_bytes())
        }
        None => {
            hash.update([0]);
            Ok(())
        }
    }
}

fn update_length_prefixed_hash(hash: &mut Sha256, bytes: &[u8]) -> Result<(), ExecutionError> {
    let length = u64::try_from(bytes.len()).map_err(|_| {
        ExecutionError::InvalidSparqlTerm("shuffle key component is too large".to_owned())
    })?;
    hash.update(length.to_be_bytes());
    hash.update(bytes);
    Ok(())
}

/// Decode and fully validate a certified Arrow IPC fragment stream.
///
/// # Errors
///
/// Returns an error for a partial stream, unexpected schema or metadata,
/// inconsistent RDF term columns, or a decoded row count above `max_rows`.
pub fn read_fragment_arrow_stream(
    bytes: &[u8],
    max_rows: usize,
) -> Result<FragmentBindingBatch, ExecutionError> {
    if !bytes.ends_with(&ARROW_STREAM_EOS) {
        return Err(ExecutionError::InvalidArrowStream(
            "stream end marker is absent".to_owned(),
        ));
    }
    FragmentBindingStream::try_new(Cursor::new(bytes), max_rows)?.into_batch()
}

impl<R: Read> FragmentBindingStream<R> {
    /// Open and validate a fragment schema before yielding a binding.
    ///
    /// # Errors
    ///
    /// Returns an error for malformed metadata/schema or a zero row ceiling.
    pub fn try_new(input: R, max_rows: usize) -> Result<Self, ExecutionError> {
        if max_rows == 0 {
            return Err(ExecutionError::IntermediateRowLimit);
        }
        let reader = StreamReader::try_new(input, None)
            .map_err(|error| ExecutionError::InvalidArrowStream(error.to_string()))?;
        let schema = reader.schema();
        let metadata = decode_metadata(schema.metadata())?;
        validate_fragment_metadata(&metadata)?;
        let variable_count = required_metadata(schema.metadata(), META_VARIABLE_COUNT)?
            .parse::<usize>()
            .map_err(|_| {
                ExecutionError::InvalidArrowMetadata("variable count is not an integer".to_owned())
            })?;
        if schema.fields().len() != variable_count.saturating_mul(4) {
            return Err(ExecutionError::InvalidArrowMetadata(
                "field count does not match the declared variable count".to_owned(),
            ));
        }
        validate_schema_metadata_keys(schema.metadata(), variable_count)?;
        let head = (0..variable_count)
            .map(|ordinal| {
                required_metadata(schema.metadata(), &format!("ngkg.variable.{ordinal:04}"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        validate_head(&head)?;
        validate_arrow_schema(&schema, variable_count)?;
        Ok(Self {
            reader,
            metadata,
            head,
            current_batch: None,
            current_row: 0,
            decoded_rows: 0,
            max_rows,
            terminal: false,
        })
    }

    /// Return the certificate metadata embedded in the Arrow schema.
    #[must_use]
    pub const fn metadata(&self) -> &FragmentBatchMetadata {
        &self.metadata
    }

    /// Return the ordered SPARQL result variables embedded in the schema.
    #[must_use]
    pub fn head(&self) -> &[String] {
        &self.head
    }

    /// Decode the remaining stream into the legacy owned response.
    ///
    /// This is retained for callers whose following operator still has an
    /// explicit row ceiling. Streaming callers should iterate directly.
    ///
    /// # Errors
    ///
    /// Returns an error for any invalid batch, RDF term or row limit.
    pub fn into_batch(mut self) -> Result<FragmentBindingBatch, ExecutionError> {
        let metadata = self.metadata.clone();
        let head = self.head.clone();
        let mut bindings = Vec::new();
        for binding in &mut self {
            bindings.push(binding?);
        }
        Ok(FragmentBindingBatch {
            metadata,
            head,
            bindings,
        })
    }

    fn next_binding(&mut self) -> Result<Option<Value>, ExecutionError> {
        loop {
            if let Some(batch) = &self.current_batch
                && self.current_row < batch.num_rows()
            {
                let row_index = self.current_row;
                self.current_row += 1;
                self.decoded_rows = self
                    .decoded_rows
                    .checked_add(1)
                    .filter(|rows| *rows <= self.max_rows)
                    .ok_or(ExecutionError::IntermediateRowLimit)?;
                let mut binding = Map::new();
                for (ordinal, variable) in self.head.iter().enumerate() {
                    if let Some(term) =
                        decode_arrow_term_at(batch, ordinal.saturating_mul(4), row_index)?
                    {
                        binding.insert(variable.clone(), term);
                    }
                }
                return Ok(Some(Value::Object(binding)));
            }
            match self.reader.next() {
                Some(Ok(batch)) => {
                    if self
                        .decoded_rows
                        .checked_add(batch.num_rows())
                        .is_none_or(|rows| rows > self.max_rows)
                    {
                        return Err(ExecutionError::IntermediateRowLimit);
                    }
                    self.current_batch = Some(batch);
                    self.current_row = 0;
                }
                Some(Err(error)) => {
                    return Err(ExecutionError::InvalidArrowStream(error.to_string()));
                }
                None => return Ok(None),
            }
        }
    }
}

impl<R: Read> Iterator for FragmentBindingStream<R> {
    type Item = Result<Value, ExecutionError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.terminal {
            return None;
        }
        match self.next_binding() {
            Ok(Some(binding)) => Some(Ok(binding)),
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

fn validate_schema_metadata_keys(
    metadata: &HashMap<String, String>,
    variable_count: usize,
) -> Result<(), ExecutionError> {
    let fixed = BTreeSet::from([
        META_FORMAT,
        META_DATASET_ID,
        META_SNAPSHOT_ID,
        META_QUERY_SHA256,
        META_FRAGMENT_ID,
        META_WORKER_ID,
        META_MULTISET_SHA256,
        META_VARIABLE_COUNT,
    ]);
    for key in metadata.keys() {
        if fixed.contains(key.as_str()) {
            continue;
        }
        let valid_variable_key = key
            .strip_prefix("ngkg.variable.")
            .and_then(|value| value.parse::<usize>().ok())
            .is_some_and(|ordinal| {
                ordinal < variable_count && key == &format!("ngkg.variable.{ordinal:04}")
            });
        if !valid_variable_key {
            return Err(ExecutionError::InvalidArrowMetadata(
                "schema metadata contains an unsupported member".to_owned(),
            ));
        }
    }
    Ok(())
}

struct ParsedTerm<'a> {
    kind: u8,
    value: &'a str,
    datatype: Option<&'a str>,
    language: Option<&'a str>,
}

fn parse_sparql_term(term: &Value) -> Result<ParsedTerm<'_>, ExecutionError> {
    let object = term
        .as_object()
        .ok_or_else(|| ExecutionError::InvalidSparqlTerm("term is not an object".to_owned()))?;
    if object
        .keys()
        .any(|key| !matches!(key.as_str(), "type" | "value" | "datatype" | "xml:lang"))
    {
        return Err(ExecutionError::InvalidSparqlTerm(
            "term contains an unsupported member".to_owned(),
        ));
    }
    let kind = object
        .get("type")
        .and_then(Value::as_str)
        .ok_or_else(|| ExecutionError::InvalidSparqlTerm("term type is absent".to_owned()))?;
    let value = object
        .get("value")
        .and_then(Value::as_str)
        .ok_or_else(|| ExecutionError::InvalidSparqlTerm("term value is absent".to_owned()))?;
    let datatype = optional_string_member(object, "datatype")?;
    let language = optional_string_member(object, "xml:lang")?;
    let code = match kind {
        "uri" if datatype.is_none() && language.is_none() => 1,
        "bnode" if datatype.is_none() && language.is_none() => 2,
        "literal" if !(datatype.is_some() && language.is_some()) => 3,
        _ => {
            return Err(ExecutionError::InvalidSparqlTerm(
                "term family, datatype, and language are inconsistent".to_owned(),
            ));
        }
    };
    Ok(ParsedTerm {
        kind: code,
        value,
        datatype,
        language,
    })
}

fn optional_string_member<'a>(
    object: &'a Map<String, Value>,
    key: &str,
) -> Result<Option<&'a str>, ExecutionError> {
    object
        .get(key)
        .map(|value| {
            value.as_str().ok_or_else(|| {
                ExecutionError::InvalidSparqlTerm(format!("term {key} is not a string"))
            })
        })
        .transpose()
}

fn decode_arrow_term_at(
    batch: &RecordBatch,
    base: usize,
    row: usize,
) -> Result<Option<Value>, ExecutionError> {
    let kinds = batch
        .column(base)
        .as_any()
        .downcast_ref::<UInt8Array>()
        .ok_or_else(|| ExecutionError::InvalidArrowStream("kind column type changed".to_owned()))?;
    let values = string_column(batch, base + 1, "value")?;
    let datatypes = string_column(batch, base + 2, "datatype")?;
    let languages = string_column(batch, base + 3, "language")?;
    let nulls = [
        kinds.is_null(row),
        values.is_null(row),
        datatypes.is_null(row),
        languages.is_null(row),
    ];
    if nulls.iter().all(|value| *value) {
        return Ok(None);
    }
    if nulls[0] || nulls[1] {
        return Err(ExecutionError::InvalidArrowStream(
            "bound term has a null kind or value".to_owned(),
        ));
    }
    let mut term = Map::new();
    let (kind, permits_qualifier) = match kinds.value(row) {
        1 => ("uri", false),
        2 => ("bnode", false),
        3 => ("literal", true),
        _ => {
            return Err(ExecutionError::InvalidArrowStream(
                "term kind code is unknown".to_owned(),
            ));
        }
    };
    term.insert("type".to_owned(), Value::String(kind.to_owned()));
    term.insert(
        "value".to_owned(),
        Value::String(values.value(row).to_owned()),
    );
    let datatype = (!datatypes.is_null(row)).then(|| datatypes.value(row));
    let language = (!languages.is_null(row)).then(|| languages.value(row));
    if !permits_qualifier && (datatype.is_some() || language.is_some())
        || permits_qualifier && datatype.is_some() && language.is_some()
    {
        return Err(ExecutionError::InvalidArrowStream(
            "term qualifier columns are inconsistent".to_owned(),
        ));
    }
    if let Some(value) = datatype {
        term.insert("datatype".to_owned(), Value::String(value.to_owned()));
    }
    if let Some(value) = language {
        term.insert("xml:lang".to_owned(), Value::String(value.to_owned()));
    }
    Ok(Some(Value::Object(term)))
}

fn string_column<'a>(
    batch: &'a RecordBatch,
    index: usize,
    label: &str,
) -> Result<&'a StringArray, ExecutionError> {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| ExecutionError::InvalidArrowStream(format!("{label} column type changed")))
}

fn validate_arrow_schema(schema: &Schema, variable_count: usize) -> Result<(), ExecutionError> {
    validate_arrow_schema_with_offset(schema, variable_count, 0)
}

fn validate_arrow_schema_with_offset(
    schema: &Schema,
    variable_count: usize,
    field_offset: usize,
) -> Result<(), ExecutionError> {
    for ordinal in 0..variable_count {
        let prefix = format!("v{ordinal:04}");
        let expected = [
            (format!("{prefix}.kind"), DataType::UInt8),
            (format!("{prefix}.value"), DataType::Utf8),
            (format!("{prefix}.datatype"), DataType::Utf8),
            (format!("{prefix}.language"), DataType::Utf8),
        ];
        for (component, (name, data_type)) in expected.into_iter().enumerate() {
            let field = schema.field(field_offset + ordinal.saturating_mul(4) + component);
            if field.name() != &name || field.data_type() != &data_type || !field.is_nullable() {
                return Err(ExecutionError::InvalidArrowMetadata(
                    "field layout does not match the NGKG binding contract".to_owned(),
                ));
            }
        }
    }
    Ok(())
}

fn insert_variable_metadata(
    metadata: &mut HashMap<String, String>,
    prefix: &str,
    variables: &[String],
) {
    metadata.insert(format!("{prefix}-count"), variables.len().to_string());
    for (ordinal, variable) in variables.iter().enumerate() {
        metadata.insert(format!("{prefix}.{ordinal:04}"), variable.clone());
    }
}

fn read_variable_metadata(
    metadata: &HashMap<String, String>,
    prefix: &str,
) -> Result<Vec<String>, ExecutionError> {
    let count = required_metadata(metadata, &format!("{prefix}-count"))?
        .parse::<usize>()
        .map_err(|_| {
            ExecutionError::InvalidArrowMetadata(format!("{prefix} count is not an integer"))
        })?;
    let variables = (0..count)
        .map(|ordinal| required_metadata(metadata, &format!("{prefix}.{ordinal:04}")))
        .collect::<Result<Vec<_>, _>>()?;
    validate_head(&variables)?;
    Ok(variables)
}

fn validate_shuffle_metadata_keys(
    metadata: &HashMap<String, String>,
    union_count: usize,
    left_count: usize,
    right_count: usize,
    key_count: usize,
) -> Result<(), ExecutionError> {
    let fixed = BTreeSet::from([
        META_FORMAT,
        META_DATASET_ID,
        META_SNAPSHOT_ID,
        META_QUERY_SHA256,
        "ngkg.plan-sha256",
        "ngkg.request-id",
        "ngkg.stage",
        "ngkg.partition",
        "ngkg.partition-count",
        "ngkg.left-row-count",
        "ngkg.right-row-count",
        "ngkg.variable-count",
        "ngkg.left-variable-count",
        "ngkg.right-variable-count",
        "ngkg.key-variable-count",
    ]);
    for key in metadata.keys() {
        if fixed.contains(key.as_str()) {
            continue;
        }
        let valid = [
            ("ngkg.variable.", union_count),
            ("ngkg.left-variable.", left_count),
            ("ngkg.right-variable.", right_count),
            ("ngkg.key-variable.", key_count),
        ]
        .into_iter()
        .any(|(prefix, count)| {
            key.strip_prefix(prefix)
                .and_then(|value| value.parse::<usize>().ok())
                .is_some_and(|ordinal| ordinal < count && key == &format!("{prefix}{ordinal:04}"))
        });
        if !valid {
            return Err(ExecutionError::InvalidArrowMetadata(
                "shuffle schema metadata contains an unsupported member".to_owned(),
            ));
        }
    }
    Ok(())
}

fn validate_shuffle_input(input: &ShuffleJoinInput) -> Result<(), ExecutionError> {
    validate_shuffle_header(
        &input.metadata,
        &input.left_head,
        &input.right_head,
        &input.key_variables,
    )?;
    for row in input.left_bindings.iter().chain(&input.right_bindings) {
        if shuffle_partition_for_binding(row, &input.key_variables, input.metadata.partition_count)?
            != input.metadata.partition
        {
            return Err(ExecutionError::InvalidArrowMetadata(
                "shuffle row was sent to the wrong partition".to_owned(),
            ));
        }
    }
    Ok(())
}

fn validate_shuffle_header(
    metadata: &ShuffleJoinMetadata,
    left_head: &[String],
    right_head: &[String],
    key_variables: &[String],
) -> Result<(), ExecutionError> {
    validate_head(left_head)?;
    validate_head(right_head)?;
    validate_head(key_variables)?;
    let left = left_head.iter().collect::<BTreeSet<_>>();
    let right = right_head.iter().collect::<BTreeSet<_>>();
    if metadata.partition_count < 2
        || metadata.partition >= metadata.partition_count
        || !is_lower_hex_sha256(&metadata.query_sha256)
        || !is_lower_hex_sha256(&metadata.plan_sha256)
        || key_variables
            .iter()
            .any(|variable| !left.contains(variable) || !right.contains(variable))
    {
        return Err(ExecutionError::InvalidArrowMetadata(
            "shuffle identity, ownership, or join key is invalid".to_owned(),
        ));
    }
    Ok(())
}

fn union_head(left: &[String], right: &[String]) -> Vec<String> {
    let mut union = left.to_vec();
    let mut present = left.iter().cloned().collect::<BTreeSet<_>>();
    for variable in right {
        if present.insert(variable.clone()) {
            union.push(variable.clone());
        }
    }
    union
}

fn parse_u32_metadata(
    metadata: &HashMap<String, String>,
    key: &str,
) -> Result<u32, ExecutionError> {
    required_metadata(metadata, key)?.parse().map_err(|_| {
        ExecutionError::InvalidArrowMetadata(format!("{key} is not an unsigned integer"))
    })
}

fn parse_u64_metadata(
    metadata: &HashMap<String, String>,
    key: &str,
) -> Result<u64, ExecutionError> {
    required_metadata(metadata, key)?.parse().map_err(|_| {
        ExecutionError::InvalidArrowMetadata(format!("{key} is not an unsigned integer"))
    })
}

fn decode_metadata(
    metadata: &HashMap<String, String>,
) -> Result<FragmentBatchMetadata, ExecutionError> {
    if required_metadata(metadata, META_FORMAT)? != ARROW_BINDING_FORMAT_VERSION {
        return Err(ExecutionError::InvalidArrowMetadata(
            "format version is unsupported".to_owned(),
        ));
    }
    Ok(FragmentBatchMetadata {
        dataset_id: parse_uuid_metadata(metadata, META_DATASET_ID)?,
        snapshot_id: parse_uuid_metadata(metadata, META_SNAPSHOT_ID)?,
        query_sha256: required_metadata(metadata, META_QUERY_SHA256)?,
        fragment_id: required_metadata(metadata, META_FRAGMENT_ID)?,
        worker_id: required_metadata(metadata, META_WORKER_ID)?,
        multiset_sha256: required_metadata(metadata, META_MULTISET_SHA256)?,
    })
}

fn parse_uuid_metadata(
    metadata: &HashMap<String, String>,
    key: &str,
) -> Result<Uuid, ExecutionError> {
    required_metadata(metadata, key)?
        .parse()
        .map_err(|_| ExecutionError::InvalidArrowMetadata(format!("{key} is not a UUID")))
}

fn required_metadata(
    metadata: &HashMap<String, String>,
    key: &str,
) -> Result<String, ExecutionError> {
    metadata
        .get(key)
        .filter(|value| !value.is_empty())
        .cloned()
        .ok_or_else(|| ExecutionError::InvalidArrowMetadata(format!("{key} is absent")))
}

fn validate_fragment_metadata(metadata: &FragmentBatchMetadata) -> Result<(), ExecutionError> {
    if metadata.worker_id.is_empty()
        || metadata.fragment_id.is_empty()
        || !is_lower_hex_sha256(&metadata.query_sha256)
        || !is_lower_hex_sha256(&metadata.multiset_sha256)
    {
        return Err(ExecutionError::InvalidArrowMetadata(
            "certificate metadata is malformed".to_owned(),
        ));
    }
    Ok(())
}

fn validate_head(head: &[String]) -> Result<(), ExecutionError> {
    let unique = head.iter().collect::<BTreeSet<_>>();
    if head.is_empty()
        || unique.len() != head.len()
        || head.iter().any(|variable| variable.is_empty())
    {
        return Err(ExecutionError::InvalidArrowMetadata(
            "SPARQL head is empty or contains duplicate variables".to_owned(),
        ));
    }
    Ok(())
}

fn is_lower_hex_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

/// Reduce one relation using exact keys from the other without changing multiplicity.
#[must_use]
pub fn exact_semijoin(
    rows: &[BindingRow],
    exact_keys: &BTreeSet<Vec<EncodedTerm>>,
    key_columns: &[u16],
) -> Vec<BindingRow> {
    rows.iter()
        .filter(|row| binding_key(row, key_columns).is_some_and(|key| exact_keys.contains(&key)))
        .cloned()
        .collect()
}

/// Exact compatible inner join with checked bag-multiplicity multiplication.
pub fn inner_join(
    left: &[BindingRow],
    right: &[BindingRow],
    key_columns: &[u16],
) -> Result<Vec<BindingRow>, ExecutionError> {
    let mut right_index: BTreeMap<Vec<EncodedTerm>, Vec<&BindingRow>> = BTreeMap::new();
    for row in right {
        if let Some(key) = binding_key(row, key_columns) {
            right_index.entry(key).or_default().push(row);
        }
    }
    let mut output = Vec::new();
    for left_row in left {
        let Some(key) = binding_key(left_row, key_columns) else {
            continue;
        };
        let Some(matches) = right_index.get(&key) else {
            continue;
        };
        for right_row in matches {
            let mut values = left_row.values.clone();
            for (variable, term) in &right_row.values {
                if let Some(existing) = values.get(variable) {
                    if existing != term {
                        return Err(ExecutionError::IncompatibleBinding(*variable));
                    }
                } else {
                    values.insert(*variable, term.clone());
                }
            }
            let multiplicity = left_row
                .multiplicity
                .checked_mul(right_row.multiplicity)
                .ok_or(ExecutionError::MultiplicityOverflow)?;
            let mut proof_ids = left_row.proof_ids.clone();
            proof_ids.extend_from_slice(&right_row.proof_ids);
            output.push(BindingRow {
                query_ordinal: left_row.query_ordinal,
                values,
                multiplicity,
                named_graph_id: left_row.named_graph_id.or(right_row.named_graph_id),
                proof_ids,
            });
        }
    }
    Ok(output)
}

/// Exact SPARQL JSON bag join used by the first networked fragment executor.
///
/// Repeated input rows remain repeated, so SPARQL bag multiplicity is preserved.
/// Shared variables must contain identical RDF JSON terms. The hard output bound
/// is checked before each allocation can grow the intermediate relation.
pub fn inner_join_sparql_json(
    left: &[Value],
    right: &[Value],
    max_rows: usize,
) -> Result<Vec<Value>, ExecutionError> {
    let left_variables = binding_variables(left)?;
    let right_variables = binding_variables(right)?;
    let shared = left_variables
        .intersection(&right_variables)
        .cloned()
        .collect::<Vec<_>>();
    let all_shared_bound = left.iter().chain(right).all(|row| {
        row.as_object()
            .is_some_and(|object| shared.iter().all(|variable| object.contains_key(variable)))
    });
    if !all_shared_bound {
        return compatible_nested_join(left, right, &shared, max_rows);
    }
    // Every fully-bound shared key has exactly one shuffle owner. A hash index
    // therefore gives the local worker expected O(1) probes without changing
    // result order: output still follows left-row order and right insertion order.
    let mut right_index = HashMap::<Vec<String>, Vec<&Map<String, Value>>>::new();
    for row in right {
        let object = row
            .as_object()
            .ok_or(ExecutionError::InvalidSparqlBinding)?;
        right_index
            .entry(json_binding_key(object, &shared)?)
            .or_default()
            .push(object);
    }
    let mut output = Vec::new();
    for row in left {
        let left_object = row
            .as_object()
            .ok_or(ExecutionError::InvalidSparqlBinding)?;
        let key = json_binding_key(left_object, &shared)?;
        let Some(matches) = right_index.get(&key) else {
            continue;
        };
        for right_object in matches {
            if output.len() >= max_rows {
                return Err(ExecutionError::IntermediateRowLimit);
            }
            output.push(Value::Object(merge_json_bindings(
                left_object,
                right_object,
            )?));
        }
    }
    Ok(output)
}

fn compatible_nested_join(
    left: &[Value],
    right: &[Value],
    shared: &[String],
    max_rows: usize,
) -> Result<Vec<Value>, ExecutionError> {
    let mut output = Vec::new();
    for left_row in left {
        let left_object = left_row
            .as_object()
            .ok_or(ExecutionError::InvalidSparqlBinding)?;
        for right_row in right {
            let right_object = right_row
                .as_object()
                .ok_or(ExecutionError::InvalidSparqlBinding)?;
            let compatible = shared.iter().all(|variable| {
                left_object
                    .get(variable)
                    .zip(right_object.get(variable))
                    .is_none_or(|(left, right)| left == right)
            });
            if !compatible {
                continue;
            }
            if output.len() >= max_rows {
                return Err(ExecutionError::IntermediateRowLimit);
            }
            output.push(Value::Object(merge_json_bindings(
                left_object,
                right_object,
            )?));
        }
    }
    Ok(output)
}

fn merge_json_bindings(
    left: &Map<String, Value>,
    right: &Map<String, Value>,
) -> Result<Map<String, Value>, ExecutionError> {
    let mut joined = left.clone();
    for (variable, term) in right {
        if let Some(existing) = joined.get(variable) {
            if existing != term {
                return Err(ExecutionError::IncompatibleBinding(stable_variable_id(
                    variable,
                )));
            }
        } else {
            joined.insert(variable.clone(), term.clone());
        }
    }
    Ok(joined)
}

/// Project exact SPARQL JSON bindings without deduplicating bag rows.
pub fn project_sparql_json(
    rows: &[Value],
    variables: &[String],
) -> Result<Vec<Value>, ExecutionError> {
    rows.iter()
        .map(|row| {
            let object = row
                .as_object()
                .ok_or(ExecutionError::InvalidSparqlBinding)?;
            let projected = variables
                .iter()
                .filter_map(|variable| {
                    object
                        .get(variable)
                        .cloned()
                        .map(|term| (variable.clone(), term))
                })
                .collect::<Map<_, _>>();
            Ok(Value::Object(projected))
        })
        .collect()
}

fn binding_variables(rows: &[Value]) -> Result<BTreeSet<String>, ExecutionError> {
    let mut variables = BTreeSet::new();
    for row in rows {
        variables.extend(
            row.as_object()
                .ok_or(ExecutionError::InvalidSparqlBinding)?
                .keys()
                .cloned(),
        );
    }
    Ok(variables)
}

fn json_binding_key(
    row: &Map<String, Value>,
    variables: &[String],
) -> Result<Vec<String>, ExecutionError> {
    variables
        .iter()
        .map(|variable| {
            row.get(variable)
                .map(serde_json::to_string)
                .transpose()
                .map_err(|_| ExecutionError::InvalidSparqlBinding)
                .map(|value| value.unwrap_or_else(|| "null".to_owned()))
        })
        .collect()
}

fn stable_variable_id(variable: &str) -> u16 {
    variable.bytes().fold(0_u16, |state, byte| {
        state.wrapping_mul(257).wrapping_add(u16::from(byte))
    })
}

/// Deduplicate only exact `(entity, automaton_state)` frontier pairs.
#[must_use]
pub fn next_frontier(
    candidates: impl IntoIterator<Item = FrontierKey>,
    visited: &mut BTreeSet<FrontierKey>,
) -> Vec<FrontierKey> {
    let mut next = Vec::new();
    for key in candidates {
        if visited.insert(key) {
            next.push(key);
        }
    }
    next.sort_unstable();
    next
}

fn binding_key(row: &BindingRow, key_columns: &[u16]) -> Option<Vec<EncodedTerm>> {
    key_columns
        .iter()
        .map(|column| row.values.get(column).cloned())
        .collect()
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, BTreeSet},
        io::Cursor,
    };

    use serde_json::json;
    use uuid::Uuid;

    use super::{
        BindingRow, EncodedTerm, ExecutionError, FragmentBatchMetadata, FragmentBindingStream,
        ShuffleJoinInput, ShuffleJoinMetadata, ShuffleJoinStream, grace_partition_for_binding,
        inner_join, inner_join_sparql_json, partition_sparql_json, project_sparql_json,
        read_fragment_arrow_stream, read_shuffle_join_stream, shuffle_partition_for_binding,
        write_fragment_arrow_stream, write_shuffle_join_stream, write_shuffle_join_stream_iter,
    };

    #[test]
    fn join_multiplies_bag_multiplicity() {
        let row = |ordinal, multiplicity| BindingRow {
            query_ordinal: ordinal,
            values: BTreeMap::from([(0, EncodedTerm::Entity(7))]),
            multiplicity,
            named_graph_id: Some(3),
            proof_ids: vec![ordinal],
        };
        let joined = inner_join(&[row(1, 2)], &[row(2, 3)], &[0]);
        assert!(joined.is_ok());
        assert_eq!(joined.map(|rows| rows[0].multiplicity), Ok(6));
    }

    #[test]
    fn json_join_preserves_bag_rows_and_projects_exactly() {
        let left = vec![
            json!({"node": {"type": "uri", "value": "urn:node:1"}}),
            json!({"node": {"type": "uri", "value": "urn:node:1"}}),
        ];
        let right = vec![json!({
            "node": {"type": "uri", "value": "urn:node:1"},
            "failure": {"type": "uri", "value": "urn:failure:1"}
        })];
        let joined = inner_join_sparql_json(&left, &right, 10);
        assert!(joined.is_ok());
        let projected = joined.and_then(|rows| {
            project_sparql_json(&rows, &["failure".to_owned(), "node".to_owned()])
        });
        assert!(projected.is_ok());
        assert_eq!(projected.map(|rows| rows.len()), Ok(2));
    }

    #[test]
    fn json_join_fails_before_intermediate_growth_exceeds_limit() {
        let rows = vec![json!({"x": {"type": "uri", "value": "urn:x"}}); 2];
        assert_eq!(
            inner_join_sparql_json(&rows, &rows, 3),
            Err(ExecutionError::IntermediateRowLimit)
        );
    }

    #[test]
    fn grace_bucket_hash_is_domain_separated_from_primary_shuffle() {
        let keys = vec!["x".to_owned()];
        let rows = (0..4096)
            .map(|value| json!({"x": {"type": "uri", "value": format!("urn:x:{value}")}}))
            .collect::<Vec<_>>();
        let local_buckets = rows
            .iter()
            .filter_map(|row| match shuffle_partition_for_binding(row, &keys, 64) {
                Ok(0) => Some(grace_partition_for_binding(row, &keys, 64)),
                Ok(_) => None,
                Err(error) => Some(Err(error)),
            })
            .collect::<Result<BTreeSet<_>, _>>();
        assert!(local_buckets.is_ok_and(|buckets| buckets.len() > 1));
    }

    #[test]
    fn arrow_fragment_round_trip_preserves_terms_unbound_values_and_bag_rows() {
        let metadata = FragmentBatchMetadata {
            dataset_id: Uuid::from_u128(1),
            snapshot_id: Uuid::from_u128(2),
            query_sha256: "1".repeat(64),
            fragment_id: "fragment-0001".to_owned(),
            worker_id: "worker-2".to_owned(),
            multiset_sha256: "2".repeat(64),
        };
        let head = vec!["node".to_owned(), "label".to_owned(), "missing".to_owned()];
        let bindings = vec![
            json!({
                "node": {"type": "uri", "value": "urn:node:1"},
                "label": {"type": "literal", "value": "one", "xml:lang": "en"}
            }),
            json!({
                "node": {"type": "uri", "value": "urn:node:1"},
                "label": {"type": "literal", "value": "1", "datatype": "http://www.w3.org/2001/XMLSchema#integer"}
            }),
        ];
        let mut bytes = Vec::new();
        assert!(write_fragment_arrow_stream(&mut bytes, &metadata, &head, &bindings, 1).is_ok());
        let decoded = read_fragment_arrow_stream(&bytes, 2);
        assert!(decoded.is_ok());
        assert_eq!(
            decoded.map(|batch| (batch.metadata, batch.head, batch.bindings)),
            Ok((metadata, head, bindings))
        );
    }

    #[test]
    fn arrow_fragment_decode_enforces_row_ceiling() {
        let metadata = FragmentBatchMetadata {
            dataset_id: Uuid::from_u128(1),
            snapshot_id: Uuid::from_u128(2),
            query_sha256: "1".repeat(64),
            fragment_id: "fragment-0001".to_owned(),
            worker_id: "worker-2".to_owned(),
            multiset_sha256: "2".repeat(64),
        };
        let bindings = vec![json!({"x": {"type": "bnode", "value": "b1"}}); 2];
        let mut bytes = Vec::new();
        assert!(
            write_fragment_arrow_stream(&mut bytes, &metadata, &["x".to_owned()], &bindings, 1,)
                .is_ok()
        );
        assert_eq!(
            read_fragment_arrow_stream(&bytes, 1),
            Err(ExecutionError::IntermediateRowLimit)
        );
    }

    #[test]
    fn incremental_fragment_decoder_exposes_metadata_and_enforces_rows()
    -> Result<(), Box<dyn std::error::Error>> {
        let metadata = FragmentBatchMetadata {
            dataset_id: Uuid::from_u128(11),
            snapshot_id: Uuid::from_u128(12),
            query_sha256: "a".repeat(64),
            fragment_id: "fragment-0011".to_owned(),
            worker_id: "worker-12".to_owned(),
            multiset_sha256: "b".repeat(64),
        };
        let head = vec!["x".to_owned()];
        let rows = vec![
            json!({"x": {"type": "uri", "value": "urn:x:1"}}),
            json!({"x": {"type": "uri", "value": "urn:x:2"}}),
        ];
        let mut bytes = Vec::new();
        write_fragment_arrow_stream(&mut bytes, &metadata, &head, &rows, 1)?;
        let mut stream = FragmentBindingStream::try_new(Cursor::new(&bytes), 2)?;
        assert_eq!(stream.metadata(), &metadata);
        assert_eq!(stream.head(), head.as_slice());
        assert_eq!(stream.by_ref().collect::<Result<Vec<_>, _>>()?, rows);
        let mut limited = FragmentBindingStream::try_new(Cursor::new(&bytes), 1)?;
        assert_eq!(limited.next().transpose()?, Some(rows[0].clone()));
        assert_eq!(
            limited.next(),
            Some(Err(ExecutionError::IntermediateRowLimit))
        );
        assert_eq!(limited.next(), None);
        Ok(())
    }

    #[test]
    fn arrow_fragment_decode_rejects_a_truncated_stream() {
        let metadata = FragmentBatchMetadata {
            dataset_id: Uuid::from_u128(1),
            snapshot_id: Uuid::from_u128(2),
            query_sha256: "1".repeat(64),
            fragment_id: "fragment-0001".to_owned(),
            worker_id: "worker-2".to_owned(),
            multiset_sha256: "2".repeat(64),
        };
        let mut bytes = Vec::new();
        assert!(
            write_fragment_arrow_stream(
                &mut bytes,
                &metadata,
                &["x".to_owned()],
                &[json!({"x": {"type": "uri", "value": "urn:x"}})],
                1,
            )
            .is_ok()
        );
        bytes.truncate(bytes.len().saturating_sub(1));
        assert!(matches!(
            read_fragment_arrow_stream(&bytes, 1),
            Err(ExecutionError::InvalidArrowStream(_))
        ));
    }

    #[test]
    fn arrow_fragment_encoder_rejects_non_rdf_json_terms() {
        let metadata = FragmentBatchMetadata {
            dataset_id: Uuid::from_u128(1),
            snapshot_id: Uuid::from_u128(2),
            query_sha256: "1".repeat(64),
            fragment_id: "fragment-0001".to_owned(),
            worker_id: "worker-2".to_owned(),
            multiset_sha256: "2".repeat(64),
        };
        let mut bytes = Vec::new();
        assert!(matches!(
            write_fragment_arrow_stream(
                &mut bytes,
                &metadata,
                &["x".to_owned()],
                &[json!({"x": {"type": "uri", "value": "urn:x", "extra": true}})],
                1,
            ),
            Err(ExecutionError::InvalidSparqlTerm(_))
        ));
    }

    #[test]
    fn shuffle_stream_round_trip_preserves_partitioned_bags() {
        let left = vec![
            json!({"x": {"type": "uri", "value": "urn:x:1"}}),
            json!({"x": {"type": "uri", "value": "urn:x:1"}}),
            json!({"x": {"type": "uri", "value": "urn:x:2"}}),
        ];
        let right = vec![
            json!({"x": {"type": "uri", "value": "urn:x:1"}, "y": {"type": "bnode", "value": "b1"}}),
            json!({"x": {"type": "uri", "value": "urn:x:2"}, "y": {"type": "bnode", "value": "b2"}}),
        ];
        let keys = vec!["x".to_owned()];
        let left_partitions = partition_sparql_json(&left, &keys, 2);
        let right_partitions = partition_sparql_json(&right, &keys, 2);
        assert!(left_partitions.is_ok());
        assert!(right_partitions.is_ok());
        for (partition, (left_rows, right_rows)) in left_partitions
            .unwrap_or_default()
            .into_iter()
            .zip(right_partitions.unwrap_or_default())
            .enumerate()
        {
            let input = ShuffleJoinInput {
                metadata: ShuffleJoinMetadata {
                    dataset_id: Uuid::from_u128(1),
                    snapshot_id: Uuid::from_u128(2),
                    query_sha256: "1".repeat(64),
                    plan_sha256: "2".repeat(64),
                    request_id: Uuid::from_u128(3),
                    stage: 0,
                    partition: u32::try_from(partition).unwrap_or(u32::MAX),
                    partition_count: 2,
                },
                left_head: vec!["x".to_owned()],
                right_head: vec!["x".to_owned(), "y".to_owned()],
                key_variables: keys.clone(),
                left_bindings: left_rows,
                right_bindings: right_rows,
            };
            let mut bytes = Vec::new();
            assert!(write_shuffle_join_stream(&mut bytes, &input, 1).is_ok());
            assert_eq!(read_shuffle_join_stream(&bytes, 10), Ok(input));
        }
    }

    #[test]
    fn shuffle_stream_exposes_counts_and_decodes_incrementally()
    -> Result<(), Box<dyn std::error::Error>> {
        let keys = vec!["x".to_owned()];
        let all_left = vec![
            json!({"x": {"type": "uri", "value": "urn:x:1"}}),
            json!({"x": {"type": "uri", "value": "urn:x:2"}}),
        ];
        let all_right = vec![
            json!({"x": {"type": "uri", "value": "urn:x:1"}, "y": {"type": "literal", "value": "a"}}),
            json!({"x": {"type": "uri", "value": "urn:x:2"}, "y": {"type": "literal", "value": "b"}}),
        ];
        let mut left_partitions = partition_sparql_json(&all_left, &keys, 2)?;
        let mut right_partitions = partition_sparql_json(&all_right, &keys, 2)?;
        let left = left_partitions.remove(0);
        let right = right_partitions.remove(0);
        let input = ShuffleJoinInput {
            metadata: ShuffleJoinMetadata {
                dataset_id: Uuid::from_u128(1),
                snapshot_id: Uuid::from_u128(2),
                query_sha256: "1".repeat(64),
                plan_sha256: "2".repeat(64),
                request_id: Uuid::from_u128(3),
                stage: 0,
                partition: 0,
                partition_count: 2,
            },
            left_head: vec!["x".to_owned()],
            right_head: vec!["x".to_owned(), "y".to_owned()],
            key_variables: keys,
            left_bindings: left.clone(),
            right_bindings: right.clone(),
        };
        let mut bytes = Vec::new();
        assert!(write_shuffle_join_stream(&mut bytes, &input, 1).is_ok());
        let mut stream = ShuffleJoinStream::try_new(Cursor::new(&bytes), 10)?;
        assert_eq!(stream.header().left_row_count, u64::try_from(left.len())?);
        assert_eq!(stream.header().right_row_count, u64::try_from(right.len())?);
        let decoded = stream.by_ref().collect::<Result<Vec<_>, _>>();
        assert!(decoded.is_ok());
        let decoded = decoded.unwrap_or_default();
        assert_eq!(
            decoded.iter().filter(|(side, _)| *side == 0).count(),
            left.len()
        );
        assert_eq!(
            decoded.iter().filter(|(side, _)| *side == 1).count(),
            right.len()
        );
        Ok(())
    }

    #[test]
    fn incremental_shuffle_writer_matches_owned_writer_and_fails_on_source_error()
    -> Result<(), Box<dyn std::error::Error>> {
        let keys = vec!["x".to_owned()];
        let mut left_partitions = partition_sparql_json(
            &[
                json!({"x": {"type": "uri", "value": "urn:x:1"}}),
                json!({"x": {"type": "uri", "value": "urn:x:2"}}),
            ],
            &keys,
            2,
        )?;
        let mut right_partitions = partition_sparql_json(
            &[
                json!({"x": {"type": "uri", "value": "urn:x:1"}, "y": {"type": "literal", "value": "a"}}),
            ],
            &keys,
            2,
        )?;
        let input = ShuffleJoinInput {
            metadata: ShuffleJoinMetadata {
                dataset_id: Uuid::from_u128(1),
                snapshot_id: Uuid::from_u128(2),
                query_sha256: "1".repeat(64),
                plan_sha256: "2".repeat(64),
                request_id: Uuid::from_u128(3),
                stage: 0,
                partition: 0,
                partition_count: 2,
            },
            left_head: vec!["x".to_owned()],
            right_head: vec!["x".to_owned(), "y".to_owned()],
            key_variables: keys,
            left_bindings: left_partitions.remove(0),
            right_bindings: right_partitions.remove(0),
        };
        let header = super::ShuffleJoinStreamHeader {
            metadata: input.metadata.clone(),
            left_head: input.left_head.clone(),
            right_head: input.right_head.clone(),
            key_variables: input.key_variables.clone(),
            left_row_count: u64::try_from(input.left_bindings.len())?,
            right_row_count: u64::try_from(input.right_bindings.len())?,
        };
        let mut owned = Vec::new();
        write_shuffle_join_stream(&mut owned, &input, 1)?;
        let mut incremental = Vec::new();
        write_shuffle_join_stream_iter(
            &mut incremental,
            &header,
            input.left_bindings.iter().cloned().map(Ok),
            input.right_bindings.iter().cloned().map(Ok),
            1,
        )?;
        assert_eq!(read_shuffle_join_stream(&owned, 10), Ok(input.clone()));
        assert_eq!(
            read_shuffle_join_stream(&incremental, 10),
            Ok(input.clone())
        );
        let failed_left =
            input
                .left_bindings
                .iter()
                .take(1)
                .cloned()
                .map(Ok)
                .chain(std::iter::once(Err(ExecutionError::InvalidArrowStream(
                    "source corruption".to_owned(),
                ))));
        let mut partial = Vec::new();
        assert!(
            write_shuffle_join_stream_iter(
                &mut partial,
                &header,
                failed_left,
                input.right_bindings.iter().cloned().map(Ok),
                1,
            )
            .is_err()
        );
        assert!(!partial.ends_with(&super::ARROW_STREAM_EOS));
        let mut wrong_count = header.clone();
        wrong_count.left_row_count = wrong_count.left_row_count.saturating_add(1);
        let mut incomplete = Vec::new();
        assert!(
            write_shuffle_join_stream_iter(
                &mut incomplete,
                &wrong_count,
                input.left_bindings.iter().cloned().map(Ok),
                input.right_bindings.iter().cloned().map(Ok),
                1,
            )
            .is_err()
        );
        assert!(!incomplete.ends_with(&super::ARROW_STREAM_EOS));
        Ok(())
    }

    #[test]
    fn shuffle_partition_rejects_unbound_keys() {
        assert_eq!(
            shuffle_partition_for_binding(&json!({}), &["x".to_owned()], 4),
            Err(ExecutionError::UnboundShuffleKey("x".to_owned()))
        );
    }

    #[test]
    fn shuffle_partition_rejects_an_empty_join_key() {
        assert_eq!(
            shuffle_partition_for_binding(&json!({}), &[], 4),
            Err(ExecutionError::EmptyShuffleKey)
        );
    }

    #[test]
    fn partition_join_union_equals_the_complete_bag_join() {
        let left = vec![
            json!({"x": {"type": "uri", "value": "urn:x:1"}}),
            json!({"x": {"type": "uri", "value": "urn:x:1"}}),
            json!({
                "x": {
                    "type": "literal",
                    "value": "2",
                    "datatype": "http://www.w3.org/2001/XMLSchema#integer"
                }
            }),
        ];
        let right = vec![
            json!({
                "x": {"type": "uri", "value": "urn:x:1"},
                "y": {"type": "literal", "value": "one", "xml:lang": "en"}
            }),
            json!({
                "x": {
                    "type": "literal",
                    "value": "2",
                    "datatype": "http://www.w3.org/2001/XMLSchema#integer"
                },
                "y": {"type": "bnode", "value": "b2"}
            }),
        ];
        let keys = vec!["x".to_owned()];
        let partitioned = partition_sparql_json(&left, &keys, 4).and_then(|left_parts| {
            partition_sparql_json(&right, &keys, 4).and_then(|right_parts| {
                left_parts.into_iter().zip(right_parts).try_fold(
                    Vec::new(),
                    |mut union, (left_rows, right_rows)| {
                        let mut joined = inner_join_sparql_json(&left_rows, &right_rows, 16)?;
                        union.append(&mut joined);
                        Ok(union)
                    },
                )
            })
        });
        let complete = inner_join_sparql_json(&left, &right, 16);
        assert!(partitioned.is_ok());
        assert!(complete.is_ok());
        let mut partitioned_rows = partitioned
            .map(|rows| {
                rows.into_iter()
                    .map(|row| row.to_string())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let mut complete_rows = complete
            .map(|rows| {
                rows.into_iter()
                    .map(|row| row.to_string())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        partitioned_rows.sort();
        complete_rows.sort();
        assert_eq!(partitioned_rows, complete_rows);
        assert_eq!(complete_rows.len(), 3);
    }
}
