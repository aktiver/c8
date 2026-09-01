//! Arrow-native semantic projection contracts.

use std::{collections::HashMap, sync::Arc};

use arrow_array::{Array, RecordBatch, UInt8Array};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use async_trait::async_trait;
use ngkg_source_planner::WorkEnvelope;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Object layout values stored in `object_kind8`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum ObjectKind {
    Entity = 1,
    Boolean = 2,
    Integer = 3,
    Float = 4,
    Decimal = 5,
    Timestamp = 6,
    Utf8 = 7,
    Binary = 8,
}

/// Versioned semantic-spine schema used by graph and reasoner builders.
#[must_use]
pub fn semantic_spine_schema() -> Schema {
    let fields = vec![
        Field::new("fact_id128", DataType::FixedSizeBinary(16), false),
        Field::new("fact_hash256", DataType::FixedSizeBinary(32), false),
        Field::new("subject_id64", DataType::UInt64, false),
        Field::new("subject_term_kind8", DataType::UInt8, false),
        Field::new("predicate_id32", DataType::UInt32, false),
        Field::new("object_kind8", DataType::UInt8, false),
        Field::new("object_term_kind8", DataType::UInt8, true),
        Field::new("object_id64", DataType::UInt64, true),
        Field::new("value_bool", DataType::Boolean, true),
        Field::new("value_i64", DataType::Int64, true),
        Field::new("value_f64", DataType::Float64, true),
        Field::new("value_decimal128", DataType::Decimal128(38, 12), true),
        Field::new(
            "value_timestamp_ns",
            DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::from("UTC"))),
            true,
        ),
        Field::new("value_utf8", DataType::Utf8, true),
        Field::new("value_binary", DataType::Binary, true),
        Field::new("datatype_id32", DataType::UInt32, true),
        Field::new("language_id32", DataType::UInt32, true),
        Field::new("graph_id32", DataType::UInt32, false),
        Field::new("graph_scope8", DataType::UInt8, false),
        Field::new("source_guid128", DataType::FixedSizeBinary(16), false),
        Field::new("mapping_id", DataType::Utf8, false),
        Field::new("policy_label_set_id", DataType::UInt32, false),
        Field::new("snapshot_id128", DataType::FixedSizeBinary(16), false),
    ];
    Schema::new_with_metadata(
        fields,
        HashMap::from([
            ("ngkg.schema".to_owned(), "semantic-spine".to_owned()),
            ("ngkg.schema.version".to_owned(), "2".to_owned()),
        ]),
    )
}

/// Content-addressed output object.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ContentObject {
    pub uri: String,
    pub sha256: [u8; 32],
    pub bytes: u64,
    pub schema_hash: [u8; 32],
}

/// Evidence returned by one immutable projection partition.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ProjectionOutputManifest {
    pub partition_id: String,
    pub input_hash: [u8; 32],
    pub mapping_hash: [u8; 32],
    pub core_fact_count: u64,
    pub virtual_row_count: u64,
    pub payload_row_count: u64,
    pub rejected_record_count: u64,
    pub output_objects: Vec<ContentObject>,
    pub contribution_objects: Vec<ContentObject>,
}

/// Projection is a streaming implementation boundary; no worker owns publication.
#[async_trait]
pub trait ProjectionExecutor: Send + Sync {
    async fn execute_partition(
        &self,
        envelope: &WorkEnvelope,
        compiled_mapping_hash: [u8; 32],
    ) -> Result<ProjectionOutputManifest, ProjectionError>;
}

/// Semantic-batch failures.
#[derive(Debug, Error)]
pub enum ProjectionError {
    #[error("semantic spine schema does not match version 2")]
    SchemaMismatch,
    #[error("subject_term_kind8 has an unsupported value at row {row}: {value}")]
    InvalidSubjectTermKind { row: usize, value: u8 },
    #[error("object_term_kind8 is invalid for object_kind8 at row {0}")]
    InvalidObjectTermKind(usize),
    #[error("graph_scope8 has an unsupported value at row {row}: {value}")]
    InvalidGraphScope { row: usize, value: u8 },
    #[error("object_kind8 has an unsupported value at row {row}: {value}")]
    UnknownObjectKind { row: usize, value: u8 },
    #[error("row {row} has {populated} object representations; exactly one is required")]
    InvalidObjectRepresentation { row: usize, populated: usize },
    #[error("object representation does not match object_kind8 at row {0}")]
    ObjectKindMismatch(usize),
    #[error("Arrow batch is missing or has an invalid object_kind8 column")]
    InvalidKindColumn,
    #[error("Arrow dependency failed: {0}")]
    Arrow(#[from] arrow_schema::ArrowError),
}

/// Validate that each semantic row has one and only one matching object representation.
pub fn validate_semantic_batch(batch: &RecordBatch) -> Result<(), ProjectionError> {
    if batch.schema().as_ref() != &semantic_spine_schema() {
        return Err(ProjectionError::SchemaMismatch);
    }
    let kinds = batch
        .column_by_name("object_kind8")
        .and_then(|array| array.as_any().downcast_ref::<UInt8Array>())
        .ok_or(ProjectionError::InvalidKindColumn)?;
    let subject_term_kinds = batch
        .column_by_name("subject_term_kind8")
        .and_then(|array| array.as_any().downcast_ref::<UInt8Array>())
        .ok_or(ProjectionError::InvalidKindColumn)?;
    let object_term_kinds = batch
        .column_by_name("object_term_kind8")
        .and_then(|array| array.as_any().downcast_ref::<UInt8Array>())
        .ok_or(ProjectionError::InvalidKindColumn)?;
    let graph_scopes = batch
        .column_by_name("graph_scope8")
        .and_then(|array| array.as_any().downcast_ref::<UInt8Array>())
        .ok_or(ProjectionError::InvalidKindColumn)?;
    let object_columns = [
        "object_id64",
        "value_bool",
        "value_i64",
        "value_f64",
        "value_decimal128",
        "value_timestamp_ns",
        "value_utf8",
        "value_binary",
    ];
    for row in 0..batch.num_rows() {
        let subject_term_kind = subject_term_kinds.value(row);
        if !matches!(subject_term_kind, 1 | 2) {
            return Err(ProjectionError::InvalidSubjectTermKind {
                row,
                value: subject_term_kind,
            });
        }
        let graph_scope = graph_scopes.value(row);
        if !matches!(graph_scope, 0 | 1) {
            return Err(ProjectionError::InvalidGraphScope {
                row,
                value: graph_scope,
            });
        }
        let populated = object_columns
            .iter()
            .filter_map(|name| batch.column_by_name(name))
            .filter(|array| !array.is_null(row))
            .count();
        if populated != 1 {
            return Err(ProjectionError::InvalidObjectRepresentation { row, populated });
        }
        let kind = kinds.value(row);
        let expected = match kind {
            value if value == ObjectKind::Entity as u8 => {
                if object_term_kinds.is_null(row) || !matches!(object_term_kinds.value(row), 1 | 2)
                {
                    return Err(ProjectionError::InvalidObjectTermKind(row));
                }
                "object_id64"
            }
            value if value == ObjectKind::Boolean as u8 => "value_bool",
            value if value == ObjectKind::Integer as u8 => "value_i64",
            value if value == ObjectKind::Float as u8 => "value_f64",
            value if value == ObjectKind::Decimal as u8 => "value_decimal128",
            value if value == ObjectKind::Timestamp as u8 => "value_timestamp_ns",
            value if value == ObjectKind::Utf8 as u8 => "value_utf8",
            value if value == ObjectKind::Binary as u8 => "value_binary",
            value => return Err(ProjectionError::UnknownObjectKind { row, value }),
        };
        if kind != ObjectKind::Entity as u8 && !object_term_kinds.is_null(row) {
            return Err(ProjectionError::InvalidObjectTermKind(row));
        }
        if batch
            .column_by_name(expected)
            .is_none_or(|array| array.is_null(row))
        {
            return Err(ProjectionError::ObjectKindMismatch(row));
        }
    }
    Ok(())
}
