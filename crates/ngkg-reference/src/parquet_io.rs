//! Deterministic Arrow/Parquet writers and direct row-group hydration.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs::File,
    path::Path,
    sync::Arc,
};

use arrow_array::{
    Array, ArrayRef, RecordBatch, StringArray, UInt8Array, UInt32Array, UInt64Array,
    builder::{FixedSizeBinaryBuilder, StringBuilder},
    new_null_array,
};
use arrow_schema::{DataType, Field, Schema};
use ngkg_dataset::{GraphCatalog, LogicalGraphName};
use ngkg_projection::{ObjectKind, semantic_spine_schema, validate_semantic_batch};
use parquet::{
    arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder},
    file::properties::WriterProperties,
};
use serde::Serialize;
use thiserror::Error;
use uuid::Uuid;

use crate::{
    locator::LocatorRecord,
    model::{HydratedPayload, Treatment},
    rdf::{
        DEFAULT_GRAPH_STORAGE_KEY, GraphScope, NormalizedFact, NormalizedObject, ResourceTermKind,
        public_resource_lexical,
    },
};

/// Dictionary entry for categories that are always IRIs or lexical strings.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DictionaryEntry {
    pub id: u64,
    pub term: String,
}

/// Entity entry retains RDF term kind independently from its dense ID.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EntityDictionaryEntry {
    pub id: u64,
    pub resource_kind: ResourceTermKind,
    pub term: String,
}

/// Graph entry retains logical default-versus-named identity.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GraphDictionaryEntry {
    pub id: u64,
    pub scope: GraphScope,
    pub iri: Option<String>,
}

/// Immutable local dictionary contract used by the reference compiler.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DictionaryFile {
    pub format_version: u32,
    pub entities: Vec<EntityDictionaryEntry>,
    pub predicates: Vec<DictionaryEntry>,
    pub datatypes: Vec<DictionaryEntry>,
    pub languages: Vec<DictionaryEntry>,
    pub graphs: Vec<GraphDictionaryEntry>,
    pub root_sha256: String,
}

/// Dense maps consumed only after the typed dictionary file has been committed.
pub struct EncodedDictionaries {
    pub file: DictionaryFile,
    /// Key format is `<I|B>\t<canonical-key>`; it is never emitted as public RDF.
    pub entities: BTreeMap<String, u64>,
    pub predicates: BTreeMap<String, u32>,
    pub datatypes: BTreeMap<String, u32>,
    pub languages: BTreeMap<String, u32>,
    /// Physical graph key. The default uses [`DEFAULT_GRAPH_STORAGE_KEY`].
    pub graphs: BTreeMap<String, u32>,
}

#[derive(Debug, Error)]
pub enum ParquetIoError {
    #[error("Arrow operation failed: {0}")]
    Arrow(#[from] arrow_schema::ArrowError),
    #[error("Parquet operation failed: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
    #[error("file I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("dictionary exceeds the configured maximum")]
    DictionaryLimit,
    #[error("dictionary category exceeds its integer encoding")]
    DictionaryEncodingOverflow,
    #[error("semantic fact references a missing dictionary term: {0}")]
    MissingDictionary(String),
    #[error("payload Parquet schema is missing or has an invalid column: {0}")]
    InvalidPayloadColumn(&'static str),
    #[error("locator points outside the addressed Parquet row group")]
    InvalidPayloadRow,
    #[error("RDF dataset graph catalog is invalid: {0}")]
    GraphCatalog(String),
}

/// Build deterministic typed dictionaries, including declared empty graphs.
pub fn build_dictionaries(
    facts: &[NormalizedFact],
    graph_catalog: Option<&GraphCatalog>,
    max_terms: u64,
) -> Result<EncodedDictionaries, ParquetIoError> {
    let mut entities = BTreeSet::new();
    let mut predicates = BTreeSet::new();
    let mut datatypes = BTreeSet::new();
    let mut languages = BTreeSet::new();
    let mut graphs = BTreeSet::new();

    if let Some(catalog) = graph_catalog {
        catalog
            .validate()
            .map_err(|error| ParquetIoError::GraphCatalog(error.to_string()))?;
        for graph in &catalog.graphs {
            match &graph.name {
                LogicalGraphName::Default => {
                    graphs.insert(DEFAULT_GRAPH_STORAGE_KEY.to_owned());
                }
                LogicalGraphName::Named { iri } => {
                    graphs.insert(iri.clone());
                }
            }
        }
    }

    for fact in facts {
        if !fact.graph_scope.matches_storage_key(&fact.graph_iri) {
            return Err(ParquetIoError::GraphCatalog(
                "fact graph scope differs from its physical graph key".to_owned(),
            ));
        }
        if let Some(catalog) = graph_catalog {
            let known = match fact.graph_scope {
                GraphScope::Default => catalog
                    .by_id(0)
                    .is_some_and(|graph| matches!(graph.name, LogicalGraphName::Default)),
                GraphScope::Named => catalog.named(&fact.graph_iri).is_some(),
            };
            if !known {
                return Err(ParquetIoError::GraphCatalog(format!(
                    "fact references graph absent from the catalog: {}",
                    fact.graph_iri
                )));
            }
        }
        entities.insert(resource_dictionary_key(
            fact.subject_term_kind,
            &fact.subject_iri,
        ));
        predicates.insert(fact.predicate_iri.clone());
        graphs.insert(fact.graph_iri.clone());
        match &fact.object {
            NormalizedObject::Entity { iri, term_kind, .. } => {
                entities.insert(resource_dictionary_key(*term_kind, iri));
            }
            NormalizedObject::Literal {
                datatype_iri,
                language,
                ..
            } => {
                datatypes.insert(datatype_iri.clone());
                if let Some(language) = language {
                    languages.insert(language.clone());
                }
            }
        }
    }

    let total = entities
        .len()
        .saturating_add(predicates.len())
        .saturating_add(datatypes.len())
        .saturating_add(languages.len())
        .saturating_add(graphs.len());
    if u64::try_from(total).unwrap_or(u64::MAX) > max_terms {
        return Err(ParquetIoError::DictionaryLimit);
    }

    let entities = assign_u64(entities);
    let predicates = assign_u32(predicates)?;
    let datatypes = assign_u32(datatypes)?;
    let languages = assign_u32(languages)?;
    let graphs = assign_u32(graphs)?;

    let canonical = serde_json::to_vec(&(
        "ngkg-reference-dictionaries-v2",
        &entities,
        &predicates,
        &datatypes,
        &languages,
        &graphs,
    ))
    .map_err(|error| std::io::Error::other(error.to_string()))?;
    let root_sha256 = crate::sha256_hex(&canonical);
    let file = DictionaryFile {
        format_version: 2,
        entities: entity_entries(&entities)?,
        predicates: entries_u32(&predicates),
        datatypes: entries_u32(&datatypes),
        languages: entries_u32(&languages),
        graphs: graph_entries(&graphs)?,
        root_sha256,
    };
    Ok(EncodedDictionaries {
        file,
        entities,
        predicates,
        datatypes,
        languages,
        graphs,
    })
}

pub fn write_semantic_spine(
    path: &Path,
    facts: &[NormalizedFact],
    dictionaries: &EncodedDictionaries,
    source_guid: Uuid,
    snapshot_id: Uuid,
    mapping_id: &str,
    row_group_rows: usize,
) -> Result<u64, ParquetIoError> {
    let semantic = facts
        .iter()
        .filter(|fact| fact.treatment != Treatment::Payload)
        .collect::<Vec<_>>();
    let mut fact_ids = FixedSizeBinaryBuilder::new(16);
    let mut fact_hashes = FixedSizeBinaryBuilder::new(32);
    let mut source_guids = FixedSizeBinaryBuilder::new(16);
    let mut snapshot_ids = FixedSizeBinaryBuilder::new(16);
    let mut value_utf8 = StringBuilder::new();
    let mut object_ids = Vec::with_capacity(semantic.len());
    let mut datatype_ids = Vec::with_capacity(semantic.len());
    let mut language_ids = Vec::with_capacity(semantic.len());
    let mut subject_ids = Vec::with_capacity(semantic.len());
    let mut subject_term_kinds = Vec::with_capacity(semantic.len());
    let mut predicate_ids = Vec::with_capacity(semantic.len());
    let mut object_kinds = Vec::with_capacity(semantic.len());
    let mut object_term_kinds = Vec::with_capacity(semantic.len());
    let mut graph_ids = Vec::with_capacity(semantic.len());
    let mut graph_scopes = Vec::with_capacity(semantic.len());

    for fact in &semantic {
        fact_ids.append_value(fact.fact_id)?;
        fact_hashes.append_value(fact.fact_hash)?;
        source_guids.append_value(source_guid.as_bytes())?;
        snapshot_ids.append_value(snapshot_id.as_bytes())?;
        subject_ids.push(required_u64(
            &dictionaries.entities,
            &resource_dictionary_key(fact.subject_term_kind, &fact.subject_iri),
        )?);
        subject_term_kinds.push(fact.subject_term_kind.code());
        predicate_ids.push(required_u32(&dictionaries.predicates, &fact.predicate_iri)?);
        graph_ids.push(required_u32(&dictionaries.graphs, &fact.graph_iri)?);
        graph_scopes.push(fact.graph_scope.code());
        match &fact.object {
            NormalizedObject::Entity { iri, term_kind, .. } => {
                object_kinds.push(ObjectKind::Entity as u8);
                object_term_kinds.push(Some(term_kind.code()));
                object_ids.push(Some(required_u64(
                    &dictionaries.entities,
                    &resource_dictionary_key(*term_kind, iri),
                )?));
                datatype_ids.push(None);
                language_ids.push(None);
                value_utf8.append_null();
            }
            NormalizedObject::Literal {
                lexical_value,
                datatype_iri,
                language,
                ..
            } => {
                object_kinds.push(ObjectKind::Utf8 as u8);
                object_term_kinds.push(None);
                object_ids.push(None);
                datatype_ids.push(Some(required_u32(&dictionaries.datatypes, datatype_iri)?));
                language_ids.push(
                    language
                        .as_ref()
                        .map(|value| required_u32(&dictionaries.languages, value))
                        .transpose()?,
                );
                value_utf8.append_value(lexical_value);
            }
        }
    }

    let len = semantic.len();
    let schema = Arc::new(semantic_spine_schema());
    let columns: Vec<ArrayRef> = vec![
        Arc::new(fact_ids.finish()),
        Arc::new(fact_hashes.finish()),
        Arc::new(UInt64Array::from(subject_ids)),
        Arc::new(UInt8Array::from(subject_term_kinds)),
        Arc::new(UInt32Array::from(predicate_ids)),
        Arc::new(UInt8Array::from(object_kinds)),
        Arc::new(UInt8Array::from(object_term_kinds)),
        Arc::new(UInt64Array::from(object_ids)),
        new_null_array(&DataType::Boolean, len),
        new_null_array(&DataType::Int64, len),
        new_null_array(&DataType::Float64, len),
        new_null_array(&DataType::Decimal128(38, 12), len),
        new_null_array(
            &DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, Some(Arc::from("UTC"))),
            len,
        ),
        Arc::new(value_utf8.finish()),
        new_null_array(&DataType::Binary, len),
        Arc::new(UInt32Array::from(datatype_ids)),
        Arc::new(UInt32Array::from(language_ids)),
        Arc::new(UInt32Array::from(graph_ids)),
        Arc::new(UInt8Array::from(graph_scopes)),
        Arc::new(source_guids.finish()),
        Arc::new(StringArray::from(vec![mapping_id; len])),
        Arc::new(UInt32Array::from(vec![0_u32; len])),
        Arc::new(snapshot_ids.finish()),
    ];
    let batch = RecordBatch::try_new(schema.clone(), columns)?;
    validate_semantic_batch(&batch).map_err(|error| std::io::Error::other(error.to_string()))?;
    write_batch(path, batch, row_group_rows)?;
    Ok(u64::try_from(len).unwrap_or(u64::MAX))
}

pub fn write_payload(
    path: &Path,
    facts: &[NormalizedFact],
    dictionaries: &EncodedDictionaries,
    source_guid_value: Uuid,
    snapshot_id: Uuid,
    row_group_rows: usize,
) -> Result<(u64, Vec<LocatorRecord>), ParquetIoError> {
    if row_group_rows == 0 {
        return Err(ParquetIoError::InvalidPayloadRow);
    }
    let payload = facts
        .iter()
        .filter(|fact| fact.treatment == Treatment::Payload)
        .collect::<Vec<_>>();
    let schema = Arc::new(payload_schema());
    let mut fact_id = FixedSizeBinaryBuilder::new(16);
    let mut fact_hash = FixedSizeBinaryBuilder::new(32);
    let mut subject_guid = FixedSizeBinaryBuilder::new(16);
    let mut source_guid = FixedSizeBinaryBuilder::new(16);
    let mut snapshot_guid = FixedSizeBinaryBuilder::new(16);
    let mut subject_term = StringBuilder::new();
    let mut subject_resource_kind = Vec::with_capacity(payload.len());
    let mut predicate_iri = StringBuilder::new();
    let mut lexical_value = StringBuilder::new();
    let mut datatype_iri = StringBuilder::new();
    let mut language = StringBuilder::new();
    let mut graph_scope = Vec::with_capacity(payload.len());
    let mut graph_iri = StringBuilder::new();
    let mut locator = Vec::with_capacity(payload.len());

    for (index, fact) in payload.iter().enumerate() {
        let NormalizedObject::Literal {
            lexical_value: lexical,
            datatype_iri: datatype,
            language: lang,
            ..
        } = &fact.object
        else {
            return Err(ParquetIoError::InvalidPayloadColumn(
                "payload object must be a literal",
            ));
        };
        fact_id.append_value(fact.fact_id)?;
        fact_hash.append_value(fact.fact_hash)?;
        subject_guid.append_value(fact.subject_guid.as_bytes())?;
        source_guid.append_value(source_guid_value.as_bytes())?;
        snapshot_guid.append_value(snapshot_id.as_bytes())?;
        subject_term.append_value(public_resource_lexical(
            fact.subject_term_kind,
            &fact.subject_iri,
        ));
        subject_resource_kind.push(fact.subject_term_kind.code());
        predicate_iri.append_value(&fact.predicate_iri);
        lexical_value.append_value(lexical);
        datatype_iri.append_value(datatype);
        if let Some(lang) = lang {
            language.append_value(lang);
        } else {
            language.append_null();
        }
        match fact.graph_scope {
            GraphScope::Default => {
                graph_scope.push(0_u8);
                graph_iri.append_null();
            }
            GraphScope::Named => {
                graph_scope.push(1_u8);
                graph_iri.append_value(&fact.graph_iri);
            }
        }
        locator.push(LocatorRecord {
            entity_guid: fact.subject_guid,
            row_group: u32::try_from(index / row_group_rows)
                .map_err(|_| ParquetIoError::DictionaryEncodingOverflow)?,
            row_in_group: u32::try_from(index % row_group_rows)
                .map_err(|_| ParquetIoError::DictionaryEncodingOverflow)?,
            graph_id: required_u32(&dictionaries.graphs, &fact.graph_iri)?,
            predicate_id: required_u32(&dictionaries.predicates, &fact.predicate_iri)?,
        });
    }

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(fact_id.finish()),
            Arc::new(fact_hash.finish()),
            Arc::new(subject_guid.finish()),
            Arc::new(source_guid.finish()),
            Arc::new(subject_term.finish()),
            Arc::new(UInt8Array::from(subject_resource_kind)),
            Arc::new(predicate_iri.finish()),
            Arc::new(lexical_value.finish()),
            Arc::new(datatype_iri.finish()),
            Arc::new(language.finish()),
            Arc::new(UInt8Array::from(graph_scope)),
            Arc::new(graph_iri.finish()),
            Arc::new(snapshot_guid.finish()),
        ],
    )?;
    write_batch(path, batch, row_group_rows)?;
    Ok((u64::try_from(payload.len()).unwrap_or(u64::MAX), locator))
}

pub fn hydrate_rows(
    payload_path: &Path,
    records: &[LocatorRecord],
) -> Result<Vec<HydratedPayload>, ParquetIoError> {
    let mut grouped: BTreeMap<u32, Vec<u32>> = BTreeMap::new();
    for record in records {
        grouped
            .entry(record.row_group)
            .or_default()
            .push(record.row_in_group);
    }
    let mut hydrated = Vec::new();
    for (row_group, wanted) in grouped {
        let hydrated_before = hydrated.len();
        let file = File::open(payload_path)?;
        let mut reader = ParquetRecordBatchReaderBuilder::try_new(file)?
            .with_row_groups(vec![
                usize::try_from(row_group).map_err(|_| ParquetIoError::InvalidPayloadRow)?,
            ])
            .build()?;
        let wanted = wanted.into_iter().collect::<BTreeSet<_>>();
        let mut row_offset = 0_u32;
        for batch in &mut reader {
            let batch = batch?;
            for row in 0..batch.num_rows() {
                let absolute = row_offset
                    .checked_add(u32::try_from(row).map_err(|_| ParquetIoError::InvalidPayloadRow)?)
                    .ok_or(ParquetIoError::InvalidPayloadRow)?;
                if wanted.contains(&absolute) {
                    hydrated.push(payload_from_batch(&batch, row)?);
                }
            }
            row_offset = row_offset
                .checked_add(
                    u32::try_from(batch.num_rows())
                        .map_err(|_| ParquetIoError::InvalidPayloadRow)?,
                )
                .ok_or(ParquetIoError::InvalidPayloadRow)?;
        }
        if hydrated.len().saturating_sub(hydrated_before) != wanted.len() {
            return Err(ParquetIoError::InvalidPayloadRow);
        }
    }
    hydrated.sort_unstable_by(|left, right| {
        (
            left.subject_resource_kind,
            &left.subject_term,
            &left.predicate_iri,
            &left.lexical_value,
            left.graph_scope,
            &left.graph_iri,
        )
            .cmp(&(
                right.subject_resource_kind,
                &right.subject_term,
                &right.predicate_iri,
                &right.lexical_value,
                right.graph_scope,
                &right.graph_iri,
            ))
    });
    Ok(hydrated)
}

fn write_batch(
    path: &Path,
    batch: RecordBatch,
    row_group_rows: usize,
) -> Result<(), ParquetIoError> {
    if row_group_rows == 0 {
        return Err(ParquetIoError::InvalidPayloadRow);
    }
    let properties = WriterProperties::builder()
        .set_max_row_group_size(row_group_rows)
        .build();
    let mut writer = ArrowWriter::try_new(File::create(path)?, batch.schema(), Some(properties))?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}

fn payload_schema() -> Schema {
    Schema::new(vec![
        Field::new("fact_id128", DataType::FixedSizeBinary(16), false),
        Field::new("fact_hash256", DataType::FixedSizeBinary(32), false),
        Field::new("subject_guid128", DataType::FixedSizeBinary(16), false),
        Field::new("source_guid128", DataType::FixedSizeBinary(16), false),
        Field::new("subject_term", DataType::Utf8, false),
        Field::new("subject_resource_kind", DataType::UInt8, false),
        Field::new("predicate_iri", DataType::Utf8, false),
        Field::new("lexical_value", DataType::Utf8, false),
        Field::new("datatype_iri", DataType::Utf8, false),
        Field::new("language", DataType::Utf8, true),
        Field::new("graph_scope", DataType::UInt8, false),
        Field::new("graph_iri", DataType::Utf8, true),
        Field::new("snapshot_id128", DataType::FixedSizeBinary(16), false),
    ])
}

fn payload_from_batch(batch: &RecordBatch, row: usize) -> Result<HydratedPayload, ParquetIoError> {
    if row >= batch.num_rows() {
        return Err(ParquetIoError::InvalidPayloadRow);
    }
    let subject = string_column(batch, "subject_term")?;
    let subject_kind = u8_column(batch, "subject_resource_kind")?;
    let predicate = string_column(batch, "predicate_iri")?;
    let lexical = string_column(batch, "lexical_value")?;
    let datatype = string_column(batch, "datatype_iri")?;
    let language = string_column(batch, "language")?;
    let graph_scope = u8_column(batch, "graph_scope")?;
    let graph = string_column(batch, "graph_iri")?;

    let subject_resource_kind = match subject_kind.value(row) {
        1 => ResourceTermKind::NamedNode,
        2 => ResourceTermKind::BlankNode,
        _ => return Err(ParquetIoError::InvalidPayloadRow),
    };
    let subject_term = subject.value(row).to_owned();
    let subject_is_valid = match subject_resource_kind {
        ResourceTermKind::NamedNode => !subject_term.is_empty() && !subject_term.starts_with("_:"),
        ResourceTermKind::BlankNode => subject_term
            .strip_prefix("_:")
            .is_some_and(|label| !label.is_empty()),
    };
    if !subject_is_valid {
        return Err(ParquetIoError::InvalidPayloadRow);
    }

    let (graph_scope, graph_iri) = match graph_scope.value(row) {
        0 if graph.is_null(row) => (GraphScope::Default, None),
        1 if !graph.is_null(row) && !graph.value(row).is_empty() => {
            (GraphScope::Named, Some(graph.value(row).to_owned()))
        }
        _ => return Err(ParquetIoError::InvalidPayloadRow),
    };

    Ok(HydratedPayload {
        subject_term,
        subject_resource_kind,
        predicate_iri: predicate.value(row).to_owned(),
        lexical_value: lexical.value(row).to_owned(),
        datatype_iri: Some(datatype.value(row).to_owned()),
        language: (!language.is_null(row)).then(|| language.value(row).to_owned()),
        graph_scope,
        graph_iri,
    })
}

fn string_column<'a>(
    batch: &'a RecordBatch,
    name: &'static str,
) -> Result<&'a StringArray, ParquetIoError> {
    batch
        .column_by_name(name)
        .and_then(|column| column.as_any().downcast_ref::<StringArray>())
        .ok_or(ParquetIoError::InvalidPayloadColumn(name))
}

fn u8_column<'a>(
    batch: &'a RecordBatch,
    name: &'static str,
) -> Result<&'a UInt8Array, ParquetIoError> {
    batch
        .column_by_name(name)
        .and_then(|column| column.as_any().downcast_ref::<UInt8Array>())
        .ok_or(ParquetIoError::InvalidPayloadColumn(name))
}

fn resource_dictionary_key(kind: ResourceTermKind, canonical_key: &str) -> String {
    format!("{}\t{canonical_key}", kind.dictionary_tag())
}

fn assign_u64(values: BTreeSet<String>) -> BTreeMap<String, u64> {
    values
        .into_iter()
        .enumerate()
        .map(|(id, value)| (value, u64::try_from(id).unwrap_or(u64::MAX)))
        .collect()
}

fn assign_u32(values: BTreeSet<String>) -> Result<BTreeMap<String, u32>, ParquetIoError> {
    values
        .into_iter()
        .enumerate()
        .map(|(id, value)| {
            u32::try_from(id)
                .map(|id| (value, id))
                .map_err(|_| ParquetIoError::DictionaryEncodingOverflow)
        })
        .collect()
}

fn entity_entries(
    values: &BTreeMap<String, u64>,
) -> Result<Vec<EntityDictionaryEntry>, ParquetIoError> {
    values
        .iter()
        .map(|(key, id)| {
            let (tag, canonical_key) = key
                .split_once('\t')
                .ok_or_else(|| ParquetIoError::MissingDictionary(key.clone()))?;
            let resource_kind = match tag {
                "I" => ResourceTermKind::NamedNode,
                "B" => ResourceTermKind::BlankNode,
                _ => return Err(ParquetIoError::MissingDictionary(key.clone())),
            };
            Ok(EntityDictionaryEntry {
                id: *id,
                resource_kind,
                term: public_resource_lexical(resource_kind, canonical_key),
            })
        })
        .collect()
}

fn graph_entries(
    values: &BTreeMap<String, u32>,
) -> Result<Vec<GraphDictionaryEntry>, ParquetIoError> {
    values
        .iter()
        .map(|(key, id)| {
            let scope = GraphScope::from_storage_key(key)
                .ok_or_else(|| ParquetIoError::MissingDictionary(key.clone()))?;
            Ok(GraphDictionaryEntry {
                id: u64::from(*id),
                scope,
                iri: matches!(scope, GraphScope::Named).then(|| key.clone()),
            })
        })
        .collect()
}

fn entries_u32(values: &BTreeMap<String, u32>) -> Vec<DictionaryEntry> {
    values
        .iter()
        .map(|(term, id)| DictionaryEntry {
            id: u64::from(*id),
            term: term.clone(),
        })
        .collect()
}

fn required_u64(values: &BTreeMap<String, u64>, key: &str) -> Result<u64, ParquetIoError> {
    values
        .get(key)
        .copied()
        .ok_or_else(|| ParquetIoError::MissingDictionary(key.to_owned()))
}

fn required_u32(values: &BTreeMap<String, u32>, key: &str) -> Result<u32, ParquetIoError> {
    values
        .get(key)
        .copied()
        .ok_or_else(|| ParquetIoError::MissingDictionary(key.to_owned()))
}
