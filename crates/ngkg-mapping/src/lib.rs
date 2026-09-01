//! Closed semantic projection contracts and deterministic compilation.

use std::collections::{BTreeMap, BTreeSet};

use oxiri::Iri;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

/// Physical treatment of one RDF predicate.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Treatment {
    /// Materialized into graph indexes and the reasoner-visible ABox.
    Core,
    /// Queryable through a compiled plan over typed columnar data.
    Virtual,
    /// Available only after GUID/FactID hydration.
    Payload,
}

/// Object construction is explicit and closed.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ObjectMap {
    EntityGuidColumn {
        column: String,
    },
    ConstantIri {
        iri: String,
    },
    TypedValueColumn {
        column: String,
        datatype_iri: String,
    },
    LanguageValueColumns {
        value_column: String,
        language_column: String,
    },
}

/// One governed predicate projection.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct PredicateMap {
    pub iri: String,
    pub object: ObjectMap,
    pub treatment: Treatment,
    pub participates_in_reasoning: bool,
    pub queryable_as_rdf: bool,
}

/// Explicit disposition of every source field.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FieldDisposition {
    Mapped,
    PolicyIgnored,
    SourceEvidence,
}

/// Versioned semantic projection manifest.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct SemanticProjection {
    pub mapping_id: String,
    pub dataset_namespace: Uuid,
    pub source_table: String,
    pub source_schema_hash: String,
    pub subject_iri_column: String,
    pub named_graph_iri_column: String,
    pub source_guid_column: String,
    pub record_guid_column: String,
    pub predicates: Vec<PredicateMap>,
    pub field_coverage: BTreeMap<String, FieldDisposition>,
    pub authorization_label_columns: Vec<String>,
}

/// Immutable output of mapping compilation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CompiledMapping {
    pub mapping_id: String,
    pub contract_hash: String,
    pub required_columns: Vec<String>,
    pub core_predicates: Vec<String>,
    pub virtual_predicates: Vec<String>,
    pub payload_predicates: Vec<String>,
}

/// Invalid semantic states are rejected before data work starts.
#[derive(Debug, Error, Eq, PartialEq)]
pub enum MappingError {
    #[error("invalid IRI in {field}: {value}")]
    InvalidIri { field: &'static str, value: String },
    #[error("reasoning-visible predicate must be core: {0}")]
    ReasoningPredicateNotCore(String),
    #[error("payload predicate cannot be queried as RDF: {0}")]
    PayloadMarkedQueryable(String),
    #[error("core and virtual predicates must be queryable as RDF: {0}")]
    SemanticPredicateNotQueryable(String),
    #[error("column name cannot be empty")]
    EmptyColumn,
    #[error("duplicate predicate projection: {0}")]
    DuplicatePredicate(String),
    #[error("field coverage is missing required column: {0}")]
    MissingFieldCoverage(String),
    #[error("source schema hash must contain exactly 64 lowercase hexadecimal characters")]
    InvalidSchemaHash,
    #[error("mapping serialization failed: {0}")]
    Serialization(String),
}

/// Validate and normalize a mapping into an immutable execution contract.
pub fn compile(mapping: &SemanticProjection) -> Result<CompiledMapping, MappingError> {
    require_iri("mapping_id", &mapping.mapping_id)?;
    if mapping.source_schema_hash.len() != 64
        || !mapping
            .source_schema_hash
            .bytes()
            .all(|value| value.is_ascii_hexdigit() && !value.is_ascii_uppercase())
    {
        return Err(MappingError::InvalidSchemaHash);
    }
    let mut required = BTreeSet::from([
        mapping.subject_iri_column.clone(),
        mapping.named_graph_iri_column.clone(),
        mapping.source_guid_column.clone(),
        mapping.record_guid_column.clone(),
    ]);
    required.extend(mapping.authorization_label_columns.iter().cloned());
    if required.iter().any(String::is_empty) {
        return Err(MappingError::EmptyColumn);
    }
    let mut seen = BTreeSet::new();
    let mut core = Vec::new();
    let mut virtualized = Vec::new();
    let mut payload = Vec::new();
    for predicate in &mapping.predicates {
        require_iri("predicate", &predicate.iri)?;
        if !seen.insert(predicate.iri.clone()) {
            return Err(MappingError::DuplicatePredicate(predicate.iri.clone()));
        }
        if predicate.participates_in_reasoning && predicate.treatment != Treatment::Core {
            return Err(MappingError::ReasoningPredicateNotCore(
                predicate.iri.clone(),
            ));
        }
        if predicate.treatment == Treatment::Payload && predicate.queryable_as_rdf {
            return Err(MappingError::PayloadMarkedQueryable(predicate.iri.clone()));
        }
        if predicate.treatment != Treatment::Payload && !predicate.queryable_as_rdf {
            return Err(MappingError::SemanticPredicateNotQueryable(
                predicate.iri.clone(),
            ));
        }
        match &predicate.object {
            ObjectMap::EntityGuidColumn { column } => {
                require_column(column, &mut required)?;
            }
            ObjectMap::ConstantIri { iri } => require_iri("constant_object", iri)?,
            ObjectMap::TypedValueColumn {
                column,
                datatype_iri,
            } => {
                require_column(column, &mut required)?;
                require_iri("datatype", datatype_iri)?;
            }
            ObjectMap::LanguageValueColumns {
                value_column,
                language_column,
            } => {
                require_column(value_column, &mut required)?;
                require_column(language_column, &mut required)?;
            }
        }
        match predicate.treatment {
            Treatment::Core => core.push(predicate.iri.clone()),
            Treatment::Virtual => virtualized.push(predicate.iri.clone()),
            Treatment::Payload => payload.push(predicate.iri.clone()),
        }
    }
    for column in &required {
        if !mapping.field_coverage.contains_key(column) {
            return Err(MappingError::MissingFieldCoverage(column.clone()));
        }
    }
    let canonical = serde_json::to_vec(mapping)
        .map_err(|error| MappingError::Serialization(error.to_string()))?;
    Ok(CompiledMapping {
        mapping_id: mapping.mapping_id.clone(),
        contract_hash: format!("blake3:{}", blake3::hash(&canonical).to_hex()),
        required_columns: required.into_iter().collect(),
        core_predicates: core,
        virtual_predicates: virtualized,
        payload_predicates: payload,
    })
}

fn require_column(column: &str, required: &mut BTreeSet<String>) -> Result<(), MappingError> {
    if column.is_empty() {
        return Err(MappingError::EmptyColumn);
    }
    required.insert(column.to_owned());
    Ok(())
}

fn require_iri(field: &'static str, value: &str) -> Result<(), MappingError> {
    Iri::parse(value.to_owned())
        .map(|_| ())
        .map_err(|_| MappingError::InvalidIri {
            field,
            value: value.to_owned(),
        })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::{
        FieldDisposition, ObjectMap, PredicateMap, SemanticProjection, Treatment, compile,
    };
    use uuid::Uuid;

    fn valid_mapping() -> SemanticProjection {
        let columns = ["subject", "graph", "source", "record", "entity"];
        SemanticProjection {
            mapping_id: "urn:ngkg:mapping:test:v1".to_owned(),
            dataset_namespace: Uuid::from_u128(7),
            source_table: "facts".to_owned(),
            source_schema_hash: "ab".repeat(32),
            subject_iri_column: "subject".to_owned(),
            named_graph_iri_column: "graph".to_owned(),
            source_guid_column: "source".to_owned(),
            record_guid_column: "record".to_owned(),
            predicates: vec![PredicateMap {
                iri: "urn:ngkg:observedOn".to_owned(),
                object: ObjectMap::EntityGuidColumn {
                    column: "entity".to_owned(),
                },
                treatment: Treatment::Core,
                participates_in_reasoning: true,
                queryable_as_rdf: true,
            }],
            field_coverage: columns
                .into_iter()
                .map(|name| (name.to_owned(), FieldDisposition::Mapped))
                .collect::<BTreeMap<_, _>>(),
            authorization_label_columns: Vec::new(),
        }
    }

    #[test]
    fn compilation_is_deterministic() {
        let mapping = valid_mapping();
        assert_eq!(compile(&mapping), compile(&mapping));
    }

    #[test]
    fn reasoning_payload_is_rejected() {
        let mut mapping = valid_mapping();
        mapping.predicates[0].treatment = Treatment::Payload;
        assert!(compile(&mapping).is_err());
    }
}
