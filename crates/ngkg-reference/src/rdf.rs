//! Standards-based TriG parsing and deterministic blank-node skolemization.

use std::{collections::BTreeMap, fs::File, io::BufReader, path::Path};

use ngkg_identity::{FactIdentityInput, fact_identity, guid_for_canonical_iri, skolem_iri};
use oxigraph::{
    io::{RdfFormat, RdfParser},
    model::{GraphName, NamedOrBlankNode, Quad, Term},
};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

use crate::model::{PredicateRule, ProjectionPolicy, Treatment};

/// Internal dictionary key for an RDF default graph.
///
/// This value is never serialized as an RDF graph IRI. A source named graph using
/// this IRI is rejected so the physical dictionary key cannot be confused with a
/// logical named graph.
pub const DEFAULT_GRAPH_STORAGE_KEY: &str = "urn:ngkg:internal:graph-key:default";

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
/// Logical RDF graph kind retained independently from its physical dictionary key.
pub enum GraphScope {
    /// The one unlabeled graph in an RDF dataset.
    Default,
    /// An RDF named graph identified by an absolute IRI.
    Named,
}

/// RDF resource term type retained independently from its internal GUID key.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ResourceTermKind {
    /// Absolute IRI RDF term.
    NamedNode,
    /// Dataset-scoped blank-node RDF term.
    BlankNode,
}

impl ResourceTermKind {
    /// Dictionary namespace tag; blank nodes never enter the IRI namespace.
    #[must_use]
    pub const fn dictionary_tag(self) -> char {
        match self {
            Self::NamedNode => 'I',
            Self::BlankNode => 'B',
        }
    }

    /// Stable physical code stored in columnar semantic facts.
    #[must_use]
    pub const fn code(self) -> u8 {
        match self {
            Self::NamedNode => 1,
            Self::BlankNode => 2,
        }
    }
}

impl GraphScope {
    /// Stable physical code stored in columnar payload rows.
    #[must_use]
    pub const fn code(self) -> u8 {
        match self {
            Self::Default => 0,
            Self::Named => 1,
        }
    }

    /// Recover the logical RDF graph kind from a validated physical key.
    /// Invalid named-graph IRIs fail closed as `None`.
    #[must_use]
    pub fn from_storage_key(storage_key: &str) -> Option<Self> {
        if storage_key == DEFAULT_GRAPH_STORAGE_KEY {
            Some(Self::Default)
        } else if oxigraph::model::NamedNode::new(storage_key).is_ok() {
            Some(Self::Named)
        } else {
            None
        }
    }

    /// Verify that a logical graph kind and physical graph key agree.
    #[must_use]
    pub fn matches_storage_key(self, storage_key: &str) -> bool {
        Self::from_storage_key(storage_key) == Some(self)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
/// Canonical object representation used by deterministic distributed stages.
pub enum NormalizedObject {
    /// Named or source-scoped entity.
    Entity {
        /// Canonical absolute IRI.
        iri: String,
        /// Deterministic dataset-scoped GUID.
        guid: Uuid,
        /// Named node or source-scoped blank node.
        term_kind: ResourceTermKind,
    },
    /// RDF literal with its exact N-Triples representation.
    Literal {
        /// Unescaped lexical value.
        lexical_value: String,
        /// Absolute datatype IRI.
        datatype_iri: String,
        /// Optional normalized language tag.
        language: Option<String>,
        /// Canonical N-Triples literal syntax.
        ntriples: String,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
/// One normalized logical RDF fact with stable identity and projection policy.
pub struct NormalizedFact {
    /// Compact 128-bit FactID.
    pub fact_id: [u8; 16],
    /// Full collision fingerprint used for validation and partitioning.
    pub fact_hash: [u8; 32],
    /// Canonical subject IRI.
    pub subject_iri: String,
    /// Exact subject RDF term kind. `subject_iri` is an internal canonical key for
    /// blank nodes and is never serialized as an IRI when this value is `BlankNode`.
    pub subject_term_kind: ResourceTermKind,
    /// Dataset-scoped subject GUID.
    pub subject_guid: Uuid,
    /// Absolute predicate IRI.
    pub predicate_iri: String,
    /// Canonical entity or literal object.
    pub object: NormalizedObject,
    /// Physical graph dictionary key. For [`GraphScope::Default`] this is
    /// [`DEFAULT_GRAPH_STORAGE_KEY`] and must never be exposed as an RDF graph IRI.
    pub graph_iri: String,
    /// Logical default-versus-named graph identity.
    pub graph_scope: GraphScope,
    /// Core, virtual, or payload treatment.
    pub treatment: Treatment,
    /// Whether the fact is visible to offline reasoning.
    pub participates_in_reasoning: bool,
    /// Whether SPARQL may address the fact as RDF.
    pub queryable_as_rdf: bool,
}

#[derive(Debug, Error)]
/// Fail-closed RDF parsing and normalization failures.
pub enum RdfCompileError {
    /// Source bytes could not be opened or read.
    #[error("RDF input could not be opened: {0}")]
    Io(#[from] std::io::Error),
    /// Standards parser rejected the RDF serialization.
    #[error("RDF parsing failed: {0}")]
    Parse(String),
    /// The policy prohibits a default-graph assertion.
    #[error("default graph is rejected by this projection policy")]
    DefaultGraphRejected,
    /// Blank-node graph names are outside the dataset contract.
    #[error("blank-node graph names are not supported by the named-graph contract")]
    BlankGraphRejected,
    /// The internal default-graph dictionary key cannot be supplied as a named graph.
    #[error("named graph uses NGKG's reserved default-graph storage key")]
    ReservedGraphName,
    /// The exhaustive mapping policy has no rule for a predicate.
    #[error("predicate has no exhaustive projection rule: {0}")]
    UnknownPredicate(String),
    /// More than one rule addresses the same predicate.
    #[error("projection contains duplicate predicate rule: {0}")]
    DuplicatePredicate(String),
    /// A rule could hide reasoning-visible or queryable data.
    #[error("predicate rule is semantically invalid: {0}")]
    InvalidRule(String),
    /// The source exceeded its operator-controlled quad ceiling.
    #[error("input exceeds the configured maximum of {0} quads")]
    QuadLimit(u64),
    /// Entity or fact identity construction failed.
    #[error("identity generation failed: {0}")]
    Identity(String),
    /// Two distinct full fingerprints mapped to one compact FactID.
    #[error("two distinct facts produced the same compact FactID")]
    FactIdCollision,
    /// Graph roles, visibility, or authorization metadata are invalid.
    #[error("RDF dataset graph catalog is invalid: {0}")]
    GraphCatalog(String),
    /// Canonical N-Quads shards must use NGKG's source-scoped blank-node labels.
    #[error("canonical N-Quads contains a non-canonical blank-node label")]
    NonCanonicalBlankNode,
    /// Source IRIs may not impersonate NGKG's internal blank-node namespace.
    #[error("source named node uses NGKG's reserved blank-node identity namespace")]
    ReservedBlankNodeNamespace,
}

pub fn validate_policy(
    policy: &ProjectionPolicy,
) -> Result<BTreeMap<String, PredicateRule>, RdfCompileError> {
    if policy.policy_id.is_empty() {
        return Err(RdfCompileError::InvalidRule("policyId is empty".to_owned()));
    }
    let mut rules = BTreeMap::new();
    for rule in &policy.rules {
        oxigraph::model::NamedNode::new(rule.predicate_iri.clone())
            .map_err(|_| RdfCompileError::InvalidRule(rule.predicate_iri.clone()))?;
        let valid = match rule.treatment {
            Treatment::Core => rule.participates_in_reasoning && rule.queryable_as_rdf,
            Treatment::Virtual => !rule.participates_in_reasoning && rule.queryable_as_rdf,
            Treatment::Payload => !rule.participates_in_reasoning && !rule.queryable_as_rdf,
        };
        if !valid {
            return Err(RdfCompileError::InvalidRule(format!(
                "{} has a treatment/reasoning/queryability combination that could hide data",
                rule.predicate_iri
            )));
        }
        if rules
            .insert(rule.predicate_iri.clone(), rule.clone())
            .is_some()
        {
            return Err(RdfCompileError::DuplicatePredicate(
                rule.predicate_iri.clone(),
            ));
        }
    }
    Ok(rules)
}

/// Parse the complete TriG grammar; never split uploaded bytes at arbitrary offsets.
pub fn parse_trig(
    path: &Path,
    source_sha256: [u8; 32],
    dataset_namespace: Uuid,
    source_guid: Uuid,
    source_snapshot: &str,
    policy: &ProjectionPolicy,
    max_quads: u64,
) -> Result<Vec<NormalizedFact>, RdfCompileError> {
    parse_rdf(
        path,
        RdfFormat::TriG,
        source_sha256,
        dataset_namespace,
        source_guid,
        source_snapshot,
        policy,
        max_quads,
        false,
    )
}

/// Parse a canonical N-Quads shard emitted by the distributed safe-scan stage.
///
/// Blank nodes already use source-scoped canonical labels in these shards, so
/// parsing shard files independently cannot change their RDF term type or identity.
pub fn parse_nquads(
    path: &Path,
    source_sha256: [u8; 32],
    dataset_namespace: Uuid,
    source_guid: Uuid,
    source_snapshot: &str,
    policy: &ProjectionPolicy,
    max_quads: u64,
) -> Result<Vec<NormalizedFact>, RdfCompileError> {
    parse_rdf(
        path,
        RdfFormat::NQuads,
        source_sha256,
        dataset_namespace,
        source_guid,
        source_snapshot,
        policy,
        max_quads,
        true,
    )
}

fn parse_rdf(
    path: &Path,
    format: RdfFormat,
    source_sha256: [u8; 32],
    dataset_namespace: Uuid,
    source_guid: Uuid,
    source_snapshot: &str,
    policy: &ProjectionPolicy,
    max_quads: u64,
    canonical_blank_nodes: bool,
) -> Result<Vec<NormalizedFact>, RdfCompileError> {
    let rules = validate_policy(policy)?;
    let file = BufReader::new(File::open(path)?);
    let parser = RdfParser::from_format(format).for_reader(file);
    let mut facts = Vec::new();
    for parsed in parser {
        let quad = parsed.map_err(|error| RdfCompileError::Parse(error.to_string()))?;
        if u64::try_from(facts.len()).unwrap_or(u64::MAX) >= max_quads {
            return Err(RdfCompileError::QuadLimit(max_quads));
        }
        facts.push(normalize_quad(
            &quad,
            source_sha256,
            dataset_namespace,
            source_guid,
            source_snapshot,
            policy,
            &rules,
            canonical_blank_nodes,
        )?);
    }
    let mut unique = BTreeMap::<[u8; 16], NormalizedFact>::new();
    for fact in facts {
        if let Some(existing) = unique.get(&fact.fact_id) {
            if existing.fact_hash != fact.fact_hash {
                return Err(RdfCompileError::FactIdCollision);
            }
        } else {
            unique.insert(fact.fact_id, fact);
        }
    }
    let mut facts = unique.into_values().collect::<Vec<NormalizedFact>>();
    facts.sort_unstable_by(|left, right| {
        (
            left.graph_scope,
            &left.graph_iri,
            left.subject_term_kind,
            &left.subject_iri,
            &left.predicate_iri,
            object_sort_key(&left.object),
            left.fact_id,
        )
            .cmp(&(
                right.graph_scope,
                &right.graph_iri,
                right.subject_term_kind,
                &right.subject_iri,
                &right.predicate_iri,
                object_sort_key(&right.object),
                right.fact_id,
            ))
    });
    Ok(facts)
}

fn normalize_quad(
    quad: &Quad,
    source_sha256: [u8; 32],
    dataset_namespace: Uuid,
    source_guid: Uuid,
    source_snapshot: &str,
    policy: &ProjectionPolicy,
    rules: &BTreeMap<String, PredicateRule>,
    canonical_blank_nodes: bool,
) -> Result<NormalizedFact, RdfCompileError> {
    let (subject_iri, subject_term_kind) = normalize_named_or_blank(
        &quad.subject,
        source_sha256,
        source_guid,
        canonical_blank_nodes,
    )?;
    if subject_term_kind == ResourceTermKind::NamedNode
        && subject_iri.starts_with("urn:ngkg:skolem:")
    {
        return Err(RdfCompileError::ReservedBlankNodeNamespace);
    }
    let subject_guid = guid_for_canonical_iri(dataset_namespace, &subject_iri)
        .map_err(|error| RdfCompileError::Identity(error.to_string()))?;
    let predicate_iri = quad.predicate.as_str().to_owned();
    let rule = rules
        .get(&predicate_iri)
        .ok_or_else(|| RdfCompileError::UnknownPredicate(predicate_iri.clone()))?;
    let (graph_scope, graph_iri) = match &quad.graph_name {
        GraphName::NamedNode(node) if node.as_str() == DEFAULT_GRAPH_STORAGE_KEY => {
            return Err(RdfCompileError::ReservedGraphName);
        }
        GraphName::NamedNode(node) => (GraphScope::Named, node.as_str().to_owned()),
        GraphName::BlankNode(_) => return Err(RdfCompileError::BlankGraphRejected),
        GraphName::DefaultGraph if policy.reject_default_graph => {
            return Err(RdfCompileError::DefaultGraphRejected);
        }
        GraphName::DefaultGraph => (GraphScope::Default, DEFAULT_GRAPH_STORAGE_KEY.to_owned()),
    };
    let object = match &quad.object {
        Term::NamedNode(node) => {
            let iri = node.as_str().to_owned();
            if iri.starts_with("urn:ngkg:skolem:") {
                return Err(RdfCompileError::ReservedBlankNodeNamespace);
            }
            let guid = guid_for_canonical_iri(dataset_namespace, &iri)
                .map_err(|error| RdfCompileError::Identity(error.to_string()))?;
            NormalizedObject::Entity {
                iri,
                guid,
                term_kind: ResourceTermKind::NamedNode,
            }
        }
        Term::BlankNode(node) => {
            let iri = canonical_blank_key(
                node.as_str(),
                source_sha256,
                source_guid,
                canonical_blank_nodes,
            )?;
            let guid = guid_for_canonical_iri(dataset_namespace, &iri)
                .map_err(|error| RdfCompileError::Identity(error.to_string()))?;
            NormalizedObject::Entity {
                iri,
                guid,
                term_kind: ResourceTermKind::BlankNode,
            }
        }
        Term::Literal(literal) => NormalizedObject::Literal {
            lexical_value: literal.value().to_owned(),
            datatype_iri: literal.datatype().as_str().to_owned(),
            language: literal.language().map(ToOwned::to_owned),
            ntriples: literal.to_string(),
        },
    };
    let subject_canonical = format!("{}\t{subject_iri}", subject_term_kind.dictionary_tag());
    let object_canonical = match &object {
        NormalizedObject::Entity { iri, term_kind, .. } => {
            format!("{}\t{iri}", term_kind.dictionary_tag())
        }
        NormalizedObject::Literal { ntriples, .. } => format!("L\t{ntriples}"),
    };
    let identity = fact_identity(&FactIdentityInput {
        subject: subject_canonical.as_bytes(),
        predicate_iri: &predicate_iri,
        object_canonical: object_canonical.as_bytes(),
        graph_iri: &graph_iri,
        source_guid,
        source_snapshot,
    });
    debug_assert!(graph_scope.matches_storage_key(&graph_iri));
    Ok(NormalizedFact {
        fact_id: identity.compact_id,
        fact_hash: identity.collision_fingerprint,
        subject_iri,
        subject_term_kind,
        subject_guid,
        predicate_iri,
        object,
        graph_iri,
        graph_scope,
        treatment: rule.treatment,
        participates_in_reasoning: rule.participates_in_reasoning,
        queryable_as_rdf: rule.queryable_as_rdf,
    })
}

fn normalize_named_or_blank(
    value: &NamedOrBlankNode,
    source_sha256: [u8; 32],
    source_guid: Uuid,
    canonical_blank_nodes: bool,
) -> Result<(String, ResourceTermKind), RdfCompileError> {
    match value {
        NamedOrBlankNode::NamedNode(node) => {
            Ok((node.as_str().to_owned(), ResourceTermKind::NamedNode))
        }
        NamedOrBlankNode::BlankNode(node) => Ok((
            canonical_blank_key(
                node.as_str(),
                source_sha256,
                source_guid,
                canonical_blank_nodes,
            )?,
            ResourceTermKind::BlankNode,
        )),
    }
}

fn canonical_blank_key(
    label: &str,
    source_sha256: [u8; 32],
    source_guid: Uuid,
    canonical_blank_nodes: bool,
) -> Result<String, RdfCompileError> {
    if !canonical_blank_nodes {
        return Ok(skolem_iri(
            &source_sha256,
            &format!("{}:{label}", source_guid.hyphenated()),
        ));
    }
    let digest = label
        .strip_prefix("ngkg")
        .filter(|digest| {
            digest.len() == 64
                && digest
                    .bytes()
                    .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
        })
        .ok_or(RdfCompileError::NonCanonicalBlankNode)?;
    Ok(format!("urn:ngkg:skolem:{digest}"))
}

fn object_sort_key(object: &NormalizedObject) -> &str {
    match object {
        NormalizedObject::Entity { iri, .. } => iri,
        NormalizedObject::Literal { ntriples, .. } => ntriples,
    }
}

#[must_use]
/// Serialize one normalized fact as a canonical LF-terminated N-Quads row.
pub fn nquad_line(fact: &NormalizedFact) -> String {
    let object = match &fact.object {
        NormalizedObject::Entity { iri, term_kind, .. } => resource_ntriples(*term_kind, iri),
        NormalizedObject::Literal { ntriples, .. } => ntriples.clone(),
    };
    match fact.graph_scope {
        GraphScope::Default => format!(
            "{} <{}> {} .\n",
            resource_ntriples(fact.subject_term_kind, &fact.subject_iri),
            fact.predicate_iri,
            object
        ),
        GraphScope::Named => format!(
            "{} <{}> {} <{}> .\n",
            resource_ntriples(fact.subject_term_kind, &fact.subject_iri),
            fact.predicate_iri,
            object,
            fact.graph_iri
        ),
    }
}

#[must_use]
pub fn ntriple_line(fact: &NormalizedFact) -> String {
    let object = match &fact.object {
        NormalizedObject::Entity { iri, term_kind, .. } => resource_ntriples(*term_kind, iri),
        NormalizedObject::Literal { ntriples, .. } => ntriples.clone(),
    };
    format!(
        "{} <{}> {} .\n",
        resource_ntriples(fact.subject_term_kind, &fact.subject_iri),
        fact.predicate_iri,
        object,
    )
}

fn resource_ntriples(kind: ResourceTermKind, canonical_key: &str) -> String {
    match kind {
        ResourceTermKind::NamedNode => format!("<{canonical_key}>"),
        ResourceTermKind::BlankNode => public_resource_lexical(kind, canonical_key),
    }
}

/// Return a public RDF resource lexical value without converting blank nodes to IRIs.
#[must_use]
pub fn public_resource_lexical(kind: ResourceTermKind, canonical_key: &str) -> String {
    match kind {
        ResourceTermKind::NamedNode => canonical_key.to_owned(),
        ResourceTermKind::BlankNode => {
            let digest = canonical_key
                .strip_prefix("urn:ngkg:skolem:")
                .unwrap_or(canonical_key);
            format!("_:ngkg{digest}")
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, path::PathBuf};

    use uuid::Uuid;

    use super::{
        DEFAULT_GRAPH_STORAGE_KEY, GraphScope, RdfCompileError, ResourceTermKind, nquad_line,
        parse_nquads, parse_trig, validate_policy,
    };
    use crate::model::{PredicateRule, ProjectionPolicy, Treatment};

    const PREDICATE: &str = "https://example.test/predicate";

    fn policy(reject_default_graph: bool) -> ProjectionPolicy {
        ProjectionPolicy {
            policy_id: "urn:ngkg:test-policy".to_owned(),
            reject_default_graph,
            rules: vec![PredicateRule {
                predicate_iri: PREDICATE.to_owned(),
                treatment: Treatment::Core,
                participates_in_reasoning: true,
                queryable_as_rdf: true,
            }],
        }
    }

    fn temporary_trig(contents: &str) -> Result<PathBuf, std::io::Error> {
        let path = std::env::temp_dir().join(format!("ngkg-rdf-test-{}.trig", Uuid::new_v4()));
        fs::write(&path, contents)?;
        Ok(path)
    }

    #[test]
    fn reasoning_visible_virtual_predicate_is_rejected() {
        let policy = ProjectionPolicy {
            policy_id: "urn:ngkg:test-policy".to_owned(),
            reject_default_graph: true,
            rules: vec![PredicateRule {
                predicate_iri: "https://example.test/value".to_owned(),
                treatment: Treatment::Virtual,
                participates_in_reasoning: true,
                queryable_as_rdf: true,
            }],
        };
        assert!(matches!(
            validate_policy(&policy),
            Err(RdfCompileError::InvalidRule(_))
        ));
    }

    #[test]
    fn default_graph_round_trips_as_default_not_named() -> Result<(), Box<dyn std::error::Error>> {
        let path = temporary_trig(&format!(
            "<https://example.test/subject> <{PREDICATE}> <https://example.test/object> .\n"
        ))?;
        let parsed = parse_trig(
            &path,
            [7_u8; 32],
            Uuid::new_v4(),
            Uuid::new_v4(),
            "snapshot-1",
            &policy(false),
            10,
        );
        fs::remove_file(&path)?;
        let facts = parsed?;
        assert_eq!(facts.len(), 1);
        assert_eq!(facts[0].graph_scope, GraphScope::Default);
        assert_eq!(facts[0].graph_iri, DEFAULT_GRAPH_STORAGE_KEY);
        assert_eq!(
            GraphScope::from_storage_key(&facts[0].graph_iri),
            Some(GraphScope::Default)
        );
        assert_eq!(
            nquad_line(&facts[0]),
            format!(
                "<https://example.test/subject> <{PREDICATE}> <https://example.test/object> .\n"
            )
        );
        Ok(())
    }

    #[test]
    fn repeated_named_graph_blocks_are_set_union() -> Result<(), Box<dyn std::error::Error>> {
        let statement =
            format!("<https://example.test/subject> <{PREDICATE}> <https://example.test/object> .");
        let path = temporary_trig(&format!(
            "<https://example.test/graph> {{ {statement} }}\n<https://example.test/graph> {{ {statement} }}\n"
        ))?;
        let parsed = parse_trig(
            &path,
            [9_u8; 32],
            Uuid::new_v4(),
            Uuid::new_v4(),
            "snapshot-1",
            &policy(true),
            10,
        );
        fs::remove_file(&path)?;
        let facts = parsed?;
        assert_eq!(facts.len(), 1);
        assert_eq!(facts[0].graph_scope, GraphScope::Named);
        assert_eq!(facts[0].graph_iri, "https://example.test/graph");
        assert_eq!(
            GraphScope::from_storage_key(&facts[0].graph_iri),
            Some(GraphScope::Named)
        );
        Ok(())
    }

    #[test]
    fn blank_nodes_round_trip_as_blank_terms_with_stable_identity()
    -> Result<(), Box<dyn std::error::Error>> {
        let source_sha = [13_u8; 32];
        let namespace = Uuid::new_v4();
        let source_guid = Uuid::new_v4();
        let path = temporary_trig(&format!(
            "<https://example.test/graph> {{ _:subject <{PREDICATE}> _:object . }}\n"
        ))?;
        let first = parse_trig(
            &path,
            source_sha,
            namespace,
            source_guid,
            "snapshot-1",
            &policy(true),
            10,
        )?;
        assert_eq!(first[0].subject_term_kind, ResourceTermKind::BlankNode);
        assert!(matches!(
            &first[0].object,
            super::NormalizedObject::Entity {
                term_kind: ResourceTermKind::BlankNode,
                ..
            }
        ));
        let canonical = nquad_line(&first[0]);
        assert!(canonical.starts_with("_:ngkg"));
        assert!(!canonical.contains("<urn:ngkg:skolem:"));
        let shard_staging = temporary_trig(&canonical)?;
        let shard = shard_staging.with_extension("nq");
        fs::rename(shard_staging, &shard)?;
        let second = parse_nquads(
            &shard,
            source_sha,
            namespace,
            source_guid,
            "snapshot-1",
            &policy(true),
            10,
        )?;
        let distinct_source = parse_trig(
            &path,
            source_sha,
            namespace,
            Uuid::new_v4(),
            "snapshot-1",
            &policy(true),
            10,
        )?;
        fs::remove_file(path)?;
        fs::remove_file(shard)?;
        assert_eq!(first, second);
        assert_ne!(first[0].subject_iri, distinct_source[0].subject_iri);
        assert_ne!(first[0].subject_guid, distinct_source[0].subject_guid);
        Ok(())
    }

    #[test]
    fn reserved_default_storage_key_cannot_be_uploaded_as_named_graph()
    -> Result<(), Box<dyn std::error::Error>> {
        let path = temporary_trig(&format!(
            "<{DEFAULT_GRAPH_STORAGE_KEY}> {{ <https://example.test/subject> <{PREDICATE}> <https://example.test/object> . }}\n"
        ))?;
        let parsed = parse_trig(
            &path,
            [11_u8; 32],
            Uuid::new_v4(),
            Uuid::new_v4(),
            "snapshot-1",
            &policy(true),
            10,
        );
        fs::remove_file(&path)?;
        assert!(matches!(parsed, Err(RdfCompileError::ReservedGraphName)));
        assert_eq!(GraphScope::from_storage_key("not an IRI"), None);
        Ok(())
    }
}
