//! W3C OWL 2 Direct-Semantics basic-graph-pattern admission for NGKG Phase 40.7.
//!
//! This crate owns semantic *admission*, not query parsing and not entailment. SPARQL is parsed
//! once by `ngkg-sparql-compiler`; this layer walks the typed algebra, applies BGP-local W3C
//! variable typing, disambiguates constant entities from the checksum-bound OWL signature, and
//! fails closed when a triple pattern cannot be mapped unambiguously to the extended OWL 2
//! structural grammar. Phase 40.8 must still ground each candidate and prove that the resulting
//! instantiated axioms are legal OWL 2 DL and entailed by the active ontology.

use std::{
    collections::{BTreeMap, BTreeSet},
    thread,
};

use ngkg_sparql_compiler::CompiledSparqlQuery;
use ngkg_types::{
    DirectBgpLegalityFailure, DirectBgpLegalityFailureCode, DirectBgpLegalityRecord,
    DirectBgpLegalityStatus, DirectBgpScope, DirectExactBgpTemplate, DirectExactTermPattern,
    DirectExactTriplePattern, DirectExactVariable, DirectVariableRole, DirectVariableRoleSource,
    DirectVariableTyping,
};
use sha2::{Digest, Sha256};
use spargebra::{
    Query,
    algebra::{AggregateExpression, Expression, GraphPattern, OrderExpression},
    term::{NamedNodePattern, TermPattern, TriplePattern},
};
use thiserror::Error;

const MAX_CLASSIFICATION_LANES: usize = 32;
const DEFAULT_MAX_BGPS: usize = 4096;
const DEFAULT_MAX_TRIPLES_PER_BGP: usize = 65_536;

const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
const RDF_FIRST: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#first";
const RDF_REST: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#rest";
const RDF_NIL: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#nil";
const RDFS_SUBCLASS: &str = "http://www.w3.org/2000/01/rdf-schema#subClassOf";
const RDFS_SUBPROPERTY: &str = "http://www.w3.org/2000/01/rdf-schema#subPropertyOf";
const RDFS_DOMAIN: &str = "http://www.w3.org/2000/01/rdf-schema#domain";
const RDFS_RANGE: &str = "http://www.w3.org/2000/01/rdf-schema#range";
const RDFS_DATATYPE: &str = "http://www.w3.org/2000/01/rdf-schema#Datatype";
const OWL: &str = "http://www.w3.org/2002/07/owl#";

fn owl(local: &str) -> String {
    format!("{OWL}{local}")
}

/// Immutable declaration index derived from the Phase 40.1 checksum-bound merged-ontology
/// signature. BTreeSet is intentional: classification evidence must be deterministic across CPU
/// count, allocator behavior, and Kubernetes node placement.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct OwlSignatureIndex {
    pub classes: BTreeSet<String>,
    pub object_properties: BTreeSet<String>,
    pub data_properties: BTreeSet<String>,
    pub annotation_properties: BTreeSet<String>,
    pub named_individuals: BTreeSet<String>,
    pub datatypes: BTreeSet<String>,
}

impl OwlSignatureIndex {
    #[must_use]
    pub fn with_builtins(mut self) -> Self {
        self.classes.extend([owl("Thing"), owl("Nothing")]);
        self.object_properties
            .extend([owl("topObjectProperty"), owl("bottomObjectProperty")]);
        self.data_properties
            .extend([owl("topDataProperty"), owl("bottomDataProperty")]);
        self
    }
}

/// Temporary Phase 40.7 admission ceilings. Phase 40.10 moves these into authoritative Helm
/// values and worker/operator configuration. The defaults are intentionally bounded now so an
/// adversarial query cannot turn classification into unbounded CPU or memory work.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DirectBgpClassificationLimits {
    pub max_bgps: usize,
    pub max_triples_per_bgp: usize,
    pub max_cpu_lanes: usize,
}

impl Default for DirectBgpClassificationLimits {
    fn default() -> Self {
        Self {
            max_bgps: DEFAULT_MAX_BGPS,
            max_triples_per_bgp: DEFAULT_MAX_TRIPLES_PER_BGP,
            max_cpu_lanes: MAX_CLASSIFICATION_LANES,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DirectBgpClassification {
    pub records: Vec<DirectBgpLegalityRecord>,
    pub property_paths_outside_direct_bgps: bool,
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum DirectBgpClassifierError {
    #[error("Direct-BGP classifier resource limit exceeded: {0}")]
    ResourceLimit(String),
    #[error("Direct-BGP classification worker failed")]
    WorkerFailure,
}

#[derive(Clone)]
struct BgpLeaf {
    ordinal: usize,
    scope: DirectBgpScope,
    patterns: Vec<TriplePattern>,
}

/// Classify all BGP leaves in one already parsed SPARQL query. Independent leaves are processed
/// in bounded CPU lanes. Results are sorted back to typed-algebra preorder, so CPU scheduling can
/// never affect serialized legality evidence.

/// Recover the exact typed triple template for one already-admitted BGP ordinal.
///
/// The legality record is authoritative for variable roles and BGP identity. This helper only
/// carries the already parsed terms across the Rust→HermiT boundary; it does not reclassify the
/// BGP or broaden the Phase 40.7 admission decision.
pub fn extract_direct_bgp_template(
    query: &CompiledSparqlQuery,
    record: &DirectBgpLegalityRecord,
) -> Result<DirectExactBgpTemplate, DirectBgpClassifierError> {
    let mut leaves = Vec::new();
    let mut property_paths = false;
    collect_bgps(
        query_pattern(query.query()),
        &DirectBgpScope::Default,
        &mut leaves,
        &mut property_paths,
    );
    let ordinal = usize::try_from(record.ordinal).map_err(|_| {
        DirectBgpClassifierError::ResourceLimit(
            "BGP ordinal does not fit platform usize".to_owned(),
        )
    })?;
    let leaf = leaves.get(ordinal).ok_or_else(|| {
        DirectBgpClassifierError::ResourceLimit(format!(
            "BGP ordinal {} is absent from typed algebra",
            record.ordinal
        ))
    })?;
    let observed = bgp_sha256(&leaf.scope, &leaf.patterns);
    if observed != record.bgp_sha256 || leaf.scope != record.graph_scope {
        return Err(DirectBgpClassifierError::ResourceLimit(
            "typed BGP no longer matches Phase 40.7 legality evidence".to_owned(),
        ));
    }
    let variables = record
        .variables
        .iter()
        .map(|typing| DirectExactVariable {
            name: typing.variable.clone(),
            role: typing.role,
            source: typing.source,
        })
        .collect::<Vec<_>>();
    let triples = leaf
        .patterns
        .iter()
        .map(exact_triple_pattern)
        .collect::<Vec<_>>();
    Ok(DirectExactBgpTemplate {
        ordinal: record.ordinal,
        bgp_sha256: record.bgp_sha256.clone(),
        graph_scope: record.graph_scope.clone(),
        variables,
        triples,
    })
}

fn exact_triple_pattern(triple: &TriplePattern) -> DirectExactTriplePattern {
    DirectExactTriplePattern {
        subject: exact_term_pattern(&triple.subject),
        predicate: match &triple.predicate {
            NamedNodePattern::NamedNode(node) => DirectExactTermPattern::Iri {
                value: node.as_str().to_owned(),
            },
            NamedNodePattern::Variable(variable) => DirectExactTermPattern::Variable {
                name: variable.as_str().to_owned(),
            },
        },
        object: exact_term_pattern(&triple.object),
    }
}

fn exact_term_pattern(term: &TermPattern) -> DirectExactTermPattern {
    match term {
        TermPattern::NamedNode(node) => DirectExactTermPattern::Iri {
            value: node.as_str().to_owned(),
        },
        TermPattern::BlankNode(node) => DirectExactTermPattern::BlankNode {
            value: node.as_str().to_owned(),
        },
        TermPattern::Literal(literal) => DirectExactTermPattern::Literal {
            lexical_form: literal.value().to_owned(),
            datatype_iri: literal.datatype().as_str().to_owned(),
            language: literal.language().map(ToOwned::to_owned),
        },
        TermPattern::Variable(variable) => DirectExactTermPattern::Variable {
            name: variable.as_str().to_owned(),
        },
    }
}

pub fn classify_direct_bgps(
    query: &CompiledSparqlQuery,
    signature: &OwlSignatureIndex,
    limits: DirectBgpClassificationLimits,
) -> Result<DirectBgpClassification, DirectBgpClassifierError> {
    let mut leaves = Vec::new();
    let mut property_paths = false;
    collect_bgps(
        query_pattern(query.query()),
        &DirectBgpScope::Default,
        &mut leaves,
        &mut property_paths,
    );
    if leaves.len() > limits.max_bgps {
        return Err(DirectBgpClassifierError::ResourceLimit(format!(
            "{} BGPs exceeds max {}",
            leaves.len(),
            limits.max_bgps
        )));
    }
    if let Some(leaf) = leaves
        .iter()
        .find(|leaf| leaf.patterns.len() > limits.max_triples_per_bgp)
    {
        return Err(DirectBgpClassifierError::ResourceLimit(format!(
            "BGP {} has {} triples, max {}",
            leaf.ordinal,
            leaf.patterns.len(),
            limits.max_triples_per_bgp
        )));
    }
    if leaves.is_empty() {
        return Ok(DirectBgpClassification {
            records: Vec::new(),
            property_paths_outside_direct_bgps: property_paths,
        });
    }

    let available = thread::available_parallelism().map_or(1, |count| count.get());
    let lanes = available
        .min(limits.max_cpu_lanes.max(1))
        .min(leaves.len())
        .max(1);
    let chunk_size = leaves.len().div_ceil(lanes);
    let mut records = Vec::with_capacity(leaves.len());
    thread::scope(|scope| {
        let mut handles = Vec::with_capacity(lanes);
        for chunk in leaves.chunks(chunk_size) {
            let signature = signature.clone();
            handles.push(scope.spawn(move || {
                chunk
                    .iter()
                    .map(|leaf| classify_leaf(leaf, &signature))
                    .collect::<Vec<_>>()
            }));
        }
        for handle in handles {
            let mut lane = handle
                .join()
                .map_err(|_| DirectBgpClassifierError::WorkerFailure)?;
            records.append(&mut lane);
        }
        Ok::<(), DirectBgpClassifierError>(())
    })?;
    records.sort_by_key(|record| record.ordinal);
    Ok(DirectBgpClassification {
        records,
        property_paths_outside_direct_bgps: property_paths,
    })
}

fn query_pattern(query: &Query) -> &GraphPattern {
    match query {
        Query::Select { pattern, .. }
        | Query::Ask { pattern, .. }
        | Query::Construct { pattern, .. }
        | Query::Describe { pattern, .. } => pattern,
    }
}

fn collect_bgps(
    pattern: &GraphPattern,
    scope: &DirectBgpScope,
    output: &mut Vec<BgpLeaf>,
    has_property_path: &mut bool,
) {
    match pattern {
        GraphPattern::Bgp { patterns } => {
            let ordinal = output.len();
            output.push(BgpLeaf {
                ordinal,
                scope: scope.clone(),
                patterns: patterns.clone(),
            });
        }
        GraphPattern::Path { .. } => *has_property_path = true,
        GraphPattern::Join { left, right }
        | GraphPattern::Lateral { left, right }
        | GraphPattern::Union { left, right }
        | GraphPattern::Minus { left, right } => {
            collect_bgps(left, scope, output, has_property_path);
            collect_bgps(right, scope, output, has_property_path);
        }
        GraphPattern::LeftJoin {
            left,
            right,
            expression,
        } => {
            collect_bgps(left, scope, output, has_property_path);
            collect_bgps(right, scope, output, has_property_path);
            if let Some(expression) = expression {
                collect_expression_bgps(expression, scope, output, has_property_path);
            }
        }
        GraphPattern::Filter { expr, inner } => {
            collect_bgps(inner, scope, output, has_property_path);
            collect_expression_bgps(expr, scope, output, has_property_path);
        }
        GraphPattern::Extend {
            inner, expression, ..
        } => {
            collect_bgps(inner, scope, output, has_property_path);
            collect_expression_bgps(expression, scope, output, has_property_path);
        }
        GraphPattern::OrderBy { inner, expression } => {
            collect_bgps(inner, scope, output, has_property_path);
            for order in expression {
                match order {
                    OrderExpression::Asc(expression) | OrderExpression::Desc(expression) => {
                        collect_expression_bgps(expression, scope, output, has_property_path);
                    }
                }
            }
        }
        GraphPattern::Group {
            inner, aggregates, ..
        } => {
            collect_bgps(inner, scope, output, has_property_path);
            for (_, aggregate) in aggregates {
                collect_aggregate_bgps(aggregate, scope, output, has_property_path);
            }
        }
        GraphPattern::Project { inner, .. }
        | GraphPattern::Distinct { inner }
        | GraphPattern::Reduced { inner }
        | GraphPattern::Slice { inner, .. } => {
            collect_bgps(inner, scope, output, has_property_path)
        }
        GraphPattern::Graph { name, inner } => {
            let nested = match name {
                NamedNodePattern::NamedNode(node) => DirectBgpScope::Named {
                    graph_iri: node.as_str().to_owned(),
                },
                NamedNodePattern::Variable(variable) => DirectBgpScope::NamedVariable {
                    variable: variable.as_str().to_owned(),
                },
            };
            collect_bgps(inner, &nested, output, has_property_path);
        }
        GraphPattern::Values { .. } | GraphPattern::Service { .. } => {}
    }
}

fn collect_aggregate_bgps(
    aggregate: &AggregateExpression,
    scope: &DirectBgpScope,
    output: &mut Vec<BgpLeaf>,
    has_property_path: &mut bool,
) {
    match aggregate {
        AggregateExpression::CountSolutions { .. } => {}
        AggregateExpression::FunctionCall { expr, .. } => {
            collect_expression_bgps(expr, scope, output, has_property_path);
        }
    }
}

fn collect_expression_bgps(
    expression: &Expression,
    scope: &DirectBgpScope,
    output: &mut Vec<BgpLeaf>,
    has_property_path: &mut bool,
) {
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
            collect_expression_bgps(left, scope, output, has_property_path);
            collect_expression_bgps(right, scope, output, has_property_path);
        }
        Expression::In(left, values) => {
            collect_expression_bgps(left, scope, output, has_property_path);
            for value in values {
                collect_expression_bgps(value, scope, output, has_property_path);
            }
        }
        Expression::UnaryPlus(inner) | Expression::UnaryMinus(inner) | Expression::Not(inner) => {
            collect_expression_bgps(inner, scope, output, has_property_path);
        }
        Expression::Exists(pattern) => {
            collect_bgps(pattern, scope, output, has_property_path);
        }
        Expression::If(condition, yes, no) => {
            collect_expression_bgps(condition, scope, output, has_property_path);
            collect_expression_bgps(yes, scope, output, has_property_path);
            collect_expression_bgps(no, scope, output, has_property_path);
        }
        Expression::Coalesce(values) => {
            for value in values {
                collect_expression_bgps(value, scope, output, has_property_path);
            }
        }
        Expression::FunctionCall(_, arguments) => {
            for argument in arguments {
                collect_expression_bgps(argument, scope, output, has_property_path);
            }
        }
    }
}

fn classify_leaf(leaf: &BgpLeaf, signature: &OwlSignatureIndex) -> DirectBgpLegalityRecord {
    let bgp_sha256 = bgp_sha256(&leaf.scope, &leaf.patterns);
    let triple_count = u64::try_from(leaf.patterns.len()).unwrap_or(u64::MAX);
    let mut context = LeafContext::new(signature);

    // W3C 7.1.3: variable declarations are BGP-local and a variable may not be declared more
    // than one entity type. Scan declarations first so later triple order is irrelevant.
    for (ordinal, triple) in leaf.patterns.iter().enumerate() {
        if let Err(failure) = context.observe_explicit_declaration(ordinal, triple) {
            return illegal_record(leaf, bgp_sha256, triple_count, context, failure);
        }
    }
    // Structural predicates such as owl:someValuesFrom depend on owl:onProperty in the same
    // anonymous class expression. Capture that relation before classifying individual triples.
    context.index_structural_nodes(&leaf.patterns);

    for (ordinal, triple) in leaf.patterns.iter().enumerate() {
        match context.classify_triple(ordinal, triple) {
            Ok(form) => {
                context.forms.insert(form);
            }
            Err(failure) => {
                return illegal_record(leaf, bgp_sha256, triple_count, context, failure);
            }
        }
    }
    if let Err(failure) = context.finish() {
        return illegal_record(leaf, bgp_sha256, triple_count, context, failure);
    }
    let variables = context.variable_evidence();
    DirectBgpLegalityRecord {
        ordinal: u64::try_from(leaf.ordinal).unwrap_or(u64::MAX),
        bgp_sha256,
        graph_scope: leaf.scope.clone(),
        triple_count,
        recognized_forms: context.forms.into_iter().collect(),
        variables,
        status: DirectBgpLegalityStatus::Legal,
        grounded_owl2dl_check_required: true,
        failure: None,
    }
}

fn illegal_record(
    leaf: &BgpLeaf,
    bgp_sha256: String,
    triple_count: u64,
    context: LeafContext<'_>,
    failure: DirectBgpLegalityFailure,
) -> DirectBgpLegalityRecord {
    let variables = context.variable_evidence();
    DirectBgpLegalityRecord {
        ordinal: u64::try_from(leaf.ordinal).unwrap_or(u64::MAX),
        bgp_sha256,
        graph_scope: leaf.scope.clone(),
        triple_count,
        recognized_forms: context.forms.into_iter().collect(),
        variables,
        status: DirectBgpLegalityStatus::Illegal,
        grounded_owl2dl_check_required: true,
        failure: Some(failure),
    }
}

fn bgp_sha256(scope: &DirectBgpScope, patterns: &[TriplePattern]) -> String {
    let mut triples = patterns.iter().map(canonical_triple).collect::<Vec<_>>();
    triples.sort(); // RDF BGP is a multiset of triple patterns; preserve duplicate lines.
    let scope_line = match scope {
        DirectBgpScope::Default => "scope:default".to_owned(),
        DirectBgpScope::Named { graph_iri } => format!("scope:named:{graph_iri}"),
        DirectBgpScope::NamedVariable { variable } => format!("scope:named-variable:{variable}"),
    };
    let mut hasher = Sha256::new();
    hasher.update(b"ngkg-direct-bgp-v1\0");
    hasher.update(scope_line.as_bytes());
    for triple in triples {
        hasher.update(b"\0");
        hasher.update(triple.as_bytes());
    }
    hex::encode(hasher.finalize())
}

fn canonical_triple(triple: &TriplePattern) -> String {
    format!("{} {} {}", triple.subject, triple.predicate, triple.object)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PropertyKind {
    Object,
    Data,
}

struct LeafContext<'a> {
    signature: &'a OwlSignatureIndex,
    explicit: BTreeMap<String, DirectVariableRole>,
    inferred: BTreeMap<String, DirectVariableRole>,
    forms: BTreeSet<String>,
    on_property: BTreeMap<String, PropertyKind>,
    restrictions: BTreeSet<String>,
}

impl<'a> LeafContext<'a> {
    fn new(signature: &'a OwlSignatureIndex) -> Self {
        Self {
            signature,
            explicit: BTreeMap::new(),
            inferred: BTreeMap::new(),
            forms: BTreeSet::new(),
            on_property: BTreeMap::new(),
            restrictions: BTreeSet::new(),
        }
    }

    fn observe_explicit_declaration(
        &mut self,
        ordinal: usize,
        triple: &TriplePattern,
    ) -> Result<(), DirectBgpLegalityFailure> {
        if named_predicate(triple) != Some(RDF_TYPE) {
            return Ok(());
        }
        let Some(variable) = variable(&triple.subject) else {
            return Ok(());
        };
        let Some(object) = named_term(&triple.object) else {
            return Ok(());
        };
        let role = if object == owl("Class") {
            Some(DirectVariableRole::Class)
        } else if object == owl("ObjectProperty") {
            Some(DirectVariableRole::ObjectProperty)
        } else if object == owl("DatatypeProperty") {
            Some(DirectVariableRole::DataProperty)
        } else if object == owl("AnnotationProperty") {
            Some(DirectVariableRole::AnnotationProperty)
        } else if object == RDFS_DATATYPE {
            Some(DirectVariableRole::Datatype)
        } else if object == owl("NamedIndividual") {
            Some(DirectVariableRole::NamedIndividual)
        } else {
            None
        };
        if let Some(role) = role {
            if let Some(existing) = self.explicit.insert(variable.to_owned(), role)
                && existing != role
            {
                return Err(failure(
                    DirectBgpLegalityFailureCode::ConflictingVariableType,
                    format!("?{variable} is declared as both {existing:?} and {role:?}"),
                    ordinal,
                    Some(variable),
                ));
            }
        }
        Ok(())
    }

    fn index_structural_nodes(&mut self, patterns: &[TriplePattern]) {
        for triple in patterns {
            let subject = structural_node_key(&triple.subject);
            let Some(subject) = subject else {
                continue;
            };
            match named_predicate(triple) {
                Some(RDF_TYPE)
                    if named_term(&triple.object).as_deref()
                        == Some(owl("Restriction").as_str()) =>
                {
                    self.restrictions.insert(subject);
                }
                Some(p) if p == owl("onProperty") => {
                    if let Ok(kind) = self.property_kind(&triple.object, false) {
                        self.on_property.insert(subject, kind);
                    }
                }
                _ => {}
            }
        }
    }

    fn classify_triple(
        &mut self,
        ordinal: usize,
        triple: &TriplePattern,
    ) -> Result<String, DirectBgpLegalityFailure> {
        let predicate = match &triple.predicate {
            NamedNodePattern::NamedNode(node) => Some(node.as_str()),
            NamedNodePattern::Variable(variable) => {
                let role = self.explicit.get(variable.as_str()).copied().ok_or_else(|| failure(
                    DirectBgpLegalityFailureCode::UndeclaredEntityVariable,
                    format!("predicate variable ?{} requires a BGP-local owl:ObjectProperty or owl:DatatypeProperty declaration", variable.as_str()),
                    ordinal, Some(variable.as_str())))?;
                return match role {
                    DirectVariableRole::ObjectProperty => {
                        self.require_role(
                            &triple.subject,
                            DirectVariableRole::NamedIndividual,
                            ordinal,
                        )?;
                        self.require_role(
                            &triple.object,
                            DirectVariableRole::NamedIndividual,
                            ordinal,
                        )?;
                        Ok("ObjectPropertyAssertion".to_owned())
                    }
                    DirectVariableRole::DataProperty => {
                        self.require_role(
                            &triple.subject,
                            DirectVariableRole::NamedIndividual,
                            ordinal,
                        )?;
                        self.require_literal_position(&triple.object, ordinal)?;
                        Ok("DataPropertyAssertion".to_owned())
                    }
                    DirectVariableRole::AnnotationProperty => {
                        self.require_annotation_assertion_terms(
                            &triple.subject,
                            &triple.object,
                            ordinal,
                        )?;
                        Ok("AnnotationAssertion".to_owned())
                    }
                    _ => Err(failure(
                        DirectBgpLegalityFailureCode::AmbiguousVariableRole,
                        format!(
                            "predicate variable ?{} is declared with a non-property role",
                            variable.as_str()
                        ),
                        ordinal,
                        Some(variable.as_str()),
                    )),
                };
            }
        };
        let Some(predicate) = predicate else {
            return Err(failure(
                DirectBgpLegalityFailureCode::InvalidStructuralShape,
                "predicate role could not be resolved after typed-algebra dispatch".to_owned(),
                ordinal,
                None,
            ));
        };

        if predicate == RDF_TYPE {
            return self.classify_rdf_type(ordinal, triple);
        }
        if predicate == RDFS_SUBCLASS
            || predicate == owl("equivalentClass")
            || predicate == owl("disjointWith")
            || predicate == owl("complementOf")
        {
            self.require_class_position(&triple.subject, ordinal)?;
            self.require_class_position(&triple.object, ordinal)?;
            return Ok(match predicate {
                RDFS_SUBCLASS => "SubClassOf",
                p if p == owl("equivalentClass") => "EquivalentClasses",
                p if p == owl("disjointWith") => "DisjointClasses",
                _ => "ObjectComplementOf",
            }
            .to_owned());
        }
        if predicate == owl("sameAs") || predicate == owl("differentFrom") {
            self.require_role(
                &triple.subject,
                DirectVariableRole::NamedIndividual,
                ordinal,
            )?;
            self.require_role(&triple.object, DirectVariableRole::NamedIndividual, ordinal)?;
            return Ok(if predicate == owl("sameAs") {
                "SameIndividual"
            } else {
                "DifferentIndividuals"
            }
            .to_owned());
        }
        if predicate == owl("inverseOf") {
            self.require_property_role(&triple.subject, PropertyKind::Object, ordinal)?;
            self.require_property_role(&triple.object, PropertyKind::Object, ordinal)?;
            return Ok("InverseObjectProperties".to_owned());
        }
        if predicate == RDFS_SUBPROPERTY
            || predicate == owl("equivalentProperty")
            || predicate == owl("propertyDisjointWith")
        {
            let left = self
                .property_kind(&triple.subject, true)
                .map_err(|detail| {
                    failure(
                        DirectBgpLegalityFailureCode::AmbiguousVariableRole,
                        detail,
                        ordinal,
                        variable(&triple.subject),
                    )
                })?;
            let right = self.property_kind(&triple.object, true).map_err(|detail| {
                failure(
                    DirectBgpLegalityFailureCode::AmbiguousVariableRole,
                    detail,
                    ordinal,
                    variable(&triple.object),
                )
            })?;
            if left != right {
                return Err(failure(
                    DirectBgpLegalityFailureCode::InvalidStructuralShape,
                    "property axiom mixes object and data properties".to_owned(),
                    ordinal,
                    None,
                ));
            }
            self.require_property_role(&triple.subject, left, ordinal)?;
            self.require_property_role(&triple.object, right, ordinal)?;
            return Ok(match predicate {
                RDFS_SUBPROPERTY => {
                    if left == PropertyKind::Object {
                        "SubObjectPropertyOf"
                    } else {
                        "SubDataPropertyOf"
                    }
                }
                p if p == owl("equivalentProperty") => {
                    if left == PropertyKind::Object {
                        "EquivalentObjectProperties"
                    } else {
                        "EquivalentDataProperties"
                    }
                }
                _ => {
                    if left == PropertyKind::Object {
                        "DisjointObjectProperties"
                    } else {
                        "DisjointDataProperties"
                    }
                }
            }
            .to_owned());
        }
        if predicate == RDFS_DOMAIN || predicate == RDFS_RANGE {
            let kind = self
                .property_kind(&triple.subject, true)
                .map_err(|detail| {
                    failure(
                        DirectBgpLegalityFailureCode::AmbiguousVariableRole,
                        detail,
                        ordinal,
                        variable(&triple.subject),
                    )
                })?;
            self.require_property_role(&triple.subject, kind, ordinal)?;
            if predicate == RDFS_DOMAIN || kind == PropertyKind::Object {
                self.require_class_position(&triple.object, ordinal)?;
            } else {
                self.require_data_range_position(&triple.object, ordinal)?;
            }
            return Ok(match (predicate, kind) {
                (RDFS_DOMAIN, PropertyKind::Object) => "ObjectPropertyDomain",
                (RDFS_DOMAIN, PropertyKind::Data) => "DataPropertyDomain",
                (RDFS_RANGE, PropertyKind::Object) => "ObjectPropertyRange",
                _ => "DataPropertyRange",
            }
            .to_owned());
        }

        if is_restriction_predicate(predicate) {
            return self.classify_restriction(ordinal, triple, predicate);
        }
        if is_list_or_complex_predicate(predicate) {
            return self.classify_complex_structure(ordinal, triple, predicate);
        }
        if predicate == RDF_FIRST || predicate == RDF_REST {
            return self.classify_rdf_list_triple(ordinal, triple, predicate);
        }
        if is_negative_assertion_predicate(predicate) {
            return self.classify_negative_assertion(ordinal, triple, predicate);
        }

        if self.signature.object_properties.contains(predicate) {
            self.require_role(
                &triple.subject,
                DirectVariableRole::NamedIndividual,
                ordinal,
            )?;
            self.require_role(&triple.object, DirectVariableRole::NamedIndividual, ordinal)?;
            return Ok("ObjectPropertyAssertion".to_owned());
        }
        if self.signature.data_properties.contains(predicate) {
            self.require_role(
                &triple.subject,
                DirectVariableRole::NamedIndividual,
                ordinal,
            )?;
            self.require_literal_position(&triple.object, ordinal)?;
            return Ok("DataPropertyAssertion".to_owned());
        }
        if self.signature.annotation_properties.contains(predicate)
            || is_builtin_annotation_property(predicate)
        {
            self.require_annotation_assertion_terms(&triple.subject, &triple.object, ordinal)?;
            return Ok("AnnotationAssertion".to_owned());
        }
        if is_datatype_facet(predicate) {
            if structural_node_key(&triple.subject).is_none()
                || variable(&triple.object).is_some()
                || !matches!(&triple.object, TermPattern::Literal(_))
            {
                return Err(failure(DirectBgpLegalityFailureCode::InvalidStructuralShape,
                    "datatype facet restrictions require an anonymous structural node and a fixed literal facet value; W3C forbids variables in facet mappings".to_owned(), ordinal, variable(&triple.object)));
            }
            return Ok("DatatypeFacetRestriction".to_owned());
        }
        Err(failure(
            DirectBgpLegalityFailureCode::UnknownPredicate,
            format!(
                "predicate <{predicate}> is neither a declared OWL property nor a supported OWL structural predicate"
            ),
            ordinal,
            None,
        ))
    }

    fn classify_rdf_type(
        &mut self,
        ordinal: usize,
        triple: &TriplePattern,
    ) -> Result<String, DirectBgpLegalityFailure> {
        let Some(object) = named_term(&triple.object) else {
            if let Some(variable) = variable(&triple.object) {
                self.require_explicit(variable, DirectVariableRole::Class, ordinal)?;
                self.require_role(
                    &triple.subject,
                    DirectVariableRole::NamedIndividual,
                    ordinal,
                )?;
                return Ok("ClassAssertion".to_owned());
            }
            return Err(failure(DirectBgpLegalityFailureCode::InvalidStructuralShape,
                "rdf:type object in a Direct BGP must be an OWL class/declaration IRI or an explicitly class-typed variable".to_owned(), ordinal, variable(&triple.object)));
        };
        if let Some(form) = declaration_form(object.as_str()) {
            // Explicit variable declarations were already recorded. Constant declarations remain
            // legal non-logical OWL axioms.
            return Ok(form.to_owned());
        }
        if object == owl("Restriction") {
            if variable(&triple.subject).is_some() {
                return Err(failure(
                    DirectBgpLegalityFailureCode::InvalidStructuralShape,
                    "owl:Restriction structural node cannot be a SPARQL variable".to_owned(),
                    ordinal,
                    variable(&triple.subject),
                ));
            }
            return Ok("ObjectOrDataRestriction".to_owned());
        }
        if object == owl("Ontology")
            || object == owl("Axiom")
            || object == owl("AllDisjointClasses")
            || object == owl("AllDisjointProperties")
            || object == owl("AllDifferent")
            || object == owl("NegativePropertyAssertion")
        {
            if variable(&triple.subject).is_some() {
                return Err(failure(
                    DirectBgpLegalityFailureCode::InvalidStructuralShape,
                    "OWL structural administrative node cannot be a SPARQL variable".to_owned(),
                    ordinal,
                    variable(&triple.subject),
                ));
            }
            return Ok("OwlStructuralNode".to_owned());
        }
        if self.is_class_iri(object.as_str()) {
            self.require_role(
                &triple.subject,
                DirectVariableRole::NamedIndividual,
                ordinal,
            )?;
            return Ok("ClassAssertion".to_owned());
        }
        // OWL property-characteristic declarations are logical axioms over declared properties.
        if object == owl("FunctionalProperty") {
            let kind = self
                .property_kind(&triple.subject, true)
                .map_err(|detail| {
                    failure(
                        DirectBgpLegalityFailureCode::AmbiguousVariableRole,
                        detail,
                        ordinal,
                        variable(&triple.subject),
                    )
                })?;
            self.require_property_role(&triple.subject, kind, ordinal)?;
            return Ok(if kind == PropertyKind::Object {
                "FunctionalObjectProperty"
            } else {
                "FunctionalDataProperty"
            }
            .to_owned());
        }
        if let Some(kind) = property_characteristic(object.as_str()) {
            self.require_property_role(&triple.subject, kind, ordinal)?;
            return Ok("ObjectPropertyCharacteristic".to_owned());
        }
        Err(failure(
            DirectBgpLegalityFailureCode::InvalidStructuralShape,
            format!(
                "rdf:type object <{object}> is not a declared class or supported OWL structural type"
            ),
            ordinal,
            None,
        ))
    }

    fn classify_restriction(
        &mut self,
        ordinal: usize,
        triple: &TriplePattern,
        predicate: &str,
    ) -> Result<String, DirectBgpLegalityFailure> {
        let node = structural_node_key(&triple.subject).ok_or_else(|| {
            failure(
                DirectBgpLegalityFailureCode::InvalidStructuralShape,
                format!("{predicate} subject must be an anonymous OWL structural node"),
                ordinal,
                variable(&triple.subject),
            )
        })?;
        if predicate == owl("onProperty") {
            let kind = self.property_kind(&triple.object, true).map_err(|detail| {
                failure(
                    DirectBgpLegalityFailureCode::AmbiguousVariableRole,
                    detail,
                    ordinal,
                    variable(&triple.object),
                )
            })?;
            self.require_property_role(&triple.object, kind, ordinal)?;
            self.on_property.insert(node, kind);
            return Ok("RestrictionOnProperty".to_owned());
        }
        let kind = self.on_property.get(&node).copied().ok_or_else(|| {
            failure(
                DirectBgpLegalityFailureCode::InvalidStructuralShape,
                format!("{predicate} requires owl:onProperty in the same BGP structural node"),
                ordinal,
                None,
            )
        })?;
        if predicate == owl("someValuesFrom") || predicate == owl("allValuesFrom") {
            if kind == PropertyKind::Object {
                self.require_class_position(&triple.object, ordinal)?;
            } else {
                self.require_data_range_position(&triple.object, ordinal)?;
            }
            return Ok(if predicate == owl("someValuesFrom") {
                "SomeValuesFrom"
            } else {
                "AllValuesFrom"
            }
            .to_owned());
        }
        if predicate == owl("hasValue") {
            if kind == PropertyKind::Object {
                self.require_role(&triple.object, DirectVariableRole::NamedIndividual, ordinal)?;
            } else {
                self.require_literal_position(&triple.object, ordinal)?;
            }
            return Ok("HasValue".to_owned());
        }
        if predicate == owl("hasSelf") {
            if kind != PropertyKind::Object || !is_literal_boolean(&triple.object, true) {
                return Err(failure(
                    DirectBgpLegalityFailureCode::InvalidStructuralShape,
                    "owl:hasSelf requires an object property and literal true".to_owned(),
                    ordinal,
                    variable(&triple.object),
                ));
            }
            return Ok("ObjectHasSelf".to_owned());
        }
        if is_cardinality_predicate(predicate) {
            if variable(&triple.object).is_some()
                || !is_non_negative_integer_literal(&triple.object)
            {
                return Err(failure(DirectBgpLegalityFailureCode::InvalidStructuralShape,
                    "OWL cardinalities must be fixed non-negative integer literals, never variables".to_owned(), ordinal, variable(&triple.object)));
            }
            return Ok("CardinalityRestriction".to_owned());
        }
        if predicate == owl("onClass") {
            if kind != PropertyKind::Object {
                return Err(failure(
                    DirectBgpLegalityFailureCode::InvalidStructuralShape,
                    "owl:onClass requires an object-property restriction".to_owned(),
                    ordinal,
                    None,
                ));
            }
            self.require_class_position(&triple.object, ordinal)?;
            return Ok("QualifiedObjectCardinalityClass".to_owned());
        }
        if predicate == owl("onDataRange") {
            if kind != PropertyKind::Data {
                return Err(failure(
                    DirectBgpLegalityFailureCode::InvalidStructuralShape,
                    "owl:onDataRange requires a data-property restriction".to_owned(),
                    ordinal,
                    None,
                ));
            }
            self.require_data_range_position(&triple.object, ordinal)?;
            return Ok("QualifiedDataCardinalityRange".to_owned());
        }
        Err(failure(
            DirectBgpLegalityFailureCode::UnsupportedOwlStructure,
            format!("unsupported restriction predicate {predicate}"),
            ordinal,
            None,
        ))
    }

    fn classify_complex_structure(
        &mut self,
        ordinal: usize,
        triple: &TriplePattern,
        predicate: &str,
    ) -> Result<String, DirectBgpLegalityFailure> {
        // RDF list internals are validated after grounding in Phase 40.8. At query admission we
        // require fixed structural nodes and disallow variables in list-head positions, which
        // avoids ambiguous class/property/list roles.
        if variable(&triple.subject).is_some() || variable(&triple.object).is_some() {
            return Err(failure(
                DirectBgpLegalityFailureCode::AmbiguousVariableRole,
                format!(
                    "variables inside {predicate} RDF structural-list links are not unambiguously typed by the Phase 40.7 preflight"
                ),
                ordinal,
                variable(&triple.subject).or_else(|| variable(&triple.object)),
            ));
        }
        Ok(match predicate {
            p if p == owl("intersectionOf") => "ObjectIntersectionOf",
            p if p == owl("unionOf") => "ObjectUnionOf",
            p if p == owl("oneOf") => "ObjectOrDataOneOf",
            p if p == owl("propertyChainAxiom") => "SubObjectPropertyOfChain",
            p if p == owl("hasKey") => "HasKey",
            p if p == owl("members") => "NaryDisjointStructure",
            p if p == owl("distinctMembers") => "DifferentIndividualsStructure",
            p if p == owl("disjointUnionOf") => "DisjointUnion",
            _ => "OwlComplexStructure",
        }
        .to_owned())
    }

    fn classify_rdf_list_triple(
        &mut self,
        ordinal: usize,
        triple: &TriplePattern,
        predicate: &str,
    ) -> Result<String, DirectBgpLegalityFailure> {
        if variable(&triple.subject).is_some()
            || (predicate == RDF_REST && variable(&triple.object).is_some())
        {
            return Err(failure(
                DirectBgpLegalityFailureCode::AmbiguousVariableRole,
                "RDF list spine variables are not legal OWL structural placeholders".to_owned(),
                ordinal,
                variable(&triple.subject).or_else(|| variable(&triple.object)),
            ));
        }
        // rdf:first values may be constants or variables only when their role can be established
        // from a surrounding OWL list construct. Full list-role propagation is deferred to the
        // grounded structural validation in 40.8, so variable members fail closed here.
        if predicate == RDF_FIRST && variable(&triple.object).is_some() {
            return Err(failure(DirectBgpLegalityFailureCode::AmbiguousVariableRole,
                "rdf:first variable requires structural-list role propagation and is rejected by the Phase 40.7 fail-closed classifier".to_owned(), ordinal, variable(&triple.object)));
        }
        Ok("RdfListStructure".to_owned())
    }

    fn classify_negative_assertion(
        &mut self,
        ordinal: usize,
        triple: &TriplePattern,
        predicate: &str,
    ) -> Result<String, DirectBgpLegalityFailure> {
        if variable(&triple.subject).is_some() {
            return Err(failure(
                DirectBgpLegalityFailureCode::InvalidStructuralShape,
                "negative-property assertion structural node cannot be a SPARQL variable"
                    .to_owned(),
                ordinal,
                variable(&triple.subject),
            ));
        }
        if predicate == owl("sourceIndividual") || predicate == owl("targetIndividual") {
            self.require_role(&triple.object, DirectVariableRole::NamedIndividual, ordinal)?;
        } else if predicate == owl("targetValue") {
            self.require_literal_position(&triple.object, ordinal)?;
        } else if predicate == owl("assertionProperty") {
            let kind = self.property_kind(&triple.object, true).map_err(|detail| {
                failure(
                    DirectBgpLegalityFailureCode::AmbiguousVariableRole,
                    detail,
                    ordinal,
                    variable(&triple.object),
                )
            })?;
            self.require_property_role(&triple.object, kind, ordinal)?;
        }
        Ok("NegativePropertyAssertion".to_owned())
    }

    fn require_class_position(
        &mut self,
        term: &TermPattern,
        ordinal: usize,
    ) -> Result<(), DirectBgpLegalityFailure> {
        if let Some(variable) = variable(term) {
            return self.require_explicit(variable, DirectVariableRole::Class, ordinal);
        }
        if let Some(iri) = named_term(term) {
            if self.is_class_iri(&iri) {
                return Ok(());
            }
            return Err(failure(
                DirectBgpLegalityFailureCode::InvalidStructuralShape,
                format!("<{iri}> is not declared as an OWL class"),
                ordinal,
                None,
            ));
        }
        if structural_node_key(term).is_some() {
            return Ok(());
        }
        Err(failure(
            DirectBgpLegalityFailureCode::InvalidStructuralShape,
            "literal cannot occur in an OWL class position".to_owned(),
            ordinal,
            None,
        ))
    }

    fn require_data_range_position(
        &mut self,
        term: &TermPattern,
        ordinal: usize,
    ) -> Result<(), DirectBgpLegalityFailure> {
        if let Some(variable) = variable(term) {
            return self.require_explicit(variable, DirectVariableRole::Datatype, ordinal);
        }
        if let Some(iri) = named_term(term) {
            if self.signature.datatypes.contains(&iri) {
                return Ok(());
            }
            return Err(failure(
                DirectBgpLegalityFailureCode::InvalidStructuralShape,
                format!("<{iri}> is not present in the qualified datatype signature"),
                ordinal,
                None,
            ));
        }
        if structural_node_key(term).is_some() {
            return Ok(());
        }
        Err(failure(DirectBgpLegalityFailureCode::InvalidStructuralShape, "data-range position requires a datatype IRI, explicitly datatype-typed variable, or anonymous data-range structure".to_owned(), ordinal, None))
    }

    fn require_annotation_assertion_terms(
        &mut self,
        subject: &TermPattern,
        object: &TermPattern,
        ordinal: usize,
    ) -> Result<(), DirectBgpLegalityFailure> {
        if let Some(variable) = variable(subject) {
            self.infer(variable, DirectVariableRole::NamedIndividual, ordinal)?;
        } else if matches!(subject, TermPattern::Literal(_)) {
            return Err(failure(
                DirectBgpLegalityFailureCode::InvalidStructuralShape,
                "annotation subject cannot be a literal".to_owned(),
                ordinal,
                None,
            ));
        }
        if let Some(variable) = variable(object) {
            // Annotation values can be IRI/anonymous-individual/literal. Without another local use
            // the variable is genuinely ambiguous, so fail closed rather than guessing.
            if !self.explicit.contains_key(variable) && !self.inferred.contains_key(variable) {
                return Err(failure(
                    DirectBgpLegalityFailureCode::AmbiguousVariableRole,
                    "annotation-value variable is ambiguous between individual and literal roles"
                        .to_owned(),
                    ordinal,
                    Some(variable),
                ));
            }
        }
        Ok(())
    }

    fn require_literal_position(
        &mut self,
        term: &TermPattern,
        ordinal: usize,
    ) -> Result<(), DirectBgpLegalityFailure> {
        if let Some(variable) = variable(term) {
            return self.infer(variable, DirectVariableRole::Literal, ordinal);
        }
        if matches!(term, TermPattern::Literal(_)) {
            return Ok(());
        }
        Err(failure(
            DirectBgpLegalityFailureCode::InvalidStructuralShape,
            "data-property object must be a literal or literal-position variable".to_owned(),
            ordinal,
            None,
        ))
    }

    fn require_role(
        &mut self,
        term: &TermPattern,
        role: DirectVariableRole,
        ordinal: usize,
    ) -> Result<(), DirectBgpLegalityFailure> {
        if let Some(variable) = variable(term) {
            return self.infer(variable, role, ordinal);
        }
        match role {
            DirectVariableRole::NamedIndividual => {
                if matches!(term, TermPattern::NamedNode(_) | TermPattern::BlankNode(_)) {
                    Ok(())
                } else {
                    Err(failure(
                        DirectBgpLegalityFailureCode::InvalidStructuralShape,
                        "individual position cannot contain a literal".to_owned(),
                        ordinal,
                        None,
                    ))
                }
            }
            _ => Ok(()),
        }
    }

    fn require_property_role(
        &mut self,
        term: &TermPattern,
        kind: PropertyKind,
        ordinal: usize,
    ) -> Result<(), DirectBgpLegalityFailure> {
        let role = if kind == PropertyKind::Object {
            DirectVariableRole::ObjectProperty
        } else {
            DirectVariableRole::DataProperty
        };
        if let Some(variable) = variable(term) {
            return self.require_explicit(variable, role, ordinal);
        }
        let Some(iri) = named_term(term) else {
            return Err(failure(
                DirectBgpLegalityFailureCode::InvalidStructuralShape,
                "property position requires IRI or explicitly property-typed variable".to_owned(),
                ordinal,
                None,
            ));
        };
        let valid = match kind {
            PropertyKind::Object => self.signature.object_properties.contains(&iri),
            PropertyKind::Data => self.signature.data_properties.contains(&iri),
        };
        if valid {
            Ok(())
        } else {
            Err(failure(
                DirectBgpLegalityFailureCode::InvalidStructuralShape,
                format!("<{iri}> is not declared as the required {kind:?} property"),
                ordinal,
                None,
            ))
        }
    }

    fn property_kind(
        &self,
        term: &TermPattern,
        require_explicit_variable: bool,
    ) -> Result<PropertyKind, String> {
        if let Some(variable) = variable(term) {
            let role = self.explicit.get(variable).copied();
            return match role {
                Some(DirectVariableRole::ObjectProperty) => Ok(PropertyKind::Object),
                Some(DirectVariableRole::DataProperty) => Ok(PropertyKind::Data),
                Some(other) => Err(format!(
                    "?{variable} has incompatible explicit role {other:?}"
                )),
                None if require_explicit_variable => Err(format!(
                    "?{variable} requires an explicit owl:ObjectProperty or owl:DatatypeProperty declaration"
                )),
                None => Err(format!("?{variable} has no property declaration")),
            };
        }
        let Some(iri) = named_term(term) else {
            return Err("property position is neither IRI nor variable".to_owned());
        };
        let object = self.signature.object_properties.contains(&iri);
        let data = self.signature.data_properties.contains(&iri);
        match (object, data) {
            (true, false) => Ok(PropertyKind::Object),
            (false, true) => Ok(PropertyKind::Data),
            (true, true) => Err(format!(
                "<{iri}> is ambiguously declared as object and data property"
            )),
            (false, false) => Err(format!("<{iri}> is not a declared object/data property")),
        }
    }

    fn require_explicit(
        &mut self,
        variable: &str,
        role: DirectVariableRole,
        ordinal: usize,
    ) -> Result<(), DirectBgpLegalityFailure> {
        match self.explicit.get(variable).copied() {
            Some(found) if found == role => Ok(()),
            Some(found) => Err(failure(
                DirectBgpLegalityFailureCode::ConflictingVariableType,
                format!("?{variable} is declared as {found:?} but occurs in {role:?} position"),
                ordinal,
                Some(variable),
            )),
            None => Err(failure(
                DirectBgpLegalityFailureCode::UndeclaredEntityVariable,
                format!(
                    "?{variable} occurs in OWL {role:?} position without the required BGP-local declaration"
                ),
                ordinal,
                Some(variable),
            )),
        }
    }

    fn infer(
        &mut self,
        variable: &str,
        role: DirectVariableRole,
        ordinal: usize,
    ) -> Result<(), DirectBgpLegalityFailure> {
        // W3C CP4 permits undeclared variables only in individual/literal positions. Explicit
        // declarations override inference but must agree with the position.
        if !matches!(
            role,
            DirectVariableRole::NamedIndividual | DirectVariableRole::Literal
        ) {
            return self.require_explicit(variable, role, ordinal);
        }
        if let Some(explicit) = self.explicit.get(variable).copied() {
            if explicit == role {
                return Ok(());
            }
            return Err(failure(
                DirectBgpLegalityFailureCode::ConflictingVariableType,
                format!("?{variable} is declared as {explicit:?} but occurs in {role:?} position"),
                ordinal,
                Some(variable),
            ));
        }
        if let Some(existing) = self.inferred.insert(variable.to_owned(), role)
            && existing != role
        {
            return Err(failure(
                DirectBgpLegalityFailureCode::AmbiguousVariableRole,
                format!("?{variable} is used in both {existing:?} and {role:?} positions"),
                ordinal,
                Some(variable),
            ));
        }
        Ok(())
    }

    fn is_class_iri(&self, iri: &str) -> bool {
        self.signature.classes.contains(iri)
    }

    fn finish(&self) -> Result<(), DirectBgpLegalityFailure> {
        // Any explicit class/property declaration that never conflicts is legal. Structural-node
        // completeness beyond the query-level W3C mapping is checked again after candidate
        // grounding in 40.8; no entailment result can bypass that check.
        Ok(())
    }

    fn variable_evidence(&self) -> Vec<DirectVariableTyping> {
        let names = self
            .explicit
            .keys()
            .chain(self.inferred.keys())
            .cloned()
            .collect::<BTreeSet<_>>();
        names
            .into_iter()
            .map(|variable| {
                if let Some(role) = self.explicit.get(&variable).copied() {
                    DirectVariableTyping {
                        variable,
                        role,
                        source: DirectVariableRoleSource::ExplicitDeclaration,
                    }
                } else {
                    DirectVariableTyping {
                        role: self.inferred[&variable],
                        variable,
                        source: DirectVariableRoleSource::StructuralPosition,
                    }
                }
            })
            .collect()
    }
}

fn declaration_form(iri: &str) -> Option<&'static str> {
    if iri == owl("Class") {
        Some("DeclarationClass")
    } else if iri == owl("ObjectProperty") {
        Some("DeclarationObjectProperty")
    } else if iri == owl("DatatypeProperty") {
        Some("DeclarationDataProperty")
    } else if iri == owl("NamedIndividual") {
        Some("DeclarationNamedIndividual")
    } else if iri == owl("AnnotationProperty") {
        Some("DeclarationAnnotationProperty")
    } else if iri == RDFS_DATATYPE {
        Some("DeclarationDatatype")
    } else {
        None
    }
}

fn property_characteristic(iri: &str) -> Option<PropertyKind> {
    if [
        "InverseFunctionalProperty",
        "TransitiveProperty",
        "SymmetricProperty",
        "AsymmetricProperty",
        "ReflexiveProperty",
        "IrreflexiveProperty",
    ]
    .iter()
    .any(|local| iri == owl(local))
    {
        Some(PropertyKind::Object)
    } else {
        None
    }
}

fn is_restriction_predicate(iri: &str) -> bool {
    [
        "onProperty",
        "someValuesFrom",
        "allValuesFrom",
        "hasValue",
        "hasSelf",
        "minCardinality",
        "maxCardinality",
        "cardinality",
        "minQualifiedCardinality",
        "maxQualifiedCardinality",
        "qualifiedCardinality",
        "onClass",
        "onDataRange",
    ]
    .iter()
    .any(|local| iri == owl(local))
}
fn is_cardinality_predicate(iri: &str) -> bool {
    [
        "minCardinality",
        "maxCardinality",
        "cardinality",
        "minQualifiedCardinality",
        "maxQualifiedCardinality",
        "qualifiedCardinality",
    ]
    .iter()
    .any(|local| iri == owl(local))
}
fn is_list_or_complex_predicate(iri: &str) -> bool {
    [
        "intersectionOf",
        "unionOf",
        "oneOf",
        "propertyChainAxiom",
        "hasKey",
        "members",
        "distinctMembers",
        "disjointUnionOf",
        "datatypeComplementOf",
        "onDatatype",
        "withRestrictions",
    ]
    .iter()
    .any(|local| iri == owl(local))
}
fn is_negative_assertion_predicate(iri: &str) -> bool {
    [
        "sourceIndividual",
        "assertionProperty",
        "targetIndividual",
        "targetValue",
    ]
    .iter()
    .any(|local| iri == owl(local))
}
fn is_datatype_facet(iri: &str) -> bool {
    matches!(
        iri,
        "http://www.w3.org/2001/XMLSchema#length"
            | "http://www.w3.org/2001/XMLSchema#minLength"
            | "http://www.w3.org/2001/XMLSchema#maxLength"
            | "http://www.w3.org/2001/XMLSchema#pattern"
            | "http://www.w3.org/2001/XMLSchema#minInclusive"
            | "http://www.w3.org/2001/XMLSchema#maxInclusive"
            | "http://www.w3.org/2001/XMLSchema#minExclusive"
            | "http://www.w3.org/2001/XMLSchema#maxExclusive"
            | "http://www.w3.org/2001/XMLSchema#totalDigits"
            | "http://www.w3.org/2001/XMLSchema#fractionDigits"
            | "http://www.w3.org/1999/02/22-rdf-syntax-ns#langRange"
    )
}

fn is_builtin_annotation_property(iri: &str) -> bool {
    matches!(
        iri,
        "http://www.w3.org/2000/01/rdf-schema#label"
            | "http://www.w3.org/2000/01/rdf-schema#comment"
            | "http://www.w3.org/2000/01/rdf-schema#seeAlso"
            | "http://www.w3.org/2000/01/rdf-schema#isDefinedBy"
            | "http://www.w3.org/2002/07/owl#versionInfo"
            | "http://www.w3.org/2002/07/owl#deprecated"
    )
}

fn named_predicate(triple: &TriplePattern) -> Option<&str> {
    match &triple.predicate {
        NamedNodePattern::NamedNode(node) => Some(node.as_str()),
        NamedNodePattern::Variable(_) => None,
    }
}
fn variable(term: &TermPattern) -> Option<&str> {
    match term {
        TermPattern::Variable(v) => Some(v.as_str()),
        _ => None,
    }
}
fn named_term(term: &TermPattern) -> Option<String> {
    match term {
        TermPattern::NamedNode(node) => Some(node.as_str().to_owned()),
        _ => None,
    }
}
fn structural_node_key(term: &TermPattern) -> Option<String> {
    match term {
        TermPattern::BlankNode(node) => Some(format!("_:{}", node.as_str())),
        _ => None,
    }
}
fn is_literal_boolean(term: &TermPattern, expected: bool) -> bool {
    match term {
        TermPattern::Literal(literal) => {
            literal.to_string() == expected.to_string()
                || literal.to_string().starts_with(&format!("\"{expected}\""))
        }
        _ => false,
    }
}
fn is_non_negative_integer_literal(term: &TermPattern) -> bool {
    let TermPattern::Literal(literal) = term else {
        return false;
    };
    let rendered = literal.to_string();
    let lexical = rendered
        .trim_matches('"')
        .split('"')
        .next()
        .unwrap_or(&rendered);
    lexical.parse::<u64>().is_ok()
}
fn failure(
    code: DirectBgpLegalityFailureCode,
    detail: String,
    ordinal: usize,
    variable: Option<&str>,
) -> DirectBgpLegalityFailure {
    DirectBgpLegalityFailure {
        code,
        detail: detail.chars().take(2048).collect(),
        triple_ordinal: u64::try_from(ordinal).ok(),
        variable: variable.map(ToOwned::to_owned),
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::*;

    fn signature() -> OwlSignatureIndex {
        OwlSignatureIndex {
            classes: ["https://example.test/Person".to_owned()]
                .into_iter()
                .collect(),
            object_properties: ["https://example.test/knows".to_owned()]
                .into_iter()
                .collect(),
            data_properties: ["https://example.test/age".to_owned()]
                .into_iter()
                .collect(),
            named_individuals: BTreeSet::new(),
            annotation_properties: ["https://example.test/note".to_owned()]
                .into_iter()
                .collect(),
            datatypes: ["http://www.w3.org/2001/XMLSchema#integer".to_owned()]
                .into_iter()
                .collect(),
        }
        .with_builtins()
    }

    #[test]
    fn declared_class_variable_is_legal_and_local() -> Result<(), Box<dyn Error>> {
        let query = CompiledSparqlQuery::parse(
            "SELECT ?c ?x WHERE { ?c a <http://www.w3.org/2002/07/owl#Class> . ?x a ?c . }",
        )?;
        let out = classify_direct_bgps(
            &query,
            &signature(),
            DirectBgpClassificationLimits::default(),
        )?;
        assert_eq!(out.records.len(), 1);
        assert_eq!(out.records[0].status, DirectBgpLegalityStatus::Legal);
        assert!(
            out.records[0]
                .variables
                .iter()
                .any(|v| v.variable == "c" && v.role == DirectVariableRole::Class)
        );
        Ok(())
    }

    #[test]
    fn datatype_variable_is_accepted_only_with_local_declaration() -> Result<(), Box<dyn Error>> {
        let query = CompiledSparqlQuery::parse(
            "SELECT ?d WHERE { ?d a <http://www.w3.org/2000/01/rdf-schema#Datatype> . <https://example.test/age> <http://www.w3.org/2000/01/rdf-schema#range> ?d . }",
        )?;
        let out = classify_direct_bgps(
            &query,
            &signature(),
            DirectBgpClassificationLimits::default(),
        )?;
        assert_eq!(out.records[0].status, DirectBgpLegalityStatus::Legal);
        assert!(
            out.records[0]
                .variables
                .iter()
                .any(|v| v.variable == "d" && v.role == DirectVariableRole::Datatype)
        );
        Ok(())
    }

    #[test]
    fn untyped_predicate_variable_fails_closed() -> Result<(), Box<dyn Error>> {
        let query = CompiledSparqlQuery::parse("SELECT * WHERE { ?s ?p ?o }")?;
        let out = classify_direct_bgps(
            &query,
            &signature(),
            DirectBgpClassificationLimits::default(),
        )?;
        assert_eq!(out.records[0].status, DirectBgpLegalityStatus::Illegal);
        assert_eq!(
            out.records[0].failure.as_ref().map(|f| f.code),
            Some(DirectBgpLegalityFailureCode::UndeclaredEntityVariable)
        );
        Ok(())
    }

    #[test]
    fn signature_disambiguates_object_and_data_assertions() -> Result<(), Box<dyn Error>> {
        for (query_text, form) in [
            (
                "SELECT * WHERE { ?s <https://example.test/knows> ?o }",
                "ObjectPropertyAssertion",
            ),
            (
                "SELECT * WHERE { ?s <https://example.test/age> ?o }",
                "DataPropertyAssertion",
            ),
        ] {
            let query = CompiledSparqlQuery::parse(query_text)?;
            let out = classify_direct_bgps(
                &query,
                &signature(),
                DirectBgpClassificationLimits::default(),
            )?;
            assert_eq!(out.records[0].status, DirectBgpLegalityStatus::Legal);
            assert!(out.records[0].recognized_forms.contains(&form.to_owned()));
        }
        Ok(())
    }

    #[test]
    fn conflicting_variable_declarations_are_illegal() -> Result<(), Box<dyn Error>> {
        let query = CompiledSparqlQuery::parse(
            "SELECT * WHERE { ?p a <http://www.w3.org/2002/07/owl#ObjectProperty> ; a <http://www.w3.org/2002/07/owl#DatatypeProperty> . ?s ?p ?o }",
        )?;
        let out = classify_direct_bgps(
            &query,
            &signature(),
            DirectBgpClassificationLimits::default(),
        )?;
        assert_eq!(out.records[0].status, DirectBgpLegalityStatus::Illegal);
        assert_eq!(
            out.records[0].failure.as_ref().map(|f| f.code),
            Some(DirectBgpLegalityFailureCode::ConflictingVariableType)
        );
        Ok(())
    }

    #[test]
    fn declarations_do_not_leak_across_union_bgps() -> Result<(), Box<dyn Error>> {
        let query = CompiledSparqlQuery::parse(
            "SELECT * WHERE { { ?p a <http://www.w3.org/2002/07/owl#ObjectProperty> . ?s ?p ?o } UNION { ?x ?p ?y } }",
        )?;
        let out = classify_direct_bgps(
            &query,
            &signature(),
            DirectBgpClassificationLimits::default(),
        )?;
        assert_eq!(out.records.len(), 2);
        assert_eq!(out.records[0].status, DirectBgpLegalityStatus::Legal);
        assert_eq!(out.records[1].status, DirectBgpLegalityStatus::Illegal);
        Ok(())
    }

    #[test]
    fn bgps_inside_filter_exists_are_classified_independently() -> Result<(), Box<dyn Error>> {
        let query = CompiledSparqlQuery::parse(
            "SELECT * WHERE { ?s <https://example.test/knows> ?o . FILTER EXISTS { ?x ?p ?y } }",
        )?;
        let out = classify_direct_bgps(
            &query,
            &signature(),
            DirectBgpClassificationLimits::default(),
        )?;
        assert_eq!(out.records.len(), 2);
        assert_eq!(out.records[0].status, DirectBgpLegalityStatus::Legal);
        assert_eq!(out.records[1].status, DirectBgpLegalityStatus::Illegal);
        assert_eq!(
            out.records[1].failure.as_ref().map(|failure| failure.code),
            Some(DirectBgpLegalityFailureCode::UndeclaredEntityVariable),
        );
        Ok(())
    }

    #[test]
    fn graph_scope_is_preserved_and_property_paths_are_outside_direct_bgps()
    -> Result<(), Box<dyn Error>> {
        let query = CompiledSparqlQuery::parse(
            "SELECT * WHERE { GRAPH <https://example.test/g> { ?s <https://example.test/knows> ?o } ?s <https://example.test/knows>+ ?o }",
        )?;
        let out = classify_direct_bgps(
            &query,
            &signature(),
            DirectBgpClassificationLimits::default(),
        )?;
        assert!(out.property_paths_outside_direct_bgps);
        assert!(matches!(
            out.records[0].graph_scope,
            DirectBgpScope::Named { .. }
        ));
        Ok(())
    }

    #[test]
    fn exact_template_is_bound_to_legality_record() -> Result<(), Box<dyn Error>> {
        let query = CompiledSparqlQuery::parse(
            "SELECT ?s ?o WHERE { ?s <https://example.test/knows> ?o }",
        )?;
        let out = classify_direct_bgps(
            &query,
            &signature(),
            DirectBgpClassificationLimits::default(),
        )?;
        let template = extract_direct_bgp_template(&query, &out.records[0])?;
        assert_eq!(template.bgp_sha256, out.records[0].bgp_sha256);
        assert_eq!(template.triples.len(), 1);
        assert_eq!(template.variables.len(), 2);
        Ok(())
    }
}
