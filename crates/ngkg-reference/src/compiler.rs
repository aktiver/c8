//! Atomic orchestration of the first real NGKG reference compilation.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File},
    io::Write,
    path::{Path, PathBuf},
};

use ngkg_dataset::{
    GraphCatalog, LogicalGraphName, ProtocolDatasetSpecification, ResolvedDataset, compile_catalog,
    resolve_dataset,
};
use ngkg_query_executor::{inner_join_sparql_json, project_sparql_json};
use ngkg_sparql_compiler::{CompiledSparqlQuery, QueryForm, SPARQL_ALGEBRA_FORMAT_VERSION};
use oxigraph::model::NamedNode;
use oxigraph::{
    io::{RdfFormat, RdfParser},
    model::Term,
};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::{
    artifact_record,
    datatype_policy::{validate_reasoning_literals, write_embedded_policy},
    decode_sha256,
    locator::write_locator,
    model::{
        ArtifactRecord, CertifiedQueryInput, CertifiedQueryRecord, DistributedQueryCertificate,
        DistributedQueryFragment, DistributedQueryPlanFile, GraphCapabilityIndexFile,
        GraphCapabilityRecord, InputArtifact, QueryRoutingCertificate, ReasonerInputArtifact,
        ReasonerRequest, ReferenceCompileManifest, ReferenceSnapshotManifest, Treatment,
        TrustedReasonerConfig, TrustedResourceCeilings,
    },
    parquet_io::{build_dictionaries, write_payload, write_semantic_spine},
    query::{
        ExecutedQueryResult, ExpectedQueryResult, ExpectedSolutions, QUERY_RESULT_HASH_VERSION,
        QueryExecutionLimits, ReferenceQueryError, build_store, canonical_sparql_multiset_sha256,
        execute_compiled_query_with_dataset, execute_select, parse_expected, query_file,
        verify_binding_values, verify_expected, verify_source_links,
    },
    rdf::{
        GraphScope, NormalizedFact, NormalizedObject, nquad_line, ntriple_line, parse_nquads,
        parse_trig,
    },
    reasoner::invoke_reasoner,
    sha256_file,
};

const ROUTE_MODE_TYPED_ACTIVE_DATASET: &str = "typed_active_dataset";
const ROUTE_MODE_TYPED_DECLARED_GRAPH: &str = "typed_declared_graph";
const ROUTE_MODE_TYPED_PROPERTY_PATH_FULL_ACTIVE_DEFAULT: &str =
    "typed_property_path_full_active_default";
const ROUTE_MODE_TYPED_ACTIVE_DEFAULT_NO_CAPABILITY: &str = "typed_active_default_no_capability";
const ROUTE_MODE_TYPED_CAPABILITY_DEPENDENCY: &str = "typed_capability_dependency";
const ROUTE_MODE_TYPED_ACTIVE_DATASET_FALLBACK: &str = "typed_active_dataset_fallback";
const CERTIFIED_QUERY_SCOPE: &str = "immutable snapshot plus exact query bytes, canonical SPARQL algebra, active RDF dataset, authorization set, and independently verified form-specific SPARQL result";

#[derive(Debug, Error)]
pub enum ReferenceCompileError {
    #[error("manifest or artifact I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("compile manifest YAML is invalid: {0}")]
    Manifest(#[from] serde_yaml::Error),
    #[error("JSON artifact generation failed: {0}")]
    Json(#[from] serde_json::Error),
    #[error("input artifact path escapes the configured input root: {0}")]
    InputRoot(PathBuf),
    #[error("output path escapes the configured output root: {0}")]
    OutputRoot(PathBuf),
    #[error("artifact checksum mismatch for {path}: expected {expected}, observed {observed}")]
    ChecksumMismatch {
        path: PathBuf,
        expected: String,
        observed: String,
    },
    #[error("SHA-256 must contain exactly 64 lowercase hexadecimal characters")]
    InvalidSha256,
    #[error("source input exceeds maxInputBytes")]
    InputLimit,
    #[error("formatVersion must be 1")]
    FormatVersion,
    #[error("compile limits and query corpus must be non-empty and non-zero")]
    InvalidLimits,
    #[error("snapshot or staging directory already exists: {0}")]
    ExistingOutput(PathBuf),
    #[error("closureGraphIri is not a valid absolute IRI")]
    InvalidClosureGraph,
    #[error("RDF compilation failed: {0}")]
    Rdf(#[from] crate::rdf::RdfCompileError),
    #[error("datatype policy or reasoning-literal validation failed: {0}")]
    DatatypePolicy(#[from] crate::datatype_policy::DatatypePolicyError),
    #[error("Parquet compilation failed: {0}")]
    Parquet(#[from] crate::parquet_io::ParquetIoError),
    #[error("locator compilation failed: {0}")]
    Locator(#[from] crate::locator::LocatorFileError),
    #[error("reasoner invocation failed: {0}")]
    Reasoner(#[from] crate::reasoner::ReasonerInvocationError),
    #[error("reference query certification failed: {0}")]
    Query(#[from] crate::query::ReferenceQueryError),
    #[error("duplicate certified query ID or hash: {0}")]
    DuplicateQuery(String),
    #[error("unsupported ontology serialization for reference ingestion: {0}")]
    UnsupportedOntologyFormat(PathBuf),
    #[error("ontology RDF parsing failed: {0}")]
    OntologyParse(String),
    #[error("ontology document does not declare an owl:Ontology IRI: {0}")]
    MissingOntologyIri(PathBuf),
    #[error("ontology document declares more than one owl:Ontology IRI: {0}")]
    MultipleOntologyIris(PathBuf),
    #[error("ontology document declares more than one owl:versionIRI for its ontology header: {0}")]
    MultipleVersionIris(PathBuf),
    #[error("owl:versionIRI or owl:imports is not attached to the document ontology header: {0}")]
    MisplacedOntologyHeader(PathBuf),
    #[error("owl:imports target is not present in the checksum-bound ontology bundle: {0}")]
    UnresolvedImport(String),
    #[error("ontology IRI or version IRI is declared by multiple documents: {0}")]
    DuplicateOntologyIri(String),
    #[error("source named graph collides with the reserved reasoner closure graph")]
    ClosureGraphCollision,
}

/// Compile one immutable source into a locally atomic reference snapshot.
pub fn compile_from_manifest(
    manifest_path: &Path,
    allowed_input_root: &Path,
    allowed_output_root: &Path,
    trusted_reasoner: &TrustedReasonerConfig,
    ceilings: TrustedResourceCeilings,
) -> Result<PathBuf, ReferenceCompileError> {
    let allowed_input_root = fs::canonicalize(allowed_input_root)?;
    let allowed_output_root = fs::canonicalize(allowed_output_root)?;
    let manifest_path = fs::canonicalize(manifest_path)?;
    require_under(&manifest_path, &allowed_input_root, true)?;
    let manifest_bytes = fs::read(&manifest_path)?;
    let mut manifest: ReferenceCompileManifest = serde_yaml::from_slice(&manifest_bytes)?;
    validate_manifest(&manifest, ceilings)?;
    let base = manifest_path
        .parent()
        .ok_or_else(|| std::io::Error::other("manifest has no parent"))?;

    manifest.source = resolve_artifact(&manifest.source, base, &allowed_input_root)?;
    let source_metadata = fs::metadata(&manifest.source.path)?;
    if source_metadata.len() > manifest.limits.max_input_bytes {
        return Err(ReferenceCompileError::InputLimit);
    }
    manifest.ontology_bundle = manifest
        .ontology_bundle
        .iter()
        .map(|artifact| resolve_artifact(artifact, base, &allowed_input_root))
        .collect::<Result<Vec<_>, _>>()?;
    let trusted_reasoner = resolve_trusted_reasoner(trusted_reasoner)?;
    for query in &mut manifest.certified_queries {
        query.query = resolve_artifact(&query.query, base, &allowed_input_root)?;
        query.expected = resolve_artifact(&query.expected, base, &allowed_input_root)?;
    }

    let output_root = resolve_output_root(&manifest.output_directory, base, &allowed_output_root)?;
    let final_directory = output_root.join(manifest.snapshot_id.to_string());
    let staging_directory = output_root.join(format!(".staging-{}", manifest.snapshot_id));
    if final_directory.exists() {
        return Err(ReferenceCompileError::ExistingOutput(final_directory));
    }
    if staging_directory.exists() {
        return Err(ReferenceCompileError::ExistingOutput(staging_directory));
    }
    fs::create_dir(&staging_directory)?;
    compile_into(
        &manifest,
        &trusted_reasoner,
        &manifest_bytes,
        &staging_directory,
    )?;
    sync_tree(&staging_directory)?;
    fs::rename(&staging_directory, &final_directory)?;
    sync_directory(&output_root)?;
    Ok(final_directory.join("snapshot-manifest.json"))
}

fn compile_into(
    manifest: &ReferenceCompileManifest,
    trusted_reasoner: &TrustedReasonerConfig,
    original_manifest_bytes: &[u8],
    stage: &Path,
) -> Result<(), ReferenceCompileError> {
    for directory in [
        "data",
        "reasoner",
        "indexes",
        "contracts",
        "queries",
        "certification",
        "ontology",
    ] {
        fs::create_dir(stage.join(directory))?;
    }
    fs::write(
        stage.join("contracts/compile-request.yaml"),
        original_manifest_bytes,
    )?;
    let projection_bytes = serde_json::to_vec_pretty(&manifest.projection)?;
    fs::write(
        stage.join("contracts/projection-policy.json"),
        &projection_bytes,
    )?;

    let source_identity_sha256 = manifest
        .source_identity_sha256
        .as_deref()
        .unwrap_or(&manifest.source.sha256);
    let source_sha =
        decode_sha256(source_identity_sha256).ok_or(ReferenceCompileError::InvalidSha256)?;
    let facts = match manifest
        .source
        .path
        .extension()
        .and_then(|value| value.to_str())
    {
        Some("nq") | Some("nquads") => parse_nquads(
            &manifest.source.path,
            source_sha,
            manifest.dataset_namespace,
            manifest.source_guid,
            &manifest.source_snapshot,
            &manifest.projection,
            manifest.limits.max_quads,
        )?,
        _ => parse_trig(
            &manifest.source.path,
            source_sha,
            manifest.dataset_namespace,
            manifest.source_guid,
            &manifest.source_snapshot,
            &manifest.projection,
            manifest.limits.max_quads,
        )?,
    };
    validate_source_graph_profile(&facts, &manifest.reasoning.closure_graph_iri)?;
    let datatype_policy_path = stage.join("reasoner/datatype-policy.json");
    let (datatype_policy, datatype_policy_sha256) = write_embedded_policy(&datatype_policy_path)?;
    let datatype_validation =
        validate_reasoning_literals(&facts, &datatype_policy, &datatype_policy_sha256)?;
    fs::write(
        stage.join("reasoner/datatype-validation.json"),
        serde_json::to_vec_pretty(&datatype_validation)?,
    )?;
    let graph_catalog = write_dataset_graph_catalog(stage, manifest, &facts)?;
    let dictionaries = build_dictionaries(
        &facts,
        Some(&graph_catalog),
        manifest.limits.max_dictionary_terms,
    )?;
    let dictionary_bytes = serde_json::to_vec_pretty(&dictionaries.file)?;
    fs::write(stage.join("indexes/dictionaries.json"), &dictionary_bytes)?;

    let query_dataset_path = stage.join("data/query-dataset.nq");
    let core_abox_path = stage.join("reasoner/core-abox.nt");
    write_semantic_exports(&facts, &graph_catalog, &query_dataset_path, &core_abox_path)?;
    let spine_path = stage.join("data/semantic-spine.parquet");
    let semantic_count = write_semantic_spine(
        &spine_path,
        &facts,
        &dictionaries,
        manifest.source_guid,
        manifest.snapshot_id,
        &manifest.projection.policy_id,
        manifest.limits.parquet_row_group_rows,
    )?;
    let payload_path = stage.join("data/payload.parquet");
    let (payload_count, mut locator_records) = write_payload(
        &payload_path,
        &facts,
        &dictionaries,
        manifest.source_guid,
        manifest.snapshot_id,
        manifest.limits.parquet_row_group_rows,
    )?;
    let payload_sha = sha256_file(&payload_path)?;
    let locator_path = stage.join("indexes/locator.bin");
    write_locator(
        &locator_path,
        manifest.snapshot_id,
        payload_sha,
        &mut locator_records,
    )?;
    let graph_capabilities = write_graph_capabilities(stage, manifest, &facts, &graph_catalog)?;

    let copied_ontologies = copy_ontologies(stage, &manifest.ontology_bundle)?;
    let core_hash = sha256_file(&core_abox_path)?;
    let mut reasoner_inputs = copied_ontologies;
    reasoner_inputs.push(ReasonerInputArtifact {
        path: fs::canonicalize(&core_abox_path)?,
        sha256: hex::encode(core_hash),
        ontology_iris: Vec::new(),
    });
    let aggregate_input_sha256 = aggregate_reasoner_hash(&reasoner_inputs)?;
    let closure_path = stage.join("reasoner/closure.nt");
    let report_path = stage.join("reasoner/report.json");
    let owl_signature_path = stage.join("reasoner/owl-signature.json");
    let owl_profile_qualification_path = stage.join("reasoner/owl-profile-qualification.json");
    let owl_consistency_qualification_path =
        stage.join("reasoner/owl-consistency-qualification.json");
    let reasoner_request = ReasonerRequest {
        format_version: 4,
        dataset_id: manifest.dataset_id,
        snapshot_id: manifest.snapshot_id,
        inputs: reasoner_inputs,
        aggregate_input_sha256,
        output_closure_path: fs::canonicalize(stage.join("reasoner"))?.join("closure.nt"),
        output_report_path: fs::canonicalize(stage.join("reasoner"))?.join("report.json"),
        output_owl_signature_path: fs::canonicalize(stage.join("reasoner"))?
            .join("owl-signature.json"),
        output_owl_profile_qualification_path: fs::canonicalize(stage.join("reasoner"))?
            .join("owl-profile-qualification.json"),
        output_owl_consistency_qualification_path: fs::canonicalize(stage.join("reasoner"))?
            .join("owl-consistency-qualification.json"),
        datatype_policy_path: fs::canonicalize(&datatype_policy_path)?,
        datatype_policy_sha256: datatype_policy_sha256.clone(),
        max_named_individuals: manifest.reasoning.max_named_individuals,
        max_properties: manifest.reasoning.max_properties,
    };
    let report = invoke_reasoner(
        trusted_reasoner,
        &reasoner_request,
        &stage.join("reasoner/request.json"),
        &stage.join("reasoner/stdout.log"),
        &stage.join("reasoner/stderr.log"),
        manifest.limits.max_reasoner_seconds,
    )?;
    // Reparse through Oxigraph before any query can rely on adapter output.
    let store = build_store(
        &query_dataset_path,
        &closure_path,
        &manifest.reasoning.closure_graph_iri,
    )?;
    let reasoner_report_sha = hex::encode(sha256_file(&report_path)?);
    let owl_signature_sha256 = hex::encode(sha256_file(&owl_signature_path)?);
    let owl_profile_qualification_sha256 =
        hex::encode(sha256_file(&owl_profile_qualification_path)?);
    let owl_consistency_qualification_sha256 =
        hex::encode(sha256_file(&owl_consistency_qualification_path)?);
    if report.owl_signature_sha256 != owl_signature_sha256
        || report.datatype_policy_sha256 != datatype_policy_sha256
        || report.owl_profile_qualification_sha256 != owl_profile_qualification_sha256
        || report.owl_consistency_qualification_sha256 != owl_consistency_qualification_sha256
    {
        return Err(ReferenceCompileError::Reasoner(
            crate::reasoner::ReasonerInvocationError::RequestMismatch,
        ));
    }
    let certified_queries = certify_queries(
        stage,
        manifest,
        &store,
        &reasoner_report_sha,
        &facts,
        &graph_catalog,
        &graph_capabilities,
        &closure_path,
    )?;
    let coverage_bytes = serde_json::to_vec_pretty(&certified_queries)?;
    fs::write(stage.join("certification/coverage.json"), coverage_bytes)?;

    let verification = serde_json::json!({
        "formatVersion": 1,
        "datasetId": manifest.dataset_id,
        "snapshotId": manifest.snapshot_id,
        "sourceQuadCount": facts.len(),
        "semanticSpineRowCount": semantic_count,
        "payloadRowCount": payload_count,
        "locatorRecordCount": locator_records.len(),
        "owlProfile": report.owl_profile,
        "owlDirectSemantics": report.direct_semantics,
        "owlProfileValid": report.profile_valid,
        "owlProfileViolationCount": report.profile_violation_count,
        "owlSignatureSha256": owl_signature_sha256,
        "datatypePolicySha256": datatype_policy_sha256,
        "owlProfileQualificationSha256": owl_profile_qualification_sha256,
        "owlConsistencyQualificationSha256": owl_consistency_qualification_sha256,
        "datatypePolicyId": datatype_validation.policy_id,
        "datatypeValidatedLiteralCount": datatype_validation.literal_count,
        "datatypeValidationWorkerCount": datatype_validation.worker_count,
        "reasonerConsistencyChecked": report.consistency_checked,
        "reasonerConsistent": report.consistent,
        "reasonerProofDagAvailable": report.proof_dag_available,
        "reasonerMaterializationScope": report.materialization_scope,
        "certifiedQueryCount": certified_queries.len(),
        "routingCertificateCount": certified_queries.iter().filter(|query| query.routing.is_some()).count(),
        "selectiveRouteCount": certified_queries.iter().filter(|query| {
            query.routing.as_ref().is_some_and(|routing| {
                usize::try_from(routing.total_graph_count).ok().is_some_and(|total| {
                    routing.selected_graph_iris.len() < total
                })
            })
        }).count(),
        "distributedQueryPlanCount": certified_queries.iter().filter(|query| {
            query.routing.as_ref().and_then(|routing| routing.distributed.as_ref()).is_some()
        }).count(),
        "status": "reference-snapshot-certified"
    });
    fs::write(
        stage.join("certification/verification.json"),
        serde_json::to_vec_pretty(&verification)?,
    )?;

    let artifacts = collect_artifacts(stage)?;
    let ontology_bundle_sha256 = aggregate_artifact_hash(&manifest.ontology_bundle)?;
    let snapshot = ReferenceSnapshotManifest {
        format_version: 1,
        dataset_id: manifest.dataset_id,
        snapshot_id: manifest.snapshot_id,
        parent_snapshot_id: manifest.parent_snapshot_id,
        dataset_namespace: manifest.dataset_namespace,
        source_sha256: source_identity_sha256.to_owned(),
        ontology_bundle_sha256,
        projection_policy_sha256: crate::sha256_hex(&projection_bytes),
        dictionary_root_sha256: dictionaries.file.root_sha256,
        artifacts,
        certified_queries,
        reasoner_name: report.reasoner_name,
        reasoner_version: report.reasoner_version,
        owl_signature_sha256: Some(owl_signature_sha256),
        datatype_policy_sha256: Some(datatype_policy_sha256),
        owl_profile_qualification_sha256: Some(owl_profile_qualification_sha256),
        owl_consistency_qualification_sha256: Some(owl_consistency_qualification_sha256),
        closure_graph_iri: manifest.reasoning.closure_graph_iri.clone(),
        reasoning_scope: report.materialization_scope,
        publication:
            "atomic-local-build; an external catalog must verify and publish this manifest"
                .to_owned(),
    };
    fs::write(
        stage.join("snapshot-manifest.json"),
        serde_json::to_vec_pretty(&snapshot)?,
    )?;
    Ok(())
}

fn validate_manifest(
    manifest: &ReferenceCompileManifest,
    ceilings: TrustedResourceCeilings,
) -> Result<(), ReferenceCompileError> {
    if manifest.format_version != 1 {
        return Err(ReferenceCompileError::FormatVersion);
    }
    if manifest.ontology_bundle.is_empty()
        || manifest.certified_queries.is_empty()
        || manifest.limits.max_input_bytes == 0
        || manifest.limits.max_quads == 0
        || manifest.limits.max_dictionary_terms == 0
        || manifest.limits.max_reasoner_seconds == 0
        || manifest.limits.parquet_row_group_rows == 0
        || manifest.reasoning.max_named_individuals == 0
        || manifest.reasoning.max_properties == 0
    {
        return Err(ReferenceCompileError::InvalidLimits);
    }
    if manifest.limits.max_input_bytes > ceilings.max_input_bytes
        || manifest.limits.max_quads > ceilings.max_quads
        || manifest.limits.max_dictionary_terms > ceilings.max_dictionary_terms
        || manifest.limits.max_reasoner_seconds > ceilings.max_reasoner_seconds
        || manifest.limits.parquet_row_group_rows > ceilings.max_parquet_row_group_rows
        || manifest.reasoning.max_named_individuals > ceilings.max_named_individuals
        || manifest.reasoning.max_properties > ceilings.max_properties
    {
        return Err(ReferenceCompileError::InvalidLimits);
    }
    NamedNode::new(manifest.reasoning.closure_graph_iri.clone())
        .map_err(|_| ReferenceCompileError::InvalidClosureGraph)?;
    if manifest
        .graph_catalog
        .iter()
        .any(|graph| graph.graph_iri == manifest.reasoning.closure_graph_iri)
    {
        return Err(ReferenceCompileError::ClosureGraphCollision);
    }
    let mut ids = BTreeSet::new();
    let mut hashes = BTreeSet::new();
    for query in &manifest.certified_queries {
        if !ids.insert(query.query_id.clone()) {
            return Err(ReferenceCompileError::DuplicateQuery(
                query.query_id.clone(),
            ));
        }
        if !hashes.insert(query.query.sha256.clone()) {
            return Err(ReferenceCompileError::DuplicateQuery(
                query.query.sha256.clone(),
            ));
        }
    }
    Ok(())
}

fn validate_source_graph_profile(
    facts: &[NormalizedFact],
    closure_graph_iri: &str,
) -> Result<(), ReferenceCompileError> {
    if facts
        .iter()
        .any(|fact| fact.graph_scope == GraphScope::Named && fact.graph_iri == closure_graph_iri)
    {
        return Err(ReferenceCompileError::ClosureGraphCollision);
    }
    Ok(())
}

fn write_semantic_exports(
    facts: &[NormalizedFact],
    graph_catalog: &GraphCatalog,
    query_dataset_path: &Path,
    core_abox_path: &Path,
) -> Result<(), std::io::Error> {
    let named_records = graph_catalog
        .graphs
        .iter()
        .filter_map(|record| match &record.name {
            LogicalGraphName::Named { iri } => Some((iri.as_str(), record)),
            LogicalGraphName::Default => None,
        })
        .collect::<BTreeMap<_, _>>();
    let mut query_dataset = File::create(query_dataset_path)?;
    let mut core_abox = File::create(core_abox_path)?;
    for fact in facts {
        let record = match fact.graph_scope {
            GraphScope::Default => graph_catalog.graphs.first(),
            GraphScope::Named => named_records.get(fact.graph_iri.as_str()).copied(),
        }
        .ok_or_else(|| std::io::Error::other("normalized fact is absent from graph catalog"))?;
        if record.query_visible && (fact.queryable_as_rdf || fact.treatment == Treatment::Core) {
            query_dataset.write_all(nquad_line(fact).as_bytes())?;
        }
        if record.reasoning_visible
            && fact.treatment == Treatment::Core
            && fact.participates_in_reasoning
        {
            core_abox.write_all(ntriple_line(fact).as_bytes())?;
        }
    }
    query_dataset.sync_all()?;
    core_abox.sync_all()?;
    Ok(())
}

fn copy_ontologies(
    stage: &Path,
    inputs: &[InputArtifact],
) -> Result<Vec<ReasonerInputArtifact>, ReferenceCompileError> {
    let mut copied = Vec::with_capacity(inputs.len());
    let mut all_ontology_iris = BTreeMap::new();
    let mut all_imports = BTreeSet::new();
    for (index, input) in inputs.iter().enumerate() {
        let (ontology_iris, imports) = scan_ontology_document(&input.path)?;
        if ontology_iris.is_empty() {
            return Err(ReferenceCompileError::MissingOntologyIri(
                input.path.clone(),
            ));
        }
        for ontology_iri in &ontology_iris {
            if all_ontology_iris
                .insert(ontology_iri.clone(), input.path.clone())
                .is_some()
            {
                return Err(ReferenceCompileError::DuplicateOntologyIri(
                    ontology_iri.clone(),
                ));
            }
        }
        all_imports.extend(imports);
        let extension = input
            .path
            .extension()
            .and_then(|value| value.to_str())
            .unwrap_or("owl");
        let destination = stage.join(format!("ontology/{index:04}.{extension}"));
        fs::copy(&input.path, &destination)?;
        let observed = hex::encode(sha256_file(&destination)?);
        if observed != input.sha256 {
            return Err(ReferenceCompileError::ChecksumMismatch {
                path: input.path.clone(),
                expected: input.sha256.clone(),
                observed,
            });
        }
        copied.push(ReasonerInputArtifact {
            path: fs::canonicalize(destination)?,
            sha256: input.sha256.clone(),
            ontology_iris: ontology_iris.into_iter().collect(),
        });
    }
    if let Some(unresolved) = all_imports
        .iter()
        .find(|imported| !all_ontology_iris.contains_key(*imported))
    {
        return Err(ReferenceCompileError::UnresolvedImport(
            (*unresolved).clone(),
        ));
    }
    Ok(copied)
}

fn scan_ontology_document(
    path: &Path,
) -> Result<(BTreeSet<String>, BTreeSet<String>), ReferenceCompileError> {
    let extension = path
        .extension()
        .and_then(|value| value.to_str())
        .unwrap_or_default()
        .to_ascii_lowercase();
    let format = match extension.as_str() {
        "ttl" => RdfFormat::Turtle,
        "nt" => RdfFormat::NTriples,
        "rdf" | "xml" => RdfFormat::RdfXml,
        _ => {
            return Err(ReferenceCompileError::UnsupportedOntologyFormat(
                path.to_path_buf(),
            ));
        }
    };
    let input = std::io::BufReader::new(File::open(path)?);
    let mut ontology_subjects = BTreeSet::new();
    let mut version_pairs = Vec::new();
    let mut import_pairs = Vec::new();
    for parsed in RdfParser::from_format(format).for_reader(input) {
        let quad =
            parsed.map_err(|error| ReferenceCompileError::OntologyParse(error.to_string()))?;
        let subject = match &quad.subject {
            oxigraph::model::NamedOrBlankNode::NamedNode(node) => Some(node.as_str().to_owned()),
            oxigraph::model::NamedOrBlankNode::BlankNode(_) => None,
        };
        if quad.predicate.as_str() == "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
            && matches!(&quad.object, Term::NamedNode(node) if node.as_str() == "http://www.w3.org/2002/07/owl#Ontology")
        {
            if let Some(subject) = subject {
                ontology_subjects.insert(subject);
            }
        } else if quad.predicate.as_str() == "http://www.w3.org/2002/07/owl#versionIRI" {
            match (subject, &quad.object) {
                (Some(subject), Term::NamedNode(version)) => {
                    version_pairs.push((subject, version.as_str().to_owned()))
                }
                _ => {
                    return Err(ReferenceCompileError::MisplacedOntologyHeader(
                        path.to_path_buf(),
                    ));
                }
            }
        } else if quad.predicate.as_str() == "http://www.w3.org/2002/07/owl#imports" {
            match (subject, &quad.object) {
                (Some(subject), Term::NamedNode(imported)) => {
                    import_pairs.push((subject, imported.as_str().to_owned()))
                }
                _ => {
                    return Err(ReferenceCompileError::MisplacedOntologyHeader(
                        path.to_path_buf(),
                    ));
                }
            }
        }
    }
    if ontology_subjects.is_empty() {
        return Err(ReferenceCompileError::MissingOntologyIri(
            path.to_path_buf(),
        ));
    }
    if ontology_subjects.len() != 1 {
        return Err(ReferenceCompileError::MultipleOntologyIris(
            path.to_path_buf(),
        ));
    }
    let Some(ontology_iri) = ontology_subjects.iter().next().cloned() else {
        return Err(ReferenceCompileError::MissingOntologyIri(
            path.to_path_buf(),
        ));
    };
    if version_pairs
        .iter()
        .any(|(subject, _)| subject != &ontology_iri)
        || import_pairs
            .iter()
            .any(|(subject, _)| subject != &ontology_iri)
    {
        return Err(ReferenceCompileError::MisplacedOntologyHeader(
            path.to_path_buf(),
        ));
    }
    let versions = version_pairs
        .into_iter()
        .map(|(_, version)| version)
        .collect::<BTreeSet<_>>();
    if versions.len() > 1 {
        return Err(ReferenceCompileError::MultipleVersionIris(
            path.to_path_buf(),
        ));
    }
    let mut aliases = BTreeSet::from([ontology_iri]);
    aliases.extend(versions);
    let imports = import_pairs
        .into_iter()
        .map(|(_, imported)| imported)
        .collect::<BTreeSet<_>>();
    Ok((aliases, imports))
}

fn query_execution_limits(
    manifest: &ReferenceCompileManifest,
) -> Result<QueryExecutionLimits, ReferenceCompileError> {
    let max_solution_rows = usize::try_from(manifest.limits.max_quads)
        .map_err(|_| ReferenceCompileError::InvalidLimits)?;
    let max_graph_triples = max_solution_rows;
    let max_graph_blank_nodes = usize::try_from(
        manifest
            .limits
            .max_dictionary_terms
            .min(manifest.limits.max_quads),
    )
    .map_err(|_| ReferenceCompileError::InvalidLimits)?;
    QueryExecutionLimits {
        max_solution_rows,
        max_graph_triples,
        max_graph_blank_nodes,
    }
    .validate()
    .map_err(ReferenceCompileError::Query)
}

fn certify_queries(
    stage: &Path,
    manifest: &ReferenceCompileManifest,
    store: &oxigraph::store::Store,
    reasoner_report_sha256: &str,
    facts: &[NormalizedFact],
    graph_catalog: &GraphCatalog,
    capabilities: &GraphCapabilityIndexFile,
    closure_path: &Path,
) -> Result<Vec<CertifiedQueryRecord>, ReferenceCompileError> {
    let capability_path = stage.join("indexes/graph-capabilities.json");
    let capability_index_sha256 = hex::encode(sha256_file(&capability_path)?);
    let limits = query_execution_limits(manifest)?;
    let mut records = Vec::with_capacity(manifest.certified_queries.len());
    for query in &manifest.certified_queries {
        let query_text = query_file(&query.query.path)?;
        let compiled =
            CompiledSparqlQuery::parse(&query_text).map_err(ReferenceQueryError::from)?;
        compiled
            .require_certifiable()
            .map_err(ReferenceQueryError::from)?;
        let ordered = compiled.solution_order_is_significant();
        if query.ordered != ordered {
            return Err(ReferenceCompileError::Query(
                ReferenceQueryError::InvalidExpected(format!(
                    "query {} ordered flag differs from the top-level SPARQL ORDER BY semantics",
                    query.query_id
                )),
            ));
        }
        let expected_bytes = fs::read(&query.expected.path)?;
        let expected = parse_expected(
            &query.expected.path,
            &expected_bytes,
            compiled.form(),
            limits,
        )?;
        let query_dataset = compiled.dataset_specification().clone();
        let certification_labels = graph_catalog
            .graphs
            .iter()
            .filter(|graph| graph.query_visible)
            .flat_map(|graph| graph.authorization_labels.iter().cloned())
            .collect::<BTreeSet<_>>();
        let resolved_dataset = resolve_dataset(
            graph_catalog,
            &certification_labels,
            &query_dataset,
            &ProtocolDatasetSpecification::default(),
        )
        .map_err(|error| {
            ReferenceCompileError::Query(ReferenceQueryError::Dataset(error.to_string()))
        })?;
        let include_internal_closure = !query_dataset.specified;
        let observed = execute_compiled_query_with_dataset(
            store,
            &compiled,
            &resolved_dataset,
            graph_catalog,
            include_internal_closure,
            limits,
        )?;
        let observed_result_sha256 = verify_expected(&observed, &expected, ordered, limits)?;
        verify_source_links(store, observed.entity_iris(), &query.required_source_iris)?;
        let observed_multiset_sha256 = match &observed {
            ExecutedQueryResult::Solutions(value) => Some(canonical_sparql_multiset_sha256(
                &value.head,
                &value.bindings,
                ordered,
            )?),
            ExecutedQueryResult::Boolean(_) | ExecutedQueryResult::Graph { .. } => None,
        };
        let mut routing = certify_routed_query(
            stage,
            manifest,
            query,
            &compiled,
            &expected,
            facts,
            graph_catalog,
            &resolved_dataset,
            include_internal_closure,
            capabilities,
            closure_path,
            &capability_index_sha256,
            limits,
        )?;
        routing.distributed = match (&observed, &expected, observed_multiset_sha256.as_deref()) {
            (
                ExecutedQueryResult::Solutions(observed_select),
                ExpectedQueryResult::Solutions(expected_select),
                Some(full_multiset_sha256),
            ) => certify_distributed_query(
                stage,
                manifest,
                query,
                &compiled,
                expected_select,
                &observed_select.head,
                facts,
                closure_path,
                &routing.selected_graph_iris,
                full_multiset_sha256,
            )?,
            _ => None,
        };
        let query_copy = stage.join(format!("queries/{}.rq", safe_name(&query.query_id)));
        let expected_extension = query
            .expected
            .path
            .extension()
            .and_then(|value| value.to_str())
            .filter(|value| !value.is_empty())
            .unwrap_or("result");
        let expected_copy = stage.join(format!(
            "queries/{}.expected.{expected_extension}",
            safe_name(&query.query_id)
        ));
        fs::write(&query_copy, query_text.as_bytes())?;
        fs::write(&expected_copy, &expected_bytes)?;
        write_observed_result(stage, &query.query_id, &observed, &observed_result_sha256)?;
        fs::write(
            stage.join(format!(
                "certification/{}.algebra.sse",
                safe_name(&query.query_id)
            )),
            compiled.canonical_sse().as_bytes(),
        )?;
        records.push(CertifiedQueryRecord {
            query_id: query.query_id.clone(),
            query_sha256: query.query.sha256.clone(),
            expected_sha256: query.expected.sha256.clone(),
            sparql_algebra_format_version: SPARQL_ALGEBRA_FORMAT_VERSION,
            sparql_algebra_sha256: compiled.canonical_sse_sha256().to_owned(),
            query_form: compiled.form(),
            result_hash_version: QUERY_RESULT_HASH_VERSION,
            ordered,
            max_solution_rows: u64::try_from(limits.max_solution_rows)
                .map_err(|_| ReferenceCompileError::InvalidLimits)?,
            max_graph_triples: u64::try_from(limits.max_graph_triples)
                .map_err(|_| ReferenceCompileError::InvalidLimits)?,
            max_graph_blank_nodes: u64::try_from(limits.max_graph_blank_nodes)
                .map_err(|_| ReferenceCompileError::InvalidLimits)?,
            observed_result_sha256,
            observed_multiset_sha256,
            reasoner_report_sha256: reasoner_report_sha256.to_owned(),
            scope: CERTIFIED_QUERY_SCOPE.to_owned(),
            routing: Some(routing),
        });
    }
    records.sort_unstable_by(|left, right| left.query_id.cmp(&right.query_id));
    Ok(records)
}

fn write_observed_result(
    stage: &Path,
    query_id: &str,
    observed: &ExecutedQueryResult,
    observed_result_sha256: &str,
) -> Result<(), ReferenceCompileError> {
    let safe = safe_name(query_id);
    let audit = match observed {
        ExecutedQueryResult::Solutions(value) => serde_json::json!({
            "queryForm": QueryForm::Select,
            "resultSha256": observed_result_sha256,
            "head": value.head,
            "bindings": value.bindings,
        }),
        ExecutedQueryResult::Boolean(value) => serde_json::json!({
            "queryForm": QueryForm::Ask,
            "resultSha256": observed_result_sha256,
            "boolean": value,
        }),
        ExecutedQueryResult::Graph { form, graph } => {
            fs::write(
                stage.join(format!("certification/{safe}.observed.nt")),
                graph.ntriples.concat(),
            )?;
            serde_json::json!({
                "queryForm": form,
                "resultSha256": observed_result_sha256,
                "tripleCount": graph.ntriples.len(),
                "canonicalNTriplesArtifact": format!("certification/{safe}.observed.nt"),
            })
        }
    };
    fs::write(
        stage.join(format!("certification/{safe}.observed.json")),
        serde_json::to_vec_pretty(&audit)?,
    )?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn certify_routed_query(
    stage: &Path,
    manifest: &ReferenceCompileManifest,
    query: &CertifiedQueryInput,
    compiled: &CompiledSparqlQuery,
    expected: &ExpectedQueryResult,
    facts: &[NormalizedFact],
    graph_catalog: &GraphCatalog,
    resolved_dataset: &ResolvedDataset,
    include_internal_closure: bool,
    capabilities: &GraphCapabilityIndexFile,
    closure_path: &Path,
    capability_index_sha256: &str,
    limits: QueryExecutionLimits,
) -> Result<QueryRoutingCertificate, ReferenceCompileError> {
    let catalog_graphs = capabilities
        .graphs
        .iter()
        .map(|graph| graph.graph_iri.clone())
        .collect::<BTreeSet<_>>();
    let active_default_graphs =
        graph_iris_for_ids(graph_catalog, &resolved_dataset.default_graph_ids)?
            .into_iter()
            .collect::<BTreeSet<_>>();
    let active_named_graphs = graph_iris_for_ids(graph_catalog, &resolved_dataset.named_graph_ids)?
        .into_iter()
        .collect::<BTreeSet<_>>();
    let active_graphs = active_default_graphs
        .union(&active_named_graphs)
        .cloned()
        .collect::<BTreeSet<_>>();
    let route = compiled.route_analysis();
    let mut selected = route
        .declared_graph_iris
        .intersection(&active_named_graphs)
        .cloned()
        .collect::<BTreeSet<_>>();
    if route.has_graph_variable {
        selected.extend(active_named_graphs.iter().cloned());
    }
    let mut selection_mode = if selected.is_empty() {
        ROUTE_MODE_TYPED_ACTIVE_DATASET.to_owned()
    } else {
        ROUTE_MODE_TYPED_DECLARED_GRAPH.to_owned()
    };
    if route.has_default_graph_pattern {
        let mut candidates = BTreeSet::new();
        if !route.has_property_path {
            for iri in &route.semantic_iris {
                if let Some(graphs) = capabilities.predicate_to_graphs.get(iri) {
                    candidates.extend(graphs.iter().cloned());
                }
                if let Some(graphs) = capabilities.class_to_graphs.get(iri) {
                    candidates.extend(graphs.iter().cloned());
                }
            }
            candidates = candidates
                .intersection(&active_default_graphs)
                .cloned()
                .collect();
            if !candidates.is_empty() {
                expand_dependencies(&mut candidates, &capabilities.dependencies)?;
                candidates = candidates.intersection(&active_graphs).cloned().collect();
            }
        }
        if candidates.is_empty() {
            selected.extend(active_default_graphs.iter().cloned());
            selection_mode = if route.has_property_path {
                ROUTE_MODE_TYPED_PROPERTY_PATH_FULL_ACTIVE_DEFAULT.to_owned()
            } else {
                ROUTE_MODE_TYPED_ACTIVE_DEFAULT_NO_CAPABILITY.to_owned()
            };
        } else {
            selected.extend(candidates);
            selection_mode = ROUTE_MODE_TYPED_CAPABILITY_DEPENDENCY.to_owned();
        }
    }
    let route_relative_path = format!("data/routes/{}.nq", query.query.sha256);
    let route_path = stage.join(&route_relative_path);
    fs::create_dir_all(
        route_path
            .parent()
            .ok_or_else(|| std::io::Error::other("route artifact has no parent"))?,
    )?;
    write_routed_dataset(&route_path, facts, &selected)?;
    let (routed_result_sha256, routed_multiset_sha256) = match validate_routed_query(
        &route_path,
        closure_path,
        &manifest.reasoning.closure_graph_iri,
        compiled,
        expected,
        query,
        graph_catalog,
        resolved_dataset,
        include_internal_closure,
        limits,
    ) {
        Ok(hash) => hash,
        Err(_) if selected != active_graphs => {
            selected = active_graphs.clone();
            selection_mode = ROUTE_MODE_TYPED_ACTIVE_DATASET_FALLBACK.to_owned();
            write_routed_dataset(&route_path, facts, &selected)?;
            validate_routed_query(
                &route_path,
                closure_path,
                &manifest.reasoning.closure_graph_iri,
                compiled,
                expected,
                query,
                graph_catalog,
                resolved_dataset,
                include_internal_closure,
                limits,
            )?
        }
        Err(error) => return Err(ReferenceCompileError::Query(error)),
    };
    let route_artifact_sha256 = hex::encode(sha256_file(&route_path)?);
    let route_artifact_bytes = fs::metadata(&route_path)?.len();
    Ok(QueryRoutingCertificate {
        format_version: 1,
        capability_index_sha256: capability_index_sha256.to_owned(),
        selected_graph_iris: selected.into_iter().collect(),
        total_graph_count: u32::try_from(catalog_graphs.len())
            .map_err(|_| ReferenceCompileError::InvalidLimits)?,
        selection_mode,
        dataset_selection_source: resolved_dataset.selection_source,
        default_graph_iris: graph_iris_for_ids(graph_catalog, &resolved_dataset.default_graph_ids)?,
        named_graph_iris: graph_iris_for_ids(graph_catalog, &resolved_dataset.named_graph_ids)?,
        active_dataset_sha256: resolved_dataset.active_dataset_sha256.clone(),
        include_internal_closure,
        route_artifact_relative_path: route_relative_path,
        route_artifact_sha256,
        route_artifact_bytes,
        routed_result_sha256,
        routed_multiset_sha256,
        distributed: None,
    })
}

#[allow(clippy::too_many_arguments)]
fn certify_distributed_query(
    stage: &Path,
    manifest: &ReferenceCompileManifest,
    query: &CertifiedQueryInput,
    compiled: &CompiledSparqlQuery,
    expected: &ExpectedSolutions,
    final_head: &[String],
    facts: &[NormalizedFact],
    closure_path: &Path,
    selected_graph_iris: &[String],
    full_multiset_sha256: &str,
) -> Result<Option<DistributedQueryCertificate>, ReferenceCompileError> {
    if query.ordered {
        return Ok(None);
    }
    let Some(graph_blocks) = compiled.distributed_graph_fragments() else {
        return Ok(None);
    };
    let selected = selected_graph_iris.iter().cloned().collect::<BTreeSet<_>>();
    if graph_blocks
        .iter()
        .any(|block| !selected.contains(&block.graph_iri))
    {
        return Ok(None);
    }
    let candidate = stage.join(format!(".distributed-{}", query.query.sha256));
    fs::create_dir_all(candidate.join("data"))?;
    fs::create_dir_all(candidate.join("queries"))?;
    let mut fragments = Vec::with_capacity(graph_blocks.len());
    let mut fragment_bindings = Vec::with_capacity(graph_blocks.len());
    for (index, block) in graph_blocks.iter().enumerate() {
        let fragment_id = format!("fragment-{index:04}");
        let data_name = format!("{fragment_id}.nq");
        let query_name = format!("{fragment_id}.rq");
        let candidate_data = candidate.join("data").join(&data_name);
        let candidate_query = candidate.join("queries").join(&query_name);
        write_routed_dataset(
            &candidate_data,
            facts,
            &BTreeSet::from([block.graph_iri.clone()]),
        )?;
        let fragment_query = block.query_text.clone();
        fs::write(&candidate_query, fragment_query.as_bytes())?;
        let store = build_store(
            &candidate_data,
            closure_path,
            &manifest.reasoning.closure_graph_iri,
        )?;
        let observed = execute_select(&store, &fragment_query)?;
        let observed_multiset_sha256 =
            canonical_sparql_multiset_sha256(&observed.head, &observed.bindings, false)?;
        let data_relative = format!("data/distributed/{}/{data_name}", query.query.sha256);
        let query_relative = format!("queries/distributed/{}/{query_name}", query.query.sha256);
        fragments.push(DistributedQueryFragment {
            fragment_id: fragment_id.clone(),
            graph_iri: block.graph_iri.clone(),
            dataset_artifact_relative_path: data_relative,
            dataset_artifact_sha256: hex::encode(sha256_file(&candidate_data)?),
            dataset_artifact_bytes: fs::metadata(&candidate_data)?.len(),
            query_artifact_relative_path: query_relative,
            query_artifact_sha256: hex::encode(sha256_file(&candidate_query)?),
            query_artifact_bytes: fs::metadata(&candidate_query)?.len(),
            head: observed.head,
            row_count: u64::try_from(observed.bindings.len())
                .map_err(|_| ReferenceCompileError::InvalidLimits)?,
            observed_multiset_sha256,
        });
        fragment_bindings.push(observed.bindings);
    }
    let max_rows = usize::try_from(manifest.limits.max_quads).unwrap_or(usize::MAX);
    if fragment_bindings
        .first()
        .is_some_and(|bindings| bindings.len() > max_rows)
    {
        fs::remove_dir_all(&candidate)?;
        return Ok(None);
    }
    let mut fragment_bindings = fragment_bindings.into_iter();
    let Some(mut joined) = fragment_bindings.next() else {
        fs::remove_dir_all(&candidate)?;
        return Ok(None);
    };
    for bindings in fragment_bindings {
        match inner_join_sparql_json(&joined, &bindings, max_rows) {
            Ok(rows) => joined = rows,
            Err(_) => {
                fs::remove_dir_all(&candidate)?;
                return Ok(None);
            }
        }
    }
    let projected = match project_sparql_json(&joined, final_head) {
        Ok(rows) => rows,
        Err(_) => {
            fs::remove_dir_all(&candidate)?;
            return Ok(None);
        }
    };
    let distributed_multiset_sha256 =
        match verify_binding_values(final_head, &projected, expected, query.ordered) {
            Ok(hash) if hash == full_multiset_sha256 => {
                canonical_sparql_multiset_sha256(final_head, &projected, query.ordered)?
            }
            Ok(_) | Err(_) => {
                fs::remove_dir_all(&candidate)?;
                return Ok(None);
            }
        };
    let data_destination = stage.join("data/distributed").join(&query.query.sha256);
    let query_destination = stage.join("queries/distributed").join(&query.query.sha256);
    fs::create_dir_all(
        data_destination
            .parent()
            .ok_or_else(|| std::io::Error::other("distributed data has no parent"))?,
    )?;
    fs::create_dir_all(
        query_destination
            .parent()
            .ok_or_else(|| std::io::Error::other("distributed query has no parent"))?,
    )?;
    fs::rename(candidate.join("data"), &data_destination)?;
    fs::rename(candidate.join("queries"), &query_destination)?;
    fs::remove_dir(&candidate)?;
    let plan = DistributedQueryPlanFile {
        format_version: 1,
        dataset_id: manifest.dataset_id,
        snapshot_id: manifest.snapshot_id,
        query_sha256: query.query.sha256.clone(),
        ordered: query.ordered,
        final_head: final_head.to_vec(),
        join_order: fragments
            .iter()
            .map(|fragment| fragment.fragment_id.clone())
            .collect(),
        fragments,
    };
    let plan_relative = format!("plans/distributed/{}.json", query.query.sha256);
    let plan_path = stage.join(&plan_relative);
    fs::create_dir_all(
        plan_path
            .parent()
            .ok_or_else(|| std::io::Error::other("distributed plan has no parent"))?,
    )?;
    fs::write(&plan_path, serde_json::to_vec_pretty(&plan)?)?;
    Ok(Some(DistributedQueryCertificate {
        format_version: 1,
        plan_artifact_relative_path: plan_relative,
        plan_artifact_sha256: hex::encode(sha256_file(&plan_path)?),
        plan_artifact_bytes: fs::metadata(&plan_path)?.len(),
        fragment_count: u32::try_from(plan.fragments.len())
            .map_err(|_| ReferenceCompileError::InvalidLimits)?,
        distributed_multiset_sha256,
    }))
}

fn validate_routed_query(
    route_path: &Path,
    closure_path: &Path,
    closure_graph_iri: &str,
    compiled: &CompiledSparqlQuery,
    expected: &ExpectedQueryResult,
    query: &CertifiedQueryInput,
    graph_catalog: &GraphCatalog,
    resolved_dataset: &ResolvedDataset,
    include_internal_closure: bool,
    limits: QueryExecutionLimits,
) -> Result<(String, Option<String>), crate::query::ReferenceQueryError> {
    let store = build_store(route_path, closure_path, closure_graph_iri)?;
    let observed = execute_compiled_query_with_dataset(
        &store,
        compiled,
        resolved_dataset,
        graph_catalog,
        include_internal_closure,
        limits,
    )?;
    let result_sha256 = verify_expected(&observed, expected, query.ordered, limits)?;
    verify_source_links(&store, observed.entity_iris(), &query.required_source_iris)?;
    let multiset_sha256 = match observed {
        ExecutedQueryResult::Solutions(value) => Some(canonical_sparql_multiset_sha256(
            &value.head,
            &value.bindings,
            query.ordered,
        )?),
        ExecutedQueryResult::Boolean(_) | ExecutedQueryResult::Graph { .. } => None,
    };
    Ok((result_sha256, multiset_sha256))
}

fn graph_iris_for_ids(
    catalog: &GraphCatalog,
    graph_ids: &[u32],
) -> Result<Vec<String>, ReferenceCompileError> {
    graph_ids
        .iter()
        .map(|graph_id| {
            let graph = catalog
                .by_id(*graph_id)
                .ok_or(ReferenceCompileError::InvalidLimits)?;
            let LogicalGraphName::Named { iri } = &graph.name else {
                return Err(ReferenceCompileError::InvalidLimits);
            };
            Ok(iri.clone())
        })
        .collect()
}

fn write_routed_dataset(
    path: &Path,
    facts: &[NormalizedFact],
    selected: &BTreeSet<String>,
) -> Result<(), std::io::Error> {
    let mut output = File::create(path)?;
    for fact in facts.iter().filter(|fact| {
        selected.contains(&fact.graph_iri)
            && (fact.queryable_as_rdf || fact.treatment == Treatment::Core)
    }) {
        output.write_all(nquad_line(fact).as_bytes())?;
    }
    output.sync_all()
}

fn expand_dependencies(
    selected: &mut BTreeSet<String>,
    dependencies: &BTreeMap<String, Vec<String>>,
) -> Result<(), ReferenceCompileError> {
    let mut queue = selected
        .iter()
        .cloned()
        .collect::<std::collections::VecDeque<_>>();
    while let Some(graph) = queue.pop_front() {
        if let Some(required) = dependencies.get(&graph) {
            for dependency in required {
                if !dependencies.contains_key(dependency) {
                    return Err(ReferenceCompileError::InvalidLimits);
                }
                if selected.insert(dependency.clone()) {
                    queue.push_back(dependency.clone());
                }
            }
        }
    }
    Ok(())
}

fn string_set_map(values: BTreeMap<String, BTreeSet<String>>) -> BTreeMap<String, Vec<String>> {
    values
        .into_iter()
        .map(|(key, entries)| (key, entries.into_iter().collect()))
        .collect()
}

fn write_graph_capabilities(
    stage: &Path,
    manifest: &ReferenceCompileManifest,
    facts: &[NormalizedFact],
    graph_catalog: &GraphCatalog,
) -> Result<GraphCapabilityIndexFile, ReferenceCompileError> {
    let mut counts = BTreeMap::<String, u64>::new();
    let mut predicates = BTreeMap::<String, BTreeSet<String>>::new();
    let mut classes = BTreeMap::<String, BTreeSet<String>>::new();
    let mut entity_graphs = BTreeMap::<String, BTreeSet<String>>::new();
    let query_visible_graphs = graph_catalog
        .graphs
        .iter()
        .filter_map(|record| match &record.name {
            LogicalGraphName::Named { iri } if record.query_visible => Some(iri.as_str()),
            LogicalGraphName::Default | LogicalGraphName::Named { .. } => None,
        })
        .collect::<BTreeSet<_>>();
    for fact in facts.iter().filter(|fact| {
        is_routable_named_fact(fact) && query_visible_graphs.contains(fact.graph_iri.as_str())
    }) {
        let count = counts.entry(fact.graph_iri.clone()).or_default();
        *count = count
            .checked_add(1)
            .ok_or(ReferenceCompileError::InvalidLimits)?;
        predicates
            .entry(fact.predicate_iri.clone())
            .or_default()
            .insert(fact.graph_iri.clone());
        entity_graphs
            .entry(fact.subject_iri.clone())
            .or_default()
            .insert(fact.graph_iri.clone());
        if let NormalizedObject::Entity { iri, .. } = &fact.object {
            entity_graphs
                .entry(iri.clone())
                .or_default()
                .insert(fact.graph_iri.clone());
            if fact.predicate_iri == "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" {
                classes
                    .entry(iri.clone())
                    .or_default()
                    .insert(fact.graph_iri.clone());
            }
        }
    }
    let graphs = graph_catalog
        .graphs
        .iter()
        .filter_map(|record| match &record.name {
            LogicalGraphName::Named { iri } if record.query_visible => Some((record, iri)),
            LogicalGraphName::Default | LogicalGraphName::Named { .. } => None,
        })
        .map(|(record, graph_iri)| {
            Ok(GraphCapabilityRecord {
                graph_id: record.graph_id,
                graph_iri: graph_iri.clone(),
                role: record.role.clone(),
                authorization_labels: record.authorization_labels.clone(),
                reasoning_visible: record.reasoning_visible,
                queryable_fact_count: counts.get(graph_iri).copied().unwrap_or(0),
            })
        })
        .collect::<Result<Vec<_>, ReferenceCompileError>>()?;
    let mut dependencies = graphs
        .iter()
        .map(|graph| (graph.graph_iri.clone(), BTreeSet::new()))
        .collect::<BTreeMap<_, _>>();
    for related in entity_graphs.values().filter(|graphs| graphs.len() > 1) {
        for source in related {
            dependencies
                .entry(source.clone())
                .or_default()
                .extend(related.iter().filter(|target| *target != source).cloned());
        }
    }
    let index = GraphCapabilityIndexFile {
        format_version: 2,
        dataset_id: manifest.dataset_id,
        snapshot_id: manifest.snapshot_id,
        graph_catalog_sha256: hex::encode(sha256_file(
            &stage.join("indexes/rdf-dataset-catalog.json"),
        )?),
        graphs,
        predicate_to_graphs: string_set_map(predicates),
        class_to_graphs: string_set_map(classes),
        dependencies: string_set_map(dependencies),
    };
    fs::write(
        stage.join("indexes/graph-capabilities.json"),
        serde_json::to_vec_pretty(&index)?,
    )?;
    Ok(index)
}

fn write_dataset_graph_catalog(
    stage: &Path,
    manifest: &ReferenceCompileManifest,
    facts: &[NormalizedFact],
) -> Result<GraphCatalog, ReferenceCompileError> {
    let mut default_quad_count = 0_u64;
    let mut named_quad_counts = BTreeMap::<String, u64>::new();
    for fact in facts {
        match fact.graph_scope {
            GraphScope::Default => {
                default_quad_count = default_quad_count
                    .checked_add(1)
                    .ok_or(ReferenceCompileError::InvalidLimits)?;
            }
            GraphScope::Named => {
                let count = named_quad_counts.entry(fact.graph_iri.clone()).or_default();
                *count = count
                    .checked_add(1)
                    .ok_or(ReferenceCompileError::InvalidLimits)?;
            }
        }
    }
    let catalog = compile_catalog(
        manifest.dataset_id,
        manifest.snapshot_id,
        default_quad_count,
        &named_quad_counts,
        &manifest.graph_catalog,
    )
    .map_err(|error| {
        ReferenceCompileError::Rdf(crate::rdf::RdfCompileError::GraphCatalog(error.to_string()))
    })?;
    fs::write(
        stage.join("indexes/rdf-dataset-catalog.json"),
        serde_json::to_vec_pretty(&catalog)?,
    )?;
    Ok(catalog)
}

fn is_routable_named_fact(fact: &NormalizedFact) -> bool {
    fact.graph_scope == GraphScope::Named
        && (fact.queryable_as_rdf || fact.treatment == Treatment::Core)
}

fn collect_artifacts(stage: &Path) -> Result<Vec<ArtifactRecord>, ReferenceCompileError> {
    let mut paths = Vec::new();
    collect_files(stage, stage, &mut paths)?;
    paths.retain(|path| path != "snapshot-manifest.json");
    paths.sort_unstable();
    paths
        .into_iter()
        .map(|relative| artifact_record(stage, &relative).map_err(ReferenceCompileError::Io))
        .collect()
}

fn collect_files(
    root: &Path,
    current: &Path,
    output: &mut Vec<String>,
) -> Result<(), std::io::Error> {
    let mut entries = fs::read_dir(current)?.collect::<Result<Vec<_>, _>>()?;
    entries.sort_unstable_by_key(|entry| entry.file_name());
    for entry in entries {
        let path = entry.path();
        if path.is_dir() {
            collect_files(root, &path, output)?;
        } else if path.is_file() {
            let relative = path
                .strip_prefix(root)
                .map_err(|error| std::io::Error::other(error.to_string()))?
                .to_string_lossy()
                .replace('\\', "/");
            output.push(relative);
        }
    }
    Ok(())
}

fn resolve_artifact(
    artifact: &InputArtifact,
    base: &Path,
    allowed_root: &Path,
) -> Result<InputArtifact, ReferenceCompileError> {
    if decode_sha256(&artifact.sha256).is_none()
        || artifact.sha256.to_lowercase() != artifact.sha256
    {
        return Err(ReferenceCompileError::InvalidSha256);
    }
    let path = resolve_existing_path(&artifact.path, base)?;
    require_under(&path, allowed_root, true)?;
    let observed = hex::encode(sha256_file(&path)?);
    if observed != artifact.sha256 {
        return Err(ReferenceCompileError::ChecksumMismatch {
            path,
            expected: artifact.sha256.clone(),
            observed,
        });
    }
    Ok(InputArtifact {
        path,
        sha256: artifact.sha256.clone(),
    })
}

fn resolve_trusted_reasoner(
    configuration: &TrustedReasonerConfig,
) -> Result<TrustedReasonerConfig, ReferenceCompileError> {
    if configuration.expected_name.is_empty() || configuration.expected_version.is_empty() {
        return Err(ReferenceCompileError::InvalidLimits);
    }
    if decode_sha256(&configuration.adapter_jar.sha256).is_none()
        || configuration.adapter_jar.sha256.to_lowercase() != configuration.adapter_jar.sha256
    {
        return Err(ReferenceCompileError::InvalidSha256);
    }
    let java_executable = fs::canonicalize(&configuration.java_executable)?;
    let adapter_path = fs::canonicalize(&configuration.adapter_jar.path)?;
    let observed = hex::encode(sha256_file(&adapter_path)?);
    if observed != configuration.adapter_jar.sha256 {
        return Err(ReferenceCompileError::ChecksumMismatch {
            path: adapter_path,
            expected: configuration.adapter_jar.sha256.clone(),
            observed,
        });
    }
    Ok(TrustedReasonerConfig {
        java_executable,
        adapter_jar: InputArtifact {
            path: adapter_path,
            sha256: configuration.adapter_jar.sha256.clone(),
        },
        expected_name: configuration.expected_name.clone(),
        expected_version: configuration.expected_version.clone(),
    })
}

fn resolve_existing_path(path: &Path, base: &Path) -> Result<PathBuf, std::io::Error> {
    if path.is_absolute() {
        fs::canonicalize(path)
    } else {
        fs::canonicalize(base.join(path))
    }
}

fn resolve_output_root(
    path: &Path,
    base: &Path,
    allowed_root: &Path,
) -> Result<PathBuf, ReferenceCompileError> {
    let joined = if path.is_absolute() {
        path.to_path_buf()
    } else {
        base.join(path)
    };
    let output = fs::canonicalize(&joined)?;
    require_under(&output, allowed_root, false)?;
    Ok(output)
}

fn require_under(path: &Path, root: &Path, input: bool) -> Result<(), ReferenceCompileError> {
    if path.starts_with(root) {
        return Ok(());
    }
    if input {
        Err(ReferenceCompileError::InputRoot(path.to_path_buf()))
    } else {
        Err(ReferenceCompileError::OutputRoot(path.to_path_buf()))
    }
}

fn aggregate_artifact_hash(artifacts: &[InputArtifact]) -> Result<String, ReferenceCompileError> {
    let mut hasher = Sha256::new();
    for artifact in artifacts {
        let digest = decode_sha256(&artifact.sha256).ok_or(ReferenceCompileError::InvalidSha256)?;
        hasher.update((digest.len() as u64).to_be_bytes());
        hasher.update(digest);
    }
    Ok(hex::encode(hasher.finalize()))
}

fn aggregate_reasoner_hash(
    artifacts: &[ReasonerInputArtifact],
) -> Result<String, ReferenceCompileError> {
    let mut hasher = Sha256::new();
    for artifact in artifacts {
        let digest = decode_sha256(&artifact.sha256).ok_or(ReferenceCompileError::InvalidSha256)?;
        hasher.update((digest.len() as u64).to_be_bytes());
        hasher.update(digest);
    }
    Ok(hex::encode(hasher.finalize()))
}

fn safe_name(value: &str) -> String {
    value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '-' | '_') {
                character
            } else {
                '_'
            }
        })
        .collect()
}

fn sync_directory(path: &Path) -> Result<(), std::io::Error> {
    File::open(path)?.sync_all()
}

fn sync_tree(path: &Path) -> Result<(), std::io::Error> {
    let mut directories = vec![path.to_path_buf()];
    let mut cursor = 0;
    while cursor < directories.len() {
        let directory = directories[cursor].clone();
        cursor += 1;
        for entry in fs::read_dir(&directory)? {
            let entry = entry?;
            let child = entry.path();
            if child.is_dir() {
                directories.push(child);
            } else if child.is_file() {
                File::open(child)?.sync_all()?;
            }
        }
    }
    for directory in directories.into_iter().rev() {
        sync_directory(&directory)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, BTreeSet},
        fs,
        path::PathBuf,
    };

    use super::{
        ReferenceCompileError, copy_ontologies, is_routable_named_fact, scan_ontology_document,
        validate_source_graph_profile, write_semantic_exports,
    };
    use crate::{
        model::{InputArtifact, Treatment},
        rdf::{
            DEFAULT_GRAPH_STORAGE_KEY, GraphScope, NormalizedFact, NormalizedObject,
            ResourceTermKind,
        },
        sha256_file,
    };
    use ngkg_dataset::{GraphDeclaration, compile_catalog};
    use ngkg_sparql_compiler::CompiledSparqlQuery;
    use uuid::Uuid;

    fn core_fact(
        subject: &str,
        graph_scope: GraphScope,
        graph_iri: &str,
        ordinal: u8,
    ) -> NormalizedFact {
        NormalizedFact {
            fact_id: [ordinal; 16],
            fact_hash: [ordinal; 32],
            subject_iri: subject.to_owned(),
            subject_term_kind: ResourceTermKind::NamedNode,
            subject_guid: Uuid::from_u128(u128::from(ordinal).saturating_add(1)),
            predicate_iri: "https://example.test/predicate".to_owned(),
            object: NormalizedObject::Entity {
                iri: format!("https://example.test/object/{ordinal}"),
                guid: Uuid::from_u128(u128::from(ordinal).saturating_add(100)),
                term_kind: ResourceTermKind::NamedNode,
            },
            graph_iri: graph_iri.to_owned(),
            graph_scope,
            treatment: Treatment::Core,
            participates_in_reasoning: true,
            queryable_as_rdf: true,
        }
    }

    #[test]
    fn semantic_exports_enforce_graph_catalog_visibility() -> Result<(), Box<dyn std::error::Error>>
    {
        let root =
            std::env::temp_dir().join(format!("ngkg-semantic-export-test-{}", Uuid::new_v4()));
        fs::create_dir(&root)?;
        let query_path = root.join("query.nq");
        let abox_path = root.join("abox.nt");
        let graphs = [
            ("https://example.test/query", true, false),
            ("https://example.test/reasoning", false, true),
            ("https://example.test/both", true, true),
        ];
        let declarations = graphs
            .iter()
            .map(|(iri, query_visible, reasoning_visible)| GraphDeclaration {
                graph_iri: (*iri).to_owned(),
                role: "semkg".to_owned(),
                authorization_labels: BTreeSet::from(["test-access".to_owned()]),
                query_visible: *query_visible,
                reasoning_visible: *reasoning_visible,
            })
            .collect::<Vec<_>>();
        let counts = graphs
            .iter()
            .map(|(iri, _, _)| ((*iri).to_owned(), 1_u64))
            .collect::<BTreeMap<_, _>>();
        let catalog = compile_catalog(
            Uuid::from_u128(1),
            Uuid::from_u128(2),
            1,
            &counts,
            &declarations,
        )?;
        let facts = vec![
            core_fact(
                "https://example.test/default-subject",
                GraphScope::Default,
                DEFAULT_GRAPH_STORAGE_KEY,
                1,
            ),
            core_fact(
                "https://example.test/query-subject",
                GraphScope::Named,
                "https://example.test/query",
                2,
            ),
            core_fact(
                "https://example.test/reasoning-subject",
                GraphScope::Named,
                "https://example.test/reasoning",
                3,
            ),
            core_fact(
                "https://example.test/both-subject",
                GraphScope::Named,
                "https://example.test/both",
                4,
            ),
        ];

        write_semantic_exports(&facts, &catalog, &query_path, &abox_path)?;
        let query = fs::read_to_string(&query_path)?;
        assert!(query.contains("https://example.test/query-subject"));
        assert!(query.contains("https://example.test/both-subject"));
        assert!(!query.contains("https://example.test/reasoning-subject"));
        assert!(!query.contains("https://example.test/default-subject"));
        let abox = fs::read_to_string(&abox_path)?;
        assert!(abox.contains("https://example.test/reasoning-subject"));
        assert!(abox.contains("https://example.test/both-subject"));
        assert!(!abox.contains("https://example.test/query-subject"));
        assert!(!abox.contains("https://example.test/default-subject"));
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn unresolved_import_never_reaches_the_reasoner_process() {
        let source = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../test-corpus/failures/unresolved-import.ttl");
        let root = std::env::temp_dir().join(format!("ngkg-ontology-test-{}", Uuid::new_v4()));
        assert!(fs::create_dir_all(root.join("ontology")).is_ok());
        let hash_result = sha256_file(&source);
        assert!(
            hash_result.is_ok(),
            "checked-in ontology fixture could not be read"
        );
        let hash = match hash_result {
            Ok(value) => value,
            Err(_) => return,
        };
        let input = InputArtifact {
            path: source,
            sha256: hex::encode(hash),
        };
        let result = copy_ontologies(&root, &[input]);
        assert!(matches!(
            result,
            Err(ReferenceCompileError::UnresolvedImport(_))
        ));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn ontology_preflight_accepts_version_iri_import_alias()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = std::env::temp_dir().join(format!("ngkg-import-alias-{}", Uuid::new_v4()));
        fs::create_dir_all(root.join("stage/ontology"))?;
        let parent = root.join("parent.ttl");
        let child = root.join("child.ttl");
        fs::write(
            &parent,
            r#"
            @prefix owl: <http://www.w3.org/2002/07/owl#> .
            <https://example.test/parent> a owl:Ontology ;
                owl:imports <https://example.test/child/2026> .
        "#,
        )?;
        fs::write(
            &child,
            r#"
            @prefix owl: <http://www.w3.org/2002/07/owl#> .
            <https://example.test/child> a owl:Ontology ;
                owl:versionIRI <https://example.test/child/2026> .
        "#,
        )?;
        let inputs = vec![
            InputArtifact {
                path: parent.clone(),
                sha256: hex::encode(sha256_file(&parent)?),
            },
            InputArtifact {
                path: child.clone(),
                sha256: hex::encode(sha256_file(&child)?),
            },
        ];
        let copied = copy_ontologies(&root.join("stage"), &inputs)?;
        assert_eq!(copied.len(), 2);
        assert!(
            copied[1]
                .ontology_iris
                .contains(&"https://example.test/child/2026".to_owned())
        );
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn ontology_preflight_rejects_multiple_ontology_headers()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = std::env::temp_dir().join(format!("ngkg-multi-header-{}", Uuid::new_v4()));
        fs::create_dir_all(&root)?;
        let source = root.join("bad.ttl");
        fs::write(
            &source,
            r#"
            @prefix owl: <http://www.w3.org/2002/07/owl#> .
            <https://example.test/a> a owl:Ontology .
            <https://example.test/b> a owl:Ontology .
        "#,
        )?;
        assert!(matches!(
            scan_ontology_document(&source),
            Err(ReferenceCompileError::MultipleOntologyIris(_))
        ));
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn ontology_preflight_rejects_misplaced_import_header() -> Result<(), Box<dyn std::error::Error>>
    {
        let root = std::env::temp_dir().join(format!("ngkg-misplaced-import-{}", Uuid::new_v4()));
        fs::create_dir_all(&root)?;
        let source = root.join("bad.ttl");
        fs::write(
            &source,
            r#"
            @prefix owl: <http://www.w3.org/2002/07/owl#> .
            <https://example.test/a> a owl:Ontology .
            <https://example.test/not-the-header> owl:imports <https://example.test/b> .
        "#,
        )?;
        assert!(matches!(
            scan_ontology_document(&source),
            Err(ReferenceCompileError::MisplacedOntologyHeader(_))
        ));
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn typed_query_analysis_ignores_comments_and_literal_lookalikes()
    -> Result<(), Box<dyn std::error::Error>> {
        let query = r#"
            # GRAPH <https://example.test/ignored-comment>
            SELECT ?subject
            FROM NAMED <https://example.test/domain-a>
            WHERE {
              GRAPH <https://example.test/domain-a> {
                ?subject <https://example.test/predicate> "<https://example.test/ignored-literal>" .
              }
            }
        "#;
        let compiled = CompiledSparqlQuery::parse(query)?;
        assert_eq!(
            compiled.route_analysis().declared_graph_iris,
            BTreeSet::from(["https://example.test/domain-a".to_owned()])
        );
        assert_eq!(
            compiled.route_analysis().semantic_iris,
            BTreeSet::from(["https://example.test/predicate".to_owned()])
        );
        Ok(())
    }

    #[test]
    fn checked_cross_domain_query_selects_its_declared_graphs() {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../test-corpus/queries/q01-cross-domain.rq");
        let query_result = fs::read_to_string(path);
        assert!(
            query_result.is_ok(),
            "checked query corpus must be readable"
        );
        let query = match query_result {
            Ok(value) => value,
            Err(_) => return,
        };
        let compiled = match CompiledSparqlQuery::parse(&query) {
            Ok(value) => value,
            Err(error) => {
                assert!(false, "checked query must compile: {error}");
                return;
            }
        };
        assert_eq!(
            compiled.route_analysis().declared_graph_iris,
            BTreeSet::from([
                "urn:ngkg:graph:hdfs".to_owned(),
                "urn:ngkg:graph:operations".to_owned(),
            ])
        );
        assert!(
            compiled
                .route_analysis()
                .semantic_iris
                .contains("https://ngkg.io/ontology/LatencyObservation")
        );
        assert!(
            compiled
                .route_analysis()
                .semantic_iris
                .contains("https://ngkg.io/ontology/affectedBy")
        );
        let Some(blocks) = compiled.distributed_graph_fragments() else {
            assert!(
                false,
                "checked cross-domain query must remain distributable"
            );
            return;
        };
        assert_eq!(blocks.len(), 2);
        assert_eq!(blocks[0].graph_iri, "urn:ngkg:graph:hdfs");
        assert_eq!(blocks[1].graph_iri, "urn:ngkg:graph:operations");
        assert!(blocks[0].query_text.contains("FILTER"));
    }

    #[test]
    fn internal_default_graph_key_never_enters_named_graph_routing() {
        let mut fact = NormalizedFact {
            fact_id: [1_u8; 16],
            fact_hash: [2_u8; 32],
            subject_iri: "https://example.test/subject".to_owned(),
            subject_term_kind: ResourceTermKind::NamedNode,
            subject_guid: Uuid::from_u128(1),
            predicate_iri: "https://example.test/predicate".to_owned(),
            object: NormalizedObject::Entity {
                iri: "https://example.test/object".to_owned(),
                guid: Uuid::from_u128(2),
                term_kind: ResourceTermKind::NamedNode,
            },
            graph_iri: DEFAULT_GRAPH_STORAGE_KEY.to_owned(),
            graph_scope: GraphScope::Default,
            treatment: Treatment::Core,
            participates_in_reasoning: true,
            queryable_as_rdf: true,
        };
        assert!(!is_routable_named_fact(&fact));
        assert!(
            validate_source_graph_profile(
                std::slice::from_ref(&fact),
                "https://example.test/closure"
            )
            .is_ok()
        );
        fact.graph_scope = GraphScope::Named;
        fact.graph_iri = "https://example.test/closure".to_owned();
        assert!(matches!(
            validate_source_graph_profile(
                std::slice::from_ref(&fact),
                "https://example.test/closure"
            ),
            Err(ReferenceCompileError::ClosureGraphCollision)
        ));
    }
}
