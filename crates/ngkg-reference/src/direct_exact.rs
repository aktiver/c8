//! Phase 40.8 active-ontology materialization for the exhaustive Direct reasoner.
//!
//! The exact fallback never trusts a caller-provided "active dataset" file. It receives the
//! immutable snapshot `query-dataset.nq`, the checksum-bound graph catalog, and the exact
//! `ResolvedDataset` that Phase 40.7 used. This module revalidates the logical dataset hashes and
//! materializes the BGP scoping graph itself. That avoids confusing the logical
//! `activeDatasetSha256` with an artifact SHA and preserves the crucial SPARQL distinction between
//! union-default set union and explicit FROM RDF merge (blank nodes standardized apart).

use std::{collections::BTreeSet, fs, io::Write, path::Path};

use ngkg_dataset::{
    DatasetSelectionSource, GraphCatalog, LogicalGraphName, ResolvedDataset,
    validate_resolved_dataset,
};
use ngkg_direct_reasoner::DirectExactOntologyBundle;
use ngkg_types::{DirectBgpGraphContext, DirectBgpScope, DirectExactOntologyInput};
use oxigraph::{
    io::{RdfFormat, RdfParser},
    model::{GraphName, NamedOrBlankNode, Term},
};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::model::{OwlSignature, ReferenceSnapshotManifest};

#[derive(Debug, Error)]
pub enum DirectActiveOntologyError {
    #[error("snapshot query-dataset I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("snapshot query-dataset N-Quads parsing failed: {0}")]
    Parse(String),
    #[error("resolved active-dataset envelope does not match the graph catalog/hashes")]
    DatasetIntegrity,
    #[error("named graph requested by Direct-BGP is absent from the active named dataset")]
    NamedGraphAbsent,
    #[error("snapshot ontology artifact does not match the OWL signature")]
    OntologyArtifact,
    #[error("snapshot query-dataset artifact does not match the manifest")]
    QueryDatasetArtifact,
    #[error("scoped default graph hash differs from the Direct-BGP graph context")]
    DefaultGraphHash,
    #[error("ontology aggregate input hash contains an invalid SHA-256")]
    AggregateHash,
}

/// Build the exact scoped ABox and complete ontology input bundle.
///
/// `query_dataset_path` is the immutable snapshot `data/query-dataset.nq`, not a caller-created
/// active-dataset file. `resolved` is revalidated against the catalog before any quad is read.
pub fn build_direct_active_ontology_bundle(
    snapshot_root: &Path,
    manifest: &ReferenceSnapshotManifest,
    signature: &OwlSignature,
    graph_catalog: &GraphCatalog,
    resolved: &ResolvedDataset,
    query_dataset_path: &Path,
    graph_scope: &DirectBgpScope,
    graph_binding_iri: Option<&str>,
    work_dir: &Path,
) -> Result<DirectExactOntologyBundle, DirectActiveOntologyError> {
    validate_resolved_dataset(graph_catalog, resolved)
        .map_err(|_| DirectActiveOntologyError::DatasetIntegrity)?;
    if resolved.active_dataset_sha256.is_empty() || resolved.authorized_graph_set_sha256.is_empty()
    {
        return Err(DirectActiveOntologyError::DatasetIntegrity);
    }
    let query_artifact = manifest
        .artifacts
        .iter()
        .find(|artifact| artifact.relative_path == "data/query-dataset.nq")
        .ok_or(DirectActiveOntologyError::QueryDatasetArtifact)?;
    if hex::encode(sha256_file(query_dataset_path)?) != query_artifact.sha256 {
        return Err(DirectActiveOntologyError::QueryDatasetArtifact);
    }

    fs::create_dir_all(work_dir)?;
    let graph_context = match graph_scope {
        DirectBgpScope::Default => DirectBgpGraphContext::Default {
            active_default_graph_sha256: "0".repeat(64),
        },
        DirectBgpScope::Named { graph_iri } => DirectBgpGraphContext::Named {
            graph_iri: graph_iri.clone(),
        },
        DirectBgpScope::NamedVariable { .. } => DirectBgpGraphContext::Named {
            graph_iri: graph_binding_iri
                .ok_or(DirectActiveOntologyError::NamedGraphAbsent)?
                .to_owned(),
        },
    };
    let abox_path = work_dir.join("active-scope-abox.nt");
    write_scoped_abox(
        query_dataset_path,
        graph_catalog,
        resolved,
        &graph_context,
        &abox_path,
    )?;
    let abox_sha = hex::encode(sha256_file(&abox_path)?);
    let graph_context = match graph_context {
        DirectBgpGraphContext::Default { .. } => DirectBgpGraphContext::Default {
            active_default_graph_sha256: abox_sha.clone(),
        },
        named => named,
    };

    let mut inputs = Vec::new();
    for document in &signature.ontology_documents {
        let artifact = manifest
            .artifacts
            .iter()
            .find(|artifact| {
                artifact.sha256 == document.sha256
                    && artifact.relative_path.starts_with("ontology/")
            })
            .ok_or(DirectActiveOntologyError::OntologyArtifact)?;
        let path = snapshot_root.join(&artifact.relative_path);
        if hex::encode(sha256_file(&path)?) != document.sha256 {
            return Err(DirectActiveOntologyError::OntologyArtifact);
        }
        inputs.push(DirectExactOntologyInput {
            path: fs::canonicalize(path)?.to_string_lossy().into_owned(),
            sha256: document.sha256.clone(),
            ontology_iris: document.ontology_iris.clone(),
        });
    }
    inputs.push(DirectExactOntologyInput {
        path: fs::canonicalize(abox_path)?.to_string_lossy().into_owned(),
        sha256: abox_sha.clone(),
        ontology_iris: Vec::new(),
    });
    inputs.sort_by(|left, right| left.path.cmp(&right.path));
    let aggregate_input_sha256 = aggregate_hash(&inputs)?;
    Ok(DirectExactOntologyBundle {
        inputs,
        aggregate_input_sha256,
        graph_context,
        scoped_graph_sha256: abox_sha,
    })
}

fn write_scoped_abox(
    query_dataset_path: &Path,
    catalog: &GraphCatalog,
    resolved: &ResolvedDataset,
    graph_context: &DirectBgpGraphContext,
    output: &Path,
) -> Result<(), DirectActiveOntologyError> {
    let default_ids = resolved
        .default_graph_ids
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    let named_ids = resolved
        .named_graph_ids
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    let mut triples = BTreeSet::new();
    let mut named_seen = false;
    let parser =
        RdfParser::from_format(RdfFormat::NQuads).for_reader(fs::File::open(query_dataset_path)?);
    for item in parser {
        let quad = item.map_err(|error| DirectActiveOntologyError::Parse(error.to_string()))?;
        let GraphName::NamedNode(graph) = &quad.graph_name else {
            // The physical source default graph is never part of NGKG's OWL-Direct service
            // dataset; ResolvedDataset contains named graph IDs only.
            continue;
        };
        let graph_record = catalog
            .named(graph.as_str())
            .ok_or(DirectActiveOntologyError::DatasetIntegrity)?;
        let include = match graph_context {
            DirectBgpGraphContext::Default { .. } => default_ids.contains(&graph_record.graph_id),
            DirectBgpGraphContext::Named { graph_iri } => {
                if graph.as_str() == graph_iri && named_ids.contains(&graph_record.graph_id) {
                    named_seen = true;
                    true
                } else {
                    false
                }
            }
        };
        if !include {
            continue;
        }
        let standardize_apart = matches!(graph_context, DirectBgpGraphContext::Default { .. })
            && resolved.selection_source != DatasetSelectionSource::ServiceDefault;
        let subject = render_subject(&quad.subject, graph_record.graph_id, standardize_apart);
        let object = render_object(&quad.object, graph_record.graph_id, standardize_apart);
        triples.insert(format!(
            "{subject} <{}> {object} .\n",
            quad.predicate.as_str()
        ));
    }
    if matches!(graph_context, DirectBgpGraphContext::Named { .. }) && !named_seen {
        return Err(DirectActiveOntologyError::NamedGraphAbsent);
    }
    let mut file = fs::File::create(output)?;
    for triple in triples {
        file.write_all(triple.as_bytes())?;
    }
    file.sync_all()?;
    Ok(())
}

fn render_subject(subject: &NamedOrBlankNode, graph_id: u32, standardize_apart: bool) -> String {
    match subject {
        NamedOrBlankNode::NamedNode(node) => node.to_string(),
        NamedOrBlankNode::BlankNode(node) => {
            render_blank(node.as_str(), graph_id, standardize_apart)
        }
    }
}

fn render_object(object: &Term, graph_id: u32, standardize_apart: bool) -> String {
    match object {
        Term::NamedNode(node) => node.to_string(),
        Term::BlankNode(node) => render_blank(node.as_str(), graph_id, standardize_apart),
        Term::Literal(literal) => literal.to_string(),
    }
}

fn render_blank(value: &str, graph_id: u32, standardize_apart: bool) -> String {
    if standardize_apart {
        // FROM/FROM NAMED uses RDF merge semantics. Prefixing with the dense source graph ID is a
        // deterministic standardize-apart operation and cannot collide with a source blank label.
        format!("_:ngkg_from_g{graph_id}_{value}")
    } else {
        format!("_:{value}")
    }
}

fn aggregate_hash(
    inputs: &[DirectExactOntologyInput],
) -> Result<String, DirectActiveOntologyError> {
    let mut aggregate = Sha256::new();
    for input in inputs {
        let bytes =
            hex::decode(&input.sha256).map_err(|_| DirectActiveOntologyError::AggregateHash)?;
        if bytes.len() != 32 {
            return Err(DirectActiveOntologyError::AggregateHash);
        }
        aggregate.update((bytes.len() as u64).to_be_bytes());
        aggregate.update(bytes);
    }
    Ok(hex::encode(aggregate.finalize()))
}

fn sha256_file(path: &Path) -> Result<[u8; 32], std::io::Error> {
    use std::io::Read;
    let mut file = fs::File::open(path)?;
    let mut hash = Sha256::new();
    let mut buffer = [0_u8; 1024 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hash.update(&buffer[..read]);
    }
    Ok(hash.finalize().into())
}
