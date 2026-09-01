//! Durable external identity and deterministic snapshot-local dictionaries.

use std::collections::BTreeMap;

use oxiri::Iri;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

/// Full and compact identities for one provenance-qualified RDF assertion.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct FactIdentity {
    pub compact_id: [u8; 16],
    pub collision_fingerprint: [u8; 32],
}

/// Canonical assertion components.
pub struct FactIdentityInput<'a> {
    pub subject: &'a [u8],
    pub predicate_iri: &'a str,
    pub object_canonical: &'a [u8],
    pub graph_iri: &'a str,
    pub source_guid: Uuid,
    pub source_snapshot: &'a str,
}

/// One reducer run. Input must already use the approved canonical bytes.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DictionaryRun {
    pub range_id: u32,
    pub canonical_terms: Vec<Vec<u8>>,
}

/// Deterministic dense assignment and range counts.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct DenseDictionary {
    pub entries: BTreeMap<Vec<u8>, u64>,
    pub range_counts: BTreeMap<u32, u64>,
    pub root_hash: [u8; 32],
}

/// Identity failures block publication.
#[derive(Debug, Error, Eq, PartialEq)]
pub enum IdentityError {
    #[error("invalid canonical IRI: {0}")]
    InvalidIri(String),
    #[error("identity conflict for source IRI: {0}")]
    CrosswalkConflict(String),
    #[error("compact FactID collision detected")]
    FactIdCollision,
    #[error("dictionary contains no terms")]
    EmptyDictionary,
}

/// Deterministic GUID for an authoritative canonical IRI.
pub fn guid_for_canonical_iri(
    dataset_namespace: Uuid,
    canonical_iri: &str,
) -> Result<Uuid, IdentityError> {
    let iri = Iri::parse(canonical_iri.to_owned())
        .map_err(|_| IdentityError::InvalidIri(canonical_iri.to_owned()))?;
    Ok(Uuid::new_v5(&dataset_namespace, iri.as_str().as_bytes()))
}

/// Deterministic blank-node skolem IRI scoped to immutable source and label.
#[must_use]
pub fn skolem_iri(source_sha256: &[u8; 32], source_label: &str) -> String {
    let mut hasher = blake3::Hasher::new();
    append_component(&mut hasher, b"ngkg-skolem-v1");
    append_component(&mut hasher, source_sha256);
    append_component(&mut hasher, source_label.as_bytes());
    format!("urn:ngkg:skolem:{}", hasher.finalize().to_hex())
}

/// Canonical, length-delimited FactID construction.
#[must_use]
pub fn fact_identity(input: &FactIdentityInput<'_>) -> FactIdentity {
    let mut hasher = blake3::Hasher::new();
    append_component(&mut hasher, b"ngkg-fact-v1");
    append_component(&mut hasher, input.subject);
    append_component(&mut hasher, input.predicate_iri.as_bytes());
    append_component(&mut hasher, input.object_canonical);
    append_component(&mut hasher, input.graph_iri.as_bytes());
    append_component(&mut hasher, input.source_guid.as_bytes());
    append_component(&mut hasher, input.source_snapshot.as_bytes());
    let digest = hasher.finalize();
    let mut compact_id = [0_u8; 16];
    compact_id.copy_from_slice(&digest.as_bytes()[..16]);
    FactIdentity {
        compact_id,
        collision_fingerprint: *digest.as_bytes(),
    }
}

/// Reject a compact-key collision rather than coalescing assertions.
pub fn verify_fact_collision(left: FactIdentity, right: FactIdentity) -> Result<(), IdentityError> {
    if left.compact_id == right.compact_id
        && left.collision_fingerprint != right.collision_fingerprint
    {
        return Err(IdentityError::FactIdCollision);
    }
    Ok(())
}

/// Merge independently produced runs and assign dense IDs by canonical byte order.
pub fn merge_dictionary_runs(runs: &[DictionaryRun]) -> Result<DenseDictionary, IdentityError> {
    let mut terms = Vec::new();
    let mut range_counts = BTreeMap::new();
    for run in runs {
        let mut local = run.canonical_terms.clone();
        local.sort_unstable();
        local.dedup();
        range_counts.insert(run.range_id, u64::try_from(local.len()).unwrap_or(u64::MAX));
        terms.extend(local);
    }
    terms.sort_unstable();
    terms.dedup();
    if terms.is_empty() {
        return Err(IdentityError::EmptyDictionary);
    }
    let mut entries = BTreeMap::new();
    let mut hasher = blake3::Hasher::new();
    append_component(&mut hasher, b"ngkg-dictionary-v1");
    for (index, term) in terms.into_iter().enumerate() {
        let id = u64::try_from(index).unwrap_or(u64::MAX);
        append_component(&mut hasher, &id.to_be_bytes());
        append_component(&mut hasher, &term);
        entries.insert(term, id);
    }
    Ok(DenseDictionary {
        entries,
        range_counts,
        root_hash: *hasher.finalize().as_bytes(),
    })
}

fn append_component(hasher: &mut blake3::Hasher, value: &[u8]) {
    hasher.update(&(value.len() as u64).to_be_bytes());
    hasher.update(value);
}

#[cfg(test)]
mod tests {
    use super::{
        DictionaryRun, FactIdentityInput, fact_identity, guid_for_canonical_iri,
        merge_dictionary_runs,
    };
    use uuid::Uuid;

    #[test]
    fn identity_does_not_depend_on_partition_order() {
        let namespace = Uuid::from_u128(42);
        let first = guid_for_canonical_iri(namespace, "https://ngkg.io/id/node-1");
        let second = guid_for_canonical_iri(namespace, "https://ngkg.io/id/node-1");
        assert_eq!(first, second);
        let a = DictionaryRun {
            range_id: 1,
            canonical_terms: vec![b"z".to_vec(), b"a".to_vec()],
        };
        let b = DictionaryRun {
            range_id: 2,
            canonical_terms: vec![b"m".to_vec(), b"a".to_vec()],
        };
        assert_eq!(
            merge_dictionary_runs(&[a.clone(), b.clone()]).map(|value| value.entries),
            merge_dictionary_runs(&[b, a]).map(|value| value.entries)
        );
    }

    #[test]
    fn graph_and_provenance_change_fact_identity() {
        let source = Uuid::from_u128(7);
        let make = |graph: &str| {
            fact_identity(&FactIdentityInput {
                subject: b"s",
                predicate_iri: "urn:p",
                object_canonical: b"o",
                graph_iri: graph,
                source_guid: source,
                source_snapshot: "source-v1",
            })
        };
        assert_ne!(make("urn:g1"), make("urn:g2"));
    }
}
