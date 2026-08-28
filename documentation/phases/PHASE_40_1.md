# Phase 40.1 — OWL Signature Runtime Contract

Phase 40.1 builds directly on Phase 40 and adds the first OWL Direct prerequisite artifact: a deterministic OWL signature generated from the actual merged ontology loaded by the version-pinned HermiT/OWLAPI adapter.

Implemented scope:

- `contracts/owl-signature.schema.json` defines the closed JSON contract.
- `ReasonerRequest` declares the signature output path.
- the Java adapter emits classes, object/data/annotation properties, named individuals, datatypes, imports and checksum-bound ontology-document identities;
- `reasoner/report.json` binds the exact signature bytes by SHA-256;
- Rust independently validates identity, request-input equality, IRI validity, deterministic ordering and signature hash before accepting reasoner success;
- newly compiled snapshot manifests expose `owlSignatureSha256`, while older manifests remain deserializable;
- positive and negative schema/semantic fixtures are executable through `scripts/validate_owl_signature.py`.

Phase 40.1 does not implement datatype policy, legal Direct-BGP classification, proof DAGs or exact Direct fallback. Those remain Phase 40.2 and later milestones.
