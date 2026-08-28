# Phase 40.2 — Datatype Policy Runtime Contract

Phase 40.2 builds directly on Phase 40.1 and adds the supported datatype map required before legal OWL Direct BGP compilation.

Implemented scope:

- a strict `contracts/datatype-policy.schema.json` contract and repository-shipped `policies/owl-direct-datatype-policy.json`;
- fail-closed validation of reasoning-visible normalized RDF literals before HermiT invocation;
- deterministic multi-core validation with source-index-stable failures and exact merged datatype counts;
- request/report SHA-256 binding for the datatype policy;
- OWLAPI merged-ontology datatype coverage validation before HermiT reasoning;
- immutable `reasoner/datatype-policy.json` and `reasoner/datatype-validation.json` snapshot artifacts;
- new snapshot manifests expose `datatypePolicySha256` while older manifests remain deserializable;
- positive/negative schema fixtures and an independent policy validator.

Phase 40.2 does not implement Direct-BGP result contracts, Direct certificates, BGP legality, proof DAGs, or exact arbitrary OWL Direct fallback. Those remain Phase 40.3 and later milestones.
