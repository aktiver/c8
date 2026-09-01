# Phase 40.3 — Direct-BGP Result Runtime Contract

Phase 40.3 builds directly on Phase 40.2 and adds the exact result object required before legal OWL Direct BGP compilation and fallback reasoning can be implemented.

Implemented scope:

- shared Rust `DirectBgpResult` and exact RDF-term/bag contracts in `ngkg-types`;
- dataset/snapshot/query/BGP/active-dataset/authorization/OWL-signature/datatype-policy bindings;
- explicit default-versus-named graph context;
- fail-closed exactness/completeness/error invariants;
- deterministic bounded multi-core validation over large solution vectors without bag expansion;
- strict Draft 2020-12 JSON Schema, positive/negative fixtures and independent validation;
- cumulative/static gate and Phase 40.2→40.3 checksum inheritance evidence.

Phase 40.3 does not implement Direct certificates, BGP legality, exact HermiT candidate enumeration, proof DAGs, or arbitrary OWL Direct completeness. Those remain Phase 40.4 and later milestones.
