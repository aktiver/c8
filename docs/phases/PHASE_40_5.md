# Phase 40.5 — Combined OWL 2 DL Profile / Import Qualification

Phase 40.5 hardens the inherited OWL 2 DL gate without claiming arbitrary OWL Direct query completeness.

Implemented:

- exactly one ontology header per ontology document;
- ontology/version IRI alias binding to one checksum-bound local document;
- fail-closed misplaced `owl:versionIRI` / `owl:imports` detection;
- unresolved and duplicate ontology/import identity rejection before reasoner invocation;
- OWLAPI-side reconstruction of all loaded ontology/version identities;
- complete local import-edge resolution evidence;
- deterministic `owl-profile-qualification.json` artifact;
- report format v4 binding to that artifact SHA-256;
- Rust-side independent qualification and import-target verification;
- snapshot/certification exposure of the qualification SHA-256;
- schema, fixtures, Java/Rust tests and cumulative static gates.

Not implemented by this phase: enhanced consistency certificates (40.6), Direct-BGP legality (40.7), exact arbitrary Direct fallback (40.8), proof DAG wiring (40.9), or production/native qualification.
