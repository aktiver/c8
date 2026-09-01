# Phase 40.5 OWL 2 DL Profile and Import Qualification

Phase 40.5 turns the existing combined OWL 2 DL profile check into checksum-bound publication evidence. The compiler still rejects unresolved imports before the Java adapter starts, but the adapter now independently records what OWLAPI actually loaded and resolved.

Every ontology document must have exactly one `owl:Ontology` header. An optional `owl:versionIRI` and every `owl:imports` declaration must be attached to that same header. The compiler treats the ontology IRI and optional version IRI as aliases for one checksum-bound local document; an import may resolve through either alias, but one alias may never map to two documents.

The adapter emits `reasoner/owl-profile-qualification.json` after OWLAPI has loaded the complete local input set and constructed the merged ontology. It records the ontology/version identity of each loaded ontology document, each resolved import edge and its target document SHA-256, loaded/input document counts, merged axiom count, and the complete `OWL2DLProfile` result. `completeLocalImportClosure=true` is required for any accepted result.

`reasoner/report.json` is Phase 40.5 format version 4 and contains `owlProfileQualificationSha256`. Rust independently re-hashes and validates the qualification artifact, checks all identities against the reasoner request and OWL signature, verifies each import target against the exact checksum-bound request document, and requires the report's profile evidence to match the qualification bytes exactly.

A profile-invalid ontology may write bounded qualification/report evidence for diagnostics, but it still fails closed and cannot publish. Phase 40.5 does not strengthen consistency semantics beyond the inherited HermiT check; consistency qualification hardening remains Phase 40.6.

## HPC position

Import/header scanning and OWL profile qualification are correctness-dominated and operate over the relatively small ontology bundle already resident in OWLAPI. They are intentionally not split across graph partitions or BLAS/OpenMP kernels. Existing multi-node Arrow/Parquet/shuffle execution remains unchanged. Later Phase 40.15 owns distributed exact execution; Phase 40.10-40.14 own resource ceilings and cpuset discipline. This phase favors deterministic import closure over artificial parallelism.

The qualification must prove the complete local import closure before publication.
