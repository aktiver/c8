# OWL 2 DL Consistency Qualification

Phase 40.6 makes logical consistency a checksum-bound publication gate over the exact combined OWL 2 DL ontology qualified in Phase 40.5.

The HermiT adapter calls `OWLReasoner.isConsistent()` on the complete merged ontology after local import closure, datatype policy, and `OWL2DLProfile` qualification. It emits `reasoner/owl-consistency-qualification.json`; the reasoner report and immutable snapshot bind that artifact by SHA-256, and Rust independently verifies every dataset/snapshot/input/signature/policy/profile/reasoner/count identity before accepting the result.

Consistency is global. NGKG never infers global consistency from per-graph or arbitrary per-node consistency checks. Named graph partitioning remains a physical execution concern; the consistency decision is for the complete checksum-bound semantic ontology and is not split into arbitrary graph-local consistency claims.

If the ontology is inconsistent, `publicationPermitted` is false, no normal snapshot publication may proceed, and OWL Direct query answering must fail closed. An inconsistent ontology never yields a successful partial semantic result.

## HPC boundary

Checksum verification, RDF normalization, datatype validation, artifact hashing, and independent snapshot jobs may execute across bounded CPU lanes and Kubernetes reasoning nodes. The exact logical decision remains HermiT over the combined ontology unless a future sound module-completeness proof establishes an equivalent distributed reasoning decomposition.

Phase 40.6 does not implement Direct-BGP legality or arbitrary exact Direct-BGP fallback; those remain Phase 40.7 and 40.8.
