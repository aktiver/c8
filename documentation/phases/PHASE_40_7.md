# Phase 40.7 — Legal OWL Direct-BGP Classification/Validation

Parent: Phase 40.6.

Implemented: shared legality report contract, strict JSON Schema, typed-algebra BGP walker, checksum-bound OWL-signature declaration index, BGP-local W3C variable-role validation, structural OWL predicate classification, fail-closed unknown/ambiguous handling, bounded deterministic multi-core classification, authenticated REST preflight + Swagger/OpenAPI, reasoner-client legality handoff, fixtures and cumulative gates.

Not claimed: exact OWL Direct entailment, W3C C1/C2/C3 candidate satisfaction, proof DAG completeness, arbitrary named-graph entailment by merged-graph reasoning, or final standards conformance. Those remain Phase 40.8+.


## Historical ancestry gate supersession

Phase 40.7 legitimately edits files that existed in the Phase 40.6 release. The Phase 40.6
static verifier therefore validates its original 40.5→40.6 delta against the preserved final
Phase 40.6 file manifest when running inside a descendant. Phase 40.7 independently SHA-binds
that exact final Phase 40.6 manifest as its parent. This supersedes only the old
"re-hash the current descendant bytes" implementation detail; no Phase 40.6 semantic
qualification assertion is removed or weakened.
