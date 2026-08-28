# Phase 40.13.16 acceptance

The phase is accepted only when all of the following are demonstrated from a clean source tree:

1. The normal query route dispatches every legal query form through a checksum-bound distributed
   algebra worker envelope when the production gate is enabled.
2. At least two distinct worker identities complete every request and produce identical canonical
   results; ordered queries must also return the same sequence.
3. Missing, duplicate, stale, unauthorized, timed-out, malformed, or unequal replicas fail closed.
4. SELECT multiset/order, ASK Boolean, and CONSTRUCT/DESCRIBE graph equivalence match the scalar
   oracle across the complete SPARQL 1.1 query corpus.
5. Existing certified Arrow/shuffle execution remains available for its exact native fast path.
6. The fragment HPA observes algebra/shuffle backlog, spill, CPU, and memory; live pod and node
   scaling does not change answers.
7. Cargo format/check/Clippy/tests, OpenAPI parity, Helm lint/render, failure injection, and live
   multinode qualification pass from pinned toolchains.
8. No ontology-alignment, schema-matching, or raw-data-mapping functionality exists.

Volatile query-scoped functions are accepted on the uncached scalar lane rather than replicated;
federated `SERVICE` execution is governed by the later secured-federation phase.
