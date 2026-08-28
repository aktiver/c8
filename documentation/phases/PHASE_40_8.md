# Phase 40.8 — Exact HermiT / OWL Direct fallback

Phase 40.8 consumes only Phase 40.7-admitted legal BGPs. The reference worker revalidates the immutable snapshot, authorization-qualified `ResolvedDataset`, graph catalog, query algebra, OWL signature, datatype policy, profile evidence and consistency evidence before constructing a scoped active ontology.

The exact engine enumerates the finite typed candidate product, divides it into deterministic ordinal partitions, and runs bounded HermiT JVM lanes. Each candidate is grounded, checked with `OWL2DLProfile` against the active ontology, and its logical axioms are tested with HermiT `isEntailed`. A global gap-free partition barrier is required before a `DirectBgpResult` can be marked exact/complete and before a Direct certificate can be emitted.

## HPC model

Candidate partition identity is independent of CPU count. `available_parallelism()` controls only concurrent local lanes, capped at eight in 40.8; each JVM is constrained to one active processor and a bounded heap. Immutable ordinal partitions are the future Phase 40.15 multi-node scheduling unit. Worker stderr is file-backed to prevent pipe deadlock and request/result spools are checksum-bound.

## Dataset correctness

`activeDatasetSha256` is a logical dataset hash, not a file checksum. The worker recomputes it from the graph catalog. Service-default uses RDF set union. Explicit `FROM` datasets use RDF merge with deterministic blank-node standardization apart. `GRAPH <g>` and concrete `GRAPH ?g` bindings reason over only the selected active named graph.

## Fail-closed boundary

The phase intentionally does not claim arbitrary OWL Direct completeness. W3C anonymous-individual instance mapping (`sigma`) multiplicity is not yet independently qualified; BGP/query cases requiring it return an explicit exact-path failure instead of a named-individual-only partial result. Property paths remain outside Direct-BGP entailment. Full proof/support wiring is Phase 40.9; Helm ceilings are Phase 40.10; multi-node exact scheduling is Phase 40.15.
