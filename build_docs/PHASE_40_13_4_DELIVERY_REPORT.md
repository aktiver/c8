# NGKG Phase 40.13.4 delivery report

## Outcome

Phase 40.13.4 implements the next standards-correctness slice from the governing production plan on top of Phase 40.13.3. The official SPARQL query/results baseline improves from 287 passed / 51 failed to 326 passed / 12 failed without suppressing or relabeling genuine evaluator defects.

This candidate does not claim full SPARQL, exact online OWL completeness, complete distributed algebra, or production Kubernetes qualification.

## Code delivered

- Added standards-aware SELECT result equivalence using RDF value normalization, language-tag case normalization, multiset semantics, ordered-result handling, and blank-node isomorphism instead of literal blank-node labels.
- Preserved result limits during canonical comparison and added bounded mismatch diagnostics that expose the first differing rows.
- Retained source lexical variable order for result headers without changing typed algebra semantics.
- Added a conservative parser retry for legal comma-adjacent tokens outside comments, IRIs, and quoted strings; literals and IRIs are never rewritten.
- Established retrieval/action base IRIs for relocated positive and negative syntax tests.
- Added an RFC 4180-style CSV record parser and bijective blank-node comparison while deliberately rejecting numeric lexical rewrites.
- Added six native regressions for query equivalence, legal token separation, variable scanning, CSV quoting, blank-node renaming, and lexical preservation.
- Added a machine-readable 12-case known-gap ledger, feature-matrix corrections, a regression-threshold verifier, a Phase 40.13.4 qualification script, and an acceptance-gate entry.

## Qualification

| Gate | Result |
|---|---:|
| Locked Rust workspace tests | 202 passed, 0 failed |
| All-target/all-feature workspace check | Passed |
| Clippy safety denies | Passed |
| W3C TriG | 357 passed, 0 failed |
| W3C SPARQL query/results | 326 passed, 12 failed |
| Cumulative static gates | 44 passed |
| OpenAPI parity | Passed |
| Base/production HPC and HPA contracts | Passed statically |

The 39-case improvement consists of repaired parser/base-IRI handling and standards-correct result comparison. The remaining failures are preserved as real implementation work in `conformance/sparql11-known-gaps-phase40.13.4.json`.

## HPC and Kubernetes relationship

The serving and conformance planes retain bounded, cgroup/cpuset-aware concurrency and one native OpenMP/BLAS/Rayon lane per child. Query, fragment, and hydration replica sets retain CPU, memory, and `ngkg_admission_pending` HPA inputs; the production profile still requires the custom-metrics API.

This phase validates those contracts statically. A live multinode cluster must still prove metric freshness, HPA decisions, node-autoscaler response, topology/locality placement, drain fencing, retry idempotency, bounded spill, and result equivalence while replicas move.

## Next coding order

1. Close the 12 scalar failures: lossless RDF lexical/datatype storage, zero-length constant property paths, query/solution-scoped `BNODE`, GRAPH/VALUES/aggregate scoping, and MINUS domain semantics.
2. Require a zero-failure 338-case query/results report before using the scalar engine as the distributed differential oracle.
3. Wire exact HermiT entailment dispatch for the 70 entailment cases.
4. Differentially qualify distributed operators against the zero-failure scalar oracle.
5. Add secured federation/protocol drivers and then perform live multinode autoscaling and chaos qualification.

Ontology alignment is absent and explicitly outside the database product.
