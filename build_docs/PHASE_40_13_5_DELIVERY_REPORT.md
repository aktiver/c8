# NGKG Phase 40.13.5 delivery report

## Outcome

Phase 40.13.5 closes all twelve executable scalar SPARQL gaps recorded by Phase
40.13.4. The pinned official SPARQL 1.1 query/result suite now reports 338
passed and 0 failed. This establishes the exact scalar differential oracle
required by the governing production plan before broader distributed algebra
qualification.

This candidate does not claim full SPARQL, exact online OWL completeness,
complete distributed algebra, secured federation, or production Kubernetes
qualification.

## Code delivered

- Pinned local compatibility copies of `oxigraph` 0.5.9, `spareval` 0.2.6,
  and `sparopt` 0.3.6 through `[patch.crates-io]`, so reference, distributed,
  and online builds consume identical semantics from the locked workspace.
- Corrected `GROUP_CONCAT` to produce a simple literal, including language-tagged
  input and singleton groups.
- Implemented query- and solution-scoped `BNODE(string)` identity.
- Corrected `MINUS` shared-domain calculation under correlated `GRAPH` input.
- Added constant-endpoint identity for zero-length `*` and `?` paths on empty
  graphs.
- Preserved active `GRAPH` scope through aggregates, `VALUES`, and correlated
  subqueries without exposing hidden correlation variables.
- Preserved numeric RDF lexical forms and derived datatype identity in storage,
  while retaining SPARQL value-space behavior for expression results and EBV.
- Added five native regression tests covering every repaired failure class.
- Added a zero-gap ledger, exact report verifier, static compatibility verifier,
  qualification script, updated feature inventory, and acceptance-gate entry.
- No ontology-alignment functionality was added.

## Qualification

| Gate | Result |
|---|---:|
| Locked Rust workspace tests | 207 passed, 0 failed |
| All-target/all-feature workspace check | Passed |
| Clippy safety denies | Passed |
| W3C TriG | 357 passed, 0 failed |
| W3C SPARQL query/results | 338 passed, 0 failed |
| Cumulative static contracts | 49 passed |
| OpenAPI parity | Passed |
| Base/production HPC and HPA contracts | Passed statically |

The conformance report is bound to W3C RDF Tests commit
`8af71fed933539d09d5f4658fb1ea7ba4c8e30b9`, records bounded parallel execution,
and enforces one nested OpenMP/BLAS/MKL/BLIS/NumExpr/Rayon lane per child.

## HPC and Kubernetes relationship

The semantic repair is compiled into every process that uses the workspace
evaluator. Existing query, fragment, hydration, materialization, and reasoner
worker classes retain bounded cgroup/cpuset-aware concurrency. Query, fragment,
and hydration replica sets retain CPU, memory, and `ngkg_admission_pending` HPA
signals; the production profile requires the custom-metrics API. Operator jobs
retain immutable resource-ceiling propagation and worker-side hash checks.

This phase validates those contracts statically and uses bounded multicore
conformance execution. It does not turn still-scalar algebra into distributed
algebra, and it does not substitute for a live multinode test. A real cluster
must still prove metric freshness, HPA decisions, node-autoscaler response,
topology/locality placement, drain fencing, retry idempotency, bounded spill,
and result equivalence while replicas move.

## Next coding order

1. Wire certified exact HermiT outcomes into online query execution and qualify
   the 70 entailment-regime cases.
2. Differentially distribute `OPTIONAL`, `UNION`, `MINUS`, aggregation,
   `DISTINCT`, sorting, subqueries, and property-path frontiers against the
   zero-failure scalar oracle.
3. Add secured `SERVICE`/`SERVICE SILENT`, protocol, result negotiation, and
   service-description drivers for the remaining 37 non-entailment cases.
4. Run live multinode autoscaling, topology, chaos, recovery, and soak gates.

Ontology alignment is absent and explicitly outside the database product.
