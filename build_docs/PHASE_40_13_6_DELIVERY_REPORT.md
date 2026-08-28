# NGKG Phase 40.13.6 delivery report

## Outcome

Phase 40.13.6 adds the exact online OWL Direct-Semantics vertical-slice source
architecture on top of the zero-failure Phase 40.13.5 scalar SPARQL oracle. It
connects authorized snapshot routing, deterministic candidate partitioning,
bounded HermiT worker execution, fail-closed completeness barriers,
proof/certificate merging, and a dedicated autoscaled Kubernetes worker pool.

This is not a raw-data mapping or ontology-alignment workflow. The caller has
already selected ontology-grounded RDF. Assembly means reproducibly loading
only authorized asserted ontology modules, resolving immutable pinned imports,
and validating the combined synthetic OWL snapshot.

## Delivered code

- `ngkg-online-reasoning`: authorized graph-role selection, complete snapshot
  binding, certified/incomplete/unknown routing, deterministic partition plans,
  bounded distributed dispatch, complete-set validation, exact merge, and
  workload-based replica policy.
- `ngkg-direct-reasoner`: public one-partition HermiT execution and shared exact
  result merger/request-set hashing for local and distributed paths.
- `direct-reasoner-worker`: authenticated internal execution endpoint, bounded
  queue/concurrency, shared-ontology-root validation, immutable output paths,
  CPU/heap admission checks, health, and reasoner workload metrics.
- Online-serving Direct-Semantics routing endpoint and OpenAPI contract.
- Closed JSON Schemas for ontology-snapshot bindings and distributed plans.
- Dedicated Kubernetes StatefulSet, service, PDB, NetworkPolicy, anti-affinity,
  node-pool placement, Guaranteed QoS, and multi-signal HPA.
- Phase acceptance, static qualification, documentation, and machine-readable
  evidence.

## Correctness and failure boundaries

- Only authorized `*/semkg` graphs become asserted modules. Closure and
  provenance are never inferred to be asserted input.
- Unknown is never treated as false. Uncertified acceleration lanes route to
  exact HermiT.
- Partition count is derived from the finite candidate ceiling and remains
  independent of current pod count.
- Every partition must cover one exact contiguous range and agree on dataset,
  snapshot, query, BGP, candidate-space, aggregate-input, HermiT version, and
  checksums before merge.
- Any missing response, timeout, transport error, non-success status, response
  overflow, malformed result, checksum mismatch, or incomplete range aborts
  the exact result. Partial answers cannot receive a completeness certificate.

## HPC and Kubernetes design

The reasoner pool is isolated from query/fragment/catalog workers. Each pod
validates one cgroup-aware budget across Rust orchestration, JVM reasoner lanes,
blocking I/O, OpenMP, BLAS, and control work. JVM heap admission is bounded to
80 percent of the pod memory limit. Deterministic partitions are spread across
ready worker addresses with bounded concurrency; Kubernetes may change the
number of replicas without changing semantic partition identity.

The production contract uses at least two reasoner replicas and two eligible
nodes, pod anti-affinity, and HPA signals for CPU, memory, queued candidate
partitions, estimated axioms, oldest queue age, and mean service latency.
Scale-to-zero is deliberately disabled pending durable-queue and cold-start-SLO
evidence. Coordinator/catalog capacity remains running.

## Qualification status and next work

Static Phase 40.13.6 and cumulative Phase 40.13.1–40.13.5 checks, API parity,
and data-file parsing passed. This runtime does not provide Cargo, Maven, Helm,
or `kubectl`, so native compilation, Java tests, chart rendering, and live
cluster behavior remain unqualified.

Phase 40.13.7 must bind the standard `/sparql` response path to this dispatcher,
execute all 70 entailment-regime cases, compare certified index/closure results
against exact HermiT, and qualify inconsistent ontologies, imports, datatypes,
equality, number restrictions, property chains, timeouts, retries, and partial
worker failure. No production or complete-entailment claim should be activated
before those gates pass.
