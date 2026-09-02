# NGKG Phase 40.13.7 delivery report

## Outcome

Phase 40.13.7 connects normal `/query` and SPARQL Protocol requests to the Phase 40.13.6 exact
OWL 2 Direct-Semantics worker plane. It is an exact-online source candidate, not a production or
full-entailment qualification claim.

This phase performs ontology loading over already ontology-grounded RDF. It contains no raw-data
mapping, vocabulary matching, or ontology-alignment workflow.

## Delivered source

- Normal query requests enter the OWL Direct legality/routing path when online Direct execution is
  enabled.
- The active dataset is authorization-preservingly restricted to graph role `semkg`; every
  admitted graph must also use the `*/semkg` IRI convention. Closure, provenance, alignment, and
  other graph roles cannot enter the reasoner ABox.
- Pinned ontology/import documents are checksum-materialized from the immutable snapshot manifest
  into the shared read-only reasoner input root.
- Exact candidate requests are deterministic and independent of current pod count.
- Requests are dispatched concurrently through a Kubernetes ClusterIP service with bounded retry,
  request checksum verification, response byte ceilings, and a complete-partition barrier.
- Duplicate immutable deliveries reuse or atomically replace the same deterministic partition
  result. Conflicting cached identities fail closed.
- Exact BGP multisets replace the parsed BGP leaves, including BGPs inside `EXISTS`. The unchanged
  scalar SPARQL algebra then evaluates joins, `OPTIONAL`, `UNION`, `MINUS`, filters, aggregates,
  `DISTINCT`, ordering, slicing, subqueries, `ASK`, `CONSTRUCT`, and `DESCRIBE`.
- `GRAPH ?g` executes each authorized named `semkg` graph independently and injects its graph
  binding into one exact multiset relation.
- `/query` returns full HermiT certificates and proof manifests plus their SHA-256 identities.
  SPARQL Protocol responses preserve standard result media types and return one evidence-envelope
  SHA-256 header.
- A differential equality gate exists for any future semantic-index or finite-closure completeness
  claim. Unknown coverage continues to route to HermiT.

## HPC and Kubernetes

- The query coordinator uses bounded async partition fan-out; HermiT work remains isolated in the
  dedicated worker pool.
- Worker partitions are immutable and scheduling-independent, so HPA replica or node changes do
  not change answer identity.
- The reasoner StatefulSet uses a headless governing service and a separate load-balanced ClusterIP
  dispatch service, required pod anti-affinity, Guaranteed-QoS whole-CPU budgets, bounded one-CPU
  JVM lanes, and workload metrics for queue depth, estimated axioms, queue age, latency, CPU, and
  memory.
- Scale-to-zero remains disabled. Coordinator/catalog capacity remains running.

## Qualification status

Passed in this environment:

- Phase 40.13.7 static contract.
- Cumulative Phase 40.13.1 through 40.13.6 static contracts.
- Control-plane and online-data-plane OpenAPI route parity.
- Python syntax checks.
- JSON plus Helm values/profile YAML parsing.

Not executable in this environment:

- Rust formatting, native compilation, Clippy, and Rust tests (Cargo/Rust are absent).
- Maven HermiT adapter build/tests (Maven is absent).
- Helm lint/template (Helm is absent).
- The pinned W3C checkout and `rdflib` entailment runner.
- A live multinode Kubernetes HPA/node-autoscaler/chaos run (`kubectl` and a cluster are absent).

The W3C runner now accepts a dedicated `--entailment-driver`, executes tests that declare OWL
Direct Semantics, and separately records tests for RDF, RDFS, D, or OWL RDF-Based regimes instead
of falsely passing them through HermiT. The archived “70 entailment cases” are not all necessarily
OWL Direct cases; qualification evidence must report the regime-expanded applicability counts.

## Release boundary

Do not set the OWL Direct service-description gate or call Phase 40.13.7 complete until the native,
Maven, Helm, applicable W3C entailment, differential, failure-injection, and live-cluster gates all
pass from checksum-bound artifacts.
