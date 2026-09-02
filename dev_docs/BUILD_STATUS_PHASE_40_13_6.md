# Build status — Phase 40.13.6

Status: **exact online OWL Direct-Semantics source candidate; native, Maven,
entailment-suite, and live-cluster qualification remain open**.

## Implemented and statically validated

- Authorized asserted-module selection admits only `*/semkg` ontology graphs;
  closure and provenance graphs remain excluded.
- Immutable snapshot bindings include dataset, snapshot, graph-set, active
  dataset, datatype-policy, signature, profile, consistency, import, and
  synthetic-ontology hashes.
- Legal BGPs use certified index/closure lanes only with complete coverage;
  unknown or incomplete coverage falls back to exact HermiT and illegal Direct
  BGPs fail closed.
- Deterministic candidate partitions dispatch through a bounded HTTP worker
  pool. Missing, duplicate, failed, timed-out, oversized, or identity-mismatched
  partitions cannot produce a completeness certificate.
- Exact results use the existing HermiT proof/support and Direct-Certificate
  merger.
- A dedicated reasoner StatefulSet, service, PDB, NetworkPolicy, Guaranteed-QoS
  resources, anti-affinity, HPC budgets, and workload-aware HPA contract are
  present.
- HermiT remains pinned to `org.semanticweb.hermit:1.4.5.519`.
- Phase 40.13.6 static verification, OpenAPI parity, JSON/YAML/TOML parsing, and
  cumulative static gates through Phase 40.13.5 passed in this environment.
- Ontology alignment is absent and out of scope.

## Not qualified in this environment

- Cargo/Rust, Maven, Helm, and `kubectl` are not installed, so the new Rust
  crates, Java adapter, Helm render, and Kubernetes resources were not natively
  compiled or rendered here.
- The inherited 338/338 scalar SPARQL result is retained as parent evidence; it
  was not rerun in this environment.
- The 70 entailment-regime cases were not executed. Completing them is Phase
  40.13.7.
- The standard `/sparql` and `/query` handlers do not yet assemble, dispatch,
  and merge exact entailment partitions end to end. This candidate supplies
  the authorized online routing endpoint, dispatcher/merger, and executable
  worker boundary.
- No live multinode custom-metrics, HPA, node-autoscaler, cold-start, retry,
  worker-loss, chaos, recovery, or soak qualification was performed.

Therefore exact-online, full-entailment, distributed-SPARQL, and production
claims remain disabled.
