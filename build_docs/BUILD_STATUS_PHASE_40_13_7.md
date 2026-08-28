# Build status — Phase 40.13.7

Status: **exact online entailment source candidate; production qualification is open**.

## Source gates passed here

- Normal `/query` and `/sparql` request flow reaches exact distributed OWL Direct execution.
- Only authorization-preserving `semkg` active graphs enter exact ABox assembly.
- Unknown index/closure coverage routes to HermiT; illegal Direct BGPs fail closed.
- Every successful BGP requires a complete checksum-consistent partition set.
- Exact multiset substitution covers all four SPARQL query forms and nested `EXISTS` BGPs.
- Full certificate/proof objects and checksums are attached to `/query` responses.
- Bounded concurrent dispatch, retries, duplicate delivery, and cached-result identity checks exist.
- Load-balanced reasoner Service, shared immutable workspace, workload-aware HPA, anti-affinity,
  and cgroup/JVM resource budgets are wired in Helm source.
- Static contracts through Phase 40.13.7 and OpenAPI parity pass.
- No raw-data mapping or ontology-alignment database functionality was added.

## Gates not run here

- `cargo fmt --check`, `cargo check`, Clippy, and Rust workspace tests.
- `mvn test` and adapter packaging.
- `helm lint` and deterministic `helm template`.
- Applicable W3C OWL Direct entailment cases.
- Imports, inconsistency, datatype, equality, cardinality, and property-chain live fixtures.
- Timeout, retry, duplicate, checksum, and partial-worker live failure injection.
- Indexed/closure versus exact HermiT differential corpus.
- Multinode HPA/node-autoscaler scale, drain, reschedule, and deterministic-answer qualification.

These remain blocking. This artifact must not advertise a complete OWL Direct or production claim.
