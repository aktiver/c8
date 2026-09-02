# NGKG Phase 40.13.2 delivery report

## Outcome

The release-blocking online-serving ownership defect from Phase 40.13.1 is repaired. The entire Rust workspace now checks and its 195 tests pass. The existing HPC and Kubernetes autoscaling work is connected and statically validated, but live cluster scaling is not qualified in this environment.

## Runtime repairs

- Online handlers no longer carry borrowed repository/state/path/plan values across awaits. State-manager entry points own `Arc<Self>` and materialization calls own their inputs.
- Immutable serving-state acquisition uses an owned repository future, satisfying Axum's general `Send + 'static` handler contract.
- Payload cache misses are serialized with a dedicated single-flight mutex. Network and disk reads occur without holding the shared payload map, after which checksummed immutable shards are inserted under the byte ceiling.
- Distributed fragment dispatch clones the bounded fragment vector before constructing concurrent futures, avoiding a plan borrow across the stream lifetime.

## Correctness repairs found by the full test run

- SPARQL Results simple literals without datatype or language are accepted by both JSON-to-Arrow encoding and Arrow decoding. A literal carrying both qualifiers remains invalid.
- The reference service default graph is materialized as an RDF set union, preventing duplicate solutions when the same triple exists in multiple named graphs.
- Blank nodes are deterministically scoped to their named graph before dataset merging, so equal source labels from different graphs never collapse.
- Invalid N-Quads numeric/boolean fixtures were replaced with valid typed RDF literals.
- The online OpenAPI route contract test includes the OWL Direct validation endpoint.

## HPC and Kubernetes behavior

All online roles consume the shared cgroup/cpuset-aware thread budget. Rust compute, blocking I/O, control, OpenMP, OpenBLAS, and MKL lanes must fit the pod CPU assignment; nested native parallelism is disabled. Query, fragment, and hydration pools expose bounded admission pressure. Their HPAs use CPU and memory, and the production profile additionally uses `ngkg_admission_pending` through a required custom-metrics API with conservative scale-down stabilization.

This is the implemented scaling control path, not proof of production scaling. A real cluster must still demonstrate metric delivery, burst response, locality, drain fencing, node provisioning, scale-down safety, and semantic correctness during rescheduling.

## Qualification and remaining order

Native workspace check, 195 tests, Clippy deny rules, OpenAPI parity, value cross-checks, static contracts, and structural validation pass. Gate A remains open for formatting/warnings-as-errors cleanup, Maven tests, Helm rendering, a clean release build in the pinned builder, SBOM/signing, and live Kubernetes qualification.

The next engineering work remains Workstream B from the governing plan: produce the normative SPARQL 1.1 feature matrix and executable official manifest runner, then close front-end/reference-engine conformance before expanding distributed operator coverage. Exact online HermiT dispatch follows the standards-correct scalar boundary; multinode execution and autoscaling qualification follow only after their semantic differential tests exist.

Ontology alignment is neither present nor planned.
