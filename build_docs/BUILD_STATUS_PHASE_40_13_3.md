# Build status — Phase 40.13.3

Status: **native build and regression suite pass; SPARQL conformance and production release gates remain open**.

## Passed in this build environment

- Rust 1.97.1 / Cargo 1.97.1 locked workspace: 196 tests passed, 0 failed.
- SPARQL compiler retrieval-base regression: passed.
- W3C harness Python safety tests: 4 passed.
- Phase 39.2, Phase 40.13.1 and Phase 40.13.3 static contracts: passed.
- SPARQL 1.1 functional inventory schema/evidence validation: 22 entries passed.
- Official pinned W3C TriG manifest: 357 passed, 0 failed, including 143 evaluation cases.
- Official pinned SPARQL query/results manifests: 287 passed, 51 failed.
- Full pinned manifest inventory: 802 total, 695 executable by this driver, 107 explicitly unsupported.

## Open release gates

- The 51 executable SPARQL query/results failures are release blockers; the matrix remains `inventory`, not `qualified`.
- Seventy entailment-regime tests are not executed as simple RDF tests. Exact HermiT/entailment dispatch must be wired before they can become executable conformance evidence.
- Thirty-seven protocol and service-description tests need dedicated protocol-level drivers.
- Helm and `kubectl` were unavailable, so no live chart rendering, custom-metrics delivery, HPA behavior, node provisioning, rescheduling or multinode fault qualification was performed.
- The inherited warning/documentation and repository-wide formatter drift remain visible but did not fail the locked workspace test run.

Ontology alignment is absent and remains out of scope.
