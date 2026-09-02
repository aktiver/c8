# Build status — Phase 40.13.4

Status: **native workspace and regression gates pass; scalar SPARQL conformance and production release gates remain open**.

## Passed in this build environment

- Rust 1.97.1 / Cargo 1.97.1 locked all-feature workspace: 202 tests passed, 0 failed.
- Full workspace all-target/all-feature check: passed.
- Clippy completed with `unwrap`, `expect`, and `panic` denied; inherited documentation/style warnings remain visible.
- Modified Rust sources pass targeted `rustfmt --check`; inherited repository-wide formatter drift was not rewritten.
- W3C harness Python safety tests: 4 passed.
- Cumulative static gates through Phase 40.13: 44 passed.
- Phase 40.13.1 through 40.13.4 static contracts: passed.
- SPARQL feature inventory schema/evidence validation: 22 entries passed.
- Official pinned W3C TriG manifest: 357 passed, 0 failed.
- Official pinned SPARQL query/results manifests: 326 passed, 12 failed, improved from 287/51.
- Control-plane and online data-plane OpenAPI parity: passed.
- Base and production-profile HPC ceilings, online admission ceilings, operator propagation, and workload-aware HPA values: passed static validation.

## Open release gates

- Twelve executable W3C query/results cases remain red: three aggregate/GRAPH cases, one VALUES/GRAPH case, one `BNODE(string)` scope case, one MINUS/GRAPH case, four zero-length property-path cases, and two lossless RDF lexical/datatype result cases.
- Seventy entailment-regime cases still require exact online HermiT dispatch.
- Thirty-seven protocol and service-description cases still require protocol-level drivers.
- Maven, Helm, and `kubectl` are unavailable in this build environment. No HermiT Maven build, Helm render/lint, custom-metrics delivery, HPA decision, node provisioning, rescheduling, or multinode failure qualification was performed.
- Full SPARQL, distributed-algebra equivalence, exact online reasoning, and production Kubernetes claims remain disabled.

Ontology alignment is absent and remains out of scope.
