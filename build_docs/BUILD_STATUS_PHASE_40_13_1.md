# Build Status — Phase 40.13.1

Status: `repaired-native-partially-qualified-candidate`

Implemented in this recovery increment:

- repaired the exact HermiT partition merge so the trusted proof-support and certificate-byte ceilings are passed into and enforced by the merge boundary;
- added boundary regression tests for both exact-reasoner merge ceilings;
- separated legal SPARQL parsing from immutable snapshot-certification policy;
- legal `SERVICE`, `RAND`, `NOW`, `UUID`, `STRUUID`, and `BNODE` syntax is retained in typed algebra and classified for runtime policy instead of being rejected as a compiler error;
- offline certification still rejects remote or volatile queries, while online volatile queries can use the bounded uncached scalar path;
- `SERVICE` is rejected as an unavailable execution capability after successful parsing until the policy-controlled federated handler is implemented;
- wired the shared cpuset-aware Rust/OpenMP/BLAS budget into every online-serving role and corrected Helm thread allocations so each role fits its guaranteed CPU request;
- added optional workload-aware HPA inputs from the existing per-pod `ngkg_admission_pending` metric, with a production profile that requires the Kubernetes custom-metrics API.
- generated a Rust 1.97.1 dependency lock and repaired native blockers across SPARQL/OWL algebra traversal, RDF/Arrow types, Kubernetes resource construction, distributed planning, artifact work, reasoner lifetimes, and query-planner errors;
- compiled every workspace target except `ngkg-online-serving` with all features and passed 32 targeted unit tests across the SPARQL compiler, OWL Direct classifier, exact reasoner, HPC runtime, distributed build/artifacts, hydration, and query planner.

The remaining native blocker is isolated to the online-serving Axum handler boundary: Rust 1.97.1 cannot currently prove the handler futures are generally `Send` across state-manager/catalog awaits. This is a release-blocking concurrency boundary, not a waived warning. The rest of the workspace compiles with `--all-targets --all-features` when that package is excluded.

This increment does not claim complete SPARQL federation, complete W3C conformance, online exact-HermiT dispatch, distributed property-path execution, complete distributed algebra, live multinode qualification, or production readiness. Ontology alignment remains outside the database and is neither implemented nor planned. Maven, Helm, and live Kubernetes qualification remain unavailable in the current execution environment. Missing qualification never enables a standards or production claim.
