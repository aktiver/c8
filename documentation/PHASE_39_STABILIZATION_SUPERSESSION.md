# Phase 39.1–39.5 Stabilization Supersession Record

This record identifies only strict supersessions; unchanged Phase 15–39 invariants remain mandatory.

| Older invariant/evidence | Stabilized invariant | Reason |
|---|---|---|
| W3C suite checkout/fetch could appear as conformance evidence | Phase 39.2 requires manifest-driven per-test execution evidence | Fetching test data is not executing tests |
| Phase 36 release script enumerated individual Phase 36/37 static verifiers | Phase 39.5 invokes the cumulative Phase 15→39.5 static runner | The cumulative runner is strictly broader and prevents phase omissions |
| Unknown query byte hash returned `UncertifiedQuery` before scalar execution | Phase 39.4 routes supported unknown queries to bounded scalar exact-RDF execution | Offline certificates are an optimization/certification mechanism, not a correctness admission whitelist |
| Phase 21 semantic state prohibited full query-dataset residency | Phase 39.4 permits one checksum-verified full immutable dataset runtime only for the ad-hoc scalar fallback while certified hashes still use routed runtimes | General exact query admission requires a complete local RDF dataset; the selective certified fast path is preserved |
| Phase 29 cache proof referenced the older SELECT-only identity | Existing Phase 39 form-aware v2 cache/result proof remains authoritative | The newer assertion is strictly stronger across all query forms |
| Acceptance YAML omitted Phase 17–33 despite existing qualification scripts | Phase 39.5 enumerates Phase 17–33 in the authoritative registry | Cumulative release evidence must be discoverable and executable from one registry |

No historical gate is removed solely because it is inconvenient. A fast/distributed query path still requires its exact offline equivalence certificate; Phase 39.4 changes only the fallback for supported ad-hoc queries.
