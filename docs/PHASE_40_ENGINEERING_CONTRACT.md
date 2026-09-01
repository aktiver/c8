# NGKG Phase 40 Engineering Contract

Phase 40 is the governance and qualification baseline built directly on the Phase 39.5 stabilization candidate. It does not claim OWL Direct runtime completeness. It freezes the inherited implementation, enumerates the Phase 40 work sequence, records current versus planned HPC ceilings, and makes REST/OpenAPI visibility a release invariant before semantic changes begin.

## Incremental implementation sequence

1. **40.1** — OWL signature runtime contract and JSON Schema.
2. **40.2** — Datatype-policy runtime contract and JSON Schema.
3. **40.3** — Direct-BGP result runtime contract and JSON Schema.
4. **40.4** — Direct certificate runtime contract and JSON Schema.
5. **40.5** — Combined OWL 2 DL profile/import qualification hardening.
6. **40.6** — OWL consistency qualification hardening.
7. **40.7** — Legal Direct-BGP classification and validation.
8. **40.8** — Exact HermiT/Direct reasoner fallback.
9. **40.9** — Proof/support IDs and certificate verification.
10. **40.10** — Phase 40 Helm ceilings.
11. **40.11** — Reference-worker ceiling wiring.
12. **40.12** — Distributed-worker ceiling wiring.
13. **40.13** — Operator/distributed-operator ceiling wiring.
14. **40.14** — HPC thread/cpuset discipline.
15. **40.15** — Multi-node exact execution hardening.
16. **40.16** — Kubernetes networking, security and placement.
17. **40.17** — HPA/KEDA/node-autoscaling completion.
18. **40.18** — Compile-manifest and staging updates.
19. **40.19** — JSON Schema synchronization.
20. **40.20** — JSON Schema meta-validation and OpenAPI validation.
21. **40.21** — Platform/Helm/RKE2 validation.
22. **40.22** — Placeholder and edit-artifact scans.
23. **40.23** — Phase 39.5→40 checksum inheritance verification.
24. **40.24** — Cumulative Phase 15→40 gates.
25. **40.25** — Cargo/Maven/Helm/RKE2 qualification attempt.
26. **40.26** — Final per-file SHA-256 manifest.
27. **40.27** — Independently verified `NGKG_PHASE_40_CANDIDATE.zip`.

## Baseline invariants

- Phase 39.5 files may change only with explicit Phase 40 evidence; inherited files may not disappear silently.
- Standards claims remain disabled until their exact qualification gates pass.
- Existing distributed Arrow, Parquet, mmap, NVMe, Grace-join and bounded-concurrency paths remain intact.
- New Direct-reasoner ceilings are registry-only until the 40.10–40.13 wiring phases.
- Every REST operation must be represented in its OpenAPI contract and reachable from a vendored Swagger UI.
- A static gate is evidence of source/configuration consistency only; it never substitutes for Cargo, Maven, Helm or live RKE2 qualification.
