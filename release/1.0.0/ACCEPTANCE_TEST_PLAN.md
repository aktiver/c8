# NGKG 1.0.0 GA acceptance test plan

1. Build the locked Rust workspace and production images independently on two isolated builders with network access disabled after dependency hydration.
2. Bind both build manifests to the same source checksum and reject any binary, image, chart, CRD, migration, schema, utility, or archive mismatch.
3. Install by digest into isolated HA RKE, RKE2, EKS, AKS, and GKE clusters using the declared provider overlays.
4. Run the complete Phase 40.13.24 matrix and final RC1 acceptance using the exact GA images.
5. Re-run SPARQL, authorized RDF dataset, OWL 2 DL, reasoning, multihop context-graph, storage, federation, and recovery correctness gates.
6. Prove multinode/multicore execution, retry and node-loss determinism, bounded spill, checkpoint recovery, and termination.
7. Drive CPU and memory independently to the 80% threshold and prove scale-out when either threshold is reached; prove safe scale-down and scale-from-zero.
8. Ingest checksum-frozen TriG through S3, Azure Blob, GCS, and the qualified S3-compatible RKE/RKE2 path with workload identity and read-only mounts.
9. Execute approved pod, node, zone, network, PostgreSQL, storage, checksum, upgrade, rollback, backup, restore, and clean-cluster disaster drills.
10. Exercise tenant isolation, authorization denial, TLS/mTLS, KMS, secret rotation, network policy, rate limiting, audit-chain, federation SSRF, and DNS-rebinding controls.
11. Validate `/query_logs` user/tenant attribution, query identity or redaction, epoch timestamps, human duration, and node/core/RAM accounting without cross-tenant leakage.
12. Run correctness-gated performance and capacity workloads and retain the complete hardware, software, dataset, concurrency, cost, and scaling context.
13. Validate SLO dashboards, metrics, traces, alerts, support evidence, certificate/key rotation, and operator runbooks.
14. Audit production images, processes, Cargo locks, charts, and manifests: Rust is the production runtime; Jena is absent; HermiT remains isolated to its pinned exact boundary.
15. Produce SPDX and CycloneDX SBOMs, provenance, secret/license/CVE reports, detached signatures, immutable OCI references, and a complete artifact manifest.
16. Publish to staging, download every artifact, and independently verify checksums and signatures.
17. Complete the defect ledger with no unresolved critical/high or release-blocking issue and passing regressions for every resolved RC defect.
18. Run `assess_ga_readiness.py --require-publishable`, then `certify_ga_release.py` without `--test-harness`; only the resulting signed `decision: go` certificate authorizes GA publication.
