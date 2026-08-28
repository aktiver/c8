# NGKG 1.0.0-RC1 final acceptance-test plan

1. Verify the source archive and top-level checksum signature in an isolated workspace.
2. Reproduce all Rust binaries, OCI image indexes, Helm packages, CRD bundles, migrations, API/schema bundles, and utilities on two isolated network-controlled builders.
3. Compare the complete output manifests byte-for-byte and reject any mismatch.
4. Verify artifact signatures, provenance, SPDX and CycloneDX SBOMs, secret scans, license policy, and zero unapproved critical/high CVEs.
5. Execute all cumulative static, Rust, Maven, JSON Schema, OpenAPI, Helm, and Kubernetes server-side dry-run gates.
6. Execute pinned RDF 1.1 TriG, SPARQL 1.1, OWL 2 DL, result-format, protocol, federation, and differential suites.
7. Validate authorized cross-domain multi-hop `CONSTRUCT` and `DESCRIBE` context graphs against the scalar oracle with complete proof/support coverage.
8. Install from empty RKE, RKE2, EKS, AKS, and GKE clusters using only frozen Helm values and immutable image digests.
9. Exercise Amazon S3, Azure Blob, Google Cloud Storage, and S3-compatible whole-TriG ingestion through workload identity.
10. Run multi-node ingestion, compilation, reasoning, SPARQL, property-path, federation, recovery, and 250-user capacity qualification with 80%-CPU-or-memory autoscaling and scale-from-zero pools.
11. Retain 72-hour concurrent soak evidence and prove zero semantic mismatch or uncertified partial response.
12. Inject approved pod/node, network, database, CSI, checksum, duplicate-delivery, and object-corruption failures; require exact recovery and semantic identity.
13. Upgrade from the prior qualified deployment, inject a failed upgrade, roll back, and prove no partially migrated or uncertified snapshot became visible.
14. Back up the active snapshot, restore into a fresh cluster, verify RPO/RTO, and compare all snapshot, query, proof, and artifact identities.
15. Verify tenant isolation, forged identity denial, workload identity, default-deny networking, TLS/mTLS, audit-chain integrity, rate limits, federation SSRF/DNS defenses, key rotation, and secret absence.
16. Generate the closed prerequisite ledger, compatibility freeze, signed artifact manifest, support matrix, known-issues digest, and final acceptance evidence.
17. Issue an RC1 publication certificate only when every evidence class is live, every subject digest matches the release, every failure count is zero, and every required artifact is signed.
