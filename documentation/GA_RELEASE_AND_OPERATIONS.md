# NGKG 1.0.0 GA release and operations guide

## Release boundary

NGKG 1.0.0 is a Kubernetes-native distributed Rust RDF/OWL database release. The production system does not link, embed, or deploy Apache Jena. HermiT/OWLAPI remains a pinned, isolated exact OWL 2 DL qualification boundary and cannot silently replace the Rust query, storage, compiler, or distributed execution runtime.

GA publication is fail-closed. Source inspection, static tests, synthetic fixtures, or test-harness certificates demonstrate the machinery but do not authorize a supported production release. Publication requires live certificates from the exact final images, two identical isolated builds, signed immutable artifacts, a completed provider matrix, and a `decision: go` GA certificate.

## Installation targets

The core Helm behavior is provider-neutral. Provider overlays supply workload identity, storage classes, object-store drivers, KMS references, CNI details, and the selected node provisioner for RKE/RKE2, EKS, AKS, or GKE. At least three worker nodes and two failure zones are required when the provider exposes zones. The qualified version matrix in `release/1.0.0/support-matrix.yaml` is authoritative; empty version lists mean the combination is not supported yet.

## Autoscaling and HPC execution

CPU or memory reaching 80% of requested capacity triggers scale-out. HPA/KEDA owns pod demand, Kueue admits finite batch resources, distributed operators create Indexed Jobs, and exactly one cluster or provider autoscaler owns node-pool demand. Query, reasoning, traversal, compilation, ingestion, and recovery lanes scale independently. Scale-down is blocked while spill, response spools, frontier state, checkpoints, or recovery ownership is active.

Workers use cgroup-v2 limits to bound Rust execution lanes and buffers. Whole-core Guaranteed QoS and single-threaded nested math/runtime libraries prevent oversubscription when multiple partitions execute on one node. Parallel-safe qualification work uses Kueue Indexed Jobs across topology-spread workers; node-loss, upgrade, rollback, and destructive recovery cases run in isolated serialized provider lanes.

## TriG cloud ingestion

Amazon S3, Azure Blob Storage, Google Cloud Storage, and qualified S3-compatible RKE/RKE2 paths use workload identity and read-only mounts or object APIs. The compiler accepts checksum-frozen whole-TriG manifests, preserves named graph identity, validates the canonical `https://c8-next-generation.io/<scope>/<subdomain>/{semkg|closure|provenance}` roles, and publishes nothing until all compilation and reasoning partitions certify.

## Query and reasoning operations

Clients use `/sparql` or `/query`; union-default construction occurs only after tenant authorization. OWL 2 DL snapshots bind asserted graphs, imports, closure, proofs, and datatype policy to one immutable identity. Multihop cross-domain results are returned as a unified reasoned context graph only when the distributed result equals the certified scalar semantics and all required proof coverage is available.

`/v1/query_logs` is an authorized operational route. Records include the requesting principal and tenant, request/query identity or approved redaction, snapshot, start and end epochs, human-readable duration, and nodes, cores, and RAM activated or consumed. Query-log access must remain tenant-scoped, rate-limited, audit-linked, and unavailable to ordinary users without the explicit observation permission.

## Upgrade and rollback

Back up and verify the catalog, active snapshot metadata, object roots, proof roots, and Kubernetes custom resources before upgrade. Apply CRDs and database migrations in the documented order, preserve controller/worker skew limits, and keep the old active snapshot visible until the new version has completed certification. A failed upgrade rolls services and Helm state back; an irreversible database migration requires restore into a clean cluster rather than an unsafe reverse migration.

## Backup and disaster recovery

Backups are checksum-bound manifests over catalog state and immutable objects. Restore into an isolated namespace or clean cluster, verify every partition and proof root, re-establish workload identity and encryption references, then activate with compare-and-swap. Never expose a partially restored or partially certified snapshot. Retain measured recovery-point and recovery-time evidence for the supported platform declaration.

## Security operations

Use provider workload identity instead of long-lived cloud keys. Keep default-deny ingress and egress, encrypted external and internal transport, KMS-backed storage, non-root containers, dropped capabilities, seccomp, tenant-scoped quotas, immutable audit chains, and federation endpoint allowlists enabled. Rotate certificates, signing identities, KMS references, and federation secrets according to the runbooks; a rotation must not change immutable snapshot identity.

## Monitoring and support evidence

Monitor request and SPARQL latency, queue depth, CPU/memory saturation, scaling lag, worker placement, spill and checkpoint pressure, reasoning load, storage errors, checksum failures, throttling, recovery progress, and audit-chain health. Correlation identifiers connect the incoming request to its plan, workers, storage reads, proof evidence, resource accounting, and final response. Preserve the exact values, image digests, snapshot IDs, logs, metrics, traces, events, and certificate checksums used for a support case.

## GA go/no-go procedure

Run `acceptance/ga.sh` for source/static barriers. Execute the live plan in `release/1.0.0/ACCEPTANCE_TEST_PLAN.md`, populate the exact five-provider support matrix, close the defect ledger, build and sign every artifact, and independently verify staged downloads. `scripts/assess_ga_readiness.py --require-publishable` must pass before `scripts/certify_ga_release.py` runs without test-harness mode. Any semantic mismatch, cross-tenant access, data-integrity failure, recovery failure, unsigned artifact, reproducibility mismatch, unresolved critical/high defect, or missing provider certificate produces a no-go decision.
