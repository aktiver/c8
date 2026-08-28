# Phase 40.13.10 delivery report

Phase 40.13.10 adds the production-shaped entry point for RDF datasets that already exist as TriG
objects in AWS S3, Azure Blob Storage, or Google Cloud Storage. It also removes opaque UUIDs from
the normal human workflow by introducing tenant-scoped dataset names while retaining UUIDs as
internal catalog identities.

## Delivered

- `POST /v1/datasets` creates or returns a generically named dataset and server-generated UUID.
- `POST /v1/datasets/{datasetName}/imports` accepts an idempotent existing-bucket import.
- `GET /v1/datasets/{datasetName}/imports/{operationId}` returns tenant-bound import status.
- An internal UUID import route remains for automation and backward compatibility.
- Target snapshot IDs are generated deterministically when the caller omits them.
- Catalog migration 0006 adds tenant/name uniqueness and preserves legacy UUID datasets.
- The `NgkgSourceImport` CRD stores immutable, credential-free desired state.
- The operator creates read-only AWS/Azure/GCP CSI volumes, PVCs, least-privilege worker RBAC,
  Kueue jobs, strict pod security, bounded scratch, and a source-ingestion node placement contract.
- The worker discovers explicit keys or a prefix, blocks path/symlink escape, parses TriG while
  hashing, preserves per-object blank-node scope, enforces resource ceilings, and atomically
  publishes one immutable source manifest.
- Multi-object scans use bounded concurrent Rust blocking lanes derived from the pod CPU budget.
- Workload charts add the `ngkg-source-ingestion` Kueue flavor/quota and a scale-from-zero node pool.

## Standards and semantic boundary

This is RDF source acquisition, not ontology assembly and not ontology alignment. It does not infer
correspondences, map raw columns, merge vocabularies, or place closure/provenance into asserted
ontology input. It validates TriG syntax and graph identity without changing the RDF dataset.

The phase does not claim query readiness. Distributed syntax-aware decode/shuffle, source-plan
generation, projection/index construction, exact OWL qualification, snapshot certification, and
publication remain the next end-to-end ingestion increment. A 500 GB single object is streamed
without HTTP upload or local full-file buffering, but its syntax parse is not yet split across nodes.

## Qualification level

All available static and API-contract gates pass. Native Rust, Helm, live cloud CSI, Kueue, and
multinode autoscaling could not execute because those toolchains and a Kubernetes cluster are absent
from this environment. The artifact is therefore a candidate, not a production release.
