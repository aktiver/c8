# Phase 40.13.15 acceptance: atomic publication and query activation

- Cloud imports are registered in the tenant-isolated PostgreSQL operation catalog before Kubernetes work begins.
- The activation worker checksum-verifies the semantic, authorization/qualification, and offline-reasoning roots plus every referenced partition artifact.
- Only authorized `https://c8-next-generation.io/*/*/semkg` graphs are query- and reasoning-visible; closure, provenance, alignment, and raw mapping inputs are rejected.
- One immutable activation manifest binds dataset, snapshot, parent, graph set, datatype policy, ontology, finite closure, proof support, and all partition counts.
- The activation record and certified snapshot are committed in one PostgreSQL transaction.
- Publication uses the existing active-parent compare-and-swap; a conflict never exposes a partial snapshot.
- Manual publication refuses a snapshot without a matching legacy serving certificate or cloud activation record.
- Ordinary `/sparql` and non-hydrating `/query` requests resolve the published cloud snapshot through catalog truth and the scalar standards oracle.
- Physical payload hydration remains fail-closed until the cloud layout has a certified locator; it is not silently synthesized.
- Kueue/Indexed Jobs retain stable logical partitions, bounded parallel verification, cgroup CPU/thread budgets, and Cluster Autoscaler-compatible pending demand.
- No ontology alignment or raw-data mapping functionality is present.

Native Rust, PostgreSQL integration, Helm render/lint, and multinode Kubernetes tests remain mandatory release gates.
