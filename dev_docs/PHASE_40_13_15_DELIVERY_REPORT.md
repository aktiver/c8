# Phase 40.13.15 delivery report

Phase 40.13.15 closes the publication gap between the cloud compiler/reasoner pipeline and the catalog used by ordinary online queries. Cloud-import desired state is now backed by a durable PostgreSQL operation, and a new all-roots barrier verifies the semantic compilation root, the authorized OWL 2 DL qualification root, the exact HermiT offline-reasoning root, and every referenced partition artifact before any snapshot can become visible.

The worker produces two immutable records. A reference-compatible scalar serving image lets the existing standards-correct query oracle open the snapshot through `/sparql` and `/query`; a separate activation manifest cryptographically binds dataset, snapshot, parent, graph set, datatype policy, synthetic ontology, finite closure, proof supports, source facts, and both logical partition sets. The catalog inserts that activation record and the certified snapshot in one transaction. Publication then uses the existing compare-and-swap against the expected active parent, so readers observe either the old snapshot or the complete new snapshot, never a mixture.

The publication coordinator is deliberately single-writer, while expensive artifact verification is bounded and concurrent. Earlier semantic and reasoning stages remain distributed as stable Indexed Jobs across cores and nodes; Kueue limits active fan-out, and pending pods provide Cluster Autoscaler demand. Rust, OpenMP, BLAS, and JVM thread ceilings remain cgroup-aware. A single transaction is not parallelized because doing so would weaken atomicity.

Only explicitly authorized `https://c8-next-generation.io/*/*/semkg` graphs are query- and reasoning-visible. Closure, provenance, and alignment graphs never enter asserted ontology assembly. The code adds no ontology alignment, schema matching, source-to-ontology mapping, or raw-data transformation job.

The scalar compatibility path is admitted only up to 64 GiB, matching the current online object ceiling. Larger inputs—including a 500 GB TriG source—complete compilation and reasoning but remain inactive until Phase 40.13.16 consumes partition-native artifacts directly through the complete distributed SPARQL runtime. Physical payload hydration also fails closed on cloud activation until a real locator/payload serving layout is qualified.

Static evidence is green. Native Rust, PostgreSQL fault tests, Helm, and live multinode Kubernetes gates are blocked by missing tools/infrastructure in this environment and remain required before production claims.
