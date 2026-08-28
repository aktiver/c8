# Phase 40.13.12 delivery report

Phase 40.13.12 adds a real cloud-to-semantic-storage compilation path on the verified Phase
40.13.11 handoff. It distributes fragment mapping and logical partition reduction across
Kubernetes Indexed Jobs, uses bounded external sort/merge and concurrent object verification,
and produces dense term/GUID dictionaries, canonical graph partitions, Parquet facts, adjacency
indexes, semantic indexes, graph inventory, and one checksum-bound inactive root.

The Kubernetes operator now advances a cloud import through four fail-closed barriers and records
each job plus dictionary/root checksums in `NgkgSourceImport.status`. Kueue queue labels and the
`semantic-projection` workload class allow pending jobs to drive node autoscaling, while pod CPU,
memory, scratch, parser-lane, fan-in, row-group, and concurrency ceilings constrain resource use.

This phase intentionally does not make data query-visible. Physical graph classification is not
authorization; `*/semkg` entries are candidates only. Phase 40.13.13 must perform deterministic
OWL 2 DL snapshot qualification, and Phase 40.13.15 must atomically publish the qualified root.

Static evidence passed. Native compilation and live Kubernetes/cloud tests remain explicitly
blocked in this environment; see `BUILD_STATUS_PHASE_40_13_12.md`.
