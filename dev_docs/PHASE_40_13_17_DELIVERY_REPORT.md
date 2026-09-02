# Phase 40.13.17 delivery report

## Outcome

This candidate adds an online partition-native property-path traversal lane over Phase 40.13.12
semantic adjacency artifacts. It distributes immutable storage partitions across fragment-worker
pods, splits hot vertices across bounded Rust core lanes, persists bounded checkpoints, and uses a
dense global barrier for exact termination. The scalar oracle continues to own the externally
published SPARQL result until live differential qualification proves the new lane.

## Production logic added

- checksum-bound forward/reverse fixed-record adjacency readers with binary vertex seeks;
- global-dictionary predicate resolution without loading the enterprise dictionary into RAM;
- correct union-default, fixed named-graph, and graph-variable traversal scopes;
- literal endpoint indexing and graph-scoped origin/visited/endpoint identities;
- authenticated internal partition path REST route and OpenAPI contract;
- deterministic storage-partition-to-pod scheduling and concurrent wave dispatch;
- bounded within-pod hot-vertex splitting over shared immutable adjacency;
- fail-closed missing/duplicate/corrupt partition and work-item barriers;
- atomic local plus immutable object-store iteration checkpoints;
- cumulative path spill, frontier, visited, scan-row, response, iteration, and thread ceilings;
- HPA signals for pending partitions, active frontiers, and checkpoint bytes;
- phase corpus, static gate, live-cluster qualification script, and evidence contract.

The semantic compiler now retains literal objects in path adjacency. Existing Phase 40.13.12–16
snapshots must be recompiled before this lane is enabled because their adjacency may omit those
standards-valid endpoints.

## Qualification executed here

- Phase 40.13.17 static contract: passed.
- Phase 40.13.16 static contract: passed.
- REST/OpenAPI parity: 16 control-plane and 18 online operations, passed.
- Helm base and production-overlay cross-resource validation: passed.
- JSON Schema, OpenAPI YAML, corpus JSON, Python, and shell syntax: passed.

## Open gates

Cargo/rustc/rustfmt, Maven, Helm, and kubectl are unavailable in this environment. Native
formatting, compilation, Clippy, Rust tests, complete cumulative static qualification, Helm
lint/render, live multinode path equality, pod/node churn, checkpoint resume, HPA response, and
cluster autoscaling were not run.

The new traversal computes and checksum-binds endpoint sets, but it does not yet substitute those
sets into every surrounding distributed algebra context. Phase 40.13.16's scalar oracle therefore
still supplies the final result and retains its 64 GiB compatibility-image ceiling. This candidate
is not a production-qualified large-snapshot release.

No ontology alignment, schema matching, or raw-data mapping was added.
