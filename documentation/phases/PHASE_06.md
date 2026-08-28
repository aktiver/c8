# Phase 6 — Semantic indexes, proofs, and direct locators

Phase 6 builds class/property extents, graph routing, proof/dependency structures, statistics, and the Global Locator Index directly from Arrow contributions. Every index open verifies dataset, snapshot, ontology, dictionary, policy, schema, format, and payload checksum.

The locator directory partitions the full key space into non-overlapping ranges with replicas. A key routes to one responsible shard; it is never broadcast and no file listing is used. Returned physical ranges are coalesced by object, row group, column mask, graph, and checksum while preserving query ordinals at the caller.

