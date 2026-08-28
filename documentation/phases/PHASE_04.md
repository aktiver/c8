# Phase 4 — Durable identity and dense execution IDs

Phase 4 separates durable identity from snapshot-local execution encoding. Governed IRIs map deterministically to UUIDv5 GUIDs; blank nodes use source-scoped skolem IRIs; FactIDs hash subject, predicate, object, graph, provenance, and source snapshot with length delimiters and retain a full collision fingerprint.

Dictionary reducers sort and merge canonical bytes, deduplicate exactly, and assign dense IDs only after global range agreement. Worker order and retry cannot change durable identities. Conflicts and compact-key collisions fail closed.

