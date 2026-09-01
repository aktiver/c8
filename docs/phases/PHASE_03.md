# Phase 3 — Compiled semantic projection

Phase 3 makes linked-data meaning explicit. The mapping compiler validates IRIs, source-schema identity, object construction, complete source-field disposition, authorization labels, and the closed `core`/`virtual`/`payload` treatment.

Reasoning-visible predicates must be `core`; payload cannot participate in RDF matching; `core` and `virtual` predicates must be queryable. Compilation emits deterministic required-column and predicate groups plus a content hash. No table or column name is used to infer business meaning.

