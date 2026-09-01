# Phase 1 — Durable catalog and REST control plane

Phase 1 adds PostgreSQL-backed operation identity, row-level tenant isolation, immutable audit rows, explicit state transitions, and an Axum API that acknowledges durable job creation rather than pretending asynchronous ingestion has finished.

The API requires an idempotency key and hashes the validated request. A repeated key with different bytes returns a conflict. Readiness depends on PostgreSQL. Missing configuration or catalog access stops startup/readiness; there is no in-memory fallback.

